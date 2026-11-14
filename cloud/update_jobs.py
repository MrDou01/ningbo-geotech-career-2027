#!/usr/bin/env python3
"""Public-source change monitor; never performs eligibility or talent assessment.

Standard library only. No credentials, cookies, private materials or local research
files are read. HTTP success and text changes are observations, not verification.
"""
from __future__ import annotations

import argparse
import copy
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time as dt_time, timedelta, timezone
import hashlib
from html.parser import HTMLParser
import http.client
import ipaddress
import json
import os
from pathlib import Path
import queue
import re
import socket
import sqlite3
import ssl
import sys
import tempfile
import threading
import time
from urllib.parse import parse_qsl, quote, unquote, urljoin, urlsplit, urlunsplit

SHANGHAI = timezone(timedelta(hours=8), "Asia/Shanghai")
MAX_BYTES = 2 * 1024 * 1024
MAX_ATTACHMENT_BYTES = 5 * 1024 * 1024
MAX_ATTACHMENTS = 6
FETCH_TIMEOUT = 15
RUN_TIMEOUT = 600
SENSITIVE_KEYS = re.compile(r"(password|passwd|secret|token|authorization|cookie|api.?key|signature)", re.I)
RECRUITMENT = re.compile(r"招聘|招募|引才|人才引进|博士后|校园招聘|recruit|career|vacanc|job", re.I)
CAVEAT = "仅自动抓取、发现链接和变化监测；不自动完成岗位资格语义审查、论文认定或人才定档，不刷新人工核验日期。"


def json_text(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, indent=2) + "\n"


def stamp(now):
    return now.astimezone(SHANGHAI).isoformat(timespec="seconds")


def normalize_url(url):
    if not isinstance(url, str) or len(url) > 4096 or re.search(r"[\x00-\x20\x7f\\]", url):
        raise ValueError("invalid_url")
    try:
        parsed = urlsplit(url)
        if parsed.scheme.lower() not in ("http", "https") or not parsed.hostname:
            raise ValueError("invalid_scheme_or_host")
        if parsed.username is not None or parsed.password is not None:
            raise ValueError("userinfo_forbidden")
        host = parsed.hostname.rstrip(".").encode("idna").decode("ascii").lower()
        if host == "localhost" or host.endswith((".localhost", ".local", ".internal")):
            raise ValueError("local_host_forbidden")
        port = parsed.port
        if port is not None and port not in (80, 443):
            raise ValueError("non_web_port")
        if any(SENSITIVE_KEYS.search(key) for key, _ in parse_qsl(parsed.query, keep_blank_values=True)):
            raise ValueError("credential_query_forbidden")
        authority = ("[" + host + "]") if ":" in host else host
        if port and port != (443 if parsed.scheme.lower() == "https" else 80):
            authority += ":" + str(port)
        return urlunsplit((parsed.scheme.lower(), authority,
                           quote(parsed.path or "/", safe="/%:@!$&'()*+,;=-._~"),
                           quote(parsed.query, safe="=&%/:?@!$'()*+,;~-._"), ""))
    except (UnicodeError, TypeError) as exc:
        raise ValueError("invalid_url") from exc


def public_ip(address):
    ip = ipaddress.ip_address(address)
    return ip.is_global and not (ip.is_multicast or ip.is_reserved or ip.is_unspecified
                                or ip.is_loopback or ip.is_link_local or ip.is_private)


def validate_public_url(url, resolver=None):
    normalized = normalize_url(url)
    p = urlsplit(normalized)
    resolver = resolver or socket.getaddrinfo
    try:
        literal = ipaddress.ip_address(p.hostname)
    except ValueError:
        records = resolver(p.hostname, p.port or (443 if p.scheme == "https" else 80),
                           type=socket.SOCK_STREAM)
        addresses = list(dict.fromkeys(item[4][0] for item in records))
    else:
        addresses = [str(literal)]
    if not addresses or not all(public_ip(address) for address in addresses):
        raise ValueError("non_public_address")
    return normalized, addresses


def safe_url(url):
    try:
        return normalize_url(url)
    except (ValueError, TypeError):
        # Never log credentials or raw invalid URLs.
        return "[rejected-url]"


def bounded_resolve(host, port, **kwargs):
    result = queue.Queue(maxsize=1)

    def resolve():
        try:
            result.put(socket.getaddrinfo(host, port, **kwargs))
        except Exception:
            result.put(None)
    threading.Thread(target=resolve, daemon=True).start()
    try:
        value = result.get(timeout=4)
    except queue.Empty:
        raise TimeoutError("dns_timeout")
    if value is None:
        raise OSError("dns_failed")
    return value


class VisibleHTML(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.hidden = 0
        self.parts = []
        self.links = []
        self.anchor = None

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag in ("script", "style", "noscript", "template"):
            self.hidden += 1
        if self.hidden:
            return
        if tag == "a":
            self.anchor = [dict(attrs).get("href", ""), []]
        if tag in ("p", "div", "br", "li", "tr", "h1", "h2", "h3"):
            self.parts.append(" ")

    def handle_endtag(self, tag):
        if tag.lower() in ("script", "style", "noscript", "template"):
            self.hidden = max(0, self.hidden - 1)
        if not self.hidden and tag.lower() == "a" and self.anchor:
            if len(self.links) < 500:
                self.links.append({"url": self.anchor[0],
                                   "title": " ".join("".join(self.anchor[1]).split())[:180]})
            self.anchor = None

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)
            if self.anchor:
                self.anchor[1].append(data)

    @property
    def text(self):
        return " ".join(" ".join(self.parts).split())


def fetch_public(url, binary=False):
    """Resolve and pin public IP per hop; bypass proxies and revalidate redirects."""
    start = time.monotonic()
    byte_limit = MAX_ATTACHMENT_BYTES if binary else MAX_BYTES
    result = {"url": safe_url(url), "finalUrl": safe_url(url), "httpStatus": None,
              "ok": False, "bodyText": "", "links": [], "contentType": "", "redirects": []}
    current = url
    connection = None
    try:
        for hop in range(4):
            if time.monotonic() - start >= FETCH_TIMEOUT:
                raise TimeoutError("fetch_timeout")
            current, addresses = validate_public_url(current, bounded_resolve)
            result["finalUrl"] = current
            p = urlsplit(current)
            remaining = max(0.1, FETCH_TIMEOUT - (time.monotonic() - start))
            if p.scheme == "https":
                connection = http.client.HTTPSConnection(p.hostname, p.port or 443,
                    timeout=remaining, context=ssl.create_default_context())
            else:
                connection = http.client.HTTPConnection(p.hostname, p.port or 80, timeout=remaining)
            # Host header and TLS SNI still use the original hostname; connect to the
            # validated address to prevent DNS rebinding between check and request.
            pinned_ip = addresses[0]
            connection._create_connection = lambda address, timeout, source_address=None: socket.create_connection(
                (pinned_ip, p.port or (443 if p.scheme == "https" else 80)),
                timeout=timeout, source_address=source_address)
            target = urlunsplit(("", "", p.path or "/", p.query, ""))
            connection.request("GET", target, headers={
                "User-Agent": "PublicRecruitmentChangeMonitor/1.0",
                "Accept": "text/html,text/plain,application/xhtml+xml;q=0.9",
                "Accept-Encoding": "identity", "Connection": "close"})
            response = connection.getresponse()
            result["httpStatus"] = response.status
            result["contentType"] = response.getheader("Content-Type", "")[:200]
            if response.status in (301, 302, 303, 307, 308):
                location = response.getheader("Location")
                if not location or hop == 3:
                    raise ValueError("redirect_limit_or_missing_location")
                next_url = urljoin(current, location)
                result["redirects"].append({"url": current, "httpStatus": response.status,
                                            "location": safe_url(next_url)})
                connection.close()
                current = next_url
                continue
            if response.status != 200:
                result["error"] = "http_status"
                return result
            content_type = result["contentType"].lower()
            if not binary and not any(x in content_type for x in ("text/html", "text/plain", "application/xhtml+xml")):
                result["error"] = "unsupported_content_type"
                return result
            if response.getheader("Content-Encoding", "identity").lower() not in ("identity", ""):
                result["error"] = "unsupported_content_encoding"
                return result
            declared = response.getheader("Content-Length", "")
            if declared.isdigit() and int(declared) > byte_limit:
                result["error"] = "body_too_large"
                return result
            chunks = []
            total = 0
            while True:
                remaining = FETCH_TIMEOUT - (time.monotonic() - start)
                if remaining <= 0:
                    raise TimeoutError("fetch_timeout")
                read_socket = connection.sock or getattr(getattr(response.fp, "raw", None), "_sock", None)
                if read_socket:
                    read_socket.settimeout(min(3, remaining))
                chunk = response.read1(min(65536, byte_limit + 1 - total))
                if not chunk:
                    break
                chunks.append(chunk)
                total += len(chunk)
                if total > byte_limit:
                    result["error"] = "body_too_large"
                    return result
            body = b"".join(chunks)
            if binary:
                result.update(ok=True, dataBytes=body, bytes=total)
                return result
            charset_match = re.search(r"charset\s*=\s*[\"']?([\w-]+)", content_type)
            if not charset_match:
                charset_match = re.search(r"charset\s*=\s*[\"']?([\w-]+)", body[:8192].decode("ascii", "ignore"), re.I)
            charset = charset_match.group(1) if charset_match else "utf-8"
            try:
                decoded = body.decode(charset, "replace")
            except LookupError:
                decoded = body.decode("utf-8", "replace")
            if "html" in content_type:
                parser = VisibleHTML()
                parser.feed(decoded)
                result["bodyText"] = parser.text
                result["links"] = parser.links
            else:
                result["bodyText"] = " ".join(decoded.split())
            result["bytes"] = total
            result["ok"] = True
            return result
    except Exception as exc:
        result["error"] = type(exc).__name__  # no raw response, URL credentials or server messages
    finally:
        if connection:
            connection.close()
        result["elapsedMs"] = round((time.monotonic() - start) * 1000)
    return result


def parse_deadline(job):
    full_time = job.get("deadlineTime")
    if isinstance(full_time, str) and re.match(r"^\d{4}-\d{2}-\d{2}T", full_time):
        try:
            value = datetime.fromisoformat(full_time.replace("Z", "+00:00"))
            return (value if value.tzinfo else value.replace(tzinfo=SHANGHAI)).astimezone(SHANGHAI)
        except ValueError:
            return None
    raw = job.get("deadline")
    if not isinstance(raw, str):
        return None
    try:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
            day = datetime.strptime(raw, "%Y-%m-%d").date()
            clock = job.get("deadlineTime")
            if clock:
                if not isinstance(clock, str) or not re.fullmatch(r"\d{1,2}:\d{2}(?::\d{2})?", clock):
                    return None
                parts = [int(part) for part in clock.split(":")]
                end = dt_time(*parts)
            else:
                end = dt_time(23, 59, 59, 999999)
            return datetime.combine(day, end, SHANGHAI)
        if not re.match(r"^\d{4}-\d{2}-\d{2}T", raw):
            return None
        value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        return (value if value.tzinfo else value.replace(tzinfo=SHANGHAI)).astimezone(SHANGHAI)
    except (ValueError, TypeError):
        return None


def is_official(job):
    trust = job.get("sourceTrust", "")
    return trust in ("official", "official_platform") or (
        not trust and "官方" in str(job.get("sourceLevel", "")) and "二手" not in str(job.get("sourceLevel", "")))


def known_deadline(job):
    return (is_official(job) and any(job.get(key) for key in
            ("verifiedAt", "windowVerifiedAt", "deadlineVerifiedAt", "windowRevalidatedAt")))


def validate_data(data):
    if not isinstance(data, dict) or not isinstance(data.get("jobs"), list) or not data["jobs"]:
        raise ValueError("dashboard must contain a nonempty jobs list")
    ids = set()
    for job in data["jobs"]:
        if not isinstance(job, dict) or not isinstance(job.get("id"), str) or not job["id"]:
            raise ValueError("invalid job record")
        if job["id"] in ids:
            raise ValueError("duplicate job id")
        ids.add(job["id"])
        if "links" in job and not isinstance(job["links"], list):
            raise ValueError("invalid job links")
    for key in ("papers", "talentAssessments", "journalRankings", "cloudCandidates", "cloudCandidateArchive"):
        if key in data and not isinstance(data[key], list):
            raise ValueError("invalid list: " + key)
    if "profile" in data and not isinstance(data["profile"], (dict, list)):
        raise ValueError("invalid profile")
    if "cloudUpdate" in data and not isinstance(data["cloudUpdate"], dict):
        raise ValueError("invalid cloudUpdate")


def job_urls(job):
    values = [job.get("announcementUrl"), job.get("recruitmentUrl")]
    values += [link.get("url") if isinstance(link, dict) else link
               for link in job.get("links", [])]
    seen = set()
    for value in values:
        if value and isinstance(value, str):
            try:
                normalized = normalize_url(value)
            except ValueError:
                normalized = value
            if normalized not in seen:
                seen.add(normalized)
                yield normalized


def select_pages(data, now, max_pages):
    previous = data.get("cloudUpdate", {}).get("urlState", {})
    pool = {}
    for job in data["jobs"]:
        deadline = parse_deadline(job)
        urgent = deadline is not None and now <= deadline <= now + timedelta(days=14)
        nearby = "宁波" in str(job.get("region", "")) or "宁波" in str(job.get("organization", ""))
        score = (100 if urgent else 0) + (40 if nearby else 0) + (20 if job.get("priority") == "A" else 0)
        for url in job_urls(job):
            item = pool.setdefault(url, {"url": url, "jobs": [], "province": str(job.get("province") or "未知"),
                                         "score": 0})
            item["jobs"].append(job)
            item["score"] = max(item["score"], score)
    def rank(item):
        old = previous.get(item["url"], {})
        # Unchecked / least recently checked first; priority breaks ties. This
        # keeps urgent and nearby sources prominent without starving provinces.
        return (old.get("checkedAt", ""), -item["score"], item["url"])
    ranked = sorted(pool.values(), key=rank)
    provinces = sorted({item["province"] for item in ranked})
    cursor = int(data.get("cloudUpdate", {}).get("provinceCursor", 0)) if provinces else 0
    provinces = provinces[cursor % len(provinces):] + provinces[:cursor % len(provinces)] if provinces else []
    selected = []
    chosen = set()
    # Reserve half the budget for a round-robin across all represented provinces.
    groups = {province: [item for item in ranked if item["province"] == province] for province in provinces}
    reserve = min(max_pages, max(1, max_pages // 2)) if max_pages else 0
    while len(selected) < reserve and any(groups.values()):
        for province in provinces:
            if groups[province] and len(selected) < reserve:
                item = groups[province].pop(0)
                selected.append(item)
                chosen.add(item["url"])
    for item in ranked:
        if len(selected) >= max_pages:
            break
        if item["url"] not in chosen:
            selected.append(item)
            chosen.add(item["url"])
    return selected, (cursor + max(1, reserve)) % len(provinces) if provinces else 0


def official_seed(job, url):
    """Only same-host links of an explicitly official announcement seed qualify."""
    if not is_official(job):
        return False
    try:
        seed_hosts = {urlsplit(normalize_url(job[key])).hostname for key in ("announcementUrl", "recruitmentUrl")
                      if isinstance(job.get(key), str) and job[key]}
        return urlsplit(normalize_url(url)).hostname in seed_hosts
    except ValueError:
        return False



CANDIDATE_FILTER_VERSION = "title-relevance-v1"
NON_POSITION_TITLE = re.compile(
    r"招聘会|双选会|宣讲会|招聘场地|场地.{0,8}(?:简介|介绍)|费用减免|"
    r"报名.{0,8}(?:费用|指南)|缴费|报考指南|报名指南|申请指南|办事指南|"
    r"拟聘|拟录用|录用公示|录取公示|公示|招聘结果|考试结果|考核结果|"
    r"成绩|录取名单|合格名单|面试名单|体检名单|资格复审名单|"
    r"面试通知|笔试通知|考试通知|常见问题|操作手册|指南|结果", re.I)
UNRELATED_TITLE = re.compile(r"临床|护理|医科|口腔|助产|医学检验|药学|clinical|nursing", re.I)
RELATED_TITLE = re.compile(
    r"岩土|土木|地震|地质|力学|仿真|\\bCAE\\b|工程|能源|交通|港口|水利|"
    r"科研软件|博士后|科研院所|研究所|研究院|研究中心|postdoc|"
    r"geotechn|civil engineering|seismic|geolog|mechanics|simulation", re.I)
GENERIC_TITLES = {
    "招聘", "招聘信息", "招聘公告", "招聘动态", "最新招聘", "最新招聘信息",
    "人才招聘", "人才引进", "校园招聘", "校园招聘信息", "社会招聘",
    "招聘岗位", "招聘职位", "职位列表", "岗位列表", "招聘专区", "招聘专栏",
    "博士后", "博士后招聘", "博士后招聘信息", "博士后工作站", "科研招聘",
    "就业信息", "报名入口", "网上报名", "招聘报名", "招聘报名入口",
    "招聘会", "招聘场地简介", "医科招聘信息", "更多招聘",
    "job", "jobs", "career", "careers", "recruitment", "vacancies",
}


def candidate_filter_reason(title, organizations=()):
    """Conservative discovery filter, not a semantic eligibility assessment."""
    title = " ".join(str(title or "").split())
    compact = re.sub(r"\\s+", "", title).lower()
    generic = re.sub(r"20\\d{2}(?:年|届)?|[【】\\[\\]（）():：·—_-]", "", compact)
    if not title or generic in GENERIC_TITLES:
        return "泛招聘导航或缺少具体主体/方向的短标题"
    if NON_POSITION_TITLE.search(title):
        return "招聘会、指南、场地、缴费、公示或结果等非岗位信息"
    if UNRELATED_TITLE.search(title):
        return "标题明确属于临床、护理、医科等非目标方向"
    if not RECRUITMENT.search(title):
        return "标题缺少招聘或博士后招收信号"
    organization_match = any(isinstance(org, str) and len(org.strip()) >= 4
                             and re.sub(r"\\s+", "", org).lower() in compact
                             for org in organizations)
    if not RELATED_TITLE.search(title) and not organization_match:
        return "标题未提供目标专业、科研方向或原单位名称的关联证据"
    return None



COMPACT_OBSERVATION_FIELDS = ("checkedAt", "httpStatus", "bodyFetched", "textHash",
                              "contentChanged", "error")


def compact_observation(observation):
    return {key: copy.deepcopy(observation[key]) for key in COMPACT_OBSERVATION_FIELDS
            if key in observation}


def compact_existing_monitors(data):
    for job in data["jobs"]:
        monitor = job.get("cloudMonitor")
        if not isinstance(monitor, dict) or not isinstance(monitor.get("sources"), dict):
            continue
        monitor["sources"] = {url: compact_observation(observation)
                              for url, observation in monitor["sources"].items()
                              if isinstance(observation, dict)}


def filter_existing_candidates(data, now):
    by_id = {job["id"]: job for job in data["jobs"]}
    kept = []
    archived = []
    archive = data.setdefault("cloudCandidateArchive", [])
    for candidate in data.get("cloudCandidates", []):
        if not isinstance(candidate, dict) or candidate.get("reviewStatus", "pending") != "pending" or candidate.get("reviewedAt") or candidate.get("reviewedBy"):
            kept.append(candidate)
            continue
        organizations = [candidate.get("organization")]
        organizations += [by_id[job_id].get("organization") for job_id in candidate.get("sourceJobIds", [])
                          if job_id in by_id]
        reason = candidate_filter_reason(candidate.get("title"), organizations)
        if reason:
            # Keep the complete original object, including original timestamps/evidence.
            archive.append({"candidate": copy.deepcopy(candidate), "archivedAt": stamp(now),
                            "reason": reason, "ruleVersion": CANDIDATE_FILTER_VERSION})
            archived.append(candidate.get("id") or candidate.get("url"))
        else:
            kept.append(candidate)
    data["cloudCandidates"] = kept
    return archived


def candidate_from_link(link, base_url, item, known_urls, existing, now):
    if not isinstance(link, dict) or candidate_filter_reason(
            link.get("title"), [job.get("organization") for job in item["jobs"]]):
        return None
    if not any(official_seed(job, item["url"]) for job in item["jobs"]):
        return None
    try:
        url = normalize_url(urljoin(base_url, link.get("url", "")))
        if urlsplit(url).hostname != urlsplit(item["url"]).hostname:
            return None
        # Seed host has passed public resolution; candidates are NOT fetched here.
        host = urlsplit(url).hostname
        try:
            if not public_ip(host):
                return None
        except ValueError:
            pass
    except (ValueError, TypeError):
        return None
    if url in known_urls or url in existing:
        return None
    return {"id": "CLOUD-" + hashlib.sha256(url.encode()).hexdigest()[:16],
            "url": url, "title": " ".join(str(link.get("title", "")).split())[:180],
            "sourceUrl": item["url"], "sourceJobIds": [job["id"] for job in item["jobs"]],
            "discoveredAt": stamp(now), "reviewStatus": "pending",
            "availability": "unknown", "eligibility": "uncertain",
            "eligibility2027": "尚未核验，不代表2027届可投",
            "sourceKind": "same_host_link_from_recorded_official_source",
            "notes": "仅发现链接；未读取候选正文，未确认新发日期、现有名额、岗位资格或单位性质。"}



def attachment_link(link, item, base_url):
    if not isinstance(link, dict):
        return None
    civil = any(any(word in " ".join(str(job.get(key, "")) for key in
                ("route", "routeCategory", "position", "organization")) for word in ("公务员", "选调"))
                and official_seed(job, item["url"]) for job in item["jobs"])
    if not civil:
        return None
    try:
        url = normalize_url(urljoin(base_url, link.get("url", "")))
        if urlsplit(url).hostname != urlsplit(item["url"]).hostname:
            return None
        filename = unquote(urlsplit(url).path.rsplit("/", 1)[-1])
        if Path(filename).suffix.lower() not in (".xls", ".xlsx", ".pdf", ".doc", ".docx", ".zip"):
            return None
        filename = re.sub(r"[^\w.\-]", "_", filename)[-100:]
        if not filename or filename.startswith("."):
            filename = "attachment" + Path(filename).suffix
        return url, filename
    except (ValueError, TypeError):
        return None


def attachment_signature(payload, filename):
    suffix = Path(filename).suffix.lower()
    if suffix == ".pdf":
        return payload[:1024].lstrip().startswith(b"%PDF-")
    if suffix in (".zip", ".docx", ".xlsx"):
        return payload[:4] in tuple(bytes.fromhex(magic) for magic in ("504b0304", "504b0506", "504b0708"))
    if suffix in (".doc", ".xls"):
        return payload.startswith(bytes.fromhex("d0cf11e0a1b11ae1"))
    return False

def write_sqlite(path, data):
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("PRAGMA user_version = 1")
        for table in ("jobs", "links", "paper_policies", "profile_records",
                      "talent_assessments", "journal_rankings", "candidates", "dashboard_snapshot"):
            conn.execute("CREATE TABLE " + table + " (id TEXT PRIMARY KEY, payload_json TEXT NOT NULL)")
        def insert(table, key, payload):
            conn.execute("INSERT INTO " + table + " VALUES (?, ?)",
                         (str(key), json.dumps(payload, ensure_ascii=False, allow_nan=False)))
        for job in data["jobs"]:
            insert("jobs", job["id"], job)
            for index, link in enumerate(job.get("links", [])):
                insert("links", job["id"] + ":link:" + str(index), {"jobId": job["id"], "link": link})
            if job.get("announcementUrl"):
                insert("links", job["id"] + ":announcement", {"jobId": job["id"], "url": job["announcementUrl"], "kind": "announcement"})
            if "paperPolicy" in job:
                insert("paper_policies", "job:" + job["id"], {"jobId": job["id"], "policy": job["paperPolicy"]})
        for index, policy in enumerate(data.get("papers", [])):
            insert("paper_policies", "paper:" + str(index), policy)
        insert("profile_records", "__profile__", data.get("profile", {}))
        profile = data.get("profile", {})
        records = profile.get("records", []) if isinstance(profile, dict) else profile
        if isinstance(records, list):
            for index, record in enumerate(records):
                insert("profile_records", "record:" + str(index), record)
        for key, table in (("talentAssessments", "talent_assessments"),
                           ("journalRankings", "journal_rankings"), ("cloudCandidates", "candidates")):
            for index, record in enumerate(data.get(key, [])):
                insert(table, index, record)
        insert("dashboard_snapshot", "dashboard", data)
        conn.commit()


def stage_text(target, text):
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, path = tempfile.mkstemp(prefix="." + target.name + ".", suffix=".tmp", dir=target.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        Path(path).unlink(missing_ok=True)
        raise
    return Path(path)



def fetch_one(url, fetcher):
    try:
        normalize_url(url)
        result = fetcher(url)
        if not isinstance(result, dict):
            raise ValueError("invalid_fetch_result")
        return result
    except Exception as exc:
        return {"ok": False, "httpStatus": None, "error": type(exc).__name__, "finalUrl": safe_url(url)}


def fetch_pages(items, fetcher, started):
    # Four outstanding requests at most; all data/SQLite mutations remain serial.
    with ThreadPoolExecutor(max_workers=4, thread_name_prefix="public-fetch") as pool:
        for offset in range(0, len(items), 4):
            if time.monotonic() - started >= RUN_TIMEOUT:
                return
            batch = items[offset:offset + 4]
            futures = [pool.submit(fetch_one, item["url"], fetcher) for item in batch]
            for item, future in zip(batch, futures):
                yield item, future.result()

def run_update(data_path, output_path, evidence_dir, database_path, max_pages=80, now=None, fetcher=None, attachment_fetcher=None):
    if not isinstance(max_pages, int) or not 0 <= max_pages <= 500:
        raise ValueError("max_pages must be between 0 and 500")
    now = now or datetime.now(SHANGHAI)
    if now.tzinfo is None:
        raise ValueError("now must include a timezone")
    now = now.astimezone(SHANGHAI)
    raw = Path(data_path).read_bytes()
    if len(raw) > 100 * 1024 * 1024:
        raise ValueError("dashboard too large")
    data = json.loads(raw.decode("utf-8-sig"), parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")))
    validate_data(data)  # Must precede every file mutation, including directory creation.
    original = copy.deepcopy(data)
    compact_existing_monitors(data)
    previous = data.get("cloudUpdate", {})
    states = copy.deepcopy(previous.get("urlState", {}))
    archived_candidates = filter_existing_candidates(data, now)
    candidates = data.setdefault("cloudCandidates", [])
    known_urls = {url for job in data["jobs"] for url in job_urls(job)}
    candidate_urls = {candidate.get("url") for candidate in candidates if isinstance(candidate, dict)}
    expired = []
    for job in data["jobs"]:
        deadline = parse_deadline(job)
        if deadline and known_deadline(job) and now >= deadline and job.get("availability") != "historical":
            job["cloudLifecycle"] = {"evaluatedAt": stamp(now), "deadlineAt": stamp(deadline),
                "previousAvailability": job.get("availability"), "previousStatus": job.get("currentStatus"),
                "scope": "已记录且有官方核验依据的截止时间计算；不确认延期或重新开放。"}
            job["availability"] = "historical"
            job["availabilityCode"] = "historical"
            expired.append(job["id"])
    selected, cursor = select_pages(data, now, max_pages)
    fetcher = fetcher or fetch_public
    attachment_fetcher = attachment_fetcher or (lambda url: fetch_public(url, binary=True))
    attachment_attempts = []
    attachment_files = []
    attachment_urls = set()
    attempts = []
    new_candidates = []
    started = time.monotonic()
    for item, result in fetch_pages(selected, fetcher, started):
        url = item["url"]
        old = states.get(url, {})
        text = result.get("bodyText", "") if isinstance(result.get("bodyText", ""), str) else ""
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest() if result.get("ok") else old.get("textHash")
        changed = bool(result.get("ok") and old.get("textHash") and digest != old["textHash"])
        observation = {"url": safe_url(url), "finalUrl": safe_url(result.get("finalUrl", url)),
            "jobIds": [job["id"] for job in item["jobs"]], "checkedAt": stamp(now),
            "httpStatus": result.get("httpStatus"), "reachable": result.get("httpStatus") is not None,
            "bodyFetched": bool(result.get("ok")), "contentType": str(result.get("contentType", ""))[:200],
            "textHash": digest, "previousTextHash": old.get("textHash"), "contentChanged": changed,
            "diff": {"kind": "changed" if changed else "unchanged" if result.get("ok") and old.get("textHash") else "baseline" if result.get("ok") else "fetch_failed",
                     "previousCharacters": old.get("characters"), "characters": len(text) if result.get("ok") else None},
            "error": re.sub(r"[^a-zA-Z0-9_-]", "", str(result.get("error", "")))[:60] or None,
            "elapsedMs": result.get("elapsedMs"), "scope": "fetch_and_normalized_text_hash_only"}
        observation["redirects"] = [{"url": safe_url(hop.get("url", "")), "httpStatus": hop.get("httpStatus"),
                                      "location": safe_url(hop.get("location", ""))}
                                     for hop in result.get("redirects", []) if isinstance(hop, dict)]
        attempts.append(observation)
        if observation["url"] != "[rejected-url]":
            states[url] = {"checkedAt": stamp(now), "textHash": digest,
                           "characters": len(text) if result.get("ok") else old.get("characters"),
                           "httpStatus": result.get("httpStatus"), "bodyFetched": bool(result.get("ok"))}
        for job in item["jobs"]:
            monitor = job.setdefault("cloudMonitor", {})
            monitor["checkedAt"] = stamp(now)
            monitor["scope"] = "自动抓取，不是人工全文/资格核验"
            monitor.setdefault("sources", {})[observation["url"]] = compact_observation(observation)
        if result.get("ok"):
            for link in result.get("links", [])[:500]:
                candidate = candidate_from_link(link, result.get("finalUrl", url), item, known_urls, candidate_urls, now)
                if candidate:
                    candidates.append(candidate)
                    new_candidates.append(candidate["id"])
                    candidate_urls.add(candidate["url"])
        if result.get("ok") and len(attachment_attempts) < MAX_ATTACHMENTS:
            for link in result.get("links", [])[:500]:
                found = attachment_link(link, item, result.get("finalUrl", url))
                if not found or found[0] in attachment_urls or len(attachment_attempts) >= MAX_ATTACHMENTS:
                    continue
                attachment_url, filename = found
                attachment_urls.add(attachment_url)
                entry = {"url": attachment_url, "sourceUrl": safe_url(url),
                         "checkedAt": stamp(now), "reviewStatus": "original_unreviewed"}
                try:
                    downloaded = attachment_fetcher(attachment_url)
                    entry["httpStatus"] = downloaded.get("httpStatus")
                    entry["finalUrl"] = safe_url(downloaded.get("finalUrl", attachment_url))
                    payload = downloaded.get("dataBytes", b"")
                    if urlsplit(entry["finalUrl"]).hostname != urlsplit(url).hostname:
                        raise ValueError("cross_host_attachment_redirect")
                    if not downloaded.get("ok") or not isinstance(payload, bytes) or not payload or len(payload) > MAX_ATTACHMENT_BYTES:
                        raise ValueError("attachment_fetch_failed_or_size")
                    if not attachment_signature(payload, filename):
                        raise ValueError("attachment_signature_mismatch")
                    digest = hashlib.sha256(payload).hexdigest()
                    relative = Path("attachments") / now.strftime("%Y-%m-%d") / (digest[:12] + "_" + filename)
                    entry.update(sha256=digest, bytes=len(payload), path=relative.as_posix(),
                                 note="官方同域附件原件已归档，未解析或核验资格")
                    attachment_files.append((Path(evidence_dir) / relative, payload))
                    archive = data.setdefault("cloudAttachments", [])
                    if not any(a.get("sha256") == digest and a.get("url") == attachment_url for a in archive if isinstance(a, dict)):
                        archive.append(copy.deepcopy(entry))
                except Exception as exc:
                    entry["error"] = type(exc).__name__
                attachment_attempts.append(entry)
    successes = sum(attempt["bodyFetched"] for attempt in attempts)
    summary = {"success": successes > 0 or (max_pages == 0), "completed": True,
        "updatedAt": stamp(now), "checkedAt": stamp(now), "timezone": "Asia/Shanghai", "checked": len(attempts),
        "successful": successes, "failed": len(attempts) - successes,
        "changed": sum(attempt["contentChanged"] for attempt in attempts),
        "newCandidates": len(new_candidates), "archivedCandidates": len(archived_candidates),
        "candidateFilterVersion": CANDIDATE_FILTER_VERSION, "expired": len(expired),
        "provinceCursor": cursor, "provincesChecked": sorted({item["province"] for item in selected[:len(attempts)]}),
        "limitReached": len(attempts) < len(selected), "maxPages": max_pages,
        "urlState": states, "scope": CAVEAT}
    summary["attachmentsDownloaded"] = len(attachment_files)
    data["cloudUpdate"] = summary
    if isinstance(data.get("stats"), dict):
        data["stats"]["actionable"] = sum(job.get("availability") in ("open", "rolling", "upcoming") for job in data["jobs"])
    # Deliberate invariants: automated monitoring cannot alter semantic conclusions.
    for old, new in zip(original["jobs"], data["jobs"]):
        for key in ("verifiedAt", "eligibility", "eligibilityCode", "eligibility2027", "paperPolicy"):
            if old.get(key) != new.get(key):
                raise AssertionError("semantic verification field changed")
    for key in ("profile", "papers", "talentAssessments", "journalRankings"):
        if original.get(key) != data.get(key):
            raise AssertionError("protected section changed")
    evidence = {"run": {key: value for key, value in summary.items() if key != "urlState"},
                "attempts": attempts, "expiredJobIds": expired, "newCandidateIds": new_candidates,
                "inputSha256": hashlib.sha256(raw).hexdigest(), "attachments": attachment_attempts,
                "archivedCandidateIds": archived_candidates}
    output = Path(output_path)
    database = Path(database_path)
    if output.resolve() == database.resolve() or Path(data_path).resolve() == database.resolve():
        raise ValueError("database and JSON paths must differ")
    evidence_file = Path(evidence_dir) / ("cloud-" + now.strftime("%Y%m%dT%H%M%S%f") + ".json")
    staged = []
    try:
        for target, payload in attachment_files:
            target.parent.mkdir(parents=True, exist_ok=True)
            try:
                with target.open("xb") as handle:
                    handle.write(payload)
            except FileExistsError:
                if hashlib.sha256(target.read_bytes()).hexdigest() != hashlib.sha256(payload).hexdigest():
                    raise ValueError("attachment path collision")
        staged_json = stage_text(output, json_text(data))
        staged.append(staged_json)
        staged_evidence = stage_text(evidence_file, json_text(evidence))
        staged.append(staged_evidence)
        database.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_database = tempfile.mkstemp(prefix="." + database.name + ".", suffix=".tmp", dir=database.parent)
        os.close(fd)
        staged_database = Path(temp_database)
        staged.append(staged_database)
        write_sqlite(staged_database, data)
        with closing(sqlite3.connect(staged_database)) as conn:
            if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ValueError("sqlite integrity failure")
        # Fully stage and validate every artifact before touching existing outputs.
        os.replace(staged_database, database)
        os.replace(staged_evidence, evidence_file)
        os.replace(staged_json, output)
    finally:
        for path in staged:
            path.unlink(missing_ok=True)
    return {key: value for key, value in summary.items() if key != "urlState"}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--evidence-dir", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--max-pages", type=int, default=80)
    args = parser.parse_args(argv)
    try:
        result = run_update(args.data, args.output, args.evidence_dir, args.database, args.max_pages)
    except Exception as exc:
        print(json.dumps({"success": False, "error": type(exc).__name__,
                          "message": "更新未完成；输入/输出错误，请检查数据与路径。"}, ensure_ascii=False), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())