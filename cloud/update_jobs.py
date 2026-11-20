#!/usr/bin/env python3
"""Public-source recruitment ingestion and change monitor.

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
CAVEAT = "自动读取官方招聘正文并直接入库；不确定字段保留未知，不据此认定本人资格、论文等级或人才档次，不刷新原人工核验日期。"


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
    ARTICLE_MARKERS = re.compile(r"v_news_content|wp_articlecontent|trs_editor|article[-_]?content|article[-_]?body|news[-_]?content|content[-_]?detail|zhaopinxx_con[12](?:\\s|$)", re.I)
    VOID = {"br", "hr", "img", "input", "meta", "link", "area", "source", "embed", "wbr"}

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack = []
        self.parts = []
        self.article_parts = []
        self.links = []
        self.anchor = None
        self.title_parts = []
        self.heading_parts = []
        self.headings = []
        self.publication_meta = []

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        values = dict(attrs)
        parent_hidden = self.stack[-1][1] if self.stack else False
        hidden = parent_hidden or tag in ("script", "style", "noscript", "template", "nav", "footer", "aside")
        parent_article = self.stack[-1][2] if self.stack else False
        article = parent_article or tag == "article" or bool(self.ARTICLE_MARKERS.search(
            str(values.get("id", "")) + " " + str(values.get("class", ""))))
        if tag not in self.VOID:
            self.stack.append((tag, hidden, article))
        if tag == "meta":
            name = str(values.get("name", values.get("property", ""))).lower()
            if name in ("pubdate", "publishdate", "publish_date", "article:published_time", "datepublished"):
                self.publication_meta.append("发布日期：" + str(values.get("content", ""))[:40])
        if hidden:
            return
        if tag == "a":
            self.anchor = [values.get("href", ""), []]
        if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            self.heading_parts = []
        if tag in ("p", "div", "br", "li", "tr", "h1", "h2", "h3"):
            self.parts.append("\n")
            if article:
                self.article_parts.append("\n")

    def handle_endtag(self, tag):
        tag = tag.lower()
        hidden = self.stack[-1][1] if self.stack else False
        if tag == "a" and self.anchor:
            if not hidden and len(self.links) < 500:
                self.links.append({"url": self.anchor[0], "title": " ".join("".join(self.anchor[1]).split())[:180]})
            self.anchor = None
        if tag in ("h1", "h2", "h3", "h4", "h5", "h6") and self.heading_parts:
            self.headings.append(" ".join("".join(self.heading_parts).split())[:180])
            self.heading_parts = []
        for index in range(len(self.stack) - 1, -1, -1):
            if self.stack[index][0] == tag:
                del self.stack[index:]
                break
        if tag in ("p", "div", "li", "tr", "h1", "h2", "h3"):
            self.parts.append("\n")
            self.article_parts.append("\n")

    def handle_data(self, data):
        hidden = self.stack[-1][1] if self.stack else False
        if hidden:
            return
        tags = {entry[0] for entry in self.stack}
        if "title" in tags:
            self.title_parts.append(data)
        if tags.intersection(("h1", "h2", "h3", "h4", "h5", "h6")):
            self.heading_parts.append(data)
        self.parts.append(data)
        if self.stack and self.stack[-1][2]:
            self.article_parts.append(data)
        if self.anchor:
            self.anchor[1].append(data)

    @staticmethod
    def clean(parts):
        return "\n".join(" ".join(line.split()) for line in "".join(parts).splitlines() if line.strip())

    @property
    def text(self):
        return self.clean(self.parts)

    @property
    def article_text(self):
        return self.clean(self.article_parts)

    @property
    def title(self):
        return " ".join("".join(self.title_parts).split())

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
                result["articleText"] = parser.article_text
                result["pageTitle"] = parser.title
                result["headings"] = parser.headings
                result["publicationMeta"] = parser.publication_meta
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
    return is_official(job) and (any(job.get(key) for key in
            ("verifiedAt", "windowVerifiedAt", "deadlineVerifiedAt", "windowRevalidatedAt"))
            or (job.get("ingestionMode") == "automatic" and "deadline" in job.get("autoEvidence", {}).get("fields", {})))


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
    for key in ("papers", "talentAssessments", "journalRankings", "cloudCandidates", "cloudCandidateArchive", "cloudJobArchive"):
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



CANDIDATE_FILTER_VERSION = "title-relevance-v2"
NON_POSITION_TITLE = re.compile(
    r"招聘会|双选会|宣讲会|招聘场地|场地.{0,8}(?:简介|介绍)|费用减免|"
    r"报名.{0,8}(?:费用|指南)|缴费|报考指南|报名指南|申请指南|办事指南|"
    r"拟聘|拟录用|录用公示|录取公示|公示|招聘结果|考试结果|考核结果|"
    r"成绩|录取名单|合格名单|面试名单|体检名单|资格复审名单|"
    r"面试通知|笔试通知|考试通知|常见问题|操作手册|指南|结果|联谊会|通过名单|初面|复面|名单", re.I)
UNRELATED_TITLE = re.compile(r"临床|护理|医科|口腔|助产|医学检验|药学|clinical|nursing", re.I)
RELATED_TITLE = re.compile(
    r"岩土|土木|地震|地质|力学|仿真|\bCAE\b|工程|能源|交通|港口|水利|"
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
    compact = re.sub(r"\s+", "", title).lower()
    generic = re.sub(r"20\d{2}(?:年|届)?|[【】\[\]（）():：·—_-]", "", compact)
    if not title or generic in GENERIC_TITLES:
        return "泛招聘导航或缺少具体主体/方向的短标题"
    if POLICY_TITLE.search(title):
        return "政策制度文件，不是具体招聘岗位"
    if NON_POSITION_TITLE.search(title):
        return "招聘会、指南、场地、缴费、公示或结果等非岗位信息"
    if UNRELATED_TITLE.search(title) and not STRONG_RELEVANCE.search(title):
        return "标题明确属于临床、护理、医科等非目标方向"
    if not RECRUITMENT.search(title):
        return "标题缺少招聘或博士后招收信号"
    organization_match = any(isinstance(org, str) and len(org.strip()) >= 4
                             and re.sub(r"\s+", "", org).lower() in compact
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
        if not isinstance(candidate, dict) or candidate.get("reviewStatus", "queued") not in ("pending", "queued", "retry") or candidate.get("reviewedAt") or candidate.get("reviewedBy"):
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
            "discoveredAt": stamp(now), "reviewStatus": "queued",
            "availability": "unknown", "eligibility": "uncertain",
            "eligibility2027": "尚未核验，不代表2027届可投",
            "sourceKind": "same_host_link_from_recorded_official_source",
            "notes": "自动抓取队列；将轮转读取正文并直接入库，不等待人工审核。"}



AUTO_VERSION = "official_body_rules_v3"
STRONG_RELEVANCE = re.compile(r"岩土|土木|地震|地质|力学|仿真|\bCAE\b|水利|水电|地下工程|结构工程|防灾|能源|交通|港口|科研软件|计算物理|数值模拟|有限元|工程|geotechn|seismic|geolog|mechanics|simulation", re.I)
SECONDARY_HOSTS = ("gaoxiaojob.com", "jobui.com", "zhipin.com", "51job.com", "zhaopin.com", "gongzhao.com", "shixiseng.com")
UNKNOWN = "公告未明确/自动提取未取得"
DATE_PATTERN = r"(20\d{2})[年./-](\d{1,2})[月./-](\d{1,2})日?(?:\s*(\d{1,2})[:时](\d{1,2})?分?)?"
DATE_RE = re.compile(DATE_PATTERN)
ORG_SUFFIX = r"(?:有限责任公司|股份有限公司|有限公司|研究院|研究所|设计院|大学|学院|集团|实验室|中心|勘察院|监测站)"
FIELD_BOUNDARY = r"(?=\s*(?:工作地点|招聘单位|用人单位|公司名称|发布时间|发布日期|招聘专业|学历要求|报名时间|报名截止|岗位职责|招聘对象|应聘条件)\s*[：:]|[。；;\n]|$)"
SEMANTIC_FIELDS = ("position", "organization", "region", "province", "education", "requirements",
                   "sourcePublishedAt", "deadline", "deadlineTime", "eligibility2027", "availability",
                   "availabilityCode", "currentStatus", "provinceList", "scope", "route", "routeCategory", "timeWindow", "researchTags")
PROVINCES = {
    "北京": ("北京",), "天津": ("天津",), "上海": ("上海",), "重庆": ("重庆",),
    "浙江": ("浙江", "杭州", "宁波", "绍兴", "温州", "金华", "湖州", "嘉兴", "台州", "舟山", "余姚"),
    "湖北": ("湖北", "武汉", "宜昌"), "四川": ("四川", "成都"), "陕西": ("陕西", "西安"),
    "广东": ("广东", "广州", "深圳", "珠海", "佛山", "东莞"), "江苏": ("江苏", "南京", "苏州", "无锡"),
    "安徽": ("安徽", "合肥"), "山东": ("山东", "济南", "青岛"), "福建": ("福建", "福州", "厦门"),
    "湖南": ("湖南", "长沙"), "江西": ("江西", "南昌"), "河北": ("河北", "石家庄"),
    "河南": ("河南", "郑州"), "山西": ("山西", "太原"), "辽宁": ("辽宁", "沈阳", "大连"),
    "吉林": ("吉林", "长春"), "黑龙江": ("黑龙江", "哈尔滨"), "云南": ("云南", "昆明"),
    "贵州": ("贵州", "贵阳"), "广西": ("广西", "南宁"), "海南": ("海南", "海口"),
    "甘肃": ("甘肃", "兰州"), "青海": ("青海", "西宁"), "宁夏": ("宁夏", "银川"),
    "新疆": ("新疆", "乌鲁木齐"), "内蒙古": ("内蒙古", "呼和浩特"), "西藏": ("西藏", "拉萨"),
}


def canonical_job_url(url):
    normalized = normalize_url(url)
    p = urlsplit(normalized)
    query = "&".join(part for part in p.query.split("&") if part and not re.match(r"(?i)(?:utm_[^=]*|spm|from|ref)=", part))
    return urlunsplit((p.scheme, p.netloc, p.path.rstrip("/") or "/", query, ""))


def source_trust(candidate, data, final_url=None):
    try:
        target = urlsplit(normalize_url(final_url or candidate["url"])).hostname
        source = urlsplit(normalize_url(candidate.get("sourceUrl", candidate["url"]))).hostname
        requested = urlsplit(normalize_url(candidate["url"])).hostname
    except (KeyError, ValueError, TypeError):
        return False
    if any(target == host or target.endswith("." + host) or source == host or source.endswith("." + host)
           for host in SECONDARY_HOSTS):
        return False
    # Trust is re-derived from existing official job source hosts, never sourceKind alone.
    ids = set(candidate.get("sourceJobIds", []))
    for job in data["jobs"]:
        if ids and job["id"] not in ids:
            continue
        if not is_official(job):
            continue
        hosts = set()
        for key in ("announcementUrl", "recruitmentUrl"):
            try:
                hosts.add(urlsplit(normalize_url(job.get(key, ""))).hostname)
            except (ValueError, TypeError):
                pass
        if source in hosts and requested == source and target in hosts:
            return True
    return False



def public_condition_snippet(value):
    value = str(value or "")
    value = re.split(r"(?:联系人|联系电话|联系方式|咨询电话|电子邮箱|联系邮箱|邮箱)\s*[：:]?", value)[0]
    value = re.sub(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", "[联系方式省略]", value, flags=re.I)
    value = re.sub(r"(?<!\d)1[3-9]\d{9}(?!\d)|(?<!\d)0\d{2,3}[-－]\d{7,8}(?!\d)", "[联系方式省略]", value)
    return value.strip(" ：:；;，,")

def evidence_clause(text, start, end):
    left = max(text.rfind(char, 0, start) for char in "。；;\n") + 1
    right = min([index for char in "。；;\n" if (index := text.find(char, end)) >= 0] or [len(text)])
    return public_condition_snippet(text[left:right])[:240]


def labeled_value(text, labels, limit=100):
    pattern = rf"(?:{labels})\s*(?:[：:]|为|在)\s*(.{{1,{limit}}}?){FIELD_BOUNDARY}"
    match = re.search(pattern, text)
    return (public_condition_snippet(match.group(1)), evidence_clause(text, match.start(), match.end())) if match else (None, None)


def parse_date_match(match):
    values = match.groups()
    try:
        return datetime(int(values[0]), int(values[1]), int(values[2]),
                        int(values[3] or 0), int(values[4] or 0), tzinfo=SHANGHAI)
    except (ValueError, TypeError):
        return None


def explicit_dates(text):
    result = {}
    publication = re.search(r"(?:发布日期|发布时间|发布于|发文日期|发布日期时间)\s*[：:]?\s*(" + DATE_PATTERN + r")", text)
    if publication:
        date_match = DATE_RE.search(publication.group())
        value = parse_date_match(date_match)
        if value:
            result["published"] = (value, publication.group()[:160])
    for match in re.finditer(r"(?:报名|网申|简历投递|应聘|申请)(?:截止(?:时间|日期)?|截至|时间|日期|期限)\s*[：:]?\s*([^。；;\n]{1,130})", text):
        clause = match.group()
        if re.search(r"学位|毕业证|入职|学历认证|缴费|体检|资格复审|面试", clause):
            continue
        dates = list(DATE_RE.finditer(clause))
        if not dates:
            continue
        is_deadline = "截止" in clause or "截至" in clause
        is_range = len(dates) >= 2 and re.search(r"至|到|—|~|～", clause[dates[0].end():dates[1].start()])
        immediate = bool(re.search(r"即日起|自公告发布", clause))
        if is_range:
            start = parse_date_match(dates[0])
            end = parse_date_match(dates[-1])
            if start and end and end >= start:
                result["start"] = (start, clause[:240])
                result["deadline"] = (end, clause[:240], bool(dates[-1].group(4)))
        elif is_deadline or (immediate and re.search(r"至|到", clause)):
            end = parse_date_match(dates[-1])
            if end:
                result["deadline"] = (end, clause[:240], bool(dates[-1].group(4)))
                if immediate and "published" in result:
                    result["start"] = result["published"]
        elif re.search(r"开始|起", clause[dates[0].end():]):
            start = parse_date_match(dates[0])
            if start:
                result["start"] = (start, clause[:240])
    return result


POLICY_TITLE = re.compile(r"管理(?:办法|规定|制度|细则)|实施(?:办法|细则)|工作(?:条例|规程|规定)|规章制度|薪酬办法|考核办法|工作流程|管理政策|第[一二三四五六七八九十百\d]+章", re.I)


def is_generic_heading(title):
    compact = re.sub(r"\s+", "", str(title or "")).lower()
    return not compact or compact in GENERIC_TITLES or bool(re.match(r"^第[一二三四五六七八九十百\d]+[章节条]", compact))


def clean_document_title(value):
    value = " ".join(str(value or "").split())
    parts = re.split(r"\s*(?:\|| - |-(?=[\u4e00-\u9fff]))\s*", value)
    if len(parts) > 1 and re.search(ORG_SUFFIX + r"|人事|人力资源|教师工作|就业|招聘网", parts[-1]):
        return "-".join(parts[:-1]).strip()
    return value


def choose_document_title(candidate, result):
    # Document title takes precedence over sidebar H1/H2 and chapter headings.
    page_title = clean_document_title(result.get("pageTitle"))
    if page_title and not is_generic_heading(page_title) and (RECRUITMENT.search(page_title) or POLICY_TITLE.search(page_title)):
        return page_title[:180]
    for heading in result.get("headings", []):
        heading = clean_document_title(heading)
        if not is_generic_heading(heading) and (RECRUITMENT.search(heading) or POLICY_TITLE.search(heading)):
            return heading[:180]
    return clean_document_title(candidate.get("title"))[:180]


def title_cohort_expiry(title, now):
    title = re.sub(r"\s+", "", str(title))
    target_year = now.year + (1 if now.month >= 8 else 0)
    # Explicit graduation cohort is distinct from the publication year.
    for match in re.finditer(r"(20\d{2})(?:年(?:度)?)?(?:届|应届|高校毕业生|毕业生)", title):
        year = int(match.group(1))
        if year < target_year:
            return {"cohortYear": year, "targetYear": target_year, "basis": "explicit_old_graduation_cohort"}
    for match in re.finditer(r"(20\d{2})(?:年(?:度)?)?(春季|春招|秋季|秋招)?(?:校园招聘|校招|招聘)", title):
        year, season = int(match.group(1)), match.group(2)
        old_spring = season in ("春季", "春招") and (year < now.year or (year == now.year and now.month >= 8))
        old_campus = "校园" in match.group() or "校招" in match.group()
        # A current-year label without an explicit graduation cohort or elapsed
        # spring season does not establish expiry (including autumn recruitment).
        expired_campus = old_campus and year < now.year
        if old_spring or expired_campus:
            return {"cohortYear": year, "targetYear": target_year, "basis": "explicit_old_campus_or_spring_cycle"}
    return None


def detail_recency(candidate, now):
    title = str(candidate.get("title", ""))
    years = [int(value) for value in re.findall(r"20\d{2}", title)]
    target = now.year + (1 if now.month >= 8 else 0)
    return (0 if target in years else 1 if now.year in years else 2 if not years else 3,
            -max(years) if years else 0)


def organization_from_title(candidate, result, title, text):
    # Employer suffix on university employment platforms is an explicit identity,
    # whereas the recruitment-title prefix is not an organization name.
    normalized_text = re.sub(r"\s+", "", text)
    for full in (result.get("pageTitle", ""), candidate.get("title", "")):
        segments = re.split(r"\s*(?:\|| - |-(?=[\u4e00-\u9fff]))\s*", str(full))
        if len(segments) > 1:
            for segment in reversed(segments[1:]):
                segment = segment.strip()
                if (re.fullmatch(r"[^，。；:\n]{2,80}" + ORG_SUFFIX, segment)
                    and not RECRUITMENT.search(segment) and not re.search(r"20\d{2}", segment)
                    and (re.search(r"(?:有限公司|有限责任公司|股份公司|集团公司)$", segment)
                         or re.sub(r"\s+", "", segment) in normalized_text)):
                    return segment, str(full)[:240]
    match = re.search(r"^(?:关于)?([^，。；:\n]{2,70}?" + ORG_SUFFIX +
                      r")(?=\s|20\d{2}|招聘|公开|博士后|面向|[\u4e00-\u9fff]{2,4}(?:教授|院士)|$)", title)
    if match and not RECRUITMENT.search(match.group(1)) and not re.search(r"20\d{2}", match.group(1)):
        return match.group(1), title
    return None, None

def recruitment_body(candidate, result):
    article = str(result.get("articleText") or "").strip()
    text = article or str(result.get("bodyText") or "").strip()
    if not article:
        # Truncate recognizable related-news/footer sections in generic pages.
        text = re.split(r"(?:企业其他招聘信息|招聘日历|重要通知|相关文章|相关新闻|推荐阅读|上一条[：:]|下一条[：:]|上一篇[：:]|下一篇[：:]|友情链接)", text)[0]
    title = choose_document_title(candidate, result)
    true_page_title = str(result.get("pageTitle") or "")
    if POLICY_TITLE.search(true_page_title) or POLICY_TITLE.search(title):
        return None, "policy_document", title
    if len(re.findall(r"第[一二三四五六七八九十百\\d]+条", text)) >= 4 and re.search(r"总则|管理办法|本办法|本规定", text):
        return None, "policy_document", title
    lead = title + "\n" + text[:160]
    if NON_POSITION_TITLE.search(title) or re.search(r"联谊会|初面.{0,12}名单|面试.{0,12}名单|拟聘用|拟录用", lead):
        return None, "non_position", title
    if UNRELATED_TITLE.search(lead) and not STRONG_RELEVANCE.search(text):
        return None, "unrelated_medical", title
    if len(text) < 90:
        return None, "incomplete_body", title
    if not RECRUITMENT.search(title + text[:300]):
        return None, "not_recruitment", title
    if not STRONG_RELEVANCE.search(text):
        return None, "unrelated_body", title
    features = re.findall(r"招聘(?:条件|要求|对象|专业|岗位|人数)|岗位职责|任职要求|应聘条件|报名(?:时间|方式|截止)|学历|学位|工作地点|研究方向|现招聘|拟招聘|招收", text)
    if len(set(features)) < 2:
        return None, "not_detail_page", title
    if not article:
        anchor_chars = sum(len(str(link.get("title", ""))) for link in result.get("links", []) if isinstance(link, dict))
        if anchor_chars > len(text) * 0.65:
            return None, "navigation_page", title
    return text[:80000], None, title



def automatic_route(title, text, organization):
    # Classify the advertised role, not the host school or presumed ownership.
    role = title + "\n" + text[:250]
    if re.search(r"博士后|postdoc", title, re.I):
        return "博士后"
    if re.search(r"公务员|选调生|公开招录|事业单位", role):
        return "公务员/事业单位"
    if organization and re.search(r"大学|学院", organization) and re.search(r"教师|教研|专任|教学|教授", role):
        return "高校科研"
    if re.search(r"科研院所|研究所", organization or "") and re.search(r"科研|研究人员|研究岗|研究助理", role):
        return "高校科研"
    if re.search(r"研发|软件|CAE|算法|仿真|制造", title, re.I):
        return "企业研发"
    if re.search(r"工程师|设计师|勘察|设计|监测|工程技术", title):
        return "工程技术"
    return "综合招聘"


def automatic_scope(location):
    return "nearby" if location and re.search(r"宁波|余姚|杭州|绍兴|舟山|台州|嘉兴", location) else "national"

def extract_automatic_job(candidate, result, now):
    text, reason, title = recruitment_body(candidate, result)
    title = public_condition_snippet(title)
    if reason:
        return None, reason
    fields = {}
    def save(key, value, quote):
        if value is not None:
            fields[key] = {"value": value, "quote": public_condition_snippet(quote)[:240], "observedAt": stamp(now)}
        return value

    organization, quote = labeled_value(text, r"招聘单位|用人单位|聘用单位|单位名称|公司名称|招聘机构")
    if organization:
        organization = save("organization", organization, quote)
    else:
        organization, quote = organization_from_title(candidate, result, title, text)
        if organization:
            organization = save("organization", organization, quote)
    location, quote = labeled_value(text, r"工作地点|岗位地点|工作地区|任职地点|工作区域|工作地域")
    save("region", location, quote)
    province_matches = [province for province, cities in PROVINCES.items() if location and any(city in location for city in cities)]
    province = province_matches[0] if len(province_matches) == 1 else "未知"
    if province != "未知":
        save("province", province, quote)
    education, quote = labeled_value(text, r"学历要求|学历学位要求|学历|学位要求|所需学历", 80)
    if education:
        save("education", education, quote)
    else:
        match = re.search(r"(?:申请人|应聘者|应聘人员)?(?:须|应|需|已|即将|具有|获得|取得|要求)[^。；;\n]{0,35}(?:博士|硕士)(?:学位|学历)", text)
        if match:
            education = save("education", match.group(), match.group())
    requirement_clauses = []
    for clause in re.split(r"[。；;\n]", text):
        if re.search(r"招聘专业|所学专业|相关专业|研究方向|任职要求|应聘条件|学历要求|岗位职责", clause):
            requirement_clauses.append(public_condition_snippet(clause)[:240])
    requirements = "；".join(dict.fromkeys(requirement_clauses))[:900] or UNKNOWN
    if requirements != UNKNOWN:
        save("requirements", requirements, requirements)
    eligibility = [public_condition_snippet(clause)[:240] for clause in re.split(r"[。；;\n]", text)
                   if "2027" in clause and re.search(r"毕业|应届|学位|学历|取证|取得", clause)]
    eligibility2027 = "；".join(eligibility[:3]) or "公告未明确2027届资格/自动提取未取得"
    if eligibility:
        save("eligibility2027", eligibility2027, eligibility2027)
    dates_text = text + "\n" + "\n".join(result.get("publicationMeta", []))
    dates = explicit_dates(dates_text)
    published = dates.get("published")
    deadline_entry = dates.get("deadline")
    start = dates.get("start")
    deadline, deadline_time = None, None
    if published:
        save("sourcePublishedAt", published[0].date().isoformat(), published[1])
    if deadline_entry:
        deadline = deadline_entry[0].date().isoformat()
        deadline_time = stamp(deadline_entry[0]) if deadline_entry[2] else None
        save("deadline", deadline, deadline_entry[1])
        if deadline_time:
            save("deadlineTime", deadline_time, deadline_entry[1])
    if start:
        save("applicationStartAt", stamp(start[0]), start[1])
    window = parse_deadline({"deadline": deadline, "deadlineTime": deadline_time})
    availability = "monitoring"
    if window and now >= window:
        availability = "historical"
    elif (published and published[0] > now) or (start and start[0] > now):
        availability = "upcoming"
    elif window and window > now and ((start and start[0] <= now) or (published and published[0] <= now)):
        availability = "open"
    cohort_expiry = title_cohort_expiry(title, now)
    if cohort_expiry:
        availability = "historical"
        save("cohortExpiry", cohort_expiry, title)
    status = {"monitoring": "自动采集：相关招聘正文已入库，开放窗口或部分条件未明确",
              "historical": "自动采集：公告所列报名截止已过，保留历史",
              "upcoming": "自动采集：公告所列发布或报名开始时间尚未到达",
              "open": "自动采集：公告所列报名窗口内，个人资格仍不确定"}[availability]
    if cohort_expiry:
        status = "自动采集：标题明确为旧届或旧季招聘，归为历史；未据此虚构报名截止"
    window_quotes = list(dict.fromkeys(entry[1] for key, entry in dates.items() if key in ("start", "deadline")))
    time_window = "；".join(public_condition_snippet(value) for value in window_quotes) or UNKNOWN
    category = automatic_route(title, text, organization)
    identity = canonical_job_url(candidate["url"])
    source_url = safe_url(result.get("finalUrl", candidate["url"]))
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    job = {"id": "AUTO-" + hashlib.sha256(identity.encode()).hexdigest()[:16],
           "organization": organization or UNKNOWN, "position": title or "相关招聘公告",
           "region": location or UNKNOWN, "province": province, "provinceList": province_matches,
           "route": category, "routeCategory": category, "scope": automatic_scope(location), "timeWindow": time_window,
           "match": "专业相关（自动提取）", "priority": "B", "researchTags": list(dict.fromkeys(STRONG_RELEVANCE.findall(text)))[:12], "education": education or UNKNOWN,
           "requirements": requirements, "eligibility2027": eligibility2027,
           "eligibility": "uncertain", "eligibilityCode": "uncertain",
           "availability": availability, "availabilityCode": availability, "currentStatus": status,
           "sourcePublishedAt": published[0].date().isoformat() if published else UNKNOWN,
           "deadline": deadline, "deadlineTime": deadline_time,
           "announcementUrl": source_url, "recruitmentUrl": source_url,
           "links": [{"label": "自动抓取的招聘正文", "url": source_url}],
           "sourceTrust": "official_platform" if urlsplit(source_url).hostname.endswith(".edu.cn") else "official",
           "sourceLevel": "已知官方/高校就业来源招聘正文（机器提取）",
           "employerNature": {"code": "unknown", "label": "性质未知",
                              "evidence": "未从来源归属推定招聘雇主所有制。", "sourceUrl": source_url, "verifiedAt": None},
           "verifiedAt": "", "sourceCheckedAt": stamp(now), "autoCollectedAt": stamp(now),
           "ingestionMode": "automatic", "ingestionMethod": AUTO_VERSION,
           "notes": "自动入库，无人工审核门槛；未知字段持续自动监测，不代表本人资格或人才档次已达标。",
           "fitReason": "正文出现目标专业或相关工程科研方向；匹配由规则提取。",
           "risks": "机器抽取可能遗漏附件和复杂限制；学历、毕业日期、论文门槛分别保留原文信息或未知。",
           "paperPolicy": {"policyStatus": "公告未明确/自动提取未取得",
                           "gradingRule": "未自动认定论文分区、数量或人才等级。",
                           "authorshipLimits": "自动提取未取得完整作者位次口径。", "sourceUrl": source_url, "verifiedAt": ""},
           "autoEvidence": {"url": source_url, "discoverySourceUrl": safe_url(candidate.get("sourceUrl", source_url)),
                            "sourceJobIds": candidate.get("sourceJobIds", []), "textHash": digest,
                            "extractionVersion": AUTO_VERSION, "fields": fields,
                            "uncertainties": [key for key in ("organization", "region", "education", "sourcePublishedAt", "deadline", "eligibility2027") if key not in fields]}}
    job["autoEvidence"]["identityHash"] = hashlib.sha256(
        json.dumps({"title": title, "organization": organization, "requirements": requirements,
                    "published": job["sourcePublishedAt"], "deadline": deadline, "region": location, "education": education}, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
    return job, None


def automatic_detail_phase(data, now, fetcher, max_detail_pages, cache, previous, started, original_jobs):
    detail_state = copy.deepcopy(previous.get("detailState", {}))
    queue_items = []
    seen_urls = set()
    by_id = {job["id"]: job for job in data["jobs"]}
    for candidate in data.get("cloudCandidates", []):
        if not isinstance(candidate, dict) or candidate.get("reviewStatus") in ("imported", "rejected", "archived"):
            continue
        try:
            url = canonical_job_url(candidate.get("url", ""))
        except (ValueError, TypeError):
            continue
        if url not in seen_urls:
            seen_urls.add(url)
            candidate["reviewStatus"] = "queued" if candidate.get("reviewStatus") == "pending" else candidate.get("reviewStatus", "queued")
            queue_items.append({"url": url, "candidate": candidate})
    for job in data["jobs"]:
        if job.get("ingestionMode") != "automatic":
            continue
        try:
            url = canonical_job_url(job["announcementUrl"])
        except (KeyError, ValueError):
            continue
        if url not in seen_urls:
            seen_urls.add(url)
            ev = job.get("autoEvidence", {})
            queue_items.append({"url": url, "candidate": {
                "id": job["id"], "url": url, "title": job["position"], "jobId": job["id"],
                "sourceUrl": ev.get("discoverySourceUrl", url), "sourceJobIds": ev.get("sourceJobIds", [])}})
    def detail_priority(item):
        prior = detail_state.get(item["url"], {})
        existing = by_id.get(item["candidate"].get("jobId"))
        old_version = existing.get("autoEvidence", {}).get("extractionVersion") if existing else None
        migration = existing and old_version != AUTO_VERSION and (
            prior.get("attemptedVersion") != AUTO_VERSION or prior.get("attemptedFromVersion") != old_version)
        return (0 if migration else 1, prior.get("attemptedAt", ""),
                str(item["candidate"].get("discoveredAt", ""))[:10],
                detail_recency(item["candidate"], now), item["url"])
    queue_items.sort(key=detail_priority)
    selected = queue_items[:max_detail_pages]
    attempts, added, updated, archived, archived_jobs = [], [], [], [], []
    permanent = {"non_position", "unrelated_medical", "unrelated_body", "not_recruitment", "not_detail_page", "navigation_page", "policy_document"}
    for item in selected:
        if time.monotonic() - started >= RUN_TIMEOUT:
            break
        candidate, url = item["candidate"], item["url"]
        observation = {"url": safe_url(url), "candidateId": candidate.get("id"), "attemptedAt": stamp(now),
                       "httpStatus": None, "success": False}
        detail_state[url] = {"attemptedAt": stamp(now), "attemptedVersion": AUTO_VERSION,
                             "attemptedFromVersion": by_id.get(candidate.get("jobId"), {}).get("autoEvidence", {}).get("extractionVersion")}
        existing = by_id.get(candidate.get("jobId"))
        if not source_trust(candidate, data):
            observation["error"] = "untrusted_origin"
            candidate["reviewStatus"] = "retry"
            candidate["lastDetailError"] = "非可信官方来源；继续从已知官方来源自动发现，未作为官方岗位入库"
            if not existing:
                data.setdefault("cloudCandidateArchive", []).append({
                    "candidate": copy.deepcopy(candidate), "archivedAt": stamp(now),
                    "reason": "非可信官方来源，不以sourceKind标记作为官方证据", "ruleVersion": AUTO_VERSION})
                candidate["reviewStatus"] = "archived"
                archived.append(candidate.get("id"))
            attempts.append(observation)
            continue
        result = cache.get(url) or fetch_one(url, fetcher)
        observation["httpStatus"] = result.get("httpStatus")
        if not result.get("ok") or result.get("httpStatus") != 200 or not source_trust(candidate, data, result.get("finalUrl", url)):
            observation["error"] = "fetch_failed_or_untrusted_redirect"
            candidate["reviewStatus"] = "retry" if not existing else "imported"
            candidate["lastDetailAttemptAt"] = stamp(now)
            candidate["lastDetailError"] = observation["error"]
            attempts.append(observation)
            continue
        new, reason = extract_automatic_job(candidate, result, now)
        if reason:
            observation["error"] = reason
            candidate["lastDetailAttemptAt"] = stamp(now)
            if reason in permanent and existing and existing.get("ingestionMode") == "automatic":
                job_id = existing["id"]
                original_job = copy.deepcopy(original_jobs.get(job_id, existing))
                old_news = [copy.deepcopy(event) for event in data.get("updates", [])
                            if event.get("jobId") == job_id and
                            (event.get("ingestionMode") == "automatic" or str(event.get("type", "")).startswith("自动"))]
                data.setdefault("cloudJobArchive", []).append({
                    "job": original_job, "updates": old_news, "archivedAt": stamp(now),
                    "reason": reason, "ruleVersion": AUTO_VERSION,
                    "evidence": {"url": safe_url(result.get("finalUrl", url)),
                                 "pageTitle": public_condition_snippet(result.get("pageTitle", ""))[:240],
                                 "textHash": hashlib.sha256(str(result.get("articleText") or result.get("bodyText") or "").encode()).hexdigest()}})
                data["jobs"] = [job for job in data["jobs"] if job["id"] != job_id]
                data["updates"] = [event for event in data.get("updates", []) if not (
                    event.get("jobId") == job_id and (event.get("ingestionMode") == "automatic" or str(event.get("type", "")).startswith("自动")))]
                by_id.pop(job_id, None)
                archived_jobs.append(job_id)
                for old_candidate in data.get("cloudCandidates", []):
                    if isinstance(old_candidate, dict) and old_candidate.get("jobId") == job_id:
                        data.setdefault("cloudCandidateArchive", []).append({
                            "candidate": copy.deepcopy(old_candidate), "archivedAt": stamp(now),
                            "reason": reason, "ruleVersion": AUTO_VERSION})
                        old_candidate["reviewStatus"] = "archived"
                        archived.append(old_candidate.get("id"))
                candidate["reviewStatus"] = "archived"
                observation.update(success=True, disposition="job_archived", jobId=job_id)
            elif reason in permanent and not existing:
                data.setdefault("cloudCandidateArchive", []).append({"candidate": copy.deepcopy(candidate),
                    "archivedAt": stamp(now), "reason": reason, "ruleVersion": AUTO_VERSION})
                candidate["reviewStatus"] = "archived"
                archived.append(candidate.get("id"))
            else:
                candidate["reviewStatus"] = "retry" if not existing else "imported"
            attempts.append(observation)
            continue
        observation.update(success=True, textHash=new["autoEvidence"]["textHash"])
        detail_state[url]["successfulAt"] = stamp(now)
        # URL and source-text identity dedupe; never overwrite a manually maintained job.
        for job in data["jobs"]:
            urls = set()
            for known in job_urls(job):
                try:
                    urls.add(canonical_job_url(known))
                except ValueError:
                    pass
            same_body = job.get("autoEvidence", {}).get("identityHash") == new["autoEvidence"]["identityHash"]
            same_hash = job.get("autoEvidence", {}).get("textHash") == new["autoEvidence"]["textHash"]
            if url in urls or canonical_job_url(new["announcementUrl"]) in urls or same_body or same_hash:
                existing = job
                break
        if existing:
            if existing.get("ingestionMode") == "automatic":
                changes = {}
                was_historical = existing.get("availability") == "historical"
                earlier_evidence = copy.deepcopy(existing.get("autoEvidence", {}))
                migrating = earlier_evidence.get("extractionVersion") != AUTO_VERSION
                explicit = new["autoEvidence"]["fields"]
                prior_cohort = earlier_evidence.get("fields", {}).get("cohortExpiry", {})
                prior_cohort_value = prior_cohort.get("value", {}) if isinstance(prior_cohort, dict) else {}
                prior_deadline = parse_deadline(existing)
                current_deadline = parse_deadline(new)
                correct_v2_false_expiry = (
                    was_historical and earlier_evidence.get("extractionVersion") == "official_body_rules_v2"
                    and isinstance(prior_cohort_value, dict)
                    and prior_cohort_value.get("basis") == "explicit_old_campus_or_spring_cycle"
                    and prior_cohort_value.get("cohortYear") == now.year
                    and not re.search(r"届|春季|春招", new.get("position", ""))
                    and not title_cohort_expiry(new.get("position", ""), now)
                    and not (prior_deadline and now >= prior_deadline)
                    and not (current_deadline and now >= current_deadline)
                    and new.get("availability") != "historical")
                if correct_v2_false_expiry:
                    new["availability"] = new["availabilityCode"] = "monitoring"
                    new["currentStatus"] = "自动纠错：旧版仅按当前年份误判到期，已恢复监测；开放窗口仍按公告证据判断"
                    new["autoEvidence"]["algorithmCorrection"] = {
                        "kind": "v2_current_year_campus_false_expiry", "correctedAt": stamp(now),
                        "previousCohortEvidence": copy.deepcopy(prior_cohort)}
                for key in SEMANTIC_FIELDS:
                    if key in ("organization", "region", "province", "education", "requirements", "sourcePublishedAt",
                               "deadline", "deadlineTime", "eligibility2027") and key not in explicit and not migrating:
                        continue
                    if key in ("provinceList", "scope") and "region" not in explicit and not migrating:
                        continue
                    if key == "timeWindow" and new.get(key) == UNKNOWN and not migrating:
                        continue
                    if (key in ("availability", "availabilityCode", "currentStatus")
                            and was_historical and not correct_v2_false_expiry and new.get("availability") != "historical"):
                        continue
                    if existing.get(key) != new.get(key):
                        changes[key] = {"before": existing.get(key), "after": new.get(key)}
                        existing[key] = new.get(key)
                for key, value in earlier_evidence.get("fields", {}).items():
                    if key not in explicit and isinstance(value, dict) and not migrating:
                        retained = copy.deepcopy(value)
                        retained["retainedFromEarlier"] = True
                        retained.setdefault("observedAt", existing.get("sourceCheckedAt"))
                        retained.setdefault("sourceUrl", earlier_evidence.get("url"))
                        new["autoEvidence"]["fields"][key] = retained
                existing["sourceCheckedAt"] = stamp(now)
                existing["ingestionMethod"] = AUTO_VERSION
                existing["autoEvidence"] = new["autoEvidence"]
                if changes:
                    existing.setdefault("autoHistory", []).append({"at": stamp(now), "changes": changes,
                                                                  "sourceUrl": new["announcementUrl"], "previousEvidence": earlier_evidence})
                    updated.append(existing["id"])
                    observation["changes"] = changes
            job_id = existing["id"]
        else:
            data["jobs"].append(new)
            by_id[new["id"]] = new
            added.append(new["id"])
            job_id = new["id"]
        candidate.update(reviewStatus="imported", jobId=job_id, importedAt=stamp(now),
                         sourceCheckedAt=stamp(now), lastDetailAttemptAt=stamp(now))
        candidate.pop("lastDetailError", None)
        observation["jobId"] = job_id
        attempts.append(observation)
    data["cloudCandidates"] = [candidate for candidate in data.get("cloudCandidates", [])
                               if not isinstance(candidate, dict) or candidate.get("reviewStatus") != "archived"]
    pending = sum(isinstance(candidate, dict) and candidate.get("reviewStatus") != "imported"
                  for candidate in data.get("cloudCandidates", []))
    return {"attempts": attempts, "added": list(dict.fromkeys(added)), "updated": list(dict.fromkeys(updated)),
            "archived": archived, "archivedJobs": list(dict.fromkeys(archived_jobs)),
            "pending": pending, "state": detail_state}


def refresh_stats(data, auto_added=0, auto_archived=0):
    stats = data.setdefault("stats", {})
    jobs = data["jobs"]
    provinces = set()
    for job in jobs:
        values = job.get("provinceList", [])
        if not isinstance(values, list):
            values = []
        if not values and job.get("province") in PROVINCES:
            values = [job["province"]]
        provinces.update(value for value in values if isinstance(value, str) and value in PROVINCES)
    stats.update(jobs=len(jobs), actionable=sum(job.get("availability") in ("open", "rolling", "upcoming") for job in jobs),
                 organizations=len({job.get("organization") for job in jobs if job.get("organization") and job["organization"] != UNKNOWN}),
                 provinces=len(provinces), priorityA=sum(job.get("priority") == "A" for job in jobs),
                 nearby=sum(job.get("scope") == "nearby" for job in jobs))
    if isinstance(stats.get("nationalAdded"), int):
        stats["nationalAdded"] = max(0, stats["nationalAdded"] + auto_added - auto_archived)
    elif auto_added:
        stats["nationalAdded"] = auto_added


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
                      "talent_assessments", "journal_rankings", "candidates", "job_archive", "dashboard_snapshot"):
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
                           ("journalRankings", "journal_rankings"), ("cloudCandidates", "candidates"), ("cloudJobArchive", "job_archive")):
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

def run_update(data_path, output_path, evidence_dir, database_path, max_pages=80, now=None, fetcher=None, attachment_fetcher=None, max_detail_pages=30, detail_fetcher=None):
    if not isinstance(max_pages, int) or not 0 <= max_pages <= 500:
        raise ValueError("max_pages must be between 0 and 500")
    if not isinstance(max_detail_pages, int) or not 0 <= max_detail_pages <= 100:
        raise ValueError("max_detail_pages must be between 0 and 100")
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
    candidate_urls.update(entry.get("candidate", {}).get("url") for entry in data.get("cloudCandidateArchive", [])
                          if isinstance(entry, dict) and isinstance(entry.get("candidate"), dict))
    for entry in data.get("cloudJobArchive", []):
        if isinstance(entry, dict) and isinstance(entry.get("job"), dict):
            candidate_urls.update(job_urls(entry["job"]))
    expired = []
    for job in data["jobs"]:
        deadline = parse_deadline(job)
        if deadline and known_deadline(job) and now >= deadline and job.get("availability") != "historical":
            job["cloudLifecycle"] = {"evaluatedAt": stamp(now), "deadlineAt": stamp(deadline),
                "previousAvailability": job.get("availability"), "previousStatus": job.get("currentStatus"),
                "scope": "已记录且有官方核验依据的截止时间计算；不确认延期或重新开放。"}
            job["availability"] = "historical"
            job["availabilityCode"] = "historical"
            if job.get("ingestionMode") == "automatic":
                job["currentStatus"] = "自动采集：公告所列报名截止已过，保留历史"
            expired.append(job["id"])
    selected, cursor = select_pages(data, now, max_pages)
    fetcher = fetcher or fetch_public
    detail_fetcher = detail_fetcher or fetcher
    detail_cache = {}
    attachment_fetcher = attachment_fetcher or (lambda url: fetch_public(url, binary=True))
    attachment_attempts = []
    attachment_files = []
    attachment_urls = set()
    attempts = []
    new_candidates = []
    started = time.monotonic()
    for item, result in fetch_pages(selected, fetcher, started):
        url = item["url"]
        try:
            detail_cache[canonical_job_url(url)] = result
        except ValueError:
            pass
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
    details = automatic_detail_phase(data, now, detail_fetcher, max_detail_pages, detail_cache, previous, started,
                                     {job["id"]: job for job in original["jobs"]})
    archived_candidates.extend(details["archived"])
    updates = data.setdefault("updates", [])
    indexed = {job["id"]: job for job in data["jobs"]}
    indexed.update({entry["job"]["id"]: entry["job"] for entry in data.get("cloudJobArchive", []) if isinstance(entry.get("job"), dict)})
    for entry in data.get("cloudJobArchive", []):
        job = entry.get("job", {})
        if job.get("id") in details["archivedJobs"]:
            updates.insert(0, {"date": now.date().isoformat(), "type": "自动纠错归档",
                              "target": entry.get("evidence", {}).get("pageTitle") or job.get("position", ""),
                              "priority": "B", "jobId": job["id"],
                              "summary": "原自动记录已识别为非招聘岗位，移出岗位库；完整原记录与自动新闻保存在cloudJobArchive。",
                              "url": job.get("announcementUrl", ""), "ingestionMode": "automatic"})
    for kind, ids in (("自动新增", details["added"]), ("自动更新", details["updated"]), ("自动归档", expired)):
        for job_id in ids:
            job = indexed[job_id]
            updates.insert(0, {"date": now.date().isoformat(), "type": kind,
                              "target": job.get("organization", "") + "｜" + job.get("position", ""),
                              "priority": job.get("priority", "B"), "jobId": job_id,
                              "summary": job.get("currentStatus", "") if kind != "自动归档" else "已过公告所列截止；自动归档保留历史。",
                              "url": job.get("announcementUrl", ""), "ingestionMode": "automatic"})
    detail_successes = sum(item["success"] for item in details["attempts"])
    successes = sum(attempt["bodyFetched"] for attempt in attempts)
    summary = {"success": bool(successes or detail_successes or (not attempts and not details["attempts"])), "completed": True,
        "updatedAt": stamp(now), "checkedAt": stamp(now), "timezone": "Asia/Shanghai", "checked": len(attempts),
        "successful": successes, "failed": len(attempts) - successes,
        "changed": sum(attempt["contentChanged"] for attempt in attempts),
        "newCandidates": len(new_candidates), "archivedCandidates": len(archived_candidates),
        "candidateFilterVersion": CANDIDATE_FILTER_VERSION, "expired": len(expired),
        "provinceCursor": cursor, "provincesChecked": sorted({item["province"] for item in selected[:len(attempts)]}),
        "limitReached": len(attempts) < len(selected), "maxPages": max_pages,
        "urlState": states, "scope": CAVEAT, "maxDetailPages": max_detail_pages,
        "autoAdded": len(details["added"]), "autoUpdated": len(details["updated"]), "autoArchived": len(details["archivedJobs"]),
        "pendingDetails": details["pending"], "detailChecked": len(details["attempts"]),
        "detailSuccessful": detail_successes, "detailState": details["state"]}
    summary["attachmentsDownloaded"] = len(attachment_files)
    data["cloudUpdate"] = summary
    refresh_stats(data, len(details["added"]), len(details["archivedJobs"]))
    # Deliberate invariants: automated monitoring cannot alter semantic conclusions.
    new_jobs_by_id = {job["id"]: job for job in data["jobs"]}
    for old in original["jobs"]:
        new = new_jobs_by_id.get(old["id"])
        if new is None:
            if old.get("ingestionMode") != "automatic" or not any(
                entry.get("job") == old and entry.get("ruleVersion") == AUTO_VERSION
                for entry in data.get("cloudJobArchive", [])):
                raise AssertionError("job removed without full automatic archive")
            continue
        for key in ("verifiedAt", "eligibility", "eligibilityCode", "eligibility2027", "paperPolicy"):
            if key == "eligibility2027" and old.get("ingestionMode") == "automatic":
                continue
            if old.get(key) != new.get(key):
                raise AssertionError("semantic verification field changed")
    for key in ("profile", "papers", "talentAssessments", "journalRankings"):
        if original.get(key) != data.get(key):
            raise AssertionError("protected section changed")
    evidence = {"run": {key: value for key, value in summary.items() if key not in ("urlState", "detailState")},
                "attempts": attempts, "expiredJobIds": expired, "newCandidateIds": new_candidates,
                "inputSha256": hashlib.sha256(raw).hexdigest(), "attachments": attachment_attempts,
                "archivedCandidateIds": archived_candidates, "detailAttempts": details["attempts"],
                "autoAddedJobIds": details["added"], "autoUpdatedJobIds": details["updated"], "autoArchivedJobIds": details["archivedJobs"]}
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
    return {key: value for key, value in summary.items() if key not in ("urlState", "detailState")}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--evidence-dir", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--max-pages", type=int, default=80)
    parser.add_argument("--max-detail-pages", type=int, default=30)
    args = parser.parse_args(argv)
    try:
        result = run_update(args.data, args.output, args.evidence_dir, args.database, args.max_pages, max_detail_pages=args.max_detail_pages)
    except Exception as exc:
        print(json.dumps({"success": False, "error": type(exc).__name__,
                          "message": "更新未完成；输入/输出错误，请检查数据与路径。"}, ensure_ascii=False), file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())