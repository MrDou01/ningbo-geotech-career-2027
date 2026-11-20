"""Regression tests use only temporary files and fake network responses."""
import copy
from contextlib import closing
from datetime import datetime, timezone
import importlib.util
import io
from contextlib import redirect_stdout
from unittest.mock import patch
import json
from pathlib import Path
import socket
import sqlite3
import tempfile
import unittest

MODULE = Path(__file__).resolve().parents[1] / "update_jobs.py"
SPEC = importlib.util.spec_from_file_location("update_jobs", MODULE)
monitor = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(monitor)


def sample():
    return {
        "meta": {"generatedAt": "2026-10-29", "custom": {"keep": True}},
        "stats": {"actionable": 1, "custom": 42},
        "jobs": [{
            "id": "NI001", "organization": "某工程院", "province": "湖北",
            "region": "武汉", "priority": "A", "sourceTrust": "official",
            "announcementUrl": "https://example.edu.cn/jobs/1.html",
            "links": [{"label": "原公告", "url": "https://example.edu.cn/jobs/1.html"}],
            "deadline": "2026-11-14", "deadlineTime": "17:00",
            "availability": "open", "eligibility": "uncertain",
            "eligibilityCode": "uncertain", "verifiedAt": "2026-10-29",
            "currentStatus": "原核验结论",
            "paperPolicy": {"authorshipLimits": "未公开", "verifiedAt": "2026-10-29"},
        }],
        "profile": {"publishedSciCount": 4, "records": [{"id": "R1", "text": "保留"}]},
        "papers": [{"id": "P1", "unknownField": [1, 2]}],
        "talentAssessments": [{"id": "T1", "status": "partial"}],
        "journalRankings": [{"journal": "SoftwareX", "cas": "待核"}],
        "civil": [{"id": "C1", "custom": "untouched"}],
        "updates": [{"date": "2026-10-29"}],
        "customFutureField": {"doNotDrop": [1, 2, 3]},
    }


def fake_fetch(url, text="某工程院招聘信息 2027 届，完整条件待人工核验。", links=None):
    return {"url": url, "finalUrl": url, "httpStatus": 200, "ok": True,
            "bodyText": text, "links": links or [], "contentType": "text/html"}


DETAIL_TITLE = "某地质研究院2027岩土工程博士招聘公告"
DETAIL_BODY = """某地质研究院2027岩土工程博士招聘公告。
招聘单位：某地质研究院。
招聘岗位：岩土工程科研博士。
工作地点：武汉市。
发布日期：2026年11月20日。
学历要求：博士研究生。
招聘专业：岩土工程、工程力学。
招聘对象：2027届毕业生。
应聘者须于2027年7月31日前取得博士学历学位证书。
报名时间：2026年11月20日至2026年12月10日17:00。
报名截止时间：2026年12月10日17:00。
研究工作围绕地震灾害、土体动力响应和数值仿真开展，要求具有独立研究能力及良好合作意识。
本次招聘采用网上报名，应聘人员提交真实材料，经单位考核后择优录用。
岗位的具体任务由研究团队结合工程项目安排，待遇根据单位有关规定执行。"""
DETAIL_NOW = datetime(2026, 11, 21, 1, tzinfo=timezone.utc)


def detail_response(url, body=DETAIL_BODY, title=DETAIL_TITLE, page_body=None):
    return {"url": url, "finalUrl": url, "httpStatus": 200, "ok": True,
            "bodyText": page_body if page_body is not None else body,
            "articleText": body, "pageTitle": title, "headings": [title],
            "links": [], "contentType": "text/html"}


# Reduced real template from hr.cumt.edu.cn: section headings are h1, article
# titles h5, body v_news_content. Bodies below are paraphrased test text; no contacts.
CUMT_POLICY_TITLE = "《中国矿业大学博士后管理办法》（中矿大人字〔2026〕3号）"
CUMT_RECRUIT_TITLE = "中国矿业大学范钢伟教授团队博士后招聘启事"
CUMT_POLICY_HTML = (
    "<html><head><title>" + CUMT_POLICY_TITLE + "-中国矿业大学 人力资源部</title></head><body>"
    "<h1>学校政策</h1><h1>规章制度</h1><h5>" + CUMT_POLICY_TITLE + "</h5>"
    "<span>时间：2026-06-16</span><div class='v_news_content'>"
    "<h2>第一章 总则</h2><p>本管理制度规定博士后培养、服务、考核和经费管理的通用要求。"
    "学校的能源资源、土木工程、地质和力学研究平台按本办法组织管理工作。</p>"
    "<h2>第三章 博士后的招收</h2><p>博士后申请应具备博士学位，相关管理机构负责审核申请材料。"
    "招收标准、办理程序、在站考核与工作职责按照各章规定执行。"
    "该文件是一项学校人事管理制度，不是具体团队岗位的招聘公告。</p></div></body></html>")
CUMT_RECRUIT_HTML = (
    "<html><head><title>" + CUMT_RECRUIT_TITLE + "-中国矿业大学 人力资源部</title></head><body>"
    "<h1>博士后</h1><h1>招聘信息</h1><h5>" + CUMT_RECRUIT_TITLE + "</h5>"
    "<span>时间：2026-01-22</span><div class='v_news_content'>"
    "<p>团队围绕矿井水利用开展研究，本次招募二至四位博士后。</p>"
    "<p>研究方向包括矿井水处理和电解水制氢。申请人应已获或即将获得环境、水文、材料、"
    "化工或能源相关博士学位，具备对应研究基础和独立科研能力。</p>"
    "<p>应聘条件另要求近三年的相关SCI成果，按第一或通讯作者和JCR分区认定。"
    "具体岗位职责包括参与研究项目、独立开展研究、成果总结以及学术合作。</p>"
    "</div></body></html>")


def html_fixture_response(url, html):
    parser = monitor.VisibleHTML()
    parser.feed(html)
    return {"url": url, "finalUrl": url, "httpStatus": 200, "ok": True,
            "bodyText": parser.text, "articleText": parser.article_text,
            "pageTitle": parser.title, "headings": parser.headings,
            "links": parser.links, "contentType": "text/html"}


class UpdateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.path = self.root / "dashboard.json"
        self.db = self.root / "jobs.sqlite"
        self.evidence = self.root / "evidence"
        self.data = sample()
        self.path.write_text(json.dumps(self.data, ensure_ascii=False), encoding="utf-8")

    def tearDown(self):
        self.temp.cleanup()

    def run_monitor(self, now, fetcher=fake_fetch, max_pages=80, max_detail_pages=0):
        return monitor.run_update(self.path, self.path, self.evidence, self.db,
                                  max_pages=max_pages, now=now, fetcher=fetcher,
                                  max_detail_pages=max_detail_pages)

    def read(self):
        return json.loads(self.path.read_text(encoding="utf-8"))

    def test_deadline_shanghai_time_and_date_only(self):
        deadline = monitor.parse_deadline(self.data["jobs"][0])
        self.assertEqual(deadline.astimezone(timezone.utc).hour, 9)
        self.assertEqual(deadline.minute, 0)
        date_only = monitor.parse_deadline({"deadline": "2026-11-14"})
        self.assertEqual((date_only.hour, date_only.minute, date_only.second), (23, 59, 59))
        self.assertIsNone(monitor.parse_deadline({"deadline": "2026-02-30"}))
        self.assertIsNone(monitor.parse_deadline({"deadline": "2026-11-14", "deadlineTime": "待核"}))

    def test_expiry_preserves_verification_and_semantics(self):
        self.run_monitor(datetime(2026, 11, 14, 9, 1, tzinfo=timezone.utc))
        new = self.read()
        job = new["jobs"][0]
        self.assertEqual(job["availability"], "historical")
        self.assertEqual(job["availabilityCode"], "historical")
        self.assertEqual(job["verifiedAt"], "2026-10-29")
        self.assertEqual(job["eligibility"], "uncertain")
        self.assertEqual(job["currentStatus"], "原核验结论")
        self.assertEqual(job["paperPolicy"], self.data["jobs"][0]["paperPolicy"])
        self.assertEqual(new["stats"]["actionable"], 0)

    def test_iso_deadline_time_is_used_before_date(self):
        self.data["jobs"][0]["deadlineTime"] = "2026-11-14T17:00:00+08:00"
        self.path.write_text(json.dumps(self.data), encoding="utf-8")
        parsed = monitor.parse_deadline(self.data["jobs"][0])
        self.assertEqual(parsed.astimezone(timezone.utc).hour, 9)
        self.run_monitor(datetime(2026, 11, 14, 9, 0, tzinfo=timezone.utc))
        self.assertEqual(self.read()["jobs"][0]["availability"], "historical")
        self.assertEqual(self.read()["jobs"][0]["verifiedAt"], "2026-10-29")

    def test_zip_signature_is_real_binary_not_literal_backslashes(self):
        self.assertTrue(monitor.attachment_signature(bytes.fromhex("504b0304") + b"fixture", "a.xlsx"))
        self.assertTrue(monitor.attachment_signature(bytes.fromhex("d0cf11e0a1b11ae1"), "a.xls"))
        self.assertFalse(monitor.attachment_signature(b"PK" + bytes([92]) + b"x03", "a.zip"))

    def test_main_returns_failure_when_all_fetches_fail(self):
        args = ["--data", "x", "--output", "y", "--evidence-dir", "z", "--database", "d"]
        with patch.object(monitor, "run_update", return_value={"success": False}), redirect_stdout(io.StringIO()):
            self.assertEqual(monitor.main(args), 1)

    def test_before_deadline_not_expired(self):
        self.run_monitor(datetime(2026, 11, 14, 8, 59, tzinfo=timezone.utc))
        self.assertEqual(self.read()["jobs"][0]["availability"], "open")
        self.assertEqual(self.read()["cloudUpdate"]["expired"], 0)

    def test_http_and_changed_hash_do_not_verify(self):
        now = datetime(2026, 11, 13, 8, tzinfo=timezone.utc)
        self.run_monitor(now)
        first = self.read()
        self.assertEqual(first["cloudUpdate"]["changed"], 0)
        self.run_monitor(now, lambda url: fake_fetch(url, text="网页内容变化，2028 可能新招聘"))
        second = self.read()
        self.assertEqual(second["cloudUpdate"]["changed"], 1)
        self.assertEqual(second["jobs"][0]["verifiedAt"], "2026-10-29")
        self.assertEqual(second["jobs"][0]["eligibilityCode"], "uncertain")
        self.assertEqual(second["meta"], self.data["meta"])
        self.assertIn("checkedAt", second["cloudUpdate"])
        self.run_monitor(now, lambda url: {"httpStatus": 503, "ok": False, "error": "http_status"})
        third = self.read()
        self.assertFalse(third["cloudUpdate"]["success"])
        self.assertEqual(third["cloudUpdate"]["changed"], 0)
        old_hash = second["cloudUpdate"]["urlState"]["https://example.edu.cn/jobs/1.html"]["textHash"]
        self.assertEqual(third["cloudUpdate"]["urlState"]["https://example.edu.cn/jobs/1.html"]["textHash"], old_hash)

    def test_discovered_candidates_queue_for_automatic_details(self):
        links = [{"url": "/jobs/2.html", "title": "2027岩土工程博士招聘"},
                 {"url": "https://other.edu.cn/2", "title": "2027招聘"},
                 {"url": "/news/a", "title": "普通校友新闻"},
                 {"url": "/jobs/1.html", "title": "已入库招聘"}]
        self.run_monitor(datetime(2026, 11, 13, tzinfo=timezone.utc),
                         lambda url: fake_fetch(url, links=links))
        data = self.read()
        self.assertEqual(len(data["jobs"]), 1)
        self.assertEqual(len(data["cloudCandidates"]), 1)
        candidate = data["cloudCandidates"][0]
        self.assertIn(candidate["reviewStatus"], ("queued", "retry"))
        self.assertEqual(candidate["eligibility"], "uncertain")
        self.assertEqual(candidate["availability"], "unknown")
        self.assertNotIn("verifiedAt", candidate)
        self.run_monitor(datetime(2026, 11, 13, tzinfo=timezone.utc),
                         lambda url: fake_fetch(url, links=links))
        self.assertEqual(len(self.read()["cloudCandidates"]), 1)

    def test_secondary_source_cannot_create_official_candidate(self):
        self.data["jobs"][0]["sourceTrust"] = "secondary"
        self.path.write_text(json.dumps(self.data), encoding="utf-8")
        self.run_monitor(datetime(2026, 11, 15, tzinfo=timezone.utc),
                         lambda url: fake_fetch(url, links=[{"url": "/new", "title": "招聘"}]))
        self.assertEqual(self.read()["cloudCandidates"], [])
        self.assertEqual(self.read()["jobs"][0]["availability"], "open")

    def test_json_and_sqlite_preserve_every_section(self):
        self.run_monitor(datetime(2026, 11, 13, tzinfo=timezone.utc))
        data = self.read()
        for key in ("profile", "papers", "talentAssessments", "journalRankings", "civil", "customFutureField"):
            self.assertEqual(data[key], self.data[key])
        with closing(sqlite3.connect(self.db)) as conn:
            for table in ("jobs", "links", "paper_policies", "profile_records",
                          "talent_assessments", "journal_rankings", "candidates"):
                conn.execute("SELECT payload_json FROM " + table).fetchall()
            snapshot = json.loads(conn.execute("SELECT payload_json FROM dashboard_snapshot").fetchone()[0])
        self.assertEqual(snapshot, data)
        # Evidence stores hashes and metadata, not the fetched full body.
        evidence = json.loads(next(self.evidence.glob("cloud-*.json")).read_text(encoding="utf-8"))
        self.assertNotIn("bodyText", json.dumps(evidence))
        self.assertNotIn("完整条件待人工核验", json.dumps(evidence, ensure_ascii=False))

    def test_bad_data_never_overwrites_output_or_database(self):
        output = self.root / "existing.json"
        output.write_bytes(b"KEEP_JSON")
        self.db.write_bytes(b"KEEP_DATABASE")
        for invalid in ("{broken", json.dumps({"jobs": [{}]}),
                        json.dumps({"jobs": [self.data["jobs"][0], self.data["jobs"][0]]})):
            self.path.write_text(invalid, encoding="utf-8")
            with self.assertRaises((ValueError, json.JSONDecodeError)):
                monitor.run_update(self.path, output, self.evidence, self.db, fetcher=fake_fetch)
            self.assertEqual(output.read_bytes(), b"KEEP_JSON")
            self.assertEqual(self.db.read_bytes(), b"KEEP_DATABASE")
            self.assertFalse(self.evidence.exists())

    def test_public_url_guard(self):
        for url in ("http://127.0.0.1/", "http://10.0.0.1/", "http://[::1]/",
                    "http://169.254.169.254/", "http://localhost/", "http://x.local/",
                    "file:///etc/passwd", "https://user:password@example.com/",
                    "http://example.com:22/", "https://example.com/?token=secret"):
            with self.subTest(url=url):
                with self.assertRaises(ValueError):
                    monitor.validate_public_url(url)
        def private_resolver(*args, **kwargs):
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.168.1.1", 443))]
        with self.assertRaises(ValueError):
            monitor.validate_public_url("https://public-looking.example/", private_resolver)
        def public_resolver(*args, **kwargs):
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))]
        normalized, addresses = monitor.validate_public_url("https://example.com/a#frag", public_resolver)
        self.assertEqual(normalized, "https://example.com/a")
        self.assertEqual(addresses, ["8.8.8.8"])

    def test_html_strips_executable_and_hidden_content(self):
        parser = monitor.VisibleHTML()
        parser.feed("<style>secretStyle</style><script>secretScript</script><p>公开正文</p>"
                    "<a href='/jobs/2'>2027 招聘</a>")
        self.assertNotIn("secret", parser.text)
        self.assertIn("公开正文", parser.text)
        self.assertEqual(parser.links[0]["url"], "/jobs/2")

    def test_official_civil_attachment_archives_without_overwrite(self):
        self.data["jobs"][0]["position"] = "2027 公务员选调公告"
        self.path.write_text(json.dumps(self.data), encoding="utf-8")
        links = [{"url": "/files/positions.pdf", "title": "官方职位附件"},
                 {"url": "https://elsewhere.example/file.pdf", "title": "跨域附件"}]
        payload = b"%PDF-1.7\\npublic fixture only"
        binary = lambda url: {"ok": True, "httpStatus": 200, "finalUrl": url, "dataBytes": payload}
        now = datetime(2026, 11, 13, tzinfo=timezone.utc)
        monitor.run_update(self.path, self.path, self.evidence, self.db, now=now,
                           fetcher=lambda url: fake_fetch(url, links=links), attachment_fetcher=binary)
        first = self.read()
        self.assertEqual(len(first["cloudAttachments"]), 1)
        attachment = first["cloudAttachments"][0]
        target = self.evidence / attachment["path"]
        self.assertEqual(target.read_bytes(), payload)
        self.assertEqual(attachment["reviewStatus"], "original_unreviewed")
        self.assertEqual(first["jobs"][0]["verifiedAt"], "2026-10-29")
        before = target.stat().st_mtime_ns
        monitor.run_update(self.path, self.path, self.evidence, self.db, now=now,
                           fetcher=lambda url: fake_fetch(url, links=links), attachment_fetcher=binary)
        self.assertEqual(target.stat().st_mtime_ns, before)
        self.assertEqual(len(self.read()["cloudAttachments"]), 1)

    def test_html_disguised_as_official_attachment_is_not_archived(self):
        self.data["jobs"][0]["position"] = "公务员招聘"
        self.path.write_text(json.dumps(self.data), encoding="utf-8")
        links = [{"url": "/files/positions.xlsx", "title": "职位表"}]
        monitor.run_update(self.path, self.path, self.evidence, self.db,
                           now=datetime(2026, 11, 13, tzinfo=timezone.utc),
                           fetcher=lambda url: fake_fetch(url, links=links),
                           attachment_fetcher=lambda url: {"ok": True, "httpStatus": 200,
                               "finalUrl": url, "dataBytes": b"<html>captcha</html>"})
        self.assertNotIn("cloudAttachments", self.read())
        self.assertFalse((self.evidence / "attachments").exists())

    def test_candidate_title_relevance(self):
        for title in ("招聘会", "招聘信息", "2027校园招聘", "招聘场地简介",
                      "医科招聘信息", "报名费用减免指南", "博士后招聘指南", "博士后面试结果", "岩土博士招聘拟聘公示",
                      "护理学院2027博士招聘", "某公司销售经理招聘"):
            with self.subTest(title=title):
                self.assertIsNotNone(monitor.candidate_filter_reason(title))
        for title in ("岩土工程博士后招聘公告", "2027 CAE 仿真算法工程师招聘",
                      "地震研究所科研岗位招聘"):
            self.assertIsNone(monitor.candidate_filter_reason(title))
        self.assertIsNone(monitor.candidate_filter_reason("某某大学2027教师招聘", ["某某大学"]))

    def test_existing_noise_archived_losslessly_reviewed_kept(self):
        old_archive = {"candidate": {"id": "OLD", "title": "旧记录"}, "reason": "先前人工归档"}
        noisy = {"id": "N1", "title": "招聘信息", "url": "https://example.edu.cn/nav",
                 "reviewStatus": "pending", "discoveredAt": "2026-11-14", "evidence": {"keep": 1}}
        reviewed = {"id": "R1", "title": "招聘信息", "reviewStatus": "reviewed"}
        valid = {"id": "V1", "title": "岩土工程博士后招聘", "reviewStatus": "pending"}
        self.data["cloudCandidates"] = [noisy, reviewed, valid]
        self.data["cloudCandidateArchive"] = [old_archive]
        self.path.write_text(json.dumps(self.data), encoding="utf-8")
        self.run_monitor(datetime(2026, 11, 15, tzinfo=timezone.utc), max_pages=0)
        result = self.read()
        self.assertEqual([c["id"] for c in result["cloudCandidates"]], ["R1", "V1"])
        self.assertEqual(result["cloudCandidateArchive"][0], old_archive)
        self.assertEqual(result["cloudCandidateArchive"][1]["candidate"], noisy)
        self.assertIn("reason", result["cloudCandidateArchive"][1])
        self.assertEqual(result["cloudUpdate"]["archivedCandidates"], 1)
        self.run_monitor(datetime(2026, 11, 15, tzinfo=timezone.utc), max_pages=0)
        self.assertEqual(len(self.read()["cloudCandidateArchive"]), 2)

    def test_job_monitor_compact_but_evidence_remains_detailed(self):
        source = self.data["jobs"][0]["announcementUrl"]
        legacy = {"checkedAt": "2026-11-14", "httpStatus": 200, "bodyFetched": True,
                  "textHash": "old", "contentChanged": False, "error": None,
                  "jobIds": ["NI001"] * 200, "redirects": [{"url": "https://example.edu.cn"}],
                  "diff": {"characters": 500}, "url": source, "contentType": "text/html"}
        self.data["jobs"][0]["cloudMonitor"] = {"checkedAt": "2026-11-14", "sources": {source: legacy}}
        self.path.write_text(json.dumps(self.data), encoding="utf-8")
        self.run_monitor(datetime(2026, 11, 15, tzinfo=timezone.utc), max_pages=0)
        compact = self.read()["jobs"][0]["cloudMonitor"]["sources"][source]
        self.assertEqual(set(compact), set(monitor.COMPACT_OBSERVATION_FIELDS))
        self.assertEqual(compact["textHash"], "old")
        self.assertEqual(self.read()["jobs"][0]["verifiedAt"], "2026-10-29")
        self.run_monitor(datetime(2026, 11, 16, tzinfo=timezone.utc))
        compact = self.read()["jobs"][0]["cloudMonitor"]["sources"][source]
        self.assertEqual(set(compact), set(monitor.COMPACT_OBSERVATION_FIELDS))
        evidence = json.loads(sorted(self.evidence.glob("cloud-*.json"))[-1].read_text(encoding="utf-8"))
        self.assertEqual(evidence["attempts"][0]["jobIds"], ["NI001"])
        self.assertIn("diff", evidence["attempts"][0])
        self.assertIn("redirects", evidence["attempts"][0])

    def queue_detail(self, title=DETAIL_TITLE, url="https://example.edu.cn/jobs/new.html", candidate_id="C1"):
        candidate = {"id": candidate_id, "url": url, "title": title,
                     "sourceUrl": self.data["jobs"][0]["announcementUrl"],
                     "sourceJobIds": ["NI001"], "reviewStatus": "queued",
                     "discoveredAt": "2026-11-20T12:00:00+08:00"}
        self.data.setdefault("cloudCandidates", []).append(candidate)
        self.path.write_text(json.dumps(self.data, ensure_ascii=False), encoding="utf-8")
        return candidate

    def run_details(self, fetcher=detail_response, now=DETAIL_NOW, limit=30):
        monitor.run_update(self.path, self.path, self.evidence, self.db,
                           max_pages=0, max_detail_pages=limit, now=now,
                           fetcher=fetcher, detail_fetcher=fetcher)
        return self.read()

    def automatic_jobs(self, data):
        return [job for job in data["jobs"] if job.get("ingestionMode") == "automatic"]

    def test_automatic_body_ingestion_persists_without_human_gate(self):
        self.queue_detail()
        data = self.run_details()
        auto = self.automatic_jobs(data)
        self.assertEqual(len(auto), 1)
        job = auto[0]
        self.assertEqual(job["verifiedAt"], "")
        self.assertEqual(job["organization"], "某地质研究院")
        self.assertIn("武汉", job["region"])
        self.assertEqual(job["sourcePublishedAt"], "2026-11-20")
        self.assertEqual(job["eligibility"], "uncertain")
        self.assertEqual(job["employerNature"]["code"], "unknown")
        self.assertTrue(job["sourceCheckedAt"])
        self.assertTrue(job["ingestionMethod"])
        self.assertIn("autoEvidence", job)
        self.assertIn("博士", job["requirements"])
        self.assertIn("2027", job["eligibility2027"])
        self.assertEqual(monitor.parse_deadline(job).strftime("%Y-%m-%d %H:%M"), "2026-12-10 17:00")
        self.assertEqual(data["cloudUpdate"]["autoAdded"], 1)
        self.assertEqual(data["stats"]["jobs"], len(data["jobs"]))
        candidate = next(c for c in data["cloudCandidates"] if c["id"] == "C1")
        self.assertEqual(candidate["reviewStatus"], "imported")
        self.assertEqual(candidate["jobId"], job["id"])
        self.assertEqual(data["jobs"][0]["verifiedAt"], "2026-10-29")
        for key in ("profile", "papers", "talentAssessments", "journalRankings"):
            self.assertEqual(data[key], self.data[key])
        with closing(sqlite3.connect(self.db)) as connection:
            payload = json.loads(connection.execute(
                "SELECT payload_json FROM jobs WHERE id=?", (job["id"],)).fetchone()[0])
        self.assertEqual(payload, job)
        self.assertNotIn(DETAIL_BODY, json.dumps(job, ensure_ascii=False))

    def test_auto_ingestion_duplicate_urls_and_body_do_not_add_twice(self):
        self.queue_detail()
        self.queue_detail(url="https://example.edu.cn/jobs/mirror.html", candidate_id="C2")
        first = self.run_details()
        self.assertEqual(len(self.automatic_jobs(first)), 1)
        first_id = self.automatic_jobs(first)[0]["id"]
        second = self.run_details(now=datetime(2026, 11, 22, 1, tzinfo=timezone.utc))
        self.assertEqual([j["id"] for j in self.automatic_jobs(second)], [first_id])
        self.assertEqual(second["cloudUpdate"]["autoAdded"], 0)
        self.assertEqual(second["cloudUpdate"]["autoUpdated"], 0)

    def test_auto_updated_counts_changed_extraction_not_http_or_footer(self):
        self.queue_detail()
        first = self.run_details()
        job_id = self.automatic_jobs(first)[0]["id"]
        second = self.run_details(lambda url: detail_response(
            url, page_body=DETAIL_BODY + "\n相关新闻：2028年12月31日栏目有效期结束。"),
            now=datetime(2026, 11, 22, 1, tzinfo=timezone.utc))
        self.assertEqual(second["cloudUpdate"]["autoUpdated"], 0)
        changed_body = DETAIL_BODY.replace("工作地点：武汉市。", "工作地点：长沙市。")
        third = self.run_details(lambda url: detail_response(url, body=changed_body),
                                 now=datetime(2026, 11, 23, 1, tzinfo=timezone.utc))
        self.assertEqual(third["cloudUpdate"]["autoAdded"], 0)
        self.assertEqual(third["cloudUpdate"]["autoUpdated"], 1)
        job = next(j for j in third["jobs"] if j["id"] == job_id)
        self.assertIn("长沙", job["region"])

    def test_degree_and_platform_dates_never_become_application_deadline(self):
        self.queue_detail()
        body = DETAIL_BODY.replace(
            "报名时间：2026年11月20日至2026年12月10日17:00。", "").replace(
            "报名截止时间：2026年12月10日17:00。", "")
        body += "\n高校就业平台信息有效期：2026年12月31日。"
        result = self.run_details(lambda url: detail_response(url, body=body))
        job = self.automatic_jobs(result)[0]
        self.assertIsNone(job.get("deadline"))
        self.assertIsNone(monitor.parse_deadline(job))
        self.assertEqual(job["availability"], "monitoring")
        self.assertEqual(job["eligibility"], "uncertain")
        self.assertIn("2027", job["eligibility2027"])

    def test_source_university_never_becomes_advertised_employer_or_location(self):
        self.data["jobs"][0].update(organization="某转载大学", province="江苏", region="南京")
        self.queue_detail()
        body = DETAIL_BODY.replace("工作地点：武汉市。", "")
        body += "\n某地质研究院总部地址：北京市。学校就业平台仅提供信息发布。"
        result = self.run_details(lambda url: detail_response(url, body=body))
        job = self.automatic_jobs(result)[0]
        self.assertEqual(job["organization"], "某地质研究院")
        self.assertNotEqual(job.get("province"), "江苏")
        self.assertNotIn("南京", job.get("region", ""))
        self.assertNotIn("北京", job.get("region", ""))
        self.assertNotIn("武汉", job.get("region", ""))
        self.assertEqual(job["employerNature"]["code"], "unknown")

    def test_missing_employer_stays_unknown_not_source_organization(self):
        self.data["jobs"][0].update(organization="某转载大学", province="江苏", region="南京")
        title = "岩土工程博士后招聘公告"
        self.queue_detail(title=title)
        body = DETAIL_BODY.replace(DETAIL_TITLE, title).replace("招聘单位：某地质研究院。", "")
        result = self.run_details(lambda url: detail_response(url, body=body, title=title))
        job = self.automatic_jobs(result)[0]
        self.assertNotEqual(job["organization"], "某转载大学")
        self.assertNotEqual(job["organization"], "某地质研究院")
        self.assertTrue(any(term in job["organization"] for term in ("未明确", "未取得", "未知")))

    def test_detail_fetch_failure_retries_without_false_ingestion(self):
        self.queue_detail()
        failed = self.run_details(lambda url: {"ok": False, "httpStatus": 503, "error": "http_status",
                                               "finalUrl": url})
        self.assertEqual(len(self.automatic_jobs(failed)), 0)
        self.assertEqual(failed["cloudUpdate"]["autoAdded"], 0)
        self.assertEqual(failed["cloudUpdate"]["autoUpdated"], 0)
        self.assertGreaterEqual(failed["cloudUpdate"]["pendingDetails"], 1)
        self.assertEqual(failed["cloudCandidates"][0]["reviewStatus"], "retry")
        recovered = self.run_details(now=datetime(2026, 11, 22, 1, tzinfo=timezone.utc))
        self.assertEqual(len(self.automatic_jobs(recovered)), 1)

    def test_failed_refresh_preserves_last_successful_auto_evidence(self):
        self.queue_detail()
        first = self.run_details()
        before = copy.deepcopy(self.automatic_jobs(first)[0])
        failed = self.run_details(lambda url: {"ok": False, "httpStatus": 503, "error": "http_status",
                                               "finalUrl": url},
                                  now=datetime(2026, 11, 22, 1, tzinfo=timezone.utc))
        after = self.automatic_jobs(failed)[0]
        self.assertEqual(after["sourceCheckedAt"], before["sourceCheckedAt"])
        self.assertEqual(after["autoEvidence"], before["autoEvidence"])
        self.assertEqual(after["verifiedAt"], "")
        self.assertEqual(failed["cloudUpdate"]["autoUpdated"], 0)

    def test_non_positions_and_pure_medical_candidates_are_archived(self):
        for index, title in enumerate(("某研究院博士后联谊会", "岩土招聘初面通过名单",
                                       "某临床医科大学护理博士后招聘公告")):
            self.queue_detail(title=title, url="https://example.edu.cn/jobs/noise" + str(index),
                              candidate_id="NOISE" + str(index))
        result = self.run_details()
        self.assertEqual(len(self.automatic_jobs(result)), 0)
        archived_ids = {entry["candidate"]["id"] for entry in result["cloudCandidateArchive"]}
        self.assertTrue({"NOISE0", "NOISE1", "NOISE2"}.issubset(archived_ids))

    def test_navigation_and_related_links_do_not_supply_job_evidence(self):
        self.queue_detail()
        page = "首页 新闻中心 人才招聘 岩土工程招聘链接 博士后招聘链接 登录 注册 联系我们"
        result = self.run_details(lambda url: detail_response(url, body=page,
                                   page_body=page + "\n" + DETAIL_BODY))
        self.assertEqual(len(self.automatic_jobs(result)), 0)
        self.assertEqual(result["cloudUpdate"]["autoAdded"], 0)

    def test_past_or_future_dated_notices_are_not_promoted_as_open(self):
        self.queue_detail()
        old_body = DETAIL_BODY.replace("2026年12月10日", "2026年11月19日")
        old = self.run_details(lambda url: detail_response(url, body=old_body))
        self.assertEqual(self.automatic_jobs(old)[0]["availability"], "historical")
        self.assertEqual(self.automatic_jobs(old)[0]["eligibility"], "uncertain")

    def test_future_publication_date_is_not_current_open_job(self):
        self.queue_detail()
        future_body = DETAIL_BODY.replace("发布日期：2026年11月20日", "发布日期：2026年12月20日")
        result = self.run_details(lambda url: detail_response(url, body=future_body))
        self.assertNotEqual(self.automatic_jobs(result)[0]["availability"], "open")

    def test_existing_manual_job_url_is_deduped_without_semantic_rewrite(self):
        self.queue_detail(url=self.data["jobs"][0]["announcementUrl"])
        result = self.run_details()
        self.assertEqual(len(result["jobs"]), 1)
        self.assertEqual(result["cloudUpdate"]["autoAdded"], 0)
        self.assertEqual(result["jobs"][0]["verifiedAt"], "2026-10-29")
        self.assertEqual(result["jobs"][0]["paperPolicy"], self.data["jobs"][0]["paperPolicy"])

    def test_detail_budget_rotates_and_does_not_require_manual_review(self):
        for index in range(3):
            self.queue_detail(url="https://example.edu.cn/jobs/budget" + str(index),
                              candidate_id="B" + str(index))
        called = []
        def fail(url):
            called.append(url)
            return {"ok": False, "httpStatus": 503, "error": "http_status", "finalUrl": url}
        first = self.run_details(fail, limit=1)
        self.assertEqual(len(called), 1)
        self.assertEqual(first["cloudUpdate"]["detailChecked"], 1)
        self.assertEqual(first["cloudUpdate"]["pendingDetails"], 3)
        prior_url = called[0]
        called.clear()
        self.run_details(fail, limit=1, now=datetime(2026, 11, 22, 1, tzinfo=timezone.utc))
        self.assertEqual(len(called), 1)
        self.assertNotEqual(called[0], prior_url)

    def test_secondary_domain_cannot_impersonate_official_source(self):
        self.data["jobs"][0]["announcementUrl"] = "https://www.gaoxiaojob.com/announcement/seed.html"
        self.data["jobs"][0]["sourceTrust"] = "official"
        candidate = self.queue_detail(url="https://www.gaoxiaojob.com/announcement/new.html")
        candidate["sourceKind"] = "same_host_link_from_recorded_official_source"
        candidate["sourceTrust"] = "official"
        self.path.write_text(json.dumps(self.data), encoding="utf-8")
        result = self.run_details()
        self.assertEqual(len(self.automatic_jobs(result)), 0)
        self.assertEqual(result["cloudUpdate"]["autoAdded"], 0)
        self.assertTrue(any(entry["candidate"]["id"] == "C1"
                            for entry in result["cloudCandidateArchive"]))

    def test_archived_url_rediscovery_is_not_queued_fetched_or_rearchived(self):
        candidate = {"id": "ARCHIVED", "url": "https://example.edu.cn/jobs/old.html",
                     "title": DETAIL_TITLE, "sourceUrl": self.data["jobs"][0]["announcementUrl"],
                     "sourceJobIds": ["NI001"], "reviewStatus": "queued"}
        old_entry = {"candidate": candidate, "archivedAt": "2026-11-20T12:00:00+08:00",
                     "reason": "历史记录判定非岗位", "ruleVersion": "previous"}
        self.data["cloudCandidateArchive"] = [copy.deepcopy(old_entry)]
        self.path.write_text(json.dumps(self.data), encoding="utf-8")
        detail_calls = []
        def detail(url):
            detail_calls.append(url)
            return detail_response(url)
        monitor.run_update(self.path, self.path, self.evidence, self.db, now=DETAIL_NOW,
                           max_pages=1, max_detail_pages=30,
                           fetcher=lambda url: fake_fetch(url, links=[
                               {"url": candidate["url"], "title": DETAIL_TITLE}]),
                           detail_fetcher=detail)
        result = self.read()
        self.assertEqual(result["cloudUpdate"]["newCandidates"], 0)
        self.assertEqual(result["cloudUpdate"]["detailChecked"], 0)
        self.assertEqual(detail_calls, [])
        self.assertEqual(result["cloudCandidateArchive"], [old_entry])
        self.assertEqual(result.get("cloudCandidates"), [])

    def test_real_html_article_isolated_from_nav_and_related_news(self):
        def parse_html(html, url):
            parser = monitor.VisibleHTML()
            parser.feed(html)
            return {"url": url, "finalUrl": url, "httpStatus": 200, "ok": True,
                    "bodyText": parser.text, "articleText": parser.article_text,
                    "pageTitle": parser.title, "headings": parser.headings,
                    "links": parser.links, "contentType": "text/html"}
        html = ("<html><head><title>" + DETAIL_TITLE + "</title></head><body>"
                "<nav><a href='/medical'>护理博士后招聘</a></nav>"
                "<article><h1>" + DETAIL_TITLE + "</h1><p>" +
                DETAIL_BODY.replace("\n", "</p><p>") + "</p></article>"
                "<aside>报名截止时间：2030年1月1日；临床护理岗位招聘</aside>"
                "<div class='related-news'>相关新闻：招聘单位：别的学院。</div></body></html>")
        parsed = parse_html(html, "https://example.edu.cn/jobs/new.html")
        self.assertIn("某地质研究院", parsed["articleText"])
        self.assertNotIn("护理", parsed["articleText"])
        self.assertNotIn("别的学院", parsed["articleText"])
        self.queue_detail()
        result = self.run_details(lambda url: parse_html(html, url))
        self.assertEqual(len(self.automatic_jobs(result)), 1)
        self.assertEqual(self.automatic_jobs(result)[0]["organization"], "某地质研究院")
        self.assertEqual(self.automatic_jobs(result)[0]["deadline"], "2026-12-10")

        # A related job present only in navigation/related-news is not the article.
        self.data = sample()
        self.queue_detail(candidate_id="NAV", url="https://example.edu.cn/jobs/nav.html")
        nav_html = ("<html><head><title>服务门户</title></head><body>"
                    "<nav><h1>" + DETAIL_TITLE + "</h1><p>" + DETAIL_BODY + "</p></nav>"
                    "<article><h1>服务门户</h1><p>本页提供栏目导航，请选择所需服务。</p></article>"
                    "<div class='related-news'>" + DETAIL_BODY + "</div></body></html>")
        nav_result = self.run_details(lambda url: parse_html(nav_html, url))
        self.assertEqual(len(self.automatic_jobs(nav_result)), 0)
        self.assertEqual(nav_result["cloudUpdate"]["autoAdded"], 0)

    def test_stats_province_whitelist_and_national_added_baseline(self):
        data = {"stats": {"nationalAdded": 820}, "jobs": [
            {"id": "1", "province": "湖北/浙江", "provinceList": ["湖北", "浙江"], "scope": "national"},
            {"id": "2", "province": "湖北/浙江", "scope": "national"},
            {"id": "3", "province": "公告未明确/自动提取未取得", "scope": "national"},
            {"id": "4", "province": "北京", "provinceList": ["北京", "湖北/浙江", "未知"], "scope": "nearby"},
        ]}
        monitor.refresh_stats(data)
        self.assertEqual(data["stats"]["provinces"], 3)
        self.assertEqual(data["stats"]["nationalAdded"], 820)
        monitor.refresh_stats(data, auto_added=1)
        self.assertEqual(data["stats"]["nationalAdded"], 821)

    def test_explicit_nearby_work_locations_receive_nearby_scope(self):
        for city in ("宁波", "余姚", "杭州", "绍兴", "舟山", "台州", "嘉兴"):
            with self.subTest(city=city):
                self.data = sample()
                self.queue_detail()
                body = DETAIL_BODY.replace("工作地点：武汉市。", "工作地点：" + city + "市。")
                result = self.run_details(lambda url: detail_response(url, body=body))
                job = self.automatic_jobs(result)[0]
                self.assertEqual(job["scope"], "nearby")
                self.assertIn("浙江", job["provinceList"])
                self.assertIsInstance(job["timeWindow"], str)
                self.assertTrue(job["timeWindow"])

    def test_source_nearby_address_does_not_create_nearby_job(self):
        self.data["jobs"][0].update(organization="宁波某大学", region="宁波", province="浙江", scope="nearby")
        self.queue_detail()
        body = DETAIL_BODY.replace("工作地点：武汉市。", "")
        body += "\n转载平台联系地址：宁波市。"
        result = self.run_details(lambda url: detail_response(url, body=body))
        job = self.automatic_jobs(result)[0]
        self.assertNotEqual(job["scope"], "nearby")
        self.assertNotIn("浙江", job.get("provinceList", []))
        self.assertNotIn("宁波", job["region"])

    def test_automatic_route_categories_follow_explicit_role(self):
        cases = [
            ("某地质研究院", "岩土工程博士后", "博士后"),
            ("某大学土木工程学院", "土木工程专任教师", "高校科研"),
            ("某市交通局", "事业单位岩土技术岗位", "公务员/事业单位"),
            ("某仿真科技有限公司", "CAE软件研发工程师", "企业研发"),
            ("某交通设计院", "岩土勘察设计工程师", "工程技术"),
            ("某能源集团", "综合招聘", "综合招聘"),
        ]
        for organization, role, expected in cases:
            with self.subTest(role=role):
                self.data = sample()
                title = organization + role + "招聘公告"
                self.queue_detail(title=title)
                body = DETAIL_BODY.replace(DETAIL_TITLE, title).replace(
                    "招聘单位：某地质研究院。", "招聘单位：" + organization + "。").replace(
                    "招聘岗位：岩土工程科研博士。", "招聘岗位：" + role + "。")
                result = self.run_details(lambda url: detail_response(url, body=body, title=title))
                job = self.automatic_jobs(result)[0]
                self.assertEqual(job["routeCategory"], expected)
                self.assertIsInstance(job["timeWindow"], str)

    def test_location_refresh_updates_province_list_and_scope(self):
        self.queue_detail()
        first = self.run_details()
        original = self.automatic_jobs(first)[0]
        self.assertEqual(original["province"], "湖北")
        body = DETAIL_BODY.replace("工作地点：武汉市。", "工作地点：宁波市。")
        second = self.run_details(lambda url: detail_response(url, body=body),
                                  now=datetime(2026, 11, 22, 1, tzinfo=timezone.utc))
        changed = self.automatic_jobs(second)[0]
        self.assertEqual(changed["id"], original["id"])
        self.assertEqual(changed["province"], "浙江")
        self.assertEqual(changed["provinceList"], ["浙江"])
        self.assertEqual(changed["scope"], "nearby")
        self.assertEqual(second["cloudUpdate"]["autoUpdated"], 1)

    def test_total_run_budget_stops_details_without_losing_retry_queue(self):
        self.queue_detail()
        calls = []
        def detail(url):
            calls.append(url)
            return detail_response(url)
        with patch.object(monitor, "RUN_TIMEOUT", 0):
            result = self.run_details(detail)
        self.assertEqual(calls, [])
        self.assertEqual(result["cloudUpdate"]["detailChecked"], 0)
        self.assertEqual(result["cloudUpdate"]["pendingDetails"], 1)
        self.assertEqual(len(result["cloudCandidates"]), 1)
        self.assertEqual(len(self.automatic_jobs(result)), 0)

    def test_public_contact_details_are_not_copied_into_jobs_or_field_evidence(self):
        self.queue_detail()
        body = DETAIL_BODY.replace(
            "招聘专业：岩土工程、工程力学。",
            "招聘专业：岩土工程、工程力学，联系人：张先生，联系电话：13800138000，邮箱：contact@example.org。")
        result = self.run_details(lambda url: detail_response(url, body=body))
        job = self.automatic_jobs(result)[0]
        serialized = json.dumps(job, ensure_ascii=False)
        self.assertIn("岩土", job["requirements"])
        for contact in ("张先生", "13800138000", "contact@example.org"):
            self.assertNotIn(contact, serialized)

    def test_missing_new_deadline_retains_evidence_and_later_expires(self):
        self.queue_detail()
        first = self.run_details()
        original = copy.deepcopy(self.automatic_jobs(first)[0])
        self.assertEqual(original["deadline"], "2026-12-10")
        self.assertEqual(original["availability"], "open")
        original_evidence = copy.deepcopy(original["autoEvidence"]["fields"]["deadline"])
        body = DETAIL_BODY.replace(
            "报名时间：2026年11月20日至2026年12月10日17:00。", "").replace(
            "报名截止时间：2026年12月10日17:00。", "")
        second = self.run_details(lambda url: detail_response(url, body=body),
                                  now=datetime(2026, 11, 22, 1, tzinfo=timezone.utc))
        refreshed = self.automatic_jobs(second)[0]
        self.assertEqual(refreshed["id"], original["id"])
        self.assertEqual(refreshed["deadline"], original["deadline"])
        self.assertEqual(refreshed["deadlineTime"], original["deadlineTime"])
        retained = refreshed["autoEvidence"]["fields"]["deadline"]
        self.assertEqual(retained["value"], original_evidence["value"])
        self.assertEqual(retained["quote"], original_evidence["quote"])
        self.assertTrue(retained["retainedFromEarlier"])
        self.assertEqual(retained["observedAt"], original["sourceCheckedAt"])
        self.assertEqual(refreshed["verifiedAt"], "")
        self.assertIn("2027", refreshed["eligibility2027"])

        self.run_monitor(datetime(2026, 12, 11, 1, tzinfo=timezone.utc), max_pages=0)
        after = next(job for job in self.read()["jobs"] if job["id"] == original["id"])
        self.assertEqual(after["availability"], "historical")
        self.assertEqual(after["deadline"], "2026-12-10")
        self.assertEqual(after["verifiedAt"], "")
        self.assertEqual(after["autoEvidence"]["fields"]["deadline"], retained)

    def test_cumt_real_h5_title_beats_generic_sidebar_headings(self):
        candidate = {"id": "CUMT", "url": "https://hr.cumt.edu.cn/info/1026/4337.htm",
                     "title": CUMT_RECRUIT_TITLE}
        result = html_fixture_response(candidate["url"], CUMT_RECRUIT_HTML)
        job, reason = monitor.extract_automatic_job(candidate, result, DETAIL_NOW)
        self.assertIsNone(reason)
        self.assertEqual(job["position"], CUMT_RECRUIT_TITLE)
        self.assertEqual(job["organization"], "中国矿业大学")
        self.assertNotEqual(job["position"], "博士后")

    def test_cumt_policy_real_title_cannot_be_replaced_by_recruitment_chapter(self):
        candidate = {"id": "POLICY", "url": "https://hr.cumt.edu.cn/info/1033/4475.htm",
                     "title": CUMT_POLICY_TITLE}
        result = html_fixture_response(candidate["url"], CUMT_POLICY_HTML)
        job, reason = monitor.extract_automatic_job(candidate, result, DETAIL_NOW)
        self.assertIsNone(job)
        self.assertEqual(reason, "policy_document")

    def test_old_cohorts_expire_but_old_publication_date_alone_does_not(self):
        for title in ("某地质研究院2024应届博士招聘公告", "某地质研究院2025校园招聘公告",
                      "某地质研究院2026春季校园招聘公告", "某地质研究院2026届校园招聘公告"):
            with self.subTest(title=title):
                candidate = {"id": "COHORT", "url": "https://example.edu.cn/jobs/cohort.html", "title": title}
                body = DETAIL_BODY.replace(DETAIL_TITLE, title).replace(
                    "报名时间：2026年11月20日至2026年12月10日17:00。", "").replace(
                    "报名截止时间：2026年12月10日17:00。", "")
                job, reason = monitor.extract_automatic_job(
                    candidate, detail_response(candidate["url"], body=body, title=title), DETAIL_NOW)
                self.assertIsNone(reason)
                self.assertEqual(job["availability"], "historical")
                self.assertIn("cohortExpiry", job["autoEvidence"]["fields"])
                self.assertIn(title, job["autoEvidence"]["fields"]["cohortExpiry"]["quote"])
                self.assertIsNone(job.get("deadline"))
        title = "某地质研究院岩土博士招聘公告"
        candidate = {"url": "https://example.edu.cn/jobs/dated.html", "title": title}
        body = DETAIL_BODY.replace(DETAIL_TITLE, title).replace(
            "发布日期：2026年11月20日。", "发布日期：2024年11月20日。").replace(
            "报名时间：2026年11月20日至2026年12月10日17:00。", "").replace(
            "报名截止时间：2026年12月10日17:00。", "").replace("招聘对象：2027届毕业生。", "")
        job, reason = monitor.extract_automatic_job(
            candidate, detail_response(candidate["url"], body=body, title=title), DETAIL_NOW)
        self.assertIsNone(reason)
        self.assertEqual(job["availability"], "monitoring")
        self.assertNotIn("cohortExpiry", job["autoEvidence"]["fields"])

    def test_company_suffix_in_actual_page_title_identifies_employer(self):
        title = "2027届岩土工程校园招聘-华北工程设计有限公司"
        body = DETAIL_BODY.replace(DETAIL_TITLE, "2027届岩土工程校园招聘").replace(
            "招聘单位：某地质研究院。", "")
        candidate = {"url": "https://example.edu.cn/jobs/company.html",
                     "title": "2027届岩土工程校园招聘"}
        job, reason = monitor.extract_automatic_job(
            candidate, detail_response(candidate["url"], body=body, title=title), DETAIL_NOW)
        self.assertIsNone(reason)
        self.assertEqual(job["organization"], "华北工程设计有限公司")

    def test_hebut_main_article_excludes_other_recruitment_and_right_sidebar(self):
        title = "某工程有限公司岩土科研人员招聘公告"
        body = DETAIL_BODY.replace(DETAIL_TITLE, title).replace(
            "招聘单位：某地质研究院。", "招聘单位：某工程有限公司。").replace(
            "招聘对象：2027届毕业生。", "").replace(
            "应聘者须于2027年7月31日前取得博士学历学位证书。", "")
        html = ("<html><head><title>" + title + "-河北工业大学就业平台</title></head><body>"
                "<div class='zhaopinxx_con1'><h5>" + title + "</h5></div>"
                "<div class='zhaopinxx_con2'><p>" + body.replace("\n", "</p><p>") + "</p></div>"
                "<div class='zhaopinxx_con3'><h2>其他招聘</h2>"
                "<p>另一单位面向2027届毕业生，2027年12月31日前取得学位即可。</p></div>"
                "<div class='s_right'>推荐招聘：2027届博士绿色通道。</div></body></html>")
        candidate = {"url": "https://career.hebut.edu.cn/home/correcruit/content/id/79815.html", "title": title}
        job, reason = monitor.extract_automatic_job(candidate, html_fixture_response(candidate["url"], html), DETAIL_NOW)
        self.assertIsNone(reason)
        self.assertEqual(job["organization"], "某工程有限公司")
        self.assertNotIn("2027届毕业生", job["eligibility2027"])
        self.assertNotIn("2027年12月31日", json.dumps(job["autoEvidence"], ensure_ascii=False))
        self.assertNotIn("另一单位", json.dumps(job, ensure_ascii=False))

    def test_existing_automatic_policy_is_archived_not_deleted_and_manual_protected(self):
        self.queue_detail()
        seeded = self.run_details()
        auto = copy.deepcopy(self.automatic_jobs(seeded)[0])
        auto["ingestionMethod"] = "official_body_rules_v1"
        auto["autoEvidence"]["extractionVersion"] = "official_body_rules_v1"
        auto["position"] = "第三章 博士后的招收"
        auto["announcementUrl"] = "https://example.edu.cn/jobs/policy.html"
        auto["recruitmentUrl"] = auto["announcementUrl"]
        auto["links"] = [{"url": auto["announcementUrl"], "label": "旧自动正文"}]
        manual = copy.deepcopy(self.data["jobs"][0])
        manual.update(id="MANUAL-POLICY", announcementUrl=auto["announcementUrl"],
                      recruitmentUrl=auto["announcementUrl"], deadline=None, deadlineTime=None)
        self.data = sample()
        self.data["jobs"].extend([auto, manual])
        self.data["stats"]["nationalAdded"] = 821
        self.data["cloudCandidates"] = []
        self.path.write_text(json.dumps(self.data), encoding="utf-8")
        corrected = self.run_details(lambda url: html_fixture_response(url, CUMT_POLICY_HTML))
        self.assertFalse(any(j["id"] == auto["id"] for j in corrected["jobs"]))
        self.assertTrue(any(j["id"] == "MANUAL-POLICY" for j in corrected["jobs"]))
        saved_manual = next(j for j in corrected["jobs"] if j["id"] == "MANUAL-POLICY")
        self.assertEqual(saved_manual["verifiedAt"], manual["verifiedAt"])
        archive = corrected["cloudJobArchive"]
        self.assertEqual(len(archive), 1)
        self.assertEqual(archive[0]["job"], auto)
        self.assertEqual(archive[0]["reason"], "policy_document")
        self.assertEqual(corrected["cloudUpdate"]["autoArchived"], 1)
        self.assertEqual(corrected["stats"]["nationalAdded"], 820)
        second = self.run_details(lambda url: html_fixture_response(url, CUMT_POLICY_HTML),
                                  now=datetime(2026, 11, 22, 1, tzinfo=timezone.utc))
        self.assertEqual(second["cloudJobArchive"], archive)
        self.assertEqual(second["cloudUpdate"]["autoArchived"], 0)
        self.assertEqual(second["stats"]["nationalAdded"], 820)
        # Isolate archive dedup from the manual job URL guard tested above.
        archive_only = sample()
        archive_only["cloudJobArchive"] = copy.deepcopy(archive)
        self.path.write_text(json.dumps(archive_only), encoding="utf-8")
        detail_calls = []
        monitor.run_update(self.path, self.path, self.evidence, self.db,
                           now=datetime(2026, 11, 23, 1, tzinfo=timezone.utc), max_pages=1,
                           max_detail_pages=30, fetcher=lambda url: fake_fetch(url, links=[
                               {"url": auto["announcementUrl"], "title": DETAIL_TITLE}]),
                           detail_fetcher=lambda url: detail_calls.append(url) or detail_response(url))
        self.assertEqual(self.read()["cloudUpdate"]["newCandidates"], 0)
        self.assertEqual(detail_calls, [])
        self.assertEqual(self.read()["cloudJobArchive"], archive)

    def test_fifteen_legacy_automatic_policy_records_migrate_without_losing_evidence(self):
        self.queue_detail()
        seeded = self.run_details()
        template = self.automatic_jobs(seeded)[0]
        self.data = sample()
        original_records = []
        for index in range(15):
            job = copy.deepcopy(template)
            url = "https://example.edu.cn/jobs/legacy-policy-" + str(index) + ".html"
            job.update(id="AUTO-LEGACY-" + str(index), announcementUrl=url, recruitmentUrl=url,
                       position="第三章 博士后的招收", ingestionMethod="official_body_rules_v1",
                       links=[{"url": url, "label": "旧自动正文"}])
            job["autoEvidence"]["extractionVersion"] = "official_body_rules_v1"
            original_records.append(copy.deepcopy(job))
            self.data["jobs"].append(job)
        self.data["stats"]["nationalAdded"] = 835
        self.path.write_text(json.dumps(self.data), encoding="utf-8")
        result = self.run_details(lambda url: html_fixture_response(url, CUMT_POLICY_HTML), limit=30)
        self.assertEqual(result["cloudUpdate"]["autoArchived"], 15)
        self.assertEqual(result["stats"]["nationalAdded"], 820)
        self.assertEqual(len(result["jobs"]), 1)
        archived = {item["job"]["id"]: item["job"] for item in result["cloudJobArchive"]}
        self.assertEqual(archived, {item["id"]: item for item in original_records})
        self.assertEqual(result["jobs"][0]["verifiedAt"], "2026-10-29")

    def test_legacy_migration_priority_then_new_cohorts_without_starvation(self):
        self.queue_detail()
        seeded = self.run_details()
        old = self.automatic_jobs(seeded)[0]
        old["ingestionMethod"] = "official_body_rules_v1"
        old["autoEvidence"]["extractionVersion"] = "official_body_rules_v1"
        seeded["cloudCandidates"] = []
        for year in (2025, 2026, 2027):
            seeded["cloudCandidates"].append({
                "id": "Q" + str(year), "url": "https://example.edu.cn/jobs/queue-" + str(year) + ".html",
                "title": "某地质研究院" + str(year) + "届岩土工程校园招聘公告",
                "sourceUrl": self.data["jobs"][0]["announcementUrl"], "sourceJobIds": ["NI001"],
                "reviewStatus": "queued", "discoveredAt": "2026-11-22T08:00:00+08:00"})
        self.path.write_text(json.dumps(seeded), encoding="utf-8")
        calls = []
        def detail(url):
            calls.append(url)
            if "queue-" in url:
                year = url.split("queue-")[1][:4]
                title = "某地质研究院" + year + "届岩土工程校园招聘公告"
                return detail_response(url, body=DETAIL_BODY.replace(DETAIL_TITLE, title), title=title)
            return detail_response(url)
        for day in range(22, 26):
            self.run_details(detail, now=datetime(2026, 11, day, 1, tzinfo=timezone.utc), limit=1)
        self.assertEqual(calls, [old["announcementUrl"],
                               "https://example.edu.cn/jobs/queue-2027.html",
                               "https://example.edu.cn/jobs/queue-2026.html",
                               "https://example.edu.cn/jobs/queue-2025.html"])
        refreshed = next(j for j in self.read()["jobs"] if j["id"] == old["id"])
        self.assertEqual(refreshed["autoEvidence"]["extractionVersion"], monitor.AUTO_VERSION)

    def test_historical_transition_updates_status_atomically_without_reopening(self):
        self.queue_detail()
        seeded = self.run_details()
        original_id = self.automatic_jobs(seeded)[0]["id"]
        title = "某地质研究院2025届岩土工程校园招聘公告"
        body = DETAIL_BODY.replace(DETAIL_TITLE, title).replace("2027届毕业生", "2025届毕业生")
        changed = self.run_details(lambda url: detail_response(url, body=body, title=title),
                                   now=datetime(2026, 11, 22, 1, tzinfo=timezone.utc))
        job = next(j for j in changed["jobs"] if j["id"] == original_id)
        self.assertEqual(job["availability"], "historical")
        self.assertEqual(job["availabilityCode"], "historical")
        self.assertIn("旧届", job["currentStatus"])
        self.assertNotIn("窗口或部分条件未明确", job["currentStatus"])

        # Reproduce the real v2 artifact: historical code/text were not synchronized.
        job["availabilityCode"] = "monitoring"
        job["currentStatus"] = "自动采集：相关招聘正文已入库，开放窗口或部分条件未明确"
        job["ingestionMethod"] = "official_body_rules_v2"
        job["autoEvidence"]["extractionVersion"] = "official_body_rules_v2"
        self.path.write_text(json.dumps(changed), encoding="utf-8")
        repaired = self.run_details(lambda url: detail_response(url, body=body, title=title),
                                    now=datetime(2026, 11, 23, 1, tzinfo=timezone.utc))
        fixed = next(j for j in repaired["jobs"] if j["id"] == original_id)
        self.assertEqual(fixed["availabilityCode"], "historical")
        self.assertIn("旧届", fixed["currentStatus"])
        self.assertEqual(fixed["autoEvidence"]["extractionVersion"], monitor.AUTO_VERSION)
        self.assertEqual(fixed["verifiedAt"], "")

        reopened = self.run_details(now=datetime(2026, 11, 24, 1, tzinfo=timezone.utc))
        still_historical = next(j for j in reopened["jobs"] if j["id"] == original_id)
        self.assertEqual(still_historical["availability"], "historical")
        self.assertEqual(still_historical["availabilityCode"], "historical")
        self.assertEqual(still_historical["currentStatus"], fixed["currentStatus"])

        current_title = "某地质研究院2026年校园招聘公告"
        current_body = DETAIL_BODY.replace(DETAIL_TITLE, current_title).replace(
            "报名时间：2026年11月20日至2026年12月10日17:00。", "").replace(
            "报名截止时间：2026年12月10日17:00。", "")
        for past_deadline, expected in ((None, "monitoring"), ("2026-11-01", "historical")):
            with self.subTest(past_deadline=past_deadline):
                legacy = copy.deepcopy(reopened)
                old_auto = next(j for j in legacy["jobs"] if j["id"] == original_id)
                old_auto.update(position=current_title, availability="historical",
                                availabilityCode="historical", currentStatus="旧版按年份归档",
                                deadline=past_deadline, deadlineTime=None,
                                ingestionMethod="official_body_rules_v2")
                old_auto["autoEvidence"]["extractionVersion"] = "official_body_rules_v2"
                fields = old_auto["autoEvidence"]["fields"]
                fields.pop("deadline", None)
                fields.pop("deadlineTime", None)
                if past_deadline:
                    fields["deadline"] = {"value": past_deadline, "quote": "报名截止时间：2026年11月1日"}
                fields["cohortExpiry"] = {
                    "value": {"cohortYear": 2026, "targetYear": 2027,
                              "basis": "explicit_old_campus_or_spring_cycle"},
                    "quote": current_title}
                self.path.write_text(json.dumps(legacy), encoding="utf-8")
                corrected = self.run_details(
                    lambda url: detail_response(url, body=current_body, title=current_title),
                    now=datetime(2026, 11, 25, 1, tzinfo=timezone.utc))
                corrected_job = next(j for j in corrected["jobs"] if j["id"] == original_id)
                self.assertEqual(corrected_job["availability"], expected)
                self.assertEqual(corrected_job["availabilityCode"], expected)
                if past_deadline is None:
                    self.assertIn("恢复监测", corrected_job["currentStatus"])
                    self.assertEqual(corrected_job["autoEvidence"]["algorithmCorrection"]["kind"],
                                     "v2_current_year_campus_false_expiry")
                else:
                    self.assertNotIn("algorithmCorrection", corrected_job["autoEvidence"])

        for current in ("某院2026年校园招聘", "某院2026秋季校园招聘"):
            with self.subTest(title=current):
                self.assertIsNone(monitor.title_cohort_expiry(current, DETAIL_NOW))
        for expired in ("某院2024应届毕业生招聘", "某院2025校园招聘",
                        "某院2026春季校园招聘", "某院2026届校园招聘"):
            with self.subTest(title=expired):
                self.assertIsNotNone(monitor.title_cohort_expiry(expired, DETAIL_NOW))

    def test_province_rotation_and_urgent_priority(self):
        jobs = []
        for i, province in enumerate(("浙江", "湖北", "四川", "陕西", "广东")):
            job = copy.deepcopy(self.data["jobs"][0])
            job.update(id=str(i), province=province,
                       announcementUrl="https://example.edu.cn/jobs/" + str(i), links=[])
            jobs.append(job)
        data = {"jobs": jobs}
        selected, cursor = monitor.select_pages(data, datetime(2026, 11, 13, tzinfo=timezone.utc), 2)
        data["cloudUpdate"] = {"provinceCursor": cursor, "urlState": {}}
        second, _ = monitor.select_pages(data, datetime(2026, 11, 13, tzinfo=timezone.utc), 2)
        self.assertNotEqual(selected[0]["province"], second[0]["province"])


if __name__ == "__main__":
    unittest.main()