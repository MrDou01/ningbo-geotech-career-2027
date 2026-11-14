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

    def run_monitor(self, now, fetcher=fake_fetch, max_pages=80):
        return monitor.run_update(self.path, self.path, self.evidence, self.db,
                                  max_pages=max_pages, now=now, fetcher=fetcher)

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

    def test_candidates_are_isolated_same_host_pending(self):
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
        self.assertEqual(candidate["reviewStatus"], "pending")
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