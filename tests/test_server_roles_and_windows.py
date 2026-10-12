"""HTTP-level wiring of 1.30.0: node roles on /api/status and /api/slowlog-impact, repaired Binlog windows on /api/analytics."""
import json
import threading
import time
import unittest
import urllib.error
import urllib.request
from types import SimpleNamespace
from unittest.mock import patch

from app.config import Settings
from app.live_sessions import LiveSessionsError
from app.server import Application, AppHTTPServer
from app.window_align import HOUR_US, MINUTE_US, floor_to

STEP = 5 * MINUTE_US
SETTINGS = Settings(retention_days=60, oss_retention_days=60)


class Metadata:
    @staticmethod
    def load_settings():
        return SETTINGS

    @staticmethod
    def storage_metadata_stats():
        return {"event_count": 1, "parquet_bytes": 1, "archived_bytes": 1, "oldest_epoch_us": 1, "latest_epoch_us": 2,
                "part_count": 1, "archived_part_count": 1, "files": {}}


class Sync:
    @staticmethod
    def archive_for_settings(_settings):
        return None

    @staticmethod
    def status():
        return {"running": False, "latestJob": {}, "index": {}}


def get(application, path):
    httpd = AppHTTPServer(("127.0.0.1", 0), application)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{httpd.server_address[1]}{path}", timeout=5) as response:
            return json.loads(response.read().decode("utf-8"))
    finally:
        httpd.shutdown()
        httpd.server_close()
        thread.join(timeout=2)


def get_error(application, path):
    try:
        get(application, path)
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read().decode("utf-8"))
    raise AssertionError("the request was expected to fail")


class StatusRoleTests(unittest.TestCase):
    def test_status_labels_each_collected_node_with_its_current_role_and_zone(self):
        def collector(node):
            return SimpleNamespace(status=lambda: {"instanceId": "rm-prod", "nodeId": node, "label": "demo-main"})
        application = Application.__new__(Application)
        application.metadata, application.storage, application.sync = Metadata(), SimpleNamespace(), Sync()
        application.slow_logs = [collector("rn-1"), collector("rn-2"), collector("")]
        application.node_roles = SimpleNamespace(lookup=lambda instance: (
            {"rn-1": {"role": "primary", "zone": "cn-chengdu-a", "status": "active"}} if instance == "rm-prod" else {}))
        with patch("app.server.credential_status", return_value={"configured": True}):
            slow = application.public_status()["slowLogs"]
        self.assertEqual([(item["nodeId"], item["role"], item["zone"]) for item in slow],
                         [("rn-1", "primary", "cn-chengdu-a"), ("rn-2", "", ""), ("", "", "")])
        self.assertEqual(slow[0]["label"], "demo-main")  # the collector's own fields are untouched


class ImpactEndpointTests(unittest.TestCase):
    def test_roles_lookup_and_the_new_order_reach_the_analysis(self):
        captured = {}

        def fake_query(_metadata, _backend, query, _settings, **kwargs):
            captured.update(query=dict(query), kwargs=kwargs)
            return {"status": "ok", "nodes": []}

        roles = {"rn-1": {"role": "primary", "zone": "cn-chengdu-a"}}
        application = SimpleNamespace(metadata=Metadata(), sync=Sync(), storage=SimpleNamespace(clickhouse_slowlog_backend=object()),
                                      node_roles=SimpleNamespace(lookup=lambda instance: roles if instance == "rm-x" else {}))
        with patch("app.server.query_resource_overlap", fake_query):
            payload = get(application, "/api/slowlog-impact?source=slowlog&instance=rm-x&order=growth&metric=iops"
                                       "&startEpochUs=1775000000000000&endEpochUs=1775003600000000")
        self.assertTrue(payload["ok"])
        self.assertEqual((captured["query"]["impact_order"], captured["query"]["metric"]), ("growth", "iops"))
        self.assertEqual(captured["kwargs"]["roles"]("rm-x"), roles)
        # the old callers' spellings keep working: executions (alert cards) and correlation
        for order in ("executions", "correlation"):
            with patch("app.server.query_resource_overlap", fake_query):
                get(application, f"/api/slowlog-impact?source=slowlog&instance=rm-x&order={order}")
            self.assertEqual(captured["query"]["impact_order"], order)


class LiveSessionsEndpointTests(unittest.TestCase):
    @staticmethod
    def serve(snapshot):
        collectors = [SimpleNamespace(config=SimpleNamespace(instance_id="rm-x", node_id="rn-1")),
                      SimpleNamespace(config=SimpleNamespace(instance_id="rm-x", node_id=""))]
        return SimpleNamespace(metadata=Metadata(), sync=Sync(), slow_logs=collectors, live_sessions=SimpleNamespace(snapshot=snapshot))

    def test_a_collected_node_is_read_and_returned(self):
        calls = []

        def snapshot(instance, node):
            calls.append((instance, node))
            return {"status": "ok", "sessions": []}
        payload = get(self.serve(snapshot), "/api/live-sessions?instance=rm-x&nodeId=rn-1")
        self.assertEqual((payload["ok"], payload["data"]["status"], calls), (True, "ok", [("rm-x", "rn-1")]))

    def test_only_nodes_whose_slow_log_is_collected_here_can_be_read(self):
        calls = []
        application = self.serve(lambda instance, node: calls.append((instance, node)))
        for path in ("/api/live-sessions?instance=rm-x&nodeId=rn-other", "/api/live-sessions?instance=rm-other&nodeId=rn-1",
                     "/api/live-sessions?instance=rm-x", "/api/live-sessions"):
            status, body = get_error(application, path)
            self.assertEqual((status, body["error"]["code"]), (400, "LIVE_SESSIONS_UNKNOWN_NODE"))
        self.assertEqual(calls, [])   # DAS is never asked about a node this service does not collect

    def test_a_missing_permission_and_a_failing_api_are_different_errors(self):
        for code, status in (("das_permission_denied", 403), ("das_timeout", 502)):
            def snapshot(_instance, _node, code=code):
                raise LiveSessionsError(code, "reason " + code)
            got, body = get_error(self.serve(snapshot), "/api/live-sessions?instance=rm-x&nodeId=rn-1")
            self.assertEqual((got, body["error"]["code"], body["error"]["message"]), (status, code.upper(), "reason " + code))


class BinlogWindowEndpointTests(unittest.TestCase):
    def serve(self, binlog_rows):
        seen = {}

        class Storage:
            @staticmethod
            def analytics_summary(query, _settings, _archive, **_kwargs):
                seen.update(query)
                return {"coverage": {"complete": True}, "sql": {}}

        Storage.binlog_rows = binlog_rows
        return SimpleNamespace(metadata=Metadata(), sync=Sync(), storage=Storage), seen

    def test_a_missing_tail_is_cut_back_before_the_query_runs_and_reported(self):
        now = int(time.time() * 1_000_000)
        edge = now - 12 * MINUTE_US
        calls = []
        rows = SimpleNamespace(coverage=lambda instance, lo, hi: calls.append((instance, lo, hi)) or {"intervals": [[now - 3 * HOUR_US, edge]]})
        application, seen = self.serve(rows)
        start = now - 2 * HOUR_US
        payload = get(application, f"/api/analytics?source=binlog&instance=rm-x&startEpochUs={start}&endEpochUs={now}")["data"]
        cut = floor_to(edge, STEP)
        self.assertEqual((seen["start_epoch_us"], seen["end_epoch_us"]), (start, cut))
        self.assertEqual(payload["adjusted"]["kind"], "truncated")
        self.assertEqual(payload["adjusted"]["truncated_us"], now - cut)
        self.assertEqual(payload["adjusted"]["reason"], "incomplete_binlog_coverage")
        self.assertEqual(payload["ready_through_us"], edge)
        self.assertEqual([c[0] for c in calls], ["rm-x"])

    def test_a_covered_window_runs_as_asked_and_still_reports_how_far_behind_the_row_store_is(self):
        now = int(time.time() * 1_000_000)
        edge = now - 8 * MINUTE_US
        application, seen = self.serve(SimpleNamespace(coverage=lambda *_a: {"intervals": [[now - 3 * HOUR_US, edge]]}))
        start, end = now - 2 * HOUR_US, now - 30 * MINUTE_US
        payload = get(application, f"/api/analytics?source=binlog&instance=rm-x&startEpochUs={start}&endEpochUs={end}")["data"]
        self.assertEqual((seen["start_epoch_us"], seen["end_epoch_us"]), (start, end))
        self.assertIsNone(payload["adjusted"])
        self.assertEqual(payload["ready_through_us"], edge)

    def test_without_the_row_store_the_window_is_never_touched(self):
        application, seen = self.serve(None)
        now = int(time.time() * 1_000_000)
        payload = get(application, f"/api/analytics?source=binlog&instance=rm-x&startEpochUs={now - HOUR_US}&endEpochUs={now}")["data"]
        self.assertEqual((seen["start_epoch_us"], seen["end_epoch_us"]), (now - HOUR_US, now))
        self.assertIsNone(payload["adjusted"])
        self.assertIsNone(payload["ready_through_us"])

    def test_the_slow_log_analytics_response_is_left_exactly_as_before(self):
        application, _seen = self.serve(SimpleNamespace(coverage=lambda *_a: self.fail("slow-log analytics must not read Binlog coverage")))
        now = int(time.time() * 1_000_000)
        payload = get(application, f"/api/analytics?source=slowlog&instance=rm-x&startEpochUs={now - HOUR_US}&endEpochUs={now}")["data"]
        self.assertNotIn("adjusted", payload)
        self.assertNotIn("ready_through_us", payload)


if __name__ == "__main__":
    unittest.main()
