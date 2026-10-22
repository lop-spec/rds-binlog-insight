"""The live tail of the slow log: what it keeps, what it vouches for, and the switch that turns it off."""
from __future__ import annotations

import os
import time
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app import slowlog_tail
from app.slow_log_collector import SlowLogConfig
from app.slowlog_tail import (FAILURE_LOG_EVERY, MAX_WINDOW_SECONDS, MIN_WINDOW_SECONDS, PUBLISH_GUARD_SECONDS, RETAIN_SECONDS,
                              STALE_SECONDS, NodeTail, SlowLogTails, scope_matches)

NOW = 1_790_000_000.0  # seconds
US = 1_000_000


class Clock:
    def __init__(self, now: float = NOW) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


class FakeDas:
    """DAS as it answers: newest first, 100 per page, TotalRecords of the window; a record shows up once it is published."""

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self.records: list[tuple[float, dict]] = []  # (published_at, record)
        self.calls: list[dict] = []
        self.fail = False

    def add(self, start_s: float, *, duration_ms: int = 200, published_after: float = 40.0, sql: str | None = None,
            thread: int | None = None, node: str = "rn-prod-01", examined: int = 0, table: str = "orders",
            database: str = "shop", account: str = "app") -> dict:
        number = len(self.records)
        record = {"Timestamp": int(start_s * 1000), "QueryTime": duration_ms, "RowsExamined": examined, "RowsSent": 1, "LockTime": 0,
                  "SqlId": f"sql-{number % 5}", "ThreadId": thread if thread is not None else 1000 + number, "AccountName": account,
                  "HostAddress": "10.0.0.8", "DBName": database, "TableName": table, "SqlType": "select", "NodeId": node,
                  "SQLText": sql or f"SELECT * FROM {table} WHERE id = {number % 5}"}
        self.records.append((start_s + duration_ms / 1000 + published_after, record))
        return record

    def call(self, _action: str, params: dict) -> dict:
        self.calls.append(dict(params))
        if self.fail:
            raise RuntimeError("DAS 网络错误：down")
        low, high = int(params["StartTime"]), int(params["EndTime"])
        visible = [record for published, record in self.records if published <= self.clock.now and low <= record["Timestamp"] <= high]
        visible.sort(key=lambda record: record["Timestamp"], reverse=True)
        size, page = int(params["PageSize"]), int(params["PageNumber"])
        return {"Data": {"Logs": [dict(record) for record in visible[(page - 1) * size: page * size]], "TotalRecords": len(visible),
                         "NodeId": "rn-prod-01"}}


def config(node: str = "rn-prod-01", instance: str = "rm-prod") -> SlowLogConfig:
    return SlowLogConfig({"instanceId": instance, "nodeId": node, "enabled": True})


def make(clock: Clock | None = None, node: str = "rn-prod-01") -> tuple[NodeTail, FakeDas, Clock]:
    clock = clock or Clock()
    das = FakeDas(clock)
    return NodeTail(config(node), lambda: das, clock=clock), das, clock


def busy(das: FakeDas, count: int = 5200) -> None:
    """Several statements a second over the first 15 minute read: a node whose window has to be the short one."""
    for number in range(count):
        das.add(NOW - 880 + number * 0.15, published_after=1.0)


class ScanTests(unittest.TestCase):
    def test_the_first_scan_reads_fifteen_minutes_and_keeps_what_the_analysis_needs(self) -> None:
        tail, das, _clock = make()
        das.add(NOW - 800, examined=500)
        das.add(NOW - 100, duration_ms=3000)
        das.add(NOW - 1000)  # older than the 15 minute backfill
        self.assertEqual(tail.scan(), 2)
        low, high = das.calls[0]["StartTime"], das.calls[0]["EndTime"]
        self.assertEqual((high - low) // 1000, 900)
        rows = {row["start_us"]: row for row in tail.rows(0, 2**62, {})}
        self.assertEqual(sorted(rows), [int((NOW - 800) * US), int((NOW - 100) * US)])
        old, new = rows[int((NOW - 800) * US)], rows[int((NOW - 100) * US)]
        self.assertEqual((old["rows_examined"], new["rows_examined"]), (500, None))  # a zero cost is "not reported", like ClickHouse
        self.assertEqual((new["duration_ms"], new["node_id"], new["database_name"], new["account"]), (3000, "rn-prod-01", "shop", "app"))
        self.assertTrue(new["fingerprint"] and new["event_id"].startswith("slow-"))
        view = tail.view()
        self.assertEqual(view["covered_from_us"], int((NOW - 900) * US))
        self.assertEqual(view["complete_through_us"], int((NOW - PUBLISH_GUARD_SECONDS) * US))
        self.assertEqual(view["newest_us"], int((NOW - 100) * US))

    def test_a_statement_published_late_is_found_by_a_later_scan_and_never_twice(self) -> None:
        tail, das, clock = make()
        tail.scan()
        das.add(NOW - 30, published_after=70.0)  # visible from NOW + 40 s
        clock.now = NOW + 20
        self.assertEqual(tail.scan(), 0)
        clock.now = NOW + 45
        self.assertEqual(tail.scan(), 1)
        clock.now = NOW + 65
        self.assertEqual(tail.scan(), 0)  # same event id: kept once
        self.assertEqual(len(tail.rows(0, 2**62, {})), 1)

    def test_the_window_follows_the_record_rate(self) -> None:
        tail, das, clock = make()
        busy(das)
        tail.scan()
        self.assertEqual(tail.status()["windowSeconds"], MIN_WINDOW_SECONDS)
        quiet, das2, _clock2 = make()
        das2.add(NOW - 100)
        quiet.scan()
        self.assertEqual(quiet.status()["windowSeconds"], MAX_WINDOW_SECONDS)
        clock.now = NOW + 20  # and the next scan of the busy node really reads the short window
        tail.scan()
        low, high = das.calls[-1]["StartTime"], das.calls[-1]["EndTime"]
        self.assertEqual((high - low) // 1000, MIN_WINDOW_SECONDS)

    def test_old_events_and_their_statement_texts_are_dropped_after_the_retention(self) -> None:
        tail, das, clock = make()
        das.add(NOW - 600, sql="SELECT 1 FROM only_old WHERE a = 1", published_after=1.0)
        tail.scan()
        first = tail.rows(0, 2**62, {})
        self.assertEqual(len(first), 1)
        fingerprint = first[0]["fingerprint"]
        self.assertIn("only_old", tail.profiles([fingerprint])[fingerprint]["normalized_sql"].lower())
        clock.now = NOW + RETAIN_SECONDS + 700
        das.add(clock.now - 50, published_after=1.0)
        clock.now += 2
        tail.scan()
        self.assertEqual(len(tail.rows(0, 2**62, {})), 1)  # only the new one
        self.assertEqual(tail.profiles([fingerprint]), {})  # nothing keeps the old text alive any more
        self.assertGreaterEqual(tail.view()["covered_from_us"], int((clock.now - RETAIN_SECONDS) * US))

    def test_past_the_event_cap_the_oldest_go_and_the_coverage_starts_after_them(self) -> None:
        tail, das, _clock = make()
        for number in range(30):
            das.add(NOW - 600 + number * 10, published_after=1.0)
        with patch.object(slowlog_tail, "MAX_EVENTS", 20):
            tail.scan()
        rows = sorted(tail.rows(0, 2**62, {}), key=lambda row: row["start_us"])
        self.assertEqual(len(rows), 20)
        self.assertEqual(rows[0]["start_us"], int((NOW - 600 + 10 * 10) * US))  # the ten oldest were dropped
        self.assertGreater(tail.view()["covered_from_us"], int((NOW - 600 + 9 * 10) * US))  # and are not vouched for

    def test_kept_statement_texts_are_bounded(self) -> None:
        tail, das, _clock = make()
        das.add(NOW - 100, sql="SELECT * FROM orders WHERE note = '" + "x" * 9000 + "'", published_after=1.0)
        tail.scan()
        fingerprint = tail.rows(0, 2**62, {})[0]["fingerprint"]
        profile = tail.profiles([fingerprint])[fingerprint]
        self.assertLessEqual(len(profile["sample_sql"]), slowlog_tail.SQL_KEEP_CHARS)
        self.assertLessEqual(len(profile["normalized_sql"]), slowlog_tail.SQL_KEEP_CHARS)

    def test_a_node_whose_scans_stopped_vouches_for_nothing(self) -> None:
        tail, _das, clock = make()
        tail.scan()
        self.assertIsNotNone(tail.view())
        clock.now = NOW + STALE_SECONDS + 1
        self.assertIsNone(tail.view())

    def test_a_pause_is_caught_up_by_reading_back_further_and_the_coverage_stays_one_stretch(self) -> None:
        tail, das, clock = make()
        busy(das, 6700)  # keeps coming for the next two minutes, so the node stays busy and its window stays the short one
        tail.scan()
        clock.now = NOW + 15
        tail.scan()
        self.assertEqual(tail.status()["windowSeconds"], MIN_WINDOW_SECONDS)
        late = das.add(NOW + 40, published_after=30.0)  # published while the scans pause
        clock.now = NOW + 15 + 100  # more than window - guard apart: a plain 150 s window could miss what was published meanwhile
        tail.scan()
        low, high = das.calls[-1]["StartTime"], das.calls[-1]["EndTime"]
        self.assertGreater((high - low) // 1000, MIN_WINDOW_SECONDS)  # it read back across the pause
        self.assertEqual(tail.view()["covered_from_us"], int((NOW - 900) * US))
        self.assertIn(int(late["Timestamp"]) * 1000, [row["start_us"] for row in tail.rows(0, 2**62, {})])

    def test_a_pause_longer_than_the_backfill_restarts_the_coverage_at_the_new_window(self) -> None:
        tail, das, clock = make()
        busy(das, 6700)
        tail.scan()
        clock.now = NOW + 15 + 1000
        tail.scan()
        self.assertEqual(tail.view()["covered_from_us"], int((clock.now - 900) * US))

    def test_a_read_cut_by_the_page_cap_only_vouches_for_what_it_reached(self) -> None:
        tail, das, _clock = make()
        for number in range(450):
            das.add(NOW - 20 - number, published_after=1.0)  # one per second, the newest 20 s ago
        with patch.object(slowlog_tail, "MAX_PAGES", 2):
            tail.scan()
        self.assertEqual(tail.view()["covered_from_us"], int((NOW - 20 - 199) * US))  # two pages = the newest 200 records
        self.assertEqual(len(tail.rows(0, 2**62, {})), 200)

    def test_excluded_accounts_are_left_out_like_the_collector_does(self) -> None:
        clock = Clock()
        das = FakeDas(clock)
        excluding = SlowLogConfig({"instanceId": "rm-prod", "nodeId": "rn-prod-01", "enabled": True, "excludeAccounts": "monitor"})
        tail = NodeTail(excluding, lambda: das, clock=clock)
        das.add(NOW - 100, account="monitor", published_after=1.0)
        das.add(NOW - 90, account="app", published_after=1.0)
        self.assertEqual(tail.scan(), 1)

    def test_a_failing_read_raises_and_is_logged_first_and_then_every_so_often(self) -> None:
        tail, das, clock = make()
        tail.scan()
        das.fail = True
        clock.now += 20
        with self.assertRaises(RuntimeError):
            tail.scan()
        with patch.object(slowlog_tail.LOGGER, "warning") as warning:
            for _ in range(FAILURE_LOG_EVERY):
                tail.note_failure(RuntimeError("DAS 网络错误：down"))
        self.assertEqual(warning.call_count, 2)  # the first failure of the streak, and the FAILURE_LOG_EVERY-th
        self.assertEqual((tail.status()["failures"], tail.status()["lastError"]), (FAILURE_LOG_EVERY, "RuntimeError: DAS 网络错误：down"))
        das.fail = False
        clock.now += 20
        tail.scan()
        self.assertEqual(tail.status()["lastError"], "")
        with self.assertLogs("app.slowlog_tail", "WARNING") as logs:
            tail.note_failure(RuntimeError("again"))
        self.assertIn("(1 in a row)", logs.output[0])  # a new streak logs again

    def test_the_client_is_reused_between_scans_and_rebuilt_after_a_failure(self) -> None:
        clock = Clock()
        das = FakeDas(clock)
        built = []
        tail = NodeTail(config(), lambda: built.append(1) or das, clock=clock)
        tail.scan()
        clock.now += 20
        tail.scan()
        self.assertEqual(len(built), 1)
        das.fail = True
        clock.now += 20
        with self.assertRaises(RuntimeError):
            tail.scan()
        das.fail = False
        clock.now += 20
        tail.scan()
        self.assertEqual(len(built), 2)


class ScopeTests(unittest.TestCase):
    def test_the_filters_are_the_ones_clickhouse_applies(self) -> None:
        tail, das, _clock = make()
        das.add(NOW - 50, published_after=1.0, table="orders, Order_Items", database="Shop")
        tail.scan()
        join = tail.event(tail.rows(0, 2**62, {})[0]["event_id"])
        self.assertTrue(scope_matches(join, {}))
        self.assertTrue(scope_matches(join, {"node_id": "rn-prod-01", "database": "shop", "table": "order_items", "operation": "select"}))
        self.assertFalse(scope_matches(join, {"node_id": "rn-prod-02"}))
        self.assertFalse(scope_matches(join, {"database": "other"}))
        self.assertFalse(scope_matches(join, {"table": "order"}))  # a name of the list, not a substring
        self.assertFalse(scope_matches(join, {"operation": "update"}))
        self.assertTrue(scope_matches(join, {"table": "orders, order_items"}))  # a value with a comma must be the whole list
        self.assertFalse(scope_matches(join, {"table": "orders,order_items"}))
        self.assertEqual(len(tail.rows(0, 2**62, {"database": "shop"})), 1)
        self.assertEqual(tail.rows(0, 2**62, {"database": "other"}), [])
        self.assertEqual(tail.rows(0, int((NOW - 60) * US), {}), [])  # the range bounds apply too


class ServiceTests(unittest.TestCase):
    @staticmethod
    def collectors(clock: Clock, *nodes: str, instance: str = "rm-prod"):
        built = {node: FakeDas(clock) for node in nodes}
        return [SimpleNamespace(config=config(node, instance), das_client=lambda das=built[node]: das) for node in nodes], built

    def test_the_instance_view_is_the_intersection_of_its_nodes_and_needs_every_node(self) -> None:
        clock = Clock()
        collectors, _built = self.collectors(clock, "rn-1", "rn-2")
        tails = SlowLogTails(collectors, clock=clock)
        self.assertIsNone(tails.view("rm-prod"))  # no node has scanned yet
        tails._tails[0].scan()
        self.assertIsNone(tails.view("rm-prod"))  # one of two
        clock.now = NOW + 30
        tails._tails[1].scan()
        view = tails.view("rm-prod")
        self.assertEqual(view["covered_from_us"], int((NOW + 30 - 900) * US))  # the later start of the two
        self.assertEqual(view["complete_through_us"], int((NOW - PUBLISH_GUARD_SECONDS) * US))  # the older scan of the two
        self.assertEqual(view["nodes"], 2)
        self.assertIsNone(tails.view("rm-other"))
        clock.now = NOW + 30 + STALE_SECONDS  # the first node is stale now: the whole instance vouches for nothing
        self.assertIsNone(tails.view("rm-prod"))

    def test_details_and_statement_texts_are_served_to_the_page(self) -> None:
        clock = Clock()
        collectors, built = self.collectors(clock, "rn-1")
        built["rn-1"].add(NOW - 60, duration_ms=2500, published_after=1.0, sql="SELECT a FROM fresh_table WHERE id = 7", examined=9000)
        tails = SlowLogTails(collectors, clock=clock)
        tails._tails[0].scan()
        row = tails.rows("rm-prod", 0, 2**62, {})[0]
        detail = tails.event_detail(row["event_id"], "rm-prod")
        self.assertEqual((detail["raw_event_type"], detail["tiers_used"], detail["rows_examined"], detail["execution_time_ms"]),
                         ("SLOW_LOG", ["live-tail"], 9000, 2500))
        self.assertIn("fresh_table", detail["sql_text"])
        self.assertIsNone(tails.event_detail(row["event_id"], "rm-other"))
        self.assertIsNone(tails.event_detail("slow-unknown", "rm-prod"))
        key = ("rm-prod", row["fingerprint"])
        profiles = tails.profiles({key, ("rm-prod", "nope"), ("rm-other", row["fingerprint"])})
        self.assertEqual(list(profiles), [key])
        self.assertIn("fresh_table", profiles[key]["normalized_sql"].lower())
        self.assertEqual(profiles[key]["action"], "SELECT")

    def test_switched_off_there_are_no_threads_no_calls_and_no_view(self) -> None:
        collectors, built = self.collectors(Clock(), "rn-1")
        with patch.dict(os.environ, {slowlog_tail.ENV_SWITCH: "0"}):
            tails = SlowLogTails(collectors)
        self.assertFalse(tails.enabled)
        tails.start()
        self.assertEqual(tails._threads, [])
        self.assertIsNone(tails.view("rm-prod"))
        self.assertEqual((tails.rows("rm-prod", 0, 2**62, {}), tails.status()), ([], []))
        self.assertEqual(built["rn-1"].calls, [])
        with patch.dict(os.environ, {slowlog_tail.ENV_SWITCH: "1"}):
            self.assertTrue(SlowLogTails(collectors).enabled)

    def test_disabled_collectors_are_not_tailed(self) -> None:
        off = SimpleNamespace(config=SlowLogConfig({"instanceId": "rm-prod", "nodeId": "rn-1", "enabled": False}), das_client=Mock())
        self.assertEqual(SlowLogTails([off]).status(), [])

    def test_the_threads_scan_and_survive_a_failing_node(self) -> None:
        collectors, built = self.collectors(Clock(), "rn-1", "rn-2")
        built["rn-2"].fail = True
        tails = SlowLogTails(collectors, poll_seconds=0.02)
        with patch.object(slowlog_tail, "MIN_PAUSE_SECONDS", 0.02):
            tails.start()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                status = {item["nodeId"]: item for item in tails.status()}
                if status["rn-1"]["scans"] >= 2 and status["rn-2"]["failures"] >= 2:
                    break
                time.sleep(0.02)
            tails.shutdown()
        status = {item["nodeId"]: item for item in tails.status()}
        self.assertGreaterEqual(status["rn-1"]["scans"], 2)
        self.assertGreaterEqual(status["rn-2"]["failures"], 2)
        self.assertIn("down", status["rn-2"]["lastError"])


if __name__ == "__main__":
    unittest.main()
