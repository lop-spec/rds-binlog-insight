from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock, patch

from app.slow_log_collector import SlowLogCollector, SlowLogConfig, build_slow_events


def _epoch_us(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1_000_000)


def _record(start: str = "2026-08-30T16:24:54Z") -> dict[str, object]:
    return {
        "QueryStartTime": start,
        "SQLText": (
            "SELECT id, tenant_id, category_id, sku_id FROM inventory_skus "
            "WHERE 62012091 <= id AND id < 65659861"
        ),
        "SqlType": "select",
        "DBName": "example_app",
        "TableName": "inventory_skus",
        "QueryTime": 1_530_038,
        "RowsExamined": 3_643_736,
        "RowsSent": 3_643_736,
        "SqlId": "data-hub-export",
        "ThreadId": 9001,
        "AccountName": "batch_export",
        "HostAddress": "10.0.0.16",
        "NodeId": "rn-prod-01",
    }


class _Metadata:
    def load_settings(self):  # pragma: no cover - _client is replaced in tests
        raise AssertionError("unexpected credential lookup")


class _Storage:
    def __init__(self, root: Path) -> None:
        self.paths = {"root": root}
        self.existing: set[str] = set()
        self.lookup_calls: list[set[str]] = []

    def slowlog_existing_event_ids(self, event_ids, instance: str = "") -> set[str]:
        values = set(event_ids)
        self.lookup_calls.append(values)
        return values & self.existing


class _Client:
    def __init__(self, record: dict[str, object]) -> None:
        self.record = record
        self.calls: list[dict[str, object]] = []

    def call(self, _action: str, params: dict[str, object]) -> dict[str, object]:
        self.calls.append(dict(params))
        event_ms = _epoch_us(str(self.record["QueryStartTime"])) // 1000
        included = int(params["StartTime"]) <= event_ms <= int(params["EndTime"])
        rows = [dict(self.record)] if included else []
        return {
            "Data": {
                "Logs": rows,
                "TotalRecords": len(rows),
                "NodeId": self.record["NodeId"],
            }
        }


def _collector(root: Path, storage: _Storage, client: _Client) -> SlowLogCollector:
    collector = SlowLogCollector(
        _Metadata(),
        storage,
        SlowLogConfig(
            {
                "instanceId": "rm-prod",
                "nodeId": "rn-prod-01",
                "enabled": True,
                "lagSeconds": 60,
                "replaySeconds": 7200,
                "maxWindowMinutes": 120,
            }
        ),
        credential_loader=lambda _target: None,
    )
    collector._client = lambda: client  # type: ignore[method-assign]
    return collector


class SlowLogCollectorReliabilityTests(unittest.TestCase):
    def test_enqueue_retries_bounded_on_sqlite_lock_and_leaves_trace(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            storage = _Storage(Path(directory))
            collector = _collector(Path(directory), storage, _Client(_record()))
            locked = sqlite3.OperationalError("database is locked")
            storage.slowlog_index = Mock(  # type: ignore[attr-defined]
                enqueue_parts=Mock(side_effect=[locked, locked, 2])
            )
            parts = [{"path": "/data/events/a.parquet"}, {"path": "/data/events/b.parquet"}]

            with patch("app.slow_log_collector.time.sleep") as sleep:
                self.assertEqual(collector._enqueue_with_retry(parts), 2)
            self.assertEqual(storage.slowlog_index.enqueue_parts.call_count, 3)
            self.assertEqual(sleep.call_count, 2)

            storage.slowlog_index = Mock(  # type: ignore[attr-defined]
                enqueue_parts=Mock(side_effect=locked)
            )
            with patch("app.slow_log_collector.time.sleep") as sleep:
                with self.assertLogs("app.slow_log_collector", level="ERROR") as logs:
                    with self.assertRaisesRegex(sqlite3.OperationalError, "locked"):
                        collector._enqueue_with_retry(parts)
            self.assertEqual(storage.slowlog_index.enqueue_parts.call_count, 4)
            self.assertEqual(sleep.call_count, 3)
            self.assertTrue(any("reconcile" in line for line in logs.output))
            self.assertIn("locked", collector._status["lastIndexError"])

            storage.slowlog_index = Mock(  # type: ignore[attr-defined]
                enqueue_parts=Mock(side_effect=sqlite3.OperationalError("no such table"))
            )
            with patch("app.slow_log_collector.time.sleep") as sleep:
                with self.assertRaisesRegex(sqlite3.OperationalError, "no such table"):
                    collector._enqueue_with_retry(parts)
            self.assertEqual(sleep.call_count, 0)

    def test_failed_ingest_does_not_mark_records_seen_before_retry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            storage = _Storage(Path(directory))
            client = _Client(_record())
            collector = _collector(Path(directory), storage, client)
            ingest = Mock(side_effect=[sqlite3.OperationalError("database is locked"), None])
            collector._ingest_batch = ingest  # type: ignore[method-assign]
            watermark = _epoch_us("2026-08-30T16:00:00Z")
            now = _epoch_us("2026-08-30T17:00:00Z") / 1_000_000

            with patch("app.slow_log_collector.time.time", return_value=now):
                with self.assertRaisesRegex(sqlite3.OperationalError, "locked"):
                    collector._poll_once(watermark)
                advanced, ingested = collector._poll_once(watermark)

            self.assertGreater(advanced, watermark)
            self.assertEqual(ingested, 1)
            self.assertEqual(ingest.call_count, 2)

    def test_replay_window_recovers_query_published_after_its_start_window(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            storage = _Storage(Path(directory))
            client = _Client(_record("2026-08-30T16:24:54Z"))
            collector = _collector(Path(directory), storage, client)
            ingested: list[list[dict[str, object]]] = []
            collector._ingest_batch = lambda rows: ingested.append(rows)  # type: ignore[method-assign]
            watermark = _epoch_us("2026-08-30T17:00:00Z")
            now = _epoch_us("2026-08-30T17:10:00Z") / 1_000_000

            with patch("app.slow_log_collector.time.time", return_value=now):
                advanced, count = collector._poll_once(watermark)

            self.assertEqual(count, 1)
            self.assertEqual(len(ingested), 1)
            self.assertGreater(advanced, watermark)
            self.assertLessEqual(
                int(client.calls[0]["StartTime"]),
                _epoch_us("2026-08-30T16:24:54Z") // 1000,
            )

    def test_historical_replay_skips_events_already_in_serving_index(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            storage = _Storage(Path(directory))
            client = _Client(_record())
            collector = _collector(Path(directory), storage, client)
            events = collector._build_events([_record()])
            self.assertEqual(len(events), 1)
            storage.existing = {str(events[0]["event_id"])}
            ingest = Mock()
            collector._ingest_batch = ingest  # type: ignore[method-assign]
            watermark = _epoch_us("2026-08-30T16:00:00Z")
            now = _epoch_us("2026-08-30T17:00:00Z") / 1_000_000

            with patch("app.slow_log_collector.time.time", return_value=now):
                advanced, count = collector._poll_once(watermark)

            self.assertGreater(advanced, watermark)
            self.assertEqual(count, 0)
            self.assertEqual(len(storage.lookup_calls), 1)
            ingest.assert_not_called()


class _PagedClient:
    """DAS as it answers: newest first, 100 per page, TotalRecords of the whole window."""

    def __init__(self, records: list[dict[str, object]]) -> None:
        self.records = sorted(records, key=lambda row: int(row["Timestamp"]), reverse=True)  # type: ignore[arg-type]
        self.calls: list[dict[str, object]] = []

    def call(self, _action: str, params: dict[str, object]) -> dict[str, object]:
        self.calls.append(dict(params))
        inside = [row for row in self.records if int(params["StartTime"]) <= int(row["Timestamp"]) <= int(params["EndTime"])]  # type: ignore[arg-type]
        size, page = int(params["PageSize"]), int(params["PageNumber"])  # type: ignore[arg-type]
        return {"Data": {"Logs": [dict(row) for row in inside[(page - 1) * size: page * size]], "TotalRecords": len(inside),
                         "NodeId": "rn-prod-01"}}


def _burst(count: int, newest_s: float, step_s: float) -> list[dict[str, object]]:
    rows = []
    for number in range(count):
        row = dict(_record())
        row.pop("QueryStartTime", None)
        row.update(Timestamp=int((newest_s - number * step_s) * 1000), ThreadId=100000 + number, QueryTime=150 + number % 7)
        rows.append(row)
    return rows


class SlowLogPageCapTests(unittest.TestCase):
    """A busy node must not freeze its watermark: the answer is newest first, so the page cap cuts the oldest records."""

    NOW = _epoch_us("2026-08-30T17:00:00Z") / 1_000_000

    def collector(self, directory: str, records: list[dict[str, object]]):
        storage = _Storage(Path(directory))
        client = _PagedClient(records)
        collector = _collector(Path(directory), storage, client)  # type: ignore[arg-type]
        ingested: list[dict[str, object]] = []

        def ingest(rows: list[dict[str, object]]) -> None:
            ingested.extend(rows)
            storage.existing.update(str(row["event_id"]) for row in rows)  # what the serving index would answer next time

        collector._ingest_batch = ingest  # type: ignore[method-assign]
        return collector, client, ingested

    def test_a_cut_replay_still_moves_the_watermark_when_the_new_part_was_read_in_full(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            collector, client, _ingested = self.collector(directory, _burst(800, self.NOW - 100, 8))  # 6,400 s of history
            watermark = int((self.NOW - 300) * 1_000_000)
            with patch("app.slow_log_collector._MAX_PAGES", 5), patch("app.slow_log_collector.time.time", return_value=self.NOW):
                with self.assertLogs("app.slow_log_collector", level="WARNING") as logs:
                    advanced, _count = collector._poll_once(watermark)
            self.assertGreater(advanced, watermark)  # everything newer than the old watermark was read
            self.assertTrue(any("回放窗口被页数上限截断" in line for line in logs.output))
            self.assertEqual(len(client.calls), 5)
            self.assertLess(collector._replay_now, collector.config.replay_seconds)  # the next replay window is shorter

    def test_a_cut_inside_the_new_part_keeps_the_watermark_and_loses_nothing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            records = _burst(900, self.NOW - 70, 1)  # all newer than the watermark
            collector, _client, ingested = self.collector(directory, records)
            watermark = original = int((self.NOW - 1000) * 1_000_000)
            polls = 0
            with patch("app.slow_log_collector._MAX_PAGES", 5), patch("app.slow_log_collector.time.time", return_value=self.NOW):
                while watermark == original and polls < 8:
                    watermark, _count = collector._poll_once(watermark)
                    polls += 1
            self.assertGreater(watermark, original)
            self.assertLessEqual(polls, 6)
            behind_watermark = [row for row in records if int(row["Timestamp"]) <= watermark // 1000]  # type: ignore[arg-type]
            owed, _seen = build_slow_events(behind_watermark, instance_id="rm-prod", default_node_id="rn-prod-01")
            self.assertTrue(owed)
            self.assertLessEqual({str(row["event_id"]) for row in owed}, {str(row["event_id"]) for row in ingested})  # nothing skipped
            # the part still ahead of the watermark is read by the next polls, never skipped
            with patch("app.slow_log_collector._MAX_PAGES", 5), patch("app.slow_log_collector.time.time", return_value=self.NOW):
                for _ in range(4):
                    watermark, _count = collector._poll_once(watermark)
            self.assertEqual(len({row["event_id"] for row in ingested}), 900)

    def test_the_replay_window_follows_the_record_rate_and_grows_back(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            collector, _client, _ingested = self.collector(directory, _burst(1500, self.NOW - 100, 4))
            with patch("app.slow_log_collector._PAGE_BUDGET", 5), patch("app.slow_log_collector.time.time", return_value=self.NOW):
                collector._poll_once(int((self.NOW - 300) * 1_000_000))
            self.assertLess(collector._replay_now, collector.config.replay_seconds)
            self.assertGreaterEqual(collector._replay_now, 600)
            self.assertEqual(collector.status()["replayNowSeconds"], int(collector._replay_now))
            collector._tune_window(0, 100.0, 0, False, True)  # a calm node gets the configured replay window back
            self.assertEqual(collector._replay_now, collector.config.replay_seconds)


class SlowLogMemoryTests(unittest.TestCase):
    """1.34.3: a poll converts page by page, a restart keeps the tuned windows, and the nodes start spread out."""

    NOW = SlowLogPageCapTests.NOW

    def test_page_by_page_events_equal_the_whole_window_and_drop_a_record_repeated_on_a_later_page(self) -> None:
        records = _burst(250, self.NOW - 100, 4)
        # DAS can hand the same statement on two pages (here record 10 again at the top of page 2).
        pages = [records[:100], [dict(records[10])] + records[100:199], records[199:]]

        class Pages:
            def call(self, _action: str, params: dict[str, object]) -> dict[str, object]:
                rows = pages[int(params["PageNumber"]) - 1]  # type: ignore[arg-type]
                return {"Data": {"Logs": [dict(row) for row in rows], "TotalRecords": 251, "NodeId": "rn-prod-01"}}

        with tempfile.TemporaryDirectory() as directory:
            collector = _collector(Path(directory), _Storage(Path(directory)), Pages())  # type: ignore[arg-type]
            ingested: list[dict[str, object]] = []
            collector._ingest_batch = ingested.extend  # type: ignore[method-assign]
            with patch("app.slow_log_collector.time.time", return_value=self.NOW):
                collector._poll_once(int((self.NOW - 300) * 1_000_000))
        whole, observed = build_slow_events(records, instance_id="rm-prod", default_node_id="rn-prod-01")
        self.assertEqual(len(whole), 250)
        self.assertEqual([row["event_id"] for row in ingested], [row["event_id"] for row in whole])
        self.assertEqual(collector._seen_ids, observed)
        self.assertEqual(collector.status()["windowRecords"], 251)

    def test_a_restart_keeps_the_tuned_window_sizes_within_the_configured_bounds(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            storage = _Storage(Path(directory))
            first = _collector(Path(directory), storage, _Client(_record()))
            first._replay_now, first._forward_seconds = 900, 240
            first._save_watermark(123)
            again = _collector(Path(directory), storage, _Client(_record()))
            self.assertEqual(again._load_watermark(), 123)
            self.assertEqual((again._replay_now, again._forward_seconds), (900, 240))
            self.assertEqual(again.status()["replayNowSeconds"], 900)
            first._replay_now, first._forward_seconds = 10**9, 1  # an edited or stale file cannot leave the bounds
            first._save_watermark(124)
            third = _collector(Path(directory), storage, _Client(_record()))
            third._load_watermark()
            self.assertEqual((third._replay_now, third._forward_seconds), (7200, 120))

    def test_a_delayed_start_does_not_poll_before_its_turn(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            collector = _collector(Path(directory), _Storage(Path(directory)), _Client(_record()))
            collector._poll_once = Mock(return_value=(1, 0))  # type: ignore[method-assign]
            collector.start(delay_seconds=60)
            collector.shutdown()
            collector._poll_once.assert_not_called()


class SharedBuilderTests(unittest.TestCase):
    def test_the_collector_and_the_live_tail_build_the_same_event(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            collector = _collector(Path(directory), _Storage(Path(directory)), _Client(_record()))
            through_collector = collector._build_events([_record()])
            events, observed = build_slow_events([_record()], instance_id="rm-prod", default_node_id="rn-prod-01")
            self.assertEqual(through_collector, events)
            known = {str(events[0]["event_id"])}
            again, seen = build_slow_events([_record(), _record()], instance_id="rm-prod", default_node_id="rn-prod-01", seen_ids=known)
            self.assertEqual((again, seen), ([], known))  # known ids are looked at but not returned

    def test_accounts_are_excluded_by_the_caller_in_lowercase(self) -> None:
        events, observed = build_slow_events([_record()], instance_id="rm-prod", default_node_id="rn-prod-01",
                                             exclude_accounts=("batch_export",))
        self.assertEqual((events, observed), ([], set()))


class SlowLogConfigDefaultsTests(unittest.TestCase):
    def test_watermark_margin_defaults_to_90_seconds_and_stays_overridable_within_bounds(self) -> None:
        # 1.30.0: the margin was 300 s, which kept the newest slow logs ~6 minutes behind now; late-published records are
        # caught by the replay window, so the margin only has to cover DAS publication jitter.
        config = SlowLogConfig({"instanceId": "rm-prod", "nodeId": "rn-prod-01", "enabled": True})
        self.assertEqual((config.lag_seconds, config.poll_seconds, config.replay_seconds), (90, 120, 7200))
        self.assertEqual(SlowLogConfig({"instanceId": "rm", "lagSeconds": 300}).lag_seconds, 300)  # explicit values win
        self.assertEqual(SlowLogConfig({"instanceId": "rm", "lagSeconds": 30}).lag_seconds, 60)  # clamped to the 60 s floor


if __name__ == "__main__":
    unittest.main()
