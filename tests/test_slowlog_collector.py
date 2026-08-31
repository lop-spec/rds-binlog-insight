from __future__ import annotations

import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import Mock, patch

from app.slow_log_collector import SlowLogCollector, SlowLogConfig


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


if __name__ == "__main__":
    unittest.main()
