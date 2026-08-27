from __future__ import annotations

import io
import hashlib
import sqlite3
import tempfile
import threading
import unittest
import xml.etree.ElementTree as ET
from dataclasses import replace
from importlib.util import find_spec
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from app.clickhouse_client import ClickHouseClient, ClickHouseConfig
from app.clickhouse_change_queue import reconcile_pending_source_changes
from app.clickhouse_manifest import (
    ClickHouseManifest,
    ClickHouseManifestError,
)
from app.clickhouse_query import ClickHouseQueryBackend
from app.io_pressure import IoPressureGate, IoPressurePaused
from tools.clickhouse_query_backfill import _range_stats

PARQUET_FIXTURE = b"PAR1-test"

if find_spec("oss2") is not None and find_spec("pyarrow") is not None:
    from app.clickhouse_ingest import (
        IngestPaused,
        HealthCanary,
        MergeGovernor,
        _admit_io_pressure,
        _ingest_pacing_seconds,
        _probe_health,
        _probe_io_pressure,
        _initial_reconcile_monotonic,
        _require_ingest_recovery,
        _reconcile_interval,
        ingest_one,
        reconcile_once,
    )
    from tools.clickhouse_oss_backfill import (
        _batches,
        _cleanup_partial_batch,
        _status_name as oss_backfill_status_name,
        _wait_for_admission,
    )
else:  # The production image installs oss2; lightweight host checks may not.
    ingest_one = None
    IngestPaused = RuntimeError
    _probe_health = None
    _probe_io_pressure = None
    _initial_reconcile_monotonic = None
    _require_ingest_recovery = None
    _reconcile_interval = None
    reconcile_once = None
    HealthCanary = None
    MergeGovernor = None
    _admit_io_pressure = None
    _ingest_pacing_seconds = None
    _cleanup_partial_batch = None
    _batches = None
    oss_backfill_status_name = None
    _wait_for_admission = None


def _part(path: Path, identity: str, *, rows: int = 3) -> dict[str, object]:
    now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
    return {
        "path": str(path),
        "logical_part_id": identity,
        "sha256": hashlib.sha256(PARQUET_FIXTURE).hexdigest(),
        "content_revision": 1,
        "min_event_epoch_us": now_us - 60_000_000,
        "max_event_epoch_us": now_us,
        "row_count": rows,
        "size_bytes": len(PARQUET_FIXTURE),
        "oss_key": f"objects/{identity}.parquet",
    }


def _config() -> ClickHouseConfig:
    return ClickHouseConfig(
        enabled=True,
        serving_enabled=True,
        host="clickhouse",
        port=8123,
        database="insight",
        table="events",
        user="test",
        password="secret",
        hot_hours=27,
        serving_hours=25,
        reconcile_seconds=30,
        idle_seconds=1.0,
        health_url="",
        health_host_header="",
        health_max_seconds=1.0,
        min_free_gb=20,
        io_pressure_max_full_avg10=5.0,
    )


class _Metadata:
    def __init__(self, parts: list[dict[str, object]]):
        self.parts = {str(part["path"]): part for part in parts}

    def part_by_path(self, path: str):
        return self.parts.get(path)

    def parts_in_range(self, **_kwargs):
        return list(self.parts.values())


class _TemporarilyLockedMetadata(_Metadata):
    def __init__(self, parts: list[dict[str, object]], locked_reads: int):
        super().__init__(parts)
        self.locked_reads = locked_reads
        self.reads = 0

    def part_by_path(self, path: str):
        self.reads += 1
        if self.locked_reads:
            self.locked_reads -= 1
            raise sqlite3.OperationalError("database is locked")
        return super().part_by_path(path)


class _Archive:
    def open_part_reader(self, _part):
        return io.BytesIO(PARQUET_FIXTURE)

    def download_part(self, _part, _destination: Path):
        raise AssertionError("ClickHouse ingest must not stage OSS parts on disk")


class _Client:
    def __init__(self):
        self.parts: dict[str, dict[str, object]] = {}
        self.queries: list[tuple[str, dict[str, object]]] = []
        self.merge_calls: list[str] = []

    def start_merges(self):
        self.merge_calls.append("start")

    def stop_merges(self):
        self.merge_calls.append("stop")

    def part_state(self, identity: str):
        return self.parts.get(
            identity,
            {
                "rows": 0,
                "sha_count": 0,
                "sha256": "",
                "min_revision": 0,
                "max_revision": 0,
            },
        )

    def paired_part_states(self, identities):
        result = {}
        for identity in identities:
            state = dict(self.part_state(identity))
            state.update(
                {
                    "name_rows": int(state.get("rows") or 0),
                    "name_sha_count": int(state.get("sha_count") or 0),
                    "name_sha256": str(state.get("sha256") or ""),
                    "name_min_revision": int(state.get("min_revision") or 0),
                    "name_max_revision": int(state.get("max_revision") or 0),
                }
            )
            result[identity] = state
        return result

    def insert_parquet(
        self,
        _path: Path,
        *,
        part_key: str,
        sha256: str,
        content_revision: int,
    ):
        self.parts[part_key] = {
            "rows": 3,
            "sha_count": 1,
            "sha256": sha256,
            "min_revision": content_revision,
            "max_revision": content_revision,
        }

    def insert_parquet_stream(
        self,
        source,
        *,
        content_length: int,
        part_key: str,
        sha256: str,
        content_revision: int,
    ):
        payload = source.read()
        if len(payload) != content_length:
            raise AssertionError("stream length mismatch")
        self.parts[part_key] = {
            "rows": 3,
            "sha_count": 1,
            "sha256": sha256,
            "min_revision": content_revision,
            "max_revision": content_revision,
        }

    def delete_part(self, identity: str):
        self.parts.pop(identity, None)

    def json_rows(self, sql: str, *, parameters=None, **_kwargs):
        self.queries.append((sql, dict(parameters or {})))
        return [
            {
                "event_id": "event-1",
                "event_epoch_us": 10,
                "locator": "part-a:*",
            }
        ]


class ClickHouseManifestTests(unittest.TestCase):
    def test_runtime_open_never_creates_schema(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "manifest.sqlite3"
            with self.assertRaises(ClickHouseManifestError):
                ClickHouseManifest(path, run_migrations=False)
            self.assertFalse(path.exists())
            ClickHouseManifest(path, run_migrations=True)
            self.assertTrue(path.is_file())

    def test_reconcile_replacement_and_delete_are_persistent(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "manifest.sqlite3"
            manifest = ClickHouseManifest(path, run_migrations=True)
            first = _part(Path(temp) / "part.parquet", "part-v1")
            now_us = int(first["max_event_epoch_us"])
            manifest.reconcile(
                [first],
                start_epoch_us=now_us - 3_600_000_000,
                end_epoch_us=now_us,
            )
            claim = manifest.claim_next()
            self.assertEqual(claim["job_kind"], "load")
            self.assertTrue(manifest.mark_ready(str(first["path"]), "part-v1", 3))
            self.assertTrue(manifest.coverage([first])["complete"])

            replacement = {**first, "logical_part_id": "part-v2", "sha256": "sha-v2"}
            result = manifest.reconcile(
                [replacement],
                start_epoch_us=now_us - 3_600_000_000,
                end_epoch_us=now_us,
            )
            self.assertEqual(result["replacement_deletes"], 1)
            self.assertFalse(manifest.coverage([replacement])["complete"])
            delete = manifest.claim_next()
            self.assertEqual((delete["logical_part_id"], delete["job_kind"]), ("part-v1", "delete"))
            manifest.mark_retired(str(first["path"]), "part-v1")
            load = manifest.claim_next()
            self.assertEqual((load["logical_part_id"], load["job_kind"]), ("part-v2", "load"))
            manifest.mark_ready(str(first["path"]), "part-v2", 3)
            self.assertTrue(manifest.coverage([replacement])["complete"])

            manifest.reconcile(
                [],
                start_epoch_us=now_us - 3_600_000_000,
                end_epoch_us=now_us,
            )
            removed = manifest.claim_next()
            self.assertEqual((removed["logical_part_id"], removed["job_kind"]), ("part-v2", "delete"))

    def test_failed_job_retries_same_action(self):
        with tempfile.TemporaryDirectory() as temp:
            manifest = ClickHouseManifest(
                Path(temp) / "manifest.sqlite3",
                run_migrations=True,
            )
            part = _part(Path(temp) / "part.parquet", "part-a")
            now_us = int(part["max_event_epoch_us"])
            manifest.reconcile(
                [part],
                start_epoch_us=now_us - 1,
                end_epoch_us=now_us,
            )
            claim = manifest.claim_next()
            manifest.mark_failed(str(part["path"]), str(claim["logical_part_id"]), "boom")
            stats = manifest.stats()
            self.assertEqual(stats["failed_parts"], 1)
            newer = {
                **_part(Path(temp) / "newer.parquet", "part-newer"),
                "min_event_epoch_us": now_us + 10,
                "max_event_epoch_us": now_us + 20,
            }
            manifest.reconcile(
                [part, newer],
                start_epoch_us=now_us - 1,
                end_epoch_us=now_us + 20,
            )
            with manifest.connection() as connection:
                connection.execute(
                    "UPDATE clickhouse_parts SET next_retry_us = 0"
                )
            retry = manifest.claim_next()
            self.assertEqual(retry["job_kind"], "load")
            self.assertEqual(retry["logical_part_id"], "part-a")

    def test_pending_claim_does_not_starve_old_work_behind_new_arrivals(self):
        with tempfile.TemporaryDirectory() as temp:
            manifest = ClickHouseManifest(
                Path(temp) / "manifest.sqlite3",
                run_migrations=True,
            )
            now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
            older = {
                **_part(Path(temp) / "older.parquet", "part-older"),
                "min_event_epoch_us": now_us - 40,
                "max_event_epoch_us": now_us - 30,
            }
            newer = {
                **_part(Path(temp) / "newer.parquet", "part-newer"),
                "min_event_epoch_us": now_us - 20,
                "max_event_epoch_us": now_us - 10,
            }
            manifest.reconcile(
                [older, newer],
                start_epoch_us=now_us - 40,
                end_epoch_us=now_us,
            )

            claim = manifest.claim_next()

            self.assertEqual(claim["logical_part_id"], "part-older")

    def test_bulk_pending_claim_prioritizes_newest_hot_work(self):
        with tempfile.TemporaryDirectory() as temp:
            manifest = ClickHouseManifest(
                Path(temp) / "manifest.sqlite3",
                run_migrations=True,
            )
            now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
            older = {
                **_part(Path(temp) / "older.parquet", "part-older"),
                "min_event_epoch_us": now_us - 40,
                "max_event_epoch_us": now_us - 30,
            }
            newer = {
                **_part(Path(temp) / "newer.parquet", "part-newer"),
                "min_event_epoch_us": now_us - 20,
                "max_event_epoch_us": now_us - 10,
            }
            manifest.reconcile(
                [older, newer],
                start_epoch_us=now_us - 40,
                end_epoch_us=now_us,
            )

            claim = manifest.claim_next(prefer_newest=True)

            self.assertEqual(claim["logical_part_id"], "part-newer")

    def test_concurrent_claims_are_unique(self):
        with tempfile.TemporaryDirectory() as temp:
            manifest = ClickHouseManifest(
                Path(temp) / "manifest.sqlite3", run_migrations=True
            )
            now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
            parts = [
                _part(Path(temp) / f"part-{index}.parquet", f"part-{index}")
                for index in range(20)
            ]
            manifest.reconcile(
                parts,
                start_epoch_us=now_us - 3_600_000_000,
                end_epoch_us=now_us,
            )
            claimed: list[str] = []
            lock = threading.Lock()

            def claim() -> None:
                row = manifest.claim_next()
                self.assertIsNotNone(row)
                with lock:
                    claimed.append(str(row["logical_part_id"]))

            threads = [threading.Thread(target=claim) for _ in parts]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(len(claimed), len(parts))
            self.assertEqual(len(set(claimed)), len(parts))

    def test_source_change_handoff_queues_before_versioned_ack(self):
        with tempfile.TemporaryDirectory() as temp:
            manifest = ClickHouseManifest(
                Path(temp) / "manifest.sqlite3", run_migrations=True
            )
            stale = _part(Path(temp) / "stale.parquet", "stale-v1")
            manifest.reconcile(
                [stale],
                start_epoch_us=int(stale["min_event_epoch_us"]),
                end_epoch_us=int(stale["max_event_epoch_us"]),
            )
            manifest.claim_next()
            manifest.mark_ready(str(stale["path"]), "stale-v1", 3)
            fresh = {
                **_part(Path(temp) / "fresh.parquet", "fresh-v1"),
                "exists": True,
                "query_visible": 1,
                "log_file_name": "mysql-bin.000002",
                "change_version": 11,
            }
            deferred = {
                **_part(Path(temp) / "local.parquet", "local-v1"),
                "oss_key": "",
                "exists": True,
                "query_visible": 1,
                "log_file_name": "mysql-bin.000003",
                "change_version": 12,
            }

            class _Changes:
                def __init__(self):
                    self.acked = []

                def pending_clickhouse_changes(self, *, limit):
                    self.limit = limit
                    return [
                        fresh,
                        deferred,
                        {
                            "path": str(stale["path"]),
                            "exists": False,
                            "change_version": 13,
                        },
                    ]

                def ack_clickhouse_changes(self, values):
                    self.acked.extend(values)
                    return len(values)

            changes = _Changes()
            result = reconcile_pending_source_changes(changes, manifest, limit=128)

            self.assertEqual(changes.limit, 128)
            self.assertEqual(
                changes.acked,
                [(str(fresh["path"]), 11), (str(stale["path"]), 13)],
            )
            self.assertEqual(result["queued"], 1)
            self.assertEqual(result["removed"], 1)
            self.assertEqual(result["deferred"], 1)
            with manifest.connection() as connection:
                states = {
                    str(row["logical_part_id"]): str(row["status"])
                    for row in connection.execute(
                        "SELECT logical_part_id, status FROM clickhouse_parts"
                    )
                }
            self.assertEqual(states["fresh-v1"], "pending")
            self.assertEqual(states["stale-v1"], "delete_pending")


class ClickHouseResourceConfigTests(unittest.TestCase):
    def test_query_backfill_source_projection_is_explicit_and_optional(self):
        class _StatsClient:
            def __init__(self):
                self.settings = []

            def json_rows(self, _sql, **kwargs):
                self.settings.append(dict(kwargs.get("settings") or {}))
                return [
                    {
                        "rows": 1,
                        "min_epoch_us": 1,
                        "max_epoch_us": 1,
                        "hash_sum": 1,
                        "hash_xor": 1,
                    }
                ]

        client = _StatsClient()
        _range_stats(
            client,
            "insight.events_query",
            "2026-08-25",
            1,
            2,
            source_projection="",
        )
        _range_stats(
            client,
            "insight.events",
            "2026-08-25",
            1,
            2,
            source_projection="events_time_order_v2",
        )
        self.assertNotIn(
            "preferred_optimize_projection_name", client.settings[0]
        )
        self.assertEqual(
            client.settings[1]["preferred_optimize_projection_name"],
            "events_time_order_v2",
        )

    def test_interactive_query_uses_one_thread_on_one_cpu_container(self):
        captured: dict[str, object] = {}

        class _Response:
            status = 200

            @staticmethod
            def read() -> bytes:
                return b"1\n"

        class _Connection:
            def __init__(self, _host, _port, timeout):
                captured["timeout"] = timeout

            def request(self, _method, target, **_kwargs):
                captured["target"] = target

            def getresponse(self):
                return _Response()

            def close(self):
                return None

        with patch(
            "app.clickhouse_client.http.client.HTTPConnection",
            _Connection,
        ):
            self.assertEqual(ClickHouseClient(_config()).query("SELECT 1"), "1\n")

        parameters = parse_qs(urlsplit(str(captured["target"])).query)
        self.assertEqual(parameters["max_threads"], ["1"])

    def test_background_work_is_bounded_for_two_cpu_container(self):
        root = Path(__file__).resolve().parents[1]
        config = ET.parse(
            root / "clickhouse" / "config.d" / "low-resource.xml"
        ).getroot()
        # CPU/disk work is bounded by the dedicated pools and container
        # limits.  The global pool is a thread capacity ceiling; an explicit
        # value of 128 caused a live server to accept connections while even
        # SELECT 1 waited forever for a worker.
        self.assertIsNone(config.find("max_thread_pool_size"))
        pool = int(config.findtext("background_pool_size", "0"))
        ratio = int(
            config.findtext("background_merges_mutations_concurrency_ratio", "0")
        )
        self.assertEqual(pool, 1)
        self.assertEqual(ratio, 1)
        # The lightweight scheduler needs enough slots to load system logs and
        # outdated parts concurrently during a cold start. Eight starved a
        # 30M-row live copy even though the merge pool was idle.
        self.assertEqual(
            int(config.findtext("background_schedule_pool_size", "0")), 32
        )
        merge_tree = config.find("merge_tree")
        self.assertIsNotNone(merge_tree)
        capacity = pool * ratio
        for name in (
            "number_of_free_entries_in_pool_to_execute_mutation",
            "number_of_free_entries_in_pool_to_execute_optimize_entire_partition",
            "number_of_free_entries_in_pool_to_lower_max_size_of_merge",
        ):
            self.assertLess(int(merge_tree.findtext(name, "0")), capacity)
        self.assertEqual(
            int(
                merge_tree.findtext(
                    "max_bytes_to_merge_at_max_space_in_pool", "0"
                )
            ),
            128 * 1024 * 1024,
        )

    def test_healthcheck_executes_an_authenticated_query(self):
        root = Path(__file__).resolve().parents[1]
        compose = (root / "compose.yaml").read_text(encoding="utf-8")
        insight_service = compose.split("  insight:\n", 1)[1].split(
            "  indexer:\n", 1
        )[0]
        self.assertIn(
            'RDS_BINLOG_CLICKHOUSE_SERVING_ENABLED: "${RDS_BINLOG_CLICKHOUSE_SERVING_ENABLED:-0}"',
            insight_service,
        )
        self.assertIn(
            "RDS_BINLOG_CLICKHOUSE_NAME_QUERY_TABLE: events_query_by_name",
            insight_service,
        )
        env_example = (root / ".env.example").read_text(encoding="utf-8")
        self.assertIn("RDS_BINLOG_CLICKHOUSE_SERVING_ENABLED=0", env_example)
        self.assertNotIn("RDS_BINLOG_CLICKHOUSE_DEVICE_WRITE_BPS", env_example)
        self.assertIn(
            "RDS_BINLOG_CLICKHOUSE_BACKFILL_IO_FULL_AVG10_MAX=10",
            env_example,
        )
        self.assertIn(
            "RDS_BINLOG_SLOWLOG_SOURCE_IO_FULL_AVG10_MAX=10",
            env_example,
        )
        clickhouse_service = compose.split("  clickhouse:\n", 1)[1].split(
            "  clickhouse-ingester:\n", 1
        )[0]
        healthcheck = next(
            line for line in clickhouse_service.splitlines()
            if line.strip().startswith("test:")
        )
        self.assertIn('clickhouse-client --user', healthcheck)
        self.assertIn('$$CLICKHOUSE_PASSWORD', healthcheck)
        self.assertIn('--query \\"SELECT 1\\"', healthcheck)
        self.assertNotIn("/ping", healthcheck)
        self.assertNotIn("device_read_bps", clickhouse_service)
        self.assertNotIn("device_write_bps", clickhouse_service)
        self.assertIn(
            'cpus: "${RDS_BINLOG_CLICKHOUSE_CPUS:-1.0}"',
            clickhouse_service,
        )
        self.assertIn("weight: 10", clickhouse_service)
        ingester_service = compose.split("  clickhouse-ingester:\n", 1)[1]
        self.assertIn(
            "RDS_BINLOG_CLICKHOUSE_HEALTH_URL: http://insight:8769/api/storage",
            ingester_service,
        )
        self.assertIn(
            "RDS_BINLOG_CLICKHOUSE_NAME_QUERY_TABLE: events_query_by_name",
            ingester_service,
        )
        self.assertIn(
            'RDS_BINLOG_CLICKHOUSE_IO_FULL_AVG10_MAX: "${RDS_BINLOG_CLICKHOUSE_BACKFILL_IO_FULL_AVG10_MAX:-10}"',
            ingester_service,
        )
        self.assertNotIn("device_write_bps", ingester_service)
        slowlog_service = compose.split("  slowlog-indexer:\n", 1)[1].split(
            "  clickhouse:\n", 1
        )[0]
        self.assertIn(
            'RDS_BINLOG_SLOWLOG_IO_FULL_AVG10_MAX: "${RDS_BINLOG_SLOWLOG_SOURCE_IO_FULL_AVG10_MAX:-10}"',
            slowlog_service,
        )
        self.assertIn(
            'RDS_BINLOG_SLOWLOG_RECONCILE_SECONDS: "${RDS_BINLOG_SLOWLOG_RECONCILE_SECONDS:-3600}"',
            slowlog_service,
        )
        self.assertIn('RDS_BINLOG_SLOWLOG_BATCH: "1"', slowlog_service)
        self.assertIn(
            'RDS_BINLOG_SLOWLOG_RECONCILE_BATCH: "128"',
            slowlog_service,
        )
        self.assertIn(
            "RDS_BINLOG_CLICKHOUSE_HEALTH_URL: http://insight:8769/api/storage",
            slowlog_service,
        )
        self.assertIn('RDS_BINLOG_CLICKHOUSE_HEALTH_MAX_SECONDS: "1"', slowlog_service)
        self.assertNotIn("device_read_bps", slowlog_service)
        self.assertNotIn("device_write_bps", slowlog_service)

    @unittest.skipIf(MergeGovernor is None, "oss2 is not installed on this host")
    def test_merge_governor_starts_once_and_never_cancels_merges(self):
        class _MergeClient:
            def __init__(self):
                self.calls = []

            def start_merges(self):
                self.calls.append("start")

        client = _MergeClient()
        governor = MergeGovernor(client)
        governor.resume()
        governor.resume()
        self.assertFalse(hasattr(governor, "pause"))
        self.assertEqual(client.calls, ["start"])

    def test_schema_caps_each_background_merge(self):
        root = Path(__file__).resolve().parents[1]
        schema = (root / "clickhouse" / "002_events.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "max_bytes_to_merge_at_max_space_in_pool = 134217728",
            schema,
        )
        self.assertIn("ALTER TABLE insight.events MODIFY SETTING", schema)

    def test_query_schema_is_reverse_ordered_and_explicitly_migrated(self):
        root = Path(__file__).resolve().parents[1]
        schema = (root / "clickhouse" / "005_events_query.sql").read_text(
            encoding="utf-8"
        )
        migration = (root / "app" / "clickhouse_migrate.py").read_text(
            encoding="utf-8"
        )
        self.assertIn("CREATE TABLE IF NOT EXISTS insight.events_query", schema)
        self.assertIn("event_epoch_us DESC", schema)
        self.assertIn("allow_experimental_reverse_key = 1", schema)
        self.assertIn("PROJECTION IF NOT EXISTS names_hourly_v1", schema)
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS insight.events_query_by_name", schema
        )
        self.assertIn("database_name ASC", schema)
        self.assertIn("event_epoch_us DESC", schema)
        self.assertIn("events_query_stage", schema)
        self.assertIn('Path("/app/clickhouse/005_events_query.sql")', migration)

    def test_delete_part_mirrors_query_copy_before_source_truth(self):
        client = ClickHouseClient(_config())
        with patch.object(client, "json_rows", return_value=[]), patch.object(
            client, "query"
        ) as query:
            client.delete_part("part-a")
        self.assertEqual(query.call_count, 3)
        self.assertIn(
            "insight.events_query_by_name", query.call_args_list[0].args[0]
        )
        self.assertIn("insight.events_query", query.call_args_list[1].args[0])
        self.assertIn("insight.events ", query.call_args_list[2].args[0])

    def test_delete_parts_batches_crash_recovery_identities(self):
        client = ClickHouseClient(_config())
        with patch.object(client, "json_rows", return_value=[]), patch.object(
            client, "query"
        ) as query:
            client.delete_parts(["part-a", "part-b", "part-a"])

        self.assertEqual(query.call_count, 3)
        for call in query.call_args_list:
            self.assertIn("_source_part_key IN", call.args[0])
            self.assertEqual(
                set(call.kwargs["parameters"].values()),
                {"part-a", "part-b"},
            )
            self.assertEqual(call.kwargs["settings"]["mutations_sync"], 0)
            self.assertEqual(call.kwargs["timeout"], 1800)
            self.assertEqual(
                call.kwargs["settings"]["max_execution_time"], 1800
            )

    def test_delete_parts_skips_keys_with_an_active_mutation(self):
        client = ClickHouseClient(_config())
        with patch.object(
            client,
            "json_rows",
            return_value=[
                {
                    "command": (
                        "(DELETE WHERE _source_part_key IN ('part-a'))"
                    )
                }
            ],
        ), patch.object(client, "query") as query:
            client.delete_parts(["part-a", "part-b"])

        self.assertEqual(query.call_count, 3)
        for call in query.call_args_list:
            self.assertEqual(
                set(call.kwargs["parameters"].values()),
                {"part-b"},
            )

    def test_paired_part_states_requires_both_physical_query_tables(self):
        client = ClickHouseClient(_config())
        time_state = {
            "part-a": {
                "rows": 7,
                "sha_count": 1,
                "sha256": "a" * 64,
                "min_revision": 9,
                "max_revision": 9,
            }
        }
        name_state: dict[str, dict[str, object]] = {}
        with patch.object(
            client,
            "_part_states_for_table",
            side_effect=[time_state, name_state],
        ) as states_for_table:
            paired = client.paired_part_states(["part-a"])

        self.assertEqual(states_for_table.call_count, 2)
        self.assertEqual(paired["part-a"]["rows"], 7)
        self.assertEqual(paired["part-a"]["name_rows"], 0)
        self.assertEqual(paired["part-a"]["name_sha256"], "")

    def test_staging_client_uses_explicit_tables_and_partition_moves(self):
        client = ClickHouseClient(_config())
        with patch.object(
            client,
            "_part_states_for_table",
            side_effect=[{}, {}],
        ) as states_for_table:
            client.paired_part_states_for_tables(
                ["part-a"],
                time_table="insight.time_stage",
                name_table="insight.name_stage",
            )
        self.assertEqual(
            [call.args[0] for call in states_for_table.call_args_list],
            ["insight.time_stage", "insight.name_stage"],
        )

        with patch.object(client, "query") as query:
            client.truncate_table("insight.time_stage")
            client.copy_table(
                source="insight.time_stage",
                destination="insight.name_stage",
            )
            client.move_partitions(
                source="insight.time_stage",
                destination="insight.time_final",
                partitions=["2026-08-24", "2026-08-26"],
            )

        self.assertIn("TRUNCATE TABLE insight.time_stage", query.call_args_list[0].args[0])
        self.assertIn(
            "INSERT INTO insight.name_stage",
            query.call_args_list[1].args[0],
        )
        self.assertIn(
            "FROM insight.time_stage",
            query.call_args_list[1].args[0],
        )
        self.assertIn(
            "MOVE PARTITION '2026-08-24' TO TABLE insight.time_final",
            query.call_args_list[2].args[0],
        )
        self.assertIn(
            "MOVE PARTITION '2026-08-26' TO TABLE insight.time_final",
            query.call_args_list[3].args[0],
        )

    def test_storage_summary_and_table_status_use_system_metadata(self):
        client = ClickHouseClient(_config())
        with patch.object(
            client,
            "json_rows",
            side_effect=[
                [{"rows": 9, "active_parts": 3, "partitions": 2}],
                [{"objects": 1, "engine": "MergeTree"}],
            ],
        ) as rows:
            summary = client.table_storage_summary("insight.events_query")
            status = client.table_status("insight.events_query")

        self.assertEqual(
            summary,
            {"rows": 9, "active_parts": 3, "partitions": 2},
        )
        self.assertEqual(status, {"exists": True, "engine": "MergeTree"})
        self.assertEqual(
            rows.call_args_list[0].kwargs["parameters"],
            {"database": "insight", "table": "events_query"},
        )

    @unittest.skipIf(
        _cleanup_partial_batch is None,
        "production ClickHouse dependencies are not installed",
    )
    def test_failed_oss_batch_cleans_partial_rows_once(self):
        sha = "a" * 64
        parts = [
            {
                "logical_part_id": key,
                "row_count": 3,
                "sha256": sha,
                "content_revision": 1,
            }
            for key in ("exact", "partial", "empty")
        ]
        exact = {
            "rows": 3,
            "name_rows": 3,
            "sha_count": 1,
            "sha256": sha,
            "min_revision": 1,
            "max_revision": 1,
            "name_sha_count": 1,
            "name_sha256": sha,
            "name_min_revision": 1,
            "name_max_revision": 1,
        }

        class _Client:
            deleted: list[list[str]] = []

            @staticmethod
            def paired_part_states(_identities):
                return {
                    "exact": exact,
                    "partial": {"rows": 3, "name_rows": 0},
                    "empty": {"rows": 0, "name_rows": 0},
                }

            def delete_parts(self, identities):
                self.deleted.append(list(identities))

        client = _Client()
        stale = _cleanup_partial_batch(client, parts)

        self.assertEqual(stale, ["partial"])
        self.assertEqual(client.deleted, [["partial"]])

    @unittest.skipIf(
        _batches is None,
        "production ClickHouse dependencies are not installed",
    )
    def test_oss_backfill_batches_bound_estimated_rows(self):
        parts = [
            {"logical_part_id": "a", "size_bytes": 10, "row_count": 120},
            {"logical_part_id": "b", "size_bytes": 10, "row_count": 90},
            {"logical_part_id": "c", "size_bytes": 10, "row_count": 10},
        ]

        batches = list(
            _batches(parts, max_parts=64, max_bytes=1_000, max_rows=200)
        )

        self.assertEqual(
            [[part["logical_part_id"] for part in batch] for batch in batches],
            [["a"], ["b", "c"]],
        )

    @unittest.skipIf(
        oss_backfill_status_name is None,
        "production ClickHouse dependencies are not installed",
    )
    def test_v3_backfill_status_file_does_not_collide_with_v2(self):
        self.assertEqual(
            oss_backfill_status_name("oss-all-manifest.sqlite3"),
            "clickhouse-oss-backfill-status.json",
        )
        self.assertEqual(
            oss_backfill_status_name("oss-all-v3-manifest.sqlite3"),
            "clickhouse-oss-all-v3-backfill-status.json",
        )

    @unittest.skipIf(
        _wait_for_admission is None,
        "production ClickHouse dependencies are not installed",
    )
    @patch("tools.clickhouse_oss_backfill.time.sleep")
    @patch("tools.clickhouse_oss_backfill._probe_capacity")
    @patch("tools.clickhouse_oss_backfill._probe_health")
    @patch("tools.clickhouse_oss_backfill._probe_io_pressure")
    def test_full_backfill_waits_for_transient_safety_fuse(
        self,
        pressure,
        health,
        capacity,
        sleep,
    ):
        pressure.side_effect = [IngestPaused("high PSI"), 0.0]
        pauses = []

        _wait_for_admission(
            _config(),
            Path("/data"),
            on_pause=pauses.append,
        )

        self.assertEqual(pauses, ["high PSI"])
        sleep.assert_called_once_with(5.0)
        health.assert_called_once()
        capacity.assert_called_once()

    def test_ingest_avoids_parquet_v3_dictionary_overallocation(self):
        root = Path(__file__).resolve().parents[1]
        client = (root / "app" / "clickhouse_client.py").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            '"input_format_parquet_use_native_reader_v3": 0',
            client,
        )

    def test_background_insert_has_its_own_bounded_execution_window(self):
        captured: dict[str, object] = {}

        class _Response:
            status = 200

            @staticmethod
            def read() -> bytes:
                return b""

        class _Connection:
            def __init__(self, host, port, timeout):
                captured["timeout"] = timeout

            def putrequest(self, method, target):
                captured["target"] = target

            def putheader(self, _name, _value):
                return None

            def endheaders(self):
                return None

            def send(self, _chunk):
                return None

            def getresponse(self):
                return _Response()

            def close(self):
                return None

        with tempfile.TemporaryDirectory() as temporary:
            parquet = Path(temporary) / "part.parquet"
            parquet.write_bytes(b"parquet-fixture")
            with patch(
                "app.clickhouse_client.http.client.HTTPConnection",
                _Connection,
            ):
                ClickHouseClient(_config()).insert_parquet(
                    parquet,
                    part_key="part-1",
                    sha256="sha-1",
                    content_revision=1,
                )

        parameters = parse_qs(urlsplit(str(captured["target"])).query)
        self.assertEqual(captured["timeout"], 1800)
        self.assertEqual(parameters["max_execution_time"], ["300"])

    def test_background_insert_accepts_verified_memory_stream(self):
        captured: dict[str, object] = {"body": bytearray(), "headers": {}}

        class _Response:
            status = 200

            @staticmethod
            def read() -> bytes:
                return b""

        class _Connection:
            def __init__(self, _host, _port, timeout):
                captured["timeout"] = timeout

            def putrequest(self, _method, target):
                captured["target"] = target

            def putheader(self, name, value):
                captured["headers"][name] = value

            def endheaders(self):
                return None

            def send(self, chunk):
                captured["body"].extend(chunk)

            def getresponse(self):
                return _Response()

            def close(self):
                return None

        payload = b"parquet-stream-fixture"
        with patch(
            "app.clickhouse_client.http.client.HTTPConnection",
            _Connection,
        ):
            ClickHouseClient(_config()).insert_parquet_stream(
                io.BytesIO(payload),
                content_length=len(payload),
                part_key="part-stream",
                sha256="sha-stream",
                content_revision=2,
            )

        self.assertEqual(bytes(captured["body"]), payload)
        self.assertEqual(captured["headers"]["Content-Length"], str(len(payload)))
        self.assertEqual(captured["timeout"], 1800)


class ClickHouseIngestTests(unittest.TestCase):
    def test_generic_backfill_yields_to_exact_slowlog_source_before_io(self):
        root = Path(__file__).resolve().parents[1]
        source = (root / "app" / "clickhouse_ingest.py").read_text(
            encoding="utf-8"
        )
        worker_loop = source.split("while not stopping:", 1)[1]
        self.assertIn("SourceIndexPriorityGate", source)
        self.assertLess(
            worker_loop.index("source_gate.check()"),
            worker_loop.index("_probe_io_pressure(config)"),
        )
        self.assertIn('"phase": "source-priority"', worker_loop)

    def test_clickhouse_config_reads_independent_io_recovery_ratio(self):
        with patch.dict(
            "os.environ",
            {"RDS_BINLOG_CLICKHOUSE_IO_RECOVERY_RATIO": "0.8"},
            clear=False,
        ):
            config = ClickHouseConfig.from_env()
        self.assertEqual(config.io_pressure_recovery_ratio, 0.8)

    @unittest.skipIf(
        _ingest_pacing_seconds is None, "oss2 is not installed on this host"
    )
    def test_generic_ingest_paces_every_successful_part(self):
        self.assertEqual(
            _ingest_pacing_seconds(replace(_config(), idle_seconds=0.2)),
            1.0,
        )
        self.assertEqual(
            _ingest_pacing_seconds(replace(_config(), idle_seconds=2.0)),
            2.0,
        )

    def test_shared_io_gate_pauses_and_requires_hysteresis_recovery(self):
        with tempfile.TemporaryDirectory() as temp:
            pressure = Path(temp) / "io"
            gate = IoPressureGate(limit=10.0, path=pressure)
            pressure.write_text(
                "some avg10=15.00 avg60=8.00 avg300=2.00 total=1\n"
                "full avg10=12.00 avg60=7.00 avg300=1.00 total=1\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(IoPressurePaused, "safety ceiling"):
                gate.check()
            self.assertTrue(gate.paused)

            pressure.write_text(
                "some avg10=7.00 avg60=5.00 avg300=2.00 total=1\n"
                "full avg10=6.00 avg60=4.00 avg300=1.00 total=1\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(IoPressurePaused, "recovery ceiling"):
                gate.check()
            self.assertTrue(gate.paused)

            pressure.write_text(
                "some avg10=4.00 avg60=4.00 avg300=2.00 total=1\n"
                "full avg10=5.00 avg60=3.00 avg300=1.00 total=1\n",
                encoding="utf-8",
            )
            self.assertEqual(gate.check(), 5.0)
            self.assertFalse(gate.paused)

    @unittest.skipIf(ingest_one is None, "oss2 is not installed on this host")
    def test_storage_canary_reads_complete_bounded_payload(self):
        payload = b'{"padding":"' + (b"a" * 270_000) + b'","ok":true}'

        class _Response:
            status = 200

            def __init__(self):
                self.read_limit = 0

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self, limit):
                self.read_limit = int(limit)
                return payload[:limit]

        response = _Response()
        config = replace(
            _config(),
            health_url="http://insight:8769/api/storage",
        )
        with patch("app.clickhouse_ingest.urlopen", return_value=response):
            self.assertLess(_probe_health(config), 1.0)
        self.assertEqual(response.read_limit, 1024 * 1024)

    @unittest.skipIf(ingest_one is None, "oss2 is not installed on this host")
    def test_storage_canary_timeout_is_a_pause_not_a_failed_part(self):
        config = replace(
            _config(),
            health_url="http://insight:8769/api/storage",
        )
        with patch(
            "app.clickhouse_ingest.urlopen",
            side_effect=TimeoutError("timed out"),
        ):
            with self.assertRaisesRegex(IngestPaused, "health probe failed"):
                _probe_health(config)

    @unittest.skipIf(ingest_one is None, "oss2 is not installed on this host")
    def test_storage_canary_reuses_only_recent_success(self):
        canary = HealthCanary(_config(), cache_seconds=5.0)
        with patch(
            "app.clickhouse_ingest._probe_health",
            side_effect=[0.10, 0.20],
        ) as probe, patch(
            "app.clickhouse_ingest.time.monotonic",
            side_effect=[10.0, 10.1, 12.0, 16.0, 16.2],
        ):
            self.assertEqual(canary.probe(), 0.10)
            self.assertEqual(canary.probe(), 0.10)
            self.assertEqual(canary.probe(), 0.20)
        self.assertEqual(probe.call_count, 2)

    @unittest.skipIf(ingest_one is None, "oss2 is not installed on this host")
    def test_storage_canary_does_not_cache_failure(self):
        canary = HealthCanary(_config(), cache_seconds=5.0)
        with patch(
            "app.clickhouse_ingest._probe_health",
            side_effect=[IngestPaused("slow"), 0.10],
        ) as probe, patch(
            "app.clickhouse_ingest.time.monotonic",
            side_effect=[10.0, 11.0, 11.1],
        ):
            with self.assertRaisesRegex(IngestPaused, "slow"):
                canary.probe()
            self.assertEqual(canary.probe(), 0.10)
        self.assertEqual(probe.call_count, 2)

    @unittest.skipIf(HealthCanary is None, "oss2 is not installed on this host")
    def test_default_storage_canary_rechecks_after_one_second(self):
        canary = HealthCanary(_config())
        with patch(
            "app.clickhouse_ingest._probe_health",
            side_effect=[0.10, 0.20],
        ) as probe, patch(
            "app.clickhouse_ingest.time.monotonic",
            side_effect=[10.0, 10.0, 10.5, 11.1, 11.1],
        ):
            self.assertEqual(canary.probe(), 0.10)
            self.assertEqual(canary.probe(), 0.10)
            self.assertEqual(canary.probe(), 0.20)
        self.assertEqual(probe.call_count, 2)

    @unittest.skipIf(ingest_one is None, "oss2 is not installed on this host")
    def test_known_backlog_throttles_full_metadata_reconciliation(self):
        self.assertEqual(
            _reconcile_interval(_config(), {"pending_parts": 1001}),
            3600.0,
        )
        self.assertEqual(
            _reconcile_interval(_config(), {"pending_parts": 1}),
            600.0,
        )
        self.assertEqual(
            _reconcile_interval(_config(), {"pending_parts": 0}),
            30.0,
        )

    @unittest.skipIf(ingest_one is None, "oss2 is not installed on this host")
    def test_stale_persistent_reconcile_is_due_immediately_after_restart(self):
        now_us = 10_000_000_000
        stats = {
            "pending_parts": 1001,
            "reconcile_completed_at_us": now_us - 3_601_000_000,
        }

        self.assertEqual(
            _initial_reconcile_monotonic(
                _config(),
                stats,
                now_monotonic=50_000.0,
                now_epoch_us=now_us,
            ),
            0.0,
        )

    @unittest.skipIf(ingest_one is None, "oss2 is not installed on this host")
    def test_fresh_persistent_reconcile_keeps_only_remaining_delay(self):
        now_us = 10_000_000_000
        stats = {
            "pending_parts": 1001,
            "reconcile_completed_at_us": now_us - 120_000_000,
        }

        self.assertEqual(
            _initial_reconcile_monotonic(
                _config(),
                stats,
                now_monotonic=50_000.0,
                now_epoch_us=now_us,
            ),
            49_880.0,
        )

    @unittest.skipIf(ingest_one is None, "oss2 is not installed on this host")
    def test_io_pressure_fuse_uses_host_full_avg10(self):
        with tempfile.TemporaryDirectory() as temp:
            pressure = Path(temp) / "io"
            pressure.write_text(
                "some avg10=7.00 avg60=4.00 avg300=2.00 total=1\n"
                "full avg10=6.25 avg60=3.00 avg300=1.00 total=1\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(IngestPaused, "I/O pressure"):
                _probe_io_pressure(_config(), path=pressure)
            pressure.write_text(
                "some avg10=7.00 avg60=4.00 avg300=2.00 total=1\n"
                "full avg10=1.25 avg60=3.00 avg300=1.00 total=1\n",
                encoding="utf-8",
            )
            self.assertEqual(_probe_io_pressure(_config(), path=pressure), 1.25)

    @unittest.skipIf(ingest_one is None, "oss2 is not installed on this host")
    def test_ingest_recovery_hysteresis_preserves_explicit_disable(self):
        enabled = replace(_config(), io_pressure_max_full_avg10=20.0)
        with self.assertRaisesRegex(IngestPaused, "recovery ceiling"):
            _require_ingest_recovery(enabled, 10.01, paused=True)
        self.assertIsNone(
            _require_ingest_recovery(enabled, 10.0, paused=True)
        )
        disabled = replace(_config(), io_pressure_max_full_avg10=0.0)
        self.assertIsNone(
            _require_ingest_recovery(disabled, 99.0, paused=True)
        )
        production_tuned = replace(
            enabled,
            io_pressure_recovery_ratio=0.8,
        )
        with self.assertRaisesRegex(IngestPaused, "recovery ceiling"):
            _require_ingest_recovery(
                production_tuned,
                16.01,
                paused=True,
            )
        self.assertIsNone(
            _require_ingest_recovery(
                production_tuned,
                16.0,
                paused=True,
            )
        )

    @unittest.skipIf(
        _admit_io_pressure is None, "oss2 is not installed on this host"
    )
    def test_generic_high_psi_uses_forced_healthy_serving_canary(self):
        checks: list[bool] = []
        canary = SimpleNamespace(
            probe=lambda *, force=False: checks.append(bool(force)) or 0.01
        )
        pressure = IngestPaused("host I/O pressure exceeded safety ceiling")
        with patch(
            "app.clickhouse_ingest._probe_io_pressure",
            side_effect=pressure,
        ):
            override, reason = _admit_io_pressure(
                _config(), canary, paused=True
            )
        self.assertTrue(override)
        self.assertIs(reason, pressure)
        self.assertEqual(checks, [True])

    @unittest.skipIf(
        _admit_io_pressure is None, "oss2 is not installed on this host"
    )
    def test_generic_high_psi_stays_paused_when_serving_canary_fails(self):
        canary = SimpleNamespace(
            probe=lambda *, force=False: (_ for _ in ()).throw(
                IngestPaused("production health probe failed")
            )
        )
        with patch(
            "app.clickhouse_ingest._probe_io_pressure",
            side_effect=IngestPaused("host I/O pressure exceeded safety ceiling"),
        ):
            with self.assertRaisesRegex(
                IngestPaused, "production health probe failed"
            ):
                _admit_io_pressure(_config(), canary, paused=True)

    @unittest.skipIf(ingest_one is None, "oss2 is not installed on this host")
    @patch("app.clickhouse_ingest.shutil.disk_usage")
    def test_canary_override_allows_one_bounded_part_without_rechecking_psi(
        self, disk_usage
    ):
        disk_usage.return_value = SimpleNamespace(free=500 * 1024**3)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            part = _part(root / "part.parquet", "part-a")
            manifest = ClickHouseManifest(
                root / "manifest.sqlite3", run_migrations=True
            )
            now_us = int(part["max_event_epoch_us"])
            manifest.reconcile(
                [part],
                start_epoch_us=now_us - 1,
                end_epoch_us=now_us,
            )
            with patch(
                "app.clickhouse_ingest._probe_io_pressure",
                side_effect=IngestPaused("host I/O pressure is high"),
            ) as pressure:
                result = ingest_one(
                    _Metadata([part]),
                    manifest,
                    _Client(),
                    _Archive(),
                    _config(),
                    root / "scratch",
                    root,
                    health_probe=lambda: 0.01,
                    allow_high_io_pressure=True,
                )
        self.assertEqual(result["state"], "ready")
        pressure.assert_not_called()

    @unittest.skipIf(ingest_one is None, "oss2 is not installed on this host")
    @patch(
        "app.clickhouse_ingest._probe_io_pressure",
        side_effect=IngestPaused("host I/O pressure is high"),
    )
    def test_io_pressure_pauses_before_manifest_claim(self, _pressure):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            part = _part(root / "part.parquet", "part-a")
            manifest = ClickHouseManifest(
                root / "manifest.sqlite3", run_migrations=True
            )
            now_us = int(part["max_event_epoch_us"])
            manifest.reconcile(
                [part],
                start_epoch_us=now_us - 1,
                end_epoch_us=now_us,
            )
            with self.assertRaisesRegex(IngestPaused, "I/O pressure"):
                ingest_one(
                    _Metadata([part]),
                    manifest,
                    _Client(),
                    _Archive(),
                    _config(),
                    root / "scratch",
                    root,
                )
            with manifest.connection() as connection:
                status = connection.execute(
                    "SELECT status FROM clickhouse_parts"
                ).fetchone()["status"]
            self.assertEqual(status, "pending")

    @unittest.skipIf(ingest_one is None, "oss2 is not installed on this host")
    @patch(
        "app.clickhouse_ingest._probe_io_pressure",
        side_effect=IngestPaused("host I/O pressure is high"),
    )
    def test_io_pressure_pauses_before_reconcile_scan(self, _pressure):
        class _UnscannedMetadata:
            def parts_in_range(self, **_kwargs):
                raise AssertionError("metadata scan must not start")

        with tempfile.TemporaryDirectory() as temp:
            manifest = ClickHouseManifest(
                Path(temp) / "manifest.sqlite3", run_migrations=True
            )
            with self.assertRaisesRegex(IngestPaused, "I/O pressure"):
                reconcile_once(_UnscannedMetadata(), manifest, _config())

    @unittest.skipIf(ingest_one is None, "oss2 is not installed on this host")
    @patch("app.clickhouse_ingest._probe_io_pressure", return_value=0.0)
    @patch("app.clickhouse_ingest.shutil.disk_usage")
    def test_ingest_verifies_and_commits_manifest(self, disk_usage, _pressure):
        disk_usage.return_value = SimpleNamespace(free=500 * 1024**3)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            part = _part(root / "part.parquet", "part-a")
            metadata = _Metadata([part])
            manifest = ClickHouseManifest(
                root / "manifest.sqlite3", run_migrations=True
            )
            now_us = int(part["max_event_epoch_us"])
            manifest.reconcile(
                [part],
                start_epoch_us=now_us - 1,
                end_epoch_us=now_us,
            )
            client = _Client()
            result = ingest_one(
                metadata,
                manifest,
                client,
                _Archive(),
                _config(),
                root / "scratch",
                root,
            )
            self.assertEqual(result["state"], "ready")
            self.assertTrue(manifest.coverage([part])["complete"])
            self.assertFalse(any((root / "scratch").glob("*.parquet")))
            # Merge scheduling is governed across complete iterations. Toggling
            # it around every INSERT aborts in-flight merges and causes I/O
            # amplification instead of compaction.
            self.assertEqual(client.merge_calls, [])

    @unittest.skipIf(ingest_one is None, "oss2 is not installed on this host")
    @patch("app.clickhouse_ingest._probe_io_pressure", return_value=0.0)
    @patch("app.clickhouse_ingest.shutil.disk_usage")
    def test_async_delete_wait_is_paused_without_failure_backoff(
        self, disk_usage, _pressure
    ):
        disk_usage.return_value = SimpleNamespace(free=500 * 1024**3)

        class _AsyncDeleteClient(_Client):
            def delete_part(self, _identity: str):
                # ClickHouse accepted an asynchronous mutation; rows remain
                # visible until the background mutation converges.
                return None

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            part = _part(root / "part.parquet", "part-a")
            manifest = ClickHouseManifest(
                root / "manifest.sqlite3", run_migrations=True
            )
            now_us = int(part["max_event_epoch_us"])
            manifest.reconcile(
                [part],
                start_epoch_us=now_us - 1,
                end_epoch_us=now_us,
            )
            client = _AsyncDeleteClient()
            client.parts["part-a"] = {
                "rows": 1,
                "sha_count": 1,
                "sha256": "0" * 64,
                "min_revision": 0,
                "max_revision": 0,
            }

            with self.assertRaisesRegex(
                IngestPaused, "delete mutation is still applying"
            ):
                ingest_one(
                    _Metadata([part]),
                    manifest,
                    client,
                    _Archive(),
                    _config(),
                    root / "scratch",
                    root,
                )

            with manifest.connection() as connection:
                state = connection.execute(
                    "SELECT status, attempts, next_retry_us FROM clickhouse_parts"
                ).fetchone()
            self.assertEqual(state["status"], "pending")
            self.assertEqual(state["attempts"], 0)
            self.assertGreater(int(state["next_retry_us"]), 0)

    @unittest.skipIf(ingest_one is None, "oss2 is not installed on this host")
    @patch("app.clickhouse_ingest._probe_io_pressure", return_value=0.0)
    @patch("app.clickhouse_ingest.shutil.disk_usage")
    def test_object_ingest_requires_name_table_parity_before_ready(
        self, disk_usage, _pressure
    ):
        disk_usage.return_value = SimpleNamespace(free=500 * 1024**3)

        class _MissingNameClient(_Client):
            def paired_part_states(self, identities):
                result = super().paired_part_states(identities)
                for state in result.values():
                    state.update(
                        {
                            "name_rows": 0,
                            "name_sha_count": 0,
                            "name_sha256": "",
                            "name_min_revision": 0,
                            "name_max_revision": 0,
                        }
                    )
                return result

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            part = _part(root / "part.parquet", "part-a")
            manifest = ClickHouseManifest(
                root / "manifest.sqlite3", run_migrations=True
            )
            now_us = int(part["max_event_epoch_us"])
            manifest.reconcile(
                [part],
                start_epoch_us=now_us - 1,
                end_epoch_us=now_us,
            )

            with self.assertRaisesRegex(RuntimeError, "name_rows=0/3"):
                ingest_one(
                    _Metadata([part]),
                    manifest,
                    _MissingNameClient(),
                    _Archive(),
                    _config(),
                    root / "scratch",
                    root,
                    verify_name_table=True,
                )

            self.assertEqual(manifest.stats()["ready_parts"], 0)
            self.assertEqual(manifest.stats()["failed_parts"], 1)

    @unittest.skipIf(ingest_one is None, "oss2 is not installed on this host")
    @patch("app.clickhouse_ingest._probe_io_pressure", return_value=0.0)
    @patch("app.clickhouse_ingest.shutil.disk_usage")
    def test_ingest_streams_verified_oss_part_without_local_staging(
        self, disk_usage, _pressure
    ):
        disk_usage.return_value = SimpleNamespace(free=500 * 1024**3)
        payload = b"PAR1-streamed-parquet"

        class _StreamingArchive:
            def open_part_reader(self, _part):
                return io.BytesIO(payload)

            def download_part(self, _part, _destination):
                raise AssertionError("streaming ingest must not stage on disk")

        class _StreamingClient(_Client):
            def __init__(self):
                super().__init__()
                self.streamed = b""

            def insert_parquet(self, *_args, **_kwargs):
                raise AssertionError("streaming ingest must not use a file path")

            def insert_parquet_stream(
                self,
                source,
                *,
                content_length,
                part_key,
                sha256,
                content_revision,
            ):
                self.streamed = source.read()
                self.asserted_length = content_length
                self.parts[part_key] = {
                    "rows": 3,
                    "sha_count": 1,
                    "sha256": sha256,
                    "min_revision": content_revision,
                    "max_revision": content_revision,
                }

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            part = _part(root / "part.parquet", "part-stream")
            part["size_bytes"] = len(payload)
            part["sha256"] = hashlib.sha256(payload).hexdigest()
            metadata = _Metadata([part])
            manifest = ClickHouseManifest(
                root / "manifest.sqlite3", run_migrations=True
            )
            now_us = int(part["max_event_epoch_us"])
            manifest.reconcile(
                [part],
                start_epoch_us=now_us - 1,
                end_epoch_us=now_us,
            )
            client = _StreamingClient()

            result = ingest_one(
                metadata,
                manifest,
                client,
                _StreamingArchive(),
                _config(),
                root / "scratch",
                root,
            )

            self.assertEqual(result["state"], "ready")
            self.assertEqual(client.streamed, payload)
            self.assertEqual(client.asserted_length, len(payload))
            self.assertFalse((root / "scratch").exists())
            self.assertTrue(manifest.coverage([part])["complete"])

    @unittest.skipIf(ingest_one is None, "oss2 is not installed on this host")
    @patch("app.clickhouse_ingest._probe_io_pressure", return_value=0.0)
    @patch("app.clickhouse_ingest.shutil.disk_usage")
    @patch("app.clickhouse_ingest.time.sleep")
    def test_ingest_retries_transient_metadata_lock(
        self, sleep, disk_usage, _pressure
    ):
        disk_usage.return_value = SimpleNamespace(free=500 * 1024**3)
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            part = _part(root / "part.parquet", "part-a")
            metadata = _TemporarilyLockedMetadata([part], locked_reads=2)
            manifest = ClickHouseManifest(
                root / "manifest.sqlite3", run_migrations=True
            )
            now_us = int(part["max_event_epoch_us"])
            manifest.reconcile(
                [part],
                start_epoch_us=now_us - 1,
                end_epoch_us=now_us,
            )
            result = ingest_one(
                metadata,
                manifest,
                _Client(),
                _Archive(),
                _config(),
                root / "scratch",
                root,
            )
            self.assertEqual(result["state"], "ready")
            self.assertEqual(metadata.reads, 4)
            self.assertEqual(sleep.call_count, 2)
            self.assertEqual(manifest.stats()["failed_parts"], 0)


class ClickHouseQueryTests(unittest.TestCase):
    def test_query_requires_full_manifest_coverage(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            part = _part(root / "part.parquet", "part-a")
            metadata = _Metadata([part])
            manifest = ClickHouseManifest(
                root / "manifest.sqlite3", run_migrations=True
            )
            client = _Client()
            backend = ClickHouseQueryBackend(
                metadata,
                root,
                _config(),
                client=client,
                manifest=manifest,
            )
            now_us = int(part["max_event_epoch_us"])
            query = {
                "source": "binlog",
                "start_epoch_us": now_us - 60_000_000,
                "end_epoch_us": now_us,
                "database": "biz",
                "keyword": "order 42",
                "operations": ["UPDATE"],
                "limit": 100,
            }
            self.assertIsNone(
                backend.query_events(query, retention_days=60, limit_cap=1000)
            )
            manifest.reconcile(
                [part],
                start_epoch_us=now_us - 3_600_000_000,
                end_epoch_us=now_us,
            )
            manifest.claim_next()
            manifest.mark_ready(str(part["path"]), "part-a", 3)
            result = backend.query_events(
                query, retention_days=60, limit_cap=1000
            )
            self.assertEqual(result["tiers_used"], ["clickhouse-hot"])
            self.assertEqual(result["rows"][0]["event_id"], "event-1")
            sql, parameters = client.queries[-1]
            self.assertIn("positionCaseInsensitiveUTF8", sql)
            self.assertIn("operation IN", sql)
            # LIMIT BY retains one key for every row in a wide time window and
            # can exhaust ClickHouse memory before the final LIMIT is applied.
            # The hot path reads a small ordered batch and deduplicates it in
            # application code instead.
            self.assertNotIn("LIMIT 1 BY event_id", sql)
            self.assertIn("LIMIT {raw_limit:UInt64}", sql)
            self.assertIn("FROM insight.events_query", sql)
            self.assertIn("event_date >=", sql)
            self.assertIn("_content_revision DESC", sql)
            self.assertIn("biz", parameters.values())

    def test_query_resolves_fuzzy_names_then_reads_exact_name_ranges(self):
        class _NameAwareClient(_Client):
            def json_rows(self, sql: str, *, parameters=None, **_kwargs):
                values = dict(parameters or {})
                self.queries.append((sql, values))
                if "GROUP BY instance_id, database_name, table_name" in sql:
                    return [
                        {
                            "instance_id": "rm-test",
                            "database_name": "example_source",
                            "table_name": "sys_users",
                            "physical_rows": 120,
                        },
                        {
                            "instance_id": "rm-test",
                            "database_name": "example_source",
                            "table_name": "example_shop,sys_users",
                            "physical_rows": 10,
                        },
                    ]
                return [
                    {
                        "event_id": "event-1",
                        "event_epoch_us": 10,
                        "locator": "part-a:*",
                    }
                ]

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            part = _part(root / "part.parquet", "part-a")
            metadata = _Metadata([part])
            manifest = ClickHouseManifest(
                root / "manifest.sqlite3", run_migrations=True
            )
            now_us = int(part["max_event_epoch_us"])
            manifest.reconcile(
                [part],
                start_epoch_us=now_us - 3_600_000_000,
                end_epoch_us=now_us,
            )
            manifest.claim_next()
            manifest.mark_ready(str(part["path"]), "part-a", 3)
            client = _NameAwareClient()
            backend = ClickHouseQueryBackend(
                metadata,
                root,
                _config(),
                client=client,
                manifest=manifest,
            )

            result = backend.query_events(
                {
                    "source": "binlog",
                    "start_epoch_us": now_us - 60_000_000,
                    "end_epoch_us": now_us,
                    "instance": "RM-TEST",
                    "database": "example_app",
                    "table": "sys_users",
                    "limit": 100,
                },
                retention_days=60,
                limit_cap=1000,
            )

            self.assertEqual(result["rows"][0]["event_id"], "event-1")
            self.assertEqual(len(client.queries), 2)
            resolver_sql, resolver_parameters = client.queries[0]
            event_sql, event_parameters = client.queries[1]
            self.assertIn("toStartOfHour(event_time_utc)", resolver_sql)
            self.assertIn(
                "GROUP BY instance_id, database_name, table_name", resolver_sql
            )
            self.assertIn("count() AS physical_rows", resolver_sql)
            self.assertIn("RM-TEST", resolver_parameters.values())
            self.assertIn(
                "(instance_id, database_name, table_name) IN", event_sql
            )
            self.assertIn("FROM insight.events_query_by_name", event_sql)
            self.assertNotIn(
                "positionCaseInsensitiveUTF8(database_name", event_sql
            )
            self.assertNotIn(
                "positionCaseInsensitiveUTF8(table_name", event_sql
            )
            self.assertIn("sys_users", event_parameters.values())
            self.assertIn("example_shop,sys_users", event_parameters.values())

    def test_query_returns_empty_without_scanning_when_name_resolver_has_no_match(self):
        class _NoNameClient(_Client):
            def json_rows(self, sql: str, *, parameters=None, **_kwargs):
                self.queries.append((sql, dict(parameters or {})))
                return []

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            part = _part(root / "part.parquet", "part-a")
            metadata = _Metadata([part])
            manifest = ClickHouseManifest(
                root / "manifest.sqlite3", run_migrations=True
            )
            now_us = int(part["max_event_epoch_us"])
            manifest.reconcile(
                [part],
                start_epoch_us=now_us - 3_600_000_000,
                end_epoch_us=now_us,
            )
            manifest.claim_next()
            manifest.mark_ready(str(part["path"]), "part-a", 3)
            client = _NoNameClient()
            backend = ClickHouseQueryBackend(
                metadata,
                root,
                _config(),
                client=client,
                manifest=manifest,
            )

            result = backend.query_events(
                {
                    "source": "binlog",
                    "start_epoch_us": now_us - 60_000_000,
                    "end_epoch_us": now_us,
                    "instance": "rm-test",
                    "database": "missing-db",
                    "table": "missing-table",
                    "limit": 100,
                },
                retention_days=60,
                limit_cap=1000,
            )

            self.assertEqual(result["rows"], [])
            self.assertFalse(result["has_more"])
            self.assertEqual(len(client.queries), 1)
            self.assertIn(
                "GROUP BY instance_id, database_name, table_name",
                client.queries[0][0],
            )

    def test_query_deduplicates_ordered_batches_and_continues_when_needed(self):
        class _PagedClient(_Client):
            def __init__(self):
                super().__init__()
                self.rows = [
                    {"event_id": "event-4", "event_epoch_us": 400 - index}
                    for index in range(256)
                ] + [
                    {"event_id": "event-3", "event_epoch_us": 300},
                    {"event_id": "event-2", "event_epoch_us": 200},
                    {"event_id": "event-1", "event_epoch_us": 100},
                    {"event_id": "event-0", "event_epoch_us": 0},
                ]

            def json_rows(self, sql: str, *, parameters=None, **_kwargs):
                values = dict(parameters or {})
                self.queries.append((sql, values))
                start = int(values["raw_offset"])
                size = int(values["raw_limit"])
                return self.rows[start : start + size]

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            part = _part(root / "part.parquet", "part-a")
            metadata = _Metadata([part])
            manifest = ClickHouseManifest(
                root / "manifest.sqlite3", run_migrations=True
            )
            now_us = int(part["max_event_epoch_us"])
            manifest.reconcile(
                [part],
                start_epoch_us=now_us - 3_600_000_000,
                end_epoch_us=now_us,
            )
            manifest.claim_next()
            manifest.mark_ready(str(part["path"]), "part-a", 3)
            client = _PagedClient()
            backend = ClickHouseQueryBackend(
                metadata,
                root,
                _config(),
                client=client,
                manifest=manifest,
            )
            result = backend.query_events(
                {
                    "source": "binlog",
                    "start_epoch_us": now_us - 60_000_000,
                    "end_epoch_us": now_us,
                    "offset": 1,
                    "limit": 2,
                },
                retention_days=60,
                limit_cap=1000,
            )
            self.assertEqual(
                [row["event_id"] for row in result["rows"]],
                ["event-3", "event-2"],
            )
            self.assertTrue(result["has_more"])
            self.assertEqual(len(client.queries), 2)
            self.assertEqual(client.queries[0][1]["raw_offset"], 0)
            self.assertEqual(client.queries[1][1]["raw_offset"], 256)


if __name__ == "__main__":
    unittest.main()
