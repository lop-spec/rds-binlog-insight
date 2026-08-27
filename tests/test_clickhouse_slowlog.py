from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow.parquet as pq

from app.clickhouse_manifest import ClickHouseManifest
from app.clickhouse_migrate import main as clickhouse_migrate_main
from app.clickhouse_slowlog import (
    ClickHouseSlowLogQueryBackend,
    ClickHouseSlowLogWorker,
    SourceIndexLagPaused,
    SourceIndexPriorityGate,
    ingest_slowlog_batch,
)
from app.io_pressure import IoPressurePaused
from app.slowlog_index import SQL_ORDERS, SlowLogIndex, slowlog_order_key


def _part(path: Path, identity: str = "part-v1") -> dict[str, object]:
    now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
    return {
        "path": str(path),
        "logical_part_id": identity,
        "sha256": f"sha-{identity}",
        "content_revision": 1,
        "min_event_epoch_us": now_us - 60_000_000,
        "max_event_epoch_us": now_us,
        "row_count": 1,
        "size_bytes": 128,
        "instance_id": "rm-prod",
    }


class _Manifest:
    def __init__(self, complete: bool) -> None:
        self.complete = complete
        self.coverage_calls = 0

    def coverage(self, parts):
        self.coverage_calls += 1
        missing = [] if self.complete else [str(parts[0]["path"])]
        return {
            "complete": self.complete,
            "total_parts": len(parts),
            "covered_parts": len(parts) - len(missing),
            "covered_rows": 1 if self.complete else 0,
            "missing_parts": missing,
            "reconcile_completed_at_us": 1,
        }

    def stats(self):
        return {"ready_parts": 1 if self.complete else 0}


class _Metadata:
    def __init__(self, parts):
        self.parts = list(parts)

    def parts_in_range(self, **_kwargs):
        return list(self.parts)

    def part_by_path(self, path):
        return next(
            (part for part in self.parts if str(part["path"]) == str(path)),
            None,
        )


class _Client:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.query_kwargs: list[dict[str, object]] = []

    def json_rows(self, sql, **_kwargs):
        self.queries.append(sql)
        self.query_kwargs.append(dict(_kwargs))
        if "slowlog:rollup" in sql:
            return [
                {
                    "grouping_mask": 15,
                    "group_fingerprint": "f" * 32,
                    "group_database_name": "",
                    "group_table_name": "",
                    "group_operation": "",
                    "group_ts": 0,
                    "executions": 3,
                    "scan_rows": 30,
                    "scan_rows_max": 20,
                    "rows_sent": 6,
                    "rows_sent_max": 4,
                    "query_time_ms_total": 90,
                    "query_time_ms_max": 60,
                    "lock_time_ms_total": 9,
                    "lock_time_ms_max": 6,
                    "sql_bytes": 120,
                    "first_epoch_us": 1_000_000,
                    "last_epoch_us": 2_000_000,
                    "objects": 1,
                    "instance_id": "rm-prod",
                    "database_name": "biz",
                    "table_name": "orders",
                    "operation": "SELECT",
                    "sql_id": "sql-1",
                    "sample_event_id": "event-1",
                    "action": "SELECT",
                    "instance_ids": ["rm-prod"],
                    "fingerprints": 1,
                },
                {
                    "grouping_mask": 19,
                    "group_fingerprint": "",
                    "group_database_name": "biz",
                    "group_table_name": "orders",
                    "group_operation": "",
                    "group_ts": 0,
                    "database_name": "biz",
                    "table_name": "orders",
                    "executions": 3,
                    "sql_bytes": 120,
                    "fingerprints": 1,
                    "scan_rows": 30,
                    "rows_sent": 6,
                    "query_time_ms_total": 90,
                },
                {
                    "grouping_mask": 29,
                    "group_fingerprint": "",
                    "group_database_name": "",
                    "group_table_name": "",
                    "group_operation": "SELECT",
                    "group_ts": 0,
                    "operation": "SELECT",
                    "executions": 3,
                    "sql_bytes": 120,
                    "scan_rows": 30,
                },
                {
                    "grouping_mask": 30,
                    "group_fingerprint": "",
                    "group_database_name": "",
                    "group_table_name": "",
                    "group_operation": "",
                    "group_ts": 0,
                    "executions": 3,
                    "query_time_ms_total": 90,
                    "scan_rows": 30,
                    "rows_sent": 6,
                },
            ]
        raise AssertionError(sql)


class _StatementIndex:
    def __init__(self) -> None:
        self.calls: list[list[tuple[str, str]]] = []

    def statement_profiles(self, keys):
        batch = list(keys)
        self.calls.append(batch)
        return {
            ("rm-prod", "f" * 32): {
                "instance_id": "rm-prod",
                "fingerprint": "f" * 32,
                "sql_id": "sql-1",
                "action": "SELECT",
                "normalized_sql": "SELECT * FROM orders WHERE id=?",
                "sample_sql": "SELECT * FROM orders WHERE id=1",
            }
        }


class _IngestClient:
    def __init__(self) -> None:
        self.rows: dict[tuple[str, str], int] = {}
        self.deleted: list[tuple[str, str]] = []
        self.inserts = 0

    def delete_occurrences(self, jobs):
        for job in jobs:
            key = (str(job["part_path"]), str(job["logical_part_id"]))
            self.deleted.append(key)
            self.rows.pop(key, None)

    def insert_parquet(self, path):
        self.inserts += 1
        counts: dict[tuple[str, str], int] = {}
        for row in pq.read_table(path).to_pylist():
            key = (row["_source_part_path"], row["_source_part_id"])
            counts[key] = counts.get(key, 0) + 1
        self.rows.update(counts)

    def part_counts(self, jobs):
        return {
            (str(job["part_path"]), str(job["logical_part_id"])): self.rows.get(
                (str(job["part_path"]), str(job["logical_part_id"])), 0
            )
            for job in jobs
        }


class ClickHouseSlowLogSchemaTests(unittest.TestCase):
    def test_source_index_priority_gate_yields_while_source_worker_is_paused(self):
        with tempfile.TemporaryDirectory() as directory:
            status_path = Path(directory) / "slowlog-worker-status.json"
            now = datetime(2026, 8, 25, 5, 0, tzinfo=UTC)
            status_path.write_text(
                json.dumps(
                    {
                        "running": True,
                        "state": "paused",
                        "updatedAt": now.isoformat().replace("+00:00", "Z"),
                        "stats": {
                            "pending_parts": 1,
                            "failed_parts": 0,
                            "oldest_pending_age_seconds": 1,
                            "reconcile_complete": True,
                        },
                    }
                ),
                encoding="utf-8",
            )
            gate = SourceIndexPriorityGate(status_path)
            with self.assertRaisesRegex(
                SourceIndexLagPaused,
                "source slow-log worker is paused",
            ):
                gate.check(now=now)

    def test_clickhouse_lane_may_use_its_own_fuses_when_source_is_clean(self):
        with tempfile.TemporaryDirectory() as directory:
            status_path = Path(directory) / "slowlog-worker-status.json"
            now = datetime(2026, 8, 25, 5, 0, tzinfo=UTC)
            status_path.write_text(
                json.dumps(
                    {
                        "running": True,
                        "state": "paused",
                        "updatedAt": now.isoformat().replace("+00:00", "Z"),
                        "stats": {
                            "pending_parts": 0,
                            "failed_parts": 0,
                            "oldest_pending_age_seconds": 0,
                            "reconcile_complete": True,
                        },
                    }
                ),
                encoding="utf-8",
            )
            gate = SourceIndexPriorityGate(
                status_path,
                block_paused_state=False,
            )
            self.assertEqual(gate.check(now=now)["pending_parts"], 0)

    def test_source_index_priority_gate_requires_hysteresis_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            status_path = Path(directory) / "slowlog-worker-status.json"
            now = datetime(2026, 8, 25, 5, 0, tzinfo=UTC)

            def write_status(*, pending: int, age: int) -> None:
                status_path.write_text(
                    json.dumps(
                        {
                            "running": True,
                            "state": "completed",
                            "updatedAt": now.isoformat().replace("+00:00", "Z"),
                            "stats": {
                                "pending_parts": pending,
                                "failed_parts": 0,
                                "oldest_pending_age_seconds": age,
                                "reconcile_complete": True,
                            },
                        }
                    ),
                    encoding="utf-8",
                )

            gate = SourceIndexPriorityGate(
                status_path,
                max_pending_parts=128,
                max_pending_age_seconds=600,
                recovery_ratio=0.8,
                max_stale_seconds=30,
            )
            write_status(pending=129, age=601)
            with self.assertRaises(SourceIndexLagPaused):
                gate.check(now=now)

            # Crossing only the admission ceiling is insufficient after a
            # pause; both values must reach the lower recovery ceiling.
            write_status(pending=110, age=500)
            with self.assertRaises(SourceIndexLagPaused):
                gate.check(now=now + timedelta(seconds=1))

            write_status(pending=100, age=400)
            status = gate.check(now=now + timedelta(seconds=2))
            self.assertEqual(status["pending_parts"], 100)

    def test_clickhouse_slowlog_worker_paces_successful_part(self):
        class _Stopping:
            def __init__(self) -> None:
                self.stopped = False
                self.delays: list[float] = []

            def is_set(self) -> bool:
                return self.stopped

            def set(self) -> None:
                self.stopped = True

            def wait(self, delay: float) -> bool:
                self.delays.append(delay)
                self.stopped = True
                return True

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = SimpleNamespace(
                enabled=True,
                base=SimpleNamespace(
                    io_pressure_recovery_ratio=0.8,
                    min_free_gb=1,
                ),
                idle_seconds=5.0,
                reconcile_seconds=300,
                retention_days=61,
                batch_parts=1,
            )
            worker = ClickHouseSlowLogWorker(root, config)
            stopping = _Stopping()
            worker.stopping = stopping
            source_checks: list[bool] = []
            canary_checks: list[bool] = []
            manifest = SimpleNamespace(
                recover_loading=lambda: None,
                stats=lambda: {"pending_parts": 1},
            )
            with (
                patch(
                    "app.config.ensure_data_dirs",
                    return_value={
                        "index": root,
                        "logs": root,
                        "scratch": root,
                    },
                ),
                patch("app.metadata.MetadataStore"),
                patch("app.clickhouse_slowlog.SlowLogIndex"),
                patch(
                    "app.clickhouse_slowlog.ClickHouseManifest",
                    return_value=manifest,
                ),
                patch("app.clickhouse_slowlog.ClickHouseSlowLogClient"),
                patch(
                    "app.io_pressure.IoPressureGate.from_env",
                    return_value=SimpleNamespace(check=lambda: 0.0),
                ),
                patch(
                    "app.clickhouse_ingest.HealthCanary",
                    return_value=SimpleNamespace(
                        probe=lambda *, force=False: canary_checks.append(
                            bool(force)
                        )
                    ),
                ),
                patch(
                    "app.clickhouse_slowlog.SourceIndexPriorityGate",
                    return_value=SimpleNamespace(
                        check=lambda: source_checks.append(True)
                    ),
                ),
                patch("app.clickhouse_slowlog.shutil.disk_usage") as usage,
                patch(
                    "app.clickhouse_slowlog.reconcile_slowlog_manifest",
                    return_value={},
                ),
                patch(
                    "app.clickhouse_slowlog.ingest_slowlog_batch",
                    return_value={"claimed_parts": 1},
                ),
                patch("app.maintenance_status.write_json_status"),
            ):
                usage.return_value.free = 1024**4
                self.assertEqual(worker.run(), 0)

            self.assertEqual(stopping.delays, [5.0])
            self.assertEqual(source_checks, [True])
            self.assertEqual(canary_checks, [False, True])

    def test_clickhouse_slowlog_worker_uses_canary_for_false_io_pressure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = SimpleNamespace(
                enabled=True,
                base=SimpleNamespace(
                    io_pressure_recovery_ratio=0.8,
                    min_free_gb=1,
                ),
                idle_seconds=5.0,
                reconcile_seconds=300,
                retention_days=61,
                batch_parts=1,
            )
            worker = ClickHouseSlowLogWorker(root, config)
            source_checks: list[bool] = []
            canary_checks: list[bool] = []
            manifest = SimpleNamespace(
                recover_loading=lambda: None,
                stats=lambda: {"pending_parts": 1},
            )

            def pause_for_pressure() -> float:
                worker.stopping.set()
                raise IoPressurePaused(
                    "host I/O pressure exceeded safety ceiling"
                )

            with (
                patch(
                    "app.config.ensure_data_dirs",
                    return_value={
                        "index": root,
                        "logs": root,
                        "scratch": root,
                    },
                ),
                patch("app.metadata.MetadataStore"),
                patch("app.clickhouse_slowlog.SlowLogIndex"),
                patch(
                    "app.clickhouse_slowlog.ClickHouseManifest",
                    return_value=manifest,
                ),
                patch("app.clickhouse_slowlog.ClickHouseSlowLogClient"),
                patch(
                    "app.io_pressure.IoPressureGate.from_env",
                    return_value=SimpleNamespace(check=pause_for_pressure),
                ),
                patch(
                    "app.clickhouse_ingest.HealthCanary",
                    return_value=SimpleNamespace(
                        probe=lambda *, force=False: canary_checks.append(
                            bool(force)
                        )
                    ),
                ),
                patch(
                    "app.clickhouse_slowlog.SourceIndexPriorityGate",
                    return_value=SimpleNamespace(
                        check=lambda: source_checks.append(True)
                    ),
                ),
                patch("app.clickhouse_slowlog.shutil.disk_usage") as usage,
                patch(
                    "app.clickhouse_slowlog.reconcile_slowlog_manifest",
                    return_value={},
                ),
                patch(
                    "app.clickhouse_slowlog.ingest_slowlog_batch",
                    return_value={"claimed_parts": 1},
                ) as ingest,
                patch("app.maintenance_status.write_json_status") as status,
            ):
                usage.return_value.free = 1024**4
                self.assertEqual(worker.run(), 0)

            ingest.assert_called_once()
            self.assertEqual(source_checks, [True])
            self.assertEqual(canary_checks, [True, True])
            completed = [
                call.args[1]
                for call in status.call_args_list
                if call.args[1]["state"] == "completed"
            ]
            self.assertEqual(len(completed), 1)
            self.assertTrue(
                completed[0]["result"]["ioPressureCanaryOverride"]
            )

    def test_schema_is_time_first_plain_mergetree_without_default_wide_projection(self):
        root = Path(__file__).resolve().parents[1]
        schema = (root / "clickhouse" / "003_slowlog_events.sql").read_text(
            encoding="utf-8"
        )
        self.assertIn("CREATE TABLE IF NOT EXISTS insight.slowlog_events", schema)
        self.assertIn("ENGINE = MergeTree", schema)
        self.assertNotIn("ReplacingMergeTree", schema)
        self.assertIn("PRIMARY KEY (instance_id, event_epoch_us)", schema)
        self.assertIn("ORDER BY\n(\n    instance_id,\n    event_epoch_us", schema)
        self.assertIn("TTL event_date + INTERVAL 61 DAY DELETE", schema)
        migration = (root / "app" / "clickhouse_migrate.py").read_text(
            encoding="utf-8"
        )
        self.assertNotIn(
            'Path("/app/clickhouse/004_slowlog_canonical_projection.sql")',
            migration,
        )

    def test_explicit_migration_creates_both_independent_manifests(self):
        with tempfile.TemporaryDirectory() as temp, patch(
            "sys.argv",
            [
                "clickhouse-migrate",
                "--data-dir",
                temp,
                "--manifest-only",
            ],
        ):
            self.assertEqual(clickhouse_migrate_main(), 0)
            root = Path(temp) / "index" / "clickhouse"
            self.assertTrue((root / "manifest.sqlite3").is_file())
            self.assertTrue((root / "slowlog-manifest.sqlite3").is_file())

    def test_explicit_migration_splits_comments_and_quoted_semicolons(self):
        with tempfile.TemporaryDirectory() as temp:
            schema_path = Path(temp) / "schema.sql"
            schema_path.write_text(
                """
                CREATE DATABASE IF NOT EXISTS insight;
                -- A comment may contain punctuation; it is not a statement.
                CREATE TABLE IF NOT EXISTS insight.probe
                (value String DEFAULT ';') ENGINE = Memory;
                /* A block comment may contain a semicolon; too. */
                ALTER TABLE insight.probe COMMENT COLUMN value 'kept;whole';
                """,
                encoding="utf-8",
            )
            with patch(
                "sys.argv",
                [
                    "clickhouse-migrate",
                    "--data-dir",
                    temp,
                    "--schema",
                    str(schema_path),
                ],
            ), patch(
                "app.clickhouse_migrate.ClickHouseClient"
            ) as client_class:
                client_class.return_value.ping.return_value = "test"
                self.assertEqual(clickhouse_migrate_main(), 0)

            statements = [
                call.args[0]
                for call in client_class.return_value.query.call_args_list
            ]
            self.assertEqual(len(statements), 3)
            self.assertTrue(statements[0].startswith("CREATE DATABASE"))
            self.assertIn("CREATE TABLE", statements[1])
            self.assertIn("DEFAULT ';'", statements[1])
            self.assertIn("ALTER TABLE", statements[2])
            self.assertIn("'kept;whole'", statements[2])

    def test_compose_keeps_slowlog_serving_off_and_bounds_the_worker(self):
        root = Path(__file__).resolve().parents[1]
        compose = (root / "compose.yaml").read_text(encoding="utf-8")
        self.assertIn(
            'RDS_BINLOG_CLICKHOUSE_SLOWLOG_SERVING_ENABLED: "${RDS_BINLOG_CLICKHOUSE_SLOWLOG_SERVING_ENABLED:-0}"',
            compose,
        )
        service = compose.split("  clickhouse-slowlog-ingester:\n", 1)[1]
        self.assertIn("app.clickhouse_slowlog", service)
        self.assertIn(
            'RDS_BINLOG_CLICKHOUSE_IO_FULL_AVG10_MAX: "${RDS_BINLOG_CLICKHOUSE_SLOWLOG_BACKFILL_IO_FULL_AVG10_MAX:-10}"',
            service,
        )
        self.assertIn(
            'RDS_BINLOG_CLICKHOUSE_IO_RECOVERY_RATIO: "0.8"',
            service,
        )
        self.assertIn(
            'RDS_BINLOG_CLICKHOUSE_SLOWLOG_BATCH_PARTS: "64"',
            service,
        )
        self.assertIn(
            'RDS_BINLOG_CLICKHOUSE_SLOWLOG_IDLE_SECONDS: "5"',
            service,
        )
        self.assertIn(
            'RDS_BINLOG_CLICKHOUSE_SLOWLOG_SOURCE_MAX_PENDING_PARTS: "128"',
            service,
        )
        self.assertIn(
            'RDS_BINLOG_CLICKHOUSE_SLOWLOG_SOURCE_MAX_PENDING_AGE_SECONDS: "600"',
            service,
        )
        self.assertIn(
            'RDS_BINLOG_CLICKHOUSE_SLOWLOG_SOURCE_RECOVERY_RATIO: "0.8"',
            service,
        )
        self.assertNotIn("device_read_bps", service)
        self.assertNotIn("device_write_bps", service)

        indexer_service = compose.split("  slowlog-indexer:\n", 1)[1].split(
            "  clickhouse:\n",
            1,
        )[0]
        self.assertIn(
            'RDS_BINLOG_SLOWLOG_IO_RECOVERY_RATIO: "0.8"',
            indexer_service,
        )
        self.assertIn('RDS_BINLOG_SLOWLOG_BATCH: "1"', indexer_service)
        self.assertIn('RDS_BINLOG_SLOWLOG_WORKERS: "1"', indexer_service)
        self.assertIn('RDS_BINLOG_SLOWLOG_RECONCILE_BATCH: "128"', indexer_service)
        self.assertIn(
            "RDS_BINLOG_CLICKHOUSE_HEALTH_URL: http://insight:8769/api/storage",
            indexer_service,
        )
        self.assertNotIn("RDS_BINLOG_REALTIME_BATCH", indexer_service)
        self.assertNotIn("RDS_BINLOG_REALTIME_WORKERS", indexer_service)
        self.assertIn("      - --idle-seconds\n      - \"0.2\"", indexer_service)


class ClickHouseSlowLogExportTests(unittest.TestCase):
    def test_export_reuses_exact_index_fingerprint_and_part_identity(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            index = SlowLogIndex(root / "slowlog.sqlite3", run_migrations=True)
            part = _part(root / "part.parquet")
            with index.connection() as connection:
                connection.execute(
                    """
                    INSERT INTO slowlog_parts(
                        part_path,logical_part_id,object_sha256,content_revision,
                        instance_id,row_count,indexed_rows,min_event_epoch_us,
                        max_event_epoch_us,format_version,indexed_at_us
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        part["path"], part["logical_part_id"], part["sha256"], 1,
                        "rm-prod", 1, 1, 1_000_000, 2_000_000, 8, 1,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO slowlog_events(
                        event_id,part_path,event_epoch_us,instance_id,node_id,
                        operation,database_name,table_name,fingerprint,sql_id,
                        sql_bytes,query_time_ms,lock_time_ms,rows_examined,
                        rows_sent,database_account,client_ip,thread_id,
                        source_file_name,is_canonical
                    ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
                    """,
                    (
                        "event-1", part["path"], 1_500_000, "rm-prod", "node-1",
                        "SELECT", "biz", "orders", "f" * 32, "sql-1", 40,
                        30, 3, 10, 2, "app", "10.0.0.1", 7, "slowlog-1",
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO slowlog_statements(
                        instance_id,fingerprint,sql_id,action,normalized_sql,
                        sample_sql,first_seen_us,last_seen_us
                    ) VALUES(?,?,?,?,?,?,?,?)
                    """,
                    (
                        "rm-prod", "f" * 32, "sql-1", "SELECT",
                        "SELECT * FROM orders WHERE id=?",
                        "SELECT * FROM orders WHERE id=1", 1_500_000, 1_500_000,
                    ),
                )
            destination = root / "batch.parquet"
            result = index.export_clickhouse_parts([part], destination)
            self.assertEqual(result["part_rows"], {str(part["path"]): 1})
            row = pq.read_table(destination).to_pylist()[0]
            self.assertEqual(row["fingerprint"], "f" * 32)
            self.assertEqual(row["_source_part_id"], "part-v1")
            self.assertEqual(row["normalized_sql"], "SELECT * FROM orders WHERE id=?")

    def test_statement_profiles_are_bounded_by_requested_instance_fingerprints(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            index = SlowLogIndex(root / "slowlog.sqlite3", run_migrations=True)
            with index.connection() as connection:
                connection.executemany(
                    """
                    INSERT INTO slowlog_statements(
                        instance_id,fingerprint,sql_id,action,normalized_sql,
                        sample_sql,first_seen_us,last_seen_us
                    ) VALUES(?,?,?,?,?,?,?,?)
                    """,
                    [
                        ("rm-a", "fp-a", "sql-a", "SELECT", "SELECT ?", "SELECT 1", 1, 2),
                        ("rm-b", "fp-b", "sql-b", "UPDATE", "UPDATE t", "UPDATE t", 1, 2),
                    ],
                )
            profiles = index.statement_profiles(
                [("rm-a", "fp-a"), ("rm-a", "fp-a"), ("rm-x", "missing")]
            )
            self.assertEqual(set(profiles), {("rm-a", "fp-a")})
            self.assertEqual(profiles[("rm-a", "fp-a")]["sample_sql"], "SELECT 1")


class ClickHouseSlowLogIngestTests(unittest.TestCase):
    @staticmethod
    def _seed(index: SlowLogIndex, part: dict[str, object]) -> None:
        identity = str(part["logical_part_id"])
        event_id = f"event-{identity}"
        fingerprint = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]
        sql_id = f"sql-{identity}"
        with index.connection() as connection:
            connection.execute(
                """
                INSERT INTO slowlog_parts(
                    part_path,logical_part_id,object_sha256,content_revision,
                    instance_id,row_count,indexed_rows,min_event_epoch_us,
                    max_event_epoch_us,format_version,indexed_at_us
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    part["path"], part["logical_part_id"], part["sha256"], 1,
                    "rm-prod", 1, 1, part["min_event_epoch_us"],
                    part["max_event_epoch_us"], 8, 1,
                ),
            )
            connection.execute(
                """
                INSERT INTO slowlog_events(
                    event_id,part_path,event_epoch_us,instance_id,node_id,
                    operation,database_name,table_name,fingerprint,sql_id,
                    sql_bytes,query_time_ms,lock_time_ms,rows_examined,
                    rows_sent,database_account,client_ip,thread_id,
                    source_file_name,is_canonical
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1)
                """,
                (
                    event_id, part["path"], part["max_event_epoch_us"],
                    "rm-prod", "node-1", "SELECT", "biz", "orders",
                    fingerprint, sql_id, 40, 30, 3, 10, 2, "app",
                    "10.0.0.1", 7, "slowlog-1",
                ),
            )
            connection.execute(
                """
                INSERT INTO slowlog_statements(
                    instance_id,fingerprint,sql_id,action,normalized_sql,
                    sample_sql,first_seen_us,last_seen_us
                ) VALUES(?,?,?,?,?,?,?,?)
                """,
                (
                    "rm-prod", fingerprint, sql_id, "SELECT",
                    "SELECT * FROM orders WHERE id=?",
                    "SELECT * FROM orders WHERE id=1",
                    part["max_event_epoch_us"], part["max_event_epoch_us"],
                ),
            )

    def test_batch_is_delete_then_insert_and_manifest_commits_exact_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            part = _part(root / "part.parquet")
            index = SlowLogIndex(root / "slowlog.sqlite3", run_migrations=True)
            self._seed(index, part)
            manifest = ClickHouseManifest(
                root / "manifest.sqlite3", run_migrations=True
            )
            manifest.reconcile(
                [part],
                start_epoch_us=int(part["min_event_epoch_us"]),
                end_epoch_us=int(part["max_event_epoch_us"]),
            )
            client = _IngestClient()
            metadata = _Metadata([part])
            result = ingest_slowlog_batch(
                metadata,
                index,
                manifest,
                client,
                batch_parts=64,
                scratch=root / "scratch",
            )
            self.assertEqual(result["loaded_parts"], 1)
            self.assertEqual(result["loaded_rows"], 1)
            self.assertTrue(manifest.coverage([part])["complete"])
            key = (str(part["path"]), str(part["logical_part_id"]))
            self.assertEqual(client.deleted, [key])
            self.assertEqual(client.rows[key], 1)
            self.assertFalse(any((root / "scratch").glob("*.parquet")))

    def test_batch_combines_multiple_source_parts_into_one_clickhouse_insert(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            parts = [
                _part(root / f"part-{position}.parquet", f"part-v{position}")
                for position in range(4)
            ]
            index = SlowLogIndex(root / "slowlog.sqlite3", run_migrations=True)
            for part in parts:
                self._seed(index, part)
            manifest = ClickHouseManifest(
                root / "manifest.sqlite3", run_migrations=True
            )
            manifest.reconcile(
                parts,
                start_epoch_us=min(
                    int(part["min_event_epoch_us"]) for part in parts
                ),
                end_epoch_us=max(
                    int(part["max_event_epoch_us"]) for part in parts
                ),
            )
            client = _IngestClient()
            result = ingest_slowlog_batch(
                _Metadata(parts),
                index,
                manifest,
                client,
                batch_parts=64,
                scratch=root / "scratch",
            )
            self.assertEqual(result["claimed_parts"], 4)
            self.assertEqual(result["loaded_parts"], 4)
            self.assertEqual(result["loaded_rows"], 4)
            self.assertEqual(client.inserts, 1)
            self.assertEqual(len(client.rows), 4)
            self.assertTrue(manifest.coverage(parts)["complete"])
            self.assertFalse(any((root / "scratch").glob("*.parquet")))

    def test_missing_source_index_releases_claim_without_empty_commit(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            part = _part(root / "part.parquet")
            index = SlowLogIndex(root / "slowlog.sqlite3", run_migrations=True)
            manifest = ClickHouseManifest(
                root / "manifest.sqlite3", run_migrations=True
            )
            manifest.reconcile(
                [part],
                start_epoch_us=int(part["min_event_epoch_us"]),
                end_epoch_us=int(part["max_event_epoch_us"]),
            )
            client = _IngestClient()
            result = ingest_slowlog_batch(
                _Metadata([part]),
                index,
                manifest,
                client,
                batch_parts=64,
                scratch=root / "scratch",
            )
            self.assertEqual(result["waiting_parts"], 1)
            self.assertEqual(client.inserts, 0)
            self.assertFalse(manifest.coverage([part])["complete"])


class ClickHouseSlowLogQueryTests(unittest.TestCase):
    def test_order_key_breaks_numeric_ties_by_fingerprint(self):
        rows = [
            {"executions": 7, "scan_rows": 11, "fingerprint": "a" * 32},
            {"executions": 7, "scan_rows": 11, "fingerprint": "f" * 32},
        ]
        ordered = sorted(
            rows,
            key=lambda row: slowlog_order_key(
                row, ("executions", "scan_rows")
            ),
            reverse=True,
        )
        self.assertEqual(ordered[0]["fingerprint"], "f" * 32)

    def test_query_reuses_short_lived_coverage_for_same_part_identities(self):
        with tempfile.TemporaryDirectory() as temp:
            part = _part(Path(temp) / "part.parquet")
            manifest = _Manifest(True)
            backend = ClickHouseSlowLogQueryBackend(
                _Metadata([part]),
                Path(temp),
                client=_Client(),
                manifest=manifest,
                statement_index=_StatementIndex(),
                table="insight.slowlog_events",
                serving_enabled=True,
            )
            query = {
                "source": "slowlog",
                "instance": "rm-prod",
                "start_epoch_us": part["min_event_epoch_us"],
                "end_epoch_us": part["max_event_epoch_us"],
            }

            self.assertIsNotNone(
                backend.summarize(query, retention_days=60, parts=[part])
            )
            self.assertIsNotNone(
                backend.summarize(query, retention_days=60, parts=[dict(part)])
            )
            self.assertEqual(manifest.coverage_calls, 1)

            changed = {**part, "logical_part_id": "part-v2"}
            self.assertIsNotNone(
                backend.summarize(query, retention_days=60, parts=[changed])
            )
            self.assertEqual(manifest.coverage_calls, 2)

    def test_query_requires_complete_independent_manifest(self):
        with tempfile.TemporaryDirectory() as temp:
            part = _part(Path(temp) / "part.parquet")
            client = _Client()
            backend = ClickHouseSlowLogQueryBackend(
                _Metadata([part]),
                Path(temp),
                client=client,
                manifest=_Manifest(False),
                statement_index=_StatementIndex(),
                table="insight.slowlog_events",
                serving_enabled=True,
            )
            result = backend.summarize(
                {
                    "source": "slowlog",
                    "instance": "rm-prod",
                    "start_epoch_us": part["min_event_epoch_us"],
                    "end_epoch_us": part["max_event_epoch_us"],
                },
                retention_days=60,
            )
            self.assertIsNone(result)
            self.assertEqual(client.queries, [])

    def test_query_builds_existing_response_shape_and_all_orders(self):
        with tempfile.TemporaryDirectory() as temp:
            part = _part(Path(temp) / "part.parquet")
            client = _Client()
            statements = _StatementIndex()
            backend = ClickHouseSlowLogQueryBackend(
                _Metadata([part]),
                Path(temp),
                client=client,
                manifest=_Manifest(True),
                statement_index=statements,
                table="insight.slowlog_events",
                serving_enabled=True,
            )
            result = backend.summarize(
                {
                    "source": "slowlog",
                    "instance": "rm-prod",
                    "database": "biz",
                    "table": "orders",
                    "operation": "select",
                    "start_epoch_us": part["min_event_epoch_us"],
                    "end_epoch_us": part["max_event_epoch_us"],
                    "limit": 50,
                    "order": "scan_rows",
                },
                retention_days=60,
            )
            self.assertIsNotNone(result)
            assert result is not None
            self.assertEqual(result["sql"]["totals"]["executions"], 3)
            self.assertEqual(result["sql"]["totals"]["actual_scan_rows"], 30)
            self.assertEqual(result["sql"]["statements"][0]["source_kind"], "slowlog")
            self.assertEqual(set(result["sql"]["orders"]), set(SQL_ORDERS))
            self.assertEqual(result["clickhouse_slowlog_coverage"]["covered_parts"], 1)
            self.assertEqual(
                result["sql"]["statements"][0]["normalized_sql"],
                "SELECT * FROM orders WHERE id=?",
            )
            self.assertEqual(len(client.queries), 1)
            rollup = client.queries[0]
            self.assertIn("GROUP BY GROUPING SETS", rollup)
            self.assertIn("GROUP BY instance_id,event_epoch_us,event_id", rollup)
            self.assertIn("argMin(tuple(", rollup)
            self.assertEqual(rollup.count("argMin("), 1)
            self.assertTrue(
                all(
                    clause.endswith("fingerprint DESC")
                    for clause in SQL_ORDERS.values()
                )
            )
            self.assertNotIn("normalized_sql", rollup)
            self.assertNotIn("sample_sql", rollup)
            self.assertIn("insight.slowlog_events", rollup)
            self.assertEqual(
                client.query_kwargs[0]["settings"],
                {
                    "max_memory_usage": 750_000_000,
                    "max_bytes_before_external_group_by": 600_000_000,
                },
            )
            self.assertEqual(statements.calls, [[("rm-prod", "f" * 32)]])


if __name__ == "__main__":
    unittest.main()
