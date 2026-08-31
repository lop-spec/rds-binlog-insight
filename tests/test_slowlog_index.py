from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import time
import unittest
from contextlib import closing, contextmanager
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from app.config import Settings
from app.metadata import MetadataStore
from app.rds_api import RemoteBinlog
from app.server import _analytics_query, _event_query
from app.slowlog_index import SlowLogIndex, SlowLogIndexError
from app.storage import EventStorage


def _part(path: Path, identity: str, rows: list[dict[str, object]]) -> dict[str, object]:
    pq.write_table(pa.Table.from_pylist(rows), path, compression="zstd")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    epochs = [int(row["event_epoch_us"]) for row in rows]
    return {
        "path": str(path),
        "logical_part_id": identity,
        "sha256": digest,
        "object_sha256": digest,
        "row_count": len(rows),
        "min_event_epoch_us": min(epochs),
        "max_event_epoch_us": max(epochs),
        "event_date": "2026-08-21",
        "size_bytes": path.stat().st_size,
        "instance_id": "rm-prod",
        "log_file_name": "slow-log/rm-prod/test",
    }


def _event(
    event_id: str,
    epoch_us: int,
    *,
    rows_examined: int,
    rows_sent: int,
    query_ms: int,
    table: str = "orders",
    node_id: str = "pi-node-a",
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_epoch_us": epoch_us,
        "instance_id": "rm-prod",
        "host_instance_id": "slow-log",
        "source_file_name": "slow-log/rm-prod/test",
        "raw_event_type": "SLOW_LOG",
        "operation": "SELECT",
        "database_name": "example_app",
        "table_name": table,
        "thread_id": 17,
        "execution_time_ms": query_ms,
        "sql_kind": "ORIGINAL",
        "sql_text": f"/* app@10.0.0.8 db=example_app */ SELECT * FROM {table} WHERE id = 42",
        "columns_json": json.dumps(
            {
                "rows_examined": rows_examined,
                "rows_sent": rows_sent,
                "lock_time_ms": 7,
                "sql_id": "das-sql-id-1",
                "node_id": node_id,
            }
        ),
        "connection_name": "10.0.0.8",
        "database_account": "app",
        "execution_status": "success",
        "affected_rows": rows_sent,
        "started_epoch_us": epoch_us,
        "finished_epoch_us": epoch_us + query_ms * 1000,
    }


def _register_slowlog_part(
    store: MetadataStore,
    part: dict[str, object],
) -> dict[str, object]:
    settings = Settings(db_instance_id="rm-prod")
    remote = RemoteBinlog(
        log_file_name="slow-log/rm-prod/test",
        log_begin_utc="2026-08-21T00:00:00Z",
        log_end_utc="2026-08-21T01:00:00Z",
        file_size=int(part.get("size_bytes") or 1),
        checksum_crc64="",
        download_link="",
        intranet_download_link="",
        link_expired_utc="",
        remote_status="Completed",
        host_instance_id="slow-log",
    )
    file_id, _ = store.upsert_remote(settings, remote)
    store.replace_parts(file_id, [part])
    store.set_file_state(file_id, "done", event_count=int(part["row_count"]))
    committed = store.part_by_path(str(part["path"]))
    assert committed is not None
    return committed


class SlowLogIndexTests(unittest.TestCase):
    def test_summary_verifier_can_keep_temp_tables_in_memory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            source = root / "slow.parquet"
            part = _part(
                source,
                "logical-v1",
                [_event("memory-temp", epoch_us, rows_examined=3, rows_sent=1, query_ms=2)],
            )
            index = SlowLogIndex(root / "slowlog.sqlite3")
            index.build_part(part, source)
            observed: list[int] = []
            original_connection = index.connection

            @contextmanager
            def checked_connection(*, control=None, temp_store_memory=False):
                with original_connection(
                    control=control,
                    temp_store_memory=temp_store_memory,
                ) as connection:
                    observed.append(
                        int(connection.execute("PRAGMA temp_store").fetchone()[0])
                    )
                    yield connection

            with patch.object(index, "connection", checked_connection):
                summary = index.summarize(
                    start_epoch_us=epoch_us - 1,
                    end_epoch_us=epoch_us + 1,
                    instance="rm-prod",
                    temp_store_memory=True,
                )
            self.assertEqual(summary["sql"]["totals"]["executions"], 1)
            self.assertEqual(observed, [2])

    def test_summary_honors_query_cancellation(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            source = root / "slow.parquet"
            part = _part(
                source,
                "logical-v1",
                [_event("cancel-me", epoch_us, rows_examined=1, rows_sent=1, query_ms=1)],
            )
            index = SlowLogIndex(root / "slowlog.sqlite3")
            index.build_part(part, source)

            class _Control:
                def __init__(self):
                    self.calls = 0

                def check_cancelled(self):
                    self.calls += 1
                    if self.calls >= 2:
                        raise RuntimeError("query cancelled")

            control = _Control()
            with self.assertRaisesRegex(RuntimeError, "query cancelled"):
                index.summarize(
                    start_epoch_us=epoch_us - 1,
                    end_epoch_us=epoch_us + 1,
                    instance="rm-prod",
                    control=control,
                )
            self.assertGreaterEqual(control.calls, 2)

    def test_v7_index_migrates_node_column_and_indexes_in_place(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "slowlog.sqlite3"
            index = SlowLogIndex(path)
            with index.connection() as conn:
                for name in (
                    "idx_slowlog_event_instance_node_time",
                    "idx_slowlog_event_node_time",
                    "idx_slowlog_event_analytics",
                ):
                    conn.execute(f"DROP INDEX IF EXISTS {name}")
                conn.execute("ALTER" " TABLE slowlog_events DROP COLUMN node_id")
                conn.execute(
                    """
                    CREATE INDEX idx_slowlog_event_analytics
                    ON slowlog_events(
                        is_canonical, instance_id, event_epoch_us, event_id,
                        operation, database_name, table_name, fingerprint, sql_id,
                        query_time_ms, lock_time_ms, rows_examined, rows_sent, sql_bytes
                    )
                    """
                )
                conn.execute("PRAGMA user_version = 7")

            migrated = SlowLogIndex(path)
            with migrated.connection() as conn:
                version = int(conn.execute("PRAGMA user_version").fetchone()[0])
                columns = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA table_info(slowlog_events)")
                }
                indexes = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA index_list(slowlog_events)")
                }
                index_sql = {
                    str(row["name"]): str(row["sql"] or "")
                    for row in conn.execute(
                        "SELECT name, sql FROM sqlite_master "
                        "WHERE type = 'index' AND tbl_name = 'slowlog_events'"
                    )
                }
            self.assertEqual(version, 8)
            self.assertIn("node_id", columns)
            self.assertIn("idx_slowlog_event_instance_node_time", indexes)
            self.assertIn("idx_slowlog_event_node_time", indexes)
            self.assertIn("idx_slowlog_event_analytics", indexes)
            self.assertNotIn("node_id", index_sql["idx_slowlog_event_analytics"])
            self.assertIn(
                "node_id <> ''",
                index_sql["idx_slowlog_event_instance_node_time"],
            )

    def test_runtime_rejects_incomplete_v8_and_explicit_migration_repairs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "slowlog.sqlite3"
            index = SlowLogIndex(path)
            with index.connection() as conn:
                conn.execute("DROP INDEX idx_slowlog_event_instance_node_time")
                conn.execute("DROP INDEX idx_slowlog_event_node_time")
                self.assertEqual(
                    int(conn.execute("PRAGMA user_version").fetchone()[0]),
                    8,
                )

            with self.assertRaisesRegex(
                SlowLogIndexError,
                "schema is incomplete",
            ) as raised:
                SlowLogIndex(path, run_migrations=False)
            self.assertEqual(
                raised.exception.code,
                "SLOWLOG_INDEX_SCHEMA_INCOMPLETE",
            )

            SlowLogIndex(path, run_migrations=True)
            SlowLogIndex(path, run_migrations=False)

    def test_runtime_keeps_wal_anchor_until_close(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "slowlog.sqlite3"
            SlowLogIndex(path, run_migrations=True)
            runtime = SlowLogIndex(path, run_migrations=False)
            wal_path = Path(str(path) + "-wal")

            with runtime.connection() as conn:
                conn.execute(
                    "UPDATE slowlog_reconcile_state "
                    "SET updated_at_us = updated_at_us + 1 WHERE singleton = 1"
                )

            self.assertTrue(wal_path.is_file())
            self.assertGreater(wal_path.stat().st_size, 0)
            runtime.close()
            self.assertFalse(wal_path.exists())

    def test_node_id_filters_analytics_and_is_returned_by_detail(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "slow-nodes.parquet"
            rows = [
                _event(
                    "slow-node-a",
                    1_787_286_000_000_000,
                    rows_examined=100,
                    rows_sent=1,
                    query_ms=900,
                    node_id="pi-node-a",
                ),
                _event(
                    "slow-node-b",
                    1_787_286_010_000_000,
                    rows_examined=900,
                    rows_sent=2,
                    query_ms=2_000,
                    node_id="pi-node-b",
                ),
            ]
            part = _part(source, "logical-nodes", rows)
            index = SlowLogIndex(root / "slowlog.sqlite3")
            index.build_part(part, source)

            summary = index.summarize(
                start_epoch_us=1_787_285_900_000_000,
                end_epoch_us=1_787_286_100_000_000,
                instance="rm-prod",
                node_id="pi-node-a",
                limit=20,
            )
            self.assertEqual(summary["sql"]["totals"]["executions"], 1)
            self.assertEqual(
                summary["sql"]["statements"][0]["sample_event_id"],
                "slow-node-a",
            )
            detail = index.event_detail("slow-node-a", "rm-prod")
            self.assertIsNotNone(detail)
            self.assertEqual(detail["node_id"], "pi-node-a")

    def test_build_is_idempotent_and_analytics_use_actual_rows_examined(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "slow.parquet"
            rows = [
                _event(
                    "slow-1",
                    1_787_286_000_000_000,
                    rows_examined=1_410_900,
                    rows_sent=1,
                    query_ms=9_100,
                ),
                _event(
                    "slow-2",
                    1_787_286_060_000_000,
                    rows_examined=100,
                    rows_sent=2,
                    query_ms=500,
                ),
            ]
            part = _part(source, "logical-v1", rows)
            index = SlowLogIndex(root / "slowlog.sqlite3")

            index.enqueue_parts([part])
            self.assertFalse(index.coverage([part])["complete"])
            built = index.build_part(part, source)
            self.assertEqual(built["indexed_rows"], 2)
            self.assertTrue(index.coverage([part])["complete"])
            self.assertEqual(index.stats()["pending_parts"], 0)
            self.assertEqual(index.build_part(part, source)["skipped"], "current")

            page = index.query_events(
                {
                    "instance": "rm-prod",
                    "database": "example_app",
                    "table": "orders",
                    "operations": ["SELECT"],
                    "limit": 100,
                    "offset": 0,
                },
                start_epoch_us=1_787_285_900_000_000,
                end_epoch_us=1_787_286_100_000_000,
            )
            self.assertEqual([row["event_id"] for row in page["rows"]], ["slow-2", "slow-1"])
            self.assertEqual(page["tiers_used"], ["slowlog-index"])
            keyword_page = index.query_events(
                {"instance": "rm-prod", "keyword": "orders", "limit": 100},
                start_epoch_us=1_787_285_900_000_000,
                end_epoch_us=1_787_286_100_000_000,
            )
            self.assertEqual(len(keyword_page["rows"]), 2)
            fingerprint_page = index.query_events(
                {
                    "instance": "rm-prod",
                    "fingerprint": page["rows"][0]["fingerprint"],
                    "limit": 100,
                },
                start_epoch_us=1_787_285_900_000_000,
                end_epoch_us=1_787_286_100_000_000,
            )
            self.assertEqual(len(fingerprint_page["rows"]), 2)

            summary = index.summarize(
                start_epoch_us=1_787_285_900_000_000,
                end_epoch_us=1_787_286_100_000_000,
                instance="rm-prod",
                database="example_app",
                table="orders",
                operation="SELECT",
                limit=20,
                order="scan_rows",
            )
            totals = summary["sql"]["totals"]
            self.assertEqual(totals["executions"], 2)
            self.assertEqual(totals["scan_rows"], 1_411_000)
            self.assertEqual(totals["rows_sent"], 3)
            self.assertEqual(totals["query_time_ms_total"], 9_600)
            self.assertEqual(summary["sql"]["scan_source"], "actual")
            statement = summary["sql"]["statements"][0]
            self.assertEqual(statement["scan_rows"], 1_411_000)
            self.assertEqual(statement["sql_id"], "das-sql-id-1")

            detail = index.event_detail("slow-1")
            self.assertIsNotNone(detail)
            self.assertIn("SELECT * FROM orders", detail["sql_text"])
            with index.connection() as conn:
                hot_columns = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA table_info(slowlog_events)")
                }
                detail_columns = {
                    str(row["name"])
                    for row in conn.execute("PRAGMA table_info(slowlog_event_details)")
                }
                analytics_plan = conn.execute(
                    """
                    EXPLAIN QUERY PLAN
                    SELECT e.event_id, e.event_epoch_us, e.instance_id,
                           e.operation, e.database_name, e.table_name,
                           e.fingerprint, e.sql_id, e.query_time_ms,
                           e.lock_time_ms, e.rows_examined, e.rows_sent,
                           e.sql_bytes
                    FROM slowlog_events e INDEXED BY idx_slowlog_event_analytics
                    WHERE e.is_canonical = 1
                      AND e.event_epoch_us >= ? AND e.event_epoch_us <= ?
                      AND e.instance_id = ?
                    """,
                    (
                        1_787_285_900_000_000,
                        1_787_286_100_000_000,
                        "rm-prod",
                    ),
                ).fetchall()
                recent_plan = conn.execute(
                    """
                    EXPLAIN QUERY PLAN
                    SELECT COUNT(*) FROM slowlog_events
                    INDEXED BY idx_slowlog_event_recent_count
                    WHERE is_canonical = 1 AND event_epoch_us >= ?
                    """,
                    (1_787_285_900_000_000,),
                ).fetchall()
            self.assertNotIn("sql_text", hot_columns)
            self.assertNotIn("columns_json", hot_columns)
            self.assertIn("sql_bytes", hot_columns)
            self.assertEqual(detail_columns, {"event_id", "part_path", "sql_text_z"})
            with index.connection() as conn:
                stored_body = conn.execute(
                    "SELECT typeof(sql_text_z), slowlog_sql_text(sql_text_z) "
                    "FROM slowlog_event_details WHERE event_id = ?",
                    ("slow-1",),
                ).fetchone()
            self.assertEqual(stored_body[0], "blob")
            self.assertEqual(stored_body[1], detail["sql_text"])
            self.assertIn(
                "idx_slowlog_event_analytics",
                " ".join(str(row[3]) for row in analytics_plan),
            )
            self.assertIn(
                "COVERING",
                " ".join(str(row[3]) for row in analytics_plan).upper(),
            )
            self.assertIn(
                "idx_slowlog_event_recent_count",
                " ".join(str(row[3]) for row in recent_plan),
            )

            where, params = index._event_where(
                {
                    "instance": "rm-prod",
                    "database": "example_app",
                    "table": "orders",
                },
                1_787_285_900_000_000,
                1_787_286_100_000_000,
            )
            index_hint = index._event_index_hint(
                {
                    "instance": "rm-prod",
                    "database": "example_app",
                    "table": "orders",
                }
            )
            with index.connection() as conn:
                plan = conn.execute(
                    f"EXPLAIN QUERY PLAN SELECT event_id FROM slowlog_events "
                    f"{index_hint} WHERE is_canonical = 1 AND {where}",
                    params,
                ).fetchall()
            self.assertIn(
                "idx_slowlog_event_object_nocase_time",
                " ".join(str(row[3]) for row in plan),
            )

    def test_analytics_exposes_exact_max_samples_and_existing_event_lookup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "slow-attribution.parquet"
            scan_event = _event(
                "max-scan",
                1_787_286_000_000_000,
                rows_examined=18_974_956,
                rows_sent=0,
                query_ms=49_476,
            )
            scan_event.update(
                {
                    "sql_text": (
                        "/* api_writer@10.0.0.62 db=example_app */ "
                        "SELECT * FROM orders WHERE id = 971"
                    ),
                    "database_account": "api_writer",
                    "connection_name": "10.0.0.62",
                    "thread_id": 971,
                }
            )
            query_event = _event(
                "max-query",
                1_787_286_060_000_000,
                rows_examined=3_643_736,
                rows_sent=3_643_736,
                query_ms=1_530_038,
            )
            query_event.update(
                {
                    "sql_text": (
                        "/* batch_export@10.0.0.16 db=example_app */ "
                        "SELECT * FROM orders WHERE id = 972"
                    ),
                    "database_account": "batch_export",
                    "connection_name": "10.0.0.16",
                    "thread_id": 972,
                }
            )
            part = _part(source, "logical-attribution", [scan_event, query_event])
            index = SlowLogIndex(root / "slowlog.sqlite3")
            index.build_part(part, source)

            summary = index.summarize(
                start_epoch_us=1_787_285_900_000_000,
                end_epoch_us=1_787_286_100_000_000,
                instance="rm-prod",
                limit=20,
                order="scan_rows",
            )
            statement = summary["sql"]["statements"][0]
            self.assertEqual(statement["max_scan_event_id"], "max-scan")
            self.assertEqual(statement["max_query_event_id"], "max-query")
            samples = summary["sql"]["sample_events"]
            self.assertEqual(samples["max-scan"]["database_account"], "api_writer")
            self.assertEqual(samples["max-scan"]["client_ip"], "10.0.0.62")
            self.assertEqual(samples["max-scan"]["thread_id"], 971)
            self.assertIn("id = 971", samples["max-scan"]["sql_text"])
            self.assertEqual(samples["max-query"]["database_account"], "batch_export")
            self.assertEqual(samples["max-query"]["client_ip"], "10.0.0.16")
            self.assertEqual(samples["max-query"]["query_time_ms"], 1_530_038)
            self.assertEqual(
                index.existing_event_ids(
                    {"max-scan", "max-query", "missing"},
                    instance="rm-prod",
                ),
                {"max-scan", "max-query"},
            )

    def test_changed_part_replaces_old_events_without_stale_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "slow.parquet"
            first = _part(
                source,
                "logical-v1",
                [_event("old", 1_787_286_000_000_000, rows_examined=10, rows_sent=1, query_ms=100)],
            )
            index = SlowLogIndex(root / "slowlog.sqlite3")
            index.enqueue_parts([first])
            index.build_part(first, source)

            second = _part(
                source,
                "logical-v2",
                [_event("new", 1_787_286_010_000_000, rows_examined=20, rows_sent=1, query_ms=200)],
            )
            index.enqueue_parts([second])
            index.build_part(second, source)

            page = index.query_events(
                {"limit": 100, "offset": 0},
                start_epoch_us=1_787_285_000_000_000,
                end_epoch_us=1_787_287_000_000_000,
            )
            self.assertEqual([row["event_id"] for row in page["rows"]], ["new"])
            self.assertFalse(index.coverage([first])["complete"])
            self.assertTrue(index.coverage([second])["complete"])

    def test_ready_queue_retries_due_failure_before_newer_work(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            index = SlowLogIndex(Path(directory) / "slowlog.sqlite3")
            failed = {
                "path": "/data/older.parquet",
                "logical_part_id": "older-v1",
                "sha256": "older-sha",
                "content_revision": 1,
                "min_event_epoch_us": 10,
                "max_event_epoch_us": 20,
                "row_count": 1,
            }
            newer = {
                "path": "/data/newer.parquet",
                "logical_part_id": "newer-v1",
                "sha256": "newer-sha",
                "content_revision": 1,
                "min_event_epoch_us": 30,
                "max_event_epoch_us": 40,
                "row_count": 1,
            }
            index.enqueue_parts([failed, newer])
            index.record_failure(failed, "database is locked")
            with index.connection() as conn:
                conn.execute(
                    "UPDATE slowlog_queue SET next_retry_us = 0 "
                    "WHERE part_path = ?",
                    (failed["path"],),
                )

            self.assertEqual(index.ready_paths(limit=1), [failed["path"]])

    def test_ready_queue_does_not_starve_old_work_behind_new_arrivals(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            index = SlowLogIndex(Path(directory) / "slowlog.sqlite3")
            older = {
                "path": "/data/older.parquet",
                "logical_part_id": "older-v1",
                "sha256": "older-sha",
                "content_revision": 1,
                "min_event_epoch_us": 10,
                "max_event_epoch_us": 20,
                "row_count": 1,
            }
            newer = {
                "path": "/data/newer.parquet",
                "logical_part_id": "newer-v1",
                "sha256": "newer-sha",
                "content_revision": 1,
                "min_event_epoch_us": 30,
                "max_event_epoch_us": 40,
                "row_count": 1,
            }
            index.enqueue_parts([older, newer])
            with index.connection() as conn:
                conn.execute(
                    "UPDATE slowlog_queue SET enqueued_at_us = CASE part_path "
                    "WHEN ? THEN 100 WHEN ? THEN 200 END",
                    (older["path"], newer["path"]),
                )

            self.assertEqual(index.ready_paths(limit=1), [older["path"]])

    def test_overlapping_parts_are_exactly_deduplicated_and_survive_one_removal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            epoch_us = 1_787_286_000_000_000
            row = _event(
                "overlap-1",
                epoch_us,
                rows_examined=1_410_900,
                rows_sent=1,
                query_ms=9_100,
            )
            source_a = root / "slow-a.parquet"
            source_b = root / "slow-b.parquet"
            part_a = _part(source_a, "logical-a", [row])
            part_b = _part(source_b, "logical-b", [row])
            index = SlowLogIndex(root / "slowlog.sqlite3")

            index.build_part(part_b, source_b)
            index.build_part(part_a, source_a)

            stats = index.stats()
            self.assertEqual(stats["indexed_rows"], 2)
            self.assertEqual(stats["unique_events"], 1)
            self.assertEqual(stats["duplicate_occurrences"], 1)
            with index.connection() as conn:
                occurrence_counts = conn.execute(
                    "SELECT COUNT(*) AS occurrences, SUM(is_canonical) AS canonical "
                    "FROM slowlog_events WHERE event_id = 'overlap-1'"
                ).fetchone()
                canonical_path = conn.execute(
                    "SELECT part_path FROM slowlog_events "
                    "WHERE event_id = 'overlap-1' AND is_canonical = 1"
                ).fetchone()[0]
            self.assertEqual(occurrence_counts["occurrences"], 2)
            self.assertEqual(occurrence_counts["canonical"], 1)
            self.assertEqual(canonical_path, min(str(source_a), str(source_b)))
            page = index.query_events(
                {"limit": 100, "offset": 0},
                start_epoch_us=epoch_us,
                end_epoch_us=epoch_us,
            )
            self.assertEqual([item["event_id"] for item in page["rows"]], ["overlap-1"])
            summary = index.summarize(
                start_epoch_us=epoch_us,
                end_epoch_us=epoch_us,
                instance="rm-prod",
                order="scan_rows",
            )
            self.assertEqual(summary["sql"]["totals"]["executions"], 1)
            self.assertEqual(summary["sql"]["totals"]["scan_rows"], 1_410_900)

            index.remove_path(str(source_a))
            self.assertIsNotNone(index.event_detail("overlap-1"))
            self.assertEqual(index.stats()["unique_events"], 1)
            with index.connection() as conn:
                canonical = conn.execute(
                    "SELECT COUNT(*) FROM slowlog_events "
                    "WHERE event_id = 'overlap-1' AND is_canonical = 1"
                ).fetchone()[0]
            self.assertEqual(canonical, 1)

    def test_equal_event_ids_in_different_instances_are_not_deduplicated(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            epoch_us = 1_787_286_000_000_000
            first_row = _event(
                "shared-id",
                epoch_us,
                rows_examined=10,
                rows_sent=1,
                query_ms=100,
            )
            second_row = dict(first_row)
            second_row["instance_id"] = "rm-other"
            source_a = root / "prod.parquet"
            source_b = root / "other.parquet"
            part_a = _part(source_a, "logical-prod", [first_row])
            part_b = _part(source_b, "logical-other", [second_row])
            part_b["instance_id"] = "rm-other"
            index = SlowLogIndex(root / "slowlog.sqlite3")

            index.build_part(part_b, source_b)
            index.build_part(part_a, source_a)

            stats = index.stats()
            self.assertEqual(stats["indexed_rows"], 2)
            self.assertEqual(stats["unique_events"], 2)
            self.assertEqual(stats["duplicate_occurrences"], 0)
            prod = index.query_events(
                {"instance": "rm-prod", "limit": 100},
                start_epoch_us=epoch_us,
                end_epoch_us=epoch_us,
            )
            other = index.query_events(
                {"instance": "rm-other", "limit": 100},
                start_epoch_us=epoch_us,
                end_epoch_us=epoch_us,
            )
            self.assertEqual(len(prod["rows"]), 1)
            self.assertEqual(len(other["rows"]), 1)
            self.assertEqual(
                index.event_detail("shared-id", "rm-other")["instance_id"],
                "rm-other",
            )
            with index.connection() as conn:
                invalid = conn.execute(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT instance_id, event_id FROM slowlog_events
                        GROUP BY instance_id, event_id
                        HAVING SUM(is_canonical) <> 1
                    )
                    """
                ).fetchone()[0]
            self.assertEqual(invalid, 0)


class SlowLogRoutingTests(unittest.TestCase):
    def test_existing_v1_database_installs_storage_rollup_without_backfill(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "existing-v1.parquet"
            store = MetadataStore(root / "metadata.sqlite3")
            _register_slowlog_part(
                store,
                _part(
                    source,
                    "existing-v1",
                    [
                        _event(
                            "existing-v1-event",
                            1_787_286_000_000_000,
                            rows_examined=5,
                            rows_sent=1,
                            query_ms=20,
                        )
                    ],
                ),
            )
            with store.connection() as conn:
                for trigger in (
                    "trg_storage_file_stats_part_insert",
                    "trg_storage_file_stats_part_update",
                    "trg_storage_file_stats_part_delete",
                ):
                    conn.execute(f"DROP TRIGGER {trigger}")
                conn.execute("DROP TABLE parquet_file_stats_state")
                conn.execute("DROP TABLE parquet_file_stats")

            reopened = MetadataStore(root / "metadata.sqlite3")
            with reopened.connection() as conn:
                state = conn.execute(
                    "SELECT complete FROM parquet_file_stats_state "
                    "WHERE singleton = 1"
                ).fetchone()
                trigger_count = int(
                    conn.execute(
                        "SELECT COUNT(*) FROM sqlite_master "
                        "WHERE type = 'trigger' AND name LIKE "
                        "'trg_storage_file_stats_%'"
                    ).fetchone()[0]
                )
            self.assertEqual(int(state["complete"]), 0)
            self.assertEqual(trigger_count, 3)
            self.assertEqual(reopened.storage_metadata_stats()["part_count"], 1)
            reopened.rebuild_storage_file_stats()
            self.assertEqual(reopened.storage_metadata_stats()["part_count"], 1)

    def test_storage_stats_rollup_tracks_part_lifecycle_and_visibility(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "storage-rollup.parquet"
            store = MetadataStore(root / "metadata.sqlite3")
            committed = _register_slowlog_part(
                store,
                _part(
                    source,
                    "storage-rollup-v1",
                    [
                        _event(
                            "stats-1",
                            1_787_286_000_000_000,
                            rows_examined=5,
                            rows_sent=1,
                            query_ms=20,
                        )
                    ],
                ),
            )
            stats = store.storage_metadata_stats()
            self.assertEqual(stats["part_count"], 1)
            self.assertEqual(stats["event_count"], 1)
            self.assertEqual(stats["archived_part_count"], 0)

            store.mark_part_archived(
                str(committed["path"]), oss_key="archive/key", oss_etag="etag"
            )
            archived = store.storage_metadata_stats()
            self.assertEqual(archived["archived_part_count"], 1)
            self.assertEqual(archived["archived_bytes"], archived["parquet_bytes"])

            store.set_file_visibility(str(committed["binlog_id"]), False)
            self.assertEqual(store.storage_metadata_stats()["part_count"], 0)
            store.set_file_visibility(str(committed["binlog_id"]), True)

            rebuilt = store.rebuild_storage_file_stats()
            self.assertTrue(rebuilt["complete"])
            self.assertEqual(rebuilt["files"], 1)
            self.assertEqual(store.storage_metadata_stats()["part_count"], 1)

            store.delete_part(str(committed["path"]))
            deleted = store.storage_metadata_stats()
            self.assertEqual(deleted["part_count"], 0)
            self.assertEqual(deleted["event_count"], 0)

    def test_event_query_keeps_fingerprint(self) -> None:
        self.assertEqual(
            _event_query({"fingerprint": ["fp-123"]})["fingerprint"],
            "fp-123",
        )

    def test_analytics_query_defaults_to_binlog(self) -> None:
        self.assertEqual(_analytics_query({})["source"], "binlog")

    def test_analytics_query_keeps_source(self) -> None:
        query = _analytics_query(
            {"source": ["slowlog"], "nodeId": ["pi-node-a"]}
        )
        self.assertEqual(query["source"], "slowlog")
        self.assertEqual(query["node_id"], "pi-node-a")

    def test_analytics_query_rejects_unsafe_node_id(self) -> None:
        with self.assertRaisesRegex(ValueError, "Node ID"):
            _analytics_query({"source": ["slowlog"], "nodeId": ["../../bad"]})

    def test_metadata_prunes_slowlog_and_binlog_at_part_level(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = MetadataStore(Path(directory) / "metadata.sqlite3")
            settings = Settings(db_instance_id="rm-prod")
            for name, host, path_name in (
                ("slow-log/rm-prod/1", "slow-log", "slow.parquet"),
                ("mysql-bin.000001", "mysql", "binlog.parquet"),
            ):
                remote = RemoteBinlog(
                    log_file_name=name,
                    log_begin_utc="2026-08-21T00:00:00Z",
                    log_end_utc="2026-08-21T01:00:00Z",
                    file_size=1,
                    checksum_crc64="",
                    download_link="",
                    intranet_download_link="",
                    link_expired_utc="",
                    remote_status="Completed",
                    host_instance_id=host,
                )
                file_id, _ = store.upsert_remote(settings, remote)
                store.replace_parts(
                    file_id,
                    [
                        {
                            "path": str(Path(directory) / path_name),
                            "event_date": "2026-08-21",
                            "row_count": 1,
                            "min_event_epoch_us": 1_787_284_800_000_000,
                            "max_event_epoch_us": 1_787_284_900_000_000,
                            "size_bytes": 1,
                            "sha256": hashlib.sha256(path_name.encode()).hexdigest(),
                        }
                    ],
                )
                store.set_file_state(file_id, "done", event_count=1)

            slow = store.parts_in_range(
                start_epoch_us=1_787_284_000_000_000,
                end_epoch_us=1_787_285_000_000_000,
                source="slowlog",
            )
            binlog = store.parts_in_range(
                start_epoch_us=1_787_284_000_000_000,
                end_epoch_us=1_787_285_000_000_000,
                source="binlog",
            )
            self.assertEqual([Path(row["path"]).name for row in slow], ["slow.parquet"])
            self.assertEqual([Path(row["path"]).name for row in binlog], ["binlog.parquet"])

    def test_additive_metadata_indexes_are_installed_only_by_migration_owner(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metadata.sqlite3"
            owner = MetadataStore(path)
            with owner.connection() as conn:
                conn.execute("DROP INDEX idx_binlog_slowlog_source")
                conn.execute("DROP INDEX idx_binlog_visibility_id")
                conn.execute("DROP INDEX idx_part_binlog_path")
                conn.execute("DROP INDEX IF EXISTS idx_part_event_date_path")
            with owner.catalog_store.connection() as conn:
                conn.execute("DROP TABLE backfill_state")

            with self.assertRaisesRegex(RuntimeError, "schema is incomplete"):
                MetadataStore(path, run_migrations=False)
            with closing(sqlite3.connect(path)) as conn:
                worker_indexes = {
                    str(row[0])
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'index'"
                    ).fetchall()
                }
            self.assertNotIn("idx_binlog_slowlog_source", worker_indexes)
            self.assertNotIn("idx_binlog_visibility_id", worker_indexes)
            self.assertNotIn("idx_part_binlog_path", worker_indexes)
            self.assertNotIn("idx_part_event_date_path", worker_indexes)

            MetadataStore(path)
            with closing(sqlite3.connect(path)) as conn:
                owner_indexes = {
                    str(row[0])
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'index'"
                    ).fetchall()
                }
            self.assertIn("idx_binlog_slowlog_source", owner_indexes)
            self.assertIn("idx_binlog_visibility_id", owner_indexes)
            self.assertIn("idx_part_binlog_path", owner_indexes)
            self.assertIn("idx_part_event_date_path", owner_indexes)
            MetadataStore(path, run_migrations=False)
            with closing(sqlite3.connect(path)) as conn:
                plan = conn.execute(
                    """
                    EXPLAIN QUERY PLAN
                    SELECT p.*, b.log_file_name
                    FROM parquet_parts p
                        INDEXED BY idx_part_event_date_path
                    CROSS JOIN binlog_files b
                    WHERE b.id = p.binlog_id
                      AND b.query_visible = 1
                    ORDER BY p.event_date ASC, p.path ASC
                    LIMIT 2048 OFFSET 0
                    """
                ).fetchall()
            plan_text = " ".join(str(row[3]) for row in plan)
            self.assertIn("idx_part_event_date_path", plan_text)
            self.assertNotIn("TEMP B-TREE", plan_text.upper())

    def test_ui_exposes_slowlog_actual_metrics(self) -> None:
        root = Path(__file__).resolve().parents[1]
        html = (root / "web" / "index.html").read_text("utf-8")
        script = (root / "web" / "app.js").read_text("utf-8")
        self.assertIn('id="analytics-source"', html)
        self.assertIn('id="analytics-node"', html)
        self.assertIn('<option value="slowlog">RDS 慢日志</option>', html)
        self.assertIn("实际扫描行数", script)
        self.assertIn("RowsExamined", script)
        self.assertIn('params.set("source", source)', script)
        self.assertIn('params.set("nodeId", nodeId)', script)
        self.assertIn("data-slow-event-id", script)
        self.assertIn("&instance=${encodeURIComponent(instance)}", script)

    def test_storage_routes_complete_slowlog_scope_to_dedicated_index(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "slow.parquet"
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            part = _part(
                source,
                "logical-v1",
                [_event("slow-route", epoch_us, rows_examined=321, rows_sent=2, query_ms=800)],
            )
            metadata = MetadataStore(root / "metadata.sqlite3")
            committed = _register_slowlog_part(metadata, part)
            storage = EventStorage(metadata, root)
            storage.slowlog_index.build_part(committed, source)
            query = {
                "source": "slowlog",
                "instance": "rm-prod",
                "start_epoch_us": epoch_us - 60_000_000,
                "end_epoch_us": epoch_us,
                "limit": 50,
                "offset": 0,
                "order": "scan_rows",
            }
            settings = Settings(db_instance_id="rm-prod", retention_days=90)

            events = storage.query_events_tiered(query, settings, None)
            self.assertEqual(events["tiers_used"], ["slowlog-index"])
            self.assertTrue(events["slowlog_index_coverage"]["complete"])
            self.assertEqual(events["rows"][0]["rows_examined"], 321)

            summary = storage.analytics_summary(query, settings, None, scan_limit=0)
            self.assertEqual(summary["sql"]["mode"], "slowlog")
            self.assertEqual(summary["sql"]["totals"]["actual_scan_rows"], 321)
            self.assertEqual(summary["evidence"]["metrics"], "actual")

            detail = storage.event_detail_tiered("slow-route", settings, None)
            self.assertIsNotNone(detail)
            self.assertEqual(detail["rows_examined"], 321)

            cleanup = storage.cleanup(0)
            self.assertEqual(cleanup["deleted_parts"], 1)
            self.assertIsNone(storage.slowlog_index.event_detail("slow-route"))

    def test_storage_prefers_complete_clickhouse_slowlog_analytics(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "slow.parquet"
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            part = _part(
                source,
                "logical-v1",
                [_event("slow-clickhouse", epoch_us, rows_examined=321, rows_sent=2, query_ms=800)],
            )
            metadata = MetadataStore(root / "metadata.sqlite3")
            committed = _register_slowlog_part(metadata, part)
            storage = EventStorage(metadata, root)
            storage.slowlog_index.build_part(committed, source)
            query = {
                "source": "slowlog",
                "instance": "rm-prod",
                "start_epoch_us": epoch_us - 60_000_000,
                "end_epoch_us": epoch_us,
                "limit": 50,
                "order": "scan_rows",
            }
            settings = Settings(db_instance_id="rm-prod", retention_days=90)
            expected = storage.slowlog_index.summarize(
                start_epoch_us=query["start_epoch_us"],
                end_epoch_us=query["end_epoch_us"],
                instance="rm-prod",
                limit=50,
                order="scan_rows",
            )
            expected["clickhouse_slowlog_coverage"] = {
                "complete": True,
                "total_parts": 1,
                "covered_parts": 1,
                "missing_parts": [],
            }

            class _Backend:
                def __init__(self):
                    self.calls = 0

                def summarize(self, *_args, **_kwargs):
                    self.calls += 1
                    return expected

                def stats(self):
                    return {"ready_parts": 1}

            backend = _Backend()
            storage.clickhouse_slowlog_backend = backend
            summary = storage.analytics_summary(query, settings, None, scan_limit=0)
            self.assertEqual(backend.calls, 1)
            self.assertEqual(summary["sql"]["totals"]["actual_scan_rows"], 321)
            self.assertEqual(summary["evidence"]["engine"], "clickhouse")
            self.assertEqual(summary["coverage"]["covered_parts"], 1)

    def test_incomplete_slowlog_event_scope_keeps_legacy_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "slow.parquet"
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            part = _part(
                source,
                "logical-v1",
                [_event("slow-fallback", epoch_us, rows_examined=10, rows_sent=1, query_ms=100)],
            )
            metadata = MetadataStore(root / "metadata.sqlite3")
            _register_slowlog_part(metadata, part)
            storage = EventStorage(metadata, root)
            query = {
                "source": "slowlog",
                "instance": "rm-prod",
                "start_epoch_us": epoch_us - 60_000_000,
                "end_epoch_us": epoch_us,
                "limit": 50,
                "offset": 0,
            }
            settings = Settings(db_instance_id="rm-prod", retention_days=90)
            legacy = {"rows": [], "has_more": False, "tiers_used": ["legacy-scan"]}
            with patch.object(
                storage,
                "_query_events_tiered_singleflight",
                return_value=legacy,
            ):
                result = storage.query_events_tiered(query, settings, None)
            self.assertTrue(result["slowlog_index_fallback"])
            self.assertFalse(result["slowlog_index_coverage"]["complete"])
            self.assertEqual(
                result["slowlog_index_coverage"]["repair_queued_parts"],
                1,
            )
            self.assertEqual(storage.slowlog_index.stats()["pending_parts"], 1)


if __name__ == "__main__":
    unittest.main()
