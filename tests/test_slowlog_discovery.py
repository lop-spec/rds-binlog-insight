from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.clickhouse_manifest import ClickHouseManifest
from app.clickhouse_slowlog import (
    ClickHouseSlowLogWorker,
    discover_recent_slowlog_parts,
    reconcile_slowlog_manifest,
)
from app.config import Settings
from app.metadata import MetadataStore
from app.rds_api import RemoteBinlog

HOUR_US = 3_600_000_000


def _register(store: MetadataStore, name: str, epoch_us: int, *, host: str = "slow-log",
              visible: bool = True) -> str:
    remote = RemoteBinlog(
        log_file_name=name,
        log_begin_utc="2026-10-20T00:00:00Z",
        log_end_utc="2026-10-20T01:00:00Z",
        file_size=1,
        checksum_crc64="",
        download_link="",
        intranet_download_link="",
        link_expired_utc="",
        remote_status="Completed",
        host_instance_id=host,
    )
    file_id, _ = store.upsert_remote(Settings(db_instance_id="rm-example-a"), remote)
    path = f"/data/events/{file_id}-000000.parquet"
    store.replace_parts(file_id, [{
        "path": path,
        "logical_part_id": f"id-{file_id}",
        "sha256": f"sha-{file_id}",
        "object_sha256": f"sha-{file_id}",
        "row_count": 1,
        "min_event_epoch_us": epoch_us,
        "max_event_epoch_us": epoch_us + 60_000_000,
        "event_date": "2026-10-20",
        "size_bytes": 10,
    }])
    if visible:
        store.set_file_state(file_id, "done", event_count=1)
    else:  # the collector hides a file while it rewrites its parts
        store.set_file_state(file_id, "parsing", query_visible=False)
    return path


def _status(manifest: ClickHouseManifest, path: str) -> str | None:
    with manifest.connection() as connection:
        row = connection.execute(
            "SELECT status FROM clickhouse_parts WHERE part_path = ?", (path,)).fetchone()
    return str(row["status"]) if row else None


class SlowLogDiscoveryTests(unittest.TestCase):
    def test_recent_pass_queues_new_parts_and_never_sweeps(self):
        now = time.time_ns() // 1000
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = MetadataStore(root / "metadata.sqlite3")
            manifest = ClickHouseManifest(root / "manifest.sqlite3", run_migrations=True)
            old = _register(store, "slow-log/rm-example-a/old", now - 30 * HOUR_US)
            reconcile_slowlog_manifest(store, manifest, retention_days=3, now_us=now)
            self.assertEqual(_status(manifest, old), "pending")
            full_state = manifest.stats()["reconcile_completed_at_us"]

            fresh = _register(store, "slow-log/rm-example-a/fresh", now - HOUR_US)
            hidden = _register(store, "slow-log/rm-example-a/hidden", now - HOUR_US, visible=False)
            binlog = _register(store, "mysql-bin.000001", now - HOUR_US, host="100000001")
            stale = _register(store, "slow-log/rm-example-a/backfill", now - 10 * HOUR_US)
            with store.connection() as connection:
                connection.execute("DELETE FROM parquet_parts WHERE path = ?", (old,))

            result = discover_recent_slowlog_parts(store, manifest, lookback_seconds=6 * 3600, now_us=now)
            self.assertEqual(result["queued_parts"], 1)
            self.assertEqual(_status(manifest, fresh), "pending")
            self.assertIsNone(_status(manifest, hidden))
            self.assertIsNone(_status(manifest, binlog))
            self.assertIsNone(_status(manifest, stale))  # older than the lookback: the next full sweep's job
            self.assertEqual(_status(manifest, old), "pending")  # gone from the source, but only the sweep deletes
            self.assertEqual(manifest.stats()["reconcile_completed_at_us"], full_state)

            reconcile_slowlog_manifest(store, manifest, retention_days=3, now_us=now)
            self.assertEqual(_status(manifest, stale), "pending")
            self.assertEqual(_status(manifest, old), "delete_pending")

    def test_recent_query_seeks_the_time_index(self):
        with tempfile.TemporaryDirectory() as directory:
            store = MetadataStore(Path(directory) / "metadata.sqlite3")
            with store.connection() as connection:
                plan = " ".join(str(row[-1]) for row in connection.execute(
                    "EXPLAIN QUERY PLAN SELECT p.path FROM parquet_parts p CROSS JOIN binlog_files b "
                    "WHERE p.min_event_epoch_us >= ? AND b.id = p.binlog_id AND b.query_visible = 1 "
                    "AND b.host_instance_id = ? AND b.log_file_name LIKE ?", (0, "slow-log", "slow-log/%")))
            self.assertIn("idx_part_time", plan)

    def test_worker_runs_the_full_sweep_hourly_and_the_recent_pass_in_between(self):
        clock = {"now": 1000.0}

        class _Stopping:
            def __init__(self) -> None:
                self.waits = 0

            def is_set(self) -> bool:
                return self.waits >= 5

            def set(self) -> None:
                self.waits = 5

            def wait(self, delay: float) -> bool:
                self.waits += 1
                clock["now"] += 31.0 if self.waits < 4 else 3600.0
                return False

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = SimpleNamespace(
                enabled=True,
                base=SimpleNamespace(io_pressure_recovery_ratio=0.8, min_free_gb=1),
                idle_seconds=5.0,
                reconcile_seconds=30,
                full_reconcile_seconds=3600,
                discovery_lookback_seconds=6 * 3600,
                retention_days=61,
                batch_parts=1,
            )
            worker = ClickHouseSlowLogWorker(root, config)
            worker.stopping = _Stopping()
            calls: list[str] = []
            manifest = SimpleNamespace(recover_loading=lambda: None, stats=lambda: {"pending_parts": 0})
            with (
                patch("app.config.ensure_data_dirs", return_value={"index": root, "logs": root, "scratch": root}),
                patch("app.metadata.MetadataStore"),
                patch("app.clickhouse_slowlog.SlowLogIndex"),
                patch("app.clickhouse_slowlog.ClickHouseManifest", return_value=manifest),
                patch("app.clickhouse_slowlog.ClickHouseSlowLogClient"),
                patch("app.io_pressure.IoPressureGate.from_env", return_value=SimpleNamespace(check=lambda: 0.0)),
                patch("app.clickhouse_ingest.HealthCanary",
                      return_value=SimpleNamespace(probe=lambda *, force=False: None)),
                patch("app.clickhouse_slowlog.SourceIndexPriorityGate",
                      return_value=SimpleNamespace(check=lambda: None)),
                patch("app.clickhouse_slowlog.shutil.disk_usage") as usage,
                patch("app.clickhouse_slowlog.time.monotonic", side_effect=lambda: clock["now"]),
                patch("app.clickhouse_slowlog.reconcile_slowlog_manifest",
                      side_effect=lambda *a, **k: calls.append("full") or {}),
                patch("app.clickhouse_slowlog.discover_recent_slowlog_parts",
                      side_effect=lambda *a, **k: calls.append(f"recent:{k['lookback_seconds']}") or {}),
                patch("app.clickhouse_slowlog.ingest_slowlog_batch", return_value={"claimed_parts": 0}),
                patch("app.maintenance_status.write_json_status"),
            ):
                usage.return_value.free = 1024**4
                self.assertEqual(worker.run(), 0)

            self.assertEqual(calls, ["full", "recent:21600", "recent:21600", "recent:21600", "full"])


if __name__ == "__main__":
    unittest.main()
