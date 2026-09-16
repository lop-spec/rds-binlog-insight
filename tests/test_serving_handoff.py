from __future__ import annotations

import json
import statistics
import tempfile
import threading
import time
import unittest
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app.config import Settings
from app.credentials import CloudCredential
from app.metadata import MetadataStore
from app.pipeline import SyncManager
from app.server import AppHTTPServer
from app.slowlog_index import SlowLogIndex
from app.storage import EventStorage
from tests.test_slowlog_index import _event, _part, _register_slowlog_part
from tests.test_sqlite_serving_performance import insert_fixture

EPOCH = 1_787_286_000_000_000


class RequestCoverageTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.store = MetadataStore(root / "metadata.sqlite3")
        self.settings = Settings(db_instance_id="rm-prod", retention_days=365)
        clock = patch("app.storage.datetime", wraps=datetime)
        clock.start().now.return_value = datetime.fromtimestamp(EPOCH / 1_000_000 + 86400, UTC)
        self.addCleanup(clock.stop)
        self.storage = EventStorage(self.store, root)
        self.addCleanup(self.storage.slowlog_index.close)
        rows = [_event(f"event-{i}", EPOCH + i, rows_examined=10, rows_sent=1, query_ms=100) for i in range(2)]
        source = root / "slow.parquet"
        self.part = _register_slowlog_part(self.store, _part(source, "fixture", rows))
        self.storage.slowlog_index.build_part(self.part, source)
        self.query = {"source": "slowlog", "instance": "rm-prod", "start_epoch_us": EPOCH, "end_epoch_us": EPOCH + 1}

    def test_real_http_events_and_analytics_scan_and_check_once_per_request(self):
        sync = SimpleNamespace(archive_for_settings=Mock(side_effect=AssertionError("OSS must stay lazy")))
        application = SimpleNamespace(metadata=self.store, storage=self.storage, sync=sync)
        httpd = AppHTTPServer(("127.0.0.1", 0), application)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            with (patch.object(self.store, "load_settings", return_value=self.settings),
                  patch.object(self.store, "parts_in_range", wraps=self.store.parts_in_range) as scans,
                  patch.object(self.storage, "_slowlog_coverage_with_repair", wraps=self.storage._slowlog_coverage_with_repair) as coverage):
                for endpoint in ("events", "analytics"):
                    scans.reset_mock()
                    coverage.reset_mock()
                    url = (f"http://127.0.0.1:{httpd.server_address[1]}/api/{endpoint}"
                           f"?source=slowlog&instance=rm-prod&startEpochUs={EPOCH}&endEpochUs={EPOCH+1}")
                    with urllib.request.urlopen(url, timeout=5) as response:
                        actual = json.loads(response.read())["data"]
                    self.assertEqual(scans.call_count, 1)
                    self.assertEqual(coverage.call_count, 1)
                    sync.archive_for_settings.assert_not_called()
                    if endpoint == "events":
                        self.assertEqual([row["event_id"] for row in actual["rows"]], ["event-1", "event-0"])
                    else:
                        expected = self.storage.analytics_summary(dict(self.query), self.settings, None)
                        self.assertEqual(actual["sql"], expected["sql"])
                        self.assertTrue(actual["coverage"]["complete"])
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=5)

    def test_complete_clickhouse_skips_sqlite_coverage_preflight(self):
        self.storage.clickhouse_slowlog_backend = SimpleNamespace(
            summarize=Mock(return_value={
                "sql": {"summary": {"executions": 2}},
                "clickhouse_slowlog_coverage": {"complete": True},
            }),
            stats=Mock(return_value={"ready_parts": 1}),
        )
        with patch.object(self.storage, "_slowlog_coverage_with_repair",
                          side_effect=AssertionError("SQLite coverage must stay lazy")):
            result = self.storage.analytics_summary(self.query, self.settings, None)
        self.assertEqual(result["evidence"]["engine"], "clickhouse")
        self.assertTrue(result["coverage"]["complete"])
        self.assertEqual(result["sql"]["summary"]["executions"], 2)

    def test_incomplete_events_checks_and_enqueues_once_and_uses_factory_once(self):
        self.storage.slowlog_index.remove_path(self.part["path"])
        archive = object()
        factory = Mock(return_value=archive)
        with (patch.object(self.storage, "_query_events_tiered_singleflight", return_value={"rows": []}) as fallback,
              patch.object(self.storage.slowlog_index, "enqueue_parts", wraps=self.storage.slowlog_index.enqueue_parts) as queued,
              patch.object(self.store, "parts_in_range", wraps=self.store.parts_in_range) as scans,
              self.assertLogs("app.storage", level="WARNING")):
            result = self.storage.query_events_tiered(self.query, self.settings, None, archive_factory=factory)
        self.assertFalse(result["slowlog_index_coverage"]["complete"])
        self.assertTrue(result["slowlog_index_fallback"])
        self.assertEqual(scans.call_count, 1)
        self.assertEqual(queued.call_count, 1)
        factory.assert_called_once_with()
        self.assertIs(fallback.call_args.args[2], archive)

    def test_incomplete_analytics_retains_coverage_and_lazy_archive_failures(self):
        self.storage.slowlog_index.remove_path(self.part["path"])
        factory = Mock(return_value=None)
        with (patch.object(self.storage.slowlog_index, "enqueue_parts", wraps=self.storage.slowlog_index.enqueue_parts) as queued,
              patch.object(self.store, "parts_in_range", wraps=self.store.parts_in_range) as scans,
              self.assertLogs("app.storage", level="WARNING")):
            result = self.storage.analytics_summary(self.query, self.settings, None, scan_limit=0, archive_factory=factory)
        factory.assert_called_once_with()
        self.assertEqual(queued.call_count, 1)
        self.assertEqual(scans.call_count, 1)
        self.assertFalse(result["coverage"]["complete"])
        error = Mock(side_effect=RuntimeError("archive unavailable"))
        for method in (self.storage.query_events_tiered, self.storage.analytics_summary):
            with self.assertRaisesRegex(RuntimeError, "archive unavailable"):
                method(self.query, self.settings, None, archive_factory=error)
        self.assertEqual(error.call_count, 2)

    def test_no_cross_request_cache_after_index_writer_changes_coverage(self):
        # First request complete; a concurrent removal must be visible to the next
        # request. No application/session/global memoization is introduced.
        first = self.storage.query_events_tiered(self.query, self.settings, None)
        self.assertTrue(first["slowlog_index_coverage"]["complete"])
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(self.storage.slowlog_index.remove_path, self.part["path"]).result(timeout=5)
        factory = Mock(return_value=None)
        with patch.object(self.storage, "_query_events_tiered_singleflight", return_value={"rows": []}):
            second = self.storage.query_events_tiered(self.query, self.settings, None, archive_factory=factory)
        self.assertFalse(second["slowlog_index_coverage"]["complete"])
        factory.assert_called_once_with()

    def test_physical_cleanup_failure_preserves_index_and_metadata_until_retry(self):
        target = Path(self.part["path"])
        unlink = Path.unlink

        def fail_target(path, *args, **kwargs):
            if path == target:
                raise PermissionError("fixture body busy")
            return unlink(path, *args, **kwargs)

        with patch.object(Path, "unlink", fail_target):
            failed = self.storage.cleanup(0)
        self.assertEqual(failed["deleted_parts"], 0)
        self.assertIn("fixture body busy", failed["errors"][0])
        self.assertIsNotNone(self.store.part_by_path(str(target)))
        self.assertEqual(self.storage.slowlog_index.existing_event_ids(["event-0"], "rm-prod"), {"event-0"})
        retried = self.storage.cleanup(0)
        self.assertEqual(retried["errors"], [])
        self.assertEqual(retried["deleted_parts"], 1)
        self.assertIsNone(self.store.part_by_path(str(target)))
        self.assertEqual(self.storage.slowlog_index.existing_event_ids(["event-0"], "rm-prod"), set())

    def test_existing_archive_wins_over_factory(self):
        self.storage.slowlog_index.remove_path(self.part["path"])
        existing = object()
        factory = Mock(side_effect=AssertionError("must not replace existing archive"))
        with patch.object(self.storage, "_query_events_tiered_singleflight", return_value={"rows": []}) as fallback:
            self.storage.query_events_tiered(self.query, self.settings, existing, archive_factory=factory)
        self.assertIs(fallback.call_args.args[2], existing)
        factory.assert_not_called()


class ConcurrentCollectionTests(unittest.TestCase):
    def test_independent_writers_repeated_collection_keeps_one_canonical_event(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            indexes = [SlowLogIndex(root / "slowlog.sqlite3") for _ in range(2)]
            try:
                row = _event("same-event", EPOCH, rows_examined=100, rows_sent=1, query_ms=50)
                parts = [_part(root / f"{i}.parquet", f"part-{i}", [row]) for i in range(2)]
                barrier = threading.Barrier(2)

                def collect(i):
                    index = indexes[i]
                    barrier.wait(timeout=5)
                    # Both collectors may see an absent id; publication remains
                    # responsible for canonical uniqueness, not this read probe.
                    index.existing_event_ids(["same-event"], "rm-prod")
                    for _ in range(3):
                        index.build_part(parts[i], Path(parts[i]["path"]))
                    return index.existing_event_ids(["same-event"], "rm-prod")

                with ThreadPoolExecutor(max_workers=2) as pool:
                    futures = [pool.submit(collect, i) for i in range(2)]
                    for future in futures:
                        self.assertEqual(future.result(timeout=15), {"same-event"})
                with indexes[0].connection() as conn:
                    row = conn.execute("SELECT COUNT(*), SUM(is_canonical) FROM slowlog_events").fetchone()
                self.assertEqual(tuple(row), (2, 1))
                indexes[0].remove_path(parts[0]["path"])
                self.assertEqual(indexes[1].existing_event_ids(["same-event"], "rm-prod"), {"same-event"})
            finally:
                for index in indexes:
                    index.close()


class RetentionHandoffTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.store = MetadataStore(root / "metadata.sqlite3")
        self.storage = EventStorage(self.store, root)
        self.addCleanup(self.storage.slowlog_index.close)
        self.settings = Settings(auto_sync=False, db_instance_id="rm-test", retention_days=60)
        clock = patch("app.storage.datetime", wraps=datetime)
        clock.start().now.return_value = datetime.fromtimestamp(EPOCH / 1_000_000 + 86400, UTC)
        self.addCleanup(clock.stop)
        self.store.save_settings(self.settings)
        self.owner = SyncManager(self.store, self.storage, start_scheduler=False)
        self.addCleanup(self.owner.shutdown)
        self.secondary = SyncManager(self.store, self.storage, start_scheduler=False, role="secondary", retention_owner=self.owner)
        self.addCleanup(self.secondary.shutdown)
        self.result = {"errors": [], "deleted_parts": 0, "rewritten_parts": 0, "removed_rows": 0}

    def test_both_roles_handoff_all_three_sweeps_only_to_a_live_shared_owner(self):
        with (patch.object(self.owner, "_scheduler", Mock(is_alive=Mock(return_value=True))),
              patch.object(self.storage, "cleanup") as cleanup,
              patch.object(self.storage, "enforce_local_cache_limit") as local,
              patch.object(self.storage, "enforce_query_cache_limit") as query):
            for manager in (self.owner, self.secondary):
                self.assertEqual(manager._cleanup_after_sync(self.settings), {"errors": []})
            cleanup.assert_not_called()
            local.assert_not_called()
            query.assert_not_called()

    def test_standalone_dead_owner_and_override_keep_cleanup_and_errors(self):
        with (patch.object(self.storage, "cleanup", side_effect=lambda *_a, **_k: {**self.result, "errors": ["physical cleanup failed"]}) as cleanup,
              patch.object(self.storage, "enforce_local_cache_limit", return_value={"errors": ["cache failed"]}),
              patch.object(self.storage, "enforce_query_cache_limit", return_value={"errors": []})):
            scenarios = [(None, self.settings), (Mock(is_alive=Mock(return_value=False)), self.settings),
                         (Mock(is_alive=Mock(return_value=True)), replace(self.settings, retention_days=30)),
                         (Mock(is_alive=Mock(return_value=True)), replace(self.settings, oss_enabled=True))]
            for scheduler, settings in scenarios:
                with patch.object(self.owner, "_scheduler", scheduler):
                    self.assertEqual(self.secondary._cleanup_after_sync(settings)["errors"],
                                     ["physical cleanup failed", "cache failed"])
            with patch.object(self.secondary, "_retention_owner", None):
                self.secondary._cleanup_after_sync(self.settings)
            self.assertEqual(cleanup.call_count, 5)

    def test_busy_primary_and_stopping_scheduler_cannot_take_secondary_cleanup(self):
        with (patch.object(self.owner, "_scheduler", Mock(is_alive=Mock(return_value=True))),
              patch.object(self.storage, "cleanup", return_value=self.result) as cleanup,
              patch.object(self.storage, "enforce_local_cache_limit", return_value={"errors": []}),
              patch.object(self.storage, "enforce_query_cache_limit", return_value={"errors": []})):
            with patch.object(self.owner, "_worker", Mock(is_alive=Mock(return_value=True))):
                with self.assertLogs("app.pipeline", "INFO") as logs:
                    self.secondary._cleanup_after_sync(self.settings)
                self.assertTrue(any("primary-worker-active" in row for row in logs.output))
                # The primary is itself finishing; next scheduler pass can run.
                self.owner._cleanup_after_sync(self.settings)
                self.assertEqual(cleanup.call_count, 1)
            with patch.object(self.owner, "_shutdown", Mock(is_set=Mock(return_value=True))):
                self.secondary._cleanup_after_sync(self.settings)
                self.assertEqual(cleanup.call_count, 2)
            with patch.object(self.owner, "storage", object()):
                self.secondary._cleanup_after_sync(self.settings)
                self.assertEqual(cleanup.call_count, 3)

    def test_scheduled_failure_logs_and_retries_next_due_pass_without_stopping_sync(self):
        for failure in (OSError("disk failure"), {**self.result, "errors": ["one part failed"]}):
            with self.subTest(failure=failure):
                self.owner._last_retention_cleanup = float("-inf")
                with (patch.object(self.storage, "cleanup", side_effect=[failure, self.result]) as cleanup,
                      patch.object(self.storage, "enforce_local_cache_limit", return_value={"errors": []}),
                      patch.object(self.storage, "enforce_query_cache_limit", return_value={"errors": []}),
                      patch("app.pipeline.time.monotonic", side_effect=[10, 20, 3611])):
                    with self.assertLogs("app.pipeline", level="WARNING"):
                        self.owner._run_retention_cleanup_if_due(self.settings)
                    self.owner._run_retention_cleanup_if_due(self.settings)
                    self.assertEqual(cleanup.call_count, 1)
                    self.owner._run_retention_cleanup_if_due(self.settings)
                    self.assertEqual(cleanup.call_count, 2)
                self.assertFalse(self.owner._retention_cleanup_lock.locked())

    def test_zero_file_sync_fixed_fixture_skips_all_global_sweeps(self):
        # Real cleanup over 1000 retained local bodies versus the live-owner
        # handoff, on the same unchanged parts. Only sync bookkeeping differs.
        with self.store.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            insert_fixture(conn, "binlog_files", [{"id": "fixture", "instance_id": "rm-test",
                                                     "log_file_name": "mysql-bin.fixture", "query_visible": 1}])
            parts = []
            for i in range(1000):
                path = self.storage.paths["events"] / f"fixture-{i}.parquet"
                path.touch()
                parts.append({"path": str(path), "logical_part_id": f"fixture-{i}",
                              "binlog_id": "fixture", "min_event_epoch_us": EPOCH,
                              "max_event_epoch_us": EPOCH + 1, "row_count": 1})
            insert_fixture(conn, "parquet_parts", parts)
            conn.commit()
        client = SimpleNamespace(verify_instance=lambda: {"dbInstanceId": "rm-test", "engine": "mysql"})
        times = {"before": [], "after": []}
        counts = {}
        with (patch.object(self.owner, "client_factory", return_value=client),
              patch.object(self.owner, "archive_for_settings", return_value=None),
              patch.object(self.owner, "_discover", return_value=[]),
              patch.object(self.store, "list_parts", wraps=self.store.list_parts) as sweeps):
            for cycle in range(4):
                cases = [("before", None), ("after", Mock(is_alive=Mock(return_value=True)))]
                for label, scheduler in (cases if cycle % 2 else cases[::-1]):
                    job = self.store.create_job("sync", "rm-test")
                    sweeps.reset_mock()
                    with patch.object(self.owner, "_scheduler", scheduler):
                        started = time.perf_counter()
                        self.owner._run(job, self.settings, CloudCredential("fixture", "fixture"))
                        times[label].append((time.perf_counter() - started) * 1000)
                    counts[label] = sweeps.call_count
                    self.assertEqual(self.store.latest_job()["status"], "success")
        self.assertGreaterEqual(counts["before"], 2)
        self.assertEqual(counts["after"], 0)
        self.assertEqual(len(self.store.list_parts(limit=2000)), 1000)
        self.assertTrue(all(Path(part["path"]).is_file() for part in parts))
        print(json.dumps({"case": "zero-file-sync-1000-retained-parts",
                          "before_ms": round(statistics.median(times["before"]), 3),
                          "after_ms": round(statistics.median(times["after"]), 3),
                          "before_global_scans": counts["before"], "after_global_scans": counts["after"]}), flush=True)

    def test_standalone_sync_still_reports_retention_partial(self):
        client = SimpleNamespace(verify_instance=lambda: {"dbInstanceId": "rm-test", "engine": "mysql"})
        job = self.store.create_job("sync", "rm-test")
        with (patch.object(self.owner, "client_factory", return_value=client),
              patch.object(self.owner, "archive_for_settings", return_value=None),
              patch.object(self.owner, "_discover", return_value=[]),
              patch.object(self.storage, "cleanup", return_value={**self.result, "errors": ["fixture failure"]}),
              patch.object(self.storage, "enforce_local_cache_limit", return_value={"errors": []}),
              patch.object(self.storage, "enforce_query_cache_limit", return_value={"errors": []})):
            self.owner._run(job, self.settings, CloudCredential("fixture", "fixture"))
        self.assertEqual(self.store.latest_job()["error_code"], "RETENTION_PARTIAL")
        self.assertEqual(self.store.latest_job()["status"], "warning")


if __name__ == "__main__":
    unittest.main()
