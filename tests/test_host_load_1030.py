"""1.34.14: the host-load fixes of 10-30 (sync idle poll, discovery/recovery plans, raw upload CRC, rollup windows,
indexer stats, row-store move slots and purge dedup)."""
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from app import binlog_rows as br
from app import metadata as metadata_module
from app import pipeline as pipeline_module
from app.binlog_lite import RawBinlogError
from app.config import Settings
from app.metadata import MetadataStore
from app.pipeline import SyncManager
from app.raw_binlog import RawBinlogStore
from tests.test_core import remote


@contextmanager
def traced_statements(store):
    """Every SQL statement the store's connections run, with bound values expanded."""
    seen: list[str] = []
    real_connect = sqlite3.connect

    def connect(*args, **kwargs):
        conn = real_connect(*args, **kwargs)
        conn.set_trace_callback(seen.append)
        return conn

    with mock.patch.object(metadata_module.sqlite3, "connect", side_effect=connect):
        yield seen


def plan(store, sql):
    with sqlite3.connect(store.path) as conn:
        return " | ".join(str(row[3]) for row in conn.execute("EXPLAIN QUERY PLAN " + sql))


class DiscoveryPlanTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = MetadataStore(Path(self.temp.name) / "metadata.sqlite3")
        self.settings = Settings(db_instance_id="rm-test000001")
        self.other = Settings(db_instance_id="rm-test000002")
        base = datetime(2026, 10, 1, tzinfo=UTC)
        self.items = [remote(f"mysql-bin.{i:06}", (base + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%SZ"))
                      for i in range(60)]
        self.ids = [file_id for file_id, _state in self.store.upsert_remotes(self.settings, self.items)]
        self.store.upsert_remotes(self.other, self.items[:5])
        states = ["done", "failed", "downloading", "downloaded", "parsing", "stored", "unavailable", "discovered"]
        for index, file_id in enumerate(self.ids):
            self.store.set_file_state(file_id, states[index % len(states)])

    def test_recoverable_files_keep_their_rows_and_order(self):
        records = self.store.recoverable_files(self.settings.db_instance_id)
        self.assertEqual({r["state"] for r in records}, {"failed", "downloading", "downloaded", "parsing", "stored"})
        self.assertEqual([r["log_begin_utc"] for r in records], sorted(r["log_begin_utc"] for r in records))
        with_discovered = self.store.recoverable_files(self.settings.db_instance_id, include_discovered=True)
        self.assertEqual(len(with_discovered), len(records) + sum(1 for i in range(60) if i % 8 == 7))
        self.assertTrue(all(r["instance_id"] == self.settings.db_instance_id for r in with_discovered))
        other = self.store.recoverable_files(self.other.db_instance_id, include_discovered=True)
        self.assertEqual(len(other), 5)  # still discovered there

    def test_recoverable_files_seek_instance_and_state(self):
        with traced_statements(self.store) as seen:
            self.store.recoverable_files(self.settings.db_instance_id, include_discovered=True)
        sql = next(s for s in seen if "FROM binlog_files" in s)
        self.assertIn("idx_binlog_completion (instance_id=? AND state=?)", plan(self.store, sql))

    def test_upsert_page_reads_by_primary_key(self):
        with traced_statements(self.store) as seen:
            again = self.store.upsert_remotes(self.settings, self.items[:10])
        self.assertEqual([file_id for file_id, _ in again], self.ids[:10])
        sql = next(s for s in seen if s.lstrip().startswith("SELECT id, state"))
        self.assertIn("sqlite_autoindex_binlog_files_1", plan(self.store, sql))


class IdleSyncPollTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        root = Path(self.temp.name)
        self.settings = Settings(db_instance_id="rm-test000001")
        self.manager = SyncManager.__new__(SyncManager)
        self.manager.metadata = MetadataStore(root / "metadata.sqlite3")
        self.manager.storage = SimpleNamespace(paths={"downloads": root})
        self.manager._pause_after_current = threading.Event()
        self.manager._shutdown = threading.Event()
        self.manager._event = mock.Mock()
        self.manager._update_pipeline_status = mock.Mock()
        self.manager._scan_start = lambda _settings: datetime.now(UTC) - timedelta(days=1)
        self.job = self.manager.metadata.create_job("sync", self.settings.db_instance_id)

    def test_a_caught_up_job_does_not_rewrite_its_row_while_the_retained_scan_runs(self):
        release = threading.Event()
        fresh = remote("bin.fresh", datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))

        def pages(begin, _end):
            if datetime.fromisoformat(begin.replace("Z", "+00:00")) < datetime.now(UTC) - timedelta(hours=2):
                release.wait(5)  # the retained scan is still paging
            yield [fresh]

        polls = []
        real_pending = pipeline_module.RetainedDiscovery.pending

        def pending(discovery, **kwargs):
            polls.append(time.monotonic())
            if len(polls) >= 6:
                release.set()
            return real_pending(discovery, **kwargs)

        def run_batch(job, client, settings, entries, flavor, archive, *, completed, unavailable, refresh_pending):
            for file_id, _item, _state in entries:
                self.manager.metadata.set_file_state(file_id, "done")
            return completed + len(entries), unavailable, False

        self.manager._run_pending_parallel = run_batch
        writes = []
        real_update = self.manager.metadata.update_job
        self.manager.metadata.update_job = lambda job_id, **values: (writes.append(values), real_update(job_id, **values))
        with mock.patch.object(pipeline_module, "RETAINED_SCAN_POLL_SECONDS", 0.01), \
                mock.patch.object(pipeline_module.RetainedDiscovery, "pending", pending):
            result = self.manager._consume_pending(self.job, SimpleNamespace(iter_binlog_batches=pages), self.settings,
                                                   "mysql", None, "host-a")
        self.assertEqual(result, (1, 0, False))
        self.assertGreaterEqual(len(polls), 6)
        idle_writes = [w for w in writes if w.get("message") == "正在确认是否已到 API 最新文件"]
        # one write when the job ran dry, one after the final live check; the idle polls in between write nothing
        self.assertLessEqual(len(idle_writes), 2)

    def test_poll_interval_is_not_a_spin(self):
        self.assertGreaterEqual(pipeline_module.RETAINED_SCAN_POLL_SECONDS, 1.0)


class RawUploadCrcTests(unittest.TestCase):
    class Bucket:
        def __init__(self):
            self.enable_crc = True
            self.calls = []

        def put_object_from_file(self, key, filename, headers=None):
            self.calls.append((key, self.enable_crc))

    def archive(self):
        from app.oss_store import OssArchive
        archive = OssArchive.__new__(OssArchive)
        archive.bucket = self.Bucket()
        archive.upload_attempts = 1
        archive.upload_retry_delay_seconds = 0
        return archive

    def test_raw_archive_uploads_without_the_sdk_crc_pass_and_keeps_the_shared_bucket(self):
        archive = self.archive()
        with tempfile.NamedTemporaryFile() as handle:
            handle.write(b"x" * 10)
            handle.flush()
            result = archive._put_object_from_file_verified(
                "k1", Path(handle.name), {}, verify=lambda: {"ok": True}, missing_message="m", missing_code="c",
                client_crc=False)
            archive._put_object_from_file_verified(
                "k2", Path(handle.name), {}, verify=lambda: {"ok": True}, missing_message="m", missing_code="c")
        self.assertEqual(result, {"ok": True})
        self.assertEqual(archive.bucket.calls, [("k1", False), ("k2", True)])
        self.assertTrue(archive.bucket.enable_crc)

    def test_raw_binlog_store_put_asks_for_it(self):
        archive = mock.Mock()
        store = RawBinlogStore.__new__(RawBinlogStore)
        store._head = mock.Mock(side_effect=[None, {"ok": True}])
        with tempfile.NamedTemporaryFile() as handle:
            handle.write(b"x")
            handle.flush()
            store._put(archive, Path(handle.name), "key", "a" * 64, "123")
        self.assertIs(archive._put_object_from_file_verified.call_args.kwargs["client_crc"], False)


class OldWindowPartsTests(unittest.TestCase):
    HOUR = 3_600_000_000

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.store = MetadataStore(Path(self.temp.name) / "metadata.sqlite3")
        settings = Settings(db_instance_id="rm-test000001")
        now = int(datetime.now(UTC).timestamp() * 1_000_000)
        self.oldest = now - 40 * 24 * self.HOUR
        file_id, _ = self.store.upsert_remote(settings, remote("mysql-bin.000001", "2026-09-01T00:00:00Z"))
        self.store.set_file_visibility(file_id, True)
        stamp = "2026-10-30T00:00:00Z"
        with self.store.connection() as conn:
            conn.executemany(
                "INSERT INTO parquet_parts(path, binlog_id, logical_part_id, event_date, row_count, min_event_epoch_us, "
                "max_event_epoch_us, size_bytes, sha256, object_sha256, created_at, updated_at) "
                "VALUES(?, ?, ?, '2026-09-01', 1, ?, ?, 1, ?, ?, ?, ?)",
                [(f"p/{i:05}.parquet", file_id, f"l{i}", self.oldest + i * self.HOUR, self.oldest + i * self.HOUR + 600_000_000,
                  f"{i:064x}", f"{i:064x}", stamp, stamp) for i in range(40 * 24)]
                + [("p/long.parquet", file_id, "long", self.oldest, now, "f" * 64, "f" * 64, stamp, stamp)])

    def test_an_old_hour_seeks_the_time_index_and_finds_long_parts(self):
        start = self.oldest + 3 * self.HOUR
        with traced_statements(self.store) as seen:
            rows = self.store.parts_in_range(start_epoch_us=start, end_epoch_us=start + self.HOUR - 1)
        self.assertEqual(sorted(r["path"] for r in rows), ["p/00003.parquet", "p/long.parquet"])
        sql = next(s for s in seen if "FROM parquet_parts p" in s)
        self.assertIn("idx_part_time", plan(self.store, sql))

    def test_a_recent_hour_keeps_the_cold_compression_seek(self):
        start = self.oldest + (40 * 24 - 2) * self.HOUR
        with traced_statements(self.store) as seen:
            rows = self.store.parts_in_range(start_epoch_us=start, end_epoch_us=start + self.HOUR - 1)
        self.assertEqual(sorted(r["path"] for r in rows), [f"p/{40 * 24 - 2:05}.parquet", "p/long.parquet"])
        sql = next(s for s in seen if "FROM" in s and "parquet_parts p" in s)
        self.assertIn("idx_part_cold_compression", plan(self.store, sql))


class StaticStatsTests(unittest.TestCase):
    def test_reused_until_stale_and_recomputed_when_keys_change(self):
        from app.index_worker import cached_static_stats
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "stats.json"
            calls = []
            compute = {"a": lambda: calls.append("a") or {"n": len(calls)}, "b": lambda: calls.append("b") or {"m": 1}}
            clock = [1000.0]
            first = cached_static_stats(path, {}, compute, ["a"], ttl_seconds=60, now=lambda: clock[0])
            clock[0] += 30
            second = cached_static_stats(path, {}, compute, ["a"], ttl_seconds=60, now=lambda: clock[0])  # new process
            self.assertEqual(first, second)
            self.assertEqual(calls, ["a"])
            clock[0] += 31
            cached_static_stats(path, {}, compute, ["a"], ttl_seconds=60, now=lambda: clock[0])
            self.assertEqual(calls, ["a", "a"])
            cached_static_stats(path, {}, compute, ["a", "b"], ttl_seconds=60, now=lambda: clock[0])
            self.assertEqual(calls, ["a", "a", "a", "b"])
            self.assertEqual(cached_static_stats(path, {}, compute, [], ttl_seconds=60), {})
            self.assertEqual(cached_static_stats(path, {}, compute, ["a"], ttl_seconds=0), {})


class MoveSlotTests(unittest.TestCase):
    def worker(self, memory):
        from app import binlog_rows_worker as worker_module
        w = worker_module.Worker.__new__(worker_module.Worker)
        w.stop = threading.Event()
        w.publish = mock.Mock()
        w.clickhouse_memory = mock.Mock(side_effect=memory)
        return w, worker_module

    def test_at_most_the_configured_moves_run_at_once(self):
        w, module = self.worker(lambda: 0)
        w.move_slots = threading.BoundedSemaphore(2)
        active, peak, lock = [0], [0], threading.Lock()

        def move():
            with w.move_slot(0):
                with lock:
                    active[0] += 1
                    peak[0] = max(peak[0], active[0])
                time.sleep(0.05)
                with lock:
                    active[0] -= 1

        threads = [threading.Thread(target=move) for _ in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
        self.assertEqual(peak[0], 2)

    def test_a_move_waits_for_clickhouse_memory_then_starts(self):
        gib = 1024 ** 3
        w, module = self.worker([int(5 * gib), int(5 * gib), int(1 * gib)])
        w.stop.wait = mock.Mock()
        with mock.patch.object(module, "MOVE_MEMORY_MAX", int(3.9 * gib)), \
                self.assertLogs("binlog_rows_worker", level="INFO") as logs:
            with w.move_slot(1):
                pass
        self.assertEqual(w.clickhouse_memory.call_count, 3)
        self.assertIn("BINLOG_ROWS_MOVE_WAIT", logs.output[0])
        w.publish.assert_called_once_with(1, state="waiting", reason="move-memory")

    def test_a_stopped_lane_leaves_no_marker(self):
        w, module = self.worker(lambda: 0)
        w.move_slots = threading.BoundedSemaphore(1)
        w.move_slots.acquire()
        w.stop.set()
        with tempfile.TemporaryDirectory() as tmp:
            w.storage = mock.Mock(paths={"index": Path(tmp)})
            w.ingested = mock.Mock(return_value=set())
            ingestor = mock.Mock()
            ingestor.partitions.return_value = [("2026-10-30", 1)]
            entries = [{"file_id": f"{i:032x}", "instance_id": "i", "source_file_name": f"mysql-bin.{i}", "lo": 1, "hi": 2}
                       for i in (1, 2)]
            with self.assertRaisesRegex(RawBinlogError, "停止"):
                w.commit_batch(ingestor, entries, {e["file_id"]: 1 for e in entries}, 0)
            self.assertEqual(list(w.inflight_dir().glob("*.json")), [])
            ingestor.move.assert_not_called()


class PurgeDedupTests(unittest.TestCase):
    PARTS = [("2026-10-29", 0), ("2026-10-29", 3)]

    def client(self, *, pending=(), done_after=1, count="0"):
        ch = mock.Mock()
        checks = {"n": 0}

        def rows(sql, **kwargs):
            if "multiSearchAny" in sql:
                return [{"mutation_id": m} for m in pending]
            checks["n"] += 1
            return [{"n": 0 if checks["n"] >= done_after else 1}]

        ch.rows.side_effect = rows
        ch.execute.side_effect = lambda sql, **kwargs: count if sql.startswith("SELECT count()") else ""
        return ch

    def deletes(self, ch):
        return [c for c in ch.execute.call_args_list if c.args[0].startswith("DELETE")]

    def test_a_purge_still_running_is_awaited_instead_of_queued_again(self):
        ch = self.client(pending=["mutation_7.txt"], done_after=2)
        with mock.patch.object(br.time, "sleep"), self.assertLogs("app.binlog_rows", level="INFO") as logs:
            br.purge_file_rows(ch, ["raw:a"], 1, 2, partitions=self.PARTS, reason="retry", check_remaining=True)
        self.assertEqual(self.deletes(ch), [])
        self.assertTrue(any("BINLOG_ROWS_PURGE_WAIT" in line for line in logs.output))
        self.assertTrue(any("BINLOG_ROWS_PURGE_NOTHING_LEFT" in line for line in logs.output))

    def test_a_marker_without_rows_left_deletes_nothing(self):
        ch = self.client(count="0\n")
        br.purge_file_rows(ch, ["raw:a"], 1, 2, partitions=self.PARTS, reason="retry", check_remaining=True)
        self.assertEqual(self.deletes(ch), [])

    def test_rows_left_are_deleted_within_the_partitions(self):
        ch = self.client(count="12\n")
        br.purge_file_rows(ch, ["raw:a"], 1, 2, partitions=self.PARTS, reason="retry", check_remaining=True)
        (delete,) = self.deletes(ch)
        self.assertIn("event_date IN ('2026-10-29') AND tbl_bucket IN (0, 3)", delete.args[0])

    def test_a_failed_move_deletes_without_counting_first(self):
        ch = self.client(count="0")
        br.purge_file_rows(ch, ["raw:a"], 1, 2, partitions=self.PARTS, reason="move-failed")
        self.assertEqual(len(self.deletes(ch)), 1)
        self.assertFalse(any(c.args[0].startswith("SELECT count()") for c in ch.execute.call_args_list))

    def test_an_unanswered_count_falls_back_to_the_delete(self):
        ch = self.client()
        ch.execute.side_effect = lambda sql, **kwargs: (_ for _ in ()).throw(RawBinlogError("down", "X")) \
            if sql.startswith("SELECT count()") else ""
        with self.assertLogs("app.binlog_rows", level="WARNING"):
            br.purge_file_rows(ch, ["raw:a"], 1, 2, partitions=self.PARTS, reason="retry", check_remaining=True)
        self.assertEqual(len(self.deletes(ch)), 1)

    def test_a_purge_that_outlives_the_wait_keeps_its_marker(self):
        ch = self.client(pending=["mutation_8.txt"], done_after=10 ** 9)
        with mock.patch.object(br, "PURGE_WAIT_SECONDS", 0), mock.patch.object(br.time, "sleep"), \
                self.assertLogs("app.binlog_rows", level="WARNING"):
            with self.assertRaisesRegex(RawBinlogError, "mutation"):
                br.purge_file_rows(ch, ["raw:a"], 1, 2, partitions=self.PARTS, reason="retry", check_remaining=True)
        self.assertEqual(self.deletes(ch), [])


if __name__ == "__main__":
    unittest.main()
