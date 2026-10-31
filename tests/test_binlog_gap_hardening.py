"""1.35.0: binlog collection must not leave gaps that can be avoided, and must report the ones it cannot avoid."""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app import binlog_rows as br
from app.binlog_lite import RawBinlogError
from app.config import Settings
from app.downloader import DownloadError
from app.metadata import MetadataStore, retry_busy
from app.pipeline import SyncManager
from app.rds_api import RdsApiError, RdsRpcClient, RemoteBinlog
from app.credentials import CloudCredential
from app.sync_discovery import HostPlan, RetainedDiscovery, fair_pending, sequence_holes
from app.sync_watchdog import SyncWatchdog

INSTANCE = "rm-test000001"


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def binlog(name: str, begin: datetime, end: datetime, host: str = "host-a", *, size: int = 123,
           status: str = "Completed") -> RemoteBinlog:
    return RemoteBinlog(
        log_file_name=name, log_begin_utc=iso(begin), log_end_utc=iso(end), file_size=size,
        checksum_crc64="456", download_link="https://fixture.invalid/public",
        intranet_download_link="https://fixture.invalid/internal", link_expired_utc="", remote_status=status,
        host_instance_id=host,
    )


def plan_rows(*files):
    return [{"host_instance_id": f.host_instance_id, "log_file_name": f.log_file_name,
             "log_begin_utc": f.log_begin_utc, "log_end_utc": f.log_end_utc} for f in files]


T0 = datetime(2026, 10, 29, 16, 0, tzinfo=UTC)


def at(minutes: float) -> datetime:
    return T0 + timedelta(minutes=minutes)


class HostPlanTests(unittest.TestCase):
    """Only the primary's files used to count: a switch lost the old primary's rest and doubled the new one's past."""

    def test_steady_state_takes_the_primary_and_never_a_replica(self):
        plan = HostPlan(plan_rows(binlog("mysql-bin.000010", at(0), at(1))), "host-a")
        self.assertTrue(plan.accept_new(binlog("mysql-bin.000011", at(1), at(2))))
        self.assertFalse(plan.accept_new(binlog("mysql-bin.000500", at(1), at(2), host="replica")))

    def test_new_primary_skips_its_files_from_before_the_switch_and_takes_the_boundary_file(self):
        old = [binlog(f"mysql-bin.{100 + i}", at(i), at(i + 1), host="old") for i in range(120)]
        plan = HostPlan(plan_rows(*old), "new")
        before = binlog("mysql-bin.000050", at(10), at(11), host="new")
        boundary = binlog("mysql-bin.000160", at(119.5), at(121), host="new")
        after = binlog("mysql-bin.000161", at(121), at(122), host="new")
        self.assertFalse(plan.accept_new(before))  # replicated copy of what the old primary's files hold
        self.assertTrue(plan.accept_new(boundary))
        plan.add(boundary)
        self.assertTrue(plan.accept_new(after))

    def test_former_primary_keeps_its_uncollected_files_until_the_new_primary_takes_over(self):
        old = [binlog(f"mysql-bin.{100 + i}", at(i), at(i + 1), host="old") for i in range(10)]
        plan = HostPlan(plan_rows(*old), "new")
        late_old = binlog("mysql-bin.000110", at(10), at(11), host="old")
        self.assertTrue(plan.accept_new(late_old))  # listed after the switch was noticed: still part of the timeline
        plan.add(late_old)
        first_new = binlog("mysql-bin.000900", at(10.5), at(12), host="new")
        self.assertTrue(plan.accept_new(first_new))
        plan.add(first_new)
        self.assertFalse(plan.accept_new(binlog("mysql-bin.000111", at(11), at(12), host="old")))

    def test_unknown_primary_stays_with_known_hosts(self):
        plan = HostPlan(plan_rows(binlog("mysql-bin.1", at(0), at(1))), "")
        self.assertTrue(plan.accept_new(binlog("mysql-bin.2", at(1), at(2))))
        self.assertFalse(plan.accept_new(binlog("mysql-bin.9", at(1), at(2), host="other")))
        self.assertFalse(HostPlan([], "").accept_new(binlog("mysql-bin.1", at(0), at(1))))

    def test_sequence_holes_name_the_missing_numbers_and_their_window(self):
        rows = [{"host_instance_id": "a", "log_file_name": f"mysql-bin.{n:06}", "log_begin_utc": iso(at(n)),
                 "log_end_utc": iso(at(n + 1))} for n in (1, 2, 5, 6)]
        holes = sequence_holes(rows, 0)
        self.assertEqual(holes, [("a", "mysql-bin.000002", "mysql-bin.000005", iso(at(3)), iso(at(5)), 2)])


class FairPendingTests(unittest.TestCase):
    def test_files_close_to_source_expiry_go_first_oldest_first(self):
        now = datetime.now(UTC)
        entries = [(f"f{i}", binlog(f"mysql-bin.{i}", now - timedelta(hours=h), now - timedelta(hours=h - 0.01)),
                    "discovered") for i, h in enumerate((150, 0.2, 140, 3, 0.1))]
        urgent_before = iso(now - timedelta(days=7) + timedelta(hours=48))
        order = [entry[0] for entry in fair_pending(entries, now, urgent_before=urgent_before)]
        self.assertEqual(order[:2], ["f0", "f2"])
        self.assertEqual(set(order), {"f0", "f1", "f2", "f3", "f4"})


class DiscoveryFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.settings = Settings(db_instance_id=INSTANCE)
        self.metadata = MetadataStore(self.root / "metadata.sqlite3")
        self.manager = SyncManager.__new__(SyncManager)
        self.manager.metadata = self.metadata
        self.manager.storage = SimpleNamespace(paths={"downloads": self.root})
        self.manager._pause_after_current = threading.Event()
        self.manager._shutdown = threading.Event()
        self.manager._event = Mock()
        self.job = self.metadata.create_job("sync", INSTANCE)

    def state(self, item):
        return self.metadata.file_record(self.metadata.file_id(INSTANCE, item))["state"]


class DiscoveryTests(DiscoveryFixture):
    def test_unavailable_file_listed_again_is_collected(self):
        now = datetime.now(UTC)
        item = binlog("mysql-bin.000001", now - timedelta(hours=2), now - timedelta(hours=2) + timedelta(seconds=30))
        file_id, _ = self.metadata.upsert_remote(self.settings, item)
        self.metadata.set_file_state(file_id, "unavailable", error_code="INSTANCE_BINLOG_NOT_FOUND")
        client = SimpleNamespace(iter_binlog_batches=lambda *_: iter([[item]]))
        with self.assertLogs("app.metadata", "WARNING"):
            with RetainedDiscovery(self.manager, self.job, client, self.settings, "host-a") as discovery:
                discovery.future.result(timeout=5)
                pending = discovery.pending()
        self.assertEqual([entry[0] for entry in pending], [file_id])
        self.assertEqual(self.state(item), "discovered")

    def test_former_primary_file_is_collected_unless_the_primary_covers_it(self):
        now = datetime.now(UTC)
        covered = binlog("mysql-bin.000010", now - timedelta(hours=3), now - timedelta(hours=3) + timedelta(minutes=1),
                         host="old")
        uncovered = binlog("mysql-bin.000011", now - timedelta(hours=5), now - timedelta(hours=5) + timedelta(minutes=1),
                           host="old")
        primary = binlog("mysql-bin.000500", now - timedelta(hours=3, minutes=1), now - timedelta(hours=2), host="new")
        for item in (covered, uncovered):
            self.metadata.upsert_remote(self.settings, item)
        client = SimpleNamespace(iter_binlog_batches=lambda *_: iter([[primary]]))
        with RetainedDiscovery(self.manager, self.job, client, self.settings, "new") as discovery:
            discovery.future.result(timeout=5)
            names = {entry[1].log_file_name for entry in discovery.pending()}
        self.assertEqual(names, {"mysql-bin.000011", "mysql-bin.000500"})
        self.assertEqual(self.state(covered), "skipped")
        self.assertEqual(self.state(uncovered), "discovered")

    def test_files_waiting_for_their_retry_are_not_offered(self):
        now = datetime.now(UTC)
        first = binlog("mysql-bin.000001", now - timedelta(hours=2), now - timedelta(hours=2) + timedelta(seconds=30))
        second = binlog("mysql-bin.000002", now - timedelta(hours=2) + timedelta(seconds=30), now - timedelta(hours=1))
        waiting, _ = self.metadata.upsert_remote(self.settings, first)
        client = SimpleNamespace(iter_binlog_batches=lambda *_: iter([[first, second]]))
        with RetainedDiscovery(self.manager, self.job, client, self.settings, "host-a",
                               eligible=lambda file_id: file_id != waiting) as discovery:
            discovery.future.result(timeout=5)
            self.assertEqual([entry[1].log_file_name for entry in discovery.pending()], ["mysql-bin.000002"])

    def test_a_failed_live_listing_is_retried_instead_of_failing_the_job(self):
        calls = []

        def pages(begin, end):
            calls.append(begin)
            span = datetime.fromisoformat(end.replace("Z", "+00:00")) - datetime.fromisoformat(begin.replace("Z", "+00:00"))
            if span == timedelta(hours=1):  # the live window
                raise RdsApiError("throttled", code="Throttling.User")
            yield []
        with RetainedDiscovery(self.manager, self.job, SimpleNamespace(iter_binlog_batches=pages), self.settings,
                               "host-a") as discovery:
            discovery.future.result(timeout=5)
            with self.assertLogs("app.sync_discovery", "WARNING"):
                self.assertEqual(discovery.pending(force_recent=True), [])
            discovery.live_failing_since = time.monotonic() - 3600
            with self.assertRaises(RdsApiError):
                discovery.pending(force_recent=True)

    def test_retained_scan_lists_bounded_slices_and_relists_a_drifting_one(self):
        windows = []
        drift = {"left": 1}

        def pages(begin, end):
            windows.append((begin, end))
            if drift["left"]:
                drift["left"] -= 1
                raise RdsApiError("drift", code="INCOMPLETE_PAGINATION")
            yield []
        with self.assertLogs("app.sync_discovery", "WARNING"):
            with RetainedDiscovery(self.manager, self.job, SimpleNamespace(iter_binlog_batches=pages), self.settings,
                                   "host-a") as discovery:
                self.assertTrue(discovery.future.result(timeout=5))
        spans = [(datetime.fromisoformat(e.replace("Z", "+00:00")) - datetime.fromisoformat(b.replace("Z", "+00:00")))
                 for b, e in windows]
        self.assertTrue(all(span <= timedelta(hours=6) for span in spans))
        self.assertEqual(windows[0], windows[1])  # the drifting slice was listed again
        first = datetime.fromisoformat(windows[0][0].replace("Z", "+00:00"))
        self.assertLess(datetime.now(UTC) - first, timedelta(days=9))  # source retention + 1 day, not 60 days

    def test_a_file_still_uploading_is_registered_once_complete(self):
        now = datetime.now(UTC)
        uploading = binlog("mysql-bin.000001", now - timedelta(minutes=30), now - timedelta(minutes=29),
                           status="Uploading")
        listing = {"items": [uploading]}
        client = SimpleNamespace(iter_binlog_batches=lambda *_: iter([listing["items"]]))
        with RetainedDiscovery(self.manager, self.job, client, self.settings, "host-a") as discovery:
            discovery.future.result(timeout=5)
            self.assertEqual(discovery.pending(), [])
            self.assertIsNone(self.metadata.file_record(self.metadata.file_id(INSTANCE, uploading)))
            done = replace(uploading, remote_status="Completed", file_size=999)
            listing["items"] = [done]
            self.assertEqual([e[1].file_size for e in discovery.pending(force_recent=True)], [999])

    def test_primary_change_inside_a_job_is_picked_up(self):
        discovery = RetainedDiscovery(self.manager, self.job, SimpleNamespace(), self.settings, "old",
                                      resolve_primary=lambda: "new")
        discovery._refresh_primary()
        self.assertEqual(discovery.primary, "new")
        self.assertEqual(discovery.plan().primary, "new")
        self.assertIn("PRIMARY_CHANGED", [c.args[2] for c in self.manager._event.call_args_list])


class StorageStub:
    def __init__(self, root):
        self.paths = {"downloads": root}

    @staticmethod
    def purge_legacy_body_caches():
        return {"deleted_bytes": 0, "errors": []}


class ManagerFixture(DiscoveryFixture):
    def setUp(self):
        super().setUp()
        self.manager = SyncManager(self.metadata, StorageStub(self.root), start_scheduler=False)
        self.manager._event = Mock()

    def tearDown(self):
        self.manager.shutdown()


class FileFailureTests(ManagerFixture):
    def register(self, item):
        file_id, _ = self.metadata.upsert_remote(self.settings, item)
        return file_id

    def test_a_transient_failure_defers_the_file_and_the_job_goes_on(self):
        now = datetime.now(UTC)
        item = binlog("mysql-bin.1", now - timedelta(minutes=5), now - timedelta(minutes=4))
        file_id = self.register(item)
        result = self.manager._file_failed(self.job, file_id, item, DownloadError("reset", "NETWORK_ERROR"), 0)
        self.assertEqual(result, 0)
        self.assertEqual(self.metadata.file_record(file_id)["state"], "failed")
        self.assertFalse(self.manager._file_due(file_id))
        self.assertEqual(self.manager.deferred_files(), 1)
        self.assertIn("FILE_DEFERRED", [c.args[2] for c in self.manager._event.call_args_list])

    def test_recent_file_missing_from_the_listing_is_retried_not_declared_lost(self):
        now = datetime.now(UTC)
        item = binlog("mysql-bin.1", now - timedelta(hours=3), now - timedelta(hours=2))
        file_id = self.register(item)
        self.manager._file_failed(self.job, file_id, item, DownloadError("gone", "DOWNLOAD_LINK_REFRESH_MISSING"), 0)
        self.assertEqual(self.metadata.file_record(file_id)["state"], "failed")

    def test_file_past_the_source_retention_missing_from_the_listing_is_unavailable(self):
        now = datetime.now(UTC)
        item = binlog("mysql-bin.1", now - timedelta(days=8), now - timedelta(days=8) + timedelta(minutes=1))
        file_id = self.register(item)
        result = self.manager._file_failed(self.job, file_id, item,
                                           DownloadError("gone", "DOWNLOAD_LINK_REFRESH_MISSING"), 0)
        self.assertEqual(result, 1)
        record = self.metadata.file_record(file_id)
        self.assertEqual((record["state"], record["error_code"]), ("unavailable", "INSTANCE_BINLOG_NOT_FOUND"))

    def test_systemic_failure_still_fails_the_job(self):
        now = datetime.now(UTC)
        item = binlog("mysql-bin.1", now - timedelta(minutes=5), now - timedelta(minutes=4))
        file_id = self.register(item)
        with self.assertRaises(RdsApiError):
            self.manager._file_failed(self.job, file_id, item, RdsApiError("denied", code="Forbidden.RAM"), 0)

    def test_a_run_of_failures_trips_the_breaker(self):
        now = datetime.now(UTC)
        from app import pipeline
        with self.assertRaises(DownloadError), self.assertLogs("app.pipeline", "ERROR"):
            for index in range(pipeline.FILE_FAILURE_BREAKER):
                item = binlog(f"mysql-bin.{index}", now - timedelta(minutes=5), now - timedelta(minutes=4))
                self.manager._file_failed(self.job, self.register(item), item,
                                          DownloadError("reset", "NETWORK_ERROR"), 0)

    def test_changed_identity_settles_the_old_row(self):
        now = datetime.now(UTC)
        item = binlog("mysql-bin.1", now - timedelta(minutes=5), now - timedelta(minutes=4))
        file_id = self.register(item)
        self.manager._file_failed(self.job, file_id, item, DownloadError("renamed", "SOURCE_IDENTITY_CHANGED"), 0)
        self.assertEqual(self.metadata.file_record(file_id)["state"], "superseded")

    def test_primary_lookup_failure_falls_back_to_the_last_known_primary(self):
        client = SimpleNamespace(primary_host_instance_id=Mock(side_effect=[
            "host-a", RdsApiError("two masters", code="PRIMARY_NODE_NOT_UNIQUE")]))
        self.assertEqual(self.manager._resolve_primary(client), "host-a")
        with self.assertLogs("app.pipeline", "WARNING"):
            self.assertEqual(self.manager._resolve_primary(client), "host-a")

    def test_source_retention_from_the_backup_policy_bounds_the_scan(self):
        self.manager._refresh_source_retention(SimpleNamespace(log_backup_retention_days=lambda: 3))
        start = self.manager._scan_start(Settings(db_instance_id=INSTANCE, retention_days=60))
        self.assertAlmostEqual((datetime.now(UTC) - start).total_seconds(), 4 * 86400, delta=60)
        self.manager._refresh_source_retention(SimpleNamespace(log_backup_retention_days=lambda: Mock()))
        self.assertEqual(self.manager._source_retention(), 3)

    def test_refresh_lists_the_files_own_span_and_reports_a_new_identity(self):
        now = datetime.now(UTC)
        old = binlog("mysql-bin.1", now - timedelta(minutes=5), now - timedelta(minutes=4))
        renamed = replace(old, file_size=999)
        client = SimpleNamespace(list_binlogs=Mock(return_value=[renamed]))
        with self.assertRaises(DownloadError) as raised:
            self.manager._refresh_item(client, self.settings, old)
        self.assertEqual(raised.exception.code, "SOURCE_IDENTITY_CHANGED")
        begin, end = client.list_binlogs.call_args.args
        self.assertEqual((begin, end), (iso(now - timedelta(minutes=6)), iso(now - timedelta(minutes=3))))
        self.assertIsNotNone(self.metadata.file_record(self.metadata.file_id(INSTANCE, renamed)))

    def test_download_resumes_after_a_dropped_connection(self):
        now = datetime.now(UTC)
        item = binlog("mysql-bin.1", now - timedelta(minutes=5), now - timedelta(minutes=4))
        file_id = self.register(item)
        result = SimpleNamespace(path=self.root / "x.binlog", size_bytes=123, sha256="s" * 64)
        with patch("app.pipeline.download_file", side_effect=[DownloadError("reset", "NETWORK_ERROR"), result]) as dl, \
                patch("app.pipeline.DOWNLOAD_RETRY_DELAYS", (0.01, 0.01)):
            self.assertEqual(self.manager._download(self.job, Mock(), self.settings, file_id, item),
                             (result.path, result.sha256))
        self.assertEqual(dl.call_count, 2)


class LifecycleTests(ManagerFixture):
    def test_a_lifecycle_rule_failure_does_not_stop_collection(self):
        from app.oss_store import OssArchiveError

        class Stop(Exception):
            pass
        archive = Mock()
        archive.ensure_ready.side_effect = OssArchiveError("rule", "OSS_LIFECYCLE_ERROR")
        archive.probe.return_value = {"ok": True}
        self.manager.archive_for_settings = Mock(return_value=archive)
        self.manager._archive_existing_parts = Mock(return_value=0)
        self.manager.client_factory = Mock(return_value=SimpleNamespace(verify_instance=Mock(side_effect=Stop())))
        with self.assertRaises(Stop):
            self.manager._run(self.job, self.settings, CloudCredential("fixture", "fixture"))
        codes = [c.args[2] for c in self.manager._event.call_args_list]
        self.assertIn("OSS_LIFECYCLE_DEFERRED", codes)
        self.assertTrue(self.manager._archive_status["ready"])

    def test_an_access_failure_still_stops_the_job(self):
        from app.oss_store import OssArchiveError
        archive = Mock()
        archive.ensure_ready.side_effect = OssArchiveError("denied", "OSS_ACCESS_DENIED")
        self.manager.archive_for_settings = Mock(return_value=archive)
        with self.assertRaises(OssArchiveError):
            self.manager._run(self.job, self.settings, CloudCredential("fixture", "fixture"))


class RawLaneTests(unittest.TestCase):
    def manager(self):
        manager = SyncManager.__new__(SyncManager)
        manager.metadata = Mock()
        manager.metadata.file_record.return_value = {"state": "downloading"}
        manager._pause_after_current = threading.Event()
        manager._shutdown = threading.Event()
        manager._update_pipeline_status = Mock()
        manager._event = Mock()
        manager.archive_for_settings = Mock(return_value=Mock())
        return manager

    def test_one_failing_file_does_not_stop_the_others(self):
        manager = self.manager()
        pending = [(str(i), SimpleNamespace(file_size=100, log_file_name=str(i)), "discovered") for i in range(6)]

        def download(job, client, settings, file_id, item):
            if file_id == "2":
                raise DownloadError("reset", "NETWORK_ERROR")
            return Path(file_id), "sha"
        manager._download = download
        manager._process_one = Mock(return_value=0)
        result = manager._run_pending_raw("job", None, Settings(), pending, "mysql", Mock(), completed=0, unavailable=0)
        self.assertEqual(result, (5, 0, False))
        self.assertFalse(manager._file_due("2"))

    def test_an_oversized_file_gets_its_own_lane(self):
        from app import pipeline
        manager = self.manager()
        release = threading.Event()
        order = []
        big = (str("big"), SimpleNamespace(file_size=pipeline.RAW_PREFETCH_BYTES + 1, log_file_name="big"), "discovered")
        small = [(str(i), SimpleNamespace(file_size=100, log_file_name=str(i)), "discovered") for i in range(3)]

        def download(job, client, settings, file_id, item):
            if file_id == "big":
                self.assertTrue(release.wait(3), "small files waited behind the oversized one")
            return Path(file_id), "sha"

        def process(job, client, settings, file_id, *args, **kwargs):
            order.append(file_id)
            if len([f for f in order if f != "big"]) == 3:
                release.set()
            return 0
        manager._download = download
        manager._process_one = process
        with self.assertLogs("app.pipeline", "WARNING"):
            result = manager._run_pending_raw("job", None, Settings(), [big, *small], "mysql", Mock(),
                                              completed=0, unavailable=0)
        self.assertEqual(result, (4, 0, False))
        self.assertEqual(order[-1], "big")

    def test_an_archived_original_is_finished_without_downloading_it(self):
        manager = self.manager()
        manager.storage = SimpleNamespace(paths={}, raw_binlogs=SimpleNamespace(get=lambda file_id: {"raw": {}}))
        manager._download = Mock(side_effect=AssertionError("downloaded again"))
        seen = []
        manager._process_one = lambda job, client, settings, file_id, item, prior, *a, **k: seen.append(prior) or 0
        result = manager._run_pending_raw("job", None, Settings(),
                                          [("1", SimpleNamespace(file_size=100, log_file_name="1"), "discovered")],
                                          "mysql", Mock(), completed=0, unavailable=0)
        self.assertEqual(result, (1, 0, False))
        self.assertEqual(seen, ["stored"])

    def test_admission_waits_for_disk_space_instead_of_failing(self):
        manager = self.manager()
        manager._download = Mock()
        manager._process_one = Mock(return_value=0)
        manager._disk_admits = Mock(return_value=False)
        timer = threading.Timer(0.3, manager._pause_after_current.set)
        timer.start()
        self.addCleanup(timer.cancel)
        with self.assertLogs("app.pipeline", "WARNING"):
            result = manager._run_pending_raw("job", None, Settings(),
                                              [("1", SimpleNamespace(file_size=100, log_file_name="1"), "discovered")],
                                              "mysql", Mock(), completed=0, unavailable=0)
        self.assertEqual(result, (0, 0, True))
        manager._download.assert_not_called()


class HousekeepingTests(ManagerFixture):
    def test_expired_unfinished_files_are_settled_but_local_ones_kept(self):
        now = datetime.now(UTC)
        old = binlog("mysql-bin.1", now - timedelta(days=12), now - timedelta(days=12) + timedelta(minutes=1))
        local = binlog("mysql-bin.2", now - timedelta(days=12), now - timedelta(days=12) + timedelta(minutes=2))
        fresh = binlog("mysql-bin.3", now - timedelta(hours=1), now - timedelta(minutes=59))
        ids = [self.metadata.upsert_remote(self.settings, item)[0] for item in (old, local, fresh)]
        (self.root / f"{ids[1]}.binlog.part").write_bytes(b"partial")
        self.manager._settings_loader = lambda: self.settings
        self.manager.scope_instance_id = INSTANCE
        with self.assertLogs("app.pipeline", "WARNING"):
            self.assertEqual(self.manager._settle_expired_sources_if_due(self.settings), 1)
        self.assertEqual(self.metadata.file_record(ids[0])["error_code"], "SOURCE_EXPIRED")
        self.assertEqual([self.metadata.file_record(i)["state"] for i in ids[1:]], ["discovered", "discovered"])

    def test_download_sweep_removes_old_leftovers_only(self):
        now = datetime.now(UTC)
        done = binlog("mysql-bin.1", now - timedelta(hours=9), now - timedelta(hours=8))
        active = binlog("mysql-bin.2", now - timedelta(hours=8), now - timedelta(hours=7))
        done_id, _ = self.metadata.upsert_remote(self.settings, done)
        active_id, _ = self.metadata.upsert_remote(self.settings, active)
        self.metadata.set_file_state(done_id, "done")
        old = time.time() - 30 * 3600
        paths = {name: self.root / name for name in (f"{done_id}.binlog", f"{active_id}.binlog.part",
                                                      f"{done_id}.binlog.corrupt-1", "x.binlog.part.corrupt-2")}
        for path in paths.values():
            path.write_bytes(b"x")
            os.utime(path, (old, old))
        temp = self.root / ".raw-index-abc"
        temp.mkdir()
        os.utime(temp, (old, old))
        result = self.manager._sweep_downloads_if_due()
        self.assertEqual(result, {"forensic": 2, "temp": 1, "orphans": 1})
        self.assertTrue(paths[f"{active_id}.binlog.part"].exists())
        self.assertFalse(paths[f"{done_id}.binlog"].exists())

    def test_guard_reports_lost_sources_expiry_risk_and_stuck_files(self):
        now = datetime.now(UTC)
        self.manager._settings_loader = lambda: self.settings
        self.manager.scope_instance_id = INSTANCE
        near = binlog("mysql-bin.1", now - timedelta(days=6), now - timedelta(days=6) + timedelta(minutes=1))
        lost = binlog("mysql-bin.2", now - timedelta(days=6), now - timedelta(days=6) + timedelta(minutes=2))
        near_id = self.metadata.upsert_remote(self.settings, near)[0]
        lost_id = self.metadata.upsert_remote(self.settings, lost)[0]
        self.metadata.set_file_state(lost_id, "unavailable", error_code="INSTANCE_BINLOG_NOT_FOUND")
        for _ in range(5):
            self.metadata.set_file_state(near_id, "failed", increment_attempt=True)
        guard = self.manager.binlog_guard()
        self.assertFalse(guard["ok"])
        self.assertEqual(guard["issues"], ["lost", "expiry_risk", "stuck", "behind"])
        self.assertLess(guard["expiresInSeconds"], 2 * 86400)

    def test_guard_is_quiet_without_backlog(self):
        self.manager._settings_loader = lambda: self.settings
        self.manager.scope_instance_id = INSTANCE
        self.assertEqual(self.manager.binlog_guard()["state"], "ok")


class WatchdogTests(unittest.TestCase):
    def manager(self, **state):
        base = {"instance": INSTANCE, "running": True, "progressAge": 0.0, "schedulerAge": 0.0,
                "abortRequested": False, "abortAge": 0.0, "autoSync": True}
        base.update(state)
        return SimpleNamespace(watchdog_state=lambda: base, request_abort=Mock())

    def watchdog(self, managers, tmp):
        exits = []
        return SyncWatchdog(lambda: managers, Path(tmp), stall_seconds=100, exit_grace_seconds=50,
                            scheduler_stall_seconds=300, exit_fn=exits.append), exits

    def test_a_stalled_job_is_aborted_then_the_process_restarted(self):
        with tempfile.TemporaryDirectory() as tmp:
            stalled = self.manager(progressAge=101)
            dog, exits = self.watchdog([stalled], tmp)
            with self.assertLogs("app.sync_watchdog", "ERROR"):
                self.assertEqual(dog.check(), "abort")
            stalled.request_abort.assert_called_once()
            stuck = self.manager(progressAge=500, abortRequested=True, abortAge=51)
            dog, exits = self.watchdog([stuck], tmp)
            with self.assertLogs("app.sync_watchdog", "ERROR"):
                self.assertEqual(dog.check(), "exit")
            self.assertEqual(exits, [75])
            self.assertTrue(list(Path(tmp).glob("sync-stall-*.txt")))

    def test_a_stuck_scheduler_restarts_the_process_and_a_healthy_one_is_left_alone(self):
        with tempfile.TemporaryDirectory() as tmp:
            dog, exits = self.watchdog([self.manager(running=False, schedulerAge=301)], tmp)
            with self.assertLogs("app.sync_watchdog", "ERROR"):
                self.assertEqual(dog.check(), "exit")
            self.assertEqual(exits, [76])
            dog, exits = self.watchdog([self.manager(progressAge=99), self.manager(running=False, autoSync=False,
                                                                                    schedulerAge=9999)], tmp)
            self.assertEqual(dog.check(), "")
            self.assertEqual(exits, [])

    def test_real_manager_state_and_abort(self):
        with tempfile.TemporaryDirectory() as tmp:
            metadata = MetadataStore(Path(tmp) / "metadata.sqlite3")
            manager = SyncManager(metadata, StorageStub(Path(tmp)), start_scheduler=False,
                                  settings_loader=lambda: Settings(db_instance_id=INSTANCE))
            self.addCleanup(manager.shutdown)
            state = manager.watchdog_state()
            self.assertFalse(state["running"])
            with self.assertLogs("app.pipeline", "ERROR"):
                self.assertTrue(manager.request_abort("test"))
            self.assertTrue(manager._abort_requested())
            self.assertFalse(manager.request_abort("again"))
            job = metadata.create_job("sync", INSTANCE)
            manager._finish_stopped_job(job, "paused")
            self.assertEqual(metadata.jobs()[0]["error_code"], "SYNC_STALLED")


class BusyRetryTests(unittest.TestCase):
    def test_locked_database_is_waited_out(self):
        attempts = []

        def write():
            attempts.append(1)
            if len(attempts) < 3:
                raise sqlite3.OperationalError("database is locked")
            return "ok"
        with self.assertLogs("app.metadata", "WARNING"):
            self.assertEqual(retry_busy(write, seconds=5), "ok")
        self.assertEqual(len(attempts), 3)

    def test_other_errors_and_the_deadline_still_raise(self):
        with self.assertRaises(sqlite3.OperationalError):
            retry_busy(Mock(side_effect=sqlite3.OperationalError("no such table: x")), seconds=5)
        with self.assertRaises(sqlite3.OperationalError):
            retry_busy(Mock(side_effect=sqlite3.OperationalError("database is locked")), seconds=0)

    def test_collector_writes_are_wrapped(self):
        for name in ("set_file_state", "upsert_remotes", "update_job", "add_job_event", "create_job",
                     "update_download_progress", "set_file_visibility"):
            self.assertTrue(hasattr(getattr(MetadataStore, name), "__wrapped__"), name)


class RdsRetryTests(unittest.TestCase):
    def client(self):
        return RdsRpcClient(Settings(db_instance_id=INSTANCE), CloudCredential("fixture", "fixture"))

    def test_throttling_is_retried_with_a_fresh_signature(self):
        client = self.client()
        client._call_once = Mock(side_effect=[RdsApiError("slow down", code="Throttling.User"), {"ok": 1}])
        with patch("app.rds_api.time.sleep") as sleep, self.assertLogs("app.rds_api", "WARNING"):
            self.assertEqual(client.call("DescribeBinlogFiles", {}), {"ok": 1})
        sleep.assert_called_once()

    def test_permission_errors_are_not_retried(self):
        client = self.client()
        client._call_once = Mock(side_effect=RdsApiError("denied", code="Forbidden.RAM"))
        with self.assertRaises(RdsApiError):
            client.call("DescribeBinlogFiles", {})
        client._call_once.assert_called_once()

    def test_log_backup_retention_is_read_from_the_backup_policy(self):
        client = self.client()
        client.call = Mock(return_value={"LogBackupRetentionPeriod": "7"})
        self.assertEqual(client.log_backup_retention_days(), 7)
        client.call = Mock(return_value={"LogBackupRetentionPeriod": ""})
        self.assertIsNone(client.log_backup_retention_days())


class CoverageGapTests(unittest.TestCase):
    S = 1_000_000

    def test_time_without_any_registered_file_is_a_gap_not_covered(self):
        files = [{"id": "a", "lo": 0, "hi": 10 * self.S, "state": "done"},
                 {"id": "b", "lo": 50 * self.S, "hi": 60 * self.S, "state": "done"}]
        result = br.coverage_runs(files, {"a", "b"}, 0, 60 * self.S)
        self.assertEqual([(g["start"], g["end"], g["reason"]) for g in result["gaps"]],
                         [(10 * self.S, 50 * self.S, "source_unlisted")])
        self.assertEqual(result["covered_us"], 20 * self.S)
        expired = br.coverage_runs(files, {"a", "b"}, 0, 60 * self.S, expired_before=60 * self.S)
        self.assertEqual(expired["gaps"][0]["reason"], "source_missing")

    def test_a_missing_single_transaction_file_is_reported(self):
        files = [{"id": "a", "lo": 0, "hi": 10 * self.S, "state": "done"},
                 {"id": "big", "lo": 10 * self.S, "hi": 10 * self.S, "state": "done"},
                 {"id": "b", "lo": 10 * self.S, "hi": 20 * self.S, "state": "done"}]
        result = br.coverage_runs(files, {"a", "b"}, 0, 20 * self.S)
        self.assertEqual([(g["start"], g["end"], g["reason"]) for g in result["gaps"]],
                         [(10 * self.S, 11 * self.S, "pending")])
        self.assertIn("未覆盖 1 段", br.coverage_note(result))

    def test_a_replica_copy_covers_and_settled_copies_are_no_gap(self):
        files = [{"id": "a", "lo": 0, "hi": 10 * self.S, "state": "done", "host": "old"},
                 {"id": "dup", "lo": 2 * self.S, "hi": 8 * self.S, "state": "discovered", "host": "new"},
                 {"id": "skip", "lo": 8 * self.S, "hi": 9 * self.S, "state": "skipped", "host": "new"}]
        result = br.coverage_runs(files, {"a"}, 0, 10 * self.S)
        self.assertEqual(result["gaps"], [])
        self.assertEqual(result["intervals"], [[0, 10 * self.S]])

    def test_collecting_and_pending_are_told_apart_and_the_head_is_no_data(self):
        files = [{"id": "a", "lo": 10 * self.S, "hi": 20 * self.S, "state": "downloading"},
                 {"id": "b", "lo": 20 * self.S, "hi": 30 * self.S, "state": "done"}]
        result = br.coverage_runs(files, set(), 0, 30 * self.S)
        self.assertEqual([g["reason"] for g in result["gaps"]], ["no_data", "collecting", "pending"])


class LightweightIndexFallbackTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.metadata = MetadataStore(self.root / "metadata.sqlite3")
        from app.raw_binlog import RawBinlogStore
        self.store = RawBinlogStore(self.metadata)
        self.item = binlog("mysql-bin.000001", T0, at(1))
        self.file_id, _ = self.metadata.upsert_remote(Settings(db_instance_id=INSTANCE), self.item)
        self.metadata.set_file_state(self.file_id, "downloaded", local_sha256="a" * 64)
        self.path = self.root / "source.binlog"
        self.path.write_bytes(b"\xfebin" + b"x" * 100)

    def archive(self, **kwargs):
        archive = Mock()
        archive.prefix = "p/"
        with patch.object(self.store, "_put", side_effect=lambda a, path, key, sha, crc, progress=None:
                          dict(oss_key=key, size_bytes=path.stat().st_size, sha256=sha, crc64=str(crc))):
            return self.store.archive(archive, self.path, self.file_id, self.item, "mysql", **kwargs)

    def test_a_scan_timeout_still_archives_the_original(self):
        with patch("app.raw_binlog.scan", side_effect=RawBinlogError("slow", "RAW_INDEX_TIMEOUT")), \
                self.assertLogs("app.raw_binlog", "WARNING"):
            descriptor = self.archive()
        self.assertIsNotNone(self.store.get(self.file_id))
        with self.metadata.connection() as conn:
            summary = json.loads(conn.execute(
                "SELECT summary FROM raw_binlog_archives WHERE file_id = ?", (self.file_id,)).fetchone()[0])
        self.assertTrue(summary["unknown"])
        self.assertIn("scan-raw_index_timeout", summary["uncertainty"])
        self.assertEqual(descriptor["raw"]["sha256"], "a" * 64)

    def test_a_file_that_is_no_binlog_still_fails(self):
        with patch("app.raw_binlog.scan", side_effect=RawBinlogError("encrypted", "RAW_BINLOG_FORMAT_UNSUPPORTED")):
            with self.assertRaises(RawBinlogError):
                self.archive()
        self.assertIsNone(self.store.get(self.file_id))

    def test_preserve_first_skips_the_scan(self):
        with patch("app.raw_binlog.scan", side_effect=AssertionError("scanned")), \
                self.assertLogs("app.raw_binlog", "WARNING"):
            self.archive(full_index=False)
        self.assertIsNotNone(self.store.get(self.file_id))

    def test_scan_timeout_grows_with_the_file(self):
        from app.binlog_lite import scan_timeout
        self.assertEqual(scan_timeout(0), 180)
        self.assertEqual(scan_timeout(23 * 1024**3), 180 + 60 * 23)


class RowsWorkerRetryTests(unittest.TestCase):
    def worker(self):
        from app import binlog_rows_worker as worker_module
        worker = worker_module.Worker.__new__(worker_module.Worker)
        return worker

    def test_failures_back_off_and_never_drop_the_file_for_good(self):
        worker = self.worker()
        entry = {"file_id": "f" * 32, "source_file_name": "mysql-bin.1"}
        failures = {}
        delays = []
        with self.assertLogs("binlog_rows_worker", "ERROR"):
            for _ in range(6):
                delays.append(worker.failed(entry, failures, RuntimeError("too many parts")))
        self.assertEqual(delays[:3], [30.0, 60.0, 120.0])
        self.assertFalse(worker.due(entry["file_id"]))
        self.assertEqual(worker.stuck_files()[0]["failures"], 6)
        worker.retry_at[entry["file_id"]] = (6, time.monotonic() - 1, "mysql-bin.1")
        self.assertTrue(worker.due(entry["file_id"]))
        worker.succeeded(entry["file_id"])
        self.assertEqual(worker.stuck_files(), [])

    def test_buffers_accept_a_very_large_file(self):
        statements = br.worker_buffer_schema()
        self.assertTrue(any(s.startswith("ALTER TABLE") and "parts_to_throw_insert" in s for s in statements))
        self.assertTrue(all("parts_to_delay_insert" in s for s in statements))
        self.assertEqual(br.RowsIngestor.LOAD_SETTINGS["max_insert_block_size"], 65536)


if __name__ == "__main__":
    unittest.main()
