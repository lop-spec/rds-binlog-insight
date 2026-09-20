from __future__ import annotations

import tempfile
import unittest
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from app.config import Settings
from app.metadata import MetadataStore
from app.rds_api import RemoteBinlog
from app.sync_metrics import estimate_sync_performance

NOW = datetime(2026, 9, 20, 12, tzinfo=UTC)


def row(index=0, **changes):
    result = dict(host_instance_id="host-a", log_file_name=f"mysql-bin.{index:06d}",
                  completed_at="2026-09-20T11:00:00Z", processing_seconds=50,
                  log_end_utc="2026-09-20T11:00:00Z", file_size=100,
                  remote_status="Completed")
    result.update(changes)
    return result


def estimate(completions=None, sources=None, **changes):
    args = dict(completion_rows=completions if completions is not None else [row(i) for i in range(4)],
                source_rows=sources or [], known_remaining_files=8, running=True,
                host_instance_id="host-a", now=NOW)
    args.update(changes)
    return estimate_sync_performance(**args)


class SyncMetricTests(unittest.TestCase):
    def test_same_window_counts_overlap_and_idle_not_inverse_duration(self):
        value = estimate()
        self.assertEqual(value["seconds_per_file"], 50)
        self.assertEqual(value["processing_files_per_hour"], 2)
        self.assertEqual(value["processing_bytes_per_hour"], 200)
        self.assertEqual(value["inventory_remaining_seconds"], 14400)
        self.assertIsNone(value["estimated_net_remaining_seconds"])
        self.assertEqual(value["continuous_state"], "unknown")

    def test_hosts_sources_and_bytes_are_never_mixed(self):
        extra = [row(i, host_instance_id="host-b", file_size=10000) for i in range(8)]
        extra += [row(i, host_instance_id="slow-log", log_file_name=f"slow-log/{i}") for i in range(8)]
        extra += [row(i, log_file_name=f"tabularis-audit-{i}") for i in range(8)]
        completions = [row(i) for i in range(4)] + extra
        sources = [row(i, file_size=200) for i in range(6)] + extra
        value = estimate(completions, sources)
        self.assertEqual(value["processing_files_per_hour"], 2)
        self.assertEqual(value["source_files_per_hour"], 3)
        self.assertEqual(value["processing_bytes_per_hour"], 200)
        self.assertEqual(value["source_bytes_per_hour"], 600)
        self.assertEqual(len(value["groups"]), 4)
        self.assertEqual(value["continuous_state"], "not_catching_up")
        self.assertEqual(value["state"], "available")  # inventory ETA, not net ETA

    def test_future_stale_lower_boundary_and_noncompleted_sources_excluded(self):
        excluded = [row(completed_at=t, log_end_utc=t) for t in (
            "2026-09-20T09:59:59Z", "2026-09-20T10:00:00Z", "2026-09-20T12:00:01Z")]
        included = [row(completed_at=t, log_end_utc=t) for t in (
            "2026-09-20T10:00:00.001Z", "2026-09-20T12:00:00Z", "2026-09-20T20:00:00+08:00")]
        value = estimate(excluded + included, excluded + included + [row(remote_status="Writing")])
        self.assertEqual(value["completion_sample_size"], 3)
        self.assertEqual(value["source_sample_size"], 3)
        self.assertEqual(value["state"], "warming_up")

    def test_truncation_is_not_a_complete_throughput_window(self):
        value = estimate(sources=[row()], completion_truncated=True, source_truncated=True)
        self.assertIsNone(value["processing_files_per_hour"])
        self.assertIsNone(value["inventory_remaining_seconds"])
        self.assertEqual(value["processing_rate_reason"], "sample_limit")
        self.assertEqual(value["source_files_per_hour"], 0.5)  # explicit lower bound
        self.assertEqual(value["source_rate_reason"], "sample_limit_observed_lower_bound")
        self.assertFalse(value["source_inventory_complete"])

    def test_missing_or_multiple_hosts_never_infer_inventory_capacity(self):
        one = estimate(host_instance_id=None)
        self.assertEqual(one["processing_files_per_hour"], 2)
        self.assertFalse(one["host_authoritative"])
        self.assertIsNone(one["inventory_remaining_seconds"])
        many = estimate([row(), row(host_instance_id="host-b")], host_instance_id=None)
        self.assertIsNone(many["processing_files_per_hour"])
        self.assertEqual(many["processing_rate_reason"], "host_sample_unavailable")

    def test_failed_unavailable_files_are_not_cleared_or_given_eta(self):
        value = estimate(known_remaining_files=0, failed_files=2, running=False)
        self.assertEqual(value["state"], "blocked")
        self.assertIsNone(value["inventory_remaining_seconds"])
        value = estimate(known_remaining_files=1, active_files=1)
        self.assertEqual(value["state"], "live_following")
        self.assertEqual(value["estimated_backlog_files"], 1)
        self.assertIsNone(value["inventory_remaining_seconds"])

    def test_no_projection_from_paused_or_unverified_job(self):
        self.assertIsNone(estimate(running=False)["inventory_remaining_seconds"])
        value = estimate(running=False, known_remaining_files=0, workload_ready=False)
        self.assertEqual(value["state"], "warming_up")

    def test_zero_duration_done_files_still_count_as_completions(self):
        value = estimate([row(i, processing_seconds=d) for i, d in enumerate([0, -1, "bad", float("nan")])])
        self.assertEqual(value["processing_files_per_hour"], 2)
        self.assertIsNone(value["seconds_per_file"])

    def test_invalid_timestamps_refuse_rate_with_reason(self):
        value = estimate([row(completed_at="bad")], [row(log_end_utc="bad")])
        self.assertIsNone(value["processing_files_per_hour"])
        self.assertIsNone(value["source_files_per_hour"])
        self.assertEqual(value["processing_rate_reason"], "invalid_timestamp")
        self.assertEqual(value["source_rate_reason"], "invalid_timestamp")

    def test_faster_than_observed_source_does_not_claim_positive_net_eta(self):
        value = estimate(sources=[row()])
        self.assertGreater(value["processing_files_per_hour"], value["source_files_per_hour"])
        self.assertEqual(value["continuous_state"], "unknown")
        self.assertIsNone(value["estimated_net_remaining_seconds"])
        self.assertIsNone(value["estimated_remaining_seconds"])
        self.assertEqual(value["estimated_catch_up_at_utc"], "")


class SyncMetricDatabaseTests(unittest.TestCase):
    def test_queries_are_bounded_indexed_instance_scoped_and_one_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            store = MetadataStore(Path(directory) / "metadata.sqlite3")
            for instance in ("rm-fixture-a", "rm-fixture-b"):
                for i in range(8):
                    begin = (NOW - timedelta(minutes=20 + i)).isoformat().replace("+00:00", "Z")
                    item = RemoteBinlog(log_file_name=f"mysql-bin.{i:06d}", log_begin_utc=begin,
                                        log_end_utc=begin, file_size=100, checksum_crc64="", download_link="",
                                        intranet_download_link="", link_expired_utc="", remote_status="Completed",
                                        host_instance_id="host-a")
                    file_id, _ = store.upsert_remote(Settings(db_instance_id=instance), item)
                    with patch("app.metadata.utc_now_text", return_value=begin):
                        store.set_file_state(file_id, "done")
            job = dict(instance_id="rm-fixture-a", status="running", total_files=20, completed_files=8)
            statements = []
            plans = []
            original = store.connection

            @contextmanager
            def traced():
                with original() as conn:
                    conn.set_trace_callback(statements.append)
                    yield conn
                    conn.set_trace_callback(None)
                    for query in statements:
                        if query.strip().startswith("SELECT"):
                            plans.extend(str(r[3]) for r in conn.execute("EXPLAIN QUERY PLAN " + query))

            with patch.object(store, "connection", traced):
                result = store.sync_performance(job, host_instance_id="host-a", now=NOW)
            self.assertEqual(result["completion_sample_size"], 8)
            self.assertEqual(result["source_sample_size"], 8)
            self.assertEqual(result["processing_files_per_hour"], 4)
            self.assertEqual(sum(s.strip().startswith("SELECT") for s in statements), 2)
            self.assertIn("BEGIN", statements)
            self.assertTrue(any("idx_binlog_completion" in plan for plan in plans), plans)
            self.assertTrue(any("idx_binlog_order" in plan for plan in plans), plans)
            self.assertFalse(any("TEMP B-TREE" in plan or "SCAN binlog_files" in plan for plan in plans), plans)
            with self.assertLogs("app.metadata", level="WARNING"):
                capped = store.sync_performance(job, now=NOW, completion_limit=4, source_limit=4)
            self.assertIsNone(capped["processing_files_per_hour"])
            self.assertEqual(capped["completion_sample_size"], 4)


if __name__ == "__main__":
    unittest.main()
