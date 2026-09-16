from __future__ import annotations

import contextlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from app.config import Settings
from app.query_tasks import QueryCancelled, QueryControl
from app.search_index import INDEX_SCHEMA_VERSION, SearchIndex
from app.storage import EventStorage


class SearchIndexCancellationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.index = SearchIndex(Path(self.temp.name) / "search.sqlite3")
        self.control = QueryControl("test", Mock())
        self.part = {"path": "part.parquet", "sha256": "sha"}
        with self.index.connection() as conn:
            conn.execute(
                "INSERT INTO indexed_parts VALUES(?, ?, '', 1, 1, ?, '')",
                (self.part["path"], self.part["sha256"], INDEX_SCHEMA_VERSION),
            )
            conn.execute(
                "INSERT INTO blocks(part_path, part_sha256, row_group_id, "
                "min_event_epoch_us, max_event_epoch_us, row_count, "
                "databases_json, tables_json, operations_json) "
                "VALUES(?, ?, 0, 1, 10, 1, '[]', '[]', '[]')",
                (self.part["path"], self.part["sha256"]),
            )

    def plan(self, query=None, control=None):
        return self.index.candidate_blocks(
            [self.part], query or {}, start_epoch_us=1, end_epoch_us=10,
            control=control,
        )

    def test_cancelled_before_planning_does_not_read_coverage(self):
        self.control.cancel()
        with patch.object(self.index, "_coverage", side_effect=AssertionError("read")):
            with self.assertRaises(QueryCancelled):
                self.plan(control=self.control)

    def test_controlled_and_uncontrolled_plans_are_identical(self):
        self.assertEqual(self.plan(), self.plan(control=self.control))

    def test_sql_interrupt_preserves_cancellation_and_connection_cleanup(self):
        sentinel = QueryCancelled("cancel recursive SQL")
        calls = 0

        def check():
            nonlocal calls
            calls += 1
            if calls >= 4:
                raise sentinel

        self.control.check_cancelled = check
        with self.assertRaises(QueryCancelled) as raised:
            with self.index.connection(control=self.control) as conn:
                conn.execute(
                    "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL "
                    "SELECT x+1 FROM n WHERE x<1000000) SELECT sum(x) FROM n"
                ).fetchone()
        self.assertIs(raised.exception, sentinel)
        with self.assertRaises(sqlite3.ProgrammingError):
            conn.execute("SELECT 1")
        with self.index.connection() as fresh:
            self.assertEqual(fresh.execute("SELECT 42").fetchone()[0], 42)

    def test_unrelated_sql_error_not_translated_to_cancellation(self):
        with self.assertRaisesRegex(sqlite3.OperationalError, "no such table"):
            with self.index.connection(control=self.control) as conn:
                conn.execute("SELECT * FROM absent_table")

    def test_fts_execution_can_be_interrupted_after_coverage(self):
        def token_query(conn, term):
            self.control.cancel()
            conn.execute(
                "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL "
                "SELECT x+1 FROM n WHERE x<1000000) SELECT sum(x) FROM n"
            ).fetchone()
            self.fail("cancel was not observed during SQL")

        with patch.object(self.index, "_token_ids", side_effect=token_query):
            with self.assertRaises(QueryCancelled):
                self.plan({"keyword": "user_id"}, self.control)

    def test_cancel_between_coverage_pages(self):
        parts = [self.part] + [
            {"path": f"unknown-{n}.parquet", "sha256": "sha"} for n in range(450)
        ]
        original = self.index._identity_matches

        def match(*args):
            self.control.cancel()
            return original(*args)

        with patch.object(self.index, "_identity_matches", side_effect=match):
            with self.assertRaises(QueryCancelled):
                self.index._coverage(parts, control=self.control)

    def test_python_candidate_filter_checks_cancellation(self):
        original = self.index._identity_matches
        calls = 0

        def match(*args):
            nonlocal calls
            calls += 1
            # First match is coverage; second is candidate filtering.
            if calls == 2:
                self.control.cancel()
            return original(*args)

        with patch.object(self.index, "_identity_matches", side_effect=match):
            with self.assertRaises(QueryCancelled):
                self.plan(control=self.control)

    def test_storage_passes_control_to_candidate_planner(self):
        storage = EventStorage.__new__(EventStorage)
        storage.metadata = Mock()
        storage.metadata.storage_metadata_stats.return_value = {}
        storage.metadata.complete_query_certificate.return_value = ({}, None)
        storage.metadata.parts_in_range.return_value = [self.part]
        storage.clickhouse_backend = None
        storage._query_window = Mock(return_value=(1, 10))
        storage.search_index = Mock()
        storage.search_index.candidate_blocks.side_effect = QueryCancelled("stop")
        with self.assertRaises(QueryCancelled):
            storage._query_events_tiered_impl(
                {"start_epoch_us": 1, "end_epoch_us": 10}, Settings(), None,
                control=self.control,
            )
        self.assertIs(
            storage.search_index.candidate_blocks.call_args.kwargs.get("control"),
            self.control,
        )


class SearchIndexScopeTests(unittest.TestCase):
    setUp = SearchIndexCancellationTests.setUp

    def seed_parts(self):
        parts = [
            {"path": f"p-{n:04d}.parquet", "sha256": "sha"}
            for n in range(405)
        ]
        structural = {"path": "structural.parquet", "sha256": "sha"}
        stale = {"path": "stale.parquet", "sha256": "sha"}
        unknown = {"path": "unknown.parquet", "sha256": "sha"}
        with self.index.connection() as conn:
            for part in parts + [stale, structural]:
                table = "structural_parts" if part == structural else "indexed_parts"
                version = INDEX_SCHEMA_VERSION - (part == stale)
                conn.execute(
                    f"INSERT INTO {table} VALUES(?, ?, '', 2, 2, ?, '')",
                    (part["path"], part["sha256"], version),
                )
            for part in parts + [stale, structural, unknown, {"path": "foreign"}]:
                for group in range(2):
                    conn.execute(
                        "INSERT INTO blocks(part_path, part_sha256, row_group_id, "
                        "min_event_epoch_us, max_event_epoch_us, row_count, "
                        "databases_json, tables_json, operations_json) "
                        "VALUES(?, 'sha', ?, ?, ?, 1, '[]', '[]', ?)",
                        (part["path"], group, group * 5, group * 5 + 5,
                         json.dumps(["UPDATE" if group else "DELETE"])),
                    )
            # A stale block under an otherwise covered path must still fail
            # the identity check, even though it passes the new path prefilter.
            conn.execute(
                "INSERT INTO blocks(part_path, part_sha256, row_group_id, "
                "min_event_epoch_us, max_event_epoch_us, row_count, "
                "databases_json, tables_json, operations_json) "
                "VALUES(?, 'old-sha', 2, 1, 10, 1, '[]', '[]', '[\"UPDATE\"]')",
                (parts[0]["path"],),
            )
        return parts + [structural, stale, unknown]

    @contextlib.contextmanager
    def capture_block_reads(self, reads, control=None):
        original = self.index.connection

        @contextlib.contextmanager
        def connection(**kwargs):
            with original(**kwargs) as conn:
                def trace(sql):
                    if sql.startswith("SELECT * FROM blocks"):
                        reads.append(sql)
                        if control is not None:
                            control.cancel()
                conn.set_trace_callback(trace)
                yield conn

        with patch.object(self.index, "connection", side_effect=connection):
            yield

    def test_path_batches_equal_legacy_global_scan_including_ties_and_coverage(self):
        parts = self.seed_parts()
        part_map = {p["path"]: p for p in parts}
        covered = set(part_map) - {"stale.parquet", "unknown.parquet"}
        with self.index.connection() as conn:
            legacy_rows = conn.execute(
                "SELECT * FROM blocks WHERE max_event_epoch_us >= 3 "
                "AND min_event_epoch_us <= 7 "
                "ORDER BY max_event_epoch_us DESC, min_event_epoch_us DESC, id DESC"
            ).fetchall()
        for operations in ([], ["UPDATE"], ["INSERT"]):
            with self.subTest(operations=operations):
                expected = []
                for row in legacy_rows:
                    path = row["part_path"]
                    if path not in covered or row["part_sha256"] != "sha":
                        continue
                    if operations and set(operations).isdisjoint(
                        json.loads(row["operations_json"])
                    ):
                        continue
                    expected.append({
                        "path": path, "part": part_map[path],
                        "row_group_id": row["row_group_id"],
                        "min_event_epoch_us": row["min_event_epoch_us"],
                        "max_event_epoch_us": row["max_event_epoch_us"],
                        "complete": path != "structural.parquet",
                    })
                reads = []
                with self.capture_block_reads(reads):
                    plan = self.index.candidate_blocks(
                        list(reversed(parts)), {"operations": operations},
                        start_epoch_us=3, end_epoch_us=7,
                    )
                self.assertEqual(plan["entries"], expected)
                self.assertEqual(plan["covered_paths"], covered)
                self.assertEqual(plan["structural_covered_paths"], {"structural.parquet"})
                self.assertEqual(plan["full_covered_paths"], covered - {"structural.parquet"})
                self.assertEqual(plan["unknown_paths"], {"stale.parquet", "unknown.parquet"})
                self.assertEqual(plan["skipped_parts"], len(covered - {e["path"] for e in expected}))
                self.assertEqual(len(reads), 2)
                self.assertTrue(all("WHERE part_path IN (" in sql for sql in reads))
                self.assertTrue(all("'foreign'" not in sql for sql in reads))

    def test_cancellation_stops_before_next_block_path_batch(self):
        parts = self.seed_parts()
        reads = []
        with self.capture_block_reads(reads, self.control):
            with self.assertRaises(QueryCancelled):
                self.index.candidate_blocks(
                    parts, {}, start_epoch_us=1, end_epoch_us=10,
                    control=self.control,
                )
        self.assertEqual(len(reads), 1)

    def test_unknown_only_does_not_read_blocks_or_claim_coverage(self):
        reads = []
        with self.capture_block_reads(reads):
            plan = self.index.candidate_blocks(
                [{"path": self.part["path"], "sha256": "changed-sha"}], {},
                start_epoch_us=1, end_epoch_us=10,
            )
        self.assertEqual(reads, [])
        self.assertEqual(plan["entries"], [])
        self.assertEqual(plan["covered_paths"], set())
        self.assertEqual(plan["unknown_paths"], {self.part["path"]})


if __name__ == "__main__":
    unittest.main()
