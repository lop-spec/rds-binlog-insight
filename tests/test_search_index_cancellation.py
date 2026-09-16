from __future__ import annotations

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


if __name__ == "__main__":
    unittest.main()
