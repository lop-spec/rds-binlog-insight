import json
import re
import unittest
from unittest import mock

from app import binlog_rows as br
from app.binlog_lite import RawBinlogError


START = 1_789_017_442_000_000
END = 1_789_967_842_000_000


def scoped(**extra):
    query = {"instance": "rm-1", "database": "shop", "table": "customer_profile", "source": "binlog"}
    query.update(extra)
    return query


class KeywordTests(unittest.TestCase):
    def test_pure_token_uses_text_index_without_substring(self):
        params = {}
        clause = br.keyword_conditions({"keyword": "157683"}, params)
        self.assertEqual(clause, f"(hasToken({br.SEARCH_EXPR}, {{kw0:String}}))")
        self.assertEqual(params, {"kw0": "157683"})
        self.assertFalse(br.keyword_uses_scan({"keyword": "157683"}))

    def test_separated_ascii_term_prefilters_tokens_then_checks_exact_sequence(self):
        params = {}
        clause = br.keyword_conditions({"keyword": "Shop_ID"}, params)
        self.assertIn("hasAllTokens", clause)
        self.assertIn("position(", clause)
        self.assertEqual(params["kw0"], "shop_id")
        self.assertEqual(params["kw0t"], "['shop','id']")

    def test_non_ascii_term_is_a_bounded_scan(self):
        params = {}
        clause = br.keyword_conditions({"keyword": "虫草花"}, params)
        self.assertEqual(clause, f"(position({br.SEARCH_EXPR}, {{kw0:String}}) > 0)")
        self.assertTrue(br.keyword_uses_scan({"keyword": "虫草花"}))

    def test_or_mode_and_term_limit(self):
        params = {}
        clause = br.keyword_conditions({"keyword": " ".join(f"t{i}" for i in range(30)), "keyword_mode": "or"}, params)
        self.assertEqual(clause.count(" OR "), 19)
        self.assertEqual(len([k for k in params if k.startswith("kw")]), 20)

    def test_user_text_never_reaches_sql(self):
        hostile = "x'); DROP TABLE t; --"
        sql, params, _, _ = br.build_query(scoped(keyword=hostile, transaction=hostile), START, END)
        self.assertNotIn("DROP TABLE", sql)
        self.assertIn(hostile.lower().split()[0], params.values())

    def test_array_literal_escaping(self):
        self.assertEqual(br.ch_array(["a'b", "c\\d"]), "['a\\'b','c\\\\d']")


class BuildQueryTests(unittest.TestCase):
    def test_requires_instance_database_and_table(self):
        for missing in ("instance", "database", "table"):
            query = scoped()
            query[missing] = ""
            with self.assertRaises(RawBinlogError) as ctx:
                br.build_query(query, START, END)
            self.assertEqual(ctx.exception.code, "INDEX_QUERY_SCOPE_REQUIRED")

    def test_prunes_by_bucket_table_date_and_time(self):
        sql, params, limit, offset = br.build_query(scoped(limit=50, offset=100, operations=["update", "DELETE"]), START, END)
        for fragment in ("instance_id = {instance:String}", "tbl_bucket = cityHash64(lower({db:String}), lower({tbl:String})) % 8",
                         "database_name = {db:String}", "table_name = {tbl:String}",
                         "event_date BETWEEN toDate({d0:String}) AND toDate({d1:String})",
                         "event_epoch_us BETWEEN {lo:Int64} AND {hi:Int64}", "has({ops:Array(String)}, operation)",
                         "ORDER BY event_epoch_us DESC", "LIMIT 51 OFFSET 100"):
            self.assertIn(fragment, sql)
        self.assertEqual((limit, offset), (50, 100))
        self.assertEqual(params["ops"], "['UPDATE','DELETE']")
        self.assertEqual((params["d0"], params["d1"]), ("2026-09-10", "2026-09-21"))

    def test_page_depth_limit(self):
        with self.assertRaises(RawBinlogError) as ctx:
            br.build_query(scoped(limit=100, offset=1950), START, END)
        self.assertEqual(ctx.exception.code, "RAW_QUERY_PAGE_LIMIT")

    def test_primary_key_needs_registry_and_matches_raw_or_quoted_value(self):
        with self.assertRaises(RawBinlogError):
            br.build_query(scoped(exact={"kind": "PRIMARY_KEY", "value": "5917"}), START, END)
        sql, params, _, _ = br.build_query(scoped(exact={"kind": "PRIMARY_KEY", "value": "5917"}), START, END,
                                           row_image_key="@1")
        self.assertIn("JSONExtractRaw(after_json, {pk_key:String})", sql)
        self.assertEqual((params["pk_key"], params["pk_raw"], params["pk_quoted"], params["pk_token"]),
                         ("@1", "5917", '"5917"', "5917"))


class CoverageTests(unittest.TestCase):
    FILES = [
        {"id": "a", "lo": 0, "hi": 10, "state": "done"},
        {"id": "b", "lo": 10, "hi": 20, "state": "done"},
        {"id": "c", "lo": 20, "hi": 30, "state": "unavailable"},
        {"id": "d", "lo": 30, "hi": 40, "state": "unavailable"},
        {"id": "e", "lo": 40, "hi": 50, "state": "done"},
        {"id": "f", "lo": 50, "hi": 60, "state": "done"},
        {"id": "g", "lo": 60, "hi": 70, "state": "done"},
    ]

    def test_consecutive_ingested_files_merge_and_gaps_keep_their_reason(self):
        result = br.coverage_runs(self.FILES, {"a", "b", "e", "g"})
        self.assertEqual(result["intervals"], [[0, 20], [40, 50], [60, 70]])
        self.assertEqual([(g["start"], g["end"], g["reason"], g["files"]) for g in result["gaps"]],
                         [(20, 40, "source_missing", 2), (50, 60, "pending", 1)])

    def test_clipping_and_tail_after_newest_file(self):
        result = br.coverage_runs(self.FILES, {"a", "b", "e", "f", "g"}, 5, 90)
        self.assertEqual(result["intervals"], [[5, 20], [40, 70]])
        self.assertEqual([(g["start"], g["end"], g["reason"]) for g in result["gaps"]],
                         [(20, 40, "source_missing"), (70, 90, "not_collected_yet")])
        self.assertEqual((result["covered_us"], result["requested_us"]), (45, 85))
        self.assertIn("未覆盖 2 段", br.coverage_note(result))

    def test_no_files_is_an_explicit_gap_not_a_silent_zero(self):
        result = br.coverage_runs([], set(), 0, 100)
        self.assertEqual(result["gaps"], [{"start": 0, "end": 100, "reason": "no_data", "files": 0}])
        self.assertEqual(br.coverage_note(br.coverage_runs(self.FILES, set("abcdefg"), 0, 70)), "所选区间已完整入库并检索")


class PresentationTests(unittest.TestCase):
    def test_render_sql_for_each_operation(self):
        base = {"database_name": "shop", "table_name": "t"}
        self.assertEqual(br.render_sql({**base, "operation": "INSERT", "after_json": '{"@1":1,"@2":"x\'y"}'}),
                         "INSERT INTO `shop`.`t` (`@1`, `@2`) VALUES (1, 'x\\'y')")
        self.assertEqual(br.render_sql({**base, "operation": "UPDATE", "before_json": '{"@1":1,"@3":null}',
                                        "after_json": '{"@1":1,"@3":{"a":1}}'}),
                         'UPDATE `shop`.`t` SET `@1` = 1, `@3` = {"a":1} WHERE `@1` = 1 AND `@3` = NULL')
        self.assertEqual(br.render_sql({**base, "operation": "DELETE", "before_json": '{"@1":7}'}),
                         "DELETE FROM `shop`.`t` WHERE `@1` = 7")
        self.assertEqual(br.render_sql({**base, "operation": "OTHER"}), "")

    def test_locator_round_trip_and_rejection(self):
        row = {"instance_id": "rm-1", "database_name": "shop", "table_name": "t:x", "event_epoch_us": 123, "event_id": "e"}
        locator = br.encode_locator(row)
        self.assertTrue(locator.startswith("ch:"))
        self.assertEqual(br.decode_locator(locator), ("rm-1", "shop", "t:x", 123, "e"))
        for bad in ("raw:abc:4", "ch:!!!", "ch:" + "e30"):
            self.assertIsNone(br.decode_locator(bad))

    def test_present_attaches_statement_and_iso_time(self):
        row = {"event_id": "e", "event_epoch_us": 1_789_900_000_000_000, "instance_id": "rm-1", "database_name": "shop",
               "table_name": "t", "operation": "DELETE", "before_json": '{"@1":1}', "after_json": "", "row_query_hash": "42",
               "sql_kind": ""}
        out = br.present(row, {42: "DELETE FROM t WHERE id=1"})
        self.assertEqual(out["row_query"], "DELETE FROM t WHERE id=1")
        self.assertEqual(out["sql_kind"], "PSEUDO")
        self.assertTrue(out["event_time_utc"].endswith("Z"))
        self.assertNotIn("row_query_hash", out)
        self.assertEqual(br.decode_locator(out["event_locator"])[4], "e")


class SchemaTests(unittest.TestCase):
    def test_parser_mapping_covers_every_stage_column_once(self):
        aliases = re.findall(r" AS ([a-z_]+)(?:,|$)", br.parser_select_sql())
        self.assertEqual(tuple(aliases), br.STAGE_COLUMN_NAMES)

    def test_index_expression_equals_query_expression(self):
        rows_ddl = br.build_schema()[0]
        self.assertIn(f"INDEX search {br.SEARCH_EXPR} TYPE text(tokenizer = 'splitByNonAlpha')", rows_ddl)
        self.assertIn("storage_policy = 'binlog_rows'", rows_ddl)
        self.assertIn("PARTITION BY (event_date, tbl_bucket)", rows_ddl)

    def test_event_view_maps_every_stage_column(self):
        view = next(s for s in br.build_schema() if "binlog_rows_mv_events_v1" in s)
        for name in br.STAGE_COLUMN_NAMES:
            if name != "row_query":
                self.assertIn(name, view)
        self.assertIn("% 8) AS tbl_bucket", view)
        self.assertIn(f"leftUTF8(row_query, {br.STATEMENT_MAX_BYTES}))) AS row_query_hash", view)
        lanes = [s for s in br.build_schema() if "binlog_rows_buf_worker_v1_" in s]
        self.assertEqual(len(lanes), br.WORKER_LANES_MAX)


class BackendTests(unittest.TestCase):
    def test_serving_flag_gates_construction(self):
        with mock.patch.dict("os.environ", {"RDS_BINLOG_ROWS_SERVING": "0"}, clear=False):
            self.assertIsNone(br.BinlogRows.from_env(object()))
        with mock.patch.dict("os.environ", {"RDS_BINLOG_ROWS_SERVING": "1", "CLICKHOUSE_USER": ""}, clear=False):
            self.assertIsNone(br.BinlogRows.from_env(object()))
        with mock.patch.dict("os.environ", {"RDS_BINLOG_ROWS_SERVING": "1", "CLICKHOUSE_USER": "u",
                                            "CLICKHOUSE_PASSWORD": "p"}, clear=False):
            self.assertIsInstance(br.BinlogRows.from_env(object()), br.BinlogRows)

    def test_query_reports_gaps_and_attaches_statements(self):
        backend = br.BinlogRows(object(), mock.Mock())
        backend.coverage = mock.Mock(return_value={"intervals": [[START, END - 10]], "covered_us": END - 10 - START,
                                                    "requested_us": END - START,
                                                    "gaps": [{"start": END - 10, "end": END, "reason": "pending", "files": 1}]})
        row = {"event_id": "e", "event_epoch_us": START + 5, "instance_id": "rm-1", "database_name": "shop",
               "table_name": "customer_profile", "operation": "UPDATE", "before_json": "{}", "after_json": "{}",
               "row_query_hash": 9, "sql_kind": "PSEUDO"}
        backend.ch.rows.side_effect = [[row, dict(row, event_id="f")], [{"row_query_hash": "9", "row_query": "UPDATE x"}]]
        result = backend.query(scoped(limit=1), START, END)
        self.assertEqual(result["tiers_used"], [br.TIER])
        self.assertTrue(result["has_more"])
        self.assertFalse(result["exact_index_complete"])
        self.assertEqual(result["coverage_gap_count"], 1)
        self.assertEqual(result["rows"][0]["row_query"], "UPDATE x")
        sql = backend.ch.rows.call_args_list[0].args[0]
        self.assertIn("FROM insight.binlog_rows_v1", sql)

    def test_query_timeout_is_explicit(self):
        backend = br.BinlogRows(object(), mock.Mock())
        backend.coverage = mock.Mock(return_value={"intervals": [], "gaps": [], "covered_us": 0, "requested_us": 1})
        backend.ch.rows.side_effect = RawBinlogError("Code: 159. TIMEOUT_EXCEEDED", "CLICKHOUSE_BINLOG_ROWS_UNAVAILABLE")
        with self.assertRaises(RawBinlogError) as ctx:
            backend.query(scoped(keyword="虫草花"), START, END)
        self.assertEqual(ctx.exception.code, "QUERY_DEADLINE_EXCEEDED")
        self.assertIn("中文", str(ctx.exception))


class StorageRoutingTests(unittest.TestCase):
    def test_indexed_binlog_query_uses_rows_backend_when_enabled(self):
        try:
            from app.storage import EventStorage
        except ImportError as exc:  # heavy deps only exist in the image
            self.skipTest(f"storage deps unavailable: {exc}")
        storage = EventStorage.__new__(EventStorage)
        storage._query_window = lambda query, days: (START, END)
        storage.query_activity = mock.MagicMock()
        storage.binlog_rows = mock.Mock()
        storage.binlog_rows.query.return_value = {"rows": [], "tiers_used": [br.TIER]}
        storage.raw_event_index = mock.Mock()
        settings = mock.Mock(retention_days=60)
        result = storage.query_events_tiered(scoped(indexed_only=True, start_epoch_us=START, end_epoch_us=END),
                                             settings, None)
        self.assertEqual(result["tiers_used"], [br.TIER])
        storage.raw_event_index.query.assert_not_called()
        storage.binlog_rows = None
        storage.raw_event_index.query.return_value = {"rows": [], "tiers_used": ["raw-event-index"]}
        storage.query_events_tiered(scoped(indexed_only=True, start_epoch_us=START, end_epoch_us=END), settings, None)
        storage.raw_event_index.query.assert_called_once()


if __name__ == "__main__":
    unittest.main()


class DownloadTests(unittest.TestCase):
    def test_parallel_ranges_reassemble_exact_bytes_and_reject_short_reads(self):
        import tempfile
        import threading
        from pathlib import Path
        from app import binlog_rows_worker as worker

        payload = bytes(range(256)) * 997

        class Body:
            def __init__(self, data):
                self.data = data

            def read(self, size=-1):
                chunk, self.data = self.data[:size], self.data[size:]
                return chunk

            def close(self):
                pass

        class Bucket:
            def __init__(self, short=False):
                self.short = short

            def get_object(self, key, byte_range):
                start, end = byte_range
                data = payload[start:end + 1]
                return Body(data[:-1] if self.short and start == 0 else data)

        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "f.bin"
            worker.download_ranges(Bucket(), "k", len(payload), target, threading.Event())
            self.assertEqual(target.read_bytes(), payload)
            with self.assertRaises(RawBinlogError):
                worker.download_ranges(Bucket(short=True), "k", len(payload), target, threading.Event())


class StreamCountTests(unittest.TestCase):
    def test_counts_records_across_reads_and_missing_final_newline(self):
        import io
        import threading
        from app.binlog_rows_worker import LineCountingStream

        for payload, expected in ((b'{"a":1}\n{"a":2}\n', 2), (b'{"a":1}\n{"a":2}', 2), (b"", 0)):
            stream = LineCountingStream(io.BytesIO(payload), threading.Event())
            while stream.read(3):
                pass
            self.assertEqual(stream.lines, expected)
