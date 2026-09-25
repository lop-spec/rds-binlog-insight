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
        self.assertEqual(clause, f"(position({br.RAW_EXPR}, {{kw0:String}}) > 0)")
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
                         "ORDER BY event_epoch_us DESC"):
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
        lanes = [s for s in br.build_schema() if "binlog_rows_buf_worker_v2_" in s]
        self.assertEqual(len(lanes), br.WORKER_LANES_MAX)
        for statement in lanes:
            self.assertIn(f"ORDER BY ({br.BUCKET_EXPR})", statement)


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


class Release1291Tests(unittest.TestCase):
    def test_boundary_touching_gaps_are_not_reported(self):
        files = [{"id": "p1", "lo": 0, "hi": 10, "state": "done"}, {"id": "a", "lo": 10, "hi": 20, "state": "done"},
                 {"id": "p2", "lo": 20, "hi": 30, "state": "done"}]
        result = br.coverage_runs(files, {"a"}, 10, 20)
        self.assertEqual(result["intervals"], [[10, 20]])
        self.assertEqual(result["gaps"], [])
        self.assertEqual(br.coverage_note(result), "所选区间已完整入库并检索")

    def test_move_keeps_only_table_rows_and_ddl(self):
        client = mock.Mock()
        br.RowsIngestor(client).move(["raw:f"], "t")
        sql = client.execute.call_args_list[0].args[0]
        self.assertIn("WHERE (table_name != '' OR operation = 'DDL')", sql)

    def test_move_writes_one_bucket_per_insert(self):
        client = mock.Mock()
        br.RowsIngestor(client).move(["raw:f"], "t")
        statements = [c.args[0] for c in client.execute.call_args_list]
        self.assertEqual(len(statements), br.BUCKETS)
        for bucket, sql in enumerate(statements):
            self.assertTrue(sql.endswith(f"AND {br.BUCKET_EXPR} = {bucket}"), sql)
        self.assertIn(f"toUInt8({br.BUCKET_EXPR}) AS tbl_bucket", "\n".join(br.build_schema()))

    def test_original_sql_text_becomes_row_query(self):
        select = br.parser_select_sql()
        self.assertIn("coalesce(sql_kind, '') = 'ORIGINAL'", select)
        self.assertIn("sql_text Nullable(String)", br.PARSER_INPUT_STRUCTURE)

    def test_worker_runs_the_collector_parser_in_slim_mode(self):
        from pathlib import Path
        from app import binlog_rows_worker as worker

        with mock.patch.dict("os.environ", {"RDS_BINLOG_ROWS_PARSER": ""}), \
                mock.patch("app.parser_bridge.parser_executable", return_value=Path("/app/tools/binlog-parser")):
            self.assertEqual(worker.parser_command(), [str(Path("/app/tools/binlog-parser")), "--slim"])
        with mock.patch.dict("os.environ", {"RDS_BINLOG_ROWS_PARSER": "/opt/parser"}):
            self.assertEqual(worker.parser_command(), ["/opt/parser", "--slim"])


class Release1292Tests(unittest.TestCase):
    def test_index_text_splits_tokens_at_non_ascii_runs(self):
        self.assertEqual(br.SEARCH_EXPR, "replaceRegexpAll(" + br.RAW_EXPR + ", '[^\\\\x00-\\\\x7f]+', ' ')")
        self.assertIn(f"INDEX search {br.SEARCH_EXPR} TYPE text", br.build_schema()[0])

    def test_mixed_cjk_term_uses_token_prefilter_and_exact_sequence(self):
        params = {}
        clause = br.keyword_conditions({"keyword": "订单157683"}, params)
        self.assertIn(f"hasAllTokens({br.SEARCH_EXPR}, {{kw0t:Array(String)}})", clause)
        self.assertIn(f"position({br.RAW_EXPR}, {{kw0:String}}) > 0", clause)
        self.assertEqual(params["kw0t"], "['157683']")

    def test_coverage_ignores_virtual_non_binlog_files(self):
        import sqlite3
        from contextlib import contextmanager

        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        conn.execute("CREATE TABLE binlog_files (id, instance_id, host_instance_id, log_file_name, log_begin_utc, log_end_utc, state)")
        rows = [("a", "i", "100", "mysql-bin.1", "2026-09-20T00:00:00Z", "2026-09-20T00:10:00Z", "done"),
                ("g", "i", "general-log", "general-log/i/1-2", "2026-09-20T00:10:00Z", "2026-09-20T00:20:00Z", "done"),
                ("s", "i", "slow-log", "slow-log/i/n/1-2", "2026-09-20T00:10:00Z", "2026-09-20T00:20:00Z", "done"),
                ("x", "i", "999", "odd/virtual", "2026-09-20T00:10:00Z", "2026-09-20T00:20:00Z", "done")]
        conn.executemany("INSERT INTO binlog_files VALUES (?, ?, ?, ?, ?, ?, ?)", rows)

        class Meta:
            @contextmanager
            def connection(self):
                yield conn

        backend = br.BinlogRows(Meta(), mock.Mock())
        self.assertEqual([f["id"] for f in backend._files("i", None, None)], ["a"])


class Release1293Tests(unittest.TestCase):
    def _worker(self, tmp, manifest):
        from pathlib import Path
        from app import binlog_rows_worker as worker_module

        worker = worker_module.Worker.__new__(worker_module.Worker)
        worker.storage = mock.Mock(paths={"index": Path(tmp)})
        worker.ch = mock.Mock()
        worker.ingested = mock.Mock(return_value=set(manifest))
        return worker

    def test_committed_file_is_never_claimed_again(self):
        from app.binlog_rows_worker import Claims

        claims = Claims()
        self.assertTrue(claims.take("f"))
        self.assertFalse(claims.take("f"))
        claims.finish("f")
        self.assertFalse(claims.take("f"))
        self.assertTrue(claims.take("g"))
        claims.release("g")
        self.assertTrue(claims.take("g"))

    def test_commit_marker_survives_until_manifest_and_recovery_purges_orphans(self):
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmp:
            worker = self._worker(tmp, manifest=[])
            ingestor = mock.Mock()
            entry = {"file_id": "f1", "instance_id": "i", "source_file_name": "mysql-bin.1"}
            ingestor.record.side_effect = RuntimeError("killed before manifest")
            with self.assertRaises(RuntimeError):
                worker.commit(ingestor, entry, ["raw:f1"], 10, "raw", True, 0)
            marker = Path(tmp) / "binlog-rows-inflight" / "f1.json"
            self.assertTrue(marker.exists())
            self.assertEqual(worker.recover_inflight(), 1)
            sql = worker.ch.execute.call_args.args[0]
            self.assertIn("DELETE FROM insight.binlog_rows_v1", sql)
            self.assertEqual(json.loads(worker.ch.execute.call_args.kwargs["params"]["k"]), ["raw:f1"])
            self.assertFalse(marker.exists())

    def test_recovery_keeps_committed_files(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            worker = self._worker(tmp, manifest=["f2"])
            ingestor = mock.Mock()
            worker.commit(ingestor, {"file_id": "f2", "instance_id": "i", "source_file_name": "b"}, ["raw:f2"], 5,
                          "raw", True, 0)
            self.assertEqual(list((worker.inflight_dir()).glob("*.json")), [])
            (worker.inflight_dir() / "f2.json").write_text(json.dumps({"instance_id": "i", "file_id": "f2", "part_keys": ["raw:f2"]}))
            self.assertEqual(worker.recover_inflight(), 0)
            worker.ch.execute.assert_not_called()


class Release1294Tests(unittest.TestCase):
    def test_day_slices_cover_range_newest_first(self):
        day = br.DAY_US
        start, end = 3 * day + 5, 5 * day + 7
        self.assertEqual(br.day_slices(start, end), [(5 * day, 5 * day + 7), (4 * day, 5 * day - 1), (start, 4 * day - 1)])
        self.assertEqual(br.day_slices(10, 20), [(10, 20)])

    def _backend(self, pages):
        backend = br.BinlogRows(object(), mock.Mock())
        backend.coverage = mock.Mock(return_value={"intervals": [], "gaps": [], "covered_us": 0, "requested_us": 1})
        backend.ch.rows.side_effect = pages
        return backend

    def row(self, i):
        return {"event_id": f"e{i}", "event_epoch_us": START + i, "instance_id": "rm-1", "database_name": "shop",
                "table_name": "customer_profile", "operation": "UPDATE", "before_json": "{}", "after_json": "{}",
                "row_query_hash": 0, "sql_kind": "PSEUDO"}

    def test_stops_after_the_first_slice_that_fills_the_page(self):
        backend = self._backend([[self.row(i) for i in range(3)]])
        result = backend.query(scoped(limit=2), START, START + 5 * br.DAY_US)
        self.assertEqual(backend.ch.rows.call_count, 1)
        self.assertIn("LIMIT 3", backend.ch.rows.call_args.args[0])
        self.assertTrue(result["has_more"])
        self.assertEqual([r["event_id"] for r in result["rows"]], ["e0", "e1"])

    def test_offset_spans_slices(self):
        backend = self._backend([[self.row(0)], [self.row(1), self.row(2)], [self.row(3), self.row(4)]])
        result = backend.query(scoped(limit=2, offset=1), START, START + 3 * br.DAY_US)
        self.assertEqual([r["event_id"] for r in result["rows"]], ["e1", "e2"])
        self.assertTrue(result["has_more"])
        limits = [c.args[0].rsplit("LIMIT ", 1)[1] for c in backend.ch.rows.call_args_list]
        self.assertEqual(limits, ["4", "3", "1"])


class Release1296Tests(unittest.TestCase):
    def _worker(self, slice_bytes, ch_bytes=0):
        import tempfile
        from app import binlog_rows_worker as worker

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = f"{tmp.name}/memory.stat"
        if slice_bytes is not None:
            with open(path, "w") as handle:
                handle.write(f"anon {slice_bytes}\nfile {20 * 1024 ** 3}\nshmem 0\n")
        w = worker.Worker.__new__(worker.Worker)
        w.stop = mock.Mock()
        w.stop.is_set.side_effect = [False, True]
        w.lane_specs = [("live", None)]
        w.publish = mock.Mock()
        w.clickhouse_memory = mock.Mock(return_value=ch_bytes)
        return w, path

    def test_slice_anon_above_the_line_pauses(self):
        w, path = self._worker(9 * 1024 ** 3)
        env = {"RDS_BINLOG_ROWS_SLICE_STAT_FILE": path, "RDS_BINLOG_ROWS_SLICE_ANON_MAX_GIB": "8"}
        with mock.patch.dict("os.environ", env), self.assertLogs("binlog_rows_worker", level="WARNING") as logs:
            self.assertFalse(w.wait_for_capacity(0))
        self.assertIn("reason=slice-anon-memory", logs.output[0])
        w.clickhouse_memory.assert_not_called()

    def test_page_cache_does_not_count(self):
        # 20 GiB of file pages in memory.stat must not pause while anon is below the line
        w, path = self._worker(4 * 1024 ** 3)
        env = {"RDS_BINLOG_ROWS_SLICE_STAT_FILE": path, "RDS_BINLOG_ROWS_SLICE_ANON_MAX_GIB": "8"}
        with mock.patch.dict("os.environ", env):
            self.assertTrue(w.wait_for_capacity(0))

    def test_unreadable_slice_file_pauses_with_its_reason(self):
        w, path = self._worker(None)
        with mock.patch.dict("os.environ", {"RDS_BINLOG_ROWS_SLICE_STAT_FILE": path}), \
                self.assertLogs("binlog_rows_worker", level="WARNING") as logs:
            self.assertFalse(w.wait_for_capacity(0))
        self.assertIn("slice-memory-unreadable", logs.output[0])

    def test_index_phases_env(self):
        from app import index_worker

        with mock.patch.dict("os.environ", {"RDS_BINLOG_INDEX_PHASES": ""}):
            self.assertEqual(index_worker._env_phases("RDS_BINLOG_INDEX_PHASES"), frozenset(index_worker.INDEX_PHASES_ALL))
        with mock.patch.dict("os.environ", {"RDS_BINLOG_INDEX_PHASES": "analytics, select,rollup"}):
            self.assertEqual(index_worker._env_phases("RDS_BINLOG_INDEX_PHASES"),
                             frozenset({"analytics", "select", "rollup"}))
        with mock.patch.dict("os.environ", {"RDS_BINLOG_INDEX_PHASES": "analytics,fulltext"}), \
                self.assertRaises(SystemExit):
            index_worker._env_phases("RDS_BINLOG_INDEX_PHASES")


class Release1299Tests(unittest.TestCase):
    def _worker(self, kind, anon_bytes):
        import tempfile
        from app import binlog_rows_worker as worker

        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = f"{tmp.name}/memory.stat"
        with open(path, "w") as handle:
            handle.write(f"anon {anon_bytes}\n")
        w = worker.Worker.__new__(worker.Worker)
        w.stop = mock.Mock()
        w.stop.is_set.side_effect = [False, True]
        w.lane_specs = [(kind, None if kind == "live" else ("rm-1", "a", "b"))]
        w.publish = mock.Mock()
        w.clickhouse_memory = mock.Mock(return_value=0)
        w.collector_lag_seconds = mock.Mock(return_value=0)
        return w, path

    def test_live_lane_runs_where_a_window_lane_pauses(self):
        env = {"RDS_BINLOG_ROWS_SLICE_ANON_MAX_GIB": "8", "RDS_BINLOG_ROWS_LIVE_HEADROOM_GIB": "0.75"}
        anon = int(8.4 * 1024 ** 3)
        live, path = self._worker("live", anon)
        with mock.patch.dict("os.environ", {**env, "RDS_BINLOG_ROWS_SLICE_STAT_FILE": path}):
            self.assertTrue(live.wait_for_capacity(0))
        window, path = self._worker("window", anon)
        with mock.patch.dict("os.environ", {**env, "RDS_BINLOG_ROWS_SLICE_STAT_FILE": path}), \
                self.assertLogs("binlog_rows_worker", level="WARNING"):
            self.assertFalse(window.wait_for_capacity(0))
