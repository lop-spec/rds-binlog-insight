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
        lanes = [s for s in br.build_schema() if "binlog_rows_buf_worker_v4_" in s]
        self.assertEqual(len(lanes), br.WORKER_LANES_MAX)
        for statement in lanes:
            self.assertIn(f"PARTITION BY toUInt8({br.BUCKET_EXPR}) ORDER BY _source_part_key", statement)

    def test_each_move_reads_only_its_bucket_and_skips_absent_buckets(self):
        client = mock.Mock()
        br.RowsIngestor(client, buffer_table="b").move(["raw:f"], "t", partitions=[("2026-10-21", 2), ("2026-10-21", 5), ("2026-10-22", 5)])
        sqls = [c.args[0] for c in client.execute.call_args_list]
        self.assertEqual(len(sqls), 2)
        self.assertIn("_partition_id = '2'", sqls[0])
        self.assertIn("_partition_id = '5'", sqls[1])


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
        self.assertIn("AND (table_name != '' OR operation = 'DDL') AND", sql)

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
            ingestor.partitions.return_value = [("2026-09-14", 0)]
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
            ingestor.partitions.return_value = [("2026-09-14", 0)]
            worker.commit(ingestor, {"file_id": "f2", "instance_id": "i", "source_file_name": "b"}, ["raw:f2"], 5,
                          "raw", True, 0)
            self.assertEqual(list((worker.inflight_dir()).glob("*.json")), [])
            (worker.inflight_dir() / "f2.json").write_text(json.dumps({"instance_id": "i", "file_id": "f2", "part_keys": ["raw:f2"]}))
            worker.ch.reset_mock()  # the move slot asked ClickHouse for its memory before the move
            self.assertEqual(worker.recover_inflight(), 0)
            worker.ch.execute.assert_not_called()


class BackgroundRecoveryTests(unittest.TestCase):
    """1.33.12: markers left by a predecessor are purged while the lanes run; their files stay claimed until then."""

    def worker(self, tmp):
        from pathlib import Path
        from app import binlog_rows_worker as worker_module
        worker = worker_module.Worker.__new__(worker_module.Worker)
        worker.storage = mock.Mock(paths={"index": Path(tmp)})
        worker.ingested = mock.Mock(return_value=set())
        worker.claims = worker_module.Claims()
        worker.ch = mock.Mock()
        return worker

    @staticmethod
    def marker(folder, name):
        path = folder / f"{name}.json"
        path.write_text(json.dumps({"instance_id": "i", "file_id": name, "part_keys": [f"raw:{name}"], "lo": 1, "hi": 2,
                                    "partitions": [["2026-10-19", 0]]}))
        return path

    def test_recovery_runs_beside_the_lanes_and_holds_its_files(self):
        import tempfile
        import threading
        with tempfile.TemporaryDirectory() as tmp:
            worker = self.worker(tmp)
            release = threading.Event()
            worker.ch.execute.side_effect = lambda *a, **k: release.wait(5)
            folder = worker.inflight_dir()
            for name in ("old1", "old2"):
                self.marker(folder, name)
            thread = worker.recover_in_background()
            self.assertIsNotNone(thread)
            self.assertFalse(worker.claims.take("old2"))  # a lane cannot take a file still being purged
            self.assertTrue(worker.claims.take("fresh"))  # other files go on
            release.set()
            thread.join(5)
            self.assertFalse(thread.is_alive())
            self.assertEqual(list(folder.glob("*.json")), [])
            self.assertTrue(worker.claims.take("old2"))   # released once its rows are gone

    def test_a_failed_purge_keeps_its_marker_and_releases_the_file(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            worker = self.worker(tmp)
            worker.ch.execute.side_effect = RawBinlogError("memory", "X")
            path = self.marker(worker.inflight_dir(), "bad")
            worker.recover_in_background().join(5)
            self.assertTrue(path.exists())
            self.assertTrue(worker.claims.take("bad"))

    def test_nothing_left_means_no_thread(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(self.worker(tmp).recover_in_background())


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


class Release12910Tests(unittest.TestCase):
    def test_ranged_bucket_turns_crc_off_on_a_copy_only(self):
        from app import binlog_rows_worker as worker

        class Bucket:
            enable_crc = True

        shared = Bucket()
        view = worker.ranged_bucket(shared)
        self.assertIsNot(view, shared)
        self.assertFalse(view.enable_crc)
        self.assertTrue(shared.enable_crc)
        plain = Bucket()
        plain.enable_crc = False
        self.assertIs(worker.ranged_bucket(plain), plain)

    def test_raw_verification_no_longer_uses_crc64(self):
        import inspect
        from app import binlog_rows_worker as worker

        source = inspect.getsource(worker.Worker.ingest_raw)
        self.assertNotIn("Crc64", source)
        self.assertIn('digest.hexdigest() != raw["sha256"]', source)

    def test_throttled_call_reuses_until_the_interval_passes(self):
        from app.clickhouse_slowlog import ThrottledCall

        now = [100.0]
        calls = []
        throttled = ThrottledCall(lambda: calls.append(1) or len(calls), 30, clock=lambda: now[0])
        self.assertEqual(throttled(), 1)
        now[0] += 10
        self.assertEqual(throttled(), 1)
        self.assertEqual(throttled(force=True), 2)
        now[0] += 31
        self.assertEqual(throttled(), 3)


class Release12911Tests(unittest.TestCase):
    DAY = 86_400_000_000

    def test_failed_move_purges_only_the_files_days(self):
        client = mock.Mock()
        client.execute.side_effect = [None, RawBinlogError("memory", "X"), None, None]
        lo = 1789430400000000  # 2026-09-15T00:00Z
        with self.assertRaises(RawBinlogError):
            br.RowsIngestor(client).move(["raw:f"], "t", lo_us=lo, hi_us=lo + 3_600_000_000)
        # the failed bucket's INSERT is stopped on the server before the purge (1.29.16)
        failed_qid = client.execute.call_args_list[1].kwargs["query_id"]
        kill = client.execute.call_args_list[2]
        self.assertIn("KILL QUERY WHERE query_id = {q:String} SYNC", kill.args[0])
        self.assertEqual(kill.kwargs["params"]["q"], failed_qid)
        self.assertTrue(failed_qid.endswith("-b1"))
        call = client.execute.call_args_list[-1]
        self.assertIn("DELETE FROM insight.binlog_rows_v1 WHERE event_date BETWEEN {d0:Date} AND {d1:Date}", call.args[0])
        self.assertEqual((call.kwargs["params"]["d0"], call.kwargs["params"]["d1"]), ("2026-09-14", "2026-09-16"))

    def test_unbounded_purge_is_logged(self):
        client = mock.Mock()
        with self.assertLogs("app.binlog_rows", level="WARNING") as logs:
            br.purge_file_rows(client, ["raw:f"], None, None, reason="test")
        self.assertIn("BINLOG_ROWS_PURGE_UNBOUNDED", logs.output[0])
        self.assertNotIn("event_date", client.execute.call_args.args[0])

    def test_recovery_bounds_the_purge_with_the_marker_span(self):
        import tempfile
        from pathlib import Path
        from app import binlog_rows_worker as worker_module

        with tempfile.TemporaryDirectory() as tmp:
            worker = worker_module.Worker.__new__(worker_module.Worker)
            worker.storage = mock.Mock(paths={"index": Path(tmp)})
            worker.ch = mock.Mock()
            worker.ingested = mock.Mock(return_value=set())
            ingestor = mock.Mock()
            ingestor.partitions.return_value = [("2026-09-15", 2), ("2026-09-15", 6)]
            ingestor.record.side_effect = RuntimeError("killed before manifest")
            entry = {"file_id": "f9", "instance_id": "i", "source_file_name": "mysql-bin.9",
                     "lo": 1789430400000000, "hi": 1789430460000000}
            with self.assertRaises(RuntimeError):
                worker.commit(ingestor, entry, ["raw:f9"], 1, "raw", True, 0)
            self.assertEqual(ingestor.move.call_args.kwargs,
                             {"lo_us": entry["lo"], "hi_us": entry["hi"],
                              "partitions": [("2026-09-15", 2), ("2026-09-15", 6)]})
            self.assertEqual(worker.recover_inflight(), 1)
            call = worker.ch.execute.call_args
            self.assertIn("event_date IN ('2026-09-15') AND tbl_bucket IN (2, 6)", call.args[0])
            # a marker written before 1.29.14 has no partitions: the purge falls back to the file's days
            marker = Path(tmp) / "binlog-rows-inflight" / "f8.json"
            marker.write_text(json.dumps({"instance_id": "i", "file_id": "f8", "part_keys": ["raw:f8"],
                                          "lo": entry["lo"], "hi": entry["hi"]}))
            self.assertEqual(worker.recover_inflight(), 1)
            call = worker.ch.execute.call_args
            self.assertIn("event_date BETWEEN", call.args[0])
            self.assertEqual(call.kwargs["params"]["d0"], "2026-09-14")


class Release12912Tests(unittest.TestCase):
    def test_slowlog_reconcile_reads_parts_in_one_pass(self):
        from app.clickhouse_slowlog import reconcile_slowlog_manifest

        day = 86_400_000_000
        now = 100 * day
        parts = [{"path": f"p{i}", "min_event_epoch_us": now - i * day, "max_event_epoch_us": now - i * day + 1}
                 for i in range(5)]
        metadata = mock.Mock()
        metadata.iter_slowlog_parts.return_value = iter(parts)
        manifest = mock.Mock()
        reconcile_slowlog_manifest(metadata, manifest, retention_days=3, now_us=now)
        metadata.slowlog_parts_page.assert_not_called()
        source = manifest.reconcile_streaming.call_args.args[0]
        self.assertNotIsInstance(source, list)
        self.assertEqual([p["path"] for p in source], ["p0", "p1", "p2", "p3", "p4"])
        self.assertTrue(manifest.reconcile_streaming.call_args.kwargs["filter_parts_to_window"])
        self.assertEqual(manifest.reconcile_streaming.call_args.kwargs["start_epoch_us"], now - 3 * day)


class Release12914Tests(unittest.TestCase):
    MEM = "ClickHouse 返回 500: Code: 241. DB::Exception: Query memory limit exceeded: would use 1.12 GiB"

    def _rows(self, responder):
        from app.binlog_rows import BinlogRows

        store = BinlogRows.__new__(BinlogRows)
        store.ch = mock.Mock()
        store.ch.rows.side_effect = responder
        store.registry = lambda db, tbl: None
        store.coverage = lambda instance, start, end: {"intervals": [[start, end]], "gaps": []}
        store._statements = lambda rows: {}
        return store

    def test_memory_limited_slice_is_split_newest_first(self):
        from app import binlog_rows as br

        day = br.DAY_US
        start, end = 10 * day, 10 * day + day - 1
        calls = []

        def responder(sql, params, settings, timeout, query_id):
            calls.append((params["lo"], params["hi"]))
            self.assertEqual(settings["read_in_order_use_buffering"], 0)
            self.assertEqual(settings["max_memory_usage"], br.QUERY_MEMORY_BYTES)
            if params["hi"] - params["lo"] > day // 2:  # the full day fails, halves succeed
                raise br.RawBinlogError(self.MEM, "CLICKHOUSE_BINLOG_ROWS_UNAVAILABLE")
            n = int(sql.rsplit("LIMIT", 1)[1])
            return [{"event_epoch_us": params["hi"] - i, "row_query_hash": 0} for i in range(min(n, 3))]

        store = self._rows(responder)
        with mock.patch.object(br, "present", side_effect=lambda r, s: r):
            out = store.query({"instance": "i", "database": "d", "table": "t", "limit": 5}, start, end)
        mid = start + (end - start) // 2
        self.assertEqual(calls, [(start, end), (mid + 1, end), (start, mid)])
        stamps = [r["event_epoch_us"] for r in out["rows"]]
        self.assertEqual(stamps, sorted(stamps, reverse=True))
        self.assertEqual(stamps[:3], [end, end - 1, end - 2])
        self.assertEqual(len(stamps), 5)

    def test_split_stops_at_minimum_slice_and_other_errors_propagate(self):
        from app import binlog_rows as br

        start = 10 * br.DAY_US
        end = start + br.QUERY_SPLIT_MIN_US  # below 2 x minimum: no split
        store = self._rows(lambda *a, **k: (_ for _ in ()).throw(br.RawBinlogError(self.MEM, "X")))
        with self.assertRaises(br.RawBinlogError):
            store.query({"instance": "i", "database": "d", "table": "t"}, start, end)
        self.assertEqual(store.ch.rows.call_count, 1)
        store = self._rows(lambda *a, **k: (_ for _ in ()).throw(br.RawBinlogError("Code: 62. syntax", "X")))
        with self.assertRaises(br.RawBinlogError):
            store.query({"instance": "i", "database": "d", "table": "t"}, 10 * br.DAY_US, 11 * br.DAY_US - 1)
        self.assertEqual(store.ch.rows.call_count, 1)

    def test_purge_is_bounded_to_partitions(self):
        from app.binlog_rows import purge_file_rows

        ch = mock.Mock()
        purge_file_rows(ch, ["raw:a"], 1, 2, partitions=[("2026-09-23", 0), ("2026-09-23", 3), ("2026-09-24", 0)],
                        reason="t")
        sql = ch.execute.call_args.args[0]
        self.assertIn("event_date IN ('2026-09-23', '2026-09-24') AND tbl_bucket IN (0, 3) AND _source_part_key", sql)
        self.assertNotIn("BETWEEN", sql)
        ch.reset_mock()
        purge_file_rows(ch, ["raw:a"], 1, 2, partitions=[], reason="t")
        ch.execute.assert_not_called()
        with self.assertRaises(ValueError):
            purge_file_rows(ch, ["raw:a"], partitions=[("2026-09-23') OR 1=1 --", 0)])
        with self.assertRaises(ValueError):
            purge_file_rows(ch, ["raw:a"], partitions=[("2026-09-23", 8)])

    def test_failed_move_purges_only_attempted_buckets(self):
        from app import binlog_rows as br

        ing = br.RowsIngestor(mock.Mock(), buffer_table="insight.buf")
        seen = []

        def execute(sql, **kwargs):
            seen.append(sql)
            if "% 8 = 2" in sql:
                raise br.RawBinlogError("Code: 241. memory", "X")
            return ""

        ing.ch.execute.side_effect = execute
        parts = [("2026-09-23", b) for b in range(8)]
        with self.assertRaises(br.RawBinlogError):
            ing.move(["raw:a"], "t", lo_us=1, hi_us=2, partitions=parts)
        purge = [s for s in seen if s.startswith("DELETE")]
        self.assertEqual(len(purge), 1)
        self.assertIn("tbl_bucket IN (0, 1, 2)", purge[0])

    def test_partitions_read_from_buffer(self):
        from app import binlog_rows as br

        ing = br.RowsIngestor(mock.Mock(), buffer_table="insight.buf")
        ing.ch.execute.return_value = "2026-09-23\t0\n2026-09-23\t5\n"
        self.assertEqual(ing.partitions(), [("2026-09-23", 0), ("2026-09-23", 5)])
        self.assertIn("FROM insight.buf", ing.ch.execute.call_args.args[0])


class Release12916Tests(unittest.TestCase):
    """Binlog analytics from the row-store aggregates; retry and server-side insert safety."""

    def test_aggregate_statements_worker_and_history(self):
        sqls = br.aggregate_statements("insight.buf", br.ROW_STORE_FILTER)
        stats, txns, all_tables = sqls["agg"], sqls["aggtxn"], sqls["aggtxnall"]
        self.assertIn("INSERT INTO insight.binlog_agg_5m_v1", stats)
        self.assertIn("normalizedQueryHash(leftUTF8(row_query, 4096))", stats)
        self.assertIn("normalizeQuery(leftUTF8(row_query, 4096))", stats)
        self.assertIn("countIf(row_index = 1)", stats)
        self.assertIn("ORDER BY event_date, bucket_us, instance_id, database_name, table_name, operation, fp", stats)
        self.assertIn("INSERT INTO insight.binlog_agg_txn_5m_v2", txns)
        self.assertIn("uniqExactIf(transaction_id, transaction_id != '')", txns)
        self.assertIn("'*' AS a_db, '*' AS a_tbl", all_tables)
        self.assertIn("GROUP BY event_date, bucket_us, instance_id ORDER BY", all_tables)
        # aliases never equal a source column: the WHERE filter would read the alias instead
        import re
        source_columns = set(br.STAGE_COLUMN_NAMES) | {"tbl_bucket", "row_query_hash"}
        for sql in sqls.values():
            self.assertFalse(set(re.findall(r"\bAS (\w+)", sql)) & source_columns, sql[:100])
        history = br.aggregate_statements("insight.binlog_rows_v1", "event_date = '2026-09-20'", with_text=False)["agg"]
        self.assertIn("toUInt64(0) AS fp", history)
        self.assertNotIn("row_query", history)
        self.assertNotIn("before_json", history)
        schema = "\n".join(br.build_schema())
        self.assertIn("CREATE TABLE IF NOT EXISTS insight.binlog_agg_5m_v1", schema)
        self.assertIn("non_replicated_deduplication_window = 20000", schema)

    def test_aggregate_uses_one_dedup_token_per_file_and_table(self):
        client = mock.Mock()
        br.RowsIngestor(client, buffer_table="insight.buf").aggregate("f1", "binlog-rows-l3")
        tokens = [c.kwargs["settings"]["insert_deduplication_token"] for c in client.execute.call_args_list]
        self.assertEqual(tokens, ["agg:f1", "aggtxn:f1", "aggtxnall:f1"])
        self.assertTrue(all(c.kwargs["settings"]["max_threads"] == 1 for c in client.execute.call_args_list))

    def _worker(self, tmp):
        from pathlib import Path
        from app import binlog_rows_worker as worker_module

        worker = worker_module.Worker.__new__(worker_module.Worker)
        worker.storage = mock.Mock(paths={"index": Path(tmp)})
        worker.ch = mock.Mock()
        worker.ingested = mock.Mock(return_value=set())
        return worker

    def test_commit_purges_an_earlier_attempt_then_moves_aggregates_records(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            worker = self._worker(tmp)
            entry = {"file_id": "f7", "instance_id": "i", "source_file_name": "mysql-bin.7",
                     "lo": 1789430400000000, "hi": 1789430460000000}
            (worker.inflight_dir() / "f7.json").write_text(json.dumps(
                {"instance_id": "i", "file_id": "f7", "part_keys": ["raw:f7"], "file": "mysql-bin.7",
                 "lo": entry["lo"], "hi": entry["hi"], "partitions": [["2026-09-15", 1]]}))
            calls = []
            ingestor = mock.Mock()
            ingestor.partitions.return_value = [("2026-09-15", 1)]
            ingestor.move.side_effect = lambda *a, **k: calls.append("move")
            ingestor.aggregate.side_effect = lambda *a, **k: calls.append("aggregate")
            ingestor.record.side_effect = lambda *a, **k: calls.append("record")
            worker.ch.execute.side_effect = lambda sql, **k: calls.append("purge" if sql.startswith("DELETE") else sql)
            worker.commit(ingestor, entry, ["raw:f7"], 1, "raw", True, 0)
            steps = [c for c in calls if c in ("purge", "move", "aggregate", "record")]
            self.assertEqual(steps, ["purge", "move", "aggregate", "record"])
            # 1.34.14: the retry first asks whether rows of the file are left; no answer means delete as before
            self.assertTrue(str(calls[0]).startswith("SELECT count() FROM insight.binlog_rows_v1"), calls[0])
            self.assertEqual(list(worker.inflight_dir().glob("*.json")), [])

    def test_failed_move_that_purged_drops_its_marker(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            worker = self._worker(tmp)
            entry = {"file_id": "f8", "instance_id": "i", "source_file_name": "b", "lo": 1, "hi": 2}
            ingestor = mock.Mock()
            ingestor.partitions.return_value = []
            purged = RawBinlogError("memory", "X")
            purged.rows_purged = True
            ingestor.move.side_effect = purged
            with self.assertRaises(RawBinlogError):
                worker.commit(ingestor, entry, ["raw:f8"], 1, "raw", True, 0)
            self.assertFalse((worker.inflight_dir() / "f8.json").exists())
            ingestor.move.side_effect = RawBinlogError("purge failed too", "X")  # no rows_purged: keep marker
            with self.assertRaises(RawBinlogError):
                worker.commit(ingestor, entry, ["raw:f8"], 1, "raw", True, 0)
            self.assertTrue((worker.inflight_dir() / "f8.json").exists())

    def _analytics(self, coverage, responder):
        from app.binlog_rows_analytics import BinlogRowsAnalytics

        rows = mock.Mock()
        rows.coverage.return_value = coverage
        rows.ch.rows.side_effect = responder
        return BinlogRowsAnalytics(rows)

    def test_analytics_returns_none_without_row_store_coverage(self):
        a = self._analytics({"indexedFiles": 0, "pendingFiles": 0, "missingFiles": 0, "gaps": []},
                            lambda *a, **k: self.fail("no query without coverage"))
        self.assertIsNone(a.summarize({"instance": "i"}, 0, 3_600_000_000))
        self.assertIsNone(a.summarize({"instance": ""}, 0, 3_600_000_000))

    def test_analytics_shape_matches_the_parquet_contract(self):
        from app.analytics_index import SQL_ORDERS

        seen = []

        def responder(sql, params, settings, timeout):
            seen.append(sql)
            if "countIf(fp = 0) AS a_synthetic" in sql:
                return [{"a_events": 10, "a_executions": 4, "a_payload": 100, "a_slow": 1, "a_fingerprints": 2,
                         "a_objects": 1, "a_synthetic": 1}]
            if "row_number() OVER" in sql:
                base = {"events": 5, "executions": 2, "row_events": 5, "payload_bytes": 50, "exec_time_ms_total": 0,
                        "exec_time_ms_max": 0, "slow_events": 0, "first_epoch_us": 1, "last_epoch_us": 2,
                        "objects": 1, "db_name": "shop", "tbl_name": "t", "op_name": "UPDATE",
                        "est_scan_total": 0, **{f"r_{k}": 1 for k in SQL_ORDERS}}
                return [{**base, "fingerprint": "123", "fp_max": 123},
                        {**base, "fingerprint": "t:UPDATE:shop.t", "fp_max": 0, **{f"r_{k}": 2 for k in SQL_ORDERS}}]
            if "AS a_normalized" in sql:
                self.assertEqual(params["fps"], "[123]")
                return [{"a_fp": 123, "a_normalized": "UPDATE t SET a = ?", "a_sample": "UPDATE t SET a = 1"}]
            if "binlog_agg_txn_5m_v2" in sql and "GROUP BY db_name, tbl_name" in sql:
                self.assertIn("AND NOT (database_name = '*' AND table_name = '*') AND table_name IN", sql)
                return [{"db_name": "shop", "tbl_name": "t", "a_txns": 3}]
            if "binlog_agg_txn_5m_v2" in sql and "GROUP BY" not in sql:
                self.assertIn("AND database_name = '*' AND table_name = '*'", sql)
                return [{"a_txns": 7}]
            if "AS a_updates" in sql:
                return [{"db_name": "shop", "tbl_name": "t", "a_events": 10, "a_updates": 10, "a_deletes": 0}]
            if sql.startswith("SELECT sum(events) AS a_events, sum(payload_bytes) AS a_payload FROM"):
                return [{"a_events": 10, "a_payload": 100}]
            return []

        coverage = {"indexedFiles": 3, "pendingFiles": 1, "missingFiles": 0, "gaps": [{"reason": "pending"}],
                    "requested_us": 10, "covered_us": 5}
        out = self._analytics(coverage, responder).summarize(
            {"instance": "i", "limit": 10, "order": "executions", "operation": "update"}, 0, 3_600_000_000)
        self.assertEqual(set(out), {"mode", "window", "sql", "transactions", "locks", "coverage", "evidence"})
        self.assertEqual(set(out["sql"]["orders"]), set(SQL_ORDERS))
        real, synthetic = out["sql"]["statements"]
        self.assertEqual((real["source_kind"], real["normalized_sql"]), ("rows-query", "UPDATE t SET a = ?"))
        self.assertEqual((synthetic["source_kind"], synthetic["normalized_sql"]), ("synthetic", "UPDATE shop.t"))
        self.assertEqual(out["sql"]["totals"]["fingerprints"], 2)
        self.assertNotIn("_synthetic_rows", out["sql"])
        self.assertEqual(out["transactions"]["totals"]["transactions"], 7)
        self.assertIsNone(out["transactions"]["totals"]["max_duration_us"])
        self.assertEqual(out["locks"]["table_hotspots"][0]["txn_count"], 3)
        cov = out["coverage"]
        self.assertEqual((cov["unit"], cov["total_parts"], cov["covered_parts"], cov["pending_parts"]),
                         ("files", 4, 3, 1))
        self.assertFalse(cov["complete"])
        self.assertIn("合成", cov["note"])
        self.assertTrue(any("operation = {operation:String}" in s for s in seen))
        # ClickHouse aliases are query-global: outside the renaming subquery no alias may equal a column name
        import re
        columns = {"event_date", "bucket_us", "instance_id", "database_name", "table_name", "operation", "fp",
                   "events", "executions", "payload_bytes", "slow_events", "exec_time_ms_total", "exec_time_ms_max",
                   "first_epoch_us", "last_epoch_us", "normalized_sql", "sample_sql", "txns"}
        for sql in seen:
            if "AS src" in sql:
                continue
            clash = set(re.findall(r"\bAS (\w+)", sql)) & columns
            self.assertFalse(clash, (clash, sql[:120]))

    def test_storage_serves_binlog_analytics_from_the_row_store_and_falls_back(self):
        from app.storage import EventStorage

        fake = mock.Mock()
        fake._query_window.return_value = (0, 3_600_000_000)
        fake.metadata.parts_in_range.return_value = []
        fake.binlog_rows.analytics.return_value = {"mode": "binlog-rows"}
        query, settings = {"source": "binlog", "instance": "i"}, mock.Mock(retention_days=60)
        self.assertEqual(EventStorage.analytics_summary(fake, query, settings, None), {"mode": "binlog-rows"})
        fake.binlog_rows.analytics.side_effect = RuntimeError("clickhouse down")
        fake.analytics_index.coverage.side_effect = LookupError("parquet path reached")
        with self.assertLogs("app.storage", level="ERROR") as logs, self.assertRaises(LookupError):
            EventStorage.analytics_summary(fake, query, settings, None)
        self.assertIn("BINLOG_ROWS_ANALYTICS_FAILED", logs.output[0])


class Release12918Tests(unittest.TestCase):
    def test_row_store_answers_before_the_parquet_part_listing(self):
        from app.storage import EventStorage

        fake = mock.Mock()
        fake._query_window.return_value = (0, 3_600_000_000)
        fake.binlog_rows.analytics.return_value = {"mode": "binlog-rows"}
        out = EventStorage.analytics_summary(fake, {"source": "binlog", "instance": "i"}, mock.Mock(retention_days=60),
                                             None)
        self.assertEqual(out, {"mode": "binlog-rows"})
        fake.metadata.parts_in_range.assert_not_called()

    def test_ddl_without_table_has_no_dml_figure(self):
        from app.analytics_index import SQL_ORDERS
        from app.binlog_rows_analytics import BinlogRowsAnalytics

        def responder(sql, params, settings, timeout):
            if "operation = 'DDL' GROUP BY a_bucket" in sql:
                return [{"a_bucket": 0, "db_name": "shop", "tbl_name": "", "a_fp": 0, "a_first": 5, "a_sample": ""},
                        {"a_bucket": 0, "db_name": "shop", "tbl_name": "t", "a_fp": 1, "a_first": 4,
                         "a_sample": "ALTER TABLE t"}]
            if "operation != 'DDL'" in sql:
                self.assertEqual(params["tbl"], "t")
                return [{"a_events": 9}]
            if "countIf(fp = 0) AS a_synthetic" in sql:
                return [{"a_events": 0, "a_executions": 0, "a_payload": 0, "a_slow": 0, "a_fingerprints": 0,
                         "a_objects": 0, "a_synthetic": 0}]
            if sql.startswith("SELECT sum(events) AS a_events, sum(payload_bytes) AS a_payload FROM"):
                return [{"a_events": 0, "a_payload": 0}]
            return []

        rows = mock.Mock()
        rows.coverage.return_value = {"indexedFiles": 1, "pendingFiles": 0, "missingFiles": 0, "gaps": []}
        rows.ch.rows.side_effect = responder
        ddl = BinlogRowsAnalytics(rows).summarize({"instance": "i"}, 0, 3_600_000_000)["locks"]["ddl_windows"]
        self.assertEqual([d["concurrent_dml_events"] for d in ddl], [None, 9])
        self.assertEqual(ddl[0]["sample_sql"], "（历史数据未记录语句）")
        self.assertTrue(SQL_ORDERS)

    def test_write_hotspots_lists_the_busiest_tables_with_the_stretch_before_them(self):
        from app.binlog_rows_analytics import BinlogRowsAnalytics

        seen = []

        def responder(sql, params, settings, timeout):
            seen.append((sql, params))
            if "sumIf(events, operation = 'INSERT')" in sql:
                return [{"db_name": "shop", "tbl_name": "goods_index", "a_events": 9000, "a_inserts": 4000, "a_updates": 0,
                         "a_deletes": 5000, "a_payload": 123456},
                        {"db_name": "shop", "tbl_name": "orders", "a_events": 1000, "a_inserts": 600, "a_updates": 400,
                         "a_deletes": 0, "a_payload": 99}]
            if sql.startswith("SELECT sum(events) AS a_events FROM"):
                return [{"a_events": 12000}]
            return [{"db_name": "shop", "tbl_name": "goods_index", "a_events": 900}]     # the stretch before: orders is absent

        rows = mock.Mock()
        rows.ch.rows.side_effect = responder
        start, end = 1791950400000000, 1791951600000000                                  # twenty minutes
        out = BinlogRowsAnalytics(rows).write_hotspots("rm-x", start, end)
        self.assertEqual(out["status"], "ok")
        self.assertEqual((out["start_us"], out["end_us"], out["before_start_us"], out["total_events"]),
                         (start, end, start - (end - start), 12000))
        self.assertEqual(out["tables"][0], {"database_name": "shop", "table_name": "goods_index", "events": 9000, "inserts": 4000,
                                             "updates": 0, "deletes": 5000, "payload_bytes": 123456, "share": 0.75, "before_events": 900})
        self.assertEqual((out["tables"][1]["table_name"], out["tables"][1]["before_events"]), ("orders", 0))   # new in this window
        windows = [params for sql, params in seen]
        self.assertTrue(all("operation != 'DDL'" in sql for sql, _ in seen))             # DDL has its own list
        self.assertEqual(len(seen), 3)
        current = next(p for sql, p in seen if "sumIf(events, operation = 'INSERT')" in sql)
        before = next(p for sql, p in seen if "LIMIT 1000" in sql)
        self.assertEqual((current["lo"], current["hi"]), (start // 300_000_000 * 300_000_000, end))
        self.assertEqual(before["hi"], start - 1)                                         # ends where the window begins
        self.assertEqual(before["lo"], (start - (end - start)) // 300_000_000 * 300_000_000)
        self.assertEqual({p["i"] for p in windows}, {"rm-x"})

    def test_write_hotspots_without_rows_is_empty_not_an_error_and_the_limit_is_bounded(self):
        from app.binlog_rows_analytics import BinlogRowsAnalytics

        limits = []
        rows = mock.Mock()
        rows.ch.rows.side_effect = lambda sql, params, settings, timeout: limits.append(params.get("n")) or []
        out = BinlogRowsAnalytics(rows).write_hotspots("rm-x", 0, 600_000_000, limit=500)
        self.assertEqual((out["status"], out["tables"], out["total_events"]), ("empty", [], 0))
        self.assertIn(20, limits)                                                        # a caller cannot ask for more than 20 tables

    def test_ddl_events_returns_the_ddl_inside_a_window_with_its_event_time(self):
        from app.binlog_rows_analytics import BinlogRowsAnalytics

        seen = {}

        def responder(sql, params, settings, timeout):
            seen.update(sql=sql, params=params)
            return [{"a_bucket": 0, "db_name": "", "tbl_name": "", "a_fp": 1, "a_first": 1791947877000000,
                     "a_sample": "ALTER TABLE csr.biz_shop_job ADD INDEX i (a)"},
                    {"a_bucket": 0, "db_name": "shop", "tbl_name": "t", "a_fp": 0, "a_first": 1791947890000000, "a_sample": None}]

        rows = mock.Mock()
        rows.ch.rows.side_effect = responder
        out = BinlogRowsAnalytics(rows).ddl_events("rm-x", 1791947700000000, 1791948000000000)
        self.assertEqual(out, [{"event_epoch_us": 1791947877000000, "database_name": "", "table_name": "",
                                "sample_sql": "ALTER TABLE csr.biz_shop_job ADD INDEX i (a)"},
                               {"event_epoch_us": 1791947890000000, "database_name": "shop", "table_name": "t", "sample_sql": ""}])
        self.assertIn("operation = 'DDL'", seen["sql"])
        self.assertIn("HAVING a_first BETWEEN {a:Int64} AND {b:Int64}", seen["sql"])
        self.assertEqual((seen["params"]["i"], seen["params"]["a"], seen["params"]["b"]),
                         ("rm-x", 1791947700000000, 1791948000000000))
        self.assertEqual(seen["params"]["d0"], seen["params"]["d1"])   # one UTC day for a few minutes


class StreamIngestTests(unittest.TestCase):
    """1.33.11: the archived binlog goes from OSS straight into the parser; the SHA256 is taken on the way."""

    PAYLOAD = bytes(range(256)) * 997  # not a multiple of the chunk size

    class Body:
        def __init__(self, data):
            self.data = data

        def read(self, size=-1):
            chunk, self.data = self.data[:size], self.data[size:]
            return chunk

        def close(self):
            pass

    def bucket(self, payload, short_at=None):
        test = self

        class Bucket:
            enable_crc = False
            asked = []

            def get_object(self, key, byte_range):
                start, end = byte_range
                Bucket.asked.append(start)
                data = payload[start:end + 1]
                return test.Body(data[:-1] if start == short_at else data)
        return Bucket()

    def test_chunks_come_back_in_order_and_whole(self):
        import threading
        from app import binlog_rows_worker as worker
        got = b"".join(worker.stream_ranges(self.bucket(self.PAYLOAD), "k", len(self.PAYLOAD), threading.Event(),
                                            streams=3, chunk=1000))
        self.assertEqual(got, self.PAYLOAD)

    def test_a_short_range_fails_the_stream(self):
        import threading
        from app import binlog_rows_worker as worker
        with self.assertRaises(RawBinlogError):
            b"".join(worker.stream_ranges(self.bucket(self.PAYLOAD, short_at=5000), "k", len(self.PAYLOAD), threading.Event(),
                                          streams=2, chunk=1000))

    def test_prefetch_is_bounded(self):
        import threading
        from app import binlog_rows_worker as worker
        bucket = self.bucket(self.PAYLOAD)
        stream = worker.stream_ranges(bucket, "k", len(self.PAYLOAD), threading.Event(), streams=2, chunk=1000)
        next(stream)
        import time
        time.sleep(0.05)
        self.assertLessEqual(len(bucket.asked), 5)  # the first chunk plus at most 2 x streams ahead
        stream.close()

    def run_stream(self, payload, sha, parser_code, rows_in_buffer=None):
        import hashlib
        import sys
        import threading
        from app import binlog_rows_worker as worker
        w = worker.Worker.__new__(worker.Worker)
        w.stop = threading.Event()
        w.commit = mock.Mock()
        ingestor = mock.Mock()

        def load(stream, entry):
            while stream.read(4096):
                pass
            ingestor.loaded = stream.lines
        ingestor.load_ndjson_stream.side_effect = load
        ingestor.load_rowbinary_stream.side_effect = load
        ingestor.file_rows.side_effect = lambda _file: ingestor.loaded if rows_in_buffer is None else rows_in_buffer
        archive = mock.Mock()
        archive.bucket = self.bucket(payload)
        descriptor = {"raw": {"oss_key": "k", "size_bytes": len(self.PAYLOAD), "sha256": sha or hashlib.sha256(self.PAYLOAD).hexdigest()}}
        entry = {"file_id": "f", "instance_id": "i"}
        with mock.patch.object(worker, "parser_command", return_value=[sys.executable, "-c", parser_code]):
            total = w.ingest_raw_stream(ingestor, entry, 0, archive, descriptor)
        return total, w, ingestor

    # reads all of stdin, then prints one record per 1000 bytes and reports the count on stderr like binlog-parser
    PARSER = ("import sys\ndata = sys.stdin.buffer.read()\nn = len(data) // 1000\nfor i in range(n):\n    print('{\"n\":%d}' % i)\n"
              "sys.stderr.write('parsed %d audit records\\n' % n)\n")

    def test_a_good_stream_is_committed_with_the_parsers_row_count(self):
        total, w, ingestor = self.run_stream(self.PAYLOAD, None, self.PARSER)
        self.assertEqual(total, len(self.PAYLOAD) // 1000)
        w.commit.assert_called_once()
        self.assertEqual(w.commit.call_args.args[3], total)

    def test_a_wrong_sha_is_never_committed(self):
        with self.assertRaisesRegex(RawBinlogError, "SHA256"):
            self.run_stream(self.PAYLOAD, "0" * 64, self.PARSER)

    def test_a_short_object_is_never_committed(self):
        import hashlib
        with self.assertRaises(RawBinlogError):
            self.run_stream(self.PAYLOAD[:-10], hashlib.sha256(self.PAYLOAD).hexdigest(), self.PARSER)

    def test_a_failing_parser_is_never_committed(self):
        with self.assertRaisesRegex(RawBinlogError, "解析器"):
            self.run_stream(self.PAYLOAD, None, "import sys\nsys.stderr.write('bad event')\nsys.exit(3)\n")

    def test_a_buffer_that_lost_rows_is_never_committed(self):
        with self.assertRaisesRegex(RawBinlogError, "缓冲行数"):
            self.run_stream(self.PAYLOAD, None, self.PARSER, rows_in_buffer=1)


class BatchCommitTests(unittest.TestCase):
    """1.34.0: pending raw files go through one buffer and one move; aggregates and manifest stay per file."""

    def worker(self, tmp):
        import threading
        from pathlib import Path
        from app import binlog_rows_worker as worker_module
        w = worker_module.Worker.__new__(worker_module.Worker)
        w.storage = mock.Mock(paths={"index": Path(tmp)})
        w.stop = threading.Event()
        w.claims = worker_module.Claims()
        w.ingested = mock.Mock(return_value=set())
        w._archive = mock.Mock()
        w.archive = mock.Mock(return_value=w._archive)
        w.ch = mock.Mock()
        return w

    @staticmethod
    def entries(n):
        return [{"file_id": f"{i:032x}", "instance_id": "i", "host_instance_id": "h", "source_file_name": f"mysql-bin.{i}",
                 "lo": 1000 + i, "hi": 2000 + i, "kind": "raw", "descriptor": {"raw": {}}} for i in range(1, n + 1)]

    def test_one_move_for_the_batch_and_per_file_aggregates_and_manifest(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            w = self.worker(tmp)
            ingestor = mock.Mock()
            ingestor.partitions.return_value = [("2026-10-21", 3)]
            batch = self.entries(3)
            w.load_raw_stream = mock.Mock(side_effect=[10, 20, 30])
            rows = w.ingest_batch(ingestor, batch, 0)
            self.assertEqual(list(rows.values()), [10, 20, 30])
            ingestor.move.assert_called_once()
            self.assertEqual(ingestor.move.call_args.args[0], ["raw:" + e["file_id"] for e in batch])
            self.assertEqual((ingestor.move.call_args.kwargs["lo_us"], ingestor.move.call_args.kwargs["hi_us"]), (1001, 2003))
            self.assertEqual([c.kwargs["only_file"] for c in ingestor.aggregate.call_args_list], [True] * 3)
            self.assertEqual([c.args[1] for c in ingestor.record.call_args_list], [10, 20, 30])
            self.assertEqual(list(w.inflight_dir().glob("*.json")), [])

    def test_a_file_that_fails_to_load_names_itself_and_nothing_is_committed(self):
        import tempfile
        from app.binlog_rows_worker import BatchLoadError
        with tempfile.TemporaryDirectory() as tmp:
            w = self.worker(tmp)
            ingestor = mock.Mock()
            batch = self.entries(3)
            w.load_raw_stream = mock.Mock(side_effect=[10, RawBinlogError("sha", "RAW_INDEX_VERIFY_FAILED"), 30])
            with self.assertRaises(BatchLoadError) as caught:
                w.ingest_batch(ingestor, batch, 0)
            self.assertIs(caught.exception.entry, batch[1])
            ingestor.move.assert_not_called()
            ingestor.record.assert_not_called()
            self.assertGreaterEqual(ingestor.truncate.call_count, 2)

    def test_a_failed_move_purges_the_batch_and_drops_its_markers(self):
        import tempfile
        with tempfile.TemporaryDirectory() as tmp:
            w = self.worker(tmp)
            ingestor = mock.Mock()
            ingestor.partitions.return_value = [("2026-10-21", 3)]
            failure = RawBinlogError("memory", "X")
            failure.rows_purged = True
            ingestor.move.side_effect = failure
            w.load_raw_stream = mock.Mock(side_effect=[10, 20])
            with self.assertRaises(RawBinlogError):
                w.ingest_batch(ingestor, self.entries(2), 0)
            self.assertEqual(list(w.inflight_dir().glob("*.json")), [])
            ingestor.record.assert_not_called()

    def test_run_batch_takes_pending_raw_files_and_blames_only_the_failing_one(self):
        import tempfile
        from app import binlog_rows_worker as worker_module
        from app.binlog_rows_worker import BatchLoadError
        with tempfile.TemporaryDirectory() as tmp:
            w = self.worker(tmp)
            w.publish = mock.Mock()
            w.mark_ingested = mock.Mock()
            todo = self.entries(6)
            todo[2]["kind"] = "parquet"
            self.assertTrue(w.claims.take(todo[0]["file_id"]))
            seen = []

            def batch_run(ingestor, batch, lane):
                seen.append([e["file_id"] for e in batch])
                raise BatchLoadError(batch[1], RuntimeError("bad"))
            w.ingest_batch = batch_run
            failures = {}
            with mock.patch.object(worker_module, "BATCH_FILES", 4):
                w.stop.set()  # skip the 10 s back-off wait
                w.run_batch(mock.Mock(), 0, todo[0], todo[1:], failures)
            self.assertEqual(seen, [[todo[i]["file_id"] for i in (0, 1, 3, 4)]])  # parquet skipped, at most four
            self.assertEqual(failures, {todo[1]["file_id"]: 1})
            self.assertTrue(all(w.claims.take(todo[i]["file_id"]) for i in (0, 1, 3, 4)))  # all released
