from __future__ import annotations

import json
import tempfile
import threading
import unittest
from contextlib import ExitStack
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from app.columnar_input import open_parser_arrow, parser_schema
from app.storage import (
    EVENT_COLUMNS,
    PARSER_JSON_COLUMNS,
    EventStorage,
    StorageError,
    ensure_data_dirs,
    ingest_parser_file_detached,
)


def write_ipc(path, rows, schema=None, batch_size=2):
    table = pa.Table.from_pylist(rows, schema=schema or parser_schema(PARSER_JSON_COLUMNS))
    with pa.OSFile(str(path), "wb") as sink, pa.ipc.new_file(sink, table.schema) as writer:
        for batch in table.to_batches(max_chunksize=batch_size):
            writer.write_batch(batch)


class ColumnarInputTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.path = Path(self.root.name) / "input.arrow"

    def read(self, **options):
        with ExitStack() as resources:
            return open_parser_arrow(resources, self.path, PARSER_JSON_COLUMNS, **options).read_all()

    def test_all_parser_types_nulls_and_unsigned_limit(self):
        row = {name: ("中文\x00🙂" if kind == "VARCHAR" else 7)
               for name, kind in PARSER_JSON_COLUMNS.items()}
        row["table_map_id"] = 2**64 - 1
        row["event_epoch_us"] = 1789607092576176
        write_ipc(self.path, [row, {}, row], batch_size=1)
        actual = self.read().to_pylist()
        self.assertEqual(actual[0], row)
        self.assertEqual(actual[1], dict.fromkeys(PARSER_JSON_COLUMNS))
        self.assertEqual(actual[2], row)

    def test_missing_reordered_unknown_duplicate_or_wrong_schema_fails_closed(self):
        exact = parser_schema(PARSER_JSON_COLUMNS)
        for schema in (
            pa.schema([("sql_text", pa.string())]),
            pa.schema(list(reversed(exact))),
            pa.schema([("future_field", pa.string())]),
            pa.schema([("sql_text", pa.string()), ("sql_text", pa.string())]),
            pa.schema([("table_map_id", pa.int64())]),
        ):
            with self.subTest(schema=schema):
                write_ipc(self.path, [], schema)
                with self.assertRaises(ValueError):
                    self.read()

    def test_size_budget_and_corrupt_footer_fail(self):
        write_ipc(self.path, [{"sql_text": "SELECT 1"}])
        with self.assertRaisesRegex(ValueError, "byte budget"):
            self.read(max_bytes=1)
        data = self.path.read_bytes()
        self.path.write_bytes(data[:-3])
        with self.assertRaises(pa.ArrowInvalid):
            self.read()

    def test_empty_typed_input(self):
        write_ipc(self.path, [])
        self.assertEqual(self.read().num_rows, 0)
        self.assertEqual(self.read().schema, parser_schema(PARSER_JSON_COLUMNS))

    def test_decoded_columns_are_independently_counted_in_budget(self):
        schema = parser_schema(PARSER_JSON_COLUMNS)
        arrays = [
            pa.array(["x" * 100] * 20000, type=field.type)
            if field.name == "sql_text"
            else pa.nulls(20000, type=field.type)
            for field in schema
        ]
        table = pa.Table.from_arrays(arrays, schema=schema)
        options = pa.ipc.IpcWriteOptions(compression="zstd")
        with pa.OSFile(str(self.path), "wb") as sink:
            with pa.ipc.new_file(sink, table.schema, options=options) as writer:
                writer.write_table(table)
        limit = self.path.stat().st_size + 1024
        self.assertGreater(table.nbytes, limit)
        with self.assertRaisesRegex(ValueError, "decoded columns"):
            self.read(max_bytes=limit)

    def test_manifest_row_count_is_checked_before_parquet_publication(self):
        row = dict.fromkeys(PARSER_JSON_COLUMNS)
        row.update(event_id="row-count", event_epoch_us=1789607092576176,
                   operation="INSERT")
        write_ipc(self.path, [row])
        storage = EventStorage.__new__(EventStorage)
        storage.paths = ensure_data_dirs(Path(self.root.name) / "row-count")
        storage._part_body_locks = [threading.RLock() for _ in range(256)]
        with self.assertRaises(StorageError) as raised:
            storage.ingest_arrow_file(
                arrow_path=self.path,
                file_id="fixture-source",
                instance_id="rm-test000001",
                host_instance_id="fixture-host",
                source_file_name="mysql-bin.fixture",
                expected_rows=2,
                publish_metadata=False,
                append=True,
            )
        self.assertEqual(
            raised.exception.code,
            "PARSER_CHUNK_ROW_COUNT_MISMATCH",
        )
        self.assertEqual(list(storage.paths["events"].rglob("*.parquet")), [])

    def test_detached_transform_accepts_arrow_chunk_contract(self):
        row = dict.fromkeys(PARSER_JSON_COLUMNS)
        row.update(event_id="detached-arrow", event_epoch_us=1789607092576176,
                   operation="UPDATE", database_name="fixture")
        write_ipc(self.path, [row])
        data_root = Path(self.root.name) / "detached-arrow"
        count, parts = ingest_parser_file_detached({
            "data_dir": str(data_root),
            "file_id": "fixture-source",
            "instance_id": "rm-test000001",
            "host_instance_id": "fixture-host",
            "source_file_name": "mysql-bin.fixture",
            "parser_path": str(self.path),
            "parser_format": "arrow",
            "expected_rows": 1,
            "part_key": "000000",
        })
        self.assertEqual(count, 1)
        self.assertTrue(parts)
        self.assertTrue(all(Path(part["path"]).is_file() for part in parts))

    def test_ndjson_and_arrow_have_identical_47_field_parquet_and_catalog(self):
        row = {name: ("" if kind == "VARCHAR" else 0)
               for name, kind in PARSER_JSON_COLUMNS.items()}
        row.update(event_id="e1", event_epoch_us=1789607092576176,
                   header_epoch_us=1789606972000000, commit_epoch_us=1789607092576176,
                   raw_event_type="QueryEvent", operation="INSERT", database_name="Fixture",
                   table_name="表", table_map_id=2**64-1, schema_version_id="version",
                   server_id=77, thread_id=11, transaction_id="txn", gtid="gtid", xid="91",
                   start_position=324, end_position=480, row_index=0,
                   execution_time_ms=120000, sql_kind="ORIGINAL",
                   sql_text="INSERT INTO 表 VALUES ('中文🙂\\0')", sql_bytes_base64="AP8=",
                   before_json='{"x":null}', after_json='{"x":"9007199254740993"}',
                   columns_json='[{"name":"x","type":"DECIMAL"}]', row_query="original query",
                   txn_last_committed=8, txn_sequence_number=9, txn_length_bytes=21734,
                   connection_id="connection", connection_name="name", database_account="account",
                   execution_status="success", error_message="", affected_rows=2,
                   started_epoch_us=1789606972000000, finished_epoch_us=1789607092576176,
                   batch_id="batch", statement_index=2, transaction_context_id="context")
        second = dict(row, event_id="e2", event_epoch_us=1789693492576176,
                      commit_epoch_us=1789693492576176, operation="DELETE", row_index=1,
                      database_name="Other", table_name="B")
        third = {"sql_text": "SELECT 1", "event_epoch_us": 1789607092576176,
                 "end_position": 999, "operation": "SELECT"}
        rows = [second, third, row]
        ndjson = self.path.with_suffix(".ndjson")
        ndjson.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in rows), encoding="utf-8")
        write_ipc(self.path, rows, batch_size=1)
        outputs = []
        catalogs = []
        for mode in ("ndjson", "arrow"):
            storage = EventStorage.__new__(EventStorage)
            storage.paths = ensure_data_dirs(Path(self.root.name) / mode)
            storage._part_body_locks = [threading.RLock() for _ in range(256)]
            common = dict(file_id="fixture-source", instance_id="rm-test000001",
                          host_instance_id="fixture-host", source_file_name="mysql-bin.fixture",
                          publish_metadata=False, append=True)
            if mode == "ndjson":
                count, parts = storage.ingest_ndjson_file(ndjson_path=ndjson, **common)
            else:
                count, parts = storage.ingest_arrow_file(arrow_path=self.path, **common)
            self.assertEqual(count, len(rows))
            tables = [pq.ParquetFile(p["path"]).read() for p in parts]
            table = pa.concat_tables(tables)
            self.assertEqual(table.column_names, list(EVENT_COLUMNS))
            self.assertEqual(len(table.column_names), 47)
            outputs.append(table.to_pylist())
            catalogs.append([p["catalog"] for p in parts])
        self.assertEqual(outputs[0], outputs[1])
        self.assertEqual(catalogs[0], catalogs[1])
        first = next(r for r in outputs[1] if r["event_id"] == "e1")
        self.assertEqual(first["table_map_id"], 2**64 - 1)
        self.assertEqual(first["sql_bytes_base64"], "AP8=")
        self.assertEqual(first["columns_json"], row["columns_json"])


if __name__ == "__main__":
    unittest.main()
