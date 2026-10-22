"""1.34.3: what the API service holds at once — general log batches, Mongo windows and TLS contexts."""
from __future__ import annotations

import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from app import general_log_collector, rds_api
from app.general_log_collector import GeneralLogCollector, GeneralLogConfig
from app.mongo_collector import MongoCollector


class _Cursor:
    """An unbuffered pymysql cursor as the collector uses it: the probe answers, then the batch comes in chunks."""

    def __init__(self, rows: list[tuple]) -> None:
        self.rows = rows
        self.results: list[list[tuple]] = []
        self.description = None
        self.chunk_sizes: list[int] = []

    def __enter__(self) -> "_Cursor":
        return self

    def __exit__(self, *_exc) -> None:
        return None

    def execute(self, sql: str, _params=None) -> None:
        if sql == "SELECT 1":
            self.results = [[(1,)]]
        elif sql.startswith("SELECT MAX("):
            self.results = [[(time.time(),)]]
        else:
            self.description = [(name,) for name in ("epoch_us", "user_host", "thread_id", "server_id", "command_type", "sql_text")]
            self.results = [list(self.rows)]

    def fetchall(self) -> list[tuple]:
        rows, self.results = self.results[0], []
        return rows

    def fetchmany(self, size: int) -> list[tuple]:
        pending = self.results[0]
        chunk, self.results[0] = pending[:size], pending[size:]
        self.chunk_sizes.append(len(chunk))
        return chunk


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor
        self.cursor_classes: list[object] = []
        self.closed = False

    def cursor(self, cursor_class=None) -> _Cursor:
        self.cursor_classes.append(cursor_class)
        return self._cursor

    def close(self) -> None:
        self.closed = True


class GeneralLogBatchTests(unittest.TestCase):
    def test_the_default_batch_is_fifty_thousand_rows(self) -> None:
        self.assertEqual(general_log_collector._BATCH_ROW_LIMIT, 50_000)

    def test_rows_are_read_unbuffered_in_chunks_and_written_line_by_line(self) -> None:
        import pymysql.cursors

        rows = [(1_790_000_000_000_000 + number, "app[app] @ [10.0.0.8]", 7, 1, "Query", f"SELECT {number}")
                for number in range(12_001)]
        cursor = _Cursor(rows)
        connection = _Connection(cursor)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            storage = type("Storage", (), {"paths": {"root": root, "staging": root / "staging"}})()
            collector = GeneralLogCollector(None, storage, config=GeneralLogConfig({"instanceId": "rm-test", "enabled": True}))
            collector._connect = lambda _user, _password: connection  # type: ignore[method-assign]
            fetched = collector._fetch_rows(0)
            self.assertEqual(len(fetched), 12_001)
            self.assertEqual(fetched[-1]["sql_text"], "SELECT 12000")
            self.assertEqual(connection.cursor_classes, [pymysql.cursors.SSCursor])
            self.assertEqual(cursor.chunk_sizes, [5000, 5000, 2001, 0])
            self.assertTrue(connection.closed)

            written: list[str] = []

            class Metadata:
                def load_settings(self):
                    return rds_api_settings()

                def upsert_remote(self, _settings, _item):
                    return 1, "done"  # stop right after the file is written

            collector.metadata = Metadata()
            events, _max_epoch = collector._build_events(fetched[:3])
            original_unlink = Path.unlink

            def capture(path: Path, *args, **kwargs):
                written.append(path.read_text("utf-8"))
                return original_unlink(path, *args, **kwargs)

            with patch.object(Path, "unlink", capture):
                collector._ingest_batch(events)
            self.assertEqual(len(written), 1)
            lines = written[0].split("\n")
            self.assertEqual((len(lines), lines[-1]), (4, ""))  # three events, one per line, newline terminated


def rds_api_settings():
    from app.config import Settings

    return Settings()


class MongoWindowTests(unittest.TestCase):
    def test_the_live_and_history_lanes_hold_one_window_at_a_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            collector = MongoCollector(type("Store", (), {"root": Path(directory)})(), dict(instanceId="dds-example"),
                                       lambda: None)
            inside = 0
            most = 0
            guard = threading.Lock()

            def window(_start, _end):
                nonlocal inside, most
                with guard:
                    inside += 1
                    most = max(most, inside)
                time.sleep(0.05)
                with guard:
                    inside -= 1
                return {"records": 0}

            collector._slow_window = window  # type: ignore[method-assign]
            threads = [threading.Thread(target=collector.slow_window, args=(number, number + 1)) for number in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(most, 1)


class RetentionSweepTests(unittest.TestCase):
    def test_parts_are_streamed_in_list_order_and_the_sweep_never_materializes_them(self) -> None:
        from app.metadata import MetadataStore
        from app.storage import EventStorage
        from tests.test_sqlite_serving_performance import insert_fixture

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = MetadataStore(root / "metadata.sqlite3")
            storage = EventStorage(store, root)
            now_us = int(time.time() * 1_000_000)
            with store.connection() as conn:
                conn.execute("BEGIN IMMEDIATE")
                insert_fixture(conn, "binlog_files", [
                    {"id": "visible", "instance_id": "rm-test", "log_file_name": "mysql-bin.1", "query_visible": 1},
                    {"id": "hidden", "instance_id": "rm-test", "log_file_name": "mysql-bin.2", "query_visible": 0}])
                parts = []
                for number in range(30):
                    path = storage.paths["events"] / f"part-{number:02d}.parquet"
                    path.touch()
                    parts.append({"path": str(path), "logical_part_id": f"part-{number}",
                                  "binlog_id": "hidden" if number % 3 == 0 else "visible",
                                  "event_date": f"2026-10-{1 + number % 20:02d}",
                                  "min_event_epoch_us": now_us, "max_event_epoch_us": now_us + 1, "row_count": 1})
                insert_fixture(conn, "parquet_parts", parts)
                conn.commit()
            self.assertEqual(list(store.iter_parts(batch=7)), store.list_parts(limit=1000))
            self.assertEqual(list(store.iter_parts(visible_only=False, batch=7)),
                             store.list_parts(limit=1000, visible_only=False))
            self.assertEqual(sorted(store.iter_part_paths(batch=4)), sorted(part["path"] for part in parts))
            with patch.object(store, "list_parts", side_effect=AssertionError("the sweep materialized every part")):
                result = storage.cleanup(30)
            self.assertEqual((result["deleted_parts"], result["errors"]), (0, []))


class SharedTlsContextTests(unittest.TestCase):
    def test_every_caller_gets_the_one_verifying_context(self) -> None:
        import ssl

        first = rds_api.shared_ssl_context()
        self.assertIs(first, rds_api.shared_ssl_context())
        self.assertEqual(first.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(first.check_hostname)


if __name__ == "__main__":
    unittest.main()
