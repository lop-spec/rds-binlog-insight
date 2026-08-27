from __future__ import annotations

import json
import tempfile
import threading
import unittest
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.config import Settings
from app.metadata import MetadataStore
from app.server import AppHTTPServer
from app.storage import EventStorage


class LocalExecutionDetailTests(unittest.TestCase):
    def test_dbx_and_tabularis_history_ignore_body_retention_and_parquet(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            metadata = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(metadata, data_root)
            instance_id = "rm-test000001"
            settings = Settings(
                db_instance_id=instance_id,
                retention_days=60,
                auto_sync=False,
            )
            old_epoch_us = int(
                (
                    datetime.now(UTC)
                    - timedelta(days=settings.retention_days + 1)
                ).timestamp()
                * 1_000_000
            )
            expected_sql = {
                "dbx-hist:connection:statement": "SELECT 'dbx history detail'",
                "tabularis-hist:connection:statement": (
                    "UPDATE audit_table SET source = 'tabularis history detail'"
                ),
            }
            metadata.record_tabularis_audit_log(
                [
                    {
                        "event_id": event_id,
                        "event_epoch_us": old_epoch_us + index,
                        "instance_id": instance_id,
                        "connection_name": "DBX" if event_id.startswith("dbx-") else "Tabularis",
                        "execution_status": "success",
                        "operation": "SELECT" if event_id.startswith("dbx-") else "UPDATE",
                        "sql_kind": "SQL",
                        "sql_text": sql_text,
                        "started_epoch_us": old_epoch_us + index,
                        "finished_epoch_us": old_epoch_us + index,
                        "source_file_name": f"tabularis-audit-{event_id}.ndjson",
                    }
                    for index, (event_id, sql_text) in enumerate(expected_sql.items())
                ]
            )
            with patch.object(
                storage,
                "_event_detail_tiered_impl",
                side_effect=AssertionError("permanent local detail must not read Parquet"),
            ):
                for event_id, sql_text in expected_sql.items():
                    with self.subTest(event_id=event_id):
                        detail = storage.event_detail_tiered(
                            event_id,
                            settings,
                            None,
                            "",
                            instance_id,
                        )
                        self.assertIsNotNone(detail)
                        self.assertEqual(detail["sql_text"], sql_text)
                        self.assertEqual(detail["instance_id"], instance_id)
                        self.assertEqual(detail["raw_event_type"], "TABULARIS_AUDIT")
                        self.assertEqual(detail["tiers_used"], ["audit-index"])
                        self.assertLess(
                            int(detail["event_epoch_us"]),
                            int(
                                (
                                    datetime.now(UTC)
                                    - timedelta(days=settings.retention_days)
                                ).timestamp()
                                * 1_000_000
                            ),
                        )

    def test_http_detail_uses_permanent_local_index_before_oss(self) -> None:
        settings = Settings(db_instance_id="rm-test000001", auto_sync=False)
        event_id = "dbx-hist:connection:statement"

        class Metadata:
            @staticmethod
            def load_settings() -> Settings:
                return settings

        class Storage:
            @staticmethod
            def local_execution_event_detail(requested_id: str, instance: str):
                return {
                    "event_id": requested_id,
                    "instance_id": instance,
                    "sql_text": "SELECT 'permanent DBX detail'",
                    "tiers_used": ["audit-index"],
                }

            @staticmethod
            def slowlog_event_detail(*_args, **_kwargs):
                raise AssertionError("local execution detail must be checked first")

            @staticmethod
            def event_detail_tiered(*_args, **_kwargs):
                raise AssertionError("local execution detail must not enter fallback")

        class Sync:
            @staticmethod
            def archive_for_settings(_settings):
                raise AssertionError("local execution detail must not initialize OSS")

        application = SimpleNamespace(
            metadata=Metadata(),
            storage=Storage(),
            sync=Sync(),
        )
        httpd = AppHTTPServer(("127.0.0.1", 0), application)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            query = urllib.parse.urlencode(
                {"id": event_id, "instance": settings.db_instance_id}
            )
            url = f"http://127.0.0.1:{httpd.server_address[1]}/api/event?{query}"
            with urllib.request.urlopen(url, timeout=2) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["data"]["event_id"], event_id)
            self.assertEqual(
                payload["data"]["sql_text"],
                "SELECT 'permanent DBX detail'",
            )
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
