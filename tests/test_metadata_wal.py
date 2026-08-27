from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.config import Settings
from app.metadata import MetadataStore
from app.rds_api import RemoteBinlog


class MetadataWalLifecycleTests(unittest.TestCase):
    def test_store_keeps_wal_open_until_explicit_close(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metadata.sqlite3"
            migration_owner = MetadataStore(path)
            migration_owner.close()
            store = MetadataStore(path, run_migrations=False)
            store.save_settings(Settings(db_instance_id="rm-prod"))

            self.assertTrue(
                Path(f"{path}-wal").is_file(),
                "a live MetadataStore must prevent last-connection WAL cleanup",
            )
            with store.connection() as conn:
                self.assertEqual(
                    str(conn.execute("PRAGMA journal_mode").fetchone()[0]),
                    "wal",
                )

            store.close()

    def test_archive_batch_is_atomic(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = MetadataStore(root / "metadata.sqlite3")
            settings = Settings(db_instance_id="rm-prod")
            remote = RemoteBinlog(
                log_file_name="slow-log/rm-prod/archive-batch",
                log_begin_utc="2026-08-21T00:00:00Z",
                log_end_utc="2026-08-21T01:00:00Z",
                file_size=2,
                checksum_crc64="",
                download_link="",
                intranet_download_link="",
                link_expired_utc="",
                remote_status="Completed",
                host_instance_id="slow-log",
            )
            file_id, _ = store.upsert_remote(settings, remote)
            parts = [
                {
                    "path": str(root / f"archive-{index}.parquet"),
                    "logical_part_id": f"archive-{index}",
                    "sha256": f"{index + 1:064x}",
                    "object_sha256": f"{index + 1:064x}",
                    "row_count": 1,
                    "min_event_epoch_us": 1_787_286_000_000_000 + index,
                    "max_event_epoch_us": 1_787_286_000_000_000 + index,
                    "event_date": "2026-08-21",
                    "size_bytes": 10,
                }
                for index in range(2)
            ]
            store.replace_parts(file_id, parts)

            updates = [
                {
                    "path": str(part["path"]),
                    "oss_key": f"archive/{index}",
                    "oss_etag": f"etag-{index}",
                    "oss_offset": index * 10,
                    "oss_length": 10,
                    "oss_object_sha256": f"pack-{index}",
                }
                for index, part in enumerate(parts)
            ]
            with self.assertRaisesRegex(RuntimeError, "missing"):
                store.mark_parts_archived(
                    [updates[0], {**updates[1], "path": str(root / "missing")}]
                )
            self.assertTrue(
                all(
                    not str(store.part_by_path(str(part["path"]))["oss_key"])
                    for part in parts
                )
            )

            store.mark_parts_archived(updates)
            committed = [
                store.part_by_path(str(part["path"])) for part in parts
            ]
            self.assertEqual(
                [str(part["oss_key"]) for part in committed],
                ["archive/0", "archive/1"],
            )
            store.close()


if __name__ == "__main__":
    unittest.main()
