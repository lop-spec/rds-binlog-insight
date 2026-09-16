from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

from app.catalog_store import CatalogStore
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


class CatalogWalLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "catalog.sqlite3"
        migration_owner = CatalogStore(self.path)
        getattr(migration_owner, "close", lambda: None)()
        self.store = CatalogStore(self.path, run_migrations=False)
        self.addCleanup(lambda: getattr(self.store, "close", lambda: None)())

    @staticmethod
    def entry(index: int, revision: int = 1) -> dict:
        return {
            "path": f"part-{index}.parquet", "sha256": f"{index:064x}",
            "content_revision": revision,
            "catalog": {"databases": ["business"], "tables": ["events"],
                        "operations": ["insert"]},
            "indexed_at": "2026-01-01T00:00:00Z",
        }

    def test_live_catalog_does_not_checkpoint_and_delete_wal_per_batch(self) -> None:
        wal = Path(f"{self.path}-wal")
        sizes = []
        for index in range(32):
            self.store.upsert_many([self.entry(index)])
            self.assertTrue(wal.is_file(), "live catalog lost WAL after a batch")
            sizes.append(wal.stat().st_size)
        self.assertGreater(sizes[-1], sizes[0])
        self.assertEqual(len(self.store.catalogs([self.entry(i)["path"] for i in range(32)])), 32)

    def test_anchor_does_not_weaken_durability_or_pin_a_snapshot(self) -> None:
        self.store.upsert_many([self.entry(1)])
        with self.store.connection() as conn:
            self.assertEqual(conn.execute("PRAGMA synchronous").fetchone()[0], 2)
            self.assertEqual(conn.execute("PRAGMA wal_autocheckpoint").fetchone()[0], 1000)
            self.assertEqual(tuple(conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()), (0, 0, 0))
        self.assertFalse(self.store._wal_anchor.in_transaction)
        with self.assertRaisesRegex(sqlite3.OperationalError, "readonly"):
            self.store._wal_anchor.execute("DELETE FROM catalogs")
        self.assertEqual(self.store.catalogs(["part-1.parquet"])["part-1.parquet"]["tables"], ["events"])

    def test_runtime_reopen_and_cross_thread_close_are_safe(self) -> None:
        self.store.upsert_many([self.entry(1)])
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(self.store.close).result(timeout=5)
        self.store.close()
        self.assertFalse(Path(f"{self.path}-wal").exists())
        self.store = CatalogStore(self.path, run_migrations=False)
        self.assertTrue(Path(f"{self.path}-wal").exists())
        self.assertIn("part-1.parquet", self.store.catalogs(["part-1.parquet"]))

    def test_failed_batch_rolls_back_and_releases_writer(self) -> None:
        original = self.store._upsert_encoded
        def fail_after_insert(conn, rows):
            original(conn, rows)
            raise RuntimeError("injected transaction failure")
        with mock.patch.object(self.store, "_upsert_encoded", side_effect=fail_after_insert):
            with self.assertRaisesRegex(RuntimeError, "injected"):
                self.store.upsert_many([self.entry(1), self.entry(2)])
        self.assertEqual(self.store.catalogs(["part-1.parquet", "part-2.parquet"]), {})
        self.store.upsert_many([self.entry(1, revision=2)])
        self.store.upsert_many([self.entry(1, revision=1)])
        self.assertEqual(self.store.catalogs(["part-1.parquet"])["part-1.parquet"]["content_revision"], 2)

    def test_committed_catalog_survives_abrupt_worker_exit(self) -> None:
        self.store.close()
        result = subprocess.run(
            [sys.executable, "-c",
             "import json,os,sys;from pathlib import Path;"
             "from app.catalog_store import CatalogStore;"
             "s=CatalogStore(Path(sys.argv[1]),run_migrations=False);"
             "s.upsert_many([json.loads(sys.argv[2])]);os._exit(73)",
             str(self.path), json.dumps(self.entry(1))],
            capture_output=True, text=True, timeout=10,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        self.assertEqual(result.returncode, 73, result.stderr)
        self.store = CatalogStore(self.path, run_migrations=False)
        self.assertEqual(self.store.catalogs(["part-1.parquet"])["part-1.parquet"]["tables"], ["events"])

    def test_runtime_rejects_non_wal_without_reconfiguring_database(self) -> None:
        self.store.close()
        conn = sqlite3.connect(self.path, isolation_level=None)
        try:
            self.assertEqual(conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0], "delete")
        finally:
            conn.close()
        with self.assertRaisesRegex(RuntimeError, "requires journal_mode=wal"):
            CatalogStore(self.path, run_migrations=False)
        conn = sqlite3.connect(self.path)
        try:
            self.assertEqual(conn.execute("PRAGMA journal_mode").fetchone()[0], "delete")
        finally:
            conn.close()

    def test_metadata_owner_closes_both_anchors(self) -> None:
        metadata_path = self.path.parent / "metadata.sqlite3"
        migration_owner = MetadataStore(metadata_path)
        migration_owner.close()
        store = MetadataStore(metadata_path, run_migrations=False)
        try:
            self.assertIsNotNone(store._wal_anchor)
            self.assertIsNotNone(store.catalog_store._wal_anchor)
            store.catalog_store.upsert_many([self.entry(1)])
            store.close()
            self.assertIsNone(store._wal_anchor)
            self.assertIsNone(store.catalog_store._wal_anchor)
            store.close()
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main()
