from __future__ import annotations

import hashlib
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from app.clickhouse_manifest import ClickHouseManifest


def _part(path: Path, identity: str, *, offset: int = 0) -> dict[str, object]:
    now_us = int(datetime.now(UTC).timestamp() * 1_000_000) + offset
    return {
        "path": str(path),
        "logical_part_id": identity,
        "sha256": hashlib.sha256(identity.encode()).hexdigest(),
        "content_revision": 1,
        "min_event_epoch_us": now_us - 60_000_000,
        "max_event_epoch_us": now_us,
        "row_count": 3,
        "size_bytes": 9,
    }


class StreamingManifestTests(unittest.TestCase):
    def _ready(self, manifest: ClickHouseManifest, parts: list[dict[str, object]]) -> None:
        start = min(int(p["min_event_epoch_us"]) for p in parts)
        end = max(int(p["max_event_epoch_us"]) for p in parts)
        manifest.reconcile(parts, start_epoch_us=start, end_epoch_us=end)
        for part in parts:
            job = manifest.claim_next()
            self.assertIsNotNone(job)
            manifest.mark_ready(str(job["part_path"]), str(job["logical_part_id"]), 3)

    def test_full_sweep_keeps_unchanged_and_deletes_unseen(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            manifest = ClickHouseManifest(Path(temp) / "manifest.sqlite3", run_migrations=True)
            kept = _part(Path(temp) / "kept.parquet", "kept-v1")
            gone = _part(Path(temp) / "gone.parquet", "gone-v1", offset=1)
            self._ready(manifest, [kept, gone])
            with manifest.connection() as connection:
                before = connection.execute(
                    "SELECT updated_at_us FROM clickhouse_parts WHERE logical_part_id='kept-v1'"
                ).fetchone()[0]
            result = manifest.reconcile_streaming(
                iter([kept]),
                start_epoch_us=int(kept["min_event_epoch_us"]),
                end_epoch_us=int(gone["max_event_epoch_us"]),
            )
            with manifest.connection() as connection:
                rows = dict(connection.execute(
                    "SELECT logical_part_id, status || ':' || updated_at_us FROM clickhouse_parts"
                ).fetchall())
            self.assertEqual(rows["kept-v1"], f"ready:{before}")
            self.assertTrue(rows["gone-v1"].startswith("delete_pending:"))
            self.assertEqual(result["queued_parts"], 0)
            self.assertEqual(result["missing_deletes"], 1)

    def test_replacement_queues_old_identity_and_new_load(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            manifest = ClickHouseManifest(Path(temp) / "manifest.sqlite3", run_migrations=True)
            first = _part(Path(temp) / "same.parquet", "v1")
            self._ready(manifest, [first])
            replacement = {**first, "logical_part_id": "v2", "sha256": "replacement"}
            result = manifest.reconcile_streaming(
                iter([replacement]),
                start_epoch_us=int(first["min_event_epoch_us"]),
                end_epoch_us=int(first["max_event_epoch_us"]),
            )
            self.assertEqual(result["replacement_deletes"], 1)
            delete = manifest.claim_next()
            self.assertEqual((delete["logical_part_id"], delete["job_kind"]), ("v1", "delete"))
            manifest.mark_retired(str(first["path"]), "v1")
            load = manifest.claim_next()
            self.assertEqual((load["logical_part_id"], load["job_kind"]), ("v2", "load"))


if __name__ == "__main__":
    unittest.main()
