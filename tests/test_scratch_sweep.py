from __future__ import annotations

import os
import tempfile
import time
import unittest
from pathlib import Path

from app.scratch_sweep import STAMP_NAME, sweep_stale_scratch


def _write(path: Path, size: int, age_seconds: float, now: float) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    os.utime(path, (now - age_seconds, now - age_seconds))
    return path


class ScratchSweepTests(unittest.TestCase):
    def test_removes_only_stale_leftovers_of_known_kinds(self):
        now = time.time()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old_body = _write(root / ".analytics-aaa.parquet", 100, 30 * 3600, now)
            old_partial = _write(root / "..analytics-bbb.parquet.0123456789ab.part", 50, 30 * 3600, now)
            live_body = _write(root / ".analytics-ccc.parquet", 100, 60, now)
            foreign = _write(root / "keep-me.parquet", 100, 30 * 3600, now)
            old_lane = _write(root / "binlog-rows" / "lane0-old" / "source.binlog", 400, 30 * 3600, now)
            os.utime(old_lane.parent, (now - 30 * 3600, now - 30 * 3600))
            busy_lane = _write(root / "binlog-rows" / "lane1-busy" / "source.binlog", 400, 30 * 3600, now)
            _write(root / "binlog-rows" / "lane1-busy" / "parser.stderr", 1, 10, now)  # still being written

            result = sweep_stale_scratch(root, now=now)

            self.assertEqual((result["files"], result["directories"], result["bytes"]), (2, 1, 550))
            self.assertFalse(old_body.exists())
            self.assertFalse(old_partial.exists())
            self.assertFalse(old_lane.parent.exists())
            self.assertTrue(live_body.exists())
            self.assertTrue(foreign.exists())
            self.assertTrue(busy_lane.exists())

    def test_runs_at_most_once_per_interval(self):
        now = time.time()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.assertFalse(sweep_stale_scratch(root, now=now)["skipped"])
            self.assertTrue((root / STAMP_NAME).exists())
            stale = _write(root / ".query-ddd.parquet", 10, 30 * 3600, now)
            self.assertTrue(sweep_stale_scratch(root, now=now + 60)["skipped"])
            self.assertTrue(stale.exists())
            self.assertEqual(sweep_stale_scratch(root, now=now + 3601)["files"], 1)
            self.assertEqual(sweep_stale_scratch(root, now=now + 3602, min_interval_seconds=0)["files"], 0)

    def test_missing_scratch_is_a_no_op(self):
        result = sweep_stale_scratch(Path(tempfile.gettempdir()) / "absent-scratch-for-sweep-test")
        self.assertEqual((result["files"], result["directories"], result["errors"]), (0, 0, []))


if __name__ == "__main__":
    unittest.main()
