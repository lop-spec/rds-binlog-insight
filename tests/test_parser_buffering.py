from __future__ import annotations

import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from app.parser_bridge import (
    NATIVE_CHUNK_MAX_BYTES,
    NATIVE_CHUNK_MAX_OUTSTANDING,
    ParserError,
    parse_ndjson_chunks_buffered,
)


class ParserBufferingTests(unittest.TestCase):
    def test_default_staging_bound_fits_one_gibibyte_tmpfs(self) -> None:
        self.assertEqual(NATIVE_CHUNK_MAX_OUTSTANDING, 2)
        self.assertLessEqual(
            NATIVE_CHUNK_MAX_BYTES * NATIVE_CHUNK_MAX_OUTSTANDING,
            768 * 1024 * 1024,
        )

    def test_does_not_ack_beyond_one_ready_chunk(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.binlog"
            source.write_bytes(b"test")
            staging = root / "staging"
            command = [
                sys.executable,
                "-c",
                (
                    "import json, os, sys\n"
                    "from pathlib import Path\n"
                    f"root = Path({str(staging.resolve())!r})\n"
                    "root.mkdir(parents=True, exist_ok=True)\n"
                    "for index in range(3):\n"
                    " final = root / f'file-bounded-{index:06d}.ndjson'\n"
                    " partial = Path(str(final) + '.part')\n"
                    " payload = (json.dumps({'value': index}) + '\\n').encode()\n"
                    " partial.write_bytes(payload)\n"
                    " os.replace(partial, final)\n"
                    " print(json.dumps({'path': str(final.resolve()), 'rows': 1, 'bytes': len(payload)}), flush=True)\n"
                    " if sys.stdin.readline().strip() != 'ok': raise SystemExit(2)\n"
                ),
            ]
            with patch("app.parser_bridge._parser_command", return_value=command):
                chunks = parse_ndjson_chunks_buffered(
                    source,
                    "file-bounded",
                    staging,
                    max_lines=1,
                    max_bytes=1024,
                    max_prefetch=1,
                )
                first = next(chunks)
                second = staging / "file-bounded-000001.ndjson"
                third = staging / "file-bounded-000002.ndjson"
                deadline = time.monotonic() + 2
                while not second.is_file() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(second.is_file())
                time.sleep(0.1)
                third_started_early = third.exists()
                remaining = list(chunks)

            self.assertFalse(third_started_early)
            self.assertEqual(
                [first.name, *(path.name for path in remaining)],
                [
                    "file-bounded-000000.ndjson",
                    "file-bounded-000001.ndjson",
                    "file-bounded-000002.ndjson",
                ],
            )

    def test_retry_cleans_all_orphaned_native_chunks_before_start(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.binlog"
            source.write_bytes(b"test")
            staging = root / "staging"
            staging.mkdir()
            stale_final = staging / "older-file-000005.ndjson"
            stale_partial = staging / "older-file-000006.ndjson.part"
            unrelated = staging / "keep-me.ndjson"
            stale_final.write_bytes(b"stale")
            stale_partial.write_bytes(b"partial")
            unrelated.write_bytes(b"keep")
            command = [
                sys.executable,
                "-c",
                (
                    "import json, sys\n"
                    "from pathlib import Path\n"
                    f"root = Path({str(staging.resolve())!r})\n"
                    "stale = sorted(root.glob('*-??????.ndjson*'))\n"
                    "if stale:\n"
                    " print(','.join(path.name for path in stale), file=sys.stderr)\n"
                    " raise SystemExit(4)\n"
                    "final = root / 'file-retry-000000.ndjson'\n"
                    "payload = b'{\\\"event\\\": 1}\\n'\n"
                    "final.write_bytes(payload)\n"
                    "print(json.dumps({'path': str(final.resolve()), 'rows': 1, 'bytes': len(payload)}), flush=True)\n"
                    "if sys.stdin.readline().strip() != 'ok': raise SystemExit(2)\n"
                ),
            ]

            with patch("app.parser_bridge._parser_command", return_value=command):
                chunks = list(
                    parse_ndjson_chunks_buffered(
                        source,
                        "file-retry",
                        staging,
                        max_lines=1,
                        max_bytes=1024,
                        max_prefetch=1,
                    )
                )

            self.assertFalse(stale_final.exists())
            self.assertFalse(stale_partial.exists())
            self.assertTrue(unrelated.exists())
            self.assertEqual([path.name for path in chunks], ["file-retry-000000.ndjson"])
            for path in chunks:
                path.unlink(missing_ok=True)

    def test_failed_parser_removes_all_matching_staging_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.binlog"
            source.write_bytes(b"test")
            staging = root / "staging"
            command = [
                sys.executable,
                "-c",
                (
                    "import sys\n"
                    "from pathlib import Path\n"
                    f"root = Path({str(staging.resolve())!r})\n"
                    "root.mkdir(parents=True, exist_ok=True)\n"
                    "(root / 'file-failed-000005.ndjson').write_bytes(b'stale')\n"
                    "(root / 'file-failed-000006.ndjson.part').write_bytes(b'partial')\n"
                    "print('forced parser failure', file=sys.stderr)\n"
                    "raise SystemExit(3)\n"
                ),
            ]

            with (
                patch("app.parser_bridge._parser_command", return_value=command),
                self.assertRaises(ParserError),
            ):
                list(
                    parse_ndjson_chunks_buffered(
                        source,
                        "file-failed",
                        staging,
                        max_lines=1,
                        max_bytes=1024,
                        max_prefetch=1,
                    )
                )

            self.assertEqual(list(staging.glob("*-??????.ndjson*")), [])


if __name__ == "__main__":
    unittest.main()
