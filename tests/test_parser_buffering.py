from __future__ import annotations

import os
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from app.parser_bridge import (
    NATIVE_CHUNK_MAX_BYTES,
    NATIVE_CHUNK_MAX_OUTSTANDING,
    PARSER_TRANSPORT_ARROW,
    ParserError,
    _parser_command,
    parse_ndjson_chunks_buffered,
    parse_parser_chunks_buffered,
    parser_transport_format,
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
                    " manifest = {'protocol': 'parser-chunk-v1', 'format': 'ndjson-v1', "
                    "'sequence': index, 'path': str(final.resolve()), 'rows': 1, "
                    "'bytes': len(payload)}\n"
                    " print(json.dumps(manifest), flush=True)\n"
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
            stale_arrow = staging / "older-file-000007.arrow"
            stale_arrow_partial = staging / "older-file-000008.arrow.part"
            unrelated = staging / "keep-me.ndjson"
            stale_final.write_bytes(b"stale")
            stale_partial.write_bytes(b"partial")
            stale_arrow.write_bytes(b"stale-arrow")
            stale_arrow_partial.write_bytes(b"partial-arrow")
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
            self.assertFalse(stale_arrow.exists())
            self.assertFalse(stale_arrow_partial.exists())
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

    def test_arrow_chunks_use_strict_manifest_ack_and_same_outstanding_bound(self) -> None:
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
                    " final = root / f'file-arrow-{index:06d}.arrow'\n"
                    " partial = Path(str(final) + '.part')\n"
                    " payload = b'ARROW' + bytes([index])\n"
                    " partial.write_bytes(payload)\n"
                    " os.replace(partial, final)\n"
                    " manifest = {'protocol': 'parser-chunk-v1', 'format': 'arrow-ipc-file-v1', "
                    "'sequence': index, 'path': str(final.resolve()), 'rows': 1, "
                    "'bytes': len(payload), 'decoded_bytes': 100}\n"
                    " print(json.dumps(manifest, separators=(',', ':')), flush=True)\n"
                    " ack = json.loads(sys.stdin.readline())\n"
                    " expected = {'protocol': 'parser-chunk-ack-v1', 'sequence': index, 'status': 'ok'}\n"
                    " if ack != expected: raise SystemExit(2)\n"
                ),
            ]
            with patch("app.parser_bridge._parser_command", return_value=command):
                chunks = parse_parser_chunks_buffered(
                    source,
                    "file-arrow",
                    staging,
                    chunk_format="arrow",
                    max_lines=1,
                    max_bytes=1024,
                    max_prefetch=1,
                )
                first = next(chunks)
                second = staging / "file-arrow-000001.arrow"
                third = staging / "file-arrow-000002.arrow"
                deadline = time.monotonic() + 2
                while not second.is_file() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(second.is_file())
                time.sleep(0.1)
                third_started_early = third.exists()
                remaining = list(chunks)

            self.assertFalse(third_started_early)
            actual = [first, *remaining]
            self.assertEqual([chunk.sequence for chunk in actual], [0, 1, 2])
            self.assertTrue(all(chunk.transport_format == "arrow" for chunk in actual))
            self.assertEqual([chunk.rows for chunk in actual], [1, 1, 1])
            for chunk in actual:
                chunk.path.unlink(missing_ok=True)

    def test_arrow_parser_failure_after_ack_cleans_final_and_partial(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.binlog"
            source.write_bytes(b"test")
            staging = root / "staging"
            command = [
                sys.executable,
                "-c",
                (
                    "import json, sys\n"
                    "from pathlib import Path\n"
                    f"root = Path({str(staging.resolve())!r})\n"
                    "root.mkdir(parents=True, exist_ok=True)\n"
                    "final = root / 'file-mid-failure-000000.arrow'\n"
                    "payload = b'ARROW'\n"
                    "final.write_bytes(payload)\n"
                    "manifest = {'protocol': 'parser-chunk-v1', "
                    "'format': 'arrow-ipc-file-v1', 'sequence': 0, "
                    "'path': str(final.resolve()), 'rows': 1, "
                    "'bytes': len(payload), 'decoded_bytes': 100}\n"
                    "print(json.dumps(manifest), flush=True)\n"
                    "json.loads(sys.stdin.readline())\n"
                    "(root / 'file-mid-failure-000001.arrow.part').write_bytes(b'partial')\n"
                    "print('forced parser failure after ACK', file=sys.stderr)\n"
                    "raise SystemExit(3)\n"
                ),
            ]
            with patch("app.parser_bridge._parser_command", return_value=command):
                with self.assertRaises(ParserError) as raised:
                    list(parse_parser_chunks_buffered(
                        source,
                        "file-mid-failure",
                        staging,
                        chunk_format="arrow",
                        max_lines=1,
                        max_bytes=1024,
                    ))
            self.assertEqual(raised.exception.code, "PARSER_FAILED")
            self.assertEqual(
                list(staging.glob("file-mid-failure-*.arrow*")), []
            )

    def test_arrow_consumer_close_cancels_parser_and_cleans_owned_lane(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.binlog"
            source.write_bytes(b"test")
            staging = root / "staging"
            command = [
                sys.executable,
                "-c",
                (
                    "import json, sys\n"
                    "from pathlib import Path\n"
                    f"root = Path({str(staging.resolve())!r})\n"
                    "root.mkdir(parents=True, exist_ok=True)\n"
                    "for index in range(100):\n"
                    " final = root / f'file-close-{index:06d}.arrow'\n"
                    " payload = b'ARROW' + bytes([index % 256])\n"
                    " final.write_bytes(payload)\n"
                    " manifest = {'protocol': 'parser-chunk-v1', "
                    "'format': 'arrow-ipc-file-v1', 'sequence': index, "
                    "'path': str(final.resolve()), 'rows': 1, "
                    "'bytes': len(payload), 'decoded_bytes': 100}\n"
                    " print(json.dumps(manifest), flush=True)\n"
                    " ack = json.loads(sys.stdin.readline())\n"
                    " if ack.get('sequence') != index: raise SystemExit(2)\n"
                ),
            ]
            with patch("app.parser_bridge._parser_command", return_value=command):
                chunks = parse_parser_chunks_buffered(
                    source,
                    "file-close",
                    staging,
                    chunk_format="arrow",
                    max_lines=1,
                    max_bytes=1024,
                    max_prefetch=1,
                )
                first = next(chunks)
                self.assertEqual(first.sequence, 0)
                deadline = time.monotonic() + 2
                second = staging / "file-close-000001.arrow"
                while not second.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertTrue(second.exists())
                chunks.close()

            self.assertEqual(list(staging.glob("file-close-*.arrow*")), [])

    def test_manifest_stream_is_line_framed_and_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.binlog"
            source.write_bytes(b"test")
            staging = root / "staging"
            command = [
                sys.executable,
                "-c",
                "import sys; sys.stdout.buffer.write(b'x' * 65537); sys.stdout.flush()",
            ]
            with patch("app.parser_bridge._parser_command", return_value=command):
                with self.assertRaises(ParserError) as raised:
                    list(parse_parser_chunks_buffered(
                        source,
                        "file-line-bound",
                        staging,
                        chunk_format="arrow",
                        max_lines=1,
                        max_bytes=1024,
                    ))
            self.assertEqual(raised.exception.code, "PARSER_OUTPUT_READ_FAILED")
            self.assertEqual(list(staging.glob("file-line-bound-*.arrow*")), [])

    def test_arrow_manifest_rejects_unknown_fields_and_cleans_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.binlog"
            source.write_bytes(b"test")
            staging = root / "staging"
            command = [
                sys.executable,
                "-c",
                (
                    "import json, sys\n"
                    "from pathlib import Path\n"
                    f"root = Path({str(staging.resolve())!r})\n"
                    "root.mkdir(parents=True, exist_ok=True)\n"
                    "final = root / 'file-invalid-000000.arrow'\n"
                    "final.write_bytes(b'ARROW')\n"
                    "manifest = {'protocol': 'parser-chunk-v1', 'format': 'arrow-ipc-file-v1', "
                    "'sequence': 0, 'path': str(final.resolve()), 'rows': 1, 'bytes': 5, "
                    "'decoded_bytes': 100, 'unexpected': True}\n"
                    "print(json.dumps(manifest), flush=True)\n"
                    "sys.stdin.readline()\n"
                ),
            ]
            with patch("app.parser_bridge._parser_command", return_value=command):
                with self.assertRaises(ParserError) as raised:
                    list(parse_parser_chunks_buffered(
                        source,
                        "file-invalid",
                        staging,
                        chunk_format="arrow",
                        max_lines=1,
                        max_bytes=1024,
                    ))
            self.assertEqual(raised.exception.code, "PARSER_CHUNK_MANIFEST_INVALID")
            self.assertEqual(list(staging.glob("file-invalid-*.arrow*")), [])

    def test_unversioned_ndjson_manifest_requires_exact_legacy_shape(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.binlog"
            source.write_bytes(b"test")
            staging = root / "staging"
            command = [
                sys.executable,
                "-c",
                (
                    "import json, sys\n"
                    "from pathlib import Path\n"
                    f"root = Path({str(staging.resolve())!r})\n"
                    "root.mkdir(parents=True, exist_ok=True)\n"
                    "final = root / 'file-legacy-invalid-000000.ndjson'\n"
                    "final.write_bytes(b'{}\\n')\n"
                    "manifest = {'path': str(final.resolve()), 'rows': 1, "
                    "'bytes': 3, 'unexpected': True}\n"
                    "print(json.dumps(manifest), flush=True)\n"
                    "sys.stdin.readline()\n"
                ),
            ]
            with patch("app.parser_bridge._parser_command", return_value=command):
                with self.assertRaises(ParserError) as raised:
                    list(parse_ndjson_chunks_buffered(
                        source,
                        "file-legacy-invalid",
                        staging,
                        max_lines=1,
                        max_bytes=1024,
                    ))
            self.assertEqual(raised.exception.code, "PARSER_CHUNK_MANIFEST_INVALID")
            self.assertEqual(list(staging.glob("file-legacy-invalid-*.ndjson*")), [])

    def test_source_file_id_cannot_escape_the_staging_lane(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.binlog"
            source.write_bytes(b"test")
            staging = root / "staging"
            invalid_values = (
                "../escape", "C:escape", "bad\nid", "设备", "..",
                " trailing", "x" * 129,
            )
            with patch("app.parser_bridge._parser_command") as command:
                for value in invalid_values:
                    with self.subTest(value=value):
                        with self.assertRaises(ParserError) as raised:
                            list(parse_parser_chunks_buffered(
                                source,
                                value,
                                staging,
                                chunk_format="arrow",
                                max_lines=1,
                                max_bytes=1024,
                            ))
                        self.assertEqual(
                            raised.exception.code, "PARSER_SOURCE_FILE_ID_INVALID"
                        )
            command.assert_not_called()
            self.assertFalse(staging.exists())
            self.assertFalse((root / "escape.native.stderr").exists())

    def test_ndjson_rollback_command_remains_compatible_with_legacy_binary(self) -> None:
        parser = Path("/fixture/binlog-parser")
        source = Path("/fixture/source.binlog")
        output = Path("/fixture/staging")
        with patch("app.parser_bridge.parser_executable", return_value=parser):
            ndjson = _parser_command(
                source, "file", "mysql", output_dir=output,
                chunk_format="ndjson", max_lines=1, max_bytes=1024,
            )
            arrow = _parser_command(
                source, "file", "mysql", output_dir=output,
                chunk_format="arrow", max_lines=1, max_bytes=1024,
            )
        self.assertNotIn("--chunk-format", ndjson)
        position = arrow.index("--chunk-format")
        self.assertEqual(arrow[position + 1], "arrow")

    def test_raw_cache_command_binds_source_identity_and_original_size(self) -> None:
        parser = Path("/fixture/binlog-parser")
        source_id = "a" * 64
        with patch("app.parser_bridge.parser_executable", return_value=parser):
            command = _parser_command(
                Path("/fixture/source.rawcache"),
                source_id,
                "mysql",
                raw_cache_expected_size=987654,
            )
            with self.assertRaises(ParserError) as bad_identity:
                _parser_command(
                    Path("/fixture/source.rawcache"),
                    "not-a-sha",
                    "mysql",
                    raw_cache_expected_size=1,
                )
            with self.assertRaises(ParserError) as bad_size:
                _parser_command(
                    Path("/fixture/source.rawcache"),
                    source_id,
                    "mysql",
                    raw_cache_expected_size=-1,
                )
        identity_at = command.index("--raw-cache-source-id")
        size_at = command.index("--raw-cache-expected-size")
        self.assertEqual(command[identity_at + 1], source_id)
        self.assertEqual(command[size_at + 1], "987654")
        self.assertEqual(bad_identity.exception.code, "PARSER_RAW_CACHE_INPUT_INVALID")
        self.assertEqual(bad_size.exception.code, "PARSER_RAW_CACHE_INPUT_INVALID")

    def test_collector_defaults_to_arrow_with_explicit_ndjson_rollback(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("RDS_BINLOG_PARSER_TRANSPORT", None)
            self.assertEqual(parser_transport_format(), PARSER_TRANSPORT_ARROW)
        with patch.dict(os.environ, {"RDS_BINLOG_PARSER_TRANSPORT": "ndjson"}):
            self.assertEqual(parser_transport_format(), "ndjson")
        with self.assertRaises(ParserError) as raised:
            parser_transport_format("csv")
        self.assertEqual(raised.exception.code, "PARSER_TRANSPORT_INVALID")


if __name__ == "__main__":
    unittest.main()
