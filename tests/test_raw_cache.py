from __future__ import annotations

import hashlib
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from app.raw_cache import (END, FRAME, HEADER, MIN_FRAME, RawCacheError,
                           RawCacheWriter, inspect_cache, iter_raw)

IDENTITY = hashlib.sha256(b"fixture source identity").hexdigest()


class RawCacheTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.addCleanup(self.root.cleanup)
        self.path = Path(self.root.name) / "fixture.cache"
        self.raw = b"\xfebin" + bytes(range(256)) * 513

    def writer(self, path=None, **options):
        defaults = dict(source_id=IDENTITY, expected_size=len(self.raw),
                        physical_budget=1024 * 1024, frame_bytes=MIN_FRAME)
        defaults.update(options)
        return RawCacheWriter(path or self.path, **defaults)

    def inspect(self, path=None, **options):
        return inspect_cache(path or self.path, source_id=IDENTITY,
                             expected_size=options.get("size", len(self.raw)))

    def read(self, path=None):
        return b"".join(iter_raw(path or self.path, source_id=IDENTITY,
                                 expected_size=len(self.raw)))

    def complete(self, **options):
        with self.writer(**options) as writer:
            # Irregular network reads must not define persisted frame boundaries.
            for at in range(0, len(self.raw), 1777):
                writer.write(self.raw[at:at + 1777])
            return writer.finish(expected_sha256=hashlib.sha256(self.raw).hexdigest())

    def test_lossless_roundtrip_both_codecs(self):
        for codec in ("zstd", "lz4_frame"):
            with self.subTest(codec=codec):
                path = self.path.with_suffix("." + codec)
                checksum = self.complete(path=path, codec=codec)
                state = self.inspect(path)
                self.assertTrue(state.complete)
                self.assertEqual(state.sha256, checksum)
                self.assertEqual(state.raw_bytes, len(self.raw))
                self.assertEqual(state.frames, 3)
                self.assertEqual(self.read(path), self.raw)

    def test_never_overwrite_existing_asset(self):
        self.path.write_bytes(b"existing")
        with self.assertRaises(FileExistsError):
            self.writer()
        self.assertEqual(self.path.read_bytes(), b"existing")

    def test_checkpoint_and_resume_preserve_original(self):
        with self.writer() as writer:
            writer.write(self.raw[:MIN_FRAME + 3])
            self.assertEqual(writer.checkpoint(), MIN_FRAME + 3)
        before = self.path.read_bytes()
        self.assertFalse(self.inspect().complete)
        with self.assertRaises(RawCacheError):
            self.read()
        destination = self.path.with_suffix(".resumed")
        with RawCacheWriter.resume(self.path, destination, source_id=IDENTITY,
                                   expected_size=len(self.raw), physical_budget=1024 * 1024) as writer:
            self.assertEqual(writer.raw_offset, MIN_FRAME + 3)
            writer.write(self.raw[writer.raw_offset:])
            writer.finish()
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(self.read(destination), self.raw)

    def test_every_torn_frame_or_footer_tail_is_recoverable_without_mutation(self):
        self.complete()
        original = self.path.read_bytes()
        # Representative cuts include partial markers, headers, bodies and footer.
        cuts = {HEADER.size, HEADER.size + 1, HEADER.size + FRAME.size - 1,
                len(original) - END.size, len(original) - END.size + 1,
                len(original) - 1}
        cuts.update(range(HEADER.size, len(original), 71))
        for index, cut in enumerate(sorted(cuts)):
            with self.subTest(cut=cut):
                source = self.path.with_suffix(f".torn-{index}")
                source.write_bytes(original[:cut])
                state = self.inspect(source)
                self.assertFalse(state.complete)
                destination = self.path.with_suffix(f".new-{index}")
                with RawCacheWriter.resume(source, destination, source_id=IDENTITY,
                                           expected_size=len(self.raw), physical_budget=1024 * 1024) as writer:
                    self.assertEqual(writer.raw_offset, state.raw_bytes)
                    writer.write(self.raw[writer.raw_offset:])
                    writer.finish()
                self.assertEqual(source.read_bytes(), original[:cut])
                self.assertEqual(self.read(destination), self.raw)

    def test_wrong_identity_or_size_is_rejected(self):
        self.complete()
        for identity, size in (("a" * 64, len(self.raw)), (IDENTITY, len(self.raw) + 1)):
            with self.subTest(identity=identity, size=size), self.assertRaises(RawCacheError):
                inspect_cache(self.path, source_id=identity, expected_size=size)

    def test_frame_checksum_corruption_is_not_a_resumable_tail(self):
        self.complete()
        raw = bytearray(self.path.read_bytes())
        raw[HEADER.size + FRAME.size - 1] ^= 1
        self.path.write_bytes(raw)
        with self.assertRaisesRegex(RawCacheError, "SHA mismatch"):
            self.inspect()
        with self.assertRaises(RawCacheError):
            RawCacheWriter.resume(self.path, self.path.with_suffix(".new"),
                                  source_id=IDENTITY, expected_size=len(self.raw), physical_budget=1024 * 1024)
        self.assertFalse(self.path.with_suffix(".new").exists())

    def test_forged_frame_size_is_rejected_before_allocation(self):
        with self.writer() as writer:
            writer.checkpoint()
        with self.path.open("ab") as handle:
            handle.write(FRAME.pack(b"FRM1", 2**32 - 1, 2**32 - 1, bytes(32)))
        with self.assertRaisesRegex(RawCacheError, "budget"):
            self.inspect()

    def test_unknown_marker_is_corruption(self):
        with self.writer() as writer:
            writer.checkpoint()
        with self.path.open("ab") as handle:
            handle.write(b"WHAT")
        with self.assertRaisesRegex(RawCacheError, "marker"):
            self.inspect()

    def test_final_sha_and_trailing_data_are_rejected(self):
        self.complete()
        good = self.path.read_bytes()
        for raw in (good[:-1] + bytes([good[-1] ^ 1]), good + b"extra"):
            self.path.write_bytes(raw)
            with self.assertRaises(RawCacheError):
                self.inspect()
        self.path.write_bytes(good)
        with self.assertRaisesRegex(RawCacheError, "reused"):
            RawCacheWriter.resume(self.path, self.path.with_suffix(".new"),
                                  source_id=IDENTITY, expected_size=len(self.raw), physical_budget=1024 * 1024)

    def test_physical_limit_fails_before_frame_write(self):
        with self.writer(physical_budget=HEADER.size + END.size) as writer:
            with self.assertRaisesRegex(RawCacheError, "budget"):
                writer.write(self.raw[:MIN_FRAME])
            with self.assertRaisesRegex(RawCacheError, "failed"):
                writer.finish()
        self.assertEqual(self.path.stat().st_size, HEADER.size)

    def test_recovery_budget_counts_retained_source(self):
        with self.writer() as writer:
            writer.write(self.raw[:MIN_FRAME]); writer.checkpoint()
        size = self.path.stat().st_size
        with self.assertRaisesRegex(RawCacheError, "combined"):
            RawCacheWriter.resume(self.path, self.path.with_suffix(".new"),
                                  source_id=IDENTITY, expected_size=len(self.raw), physical_budget=2 * size)
        self.assertFalse(self.path.with_suffix(".new").exists())

    def test_incomplete_oversized_and_wrong_sha_never_mark_complete(self):
        for mode in ("incomplete", "oversized", "wrong-sha"):
            path = self.path.with_suffix("." + mode)
            with self.subTest(mode=mode), self.writer(path) as writer:
                with self.assertRaises(RawCacheError):
                    if mode == "oversized":
                        writer.write(self.raw + b"extra")
                    elif mode == "incomplete":
                        writer.write(self.raw[:-1]); writer.finish()
                    else:
                        writer.write(self.raw); writer.finish(expected_sha256="0" * 64)
            self.assertFalse(self.inspect(path).complete)

    def test_fsync_failure_is_not_success_and_poison_writer(self):
        with self.writer() as writer:
            writer.write(self.raw[:100])
            with patch("app.raw_cache.os.fsync", side_effect=OSError("fixture disk error")):
                with self.assertRaises(OSError):
                    writer.checkpoint()
            with self.assertRaisesRegex(RawCacheError, "failed"):
                writer.write(b"x")
        self.assertFalse(self.inspect().complete)

    def test_empty_source_and_incompressible_input(self):
        self.raw = b""
        self.complete()
        self.assertTrue(self.inspect(size=0).complete)
        self.raw = os.urandom(2 * MIN_FRAME + 3)
        path = self.path.with_suffix(".random")
        self.complete(path=path)
        self.assertEqual(self.read(path), self.raw)


if __name__ == "__main__":
    unittest.main()
