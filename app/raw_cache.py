"""Bounded, lossless raw-source cache; not a source/metadata completion marker.

The source SHA/CRC and file publication remain the caller's responsibility.
Recovery never truncates or overwrites an existing asset: verified frames are
copied into a distinct, exclusive destination. The native reader and pipeline
integration remain default-off candidate code; enabling or deploying them is a
separate operational decision and does not relax whole-file publication gates.
"""
from __future__ import annotations

import hashlib
import os
import struct
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, Iterator

import pyarrow as pa

MAGIC = b"RDSRAW1\n"
HEADER = struct.Struct("<8sBQ32sI")
FRAME = struct.Struct("<4sII32s")
END = struct.Struct("<4sQ32s")
CODECS = {1: "zstd", 2: "lz4_frame"}
MIN_FRAME = 64 * 1024
MAX_FRAME = 16 * 1024 * 1024
DEFAULT_FRAME = 4 * 1024 * 1024


class RawCacheError(ValueError):
    pass


@dataclass(frozen=True)
class CacheState:
    source_id: str
    expected_size: int
    codec: str
    frame_bytes: int
    raw_bytes: int
    verified_prefix_bytes: int
    physical_bytes: int
    frames: int
    complete: bool
    sha256: str
    _digest: object = field(repr=False, compare=False)


def _identity(value: str) -> bytes:
    if len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
        raise RawCacheError("source_id must be a lowercase SHA256 identity")
    return bytes.fromhex(value)


def _header(handle: BinaryIO, source_id: str, expected_size: int):
    raw = handle.read(HEADER.size)
    if len(raw) != HEADER.size:
        raise RawCacheError("raw cache header is truncated")
    magic, code, size, identity, frame_bytes = HEADER.unpack(raw)
    if (magic != MAGIC or code not in CODECS or size != expected_size
            or identity != _identity(source_id)
            or not MIN_FRAME <= frame_bytes <= MAX_FRAME):
        raise RawCacheError("raw cache header/identity/size is incompatible")
    return CODECS[code], frame_bytes


def _records(handle: BinaryIO, *, source_id: str, expected_size: int,
             require_complete: bool) -> Iterator[bytes | CacheState]:
    codec_name, frame_bytes = _header(handle, source_id, expected_size)
    codec = pa.Codec(codec_name)
    digest = hashlib.sha256()
    total = frames = 0
    prefix = HEADER.size
    physical = os.fstat(handle.fileno()).st_size
    complete = False
    while True:
        marker = handle.read(4)
        if len(marker) < 4:
            break  # a torn, never-committed tail can be recovered into a new file
        if marker == b"END1":
            rest = handle.read(END.size - 4)
            if len(rest) != END.size - 4:
                break
            _, count, checksum = END.unpack(marker + rest)
            if count != total or total != expected_size or checksum != digest.digest():
                raise RawCacheError("raw cache final size/SHA mismatch")
            if handle.read(1):
                raise RawCacheError("raw cache has trailing data after its footer")
            prefix = handle.tell()
            complete = True
            break
        if marker != b"FRM1":
            raise RawCacheError("raw cache frame marker is corrupt")
        rest = handle.read(FRAME.size - 4)
        if len(rest) != FRAME.size - 4:
            break
        _, raw_size, compressed_size, checksum = FRAME.unpack(marker + rest)
        if (not 0 < raw_size <= frame_bytes
                or not 0 < compressed_size <= frame_bytes * 2 + 65536
                or total + raw_size > expected_size):
            raise RawCacheError("raw cache frame exceeds the declared size budget")
        payload = handle.read(compressed_size)
        if len(payload) != compressed_size:
            break
        try:
            raw = codec.decompress(payload, decompressed_size=raw_size, asbytes=True)
        except Exception as exc:
            raise RawCacheError("raw cache frame decompression failed") from exc
        if len(raw) != raw_size or hashlib.sha256(raw).digest() != checksum:
            raise RawCacheError("raw cache frame size/SHA mismatch")
        digest.update(raw)
        total += raw_size
        frames += 1
        prefix = handle.tell()
        yield raw
    if require_complete and not complete:
        raise RawCacheError("raw cache is incomplete; no complete source may be published")
    yield CacheState(source_id, expected_size, codec_name, frame_bytes, total,
                     prefix, physical, frames, complete, digest.hexdigest(), digest)


def inspect_cache(path: Path, *, source_id: str, expected_size: int) -> CacheState:
    """Read-only verification; tolerate only truncated tails, never corruption."""
    with Path(path).open("rb") as handle:
        for record in _records(handle, source_id=source_id,
                               expected_size=expected_size, require_complete=False):
            if isinstance(record, CacheState):
                return record
    raise RawCacheError("raw cache ended without a verification result")


def _iter_raw(path: Path, *, source_id: str, expected_size: int,
              require_complete: bool) -> Iterator[bytes]:
    with Path(path).open("rb") as handle:
        for record in _records(handle, source_id=source_id,
                               expected_size=expected_size,
                               require_complete=require_complete):
            if isinstance(record, bytes):
                yield record


def iter_raw(path: Path, *, source_id: str, expected_size: int) -> Iterator[bytes]:
    """Yield verified frames; exhausting the iterator verifies the final footer.

    Frames are private input until full consumption AND source CRC validation.
    A consumer stopping early has not verified the complete source.
    """
    yield from _iter_raw(path, source_id=source_id, expected_size=expected_size,
                         require_complete=True)


def iter_verified_prefix(path: Path, *, source_id: str,
                         expected_size: int) -> Iterator[bytes]:
    """Yield only complete verified frames from an incomplete recovery asset.

    This does not prove source completion and must only seed a checksum stream
    whose suffix is fetched with a validated HTTP Range response.
    """
    yield from _iter_raw(path, source_id=source_id, expected_size=expected_size,
                         require_complete=False)


class RawCacheWriter:
    def __init__(self, path: Path, *, source_id: str, expected_size: int,
                 physical_budget: int, codec: str = "zstd",
                 frame_bytes: int = DEFAULT_FRAME):
        identity = _identity(source_id)
        if codec not in CODECS.values() or not pa.Codec.is_available(codec):
            raise RawCacheError("requested raw cache codec is unavailable")
        if (expected_size < 0 or not MIN_FRAME <= frame_bytes <= MAX_FRAME
                or physical_budget < HEADER.size + END.size):
            raise RawCacheError("invalid raw cache size/budget")
        self.path = Path(path)
        self.source_id = source_id
        self.expected_size = expected_size
        self.physical_budget = physical_budget
        self.frame_bytes = frame_bytes
        self.codec_name = codec
        self.codec = pa.Codec(codec, compression_level=1) if codec == "zstd" else pa.Codec(codec)
        self._digest = hashlib.sha256()
        self._raw_bytes = 0
        self._buffer = bytearray()
        self._failed = False
        self._directory_synced = False
        self._handle = self.path.open("xb")
        try:
            code = next(k for k, v in CODECS.items() if v == codec)
            self._handle.write(HEADER.pack(MAGIC, code, expected_size, identity, frame_bytes))
        except BaseException:
            self._handle.close()
            raise

    @classmethod
    def resume(cls, source: Path, destination: Path, *, source_id: str,
               expected_size: int, physical_budget: int) -> "RawCacheWriter":
        """Budget includes both the retained old asset and the new destination."""
        with Path(source).open("rb") as handle:
            before = os.fstat(handle.fileno())
            state = None
            for record in _records(handle, source_id=source_id,
                                   expected_size=expected_size, require_complete=False):
                if isinstance(record, CacheState):
                    state = record
            assert state is not None
            if state.complete:
                raise RawCacheError("complete raw cache must be reused, not resumed")
            remaining = physical_budget - state.physical_bytes
            if remaining < state.verified_prefix_bytes + END.size:
                raise RawCacheError("recovery copies exceed combined physical budget")
            writer = cls(destination, source_id=source_id, expected_size=expected_size,
                         codec=state.codec, frame_bytes=state.frame_bytes,
                         physical_budget=remaining)
            try:
                handle.seek(HEADER.size)
                left = state.verified_prefix_bytes - HEADER.size
                while left:
                    data = handle.read(min(left, DEFAULT_FRAME))
                    if not data:
                        raise RawCacheError("recovery source changed during copy")
                    writer._handle.write(data)
                    left -= len(data)
                after = os.fstat(handle.fileno())
                if (before.st_size, before.st_mtime_ns, before.st_ctime_ns) != (
                        after.st_size, after.st_mtime_ns, after.st_ctime_ns):
                    raise RawCacheError("recovery source changed during copy")
                writer._digest = state._digest.copy()
                writer._raw_bytes = state.raw_bytes
                writer.checkpoint()
                return writer
            except BaseException:
                writer.close()
                raise

    @property
    def raw_offset(self) -> int:
        """Accepted raw offset, not necessarily durable until checkpoint()."""
        return self._raw_bytes + len(self._buffer)

    def _check_open(self):
        if self._failed or self._handle.closed:
            raise RawCacheError("raw cache writer is closed or failed")

    def write(self, data: bytes) -> None:
        self._check_open()
        if self.raw_offset + len(data) > self.expected_size:
            self._failed = True
            raise RawCacheError("raw source exceeds expected size")
        view = memoryview(data)
        while view:
            count = min(len(view), self.frame_bytes - len(self._buffer))
            self._buffer.extend(view[:count])
            view = view[count:]
            if len(self._buffer) == self.frame_bytes:
                self._flush_frame()

    def _flush_frame(self):
        if not self._buffer:
            return
        try:
            payload = self.codec.compress(self._buffer, asbytes=True)
            needed = FRAME.size + len(payload)
            if self._handle.tell() + needed + END.size > self.physical_budget:
                raise RawCacheError("raw cache physical budget exceeded")
            self._handle.write(FRAME.pack(b"FRM1", len(self._buffer), len(payload),
                                          hashlib.sha256(self._buffer).digest()))
            self._handle.write(payload)
            self._digest.update(self._buffer)
            self._raw_bytes += len(self._buffer)
            self._buffer.clear()
        except BaseException:
            self._failed = True
            raise

    def checkpoint(self) -> int:
        self._check_open()
        self._flush_frame()
        try:
            self._handle.flush()
            os.fsync(self._handle.fileno())
            # fsync(file) alone does not persist a newly created directory entry.
            # Deployment/recovery acceptance is Linux; Windows runs unit tests only.
            if not self._directory_synced and os.name == "posix":
                directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(directory)
                finally:
                    os.close(directory)
                self._directory_synced = True
        except BaseException:
            self._failed = True
            raise
        return self._raw_bytes

    def finish(self, *, expected_sha256: str | None = None) -> str:
        self._check_open()
        if self.raw_offset != self.expected_size:
            self._failed = True
            raise RawCacheError("cannot finish an incomplete raw source")
        self._flush_frame()
        checksum = self._digest.hexdigest()
        if expected_sha256 is not None and checksum != expected_sha256:
            self._failed = True
            raise RawCacheError("raw source SHA mismatch")
        try:
            self._handle.write(END.pack(b"END1", self._raw_bytes, self._digest.digest()))
            self.checkpoint()
        except BaseException:
            self._failed = True
            raise
        finally:
            self._handle.close()
        return checksum

    def close(self):
        """Close without marking complete; retain all bytes for diagnosis/recovery."""
        self._handle.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
