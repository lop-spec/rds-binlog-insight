"""Seekable binlog: concatenated independent Zstd frames, addressed in plaintext bytes."""
from __future__ import annotations

import hashlib
import re
from pathlib import Path

from .binlog_lite import RawBinlogError

FORMAT = 'zstd-frames-v2'
BLOCK = 8 << 20


def fail(message):
    raise RawBinlogError(message, 'RAW_INDEX_VERIFY_FAILED')


def physical(raw):
    if 'format' not in raw:
        return raw
    if raw['format'] != FORMAT:
        fail('unknown raw format')
    return raw['stored']


def encode(path, destination, source_sha, source_crc, progress=None):
    from oss2.utils import Crc64
    import pyarrow as pa
    codec = pa.Codec('zstd', compression_level=3)
    plain, packed = hashlib.sha256(), hashlib.sha256()
    crc, stored_crc = Crc64(), Crc64()
    frames, total = [], 0
    with Path(path).open('rb') as src, Path(destination).open('wb') as out:
        while data := src.read(BLOCK):
            comp = codec.compress(data, asbytes=True)
            if codec.decompress(comp, decompressed_size=len(data), asbytes=True) != data:
                fail('frame roundtrip mismatch')
            frames.append([out.tell(), len(comp), hashlib.sha256(data).hexdigest()])
            out.write(comp)
            plain.update(data); crc(data); total += len(data)
            packed.update(comp); stored_crc(comp)
            if progress:
                progress()
    if not total or plain.hexdigest() != source_sha or str(crc.crc) != str(source_crc):
        fail('source digest mismatch during compression')
    return (dict(format=FORMAT, size_bytes=total, sha256=source_sha, crc64=str(source_crc),
                 block=BLOCK, frames=frames), packed.hexdigest(), str(stored_crc.crc))


def validate(raw):
    stored = physical(raw)
    size, frames, block = raw['size_bytes'], raw['frames'], raw['block']
    if not 0 < block <= 32 << 20 or size <= 0 or len(frames) != (size + block - 1) // block:
        fail('invalid frame map')
    off = 0
    for pos, length, sha in frames:
        if pos != off or not 0 < length <= block + (block >> 7) + 1024 or not re.fullmatch('[0-9a-f]{64}', sha):
            fail('invalid frame entry')
        off += length
    if off != stored['size_bytes']:
        fail('frame map does not cover compressed object')


def chunks(bucket, raw, start, end, *, check=lambda: None, charge=lambda n: None, request=lambda: None):
    import pyarrow as pa
    validate(raw)
    size, block, stored = raw['size_bytes'], raw['block'], raw['stored']
    if not 0 <= start < end <= size:
        fail('frame range out of bounds')
    codec = pa.Codec('zstd')
    for i in range(start // block, (end - 1) // block + 1):
        check()
        pos, length, sha = raw['frames'][i]
        plain_size = min(block, size - i * block)
        charge(max(length, plain_size))
        request()
        resp = bucket.get_object(stored['oss_key'], byte_range=(pos, pos + length - 1))
        try:
            if resp.headers.get('Content-Range', resp.headers.get('content-range')) != f'bytes {pos}-{pos+length-1}/{stored["size_bytes"]}':
                fail('unexpected compressed range')
            data = bytearray()
            while len(data) <= length:
                check()
                piece = resp.read(min(1 << 20, length + 1 - len(data)))
                if not piece:
                    break
                data.extend(piece)
            if len(data) != length:
                fail('truncated or oversized compressed frame')
        finally:
            resp.close()
        plain = codec.decompress(data, decompressed_size=plain_size, asbytes=True)
        if hashlib.sha256(plain).hexdigest() != sha:
            fail('plaintext frame checksum mismatch')
        yield plain[max(0,start-i*block):min(plain_size,end-i*block)]
