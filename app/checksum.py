from __future__ import annotations

from pathlib import Path

_MASK = 0xFFFFFFFFFFFFFFFF
_POLY_REVERSED = 0xC96C5795D7870F42


def _build_table() -> tuple[int, ...]:
    table: list[int] = []
    for value in range(256):
        crc = value
        for _ in range(8):
            crc = (crc >> 1) ^ (_POLY_REVERSED if crc & 1 else 0)
        table.append(crc & _MASK)
    return tuple(table)


_TABLE = _build_table()


def crc64_xz_update(data: bytes, crc: int = 0) -> int:
    """Alibaba OSS CRC64-ECMA/XZ compatible incremental checksum."""
    state = crc ^ _MASK
    for byte in data:
        state = _TABLE[(state ^ byte) & 0xFF] ^ (state >> 8)
    return state ^ _MASK


def crc64_file(path: Path, chunk_size: int = 4 * 1024 * 1024) -> int:
    crc = 0
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            crc = crc64_xz_update(chunk, crc)
    return crc

