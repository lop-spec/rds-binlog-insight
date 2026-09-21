"""Lossless binlog directory: inspect envelopes, never deserialize row images.

Ranges always start at a GTID boundary. Unknown/packed event types deliberately
keep the range eligible. This index is not a primary-key or row-count index.
"""
from __future__ import annotations

import logging
import mmap
import struct
import uuid
from pathlib import Path

LOGGER = logging.getLogger(__name__)
HEADER = struct.Struct('<IBIIIH')
MAGIC = b'\xfebin'
MAX_TIME = 2**62
ROWS = {20, 21, 22, 23, 24, 25, 30, 31, 32, 39}
# Known non-row event types that cannot hide tables. Query events are handled
# separately; their SQL is NOT parsed to guess table names.
SAFE = {2, 3, 4, 5, 13, 14, 15, 16, 19, 27, 28, 29, 33, 34, 35, 36, 38}


class RawBinlogError(RuntimeError):
    def __init__(self, message: str, code: str = 'RAW_BINLOG_INVALID'):
        super().__init__(message)
        self.code = code


def scan(path: Path) -> dict:
    size = path.stat().st_size
    if size < 23:
        raise RawBinlogError('Binlog 文件过短，保留原文件')
    regions, tables = [], set()
    first_gtid = None
    region = None
    events = 0
    fde_seen = False
    checksum_bytes = 0
    uncertainty = set()
    force_whole = False
    with path.open('rb') as source, mmap.mmap(source.fileno(), 0, access=mmap.ACCESS_READ) as data:
        if data[:4] != MAGIC:
            raise RawBinlogError('不是未加密的 MySQL binlog；拒绝伪装成已建立轻量索引', 'RAW_BINLOG_FORMAT_UNSUPPORTED')
        pos = 4
        region = dict(start=4, end=size, tables=set(), lo=MAX_TIME, hi=0, unknown=False)
        while pos < size:
            if size - pos < HEADER.size:
                raise RawBinlogError(f'事件头截断：offset={pos}')
            stamp, kind, server, length, log_pos, flags = HEADER.unpack_from(data, pos)
            if length < HEADER.size or pos + length > size:
                raise RawBinlogError(f'事件长度非法：offset={pos} length={length}')
            end = pos + length
            body = pos + HEADER.size
            payload_end = end - checksum_bytes
            if events == 0 and kind != 15:
                raise RawBinlogError('文件首事件不是 FormatDescriptionEvent')
            if kind == 15:
                if length < 19 + 57 or data[body:body+2] != b'\x04\x00' or data[body+56] != 19:
                    raise RawBinlogError('不支持此 binlog 格式版本', 'RAW_BINLOG_FORMAT_UNSUPPORTED')
                # v4 MySQL 5.6+ FDE describes its own checksum algorithm.
                checksum_bytes = 4 if data[end-5] == 1 else 0
                fde_seen = True
            if kind in {33, 34}:
                if first_gtid is None:
                    first_gtid = pos
                    # Unexpected row/statement prefix is retained as a range,
                    # rather than being silently omitted during table pruning.
                    if region['tables'] or region['unknown']:
                        region['end'] = pos
                        regions.append(region)
                        force_whole = True
                        uncertainty.add('non-format-prefix')
                else:
                    region['end'] = pos
                    regions.append(region)
                region = dict(start=pos, end=size, tables=set(), lo=MAX_TIME, hi=0, unknown=False)
                if payload_end-body < 25:
                    raise RawBinlogError('GTID identity 截断')
                region['gtid'] = f"{uuid.UUID(bytes=data[body+1:body+17])}:{int.from_bytes(data[body+17:body+25], 'little')}"
                if kind == 34:
                    force_whole = True
                    uncertainty.add('anonymous-gtid')
                # MySQL logical timestamp GTID: flags+SID+GNO+type+last+seq
                # followed by 7-byte immediate commit time; bit 55 signals an
                # additional original commit time. Include BOTH conservatively.
                if payload_end - body >= 49 and data[body+25] == 2:
                    immediate = int.from_bytes(data[body+42:body+49], 'little')
                    values = [immediate & ((1 << 55) - 1)]
                    if immediate & (1 << 55):
                        if payload_end - body < 56:
                            raise RawBinlogError('GTID original commit timestamp 截断')
                        values.append(int.from_bytes(data[body+49:body+56], 'little'))
                    for value in values:
                        if value:
                            region['lo'] = min(region['lo'], value)
                            region['hi'] = max(region['hi'], value)
            if stamp:
                region['lo'] = min(region['lo'], stamp * 1_000_000)
                region['hi'] = max(region['hi'], stamp * 1_000_000 + 999_999)
            if kind == 19:
                # table_id(6), flags(2), schema_len(1), schema, NUL,
                # table_len(1), table, NUL. Column definitions are skipped.
                cursor = body + 8
                if cursor >= payload_end:
                    raise RawBinlogError('TABLE_MAP 截断')
                db_len = data[cursor]
                cursor += 1
                if cursor + db_len + 2 > payload_end:
                    raise RawBinlogError('TABLE_MAP database 截断')
                database = data[cursor:cursor+db_len].decode('utf-8', errors='strict')
                cursor += db_len + 1
                table_len = data[cursor]
                cursor += 1
                if cursor + table_len + 1 > payload_end:
                    raise RawBinlogError('TABLE_MAP table 截断')
                table = data[cursor:cursor+table_len].decode('utf-8', errors='strict')
                region['tables'].add((database, table))
                tables.add((database, table))
            elif kind == 2:
                # Statement DDL/DML may affect any table. BEGIN/COMMIT do not.
                if payload_end - body < 13:
                    raise RawBinlogError('QUERY_EVENT 截断')
                db_len = data[body+8]
                status_len = int.from_bytes(data[body+11:body+13], 'little')
                sql_start = body + 13 + status_len + db_len + 1
                if sql_start > payload_end:
                    raise RawBinlogError('QUERY_EVENT SQL 截断')
                sql = data[sql_start:payload_end].strip().upper()
                if sql not in {b'BEGIN', b'COMMIT', b'ROLLBACK'}:
                    region['unknown'] = True
                    uncertainty.add('statement-query')
            elif kind not in SAFE and kind not in ROWS:
                region['unknown'] = True
                # Compressed transactions may contain earlier event timestamps.
                region['lo'], region['hi'] = 0, MAX_TIME
                uncertainty.add(f'opaque-event-{kind}')
            events += 1
            pos = end
        if not fde_seen:
            raise RawBinlogError('缺少 FormatDescriptionEvent')
        if first_gtid is None:
            region['lo'], region['hi'], region['unknown'] = 0, MAX_TIME, True
            uncertainty.add('no-gtid-boundaries')
        regions.append(region)
    if force_whole:
        first_gtid = 4
        regions = [dict(start=4, end=size, tables=tables.copy(), lo=0, hi=MAX_TIME, unknown=True)]
    for value in regions:
        value['tables'] = sorted(value['tables'])
        if value['lo'] == MAX_TIME or not value['hi']:
            value['lo'], value['hi'] = 0, MAX_TIME
    if uncertainty:
        LOGGER.warning('RAW_INDEX_CONSERVATIVE file=%s reasons=%s', path.name, ','.join(sorted(uncertainty)))
    return dict(version=1, size=size, prefix_end=first_gtid or 4,
                lo=min(r['lo'] for r in regions), hi=max(r['hi'] for r in regions),
                tables=sorted(tables), unknown=any(r['unknown'] for r in regions),
                events=events, regions=regions, uncertainty=sorted(uncertainty))


def allows(entry: dict, query: dict, start: int, end: int) -> bool:
    if entry['hi'] < start or entry['lo'] > end:
        return False
    if entry.get('unknown'):
        return True
    db, table = (str(query.get(k) or '').strip().lower() for k in ('database', 'table'))
    if not db and not table:
        return True
    exact = isinstance(query.get('exact'), dict)
    def match(needle, value):
        return not needle or (needle == value.lower() if exact else needle in value.lower())
    return any(match(db, d) and match(table, t) for d, t in entry['tables'])
