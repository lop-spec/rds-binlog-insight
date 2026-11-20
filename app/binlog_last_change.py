"""Read-side cutover for sealed last-effective-change windows.

A window is keyed by exact instance, database and table. Missing windows keep
using the full row store. A malformed catalog fails closed. A publication does
not change source retention and this module cannot write or delete any data.
"""
from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from pathlib import Path

TABLE = 'insight.binlog_last_change_v1'
CATALOG = 'binlog-last-change-windows.v1.json'
TIER = 'clickhouse-binlog-last-change'
MODE = 'last-effective-change'
MIN_AGE_US = 14 * 86400 * 1_000_000
MAX_WINDOW_US = 7 * 86400 * 1_000_000
MAX_CATALOG_BYTES = 2 * 1024 * 1024
COLUMNS = frozenset('event_id event_epoch_us instance_id host_instance_id source_file_name raw_event_type '
                    'operation database_name table_name transaction_id gtid start_position end_position row_index '
                    'sql_kind before_json after_json'.split())

DDL = f'''CREATE TABLE IF NOT EXISTS {TABLE} (
    window_id FixedString(32),
    event_id String CODEC(ZSTD(1)),
    event_epoch_us Int64 CODEC(Delta, ZSTD(1)),
    event_date Date MATERIALIZED toDate(fromUnixTimestamp64Micro(event_epoch_us, 'UTC')),
    instance_id LowCardinality(String),
    host_instance_id LowCardinality(String),
    source_file_name LowCardinality(String),
    database_name LowCardinality(String),
    table_name LowCardinality(String),
    tbl_bucket UInt8 MATERIALIZED cityHash64(lower(database_name), lower(table_name)) % 8,
    raw_event_type LowCardinality(String) DEFAULT 'ARCHIVED_LAST_CHANGE',
    operation LowCardinality(String),
    transaction_id String CODEC(ZSTD(1)),
    gtid String CODEC(ZSTD(1)),
    start_position Int64 CODEC(Delta, ZSTD(1)),
    end_position Int64 CODEC(Delta, ZSTD(1)),
    row_index Int32 CODEC(Delta, ZSTD(1)),
    sql_kind LowCardinality(String) DEFAULT 'LAST_CHANGE',
    before_json String CODEC(ZSTD(3)),
    after_json String CODEC(ZSTD(3)),
    key_before_json String CODEC(ZSTD(3)),
    key_after_json String CODEC(ZSTD(3))
) ENGINE=MergeTree PARTITION BY event_date
ORDER BY (instance_id, database_name, table_name, window_id, event_epoch_us, end_position, row_index, event_id)
TTL event_date + toIntervalDay(62)
SETTINGS storage_policy='binlog_rows', ttl_only_drop_parts=1'''


@dataclass(frozen=True, slots=True)
class Window:
    instance: str
    database: str
    table: str
    start_us: int                 # inclusive
    end_us: int                   # exclusive
    window_id: str
    row_count: int

    def for_display(self) -> dict:
        return {'start': self.start_us, 'end': self.end_us, 'mode': MODE}


def _no_duplicate_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate catalog key')
        result[key] = value
    return result


def load(path: Path, instance: str, database: str, table: str, *, now_us: int | None = None) -> list[Window]:
    try:
        with path.open('rb') as f:
            buf = f.read(MAX_CATALOG_BYTES + 1)
    except FileNotFoundError:
        return []
    if len(buf) > MAX_CATALOG_BYTES:
        raise ValueError('oversized archive catalog')
    doc = json.loads(buf, object_pairs_hook=_no_duplicate_keys)
    if (not isinstance(doc, dict) or type(doc.get('version')) is not int or doc['version'] != 1
            or not isinstance(doc.get('windows'), list)):
        raise ValueError('invalid archive catalog')
    deadline = (time.time_ns() // 1000 if now_us is None else now_us) - MIN_AGE_US
    result = []
    seen = set()
    last = {}
    for raw in doc['windows']:
        if not isinstance(raw, dict) or raw.keys() != Window.__dataclass_fields__.keys():
            raise ValueError('invalid archive entry')
        w = Window(**raw)
        names = (w.instance, w.database, w.table)
        if (any(not isinstance(x, str) or not 0 < len(x) <= 256 or any(ord(c) < 32 for c in x) for x in names)
                or type(w.start_us) is not int or type(w.end_us) is not int or type(w.row_count) is not int
                or not 0 < w.start_us < w.end_us <= deadline or w.end_us - w.start_us > MAX_WINDOW_US
                or w.row_count < 0 or not isinstance(w.window_id, str)
                or not re.fullmatch('[a-f0-9]{32}', w.window_id) or w.window_id in seen
                or w.start_us < last.get(names, 0)):
            raise ValueError('invalid or overlapping archive window')
        last[names] = w.end_us
        seen.add(w.window_id)
        if names == (instance, database, table):
            result.append(w)
    return result


def intersects(w: Window, lo: int, hi: int) -> bool:
    return w.start_us <= hi and lo < w.end_us


def slices(lo: int, hi: int, windows: list[Window]) -> list[tuple[int, int, Window | None]]:
    """Partition the inclusive query slice, newest first, without gaps/overlap."""
    result = []
    cursor = hi
    for w in reversed(windows):
        if not intersects(w, lo, cursor):
            continue
        if cursor >= w.end_us:
            result.append((w.end_us, cursor, None))
        result.append((max(lo, w.start_us), min(cursor, w.end_us - 1), w))
        cursor = w.start_us - 1
    if cursor >= lo:
        result.append((lo, cursor, None))
    return result


def select_columns(columns: tuple[str, ...]) -> str:
    defaults = {
        'row_query_hash': 'toUInt64(0)', 'server_id': 'toInt64(0)', 'thread_id': 'toInt64(0)',
        'execution_time_ms': 'toInt64(0)', 'error_code': 'toInt32(0)', 'affected_rows': 'toInt64(0)',
        'statement_index': 'toInt32(-1)',
    }
    empty = "''"
    return ', '.join(c if c in COLUMNS else f"{defaults.get(c, empty)} AS {c}" for c in columns)
