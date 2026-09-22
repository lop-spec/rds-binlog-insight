"""Rebuildable asynchronous raw-event index; never part of the archive commit.

Only completed, source-bound files may serve queries. Payloads are compressed
separately from covering keys, so ordered LIMIT does not decode discarded rows.
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
import zlib
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

from .binlog_lite import MAX_TIME, RawBinlogError, allows
from .exact_index import canonical_value

LOGGER = logging.getLogger(__name__)
ORDER = 'stamp DESC, filename DESC, position DESC, ordinal DESC, event_id DESC'
SCHEMA = '''
CREATE TABLE IF NOT EXISTS files (
 file_id TEXT PRIMARY KEY, signature TEXT NOT NULL, complete INTEGER NOT NULL DEFAULT 0,
 row_count INTEGER NOT NULL DEFAULT 0, first_stamp INTEGER, last_stamp INTEGER,
 error TEXT NOT NULL DEFAULT '', retry_at REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS payloads (
 id INTEGER PRIMARY KEY, file_id TEXT NOT NULL REFERENCES files ON DELETE CASCADE, body BLOB NOT NULL
);
CREATE INDEX IF NOT EXISTS payload_file ON payloads(file_id);
CREATE TABLE IF NOT EXISTS events (
 event_id TEXT PRIMARY KEY, file_id TEXT NOT NULL REFERENCES files ON DELETE CASCADE,
 instance TEXT NOT NULL, db TEXT COLLATE NOCASE NOT NULL, tbl TEXT COLLATE NOCASE NOT NULL,
 stamp INTEGER NOT NULL, filename TEXT NOT NULL, position INTEGER NOT NULL, ordinal INTEGER NOT NULL,
 operation TEXT NOT NULL, txn TEXT NOT NULL, gtid TEXT NOT NULL, pk_state TEXT NOT NULL,
 payload_id INTEGER NOT NULL REFERENCES payloads ON DELETE CASCADE, payload_ordinal INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS event_file ON events(file_id);
CREATE INDEX IF NOT EXISTS event_time ON events(instance,db,tbl,stamp DESC,filename DESC,position DESC,ordinal DESC,event_id DESC);
CREATE INDEX IF NOT EXISTS event_schema ON events(instance,db,tbl,pk_state,stamp);
CREATE TABLE IF NOT EXISTS keys (
 instance TEXT NOT NULL, db TEXT COLLATE NOCASE NOT NULL, tbl TEXT COLLATE NOCASE NOT NULL,
 type_id INTEGER NOT NULL, value BLOB NOT NULL,
 stamp INTEGER NOT NULL, filename TEXT NOT NULL, position INTEGER NOT NULL, ordinal INTEGER NOT NULL,
 event_id TEXT NOT NULL REFERENCES events ON DELETE CASCADE,
 PRIMARY KEY(instance,db,tbl,type_id,value,stamp DESC,filename DESC,position DESC,ordinal DESC,event_id DESC)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS key_event ON keys(event_id);
CREATE TABLE IF NOT EXISTS types (
 file_id TEXT NOT NULL REFERENCES files ON DELETE CASCADE, db TEXT COLLATE NOCASE NOT NULL,
 tbl TEXT COLLATE NOCASE NOT NULL, type_id INTEGER NOT NULL,
 PRIMARY KEY(file_id,db,tbl,type_id)
) WITHOUT ROWID;
'''


def signature(entry):
    return hashlib.sha256(json.dumps(entry, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def union(intervals):
    result = []
    for lo, hi in sorted(intervals):
        if lo > hi:
            continue
        if result and lo <= result[-1][1]+1:
            result[-1][1] = max(result[-1][1], hi)
        else:
            result.append([lo, hi])
    return result


def subtract(intervals, holes):
    result, index = [], 0
    for lo, hi in union(intervals):
        while index < len(holes) and holes[index][1] < lo:
            index += 1
        cursor = lo
        j = index
        while j < len(holes) and holes[j][0] <= hi:
            a, b = holes[j]
            if cursor < a:
                result.append([cursor, a-1])
            cursor = max(cursor, b+1)
            j += 1
        if cursor <= hi:
            result.append([cursor, hi])
    return result


def epoch(text, default):
    try:
        return int(datetime.fromisoformat(str(text).replace('Z', '+00:00')).timestamp()*1e6)
    except (ValueError, TypeError):
        return default


class RawEventIndex:
    def __init__(self, metadata, root, *, schema_sha=''):
        self.metadata = metadata
        self.path = Path(root)/'raw-events-v1.sqlite3'
        self.schema_sha = schema_sha

    def binding(self, entry):
        return signature(entry)+':'+self.schema_sha

    @contextmanager
    def connection(self, *, write=False):
        if write:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            new = not self.path.exists()
            conn = sqlite3.connect(self.path, timeout=5)
            if new:
                conn.execute('PRAGMA auto_vacuum=INCREMENTAL')
            conn.execute('PRAGMA journal_mode=WAL')
            conn.execute('PRAGMA synchronous=FULL')
            conn.execute('PRAGMA foreign_keys=ON')
            conn.executescript(SCHEMA)
        else:
            conn = sqlite3.connect(self.path.resolve().as_uri()+'?mode=ro', uri=True, timeout=5)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()

    def catalog(self, query, *, control=None):
        """One source snapshot, including registered holes and legacy files.

        Discovery cannot prove that historical source logs still exist. These
        intervals certify the registered source catalog, not unknown history.
        """
        instance = str(query.get('instance') or '')
        with self.metadata.connection(control=control) as conn:
            rows = conn.execute('''SELECT b.id,b.log_begin_utc,b.log_end_utc,b.state,
                r.lo,r.hi,r.summary,r.descriptor FROM binlog_files b
                LEFT JOIN raw_binlog_archives r ON r.file_id=b.id
                WHERE b.log_file_name LIKE 'mysql-bin.%' AND b.instance_id=?''', (instance,)).fetchall()
        result = []
        for row in rows:
            if control is not None:
                control.check_cancelled()
            if row['descriptor']:
                summary = json.loads(row['summary'])
                relevant = allows(summary, query, 0, MAX_TIME)
                entry = json.loads(row['descriptor'])
                result.append((row['id'], int(row['lo']), int(row['hi']), self.binding(entry), relevant))
            else:
                result.append((row['id'], epoch(row['log_begin_utc'], 0),
                               epoch(row['log_end_utc'], MAX_TIME), '', True))
        return result

    def coverage(self, query, *, conn=None, catalog=None):
        if not query.get('instance'):
            return {'intervals': [], 'reason': '请选择一个实例', 'indexedFiles': 0, 'pendingFiles': 0}
        catalog = self.catalog(query) if catalog is None else catalog
        if conn is None:
            if not self.path.exists():
                return {'intervals': [], 'reason': '后台尚未建立事件索引', 'indexedFiles': 0, 'pendingFiles': len(catalog)}
            with self.connection() as reader:
                return self.coverage(query, conn=reader, catalog=catalog)
        files = {r['file_id']: r for r in conn.execute('SELECT * FROM files')}
        good, bad, valid = [], [], []
        pruned = 0
        for file_id, lo, hi, digest, relevant in catalog:
            if not relevant:
                # The verified envelope index proves this file cannot contain
                # the requested table, including negative/empty queries.
                if lo > 0 and hi < MAX_TIME:
                    good.append((lo, hi))
                pruned += 1
                continue
            cached = files.get(file_id)
            if cached and cached['complete'] and cached['signature'] == digest:
                valid.append(file_id)
                # Opaque events span all time for pruning, not a proof of
                # continuous history from epoch zero to infinity.
                if lo > 0 and hi < MAX_TIME:
                    good.append((lo, hi))
                elif cached['first_stamp'] is not None and cached['last_stamp'] is not None:
                    good.append((cached['first_stamp'], cached['last_stamp']))
            else:
                bad.append((lo, hi))
        intervals = subtract(good, union(bad))
        return {'intervals': intervals, 'indexedFiles': len(valid), 'pendingFiles': len(catalog)-len(valid)-pruned, 'prunedFiles': pruned,
                'reason': '' if intervals else '没有连续且完整的已索引区间；请等待后台索引',
                'scope': 'registered-source-catalog', '_valid_files': valid}

    def pending(self):
        with self.metadata.connection() as conn:
            entries = [json.loads(row[0]) for row in conn.execute(
                'SELECT descriptor FROM raw_binlog_archives ORDER BY hi DESC,file_id')]
        done = {}
        if self.path.exists():
            with self.connection() as conn:
                done = {r['file_id']: r for r in conn.execute('SELECT * FROM files')}
        for entry in entries:
            current = done.get(entry['file_id'])
            if current and current['signature'] == self.binding(entry) and (current['complete'] or current['retry_at'] > time.time()):
                continue
            yield entry

    def build(self, entry, rows, exact_index, checkpoint=lambda: None):
        """Chunk commits are invisible until EOF, checksums and source match.

        A killed worker leaves an incomplete file, never a partially valid page.
        The worker is single-writer; the existing supervisor owns its lifecycle.
        """
        file_id, digest = entry['file_id'], self.binding(entry)
        cache, count, batch_bytes = {}, 0, 0
        first_stamp = last_stamp = None
        payload_rows, payload_id = [], None
        with self.connection(write=True) as conn:
            conn.execute('INSERT OR IGNORE INTO files(file_id,signature) VALUES (?,?)', (file_id, digest))
            conn.execute('UPDATE files SET complete=0,error=\'\',signature=? WHERE file_id=?', (digest, file_id))
            conn.commit()
            # Bound reclamation on a retry; no giant rollback/journal burst.
            while True:
                checkpoint()
                ids = [r[0] for r in conn.execute('SELECT event_id FROM events WHERE file_id=? LIMIT 256', (file_id,))]
                if not ids:
                    break
                conn.executemany('DELETE FROM events WHERE event_id=?', ((key,) for key in ids))
                conn.commit()
            self._remove_payloads(conn, file_id, checkpoint)
            conn.execute('DELETE FROM types WHERE file_id=?', (file_id,))

            def flush_payload():
                nonlocal batch_bytes, payload_id
                if payload_id is not None:
                    conn.execute('UPDATE payloads SET body=? WHERE id=?',
                                 (zlib.compress(b'\n'.join(payload_rows), level=1), payload_id))
                    conn.commit()
                payload_rows.clear()
                payload_id, batch_bytes = None, 0
                checkpoint()

            try:
                for row in rows:
                    encoded = json.dumps(row, ensure_ascii=False, separators=(',', ':')).encode()
                    if len(encoded) > 32*1024**2:
                        raise RawBinlogError('单个事件超过索引结果预算', 'RAW_QUERY_RESULT_LIMIT')
                    if len(payload_rows) >= 256 or (payload_rows and batch_bytes+len(encoded) > 4*1024**2):
                        flush_payload()
                    if payload_id is None:
                        checkpoint()
                        payload_id = conn.execute('INSERT INTO payloads(file_id,body) VALUES (?,?)', (file_id, b'')).lastrowid
                    context = exact_index._primary_context(row, cache)
                    values = (row['event_id'], file_id, row['instance_id'], str(row.get('database_name') or '').lower(),
                        str(row.get('table_name') or '').lower(), int(row.get('event_epoch_us') or 0), row['source_file_name'],
                        int(row.get('end_position') or 0), int(row.get('row_index') or 0), row['operation'],
                        str(row.get('transaction_id') or ''), str(row.get('gtid') or ''), context['state'],
                        payload_id, len(payload_rows))
                    conn.execute('INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', values)
                    payload_rows.append(encoded)
                    if context['state'] == 'supported':
                        type_id = int(context['type_id'])
                        conn.execute('INSERT OR IGNORE INTO types VALUES (?,?,?,?)', (file_id, values[3], values[4], type_id))
                        for value in {v[2] for v in context['values']}:
                            conn.execute('INSERT INTO keys VALUES (?,?,?,?,?,?,?,?,?,?)',
                                (values[2], values[3], values[4], type_id, value, *values[5:9], values[0]))
                    count += 1
                    stamp = values[5]
                    first_stamp = stamp if first_stamp is None else min(first_stamp, stamp)
                    last_stamp = stamp if last_stamp is None else max(last_stamp, stamp)
                    batch_bytes += len(encoded)+1
                flush_payload()
                current = self.metadata.connection()
                with current as source:
                    latest = source.execute('SELECT descriptor FROM raw_binlog_archives WHERE file_id=?', (file_id,)).fetchone()
                if not latest or self.binding(json.loads(latest[0])) != digest:
                    raise RawBinlogError('索引期间原档目录发生变化', 'RAW_INDEX_SOURCE_CHANGED')
                conn.execute('UPDATE files SET complete=1,row_count=?,first_stamp=?,last_stamp=?,retry_at=0,error=\'\' WHERE file_id=?',
                             (count, first_stamp, last_stamp, file_id))
                conn.commit()
            except BaseException as exc:
                conn.rollback()
                conn.execute('UPDATE files SET complete=0,error=?,retry_at=? WHERE file_id=?', (str(exc)[:1000], time.time()+300, file_id))
                conn.commit()
                LOGGER.error('RAW_EVENT_INDEX_FAILED file=%s reason=%s', file_id, exc)
                raise
        return {'fileId': file_id, 'rows': count}

    @staticmethod
    def _remove_payloads(conn, file_id, checkpoint):
        while True:
            checkpoint()
            ids = [r[0] for r in conn.execute('SELECT id FROM payloads WHERE file_id=? LIMIT 4', (file_id,))]
            if not ids:
                return
            conn.executemany('DELETE FROM payloads WHERE id=?', ((key,) for key in ids))
            conn.commit()

    @staticmethod
    def _payload_block(conn, block_id):
        record = conn.execute('SELECT body FROM payloads WHERE id=?', (block_id,)).fetchone()
        if record is None:
            raise RawBinlogError('索引正文块缺失', 'RAW_INDEX_PAYLOAD_MISSING')
        decoder = zlib.decompressobj()
        plain = decoder.decompress(record[0], 32*1024**2+1)
        if len(plain) > 32*1024**2 or not decoder.eof:
            raise RawBinlogError('索引正文块超过预算', 'RAW_QUERY_RESULT_LIMIT')
        return plain.split(b'\n')

    def reclaim(self, checkpoint=lambda: None):
        """Reclaim derivatives only after their source manifest is retired."""
        if not self.path.exists():
            return 0
        with self.metadata.connection() as source:
            live = {r[0] for r in source.execute('SELECT file_id FROM raw_binlog_archives')}
        removed = 0
        with self.connection(write=True) as conn:
            stale = [r[0] for r in conn.execute('SELECT file_id FROM files') if r[0] not in live]
            for file_id in stale:
                conn.execute('UPDATE files SET complete=0 WHERE file_id=?', (file_id,))
                conn.commit()
                while True:
                    checkpoint()
                    ids = [r[0] for r in conn.execute('SELECT event_id FROM events WHERE file_id=? LIMIT 256', (file_id,))]
                    if not ids:
                        break
                    conn.executemany('DELETE FROM events WHERE event_id=?', ((key,) for key in ids))
                    conn.commit()
                    removed += len(ids)
                self._remove_payloads(conn, file_id, checkpoint)
                conn.execute('DELETE FROM files WHERE file_id=?', (file_id,))
                conn.commit()
            checkpoint()
            conn.execute('PRAGMA incremental_vacuum(256)')
        if removed:
            LOGGER.info('RAW_INDEX_RECLAIM rows=%d retired_files=%d', removed, len(stale))
        return removed

    def detail(self, event_id, locator, instance):
        if not self.path.exists():
            return None
        with self.connection() as conn:
            record = conn.execute('''SELECT e.file_id,e.instance,e.payload_id,e.payload_ordinal,f.signature FROM events e
                JOIN files f ON f.file_id=e.file_id AND f.complete=1 WHERE e.event_id=?''', (event_id,)).fetchone()
            if not record or (instance and record['instance'] != instance):
                return None
            with self.metadata.connection() as source:
                current = source.execute('SELECT descriptor FROM raw_binlog_archives WHERE file_id=?', (record['file_id'],)).fetchone()
            if not current or self.binding(json.loads(current[0])) != record['signature']:
                return None
            block = self._payload_block(conn, record['payload_id'])
            row = json.loads(block[record['payload_ordinal']])
            return row if row.get('event_locator') == locator else None

    def query(self, query, start, end, *, control=None):
        for name in ('instance', 'database', 'table'):
            if not str(query.get(name) or '').strip():
                raise RawBinlogError('索引快查要求单实例、完整库名和表名', 'INDEX_QUERY_SCOPE_REQUIRED')
        if str(query.get('source') or '') != 'binlog' or any(query.get(k) for k in ('keyword', 'connection', 'account', 'status', 'fingerprint')):
            raise RawBinlogError('此条件不支持索引快查；请显式选择高级扫描', 'INDEX_QUERY_UNSUPPORTED')
        if not self.path.exists():
            raise RawBinlogError('后台尚未完成事件索引', 'INDEX_COVERAGE_INCOMPLETE')
        until = time.monotonic()+10
        def check():
            if control is not None:
                control.check_cancelled()
            if time.monotonic() >= until:
                raise RawBinlogError('索引查询超过10秒，未返回不完整结果', 'QUERY_DEADLINE_EXCEEDED')
        check()
        catalog = self.catalog({**query, 'exact': query.get('exact') or {'kind': 'SCOPE'}}, control=SimpleNamespace(check_cancelled=check))
        check()
        limit, offset = min(max(int(query.get('limit') or 100), 1), 1000), int(query.get('offset') or 0)
        if offset < 0 or offset+limit+1 > 2000:
            raise RawBinlogError('索引分页深度超过2000，请缩小区间', 'RAW_QUERY_PAGE_LIMIT')
        with self.connection() as conn:
            conn.execute('BEGIN')
            failure = []
            def interrupt():
                try:
                    check()
                    return 0
                except BaseException as exc:
                    failure.append(exc)
                    return 1
            conn.set_progress_handler(interrupt, 1000)
            try:
                coverage = self.coverage(query, conn=conn, catalog=catalog)
                if not any(lo <= start <= end <= hi for lo, hi in coverage['intervals']):
                    raise RawBinlogError('所选时间含未索引或缺失文件；请用已索引时间快捷选项', 'INDEX_COVERAGE_INCOMPLETE')
                # A read-only TEMP table pins the exact committed source set.
                conn.execute('CREATE TEMP TABLE eligible(id TEXT PRIMARY KEY) WITHOUT ROWID')
                conn.executemany('INSERT INTO eligible VALUES (?)', ((key,) for key in coverage['_valid_files']))
                scope = (query['instance'], query['database'].lower(), query['table'].lower(), start, end)
                where = 'e.instance=? AND e.db=? AND e.tbl=? AND e.stamp BETWEEN ? AND ? AND e.file_id IN (SELECT id FROM eligible)'
                args = list(scope)
                if query.get('operations'):
                    where += ' AND e.operation IN ('+','.join('?' for _ in query['operations'])+')'
                    args.extend(query['operations'])
                if query.get('transaction'):
                    where += ' AND (e.txn=? OR e.gtid=?)'
                    args.extend([query['transaction']]*2)
                exact = query.get('exact')
                if exact:
                    if conn.execute('SELECT 1 FROM events e WHERE '+where+' AND e.pk_state=\'unknown\' LIMIT 1', args).fetchone():
                        raise RawBinlogError('区间内存在无法验证的历史主键schema', 'EXACT_SCHEMA_UNKNOWN')
                    candidates = []
                    type_ids = [r[0] for r in conn.execute('SELECT DISTINCT type_id FROM types WHERE db=? AND tbl=? AND file_id IN (SELECT id FROM eligible)', (scope[1], scope[2]))]
                    for type_id in type_ids:
                        key = canonical_value(exact['value'], type_id, query=True)
                        if key is None:
                            raise RawBinlogError('主键值与历史字段类型不匹配', 'EXACT_SCHEMA_UNKNOWN')
                        sql = '''SELECT e.event_id,e.stamp,e.filename,e.position,e.ordinal FROM keys k JOIN events e ON e.event_id=k.event_id
                            WHERE k.instance=? AND k.db=? AND k.tbl=? AND k.type_id=? AND k.value=? AND k.stamp BETWEEN ? AND ? AND '''+where
                        sql += ' ORDER BY '+','.join('k.'+piece.strip() for piece in ORDER.split(','))+' LIMIT ?'
                        candidates.extend(conn.execute(sql, [*scope[:3], type_id, key, start, end, *args, offset+limit+1]).fetchall())
                else:
                    sql = 'SELECT e.event_id,e.stamp,e.filename,e.position,e.ordinal FROM events e WHERE '+where+' ORDER BY '+','.join('e.'+p.strip() for p in ORDER.split(','))+' LIMIT ?'
                    candidates = conn.execute(sql, [*args, offset+limit+1]).fetchall()
                unique = {r['event_id']: r for r in candidates}
                ordered = sorted(unique.values(), key=lambda r: (r['stamp'], r['filename'], r['position'], r['ordinal'], r['event_id']), reverse=True)
                rows, size, block_id, block = [], 0, None, []
                for record in ordered[offset:offset+limit]:
                    check()
                    payload = conn.execute('SELECT payload_id,payload_ordinal FROM events WHERE event_id=?', (record['event_id'],)).fetchone()
                    if block_id != payload['payload_id']:
                        block = self._payload_block(conn, payload['payload_id'])
                        block_id = payload['payload_id']
                    plain = block[payload['payload_ordinal']]
                    size += len(plain)
                    if size > 32*1024**2:
                        raise RawBinlogError('索引结果超过32MiB预算', 'RAW_QUERY_RESULT_LIMIT')
                    rows.append(json.loads(plain))
                check()
                return {'rows': rows, 'has_more': len(ordered)>offset+limit,
                    'limit': limit, 'offset': offset, 'tiers_used': ['raw-event-index'],
                    'range_requests': 0, 'range_bytes': 0, 'exact_index_complete': True,
                    'coverage_found': True, 'range_start_epoch_us': start, 'range_end_epoch_us': end,
                    'coverage_note': '已登记原档在所选区间完整索引；未发现的历史源缺口不在证明范围内'}
            except sqlite3.OperationalError:
                if failure:
                    raise failure[0]
                raise
            finally:
                conn.set_progress_handler(None, 0)
