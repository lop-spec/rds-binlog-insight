"""Bounded, cancellable query-time decoding of complete transaction ranges."""
from __future__ import annotations

import gzip
import hashlib
import heapq
import json
import logging
import tempfile
import threading
import time
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

from .binlog_lite import MAX_TIME, RawBinlogError, allows
from .raw_binlog import MAX_BYTES, QUERY_SECONDS
from .parser_bridge import parse_ndjson_chunks

LOGGER = logging.getLogger(__name__)


class Budget:
    def __init__(self, control=None):
        self.control = control
        self.until = time.monotonic() + QUERY_SECONDS
        self.bytes = 0
        self.requests = 0
        self.cancel = threading.Event()
        self.timer = threading.Timer(QUERY_SECONDS, self.cancel.set)
        self.timer.daemon = True
        self.timer.start()

    def check(self):
        if self.control is not None:
            self.control.check_cancelled()
        if self.cancel.is_set() or time.monotonic() >= self.until:
            self.cancel.set()
            raise RawBinlogError(f'单批原始 Binlog 查询超过 {QUERY_SECONDS} 秒，已停止；未返回不完整结果', 'QUERY_DEADLINE_EXCEEDED')

    def add(self, size):
        self.check()
        self.bytes += size
        if self.bytes > MAX_BYTES:
            self.cancel.set()
            raise RawBinlogError('原始 Binlog 查询读取字节超限', 'QUERY_BINLOG_BYTE_LIMIT')

    def close(self):
        self.cancel.set()
        self.timer.cancel()


def read_object(archive, item, budget):
    if item['size_bytes'] > 32 * 1024**2:
        raise RawBinlogError('轻量索引对象过大，拒绝超限读取', 'RAW_INDEX_SIZE_LIMIT')
    response = archive.bucket.get_object(item['oss_key'])
    budget.requests += 1
    try:
        pieces = []
        while True:
            budget.check()
            chunk = response.read(1024**2)
            if not chunk:
                break
            budget.add(len(chunk))
            pieces.append(chunk)
            if sum(map(len, pieces)) > item['size_bytes']:
                raise RawBinlogError('轻量索引长度异常')
        data = b''.join(pieces)
    finally:
        response.close()
    if len(data) != item['size_bytes'] or hashlib.sha256(data).hexdigest() != item['sha256']:
        raise RawBinlogError('轻量索引 SHA256 或长度校验失败', 'RAW_INDEX_VERIFY_FAILED')
    import io
    with gzip.GzipFile(fileobj=io.BytesIO(data)) as handle:
        decoded = handle.read(256*1024**2+1)
    if len(decoded) > 256*1024**2:
        raise RawBinlogError('轻量索引解压大小超限', 'RAW_INDEX_SIZE_LIMIT')
    return json.loads(decoded)


def copy_range(archive, item, start, end, target, budget):
    if not 0 <= start < end <= item['size_bytes']:
        raise RawBinlogError('轻量索引字节范围越界')
    budget.check()
    response = archive.bucket.get_object(item['oss_key'], byte_range=(start, end-1))
    budget.requests += 1
    received = 0
    try:
        expected_range = f'bytes {start}-{end-1}/{item["size_bytes"]}'
        if response.headers.get('Content-Range', response.headers.get('content-range', '')) != expected_range:
            raise RawBinlogError('OSS 未按请求返回准确字节区间', 'RAW_RANGE_INVALID')
        while True:
            budget.check()
            chunk = response.read(min(1024**2, end-start-received+1))
            if not chunk:
                break
            received += len(chunk)
            budget.add(len(chunk))
            if received > end-start:
                raise RawBinlogError('OSS 区间读取超出预期长度', 'RAW_RANGE_INVALID')
            target.write(chunk)
    finally:
        response.close()
    if received != end-start:
        raise RawBinlogError('OSS 区间读取截断', 'RAW_RANGE_INVALID')


def decode(storage, archive, entry, query, start, end, budget, *, position=None):
    if archive is None:
        raise RawBinlogError('查询原始 Binlog 需要 OSS', 'RAW_ARCHIVE_OSS_REQUIRED')
    storage.raw_binlogs.verify(archive, entry)
    index = read_object(archive, entry['index'], budget)
    regions = [r for r in index['regions'] if (
        r['start'] <= position < r['end'] if position is not None else allows(r, query, start, end))]
    if not regions:
        return
    # Merge adjacent eligible transactions. No row image is cut in half. The
    # original FDE and GTID/TableMap/RowsQuery context accompanies every range.
    ranges = [(0, index['prefix_end'])]
    for region in regions:
        a, b = region['start'], region['end']
        if a <= ranges[-1][1]:
            ranges[-1] = (ranges[-1][0], max(b, ranges[-1][1]))
        else:
            ranges.append((a, b))
    if sum(b-a for a,b in ranges) > 512*1024**2:
        raise RawBinlogError('候选事务超过 512 MiB 临时文件预算，请缩小时间或库表范围', 'RAW_QUERY_STAGING_LIMIT')
    with tempfile.TemporaryDirectory(prefix='raw-query-', dir=storage.paths['scratch']) as directory:
        root = Path(directory)
        reduced = root/'selected.binlog'
        with reduced.open('wb') as output:
            for a, b in ranges:
                copy_range(archive, entry['raw'], a, b, output, budget)
        # Native IDs include the global decoded-row ordinal and therefore change
        # after range pruning. New raw-archive events use a versioned per-GTID
        # ordinal; existing Parquet IDs and old detail links are unchanged.
        ordinals = {}
        gtid_positions = {r.get('gtid'): r['start'] for r in index['regions'] if r.get('gtid')}
        chunks = parse_ndjson_chunks(reduced, entry['file_id'], root/'decoded',
                                     entry.get('flavor') or 'mysql', max_lines=1000,
                                     max_bytes=8*1024**2, cancel_event=budget.cancel,
                                     no_progress_seconds=30)
        with closing(chunks):
            try:
                for path in chunks:
                    try:
                        with path.open(encoding='utf-8') as handle:
                            for line in handle:
                                budget.check()
                                row = json.loads(line)
                                yield normalize_row(row, entry, ordinals, gtid_positions)
                    finally:
                        path.unlink(missing_ok=True)
            except Exception:
                budget.check()  # Preserve user cancellation/deadline over parser wrapper errors.
                raise


def normalize_row(row, entry, ordinals, gtid_positions):
    """One identity contract for range decoding and asynchronous full-file indexing."""
    row.update(instance_id=entry['instance_id'], host_instance_id=entry['host_instance_id'],
               source_file_id=entry['file_id'], source_file_name=entry['source_file_name'])
    row['operation'] = str(row.get('operation') or 'OTHER').upper()
    transaction = str(row.get('gtid') or row.get('transaction_id') or 'ungrouped')
    ordinals[transaction] = ordinals.get(transaction, 0)+1
    identity = '\x1f'.join(('raw-v1', entry['file_id'], transaction, str(ordinals[transaction])))
    row['event_id'] = hashlib.sha256(identity.encode()).hexdigest()
    row['event_time_utc'] = datetime.fromtimestamp(int(row.get('event_epoch_us') or 0)/1e6, UTC).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    position = gtid_positions.get(row.get('gtid'), 4)
    row['event_locator'] = f"raw:{entry['file_id']}:{position}"
    return row


def matches(storage, row, query, start, end, schema_cache):
    if not start <= int(row.get('event_epoch_us') or 0) <= end:
        return False
    exact = query.get('exact')
    for key, columns in (
        ('instance', ('instance_id',)), ('database', ('database_name',)), ('table', ('table_name',)),
        ('connection', ('connection_id', 'connection_name')), ('account', ('database_account',)), ('status', ('execution_status',))):
        value = str(query.get(key) or '').strip().lower()
        if not value:
            continue
        values = [str(row.get(c) or '').lower() for c in columns]
        equal = key in {'instance', 'status'} or isinstance(exact, dict)
        if not any(value == v if equal else value in v for v in values):
            return False
    ops = [str(v).upper() for v in query.get('operations') or [] if str(v).strip()]
    if ops and row['operation'] not in ops:
        return False
    transaction = str(query.get('transaction') or '').strip()
    if transaction and transaction not in {row.get('transaction_id'), row.get('gtid')}:
        return False
    terms = str(query.get('keyword') or '').strip().lower().split()[:20]
    if terms:
        text = [str(row.get(k) or '').lower() for k in ('sql_text', 'before_json', 'after_json', 'transaction_id',
                 'source_file_name', 'connection_name', 'database_account', 'error_message')]
        checks = [any(t in value for value in text) for t in terms]
        if not (any(checks) if str(query.get('keyword_mode')).upper() == 'OR' else all(checks)):
            return False
    if isinstance(exact, dict):
        result = storage.exact_index.primary_key_match(row, exact.get('value'), schema_cache=schema_cache)
        if result is None:
            raise RawBinlogError('原始行镜像缺少可验证的主键 schema，不能把未知当作不匹配', 'EXACT_SCHEMA_UNKNOWN')
        return result
    return True


def query_raw(storage, archive, query, start, end, plan, control=None, limit_cap=1000):
    if not storage.raw_binlogs.query_lock.acquire(blocking=False):
        raise RawBinlogError('已有原始 Binlog 查询运行中，请稍后重试', 'RAW_QUERY_BUSY')
    budget = Budget(control)
    limit = min(max(int(query.get('limit') or 100), 1), limit_cap)
    offset = min(max(int(query.get('offset') or 0), 0), 100_000)
    keep = limit+offset+1
    # Bound retained results independently from potentially large candidate files.
    if keep > 2000:
        budget.close()
        storage.raw_binlogs.query_lock.release()
        raise RawBinlogError('原始 Binlog 单次分页深度上限 2000；请缩小时间范围', 'RAW_QUERY_PAGE_LIMIT')
    heap, schema_cache, sequence = [], {}, 0
    retained, weights = 0, {}
    try:
        if control is not None:
            control.set_plan(total_parts=plan['candidate_files'], candidate_parts=plan['candidate_files'],
                             indexed_parts=len(plan['raw']), unknown_parts=0, estimated_bytes=plan['estimated_bytes'])
        for entry in plan['raw']:
            budget.check()
            with closing(decode(storage, archive, entry, query, start, end, budget)) as decoded:
                for row in decoded:
                    if matches(storage, row, query, start, end, schema_cache):
                        sequence += 1
                        value = (storage._row_sort_key(row), sequence, row)
                        if len(heap) < keep or value[0] > heap[0][0]:
                            weight = len(json.dumps(row,ensure_ascii=False).encode('utf-8'))
                            if len(heap) == keep:
                                removed = heapq.heapreplace(heap, value)
                                retained -= weights.pop(removed[1])
                            else:
                                heapq.heappush(heap, value)
                            weights[sequence] = weight
                            retained += weight
                            if retained > 32*1024**2:
                                raise RawBinlogError('查询结果超过 32 MiB 内存预算，请减少页大小或缩小范围', 'RAW_QUERY_RESULT_LIMIT')
            if control is not None:
                control.advance()
        rows = [r for _, _, r in sorted(heap, reverse=True)]
        LOGGER.info('RAW_QUERY_COMPLETE files=%s range_requests=%s bytes=%s matches=%s', len(plan['raw']), budget.requests, budget.bytes, sequence)
        return dict(rows=rows, has_more=sequence>keep, candidate_binlogs=plan['candidate_files'],
                    range_requests=budget.requests, range_bytes=budget.bytes,
                    raw_matches=sequence, tiers_used=['raw-binlog-oss'])
    finally:
        budget.close()
        storage.raw_binlogs.query_lock.release()


def event_detail(storage, archive, event_id, locator, instance=''):
    try:
        prefix, file_id, text = locator.split(':')
        position = int(text)
        if prefix != 'raw' or position < 4:
            return None
    except (ValueError, TypeError):
        return None
    entry = storage.raw_binlogs.get(file_id)
    if entry is None or (instance and entry['instance_id'] != instance):
        return None
    if entry['raw']['size_bytes'] > MAX_BYTES:
        raise RawBinlogError('事件所在 Binlog 超过单次查询大小上限', 'QUERY_BINLOG_LIMIT')
    if not storage.raw_binlogs.query_lock.acquire(blocking=False):
        raise RawBinlogError('已有原始 Binlog 查询运行中，请稍后重试', 'RAW_QUERY_BUSY')
    budget = Budget()
    try:
        with closing(decode(storage, archive, entry, {}, 0, MAX_TIME, budget, position=position)) as rows:
            for row in rows:
                if row['event_id'] == event_id:
                    return row
        return None
    finally:
        budget.close()
        storage.raw_binlogs.query_lock.release()
