"""One owned query process. No metadata DB, index writes, pipeline or full-object fallback."""
from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import sys
import time
from contextlib import ExitStack

MAX_READ_BYTES = 256 * 1024 * 1024
MAX_REQUESTS = 4096
MAX_OBJECTS = 4096
MAX_GROUP_BYTES = 64 * 1024 * 1024
MAX_RESULT_BYTES = 16 * 1024 * 1024


def run_query(payload, emit=lambda message: None, *, archive=None, data_dir=None, scratch=None):
    from app.indexed_query_client import IndexedQueryError
    from app.config import Settings
    from app.oss_store import OssArchive
    from app.storage import EventStorage, QUERY_RESULT_COLUMNS
    import pyarrow as pa
    import pyarrow.parquet as pq

    pa.set_cpu_count(1)
    pa.set_io_thread_count(1)
    deadline = time.monotonic() + min(float(payload.get('seconds', 55)), 55)
    root = Path(data_dir or os.environ.get('RDS_BINLOG_DATA_DIR', '/data')).resolve() / 'events'
    settings = Settings.from_mapping(payload['settings'])
    query = dict(payload['query'])
    limit, offset = int(query['limit']), int(query.get('offset', 0))
    if not 1 <= limit <= 100_000 or not 0 <= offset <= 100_000:
        raise IndexedQueryError('分页参数越界', 'QUERY_INVALID_PAGE')
    target = limit + offset + 1
    internal = {**query, 'limit': target, 'offset': 0}
    columns = ['event_time_utc', *QUERY_RESULT_COLUMNS]
    work = payload['work']
    totals = dict(range_requests=0, range_bytes=0, local_read_bytes=0,
                  budget_bytes=0, budget_requests=0, local_parts_read=0,
                  oss_range_parts_read=0, candidate_blocks=0,
                  positive_probe_cached_parts=0, predicate_row_groups_scanned=0,
                  predicate_row_groups_selected=0)
    top = {}
    complete = True

    def fail(message, code):
        raise IndexedQueryError(message, code)

    def check():
        if time.monotonic() >= deadline:
            fail('正文扫描超过时限', 'QUERY_DEADLINE_EXCEEDED')

    def charge_local(length):
        check()
        if totals['budget_bytes'] + length > MAX_READ_BYTES:
            fail('跨对象读取字节预算耗尽', 'OSS_QUERY_BUDGET_EXCEEDED')
        totals['budget_bytes'] += length
        totals['local_read_bytes'] += length

    class LocalReader(io.RawIOBase):
        def __init__(self, handle):
            self.handle = handle
        def readable(self):
            return True
        def seekable(self):
            return True
        def tell(self):
            return self.handle.tell()
        def seek(self, *args):
            return self.handle.seek(*args)
        def read(self, size=-1):
            if size < 0:
                size = os.fstat(self.handle.fileno()).st_size - self.tell()
            charge_local(size)
            return self.handle.read(size)

    class BoundedStorage(EventStorage):
        def _duckdb_connect(self, database=':memory:'):
            conn = super()._duckdb_connect(database)
            try:
                conn.execute("SET memory_limit='128MB'; SET max_temp_directory_size='128MB'")
            except BaseException:
                conn.close()
                raise
            return conn

    # Deliberately do not call EventStorage.__init__: it creates writable indexes.
    storage = object.__new__(BoundedStorage)
    storage.paths = {'scratch': Path(scratch or os.environ.get('TMPDIR', '/scratch'))}

    def merge(rows, more=False):
        nonlocal top, complete
        for row in rows:
            previous = top.get(row['event_id'])
            if previous is not None and (
                {k: v for k, v in previous.items() if k != 'locator'} !=
                {k: v for k, v in row.items() if k != 'locator'}
            ):
                fail('同一事件身份对应不同内容', 'QUERY_IDENTITY_CONFLICT')
            top.setdefault(row['event_id'], row)
        ordered = sorted(top.values(), key=storage._row_sort_key, reverse=True)
        complete = complete and not more and len(ordered) <= target
        top = {row['event_id']: row for row in ordered[:target]}
        if len(json.dumps(list(top.values()), ensure_ascii=False).encode('utf-8')) > MAX_RESULT_BYTES:
            fail('完整字段结果超过16 MiB，未截字段或返回残缺页', 'QUERY_RESULT_BUDGET_EXCEEDED')

    for number, entry in enumerate(work):
        check()
        before_bytes = totals['local_read_bytes'] + totals['range_bytes']
        part = entry['part']
        if 'cached_rows' in entry:
            merge(entry['cached_rows'])
            totals['positive_probe_cached_parts'] += 1
        else:
            if totals['local_parts_read'] + totals['oss_range_parts_read'] >= MAX_OBJECTS:
                fail('跨对象数量预算耗尽', 'QUERY_OBJECT_BUDGET_EXCEEDED')
            with ExitStack() as stack:
                remote = bool(settings.oss_enabled and part.get('oss_key') and part.get('oss_etag'))
                if remote:
                    if archive is None:
                        archive = OssArchive(settings)
                    reader = stack.enter_context(archive.open_part_reader(
                        part, max_bytes=MAX_READ_BYTES - totals['budget_bytes'],
                        max_requests=MAX_REQUESTS - totals['budget_requests'],
                        check_cancelled=check))
                    totals['oss_range_parts_read'] += 1
                else:
                    path = Path(part['path']).resolve()
                    if not path.is_relative_to(root) or not path.is_file():
                        fail('正文缺失或没有固定 OSS ETag', 'QUERY_IDENTITY_UNPINNED')
                    handle = stack.enter_context(path.open('rb'))
                    initial_stat = os.fstat(handle.fileno())
                    if initial_stat.st_size != int(part['size_bytes']):
                        fail('本地正文长度改变', 'OSS_RANGE_VERIFY_FAILED')
                    # Hash and decode the SAME descriptor, safe across atomic replacement.
                    digest = hashlib.sha256()
                    remaining = int(part['size_bytes'])
                    while remaining:
                        size = min(remaining, 1024 * 1024)
                        charge_local(size)
                        chunk = handle.read(size)
                        if len(chunk) != size:
                            fail('本地正文读取不完整', 'OSS_RANGE_VERIFY_FAILED')
                        digest.update(chunk)
                        remaining -= size
                    if digest.hexdigest() != part['sha256']:
                        fail('本地正文身份改变', 'OSS_RANGE_VERIFY_FAILED')
                    handle.seek(0)
                    reader = LocalReader(handle)
                    totals['local_parts_read'] += 1
                try:
                    parquet = pq.ParquetFile(reader)
                    requested = entry.get('row_groups')
                    groups = list(range(parquet.num_row_groups)) if requested is None else sorted(set(requested))
                    if any(g < 0 or g >= parquet.num_row_groups for g in groups):
                        fail('索引指向不存在的行组', 'INDEX_ROW_GROUP_INVALID')
                    selected = [name for name in columns if name in parquet.schema_arrow.names]
                    for group in groups:
                        check()
                        if parquet.metadata.row_group(group).total_byte_size > MAX_GROUP_BYTES:
                            fail('单行组未压缩编码大小超过64 MiB', 'QUERY_DECODE_BUDGET_EXCEEDED')
                        if requested is None:
                            selected_groups, scanned = storage._structural_candidate_row_groups(
                                parquet, [group], internal, part)
                            totals['predicate_row_groups_scanned'] += scanned
                            totals['predicate_row_groups_selected'] += len(selected_groups)
                            if not selected_groups:
                                continue
                        table = parquet.read_row_group(group, columns=selected, use_threads=False)
                        table = storage._with_audit_columns(table, columns)
                        page = storage._query_arrow_table(
                            table, internal, settings.retention_days,
                            locator=f"{part.get('logical_part_id') or part['sha256']}:{group}",
                            limit_cap=target, deduplicate=True)
                        merge(page['rows'], page['has_more'])
                        totals['candidate_blocks'] += 1
                        del table, page
                    if not remote:
                        final_stat = os.fstat(handle.fileno())
                        if (initial_stat.st_size, initial_stat.st_mtime_ns, initial_stat.st_ctime_ns) != (
                            final_stat.st_size, final_stat.st_mtime_ns, final_stat.st_ctime_ns
                        ):
                            fail('读取期间本地正文改变', 'OSS_RANGE_VERIFY_FAILED')
                finally:
                    if remote:
                        totals['range_requests'] += reader.request_count
                        totals['range_bytes'] += reader.bytes_read
                        totals['budget_bytes'] += reader.reserved_bytes
                        totals['budget_requests'] += reader.request_count
                    emit({'usage': dict(totals)})
        emit({'progress': {'file': Path(part['path']).name,
                           'bytes': totals['local_read_bytes'] + totals['range_bytes'] - before_bytes}})
        if len(top) >= target and number + 1 < len(work):
            boundary = min(row['event_epoch_us'] for row in top.values())
            # Equal timestamps require the full stable ordering key; never prune ties.
            if int(work[number + 1]['max_event_epoch_us']) < boundary:
                complete = False
                break
    check()
    ordered = sorted(top.values(), key=storage._row_sort_key, reverse=True)
    result = {**totals, 'rows': ordered[offset:offset + limit],
              'limit': limit, 'offset': offset, 'has_more': len(ordered) > offset + limit,
              'backend': 'indexed-parquet-worker', 'query_scan_workers': 1,
              'oss_parts_read': totals['oss_range_parts_read'],
              'oss_temporary_parts_read': 0, 'oss_downloaded_parts': 0,
              'full_object_fallback_bytes': 0, 'query_cache_parts_read': 0,
              'query_certificate_hit': False,
              'tiers_used': (['local-index'] if totals['local_parts_read'] or totals['positive_probe_cached_parts'] else [])
                            + (['oss-range'] if totals['oss_range_parts_read'] else [])}
    if complete:
        result['complete_rows'] = ordered
    return result


def main():
    def emit(message):
        print(json.dumps(message, ensure_ascii=False, separators=(',', ':')), flush=True)
    try:
        payload = json.loads(sys.stdin.buffer.read(16 * 1024 * 1024 + 1))
        emit({'result': run_query(payload, emit)})
    except Exception as exc:
        emit({'error': {'code': getattr(exc, 'code', 'INDEXED_QUERY_FAILED'),
                        'message': str(exc)[:512]}})
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
