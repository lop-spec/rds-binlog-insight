"""Serial P1 mechanism benchmark, never a production/30-day acceptance result.

Uses an independent Python oracle and the real SearchIndex/OssRangeReader/DuckDB
predicates. Range responses are served from a local immutable fixture, not OSS:
byte counts are actual returned payload bytes, timings are NOT network latency.
An optional --parquet input is read-only; all derived state lives in a temp dir.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import tempfile
import time
import sys
from threading import Timer
from types import SimpleNamespace

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from app.oss_store import OssRangeReader
from app.search_index import SearchIndex
from app.storage import EventStorage

# Kept independent of the index's field list so a missing index column fails.
KEYWORD_FIELDS = ('sql_text', 'before_json', 'after_json', 'transaction_id',
                  'source_file_name', 'connection_name', 'database_account', 'error_message')
MAX_INPUT_BYTES = 64 * 1024 * 1024
MAX_ROWS = 200_000
MAX_DECODED_BYTES = 512 * 1024 * 1024
MAX_STATE_BYTES = 512 * 1024 * 1024


def process_peak_rss_bytes():
    """Process-lifetime peak (includes oracle/index/Arrow), not a per-query peak."""
    if sys.platform != 'win32':
        import resource
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return int(value if sys.platform == 'darwin' else value * 1024)
    import ctypes
    from ctypes import wintypes
    class Counters(ctypes.Structure):
        _fields_ = [('cb', wintypes.DWORD), ('faults', wintypes.DWORD)] + [
            (name, ctypes.c_size_t) for name in ('peak', 'working', 'peak_paged',
                'paged', 'peak_nonpaged', 'nonpaged', 'pagefile', 'peak_pagefile')]
    current = ctypes.windll.kernel32.GetCurrentProcess
    current.restype = wintypes.HANDLE
    get = ctypes.windll.psapi.GetProcessMemoryInfo
    get.argtypes = [wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD]
    counters = Counters()
    counters.cb = ctypes.sizeof(counters)
    if not get(current(), ctypes.byref(counters), counters.cb):
        raise OSError('process peak RSS measurement failed')
    return counters.peak


def sort_key(row):
    return (int(row['event_epoch_us']), str(row.get('source_file_name') or ''),
            int(row.get('end_position') or 0), int(row.get('row_index') or 0),
            str(row.get('event_id') or ''))


def matches(row, query):
    epoch = int(row['event_epoch_us'])
    if not query['start_epoch_us'] <= epoch <= query['end_epoch_us']:
        return False
    for key, field in (('database', 'database_name'), ('table', 'table_name')):
        if query.get(key) and query[key].lower() not in str(row.get(field) or '').lower():
            return False
    if query.get('operations') and row.get('operation') not in query['operations']:
        return False
    terms = str(query.get('keyword') or '').lower().split()[:20]
    hits = [any(term in str(row.get(field) or '').lower() for field in KEYWORD_FIELDS)
            for term in terms]
    return not hits or (any(hits) if query.get('keyword_mode') == 'OR' else all(hits))


def page(rows, query):
    # Same canonical identity/ordering; oracle filtering is independent Python.
    unique = {}
    for row in rows:
        unique.setdefault(str(row['event_id']), row)
    ordered = sorted(unique.values(), key=sort_key, reverse=True)
    offset, limit = query.get('offset', 0), query.get('limit', 20)
    return ordered[offset:offset + limit], len(ordered) > offset + limit


def fingerprint(rows):
    return hashlib.sha256(json.dumps(rows, ensure_ascii=False, sort_keys=True,
                                     default=str).encode()).hexdigest()


def fixture(path, count):
    epoch = int(time.time() * 1_000_000) - count * 1000
    rows = []
    for n in range(count):
        group = n // 1024
        rows.append({
            'event_id': f'event-{n:08}', 'event_epoch_us': epoch + n * 1000,
            'database_name': 'example_app', 'table_name': f'table_{group % 8:02}',
            'operation': 'UPDATE', 'sql_text': f'update table_{group % 8:02} set value={n}',
            'before_json': json.dumps({'id': n, 'value': hashlib.sha256(str(n).encode()).hexdigest()}),
            'after_json': json.dumps({'id': n, 'value': hashlib.sha256(str(n + 1).encode()).hexdigest()}),
            'transaction_id': f'transaction-{hashlib.md5(str(group).encode()).hexdigest()}',
            'source_file_name': 'mysql-bin.000001', 'end_position': n + 1, 'row_index': n,
            'connection_name': 'unique_audit_connection' if n == count // 2 else '',
            'database_account': '', 'error_message': '',
        })
    pq.write_table(pa.Table.from_pylist(rows), path, compression='zstd',
                   compression_level=1, row_group_size=1024)


def native_page(conn, sql, params, deadline):
    """Return only after the owned native query finished or was interrupted."""
    remaining = deadline - time.perf_counter()
    if remaining <= 0:
        raise TimeoutError('P1 native query deadline expired before execution')
    timer = Timer(remaining, conn.interrupt)
    timer.daemon = True
    timer.start()
    try:
        result = conn.execute(sql, params).to_arrow_table().to_pylist()
    finally:
        timer.cancel()
        timer.join()
    if time.perf_counter() > deadline:
        raise TimeoutError('P1 native query exceeded its deadline')
    return result


def benchmark(path, root, *, require_reduction=False):
    if not 0 < path.stat().st_size <= MAX_INPUT_BYTES:
        raise ValueError('input must be a bounded immutable sample, at most 64 MiB')
    payload_sha = hashlib.sha256(path.read_bytes()).hexdigest()
    parquet = pq.ParquetFile(path)
    decoded = sum(parquet.metadata.row_group(n).total_byte_size for n in range(parquet.num_row_groups))
    if parquet.metadata.num_rows > MAX_ROWS or decoded > MAX_DECODED_BYTES:
        raise ValueError('sample exceeds row/decoded-byte budget; no index was created')
    rows = parquet.read(use_threads=False).to_pylist()
    groups = parquet.num_row_groups
    parquet.close()
    assert rows and all('event_id' in r for r in rows)
    if len({r['event_id'] for r in rows}) != len(rows):
        raise ValueError('single-part benchmark requires unique identities; use version/dedup integration tests for overlapping parts')
    part = {'path': str(path), 'sha256': payload_sha, 'row_count': len(rows),
            'min_event_epoch_us': min(r['event_epoch_us'] for r in rows),
            'max_event_epoch_us': max(r['event_epoch_us'] for r in rows)}
    print(json.dumps({'phase': 'index_start', 'rows': len(rows), 'input_bytes': path.stat().st_size,
                      'decoded_metadata_bytes': decoded}), flush=True)
    index = SearchIndex(root / 'search.sqlite3')
    started = time.perf_counter()
    index.index_parquet(part, path)
    index_seconds = time.perf_counter() - started
    state_bytes = sum(p.stat().st_size for p in root.glob('search.sqlite3*'))
    if state_bytes > MAX_STATE_BYTES:
        raise ValueError('index exceeds sample state budget; queries were not started')
    base = {'start_epoch_us': part['min_event_epoch_us'],
            'end_epoch_us': part['max_event_epoch_us'], 'limit': 20}
    chosen = rows[len(rows) // 2]
    needle = str(chosen.get('transaction_id') or chosen.get('after_json') or 'P1RareNeedle')
    needle = needle.split()[0][:128] or 'P1RareNeedle'
    cases = [
        ('selective', {**base, 'table': chosen.get('table_name', ''), 'keyword': needle}),
        ('zero', {**base, 'keyword': 'P1NoSuchValue_' + payload_sha}),
        ('common', {**base, 'keyword': 'id'}),
        ('short', {**base, 'keyword': 'a'}),
        ('and', {**base, 'keyword': needle + ' P1NoSuchValue_' + payload_sha}),
        ('or', {**base, 'keyword': needle + ' a', 'keyword_mode': 'OR'}),
        ('page', {**base, 'keyword': 'id', 'offset': 3, 'limit': 7}),
        ('audit', {**base, 'keyword': 'unique_audit_connection'}),
    ]
    results = []
    for ordinal, (name, query) in enumerate(cases):
        expected, expected_more = page([r for r in rows if matches(r, query)], query)
        per_case = {'case': name, 'oracle_rows': len(expected), 'oracle_has_more': expected_more}
        # Alternate order, fresh reader/cache for each path. No cache purge.
        for mode in (('full_scan', 'indexed') if ordinal % 2 == 0 else ('indexed', 'full_scan')):
            begin = time.perf_counter()
            if mode == 'indexed':
                plan = index.candidate_blocks([part], query,
                    start_epoch_us=base['start_epoch_us'], end_epoch_us=base['end_epoch_us'])
                assert not plan['unknown_paths']
                selected = sorted({entry['row_group_id'] for entry in plan['entries']})
            else:
                selected = list(range(groups))
            after_plan = time.perf_counter()
            read_seconds = native_seconds = 0.0
            # Virtual pack member: prefix/suffix are never legal query ranges.
            prefix = 12345
            calls = []
            def get_object(_key, byte_range):
                a, b = byte_range
                assert prefix <= a <= b < prefix + path.stat().st_size
                with path.open('rb') as handle:
                    handle.seek(a - prefix)
                    payload = handle.read(b - a + 1)
                calls.append((a, b, len(payload)))
                return SimpleNamespace(read=lambda: payload, headers={'ETag': payload_sha})
            reader = OssRangeReader(SimpleNamespace(get_object=get_object), 'fixture-pack',
                path.stat().st_size, payload_sha, base_offset=prefix,
                max_bytes=MAX_INPUT_BYTES * 4, max_requests=4096,
                check_cancelled=lambda: check_deadline(begin))
            actual = []
            profile = {}
            try:
                if selected:
                    source = pq.ParquetFile(reader)
                    table = source.read_row_groups(selected, use_threads=False)
                    native_start = time.perf_counter()
                    read_seconds = native_start - after_plan
                    conn = duckdb.connect()
                    try:
                        conn.execute("SET threads=1")
                        conn.execute("SET memory_limit='256MB'")
                        conn.execute("SET max_temp_directory_size='128MB'")
                        conn.execute("SET temp_directory=" + "'" + str(root / 'scratch').replace("'", "''") + "'")
                        profile_path = root / f'{name}-{mode}-profile.json'
                        conn.execute("PRAGMA enable_profiling='json'")
                        conn.execute("SET profiling_output='" + str(profile_path).replace("'", "''") + "'")
                        # Old schemas have the same empty audit defaults as serving.
                        for field in KEYWORD_FIELDS:
                            if field not in table.column_names:
                                table = table.append_column(field, pa.array([''] * table.num_rows))
                        conn.register('events', table)
                        where, params = EventStorage._filters(query, 3650)
                        # Mirror serving's native ORDER BY/LIMIT, rather than
                        # materializing every match into Python just to page it.
                        filtered = native_page(conn,
                            'SELECT * FROM events WHERE ' + where +
                            ' ORDER BY event_epoch_us DESC, source_file_name DESC,'
                            ' end_position DESC, row_index DESC, event_id DESC LIMIT ? OFFSET ?',
                            [*params, query.get('limit', 20) + 1, query.get('offset', 0)],
                            begin + 60,
                        )
                        # Compare original columns, not schema-default additions.
                        original_names = set(rows[0])
                        actual = [{k: v for k, v in r.items() if k in original_names} for r in filtered]
                    finally:
                        conn.close()
                    if not profile_path.is_file():
                        raise RuntimeError('native query profile was not written; no peak values fabricated')
                    profile = json.loads(profile_path.read_text('utf-8'))
                    source.close()
                    native_seconds = time.perf_counter() - native_start
                visible, more = actual[:query.get('limit', 20)], len(actual) > query.get('limit', 20)
                assert visible == expected and more == expected_more, f'{name}/{mode}: oracle mismatch'
                stats = reader.stats()
                assert stats['range_bytes'] == sum(c[2] for c in calls)
                assert stats['range_requests'] == len(calls)
                per_case[mode] = {**stats, 'row_groups': len(selected),
                                  'seconds': round(time.perf_counter() - begin, 6),
                                  'plan_seconds': round(after_plan - begin, 6),
                                  'range_decode_seconds': round(read_seconds, 6),
                                  'native_page_seconds': round(native_seconds, 6),
                                  'member_ranges': [[a - prefix, b - prefix] for a, b, _ in calls],
                                  'content_sha256': fingerprint(visible), 'oracle_match': True,
                                  'duckdb_peak_buffer_bytes': profile.get('system_peak_buffer_memory'),
                                  'duckdb_peak_scratch_bytes': profile.get('system_peak_temp_dir_size')}
            finally:
                reader.close()
        full = per_case['full_scan']['range_bytes']
        per_case['byte_ratio'] = per_case['indexed']['range_bytes'] / full if full else None
        print(json.dumps(per_case), flush=True)
        results.append(per_case)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == payload_sha, 'input mutated'
    if require_reduction:
        selective = next(r for r in results if r['case'] == 'selective')
        assert selective['byte_ratio'] <= 0.5, 'fixture requires at least 50% actual-byte reduction'
        assert next(r for r in results if r['case'] == 'zero')['indexed']['range_bytes'] == 0
    summary = {'phase': 'complete', 'scope': 'local fixture range-I/O mechanism, NOT production P95',
               'rows': len(rows), 'row_groups': groups, 'input_bytes': path.stat().st_size,
               'input_sha256': payload_sha, 'index_bytes': state_bytes,
               'index_seconds': round(index_seconds, 3), 'oracle_cases': len(results),
               'all_oracles_match': True, 'source_unchanged': True,
               'benchmark_process_peak_rss_bytes': process_peak_rss_bytes(), 'cases': results}
    print(json.dumps(summary), flush=True)
    return summary


def benchmark_raw_events(path, root):
    """Exercise the covering-row layout, not native parsing or production I/O."""
    from app.metadata import MetadataStore
    from app.raw_binlog import RawBinlogStore
    from app.raw_event_index import RawEventIndex
    from app.raw_binlog_query import matches
    from app.binlog_lite import RawBinlogError
    from app.exact_index import ExactIndex
    from app.rds_api import RemoteBinlog
    from app.config import Settings
    if not 0 < path.stat().st_size <= MAX_INPUT_BYTES:
        raise ValueError('sample must be at most 64 MiB')
    table = pq.read_table(path, use_threads=False)
    if table.num_rows > MAX_ROWS or table.nbytes > MAX_DECODED_BYTES:
        raise ValueError('sample exceeds row/decoded-byte budget')
    rows = table.to_pylist()
    metadata = MetadataStore(root/'metadata.sqlite3')
    store = RawBinlogStore(metadata)
    item = RemoteBinlog(log_file_name='mysql-bin.000001', file_size=path.stat().st_size,
        log_begin_utc='2026-09-01T00:00:00Z', log_end_utc='2026-09-30T00:00:00Z',
        checksum_crc64='', download_link='', intranet_download_link='', link_expired_utc='', remote_status='Completed', host_instance_id='fixture')
    file_id, _ = metadata.upsert_remote(Settings(db_instance_id='fixture'), item)
    for row in rows:
        row.update(instance_id='fixture', source_file_id=file_id, event_locator=f'raw:{file_id}:4',
            columns_json=json.dumps([{'index': 0, 'name': 'id', 'type_id': 8, 'primary_key': True}]))
    lo, hi = min(r['event_epoch_us'] for r in rows), max(r['event_epoch_us'] for r in rows)
    descriptor = {'file_id': file_id, 'raw': {'size_bytes': path.stat().st_size}}
    summary = {'lo': lo, 'hi': hi, 'tables': sorted({(r['database_name'], r['table_name']) for r in rows}), 'unknown': False}
    with metadata.connection() as conn:
        conn.execute('INSERT INTO raw_binlog_archives VALUES(?,?,?,?,?,?,?)',
            (file_id, lo, hi, json.dumps(summary), json.dumps(descriptor), time.time(), time.time()))
    index = RawEventIndex(metadata, root/'raw-index')
    start = time.perf_counter()
    index.build(descriptor, iter(rows), ExactIndex(root/'exact'))
    build_seconds = time.perf_counter()-start
    sizes = sum(p.stat().st_size for p in index.path.parent.iterdir())
    if sizes > MAX_STATE_BYTES:
        raise RuntimeError('sample index exceeded 512 MiB budget')
    selected = rows[len(rows)//2]
    scope = {'source': 'binlog', 'instance': 'fixture', 'database': selected['database_name'], 'table': selected['table_name'], 'limit': 10}
    results = []
    for name, query in [('time', scope), ('page', {**scope, 'offset': 10}),
        ('pk-positive', {**scope, 'exact': {'value': json.loads(selected['after_json'])['id']}}),
        ('pk-negative', {**scope, 'exact': {'value': -9999}}),
        ('keyword-positive', {**scope, 'keyword': str(json.loads(selected['after_json'])['id'])}),
        ('keyword-negative', {**scope, 'keyword': 'no-such-index-token-8372'}),
        ('keyword-page', {**scope, 'keyword': 'id', 'offset': 10}),
        ('keyword-filtered', {**scope, 'keyword': 'id', 'status': 'success', 'operations': ['UPDATE']})]:
        start = time.perf_counter()
        structural = [r for r in rows if r['database_name'] == query['database'] and r['table_name'] == query['table']
            and matches(None, r, {**query, 'exact': None, 'keyword':'', 'status':'', 'account':'', 'connection':''}, lo, hi, {})
            and (not query.get('exact') or any(json.loads(r[column]).get('id') == query['exact']['value'] for column in ('before_json', 'after_json')))]
        if query.get('status') and any(not r.get('execution_status') for r in structural):
            # Missing source fields are not a fast successful negative query.
            for _ in range(3):
                try:
                    index.query(query, lo, hi)
                except RawBinlogError as exc:
                    assert exc.code == 'INDEX_FILTER_UNAVAILABLE', (name, exc.code)
                else:
                    raise AssertionError('unknown status was reported as a successful query')
            results.append({'case':name, 'outcome':'rejected', 'error_code':'INDEX_FILTER_UNAVAILABLE',
                            'counts_as_successful_latency_acceptance':False})
            continue
        filtered = [r for r in structural if matches(None, r, {**query, 'exact': None}, lo, hi, {})]
        expected, more = page(filtered, query)
        if name in ('pk-positive', 'keyword-positive'):
            assert expected, name+' must contain real matching rows'
        oracle_seconds = time.perf_counter()-start
        timings = []
        for _ in range(3):
            start = time.perf_counter()
            result = index.query(query, lo, hi)
            timings.append(time.perf_counter()-start)
            assert timings[-1] < 60, 'indexed query missed one-minute budget'
            assert result['rows'] == expected and result['has_more'] == more, name
            assert result['range_requests'] == 0
        results.append({'case': name, 'oracle_seconds': oracle_seconds, 'indexed_seconds': timings,
            'rows': len(expected), 'rows_sha256': fingerprint(expected), 'all_fields_match': True})
    result = {'scope': 'local normalized-row fixture; warm OS cache; NOT native parsing, production, or 1TB acceptance',
        'rows': len(rows), 'fixture_parquet_bytes': path.stat().st_size, 'index_bytes': sizes,
        'index_bytes_per_event': sizes/len(rows), 'build_seconds': build_seconds,
        'process_peak_rss_bytes': process_peak_rss_bytes(), 'cases': results}
    print(json.dumps(result), flush=True)
    return result


def check_deadline(start):
    if time.perf_counter() - start > 60:
        raise TimeoutError('P1 query exceeded 60-second local fixture budget')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--rows', type=int, default=16384)
    parser.add_argument('--parquet', type=Path)
    parser.add_argument('--raw-event-index', action='store_true', help='measure asynchronous covering-row index with a normalized fixture')
    args = parser.parse_args()
    if not 1024 <= args.rows <= MAX_ROWS:
        parser.error('--rows must be between 1024 and 200000')
    with tempfile.TemporaryDirectory(prefix='p1-index-layout-') as directory:
        root = Path(directory)
        path = args.parquet.resolve() if args.parquet else root / 'fixture.parquet'
        if not args.parquet:
            fixture(path, args.rows)
        if args.raw_event_index:
            benchmark_raw_events(path, root)
        else:
            benchmark(path, root, require_reduction=not bool(args.parquet) and args.rows >= 8192)


if __name__ == '__main__':
    main()
