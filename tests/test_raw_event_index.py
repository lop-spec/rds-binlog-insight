from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from app.binlog_lite import RawBinlogError
from app.exact_index import ExactIndex
from app.raw_event_index import RawEventIndex, subtract, union
from app.raw_index_worker import ResourceBudget
from tests import test_raw_binlog as fixtures


class RawEventIndexTests(unittest.TestCase):
    setUp = fixtures.ManifestTests.setUp
    add_raw = fixtures.ManifestTests.add_raw

    def index(self):
        return RawEventIndex(self.metadata, Path(self.temp.name)/'index')

    def rows(self, file_id, n=20, pk=True):
        for i in range(n):
            yield dict(event_id=f'{file_id}:{i}', instance_id='test', database_name='db', table_name='one',
                event_epoch_us=110+i, source_file_name='mysql-bin.000001', end_position=i+100,
                row_index=0, operation='UPDATE', gtid='sid:1', transaction_id='sid:1',
                columns_json=json.dumps([{'index': 0, 'name': 'id', 'type_id': 8, 'primary_key': pk}]),
                before_json=json.dumps({'id': i}), after_json=json.dumps({'id': i+100}),
                event_locator=f'raw:{file_id}:4', sql_text='unchanged payload')

    def build(self, index, file_id, rows=None):
        exact = ExactIndex(Path(self.temp.name)/'exact')
        return index.build(self.store.get(file_id), self.rows(file_id) if rows is None else rows, exact)

    def query(self, index, **kwargs):
        return index.query({'instance': 'test', 'database': 'db', 'table': 'one', 'source': 'binlog', **kwargs}, 100, 200)

    def test_same_full_rows_order_and_pagination_no_oss(self):
        file_id = self.add_raw(1)
        index = self.index()
        self.build(index, file_id)
        expected = list(reversed(list(self.rows(file_id))))
        for offset in (0, 7, 14, 21):
            result = self.query(index, limit=7, offset=offset)
            self.assertEqual(result['rows'], expected[offset:offset+7])
            self.assertEqual(result['has_more'], len(expected)>offset+7)
            self.assertEqual(result['range_requests'], 0)
        self.assertEqual(index.coverage({'instance': 'test'})['intervals'], [[100, 200]])

    def test_primary_before_and_after_are_exact_not_substring(self):
        file_id = self.add_raw(1)
        index = self.index()
        self.build(index, file_id)
        for value in ('3', '103'):
            rows = self.query(index, exact={'value': value})['rows']
            self.assertEqual([r['event_id'] for r in rows], [file_id+':3'])
        self.assertEqual(self.query(index, exact={'value': '333'})['rows'], [])

    def test_unknown_schema_does_not_become_no_match(self):
        file_id = self.add_raw(1)
        index = self.index()
        self.build(index, file_id, self.rows(file_id, pk=False))
        with self.assertRaises(RawBinlogError) as caught:
            self.query(index, exact={'value': '3'})
        self.assertEqual(caught.exception.code, 'EXACT_SCHEMA_UNKNOWN')
        self.assertEqual(len(self.query(index)['rows']), 20)

    def test_incomplete_overlap_blocks_fast_query_without_fallback(self):
        one, two = self.add_raw(1), self.add_raw(2)
        index = self.index()
        self.build(index, one)
        self.assertEqual(index.coverage({'instance': 'test'})['intervals'], [])
        with self.assertRaises(RawBinlogError) as caught:
            self.query(index)
        self.assertEqual(caught.exception.code, 'INDEX_COVERAGE_INCOMPLETE')
        self.build(index, two)
        self.assertEqual(len(self.query(index)['rows']), 40)

    def test_failed_file_is_invisible_retry_is_idempotent(self):
        file_id = self.add_raw(1)
        index = self.index()
        def broken():
            yield from self.rows(file_id, 300)
            raise RuntimeError('CRC failure at end')
        with self.assertRaisesRegex(RuntimeError, 'CRC failure'):
            self.build(index, file_id, broken())
        self.assertEqual(index.coverage({'instance': 'test'})['intervals'], [])
        self.build(index, file_id)
        self.build(index, file_id)
        self.assertEqual(len(self.query(index)['rows']), 20)
        with index.connection() as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM keys').fetchone()[0], 40)

    def test_schema_mapping_change_invalidates_index(self):
        file_id = self.add_raw(1)
        index = self.index()
        self.build(index, file_id)
        changed = RawEventIndex(self.metadata, index.path.parent, schema_sha='new-registry')
        self.assertEqual(changed.coverage({'instance': 'test'})['intervals'], [])
        self.assertEqual(next(changed.pending())['file_id'], file_id)

    def test_detail_reuses_identical_payload_but_rejects_wrong_scope(self):
        file_id = self.add_raw(1)
        index = self.index()
        self.build(index, file_id)
        expected = list(self.rows(file_id))[0]
        self.assertEqual(index.detail(expected['event_id'], expected['event_locator'], 'test'), expected)
        self.assertIsNone(index.detail(expected['event_id'], expected['event_locator'], 'other'))
        self.assertIsNone(index.detail(expected['event_id'], 'raw:wrong:4', 'test'))

    def test_source_retirement_reclaims_only_derived_rows(self):
        one, two = self.add_raw(1), self.add_raw(2)
        index = self.index()
        self.build(index, one)
        self.build(index, two)
        with self.metadata.connection() as conn:
            conn.execute('DELETE FROM raw_binlog_archives WHERE file_id=?', (one,))
        self.assertEqual(index.reclaim(), 20)
        with index.connection() as conn:
            self.assertEqual(conn.execute('SELECT count(*) FROM events').fetchone()[0], 20)
            self.assertEqual(conn.execute('SELECT count(*) FROM keys').fetchone()[0], 40)
            self.assertEqual(conn.execute('PRAGMA foreign_key_check').fetchall(), [])
        self.assertIsNotNone(self.metadata.file_record(one))
        self.assertIsNotNone(self.store.get(two))

    def test_opaque_directory_uses_decoded_bounds_not_epoch_zero(self):
        file_id = self.add_raw(1)
        with self.metadata.connection() as conn:
            conn.execute('UPDATE raw_binlog_archives SET lo=0,hi=? WHERE file_id=?', (2**63-1, file_id))
        index = self.index()
        self.build(index, file_id)
        self.assertEqual(index.coverage({'instance': 'test'})['intervals'], [[110, 129]])

    def test_source_change_invalidates_index(self):
        file_id = self.add_raw(1)
        index = self.index()
        self.build(index, file_id)
        with self.metadata.connection() as conn:
            conn.execute("UPDATE raw_binlog_archives SET descriptor=json_set(descriptor,'$.raw.size_bytes',101) WHERE file_id=?", (file_id,))
        self.assertEqual(index.coverage({'instance': 'test'})['intervals'], [])

    def test_absent_table_is_proven_empty_not_unindexed(self):
        file_id = self.add_raw(1)
        index = self.index()
        self.build(index, file_id)
        result = self.query(index, table='absent')
        self.assertEqual(result['rows'], [])
        self.assertTrue(result['exact_index_complete'])

    def test_range_holes_never_bridged(self):
        self.assertEqual(subtract([[100, 200], [300, 500]], union([[150, 160], [350, 450]])),
                         [[100, 149], [161, 200], [300, 349], [451, 500]])
        self.assertEqual(union([[10, 20], [21, 30], [33, 40]]), [[10, 30], [33, 40]])

    def test_no_index_read_creates_no_database(self):
        index = self.index()
        self.assertEqual(index.coverage({'instance': 'test'})['intervals'], [])
        self.assertFalse(index.path.exists())

    def test_query_cancellation_propagates(self):
        file_id = self.add_raw(1)
        index = self.index()
        self.build(index, file_id)
        from app.query_tasks import QueryCancelled
        control = Mock()
        control.check_cancelled.side_effect = QueryCancelled('cancel')
        with self.assertRaises(QueryCancelled):
            index.query({'instance': 'test', 'database': 'db', 'table': 'one', 'source': 'binlog'}, 100, 200, control=control)

    def test_operation_transaction_and_page_boundaries(self):
        file_id = self.add_raw(1)
        index = self.index()
        self.build(index, file_id)
        self.assertEqual(self.query(index, operations=['DELETE'])['rows'], [])
        self.assertEqual(self.query(index, transaction='other') ['rows'], [])
        self.assertEqual(len(self.query(index, operations=['UPDATE'], transaction='sid:1')['rows']), 20)
        with self.assertRaises(RawBinlogError) as caught:
            self.query(index, offset=2000)
        self.assertEqual(caught.exception.code, 'RAW_QUERY_PAGE_LIMIT')

    def test_no_keyword_silent_scan(self):
        with self.assertRaises(RawBinlogError) as caught:
            self.query(self.index(), keyword='123')
        self.assertEqual(caught.exception.code, 'INDEX_QUERY_UNSUPPORTED')

    def test_http_fast_query_coverage_assets_and_detail_need_no_oss(self):
        import threading
        import time
        import urllib.request
        from http.server import ThreadingHTTPServer
        from types import SimpleNamespace
        from app.server import RequestHandler
        from app.storage import EventStorage
        from app.config import Settings
        from app.query_tasks import QueryTaskManager
        storage = EventStorage(self.metadata, Path(self.temp.name))
        self.addCleanup(storage.slowlog_index.close)
        file_id = self.add_raw(1)
        self.build(storage.raw_event_index, file_id)
        forbidden = Mock(side_effect=AssertionError('indexed read must not initialize OSS'))
        manager = QueryTaskManager(self.metadata, storage, settings_loader=Settings, archive_loader=forbidden)
        self.addCleanup(manager.shutdown)
        server = ThreadingHTTPServer(('127.0.0.1', 0), RequestHandler)
        server.allowed_hosts = {'127.0.0.1'}
        server.application = SimpleNamespace(storage=storage, metadata=self.metadata, queries=manager,
                                             sync=SimpleNamespace(archive_for_settings=forbidden))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        def stop():
            server.shutdown()
            server.server_close()
            thread.join(5)
        self.addCleanup(stop)
        base = f'http://127.0.0.1:{server.server_port}'
        def request(path, payload=None):
            data = None if payload is None else json.dumps(payload).encode()
            with urllib.request.urlopen(urllib.request.Request(base+path, data=data, headers={'Content-Type': 'application/json'}), timeout=5) as response:
                return response.read()
        with patch.object(storage, '_query_window', return_value=(100, 200)):
            self.assertIn(b'function indexedQuickRange', request('/assets/indexed-range.js'))
            coverage = json.loads(request('/api/index-coverage?instance=test&database=db&table=one'))['data']
            self.assertEqual(coverage['intervals'], [[100, 200]])
            self.assertNotIn('_valid_files', coverage)
            task = json.loads(request('/api/query-tasks', {'indexedOnly': True, 'source': 'binlog', 'instance': 'test',
                'database': 'db', 'table': 'one', 'startEpochUs': 100, 'endEpochUs': 200, 'limit': 10}))['data']
            deadline = time.monotonic()+10
            while True:
                state = json.loads(request('/api/query-task?id='+task['taskId']))['data']
                if state['status'] not in {'queued', 'running'}:
                    break
                if time.monotonic() > deadline:
                    self.fail('local query task deadline')
                time.sleep(.02)
            self.assertEqual(state['status'], 'succeeded', state)
            self.assertEqual(len(state['result']['rows']), 10)
            first = state['result']['rows'][0]
            detail = json.loads(request('/api/event?'+urllib.parse.urlencode({
                'id': first['event_id'], 'locator': first['event_locator'], 'instance': 'test'})))['data']
            self.assertEqual(detail, first)
            forbidden.assert_not_called()

    def test_supported_order_uses_index_not_temp_sort(self):
        file_id = self.add_raw(1)
        index = self.index()
        self.build(index, file_id)
        with index.connection() as conn:
            plan = '\n'.join(str(tuple(r)) for r in conn.execute("EXPLAIN QUERY PLAN SELECT event_id FROM events WHERE instance='test' AND db='db' AND tbl='one' AND stamp BETWEEN 100 AND 200 ORDER BY stamp DESC,filename DESC,position DESC,ordinal DESC,event_id DESC LIMIT 10"))
        self.assertIn('event_time', plan)
        self.assertNotIn('TEMP B-TREE', plan)


class ResourceBudgetTests(unittest.TestCase):
    def sample(self, **changes):
        return {'idle': .8, 'io': 0, 'memory': 0, 'query': False, 'free': 100*1024**3, **changes}

    def test_dynamic_allocation_and_hysteresis(self):
        self.assertEqual(ResourceBudget.allocation(self.sample())[0], 1)
        self.assertLess(ResourceBudget.allocation(self.sample(idle=.3))[0], 1)
        for sample in (self.sample(idle=.05), self.sample(io=2), self.sample(memory=1), self.sample(query=True), self.sample(free=1)):
            self.assertEqual(ResourceBudget.allocation(sample)[0], 0)
        self.assertEqual(ResourceBudget.allocation(self.sample(io=.7), paused=True)[0], 0)
        self.assertGreater(ResourceBudget.allocation(self.sample(io=.4), paused=True)[0], 0)

    def test_pressure_yields_then_recovers_without_restarting_work(self):
        clock = [10.0]
        def sleep(n): clock[0] += n
        samples = iter([self.sample(io=2), self.sample(io=.7), self.sample(io=.1)])
        publish = Mock()
        gate = ResourceBudget(Mock(), Path('.'), publish, sample=lambda: next(samples), clock=lambda: clock[0], sleep=sleep)
        gate.check()
        self.assertFalse(gate.paused)
        self.assertEqual(clock[0], 14)
        self.assertEqual(publish.call_count, 3)


class NativeRawIndexTests(unittest.TestCase):
    setUp = fixtures.ManifestTests.setUp
    add_raw = fixtures.ManifestTests.add_raw

    @unittest.skipUnless(__import__('sys').platform == 'linux', 'Linux native parser required; covered by cloud CI')
    def test_native_full_file_index_matches_pruned_raw_decoder(self):
        import gzip
        import hashlib
        import io
        import struct
        import uuid
        import zlib
        from types import SimpleNamespace
        from oss2.utils import Crc64
        from app.binlog_lite import scan
        from app.raw_binlog_query import Budget, decode
        from app.raw_index_worker import decoded_rows
        from app.storage import EventStorage
        data = bytearray(b'\xfebin')
        epoch = 1790000000
        def event(kind, body, stamp):
            size = 19+len(body)+4
            header = struct.pack('<IBIIIH', stamp, kind, 1, size, len(data)+size, 0)
            payload = header+body
            data.extend(payload+struct.pack('<I', zlib.crc32(payload)))
        post = bytearray(40)
        for kind, length in ((2, 13), (4, 8), (19, 8), (30, 10), (31, 10), (32, 10)):
            post[kind-1] = length
        fde = struct.pack('<H', 4)+b'8.0.41-log'.ljust(50, b'\x00')+struct.pack('<I', epoch)+bytes([19])+post+bytes([1])
        event(15, fde, epoch)
        for number, table in enumerate(('one', 'other', 'one'), 1):
            stamp = epoch+number
            event(33, bytes([1])+uuid.UUID(int=123).bytes+struct.pack('<Q', number), stamp)
            query = struct.pack('<IIBHH', 1, 0, 2, 0, 0)+b'db\x00BEGIN'
            event(2, query, stamp)
            table_map = (1).to_bytes(6, 'little')+b'\x00\x00'+bytes([2])+b'db\x00'+bytes([len(table)])+table.encode()+b'\x00'
            table_map += bytes([1, 8, 0, 0, 4, 3, 2])+b'id'+bytes([8, 1, 0])
            event(19, table_map, stamp)
            prefix = (1).to_bytes(6, 'little')+struct.pack('<HH', 1, 2)+bytes([1])
            if number == 1:
                event(31, prefix+bytes([1, 1, 0])+struct.pack('<q', 3)+bytes([0])+struct.pack('<q', 103), stamp)
            else:
                event(30, prefix+bytes([1, 0])+struct.pack('<q', number+4), stamp)
            event(16, struct.pack('<Q', number), stamp)
        file_id = self.add_raw(1)
        source = Path(self.temp.name)/'source.binlog'
        source.write_bytes(data)
        directory = scan(source)
        packed = gzip.compress(json.dumps(directory).encode())
        crc = Crc64()
        crc(data)
        entry = dict(file_id=file_id, instance_id='test', host_instance_id='node', source_file_name='mysql-bin.000001',
            raw={'size_bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(), 'crc64': str(crc.crc), 'oss_key': 'raw'},
            index={'size_bytes': len(packed), 'sha256': hashlib.sha256(packed).hexdigest(), 'oss_key': 'directory'})
        class Bucket:
            def get_object(self, key, byte_range=None):
                body = data if key == 'raw' else packed
                if byte_range is None:
                    return io.BytesIO(body)
                lo, hi = byte_range
                response = io.BytesIO(body[lo:hi+1])
                response.headers = {'Content-Range': f'bytes {lo}-{hi}/{len(body)}'}
                return response
        with self.metadata.connection() as conn:
            conn.execute('UPDATE raw_binlog_archives SET lo=?,hi=?,summary=?,descriptor=? WHERE file_id=?',
                (directory['lo'], directory['hi'], json.dumps(directory), json.dumps(entry), file_id))
        storage = EventStorage(self.metadata, Path(self.temp.name))
        self.addCleanup(storage.slowlog_index.close)
        archive = SimpleNamespace(bucket=Bucket())
        budget = Budget()
        self.addCleanup(budget.close)
        with patch.object(storage.raw_binlogs, 'verify'), patch('app.raw_index_worker.shutil.disk_usage', return_value=SimpleNamespace(free=100*1024**3)):
            storage.raw_event_index.build(entry, decoded_rows(storage, archive, entry, budget), storage.exact_index)
            query = {'instance': 'test', 'database': 'db', 'table': 'one', 'source': 'binlog', 'limit': 10}
            expected = [r for r in decode(storage, archive, entry, query, directory['lo'], directory['hi'], budget)
                        if r.get('database_name') == 'db' and r.get('table_name') == 'one']
        expected.sort(key=storage._row_sort_key, reverse=True)
        result = storage.raw_event_index.query(query, directory['lo'], directory['hi'])
        self.assertEqual(len(expected), 2)
        self.assertEqual(result['rows'], expected)
        for value in ('3', '103'):
            exact = storage.raw_event_index.query({**query, 'exact': {'value': value}}, directory['lo'], directory['hi'])
            self.assertEqual(len(exact['rows']), 1)
            self.assertEqual(exact['rows'][0]['operation'], 'UPDATE')


class WorkerDecodeTests(unittest.TestCase):
    def test_sequential_read_checks_checksums_and_preserves_event_identity(self):
        import gzip
        import hashlib
        import io
        from types import SimpleNamespace
        from oss2.utils import Crc64
        from app.raw_index_worker import decoded_rows
        from app.raw_binlog_query import normalize_row
        raw = b'fixture bytes, not a native parser test'
        crc = Crc64()
        crc(raw)
        directory = gzip.compress(json.dumps({'regions': [{'gtid': 'sid:1', 'start': 101}]}).encode())
        entry = {'file_id': 'file', 'instance_id': 'test', 'host_instance_id': 'node',
            'source_file_name': 'mysql-bin.000001',
            'raw': {'size_bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest(), 'crc64': str(crc.crc), 'oss_key': 'raw'},
            'index': {'size_bytes': len(directory), 'sha256': hashlib.sha256(directory).hexdigest(), 'oss_key': 'directory'}}
        row = {'gtid': 'sid:1', 'operation': 'UPDATE', 'event_epoch_us': 123456789}
        with tempfile.TemporaryDirectory() as root:
            storage = SimpleNamespace(raw_binlogs=Mock(), paths={'scratch': Path(root)})
            archive = SimpleNamespace(bucket=SimpleNamespace(get_object=lambda key: io.BytesIO(raw if key == 'raw' else directory)))
            gate = SimpleNamespace(check=lambda: None, add=lambda n: None, requests=0, cancel=Mock())
            def parser(path, file_id, chunks, *args, **kwargs):
                self.assertEqual(path.read_bytes(), raw)
                chunks.mkdir()
                chunk = chunks/'0.ndjson'
                chunk.write_text(json.dumps(row)+'\n')
                yield chunk
            with patch('app.raw_index_worker.parse_ndjson_chunks', side_effect=parser), patch('app.raw_index_worker.shutil.disk_usage', return_value=SimpleNamespace(free=100*1024**3)):
                actual = list(decoded_rows(storage, archive, entry, gate))
                self.assertEqual(actual, [normalize_row(dict(row), entry, {}, {'sid:1': 101})])
                entry['raw']['sha256'] = 'invalid'
                with self.assertRaises(RawBinlogError) as caught:
                    list(decoded_rows(storage, archive, entry, gate))
                self.assertEqual(caught.exception.code, 'RAW_INDEX_VERIFY_FAILED')
            self.assertEqual(list(Path(root).iterdir()), [])

    def test_payload_parser_strict_bool_and_legacy_default(self):
        from app.server import _event_query_payload
        self.assertTrue(_event_query_payload({'indexedOnly': True})['indexed_only'])
        self.assertFalse(_event_query_payload({})['indexed_only'])
        with self.assertRaises(ValueError):
            _event_query_payload({'indexedOnly': 'false'})


if __name__ == '__main__':
    unittest.main()
