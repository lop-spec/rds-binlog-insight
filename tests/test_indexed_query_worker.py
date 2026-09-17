from __future__ import annotations

import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import unittest
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from app import indexed_query_native as native
from app import indexed_query_client as client
from app.indexed_query_worker import QueryServer
from app.oss_store import OssArchive
from app.storage import EventStorage, StorageError
from tests import test_all_source_routing as routing


class NativeQueryTests(unittest.TestCase):
    setUp = routing.AllSourceRoutingTests.setUp

    def payload(self, **query):
        query = {**self.query, 'limit': 100, **query}
        parts = self.metadata.parts_in_range(start_epoch_us=self.epoch,
                                             end_epoch_us=self.epoch + 9)
        parts.sort(key=lambda p: (p['max_event_epoch_us'], p['min_event_epoch_us'], p['path']), reverse=True)
        return {'settings': asdict(self.settings), 'query': query,
                'work': [{'part': p, 'row_groups': None,
                          'max_event_epoch_us': p['max_event_epoch_us']} for p in parts]}

    def run_native(self, payload, **kwargs):
        return native.run_query(payload, data_dir=self.directory.name,
                                scratch=self.storage.paths['scratch'], **kwargs)

    def test_cross_source_pages_match_independent_identity_content_oracle(self):
        for offset in (0, 3, 6, 9, 12):
            with self.subTest(offset=offset):
                result = self.run_native(self.payload(limit=3, offset=offset))
                expected = list(reversed(range(10)))[offset:offset + 3]
                self.assertEqual([r['event_id'] for r in result['rows']], [f'event-{i}' for i in expected])
                self.assertEqual([json.loads(r['after_json']) for r in result['rows']],
                                 [{'order_id': i} for i in expected])
                self.assertEqual(result['has_more'], offset + 3 < 10)
                self.assertEqual(result['full_object_fallback_bytes'], 0)

    def test_duplicate_parts_and_rows_do_not_consume_page_positions(self):
        payload = self.payload(limit=2, offset=1)
        entry = payload['work'][0]
        original = entry['part']
        table = pq.ParquetFile(original['path']).read()
        path = Path(original['path']).with_name('duplicated.parquet')
        pq.write_table(pa.concat_tables([table, table]), path)
        part = {**original, 'path': str(path), 'size_bytes': path.stat().st_size,
                'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
        payload['work'] = [{**entry, 'part': part}, {**entry, 'part': part}]
        result = self.run_native(payload)
        self.assertEqual([r['event_id'] for r in result['rows']], ['event-6', 'event-3'])
        self.assertTrue(result['has_more'])

    def test_literal_wildcards_and_unknown_index_do_not_false_match(self):
        for keyword, count in [('order_id', 10), ('%', 0), ('order%id', 0), ('nonexistentneedle order_id', 0)]:
            with self.subTest(keyword=keyword):
                self.assertEqual(len(self.run_native(self.payload(keyword=keyword))['rows']), count)
        result = self.run_native(self.payload(keyword='nonexistentneedle order_id', keyword_mode='OR'))
        self.assertEqual(len(result['rows']), 10)

    def test_decode_guard_precedes_even_structural_probe(self):
        with patch.object(native, 'MAX_GROUP_BYTES', 1), \
                patch.object(EventStorage, '_structural_candidate_row_groups',
                             side_effect=AssertionError('decoded before budget check')):
            with self.assertRaises(client.IndexedQueryError) as caught:
                self.run_native(self.payload(table='orders'))
        self.assertEqual(caught.exception.code, 'QUERY_DECODE_BUDGET_EXCEEDED')

    def test_local_identity_changed_is_rejected(self):
        payload = self.payload()
        payload['work'][0]['part']['sha256'] = '0' * 64
        with self.assertRaises(client.IndexedQueryError) as caught:
            self.run_native(payload)
        self.assertEqual(caught.exception.code, 'OSS_RANGE_VERIFY_FAILED')

    def test_budget_is_not_reset_for_next_local_part(self):
        payload = self.payload()
        first = self.run_native({**payload, 'work': payload['work'][:1]})
        budget = first['budget_bytes'] + payload['work'][1]['part']['size_bytes'] - 1
        with patch.object(native, 'MAX_READ_BYTES', budget):
            with self.assertRaises(client.IndexedQueryError) as caught:
                self.run_native(payload)
        self.assertEqual(caught.exception.code, 'OSS_QUERY_BUDGET_EXCEEDED')

    def remote_payload(self):
        payload = self.payload()
        packed = bytearray(b'pack-header')
        for entry in payload['work']:
            part = entry['part']
            body = Path(part['path']).read_bytes()
            part.update(oss_key='fixture-pack', oss_offset=len(packed),
                        oss_length=len(body), oss_etag='fixed-version')
            packed.extend(body)
        calls = []

        def get_object(key, byte_range):
            calls.append(byte_range)
            result = io.BytesIO(packed[byte_range[0]:byte_range[1] + 1])
            result.headers = {'ETag': 'fixed-version'}
            return result

        settings = type(self.settings)(oss_enabled=True, oss_bucket='fixture-bucket',
                                       oss_endpoint='oss-cn-hangzhou-internal.aliyuncs.com',
                                       oss_region_id='cn-hangzhou', oss_prefix='fixture/')
        payload['settings'] = asdict(settings)
        archive = OssArchive(settings, bucket=SimpleNamespace(get_object=get_object))
        return payload, archive, calls

    def test_real_pack_offsets_projection_and_cross_object_get_budget(self):
        payload, archive, calls = self.remote_payload()
        result = self.run_native(payload, archive=archive)
        self.assertEqual([r['event_id'] for r in result['rows']], [f'event-{i}' for i in reversed(range(10))])
        self.assertEqual(result['range_requests'], len(calls))
        self.assertGreaterEqual(result['budget_bytes'], result['range_bytes'])
        self.assertEqual(result['local_parts_read'], 0)
        first = self.run_native({**payload, 'work': payload['work'][:1]}, archive=archive)
        calls.clear()
        with patch.object(native, 'MAX_REQUESTS', first['range_requests']):
            with self.assertRaises(Exception) as caught:
                self.run_native(payload, archive=archive)
        self.assertEqual(caught.exception.code, 'OSS_QUERY_BUDGET_EXCEEDED')
        self.assertEqual(len(calls), first['range_requests'])

    def test_disabled_oss_does_not_read_residual_archive_references(self):
        payload, _, _ = self.remote_payload()
        payload['settings']['oss_enabled'] = False
        with patch('app.oss_store.OssArchive', side_effect=AssertionError('disabled OSS was contacted')):
            result = self.run_native(payload)
        self.assertEqual(len(result['rows']), 10)
        self.assertEqual(result['oss_range_parts_read'], 0)
        self.assertGreater(result['local_parts_read'], 0)

    def test_etag_change_never_uses_available_local_copy(self):
        payload, archive, calls = self.remote_payload()
        payload['work'][0]['part']['oss_etag'] = 'another-version'
        with self.assertRaises(Exception) as caught:
            self.run_native(payload, archive=archive)
        self.assertEqual(caught.exception.code, 'OSS_RANGE_VERIFY_FAILED')
        self.assertEqual(len(calls), 1)

    def test_real_entry_uses_index_before_worker_and_never_calls_ck(self):
        backend = SimpleNamespace(query_events=Mock(side_effect=AssertionError('CK used')), raw_serving=True)
        self.storage.clickhouse_backend = backend
        with patch.dict(os.environ, {'RDS_BINLOG_INDEXED_QUERY_WORKER_URL': 'http://worker'}), \
                patch.object(client, 'execute', side_effect=lambda url, p, c: self.run_native(p)) as execute, \
                patch.object(self.storage.search_index, 'candidate_blocks',
                             wraps=self.storage.search_index.candidate_blocks) as index, \
                patch.object(self.storage, '_read_part_table', side_effect=AssertionError('main-process scan')):
            result = self.storage._query_events_tiered_impl(self.query, self.settings, None)
        self.assertEqual([r['event_id'] for r in result['rows']], ['event-9', 'event-8', 'event-7'])
        self.assertEqual(result['backend'], 'indexed-parquet-worker')
        index.assert_called_once()
        execute.assert_called_once()
        backend.query_events.assert_not_called()

    def test_worker_error_does_not_fall_back(self):
        with patch.dict(os.environ, {'RDS_BINLOG_INDEXED_QUERY_WORKER_URL': 'http://worker'}), \
                patch.object(client, 'execute', side_effect=client.IndexedQueryError('budget')), \
                patch.object(self.storage, '_read_part_table', side_effect=AssertionError('fallback')):
            with self.assertRaises(client.IndexedQueryError):
                self.storage._query_events_tiered_impl(self.query, self.settings, None)

    def test_source_revision_change_prevents_publishing_results(self):
        def execute(url, payload, control):
            result = self.run_native(payload)
            with self.metadata.connection() as conn:
                conn.execute('UPDATE parquet_parts SET content_revision=content_revision+1000')
            return result
        with patch.dict(os.environ, {'RDS_BINLOG_INDEXED_QUERY_WORKER_URL': 'http://worker'}), \
                patch.object(client, 'execute', side_effect=execute):
            with self.assertRaises(StorageError) as caught:
                self.storage._query_events_tiered_impl(self.query, self.settings, None)
        self.assertEqual(caught.exception.code, 'QUERY_SOURCE_CHANGED')

    def test_tombstone_visibility_change_prevents_publishing_results(self):
        def execute(url, payload, control):
            result = self.run_native(payload)
            with self.metadata.connection() as conn:
                conn.execute('UPDATE binlog_files SET query_visible=0')
            return result
        with patch.dict(os.environ, {'RDS_BINLOG_INDEXED_QUERY_WORKER_URL': 'http://worker'}), \
                patch.object(client, 'execute', side_effect=execute):
            with self.assertRaises(StorageError) as caught:
                self.storage._query_events_tiered_impl(self.query, self.settings, None)
        self.assertEqual(caught.exception.code, 'QUERY_SOURCE_CHANGED')

    def test_result_budget_is_an_error_not_field_truncation(self):
        with patch.object(native, 'MAX_RESULT_BYTES', 1):
            with self.assertRaises(client.IndexedQueryError) as caught:
                self.run_native(self.payload())
        self.assertEqual(caught.exception.code, 'QUERY_RESULT_BUDGET_EXCEEDED')

    def test_like_era_certificate_and_probe_keys_are_not_reused(self):
        from app import storage as module
        dumps = json.dumps
        part = self.payload()['work'][0]['part']
        query = {**self.query, 'keyword': '%'}
        def fingerprints():
            return (self.storage._query_probe_fingerprint(query, part, self.epoch, self.epoch + 9),
                    self.storage._query_certificate_fingerprint(query, self.epoch, self.epoch + 9,
                                                                self.settings.db_instance_id))
        current = fingerprints()
        with patch.object(module.json, 'dumps', side_effect=lambda p, **k: dumps({**p, 'schema': 4}, **k)):
            old = fingerprints()
        self.assertTrue(all(a != b for a, b in zip(current, old)))

    def test_missing_source_is_not_an_empty_success(self):
        Path(self.payload()['work'][0]['part']['path']).unlink()
        with patch.dict(os.environ, {'RDS_BINLOG_INDEXED_QUERY_WORKER_URL': 'http://worker'}), \
                patch.object(client, 'execute') as execute:
            with self.assertRaises(StorageError) as caught:
                self.storage._query_events_tiered_impl(self.query, self.settings, None)
        self.assertEqual(caught.exception.code, 'INDEXED_QUERY_SOURCE_INCOMPLETE')
        execute.assert_not_called()


class WorkerProtocolTests(unittest.TestCase):
    payload = NativeQueryTests.payload

    def setUp(self):
        NativeQueryTests.setUp(self)
        self.server = QueryServer(('127.0.0.1', 0), scratch=str(self.storage.paths['scratch']))
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.url = f'http://127.0.0.1:{self.server.server_port}'

    def test_http_native_result_and_cleanup(self):
        with patch.dict(os.environ, {'RDS_BINLOG_DATA_DIR': self.directory.name}):
            result = client.execute(self.url, self.payload(), client.DeadlineControl(None))
        self.assertEqual(len(result['rows']), 10)
        self.assertEqual(list(self.storage.paths['scratch'].glob('query-*')), [])

    def test_cancel_fences_a_late_post(self):
        payload = {**self.payload(), 'id': 'a' * 32}
        session = requests.Session()
        session.trust_env = False
        self.addCleanup(session.close)
        self.assertTrue(session.post(self.url + '/cancel', json={'id': payload['id']}, timeout=5).json()['terminal'])
        self.assertEqual(session.post(self.url + '/query', json=payload, timeout=5).status_code, 409)
        self.assertIsNone(self.server.active)

    def test_http_error_is_returned_only_after_native_exit_and_cleanup(self):
        payload = self.payload()
        payload['work'][0]['part']['sha256'] = '0' * 64
        with patch.dict(os.environ, {'RDS_BINLOG_DATA_DIR': self.directory.name}):
            with self.assertRaises(client.IndexedQueryError) as caught:
                client.execute(self.url, payload, client.DeadlineControl(None))
        self.assertEqual(caught.exception.code, 'OSS_RANGE_VERIFY_FAILED')
        self.assertEqual(list(self.storage.paths['scratch'].glob('query-*')), [])

    def test_worker_refuses_concurrent_native_job(self):
        self.server.active = {'id': 'b' * 32}
        session = requests.Session()
        session.trust_env = False
        self.addCleanup(session.close)
        try:
            with patch('app.indexed_query_worker.subprocess.Popen') as launch:
                response = session.post(self.url + '/query', json={**self.payload(), 'id': 'c' * 32}, timeout=5)
            self.assertEqual(response.status_code, 429)
            launch.assert_not_called()
        finally:
            self.server.active = None

    def test_deadline_terminates_owned_process(self):
        processes = []
        popen = subprocess.Popen
        def launch(args, **kwargs):
            process = popen([sys.executable, '-c', 'import time; time.sleep(30)'], **kwargs)
            processes.append(process)
            return process
        with patch('app.indexed_query_worker.subprocess.Popen', side_effect=launch):
            with self.assertRaises(client.IndexedQueryError) as caught:
                client.execute(self.url, self.payload(), client.DeadlineControl(None, seconds=2.2))
        self.assertEqual(caught.exception.code, 'QUERY_DEADLINE_EXCEEDED')
        self.assertEqual(len(processes), 1)
        self.assertIsNotNone(processes[0].poll())
        self.assertEqual(list(self.storage.paths['scratch'].glob('query-*')), [])

    def test_cancel_kills_owned_native_sql_and_confirms_terminal(self):
        processes = []
        popen = subprocess.Popen
        sql = ("import duckdb,json; c=duckdb.connect(); c.execute(\"SET threads=1; SET memory_limit='64MB'\"); "
               "print(json.dumps({'progress':{'file':'native-running','bytes':0}}),flush=True); "
               "c.execute('SELECT sum(i) FROM range(1000000000000) t(i)')")

        def launch(args, **kwargs):
            process = popen([sys.executable, '-c', sql], **kwargs)
            processes.append(process)
            return process

        class Cancelled(Exception):
            pass

        class Control:
            cancelled = False
            def check_cancelled(self):
                if self.cancelled:
                    raise Cancelled('cancel native SQL')
            def advance(self, **kwargs):
                self.cancelled = True

        with patch('app.indexed_query_worker.subprocess.Popen', side_effect=launch):
            with self.assertRaises(Cancelled):
                client.execute(self.url, self.payload(), client.DeadlineControl(Control()))
        self.assertEqual(len(processes), 1)
        self.assertIsNotNone(processes[0].poll())
        self.assertEqual(list(self.storage.paths['scratch'].glob('query-*')), [])


class ClientProtocolTests(unittest.TestCase):
    def test_http_rejection_still_fences_owned_id(self):
        response = Mock(status_code=503)
        cancelled = Mock()
        cancelled.json.return_value = {'terminal': True}
        session = Mock()
        session.post.side_effect = [response, cancelled]
        with patch.object(client.requests, 'Session', return_value=session):
            with self.assertRaises(client.IndexedQueryError) as caught:
                client.execute('http://worker', {}, client.DeadlineControl(None))
        self.assertEqual(caught.exception.code, 'QUERY_WORKER_UNAVAILABLE')
        submitted = json.loads(session.post.call_args_list[0].kwargs['data'])
        self.assertEqual(session.post.call_args_list[1].kwargs['json']['id'], submitted['id'])
        response.close.assert_called_once()
        session.close.assert_called_once()

    def test_lost_stream_requires_confirmed_native_terminal(self):
        for confirmed, code in [(True, 'QUERY_WORKER_LOST'), (False, 'QUERY_CANCEL_UNCONFIRMED')]:
            with self.subTest(confirmed=confirmed):
                response = Mock(status_code=200)
                response.iter_lines.return_value = iter([b'{"heartbeat": true}'])
                cancelled = Mock()
                cancelled.json.return_value = {'terminal': confirmed}
                session = Mock()
                session.post.side_effect = [response, cancelled]
                with patch.object(client.requests, 'Session', return_value=session):
                    with self.assertRaises(client.IndexedQueryError) as caught:
                        client.execute('http://worker', {}, client.DeadlineControl(None))
                self.assertEqual(caught.exception.code, code)
                response.close.assert_called_once()
                session.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
