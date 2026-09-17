"""P1 contracts: conservative candidates, exact semantics and actual range I/O."""
from __future__ import annotations

import hashlib
import io
from contextlib import nullcontext
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from app.oss_store import OssRangeReader, OssArchiveError
from app.search_index import SearchIndex
from app.storage import EventStorage
from tools.benchmark_index_layout import native_page


class IndexedParquetContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.index = SearchIndex(self.root / 'search.sqlite3')
        self.epoch = int(time.time() * 1_000_000)

    def part(self, extra):
        values = {
            'event_epoch_us': [self.epoch], 'database_name': ['app'],
            'table_name': ['events'], 'operation': ['UPDATE'],
            'sql_text': [''], 'before_json': ['{}'], 'after_json': ['{}'],
            'transaction_id': [''], 'source_file_name': ['binlog'],
            'connection_name': [''], 'database_account': [''], 'error_message': [''],
            **{k: [v] for k, v in extra.items()},
        }
        path = self.root / 'part.parquet'
        pq.write_table(pa.table(values), path)
        part = {'path': str(path), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest(),
                'row_count': 1, 'min_event_epoch_us': self.epoch,
                'max_event_epoch_us': self.epoch}
        self.index.index_parquet(part, path)
        return part

    def plan(self, part, keyword):
        return self.index.candidate_blocks([part], {'keyword': keyword},
            start_epoch_us=self.epoch, end_epoch_us=self.epoch)

    def test_every_public_keyword_column_is_indexed(self):
        for column in ('sql_text', 'before_json', 'after_json', 'transaction_id',
                       'source_file_name', 'connection_name', 'database_account', 'error_message'):
            with self.subTest(column=column):
                part = self.part({column: 'Prefix-UniqueNeedle-Suffix'})
                self.assertEqual(len(self.plan(part, 'uniqueneedle')['entries']), 1)
                self.assertEqual(self.plan(part, 'trulyabsent')['entries'], [])

    def test_v2_keyword_index_is_only_structural_not_false_negative(self):
        part = self.part({'connection_name': 'uniqueconnection'})
        # Reproduce old postings that omitted audit keyword columns.
        with self.index.connection() as conn:
            conn.execute('DELETE FROM keyword_fts')
            conn.execute('DELETE FROM token_fts')
            conn.execute('UPDATE indexed_parts SET schema_version = 2')
            conn.execute('UPDATE structural_parts SET schema_version = 2')
        plan = self.plan(part, 'uniqueconnection')
        self.assertEqual(len(plan['entries']), 1)
        self.assertEqual(plan['full_covered_paths'], set())
        self.assertEqual(plan['structural_covered_paths'], {part['path']})
        self.assertFalse(plan['entries'][0]['complete'])
        self.assertTrue(self.index.is_structural_current(part))

    def test_keyword_uses_literal_contains_like_clickhouse_not_sql_wildcards(self):
        columns = ('sql_text', 'before_json', 'after_json', 'transaction_id',
                   'source_file_name', 'connection_name', 'database_account', 'error_message')
        conn = duckdb.connect()
        self.addCleanup(conn.close)
        table = pa.table({'event_epoch_us': [self.epoch] * 3,
                          **{k: ['abc_def', 'abcXdef', 'rate%value'] if k == 'sql_text'
                             else ['', '', ''] for k in columns}})
        conn.register('events', table)
        for keyword, expected in [('abc_def', ['abc_def']), ('%', ['rate%value']),
                                  ('ABC', ['abc_def', 'abcXdef'])]:
            with self.subTest(keyword=keyword):
                where, params = EventStorage._filters({'keyword': keyword}, 60)
                rows = conn.execute('select sql_text from events where ' + where, params).fetchall()
                self.assertEqual([r[0] for r in rows], expected)

    def test_short_terms_or_and_unknown_identity_remain_conservative(self):
        part = self.part({'sql_text': 'ab xxUniqueNeedlexx'})
        self.assertEqual(len(self.plan(part, 'ab')['entries']), 1)
        self.assertEqual(len(self.plan(part, 'uniqueneedle')['entries']), 1)
        q = {'keyword': 'ab absent', 'keyword_mode': 'OR'}
        plan = self.index.candidate_blocks([part], q, start_epoch_us=self.epoch, end_epoch_us=self.epoch)
        self.assertEqual(len(plan['entries']), 1)
        changed = {**part, 'sha256': 'changed'}
        self.assertEqual(self.plan(changed, 'absent')['unknown_paths'], {part['path']})


class RangeBudgetTests(unittest.TestCase):
    def bucket(self, payload, etag='version'):
        self.calls = []
        def get_object(key, byte_range):
            self.calls.append((key, byte_range))
            a, b = byte_range
            return SimpleNamespace(read=lambda: payload[a:b + 1], headers={'ETag': etag})
        return SimpleNamespace(get_object=get_object)

    def test_budget_stops_before_request_and_pack_ranges_stay_inside_member(self):
        reader = OssRangeReader(self.bucket(b'prefixabcdefghsuffix'), 'pack', 8, 'version',
                                base_offset=6, read_ahead_bytes=1, max_bytes=4, max_requests=1)
        self.assertEqual(reader.read(4), b'abcd')
        with self.assertRaises(OssArchiveError) as error:
            reader.read(1)
        self.assertEqual(error.exception.code, 'OSS_QUERY_BUDGET_EXCEEDED')
        self.assertEqual(self.calls, [('pack', (6, 9))])
        self.assertEqual(reader.stats(), {'range_requests': 1, 'range_bytes': 4})
        reader.close()

    def test_failed_identity_read_is_counted_and_missing_etag_is_rejected(self):
        for etag in ('wrong', ''):
            with self.subTest(etag=etag):
                reader = OssRangeReader(self.bucket(b'abcd', etag), 'object', 4, 'version')
                with self.assertRaises(OssArchiveError):
                    reader.read()
                self.assertEqual(reader.stats(), {'range_requests': 1, 'range_bytes': 4})
                reader.close()

    def test_transport_retry_cannot_bypass_byte_budget(self):
        calls = []
        def fail(*args, **kwargs):
            calls.append(1)
            raise OSError('partial transfer not measurable')
        reader = OssRangeReader(SimpleNamespace(get_object=fail), 'object', 4,
                                max_bytes=4, retry_delay_seconds=0)
        with self.assertRaises(OssArchiveError) as error:
            reader.read()
        self.assertEqual(error.exception.code, 'OSS_QUERY_BUDGET_EXCEEDED')
        self.assertEqual(len(calls), 1)
        reader.close()

    def test_storage_does_not_download_whole_object_after_budget_refusal(self):
        with tempfile.TemporaryDirectory() as directory:
            storage = object.__new__(EventStorage)
            storage.paths = {'scratch': Path(directory)}
            storage._part_body_lock = lambda path: nullcontext()
            storage._local_body_matches = lambda path, part: False
            def download(*args):
                self.fail('budget refusal triggered a full-object fallback')
            archive = SimpleNamespace(
                open_part_reader=lambda part: OssRangeReader(
                    self.bucket(b'PAR1' * 32), 'pack', 128, max_bytes=0),
                download_part=download)
            with self.assertRaises(OssArchiveError) as error:
                storage._read_part_table({'path': str(Path(directory) / 'missing.parquet')},
                                         None, archive)
            self.assertEqual(error.exception.code, 'OSS_QUERY_BUDGET_EXCEEDED')
            self.assertEqual(self.calls, [])

    def test_storage_fallback_preserves_failed_range_accounting_and_logs_reason(self):
        class FailedReader(io.BytesIO):
            def read(self, *args):
                raise OSError('range transport broke')
            def stats(self):
                return {'range_requests': 3, 'range_bytes': 12}
        with tempfile.TemporaryDirectory() as directory:
            storage = object.__new__(EventStorage)
            storage.paths = {'scratch': Path(directory)}
            storage._part_body_lock = lambda path: nullcontext()
            storage._local_body_matches = lambda path, part: False
            def download(part, destination):
                pq.write_table(pa.table({'event_epoch_us': [1]}), destination)
            archive = SimpleNamespace(open_part_reader=lambda part: FailedReader(b'PAR1' * 32),
                                      download_part=download)
            with self.assertLogs('app.storage', level='WARNING') as logs:
                table, origin, stats = storage._read_part_table(
                    {'path': str(Path(directory) / 'missing.parquet')}, None, archive)
            self.assertEqual(table.num_rows, 1)
            self.assertEqual(origin, 'oss-temporary')
            self.assertEqual(stats['range_requests'], 3)
            self.assertEqual(stats['range_bytes'], 12)
            self.assertGreater(stats['full_object_fallback_bytes'], 0)
            self.assertIn('OSError', logs.output[0])

    def test_cancelled_read_does_not_fetch_or_retry(self):
        class Cancelled(RuntimeError):
            code = 'QUERY_CANCELLED'
        def cancel():
            raise Cancelled('stop')
        reader = OssRangeReader(self.bucket(b'abcd'), 'object', 4,
                                check_cancelled=cancel)
        with self.assertRaises(Cancelled):
            reader.read()
        self.assertEqual(self.calls, [])
        reader.close()


class BenchmarkNativeDeadlineTests(unittest.TestCase):
    def test_native_deadline_interrupts_query_and_connection_reaches_terminal(self):
        conn = duckdb.connect()
        self.addCleanup(conn.close)
        conn.execute('SET threads=1')
        begin = time.perf_counter()
        with self.assertRaises((duckdb.InterruptException, TimeoutError)):
            native_page(conn, 'SELECT sum(hash(i::VARCHAR)) FROM range(1000000000000) t(i)',
                        [], begin + 0.025)
        self.assertLess(time.perf_counter() - begin, 2)
        self.assertEqual(conn.execute('SELECT 42').fetchone(), (42,))
        with self.assertRaises(TimeoutError):
            native_page(conn, 'SELECT 42', [], time.perf_counter() - 1)
        self.assertEqual(native_page(conn, 'SELECT 42 AS answer', [], time.perf_counter() + 2),
                         [{'answer': 42}])


if __name__ == '__main__':
    unittest.main()
