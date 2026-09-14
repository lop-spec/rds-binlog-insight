"""Bounded planning must preserve substring, coverage and exact-page semantics."""
import hashlib
import json
import sqlite3
import tempfile
import threading
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq

from app.exact_index import ExactIndex, SEGMENT_SCHEMA, canonical_value
from app.query_cancellation import cancellable_sqlite
from app.query_tasks import QueryCancelled
from app.search_index import SearchIndex
from app.storage import EventStorage


def insert(conn, table, values):
    columns = list(conn.execute(f'PRAGMA table_info({table})'))
    data = {row[1]: values.get(row[1], 0 if 'INT' in row[2] else '') for row in columns}
    conn.execute(f"INSERT INTO {table} ({','.join(data)}) VALUES ({','.join('?' for _ in data)})", list(data.values()))


class Cancellation:
    def __init__(self):
        self.event = threading.Event()

    def check_cancelled(self):
        if self.event.is_set():
            raise QueryCancelled('cancelled')


class PlanningTests(unittest.TestCase):
    def test_sqlite_planning_is_interruptible_and_handler_is_removed(self):
        conn = sqlite3.connect(':memory:')
        control = Cancellation()
        timer = threading.Timer(.05, control.event.set)
        start = time.monotonic()
        timer.start()
        try:
            with self.assertRaises(QueryCancelled), cancellable_sqlite(conn, control):
                conn.execute('WITH RECURSIVE n(x) AS (VALUES(0) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT sum(x) FROM n').fetchone()
            self.assertLess(time.monotonic() - start, 1.0)
            self.assertEqual(conn.execute('select 1').fetchone()[0], 1)
        finally:
            timer.join()
            conn.close()

    def test_catalog_pages_are_bounded_and_cancellable(self):
        storage = EventStorage.__new__(EventStorage)
        class Metadata:
            sizes = []
            def part_catalogs(self, paths, **kwargs):
                self.sizes.append(len(paths))
                return {p: {'sha256': p} for p in paths}
        storage.metadata = Metadata()
        paths = [str(n) for n in range(513)]
        self.assertEqual([p for p, c in storage._iter_query_catalogs(paths)], paths)
        self.assertEqual(storage.metadata.sizes, [128, 128, 128, 128, 1])
        control = Cancellation()
        values = storage._iter_query_catalogs(paths, control)
        next(values)
        control.event.set()
        with self.assertRaises(QueryCancelled):
            list(values)

    def test_search_reads_only_scoped_blocks_and_postings(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            index = SearchIndex(root/'search.sqlite3')
            parts = []
            for n, word in enumerate(('X209281Y', '20928', 'unrelated')):
                path = root/f'{n}.parquet'
                pq.write_table(pa.Table.from_pylist([{
                    'event_epoch_us': 100+n, 'database_name': 'example_app',
                    'table_name': 'customer', 'operation': 'UPDATE',
                    'sql_text': word, 'before_json': '', 'after_json': '',
                    'transaction_id': '', 'source_file_name': '',
                }]), path)
                part = {'path': str(path), 'sha256': str(n), 'logical_part_id': str(n), 'row_count': 1}
                index.index_parquet(part, path)
                parts.append(part)
            statements = []
            connection = index.connection
            @contextmanager
            def traced():
                with connection() as conn:
                    conn.set_trace_callback(statements.append)
                    yield conn
            index.connection = traced
            result = index.candidate_blocks(parts[:2], {'database': 'example_app', 'table': 'customer', 'keyword': '209281'}, start_epoch_us=0, end_epoch_us=1000)
            self.assertEqual([r['path'] for r in result['entries']], [parts[0]['path']])
            block_reads = [q for q in statements if 'SELECT b.* FROM' in q]
            self.assertEqual(len(block_reads), 1)
            self.assertIn('b.part_path=q.path', block_reads[0])
            self.assertTrue(any(q.startswith('INSERT INTO query_hits ') for q in statements))
            self.assertIn('b.id IN (SELECT rowid FROM keyword_fts', block_reads[0])
            # A short OR term cannot be used to exclude an unindexed substring.
            result = index.candidate_blocks(parts[:2], {'keyword': 'not-found 20', 'keyword_mode': 'OR'}, start_epoch_us=0, end_epoch_us=1000)
            self.assertEqual(len(result['entries']), 2)

    def test_exact_coverage_loads_only_unresolved_catalogs(self):
        with tempfile.TemporaryDirectory() as tmp:
            index = ExactIndex(Path(tmp))
            index.registered_tables = frozenset()
            parts = [{'path': str(n), 'sha256': str(n)} for n in range(300)]
            with index.connection() as conn:
                insert(conn, 'segments', {'id': 'segment', 'file_name': 's', 'registry_sha256': index.registry_sha256})
                for part in parts[:200]:
                    insert(conn, 'segment_parts', {'part_path': part['path'], 'logical_part_id': part['sha256'], 'segment_id': 'segment'})
            original_connection = index.connection
            @contextmanager
            def limited_connection():
                with original_connection() as conn:
                    conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 200)
                    yield conn
            index.connection = limited_connection
            sizes = []
            def catalogs(paths):
                sizes.append(len(paths))
                return {p: {'sha256': p, 'databases': ['other'], 'tables': ['other']} for p in paths}
            result = index.coverage(parts, catalogs={}, database='example_app', table='customer', catalog_loader=catalogs)
            self.assertTrue(result['complete'])
            self.assertEqual(result['covered_parts'], 300)
            self.assertEqual(sum(sizes), 100)
            self.assertLessEqual(max(sizes), 128)
            parts[0]['sha256'] = 'replacement'
            result = index.coverage(parts, catalogs={}, database='example_app', table='customer', catalog_loader=catalogs)
            self.assertFalse(result['complete'])
            self.assertIn('0', result['missing_parts'])

    def test_exact_page_sort_offset_and_early_stop(self):
        with tempfile.TemporaryDirectory() as tmp:
            index = ExactIndex(Path(tmp))
            index.registered_tables = frozenset()
            parts = []
            key = canonical_value('42', 8, query=True)
            for n in range(5):
                identity = f'part-{n}'
                name = f'{n}.sqlite'
                epoch = 100 + n
                parts.append({'path': identity, 'sha256': identity})
                with index.connection() as conn:
                    insert(conn, 'segments', {'id': identity, 'file_name': name, 'registry_sha256': index.registry_sha256, 'max_event_epoch_us': epoch})
                    insert(conn, 'segment_parts', {'part_path': identity, 'segment_id': identity, 'logical_part_id': identity})
                    insert(conn, 'part_tables', {'part_path': identity, 'segment_id': identity, 'logical_part_id': identity, 'database_name': 'example_app', 'table_name': 'customer', 'coverage_state': 'complete'})
                conn = sqlite3.connect(index.segments_dir/name)
                conn.executescript(SEGMENT_SCHEMA)
                insert(conn, 'part_tables', {'part_path': identity, 'logical_part_id': identity, 'database_name': 'example_app', 'table_name': 'customer', 'coverage_state': 'complete', 'type_id': 8})
                insert(conn, 'docs', {'doc_id': 1, 'event_id': identity, 'logical_part_id': identity, 'event_epoch_us': epoch, 'operation': 'UPDATE', 'database_name': 'example_app', 'table_name': 'customer'})
                insert(conn, 'exact_values', {'database_name': 'example_app', 'table_name': 'customer', 'type_id': 8, 'value_key': key, 'value_hash': hashlib.blake2b(key, digest_size=16).digest(), 'doc_id': 1})
                conn.commit()
                conn.close()
            kwargs = dict(catalogs={}, database='example_app', table='customer', value='42', start_epoch_us=0, end_epoch_us=1000, operations=['UPDATE'])
            result = index.lookup(parts, limit=1, offset=1, **kwargs)
            self.assertEqual([r['event_id'] for r in result['rows']], ['part-3'])
            self.assertTrue(result['has_more'])
            self.assertEqual(result['segments'], 3)
            result = index.lookup(parts, limit=10, offset=0, **kwargs)
            self.assertEqual([r['event_id'] for r in result['rows']], [f'part-{n}' for n in reversed(range(5))])
            self.assertFalse(result['has_more'])
            # Missing older files cannot be hidden by a full newest-first page.
            original_is_file = Path.is_file
            with patch.object(Path, 'is_file', lambda p: False if p.name == '0.sqlite' else original_is_file(p)):
                result = index.lookup(parts, limit=1, offset=0, **kwargs)
            self.assertFalse(result['complete'])
            self.assertEqual(result['rows'], [])


if __name__ == '__main__':
    unittest.main()
