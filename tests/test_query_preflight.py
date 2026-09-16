from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app import metadata as module
from app.clickhouse_query import ClickHouseRawOssUnavailable
from app.metadata import MetadataStore
from app.storage import StorageError
from tests import test_all_source_routing as routing


class Cancelled(Exception):
    pass


class MetadataCancellationTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.metadata = MetadataStore(Path(directory.name) / 'metadata.sqlite3')

    def test_cancelled_before_connection_opens(self):
        control = Mock()
        control.check_cancelled.side_effect = Cancelled('cancel before connect')
        with patch.object(module.sqlite3, 'connect') as connect:
            with self.assertRaisesRegex(Cancelled, 'cancel before connect'):
                self.metadata.storage_metadata_stats(control=control)
        connect.assert_not_called()

    def test_sql_interrupt_preserves_exception_and_closes_connection(self):
        cancellation = Cancelled('cancel during SQL')
        control = Mock()
        control.check_cancelled.side_effect = [None, None, cancellation]
        with self.assertRaises(Cancelled) as caught:
            with self.metadata.connection(control=control) as conn:
                conn.execute('WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL '
                             'SELECT x+1 FROM n WHERE x<1000000) '
                             'SELECT SUM(x) FROM n').fetchone()
        self.assertIs(caught.exception, cancellation)
        with self.assertRaises(sqlite3.ProgrammingError):
            conn.execute('SELECT 1')
        with self.metadata.connection() as fresh:
            self.assertEqual(fresh.execute('SELECT 1').fetchone()[0], 1)

    def test_unrelated_sql_error_is_not_cancellation(self):
        with self.assertRaisesRegex(sqlite3.OperationalError, 'no such table'):
            with self.metadata.connection(control=Mock()) as conn:
                conn.execute('SELECT * FROM missing_fixture_table')

    def test_certificate_aggregation_is_interruptible_and_rolls_back(self):
        control = Mock()
        cancellation = Cancelled('cancel content-token scan')
        control.check_cancelled.side_effect = [None, None, cancellation]

        def long_token(conn, *_):
            conn.execute('WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL '
                         'SELECT x+1 FROM n WHERE x<1000000) '
                         'SELECT SUM(x) FROM n').fetchone()
            self.fail('uncancellable content token scan')

        with patch.object(self.metadata, '_part_content_token', long_token):
            with self.assertRaises(Cancelled) as caught:
                self.metadata.complete_query_certificate(
                    'fixture', start_epoch_us=0, end_epoch_us=100,
                    control=control)
        self.assertIs(caught.exception, cancellation)
        token, rows = self.metadata.complete_query_certificate(
            'fixture', start_epoch_us=0, end_epoch_us=100)
        self.assertEqual(token['part_count'], 0)
        self.assertIsNone(rows)

    def test_stats_parts_and_probe_forward_control_to_sqlite(self):
        control = Mock()
        with patch.object(self.metadata, 'connection',
                          wraps=self.metadata.connection) as connection:
            self.metadata.storage_metadata_stats(control=control)
            self.metadata.parts_in_range(start_epoch_us=0, end_epoch_us=100,
                                         control=control)
            self.metadata.has_complete_query_certificate(
                'fixture', start_epoch_us=0, end_epoch_us=100, control=control)
        self.assertEqual(connection.call_count, 3)
        self.assertTrue(all(c.kwargs['control'] is control
                            for c in connection.call_args_list))

    def test_certificate_probe_is_exact_but_does_not_validate_contents(self):
        token, _ = self.metadata.complete_query_certificate(
            'fixture', start_epoch_us=0, end_epoch_us=100)
        self.assertTrue(self.metadata.record_complete_query_certificate(
            'fixture', start_epoch_us=0, end_epoch_us=100,
            expected_token=token, rows=[]))
        with patch.object(self.metadata, '_part_content_token',
                          side_effect=AssertionError('probe aggregated parts')):
            self.assertTrue(self.metadata.has_complete_query_certificate(
                'fixture', start_epoch_us=0, end_epoch_us=100))
            self.assertFalse(self.metadata.has_complete_query_certificate(
                'other', start_epoch_us=0, end_epoch_us=100))
            self.assertFalse(self.metadata.has_complete_query_certificate(
                'fixture', start_epoch_us=0, end_epoch_us=101))


class QueryPreflightTests(unittest.TestCase):
    setUp = routing.AllSourceRoutingTests.setUp

    def backend(self, *, result=None, error=None):
        backend = SimpleNamespace(raw_serving=True,
                                  query_events=Mock(return_value=result,
                                                    side_effect=error))
        self.storage.clickhouse_backend = backend
        return backend

    def execute(self, source='binlog', control=None):
        return self.storage._query_events_tiered_impl(
            {**self.query, 'source': source}, self.settings, None,
            control=control)

    def test_clickhouse_cache_miss_does_no_content_token_aggregation(self):
        expected = {'rows': [{'event_id': 'authoritative'}], 'has_more': False}
        backend = self.backend(result=expected)
        with patch.object(self.metadata, '_part_content_token',
                          side_effect=AssertionError('full scan on cache miss')):
            actual = self.execute()
        self.assertEqual(actual['rows'], expected['rows'])
        backend.query_events.assert_called_once()

    def test_incomplete_raw_coverage_still_fails_closed_without_full_scan(self):
        self.backend(error=ClickHouseRawOssUnavailable('fixture incomplete'))
        with patch.object(self.metadata, '_part_content_token',
                          side_effect=AssertionError('full scan on cache miss')):
            with self.assertLogs('app.storage', level='ERROR'):
                with self.assertRaises(StorageError) as caught:
                    self.execute()
        self.assertEqual(caught.exception.code,
                         'CLICKHOUSE_RAW_OSS_QUERY_UNAVAILABLE')

    def test_valid_existing_certificate_still_precedes_clickhouse(self):
        backend = self.backend(error=AssertionError('valid cache ignored'))
        with patch.object(self.metadata, '_part_content_token',
                          wraps=self.metadata._part_content_token) as token:
            actual = self.execute('database')
        self.assertTrue(actual['query_certificate_hit'])
        self.assertEqual([x['event_id'] for x in actual['rows']],
                         [x['event_id'] for x in self.database['rows'][:3]])
        token.assert_called_once()
        backend.query_events.assert_not_called()

    def test_existing_but_stale_certificate_never_bypasses_validation(self):
        with self.metadata.connection() as conn:
            conn.execute('UPDATE parquet_parts '
                         'SET content_revision=content_revision+100000')
        backend = self.backend(result={'rows': [{'event_id': 'fresh'}]})
        actual = self.execute('database')
        self.assertEqual(actual['rows'], [{'event_id': 'fresh'}])
        backend.query_events.assert_called_once()

    def test_declined_backend_preserves_parquet_results_and_cache_token(self):
        with patch.object(self.metadata, 'record_complete_query_certificate',
                          return_value=False):
            expected = self.execute()
        self.backend(result=None)
        with patch.object(self.metadata, '_part_content_token',
                          wraps=self.metadata._part_content_token) as token:
            with self.assertLogs('app.storage', level='WARNING'):
                actual = self.execute()
        self.assertGreaterEqual(token.call_count, 1)
        self.assertEqual(actual['rows'], expected['rows'])
        self.assertEqual(actual['has_more'], expected['has_more'])

    def test_storage_forwards_control_to_metadata_preflight(self):
        control = Mock()
        with patch.object(self.metadata, 'storage_metadata_stats',
                          wraps=self.metadata.storage_metadata_stats) as stats, \
                patch.object(self.metadata, 'complete_query_certificate',
                             wraps=self.metadata.complete_query_certificate) as cert, \
                patch.object(self.metadata, 'parts_in_range',
                             wraps=self.metadata.parts_in_range) as parts:
            self.execute(control=control)
        for method in (stats, cert, parts):
            self.assertIs(method.call_args.kwargs['control'], control)


if __name__ == '__main__':
    unittest.main()
