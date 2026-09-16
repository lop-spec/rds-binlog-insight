from __future__ import annotations

import json
import random
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app.clickhouse_query import ClickHouseRawOssUnavailable
from app.config import Settings
from app.metadata import MetadataStore
from app.storage import EventStorage, StorageError
from tests.test_core import remote


class AllSourceRoutingTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        root = Path(self.directory.name)
        self.metadata = MetadataStore(root / 'metadata.sqlite3')
        self.storage = EventStorage(self.metadata, root)
        self.addCleanup(self.storage.slowlog_index.close)
        self.settings = Settings(db_instance_id='rm-fixture', retention_days=60)
        self.epoch = int(time.time() * 1_000_000) - 60_000_000
        for name, kind, indices in [
            ('binlog', 'WriteRowsEventV2', [0, 3, 6, 9]),
            ('slow-log/fixture', 'SLOW_LOG', [2, 5, 8]),
            ('tabularis-audit-fixture', 'TABULARIS_AUDIT', [1, 4, 7]),
        ]:
            file_id, _ = self.metadata.upsert_remote(
                self.settings, remote(name, '2026-07-29T01:00:00Z'))
            ndjson = root / 'fixture.ndjson'
            ndjson.write_text(''.join(json.dumps({
                'event_id': f'event-{i}', 'event_epoch_us': self.epoch + i,
                'raw_event_type': kind, 'operation': 'INSERT',
                'database_name': 'fixture', 'table_name': 'orders',
                'after_json': json.dumps({'order_id': i}),
            }) + '\n' for i in indices))
            self.storage.ingest_ndjson_file(
                file_id=file_id, instance_id='rm-fixture', host_instance_id='host-a',
                source_file_name=name, ndjson_path=ndjson)
        self.query = {'source': '', 'start_epoch_us': self.epoch,
                      'end_epoch_us': self.epoch + 9, 'limit': 3}
        self.database = self.storage.query_events_tiered(
            {**self.query, 'source': 'database', 'limit': 100}, self.settings, None)

    def install_backend(self):
        def query(query, **_kwargs):
            if query['source'] != 'database':
                return None
            self.assertEqual(query['offset'], 0)
            limit = query['limit']
            return {**self.database, 'rows': self.database['rows'][:limit],
                    'has_more': len(self.database['rows']) > limit,
                    'limit': limit, 'offset': 0, 'tiers_used': ['clickhouse-raw-oss']}
        backend = SimpleNamespace(query_events=Mock(side_effect=query), raw_serving=True)
        self.storage.clickhouse_backend = backend
        return backend

    def test_union_matches_legacy_rows_order_and_pagination(self):
        # Do not prime the all-source certificate while computing the reference;
        # a valid certificate rightly bypasses every backend, including CH.
        with patch.object(self.metadata, 'record_complete_query_certificate', return_value=False):
            expected = [self.storage._query_events_tiered_impl(
                {**self.query, 'offset': offset}, self.settings, None)
                        for offset in (0, 3, 6, 9)]
        backend = self.install_backend()
        actual = [self.storage._query_events_tiered_impl(
            {**self.query, 'offset': offset}, self.settings, None)
                  for offset in (0, 3, 6, 9)]
        for before, after in zip(expected, actual, strict=True):
            self.assertEqual([r['event_id'] for r in before['rows']],
                             [r['event_id'] for r in after['rows']])
            self.assertEqual(before['has_more'], after['has_more'])
            self.assertEqual(after['source_merge'], 'clickhouse-database+exact-audit')
            self.assertIn('clickhouse-raw-oss', after['tiers_used'])
        self.assertTrue(backend.query_events.called)
        self.assertEqual(sum(len(page['rows']) for page in actual), 10)

    def test_audit_rank_bound_is_used_by_real_parquet_route(self):
        self.install_backend()
        with patch.object(self.metadata, 'parts_in_range',
                          wraps=self.metadata.parts_in_range) as parts:
            result = self.storage._query_events_tiered_impl(
                self.query, self.settings, None)
        audit_calls = [c.kwargs for c in parts.call_args_list
                       if c.kwargs.get('source') == 'audit']
        self.assertEqual(len(audit_calls), 1)
        self.assertEqual(audit_calls[0]['start_epoch_us'], self.epoch + 6)
        self.assertEqual(audit_calls[0]['end_epoch_us'], self.epoch + 9)
        self.assertEqual(result['audit_search_start_epoch_us'], self.epoch + 6)
        self.assertEqual([r['event_id'] for r in result['rows']],
                         ['event-9', 'event-8', 'event-7'])
        self.assertTrue(result['has_more'])

    def test_audit_bound_needs_unique_full_page_and_proven_more(self):
        query = {**self.query, 'source': 'database', 'offset': 0}
        rows = self.database['rows'][:3]
        for result in [
            {'rows': rows, 'has_more': False},
            {'rows': rows[:2], 'has_more': True},
            {'rows': [rows[0]] * 3, 'has_more': True},
            {'rows': [], 'has_more': True},
        ]:
            with self.subTest(result=result):
                audit = EventStorage._audit_union_query(query, result)
                self.assertEqual(audit['start_epoch_us'], self.epoch)
                self.assertEqual(audit['source'], 'audit')
        self.assertEqual(query['source'], 'database')
        self.assertEqual(query['start_epoch_us'], self.epoch)

    def test_rank_pruning_matches_independent_union_oracle(self):
        # Vary interleaving, timestamp ties, empty/exhausted sources and offset.
        # Oracle never uses the pruning helper or the production sort function.
        def key(row):
            return (row['event_epoch_us'], row['source_file_name'],
                    row['end_position'], row['row_index'], row['event_id'])

        for seed in range(16):
            rng = random.Random(seed)
            rows = [{'event_id': f'event-{i:03}',
                     'event_epoch_us': 100 + rng.randrange(8),
                     'source_file_name': f'file-{rng.randrange(3)}',
                     'end_position': rng.randrange(4),
                     'row_index': rng.randrange(3)} for i in range(64)]
            database = sorted(rows[:seed * 4], key=key, reverse=True)
            audit = sorted(rows[seed * 4:], key=key, reverse=True)
            oracle = sorted(rows, key=key, reverse=True)
            for offset in (0, 1, 9, 60, 64):
                for limit in (1, 3, 7):
                    with self.subTest(seed=seed, offset=offset, limit=limit):
                        top = offset + limit
                        db_result = {'rows': database[:top],
                                     'has_more': len(database) > top}
                        query = {'source': 'database', 'start_epoch_us': 100,
                                 'end_epoch_us': 107, 'limit': top, 'offset': 0}
                        bounded = EventStorage._audit_union_query(query, db_result)
                        selected = [r for r in audit if r['event_epoch_us'] >=
                                    bounded['start_epoch_us']]
                        actual = EventStorage._merge_source_pages(
                            db_result, {'rows': selected[:top],
                                        'has_more': len(selected) > top},
                            limit=limit, offset=offset)
                        self.assertEqual(actual['rows'], oracle[offset:offset + limit])
                        self.assertEqual(actual['has_more'], offset + limit < len(oracle))

    def test_audit_failure_is_not_reported_as_complete(self):
        self.install_backend()
        original = self.storage._query_events_tiered_impl
        with patch.object(self.storage, '_query_events_tiered_impl',
                          return_value={'unavailable_parts': 1}):
            with self.assertRaises(StorageError) as raised:
                original(self.query, self.settings, None)
        self.assertEqual(raised.exception.code, 'ALL_SOURCES_AUDIT_INCOMPLETE')

    def test_deep_offset_does_not_silently_truncate_union(self):
        backend = self.install_backend()
        with self.assertLogs('app.storage', level='WARNING'):
            result = self.storage.query_events_tiered(
                {**self.query, 'offset': 1001}, self.settings, None)
        self.assertEqual(result['rows'], [])
        self.assertNotIn('source_merge', result)
        self.assertEqual(backend.query_events.call_args.args[0]['source'], '')

    def test_manifest_catchup_keeps_previous_all_source_path(self):
        backend = self.install_backend()
        backend.query_events.side_effect = ClickHouseRawOssUnavailable('fixture incomplete')
        with self.assertLogs('app.storage', level='ERROR'):
            result = self.storage._query_events_tiered_impl(self.query, self.settings, None)
        self.assertEqual([r['event_id'] for r in result['rows']],
                         ['event-9', 'event-8', 'event-7'])
        self.assertNotIn('source_merge', result)

    def test_raw_query_failure_still_refuses_unbounded_fallback(self):
        backend = self.install_backend()
        backend.query_events.side_effect = RuntimeError('fixture query resource failure')
        with self.assertLogs('app.storage', level='ERROR'), self.assertRaises(StorageError) as raised:
            self.storage._query_events_tiered_impl(self.query, self.settings, None)
        self.assertEqual(raised.exception.code, 'CLICKHOUSE_RAW_OSS_QUERY_UNAVAILABLE')

    def test_equal_timestamps_use_full_stable_order_key(self):
        rows = [{'event_id': str(i), 'event_epoch_us': 10, 'source_file_name': str(i % 2),
                 'end_position': i // 2, 'row_index': i % 3} for i in range(12)]
        ordered = sorted(rows, key=EventStorage._row_sort_key, reverse=True)
        for offset in range(0, 12, 3):
            top = offset + 3
            database = sorted(rows[::2], key=EventStorage._row_sort_key, reverse=True)
            audit = sorted(rows[1::2], key=EventStorage._row_sort_key, reverse=True)
            result = EventStorage._merge_source_pages(
                {'rows': database[:top], 'has_more': len(database) > top},
                {'rows': audit[:top], 'has_more': len(audit) > top}, limit=3, offset=offset)
            self.assertEqual(result['rows'], ordered[offset:offset + 3])
            self.assertEqual(result['has_more'], offset + 3 < 12)


if __name__ == '__main__':
    unittest.main()
