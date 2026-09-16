"""Bounded event pages and cancellation must not regress JOIN correctness."""
from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from app import slowlog_index as module
from app.config import Settings
from app.metadata import MetadataStore
from app.slowlog_index import SlowLogIndex
from app.storage import EventStorage
from tests.test_slowlog_index import _event, _part, _register_slowlog_part


class SlowlogEventExecution(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.end = int(time.time()*1_000_000)-60_000_000
        self.start = self.end-30*86400*1_000_000
        self.index = SlowLogIndex(self.root/'slowlog.sqlite3')
        rows = [_event(f'event-{i:05}',self.start+i*1_000_000,
                       table='customers,orders' if i%2 else 'orders',
                       rows_examined=1,rows_sent=1,query_ms=1) for i in range(1500)]
        source = self.root/'fixture.parquet'
        self.part = _part(source,'fixture',rows)
        self.index.build_part(self.part,source)
        self.query = dict(instance='rm-prod',database='example_app',table='orders',
                          keyword='select',limit=5,offset=3)

    def execute(self,**kwargs):
        return self.index.query_events(self.query,start_epoch_us=self.start,
                                       end_epoch_us=self.end,**kwargs)

    def test_member_query_streams_recent_rows_instead_of_sorting_whole_database(self):
        with patch.object(module,'_decompress_sql',wraps=module._decompress_sql) as decode:
            actual = self.execute()
            bounded_calls = decode.call_count
        with patch.object(self.index,'_event_index_hint',return_value=' INDEXED BY idx_slowlog_event_object_nocase_time'), \
                patch.object(module,'_decompress_sql',wraps=module._decompress_sql) as decode:
            baseline = self.execute()
            baseline_calls = decode.call_count
        self.assertEqual(actual,baseline)
        self.assertTrue(actual['has_more'])
        self.assertEqual([r['event_id'] for r in actual['rows']],
                         [f'event-{i:05}' for i in range(1496,1491,-1)])
        self.assertLess(bounded_calls,50)
        self.assertGreaterEqual(baseline_calls,1500)

    def test_database_only_is_time_ordered_but_exact_combination_keeps_object_index(self):
        for table in ['', 'orders']:
            self.assertIn('idx_slowlog_event_instance_time',
                          self.index._event_index_hint({**self.query,'table':table}))
        self.assertIn('idx_slowlog_event_object_nocase_time',
                      self.index._event_index_hint({**self.query,'table':'customers,orders'}))
        self.assertIn('idx_slowlog_event_fingerprint_time',
                      self.index._event_index_hint({**self.query,'fingerprint':'fp'}))

    def test_cancel_interrupts_sqlite_scan_not_only_after_entire_query_finishes(self):
        class Cancelled(Exception): pass
        control = Mock()
        control.check_cancelled.side_effect = [None,Cancelled('fixture cancellation')]
        self.query['keyword']='absent-keyword'
        with patch.object(module,'_decompress_sql',wraps=module._decompress_sql) as decode:
            with self.assertRaisesRegex(Cancelled,'fixture cancellation'):
                self.execute(control=control)
            self.assertLess(decode.call_count,1500)
        # The cancelled connection closes; future exact queries still succeed.
        self.query['keyword']='select'
        self.assertEqual(len(self.execute()['rows']),5)

    def test_storage_passes_its_task_control_to_event_index(self):
        metadata = MetadataStore(self.root/'metadata.sqlite3')
        _register_slowlog_part(metadata,self.part)
        storage = EventStorage(metadata,self.root/'storage')
        storage.slowlog_index = self.index
        control = Mock()
        with patch.object(storage,'_slowlog_coverage_with_repair',return_value={'complete':True}), \
                patch.object(self.index,'query_events',wraps=self.index.query_events) as query:
            storage.query_events_tiered({**self.query,'source':'slowlog',
                'start_epoch_us':self.start,'end_epoch_us':self.start+1499*1_000_000},
                Settings(db_instance_id='rm-prod'),None,control=control)
        self.assertIs(query.call_args.kwargs['control'],control)


if __name__ == '__main__':
    unittest.main()
