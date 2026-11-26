import unittest
from unittest.mock import Mock, patch
from pathlib import Path

from app import binlog_core_history as core
from app.binlog_rows import BinlogRows

Q = dict(instance='fixture', database='demo', table='items', source='binlog', limit=2)


class CoreHistoryTests(unittest.TestCase):
    def setUp(self):
        self.rows = BinlogRows(Mock(path=Path('/tmp/metadata.db')), Mock())
        self.rows._query_full = Mock(return_value=dict(rows=[{'event_id':'recent'}], has_more=False,
                            covered_intervals=[[100,120]], coverage_gaps=[]))
        self.delta = dict(instance_id='fixture', database_name='demo', table_name='items', event_epoch_us=99,
                          change_id='separate-column', column='state', before_present=True, after_present=True,
                          before_value='"b"', after_value='"c"', redacted=False)
        self.rows.ch.rows.return_value = [self.delta]

    @patch.object(core,'cutoff',return_value=100)
    @patch.object(core,'scope',return_value=('scope', {}))
    @patch.object(core,'source_span',return_value=((70,105), ('id',)))
    def test_crossing_reads_disjoint_tiers(self, span, scope, cutoff):
        r = self.rows.query(Q,80,120)
        self.assertEqual(self.rows._query_full.call_args.args[1:],(100,120))
        sql = self.rows.ch.rows.call_args
        self.assertIn(core.TABLE,sql.args[0]); self.assertEqual(sql.kwargs['params']['hi'],99)
        self.assertEqual([x['event_id'] for x in r['rows']],['recent','separate-column'])
        self.assertEqual(r['rows'][1]['before_json'],'{"state":"b"}')
        self.assertEqual(r['rows'][1]['after_json'],'{"state":"c"}')
        self.assertEqual(r['rows'][1]['sql_text'],''); self.assertFalse(r['exact_index_complete'])

    @patch.object(core,'cutoff',return_value=100)
    @patch.object(core,'scope',return_value=('scope',{}))
    @patch.object(core,'source_span',return_value=(None,()))
    def test_uncovered_old_is_not_a_full_read(self, span, scope, cutoff):
        r=self.rows.query(Q,70,90)
        self.assertEqual(r['rows'],[]); self.assertFalse(r['coverage_found']); self.assertEqual(r['coverage_gap_count'],1)
        self.rows._query_full.assert_not_called(); self.rows.ch.rows.assert_not_called()

    @patch.object(core,'cutoff',return_value=100)
    def test_boundary_stays_in_full(self, cutoff):
        self.rows.query(Q,100,120)
        self.rows._query_full.assert_called_once(); self.rows.ch.rows.assert_not_called()

    def test_redacted_value_is_not_printed(self):
        r=core.present({**self.delta,'redacted':True,'before_value':None,'after_value':None})
        self.assertIn('已脱敏',r['after_json'])


if __name__ == '__main__': unittest.main()
