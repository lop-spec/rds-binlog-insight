import dataclasses
import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from app import binlog_last_change as lc, binlog_rows as br
from app.binlog_lite import RawBinlogError

BASE = 1_700_000_000_000_000
SCOPE = ('instance-a', 'demo', 'customers')
W = lc.Window(*SCOPE, BASE + 20, BASE + 40, '1'*32, 1)


def doc(windows):
    return json.dumps({'version':1, 'windows': [dataclasses.asdict(w) for w in windows]})


class Contract(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / lc.CATALOG
        self.backend = br.BinlogRows(SimpleNamespace(path=self.path.with_name('metadata.sqlite3')),
                                     mock.Mock(), registry=lambda *_: '@1')
        self.backend.coverage = mock.Mock(return_value={'intervals':[[BASE, BASE+80]],'gaps':[]})
        self.q = dict(zip(('instance','database','table'),SCOPE),source='binlog')

    def put(self, *windows):
        self.path.write_text(doc(windows), encoding='utf-8')

    def test_no_catalog_or_other_table_leaves_original_query(self):
        self.backend.ch.rows.return_value = []
        res = self.backend.query(self.q, BASE, BASE+79)
        self.assertEqual(res['tiers_used'],[br.TIER])
        self.assertNotIn('last_change_windows',res)
        self.put(dataclasses.replace(W,table='other'))
        self.backend.ch.reset_mock()
        self.backend.query(self.q,BASE,BASE+79)
        self.assertIn(br.ROWS_TABLE,self.backend.ch.rows.call_args.args[0])
        self.assertNotIn(lc.TABLE,self.backend.ch.rows.call_args.args[0])

    def test_split_paginates_mixed_scope_and_uses_dedicated_key(self):
        self.put(W)
        self.backend.ch.rows.return_value = []
        query = {**self.q,'exact':{'kind':'PRIMARY_KEY','value':'12'},'limit':2,'offset':1}
        res = self.backend.query(query, BASE+10, BASE+45)
        self.assertEqual(res['last_change_windows'],[W.for_display()])
        self.assertFalse(res['exact_index_complete'])
        self.assertIn('末次有效修改',res['coverage_note'])
        c = self.backend.ch.rows.call_args_list
        self.assertEqual([(x.kwargs['params']['lo']-BASE,x.kwargs['params']['hi']-BASE) for x in c],
                         [(40,45),(20,39),(10,19)])
        self.assertIn(lc.TABLE,c[1].args[0]); self.assertIn('key_before_json',c[1].args[0])
        self.assertEqual(c[1].kwargs['params']['wid'],W.window_id)
        self.assertNotIn('wid',c[0].kwargs['params'])

    def test_changed_event_has_no_sql_and_removed_locator_never_reads_old_table(self):
        self.put(W)
        selected = dict(zip(('instance_id','database_name','table_name'),SCOPE),event_id='e',
                        event_epoch_us=W.start_us,before_json='{"@2":"b"}',after_json='{"@2":"c"}',
                        key_before_json='{"@1":12}',key_after_json='{"@1":12}',sql_kind='LAST_CHANGE',
                        row_query_hash=0,operation='UPDATE')
        loc = br.encode_locator(selected)
        self.backend.ch.rows.return_value = [selected]
        detail = self.backend.detail(loc)
        self.assertEqual((detail['row_query'],detail['sql_text'],detail['event_locator']),('', '', loc))
        self.backend.ch.rows.return_value = []
        with self.assertRaises(RawBinlogError) as ctx:
            self.backend.detail(loc)
        self.assertEqual(ctx.exception.code,'EVENT_DETAIL_NOT_RETAINED')
        self.assertIn(lc.TABLE,self.backend.ch.rows.call_args.args[0])

    def test_active_empty_window_does_not_fall_back(self):
        self.put(dataclasses.replace(W,row_count=0))
        self.backend.ch.rows.return_value = []
        result=self.backend.query(self.q,W.start_us,W.end_us-1)
        self.assertEqual(self.backend.ch.rows.call_count,1)
        self.assertIn(lc.TABLE,self.backend.ch.rows.call_args.args[0])
        self.assertEqual(result['rows'],[])
        self.assertTrue(result['last_change_windows'])

    def test_bad_manifest_fails_closed_instead_of_full_or_empty(self):
        self.path.write_text('{"version":1,"version":1,"windows":[]}',encoding='utf-8')
        with self.assertRaises(RawBinlogError):
            self.backend.query(self.q,BASE,BASE+80)
        self.backend.ch.rows.assert_not_called()

    def test_reject_recent_overlap_or_duplicate(self):
        now = time.time_ns()//1000
        for pair in ((W,dataclasses.replace(W,start_us=BASE+30,window_id='2'*32)),
                     (W,dataclasses.replace(W,table='other')),
                     (dataclasses.replace(W,start_us=now-60_000_000,end_us=now),)):
            self.put(*pair)
            with self.assertRaises(ValueError):
                lc.load(self.path,*SCOPE,now_us=now)

    def test_adjacent_boundaries_and_other_days(self):
        second=dataclasses.replace(W,start_us=W.end_us,end_us=W.end_us+10,window_id='2'*32)
        self.assertEqual(lc.slices(W.start_us-1,second.end_us,[W,second]),[
            (second.end_us,second.end_us,None), (second.start_us,second.end_us-1,second),
            (W.start_us,W.end_us-1,W),(W.start_us-1,W.start_us-1,None)])


if __name__ == '__main__':
    unittest.main()
