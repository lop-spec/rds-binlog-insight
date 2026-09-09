"""Release gates: exact multiplicity, scope, baselines and no false causality."""
import json
import tempfile
import unittest
from pathlib import Path

from app.mongo_insight import normalize_record, rollup_records, analyze, counter_interval, MINUTE
from app.mongo_store import MongoStore

T = 1_800_000_000_000_000


def record(ns='demo.messages_1', command=None, op='command', ms=120, docs=10, **extra):
    q = dict(op=op, ns=ns, command=command or {'find': ns.split('.')[1], 'filter': {'x': 42}},
             millis=ms, docsExamined=docs, keysExamined=docs, nreturned=1,
             replRole={'stateStr': 'PRIMARY'}, **extra)
    return dict(SQLText=json.dumps(q), ExecutionStartTime='2027-01-15T08:00:00Z', QueryTimes=ms,
                DocsExamined=docs, DBName='demo', TableName=ns.split('.')[1])


class MongoGates(unittest.TestCase):
    def norm(self, value):
        return normalize_record(value, 'dds-example', ['messages'])

    def test_command_not_first_key_and_typed_fingerprint(self):
        a=self.norm(record(command={'lsid':{'id':'secret'},'find':'messages_1','filter':{'x':42}}))
        b=self.norm(record(ns='demo.messages_2',command={'find':'messages_2','filter':{'x':99}}))
        self.assertEqual(a['command'], 'find')
        self.assertEqual(a['group_id'], b['group_id'])
        self.assertNotIn('secret', a['profile'])
        c=self.norm(record(command={'find':'messages_1','filter':{'x':'99'}}))
        self.assertNotEqual(a['group_id'],c['group_id'])

    def test_dollar_prefixed_literals_are_redacted(self):
        a=self.norm(record(command={'find':'messages_1','filter':{'password':'$private-value','find':'private-name'}}))
        self.assertNotIn('private-value',a['profile'])
        self.assertNotIn('private-name',a['profile'])
        b=self.norm(record(op='update',command={'q':{'_id':1},'u':{'$set':{'password':'$private-value'}}}))
        self.assertNotIn('private-value',b['profile'])

    def test_sort_order_and_direction_are_semantic(self):
        a=self.norm(record(command={'find':'messages_1','sort':{'x':1,'y':-1}}))
        b=self.norm(record(command={'find':'messages_1','sort':{'y':-1,'x':1}}))
        c=self.norm(record(command={'find':'messages_1','sort':{'x':-1,'y':-1}}))
        self.assertEqual(len({a['group_id'],b['group_id'],c['group_id']}),3)

    def test_suboperation_not_added_to_command_and_missing_not_zero(self):
        a=self.norm(record(ns='demo.$cmd',command={'update':'messages_1'}))
        b=self.norm(record(op='update',command={'q':{'_id':1},'u':{'$set':{'x':1}}}))
        self.assertEqual(a['namespace'], 'demo.messages_1')
        self.assertEqual((a['kind'],b['kind']),('command','suboperation'))
        self.assertIsNone(a['cpu_ns'])
        self.assertNotEqual(a['group_id'],b['group_id'])

    def test_legitimate_identical_records_kept_and_bad_record_fails(self):
        rows=rollup_records([record(),record()], 'dds-example', ['messages'])
        self.assertEqual(sum(r['count'] for r in rows),2)
        with self.assertRaises(ValueError):
            rollup_records([{'SQLText':'broken'}], 'dds-example', [])

    def test_late_query_cannot_explain_earlier_peak(self):
        a=self.norm(record())
        a['start_us']=T+5*60_000_000
        a['finish_us']=a['start_us']+120_000
        a['duration_us']=120_000
        from app.mongo_insight import rollup_events
        rows=rollup_events([a])
        perf=[dict(timestamp=(T+i*60_000_000)//1000,role='Primary',metric='CPUUtilization',value=90 if i==2 else 10) for i in range(1,11)]
        result=analyze(rows,[],perf,T,T+10*60_000_000,coverage=True,baseline_coverage=True)
        self.assertEqual(result['statements'][0]['evidence']['resource_peak_precedes_candidate'],True)
        self.assertNotEqual(result['statements'][0]['conclusion'],'high_confidence')

    def test_longest_counterexample_survives_growth_top_n(self):
        from app.mongo_insight import rollup_events
        old=self.norm(record(ms=65000));old['start_us']=T+5*MINUTE;old['finish_us']=old['start_us']+65_000_000
        old['sample']=json.dumps({**json.loads(old['sample']),'start_us':old['start_us']})
        fast=self.norm(record(command={'distinct':'messages_1','key':'x'},ms=120))
        fast['start_us']=T+MINUTE;fast['finish_us']=fast['start_us']+120_000
        before={**old,'start_us':T-10*MINUTE,'finish_us':T-10*MINUTE+65_000_000}
        metrics=[dict(timestamp=(T+i*MINUTE)//1000,role='Primary',metric='CPUUtilization',value=90 if i==2 else 10) for i in range(1,11)]
        result=analyze(rollup_events([old,fast]),rollup_events([before]),metrics,T,T+10*MINUTE,coverage=True,baseline_coverage=True,limit=1)
        self.assertNotEqual(result['statements'][0]['group_id'],old['group_id'])
        self.assertEqual(result['outliers'][0]['max_us'],65_000_000)
        self.assertTrue(result['outliers'][0]['evidence']['sample_after_resource_peak'])

    def test_no_baseline_no_growth_claim(self):
        rows=rollup_records([record()], 'dds-example', ['messages'])
        t=rows[0]['bucket']
        r=analyze(rows,[],[],t,t+60_000_000,coverage=True,baseline_coverage=False)
        self.assertIsNone(r['statements'][0]['count_delta'])
        self.assertEqual(r['status'],'incomplete_baseline')

    def test_counter_reset_node_and_epoch(self):
        a=dict(node='a',epoch=1,time_us=T,commands={'find':{'total':100}})
        b=dict(node='a',epoch=1,time_us=T+60_000_000,commands={'find':{'total':130}})
        self.assertEqual(counter_interval(a,b)['commands']['find']['total'],30)
        for changed in ({'node':'b'},{'epoch':2},{'commands':{'find':{'total':1}}}):
            self.assertNotEqual(counter_interval(a,{**b,**changed})['status'],'ok')

    def test_immutable_window_replay_and_incomplete_publish(self):
        with tempfile.TemporaryDirectory() as td:
            store=MongoStore(Path(td), backend='parquet')
            rows=[record(),record()]
            t=self.norm(rows[0])['start_us']//300_000_000*300_000_000
            store.publish('dds-example',t,t+300_000_000,rows,['messages'])
            store.publish('dds-example',t,t+300_000_000,rows,['messages'])
            result,coverage=store.read('dds-example',t,t+300_000_000)
            self.assertTrue(coverage['complete'])
            self.assertEqual(sum(x['count'] for x in result),2)
            with self.assertRaises(ValueError):
                store.publish('dds-example',t,t+300_000_000,[{'SQLText':'bad'}],['messages'])
            self.assertEqual(sum(x['count'] for x in store.read('dds-example',t,t+300_000_000)[0]),2)
            self.assertFalse(store.read('dds-example',t,t+600_000_000)[1]['complete'])


if __name__=='__main__': unittest.main()
