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

    def database_record(self, namespace='demo', command=None):
        value=record(ns='demo.$cmd',command=command or {'dbStats':1,'lsid':{'id':'private'},'$db':'demo'})
        document=json.loads(value['SQLText']);document['ns']=namespace
        value['SQLText']=json.dumps(document);value.pop('TableName')
        return value

    def test_database_stats_has_no_fabricated_collection_and_retains_costs(self):
        events=[self.norm(self.database_record(ns)) for ns in ['demo','demo.$cmd','']]
        self.assertEqual(len({e['group_id'] for e in events}),1)
        for event in events:
            profile=json.loads(event['profile']);sample=json.loads(event['sample'])
            self.assertEqual((event['namespace'],event['command'],event['kind']),('demo','dbStats','command'))
            self.assertEqual((profile['database'],profile['scope'],sample['scope']),('demo','database','database'))
            self.assertFalse(profile['incomplete'])
            self.assertEqual(profile['shape'],{'dbStats':'?number'})
            self.assertEqual((event['duration_us'],event['docs'],event['keys']),(120000,10,10))
            self.assertNotIn('private',event['profile'])

    def test_unknown_database_command_retains_costs_with_explicit_incomplete_reason(self):
        with self.assertLogs('app.mongo_insight',level='WARNING') as logs:
            event=self.norm(self.database_record(command={'serverStatus':1,'$db':'demo'}))
        self.assertIn('unrecognized_database_command',logs.output[0])
        self.assertTrue(json.loads(event['profile'])['incomplete'])
        self.assertEqual(event['duration_us'],120000)
        self.assertEqual(event['namespace'],'demo')

    def test_database_namespace_does_not_relax_missing_collection_or_database(self):
        for command in [{'find':'messages_1'},{'update':'messages_1'},{'getMore':123,'collection':'messages_1'}]:
            with self.subTest(command=command),self.assertRaisesRegex(ValueError,'namespace_missing'):
                self.norm(self.database_record(command=command))
        for namespace in ['', '.bad']:
            value=self.database_record(namespace);value['DBName']=''
            with self.assertRaisesRegex(ValueError,'namespace_missing'):
                self.norm(value)
        value=self.database_record(123)
        with self.assertRaisesRegex(ValueError,'invalid_namespace'):
            self.norm(value)

    def test_database_aggregate_and_truncated_collection_command_are_distinct(self):
        event=self.norm(self.database_record(command={'aggregate':1,'pipeline':[{'$currentOp':{}}],'cursor':{}}))
        self.assertEqual(json.loads(event['profile'])['scope'],'database')
        event=self.norm(self.database_record('demo.$cmd',{'$truncated':'find body unavailable'}))
        self.assertNotIn('scope',json.loads(event['profile']))
        self.assertTrue(json.loads(event['profile'])['incomplete'])

    def test_existing_collection_fingerprint_is_unchanged(self):
        self.assertEqual(self.norm(record())['group_id'],'242aad3e2b611648daa30e097d00ee9f746b285f17515ba3f53ea4f31ec29b6e')

    def test_database_record_does_not_block_or_reduce_complete_batch(self):
        import pyarrow.parquet as pq
        from app.mongo_insight import canonical
        with tempfile.TemporaryDirectory() as td:
            store=MongoStore(Path(td),backend='parquet')
            records=[record(),self.database_record(),record()]
            start=self.norm(records[0])['start_us']//300_000_000*300_000_000
            manifest=store.publish('dds-example',start,start+300_000_000,records,['messages'])
            rows,coverage=store.read('dds-example',start,start+300_000_000)
            self.assertTrue(coverage['complete'])
            self.assertEqual((coverage['records'],sum(x['count'] for x in rows)),(3,3))
            self.assertEqual(sum(x['count'] for x in rows if json.loads(x['profile'])['command']=='dbStats'),1)
            raw=store.manifest_path('dds-example',start).parent/manifest['raw']
            self.assertEqual(pq.read_table(raw).column('record').to_pylist(),[canonical(r) for r in records])

    def test_parser_revision_invalidates_replay_without_changing_collection_fingerprint(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as td:
            store=MongoStore(Path(td),backend='parquet')
            start=self.norm(record())['start_us']//300_000_000*300_000_000
            with patch('app.mongo_store.NORMALIZATION_VERSION',2):
                before=store.publish('dds-example',start,start+300_000_000,[record()],['messages'])
            folder=store.manifest_path('dds-example',start).parent
            original=(folder/before['raw']).read_bytes()
            after=store.publish('dds-example',start,start+300_000_000,[record()],['messages'])
            self.assertNotEqual(before['revision'],after['revision'])
            self.assertEqual((folder/before['raw']).read_bytes(),original)
            rows,coverage=store.read('dds-example',start,start+300_000_000)
            self.assertTrue(coverage['complete']);self.assertEqual(sum(r['count'] for r in rows),1)
            self.assertEqual(rows[0]['group_id'],self.norm(record())['group_id'])

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

    def test_native_counts_use_actual_intervals_and_missing_failures_remain_unknown(self):
        from app.mongo_insight import summarize_native_intervals
        samples=[dict(node='n',role='Primary',timestamp=T//1000,interval=dict(status='ok',start_us=T,end_us=T+MINUTE,commands={'update':{'total':120}})),
                 dict(node='n',role='Primary',timestamp=T//1000,interval=dict(status='warmup'))]
        rows,gaps=summarize_native_intervals(samples,T,T+5*MINUTE)
        row=rows[('n','Primary','update')]
        self.assertEqual(row['qps'],2)
        self.assertEqual(row['coverage_seconds'],60)
        self.assertEqual(row['window_seconds'],300)
        self.assertIsNone(row['failed'])
        self.assertEqual(len(gaps),1)

    def test_no_baseline_no_growth_claim(self):
        rows=rollup_records([record()], 'dds-example', ['messages'])
        t=rows[0]['bucket']
        r=analyze(rows,[],[],t,t+60_000_000,coverage=True,baseline_coverage=False)
        self.assertIsNone(r['statements'][0]['count_delta'])
        self.assertEqual(r['status'],'incomplete_baseline')

    def test_missing_baseline_keeps_current_costs_and_sorts_by_real_cost(self):
        rows=rollup_records([record(ms=120),record(command={'aggregate':'messages_1','pipeline':[]},ms=5000)],'dds-example',['messages'])
        t=rows[0]['bucket']
        r=analyze(rows,[],[],t,t+MINUTE,coverage=False,baseline_coverage=False)
        self.assertEqual(r['order'],'duration_total')
        self.assertEqual(r['requested_order'],'duration_growth')
        self.assertEqual(r['statements'][0]['max_us'],5_000_000)
        self.assertEqual(r['statements'][0]['costs']['duration_us']['observed'],5_000_000)
        self.assertIsNone(r['statements'][0]['costs']['duration_us']['delta'])
        self.assertEqual(r['statements'][0]['assessment'],'incomplete_source')
        self.assertEqual([x['kind'] for x in r['totals']],['command'])

    def test_baseline_gap_does_not_block_current_window_correlation(self):
        from app.mongo_insight import rollup_events
        events=[]
        for i in range(1,11):
            a=self.norm(record(ms=i*100));a['start_us']=T+(i-1)*MINUTE;a['finish_us']=a['start_us']+a['duration_us'];events.append(a)
        points=[dict(timestamp=(T+i*MINUTE)//1000,role='Primary',metric='CPUUtilization',value=i*i) for i in range(1,11)]
        r=analyze(rollup_events(events),[],points,T,T+10*MINUTE,coverage=True,baseline_coverage=False)
        row=r['statements'][0]
        self.assertGreater(row['evidence']['pearson'],0.9)
        self.assertEqual(row['assessment'],'incomplete_baseline')
        self.assertIsNone(row['count_delta'])
        missing=analyze(rollup_events(events),[],points,T,T+10*MINUTE,coverage=False,baseline_coverage=False)
        self.assertIsNone(missing['statements'][0]['evidence']['pearson'])

    def test_known_baseline_remains_visible_when_current_source_partial(self):
        rows=rollup_records([record()], 'dds-example', ['messages']);t=rows[0]['bucket']
        r=analyze(rows,rows,[],t,t+MINUTE,coverage=False,baseline_coverage=True)
        row=r['statements'][0]
        self.assertEqual(row['baseline_count'],1)
        self.assertEqual(row['costs']['duration_us']['baseline'],120000)
        self.assertIsNone(row['count_delta'])

    def test_field_coverage_and_unavailable_ranking_are_explicit(self):
        rows=rollup_records([record(cpuNanos=500),record()], 'dds-example', ['messages']);t=rows[0]['bucket']
        r=analyze(rows,[],[],t,t+MINUTE,coverage=True,baseline_coverage=True,order='cpu_growth')
        self.assertEqual(r['order'],'cpu_total')
        cost=r['statements'][0]['costs']['cpu_ns']
        self.assertEqual((cost['observed'],cost['known'],cost['total']),(500,1,2))
        self.assertIsNone(cost['delta'])

    def test_missing_window_ranges_are_compacted_without_losing_gaps(self):
        from app.mongo_store import window_ranges
        self.assertEqual(window_ranges([T,T+5*MINUTE,T+15*MINUTE]),[
            {'start_us':T,'end_us':T+10*MINUTE},{'start_us':T+15*MINUTE,'end_us':T+20*MINUTE}])

    def test_counter_reset_node_and_epoch(self):
        a=dict(node='a',epoch=1,time_us=T,commands={'find':{'total':100}})
        b=dict(node='a',epoch=1,time_us=T+60_000_000,commands={'find':{'total':130}})
        self.assertEqual(counter_interval(a,b)['commands']['find']['total'],30)
        for changed in ({'node':'b'},{'epoch':2},{'commands':{'find':{'total':1}}}):
            self.assertNotEqual(counter_interval(a,{**b,**changed})['status'],'ok')

    def test_invalid_namespace_retains_exact_batch_without_publishing(self):
        import pyarrow.parquet as pq
        from app.mongo_insight import canonical
        with tempfile.TemporaryDirectory() as td:
            store=MongoStore(Path(td),backend='parquet')
            bad=record()
            document=json.loads(bad['SQLText']);document.pop('ns')
            bad.update(SQLText=json.dumps(document),DBName='',TableName='')
            records=[record(),bad,record()]
            t=self.norm(records[0])['start_us']//300_000_000*300_000_000
            for attempt in range(2):
                with self.assertLogs('app.mongo_store',level='WARNING') as logs:
                    with self.assertRaisesRegex(ValueError,'namespace_missing'):
                        store.publish('dds-example',t,t+300_000_000,records,['messages'])
                self.assertIn('record_index=1',logs.output[0])
                self.assertIn('namespace_missing',logs.output[0])
                manifest=store.manifest_path('dds-example',t)
                self.assertFalse(manifest.exists())
                raw=list(manifest.parent.glob('*.raw.parquet'))
                self.assertEqual(len(raw),1)
                self.assertEqual(pq.read_table(raw[0]).column('record').to_pylist(),[canonical(r) for r in records])
                self.assertEqual(list(manifest.parent.glob('*.rollup.parquet')),[])
                self.assertFalse(store.manifests('dds-example',t,t+300_000_000)[1]['complete'])

    def test_failed_raw_write_does_not_parse_or_publish(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory() as td:
            store=MongoStore(Path(td),backend='parquet')
            t=self.norm(record())['start_us']//300_000_000*300_000_000
            with patch('app.mongo_store.atomic_parquet',side_effect=OSError('disk full')),patch('app.mongo_store.normalize_record') as normalize:
                with self.assertRaisesRegex(OSError,'disk full'):
                    store.publish('dds-example',t,t+300_000_000,[record()],[])
                normalize.assert_not_called()
            self.assertFalse(store.manifest_path('dds-example',t).exists())

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


class CorrelationRankingGates(unittest.TestCase):
    def signals(self):
        from app.mongo_insight import rollup_events
        patterns={'rising':[1,2,3,4,5,6,7,8,9,10], 'falling':[10,9,8,7,6,5,4,3,2,1], 'constant':[20]*10}
        events=[]
        for name, values in patterns.items():
            for i,value in enumerate(values):
                event=normalize_record(record(ns='demo.'+name,ms=value*10),'dds-example',[])
                event['start_us']=T+i*MINUTE;event['finish_us']=event['start_us']+event['duration_us']
                events.append(event)
        return rollup_events(events)

    def query(self, metric='CPUUtilization', values=range(1,11), **changes):
        points=[dict(timestamp=(T+(i+1)*MINUTE)//1000,role='Primary',metric=metric,value=v) for i,v in enumerate(values)]
        args=dict(coverage=True,baseline_coverage=False,metric=metric,order='correlation')
        args.update(changes)
        return analyze(self.signals(),[],points,T,T+10*MINUTE,**args)

    def test_all_five_metrics_rank_by_signed_correlation_not_cost_or_growth(self):
        from app.mongo_metrics import ANALYSIS_METRICS
        self.assertEqual(len(ANALYSIS_METRICS),5)
        for metric in ANALYSIS_METRICS:
            with self.subTest(metric=metric):
                result=self.query(metric)
                self.assertEqual(result['order'],'correlation');self.assertEqual(result['order_reason'],'')
                self.assertEqual([r['namespace'] for r in result['statements']],['demo.rising','demo.falling','demo.constant'])
                self.assertAlmostEqual(result['statements'][0]['evidence']['pearson'],1)
                self.assertAlmostEqual(result['statements'][1]['evidence']['pearson'],-1)
                self.assertIsNone(result['statements'][2]['evidence']['pearson'])
                self.assertEqual(result['ranking']['ranked_groups'],2)
                self.assertEqual(result['ranking']['unranked_groups'],1)
                self.assertTrue(result['ranking']['available'])

    def test_changing_metric_signal_reverses_ranking(self):
        result=self.query('AvgRt',list(range(10,0,-1)))
        self.assertEqual(result['statements'][0]['namespace'],'demo.falling')

    def test_missing_source_points_constant_series_or_coarse_grain_never_fall_back(self):
        for args in [dict(coverage=False),dict(values=[1,2]),dict(values=[1]*10),dict(bucket_width=5*MINUTE)]:
            with self.subTest(args=args):
                result=self.query(**args)
                self.assertEqual(result['order'],'correlation');self.assertEqual(result['requested_order'],'correlation')
                self.assertFalse(result['ranking']['available']);self.assertTrue(result['order_reason'])
                self.assertTrue(all(r['evidence']['pearson'] is None for r in result['statements']))

    def test_complete_baseline_does_not_change_current_ranking(self):
        a=self.query();b=self.query(baseline_coverage=True)
        self.assertEqual([r['group_id'] for r in a['statements']],[r['group_id'] for r in b['statements']])

    def test_zero_slow_records_has_an_explicit_unavailable_result(self):
        r=analyze([],[],[],T,T+10*MINUTE,coverage=True,baseline_coverage=True,order='correlation')
        self.assertEqual(r['ranking']['reason'],'no_slow_records')
        self.assertEqual(r['order'],'correlation')


class LockWaitGates(unittest.TestCase):
    def sample(self, minute=1, node='n1', role='Primary', **queue):
        return dict(timestamp=(T+minute*MINUTE)//1000,node=node,role=role,global_lock={'currentQueue':queue})

    def points(self, samples):
        from app.mongo_metrics import lock_wait_points
        return lock_wait_points(samples,T,T+10*MINUTE)

    def test_native_queue_total_and_explicit_zero_are_preserved(self):
        values=self.points([self.sample(total=3),self.sample(2,total=0)])
        self.assertEqual([p['value'] for p in values],[3,0])
        self.assertTrue(all(p['metric']=='LockWaits' and p['unit']=='queued_operations' for p in values))

    def test_complete_readers_writers_sum_is_not_concurrency_or_write_concern(self):
        sample=self.sample(readers=2,writers=4)
        sample.update(ConcurrentReads=999,waitForWriteConcernDuration=888)
        self.assertEqual(self.points([sample])[0]['value'],6)
        self.assertEqual(self.points([self.sample(readers=2)]),[])

    def test_missing_snapshot_is_not_filled_and_role_is_preserved(self):
        values=self.points([self.sample(1,total=2),self.sample(3,role='Secondary',total=4)])
        self.assertEqual([p['timestamp'] for p in values],[(T+MINUTE)//1000,(T+3*MINUTE)//1000])
        self.assertEqual([p['role'] for p in values],['Primary','Secondary'])

    def test_distinct_nodes_or_conflicting_same_time_samples_are_not_averaged(self):
        self.assertEqual(self.points([self.sample(total=2),self.sample(node='n2',total=4)]),[])
        self.assertEqual(self.points([self.sample(total=2),self.sample(total=4)]),[])

    def test_latest_snapshot_not_an_old_good_value_represents_the_minute(self):
        old=self.sample(total=1);old['timestamp']-=30000
        new=self.sample(total=4)
        self.assertEqual(self.points([new,old])[0]['value'],4)
        new['global_lock']={}
        self.assertEqual(self.points([old,new]),[])

    def test_missing_invalid_and_negative_fields_never_become_zero(self):
        for value in [None,-1,float('nan'),True,'2',1.2]:
            with self.subTest(value=value):self.assertEqual(self.points([self.sample(total=value)]),[])

    def test_malformed_queue_or_inconsistent_totals_are_logged_as_gaps(self):
        for lock in ['bad',[],{'currentQueue':[]},{'currentQueue':{'readers':'bad','writers':2}},
                     {'currentQueue':{'total':4,'readers':1,'writers':1}}]:
            sample=self.sample();sample['global_lock']=lock
            with self.subTest(lock=lock),self.assertLogs('app.mongo_metrics',level='WARNING'):
                self.assertEqual(self.points([sample]),[])

    def test_service_uses_compact_native_without_requesting_a_fake_cloud_metric(self):
        from app.mongo_service import MongoService
        from unittest.mock import Mock
        service=MongoService.__new__(MongoService);service.collectors=[]
        store=Mock();store.width.return_value=MINUTE;store.read.return_value=([],{'complete':True})
        store.read_telemetry.side_effect=lambda instance,kind,start,end,**kw:[self.sample(total=3)] if kind=='native' and start==T else []
        store.latest_native.return_value=[];service.stores={'dds-example':store}
        result=service.query(dict(instance='dds-example',startEpochUs=T,endEpochUs=T+10*MINUTE,baselineStart=T-10*MINUTE,metric='LockWaits'))
        self.assertEqual(result['order'],'attribution');self.assertEqual(result['metric_points'][0]['value'],3)
        self.assertFalse(result['ranking']['available'])  # Queue gauges cannot attribute lock ownership.
        self.assertTrue(all(call.args[1]!='metrics' for call in store.read_telemetry.call_args_list))
        self.assertTrue(all(call.kwargs.get('compact') is True for call in store.read_telemetry.call_args_list if call.args[1]=='native'))


if __name__=='__main__': unittest.main()
