"""Incident-independent counterexamples: correlation must never certify a cause."""
import json
import unittest

from app.mongo_insight import analyze, normalize_record, rollup_events, MINUTE
from app.resource_evidence import metric_windows, metric_comparison, native_continuity
from app.slowlog_impact import rank_performance_growth, DAY_US
from tests.test_mongo_insight import record, T


class AttributionGates(unittest.TestCase):
    def events(self, *, baseline=False, cpu=1000, duration=100, docs=10, command='find', wait=None):
        result=[]
        for i in range(10):
            extra={} if cpu is None else {'cpuNanos':cpu*(i+1)}
            if wait is not None:extra['waitForWriteConcernDuration']=wait
            e=normalize_record(record(ms=duration*(i+1), docs=docs, command={command:'messages_1'},**extra),'fixture',[])
            e['start_us']=T+i*MINUTE-(DAY_US if baseline else 0)
            e['finish_us']=e['start_us']+e['duration_us']
            e['sample']=json.dumps({**json.loads(e['sample']),'start_us':e['start_us']})
            result.append(e)
        return result

    def points(self, *, baseline=False, value=None, node='a'):
        return [dict(timestamp=(T+i*MINUTE-(DAY_US if baseline else 0))//1000,
                     role='Primary',node=node,metric='CPUUtilization',period=60,value=value if value is not None else i*5)
                for i in range(1,11)]

    def run_case(self, now, before, **kwargs):
        return analyze(rollup_events(now),rollup_events(before),self.points(),T,T+10*MINUTE,
                       coverage=True,baseline_coverage=True,order='attribution',
                       baseline_metrics=self.points(baseline=True,value=5),**kwargs)

    def test_waiting_victim_does_not_outrank_direct_cpu_growth(self):
        victim=self.events(duration=1000,wait=500)
        culprit=self.events(command='aggregate',cpu=10000)
        before=self.events(baseline=True)+self.events(baseline=True,command='aggregate')
        result=self.run_case(victim+culprit,before)
        self.assertEqual(result['statements'][0]['command'],'aggregate')
        row=result['statements'][1]
        self.assertGreater(row['evidence']['pearson'],.99)
        self.assertEqual(row['attribution']['status'],'no_resource_cost_growth')
        self.assertIn('elapsed_growth_without_resource_cost_growth',row['attribution']['reasons'])
        self.assertFalse(row['attribution']['causal'])

    def test_direct_growth_decomposition_reconciles(self):
        result=self.run_case(self.events(cpu=5000),self.events(baseline=True))
        a=result['statements'][0]['attribution']
        self.assertEqual(a['status'],'direct_cost_growth')
        self.assertAlmostEqual(a['frequency_component']+a['per_call_component'],a['delta'])
        self.assertEqual(a['resource_comparison']['status'],'ok')

    def test_missing_cpu_never_becomes_zero_or_elapsed_cpu_candidate(self):
        row=self.run_case(self.events(cpu=None,duration=2000),self.events(baseline=True))['statements'][0]
        self.assertEqual(row['attribution']['status'],'resource_cost_incomplete')
        self.assertIsNone(row['attribution']['delta'])
        self.assertEqual(row['conclusion'],'insufficient_evidence')

    def test_memory_is_stock_even_when_costs_grow(self):
        r=self.run_case(self.events(cpu=10000),self.events(baseline=True),metric='MemoryUtilization')
        self.assertFalse(r['ranking']['available'])
        self.assertEqual(r['statements'][0]['attribution']['status'],'memory_requires_component_deltas')
        self.assertIsNone(r['statements'][0]['evidence']['pearson'])

    def test_resource_not_up_is_retained_as_counterevidence(self):
        r=analyze(rollup_events(self.events(cpu=10000)),rollup_events(self.events(baseline=True)),self.points(value=5),
                  T,T+10*MINUTE,coverage=True,baseline_coverage=True,baseline_metrics=self.points(baseline=True,value=50))
        self.assertIn('resource_window_mean_not_increased',r['statements'][0]['attribution']['reasons'])

    def test_metric_node_change_blocks_attribution_not_just_correlation(self):
        p=self.points();p[-1]['node']='b'
        r=analyze(rollup_events(self.events(cpu=10000)),rollup_events(self.events(baseline=True)),p,
                  T,T+10*MINUTE,coverage=True,baseline_coverage=True,order='attribution')
        self.assertIsNone(r['statements'][0]['evidence']['pearson'])
        self.assertEqual(r['statements'][0]['attribution']['priority'],0)

    def test_duplicate_conflicting_points_do_not_depend_on_input_order(self):
        p=self.points();p.append({**p[0],'value':99})
        for source in (p,list(reversed(p))):
            w,_=metric_windows(source,'CPUUtilization',T,T+10*MINUTE)
            self.assertEqual(w['Primary']['reason'],'ambiguous_metric_samples')

    def test_partial_first_and_last_minutes_are_excluded(self):
        w,times=metric_windows(self.points(),'CPUUtilization',T+MINUTE//2,T+9*MINUTE+MINUTE//2)
        self.assertEqual(times,[T+i*MINUTE for i in range(2,10)])
        self.assertEqual(len(w['Primary']['values']),8)

    def test_cross_window_node_change_is_not_resource_comparison(self):
        now,_=metric_windows(self.points(),'CPUUtilization',T,T+10*MINUTE)
        old,_=metric_windows(self.points(baseline=True,node='b'),'CPUUtilization',T-DAY_US,T-DAY_US+10*MINUTE)
        self.assertEqual(metric_comparison(now['Primary'],old['Primary'])['status'],'node_or_epoch_changed')

    def test_role_only_cms_points_are_not_node_identity_proof(self):
        p=self.points(node='');w,_=metric_windows(p,'CPUUtilization',T,T+10*MINUTE)
        self.assertEqual(metric_comparison(w['Primary'],w['Primary'])['status'],'role_only_identity_unverified')

    def test_missing_baseline_keeps_current_correlation_not_growth_claim(self):
        r=analyze(rollup_events(self.events()),[],self.points(),T,T+10*MINUTE,
                  coverage=True,baseline_coverage=False,order='attribution')
        self.assertGreater(r['statements'][0]['evidence']['pearson'],.99)
        self.assertEqual(r['statements'][0]['attribution']['status'],'incomplete_baseline')
        self.assertFalse(r['ranking']['available'])

    def test_mysql_scan_growth_precedes_wait_only_overlap(self):
        def events(baseline):
            result=[]
            for i in range(6):
                for fp in ('victim','reader'):
                    result.append(dict(event_id=f'{baseline}-{i}-{fp}',node_id='n',sql_id=fp,fingerprint=fp,database_name='demo',
                                       start_us=T+i*MINUTE-(DAY_US if baseline else 0),
                                       duration_ms=1000 if baseline or fp=='reader' else 10000,
                                       rows_examined=10 if baseline or fp=='victim' else 1000,
                                       lock_time_ms=0 if baseline or fp=='reader' else 9000))
            return result
        p=[dict(timestamp=(T+i*MINUTE)//1000,nodeId='n',Average=i*5) for i in range(1,7)]
        r=rank_performance_growth(events(False),events(True),p,start_us=T,end_us=T+6*MINUTE-1,index_complete=True,baseline_complete=True)
        rows=r['nodes'][0]['statements']
        self.assertEqual(rows[0]['sql_id'],'reader')
        self.assertEqual(rows[0]['attribution']['status'],'scan_growth_related_evidence')
        self.assertIn('lock_wait_increased_possible_victim',rows[1]['attribution']['warnings'])
        self.assertFalse(rows[0]['attribution']['causal'])

    def test_different_partial_edges_cannot_create_cost_growth(self):
        r=self.run_case(self.events(cpu=10000),self.events(baseline=True),baseline_start=T-DAY_US+MINUTE//2)
        a=r['statements'][0]['attribution']
        self.assertEqual(a['status'],'incomparable_windows')
        self.assertIsNone(a['delta'])
        self.assertFalse(r['ranking']['available'])

    def test_mysql_missing_baseline_cost_is_not_zero(self):
        now=[dict(event_id=str(i),node_id='n',sql_id='q',fingerprint='q',database_name='demo',
                  start_us=T+i*MINUTE,duration_ms=1000,rows_examined=100) for i in range(6)]
        old=[dict(e,start_us=e['start_us']-DAY_US,rows_examined=None) for e in now]
        p=[dict(timestamp=(T+i*MINUTE)//1000,nodeId='n',Average=i*5) for i in range(1,7)]
        r=rank_performance_growth(now,old,p,start_us=T,end_us=T+6*MINUTE-1,index_complete=True,baseline_complete=True)
        a=r['nodes'][0]['statements'][0]['attribution']
        self.assertIsNone(a['rows_examined']['baseline'])
        self.assertIsNone(a['rows_examined']['delta'])
        self.assertEqual(a['status'],'elapsed_overlap_only')

    def test_native_epoch_change_blocks_cost_candidate(self):
        continuity=native_continuity([dict(role='Primary',node='a',epoch=e) for e in ('old','new')])
        r=self.run_case(self.events(cpu=10000),self.events(baseline=True),continuity=continuity)
        self.assertEqual(r['statements'][0]['attribution']['status'],'node_or_epoch_changed')
        self.assertFalse(r['ranking']['available'])

    def test_invalid_values_are_not_hidden_by_a_later_gap(self):
        for value in (None,float('nan'),float('inf'),-1,True):
            p=self.points();p[0]['value']=value;p.pop()
            windows,_=metric_windows(p,'CPUUtilization',T,T+10*MINUTE)
            self.assertEqual(windows['Primary']['reason'],'invalid_metric_value')

    def test_identical_duplicate_does_not_inflate_metric(self):
        p=self.points();p+=p[:]
        w,_=metric_windows(p,'CPUUtilization',T,T+10*MINUTE)
        self.assertEqual(w['Primary']['reason'],'ok')
        self.assertEqual(len(w['Primary']['values']),10)

    def test_constant_expensive_work_is_not_growth_candidate(self):
        r=self.run_case(self.events(cpu=100000),self.events(cpu=100000,baseline=True))
        self.assertEqual(r['statements'][0]['attribution']['status'],'no_resource_cost_growth')
        self.assertFalse(r['ranking']['available'])

    def test_mysql_overlapping_yesterday_is_rejected(self):
        r=rank_performance_growth([],[],[],start_us=T,end_us=T+2*DAY_US,index_complete=True,baseline_complete=True)
        self.assertEqual(r['status'],'overlapping_baseline')
