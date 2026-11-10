import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from app.mongo_pressure import performance_points, pressure_evidence, slow_record_evidence
from app.mongo_insight import MINUTE, normalize_record
from app.mongo_store import MongoStore
from app.mongo_service import MongoService
from app.mongo_sls import SlsReadonly, summarize_logs, validate_sources
from tests.test_mongo_insight import T, record


class PressureTests(unittest.TestCase):
    def points(self, role='Primary', node='n'):
        out = []
        for i in range(12):
            row = {'WtCacheUpdatesUsage': 4+i*.5 if i<6 else 7,
                   'ApplicationEvictions': 0 if i<5 else 10,
                   'WriteTicketsUsed': 8 if i<6 else 20,
                   'WriteTicketsAvailable': 12 if i<6 else 0,
                   'WriteQueue': 0 if i<6 else 50,
                   'CPUUtilization': 30}
            for metric, value in row.items():
                out.append(dict(timestamp=T//1000+i*5000, period=5, metric=metric,
                                role=role,node=node,epoch='epoch',value=value))
        return out

    def test_cache_then_eviction_then_ticket_with_cpu_low(self):
        n=pressure_evidence(self.points(),T,T+MINUTE)['nodes'][0]
        self.assertEqual(n['status'],'cache_pressure_precedes_ticket_saturation')
        self.assertEqual(n['episodes'][0]['samples'],6)
        self.assertIsNone(n['configured_updates_trigger'])
        self.assertFalse(n['causal']); self.assertTrue(n['complete'])
        self.assertTrue(all(p['value']==20 for p in n['write_capacity']))

    def test_zero_available_without_queue_does_not_certify_overload(self):
        p=self.points()
        for x in p:
            if x['metric']=='WriteQueue':x['value']=0
        self.assertEqual(pressure_evidence(p,T,T+MINUTE)['nodes'][0]['episodes'],[])

    def test_gap_breaks_zero_run_not_forward_filled(self):
        p=[x for x in self.points() if not (x['metric']=='WriteTicketsAvailable' and x['timestamp'] in (T//1000+35000,T//1000+45000,T//1000+55000))]
        n=pressure_evidence(p,T,T+MINUTE)['nodes'][0]
        self.assertFalse(n['complete']);self.assertFalse(n['episodes'])

    def test_nodes_roles_epochs_are_never_combined(self):
        p=self.points();p+=self.points(role='Secondary',node='b')
        p.append(dict(p[0],epoch='new',value=30))
        self.assertEqual(len(pressure_evidence(p,T,T+MINUTE)['nodes']),3)

    def test_conflicting_duplicate_is_a_gap(self):
        p=self.points();p.append(dict(p[-2],value=999))
        n=pressure_evidence(p,T,T+MINUTE)['nodes'][0]
        self.assertFalse(n['complete']);self.assertEqual(n['conflicts'],1)

    def test_dds_format_order_unit_and_start_semantics(self):
        payload={'PerformanceKeys':{'PerformanceKey':[dict(Key='MongoDB_WTCacheUsage',ValueFormat='wt_cache_dirty_usage&wt_cache_updates_usage',PerformanceValues={'PerformanceValue':[dict(Date='2027-01-15T08:00:00Z',Value='3&9.9')]})]}}
        from app.mongo_insight import epoch_us
        t=epoch_us('2027-01-15T08:00:00Z')
        p=performance_points(payload,'Primary',t,t+MINUTE,5)
        self.assertEqual(next(x['value'] for x in p if x['metric']=='WtCacheUpdatesUsage'),9.9)
        self.assertTrue(all(x['timestamp_semantics']=='period_start' for x in p))
        payload['PerformanceKeys']['PerformanceKey'][0]['PerformanceValues']['PerformanceValue'][0]['Value']='nan&101'
        self.assertEqual(performance_points(payload,'Primary',t,t+MINUTE,5),[])

    def test_normalizer_retains_wait_costs_not_customer_values(self):
        e=normalize_record(record(command={'find':'events','filter':{'private':'do-not-return'}},
            storage={'data':{'txnBytesDirty':50},'timeWaitingMicros':{'cache':12}},
            queues={'execution':{'totalTimeQueuedMicros':90}},waitForWriteConcernDurationMillis=3),'fixture',[])
        sample=json.loads(e['sample'])
        self.assertEqual(sample['pressure_costs']['cache_wait_us'],12)
        self.assertEqual(sample['pressure_costs']['execution_queue_us'],90)
        self.assertEqual(e['write_wait_us'],3000)
        self.assertNotIn('do-not-return',e['sample'])

    def test_partial_baseline_and_missing_fields_do_not_become_growth(self):
        e=normalize_record(record(command={'find':'events'},storage={'data':{'bytesRead':10}}),'fixture',[])
        row=slow_record_evidence([e],[e],True,False)['groups'][0]
        self.assertIsNone(row['costs']['bytes_read']['delta'])
        self.assertIsNone(row['costs']['cache_wait_us']['observed'])
        self.assertFalse(row['causal'])

    def test_suboperations_excluded_and_wait_only_not_producer(self):
        e=normalize_record(record(command={'find':'events'},storage={'data':{'bytesRead':10}}),'fixture',[])
        sub=dict(e,kind='suboperation')
        row=slow_record_evidence([e,sub],[e],True,True)['groups'][0]
        self.assertEqual(row['count'],1)
        self.assertEqual(row['assessment'],'waiting_or_incomplete_evidence')

    def test_raw_budgets_disclosed_and_no_schema_change(self):
        with tempfile.TemporaryDirectory() as td:
            store=MongoStore(Path(td),backend='parquet')
            store.publish('fixture',T,T+5*MINUTE,[record(),record()],[])
            rows,cov=store.pressure_records('fixture',T,T+5*MINUTE,[],max_records=1)
            self.assertFalse(cov['complete']);self.assertTrue(cov['budget_exhausted'])
            self.assertEqual(len(rows),1)

    def test_updated_cache_metric_does_not_certify_per_statement_cache_cost(self):
        from app.resource_evidence import assess
        self.assertEqual(assess({},'WtCacheUpdatesUsage')['status'],'memory_requires_component_deltas')


class SlsTests(unittest.TestCase):
    def source(self):return dict(id='app',project='fixture-project',logstore='app-logs',endpointTokens=['fixture.invalid:3717'])

    def test_read_only_signed_get_and_fixed_scope(self):
        k=SimpleNamespace(access_key_id='fixture-id',access_key_secret='fixture-only',security_token='fixture-token')
        request=SlsReadonly('cn-example-1',SimpleNamespace(current=lambda:k)).request(self.source(),T,T+MINUTE,0)
        self.assertEqual(request.method,'GET');self.assertIsNone(request.data)
        self.assertIn('/shards/lb?',request.full_url)
        self.assertIn('fixture-project.cn-example-1.log.aliyuncs.com',request.full_url)
        self.assertIn('LOG fixture-id:',request.headers['Authorization'])
        self.assertEqual(request.headers['X-acs-security-token'],'fixture-token')

    def test_target_injection_and_duplicate_ids_rejected(self):
        s=self.source();s['endpointTokens']=['x" or *']
        with self.assertRaises(ValueError):validate_sources([s])
        with self.assertRaises(ValueError):validate_sources([self.source(),self.source()])

    def test_log_body_never_returned_and_partial_is_not_complete(self):
        raw=dict(__time__=T//1_000_000,content='incomplete read of message header SECRET-CUSTOMER-CONTENT',__source__='fixture-node')
        s=summarize_logs([raw],False,'app')
        self.assertNotIn('SECRET',json.dumps(s));self.assertFalse(s['complete'])
        self.assertEqual(s['node_count'],1)
        client=SlsReadonly('cn-example-1',None);client.page=Mock(return_value=([raw]*100,True))
        r=client.collect(self.source(),T,T+MINUTE,max_pages=1)
        self.assertFalse(r['complete']);self.assertEqual(r['rows'],100)


class AdditionalGates(unittest.TestCase):
    def test_http_overlap_lower_bound_and_query_data_redacted(self):
        from app.mongo_sls import summarize_http
        rows=[]
        for i in range(5):
            rows.append({'__time__':T//1_000_000+60+i,'content':
                '[GIN] | 200 | 60s | 192.0.2.1 | GET "/api/getMsgList/42?page=1&key_words=PRIVATE-CUSTOMER"'})
        result=summarize_http(rows,True,dict(id='http',pathToken='getMsgList',maxConcurrent=3))
        self.assertEqual(result['tenants'][0]['observed_overlap_lower_bound'],5)
        self.assertTrue(result['tenants'][0]['exceeded'])
        self.assertNotIn('PRIVATE',json.dumps(result));self.assertTrue(result['complete'])
        bad=summarize_http([{'content':'unparseable'}],True,dict(id='http',pathToken='getMsgList'))
        self.assertFalse(bad['complete'])

    def test_new_collector_persists_separate_pressure_kind(self):
        from app.mongo_collector import MongoCollector
        c=MongoCollector(Mock(),dict(instanceId='dds-example',region='cn-example-1'),lambda:None)
        cms=Mock();cms.call.return_value={'Code':200,'Datapoints':json.dumps([dict(instanceId='dds-example',role='Primary',timestamp=T//1000+60000,Average=42)])}
        from datetime import datetime,timezone
        date=datetime.fromtimestamp(T/1e6,timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        dds=Mock();dds.call.return_value={'PerformanceKeys':{'PerformanceKey':[dict(Key='MongoDB_WTCacheUsage',ValueFormat='wt_cache_updates_usage',PerformanceValues={'PerformanceValue':[dict(Date=date,Value='9')]})]}}
        c.rpc=lambda cms=False: self.cms if cms else dds
        self.cms=cms
        c.cloud_window(T,T+MINUTE)
        calls=c.store.telemetry.call_args_list
        self.assertEqual(calls[-1].args[1],'pressure')
        self.assertEqual(len(calls[-1].args[2]),2)
        self.assertEqual(calls[-1].args[2][0]['timestamp_semantics'],'period_start')


class InvestigationTests(unittest.TestCase):
    def service(self,root):
        service=MongoService(root,lambda:None,start=False)
        store=Mock();store.pressure_records.return_value=([],dict(complete=False))
        service.stores['dds-example']=store
        c=Mock();c.instance='dds-example';c.entry=dict(instanceId='dds-example',region='cn-example-1')
        c.rpc.return_value.call.side_effect=lambda action,params: {'PerformanceKeys':{'PerformanceKey':[]}} if action=='DescribeDBInstancePerformance' else {'TotalRecordCount':0,'Items':{'LogRecords':[]}} if action=='DescribeRunningLogRecords' else {'LogAuditStatus':'Disabled'}
        service.collectors=[c]
        return service,c

    def params(self):return dict(instance='dds-example',startEpochUs=T,endEpochUs=T+MINUTE,baselineStart=T-MINUTE)

    def test_no_window_shifting_no_business_writes_and_cache(self):
        with tempfile.TemporaryDirectory() as td:
            service,c=self.service(Path(td));x=service.investigator.run(self.params())
            self.assertEqual(x['start_us'],T);self.assertEqual(x['root_cause'],'unresolved')
            self.assertEqual(x['sls']['status'],'not_configured');self.assertEqual(x['audit']['status'],'Disabled')
            count=c.rpc.return_value.call.call_count
            service.investigator.run(self.params());self.assertEqual(c.rpc.return_value.call.call_count,count)
            self.assertTrue(all(call.args[0].startswith('Describe') for call in c.rpc.return_value.call.call_args_list))

    def test_wide_or_unconfigured_sls_rejected_before_reads(self):
        with tempfile.TemporaryDirectory() as td:
            service,c=self.service(Path(td))
            for extra in [dict(endEpochUs=T+31*MINUTE),dict(logSource='other'),dict(startEpochUs=T+1)]:
                with self.assertRaises(ValueError):service.investigator.run({**self.params(),**extra})
            c.rpc.assert_not_called()

    def test_provider_failure_is_gap_not_zero_normal(self):
        with tempfile.TemporaryDirectory() as td:
            service,c=self.service(Path(td));c.rpc.return_value.call.side_effect=RuntimeError('fixture')
            result=service.investigator.run(self.params())
            self.assertTrue(result['gaps']);self.assertEqual(result['performance']['nodes'],[])
            self.assertEqual(result['root_cause'],'unresolved')

    def test_updated_cache_series_converts_dds_start_to_analysis_end(self):
        with tempfile.TemporaryDirectory() as td:
            service,c=self.service(Path(td));store=service.stores['dds-example']
            store.width.return_value=MINUTE;store.windows_complete.return_value=True
            store.read.return_value=([],dict(complete=True));store.latest_native.return_value=[]
            store.read_telemetry.side_effect=lambda instance,kind,lo,hi,**kw: [dict(metric='WtCacheUpdatesUsage',timestamp=T//1000,period=60,source='dds',role='Primary',node='',value=9,timestamp_semantics='period_start')] if kind=='pressure' else []
            result=service.query({**self.params(),'metric':'WtCacheUpdatesUsage'})
            self.assertEqual(result['metric_points'][0]['timestamp'],(T+MINUTE)//1000)


if __name__=='__main__':unittest.main()
