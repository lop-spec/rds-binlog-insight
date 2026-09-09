import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from types import SimpleNamespace

from app.mongo_collector import MongoCollector, load_instances
from app.mongo_service import MongoService
from app.mongo_insight import counter_interval, MINUTE
from tests.test_mongo_insight import record, T


class CollectorGates(unittest.TestCase):
    def collector(self, responses):
        store=Mock();store.publish.return_value={'records':0}
        c=MongoCollector(store,{'instanceId':'dds-example','region':'cn-example-1','families':[]},lambda:None)
        client=Mock();client.call.side_effect=responses;c.rpc=lambda cms=False:client
        return c

    def test_provider_partial_never_publishes(self):
        c=self.collector([{'TotalRecordCount':100,'Items':{'LogRecords':[]}}])
        with self.assertRaisesRegex(RuntimeError,'incomplete'):c.slow_window(T,T+5*MINUTE)
        c.store.publish.assert_not_called()

    def test_source_total_change_never_publishes(self):
        c=self.collector([{'TotalRecordCount':2,'Items':{'LogRecords':[record()]}},
                          {'TotalRecordCount':3,'Items':{'LogRecords':[record()]}}])
        with self.assertRaisesRegex(RuntimeError,'source_changed'):c.slow_window(T,T+5*MINUTE)
        c.store.publish.assert_not_called()

    def test_empty_is_a_complete_source_window(self):
        c=self.collector([{'TotalRecordCount':0,'Items':{'LogRecords':[]}}])
        c.slow_window(T,T+5*MINUTE)
        self.assertEqual(c.store.publish.call_args.args[3],[])

    def test_host_allowlist_rejects_other_instance(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            (root/'mongo-instances.json').write_text(json.dumps([dict(instanceId='dds-example',region='cn-example-1',nodes=['unrelated.example.com'])]))
            with self.assertRaisesRegex(ValueError,'allowlist'):load_instances(root)

    def test_unsupported_metric_does_not_discard_other_series(self):
        response={'Code':200,'Datapoints':json.dumps([dict(instanceId='dds-example',role='Primary',timestamp=T//1000,Average=42)])}
        c=self.collector([RuntimeError('the metric(NoMetric) is not exist'),response,response])
        with patch('app.mongo_collector.METRICS',('NoMetric','CPUUtilization')):
            self.assertEqual(c.cloud_window(T,T+MINUTE),1)
            self.assertEqual(c.state['metrics_unavailable']['NoMetric'],'provider_unsupported')
            self.assertEqual(c.cloud_window(T,T+MINUTE),1)
            self.assertEqual(c.state['metrics_unavailable']['NoMetric'],'provider_unsupported_cached')
        self.assertEqual(c.store.telemetry.call_count,2)

    def test_region_identifiers_with_and_without_numeric_suffix(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td)
            for region in ['cn-example','ap-example-1']:
                value=[dict(instanceId='dds-example',region=region,nodes=['dds-example1.mongodb.'+region+'.rds.aliyuncs.com'])]
                (root/'mongo-instances.json').write_text(json.dumps(value))
                self.assertEqual(load_instances(root),value)

    def test_failed_identity_is_never_cached(self):
        with tempfile.TemporaryDirectory() as td:
            root=Path(td);secret=root/'readonly.json';secret.write_text(json.dumps(dict(username='fixture',password='fixture-test-only',authSource='admin')));secret.chmod(0o600)
            client=Mock();client.admin.command.return_value={'authInfo':{'authenticatedUsers':[{'user':'fixture','db':'admin'}],'authenticatedUserRoles':[{'role':'root','db':'admin'}]}}
            factory=Mock(return_value=client)
            store=Mock();store.root=root/'mongo-insight'
            c=MongoCollector(store,dict(instanceId='dds-example',nodes=['fixture-node'],credentialsFile=str(secret),readonlyUsername='fixture'),lambda:None)
            with patch.dict('sys.modules',{'pymongo':SimpleNamespace(MongoClient=factory,ReadPreference=SimpleNamespace(NEAREST='nearest'))}):
                for _ in range(2):
                    with self.assertRaisesRegex(RuntimeError,'incomplete_node'):c.sample_nodes()
                    self.assertEqual(c.clients,{})
            self.assertEqual(client.close.call_count,2)
            self.assertEqual(client.admin.command.call_count,2)

    def test_missing_configuration_does_not_change_mysql(self):
        with tempfile.TemporaryDirectory() as td:
            service=MongoService(Path(td),lambda:None,start=False)
            self.assertEqual(service.status()['status'],'not_configured')
            service.shutdown()

    def test_native_growth_requires_observed_intervals_in_both_windows(self):
        with tempfile.TemporaryDirectory() as td:
            service=MongoService(Path(td),lambda:None,start=False);store=Mock();service.stores['dds-example']=store
            store.width.return_value=MINUTE;store.read.return_value=([],{'complete':False});store.latest_native.return_value=[]
            base=T-86400*1_000_000
            def samples(start,count):
                return [dict(node='n',role='Primary',interval=dict(status='ok',start_us=start+i*MINUTE,end_us=start+(i+1)*MINUTE,commands={'update':{'total':count}})) for i in range(2)]
            old=samples(base,120)
            def read(instance,kind,lo,hi,**kw):return (old if lo==base else samples(T,240)) if kind=='native' else []
            store.read_telemetry.side_effect=read
            params=dict(instance='dds-example',startEpochUs=T,endEpochUs=T+5*MINUTE,baselineStart=base)
            row=service.query(params)['native_counters'][0]
            self.assertEqual(row['qps_delta'],2);self.assertEqual(row['baseline_coverage_seconds'],120)
            old.pop()
            self.assertIsNone(service.query(params)['native_counters'][0]['qps_delta'])

    def test_ingest_auth_is_not_optional(self):
        with tempfile.TemporaryDirectory() as td:
            service=MongoService(Path(td),lambda:None,start=False)
            self.assertFalse(service.authorized('Bearer arbitrary'))
            p=Path(td)/'.credentials'/'mongo-ingest-token';p.parent.mkdir();p.write_text('example-private-test-token')
            self.assertFalse(service.authorized('Bearer wrong'))
            self.assertTrue(service.authorized('Bearer example-private-test-token'))

    def test_bad_client_counts_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            service=MongoService(Path(td),lambda:None,start=False)
            service.stores['dds-example']=Mock()
            with self.assertRaisesRegex(ValueError,'counts'):
                service.ingest(dict(instance='dds-example',service='app',process_epoch='p',batch_id='b',rows=[dict(start_us=1,end_us=10,count=1,failed=2)]))


if __name__=='__main__':unittest.main()
