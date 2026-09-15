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

    def test_command_bson_utc_is_independent_of_local_timezone(self):
        from datetime import datetime,timezone,timedelta
        from app.mongo_collector import bson_epoch_us
        utc=datetime(2026,9,9,13,0,tzinfo=timezone.utc)
        expected=int(utc.timestamp()*1e6)
        import os,time
        try:
            with patch.dict(os.environ,{'TZ':'Asia/Shanghai'}):
                if hasattr(time,'tzset'):time.tzset()
                self.assertEqual(bson_epoch_us(utc.replace(tzinfo=None)),expected)
                self.assertEqual(bson_epoch_us(utc.astimezone(timezone(timedelta(hours=8)))),expected)
        finally:
            if hasattr(time,'tzset'):time.tzset()

    def test_provider_partial_never_publishes(self):
        c=self.collector([{'TotalRecordCount':100,'Items':{'LogRecords':[]}}])
        with self.assertRaisesRegex(RuntimeError,'incomplete'):c.slow_window(T,T+5*MINUTE)
        c.store.publish.assert_not_called()

    def test_source_total_change_never_publishes(self):
        c=self.collector([{'TotalRecordCount':2,'Items':{'LogRecords':[record()]}},
                          {'TotalRecordCount':3,'Items':{'LogRecords':[record()]}}])
        with self.assertRaisesRegex(RuntimeError,'source_changed'):c.slow_window(T,T+5*MINUTE)
        c.store.publish.assert_not_called()

    def test_dense_window_finishes_after_old_deadline_without_partial_publish(self):
        # 28,449 records require 285 requests; even 1.2s/request exceeds 240s.
        total=28449;clock=[0.0];starts=[]
        c=self.collector([])
        client=Mock()
        def response(action,params):
            self.assertEqual(action,'DescribeSlowLogRecords')
            self.assertEqual(params['PageNumber'],len(starts)+1)
            c.store.publish.assert_not_called()
            starts.append(clock[0]);clock[0]+=1.2
            n=min(100,total-(params['PageNumber']-1)*100)
            return {'TotalRecordCount':total,'Items':{'LogRecords':[record() for _ in range(n)]}}
        client.call.side_effect=response;c.rpc=lambda cms=False:client
        def wait(delay):clock[0]+=delay;return False
        with patch('app.mongo_collector.time.monotonic',side_effect=lambda:clock[0]),patch.object(c.stop_event,'wait',side_effect=wait):
            c.slow_window(T,T+5*MINUTE)
        self.assertGreater(clock[0],240)
        self.assertEqual(len(starts),285)
        self.assertTrue(all(b-a>=2.1-1e-8 for a,b in zip(starts,starts[1:])))
        c.store.publish.assert_called_once()
        self.assertEqual(len(c.store.publish.call_args.args[3]),total)
        self.assertEqual(c.state['slowlog_fetch_progress']['records'],total)

    def test_pacing_shared_between_realtime_and_history_calls(self):
        from concurrent.futures import ThreadPoolExecutor
        c=self.collector([]);clock=[0.0];starts=[];client=Mock()
        def response(*args):starts.append(clock[0]);return {}
        def wait(delay):clock[0]+=delay;return False
        client.call.side_effect=response
        with patch('app.mongo_collector.time.monotonic',side_effect=lambda:clock[0]),patch.object(c.stop_event,'wait',side_effect=wait):
            with ThreadPoolExecutor(max_workers=2) as pool:
                list(pool.map(lambda _:c._slow_page(client,{}),range(32)))
        self.assertEqual(len(starts),32)
        self.assertGreaterEqual(starts[30]-starts[0],60)
        self.assertTrue(all(b-a>=2.1-1e-8 for a,b in zip(starts,starts[1:])))

    def test_cancelled_rate_wait_never_calls_provider_or_publishes(self):
        c=self.collector([]);client=Mock();c.rpc=lambda cms=False:client
        with patch.object(c.stop_event,'wait',return_value=True):
            with self.assertRaisesRegex(RuntimeError,'collector_stopping'):c.slow_window(T,T+5*MINUTE)
        client.call.assert_not_called();c.store.publish.assert_not_called()

    def test_oversized_window_fails_explicitly_without_publishing(self):
        c=self.collector([{'TotalRecordCount':50001,'Items':{'LogRecords':[record()]}}])
        with self.assertRaisesRegex(RuntimeError,'page_limit'):c.slow_window(T,T+5*MINUTE)
        c.store.publish.assert_not_called()

    def test_failed_dense_window_does_not_advance_checkpoint_and_can_retry(self):
        from app.mongo_store import MongoStore,atomic_json
        with tempfile.TemporaryDirectory() as td:
            store=MongoStore(Path(td),backend='parquet')
            c=MongoCollector(store,dict(instanceId='dds-example'),lambda:None)
            checkpoint=store.base(c.instance)/'slow-checkpoint.json';atomic_json(checkpoint,{'next':T})
            before=checkpoint.read_bytes();c.slow_window=Mock(side_effect=TimeoutError('provider'))
            with patch('app.mongo_collector.time.time',return_value=T/1e6+1000):
                with self.assertRaises(TimeoutError):c.slow_tick()
                self.assertEqual(checkpoint.read_bytes(),before)
                c.slow_window=Mock(return_value=dict(end=T+5*MINUTE,records=28449))
                self.assertEqual(c.slow_tick(),28449)
            c.slow_window.assert_called_once_with(T,T+5*MINUTE)
            self.assertEqual(json.loads(checkpoint.read_text())['next'],T+5*MINUTE)

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

    def test_history_fills_missing_windows_without_rewinding_live_checkpoint(self):
        from app.mongo_store import MongoStore,atomic_json
        window=5*MINUTE
        with tempfile.TemporaryDirectory() as td:
            store=MongoStore(Path(td),backend='parquet')
            c=MongoCollector(store,dict(instanceId='dds-example'),lambda:None)
            c.history_bounds=Mock(return_value=(T-10*window,T))
            p=store.base(c.instance)/'slow-checkpoint.json';atomic_json(p,{'next':T});before=p.read_bytes()
            c.slow_window=Mock(side_effect=lambda lo,hi:store.publish(c.instance,lo,hi,[],[]))
            c.history_slow_tick();self.assertEqual(c.slow_window.call_args.args,(T-4*window,T-3*window))
            c.history_slow_tick();self.assertEqual(c.slow_window.call_args.args,(T-5*window,T-4*window))
            self.assertEqual(p.read_bytes(),before)
            c.slow_window.side_effect=RuntimeError('provider rate limited')
            with self.assertRaises(RuntimeError):c.history_slow_tick()
            c.slow_window.side_effect=lambda lo,hi:store.publish(c.instance,lo,hi,[],[])
            c.history_slow_tick();self.assertEqual(c.slow_window.call_args.args,(T-7*window,T-6*window))
            atomic_json(p,{'next':T-2*window});c.slow_window.reset_mock();c.history_slow_tick()
            c.slow_window.assert_not_called()
            self.assertEqual(c.state['history_pause'],'realtime_slowlog_catching_up')

    def test_history_metric_hour_is_persistent_and_no_native_backfill(self):
        from app.mongo_store import MongoStore,atomic_json
        with tempfile.TemporaryDirectory() as td:
            store=MongoStore(Path(td),backend='parquet')
            c=MongoCollector(store,dict(instanceId='dds-example'),lambda:None)
            c.history_bounds=Mock(return_value=(T-60*MINUTE,T))
            atomic_json(store.base(c.instance)/'slow-checkpoint.json',{'next':T})
            c.cloud_window=Mock(return_value=120);c.sample_nodes=Mock()
            c.history_metric_tick();c.history_metric_tick()
            c.cloud_window.assert_called_once_with(T-60*MINUTE,T,state_key='history_metrics_unavailable')
            c.sample_nodes.assert_not_called()
            self.assertEqual(c.state['history_metric_progress']['missing_hours'],0)

    def test_history_target_does_not_age_out_initial_missing_baseline(self):
        from app.mongo_store import MongoStore
        with tempfile.TemporaryDirectory() as td:
            c=MongoCollector(MongoStore(Path(td),backend='parquet'),dict(instanceId='dds-example'),lambda:None)
            with patch('app.mongo_collector.time.time',return_value=T/1e6):start,end=c.history_bounds()
            with patch('app.mongo_collector.time.time',return_value=T/1e6+3600):later_start,later_end=c.history_bounds()
            self.assertEqual(start,later_start)
            self.assertEqual(later_end-end,3600*1e6)

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
