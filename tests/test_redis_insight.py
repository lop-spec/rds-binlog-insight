import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from app.redis_protocol import parse_info, delta, safe_key, RedisWire, load_instances
from app.redis_service import RedisService, pack, unpack

INFO = '# Server\r\nuptime_in_seconds:100\r\nproxy_run_id:proxy-not-node\r\n# CPU\r\nused_cpu_sys:10\r\nused_cpu_user:20\r\n# Commandstats\r\ncmdstat_get:calls=100,usec=1000\r\ncmdstat_eval:calls=20,usec=2000\r\n'
ENTRY = {'instanceId':'r-example01','region':'cn-hangzhou','host':'r-example01.redis.cn-hangzhou.rds.aliyuncs.com','port':6379,'username':'fixture_ro','credentialsFile':'.credentials/redis.json','enabled':True,'label':'fixture','intervalSeconds':20,'retentionDays':30,'maxMiB':64}


class Protocol(unittest.TestCase):
    def snap(self, t=1000, uptime=100):
        return dict(parse_info(INFO.replace('uptime_in_seconds:100','uptime_in_seconds:'+str(uptime))),ts=t,node_id='r-example01-db-1#101')
    def test_counters_and_proxy_not_node_identity(self):
        s=self.snap();self.assertEqual(s['run_id'],'');self.assertEqual(s['commands']['get']['calls'],100)
        self.assertEqual(s['identity_method'],'physical_node_and_uptime')
    def test_delta_and_cpu(self):
        a=self.snap();b=self.snap(1020,120);b['commands']['get']={'calls':130,'usec':1500};b['cpu']['used_cpu_user']=21
        d,r=delta(a,b,20);self.assertEqual(r,'');self.assertEqual(d['commands']['get'],{'calls':30,'usec':500});self.assertEqual(d['process_cpu_percent'],5)
    def test_first_snapshot_is_not_window_cost(self):
        self.assertEqual(delta(None,self.snap(),20),(None,'baseline'))
    def test_gap(self):
        self.assertEqual(delta(self.snap(),self.snap(1080,180),20)[1],'sample_gap')
    def test_switch(self):
        b=self.snap(1020,120);b['node_id']='r-example01-db-1#102';self.assertEqual(delta(self.snap(),b,20)[1],'node_changed')
    def test_restart_without_run_id(self):
        self.assertEqual(delta(self.snap(),self.snap(1020,5),20)[1],'uptime_discontinuity')
    def test_reset(self):
        b=self.snap(1020,120);b['commands']['get']['usec']=2;self.assertEqual(delta(self.snap(),b,20)[1],'counter_reset')
    def test_new_command_no_invented_baseline(self):
        a=self.snap();b=self.snap(1020,120);b['commands']['del']={'calls':50,'usec':100};self.assertNotIn('del',delta(a,b,20)[0]['commands'])
    def test_missing_counters_fails(self):
        with self.assertRaises(RuntimeError):parse_info('# Server\nuptime_in_seconds:1')
    def test_key_privacy(self):
        key='pre:ws:binding:123:customer-secret';self.assertNotIn('123',safe_key(key));self.assertNotIn('secret',safe_key(key))
        self.assertEqual(safe_key('pre:follow_order:outbox:zset:{fo:25}'),'pre:follow_order:outbox:zset:{fo:25}')
    def test_wire_rejects_business_reads_and_writes(self):
        w=object.__new__(RedisWire);w._send=lambda a:'ok'
        for args in [('MONITOR',),('GET','key'),('CONFIG','GET','*'),('IINFO','1','all'),('INFO','KEY','other')]:
            with self.assertRaises(RuntimeError):w.send(*args)
        self.assertEqual(w.send('IINFO','1','server','commandstats','cpu'),'ok')
    def test_compression(self):
        self.assertEqual(unpack(pack({'a':1})),{'a':1})


class History(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.now=2_000_000_000.0
        p=self.root/'redis-instances.json';p.write_text(json.dumps([ENTRY]));p.chmod(0o600)
        self.s=RedisService(self.root,lambda:None,lambda _:None,clock=lambda:self.now)
        self.node={'index':1,'node':'db-1','node_id':'r-example01-db-1#101'}
        self.s.meta[ENTRY['instanceId']]=[self.node]
    def tearDown(self):self.s.stop();self.tmp.cleanup()
    def query(self,start,end,node=''):
        return self.s.query({'instance':'r-example01','startEpochUs':str(int(start*1e6)),'endEpochUs':str(int(end*1e6)),'node':node})
    def seed(self,start,calls=10,usec=100):
        for offset,count in [(0,0),(20,calls),(40,calls*2)]:
            raw=INFO.replace('uptime_in_seconds:100','uptime_in_seconds:'+str(100+offset)).replace('calls=100,usec=1000','calls='+str(100+count)+',usec='+str(1000+count*usec))
            self.s.record(ENTRY,self.node,raw,start+offset)
    def test_record_and_unknown_baseline(self):
        self.seed(self.now-40);r=self.query(self.now-40,self.now);g=next(x for x in r['rows'] if x['command']=='get')
        self.assertEqual(g['calls'],20);self.assertEqual(g['usec'],2000);self.assertIsNone(g['delta_service_seconds']);self.assertEqual(r['coverage'][0]['seconds'],40)
    def test_yesterday_direct_cost_delta(self):
        self.seed(self.now-86440,calls=5);self.seed(self.now-40,calls=10)
        r=self.query(self.now-40,self.now);g=next(x for x in r['rows'] if x['command']=='get');self.assertEqual(g['delta_calls'],10);self.assertEqual(g['delta_service_seconds'],.001)
    def test_partial_window_not_extrapolated(self):
        self.seed(self.now-40);r=self.query(self.now-35,self.now);g=next(x for x in r['rows'] if x['command']=='get');self.assertEqual(g['calls'],10);self.assertEqual(g['seconds'],20)
    def test_missing_node_not_zero_activity(self):
        self.seed(self.now-40);r=self.query(self.now-40,self.now,'db-3');self.assertEqual(r['rows'],[])
    def test_old_cloud_rate_does_not_invent_command_duration(self):
        with self.s.db() as c:
            for shift, rate in [(0, 10), (-86400, 5)]:
                c.execute('INSERT INTO metrics VALUES(?,?,?,?)', ('r-example01','db-1',self.now-20+shift,json.dumps({'CpuUsage':80,'del':rate})))
        r=self.query(self.now-40,self.now)
        self.assertEqual(r['rows'],[])
        self.assertEqual(r['rate_rows'][0]['qps'],10)
        self.assertEqual(r['rate_rows'][0]['baseline_qps'],5)
        self.assertEqual(r['rate_rows'][0]['source'],'cloud_rates_no_duration')
    def test_no_future_bucket(self):
        with self.s.db() as c:c.execute('INSERT INTO buckets VALUES(?,?,?)',('r-example01',self.now+1,'[]'))
        self.assertIsNone(self.query(self.now-40,self.now)['buckets'])
    def test_bad_target_and_window(self):
        for node in ['db-1 OR 1=1','<script>']:
            with self.assertRaises(ValueError):self.query(self.now-40,self.now,node)
        with self.assertRaises(ValueError):self.query(self.now-90000,self.now)
    def test_private_config_and_fixed_host(self):
        p=self.root/'redis-instances.json';e=dict(ENTRY,host='other.redis.aliyuncs.com');p.write_text(json.dumps([e]))
        with self.assertRaises(ValueError):load_instances(self.root)
    def test_restart_baseline_reloaded(self):
        self.seed(self.now-40);other=RedisService(self.root,lambda:None,lambda _:None,clock=lambda:self.now)
        self.assertEqual(other.last[('r-example01','db-1')]['ts'],self.now)
    def test_authorization_fail_closed(self):
        self.assertFalse(self.s.authorized('Bearer fake'))
        p=self.root/'.credentials';p.mkdir();token=p/'redis-ingest.json';token.write_text(json.dumps({'token':'x'*40}));token.chmod(0o600)
        self.assertTrue(self.s.authorized('Bearer '+'x'*40));self.assertFalse(self.s.authorized('Bearer '+'x'*39))
    def sample(self):
        return {'id':'unique_sample_123456','time':self.now,'command':'eval','keys':['pre:follow_order:outbox:zset:{fo:2}'],'category':'claim_due','script':'a'*40,'pod':'worker-abc','sample_rate':.001,'client_us':321,'failed':False}
    def test_ingest_rejects_values_no_cpu_claim_and_dedups(self):
        sample=self.sample()
        with patch('app.redis_service.shutil.disk_usage',return_value=type('D',(),{'free':200*1024**3})()):
            self.s.ingest({'instance':'r-example01','samples':[sample]});self.s.ingest({'instance':'r-example01','samples':[sample]})
            with self.assertRaises(ValueError):self.s.ingest({'instance':'r-example01','samples':[dict(sample,value='secret')]})
            with self.assertRaises(ValueError):self.s.ingest({'instance':'r-example01','samples':[dict(sample,failed={'value':'private'})]})
        r=self.query(self.now-40,self.now+1);self.assertEqual(len(r['key_samples']),1);self.assertEqual(r['key_samples'][0]['latency_scope'],'client_round_trip');self.assertIsNone(r['key_samples'][0]['server_usec'])
    def test_storage_fuse(self):
        with patch('app.redis_service.shutil.disk_usage',return_value=type('D',(),{'free':1024})()):
            with self.assertRaises(RuntimeError):self.s.check_storage(ENTRY)
    def test_no_config_no_worker_or_files(self):
        with tempfile.TemporaryDirectory() as d:
            s=RedisService(Path(d),None,None);s.start();self.assertIsNone(s.thread);self.assertEqual(s.status()['status'],'not_configured');self.assertFalse(s.dbpath.exists())
    def test_cloud_readwrite_account_rejected_before_data_connection(self):
        self.s.cloud_factory=lambda *a:{'Accounts':{'Account':[{'AccountName':'fixture_ro','AccountType':'Normal','AccountStatus':'Available','DatabasePrivileges':{'DatabasePrivilege':[{'AccountPrivilege':'RoleReadWrite'}]}}]}}
        with self.assertRaisesRegex(RuntimeError,'not_readonly'):
            self.s.topology(ENTRY)
    def test_collection_errors_keep_missing(self):
        class Bad:
            def send(self,*a):raise TimeoutError()
            def close(self):pass
        self.s.wire_factory=lambda e:Bad();self.s.next_topology['r-example01']=self.now+60
        with patch.object(self.s,'check_storage'):
            self.s.collect(ENTRY)
        self.assertTrue(self.s.status()['instances'][0]['health']['errors']);self.assertEqual(self.query(self.now-40,self.now)['rows'],[])
