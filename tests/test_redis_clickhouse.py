import json,os,tempfile,time,unittest,uuid
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from app.clickhouse_client import ClickHouseClient,ClickHouseConfig
from app.redis_service import RedisService
from tests.test_redis_insight import ENTRY

@unittest.skipUnless(os.environ.get('REDIS_CI_FIXTURE')=='1','isolated Redis ClickHouse fixture only')
class RedisClickHouse(unittest.TestCase):
 def setUp(self):
  cfg=ClickHouseConfig.from_env()
  if cfg.host!='127.0.0.1' or cfg.port!=18123 or cfg.database!='mongo_ci_fixture' or cfg.user!='fixture':raise RuntimeError('isolated_fixture_scope_required')
  self.client=ClickHouseClient(replace(cfg,enabled=True));self.tmp=tempfile.TemporaryDirectory();root=Path(self.tmp.name)
  self.identity='r-fixture'+uuid.uuid4().hex[:12];e={**ENTRY,'instanceId':self.identity,'host':self.identity+'.redis.cn-hangzhou.rds.aliyuncs.com'}
  p=root/'redis-instances.json';p.write_text(json.dumps([e]));p.chmod(0o600);self.now=float(int(time.time()));self.s=RedisService(root,None,None,aggregate_client_factory=lambda:self.client,clock=lambda:self.now)
 def tearDown(self):self.s.stop();self.tmp.cleanup()
 def batch(self):
  return {'instance':self.identity,'process':'process01','deployment':'fixture-worker','pod':'worker-1','batch':'batch01','start':self.now-7220,'end':self.now-7200,'rows':[{'command':'get','shape':'pre:ws:binding:*','path':'lookup_binding','script':'','calls':20,'errors':1,'client_us':200,'latency_known_calls':20,'request_bytes':200,'response_bytes':400,'bytes_known_calls':20}],'totals':{'get':20},'dropped_calls':0,'pipeline_count':1,'pipeline_client_us':250}
 def test_counts_replay_and_hour_rollup(self):
  p=self.batch()
  with patch.object(self.s,'check_storage'):self.s.aggregates.ingest(p)
  self.s.aggregates.flush();self.assertEqual(self.s.aggregates.state['status'],'ok',self.s.aggregates.state)
  result=self.s.aggregates.query(self.identity,self.now-7230,self.now-7190)
  self.assertEqual(result['reported_calls'],{'get':20});self.assertEqual(int(result['rows'][0]['calls']),20);self.assertEqual(result['pipeline_client_us'],250)
  with self.s.db() as c:c.execute('UPDATE redis_client_spool SET done=0')
  self.now+=6;self.s.aggregates.flush()
  result=self.s.aggregates.query(self.identity,self.now-7240,self.now-7190)
  self.assertEqual(int(result['rows'][0]['calls']),20)
  self.s.aggregates.rollup_hours(self.client,self.now)
  rows=self.client.json_rows('SELECT sum(calls) calls FROM mongo_ci_fixture.redis_client_hours FINAL WHERE instance={instance:String}',parameters={'instance':self.identity})
  self.assertEqual(int(rows[0]['calls']),20)
 def test_cursor_and_offline_profiles_remain_distinct(self):
  ts=self.now-7200
  self.s.aggregates.profile(self.identity,'db-1',ts,{'groups':{'pre:ws:binding:*':{'observed_keys':2}},'scope':'cursor_sample'})
  p={'instance':self.identity,'node':'db-1','time':ts,'rdb_sha256':'a'*64,'complete':True,'total_keys':20,'groups':{'pre:ws:binding:*':{'keys':20,'estimated_bytes':2000,'expires':15,'types':{'string':20}}}}
  with patch.object(self.s,'check_storage'):self.s.aggregates.import_profile(p)
  self.s.aggregates.flush();self.assertEqual(self.s.aggregates.state['status'],'ok',self.s.aggregates.state)
  rows=self.client.json_rows('SELECT count() n FROM mongo_ci_fixture.redis_prefix_profiles FINAL WHERE instance={instance:String}',parameters={'instance':self.identity});self.assertEqual(int(rows[0]['n']),2)
  rows=self.client.json_rows('SELECT count() n FROM mongo_ci_fixture.redis_prefix_hours FINAL WHERE instance={instance:String}',parameters={'instance':self.identity});self.assertEqual(int(rows[0]['n']),2)
