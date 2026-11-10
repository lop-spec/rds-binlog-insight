import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace
from app.redis_telemetry import enrich,config_numbers,derived
from app.redis_protocol import parse_info,delta,RedisWire
from app.redis_context import key_shape,WorkloadReader
from app.redis_diagnosis import unit_costs,poll_model
from app.redis_service import RedisService
from app.redis_history import decode
from tests.test_redis_insight import INFO,ENTRY

class Fields(unittest.TestCase):
 def test_only_numeric_config_whitelist(self):
  text='followorder:\n  worker:\n    enable: true\n    deliver:\n      interval_ms: 500\n      batch: 100\n      password: 12345\n    host: secret\nmysql:\n  password: 12345\n'
  got=config_numbers(text);self.assertEqual(got,{'followorder.worker.enable':True,'followorder.worker.deliver.interval_ms':500,'followorder.worker.deliver.batch':100});self.assertNotIn('secret',json.dumps(got))
 def test_dynamic_config_not_evaluated(self):
  self.assertEqual(config_numbers('followorder:\n  worker:\n    enable: *anchor\n    batch: ${PASSWORD}\n'),{})
 def test_stats_memory_and_missing_sections(self):
  t=enrich({'stats':{'total_commands_processed':'100','expired_keys':'20','host':'private'},'memory':{'used_memory':'1000'},'keyspace':{'db0':'keys=20,expires=19,avg_ttl=900'}})
  self.assertEqual(t['counters']['expired_keys'],20);self.assertNotIn('expired_keys',t['missing_fields']);self.assertIn('errorstats',t['missing_sections']);self.assertNotIn('host',str(t));self.assertEqual(t['keyspace']['db0']['expires'],19)
 def test_percentiles_are_not_window_quantiles(self):
  t=enrich({'latencystats':{'latency_percentiles_usec_get':'p50=1,p99=8,p99.9=10'},'errorstats':{'errorstat_ERR':'count=2'}})
  self.assertEqual(t['percentiles']['get']['p99.9'],10);self.assertIn('not_window',t['percentile_scope']);self.assertEqual(t['error_counts'],{'ERR':2})
 def test_cpu_residual_never_claimed_network(self):
  a={'cpu':{'used_cpu_sys':1,'used_cpu_user':2},'telemetry':{'counters':{'total_commands_processed':100}}};b={'cpu':{'used_cpu_sys':2,'used_cpu_user':3},'telemetry':{'counters':{'total_commands_processed':120}}}
  v=derived(a,b,20,10);self.assertIsNone(v['non_command_cpu_seconds']);self.assertEqual(v['cpu_usec_per_processed_command'],100000)
 def test_command_failures_missing_not_zero(self):
  s=parse_info(INFO);self.assertNotIn('failed_calls',s['commands']['get'])
  s=parse_info(INFO.replace('calls=100,usec=1000','calls=100,usec=1000,rejected_calls=2,failed_calls=3'));self.assertEqual(s['commands']['get']['failed_calls'],3)
 def test_shapes_have_bounded_namespace_no_id_hash(self):
  self.assertEqual(key_shape('pre:ws:binding:Alice123:999'),'pre:ws:binding:*:*')
  self.assertEqual(key_shape('pre:follow_order:outbox:processing:zset:{fo:25}'),'pre:follow_order:outbox:processing:zset:{fo:25}')
 def test_no_host_root_from_unit_index(self):
  values={(n,'get'):{'calls':100,'usec':u,'seconds':60} for n,u in [('db-0',100),('db-1',140),('db-2',100)]}
  v=unit_costs(values);self.assertEqual(next(x for x in v['nodes'] if x['node']=='db-1')['median_command_index'],1.4);self.assertIn('not_host_cause',v['nodes'][0]['scope'])
 def test_peers_with_different_coverage_not_compared(self):
  self.assertEqual(unit_costs({('db-'+str(i),'get'):{'calls':100,'usec':100,'seconds':20 if i==1 else 60} for i in range(3)})['rows'],[])
 def test_worker_missing_enable_unknown(self):
  source={'time':100,'data':{'deployments':[{'deployment':'worker','pods':[{'ready':True}]}]}}
  self.assertIsNone(poll_model(source,{})['deployments'][0]['prediction'])
 def test_wire_metadata_limited_to_current_page(self):
  w=object.__new__(RedisWire);w._send=lambda a:'ok';w.allow_metadata(['key'])
  self.assertEqual(w.send('TTL','key'),'ok')
  with self.assertRaises(RuntimeError):w.send('TTL','not-registered')
  with self.assertRaises(RuntimeError):w.send('GET','key')
  with self.assertRaises(RuntimeError):w.send('ISCAN','1','0','COUNT','1000')
  self.assertEqual(w.send('ISCAN','1','0','COUNT','16'),'ok')

class Evidence(unittest.TestCase):
 def setUp(self):
  self.t=tempfile.TemporaryDirectory();self.root=Path(self.t.name);p=self.root/'redis-instances.json';p.write_text(json.dumps([ENTRY]));p.chmod(0o600);self.now=2_000_000_000.;self.s=RedisService(self.root,lambda:None,lambda _:None,clock=lambda:self.now)
 def tearDown(self):self.s.stop();self.t.cleanup()
 def test_changes_are_observed_not_exact_event_times(self):
  h=self.s.evidence;h.changed('r-example01','sources','',self.now-30,{'replicas':2},'scale');h.changed('r-example01','sources','',self.now,{'replicas':6},'scale');v=h.rows('r-example01','event',self.now-60,self.now+1)['rows'];self.assertEqual(len(v),1);self.assertFalse(v[0]['data']['exact_change_time'])
 def test_no_future_snapshot(self):
  h=self.s.evidence;h.put('r-example01','queues','',self.now+5,{'items':[]});self.assertIsNone(h.latest('r-example01','queues',before=self.now))
 def test_rollup_is_reproducible_no_double_count(self):
  node={'node':'db-1','node_id':'r-example01-db-1#1'};start=int((self.now-600)//60)*60
  for i in range(7):
   raw=INFO.replace('uptime_in_seconds:100','uptime_in_seconds:'+str(100+i*20)).replace('calls=100,usec=1000','calls='+str(100+i*10)+',usec='+str(1000+i*100))
   self.s.record(ENTRY,node,raw,start+i*20)
  self.s.evidence.compact(True);first=self.s.evidence.rollups('r-example01',start-60,start+300,60)['rows'];self.s.evidence.compact(True);second=self.s.evidence.rollups('r-example01',start-60,start+300,60)['rows'];self.assertEqual(first,second);self.assertEqual(sum(r['commands']['get']['calls'] for r in first),60)
 def test_rollup_keeps_node_fields_and_daily_view(self):
  node={'node':'db-1','node_id':'r-example01-db-1#1'};start=int(self.now//3600)*3600-7200
  for i in range(7):
   raw=INFO.replace('uptime_in_seconds:100','uptime_in_seconds:'+str(100+i*20)).replace('used_cpu_sys:10','used_cpu_sys:'+str(10+i)).replace('calls=100,usec=1000','calls='+str(100+i*10)+',usec='+str(1000+i*100))+'# Stats\r\ntotal_commands_processed:'+str(1000+i*20)+'\r\n# Memory\r\nused_memory:'+str(1000+i*100)+'\r\n'
   self.s.record(ENTRY,node,raw,start+i*20)
  self.s.evidence.compact(True)
  minutes=self.s.evidence.rollups('r-example01',start-60,start+300,60)['rows']
  self.assertEqual(sum(r['counter_deltas'].get('total_commands_processed',0) for r in minutes),120)
  self.assertEqual(max(r['gauges']['used_memory']['max'] for r in minutes),1600)
  out=self.s.history_query({'instance':'r-example01','startEpochUs':str(int((start-60)*1e6)),'endEpochUs':str(int(self.now*1e6)),'period':'86400','summary':'1'})
  self.assertEqual(sum(r['calls'] for r in out['rows'] if r['command']=='get'),60);self.assertTrue(out['points']);self.assertFalse(out['truncated'])
 def batch(self):
  row={'command':'get','shape':'pre:ws:binding:*','path':'lookup_binding','script':'','calls':20,'errors':1,'client_us':200,'latency_known_calls':20,'request_bytes':200,'response_bytes':400,'bytes_known_calls':20}
  return {'instance':'r-example01','process':'process01','deployment':'fixture-worker','pod':'worker-1','batch':'batch01','start':self.now-20,'end':self.now,'rows':[row],'totals':{'get':20},'dropped_calls':0,'pipeline_count':0,'pipeline_client_us':0}
 def ingest(self,p):
  with patch.object(self.s,'check_storage'):return self.s.aggregates.ingest(p)
 def test_aggregate_dedup_and_conflict(self):
  p=self.batch();self.assertTrue(self.ingest(p)['accepted']);self.assertTrue(self.ingest(p)['duplicate']);p['rows'][0]['client_us']=201
  with self.assertRaisesRegex(ValueError,'conflict'):self.ingest(p)
 def test_aggregate_rejects_values_ids_and_unknown_fields(self):
  for key,value in [('value','private'),('keys',['private'])]:
   p=self.batch();p['rows'][0][key]=value
   with self.assertRaises(ValueError):self.ingest(p)
  p=self.batch();p['rows'][0]['shape']='pre:ws:binding:1234'
  with self.assertRaises(ValueError):self.ingest(p)
 def test_overflow_bucket_retains_command_totals(self):
  p=self.batch();p['rows'][0].update(command='other',shape='other',path='other');self.assertTrue(self.ingest(p)['accepted'])
 def test_aggregate_sum_mismatch_rejected(self):
  p=self.batch();p['totals']['get']=50
  with self.assertRaises(ValueError):self.ingest(p)
 def test_query_without_ch_does_not_create_schema(self):
  self.s.aggregates.client_factory=lambda:(_ for _ in ()).throw(AssertionError('must not connect'))
  self.assertEqual(self.s.aggregates.query('r-example01',self.now-60,self.now)['status'],'not_connected')
 def test_offline_import_is_value_free_and_idempotent(self):
  p={'instance':'r-example01','node':'db-1','time':self.now,'rdb_sha256':'a'*64,'complete':True,'total_keys':20,'groups':{'pre:ws:binding:*':{'keys':20,'estimated_bytes':2000,'expires':15,'types':{'string':20}}}}
  with patch.object(self.s,'check_storage'):
   self.assertTrue(self.s.aggregates.import_profile(p)['accepted']);self.assertTrue(self.s.aggregates.import_profile(p)['duplicate'])
   p['groups']['pre:ws:binding:*']['keys']=21
   with self.assertRaises(ValueError):self.s.aggregates.import_profile(p)
 def test_offline_import_rejects_raw_identifiers(self):
  p={'instance':'r-example01','node':'db-1','time':self.now,'rdb_sha256':'a'*64,'complete':True,'total_keys':20,'groups':{'pre:ws:binding:alice':{'keys':20,'estimated_bytes':2000,'expires':15,'types':{'string':20}}}}
  with self.assertRaises(ValueError):self.s.aggregates.import_profile(p)
 def test_late_window_rebuilds_its_actual_hour(self):
  class Fake:
   config=SimpleNamespace(database='fixture');queries=[]
   def query(self,s,**kw):self.queries.append(s);return ''
   def json_rows(self,s,**kw):return [{'bytes':0}]
   def insert_json_rows(self,*a,**kw):return 1
  fake=Fake();self.s.aggregates.client_factory=lambda:fake;p=self.batch();p['start']-=5*3600;p['end']-=5*3600;self.ingest(p);self.s.aggregates.flush()
  hour=int(p['start']//3600)*3600
  inserts=[s for s in fake.queries if 'INSERT INTO fixture.redis_client_hours' in s]
  self.assertTrue(any('toDateTime('+str(hour)+')' in s for s in inserts),'accepted late data was not selected for hourly rollup')
 def test_dirty_hour_survives_restart_and_failed_rebuild(self):
  hour=int((self.now-5*3600)//3600)*3600
  with self.s.db() as c:c.execute('INSERT INTO redis_ch_dirty_hours VALUES(?)',(hour,))
  other=RedisService(self.root,None,None,clock=lambda:self.now)
  class Broken:
   config=SimpleNamespace(database='fixture')
   def query(self,*a,**kw):raise TimeoutError()
  with self.assertRaises(TimeoutError):other.aggregates.rollup_hours(Broken(),self.now)
  with other.db() as c:self.assertEqual(c.execute('SELECT hour FROM redis_ch_dirty_hours').fetchone()[0],hour)
  class Good:
   config=SimpleNamespace(database='fixture');calls=0
   def query(self,*a,**kw):self.calls+=1;return ''
  fake=Good();other.aggregates.rollup_hours(fake,self.now);self.assertEqual(fake.calls,2);other.aggregates.rollup_hours(fake,self.now);self.assertEqual(fake.calls,2);other.stop()
 def test_ch_idempotent_spool_retry(self):
  class Fake:
   config=SimpleNamespace(database='fixture');tables=[];inserts=[]
   def query(self,s,**kw):self.tables.append(s);return ''
   def json_rows(self,s,**kw):return [{'bytes':0}]
   def insert_json_rows(self,table,rows,**kw):self.inserts.append((table,rows));return len(rows)
  fake=Fake();self.s.aggregates.client_factory=lambda:fake;self.ingest(self.batch());self.s.aggregates.flush();self.now+=30;self.s.aggregates.flush();self.assertEqual(len(fake.inserts),1);self.assertEqual(fake.inserts[0][1][0]['calls'],20)
