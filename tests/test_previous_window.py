import json,tempfile,unittest
from pathlib import Path
from unittest.mock import Mock,patch
from app.comparison_window import previous_window,comparison_window
from app.redis_service import RedisService,pack
from app.redis_ranking import association,interval_series
from tests.test_redis_insight import ENTRY,INFO
from app.slowlog_impact_service import query_resource_overlap
from app.config import Settings

class Windows(unittest.TestCase):
 def test_exact_one_hour_and_nonstandard_duration(self):
  self.assertEqual(previous_window(13*3600+20*60,14*3600+20*60),(12*3600+20*60,13*3600+20*60))
  self.assertEqual(previous_window(1000,1137),(863,1000))
 def test_explicit_window_no_overlap(self):
  self.assertEqual(comparison_window(1000,1100,200),(200,300))
  with self.assertRaises(ValueError):comparison_window(1000,1100,950)
 def test_mysql_orchestration_reads_exact_previous_window(self):
  start=1800000000000000;end=start+3600000000
  backend=Mock();metadata=Mock();backend.serving_enabled=True;backend._window.side_effect=lambda q,r:(q.get('start_epoch_us',start),q.get('end_epoch_us',end));backend._manifest_coverage.return_value={'complete':True,'total_parts':1};backend._scope_sql.side_effect=lambda q,lo,hi:('SELECT fixture',{'lo':lo,'hi':hi});backend._rows.return_value=[]
  query_resource_overlap(metadata,backend,{'source':'slowlog','instance':'fixture'},Settings(),Mock())
  spans=[(c.kwargs['start_epoch_us'],c.kwargs['end_epoch_us']) for c in metadata.parts_in_range.call_args_list]
  self.assertIn((start-(end-start),start),spans)
  self.assertNotIn((start-86400000000,end-86400000000),spans)
 def test_mongo_api_default_reads_previous_window(self):
  from app.mongo_service import MongoService
  store=Mock();store.windows_complete.return_value=True;store.width.return_value=60000000;store.read.return_value=([],{'complete':True});store.read_telemetry.return_value=[];store.latest_native.return_value=[]
  service=object.__new__(MongoService);service.stores={'fixture':store};service.collectors=[];service.tails={};start=1800000000000000;end=start+600000000
  with patch('app.mongo_service.analyze',return_value={}):
   result=service.query({'instance':'fixture','startEpochUs':str(start),'endEpochUs':str(end)})
  self.assertEqual((result['baseline_start'],result['baseline_end']),(start-(end-start),start))
  self.assertEqual(store.read.call_args_list[1].args[1:3],(start-(end-start),start))
 def test_constant_and_negative_coefficients_not_called_high(self):
  y={60*i:i for i in range(10)}
  self.assertAlmostEqual(association(y,y)['r'],1)
  self.assertLess(association({t:10-v for t,v in y.items()},y)['r'],-.99)
  self.assertIsNone(association({t:999 for t in y},y)['r'])
  self.assertIsNone(association({60:1},y)['r'])

class RedisRanking(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();root=Path(self.tmp.name);p=root/'redis-instances.json';p.write_text(json.dumps([ENTRY]));p.chmod(0o600);self.start=2000000040.;self.end=self.start+600
  self.s=RedisService(root,None,None,clock=lambda:self.end+1);self.s.meta[ENTRY['instanceId']]=[{'node':'db-1','node_id':'fixture-db-1#1'}]
 def tearDown(self):self.s.stop();self.tmp.cleanup()
 def seed(self,start,*,current=False,broken=False):
  with self.s.db() as c:
   for i in range(30):
    lo=start+i*20;hi=lo+20;weight=i//3+1;cpu=weight*5
    commands={'ramp':{'calls':weight*20 if current else 20,'usec':weight*1000 if current else 1000},'flat':{'calls':10000000,'usec':10000000},'inverse':{'calls':(11-weight)*20,'usec':(11-weight)*1000},'steady_ramp':{'calls':weight*20,'usec':weight*1000}}
    c.execute('INSERT INTO intervals VALUES(?,?,?,?,?,?)',(ENTRY['instanceId'],'db-1',lo,hi,pack({'commands':commands,'process_cpu_percent':cpu,'derived':{}}),'node_changed' if broken and i==15 else ''))
    c.execute('INSERT INTO snapshots VALUES(?,?,?,?)',(ENTRY['instanceId'],'db-1',hi,pack({'node_id':'fixture#1','run_id':'epoch','cpu':{},'telemetry':{}})))
  for i in range(10):
   key={'kind':'HotKeys','db':0,'key':'pre:ws:binding:*:#fixture','qps':(i+1)*200 if current else 200}
   self.s.evidence.put(ENTRY['instanceId'],'keys','db-1',start+i*60+10,{'items':[key]})
 def query(self):return self.s.query({'instance':ENTRY['instanceId'],'startEpochUs':str(int(self.start*1e6)),'endEpochUs':str(int(self.end*1e6))})
 def test_incremental_correlated_command_and_key_not_big_constant(self):
  self.seed(self.start-600);self.seed(self.start,current=True);d=self.query();r=d['related'];rows={x['command']:x for x in r['commands']}
  self.assertEqual(d['baseline_start'],self.start-600);self.assertEqual(d['baseline_end'],self.start)
  self.assertEqual(rows['ramp']['assessment'],'high_correlation_with_growth');self.assertNotIn('flat',rows);self.assertNotIn('inverse',rows)
  self.assertEqual(rows['steady_ramp']['assessment'],'high_correlation_no_growth');self.assertFalse(r['causal'])
  self.assertEqual(r['keys'][0]['key'],'pre:ws:binding:*:#fixture');self.assertGreater(r['keys'][0]['correlation']['r'],.9)
 def test_node_change_prevents_pooled_correlation(self):
  self.seed(self.start-600);self.seed(self.start,current=True,broken=True);r=self.query()['related'];self.assertEqual(r['commands'],[]);self.assertEqual(r['keys'],[])
 def test_missing_previous_window_is_not_zero(self):
  self.seed(self.start,current=True);r=self.query()['related'];row=next(x for x in r['commands'] if x['command']=='ramp')
  self.assertEqual(row['assessment'],'high_correlation_baseline_missing');self.assertIsNone(row['delta_calls'])
 def test_missing_hot_key_snapshots_not_zero_filled(self):
  self.seed(self.start,current=True)
  with self.s.db() as c:c.execute("DELETE FROM redis_evidence WHERE kind='keys' AND ts>?",(self.start+180,))
  r=self.query()['related'];self.assertEqual(r['keys'],[]);self.assertGreater(r['key_status']['uncomputable_shapes'],0)
