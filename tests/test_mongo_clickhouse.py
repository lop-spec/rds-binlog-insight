"""Runs only against the isolated CI fixture, never the operational database."""
import os
import tempfile
import unittest
from pathlib import Path
from app.mongo_store import MongoStore
from tests.test_mongo_insight import record,T
from app.mongo_insight import MINUTE


@unittest.skipUnless(os.environ.get('MONGO_CI_FIXTURE')=='1','isolated CI ClickHouse fixture only')
class ClickHouseGates(unittest.TestCase):
    def test_serving_exactness_and_loss_detection(self):
        self.assertEqual(os.environ.get('RDS_BINLOG_CLICKHOUSE_DATABASE'),'mongo_ci_fixture')
        with tempfile.TemporaryDirectory() as td:
            store=MongoStore(Path(td));store.migrate();store.check()
            instance='dds-fixture-example'
            rows=[record(ms=120),record(ms=65000,command={'find':'messages_1','filter':{'x':43}})]
            m=store.publish(instance,T,T+5*MINUTE,rows,['messages'])
            store.publish(instance,T,T+5*MINUTE,rows,['messages'])
            values,cov=store.read(instance,T,T+5*MINUTE)
            self.assertTrue(cov['complete']);self.assertEqual(sum(r['count'] for r in values),2)
            from app.mongo_insight import summarize
            group=next(iter(summarize(values).values()))
            import json
            self.assertEqual(json.loads(group['sample'])['duration_us'],65_000_000)
            point=dict(timestamp=T//1000,role='Primary',node='fixture-node',metric='CPUUtilization',period=60,value=42)
            store.telemetry(instance,'metrics',[point]);store.telemetry(instance,'metrics',[point])
            self.assertEqual(len(store.read_telemetry(instance,'metrics',T,T+MINUTE,metric='CPUUtilization')),1)
            native=dict(timestamp=T//1000,node='fixture-node',role='Primary',interval={'status':'ok','commands':{'find':{'total':2}}},global_lock={'currentQueue':{'total':3,'readers':1,'writers':2}})
            store.telemetry(instance,'native',[native])
            compact=store.read_telemetry(instance,'native',T,T+MINUTE,compact=True)[0]
            self.assertEqual(compact['interval']['commands']['find']['total'],2)
            self.assertEqual(compact['global_lock']['currentQueue'],native['global_lock']['currentQueue'])
            store.telemetry(instance,'native',[{**native,'timestamp':(T+MINUTE)//1000,'global_lock':None}])
            self.assertIsNone(store.read_telemetry(instance,'native',T+MINUTE,T+MINUTE,compact=True)[0]['global_lock'])
            self.assertEqual(len(store.latest_native(instance,T+MINUTE)),1)
            store.client.query('TRUNCATE TABLE mongo_ci_fixture.mongo_rollups')
            self.assertFalse(store.read(instance,T,T+5*MINUTE)[1]['complete'])
            store.publish(instance,T,T+5*MINUTE,rows,['messages'])
            self.assertTrue(store.read(instance,T,T+5*MINUTE)[1]['complete'])

if __name__=='__main__':unittest.main()
