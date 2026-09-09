import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

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

    def test_missing_configuration_does_not_change_mysql(self):
        with tempfile.TemporaryDirectory() as td:
            service=MongoService(Path(td),lambda:None,start=False)
            self.assertEqual(service.status()['status'],'not_configured')
            service.shutdown()

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
