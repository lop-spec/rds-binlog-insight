import unittest
from app.instance_registry import instance_registry

class InstanceRegistryTests(unittest.TestCase):
    def test_configured_starrocks_is_registered_without_events(self):
        entries=[{'instanceId':'c-example01','label':'example-sr','enabled':True,'credentialsFile':'private.json','host':'private','password':'private'}]
        rows=instance_registry(['rm-example01','dds-example01'],entries)
        sr=next(x for x in rows if x['id']=='c-example01')
        self.assertEqual(sr['engine'],'starrocks');self.assertEqual(sr['label'],'example-sr')
        self.assertEqual(sr['readMode'],'source_readonly');self.assertFalse(sr['rowChanges'])
        self.assertIn('audit',sr['capabilities']);self.assertNotIn('host',sr);self.assertNotIn('credentialsFile',sr)
    def test_disabled_cluster_is_not_registered(self):
        self.assertEqual(instance_registry([], [{'instanceId':'c-example01','enabled':False}]),[])
    def test_engine_specific_ids_are_preserved_and_deduplicated(self):
        rows=instance_registry(['c-example01','rm-example01','rm-example01','mongodb-test'],[{'instanceId':'c-example01','enabled':True}])
        self.assertEqual(len(rows),3)
        self.assertEqual(next(x for x in rows if x['id']=='mongodb-test')['engine'],'mongodb')
        self.assertEqual(next(x for x in rows if x['id']=='c-example01')['engine'],'starrocks')

if __name__=='__main__':unittest.main()
