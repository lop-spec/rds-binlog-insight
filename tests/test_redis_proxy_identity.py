import json,tempfile,unittest
from pathlib import Path
from app.redis_service import RedisService
from tests.test_redis_insight import ENTRY

class ProxyIdentity(unittest.TestCase):
    def test_proxy_zero_based_index_is_not_sparse_cloud_node_suffix(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);p=root/'redis-instances.json';p.write_text(json.dumps([ENTRY]));p.chmod(0o600)
            def cloud(e,action,params,das):
                if action=='DescribeAccounts':return {'Accounts':{'Account':[{'AccountName':e['username'],'AccountType':'Normal','AccountStatus':'Available','DatabasePrivileges':{'DatabasePrivilege':[{'AccountPrivilege':'RoleReadOnly'}]}}]}}
                if action=='DescribeLogicInstanceTopology':return {'RedisShardList':{'NodeInfo':[{'NodeType':'db','NodeId':e['instanceId']+'-db-1#101'},{'NodeType':'db','NodeId':e['instanceId']+'-db-11#102'}]}}
                raise AssertionError(action)
            s=RedisService(root,lambda:None,lambda _:None,cloud_factory=cloud)
            nodes=s.topology(ENTRY)
            self.assertEqual([n['index'] for n in nodes],[0,1])
            self.assertEqual([n['node'] for n in nodes],['db-0','db-1'])
            self.assertTrue(all(n['cloud_node_id'] is None and ':proxy-index:' in n['node_id'] for n in nodes))
            self.assertEqual([n['node'] for n in s.cloud_meta[ENTRY['instanceId']]],['cloud-db-1','cloud-db-11'])
            self.assertEqual(s.cloud_meta[ENTRY['instanceId']][1]['node_id'],ENTRY['instanceId']+'-db-11#102')
