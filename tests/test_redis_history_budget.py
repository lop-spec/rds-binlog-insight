import json
import tempfile
import unittest
from pathlib import Path
from app.redis_protocol import load_instances


class RedisHistoryBudget(unittest.TestCase):
    def test_operator_budget_is_not_capped_at_four_gib(self):
        with tempfile.TemporaryDirectory() as root:
            p=Path(root)/'redis-instances.json'
            entry=dict(instanceId='r-fixture01',region='cn-hangzhou',host='r-fixture01.redis.cn-hangzhou.rds.aliyuncs.com',
                       port=6379,username='fixture_ro',credentialsFile='.credentials/redis.json',enabled=True,maxMiB=16384)
            p.write_text(json.dumps([entry]));p.chmod(0o600)
            self.assertEqual(load_instances(root)[0]['maxMiB'],16384)
            for value in [True,63,-1,1.5,'16384']:
                p.write_text(json.dumps([{**entry,'maxMiB':value}]))
                with self.assertRaises(ValueError):load_instances(root)
