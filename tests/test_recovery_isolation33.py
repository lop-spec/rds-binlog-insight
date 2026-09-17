import os,struct,unittest,zlib
from unittest.mock import patch
from tools.recovery_isolation33 import read_query_events,require_ci

class RecoveryFixtureTests(unittest.TestCase):
    def sample(self):
        payload=struct.pack('<IIBHH',11,0,7,0,0)+b'fixture\0INSERT INTO recovery_rows VALUES (1, \'x\')'
        size=19+len(payload)+4;body=struct.pack('<IBIIIH',1700000000,2,77,size,4+size,0)+payload
        return b'\xfebin'+body+struct.pack('<I',zlib.crc32(body))
    def test_independent_query_oracle(self):
        rows=read_query_events(self.sample());self.assertEqual(rows[0]['database_name'],'fixture');self.assertEqual(rows[0]['start_position'],4);self.assertEqual(rows[0]['thread_id'],11)
    def test_corrupt_or_truncated_binlog_fails(self):
        raw=self.sample()
        for invalid in [raw[:-1],raw[:-1]+bytes([raw[-1]^1]),b'bad!'+raw[4:]]:
            with self.assertRaises(AssertionError):read_query_events(invalid)
    def test_local_execution_rejected_before_docker(self):
        with patch.dict(os.environ,{'GITHUB_ACTIONS':'false'}),patch('tools.recovery_isolation33.run') as run:
            with self.assertRaises(AssertionError):require_ci()
            run.assert_not_called()

if __name__=='__main__':unittest.main()
