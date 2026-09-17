import os,struct,unittest,zlib
from unittest.mock import patch
from tools.recovery_isolation33 import read_query_events,require_ci,crc64_xz,event_identity

class RecoveryFixtureTests(unittest.TestCase):
    def sample(self):
        payload=struct.pack('<IIBHH',11,0,7,0,0)+b'fixture\0INSERT INTO recovery_rows VALUES (1, \'x\')'
        size=19+len(payload)+4;body=struct.pack('<IBIIIH',1700000000,2,77,size,4+size,0)+payload
        return b'\xfebin'+body+struct.pack('<I',zlib.crc32(body))
    def test_independent_query_oracle(self):
        rows=read_query_events(self.sample());self.assertEqual(rows[0]['database_name'],'fixture');self.assertEqual(rows[0]['start_position'],4);self.assertEqual(rows[0]['thread_id'],11)
    def test_xid_inherits_its_transaction_connection_not_zero_or_previous(self):
        raw=bytearray(b'\xfebin')
        def event(kind,payload):
            size=19+len(payload)+4
            body=struct.pack('<IBIIIH',1700000000,kind,77,size,len(raw)+size,0)+payload
            raw.extend(body+struct.pack('<I',zlib.crc32(body)))
        for thread,xid in [(11,18),(29,19)]:
            for sql in ['BEGIN',"INSERT INTO recovery_rows VALUES (1, 'x')"]:
                event(2,struct.pack('<IIBHH',thread,0,7,0,0)+b'fixture\0'+sql.encode())
            event(16,struct.pack('<Q',xid))
        rows=read_query_events(raw)
        self.assertEqual([r['thread_id'] for r in rows],[11,11,11,29,29,29])
        self.assertEqual([r['sql_text'] for r in rows if r['raw_event_type']=='XIDEvent'],['COMMIT /* XID 18 */','COMMIT /* XID 19 */'])
    def test_corrupt_or_truncated_binlog_fails(self):
        raw=self.sample()
        for invalid in [raw[:-1],raw[:-1]+bytes([raw[-1]^1]),b'bad!'+raw[4:]]:
            with self.assertRaises(AssertionError):read_query_events(invalid)
    def test_crc_and_native_identity_independent_vectors(self):
        self.assertEqual(crc64_xz(b'123456789'),0x995DC9BBDF1939FA)
        row={'start_position':324,'end_position':480,'emitted_ordinal':2,'operation':'INSERT'}
        self.assertEqual(event_identity('e9c5eea00d4b767d96890b5a4fec0b91cc255b26170877a68769a2f9d9472479',row),'1444629e474bacd5021c224f3d23b3f99ced20e78ddb718fc71a743fec120cdc')
    def test_local_execution_rejected_before_docker(self):
        with patch.dict(os.environ,{'GITHUB_ACTIONS':'false'}),patch('tools.recovery_isolation33.run') as run:
            with self.assertRaises(AssertionError):require_ci()
            run.assert_not_called()

if __name__=='__main__':unittest.main()
