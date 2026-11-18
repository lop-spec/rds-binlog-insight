import copy
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch, Mock

from oss2.utils import Crc64
from app import raw_frames as fr
from app.binlog_lite import RawBinlogError
from app.raw_binlog import RawBinlogStore
from app.raw_binlog_query import Budget, copy_range
from app.metadata import MetadataStore
from app.config import Settings
from app.rds_api import RemoteBinlog


def crc(data):
    c = Crc64(); c(data); return str(c.crc)


class Store:
    prefix = 'mysql-binlog/test/'
    def __init__(self):
        self.bucket = self
        self.objects = {}
        self.gets = []
    def _head_verified(self, key, desc):
        if key not in self.objects: return None
        assert len(self.objects[key]) == desc['size_bytes']
        assert hashlib.sha256(self.objects[key]).hexdigest() == desc['sha256']
        return {'oss_key': key}
    def head_object(self, key):
        return SimpleNamespace(headers={'x-oss-hash-crc64ecma': crc(self.objects[key])})
    def _put_object_from_file_verified(self, key, path, headers, **kwargs):
        assert key not in self.objects and headers['x-oss-forbid-overwrite'] == 'true'
        self.objects[key] = path.read_bytes()
        return kwargs['verify']()
    def get_object(self, key, byte_range):
        a,b = byte_range; data = self.objects[key]; self.gets.append((key,a,b))
        r = io.BytesIO(data[a:b+1]); r.headers = {'Content-Range': f'bytes {a}-{b}/{len(data)}'}; return r


class FrameTest(unittest.TestCase):
    def setUp(self):
        d = tempfile.TemporaryDirectory(); self.addCleanup(d.cleanup)
        self.root = Path(d.name); self.meta = MetadataStore(self.root/'meta.db'); self.db = RawBinlogStore(self.meta)
        self.oss = Store(); self.plain = bytes(range(251))*13
        self.path = self.root/'fixture.binlog'; self.path.write_bytes(self.plain)
        self.item = RemoteBinlog(log_file_name='mysql-bin.000001',log_begin_utc='2026-01-01T00:00:00Z',log_end_utc='2026-01-01T00:00:01Z',file_size=len(self.plain),checksum_crc64=crc(self.plain),download_link='',intranet_download_link='',link_expired_utc='',remote_status='Completed',host_instance_id='node')
        self.fid,_ = self.meta.upsert_remote(Settings(db_instance_id='test'),self.item)
    def write(self, framed=True):
        with patch.dict('os.environ',{'RDS_BINLOG_WRITE_FRAMED_ZSTD':str(int(framed))}), patch.object(fr,'BLOCK',512), patch('app.raw_binlog.scan',return_value=(b'{"regions":[]}',dict(lo=1,hi=2,events=3))), patch('app.parser_bridge.checksum_file',return_value=SimpleNamespace(sha256=hashlib.sha256(self.plain).hexdigest(),crc64=crc(self.plain))):
            return self.db.archive(self.oss,self.path,self.fid,self.item,'mysql')
    def test_single_payload_and_recover_without_sqlite(self):
        d = self.write(); self.db.verify(self.oss,d)
        self.assertEqual(len(self.oss.objects),3)
        self.assertFalse(any(k.endswith('.binlog') for k in self.oss.objects))
        m = json.loads(self.oss.objects[d['recovery']['oss_key']]); r=m['raw']
        self.assertEqual(m['file_id'],self.fid)
        self.assertEqual(b''.join(fr.chunks(self.oss,r,0,r['size_bytes'])),self.plain)
        self.assertEqual(self.write(),d)
        from app.restore_frames import restore
        handles=[]
        for obj in [d['recovery'],r['stored'],m['index']]:
            path=self.root/Path(obj['oss_key']).name
            path.write_bytes(self.oss.objects[obj['oss_key']]); handles.append(path)
        dest=self.root/'out'
        self.assertEqual(restore(*handles,dest),len(self.plain))
        self.assertEqual((dest/'source.binlog').read_bytes(),self.plain)
        self.assertTrue((dest/'READY').is_file())
        with self.assertRaises(FileExistsError): restore(*handles,dest)
        handles[2].write_bytes(b'corrupted'); other=self.root/'other'
        with self.assertRaises(RawBinlogError): restore(*handles,other)
        self.assertFalse(other.exists())
    def test_cross_frame_partial(self):
        r=self.write()['raw']; out=io.BytesIO(); b=Budget(); self.addCleanup(b.close)
        copy_range(self.oss,r,1010,1040,out,b)
        self.assertEqual(out.getvalue(),self.plain[1010:1040]); self.assertEqual(b.requests,2)
    def test_legacy_read_and_retry_after_switch(self):
        d=self.write(False); self.assertEqual(self.write(True),d)
        out=io.BytesIO(); b=Budget(); self.addCleanup(b.close)
        copy_range(self.oss,d['raw'],5,20,out,b); self.assertEqual(out.getvalue(),self.plain[5:20])
    def test_corrupt_manifest_and_frame_fail_closed(self):
        raw=self.write()['raw']; broken=copy.deepcopy(raw); broken['frames'][0][0]=5
        with self.assertRaises(RawBinlogError): list(fr.chunks(self.oss,broken,0,10))
        raw['frames'][0][2]='0'*64
        with self.assertRaises(RawBinlogError): list(fr.chunks(self.oss,raw,0,10))
    def test_failed_catalog_upload_does_not_publish(self):
        original=self.oss._put_object_from_file_verified
        def fault(k,*a,**kw):
            if k.endswith('.manifest.json'): raise OSError('fixture')
            return original(k,*a,**kw)
        with patch.object(self.oss,'_put_object_from_file_verified',side_effect=fault), self.assertRaises(OSError): self.write()
        self.assertIsNone(self.db.get(self.fid)); self.assertEqual(self.path.read_bytes(),self.plain)
        self.assertIn('recovery',self.write())
