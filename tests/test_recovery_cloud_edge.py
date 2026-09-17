"""HTTP protocol tests; the durable Linux fsync path is exercised only by CI."""
import json,tempfile,threading,unittest,urllib.request,urllib.error
from pathlib import Path
from unittest.mock import patch
from xml.etree.ElementTree import fromstring
from urllib.parse import urlencode
from tools.recovery_fixture33 import cloud_edge as edge

class CloudEdgeTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name)
        def atomic(path,raw):path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)
        self.patches=[patch.object(edge,'ROOT',self.root),patch.object(edge,'atomic',atomic)]
        for p in self.patches:p.start()
        self.server=edge.ThreadingHTTPServer(('127.0.0.1',0),edge.Handler)
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True);self.thread.start()
        self.url='http://127.0.0.1:'+str(self.server.server_port)
    def tearDown(self):
        self.server.shutdown();self.server.server_close();self.thread.join(2)
        for p in reversed(self.patches):p.stop()
        self.tmp.cleanup()
    def request(self,path,method='GET',data=None,headers=None):
        with urllib.request.urlopen(urllib.request.Request(self.url+path,data=data,method=method,headers=headers or {}),timeout=3) as r:return r.status,dict(r.headers),r.read()
    def test_crc_head_range_and_immutable_put(self):
        data=b'123456789';self.request('/a','PUT',data,{'x-oss-meta-sha256':'test-sha','x-oss-forbid-overwrite':'true'})
        _,h,body=self.request('/a','HEAD');self.assertEqual(body,b'');self.assertEqual(h['Content-Length'],'9');self.assertEqual(h['x-oss-hash-crc64ecma'],str(0x995DC9BBDF1939FA))
        code,h,body=self.request('/a',headers={'Range':'bytes=2-5'});self.assertEqual((code,body),(206,b'3456'));self.assertEqual(h['Content-Range'],'bytes 2-5/9')
        with self.assertRaises(urllib.error.HTTPError) as caught:self.request('/a','PUT',b'wrong',{'x-oss-forbid-overwrite':'true'})
        self.assertEqual(caught.exception.code,409);self.assertEqual(self.request('/a')[2],data)
    def test_lifecycle_and_list_pagination(self):
        xml=b'<LifecycleConfiguration><Rule><ID>fixture</ID></Rule></LifecycleConfiguration>'
        self.request('/?lifecycle','PUT',xml);self.assertEqual(self.request('/?lifecycle')[2],xml)
        for key in ['p/a','p/b','p/c']:self.request('/'+key,'PUT',key.encode())
        seen=[];token=''
        for _ in range(3):
            _,_,body=self.request('/?'+urlencode({'list-type':'2','prefix':'p/','max-keys':1,'continuation-token':token}))
            value=fromstring(body);seen.extend(x.text for x in value.findall('Contents/Key'));token=value.findtext('NextContinuationToken','')
        self.assertEqual(seen,['p/a','p/b','p/c']);self.assertEqual(token,'');self.assertEqual(value.findtext('IsTruncated'),'false')
