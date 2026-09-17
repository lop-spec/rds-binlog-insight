"""Isolated, persistent OSS HTTP(S) boundary. No production/cloud SDK credentials."""
import base64,hashlib,json,os,re,ssl,threading,time,uuid
from datetime import datetime,timezone
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit,unquote,parse_qs
from xml.etree.ElementTree import Element,SubElement,tostring,fromstring
from tools.recovery_isolation33 import crc64_xz
ROOT=Path('/edge');LOCK=threading.Lock()

def journal(event,**values):
    raw=(json.dumps({'event':event,'atNs':time.time_ns(),**values})+'\n').encode()
    with LOCK:
        with (ROOT/'journal.jsonl').open('ab') as f:f.write(raw);f.flush();os.fsync(f.fileno())

def atomic(path,raw):
    tmp=path.with_name('.'+uuid.uuid4().hex);tmp.parent.mkdir(parents=True,exist_ok=True)
    with tmp.open('xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
    os.replace(tmp,path)
    sync_directory(path.parent)

def sync_directory(path):
    fd=os.open(path,os.O_DIRECTORY)
    try:os.fsync(fd)
    finally:os.close(fd)

def delete_object(key):
    assert key and all(x not in {'.','..',''} for x in key.split('/')),'invalid delete key'
    assert '\\' not in key and not key.startswith('/')
    existed=False
    for path in [ROOT/'objects'/key,ROOT/'headers'/(key+'.json')]:
        if path.exists():path.unlink();sync_directory(path.parent);existed=True
    journal('delete_durable',key=key,existed=existed)

def subresource(query,name):
    return parse_qs(query,keep_blank_values=True)=={name:['']}

class Handler(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    def log_message(self,*args):pass
    def log_error(self,format,*args):journal('http_protocol_error',method=self.command,path=urlsplit(self.path).path,reason=format%args)
    def path_info(self):
        u=urlsplit(self.path);key=unquote(u.path).lstrip('/')
        if any(x in {'.','..',''} for x in key.split('/')) and key:raise ValueError('invalid object path')
        return key,u.query
    def send(self,code,body=b'',headers=None,length=None):
        if code>=400:journal('http_error',method=self.command,path=urlsplit(self.path).path,queryFields=sorted(parse_qs(urlsplit(self.path).query,keep_blank_values=True)),status=code,reason=body.decode(errors='replace')[:300])
        self.send_response(code);self.send_header('Content-Length',str(len(body) if length is None else length));self.send_header('x-oss-request-id','fixture-'+uuid.uuid4().hex)
        for k,v in (headers or {}).items():self.send_header(k,str(v))
        self.end_headers()
        if self.command!='HEAD':self.wfile.write(body)
    def do_PUT(self):
        key,query=self.path_info();size=int(self.headers.get('Content-Length','0'));assert 0<=size<=64*1024**2
        body=self.rfile.read(size);assert len(body)==size
        if subresource(query,'lifecycle'):atomic(ROOT/'lifecycle.xml',body);journal('lifecycle_put');self.send(200);return
        assert key
        if self.headers.get('x-oss-forbid-overwrite')=='true' and (ROOT/'objects'/key).exists():
            self.send(409,b'<Error><Code>FileAlreadyExists</Code><Message>immutable fixture object</Message></Error>');return
        sha=hashlib.sha256(body).hexdigest();crc=crc64_xz(body);etag='"'+hashlib.md5(body).hexdigest()+'"'
        headers={k.lower():v for k,v in self.headers.items() if k.lower().startswith('x-oss-meta-')}
        headers.update({'ETag':etag,'x-oss-hash-crc64ecma':str(crc),'Content-Type':'application/octet-stream'})
        atomic(ROOT/'objects'/key,body);atomic(ROOT/'headers'/(key+'.json'),json.dumps(headers).encode())
        journal('put_durable',key=key,sha256=sha,crc64=str(crc),bytes=size)
        self.send(200,headers=headers)
    def do_DELETE(self):
        key,query=self.path_info();assert not query
        delete_object(key);self.send(204)
    def do_POST(self):
        key,query=self.path_info()
        if not subresource(query,'delete'):self.send(501,b'unsupported fixture POST');return
        size=int(self.headers.get('Content-Length','0'));assert 0<size<=1024**2
        raw=self.rfile.read(size);assert len(raw)==size
        request=fromstring(raw);objects=request.findall('{*}Object');assert 0<len(objects)<=1000
        result=Element('DeleteResult',xmlns='http://s3.amazonaws.com/doc/2006-03-01/')
        for item in objects:
            name=item.findtext('{*}Key');delete_object(name)
            if request.findtext('{*}Quiet')!='true':SubElement(SubElement(result,'Deleted'),'Key').text=name
        self.send(200,tostring(result),{'Content-Type':'application/xml'})
    def do_HEAD(self):self.do_GET()
    def do_GET(self):
        key,query=self.path_info()
        if subresource(query,'lifecycle'):
            journal('lifecycle_get');p=ROOT/'lifecycle.xml';self.send(200,p.read_bytes() if p.exists() else b'<LifecycleConfiguration/>',{'Content-Type':'application/xml'});return
        params=parse_qs(query)
        if params.get('list-type')==['2']:
            prefix=params.get('prefix',[''])[0];limit=int(params.get('max-keys',['1000'])[0]);assert 1<=limit<=1000
            token=params.get('continuation-token',[''])[0];after=base64.urlsafe_b64decode(token).decode() if token else ''
            result=Element('ListBucketResult')
            objects=sorted(p for p in (ROOT/'objects').rglob('*') if p.is_file() and p.relative_to(ROOT/'objects').as_posix().startswith(prefix) and p.relative_to(ROOT/'objects').as_posix()>after) if (ROOT/'objects').exists() else []
            for k,v in [('Name','test-fixture-bucket'),('Prefix',prefix),('MaxKeys',str(limit)),('IsTruncated',str(len(objects)>limit).lower())]:SubElement(result,k).text=v
            for p in objects[:limit]:
                item=SubElement(result,'Contents');SubElement(item,'Key').text=p.relative_to(ROOT/'objects').as_posix();SubElement(item,'Size').text=str(p.stat().st_size);SubElement(item,'LastModified').text=datetime.fromtimestamp(p.stat().st_mtime,timezone.utc).strftime('%Y-%m-%dT%H:%M:%S.000Z');SubElement(item,'ETag').text='"'+hashlib.md5(p.read_bytes()).hexdigest()+'"'
                SubElement(item,'Type').text='Normal';SubElement(item,'StorageClass').text='Standard'
            if len(objects)>limit:SubElement(result,'NextContinuationToken').text=base64.urlsafe_b64encode(objects[limit-1].relative_to(ROOT/'objects').as_posix().encode()).decode()
            self.send(200,tostring(result),{'Content-Type':'application/xml'});return
        if key=='healthz':self.send(200,b'OK');return
        if key=='source.binlog':p=Path('/fixture/source.binlog');headers={'Content-Type':'application/octet-stream'}
        else:
            p=ROOT/'objects'/key;h=ROOT/'headers'/(key+'.json')
            if not p.is_file() or not h.is_file():self.send(404,b'<Error><Code>NoSuchKey</Code><Message>fixture missing</Message></Error>');return
            headers=json.loads(h.read_text())
        body=p.read_bytes();total=len(body);code=200
        if self.command=='HEAD':journal('head',key=key,bytes=total);self.send(200,headers=headers,length=total);return
        r=self.headers.get('Range')
        if r:
            match=re.fullmatch(r'bytes=(\d*)-(\d*)',r);assert match and any(match.groups()),'invalid Range'
            lo=int(match[1]) if match[1] else max(total-int(match[2]),0)
            hi=min(int(match[2]),total-1) if match[1] and match[2] else total-1
            if lo>=total:self.send(416,headers={'Content-Range':f'bytes */{total}'});return
            body=body[lo:hi+1];headers['Content-Range']=f'bytes {lo}-{hi}/{total}';code=206
        journal('get',key=key,bytes=len(body),range=r);self.send(code,body,headers)

if __name__=='__main__':
    guard=json.loads(Path('/fixture/guard.json').read_text());assert guard['scope']=='sql-insight-recovery-ci'
    ROOT.mkdir(exist_ok=True);http=ThreadingHTTPServer(('0.0.0.0',8080),Handler);tls=ThreadingHTTPServer(('0.0.0.0',443),Handler)
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain('/fixture/cert.pem','/fixture/key.pem');tls.socket=context.wrap_socket(tls.socket,server_side=True)
    threading.Thread(target=http.serve_forever,daemon=True).start();tls.serve_forever()
