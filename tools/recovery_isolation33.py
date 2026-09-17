"""Credential-free CI recovery experiment. Never import/run this on a service host."""
from __future__ import annotations
import hashlib,json,os,struct,subprocess,time,uuid,zlib
from datetime import datetime,timezone
from pathlib import Path

APP='ghcr.io/lop-spec/rds-binlog-insight@sha256:e33906e5a6732994192cbf073bb39bb424feb6f37ce3fd4db6dab3577dfa0698'
WORKER='ghcr.io/lop-spec/rds-binlog-insight@sha256:ee9c175e1c0836d2dd584fbdfe592f432a11315fe32f3817c3f5381ed5b6ce80'
CH='ghcr.io/lop-spec/rds-binlog-insight@sha256:69edd18f624dd310172de17babef4131a35f18e6cbeceabd918d34de80d9b407'
MYSQL='mysql:8.0'
SCOPE='sql-insight-recovery-ci'
INSTANCE='rm-test000001'


def run(args,timeout=30,input=None):
    r=subprocess.run(args,input=input,capture_output=True,text=True,timeout=timeout)
    if r.returncode:raise RuntimeError(f'{args[:3]} exit {r.returncode}: {r.stderr[-3000:]} {r.stdout[-1000:]}')
    return r.stdout

def inspect(name):return json.loads(run(['docker','inspect',name],10))[0]

def write_json(path,value):
    raw=json.dumps(value,indent=2,ensure_ascii=False).encode()
    with Path(path).open('xb') as f:f.write(raw)
    with Path(str(path)+'.sha256').open('x') as f:f.write(hashlib.sha256(raw).hexdigest()+'  '+Path(path).name+'\n')
    assert Path(path).read_bytes()==raw
    return hashlib.sha256(raw).hexdigest()

def require_ci():
    assert os.environ.get('GITHUB_ACTIONS')=='true','isolated CI only'
    assert os.name=='posix' and Path('/var/run/docker.sock').exists(),'CI Docker required'

def remove_owned(name):
    c=inspect(name);assert c['Config']['Labels'].get('scope')==SCOPE
    if c['State']['Running']:run(['docker','stop','--time','10',name],20)
    run(['docker','rm',name],10)

def crc64_xz(raw):
    # Independent fixture oracle (not the application/native CRC implementation).
    table=[]
    for byte in range(256):
        value=byte
        for _ in range(8):value=(value>>1)^(0xc96c5795d7870f42 if value&1 else 0)
        table.append(value)
    value=(1<<64)-1
    for byte in raw:value=table[(value^byte)&255]^(value>>8)
    return value^((1<<64)-1)

def event_identity(file_id,row):
    # Immutable parser emit ABI: file/start/end/1-based emitted ordinal/row/operation.
    fields=[file_id,row['start_position'],row['end_position'],row['emitted_ordinal'],0,row['operation']]
    return hashlib.sha256('\x1f'.join(map(str,fields)).encode()).hexdigest()

def read_query_events(raw):
    """Independent fixture Query/XID reader, including GTID commit time and CRC32."""
    assert raw[:4]==b'\xfebin','binlog magic'
    at=4;rows=[];commit_us=0
    while at<len(raw):
        timestamp,kind,server,size,end,flags=struct.unpack_from('<IBIIIH',raw,at)
        assert size>=23 and at+size<=len(raw),'truncated event'
        event=raw[at:at+size]
        assert zlib.crc32(event[:-4])==struct.unpack('<I',event[-4:])[0],'binlog event CRC32 mismatch'
        if kind in {33,34}:
            body=event[19:-4]
            assert len(body)>=49 and body[25]==2,'fixture GTID logical timestamp missing'
            commit_us=int.from_bytes(body[42:49],'little')&((1<<55)-1)
        if kind==2:
            body=event[19:-4];thread,elapsed,db_len,error,status_len=struct.unpack_from('<IIBHH',body)
            database=body[13+status_len:13+status_len+db_len].decode()
            sql=body[14+status_len+db_len:].decode()
            assert sql=='BEGIN' or sql.startswith('INSERT INTO recovery_rows '),'unexpected fixture SQL'
            rows.append({'start_position':at,'end_position':end,'server_id':server,'thread_id':thread,'header_seconds':timestamp,'commit_epoch_us':commit_us,'database_name':database,'sql_text':sql,'operation':'TRANSACTION' if sql=='BEGIN' else 'INSERT','raw_event_type':'QueryEvent','emitted_ordinal':len(rows)+1})
        elif kind==16:
            xid=struct.unpack_from('<Q',event,19)[0]
            rows.append({'start_position':at,'end_position':end,'server_id':server,'thread_id':0,'header_seconds':timestamp,'commit_epoch_us':commit_us,'database_name':'','sql_text':f'COMMIT /* XID {xid} */','operation':'TRANSACTION','raw_event_type':'XIDEvent','emitted_ordinal':len(rows)+1})
        at+=size
    assert at==len(raw)
    return rows

def prepare(root):
    fixture=root/'fixture';fixture.mkdir();name='recovery-mysql-'+uuid.uuid4().hex[:12]
    run(['docker','pull',MYSQL],240);run(['docker','pull',APP],240)
    run(['docker','run','-d','--name',name,'--label','scope='+SCOPE,'--network','none','--memory','1g','--memory-swap','1g','--cpus','1','-e','MYSQL_ROOT_PASSWORD=fixture-only',MYSQL,'--server-id=77','--log-bin=mysql-bin','--binlog-format=STATEMENT','--binlog-checksum=CRC32'],30)
    def mysql(sql):return run(['docker','exec','-i',name,'mysql','-uroot','-pfixture-only','--batch','--skip-column-names'],15,input=sql)
    try:
        deadline=time.monotonic()+100
        while True:
            try:mysql('SELECT 1;');break
            except RuntimeError:
                assert time.monotonic()<deadline,'fixture MySQL readiness deadline'
                assert inspect(name)['State']['Running'],'fixture MySQL exited'
                time.sleep(2)
        mysql('CREATE DATABASE fixture; CREATE TABLE fixture.recovery_rows (id BIGINT PRIMARY KEY, payload VARCHAR(200)) ENGINE=InnoDB; FLUSH BINARY LOGS;')
        filename=mysql('SHOW MASTER STATUS;').split('\t')[0].strip()
        now=int(time.time())-120
        statements=[f"INSERT INTO recovery_rows VALUES ({i}, 'fixture value {i:04d} unicode 中文')" for i in range(137)]
        mysql('USE fixture; SET TIMESTAMP='+str(now)+'; BEGIN;\n'+';\n'.join(statements)+'; COMMIT;')
        mysql('FLUSH BINARY LOGS;')
        run(['docker','cp',f'{name}:/var/lib/mysql/{filename}',str(fixture/'source.binlog')],15)
        raw=(fixture/'source.binlog').read_bytes();events=read_query_events(raw)
        inserts=[r for r in events if r['sql_text'] in set(statements)]
        assert [r['sql_text'] for r in inserts]==statements,'input SQL vs independent binary oracle mismatch'
        begin=datetime.fromtimestamp(now-10,timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ');end=datetime.fromtimestamp(time.time()+10,timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
        remote={'log_file_name':filename,'host_instance_id':'fixture-host','log_begin_utc':begin,'log_end_utc':end,'file_size':len(raw),'checksum_crc64':'','download_link':'http://fixture-edge:8080/source.binlog','intranet_download_link':'http://fixture-edge:8080/source.binlog','link_expired_utc':'2099-01-01T00:00:00Z','remote_status':'Completed'}
        # Independently implement the documented remote/file identity, using only fixture input.
        stable=hashlib.sha256('\x1f'.join([filename,begin,end,str(len(raw)),'fixture-host']).encode()).hexdigest()
        identity={'file_id':hashlib.sha256((INSTANCE+'\x1f'+stable).encode()).hexdigest(),'crc64':str(crc64_xz(raw))}
        assert crc64_xz(b'123456789')==0x995DC9BBDF1939FA
        write_json(fixture/'remote.json',remote)
        parsed=run(['docker','run','--rm','--network','none','--memory','256m','--memory-swap','256m','--entrypoint','/app/tools/binlog-parser','-v',f'{fixture}:/fixture:ro',APP,'--input','/fixture/source.binlog','--source-file-id',identity['file_id'],'--flavor','mysql'],30)
        (fixture/'native.ndjson').write_text(parsed)
        parsed_rows=[json.loads(l) for l in parsed.splitlines() if l.strip()]
        native_inserts=[r for r in parsed_rows if r.get('sql_text') in set(statements)]
        assert [r['sql_text'] for r in native_inserts]==statements,'native parser lost or changed fixture SQL'
        for actual,expected in zip(parsed_rows,events,strict=True):
            for key in ['start_position','end_position','server_id','database_name','commit_epoch_us','sql_text','operation','raw_event_type']:assert actual[key]==expected[key],(key,actual,expected)
            assert actual['event_id']==event_identity(identity['file_id'],expected),'independent native event identity mismatch'
        assert len({r['commit_epoch_us'] for r in events})==1,'fixture must exercise timestamp ties'
        oracle_rows=[];source_files=[]
        for instance in [INSTANCE,'rm-test000002']:
            file_id=hashlib.sha256((instance+'\x1f'+stable).encode()).hexdigest()
            source_files.append({'id':file_id,'instance_id':instance,'expectedEvents':len(events)})
            for e in events:
                row={k:e[k] for k in ['start_position','end_position','server_id','thread_id','database_name','sql_text','operation','raw_event_type']}
                row.update(instance_id=instance,event_id=event_identity(file_id,e),event_epoch_us=e['commit_epoch_us'],row_index=0,table_name='',before_json='',after_json='',row_query='',host_instance_id='fixture-host',source_file_name=filename)
                oracle_rows.append(row)
        write_json(fixture/'oracle.json',{'fixtureSha256':hashlib.sha256(raw).hexdigest(),'services':['insight','clickhouse','indexer','raw-worker','slowlog-ingester','slowlog-worker'],'files':source_files,'rows':oracle_rows})
        remote['checksum_crc64']=identity['crc64']
        write_json(fixture/'manifest.json',{'identity':{'engine':'mysql','engineVersion':'8.0'},'binlogs':[remote]})
        write_json(fixture/'independent-input.json',{'instance_id':INSTANCE,'file_id':identity['file_id'],'binlogSha256':hashlib.sha256(raw).hexdigest(),'queryEvents':events,'expectedInsertRows':inserts,'nativeCount':len(parsed_rows),'nativeSample':native_inserts[:2]})
        print(json.dumps({'event':'fixturePrepared','binlogBytes':len(raw),'independentInserts':len(inserts),'nativeEvents':len(parsed_rows),'nativeSample':native_inserts[:2]}),flush=True)
        return {'binlogSha256':hashlib.sha256(raw).hexdigest(),'inputInsertRows':len(inserts),'nativeEvents':len(parsed_rows),'image':json.loads(run(['docker','image','inspect',MYSQL],10))[0]['Id']}
    finally:
        (root/'mysql.log').write_text(run(['docker','logs',name],10));remove_owned(name)

def main():
    require_ci();root=Path('recovery-evidence').resolve();root.mkdir()
    proof={'scope':'real fixture and process-recovery experiment; full verdict requires query/archive oracle','sourceSha':os.environ['GITHUB_SHA'],'gate':{'fixturePrepared':False,'processRecovery':False,'wholeStackRecovery':False},'drills':[]}
    try:
        proof['fixture']=prepare(root);proof['gate']['fixturePrepared']=True
        from tools.recovery_fixture33.stack import exercise
        exercise(root,root/'fixture',proof['drills']);proof['gate']['processRecovery']=True
    except Exception as exc:proof['failure']=str(exc);raise
    finally:write_json(root/'recovery-isolated33.json',proof)

if __name__=='__main__':main()
