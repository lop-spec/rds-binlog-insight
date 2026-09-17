"""Real six-service fixture graph; immutable images, no production connection."""
import hashlib,json,os,shutil,sqlite3,subprocess,time,urllib.request,uuid
from pathlib import Path
from tools.recovery_isolation33 import APP,WORKER,CH,MYSQL,SCOPE,INSTANCE,run,inspect,remove_owned,write_json,crc64_xz
TOOLS=Path(__file__).resolve().parents[1]
SPEC=json.loads((Path(__file__).parent/'runtime.json').read_text())['services']
STATUS={'indexer':'index/index-worker-status.json','slowlog-worker':'index/slowlog-worker-status.json','slowlog-ingester':'logs/clickhouse-slowlog-worker-status.json','raw-worker':'logs/clickhouse-raw-oss-worker-status.json'}

def api(url,payload=None):
    request=urllib.request.Request(url,data=json.dumps(payload).encode() if payload is not None else None,headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(request,timeout=3) as r:return json.load(r)

def wait_for(check,seconds,label):
    end=time.monotonic()+seconds;last=None;reported=0
    while True:
        try:
            result=check()
            if result:return result
        except (OSError,RuntimeError,KeyError,ValueError,sqlite3.Error) as exc:last=str(exc)
        if time.monotonic()>end:raise TimeoutError(f'{label}: {last}')
        if time.monotonic()-reported>10:print(json.dumps({'waiting':label,'lastError':last}),flush=True);reported=time.monotonic()
        time.sleep(.5)

def db_rows(path,sql):
    c=sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=1);c.row_factory=sqlite3.Row
    try:return [dict(r) for r in c.execute(sql)]
    finally:c.close()

class Stack:
    def __init__(self,root,fixture,stage):
        self.root=root/stage;self.root.mkdir();self.stage=stage;self.fixture=fixture
        self.net='recovery-'+uuid.uuid4().hex[:12];self.slice=self.net+'.slice';self.names={};self.created=[];self.started=0
        for p in ['data','control','edge','ch-data','ch-logs']: (self.root/p).mkdir(mode=0o777)
        os.chmod(self.root/'control',0o777)
        run(['sudo','chown','1003:1003',str(self.root/'data'),str(self.root/'control')])
        write_json(self.root/'control/fault.json',{'stage':stage})
    def common(self):
        return {'RDS_BINLOG_DATA_DIR':'/data','RDS_BINLOG_STAGING_DIR':'/data/staging','RDS_BINLOG_TEST_MODE':'1','RDS_BINLOG_MOCK_MANIFEST':'/fixture/manifest.json','RDS_RECOVERY_FIXTURE':'1','RDS_BINLOG_CLOUD_AUTH_MODE':'access_key','ALIBABA_CLOUD_ACCESS_KEY_ID':'test-ak','ALIBABA_CLOUD_ACCESS_KEY_SECRET':'test-secret','RDS_BINLOG_CLICKHOUSE_HOST':'clickhouse','RDS_BINLOG_GLOG_HOST':'fixture-mysql','RDS_BINLOG_GLOG_USER':'root','RDS_BINLOG_GLOG_PASSWORD':'fixture-only','RDS_BINLOG_GLOG_INSTANCE_ID':INSTANCE,'REQUESTS_CA_BUNDLE':'/fixture/cert.pem','SSL_CERT_FILE':'/fixture/cert.pem','PYTHONPATH':'/harness:/harness/tools/recovery_fixture33:/app','CLICKHOUSE_USER':'query','CLICKHOUSE_PASSWORD':'fixture-only'}
    def environment(self,values):return [arg for k,v in values.items() for arg in ['-e',k+'='+str(v)]]
    def mounts(self):return ['-v',f'{TOOLS}:/harness/tools:ro','-v',f'{self.fixture}:/fixture:ro','-v',f'{self.root}/control:/control','-v',f'{self.root}/data:/data']
    def create(self,role,image,options,command):
        name=self.net+'-'+role
        run(['docker','run','-d','--name',name,'--label','scope='+SCOPE,'--label','run='+os.environ['GITHUB_RUN_ID'],'--network',self.net,*options,image,*command],30)
        self.created.append(name);return name
    def init_container(self,command):
        env=self.common();env.pop('RDS_RECOVERY_FIXTURE');env.update({'RDS_BINLOG_CLICKHOUSE_ENABLED':'1','RDS_BINLOG_CLICKHOUSE_RAW_OSS_ENABLED':'1','CLICKHOUSE_USER':'fixture-admin'})
        return run(['docker','run','--rm','--network',self.net,'--user','1003:1003',*self.environment(env),*self.mounts(),APP,*command],120)
    def boot(self):
        run(['docker','network','create','--internal','--label','scope='+SCOPE,self.net])
        run(['sudo','systemctl','set-property','--runtime',self.slice,'MemoryHigh=12G','MemoryMax=13G','CPUQuota=330%','IOAccounting=yes'])
        run(['sudo','systemctl','start',self.slice])
        group=run(['systemctl','show',self.slice,'--property=ControlGroup','--value']).strip()
        assert group.startswith('/') and group!='/'
        self.cgroup=Path('/sys/fs/cgroup')/group.lstrip('/')
        device=os.stat(run(['docker','info','--format','{{.DockerRootDir}}']).strip()).st_dev
        assert os.major(device)>0,'real backing device required for I/O cap'
        run(['sudo','tee',str(self.cgroup/'io.max')],input=f'{os.major(device)}:{os.minor(device)} rbps=67108864 wbps=33554432\n')
        write_json(self.root/'parent-limits.json',{k:(self.cgroup/k).read_text().strip() for k in ['memory.high','memory.max','cpu.max','io.max']})
        edge=self.create('edge',APP,['--network-alias','fixture-edge','--network-alias','test-fixture-bucket.oss-cn-hangzhou-internal.aliyuncs.com','--memory','256m','--memory-swap','256m','-e','PYTHONPATH=/harness','-v',f'{TOOLS}:/harness/tools:ro','-v',f'{self.fixture}:/fixture:ro','-v',f'{self.root}/edge:/edge'],['python','-m','tools.recovery_fixture33.cloud_edge'])
        mysql=self.create('mysql',MYSQL,['--network-alias','fixture-mysql','--memory','1g','--memory-swap','1g','--cpus','1','-e','MYSQL_ROOT_PASSWORD=fixture-only','-e','MYSQL_ROOT_HOST=%'],[])
        wait_for(lambda:run(['docker','exec',mysql,'mysql','-uroot','-pfixture-only','-e','SELECT 1'],5),100,'fixture MySQL ready')
        for role,spec in SPEC.items():
            if role!='clickhouse':continue
            options=self.options(role,spec)+['--network-alias','clickhouse','--ulimit','nofile=262144:262144','-v',f'{self.root}/ch-data:/var/lib/clickhouse','-v',f'{self.root}/ch-logs:/var/log/clickhouse-server','-v',f'{self.fixture}/server.xml:/etc/clickhouse-server/config.d/zz-recovery.xml:ro','-v',f'{self.fixture}/users.xml:/etc/clickhouse-server/users.d/zz-recovery.xml:ro','-v',f'{self.fixture}:/fixture:ro']
            env={'RDS_BINLOG_CLICKHOUSE_OSS_ENABLED':'1','RDS_BINLOG_CLICKHOUSE_OSS_AUTH_MODE':'access_key','RDS_BINLOG_OSS_CREDENTIAL_FILE':'/fixture/credentials.json','CLICKHOUSE_USER':'fixture-admin','CLICKHOUSE_PASSWORD':'fixture-only','CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT':'1'}
            self.names[role]=self.create(role,CH,options+self.environment(env),[])
        wait_for(lambda:self.ch('SELECT 1'),60,'ClickHouse ready')
        init="from pathlib import Path;from app.metadata import MetadataStore;from app.config import Settings;from app.storage import EventStorage;m=MetadataStore(Path('/data/metadata.sqlite3'));m.save_settings(Settings(db_instance_id='rm-test000001',auto_sync=True,oss_enabled=True,oss_bucket='test-fixture-bucket',oss_region_id='cn-hangzhou',oss_endpoint='https://oss-cn-hangzhou-internal.aliyuncs.com',oss_prefix='fixture/',oss_auth_mode='access_key'));EventStorage(m,Path('/data'));Path('/data/binlog-instances.json').write_text('[{\"instanceId\":\"rm-test000002\",\"label\":\"secondary-fixture\",\"autoSync\":true}]');print('fixture metadata initialized')"
        (self.root/'init.log').write_text(self.init_container(['python','-c',init]))
        (self.root/'migrate.log').write_text(self.init_container(['python','-m','app.clickhouse_migrate','--data-dir','/data','--raw-oss-tables']))
        for role,spec in SPEC.items():
            if role=='clickhouse':continue
            env={**self.common(),**spec['flags']};env['CLICKHOUSE_USER']='query' if role=='insight' else 'ingester'
            if role=='insight':env['RECOVERY_FAULT_ACTOR']='1'
            opts=self.options(role,spec)+self.environment(env)+self.mounts()
            if role=='insight':opts+=['-p','127.0.0.1::8769']
            self.names[role]=self.create(role,APP if role=='insight' else WORKER,opts,spec['command'])
        port=inspect(self.names['insight'])['NetworkSettings']['Ports']['8769/tcp'][0]['HostPort'];self.url='http://127.0.0.1:'+port
        wait_for(lambda:api(self.url+'/healthz'),40,'application ready')
        wait_for(lambda:all((self.root/'data'/path).exists() for path in STATUS.values()),50,'all worker status files')
        return self.snapshot()
    def options(self,role,spec):
        result=['--restart','unless-stopped','--cgroup-parent',self.slice,'--memory',str(spec['memory']),'--memory-swap',str(spec['swap']),'--cpus',str(spec['nanoCpus']/1e9),'--pids-limit',str(spec['pidsLimit']),'--security-opt','no-new-privileges:true']
        if spec['readOnly']:result+=['--read-only']
        if role!='clickhouse':result+=['--user','1003:1003']
        tmp='256m' if role in ['insight','indexer','clickhouse'] else '128m'
        result+=['--tmpfs',f'/tmp:size={tmp},mode=1777']
        if role=='insight':result+=['--tmpfs','/data/staging:size=1g,mode=1777']
        if role in ['raw-worker','slowlog-ingester']:result+=['--tmpfs','/data/scratch:size=256m,mode=1777']
        return result
    def ch(self,sql):return run(['docker','exec',self.names['clickhouse'],'clickhouse-client','--user','fixture-admin','--password','fixture-only','--query',sql],10).strip()
    def snapshot(self):
        result={}
        for role,name in self.names.items():
            c=inspect(name);assert c['Image']==SPEC[role]['imageId'],(role,c['Image'])
            h=c['HostConfig'];result[role]={'scope':c['Config']['Labels']['scope'],'imageDigest':c['Image'],'running':c['State']['Running'],'pid':c['State']['Pid'],'restartCount':c['RestartCount'],'startedAt':c['State']['StartedAt'],'resourceSpec':{k:h.get(k) for k in ['Memory','MemorySwap','NanoCpus','PidsLimit','RestartPolicy','ReadonlyRootfs','Tmpfs','CgroupParent']}}
        return result
    def files(self):return db_rows(self.root/'data/metadata.sqlite3',"SELECT id,state,event_count,raw_deleted_at,error_message FROM binlog_files")
    def recovered(self,before):
        current=self.snapshot()
        if not all(v['running'] and v['restartCount']>before[k]['restartCount'] for k,v in current.items()):return None
        api(self.url+'/healthz');self.ch('SELECT 1')
        for role,path in STATUS.items():
            f=self.root/'data'/path
            if f.stat().st_mtime_ns<self.fault_ns:return None
            value=json.loads(f.read_text())
            if value.get('lastError') or value.get('state') in ['error','failed','starting']:return None
        files=self.files()
        if not files or not all(f['state']=='done' for f in files):return None
        for value in current.values():value['healthy']=True
        return current
    def close(self):
        errors=[]
        for name in reversed(self.created):
            try:
                logs=subprocess.run(['docker','logs','--tail','300',name],capture_output=True,text=True,timeout=10)
                (self.root/(name+'.log')).write_text(logs.stdout+logs.stderr)
                remove_owned(name)
            except Exception as exc:errors.append(str(exc))
        try:run(['docker','network','rm',self.net]);run(['sudo','systemctl','stop',self.slice])
        except Exception as exc:errors.append(str(exc))
        if errors:raise RuntimeError('fixture cleanup failed: '+str(errors))

def create_material(fixture):
    write_json(fixture/'guard.json',{'scope':SCOPE})
    write_json(fixture/'credentials.json',{'access_key_id':'test-ak','access_key_secret':'test-secret','security_token':''})
    run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(fixture/'key.pem'),'-out',str(fixture/'cert.pem'),'-days','2','-subj','/CN=test-fixture-bucket.oss-cn-hangzhou-internal.aliyuncs.com','-addext','subjectAltName=DNS:test-fixture-bucket.oss-cn-hangzhou-internal.aliyuncs.com,DNS:fixture-edge'],10)
    (fixture/'server.xml').write_text('<clickhouse><max_server_memory_usage>2500000000</max_server_memory_usage><max_server_memory_usage_to_ram_ratio>0.8</max_server_memory_usage_to_ram_ratio><memory_worker_correct_memory_tracker>0</memory_worker_correct_memory_tracker><memory_worker_use_cgroup>1</memory_worker_use_cgroup><openSSL><client><caConfig>/fixture/cert.pem</caConfig><verificationMode>strict</verificationMode></client></openSSL></clickhouse>')
    (fixture/'users.xml').write_text('<clickhouse><profiles><query><max_threads>1</max_threads><max_memory_usage>500000000</max_memory_usage><max_execution_time>60</max_execution_time><log_queries>1</log_queries></query><ingester><max_threads>4</max_threads><max_insert_threads>2</max_insert_threads><max_memory_usage>3000000000</max_memory_usage><max_execution_time>0</max_execution_time></ingester></profiles><users>'+''.join(f'<{u}><password>fixture-only</password><networks><ip>::/0</ip></networks><profile>{u}</profile><quota>default</quota></{u}>' for u in ['query','ingester'])+'</users></clickhouse>')

def exercise(root,fixture):
    create_material(fixture)
    for image in [WORKER,CH]:run(['docker','pull',image],240)
    results=[]
    for stage in ['raw_fsync','chunk_commit','parquet_commit','oss_upload_before_verify']:
        s=Stack(root,fixture,stage);proof={'stage':stage};results.append(proof)
        try:
            proof['before']=s.boot();write_json(s.root/'before.json',proof['before'])
            (s.root/'control/release').touch()
            proof['boundary']=wait_for(lambda:json.loads((s.root/'control/fired.json').read_text()) if (s.root/'control/fired.json').exists() else None,90,'fault boundary '+stage)
            s.fault_ns=time.time_ns();started=time.monotonic()
            run(['sudo','--preserve-env=GITHUB_ACTIONS,RUNNER_ENVIRONMENT,GITHUB_WORKSPACE,GITHUB_RUN_ID','python3','-m','tools.recovery_fixture33.kill_stack',str(s.root),*s.names.values()],30)
            proof['fault']=json.loads((s.root/'fault-signals.json').read_text())
            proof['after']=wait_for(lambda:s.recovered(proof['before']),120,'automatic whole-stack recovery '+stage)
            proof['recoverySeconds']=time.monotonic()-started;proof['files']=s.files()
            # Further query/archive oracle validation is deliberately separate from process recovery.
            print(json.dumps({'stage':stage,'recoverySeconds':proof['recoverySeconds'],'files':proof['files']}),flush=True)
        except BaseException as exc:proof['failure']=str(exc);raise
        finally:
            write_json(s.root/'drill.json',proof)
            try:s.close()
            finally:write_json(root/('drill-'+stage+'.json'),proof)
    return results
