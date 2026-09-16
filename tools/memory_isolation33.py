"""CI-only ABBA experiment. Synthetic evidence is NOT a reproduction of production OOM."""
from __future__ import annotations
import argparse,base64,concurrent.futures,hashlib,json,os,statistics,subprocess,sys,threading,time,uuid
from pathlib import Path
from urllib.request import Request,urlopen
from urllib.parse import urlencode

APP='ghcr.io/lop-spec/rds-binlog-insight@sha256:e33906e5a6732994192cbf073bb39bb424feb6f37ce3fd4db6dab3577dfa0698'
# Version components, not a public host address; canonicalize only once.
VERSION_PARTS=(26,3,17,110)
VERSION='.'.join(map(str,VERSION_PARTS))
CH='clickhouse/clickhouse-server:'+VERSION
LIMIT=3*1024**3
ROWS=262144
WIDE_ROWS=65536
WIDTH=8192
EPOCH=1786954001186000
STEP=2592000000000//ROWS
CAP=500000000

def cases():
    out=[]
    for modulo,offset in [(7,0),(7,50),(3,0),(3,50)]:
        ids=[n for n in range(ROWS-1,-1,-1) if n%modulo==0][offset:offset+50]
        out.append({'name':f'slowlog-{modulo}-offset-{offset}',
                    'sql':f'SELECT id,event_epoch_us,row_query FROM mongo_ci_fixture.slowlog WHERE id%{modulo}=0 AND event_epoch_us BETWEEN {EPOCH} AND {EPOCH+2592000000000} ORDER BY event_epoch_us DESC,id DESC LIMIT 50 OFFSET {offset}',
                    'expected':[{'id':n,'event_epoch_us':EPOCH+n*STEP,'row_query':f'SELECT * FROM fixture.orders WHERE customer_id={n%97} AND order_id={n}'} for n in ids]})
    out.append({'name':'legacy-parquet-wide','sql':"SELECT count() AS n,sum(length(row_query)) AS bytes FROM file('wide.parquet',Parquet)",
                'expected':[{'n':WIDE_ROWS,'bytes':WIDE_ROWS*WIDTH}]})
    return out

def normalized(rows):
    return [{k:int(v) if k in ('id','event_epoch_us','n','bytes') else v for k,v in r.items()} for r in rows]

def fixture_only():
    if os.environ.get('MEMORY_ISOLATION_FIXTURE')!='1':raise RuntimeError('isolated fixture opt-in required')

def generate():
    fixture_only()
    import pyarrow as pa
    import pyarrow.parquet as pq
    # One highly compressed 512 MiB row group exercises the legacy decoder,
    # without exporting production SQL or making a multi-GiB single value.
    values=[(f'{i:08d}'+('x'*(WIDTH-8))) for i in range(WIDE_ROWS)]
    table=pa.table({'row_query':pa.array(values,type=pa.string())})
    del values
    pq.write_table(table,'/fixture/wide.parquet',row_group_size=WIDE_ROWS,use_dictionary=False,compression='zstd')
    raw=Path('/fixture/wide.parquet').read_bytes()
    print(json.dumps({'rows':WIDE_ROWS,'logicalBytes':WIDE_ROWS*WIDTH,'fileBytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}))

def client(index):
    fixture_only()
    sys.path.insert(0,'/src')
    from app.clickhouse_client import ClickHouseClient,ClickHouseConfig
    from app.clickhouse_query import query_rows_with_cancel
    from tests.test_clickhouse_query_lifecycle import Control
    config=ClickHouseConfig.from_env()
    assert (config.host,config.port,config.database)==('127.0.0.1',18123,'mongo_ci_fixture')
    timeout_case=index==-1;case={'name':'forced-http-timeout','sql':'SELECT sleep(3)'} if timeout_case else cases()[index]
    class OwnedClient(ClickHouseClient):
        owned=''
        acknowledgements=0
        def json_rows(self,sql,**kwargs):
            settings=kwargs.get('settings') or {}
            if settings.get('query_id'):
                self.owned=settings['query_id']
                if timeout_case:kwargs['settings']={**settings,'cancel_http_readonly_queries_on_client_close':0}
            return super().json_rows(sql,**kwargs)
        def query(self,sql,**kwargs):
            answer=super().query(sql,**kwargs)
            if sql.startswith('KILL QUERY'):self.acknowledgements+=1
            return answer
    c=OwnedClient(config);start=time.monotonic()
    result={'name':case['name'],'maxMemoryUsage':CAP,'rows':None,'error':None,'cleanupRemaining':None}
    try:
        result['rows']=query_rows_with_cancel(c,case['sql'],{},Control(),timeout=1 if timeout_case else 50,
            settings={'max_memory_usage':CAP,'max_execution_time':45,'max_threads':1,'input_format_parquet_use_native_reader_v3':0,
                      'input_format_parquet_max_block_size':1024,'input_format_parquet_prefer_block_bytes':8388608,
                      'input_format_max_block_size_bytes':33554432,'input_format_parquet_enable_row_group_prefetch':0})
    except Exception as exc:result['error']={'type':type(exc).__name__,'message':str(exc)[:1200]}
    finally:
        result.update(seconds=time.monotonic()-start,queryId=c.owned,cleanupAcks=c.acknowledgements)
        try:
            rows=ClickHouseClient.json_rows(c,'SELECT count() n FROM system.processes WHERE query_id={id:String}',parameters={'id':c.owned},timeout=3,settings={'max_memory_usage':50000000,'max_threads':1,'max_execution_time':2})
            result['cleanupRemaining']=int(rows[0]['n'])
        except Exception as exc:result['cleanupCheckError']=type(exc).__name__
        if result['cleanupRemaining']:
            c.query('KILL QUERY WHERE query_id={id:String} SYNC',parameters={'id':c.owned},timeout=10)
    result['oracle']=normalized(result['rows'] or [])==case.get('expected') if not timeout_case else None
    result['timeoutCleanupPassed']=bool(result['error'] and 'timed out' in result['error']['message'] and result['cleanupRemaining']==0 and c.acknowledgements==1) if timeout_case else None
    print(json.dumps(result),flush=True)

def run(args,timeout=90):
    r=subprocess.run(args,capture_output=True,text=True,timeout=timeout)
    if r.returncode:raise RuntimeError(f'{args[:3]} exit={r.returncode}: {r.stderr[-2000:]}')
    return r.stdout

def sql(query,timeout=5,**settings):
    values={'max_threads':1,'max_memory_usage':50000000,'max_execution_time':3,**settings}
    req=Request('http://127.0.0.1:18123/?'+urlencode(values),data=query.encode(),headers={'Authorization':'Basic '+base64.b64encode(b'fixture:fixture-ci-only').decode()})
    with urlopen(req,timeout=timeout) as response:return response.read().decode()

def rows(query,**settings):return [json.loads(line) for line in sql(query+' FORMAT JSONEachRow',**settings).splitlines() if line]

def inspect(name):return json.loads(run(['docker','inspect',name],10))[0]
def counters(path):return {k:int(v) for k,v in (s.split() for s in path.read_text().splitlines())}

def monitor(name,stop,samples):
    group=None;next_metrics=0
    while not stop.wait(.2):
        item={'at':time.time()}
        try:
            if group is None:
                c=inspect(name);pid=c['State']['Pid']
                group=Path('/sys/fs/cgroup')/Path(f'/proc/{pid}/cgroup').read_text().split('0::')[1].strip().lstrip('/')
            stat=counters(group/'memory.stat')
            item.update(current=int((group/'memory.current').read_text()),events=counters(group/'memory.events'),
                        stat={k:stat.get(k,0) for k in ('anon','file','sock','kernel','slab_reclaimable')})
            if time.monotonic()>=next_metrics:
                try:item['metrics']=rows("SELECT metric,toFloat64(value) value FROM system.metrics WHERE metric='MemoryTracking' UNION ALL SELECT metric,value FROM system.asynchronous_metrics WHERE metric IN ('MemoryResident','CGroupMemoryUsed','jemalloc.allocated','jemalloc.resident')",timeout=1)
                except Exception as e:item['metricError']=type(e).__name__
                next_metrics=time.monotonic()+1
        except Exception as e:item['sampleError']=type(e).__name__
        samples.append(item)

def summarize(phase):
    samples=[s for s in phase['samples'] if 'current' in s];gaps=[];accounting=[]
    for s in samples:
        metrics={v['metric']:v['value'] for v in s.get('metrics',[])}
        if 'MemoryTracking' in metrics:gaps.append(s['current']-metrics['MemoryTracking'])
        stat=s['stat'];accounting.append(s['current']-sum(stat.get(k,0) for k in ('anon','file','kernel','sock')))
    results=phase.get('queries',[])
    return {'peakCgroupBytes':max((s['current'] for s in samples),default=None),
            'medianCgroupMinusTracking':statistics.median(gaps) if gaps else None,
            'medianCgroupMinusCategories':statistics.median(accounting) if accounting else None,
            'oomEvents':max((s['events'].get('oom_kill',0) for s in samples),default=0),
            'failedQueries':sum(bool(q.get('error')) for q in results),'oracleFailures':sum(q.get('oracle') is False for q in results),
            'cleanupFailures':sum(q.get('cleanupRemaining')!=0 for q in results),
            'timeouts':sum('timed out' in str(q.get('error')) for q in results),
            'successfulWithin60':sum(q.get('oracle') is True and q['seconds']<=60 for q in results)}

def main():
    assert os.environ.get('GITHUB_ACTIONS')=='true','cloud CI only; never operate production Docker'
    root=Path('memory-evidence').resolve();root.mkdir(exist_ok=False)
    data=root/'fixture';data.mkdir();source=Path.cwd()
    for image in [APP,CH]:run(['docker','pull',image],240)
    app_cmd=['docker','run','--rm','--network','host','--cpus','2','--memory','2g','--user',str(os.getuid()),'--entrypoint','python','--workdir','/src','-v',f'{source}:/src:ro','-e','MEMORY_ISOLATION_FIXTURE=1']
    generated=run(app_cmd+['-v',f'{data}:/fixture',APP,'tools/memory_isolation33.py','--generate'],150)
    evidence={'scope':'synthetic ABBA; not a proven reproduction of incident 31','version':VERSION,'hardLimit':LIMIT,
              'appImage':APP,'sourceSha':os.environ.get('GITHUB_SHA'),'images':{},'fixture':json.loads(generated),'phases':[]}
    for image in [APP,CH]:
        c=json.loads(run(['docker','image','inspect',image],10))[0];evidence['images'][image]={'id':c['Id'],'repoDigests':c['RepoDigests']}
    def save():
        raw=json.dumps(evidence,indent=2).encode();p=root/'memory-isolated33.json';p.write_bytes(raw)
        (root/'memory-isolated33.json.sha256').write_text(hashlib.sha256(raw).hexdigest()+'  memory-isolated33.json\n')
    clients=app_cmd+['-e','RDS_BINLOG_CLICKHOUSE_HOST=127.0.0.1','-e','RDS_BINLOG_CLICKHOUSE_PORT=18123','-e','RDS_BINLOG_CLICKHOUSE_DATABASE=mongo_ci_fixture','-e','CLICKHOUSE_USER=fixture','-e','CLICKHOUSE_PASSWORD=fixture-ci-only',APP,'tools/memory_isolation33.py']
    def execute(index):
        try:return json.loads(run(clients+['--client',str(index)],75))
        except Exception as e:return {'name':f'client-{index}','error':str(e),'cleanupRemaining':None}
    try:
        for ordinal,variant in enumerate(['baseline','candidate','candidate','baseline']):
            name=f'memory-isolation-{uuid.uuid4().hex[:12]}'
            cap,correct=(2500000000,0) if variant=='baseline' else (1800000000,1)
            conf=root/f'config-{ordinal}.xml';conf.write_text(f'<clickhouse><max_server_memory_usage>{cap}</max_server_memory_usage><max_server_memory_usage_to_ram_ratio>0.8</max_server_memory_usage_to_ram_ratio><memory_worker_correct_memory_tracker>{correct}</memory_worker_correct_memory_tracker><memory_worker_use_cgroup>1</memory_worker_use_cgroup><background_pool_size>1</background_pool_size><background_merges_mutations_concurrency_ratio>1</background_merges_mutations_concurrency_ratio><background_schedule_pool_size>32</background_schedule_pool_size><background_buffer_flush_schedule_pool_size>2</background_buffer_flush_schedule_pool_size><background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size><merge_tree><number_of_free_entries_in_pool_to_execute_mutation>0</number_of_free_entries_in_pool_to_execute_mutation><number_of_free_entries_in_pool_to_execute_optimize_entire_partition>0</number_of_free_entries_in_pool_to_execute_optimize_entire_partition><number_of_free_entries_in_pool_to_lower_max_size_of_merge>0</number_of_free_entries_in_pool_to_lower_max_size_of_merge></merge_tree><mark_cache_size>5368709120</mark_cache_size><uncompressed_cache_size>8589934592</uncompressed_cache_size></clickhouse>')
            phase={'ordinal':ordinal,'variant':variant,'cap':cap,'correct':correct,'samples':[],'queries':[]};evidence['phases'].append(phase)
            stop=threading.Event();thread=None
            try:
                run(['docker','run','-d','--name',name,'--label','scope=memory-isolation-ci','--memory','3g','--memory-swap','3g','--cpus','2','--restart','no','-p','127.0.0.1:18123:8123','-v',f'{conf}:/etc/clickhouse-server/config.d/memory-fixture.xml:ro','-v',f'{data}:/var/lib/clickhouse/user_files:ro','-e','CLICKHOUSE_DB=mongo_ci_fixture','-e','CLICKHOUSE_USER=fixture','-e','CLICKHOUSE_PASSWORD=fixture-ci-only',CH],30)
                for attempt in range(40):
                    try:
                        version=sql('SELECT version()').strip()
                        if version!=VERSION:raise AssertionError('wrong ClickHouse version: '+version)
                        break
                    except (OSError,TimeoutError):time.sleep(1)
                else:raise RuntimeError('fixture readiness deadline')
                c=inspect(name);assert c['HostConfig']['Memory']==LIMIT and c['HostConfig']['MemorySwap']==LIMIT
                phase['settings']=rows("SELECT name,value FROM system.server_settings WHERE name IN ('max_server_memory_usage','memory_worker_correct_memory_tracker','memory_worker_use_cgroup')")
                effective={r['name']:r['value'] for r in phase['settings']}
                assert effective['max_server_memory_usage']==str(cap) and effective['memory_worker_correct_memory_tracker']==str(correct)
                thread=threading.Thread(target=monitor,args=(name,stop,phase['samples']),daemon=True);thread.start()
                phase['timeoutBefore']=execute(-1)
                sql('CREATE TABLE mongo_ci_fixture.slowlog(id UInt64,event_epoch_us UInt64,row_query String) ENGINE=MergeTree ORDER BY (event_epoch_us,id)')
                sql(f"INSERT INTO mongo_ci_fixture.slowlog SELECT number,{EPOCH}+number*{STEP},concat('SELECT * FROM fixture.orders WHERE customer_id=',toString(number%97),' AND order_id=',toString(number)) FROM numbers({ROWS})",timeout=30,max_execution_time=25,max_memory_usage=CAP)
                with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                    for pair in [(0,2),(1,3),(4,4)]:phase['queries'].extend(pool.map(execute,pair))
                phase['timeoutAfter']=execute(-1)
                phase['remainingOwned']=rows("SELECT query_id FROM system.processes WHERE startsWith(query_id,'rds-insight-')")
            except Exception as e:phase['failure']=str(e)
            finally:
                stop.set()
                if thread:thread.join(8)
                try:
                    c=inspect(name);phase['containerState']=c['State'];assert c['Config']['Labels'].get('scope')=='memory-isolation-ci'
                    (root/f'{ordinal}-container.log').write_text(run(['docker','logs','--tail','100',name],10))
                    run(['docker','stop','--time','30',name],40)
                    run(['docker','rm',name],10)
                except Exception as e:phase['cleanupError']=str(e)
                phase['summary']=summarize(phase);save();print(json.dumps({'phase':ordinal,'variant':variant,**phase['summary'],'failure':phase.get('failure')}),flush=True)
        evidence['gate']={'allFixturesRan':all(not p.get('failure') and not p.get('cleanupError') for p in evidence['phases']),
                          'candidateNoOom':all(not p.get('containerState',{}).get('OOMKilled',True) and p['summary']['oomEvents']==0 for p in evidence['phases'] if p['variant']=='candidate'),
                          'allOraclesAndCleanup':all(p['summary']['oracleFailures']==0 and p['summary']['cleanupFailures']==0 and len(p['queries'])==6 and not p.get('remainingOwned') and p.get('timeoutBefore',{}).get('timeoutCleanupPassed') and p.get('timeoutAfter',{}).get('timeoutCleanupPassed') for p in evidence['phases'])}
        a=[p for p in evidence['phases'] if p['variant']=='baseline'];b=[p for p in evidence['phases'] if p['variant']=='candidate']
        evidence['gate']['candidateDoesNotIncreaseFailures']=sum(p['summary']['failedQueries'] for p in b)<=sum(p['summary']['failedQueries'] for p in a)
        evidence['productionOomReproduced']=False
        evidence['limitations']=['Synthetic data only; cannot attribute incident 31 from this experiment.','Cgroup categories and MemoryTracking are different measurements; correction does not fix kernel accounting.','No production settings, containers or data changed.']
        save()
        if not all(evidence['gate'].values()):raise RuntimeError('isolation gates not all passed; see evidence')
    except BaseException:
        save();raise

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--generate',action='store_true');parser.add_argument('--client',type=int);args=parser.parse_args()
    if args.generate:generate()
    elif args.client is not None:client(args.client)
    else:main()
