"""Full client counters with bounded spool, immutable batch IDs and explicit gaps."""
import hashlib
import logging
LOGGER = logging.getLogger(__name__)
import json
import math
import re
import threading
import time
from .redis_protocol import key_shape,error_code
from .redis_history import encode,decode

IDENT=re.compile(r'[A-Za-z0-9_.:/-]{1,160}')
COMMAND=re.compile(r'[a-z][a-z0-9_|-]{0,63}')


class RedisAggregates:
    def __init__(self,service,client_factory=None):
        self.service=service;self.client_factory=client_factory;self.client=None;self.state={'status':'not_connected'};self.lock=threading.Lock();self.last_flush=0;self.last_hour=0;self.schema_ready=False;self.last_budget=0
        with service.db() as c:
            c.executescript('''CREATE TABLE IF NOT EXISTS redis_client_spool(instance TEXT,batch TEXT,ts REAL,digest TEXT,payload BLOB,done INTEGER DEFAULT 0,PRIMARY KEY(instance,batch));
                CREATE INDEX IF NOT EXISTS redis_client_pending ON redis_client_spool(done,ts);
                CREATE TABLE IF NOT EXISTS redis_profile_spool(instance TEXT,node TEXT,ts REAL,id TEXT,payload BLOB,done INTEGER DEFAULT 0,PRIMARY KEY(instance,node,ts,id));
                CREATE TABLE IF NOT EXISTS redis_profile_imports(instance TEXT,node TEXT,source_hash TEXT,ts REAL,digest TEXT,PRIMARY KEY(instance,node,source_hash));
                CREATE TABLE IF NOT EXISTS redis_ch_dirty_hours(hour INTEGER PRIMARY KEY);''')
            # Also recover acknowledged windows written by the preceding release.
            c.execute('INSERT OR IGNORE INTO redis_ch_dirty_hours SELECT DISTINCT CAST(ts/3600 AS INTEGER)*3600 FROM redis_profile_spool WHERE done=1')
            for shift in (0,120):c.execute('INSERT OR IGNORE INTO redis_ch_dirty_hours SELECT DISTINCT CAST((ts-?)/3600 AS INTEGER)*3600 FROM redis_client_spool WHERE done=1',(shift,))

    def _client(self):
        if self.client is not None and self.schema_ready:return self.client
        if self.client_factory:self.client=self.client_factory()
        else:
            from .clickhouse_client import ClickHouseClient,ClickHouseConfig
            config=ClickHouseConfig.from_env()
            if not config.enabled:raise RuntimeError('redis_clickhouse_not_enabled')
            self.client=ClickHouseClient(config)
        database=self.client.config.database
        # Only new diagnostic tables; never a source/business database.
        for name,ttl in [('redis_client_counts',30),('redis_client_hours',180)]:
            self.client.query(f'''CREATE TABLE IF NOT EXISTS {database}.{name} (
                instance String, process String, batch String, row_id String, ts DateTime('UTC'), window_end DateTime('UTC'),
                deployment String, pod String, command String, shape String, path String, script String,
                calls UInt64, errors UInt64, client_us UInt64, latency_known_calls UInt64,
                request_bytes UInt64, response_bytes UInt64, bytes_known_calls UInt64,
                version UInt64, payload String
            ) ENGINE=ReplacingMergeTree(version) PARTITION BY toDate(ts) ORDER BY (instance,ts,process,batch,row_id)
            TTL ts + INTERVAL {ttl} DAY SETTINGS index_granularity=8192''',timeout=15)
        self.client.query(f'''CREATE TABLE IF NOT EXISTS {database}.redis_prefix_profiles (
            instance String,node String,ts DateTime('UTC'),id String,payload String,version UInt64
        ) ENGINE=ReplacingMergeTree(version) PARTITION BY toDate(ts) ORDER BY(instance,node,ts,id)
        TTL ts + INTERVAL 30 DAY''',timeout=15)
        self.client.query(f'''CREATE TABLE IF NOT EXISTS {database}.redis_prefix_hours (
            instance String,node String,ts DateTime('UTC'),id String,payload String,version UInt64
        ) ENGINE=ReplacingMergeTree(version) PARTITION BY toDate(ts) ORDER BY(instance,node,ts,id)
        TTL ts + INTERVAL 180 DAY''',timeout=15)
        self.schema_ready=True
        return self.client

    def ingest(self,p):
        envelope={'instance','process','deployment','pod','batch','start','end','rows','totals','dropped_calls','pipeline_count','pipeline_client_us'}
        if not isinstance(p,dict) or set(p)!=envelope:raise ValueError('redis_aggregate_envelope')
        entry=next((e for e in self.service.entries if e['instanceId']==p['instance']),None)
        if entry is None:raise ValueError('redis_aggregate_instance')
        for k in ('process','deployment','pod','batch'):
            if not isinstance(p[k],str) or not IDENT.fullmatch(p[k]):raise ValueError('redis_aggregate_identity')
        for k in ('start','end'):
            if isinstance(p[k],bool) or not isinstance(p[k],(int,float)) or not math.isfinite(p[k]):raise ValueError('redis_aggregate_time')
        now=self.service.clock()
        if not now-86400<=p['start']<p['end']<=now+30 or p['end']-p['start']>120:raise ValueError('redis_aggregate_window')
        def uint(n):return isinstance(n,int) and not isinstance(n,bool) and 0<=n<=10**15
        for k in ('dropped_calls','pipeline_count','pipeline_client_us'):
            if not uint(p[k]):raise ValueError('redis_aggregate_counter')
        if not isinstance(p['rows'],list) or not 1<=len(p['rows'])<=500:raise ValueError('redis_aggregate_shape_budget')
        fields={'command','shape','path','script','calls','errors','client_us','latency_known_calls','request_bytes','response_bytes','bytes_known_calls'}
        rows=[];ids=set();commands={}
        for r in p['rows']:
            if not isinstance(r,dict) or set(r)!=fields:raise ValueError('redis_aggregate_fields')
            if not isinstance(r['command'],str) or not COMMAND.fullmatch(r['command']):raise ValueError('redis_aggregate_command')
            if not isinstance(r['shape'],str) or len(r['shape'])>512 or not re.fullmatch(r'[a-z0-9_:{}*|-]{1,512}',r['shape']):raise ValueError('redis_aggregate_shape')
            if not isinstance(r['path'],str) or not IDENT.fullmatch(r['path']) or not isinstance(r['script'],str) or r['script'] and not re.fullmatch(r'[a-f0-9]{40,64}',r['script']):raise ValueError('redis_aggregate_path')
            if any(not uint(r[k]) for k in fields-{'command','shape','path','script'}):raise ValueError('redis_aggregate_counter')
            if r['errors']>r['calls'] or r['latency_known_calls']>r['calls'] or r['bytes_known_calls']>r['calls']:raise ValueError('redis_aggregate_counter_scope')
            # Shape text must already be canonical: reject raw identifiers instead of silently merging them.
            if key_shape(r['shape'])!=r['shape'] and r['shape']!='other':raise ValueError('redis_aggregate_raw_key_rejected')
            rid=hashlib.sha256(json.dumps([r[k] for k in ('command','shape','path','script')]).encode()).hexdigest()
            if rid in ids:raise ValueError('redis_duplicate_aggregate_shape')
            ids.add(rid);rows.append({**r,'row_id':rid});commands[r['command']]=commands.get(r['command'],0)+r['calls']
        if not isinstance(p['totals'],dict) or len(p['totals'])>512 or any(not COMMAND.fullmatch(k) or not uint(v) for k,v in p['totals'].items()):raise ValueError('redis_command_totals')
        if sum(commands.values())!=sum(p['totals'].values()):raise ValueError('redis_aggregate_totals_mismatch')
        # Exact per-command totals survive the 500-shape overflow bucket.
        if any(v>p['totals'].get(cmd,0) for cmd,v in commands.items() if cmd!='other'):raise ValueError('redis_aggregate_command_mismatch')
        self.service.check_storage(entry)
        canonical=json.dumps(p,sort_keys=True,separators=(',',':')).encode();digest=hashlib.sha256(canonical).hexdigest()
        with self.lock,self.service.db() as c:
            old=c.execute('SELECT digest FROM redis_client_spool WHERE instance=? AND batch=?',(p['instance'],p['batch'])).fetchone()
            if old:
                if old[0]!=digest:raise ValueError('redis_aggregate_batch_conflict')
                return {'accepted':False,'duplicate':True,'spooled':True}
            pending=c.execute('SELECT COUNT(*) FROM (SELECT 1 FROM redis_client_spool WHERE done=0 LIMIT 2001)').fetchone()[0]
            if pending>=2000:raise RuntimeError('redis_aggregate_spool_full')
            c.execute('INSERT INTO redis_client_spool VALUES(?,?,?,?,?,0)',(p['instance'],p['batch'],p['end'],digest,encode(p)))
        self.state.update(last_received=now,last_process=p['process'])
        return {'accepted':True,'spooled':True,'client_scope':'full_logical_calls; transport_or_unhooked_paths_may_be_missing','server_cpu':False}

    def import_profile(self,p):
        expected={'instance','node','time','rdb_sha256','complete','total_keys','groups'}
        if not isinstance(p,dict) or set(p)!=expected:raise ValueError('redis_profile_envelope')
        entry=next((e for e in self.service.entries if e['instanceId']==p['instance']),None)
        if not entry or not re.fullmatch(r'db-\d{1,2}',str(p['node'])):raise ValueError('redis_profile_target')
        if not isinstance(p['time'],(int,float)) or isinstance(p['time'],bool) or not math.isfinite(p['time']) or not self.service.clock()-7*86400<=p['time']<=self.service.clock()+30:raise ValueError('redis_profile_time')
        if not isinstance(p['rdb_sha256'],str) or not re.fullmatch(r'[a-f0-9]{64}',p['rdb_sha256']) or not isinstance(p['complete'],bool):raise ValueError('redis_profile_provenance')
        if not isinstance(p['groups'],dict) or not 1<=len(p['groups'])<=3000:raise ValueError('redis_profile_group_budget')
        groups={};total=0
        def uint(v):return isinstance(v,int) and not isinstance(v,bool) and 0<=v<=10**15
        if not uint(p['total_keys']):raise ValueError('redis_profile_total')
        for prefix,v in p['groups'].items():
            if not isinstance(prefix,str) or len(prefix)>512 or key_shape(prefix)!=prefix or not isinstance(v,dict) or set(v)!={'keys','estimated_bytes','expires','types'}:raise ValueError('redis_profile_fields')
            if any(not uint(v[k]) for k in ('keys','estimated_bytes','expires')) or v['expires']>v['keys']:raise ValueError('redis_profile_count')
            if not isinstance(v['types'],dict) or set(v['types'])-{'string','hash','list','set','zset','stream','module','other'} or any(not uint(n) for n in v['types'].values()) or sum(v['types'].values())!=v['keys']:raise ValueError('redis_profile_types')
            total+=v['keys'];groups[prefix]={'observed_keys':v['keys'],'memory_bytes_observed':v['estimated_bytes'],'memory_known':v['keys'],'ttl_none':v['keys']-v['expires'],'ttl_expiring':v['expires'],'types':v['types']}
        if total>p['total_keys'] or p['complete'] and total!=p['total_keys']:raise ValueError('redis_profile_total_mismatch')
        self.service.check_storage(entry)
        payload={'groups':groups,'source':'offline_rdb','complete':p['complete'],'total_keys':p['total_keys'],'scope':'imported_offline_counts; completeness supplied by producer, not independently certified','memory_scope':'offline estimated memory, not Redis MEMORY USAGE','rdb_sha256':p['rdb_sha256']}
        digest=hashlib.sha256(json.dumps(p,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        with self.lock,self.service.db() as c:
            old=c.execute('SELECT digest FROM redis_profile_imports WHERE instance=? AND node=? AND source_hash=?',(p['instance'],p['node'],p['rdb_sha256'])).fetchone()
            if old:
                if old[0]!=digest:raise ValueError('redis_profile_source_conflict')
                return {'accepted':False,'duplicate':True}
            c.execute('INSERT INTO redis_profile_imports VALUES(?,?,?,?,?)',(p['instance'],p['node'],p['rdb_sha256'],p['time'],digest))
            c.execute('INSERT INTO redis_evidence VALUES(?,?,?,?,?,?)',(p['instance'],'profile',p['node'],p['time'],p['rdb_sha256'],encode(payload)))
            c.execute('INSERT OR IGNORE INTO redis_profile_spool VALUES(?,?,?,?,?,0)',(p['instance'],p['node'],p['time'],p['rdb_sha256'],encode(payload)))
        return {'accepted':True,'groups':len(groups),'source':'offline_rdb','complete_claimed':p['complete']}

    def profile(self,instance,node,ts,payload):
        blob=encode(payload)
        with self.service.db() as c:c.execute('INSERT OR IGNORE INTO redis_profile_spool VALUES(?,?,?,?,?,0)',(instance,node,ts,hashlib.sha256(blob).hexdigest(),blob))

    def flush(self):
        now=self.service.clock()
        if now-self.last_flush<5:return
        self.last_flush=now
        try:
            client=self._client();database=client.config.database
            with self.service.db() as c:
                batches=c.execute('SELECT * FROM redis_client_spool WHERE done=0 ORDER BY ts LIMIT 200').fetchall()
                profiles=c.execute('SELECT * FROM redis_profile_spool WHERE done=0 ORDER BY ts LIMIT 30').fetchall()
            if now-self.last_budget>=60:
                usage=client.json_rows('SELECT sum(bytes_on_disk) bytes FROM system.parts WHERE database={db:String} AND table IN (\'redis_client_counts\',\'redis_client_hours\',\'redis_prefix_profiles\',\'redis_prefix_hours\') AND active',parameters={'db':database},timeout=5)
                cap=min(int(e.get('maxClickHouseMiB',8192)) for e in self.service.entries)*1024**2
                if usage and int(usage[0].get('bytes') or 0)>=cap:raise RuntimeError('redis_clickhouse_storage_fuse')
                self.last_budget=now
            values=[];selected=[];dirty=set()
            for b in batches:
                if len(values)>=20000:break
                selected.append(b)
                p=decode(b['payload']);dirty.add(int(p['start']//3600)*3600);metadata={k:p[k] for k in ('totals','dropped_calls','pipeline_count','pipeline_client_us','start','end')}
                for index,r in enumerate(p['rows']):
                    rid=hashlib.sha256(json.dumps([r[k] for k in ('command','shape','path','script')]).encode()).hexdigest()
                    values.append({'instance':p['instance'],'process':p['process'],'batch':p['batch'],'row_id':rid,'ts':int(p['start']),'window_end':int(p['end']),'deployment':p['deployment'],'pod':p['pod'],**r,'version':1,'payload':json.dumps(metadata) if index==0 else ''})
            for offset in range(0,len(values),5000):client.insert_json_rows(database+'.redis_client_counts',values[offset:offset+5000],timeout=15)
            if profiles:
                client.insert_json_rows(database+'.redis_prefix_profiles',[{'instance':r['instance'],'node':r['node'],'ts':int(r['ts']),'id':r['id'],'payload':json.dumps(decode(r['payload'])),'version':1} for r in profiles],timeout=15)
            with self.service.db() as c:
                for b in selected:c.execute('UPDATE redis_client_spool SET done=1 WHERE instance=? AND batch=?',(b['instance'],b['batch']))
                for r in profiles:
                    dirty.add(int(r['ts']//3600)*3600)
                    c.execute('UPDATE redis_profile_spool SET done=1 WHERE instance=? AND node=? AND ts=? AND id=?',(r['instance'],r['node'],r['ts'],r['id']))
                c.executemany('INSERT OR IGNORE INTO redis_ch_dirty_hours VALUES(?)',[(h,) for h in dirty])
                c.execute('DELETE FROM redis_client_spool WHERE rowid IN (SELECT rowid FROM redis_client_spool WHERE done=1 AND ts<? LIMIT 2000)',(now-7*86400,))
                c.execute('DELETE FROM redis_profile_spool WHERE rowid IN (SELECT rowid FROM redis_profile_spool WHERE done=1 AND ts<? LIMIT 2000)',(now-86400,))
                c.execute('DELETE FROM redis_profile_imports WHERE ts<?',(now-90*86400,))
            with self.service.db() as c:pending=c.execute('SELECT COUNT(*) FROM redis_client_spool WHERE done=0').fetchone()[0]
            self.state.pop('error',None)
            self.state.update(status='ok',last_flushed=now,pending_batches=pending,client_connected=bool(self.state.get('last_received')))
            self.rollup_hours(client,now)
        except Exception as exc:
            LOGGER.exception('Redis durable aggregate flush failed; pending batches retained')
            self.state.update(status='buffering_or_unavailable',error=error_code(exc),last_attempt=now)

    def rollup_hours(self,client,now):
        db=client.config.database;closed=int((now-120)//3600)*3600
        with self.service.db() as c:hours=[r[0] for r in c.execute('SELECT hour FROM redis_ch_dirty_hours WHERE hour<? ORDER BY hour LIMIT 6',(closed,))]
        # One closed hour per idempotent rebuild, including late arrivals.
        for start in hours:
            end=start+3600
            client.query(f'''INSERT INTO {db}.redis_client_hours SELECT instance,process,concat('hour-',toString(toStartOfHour(ts))),
                hex(SHA256(toJSONString(tuple(deployment,pod,command,shape,path,script)))),toStartOfHour(ts),toStartOfHour(ts)+INTERVAL 1 HOUR,
                deployment,pod,command,shape,path,script,sum(calls),sum(errors),sum(client_us),sum(latency_known_calls),
                sum(request_bytes),sum(response_bytes),sum(bytes_known_calls),toUInt64({int(now)}),''
                FROM {db}.redis_client_counts FINAL WHERE ts>=toDateTime({start}) AND ts<toDateTime({end})
                GROUP BY instance,process,toStartOfHour(ts),deployment,pod,command,shape,path,script''',timeout=15)
            client.query(f'''INSERT INTO {db}.redis_prefix_hours SELECT instance,node,toStartOfHour(ts),
                if(JSONExtractString(payload,'source')='offline_rdb','offline_rdb','cursor_sample'),
                argMax(payload,ts),toUInt64({int(now)}) FROM {db}.redis_prefix_profiles FINAL
                WHERE ts>=toDateTime({start}) AND ts<toDateTime({end}) GROUP BY instance,node,toStartOfHour(ts),if(JSONExtractString(payload,'source')='offline_rdb','offline_rdb','cursor_sample')''',timeout=15)
            with self.service.db() as c:c.execute('DELETE FROM redis_ch_dirty_hours WHERE hour=?',(start,))
        with self.service.db() as c:self.state['pending_closed_hours']=c.execute('SELECT count(*) FROM redis_ch_dirty_hours WHERE hour<?',(closed,)).fetchone()[0]

    def query(self,instance,start,end,baseline=True):
        if end-start>86400:raise ValueError('redis_aggregate_query_max_day')
        try:
            if self.client is None or not self.schema_ready:return {'status':'not_connected','rows':[],'reported_calls':{},'complete':False}
            client=self.client;db=client.config.database
            rows=client.json_rows(f'''SELECT deployment,command,shape,path,script,sum(calls) calls,sum(errors) errors,
                sum(client_us) client_us,sum(latency_known_calls) latency_known_calls,
                sum(request_bytes) request_bytes,sum(response_bytes) response_bytes,sum(bytes_known_calls) bytes_known_calls,
                uniqExact(pod) reported_pods FROM {db}.redis_client_counts FINAL
                WHERE instance={{instance:String}} AND ts>=toDateTime({int(start)}) AND window_end<=toDateTime({int(end)})
                GROUP BY deployment,command,shape,path,script ORDER BY calls DESC LIMIT 501''',parameters={'instance':instance},settings={'max_execution_time':10,'max_memory_usage':128000000},timeout=12)
            reports=client.json_rows(f'''SELECT process,batch,any(payload) metadata FROM {db}.redis_client_counts FINAL
                WHERE instance={{instance:String}} AND ts>=toDateTime({int(start)}) AND window_end<=toDateTime({int(end)}) AND payload!=''
                GROUP BY process,batch LIMIT 10001''',parameters={'instance':instance},settings={'max_execution_time':10,'max_memory_usage':128000000},timeout=12)
            reported={};dropped=0;pipeline_count=0;pipeline_us=0
            for r in reports[:10000]:
                m=json.loads(r['metadata']);dropped+=m['dropped_calls'];pipeline_count+=m.get('pipeline_count',0);pipeline_us+=m.get('pipeline_client_us',0)
                for cmd,n in m['totals'].items():reported[cmd]=reported.get(cmd,0)+n
            previous=self.query(instance,start-(end-start),start,False) if baseline else {}
            dims=('deployment','command','shape','path','script')
            old={tuple(r[k] for k in dims):r for r in previous.get('rows',[])}
            for row in rows:
                before=old.get(tuple(row[k] for k in dims))
                row['baseline_calls_observed']=int(before['calls']) if before and not previous.get('truncated') else None
                row['delta_calls_observed']=int(row['calls'])-int(before['calls']) if before and not previous.get('truncated') and len(rows)<=500 and not dropped and not previous.get('dropped_calls_reported') else None
            rows.sort(key=lambda r:(r['delta_calls_observed'] is not None,r['delta_calls_observed'] if r['delta_calls_observed'] is not None else int(r['calls'])),reverse=True)
            return {'status':'observed' if reports else 'not_connected_or_no_reports','rows':rows[:500],'truncated':len(rows)>500 or len(reports)>10000,'reported_calls':reported,'dropped_calls_reported':dropped,
                    'pipeline_count':pipeline_count,'pipeline_client_us':pipeline_us,'baseline_status':previous.get('status'),
                    'scope':'logical_client_counters_not_wire_attempts; changes are between observed reports, not certified complete source coverage; pipeline latency not allocated to commands','complete':False}
        except Exception as exc:return {'status':'unavailable','error':error_code(exc),'rows':[],'reported_calls':{},'complete':False}
