"""Immutable Parquet source windows + shared ClickHouse serving, atomically published.

No business-database writes. Schema installation is an explicit CLI operation.
A window manifest is the single visibility pointer: partial loads cannot serve.
"""
from __future__ import annotations

import argparse
import json
import logging
import math
import os
import threading
import time
import uuid
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .clickhouse_client import ClickHouseClient, ClickHouseConfig
from .mongo_insight import COSTS, MINUTE, WINDOW, VERSION, canonical, digest, normalize_record, rollup_events
from .storage import _BodyFileLock

LOGGER = logging.getLogger(__name__)
NUMBERS = ('bucket','count','failed','collscan','spill','overlap_us','max_us','first_us','last_us') + COSTS + tuple(k+'_known' for k in COSTS)
STRINGS = ('role','group_id','profile','sample')
SCHEMA = pa.schema([(k,pa.int64()) for k in NUMBERS]+[(k,pa.string()) for k in STRINGS])
MAX_ROWS = 250_000


def atomic_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True,exist_ok=True)
    temporary = path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    with temporary.open('w',encoding='utf-8') as f:
        f.write(canonical(data))
        f.flush()
        os.fsync(f.fileno())
    os.replace(temporary,path)


def atomic_parquet(table: pa.Table, path: Path) -> None:
    temporary=path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    pq.write_table(table,temporary,compression='zstd')
    with temporary.open('r+b') as handle:os.fsync(handle.fileno())
    os.replace(temporary,path)


class MongoStore:
    def __init__(self, root: Path, backend: str='clickhouse', client=None):
        if backend not in {'clickhouse','parquet'}:
            raise ValueError('invalid_mongo_backend')
        self.root=Path(root).resolve()/'mongo-insight'
        self.root.mkdir(parents=True,exist_ok=True)
        self.backend=backend
        self.lock=threading.RLock()
        self.client=client
        self.database='insight'
        if backend=='clickhouse':
            cfg=ClickHouseConfig.from_env()
            self.database=cfg.database
            self.client=client or ClickHouseClient(cfg)
        else:
            LOGGER.warning('mongo_store explicit parquet backend: local replay; ClickHouse acceleration disabled')

    def table(self, name):
        # Identifier is validated by the existing ClickHouse configuration.
        return self.database+'.mongo_'+name

    def migrate(self):
        if self.backend!='clickhouse': return
        fields=['instance String','window_start Int64','revision String']
        fields += [k+' Int64' for k in NUMBERS]+[k+' String' for k in STRINGS]
        self.client.query('CREATE TABLE IF NOT EXISTS '+self.table('rollups')+' ('+', '.join(fields)+
                          ', created_at DateTime DEFAULT now()) ENGINE=ReplacingMergeTree '
                          'PARTITION BY toDate(created_at) ORDER BY (instance,revision,bucket,role,group_id) '
                          'TTL created_at + INTERVAL 7 DAY')
        self.client.query('CREATE TABLE IF NOT EXISTS '+self.table('telemetry')+
                          ' (instance String, kind String, id String, timestamp Int64, role String, node String, '
                          'payload String, created_at DateTime DEFAULT now()) ENGINE=ReplacingMergeTree '
                          'PARTITION BY toDate(created_at) ORDER BY (instance,kind,id) TTL created_at + INTERVAL 7 DAY')

    def check(self):
        if self.backend=='clickhouse':
            for name in ('rollups','telemetry'):
                self.client.query('SELECT count() FROM '+self.table(name)+' WHERE 0',timeout=5)

    def base(self, instance):
        return self.root/digest(instance)[:24]

    def manifest_path(self, instance, start):
        return self.base(instance)/'windows'/str(start)/'manifest.json'

    def publish(self, instance, start, end, records, prefixes, archive=None):
        if start%WINDOW or end!=start+WINDOW:
            raise ValueError('source_windows_must_be_five_minutes')
        events=[normalize_record(r,instance,prefixes) for r in records]
        if any(not start<=e['start_us']<end for e in events):
            raise ValueError('event_outside_source_window')
        revision=digest({'instance':instance,'start':start,'end':end,'version':VERSION,
                         'prefixes':prefixes,'records':sorted(canonical(r) for r in records)})
        path=self.manifest_path(instance,start)
        path.parent.mkdir(parents=True,exist_ok=True)
        with _BodyFileLock(self.lock,path.parent/'publish.lock'):
            if path.exists():
                old=json.loads(path.read_text(encoding='utf-8'))
                if old['revision']==revision and old.get('backend')==self.backend and old.get('indexed'):
                    index_ok=True
                    if self.backend=='clickhouse':
                        actual=self.client.json_rows('SELECT count() AS n FROM '+self.table('rollups')+' FINAL WHERE instance={i:String} AND revision={r:String}',parameters={'i':instance,'r':revision},timeout=5)
                        index_ok=int(actual[0]['n'])==old['rollup_rows']
                    if index_ok and (archive is None or old.get('archived')):
                        return old
                    LOGGER.warning('mongo_window repair: incomplete index/archive instance=%s window=%s',instance,start)
            rows=rollup_events(events)
            raw=path.parent/(revision+'.raw.parquet')
            roll=path.parent/(revision+'.rollup.parquet')
            # Unique immutable versions remain recoverable even if indexing fails.
            if not raw.exists():
                atomic_parquet(pa.table({'record':[canonical(r) for r in records]},schema=pa.schema([('record',pa.string())])),raw)
            if not roll.exists():
                atomic_parquet(pa.Table.from_pylist(rows,schema=SCHEMA),roll)
            if self.backend=='clickhouse':
                stored=[dict(instance=instance,window_start=start,revision=revision,**{k:r[k] for k in NUMBERS+STRINGS}) for r in rows]
                for offset in range(0,len(stored),200):
                    self.client.insert_json_rows(self.table('rollups'),stored[offset:offset+200],timeout=15)
            manifest=dict(instance=instance,start=start,end=end,revision=revision,records=len(records),
                          rollup_rows=len(rows),backend=self.backend,indexed=True,archived=False,
                          raw=raw.name,rollup=roll.name,published_at=time.time())
            if archive is not None:
                prefix=archive.prefix.rstrip('/')+'/mongo-insight/'+digest(instance)[:24]+'/'+str(start)+'/'+revision
                for file in (raw,roll):
                    archive.bucket.put_object_from_file(prefix+'/'+file.name,str(file))
                manifest['archived']=True
                archive.bucket.put_object(prefix+'/manifest.json',canonical(manifest).encode())
            else:
                LOGGER.warning('mongo_archive unavailable: retained immutable local source instance=%s window=%s',instance,start)
            atomic_json(path,manifest)
            return manifest

    def manifests(self, instance, start, end):
        if end<=start or end-start>7*86400*1_000_000:
            raise ValueError('window_must_be_between_zero_and_seven_days')
        found=[]
        missing=[]
        for t in range(start//WINDOW*WINDOW,((end-1)//WINDOW+1)*WINDOW,WINDOW):
            path=self.manifest_path(instance,t)
            if not path.exists():
                missing.append(t)
                continue
            m=json.loads(path.read_text(encoding='utf-8'))
            if m.get('instance')!=instance or m.get('start')!=t or m.get('end')!=t+WINDOW or not m.get('indexed'):
                raise RuntimeError('source_manifest_identity_mismatch')
            found.append((path,m))
        return found,dict(complete=not missing,expected_windows=len(found)+len(missing),
                          collected_windows=len(found),missing_windows=missing[:20],missing_count=len(missing),
                          records=sum(m['records'] for _,m in found),source='dds_slow_records')

    @staticmethod
    def width(start,end):
        return max(MINUTE,math.ceil((end-start)/MINUTE/180)*MINUTE)

    def read(self, instance, start, end, *, role='', namespace='', command='', kind='', width=None):
        manifests,coverage=self.manifests(instance,start,end)
        width=width or self.width(start,end)
        if not manifests:
            return [],coverage
        # Partial edge minutes are deliberately excluded from exact comparisons.
        lo=((start+MINUTE-1)//MINUTE)*MINUTE
        hi=end//MINUTE*MINUTE
        coverage['excluded_partial_minutes']=int(lo!=start)+int(hi!=end)
        sums=('count','failed','collscan','spill','overlap_us')+COSTS+tuple(k+'_known' for k in COSTS)
        sample_expression='argMax(src.sample,tuple(src.max_us,src.sample))' if self.backend=='clickhouse' else 'arg_max(src.sample,row(src.max_us,src.sample))'
        selection=[f'CAST(floor(src.bucket / {width}) * {width} AS BIGINT) AS bucket','src.role AS role','src.group_id AS group_id','min(src.profile) AS profile',
                   sample_expression+' AS sample','max(src.max_us) AS max_us','min(src.first_us) AS first_us','max(src.last_us) AS last_us']
        selection += [f'sum(src.{k}) AS {k}' for k in sums]
        if self.backend=='clickhouse':
            revisions=[m['revision'] for _,m in manifests]
            if any(len(v)!=64 or any(c not in '0123456789abcdef' for c in v) for v in revisions):
                raise ValueError('invalid_revision')
            scope='SELECT * FROM '+self.table('rollups')+' FINAL WHERE instance={instance:String} AND revision IN ('+','.join("'"+v+"'" for v in revisions)+')'
            params={'instance':instance,'lo':lo,'hi':hi}
            settings={'max_execution_time':10,'max_memory_usage':300_000_000,'max_query_size':1_000_000,'enable_positional_arguments':1}
            counts=self.client.json_rows('SELECT revision,count() AS n FROM ('+scope+') GROUP BY revision',parameters=params,settings=settings,timeout=15)
            indexed={r['revision']:int(r['n']) for r in counts}
            gaps=[m['start'] for _,m in manifests if indexed.get(m['revision'],0)!=m['rollup_rows']]
            if gaps:
                coverage.update(complete=False,index_missing_windows=gaps[:20])
                LOGGER.warning('mongo_query incomplete_index: instance=%s windows=%s',instance,len(gaps))
            scope+=' AND bucket >= {lo:Int64} AND bucket < {hi:Int64}'
            if role:
                scope+=' AND role={role:String}';params['role']=role
            sql='SELECT '+','.join(selection)+' FROM ('+scope+') AS src GROUP BY 1,2,3 LIMIT '+str(MAX_ROWS+1)
            rows=self.client.json_rows(sql,parameters=params,settings=settings,timeout=15)
        else:
            files=[str(p.parent/m['rollup']) for p,m in manifests if m['rollup_rows']]
            if not files:return [],coverage
            with duckdb.connect() as db:
                db.execute("SET threads=1; SET memory_limit='256MB'")
                sql='SELECT '+','.join(selection)+' FROM read_parquet(?) AS src WHERE src.bucket >= ? AND src.bucket < ?'
                params=[files,lo,hi]
                if role:sql+=' AND role=?';params.append(role)
                sql+=' GROUP BY ALL LIMIT '+str(MAX_ROWS+1)
                timer=threading.Timer(12,db.interrupt);timer.daemon=True;timer.start()
                try:
                    cursor=db.execute(sql,params)
                    names=[c[0] for c in cursor.description]
                    rows=[dict(zip(names,row)) for row in cursor.fetchall()]
                finally:timer.cancel()
        if len(rows)>MAX_ROWS:
            LOGGER.warning('mongo_query unavailable: aggregate_budget_exceeded')
            raise RuntimeError('aggregate_budget_exceeded: narrow the window or scope')
        selected=[]
        for row in rows:
            profile=json.loads(row['profile'])
            if namespace and namespace not in profile['namespace']:continue
            if command and command!=profile['command']:continue
            if kind and kind!=profile['kind']:continue
            selected.append(row)
        return selected,coverage

    def telemetry(self, instance, kind, points):
        if not points:return
        rows=[]
        for p in points:
            identity={k:p.get(k) for k in ('timestamp','role','node','metric','period')}
            rows.append(dict(instance=instance,kind=kind,id=p.get('identity') or digest(identity),timestamp=int(p['timestamp']),
                             role=str(p.get('role','Unknown')),node=str(p.get('node','')),payload=canonical(p)))
        directory=self.base(instance)/'telemetry'/kind
        directory.mkdir(parents=True,exist_ok=True)
        file=directory/(str(min(r['timestamp'] for r in rows))+'-'+digest(rows)+'.parquet')
        if not file.exists():atomic_parquet(pa.Table.from_pylist(rows),file)
        if self.backend=='clickhouse':
            for i in range(0,len(rows),200):self.client.insert_json_rows(self.table('telemetry'),rows[i:i+200],timeout=15)

    def read_telemetry(self, instance, kind, start, end, *, metric='', compact=False):
        if self.backend=='clickhouse':
            select="concat('{\"interval\":', JSONExtractRaw(src.payload,'interval'), ',\"timestamp\":', toString(src.timestamp), ',\"node\":', toJSONString(src.node), ',\"role\":', toJSONString(src.role), '}') AS payload" if compact else 'src.payload AS payload'
            predicate=" AND JSONExtractString(payload,'metric')={metric:String}" if metric else ''
            rows=self.client.json_rows('SELECT '+select+' FROM (SELECT * FROM '+self.table('telemetry')+
                ' FINAL WHERE instance={instance:String} AND kind={kind:String} AND timestamp >= {start:Int64} AND timestamp <= {end:Int64}'+predicate+') AS src LIMIT 100001',
                parameters={'instance':instance,'kind':kind,'start':start//1000,'end':end//1000,'metric':metric},timeout=10)
        else:
            files=list((self.base(instance)/'telemetry'/kind).glob('*.parquet'))
            if not files:return []
            with duckdb.connect() as db:
                rows=[{'payload':r[0]} for r in db.execute('SELECT arg_max(payload,timestamp) FROM read_parquet(?) WHERE timestamp BETWEEN ? AND ? GROUP BY id LIMIT 100001',
                                                        [[str(f) for f in files],start//1000,end//1000]).fetchall()]
        if len(rows)>100000:raise RuntimeError('telemetry_budget_exceeded')
        values=[json.loads(r['payload']) for r in rows]
        return [v for v in values if not metric or v.get('metric')==metric]

    def latest_native(self, instance, end):
        if self.backend=='clickhouse':
            rows=self.client.json_rows('SELECT argMax(payload,timestamp) AS payload FROM '+self.table('telemetry')+
                " FINAL WHERE instance={instance:String} AND kind='native' AND timestamp <= {end:Int64} AND timestamp >= {start:Int64} GROUP BY node LIMIT 10",
                parameters={'instance':instance,'end':end//1000,'start':(end-120*MINUTE)//1000},timeout=5)
            return [json.loads(r['payload']) for r in rows]
        values=self.read_telemetry(instance,'native',end-120*MINUTE,end)
        latest={}
        for value in sorted(values,key=lambda v:v['timestamp']):latest[value['node']]=value
        return list(latest.values())


def main():
    from .config import data_root
    p=argparse.ArgumentParser();p.add_argument('--data-dir',type=Path,default=data_root());p.add_argument('--migrate',action='store_true')
    a=p.parse_args();store=MongoStore(a.data_dir)
    if a.migrate:store.migrate()
    store.check();print('Mongo insight schema ready')


if __name__=='__main__':main()
