"""Bounded additions to the existing Redis history database, not another collector."""
import hashlib
import json
import time
import zlib
from collections import defaultdict

RETENTION = {'node':30,'keys':90,'queues':90,'profile':30,'sources':0,'config':0,'event':0,'cloud_event':0,'capability':90,'night':400,'aggregate':7}


def encode(v):
    return zlib.compress(json.dumps(v,separators=(',',':'),ensure_ascii=False).encode(),1)


def decode(v):
    return json.loads(zlib.decompress(v)) if isinstance(v,bytes) else json.loads(v)


class EvidenceHistory:
    def __init__(self, service):
        self.service=service
        self.last_compact=0
        with service.db() as c:
            c.executescript('''
                CREATE TABLE IF NOT EXISTS redis_evidence(instance TEXT,kind TEXT,node TEXT,ts REAL,id TEXT,payload BLOB,PRIMARY KEY(instance,kind,node,ts,id));
                CREATE INDEX IF NOT EXISTS redis_evidence_time ON redis_evidence(instance,kind,ts,node);
                CREATE TABLE IF NOT EXISTS redis_rollups(instance TEXT,node TEXT,period INTEGER,ts INTEGER,payload BLOB,PRIMARY KEY(instance,node,period,ts));
                CREATE INDEX IF NOT EXISTS redis_rollup_time ON redis_rollups(instance,period,ts,node);
                CREATE TABLE IF NOT EXISTS redis_maintenance(instance TEXT,name TEXT,watermark REAL,PRIMARY KEY(instance,name));
            ''')

    def put(self, instance, kind, node, ts, value, identity=''):
        if kind not in RETENTION:raise ValueError('unknown_redis_evidence_kind')
        with self.service.db() as c:
            c.execute('INSERT OR REPLACE INTO redis_evidence VALUES(?,?,?,?,?,?)',(instance,kind,node,ts,identity,encode(value)))

    def latest(self, instance, kind, before=None, node=''):
        with self.service.db() as c:
            r=c.execute('SELECT ts,payload FROM redis_evidence WHERE instance=? AND kind=? AND node=? AND ts<=? ORDER BY ts DESC LIMIT 1',(instance,kind,node,before if before is not None else self.service.clock())).fetchone()
        return {'time':r['ts'],'data':decode(r['payload'])} if r else None

    def rows(self,instance,kind,start,end,node='',limit=200):
        filt,args=(' AND node=?',[node]) if node else ('',[])
        with self.service.db() as c:
            records=c.execute('SELECT ts,node,payload FROM redis_evidence WHERE instance=? AND kind=? AND ts>=? AND ts<?'+filt+' ORDER BY ts DESC LIMIT ?', [instance,kind,start,end]+args+[limit+1]).fetchall()
        return {'rows':[{'time':r['ts'],'node':r['node'],'data':decode(r['payload'])} for r in records[:limit]],'truncated':len(records)>limit}

    def changed(self,instance,kind,node,now,value,event_type):
        previous=self.latest(instance,kind,node=node)
        if previous and previous['data']==value:return False
        self.put(instance,kind,node,now,value)
        if previous:
            event={'type':event_type,'before':previous['data'],'after':value,'observed_at':now,'source':'snapshot_diff','exact_change_time':False,'previous_observed_at':previous['time']}
            self.put(instance,'event',node,now,event,hashlib.sha256(encode(event)).hexdigest())
        return True

    def compact(self,force=False):
        now=self.service.clock()
        if not force and now-self.last_compact<300:return
        self.last_compact=now
        for entry in self.service.entries:
            instance=entry['instanceId'];end=int((now-120)//60)*60
            with self.service.db() as c:
                row=c.execute('SELECT watermark FROM redis_maintenance WHERE instance=? AND name=?',(instance,'minute')).fetchone()
                if row:start=int(row[0])
                else:
                    first=c.execute('SELECT start FROM intervals WHERE instance=? ORDER BY start LIMIT 1',(instance,)).fetchone()
                    start=int((first[0] if first else end)//60)*60
                # Up to six hours per pass; never scan all 30 days on each poll.
                end=min(end,start+21600)
                records=c.execute('SELECT node,start,end,payload,reason FROM intervals WHERE instance=? AND end>=? AND end<? ORDER BY end LIMIT 12001',(instance,start,end)).fetchall()
                if len(records)>12000:raise RuntimeError('redis_rollup_input_budget')
                groups=defaultdict(list);snapshots=defaultdict(list)
                for r in records:
                    if not r['reason']:groups[(r['node'],int(r['end']//60)*60)].append(r)
                snap_rows=c.execute('SELECT node,ts,payload FROM snapshots WHERE instance=? AND ts>=? AND ts<? ORDER BY ts LIMIT 12001',(instance,start,end)).fetchall()
                if len(snap_rows)>12000:raise RuntimeError('redis_rollup_snapshot_budget')
                for snap in snap_rows:
                    key=(snap['node'],int(snap['ts']//60)*60);snapshots[key].append(snap);groups.setdefault(key,[])
                for (node,stamp),rows in groups.items():
                    payload={'commands':{},'seconds':0,'process_cpu_seconds':0,'process_cpu_known_seconds':0,'counter_deltas':{},'counter_known_seconds':{},'cpu_components':{},'gauges':{},'min_start':min((r['start'] for r in rows),default=stamp),'max_end':max((r['end'] for r in rows),default=stamp+60),'scope':'interval_end_bucket_no_proportional_split'}
                    for snap in snapshots[(node,stamp)]:
                        raw=decode(snap['payload']);t=raw.get('telemetry',{})
                        payload['telemetry_snapshot']={'time':snap['ts'],**t,'cpu_cumulative':raw.get('cpu',{}),'uptime':raw.get('uptime'),'identity_method':raw.get('identity_method')}
                        for field,value in t.get('gauges',{}).items():
                            g=payload['gauges'].setdefault(field,{'min':value,'max':value,'last':value,'sum':0,'count':0})
                            g.update(min=min(g['min'],value),max=max(g['max'],value),last=value,sum=g['sum']+value,count=g['count']+1)
                    for r in rows:
                        d=decode(r['payload']);seconds=r['end']-r['start'];payload['seconds']+=seconds
                        if d.get('process_cpu_percent') is not None:
                            payload['process_cpu_seconds']+=d['process_cpu_percent']/100*seconds;payload['process_cpu_known_seconds']+=seconds
                        for field,value in d.get('derived',{}).get('counter_deltas',{}).items():
                            payload['counter_deltas'][field]=payload['counter_deltas'].get(field,0)+value
                            payload['counter_known_seconds'][field]=payload['counter_known_seconds'].get(field,0)+seconds
                        for field in ('user_cpu_seconds','sys_cpu_seconds'):
                            value=d.get('derived',{}).get(field)
                            if value is not None:
                                part=payload['cpu_components'].setdefault(field,{'seconds':0,'known_seconds':0});part['seconds']+=value;part['known_seconds']+=seconds
                        for cmd,cost in d['commands'].items():
                            dst=payload['commands'].setdefault(cmd,{'calls':0,'usec':0,'seconds':0})
                            for k in ('calls','usec'):dst[k]+=cost[k]
                            dst['seconds']+=seconds
                            for field in ('rejected_calls','failed_calls'):
                                if field in cost:
                                    dst[field]=dst.get(field,0)+cost[field];dst[field+'_known_seconds']=dst.get(field+'_known_seconds',0)+seconds
                    c.execute('INSERT OR REPLACE INTO redis_rollups VALUES(?,?,?,?,?)',(instance,node,60,stamp,encode(payload)))
                if end>start:c.execute('INSERT OR REPLACE INTO redis_maintenance VALUES(?,?,?)',(instance,'minute',end))
                # Rebuild only closed recent hours, plus hours just compacted on catch-up.
                prior=c.execute('SELECT watermark FROM redis_maintenance WHERE instance=? AND name=?',(instance,'hour')).fetchone()
                earliest=c.execute('SELECT ts FROM redis_rollups WHERE instance=? AND period=60 ORDER BY ts LIMIT 1',(instance,)).fetchone()
                lo=int(prior[0]) if prior else int(earliest[0]//3600)*3600 if earliest else end//3600*3600
                hour_end=min(int((now-120)//3600)*3600,end//3600*3600,lo+21600)
                if lo<hour_end:
                    minutes=c.execute('SELECT node,ts,payload FROM redis_rollups WHERE instance=? AND period=60 AND ts>=? AND ts<? ORDER BY ts LIMIT 26001',(instance,lo,hour_end)).fetchall()
                    if len(minutes)>26000:raise RuntimeError('redis_hour_rollup_budget')
                    hours=defaultdict(list)
                    for m in minutes:hours[(m['node'],m['ts']//3600*3600)].append(decode(m['payload']))
                    for (node,stamp),parts in hours.items():
                        total={'commands':{},'seconds':sum(p['seconds'] for p in parts),'process_cpu_seconds':sum(p['process_cpu_seconds'] for p in parts),'process_cpu_known_seconds':sum(p['process_cpu_known_seconds'] for p in parts),'min_start':min(p['min_start'] for p in parts),'max_end':max(p['max_end'] for p in parts),'counter_deltas':{},'counter_known_seconds':{},'cpu_components':{},'gauges':{},'scope':'interval_end_bucket_no_proportional_split'}
                        for part in parts:
                            if part.get('telemetry_snapshot'):total['telemetry_snapshot']=part['telemetry_snapshot']
                            for name in ('counter_deltas','counter_known_seconds'):
                                for field,value in part.get(name,{}).items():total[name][field]=total[name].get(field,0)+value
                            for field,piece in part.get('cpu_components',{}).items():
                                target=total['cpu_components'].setdefault(field,{'seconds':0,'known_seconds':0})
                                for k in target:target[k]+=piece[k]
                            for field,g in part.get('gauges',{}).items():
                                target=total['gauges'].setdefault(field,{'min':g['min'],'max':g['max'],'last':g['last'],'sum':0,'count':0})
                                target.update(min=min(target['min'],g['min']),max=max(target['max'],g['max']),last=g['last'],sum=target['sum']+g['sum'],count=target['count']+g['count'])
                            for cmd,cost in part['commands'].items():
                                target=total['commands'].setdefault(cmd,{'calls':0,'usec':0,'seconds':0})
                                for k,value in cost.items():target[k]=target.get(k,0)+value
                        c.execute('INSERT OR REPLACE INTO redis_rollups VALUES(?,?,?,?,?)',(instance,node,3600,stamp,encode(total)))
                    c.execute('INSERT OR REPLACE INTO redis_maintenance VALUES(?,?,?)',(instance,'hour',hour_end))
                # Longer-tier evidence is retained only once it has been successfully generated.
                for kind,days in RETENTION.items():
                    if days:c.execute('DELETE FROM redis_evidence WHERE rowid IN (SELECT rowid FROM redis_evidence WHERE instance=? AND kind=? AND ts<? LIMIT 20000)',(instance,kind,now-days*86400))
                for period,days in [(60,90),(3600,400)]:
                    c.execute('DELETE FROM redis_rollups WHERE rowid IN (SELECT rowid FROM redis_rollups WHERE instance=? AND period=? AND ts<? LIMIT 20000)',(instance,period,now-days*86400))

    def rollups(self,instance,start,end,period,node=''):
        if period not in (60,3600,86400):raise ValueError('invalid_redis_rollup_period')
        filt,args=(' AND node=?',[node]) if node else ('',[])
        if period==86400:
            groups={};count=0
            with self.service.db() as c:
                for r in c.execute('SELECT node,ts,payload FROM redis_rollups WHERE instance=? AND period=3600 AND ts>=? AND ts+3600<=?'+filt+' ORDER BY ts LIMIT 60001',[instance,start,end]+args):
                    count+=1
                    if count>60000:break
                    p=decode(r['payload']);day=int((r['ts']+8*3600)//86400)*86400-8*3600
                    target=groups.setdefault((r['node'],day),{'node':r['node'],'time':day,'period':86400,'commands':{},'process_cpu_seconds':0,'process_cpu_known_seconds':0,'seconds':0,'scope':'daily_view_from_observed_hour_intervals; partial edge day possible'})
                    for k in ('process_cpu_seconds','process_cpu_known_seconds','seconds'):target[k]+=p[k]
                    for cmd,cost in p['commands'].items():
                        dest=target['commands'].setdefault(cmd,{})
                        for k,v in cost.items():dest[k]=dest.get(k,0)+v
                    if p.get('telemetry_snapshot'):target['telemetry_snapshot']=p['telemetry_snapshot']
            return {'rows':list(groups.values()),'truncated':count>60000,'scope':'daily_view_from_hours; CPU is weighted by observed seconds; gauges and percentiles use last snapshot'}
        with self.service.db() as c:
            rows=c.execute('SELECT node,ts,payload FROM redis_rollups WHERE instance=? AND period=? AND ts>=? AND ts+?<=?'+filt+' ORDER BY ts LIMIT 10001',[instance,period,start,period,end]+args).fetchall()
        return {'rows':[{'node':r['node'],'time':r['ts'],'period':period,**decode(r['payload'])} for r in rows[:10000]],'truncated':len(rows)>10000,'scope':'intervals_grouped_by_end_not_reconstructed_instantaneous_events'}
