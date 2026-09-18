"""Read-only DDS/CMS/native Mongo collectors; no profiling or parameter changes."""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from .credentials import load_credential
from .rds_api import RdsRpcClient
from .slow_log_collector import DasRpcClient
from .mongo_insight import COMMANDS, MINUTE, TOP_NAMESPACES, WINDOW, collection_interval, counter_interval, epoch_us
from .mongo_store import atomic_json

LOGGER=logging.getLogger(__name__)
METRICS=('CPUUtilization','MemoryUtilization','ConnectionAmount','IOPSUtilization','ReplicationLag',
         'QPS','ScannedDocs','ScannedKeys','ReadIops','WriteIops','AvgRt','ReadAvgRt','WriteAvgRt',
         'WtCacheUsage','WtCacheDirtyUsage','ConcurrentReads','ConcurrentWrites','CentralCacheFree','TcmallocCacheMemRatio')


class DdsClient(RdsRpcClient):
    VERSION='2015-12-01'


class CmsClient(DasRpcClient):
    VERSION='2019-01-01'


def iso(t):
    return datetime.fromtimestamp(t/1e6,timezone.utc).strftime('%Y-%m-%dT%H:%MZ')


def bson_epoch_us(value):
    # Database.command defaults to naive UTC, independently of client codecs.
    return epoch_us(value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value)


def load_instances(root: Path):
    path=root/'mongo-instances.json'
    if not path.exists():
        LOGGER.warning('mongo_insight disabled: mongo-instances.json absent')
        return []
    value=json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(value,list):raise ValueError('mongo_instances_array_required')
    seen=set()
    for item in value:
        instance=item.get('instanceId','')
        if not re.fullmatch(r'dds-[a-z0-9-]+',instance) or instance in seen:raise ValueError('invalid_or_duplicate_mongo_instance')
        seen.add(instance)
        region=item.get('region','')
        if not re.fullmatch(r'[a-z]+-[a-z]+(?:-\d+)?',region):raise ValueError('invalid_mongo_region')
        for host in item.get('nodes',[]):
            if not re.fullmatch(re.escape(instance)+r'\d+\.mongodb\.'+re.escape(region)+r'\.rds\.aliyuncs\.com',host):
                raise ValueError('mongo_node_outside_instance_allowlist')
        if item.get('port',3717)!=3717:raise ValueError('mongo_port_not_allowlisted')
        if not isinstance(item.get('families',[]),list) or any(not re.fullmatch(r'[A-Za-z0-9_-]+',p) for p in item.get('families',[])):
            raise ValueError('invalid_mongo_family_registry')
        limit=item.get('topNamespaceLimit',TOP_NAMESPACES)
        if not isinstance(limit,int) or isinstance(limit,bool) or not 1<=limit<=1000:
            raise ValueError('invalid_mongo_top_namespace_limit')
    return value


class MongoCollector:
    SLOWLOG_PAGE_TIMEOUT = 12
    # DDS documents 30 DescribeSlowLogRecords requests/minute. Share this gate
    # between realtime and history lanes; do not speed up by adding readers.
    SLOWLOG_REQUEST_INTERVAL = 2.1
    SLOWLOG_MAX_PAGES = 500

    def __init__(self, store, entry, settings_loader, archive_loader=None):
        self.store=store;self.entry=entry;self.instance=entry['instanceId']
        self.settings_loader=settings_loader;self.archive_loader=archive_loader
        self.stop_event=threading.Event();self.threads=[];self.clients={};self.before={};self.unsupported_metrics={}
        self.history_retry={}
        self.slow_request_lock=threading.Lock();self.slow_next_request=0.0
        self.state={'instanceId':self.instance,'label':entry.get('label',self.instance),'enabled':entry.get('enabled',False),
                    'slowlog':'not_started','metrics':'not_started','counters':'not_started','client_aggregates':'not_connected'}

    def rpc(self, cms=False):
        settings=self.settings_loader()
        credential=load_credential(settings.credential_target)
        if credential is None:raise RuntimeError('cloud_credential_unavailable')
        endpoint='https://metrics.'+self.entry['region']+'.aliyuncs.com' if cms else 'https://mongodb.aliyuncs.com'
        return (CmsClient if cms else DdsClient)(replace(settings,db_instance_id=self.instance,region_id=self.entry['region'],endpoint=endpoint),credential,timeout=self.SLOWLOG_PAGE_TIMEOUT)

    def _slow_page(self,client,params):
        with self.slow_request_lock:
            delay=max(0.0,self.slow_next_request-time.monotonic())
            if self.stop_event.wait(delay):raise RuntimeError('collector_stopping')
            self.slow_next_request=time.monotonic()+self.SLOWLOG_REQUEST_INTERVAL
            return client.call('DescribeSlowLogRecords',params)

    def slow_window(self,start,end):
        client=self.rpc();records=[];total=None;begin=time.monotonic();deadline=begin+240
        for page in range(1,self.SLOWLOG_MAX_PAGES+1):
            if self.stop_event.is_set():raise RuntimeError('collector_stopping')
            if time.monotonic()>deadline:raise RuntimeError('slowlog_window_deadline')
            r=self._slow_page(client,{'DBInstanceId':self.instance,'StartTime':iso(start),'EndTime':iso(end),
                          'PageSize':100,'PageNumber':page,'OrderType':'asc'})
            count=int(r.get('TotalRecordCount',-1))
            if count<0 or (total is not None and count!=total):raise RuntimeError('source_changed_during_pagination')
            if total is None:
                pages=max(1,(count+99)//100)
                if pages>self.SLOWLOG_MAX_PAGES:raise RuntimeError('slowlog_page_limit_or_count_mismatch')
                # A dense window cannot fit the old fixed 240s budget even when
                # every request succeeds. Budget all pages plus API pacing;
                # retain per-request timeout, finite page cap and stop checks.
                deadline=begin+max(240,pages*(self.SLOWLOG_PAGE_TIMEOUT+self.SLOWLOG_REQUEST_INTERVAL)+30)
            total=count
            batch=r.get('Items',{}).get('LogRecords',[])
            if not isinstance(batch,list):raise RuntimeError('invalid_slowlog_page')
            records.extend(batch)
            progress=dict(start_us=start,end_us=end,page=page,records=len(records),total=total,
                          elapsed_seconds=round(time.monotonic()-begin,2))
            self.state['slowlog_fetch_progress']=progress
            if page==1 or page%10==0 or len(records)>=total:
                LOGGER.info('mongo_slowlog pagination: instance=%s window=%s page=%s records=%s total=%s elapsed=%s',
                            self.instance,start,page,len(records),total,progress['elapsed_seconds'])
            if len(records)>=total:break
            if not batch:raise RuntimeError('incomplete_slowlog_pagination')
        if len(records)!=total:raise RuntimeError('slowlog_page_limit_or_count_mismatch')
        # Provider time endpoints can be inclusive; our ownership is [start,end).
        records=[r for r in records if start<=epoch_us(r['ExecutionStartTime'])<end]
        archive=self.archive_loader(self.settings_loader()) if self.archive_loader else None
        return self.store.publish(self.instance,start,end,records,self.entry.get('families',[]),archive=archive)

    def cloud_window(self,start,end,*,state_key='metrics_unavailable'):
        client=self.rpc(cms=True);points=[]
        unavailable={}
        for metric in METRICS:
            if self.unsupported_metrics.get(metric,0)>time.time():
                unavailable[metric]='provider_unsupported_cached'
                LOGGER.warning('mongo_metric unavailable: instance=%s metric=%s provider_unsupported_cached',self.instance,metric)
                continue
            token=None;seen=set();got=0;unsupported=False
            for page in range(50):
                params={'Namespace':'acs_mongodb','MetricName':metric,'Dimensions':json.dumps([{'instanceId':self.instance}]),
                        'StartTime':str(start//1000),'EndTime':str(end//1000),'Period':'60','Length':'1000'}
                if token:params['NextToken']=token
                try:r=client.call('DescribeMetricList',params)
                except RuntimeError as exc:
                    if re.search(r'metric.*is not exist',str(exc),re.I):
                        self.unsupported_metrics[metric]=time.time()+6*3600
                        unavailable[metric]='provider_unsupported'
                        LOGGER.warning('mongo_metric unavailable: instance=%s metric=%s provider_unsupported',self.instance,metric)
                        unsupported=True;break
                    raise
                if str(r.get('Code'))!='200':raise RuntimeError('cms_error:'+str(r.get('Code')))
                rows=json.loads(r.get('Datapoints') or '[]')
                for row in rows:
                    if row.get('instanceId')!=self.instance:raise RuntimeError('cms_instance_identity_mismatch')
                    if row.get('Average') is None:continue
                    points.append(dict(timestamp=int(row['timestamp']),role=row.get('role','Unknown'),node=row.get('nodeNum',''),
                                       metric=metric,value=row['Average'],period=60,source='cms',timestamp_semantics='period_end'))
                    got+=1
                token=r.get('NextToken')
                if not token:break
                if token in seen:raise RuntimeError('cms_pagination_cycle')
                seen.add(token)
            else:raise RuntimeError('cms_page_limit')
            if not got and not unsupported:
                unavailable[metric]='no_points'
                LOGGER.warning('mongo_metric unavailable: instance=%s metric=%s no_points',self.instance,metric)
        self.state[state_key]=unavailable
        if not points:raise RuntimeError('cms_no_points')
        self.store.telemetry(self.instance,'metrics',points)
        return len(points)

    def namespace_totals(self, host):
        """Cumulative per-namespace lock counters from `top`.

        A failure here degrades the namespace lane to "no interval" and leaves the native
        counter lane intact; it never produces a partial or wrong difference.
        """
        try:
            totals=self.clients[host].admin.command({'top':1}).get('totals',{})
        except Exception as exc:
            self.state['top:'+host]=type(exc).__name__
            LOGGER.warning('mongo_top unavailable: endpoint=%s reason=%s; namespace lane skipped this cycle',
                           host,type(exc).__name__+':'+str(exc)[:160])
            return {}
        rows={}
        for namespace,value in totals.items():
            if namespace=='note' or not isinstance(value,dict):continue
            read=value.get('readLock') or {}
            write=value.get('writeLock') or {}
            rows[namespace]=dict(read_count=int(read.get('count',0)),read_us=int(read.get('time',0)),
                                 write_count=int(write.get('count',0)),write_us=int(write.get('time',0)))
        if not rows:LOGGER.warning('mongo_top empty_totals: endpoint=%s',host)
        self.state.pop('top:'+host,None)
        return rows

    def publish_namespaces(self, host, retained):
        # Called before self.before is advanced, so this differences against the prior sample.
        if not retained.get('collections'):return
        interval=collection_interval(self.before.get(host),retained,self.entry.get('families',[]),
                                     limit=int(self.entry.get('topNamespaceLimit',TOP_NAMESPACES)))
        self.state['namespaces']=interval['status']
        if interval['status']!='ok':return
        point=dict(interval,metric='top',timestamp=retained['timestamp'],node=retained['node'],
                   endpoint=host,period=round(interval['interval_seconds']))
        self.store.telemetry(self.instance,'namespaces',[point])

    def sample_nodes(self):
        if not self.entry.get('nodes') or not self.entry.get('credentialsFile'):
            raise RuntimeError('readonly_node_credentials_not_configured')
        from pymongo import MongoClient, ReadPreference
        secret=Path(self.entry['credentialsFile'])
        if not secret.is_absolute():secret=self.store.root.parent/secret
        if os.name!='nt' and secret.stat().st_mode & 0o077:raise RuntimeError('mongo_credentials_require_0600')
        auth=json.loads(secret.read_text(encoding='utf-8'))
        approved_user=self.entry.get('readonlyUsername')
        if not approved_user or auth.get('username')!=approved_user or auth.get('authSource','admin')!='admin':
            raise RuntimeError('mongo_readonly_identity_required')
        success=0
        for host in self.entry['nodes']:
            candidate_client=None
            try:
                if host not in self.clients:
                    candidate_client=MongoClient(host=host,port=3717,username=auth['username'],password=auth['password'],
                        tls=bool(self.entry.get('tls',False)),
                        authSource='admin',authMechanism='SCRAM-SHA-256',directConnection=True,read_preference=ReadPreference.NEAREST,
                        retryWrites=False,tz_aware=True,appname='sql-insight-readonly',maxPoolSize=1,serverSelectionTimeoutMS=5000,
                        connectTimeoutMS=5000,socketTimeoutMS=10000)
                    identity=candidate_client.admin.command({'connectionStatus':1})
                    users=identity.get('authInfo',{}).get('authenticatedUsers',[])
                    roles=identity.get('authInfo',{}).get('authenticatedUserRoles',[])
                    if not any(u.get('user')==approved_user and u.get('db')=='admin' for u in users):
                        raise RuntimeError('mongo_authenticated_identity_mismatch')
                    if roles and any(r.get('role') not in {'read','readAnyDatabase','clusterMonitor'} for r in roles):
                        raise RuntimeError('mongo_identity_has_unapproved_roles')
                    self.clients[host]=candidate_client
                    candidate_client=None
                begin=time.time_ns()//1000
                s=self.clients[host].admin.command({'serverStatus':1,'opLatencies':{'histograms':True},'opWorkingTime':{'histogram':True}})
                finish=time.time_ns()//1000
                server_us=bson_epoch_us(s['localTime'])
                node=str(s['host'])
                repl=s.get('repl',{})
                role='Primary' if repl.get('isWritablePrimary',repl.get('ismaster')) else 'Secondary' if repl.get('secondary') else 'Unknown'
                uptime_ms=int(s.get('uptimeMillis',s.get('uptime',0)*1000))
                epoch=str(s.get('pid',''))+':'+str(round((server_us-uptime_ms*1000)/10_000_000))
                commands={name:{k:int(v[k]) for k in ('total','failed','rejected') if k in v}
                          for name,v in s.get('metrics',{}).get('commands',{}).items() if name in COMMANDS and isinstance(v,dict) and 'total' in v}
                sample=dict(node=node,endpoint=host,role=role,epoch=epoch,time_us=(begin+finish)//2,timestamp=(begin+finish)//2000,
                            server_time_us=server_us,server_time_source='bson_utc',clock_skew_us=server_us-(begin+finish)//2,rtt_us=finish-begin,commands=commands,
                            mem=s.get('mem',{}),connections=s.get('connections',{}),global_lock=s.get('globalLock',{}),
                            op_latencies=s.get('opLatencies',{}),op_working_time=s.get('opWorkingTime',{}),
                            locks=s.get('locks',{}),tcmalloc=s.get('tcmalloc',{}),
                            wt_cache=s.get('wiredTiger',{}).get('cache',{}),
                            cursor=s.get('metrics',{}).get('cursor',{}),flow_control=s.get('flowControl',{}),
                            transactions=s.get('transactions',{}),opcounters=s.get('opcounters',{}),
                            repl_counters=s.get('opcountersRepl',{}))
                # `top` totals stay out of the persisted native payload: they are cumulative
                # per-namespace counters kept in memory only to difference the next sample.
                retained=dict(sample,collections=self.namespace_totals(host))
                delta=counter_interval(self.before.get(host),retained)
                if abs(sample['clock_skew_us'])>30_000_000:
                    LOGGER.warning('mongo_clock_skew: node=%s skew_us=%s; native counters use collector interval time',node,sample['clock_skew_us'])
                sample['interval']=delta
                self.store.telemetry(self.instance,'native',[sample])
                self.publish_namespaces(host,retained)
                self.before[host]=retained
                success+=1
            except Exception as exc:
                if candidate_client is not None:candidate_client.close()
                LOGGER.warning('mongo_node_sample unavailable: endpoint=%s reason=%s',host,type(exc).__name__+':'+str(exc)[:300])
                self.state['node:'+host]=type(exc).__name__
        if success!=len(self.entry['nodes']):raise RuntimeError('incomplete_node_sampling')
        return success

    def slow_tick(self):
        end=(int(time.time()*1e6)-180_000_000)//WINDOW*WINDOW
        checkpoint=self.store.base(self.instance)/'slow-checkpoint.json'
        start=int(json.loads(checkpoint.read_text())['next']) if checkpoint.exists() else end-3*WINDOW
        # Catch up forward first. One completed small window is a progress unit.
        if start<end:
            m=self.slow_window(start,start+WINDOW)
            atomic_json(checkpoint,{'next':start+WINDOW})
            self.state['slowlog_window']=m['end'];return m['records']
        # Refresh closed but recently completed windows for delayed provider logs.
        self.state['slowlog_window']=end
        t=end-(1+int(time.time()//60)%3)*WINDOW
        return self.slow_window(t,t+WINDOW)['records']

    def history_bounds(self):
        end=(int(time.time()*1e6)-180_000_000)//WINDOW*WINDOW
        path=self.store.base(self.instance)/'history-target.json'
        with self.store.lock:
            if not path.exists():atomic_json(path,{'start':end-48*60*MINUTE})
            start=min(int(json.loads(path.read_text())['start']),end-48*60*MINUTE)
        if start<end-7*24*60*MINUTE:
            LOGGER.warning('mongo_history expired_target: instance=%s incomplete recovery exceeded seven-day budget',self.instance)
            start=end-7*24*60*MINUTE
        return start,end

    def history_ready(self):
        _,end=self.history_bounds()
        checkpoint=self.store.base(self.instance)/'slow-checkpoint.json'
        if not checkpoint.exists() or int(json.loads(checkpoint.read_text())['next'])<end-WINDOW:
            LOGGER.warning('mongo_history paused: realtime_slowlog_catching_up instance=%s',self.instance)
            self.state['history_pause']='realtime_slowlog_catching_up'
            return False
        self.state['history_pause']=''
        return True

    def history_slow_tick(self):
        # Discover missing immutable windows; never rewind the live checkpoint.
        if not self.history_ready():return 0
        start,end=self.history_bounds()
        found,coverage=self.store.manifests(self.instance,start,end)
        self.state['history_slow_progress']={k:coverage[k] for k in ('collected_windows','expected_windows','missing_count')}
        existing={m['start'] for _,m in found}
        if not coverage['missing_count'] and self.state.get('history_metric_progress',{}).get('missing_hours')==0:
            atomic_json(self.store.base(self.instance)/'history-target.json',{'start':end-48*60*MINUTE})
        for t in range(end-4*WINDOW,start-WINDOW,-WINDOW):
            if t in existing or self.history_retry.get(('slow',t),0)>time.time():continue
            self.state['history_slow_window']=t
            try:m=self.slow_window(t,t+WINDOW)
            except Exception:
                self.history_retry[('slow',t)]=time.time()+600
                raise
            return m['records']
        return 0

    def history_metric_tick(self):
        if not self.history_ready():return 0
        hour=60*MINUTE
        start,end=self.history_bounds();start=start//hour*hour;end=end//hour*hour
        path=self.store.base(self.instance)/'history-metrics.json'
        done=json.loads(path.read_text()) if path.exists() else {}
        pending=[t for t in range(end-hour,start-hour,-hour) if str(t) not in done]
        self.state['history_metric_progress']=dict(collected_hours=(end-start)//hour-len(pending),expected_hours=(end-start)//hour,missing_hours=len(pending))
        for t in pending:
            if self.history_retry.get(('metrics',t),0)>time.time():continue
            self.state['history_metric_hour']=t
            try:n=self.cloud_window(t,t+hour,state_key='history_metrics_unavailable')
            except Exception:
                self.history_retry[('metrics',t)]=time.time()+600
                raise
            # This records completed provider fetches, not guaranteed metric coverage.
            done[str(t)]=dict(points=n,fetched_at=time.time())
            atomic_json(path,done)
            return n
        return 0

    def _loop(self,name,callback,seconds):
        while not self.stop_event.is_set():
            begin=time.monotonic()
            try:
                n=callback();self.state[name]='ok';self.state[name+'_updated']=time.time();self.state[name+'_last_size']=n
            except Exception as exc:
                self.state[name]=str(exc)[:160]
                LOGGER.warning('mongo_collector unavailable: instance=%s lane=%s reason=%s',self.instance,name,type(exc).__name__+':'+str(exc)[:160])
            self.stop_event.wait(max(2,seconds-(time.monotonic()-begin)))

    def start(self):
        if not self.entry.get('enabled'):
            LOGGER.warning('mongo_collector disabled: instance=%s configuration_disabled',self.instance);return
        for name,callback,seconds in [('slowlog',self.slow_tick,30),
                ('metrics',lambda:self.cloud_window(int(time.time()*1e6)-6*MINUTE,int(time.time()*1e6)),60),
                ('counters',self.sample_nodes,60),
                ('history_slowlog',self.history_slow_tick,60),
                ('history_metrics',self.history_metric_tick,120)]:
            thread=threading.Thread(target=self._loop,args=(name,callback,seconds),daemon=True,name='mongo-'+name)
            thread.start();self.threads.append(thread)

    def shutdown(self):
        self.stop_event.set()
        for t in self.threads:t.join(timeout=2)
        for c in self.clients.values():c.close()

    def status(self):return dict(self.state)
