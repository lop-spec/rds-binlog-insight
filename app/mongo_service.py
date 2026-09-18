"""Small engine adapter plugged into the existing web application."""
from __future__ import annotations

import hmac
import json
import logging
import math
import time
from pathlib import Path

from .mongo_collector import MongoCollector, load_instances, METRICS
from .mongo_insight import analyze, MINUTE, canonical, digest, summarize_namespace_intervals, summarize_native_intervals
from .mongo_store import MongoStore
from .mongo_metrics import ANALYSIS_METRICS, lock_wait_points

LOGGER=logging.getLogger(__name__)


class MongoService:
    def __init__(self,root,settings_loader,archive_loader=None,*,start=True):
        self.root=Path(root);self.collectors=[];self.stores={};self.error='';self.entries=[]
        try:
            self.entries=load_instances(self.root)
            for entry in self.entries:
                store=MongoStore(self.root,backend=entry.get('backend','clickhouse'))
                store.check()
                self.stores[entry['instanceId']]=store
                collector=MongoCollector(store,entry,settings_loader,archive_loader)
                self.collectors.append(collector)
                if start:collector.start()
        except Exception as exc:
            self.error=type(exc).__name__+':'+str(exc)[:160]
            LOGGER.warning('mongo_service unavailable: %s; MySQL unchanged',self.error)

    def status(self):
        path=self.root/'mongo-replays.json'
        cases=json.loads(path.read_text(encoding='utf-8')) if path.exists() else []
        return dict(status='unavailable' if self.error else 'ok' if self.entries else 'not_configured',reason=self.error,
                    instances=[{'id':e['instanceId'],'label':e.get('label',e['instanceId'])} for e in self.entries],
                    collectors=[c.status() for c in self.collectors],metrics=list(ANALYSIS_METRICS),replays=cases,
                    scopes={'slowlog':'collected_records','native':'node_command_counters','client':'connected_services_only',
                            'namespaces':'namespace_lock_time_not_cpu'})

    def shutdown(self):
        for c in self.collectors:c.shutdown()

    def query(self, params):
        def get(key,default=''):
            value=params.get(key,default)
            return value[0] if isinstance(value,list) else value
        instance=get('instance')
        if instance not in self.stores:raise ValueError('configured_mongo_instance_required')
        start=int(get('startEpochUs'));end=int(get('endEpochUs'))
        if end<=start or end-start>7*86400*1_000_000:raise ValueError('mongo_window_exceeds_seven_days')
        base=int(get('baselineStart',start-86400*1_000_000))
        base_end=base+(end-start)
        if base_end>start:raise ValueError('baseline_must_not_overlap_current_window')
        role=get('role');kind=get('kind','command');command=get('command');namespace=get('namespace')
        if role not in ('','Primary','Secondary','Unknown'):raise ValueError('invalid_role')
        if kind not in ('command','suboperation',''):raise ValueError('invalid_record_kind')
        metric=get('metric','CPUUtilization')
        if metric not in (*METRICS, 'LockWaits'):raise ValueError('unsupported_mongo_metric')
        store=self.stores[instance];width=store.width(start,end)
        rows,coverage=store.read(instance,start,end,role=role,kind=kind,command=command,namespace=namespace,width=width)
        before,baseline=store.read(instance,base,base_end,role=role,kind=kind,command=command,namespace=namespace,width=width)
        optional={}
        def read_optional(name,fn):
            try:return fn()
            except Exception as exc:
                optional[name]=type(exc).__name__+':'+str(exc)[:100]
                LOGGER.warning('mongo_query %s unavailable: %s',name,optional[name]);return []
        native=read_optional('native',lambda:store.read_telemetry(instance,'native',start,end,compact=True))
        points=(lock_wait_points(native,start,end) if metric=='LockWaits' else
                read_optional('metrics',lambda:store.read_telemetry(instance,'metrics',start,end,metric=metric)))
        native_before=read_optional('native_baseline',lambda:store.read_telemetry(instance,'native',base,base_end,compact=True))
        latest=read_optional('native_latest',lambda:store.latest_native(instance,end))
        clients=read_optional('client',lambda:store.read_telemetry(instance,'client',start,end))
        # Namespace intervals carry one row per collection per minute per node; the same
        # 180-minute grain used for cost buckets bounds the aggregation here.
        if end-start<=180*MINUTE:
            namespaces=summarize_namespace_intervals(read_optional('namespaces',lambda:store.read_telemetry(instance,'namespaces',start,end)),start,end,role)
        else:
            namespaces=dict(collections=[],families=[],intervals=0,observed_seconds=0,truncated_intervals=False,
                            scope='namespace_lock_time_not_cpu',unavailable='window_exceeds_180_minutes')
            LOGGER.warning('mongo_namespace_summary unavailable: window_exceeds_180_minutes instance=%s',instance)
        result=analyze(rows,before,points,start,end,coverage=coverage['complete'],baseline_coverage=baseline['complete'],
                       metric=metric,order=get('order','correlation'),limit=int(get('limit','50')),bucket_width=width)
        counter_groups,counter_gaps=summarize_native_intervals(native,start,end,role)
        baseline_groups,baseline_gaps=summarize_native_intervals(native_before,base,base_end,role)
        for key,row in counter_groups.items():
            previous=baseline_groups.get(key)
            comparable=bool(previous and len(previous['intervals'])>=2 and len(row['intervals'])>=2)
            row.update(baseline_qps=previous['qps'] if previous else None,
                       baseline_coverage_seconds=previous['seconds'] if previous else 0,
                       qps_delta=row['qps']-previous['qps'] if comparable else None,
                       comparison_scope='observed_interval_rates' if comparable else 'insufficient_intervals')
        counter_rows=sorted(counter_groups.values(),key=lambda x:-(x['qps_delta'] if x['qps_delta'] is not None else x['qps']))
        if not counter_rows or any(r['qps_delta'] is None for r in counter_rows):
            LOGGER.warning('mongo_native_comparison unavailable: instance=%s missing_or_insufficient_observed_intervals',instance)
        result.update(instance=instance,baseline_start=base,baseline_end=base_end,coverage=coverage,baseline_coverage=baseline,
                      metric_points=points,native_counters=counter_rows,native_gaps=counter_gaps[:20],native_baseline_gaps=baseline_gaps[:20],
                      native_latest=latest,client_aggregates=clients,namespace_top=namespaces,optional_unavailable=optional,
                      collection_status=next((c.status() for c in self.collectors if c.instance==instance),{}))
        result['client_count_scope']='connected_services_only' if clients else 'not_connected'
        return result

    def authorized(self,authorization):
        token_file=self.root/'.credentials'/'mongo-ingest-token'
        if not token_file.exists():
            LOGGER.warning('mongo_client_ingest disabled: token not configured');return False
        token=token_file.read_text(encoding='utf-8').strip()
        return bool(token) and hmac.compare_digest(authorization,'Bearer '+token)

    def ingest(self,payload):
        instance=payload.get('instance')
        if instance not in self.stores:raise ValueError('configured_instance_required')
        service=str(payload.get('service',''));epoch=str(payload.get('process_epoch',''));batch=str(payload.get('batch_id',''))
        if not all(0<len(v)<=128 for v in (service,epoch,batch)):raise ValueError('service_epoch_batch_required')
        source=payload.get('rows',[])
        if not isinstance(source,list) or not 1<=len(source)<=2000:raise ValueError('client_batch_limit')
        rows=[]
        for row in source:
            lo=int(row['start_us']);hi=int(row['end_us']);count=int(row['count']);failed=int(row.get('failed',0))
            if hi<=lo or hi-lo>3600*1_000_000 or not 0<=failed<=count or count>1_000_000_000:raise ValueError('invalid_client_interval_counts')
            if abs(hi-time.time()*1e6)>7*86400*1e6:raise ValueError('client_interval_outside_retention')
            command=str(row['command']);namespace=str(row['namespace']);fingerprint=str(row.get('fingerprint',''))
            if not command or len(command)>64 or not namespace or len(namespace)>256 or len(fingerprint)>128:raise ValueError('invalid_client_dimensions')
            duration=int(row.get('duration_us',0))
            if duration<0:raise ValueError('invalid_client_duration')
            normalized=dict(service=service,process_epoch=epoch,batch_id=batch,command=command,namespace=namespace,fingerprint=fingerprint,
                            start_us=lo,end_us=hi,count=count,failed=failed,duration_us=duration,
                            lost=int(payload.get('lost',0)),scope='connected_service_command_attempts',
                            timestamp=hi//1000,role='Client',node=service+':'+epoch)
            normalized['identity']=digest([instance,service,epoch,batch,namespace,command,fingerprint])
            rows.append(normalized)
        self.stores[instance].telemetry(instance,'client',rows)
        return dict(accepted=len(rows),scope='connected_services_only',batch_id=batch)
