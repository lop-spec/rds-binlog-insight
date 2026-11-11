"""Cheap collector readiness, independent of process liveness and cloud reads."""
from __future__ import annotations
from datetime import UTC, datetime
import time
import math


def collector_health(app):
    now = time.time()
    lanes = []
    def add(kind, identity, enabled, stamp=None, error='', thread=None, budget=900):
        if not enabled:
            state = 'disabled'
        elif error:
            state = 'error'
        elif thread is not None and not thread.is_alive():
            state = 'thread_stopped'
        elif not stamp:
            state = 'warming_up'
        else:
            try:
                if isinstance(stamp,(int,float)):
                    observed=float(stamp)
                else:
                    parsed=datetime.fromisoformat(stamp.replace('Z','+00:00'))
                    if parsed.tzinfo is None:raise ValueError('timezone_required')
                    observed=parsed.timestamp()
                state = 'invalid_timestamp' if not math.isfinite(observed) or observed>now+300 else 'stale' if now-observed > budget else 'ok'
            except (ValueError, TypeError, OverflowError):
                state = 'invalid_timestamp'
        lanes.append(dict(kind=kind, instance=identity, state=state, ok=state in ('disabled','ok'), last_success=stamp))
    for kind, attr in [('general_log','general_logs'), ('slow_log','slow_logs')]:
        for c in getattr(app,attr,[]):
            state=c.status()
            add(kind,state.get('instanceId',''),state.get('enabled',False),state.get('lastPollUtc'),state.get('lastError'),getattr(c,'_thread',None),max(900,getattr(c.config,'poll_seconds',120)*3))
    for name in ('mongo','redis','starrocks'):
        engine=getattr(app,name,None)
        if engine is None:continue
        state=engine.status()
        if state.get('status') in ('unavailable','partial'):add(name,'',True,error='initialization_unavailable')
        if name=='mongo':
            for c in getattr(engine,'collectors',[]):
                health=c.status()
                for lane in ('slowlog','metrics','counters'):
                    value=health.get(lane,'not_started')
                    thread=next((t for t in c.threads if t.name=='mongo-'+lane),None)
                    error='' if value in ('ok','not_started','not_connected','not_configured','disabled') else value
                    add(name+':'+lane,c.instance,health.get('enabled',False),health.get(lane+'_updated'),error,thread,1800 if lane=='slowlog' else 900)
        if name=='starrocks':
            lanes.append(dict(kind=name,state=state.get('status','unknown'),ok=state.get('status') in ('ok','not_configured'),scope='on_demand_not_a_background_collector'))
        if name=='redis':
            for entry in state.get('instances',[]):
                health=entry.get('health',{})
                add(name,entry['id'],True,health.get('last_success'),health.get('last_error') or bool(health.get('errors')),getattr(engine,'thread',None))
    sessions=getattr(app,'session_collector',None)
    if sessions is not None:
        state=sessions.status()
        add('sessions','',state.get('enabled',False),getattr(sessions,'_last_pass_at',None),state.get('loop_error',''),getattr(sessions,'_thread',None))
    audit=getattr(app,'audit_ingest',None)
    if audit is not None:
        state=audit.status()
        lanes.append(dict(kind='audit_delivery',state=state.get('status','unknown'),ok=state.get('status')=='ok',scope='event_driven_last_receive_not_source_completeness'))
    return dict(ok=all(l['ok'] for l in lanes), lanes=lanes, generated_at=datetime.now(UTC).isoformat())


class UnavailableEngine:
    def __init__(self, name):
        self.name=name
        self.entries=[]
        self.collectors=[]
        self.aggregates=None
    def status(self):return {'status':'unavailable','reason':self.name+'_initialization_failed','instances':[]}
    def start(self):pass
    def stop(self):pass
    def shutdown(self):pass
    def authorized(self, _):return False
    def __getattr__(self, _):raise RuntimeError(self.name+'_unavailable')
