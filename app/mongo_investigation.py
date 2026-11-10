"""On-demand incident lane. Only control-plane reads + retained Insight files."""
from __future__ import annotations

import json
import logging
import threading
import time

from .mongo_collector import iso
from .mongo_insight import MINUTE, epoch_us
from .mongo_pressure import PERFORMANCE, performance_points, pressure_evidence, slow_record_evidence
from .mongo_sls import SlsReadonly, validate_sources
from .credentials import load_credential
from .pod_lookup import inventory_path, resolve_pod
from .resource_evidence import native_continuity

LOGGER = logging.getLogger(__name__)


def running_event(row, role):
    """No command payloads, auth metadata, customer messages or raw log text."""
    try:
        content = str(row.get('Content') or '')
        doc = json.loads(content if content.startswith('{') else '{' + content + '}')
        timestamp = epoch_us(row['CreateTime']) // 1000
    except (ValueError, KeyError, TypeError):
        return None
    msg, attr = str(doc.get('msg') or ''), doc.get('attr') or {}
    event = dict(timestamp=timestamp, role=role, category='other')
    if msg == 'WiredTiger message':
        text = str((attr.get('message') or {}).get('msg') or '')
        import re
        match = re.search(r'Checkpoint (?:ran for|has been running for) (\d+) seconds.*?\((\d+) MB\)', text)
        if match:
            event.update(category='checkpoint', duration_seconds=int(match[1]), written_mb=int(match[2]),
                         finished='ran for' in text)
            return event
    if any(w in msg.lower() for w in ('stepdown', 'election', 'transition to', 'shutdown')):
        event['category'] = 'role_or_process_event'
        return event
    return None


class MongoInvestigator:
    def __init__(self, service):
        self.service = service
        self.lock = threading.Lock()
        self.cache = {}

    def run(self, params):
        def get(k, default=''):
            v = params.get(k, default)
            return v[0] if isinstance(v, list) else v
        instance = get('instance')
        collector = next((c for c in self.service.collectors if c.instance == instance), None)
        if collector is None or instance not in self.service.stores:
            raise ValueError('configured_mongo_instance_required')
        start, end = int(get('startEpochUs')), int(get('endEpochUs'))
        if not 0 < end-start <= 30*MINUTE or start % MINUTE or end % MINUTE:
            raise ValueError('pressure_requires_whole_minute_window_up_to_thirty_minutes')
        base = int(get('baselineStart', start-(end-start)))
        if base < 0 or base % MINUTE or base+(end-start) > start:
            raise ValueError('baseline_must_not_overlap_current_window')
        role = get('role')
        if role not in ('', 'Primary', 'Secondary'):
            raise ValueError('invalid_role')
        entry = collector.entry
        source_id = get('logSource')
        sources = validate_sources(entry.get('logSources', []))
        source = next((s for s in sources if s['id'] == source_id), None)
        if source_id and source is None:
            raise ValueError('unconfigured_mongo_log_source')
        key = (instance, start, end, base, role, json.dumps(source, sort_keys=True))
        saved = self.cache.get(key)
        if saved and saved[0] > time.monotonic():
            return saved[1]
        if not self.lock.acquire(blocking=False):
            raise ValueError('mongo_investigation_busy')
        try:
            result = self._run(collector, start, end, base, role, source)
            self.cache = {k: v for k, v in self.cache.items() if v[0] > time.monotonic()}
            if len(self.cache) >= 8:
                self.cache.pop(next(iter(self.cache)))
            self.cache[key] = time.monotonic()+60, result
            return result
        finally:
            self.lock.release()

    def _run(self, collector, start, end, base, role, source):
        deadline = time.monotonic()+90
        dds = collector.rpc()
        # Interactive reads have their own short budget, never retry a failed page.
        dds.timeout = 8
        dds.RETRY_DELAYS = ()
        gaps = {}
        calls = 0
        def call(action, params):
            nonlocal calls
            if calls >= 14 or time.monotonic() >= deadline:
                raise RuntimeError('investigation_budget_exhausted')
            calls += 1
            return dds.call(action, params)
        points, events, logs = [], [], []
        roles = [role] if role else ['Primary', 'Secondary']
        for r in roles:
            try:
                payload = call('DescribeDBInstancePerformance', dict(
                    DBInstanceId=collector.instance, ReplicaSetRole=r, StartTime=iso(start), EndTime=iso(end),
                    Interval='5', Key=','.join(dict.fromkeys(s[0] for s in PERFORMANCE.values()))))
                batch = performance_points(payload, r, start, end, 5)
                points.extend(batch)
                absent = sorted(set(PERFORMANCE)-{p['metric'] for p in batch})
                if absent: gaps['performance:'+r] = dict(missing_metrics=absent)
            except Exception as exc:
                gaps['performance:'+r] = dict(reason=type(exc).__name__)
            got, total, complete = 0, None, False
            try:
                for page in range(1, 4):
                    payload = call('DescribeRunningLogRecords', dict(DBInstanceId=collector.instance,
                        RoleType=r.lower(), StartTime=iso(start), EndTime=iso(end), PageSize=100, PageNumber=page,
                        QueryKeywords='checkpoint election stepdown shutdown', LogicalOperator='or', OrderType='asc'))
                    count = int(payload['TotalRecordCount'])
                    if total is not None and count != total:
                        break
                    total = count
                    batch = payload.get('Items', {}).get('LogRecords', [])
                    got += len(batch)
                    for row in batch:
                        e = running_event(row, r)
                        if e: events.append(e)
                    if got >= total:
                        complete = True; break
                    if not batch: break
            except Exception as exc:
                gaps['running_logs:'+r] = dict(reason=type(exc).__name__)
            logs.append(dict(role=r, total=total, fetched=got, complete=complete,
                             filter='checkpoint/election/stepdown/shutdown'))
        audit = dict(status='unknown')
        try:
            audit['status'] = call('DescribeAuditPolicy', {'DBInstanceId': collector.instance}).get('LogAuditStatus', 'unknown')
        except Exception as exc:
            gaps['audit'] = dict(reason=type(exc).__name__)
        store = self.service.stores[collector.instance]
        prefixes = collector.entry.get('families', [])
        current, before = [], []
        coverage, baseline = dict(complete=False), dict(complete=False)
        try:
            current, coverage = store.pressure_records(collector.instance, start, end, prefixes, role=role, deadline=deadline)
            if time.monotonic() < deadline:
                before, baseline = store.pressure_records(collector.instance, base, base+(end-start), prefixes, role=role, deadline=deadline)
            else:
                gaps['baseline'] = dict(reason='investigation_budget_exhausted')
        except Exception as exc:
            gaps['slow_records'] = dict(reason=type(exc).__name__)
        workload = slow_record_evidence(current, before, coverage['complete'], baseline['complete'])
        # Bind the IP at the sample time, never use today's Pod name as historical proof.
        for g in workload['groups']:
            for sample in g['samples']:
                client = sample.get('client')
                if client:
                    sample['pod'] = resolve_pod(client, sample['start_us'], inventory_path(self.service.root), collector.instance)
        native = []
        try:
            if time.monotonic() >= deadline: raise RuntimeError('investigation_budget_exhausted')
            for s in store.read_telemetry(collector.instance,'native',start,end):
                if role and s.get('role') != role: continue
                cache = s.get('wt_cache') or {}
                native.append({**{k:s.get(k) for k in ('timestamp','node','epoch','role','mem','connections','cursor','global_lock')},
                               'cache':{k:cache[k] for k in ('maximum bytes configured','bytes currently in the cache',
                                   'tracked dirty bytes in the cache','bytes allocated for updates') if k in cache}})
        except Exception as exc:
            gaps['native_history'] = dict(reason=type(exc).__name__)
        sls = dict(status='not_configured' if not collector.entry.get('logSources') else 'not_selected', complete=False)
        if source:
            try:
                if time.monotonic() >= deadline-8:
                    raise RuntimeError('investigation_budget_exhausted')
                credential = load_credential(collector.settings_loader().credential_target)
                if credential is None: raise RuntimeError('cloud_credential_unavailable')
                sls = SlsReadonly(collector.entry['region'], credential).collect(source, start, end, max_pages=5, deadline=deadline)
                sls['status'] = 'ok' if sls['complete'] else 'partial'
            except Exception as exc:
                sls = dict(status='unavailable', complete=False, reason=type(exc).__name__)
        if gaps:
            LOGGER.warning('mongo_investigation partial: instance=%s lanes=%s', collector.instance, ','.join(gaps))
        return dict(instance=collector.instance, start_us=start, end_us=end, baseline_start=base,
                    metric='WtCacheUpdatesUsage', performance=pressure_evidence(points, start, end),
                    metric_points=points, metric_labels={k: v[2] for k, v in PERFORMANCE.items()},
                    running_events=events[:200], running_log_coverage=logs, audit=audit,
                    workload=workload, coverage=coverage, baseline_coverage=baseline, sls=sls, gaps=gaps,
                    native_snapshots=native[:200], native_continuity=native_continuity(native),
                    pod_scope='historical_ip_bindings_only_no_pod_cpu_or_restart_history',
                    requests=calls, causal=False, root_cause='unresolved',
                    warning='节点级更新缓存不归属到单条命令；仅有时序与成本增量不能证明唯一触发源。历史快照持有者、全量快速写入及隐藏节点未覆盖时明确保留缺口。')
