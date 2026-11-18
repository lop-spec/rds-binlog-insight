"""On-demand incident lane. Only control-plane reads + retained Insight files."""
from __future__ import annotations

from collections import deque
from concurrent.futures import ThreadPoolExecutor
import json
import logging
import threading
import time

from .mongo_collector import iso
from .mongo_insight import MINUTE, epoch_us
from .mongo_pressure import PERFORMANCE, performance_points, pressure_evidence
from .mongo_sls import SlsReadonly, validate_sources
from .mongo_workload import indexed_workload
from .credentials import load_credential
from .pod_lookup import inventory_path, resolve_pod
from .resource_evidence import native_continuity

LOGGER = logging.getLogger(__name__)
MAX_DETAIL_WINDOW = 30 * MINUTE
MAX_EVIDENCE_WINDOW = 7 * 24 * 60 * MINUTE


def lane_failure(exc):
    LOGGER.exception('Mongo investigation lane failed')
    message = str(exc).lower()
    kind = 'throttled' if 'thrott' in message else 'permission_denied' if any(k in message for k in ('forbidden','permission','unauthor')) else 'budget_exhausted' if 'budget' in message else 'unavailable'
    return {'reason': kind, 'exception_type': type(exc).__name__}


def running_event(row, role, timestamp=None):
    """No command payloads, auth metadata, customer messages or raw log text."""
    try:
        content = str(row.get('Content') or '')
        doc = json.loads(content if content.startswith('{') else '{' + content + '}')
        if timestamp is None:
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
        self.running_log_calls = deque()

    def run(self, params):
        def get(k, default=''):
            v = params.get(k, default)
            return v[0] if isinstance(v, list) else v
        instance = get('instance')
        collector = next((c for c in self.service.collectors if c.instance == instance), None)
        if collector is None or instance not in self.service.stores:
            raise ValueError('configured_mongo_instance_required')
        start, end = int(get('startEpochUs')), int(get('endEpochUs'))
        evidence_only = get('evidenceOnly') == '1'
        if start < 0 or not 0 < end-start <= MAX_EVIDENCE_WINDOW or start % 1_000_000 or end % 1_000_000:
            raise ValueError('invalid_mongo_evidence_window')
        base = int(get('baselineStart', start-(end-start)))
        if not evidence_only and (end-start > MAX_DETAIL_WINDOW or start % MINUTE or end % MINUTE or base % MINUTE):
            raise ValueError('pressure_requires_whole_minute_window_up_to_thirty_minutes')
        if base < 0 or base+(end-start) > start:
            raise ValueError('baseline_must_not_overlap_current_window')
        role = get('role')
        if role not in ('', 'Primary', 'Secondary', 'Unknown'):
            raise ValueError('invalid_role')
        sources = validate_sources(collector.entry.get('logSources', []))
        # Older callers can still send a registered source ID; all configured sources are automatic.
        if get('logSource') not in ('', 'auto', *(s['id'] for s in sources)):
            raise ValueError('unconfigured_mongo_log_source')
        focus_start, focus_end, focus_reason = start, end, 'entire_window'
        if end-start > MAX_DETAIL_WINDOW:
            anchor = int(get('focusEpochUs') or end-1)
            if not start <= anchor < end:
                raise ValueError('mongo_evidence_focus_outside_window')
            focus_start = max(start, min(end-MAX_DETAIL_WINDOW, (anchor // MINUTE - 15) * MINUTE))
            focus_end = focus_start + MAX_DETAIL_WINDOW
            focus_reason = 'metric_peak' if get('focusEpochUs') else 'window_end_no_metric_peak'
        key = (instance, start, end, base, role, evidence_only, focus_start, focus_end, focus_reason, json.dumps(sources, sort_keys=True))
        saved = self.cache.get(key)
        if saved and saved[0] > time.monotonic():
            return saved[1]
        if not self.lock.acquire(blocking=False):
            raise ValueError('mongo_investigation_busy')
        try:
            deadline = time.monotonic()+90
            with ThreadPoolExecutor(max_workers=1) as pool:
                logs = pool.submit(self._sls, collector, sources, start, end, deadline) if sources else None
                result = self._run(collector, focus_start, focus_end, base, role, deadline, evidence_only=evidence_only)
                result['sls'] = logs.result() if logs else dict(status='not_configured', complete=False)
            result.update(requested_start_us=start, requested_end_us=end, focus_reason=focus_reason)
            self.cache = {k: v for k, v in self.cache.items() if v[0] > time.monotonic()}
            if len(self.cache) >= 8:
                self.cache.pop(next(iter(self.cache)))
            self.cache[key] = time.monotonic()+60, result
            return result
        finally:
            self.lock.release()

    @staticmethod
    def _sls(collector, sources, start, end, deadline):
        results = []
        for ix, source in enumerate(sources):
            try:
                now = time.monotonic()
                if now >= deadline-8:
                    raise RuntimeError('investigation_budget_exhausted')
                credential = load_credential(collector.settings_loader().credential_target)
                if credential is None:
                    raise RuntimeError('cloud_credential_unavailable')
                source_deadline = now + (deadline-now)/(len(sources)-ix)
                item = SlsReadonly(collector.entry['region'], credential).collect(
                    source, start, end, max_pages=5, deadline=source_deadline)
                item['status'] = 'ok' if item['complete'] else 'partial'
            except Exception as exc:
                item = dict(status='unavailable', complete=False, **lane_failure(exc))
            item.update(source=source['id'], label=source.get('label',source['id']))
            results.append(item)
        return results[0] if len(results)==1 else dict(status='configured_sources', sources=results,
            complete=all(s.get('complete') for s in results))

    def _run(self, collector, start, end, base, role, deadline, *, evidence_only):
        dds, rpc_error = None, None
        try:
            dds = collector.rpc()
            # Interactive reads have their own short budget, never retry a failed page.
            dds.timeout = 8
            dds.RETRY_DELAYS = ()
        except Exception as exc:
            rpc_error = exc
        gaps = {}
        calls = 0
        def call(action, params):
            nonlocal calls
            if rpc_error is not None:
                raise rpc_error
            if calls >= 14 or time.monotonic() >= deadline-8:
                raise RuntimeError('investigation_budget_exhausted')
            if action == 'DescribeRunningLogRecords':
                now = time.monotonic()
                while self.running_log_calls and self.running_log_calls[0] <= now-60:
                    self.running_log_calls.popleft()
                if len(self.running_log_calls) >= 24:
                    raise RuntimeError('running_log_throttled')
                self.running_log_calls.append(now)
            calls += 1
            return dds.call(action, params)
        failure = lane_failure
        points, events, logs = [], [], []
        roles = [role] if role in ('Primary','Secondary') else ['Primary', 'Secondary']
        # DDS request parameters are whole-minute UTC strings. Fetch enclosing
        # minutes, while accepting only samples and log rows inside the exact slice.
        dds_start, dds_end = iso(start), iso((end+MINUTE-1)//MINUTE*MINUTE)
        for r in roles:
            try:
                payload = call('DescribeDBInstancePerformance', dict(
                    DBInstanceId=collector.instance, ReplicaSetRole=r, StartTime=dds_start, EndTime=dds_end,
                    Interval='5', Key=','.join(dict.fromkeys(s[0] for s in PERFORMANCE.values()))))
                batch = performance_points(payload, r, start, end, 5)
                points.extend(batch)
                absent = sorted(set(PERFORMANCE)-{p['metric'] for p in batch})
                if absent: gaps['performance:'+r] = dict(missing_metrics=absent)
            except Exception as exc:
                gaps['performance:'+r] = failure(exc)
            got, total, in_window, invalid_time, complete = 0, None, 0, 0, False
            try:
                for page in range(1, 4):
                    payload = call('DescribeRunningLogRecords', dict(DBInstanceId=collector.instance,
                        RoleType=r.lower(), StartTime=dds_start, EndTime=dds_end, PageSize=100, PageNumber=page,
                        QueryKeywords='checkpoint election stepdown shutdown', LogicalOperator='or', OrderType='asc'))
                    count = int(payload['TotalRecordCount'])
                    if total is not None and count != total:
                        gaps['running_logs:'+r] = {'reason': 'source_changed_during_pagination'}
                        break
                    total = count
                    batch = payload.get('Items', {}).get('LogRecords', [])
                    got += len(batch)
                    for row in batch:
                        try:
                            ts = epoch_us(row['CreateTime'])
                        except (ValueError, KeyError, TypeError):
                            invalid_time += 1
                            continue
                        if start <= ts < end:
                            in_window += 1
                            e = running_event(row, r, ts//1000)
                            if e: events.append(e)
                    if got >= total:
                        complete = not invalid_time; break
                    if not batch: break
            except Exception as exc:
                gaps['running_logs:'+r] = failure(exc)
            logs.append(dict(role=r, total=total, fetched=got, in_window=in_window, invalid_time=invalid_time,
                             complete=complete, remaining=max(total-got,0) if total is not None else None,
                             filter='checkpoint/election/stepdown/shutdown'))
        store = self.service.stores[collector.instance]
        detail = pressure_evidence(points, start, end)
        missing_roles = set(roles) - {n['role'] for n in detail['nodes'] if n['complete']}
        if missing_roles:
            try:
                if time.monotonic() >= deadline: raise RuntimeError('investigation_budget_exhausted')
                retained = [p for p in store.read_telemetry(collector.instance, 'pressure', start, end)
                            if p.get('role') in missing_roles and p.get('period') == 60
                            and p.get('timestamp_semantics') == 'period_start'
                            and start <= p['timestamp']*1000 <= end-MINUTE]
                points.extend(retained)
                detail = pressure_evidence(points, start, end)
                if not retained: gaps['retained_pressure'] = dict(reason='no_retained_minutes')
            except Exception as exc:
                gaps['retained_pressure'] = failure(exc)
        result = dict(instance=collector.instance, start_us=start, end_us=end, baseline_start=base,
                      metric='WtCacheUpdatesUsage', performance=detail, metric_points=points,
                      metric_labels={k: v[2] for k, v in PERFORMANCE.items()},
                      running_events=sorted(events, key=lambda e: e['timestamp'])[:200], running_log_coverage=logs,
                      gaps=gaps, requests=calls, causal=False, root_cause='unresolved')
        if evidence_only:
            if gaps:
                LOGGER.warning('mongo_investigation partial: instance=%s lanes=%s', collector.instance, ','.join(gaps))
            return result

        audit = dict(status='unknown')
        try:
            audit['status'] = call('DescribeAuditPolicy', {'DBInstanceId': collector.instance}).get('LogAuditStatus', 'unknown')
        except Exception as exc:
            gaps['audit'] = failure(exc)
        prefixes = collector.entry.get('families', [])
        rows, before = [], []
        coverage, baseline = dict(complete=False), dict(complete=False)
        try:
            rows, coverage = store.read(collector.instance, start, end, role=role, kind='command')
            before, baseline = store.read(collector.instance, base, base+(end-start), role=role, kind='command')
        except Exception as exc:
            gaps['indexed_workload'] = failure(exc)
        samples, sample_coverage = [], dict(complete=False)
        try:
            samples, sample_coverage = store.pressure_records(collector.instance, start, end, prefixes, role=role, deadline=deadline)
        except Exception as exc:
            gaps['supplemental_raw_samples'] = failure(exc)
        workload = indexed_workload(rows, before, coverage['complete'], baseline['complete'], samples)
        workload['sample_coverage'] = sample_coverage
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
            gaps['native_history'] = failure(exc)
        if gaps:
            LOGGER.warning('mongo_investigation partial: instance=%s lanes=%s', collector.instance, ','.join(gaps))
        result.update(audit=audit, workload=workload, coverage=coverage, baseline_coverage=baseline,
                      native_snapshots=native[:200], native_continuity=native_continuity(native), requests=calls,
                      pod_scope='historical_ip_bindings_only_no_pod_cpu_or_restart_history',
                      warning='节点级更新缓存不归属到单条命令；仅有时序与成本增量不能证明唯一触发源。历史快照持有者、全量快速写入及隐藏节点未覆盖时明确保留缺口。')
        return result
