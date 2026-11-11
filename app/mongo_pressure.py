"""Read-only cache/ticket evidence. No inferred thresholds or causal scores."""
from __future__ import annotations

from collections import defaultdict
import math
import json

# DDS names are provider-defined; field order is always taken from ValueFormat.
PERFORMANCE = {
    'WtCacheUpdatesUsage': ('MongoDB_WTCacheUsage', 'wt_cache_updates_usage', '更新缓存占比 %', '%'),
    'WtCacheUsage': ('MongoDB_WTCacheUsage', 'wt_cache_usage', '总缓存占比 %', '%'),
    'WtCacheDirtyUsage': ('MongoDB_WTCacheUsage', 'wt_cache_dirty_usage', '脏缓存占比 %', '%'),
    'ApplicationEvictions': ('MongoDB_WtCacheEviction', 'pages_evicted_by_application_threads', '业务线程淘汰页面（DDS口径）', 'provider_pages'),
    'WriteTicketsUsed': ('MongoDB_Wt_Concurrent_Trans', 'write_concurrent_trans_out', '已占用写 ticket', 'count'),
    'WriteTicketsAvailable': ('MongoDB_Wt_Concurrent_Trans', 'write_concurrent_trans_available', '可用写 ticket', 'count'),
    'ReadTicketsAvailable': ('MongoDB_Wt_Concurrent_Trans', 'read_concurrent_trans_available', '可用读 ticket', 'count'),
    'WriteQueue': ('MongoDB_GlobalLocks', 'gl_cq_writers', '写排队操作数', 'count'),
    'ReadQueue': ('MongoDB_GlobalLocks', 'gl_cq_readers', '读排队操作数', 'count'),
    'WriteAvgRt': ('MongoDB_RT', 'writes_avg_rt', '写平均响应 μs', 'us'),
    'CPUUtilization': ('CpuUsage', 'cpu_usage', 'CPU 使用率 %', '%'),
    'IOPSUtilization': ('IOPSUsage', 'iops_usage', 'IOPS 使用率 %', '%'),
    'DiskBandwidthUsage': ('MongoDB_MbpsUsage', 'total_mbps_usage', '磁盘吞吐使用率 %', '%'),
    'NetworkUsage': ('MongoDB_Network_Usage', 'network_usage', '网络带宽使用率 %', '%'),
    'RequestRate': ('MongoDB_Network', 'num_requests', '节点请求速率', 'provider_requests'),
    'Connections': ('MongoDB_Connections', 'current_conn', '连接数', 'count'),
    'ReplicationLag': ('MongoDB_Repl_Lag', 'repl_lag', '复制延迟 秒', 's'),
}
PRESSURE_METRICS = tuple(PERFORMANCE)
# Existing CMS series remain unchanged. New detailed gauges come from DDS.
COLLECTED_DDS = tuple(k for k in PERFORMANCE if k not in {
    'WtCacheUsage', 'WtCacheDirtyUsage', 'WriteAvgRt', 'CPUUtilization',
    'IOPSUtilization', 'ReplicationLag'})


def finite(value):
    try:
        n = float(value)
        return n if not isinstance(value, bool) and math.isfinite(n) else None
    except (TypeError, ValueError, OverflowError):
        return None


def performance_points(payload, role, start_us, end_us, period, metrics=PRESSURE_METRICS):
    """DDS timestamps mark period start; CMS period-end series must not be mixed."""
    from .mongo_insight import epoch_us
    out = []
    for key in payload.get('PerformanceKeys', {}).get('PerformanceKey', []):
        names = str(key.get('ValueFormat') or '').split('&')
        wanted = [(metric, spec, names.index(spec[1])) for metric, spec in PERFORMANCE.items()
                  if metric in metrics and spec[0] == key.get('Key') and spec[1] in names]
        for row in key.get('PerformanceValues', {}).get('PerformanceValue', []):
            try:
                t = epoch_us(row['Date'])
            except (ValueError, TypeError, KeyError):
                continue
            if not start_us <= t or t + period * 1_000_000 > end_us:
                continue
            values = str(row.get('Value') or '').split('&')
            for metric, spec, ix in wanted:
                value = finite(values[ix]) if ix < len(values) else None
                if value is None or (value < 0 and metric != 'ReplicationLag') or (metric in {'WtCacheUsage','WtCacheDirtyUsage','WtCacheUpdatesUsage','CPUUtilization'} and value > 100):
                    continue
                out.append(dict(timestamp=t // 1000, role=role, node='', metric=metric,
                                value=value, period=period, source='dds', unit=spec[3],
                                timestamp_semantics='period_start', identity_scope='role_only'))
    return out


def pressure_evidence(points, start_us, end_us):
    """Observed co-occurrence, not proof of a producer or a configured 10% trigger."""
    groups = defaultdict(list)
    for p in points:
        if p.get('metric') in PERFORMANCE:
            groups[(p.get('role', 'Unknown'), p.get('node') or '', p.get('epoch'), p.get('period'))].append(p)
    result = []
    for (role, node, epoch, period), rows in sorted(groups.items(), key=lambda x: str(x[0])):
        if period not in (5, 60):
            continue
        frames = defaultdict(dict)
        conflicts = set()
        for p in rows:
            t, v = finite(p.get('timestamp')), finite(p.get('value'))
            if t is None or v is None or not start_us <= t * 1000 < end_us:
                continue
            metric = p['metric']
            if metric in frames[t] and frames[t][metric] != v:
                conflicts.add((t, metric))
            frames[t][metric] = v
        for t, metric in conflicts:
            frames[t].pop(metric, None)
        ordered = sorted(frames.items())
        saturation = [(t, f) for t, f in ordered if f.get('WriteTicketsAvailable') == 0 and f.get('WriteQueue', 0) > 0]
        # A run is broken by a missing bucket, an absent field, or recovered availability.
        runs, current = [], []
        for t, f in saturation:
            if current and t - current[-1][0] != period * 1000:
                runs.append(current); current = []
            current.append((t, f))
        if current:
            runs.append(current)
        runs = [r for r in runs if len(r) >= 2]
        evictions = [t for t, f in ordered if f.get('ApplicationEvictions', 0) > 0]
        updates = [(t, f['WtCacheUpdatesUsage']) for t, f in ordered if 'WtCacheUpdatesUsage' in f]
        first_sat = runs[0][0][0] if runs else None
        first_evict = min(evictions) if evictions else None
        clean_before = any(f.get('ApplicationEvictions') == 0 and (first_evict is not None and t < first_evict)
                           for t, f in ordered)
        rising = bool(updates and first_sat is not None and
                      max((v for t, v in updates if t <= first_sat), default=updates[0][1]) > updates[0][1])
        linked = bool(runs and first_evict is not None and first_evict < first_sat and clean_before and rising)
        status = ('cache_pressure_precedes_ticket_saturation' if linked else
                  'write_ticket_saturation' if runs else 'eviction_observed' if evictions else 'insufficient_or_no_saturation')
        needed = ('WtCacheUpdatesUsage', 'ApplicationEvictions', 'WriteTicketsUsed', 'WriteTicketsAvailable', 'WriteQueue')
        expected = (end_us - start_us) // (period * 1_000_000)
        coverage = {m: sum(m in f for _, f in ordered) for m in needed}
        peaks, troughs, initial = {}, {}, {}
        for metric in PERFORMANCE:
            values = [(t, f[metric]) for t, f in ordered if metric in f]
            if values:
                t, value = max(values, key=lambda x: x[1])
                peaks[metric] = dict(value=value, timestamp=t)
                t, value = min(values, key=lambda x: x[1])
                troughs[metric] = dict(value=value, timestamp=t)
                initial[metric] = dict(timestamp=values[0][0],value=values[0][1])
        capacities = [dict(timestamp=t, value=f['WriteTicketsUsed'] + f['WriteTicketsAvailable'])
                      for t, f in ordered if 'WriteTicketsUsed' in f and 'WriteTicketsAvailable' in f]
        result.append(dict(role=role, node=node, epoch=epoch, period=period, status=status, causal=False,
                           identity_scope='observed_node' if node and epoch else 'role_only_unverified',
                           configured_updates_trigger=None, expected_points=expected, coverage=coverage,
                           complete=bool(expected) and all(n == expected for n in coverage.values()) and not conflicts,
                           conflicts=len(conflicts), first_application_eviction_ms=first_evict,
                           episodes=[dict(start_ms=r[0][0], end_ms=r[-1][0] + period * 1000,
                                          samples=len(r), max_write_queue=max(f['WriteQueue'] for _, f in r),
                                          window_clipped=r[-1][0]+period*1000 == end_us//1000) for r in runs],
                           peaks=peaks, troughs=troughs, initial=initial, write_capacity=capacities,
                           frames=[dict(timestamp=t, **f) for t, f in ordered]))
    return dict(nodes=result, causal=False, scope='observed_period_gauges_not_per_command_cache_ownership',
                warning='更新缓存、脏缓存、总缓存是不同指标；ticket 为动态额度。零可用需与排队和延迟一起核对，不能直接判定根因或假定阈值为10%。')


def slow_record_evidence(events, before, complete, baseline_complete):
    """Separate cost producers from wait victims; no cost delta from partial fields."""
    def aggregate(source):
        groups = {}
        for e in source:
            if e.get('kind') != 'command':
                continue
            key = e['role'], e['group_id']
            g = groups.setdefault(key, dict(role=e['role'], group_id=e['group_id'], namespace=e['namespace'],
                                             command=e['command'], count=0, costs={}, clients=set(), samples=[]))
            g['count'] += 1
            if e.get('client'):
                g['clients'].add(e['client'])
            try:
                sample = json.loads(e['sample'])
            except (ValueError,TypeError):
                sample = {'parse_error': True}
            for field in ('cpu_ns', 'bytes_read', 'duration_us', 'cache_wait_us', 'execution_queue_us', 'txn_dirty_bytes'):
                value = (sample.get('pressure_costs') or {}).get(field) if field in {'cache_wait_us','execution_queue_us','txn_dirty_bytes'} else e.get(field)
                cost = g['costs'].setdefault(field, dict(observed=0, known=0))
                if value is not None:
                    cost['observed'] += value; cost['known'] += 1
            # Highest read cost/elapsed samples retain their own timestamp and provenance.
            g['samples'] = sorted(g['samples'] + [sample], key=lambda s: (s.get('costs', {}).get('bytes_read') or 0,
                                                                          s.get('duration_us') or 0), reverse=True)[:2]
        return groups
    current, baseline = aggregate(events), aggregate(before)
    out = []
    for key, g in current.items():
        previous = baseline.get(key)
        for field, cost in g['costs'].items():
            old = (previous or {}).get('costs', {}).get(field, {})
            known = cost['known'] == g['count']
            old_known = bool(previous and old.get('known') == previous['count'])
            cost.update(total=g['count'], baseline=old.get('observed') if old_known else None,
                        observed=cost['observed'] if cost['known'] else None, delta=None)
            if complete and baseline_complete and known and old_known:
                cost['delta'] = cost['observed'] - cost['baseline']
        g['clients'] = sorted(g['clients'])[:20]
        g['baseline_count'] = previous['count'] if previous and baseline_complete else None
        g['causal'] = False
        g['assessment'] = ('workload_growth_candidate' if any((g['costs'][f]['delta'] or 0) > 0 for f in ('cpu_ns','bytes_read'))
                           else 'no_observed_cost_growth' if complete and baseline_complete and previous and all(g['costs'][f]['delta'] is not None for f in ('cpu_ns','bytes_read')) else 'waiting_or_incomplete_evidence')
        out.append(g)
    return dict(groups=sorted(out, key=lambda g: -(g['costs']['bytes_read']['delta'] or 0))[:50],
                complete=complete, baseline_complete=baseline_complete, population='command_slow_records_only',
                warning='读取字节不等于物理磁盘读取或缓存净增；等待者不是已证明的负载制造者。未开启审计也不能据慢日志还原全部快速写入。')
