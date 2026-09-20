"""Resource-matched evidence, not a causal score. No inferred missing costs."""
from __future__ import annotations

from collections import defaultdict
import logging
import math

LOGGER = logging.getLogger(__name__)
MINUTE = 60_000_000
STOCK_METRICS = {'MemoryUtilization', 'WtCacheUsage', 'CentralCacheFree',
                 'TcmallocCacheMemRatio', 'WtCacheDirtyUsage', 'DirtyCacheBytes'}
# Read bytes are NOT physical I/O operations; latency is an outcome, not CPU.
RESOURCE_COST = {'CPUUtilization': ('cpu_ns', 'direct'),
                 'ScannedDocs': ('docs', 'direct'),
                 'IOPSUtilization': ('bytes_read', 'related'),
                 'IoBandwidth': ('bytes_read', 'related'),
                 'AvgRt': ('duration_us', 'outcome'),
                 'WriteAvgRt': ('duration_us', 'outcome')}


def metric_windows(points, metric, start, end):
    """Complete period-end buckets only; ambiguous roles never overwrite a node."""
    times = list(range(((start + MINUTE - 1) // MINUTE + 1) * MINUTE,
                       end // MINUTE * MINUTE + 1, MINUTE))
    buckets = defaultdict(lambda: defaultdict(list))
    for p in points:
        if p.get('metric') != metric:
            continue
        try:
            t = int(p['timestamp']) * 1000
        except (KeyError, TypeError, ValueError, OverflowError):
            LOGGER.warning('resource_metric invalid timestamp; point omitted')
            continue
        if times and times[0] <= t <= times[-1]:
            buckets[str(p.get('role', 'Unknown'))][t].append(p)
    output = {}
    for role, samples in buckets.items():
        values, nodes, epochs = {}, set(), set()
        issues = set()
        for t in times:
            rows = samples.get(t, [])
            if not rows:
                issues.add('metric_gaps'); continue
            identities = {str(p.get('node') or p.get('nodeId') or '') for p in rows}
            observed = []
            for p in rows:
                try:
                    v = float(p['value'])
                    if p['value'] is None or isinstance(p['value'], bool) or not math.isfinite(v) or v < 0 or p.get('period', 60) != 60:
                        raise ValueError()
                    observed.append(round(v * 1000))
                except (KeyError, TypeError, ValueError, OverflowError):
                    issues.add('invalid_metric_value')
            if len(identities) > 1 or len(set(observed)) > 1:
                issues.add('ambiguous_metric_samples')
            nodes.update(identities - {''})
            epochs.update(str(p['epoch']) for p in rows if p.get('epoch') is not None)
            if len(observed) == len(rows) and len(set(observed)) == 1:
                values[t] = observed[0]
        if len(nodes) > 1 or len(epochs) > 1:
            issues.add('node_or_epoch_changed')
        reason = next((x for x in ('node_or_epoch_changed','ambiguous_metric_samples','invalid_metric_value','metric_gaps') if x in issues),'ok')
        if reason != 'ok':
            LOGGER.warning('resource_metric unavailable: role=%s metric=%s reason=%s', role, metric, reason)
        output[role] = dict(values=values, reason=reason, times=times,
                            nodes=sorted(nodes), epochs=sorted(epochs),
                            identity_verified=bool(nodes) and all(
                                p.get('node') or p.get('nodeId') for rows in samples.values() for p in rows))
    return output, times


def metric_comparison(current, previous):
    if not current or not previous:
        return dict(status='metric_baseline_unavailable', delta=None)
    if current['reason'] != 'ok' or previous['reason'] != 'ok':
        return dict(status='metric_comparison_incomplete', delta=None)
    a, b = list(current['values'].values()), list(previous['values'].values())
    if len(a) != len(b) or len(a) < 6:
        return dict(status='metric_comparison_incomplete', delta=None)
    if current['nodes'] != previous['nodes'] or current['epochs'] != previous['epochs']:
        return dict(status='node_or_epoch_changed', delta=None)
    before, now = sum(b) / len(b) / 1000, sum(a) / len(a) / 1000
    verified = current['identity_verified'] and previous['identity_verified']
    return dict(status='ok' if verified else 'role_only_identity_unverified',
                baseline_mean=before, current_mean=now, delta=now-before,
                scope='window_mean_not_peak_or_causation')


def native_continuity(samples):
    """ServerStatus identities, not seed hostnames or role labels."""
    identities = defaultdict(set)
    for p in samples:
        if p.get('node') and p.get('epoch') is not None:
            identities[str(p.get('role','Unknown'))].add((str(p['node']),str(p['epoch'])))
    return {role: ('node_or_epoch_changed' if len(values)>1 else 'single_observed_epoch')
            for role,values in identities.items()}


def assess(row, metric, *, comparison=None):
    """Separate direct growth, related work, waiting and insufficient evidence."""
    field, relation = RESOURCE_COST.get(metric, (None, 'unavailable'))
    cost = row.get('costs', {}).get(field, {})
    delta = cost.get('delta')
    reasons = []
    status, priority = 'insufficient_evidence', 0
    if metric in STOCK_METRICS:
        status = 'memory_requires_component_deltas'
    elif not field:
        status = 'resource_cost_unavailable'
    elif row.get('assessment') in {'incomplete_source', 'incomplete_baseline', 'incomplete_command', 'after_peak'}:
        status = row['assessment']
    elif delta is None:
        status = 'resource_cost_incomplete'
    elif delta > 0:
        status, priority = {'direct': ('direct_cost_growth', 3),
                            'related': ('related_cost_growth', 2),
                            'outcome': ('latency_growth_not_cause', 1)}[relation]
    else:
        status = 'no_resource_cost_growth'
        if (row.get('costs', {}).get('duration_us', {}).get('delta') or 0) > 0:
            reasons.append('elapsed_growth_without_resource_cost_growth')
    wait_delta = row.get('costs', {}).get('write_wait_us', {}).get('delta')
    if wait_delta is not None and wait_delta > 0:
        reasons.append('write_concern_wait_increased')
    if row.get('evidence', {}).get('metric_status') in {'node_or_epoch_changed', 'ambiguous_metric_samples', 'invalid_metric_value'}:
        status, priority = 'metric_identity_or_values_invalid', 0
    continuity = row.get('evidence', {}).get('native_continuity', 'unverified')
    if continuity == 'node_or_epoch_changed':
        status, priority = 'node_or_epoch_changed', 0
        reasons.append('node_or_epoch_changed')
    if comparison and comparison.get('status') == 'node_or_epoch_changed':
        status, priority = 'node_or_epoch_changed', 0
    elif comparison and comparison.get('delta') is not None and comparison['delta'] <= 0:
        reasons.append('resource_window_mean_not_increased')
    if relation == 'related':
        reasons.append('read_bytes_not_physical_iops_or_all_writes')
    if relation == 'outcome':
        reasons.append('elapsed_time_includes_waits')
    reasons += ['slow_records_not_total_executions', 'association_not_causation']
    if continuity == 'unverified':
        reasons.append('native_epoch_unverified')
    if row.get('evidence', {}).get('metric_status') not in (None, 'ok'):
        reasons.append('temporal_association_unavailable')
    if not comparison or comparison.get('status') != 'ok':
        reasons.append((comparison or {}).get('status', 'metric_baseline_unavailable'))
    before, observed = cost.get('baseline'), cost.get('observed')
    n0, n1 = row.get('baseline_count'), row.get('count')
    freq = unit = None
    if delta is not None and n0 and n1 is not None and before is not None and observed is not None:
        freq = (n1-n0) * before/n0
        unit = observed - n1 * before/n0
    return dict(status=status, priority=priority, cost_field=field, cost_relation=relation,
                observed=observed, baseline=before, delta=delta,
                frequency_component=freq, per_call_component=unit,
                reasons=reasons, resource_comparison=comparison or {},
                scope='observed_slow_record_costs', causal=False)
