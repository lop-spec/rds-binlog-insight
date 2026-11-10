"""Incident workload from the existing minute index, not truncated raw scans.

Read bytes are slow-record engine reads, not physical I/O or cache ownership.
Missing cost fields stay missing; recorded-field differences are not total deltas.
"""
from collections import Counter
import json

from .mongo_insight import COSTS, parse_counts, top_counts


def indexed_workload(rows, before, complete, baseline_complete, samples=(), limit=50):
    def aggregate(source, families=False):
        groups = {}
        for r in source:
            p = json.loads(r['profile'])
            if p['kind'] != 'command':
                continue
            key = (r['role'], p['namespace'] if families else r['group_id'])
            g = groups.setdefault(key, dict(role=r['role'], group_id=r['group_id'], namespace=p['namespace'],
                command=p['command'], count=0, costs={}, clients=Counter(), accounts=Counter(), samples=[], trend=[], commands=set()))
            g['count'] += r['count']; g['commands'].add(p['command'])
            g['clients'].update(parse_counts(r.get('clients'))); g['accounts'].update(parse_counts(r.get('accounts')))
            for field in COSTS:
                cost = g['costs'].setdefault(field, dict(observed=0, known=0))
                cost['observed'] += r[field]; cost['known'] += r[field+'_known']
            if r['count']:
                sample = json.loads(r['sample'])
                if sample not in g['samples']:
                    g['samples'].append(sample)
                    g['samples'] = sorted(g['samples'], key=lambda s: (s.get('costs', {}).get('bytes_read') or 0, s.get('duration_us') or 0), reverse=True)[:3]
            g['trend'].append(dict(start_us=r['bucket'], count=r['count'], bytes_read=r['bytes_read'] if r['bytes_read_known'] else None,
                                   bytes_read_known=r['bytes_read_known']))
        return groups

    def compare(current, baseline):
        out = []
        for key, g in current.items():
            old = baseline.get(key)
            g.update(baseline_count=(old['count'] if old else 0) if baseline_complete else None,
                     count_delta=g['count']-(old['count'] if old else 0) if complete and baseline_complete else None,
                     causal=False, source='retained_minute_index', complete=complete, baseline_complete=baseline_complete)
            for field, c in g['costs'].items():
                b = (old or {}).get('costs', {}).get(field, {})
                c.update(total=g['count'], baseline_known=b.get('known', 0), baseline_total=old['count'] if old else 0,
                         observed=c['observed'] if c['known'] else None,
                         baseline=b.get('observed') if b.get('known') else 0 if old is None and baseline_complete else None,
                         delta=None, observed_delta=None)
                comparable = complete and baseline_complete and c['observed'] is not None and c['baseline'] is not None
                if comparable:
                    c['observed_delta'] = c['observed']-c['baseline']
                    if c['known']==c['total'] and c['baseline_known']==c['baseline_total']:
                        c['delta'] = c['observed_delta']
                c['comparison_scope'] = ('complete_cost_fields' if c['delta'] is not None else
                                         'recorded_fields_only' if c['observed_delta'] is not None else 'unavailable')
            g['clients'] = top_counts(g['clients'], 20); g['accounts'] = top_counts(g['accounts'], 10)
            g['commands'] = sorted(g['commands'])
            exact_growth = any((g['costs'][f]['delta'] or 0)>0 for f in ('bytes_read','cpu_ns'))
            recorded_growth = any((g['costs'][f]['observed_delta'] or 0)>0 for f in ('bytes_read','cpu_ns'))
            elapsed_growth = (g['costs']['duration_us']['observed_delta'] or 0)>0
            g['assessment'] = ('workload_growth_candidate' if exact_growth else 'recorded_cost_increased' if recorded_growth else
                               'latency_growth_only' if elapsed_growth else 'observed_costs_only')
            out.append(g)
        return sorted(out, key=lambda g: (-(g['costs']['bytes_read']['observed'] or 0), -(g['costs']['cpu_ns']['observed'] or 0), g['namespace'], g['group_id']))

    groups = compare(aggregate(rows), aggregate(before))
    # Raw evidence only enriches representative samples; never changes indexed totals.
    lookup = {(g['role'],g['group_id']):g for g in groups}
    for e in samples:
        g = lookup.get((e['role'],e['group_id']))
        if g is not None:
            s = json.loads(e['sample'])
            if s not in g['samples']:
                g['samples'] = sorted(g['samples']+[s],key=lambda s:(s.get('costs',{}).get('bytes_read') or 0,s.get('duration_us') or 0),reverse=True)[:3]
    families = compare(aggregate(rows, True), aggregate(before, True))
    return dict(groups=groups[:max(1,min(int(limit),200))], total_groups=len(groups), families=families[:20],
                complete=complete, baseline_complete=baseline_complete, population='command_slow_records_only',
                order='recorded_read_bytes_desc_not_cache_contribution',
                warning='按已记录读取量列出业务工作量，不是缓存占用贡献榜。字段不完整时仅比较已记录值并展示两窗覆盖；不外推总量。读取字节不是物理磁盘读取或缓存净增；慢记录不等于全量请求，耗时变长也可能是等待受害者。')
