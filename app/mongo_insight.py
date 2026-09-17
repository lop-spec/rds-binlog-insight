"""Mongo-specific semantics; shared, deterministic insight math, no model calls.

Counts here are slow RECORD counts. Command and suboperation populations never
share a group. Native counters and client aggregates are separate populations.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import re
from collections import defaultdict, Counter
from datetime import datetime, timezone
from typing import Any

from .slowlog_correlation import pearson, differences

LOGGER = logging.getLogger(__name__)
MINUTE = 60_000_000
WINDOW = 5 * MINUTE
VERSION = 2  # Existing collection fingerprints must remain comparable.
NORMALIZATION_VERSION = 3  # Reprocess source windows when parsing changes.
DATABASE_COMMANDS = ('dbStats',)
COMMANDS = ('find', 'aggregate', 'distinct', 'count', 'findAndModify', 'update',
            'insert', 'delete', 'getMore', 'bulkWrite', 'mapReduce')
NOISE = {'lsid', '$db', '$clusterTime', 'txnNumber', 'comment', 'maxTimeMS'}
COSTS = ('duration_us', 'docs', 'keys', 'returned', 'cpu_ns', 'bytes_read', 'read_us',
         'planning_us', 'write_wait_us', 'modified')


def number(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, dict):
        v = v.get('$numberLong', v.get('$numberInt', v.get('$numberDouble')))
    if v is None:
        return None
    n = float(v)
    if not math.isfinite(n) or n < 0:
        raise ValueError('invalid_nonnegative_metric')
    return int(v) if isinstance(v, (str, int)) and '.' not in str(v) else int(n)


def canonical(v: Any) -> str:
    return json.dumps(v, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def digest(v: Any) -> str:
    return hashlib.sha256(canonical(v).encode()).hexdigest()


def epoch_us(value: Any) -> int:
    text = str(value).replace(' T', 'T').replace('T ', 'T').replace('Z', '+00:00')
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        raise ValueError('timestamp_timezone_required')
    return int(dt.timestamp() * 1_000_000)


def family(namespace: str, prefixes: list[str]) -> str:
    db, _, collection = namespace.partition('.')
    for prefix in prefixes:
        if re.fullmatch(re.escape(prefix) + r'_\d+', collection):
            return db + '.' + prefix + '_*'
    return namespace


def normalize_shape(value: Any, *, prefixes: list[str], key: str = '', parent: str = '', depth: int = 0, literal: bool = False) -> Any:
    if depth > 40:
        raise ValueError('command_nesting_limit')
    if key in {'filter', 'query', 'q', '$match', '$literal'} or (key in {'u','update'} and isinstance(value,dict)):
        literal = True
    elif key == '$expr':
        literal = False
    if isinstance(value, dict):
        if len(value) == 1 and next(iter(value), '') in {'$date', '$oid', '$numberLong', '$numberInt', '$numberDouble', '$binary', '$timestamp', '$regularExpression'}:
            return {next(iter(value)): '?'}
        # Arrays preserve sort document ordering, including nested pipeline sorts.
        ordered = key in {'sort', '$sort', 'hint'} and not literal
        items = [(k, normalize_shape(value[k], prefixes=prefixes, key=k, parent=key, depth=depth+1, literal=literal))
                 for k in (value if ordered else sorted(value)) if k not in NOISE]
        return {'$ordered': items} if ordered else dict(items)
    if isinstance(value, list):
        # Preserve pipeline order; literal $in arrays retain type/length shape.
        return [normalize_shape(v, prefixes=prefixes, key='' if key in {'u','update'} else key, parent=parent, depth=depth+1, literal=literal) for v in value]
    if key in COMMANDS + ('collection',) and isinstance(value, str) and not literal:
        return family('_.' + value, prefixes).split('.', 1)[1]
    if key in {'key', 'hint'} and isinstance(value, str) and not literal:
        return value
    if not literal and (key in {'upsert', 'multi', 'ordered', 'new', 'allowDiskUse'} or parent in {'sort', '$sort', 'projection', '$project', 'hint'}):
        if isinstance(value, (bool, int)):
            return value
    if isinstance(value, str) and value.startswith('$') and not literal:
        return value
    return '?' + ('null' if value is None else 'bool' if isinstance(value, bool) else
                   'number' if isinstance(value, (int, float)) else 'string')


def normalize_record(record: dict, instance: str, prefixes: list[str]) -> dict:
    q = json.loads(record['SQLText'])
    if not isinstance(q, dict):
        raise ValueError('invalid_slow_document')
    cmd = q.get('command', q.get('query', {}))
    if not isinstance(cmd, dict):
        raise ValueError('invalid_command_document')
    op = next((name for name in COMMANDS + DATABASE_COMMANDS if name in cmd), str(q.get('op', 'unknown')))
    kind = 'suboperation' if q.get('op') in {'update', 'insert', 'remove', 'delete'} and not any(name in cmd for name in COMMANDS) else 'command'
    ns = q.get('ns') or (str(record.get('DBName') or '') + '.' + str(record.get('TableName') or ''))
    if not isinstance(ns, str):
        raise ValueError('invalid_namespace')
    if ns.endswith('.$cmd') and op in COMMANDS and isinstance(cmd.get(op), str):
        ns = ns[:-4] + cmd[op]
    database, separator, collection = ns.partition('.')
    known_database_command = op in DATABASE_COMMANDS or (op in {'aggregate', 'bulkWrite'} and cmd.get(op) == 1)
    database_scope = bool(database) and (not collection or (collection == '$cmd' and known_database_command)) and (
        q.get('op') == 'command' and bool(cmd) and (op not in COMMANDS or known_database_command))
    if database_scope:
        # DDS can report dbStats as just the database name. Do not invent a
        # collection, drop the record, or let it block the entire source window.
        ns = database
        if op in {'unknown', 'command'}:
            LOGGER.warning('mongo_normalization incomplete: database=%s reason=unrecognized_database_command',database)
    elif not database or not separator or not collection:
        raise ValueError('namespace_missing')
    truncated = '$truncated' in cmd or op in {'unknown', 'command'}
    shape = normalize_shape(cmd, prefixes=prefixes)
    if truncated:
        shape = {'incomplete': True, 'source_hash': q.get('queryShapeHash') or digest(cmd)}
    profile = dict(version=VERSION, engine='mongodb', namespace=family(ns, prefixes),
                   database=ns.split('.', 1)[0], command=op, kind=kind, shape=shape, incomplete=truncated)
    if database_scope:
        profile['scope'] = 'database'
    start = epoch_us(record['ExecutionStartTime'])
    ms = number(record.get('QueryTimes', q.get('durationMillis', q.get('millis'))))
    if ms is None:
        raise ValueError('duration_missing')
    data = q.get('storage', {}).get('data', {})
    role = str(q.get('replRole', {}).get('stateStr', 'Unknown')).title()
    values = dict(duration_us=ms*1000, docs=number(record.get('DocsExamined', q.get('docsExamined'))),
                  keys=number(record.get('KeysExamined', q.get('keysExamined'))),
                  returned=number(record.get('ReturnRowCounts', q.get('nreturned'))),
                  cpu_ns=number(q.get('cpuNanos')), bytes_read=number(data.get('bytesRead')),
                  read_us=number(data.get('timeReadingMicros')), planning_us=number(q.get('planningTimeMicros')),
                  modified=number(q.get('nModified')))
    wait = number(q.get('waitForWriteConcernDuration'))
    values['write_wait_us'] = None if wait is None else wait * 1000
    sample = dict(namespace=ns, command=op, kind=kind, shape=shape, start_us=start, duration_us=ms*1000,
                  plan=q.get('planSummary'), used_disk=q.get('usedDisk'), error_code=q.get('errCode'),
                  error_name=q.get('errName'), role=role, costs=values,
                  query_shape_hash=q.get('queryShapeHash'), plan_cache_key=q.get('planCacheKey'))
    if database_scope:
        sample['scope'] = 'database'
    return dict(instance=instance, namespace=ns, role=role, command=op, kind=kind,
                group_id=digest(profile), profile=canonical(profile), start_us=start,
                finish_us=start+ms*1000, sample=canonical(sample),
                failed=int(q.get('ok') == 0 or bool(q.get('errCode'))),
                collscan=int(q.get('planSummary') == 'COLLSCAN'), spill=int(q.get('usedDisk') is True),
                **values)


def rollup_events(events: list[dict]) -> list[dict]:
    rows: dict[tuple, dict] = {}
    for event in events:
        first = event['start_us'] // MINUTE * MINUTE
        # Bound corrupted durations rather than allocating an unbounded series.
        if event['duration_us'] > 24*60*MINUTE:
            raise ValueError('duration_exceeds_one_day')
        end = max(event['finish_us'], event['start_us'] + 1)
        for bucket in range(first, ((end-1)//MINUTE+1)*MINUTE, MINUTE):
            key = bucket, event['role'], event['group_id']
            if key not in rows:
                rows[key] = dict(bucket=bucket, role=event['role'], group_id=event['group_id'], profile=event['profile'],
                                 count=0, failed=0, collscan=0, spill=0, overlap_us=0, max_us=0,
                                 first_us=event['start_us'], last_us=event['start_us'], sample=event['sample'],
                                 **{k: 0 for k in COSTS}, **{k+'_known': 0 for k in COSTS})
            row = rows[key]
            row['overlap_us'] += max(0, min(end, bucket+MINUTE)-max(event['start_us'], bucket))
            if bucket != first:
                continue
            row['count'] += 1
            for k in ('failed', 'collscan', 'spill'):
                row[k] += event[k]
            for k in COSTS:
                if event[k] is not None:
                    row[k] += event[k]
                    row[k+'_known'] += 1
            row['first_us'] = min(row['first_us'], event['start_us'])
            row['last_us'] = max(row['last_us'], event['start_us'])
            if event['duration_us'] >= row['max_us']:
                row['max_us'] = event['duration_us']
                row['sample'] = event['sample']
    return list(rows.values())


def rollup_records(records: list[dict], instance: str, prefixes: list[str]) -> list[dict]:
    return rollup_events([normalize_record(r, instance, prefixes) for r in records])


def summarize_native_intervals(samples, start, end, role=''):
    groups={};gaps=[]
    for sample in samples:
        if role and sample.get('role')!=role:continue
        interval=sample.get('interval',{})
        lo=interval.get('start_us',0);hi=interval.get('end_us',0)
        if interval.get('status')!='ok' or lo<start or hi>end or hi<=lo:
            gaps.append(dict(node=sample.get('node'),timestamp=sample.get('timestamp'),reason=interval.get('status','missing_interval')))
            continue
        seconds=(hi-lo)/1e6
        for name,counts in interval.get('commands',{}).items():
            key=sample['node'],sample['role'],name
            row=groups.setdefault(key,dict(node=key[0],role=key[1],command=name,count=0,failed=0,rejected=0,seconds=0,intervals=[]))
            row['count']+=int(counts['total'])
            for field in ('failed','rejected'):
                if field not in counts:row[field]=None
                elif row[field] is not None:row[field]+=int(counts[field])
            row['seconds']+=seconds
            row['intervals'].append(dict(start_us=lo,end_us=hi,count=counts['total'],qps=counts['total']/seconds))
    for row in groups.values():
        row.update(qps=row['count']/row['seconds'],coverage_seconds=row['seconds'],window_seconds=(end-start)/1e6)
    return groups,gaps


def counter_interval(before: dict | None, after: dict) -> dict:
    status = 'ok'
    if before is None:
        status = 'initial_sample'
    elif before['node'] != after['node']:
        status = 'node_changed'
    elif before['epoch'] != after['epoch']:
        status = 'process_restarted'
    elif before.get('role') != after.get('role'):
        status = 'role_changed'
    elif after['time_us'] <= before['time_us']:
        status = 'invalid_time'
    result = dict(status=status, node=after['node'], role=after.get('role'),
                  start_us=before['time_us'] if before else after['time_us'], end_us=after['time_us'], commands={})
    if status != 'ok':
        LOGGER.warning('mongo_counter_interval unavailable: %s node=%s', status, after['node'])
        return result
    for command, current in after.get('commands', {}).items():
        old = before.get('commands', {}).get(command)
        if old is None:
            LOGGER.warning('mongo_counter_interval command_initial_sample: %s', command)
            continue
        delta = {}
        for k in ('total', 'failed', 'rejected'):
            if k in current and k in old:
                delta[k] = current[k] - old[k]
                if delta[k] < 0:
                    result.update(status='counter_reset', commands={})
                    LOGGER.warning('mongo_counter_interval unavailable: counter_reset node=%s', after['node'])
                    return result
        result['commands'][command] = delta
    result['interval_seconds'] = (after['time_us'] - before['time_us'])/1e6
    return result


def summarize(rows: list[dict]) -> dict:
    result = {}
    for row in rows:
        key = (row['role'], row['group_id'])
        if key not in result:
            result[key] = dict(profile=json.loads(row['profile']), count=0, failed=0, collscan=0, spill=0,
                               max_us=0, first_us=row['first_us'], last_us=row['last_us'], sample=row['sample'],
                               trend=defaultdict(int), runtime=defaultdict(int),
                               **{k: 0 for k in COSTS}, **{k+'_known': 0 for k in COSTS})
        a = result[key]
        for k in ('count','failed','collscan','spill') + COSTS + tuple(k+'_known' for k in COSTS):
            a[k] += int(row[k])
        a['first_us'] = min(a['first_us'], row['first_us'])
        a['last_us'] = max(a['last_us'], row['last_us'])
        if row['max_us'] >= a['max_us']:
            a['max_us'] = row['max_us']
            a['sample'] = row['sample']
        a['trend'][int(row['bucket'])] += row['count']
        a['runtime'][int(row['bucket'])] += row.get('overlap_us', 0)
    return result


def analyze(rows: list[dict], baseline: list[dict], metrics: list[dict], start: int, end: int,
            *, coverage: bool, baseline_coverage: bool, metric: str='CPUUtilization',
            order: str='duration_growth', limit: int=50, bucket_width: int=MINUTE) -> dict:
    now, previous = summarize(rows), summarize(baseline)
    statements = []
    metric_series: dict[str, dict[int, int]] = defaultdict(dict)
    for p in metrics:
        if p.get('metric') != metric:
            continue
        t = int(p['timestamp'])*1000
        value = p.get('value')
        if value is not None and math.isfinite(float(value)) and start < t <= end:
            metric_series[str(p.get('role', 'Unknown'))][t] = round(float(value)*1000)
    # Only report populations actually selected; filtered-out layers are not zero.
    populations = {(k[0], v['profile']['kind']) for source in (now, previous) for k,v in source.items()}
    totals = []
    for role,kind in sorted(populations):
        selected = [v for k,v in now.items() if k[0] == role and v['profile']['kind'] == kind]
        before = [v for k,v in previous.items() if k[0] == role and v['profile']['kind'] == kind]
        totals.append(dict(role=role,kind=kind,count=sum(v['count'] for v in selected),
                           baseline_count=sum(v['count'] for v in before) if baseline_coverage else None))
    for key, a in now.items():
        role, group_id = key
        b = previous.get(key)
        row = dict(group_id=group_id, role=role, **a['profile'], count=a['count'],
                   failed=a['failed'], collscan=a['collscan'], spill=a['spill'], max_us=a['max_us'],
                   first_us=a['first_us'], last_us=a['last_us'], sample=json.loads(a['sample']),
                   trend=[{'bucket':t,'count':v,'runtime_us':a['runtime'].get(t,0)} for t,v in sorted(a['trend'].items())])
        valid = coverage and baseline_coverage
        row['baseline_count'] = (b['count'] if b else 0) if baseline_coverage else None
        row['count_delta'] = a['count'] - row['baseline_count'] if valid else None
        row['new_slow_shape'] = valid and row['baseline_count'] == 0
        row['costs'] = {}
        for k in COSTS:
            known = a[k+'_known']
            before_known = b[k+'_known'] if b else 0
            value = a[k] if known else None
            before_value = (b[k] if b else 0) if baseline_coverage and (not b or before_known) else None
            complete_cost = known == a['count'] and (not b or before_known == b['count'])
            delta = value-before_value if value is not None and before_value is not None and valid and complete_cost else None
            row['costs'][k] = dict(observed=value, known=known, total=a['count'], baseline=before_value, delta=delta,
                                   baseline_known=before_known, baseline_total=b['count'] if b else 0)
        row['avg_us'] = a['duration_us']/a['count'] if a['count'] else None
        n0 = row['baseline_count']
        c0 = b['duration_us']/b['count'] if valid and b and b['count'] else None
        row['frequency_cost_delta_us'] = (a['count']-n0)*c0 if c0 is not None else None
        row['per_call_cost_delta_us'] = a['count']*(row['avg_us']-c0) if c0 is not None and row['avg_us'] is not None else None
        points = metric_series.get(role,{})
        expected = list(range((start//MINUTE+1)*MINUTE,(end//MINUTE+1)*MINUTE,MINUTE))
        stock_metric = metric in {'MemoryUtilization','WtCacheUsage','CentralCacheFree','TcmallocCacheMemRatio'}
        points_ok = bool(expected) and all(t in points for t in expected) and bucket_width == MINUTE and not stock_metric
        r = dr = None
        reason = ('memory_requires_component_deltas' if stock_metric else 'coarse_grain_zoom_required' if bucket_width != MINUTE
                  else 'metric_gaps' if not points_ok else 'insufficient_buckets')
        overlap = 0
        precedes = False
        peak_time = None
        if points_ok:
            y = [points[t] for t in expected]
            x = [a['runtime'].get(t-MINUTE,0) for t in expected]
            peak_time = expected[y.index(max(y))]
            precedes = coverage and a['first_us'] >= peak_time
            low = sorted(y)[(len(y)-1)//5]
            overlap = sum(v*max(m-low,0) for v,m in zip(x,y))
            if len(x) >= 6:
                r, reason = pearson(x,y)
                dr, _ = pearson(differences(x),differences(y))
        # Current-window correlation does not require historical baseline data.
        correlation_valid = coverage and points_ok
        if not coverage:
            reason = 'incomplete_source'
        row['evidence'] = dict(metric=metric,metric_status=reason,pearson=r if correlation_valid else None,
                               difference_r=dr if correlation_valid else None, resource_overlap=str(overlap) if correlation_valid else None,
                               peak_end_us=peak_time,resource_peak_precedes_candidate=precedes,
                               source_scope='collected_slow_records',count_is_total_execution=False,
                               direct_cost_complete={k:a[k+'_known']==a['count'] for k in ('cpu_ns','bytes_read','docs')})
        growth = row['count_delta'] is not None and row['count_delta'] > 0
        cost_growth = any((row['costs'][k]['delta'] or 0)>0 for k in ('duration_us','docs','cpu_ns','bytes_read'))
        row['conclusion'] = 'candidate' if valid and cost_growth and not precedes and not row['incomplete'] else 'insufficient_evidence'
        row['assessment'] = ('incomplete_source' if not coverage else 'incomplete_baseline' if not baseline_coverage
                             else 'incomplete_command' if row['incomplete'] else 'after_peak' if precedes
                             else 'candidate' if cost_growth else 'insufficient_cost_fields'
                             if any(row['costs'][k]['delta'] is None for k in ('duration_us','docs','cpu_ns','bytes_read'))
                             else 'no_observed_growth')
        row['exclusions'] = []
        row['evidence']['sample_after_resource_peak'] = bool(peak_time is not None and row['sample']['start_us'] >= peak_time)
        if row['evidence']['sample_after_resource_peak']:
            row['exclusions'].append('代表慢记录开始晚于资源峰值，不能用这条样本解释更早异常')
        if valid and growth and not cost_growth:
            row['exclusions'].append('慢记录次数增加，但已知累计成本未增加；不是已确认的性能来源')
        if precedes:
            row['exclusions'].append('该命令首次出现晚于资源峰值，不能解释更早异常')
        if valid and not growth:
            row['exclusions'].append('该模板慢记录次数未增长')
        if not valid:
            row['exclusions'].append('来源或基线未完整，不能计算异常增量')
        if row['kind']=='suboperation':
            row['exclusions'].append('内部操作与外层命令不可相加')
        if row['incomplete']:
            row['exclusions'].append('命令正文截断或类型不完整')
        statements.append(row)
    requested_order = order
    order_reason = ''
    order_cost = {'duration_growth':'duration_us','scan_growth':'docs','cpu_growth':'cpu_ns','read_growth':'bytes_read'}
    total_cost = {'duration_total':'duration_us','scan_total':'docs','cpu_total':'cpu_ns','read_total':'bytes_read'}
    if not (coverage and baseline_coverage) and order in (*order_cost, 'count_growth', 'performance'):
        order = {'count_growth':'count','scan_growth':'scan_total','cpu_growth':'cpu_total','read_growth':'read_total'}.get(order,'duration_total')
        order_reason = 'incomplete_source' if not coverage else 'incomplete_baseline'
    if statements and ((order in order_cost and not any(r['costs'][order_cost[order]]['delta'] is not None for r in statements))
                       or (order == 'performance' and not any(r['evidence']['resource_overlap'] is not None for r in statements))):
        order = {'scan_growth':'scan_total','cpu_growth':'cpu_total','read_growth':'read_total'}.get(order,'duration_total')
        order_reason = 'ranking_evidence_unavailable'
    if statements and order in total_cost and not any(r['costs'][total_cost[order]]['observed'] is not None for r in statements):
        order = 'count';order_reason = 'ranking_field_unavailable'
    if order_reason:
        LOGGER.warning('mongo_analysis ordering changed: requested=%s effective=%s reason=%s',requested_order,order,order_reason)
    def score(row):
        if order=='count_growth': return row['count_delta'] if row['count_delta'] is not None else -1
        if order=='count': return row['count']
        if order=='max_latency': return row['max_us']
        if order in total_cost:
            v = row['costs'][total_cost[order]]['observed']
            return v if v is not None else -1
        if order=='performance': return int(row['evidence']['resource_overlap'] or 0) if row['conclusion']=='candidate' else -1
        c = order_cost.get(order,'duration_us')
        v = row['costs'][c]['delta']
        return v if v is not None else -1
    statements.sort(key=lambda row:(-score(row),row['role'],row['group_id']))
    unavailable=Counter(row['evidence']['metric_status'] for row in statements if row['evidence']['metric_status']!='ok')
    if unavailable:
        LOGGER.warning('mongo_resource_correlation unavailable: %s',dict(unavailable))
    status = 'ok' if coverage and baseline_coverage else 'incomplete_source' if not coverage else 'incomplete_baseline'
    if status != 'ok':
        LOGGER.warning('mongo_analysis unavailable: %s',status)
    return dict(engine='mongodb',status=status,statements=statements[:max(1,min(limit,200))],
                outliers=sorted(statements,key=lambda row:(-row['max_us'],row['group_id']))[:3],
                total_groups=len(statements),totals=totals,order=order,requested_order=requested_order,order_reason=order_reason,
                metric=metric,start_us=start,end_us=end,
                bucket_width=bucket_width,warning='慢记录不是全部执行；相关与重合不是因果。父子操作分层统计。',
                command_count_scope='serverStatus_node_native_counts',client_count_scope='not_connected')
