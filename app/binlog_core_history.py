"""Rolling Binlog read boundary; the audit publication markers own old coverage."""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import time
from contextlib import closing
from datetime import UTC, datetime
from pathlib import Path

from .binlog_lite import RawBinlogError

TIER = 'clickhouse-core-history'
TABLE = 'insight.binlog_core_changes_v1'
TWO_DAYS_US = 2 * 86400 * 1_000_000
FIELDS = ('instance_id,database_name,table_name,event_epoch_us,operation,transaction_id,gtid,'
          'source_file_name,start_position,end_position,row_index,`column`,before_present,after_present,'
          'before_value,after_value,redacted,key_before_json,key_after_json,change_id')


def cutoff():
    return time.time_ns() // 1000 - TWO_DAYS_US


def load(path):
    return json.loads(path.read_text(encoding='utf-8-sig'))


def scope(meta, instance):
    if not isinstance(meta, Path) or not instance:
        return None
    for epoch in meta.parent.glob('.core-binlog-shadow.*/scope-epoch.json'):
        root = epoch.parent
        e = load(epoch)
        pred = load(root / ('sample-' + e['predecessor']) / 'published.json')
        if pred['instance_id'] == instance:
            return root, e
    return None


def source_span(meta, configured, database, table):
    root, epoch = configured
    legacy = {t['table']: t for t in load(root/'anchor.json')['tables']}
    expanded = root/'expanded-anchor.json'
    if hashlib.sha256(expanded.read_bytes()).hexdigest() != epoch['anchor_sha256']:
        raise RawBinlogError('列语义范围校验失败', 'CORE_HISTORY_UNAVAILABLE')
    rules = {t['table']: t for t in load(expanded)['tables']}
    target = database+'.'+table
    if target not in rules:
        return None, ()
    start = int(epoch['first'].split('.')[-1])
    seq = {}
    instance = load(root/('sample-'+epoch['predecessor'])/'published.json')['instance_id']
    for marker in root.glob('sample-*/published.json'):
        m = load(marker)
        if (m.get('mode') != 'published' or m.get('instance_id') != instance
                or m.get('readback_rows') != m.get('expected_rows') or not re.fullmatch(r'mysql-bin\.\d+', m['file'])):
            raise ValueError('invalid publication marker')
        n = int(m['file'].split('.')[-1])
        if n in seq or marker.parent.name != 'sample-'+m['file_id']:
            raise ValueError('conflicting publication marker')
        seq[n] = m['file_id']
    first = min(seq) if target in legacy else start
    if first not in seq:
        return None, ()
    last = first
    while last+1 in seq:
        last += 1
    with closing(sqlite3.connect(meta.as_uri()+'?mode=ro', uri=True, timeout=3)) as db:
        def source(n):
            r = db.execute('SELECT log_begin_utc,log_end_utc FROM binlog_files WHERE id=?', (seq[n],)).fetchone()
            if not r or not all(r):
                raise ValueError('missing published source time')
            return [int(datetime.fromisoformat(s.replace('Z','+00:00')).timestamp()*1e6) for s in r]
        span = (source(first)[0], source(last)[1])
    keys = tuple(x['name'] for x in rules[target]['columns'] if x['k']=='PRI' and not x.get('redacted'))
    return span, keys


def statement(query, lo, hi, keys, *, change_id=None):
    params = {'i': query['instance'], 'd': query['database'], 'b': query['table'], 'lo': lo, 'hi': hi}
    terms = ['instance_id={i:String}', 'database_name={d:String}', 'table_name={b:String}',
             "event_date BETWEEN toDate(fromUnixTimestamp64Micro({lo:Int64}, 'UTC')) AND toDate(fromUnixTimestamp64Micro({hi:Int64}, 'UTC'))",
             'event_epoch_us BETWEEN {lo:Int64} AND {hi:Int64}']
    if change_id is not None:
        params['cid'] = change_id
        terms.append('change_id={cid:String}')
    if query.get('transaction'):
        params['tx'] = str(query['transaction'])
        terms.append('(gtid={tx:String} OR transaction_id={tx:String})')
    if query.get('operations'):
        params['op'] = json.dumps([str(x).upper() for x in query['operations']])
        terms.append("has(JSONExtract({op:String}, 'Array(String)'), operation)")
    exact = query.get('exact') or {}
    if exact:
        if len(keys) != 1 or str(exact.get('kind')).upper() != 'PRIMARY_KEY':
            raise RawBinlogError('此历史审计表没有可用的单列主键；请用关键词', 'INDEX_QUERY_UNSUPPORTED')
        params.update(k=keys[0], pk=str(exact['value']), pkjson=json.dumps(str(exact['value']), ensure_ascii=False))
        terms.append("(JSONExtractRaw(ifNull(key_before_json,''),{k:String}) IN ({pk:String},{pkjson:String}) OR JSONExtractRaw(ifNull(key_after_json,''),{k:String}) IN ({pk:String},{pkjson:String}))")
    search = []
    for i, word in enumerate(str(query.get('keyword') or '').split()[:20]):
        params[f'w{i}'] = word
        search.append(f"positionCaseInsensitiveUTF8(concat(`column`,' ',ifNull(before_value,''),' ',ifNull(after_value,''),' ',ifNull(key_before_json,''),' ',ifNull(key_after_json,'')),{{w{i}:String}})>0")
    if search:
        terms.append('('+(' OR ' if str(query.get('keyword_mode')).upper()=='OR' else ' AND ').join(search)+')')
    return (f'SELECT {FIELDS} FROM {TABLE} FINAL WHERE '+' AND '.join(terms)
            +' ORDER BY event_epoch_us DESC, source_file_name DESC, end_position DESC, row_index DESC, change_id DESC'), params


SETTINGS = {'max_threads': 1, 'max_final_threads': 1, 'max_memory_usage': 250000000,
            'max_block_size': 2048, 'max_execution_time': 20,
            'remote_filesystem_read_method': 'read', 'allow_prefetched_read_pool_for_remote_filesystem': 0}


def present(row):
    out = dict(row)
    out['event_id'] = row['change_id']
    out['sql_kind'] = 'CORE_HISTORY'
    out['raw_event_type'] = 'CORE_HISTORY'
    out['row_query'] = out['sql_text'] = ''
    col = json.dumps(row['column'], ensure_ascii=False)
    for side in ('before', 'after'):
        value = row[side+'_value']
        if not row[side+'_present']:
            out[side+'_json'] = ''
        else:
            text = json.dumps('已脱敏', ensure_ascii=False) if row['redacted'] else value
            out[side+'_json'] = '{'+col+':'+(text if text is not None else 'null')+'}'
    return out


def routed(rows, query, start, end, bound, configured, *, control=None):
    from .binlog_rows import build_query, day_slices, present as event, coverage_note

    db, tb = str(query.get('database') or ''), str(query.get('table') or '')
    _, _, limit, offset = build_query({**query,'exact':None}, start, end)
    recent = None
    if end >= bound:
        recent = rows._query_full({**query, 'offset':offset,'limit':limit}, max(start,bound), end, control=control)
    results = list(recent['rows']) if recent else []
    intervals = list(recent['covered_intervals']) if recent else []
    gaps = list(recent['coverage_gaps']) if recent else []
    old_end = min(bound-1, end)
    span, keys = source_span(rows.metadata.path, configured, db, tb)
    lo = max(start, span[0]) if span else start
    hi = min(old_end, span[1]) if span else start-1
    if hi >= lo:
        intervals.insert(0,[lo,hi])
        if start < lo:gaps.insert(0,{'start':start,'end':lo-1,'reason':'列语义未覆盖','files':0})
        if hi < old_end:gaps.append({'start':hi+1,'end':old_end,'reason':'列语义未覆盖','files':0})
    else:
        gaps.insert(0,{'start':start,'end':old_end,'reason':'列语义未覆盖','files':0})
    more = bool(recent and recent['has_more'])
    if hi >= lo and not more:
        # Recent query fast-stops on full pages. Only when it exhausts the window
        # do we need its exact filtered count to position an older page.
        old_offset = 0
        if recent and offset and not results:
            if offset == 1:
                page = rows._query_full({**query,'offset':0,'limit':1},bound,end,control=control)
                old_offset=offset-len(page['rows'])
            else:
                got=0
                while got < offset:
                    page=rows._query_full({**query,'offset':got,'limit':min(1000,offset-got)},bound,end,control=control)
                    got += len(page['rows'])
                    if not page['has_more']:break
                old_offset=offset-got
        elif not recent:
            old_offset=offset
        wanted = old_offset + limit - len(results) + 1
        read = []
        deadline = time.monotonic() + SETTINGS['max_execution_time']
        for a,b in day_slices(lo,hi):
            if control is not None:control.check_cancelled()
            remaining = deadline-time.monotonic()
            if remaining < 1:
                raise RawBinlogError('列审计查询超时，请缩小范围','QUERY_DEADLINE_EXCEEDED')
            sql, params = statement(query,a,b,keys)
            read += rows.ch.rows(sql+f' LIMIT {wanted-len(read)}', params=params, settings={**SETTINGS,'max_execution_time':int(remaining)},timeout=remaining+2)
            if len(read) >= wanted:break
        delta = [event(present(r),{}) for r in read[old_offset:]]
        more = len(results)+len(delta)>limit
        results.extend(delta)
    covered = sum(max(0,b-a+1) for a,b in intervals)
    summary={'gaps':gaps,'covered_us':covered,'requested_us':end-start+1}
    return {**(recent or {}), 'rows':results[:limit], 'has_more':more,'limit':limit,'offset':offset,
            'tiers_used':['clickhouse-binlog-rows',TIER], 'range_requests':0,'range_bytes':0,
            'exact_index_complete':False,'coverage_found':bool(intervals),'covered_intervals':intervals,
            'coverage_gaps':gaps[:200],'coverage_gap_count':len(gaps),'covered_us':covered,'requested_us':end-start+1,
            'range_start_epoch_us':start,'range_end_epoch_us':end,
            'coverage_note':coverage_note(summary)+'；两天内查完整记录，两天前只查已覆盖列审计，旧记录无原 SQL',
            'core_history':True, 'raw_cutoff_epoch_us':bound}
