"""StarRocks read-only engine adapter. No caller-provided SQL or connection target.

The source audit table, not a guessed CDC interface, owns historical retention.
Bounded raw windows are explicitly partial and never treated as complete totals.
"""
from __future__ import annotations

import hashlib
import json
import logging
import math
import os
import re
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .sql_fingerprint import normalize_sql, sql_action
from .starrocks_metrics import StarRocksMetrics

LOGGER = logging.getLogger(__name__)
AUDIT = '`_starrocks_audit_db_`.`starrocks_audit_tbl`'
MAX_SCAN_ROWS = 30000
MAX_SCAN_BYTES = 32 * 1024 * 1024
MAX_DISPLAY_BYTES = 2 * 1024 * 1024
FIELDS = ('queryId','timestamp','queryType','clientIp','user','db','state','errorCode','queryTime',
          'scanBytes','scanRows','returnRows','cpuCostNs','memCostBytes','stmtId','isQuery','feIp','digest','pendingTimeMs')
COSTS = ('queryTime','scanBytes','scanRows','cpuCostNs','pendingTimeMs')
SQL_COLUMNS = ','.join('`'+key+'`' for key in FIELDS) + ',LEFT(`stmt`,32768) AS stmt,LENGTH(`stmt`) AS stmt_bytes'


def one(params, key, default=''):
    value = params.get(key, default)
    if isinstance(value, list):
        if len(value) != 1:
            raise ValueError('duplicate_parameter')
        value = value[0]
    return str(value)


def number(value):
    if isinstance(value, bool) or value is None:
        return None
    try:
        n = float(value)
        return int(n) if math.isfinite(n) and n >= 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def epoch_us(value):
    dt = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone(timedelta(hours=8)))
    return int(dt.timestamp()*1_000_000)


def identity_key(row):
    # SQL query and slow_query can be two emissions of one execution. Keep
    # connection/unknown events separate; a missing query id must not collapse.
    query = str(row.get('queryId') or '')
    base = [query, str(row.get('timestamp')), str(row.get('feIp')), str(row.get('stmtId'))]
    if not query:
        base += [row.get('queryType'), row.get('clientIp'), row.get('user'), row.get('stmt')]
    return hashlib.sha256(json.dumps(base, ensure_ascii=False).encode()).hexdigest()


def validate_grants(rows, username):
    if not rows:
        raise RuntimeError('starrocks_grants_unavailable')
    for row in rows:
        grant = row.get('Grants', '')
        match = re.match(r'^GRANT\s+(SELECT|USAGE)\s+ON\s+.+\s+TO USER\s+', grant, re.I)
        if not match or re.search(r'WITH\s+GRANT\s+OPTION', grant, re.I):
            raise RuntimeError('starrocks_identity_not_readonly')
        if str(row.get('UserIdentity')) != f"'{username}'@'%'":
            raise RuntimeError('starrocks_grant_identity_mismatch')


def aggregate(rows, truncated=False):
    """Work costs belong to statements, never duplicated across JOIN tables."""
    unique = {}
    conflicts = 0
    for row in rows:
        key = identity_key(row)
        previous = unique.get(key)
        if previous is not None:
            if any(previous.get(k) != row.get(k) for k in COSTS):
                conflicts += 1
            if str(row.get('queryType')) == 'slow_query':
                unique[key] = row
        else:
            unique[key] = row
    groups = {}
    for row in unique.values():
        text = str(row.get('stmt') or '')
        normalized = normalize_sql(text)
        cut = (number(row.get('stmt_bytes')) or 0) > len(text.encode('utf-8')) or normalized.endswith(' …')
        # Truncated/unknown statements are not merged into a full template.
        source = normalized if not cut else 'truncated:'+identity_key(row)
        key = hashlib.sha256(json.dumps([row.get('catalog','default_catalog'),row.get('db'),source]).encode()).hexdigest()
        g = groups.setdefault(key, dict(id=key, database=row.get('db') or '', sql=normalized,
            action=sql_action(text), count=0, failed=0, slow=0, max_ms=None, peak_memory=None,
            truncated_sql=cut, clients=Counter(), accounts=Counter(), costs={k:{'value':0,'known':0} for k in COSTS}))
        g['count'] += 1
        g['failed'] += int(row.get('state') == 'ERR')
        ms = number(row.get('queryTime'))
        g['slow'] += int(ms is not None and ms >= 1000)
        if ms is not None:g['max_ms'] = max(g['max_ms'] or 0, ms)
        mem = number(row.get('memCostBytes'))
        if mem is not None:g['peak_memory'] = max(g['peak_memory'] or 0, mem)
        for name in ('clients','accounts'):
            v = row.get('clientIp' if name=='clients' else 'user')
            if v:g[name][str(v)] += 1
        for name in COSTS:
            n = number(row.get(name))
            if n is not None:
                g['costs'][name]['value'] += n
                g['costs'][name]['known'] += 1
    values = list(groups.values())
    for g in values:
        g['clients'] = [{'name':k,'count':v} for k,v in g['clients'].most_common(20)]
        g['accounts'] = [{'name':k,'count':v} for k,v in g['accounts'].most_common(10)]
        for c in g['costs'].values():
            if not c['known']:c['value'] = None
    return {'groups':values, 'observed_executions':len(unique),'source_records':len(rows),
            'duplicate_records':len(rows)-len(unique),'duplicate_cost_conflicts':conflicts,
            'complete':False,'truncated':truncated,'source_scope':'observed_audit_executions',
            'warning':'只统计已读到的完成审计；长查询与插件晚到可能补入，不保证源端全量。内存取峰值，不相加。'}


class StarRocksService(StarRocksMetrics):
    def __init__(self, root, settings_loader, credential_loader, *, connect=None, **kwargs):
        super().__init__(root, settings_loader, credential_loader, **kwargs)
        self.root = Path(root)
        self.connect_factory = connect
        self._database_gate = threading.BoundedSemaphore(1)
        self._source_state = {}
        # Database configuration stays out of public status and cloud-only mode.
        self.db_entries = {}
        path = self.root/'starrocks-instances.json'
        if path.exists() and not self.error:
            value = json.loads(path.read_text(encoding='utf-8'))
            entries = value.get('instances') if isinstance(value,dict) else value
            self.db_entries = {e['instanceId']:e for e in entries if e.get('credentialsFile')}

    def status(self):
        state = super().status()
        state['audit'] = {'status':'configured' if self.db_entries else 'not_connected',
                          'message':'按需读取源审计表；不保证源端无丢失'}
        state['running_queries'] = {'status':'permission_checked_on_request', 'message':'不授予或借用管理权限'}
        for item in state['instances']:
            item['audit'] = self._source_state.get(item['id'], {'status':'not_checked'})
        return state

    def entry(self, params):
        identity = one(params,'instance')
        if not any(e['instanceId']==identity and e['enabled'] for e in self.entries):
            raise ValueError('configured_starrocks_instance_required')
        if identity not in self.db_entries:
            raise ValueError('starrocks_audit_not_configured')
        entry = self.db_entries[identity]
        host = entry.get('host')
        expected = 'fe-'+identity+'-internal.starrocks.aliyuncs.com'
        if host != expected or entry.get('port',9030) != 9030 or entry.get('readonlyUsername') != 'mcp-ro':
            raise ValueError('starrocks_fixed_database_target_required')
        return entry

    def _connect(self, entry):
        import pymysql
        if self.connect_factory is not None:
            connection = self.connect_factory(entry)
        else:
            path = Path(entry['credentialsFile'])
            if not path.is_absolute():path=self.root/path
            if path.is_symlink() or not path.is_file() or path.stat().st_size > 16384:
                raise RuntimeError('starrocks_invalid_credential_file')
            if os.name!='nt' and path.stat().st_mode & 0o077:
                raise RuntimeError('starrocks_credentials_require_0600')
            auth = json.loads(path.read_text(encoding='utf-8'))
            if auth.get('username') != entry['readonlyUsername']:
                raise RuntimeError('starrocks_credential_identity_mismatch')
            connection = pymysql.connect(host=entry['host'],port=9030,user=auth['username'],password=auth['password'],
                charset='utf8mb4',autocommit=True,connect_timeout=8,read_timeout=15,write_timeout=10,
                cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                cursor.execute('SELECT CURRENT_USER() AS who,CURRENT_ROLE() AS role,@@time_zone AS tz')
                identity = cursor.fetchone()
                if identity.get('who') != "'mcp-ro'@'%'" or identity.get('role') not in ('NONE',None):
                    raise RuntimeError('starrocks_authenticated_identity_mismatch')
                if identity.get('tz') != 'Asia/Shanghai':
                    raise RuntimeError('starrocks_source_timezone_unverified')
                cursor.execute('SHOW GRANTS')
                validate_grants(cursor.fetchall(),entry['readonlyUsername'])
                # Session-only guards; never SET GLOBAL, KILL or ADMIN.
                cursor.execute('SET query_timeout=10')
                cursor.execute('SET exec_mem_limit=268435456')
            return connection
        except Exception:
            connection.close()
            raise

    def window(self, params, hours=1):
        start,end = int(one(params,'startEpochUs')),int(one(params,'endEpochUs'))
        if start<=0 or end<=start or end-start>hours*3600*1_000_000 or end>time.time_ns()//1000+60_000_000:
            raise ValueError('starrocks_audit_window_max_one_hour')
        if start < time.time_ns()//1000-30*86400*1_000_000:
            raise ValueError('starrocks_audit_outside_source_retention')
        def local(t):return datetime.fromtimestamp(t/1e6,timezone(timedelta(hours=8))).replace(tzinfo=None)
        return start,end,local(start),local(end)

    def _read_audit(self, connection, params, *, workload=False):
        start,end,lo,hi=self.window(params)
        conditions=['`timestamp` >= %s','`timestamp` < %s',"COALESCE(`user`,'') <> %s"]
        args=[lo,hi,'mcp-ro']
        for name,column in [('database','db'),('account','user')]:
            value=one(params,name)
            if len(value)>128:raise ValueError('starrocks_filter_too_long')
            if value:conditions.append('`'+column+'`=%s');args.append(value)
        if one(params,'mode')=='slow':conditions.append('`queryTime` >= 1000')
        limit = MAX_SCAN_ROWS if workload else min(200,max(1,int(one(params,'limit','100'))))
        # A stable newest-first bounded snapshot, not offset pagination over changing data.
        sql='SELECT '+SQL_COLUMNS+' FROM '+AUDIT+' WHERE '+' AND '.join(conditions)+' ORDER BY `timestamp` DESC,`queryId` DESC,`queryType` LIMIT '+str(limit+1)
        with connection.cursor() as cur:
            cur.execute('EXPLAIN '+sql,args)
            plan='\n'.join(str(next(iter(r.values()))) for r in cur.fetchall())
            partitions=re.search(r'partitions=(\d+)/(\d+)',plan)
            if not partitions or int(partitions[1])>2:
                raise RuntimeError('starrocks_audit_partition_guard')
        # The bounded streaming cursor is closed by closing the connection, never
        # draining an unlimited result after the byte budget has been reached.
        import pymysql
        cur=connection.cursor(pymysql.cursors.SSDictCursor)
        cur.execute(sql,args)
        rows=[];size=0;truncated=False
        try:
            for row in cur:
                item=dict(row)
                size+=len(json.dumps(item,default=str,ensure_ascii=False).encode())
                if len(rows)>=limit or size>(MAX_SCAN_BYTES if workload else MAX_DISPLAY_BYTES):
                    truncated=True;break
                rows.append(item)
        finally:
            # Close the socket before the cursor destructor can drain the result.
            if cur._result is not None:
                cur._result.unbuffered_active = False
            cur._result = None
            connection._result = None
            connection.close()
        return rows,dict(start_us=start,end_us=end,truncated=truncated,complete=False,limit=limit,
                         scanned_records=len(rows),returned_bytes=min(size,MAX_SCAN_BYTES),
                         source='starrocks_audit_table',scope='bounded_newest_first_snapshot',
                         excluded_account='mcp-ro',source_retention_days=30,
                         warning='按查询开始时间筛选；完成后才入表。晚到、源端丢失及未返回区间不当作零。采集账号自身已剔除。')

    def database_query(self, params):
        entry=self.entry(params)
        mode=one(params,'mode','audit')
        if mode not in ('audit','slow','workload','loads','running'):
            raise ValueError('unsupported_starrocks_mode')
        if not self._database_gate.acquire(blocking=False):raise RuntimeError('starrocks_database_busy')
        connection=None
        try:
            connection=self._connect(entry)
            if mode=='running':
                try:
                    with connection.cursor() as cur:
                        cur.execute("SHOW PROC '/global_current_queries'")
                        rows=cur.fetchmany(201)
                    return {'mode':mode,'rows':rows[:200],'truncated':len(rows)>200,'complete':False,
                            'warning':'运行态快照，不是全部完成请求或历史。'}
                except Exception as exc:
                    if getattr(exc,'args',())[0:1] == (5203,):
                        return {'mode':mode,'rows':[],'status':'permission_denied','complete':False,
                                'warning':'当前只读账号缺少 SYSTEM OPERATE，无法查看全局在途查询；未授权、未用其他身份回退。'}
                    raise
            if mode=='loads':
                start,end,lo,hi=self.window(params)
                with connection.cursor() as cur:
                    cur.execute('SELECT ID,LABEL,DB_NAME,TABLE_NAME,USER,STATE,TYPE,SCAN_ROWS,SCAN_BYTES,SINK_ROWS,CREATE_TIME,LOAD_FINISH_TIME FROM information_schema.loads WHERE CREATE_TIME >= %s AND CREATE_TIME < %s ORDER BY CREATE_TIME DESC LIMIT 201',(lo,hi))
                    rows=cur.fetchall()
                return {'mode':mode,'rows':rows[:200],'truncated':len(rows)>200,'complete':False,
                        'warning':'导入作业统计，不是逐行变更，也不保证所有导入类型的历史均已留存。'}
            rows,coverage=self._read_audit(connection,params,workload=mode=='workload')
            self._source_state[entry['instanceId']]={'status':'ok','checked_ms':time.time_ns()//1_000_000}
            if mode=='workload':
                result=aggregate(rows,coverage['truncated'])
                order=one(params,'order','cpuCostNs')
                if order not in ('count',*COSTS):raise ValueError('unsupported_starrocks_order')
                groups=result['groups']
                groups.sort(key=lambda g: -(g['count'] if order=='count' else (g['costs'][order]['value'] or 0)))
                result.update(mode=mode,coverage=coverage,order=order,total_groups=len(groups))
                result['groups']=groups[:min(200,max(1,int(one(params,'limit','50'))))]
                return result
            output=[]
            for row in rows:
                row['event_us']=epoch_us(row['timestamp'])
                row['id']=identity_key(row)
                row['sql_truncated']=(number(row.get('stmt_bytes')) or 0)>len(str(row.get('stmt') or '').encode('utf-8'))
                output.append(row)
            return {'mode':mode,'rows':output,'coverage':coverage,'warning':coverage['warning']}
        finally:
            if connection is not None and connection.open:connection.close()
            self._database_gate.release()
