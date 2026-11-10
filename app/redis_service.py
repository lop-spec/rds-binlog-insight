"""Redis telemetry in SQL Insight. Local bounded history, no source writes."""
from __future__ import annotations
import hashlib
import hmac
import json
import logging
import math
import os
import re
import shutil
import sqlite3
import threading
import time
import zlib
from dataclasses import replace
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .redis_protocol import RedisWire, delta, load_instances, parse_info, private_json, safe_key
from .slow_log_collector import DasRpcClient

LOGGER = logging.getLogger(__name__)
DAY = 86400
WARNING = ('累计usec是命令执行耗时，不直接等同CPU；EVAL/EXEC与内部命令可能嵌套，禁止相加计算总CPU。'
           '键级样本是客户端耗时（含网络排队），不等同服务端执行耗时；缺失不补零。')


class KvClient(DasRpcClient):
    VERSION = '2015-01-01'


def one(params, key, default=''):
    value = params.get(key, default)
    if isinstance(value, list):
        if len(value) != 1:
            raise ValueError('duplicate_parameter')
        value = value[0]
    return str(value)


def pack(value):
    return zlib.compress(json.dumps(value, separators=(',', ':')).encode(), 1)


def unpack(value):
    return json.loads(zlib.decompress(value)) if isinstance(value, bytes) else json.loads(value)


def iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')


class RedisService:
    def __init__(self, root, settings_loader, credential_loader, *, wire_factory=None, cloud_factory=None, clock=time.time):
        self.root = Path(root)
        self.clock = clock
        self.settings_loader, self.credential_loader = settings_loader, credential_loader
        self.wire_factory = wire_factory or (lambda e: RedisWire(e, self.root))
        self.cloud_factory = cloud_factory
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.thread = None
        self.error = ''
        self.entries = []
        self.last = {}
        self.meta = {}
        self.next_cloud = {}
        self.next_buckets = {}
        self.health = {}
        self.bounds = {}
        self.last_sample = {}
        self.last_housekeeping = 0
        self.ingest_times = []
        try:
            self.entries = [e for e in load_instances(self.root) if e.get('enabled')]
        except Exception as exc:
            self.error = type(exc).__name__
        self.dbpath = self.root / 'redis-history.sqlite3'
        if self.entries:
            self._init_db()
            with self.db() as c:
                for r in c.execute('SELECT instance,node,payload FROM snapshots WHERE (instance,node,ts) IN (SELECT instance,node,MAX(ts) FROM snapshots GROUP BY instance,node)'):
                    self.last[(r['instance'], r['node'])] = unpack(r['payload'])
                for e in self.entries:
                    ident = e['instanceId']
                    first = c.execute('SELECT ts FROM snapshots WHERE instance=? ORDER BY ts LIMIT 1', (ident,)).fetchone()
                    latest = c.execute('SELECT ts FROM snapshots WHERE instance=? ORDER BY ts DESC LIMIT 1', (ident,)).fetchone()
                    sample = c.execute('SELECT ts FROM samples WHERE instance=? ORDER BY ts DESC LIMIT 1', (ident,)).fetchone()
                    self.bounds[ident] = [first[0] if first else None, latest[0] if latest else None]
                    self.last_sample[ident] = sample[0] if sample else None

    @contextmanager
    def db(self):
        c = sqlite3.connect(self.dbpath, timeout=5)
        c.row_factory = sqlite3.Row
        c.execute('PRAGMA busy_timeout=5000')
        try:
            with c:
                yield c
        finally:
            c.close()

    def _init_db(self):
        self.root.mkdir(parents=True, exist_ok=True)
        if self.dbpath.is_symlink():
            raise RuntimeError('redis_history_symlink_rejected')
        fd = os.open(self.dbpath, os.O_WRONLY | os.O_CREAT, 0o600)
        os.close(fd)
        if os.name != 'nt':
            self.dbpath.chmod(0o600)
        with self.db() as c:
            c.executescript('''
                PRAGMA journal_mode=WAL;
                PRAGMA auto_vacuum=INCREMENTAL;
                CREATE TABLE IF NOT EXISTS snapshots(instance TEXT,node TEXT,ts REAL,payload TEXT,PRIMARY KEY(instance,node,ts));
                CREATE INDEX IF NOT EXISTS snapshot_time ON snapshots(instance,ts);
                CREATE TABLE IF NOT EXISTS intervals(instance TEXT,node TEXT,start REAL,end REAL,payload TEXT,reason TEXT,PRIMARY KEY(instance,node,end));
                CREATE INDEX IF NOT EXISTS interval_time ON intervals(instance,start,end,node);
                CREATE TABLE IF NOT EXISTS topology(instance TEXT,ts REAL,payload TEXT,PRIMARY KEY(instance,ts));
                CREATE TABLE IF NOT EXISTS buckets(instance TEXT,ts REAL,payload TEXT,PRIMARY KEY(instance,ts));
                CREATE TABLE IF NOT EXISTS metrics(instance TEXT,node TEXT,ts REAL,payload TEXT,PRIMARY KEY(instance,node,ts));
                CREATE TABLE IF NOT EXISTS slow_events(instance TEXT,node TEXT,ts REAL,id TEXT,payload TEXT,PRIMARY KEY(instance,node,id));
                CREATE INDEX IF NOT EXISTS slow_time ON slow_events(instance,ts,node);
                CREATE TABLE IF NOT EXISTS samples(instance TEXT,ts REAL,id TEXT,payload TEXT,PRIMARY KEY(instance,id));
                CREATE INDEX IF NOT EXISTS sample_time ON samples(instance,ts);
                CREATE TABLE IF NOT EXISTS collection(instance TEXT,node TEXT,ts REAL,error TEXT,PRIMARY KEY(instance,node,ts));
            ''')
        if os.name != 'nt':
            self.dbpath.chmod(0o600)

    def start(self):
        if self.entries and self.thread is None and os.environ.get('RDS_REDIS_COLLECT', '1') == '1':
            self.thread = threading.Thread(target=self._run, name='redis-telemetry', daemon=True)
            self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=5)

    def _run(self):
        due = {}
        while not self.stop_event.is_set():
            for e in self.entries:
                if self.stop_event.is_set():
                    break
                now = self.clock()
                if now < due.get(e['instanceId'], 0):
                    continue
                try:
                    self.collect(e)
                except Exception as exc:
                    self.health[e['instanceId']] = {'last_error': type(exc).__name__, 'last_attempt': now}
                    LOGGER.warning('Redis telemetry failed: %s', type(exc).__name__)
                due[e['instanceId']] = self.clock() + e['intervalSeconds']
            self.stop_event.wait(1)

    def rpc(self, e, action, params, *, das=False):
        if self.cloud_factory:
            return self.cloud_factory(e, action, params, das)
        if os.environ.get('RDS_BINLOG_CLOUD_AUTH_MODE') != 'ecs_ram_role':
            raise RuntimeError('redis_requires_ecs_role')
        base = self.settings_loader()
        cred = self.credential_loader(base.credential_target)
        if cred is None:
            raise RuntimeError('redis_cloud_credentials_missing')
        endpoint = 'https://das.cn-shanghai.aliyuncs.com' if das else f'https://r-kvstore.{e["region"]}.aliyuncs.com'
        scoped = replace(base, db_instance_id=e['instanceId'], region_id='cn-shanghai' if das else e['region'], endpoint=endpoint)
        client = (DasRpcClient if das else KvClient)(scoped, cred, timeout=8)
        result = client.call(action, {'InstanceId': e['instanceId'], **params})
        if result.get('Code') not in (None, 200, '200'):
            raise RuntimeError('redis_cloud_query_failed')
        return result

    def topology(self, e):
        accounts = self.rpc(e, 'DescribeAccounts', {'AccountName': e['username']}).get('Accounts', {}).get('Account', [])
        account = next((a for a in accounts if a.get('AccountName') == e['username']), None)
        privileges = account.get('DatabasePrivileges', {}).get('DatabasePrivilege', []) if account else []
        if not account or account.get('AccountType') != 'Normal' or account.get('AccountStatus') != 'Available' or not privileges or any(p.get('AccountPrivilege') != 'RoleReadOnly' for p in privileges):
            raise RuntimeError('redis_identity_not_readonly')
        raw = self.rpc(e, 'DescribeLogicInstanceTopology', {})
        nodes = raw.get('RedisShardList', {}).get('NodeInfo', [])
        result = []
        for n in nodes:
            name = n.get('NodeId', '')
            m = re.fullmatch(re.escape(e['instanceId']) + r'-db-(\d+)(?:#(\d+))?', name)
            if n.get('NodeType') == 'db' and m:
                result.append({'index': int(m[1]), 'node': 'db-'+m[1], 'node_id': name})
        if not result or len(result) > 32 or len({n['index'] for n in result}) != len(result):
            raise RuntimeError('redis_topology_unavailable')
        self.meta[e['instanceId']] = result
        with self.db() as c:
            c.execute('INSERT OR REPLACE INTO topology VALUES(?,?,?)', (e['instanceId'], self.clock(), json.dumps(result)))
        return result

    def record(self, e, node, raw, ts):
        snap = dict(parse_info(raw), ts=ts, node_id=node['node_id'])
        key = (e['instanceId'], node['node'])
        diff, reason = delta(self.last.get(key), snap, e['intervalSeconds'])
        with self.db() as c:
            c.execute('INSERT OR REPLACE INTO snapshots VALUES(?,?,?,?)', (*key, ts, pack(snap)))
            c.execute('INSERT OR REPLACE INTO intervals VALUES(?,?,?,?,?,?)', (*key, diff['start'] if diff else ts, ts, pack(diff), reason))
            c.execute('INSERT OR REPLACE INTO collection VALUES(?,?,?,?)', (*key, ts, ''))
        self.last[key] = snap
        bound = self.bounds.setdefault(e['instanceId'], [ts, ts])
        if bound[0] is None or ts < bound[0]:
            bound[0] = ts
        bound[1] = ts

    def collect(self, e):
        identity = e['instanceId']
        now = self.clock()
        # Stop before writing/collecting if our own budget or the host safety floor is reached.
        self.check_storage(e)
        nodes = self.meta.get(identity)
        cloud_due = now >= self.next_cloud.get(identity, 0)
        if not nodes or cloud_due:
            nodes = self.topology(e)  # no stale target fallback after a failed refresh
        wire = self.wire_factory(e)
        errors = []
        try:
            for node in nodes:
                if self.stop_event.is_set():
                    break
                try:
                    raw = wire.send('IINFO', str(node['index']), 'server', 'commandstats', 'cpu')
                    self.record(e, node, raw, self.clock())
                except Exception as exc:
                    errors.append(node['node']+':'+type(exc).__name__)
                    with self.db() as c:
                        c.execute('INSERT OR REPLACE INTO collection VALUES(?,?,?,?)', (identity, node['node'], self.clock(), type(exc).__name__))
                    # A timed-out socket may desynchronize RESP; never continue it.
                    break
            if now >= self.next_buckets.get(identity, 0) and not errors:
                rows = []
                for b in range(64):
                    reply = wire.send('INFO', 'KEY', f'pre:follow_order:outbox:zset:{{fo:{b}}}')
                    m = re.fullmatch(r'slot:(\d+)\s+node_index:(\d+)', str(reply).strip())
                    if not m or int(m[2]) not in {n['index'] for n in nodes}:
                        raise RuntimeError('redis_bucket_mapping_invalid')
                    rows.append({'bucket': b, 'slot': int(m[1]), 'node_index': int(m[2])})
                with self.db() as c:
                    c.execute('INSERT OR REPLACE INTO buckets VALUES(?,?,?)', (identity, self.clock(), json.dumps(rows)))
                self.next_buckets[identity] = now + 600
        finally:
            wire.close()
        if cloud_due:
            try:
                self.cloud_poll(e, nodes, now)
                self.next_cloud[identity] = now + 60
            except Exception as exc:
                errors.append('cloud:'+type(exc).__name__)
                self.next_cloud[identity] = now + 60
        self.health[identity] = {'last_attempt': now, 'last_success': self.clock() if not errors else None, 'errors': errors}
        self.housekeeping()

    def cloud_poll(self, e, nodes, now):
        for node in nodes:
            r = self.rpc(e, 'DescribeHistoryMonitorValues', {'NodeId': node['node_id'], 'StartTime': iso(now-300), 'EndTime': iso(now), 'IntervalForHistory': '01m', 'MonitorKeys': 'CpuUsage,TotalQps,Redis_Keys_Monitor,Redis_Zset_Monitor,Redis_Scripting_Monitor'})
            history = json.loads(r.get('MonitorHistory') or '{}')
            with self.db() as c:
                for t, values in history.items():
                    ts = datetime.fromisoformat(t.replace('Z', '+00:00')).timestamp()
                    cleaned = {k: float(values[k]) for k in ('CpuUsage', 'TotalQps', 'GetQps', 'PutQps', 'OtherOps', 'del', 'zrangebyscore', 'eval', 'evalsha') if k in values and math.isfinite(float(values[k]))}
                    if now-360 <= ts < now and cleaned:
                        c.execute('INSERT OR REPLACE INTO metrics VALUES(?,?,?,?)', (e['instanceId'], node['node'], ts, json.dumps(cleaned)))
            # Existing slow-log evidence only; no threshold changes or command replay.
            r = self.rpc(e, 'DescribeSlowLogRecords', {'NodeId': node['node_id'].split('#')[0], 'StartTime': iso(now-600)[:16]+'Z', 'EndTime': iso(now)[:16]+'Z', 'PageSize': 100, 'PageNumber': 1})
            logs = r.get('Items', {}).get('LogRecords', [])
            with self.db() as c:
                for row in logs:
                    raw = str(row.get('Command', ''))
                    cmd = raw.split(None, 1)[0].lower() if raw else 'unknown'
                    keys = re.findall(r'(?:pre:)?[a-z_][a-z0-9_:{}.-]{3,250}', raw)
                    keys = [safe_key(k) for k in keys if ':' in k][:8]
                    ts = datetime.fromisoformat(row['ExecuteTime'].replace('Z', '+00:00')).timestamp()
                    if not now-900 <= ts <= now:
                        continue
                    event_id = hashlib.sha256(json.dumps([node['node'], row.get('ExecuteTime'), row.get('ElapsedTime'), row.get('IPAddress'), raw]).encode()).hexdigest()
                    event = {'command': cmd, 'usec': row.get('ElapsedTime'), 'keys': keys, 'client': row.get('IPAddress'), 'thresholded': True,
                             'source_partial': int(r.get('TotalRecordCount', 0)) > len(logs), 'raw_command_retained': False}
                    c.execute('INSERT OR IGNORE INTO slow_events VALUES(?,?,?,?,?)', (e['instanceId'], node['node'], ts, event_id, json.dumps(event)))

    def check_storage(self, e):
        with self.db() as c:
            used = (c.execute('PRAGMA page_count').fetchone()[0]-c.execute('PRAGMA freelist_count').fetchone()[0])*c.execute('PRAGMA page_size').fetchone()[0]
        if used >= e['maxMiB']*1024**2 or shutil.disk_usage(self.root).free < 120*1024**3:
            raise RuntimeError('redis_history_storage_fuse')

    def housekeeping(self, force=False):
        now = self.clock()
        if not force and now-self.last_housekeeping < 3600:
            return
        self.last_housekeeping = now
        with self.db() as c:
            for e in self.entries:
                for table in ('snapshots', 'intervals', 'topology', 'buckets', 'metrics', 'slow_events', 'samples', 'collection'):
                    col = 'end' if table == 'intervals' else 'ts'
                    # Bounded deletes. A backlog is retried hourly, never an unbounded VACUUM.
                    c.execute(f'DELETE FROM {table} WHERE rowid IN (SELECT rowid FROM {table} WHERE instance=? AND {col}<? LIMIT 20000)', (e['instanceId'], now-e['retentionDays']*DAY))
                ident = e['instanceId']
                first = c.execute('SELECT ts FROM snapshots WHERE instance=? ORDER BY ts LIMIT 1', (ident,)).fetchone()
                if first:
                    self.bounds.setdefault(ident, [None, None])[0] = first[0]
            c.execute('PRAGMA incremental_vacuum(1024)')

    def status(self):
        active = []
        for e in self.entries:
            ident = e['instanceId']
            first, latest = self.bounds.get(ident, [None, None])
            sample_time = self.last_sample.get(ident)
            recent_sample = sample_time is not None and sample_time > self.clock()-3600
            active.append({'id': ident, 'label': e['label'], 'nodes': self.meta.get(ident, []), 'first': first, 'latest': latest,
                           'interval_seconds': e['intervalSeconds'], 'retention_days': e['retentionDays'], 'max_mib': e['maxMiB'], 'health': self.health.get(ident, {}),
                           'last_key_sample': sample_time, 'key_sampling': 'receiving' if recent_sample else 'not_connected'})
        return {'status': 'unavailable' if self.error else 'ok' if active else 'not_configured', 'instances': active,
                'storage': 'yy-ecs SQLite; 30天（实例可配置），容量上限熔断；不依赖OSS', 'warning': WARNING}

    def query(self, params):
        identity = one(params, 'instance')
        e = next((e for e in self.entries if e['instanceId'] == identity), None)
        if e is None:
            raise ValueError('configured_redis_instance_required')
        start, end = int(one(params, 'startEpochUs'))/1e6, int(one(params, 'endEpochUs'))/1e6
        if not math.isfinite(start+end) or start <= 0 or end <= start or end-start > DAY or end > self.clock()+60:
            raise ValueError('redis_window_max_one_day')
        node_filter = one(params, 'node')
        if node_filter and not re.fullmatch(r'db-\d{1,2}', node_filter):
            raise ValueError('invalid_redis_node')
        node_sql, args = (' AND node=?', [node_filter]) if node_filter else ('', [])
        def read_window(c, lo, hi):
            rows = c.execute('SELECT node,start,end,payload,reason FROM intervals WHERE instance=? AND start>=? AND end<=?'+node_sql+' ORDER BY end LIMIT 50001', [identity, lo, hi]+args).fetchall()
            if len(rows)>50000:
                raise ValueError('redis_query_row_budget')
            groups, durations, resets, curve = {}, {}, [], []
            for row in rows:
                if row['reason']:
                    resets.append({'node': row['node'], 'time': row['end'], 'reason': row['reason']})
                    continue
                d = unpack(row['payload'])
                sec = row['end']-row['start']
                durations[row['node']] = durations.get(row['node'], 0)+sec
                curve.append({'node': row['node'], 'timestamp': row['end']*1000, 'value': d.get('process_cpu_percent'), 'period': e['intervalSeconds']})
                for cmd, costs in d['commands'].items():
                    key = (row['node'], cmd)
                    g = groups.setdefault(key, {'calls': 0, 'usec': 0, 'seconds': 0})
                    g['calls'] += costs['calls']; g['usec'] += costs['usec']; g['seconds'] += sec
            return groups, durations, resets, curve
        with self.db() as c:
            current, durations, resets, curve = read_window(c, start, end)
            baseline, base_durations, base_resets, _ = read_window(c, start-DAY, end-DAY)
            output = []
            for (node, cmd), costs in current.items():
                before = baseline.get((node, cmd))
                sufficient = (costs['seconds'] >= (end-start)*0.95 and before is not None and before['seconds'] >= (end-start)*0.95)
                output.append({'node': node, 'command': cmd, **costs, 'qps': costs['calls']/costs['seconds'] if costs['seconds'] else None,
                               'service_seconds': costs['usec']/1e6, 'avg_us': costs['usec']/costs['calls'] if costs['calls'] else None,
                               'baseline': before, 'delta_calls': costs['calls']-before['calls'] if sufficient else None,
                               'delta_service_seconds': (costs['usec']-before['usec'])/1e6 if sufficient else None,
                               'nested': cmd in ('eval', 'evalsha', 'exec', 'eval_ro', 'evalsha_ro')})
            output.sort(key=lambda x: (x['delta_service_seconds'] is not None, x['delta_service_seconds'] or 0, x['usec']), reverse=True)
            metrics = [{'node': r['node'], 'timestamp': r['ts']*1000, **json.loads(r['payload'])} for r in c.execute('SELECT * FROM metrics WHERE instance=? AND ts>=? AND ts<?'+node_sql+' ORDER BY ts LIMIT 10001', [identity, start, end]+args)]
            rate_groups = {}
            base_metrics = c.execute('SELECT node,ts,payload FROM metrics WHERE instance=? AND ts>=? AND ts<?'+node_sql+' ORDER BY ts LIMIT 10001', [identity, start-DAY, end-DAY]+args).fetchall()
            for window, points in [('current', metrics), ('yesterday', [{'node':r['node'], **json.loads(r['payload'])} for r in base_metrics])]:
                for point in points:
                    for command in ('del', 'zrangebyscore', 'eval', 'evalsha'):
                        if command in point:
                            rate_groups.setdefault((point['node'], command), {}).setdefault(window, []).append(point[command])
            rate_rows = [{'node':n, 'command':cmd, 'qps':sum(v['current'])/len(v['current']) if v.get('current') else None,
                          'baseline_qps':sum(v['yesterday'])/len(v['yesterday']) if v.get('yesterday') else None,
                          'samples':len(v.get('current', [])), 'baseline_samples':len(v.get('yesterday', [])), 'source':'cloud_rates_no_duration'} for (n,cmd),v in sorted(rate_groups.items())]
            bucket = c.execute('SELECT ts,payload FROM buckets WHERE instance=? AND ts<=? ORDER BY ts DESC LIMIT 1', (identity, end)).fetchone()
            events = [{'node': r['node'], 'time': r['ts'], **json.loads(r['payload'])} for r in c.execute('SELECT * FROM slow_events WHERE instance=? AND ts>=? AND ts<?'+node_sql+' ORDER BY ts DESC LIMIT 101', [identity, start, end]+args)]
            samples = [{**json.loads(r['payload']), 'time': r['ts']} for r in c.execute('SELECT * FROM samples WHERE instance=? AND ts>=? AND ts<? ORDER BY ts DESC LIMIT 101', (identity, start, end))]
        expected = [n['node'] for n in self.meta.get(identity, []) if not node_filter or n['node'] == node_filter]
        nodes = sorted(set(expected) | set(durations) | set(base_durations))
        coverage = [{'node': n, 'seconds': durations.get(n, 0), 'baseline_seconds': base_durations.get(n, 0), 'expected_seconds': end-start} for n in nodes]
        return {'instance': identity, 'start': start, 'end': end, 'rows': output[:300], 'rows_truncated': len(output)>300,
                'coverage': coverage, 'resets': resets[:100], 'baseline_resets': base_resets[:100], 'process_cpu': curve[:10000],
                'metrics': metrics[:10000], 'metrics_truncated': len(metrics)>10000, 'rate_rows': rate_rows,
                'buckets': {'time': bucket['ts'], 'rows': json.loads(bucket['payload']), 'scope': 'latest_observed_not_future'} if bucket else None,
                'slow_events': events[:100], 'slow_events_truncated': len(events)>100,
                'key_samples': samples[:100], 'key_samples_truncated': len(samples)>100,
                'key_sampling': 'observed_client_samples' if samples else 'no_samples_in_window', 'key_sample_scope': 'instance_not_selected_node', 'warning': WARNING}

    def authorized(self, header):
        path = self.root / '.credentials' / 'redis-ingest.json'
        try:
            token = private_json(path).get('token', '')
            return len(token)>=32 and hmac.compare_digest(str(header), 'Bearer '+token)
        except Exception:
            return False

    def ingest(self, payload):
        if not isinstance(payload, dict) or set(payload)-{'instance', 'samples'}:
            raise ValueError('redis_sample_envelope_invalid')
        identity = payload.get('instance')
        if identity not in {e['instanceId'] for e in self.entries}:
            raise ValueError('configured_redis_instance_required')
        samples = payload.get('samples')
        if not isinstance(samples, list) or not 1<=len(samples)<=200:
            raise ValueError('redis_samples_batch_max_200')
        self.check_storage(next(e for e in self.entries if e['instanceId'] == identity))
        now = self.clock()
        validated = []
        allowed = {'id','time','command','keys','script','category','pod','sample_rate','client_us','failed'}
        for s in samples:
            if not isinstance(s, dict) or set(s)-allowed:
                raise ValueError('redis_sample_contains_unapproved_fields')
            sid = s.get('id', '')
            cmd = s.get('command', '')
            if not isinstance(sid, str) or not re.fullmatch(r'[a-zA-Z0-9_-]{16,80}', sid) or not isinstance(cmd, str) or not re.fullmatch(r'[a-z_]{2,24}', cmd):
                raise ValueError('redis_sample_id_or_command')
            ts, rate, us = s.get('time'), s.get('sample_rate'), s.get('client_us')
            if any(isinstance(x, bool) or not isinstance(x, (float,int)) or not math.isfinite(x) for x in (ts,rate,us)) or not now-3600<=ts<=now+30 or not 0<rate<=1 or not 0<=us<=120_000_000:
                raise ValueError('redis_sample_time_rate_or_latency')
            keys = s.get('keys', [])
            if not isinstance(keys, list) or len(keys)>16 or any(not isinstance(k, str) or len(k)>512 for k in keys):
                raise ValueError('redis_sample_keys')
            for k in ('script','category','pod'):
                if not isinstance(s.get(k, ''), str) or not re.fullmatch(r'[a-zA-Z0-9_.:-]{0,128}', s.get(k, '')):
                    raise ValueError('redis_sample_source')
            validated.append((identity, ts, sid, json.dumps({**s, 'keys':[safe_key(k) for k in keys], 'latency_scope':'client_round_trip', 'server_usec':None})))
        with self.lock:
            self.ingest_times = [t for t in self.ingest_times if now-t<60]
            if len(self.ingest_times)>=120:
                raise ValueError('redis_sample_ingest_rate_limit')
            with self.db() as c:
                c.executemany('INSERT OR IGNORE INTO samples VALUES(?,?,?,?)', validated)
            self.ingest_times.append(now)
            self.last_sample[identity] = max(s[1] for s in validated)
        return {'accepted':len(validated), 'deduplication':'instance_and_id', 'scope':'client_round_trip_only'}
