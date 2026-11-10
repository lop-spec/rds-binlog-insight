"""Bounded read-only Tair Proxy telemetry. Never MONITOR, EVAL, CONFIG or values."""
from __future__ import annotations
import hashlib
import json
import math
import os
import re
import socket
import ssl
from pathlib import Path


class RedisTelemetryError(RuntimeError):
    pass


def private_json(path):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 65536:
        raise RedisTelemetryError('invalid_private_file')
    if os.name != 'nt' and path.stat().st_mode & 0o077:
        raise RedisTelemetryError('credentials_require_0600')
    return json.loads(path.read_text(encoding='utf-8'))


def load_instances(root):
    path = Path(root) / 'redis-instances.json'
    if not path.exists():
        return []
    value = private_json(path)
    entries = value.get('instances') if isinstance(value, dict) else value
    if not isinstance(entries, list) or len(entries) > 10:
        raise ValueError('invalid_redis_instances')
    result, seen = [], set()
    for e in entries:
        identity, region = e.get('instanceId', ''), e.get('region', '')
        if not re.fullmatch(r'r-[a-z0-9]{4,64}', identity) or identity in seen:
            raise ValueError('invalid_redis_instance')
        if not re.fullmatch(r'[a-z]+-[a-z]+(?:-\d+)?', region):
            raise ValueError('invalid_redis_region')
        if e.get('host') != f'{identity}.redis.{region}.rds.aliyuncs.com' or e.get('port', 6379) != 6379:
            raise ValueError('redis_fixed_proxy_required')
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}', str(e.get('username', ''))) or not isinstance(e.get('enabled', False), bool):
            raise ValueError('redis_readonly_identity_required')
        for k, lo, hi, default in [('intervalSeconds', 10, 60, 20), ('retentionDays', 2, 90, 30), ('maxMiB', 64, 4096, 2048)]:
            v = e.get(k, default)
            if isinstance(v, bool) or not isinstance(v, int) or not lo <= v <= hi:
                raise ValueError('invalid_redis_' + k)
            e[k] = v
        label = e.get('label', identity)
        if not isinstance(label, str) or not label or len(label) > 128:
            raise ValueError('invalid_redis_label')
        if not isinstance(e.get('credentialsFile'), str):
            raise ValueError('redis_credentials_file_required')
        seen.add(identity)
        result.append(dict(e, label=label))
    return result


class RedisWire:
    def __init__(self, entry, root):
        p = Path(entry['credentialsFile'])
        auth = private_json(p if p.is_absolute() else Path(root) / p)
        if auth.get('username') != entry['username'] or not isinstance(auth.get('password'), str) or not auth['password']:
            raise RedisTelemetryError('redis_credential_identity_mismatch')
        self.socket = socket.create_connection((entry['host'], 6379), timeout=3)
        try:
            self.socket.settimeout(3)
            if entry.get('tls', False):
                self.socket = ssl.create_default_context().wrap_socket(self.socket, server_hostname=entry['host'])
            self.stream = self.socket.makefile('rb')
            self._send(['AUTH', auth['username'], auth['password']])
        except Exception:
            self.socket.close()
            raise RedisTelemetryError('redis_auth_or_connection_failed') from None

    def _read(self, budget, depth=0):
        if depth > 5:
            raise RedisTelemetryError('redis_reply_depth')
        line = self.stream.readline(8193)
        budget[0] -= len(line)
        if len(line) > 8192 or not line.endswith(b'\r\n') or budget[0] < 0:
            raise RedisTelemetryError('redis_reply_limit')
        kind, value = line[:1], line[1:-2]
        if kind == b'-':
            # Never echo server error text containing caller arguments.
            raise RedisTelemetryError('redis_permission_or_command_rejected')
        if kind == b'+':
            return value.decode('utf-8')
        if kind == b':':
            return int(value)
        if kind == b'$':
            n = int(value)
            if n == -1:
                return None
            if n < 0 or n > budget[0]:
                raise RedisTelemetryError('redis_reply_limit')
            raw = self.stream.read(n)
            if len(raw) != n or self.stream.read(2) != b'\r\n':
                raise RedisTelemetryError('redis_truncated_reply')
            budget[0] -= n + 2
            return raw.decode('utf-8', 'replace')
        if kind == b'*':
            n = int(value)
            if not 0 <= n <= 512:
                raise RedisTelemetryError('redis_array_limit')
            return [self._read(budget, depth+1) for _ in range(n)]
        raise RedisTelemetryError('redis_invalid_reply')

    def _send(self, args):
        encoded = [str(x).encode('utf-8') for x in args]
        self.socket.sendall(b'*'+str(len(encoded)).encode()+b'\r\n'+b''.join(b'$'+str(len(v)).encode()+b'\r\n'+v+b'\r\n' for v in encoded))
        return self._read([1024 * 1024])

    def send(self, *args):
        allowed = (len(args) == 5 and args[0] == 'IINFO' and str(args[1]).isdigit()
                   and args[2:] == ('server', 'commandstats', 'cpu'))
        allowed |= (len(args) == 3 and args[:2] == ('INFO', 'KEY')
                    and bool(re.fullmatch(r'pre:follow_order:outbox:zset:\{fo:(?:[0-9]|[1-5][0-9]|6[0-3])\}', str(args[2]))))
        if not allowed:
            raise RedisTelemetryError('redis_telemetry_command_not_allowed')
        return self._send(args)

    def close(self):
        self.stream.close()
        self.socket.close()


def parse_info(raw):
    sections, section = {}, 'default'
    for line in str(raw).splitlines():
        if line.startswith('# '):
            section = line[2:].strip().lower()
        elif ':' in line:
            k, v = line.split(':', 1)
            sections.setdefault(section, {})[k] = v
    server = sections.get('server', {})
    uptime = int(server.get('uptime_in_seconds', '-1'))
    if uptime < 0:
        raise RedisTelemetryError('redis_node_uptime_missing')
    commands = {}
    for key, value in sections.get('commandstats', {}).items():
        if not re.fullmatch(r'cmdstat_[a-z0-9_|-]{1,96}', key):
            continue
        pairs = dict(p.split('=', 1) for p in value.split(',') if '=' in p)
        if not all(pairs.get(k, '').isdigit() for k in ('calls', 'usec')):
            continue
        commands[key[8:]] = {'calls': int(pairs['calls']), 'usec': int(pairs['usec'])}
    if not commands:
        raise RedisTelemetryError('redis_commandstats_unavailable')
    cpu = {}
    for key, value in sections.get('cpu', {}).items():
        if key in ('used_cpu_sys', 'used_cpu_user'):
            n = float(value)
            if math.isfinite(n) and n >= 0:
                cpu[key] = n
    return {'uptime': uptime, 'run_id': server.get('run_id', ''), 'cpu': cpu, 'commands': commands,
            'identity_method': 'run_id' if server.get('run_id') else 'physical_node_and_uptime'}


def delta(previous, current, interval):
    """No proportional extrapolation across gaps, new counters or node epochs."""
    if previous is None:
        return None, 'baseline'
    elapsed = current['ts'] - previous['ts']
    if elapsed <= 0 or elapsed > interval * 2.5:
        return None, 'sample_gap'
    if current['node_id'] != previous['node_id'] or current['run_id'] != previous['run_id']:
        return None, 'node_changed'
    if abs((current['uptime'] - previous['uptime']) - elapsed) > 3:
        return None, 'uptime_discontinuity'
    for cmd, now in current['commands'].items():
        old = previous['commands'].get(cmd)
        if old and any(now[k] < old[k] for k in ('calls', 'usec')):
            return None, 'counter_reset'
    rows = {}
    for cmd, now in current['commands'].items():
        old = previous['commands'].get(cmd)
        if old is not None:
            rows[cmd] = {k: now[k]-old[k] for k in ('calls', 'usec')}
    process_cpu = None
    if all(k in current['cpu'] and k in previous['cpu'] for k in ('used_cpu_sys', 'used_cpu_user')):
        diff = sum(current['cpu'][k]-previous['cpu'][k] for k in ('used_cpu_sys', 'used_cpu_user'))
        if diff >= 0:
            process_cpu = 100 * diff / elapsed
    return {'start': previous['ts'], 'end': current['ts'], 'commands': rows,
            'process_cpu_percent': process_cpu}, ''


def safe_key(key):
    """Preserve namespace and numeric bucket only; never customer identifiers."""
    key = str(key)
    outbox = re.fullmatch(r'(pre:)?follow_order:outbox:(zset|hash|ver):\{fo:(\d{1,2})\}', key)
    if outbox:
        return key
    parts = key.split(':')[:3]
    shape = ':'.join(p if re.fullmatch(r'[a-z_]{1,40}', p) else '*' for p in parts)
    return shape + ':#' + hashlib.sha256(key.encode()).hexdigest()[:12]
