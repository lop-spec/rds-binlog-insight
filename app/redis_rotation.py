"""Time-partitioned Redis telemetry, rotated against the host filesystem."""
import os
import re
import shutil
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path

DAY = 86400
TABLES = ('snapshots', 'intervals', 'topology', 'buckets', 'metrics', 'slow_events', 'samples', 'collection')


def usage(path):
    if not hasattr(os, 'statvfs'):
        stat = shutil.disk_usage(path)
        return stat.used, stat.free
    stat = os.statvfs(path)
    return ((stat.f_blocks-stat.f_bfree)*stat.f_frsize, stat.f_bavail*stat.f_frsize)


def connection(path, read_only=False):
    if path.is_symlink():
        raise RuntimeError('redis_history_symlink_rejected')
    if not read_only:
        os.close(os.open(path, os.O_WRONLY | os.O_CREAT, 0o600))
    c = sqlite3.connect(path.resolve().as_uri()+('?mode=ro' if read_only else '?mode=rw'), uri=True, timeout=5)
    c.row_factory = sqlite3.Row
    return c


class RedisRotation:
    def __init__(self, root, clock, schema, disk_usage=usage):
        self.root, self.clock, self.schema, self.disk_usage = Path(root), clock, schema, disk_usage
        self.legacy = self.root/'redis-history.sqlite3'
        self.control = self.root/'redis-history-control.sqlite3'
        self.parts = self.root/'redis-history-rotation'
        self.lock = threading.RLock()
        self.readers = {}
        self.ready = set()
        self.last_evicted = None
        self.state = {}

    def initialize(self):
        self.root.mkdir(parents=True, exist_ok=True)
        self.parts.mkdir(mode=0o700, exist_ok=True)
        if any(p.is_symlink() for p in (self.parts, self.legacy, self.control)):
            raise RuntimeError('redis_history_symlink_rejected')
        if self.legacy.exists() and not self.control.exists():
            tmp = self.root/'redis-history-control.pending.sqlite3'
            c = connection(tmp)
            try:
                c.execute('ATTACH DATABASE ? AS old', (self.legacy.resolve().as_uri()+'?mode=ro',))
                with c:
                    c.execute('BEGIN')
                    entries = list(c.execute("SELECT type,name,tbl_name,sql FROM old.sqlite_master WHERE sql IS NOT NULL AND type IN ('table','index') AND name NOT LIKE 'sqlite_%' ORDER BY type DESC"))
                    for row in entries:
                        if row['tbl_name'] not in TABLES:
                            c.execute(row['sql'])
                            if row['type'] == 'table':
                                name = '"'+row['name'].replace('"','""')+'"'
                                c.execute(f'INSERT INTO main.{name} SELECT * FROM old.{name}')
            finally:
                c.close()
            os.replace(tmp, self.control)

    def day_prefix(self, ts):
        return f'day-{int(ts//DAY):08d}'

    def files(self):
        return [p for p in sorted(self.parts.glob('day-*.sqlite3')) if re.fullmatch(r'day-[0-9]{8}\.sqlite3', p.name) and p.is_file() and not p.is_symlink()]

    def file_for(self, ts):
        prefix = self.day_prefix(ts)
        return self.parts/(prefix+'.sqlite3')

    @contextmanager
    def write(self, ts):
        with self.lock:
            path = self.file_for(ts)
            c = connection(path)
            try:
                if path not in self.ready:
                    c.executescript(self.schema)
                    self.ready.add(path)
                with c:
                    yield c
            finally:
                c.close()

    @contextmanager
    def read(self, start, end):
        with self.lock:
            paths = [p for p in self.files() if self.day_prefix(start) <= p.name[:12] <= self.day_prefix(end)]
            if self.legacy.exists():
                paths.insert(0, self.legacy)
            for p in paths:
                self.readers[p] = self.readers.get(p, 0)+1
        c = None
        try:
            c = connection(self.control, True)
            if paths:
                for i, p in enumerate(paths):
                    c.execute(f'ATTACH DATABASE ? AS r{i}', (p.resolve().as_uri()+'?mode=ro',))
                for table in TABLES:
                    selects = [f'SELECT * FROM r{i}.{table}' for i in range(len(paths))]
                    # Cloud polling can rediscover a record from just before cutover.
                    # Preserve the legacy record, not two copies of the same event.
                    keys = {'metrics': ('instance','node','ts'),
                            'slow_events': ('instance','node','id'), 'samples': ('instance','id')}.get(table)
                    if keys and paths[0] == self.legacy:
                        match = ' AND '.join(f'old.{k}=new.{k}' for k in keys)
                        for i in range(1, len(paths)):
                            selects[i] = f'SELECT new.* FROM r{i}.{table} new WHERE NOT EXISTS (SELECT 1 FROM r0.{table} old WHERE {match})'
                    c.execute(f'CREATE TEMP VIEW {table} AS '+' UNION ALL '.join(selects))
            yield c
        finally:
            if c is not None:
                c.close()
            with self.lock:
                for p in paths:
                    self.readers[p] -= 1
                    if not self.readers[p]:
                        del self.readers[p]

    def reclaim(self):
        with self.lock:
            used, free = self.disk_usage(self.root)
            candidates = ([self.legacy] if self.legacy.exists() else [])+self.files()
            evicted = False
            while candidates and used+free and used*5 >= (used+free)*4:
                p = candidates.pop(0)
                if self.readers.get(p):
                    break  # never overtake an older file that a query is reading
                if p == self.legacy and Path(str(p)+'-wal').exists():
                    c = connection(p)
                    try:
                        if c.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0]:
                            break
                    finally:
                        c.close()
                p.unlink()
                evicted = True
                self.ready.discard(p)
                self.last_evicted = p.name
                used, free = self.disk_usage(self.root)
            self.state = {'mode':'host_disk_rotation','threshold_percent':80,
                          'used_percent':round(100*used/(used+free),2) if used+free else None,
                          'files':len(self.files())+int(self.legacy.exists()),'last_evicted':self.last_evicted,
                          'pressure_remaining':bool(used+free and used*5 >= (used+free)*4)}
            return evicted

    def extreme(self, sql, params, *, newest=False, before=None, all_files=False):
        result = []
        with self.lock:
            paths = ([self.legacy] if self.legacy.exists() else [])+self.files()
            for p in reversed(paths) if newest else paths:
                if before is not None and p != self.legacy and p.name[:12] > self.day_prefix(before):
                    continue
                c = connection(p, True)
                try:
                    rows = c.execute(sql, params).fetchall()
                    if rows and not all_files:
                        return rows
                    result.extend(rows)
                finally:
                    c.close()
        return result
