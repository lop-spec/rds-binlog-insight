"""One low-priority work unit, hosted by the existing index supervisor."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import shutil
import sqlite3
import tempfile
import threading
import time
from contextlib import closing
from pathlib import Path

from .binlog_lite import RawBinlogError
from .parser_bridge import parse_ndjson_chunks
from .raw_binlog_query import normalize_row, read_object

LOGGER = logging.getLogger(__name__)


class IndexDeferred(RuntimeError):
    pass


class ResourceBudget:
    """Bounded single lane with feedback duty cycle, not additional concurrency.

    Admission fails closed if host pressure cannot be measured. The parser's
    bounded stdout queue provides backpressure while Python yields between chunks.
    Hard process-tree CPU/memory ceilings remain owned by the existing container.
    """
    def __init__(self, metadata, root, publish, *, sample=None, clock=time.monotonic, sleep=time.sleep):
        self.metadata, self.root, self.publish = metadata, Path(root), publish
        self.clock, self.sleep = clock, sleep
        self.sample = sample or self.host_sample
        self.last_sample, self.last_checkpoint = 0.0, clock()
        self.previous_cpu = None
        self.duty = 0.1
        self.reason = ''
        self.requests = self.bytes = 0
        self.cancel = threading.Event()
        self.paused = False
        self.sequence = 0
        self.reclaiming = False

    @staticmethod
    def pressure(name, kind):
        for line in Path('/proc/pressure/'+name).read_text().splitlines():
            if line.startswith(kind+' '):
                return float(dict(part.split('=') for part in line.split()[1:])['avg10'])
        raise ValueError('missing PSI '+name+'/'+kind)

    def host_sample(self):
        fields = list(map(int, Path('/proc/stat').read_text().splitlines()[0].split()[1:9]))
        total, idle = sum(fields), fields[3]
        previous = self.previous_cpu
        self.previous_cpu = (total, idle)
        spare = 0.1 if previous is None or total <= previous[0] else (idle-previous[1])/(total-previous[0])
        with self.metadata.connection() as conn:
            active = conn.execute("SELECT 1 FROM query_tasks WHERE status IN ('queued','running','cancelling') LIMIT 1").fetchone() is not None
        return {'idle': spare, 'io': self.pressure('io', 'full'), 'memory': self.pressure('memory', 'full'),
                'query': active, 'free': shutil.disk_usage(self.root).free}

    @staticmethod
    def allocation(sample, paused=False):
        if sample['free'] < 20*1024**3:
            return 0.0, 'disk-reserve-below-20GiB'
        if sample['query']:
            return 0.0, 'interactive-query-priority'
        if sample['io'] > (0.5 if paused else 1.0):
            return 0.0, 'io-pressure'
        if sample['memory'] > (0.1 if paused else 0.2):
            return 0.0, 'memory-pressure'
        if sample['idle'] < (0.20 if paused else 0.10):
            return 0.0, 'cpu-pressure'
        return min(1.0, max(0.1, (sample['idle']-0.10)/0.50)), 'spare-capacity'

    def check(self):
        if self.cancel.is_set():
            raise IndexDeferred('worker stopping')
        now = self.clock()
        if now-self.last_sample < 0.5:
            return
        worked = max(0, now-self.last_checkpoint)
        wait_since = now
        while True:
            try:
                sample = self.sample()
                if self.reclaiming:
                    sample = {**sample, 'free': 20*1024**3}
                duty, reason = self.allocation(sample, self.paused)
            except (OSError, ValueError, sqlite3.Error) as exc:
                duty, reason = 0.0, 'resource-probe-unavailable: '+str(exc)
            if duty != self.duty or reason != self.reason:
                LOGGER.info('RAW_INDEX_ALLOCATION duty=%.2f reason=%s', duty, reason)
            self.duty, self.reason, self.paused = duty, reason, duty == 0
            self.sequence += 1
            self.publish('paused' if not duty else 'running', phase='raw-events',
                         token=f'raw:{self.sequence}:{self.bytes}', result={'duty': duty, 'reason': reason, 'bytes': self.bytes})
            if duty:
                self.sleep(min(5.0, worked*(1/duty-1)))
                break
            if self.clock()-wait_since >= 300:
                raise IndexDeferred(reason)
            self.sleep(2)
            if self.cancel.is_set():
                raise IndexDeferred('worker stopping')
        self.last_sample = self.last_checkpoint = self.clock()

    def add(self, size):
        self.bytes += size
        self.check()


def decoded_rows(storage, archive, entry, gate):
    """One sequential OSS GET, streaming SHA256/CRC64, bounded parser chunks.

    No collection-owned local file is retained or touched. Incomplete parsing
    never publishes the file. Existing native row semantics remain authoritative.
    """
    from oss2.utils import Crc64
    storage.raw_binlogs.verify(archive, entry)
    directory = read_object(archive, entry['index'], gate)
    positions = {r['gtid']: r['start'] for r in directory['regions'] if r.get('gtid')}
    with tempfile.TemporaryDirectory(prefix='raw-index-', dir=storage.paths['scratch']) as scratch:
        root = Path(scratch)
        raw = entry['raw']
        if raw['size_bytes'] > 8*1024**3:
            raise RawBinlogError('异步索引单文件超过8GiB预算', 'QUERY_BINLOG_BYTE_LIMIT')
        if shutil.disk_usage(root).free < raw['size_bytes']+20*1024**3:
            raise IndexDeferred('raw-file-staging-reserve')
        source = root/'source.binlog'
        digest, crc, size = hashlib.sha256(), Crc64(), 0
        response = archive.bucket.get_object(raw['oss_key'])
        try:
            with source.open('wb') as output:
                while True:
                    gate.check()
                    chunk = response.read(1024**2)
                    if not chunk:
                        break
                    size += len(chunk)
                    if size > raw['size_bytes']:
                        raise RawBinlogError('原档读取超过声明长度', 'RAW_INDEX_VERIFY_FAILED')
                    digest.update(chunk)
                    crc(chunk)
                    output.write(chunk)
                    gate.add(len(chunk))
        finally:
            response.close()
        if size != raw['size_bytes'] or digest.hexdigest() != raw['sha256'] or str(crc.crc) != raw['crc64']:
            raise RawBinlogError('原档长度/SHA256/CRC64校验失败', 'RAW_INDEX_VERIFY_FAILED')
        ordinals = {}
        with closing(parse_ndjson_chunks(source, entry['file_id'], root/'chunks', entry.get('flavor') or 'mysql',
            max_lines=256, max_bytes=2*1024**2, cancel_event=gate.cancel)) as chunks:
            for path in chunks:
                try:
                    gate.check()
                    with path.open(encoding='utf-8') as handle:
                        for line in handle:
                            yield normalize_row(json.loads(line), entry, ordinals, positions)
                finally:
                    path.unlink(missing_ok=True)


def run_one(storage, archive, publish):
    """No new queue or scheduler. Return after one file to preserve fairness."""
    index = storage.raw_event_index
    budget = ResourceBudget(storage.metadata, storage.paths['root'], publish)
    entry = None
    try:
        budget.reclaiming = True
        index.reclaim(budget.check)
        budget.reclaiming = False
        budget.last_sample = 0
        entry = next(index.pending(), None)
        if entry is None:
            return None
        budget.check()
        with closing(decoded_rows(storage, archive, entry, budget)) as rows:
            result = index.build(entry, rows, storage.exact_index, budget.check)
        LOGGER.info('RAW_EVENT_INDEX_COMPLETE file=%s rows=%s', entry['file_id'], result['rows'])
        publish('completed', phase='raw-events', token='raw-complete:'+entry['file_id'], result=result)
        return result
    except IndexDeferred as exc:
        file_id = entry['file_id'] if entry else 'reclaim'
        LOGGER.warning('RAW_EVENT_INDEX_DEFERRED file=%s reason=%s', file_id, exc)
        publish('paused', phase='raw-events', token='raw-deferred:'+file_id, error=str(exc))
        return {'deferred': True}
    finally:
        budget.cancel.set()
