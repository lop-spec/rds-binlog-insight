"""Original-binlog OSS archive and a durable, small file-level directory."""
from __future__ import annotations

import gzip
import hashlib
import json
import logging
import os
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

from .binlog_lite import RawBinlogError, allows, scan_isolated as scan
from .oss_store import OssArchiveError

LOGGER = logging.getLogger(__name__)
MAX_FILES = 16
MAX_BYTES = 8 * 1024**3
QUERY_SECONDS = 180
RAW_ENABLED = 'RDS_BINLOG_RAW_ARCHIVE'


def enabled() -> bool:
    return os.environ.get(RAW_ENABLED, '0') == '1'


def compact(value) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


class RawBinlogStore:
    def __init__(self, metadata):
        self.metadata = metadata
        self.query_lock = threading.Lock()
        self._status_cache = {}
        with metadata._write_lock, metadata.connection() as conn:
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS raw_binlog_archives (
                    file_id TEXT PRIMARY KEY REFERENCES binlog_files(id) ON DELETE CASCADE,
                    lo INTEGER NOT NULL, hi INTEGER NOT NULL,
                    summary TEXT NOT NULL, descriptor TEXT NOT NULL,
                    started REAL NOT NULL, completed REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_raw_binlog_time
                    ON raw_binlog_archives(lo,hi);
            ''')

    def get(self, file_id: str) -> dict | None:
        with self.metadata.connection() as conn:
            row = conn.execute('SELECT descriptor FROM raw_binlog_archives WHERE file_id=?', (file_id,)).fetchone()
        return json.loads(row[0]) if row else None

    @staticmethod
    def _head(archive, descriptor):
        verified = archive._head_verified(descriptor['oss_key'], descriptor)
        if not verified:
            return None
        head = archive.bucket.head_object(descriptor['oss_key'])
        actual = str(head.headers.get('x-oss-hash-crc64ecma', ''))
        if not actual or actual != descriptor['crc64']:
            raise OssArchiveError('OSS 原始对象 CRC64 不一致', 'OSS_OBJECT_VERIFY_FAILED')
        return verified

    def _put(self, archive, path, key, sha, crc):
        descriptor = dict(oss_key=key, size_bytes=path.stat().st_size, sha256=sha, crc64=str(crc))
        verify = lambda: self._head(archive, descriptor)
        if verify() is None:
            archive._put_object_from_file_verified(
                key, path, {'Content-Type': 'application/octet-stream',
                            'x-oss-forbid-overwrite': 'true', 'x-oss-meta-sha256': sha},
                verify=verify, missing_message='OSS 原文件上传后无法校验',
                missing_code='RAW_ARCHIVE_VERIFY_MISSING')
        return descriptor

    def archive(self, archive, path: Path, file_id: str, item, flavor: str) -> dict:
        if archive is None:
            raise RawBinlogError('原始 Binlog 模式必须启用 OSS；拒绝静默切回全量行解析', 'RAW_ARCHIVE_OSS_REQUIRED')
        previous = self.get(file_id)
        if previous:
            self.verify(archive, previous)
            return previous
        started = time.time()
        record = self.metadata.file_record(file_id)
        sha = str(record.get('local_sha256') or '')
        crc = str(record.get('checksum_crc64') or '')
        if len(sha) != 64 or not crc.isdigit() or crc == '0':
            from .parser_bridge import checksum_file
            checksum = checksum_file(path)
            sha, crc = checksum.sha256, checksum.crc64
        directory = scan(path)
        scan_seconds = time.time() - started
        key_base = f'{archive.prefix}raw-binlog/v1/{file_id}/{sha}'
        raw = self._put(archive, path, key_base + '.binlog', sha, crc)
        # Sidecar is content-addressed independently; raw bytes are never changed.
        plain = compact(directory).encode('utf-8')
        if len(plain) > 256*1024**2:
            raise RawBinlogError('轻量索引超过查询解压预算，保留源文件且不标记完成', 'RAW_INDEX_SIZE_LIMIT')
        payload = gzip.compress(plain, compresslevel=1, mtime=0)
        if len(payload) > 32*1024**2:
            raise RawBinlogError('轻量索引超过查询读取预算，保留源文件且不标记完成', 'RAW_INDEX_SIZE_LIMIT')
        side_sha = hashlib.sha256(payload).hexdigest()
        from oss2.utils import Crc64
        side_crc = Crc64()
        side_crc(payload)
        side_path = path.with_suffix('.lite.json.gz')
        try:
            with side_path.open('wb') as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            side = self._put(archive, side_path, key_base + f'.{side_sha}.lite.json.gz', side_sha, str(side_crc.crc))
        finally:
            side_path.unlink(missing_ok=True)
        summary = {k: v for k, v in directory.items() if k != 'regions'}
        descriptor = dict(raw=raw, index=side, file_id=file_id, flavor=flavor,
                          instance_id=record['instance_id'], host_instance_id=item.host_instance_id,
                          source_file_name=item.log_file_name, events=directory['events'],
                          scan_seconds=scan_seconds, archive_seconds=time.time()-started)
        # FULL synchronous metadata commit happens only after BOTH remote objects
        # have verified size, SHA metadata, and server CRC64. Crash before commit
        # is an idempotent retry, not a completed file.
        with self.metadata._write_lock, self.metadata.connection() as conn:
            conn.execute('''INSERT INTO raw_binlog_archives
                (file_id,lo,hi,summary,descriptor,started,completed) VALUES(?,?,?,?,?,?,?)
                ON CONFLICT(file_id) DO NOTHING''',
                (file_id, directory['lo'], directory['hi'], compact(summary), compact(descriptor), started, time.time()))
        LOGGER.info('RAW_BINLOG_ARCHIVED %s', compact(dict(file=item.log_file_name, bytes=path.stat().st_size,
                    header_events=directory['events'], scan_seconds=round(scan_seconds, 3),
                    seconds=round(time.time()-started, 3), sidecar_bytes=len(payload))))
        return descriptor

    def verify(self, archive, descriptor):
        if archive is None:
            raise RawBinlogError('原文件归档需要 OSS 校验', 'RAW_ARCHIVE_OSS_REQUIRED')
        for key in ('raw', 'index'):
            if self._head(archive, descriptor[key]) is None:
                raise RawBinlogError('原文件或轻量索引缺失，拒绝释放本地文件', 'RAW_ARCHIVE_INCOMPLETE')

    def plan(self, query: dict, start: int, end: int) -> dict:
        """Bound the UNION of old Parquet source files and raw objects, before I/O.

        Unknown legacy catalogs are included, never treated as negative matches.
        SQL LIMIT caps candidate enumeration, not returned query completeness.
        """
        if str(query.get('source') or '').lower() in {'audit', 'slowlog'}:
            return dict(raw=[], file_ids=[], candidate_files=0, estimated_bytes=0)
        instance = str(query.get('instance') or '')
        database = str(query.get('database') or '').strip().lower()
        table = str(query.get('table') or '').strip().lower()
        candidates, raw = {}, []
        with self.metadata.connection() as conn:
            # One row per physical source, not per Parquet shard. Partial catalogs
            # remain eligible. The existing source/file indexes bound this query.
            state = conn.execute('SELECT complete FROM parquet_file_stats_state WHERE singleton=1').fetchone()
            directory_table = 'parquet_file_stats' if state and state[0] else 'parquet_parts'
            if directory_table == 'parquet_parts':
                LOGGER.warning('RAW_QUERY_PREFLIGHT_LEGACY reason=file-summary-not-complete')
            old = conn.execute(f'''SELECT DISTINCT b.id,b.file_size FROM {directory_table} p
                JOIN binlog_files b ON b.id=p.binlog_id
                WHERE p.min_event_epoch_us<=? AND p.max_event_epoch_us>=?
                  AND b.log_file_name LIKE 'mysql-bin.%' AND (?='' OR b.instance_id=?)
                  AND NOT EXISTS (SELECT 1 FROM raw_binlog_archives r WHERE r.file_id=b.id)
                LIMIT ?''', (end, start, instance, instance, MAX_FILES+1)).fetchall()
            candidates.update({r['id']: int(r['file_size']) for r in old})
            self._check(candidates)
            rows = conn.execute('''SELECT r.summary,r.descriptor FROM raw_binlog_archives r
                JOIN binlog_files b ON b.id=r.file_id
                WHERE r.lo<=? AND r.hi>=? AND (?='' OR b.instance_id=?)''', (end, start, instance, instance))
            for row in rows:
                if not allows(json.loads(row['summary']), query, start, end):
                    continue
                entry = json.loads(row['descriptor'])
                raw.append(entry)
                candidates[entry['file_id']] = entry['raw']['size_bytes']
                self._check(candidates)
        return dict(raw=raw, file_ids=list(candidates), candidate_files=len(candidates), estimated_bytes=sum(candidates.values()))

    @staticmethod
    def _check(candidates):
        count, size = len(candidates), sum(candidates.values())
        if count > MAX_FILES or size > MAX_BYTES:
            LOGGER.warning('QUERY_BINLOG_LIMIT candidates>=%d bytes>=%d limits=%d/%d', count, size, MAX_FILES, MAX_BYTES)
            raise RawBinlogError(f'候选 Binlog 至少 {count} 个、{size / 1024**3:.2f} GiB；单次上限 {MAX_FILES} 个 / 8 GiB。请缩小时间、实例或库表范围；不会自动全量扫描。', 'QUERY_BINLOG_LIMIT')

    def status(self, instance: str) -> dict:
        now = time.time()
        cached = self._status_cache.get(instance)
        if cached and now-cached[0] < 15:
            return cached[1]
        with self.metadata.connection() as conn:
            archived = conn.execute('''SELECT count(*) n,min(r.started) first,max(r.completed) last,
                min(b.log_begin_utc) begin,max(b.log_end_utc) finish,sum(b.file_size) bytes
                FROM raw_binlog_archives r JOIN binlog_files b ON b.id=r.file_id
                WHERE b.instance_id=?''', (instance,)).fetchone()
            since = max(now-1800, float(archived['first'] or now))
            recent = conn.execute('''SELECT count(*) n,coalesce(sum(b.file_size),0) bytes
                FROM raw_binlog_archives r JOIN binlog_files b ON b.id=r.file_id
                WHERE b.instance_id=? AND b.state='done' AND r.completed>=?''', (instance, since)).fetchone()
            remaining = conn.execute('''SELECT state,count(*) n,coalesce(sum(file_size),0) bytes
                FROM binlog_files WHERE instance_id=? AND log_file_name LIKE 'mysql-bin.%'
                  AND state<>'done' GROUP BY state''', (instance,)).fetchall()
            # Use the largest recent complete UTC day, not average tiny files or
            # summed parallel processing_seconds. Source growth is independent.
            days = conn.execute('''SELECT substr(log_end_utc,1,10) day,count(*) n,sum(file_size) bytes
                FROM binlog_files WHERE instance_id=? AND log_file_name LIKE 'mysql-bin.%'
                  AND log_end_utc>=? AND log_end_utc<? GROUP BY day''',
                (instance, datetime.fromtimestamp(now-4*86400, UTC).strftime('%Y-%m-%dT00:00:00Z'),
                 datetime.fromtimestamp(now, UTC).strftime('%Y-%m-%dT00:00:00Z'))).fetchall()
            recent_source = conn.execute('''SELECT coalesce(sum(file_size),0) FROM binlog_files
                WHERE instance_id=? AND log_file_name LIKE 'mysql-bin.%' AND log_end_utc>=? AND log_end_utc<=?''',
                (instance, datetime.fromtimestamp(now-3600,UTC).strftime('%Y-%m-%dT%H:%M:%SZ'),
                 datetime.fromtimestamp(now,UTC).strftime('%Y-%m-%dT%H:%M:%SZ'))).fetchone()[0]
        elapsed = max(now-since, 1)
        pending = sum(r['n'] for r in remaining if r['state'] != 'unavailable')
        pending_bytes = sum(r['bytes'] for r in remaining if r['state'] != 'unavailable')
        missing = sum(r['n'] for r in remaining if r['state'] == 'unavailable')
        missing_bytes = sum(r['bytes'] for r in remaining if r['state'] == 'unavailable')
        # Conservative headroom: retain only 80% of measured end-to-end throughput.
        rate = recent['bytes'] / elapsed
        incoming = max(max((r['bytes']/86400 for r in days), default=0), recent_source/3600)
        net = rate * .8 - incoming
        eta = pending_bytes/net if net > 0 and elapsed >= 300 and recent['n'] >= 20 and incoming > 0 else None
        result = dict(enabled=enabled(), archived_files=archived['n'], lightweight_indexed_files=archived['n'],
                      archive_bytes=int(archived['bytes'] or 0), first_file_begin_utc=archived['begin'],
                      latest_file_end_utc=archived['finish'],
                      row_images_indexed=False, pending_files=pending, pending_bytes=pending_bytes,
                      unavailable_files=missing, unavailable_bytes=missing_bytes,
                      sample_seconds=round(elapsed, 1), sample_files=recent['n'],
                      files_per_hour=round(recent['n']*3600/elapsed, 2), bytes_per_second=round(rate, 2),
                      source_bytes_per_second=round(incoming, 2), safety_factor=.8,
                      estimated_catchup_seconds=round(eta, 1) if eta is not None else None,
                      estimated_including_known_unavailable_seconds=round((pending_bytes+missing_bytes)/net, 1) if eta is not None else None,
                      within_24h=eta is not None and eta <= 86400,
                      source_gaps_verified=False, max_query_files=MAX_FILES, max_query_bytes=MAX_BYTES)
        self._status_cache[instance] = (now, result)
        return result
