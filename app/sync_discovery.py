"""Stream retained reconciliation while serving a fair live/catch-up queue.

No moving checkpoint can skip history: success requires finishing the retained
scan AND draining the cached pending files AND checking the live frontier again.
Only immutable archived events are published; each admitted batch retains its
source order. Fairness changes admission between batches, not commit safety.
"""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

from .rds_api import RemoteBinlog

LOGGER = logging.getLogger(__name__)
REFRESH_SECONDS = 60
LIVE_LOOKBACK = timedelta(hours=1)


def source_order(entry):
    item = entry[1]
    return (item.log_begin_utc, item.log_end_utc, item.log_file_name, item.host_instance_id)


def fair_pending(entries, now, *, confirmed_ids=None):
    """Alternate cold/live; prefer revalidated or local files within each cohort.

    Unconfirmed cached records remain pending, never archived or dropped. The
    retained scan continues independently and promotes them as pages arrive.
    """
    boundary = (now - LIVE_LOOKBACK).strftime('%Y-%m-%dT%H:%M:%SZ')
    cold, live = [], []
    unique = {entry[0]: entry for entry in entries}
    def order(entry):
        deferred = confirmed_ids is not None and entry[0] not in confirmed_ids
        return (deferred, source_order(entry))
    for entry in sorted(unique.values(), key=order):
        (live if entry[1].log_end_utc >= boundary else cold).append(entry)
    result = []
    for index in range(max(len(cold), len(live))):
        if index < len(cold): result.append(cold[index])
        if index < len(live): result.append(live[index])
    return result


class RetainedDiscovery:
    def __init__(self, manager, job_id, client, settings, primary):
        self.manager, self.job_id, self.client = manager, job_id, client
        self.settings, self.primary = settings, primary
        self.scan_start = manager._scan_start(settings)
        self.scan_end = datetime.now(UTC)
        self.stop = threading.Event()
        self.last_recent = float('-inf')
        self.primary_windows = set()
        self.confirmed_ids = set()
        self.observed_lock = threading.Lock()
        self.pool = None
        self.future = None

    def __enter__(self):
        self.pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix='binlog-discovery')
        self.future = self.pool.submit(self._reconcile)
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.stop.set()
        self.pool.shutdown(wait=True, cancel_futures=True)
        if self.future.done() and not self.future.cancelled():
            failure = self.future.exception()
            if failure is not None:
                # Never silently lose a producer failure on pause or another error.
                LOGGER.error('Retained discovery ended with %s; parent_error=%s',
                             type(failure).__name__, exc_type)
                if exc_type is None and not self.stopping():
                    raise failure
        return False

    def stopping(self):
        return self.manager._pause_after_current.is_set() or self.manager._shutdown.is_set()

    @property
    def finished(self):
        return self.future.done() and self.future.result() is True

    def _ingest(self, batch, *, retained=False):
        items = [item for item in batch if not self.primary or item.host_instance_id == self.primary]
        records = self.manager.metadata.upsert_remotes(self.settings, items)
        with self.observed_lock:
            self.confirmed_ids.update(file_id for (file_id, _state), item in zip(records, items)
                                      if item.remote_status.lower() == 'completed')
            if retained:
                self.primary_windows.update((item.log_begin_utc, item.log_end_utc)
                                            for item in items if item.remote_status.lower() == 'completed')

    def _reconcile(self):
        fmt = lambda dt: dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        self.manager._event(self.job_id, 'info', 'RETAINED_DISCOVERY_STARTED',
                            '保留窗口逐页核对；不阻塞已知文件恢复和新日志发现')
        batches = iter(self.client.iter_binlog_batches(fmt(self.scan_start), fmt(self.scan_end)))
        count, reported = 0, time.monotonic()
        while not self.stop.is_set() and not self.stopping():
            try:
                batch = next(batches)
            except StopIteration:
                self.manager._event(self.job_id, 'info', 'RETAINED_DISCOVERY_COMPLETE',
                                    f'保留窗口核对完成：{count} 条 API 记录')
                return True
            self._ingest(batch, retained=True)
            count += len(batch)
            if time.monotonic() - reported >= 30:
                self.manager._event(self.job_id, 'info', 'RETAINED_DISCOVERY_PROGRESS',
                                    f'保留窗口仍在核对：{count} 条 API 记录；不是已追平')
                reported = time.monotonic()
        self.manager._event(self.job_id, 'info', 'RETAINED_DISCOVERY_INTERRUPTED',
                            '保留窗口核对在分页边界停止；下次重新核对，不推进完整性检查点')
        return False

    def pending(self, *, force_recent=False):
        if self.future.done(): self.future.result()  # propagate failures before admission
        if self.stopping(): return []
        now = datetime.now(UTC)
        if force_recent or time.monotonic() - self.last_recent >= REFRESH_SECONDS:
            fmt = lambda dt: dt.strftime('%Y-%m-%dT%H:%M:%SZ')
            for batch in self.client.iter_binlog_batches(fmt(now-LIVE_LOOKBACK), fmt(now)):
                if self.stopping(): return []
                self._ingest(batch)
            self.last_recent = time.monotonic()
        result = []
        finished = self.finished
        with self.observed_lock:
            confirmed_ids = self.confirmed_ids.copy()
            primary_windows = self.primary_windows.copy()
        records = self.manager.metadata.recoverable_files(
            self.settings.db_instance_id, include_discovered=True)
        for record in records:
            name = str(record['log_file_name'])
            if name.startswith(('general-log/', 'slow-log/', 'tabularis-audit-')): continue
            state, file_id = str(record['state']), str(record['id'])
            raw = self.manager.storage.paths['downloads'] / (file_id + '.binlog')
            local_resume = state == 'stored' or raw.is_file() or raw.with_suffix('.binlog.part').is_file()
            same_primary = not self.primary or str(record['host_instance_id']) == self.primary
            if not same_primary:
                # Preserve the original interrupted-file fallback only after the
                # complete primary-window set is known. Never mix duplicate hosts.
                if not finished or not local_resume: continue
                if (str(record['log_begin_utc']), str(record['log_end_utc'])) in primary_windows: continue
            if local_resume:
                confirmed_ids.add(file_id)
            if not local_resume:
                if str(record['remote_status']).lower() != 'completed': continue
                if str(record['log_end_utc']) < self.scan_start.strftime('%Y-%m-%dT%H:%M:%SZ'): continue
            item = RemoteBinlog(**{key: record[key] for key in (
                'log_file_name', 'log_begin_utc', 'log_end_utc', 'file_size',
                'checksum_crc64', 'download_link', 'intranet_download_link',
                'link_expired_utc', 'remote_status', 'host_instance_id')})
            result.append((file_id, item, state))
        deferred = sum(file_id not in confirmed_ids for file_id, _item, _state in result)
        if deferred:
            LOGGER.warning('Cached binlog availability not revalidated: %d pending records; '
                           'prefer API-confirmed/local files without discarding history', deferred)
        return fair_pending(result, now, confirmed_ids=confirmed_ids)
