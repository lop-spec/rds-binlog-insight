"""Stream retained reconciliation while serving a fair live/catch-up queue.

No moving checkpoint can skip history: success requires finishing the retained
scan AND draining the cached pending files AND checking the live frontier again.
Only immutable archived events are published; each admitted batch retains its
source order. Fairness changes admission between batches, not commit safety.

Since 1.35.0 the retained scan walks bounded time slices (one 40-minute offset
scan over six hosts' files could skip or repeat rows while files were uploaded or
expired under it), repeats while the job runs, lists the late window and the time
range of every hole in a host's binlog numbering again, and re-reads the primary
node. Which hosts' files make the instance timeline is decided by ``HostPlan``.
"""
from __future__ import annotations

import logging
import re
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta

from .rds_api import RdsApiError, RemoteBinlog

LOGGER = logging.getLogger(__name__)
REFRESH_SECONDS = 60
LIVE_LOOKBACK = timedelta(hours=1)
# RDS uploads a closed binlog minutes after rotation, sometimes much later: the scanner lists this window again
# every LATE_REFRESH_SECONDS, so a late upload is not left to the next job (jobs ran up to six hours).
LATE_LOOKBACK = timedelta(hours=6)
LATE_REFRESH_SECONDS = 600
RETAINED_PASS_SECONDS = 3600
SLICE = timedelta(hours=6)
PRIMARY_REFRESH_SECONDS = 300
HOLE_REFRESH_SECONDS = 600
HOLE_QUERIES_PER_ROUND = 5
HOLE_QUERY_LIMIT = 3
# A live listing that keeps failing this long fails the job (the scheduler starts a new one); shorter outages are
# logged and the next refresh lists again.
LIVE_FAILURE_GRACE_SECONDS = 600
# Files this close to the end of the source retention go before everything else, oldest first.
URGENT_MARGIN = timedelta(hours=48)
PLAN_RELOAD_SECONDS = 30
# RDS writes the end of one file and the start of the next with the same second.
COVER_TOLERANCE_SECONDS = 2
NUMBER = re.compile(r"\.(\d+)$")


def source_order(entry):
    item = entry[1]
    return (item.log_begin_utc, item.log_end_utc, item.log_file_name, item.host_instance_id)


def _fmt(value: datetime) -> str:
    return value.strftime('%Y-%m-%dT%H:%M:%SZ')


def _epoch(text: str) -> float:
    try:
        return datetime.fromisoformat(str(text).replace('Z', '+00:00')).timestamp()
    except ValueError:
        return 0.0


def fair_pending(entries, now, *, confirmed_ids=None, urgent_before=None):
    """Alternate cold/live; prefer revalidated or local files within each cohort.

    Unconfirmed cached records remain pending, never archived or dropped. The
    retained scan continues independently and promotes them as pages arrive.
    Files that ended before ``urgent_before`` (close to the end of the source
    retention) come first, oldest first: in a backlog they are the next ones RDS
    deletes, so the live cohort must not hold half of the lanes then.
    """
    boundary = (now - LIVE_LOOKBACK).strftime('%Y-%m-%dT%H:%M:%SZ')
    cold, live, urgent = [], [], []
    unique = {entry[0]: entry for entry in entries}
    def order(entry):
        deferred = confirmed_ids is not None and entry[0] not in confirmed_ids
        return (deferred, source_order(entry))
    for entry in sorted(unique.values(), key=order):
        if urgent_before and entry[1].log_end_utc < urgent_before:
            urgent.append(entry)
        else:
            (live if entry[1].log_end_utc >= boundary else cold).append(entry)
    result = sorted(urgent, key=source_order)
    for index in range(max(len(cold), len(live))):
        if index < len(cold): result.append(cold[index])
        if index < len(live): result.append(live[index])
    return result


def _merge(spans):
    merged = []
    for lo, hi in sorted(spans):
        if merged and lo <= merged[-1][1] + COVER_TOLERANCE_SECONDS:
            merged[-1][1] = max(merged[-1][1], hi)
        else:
            merged.append([lo, hi])
    return merged


class HostPlan:
    """Which hosts' files make the instance timeline: one copy of each change and no hole at a primary switch.

    RDS lists the binlogs of every node (all six of a cluster) and each node logs every change. Taking only the
    primary's files lost the former primary's files that were not collected yet when the primary changed (the
    source keeps them seven days), and took the new primary's files from before the switch a second time
    (2026-10-29: two hours of row changes stored twice). The registered files are the timeline; a host with
    registered files was a primary. A new file is taken when it is

    * the primary's and not fully covered by registered files of other hosts, or
    * a former primary's, starts before the primary's own run of files and is not covered by the primary.

    A pure replica's file is never taken. A registered file is never dropped again; only a former primary's
    unfinished file that the primary's files cover is settled as ``skipped``.
    """

    def __init__(self, rows, primary):
        self.primary = str(primary or '')
        self.spans = defaultdict(list)
        for row in rows:
            self.spans[str(row['host_instance_id'])].append((_epoch(row['log_begin_utc']), _epoch(row['log_end_utc'])))
        self._merged = {}
        self._floor = None

    @classmethod
    def load(cls, metadata, instance_id, primary, since_utc):
        return cls(metadata.binlog_host_rows(instance_id, since_utc), primary)

    def merged(self, host):
        cached = self._merged.get(host)
        if cached is None:
            cached = self._merged[host] = _merge(self.spans.get(host, []))
        return cached

    def covered(self, hosts, begin, end):
        return any(lo <= begin and hi >= end for host in hosts for lo, hi in self.merged(host))

    def primary_floor(self):
        """Start of the primary's latest run of files that other hosts do not cover (inf before it has any)."""
        if self._floor is None:
            others = [host for host in self.spans if host != self.primary]
            floor, previous_hi = float('inf'), None
            for lo, hi in sorted(self.spans.get(self.primary, [])):
                if self.covered(others, lo, hi):
                    continue
                if previous_hi is None or lo > previous_hi + COVER_TOLERANCE_SECONDS:
                    floor = lo
                previous_hi = hi if previous_hi is None else max(previous_hi, hi)
            self._floor = floor
        return self._floor

    def accept_new(self, item):
        host = str(item.host_instance_id)
        begin, end = _epoch(item.log_begin_utc), _epoch(item.log_end_utc)
        if not self.primary:
            return host in self.spans
        if host == self.primary:
            return not self.covered([h for h in self.spans if h != host], begin, end)
        if host in self.spans:
            return begin < self.primary_floor() and not self.covered([self.primary], begin, end)
        return False

    def covered_by_primary(self, host, begin_text, end_text):
        return bool(self.primary) and str(host) != self.primary and self.covered(
            [self.primary], _epoch(begin_text), _epoch(end_text))

    def add(self, item):
        host = str(item.host_instance_id)
        self.spans[host].append((_epoch(item.log_begin_utc), _epoch(item.log_end_utc)))
        self._merged.pop(host, None)
        self._floor = None


def sequence_holes(rows, since_epoch):
    """Missing binlog numbers between two registered files of one host, newest first, with their time window."""
    by_host = defaultdict(list)
    for row in rows:
        match = NUMBER.search(str(row['log_file_name']))
        if match:
            by_host[str(row['host_instance_id'])].append((int(match.group(1)), row))
    holes = []
    for host, numbered in by_host.items():
        numbered.sort(key=lambda pair: pair[0])
        for (low, left), (high, right) in zip(numbered, numbered[1:]):
            if high - low > 1 and _epoch(right['log_begin_utc']) >= since_epoch:
                holes.append((host, str(left['log_file_name']), str(right['log_file_name']),
                              str(left['log_end_utc']), str(right['log_begin_utc']), high - low - 1))
    holes.sort(key=lambda hole: hole[4], reverse=True)
    return holes


class RetainedDiscovery:
    def __init__(self, manager, job_id, client, settings, primary, *, resolve_primary=None, eligible=None,
                 retention_days=None):
        self.manager, self.job_id, self.client = manager, job_id, client
        self.settings, self.primary = settings, primary
        self.resolve_primary = resolve_primary
        self.eligible = eligible
        self.retention_days = retention_days
        self.scan_start = manager._scan_start(settings)
        self.scan_end = datetime.now(UTC)
        self.stop = threading.Event()
        self.last_recent = float('-inf')
        self.live_failing_since = None
        self.confirmed_ids = set()
        self.observed_lock = threading.Lock()
        self.plan_lock = threading.Lock()
        self._plan = None
        self._plan_at = float('-inf')
        self.hole_queries = defaultdict(int)
        self.pool = None
        self.future = None
        self.followup = None

    def __enter__(self):
        self.pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix='binlog-discovery')
        self.future = self.pool.submit(self._reconcile)
        self.followup = self.pool.submit(self._maintain)
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.stop.set()
        self.pool.shutdown(wait=True, cancel_futures=True)
        for future in (self.future, self.followup):
            if future is None or not future.done() or future.cancelled():
                continue
            failure = future.exception()
            if failure is not None:
                # Never silently lose a producer failure on pause or another error.
                LOGGER.error('Retained discovery ended with %s; parent_error=%s',
                             type(failure).__name__, exc_type)
                if exc_type is None and not self.stopping():
                    raise failure
        return False

    def stopping(self):
        manager = self.manager
        aborting = getattr(manager, '_abort_requested', None)
        return (manager._pause_after_current.is_set() or manager._shutdown.is_set()
                or (callable(aborting) and aborting()))

    def _touch(self):
        touch = getattr(self.manager, '_touch', None)
        if callable(touch):
            touch()

    @property
    def finished(self):
        return self.future.done() and self.future.result() is True

    # -- which files to take --------------------------------------------------------------------------------------
    def plan(self, *, reload=False):
        with self.plan_lock:
            if reload or self._plan is None or time.monotonic() - self._plan_at >= PLAN_RELOAD_SECONDS:
                since = _fmt(self.manager._scan_start(self.settings) - timedelta(days=1))
                self._plan = HostPlan.load(self.manager.metadata, self.settings.db_instance_id, self.primary, since)
                self._plan_at = time.monotonic()
            return self._plan

    def _ingest(self, batch, *, retained=False):
        metadata = self.manager.metadata
        instance = self.settings.db_instance_id
        ids = [metadata.file_id(instance, item) for item in batch]
        known = metadata.existing_file_states(instance, ids)
        plan = self.plan()
        items = []
        with self.plan_lock:
            for file_id, item in zip(ids, batch):
                if file_id in known:
                    if known[file_id] not in ('skipped', 'superseded'):
                        items.append(item)
                elif plan.accept_new(item):
                    plan.add(item)
                    items.append(item)
        records = metadata.upsert_remotes(self.settings, items)
        with self.observed_lock:
            self.confirmed_ids.update(file_id for (file_id, _state), item in zip(records, items)
                                      if item.remote_status.lower() == 'completed')
        self._touch()
        return len(items)

    def _list_window(self, begin, end, *, retained=False):
        """One bounded listing; a page count that drifts under it is listed again (at most three times)."""
        for attempt in range(3):
            listed = 0
            try:
                for batch in self.client.iter_binlog_batches(_fmt(begin), _fmt(end)):
                    if self.stop.is_set() or self.stopping():
                        return listed, False
                    self._ingest(batch, retained=retained)
                    listed += len(batch)
                return listed, True
            except RdsApiError as exc:
                if exc.code != 'INCOMPLETE_PAGINATION' or attempt == 2:
                    raise
                LOGGER.warning('RETAINED_SLICE_RELIST begin=%s end=%s attempt=%s', _fmt(begin), _fmt(end), attempt + 1)
        return 0, True

    def _retained_pass(self):
        start = self.manager._scan_start(self.settings)
        end = datetime.now(UTC)
        self.scan_start, self.scan_end = start, end
        count, reported, cursor = 0, time.monotonic(), start
        while cursor < end:
            if self.stop.is_set() or self.stopping():
                return count, False
            upper = min(cursor + SLICE, end)
            listed, complete = self._list_window(cursor, upper, retained=True)
            count += listed
            if not complete:
                return count, False
            cursor = upper
            if time.monotonic() - reported >= 30:
                self.manager._event(self.job_id, 'info', 'RETAINED_DISCOVERY_PROGRESS',
                                    f'保留窗口仍在核对：{count} 条 API 记录；不是已追平')
                reported = time.monotonic()
        return count, True

    def _reconcile(self):
        self.manager._event(self.job_id, 'info', 'RETAINED_DISCOVERY_STARTED',
                            '保留窗口逐页核对；不阻塞已知文件恢复和新日志发现')
        count, complete = self._retained_pass()
        if complete:
            self.manager._event(self.job_id, 'info', 'RETAINED_DISCOVERY_COMPLETE',
                                f'保留窗口核对完成：{count} 条 API 记录')
            return True
        self.manager._event(self.job_id, 'info', 'RETAINED_DISCOVERY_INTERRUPTED',
                            '保留窗口核对在分页边界停止；下次重新核对，不推进完整性检查点')
        return False

    def _maintain(self):
        """After the first pass: re-read the primary, list the late window and numbering holes, repeat the pass."""
        while not self.stop.is_set() and not self.future.done():
            self.stop.wait(1)
        if self.stop.is_set() or self.future.cancelled() or self.future.exception() is not None \
                or not self.future.result():
            return False
        now = time.monotonic()
        due = {'primary': now + PRIMARY_REFRESH_SECONDS, 'late': now + LATE_REFRESH_SECONDS,
               'holes': now + HOLE_REFRESH_SECONDS, 'pass': now + RETAINED_PASS_SECONDS}
        failures = 0
        while not self.stop.wait(1):
            if self.stopping():
                return True
            now = time.monotonic()
            try:
                if now >= due['primary']:
                    due['primary'] = now + PRIMARY_REFRESH_SECONDS
                    self._refresh_primary()
                if now >= due['late']:
                    due['late'] = now + LATE_REFRESH_SECONDS
                    current = datetime.now(UTC)
                    self._list_window(current - LATE_LOOKBACK, current - LIVE_LOOKBACK)
                if now >= due['holes']:
                    due['holes'] = now + HOLE_REFRESH_SECONDS
                    self._repair_holes()
                if now >= due['pass']:
                    due['pass'] = now + RETAINED_PASS_SECONDS
                    count, complete = self._retained_pass()
                    LOGGER.info('RETAINED_DISCOVERY_REPEAT records=%s complete=%s', count, complete)
                failures = 0
            except RdsApiError as exc:
                failures += 1
                LOGGER.warning('RETAINED_DISCOVERY_RETRY code=%s failures=%s', exc.code, failures)
                self.stop.wait(min(30 * failures, 300))
        return True

    def _refresh_primary(self):
        if not callable(self.resolve_primary):
            return
        try:
            primary = str(self.resolve_primary() or '')
        except Exception as exc:  # noqa: BLE001 - keep the known primary; the next round asks again
            LOGGER.warning('PRIMARY_REFRESH_FAILED keep=%s error=%s', self.primary, exc)
            return
        if primary and primary != self.primary:
            self.manager._event(self.job_id, 'warning', 'PRIMARY_CHANGED',
                                f'主节点由 {self.primary or "未知"} 变为 {primary}；前主未采完的文件继续采，新主切换前的重复文件不采')
            self.primary = primary
            self.plan(reload=True)

    def _repair_holes(self):
        """List the time range of each hole in a registered host's binlog numbering again (a file discovery missed)."""
        retention = self.retention_days or 7
        since = time.time() - retention * 86400
        rows = self.manager.metadata.binlog_host_rows(
            self.settings.db_instance_id, _fmt(datetime.now(UTC) - timedelta(days=retention, hours=1)))
        asked = 0
        for host, left, right, begin, end, missing in sequence_holes(rows, since):
            key = (host, left, right)
            if asked >= HOLE_QUERIES_PER_ROUND or self.hole_queries[key] >= HOLE_QUERY_LIMIT:
                continue
            self.hole_queries[key] += 1
            asked += 1
            lo = datetime.fromisoformat(begin.replace('Z', '+00:00')) - timedelta(seconds=1)
            hi = datetime.fromisoformat(end.replace('Z', '+00:00')) + timedelta(seconds=1)
            listed, _ = self._list_window(lo, max(hi, lo + timedelta(seconds=2)))
            LOGGER.warning('BINLOG_SEQUENCE_HOLE host=%s after=%s before=%s missing=%s listed=%s attempt=%s',
                           host, left, right, missing, listed, self.hole_queries[key])

    # -- what to process next -------------------------------------------------------------------------------------
    def _live_refresh(self, force_recent):
        if not (force_recent or time.monotonic() - self.last_recent >= REFRESH_SECONDS):
            return
        now = datetime.now(UTC)
        try:
            for batch in self.client.iter_binlog_batches(_fmt(now-LIVE_LOOKBACK), _fmt(now)):
                if self.stopping(): return
                self._ingest(batch)
        except RdsApiError as exc:
            # One throttled or dropped listing used to fail the whole job; the next refresh lists again.
            self.live_failing_since = self.live_failing_since or time.monotonic()
            if time.monotonic() - self.live_failing_since >= LIVE_FAILURE_GRACE_SECONDS:
                raise
            LOGGER.warning('LIVE_DISCOVERY_RETRY code=%s failing_for=%.0fs', exc.code,
                           time.monotonic() - self.live_failing_since)
            self.last_recent = time.monotonic()
            return
        self.live_failing_since = None
        self.last_recent = time.monotonic()

    def pending(self, *, force_recent=False):
        if self.future.done(): self.future.result()  # propagate failures before admission
        if self.followup is not None and self.followup.done() and not self.followup.cancelled():
            self.followup.result()
        if self.stopping(): return []
        self._live_refresh(force_recent)
        if self.stopping(): return []
        now = datetime.now(UTC)
        result = []
        finished = self.finished
        with self.observed_lock:
            confirmed_ids = self.confirmed_ids.copy()
        plan = self.plan()
        records = self.manager.metadata.recoverable_files(
            self.settings.db_instance_id, include_discovered=True)
        scan_floor = _fmt(self.manager._scan_start(self.settings))
        settle = []
        for record in records:
            name = str(record['log_file_name'])
            if name.startswith(('general-log/', 'slow-log/', 'tabularis-audit-')): continue
            state, file_id = str(record['state']), str(record['id'])
            raw = self.manager.storage.paths['downloads'] / (file_id + '.binlog')
            local_resume = state == 'stored' or raw.is_file() or raw.with_suffix('.binlog.part').is_file()
            host = str(record['host_instance_id'])
            if self.primary and host != self.primary:
                # A former primary's file stays in the timeline unless the primary's own files cover its span; that
                # is known once the retained pass has registered the primary's files.
                if not finished and not local_resume: continue
                with self.plan_lock:
                    covered = plan.covered_by_primary(host, record['log_begin_utc'], record['log_end_utc'])
                if covered:
                    if not local_resume and state in ('discovered', 'failed'):
                        settle.append(file_id)
                    continue
            if local_resume:
                confirmed_ids.add(file_id)
            if not local_resume:
                if str(record['remote_status']).lower() != 'completed': continue
                if str(record['log_end_utc']) < scan_floor: continue
            if callable(self.eligible) and not self.eligible(file_id): continue
            item = RemoteBinlog(**{key: record[key] for key in (
                'log_file_name', 'log_begin_utc', 'log_end_utc', 'file_size',
                'checksum_crc64', 'download_link', 'intranet_download_link',
                'link_expired_utc', 'remote_status', 'host_instance_id')})
            result.append((file_id, item, state))
        if settle:
            changed = self.manager.metadata.mark_files_state(
                settle, 'skipped', 'COVERED_BY_PRIMARY', '主节点已采文件覆盖同一时间段；不重复采集',
                only_states=('discovered', 'failed'))
            LOGGER.info('BINLOG_FILES_SKIPPED reason=covered-by-primary files=%s', changed)
        deferred = sum(file_id not in confirmed_ids for file_id, _item, _state in result)
        if deferred:
            LOGGER.warning('Cached binlog availability not revalidated: %d pending records; '
                           'prefer API-confirmed/local files without discarding history', deferred)
        urgent_before = None
        if self.retention_days:
            urgent_before = _fmt(now - timedelta(days=self.retention_days) + URGENT_MARGIN)
        return fair_pending(result, now, confirmed_ids=confirmed_ids, urgent_before=urgent_before)
