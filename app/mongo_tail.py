"""The live tail of the MongoDB slow log: the minutes the collector has not published yet, read from DAS every 20 s.

Why it exists. The collector publishes whole five-minute windows once they ended LIVE_SETTLE_US ago, so an analysis that ends
"now" lacked up to 6.5 minutes and was moved back to the last complete windows (15 minutes earlier on 2026-10-28). DAS shows a
statement a few seconds after it started and has a minute complete about 20 s after the minute ended (measured 2026-10-28), so
this module reads the unpublished minutes, keeps their rollups in memory, and the analysis uses them for windows that have no
manifest yet (``MongoService.query``), ending at ``complete_through``.

What the buffer vouches for. Each scan re-reads from the first minute it did not vouch for yet up to now, and replaces those
minutes. ``complete_through`` is the start of the last scan minus GUARD_SECONDS, down to the minute: a statement that started
before it is in the buffer when it ran shorter than about GUARD_SECONDS minus DAS's 20 s. Longer statements arrive with the
published window a few minutes later, as before. A tail whose last scan is older than STALE_SECONDS vouches for nothing.

Only rollups are kept (the rows ``publish`` writes to ClickHouse), one set per source minute, never on disk; minutes the
collector has published are dropped. ``MONGO_SLOWLOG_TAIL=0`` turns it off (no thread, no calls).
"""
from __future__ import annotations

import logging
import os
import threading
import time

from .mongo_insight import COSTS, MINUTE, WINDOW, normalize_record, rollup_events

LOGGER = logging.getLogger(__name__)

POLL_SECONDS = 20
GUARD_SECONDS = 60
STALE_SECONDS = 90
BACKFILL_SECONDS = 15 * 60
RETAIN_SECONDS = 30 * 60
FAILURE_LOG_EVERY = 15
ENV_SWITCH = 'MONGO_SLOWLOG_TAIL'
SUMS = ('count', 'failed', 'collscan', 'spill', 'overlap_us') + COSTS + tuple(k + '_known' for k in COSTS)


def enabled() -> bool:
    return os.environ.get(ENV_SWITCH, '1').strip() not in ('0', 'false', 'off', 'no')


def merge_rows(rows, extra, *, width, lo, hi, role=''):
    """Add tail rollup rows (one-minute buckets) to the rows ``MongoStore.read`` aggregated, in the same shape."""
    merged = {}
    for row in rows:
        merged[(int(row['bucket']), row['role'], row['group_id'])] = dict(row)
    for row in extra:
        bucket = int(row['bucket'])
        if not lo <= bucket < hi or (role and row['role'] != role):
            continue
        bucket = bucket // width * width
        key = bucket, row['role'], row['group_id']
        into = merged.get(key)
        if into is None:
            into = merged[key] = {k: row[k] for k in ('role', 'group_id', 'profile', 'sample', 'max_us', 'first_us', 'last_us')}
            into.update(bucket=bucket, clients=row.get('clients') or '', accounts=row.get('accounts') or '',
                        **{k: int(row[k]) for k in SUMS})
            continue
        for k in SUMS:
            into[k] = int(into[k]) + int(row[k])
        if int(row['max_us']) >= int(into['max_us']):
            into['max_us'], into['sample'] = int(row['max_us']), row['sample']
        into['first_us'] = min(int(into['first_us']), int(row['first_us']))
        into['last_us'] = max(int(into['last_us']), int(row['last_us']))
        for k in ('clients', 'accounts'):
            # Newline-joined count objects add up in ``parse_counts``, as ClickHouse's groupArray join does.
            into[k] = '\n'.join(v for v in (into.get(k) or '', row.get(k) or '') if v)
    return list(merged.values())


class MongoSlowTail:
    def __init__(self, collector, published_through):
        """``published_through()`` is the start of the first window the collector has not published (its live checkpoint)."""
        self.collector = collector
        self.instance = collector.instance
        self.prefixes = collector.entry.get('families', [])
        self.published_through = published_through
        self.lock = threading.Lock()
        self.minutes = {}  # source minute start (µs) -> (rollup rows, record count)
        self.covered_from = None  # first minute the buffer holds completely
        self.complete_through = None  # µs, whole minute
        self.last_scan = 0.0  # wall clock of the last successful scan start
        self.failures = 0
        self.last_error = ''
        self.stop_event = collector.stop_event
        self.thread = None

    def start(self):
        if not enabled():
            LOGGER.warning('mongo_slowlog_tail disabled: instance=%s %s=0', self.instance, ENV_SWITCH)
            return
        self.thread = threading.Thread(target=self._loop, daemon=True, name='mongo-slowlog-tail')
        self.thread.start()

    def _loop(self):
        while not self.stop_event.is_set():
            begin = time.monotonic()
            try:
                self.scan()
                self.failures = 0
                self.last_error = ''
            except Exception as exc:  # noqa: BLE001 - a failed scan only ages the tail; the analysis then ignores it
                self.failures += 1
                self.last_error = (type(exc).__name__ + ':' + str(exc))[:160]
                if self.failures == 1 or self.failures % FAILURE_LOG_EVERY == 0:
                    LOGGER.warning('mongo_slowlog_tail unavailable: instance=%s failures=%s reason=%s',
                                   self.instance, self.failures, self.last_error)
            self.stop_event.wait(max(2.0, POLL_SECONDS - (time.monotonic() - begin)))

    def scan(self, now=None):
        now = time.time() if now is None else now
        scan_us = int(now) * 1_000_000
        floor = (scan_us - RETAIN_SECONDS * 1_000_000) // MINUTE * MINUTE
        published = (self.published_through() or 0) // MINUTE * MINUTE
        resume = self.complete_through
        if resume is None or resume < floor:
            resume = (scan_us - BACKFILL_SECONDS * 1_000_000) // MINUTE * MINUTE
        # Minutes inside published windows are served by ClickHouse; reading them again would only cost requests.
        lo = max(resume, published)
        if lo >= scan_us:
            return 0
        records = self.collector.das_records(lo, scan_us)
        by_minute = {}
        for record in records:
            event = normalize_record(record, self.instance, self.prefixes)
            by_minute.setdefault(event['start_us'] // MINUTE * MINUTE, []).append(event)
        fresh = {m: (rollup_events(by_minute.get(m, [])), len(by_minute.get(m, []))) for m in range(lo, scan_us, MINUTE)}
        through = (scan_us - GUARD_SECONDS * 1_000_000) // MINUTE * MINUTE
        with self.lock:
            # The buffer stays one unbroken run of minutes: it continues where the last scan stopped vouching, or restarts at lo.
            if self.covered_from is None or lo != self.complete_through:
                self.covered_from = lo
            # Dropped minutes leave the run too, so a window that is missing although published can never count as covered.
            self.covered_from = max(self.covered_from, floor, published)
            for m in [m for m in self.minutes if m >= lo or m < self.covered_from]:
                del self.minutes[m]
            self.minutes.update(fresh)
            self.complete_through = max(through, self.covered_from)
            self.last_scan = now
        return len(records)

    def fresh(self, now=None):
        now = time.time() if now is None else now
        return self.complete_through is not None and now - self.last_scan <= STALE_SECONDS

    def coverage(self, windows, start, now=None):
        """``(through, rows)`` when the tail can stand in for the missing ``windows`` of an analysis that starts at ``start``.

        ``through`` is where the buffer stops vouching (whole minute); ``rows`` maps each of those windows to the rollups of its minutes.
        None when the tail is stale or does not reach back to the first minute the analysis needs.
        """
        if not windows or not self.fresh(now):
            return None
        with self.lock:
            covered_from, through = self.covered_from, self.complete_through
            first = max(min(windows), start // MINUTE * MINUTE)
            if covered_from is None or first < covered_from or through <= first:
                return None
            rows = {window: [] for window in windows}
            for minute, (minute_rows, _) in self.minutes.items():
                if minute // WINDOW * WINDOW in rows and minute < through:
                    rows[minute // WINDOW * WINDOW].extend(minute_rows)
        return through, rows

    def status(self, now=None):
        now = time.time() if now is None else now
        with self.lock:
            records = sum(n for _, n in self.minutes.values())
            minutes = len(self.minutes)
        return dict(enabled=enabled(), fresh=self.fresh(now), complete_through_us=self.complete_through,
                    covered_from_us=self.covered_from, last_scan_age_seconds=round(now - self.last_scan, 1) if self.last_scan else None,
                    minutes=minutes, records=records, failures=self.failures, last_error=self.last_error)
