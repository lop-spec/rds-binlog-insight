"""The live tail of the slow log: the newest slow statements of every collected node, straight from DAS, kept in memory.

Why it exists. The collector's pipeline (poll every 120 s up to "now - 90 s", index the part, copy it into ClickHouse) leaves the
newest 3 to 6 minutes out of every analysis. DAS itself shows a statement soon after it ended: on 2026-10-14 (6353 statements of
five nodes, first sighting minus end of statement) the median was 41 s, 98.5 % were visible within 75 s and the longest took
83 s, because RDS hands the slow log over once a minute. So this module asks DAS every 20 s, keeps the last 30 minutes of every
node, and the analysis adds what ClickHouse does not have yet (``slowlog_impact_service``).

What the buffer vouches for. ``complete_through`` is the start of the last scan minus PUBLISH_GUARD_SECONDS: a statement that
started before it, has ended and has been published is in the buffer, provided it ran no longer than the scan window minus the
guard (the 150 s window of a busy node: about 47 s; a quiet node reads 10 minutes: about 8 minutes). Longer statements arrive
through the normal pipeline a few minutes after they end, exactly as before. A node whose last scan is older than STALE_SECONDS
vouches for nothing, and neither does an instance that has such a node: the analysis then behaves as it did without the tail.

Nothing is stored on disk. A restart refills the buffer with one 15 minute read per node. ``RDS_BINLOG_SLOWLOG_TAIL=0`` turns
the whole thing off (no threads, no calls).
"""
from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from collections import Counter
from datetime import UTC, datetime
from typing import Any, Callable, Iterable

from .slow_log_collector import SlowLogConfig, build_slow_events

LOGGER = logging.getLogger(__name__)

ACTION = "DescribeSlowLogRecords"
PAGE_SIZE = 100
POLL_SECONDS = 20.0
MIN_PAUSE_SECONDS = 2.0
MIN_WINDOW_SECONDS = 150
MAX_WINDOW_SECONDS = 600
# A scan window is sized for about this many records (8 pages), so a busy node reads a short window and a quiet one a long one.
WINDOW_RECORD_BUDGET = 800
BACKFILL_SECONDS = 15 * 60
RETAIN_SECONDS = 30 * 60
# The longest publication delay measured on 2026-10-14 was 83 s.
PUBLISH_GUARD_SECONDS = 90
# Added to a pause when the next scan reads back across it.
CATCH_UP_SLACK_SECONDS = 30
STALE_SECONDS = 90
MAX_PAGES = 120
SQL_KEEP_CHARS = 2000
CLIENT_REUSE_SECONDS = 300
FAILURE_LOG_EVERY = 15
ENV_SWITCH = "RDS_BINLOG_SLOWLOG_TAIL"


def _columns(event: dict[str, Any]) -> dict[str, Any]:
    try:
        value = json.loads(event.get("columns_json") or "{}")
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _count(value: Any) -> int:
    try:
        return max(int(float(value)), 0)
    except (TypeError, ValueError):
        return 0


class TailEvent:
    """One statement as the analysis and the detail drawer need it (the full pipeline event is not kept)."""

    __slots__ = ("event_id", "start_us", "instance_id", "node_id", "database_name", "table_name", "operation", "fingerprint",
                 "sql_id", "duration_ms", "rows_examined", "rows_sent", "lock_time_ms", "account", "client_ip", "thread_id",
                 "sql_text")

    def __init__(self, event: dict[str, Any], extra: dict[str, Any], instance_id: str) -> None:
        profile = extra.get("statement_profile") or {}
        self.event_id = str(event["event_id"])
        self.start_us = int(event["event_epoch_us"])
        self.instance_id = sys.intern(instance_id)
        self.node_id = sys.intern(str(extra.get("node_id") or ""))
        self.database_name = sys.intern(str(event.get("database_name") or ""))
        self.table_name = sys.intern(str(event.get("table_name") or ""))
        self.operation = sys.intern(str(event.get("operation") or ""))
        self.fingerprint = sys.intern(str(profile.get("fingerprint") or ""))
        self.sql_id = sys.intern(str(extra.get("sql_id") or ""))
        self.duration_ms = _count(event.get("execution_time_ms"))
        self.rows_examined = _count(extra.get("rows_examined"))
        self.rows_sent = _count(extra.get("rows_sent"))
        self.lock_time_ms = _count(extra.get("lock_time_ms"))
        self.account = sys.intern(str(event.get("database_account") or ""))
        self.client_ip = sys.intern(str(event.get("connection_name") or ""))
        self.thread_id = _count(event.get("thread_id"))
        self.sql_text = str(event.get("sql_text") or "")[:SQL_KEEP_CHARS]

    def analysis_row(self) -> dict[str, Any]:
        """The shape ``read_events`` of the rise analysis returns for a ClickHouse row (a zero cost is 'not reported')."""
        return {"event_id": self.event_id, "start_us": self.start_us, "node_id": self.node_id,
                "database_name": self.database_name, "fingerprint": self.fingerprint, "sql_id": self.sql_id,
                "duration_ms": self.duration_ms, "rows_examined": self.rows_examined or None,
                "lock_time_ms": self.lock_time_ms or None, "account": self.account}

    def detail(self) -> dict[str, Any]:
        """The answer of ``/api/event`` for a slow-log statement, built from what the tail holds."""
        return {
            "event_id": self.event_id, "event_epoch_us": self.start_us,
            "event_time_utc": datetime.fromtimestamp(self.start_us / 1_000_000, UTC).isoformat().replace("+00:00", "Z"),
            "instance_id": self.instance_id, "node_id": self.node_id, "operation": self.operation,
            "database_name": self.database_name, "table_name": self.table_name, "fingerprint": self.fingerprint,
            "transaction_id": "", "gtid": "", "xid": "", "sql_kind": "ORIGINAL", "sql_text": self.sql_text,
            "before_json": "", "after_json": "", "source_file_name": "live-tail", "host_instance_id": "slow-log",
            "server_id": 0, "thread_id": self.thread_id, "start_position": 0, "end_position": 0, "row_index": 0,
            "execution_time_ms": self.duration_ms, "query_time_ms": self.duration_ms, "error_code": 0, "row_query": "",
            "raw_event_type": "SLOW_LOG", "connection_id": "", "connection_name": self.client_ip, "client_ip": self.client_ip,
            "database_account": self.account, "execution_status": "success", "error_message": "",
            "affected_rows": self.rows_sent, "started_epoch_us": self.start_us,
            "finished_epoch_us": self.start_us + self.duration_ms * 1000, "batch_id": "", "statement_index": -1,
            "transaction_context_id": "", "locator": "", "rows_examined": self.rows_examined, "rows_sent": self.rows_sent,
            "lock_time_ms": self.lock_time_ms, "sql_id": self.sql_id,
            "columns_json": json.dumps({"rows_examined": self.rows_examined, "rows_sent": self.rows_sent,
                                        "lock_time_ms": self.lock_time_ms, "sql_id": self.sql_id, "node_id": self.node_id},
                                       ensure_ascii=False, separators=(",", ":")),
            "tiers_used": ["live-tail"],
        }


def scope_matches(event: TailEvent, query: dict[str, Any]) -> bool:
    """The filters of ``ClickHouseSlowLogBackend._scope_sql`` (node, database, table, operation), applied in memory."""
    node = str(query.get("node_id") or "").strip()
    if node and event.node_id != node:
        return False
    database = str(query.get("database") or "").strip()
    if database and event.database_name.lower() != database.lower():
        return False
    table = str(query.get("table") or "").strip()
    if table:
        if "," not in table:
            # A slow-log row can name several JOIN participants: match one of the comma separated names.
            if table.lower() not in {part.strip().lower() for part in event.table_name.split(",")}:
                return False
        elif event.table_name.lower() != table.lower():
            return False
    operation = str(query.get("operation") or "").strip().upper()
    return not operation or event.operation == operation


class NodeTail:
    """The newest statements of one node. ``scan`` is called every POLL_SECONDS by the service's thread for this node."""

    def __init__(self, config: SlowLogConfig, client_factory: Callable[[], Any], *, clock: Callable[[], float] = time.time) -> None:
        self.config = config
        self._client_factory = client_factory
        self._clock = clock
        self._lock = threading.Lock()
        self._events: dict[str, TailEvent] = {}
        self._profiles: dict[str, dict[str, Any]] = {}
        self._references: Counter[str] = Counter()
        self._newest_us = 0
        self._covered_from_us = 0
        self._last_start = 0.0
        self._last_end = 0.0
        self._window_seconds = MIN_WINDOW_SECONDS
        self._client: Any = None
        self._client_made = 0.0
        self._scans = 0
        self._failures = 0
        self._failure_streak = 0
        self._last_error = ""
        self._last_new = 0
        self._last_records = 0
        self._last_pages = 0

    # ------------------------------------------------------------------ reading DAS

    def _params(self, low_ms: int, high_ms: int, page: int) -> dict[str, Any]:
        params: dict[str, Any] = {"InstanceId": self.config.instance_id, "StartTime": int(low_ms), "EndTime": int(high_ms),
                                  "PageSize": PAGE_SIZE, "PageNumber": int(page)}
        if self.config.node_id:
            params["NodeId"] = self.config.node_id
        return params

    def _das(self) -> Any:
        now = self._clock()
        if self._client is None or now - self._client_made > CLIENT_REUSE_SECONDS:
            self._client = self._client_factory()
            self._client_made = now
        return self._client

    def _read(self, low_ms: int, high_ms: int) -> tuple[list[dict[str, Any]], int, bool]:
        """Every record of the window, newest first: (records, pages read, whether the page cap cut the answer)."""
        records: list[dict[str, Any]] = []
        page = 1
        try:
            client = self._das()
            while True:
                if page > MAX_PAGES:
                    return records, page - 1, True
                response = client.call(ACTION, self._params(low_ms, high_ms, page))
                data = response.get("Data") if isinstance(response.get("Data"), dict) else response
                items = data.get("Logs") or []
                response_node = str(data.get("NodeId") or self.config.node_id).strip()
                for item in items:
                    if isinstance(item, dict):
                        record = dict(item)
                        node = str(record.get("NodeId") or response_node).strip()
                        if node:
                            record["NodeId"] = node
                        records.append(record)
                if len(items) < PAGE_SIZE or len(records) >= int(data.get("TotalRecords") or 0):
                    return records, page, False
                page += 1
        except Exception:
            self._client = None  # build a fresh client (and credential) on the next attempt
            raise

    def scan(self) -> int:
        """Read the newest statements once; returns how many were new. Raises when DAS cannot be read (the caller counts it)."""
        started = self._clock()
        with self._lock:
            first = self._covered_from_us == 0
            known = set(self._events)
            previous_start = self._last_start
            # After a pause (a failed or slow scan) read back far enough to reach what the pause left unread, so the coverage
            # stays one stretch; only a pause longer than the backfill window restarts it.
            pause = started - previous_start if previous_start else 0.0
            window = BACKFILL_SECONDS if first else int(min(BACKFILL_SECONDS, max(
                self._window_seconds, pause + PUBLISH_GUARD_SECONDS + CATCH_UP_SLACK_SECONDS)))
        low_ms, high_ms = int((started - window) * 1000), int(started * 1000)
        records, pages, capped = self._read(low_ms, high_ms)
        events, _observed = build_slow_events(records, instance_id=self.config.instance_id, default_node_id=self.config.node_id,
                                              exclude_accounts=self.config.exclude_accounts, seen_ids=known)
        window_start_us = low_ms * 1000
        stamps = [_count(record.get("Timestamp")) * 1000 for record in records if _count(record.get("Timestamp"))]
        # The answer is newest first, so a capped read lacks its oldest part: it only vouches for what it reached.
        reached_us = max(window_start_us, min(stamps, default=window_start_us)) if capped else window_start_us
        cutoff_us = int((started - RETAIN_SECONDS) * 1_000_000)
        continuous = (bool(previous_start) and started - previous_start <= window - PUBLISH_GUARD_SECONDS
                      and (not capped or reached_us <= int((previous_start - PUBLISH_GUARD_SECONDS) * 1_000_000)))
        with self._lock:
            added = 0
            for event in events:
                extra = _columns(event)
                item = TailEvent(event, extra, self.config.instance_id)
                if not item.fingerprint:
                    continue
                self._events[item.event_id] = item
                self._references[item.fingerprint] += 1
                self._newest_us = max(self._newest_us, item.start_us)
                added += 1
                profile = self._profiles.get(item.fingerprint)
                if profile is None:
                    detail = extra.get("statement_profile") or {}
                    self._profiles[item.fingerprint] = {
                        "instance_id": item.instance_id, "fingerprint": item.fingerprint, "sql_id": item.sql_id,
                        "action": str(detail.get("action") or ""), "normalized_sql": str(detail.get("normalized") or ""),
                        "sample_sql": str(detail.get("sample") or ""), "first_seen_us": item.start_us,
                        "last_seen_us": item.start_us}
                else:
                    profile["last_seen_us"] = max(int(profile["last_seen_us"]), item.start_us)
            self._prune(cutoff_us)
            # One stretch of coverage: it starts where the first window started, restarts when two scans were too far apart for
            # their windows to overlap, and never reaches back past what is still retained.
            if first or not continuous:
                self._covered_from_us = reached_us
            self._covered_from_us = max(self._covered_from_us, cutoff_us)
            self._last_start, self._last_end = started, self._clock()
            self._scans += 1
            self._failure_streak = 0
            self._last_error = ""
            self._last_new, self._last_records, self._last_pages = added, len(records), pages
            rate = len(records) / max(window, 1)
            self._window_seconds = int(min(MAX_WINDOW_SECONDS, max(MIN_WINDOW_SECONDS,
                                                                    WINDOW_RECORD_BUDGET / rate if rate > 0 else MAX_WINDOW_SECONDS)))
        return added

    def _prune(self, cutoff_us: int) -> None:
        expired = [key for key, item in self._events.items() if item.start_us < cutoff_us]
        for key in expired:
            item = self._events.pop(key)
            self._references[item.fingerprint] -= 1
            if self._references[item.fingerprint] <= 0:
                del self._references[item.fingerprint]
                self._profiles.pop(item.fingerprint, None)
        if expired:
            self._newest_us = max((item.start_us for item in self._events.values()), default=0)

    def note_failure(self, exc: BaseException) -> None:
        with self._lock:
            self._failures += 1
            self._failure_streak += 1
            self._last_error = f"{type(exc).__name__}: {str(exc)[:160]}"
            streak = self._failure_streak
        if streak == 1 or streak % FAILURE_LOG_EVERY == 0:
            LOGGER.warning("slow-log live tail scan failed (%d in a row): instance=%s node=%s %s: %s", streak,
                           self.config.instance_id, self.config.node_id or "(default)", type(exc).__name__, str(exc)[:300])

    # ------------------------------------------------------------------ what the analysis asks

    def view(self, now: float | None = None) -> dict[str, Any] | None:
        """What this node vouches for right now, or None when its last scan is missing or too old."""
        now = self._clock() if now is None else now
        with self._lock:
            if not self._last_start or not self._covered_from_us or now - self._last_start > STALE_SECONDS:
                return None
            return {"node_id": self.config.node_id, "covered_from_us": self._covered_from_us,
                    "complete_through_us": int((self._last_start - PUBLISH_GUARD_SECONDS) * 1_000_000),
                    "newest_us": self._newest_us, "events": len(self._events)}

    def rows(self, start_us: int, end_us: int, query: dict[str, Any]) -> list[dict[str, Any]]:
        with self._lock:
            return [item.analysis_row() for item in self._events.values()
                    if start_us <= item.start_us <= end_us and scope_matches(item, query)]

    def event(self, event_id: str) -> TailEvent | None:
        with self._lock:
            return self._events.get(event_id)

    def profiles(self, fingerprints: Iterable[str]) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {fp: dict(self._profiles[fp]) for fp in fingerprints if fp in self._profiles}

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {"instanceId": self.config.instance_id, "nodeId": self.config.node_id, "events": len(self._events),
                    "coveredFromUs": self._covered_from_us, "newestUs": self._newest_us, "windowSeconds": self._window_seconds,
                    "lastScanStart": self._last_start, "lastScanSeconds": round(self._last_end - self._last_start, 2),
                    "lastRecords": self._last_records, "lastNew": self._last_new, "lastPages": self._last_pages,
                    "scans": self._scans, "failures": self._failures, "lastError": self._last_error}


class SlowLogTails:
    """One NodeTail and one thread per collected node; the analysis and the detail drawer read through this object."""

    def __init__(self, collectors: Iterable[Any], *, poll_seconds: float = POLL_SECONDS, clock: Callable[[], float] = time.time,
                 enabled: bool | None = None) -> None:
        self.enabled = (os.environ.get(ENV_SWITCH, "1").strip() != "0") if enabled is None else enabled
        self._poll_seconds = poll_seconds
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        self._tails: list[NodeTail] = []
        if self.enabled:
            self._tails = [NodeTail(item.config, item.das_client, clock=clock) for item in collectors
                           if item.config.enabled and item.config.instance_id]
        else:
            LOGGER.info("slow-log live tail is off (%s=0)", ENV_SWITCH)

    def start(self) -> None:
        for index, tail in enumerate(self._tails):
            delay = self._poll_seconds * index / max(len(self._tails), 1)  # spread the nodes over one cycle
            thread = threading.Thread(target=self._run, args=(tail, delay), daemon=True,
                                      name=f"slow-log-tail-{tail.config.state_suffix}")
            thread.start()
            self._threads.append(thread)
        if self._tails:
            LOGGER.info("slow-log live tail started: %d node(s), every %.0fs", len(self._tails), self._poll_seconds)

    def shutdown(self) -> None:
        self._stop.set()
        for thread in self._threads:
            thread.join(timeout=5)

    def _run(self, tail: NodeTail, delay: float) -> None:
        self._stop.wait(delay)
        recovering = False
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                tail.scan()
                if recovering:
                    LOGGER.info("slow-log live tail recovered: instance=%s node=%s", tail.config.instance_id,
                                tail.config.node_id or "(default)")
                    recovering = False
            except Exception as exc:  # noqa: BLE001 - a resident thread must survive any single failure
                tail.note_failure(exc)
                recovering = True
            self._stop.wait(max(self._poll_seconds - (time.monotonic() - started), MIN_PAUSE_SECONDS))

    # ------------------------------------------------------------------ the analysis side

    def _of(self, instance: str) -> list[NodeTail]:
        return [tail for tail in self._tails if tail.config.instance_id == instance]

    def view(self, instance: str) -> dict[str, Any] | None:
        """What the live tail vouches for across the whole instance: the intersection of its nodes' coverage, or None."""
        tails = self._of(instance)
        if not tails:
            return None
        views = [tail.view() for tail in tails]
        if any(view is None for view in views):
            return None
        return {"covered_from_us": max(view["covered_from_us"] for view in views),
                "complete_through_us": min(view["complete_through_us"] for view in views),
                "newest_us": max(view["newest_us"] for view in views), "nodes": len(views),
                "events": sum(view["events"] for view in views)}

    def rows(self, instance: str, start_us: int, end_us: int, query: dict[str, Any]) -> list[dict[str, Any]]:
        return [row for tail in self._of(instance) for row in tail.rows(start_us, end_us, query)]

    def profiles(self, keys: Iterable[tuple[str, str]]) -> dict[tuple[str, str], dict[str, Any]]:
        found: dict[tuple[str, str], dict[str, Any]] = {}
        wanted: dict[str, set[str]] = {}
        for instance, fingerprint in keys:
            wanted.setdefault(instance, set()).add(fingerprint)
        for instance, fingerprints in wanted.items():
            for tail in self._of(instance):
                for fingerprint, profile in tail.profiles(fingerprints).items():
                    found.setdefault((instance, fingerprint), profile)
        return found

    def event_detail(self, event_id: str, instance: str = "") -> dict[str, Any] | None:
        for tail in self._tails:
            if instance and tail.config.instance_id != instance:
                continue
            item = tail.event(event_id)
            if item is not None:
                return item.detail()
        return None

    def status(self) -> list[dict[str, Any]]:
        return [tail.status() for tail in self._tails]
