"""Statements running right now on one RDS node, read on demand from DAS ``GetMySQLAllSessionAsync``.

Slow-log records appear only after a statement finishes, so a query that is still holding the node cannot be seen in them.
This asks DAS for the node's session list when a person clicks, keeps nothing, and never polls on its own. Reads are
rate-limited the way a person would not notice: one read per node at a time, a short cache, two nodes at once at most.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

LOGGER = logging.getLogger(__name__)

ACTION = "GetMySQLAllSessionAsync"
POLL_SECONDS = 1.5
DEADLINE_SECONDS = 25.0
CACHE_SECONDS = 10.0
MAX_CONCURRENT_READS = 2
MAX_SESSIONS = 50
SQL_LIMIT = 2000
# Commands of sessions that are not running a statement: idle clients, replication and background threads.
IDLE_COMMANDS = frozenset({"sleep", "daemon", "connect", "binlog dump", "binlog dump gtid"})
_PERMISSION_WORDS = ("forbidden", "nopermission", "not authorized", "unauthorized", "accessdenied", "http 403")


class LiveSessionsError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def classify_failure(exc: BaseException) -> str:
    text = str(exc).lower()
    if any(word in text for word in _PERMISSION_WORDS):
        return "das_permission_denied"
    if "timed out" in text or "timeout" in text or "网络错误" in text:
        return "das_unreachable"
    return "das_failed"


def _seconds(value: Any) -> int:
    try:
        return max(int(float(value)), 0)
    except (TypeError, ValueError):
        return 0


def normalize_sessions(data: dict[str, Any], now_us: int) -> dict[str, Any]:
    """The running statements of a finished DAS session snapshot, longest first, with the snapshot's own counters."""
    snapshot = data.get("SessionData") or {}
    listed = snapshot.get("SessionList") or []
    running = []
    for item in listed:
        command = str(item.get("Command") or "").strip()
        if command.lower() in IDLE_COMMANDS or not command:
            continue
        sql = str(item.get("SqlText") or "")
        running.append({
            "session_id": item.get("SessionId"),
            "user": str(item.get("User") or ""),
            "client": str(item.get("Client") or ""),
            "database": str(item.get("DbName") or ""),
            "command": command,
            "state": str(item.get("State") or ""),
            "seconds": _seconds(item.get("Time")),
            "sql": sql[:SQL_LIMIT],
            "sql_chars": len(sql),
        })
    running.sort(key=lambda row: (-row["seconds"], str(row["session_id"])))
    observed_ms = snapshot.get("TimeStamp") or data.get("Timestamp")
    try:
        observed_us = int(observed_ms) * 1000
    except (TypeError, ValueError):
        observed_us = now_us
    return {
        "status": "ok",
        "observed_at_us": observed_us,
        "total_sessions": int(snapshot.get("TotalSessionCount") or len(listed)),
        "running_sessions": len(running),
        "longest_running_seconds": running[0]["seconds"] if running else 0,
        "sessions": running[:MAX_SESSIONS],
        "truncated": len(running) > MAX_SESSIONS,
    }


class LiveSessions:
    def __init__(self, client_factory: Callable[[str], Any], *, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep, wall_us: Callable[[], int] = lambda: time.time_ns() // 1000,
                 cache_seconds: float = CACHE_SECONDS, poll_seconds: float = POLL_SECONDS,
                 deadline_seconds: float = DEADLINE_SECONDS) -> None:
        self._client_factory = client_factory
        self._clock, self._sleep, self._wall_us = clock, sleep, wall_us
        self._cache_seconds, self._poll_seconds, self._deadline_seconds = cache_seconds, poll_seconds, deadline_seconds
        self._cache: dict[tuple[str, str], tuple[float, dict[str, Any]]] = {}
        self._locks: dict[tuple[str, str], threading.Lock] = {}
        self._guard = threading.Lock()
        self._gate = threading.BoundedSemaphore(MAX_CONCURRENT_READS)

    def _node_lock(self, key: tuple[str, str]) -> threading.Lock:
        with self._guard:
            return self._locks.setdefault(key, threading.Lock())

    def snapshot(self, instance_id: str, node_id: str) -> dict[str, Any]:
        key = (instance_id, node_id)
        with self._node_lock(key):
            hit = self._cache.get(key)
            if hit and self._clock() < hit[0]:
                return {**hit[1], "cached": True}
            with self._gate:
                payload = self._read(instance_id, node_id)
            self._cache[key] = (self._clock() + self._cache_seconds, payload)
        return {**payload, "cached": False}

    def _fail(self, instance_id: str, node_id: str, exc: BaseException) -> LiveSessionsError:
        code = classify_failure(exc)
        LOGGER.warning("live_sessions unavailable: instance=%s node=%s code=%s detail=%s",
                       instance_id, node_id, code, str(exc)[:300])
        return LiveSessionsError(code, str(exc)[:300])

    def _read(self, instance_id: str, node_id: str) -> dict[str, Any]:
        started = self._clock()
        try:
            client = self._client_factory(instance_id)
        except Exception as exc:  # noqa: BLE001 - e.g. no credential: reported, not swallowed
            raise self._fail(instance_id, node_id, exc) from exc
        result_id = ""
        while True:
            params = {"InstanceId": instance_id, "NodeId": node_id}
            if result_id:
                params["ResultId"] = result_id
            try:
                response = client.call(ACTION, params)
            except Exception as exc:  # noqa: BLE001 - every failure is reported with its reason, never swallowed
                raise self._fail(instance_id, node_id, exc) from exc
            data = response.get("Data") or {}
            if data.get("Fail"):
                LOGGER.warning("live_sessions unavailable: instance=%s node=%s code=das_failed state=%s",
                               instance_id, node_id, data.get("State"))
                raise LiveSessionsError("das_failed", f"DAS 会话快照失败：{data.get('State') or '未知状态'}")
            if data.get("IsFinish") or data.get("Complete"):
                payload = normalize_sessions(data, self._wall_us())
                payload.update(instance_id=instance_id, node_id=node_id)
                return payload
            if self._clock() - started >= self._deadline_seconds:
                LOGGER.warning("live_sessions unavailable: instance=%s node=%s code=das_timeout waited=%.0fs",
                               instance_id, node_id, self._clock() - started)
                raise LiveSessionsError("das_timeout", "DAS 会话快照超时未完成")
            result_id = str(data.get("ResultId") or result_id)
            self._sleep(self._poll_seconds)
