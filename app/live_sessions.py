"""Sessions of one RDS node as they are right now, read from DAS ``GetMySQLAllSessionAsync`` (a MongoDB member: ``GetMongoDBCurrentOp``).

Slow-log records appear only after a statement finishes, so a query that is still holding the node cannot be seen in them.
This asks for the node's session list when a person clicks (or when the session retention loop is due), and keeps
only the last answer for a few seconds. Reads are rate-limited the way a person would not notice: one read per node at a
time, a short cache shared by every view of the snapshot (running statements, session management, retention).

Two sources give the same ``Data`` shape: the database itself (``direct_sessions``: one ``information_schema`` statement,
tens of milliseconds, used for the instances ``data/session-direct.json`` lists) and DAS (asynchronous, 2-3 s a node, two
reads at once at most). A direct read that cannot be trusted falls back to DAS for that node and is not tried again for
30 s (doubling up to 10 minutes while it keeps failing); ``source="das"`` asks for DAS on purpose.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

from .direct_sessions import DirectSessionError, DirectSessionReader

LOGGER = logging.getLogger(__name__)

ACTION = "GetMySQLAllSessionAsync"
MONGO_ACTION = "GetMongoDBCurrentOp"
POLL_SECONDS = 1.5
DEADLINE_SECONDS = 25.0
CACHE_SECONDS = 10.0
MAX_CONCURRENT_READS = 2
MAX_DIRECT_READS = 6
DIRECT_RETRY_SECONDS = 30.0
DIRECT_RETRY_MAX_SECONDS = 600.0
MAX_SESSIONS = 50
# Commands of sessions that are not running a statement: idle clients, replication and background threads.
IDLE_COMMANDS = frozenset({"sleep", "daemon", "connect", "binlog dump", "binlog dump gtid"})
_PERMISSION_WORDS = ("forbidden", "nopermission", "not authorized", "unauthorized", "accessdenied", "http 403")
_THROTTLE_WORDS = ("throttling", "flowcontrol", "flow control", "too many requests", "http 429")


class LiveSessionsError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def classify_failure(exc: BaseException) -> str:
    text = str(exc).lower()
    if any(word in text for word in _PERMISSION_WORDS):
        return "das_permission_denied"
    if any(word in text for word in _THROTTLE_WORDS):
        return "das_throttled"
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
            "sql": sql,
            "sql_chars": len(sql),
            "sql_truncated": False,
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
                 deadline_seconds: float = DEADLINE_SECONDS, direct: DirectSessionReader | None = None) -> None:
        self._client_factory = client_factory
        self._clock, self._sleep, self._wall_us = clock, sleep, wall_us
        self._cache_seconds, self._poll_seconds, self._deadline_seconds = cache_seconds, poll_seconds, deadline_seconds
        self._direct = direct
        self._cache: dict[tuple[str, str, str], tuple[float, dict[str, Any]]] = {}
        self._failures = {}
        self._direct_down: dict[tuple[str, str], tuple[float, str, int]] = {}
        self._locks: dict[tuple[str, str], threading.Lock] = {}
        self._guard = threading.Lock()
        self._gate = threading.BoundedSemaphore(MAX_CONCURRENT_READS)
        self._direct_gate = threading.BoundedSemaphore(MAX_DIRECT_READS)

    def direct_status(self) -> dict[str, Any]:
        """Whether sessions are read from the database, for which instances, and the nodes that fell back to DAS just now."""
        if self._direct is None:
            return {"enabled": False, "instances": [], "fallbacks": []}
        config = self._direct.config()
        now = self._clock()
        fallbacks = [{"instance_id": instance, "node_id": node, "code": code, "retry_in_seconds": round(until - now), "failures": streak}
                     for (instance, node), (until, code, streak) in sorted(self._direct_down.items()) if until > now]
        return {"enabled": bool(config["enabled"]), "instances": sorted(config["instances"]) if config["enabled"] else [], "fallbacks": fallbacks}

    def _node_lock(self, key: tuple[str, str]) -> threading.Lock:
        with self._guard:
            return self._locks.setdefault(key, threading.Lock())

    def snapshot(self, instance_id: str, node_id: str) -> dict[str, Any]:
        """The statements running on one node (the "看正在跑的查询" view)."""
        return self.view(instance_id, node_id, normalize_sessions)

    def view(self, instance_id: str, node_id: str, build: Callable[[dict[str, Any], int], dict[str, Any]], source: str = "auto") -> dict[str, Any]:
        """One node's snapshot shaped by ``build(data, now_us)``; the read and the cache are shared by every view.

        ``source`` is ``"auto"`` (the database when it is switched on for the instance, else DAS) or ``"das"``."""
        mode = "das" if source == "das" else "auto"
        key = (instance_id, node_id, mode)
        with self._node_lock((instance_id, node_id)):
            failed = self._failures.get(key)
            if failed and self._clock() < failed[0]:
                raise LiveSessionsError(failed[1], failed[2])
            hit = self._cache.get(key)
            cached = bool(hit and self._clock() < hit[0])
            if cached:
                data = hit[1]
            else:
                try:
                    data, ttl = self._fetch(instance_id, node_id, mode)
                except LiveSessionsError as exc:
                    self._failures[key] = (self._clock()+10, exc.code, str(exc))
                    raise
                self._failures.pop(key, None)
                self._cache[key] = (self._clock() + ttl, data)
        payload = build(data, self._wall_us())
        payload.update(instance_id=instance_id, node_id=node_id)
        payload["source"] = str(data.get("Source") or "das")
        if data.get("DirectFallback"):
            payload["fallback"] = data["DirectFallback"]
        return {**payload, "cached": cached}

    def _fetch(self, instance_id: str, node_id: str, mode: str) -> tuple[dict[str, Any], float]:
        """The node's ``Data`` and how long to keep it: from the database when that is switched on and trustworthy, else DAS."""
        fallback = ""
        if mode == "auto" and self._direct is not None and not instance_id.startswith("dds-") and self._direct.enabled_for(instance_id):
            down = self._direct_down.get((instance_id, node_id))
            if down and self._clock() < down[0]:
                fallback = down[1]
            else:
                try:
                    with self._direct_gate:
                        data = self._direct.read(instance_id, node_id)
                    self._direct_down.pop((instance_id, node_id), None)
                    return data, self._direct.cache_seconds()
                except DirectSessionError as exc:
                    streak = (down[2] if down else 0) + 1
                    delay = min(DIRECT_RETRY_SECONDS * 2 ** (streak - 1), DIRECT_RETRY_MAX_SECONDS)
                    self._direct_down[(instance_id, node_id)] = (self._clock() + delay, exc.code, streak)
                    LOGGER.warning("live_sessions direct read failed, reading DAS instead: instance=%s node=%s code=%s detail=%s retry_in=%.0fs",
                                   instance_id, node_id, exc.code, str(exc)[:300], delay)
                    fallback = exc.code
        with self._gate:
            data = self._read(instance_id, node_id)
        if fallback:
            data = {**data, "DirectFallback": fallback}
        return data, self._cache_seconds

    def _fail(self, instance_id: str, node_id: str, exc: BaseException) -> LiveSessionsError:
        code = classify_failure(exc)
        LOGGER.warning("live_sessions unavailable: instance=%s node=%s code=%s detail=%s",
                       instance_id, node_id, code, str(exc)[:300])
        return LiveSessionsError(code, str(exc)[:300])

    def _read(self, instance_id: str, node_id: str) -> dict[str, Any]:
        """The finished DAS answer (``Data``) of one node."""
        started = self._clock()
        try:
            client = self._client_factory(instance_id)
        except Exception as exc:  # noqa: BLE001 - e.g. no credential: reported, not swallowed
            raise self._fail(instance_id, node_id, exc) from exc
        if instance_id.startswith("dds-"):
            # MongoDB: one synchronous call per member. Only role ids from the node book reach here: DAS answers for the
            # primary when it does not know the NodeId, without saying so.
            try:
                response = client.call(MONGO_ACTION, {"InstanceId": instance_id, "NodeId": node_id})
            except Exception as exc:  # noqa: BLE001 - every failure is reported with its reason, never swallowed
                raise self._fail(instance_id, node_id, exc) from exc
            data = response.get("Data")
            if response.get("Success") is False or not isinstance(data, dict):
                LOGGER.warning("live_sessions unavailable: instance=%s node=%s code=das_failed message=%s",
                               instance_id, node_id, str(response.get("Message"))[:200])
                raise LiveSessionsError("das_failed", f"DAS 会话读取失败：{response.get('Message') or '没有返回数据'}")
            return data
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
                return data
            if self._clock() - started >= self._deadline_seconds:
                LOGGER.warning("live_sessions unavailable: instance=%s node=%s code=das_timeout waited=%.0fs",
                               instance_id, node_id, self._clock() - started)
                raise LiveSessionsError("das_timeout", "DAS 会话快照超时未完成")
            result_id = str(data.get("ResultId") or result_id)
            self._sleep(self._poll_seconds)
