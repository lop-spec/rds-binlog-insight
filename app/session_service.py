"""What the session management endpoints do, kept out of the HTTP handler so it can be tested without a server.

Only nodes whose slow log this service collects can be asked about (the same rule as the running-statements view): the
service must never spend its cloud identity on an arbitrary instance a caller names.
"""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

from .live_sessions import LiveSessions, LiveSessionsError
from .session_collector import SessionCollector
from .session_store import SessionStore
from .session_view import build_kill_statements, build_session_view, combine_kill_statements

LOGGER = logging.getLogger(__name__)

MAX_HISTORY_DAYS = 31
MAX_PARALLEL_NODES = 3
ROLE_ORDER = {"primary": 0, "secondary": 1}


class SessionRequestError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class SessionService:
    def __init__(self, live: LiveSessions, store: SessionStore, collector: SessionCollector, *,
                 nodes: Callable[[], list[tuple[str, str]]], roles: Callable[[str], dict[str, dict[str, str]]],
                 wall_us: Callable[[], int] = lambda: time.time_ns() // 1000) -> None:
        self.live, self.store, self.collector = live, store, collector
        self._nodes, self._roles, self._wall_us = nodes, roles, wall_us

    # -- which nodes may be asked ---------------------------------------------
    def _known(self, instance: str) -> list[str]:
        return sorted({node for inst, node in self._nodes() if inst == instance and node})

    def _role(self, instance: str, node: str) -> dict[str, str]:
        try:
            known = self._roles(instance).get(node) or {}
        except Exception:  # noqa: BLE001 - a label, never a reason to fail
            LOGGER.warning("session management: node roles unavailable for %s", instance, exc_info=True)
            known = {}
        return {"role": known.get("role", ""), "zone": known.get("zone", "")}

    def _resolve(self, instance: str, node_id: str) -> list[str]:
        known = self._known(instance)
        if not known:
            raise SessionRequestError("SESSIONS_UNKNOWN_INSTANCE", "只能查已登记采集慢日志的实例")
        if node_id:
            if node_id not in known:
                raise SessionRequestError("SESSIONS_UNKNOWN_NODE", "只能查已登记采集慢日志的实例节点")
            return [node_id]
        return sorted(known, key=lambda node: (ROLE_ORDER.get(self._role(instance, node)["role"], 2), node))

    # -- the page -------------------------------------------------------------
    def overview(self, instance: str, node_id: str, thresholds: dict[str, int]) -> dict[str, Any]:
        """Every wanted node's session view, read in parallel (the reader still lets only two reads run at once)."""
        nodes = self._resolve(instance, node_id)
        build = lambda data, now_us: build_session_view(data, now_us, thresholds)  # noqa: E731

        def one(node: str) -> dict[str, Any]:
            label = {"node_id": node, **self._role(instance, node)}
            try:
                return {**self.live.view(instance, node, build), **label}
            except LiveSessionsError as exc:
                return {**label, "status": "error", "code": exc.code.upper(), "message": str(exc)}

        with ThreadPoolExecutor(max_workers=min(MAX_PARALLEL_NODES, len(nodes))) as pool:
            results = list(pool.map(one, nodes))
        return {"instance_id": instance, "thresholds": dict(thresholds), "nodes": results,
                "partial": any(item["status"] != "ok" for item in results), "generated_us": self._wall_us()}

    def history(self, instance: str, node_id: str, start_us: int, end_us: int, limit: int) -> dict[str, Any]:
        self._resolve(instance, node_id)
        if not 0 < end_us - start_us <= MAX_HISTORY_DAYS * 86400 * 1_000_000:
            raise SessionRequestError("SESSIONS_INVALID_WINDOW", f"时间窗必须大于 0 且不超过 {MAX_HISTORY_DAYS} 天")
        result = self.store.history(instance, node_id, start_us, end_us, limit)
        labels = {node: self._role(instance, node) for node in self._known(instance)}
        result.update(instance_id=instance, start_us=start_us, end_us=end_us,
                      nodes=[{"node_id": node, **label} for node, label in sorted(labels.items())], retention=self.collector.status())
        return result

    def status(self) -> dict[str, Any]:
        return self.collector.status()

    # -- ending sessions is a text -------------------------------------------
    def kill_statements(self, payload: dict[str, Any]) -> dict[str, Any]:
        """One text for every node in ``payload["nodes"]`` (``{node_id, snapshot_us, lock_waiters, sessions}`` each); a payload
        with ``node_id`` and ``sessions`` at the top is the one-node form of the same request."""
        instance, mode = str(payload.get("instance") or "").strip(), str(payload.get("mode") or "connection")
        nodes = payload.get("nodes")
        if nodes is None:
            nodes = [{key: payload[key] for key in ("node_id", "snapshot_us", "lock_waiters", "sessions") if key in payload}]
        if not isinstance(nodes, list) or not nodes or not all(isinstance(item, dict) for item in nodes):
            raise ValueError("nodes 必须是非空的节点数组")
        known, generated_us = self._known(instance), self._wall_us()
        parts: list[dict[str, Any]] = []
        for item in nodes:
            node = str(item.get("node_id") or "").strip()
            if not node or node not in known:
                raise SessionRequestError("SESSIONS_UNKNOWN_NODE", "只能为已登记采集慢日志的实例节点生成")
            if any(part["node_id"] == node for part in parts):
                raise ValueError("同一个节点只能出现一次")
            sessions = item.get("sessions")
            if not isinstance(sessions, list) or not all(isinstance(row, dict) for row in sessions):
                raise ValueError("sessions 必须是会话对象数组")
            snapshot_us = item.get("snapshot_us")
            if isinstance(snapshot_us, bool) or not isinstance(snapshot_us, int) or snapshot_us <= 0:
                raise ValueError("snapshot_us 必须是快照时间（微秒）")
            parts.append(build_kill_statements({"node_id": node, **self._role(instance, node)}, sessions, mode=mode, snapshot_us=snapshot_us,
                                               generated_us=generated_us, lock_waiters=item.get("lock_waiters", 0)))
        parts.sort(key=lambda part: (ROLE_ORDER.get(part["role"], 2), part["node_id"]))
        result = combine_kill_statements(parts, mode=mode, generated_us=generated_us)
        LOGGER.info("session management: kill statements written instance=%s nodes=%s mode=%s sessions=%d skipped=%d (nothing was executed)",
                    instance, ",".join(part["node_id"] for part in parts), mode, len(result["statements"]), len(result["skipped"]))
        return result
