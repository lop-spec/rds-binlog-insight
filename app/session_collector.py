"""The retention loop of session management: one DAS session read per node and minute, abnormal sessions kept for a week.

DAS shows the sessions of this moment only, so a transaction that held a lock at 11:30 is gone by noon. This loop reads each
node's session list in turn (one node every ``interval / nodes`` seconds, so a node is read once per ``interval``) through
the same ``LiveSessions`` reader the page uses, and stores the abnormal sessions with ``SessionStore``.

Nothing here is a second implementation of reading DAS: it shares the reader's cache, concurrency limit and error codes. It
never runs against a node the service does not collect slow logs for, backs off when DAS refuses (a throttled or denied node
is retried after 2x, 4x ... up to 10 minutes), says why in the log on the first failure of a streak, and is switched off by
``data/session-retention.json`` (``{"enabled": false}``) without a restart. Without that file it is on for the primary
instance and the configured MongoDB instances (each member, read with ``GetMongoDBCurrentOp``), every 60 seconds, for 7 days.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from pathlib import Path
from typing import Any, Callable

from .live_sessions import LiveSessions, LiveSessionsError
from .session_store import SessionStore
from .mongo_sessions import build_mongo_session_view, is_mongo
from .session_view import build_session_view

LOGGER = logging.getLogger(__name__)

CONFIG_NAME = "session-retention.json"
DEFAULT_CONFIG = {"enabled": True, "intervalSeconds": 60, "retentionDays": 7, "instances": []}
MIN_NODE_GAP_SECONDS = 5.0
IDLE_WAIT_SECONDS = 30.0
MAX_BACKOFF_SECONDS = 600
PURGE_EVERY_SECONDS = 3600


def load_config(data_dir: Path) -> dict[str, Any]:
    """The retention settings: defaults without the file, defaults plus a warning for a file that cannot be used."""
    path = Path(data_dir) / CONFIG_NAME
    if not path.exists():
        return dict(DEFAULT_CONFIG)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("要求 JSON 对象")
        config = {**DEFAULT_CONFIG, **{key: raw[key] for key in DEFAULT_CONFIG if key in raw}}
        if not isinstance(config["enabled"], bool):
            raise ValueError("enabled 必须是 true/false")
        if not (isinstance(config["intervalSeconds"], int) and 30 <= config["intervalSeconds"] <= 3600):
            raise ValueError("intervalSeconds 必须是 30 到 3600 的整数")
        if not (isinstance(config["retentionDays"], int) and 1 <= config["retentionDays"] <= 60):
            raise ValueError("retentionDays 必须是 1 到 60 的整数")
        if not (isinstance(config["instances"], list) and all(isinstance(item, str) for item in config["instances"])):
            raise ValueError("instances 必须是实例 ID 的字符串数组")
        return config
    except (OSError, ValueError) as exc:
        LOGGER.warning("会话留存配置 %s 无法使用，按默认值运行：%s", path, exc)
        return dict(DEFAULT_CONFIG)


class SessionCollector:
    def __init__(self, live: LiveSessions, store: SessionStore, *, config_loader: Callable[[], dict[str, Any]],
                 nodes: Callable[[], list[tuple[str, str]]], default_instances: Callable[[], list[str]],
                 wall_us: Callable[[], int] = lambda: time.time_ns() // 1000) -> None:
        self.live, self.store = live, store
        self._config_loader, self._nodes, self._default_instances, self._wall_us = config_loader, nodes, default_instances, wall_us
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._state: dict[tuple[str, str], dict[str, Any]] = {}
        self._lock = threading.Lock()
        self._last_purge_us = 0
        self._last_config: dict[str, Any] = dict(DEFAULT_CONFIG)

    # -- what to read ---------------------------------------------------------
    def targets(self, config: dict[str, Any]) -> list[tuple[str, str]]:
        wanted = set(config["instances"]) or {item for item in self._default_instances() if item}
        return sorted({pair for pair in self._nodes() if pair[0] in wanted and pair[1]})

    # -- one read -------------------------------------------------------------
    def read_one(self, instance: str, node: str, config: dict[str, Any]) -> bool:
        """Read and store one node; False when it was skipped (backing off) or failed."""
        key = (instance, node)
        now = self._wall_us()
        with self._lock:
            state = self._state.setdefault(key, {"failures": 0, "next_us": 0, "last_ok_us": None, "last_error": None, "reads": 0})
            if now < state["next_us"]:
                return False
        try:
            build = build_mongo_session_view if is_mongo(instance) else build_session_view
            view = self.live.view(instance, node, lambda data, now_us: build(data, now_us))
            written = self.store.record(instance, node, view)
        except LiveSessionsError as exc:
            self._failed(key, state, exc.code, str(exc), config)
            return False
        except Exception as exc:  # noqa: BLE001 - one bad node or row must not stop the loop
            LOGGER.exception("session retention: unexpected failure instance=%s node=%s", instance, node)
            self._failed(key, state, "unexpected", f"{type(exc).__name__}: {exc}", config)
            return False
        with self._lock:
            if state["failures"]:
                LOGGER.info("session retention recovered: instance=%s node=%s after %d failed reads", instance, node, state["failures"])
            state.update(failures=0, next_us=0, last_ok_us=view["observed_at_us"], last_error=None, reads=state["reads"] + 1,
                         abnormal=view["overview"]["abnormal"], written=written)
        return True

    def _failed(self, key: tuple[str, str], state: dict[str, Any], code: str, message: str, config: dict[str, Any]) -> None:
        with self._lock:
            first = state["failures"] == 0
            state["failures"] += 1
            delay = min(config["intervalSeconds"] * 2 ** min(state["failures"], 6), MAX_BACKOFF_SECONDS)
            state.update(next_us=self._wall_us() + int(delay * 1_000_000), last_error={"code": code, "message": message[:300], "at_us": self._wall_us()})
        if first:
            LOGGER.warning("session retention: read failed instance=%s node=%s code=%s detail=%s; retrying in %ds with backoff",
                           key[0], key[1], code, message[:200], delay)

    # -- the loop -------------------------------------------------------------
    def run_pass(self, config: dict[str, Any], targets: list[tuple[str, str]]) -> int:
        """Read every target once, without the waiting between nodes (the loop adds it); the number read."""
        return sum(self.read_one(instance, node, config) for instance, node in targets)

    def purge_if_due(self, config: dict[str, Any]) -> None:
        now = self._wall_us()
        if now - self._last_purge_us < PURGE_EVERY_SECONDS * 1_000_000:
            return
        self._last_purge_us = now
        try:
            removed = self.store.purge(now - config["retentionDays"] * 86400 * 1_000_000)
            if removed:
                LOGGER.info("session retention: purged %d rows older than %d days", removed, config["retentionDays"])
        except Exception:  # noqa: BLE001 - a failed purge is retried next hour, never fatal
            LOGGER.exception("session retention: purge failed")

    def _loop(self) -> None:
        while not self._stop.is_set():
            config = self._config_loader()
            self._last_config = config
            targets = self.targets(config) if config["enabled"] else []
            if not targets:
                self._stop.wait(IDLE_WAIT_SECONDS)
                continue
            pause = max(config["intervalSeconds"] / len(targets), MIN_NODE_GAP_SECONDS)
            for instance, node in targets:
                if self._stop.is_set():
                    break
                self.read_one(instance, node, config)
                if self._stop.wait(pause):
                    break
            self.purge_if_due(config)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._loop, name="session-retention", daemon=True)
        self._thread.start()

    def shutdown(self) -> None:
        self._stop.set()

    # -- what the page shows about it ----------------------------------------
    def status(self) -> dict[str, Any]:
        config = self._last_config
        with self._lock:
            nodes = [{"instance_id": instance, "node_id": node, "last_ok_us": state["last_ok_us"], "last_error": state["last_error"],
                      "consecutive_failures": state["failures"], "reads": state["reads"], "abnormal_now": state.get("abnormal")}
                     for (instance, node), state in sorted(self._state.items())]
        return {"enabled": bool(config["enabled"]), "interval_seconds": config["intervalSeconds"], "retention_days": config["retentionDays"],
                "running": self._thread is not None and not self._stop.is_set(), "nodes": nodes, "store": self.store.stats()}
