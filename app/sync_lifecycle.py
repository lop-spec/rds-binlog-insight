"""Durable operator pauses and exclusive ownership of a collector data directory.

A maintenance deadline never enables autoSync. An indefinite operator pause must
be explicitly resumed. Kernel locks, unlike PID files, release after a crash.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import tempfile
import time
from pathlib import Path

LOGGER = logging.getLogger(__name__)


class CollectorLease:
    def __init__(self, root: Path):
        self.path = root / "collector.lock"
        self.handle = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        try:
            if os.name == "nt":
                import msvcrt
                if self.path.stat().st_size == 0:
                    handle.write(b"\0")
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            handle.close()
            LOGGER.error("COLLECTOR_OWNER_CONFLICT: another collector owns this data directory; refusing startup")
            raise RuntimeError("COLLECTOR_OWNER_CONFLICT: data directory already owned or lock unavailable") from exc
        self.handle = handle
        return self

    def __exit__(self, *_args):
        if self.handle is not None:
            if os.name == "nt":
                import msvcrt
                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            self.handle.close()
            self.handle = None
        # Do not unlink the lock: other processes may already reference its inode.


class PauseControl:
    def __init__(self, root: Path, instance_id: str):
        digest = hashlib.sha256(instance_id.encode()).hexdigest()
        self.path = root / "sync-control" / f"{digest}.json"
        self.state = {"mode": "none", "pausedAt": 0.0, "resumeAt": 0.0}
        try:
            value = json.loads(self.path.read_text(encoding="utf-8"))
            if (not isinstance(value, dict) or value.get("mode") not in {"none", "manual", "maintenance"}
                    or not isinstance(value.get("pausedAt"), (int, float))
                    or not isinstance(value.get("resumeAt"), (int, float))):
                raise ValueError("invalid pause state")
            if value["mode"] == "maintenance" and not 0 < value["pausedAt"] < value["resumeAt"] < float("inf"):
                raise ValueError("invalid maintenance deadline")
            self.state = value
        except FileNotFoundError:
            pass
        except (OSError, ValueError, UnicodeError):
            LOGGER.error("SYNC_CONTROL_INVALID: pause state unreadable; fail closed until explicit start")
            self.state = {"mode": "manual", "pausedAt": 0.0, "resumeAt": 0.0, "invalid": True}

    def _save(self, state: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(prefix=".pause-", suffix=".tmp", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                json.dump(state, stream, separators=(",", ":"), allow_nan=False)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(name, self.path)
            self.state = state
        finally:
            if os.path.exists(name):
                os.unlink(name)

    def pause(self, seconds: int | None = None) -> None:
        if seconds is not None and (isinstance(seconds, bool) or not isinstance(seconds, int) or not 1 <= seconds <= 86400):
            raise ValueError("resumeAfterSeconds must be an integer between 1 and 86400")
        now = time.time()
        self._save({"mode": "maintenance" if seconds is not None else "manual",
                    "pausedAt": now, "resumeAt": now + seconds if seconds is not None else 0.0})

    def clear(self) -> None:
        if self.state["mode"] != "none":
            self._save({"mode": "none", "pausedAt": 0.0, "resumeAt": 0.0})

    def expire(self) -> bool:
        if self.state["mode"] == "maintenance" and time.time() >= self.state["resumeAt"]:
            self.clear()
            return True
        return False


def sync_health(settings, running: bool, paused: bool, pause: dict, latest: dict | None) -> dict:
    """Separate collection readiness from process liveness; no full-table scans."""
    state, message = "waiting", "自动同步已启用，等待下一次轮询"
    ok = True
    if not settings.db_instance_id:
        ok, state, message = False, "unconfigured", "未配置采集实例"
    elif not settings.auto_sync:
        ok, state, message = False, "disabled", "自动同步已关闭；单次任务结束后不会继续采集"
    elif paused or pause.get("mode") != "none":
        ok = False
        if pause.get("mode") == "maintenance":
            state, message = "maintenance", "维护暂停；到期后按原自动同步设置恢复"
        else:
            state, message = "paused", "手动暂停未恢复；不会自动继续采集"
    elif running:
        state, message = "running", "采集任务运行中（不代表历史已补齐）"
    elif latest and latest.get("status") == "failed":
        ok, state, message = False, "failed", "上次采集失败，等待自动重试；请检查任务错误"
    progress = ""
    if latest:
        progress = max([str(latest.get("started_at") or ""), str(latest.get("finished_at") or "")]
                       + [str(event.get("created_at") or "") for event in latest.get("events", [])])
    age = None
    if progress:
        from datetime import datetime
        try:
            age = max(0, time.time() - datetime.fromisoformat(progress.replace("Z", "+00:00")).timestamp())
        except (ValueError, OverflowError):
            LOGGER.warning("SYNC_PROGRESS_INVALID: latest job has an invalid progress timestamp")
    if ok and age is not None and age > max(900, settings.poll_minutes * 120):
        ok, state, message = False, "stalled", "采集进度或调度已长时间未推进；服务存活不等于同步正常"
    return {"ok": ok, "state": state, "message": message,
            "lastProgressAt": progress, "noProgressSeconds": round(age, 1) if age is not None else None}
