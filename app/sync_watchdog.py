"""Watchdog of the binlog collectors in the API service: a stalled job is stopped, a stuck process restarted.

2026-10-28 16:08 to 10-29 02:11 (UTC) a sync job stopped making progress for ten hours while the service kept
answering its health probe; nothing restarted it and the source keeps binlogs only seven days. ``SYNC_HEALTH`` only
logged "stalled". Now:

1. a running job without progress (no listing page, downloaded bytes, scan, upload part or file step) for
   ``RDS_BINLOG_SYNC_STALL_SECONDS`` (30 min) is asked to stop at its next file boundary; the scheduler starts the
   next job at once;
2. if it has not stopped ``RDS_BINLOG_SYNC_EXIT_GRACE_SECONDS`` (10 min) later, a thread is stuck where Python
   cannot interrupt it: every thread's stack is written to ``logs/sync-stall-*.txt`` and the process exits, so the
   service supervisor starts a fresh one (downloads resume from their partial files; commits are atomic);
3. a scheduler loop that has not ticked for ``RDS_BINLOG_SCHEDULER_STALL_SECONDS`` (2 h) while auto sync is on
   does the same.

Waiting is progress: a job that waits for disk space or retries a file keeps touching its heartbeat.
"""
from __future__ import annotations

import logging
import os
import sys
import threading
import traceback
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

LOGGER = logging.getLogger(__name__)


def _env_seconds(name: str, default: float) -> float:
    try:
        return max(float(os.environ.get(name, default)), 60.0)
    except ValueError:
        return default


STALL_SECONDS = _env_seconds("RDS_BINLOG_SYNC_STALL_SECONDS", 1800)
EXIT_GRACE_SECONDS = _env_seconds("RDS_BINLOG_SYNC_EXIT_GRACE_SECONDS", 600)
SCHEDULER_STALL_SECONDS = _env_seconds("RDS_BINLOG_SCHEDULER_STALL_SECONDS", 7200)
CHECK_SECONDS = 30
DUMPS_KEPT = 10


def thread_stacks() -> str:
    names = {thread.ident: thread.name for thread in threading.enumerate()}
    lines: list[str] = []
    for ident, frame in sys._current_frames().items():
        lines.append(f"--- thread {names.get(ident, '?')} ({ident}) ---\n")
        lines.extend(traceback.format_stack(frame))
    return "".join(lines)


class SyncWatchdog(threading.Thread):
    def __init__(
        self,
        managers: Callable[[], list[Any]],
        dump_dir: Path,
        *,
        stall_seconds: float = STALL_SECONDS,
        exit_grace_seconds: float = EXIT_GRACE_SECONDS,
        scheduler_stall_seconds: float = SCHEDULER_STALL_SECONDS,
        check_seconds: float = CHECK_SECONDS,
        exit_fn: Callable[[int], Any] = os._exit,
    ) -> None:
        super().__init__(name="sync-watchdog", daemon=True)
        self.managers = managers
        self.dump_dir = Path(dump_dir)
        self.stall_seconds = stall_seconds
        self.exit_grace_seconds = exit_grace_seconds
        self.scheduler_stall_seconds = scheduler_stall_seconds
        self.check_seconds = check_seconds
        self.exit_fn = exit_fn
        self._stop = threading.Event()

    def stop(self) -> None:
        self._stop.set()

    def run(self) -> None:
        while not self._stop.wait(self.check_seconds):
            try:
                self.check()
            except Exception:  # noqa: BLE001 - the watchdog itself must keep running
                LOGGER.exception("Sync watchdog check failed")

    def check(self) -> str:
        """One round over the collectors; returns the action taken ('' when none)."""
        for manager in self.managers():
            state = manager.watchdog_state()
            instance = state.get("instance") or "?"
            if state["running"] and state["progressAge"] >= self.stall_seconds:
                if not state["abortRequested"]:
                    reason = f"{state['progressAge'] / 60:.0f} 分钟无进展"
                    self.dump(f"collector {instance} stalled: {reason}")
                    manager.request_abort(reason)
                    return "abort"
                if state["abortAge"] >= self.exit_grace_seconds:
                    self.dump(f"collector {instance} did not stop {state['abortAge']:.0f}s after the abort request")
                    LOGGER.critical("SYNC_WATCHDOG_EXIT instance=%s reason=stuck-after-abort", instance)
                    self.exit_fn(75)
                    return "exit"
            if state["autoSync"] and state["schedulerAge"] >= self.scheduler_stall_seconds:
                self.dump(f"scheduler of {instance} has not ticked for {state['schedulerAge']:.0f}s")
                LOGGER.critical("SYNC_WATCHDOG_EXIT instance=%s reason=scheduler-stalled", instance)
                self.exit_fn(76)
                return "exit"
        return ""

    def dump(self, reason: str) -> Path | None:
        stacks = thread_stacks()
        LOGGER.error("SYNC_WATCHDOG %s\n%s", reason, stacks)
        try:
            self.dump_dir.mkdir(parents=True, exist_ok=True)
            path = self.dump_dir / f"sync-stall-{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.txt"
            path.write_text(f"{reason}\n\n{stacks}", encoding="utf-8")
            for old in sorted(self.dump_dir.glob("sync-stall-*.txt"))[:-DUMPS_KEPT]:
                old.unlink(missing_ok=True)
            return path
        except OSError:
            LOGGER.warning("Sync watchdog could not write its stack dump", exc_info=True)
            return None
