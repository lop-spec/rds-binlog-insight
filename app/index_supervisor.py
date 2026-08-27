from __future__ import annotations

import argparse
import os
import signal
import sqlite3
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any

from .clickhouse_slowlog import SourceIndexLagPaused, SourceIndexPriorityGate
from .config import data_root, ensure_data_dirs
from .maintenance_status import (
    SLOWLOG_WORKER_STATUS_NAME,
    SUPERVISOR_STATUS_NAME,
    WORKER_PROGRESS_NAME,
    read_json_status,
    write_json_status,
)


def _positive_env_int(name: str, default: int, minimum: int) -> int:
    try:
        return max(int(os.environ.get(name, default)), minimum)
    except ValueError:
        return default


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return bool(default)
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _hidden_process_options() -> tuple[int, subprocess.STARTUPINFO | None]:
    creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    startupinfo = None
    if os.name == "nt":
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = 0
    return creation_flags, startupinfo


class IndexSupervisor:
    def __init__(
        self,
        data_dir: Path,
        *,
        no_progress_seconds: int,
        idle_seconds: int,
        source_priority_enabled: bool = False,
    ) -> None:
        self.data_dir = data_dir
        paths = ensure_data_dirs(data_dir)
        self.progress_path = paths["index"] / WORKER_PROGRESS_NAME
        self.status_path = paths["index"] / SUPERVISOR_STATUS_NAME
        self.source_status_path = (
            paths["index"] / SLOWLOG_WORKER_STATUS_NAME
        )
        self.source_priority_gate = (
            SourceIndexPriorityGate(self.source_status_path)
            if source_priority_enabled
            else None
        )
        self.source_priority_message = ""
        self.index_database = paths["index"] / "search.sqlite3"
        self.no_progress_seconds = max(int(no_progress_seconds), 30)
        self.idle_seconds = max(int(idle_seconds), 1)
        self.stopping = False
        self.child: subprocess.Popen[bytes] | None = None
        self.watchdog_restarts = 0
        self.worker_starts = 0
        self.last_error = ""
        previous = read_json_status(self.status_path)
        previous_index = previous.get("index")
        previous_exact = previous.get("exact")
        previous_catalog = previous.get("catalog")
        previous_analytics = previous.get("analytics")
        previous_slowlog = previous.get("slowlog")
        self.index_snapshot: dict[str, Any] = (
            {
                key: previous_index[key]
                for key in (
                    "schema_version",
                    "part_count",
                    "structural_part_count",
                    "block_count",
                    "row_count",
                    "size_bytes",
                    "last_indexed_at",
                    "last_structural_indexed_at",
                    "localBodyBytes",
                    "localBodyParts",
                )
                if key in previous_index
            }
            if isinstance(previous_index, dict)
            else {}
        )
        self.catalog_snapshot: dict[str, Any] = (
            {
                key: previous_catalog[key]
                for key in ("catalogedParts", "totalParts")
                if key in previous_catalog
            }
            if isinstance(previous_catalog, dict)
            else {}
        )
        self.exact_snapshot: dict[str, Any] = (
            dict(previous_exact)
            if isinstance(previous_exact, dict)
            else {}
        )
        self.analytics_snapshot: dict[str, Any] = (
            dict(previous_analytics)
            if isinstance(previous_analytics, dict)
            else {}
        )
        self.slowlog_snapshot: dict[str, Any] = (
            dict(previous_slowlog)
            if isinstance(previous_slowlog, dict)
            else {}
        )

    def request_stop(self, *_args: object) -> None:
        self.stopping = True
        self._stop_child()

    def _stop_child(self) -> None:
        child = self.child
        if child is None or child.poll() is not None:
            return
        child.terminate()
        try:
            child.wait(timeout=15)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait(timeout=10)

    def _publish(
        self,
        generation: str,
        progress: dict[str, Any],
        *,
        child_exit_code: int | None = None,
    ) -> None:
        phase = str(progress.get("phase") or "")
        state = str(progress.get("state") or "starting")
        running = state in {"starting", "running"}
        current_part = str(progress.get("currentPart") or "")
        worker_error = str(progress.get("lastError") or "")
        result = progress.get("result")
        if isinstance(result, dict):
            index_stats = result.get("indexStats")
            exact_stats = result.get("exactIndexStats")
            analytics_stats = result.get("analyticsStats")
            slowlog_stats = result.get("slowlogIndexStats")
            catalog_stats = result.get("catalogStats")
            local_body_stats = result.get("localBodyStats")
            if isinstance(index_stats, dict):
                self.index_snapshot.update(index_stats)
            if isinstance(exact_stats, dict):
                self.exact_snapshot.update(exact_stats)
            if isinstance(analytics_stats, dict):
                self.analytics_snapshot.update(analytics_stats)
            if isinstance(slowlog_stats, dict):
                self.slowlog_snapshot.update(slowlog_stats)
            if isinstance(catalog_stats, dict):
                self.catalog_snapshot.update(
                    {
                        "catalogedParts": int(
                            catalog_stats.get("cataloged_parts") or 0
                        ),
                        "totalParts": int(catalog_stats.get("total_parts") or 0),
                    }
                )
            if isinstance(local_body_stats, dict):
                self.index_snapshot.update(
                    {
                        "localBodyBytes": int(local_body_stats.get("bytes") or 0),
                        "localBodyParts": int(
                            local_body_stats.get("part_count") or 0
                        ),
                    }
                )
        write_json_status(
            self.status_path,
            {
                "supervisor": {
                    "running": not self.stopping,
                    "pid": os.getpid(),
                    "workerPid": self.child.pid if self.child else 0,
                    "workerStarts": self.worker_starts,
                    "watchdogRestarts": self.watchdog_restarts,
                    "noProgressSeconds": self.no_progress_seconds,
                    "lastError": (
                        self.source_priority_message
                        or self.last_error
                        or worker_error
                    ),
                    "childExitCode": child_exit_code,
                    "generation": generation,
                },
                "index": {
                    **self.index_snapshot,
                    "external": True,
                    "running": running and phase in {"index", "structure", "exact"},
                    "phase": phase,
                    "currentPart": (
                        current_part
                        if phase in {"index", "structure", "exact"}
                        else ""
                    ),
                    "lastError": (
                        worker_error
                        if phase in {"index", "structure", "exact"}
                        else ""
                    ),
                    "state": state,
                },
                "exact": {
                    **self.exact_snapshot,
                    "external": True,
                    "running": running and phase == "exact",
                    "currentPart": current_part if phase == "exact" else "",
                    "lastError": worker_error if phase == "exact" else "",
                    "state": state,
                },
                "analytics": {
                    **self.analytics_snapshot,
                    "external": True,
                    "running": running and phase in {"analytics", "rollup"},
                    "currentPart": (
                        current_part
                        if phase in {"analytics", "rollup"}
                        else ""
                    ),
                    "lastError": (
                        worker_error
                        if phase in {"analytics", "rollup"}
                        else ""
                    ),
                    "state": state,
                },
                "slowlog": {
                    **self.slowlog_snapshot,
                    "external": True,
                    "running": running and phase == "slowlog",
                    "currentPart": current_part if phase == "slowlog" else "",
                    "lastError": worker_error if phase == "slowlog" else "",
                    "state": state,
                },
                "catalog": {
                    **self.catalog_snapshot,
                    "external": True,
                    "running": running and phase == "catalog",
                    "currentParts": [current_part]
                    if phase == "catalog" and current_part
                    else [],
                    "lastError": worker_error if phase == "catalog" else "",
                    "state": state,
                },
                "worker": progress,
            },
        )

    def _quarantine_stalled_part(self, progress: dict[str, Any]) -> None:
        path = str(progress.get("currentPath") or "")
        sha256 = str(progress.get("partSha256") or "")
        if not path or not sha256 or not self.index_database.is_file():
            return
        try:
            with sqlite3.connect(self.index_database, timeout=10) as conn:
                conn.execute("PRAGMA busy_timeout=10000")
                conn.execute(
                    """
                    INSERT INTO index_failures(
                        path, sha256, error, retry_after_epoch, failed_at
                    ) VALUES(?, ?, ?, ?, datetime('now'))
                    ON CONFLICT(path) DO UPDATE SET
                        sha256 = excluded.sha256,
                        error = excluded.error,
                        retry_after_epoch = excluded.retry_after_epoch,
                        failed_at = excluded.failed_at
                    """,
                    (
                        path,
                        sha256,
                        self.last_error,
                        time.time() + 3600,
                    ),
                )
        except sqlite3.Error as exc:
            self.last_error += f"；记录一小时退避失败：{exc}"

    def _sleep_interruptibly(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while not self.stopping and time.monotonic() < deadline:
            time.sleep(min(0.5, deadline - time.monotonic()))

    def _source_priority_error(self) -> str:
        gate = self.source_priority_gate
        if gate is None:
            self.source_priority_message = ""
            return ""
        try:
            gate.check()
        except SourceIndexLagPaused as exc:
            self.source_priority_message = str(exc)
            return self.source_priority_message
        self.source_priority_message = ""
        return ""

    def run(self) -> int:
        while not self.stopping:
            priority_error = self._source_priority_error()
            if priority_error:
                self._publish(
                    "",
                    {
                        "generation": "",
                        "state": "paused",
                        "phase": "source-priority",
                        "currentPart": "",
                        "progressToken": "source-priority-paused",
                        "lastError": priority_error,
                    },
                )
                self._sleep_interruptibly(min(self.idle_seconds, 5))
                continue
            generation = uuid.uuid4().hex
            initial = {
                "generation": generation,
                "state": "starting",
                "phase": "",
                "currentPart": "",
                "progressToken": f"{generation}:spawn",
                "lastError": "",
            }
            write_json_status(self.progress_path, initial)
            creation_flags, startupinfo = _hidden_process_options()
            self.child = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "app.index_worker",
                    "--data-dir",
                    str(self.data_dir),
                    "--generation",
                    generation,
                ],
                stdin=subprocess.DEVNULL,
                creationflags=creation_flags,
                startupinfo=startupinfo,
            )
            self.worker_starts += 1
            source_priority_preempted = False
            last_token = str(initial["progressToken"])
            last_progress = time.monotonic()
            self._publish(generation, initial)
            while not self.stopping:
                progress = read_json_status(self.progress_path)
                if str(progress.get("generation") or "") != generation:
                    progress = initial
                token = str(progress.get("progressToken") or "")
                if token and token != last_token:
                    last_token = token
                    last_progress = time.monotonic()
                priority_error = self._source_priority_error()
                if priority_error:
                    self._stop_child()
                    source_priority_preempted = True
                    progress = {
                        **progress,
                        "state": "paused",
                        "phase": "source-priority",
                        "currentPart": "",
                        "lastError": priority_error,
                    }
                    self._publish(
                        generation,
                        progress,
                        child_exit_code=self.child.poll(),
                    )
                    break
                exit_code = self.child.poll()
                self._publish(
                    generation,
                    progress,
                    child_exit_code=exit_code,
                )
                if exit_code is not None:
                    if exit_code not in {0, 20}:
                        self.last_error = (
                            str(progress.get("lastError") or "")
                            or f"索引工作进程退出码 {exit_code}"
                        )
                    break
                if time.monotonic() - last_progress >= self.no_progress_seconds:
                    self.watchdog_restarts += 1
                    self.last_error = (
                        f"索引 {self.no_progress_seconds} 秒无进度，已强制重启工作进程"
                    )
                    self._stop_child()
                    self._quarantine_stalled_part(progress)
                    progress = {
                        **progress,
                        "state": "watchdog-restart",
                        "lastError": self.last_error,
                    }
                    self._publish(
                        generation,
                        progress,
                        child_exit_code=self.child.poll(),
                    )
                    break
                time.sleep(1)
            exit_code = self.child.poll()
            self.child = None
            if self.stopping:
                break
            if source_priority_preempted:
                self._sleep_interruptibly(min(self.idle_seconds, 5))
                continue
            delay = self.idle_seconds if exit_code == 20 else (1 if exit_code == 0 else 5)
            self._sleep_interruptibly(delay)
        self._publish(
            "",
            {
                "generation": "",
                "state": "stopped",
                "phase": "",
                "currentPart": "",
                "progressToken": "stopped",
                "lastError": self.last_error,
            },
            child_exit_code=None,
        )
        return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=data_root())
    parser.add_argument(
        "--no-progress-seconds",
        type=int,
        default=_positive_env_int(
            "RDS_BINLOG_INDEX_NO_PROGRESS_SECONDS",
            600,
            30,
        ),
    )
    parser.add_argument(
        "--idle-seconds",
        type=int,
        default=_positive_env_int("RDS_BINLOG_INDEX_IDLE_SECONDS", 15, 1),
    )
    args = parser.parse_args()
    supervisor = IndexSupervisor(
        args.data_dir.resolve(),
        no_progress_seconds=args.no_progress_seconds,
        idle_seconds=args.idle_seconds,
        source_priority_enabled=_bool_env(
            "RDS_BINLOG_INDEX_SOURCE_PRIORITY_ENABLED",
            False,
        ),
    )
    signal.signal(signal.SIGTERM, supervisor.request_stop)
    signal.signal(signal.SIGINT, supervisor.request_stop)
    return supervisor.run()


if __name__ == "__main__":
    raise SystemExit(main())
