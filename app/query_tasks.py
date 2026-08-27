from __future__ import annotations

import gzip
import json
import os
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

from .metadata import MetadataStore


class QueryCancelled(RuntimeError):
    code = "QUERY_CANCELLED"


class QueryControl:
    def __init__(self, task_id: str, metadata: MetadataStore):
        self.task_id = task_id
        self.metadata = metadata
        self._cancelled = threading.Event()
        self._lock = threading.Lock()
        self._completed_parts = 0
        self._scanned_bytes = 0
        self._last_flush = 0.0

    def cancel(self) -> None:
        self._cancelled.set()

    def check_cancelled(self) -> None:
        if self._cancelled.is_set():
            raise QueryCancelled("查询已取消")

    def set_plan(
        self,
        *,
        total_parts: int,
        candidate_parts: int,
        indexed_parts: int,
        unknown_parts: int,
        estimated_bytes: int,
    ) -> None:
        self.check_cancelled()
        self.metadata.update_query_task(
            self.task_id,
            total_parts=max(int(total_parts), 0),
            candidate_parts=max(int(candidate_parts), 0),
            indexed_parts=max(int(indexed_parts), 0),
            unknown_parts=max(int(unknown_parts), 0),
            estimated_bytes=max(int(estimated_bytes), 0),
            message="查询计划已生成，正在读取候选分区",
        )

    def advance(
        self,
        *,
        current_file: str = "",
        scanned_bytes: int = 0,
        force: bool = False,
    ) -> None:
        with self._lock:
            self._completed_parts += 1
            self._scanned_bytes += max(int(scanned_bytes), 0)
            now = time.monotonic()
            if not force and self._last_flush and now - self._last_flush < 0.25:
                return
            self._last_flush = now
            completed_parts = self._completed_parts
            total_scanned = self._scanned_bytes
        self.metadata.update_query_task(
            self.task_id,
            completed_parts=completed_parts,
            scanned_bytes=total_scanned,
            current_file=str(current_file),
            message="正在读取候选分区",
        )
        self.check_cancelled()

    def flush(self) -> None:
        with self._lock:
            completed_parts = self._completed_parts
            total_scanned = self._scanned_bytes
        self.metadata.update_query_task(
            self.task_id,
            completed_parts=completed_parts,
            scanned_bytes=total_scanned,
        )


def _bounded_workers(value: int | None = None) -> int:
    if value is None:
        try:
            value = int(os.environ.get("RDS_BINLOG_QUERY_TASK_WORKERS", "2"))
        except ValueError:
            value = 2
    return min(max(int(value), 1), 4)


class QueryTaskManager:
    def __init__(
        self,
        metadata: MetadataStore,
        storage: Any,
        *,
        settings_loader: Callable[[], Any],
        archive_loader: Callable[[Any], Any],
        max_workers: int | None = None,
        result_dir: Path | None = None,
    ):
        self.metadata = metadata
        self.storage = storage
        self.settings_loader = settings_loader
        self.archive_loader = archive_loader
        self.max_workers = _bounded_workers(max_workers)
        self.result_dir = result_dir or metadata.path.parent / "query-results"
        self.result_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._closing = False
        self._controls: dict[str, QueryControl] = {}
        self._futures: dict[str, Future[None]] = {}
        self.metadata.reconcile_interrupted_query_tasks()
        self._prune_history()
        self._executor = ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="query-task",
        )

    def submit(self, query: dict[str, Any]) -> str:
        with self._lock:
            if self._closing:
                raise RuntimeError("查询任务管理器正在停止")
            task_id = self.metadata.create_query_task(dict(query))
            control = QueryControl(task_id, self.metadata)
            self._controls[task_id] = control
            self._futures[task_id] = self._executor.submit(
                self._run,
                task_id,
                dict(query),
                control,
            )
        return task_id

    def _prune_history(self) -> None:
        for name in self.metadata.prune_query_tasks(keep=100):
            if not name or Path(name).name != name:
                continue
            try:
                (self.result_dir / name).unlink(missing_ok=True)
            except OSError:
                pass

    def _run(
        self,
        task_id: str,
        query: dict[str, Any],
        control: QueryControl,
    ) -> None:
        try:
            if not self.metadata.start_query_task(task_id):
                return
            control.check_cancelled()
            settings = self.settings_loader()
            archive = self.archive_loader(settings)
            result = self.storage.query_events_tiered(
                query,
                settings,
                archive,
                control=control,
            )
            control.check_cancelled()
            control.flush()
            result_path, result_bytes = self._write_result(task_id, result)
            self.metadata.finish_query_task(
                task_id,
                "succeeded",
                "查询完成",
                result_path=result_path,
                result_bytes=result_bytes,
            )
        except QueryCancelled:
            control.flush()
            self.metadata.finish_query_task(
                task_id,
                "cancelled",
                "查询已取消",
                error_code=QueryCancelled.code,
            )
        except BaseException as exc:
            control.flush()
            self.metadata.finish_query_task(
                task_id,
                "failed",
                str(exc) or "查询失败",
                error_code=str(getattr(exc, "code", "QUERY_FAILED")),
            )
        finally:
            self._prune_history()
            with self._lock:
                self._controls.pop(task_id, None)
                self._futures.pop(task_id, None)

    def cancel(self, task_id: str) -> dict[str, Any]:
        task = self.metadata.request_query_task_cancel(str(task_id))
        if task is None:
            raise KeyError(task_id)
        with self._lock:
            control = self._controls.get(task_id)
            future = self._futures.get(task_id)
        if control is not None:
            control.cancel()
        if future is not None:
            future.cancel()
        return task

    def get(self, task_id: str) -> dict[str, Any] | None:
        task = self.metadata.query_task(str(task_id), include_result=True)
        if task is None:
            return None
        result_path = str(task.pop("_result_path", "") or "")
        task["result"] = self._read_result(result_path) if result_path else None
        return task

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        return self.metadata.query_tasks(limit=limit, include_result=False)

    def _write_result(
        self,
        task_id: str,
        result: dict[str, Any],
    ) -> tuple[str, int]:
        payload = json.dumps(
            result,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        compressed = gzip.compress(payload, compresslevel=6, mtime=0)
        name = f"{task_id}.json.gz"
        destination = self.result_dir / name
        temporary = self.result_dir / f".{name}.{os.getpid()}.tmp"
        temporary.write_bytes(compressed)
        os.replace(temporary, destination)
        return name, len(compressed)

    def _read_result(self, name: str) -> dict[str, Any] | None:
        if not name or Path(name).name != name:
            return None
        path = self.result_dir / name
        try:
            return json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))
        except (OSError, ValueError, json.JSONDecodeError):
            return None

    def shutdown(self) -> None:
        with self._lock:
            if self._closing:
                return
            self._closing = True
            task_ids = list(self._controls)
        for task_id in task_ids:
            self.cancel(task_id)
        self._executor.shutdown(wait=True, cancel_futures=True)
