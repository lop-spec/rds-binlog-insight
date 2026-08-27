from __future__ import annotations

import logging
import multiprocessing
import threading
import time
from collections import deque
from collections.abc import Callable
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from .config import Settings, utc_now_text
from .credentials import CloudCredential, load_credential
from .downloader import DownloadError, download_file
from .maintenance_status import SUPERVISOR_STATUS_NAME, read_json_status
from .metadata import MetadataStore
from .oss_store import OSS_PACK_TARGET_BYTES, OssArchive, OssArchiveError
from .parser_bridge import ParserError, parse_ndjson_chunks_buffered
from .rds_api import RdsApiError, RdsRpcClient, RemoteBinlog
from .storage import EventStorage, StorageError, ingest_ndjson_file_detached

LOGGER = logging.getLogger(__name__)

FILE_PIPELINE_WORKERS = 1
DOWNLOAD_PIPELINE_WORKERS = 3
TRANSFORM_PIPELINE_WORKERS = 1
OSS_ARCHIVE_WORKERS = 4
OSS_ARCHIVE_BACKLOG_PER_FILE = OSS_ARCHIVE_WORKERS + 1
OSS_FRESH_UPLOAD_TARGET_BYTES = 1
ARCHIVE_METADATA_BATCH_SIZE = 128
LOCAL_INDEX_HANDOFF_MAX_BYTES = 1024**3
COLD_COMPRESSION_HOT_SECONDS = 24 * 60 * 60
COLD_COMPRESSION_QUERY_GRACE_SECONDS = 30
COLD_COMPRESSION_IDLE_SECONDS = 5


class RdsClientLike(Protocol):
    def verify_instance(self) -> dict[str, str]: ...

    def list_binlogs(self, start_utc: str, end_utc: str) -> list[RemoteBinlog]: ...


class PipelineError(RuntimeError):
    def __init__(self, message: str, code: str = "PIPELINE_ERROR"):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class PreparedBinlog:
    file_id: str
    item: RemoteBinlog
    raw_path: Path
    event_count: int
    parse_seconds: float


def _utc_api(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_utc(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def parse_sync_window(
    start_utc: str | None,
    end_utc: str | None,
) -> tuple[datetime | None, datetime | None]:
    start_text = str(start_utc or "").strip()
    end_text = str(end_utc or "").strip()
    if not start_text and not end_text:
        return None, None
    if not start_text or not end_text:
        raise PipelineError(
            "指定时间同步必须同时填写开始时间和结束时间",
            "INVALID_SYNC_WINDOW",
        )

    def parse_one(value: str, label: str) -> datetime:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise PipelineError(
                f"{label}格式无效，必须包含时区",
                "INVALID_SYNC_WINDOW",
            ) from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise PipelineError(
                f"{label}必须包含时区",
                "INVALID_SYNC_WINDOW",
            )
        return parsed.astimezone(UTC)

    start = parse_one(start_text, "开始时间")
    end = parse_one(end_text, "结束时间")
    if start >= end:
        raise PipelineError(
            "结束时间必须晚于开始时间",
            "INVALID_SYNC_WINDOW",
        )
    return start, end


def _overlaps_window(
    begin_text: str,
    end_text: str,
    start: datetime,
    end: datetime,
) -> bool:
    begin = _parse_utc(begin_text)
    finish = _parse_utc(end_text)
    if begin is None or finish is None:
        return True
    return begin <= end and finish >= start


class SyncManager:
    def __init__(
        self,
        metadata: MetadataStore,
        storage: EventStorage,
        *,
        client_factory: Callable[[Settings, CloudCredential], RdsClientLike]
        | None = None,
        credential_loader: Callable[[str], CloudCredential | None] = load_credential,
        archive_factory: Callable[[Settings], OssArchive] | None = None,
        start_scheduler: bool = True,
        settings_loader: Callable[[], Settings] | None = None,
        role: str = "primary",
        scope_instance_id: str = "",
        display_name: str = "",
    ):
        self.metadata = metadata
        self.storage = storage
        # 多实例：primary 用 app_settings 里的主实例；secondary 由调用方注入自己
        # 的 Settings(见 config.SecondaryInstance)。全局性的动作——保留期清理、
        # 冷压缩、历史索引——只归 primary 管，secondary 只负责把自己那个实例的
        # binlog 同步进来，否则同一份 storage 会被清理逻辑重复执行。
        self._settings_loader = settings_loader or metadata.load_settings
        self.role = "secondary" if role == "secondary" else "primary"
        self.scope_instance_id = str(scope_instance_id or "").strip()
        self.display_name = display_name or self.scope_instance_id
        self.client_factory = client_factory or (
            lambda settings, credential: RdsRpcClient(settings, credential)
        )
        self.credential_loader = credential_loader
        self._prefixed_archives: dict[str, OssArchive] = {}
        self._prefixed_archive_lock = threading.Lock()
        self.archive_factory = archive_factory or (
            lambda settings: OssArchive(
                settings,
                credential=self.credential_loader(settings.credential_target),
            )
        )
        self._state_lock = threading.RLock()
        self._worker: threading.Thread | None = None
        self._pause_after_current = threading.Event()
        self._shutdown = threading.Event()
        self._last_auto_start = 0.0
        # Monotonic clocks may be below one hour shortly after host boot.
        # A negative sentinel guarantees the first scheduled cleanup is due.
        self._last_retention_cleanup = float("-inf")
        self._retention_cleanup_lock = threading.Lock()
        self._client_refresh_lock = threading.Lock()
        self._cold_boundary_lock = threading.Lock()
        self._archive_status: dict[str, Any] = {
            "enabled": False,
            "ready": False,
            "lastError": "",
        }
        self._pipeline_status: dict[str, Any] = {
            "active": False,
            "fileWorkers": FILE_PIPELINE_WORKERS,
            "downloadWorkers": DOWNLOAD_PIPELINE_WORKERS,
            "transformWorkers": TRANSFORM_PIPELINE_WORKERS,
            "archiveWorkers": OSS_ARCHIVE_WORKERS,
            "inFlightFiles": [],
            "visibleFile": "",
        }
        legacy_cleanup = self.storage.purge_legacy_body_caches()
        self._index_status: dict[str, Any] = {
            "running": False,
            "lastError": "",
            "currentPart": "",
            "indexedParts": 0,
            "part_count": 0,
            "block_count": 0,
            "row_count": 0,
            "size_bytes": 0,
            "localBodyBytes": 0,
            "releasedLegacyBytes": int(legacy_cleanup["deleted_bytes"]),
        }
        if legacy_cleanup["errors"]:
            self._index_status["lastError"] = "；".join(
                legacy_cleanup["errors"][:3]
            )
        # 这两个数字只用于展示，但 part_catalog_stats() 是 parquet_parts(18.7 万行)
        # LEFT JOIN parquet_part_catalog(实测 1.7 GB、占 metadata.sqlite3 的 73%)
        # 的全表扫描。放在 __init__ 里等于把服务启动挂在一个 O(全表) 的统计上：
        # 2026-08-20 实测启动耗时已超过健康探测窗口(PROBE_TIMEOUT 5s × FAILURE_LIMIT 9)，
        # supervisor 直接 exit=-15 杀掉子进程重启一次才起得来，而且这个时间只会随
        # catalog 增长继续变长。改成先给 0、再由后台线程补算：启动路径上不再有全表扫描。
        self._catalog_status: dict[str, Any] = {
            "running": False,
            "lastError": "",
            "currentParts": [],
            "catalogedParts": 0,
            "totalParts": 0,
        }
        self._catalog_stats_thread: threading.Thread | None = None
        self._start_catalog_stats_backfill()
        self._catalog_retry_after: dict[str, float] = {}
        self._cold_compression_status: dict[str, Any] = {
            "enabled": False,
            "running": False,
            "state": "starting" if start_scheduler else "stopped",
            "currentPart": "",
            "convertedParts": 0,
            "savedBytes": 0,
            "lastDurationSeconds": None,
            "lastCompletedAt": "",
            "lastError": "",
        }
        # Historical indexing is intentionally never started in this process.
        # A native Arrow/SQLite stall must not be able to stop HTTP or Binlog sync.
        self._indexer: threading.Thread | None = None
        self._cataloger: threading.Thread | None = None
        self._scheduler: threading.Thread | None = None
        self._cold_compressor: threading.Thread | None = None
        if start_scheduler:
            self._scheduler = threading.Thread(
                target=self._scheduler_loop,
                name=(
                    f"binlog-auto-sync-{self.scope_instance_id}"
                    if self.role == "secondary"
                    else "binlog-auto-sync"
                ),
                daemon=True,
            )
            self._scheduler.start()
            if self.role == "primary":
                self._cold_compressor = threading.Thread(
                    target=self._cold_compression_loop,
                    name="binlog-cold-compression",
                    daemon=True,
                )
                self._cold_compressor.start()
            else:
                self._update_cold_compression_status(state="stopped")

    def _settings(self) -> Settings:
        return self._settings_loader()

    def _job_scope(self) -> str:
        if self.scope_instance_id:
            return self.scope_instance_id
        try:
            return str(self._settings().db_instance_id or "")
        except Exception:  # noqa: BLE001 - 状态查询不能因设置读取失败而中断
            return ""

    def status(self) -> dict[str, Any]:
        with self._state_lock:
            running = bool(self._worker and self._worker.is_alive())
        pipeline_status = dict(self._pipeline_status)
        active_files = (
            len(pipeline_status.get("inFlightFiles") or [])
            if pipeline_status.get("active")
            else 0
        )
        # 每个 manager 只看自己实例的任务：不过滤时会取到别的实例刚跑完的任务，
        # 把本实例的进度显示成对方的。实例 ID 取不到时退回不过滤(单实例旧行为)。
        latest = self.metadata.latest_job(self._job_scope())
        if latest is not None:
            latest["performance"] = self.metadata.sync_performance(
                latest,
                active_files=active_files,
            )
        external = read_json_status(
            self.storage.paths["index"] / SUPERVISOR_STATUS_NAME
        )
        external_index = external.get("index")
        external_catalog = external.get("catalog")
        return {
            "running": running,
            "pauseRequested": self._pause_after_current.is_set(),
            "latestJob": latest,
            "pipeline": pipeline_status,
            "archive": dict(self._archive_status),
            "coldCompression": {
                **dict(self._cold_compression_status),
                **self.storage.query_activity_status(),
            },
            "index": {
                **dict(self._index_status),
                **(external_index if isinstance(external_index, dict) else {}),
                "supervisor": external.get("supervisor") or {},
                "catalog": {
                    **dict(self._catalog_status),
                    **(
                        external_catalog
                        if isinstance(external_catalog, dict)
                        else {}
                    ),
                },
            },
        }

    def _update_pipeline_status(self, **values: Any) -> None:
        with self._state_lock:
            self._pipeline_status.update(values)

    def _update_cold_compression_status(self, **values: Any) -> None:
        with self._state_lock:
            self._cold_compression_status.update(values)

    def _prefixed_archive(self, settings: Settings, prefix: str) -> OssArchive | None:
        """按前缀取(并缓存)一个独立的 OSS 归档器。

        general log 与 binlog 必须落在不同的对象前缀下，否则同一目录里混着
        两类来源、两个实例的数据。每个前缀首次使用时单独确认一次生命周期
        规则，避免新前缀下的对象永不过期。
        """

        with self._prefixed_archive_lock:
            cached = self._prefixed_archives.get(prefix)
            if cached is not None:
                return cached
            scoped = replace(settings, oss_prefix=prefix)
            archive = self.archive_factory(scoped)
            try:
                archive.ensure_ready()
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("OSS 前缀 %s 生命周期确认失败: %s", prefix, exc)
            self._prefixed_archives[prefix] = archive
            return archive

    def _start_catalog_stats_backfill(self) -> None:
        """后台补算目录统计：启动路径上不做全表扫描，但也不能让指标长期停在 0。

        一次性线程，算完即退；失败写进 lastError 并打日志，不静默吞掉——否则
        「编目 0/0」看起来会像编目功能坏了。
        """

        def run() -> None:
            try:
                stats = self.metadata.part_catalog_stats()
            except Exception as exc:  # noqa: BLE001 - 补算失败不能影响服务
                LOGGER.warning("目录统计补算失败：%s", exc)
                self._catalog_status["lastError"] = f"目录统计补算失败：{exc}"
                return
            self._catalog_status["catalogedParts"] = int(stats["cataloged_parts"])
            self._catalog_status["totalParts"] = int(stats["total_parts"])
            LOGGER.info(
                "目录统计补算完成：%s/%s",
                stats["cataloged_parts"],
                stats["total_parts"],
            )

        thread = threading.Thread(
            target=run, name="catalog-stats-backfill", daemon=True
        )
        self._catalog_stats_thread = thread
        thread.start()

    def archive_parts_now(
        self,
        parts: list[dict[str, Any]],
        prefix: str = "",
        event_code: str = "OSS_GENERAL_LOG_ARCHIVED",
    ) -> int:
        """立即把指定分区归档到 OSS，不依赖 binlog 同步任务。

        后台 analytics/搜索索引只接受已归档到 OSS 的分区，而 general log
        采集不经过同步流程，`_archive_existing_parts` 又只在同步任务启动时
        跑一次。若采集端不自行归档，这些分区会永久停留在「待归档」状态，
        因而永远进不了「全量 SQL」聚合。
        """

        pending = [part for part in parts if not str(part.get("oss_key") or "")]
        if not pending:
            return 0
        settings = self._settings()
        if not settings.oss_enabled:
            return 0
        if prefix:
            archive = self._prefixed_archive(settings, prefix)
            settings = replace(settings, oss_prefix=prefix)
        else:
            archive = self.archive_for_settings(settings)
        if archive is None:
            return 0
        return self._archive_parts(
            "",
            settings,
            archive,
            pending,
            event_code=event_code,
            fresh=True,
        )

    def archive_for_settings(self, settings: Settings) -> OssArchive | None:
        if not settings.oss_enabled:
            return None
        return self.archive_factory(settings)

    def request_backfill_for_range(
        self,
        start_epoch_us: int,
        end_epoch_us: int,
    ) -> dict[str, Any]:
        start = datetime.fromtimestamp(int(start_epoch_us) / 1_000_000, UTC)
        end = datetime.fromtimestamp(int(end_epoch_us) / 1_000_000, UTC)
        if start >= end:
            raise PipelineError("查询时间范围无效", "INVALID_QUERY_WINDOW")
        settings = self._settings()
        settings.validate(require_identity=True)
        credential = self.credential_loader(settings.credential_target)
        if not credential:
            raise PipelineError(
                "未找到阿里云 AccessKey，无法核对 RDS Binlog 范围",
                "CREDENTIAL_MISSING",
            )
        credential.validate()
        client = self.client_factory(settings, credential)
        client.verify_instance()
        primary_host_instance_id = ""
        primary_resolver = getattr(client, "primary_host_instance_id", None)
        if callable(primary_resolver):
            primary_host_instance_id = str(primary_resolver())
        items = client.list_binlogs(_utc_api(start), _utc_api(end))
        available = [
            item
            for item in items
            if item.remote_status.lower() == "completed"
            and (
                not primary_host_instance_id
                or item.host_instance_id == primary_host_instance_id
            )
            and _overlaps_window(
                item.log_begin_utc,
                item.log_end_utc,
                start,
                end,
            )
        ]
        range_text = f"{_utc_api(start)} 至 {_utc_api(end)}"
        if not available:
            raise PipelineError(
                f"实例未找到 {range_text} 范围的 Binlog",
                "BINLOG_RANGE_NOT_FOUND",
            )
        try:
            job_id = self.start(
                reason="query-backfill",
                start_utc=_utc_api(start),
                end_utc=_utc_api(end),
            )
        except PipelineError as exc:
            if exc.code != "JOB_ALREADY_RUNNING":
                raise
            job_id = ""
        return {
            "available": True,
            "binlogFileCount": len(available),
            "jobId": job_id,
            "message": (
                f"实例存在 {range_text} 范围的 Binlog，已加入补档任务"
                if job_id
                else f"实例存在 {range_text} 范围的 Binlog；当前顺序任务完成后补档"
            ),
        }

    def start(
        self,
        *,
        reason: str = "manual",
        start_utc: str | None = None,
        end_utc: str | None = None,
    ) -> str:
        settings = self._settings()
        settings.validate(require_identity=True)
        requested_start, requested_end = parse_sync_window(start_utc, end_utc)
        credential = self.credential_loader(settings.credential_target)
        if not credential:
            raise PipelineError(
                "未找到阿里云 AccessKey；请在服务设置中保存或设置环境变量",
                "CREDENTIAL_MISSING",
            )
        credential.validate()
        with self._cold_boundary_lock, self._state_lock:
            if self._worker and self._worker.is_alive():
                raise PipelineError("已有同步任务正在运行", "JOB_ALREADY_RUNNING")
            self._pause_after_current.clear()
            window_text = (
                f"{_utc_api(requested_start)} 至 {_utc_api(requested_end)}"
                if requested_start and requested_end
                else "保留窗口至当前最新"
            )
            job_id = self.metadata.create_job(
                "sync",
                settings.db_instance_id,
                f"{reason}：准备核验 RDS 实例；范围={window_text}",
                requested_start_utc=(
                    _utc_api(requested_start) if requested_start else ""
                ),
                requested_end_utc=(
                    _utc_api(requested_end) if requested_end else ""
                ),
            )
            self._worker = threading.Thread(
                target=self._run_guarded,
                args=(
                    job_id,
                    settings,
                    credential,
                    requested_start,
                    requested_end,
                ),
                name=f"binlog-sync-{job_id[:8]}",
                daemon=True,
            )
            self._worker.start()
            return job_id

    def request_pause(self) -> bool:
        with self._state_lock:
            running = bool(self._worker and self._worker.is_alive())
        if running:
            self._pause_after_current.set()
        return running

    def shutdown(self) -> None:
        self._shutdown.set()
        self._pause_after_current.set()
        current = threading.current_thread()
        for thread in (
            self._scheduler,
            self._indexer,
            self._cataloger,
            self._cold_compressor,
            self._catalog_stats_thread,
        ):
            if thread is not None and thread is not current and thread.is_alive():
                thread.join(timeout=15)

    def _event(self, job_id: str, level: str, code: str, message: str) -> None:
        # job_id 为空表示不属于任何同步任务(如 general log 采集端自行归档)：
        # job_events 对 jobs 有外键，写入会失败，这里只落日志。
        if job_id:
            self.metadata.add_job_event(job_id, level, code, message)
        getattr(LOGGER, level if level in {"info", "warning", "error"} else "info")(
            "%s %s %s", job_id, code, message
        )

    def _run_guarded(
        self,
        job_id: str,
        settings: Settings,
        credential: CloudCredential,
        start_utc: datetime | None,
        end_utc: datetime | None,
    ) -> None:
        try:
            self._run(
                job_id,
                settings,
                credential,
                start_utc=start_utc,
                end_utc=end_utc,
            )
        except (
            RdsApiError,
            DownloadError,
            ParserError,
            StorageError,
            PipelineError,
            OssArchiveError,
        ) as exc:
            code = getattr(exc, "code", exc.__class__.__name__.upper())
            message = str(exc)
            self._event(job_id, "error", code, message)
            self.metadata.finish_job(job_id, "failed", message, code)
        except Exception as exc:
            LOGGER.exception("Unexpected sync failure")
            self._event(job_id, "error", "UNEXPECTED_ERROR", str(exc))
            self.metadata.finish_job(job_id, "failed", str(exc), "UNEXPECTED_ERROR")
        finally:
            self._update_pipeline_status(
                active=False,
                inFlightFiles=[],
                visibleFile="",
            )

    def _scan_start(self, settings: Settings) -> datetime:
        # Reconcile the complete retained window every run so a delayed OSS upload
        # cannot be skipped by a moving checkpoint.
        days = max(
            settings.initial_lookback_days,
            settings.retention_days,
            settings.oss_retention_days if settings.oss_enabled else 0,
        )
        return datetime.now(UTC) - timedelta(days=days)

    def _discover(
        self,
        client: RdsClientLike,
        settings: Settings,
        *,
        primary_host_instance_id: str = "",
        start_utc: datetime | None = None,
        end_utc: datetime | None = None,
    ) -> list[tuple[str, RemoteBinlog, str]]:
        now = datetime.now(UTC)
        scan_start = start_utc or self._scan_start(settings)
        scan_end = end_utc or now
        items = client.list_binlogs(_utc_api(scan_start), _utc_api(scan_end))
        if start_utc and end_utc:
            items = [
                item
                for item in items
                if _overlaps_window(
                    item.log_begin_utc,
                    item.log_end_utc,
                    start_utc,
                    end_utc,
                )
            ]
        if primary_host_instance_id:
            items = [
                item
                for item in items
                if item.host_instance_id == primary_host_instance_id
            ]
        primary_windows = {
            (item.log_begin_utc, item.log_end_utc)
            for item in items
            if item.remote_status.lower() == "completed"
        }
        eligible: dict[str, tuple[str, RemoteBinlog, str]] = {}
        refreshed = self.metadata.upsert_remotes(settings, items)
        for item, (file_id, state) in zip(items, refreshed, strict=True):
            if item.remote_status.lower() != "completed":
                continue
            if state != "done":
                eligible[file_id] = (file_id, item, state)

        for record in self.metadata.recoverable_files(settings.db_instance_id):
            file_id = str(record["id"])
            if file_id in eligible:
                continue
            if start_utc and end_utc and not _overlaps_window(
                str(record["log_begin_utc"]),
                str(record["log_end_utc"]),
                start_utc,
                end_utc,
            ):
                continue
            state = str(record["state"])
            raw_path = self.storage.paths["downloads"] / f"{file_id}.binlog"
            partial_path = raw_path.with_suffix(raw_path.suffix + ".part")
            if (
                state != "stored"
                and not raw_path.is_file()
                and not partial_path.is_file()
            ):
                continue
            record_window = (
                str(record["log_begin_utc"]),
                str(record["log_end_utc"]),
            )
            if (
                primary_host_instance_id
                and str(record["host_instance_id"]) != primary_host_instance_id
                and record_window in primary_windows
            ):
                continue
            item = RemoteBinlog(
                log_file_name=str(record["log_file_name"]),
                log_begin_utc=str(record["log_begin_utc"]),
                log_end_utc=str(record["log_end_utc"]),
                file_size=int(record["file_size"]),
                checksum_crc64=str(record["checksum_crc64"]),
                download_link=str(record["download_link"]),
                intranet_download_link=str(record["intranet_download_link"]),
                link_expired_utc=str(record["link_expired_utc"]),
                remote_status=str(record["remote_status"]),
                host_instance_id=str(record["host_instance_id"]),
            )
            eligible[file_id] = (file_id, item, state)

        return sorted(
            eligible.values(),
            key=lambda entry: (
                entry[1].log_begin_utc,
                entry[1].log_end_utc,
                entry[1].log_file_name,
                entry[1].host_instance_id,
            ),
        )

    def _refresh_item(
        self,
        client: RdsClientLike,
        settings: Settings,
        old: RemoteBinlog,
    ) -> RemoteBinlog:
        begin = _parse_utc(old.log_begin_utc) or self._scan_start(settings)
        end = (_parse_utc(old.log_end_utc) or datetime.now(UTC)) + timedelta(hours=1)
        for item in client.list_binlogs(_utc_api(begin - timedelta(hours=1)), _utc_api(end)):
            if self.metadata.file_id(settings.db_instance_id, item) == self.metadata.file_id(
                settings.db_instance_id, old
            ):
                self.metadata.upsert_remote(settings, item)
                return item
        raise DownloadError(
            f"刷新下载链接后未找到 {old.log_file_name}", "DOWNLOAD_LINK_REFRESH_MISSING"
        )

    def _download(
        self,
        job_id: str,
        client: RdsClientLike,
        settings: Settings,
        file_id: str,
        item: RemoteBinlog,
    ) -> tuple[Path, str]:
        started = time.monotonic()
        path = self.storage.paths["downloads"] / f"{file_id}.binlog"
        self.metadata.set_file_state(
            file_id, "downloading", increment_attempt=True, error_code="", error_message=""
        )
        last_progress = 0

        def on_progress(value: int) -> None:
            nonlocal last_progress
            if value == item.file_size or value - last_progress >= 8 * 1024 * 1024:
                self.metadata.update_download_progress(file_id, value)
                last_progress = value

        def attempt(remote: RemoteBinlog):
            return download_file(
                remote.selected_url(),
                path,
                expected_size=remote.file_size,
                expected_crc64=remote.checksum_crc64,
                progress=on_progress,
            )

        try:
            result = attempt(item)
        except DownloadError as exc:
            if exc.code not in {
                "LINK_EXPIRED",
                "DOWNLOAD_LINK_MISSING",
                "HTTP_404",
            }:
                raise
            self._event(
                job_id,
                "warning",
                "DOWNLOAD_LINK_REFRESH",
                f"{item.log_file_name} 下载链接不可用，刷新一次",
            )
            with self._client_refresh_lock:
                refreshed = self._refresh_item(client, settings, item)
            result = attempt(refreshed)
        self.metadata.set_file_state(
            file_id,
            "downloaded",
            downloaded_bytes=result.size_bytes,
            local_sha256=result.sha256,
        )
        self._event(
            job_id,
            "info",
            "FILE_DOWNLOADED",
            f"{item.log_file_name}：{result.size_bytes} 字节，"
            f"流式 CRC64/SHA-256 校验完成，耗时 {time.monotonic() - started:.3f} 秒",
        )
        return result.path, result.sha256

    def _archive_parts(
        self,
        job_id: str,
        settings: Settings,
        archive: OssArchive,
        parts: list[dict[str, Any]],
        *,
        event_code: str,
        fresh: bool = False,
    ) -> int:
        archived = 0
        pending = [
            part for part in parts if not str(part.get("oss_key") or "")
        ]
        pack_uploader = getattr(archive, "upload_parts", None)
        pack_batcher = getattr(archive, "pack_batches", None)
        batches = (
            pack_batcher(pending, OSS_PACK_TARGET_BYTES)
            if callable(pack_uploader) and callable(pack_batcher)
            else [[part] for part in pending]
        )
        for batch in batches:
            if callable(pack_uploader):
                results = pack_uploader(
                    batch,
                    scratch_dir=self.storage.paths["scratch"],
                    target_bytes=OSS_PACK_TARGET_BYTES,
                    fresh=fresh,
                )
            else:
                results = [
                    archive.upload_part(part, fresh=fresh) for part in batch
                ]
            if len(results) != len(batch):
                raise OssArchiveError(
                    "OSS 聚合上传返回的分区数量不一致",
                    "OSS_PACK_RESULT_MISMATCH",
                )
            prepared = []
            for part, result in zip(batch, results, strict=True):
                archive_values = {
                    "oss_key": str(result["oss_key"]),
                    "oss_etag": str(result["oss_etag"]),
                    "oss_offset": int(result.get("oss_offset") or 0),
                    "oss_length": int(result.get("oss_length") or 0),
                    "oss_object_sha256": str(
                        result.get("oss_object_sha256") or ""
                    ),
                }
                prepared.append((part, archive_values))
            for offset in range(0, len(prepared), ARCHIVE_METADATA_BATCH_SIZE):
                chunk = prepared[offset : offset + ARCHIVE_METADATA_BATCH_SIZE]
                self.metadata.mark_parts_archived(
                    [
                        {"path": str(part["path"]), **archive_values}
                        for part, archive_values in chunk
                    ]
                )
                for part, archive_values in chunk:
                    path = Path(str(part["path"]))
                    self.storage.note_part_archived(
                        str(path),
                        **archive_values,
                    )
                    archived_part = {**part, **archive_values}
                    # Fresh bodies cannot have been accepted by the external
                    # indexer before their verified OSS locator is committed.
                    # Opening the large search index here is therefore redundant
                    # and, with many collectors aligned on one poll boundary, can
                    # starve the HTTP liveness thread.  The indexer releases each
                    # body immediately after its own durable commit.
                    indexed = (
                        False
                        if fresh
                        else self.storage.search_index.is_current(archived_part)
                    )
                    released = (
                        self.storage.release_archived_body(archived_part)
                        if indexed
                        else 0
                    )
                    archived += 1
                    disposition = (
                        f"本地正文已释放 {released} 字节"
                        if released
                        else "本地正文暂存等待索引提交"
                    )
                    object_kind = (
                        "OSS 聚合对象"
                        if archive_values["oss_length"]
                        else "OSS 独立对象"
                    )
                    self._event(
                        job_id,
                        "info",
                        event_code,
                        f"{path.name} 已写入 {object_kind}并通过大小与 "
                        f"SHA-256 校验；{disposition}",
                    )
        if not fresh:
            cache = self.storage.enforce_local_cache_limit(
                LOCAL_INDEX_HANDOFF_MAX_BYTES
            )
            if cache["errors"]:
                raise StorageError(
                    "索引交接缓存清理失败：" + "；".join(cache["errors"][:3]),
                    "LOCAL_INDEX_HANDOFF_EVICTION_FAILED",
                )
        return archived

    def _archive_existing_parts(
        self,
        job_id: str,
        settings: Settings,
        archive: OssArchive,
    ) -> int:
        all_pending = self.metadata.pending_archive_parts()
        missing = [
            part
            for part in all_pending
            if not Path(str(part["path"])).is_file()
        ]
        if missing:
            raise StorageError(
                f"{len(missing)} 个未归档 Parquet 的本地文件缺失，拒绝跳过迁移",
                "OSS_MIGRATION_SOURCE_MISSING",
            )
        pending = [
            part
            for part in all_pending
            if Path(str(part["path"])).is_file()
        ]
        if not pending:
            cache = self.storage.enforce_local_cache_limit(
                LOCAL_INDEX_HANDOFF_MAX_BYTES
            )
            if cache["errors"]:
                raise StorageError(
                    "本地索引正文释放检查失败："
                    + "；".join(cache["errors"][:3]),
                    "LOCAL_CACHE_EVICTION_FAILED",
                )
            return 0
        self.metadata.update_job(
            job_id,
            message=f"正在按最早分区迁移 {len(pending)} 个现有 Parquet 到 OSS",
        )
        return self._archive_parts(
            job_id,
            settings,
            archive,
            pending,
            event_code="OSS_EXISTING_PART_ARCHIVED",
        )

    def _finish_stored_file(
        self,
        file_id: str,
        raw_path: Path,
        settings: Settings,
    ) -> None:
        parts = self.metadata.parts_for_file(file_id)
        for part in parts:
            path = Path(part["path"])
            if path.is_file() and path.stat().st_size == int(part["size_bytes"]):
                continue
            if settings.oss_enabled and str(part.get("oss_key") or ""):
                continue
            if not path.is_file():
                raise StorageError(
                    f"已存储分区既不在本地也未归档 OSS：{path.name}",
                    "STORED_PART_INVALID",
                )
            raise StorageError(
                f"已存储分区大小变化：{path.name}",
                "STORED_PART_INVALID",
            )
        if settings.oss_enabled and not self.metadata.file_archive_complete(file_id):
            raise StorageError(
                "当前 Binlog 尚有 Parquet 未完成 OSS 校验，保留原文件",
                "OSS_ARCHIVE_INCOMPLETE",
            )
        if raw_path.exists():
            raw_path.unlink()
        partial = raw_path.with_suffix(raw_path.suffix + ".part")
        if partial.exists():
            partial.unlink()
        self.metadata.set_file_state(file_id, "done", raw_deleted=True)

    def _process_one(
        self,
        job_id: str,
        client: RdsClientLike,
        settings: Settings,
        file_id: str,
        item: RemoteBinlog,
        prior_state: str,
        flavor: str,
        archive: OssArchive | None,
        *,
        prepared_download: tuple[Path, str] | None = None,
        defer_commit: bool = False,
        query_visible_event: threading.Event | None = None,
        archive_submitter: Callable[
            [list[dict[str, Any]]],
            Future[int],
        ]
        | None = None,
        transform_submitter: Callable[
            [dict[str, Any]],
            Future[tuple[int, list[dict[str, Any]]]],
        ]
        | None = None,
    ) -> int | PreparedBinlog:
        raw_path = self.storage.paths["downloads"] / f"{file_id}.binlog"
        visible_event = query_visible_event or threading.Event()
        if query_visible_event is None:
            visible_event.set()
            self.metadata.set_file_visibility(file_id, True)
            self.metadata.update_job(job_id, current_file=item.log_file_name)
        if prior_state == "stored":
            count = int(
                (self.metadata.file_record(file_id) or {}).get("event_count") or 0
            )
            prepared = PreparedBinlog(
                file_id=file_id,
                item=item,
                raw_path=raw_path,
                event_count=count,
                parse_seconds=0.0,
            )
            if defer_commit:
                return prepared
            self._commit_prepared(job_id, settings, prepared)
            return count
        path, _ = prepared_download or self._download(
            job_id, client, settings, file_id, item
        )
        parse_started = time.monotonic()
        self.metadata.set_file_state(file_id, "parsing")
        count = 0
        keep_paths: set[str] = set()
        archive_futures: deque[Future[int]] = deque()
        archive_buffer: list[dict[str, Any]] = []
        archive_buffer_bytes = 0

        def submit_archive_buffer(*, force: bool) -> None:
            nonlocal archive_buffer, archive_buffer_bytes
            if archive is None or not archive_buffer:
                return
            if (
                not force
                and archive_buffer_bytes < OSS_FRESH_UPLOAD_TARGET_BYTES
            ):
                return
            batch = archive_buffer
            archive_buffer = []
            archive_buffer_bytes = 0
            if archive_submitter is not None:
                archive_futures.append(archive_submitter(batch))
                if len(archive_futures) >= OSS_ARCHIVE_BACKLOG_PER_FILE:
                    archive_futures.popleft().result()
            else:
                self._archive_parts(
                    job_id,
                    settings,
                    archive,
                    batch,
                    event_code="OSS_CHUNK_ARCHIVED",
                    fresh=True,
                )

        try:
            for chunk_index, ndjson_path in enumerate(
                parse_ndjson_chunks_buffered(
                    path,
                    file_id,
                    self.storage.paths["staging"],
                    flavor,
                )
            ):
                try:
                    if transform_submitter is None:
                        chunk_count, parts = self.storage.ingest_ndjson_file(
                            file_id=file_id,
                            instance_id=settings.db_instance_id,
                            host_instance_id=item.host_instance_id,
                            source_file_name=item.log_file_name,
                            ndjson_path=ndjson_path,
                            part_key=f"{chunk_index:06d}",
                            append=True,
                        )
                    else:
                        chunk_count, parts = transform_submitter(
                            {
                                "data_dir": str(self.storage.paths["root"]),
                                "file_id": file_id,
                                "instance_id": settings.db_instance_id,
                                "host_instance_id": item.host_instance_id,
                                "source_file_name": item.log_file_name,
                                "ndjson_path": str(ndjson_path),
                                "part_key": f"{chunk_index:06d}",
                            }
                        ).result()
                        self.storage.publish_ingested_parts(
                            file_id,
                            parts,
                            append=True,
                        )
                finally:
                    if ndjson_path.exists():
                        try:
                            ndjson_path.unlink()
                        except OSError:
                            pass
                count += chunk_count
                keep_paths.update(str(part["path"]) for part in parts)
                if archive is not None:
                    archive_buffer.extend(parts)
                    archive_buffer_bytes += sum(
                        max(int(part.get("size_bytes") or 0), 0)
                        for part in parts
                    )
                    submit_archive_buffer(force=False)
                self.metadata.set_file_state(
                    file_id,
                    "parsing",
                    event_count=count,
                )
                if visible_event.is_set():
                    self.metadata.update_job(
                        job_id,
                        message=(
                            f"{item.log_file_name} 已发布 {count} 条事件；"
                            "这些事件现在即可查询"
                        ),
                    )
                    self._event(
                        job_id,
                        "info",
                        "FILE_CHUNK_PUBLISHED",
                        f"{item.log_file_name} 第 {chunk_index + 1} 批："
                        f"{chunk_count} 条事件已原子发布",
                    )
            submit_archive_buffer(force=True)
            while archive_futures:
                archive_futures.popleft().result()
        except Exception:
            archive_buffer.clear()
            for future in archive_futures:
                future.cancel()
            while archive_futures:
                future = archive_futures.popleft()
                if future.cancelled():
                    continue
                try:
                    future.result()
                except Exception:
                    pass
            raise
        self.storage.finalize_file_parts(file_id, keep_paths)
        self.metadata.set_file_state(file_id, "stored", event_count=count)
        prepared = PreparedBinlog(
            file_id=file_id,
            item=item,
            raw_path=path,
            event_count=count,
            parse_seconds=time.monotonic() - parse_started,
        )
        if defer_commit:
            return prepared
        self._commit_prepared(job_id, settings, prepared)
        return count

    def _commit_prepared(
        self,
        job_id: str,
        settings: Settings,
        prepared: PreparedBinlog,
    ) -> None:
        self._finish_stored_file(
            prepared.file_id,
            prepared.raw_path,
            settings,
        )
        self._event(
            job_id,
            "info",
            "FILE_COMPLETE",
            f"{prepared.item.log_file_name}：{prepared.event_count} 条事件，"
            "临时原文件已删除；"
            f"解析、Parquet 与 OSS 耗时 {prepared.parse_seconds:.3f} 秒",
        )

    def _record_file_error(
        self,
        job_id: str,
        file_id: str,
        item: RemoteBinlog,
        exc: Exception,
        unavailable: int,
    ) -> int:
        code = getattr(exc, "code", exc.__class__.__name__.upper())
        if code == "DOWNLOAD_LINK_REFRESH_MISSING":
            unavailable += 1
            message = (
                f"实例未找到 {item.log_begin_utc} 至 "
                f"{item.log_end_utc} 范围的 Binlog；"
                "本地断点已保留，继续处理后续可用文件"
            )
            self.metadata.set_file_state(
                file_id,
                "unavailable",
                error_code="INSTANCE_BINLOG_NOT_FOUND",
                error_message=message,
            )
            self.metadata.update_job(
                job_id,
                failed_files=unavailable,
                message=message,
            )
            self._event(
                job_id,
                "warning",
                "INSTANCE_BINLOG_NOT_FOUND",
                message,
            )
            return unavailable
        current_state = str(
            (self.metadata.file_record(file_id) or {}).get("state") or ""
        )
        self.metadata.set_file_state(
            file_id,
            "stored" if current_state == "stored" else "failed",
            error_code=code,
            error_message=str(exc),
        )
        self.metadata.update_job(
            job_id,
            failed_files=unavailable + 1,
        )
        raise exc

    def _run_pending_parallel(
        self,
        job_id: str,
        client: RdsClientLike,
        settings: Settings,
        pending: list[tuple[str, RemoteBinlog, str]],
        flavor: str,
        archive: OssArchive | None,
        *,
        completed: int,
        unavailable: int,
    ) -> tuple[int, int, bool]:
        archive_thread_local = threading.local()

        def archive_parts(parts: list[dict[str, Any]]) -> int:
            worker_archive = getattr(archive_thread_local, "archive", None)
            if worker_archive is None:
                worker_archive = self.archive_for_settings(settings)
                if worker_archive is None:
                    raise PipelineError(
                        "OSS 并行归档工作线程未取得归档客户端",
                        "OSS_WORKER_ARCHIVE_MISSING",
                    )
                archive_thread_local.archive = worker_archive
            return self._archive_parts(
                job_id,
                settings,
                worker_archive,
                parts,
                event_code="OSS_CHUNK_ARCHIVED",
                fresh=True,
            )

        def schedule_downloads(
            executor: ThreadPoolExecutor,
            batch: list[tuple[str, RemoteBinlog, str]],
        ) -> list[Future[tuple[Path, str]] | None]:
            futures: list[Future[tuple[Path, str]] | None] = []
            for file_id, item, prior_state in batch:
                self.metadata.set_file_visibility(file_id, False)
                if prior_state == "stored":
                    futures.append(None)
                    continue
                futures.append(
                    executor.submit(
                        self._download,
                        job_id,
                        client,
                        settings,
                        file_id,
                        item,
                    )
                )
            return futures

        self._update_pipeline_status(active=True)
        try:
            with (
                ThreadPoolExecutor(
                    max_workers=DOWNLOAD_PIPELINE_WORKERS,
                    thread_name_prefix="binlog-download",
                ) as download_executor,
                ThreadPoolExecutor(
                    max_workers=FILE_PIPELINE_WORKERS,
                    thread_name_prefix="binlog-file",
                ) as process_executor,
                ProcessPoolExecutor(
                    max_workers=TRANSFORM_PIPELINE_WORKERS,
                    mp_context=multiprocessing.get_context("spawn"),
                ) as transform_executor,
                ThreadPoolExecutor(
                    max_workers=OSS_ARCHIVE_WORKERS,
                    thread_name_prefix="binlog-oss",
                ) as archive_executor,
            ):
                batch_start = 0
                batch = pending[:FILE_PIPELINE_WORKERS]
                downloads = schedule_downloads(download_executor, batch)
                while batch:
                    visibility_events = [
                        threading.Event() for _entry in batch
                    ]
                    visibility_events[0].set()
                    self.metadata.set_file_visibility(batch[0][0], True)
                    self.metadata.update_job(
                        job_id,
                        current_file=batch[0][1].log_file_name,
                    )
                    self._update_pipeline_status(
                        inFlightFiles=[
                            item.log_file_name for _file_id, item, _state in batch
                        ],
                        visibleFile=batch[0][1].log_file_name,
                    )

                    staged: list[
                        tuple[
                            Future[PreparedBinlog | int] | None,
                            Exception | None,
                        ]
                    ] = []
                    for entry, download_future, visible_event in zip(
                        batch,
                        downloads,
                        visibility_events,
                        strict=True,
                    ):
                        file_id, item, prior_state = entry
                        prepared_download: tuple[Path, str] | None = None
                        download_error: Exception | None = None
                        if download_future is not None:
                            try:
                                prepared_download = download_future.result()
                            except Exception as exc:
                                download_error = exc
                        if download_error is not None:
                            staged.append((None, download_error))
                            continue

                        def submit_archive(
                            parts: list[dict[str, Any]],
                        ) -> Future[int]:
                            return archive_executor.submit(archive_parts, parts)

                        def submit_transform(
                            payload: dict[str, Any],
                        ) -> Future[tuple[int, list[dict[str, Any]]]]:
                            return transform_executor.submit(
                                ingest_ndjson_file_detached,
                                payload,
                            )

                        staged.append(
                            (
                                process_executor.submit(
                                    self._process_one,
                                    job_id,
                                    client,
                                    settings,
                                    file_id,
                                    item,
                                    prior_state,
                                    flavor,
                                    archive,
                                    prepared_download=prepared_download,
                                    defer_commit=True,
                                    query_visible_event=visible_event,
                                    archive_submitter=(
                                        submit_archive
                                        if archive is not None
                                        else None
                                    ),
                                    transform_submitter=submit_transform,
                                ),
                                None,
                            )
                        )

                    next_start = batch_start + len(batch)
                    next_batch = pending[
                        next_start : next_start + FILE_PIPELINE_WORKERS
                    ]
                    next_downloads = (
                        schedule_downloads(download_executor, next_batch)
                        if next_batch
                        else []
                    )

                    for position, (
                        entry,
                        visible_event,
                        stage,
                    ) in enumerate(
                        zip(batch, visibility_events, staged, strict=True)
                    ):
                        file_id, item, _prior_state = entry
                        future, stage_error = stage
                        if position:
                            visible_event.set()
                            self.metadata.set_file_visibility(file_id, True)
                            record = self.metadata.file_record(file_id) or {}
                            prepared_count = int(record.get("event_count") or 0)
                            self.metadata.update_job(
                                job_id,
                                current_file=item.log_file_name,
                                message=(
                                    f"{item.log_file_name} 已按源顺序开放；"
                                    f"已准备 {prepared_count} 条事件"
                                ),
                            )
                            self._event(
                                job_id,
                                "info",
                                "FILE_ORDER_ACTIVATED",
                                f"{item.log_file_name} 已按源顺序进入可查询状态",
                            )
                            self._update_pipeline_status(
                                visibleFile=item.log_file_name,
                            )
                        try:
                            if stage_error is not None:
                                raise stage_error
                            if future is None:
                                raise PipelineError(
                                    "并行文件任务缺少执行句柄",
                                    "PIPELINE_FUTURE_MISSING",
                                )
                            prepared = future.result()
                            if not isinstance(prepared, PreparedBinlog):
                                raise PipelineError(
                                    "并行文件任务返回了无效结果",
                                    "PIPELINE_RESULT_INVALID",
                                )
                            self._commit_prepared(job_id, settings, prepared)
                        except Exception as exc:
                            unavailable = self._record_file_error(
                                job_id,
                                file_id,
                                item,
                                exc,
                                unavailable,
                            )
                            continue
                        completed += 1
                        self.metadata.update_job(
                            job_id,
                            completed_files=completed,
                        )

                    batch_start = next_start
                    batch = next_batch
                    downloads = next_downloads
                    self._update_pipeline_status(
                        inFlightFiles=[],
                        visibleFile="",
                    )
                    if (
                        self._pause_after_current.is_set()
                        or self._shutdown.is_set()
                    ):
                        self.metadata.finish_job(
                            job_id,
                            "paused",
                            "已在并行文件批次边界暂停；下次从断点继续",
                        )
                        return completed, unavailable, True
        finally:
            self._update_pipeline_status(
                active=False,
                inFlightFiles=[],
                visibleFile="",
            )
        return completed, unavailable, False

    def _run(
        self,
        job_id: str,
        settings: Settings,
        credential: CloudCredential,
        *,
        start_utc: datetime | None = None,
        end_utc: datetime | None = None,
    ) -> None:
        archive = self.archive_for_settings(settings)
        if archive is not None:
            try:
                ready = archive.ensure_ready()
                self._archive_status = {
                    "enabled": True,
                    "ready": True,
                    "lastError": "",
                    **ready,
                }
                lifecycle = ready["lifecycle"]
                self._event(
                    job_id,
                    "info",
                    "OSS_READY",
                    f"OSS={settings.oss_bucket}/{settings.oss_prefix}；"
                    f"生命周期={lifecycle['expirationDays']}天",
                )
                migrated = self._archive_existing_parts(
                    job_id,
                    settings,
                    archive,
                )
                if migrated:
                    self._event(
                        job_id,
                        "info",
                        "OSS_EXISTING_MIGRATION_COMPLETE",
                        f"现有数据迁移完成：{migrated} 个 Parquet 分区",
                    )
            except Exception as exc:
                self._archive_status = {
                    "enabled": True,
                    "ready": False,
                    "lastError": str(exc),
                }
                raise
        else:
            self._archive_status = {
                "enabled": False,
                "ready": False,
                "lastError": "",
            }
        client = self.client_factory(settings, credential)
        identity = client.verify_instance()
        engine = str(identity.get("engine") or "").lower()
        flavor = "mariadb" if engine == "mariadb" else "mysql"
        primary_host_instance_id = ""
        primary_resolver = getattr(client, "primary_host_instance_id", None)
        if callable(primary_resolver):
            primary_host_instance_id = str(primary_resolver())
        self._event(
            job_id,
            "info",
            "IDENTITY_VERIFIED",
            f"实例={identity.get('dbInstanceId')}；"
            f"引擎={identity.get('engine')} {identity.get('engineVersion')}"
            + (
                f"；主节点={primary_host_instance_id}"
                if primary_host_instance_id
                else ""
            ),
        )
        completed = 0
        unavailable = 0
        discovered_ids: set[str] = set()
        while not self._shutdown.is_set():
            pending = self._discover(
                client,
                settings,
                primary_host_instance_id=primary_host_instance_id,
                start_utc=start_utc,
                end_utc=end_utc,
            )
            for file_id, _item, _state in pending:
                discovered_ids.add(file_id)
            pending = [
                entry
                for entry in pending
                if (self.metadata.file_record(entry[0]) or {}).get("state") != "done"
            ]
            self.metadata.update_job(
                job_id,
                total_files=len(discovered_ids),
                discovered_files=len(discovered_ids),
                completed_files=completed,
                current_file="" if not pending else pending[0][1].log_file_name,
                message=(
                    f"发现 {len(discovered_ids)} 个待处理文件"
                    if pending
                    else "正在确认是否已到 API 最新文件"
                ),
            )
            if not pending:
                break
            if self._pause_after_current.is_set() or self._shutdown.is_set():
                self.metadata.finish_job(
                    job_id, "paused", "已在文件边界暂停；下次从断点继续"
                )
                return
            completed, unavailable, paused = self._run_pending_parallel(
                job_id,
                client,
                settings,
                pending,
                flavor,
                archive,
                completed=completed,
                unavailable=unavailable,
            )
            if paused:
                return
            self.metadata.update_job(
                job_id,
                current_file="",
                message=(
                    "当前清单已处理完，正在确认是否有新的 Completed Binlog"
                ),
            )
        cleanup = self.storage.cleanup(
            settings.retention_days,
            archive_enabled=settings.oss_enabled,
        )
        cache = self.storage.enforce_local_cache_limit(0)
        query_cache = self.storage.enforce_query_cache_limit(0)
        cleanup["errors"].extend(cache["errors"])
        cleanup["errors"].extend(query_cache["errors"])
        if unavailable:
            completed_message = (
                "指定时间范围内其余可用 Completed Binlog 已全部解析；"
                if start_utc and end_utc
                else "已解析到当前 API 最新可用 Completed Binlog；"
            ) + (
                f"{unavailable} 个实例已不再提供的范围已标记不可用，"
                "本地断点已保留"
            )
        else:
            completed_message = (
                "指定时间范围内的 Completed Binlog 已全部解析，"
                "临时原文件均已删除，已完成事件可即时查询"
                if start_utc and end_utc
                else "已解析到当前 API 最新 Completed Binlog，"
                "临时原文件均已删除，已完成事件可即时查询"
            )
        if cleanup["errors"]:
            self._event(
                job_id,
                "warning",
                "RETENTION_PARTIAL",
                f"同步完成，但 {len(cleanup['errors'])} 个分区物理清理失败；"
                "查询边界仍严格限制为保留期",
            )
            self.metadata.finish_job(
                job_id,
                "warning",
                completed_message + "；部分物理清理待重试",
                "RETENTION_PARTIAL",
            )
        elif unavailable:
            self.metadata.finish_job(
                job_id,
                "warning",
                completed_message,
                "INSTANCE_BINLOG_NOT_FOUND",
            )
        else:
            self.metadata.finish_job(
                job_id,
                "success",
                completed_message,
            )

    def _run_retention_cleanup_if_due(self, settings: Settings) -> None:
        now = time.monotonic()
        if now - self._last_retention_cleanup < 60 * 60:
            return
        with self._state_lock:
            if self._worker and self._worker.is_alive():
                return
        if not self._retention_cleanup_lock.acquire(blocking=False):
            return
        attempted = False
        try:
            with self._state_lock:
                if self._worker and self._worker.is_alive():
                    return
            attempted = True
            result = self.storage.cleanup(
                settings.retention_days,
                archive_enabled=settings.oss_enabled,
            )
            cache = self.storage.enforce_local_cache_limit(0)
            query_cache = self.storage.enforce_query_cache_limit(0)
            result["errors"].extend(cache["errors"])
            result["errors"].extend(query_cache["errors"])
            if result["errors"]:
                LOGGER.warning(
                    "Scheduled retention cleanup completed with %d errors",
                    len(result["errors"]),
                )
            else:
                LOGGER.info(
                    "Scheduled retention cleanup complete: deleted=%s rewritten=%s "
                    "removed_rows=%s",
                    result["deleted_parts"],
                    result["rewritten_parts"],
                    result["removed_rows"],
                )
        except Exception:
            LOGGER.exception("Scheduled retention cleanup failed")
        finally:
            if attempted:
                self._last_retention_cleanup = now
            self._retention_cleanup_lock.release()

    def _cold_compression_gate(
        self,
        settings: Settings,
    ) -> tuple[bool, str]:
        if not settings.oss_enabled:
            return False, "oss-disabled"
        if not settings.db_instance_id:
            return False, "instance-not-configured"
        with self._state_lock:
            if self._worker and self._worker.is_alive():
                return False, "sync-running"
        # 不限定实例：附加实例还在同步时，冷压缩(重写已归档分区)会和它的写入
        # 抢同一批磁盘与 CPU，等全部实例都追平了再压。
        if self.metadata.pending_sync_file_count():
            return False, "pending-binlogs"
        if self.metadata.pending_archive_parts(limit=1):
            return False, "archive-pending"
        latest = self.metadata.latest_job(self._job_scope())
        if latest is None:
            return False, "awaiting-latest-check"
        if str(latest.get("requested_start_utc") or "") or str(
            latest.get("requested_end_utc") or ""
        ):
            return False, "awaiting-full-latest-check"
        if str(latest.get("status") or "") not in {"success", "warning"}:
            return False, "latest-sync-not-complete"
        performance = self.metadata.sync_performance(latest, active_files=0)
        if (
            str(performance.get("state") or "") != "caught_up"
            or int(performance.get("known_remaining_files") or 0) != 0
        ):
            return False, "not-caught-up"
        if self.storage.has_query_pressure(
            grace_seconds=COLD_COMPRESSION_QUERY_GRACE_SECONDS
        ):
            return False, "query-pressure"
        return True, "ready"

    def _delete_one_retired_oss_object(self, archive: OssArchive) -> str:
        ready = self.metadata.retired_oss_objects_ready(limit=1)
        if not ready:
            return ""
        key = str(ready[0]["oss_key"])
        try:
            archive.delete_object(key)
            self.metadata.forget_retired_oss_object(key)
            LOGGER.info("Deleted unreferenced retired OSS object %s", key)
            return key
        except Exception as exc:
            self.metadata.record_retired_oss_failure(key, str(exc))
            LOGGER.warning("Retired OSS object deletion deferred: %s", exc)
            return ""

    def _cold_compression_loop(self) -> None:
        if self._shutdown.wait(10):
            return
        archive: OssArchive | None = None
        archive_identity: tuple[str, str, str, str] | None = None
        while not self._shutdown.is_set():
            try:
                settings = self._settings()
                allowed, state = self._cold_compression_gate(settings)
                self._update_cold_compression_status(
                    enabled=bool(settings.oss_enabled),
                    running=False,
                    state=state,
                    currentPart="",
                )
                if not allowed:
                    self._shutdown.wait(COLD_COMPRESSION_IDLE_SECONDS)
                    continue

                identity = (
                    settings.oss_bucket,
                    settings.oss_endpoint,
                    settings.oss_prefix,
                    settings.oss_auth_mode,
                )
                if archive is None or archive_identity != identity:
                    archive = self.archive_for_settings(settings)
                    archive_identity = identity
                if archive is None:
                    self._shutdown.wait(COLD_COMPRESSION_IDLE_SECONDS)
                    continue

                self._delete_one_retired_oss_object(archive)
                cutoff_epoch_us = int(
                    (
                        datetime.now(UTC)
                        - timedelta(seconds=COLD_COMPRESSION_HOT_SECONDS)
                    ).timestamp()
                    * 1_000_000
                )
                candidates = self.metadata.cold_compression_candidates(
                    cutoff_epoch_us=cutoff_epoch_us,
                    limit=1,
                )
                if not candidates:
                    self._update_cold_compression_status(
                        state="idle-hot-window",
                        lastError="",
                    )
                    self._shutdown.wait(COLD_COMPRESSION_IDLE_SECONDS)
                    continue

                candidate = candidates[0]
                with self._cold_boundary_lock:
                    allowed, state = self._cold_compression_gate(settings)
                    if not allowed:
                        self._update_cold_compression_status(state=state)
                        continue
                    self._update_cold_compression_status(
                        running=True,
                        state="converting",
                        currentPart=Path(str(candidate["path"])).name,
                        lastError="",
                    )

                try:
                    result = self.storage.recompress_part_zstd9(
                        candidate,
                        archive,
                    )
                except Exception as exc:
                    self.metadata.record_part_compression_failure(
                        str(candidate["path"]),
                        expected_object_sha256=str(
                            candidate.get("object_sha256")
                            or candidate.get("sha256")
                            or ""
                        ),
                        error=str(exc),
                    )
                    self._update_cold_compression_status(
                        running=False,
                        state="error",
                        currentPart="",
                        lastError=str(exc),
                    )
                    LOGGER.exception("Cold Parquet compression failed")
                else:
                    with self._state_lock:
                        converted = int(
                            self._cold_compression_status.get("convertedParts")
                            or 0
                        ) + (1 if result["state"] == "converted" else 0)
                        saved = int(
                            self._cold_compression_status.get("savedBytes") or 0
                        ) + max(int(result.get("saved_bytes") or 0), 0)
                    self._update_cold_compression_status(
                        running=False,
                        state="part-complete",
                        currentPart="",
                        convertedParts=converted,
                        savedBytes=saved,
                        lastDurationSeconds=result.get("duration_seconds"),
                        lastCompletedAt=utc_now_text(),
                        lastError="",
                    )
                    LOGGER.info(
                        "Cold Parquet compression complete: part=%s saved=%s "
                        "duration=%ss",
                        Path(str(candidate["path"])).name,
                        result.get("saved_bytes"),
                        result.get("duration_seconds"),
                    )
            except Exception as exc:
                self._update_cold_compression_status(
                    running=False,
                    state="error",
                    currentPart="",
                    lastError=str(exc),
                )
                LOGGER.exception("Cold compression scheduler failed")
            self._shutdown.wait(COLD_COMPRESSION_IDLE_SECONDS)

    def _scheduler_loop(self) -> None:
        while not self._shutdown.wait(15):
            try:
                settings = self._settings()
                # 保留期清理作用于整个 storage(所有实例的分区)，只能由 primary
                # 执行一次；secondary 一起跑会对同一批分区重复清理。
                if self.role == "primary":
                    self._run_retention_cleanup_if_due(settings)
                # A pause is a durable operator state, not a one-file hint.
                # request_pause() lets the active file reach its atomic commit
                # boundary; after that the scheduler must stay idle until an
                # explicit /api/sync/start calls start(), which clears the flag.
                # Otherwise the next 15-second scheduler pass silently starts a
                # new job and defeats the production safety control.
                if self._pause_after_current.is_set():
                    continue
                if not settings.auto_sync or not settings.db_instance_id:
                    continue
                interval = settings.poll_minutes * 60
                if time.monotonic() - self._last_auto_start < interval:
                    continue
                with self._state_lock:
                    if self._worker and self._worker.is_alive():
                        continue
                try:
                    self.start(reason="auto")
                except PipelineError as exc:
                    LOGGER.warning("Auto sync skipped: %s", exc)
                finally:
                    self._last_auto_start = time.monotonic()
            except Exception:
                LOGGER.exception("Auto sync scheduler failed")

    def _index_loop(self) -> None:
        # Keep startup health/status responsive before the parallel workers
        # begin reading and tokenizing historical objects.
        if self._shutdown.wait(3):
            return
        while not self._shutdown.is_set():
            try:
                settings = self._settings()
                if not settings.oss_enabled:
                    self._index_status.update(
                        {"running": False, "currentPart": ""}
                    )
                    self._shutdown.wait(30)
                    continue
                parts = self.metadata.list_parts(
                    limit=1_000_000,
                    visible_only=False,
                )
                missing = self.storage.search_index.missing_parts(parts, limit=8)
                if not missing:
                    self._index_status.update(
                        {
                            "running": False,
                            "currentPart": "",
                            "currentParts": [],
                            "indexedParts": self.storage.search_index.stats()[
                                "part_count"
                            ],
                            "totalParts": len(parts),
                            "lastError": "",
                        }
                    )
                    self._shutdown.wait(30)
                    continue
                self._index_status.update(
                    {
                        "running": True,
                        "currentPart": Path(str(missing[0]["path"])).name,
                        "currentParts": [
                            Path(str(part["path"])).name for part in missing
                        ],
                        "totalParts": len(parts),
                    }
                )
                archive = self.archive_for_settings(settings)
                indexed_rows = 0
                indexed_row_groups = 0
                released = 0
                errors: list[str] = []
                with ThreadPoolExecutor(
                    max_workers=len(missing),
                    thread_name_prefix="binlog-search-index-worker",
                ) as executor:
                    futures = {
                        executor.submit(
                            self.storage.ensure_part_index,
                            part,
                            archive,
                        ): part
                        for part in missing
                    }
                    for future, part in futures.items():
                        try:
                            result = future.result()
                            indexed_rows += int(result["rows"])
                            indexed_row_groups += int(result["row_groups"])
                            released += self.storage.release_archived_body(part)
                        except Exception as exc:
                            errors.append(
                                f"{Path(str(part['path'])).name}: {exc}"
                            )
                self._index_status.update(
                    {
                        "running": True,
                        "lastError": "；".join(errors[:3]),
                        "lastIndexedRows": indexed_rows,
                        "lastIndexedRowGroups": indexed_row_groups,
                        "releasedLegacyBytes": int(
                            self._index_status.get("releasedLegacyBytes") or 0
                        )
                        + released,
                        "indexedParts": self.storage.search_index.stats()[
                            "part_count"
                        ],
                    }
                )
                if errors:
                    self._shutdown.wait(5)
            except Exception as exc:
                self._index_status.update(
                    {
                        "running": False,
                        "lastError": str(exc),
                    }
                )
                LOGGER.exception("Search index backfill failed")
                self._shutdown.wait(30)

    def _catalog_loop(self) -> None:
        if self._shutdown.wait(3):
            return
        while not self._shutdown.is_set():
            try:
                settings = self._settings()
                if not settings.oss_enabled:
                    self._catalog_status.update(
                        {"running": False, "currentParts": []}
                    )
                    self._shutdown.wait(30)
                    continue
                now = time.monotonic()
                candidates = [
                    part
                    for part in self.metadata.missing_part_catalogs(limit=256)
                    if self._catalog_retry_after.get(str(part["path"]), 0) <= now
                ][:4]
                if not candidates:
                    stats = self.metadata.part_catalog_stats()
                    self._catalog_status.update(
                        {
                            "running": False,
                            "currentParts": [],
                            "catalogedParts": int(stats["cataloged_parts"]),
                            "totalParts": int(stats["total_parts"]),
                            "lastError": "",
                        }
                    )
                    self._shutdown.wait(10)
                    continue
                self._catalog_status.update(
                    {
                        "running": True,
                        "currentParts": [
                            Path(str(part["path"])).name for part in candidates
                        ],
                    }
                )
                archive = self.archive_for_settings(settings)
                rows = 0
                errors: list[str] = []
                with ThreadPoolExecutor(
                    max_workers=len(candidates),
                    thread_name_prefix="binlog-structure-catalog-worker",
                ) as executor:
                    futures = {
                        executor.submit(
                            self.storage.ensure_part_catalog,
                            part,
                            archive,
                        ): part
                        for part in candidates
                    }
                    for future, part in futures.items():
                        try:
                            result = future.result()
                            rows += int(result["rows"])
                            self._catalog_retry_after.pop(str(part["path"]), None)
                        except Exception as exc:
                            self._catalog_retry_after[str(part["path"])] = (
                                time.monotonic() + 300
                            )
                            errors.append(
                                f"{Path(str(part['path'])).name}: {exc}"
                            )
                stats = self.metadata.part_catalog_stats()
                self._catalog_status.update(
                    {
                        "running": True,
                        "lastError": "；".join(errors[:3]),
                        "lastCatalogedRows": rows,
                        "catalogedParts": int(stats["cataloged_parts"]),
                        "totalParts": int(stats["total_parts"]),
                    }
                )
                if errors:
                    self._shutdown.wait(2)
            except Exception as exc:
                self._catalog_status.update(
                    {
                        "running": False,
                        "lastError": str(exc),
                    }
                )
                LOGGER.exception("Structure catalog backfill failed")
                self._shutdown.wait(30)
