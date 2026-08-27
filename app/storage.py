from __future__ import annotations

import copy
import csv
import hashlib
import json
import logging
import os
import shutil
import threading
import time
import uuid
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import ExitStack, contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

try:
    import fcntl
except ImportError:
    fcntl = None

try:
    import msvcrt
except ImportError:
    msvcrt = None

from .analytics_index import AnalyticsIndex
from .clickhouse_query import ClickHouseQueryBackend
from .clickhouse_slowlog import ClickHouseSlowLogQueryBackend
from .rollup_index import DAY_US, HOUR_US, RollupIndex
from .config import Settings, ensure_data_dirs
from .exact_index import ExactIndex
from .maintenance_status import (
    CLICKHOUSE_SLOWLOG_WORKER_STATUS_NAME,
    SLOWLOG_WORKER_STATUS_NAME,
    SUPERVISOR_STATUS_NAME,
    read_json_status,
    write_json_status,
)
from .metadata import MetadataStore, SLOW_LOG_FILE_PREFIX
from .oss_store import OssArchive
from .search_index import SearchIndex
from .slowlog_index import SlowLogIndex


LOGGER = logging.getLogger(__name__)

# 分析请求内即时补建未覆盖分区的时间预算：只用来吃掉「刚好差一两个分区」的
# 小缺口，大缺口一律交给后台索引器，保证分析请求本身始终是秒级的。
_ANALYTICS_SCAN_BUDGET_S = float(
    os.environ.get("RDS_BINLOG_ANALYTICS_SCAN_BUDGET_SECONDS", "2") or 2
)
_STORAGE_STATS_CACHE_SECONDS = max(
    float(os.environ.get("RDS_BINLOG_STORAGE_STATS_CACHE_SECONDS", "2") or 2),
    0.1,
)
_STORAGE_STATS_PERSIST_SECONDS = max(
    float(os.environ.get("RDS_BINLOG_STORAGE_STATS_PERSIST_SECONDS", "300") or 300),
    _STORAGE_STATS_CACHE_SECONDS,
)
_STORAGE_STATS_SNAPSHOT_NAME = "storage-stats-snapshot.json"

EVENT_COLUMNS = (
    "event_id",
    "event_epoch_us",
    "event_time_utc",
    "event_date",
    "instance_id",
    "host_instance_id",
    "source_file_id",
    "source_file_name",
    "raw_event_type",
    "operation",
    "database_name",
    "table_name",
    "table_map_id",
    "schema_version_id",
    "server_id",
    "thread_id",
    "transaction_id",
    "gtid",
    "xid",
    "start_position",
    "end_position",
    "row_index",
    "execution_time_ms",
    "error_code",
    "sql_kind",
    "sql_text",
    "sql_bytes_base64",
    "before_json",
    "after_json",
    "columns_json",
    "row_query",
    "header_epoch_us",
    "commit_epoch_us",
    "txn_last_committed",
    "txn_sequence_number",
    "txn_length_bytes",
    "connection_id",
    "connection_name",
    "database_account",
    "execution_status",
    "error_message",
    "affected_rows",
    "started_epoch_us",
    "finished_epoch_us",
    "batch_id",
    "statement_index",
    "transaction_context_id",
)

# Tabularis 客户端上报的执行事件用这个 raw_event_type 与 binlog/general log 区分。
TABULARIS_AUDIT_EVENT_TYPE = "TABULARIS_AUDIT"

# 慢日志采集器（DAS DescribeSlowLogRecords）写入的事件用这个 raw_event_type，
# 与 binlog / general log / Tabularis 审计区分，供「来源」筛选使用。
SLOW_LOG_EVENT_TYPE = "SLOW_LOG"

AUDIT_COLUMN_DEFAULTS = {
    "connection_id": "",
    "connection_name": "",
    "database_account": "",
    "execution_status": "",
    "error_message": "",
    "affected_rows": 0,
    "started_epoch_us": 0,
    "finished_epoch_us": 0,
    "batch_id": "",
    "statement_index": -1,
    "transaction_context_id": "",
}

QUERY_RESULT_COLUMNS = (
    "event_id",
    "event_epoch_us",
    "instance_id",
    "operation",
    "database_name",
    "table_name",
    "transaction_id",
    "gtid",
    "sql_kind",
    "sql_text",
    "before_json",
    "after_json",
    "source_file_name",
    "host_instance_id",
    "server_id",
    "thread_id",
    "start_position",
    "end_position",
    "row_index",
    "execution_time_ms",
    "error_code",
    "row_query",
    "raw_event_type",
    "connection_id",
    "connection_name",
    "database_account",
    "execution_status",
    "error_message",
    "affected_rows",
    "started_epoch_us",
    "finished_epoch_us",
    "batch_id",
    "statement_index",
    "transaction_context_id",
)

# Prime only a small newest slice before result scanning. Remaining unknown
# parts are indexed lazily in time-descending order, so a common query can stop
# as soon as its newest page is complete instead of warming an entire day.
QUERY_STRUCTURAL_PRIME_LIMIT = 8


def _bounded_query_workers() -> int:
    try:
        configured = int(os.environ.get("RDS_BINLOG_QUERY_SCAN_WORKERS", "4"))
    except ValueError:
        configured = 4
    # A scan materializes decoded Parquet columns. Four concurrent scans leave
    # headroom for sync and HTTP work inside the service's 5 GiB cgroup.
    return min(max(configured, 1), 4)


QUERY_SCAN_WORKERS = _bounded_query_workers()
QUERY_MEMORY_RELEASE_BATCHES = 4
NEGATIVE_PROBE_WRITE_BATCH = 128
POSITIVE_PROBE_WRITE_BATCH = 16
EVENT_DETAIL_PROBE_BATCH_ROWS = 8192
EVENT_DETAIL_VALUE_BATCH_ROWS = 32
QUERY_UNFILTERED_PREFETCH = 2

CREATE_STAGE = """
CREATE TABLE events (
    event_id VARCHAR NOT NULL,
    event_epoch_us BIGINT NOT NULL,
    event_time_utc TIMESTAMP NOT NULL,
    event_date DATE NOT NULL,
    instance_id VARCHAR NOT NULL,
    host_instance_id VARCHAR NOT NULL,
    source_file_id VARCHAR NOT NULL,
    source_file_name VARCHAR NOT NULL,
    raw_event_type VARCHAR NOT NULL,
    operation VARCHAR NOT NULL,
    database_name VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    table_map_id UBIGINT NOT NULL,
    schema_version_id VARCHAR NOT NULL,
    server_id BIGINT NOT NULL,
    thread_id BIGINT NOT NULL,
    transaction_id VARCHAR NOT NULL,
    gtid VARCHAR NOT NULL,
    xid VARCHAR NOT NULL,
    start_position BIGINT NOT NULL,
    end_position BIGINT NOT NULL,
    row_index INTEGER NOT NULL,
    execution_time_ms BIGINT NOT NULL,
    error_code INTEGER NOT NULL,
    sql_kind VARCHAR NOT NULL,
    sql_text VARCHAR NOT NULL,
    sql_bytes_base64 VARCHAR NOT NULL,
    before_json VARCHAR NOT NULL,
    after_json VARCHAR NOT NULL,
    columns_json VARCHAR NOT NULL,
    row_query VARCHAR NOT NULL,
    header_epoch_us BIGINT NOT NULL,
    commit_epoch_us BIGINT NOT NULL,
    txn_last_committed BIGINT NOT NULL,
    txn_sequence_number BIGINT NOT NULL,
    txn_length_bytes BIGINT NOT NULL,
    connection_id VARCHAR NOT NULL,
    connection_name VARCHAR NOT NULL,
    database_account VARCHAR NOT NULL,
    execution_status VARCHAR NOT NULL,
    error_message VARCHAR NOT NULL,
    affected_rows BIGINT NOT NULL,
    started_epoch_us BIGINT NOT NULL,
    finished_epoch_us BIGINT NOT NULL,
    batch_id VARCHAR NOT NULL,
    statement_index INTEGER NOT NULL,
    transaction_context_id VARCHAR NOT NULL
)
"""

PARSER_JSON_COLUMNS = {
    "event_id": "VARCHAR",
    "event_epoch_us": "BIGINT",
    "raw_event_type": "VARCHAR",
    "operation": "VARCHAR",
    "database_name": "VARCHAR",
    "table_name": "VARCHAR",
    "table_map_id": "UBIGINT",
    "schema_version_id": "VARCHAR",
    "server_id": "BIGINT",
    "thread_id": "BIGINT",
    "transaction_id": "VARCHAR",
    "gtid": "VARCHAR",
    "xid": "VARCHAR",
    "start_position": "BIGINT",
    "end_position": "BIGINT",
    "row_index": "INTEGER",
    "execution_time_ms": "BIGINT",
    "error_code": "INTEGER",
    "sql_kind": "VARCHAR",
    "sql_text": "VARCHAR",
    "sql_bytes_base64": "VARCHAR",
    "before_json": "VARCHAR",
    "after_json": "VARCHAR",
    "columns_json": "VARCHAR",
    "row_query": "VARCHAR",
    # 解析器 v2 新增：事件自身时间戳、事务提交时刻与依赖跟踪。旧 NDJSON 缺这些
    # 键时 DuckDB 的 read_json 会填 NULL，落盘前统一 COALESCE 成 0。
    "header_epoch_us": "BIGINT",
    "commit_epoch_us": "BIGINT",
    "txn_last_committed": "BIGINT",
    "txn_sequence_number": "BIGINT",
    "txn_length_bytes": "BIGINT",
    "connection_id": "VARCHAR",
    "connection_name": "VARCHAR",
    "database_account": "VARCHAR",
    "execution_status": "VARCHAR",
    "error_message": "VARCHAR",
    "affected_rows": "BIGINT",
    "started_epoch_us": "BIGINT",
    "finished_epoch_us": "BIGINT",
    "batch_id": "VARCHAR",
    "statement_index": "INTEGER",
    "transaction_context_id": "VARCHAR",
}


class StorageError(RuntimeError):
    def __init__(self, message: str, code: str = "STORAGE_ERROR"):
        super().__init__(message)
        self.code = code


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(4 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _iso_from_epoch_us(value: int) -> tuple[str, str]:
    instant = datetime.fromtimestamp(value / 1_000_000, UTC)
    return (
        instant.strftime("%Y-%m-%d %H:%M:%S.%f"),
        instant.strftime("%Y-%m-%d"),
    )


class _BodyFileLock:
    """Process-local plus cross-process lock for one immutable body path."""

    def __init__(self, local_lock: threading.RLock, lock_path: Path):
        self._local_lock = local_lock
        self._lock_path = lock_path
        self._handle = None

    def acquire(self) -> bool:
        self._local_lock.acquire()
        try:
            self._lock_path.parent.mkdir(parents=True, exist_ok=True)
            handle = self._lock_path.open("a+b")
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            if fcntl is not None:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            elif msvcrt is not None:
                msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            self._handle = handle
            return True
        except Exception:
            if "handle" in locals():
                handle.close()
            self._local_lock.release()
            raise

    def release(self) -> None:
        handle = self._handle
        self._handle = None
        try:
            if handle is not None:
                try:
                    handle.seek(0)
                    if fcntl is not None:
                        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
                    elif msvcrt is not None:
                        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                finally:
                    handle.close()
        finally:
            self._local_lock.release()

    def __enter__(self) -> "_BodyFileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        self.release()


class EventStorage:
    def __init__(self, metadata: MetadataStore, data_dir: Path | None = None):
        self.metadata = metadata
        self.paths = ensure_data_dirs(data_dir)
        self.search_index = SearchIndex(self.paths["index"] / "search.sqlite3")
        self.exact_index = ExactIndex(self.paths["index"] / "exact-v1")
        self.analytics_index = AnalyticsIndex(
            self.paths["index"] / "analytics-v1",
            connect_duckdb=self._duckdb_connect,
        )
        self.rollup_index = RollupIndex(
            str(self.analytics_index.manifest_path)
        )
        self.slowlog_index = SlowLogIndex(
            self.paths["index"] / "slowlog.sqlite3",
            run_migrations=bool(getattr(metadata, "run_migrations", True)),
        )
        self._local_cache_lock = threading.RLock()
        self._local_cache_enforce_lock = threading.Lock()
        self._part_body_locks = [threading.RLock() for _ in range(256)]
        self._query_cache_lock = threading.RLock()
        self._query_cache_stats_cache: dict[str, int] | None = None
        self._query_flights_lock = threading.Lock()
        self._query_flights: dict[str, Future[dict[str, Any]]] = {}
        self._query_scan_slots = threading.BoundedSemaphore(QUERY_SCAN_WORKERS)
        self._slowlog_fallback_lock = threading.Lock()
        self._query_activity_lock = threading.Lock()
        self._active_queries = 0
        self._last_query_finished_monotonic = 0.0
        self._local_part_cache: dict[
            str, tuple[dict[str, Any], Path, int]
        ] | None = None
        # Runtime construction is read-only. Schema creation is deliberately
        # restricted to app.clickhouse_migrate so service startup never runs DDL.
        self.clickhouse_backend = ClickHouseQueryBackend.from_env(
            self.metadata,
            self.paths["root"],
        )
        self.clickhouse_slowlog_backend = (
            ClickHouseSlowLogQueryBackend.from_env(
                self.metadata,
                self.paths["root"],
                statement_index=self.slowlog_index,
            )
        )
        self._storage_stats_lock = threading.Lock()
        self._storage_stats_ready = threading.Event()
        self._storage_stats_snapshot: dict[str, Any] | None = None
        self._storage_stats_retention_days: int | None = None
        self._storage_stats_refreshed_monotonic = 0.0
        self._storage_stats_last_persisted_monotonic = 0.0
        self._storage_stats_refreshing = False
        self._storage_stats_snapshot_path = (
            self.paths["logs"] / _STORAGE_STATS_SNAPSHOT_NAME
        )
        self._load_storage_stats_snapshot()

    @contextmanager
    def query_activity(self) -> Iterable[None]:
        with self._query_activity_lock:
            self._active_queries += 1
        try:
            yield
        finally:
            with self._query_activity_lock:
                self._active_queries = max(self._active_queries - 1, 0)
                self._last_query_finished_monotonic = time.monotonic()

    @contextmanager
    def slowlog_fallback_slot(
        self,
        control: Any | None = None,
    ) -> Iterable[None]:
        while not self._slowlog_fallback_lock.acquire(timeout=0.1):
            if control is not None:
                control.check_cancelled()
        try:
            if control is not None:
                control.check_cancelled()
            yield
        finally:
            self._slowlog_fallback_lock.release()

    def has_query_pressure(self, *, grace_seconds: float = 30.0) -> bool:
        with self._query_activity_lock:
            active = self._active_queries
            last_finished = self._last_query_finished_monotonic
        if active > 0:
            return True
        grace = max(float(grace_seconds), 0.0)
        return bool(
            grace
            and last_finished
            and time.monotonic() - last_finished < grace
        )

    def query_activity_status(self) -> dict[str, Any]:
        with self._query_activity_lock:
            active = int(self._active_queries)
            last_finished = self._last_query_finished_monotonic
        recent = bool(
            active
            or (
                last_finished
                and time.monotonic() - last_finished < 30.0
            )
        )
        return {"activeQueries": active, "recentQueryPressure": recent}

    def _duckdb_connect(
        self,
        database: str = ":memory:",
    ) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(database)
        try:
            conn.execute("SET threads = 1")
            conn.execute("SET preserve_insertion_order = false")
            conn.execute(
                "SET temp_directory = "
                + _sql_string(str(self.paths["scratch"].resolve()))
            )
        except Exception:
            conn.close()
            raise
        return conn

    def _part_body_lock(self, path: Path) -> _BodyFileLock:
        shard = self._body_lock_shard(path)
        local_lock = self._part_body_locks[shard]
        return _BodyFileLock(
            local_lock,
            self.paths["locks"] / f"body-{shard:03d}.lock",
        )

    @staticmethod
    def _body_lock_digest(path: Path) -> str:
        return hashlib.sha256(str(path.resolve()).encode("utf-8")).hexdigest()

    def _body_lock_shard(self, path: Path) -> int:
        digest = self._body_lock_digest(path)
        return int(digest[:8], 16) % len(self._part_body_locks)

    def _body_version_path(self, path: Path) -> Path:
        return self.paths["locks"] / f"{self._body_lock_digest(path)}.version"

    def _write_body_version(self, path: Path, sha256: str) -> None:
        version_path = self._body_version_path(path)
        temporary = version_path.with_name(
            f".{version_path.name}.{uuid.uuid4().hex[:12]}.tmp"
        )
        temporary.write_text(str(sha256), encoding="ascii")
        os.replace(temporary, version_path)

    def _read_body_version(self, path: Path) -> str:
        try:
            return self._body_version_path(path).read_text(
                encoding="ascii"
            ).strip()
        except FileNotFoundError:
            return ""

    def _remove_body_version(self, path: Path) -> None:
        self._body_version_path(path).unlink(missing_ok=True)

    def _local_body_matches(self, path: Path, part: dict[str, Any]) -> bool:
        if not path.is_file():
            return False
        expected_size = int(part.get("size_bytes") or 0)
        if expected_size and path.stat().st_size != expected_size:
            return False
        version = self._read_body_version(path)
        return not version or version == str(part.get("sha256") or "")

    def _remember_local_parts(self, parts: Iterable[dict[str, Any]]) -> None:
        with self._local_cache_lock:
            if self._local_part_cache is None:
                return
            for part in parts:
                path = Path(str(part["path"]))
                if path.is_file():
                    self._local_part_cache[str(path)] = (
                        dict(part),
                        path,
                        path.stat().st_size,
                    )

    def _forget_local_part(self, path: Path) -> None:
        with self._local_cache_lock:
            if self._local_part_cache is not None:
                self._local_part_cache.pop(str(path), None)

    def _invalidate_local_part_cache(self) -> None:
        with self._local_cache_lock:
            self._local_part_cache = None

    def _local_body_entries(
        self,
    ) -> list[tuple[dict[str, Any], Path, int]]:
        with self._local_cache_lock:
            if self._local_part_cache is None:
                rebuilt: dict[str, tuple[dict[str, Any], Path, int]] = {}
                for path in self.paths["events"].rglob("*.parquet"):
                    try:
                        if not path.is_file():
                            continue
                        part = self.metadata.part_by_path(str(path))
                        if part is None:
                            continue
                        rebuilt[str(path)] = (part, path, path.stat().st_size)
                    except OSError:
                        continue
                self._local_part_cache = rebuilt

            entries: list[tuple[dict[str, Any], Path, int]] = []
            stale: list[str] = []
            for path_text, (part, path, _cached_size) in (
                self._local_part_cache.items()
            ):
                try:
                    if not path.is_file():
                        stale.append(path_text)
                        continue
                    entries.append((dict(part), path, path.stat().st_size))
                except OSError:
                    stale.append(path_text)
            for path_text in stale:
                self._local_part_cache.pop(path_text, None)

        entries.sort(
            key=lambda entry: (
                int(entry[0].get("max_event_epoch_us") or 0),
                str(entry[1]),
            ),
            reverse=True,
        )
        return entries

    def local_body_parts(self) -> list[dict[str, Any]]:
        return [part for part, _path, _size in self._local_body_entries()]

    def note_part_archived(
        self,
        path_text: str,
        oss_key: str,
        *,
        oss_etag: str = "",
        oss_offset: int = 0,
        oss_length: int = 0,
        oss_object_sha256: str = "",
    ) -> None:
        with self._local_cache_lock:
            if self._local_part_cache is None:
                return
            cached = self._local_part_cache.get(path_text)
            if cached is None:
                return
            part, path, size = cached
            part["oss_key"] = oss_key
            part["oss_etag"] = oss_etag
            part["oss_offset"] = max(int(oss_offset), 0)
            part["oss_length"] = max(int(oss_length), 0)
            part["oss_object_sha256"] = str(oss_object_sha256 or "")
            self._local_part_cache[path_text] = (part, path, size)

    @staticmethod
    def _catalog_from_relation(
        conn: duckdb.DuckDBPyConnection,
        relation: str,
        *,
        where: str = "",
        parameters: list[Any] | None = None,
    ) -> dict[str, list[str]]:
        row = conn.execute(
            "SELECT "
            "array_agg(DISTINCT lower(database_name) "
            "ORDER BY lower(database_name)) "
            "FILTER (WHERE trim(database_name) <> ''), "
            "array_agg(DISTINCT lower(table_name) "
            "ORDER BY lower(table_name)) "
            "FILTER (WHERE trim(table_name) <> ''), "
            "array_agg(DISTINCT upper(operation) "
            "ORDER BY upper(operation)) "
            "FILTER (WHERE trim(operation) <> '') "
            f"FROM {relation} {where}",
            parameters or [],
        ).fetchone()
        values = row or (None, None, None)
        return {
            key: [str(value) for value in (items or [])]
            for key, items in zip(
                ("databases", "tables", "operations"),
                values,
                strict=True,
            )
        }

    @staticmethod
    def _parquet_footer_stats(path: Path) -> tuple[int, int, int] | None:
        parquet = pq.ParquetFile(path)
        metadata = parquet.metadata
        if metadata is None or int(metadata.num_rows) <= 0:
            return None
        column_index = next(
            (
                index
                for index in range(metadata.num_columns)
                if metadata.schema.column(index).name == "event_epoch_us"
            ),
            None,
        )
        if column_index is None:
            return None
        minimums: list[int] = []
        maximums: list[int] = []
        for row_group_index in range(metadata.num_row_groups):
            statistics = metadata.row_group(row_group_index).column(
                column_index
            ).statistics
            if (
                statistics is None
                or not statistics.has_min_max
                or statistics.min is None
                or statistics.max is None
            ):
                return None
            minimums.append(int(statistics.min))
            maximums.append(int(statistics.max))
        if not minimums:
            return None
        return int(metadata.num_rows), min(minimums), max(maximums)

    def _catalog_for_path(self, path: Path) -> dict[str, list[str]]:
        conn = self._duckdb_connect()
        try:
            relation = f"read_parquet({_sql_string(str(path))})"
            return self._catalog_from_relation(conn, relation)
        finally:
            conn.close()

    def _record_tuple(
        self,
        value: dict[str, Any],
        *,
        instance_id: str,
        host_instance_id: str,
        source_file_id: str,
        source_file_name: str,
    ) -> tuple[Any, ...]:
        epoch_us = int(value.get("event_epoch_us") or 0)
        if epoch_us <= 0:
            raise StorageError("解析记录缺少有效事件时间", "EVENT_TIME_MISSING")
        event_time, event_date = _iso_from_epoch_us(epoch_us)
        event_id = str(value.get("event_id") or "")
        if not event_id:
            stable = "\x1f".join(
                (
                    source_file_id,
                    str(value.get("start_position") or 0),
                    str(value.get("end_position") or 0),
                    str(value.get("row_index") or 0),
                    str(value.get("operation") or ""),
                )
            )
            event_id = hashlib.sha256(stable.encode("utf-8")).hexdigest()
        return (
            event_id,
            epoch_us,
            event_time,
            event_date,
            instance_id,
            host_instance_id,
            source_file_id,
            source_file_name,
            str(value.get("raw_event_type") or ""),
            str(value.get("operation") or "OTHER").upper(),
            str(value.get("database_name") or ""),
            str(value.get("table_name") or ""),
            int(value.get("table_map_id") or 0),
            str(value.get("schema_version_id") or ""),
            int(value.get("server_id") or 0),
            int(value.get("thread_id") or 0),
            str(value.get("transaction_id") or ""),
            str(value.get("gtid") or ""),
            str(value.get("xid") or ""),
            int(value.get("start_position") or 0),
            int(value.get("end_position") or 0),
            int(value.get("row_index") or 0),
            int(value.get("execution_time_ms") or 0),
            int(value.get("error_code") or 0),
            str(value.get("sql_kind") or ""),
            str(value.get("sql_text") or ""),
            str(value.get("sql_bytes_base64") or ""),
            str(value.get("before_json") or ""),
            str(value.get("after_json") or ""),
            str(value.get("columns_json") or ""),
            str(value.get("row_query") or ""),
            # 解析器 v2 字段；旧解析器输出缺这些键时补 0，schema 保持一致。
            int(value.get("header_epoch_us") or 0),
            int(value.get("commit_epoch_us") or 0),
            int(value.get("txn_last_committed") or 0),
            int(value.get("txn_sequence_number") or 0),
            int(value.get("txn_length_bytes") or 0),
            str(value.get("connection_id") or ""),
            str(value.get("connection_name") or ""),
            str(value.get("database_account") or ""),
            str(value.get("execution_status") or ""),
            str(value.get("error_message") or ""),
            int(value.get("affected_rows") or 0),
            int(value.get("started_epoch_us") or 0),
            int(value.get("finished_epoch_us") or 0),
            str(value.get("batch_id") or ""),
            int(value.get("statement_index") if value.get("statement_index") is not None else -1),
            str(value.get("transaction_context_id") or ""),
        )

    def _finish_stage(
        self,
        conn: duckdb.DuckDBPyConnection,
        *,
        file_id: str,
        count: int,
        moved: list[Path],
        part_key: str = "",
        append: bool = False,
        publish_metadata: bool = True,
    ) -> tuple[int, list[dict[str, Any]]]:
        if count == 0:
            if publish_metadata and not append:
                self.metadata.replace_parts(file_id, [])
            return 0, []
        dates = [
            str(row[0])
            for row in conn.execute(
                "SELECT DISTINCT event_date FROM events ORDER BY event_date"
            ).fetchall()
        ]
        parts: list[dict[str, Any]] = []
        prepared: list[tuple[Path, Path, str]] = []
        for event_date in dates:
            catalog = self._catalog_from_relation(
                conn,
                "events",
                where="WHERE event_date = ?",
                parameters=[event_date],
            )
            final_dir = self.paths["events"] / f"event_date={event_date}"
            final_dir.mkdir(parents=True, exist_ok=True)
            part_suffix = f"-{part_key}" if part_key else ""
            final_path = final_dir / f"{file_id}{part_suffix}.parquet"
            # Keep the same-directory atomic replace without exceeding the
            # traditional Windows path limit in deeply nested workspaces.
            temp_path = final_dir / f".new-{uuid.uuid4().hex[:12]}.parquet"
            query = (
                "COPY (SELECT * FROM events WHERE event_date = "
                + _sql_string(event_date)
                + " ORDER BY lower(database_name), lower(table_name), operation, "
                "floor(event_epoch_us / 300000000), event_epoch_us, "
                "end_position, row_index) TO "
                + _sql_string(str(temp_path))
                + " (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 1, "
                "ROW_GROUP_SIZE 8192, KV_METADATA {app: 'RDS Binlog Insight', "
                "schema_version: '2', layout: 'time-table-operation-rowgroup'})"
            )
            conn.execute(query)
            verify = self._parquet_footer_stats(temp_path)
            if verify is None:
                verify = conn.execute(
                    "SELECT count(*), min(event_epoch_us), max(event_epoch_us) "
                    f"FROM read_parquet({_sql_string(str(temp_path))})"
                ).fetchone()
            if not verify or int(verify[0]) <= 0:
                raise StorageError("Parquet 回读计数失败", "PARQUET_VERIFY_FAILED")
            part = {
                "path": str(final_path.resolve()),
                "event_date": event_date,
                "row_count": int(verify[0]),
                "min_event_epoch_us": int(verify[1]),
                "max_event_epoch_us": int(verify[2]),
                "size_bytes": temp_path.stat().st_size,
                "sha256": _sha256(temp_path),
                "catalog": catalog,
            }
            # Publish the verified Parquet and its filter catalog first.  The
            # background indexer later builds the Row Group index. The local
            # copy keeps new events queryable immediately and is released as
            # soon as the immutable OSS object is verified.
            parts.append(part)
            prepared.append((temp_path, final_path, str(part["sha256"])))
        if sum(part["row_count"] for part in parts) != count:
            raise StorageError("Parquet 分区总行数与解析行数不一致", "ROW_COUNT_MISMATCH")
        # The indexer runs in a separate container. Hold shared, path-stable
        # locks from the atomic body replacement through the metadata commit,
        # so a stale archived snapshot can never release a newer generation.
        with ExitStack() as body_locks:
            locked_shards: set[int] = set()
            for _, final_path, _ in sorted(
                prepared,
                key=lambda item: str(item[1]),
            ):
                shard = self._body_lock_shard(final_path)
                if shard in locked_shards:
                    continue
                body_locks.enter_context(self._part_body_lock(final_path))
                locked_shards.add(shard)
            for temp_path, final_path, sha256 in prepared:
                os.replace(temp_path, final_path)
                moved.append(final_path)
                self._write_body_version(final_path, sha256)
            if publish_metadata:
                self.publish_ingested_parts(file_id, parts, append=append)
            return count, parts

    def publish_ingested_parts(
        self,
        file_id: str,
        parts: list[dict[str, Any]],
        *,
        append: bool,
    ) -> None:
        if append:
            committed_parts = self.metadata.upsert_parts(file_id, parts)
        else:
            committed_parts = self.metadata.replace_parts(file_id, parts)
        # An identical retry intentionally preserves the already verified OSS
        # locator in MetadataStore. Reflect that committed state in the values
        # returned to the pipeline before the body lock is released. Otherwise
        # the index worker may correctly release the redundant local body while
        # the pipeline, holding a stale locator-free dict, incorrectly queues a
        # second upload of the now-missing file.
        archive_fields = (
            "logical_part_id",
            "object_sha256",
            "compression_level",
            "compression_updated_at",
            "oss_key",
            "oss_etag",
            "oss_offset",
            "oss_length",
            "oss_object_sha256",
            "oss_uploaded_at",
            "oss_verified_at",
        )
        for part in parts:
            committed = committed_parts.get(str(part["path"]))
            if (
                committed
                and str(committed.get("sha256") or "")
                == str(part.get("sha256") or "")
            ):
                for field in archive_fields:
                    part[field] = committed.get(field)
        self._remember_local_parts(parts)

    def ingest_file(
        self,
        *,
        file_id: str,
        instance_id: str,
        host_instance_id: str,
        source_file_name: str,
        events: Iterable[dict[str, Any]],
    ) -> tuple[int, list[dict[str, Any]]]:
        stage_path = self.paths["staging"] / f"{file_id}.duckdb"
        if stage_path.exists():
            stage_path.unlink()
        conn = self._duckdb_connect(str(stage_path))
        moved: list[Path] = []
        try:
            conn.execute(CREATE_STAGE)
            row_placeholders = "(" + ",".join("?" for _ in EVENT_COLUMNS) + ")"
            batch: list[tuple[Any, ...]] = []
            count = 0

            def flush_batch() -> int:
                if not batch:
                    return 0
                values_sql = ",".join(row_placeholders for _ in batch)
                parameters = [value for row in batch for value in row]
                conn.execute(f"INSERT INTO events VALUES {values_sql}", parameters)
                inserted = len(batch)
                batch.clear()
                return inserted

            for value in events:
                batch.append(
                    self._record_tuple(
                        value,
                        instance_id=instance_id,
                        host_instance_id=host_instance_id,
                        source_file_id=file_id,
                        source_file_name=source_file_name,
                    )
                )
                if len(batch) >= 500:
                    count += flush_batch()
            count += flush_batch()
            return self._finish_stage(
                conn, file_id=file_id, count=count, moved=moved
            )
        except Exception:
            for path in moved:
                # Deterministic task-owned paths are overwritten on the next retry.
                if path.exists():
                    try:
                        path.unlink()
                        self._remove_body_version(path)
                    except OSError:
                        pass
            raise
        finally:
            conn.close()
            if stage_path.exists():
                try:
                    stage_path.unlink()
                except OSError:
                    pass

    def ingest_ndjson_file(
        self,
        *,
        file_id: str,
        instance_id: str,
        host_instance_id: str,
        source_file_name: str,
        ndjson_path: Path,
        part_key: str = "",
        append: bool = False,
        publish_metadata: bool = True,
    ) -> tuple[int, list[dict[str, Any]]]:
        if not ndjson_path.is_file():
            raise StorageError(
                f"解析器输出文件不存在：{ndjson_path.name}",
                "PARSER_OUTPUT_MISSING",
            )
        conn = self._duckdb_connect()
        moved: list[Path] = []
        try:
            columns_sql = "{" + ",".join(
                f"{name}: {_sql_string(data_type)}"
                for name, data_type in PARSER_JSON_COLUMNS.items()
            ) + "}"
            source_sql = (
                f"read_json({_sql_string(str(ndjson_path))}, "
                f"format='newline_delimited', columns={columns_sql})"
            )
            separator = "chr(31)"
            fallback_event_id = (
                "sha256(concat("
                + _sql_string(file_id)
                + f", {separator}, coalesce(cast(start_position AS VARCHAR), '0'), "
                + f"{separator}, coalesce(cast(end_position AS VARCHAR), '0'), "
                + f"{separator}, coalesce(cast(row_index AS VARCHAR), '0'), "
                + f"{separator}, coalesce(operation, '')))"
            )
            conn.execute(
                "CREATE TABLE events AS SELECT "
                f"coalesce(nullif(event_id, ''), {fallback_event_id})::VARCHAR AS event_id, "
                "coalesce(event_epoch_us, 0)::BIGINT AS event_epoch_us, "
                "make_timestamp(coalesce(event_epoch_us, 0))::TIMESTAMP AS event_time_utc, "
                "cast(make_timestamp(coalesce(event_epoch_us, 0)) AS DATE) AS event_date, "
                f"{_sql_string(instance_id)}::VARCHAR AS instance_id, "
                f"{_sql_string(host_instance_id)}::VARCHAR AS host_instance_id, "
                f"{_sql_string(file_id)}::VARCHAR AS source_file_id, "
                f"{_sql_string(source_file_name)}::VARCHAR AS source_file_name, "
                "coalesce(raw_event_type, '')::VARCHAR AS raw_event_type, "
                "upper(coalesce(nullif(operation, ''), 'OTHER'))::VARCHAR AS operation, "
                "coalesce(database_name, '')::VARCHAR AS database_name, "
                "coalesce(table_name, '')::VARCHAR AS table_name, "
                "coalesce(table_map_id, 0)::UBIGINT AS table_map_id, "
                "coalesce(schema_version_id, '')::VARCHAR AS schema_version_id, "
                "coalesce(server_id, 0)::BIGINT AS server_id, "
                "coalesce(thread_id, 0)::BIGINT AS thread_id, "
                "coalesce(transaction_id, '')::VARCHAR AS transaction_id, "
                "coalesce(gtid, '')::VARCHAR AS gtid, "
                "coalesce(xid, '')::VARCHAR AS xid, "
                "coalesce(start_position, 0)::BIGINT AS start_position, "
                "coalesce(end_position, 0)::BIGINT AS end_position, "
                "coalesce(row_index, 0)::INTEGER AS row_index, "
                "coalesce(execution_time_ms, 0)::BIGINT AS execution_time_ms, "
                "coalesce(error_code, 0)::INTEGER AS error_code, "
                "coalesce(sql_kind, '')::VARCHAR AS sql_kind, "
                "coalesce(sql_text, '')::VARCHAR AS sql_text, "
                "coalesce(sql_bytes_base64, '')::VARCHAR AS sql_bytes_base64, "
                "coalesce(before_json, '')::VARCHAR AS before_json, "
                "coalesce(after_json, '')::VARCHAR AS after_json, "
                "coalesce(columns_json, '')::VARCHAR AS columns_json, "
                "coalesce(row_query, '')::VARCHAR AS row_query, "
                "coalesce(header_epoch_us, 0)::BIGINT AS header_epoch_us, "
                "coalesce(commit_epoch_us, 0)::BIGINT AS commit_epoch_us, "
                "coalesce(txn_last_committed, 0)::BIGINT AS txn_last_committed, "
                "coalesce(txn_sequence_number, 0)::BIGINT AS txn_sequence_number, "
                "coalesce(txn_length_bytes, 0)::BIGINT AS txn_length_bytes, "
                "coalesce(connection_id, '')::VARCHAR AS connection_id, "
                "coalesce(connection_name, '')::VARCHAR AS connection_name, "
                "coalesce(database_account, '')::VARCHAR AS database_account, "
                "coalesce(execution_status, '')::VARCHAR AS execution_status, "
                "coalesce(error_message, '')::VARCHAR AS error_message, "
                "coalesce(affected_rows, 0)::BIGINT AS affected_rows, "
                "coalesce(started_epoch_us, 0)::BIGINT AS started_epoch_us, "
                "coalesce(finished_epoch_us, 0)::BIGINT AS finished_epoch_us, "
                "coalesce(batch_id, '')::VARCHAR AS batch_id, "
                "coalesce(statement_index, -1)::INTEGER AS statement_index, "
                "coalesce(transaction_context_id, '')::VARCHAR AS transaction_context_id "
                f"FROM {source_sql}"
            )
            counts = conn.execute(
                "SELECT count(*), "
                "count(*) FILTER (WHERE event_epoch_us <= 0) FROM events"
            ).fetchone()
            count = int(counts[0])
            invalid_count = int(counts[1])
            if invalid_count:
                raise StorageError(
                    f"解析记录中有 {invalid_count} 条缺少有效事件时间",
                    "EVENT_TIME_MISSING",
                )
            return self._finish_stage(
                conn,
                file_id=file_id,
                count=count,
                moved=moved,
                part_key=part_key,
                append=append,
                publish_metadata=publish_metadata,
            )
        except Exception:
            for path in moved:
                if path.exists():
                    try:
                        path.unlink()
                        self._remove_body_version(path)
                    except OSError:
                        pass
            raise
        finally:
            conn.close()

    def finalize_file_parts(self, file_id: str, keep_paths: set[str]) -> int:
        removed = 0
        for part in self.metadata.parts_for_file(file_id):
            path_text = str(part["path"])
            if path_text in keep_paths:
                continue
            path = Path(path_text)
            with self._part_body_lock(path):
                self.search_index.remove_part(path_text)
                self.slowlog_index.remove_path(path_text)
                self.metadata.delete_part(path_text)
                if path.exists():
                    try:
                        path.unlink()
                        self._remove_body_version(path)
                        self._forget_local_part(path)
                    except OSError as exc:
                        raise StorageError(
                            f"清理旧分块失败：{path.name}：{exc}",
                            "STALE_PART_DELETE_FAILED",
                        ) from exc
            removed += 1
        return removed

    def _dataset_sql(self, paths: Iterable[str] | None = None) -> str | None:
        candidates = list(paths) if paths is not None else self.metadata.part_paths()
        existing = [path for path in candidates if Path(path).is_file()]
        if not existing:
            return None
        files = "[" + ",".join(_sql_string(path) for path in existing) + "]"
        return f"read_parquet({files}, union_by_name=true)"

    @staticmethod
    def _filters(
        query: dict[str, Any], retention_days: int
    ) -> tuple[str, list[Any]]:
        cutoff = int(
            (datetime.now(UTC) - timedelta(days=retention_days)).timestamp() * 1_000_000
        )
        clauses = ["event_epoch_us >= ?"]
        params: list[Any] = [cutoff]
        if query.get("start_epoch_us"):
            clauses.append("event_epoch_us >= ?")
            params.append(int(query["start_epoch_us"]))
        if query.get("end_epoch_us"):
            clauses.append("event_epoch_us <= ?")
            params.append(int(query["end_epoch_us"]))
        source = str(query.get("source") or "").strip().lower()
        if source == "audit":
            clauses.append("raw_event_type = ?")
            params.append(TABULARIS_AUDIT_EVENT_TYPE)
        elif source == "database":
            clauses.append("raw_event_type <> ?")
            params.append(TABULARIS_AUDIT_EVENT_TYPE)
        elif source == "slowlog":
            clauses.append("raw_event_type = ?")
            params.append(SLOW_LOG_EVENT_TYPE)
        elif source == "binlog":
            clauses.append("raw_event_type NOT IN (?, ?)")
            params.extend([TABULARIS_AUDIT_EVENT_TYPE, SLOW_LOG_EVENT_TYPE])
        # 事务钻取：按 GTID / XID / transaction_id 精确匹配。必须走等值而不是
        # keyword 的 LIKE——同一条 GTID 用 keyword 搜实测 34.7 秒（8 个列各做一次
        # 模糊匹配，parquet 统计信息用不上），等值匹配才能靠 min/max 裁掉分区。
        transaction = str(query.get("transaction") or "").strip()
        if transaction:
            # 只用 transaction_id 与 gtid：xid 虽在 EVENT_COLUMNS 里，但查询投影
            # 不含它，写进来会报 Binder Error: Referenced column "xid" not found。
            clauses.append("(transaction_id = ? OR gtid = ?)")
            params.extend([transaction] * 2)
        for key, columns in (
            ("instance", ("instance_id",)),
            # 界面上能看到的是连接名，连接 ID 只在事件详情里出现；两列都匹配，
            # 否则按名字筛选永远返回 0 行。
            ("connection", ("connection_id", "connection_name")),
            ("account", ("database_account",)),
            ("database", ("database_name",)),
            ("table", ("table_name",)),
            ("status", ("execution_status",)),
        ):
            value = str(query.get(key) or "").strip()
            if value:
                if key in {"instance", "status"} or isinstance(query.get("exact"), dict):
                    predicate, needle = "lower({column}) = ?", value.lower()
                else:
                    predicate, needle = (
                        "lower({column}) LIKE ?",
                        "%" + value.lower() + "%",
                    )
                clauses.append(
                    "("
                    + " OR ".join(predicate.format(column=column) for column in columns)
                    + ")"
                )
                params.extend([needle] * len(columns))
        operations = [
            str(value).upper()
            for value in (query.get("operations") or [])
            if str(value).strip()
        ]
        if operations:
            clauses.append(
                "operation IN (" + ",".join("?" for _ in operations) + ")"
            )
            params.extend(operations)
        keyword = str(query.get("keyword") or "").strip()
        if keyword:
            terms = [part for part in keyword.split() if part][:20]
            joiner = " OR " if str(query.get("keyword_mode")).upper() == "OR" else " AND "
            term_clauses = []
            for term in terms:
                term_clauses.append(
                    "("
                    + " OR ".join(
                        f"lower({column}) LIKE ?"
                        for column in (
                            "sql_text",
                            "before_json",
                            "after_json",
                            "transaction_id",
                            "source_file_name",
                            "connection_name",
                            "database_account",
                            "error_message",
                        )
                    )
                    + ")"
                )
                params.extend(["%" + term.lower() + "%"] * 8)
            clauses.append("(" + joiner.join(term_clauses) + ")")
        return " AND ".join(clauses), params

    def _query_paths(
        self,
        paths: Iterable[str] | None,
        query: dict[str, Any],
        retention_days: int,
        *,
        limit_cap: int = 1000,
    ) -> dict[str, Any]:
        dataset = self._dataset_sql(paths)
        limit = min(max(int(query.get("limit") or 100), 1), limit_cap)
        offset = min(max(int(query.get("offset") or 0), 0), 100_000)
        if not dataset:
            return {"rows": [], "has_more": False, "limit": limit, "offset": offset}
        where, params = self._filters(query, retention_days)
        columns = (
            "event_id, event_epoch_us, instance_id, strftime(event_time_utc, "
            "'%Y-%m-%dT%H:%M:%S.%fZ') AS event_time_utc, operation, "
            "database_name, table_name, transaction_id, gtid, sql_kind, sql_text, "
            "before_json, after_json, source_file_name, host_instance_id, "
            "server_id, thread_id, start_position, end_position, row_index, "
            "execution_time_ms, error_code, row_query, raw_event_type, "
            "connection_id, connection_name, database_account, execution_status, "
            "error_message, affected_rows, started_epoch_us, finished_epoch_us, "
            "batch_id, statement_index, transaction_context_id"
        )
        sql = (
            f"SELECT {columns} FROM {dataset} WHERE {where} "
            "ORDER BY event_epoch_us DESC, source_file_name DESC, "
            "end_position DESC, row_index DESC, event_id DESC LIMIT ? OFFSET ?"
        )
        params.extend([limit + 1, offset])
        conn = self._duckdb_connect()
        try:
            cursor = conn.execute(sql, params)
            names = [item[0] for item in cursor.description]
            values = cursor.fetchall()
        finally:
            conn.close()
        rows = [dict(zip(names, row, strict=True)) for row in values[:limit]]
        return {
            "rows": rows,
            "has_more": len(values) > limit,
            "limit": limit,
            "offset": offset,
        }

    def query_events(
        self, query: dict[str, Any], retention_days: int
    ) -> dict[str, Any]:
        return self._query_paths(None, query, retention_days)

    @staticmethod
    def _query_window(
        query: dict[str, Any], retention_days: int
    ) -> tuple[int, int]:
        now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
        cutoff_us = int(
            (datetime.now(UTC) - timedelta(days=retention_days)).timestamp()
            * 1_000_000
        )
        start_us = max(int(query.get("start_epoch_us") or cutoff_us), cutoff_us)
        end_us = min(int(query.get("end_epoch_us") or now_us), now_us)
        return start_us, end_us

    def slowlog_query_coverage(
        self, query: dict[str, Any], settings: Settings
    ) -> dict[str, Any]:
        """Check coverage and durably queue any crash-gap without doing I/O."""

        start_us, end_us = self._query_window(query, settings.retention_days)
        parts = self.metadata.parts_in_range(
            start_epoch_us=start_us,
            end_epoch_us=end_us,
            source="slowlog",
            instance=str(query.get("instance") or ""),
        )
        return self._slowlog_coverage_with_repair(parts)

    def _slowlog_coverage_with_repair(
        self,
        parts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Queue missing part identities while keeping heavy work off HTTP."""

        coverage = self.slowlog_index.coverage(parts)
        queued = 0
        if not coverage["complete"]:
            missing = self.slowlog_index.missing_parts(parts, limit=len(parts))
            queued = self.slowlog_index.enqueue_parts(missing)
        return {**coverage, "repair_queued_parts": queued}

    @staticmethod
    def _row_sort_key(row: dict[str, Any]) -> tuple[int, str, int, int, str]:
        return (
            int(row.get("event_epoch_us") or 0),
            str(row.get("source_file_name") or ""),
            int(row.get("end_position") or 0),
            int(row.get("row_index") or 0),
            str(row.get("event_id") or ""),
        )

    @staticmethod
    def _catalog_allows(
        catalog: dict[str, Any],
        query: dict[str, Any],
    ) -> bool:
        database = str(query.get("database") or "").strip().lower()
        if database and not any(
            database in str(value).lower()
            for value in (catalog.get("databases") or [])
        ):
            return False
        table = str(query.get("table") or "").strip().lower()
        if table and not any(
            table in str(value).lower() for value in (catalog.get("tables") or [])
        ):
            return False
        operations = {
            str(value).strip().upper()
            for value in (query.get("operations") or [])
            if str(value).strip()
        }
        if operations and operations.isdisjoint(
            {
                str(value).strip().upper()
                for value in (catalog.get("operations") or [])
            }
        ):
            return False
        return True

    @staticmethod
    def _query_probe_fingerprint(
        query: dict[str, Any],
        part: dict[str, Any],
        start_us: int,
        end_us: int,
    ) -> str:
        effective_start = max(start_us, int(part["min_event_epoch_us"]))
        effective_end = min(end_us, int(part["max_event_epoch_us"]))
        keyword = str(query.get("keyword") or "").strip()
        terms = [value.lower() for value in keyword.split() if value][:20]
        payload = {
            "schema": 4,
            "start_epoch_us": effective_start,
            "end_epoch_us": effective_end,
            "source": str(query.get("source") or "").strip().lower(),
            "database": str(query.get("database") or "").strip().lower(),
            "table": str(query.get("table") or "").strip().lower(),
            "connection": str(query.get("connection") or "").strip().lower(),
            "account": str(query.get("account") or "").strip().lower(),
            "status": str(query.get("status") or "").strip().lower(),
            "operations": sorted(
                {
                    str(value).strip().upper()
                    for value in (query.get("operations") or [])
                    if str(value).strip()
                }
            ),
            "keyword_terms": terms,
            "keyword_mode": (
                "OR"
                if terms and str(query.get("keyword_mode") or "").upper() == "OR"
                else "AND"
            ),
            "transaction": str(query.get("transaction") or "").strip(),
            "exact": query.get("exact"),
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _query_certificate_fingerprint(
        query: dict[str, Any],
        start_us: int,
        end_us: int,
        instance_id: str,
    ) -> str:
        keyword = str(query.get("keyword") or "").strip()
        terms = [value.lower() for value in keyword.split() if value][:20]
        payload = {
            # schema 3：加入 instance 过滤维度，旧证书全部失效（否则带/不带
            # 实例过滤的两个查询会命中同一份缓存结果）。
            "schema": 4,
            "instance_id": str(instance_id or "").strip(),
            "instance": str(query.get("instance") or "").strip(),
            "start_epoch_us": int(start_us),
            "end_epoch_us": int(end_us),
            "source": str(query.get("source") or "").strip().lower(),
            "database": str(query.get("database") or "").strip().lower(),
            "table": str(query.get("table") or "").strip().lower(),
            "connection": str(query.get("connection") or "").strip().lower(),
            "account": str(query.get("account") or "").strip().lower(),
            "status": str(query.get("status") or "").strip().lower(),
            "operations": sorted(
                {
                    str(value).strip().upper()
                    for value in (query.get("operations") or [])
                    if str(value).strip()
                }
            ),
            "keyword_terms": terms,
            "keyword_mode": (
                "OR"
                if terms and str(query.get("keyword_mode") or "").upper() == "OR"
                else "AND"
            ),
            "transaction": str(query.get("transaction") or "").strip(),
            "exact": query.get("exact"),
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _query_certificate_result(
        rows: list[dict[str, Any]],
        *,
        limit: int,
        offset: int,
        start_us: int,
        end_us: int,
        oldest_us: int | None,
        latest_us: int | None,
        token: dict[str, int],
    ) -> dict[str, Any]:
        visible = rows[offset : offset + limit]
        return {
            "rows": visible,
            "has_more": len(rows) > offset + limit,
            "limit": limit,
            "offset": offset,
            "coverage_found": bool(token["part_count"]),
            "tiers_used": ["local-query-certificate"],
            "local_parts_read": 0,
            "oss_parts_read": 0,
            "oss_range_parts_read": 0,
            "oss_temporary_parts_read": 0,
            "range_requests": 0,
            "range_bytes": 0,
            "full_object_fallback_bytes": 0,
            "predicate_row_groups_scanned": 0,
            "predicate_row_groups_selected": 0,
            "candidate_blocks": 0,
            "query_scan_workers": QUERY_SCAN_WORKERS,
            "indexed_parts": 0,
            "structural_indexed_parts": 0,
            "structural_prime_parts": 0,
            "index_unknown_parts": 0,
            "index_skipped_parts": token["part_count"],
            "index_coverage": 1.0,
            "catalog_skipped_parts": 0,
            "negative_probe_skipped_parts": 0,
            "positive_probe_cached_parts": 0,
            "query_cache_parts_read": 0,
            "query_certificate_hit": True,
            "query_certificate_recorded": False,
            "query_certificate_part_count": token["part_count"],
            "query_certificate_rows": len(rows),
            "oss_downloaded_parts": 0,
            "unavailable_parts": 0,
            "range_start_epoch_us": start_us,
            "range_end_epoch_us": end_us,
            "available_start_epoch_us": oldest_us,
            "available_end_epoch_us": latest_us,
        }

    @staticmethod
    def _stats_text(value: Any) -> str:
        if isinstance(value, bytes):
            return value.decode("utf-8", errors="replace")
        return str(value or "")

    def _structural_candidate_row_groups(
        self,
        parquet: pq.ParquetFile,
        groups: list[int],
        query: dict[str, Any] | None,
        part: dict[str, Any] | None = None,
    ) -> tuple[list[int], int]:
        if not query:
            return groups, 0
        table_term = str(query.get("table") or "").strip().lower()
        database_term = str(query.get("database") or "").strip().lower()
        operations = {
            str(value).strip().upper()
            for value in (query.get("operations") or [])
            if str(value).strip()
        }
        if not (table_term or database_term or operations):
            return groups, 0
        start_us = query.get("start_epoch_us")
        end_us = query.get("end_epoch_us")
        # Keep query execution read-only against the shared index database.
        # Structural pruning below reads only this Parquet handle; the external
        # indexer owns all durable index writes.
        names = parquet.schema_arrow.names
        indexes = {name: index for index, name in enumerate(names)}
        selected: list[int] = []
        scanned = 0

        for group_id in groups:
            scanned += 1
            metadata = parquet.metadata.row_group(group_id)

            def statistics(name: str) -> Any | None:
                index = indexes.get(name)
                if index is None:
                    return None
                return metadata.column(index).statistics

            time_stats = statistics("event_epoch_us")
            if time_stats is not None and time_stats.has_min_max:
                if start_us is not None and int(time_stats.max) < int(start_us):
                    continue
                if end_us is not None and int(time_stats.min) > int(end_us):
                    continue
            impossible = False
            for name, term in (
                ("table_name", table_term),
                ("database_name", database_term),
            ):
                if not term:
                    continue
                value_stats = statistics(name)
                if (
                    value_stats is not None
                    and value_stats.has_min_max
                    and value_stats.min == value_stats.max
                    and term not in self._stats_text(value_stats.min).lower()
                ):
                    impossible = True
                    break
            if impossible:
                continue
            operation_stats = statistics("operation")
            if operations and operation_stats is not None and operation_stats.has_min_max:
                minimum = self._stats_text(operation_stats.min).upper()
                maximum = self._stats_text(operation_stats.max).upper()
                if not any(minimum <= value <= maximum for value in operations):
                    continue

            probe_names = [
                name
                for name, enabled in (
                    ("event_epoch_us", start_us is not None or end_us is not None),
                    ("database_name", bool(database_term)),
                    ("table_name", bool(table_term)),
                    ("operation", bool(operations)),
                )
                if enabled and name in indexes
            ]
            if not probe_names:
                selected.append(group_id)
                continue
            probe = parquet.read_row_group(
                group_id,
                columns=probe_names,
                use_threads=True,
            )
            if not probe.num_rows:
                continue
            try:
                mask: Any | None = None

                def combine(condition: Any) -> None:
                    nonlocal mask
                    condition = pc.fill_null(condition, False)
                    mask = condition if mask is None else pc.and_(mask, condition)

                if start_us is not None and "event_epoch_us" in probe.column_names:
                    combine(pc.greater_equal(probe["event_epoch_us"], int(start_us)))
                if end_us is not None and "event_epoch_us" in probe.column_names:
                    combine(pc.less_equal(probe["event_epoch_us"], int(end_us)))
                if database_term and "database_name" in probe.column_names:
                    combine(
                        pc.match_substring(
                            pc.utf8_lower(probe["database_name"]),
                            pattern=database_term,
                        )
                    )
                if table_term and "table_name" in probe.column_names:
                    combine(
                        pc.match_substring(
                            pc.utf8_lower(probe["table_name"]),
                            pattern=table_term,
                        )
                    )
                if operations and "operation" in probe.column_names:
                    combine(
                        pc.is_in(
                            pc.utf8_upper(probe["operation"]),
                            value_set=pa.array(sorted(operations), type=pa.string()),
                        )
                    )
                if mask is None or bool(pc.any(mask).as_py()):
                    selected.append(group_id)
            except Exception:
                # Predicate acceleration must never turn an unknown group into
                # a false negative when an old Parquet schema is encountered.
                selected.append(group_id)
        return selected, scanned

    @staticmethod
    def _with_audit_columns(
        table: pa.Table,
        requested_columns: Iterable[str] | None,
    ) -> pa.Table:
        requested = set(requested_columns or ())
        for name, default in AUDIT_COLUMN_DEFAULTS.items():
            if name in requested and name not in table.column_names:
                value_type = pa.int64() if isinstance(default, int) else pa.string()
                table = table.append_column(
                    name,
                    pa.array([default] * table.num_rows, type=value_type),
                )
        return table

    def _read_part_table(
        self,
        part: dict[str, Any],
        row_group_ids: Iterable[int] | None,
        archive: OssArchive | None,
        *,
        columns: Iterable[str] | None = None,
        predicate_query: dict[str, Any] | None = None,
    ) -> tuple[Any, str, dict[str, int]]:
        path = Path(str(part["path"]))
        requested = (
            sorted({int(value) for value in row_group_ids})
            if row_group_ids is not None
            else None
        )

        selection_stats = {
            "predicate_row_groups_scanned": 0,
            "predicate_row_groups_selected": 0,
        }

        def read_source(source: Any) -> Any:
            parquet = pq.ParquetFile(source)
            groups = (
                list(range(parquet.num_row_groups))
                if requested is None
                else [
                    value
                    for value in requested
                    if 0 <= value < parquet.num_row_groups
                ]
            )
            if requested is None:
                groups, scanned = self._structural_candidate_row_groups(
                    parquet,
                    groups,
                    predicate_query,
                    part,
                )
                selection_stats["predicate_row_groups_scanned"] = scanned
                selection_stats["predicate_row_groups_selected"] = len(groups)
            if not groups:
                if requested is None:
                    selected_schema = parquet.schema_arrow
                    if columns is not None:
                        requested_names = set(columns)
                        selected_schema = pa.schema(
                            [
                                field
                                for field in selected_schema
                                if field.name in requested_names
                            ]
                        )
                    table = pa.Table.from_batches([], schema=selected_schema)
                    return self._with_audit_columns(table, columns)
                raise StorageError(
                    "索引指向的 Parquet Row Group 不存在",
                    "INDEX_ROW_GROUP_INVALID",
                )
            selected = (
                [
                    name
                    for name in columns
                    if name in set(parquet.schema_arrow.names)
                ]
                if columns is not None
                else None
            )
            table = parquet.read_row_groups(
                groups,
                columns=selected,
                use_threads=True,
            )
            return self._with_audit_columns(table, columns)

        with self._part_body_lock(path):
            if self._local_body_matches(path, part):
                return (
                    read_source(path),
                    "local-index",
                    {
                        "range_requests": 0,
                        "range_bytes": 0,
                        "full_object_fallback_bytes": 0,
                        **selection_stats,
                    },
                )
        if archive is None:
            raise StorageError(
                "查询范围仅存在于 OSS，但 OSS 客户端未就绪",
                "OSS_QUERY_UNAVAILABLE",
            )
        reader_factory = getattr(archive, "open_part_reader", None)
        if callable(reader_factory):
            reader = None
            try:
                reader = reader_factory(part)
                table = read_source(reader)
                stats = dict(reader.stats())
                stats["full_object_fallback_bytes"] = 0
                stats.update(selection_stats)
                return table, "oss-range", stats
            except Exception:
                # One evidence-preserving recovery: a transient full-object read.
                pass
            finally:
                if reader is not None:
                    reader.close()
        destination = self.paths["scratch"] / f".query-{uuid.uuid4().hex}.parquet"
        try:
            archive.download_part(part, destination)
            table = read_source(destination)
            return (
                table,
                "oss-temporary",
                {
                    "range_requests": 0,
                    "range_bytes": 0,
                    "full_object_fallback_bytes": destination.stat().st_size,
                    **selection_stats,
                },
            )
        finally:
            destination.unlink(missing_ok=True)

    def _query_arrow_table(
        self,
        table: Any,
        query: dict[str, Any],
        retention_days: int,
        *,
        locator: str,
        limit_cap: int,
    ) -> dict[str, Any]:
        limit = min(max(int(query.get("limit") or 100), 1), limit_cap)
        offset = min(max(int(query.get("offset") or 0), 0), 100_000)
        if int(table.num_rows) == 0:
            return {"rows": [], "has_more": False, "limit": limit, "offset": offset}
        table = self._filter_exact_arrow_table(table, query, retention_days)
        if int(table.num_rows) == 0:
            return {"rows": [], "has_more": False, "limit": limit, "offset": offset}
        where, params = self._filters(query, retention_days)
        columns = (
            "event_id, event_epoch_us, instance_id, strftime(event_time_utc, "
            "'%Y-%m-%dT%H:%M:%S.%fZ') AS event_time_utc, operation, "
            "database_name, table_name, transaction_id, gtid, sql_kind, sql_text, "
            "before_json, after_json, source_file_name, host_instance_id, "
            "server_id, thread_id, start_position, end_position, row_index, "
            "execution_time_ms, error_code, row_query, raw_event_type, "
            "connection_id, connection_name, database_account, execution_status, "
            "error_message, affected_rows, started_epoch_us, finished_epoch_us, "
            "batch_id, statement_index, transaction_context_id"
        )
        params.extend([limit + 1, offset])
        conn = self._duckdb_connect()
        try:
            conn.register("candidate_events", table)
            cursor = conn.execute(
                f"SELECT {columns} FROM candidate_events WHERE {where} "
                "ORDER BY event_epoch_us DESC, source_file_name DESC, "
                "end_position DESC, row_index DESC, event_id DESC "
                "LIMIT ? OFFSET ?",
                params,
            )
            names = [item[0] for item in cursor.description]
            values = cursor.fetchall()
        finally:
            conn.close()
        rows = [dict(zip(names, row, strict=True)) for row in values[:limit]]
        for row in rows:
            row["locator"] = locator
        return {
            "rows": rows,
            "has_more": len(values) > limit,
            "limit": limit,
            "offset": offset,
        }

    def _filter_exact_arrow_table(
        self,
        table: Any,
        query: dict[str, Any],
        retention_days: int,
    ) -> Any:
        exact = query.get("exact")
        if not isinstance(exact, dict) or int(table.num_rows) == 0:
            return table
        required = {
            "event_epoch_us",
            "operation",
            "database_name",
            "table_name",
            "before_json",
            "after_json",
            "columns_json",
        }
        available = set(table.column_names)
        if not required.issubset(available):
            missing = ", ".join(sorted(required - available))
            raise StorageError(
                f"精确扫描缺少主键判定列：{missing}",
                "EXACT_SCHEMA_UNKNOWN",
            )
        database = str(query.get("database") or "").strip().lower()
        table_name = str(query.get("table") or "").strip().lower()
        operations = {
            str(value).strip().upper()
            for value in (query.get("operations") or [])
            if str(value).strip()
        }
        cutoff = int(
            (datetime.now(UTC) - timedelta(days=retention_days)).timestamp()
            * 1_000_000
        )
        start_us = max(int(query.get("start_epoch_us") or cutoff), cutoff)
        end_us = int(
            query.get("end_epoch_us")
            or datetime.now(UTC).timestamp() * 1_000_000
        )
        probe = table.select(sorted(required)).to_pylist()
        selected: list[int] = []
        schema_cache: dict[str, Any] = {}
        for index, row in enumerate(probe):
            epoch = int(row.get("event_epoch_us") or 0)
            operation = str(row.get("operation") or "").upper()
            if epoch < start_us or epoch > end_us:
                continue
            if str(row.get("database_name") or "").lower() != database:
                continue
            if str(row.get("table_name") or "").lower() != table_name:
                continue
            if operations and operation not in operations:
                continue
            matched = self.exact_index.primary_key_match(
                row,
                exact.get("value"),
                schema_cache=schema_cache,
            )
            if matched is None:
                raise StorageError(
                    "目标事件缺少可证明的单列主键 schema，无法执行准确扫描兜底",
                    "EXACT_SCHEMA_UNKNOWN",
                )
            if matched:
                selected.append(index)
        return table.take(pa.array(selected, type=pa.int64()))

    def ensure_part_structural_index(
        self,
        part: dict[str, Any],
        archive: OssArchive | None,
    ) -> dict[str, Any]:
        if self.search_index.is_structural_current(part):
            return {"indexed": 0, "row_groups": 0, "rows": 0}
        path = Path(str(part["path"]))
        reader = None
        temporary: Path | None = None
        body_lock = self._part_body_lock(path)
        body_lock.acquire()
        try:
            current = self.metadata.part_by_path(str(path))
            if (
                not current
                or str(current.get("sha256") or "")
                != str(part.get("sha256") or "")
            ):
                return {"indexed": 0, "row_groups": 0, "rows": 0}
            if self._local_body_matches(path, part):
                source = path
            elif (
                str(part.get("oss_key") or "")
                and archive is not None
                and callable(getattr(archive, "open_part_reader", None))
            ):
                reader = archive.open_part_reader(part)
                source = reader
            elif archive is not None:
                temporary = (
                    self.paths["scratch"]
                    / f".structure-{uuid.uuid4().hex}.parquet"
                )
                archive.download_part(part, temporary)
                source = temporary
            else:
                raise StorageError(
                    "结构索引源只存在于 OSS，但 OSS 客户端未就绪",
                    "OSS_STRUCTURE_INDEX_UNAVAILABLE",
                )
            result = self.search_index.index_structural_parquet(part, source)
            catalog = result.get("catalog")
            if isinstance(catalog, dict):
                self.metadata.upsert_part_catalog(
                    str(part["path"]),
                    str(part["sha256"]),
                    catalog,
                )
            return result
        except Exception as exc:
            self.search_index.record_failure(part, exc)
            raise
        finally:
            try:
                if reader is not None:
                    reader.close()
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
            finally:
                body_lock.release()

    def ensure_part_index(
        self,
        part: dict[str, Any],
        archive: OssArchive | None,
    ) -> dict[str, Any]:
        if self.search_index.is_current(part):
            return {"indexed": 0, "row_groups": 0, "rows": 0}
        path = Path(str(part["path"]))
        reader = None
        temporary: Path | None = None
        body_lock = self._part_body_lock(path)
        body_lock.acquire()
        try:
            current = self.metadata.part_by_path(str(path))
            if (
                not current
                or str(current.get("sha256") or "")
                != str(part.get("sha256") or "")
            ):
                return {"indexed": 0, "row_groups": 0, "rows": 0}
            if self._local_body_matches(path, part):
                source = path
            elif (
                str(part.get("oss_key") or "")
                and archive is not None
                and callable(
                getattr(archive, "open_part_reader", None)
                )
            ):
                reader = archive.open_part_reader(part)
                source = reader
            elif archive is not None:
                temporary = (
                    self.paths["scratch"] / f".index-{uuid.uuid4().hex}.parquet"
                )
                archive.download_part(part, temporary)
                source = temporary
            else:
                raise StorageError(
                    "索引源只存在于 OSS，但 OSS 客户端未就绪",
                    "OSS_INDEX_UNAVAILABLE",
                )
            result = self.search_index.index_parquet(part, source)
            catalog = result.get("catalog")
            if isinstance(catalog, dict):
                self.metadata.upsert_part_catalog(
                    str(part["path"]),
                    str(part["sha256"]),
                    catalog,
                )
            return result
        except Exception as exc:
            self.search_index.record_failure(part, exc)
            raise
        finally:
            try:
                if reader is not None:
                    reader.close()
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
            finally:
                body_lock.release()

    def ensure_part_catalog(
        self,
        part: dict[str, Any],
        archive: OssArchive | None,
    ) -> dict[str, int]:
        current = self.metadata.part_catalogs([str(part["path"])]).get(
            str(part["path"])
        )
        if current and str(current.get("sha256") or "") == str(part["sha256"]):
            # A current compressed catalog may have been recovered before the
            # legacy metadata row. Re-acknowledge it so the durable pending
            # queue and rollback-compatible legacy table converge together.
            was_pending = self.metadata.part_catalog_pending(str(part["path"]))
            acknowledged = self.metadata.upsert_part_catalog(
                str(part["path"]),
                str(part["sha256"]),
                current,
            )
            return {
                "cataloged": 1 if was_pending and acknowledged else 0,
                "rows": int(part.get("row_count") or 0) if was_pending else 0,
            }
        path = Path(str(part["path"]))
        reader = None
        temporary: Path | None = None
        parquet = None
        body_lock = self._part_body_lock(path)
        body_lock.acquire()
        try:
            current_part = self.metadata.part_by_path(str(path))
            if (
                not current_part
                or str(current_part.get("sha256") or "")
                != str(part.get("sha256") or "")
            ):
                return {"cataloged": 0, "rows": 0}
            if self._local_body_matches(path, part):
                source = path
            elif (
                str(part.get("oss_key") or "")
                and archive is not None
                and callable(
                getattr(archive, "open_part_reader", None)
                )
            ):
                reader = archive.open_part_reader(part)
                source = reader
            elif archive is not None:
                temporary = (
                    self.paths["scratch"] / f".catalog-{uuid.uuid4().hex}.parquet"
                )
                archive.download_part(part, temporary)
                source = temporary
            else:
                raise StorageError(
                    "结构目录源只存在于 OSS，但 OSS 客户端未就绪",
                    "OSS_CATALOG_UNAVAILABLE",
                )
            parquet = pq.ParquetFile(source)
            available = set(parquet.schema_arrow.names)
            columns = [
                name
                for name in ("database_name", "table_name", "operation")
                if name in available
            ]
            if len(columns) != 3:
                raise StorageError(
                    "Parquet 缺少结构目录列",
                    "PARQUET_CATALOG_COLUMNS_MISSING",
                )
            # Catalog workers already run in parallel across objects.
            table = parquet.read(columns=columns, use_threads=False)

            def distinct(name: str, *, upper: bool = False) -> list[str]:
                values = {
                    (str(value).upper() if upper else str(value))
                    for value in table.column(name).to_pylist()
                    if str(value or "").strip()
                }
                return sorted(values)

            catalog = {
                "databases": distinct("database_name"),
                "tables": distinct("table_name"),
                "operations": distinct("operation", upper=True),
            }
            if not self.metadata.upsert_part_catalog(
                str(part["path"]),
                str(part["sha256"]),
                catalog,
            ):
                raise StorageError(
                    "结构目录提交时分区版本已变化",
                    "CATALOG_PART_CHANGED",
                )
            return {"cataloged": 1, "rows": int(table.num_rows)}
        finally:
            try:
                if parquet is not None and callable(getattr(parquet, "close", None)):
                    parquet.close()
                if reader is not None:
                    reader.close()
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
            finally:
                body_lock.release()

    def ensure_exact_segment(
        self,
        parts: list[dict[str, Any]],
        archive: OssArchive | None,
        *,
        progress: Any | None = None,
    ) -> dict[str, Any]:
        if not parts:
            return {"built": 0, "part_count": 0, "row_count": 0, "exact_docs": 0}
        totals = {"built": 0, "part_count": 0, "row_count": 0, "exact_docs": 0}
        total = len(parts)
        for position, part in enumerate(parts, start=1):
            with ExitStack() as stack:
                path = Path(str(part["path"]))
                stack.enter_context(self._part_body_lock(path))
                current = self.metadata.part_by_path(str(path))
                if (
                    current is None
                    or str(current.get("logical_part_id") or current.get("sha256") or "")
                    != str(part.get("logical_part_id") or part.get("sha256") or "")
                    or str(current.get("sha256") or "")
                    != str(part.get("sha256") or "")
                ):
                    continue
                if self._local_body_matches(path, current):
                    source: Any = path
                elif (
                    str(current.get("oss_key") or "")
                    and archive is not None
                    and callable(getattr(archive, "open_part_reader", None))
                ):
                    reader = archive.open_part_reader(current)
                    stack.callback(reader.close)
                    source = reader
                elif archive is not None:
                    temporary = (
                        self.paths["scratch"]
                        / f".exact-{uuid.uuid4().hex}.parquet"
                    )
                    archive.download_part(current, temporary)
                    stack.callback(temporary.unlink, missing_ok=True)
                    source = temporary
                else:
                    raise StorageError(
                        "精确索引源只存在于 OSS，但 OSS 客户端未就绪",
                        "OSS_EXACT_INDEX_UNAVAILABLE",
                    )
                result = self.exact_index.build_segment([(current, source)])
            totals["built"] += int(result.get("built") or 0)
            totals["part_count"] += int(result.get("part_count") or 0)
            totals["row_count"] += int(result.get("row_count") or 0)
            totals["exact_docs"] += int(result.get("exact_docs") or 0)
            if progress is not None:
                progress(
                    position,
                    total,
                    current,
                    {"row_count": int(result.get("row_count") or 0)},
                )
        return totals

    def ensure_part_analytics(
        self,
        part: dict[str, Any],
        archive: OssArchive | None,
    ) -> dict[str, Any]:
        """为单个分区构建 SQL / 事务 / 锁推断聚合。

        与其它索引不同，这里必须拿到**真实文件路径**：聚合走 DuckDB，且要读几乎
        所有列，OSS Range 读没有优势，因此正文不在本地时一次性下载到 scratch，
        聚合完成后立即删除。
        """

        path = Path(str(part["path"]))
        temporary: Path | None = None
        body_lock = self._part_body_lock(path)
        body_lock.acquire()
        try:
            current = self.metadata.part_by_path(str(path))
            if (
                current is None
                or str(current.get("sha256") or "") != str(part.get("sha256") or "")
                or str(current.get("logical_part_id") or current.get("sha256") or "")
                != str(part.get("logical_part_id") or part.get("sha256") or "")
            ):
                return {"built": 0, "row_count": 0, "skipped": "part-changed"}
            if self._local_body_matches(path, current):
                source = path
            elif str(current.get("oss_key") or "") and archive is not None:
                temporary = (
                    self.paths["scratch"] / f".analytics-{uuid.uuid4().hex}.parquet"
                )
                archive.download_part(current, temporary)
                source = temporary
            else:
                raise StorageError(
                    "分析索引源只存在于 OSS，但 OSS 客户端未就绪",
                    "OSS_ANALYTICS_UNAVAILABLE",
                )
            return self.analytics_index.build_part(current, source)
        finally:
            try:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
            finally:
                body_lock.release()

    def ensure_slowlog_part(
        self,
        part: dict[str, Any],
        archive: OssArchive | None,
        *,
        already_queued: bool = False,
    ) -> dict[str, Any]:
        """Build one idempotent slow-log serving-index part from local/OSS data."""

        path = Path(str(part["path"]))
        temporary: Path | None = None
        if not already_queued:
            self.slowlog_index.enqueue_parts([part])
        body_lock = self._part_body_lock(path)
        body_lock.acquire()
        try:
            current = self.metadata.part_by_path(str(path))
            if (
                current is None
                or str(current.get("sha256") or "")
                != str(part.get("sha256") or "")
                or str(
                    current.get("logical_part_id")
                    or current.get("sha256")
                    or ""
                )
                != str(
                    part.get("logical_part_id")
                    or part.get("sha256")
                    or ""
                )
            ):
                return {
                    "built": 0,
                    "indexed_rows": 0,
                    "skipped": "part-changed",
                }
            if self._local_body_matches(path, current):
                source = path
            elif str(current.get("oss_key") or "") and archive is not None:
                temporary = (
                    self.paths["scratch"]
                    / f".slowlog-{uuid.uuid4().hex}.parquet"
                )
                archive.download_part(current, temporary)
                source = temporary
            else:
                raise StorageError(
                    "慢日志索引源只存在于 OSS，但 OSS 客户端未就绪",
                    "OSS_SLOWLOG_INDEX_UNAVAILABLE",
                )
            return self.slowlog_index.build_part(
                current,
                source,
                queue_if_missing=False,
            )
        except BaseException as exc:
            self.slowlog_index.record_failure(part, exc)
            raise
        finally:
            try:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
            finally:
                body_lock.release()

    @staticmethod
    def _rollup_width_for(start_us: int, end_us: int) -> int:
        """按窗口跨度选统计粒度。

        ≤6 小时走分区路径（分区少、5 分钟桶最精确）；6 小时~8 天用小时桶
        （7 天 168 个点，趋势图够用）；再长用天桶，否则 30 天的小时桶要扫
        350 万行仍然到不了秒级。
        """

        span = max(int(end_us) - int(start_us), 0)
        if span <= 6 * HOUR_US:
            return 0
        if span <= 8 * DAY_US:
            return HOUR_US
        return DAY_US

    def analytics_summary(
        self,
        query: dict[str, Any],
        settings: Settings,
        archive: OssArchive | None,
        *,
        control: Any | None = None,
        scan_limit: int = 8,
    ) -> dict[str, Any]:
        """按时间窗返回三类分析结果，并如实报告覆盖度。

        已覆盖分区直接读本地聚合；未覆盖分区在本次请求内最多补建 `scan_limit`
        个（即时扫描兜底，结果同时落盘供后续复用），其余交给后台索引补齐，并在
        `coverage` 中原样列出，绝不用「查不到」冒充「没有」。
        """

        start_us, end_us = self._query_window(query, settings.retention_days)
        source = str(query.get("source") or "").strip().lower()
        instance = str(query.get("instance") or "")
        parts = self.metadata.parts_in_range(
            start_epoch_us=start_us,
            end_epoch_us=end_us,
            source=source,
            instance=instance,
        )
        if source == "slowlog":
            slowlog_coverage = self._slowlog_coverage_with_repair(parts)
            if self.clickhouse_slowlog_backend is not None:
                try:
                    clickhouse_summary = (
                        self.clickhouse_slowlog_backend.summarize(
                            query,
                            retention_days=settings.retention_days,
                            control=control,
                            parts=parts,
                        )
                    )
                except Exception:
                    if control is not None:
                        control.check_cancelled()
                    LOGGER.exception(
                        "ClickHouse slow-log analytics failed; "
                        "falling back to the exact SQLite index"
                    )
                else:
                    if clickhouse_summary is not None:
                        clickhouse_coverage = dict(
                            clickhouse_summary.get(
                                "clickhouse_slowlog_coverage"
                            )
                            or {}
                        )
                        clickhouse_summary["coverage"] = {
                            **clickhouse_coverage,
                            "pending_parts": 0,
                            "scanned_parts": [],
                            "scan_errors": [],
                            "rollup_width_us": 0,
                            "rollup_lag_parts": 0,
                        }
                        clickhouse_summary["slowlog_index"] = (
                            self.slowlog_index.stats()
                        )
                        clickhouse_summary["clickhouse_slowlog"] = (
                            self.clickhouse_slowlog_backend.stats()
                        )
                        clickhouse_summary["evidence"] = {
                            "source": "slowlog",
                            "engine": "clickhouse",
                            "metrics": "actual",
                            "lock_analysis": "observed_lock_time",
                            "notes": [
                                "RowsExamined、RowsSent、QueryTime 与 LockTime "
                                "来自 RDS 慢日志原始记录。",
                                "ClickHouse 使用独立时间优先事实表；"
                                "查询前按实例和 event_id 去重，"
                                "fingerprint 与 SQLite 回退共用同一口径。",
                            ],
                        }
                        return clickhouse_summary
            if slowlog_coverage["complete"]:
                if control is not None:
                    control.check_cancelled()
                # The exact SQLite path remains the rollback/failure fallback.
                # Serialize its temp-table scan and install a progress handler
                # so a disconnected query cannot keep a 15-second scan alive.
                with self.slowlog_fallback_slot(control):
                    summary = self.slowlog_index.summarize(
                        start_epoch_us=start_us,
                        end_epoch_us=end_us,
                        instance=instance,
                        node_id=str(query.get("node_id") or ""),
                        database=str(query.get("database") or ""),
                        table=str(query.get("table") or ""),
                        operation=str(query.get("operation") or ""),
                        limit=int(query.get("limit") or 50),
                        order=str(query.get("order") or "executions"),
                        control=control,
                    )
                summary["coverage"] = {
                    **slowlog_coverage,
                    "pending_parts": 0,
                    "scanned_parts": [],
                    "scan_errors": [],
                    "rollup_width_us": 0,
                    "rollup_lag_parts": 0,
                }
                summary["slowlog_index"] = self.slowlog_index.stats()
                summary["evidence"] = {
                    "source": "slowlog",
                    "metrics": "actual",
                    "lock_analysis": "observed_lock_time",
                    "notes": [
                        "RowsExamined、RowsSent、QueryTime 与 LockTime 均来自 RDS 慢日志原始记录，不使用 EXPLAIN 估算。",
                        "统计按 event_id 精确去重并按事件时间过滤；趋势仅在展示时分桶，不扩大查询边界。",
                    ],
                }
                return summary
        coverage = self.analytics_index.coverage(parts)
        scanned: list[str] = []
        scan_errors: list[str] = []
        if coverage["missing_parts"] and scan_limit != 0:
            pending = {str(path) for path in coverage["missing_parts"]}
            targets = [part for part in parts if str(part["path"]) in pending]
            if scan_limit < 0:
                # auto：不在查询路径补建。补一个分区要从 OSS 拉正文再走 DuckDB
                # 聚合，**单个**就几十秒，无法被时间预算中断——2026-08-11 实测
                # 补 1 个分区让 1 天窗口从 1.4 秒变成 53.7 秒。补齐是后台索引器
                # 的职责，用户的等待时间不该用来干这个；缺口如实写进 coverage，
                # 由界面显示进度。需要强制补建时显式传 scan=N。
                budget = 0
                deadline = None
            else:
                budget = int(scan_limit)
                deadline = None
            for part in targets[:budget]:
                if control is not None:
                    control.check_cancelled()
                if deadline is not None and time.monotonic() >= deadline:
                    break
                try:
                    with self.query_activity():
                        self.ensure_part_analytics(part, archive)
                    scanned.append(str(part["path"]))
                except Exception as exc:
                    scan_errors.append(f"{Path(str(part['path'])).name}: {exc}")
            if scanned:
                coverage = self.analytics_index.coverage(parts)
        # 只统计当前格式下已覆盖的分区。未覆盖的分区可能残留旧口径聚合，把它们
        # 纳入范围会让过期数据混进结果。
        pending_paths = {str(path) for path in coverage["missing_parts"]}
        covered_paths = [
            str(part["path"])
            for part in parts
            if str(part["path"]) not in pending_paths
        ]
        # 长窗口改走时间桶 rollup：分区级聚合在 7 天窗口要归并 35,000+ 分区、
        # 2,600 万行，实测单是 SQL 段就 >8 分钟；rollup 把扫描量与分区数解耦。
        # 短窗口继续走分区路径，保持 5 分钟桶的精确粒度，零回归风险。
        rollup_width = self._rollup_width_for(start_us, end_us)
        rollup_lag_parts = 0
        if rollup_width:
            # 判据用 covered_paths 而非全部分区：精确路径本来也只统计已聚合的
            # 分区，rollup 同口径即可。历史段缺任何一个已聚合分区就退回精确
            # 路径——宁可慢，不能给出缺数据的结论。
            covered_parts = [
                part
                for part in parts
                if str(part["path"]) not in pending_paths
            ]
            ok, rollup_lag_parts = self.rollup_index.covered(covered_parts)
            if not ok:
                rollup_width = 0
                rollup_lag_parts = 0
        summary = self.analytics_index.summarize(
            covered_paths,
            start_epoch_us=start_us,
            end_epoch_us=end_us,
            database=str(query.get("database") or ""),
            table=str(query.get("table") or ""),
            operation=str(query.get("operation") or ""),
            limit=int(query.get("limit") or 50),
            order=str(query.get("order") or ""),
            rollup_width=rollup_width,
            instance=instance,
        )
        missing_paths = list(coverage.get("missing_parts") or [])
        public_coverage = {
            **coverage,
            "missing_parts_total": len(missing_paths),
            "missing_parts": missing_paths[:20],
            "missing_parts_truncated": len(missing_paths) > 20,
        }
        summary["coverage"] = {
            **public_coverage,
            "scanned_parts": scanned,
            "scan_errors": scan_errors,
            "pending_parts": len(missing_paths),
            # 走 rollup 时，当前小时内还没并入的分区数：结果会少这部分最新
            # 数据，如实报出来，不静默吞掉。
            "rollup_width_us": rollup_width,
            "rollup_lag_parts": rollup_lag_parts,
        }
        if source == "slowlog":
            dedicated_missing = list(slowlog_coverage.get("missing_parts") or [])
            summary["coverage"] = {
                **slowlog_coverage,
                "missing_parts_total": len(dedicated_missing),
                "missing_parts": dedicated_missing[:20],
                "missing_parts_truncated": len(dedicated_missing) > 20,
                "pending_parts": len(dedicated_missing),
                "scanned_parts": [],
                "scan_errors": [],
                "rollup_width_us": 0,
                "rollup_lag_parts": 0,
            }
            summary["slowlog_index"] = self.slowlog_index.stats()
            summary["evidence"] = {
                "source": "slowlog",
                "metrics": "legacy-estimated-fallback",
                "lock_analysis": "unavailable",
                "notes": [
                    "慢日志专用索引尚未完整覆盖，本次沿用旧分析索引回退；页面必须显示覆盖缺口，不能把估算值标成真实扫描行数。",
                ],
            }
        else:
            summary["evidence"] = {
                "source": "binlog",
                "lock_analysis": "inferred",
                "notes": [
                    "Binlog 不含 InnoDB 锁等待、死锁与 MDL 信息，锁分析全部为推断。",
                    "事务时长取自同一事务首末事件时间戳，是持锁时长的下界估计。",
                    "统计按 5 分钟桶对齐，边界桶可能含窗口外少量事件。",
                ],
            }
        return summary

    def release_archived_body(self, part: dict[str, Any]) -> int:
        if not str(part.get("oss_key") or ""):
            return 0
        path = Path(str(part["path"]))
        with self._part_body_lock(path):
            current = self.metadata.part_by_path(str(path))
            if (
                not current
                or not str(current.get("oss_key") or "")
                or str(current.get("sha256") or "")
                != str(part.get("sha256") or "")
            ):
                return 0
            if not path.is_file():
                self._remove_body_version(path)
                return 0
            events_root = self.paths["events"].resolve()
            resolved = path.resolve()
            if not resolved.is_relative_to(events_root):
                raise StorageError(
                    f"拒绝删除事件目录外文件：{resolved}",
                    "LOCAL_BODY_PATH_ESCAPE",
                )
            version = self._read_body_version(resolved)
            if version and version != str(current.get("sha256") or ""):
                return 0
            if resolved.stat().st_size != int(current.get("size_bytes") or 0):
                return 0
            if not version and _sha256(resolved) != str(current.get("sha256") or ""):
                return 0
            size = resolved.stat().st_size
            resolved.unlink()
            self._remove_body_version(resolved)
            self._forget_local_part(path)
            return size

    def recompress_part_zstd9(
        self,
        part: dict[str, Any],
        archive: OssArchive,
    ) -> dict[str, Any]:
        """Re-encode one immutable cold part without changing its row identity."""
        started = time.monotonic()
        path = Path(str(part["path"]))
        source = self.paths["scratch"] / f".zstd-source-{uuid.uuid4().hex}.parquet"
        output = self.paths["scratch"] / f".zstd9-{uuid.uuid4().hex}.parquet"
        expected_sha256 = str(
            part.get("object_sha256") or part.get("sha256") or ""
        )
        if not expected_sha256:
            raise StorageError(
                "Parquet physical identity is missing",
                "COLD_COMPRESSION_IDENTITY_MISSING",
            )

        expected_size = max(int(part.get("size_bytes") or 0), 1)
        free_bytes = shutil.disk_usage(self.paths["scratch"]).free
        required_bytes = expected_size * 3 + 64 * 1024 * 1024
        if free_bytes < required_bytes:
            raise StorageError(
                f"Cold compression needs {required_bytes} free bytes; only "
                f"{free_bytes} are available",
                "COLD_COMPRESSION_DISK_LOW",
            )

        local_source = False
        try:
            with self._part_body_lock(path):
                current = self.metadata.part_by_path(str(path))
                if (
                    current is None
                    or str(
                        current.get("object_sha256")
                        or current.get("sha256")
                        or ""
                    )
                    != expected_sha256
                ):
                    raise StorageError(
                        "Parquet encoding changed before cold compression",
                        "COLD_COMPRESSION_PART_CHANGED",
                    )
                if int(current.get("compression_level") or 1) >= 9:
                    return {
                        "state": "already-converted",
                        "logical_part_id": str(current["logical_part_id"]),
                        "old_size_bytes": int(current["size_bytes"]),
                        "new_size_bytes": int(current["size_bytes"]),
                        "saved_bytes": 0,
                        "duration_seconds": round(time.monotonic() - started, 3),
                    }
                if not str(current.get("oss_key") or ""):
                    raise StorageError(
                        "Cold Parquet has not been archived to OSS",
                        "COLD_COMPRESSION_OSS_MISSING",
                    )
                if self._local_body_matches(path, current):
                    shutil.copyfile(path, source)
                    local_source = True

            if not local_source:
                archive.download_part(current, source)
            if (
                source.stat().st_size != int(current["size_bytes"])
                or _sha256(source) != expected_sha256
            ):
                if local_source:
                    # One bounded recovery: discard a stale local body and read
                    # the immutable archived object before declaring corruption.
                    source.unlink(missing_ok=True)
                    archive.download_part(current, source)
                    local_source = False
                if (
                    source.stat().st_size != int(current["size_bytes"])
                    or _sha256(source) != expected_sha256
                ):
                    raise StorageError(
                        "Cold Parquet source failed size or SHA-256 verification",
                        "COLD_COMPRESSION_SOURCE_INVALID",
                    )

            source_parquet = pq.ParquetFile(source)
            writer: pq.ParquetWriter | None = None
            source_group_rows: list[int] = []
            try:
                source_schema = source_parquet.schema_arrow
                if "event_epoch_us" not in source_schema.names:
                    raise StorageError(
                        "Cold Parquet is missing event_epoch_us",
                        "COLD_COMPRESSION_SCHEMA_INVALID",
                    )
                writer = pq.ParquetWriter(
                    output,
                    source_schema,
                    compression="zstd",
                    compression_level=9,
                    use_dictionary=True,
                    write_statistics=True,
                )
                for row_group_id in range(source_parquet.num_row_groups):
                    table = source_parquet.read_row_group(
                        row_group_id,
                        use_threads=False,
                    )
                    row_count = int(table.num_rows)
                    if row_count <= 0:
                        raise StorageError(
                            "Cold Parquet contains an empty Row Group",
                            "COLD_COMPRESSION_ROW_GROUP_INVALID",
                        )
                    source_group_rows.append(row_count)
                    writer.write_table(table, row_group_size=row_count)
            finally:
                if writer is not None:
                    writer.close()
                close_source = getattr(source_parquet, "close", None)
                if callable(close_source):
                    close_source()

            target_parquet = pq.ParquetFile(output)
            try:
                target_group_rows = [
                    int(target_parquet.metadata.row_group(index).num_rows)
                    for index in range(target_parquet.num_row_groups)
                ]
                target_schema = target_parquet.schema_arrow
                event_times = target_parquet.read(
                    columns=["event_epoch_us"],
                    use_threads=False,
                )["event_epoch_us"]
                actual_rows = int(len(event_times))
                actual_min = int(pc.min(event_times).as_py())
                actual_max = int(pc.max(event_times).as_py())
            finally:
                close_target = getattr(target_parquet, "close", None)
                if callable(close_target):
                    close_target()

            if not source_schema.equals(target_schema, check_metadata=True):
                raise StorageError(
                    "Cold Parquet schema changed during re-encoding",
                    "COLD_COMPRESSION_SCHEMA_MISMATCH",
                )
            if source_group_rows != target_group_rows:
                raise StorageError(
                    "Cold Parquet Row Group layout changed during re-encoding",
                    "COLD_COMPRESSION_ROW_GROUP_MISMATCH",
                )
            if (
                actual_rows != int(current["row_count"])
                or actual_min != int(current["min_event_epoch_us"])
                or actual_max != int(current["max_event_epoch_us"])
            ):
                raise StorageError(
                    "Cold Parquet row count or time range changed during re-encoding",
                    "COLD_COMPRESSION_CONTENT_MISMATCH",
                )

            new_size = output.stat().st_size
            new_sha256 = _sha256(output)
            if new_sha256 == expected_sha256:
                self.metadata.mark_part_compression_level(
                    str(path),
                    expected_object_sha256=expected_sha256,
                    compression_level=9,
                )
                return {
                    "state": "already-encoded",
                    "logical_part_id": str(current["logical_part_id"]),
                    "old_size_bytes": int(current["size_bytes"]),
                    "new_size_bytes": new_size,
                    "saved_bytes": 0,
                    "duration_seconds": round(time.monotonic() - started, 3),
                }

            new_part = {
                **current,
                "size_bytes": new_size,
                "sha256": new_sha256,
                "object_sha256": new_sha256,
                "compression_level": 9,
            }
            uploaded = archive.upload_part(
                new_part,
                source_path=output,
                fresh=False,
            )

            with self._part_body_lock(path):
                latest = self.metadata.part_by_path(str(path))
                if (
                    latest is None
                    or str(
                        latest.get("object_sha256")
                        or latest.get("sha256")
                        or ""
                    )
                    != expected_sha256
                ):
                    raise StorageError(
                        "Parquet encoding changed before metadata commit",
                        "COLD_COMPRESSION_PART_CHANGED",
                    )
                # Existing physical-SHA indexes are rebound once to the stable
                # logical identity before the physical object changes.
                self.search_index.rebind_logical_parts([latest])
                self.metadata.commit_part_encoding(
                    str(path),
                    expected_object_sha256=expected_sha256,
                    object_sha256=new_sha256,
                    size_bytes=new_size,
                    compression_level=9,
                    oss_key=str(uploaded["oss_key"]),
                    oss_etag=str(uploaded.get("oss_etag") or ""),
                    oss_offset=int(uploaded.get("oss_offset") or 0),
                    oss_length=int(uploaded.get("oss_length") or 0),
                    oss_object_sha256=str(
                        uploaded.get("oss_object_sha256") or new_sha256
                    ),
                )
                if path.is_file():
                    os.replace(output, path)
                    self._write_body_version(path, new_sha256)
                    self._remember_local_parts(
                        [{**latest, **new_part, "path": str(path)}]
                    )
                else:
                    self._remove_body_version(path)
                    self._forget_local_part(path)

            return {
                "state": "converted",
                "logical_part_id": str(current["logical_part_id"]),
                "old_object_sha256": expected_sha256,
                "new_object_sha256": new_sha256,
                "old_size_bytes": int(current["size_bytes"]),
                "new_size_bytes": new_size,
                "saved_bytes": int(current["size_bytes"]) - new_size,
                "duration_seconds": round(time.monotonic() - started, 3),
            }
        finally:
            source.unlink(missing_ok=True)
            output.unlink(missing_ok=True)

    def purge_legacy_body_caches(self) -> dict[str, Any]:
        deleted_files = 0
        deleted_bytes = 0
        errors: list[str] = []
        for root_key in ("legacy_cache", "legacy_query_cache", "scratch"):
            root = self.paths[root_key].resolve()
            patterns = (
                ("*.parquet", "*.part")
                if root_key != "scratch"
                else (".query-*.parquet", ".index-*.parquet")
            )
            for pattern in patterns:
                for path in root.glob(pattern):
                    try:
                        resolved = path.resolve()
                        if not resolved.is_relative_to(root):
                            raise StorageError(
                                f"拒绝清理缓存目录外文件：{resolved}",
                                "LEGACY_CACHE_PATH_ESCAPE",
                            )
                        if resolved.is_file():
                            size = resolved.stat().st_size
                            resolved.unlink()
                            deleted_files += 1
                            deleted_bytes += size
                    except Exception as exc:
                        errors.append(f"{path.name}: {exc}")
        return {
            "deleted_files": deleted_files,
            "deleted_bytes": deleted_bytes,
            "errors": errors,
        }

    def _prime_structural_query_index(
        self,
        parts: list[dict[str, Any]],
        index_plan: dict[str, Any],
        query: dict[str, Any],
        archive: OssArchive | None,
        *,
        start_epoch_us: int,
        end_epoch_us: int,
    ) -> tuple[dict[str, Any], int]:
        if not (
            str(query.get("database") or "").strip()
            or str(query.get("table") or "").strip()
            or any(str(value).strip() for value in (query.get("operations") or []))
        ):
            return index_plan, 0
        part_map = {str(part["path"]): part for part in parts}
        unknown_paths = list(index_plan["unknown_paths"])
        catalogs = self.metadata.part_catalogs(unknown_paths)
        candidates: list[dict[str, Any]] = []
        probes: list[tuple[str, str, str]] = []
        for path_text in unknown_paths:
            part = part_map[path_text]
            catalog = catalogs.get(path_text)
            if (
                catalog
                and str(catalog.get("sha256") or "") == str(part["sha256"])
                and not self._catalog_allows(catalog, query)
            ):
                continue
            if (
                not Path(path_text).is_file()
                and not str(part.get("oss_key") or "")
            ):
                continue
            fingerprint = self._query_probe_fingerprint(
                query,
                part,
                start_epoch_us,
                end_epoch_us,
            )
            probes.append((path_text, str(part["sha256"]), fingerprint))
            candidates.append(part)
        negative = self.metadata.negative_probe_matches(probes)
        candidates = [
            part for part in candidates if str(part["path"]) not in negative
        ]
        candidates.sort(
            key=lambda part: (
                int(part["max_event_epoch_us"]),
                int(part["min_event_epoch_us"]),
                str(part["path"]),
            ),
            reverse=True,
        )
        candidates = candidates[:QUERY_STRUCTURAL_PRIME_LIMIT]
        if not candidates:
            return index_plan, 0
        completed = 0
        with ThreadPoolExecutor(
            max_workers=min(4, len(candidates)),
            thread_name_prefix="query-structure-index",
        ) as executor:
            futures = {
                executor.submit(
                    self.ensure_part_structural_index,
                    part,
                    archive,
                ): part
                for part in candidates
            }
            for future in as_completed(futures):
                part = futures[future]
                try:
                    future.result()
                except Exception:
                    continue
                if self.search_index.is_structural_current(part):
                    completed += 1
        return (
            self.search_index.candidate_blocks(
                parts,
                query,
                start_epoch_us=start_epoch_us,
                end_epoch_us=end_epoch_us,
            ),
            completed,
        )

    @staticmethod
    def _query_flight_key(
        query: dict[str, Any],
        settings: Settings,
        archive: OssArchive | None,
        limit_cap: int,
    ) -> str:
        payload = {
            "query": query,
            "limit_cap": int(limit_cap),
            "instance_id": settings.db_instance_id,
            "retention_days": int(settings.retention_days),
            "oss": {
                "available": archive is not None,
                "bucket": settings.oss_bucket,
                "endpoint": settings.oss_endpoint,
                "prefix": settings.oss_prefix,
            },
        }
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    def query_events_tiered(
        self,
        query: dict[str, Any],
        settings: Settings,
        archive: OssArchive | None,
        *,
        limit_cap: int = 1000,
        control: Any | None = None,
    ) -> dict[str, Any]:
        source = str(query.get("source") or "").strip().lower()
        if source == "audit":
            # 本地执行日志只有几千条，直接查带索引的表；走 Parquet 那条路要打开
            # 每条事件各自的单行分区文件，实测 3896 条 0.8 秒。
            result = self.metadata.query_tabularis_audit_log(query)
            return {
                **result,
                "coverage_found": True,
                "tiers_used": ["audit-index"],
                "local_parts_read": 0,
                "oss_parts_read": 0,
            }
        slowlog_coverage: dict[str, Any] | None = None
        if source == "slowlog":
            available = self.metadata.storage_metadata_stats()
            latest_us = available.get("latest_epoch_us")
            requested_end_us = query.get("end_epoch_us")
            if (
                requested_end_us is not None
                and latest_us is not None
                and int(requested_end_us) > int(latest_us)
            ):
                latest_text = datetime.fromtimestamp(
                    int(latest_us) / 1_000_000,
                    UTC,
                ).strftime("%Y-%m-%d %H:%M:%S UTC")
                raise StorageError(
                    f"结束时间超出已有数据范围；当前已解析数据只到 {latest_text}",
                    "QUERY_END_AFTER_LATEST",
                )
            start_us, end_us = self._query_window(query, settings.retention_days)
            instance = str(query.get("instance") or "")
            parts = self.metadata.parts_in_range(
                start_epoch_us=start_us,
                end_epoch_us=end_us,
                source="slowlog",
                instance=instance,
            )
            slowlog_coverage = self._slowlog_coverage_with_repair(parts)
            if slowlog_coverage["complete"]:
                with self.query_activity():
                    if control is not None:
                        control.check_cancelled()
                        control.set_plan(
                            total_parts=len(parts),
                            candidate_parts=len(parts),
                            indexed_parts=len(parts),
                            unknown_parts=0,
                            estimated_bytes=0,
                        )
                    result = self.slowlog_index.query_events(
                        query,
                        start_epoch_us=start_us,
                        end_epoch_us=end_us,
                    )
                result["coverage_found"] = bool(parts)
                result["slowlog_index_coverage"] = slowlog_coverage
                result["available_start_epoch_us"] = available.get("oldest_epoch_us")
                result["available_end_epoch_us"] = latest_us
                return result
        with self.query_activity():
            if control is not None:
                result = self._query_events_tiered_impl(
                    query,
                    settings,
                    archive,
                    limit_cap=limit_cap,
                    control=control,
                )
            else:
                result = self._query_events_tiered_singleflight(
                    query,
                    settings,
                    archive,
                    limit_cap=limit_cap,
                )
        if slowlog_coverage is not None:
            result["slowlog_index_coverage"] = slowlog_coverage
            result["slowlog_index_fallback"] = True
        return result

    def _query_events_tiered_singleflight(
        self,
        query: dict[str, Any],
        settings: Settings,
        archive: OssArchive | None,
        *,
        limit_cap: int = 1000,
    ) -> dict[str, Any]:
        key = self._query_flight_key(query, settings, archive, limit_cap)
        with self._query_flights_lock:
            future = self._query_flights.get(key)
            owner = future is None
            if future is None:
                future = Future()
                self._query_flights[key] = future
        if not owner:
            return future.result()
        try:
            result = self._query_events_tiered_impl(
                query,
                settings,
                archive,
                limit_cap=limit_cap,
            )
        except BaseException as exc:
            future.set_exception(exc)
            raise
        else:
            future.set_result(result)
            return result
        finally:
            with self._query_flights_lock:
                if self._query_flights.get(key) is future:
                    self._query_flights.pop(key, None)

    def _exact_index_result(
        self,
        parts: list[dict[str, Any]],
        catalogs: dict[str, dict[str, Any]],
        query: dict[str, Any],
        start_us: int,
        end_us: int,
        limit: int,
        offset: int,
    ) -> dict[str, Any] | None:
        exact = query.get("exact")
        if not isinstance(exact, dict):
            return None
        result = self.exact_index.lookup(
            parts,
            catalogs=catalogs,
            database=str(query.get("database") or ""),
            table=str(query.get("table") or ""),
            value=exact.get("value"),
            start_epoch_us=start_us,
            end_epoch_us=end_us,
            operations=query.get("operations") or [],
            limit=limit,
            offset=offset,
        )
        if not bool(result.get("complete")):
            if str(exact.get("fallback") or "error").lower() == "error":
                missing = len(result.get("missing_parts") or [])
                unknown = len(result.get("unknown_parts") or [])
                raise StorageError(
                    f"主键精确索引尚未完整覆盖：缺失 {missing} 个分块，未知 schema {unknown} 个分块",
                    "EXACT_INDEX_INCOMPLETE",
                )
            return {
                "fallback_scan": True,
                "exact_index_complete": False,
                "exact_index_covered_parts": int(result.get("covered_parts") or 0),
                "exact_index_missing_parts": len(result.get("missing_parts") or []),
                "exact_index_unknown_parts": len(result.get("unknown_parts") or []),
            }
        return {
            "rows": list(result.get("rows") or []),
            "has_more": bool(result.get("has_more")),
            "limit": limit,
            "offset": offset,
            "coverage_found": bool(parts),
            "tiers_used": ["exact-index"],
            "local_parts_read": 0,
            "oss_parts_read": 0,
            "oss_range_parts_read": 0,
            "oss_temporary_parts_read": 0,
            "range_requests": 0,
            "range_bytes": 0,
            "full_object_fallback_bytes": 0,
            "predicate_row_groups_scanned": 0,
            "predicate_row_groups_selected": 0,
            "candidate_blocks": 0,
            "query_scan_workers": 0,
            "indexed_parts": int(result.get("covered_parts") or 0),
            "structural_indexed_parts": 0,
            "structural_prime_parts": 0,
            "index_unknown_parts": 0,
            "index_skipped_parts": 0,
            "index_coverage": 1.0,
            "catalog_skipped_parts": 0,
            "negative_probe_skipped_parts": 0,
            "positive_probe_cached_parts": 0,
            "query_cache_parts_read": 0,
            "query_certificate_hit": False,
            "query_certificate_recorded": False,
            "query_certificate_rows": 0,
            "oss_downloaded_parts": 0,
            "unavailable_parts": 0,
            "range_start_epoch_us": start_us,
            "range_end_epoch_us": end_us,
            "exact_index_complete": True,
            "exact_index_segments": int(result.get("segments") or 0),
            "exact_index_covered_parts": int(result.get("covered_parts") or 0),
            "exact_index_missing_parts": 0,
            "exact_index_unknown_parts": 0,
            "exact_index_oss_gets": int(result.get("oss_gets") or 0),
            "exact_index_oss_bytes": int(result.get("oss_bytes") or 0),
        }

    def _query_events_tiered_impl(
        self,
        query: dict[str, Any],
        settings: Settings,
        archive: OssArchive | None,
        *,
        limit_cap: int = 1000,
        control: Any | None = None,
    ) -> dict[str, Any]:
        if control is not None:
            control.check_cancelled()
        limit = min(
            max(int(query.get("limit") or 100), 1),
            min(max(int(limit_cap), 1), 100_000),
        )
        offset = min(max(int(query.get("offset") or 0), 0), 100_000)
        available = self.metadata.storage_metadata_stats()
        oldest_us = available.get("oldest_epoch_us")
        latest_us = available.get("latest_epoch_us")
        requested_end_us = query.get("end_epoch_us")
        if (
            requested_end_us is not None
            and latest_us is not None
            and int(requested_end_us) > int(latest_us)
        ):
            latest_text = datetime.fromtimestamp(
                int(latest_us) / 1_000_000,
                UTC,
            ).strftime("%Y-%m-%d %H:%M:%S UTC")
            raise StorageError(
                f"结束时间超出已有数据范围；当前已解析数据只到 {latest_text}",
                "QUERY_END_AFTER_LATEST",
            )
        start_us, end_us = self._query_window(query, settings.retention_days)
        if start_us > end_us:
            if control is not None:
                control.set_plan(
                    total_parts=0,
                    candidate_parts=0,
                    indexed_parts=0,
                    unknown_parts=0,
                    estimated_bytes=0,
                )
            return {
                "rows": [],
                "has_more": False,
                "limit": limit,
                "offset": offset,
                "coverage_found": False,
                "tiers_used": [],
                "catalog_skipped_parts": 0,
                "negative_probe_skipped_parts": 0,
                "positive_probe_cached_parts": 0,
                "query_cache_parts_read": 0,
                "query_certificate_hit": False,
                "query_certificate_recorded": False,
                "available_start_epoch_us": oldest_us,
                "available_end_epoch_us": latest_us,
            }
        certificate_fingerprint = self._query_certificate_fingerprint(
            query,
            start_us,
            end_us,
            settings.db_instance_id,
        )
        certificate_token, certificate_rows = (
            self.metadata.complete_query_certificate(
                certificate_fingerprint,
                start_epoch_us=start_us,
                end_epoch_us=end_us,
            )
        )
        if certificate_rows is not None:
            if control is not None:
                control.set_plan(
                    total_parts=0,
                    candidate_parts=0,
                    indexed_parts=int(certificate_token["part_count"]),
                    unknown_parts=0,
                    estimated_bytes=0,
                )
            return self._query_certificate_result(
                certificate_rows,
                limit=limit,
                offset=offset,
                start_us=start_us,
                end_us=end_us,
                oldest_us=oldest_us,
                latest_us=latest_us,
                token=certificate_token,
            )
        if self.clickhouse_backend is not None:
            hot_query = dict(query)
            hot_query["start_epoch_us"] = start_us
            hot_query["end_epoch_us"] = end_us
            try:
                hot_result = self.clickhouse_backend.query_events(
                    hot_query,
                    retention_days=settings.retention_days,
                    limit_cap=limit_cap,
                    control=control,
                )
            except Exception:
                if control is not None:
                    control.check_cancelled()
                LOGGER.exception(
                    "ClickHouse hot query failed; falling back to Parquet"
                )
            else:
                if hot_result is not None:
                    hot_result["available_start_epoch_us"] = oldest_us
                    hot_result["available_end_epoch_us"] = latest_us
                    return hot_result
        parts = self.metadata.parts_in_range(
            start_epoch_us=start_us,
            end_epoch_us=end_us,
            source=str(query.get("source") or ""),
            instance=str(query.get("instance") or ""),
        )
        if control is not None:
            control.check_cancelled()
        exact = query.get("exact")
        exact_fallback: dict[str, Any] = {}
        if isinstance(exact, dict):
            exact_catalogs = self.metadata.part_catalogs(
                [str(part["path"]) for part in parts]
            )
            exact_result = self._exact_index_result(
                parts,
                exact_catalogs,
                query,
                start_us,
                end_us,
                limit,
                offset,
            )
            if exact_result is not None:
                if exact_result.get("fallback_scan"):
                    exact_fallback = exact_result
                else:
                    if control is not None:
                        control.set_plan(
                            total_parts=0,
                            candidate_parts=0,
                            indexed_parts=int(exact_result["exact_index_covered_parts"]),
                            unknown_parts=0,
                            estimated_bytes=0,
                        )
                    exact_result["available_start_epoch_us"] = oldest_us
                    exact_result["available_end_epoch_us"] = latest_us
                    return exact_result
        index_plan = self.search_index.candidate_blocks(
            parts,
            query,
            start_epoch_us=start_us,
            end_epoch_us=end_us,
        )
        # Public queries must remain read-only against the shared search
        # index. Building even a handful of structural entries here can wait
        # behind the external indexer's SQLite writer and make the service
        # health endpoint appear dead for minutes. Unknown parts are scanned
        # directly and indexed later by the dedicated background process.
        structural_prime_parts = 0
        part_map = {str(part["path"]): part for part in parts}
        indexed_candidate_fingerprints: dict[str, str] = {}
        for entry in index_plan["entries"]:
            path_text = str(entry["path"])
            part = part_map.get(path_text)
            if part is None or path_text in indexed_candidate_fingerprints:
                continue
            indexed_candidate_fingerprints[path_text] = (
                self._query_probe_fingerprint(
                    query,
                    part,
                    start_us,
                    end_us,
                )
            )
        indexed_probes = [
            (path_text, str(part_map[path_text]["sha256"]), fingerprint)
            for path_text, fingerprint in indexed_candidate_fingerprints.items()
        ]
        negative_paths = set(
            self.metadata.negative_probe_matches(indexed_probes)
        )
        positive_pages = self.metadata.positive_probe_matches(indexed_probes)
        negative_paths.difference_update(positive_pages)
        grouped: dict[str, dict[str, Any]] = {}
        for entry in index_plan["entries"]:
            complete = bool(entry.get("complete", True))
            path_text = str(entry["path"])
            if path_text in negative_paths:
                continue
            target_entry = grouped.setdefault(
                path_text,
                {
                    "part": entry["part"],
                    "row_groups": set(),
                    "max_event_epoch_us": int(entry["max_event_epoch_us"]),
                    "indexed": complete,
                    "fingerprint": indexed_candidate_fingerprints.get(
                        path_text,
                        "",
                    ),
                },
            )
            target_entry["row_groups"].add(int(entry["row_group_id"]))
            target_entry["indexed"] = bool(target_entry["indexed"]) and complete
            target_entry["max_event_epoch_us"] = max(
                int(target_entry["max_event_epoch_us"]),
                int(entry["max_event_epoch_us"]),
            )
        unknown_paths = list(index_plan["unknown_paths"])
        catalogs = self.metadata.part_catalogs(unknown_paths)
        unknown_candidates: list[tuple[str, str, str]] = []
        unknown_fingerprints: dict[str, str] = {}
        catalog_skipped_parts = 0
        for path_text in unknown_paths:
            part = part_map[path_text]
            catalog = catalogs.get(path_text)
            if (
                catalog
                and str(catalog.get("sha256") or "") == str(part["sha256"])
                and not self._catalog_allows(catalog, query)
            ):
                catalog_skipped_parts += 1
                continue
            fingerprint = self._query_probe_fingerprint(
                query,
                part,
                start_us,
                end_us,
            )
            unknown_fingerprints[path_text] = fingerprint
            unknown_candidates.append(
                (path_text, str(part["sha256"]), fingerprint)
            )
        negative_paths.update(
            self.metadata.negative_probe_matches(unknown_candidates)
        )
        positive_pages.update(
            self.metadata.positive_probe_matches(unknown_candidates)
        )
        negative_paths.difference_update(positive_pages)
        for path_text, fingerprint in unknown_fingerprints.items():
            if path_text in negative_paths:
                continue
            part = part_map[path_text]
            grouped[path_text] = {
                "part": part,
                "row_groups": None,
                "max_event_epoch_us": int(part["max_event_epoch_us"]),
                "indexed": False,
                "fingerprint": fingerprint,
            }
        work = sorted(
            grouped.values(),
            key=lambda entry: (
                int(entry["max_event_epoch_us"]),
                int(entry["part"]["min_event_epoch_us"]),
                str(entry["part"]["path"]),
            ),
            reverse=True,
        )
        unavailable_parts = [
            part
            for part in parts
            if not Path(str(part["path"])).is_file()
            and not str(part.get("oss_key") or "")
        ]
        work = [
            entry
            for entry in work
            if Path(str(entry["part"]["path"])).is_file()
            or str(entry["part"].get("oss_key") or "")
        ]
        if control is not None:
            estimated_bytes = sum(
                0
                if str(entry["part"]["path"]) in positive_pages
                else int(entry["part"].get("size_bytes") or 0)
                for entry in work
            )
            control.set_plan(
                total_parts=len(work),
                candidate_parts=len(work),
                indexed_parts=len(
                    index_plan.get(
                        "full_covered_paths",
                        index_plan["covered_paths"],
                    )
                ),
                unknown_parts=len(index_plan["unknown_paths"]),
                estimated_bytes=estimated_bytes,
            )
        target = min(limit + offset + 1, 100_001)
        internal_query = dict(query)
        internal_query["start_epoch_us"] = start_us
        internal_query["end_epoch_us"] = end_us
        internal_query["limit"] = target
        internal_query["offset"] = 0
        rows: list[dict[str, Any]] = []
        tiers: list[str] = []
        local_parts_read = 0
        oss_range_parts_read = 0
        oss_temporary_parts_read = 0
        range_requests = 0
        range_bytes = 0
        full_object_fallback_bytes = 0
        candidate_blocks = 0
        predicate_row_groups_scanned = 0
        predicate_row_groups_selected = 0
        pending_negative_probes: list[tuple[str, str, str]] = []
        pending_positive_probes: list[
            tuple[str, str, str, list[dict[str, Any]]]
        ] = []
        positive_probe_cached_parts = 0
        scanned_batches = 0
        page_incomplete = False
        rows_truncated = False

        def flush_negative_probes() -> None:
            if not pending_negative_probes:
                return
            self.metadata.record_negative_probes(pending_negative_probes)
            pending_negative_probes.clear()

        def flush_positive_probes() -> None:
            if not pending_positive_probes:
                return
            self.metadata.record_positive_probes(pending_positive_probes)
            pending_positive_probes.clear()

        def scan_entry(entry: dict[str, Any]) -> dict[str, Any]:
            if control is not None:
                control.check_cancelled()
            part = entry["part"]
            row_groups = entry["row_groups"]
            cached_rows = positive_pages.get(str(part["path"]))
            if cached_rows is not None:
                return {
                    "entry": entry,
                    "page": {
                        "rows": [dict(row) for row in cached_rows],
                        "has_more": False,
                        "limit": internal_query["limit"],
                        "offset": 0,
                    },
                    "tier": "local-index",
                    "io_stats": {
                        "range_requests": 0,
                        "range_bytes": 0,
                        "full_object_fallback_bytes": 0,
                        "predicate_row_groups_scanned": 0,
                        "predicate_row_groups_selected": 0,
                    },
                    "candidate_blocks": 0,
                    "positive_probe_cached": True,
                }
            acquired = False
            while not acquired:
                if control is not None:
                    control.check_cancelled()
                acquired = self._query_scan_slots.acquire(timeout=0.1)
            try:
                if control is not None:
                    control.check_cancelled()
                table, tier, io_stats = self._read_part_table(
                    part,
                    row_groups,
                    archive,
                    columns=(
                        "event_time_utc",
                        *QUERY_RESULT_COLUMNS,
                        *(('columns_json',) if isinstance(internal_query.get('exact'), dict) else ()),
                    ),
                    predicate_query=(
                        internal_query if row_groups is None else None
                    ),
                )
                if control is not None:
                    control.check_cancelled()
                locator_groups = (
                    ",".join(str(value) for value in sorted(row_groups))
                    if row_groups is not None
                    else "*"
                )
                locator = (
                    f"{part.get('logical_part_id') or part['sha256']}:"
                    f"{locator_groups}"
                )
                page = self._query_arrow_table(
                    table,
                    internal_query,
                    settings.retention_days,
                    locator=locator,
                    limit_cap=100_001,
                )
                if control is not None:
                    control.check_cancelled()
            finally:
                self._query_scan_slots.release()
            return {
                "entry": entry,
                "page": page,
                "tier": tier,
                "io_stats": io_stats,
                "candidate_blocks": len(row_groups) if row_groups is not None else 0,
                "positive_probe_cached": False,
            }

        with ThreadPoolExecutor(
            max_workers=QUERY_SCAN_WORKERS,
            thread_name_prefix="query-part-scan",
        ) as executor:
            selective_query = bool(
                str(query.get("keyword") or "").strip()
                or str(query.get("database") or "").strip()
                or str(query.get("table") or "").strip()
                or str(query.get("connection") or "").strip()
                or str(query.get("account") or "").strip()
                or str(query.get("status") or "").strip()
                or query.get("operations")
            )
            scan_width = (
                QUERY_SCAN_WORKERS
                if selective_query
                else min(QUERY_SCAN_WORKERS, QUERY_UNFILTERED_PREFETCH)
            )
            next_submit = 0
            next_result = 0
            in_flight: dict[Future[dict[str, Any]], int] = {}
            completed: dict[int, dict[str, Any]] = {}
            initial_width = min(
                scan_width,
                len(work),
            )
            for _ in range(initial_width):
                if control is not None:
                    control.check_cancelled()
                in_flight[executor.submit(scan_entry, work[next_submit])] = (
                    next_submit
                )
                next_submit += 1

            stop_scanning = False
            while in_flight:
                future = next(as_completed(tuple(in_flight)))
                if control is not None:
                    control.check_cancelled()
                result_index = in_flight.pop(future)
                completed[result_index] = future.result()

                # Consume only the contiguous time-descending prefix so the
                # early-stop rule remains exact. Faster later reads may finish
                # out of order, but they immediately free a worker for more
                # OSS work instead of idling behind one slow Range request.
                while next_result in completed:
                    result = completed.pop(next_result)
                    entry = result["entry"]
                    part = entry["part"]
                    page = result["page"]
                    tier = str(result["tier"])
                    io_stats = result["io_stats"]
                    positive_probe_cached = bool(
                        result.get("positive_probe_cached")
                    )
                    if page.get("has_more"):
                        page_incomplete = True
                    candidate_blocks += int(result["candidate_blocks"])
                    if (
                        not page["rows"]
                        and str(entry.get("fingerprint") or "")
                    ):
                        pending_negative_probes.append(
                            (
                                str(part["path"]),
                                str(part["sha256"]),
                                str(entry["fingerprint"]),
                            )
                        )
                    elif (
                        page["rows"]
                        and not page.get("has_more")
                        and not positive_probe_cached
                        and str(entry.get("fingerprint") or "")
                    ):
                        pending_positive_probes.append(
                            (
                                str(part["path"]),
                                str(part["sha256"]),
                                str(entry["fingerprint"]),
                                [dict(row) for row in page["rows"]],
                            )
                        )
                    rows.extend(page["rows"])
                    if tier not in tiers:
                        tiers.append(tier)
                    if positive_probe_cached:
                        positive_probe_cached_parts += 1
                        local_parts_read += 1
                    elif tier == "local-index":
                        local_parts_read += 1
                    elif tier == "oss-range":
                        oss_range_parts_read += 1
                    else:
                        oss_temporary_parts_read += 1
                    range_requests += int(io_stats.get("range_requests") or 0)
                    range_bytes += int(io_stats.get("range_bytes") or 0)
                    full_object_fallback_bytes += int(
                        io_stats.get("full_object_fallback_bytes") or 0
                    )
                    predicate_row_groups_scanned += int(
                        io_stats.get("predicate_row_groups_scanned") or 0
                    )
                    predicate_row_groups_selected += int(
                        io_stats.get("predicate_row_groups_selected") or 0
                    )
                    if control is not None:
                        control.advance(
                            current_file=Path(str(part["path"])).name,
                            scanned_bytes=(
                                int(io_stats.get("range_bytes") or 0)
                                + int(
                                    io_stats.get(
                                        "full_object_fallback_bytes"
                                    )
                                    or 0
                                )
                            ),
                        )
                    next_result += 1
                    if next_result % QUERY_SCAN_WORKERS == 0:
                        scanned_batches += 1
                        if (
                            scanned_batches % QUERY_MEMORY_RELEASE_BATCHES
                            == 0
                        ):
                            pa.default_memory_pool().release_unused()
                    if (
                        len(pending_negative_probes)
                        >= NEGATIVE_PROBE_WRITE_BATCH
                    ):
                        flush_negative_probes()
                    if (
                        len(pending_positive_probes)
                        >= POSITIVE_PROBE_WRITE_BATCH
                    ):
                        flush_positive_probes()
                    if len(rows) > target * 4:
                        rows_truncated = True
                        unique = {
                            str(row["event_id"]): row for row in rows
                        }
                        rows = sorted(
                            unique.values(),
                            key=self._row_sort_key,
                            reverse=True,
                        )[:target]
                    if len(rows) >= target and next_result < len(work):
                        unique = {
                            str(row["event_id"]): row for row in rows
                        }
                        leading = sorted(
                            unique.values(),
                            key=self._row_sort_key,
                            reverse=True,
                        )[:target]
                        if (
                            len(leading) >= target
                            and int(
                                work[next_result]["max_event_epoch_us"]
                            )
                            < int(leading[-1]["event_epoch_us"])
                        ):
                            rows = leading
                            stop_scanning = True
                            break
                if stop_scanning:
                    for pending in in_flight:
                        pending.cancel()
                    break
                submit_width = 0
                if next_submit < len(work):
                    if target > scan_width:
                        submit_width = min(
                            max(scan_width - len(in_flight), 0),
                            len(work) - next_submit,
                        )
                    elif not in_flight:
                        submit_width = min(
                            scan_width,
                            max(target - len(rows), 1),
                            len(work) - next_submit,
                        )
                for _ in range(submit_width):
                    if control is not None:
                        control.check_cancelled()
                    in_flight[
                        executor.submit(scan_entry, work[next_submit])
                    ] = next_submit
                    next_submit += 1
        flush_negative_probes()
        flush_positive_probes()
        pa.default_memory_pool().release_unused()
        unique_rows: dict[str, dict[str, Any]] = {}
        for row in rows:
            unique_rows.setdefault(str(row["event_id"]), row)
        ordered = sorted(
            unique_rows.values(),
            key=self._row_sort_key,
            reverse=True,
        )
        certificate_complete = (
            not stop_scanning
            and next_result == len(work)
            and not page_incomplete
            and not rows_truncated
            and not unavailable_parts
        )
        certificate_recorded = False
        if certificate_complete:
            certificate_recorded = self.metadata.record_complete_query_certificate(
                certificate_fingerprint,
                start_epoch_us=start_us,
                end_epoch_us=end_us,
                expected_token=certificate_token,
                rows=ordered,
            )
        visible = ordered[offset : offset + limit]
        return {
            "rows": visible,
            "has_more": len(ordered) > offset + limit,
            "limit": limit,
            "offset": offset,
            "coverage_found": bool(parts),
            "tiers_used": tiers,
            "local_parts_read": local_parts_read,
            "oss_parts_read": oss_range_parts_read + oss_temporary_parts_read,
            "oss_range_parts_read": oss_range_parts_read,
            "oss_temporary_parts_read": oss_temporary_parts_read,
            "range_requests": range_requests,
            "range_bytes": range_bytes,
            "full_object_fallback_bytes": full_object_fallback_bytes,
            "predicate_row_groups_scanned": predicate_row_groups_scanned,
            "predicate_row_groups_selected": predicate_row_groups_selected,
            "candidate_blocks": candidate_blocks,
            "query_scan_workers": QUERY_SCAN_WORKERS,
            "indexed_parts": len(
                index_plan.get("full_covered_paths", index_plan["covered_paths"])
            ),
            "structural_indexed_parts": len(
                index_plan.get("structural_covered_paths", set())
            ),
            "structural_prime_parts": structural_prime_parts,
            "index_unknown_parts": len(index_plan["unknown_paths"]),
            "index_skipped_parts": int(index_plan["skipped_parts"]),
            "index_coverage": (
                len(index_plan["covered_paths"]) / len(parts) if parts else 1.0
            ),
            "catalog_skipped_parts": catalog_skipped_parts,
            "negative_probe_skipped_parts": len(negative_paths),
            "positive_probe_cached_parts": positive_probe_cached_parts,
            "query_cache_parts_read": 0,
            "query_certificate_hit": False,
            "query_certificate_recorded": certificate_recorded,
            "query_certificate_part_count": certificate_token["part_count"],
            "query_certificate_rows": len(ordered) if certificate_recorded else 0,
            "oss_downloaded_parts": oss_temporary_parts_read,
            "unavailable_parts": len(unavailable_parts),
            "range_start_epoch_us": start_us,
            "range_end_epoch_us": end_us,
            "available_start_epoch_us": oldest_us,
            "available_end_epoch_us": latest_us,
            **exact_fallback,
        }

    def event_detail(self, event_id: str, retention_days: int) -> dict[str, Any] | None:
        dataset = self._dataset_sql()
        if not dataset:
            return None
        cutoff = int(
            (datetime.now(UTC) - timedelta(days=retention_days)).timestamp() * 1_000_000
        )
        conn = self._duckdb_connect()
        try:
            cursor = conn.execute(
                f"SELECT * FROM {dataset} WHERE event_id = ? "
                "AND event_epoch_us >= ? LIMIT 1",
                [event_id, cutoff],
            )
            row = cursor.fetchone()
            if not row:
                return None
            names = [item[0] for item in cursor.description]
            return dict(zip(names, row, strict=True))
        finally:
            conn.close()

    @staticmethod
    def _event_detail_from_source(
        source: Any,
        event_id: str,
        cutoff_epoch_us: int,
        row_group_ids: Iterable[int] | None,
    ) -> dict[str, Any] | None:
        parquet = pq.ParquetFile(source)
        requested = (
            sorted({int(value) for value in row_group_ids})
            if row_group_ids is not None
            else None
        )
        groups = (
            list(range(parquet.num_row_groups))
            if requested is None
            else [
                value
                for value in requested
                if 0 <= value < parquet.num_row_groups
            ]
        )
        if requested is not None and not groups:
            raise StorageError(
                "索引指向的 Parquet Row Group 不存在",
                "INDEX_ROW_GROUP_INVALID",
            )
        available = set(parquet.schema_arrow.names)
        required = {"event_id", "event_epoch_us"}
        if not required.issubset(available):
            raise StorageError(
                "Parquet 缺少事件详情定位列",
                "EVENT_DETAIL_COLUMNS_MISSING",
            )

        for group_id in groups:
            group_offset = 0
            target_offset: int | None = None
            for batch in parquet.iter_batches(
                batch_size=EVENT_DETAIL_PROBE_BATCH_ROWS,
                row_groups=[group_id],
                columns=["event_id", "event_epoch_us"],
                use_threads=False,
            ):
                event_ids = batch.column(batch.schema.get_field_index("event_id"))
                epochs = batch.column(
                    batch.schema.get_field_index("event_epoch_us")
                )
                matches = pc.and_(
                    pc.equal(event_ids, event_id),
                    pc.greater_equal(epochs, int(cutoff_epoch_us)),
                )
                indexes = pc.indices_nonzero(matches)
                if len(indexes):
                    target_offset = group_offset + int(indexes[0].as_py())
                    break
                group_offset += batch.num_rows
            if target_offset is None:
                continue

            batch_offset = 0
            for batch in parquet.iter_batches(
                batch_size=EVENT_DETAIL_VALUE_BATCH_ROWS,
                row_groups=[group_id],
                use_threads=False,
            ):
                if target_offset < batch_offset + batch.num_rows:
                    row = batch.slice(target_offset - batch_offset, 1).to_pylist()
                    return row[0] if row else None
                batch_offset += batch.num_rows
        return None

    def _event_detail_from_part(
        self,
        part: dict[str, Any],
        event_id: str,
        cutoff_epoch_us: int,
        row_group_ids: Iterable[int] | None,
        archive: OssArchive | None,
    ) -> dict[str, Any] | None:
        path = Path(str(part["path"]))
        with self._part_body_lock(path):
            if self._local_body_matches(path, part):
                return self._event_detail_from_source(
                    path,
                    event_id,
                    cutoff_epoch_us,
                    row_group_ids,
                )
        if archive is None:
            raise StorageError(
                "事件详情仅存在于 OSS，但 OSS 客户端未就绪",
                "OSS_QUERY_UNAVAILABLE",
            )
        reader_factory = getattr(archive, "open_part_reader", None)
        if callable(reader_factory):
            reader = None
            try:
                reader = reader_factory(part)
                return self._event_detail_from_source(
                    reader,
                    event_id,
                    cutoff_epoch_us,
                    row_group_ids,
                )
            except Exception:
                # Preserve availability when a transient Range read fails; the
                # full object stays compressed on disk and is still decoded in
                # bounded batches below.
                pass
            finally:
                if reader is not None:
                    reader.close()
        destination = self.paths["scratch"] / (
            f".detail-{uuid.uuid4().hex}.parquet"
        )
        try:
            archive.download_part(part, destination)
            return self._event_detail_from_source(
                destination,
                event_id,
                cutoff_epoch_us,
                row_group_ids,
            )
        finally:
            destination.unlink(missing_ok=True)

    def event_detail_tiered(
        self,
        event_id: str,
        settings: Settings,
        archive: OssArchive | None,
        locator: str = "",
        instance: str = "",
    ) -> dict[str, Any] | None:
        with self.query_activity():
            slowlog = self._slowlog_event_detail(event_id, settings, instance)
            if slowlog is not None:
                return slowlog
            return self._event_detail_tiered_impl(
                event_id,
                settings,
                archive,
                locator,
            )

    def slowlog_event_detail(
        self, event_id: str, settings: Settings, instance: str = ""
    ) -> dict[str, Any] | None:
        """Read a covered slow-log detail locally before OSS fallback is built."""

        with self.query_activity():
            return self._slowlog_event_detail(event_id, settings, instance)

    def _slowlog_event_detail(
        self, event_id: str, settings: Settings, instance: str = ""
    ) -> dict[str, Any] | None:
        slowlog = self.slowlog_index.event_detail(event_id, instance)
        cutoff_us = int(
            (
                datetime.now(UTC) - timedelta(days=settings.retention_days)
            ).timestamp()
            * 1_000_000
        )
        if (
            slowlog is not None
            and int(slowlog.get("event_epoch_us") or 0) >= cutoff_us
        ):
            slowlog["tiers_used"] = ["slowlog-index"]
            return slowlog
        return None

    def _event_detail_tiered_impl(
        self,
        event_id: str,
        settings: Settings,
        archive: OssArchive | None,
        locator: str = "",
    ) -> dict[str, Any] | None:
        part: dict[str, Any] | None = None
        row_groups: list[int] | None = None
        if locator and ":" in locator:
            part_identity, group_text = locator.split(":", 1)
            if len(part_identity) == 64:
                part = self.metadata.part_by_logical_id(part_identity)
                if part is None:
                    # Existing links created before logical identities remain valid.
                    part = self.metadata.part_by_sha256(part_identity)
                if group_text != "*":
                    try:
                        row_groups = [
                            int(value)
                            for value in group_text.split(",")
                            if value.strip()
                        ]
                    except ValueError:
                        return None
        if part is None:
            return self.event_detail(event_id, settings.retention_days)
        cutoff = int(
            (datetime.now(UTC) - timedelta(days=settings.retention_days)).timestamp()
            * 1_000_000
        )
        return self._event_detail_from_part(
            part,
            event_id,
            cutoff,
            row_groups,
            archive,
        )

    def export_csv(
        self, query: dict[str, Any], retention_days: int, max_rows: int = 100_000
    ) -> tuple[Path, int]:
        export_cutoff = time.time() - 24 * 60 * 60
        for old_export in self.paths["exports"].glob("binlog-events-*.csv"):
            try:
                if old_export.is_file() and old_export.stat().st_mtime < export_cutoff:
                    old_export.unlink()
            except OSError:
                pass
        query = dict(query)
        query["limit"] = min(max_rows, 1000)
        query["offset"] = 0
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        path = self.paths["exports"] / f"binlog-events-{timestamp}-{uuid.uuid4().hex[:6]}.csv"
        count = 0
        offset = 0
        with path.open("w", encoding="utf-8-sig", newline="") as handle:
            writer: csv.DictWriter | None = None
            while count < max_rows:
                query["offset"] = offset
                page = self.query_events(query, retention_days)
                rows = page["rows"]
                if not rows:
                    break
                if writer is None:
                    writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
                    writer.writeheader()
                writer.writerows(rows)
                count += len(rows)
                offset += len(rows)
                if not page["has_more"]:
                    break
        return path, count

    def export_csv_tiered(
        self,
        query: dict[str, Any],
        settings: Settings,
        archive: OssArchive | None,
        max_rows: int = 100_000,
    ) -> tuple[Path, int]:
        export_cutoff = time.time() - 24 * 60 * 60
        for old_export in self.paths["exports"].glob("binlog-events-*.csv"):
            try:
                if old_export.is_file() and old_export.stat().st_mtime < export_cutoff:
                    old_export.unlink()
            except OSError:
                pass
        max_rows = min(max(int(max_rows), 1), 100_000)
        export_query = dict(query)
        export_query["limit"] = max_rows
        export_query["offset"] = 0
        result = self.query_events_tiered(
            export_query,
            settings,
            archive,
            limit_cap=max_rows,
        )
        if not result["coverage_found"]:
            raise StorageError(
                "本地和 OSS 均未找到查询时间范围",
                "TIER_COVERAGE_MISSING",
            )
        rows = result["rows"]
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        path = self.paths["exports"] / (
            f"binlog-events-{timestamp}-{uuid.uuid4().hex[:6]}.csv"
        )
        with path.open("w", encoding="utf-8-sig", newline="") as handle:
            if rows:
                writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
        return path, len(rows)

    def query_cache_stats(self) -> dict[str, int]:
        return {"bytes": 0, "part_count": 0}

    def enforce_query_cache_limit(self, max_bytes: int) -> dict[str, Any]:
        result = self.purge_legacy_body_caches()
        return {
            "limit_bytes": 0,
            "before_bytes": result["deleted_bytes"],
            "after_bytes": 0,
            "evicted_parts": result["deleted_files"],
            "evicted_bytes": result["deleted_bytes"],
            "errors": result["errors"],
        }

    def enforce_local_cache_limit(self, max_bytes: int) -> dict[str, Any]:
        with self._local_cache_enforce_lock:
            limit = max(int(max_bytes), 0)
            before = 0
            after = 0
            evicted = 0
            evicted_bytes = 0
            blocked_unarchived = 0
            blocked_unindexed = 0
            errors: list[str] = []
            pending_unindexed: list[tuple[dict[str, Any], int]] = []
            for part, path, size in self._local_body_entries():
                before += size
                after += size
                if not str(part.get("oss_key") or ""):
                    blocked_unarchived += size
                    continue
                if not self.search_index.is_current(part):
                    pending_unindexed.append((part, size))
                    blocked_unindexed += size
                    continue
                try:
                    removed = self.release_archived_body(part)
                    if removed:
                        evicted += 1
                        evicted_bytes += removed
                        after -= removed
                except Exception as exc:
                    errors.append(f"{path.name}: {exc}")
            # Local entries are newest-first. Keep the newest unindexed
            # handoff bodies and evict the oldest only above the hard limit.
            for part, size in reversed(pending_unindexed):
                if after <= limit:
                    break
                try:
                    removed = self.release_archived_body(part)
                    if removed:
                        evicted += 1
                        evicted_bytes += removed
                        after -= removed
                        blocked_unindexed -= size
                except Exception as exc:
                    errors.append(f"{Path(str(part['path'])).name}: {exc}")
            return {
                "limit_bytes": limit,
                "before_bytes": before,
                "after_bytes": after,
                "evicted_parts": evicted,
                "evicted_bytes": evicted_bytes,
                "errors": errors,
                "blocked_unarchived": blocked_unarchived,
                "blocked_unindexed": blocked_unindexed,
            }

    def cleanup(
        self,
        retention_days: int,
        *,
        archive_enabled: bool = False,
    ) -> dict[str, Any]:
        self._invalidate_local_part_cache()
        cutoff_us = int(
            (datetime.now(UTC) - timedelta(days=retention_days)).timestamp() * 1_000_000
        )
        deleted_parts = 0
        rewritten_parts = 0
        removed_rows = 0
        errors: list[str] = []
        for part in self.metadata.list_parts(limit=1_000_000):
            path = Path(part["path"])
            try:
                if int(part["max_event_epoch_us"]) < cutoff_us:
                    removed_rows += int(part["row_count"])
                    with self._part_body_lock(path):
                        path.unlink(missing_ok=True)
                        self._remove_body_version(path)
                        self.search_index.remove_part(str(path))
                        self.slowlog_index.remove_path(str(path))
                        self.metadata.delete_part(str(path))
                    deleted_parts += 1
                    continue
                if not path.is_file():
                    self._remove_body_version(path)
                    if not str(part.get("oss_key") or ""):
                        self.slowlog_index.remove_path(str(path))
                        self.metadata.delete_part(str(path))
                    continue
                if int(part["min_event_epoch_us"]) >= cutoff_us:
                    continue
                if archive_enabled and str(part.get("oss_key") or ""):
                    # OSS keeps the immutable source object for its lifecycle.
                    # Query predicates enforce the logical cutoff without
                    # rewriting a locally cached copy into a new archive object.
                    continue
                # A short same-directory name preserves atomic replacement and
                # remains usable by DuckDB on non-long-path-aware Windows builds.
                temp = path.with_name(f".ret-{uuid.uuid4().hex[:12]}.parquet")
                conn = self._duckdb_connect()
                try:
                    conn.execute(
                        "COPY (SELECT * FROM read_parquet("
                        + _sql_string(str(path))
                        + ") WHERE event_epoch_us >= "
                        + str(cutoff_us)
                        + " ORDER BY lower(database_name), lower(table_name), "
                        "operation, floor(event_epoch_us / 300000000), "
                        "event_epoch_us, end_position, row_index) TO "
                        + _sql_string(str(temp))
                        + " (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 1, "
                        "ROW_GROUP_SIZE 8192, KV_METADATA {app: 'RDS Binlog Insight', "
                        "schema_version: '2', "
                        "layout: 'time-table-operation-rowgroup'})"
                    )
                    row = conn.execute(
                        "SELECT count(*), min(event_epoch_us), max(event_epoch_us) "
                        f"FROM read_parquet({_sql_string(str(temp))})"
                    ).fetchone()
                finally:
                    conn.close()
                if not row or int(row[0]) == 0:
                    if temp.exists():
                        temp.unlink()
                    removed_rows += int(part["row_count"])
                    path.unlink()
                    self.search_index.remove_part(str(path))
                    self.slowlog_index.remove_path(str(path))
                    self.metadata.delete_part(str(path))
                    deleted_parts += 1
                    continue
                os.replace(temp, path)
                new_values = {
                    "row_count": int(row[0]),
                    "min_event_epoch_us": int(row[1]),
                    "max_event_epoch_us": int(row[2]),
                    "size_bytes": path.stat().st_size,
                    "sha256": _sha256(path),
                }
                removed_rows += int(part["row_count"]) - int(row[0])
                self.metadata.update_part(str(path), new_values)
                refreshed = self.metadata.part_by_path(str(path)) or {
                    **part,
                    **new_values,
                    "path": str(path),
                }
                self.search_index.index_parquet(refreshed, path)
                if str(refreshed.get("log_file_name") or "").startswith(
                    SLOW_LOG_FILE_PREFIX
                ):
                    self.slowlog_index.enqueue_parts([refreshed])
                rewritten_parts += 1
            except Exception as exc:
                errors.append(f"{path.name}: {exc}")
        # 分区被删除或重写后，对应的分析聚合必须一并失效：重写会换 sha256，
        # 覆盖检查会把它判为未覆盖并重建；删除则要真正回收行。
        pruned_analytics = 0
        try:
            pruned_analytics = self.analytics_index.prune_orphans(
                str(part["path"])
                for part in self.metadata.list_parts(
                    limit=1_000_000,
                    visible_only=False,
                )
            )
        except Exception as exc:
            errors.append(f"analytics prune: {exc}")
        return {
            "cutoff_epoch_us": cutoff_us,
            "deleted_parts": deleted_parts,
            "rewritten_parts": rewritten_parts,
            "removed_rows": removed_rows,
            "pruned_analytics_parts": pruned_analytics,
            "errors": errors,
        }

    def local_body_stats(self) -> dict[str, int]:
        total_bytes = 0
        part_count = 0
        for path in self.paths["events"].rglob("*.parquet"):
            try:
                if path.is_file():
                    total_bytes += path.stat().st_size
                    part_count += 1
            except OSError:
                continue
        return {"bytes": total_bytes, "part_count": part_count}

    def _load_storage_stats_snapshot(self) -> None:
        payload = read_json_status(self._storage_stats_snapshot_path)
        snapshot = payload.get("data")
        try:
            retention_days = int(payload.get("retentionDays"))
        except (TypeError, ValueError):
            return
        if not isinstance(snapshot, dict):
            return
        try:
            age_seconds = max(
                time.time() - self._storage_stats_snapshot_path.stat().st_mtime,
                0.0,
            )
        except OSError:
            return
        now = time.monotonic()
        with self._storage_stats_lock:
            self._storage_stats_snapshot = copy.deepcopy(snapshot)
            self._storage_stats_retention_days = retention_days
            self._storage_stats_refreshed_monotonic = now - age_seconds
            self._storage_stats_last_persisted_monotonic = now - age_seconds
            self._storage_stats_ready.set()

    def _publish_storage_stats_snapshot(
        self,
        snapshot: dict[str, Any],
        *,
        retention_days: int,
        persist: bool,
    ) -> None:
        published = copy.deepcopy(snapshot)
        with self._storage_stats_lock:
            self._storage_stats_snapshot = published
            self._storage_stats_retention_days = int(retention_days)
            self._storage_stats_refreshed_monotonic = time.monotonic()
            self._storage_stats_ready.set()
        if not persist:
            return
        try:
            write_json_status(
                self._storage_stats_snapshot_path,
                {
                    "retentionDays": int(retention_days),
                    "data": published,
                },
            )
        except Exception:
            LOGGER.exception("持久化 storage 统计快照失败，继续使用内存中的最后成功值")
        else:
            with self._storage_stats_lock:
                self._storage_stats_last_persisted_monotonic = time.monotonic()

    def start_storage_stats_snapshot_refresh(
        self,
        retention_days: int | None = None,
    ) -> bool:
        now = time.monotonic()
        with self._storage_stats_lock:
            snapshot = self._storage_stats_snapshot
            same_retention = retention_days is None or (
                self._storage_stats_retention_days == int(retention_days)
            )
            fresh = bool(
                snapshot is not None
                and same_retention
                and now - self._storage_stats_refreshed_monotonic
                < _STORAGE_STATS_CACHE_SECONDS
            )
            if fresh or self._storage_stats_refreshing:
                return False
            self._storage_stats_refreshing = True
        try:
            threading.Thread(
                target=self._refresh_storage_stats_snapshot_worker,
                args=(retention_days,),
                name="storage-stats-refresh",
                daemon=True,
            ).start()
        except Exception:
            with self._storage_stats_lock:
                self._storage_stats_refreshing = False
                self._storage_stats_ready.set()
            raise
        return True

    def _refresh_storage_stats_snapshot_worker(
        self,
        retention_days: int | None,
    ) -> None:
        try:
            resolved_retention_days = int(
                retention_days
                if retention_days is not None
                else self.metadata.load_settings().retention_days
            )
            snapshot = self._build_storage_stats_snapshot(
                resolved_retention_days
            )
            with self._storage_stats_lock:
                persist = bool(
                    not self._storage_stats_snapshot_path.is_file()
                    or time.monotonic()
                    - self._storage_stats_last_persisted_monotonic
                    >= _STORAGE_STATS_PERSIST_SECONDS
                )
            self._publish_storage_stats_snapshot(
                snapshot,
                retention_days=resolved_retention_days,
                persist=persist,
            )
        except Exception:
            LOGGER.exception(
                "后台刷新 storage 统计快照失败，继续提供最后一次成功快照"
            )
        finally:
            with self._storage_stats_lock:
                self._storage_stats_refreshing = False
                self._storage_stats_ready.set()

    def _build_storage_stats_snapshot(
        self,
        retention_days: int,
    ) -> dict[str, Any]:
        result = self.metadata.storage_metadata_stats()
        result["metadata_bytes"] = (
            self.metadata.path.stat().st_size if self.metadata.path.exists() else 0
        )
        result["download_bytes"] = sum(
            path.stat().st_size
            for path in self.paths["downloads"].glob("*")
            if path.is_file()
        )
        result["retention_days"] = retention_days
        result["format"] = "Parquet"
        result["compression"] = "ZSTD 1 (latest 1 day) + ZSTD 9 (cold)"
        parts = self.metadata.list_parts(limit=200)
        for part in parts:
            local_present = Path(str(part["path"])).is_file()
            part["local_present"] = local_present
            part["archive_present"] = bool(str(part.get("oss_key") or ""))
        local_body = self.local_body_stats()
        result["local_parquet_bytes"] = int(local_body["bytes"])
        result["local_part_count"] = int(local_body["part_count"])
        result["query_cache_bytes"] = 0
        result["query_cache_part_count"] = 0
        external_status = read_json_status(
            self.paths["index"] / SUPERVISOR_STATUS_NAME
        )
        external_index = external_status.get("index")
        if (
            isinstance(external_index, dict)
            and {"part_count", "block_count", "size_bytes"}
            <= external_index.keys()
        ):
            # The index writer already publishes these counters after every
            # bounded batch. Reading that small atomic status file avoids
            # counting a busy 16 GB FTS database from the HTTP request path.
            result["index"] = {
                **external_index,
                "stats_source": "index-worker-status",
                "stats_updated_at": str(
                    external_status.get("updatedAt") or ""
                ),
            }
        else:
            result["index"] = self.search_index.stats()
        result["index_bytes"] = int(result["index"]["size_bytes"])
        external_analytics = external_status.get("analytics")
        if (
            isinstance(external_analytics, dict)
            and {"parts", "index_bytes"} <= external_analytics.keys()
        ):
            result["analytics"] = {
                **external_analytics,
                "stats_source": "index-worker-status",
                "stats_updated_at": str(
                    external_status.get("updatedAt") or ""
                ),
            }
        else:
            result["analytics"] = self.analytics_index.stats()
        result["slowlog_index"] = self.slowlog_index.stats()
        slowlog_worker = read_json_status(
            self.paths["index"] / SLOWLOG_WORKER_STATUS_NAME
        )
        if slowlog_worker:
            result["slowlog_worker"] = slowlog_worker
        clickhouse_worker = read_json_status(
            self.paths["logs"] / "clickhouse-worker-status.json"
        )
        if clickhouse_worker:
            result["clickhouse_worker"] = clickhouse_worker
        clickhouse_oss_worker = read_json_status(
            self.paths["logs"] / "clickhouse-oss-worker-status.json"
        )
        if clickhouse_oss_worker:
            result["clickhouse_oss_worker"] = clickhouse_oss_worker
        clickhouse_slowlog_worker = read_json_status(
            self.paths["logs"] / CLICKHOUSE_SLOWLOG_WORKER_STATUS_NAME
        )
        if clickhouse_slowlog_worker:
            result["clickhouse_slowlog_worker"] = (
                clickhouse_slowlog_worker
            )
        if self.clickhouse_backend is not None:
            # Local manifest reads only: /api/storage must never block on a
            # remote ClickHouse health/statistics request.
            result["clickhouse_hot"] = self.clickhouse_backend.stats()
        if self.clickhouse_slowlog_backend is not None:
            result["clickhouse_slowlog"] = (
                self.clickhouse_slowlog_backend.stats()
            )
        result["catalog"] = self.metadata.part_catalog_stats()
        result["parts"] = parts
        return result

    def stats(self, retention_days: int) -> dict[str, Any]:
        resolved_retention_days = int(retention_days)
        now = time.monotonic()
        with self._storage_stats_lock:
            snapshot = copy.deepcopy(self._storage_stats_snapshot)
            snapshot_retention_days = self._storage_stats_retention_days
            stale = bool(
                snapshot is None
                or snapshot_retention_days != resolved_retention_days
                or now - self._storage_stats_refreshed_monotonic
                >= _STORAGE_STATS_CACHE_SECONDS
            )
        if snapshot is not None:
            if snapshot_retention_days != resolved_retention_days:
                snapshot["retention_days"] = resolved_retention_days
            if stale:
                self.start_storage_stats_snapshot_refresh(
                    resolved_retention_days
                )
            return snapshot

        self.start_storage_stats_snapshot_refresh(resolved_retention_days)
        self._storage_stats_ready.wait(0.5)
        with self._storage_stats_lock:
            snapshot = copy.deepcopy(self._storage_stats_snapshot)
        if snapshot is not None:
            if self._storage_stats_retention_days != resolved_retention_days:
                snapshot["retention_days"] = resolved_retention_days
            return snapshot

        # Cold-start fallback for callers that construct EventStorage outside the
        # HTTP application. The server starts an asynchronous prewarm before it
        # accepts traffic, so production requests normally never take this path.
        snapshot = self._build_storage_stats_snapshot(resolved_retention_days)
        self._publish_storage_stats_snapshot(
            snapshot,
            retention_days=resolved_retention_days,
            persist=True,
        )
        return copy.deepcopy(snapshot)


def ingest_ndjson_file_detached(
    payload: dict[str, Any],
) -> tuple[int, list[dict[str, Any]]]:
    """Transform one parser chunk without touching shared SQLite metadata."""
    storage = EventStorage.__new__(EventStorage)
    storage.paths = ensure_data_dirs(Path(str(payload["data_dir"])))
    storage._part_body_locks = [threading.RLock() for _ in range(256)]
    return storage.ingest_ndjson_file(
        file_id=str(payload["file_id"]),
        instance_id=str(payload["instance_id"]),
        host_instance_id=str(payload["host_instance_id"]),
        source_file_name=str(payload["source_file_name"]),
        ndjson_path=Path(str(payload["ndjson_path"])),
        part_key=str(payload.get("part_key") or ""),
        append=True,
        publish_metadata=False,
    )
