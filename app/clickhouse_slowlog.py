from __future__ import annotations

import argparse
import copy
import http.client
import json
import logging
import os
import re
import signal
import shutil
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlencode

from .clickhouse_client import ClickHouseClient, ClickHouseConfig, ClickHouseError
from .clickhouse_manifest import (
    ClickHouseManifest,
    ClickHouseManifestError,
    part_identity,
)
from .clickhouse_query import query_rows_with_cancel
from .maintenance_status import read_json_status
from .slowlog_index import (
    BUCKET_US,
    SLOWLOG_ORDER_KEYS,
    SQL_ORDERS,
    SlowLogIndex,
    slowlog_empty_transactions,
    slowlog_order_key,
    slowlog_statement_result,
    slowlog_trend_width,
)


LOGGER = logging.getLogger(__name__)
DAY_US = 24 * 60 * 60 * 1_000_000
_TABLE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")
_STATEMENT_GROUP_MASK = 15
_OBJECT_GROUP_MASK = 19
_OPERATION_GROUP_MASK = 29
_TREND_GROUP_MASK = 30
_COVERAGE_CACHE_SECONDS = 2.0
# The production all-instance rollup peaks just above the generic client's
# 500 MB limit.  One serialized slow-log query may use 750 MB; if cardinality
# grows past 600 MB ClickHouse spills aggregation state instead of aborting and
# forcing the API onto the much slower exact SQLite fallback.
_SLOWLOG_QUERY_MAX_MEMORY_BYTES = 750_000_000
_EXTERNAL_GROUP_BY_BYTES = 600_000_000


def _bool_env(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)) or default)
    except ValueError:
        value = default
    return min(max(value, minimum), maximum)


def _float_env(
    name: str,
    default: float,
    minimum: float,
    maximum: float,
) -> float:
    try:
        value = float(os.environ.get(name, str(default)) or default)
    except ValueError:
        value = default
    return min(max(value, minimum), maximum)


@dataclass(frozen=True, slots=True)
class ClickHouseSlowLogConfig:
    base: ClickHouseConfig
    enabled: bool
    serving_enabled: bool
    table: str
    retention_days: int
    batch_parts: int
    reconcile_seconds: int
    idle_seconds: float
    source_max_pending_parts: int
    source_max_pending_age_seconds: int
    source_recovery_ratio: float
    source_status_stale_seconds: int

    @classmethod
    def from_env(cls) -> "ClickHouseSlowLogConfig":
        base = ClickHouseConfig.from_env()
        table_name = os.environ.get(
            "RDS_BINLOG_CLICKHOUSE_SLOWLOG_TABLE",
            "slowlog_events",
        ).strip() or "slowlog_events"
        table = f"{base.database}.{table_name}"
        if not _TABLE.fullmatch(table):
            raise ValueError(f"Unsafe ClickHouse slow-log table: {table!r}")
        enabled = base.enabled and _bool_env(
            "RDS_BINLOG_CLICKHOUSE_SLOWLOG_ENABLED",
            True,
        )
        return cls(
            base=base,
            enabled=enabled,
            serving_enabled=enabled
            and _bool_env(
                "RDS_BINLOG_CLICKHOUSE_SLOWLOG_SERVING_ENABLED",
                False,
            ),
            table=table,
            retention_days=_int_env(
                "RDS_BINLOG_CLICKHOUSE_SLOWLOG_RETENTION_DAYS",
                61,
                2,
                3650,
            ),
            batch_parts=_int_env(
                "RDS_BINLOG_CLICKHOUSE_SLOWLOG_BATCH_PARTS",
                64,
                1,
                256,
            ),
            reconcile_seconds=_int_env(
                "RDS_BINLOG_CLICKHOUSE_SLOWLOG_RECONCILE_SECONDS",
                300,
                10,
                3600,
            ),
            idle_seconds=float(
                _int_env(
                    "RDS_BINLOG_CLICKHOUSE_SLOWLOG_IDLE_SECONDS",
                    2,
                    1,
                    60,
                )
            ),
            source_max_pending_parts=_int_env(
                "RDS_BINLOG_CLICKHOUSE_SLOWLOG_SOURCE_MAX_PENDING_PARTS",
                128,
                1,
                100_000,
            ),
            source_max_pending_age_seconds=_int_env(
                "RDS_BINLOG_CLICKHOUSE_SLOWLOG_SOURCE_MAX_PENDING_AGE_SECONDS",
                600,
                30,
                86_400,
            ),
            source_recovery_ratio=_float_env(
                "RDS_BINLOG_CLICKHOUSE_SLOWLOG_SOURCE_RECOVERY_RATIO",
                0.8,
                0.1,
                1.0,
            ),
            source_status_stale_seconds=_int_env(
                "RDS_BINLOG_CLICKHOUSE_SLOWLOG_SOURCE_STATUS_STALE_SECONDS",
                30,
                5,
                3600,
            ),
        )


class SourceIndexLagPaused(RuntimeError):
    """The exact source index needs the shared I/O lane before ClickHouse."""


class SourceIndexPriorityGate:
    """Bound source-index lag with fail-closed hysteresis.

    The source worker already publishes an atomic, small JSON status file on
    every loop.  Reading it avoids another scan of the multi-gigabyte SQLite
    index and makes the live exact fallback the explicit priority consumer of
    the shared disk while a historical ClickHouse backfill is running.
    """

    def __init__(
        self,
        status_path: Path,
        *,
        max_pending_parts: int = 128,
        max_pending_age_seconds: int = 600,
        recovery_ratio: float = 0.8,
        max_stale_seconds: int = 30,
        block_paused_state: bool = True,
    ) -> None:
        self.status_path = Path(status_path)
        self.max_pending_parts = max(int(max_pending_parts), 1)
        self.max_pending_age_seconds = max(
            int(max_pending_age_seconds), 1
        )
        self.recovery_ratio = min(max(float(recovery_ratio), 0.1), 1.0)
        self.max_stale_seconds = max(int(max_stale_seconds), 1)
        self.block_paused_state = bool(block_paused_state)
        self.paused = False

    @staticmethod
    def _updated_at(value: object) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    def check(self, *, now: datetime | None = None) -> dict[str, Any]:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        status = read_json_status(self.status_path)
        updated_at = self._updated_at(status.get("updatedAt"))
        stats = status.get("stats")
        reasons: list[str] = []
        if updated_at is None:
            reasons.append("source worker status is missing or invalid")
            stale_seconds = self.max_stale_seconds + 1
        else:
            stale_seconds = max(
                int((current - updated_at).total_seconds()),
                0,
            )
            if stale_seconds > self.max_stale_seconds:
                reasons.append(
                    "source worker status is stale: "
                    f"{stale_seconds}s > {self.max_stale_seconds}s"
                )
        if not isinstance(stats, dict):
            stats = {}
            reasons.append("source worker statistics are unavailable")

        pending = max(int(stats.get("pending_parts") or 0), 0)
        failed = max(int(stats.get("failed_parts") or 0), 0)
        age = max(
            int(stats.get("oldest_pending_age_seconds") or 0),
            0,
        )
        pending_limit = self.max_pending_parts
        age_limit = self.max_pending_age_seconds
        if self.paused:
            pending_limit = max(
                int(self.max_pending_parts * self.recovery_ratio),
                1,
            )
            age_limit = max(
                int(
                    self.max_pending_age_seconds * self.recovery_ratio
                ),
                1,
            )

        if not bool(status.get("running")):
            reasons.append("source slow-log worker is not running")
        source_state = str(status.get("state") or "")
        if source_state == "paused" and self.block_paused_state:
            reasons.append("source slow-log worker is paused")
        if source_state == "error":
            reasons.append("source slow-log worker is in error state")
        if failed:
            reasons.append(f"source slow-log index has {failed} failed parts")
        # A running reconcile sweep only re-queues crash-gap parts and says
        # nothing about lag by itself.  At 128 registry rows per source
        # iteration it would otherwise pause this lane for most of every hour
        # once the registry is large (2026-09-03: 57k parts, sweep 445
        # iterations).  Pending count and age below capture real backlog; the
        # flag stays in the result for observability only.
        if pending > pending_limit:
            reasons.append(
                f"source pending parts {pending} > {pending_limit}"
            )
        if age > age_limit:
            reasons.append(f"source pending age {age}s > {age_limit}s")

        result = {
            "pending_parts": pending,
            "failed_parts": failed,
            "oldest_pending_age_seconds": age,
            "reconcile_complete": bool(stats.get("reconcile_complete")),
            "status_stale_seconds": stale_seconds,
        }
        if reasons:
            self.paused = True
            raise SourceIndexLagPaused("; ".join(reasons))
        self.paused = False
        return result


SLOWLOG_INPUT_TYPES: tuple[tuple[str, str], ...] = (
    ("event_id", "String"),
    ("event_epoch_us", "Int64"),
    ("event_date", "Date"),
    ("instance_id", "String"),
    ("node_id", "String"),
    ("operation", "String"),
    ("database_name", "String"),
    ("table_name", "String"),
    ("fingerprint", "String"),
    ("sql_id", "String"),
    ("action", "String"),
    ("normalized_sql", "String"),
    ("sample_sql", "String"),
    ("sql_bytes", "UInt32"),
    ("query_time_ms", "UInt64"),
    ("lock_time_ms", "UInt64"),
    ("rows_examined", "UInt64"),
    ("rows_sent", "UInt64"),
    ("database_account", "String"),
    ("client_ip", "String"),
    ("thread_id", "Int64"),
    ("source_file_name", "String"),
    ("_source_part_path", "String"),
    ("_source_part_id", "String"),
    ("_source_part_sha256", "String"),
    ("_content_revision", "UInt64"),
)


class ClickHouseSlowLogClient:
    def __init__(
        self,
        config: ClickHouseSlowLogConfig,
        *,
        client: ClickHouseClient | None = None,
    ) -> None:
        self.config = config
        self.client = client or ClickHouseClient(config.base)

    @staticmethod
    def _job_key(job: dict[str, Any]) -> tuple[str, str]:
        return str(job["part_path"]), str(job["logical_part_id"])

    def _predicate(
        self,
        jobs: Iterable[dict[str, Any]],
    ) -> tuple[str, dict[str, str | int]]:
        clauses: list[str] = []
        parameters: dict[str, str | int] = {}
        for position, job in enumerate(jobs):
            path_name = f"path_{position}"
            identity_name = f"identity_{position}"
            parameters[path_name] = str(job["part_path"])
            parameters[identity_name] = str(job["logical_part_id"])
            clauses.append(
                "(_source_part_path={"
                + path_name
                + ":String} AND _source_part_id={"
                + identity_name
                + ":String})"
            )
        return " OR ".join(clauses) or "0", parameters

    def part_counts(
        self,
        jobs: Iterable[dict[str, Any]],
    ) -> dict[tuple[str, str], int]:
        batch = list(jobs)
        expected = {self._job_key(job): 0 for job in batch}
        if not batch:
            return expected
        predicate, parameters = self._predicate(batch)
        rows = self.client.json_rows(
            f"""
            SELECT _source_part_path,_source_part_id,count() AS rows
            FROM {self.config.table}
            WHERE {predicate}
            GROUP BY _source_part_path,_source_part_id
            """,
            parameters=parameters,
            timeout=30,
        )
        for row in rows:
            expected[
                (
                    str(row["_source_part_path"]),
                    str(row["_source_part_id"]),
                )
            ] = int(row["rows"] or 0)
        return expected

    def delete_occurrences(self, jobs: Iterable[dict[str, Any]]) -> None:
        batch = list(jobs)
        if not batch:
            return
        existing = self.part_counts(batch)
        targets = [job for job in batch if existing[self._job_key(job)] > 0]
        if not targets:
            return
        predicate, parameters = self._predicate(targets)
        self.client.query(
            f"ALTER TABLE {self.config.table} DELETE WHERE {predicate}",
            parameters=parameters,
            settings={"mutations_sync": 1, "max_threads": 1},
            timeout=600,
        )

    def insert_parquet(self, path: Path) -> None:
        columns = ", ".join(name for name, _kind in SLOWLOG_INPUT_TYPES)
        input_schema = ", ".join(
            f"{name} {kind}" for name, kind in SLOWLOG_INPUT_TYPES
        )
        sql = (
            f"INSERT INTO {self.config.table} ({columns}) "
            f"SELECT {columns} FROM input('{input_schema}') FORMAT Parquet"
        )
        values: dict[str, str | int] = {
            "query": sql,
            "max_threads": 2,
            "max_insert_threads": 1,
            "max_memory_usage": 1_000_000_000,
            "input_format_parallel_parsing": 0,
            "input_format_parquet_use_native_reader_v3": 0,
            "input_format_parquet_max_block_size": 8192,
            "wait_end_of_query": 1,
            "max_execution_time": 300,
        }
        connection = http.client.HTTPConnection(
            self.config.base.host,
            self.config.base.port,
            timeout=360,
        )
        try:
            connection.putrequest("POST", "/?" + urlencode(values))
            for name, value in self.client._headers().items():
                connection.putheader(name, value)
            connection.putheader("Content-Type", "application/octet-stream")
            connection.putheader("Content-Length", str(path.stat().st_size))
            connection.endheaders()
            with path.open("rb") as source:
                while chunk := source.read(1024 * 1024):
                    connection.send(chunk)
            response = connection.getresponse()
            body = response.read().decode("utf-8", errors="replace")
            if response.status != 200:
                raise ClickHouseError(
                    f"ClickHouse slow-log insert HTTP {response.status}: "
                    f"{body[:1000]}"
                )
        except (OSError, http.client.HTTPException) as exc:
            raise ClickHouseError(
                f"ClickHouse slow-log insert failed: {exc}"
            ) from exc
        finally:
            connection.close()


def reconcile_slowlog_manifest(
    metadata: Any,
    manifest: ClickHouseManifest,
    *,
    retention_days: int,
    now_us: int | None = None,
    page_size: int = 2048,
) -> dict[str, int]:
    end_us = int(now_us or time.time_ns() // 1000)
    start_us = end_us - max(int(retention_days), 1) * DAY_US
    after_path = ""
    source_parts = 0
    eligible: list[dict[str, Any]] = []
    while True:
        page = metadata.slowlog_parts_page(
            after_path=after_path,
            limit=max(int(page_size), 1),
        )
        source_parts += len(page)
        eligible.extend(
            part
            for part in page
            if int(part.get("max_event_epoch_us") or 0) >= start_us
            and int(part.get("min_event_epoch_us") or 0) <= end_us
        )
        if len(page) < max(int(page_size), 1):
            break
        after_path = str(page[-1]["path"])
    return manifest.reconcile(
        eligible,
        start_epoch_us=start_us,
        end_epoch_us=end_us,
        source_parts=source_parts,
    )


def ingest_slowlog_batch(
    metadata: Any,
    index: SlowLogIndex,
    manifest: ClickHouseManifest,
    client: Any,
    *,
    batch_parts: int,
    scratch: Path,
) -> dict[str, int]:
    scratch = Path(scratch)
    scratch.mkdir(parents=True, exist_ok=True)
    claims: list[dict[str, Any]] = []
    for _position in range(max(int(batch_parts), 1)):
        claim = manifest.claim_next()
        if claim is None:
            break
        claims.append(claim)
    result = {
        "claimed_parts": len(claims),
        "loaded_parts": 0,
        "loaded_rows": 0,
        "deleted_parts": 0,
        "waiting_parts": 0,
        "failed_parts": 0,
    }
    if not claims:
        return result

    deletes = [job for job in claims if job.get("job_kind") == "delete"]
    loads = [job for job in claims if job.get("job_kind") == "load"]
    if deletes:
        try:
            client.delete_occurrences(deletes)
        except Exception as exc:
            for job in deletes:
                manifest.mark_failed(
                    str(job["part_path"]),
                    str(job["logical_part_id"]),
                    str(exc),
                )
                result["failed_parts"] += 1
        else:
            for job in deletes:
                manifest.mark_retired(
                    str(job["part_path"]),
                    str(job["logical_part_id"]),
                )
                result["deleted_parts"] += 1

    current_parts: list[dict[str, Any]] = []
    current_jobs: dict[str, dict[str, Any]] = {}
    for job in loads:
        path = str(job["part_path"])
        current = metadata.part_by_path(path)
        current_identity = str(
            (current or {}).get("logical_part_id")
            or (current or {}).get("sha256")
            or ""
        )
        if current is None or current_identity != str(job["logical_part_id"]):
            manifest.mark_retired(path, str(job["logical_part_id"]))
            result["deleted_parts"] += 1
            continue
        current_parts.append(current)
        current_jobs[path] = job
    if not current_parts:
        return result

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".clickhouse-slowlog-",
        suffix=".parquet",
        dir=scratch,
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        try:
            exported = index.export_clickhouse_parts(current_parts, temporary)
        except Exception as exc:
            for job in current_jobs.values():
                manifest.mark_failed(
                    str(job["part_path"]),
                    str(job["logical_part_id"]),
                    str(exc),
                )
                result["failed_parts"] += 1
            raise
        for missing in exported["missing_parts"]:
            job = current_jobs[str(missing)]
            manifest.release_pending(
                str(job["part_path"]),
                str(job["logical_part_id"]),
                reason="waiting for the exact slow-log index",
                delay_seconds=10,
            )
            result["waiting_parts"] += 1
        part_rows = {
            str(path): int(rows)
            for path, rows in exported["part_rows"].items()
        }
        ready_jobs = [current_jobs[path] for path in part_rows]
        if not ready_jobs:
            return result
        try:
            client.delete_occurrences(ready_jobs)
            if int(exported["exported_rows"] or 0):
                client.insert_parquet(temporary)
            observed = client.part_counts(ready_jobs)
            for job in ready_jobs:
                path = str(job["part_path"])
                identity = str(job["logical_part_id"])
                expected_rows = part_rows[path]
                actual_rows = int(observed.get((path, identity), 0))
                current = metadata.part_by_path(path)
                current_identity = str(
                    (current or {}).get("logical_part_id")
                    or (current or {}).get("sha256")
                    or ""
                )
                if current_identity != identity:
                    manifest.release_pending(
                        path,
                        identity,
                        reason="source identity changed during ClickHouse insert",
                    )
                    result["waiting_parts"] += 1
                elif actual_rows != expected_rows:
                    manifest.mark_failed(
                        path,
                        identity,
                        f"row verification failed: expected {expected_rows}, "
                        f"observed {actual_rows}",
                    )
                    result["failed_parts"] += 1
                else:
                    manifest.mark_ready(path, identity, actual_rows)
                    result["loaded_parts"] += 1
                    result["loaded_rows"] += actual_rows
        except Exception as exc:
            for job in ready_jobs:
                manifest.mark_failed(
                    str(job["part_path"]),
                    str(job["logical_part_id"]),
                    str(exc),
                )
                result["failed_parts"] += 1
            raise
        return result
    finally:
        temporary.unlink(missing_ok=True)


class _Parameters:
    def __init__(self) -> None:
        self.values: dict[str, str | int] = {}
        self.counter = 0

    def add(self, value: str | int, kind: str) -> str:
        self.counter += 1
        name = f"p{self.counter}"
        self.values[name] = value
        return "{" + name + ":" + kind + "}"


class ClickHouseSlowLogQueryBackend:
    def __init__(
        self,
        metadata: Any,
        data_dir: Path,
        config: ClickHouseSlowLogConfig | None = None,
        *,
        client: Any | None = None,
        manifest: Any | None = None,
        statement_index: SlowLogIndex | Any | None = None,
        table: str = "",
        serving_enabled: bool | None = None,
    ) -> None:
        self.metadata = metadata
        self.data_dir = Path(data_dir)
        self.config = config or ClickHouseSlowLogConfig.from_env()
        self.table = str(table or self.config.table)
        if not _TABLE.fullmatch(self.table):
            raise ValueError(f"Unsafe ClickHouse slow-log table: {self.table!r}")
        self.serving_enabled = (
            self.config.serving_enabled
            if serving_enabled is None
            else bool(serving_enabled)
        )
        self.client = client or ClickHouseClient(self.config.base)
        self.manifest = manifest or ClickHouseManifest(
            self.data_dir
            / "index"
            / "clickhouse"
            / "slowlog-manifest.sqlite3",
            run_migrations=False,
        )
        self.statement_index = statement_index or SlowLogIndex(
            self.data_dir / "index" / "slowlog.sqlite3",
            run_migrations=False,
        )
        self._coverage_cache_lock = threading.Lock()
        self._rollup_query_lock = threading.Lock()
        self._coverage_cache_key: tuple[tuple[str, str], ...] | None = None
        self._coverage_cache_value: dict[str, Any] | None = None
        self._coverage_cache_expires_monotonic = 0.0

    @classmethod
    def from_env(
        cls,
        metadata: Any,
        data_dir: Path,
        *,
        statement_index: SlowLogIndex | Any | None = None,
    ) -> "ClickHouseSlowLogQueryBackend | None":
        try:
            config = ClickHouseSlowLogConfig.from_env()
            if not config.enabled or not config.serving_enabled:
                return None
            return cls(
                metadata,
                data_dir,
                config,
                statement_index=statement_index,
            )
        except (ClickHouseManifestError, ClickHouseError, RuntimeError, ValueError):
            LOGGER.exception("ClickHouse slow-log serving backend is unavailable")
            return None

    @staticmethod
    def _window(
        query: dict[str, Any],
        retention_days: int,
    ) -> tuple[int, int] | None:
        now = datetime.now(UTC)
        now_us = int(now.timestamp() * 1_000_000)
        cutoff = int(
            (now - timedelta(days=max(int(retention_days), 1))).timestamp()
            * 1_000_000
        )
        start = max(int(query.get("start_epoch_us") or cutoff), cutoff)
        end = min(int(query.get("end_epoch_us") or now_us), now_us)
        if end < start:
            return None
        return start, end

    def _scope_sql(
        self,
        query: dict[str, Any],
        start_us: int,
        end_us: int,
    ) -> tuple[str, dict[str, str | int]]:
        parameters = _Parameters()
        clauses = [
            "event_epoch_us >= " + parameters.add(start_us, "Int64"),
            "event_epoch_us <= " + parameters.add(end_us, "Int64"),
        ]
        for key, column in (
            ("instance", "instance_id"),
            ("node_id", "node_id"),
            ("database", "database_name"),
            ("table", "table_name"),
            ("operation", "operation"),
        ):
            value = str(query.get(key) or "").strip()
            if not value:
                continue
            if key == "operation":
                value = value.upper()
            parameter = parameters.add(value, "String")
            if key in {"database", "table"}:
                clauses.append(f"lowerUTF8({column}) = lowerUTF8({parameter})")
            else:
                clauses.append(f"{column} = {parameter}")
        key = "tuple(_source_part_path,_source_part_id)"
        # Keep one narrow argMin state per canonical event. SQL bodies live in
        # the indexed SQLite statement dimension and are hydrated only for the
        # bounded top-N union after ClickHouse has finished numeric aggregation.
        # Duplicate occurrences emitted by the collectors retain identical
        # event time and metrics, so window-local election is equivalent to the
        # SQLite global election while preserving the time-first read path.
        scope = f"""
            SELECT instance_id AS scope_instance_id,
                   event_id AS scope_event_id,
                   event_epoch_us AS metric_event_epoch_us,
                   tupleElement(canonical,1) AS metric_node_id,
                   tupleElement(canonical,2) AS metric_operation,
                   tupleElement(canonical,3) AS metric_database_name,
                   tupleElement(canonical,4) AS metric_table_name,
                   tupleElement(canonical,5) AS metric_fingerprint,
                   tupleElement(canonical,6) AS metric_sql_id,
                   tupleElement(canonical,7) AS metric_action,
                   tupleElement(canonical,8) AS metric_sql_bytes,
                   tupleElement(canonical,9) AS metric_query_time_ms,
                   tupleElement(canonical,10) AS metric_lock_time_ms,
                   tupleElement(canonical,11) AS metric_rows_examined,
                   tupleElement(canonical,12) AS metric_rows_sent
            FROM (
                SELECT instance_id,event_epoch_us,event_id,
                       argMin(tuple(
                           node_id,operation,database_name,table_name,
                           fingerprint,sql_id,action,
                           sql_bytes,query_time_ms,lock_time_ms,
                           rows_examined,rows_sent
                       ),{key}) AS canonical
                FROM {self.table}
                WHERE {' AND '.join(clauses)}
                GROUP BY instance_id,event_epoch_us,event_id
            )
        """
        return scope, parameters.values

    @staticmethod
    def _integer(row: dict[str, Any], name: str) -> int:
        return int(row.get(name) or 0)

    def _hydrate_statement_profiles(
        self,
        groups: list[dict[str, Any]],
        instances: dict[str, tuple[str, ...]],
        limit: int,
    ) -> None:
        selected: set[str] = set()
        for keys in SLOWLOG_ORDER_KEYS.values():
            selected.update(
                str(row.get("fingerprint") or "")
                for row in sorted(
                    groups,
                    key=lambda row, order_keys=keys: slowlog_order_key(
                        row, order_keys
                    ),
                    reverse=True,
                )[:limit]
            )
        profile_keys = {
            (instance, fingerprint)
            for fingerprint in selected
            for instance in instances.get(fingerprint, ())
        }
        profiles = self.statement_index.statement_profiles(profile_keys)
        for row in groups:
            fingerprint = str(row.get("fingerprint") or "")
            if fingerprint not in selected:
                continue
            matches = [
                profiles[(instance, fingerprint)]
                for instance in instances.get(fingerprint, ())
                if (instance, fingerprint) in profiles
            ]
            if not matches:
                continue
            # Match the source SQLite LEFT JOIN + MIN semantics exactly across
            # the instances represented by this fingerprint in the time scope.
            for name in ("action", "normalized_sql", "sample_sql"):
                row[name] = min(str(item.get(name) or "") for item in matches)

    def _rows(
        self,
        sql: str,
        parameters: dict[str, str | int],
        control: Any | None,
    ) -> list[dict[str, Any]]:
        if control is not None:
            control.check_cancelled()
        with self._rollup_query_lock:
            return query_rows_with_cancel(
                self.client,
                sql,
                parameters,
                control,
                timeout=30,
                query_id_prefix="rds-insight-slowlog",
                settings={
                    "max_memory_usage": _SLOWLOG_QUERY_MAX_MEMORY_BYTES,
                    "max_bytes_before_external_group_by": (
                        _EXTERNAL_GROUP_BY_BYTES
                    ),
                },
            )

    def _manifest_coverage(
        self,
        parts: list[dict[str, Any]],
    ) -> dict[str, Any]:
        key_items: list[tuple[str, str]] = []
        for part in parts:
            identity = part_identity(part)
            if identity:
                key_items.append((str(part.get("path") or ""), identity))
        key = tuple(key_items)
        now = time.monotonic()
        with self._coverage_cache_lock:
            if (
                key == self._coverage_cache_key
                and self._coverage_cache_value is not None
                and now < self._coverage_cache_expires_monotonic
            ):
                return copy.deepcopy(self._coverage_cache_value)
        coverage = self.manifest.coverage(parts)
        with self._coverage_cache_lock:
            self._coverage_cache_key = key
            self._coverage_cache_value = copy.deepcopy(coverage)
            self._coverage_cache_expires_monotonic = (
                time.monotonic() + _COVERAGE_CACHE_SECONDS
            )
        return coverage

    def summarize(
        self,
        query: dict[str, Any],
        *,
        retention_days: int,
        control: Any | None = None,
        parts: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        if not self.serving_enabled:
            return None
        if str(query.get("source") or "").strip().lower() != "slowlog":
            return None
        window = self._window(query, retention_days)
        if window is None:
            return None
        start_us, end_us = window
        if parts is None:
            parts = self.metadata.parts_in_range(
                start_epoch_us=start_us,
                end_epoch_us=end_us,
                source="slowlog",
                instance=str(query.get("instance") or ""),
            )
        coverage = self._manifest_coverage(parts)
        if not bool(coverage.get("complete")):
            return None
        if control is not None:
            control.check_cancelled()
            control.set_plan(
                total_parts=len(parts),
                candidate_parts=len(parts),
                indexed_parts=len(parts),
                unknown_parts=0,
                estimated_bytes=0,
            )
        scope, parameters = self._scope_sql(query, start_us, end_us)
        width = slowlog_trend_width(start_us, end_us)
        parameters["trend_width"] = width
        rollup_sql = f"""
            /* slowlog:rollup */
            SELECT grouping(
                       metric_fingerprint,metric_database_name,
                       metric_table_name,metric_operation,metric_bucket
                   ) AS grouping_mask,
                   metric_fingerprint AS group_fingerprint,
                   metric_database_name AS group_database_name,
                   metric_table_name AS group_table_name,
                   metric_operation AS group_operation,
                   metric_bucket AS group_ts,
                   count() AS executions,
                   sum(metric_rows_examined) AS scan_rows,
                   max(metric_rows_examined) AS scan_rows_max,
                   sum(metric_rows_sent) AS rows_sent,
                   max(metric_rows_sent) AS rows_sent_max,
                   sum(metric_query_time_ms) AS query_time_ms_total,
                   max(metric_query_time_ms) AS query_time_ms_max,
                   sum(metric_lock_time_ms) AS lock_time_ms_total,
                   max(metric_lock_time_ms) AS lock_time_ms_max,
                   argMax(
                       scope_event_id,
                       tuple(
                           metric_rows_examined,metric_query_time_ms,
                           metric_event_epoch_us,scope_event_id
                       )
                   ) AS max_scan_event_id,
                   argMax(
                       scope_event_id,
                       tuple(
                           metric_query_time_ms,metric_rows_examined,
                           metric_event_epoch_us,scope_event_id
                       )
                   ) AS max_query_event_id,
                   sum(metric_sql_bytes) AS sql_bytes,
                   min(metric_event_epoch_us) AS first_epoch_us,
                   max(metric_event_epoch_us) AS last_epoch_us,
                   uniqExact(tuple(
                       metric_database_name,metric_table_name
                   )) AS objects,
                   uniqExact(metric_fingerprint) AS fingerprints,
                   min(scope_instance_id) AS instance_id,
                   groupUniqArray(scope_instance_id) AS instance_ids,
                   min(metric_database_name) AS database_name,
                   min(metric_table_name) AS table_name,
                   min(metric_operation) AS operation,
                   max(metric_sql_id) AS sql_id,
                   min(scope_event_id) AS sample_event_id,
                   min(metric_action) AS action
            FROM (
                SELECT *,
                       intDiv(
                           metric_event_epoch_us,{{trend_width:Int64}}
                       ) * {{trend_width:Int64}} AS metric_bucket
                FROM ({scope})
            )
            GROUP BY GROUPING SETS (
                (metric_fingerprint),
                (metric_database_name,metric_table_name),
                (metric_operation),
                (metric_bucket)
            )
        """
        rollups = self._rows(rollup_sql, parameters, control)
        groups: list[dict[str, Any]] = []
        objects: list[dict[str, Any]] = []
        operations: list[dict[str, Any]] = []
        trend: list[dict[str, Any]] = []
        instances: dict[str, tuple[str, ...]] = {}
        for row in rollups:
            mask = self._integer(row, "grouping_mask")
            if mask == _STATEMENT_GROUP_MASK:
                fingerprint = str(row.get("group_fingerprint") or "")
                instances[fingerprint] = tuple(
                    sorted(str(value) for value in row.get("instance_ids") or [])
                )
                groups.append(
                    {
                        "fingerprint": fingerprint,
                        "executions": self._integer(row, "executions"),
                        "scan_rows": self._integer(row, "scan_rows"),
                        "scan_rows_max": self._integer(row, "scan_rows_max"),
                        "rows_sent": self._integer(row, "rows_sent"),
                        "rows_sent_max": self._integer(row, "rows_sent_max"),
                        "query_time_ms_total": self._integer(
                            row, "query_time_ms_total"
                        ),
                        "query_time_ms_max": self._integer(
                            row, "query_time_ms_max"
                        ),
                        "lock_time_ms_total": self._integer(
                            row, "lock_time_ms_total"
                        ),
                        "lock_time_ms_max": self._integer(
                            row, "lock_time_ms_max"
                        ),
                        "sql_bytes": self._integer(row, "sql_bytes"),
                        "first_epoch_us": self._integer(row, "first_epoch_us"),
                        "last_epoch_us": self._integer(row, "last_epoch_us"),
                        "objects": self._integer(row, "objects"),
                        "instance_id": str(row.get("instance_id") or ""),
                        "database_name": str(row.get("database_name") or ""),
                        "table_name": str(row.get("table_name") or ""),
                        "operation": str(row.get("operation") or ""),
                        "sql_id": str(row.get("sql_id") or ""),
                        "sample_event_id": str(row.get("sample_event_id") or ""),
                        "max_scan_event_id": str(
                            row.get("max_scan_event_id") or ""
                        ),
                        "max_query_event_id": str(
                            row.get("max_query_event_id") or ""
                        ),
                        "action": str(row.get("action") or ""),
                        "normalized_sql": "",
                        "sample_sql": "",
                    }
                )
            elif mask == _OBJECT_GROUP_MASK:
                objects.append(
                    {
                        "database_name": str(
                            row.get("group_database_name") or ""
                        ),
                        "table_name": str(row.get("group_table_name") or ""),
                        "events": self._integer(row, "executions"),
                        "payload_bytes": self._integer(row, "sql_bytes"),
                        "fingerprints": self._integer(row, "fingerprints"),
                        "scan_rows": self._integer(row, "scan_rows"),
                        "rows_sent": self._integer(row, "rows_sent"),
                        "query_time_ms_total": self._integer(
                            row, "query_time_ms_total"
                        ),
                    }
                )
            elif mask == _OPERATION_GROUP_MASK:
                operations.append(
                    {
                        "operation": str(row.get("group_operation") or ""),
                        "events": self._integer(row, "executions"),
                        "payload_bytes": self._integer(row, "sql_bytes"),
                        "scan_rows": self._integer(row, "scan_rows"),
                    }
                )
            elif mask == _TREND_GROUP_MASK:
                trend.append(
                    {
                        "ts": self._integer(row, "group_ts"),
                        "events": self._integer(row, "executions"),
                        "query_time_ms_total": self._integer(
                            row, "query_time_ms_total"
                        ),
                        "scan_rows": self._integer(row, "scan_rows"),
                        "rows_sent": self._integer(row, "rows_sent"),
                    }
                )
        limit = min(max(int(query.get("limit") or 50), 1), 500)
        self._hydrate_statement_profiles(groups, instances, limit)
        statements = [slowlog_statement_result(row) for row in groups]
        orders = {
            name: sorted(
                statements,
                key=lambda row, keys=keys: slowlog_order_key(row, keys),
                reverse=True,
            )[:limit]
            for name, keys in SLOWLOG_ORDER_KEYS.items()
        }
        objects = sorted(
            objects,
            key=lambda row: (
                int(row.get("scan_rows") or 0),
                int(row.get("events") or 0),
            ),
            reverse=True,
        )[:limit]
        operations = sorted(
            operations,
            key=lambda row: int(row.get("events") or 0),
            reverse=True,
        )
        trend = sorted(trend, key=lambda row: int(row.get("ts") or 0))
        order = str(query.get("order") or "executions")
        if order not in SQL_ORDERS:
            order = "executions"
        executions = sum(int(row.get("executions") or 0) for row in groups)
        scan_rows = sum(int(row.get("scan_rows") or 0) for row in groups)
        rows_sent = sum(int(row.get("rows_sent") or 0) for row in groups)
        sql_bytes = sum(int(row.get("sql_bytes") or 0) for row in groups)
        sample_ids = {
            str(row.get(key) or "")
            for row in orders[order]
            for key in ("max_scan_event_id", "max_query_event_id")
            if str(row.get(key) or "")
        }
        sample_events = self.statement_index.event_details(
            sample_ids,
            instance=str(query.get("instance") or ""),
        )
        missing = list(coverage.get("missing_parts") or [])
        public_coverage = {
            **coverage,
            "missing_parts": missing[:20],
            "missing_parts_total": len(missing),
            "missing_parts_truncated": len(missing) > 20,
        }
        return {
            "window": {
                "start_epoch_us": start_us,
                "end_epoch_us": end_us,
                "bucket_us": BUCKET_US,
                "trend_bucket_us": width,
                "rollup_width_us": 0,
            },
            "sql": {
                "mode": "slowlog",
                "scan_source": "actual",
                "order": order,
                "totals": {
                    "events": executions,
                    "executions": executions,
                    "row_events": rows_sent,
                    "rows_sent": rows_sent,
                    "payload_bytes": sql_bytes,
                    "slow_events": executions,
                    "fingerprints": len(groups),
                    "objects": len(objects),
                    "boundary_events": 0,
                    "scan_rows": scan_rows,
                    "actual_scan_rows": scan_rows,
                    "est_scan_rows": scan_rows,
                    "scan_covered_executions": executions,
                    "est_covered_executions": executions,
                    "query_time_ms_total": sum(
                        int(row.get("query_time_ms_total") or 0)
                        for row in groups
                    ),
                    "query_time_ms_max": max(
                        (
                            int(row.get("query_time_ms_max") or 0)
                            for row in groups
                        ),
                        default=0,
                    ),
                    "lock_time_ms_total": sum(
                        int(row.get("lock_time_ms_total") or 0)
                        for row in groups
                    ),
                    "lock_time_ms_max": max(
                        (
                            int(row.get("lock_time_ms_max") or 0)
                            for row in groups
                        ),
                        default=0,
                    ),
                },
                "statements": orders[order],
                "orders": orders,
                "sample_events": sample_events,
                "objects": objects,
                "operations": operations,
                "trend": trend,
            },
            "transactions": slowlog_empty_transactions(),
            "locks": {
                "long_transactions": [],
                "large_transactions": [],
                "row_hotspots": [],
                "table_hotspots": [],
                "ddl_windows": [],
                "risk": {},
            },
            "clickhouse_slowlog_coverage": public_coverage,
        }

    def stats(self) -> dict[str, Any]:
        return {
            **self.manifest.stats(),
            "enabled": self.config.enabled,
            "serving_enabled": self.serving_enabled,
            "table": self.table,
            "retention_days": self.config.retention_days,
        }


class ClickHouseSlowLogWorker:
    def __init__(
        self,
        data_dir: Path,
        config: ClickHouseSlowLogConfig | None = None,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.config = config or ClickHouseSlowLogConfig.from_env()
        self.stopping = threading.Event()

    def request_stop(self, *_args: object) -> None:
        self.stopping.set()

    def run(self) -> int:
        from .clickhouse_ingest import HealthCanary
        from .config import ensure_data_dirs
        from .io_pressure import IoPressureGate, IoPressurePaused
        from .maintenance_status import (
            CLICKHOUSE_SLOWLOG_WORKER_STATUS_NAME,
            SLOWLOG_WORKER_STATUS_NAME,
            write_json_status,
        )
        from .metadata import MetadataStore

        if not self.config.enabled:
            LOGGER.info("ClickHouse slow-log ingestion is disabled")
            return 0
        paths = ensure_data_dirs(self.data_dir)
        status_path = (
            paths["logs"] / CLICKHOUSE_SLOWLOG_WORKER_STATUS_NAME
        )
        metadata = MetadataStore(
            self.data_dir / "metadata.sqlite3",
            run_migrations=False,
        )
        index = SlowLogIndex(
            paths["index"] / "slowlog.sqlite3",
            run_migrations=False,
        )
        manifest = ClickHouseManifest(
            paths["index"]
            / "clickhouse"
            / "slowlog-manifest.sqlite3",
            run_migrations=False,
        )
        manifest.recover_loading()
        client = ClickHouseSlowLogClient(self.config)
        io_gate = IoPressureGate.from_env(
            "RDS_BINLOG_CLICKHOUSE_IO_FULL_AVG10_MAX",
            default=10.0,
            recovery_ratio=self.config.base.io_pressure_recovery_ratio,
        )
        canary = HealthCanary(self.config.base)
        source_gate = SourceIndexPriorityGate(
            paths["index"] / SLOWLOG_WORKER_STATUS_NAME,
            max_pending_parts=getattr(
                self.config, "source_max_pending_parts", 128
            ),
            max_pending_age_seconds=getattr(
                self.config, "source_max_pending_age_seconds", 600
            ),
            recovery_ratio=getattr(
                self.config, "source_recovery_ratio", 0.8
            ),
            max_stale_seconds=getattr(
                self.config, "source_status_stale_seconds", 30
            ),
            # ClickHouse has independent PSI and /api/storage admission gates.
            # A clean source queue paused only by its own PSI fuse must not
            # deadlock the lower-priority lane forever.
            block_paused_state=False,
        )
        last_reconcile = 0.0
        last_error = ""
        io_pressure_override_active = False

        def publish(
            state: str,
            *,
            phase: str,
            result: dict[str, Any] | None = None,
            error: str = "",
        ) -> None:
            nonlocal last_error
            if error:
                last_error = str(error)
            write_json_status(
                status_path,
                {
                    "running": not self.stopping.is_set(),
                    "state": state,
                    "phase": phase,
                    "pid": os.getpid(),
                    "lastError": last_error,
                    "result": result or {},
                    "stats": manifest.stats(),
                },
            )

        publish("starting", phase="startup")
        try:
            while not self.stopping.is_set():
                try:
                    source_gate.check()
                    io_pressure_canary_override = False
                    try:
                        io_gate.check()
                    except IoPressurePaused as pressure_exc:
                        # System PSI includes waits created by unrelated
                        # containers' cgroup throttles.  Once the higher-
                        # priority source queue is healthy, let the real
                        # serving canary decide whether one rate-limited
                        # ClickHouse batch is safe.  A failed canary still
                        # raises IoPressurePaused and stops this iteration.
                        canary.probe(force=True)
                        io_pressure_canary_override = True
                        if not io_pressure_override_active:
                            LOGGER.warning(
                                "Host I/O PSI is above the ClickHouse "
                                "slow-log ceiling but the serving canary is "
                                "healthy; allowing one bounded batch: %s",
                                pressure_exc,
                            )
                        io_pressure_override_active = True
                    else:
                        if io_pressure_override_active:
                            LOGGER.info(
                                "Host I/O PSI recovered below the ClickHouse "
                                "slow-log ceiling"
                            )
                        io_pressure_override_active = False
                        canary.probe()
                    free_bytes = shutil.disk_usage(self.data_dir).free
                    required = self.config.base.min_free_gb * 1024**3
                    if free_bytes < required:
                        raise IoPressurePaused(
                            "ClickHouse slow-log ingestion paused: "
                            f"free disk {free_bytes / 1024**3:.1f} GiB is "
                            f"below {self.config.base.min_free_gb} GiB"
                        )
                    now = time.monotonic()
                    reconcile_result: dict[str, Any] = {}
                    if (
                        last_reconcile == 0.0
                        or now - last_reconcile
                        >= self.config.reconcile_seconds
                    ):
                        publish("running", phase="reconcile")
                        reconcile_result = reconcile_slowlog_manifest(
                            metadata,
                            manifest,
                            retention_days=self.config.retention_days,
                        )
                        last_reconcile = time.monotonic()
                    publish("running", phase="ingest")
                    batch = ingest_slowlog_batch(
                        metadata,
                        index,
                        manifest,
                        client,
                        batch_parts=self.config.batch_parts,
                        scratch=paths["scratch"],
                    )
                    if batch["claimed_parts"]:
                        # Observe the actual effect of this batch instead of
                        # trusting only the preflight measurement.
                        canary.probe(force=True)
                    last_error = ""
                    publish(
                        "completed" if batch["claimed_parts"] else "idle",
                        phase="ingest",
                        result={
                            **batch,
                            "reconcile": reconcile_result,
                            "ioPressureCanaryOverride": (
                                io_pressure_canary_override
                            ),
                        },
                    )
                    # Match the ten-second PSI feedback window: even a
                    # successful load is paced before the next admission
                    # check, so fast small parts cannot create a write burst.
                    delay = self.config.idle_seconds
                except SourceIndexLagPaused as exc:
                    publish(
                        "paused",
                        phase="source-priority",
                        error=str(exc),
                    )
                    delay = max(self.config.idle_seconds, 5.0)
                except IoPressurePaused as exc:
                    publish(
                        "paused",
                        phase="safety-fuse",
                        error=str(exc),
                    )
                    delay = max(self.config.idle_seconds, 5.0)
                except Exception as exc:
                    LOGGER.exception("ClickHouse slow-log worker iteration failed")
                    publish(
                        "error",
                        phase="worker",
                        error=f"{type(exc).__name__}: {exc}",
                    )
                    delay = min(max(self.config.idle_seconds, 1.0), 5.0)
                self.stopping.wait(delay)
        finally:
            publish("stopped", phase="shutdown")
        return 0


def main() -> int:
    from .config import data_root

    parser = argparse.ArgumentParser(
        description="Ingest the exact slow-log serving index into ClickHouse."
    )
    parser.add_argument("--data-dir", type=Path, default=data_root())
    args = parser.parse_args()
    worker = ClickHouseSlowLogWorker(args.data_dir.resolve())
    signal.signal(signal.SIGTERM, worker.request_stop)
    signal.signal(signal.SIGINT, worker.request_stop)
    return worker.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    raise SystemExit(main())
