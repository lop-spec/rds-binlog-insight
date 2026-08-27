from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .clickhouse_client import ClickHouseClient, ClickHouseConfig, ClickHouseError
from .clickhouse_manifest import ClickHouseManifest, ClickHouseManifestError
from .clickhouse_oss import ClickHouseOssConfig
from .clickhouse_raw_oss import (
    ClickHouseRawOssConfig,
    build_exact_raw_oss_source_sql,
    build_raw_oss_candidate_sql,
)
from .metadata import MetadataStore


LOGGER = logging.getLogger(__name__)
TABULARIS_AUDIT_EVENT_TYPE = "TABULARIS_AUDIT"
SLOW_LOG_EVENT_TYPE = "SLOW_LOG"
NAME_PAIR_LIMIT = 64
RAW_CANDIDATE_PAGE_SIZE = 64
# Wide binlog Parquet columns can approach the 500 MB interactive query cap
# before ClickHouse has produced the first block. Keep each exact S3 source
# small; candidate pagination still amortizes the manifest lookup.
RAW_OBJECT_BATCH_SIZE = 4


class ClickHouseRawOssUnavailable(RuntimeError):
    """The requested raw-OSS window cannot be served completely yet."""


RESULT_COLUMNS = (
    "event_id",
    "event_epoch_us",
    "instance_id",
    "formatDateTime(event_time_utc, '%Y-%m-%dT%H:%i:%S.%fZ', 'UTC') "
    "AS event_time_utc",
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
    "concat(_source_part_key, ':*') AS locator",
)


def _window(query: dict[str, Any], retention_days: int) -> tuple[int, int]:
    now = datetime.now(UTC)
    now_us = int(now.timestamp() * 1_000_000)
    cutoff_us = int((now - timedelta(days=retention_days)).timestamp() * 1_000_000)
    return (
        max(int(query.get("start_epoch_us") or cutoff_us), cutoff_us),
        min(int(query.get("end_epoch_us") or now_us), now_us),
    )


class _Sql:
    def __init__(self):
        self.clauses: list[str] = []
        self.parameters: dict[str, str | int] = {}
        self._counter = 0

    def parameter(self, value: str | int, column_type: str) -> str:
        self._counter += 1
        name = f"p{self._counter}"
        self.parameters[name] = value
        return "{" + name + ":" + column_type + "}"

    def add(self, clause: str) -> None:
        self.clauses.append(clause)


def query_rows_with_cancel(
    client: ClickHouseClient,
    sql: str,
    parameters: dict[str, str | int],
    control: Any | None,
    *,
    timeout: int = 60,
    query_id_prefix: str = "rds-insight",
    settings: dict[str, str | int] | None = None,
) -> list[dict[str, Any]]:
    if control is None:
        return client.json_rows(
            sql,
            parameters=parameters,
            settings=settings,
            timeout=timeout,
        )
    control.check_cancelled()
    query_id = f"{query_id_prefix}-{uuid.uuid4().hex}"
    stopped = threading.Event()

    def cancel_watch() -> None:
        while not stopped.wait(0.1):
            try:
                control.check_cancelled()
            except BaseException:
                try:
                    client.query(
                        "KILL QUERY WHERE query_id = {query_id:String} SYNC",
                        parameters={"query_id": query_id},
                        timeout=10,
                    )
                except Exception:
                    LOGGER.exception("Failed to cancel ClickHouse query")
                return

    watcher = threading.Thread(
        target=cancel_watch,
        name="clickhouse-query-cancel",
        daemon=True,
    )
    watcher.start()
    try:
        rows = client.json_rows(
            sql,
            parameters=parameters,
            settings={**(settings or {}), "query_id": query_id},
            timeout=timeout,
        )
        control.check_cancelled()
        return rows
    finally:
        stopped.set()
        watcher.join(timeout=1)


class ClickHouseQueryBackend:
    def __init__(
        self,
        metadata: MetadataStore,
        data_dir: Path,
        config: ClickHouseConfig | None = None,
        *,
        client: ClickHouseClient | None = None,
        manifest: ClickHouseManifest | None = None,
        oss_config: ClickHouseOssConfig | None = None,
        raw_config: ClickHouseRawOssConfig | None = None,
        raw_pack_manifest: ClickHouseManifest | None = None,
    ):
        self.metadata = metadata
        self.data_dir = Path(data_dir)
        resolved_oss = oss_config or ClickHouseOssConfig.from_env()
        self.object_serving = bool(
            resolved_oss.enabled and resolved_oss.serving_enabled
        )
        resolved_raw = raw_config or ClickHouseRawOssConfig.from_env()
        self.raw_serving = bool(
            resolved_raw.enabled and resolved_raw.serving_enabled
        )
        if self.object_serving and self.raw_serving:
            raise ValueError(
                "Native all-history and raw OSS serving cannot both be active"
            )
        resolved_config = config or ClickHouseConfig.from_env()
        if self.object_serving:
            resolved_config = replace(
                resolved_config,
                serving_enabled=True,
                table=resolved_oss.query_table,
                query_table=resolved_oss.query_table,
                name_query_table=resolved_oss.name_query_table,
                ingest_mode="query",
            )
        self.config = resolved_config
        self.oss_config = resolved_oss
        self.raw_config = resolved_raw
        self.client = client or ClickHouseClient(self.config)
        self.raw_pack_manifest: ClickHouseManifest | None = None
        if self.raw_serving:
            self.raw_pack_manifest = raw_pack_manifest or ClickHouseManifest(
                self.data_dir
                / "index"
                / "clickhouse"
                / "raw-oss-packed-manifest.sqlite3",
                run_migrations=False,
            )
            self.manifest = manifest or self.raw_pack_manifest
        else:
            self.manifest = manifest or ClickHouseManifest(
                self.data_dir
                / "index"
                / "clickhouse"
                / (
                    self.oss_config.manifest_name
                    if self.object_serving
                    else "manifest.sqlite3"
                ),
                run_migrations=False,
            )

    @classmethod
    def from_env(
        cls,
        metadata: MetadataStore,
        data_dir: Path,
    ) -> "ClickHouseQueryBackend | None":
        config = ClickHouseConfig.from_env()
        oss_config = ClickHouseOssConfig.from_env()
        raw_config = ClickHouseRawOssConfig.from_env()
        if not config.enabled or not (
            config.serving_enabled
            or (oss_config.enabled and oss_config.serving_enabled)
            or (raw_config.enabled and raw_config.serving_enabled)
        ):
            return None
        try:
            return cls(
                metadata,
                data_dir,
                config,
                oss_config=oss_config,
                raw_config=raw_config,
            )
        except (ClickHouseManifestError, ClickHouseError, RuntimeError, ValueError):
            LOGGER.exception("ClickHouse serving backend is unavailable")
            return None

    def _eligible(
        self,
        query: dict[str, Any],
        retention_days: int,
    ) -> tuple[int, int] | None:
        source = str(query.get("source") or "").strip().lower()
        if source not in {"binlog", "database"}:
            return None
        if query.get("exact") or str(query.get("fingerprint") or "").strip():
            return None
        start_us, end_us = _window(query, retention_days)
        if end_us < start_us:
            return None
        if not self.object_serving and not self.raw_serving:
            safe_start_us = int(
                (
                    datetime.now(UTC)
                    - timedelta(hours=self.config.serving_hours)
                ).timestamp()
                * 1_000_000
            )
            if start_us < safe_start_us:
                return None
        return start_us, end_us

    def _where(
        self,
        query: dict[str, Any],
        start_us: int,
        end_us: int,
        *,
        exact_names: list[tuple[str, str, str]] | None = None,
    ) -> _Sql:
        sql = _Sql()
        start_parameter = sql.parameter(start_us, "Int64")
        end_parameter = sql.parameter(end_us, "Int64")
        sql.add(
            "event_epoch_us >= " + start_parameter
            + " AND event_epoch_us <= " + end_parameter
        )
        sql.add(
            "event_date >= toDate(fromUnixTimestamp64Micro("
            + start_parameter
            + "), 'UTC') AND event_date <= toDate(fromUnixTimestamp64Micro("
            + end_parameter
            + "), 'UTC')"
        )
        source = str(query.get("source") or "").strip().lower()
        if source == "database":
            sql.add(
                "raw_event_type != "
                + sql.parameter(TABULARIS_AUDIT_EVENT_TYPE, "String")
            )
        elif source == "binlog":
            sql.add(
                "raw_event_type NOT IN ("
                + sql.parameter(TABULARIS_AUDIT_EVENT_TYPE, "String")
                + ", "
                + sql.parameter(SLOW_LOG_EVENT_TYPE, "String")
                + ")"
            )
        transaction = str(query.get("transaction") or "").strip()
        if transaction:
            first = sql.parameter(transaction, "String")
            second = sql.parameter(transaction, "String")
            sql.add(f"(transaction_id = {first} OR gtid = {second})")
        for key, columns in (
            ("instance", ("instance_id",)),
            ("connection", ("connection_id", "connection_name")),
            ("account", ("database_account",)),
            ("database", ("database_name",)),
            ("table", ("table_name",)),
            ("status", ("execution_status",)),
        ):
            if exact_names is not None and key in {
                "instance",
                "database",
                "table",
            }:
                continue
            value = str(query.get(key) or "").strip()
            if not value:
                continue
            predicates: list[str] = []
            for column in columns:
                parameter = sql.parameter(value, "String")
                if key in {"instance", "status"}:
                    predicates.append(f"lowerUTF8({column}) = lowerUTF8({parameter})")
                else:
                    predicates.append(
                        f"positionCaseInsensitiveUTF8({column}, {parameter}) > 0"
                    )
            sql.add("(" + " OR ".join(predicates) + ")")
        if exact_names is not None:
            tuples = []
            for instance_id, database_name, table_name in exact_names:
                tuples.append(
                    "("
                    + sql.parameter(instance_id, "String")
                    + ", "
                    + sql.parameter(database_name, "String")
                    + ", "
                    + sql.parameter(table_name, "String")
                    + ")"
                )
            sql.add(
                "(instance_id, database_name, table_name) IN ("
                + ", ".join(tuples)
                + ")"
            )
        operations = [
            str(value).strip().upper()
            for value in (query.get("operations") or [])
            if str(value).strip()
        ]
        if operations:
            sql.add(
                "operation IN ("
                + ", ".join(
                    sql.parameter(operation, "String") for operation in operations
                )
                + ")"
            )
        keyword = str(query.get("keyword") or "").strip()
        if keyword:
            term_groups: list[str] = []
            for term in [part for part in keyword.split() if part][:20]:
                predicates = []
                for column in (
                    "sql_text",
                    "before_json",
                    "after_json",
                    "transaction_id",
                    "source_file_name",
                    "connection_name",
                    "database_account",
                    "error_message",
                ):
                    parameter = sql.parameter(term, "String")
                    predicates.append(
                        f"positionCaseInsensitiveUTF8({column}, {parameter}) > 0"
                    )
                term_groups.append("(" + " OR ".join(predicates) + ")")
            joiner = (
                " OR "
                if str(query.get("keyword_mode") or "AND").upper() == "OR"
                else " AND "
            )
            if term_groups:
                sql.add("(" + joiner.join(term_groups) + ")")
        return sql

    def _resolve_exact_names(
        self,
        query: dict[str, Any],
        start_us: int,
        end_us: int,
        control: Any | None,
    ) -> list[tuple[str, str, str]] | None:
        """Resolve fuzzy API names on the hourly aggregate projection.

        A resolved tuple lets the detail query seek the name-ordered projection.
        More than NAME_PAIR_LIMIT matches deliberately falls back to the
        time-ordered path so a broad fuzzy term cannot create a huge IN list.
        """

        instance = str(query.get("instance") or "").strip()
        database = str(query.get("database") or "").strip()
        table = str(query.get("table") or "").strip()
        if not instance or not database or not table:
            return None
        parameters = {
            "name_start": start_us,
            "name_end": end_us,
            "name_instance": instance,
            "name_database": database,
            "name_table": table,
            "name_limit": NAME_PAIR_LIMIT + 1,
        }
        sql = f"""
            SELECT instance_id, database_name, table_name,
                   count() AS physical_rows
            FROM {self.config.qualified_query_table}
            WHERE toStartOfHour(event_time_utc) >=
                      toStartOfHour(fromUnixTimestamp64Micro(
                          {{name_start:Int64}}
                      ), 'UTC')
              AND toStartOfHour(event_time_utc) <=
                      toStartOfHour(fromUnixTimestamp64Micro(
                          {{name_end:Int64}}
                      ), 'UTC')
              AND lowerUTF8(instance_id) = lowerUTF8({{name_instance:String}})
              AND positionCaseInsensitiveUTF8(
                      database_name, {{name_database:String}}
                  ) > 0
              AND positionCaseInsensitiveUTF8(
                      table_name, {{name_table:String}}
                  ) > 0
            GROUP BY instance_id, database_name, table_name
            ORDER BY physical_rows DESC, instance_id, database_name, table_name
            LIMIT {{name_limit:UInt64}}
        """
        rows = self._query_with_cancel(sql, parameters, control)
        if len(rows) > NAME_PAIR_LIMIT:
            return None
        return [
            (
                str(row.get("instance_id") or ""),
                str(row.get("database_name") or ""),
                str(row.get("table_name") or ""),
            )
            for row in rows
        ]

    def _query_with_cancel(
        self,
        sql: str,
        parameters: dict[str, str | int],
        control: Any | None,
        *,
        settings: dict[str, str | int] | None = None,
    ) -> list[dict[str, Any]]:
        return query_rows_with_cancel(
            self.client,
            sql,
            parameters,
            control,
            settings=settings,
        )

    def _query_raw_events(
        self,
        query: dict[str, Any],
        *,
        start_us: int,
        end_us: int,
        limit_cap: int,
        control: Any | None,
    ) -> dict[str, Any] | None:
        if self.raw_pack_manifest is None:
            raise ClickHouseRawOssUnavailable(
                "Raw OSS packed manifest is unavailable"
            )
        source_state = self.metadata.clickhouse_change_tracking_state()
        source_pending = bool(source_state.get("pending"))
        source_pending_in_window = bool(
            source_pending
            and self.metadata.clickhouse_pending_changes_overlap(
                start_epoch_us=start_us,
                end_epoch_us=end_us,
            )
        )
        coverage = self.raw_pack_manifest.window_coverage(
            source_complete=bool(source_state.get("complete")),
            source_pending=source_pending,
            source_pending_in_window=source_pending_in_window,
            start_epoch_us=start_us,
            end_epoch_us=end_us,
        )
        if not bool(coverage.get("complete")):
            raise ClickHouseRawOssUnavailable(
                "Raw OSS coverage is incomplete for the requested window"
            )
        settings = self.metadata.load_settings()
        limit = min(max(int(query.get("limit") or 100), 1), limit_cap)
        offset = min(max(int(query.get("offset") or 0), 0), 100_000)
        target_unique = offset + limit + 1
        raw_batch_size = min(max(limit * 4, 256), 4096)
        unique_rows: list[dict[str, Any]] = []
        seen_event_ids: set[str] = set()
        candidate_parts = 0
        used_packed = False
        covered_parts = int(coverage.get("covered_parts") or 0)
        if control is not None:
            control.set_plan(
                total_parts=covered_parts,
                candidate_parts=covered_parts,
                indexed_parts=covered_parts,
                unknown_parts=0,
                estimated_bytes=0,
            )
        raw_settings = {
            "input_format_parquet_use_native_reader_v3": 0,
            # Production wide-string parts exceeded the 500 MB query cap with
            # the default 65k-row block and four parallel object readers.
            # These are the same bounded reader settings used by the verified
            # Parquet ingest path, but retain the interactive 500 MB cap.
            "input_format_parquet_max_block_size": 1024,
            "input_format_parquet_prefer_block_bytes": 8 * 1024 * 1024,
            "input_format_max_block_size_bytes": 32 * 1024 * 1024,
            "input_format_parquet_enable_row_group_prefetch": 0,
            "input_format_parquet_allow_missing_columns": 1,
            "input_format_defaults_for_omitted_fields": 1,
            "input_format_null_as_default": 1,
            "max_block_size": 1024,
            "max_threads": 1,
            "max_download_threads": 1,
            "max_parsing_threads": 1,
        }
        where = self._where(query, start_us, end_us)
        cursor_max: int | None = None
        cursor_path = ""
        stop = False
        while not stop:
            candidate_sql, candidate_parameters = build_raw_oss_candidate_sql(
                self.raw_config,
                database=self.config.database,
                query=query,
                start_epoch_us=start_us,
                end_epoch_us=end_us,
                limit=RAW_CANDIDATE_PAGE_SIZE,
                cursor_max_event_epoch_us=cursor_max,
                cursor_part_path=cursor_path,
            )
            candidates = self._query_with_cancel(
                candidate_sql,
                candidate_parameters,
                control,
                settings={"max_threads": 2},
            )
            if not candidates:
                break
            candidate_parts += len(candidates)
            for batch_start in range(0, len(candidates), RAW_OBJECT_BATCH_SIZE):
                candidate_batch = candidates[
                    batch_start : batch_start + RAW_OBJECT_BATCH_SIZE
                ]
                used_packed = used_packed or any(
                    int(candidate.get("oss_length") or 0) > 0
                    for candidate in candidate_batch
                )
                source_sql, source_parameters = build_exact_raw_oss_source_sql(
                    settings,
                    self.raw_config,
                    database=self.config.database,
                    candidates=candidate_batch,
                )
                sql = f"""
                    SELECT {', '.join(RESULT_COLUMNS)}
                    FROM ({source_sql}) AS raw_oss_source
                    WHERE {' AND '.join(where.clauses)}
                    ORDER BY event_epoch_us DESC, source_file_name DESC,
                             end_position DESC, row_index DESC, event_id DESC,
                             _content_revision DESC, _source_part_key DESC
                    LIMIT {{raw_limit:UInt64}} OFFSET {{raw_offset:UInt64}}
                """
                raw_offset = 0
                while raw_offset < target_unique:
                    parameters = {
                        **source_parameters,
                        **where.parameters,
                        "raw_limit": min(
                            raw_batch_size, target_unique - raw_offset
                        ),
                        "raw_offset": raw_offset,
                    }
                    rows = self._query_with_cancel(
                        sql,
                        parameters,
                        control,
                        settings=raw_settings,
                    )
                    for row in rows:
                        event_id = str(row.get("event_id") or "")
                        if event_id in seen_event_ids:
                            continue
                        seen_event_ids.add(event_id)
                        unique_rows.append(row)
                    raw_offset += len(rows)
                    if len(rows) < int(parameters["raw_limit"]):
                        break

                unique_rows.sort(
                    key=lambda row: (
                        int(row.get("event_epoch_us") or 0),
                        str(row.get("source_file_name") or ""),
                        int(row.get("end_position") or 0),
                        int(row.get("row_index") or 0),
                        str(row.get("event_id") or ""),
                        str(row.get("locator") or ""),
                    ),
                    reverse=True,
                )
                if len(unique_rows) > target_unique:
                    removed = unique_rows[target_unique:]
                    del unique_rows[target_unique:]
                    for row in removed:
                        seen_event_ids.discard(str(row.get("event_id") or ""))

                next_index = batch_start + len(candidate_batch)
                remaining_max = int(
                    (
                        candidates[next_index]
                        if next_index < len(candidates)
                        else candidates[-1]
                    ).get("max_event_epoch_us")
                    or 0
                )
                if (
                    len(unique_rows) >= target_unique
                    and remaining_max
                    < int(unique_rows[target_unique - 1].get("event_epoch_us") or 0)
                ):
                    stop = True
                    break
            if stop or len(candidates) < RAW_CANDIDATE_PAGE_SIZE:
                break
            cursor_max = int(candidates[-1].get("max_event_epoch_us") or 0)
            cursor_path = str(candidates[-1].get("part_path") or "")
        page = unique_rows[offset : offset + limit]
        tiers = ["clickhouse-raw-oss"]
        if used_packed:
            tiers.append("clickhouse-packed")
        return {
            "rows": page,
            "has_more": len(unique_rows) > offset + limit,
            "limit": limit,
            "offset": offset,
            "coverage_found": True,
            "tiers_used": tiers,
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
            "indexed_parts": candidate_parts,
            "structural_indexed_parts": candidate_parts,
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
            "query_certificate_part_count": 0,
            "oss_downloaded_parts": 0,
            "unavailable_parts": 0,
            "range_start_epoch_us": start_us,
            "range_end_epoch_us": end_us,
            "clickhouse_coverage": {
                **coverage,
                "source_complete": bool(source_state.get("complete")),
                "source_pending": bool(source_state.get("pending")),
                "missing_parts": [],
                "missing_parts_total": 0,
                "missing_parts_truncated": False,
            },
        }

    def query_events(
        self,
        query: dict[str, Any],
        *,
        retention_days: int,
        limit_cap: int,
        control: Any | None = None,
    ) -> dict[str, Any] | None:
        eligible = self._eligible(query, retention_days)
        if eligible is None:
            return None
        start_us, end_us = eligible
        if self.raw_serving:
            return self._query_raw_events(
                query,
                start_us=start_us,
                end_us=end_us,
                limit_cap=limit_cap,
                control=control,
            )
        source = str(query.get("source") or "").strip().lower()
        if self.object_serving:
            source_state = self.metadata.clickhouse_change_tracking_state()
            coverage = self.manifest.global_coverage(
                source_complete=bool(source_state.get("complete")),
                source_pending=bool(source_state.get("pending")),
            )
            parts: list[dict[str, Any]] = []
        else:
            parts = self.metadata.parts_in_range(
                start_epoch_us=start_us,
                end_epoch_us=end_us,
                source=source,
                instance=str(query.get("instance") or ""),
            )
            coverage = self.manifest.coverage(parts)
        if not bool(coverage["complete"]):
            return None
        covered_parts = int(coverage.get("covered_parts") or len(parts))
        limit = min(max(int(query.get("limit") or 100), 1), limit_cap)
        offset = min(max(int(query.get("offset") or 0), 0), 100_000)
        if control is not None:
            control.set_plan(
                total_parts=covered_parts,
                candidate_parts=covered_parts,
                indexed_parts=covered_parts,
                unknown_parts=0,
                estimated_bytes=0,
            )
        target_unique = offset + limit + 1
        raw_batch_size = min(max(limit * 4, 256), 4096)
        unique_rows: list[dict[str, Any]] = []
        seen_event_ids: set[str] = set()
        exact_names = self._resolve_exact_names(
            query, start_us, end_us, control
        )
        if exact_names != []:
            where = self._where(
                query,
                start_us,
                end_us,
                exact_names=exact_names,
            )
            detail_table = (
                self.config.qualified_name_query_table
                if exact_names is not None
                else self.config.qualified_query_table
            )
            sql = f"""
                SELECT {', '.join(RESULT_COLUMNS)}
                FROM {detail_table}
                WHERE {' AND '.join(where.clauses)}
                ORDER BY event_epoch_us DESC, source_file_name DESC,
                         end_position DESC, row_index DESC, event_id DESC,
                         _content_revision DESC, _source_part_key DESC
                LIMIT {{raw_limit:UInt64}} OFFSET {{raw_offset:UInt64}}
            """
            raw_offset = 0
            while len(unique_rows) < target_unique:
                parameters = dict(where.parameters)
                parameters["raw_limit"] = raw_batch_size
                parameters["raw_offset"] = raw_offset
                batch = self._query_with_cancel(sql, parameters, control)
                for row in batch:
                    event_id = str(row.get("event_id") or "")
                    if event_id in seen_event_ids:
                        continue
                    seen_event_ids.add(event_id)
                    unique_rows.append(row)
                    if len(unique_rows) >= target_unique:
                        break
                raw_offset += len(batch)
                if len(batch) < raw_batch_size:
                    break
        page = unique_rows[offset : offset + limit]
        missing = list(coverage.get("missing_parts") or [])
        return {
            "rows": page,
            "has_more": len(unique_rows) > offset + limit,
            "limit": limit,
            "offset": offset,
            "coverage_found": bool(covered_parts),
            "tiers_used": [
                "clickhouse-oss" if self.object_serving else "clickhouse-hot"
            ],
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
            "indexed_parts": covered_parts,
            "structural_indexed_parts": covered_parts,
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
            "query_certificate_part_count": 0,
            "oss_downloaded_parts": 0,
            "unavailable_parts": 0,
            "range_start_epoch_us": start_us,
            "range_end_epoch_us": end_us,
            "clickhouse_coverage": {
                **coverage,
                "missing_parts": missing[:20],
                "missing_parts_total": len(missing),
                "missing_parts_truncated": len(missing) > 20,
            },
        }

    def stats(self, *, include_server: bool = False) -> dict[str, Any]:
        result = self.manifest.stats()
        result.update(
            {
                "enabled": self.config.enabled,
                "serving_enabled": self.config.serving_enabled,
                "hot_hours": self.config.hot_hours,
                "serving_hours": self.config.serving_hours,
                "object_storage": self.object_serving,
                "raw_object_storage": self.raw_serving,
                "history_days": (
                    self.oss_config.history_days
                    if self.object_serving
                    else 0
                ),
            }
        )
        if not include_server:
            return result
        try:
            rows = self.client.json_rows(
                f"""
                SELECT sum(rows) AS rows, sum(bytes_on_disk) AS bytes_on_disk
                FROM system.parts
                WHERE active AND database = {{database:String}}
                  AND table = {{table:String}}
                """,
                parameters={
                    "database": self.config.database,
                    "table": self.config.table,
                },
                timeout=10,
            )
            result["server_rows"] = int(rows[0]["rows"] or 0) if rows else 0
            result["server_bytes"] = (
                int(rows[0]["bytes_on_disk"] or 0) if rows else 0
            )
            result["reachable"] = True
        except Exception as exc:
            result["reachable"] = False
            result["server_error"] = str(exc)
        return result
