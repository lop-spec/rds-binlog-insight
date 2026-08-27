from __future__ import annotations

import http.client
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import urlencode

from .io_pressure import io_recovery_ratio_from_env


MAX_PART_STATE_IDENTITIES = 256


def validate_part_state_batch_size(value: int) -> int:
    size = int(value)
    if size < 1:
        raise ValueError("ClickHouse part state batch must contain at least 1 identity")
    if size > MAX_PART_STATE_IDENTITIES:
        raise ValueError(
            "ClickHouse part state batch supports at most "
            f"{MAX_PART_STATE_IDENTITIES} identities"
        )
    return size


SOURCE_COLUMN_TYPES: tuple[tuple[str, str], ...] = (
    ("event_id", "String"),
    ("event_epoch_us", "Int64"),
    # input() receives this schema inside a SQL string literal. The target
    # column carries UTC; an unzoned DateTime64 input avoids nested quoting and
    # ClickHouse converts the Parquet timestamp into the UTC target column.
    ("event_time_utc", "DateTime64(6)"),
    ("event_date", "Date"),
    ("instance_id", "String"),
    ("host_instance_id", "String"),
    ("source_file_id", "String"),
    ("source_file_name", "String"),
    ("raw_event_type", "String"),
    ("operation", "String"),
    ("database_name", "String"),
    ("table_name", "String"),
    ("table_map_id", "UInt64"),
    ("schema_version_id", "String"),
    ("server_id", "Int64"),
    ("thread_id", "Int64"),
    ("transaction_id", "String"),
    ("gtid", "String"),
    ("xid", "String"),
    ("start_position", "Int64"),
    ("end_position", "Int64"),
    ("row_index", "Int32"),
    ("execution_time_ms", "Int64"),
    ("error_code", "Int32"),
    ("sql_kind", "String"),
    ("sql_text", "String"),
    ("sql_bytes_base64", "String"),
    ("before_json", "String"),
    ("after_json", "String"),
    ("columns_json", "String"),
    ("row_query", "String"),
    ("header_epoch_us", "Int64"),
    ("commit_epoch_us", "Int64"),
    ("txn_last_committed", "Int64"),
    ("txn_sequence_number", "Int64"),
    ("txn_length_bytes", "Int64"),
    ("connection_id", "String"),
    ("connection_name", "String"),
    ("database_account", "String"),
    ("execution_status", "String"),
    ("error_message", "String"),
    ("affected_rows", "Int64"),
    ("started_epoch_us", "Int64"),
    ("finished_epoch_us", "Int64"),
    ("batch_id", "String"),
    ("statement_index", "Int32"),
    ("transaction_context_id", "String"),
)
SOURCE_COLUMNS = tuple(name for name, _type in SOURCE_COLUMN_TYPES)
QUERY_SOURCE_COLUMNS = (
    "event_id",
    "event_epoch_us",
    "event_time_utc",
    "event_date",
    "instance_id",
    "host_instance_id",
    "source_file_name",
    "raw_event_type",
    "operation",
    "database_name",
    "table_name",
    "server_id",
    "thread_id",
    "transaction_id",
    "gtid",
    "start_position",
    "end_position",
    "row_index",
    "execution_time_ms",
    "error_code",
    "sql_kind",
    "sql_text",
    "before_json",
    "after_json",
    "row_query",
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
METADATA_COLUMNS = (
    "_source_part_key",
    "_source_part_sha256",
    "_content_revision",
    "_ingested_at",
)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


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


def _float_env(name: str, default: float, minimum: float, maximum: float) -> float:
    try:
        value = float(os.environ.get(name, str(default)) or default)
    except ValueError:
        value = default
    return min(max(value, minimum), maximum)


def _identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe ClickHouse identifier: {value!r}")
    return value


def _qualified_identifier(value: str) -> str:
    parts = str(value).split(".")
    if len(parts) != 2:
        raise ValueError(f"Unsafe ClickHouse qualified identifier: {value!r}")
    return ".".join(_identifier(part) for part in parts)


def _sql_string(value: str) -> str:
    value = str(value)
    if any(ord(character) < 32 for character in value):
        raise ValueError("ClickHouse SQL string contains a control character")
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


@dataclass(frozen=True, slots=True)
class ClickHouseConfig:
    enabled: bool
    serving_enabled: bool
    host: str
    port: int
    database: str
    table: str
    user: str
    password: str
    hot_hours: int
    serving_hours: int
    reconcile_seconds: int
    idle_seconds: float
    health_url: str
    health_host_header: str
    health_max_seconds: float
    min_free_gb: int
    io_pressure_max_full_avg10: float
    io_pressure_recovery_ratio: float = 0.5
    query_table: str = "events_query"
    name_query_table: str = "events_query_by_name"
    ingest_mode: str = "wide"

    @classmethod
    def from_env(cls) -> "ClickHouseConfig":
        hot_hours = _int_env(
            "RDS_BINLOG_CLICKHOUSE_HOT_HOURS", 27, 2, 168
        )
        serving_hours = _int_env(
            "RDS_BINLOG_CLICKHOUSE_SERVING_HOURS", 25, 1, hot_hours
        )
        database = _identifier(
            os.environ.get("RDS_BINLOG_CLICKHOUSE_DATABASE", "insight").strip()
            or "insight"
        )
        table = _identifier(
            os.environ.get("RDS_BINLOG_CLICKHOUSE_TABLE", "events").strip()
            or "events"
        )
        query_table = _identifier(
            os.environ.get(
                "RDS_BINLOG_CLICKHOUSE_QUERY_TABLE", "events_query"
            ).strip()
            or "events_query"
        )
        name_query_table = _identifier(
            os.environ.get(
                "RDS_BINLOG_CLICKHOUSE_NAME_QUERY_TABLE",
                "events_query_by_name",
            ).strip()
            or "events_query_by_name"
        )
        ingest_mode = os.environ.get(
            "RDS_BINLOG_CLICKHOUSE_INGEST_MODE", "wide"
        ).strip().lower()
        if ingest_mode not in {"wide", "query"}:
            raise ValueError(
                "RDS_BINLOG_CLICKHOUSE_INGEST_MODE must be wide or query"
            )
        return cls(
            enabled=_bool_env("RDS_BINLOG_CLICKHOUSE_ENABLED"),
            serving_enabled=_bool_env(
                "RDS_BINLOG_CLICKHOUSE_SERVING_ENABLED"
            ),
            host=os.environ.get(
                "RDS_BINLOG_CLICKHOUSE_HOST", "rds-binlog-insight-clickhouse"
            ).strip(),
            port=_int_env("RDS_BINLOG_CLICKHOUSE_PORT", 8123, 1, 65535),
            database=database,
            table=table,
            user=os.environ.get("CLICKHOUSE_USER", "").strip(),
            password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
            hot_hours=hot_hours,
            serving_hours=serving_hours,
            reconcile_seconds=_int_env(
                "RDS_BINLOG_CLICKHOUSE_RECONCILE_SECONDS", 30, 5, 3600
            ),
            idle_seconds=_float_env(
                "RDS_BINLOG_CLICKHOUSE_IDLE_SECONDS", 2.0, 0.1, 60.0
            ),
            health_url=os.environ.get(
                "RDS_BINLOG_CLICKHOUSE_HEALTH_URL", ""
            ).strip(),
            health_host_header=os.environ.get(
                "RDS_BINLOG_CLICKHOUSE_HEALTH_HOST", ""
            ).strip(),
            health_max_seconds=_float_env(
                "RDS_BINLOG_CLICKHOUSE_HEALTH_MAX_SECONDS", 1.0, 0.05, 30.0
            ),
            min_free_gb=_int_env(
                "RDS_BINLOG_CLICKHOUSE_MIN_FREE_GB", 120, 20, 2000
            ),
            io_pressure_max_full_avg10=_float_env(
                "RDS_BINLOG_CLICKHOUSE_IO_FULL_AVG10_MAX",
                20.0,
                0.0,
                100.0,
            ),
            io_pressure_recovery_ratio=io_recovery_ratio_from_env(
                "RDS_BINLOG_CLICKHOUSE_IO_RECOVERY_RATIO",
                0.5,
            ),
            query_table=query_table,
            name_query_table=name_query_table,
            ingest_mode=ingest_mode,
        )

    @property
    def qualified_table(self) -> str:
        return f"{self.database}.{self.table}"

    @property
    def qualified_query_table(self) -> str:
        return f"{self.database}.{self.query_table}"

    @property
    def qualified_name_query_table(self) -> str:
        return f"{self.database}.{self.name_query_table}"

    def validate_credentials(self) -> None:
        if not self.host:
            raise RuntimeError("RDS_BINLOG_CLICKHOUSE_HOST is required")
        if not self.user or not self.password:
            raise RuntimeError(
                "CLICKHOUSE_USER and CLICKHOUSE_PASSWORD are required"
            )


class ClickHouseError(RuntimeError):
    pass


class ClickHouseClient:
    def __init__(self, config: ClickHouseConfig):
        config.validate_credentials()
        self.config = config

    def _headers(self) -> dict[str, str]:
        return {
            "X-ClickHouse-User": self.config.user,
            "X-ClickHouse-Key": self.config.password,
        }

    def query(
        self,
        sql: str,
        *,
        parameters: dict[str, str | int] | None = None,
        settings: dict[str, str | int] | None = None,
        timeout: int = 60,
    ) -> str:
        values: dict[str, str | int] = {
            "query": sql,
            "use_query_cache": 0,
            "max_threads": 1,
            # Interactive reads stay bounded even while the single ingester is
            # parsing a wide Parquet row group on the same small server.
            "max_memory_usage": 500_000_000,
            "max_execution_time": 60,
        }
        values.update(settings or {})
        values.update(
            {f"param_{key}": value for key, value in (parameters or {}).items()}
        )
        connection = http.client.HTTPConnection(
            self.config.host,
            self.config.port,
            timeout=timeout,
        )
        try:
            connection.request(
                "POST",
                "/?" + urlencode(values),
                body=b"",
                headers=self._headers(),
            )
            response = connection.getresponse()
            body = response.read().decode("utf-8", errors="replace")
            if response.status != 200:
                raise ClickHouseError(
                    f"ClickHouse HTTP {response.status}: {body[:1000]}"
                )
            return body
        except (OSError, http.client.HTTPException) as exc:
            raise ClickHouseError(f"ClickHouse request failed: {exc}") from exc
        finally:
            connection.close()

    def json_rows(
        self,
        sql: str,
        *,
        parameters: dict[str, str | int] | None = None,
        settings: dict[str, str | int] | None = None,
        timeout: int = 60,
    ) -> list[dict[str, Any]]:
        body = self.query(
            sql.rstrip().removesuffix("FORMAT JSONEachRow")
            + " FORMAT JSONEachRow",
            parameters=parameters,
            settings=settings,
            timeout=timeout,
        )
        return [json.loads(line) for line in body.splitlines() if line]

    def insert_json_rows(
        self,
        table: str,
        rows: list[dict[str, Any]],
        *,
        timeout: int = 60,
    ) -> int:
        """Insert a bounded manifest batch without putting row data in the URL."""

        if not rows:
            return 0
        table_parts = str(table).split(".")
        if len(table_parts) != 2 or any(
            not _IDENTIFIER.fullmatch(part) for part in table_parts
        ):
            raise ValueError(f"Unsafe ClickHouse table: {table!r}")
        columns = tuple(rows[0])
        if not columns or any(
            not _IDENTIFIER.fullmatch(column) for column in columns
        ):
            raise ValueError("Unsafe or empty ClickHouse JSON column list")
        if any(tuple(row) != columns for row in rows):
            raise ValueError("ClickHouse JSON rows must use one stable column order")
        payload = (
            "\n".join(
                json.dumps(
                    row,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
                for row in rows
            )
            + "\n"
        ).encode("utf-8")
        query = (
            f"INSERT INTO {'.'.join(table_parts)} "
            f"({', '.join(columns)}) FORMAT JSONEachRow"
        )
        target = "/?" + urlencode(
            {
                "query": query,
                "async_insert": 0,
                "wait_for_async_insert": 1,
            }
        )
        headers = self._headers()
        headers.update(
            {
                "Content-Type": "application/x-ndjson",
                "Content-Length": str(len(payload)),
            }
        )
        connection = http.client.HTTPConnection(
            self.config.host,
            self.config.port,
            timeout=timeout,
        )
        try:
            connection.request(
                "POST",
                target,
                body=payload,
                headers=headers,
            )
            response = connection.getresponse()
            body = response.read().decode("utf-8", errors="replace")
            if response.status != 200:
                raise ClickHouseError(
                    f"ClickHouse HTTP {response.status}: {body[:1000]}"
                )
            return len(rows)
        except (OSError, http.client.HTTPException) as exc:
            raise ClickHouseError(f"ClickHouse request failed: {exc}") from exc
        finally:
            connection.close()

    def ping(self) -> str:
        return self.query("SELECT version() FORMAT TabSeparated", timeout=10).strip()

    def stop_merges(self) -> None:
        self.query(
            f"SYSTEM STOP MERGES {self.config.qualified_table}",
            timeout=30,
        )

    def start_merges(self) -> None:
        self.query(
            f"SYSTEM START MERGES {self.config.qualified_table}",
            timeout=30,
        )

    def part_state(self, part_key: str) -> dict[str, Any]:
        rows = self.json_rows(
            f"""
            SELECT count() AS rows,
                   uniqExact(_source_part_sha256) AS sha_count,
                   any(_source_part_sha256) AS sha256,
                   min(_content_revision) AS min_revision,
                   max(_content_revision) AS max_revision
            FROM {self.config.qualified_table}
            WHERE _source_part_key = {{part_key:String}}
            """,
            parameters={"part_key": part_key},
        )
        return rows[0] if rows else {
            "rows": 0,
            "sha_count": 0,
            "sha256": "",
            "min_revision": 0,
            "max_revision": 0,
        }

    def _part_states_for_table(
        self,
        table: str,
        part_keys: list[str],
    ) -> dict[str, dict[str, Any]]:
        validate_part_state_batch_size(len(part_keys))
        parameters = {
            f"part_{index}": key for index, key in enumerate(part_keys)
        }
        placeholders = ", ".join(
            "{" + name + ":String}" for name in parameters
        )
        rows = self.json_rows(
            f"""
            SELECT _source_part_key AS part_key,
                   count() AS rows,
                   uniqExact(_source_part_sha256) AS sha_count,
                   any(_source_part_sha256) AS sha256,
                   min(_content_revision) AS min_revision,
                   max(_content_revision) AS max_revision
            FROM {table}
            WHERE _source_part_key IN ({placeholders})
            GROUP BY _source_part_key
            """,
            parameters=parameters,
            timeout=120,
        )
        return {str(row["part_key"]): row for row in rows}

    def all_part_states_for_table(
        self, table: str
    ) -> dict[str, dict[str, Any]]:
        """Return every exact source-part state from a bounded staging table."""

        table = _qualified_identifier(table)
        rows = self.json_rows(
            f"""
            SELECT _source_part_key AS part_key,
                   count() AS rows,
                   uniqExact(_source_part_sha256) AS sha_count,
                   any(_source_part_sha256) AS sha256,
                   min(_content_revision) AS min_revision,
                   max(_content_revision) AS max_revision
            FROM {table}
            GROUP BY _source_part_key
            """,
            settings={"max_threads": 1, "max_execution_time": 120},
            timeout=120,
        )
        return {str(row["part_key"]): row for row in rows}

    def part_states(self, part_keys: list[str]) -> dict[str, dict[str, Any]]:
        unique = list(dict.fromkeys(str(key) for key in part_keys if str(key)))
        if not unique:
            return {}
        return self._part_states_for_table(self.config.qualified_table, unique)

    def paired_part_states(
        self, part_keys: list[str]
    ) -> dict[str, dict[str, Any]]:
        """Return exact source-part state for both physical query sort tables."""
        return self.paired_part_states_for_tables(
            part_keys,
            time_table=self.config.qualified_table,
            name_table=self.config.qualified_name_query_table,
        )

    def paired_part_states_for_tables(
        self,
        part_keys: list[str],
        *,
        time_table: str,
        name_table: str,
    ) -> dict[str, dict[str, Any]]:
        """Return exact source-part state for an explicit physical table pair."""
        unique = list(dict.fromkeys(str(key) for key in part_keys if str(key)))
        if not unique:
            return {}
        validate_part_state_batch_size(len(unique))
        time_table = _qualified_identifier(time_table)
        name_table = _qualified_identifier(name_table)
        time_states = self._part_states_for_table(
            time_table, unique
        )
        if name_table == time_table:
            name_states = time_states
        else:
            name_states = self._part_states_for_table(
                name_table, unique
            )
        empty = {
            "rows": 0,
            "sha_count": 0,
            "sha256": "",
            "min_revision": 0,
            "max_revision": 0,
        }
        paired: dict[str, dict[str, Any]] = {}
        for key in unique:
            state = dict(time_states.get(key, empty))
            name_state = name_states.get(key, empty)
            state.update(
                {
                    "name_rows": int(name_state.get("rows") or 0),
                    "name_sha_count": int(
                        name_state.get("sha_count") or 0
                    ),
                    "name_sha256": str(name_state.get("sha256") or ""),
                    "name_min_revision": int(
                        name_state.get("min_revision") or 0
                    ),
                    "name_max_revision": int(
                        name_state.get("max_revision") or 0
                    ),
                }
            )
            paired[key] = state
        return paired

    def table_part_summary(self, table: str) -> dict[str, int]:
        table = _qualified_identifier(table)
        rows = self.json_rows(
            f"SELECT count() AS rows, "
            f"uniqExact(_source_part_key) AS part_count FROM {table}",
            settings={"max_threads": 1, "max_execution_time": 1800},
            timeout=1800,
        )
        row = rows[0] if rows else {}
        return {
            "rows": int(row.get("rows") or 0),
            "part_count": int(row.get("part_count") or 0),
        }

    def table_storage_summary(self, table: str) -> dict[str, int]:
        """Return an O(active-parts) physical summary without scanning rows."""

        table = _qualified_identifier(table)
        database, name = table.split(".", 1)
        rows = self.json_rows(
            """
            SELECT coalesce(sum(rows), 0) AS rows,
                   count() AS active_parts,
                   uniqExact(partition) AS partitions
            FROM system.parts
            WHERE database = {database:String}
              AND table = {table:String}
              AND active
            """,
            parameters={"database": database, "table": name},
            timeout=120,
        )
        row = rows[0] if rows else {}
        return {
            "rows": int(row.get("rows") or 0),
            "active_parts": int(row.get("active_parts") or 0),
            "partitions": int(row.get("partitions") or 0),
        }

    def table_status(self, table: str) -> dict[str, Any]:
        table = _qualified_identifier(table)
        database, name = table.split(".", 1)
        rows = self.json_rows(
            """
            SELECT count() AS objects, any(engine) AS engine
            FROM system.tables
            WHERE database = {database:String}
              AND name = {table:String}
            """,
            parameters={"database": database, "table": name},
            timeout=30,
        )
        row = rows[0] if rows else {}
        return {
            "exists": int(row.get("objects") or 0) == 1,
            "engine": str(row.get("engine") or ""),
        }

    def truncate_table(self, table: str) -> None:
        table = _qualified_identifier(table)
        self.query(
            f"TRUNCATE TABLE {table}",
            settings={"max_execution_time": 1800},
            timeout=1800,
        )

    def copy_table(self, *, source: str, destination: str) -> None:
        source = _qualified_identifier(source)
        destination = _qualified_identifier(destination)
        columns = ", ".join((*QUERY_SOURCE_COLUMNS, *METADATA_COLUMNS))
        self.query(
            f"INSERT INTO {destination} ({columns}) "
            f"SELECT {columns} FROM {source}",
            settings={
                "max_threads": 1,
                "max_insert_threads": 1,
                "max_memory_usage": 3_000_000_000,
                "max_execution_time": 1800,
                "max_block_size": 1024,
                "max_insert_block_size": 1024,
                "min_insert_block_size_rows": 1024,
                "min_insert_block_size_bytes": 8 * 1024 * 1024,
                "wait_end_of_query": 1,
            },
            timeout=1800,
        )

    def active_partitions(self, table: str) -> list[str]:
        table = _qualified_identifier(table)
        database, name = table.split(".", 1)
        rows = self.json_rows(
            """
            SELECT DISTINCT partition
            FROM system.parts
            WHERE database = {database:String}
              AND table = {table:String}
              AND active
            ORDER BY partition
            """,
            parameters={"database": database, "table": name},
            timeout=120,
        )
        return [str(row["partition"]) for row in rows]

    def move_partitions(
        self,
        *,
        source: str,
        destination: str,
        partitions: list[str],
    ) -> None:
        source = _qualified_identifier(source)
        destination = _qualified_identifier(destination)
        for partition in dict.fromkeys(str(item) for item in partitions):
            self.query(
                f"ALTER TABLE {source} MOVE PARTITION "
                f"{_sql_string(partition)} TO TABLE {destination}",
                settings={"max_execution_time": 1800},
                timeout=1800,
            )

    def _active_delete_part_keys(
        self,
        table: str,
        part_keys: list[str],
    ) -> set[str]:
        database, name = _qualified_identifier(table).split(".", 1)
        rows = self.json_rows(
            """
            SELECT command
            FROM system.mutations
            WHERE database = {database:String}
              AND table = {table:String}
              AND is_done = 0
            """,
            parameters={"database": database, "table": name},
            settings={"max_execution_time": 30},
            timeout=30,
        )
        commands = [str(row.get("command") or "") for row in rows]
        return {
            key
            for key in part_keys
            if any(key in command for command in commands)
        }

    def delete_parts(self, part_keys: list[str]) -> None:
        unique = list(dict.fromkeys(str(key) for key in part_keys if str(key)))
        if not unique:
            return
        validate_part_state_batch_size(len(unique))
        tables: list[str] = []
        if self.config.qualified_name_query_table not in {
            self.config.qualified_query_table,
            self.config.qualified_table,
        }:
            tables.append(self.config.qualified_name_query_table)
        if self.config.qualified_query_table != self.config.qualified_table:
            tables.append(self.config.qualified_query_table)
        tables.append(self.config.qualified_table)
        for table in tables:
            active = self._active_delete_part_keys(table, unique)
            pending = [key for key in unique if key not in active]
            if not pending:
                continue
            parameters = {
                f"part_{index}": key for index, key in enumerate(pending)
            }
            placeholders = ", ".join(
                "{" + name + ":String}" for name in parameters
            )
            self.query(
                f"ALTER TABLE {table} DELETE WHERE "
                f"_source_part_key IN ({placeholders})",
                parameters=parameters,
                settings={
                    # Do not occupy a foreground query slot while a potentially
                    # long MergeTree mutation is applied.  Callers verify that
                    # the rows disappeared and retry without marking the source
                    # ready when the asynchronous mutation is still running.
                    "mutations_sync": 0,
                    "max_execution_time": 1800,
                },
                timeout=1800,
            )

    def delete_part(self, part_key: str) -> None:
        self.delete_parts([part_key])

    def insert_parquet(
        self,
        path: Path,
        *,
        part_key: str,
        sha256: str,
        content_revision: int,
    ) -> None:
        content_length = path.stat().st_size
        with path.open("rb") as source:
            self.insert_parquet_stream(
                source,
                content_length=content_length,
                part_key=part_key,
                sha256=sha256,
                content_revision=content_revision,
            )

    def insert_parquet_stream(
        self,
        source: BinaryIO,
        *,
        content_length: int,
        part_key: str,
        sha256: str,
        content_revision: int,
    ) -> None:
        """Insert an already verified Parquet stream without staging it on disk."""
        content_length = max(int(content_length), 0)
        selected_columns = (
            QUERY_SOURCE_COLUMNS
            if self.config.ingest_mode == "query"
            else SOURCE_COLUMNS
        )
        source_names = ", ".join(selected_columns)
        destination_names = ", ".join(
            (*selected_columns, *METADATA_COLUMNS)
        )
        input_schema = ", ".join(
            f"{name} {column_type}"
            for name, column_type in SOURCE_COLUMN_TYPES
        )
        sql = (
            f"INSERT INTO {self.config.qualified_table} ({destination_names}) "
            f"SELECT {source_names}, {{part_key:String}}, {{sha256:String}}, "
            "{content_revision:UInt64}, now64(3) "
            f"FROM input('{input_schema}') FORMAT Parquet"
        )
        values: dict[str, str | int] = {
            "query": sql,
            "param_part_key": part_key,
            "param_sha256": sha256,
            "param_content_revision": max(int(content_revision), 0),
            "max_threads": 1,
            "max_insert_threads": 1,
            # The server default is 60 seconds, but production imports are
            # deliberately I/O-throttled and one 28-65 MB part can take about
            # 96 seconds. Keep a separate bounded background window instead
            # of inheriting the interactive-query timeout.
            "max_execution_time": 300,
            # A few highly-compressed row_query values need about 2.45 GiB
            # while ParquetV3 materializes a single value. The server-wide
            # 4.2 GB cap and two 500 MB interactive caps still bound coexistence.
            "max_memory_usage": 3_000_000_000,
            "input_format_parallel_parsing": 0,
            # ClickHouse 26.3 ParquetV3 can over-allocate 1 GiB while decoding
            # small PLAIN_DICTIONARY string chunks. The stable reader imported
            # both production repro parts with exact row/hash/revision parity.
            "input_format_parquet_use_native_reader_v3": 0,
            "input_format_parquet_max_block_size": 1024,
            "input_format_parquet_prefer_block_bytes": 8 * 1024 * 1024,
            "input_format_max_block_size_bytes": 32 * 1024 * 1024,
            "input_format_parquet_enable_row_group_prefetch": 0,
            "input_format_parquet_allow_missing_columns": 1,
            "input_format_defaults_for_omitted_fields": 1,
            "input_format_null_as_default": 1,
            "max_block_size": 1024,
            "max_insert_block_size": 1024,
            "min_insert_block_size_rows": 1024,
            "min_insert_block_size_bytes": 8 * 1024 * 1024,
            "wait_end_of_query": 1,
        }
        connection = http.client.HTTPConnection(
            self.config.host,
            self.config.port,
            timeout=1800,
        )
        try:
            connection.putrequest("POST", "/?" + urlencode(values))
            for name, value in self._headers().items():
                connection.putheader(name, value)
            connection.putheader("Content-Type", "application/octet-stream")
            connection.putheader("Content-Length", str(content_length))
            connection.endheaders()
            remaining = content_length
            while remaining:
                chunk = source.read(min(4 * 1024 * 1024, remaining))
                if not chunk:
                    raise ClickHouseError(
                        "ClickHouse insert source ended before Content-Length"
                    )
                connection.send(chunk)
                remaining -= len(chunk)
            response = connection.getresponse()
            body = response.read().decode("utf-8", errors="replace")
            if response.status != 200:
                raise ClickHouseError(
                    f"ClickHouse insert HTTP {response.status}: {body[:1000]}"
                )
        except (OSError, http.client.HTTPException) as exc:
            raise ClickHouseError(f"ClickHouse insert failed: {exc}") from exc
        finally:
            connection.close()
