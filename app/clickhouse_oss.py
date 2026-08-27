from __future__ import annotations

import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from urllib.parse import quote, urlsplit

from .config import Settings
from .clickhouse_client import (
    METADATA_COLUMNS,
    QUERY_SOURCE_COLUMNS,
)


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_BUCKET = re.compile(r"^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$")
_PREFIX = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$|^$")


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


def _identifier(value: str) -> str:
    value = str(value).strip()
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe ClickHouse identifier: {value!r}")
    return value


def _prefix(value: str) -> str:
    value = str(value).strip().lstrip("/")
    if value and not value.endswith("/"):
        value += "/"
    if (
        not value
        or not _PREFIX.fullmatch(value)
        or ".." in value.split("/")
        or "//" in value
    ):
        raise ValueError("ClickHouse OSS prefix is invalid")
    return value


@dataclass(frozen=True, slots=True)
class ClickHouseOssConfig:
    enabled: bool
    serving_enabled: bool
    prefix: str
    cache_gb: int
    query_table: str
    name_query_table: str
    materialized_view: str
    manifest_name: str
    history_days: int
    backfill_threads: int = 2
    backfill_insert_threads: int = 1
    backfill_workers: int = 1
    staged_backfill_enabled: bool = False
    incremental_mv_enabled: bool = True
    stage_query_table: str = ""
    stage_name_query_table: str = ""

    @classmethod
    def from_env(cls) -> "ClickHouseOssConfig":
        query_table = _identifier(
            os.environ.get(
                "RDS_BINLOG_CLICKHOUSE_OSS_QUERY_TABLE",
                "events_query_oss_all",
            )
        )
        name_query_table = _identifier(
            os.environ.get(
                "RDS_BINLOG_CLICKHOUSE_OSS_NAME_QUERY_TABLE",
                "events_query_by_name_oss_all",
            )
        )
        return cls(
            enabled=_bool_env("RDS_BINLOG_CLICKHOUSE_OSS_ENABLED"),
            serving_enabled=_bool_env(
                "RDS_BINLOG_CLICKHOUSE_OSS_SERVING_ENABLED"
            ),
            prefix=_prefix(
                os.environ.get(
                    "RDS_BINLOG_CLICKHOUSE_OSS_PREFIX",
                    "sql-insight-clickhouse/v2/",
                )
            ),
            cache_gb=_int_env(
                "RDS_BINLOG_CLICKHOUSE_OSS_CACHE_GB", 40, 2, 512
            ),
            query_table=query_table,
            name_query_table=name_query_table,
            materialized_view=_identifier(
                os.environ.get(
                    "RDS_BINLOG_CLICKHOUSE_OSS_NAME_MV",
                    "events_query_oss_all_to_name_mv",
                )
            ),
            manifest_name=os.environ.get(
                "RDS_BINLOG_CLICKHOUSE_OSS_MANIFEST",
                "oss-all-manifest.sqlite3",
            ).strip(),
            history_days=_int_env(
                "RDS_BINLOG_CLICKHOUSE_OSS_HISTORY_DAYS", 0, 0, 3650
            ),
            backfill_threads=_int_env(
                "RDS_BINLOG_CLICKHOUSE_OSS_BACKFILL_THREADS", 2, 1, 8
            ),
            backfill_insert_threads=_int_env(
                "RDS_BINLOG_CLICKHOUSE_OSS_BACKFILL_INSERT_THREADS", 1, 1, 4
            ),
            backfill_workers=_int_env(
                "RDS_BINLOG_CLICKHOUSE_OSS_BACKFILL_WORKERS", 1, 1, 2
            ),
            staged_backfill_enabled=_bool_env(
                "RDS_BINLOG_CLICKHOUSE_OSS_STAGED_BACKFILL_ENABLED"
            ),
            incremental_mv_enabled=_bool_env(
                "RDS_BINLOG_CLICKHOUSE_OSS_INCREMENTAL_MV_ENABLED", True
            ),
            stage_query_table=_identifier(
                os.environ.get(
                    "RDS_BINLOG_CLICKHOUSE_OSS_STAGE_QUERY_TABLE",
                    query_table + "_stage",
                )
            ),
            stage_name_query_table=_identifier(
                os.environ.get(
                    "RDS_BINLOG_CLICKHOUSE_OSS_STAGE_NAME_QUERY_TABLE",
                    name_query_table + "_stage",
                )
            ),
        )

    def __post_init__(self) -> None:
        _prefix(self.prefix)
        _identifier(self.query_table)
        _identifier(self.name_query_table)
        _identifier(self.materialized_view)
        if not re.fullmatch(r"[A-Za-z0-9._-]+\.sqlite3", self.manifest_name):
            raise ValueError("ClickHouse OSS manifest name is invalid")
        if self.query_table == self.name_query_table:
            raise ValueError("ClickHouse OSS query tables must be distinct")
        if not 1 <= int(self.backfill_threads) <= 8:
            raise ValueError("ClickHouse OSS backfill threads must be 1..8")
        if not 1 <= int(self.backfill_insert_threads) <= 4:
            raise ValueError("ClickHouse OSS insert threads must be 1..4")
        if not 1 <= int(self.backfill_workers) <= 2:
            raise ValueError("ClickHouse OSS backfill workers must be 1..2")
        if self.staged_backfill_enabled:
            _identifier(self.stage_query_table)
            _identifier(self.stage_name_query_table)
            tables = {
                self.query_table,
                self.name_query_table,
                self.stage_query_table,
                self.stage_name_query_table,
            }
            if len(tables) != 4:
                raise ValueError(
                    "ClickHouse OSS staged backfill tables must be distinct"
                )
            if int(self.backfill_workers) != 1:
                raise ValueError(
                    "ClickHouse OSS staged backfill requires one worker"
                )


def history_start_epoch_us(now: datetime, history_days: int) -> int:
    """Return zero for all-history scans; positive values retain bounded mode."""
    if int(history_days) <= 0:
        return 0
    return int((now - timedelta(days=int(history_days))).timestamp() * 1_000_000)


def _oss_base_url(settings: Settings) -> str:
    if not settings.oss_enabled:
        raise ValueError("OSS must be enabled for ClickHouse object storage")
    bucket = settings.oss_bucket.strip().lower()
    if not _BUCKET.fullmatch(bucket):
        raise ValueError("OSS bucket is invalid")
    raw_endpoint = settings.oss_endpoint.strip()
    parsed = urlsplit(
        raw_endpoint
        if "://" in raw_endpoint
        else "https://" + raw_endpoint
    )
    hostname = str(parsed.hostname or "").lower()
    expected = (
        f"oss-{settings.oss_region_id.strip().lower()}-internal.aliyuncs.com"
    )
    if (
        parsed.scheme != "https"
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
        or hostname != expected
    ):
        raise ValueError("OSS endpoint must be the matching private HTTPS endpoint")
    return f"https://{bucket}.{hostname}/"


def _dynamic_disk(
    endpoint: str,
    *,
    cache_path: str,
    cache_gb: int,
) -> str:
    return (
        "disk("
        "type=cache,"
        f"path='{cache_path}',"
        f"max_size='{max(int(cache_gb), 1)}Gi',"
        "disk=disk("
        "type=object_storage,"
        "object_storage_type=s3,"
        f"endpoint='{endpoint}',"
        "metadata_type=local,"
        "use_environment_credentials=true"
        ")"
        ")"
    )


SOURCE_PART_STATE_PROJECTION_SQL = """source_part_state_v1
    (
        SELECT _source_part_key,
               count() AS rows,
               uniqExact(_source_part_sha256) AS sha_count,
               any(_source_part_sha256) AS sha256,
               min(_content_revision) AS min_revision,
               max(_content_revision) AS max_revision
        GROUP BY _source_part_key
    )""".strip()


QUERY_COLUMNS_SQL = f"""
    event_id String CODEC(ZSTD(1)),
    event_epoch_us Int64 CODEC(DoubleDelta, ZSTD(1)),
    event_time_utc DateTime64(6, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    event_date Date CODEC(Delta, ZSTD(1)),
    instance_id LowCardinality(String) CODEC(ZSTD(1)),
    host_instance_id LowCardinality(String) CODEC(ZSTD(1)),
    source_file_name String CODEC(ZSTD(1)),
    raw_event_type LowCardinality(String) CODEC(ZSTD(1)),
    operation LowCardinality(String) CODEC(ZSTD(1)),
    database_name LowCardinality(String) CODEC(ZSTD(1)),
    table_name LowCardinality(String) CODEC(ZSTD(1)),
    server_id Int64 CODEC(Delta, ZSTD(1)),
    thread_id Int64 CODEC(Delta, ZSTD(1)),
    transaction_id String CODEC(ZSTD(1)),
    gtid String CODEC(ZSTD(1)),
    start_position Int64 CODEC(Delta, ZSTD(1)),
    end_position Int64 CODEC(Delta, ZSTD(1)),
    row_index Int32 CODEC(Delta, ZSTD(1)),
    execution_time_ms Int64 CODEC(Delta, ZSTD(1)),
    error_code Int32 CODEC(Delta, ZSTD(1)),
    sql_kind LowCardinality(String) CODEC(ZSTD(1)),
    sql_text String CODEC(ZSTD(1)),
    before_json String CODEC(ZSTD(3)),
    after_json String CODEC(ZSTD(3)),
    row_query String CODEC(ZSTD(1)),
    connection_id String CODEC(ZSTD(1)),
    connection_name String CODEC(ZSTD(1)),
    database_account String CODEC(ZSTD(1)),
    execution_status LowCardinality(String) CODEC(ZSTD(1)),
    error_message String CODEC(ZSTD(1)),
    affected_rows Int64 CODEC(Delta, ZSTD(1)),
    started_epoch_us Int64 CODEC(DoubleDelta, ZSTD(1)),
    finished_epoch_us Int64 CODEC(DoubleDelta, ZSTD(1)),
    batch_id String CODEC(ZSTD(1)),
    statement_index Int32 CODEC(Delta, ZSTD(1)),
    transaction_context_id String CODEC(ZSTD(1)),
    _source_part_key String CODEC(ZSTD(1)),
    _source_part_sha256 String CODEC(ZSTD(1)),
    _content_revision UInt64 CODEC(Delta, ZSTD(1)),
    _ingested_at DateTime64(3, 'UTC') DEFAULT now64(3),
    INDEX event_id_bloom event_id TYPE bloom_filter(0.001) GRANULARITY 4,
    INDEX source_part_bloom _source_part_key TYPE bloom_filter(0.001) GRANULARITY 4,
    INDEX transaction_bloom transaction_id TYPE bloom_filter(0.001) GRANULARITY 4,
    INDEX gtid_bloom gtid TYPE bloom_filter(0.001) GRANULARITY 4,
    INDEX operation_set operation TYPE set(64) GRANULARITY 4,
    INDEX source_type_set raw_event_type TYPE set(64) GRANULARITY 4,
    INDEX database_name_ngram lowerUTF8(database_name)
        TYPE ngrambf_v1(3, 2048, 3, 0) GRANULARITY 1,
    INDEX table_name_ngram lowerUTF8(table_name)
        TYPE ngrambf_v1(3, 2048, 3, 0) GRANULARITY 1,
    PROJECTION {SOURCE_PART_STATE_PROJECTION_SQL}
""".strip()


def build_oss_schema(
    settings: Settings,
    config: ClickHouseOssConfig,
    *,
    database: str,
) -> str:
    database = _identifier(database)
    base = _oss_base_url(settings) + _prefix(config.prefix)
    per_table_cache_gb = max(int(config.cache_gb) // 2, 1)
    time_endpoint = base + "events-query/"
    name_endpoint = base + "events-query-by-name/"
    time_disk = _dynamic_disk(
        time_endpoint,
        cache_path="/var/lib/clickhouse/caches/sql-insight/time/",
        cache_gb=per_table_cache_gb,
    )
    name_disk = _dynamic_disk(
        name_endpoint,
        cache_path="/var/lib/clickhouse/caches/sql-insight/name/",
        cache_gb=per_table_cache_gb,
    )
    time_table = f"{database}.{config.query_table}"
    name_table = f"{database}.{config.name_query_table}"
    materialized_view = f"{database}.{config.materialized_view}"
    stage_schema = ""
    if config.staged_backfill_enabled:
        stage_time_table = f"{database}.{config.stage_query_table}"
        stage_name_table = f"{database}.{config.stage_name_query_table}"
        stage_schema = f"""

CREATE TABLE IF NOT EXISTS {stage_time_table} AS {time_table};

CREATE TABLE IF NOT EXISTS {stage_name_table} AS {name_table};
"""
    # ClickHouse 26.3 cannot ALTER a table whose `disk` setting is a dynamic
    # object-storage CustomType (BAD_GET). Keep the projection in CREATE TABLE;
    # staged AS-tables inherit it. Existing projection-less namespaces must be
    # replaced by a new namespace instead of receiving an unsafe hot migration.
    materialized_view_schema = ""
    if config.incremental_mv_enabled:
        materialized_view_schema = f"""

CREATE MATERIALIZED VIEW IF NOT EXISTS {materialized_view}
TO {name_table}
AS SELECT * FROM {time_table};
"""
    ttl_clause = (
        f"TTL event_time_utc + INTERVAL {int(config.history_days)} DAY DELETE\n"
        if config.history_days > 0
        else ""
    )
    common_settings = """
        allow_experimental_reverse_key = 1,
        index_granularity = 8192,
        max_bytes_to_merge_at_max_space_in_pool = 134217728,
        non_replicated_deduplication_window = 100000
    """.strip()
    return f"""
CREATE DATABASE IF NOT EXISTS {database};

CREATE TABLE IF NOT EXISTS {time_table}
(
{QUERY_COLUMNS_SQL},
    PROJECTION names_hourly_v1
    (
        SELECT toStartOfHour(event_time_utc) AS event_hour,
               instance_id, database_name, table_name, count()
        GROUP BY event_hour, instance_id, database_name, table_name
    )
)
ENGINE = MergeTree
PARTITION BY event_date
PRIMARY KEY event_epoch_us
ORDER BY
(
    event_epoch_us DESC,
    source_file_name DESC,
    end_position DESC,
    row_index DESC,
    event_id DESC,
    _content_revision DESC,
    _source_part_key DESC
)
{ttl_clause}SETTINGS
    disk = {time_disk},
    {common_settings};

CREATE TABLE IF NOT EXISTS {name_table}
(
{QUERY_COLUMNS_SQL}
)
ENGINE = MergeTree
PARTITION BY event_date
PRIMARY KEY (instance_id, database_name, table_name, event_epoch_us)
ORDER BY
(
    instance_id ASC,
    database_name ASC,
    table_name ASC,
    event_epoch_us DESC,
    source_file_name DESC,
    end_position DESC,
    row_index DESC,
    event_id DESC,
    _content_revision DESC,
    _source_part_key DESC
)
{ttl_clause}SETTINGS
    disk = {name_disk},
    {common_settings};
{stage_schema}
{materialized_view_schema}
""".strip()


def split_direct_and_ranged_parts(
    parts: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Separate standard OSS Parquet objects from custom packed byte ranges.

    A first member has offset zero too, so `oss_length`, not `oss_offset`, is the
    decisive signal. Sending any member of a concatenated `.parquet-pack` to a
    naked `s3()` scan can silently return only the final embedded Parquet file.
    """

    key_counts: dict[str, int] = {}
    for part in parts:
        key = str(part.get("oss_key") or "")
        if key:
            key_counts[key] = key_counts.get(key, 0) + 1
    direct: list[dict[str, Any]] = []
    ranged: list[dict[str, Any]] = []
    for part in parts:
        if not str(part.get("oss_key") or ""):
            continue
        if (
            int(part.get("oss_length") or 0) > 0
            or key_counts.get(str(part.get("oss_key") or ""), 0) > 1
        ):
            ranged.append(part)
        else:
            direct.append(part)
    return direct, ranged


def _sql_string(value: str) -> str:
    value = str(value)
    if any(ord(character) < 32 for character in value):
        raise ValueError("ClickHouse SQL string contains a control character")
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def build_direct_s3_insert_sql(
    settings: Settings,
    *,
    database: str,
    table: str,
    parts: list[dict[str, Any]],
) -> str:
    """Build one credential-free INSERT for standalone OSS Parquet objects.

    `_path` is mapped back to the durable logical identity for every source
    object. Packed byte ranges are deliberately rejected because ClickHouse's
    naked S3 reader can silently expose only the final embedded Parquet member.
    """

    database = _identifier(database)
    table = _identifier(table)
    if not parts:
        raise ValueError("A direct S3 insert requires at least one part")
    direct, ranged = split_direct_and_ranged_parts(parts)
    if ranged or len(direct) != len(parts):
        raise ValueError("Direct S3 insert accepts standalone OSS objects only")
    base = _oss_base_url(settings)
    encoded_keys = [
        quote(str(part["oss_key"]), safe="/=._-") for part in direct
    ]
    path_expression = (
        encoded_keys[0]
        if len(encoded_keys) == 1
        else "{" + ",".join(encoded_keys) + "}"
    )
    url = base + path_expression
    bucket = settings.oss_bucket.strip().lower()
    paths = [
        f"{bucket}/{str(part['oss_key'])}"
        for part in direct
    ]

    def string_map(values: list[str]) -> str:
        entries: list[str] = []
        for path, value in zip(paths, values, strict=True):
            entries.extend((_sql_string(path), _sql_string(value)))
        return "map(" + ", ".join(entries) + ")"

    revision_entries: list[str] = []
    for path, part in zip(paths, direct, strict=True):
        revision_entries.extend(
            (
                _sql_string(path),
                f"toUInt64({max(int(part.get('content_revision') or 0), 0)})",
            )
        )
    part_keys = string_map(
        [str(part.get("logical_part_id") or "") for part in direct]
    )
    if any(not str(part.get("logical_part_id") or "") for part in direct):
        raise ValueError("Direct S3 insert part identity is missing")
    hashes = string_map([str(part.get("sha256") or "") for part in direct])
    revisions = "map(" + ", ".join(revision_entries) + ")"
    source_names = ", ".join(QUERY_SOURCE_COLUMNS)
    destination_names = ", ".join(
        (*QUERY_SOURCE_COLUMNS, *METADATA_COLUMNS)
    )
    return f"""
INSERT INTO {database}.{table} ({destination_names})
WITH {part_keys} AS part_keys,
     {hashes} AS part_hashes,
     {revisions} AS part_revisions
SELECT {source_names},
       part_keys[_path],
       part_hashes[_path],
       part_revisions[_path],
       now64(3)
FROM s3({_sql_string(url)}, Parquet)
WHERE mapContains(part_keys, _path)
SETTINGS
    input_format_parquet_use_native_reader_v3 = 0,
    input_format_parquet_max_block_size = 1024,
    input_format_parquet_prefer_block_bytes = 8388608,
    input_format_max_block_size_bytes = 33554432,
    input_format_parquet_enable_row_group_prefetch = 0,
    input_format_parquet_allow_missing_columns = 1,
    input_format_defaults_for_omitted_fields = 1,
    input_format_null_as_default = 1,
    max_block_size = 1024,
    max_insert_block_size = 1024,
    min_insert_block_size_rows = 1024,
    min_insert_block_size_bytes = 8388608
""".strip()
