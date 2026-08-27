from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

from .clickhouse_client import QUERY_SOURCE_COLUMNS, SOURCE_COLUMN_TYPES
from .clickhouse_oss import (
    QUERY_COLUMNS_SQL,
    _dynamic_disk,
    _identifier,
    _oss_base_url,
    _prefix,
    _sql_string,
)
from .config import Settings


RAW_SOURCE_ROOTS = {
    "binlog": ("mysql-binlog", "mysql-general-log"),
    "database": (
        "mysql-binlog",
        "mysql-general-log",
        "mysql-slow-log",
    ),
}


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


@dataclass(frozen=True, slots=True)
class ClickHouseRawOssConfig:
    """All-history query layer over the original immutable OSS Parquet.

    The manifest is intentionally tiny: one row per logical source part. Only
    custom byte-range pack members are copied into the packed exception table;
    ordinary objects stay in their existing OSS location and are read through
    ClickHouse's S3 reader.
    """

    enabled: bool
    serving_enabled: bool
    manifest_table: str
    packed_table: str
    prefix: str
    cache_gb: int

    @classmethod
    def from_env(cls) -> "ClickHouseRawOssConfig":
        return cls(
            enabled=_bool_env("RDS_BINLOG_CLICKHOUSE_RAW_OSS_ENABLED"),
            serving_enabled=_bool_env(
                "RDS_BINLOG_CLICKHOUSE_RAW_OSS_SERVING_ENABLED"
            ),
            manifest_table=_identifier(
                os.environ.get(
                    "RDS_BINLOG_CLICKHOUSE_RAW_OSS_MANIFEST_TABLE",
                    "oss_active_parts_v1",
                )
            ),
            packed_table=_identifier(
                os.environ.get(
                    "RDS_BINLOG_CLICKHOUSE_RAW_OSS_PACKED_TABLE",
                    "events_query_packed_v1",
                )
            ),
            prefix=_prefix(
                os.environ.get(
                    "RDS_BINLOG_CLICKHOUSE_RAW_OSS_PREFIX",
                    "sql-insight-clickhouse/raw-v1/",
                )
            ),
            cache_gb=_int_env(
                "RDS_BINLOG_CLICKHOUSE_RAW_OSS_CACHE_GB", 20, 2, 256
            ),
        )

    def __post_init__(self) -> None:
        _identifier(self.manifest_table)
        _identifier(self.packed_table)
        _prefix(self.prefix)
        if self.manifest_table == self.packed_table:
            raise ValueError("Raw OSS manifest and packed tables must differ")
        if not 2 <= int(self.cache_gb) <= 256:
            raise ValueError("Raw OSS cache must be 2..256 GiB")


def build_raw_oss_schema(
    settings: Settings,
    config: ClickHouseRawOssConfig,
    *,
    database: str,
) -> str:
    database = _identifier(database)
    manifest = f"{database}.{_identifier(config.manifest_table)}"
    packed = f"{database}.{_identifier(config.packed_table)}"
    packed_endpoint = (
        _oss_base_url(settings)
        + _prefix(config.prefix)
        + "packed-events/"
    )
    packed_disk = _dynamic_disk(
        packed_endpoint,
        cache_path="/var/lib/clickhouse/caches/sql-insight/raw-packed/",
        cache_gb=int(config.cache_gb),
    )
    return f"""
CREATE DATABASE IF NOT EXISTS {database};

CREATE TABLE IF NOT EXISTS {manifest}
(
    part_path String,
    logical_part_id String,
    sha256 FixedString(64),
    content_revision UInt64,
    source_kind LowCardinality(String),
    instance_id LowCardinality(String),
    event_date Date,
    min_event_epoch_us Int64,
    max_event_epoch_us Int64,
    row_count UInt64,
    size_bytes UInt64,
    oss_path String,
    oss_key String,
    oss_offset UInt64,
    oss_length UInt64,
    catalog_ready UInt8,
    database_names Array(String),
    table_names Array(String),
    operations Array(LowCardinality(String)),
    change_version UInt64,
    is_deleted UInt8,
    updated_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(change_version, is_deleted)
PRIMARY KEY part_path
ORDER BY part_path
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS {packed}
(
{QUERY_COLUMNS_SQL}
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
SETTINGS
    disk = {packed_disk},
    allow_experimental_reverse_key = 1,
    index_granularity = 8192,
    max_bytes_to_merge_at_max_space_in_pool = 134217728,
    non_replicated_deduplication_window = 100000;
""".strip()


def _utc_day_start_us(value: date) -> int:
    return int(
        datetime(value.year, value.month, value.day, tzinfo=UTC).timestamp()
        * 1_000_000
    )


def raw_oss_day_windows(
    start_epoch_us: int,
    end_epoch_us: int,
) -> list[tuple[str, int, int]]:
    """Split an inclusive UTC window newest-first to bound every OSS scan."""

    start_us = int(start_epoch_us)
    end_us = int(end_epoch_us)
    if end_us < start_us:
        return []
    start_day = datetime.fromtimestamp(start_us / 1_000_000, UTC).date()
    end_day = datetime.fromtimestamp(end_us / 1_000_000, UTC).date()
    result: list[tuple[str, int, int]] = []
    current = end_day
    while current >= start_day:
        day_start = _utc_day_start_us(current)
        next_start = _utc_day_start_us(current + timedelta(days=1))
        result.append(
            (
                current.isoformat(),
                max(start_us, day_start),
                min(end_us, next_start - 1),
            )
        )
        current -= timedelta(days=1)
    return result


def _source_kind(oss_key: str) -> str:
    return str(oss_key or "").split("/", 1)[0]


def build_raw_oss_manifest_rows(
    settings: Settings,
    parts: list[dict[str, Any]],
    *,
    catalogs: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Map source changes to versioned manifest rows.

    A missing/invisible source row becomes a ReplacingMergeTree tombstone. The
    order key is only ``part_path`` so content or OSS-key changes cannot leave
    an older active identity behind.
    """

    bucket = settings.oss_bucket.strip().lower()
    catalog_by_path = catalogs or {}
    # JSONEachRow's strict DateTime64 parser accepts this unambiguous UTC form
    # without requiring a session-wide best-effort parsing setting.
    updated_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    rows: list[dict[str, Any]] = []
    for part in parts:
        path = str(part.get("path") or "")
        if not path:
            raise ValueError("Raw OSS manifest change is missing part path")
        exists = bool(part.get("exists", True))
        visible = bool(int(part.get("query_visible") or 0)) if exists else False
        oss_key = str(part.get("oss_key") or "") if exists else ""
        active = bool(exists and visible and oss_key)
        sha256 = str(part.get("sha256") or "")
        catalog = catalog_by_path.get(path) or {}
        catalog_ready = bool(
            active
            and str(catalog.get("sha256") or "") == sha256
            and int(catalog.get("content_revision") or -1)
            == int(part.get("content_revision") or 0)
        )
        rows.append(
            {
                "part_path": path,
                "logical_part_id": str(part.get("logical_part_id") or ""),
                "sha256": sha256 if len(sha256) == 64 else "0" * 64,
                "content_revision": max(
                    int(part.get("content_revision") or 0), 0
                ),
                "source_kind": _source_kind(oss_key),
                "instance_id": str(part.get("instance_id") or ""),
                "event_date": str(part.get("event_date") or "1970-01-01"),
                "min_event_epoch_us": int(
                    part.get("min_event_epoch_us") or 0
                ),
                "max_event_epoch_us": int(
                    part.get("max_event_epoch_us") or 0
                ),
                "row_count": max(int(part.get("row_count") or 0), 0),
                "size_bytes": max(int(part.get("size_bytes") or 0), 0),
                "oss_path": f"{bucket}/{oss_key}" if active else "",
                "oss_key": oss_key if active else "",
                "oss_offset": max(int(part.get("oss_offset") or 0), 0),
                "oss_length": max(int(part.get("oss_length") or 0), 0),
                "catalog_ready": 1 if catalog_ready else 0,
                "database_names": (
                    sorted(set(catalog.get("databases") or []))
                    if catalog_ready
                    else []
                ),
                "table_names": (
                    sorted(set(catalog.get("tables") or []))
                    if catalog_ready
                    else []
                ),
                "operations": (
                    sorted(
                        {
                            str(value).upper()
                            for value in (catalog.get("operations") or [])
                            if str(value)
                        }
                    )
                    if catalog_ready
                    else []
                ),
                "change_version": max(
                    int(part.get("change_version") or 0), 0
                ),
                "is_deleted": 0 if active else 1,
                "updated_at": updated_at,
            }
        )
    return rows


def _manifest_filter_sql(
    query: dict[str, Any],
    *,
    root: str,
    direct: bool,
    parameters: dict[str, str | int],
) -> str:
    clauses = [
        "is_deleted = 0",
        f"source_kind = {_sql_string(root)}",
        "max_event_epoch_us >= {raw_start:Int64}",
        "min_event_epoch_us <= {raw_end:Int64}",
        "oss_length = 0" if direct else "oss_length > 0",
    ]
    instance = str(query.get("instance") or "").strip()
    if instance:
        parameters["raw_instance"] = instance
        clauses.append(
            "lowerUTF8(instance_id) = lowerUTF8({raw_instance:String})"
        )
    for key, column in (
        ("database", "database_names"),
        ("table", "table_names"),
    ):
        value = str(query.get(key) or "").strip()
        if not value:
            continue
        name = f"raw_{key}"
        parameters[name] = value
        clauses.append(
            "(catalog_ready = 0 OR arrayExists(value -> "
            f"positionCaseInsensitiveUTF8(value, {{{name}:String}}) > 0, "
            f"{column}))"
        )
    operations = sorted(
        {
            str(value).strip().upper()
            for value in (query.get("operations") or [])
            if str(value).strip()
        }
    )
    if operations:
        names: list[str] = []
        for index, operation in enumerate(operations):
            name = f"raw_operation_{index}"
            parameters[name] = operation
            names.append("{" + name + ":String}")
        clauses.append(
            "(catalog_ready = 0 OR hasAny(operations, ["
            + ", ".join(names)
            + "]))"
        )
    return " AND ".join(clauses)


def build_raw_oss_candidate_sql(
    config: ClickHouseRawOssConfig,
    *,
    database: str,
    query: dict[str, Any],
    start_epoch_us: int,
    end_epoch_us: int,
    limit: int = 64,
    cursor_max_event_epoch_us: int | None = None,
    cursor_part_path: str = "",
) -> tuple[str, dict[str, str | int]]:
    """Select a bounded, newest-first page of exact active OSS objects.

    ClickHouse applies an ``_path`` predicate only after expanding an S3 glob.
    On a bucket with hundreds of thousands of objects that turns a tiny query
    into a full prefix listing.  This first-stage query reads only the local
    manifest and returns exact keys; the data-stage query never uses a glob.
    """

    database = _identifier(database)
    source = str(query.get("source") or "").strip().lower()
    roots = RAW_SOURCE_ROOTS.get(source)
    if not roots:
        raise ValueError(f"Raw OSS source is unsupported: {source!r}")
    parameters: dict[str, str | int] = {
        "raw_start": int(start_epoch_us),
        "raw_end": int(end_epoch_us),
        "raw_candidate_limit": min(max(int(limit), 1), 256),
    }
    manifest = f"{database}.{_identifier(config.manifest_table)}"
    filters: list[str] = []
    for root in roots:
        for direct in (True, False):
            filters.append(
                "("
                + _manifest_filter_sql(
                    query,
                    root=root,
                    direct=direct,
                    parameters=parameters,
                )
                + ")"
            )
    cursor = ""
    if cursor_max_event_epoch_us is not None:
        parameters["raw_cursor_max"] = int(cursor_max_event_epoch_us)
        parameters["raw_cursor_path"] = str(cursor_part_path)
        cursor = """
          AND (
              max_event_epoch_us < {raw_cursor_max:Int64}
              OR (
                  max_event_epoch_us = {raw_cursor_max:Int64}
                  AND part_path < {raw_cursor_path:String}
              )
          )
        """
    sql = f"""
SELECT part_path, logical_part_id, sha256, content_revision,
       min_event_epoch_us, max_event_epoch_us, row_count, size_bytes,
       oss_path, oss_key, oss_offset, oss_length
FROM {manifest} FINAL
WHERE ({' OR '.join(filters)})
{cursor}
ORDER BY max_event_epoch_us DESC, part_path DESC
LIMIT {{raw_candidate_limit:UInt64}}
""".strip()
    return sql, parameters


def _exact_oss_url(settings: Settings, keys: list[str]) -> str:
    cleaned: list[str] = []
    for value in keys:
        key = str(value or "")
        if (
            not key
            or key.startswith("/")
            or "://" in key
            or any(character in key for character in "{},")
        ):
            raise ValueError("Raw OSS candidate contains an unsafe object key")
        cleaned.append(key)
    if not cleaned:
        raise ValueError("Raw OSS exact object list is empty")
    suffix = cleaned[0] if len(cleaned) == 1 else "{" + ",".join(cleaned) + "}"
    return _oss_base_url(settings) + suffix


def build_exact_raw_oss_source_sql(
    settings: Settings,
    config: ClickHouseRawOssConfig,
    *,
    database: str,
    candidates: list[dict[str, Any]],
) -> tuple[str, dict[str, str | int]]:
    """Build a credential-free union over exact keys and packed identities."""

    database = _identifier(database)
    packed = f"{database}.{_identifier(config.packed_table)}"
    direct = [
        candidate
        for candidate in candidates
        if int(candidate.get("oss_length") or 0) == 0
        and str(candidate.get("oss_key") or "")
    ]
    ranged = [
        candidate
        for candidate in candidates
        if int(candidate.get("oss_length") or 0) > 0
    ]
    if len(direct) + len(ranged) != len(candidates):
        raise ValueError("Raw OSS candidates include an unarchived object")
    if not candidates:
        raise ValueError("Raw OSS candidate batch is empty")

    type_by_name = dict(SOURCE_COLUMN_TYPES)
    structure = ", ".join(
        f"{name} {type_by_name[name]}" for name in QUERY_SOURCE_COLUMNS
    )
    source_columns = ", ".join(QUERY_SOURCE_COLUMNS)
    parameters: dict[str, str | int] = {}
    unions: list[str] = []
    if direct:
        part_items: list[str] = []
        sha_items: list[str] = []
        revision_items: list[str] = []
        for candidate in direct:
            path = str(candidate.get("oss_path") or "")
            if not path:
                raise ValueError("Raw OSS direct candidate is missing oss_path")
            part_items.extend(
                (
                    _sql_string(path),
                    _sql_string(str(candidate.get("logical_part_id") or "")),
                )
            )
            sha_items.extend(
                (
                    _sql_string(path),
                    _sql_string(str(candidate.get("sha256") or "")),
                )
            )
            revision_items.extend(
                (
                    _sql_string(path),
                    f"toUInt64({max(int(candidate.get('content_revision') or 0), 0)})",
                )
            )
        url = _exact_oss_url(
            settings,
            [str(candidate["oss_key"]) for candidate in direct],
        )
        unions.append(
            f"""
WITH map({', '.join(part_items)}) AS raw_part_keys,
     map({', '.join(sha_items)}) AS raw_part_hashes,
     map({', '.join(revision_items)}) AS raw_part_revisions
SELECT {source_columns},
       raw_part_keys[_path] AS _source_part_key,
       raw_part_hashes[_path] AS _source_part_sha256,
       raw_part_revisions[_path] AS _content_revision
FROM s3({_sql_string(url)}, Parquet, {_sql_string(structure)})
WHERE mapContains(raw_part_keys, _path)
""".strip()
        )
    if ranged:
        identities: list[str] = []
        for index, candidate in enumerate(ranged):
            identity_name = f"raw_pack_identity_{index}"
            hash_name = f"raw_pack_hash_{index}"
            revision_name = f"raw_pack_revision_{index}"
            parameters[identity_name] = str(
                candidate.get("logical_part_id") or ""
            )
            parameters[hash_name] = str(candidate.get("sha256") or "")
            parameters[revision_name] = max(
                int(candidate.get("content_revision") or 0), 0
            )
            identities.append(
                "("
                f"{{{identity_name}:String}}, "
                f"{{{hash_name}:String}}, "
                f"{{{revision_name}:UInt64}}"
                ")"
            )
        unions.append(
            f"""
SELECT {source_columns},
       _source_part_key,
       _source_part_sha256,
       _content_revision
FROM {packed}
WHERE (_source_part_key, _source_part_sha256, _content_revision) IN
      ({', '.join(identities)})
""".strip()
        )
    return "\nUNION ALL\n".join(unions), parameters
