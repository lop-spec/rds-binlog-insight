from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from math import isfinite
from pathlib import Path
from statistics import median
from typing import Any, Iterator

from .catalog_store import CatalogStore
from .config import Settings, json_dumps, utc_now_text
from .rds_api import RemoteBinlog


# Tabularis 审计事件登记成伪 binlog 文件，文件名固定用这个前缀（见
# tabularis_audit.py 的 log_file_name / source_file_name）；分区级筛选依赖它。
TABULARIS_AUDIT_FILE_PREFIX = "tabularis-audit-"
SLOW_LOG_FILE_PREFIX = "slow-log/"
SLOW_LOG_HOST_INSTANCE_ID = "slow-log"
METADATA_SCHEMA_VERSION = 1
LOGGER = logging.getLogger(__name__)

REQUIRED_RUNTIME_TABLES = frozenset(
    {
        "app_settings",
        "binlog_files",
        "jobs",
        "parquet_catalog_pending",
        "parquet_catalog_reconcile_state",
        "parquet_clickhouse_change_state",
        "parquet_clickhouse_pending",
        "parquet_file_stats",
        "parquet_file_stats_state",
        "parquet_part_catalog",
        "parquet_parts",
        "query_tasks",
    }
)
REQUIRED_RUNTIME_INDEXES = frozenset(
    {
        "idx_binlog_slowlog_source",
        "idx_binlog_visibility_id",
        "idx_catalog_pending_order",
        "idx_clickhouse_pending_version",
        "idx_part_archive",
        "idx_part_binlog_path",
        "idx_part_cold_compression",
        "idx_part_event_date_path",
        "idx_part_logical_id",
    }
)
REQUIRED_RUNTIME_TRIGGERS = frozenset(
    {
        "trg_catalog_pending_catalog_delete",
        "trg_catalog_pending_catalog_insert",
        "trg_catalog_pending_catalog_update",
        "trg_catalog_pending_part_content_update",
        "trg_catalog_pending_part_insert",
        "trg_catalog_pending_part_time_update",
        "trg_clickhouse_pending_file_visibility",
        "trg_clickhouse_pending_part_delete",
        "trg_clickhouse_pending_part_insert",
        "trg_clickhouse_pending_part_update",
        "trg_storage_file_stats_part_delete",
        "trg_storage_file_stats_part_insert",
        "trg_storage_file_stats_part_update",
    }
)


def _runtime_schema_gaps(conn: sqlite3.Connection) -> list[str]:
    objects: dict[str, set[str]] = {"table": set(), "index": set(), "trigger": set()}
    for row in conn.execute(
        "SELECT type, name FROM sqlite_master "
        "WHERE type IN ('table', 'index', 'trigger')"
    ):
        objects[str(row["type"])].add(str(row["name"]))
    gaps: list[str] = []
    for kind, required in (
        ("table", REQUIRED_RUNTIME_TABLES),
        ("index", REQUIRED_RUNTIME_INDEXES),
        ("trigger", REQUIRED_RUNTIME_TRIGGERS),
    ):
        gaps.extend(
            f"{kind}:{name}" for name in sorted(required - objects[kind])
        )
    return gaps


def logical_part_id(path: str, object_sha256: str) -> str:
    """Stable row-set identity; physical re-encoding keeps the stored value."""
    payload = f"rds-binlog-insight-part-v1\0{path}\0{object_sha256}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS app_settings (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    value_json TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS binlog_files (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    log_file_name TEXT NOT NULL,
    log_begin_utc TEXT NOT NULL,
    log_end_utc TEXT NOT NULL,
    file_size INTEGER NOT NULL,
    checksum_crc64 TEXT NOT NULL,
    download_link TEXT NOT NULL,
    intranet_download_link TEXT NOT NULL,
    link_expired_utc TEXT NOT NULL,
    remote_status TEXT NOT NULL,
    host_instance_id TEXT NOT NULL,
    state TEXT NOT NULL,
    query_visible INTEGER NOT NULL DEFAULT 1,
    attempts INTEGER NOT NULL DEFAULT 0,
    downloaded_bytes INTEGER NOT NULL DEFAULT 0,
    local_sha256 TEXT NOT NULL DEFAULT '',
    event_count INTEGER NOT NULL DEFAULT 0,
    error_code TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    discovered_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT NOT NULL DEFAULT '',
    processing_started_at TEXT NOT NULL DEFAULT '',
    processing_seconds REAL NOT NULL DEFAULT 0,
    raw_deleted_at TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_binlog_order
ON binlog_files(instance_id, log_begin_utc, log_end_utc, log_file_name);

CREATE INDEX IF NOT EXISTS idx_binlog_state
ON binlog_files(state, log_begin_utc);

CREATE INDEX IF NOT EXISTS idx_binlog_completion
ON binlog_files(instance_id, state, completed_at DESC);

CREATE TABLE IF NOT EXISTS parquet_parts (
    path TEXT PRIMARY KEY,
    binlog_id TEXT NOT NULL REFERENCES binlog_files(id) ON DELETE CASCADE,
    logical_part_id TEXT NOT NULL,
    event_date TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    min_event_epoch_us INTEGER NOT NULL,
    max_event_epoch_us INTEGER NOT NULL,
    size_bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    object_sha256 TEXT NOT NULL,
    compression_level INTEGER NOT NULL DEFAULT 1,
    compression_updated_at TEXT NOT NULL DEFAULT '',
    compression_error TEXT NOT NULL DEFAULT '',
    compression_retry_after TEXT NOT NULL DEFAULT '',
    oss_key TEXT NOT NULL DEFAULT '',
    oss_etag TEXT NOT NULL DEFAULT '',
    oss_offset INTEGER NOT NULL DEFAULT 0,
    oss_length INTEGER NOT NULL DEFAULT 0,
    oss_object_sha256 TEXT NOT NULL DEFAULT '',
    oss_uploaded_at TEXT NOT NULL DEFAULT '',
    oss_verified_at TEXT NOT NULL DEFAULT '',
    local_last_access_at TEXT NOT NULL DEFAULT '',
    content_revision INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_part_time
ON parquet_parts(min_event_epoch_us, max_event_epoch_us);

CREATE INDEX IF NOT EXISTS idx_part_sha
ON parquet_parts(sha256);

CREATE TABLE IF NOT EXISTS parquet_part_catalog (
    path TEXT PRIMARY KEY REFERENCES parquet_parts(path) ON DELETE CASCADE,
    sha256 TEXT NOT NULL,
    databases_json TEXT NOT NULL,
    tables_json TEXT NOT NULL,
    operations_json TEXT NOT NULL,
    indexed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS parquet_catalog_pending (
    path TEXT PRIMARY KEY REFERENCES parquet_parts(path) ON DELETE CASCADE,
    content_revision INTEGER NOT NULL,
    max_event_epoch_us INTEGER NOT NULL,
    min_event_epoch_us INTEGER NOT NULL,
    enqueued_at TEXT NOT NULL
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_catalog_pending_order
ON parquet_catalog_pending(
    max_event_epoch_us DESC,
    min_event_epoch_us DESC,
    path DESC
);

CREATE TABLE IF NOT EXISTS parquet_catalog_reconcile_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    after_path TEXT NOT NULL DEFAULT '',
    complete INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS parquet_negative_probes (
    path TEXT NOT NULL REFERENCES parquet_parts(path) ON DELETE CASCADE,
    sha256 TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    probed_at TEXT NOT NULL,
    PRIMARY KEY(path, fingerprint)
);

CREATE INDEX IF NOT EXISTS idx_negative_probe_fingerprint
ON parquet_negative_probes(fingerprint);

CREATE TABLE IF NOT EXISTS parquet_positive_probes (
    path TEXT NOT NULL REFERENCES parquet_parts(path) ON DELETE CASCADE,
    sha256 TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    rows_json TEXT NOT NULL,
    probed_at TEXT NOT NULL,
    PRIMARY KEY(path, fingerprint)
);

CREATE INDEX IF NOT EXISTS idx_positive_probe_fingerprint
ON parquet_positive_probes(fingerprint);

CREATE TABLE IF NOT EXISTS parquet_content_revision_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    revision INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS query_complete_certificates (
    fingerprint TEXT PRIMARY KEY,
    start_epoch_us INTEGER NOT NULL,
    end_epoch_us INTEGER NOT NULL,
    part_count INTEGER NOT NULL,
    max_content_revision INTEGER NOT NULL,
    content_revision_sum INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    rows_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_query_complete_certificates_created
ON query_complete_certificates(created_at DESC);

CREATE TABLE IF NOT EXISTS retired_oss_objects (
    oss_key TEXT PRIMARY KEY,
    object_sha256 TEXT NOT NULL DEFAULT '',
    oss_etag TEXT NOT NULL DEFAULT '',
    retired_at TEXT NOT NULL,
    delete_after TEXT NOT NULL,
    last_error TEXT NOT NULL DEFAULT '',
    retry_after TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_retired_oss_ready
ON retired_oss_objects(delete_after, retry_after);

CREATE TABLE IF NOT EXISTS parquet_part_identity_aliases (
    object_sha256 TEXT NOT NULL,
    logical_part_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY(object_sha256, logical_part_id)
);

CREATE INDEX IF NOT EXISTS idx_part_identity_alias_logical
ON parquet_part_identity_aliases(logical_part_id);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    project_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT NOT NULL DEFAULT '',
    current_file TEXT NOT NULL DEFAULT '',
    total_files INTEGER NOT NULL DEFAULT 0,
    completed_files INTEGER NOT NULL DEFAULT 0,
    failed_files INTEGER NOT NULL DEFAULT 0,
    discovered_files INTEGER NOT NULL DEFAULT 0,
    requested_start_utc TEXT NOT NULL DEFAULT '',
    requested_end_utc TEXT NOT NULL DEFAULT '',
    message TEXT NOT NULL DEFAULT '',
    error_code TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_jobs_started ON jobs(started_at DESC);

CREATE TABLE IF NOT EXISTS tabularis_audit_events (
    event_id TEXT PRIMARY KEY,
    source_file_id TEXT NOT NULL,
    state TEXT NOT NULL,
    accepted_at TEXT NOT NULL,
    stored_at TEXT NOT NULL DEFAULT '',
    archived_at TEXT NOT NULL DEFAULT '',
    last_error TEXT NOT NULL DEFAULT '',
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tabularis_audit_state
ON tabularis_audit_events(state, updated_at);

-- 本地执行日志的可查询副本。
--
-- 事件本体仍按 binlog 管道走 Parquet 并归档到 OSS（合规存档不变），但每条事件
-- 在那条管道里都是一个独立的伪 binlog 文件 + 一个单行 Parquet 分区；查询时要
-- 打开几千个文件，实测 3896 条要 0.8 秒。这一类数据量只有几千条，直接放带索引
-- 的表里查是毫秒级，所以查询走这里，Parquet 只作存档。
CREATE TABLE IF NOT EXISTS tabularis_audit_log (
    event_id TEXT PRIMARY KEY,
    event_epoch_us INTEGER NOT NULL,
    instance_id TEXT NOT NULL DEFAULT '',
    connection_id TEXT NOT NULL DEFAULT '',
    connection_name TEXT NOT NULL DEFAULT '',
    database_name TEXT NOT NULL DEFAULT '',
    table_name TEXT NOT NULL DEFAULT '',
    database_account TEXT NOT NULL DEFAULT '',
    execution_status TEXT NOT NULL DEFAULT '',
    operation TEXT NOT NULL DEFAULT '',
    sql_kind TEXT NOT NULL DEFAULT '',
    sql_text TEXT NOT NULL DEFAULT '',
    execution_time_ms INTEGER NOT NULL DEFAULT 0,
    affected_rows INTEGER NOT NULL DEFAULT 0,
    error_code INTEGER NOT NULL DEFAULT 0,
    error_message TEXT NOT NULL DEFAULT '',
    started_epoch_us INTEGER NOT NULL DEFAULT 0,
    finished_epoch_us INTEGER NOT NULL DEFAULT 0,
    batch_id TEXT NOT NULL DEFAULT '',
    statement_index INTEGER NOT NULL DEFAULT -1,
    transaction_context_id TEXT NOT NULL DEFAULT '',
    source_file_name TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_tabularis_audit_log_time
ON tabularis_audit_log(event_epoch_us DESC);

CREATE INDEX IF NOT EXISTS idx_tabularis_audit_log_instance
ON tabularis_audit_log(instance_id, event_epoch_us DESC);

CREATE INDEX IF NOT EXISTS idx_tabularis_audit_log_connection
ON tabularis_audit_log(connection_name, event_epoch_us DESC);

CREATE INDEX IF NOT EXISTS idx_tabularis_audit_log_status
ON tabularis_audit_log(execution_status, event_epoch_us DESC);

CREATE TABLE IF NOT EXISTS job_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    level TEXT NOT NULL,
    code TEXT NOT NULL,
    message TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_job_events_job ON job_events(job_id, id);

CREATE TABLE IF NOT EXISTS query_tasks (
    id TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    query_json TEXT NOT NULL,
    result_path TEXT NOT NULL DEFAULT '',
    result_bytes INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    started_at TEXT NOT NULL DEFAULT '',
    finished_at TEXT NOT NULL DEFAULT '',
    current_file TEXT NOT NULL DEFAULT '',
    total_parts INTEGER NOT NULL DEFAULT 0,
    completed_parts INTEGER NOT NULL DEFAULT 0,
    candidate_parts INTEGER NOT NULL DEFAULT 0,
    indexed_parts INTEGER NOT NULL DEFAULT 0,
    unknown_parts INTEGER NOT NULL DEFAULT 0,
    estimated_bytes INTEGER NOT NULL DEFAULT 0,
    scanned_bytes INTEGER NOT NULL DEFAULT 0,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    message TEXT NOT NULL DEFAULT '',
    error_code TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_query_tasks_created
ON query_tasks(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_query_tasks_status
ON query_tasks(status, created_at DESC);
"""


CATALOG_QUEUE_TRIGGERS = """
CREATE TRIGGER IF NOT EXISTS trg_catalog_pending_part_insert
AFTER INSERT ON parquet_parts
BEGIN
    INSERT INTO parquet_catalog_pending(
        path, content_revision, max_event_epoch_us,
        min_event_epoch_us, enqueued_at
    ) VALUES(
        NEW.path, NEW.content_revision, NEW.max_event_epoch_us,
        NEW.min_event_epoch_us,
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(path) DO UPDATE SET
        content_revision = excluded.content_revision,
        max_event_epoch_us = excluded.max_event_epoch_us,
        min_event_epoch_us = excluded.min_event_epoch_us,
        enqueued_at = excluded.enqueued_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_catalog_pending_part_content_update
AFTER UPDATE OF sha256, content_revision ON parquet_parts
WHEN NEW.sha256 <> OLD.sha256
  OR NEW.content_revision <> OLD.content_revision
BEGIN
    INSERT INTO parquet_catalog_pending(
        path, content_revision, max_event_epoch_us,
        min_event_epoch_us, enqueued_at
    ) VALUES(
        NEW.path, NEW.content_revision, NEW.max_event_epoch_us,
        NEW.min_event_epoch_us,
        strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    )
    ON CONFLICT(path) DO UPDATE SET
        content_revision = excluded.content_revision,
        max_event_epoch_us = excluded.max_event_epoch_us,
        min_event_epoch_us = excluded.min_event_epoch_us,
        enqueued_at = excluded.enqueued_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_catalog_pending_part_time_update
AFTER UPDATE OF max_event_epoch_us, min_event_epoch_us ON parquet_parts
BEGIN
    UPDATE parquet_catalog_pending
    SET max_event_epoch_us = NEW.max_event_epoch_us,
        min_event_epoch_us = NEW.min_event_epoch_us
    WHERE path = NEW.path;
END;

CREATE TRIGGER IF NOT EXISTS trg_catalog_pending_catalog_insert
AFTER INSERT ON parquet_part_catalog
BEGIN
    DELETE FROM parquet_catalog_pending
    WHERE path = NEW.path
      AND EXISTS(
          SELECT 1 FROM parquet_parts p
          WHERE p.path = NEW.path AND p.sha256 = NEW.sha256
      );
    INSERT INTO parquet_catalog_pending(
        path, content_revision, max_event_epoch_us,
        min_event_epoch_us, enqueued_at
    )
    SELECT p.path, p.content_revision, p.max_event_epoch_us,
           p.min_event_epoch_us,
           strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    FROM parquet_parts p
    WHERE p.path = NEW.path AND p.sha256 <> NEW.sha256
    ON CONFLICT(path) DO UPDATE SET
        content_revision = excluded.content_revision,
        max_event_epoch_us = excluded.max_event_epoch_us,
        min_event_epoch_us = excluded.min_event_epoch_us,
        enqueued_at = excluded.enqueued_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_catalog_pending_catalog_update
AFTER UPDATE ON parquet_part_catalog
BEGIN
    DELETE FROM parquet_catalog_pending
    WHERE path = NEW.path
      AND EXISTS(
          SELECT 1 FROM parquet_parts p
          WHERE p.path = NEW.path AND p.sha256 = NEW.sha256
      );
    INSERT INTO parquet_catalog_pending(
        path, content_revision, max_event_epoch_us,
        min_event_epoch_us, enqueued_at
    )
    SELECT p.path, p.content_revision, p.max_event_epoch_us,
           p.min_event_epoch_us,
           strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    FROM parquet_parts p
    WHERE p.path = NEW.path AND p.sha256 <> NEW.sha256
    ON CONFLICT(path) DO UPDATE SET
        content_revision = excluded.content_revision,
        max_event_epoch_us = excluded.max_event_epoch_us,
        min_event_epoch_us = excluded.min_event_epoch_us,
        enqueued_at = excluded.enqueued_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_catalog_pending_catalog_delete
AFTER DELETE ON parquet_part_catalog
BEGIN
    INSERT INTO parquet_catalog_pending(
        path, content_revision, max_event_epoch_us,
        min_event_epoch_us, enqueued_at
    )
    SELECT p.path, p.content_revision, p.max_event_epoch_us,
           p.min_event_epoch_us,
           strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    FROM parquet_parts p
    WHERE p.path = OLD.path
    ON CONFLICT(path) DO UPDATE SET
        content_revision = excluded.content_revision,
        max_event_epoch_us = excluded.max_event_epoch_us,
        min_event_epoch_us = excluded.min_event_epoch_us,
        enqueued_at = excluded.enqueued_at;
END;
"""


STORAGE_FILE_STATS_SCHEMA = """
CREATE TABLE IF NOT EXISTS parquet_file_stats (
    binlog_id TEXT PRIMARY KEY REFERENCES binlog_files(id) ON DELETE CASCADE,
    event_count INTEGER NOT NULL,
    parquet_bytes INTEGER NOT NULL,
    archived_bytes INTEGER NOT NULL,
    min_event_epoch_us INTEGER NOT NULL,
    max_event_epoch_us INTEGER NOT NULL,
    part_count INTEGER NOT NULL,
    archived_part_count INTEGER NOT NULL,
    zstd1_part_count INTEGER NOT NULL,
    zstd9_part_count INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS parquet_file_stats_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    complete INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

CREATE TRIGGER IF NOT EXISTS trg_storage_file_stats_part_insert
AFTER INSERT ON parquet_parts
BEGIN
    INSERT INTO parquet_file_stats(
        binlog_id, event_count, parquet_bytes, archived_bytes,
        min_event_epoch_us, max_event_epoch_us, part_count,
        archived_part_count, zstd1_part_count, zstd9_part_count
    ) VALUES(
        NEW.binlog_id, NEW.row_count, NEW.size_bytes,
        CASE WHEN NEW.oss_key <> '' THEN NEW.size_bytes ELSE 0 END,
        NEW.min_event_epoch_us, NEW.max_event_epoch_us, 1,
        CASE WHEN NEW.oss_key <> '' THEN 1 ELSE 0 END,
        CASE WHEN NEW.compression_level < 9 THEN 1 ELSE 0 END,
        CASE WHEN NEW.compression_level >= 9 THEN 1 ELSE 0 END
    )
    ON CONFLICT(binlog_id) DO UPDATE SET
        event_count = event_count + excluded.event_count,
        parquet_bytes = parquet_bytes + excluded.parquet_bytes,
        archived_bytes = archived_bytes + excluded.archived_bytes,
        min_event_epoch_us = MIN(min_event_epoch_us, excluded.min_event_epoch_us),
        max_event_epoch_us = MAX(max_event_epoch_us, excluded.max_event_epoch_us),
        part_count = part_count + 1,
        archived_part_count = archived_part_count + excluded.archived_part_count,
        zstd1_part_count = zstd1_part_count + excluded.zstd1_part_count,
        zstd9_part_count = zstd9_part_count + excluded.zstd9_part_count;
END;

CREATE TRIGGER IF NOT EXISTS trg_storage_file_stats_part_update
AFTER UPDATE OF binlog_id, row_count, size_bytes, min_event_epoch_us,
                max_event_epoch_us, oss_key, compression_level
ON parquet_parts
BEGIN
    UPDATE parquet_file_stats
    SET event_count = event_count - OLD.row_count,
        parquet_bytes = parquet_bytes - OLD.size_bytes,
        archived_bytes = archived_bytes -
            CASE WHEN OLD.oss_key <> '' THEN OLD.size_bytes ELSE 0 END,
        min_event_epoch_us = COALESCE((
            SELECT MIN(min_event_epoch_us) FROM parquet_parts
            WHERE binlog_id = OLD.binlog_id
        ), 0),
        max_event_epoch_us = COALESCE((
            SELECT MAX(max_event_epoch_us) FROM parquet_parts
            WHERE binlog_id = OLD.binlog_id
        ), 0),
        part_count = part_count - 1,
        archived_part_count = archived_part_count -
            CASE WHEN OLD.oss_key <> '' THEN 1 ELSE 0 END,
        zstd1_part_count = zstd1_part_count -
            CASE WHEN OLD.compression_level < 9 THEN 1 ELSE 0 END,
        zstd9_part_count = zstd9_part_count -
            CASE WHEN OLD.compression_level >= 9 THEN 1 ELSE 0 END
    WHERE binlog_id = OLD.binlog_id;
    DELETE FROM parquet_file_stats
    WHERE binlog_id = OLD.binlog_id AND part_count <= 0;
    INSERT INTO parquet_file_stats(
        binlog_id, event_count, parquet_bytes, archived_bytes,
        min_event_epoch_us, max_event_epoch_us, part_count,
        archived_part_count, zstd1_part_count, zstd9_part_count
    ) VALUES(
        NEW.binlog_id, NEW.row_count, NEW.size_bytes,
        CASE WHEN NEW.oss_key <> '' THEN NEW.size_bytes ELSE 0 END,
        NEW.min_event_epoch_us, NEW.max_event_epoch_us, 1,
        CASE WHEN NEW.oss_key <> '' THEN 1 ELSE 0 END,
        CASE WHEN NEW.compression_level < 9 THEN 1 ELSE 0 END,
        CASE WHEN NEW.compression_level >= 9 THEN 1 ELSE 0 END
    )
    ON CONFLICT(binlog_id) DO UPDATE SET
        event_count = event_count + excluded.event_count,
        parquet_bytes = parquet_bytes + excluded.parquet_bytes,
        archived_bytes = archived_bytes + excluded.archived_bytes,
        min_event_epoch_us = CASE
            WHEN min_event_epoch_us = 0 THEN excluded.min_event_epoch_us
            ELSE MIN(min_event_epoch_us, excluded.min_event_epoch_us)
        END,
        max_event_epoch_us = MAX(max_event_epoch_us, excluded.max_event_epoch_us),
        part_count = part_count + 1,
        archived_part_count = archived_part_count + excluded.archived_part_count,
        zstd1_part_count = zstd1_part_count + excluded.zstd1_part_count,
        zstd9_part_count = zstd9_part_count + excluded.zstd9_part_count;
END;

CREATE TRIGGER IF NOT EXISTS trg_storage_file_stats_part_delete
AFTER DELETE ON parquet_parts
BEGIN
    UPDATE parquet_file_stats
    SET event_count = event_count - OLD.row_count,
        parquet_bytes = parquet_bytes - OLD.size_bytes,
        archived_bytes = archived_bytes -
            CASE WHEN OLD.oss_key <> '' THEN OLD.size_bytes ELSE 0 END,
        min_event_epoch_us = COALESCE((
            SELECT MIN(min_event_epoch_us) FROM parquet_parts
            WHERE binlog_id = OLD.binlog_id
        ), 0),
        max_event_epoch_us = COALESCE((
            SELECT MAX(max_event_epoch_us) FROM parquet_parts
            WHERE binlog_id = OLD.binlog_id
        ), 0),
        part_count = part_count - 1,
        archived_part_count = archived_part_count -
            CASE WHEN OLD.oss_key <> '' THEN 1 ELSE 0 END,
        zstd1_part_count = zstd1_part_count -
            CASE WHEN OLD.compression_level < 9 THEN 1 ELSE 0 END,
        zstd9_part_count = zstd9_part_count -
            CASE WHEN OLD.compression_level >= 9 THEN 1 ELSE 0 END
    WHERE binlog_id = OLD.binlog_id;
    DELETE FROM parquet_file_stats
    WHERE binlog_id = OLD.binlog_id AND part_count <= 0;
END;
"""


CLICKHOUSE_CHANGE_QUEUE_SCHEMA = """
CREATE TABLE IF NOT EXISTS parquet_clickhouse_change_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    complete INTEGER NOT NULL DEFAULT 0,
    next_change_version INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

INSERT OR IGNORE INTO parquet_clickhouse_change_state(
    singleton, complete, next_change_version, updated_at
) VALUES(1, 0, 0, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'));

CREATE TABLE IF NOT EXISTS parquet_clickhouse_pending (
    path TEXT PRIMARY KEY,
    change_version INTEGER NOT NULL,
    enqueued_at TEXT NOT NULL
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_clickhouse_pending_version
ON parquet_clickhouse_pending(change_version, path);

CREATE TRIGGER IF NOT EXISTS trg_clickhouse_pending_part_insert
AFTER INSERT ON parquet_parts
WHEN EXISTS (
    SELECT 1 FROM binlog_files b
    WHERE b.id = NEW.binlog_id
      AND b.query_visible = 1
      AND b.log_file_name NOT LIKE 'tabularis-audit-%'
)
BEGIN
    UPDATE parquet_clickhouse_change_state
    SET next_change_version = next_change_version + 1,
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    WHERE singleton = 1;
    INSERT INTO parquet_clickhouse_pending(path, change_version, enqueued_at)
    SELECT NEW.path, next_change_version,
           strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    FROM parquet_clickhouse_change_state WHERE singleton = 1
    ON CONFLICT(path) DO UPDATE SET
        change_version = excluded.change_version,
        enqueued_at = excluded.enqueued_at;
END;

DROP TRIGGER IF EXISTS trg_clickhouse_pending_part_update;
CREATE TRIGGER trg_clickhouse_pending_part_update
AFTER UPDATE OF binlog_id, logical_part_id, sha256, content_revision,
                min_event_epoch_us, max_event_epoch_us, row_count,
                size_bytes, oss_key, oss_etag, oss_offset, oss_length,
                oss_object_sha256
ON parquet_parts
WHEN (
    OLD.binlog_id <> NEW.binlog_id
    OR OLD.logical_part_id <> NEW.logical_part_id
    OR OLD.sha256 <> NEW.sha256
    OR OLD.content_revision <> NEW.content_revision
    OR OLD.min_event_epoch_us <> NEW.min_event_epoch_us
    OR OLD.max_event_epoch_us <> NEW.max_event_epoch_us
    OR OLD.row_count <> NEW.row_count
    OR OLD.size_bytes <> NEW.size_bytes
    OR OLD.oss_key <> NEW.oss_key
    OR OLD.oss_etag <> NEW.oss_etag
    OR OLD.oss_offset <> NEW.oss_offset
    OR OLD.oss_length <> NEW.oss_length
    OR OLD.oss_object_sha256 <> NEW.oss_object_sha256
) AND (
    EXISTS (
        SELECT 1 FROM binlog_files b
        WHERE b.id = OLD.binlog_id
          AND b.query_visible = 1
          AND b.log_file_name NOT LIKE 'tabularis-audit-%'
    )
    OR EXISTS (
        SELECT 1 FROM binlog_files b
        WHERE b.id = NEW.binlog_id
          AND b.query_visible = 1
          AND b.log_file_name NOT LIKE 'tabularis-audit-%'
    )
)
BEGIN
    UPDATE parquet_clickhouse_change_state
    SET next_change_version = next_change_version + 1,
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    WHERE singleton = 1;
    INSERT INTO parquet_clickhouse_pending(path, change_version, enqueued_at)
    SELECT NEW.path, next_change_version,
           strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    FROM parquet_clickhouse_change_state WHERE singleton = 1
    ON CONFLICT(path) DO UPDATE SET
        change_version = excluded.change_version,
        enqueued_at = excluded.enqueued_at;
END;

-- A parent binlog deletion removes its row before cascading into parquet_parts,
-- so the child trigger cannot classify the old filename. Queue every delete;
-- the reconciler safely treats audit-only tombstones as irrelevant.
CREATE TRIGGER IF NOT EXISTS trg_clickhouse_pending_part_delete
AFTER DELETE ON parquet_parts
BEGIN
    UPDATE parquet_clickhouse_change_state
    SET next_change_version = next_change_version + 1,
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    WHERE singleton = 1;
    INSERT INTO parquet_clickhouse_pending(path, change_version, enqueued_at)
    SELECT OLD.path, next_change_version,
           strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    FROM parquet_clickhouse_change_state WHERE singleton = 1
    ON CONFLICT(path) DO UPDATE SET
        change_version = excluded.change_version,
        enqueued_at = excluded.enqueued_at;
END;

CREATE TRIGGER IF NOT EXISTS trg_clickhouse_pending_file_visibility
AFTER UPDATE OF query_visible, log_file_name ON binlog_files
WHEN OLD.query_visible <> NEW.query_visible
  OR OLD.log_file_name <> NEW.log_file_name
BEGIN
    UPDATE parquet_clickhouse_change_state
    SET next_change_version = next_change_version + 1,
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    WHERE singleton = 1;
    INSERT INTO parquet_clickhouse_pending(path, change_version, enqueued_at)
    SELECT p.path, s.next_change_version,
           strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    FROM parquet_parts p
    CROSS JOIN parquet_clickhouse_change_state s
    WHERE p.binlog_id = NEW.id AND s.singleton = 1
      AND (
          (OLD.query_visible = 1
           AND OLD.log_file_name NOT LIKE 'tabularis-audit-%')
          OR (NEW.query_visible = 1
              AND NEW.log_file_name NOT LIKE 'tabularis-audit-%')
      )
    ON CONFLICT(path) DO UPDATE SET
        change_version = excluded.change_version,
        enqueued_at = excluded.enqueued_at;
END;
"""


class MetadataStore:
    def __init__(self, path: Path, *, run_migrations: bool = True):
        self.path = path
        self.run_migrations = bool(run_migrations)
        self._write_lock = threading.RLock()
        self._anchor_lock = threading.Lock()
        self._wal_anchor: sqlite3.Connection | None = None
        if not run_migrations:
            # Keep one connection alive before runtime validation reads.
            # Otherwise every short operation can become the last connection,
            # forcing an EXCLUSIVE checkpoint/WAL cleanup window.
            if not self.path.is_file():
                self._require_schema_version()
            self._open_wal_anchor()
            try:
                self._require_schema_version()
                self.catalog_store = CatalogStore(
                    self.path.parent / "index" / "catalog.sqlite3",
                    run_migrations=False,
                )
            except Exception:
                self.close()
                raise
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.catalog_store = CatalogStore(
            self.path.parent / "index" / "catalog.sqlite3"
        )
        with self.connection() as conn:
            row = conn.execute("PRAGMA user_version").fetchone()
            version = int(row[0] if row else 0)
            if version == METADATA_SCHEMA_VERSION:
                conn.executescript(SCHEMA)
                conn.executescript(CATALOG_QUEUE_TRIGGERS)
                self._ensure_storage_file_stats_schema(conn)
                conn.executescript(CLICKHOUSE_CHANGE_QUEUE_SCHEMA)
                self._ensure_additive_serving_indexes(conn)
                gaps = _runtime_schema_gaps(conn)
                if gaps:
                    raise RuntimeError(
                        "Metadata migration is incomplete: " + ", ".join(gaps)
                    )
                return
            if version > METADATA_SCHEMA_VERSION:
                raise RuntimeError(
                    f"Metadata schema version {version} is newer than supported "
                    f"version {METADATA_SCHEMA_VERSION}"
                )
            conn.executescript(SCHEMA)
            job_columns = {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(jobs)").fetchall()
            }
            for name in ("requested_start_utc", "requested_end_utc"):
                if name not in job_columns:
                    conn.execute(
                        f"ALTER TABLE jobs ADD COLUMN {name} "
                        "TEXT NOT NULL DEFAULT ''"
                    )
            file_columns = {
                str(row["name"])
                for row in conn.execute(
                    "PRAGMA table_info(binlog_files)"
                ).fetchall()
            }
            if "query_visible" not in file_columns:
                conn.execute(
                    "ALTER TABLE binlog_files ADD COLUMN query_visible "
                    "INTEGER NOT NULL DEFAULT 1"
                )
                conn.execute(
                    "UPDATE binlog_files SET query_visible = 0 "
                    "WHERE state <> 'done'"
                )
            for name, declaration in (
                ("processing_started_at", "TEXT NOT NULL DEFAULT ''"),
                ("processing_seconds", "REAL NOT NULL DEFAULT 0"),
            ):
                if name not in file_columns:
                    conn.execute(
                        f"ALTER TABLE binlog_files ADD COLUMN {name} {declaration}"
                    )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_binlog_visibility "
                "ON binlog_files(query_visible, state, log_begin_utc)"
            )
            part_columns = {
                str(row["name"])
                for row in conn.execute(
                    "PRAGMA table_info(parquet_parts)"
                ).fetchall()
            }
            for name in (
                "oss_key",
                "oss_etag",
                "oss_uploaded_at",
                "oss_verified_at",
                "local_last_access_at",
            ):
                if name not in part_columns:
                    conn.execute(
                        f"ALTER TABLE parquet_parts ADD COLUMN {name} "
                        "TEXT NOT NULL DEFAULT ''"
                    )
            for name in ("oss_offset", "oss_length"):
                if name not in part_columns:
                    conn.execute(
                        f"ALTER TABLE parquet_parts ADD COLUMN {name} "
                        "INTEGER NOT NULL DEFAULT 0"
                    )
            if "oss_object_sha256" not in part_columns:
                conn.execute(
                    "ALTER TABLE parquet_parts ADD COLUMN oss_object_sha256 "
                    "TEXT NOT NULL DEFAULT ''"
                )
            if "content_revision" not in part_columns:
                conn.execute(
                    "ALTER TABLE parquet_parts ADD COLUMN content_revision "
                    "INTEGER NOT NULL DEFAULT 0"
                )
            for name, declaration in (
                ("logical_part_id", "TEXT NOT NULL DEFAULT ''"),
                ("object_sha256", "TEXT NOT NULL DEFAULT ''"),
                ("compression_level", "INTEGER NOT NULL DEFAULT 1"),
                ("compression_updated_at", "TEXT NOT NULL DEFAULT ''"),
                ("compression_error", "TEXT NOT NULL DEFAULT ''"),
                ("compression_retry_after", "TEXT NOT NULL DEFAULT ''"),
            ):
                if name not in part_columns:
                    conn.execute(
                        f"ALTER TABLE parquet_parts ADD COLUMN {name} {declaration}"
                    )
            conn.execute(
                "UPDATE parquet_parts SET object_sha256 = sha256 "
                "WHERE object_sha256 = ''"
            )
            missing_identities = conn.execute(
                "SELECT path, object_sha256 FROM parquet_parts "
                "WHERE logical_part_id = ''"
            ).fetchall()
            if missing_identities:
                conn.executemany(
                    "UPDATE parquet_parts SET logical_part_id = ? WHERE path = ?",
                    [
                        (
                            logical_part_id(
                                str(row["path"]), str(row["object_sha256"])
                            ),
                            str(row["path"]),
                        )
                        for row in missing_identities
                    ],
                )
            conn.execute(
                """
                INSERT OR IGNORE INTO parquet_part_identity_aliases(
                    object_sha256, logical_part_id, created_at
                )
                SELECT object_sha256, logical_part_id, ?
                FROM parquet_parts
                WHERE object_sha256 <> '' AND logical_part_id <> ''
                """,
                (utc_now_text(),),
            )
            conn.execute(
                "INSERT OR IGNORE INTO parquet_content_revision_state"
                "(singleton, revision) VALUES(1, 0)"
            )
            existing_parts = int(
                conn.execute("SELECT COUNT(*) FROM parquet_parts").fetchone()[0]
            )
            if existing_parts:
                conn.execute(
                    "UPDATE parquet_parts SET content_revision = 1 "
                    "WHERE content_revision = 0"
                )
                conn.execute(
                    "UPDATE parquet_content_revision_state "
                    "SET revision = MAX(revision, 1) WHERE singleton = 1"
                )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_part_archive "
                "ON parquet_parts(oss_key, min_event_epoch_us)"
            )
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_part_logical_id "
                "ON parquet_parts(logical_part_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_part_cold_compression "
                "ON parquet_parts(compression_level, max_event_epoch_us, "
                "compression_retry_after)"
            )
            row = conn.execute(
                "SELECT 1 FROM app_settings WHERE singleton = 1"
            ).fetchone()
            if not row:
                conn.execute(
                    "INSERT INTO app_settings(singleton, value_json, updated_at) "
                    "VALUES(1, ?, ?)",
                    (json_dumps(asdict(Settings())), utc_now_text()),
                )
            reconcile = conn.execute(
                "SELECT 1 FROM parquet_catalog_reconcile_state "
                "WHERE singleton = 1"
            ).fetchone()
            if not reconcile:
                conn.execute(
                    "INSERT INTO parquet_catalog_reconcile_state("
                    "singleton, after_path, complete, updated_at"
                    ") VALUES(1, '', ?, ?)",
                    (1 if existing_parts == 0 else 0, utc_now_text()),
                )
            conn.executescript(CATALOG_QUEUE_TRIGGERS)
            self._ensure_storage_file_stats_schema(
                conn, existing_parts=existing_parts
            )
            conn.executescript(CLICKHOUSE_CHANGE_QUEUE_SCHEMA)
            self._ensure_additive_serving_indexes(conn)
            conn.execute(f"PRAGMA user_version = {METADATA_SCHEMA_VERSION}")

    def _open_wal_anchor(self) -> None:
        """Keep WAL coordination files stable for this store's lifetime."""

        with self._anchor_lock:
            if self._wal_anchor is not None:
                return
            connection = sqlite3.connect(
                self.path,
                timeout=5,
                isolation_level=None,
                check_same_thread=False,
            )
            try:
                connection.row_factory = sqlite3.Row
                connection.execute("PRAGMA foreign_keys=ON")
                connection.execute("PRAGMA busy_timeout=5000")
                connection.execute("PRAGMA query_only=ON")
                journal_mode = str(
                    connection.execute("PRAGMA journal_mode").fetchone()[0]
                ).lower()
                if journal_mode != "wal":
                    raise RuntimeError(
                        f"Metadata WAL anchor requires journal_mode=wal, got {journal_mode}"
                    )
                connection.execute("SELECT 1 FROM sqlite_master LIMIT 1").fetchone()
            except Exception:
                connection.close()
                raise
            self._wal_anchor = connection

    def close(self) -> None:
        with self._anchor_lock:
            connection = self._wal_anchor
            self._wal_anchor = None
        if connection is not None:
            connection.close()

    def __del__(self) -> None:
        try:
            self.close()
        except Exception:
            pass

    @staticmethod
    def _ensure_storage_file_stats_schema(
        conn: sqlite3.Connection, *, existing_parts: int | None = None
    ) -> None:
        """Install additive rollup objects without backfilling historical rows."""

        conn.executescript(STORAGE_FILE_STATS_SCHEMA)
        if existing_parts is None:
            existing_parts = int(
                conn.execute("SELECT COUNT(*) FROM parquet_parts").fetchone()[0]
            )
        conn.execute(
            "INSERT OR IGNORE INTO parquet_file_stats_state("
            "singleton, complete, updated_at) VALUES(1, ?, ?)",
            (1 if existing_parts == 0 else 0, utc_now_text()),
        )

    @staticmethod
    def _ensure_additive_serving_indexes(conn: sqlite3.Connection) -> None:
        """Install rollback-compatible serving indexes from the main service only."""

        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_part_archive "
            "ON parquet_parts(oss_key, min_event_epoch_us)"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_part_logical_id "
            "ON parquet_parts(logical_part_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_part_cold_compression "
            "ON parquet_parts(compression_level, max_event_epoch_us, "
            "compression_retry_after)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_binlog_slowlog_source "
            "ON binlog_files(host_instance_id, query_visible, id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_binlog_visibility_id "
            "ON binlog_files(query_visible, id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_part_binlog_path "
            "ON parquet_parts(binlog_id, path)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_part_event_date_path "
            "ON parquet_parts(event_date, path)"
        )

    def _require_schema_version(self) -> None:
        if not self.path.is_file():
            raise RuntimeError(
                f"Metadata schema version {METADATA_SCHEMA_VERSION} is required; "
                "the database does not exist"
            )
        with self.connection() as conn:
            row = conn.execute("PRAGMA user_version").fetchone()
            gaps = _runtime_schema_gaps(conn)
        version = int(row[0] if row else 0)
        if version != METADATA_SCHEMA_VERSION:
            raise RuntimeError(
                f"Metadata schema version {version} is not supported by this "
                f"runtime; migrate to version {METADATA_SCHEMA_VERSION} first"
            )
        if gaps:
            raise RuntimeError(
                "Metadata schema is incomplete: "
                + ", ".join(gaps)
                + "; run app.metadata_migrate first"
            )

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _next_content_revision(conn: sqlite3.Connection) -> int:
        conn.execute(
            "UPDATE parquet_content_revision_state "
            "SET revision = revision + 1 WHERE singleton = 1"
        )
        row = conn.execute(
            "SELECT revision FROM parquet_content_revision_state "
            "WHERE singleton = 1"
        ).fetchone()
        if not row:
            raise RuntimeError("Parquet content revision state is missing")
        return int(row["revision"])

    @staticmethod
    def _retire_oss_object(
        conn: sqlite3.Connection,
        part: sqlite3.Row | dict[str, Any],
        *,
        grace_seconds: int = 3600,
    ) -> None:
        key = str(part["oss_key"] or "")
        if not key:
            return
        now = datetime.now(UTC)
        retired_at = now.isoformat(timespec="seconds").replace("+00:00", "Z")
        delete_after = (now + timedelta(seconds=max(int(grace_seconds), 0))).isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z")
        conn.execute(
            """
            INSERT INTO retired_oss_objects(
                oss_key, object_sha256, oss_etag, retired_at, delete_after
            ) VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(oss_key) DO UPDATE SET
                object_sha256 = CASE
                    WHEN retired_oss_objects.object_sha256 = ''
                    THEN excluded.object_sha256
                    ELSE retired_oss_objects.object_sha256 END,
                oss_etag = CASE
                    WHEN retired_oss_objects.oss_etag = ''
                    THEN excluded.oss_etag
                    ELSE retired_oss_objects.oss_etag END,
                delete_after = MAX(
                    retired_oss_objects.delete_after, excluded.delete_after
                )
            """,
            (
                key,
                str(part["oss_object_sha256"] or part["object_sha256"] or ""),
                str(part["oss_etag"] or ""),
                retired_at,
                delete_after,
            ),
        )

    @staticmethod
    def _part_content_token(
        conn: sqlite3.Connection,
        start_epoch_us: int,
        end_epoch_us: int,
    ) -> dict[str, int]:
        row = conn.execute(
            """
            SELECT COUNT(*) AS part_count,
                   COALESCE(MAX(p.content_revision), 0) AS max_content_revision,
                   COALESCE(SUM(p.content_revision), 0) AS content_revision_sum
            FROM parquet_parts p
            JOIN binlog_files b ON b.id = p.binlog_id
            WHERE p.max_event_epoch_us >= ?
              AND p.min_event_epoch_us <= ?
              AND b.query_visible = 1
            """,
            (int(start_epoch_us), int(end_epoch_us)),
        ).fetchone()
        return {
            "part_count": int(row["part_count"] or 0),
            "max_content_revision": int(row["max_content_revision"] or 0),
            "content_revision_sum": int(row["content_revision_sum"] or 0),
        }

    def load_settings(self) -> Settings:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT value_json FROM app_settings WHERE singleton = 1"
            ).fetchone()
        return Settings.from_mapping(json.loads(row["value_json"]) if row else {})

    def save_settings(self, settings: Settings) -> None:
        settings.validate(require_identity=False)
        with self._write_lock, self.connection() as conn:
            conn.execute(
                "UPDATE app_settings SET value_json = ?, updated_at = ? "
                "WHERE singleton = 1",
                (json_dumps(asdict(settings)), utc_now_text()),
            )

    @staticmethod
    def file_id(instance_id: str, item: RemoteBinlog) -> str:
        import hashlib

        return hashlib.sha256(
            f"{instance_id}\x1f{item.stable_id}".encode("utf-8")
        ).hexdigest()

    def claim_tabularis_audit_events(
        self,
        events: list[tuple[str, str]],
    ) -> tuple[list[str], int]:
        """Claim new events atomically; failed or pending events remain retryable."""

        if not events:
            return [], 0
        now = utc_now_text()
        claimed: list[str] = []
        duplicates = 0
        seen: set[str] = set()
        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for event_id, source_file_id in events:
                    if event_id in seen:
                        duplicates += 1
                        continue
                    seen.add(event_id)
                    row = conn.execute(
                        "SELECT state FROM tabularis_audit_events WHERE event_id = ?",
                        (event_id,),
                    ).fetchone()
                    if row:
                        duplicates += 1
                        continue
                    conn.execute(
                        """
                        INSERT INTO tabularis_audit_events(
                            event_id, source_file_id, state, accepted_at, updated_at
                        ) VALUES(?, ?, 'accepted', ?, ?)
                        """,
                        (event_id, source_file_id, now, now),
                    )
                    claimed.append(event_id)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return claimed, duplicates

    def tabularis_audit_rows(self, event_ids: list[str]) -> dict[str, dict[str, Any]]:
        if not event_ids:
            return {}
        placeholders = ",".join("?" for _ in event_ids)
        with self.connection() as conn:
            rows = conn.execute(
                f"SELECT * FROM tabularis_audit_events WHERE event_id IN ({placeholders})",
                event_ids,
            ).fetchall()
        return {str(row["event_id"]): dict(row) for row in rows}

    def set_tabularis_audit_state(
        self,
        event_ids: list[str],
        state: str,
        *,
        error: str = "",
    ) -> None:
        if not event_ids:
            return
        if state not in {"accepted", "stored", "archived", "failed"}:
            raise ValueError("Tabularis audit state is invalid")
        now = utc_now_text()
        stored_at = now if state in {"stored", "archived"} else ""
        archived_at = now if state == "archived" else ""
        placeholders = ",".join("?" for _ in event_ids)
        with self._write_lock, self.connection() as conn:
            conn.execute(
                f"""
                UPDATE tabularis_audit_events
                SET state = ?,
                    stored_at = CASE WHEN ? <> '' THEN ? ELSE stored_at END,
                    archived_at = CASE WHEN ? <> '' THEN ? ELSE archived_at END,
                    last_error = ?, updated_at = ?
                WHERE event_id IN ({placeholders})
                """,
                (
                    state,
                    stored_at,
                    stored_at,
                    archived_at,
                    archived_at,
                    error[:2000],
                    now,
                    *event_ids,
                ),
            )

    def acknowledged_tabularis_audit_events(self, event_ids: list[str]) -> list[str]:
        rows = self.tabularis_audit_rows(event_ids)
        return [
            event_id
            for event_id in event_ids
            if rows.get(event_id, {}).get("state") == "archived"
        ]

    TABULARIS_AUDIT_LOG_COLUMNS = (
        "event_id",
        "event_epoch_us",
        "instance_id",
        "connection_id",
        "connection_name",
        "database_name",
        "table_name",
        "database_account",
        "execution_status",
        "operation",
        "sql_kind",
        "sql_text",
        "execution_time_ms",
        "affected_rows",
        "error_code",
        "error_message",
        "started_epoch_us",
        "finished_epoch_us",
        "batch_id",
        "statement_index",
        "transaction_context_id",
        "source_file_name",
    )

    def record_tabularis_audit_log(self, events: list[dict[str, Any]]) -> int:
        """把本地执行日志写进可查询副本（幂等，按 event_id 覆盖）。"""

        if not events:
            return 0
        columns = self.TABULARIS_AUDIT_LOG_COLUMNS
        integer_columns = {
            "event_epoch_us",
            "execution_time_ms",
            "affected_rows",
            "error_code",
            "started_epoch_us",
            "finished_epoch_us",
            "statement_index",
        }
        rows = []
        for event in events:
            values = []
            for column in columns:
                value = event.get(column)
                if column in integer_columns:
                    values.append(int(value or 0))
                else:
                    values.append(str(value or ""))
            rows.append(tuple(values))
        placeholders = ",".join("?" for _ in columns)
        with self._write_lock, self.connection() as conn:
            conn.executemany(
                f"INSERT OR REPLACE INTO tabularis_audit_log("
                f"{','.join(columns)}) VALUES({placeholders})",
                rows,
            )
        return len(rows)

    def query_tabularis_audit_log(self, query: dict[str, Any]) -> dict[str, Any]:
        """本地执行日志查询：只走带索引的表，不碰 Parquet。"""

        limit = min(max(int(query.get("limit") or 100), 1), 1000)
        offset = min(max(int(query.get("offset") or 0), 0), 100_000)
        clauses: list[str] = []
        params: list[Any] = []
        if query.get("start_epoch_us"):
            clauses.append("event_epoch_us >= ?")
            params.append(int(query["start_epoch_us"]))
        if query.get("end_epoch_us"):
            clauses.append("event_epoch_us <= ?")
            params.append(int(query["end_epoch_us"]))
        for key, columns in (
            ("instance", ("instance_id",)),
            ("connection", ("connection_id", "connection_name")),
            ("account", ("database_account",)),
            ("database", ("database_name",)),
            ("table", ("table_name",)),
            ("status", ("execution_status",)),
        ):
            value = str(query.get(key) or "").strip()
            if not value:
                continue
            if key in {"instance", "status"}:
                clauses.append(
                    "(" + " OR ".join(f"lower({c}) = ?" for c in columns) + ")"
                )
                params.extend([value.lower()] * len(columns))
            else:
                clauses.append(
                    "(" + " OR ".join(f"lower({c}) LIKE ?" for c in columns) + ")"
                )
                params.extend(["%" + value.lower() + "%"] * len(columns))
        operations = [
            str(value).strip().upper()
            for value in (query.get("operations") or [])
            if str(value).strip()
        ]
        if operations:
            clauses.append(
                "operation IN (" + ",".join("?" for _ in operations) + ")"
            )
            params.extend(operations)
        terms = [value for value in str(query.get("keyword") or "").split() if value][:20]
        if terms:
            joiner = (
                " OR "
                if str(query.get("keyword_mode") or "").upper() == "OR"
                else " AND "
            )
            term_clauses = []
            for term in terms:
                term_clauses.append(
                    "("
                    + " OR ".join(
                        f"lower({column}) LIKE ?"
                        for column in (
                            "sql_text",
                            "connection_name",
                            "database_account",
                            "error_message",
                            "transaction_context_id",
                            "source_file_name",
                        )
                    )
                    + ")"
                )
                params.extend(["%" + term.lower() + "%"] * 6)
            clauses.append("(" + joiner.join(term_clauses) + ")")
        where = " AND ".join(clauses) if clauses else "1 = 1"
        columns = ",".join(self.TABULARIS_AUDIT_LOG_COLUMNS)
        with self.connection() as conn:
            rows = conn.execute(
                f"SELECT {columns} FROM tabularis_audit_log WHERE {where} "
                "ORDER BY event_epoch_us DESC, event_id DESC LIMIT ? OFFSET ?",
                (*params, limit + 1, offset),
            ).fetchall()
        has_more = len(rows) > limit
        # 这张表只存审计事件自己的列，不存 raw_event_type / event_time_utc。前端
        # 按 raw_event_type == TABULARIS_AUDIT 判断该不该显示「本地执行日志」与
        # 连接/账号，取不到就退回 binlog 分支，来源列渲染成 "undefined →
        # undefined"（start_position/end_position 在审计事件里根本不存在）。这里
        # 补齐两个派生字段，时间格式与 Parquet 通道的 strftime 输出保持一致。
        return {
            "rows": [self._audit_log_row(row) for row in rows[:limit]],
            "has_more": has_more,
            "limit": limit,
            "offset": offset,
        }

    @staticmethod
    def _audit_log_row(row: Any) -> dict[str, Any]:
        item = dict(row)
        item["raw_event_type"] = "TABULARIS_AUDIT"
        epoch_us = int(item.get("event_epoch_us") or 0)
        item["event_time_utc"] = (
            datetime.fromtimestamp(epoch_us / 1_000_000, UTC).strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            )
            + "Z"
        )
        return item

    def tabularis_audit_log_count(self) -> int:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT count(*) AS n FROM tabularis_audit_log"
            ).fetchone()
        return int(row["n"]) if row else 0

    def upsert_remote(
        self, settings: Settings, item: RemoteBinlog
    ) -> tuple[str, str]:
        file_id = self.file_id(settings.db_instance_id, item)
        now = utc_now_text()
        with self._write_lock, self.connection() as conn:
            existing = conn.execute(
                "SELECT state, download_link, intranet_download_link, "
                "link_expired_utc, remote_status FROM binlog_files WHERE id = ?",
                (file_id,),
            ).fetchone()
            if existing:
                state = str(existing["state"])
                desired = (
                    "" if state == "done" else item.download_link,
                    "" if state == "done" else item.intranet_download_link,
                    "" if state == "done" else item.link_expired_utc,
                    item.remote_status,
                )
                current = (
                    str(existing["download_link"]),
                    str(existing["intranet_download_link"]),
                    str(existing["link_expired_utc"]),
                    str(existing["remote_status"]),
                )
                if current != desired:
                    conn.execute(
                        """
                        UPDATE binlog_files
                        SET download_link = ?, intranet_download_link = ?,
                            link_expired_utc = ?, remote_status = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (*desired, now, file_id),
                    )
                return file_id, state
            conn.execute(
                """
                INSERT INTO binlog_files(
                    id, project_id, instance_id, log_file_name,
                    log_begin_utc, log_end_utc, file_size, checksum_crc64,
                    download_link, intranet_download_link, link_expired_utc,
                    remote_status, host_instance_id, state, discovered_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'discovered', ?, ?)
                """,
                (
                    file_id,
                    "",
                    settings.db_instance_id,
                    item.log_file_name,
                    item.log_begin_utc,
                    item.log_end_utc,
                    item.file_size,
                    item.checksum_crc64,
                    item.download_link,
                    item.intranet_download_link,
                    item.link_expired_utc,
                    item.remote_status,
                    item.host_instance_id,
                    now,
                    now,
                ),
            )
        return file_id, "discovered"

    def upsert_remotes(
        self,
        settings: Settings,
        items: list[RemoteBinlog],
    ) -> list[tuple[str, str]]:
        """Refresh one RDS discovery window without per-file transactions.

        A full retention scan can contain tens of thousands of already-done
        files.  Those immutable rows must remain read-only; changing only
        ``updated_at`` would create one SQLite commit per file on every poll.
        Existing rows are read once, and only genuinely new or changed remote
        locators are committed in one bounded transaction.
        """

        if not items:
            return []
        prepared = [
            (self.file_id(settings.db_instance_id, item), item)
            for item in items
        ]
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, state, download_link, intranet_download_link,
                       link_expired_utc, remote_status
                FROM binlog_files
                WHERE instance_id = ?
                """,
                (settings.db_instance_id,),
            ).fetchall()
        existing = {str(row["id"]): row for row in rows}
        now = utc_now_text()
        updates: dict[str, tuple[Any, ...]] = {}
        inserts: dict[str, tuple[Any, ...]] = {}
        results: list[tuple[str, str]] = []
        for file_id, item in prepared:
            row = existing.get(file_id)
            if row is None:
                results.append((file_id, "discovered"))
                inserts[file_id] = (
                    file_id,
                    "",
                    settings.db_instance_id,
                    item.log_file_name,
                    item.log_begin_utc,
                    item.log_end_utc,
                    item.file_size,
                    item.checksum_crc64,
                    item.download_link,
                    item.intranet_download_link,
                    item.link_expired_utc,
                    item.remote_status,
                    item.host_instance_id,
                    now,
                    now,
                )
                continue
            state = str(row["state"])
            results.append((file_id, state))
            desired = (
                "" if state == "done" else item.download_link,
                "" if state == "done" else item.intranet_download_link,
                "" if state == "done" else item.link_expired_utc,
                item.remote_status,
            )
            current = (
                str(row["download_link"]),
                str(row["intranet_download_link"]),
                str(row["link_expired_utc"]),
                str(row["remote_status"]),
            )
            if current != desired:
                updates[file_id] = (*desired, now, file_id)
        if not updates and not inserts:
            return results
        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.executemany(
                    """
                    UPDATE binlog_files
                    SET download_link = CASE WHEN state = 'done' THEN '' ELSE ? END,
                        intranet_download_link = CASE WHEN state = 'done' THEN '' ELSE ? END,
                        link_expired_utc = CASE WHEN state = 'done' THEN '' ELSE ? END,
                        remote_status = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    updates.values(),
                )
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO binlog_files(
                        id, project_id, instance_id, log_file_name,
                        log_begin_utc, log_end_utc, file_size, checksum_crc64,
                        download_link, intranet_download_link, link_expired_utc,
                        remote_status, host_instance_id, state, discovered_at,
                        updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                             'discovered', ?, ?)
                    """,
                    inserts.values(),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return results

    def file_record(self, file_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM binlog_files WHERE id = ?", (file_id,)
            ).fetchone()
        return dict(row) if row else None

    def recoverable_files(self, instance_id: str) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT *
                FROM binlog_files
                WHERE instance_id = ?
                  AND state IN ('failed', 'downloading', 'downloaded', 'parsing', 'stored')
                ORDER BY log_begin_utc, log_end_utc, log_file_name, host_instance_id
                """,
                (instance_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def pending_sync_file_count(self, instance_id: str = "") -> int:
        """待同步文件数。instance_id 为空表示统计所有实例。"""
        with self.connection() as conn:
            if instance_id:
                row = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM binlog_files
                    WHERE instance_id = ?
                      AND state NOT IN ('done', 'unavailable')
                    """,
                    (str(instance_id),),
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM binlog_files
                    WHERE state NOT IN ('done', 'unavailable')
                    """
                ).fetchone()
        return int(row[0] or 0) if row else 0

    def latest_log_end(self, instance_id: str) -> str:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT MAX(log_end_utc) AS latest
                FROM binlog_files
                WHERE instance_id = ? AND state = 'done'
                """,
                (instance_id,),
            ).fetchone()
        return str(row["latest"] or "") if row else ""

    def parts_for_file(self, file_id: str) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM parquet_parts WHERE binlog_id = ? ORDER BY path",
                (file_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def set_file_state(
        self,
        file_id: str,
        state: str,
        *,
        downloaded_bytes: int | None = None,
        local_sha256: str | None = None,
        event_count: int | None = None,
        error_code: str = "",
        error_message: str = "",
        increment_attempt: bool = False,
        raw_deleted: bool = False,
        query_visible: bool | None = None,
    ) -> None:
        now_text = utc_now_text()
        updates = [
            "state = ?",
            "updated_at = ?",
            "error_code = ?",
            "error_message = ?",
        ]
        values: list[Any] = [state, now_text, error_code, error_message]
        if downloaded_bytes is not None:
            updates.append("downloaded_bytes = ?")
            values.append(int(downloaded_bytes))
        if local_sha256 is not None:
            updates.append("local_sha256 = ?")
            values.append(local_sha256)
        if event_count is not None:
            updates.append("event_count = ?")
            values.append(int(event_count))
        if increment_attempt:
            updates.append("attempts = attempts + 1")
        if state == "downloading":
            updates.extend(
                [
                    "processing_started_at = ?",
                    "processing_seconds = 0",
                ]
            )
            values.append(now_text)
        if state == "done":
            updates.append("query_visible = 1")
            updates.append("completed_at = ?")
            values.append(now_text)
            updates.append(
                "processing_seconds = CASE "
                "WHEN processing_started_at <> '' THEN "
                "MAX(0, (julianday(?) - julianday(processing_started_at)) * 86400.0) "
                "ELSE 0 END"
            )
            values.append(now_text)
            # OSS download links are short-lived signed capabilities. They are
            # no longer needed after verified storage and must not linger.
            updates.extend(
                [
                    "download_link = ''",
                    "intranet_download_link = ''",
                    "link_expired_utc = ''",
                ]
            )
        elif query_visible is not None:
            updates.append("query_visible = ?")
            values.append(1 if query_visible else 0)
        if raw_deleted:
            updates.append("raw_deleted_at = ?")
            values.append(now_text)
        values.append(file_id)
        with self._write_lock, self.connection() as conn:
            conn.execute(
                f"UPDATE binlog_files SET {', '.join(updates)} WHERE id = ?", values
            )

    def set_file_visibility(self, file_id: str, visible: bool) -> None:
        with self._write_lock, self.connection() as conn:
            cursor = conn.execute(
                "UPDATE binlog_files "
                "SET query_visible = ?, updated_at = ? WHERE id = ?",
                (1 if visible else 0, utc_now_text(), file_id),
            )
        if cursor.rowcount != 1:
            raise RuntimeError("待切换可见性的 Binlog 元数据不存在")

    def update_download_progress(self, file_id: str, byte_count: int) -> None:
        with self._write_lock, self.connection() as conn:
            conn.execute(
                "UPDATE binlog_files SET downloaded_bytes = ?, updated_at = ? "
                "WHERE id = ?",
                (int(byte_count), utc_now_text(), file_id),
            )

    def replace_parts(
        self,
        file_id: str,
        parts: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        now = utc_now_text()
        committed_parts: dict[str, dict[str, Any]] = {}
        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute("DELETE FROM parquet_parts WHERE binlog_id = ?", (file_id,))
                content_revision = (
                    self._next_content_revision(conn) if parts else 0
                )
                conn.executemany(
                    """
                    INSERT INTO parquet_parts(
                        path, binlog_id, logical_part_id, event_date, row_count,
                        min_event_epoch_us, max_event_epoch_us,
                        size_bytes, sha256, object_sha256, compression_level,
                        compression_updated_at, local_last_access_at,
                        content_revision, created_at, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            part["path"],
                            file_id,
                            str(
                                part.get("logical_part_id")
                                or logical_part_id(
                                    str(part["path"]),
                                    str(part.get("object_sha256") or part["sha256"]),
                                )
                            ),
                            part["event_date"],
                            part["row_count"],
                            part["min_event_epoch_us"],
                            part["max_event_epoch_us"],
                            part["size_bytes"],
                            part["sha256"],
                            str(part.get("object_sha256") or part["sha256"]),
                            int(part.get("compression_level") or 1),
                            str(part.get("compression_updated_at") or ""),
                            now,
                            content_revision,
                            now,
                            now,
                        )
                        for part in parts
                    ],
                )
                self._upsert_part_catalogs(conn, parts, now)
                committed_parts = self._committed_part_rows(conn, parts)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        self._sync_embedded_catalogs(parts, committed_parts, now)
        return committed_parts

    def upsert_parts(
        self,
        file_id: str,
        parts: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        if not parts:
            return {}
        now = utc_now_text()
        committed_parts: dict[str, dict[str, Any]] = {}
        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                existing_parts: dict[str, sqlite3.Row] = {}
                changed_paths: set[str] = set()
                for part in parts:
                    existing = conn.execute(
                        "SELECT sha256, row_count, min_event_epoch_us, "
                        "max_event_epoch_us, size_bytes, content_revision "
                        "FROM parquet_parts WHERE path = ?",
                        (part["path"],),
                    ).fetchone()
                    if existing:
                        existing_parts[str(part["path"])] = existing
                    changed = existing is None or any(
                        int(existing[column]) != int(part[column])
                        for column in (
                            "row_count",
                            "min_event_epoch_us",
                            "max_event_epoch_us",
                            "size_bytes",
                        )
                    ) or str(existing["sha256"]) != str(part["sha256"])
                    if changed:
                        changed_paths.add(str(part["path"]))
                    if existing and str(existing["sha256"]) != str(part["sha256"]):
                        conn.execute(
                            "DELETE FROM parquet_part_catalog WHERE path = ?",
                            (part["path"],),
                        )
                        conn.execute(
                            "DELETE FROM parquet_negative_probes WHERE path = ?",
                            (part["path"],),
                        )
                        conn.execute(
                            "DELETE FROM parquet_positive_probes WHERE path = ?",
                            (part["path"],),
                        )
                next_revision = (
                    self._next_content_revision(conn) if changed_paths else 0
                )
                conn.executemany(
                    """
                    INSERT INTO parquet_parts(
                        path, binlog_id, logical_part_id, event_date, row_count,
                        min_event_epoch_us, max_event_epoch_us,
                        size_bytes, sha256, object_sha256, compression_level,
                        compression_updated_at, local_last_access_at,
                        content_revision, created_at, updated_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(path) DO UPDATE SET
                        binlog_id = excluded.binlog_id,
                        logical_part_id = excluded.logical_part_id,
                        event_date = excluded.event_date,
                        row_count = excluded.row_count,
                        min_event_epoch_us = excluded.min_event_epoch_us,
                        max_event_epoch_us = excluded.max_event_epoch_us,
                        size_bytes = excluded.size_bytes,
                        oss_key = CASE
                            WHEN parquet_parts.sha256 = excluded.sha256
                            THEN parquet_parts.oss_key ELSE '' END,
                        oss_etag = CASE
                            WHEN parquet_parts.sha256 = excluded.sha256
                            THEN parquet_parts.oss_etag ELSE '' END,
                        oss_offset = CASE
                            WHEN parquet_parts.sha256 = excluded.sha256
                            THEN parquet_parts.oss_offset ELSE 0 END,
                        oss_length = CASE
                            WHEN parquet_parts.sha256 = excluded.sha256
                            THEN parquet_parts.oss_length ELSE 0 END,
                        oss_object_sha256 = CASE
                            WHEN parquet_parts.sha256 = excluded.sha256
                            THEN parquet_parts.oss_object_sha256 ELSE '' END,
                        oss_uploaded_at = CASE
                            WHEN parquet_parts.sha256 = excluded.sha256
                            THEN parquet_parts.oss_uploaded_at ELSE '' END,
                        oss_verified_at = CASE
                            WHEN parquet_parts.sha256 = excluded.sha256
                            THEN parquet_parts.oss_verified_at ELSE '' END,
                        sha256 = excluded.sha256,
                        object_sha256 = excluded.object_sha256,
                        compression_level = CASE
                            WHEN parquet_parts.sha256 = excluded.sha256
                            THEN parquet_parts.compression_level
                            ELSE excluded.compression_level END,
                        compression_updated_at = CASE
                            WHEN parquet_parts.sha256 = excluded.sha256
                            THEN parquet_parts.compression_updated_at
                            ELSE excluded.compression_updated_at END,
                        compression_error = CASE
                            WHEN parquet_parts.sha256 = excluded.sha256
                            THEN parquet_parts.compression_error ELSE '' END,
                        compression_retry_after = CASE
                            WHEN parquet_parts.sha256 = excluded.sha256
                            THEN parquet_parts.compression_retry_after ELSE '' END,
                        local_last_access_at = excluded.local_last_access_at,
                        content_revision = excluded.content_revision,
                        updated_at = excluded.updated_at
                    """,
                    [
                        (
                            part["path"],
                            file_id,
                            str(
                                part.get("logical_part_id")
                                or logical_part_id(
                                    str(part["path"]),
                                    str(part.get("object_sha256") or part["sha256"]),
                                )
                            ),
                            part["event_date"],
                            part["row_count"],
                            part["min_event_epoch_us"],
                            part["max_event_epoch_us"],
                            part["size_bytes"],
                            part["sha256"],
                            str(part.get("object_sha256") or part["sha256"]),
                            int(part.get("compression_level") or 1),
                            str(part.get("compression_updated_at") or ""),
                            now,
                            (
                                next_revision
                                if str(part["path"]) in changed_paths
                                else int(
                                    existing_parts[str(part["path"])][
                                        "content_revision"
                                    ]
                                )
                            ),
                            now,
                            now,
                        )
                        for part in parts
                    ],
                )
                self._upsert_part_catalogs(conn, parts, now)
                committed_parts = self._committed_part_rows(conn, parts)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        self._sync_embedded_catalogs(parts, committed_parts, now)
        return committed_parts

    @staticmethod
    def _committed_part_rows(
        conn: sqlite3.Connection,
        parts: list[dict[str, Any]],
    ) -> dict[str, dict[str, Any]]:
        paths = list(dict.fromkeys(str(part["path"]) for part in parts))
        committed: dict[str, dict[str, Any]] = {}
        for offset in range(0, len(paths), 400):
            chunk = paths[offset : offset + 400]
            placeholders = ",".join("?" for _ in chunk)
            rows = conn.execute(
                "SELECT * FROM parquet_parts "
                f"WHERE path IN ({placeholders})",
                chunk,
            ).fetchall()
            committed.update({str(row["path"]): dict(row) for row in rows})
        return committed

    @staticmethod
    def _upsert_part_catalogs(
        conn: sqlite3.Connection,
        parts: list[dict[str, Any]],
        indexed_at: str,
    ) -> None:
        rows = []
        for part in parts:
            catalog = part.get("catalog")
            if not isinstance(catalog, dict):
                continue
            rows.append(
                (
                    str(part["path"]),
                    str(part["sha256"]),
                    json_dumps(sorted(set(catalog.get("databases") or []))),
                    json_dumps(sorted(set(catalog.get("tables") or []))),
                    json_dumps(sorted(set(catalog.get("operations") or []))),
                    indexed_at,
                )
            )
        if not rows:
            return
        conn.executemany(
            """
            INSERT INTO parquet_part_catalog(
                path, sha256, databases_json, tables_json,
                operations_json, indexed_at
            ) VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                sha256 = excluded.sha256,
                databases_json = excluded.databases_json,
                tables_json = excluded.tables_json,
                operations_json = excluded.operations_json,
                indexed_at = excluded.indexed_at
            """,
            rows,
        )

    def _sync_embedded_catalogs(
        self,
        parts: list[dict[str, Any]],
        current_parts: dict[str, dict[str, Any]],
        indexed_at: str,
    ) -> None:
        candidates = {
            str(part["path"]): part
            for part in parts
            if isinstance(part.get("catalog"), dict)
        }
        if not candidates:
            return
        entries = []
        for path, part in candidates.items():
            current = current_parts.get(path)
            if not current or str(current["sha256"]) != str(part["sha256"]):
                continue
            entries.append(
                {
                    "path": path,
                    "sha256": str(part["sha256"]),
                    "content_revision": int(current["content_revision"]),
                    "catalog": dict(part["catalog"]),
                    "indexed_at": indexed_at,
                }
            )
        if not entries:
            return
        try:
            self.catalog_store.upsert_many(entries)
        except Exception:
            # The legacy row was committed in the same metadata transaction and
            # remains the correctness fallback. The next bounded backfill or
            # read-through retries the derived compressed copy.
            LOGGER.exception("Failed to mirror embedded part catalogs")

    def part_catalogs(self, paths: list[str]) -> dict[str, dict[str, Any]]:
        if not paths:
            return {}
        current_parts: dict[str, tuple[str, int]] = {}
        with self.connection() as conn:
            for offset in range(0, len(paths), 400):
                chunk = paths[offset : offset + 400]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    "SELECT path, sha256, content_revision FROM parquet_parts "
                    f"WHERE path IN ({placeholders})",
                    chunk,
                ).fetchall()
                current_parts.update(
                    {
                        str(row["path"]): (
                            str(row["sha256"]),
                            int(row["content_revision"]),
                        )
                        for row in rows
                    }
                )

        result: dict[str, dict[str, Any]] = {}
        compressed = self.catalog_store.catalogs(paths)
        for path, catalog in compressed.items():
            current = current_parts.get(path)
            if not current or str(catalog["sha256"]) != current[0]:
                continue
            result[path] = {
                "sha256": str(catalog["sha256"]),
                "databases": list(catalog["databases"]),
                "tables": list(catalog["tables"]),
                "operations": list(catalog["operations"]),
                "indexed_at": str(catalog["indexed_at"]),
            }

        fallback_paths = [path for path in paths if path not in result]
        backfill_entries: list[dict[str, Any]] = []
        with self.connection() as conn:
            for offset in range(0, len(fallback_paths), 400):
                chunk = fallback_paths[offset : offset + 400]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    "SELECT * FROM parquet_part_catalog "
                    f"WHERE path IN ({placeholders})",
                    chunk,
                ).fetchall()
                for row in rows:
                    path = str(row["path"])
                    current = current_parts.get(path)
                    if not current or str(row["sha256"]) != current[0]:
                        continue
                    catalog = {
                        "sha256": str(row["sha256"]),
                        "databases": json.loads(str(row["databases_json"])),
                        "tables": json.loads(str(row["tables_json"])),
                        "operations": json.loads(str(row["operations_json"])),
                        "indexed_at": str(row["indexed_at"]),
                    }
                    result[path] = catalog
                    backfill_entries.append(
                        {
                            "path": path,
                            "sha256": catalog["sha256"],
                            "content_revision": current[1],
                            "catalog": catalog,
                            "indexed_at": catalog["indexed_at"],
                        }
                    )
        if backfill_entries:
            try:
                self.catalog_store.upsert_many(backfill_entries)
            except Exception:
                LOGGER.exception("Failed to backfill compressed part catalogs")
        return result

    def upsert_part_catalog(
        self,
        path: str,
        sha256: str,
        catalog: dict[str, Any],
    ) -> bool:
        now = utc_now_text()
        with self._write_lock:
            with self.connection() as conn:
                current = conn.execute(
                    "SELECT sha256, content_revision FROM parquet_parts "
                    "WHERE path = ?",
                    (path,),
                ).fetchone()
            if not current or str(current["sha256"]) != str(sha256):
                return False
            content_revision = int(current["content_revision"])
            self.catalog_store.upsert_many(
                [
                    {
                        "path": path,
                        "sha256": sha256,
                        "content_revision": content_revision,
                        "catalog": catalog,
                        "indexed_at": now,
                    }
                ]
            )
            with self.connection() as conn:
                current = conn.execute(
                    "SELECT sha256, content_revision FROM parquet_parts "
                    "WHERE path = ?",
                    (path,),
                ).fetchone()
                if (
                    not current
                    or str(current["sha256"]) != str(sha256)
                    or int(current["content_revision"]) != content_revision
                ):
                    return False
                self._upsert_part_catalogs(
                    conn,
                    [{"path": path, "sha256": sha256, "catalog": catalog}],
                    now,
                )
        return True

    def backfill_catalog_store(self, limit: int = 256) -> dict[str, int | bool]:
        target = max(int(limit), 0)
        state = self.catalog_store.backfill_state()
        if not target or bool(state["complete"]):
            return {
                "scanned": 0,
                "stored": 0,
                "complete": bool(state["complete"]),
            }
        after_path = str(state["after_path"] or "")
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT p.path, p.sha256, p.content_revision,
                       c.sha256 AS catalog_sha256,
                       c.databases_json, c.tables_json, c.operations_json,
                       c.indexed_at
                FROM parquet_parts p
                LEFT JOIN parquet_part_catalog c ON c.path = p.path
                WHERE p.path > ?
                ORDER BY p.path
                LIMIT ?
                """,
                (after_path, target),
            ).fetchall()
        entries = [
            {
                "path": str(row["path"]),
                "sha256": str(row["sha256"]),
                "content_revision": int(row["content_revision"]),
                "catalog": {
                    "databases": json.loads(str(row["databases_json"])),
                    "tables": json.loads(str(row["tables_json"])),
                    "operations": json.loads(str(row["operations_json"])),
                },
                "indexed_at": str(row["indexed_at"]),
            }
            for row in rows
            if row["catalog_sha256"] is not None
            and str(row["catalog_sha256"]) == str(row["sha256"])
        ]
        complete = len(rows) < target
        next_path = str(rows[-1]["path"]) if rows else after_path
        self.catalog_store.apply_backfill_page(
            entries,
            after_path=next_path,
            complete=complete,
            updated_at=utc_now_text(),
        )
        return {
            "scanned": len(rows),
            "stored": len(entries),
            "complete": complete,
        }

    def catalog_store_stats(self) -> dict[str, int | bool]:
        return self.catalog_store.stats()

    def part_catalog_stats(self) -> dict[str, int]:
        with self.connection() as conn:
            state = conn.execute(
                "SELECT complete FROM parquet_catalog_reconcile_state "
                "WHERE singleton = 1"
            ).fetchone()
            reconcile_complete = bool(state and int(state["complete"] or 0))
            if reconcile_complete:
                row = conn.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM parquet_parts) AS total_parts,
                        (SELECT COUNT(*) FROM parquet_catalog_pending)
                            AS pending_catalogs
                    """
                ).fetchone()
            else:
                row = conn.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM parquet_parts) AS total_parts,
                        (SELECT COUNT(*) FROM parquet_part_catalog)
                            AS stored_catalogs,
                        (SELECT COUNT(*) FROM parquet_catalog_pending)
                            AS pending_catalogs
                    """
                ).fetchone()
        total_parts = int(row["total_parts"] or 0)
        if reconcile_complete:
            cataloged_parts = max(
                total_parts - int(row["pending_catalogs"] or 0),
                0,
            )
        else:
            # During a legacy backfill, separate table counts stay fast and are
            # at least as accurate as the pre-migration UI value. Once the
            # bounded reconciliation completes, pending rows also account for
            # same-path stale SHA entries exactly.
            cataloged_parts = min(int(row["stored_catalogs"] or 0), total_parts)
        return {
            "total_parts": total_parts,
            "cataloged_parts": cataloged_parts,
        }

    def part_catalog_pending(self, path: str) -> bool:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM parquet_catalog_pending WHERE path = ?",
                (path,),
            ).fetchone()
        return row is not None

    def missing_part_catalogs(self, limit: int = 32) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT p.*, b.log_file_name, b.log_begin_utc, b.log_end_utc
                FROM parquet_catalog_pending q
                JOIN parquet_parts p ON p.path = q.path
                JOIN binlog_files b ON b.id = p.binlog_id
                WHERE q.content_revision = p.content_revision
                ORDER BY q.max_event_epoch_us DESC,
                         q.min_event_epoch_us DESC,
                         q.path DESC
                LIMIT ?
                """,
                (max(int(limit), 0),),
            ).fetchall()
        return [dict(row) for row in rows]

    def reconcile_part_catalog_pending(
        self,
        limit: int = 2048,
    ) -> dict[str, int | bool]:
        """Audit one bounded legacy page without putting a scan on startup."""

        target = max(int(limit), 0)
        if not target:
            return {"scanned": 0, "queued": 0, "complete": False}
        with self.connection() as conn:
            state = conn.execute(
                "SELECT after_path, complete "
                "FROM parquet_catalog_reconcile_state WHERE singleton = 1"
            ).fetchone()
            if not state:
                raise RuntimeError("Parquet catalog reconciliation state is missing")
            if int(state["complete"] or 0):
                return {"scanned": 0, "queued": 0, "complete": True}
            after_path = str(state["after_path"] or "")
            rows = conn.execute(
                """
                SELECT p.path,
                       CASE WHEN c.path IS NULL OR c.sha256 <> p.sha256
                            THEN 1 ELSE 0 END AS needs_catalog
                FROM parquet_parts p
                LEFT JOIN parquet_part_catalog c ON c.path = p.path
                WHERE p.path > ?
                ORDER BY p.path
                LIMIT ?
                """,
                (after_path, target),
            ).fetchall()

        candidates = [
            str(row["path"])
            for row in rows
            if int(row["needs_catalog"] or 0)
        ]
        next_path = str(rows[-1]["path"]) if rows else after_path
        complete = len(rows) < target
        now = utc_now_text()
        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                current = conn.execute(
                    "SELECT after_path, complete "
                    "FROM parquet_catalog_reconcile_state WHERE singleton = 1"
                ).fetchone()
                if not current:
                    raise RuntimeError(
                        "Parquet catalog reconciliation state is missing"
                    )
                if int(current["complete"] or 0):
                    conn.execute("COMMIT")
                    return {"scanned": 0, "queued": 0, "complete": True}
                if str(current["after_path"] or "") != after_path:
                    conn.execute("COMMIT")
                    return {"scanned": 0, "queued": 0, "complete": False}
                if candidates:
                    conn.executemany(
                        """
                        INSERT INTO parquet_catalog_pending(
                            path, content_revision, max_event_epoch_us,
                            min_event_epoch_us, enqueued_at
                        )
                        SELECT p.path, p.content_revision, p.max_event_epoch_us,
                               p.min_event_epoch_us, ?
                        FROM parquet_parts p
                        WHERE p.path = ?
                          AND NOT EXISTS(
                              SELECT 1 FROM parquet_part_catalog c
                              WHERE c.path = p.path AND c.sha256 = p.sha256
                          )
                        ON CONFLICT(path) DO UPDATE SET
                            content_revision = excluded.content_revision,
                            max_event_epoch_us = excluded.max_event_epoch_us,
                            min_event_epoch_us = excluded.min_event_epoch_us,
                            enqueued_at = excluded.enqueued_at
                        """,
                        [(now, path) for path in candidates],
                    )
                conn.execute(
                    "UPDATE parquet_catalog_reconcile_state "
                    "SET after_path = ?, complete = ?, updated_at = ? "
                    "WHERE singleton = 1",
                    (next_path, 1 if complete else 0, now),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return {
            "scanned": len(rows),
            "queued": len(candidates),
            "complete": complete,
        }

    def clickhouse_change_tracking_state(self) -> dict[str, bool]:
        """Return the constant-time source-side serving gate.

        Every query-visible non-audit part mutation is committed with one
        durable pending row by SQLite triggers. The ClickHouse reconciler
        removes that row only after its own manifest transaction commits, so
        an empty queue is a safe cross-database hand-off point.
        """

        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT complete,
                       EXISTS(
                           SELECT 1 FROM parquet_clickhouse_pending LIMIT 1
                       ) AS pending
                FROM parquet_clickhouse_change_state
                WHERE singleton = 1
                """
            ).fetchone()
        return {
            "complete": bool(row and row["complete"]),
            "pending": bool(row and row["pending"]),
        }

    def clickhouse_source_parts_page(
        self,
        *,
        after_path: str = "",
        limit: int = 2048,
    ) -> list[dict[str, Any]]:
        """Return one stable page for the explicit raw-OSS manifest load."""

        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT p.*, b.instance_id, b.log_file_name,
                       1 AS exists_now, b.query_visible
                FROM parquet_parts p
                JOIN binlog_files b ON b.id = p.binlog_id
                WHERE b.query_visible = 1
                  AND b.log_file_name NOT LIKE ?
                  AND p.path > ?
                ORDER BY p.path
                LIMIT ?
                """,
                (
                    TABULARIS_AUDIT_FILE_PREFIX + "%",
                    str(after_path or ""),
                    max(int(limit), 0),
                ),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["exists"] = bool(item.pop("exists_now"))
            item["query_visible"] = bool(item["query_visible"])
            item["change_version"] = 0
            result.append(item)
        return result

    def clickhouse_ranged_source_parts_page(
        self,
        *,
        after_path: str = "",
        limit: int = 2048,
    ) -> list[dict[str, Any]]:
        """Return archived custom-pack members in a bounded stable page."""

        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT p.*, b.instance_id, b.log_file_name
                FROM parquet_parts p
                JOIN binlog_files b ON b.id = p.binlog_id
                WHERE b.query_visible = 1
                  AND b.log_file_name NOT LIKE ?
                  AND p.oss_length > 0
                  AND p.path > ?
                ORDER BY p.path
                LIMIT ?
                """,
                (
                    TABULARIS_AUDIT_FILE_PREFIX + "%",
                    str(after_path or ""),
                    max(int(limit), 0),
                ),
            ).fetchall()
        return [dict(row) for row in rows]

    def clickhouse_source_stats(self) -> dict[str, int]:
        """Explicit full-scan audit for a serving cut, never a request path."""

        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS source_parts,
                       COALESCE(SUM(CASE WHEN p.oss_key <> '' THEN 1 ELSE 0 END), 0)
                           AS archived_parts,
                       COALESCE(SUM(CASE WHEN p.oss_length > 0 THEN 1 ELSE 0 END), 0)
                           AS ranged_parts,
                       COALESCE(SUM(p.row_count), 0) AS source_rows,
                       COALESCE(SUM(p.size_bytes), 0) AS source_bytes,
                       COALESCE(MIN(p.min_event_epoch_us), 0) AS min_event_epoch_us,
                       COALESCE(MAX(p.max_event_epoch_us), 0) AS max_event_epoch_us
                FROM parquet_parts p
                JOIN binlog_files b ON b.id = p.binlog_id
                WHERE b.query_visible = 1
                  AND b.log_file_name NOT LIKE ?
                """,
                (TABULARIS_AUDIT_FILE_PREFIX + "%",),
            ).fetchone()
        return {
            key: int(row[key] or 0)
            for key in (
                "source_parts",
                "archived_parts",
                "ranged_parts",
                "source_rows",
                "source_bytes",
                "min_event_epoch_us",
                "max_event_epoch_us",
            )
        }

    def pending_clickhouse_changes(
        self,
        *,
        limit: int = 2048,
    ) -> list[dict[str, Any]]:
        """Read one stable, coalesced source-change page without claiming it.

        Versioned acknowledgements are crash-safe: if a source part changes
        again while ClickHouse is reconciling, the old acknowledgement no
        longer matches and the newer pending row survives.
        """

        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT q.path AS change_path, q.change_version,
                       q.enqueued_at,
                       CASE WHEN p.path IS NULL THEN 0 ELSE 1 END AS exists_now,
                       p.*, b.instance_id, b.log_file_name,
                       COALESCE(b.query_visible, 0) AS query_visible
                FROM parquet_clickhouse_pending q
                LEFT JOIN parquet_parts p ON p.path = q.path
                LEFT JOIN binlog_files b ON b.id = p.binlog_id
                ORDER BY q.change_version, q.path
                LIMIT ?
                """,
                (max(int(limit), 0),),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["path"] = str(item.pop("change_path"))
            item["exists"] = bool(item.pop("exists_now"))
            result.append(item)
        return result

    def ack_clickhouse_changes(
        self,
        changes: list[tuple[str, int]],
    ) -> int:
        if not changes:
            return 0
        removed = 0
        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for path, change_version in changes:
                    removed += int(
                        conn.execute(
                            "DELETE FROM parquet_clickhouse_pending "
                            "WHERE path = ? AND change_version = ?",
                            (str(path), int(change_version)),
                        ).rowcount
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return removed

    def mark_clickhouse_change_tracking_complete(self) -> None:
        with self._write_lock, self.connection() as conn:
            conn.execute(
                """
                UPDATE parquet_clickhouse_change_state
                SET complete = 1,
                    updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
                WHERE singleton = 1
                """
            )

    def negative_probe_matches(
        self,
        entries: list[tuple[str, str, str]],
    ) -> set[str]:
        if not entries:
            return set()
        expected = set(entries)
        matches: set[str] = set()
        fingerprints = sorted({fingerprint for _, _, fingerprint in entries})
        with self.connection() as conn:
            for offset in range(0, len(fingerprints), 400):
                chunk = fingerprints[offset : offset + 400]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    "SELECT path, sha256, fingerprint "
                    "FROM parquet_negative_probes "
                    f"WHERE fingerprint IN ({placeholders})",
                    chunk,
                ).fetchall()
                for row in rows:
                    key = (
                        str(row["path"]),
                        str(row["sha256"]),
                        str(row["fingerprint"]),
                    )
                    if key in expected:
                        matches.add(key[0])
        return matches

    def positive_probe_matches(
        self,
        entries: list[tuple[str, str, str]],
    ) -> dict[str, list[dict[str, Any]]]:
        if not entries:
            return {}
        expected = set(entries)
        matches: dict[str, list[dict[str, Any]]] = {}
        fingerprints = sorted({fingerprint for _, _, fingerprint in entries})
        with self.connection() as conn:
            for offset in range(0, len(fingerprints), 400):
                chunk = fingerprints[offset : offset + 400]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    "SELECT path, sha256, fingerprint, rows_json "
                    "FROM parquet_positive_probes "
                    f"WHERE fingerprint IN ({placeholders})",
                    chunk,
                ).fetchall()
                for row in rows:
                    key = (
                        str(row["path"]),
                        str(row["sha256"]),
                        str(row["fingerprint"]),
                    )
                    if key not in expected:
                        continue
                    try:
                        cached = json.loads(str(row["rows_json"]))
                    except (TypeError, ValueError):
                        continue
                    if isinstance(cached, list) and all(
                        isinstance(value, dict) for value in cached
                    ):
                        matches[key[0]] = [dict(value) for value in cached]
        return matches

    def record_negative_probe(
        self,
        path: str,
        sha256: str,
        fingerprint: str,
    ) -> bool:
        return bool(
            self.record_negative_probes([(path, sha256, fingerprint)])
        )

    def record_negative_probes(
        self,
        entries: list[tuple[str, str, str]],
    ) -> int:
        desired = {
            (str(path), str(fingerprint)): str(sha256)
            for path, sha256, fingerprint in entries
            if str(path) and str(fingerprint)
        }
        if not desired:
            return 0
        now = utc_now_text()
        with self._write_lock, self.connection() as conn:
            current_sha256: dict[str, str] = {}
            paths = sorted({path for path, _ in desired})
            for offset in range(0, len(paths), 400):
                chunk = paths[offset : offset + 400]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    "SELECT path, sha256 FROM parquet_parts "
                    f"WHERE path IN ({placeholders})",
                    chunk,
                ).fetchall()
                current_sha256.update(
                    {
                        str(row["path"]): str(row["sha256"])
                        for row in rows
                    }
                )
            valid = [
                (path, sha256, fingerprint, now)
                for (path, fingerprint), sha256 in desired.items()
                if current_sha256.get(path) == sha256
            ]
            conn.executemany(
                "DELETE FROM parquet_positive_probes "
                "WHERE path = ? AND fingerprint = ?",
                [(path, fingerprint) for path, _, fingerprint, _ in valid],
            )
            conn.executemany(
                """
                INSERT INTO parquet_negative_probes(
                    path, sha256, fingerprint, probed_at
                ) VALUES(?, ?, ?, ?)
                ON CONFLICT(path, fingerprint) DO UPDATE SET
                    sha256 = excluded.sha256,
                    probed_at = excluded.probed_at
                """,
                valid,
            )
        return len(valid)

    def record_positive_probes(
        self,
        entries: list[tuple[str, str, str, list[dict[str, Any]]]],
        *,
        max_entry_bytes: int = 2 * 1024 * 1024,
        max_entries: int = 4096,
    ) -> int:
        desired: dict[tuple[str, str], tuple[str, str]] = {}
        for path, sha256, fingerprint, rows in entries:
            if not str(path) or not str(fingerprint) or not rows:
                continue
            encoded = json.dumps(
                rows,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            if len(encoded.encode("utf-8")) > max(int(max_entry_bytes), 0):
                continue
            desired[(str(path), str(fingerprint))] = (
                str(sha256),
                encoded,
            )
        if not desired:
            return 0
        now = utc_now_text()
        with self._write_lock, self.connection() as conn:
            current_sha256: dict[str, str] = {}
            paths = sorted({path for path, _ in desired})
            for offset in range(0, len(paths), 400):
                chunk = paths[offset : offset + 400]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    "SELECT path, sha256 FROM parquet_parts "
                    f"WHERE path IN ({placeholders})",
                    chunk,
                ).fetchall()
                current_sha256.update(
                    {
                        str(row["path"]): str(row["sha256"])
                        for row in rows
                    }
                )
            valid = [
                (path, sha256, fingerprint, rows_json, now)
                for (path, fingerprint), (sha256, rows_json) in desired.items()
                if current_sha256.get(path) == sha256
            ]
            conn.executemany(
                "DELETE FROM parquet_negative_probes "
                "WHERE path = ? AND fingerprint = ?",
                [(path, fingerprint) for path, _, fingerprint, _, _ in valid],
            )
            conn.executemany(
                """
                INSERT INTO parquet_positive_probes(
                    path, sha256, fingerprint, rows_json, probed_at
                ) VALUES(?, ?, ?, ?, ?)
                ON CONFLICT(path, fingerprint) DO UPDATE SET
                    sha256 = excluded.sha256,
                    rows_json = excluded.rows_json,
                    probed_at = excluded.probed_at
                """,
                valid,
            )
            conn.execute(
                """
                DELETE FROM parquet_positive_probes
                WHERE rowid IN (
                    SELECT rowid
                    FROM parquet_positive_probes
                    ORDER BY probed_at DESC, rowid DESC
                    LIMIT -1 OFFSET ?
                )
                """,
                (max(int(max_entries), 0),),
            )
        return len(valid)

    def complete_query_certificate(
        self,
        fingerprint: str,
        *,
        start_epoch_us: int,
        end_epoch_us: int,
    ) -> tuple[dict[str, int], list[dict[str, Any]] | None]:
        with self.connection() as conn:
            conn.execute("BEGIN")
            try:
                token = self._part_content_token(
                    conn,
                    start_epoch_us,
                    end_epoch_us,
                )
                row = conn.execute(
                    """
                    SELECT rows_json
                    FROM query_complete_certificates
                    WHERE fingerprint = ?
                      AND start_epoch_us = ?
                      AND end_epoch_us = ?
                      AND part_count = ?
                      AND max_content_revision = ?
                      AND content_revision_sum = ?
                    """,
                    (
                        str(fingerprint),
                        int(start_epoch_us),
                        int(end_epoch_us),
                        token["part_count"],
                        token["max_content_revision"],
                        token["content_revision_sum"],
                    ),
                ).fetchone()
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        if not row:
            return token, None
        try:
            cached = json.loads(str(row["rows_json"]))
        except (TypeError, ValueError):
            return token, None
        if not isinstance(cached, list) or not all(
            isinstance(value, dict) for value in cached
        ):
            return token, None
        return token, [dict(value) for value in cached]

    def record_complete_query_certificate(
        self,
        fingerprint: str,
        *,
        start_epoch_us: int,
        end_epoch_us: int,
        expected_token: dict[str, int],
        rows: list[dict[str, Any]],
        max_entry_bytes: int = 2 * 1024 * 1024,
        max_entries: int = 512,
        max_total_bytes: int = 128 * 1024 * 1024,
    ) -> bool:
        encoded = json.dumps(
            rows,
            ensure_ascii=False,
            separators=(",", ":"),
        )
        encoded_bytes = len(encoded.encode("utf-8"))
        if encoded_bytes > max(int(max_entry_bytes), 0):
            return False
        now = utc_now_text()
        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                current_token = self._part_content_token(
                    conn,
                    start_epoch_us,
                    end_epoch_us,
                )
                if current_token != {
                    "part_count": int(expected_token.get("part_count") or 0),
                    "max_content_revision": int(
                        expected_token.get("max_content_revision") or 0
                    ),
                    "content_revision_sum": int(
                        expected_token.get("content_revision_sum") or 0
                    ),
                }:
                    conn.execute("ROLLBACK")
                    return False
                conn.execute(
                    """
                    INSERT INTO query_complete_certificates(
                        fingerprint, start_epoch_us, end_epoch_us,
                        part_count, max_content_revision,
                        content_revision_sum, row_count, rows_json, created_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(fingerprint) DO UPDATE SET
                        start_epoch_us = excluded.start_epoch_us,
                        end_epoch_us = excluded.end_epoch_us,
                        part_count = excluded.part_count,
                        max_content_revision = excluded.max_content_revision,
                        content_revision_sum = excluded.content_revision_sum,
                        row_count = excluded.row_count,
                        rows_json = excluded.rows_json,
                        created_at = excluded.created_at
                    """,
                    (
                        str(fingerprint),
                        int(start_epoch_us),
                        int(end_epoch_us),
                        current_token["part_count"],
                        current_token["max_content_revision"],
                        current_token["content_revision_sum"],
                        len(rows),
                        encoded,
                        now,
                    ),
                )
                conn.execute(
                    """
                    DELETE FROM query_complete_certificates
                    WHERE fingerprint IN (
                        SELECT fingerprint
                        FROM query_complete_certificates
                        ORDER BY created_at DESC, rowid DESC
                        LIMIT -1 OFFSET ?
                    )
                    """,
                    (max(int(max_entries), 0),),
                )
                total_bytes = int(
                    conn.execute(
                        "SELECT COALESCE(SUM(length(CAST(rows_json AS BLOB))), 0) "
                        "FROM query_complete_certificates"
                    ).fetchone()[0]
                    or 0
                )
                while total_bytes > max(int(max_total_bytes), 0):
                    oldest = conn.execute(
                        "SELECT fingerprint, length(CAST(rows_json AS BLOB)) AS bytes "
                        "FROM query_complete_certificates "
                        "ORDER BY created_at, rowid LIMIT 1"
                    ).fetchone()
                    if not oldest:
                        break
                    conn.execute(
                        "DELETE FROM query_complete_certificates "
                        "WHERE fingerprint = ?",
                        (str(oldest["fingerprint"]),),
                    )
                    total_bytes -= int(oldest["bytes"] or 0)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return True

    def part_paths(self) -> list[str]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT p.path
                FROM parquet_parts p
                JOIN binlog_files b ON b.id = p.binlog_id
                WHERE b.query_visible = 1
                ORDER BY p.event_date, p.path
                """
            ).fetchall()
        return [str(row["path"]) for row in rows]

    def list_parts(
        self,
        limit: int = 500,
        *,
        visible_only: bool = True,
        offset: int = 0,
        oldest_first: bool = False,
    ) -> list[dict[str, Any]]:
        """默认最新优先。补历史欠账时用 oldest_first：未完成的分区都在最早那
        几天，倒序要空翻几百页才碰得到一个。"""

        visibility = "AND b.query_visible = 1" if visible_only else ""
        direction = "ASC" if oldest_first else "DESC"
        with self.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT p.*, b.log_file_name
                FROM parquet_parts p INDEXED BY idx_part_event_date_path
                CROSS JOIN binlog_files b
                WHERE b.id = p.binlog_id
                {visibility}
                ORDER BY p.event_date {direction}, p.path {direction}
                LIMIT ? OFFSET ?
                """,
                (max(int(limit), 0), max(int(offset), 0)),
            ).fetchall()
        return [dict(row) for row in rows]

    def part_by_path(self, path: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT p.*, b.instance_id, b.log_file_name,
                       b.log_begin_utc, b.log_end_utc
                FROM parquet_parts p
                JOIN binlog_files b ON b.id = p.binlog_id
                WHERE p.path = ?
                """,
                (str(path),),
            ).fetchone()
        return dict(row) if row else None

    def slowlog_parts_page(
        self,
        *,
        after_path: str = "",
        limit: int = 2048,
    ) -> list[dict[str, Any]]:
        """Return one bounded, stable page for slow-log reconciliation."""

        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT p.*, b.instance_id, b.log_file_name,
                       b.log_begin_utc, b.log_end_utc
                FROM parquet_parts p
                JOIN binlog_files b ON b.id = p.binlog_id
                WHERE b.query_visible = 1
                  AND b.host_instance_id = ?
                  AND b.log_file_name LIKE ?
                  AND p.path > ?
                ORDER BY p.path
                LIMIT ?
                """,
                (
                    SLOW_LOG_HOST_INSTANCE_ID,
                    SLOW_LOG_FILE_PREFIX + "%",
                    str(after_path or ""),
                    max(int(limit), 0),
                ),
            ).fetchall()
        return [dict(row) for row in rows]

    def parts_in_range(
        self,
        *,
        start_epoch_us: int,
        end_epoch_us: int,
        limit: int = 1_000_000,
        source: str = "",
        instance: str = "",
    ) -> list[dict[str, Any]]:
        """按时间窗取候选分区。

        `source` 在分区层就把两类数据分开，而不是留给下游逐分区扫描后过滤：
        Tabularis 审计事件相对数据库日志极其稀疏（本机实测 3845 / 24495 个文件
        记录），混在一起筛选时引擎必须扫遍整个时间窗的分区才能确认没有更多命中，
        实测 150 秒仍不返回；限定分区后只碰审计自己的那部分。
        """

        clauses = [
            "p.max_event_epoch_us >= ?",
            "p.min_event_epoch_us <= ?",
            "b.query_visible = 1",
        ]
        params: list[Any] = [int(start_epoch_us), int(end_epoch_us)]
        normalized = str(source or "").strip().lower()
        if normalized == "audit":
            clauses.append("b.log_file_name LIKE ?")
            params.append(TABULARIS_AUDIT_FILE_PREFIX + "%")
        elif normalized == "slowlog":
            clauses.extend(
                ["b.host_instance_id = ?", "b.log_file_name LIKE ?"]
            )
            params.extend(
                [SLOW_LOG_HOST_INSTANCE_ID, SLOW_LOG_FILE_PREFIX + "%"]
            )
        elif normalized == "binlog":
            clauses.extend(
                [
                    "b.log_file_name NOT LIKE ?",
                    "b.log_file_name NOT LIKE ?",
                ]
            )
            params.extend(
                [
                    TABULARIS_AUDIT_FILE_PREFIX + "%",
                    SLOW_LOG_FILE_PREFIX + "%",
                ]
            )
        elif normalized == "database":
            clauses.append("b.log_file_name NOT LIKE ?")
            params.append(TABULARIS_AUDIT_FILE_PREFIX + "%")
        instance_id = str(instance or "").strip()
        if instance_id:
            clauses.append("b.instance_id = ?")
            params.append(instance_id)
        params.append(int(limit))
        with self.connection() as conn:
            rows = conn.execute(
                f"""
                SELECT p.*, b.instance_id, b.log_file_name,
                       b.log_begin_utc, b.log_end_utc
                FROM parquet_parts p
                JOIN binlog_files b ON b.id = p.binlog_id
                WHERE {" AND ".join(clauses)}
                ORDER BY p.min_event_epoch_us, p.max_event_epoch_us, p.path
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def part_by_sha256(self, sha256: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT p.*, b.log_file_name, b.log_begin_utc, b.log_end_utc
                FROM parquet_parts p
                JOIN binlog_files b ON b.id = p.binlog_id
                WHERE p.sha256 = ?
                  AND b.query_visible = 1
                LIMIT 1
                """,
                (str(sha256),),
            ).fetchone()
        return dict(row) if row else None

    def part_by_logical_id(self, value: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT p.*, b.log_file_name, b.log_begin_utc, b.log_end_utc
                FROM parquet_parts p
                JOIN binlog_files b ON b.id = p.binlog_id
                WHERE (
                    p.logical_part_id = ?
                    OR p.logical_part_id IN (
                        SELECT alias.logical_part_id
                        FROM parquet_part_identity_aliases alias
                        WHERE alias.object_sha256 = ?
                    )
                )
                  AND b.query_visible = 1
                LIMIT 1
                """,
                (str(value), str(value)),
            ).fetchone()
        return dict(row) if row else None

    def cold_compression_candidates(
        self,
        *,
        cutoff_epoch_us: int,
        limit: int = 1,
    ) -> list[dict[str, Any]]:
        now = utc_now_text()
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT p.*, b.log_file_name, b.log_begin_utc, b.log_end_utc
                FROM parquet_parts p
                JOIN binlog_files b ON b.id = p.binlog_id
                WHERE p.oss_key <> ''
                  AND p.compression_level < 9
                  AND p.max_event_epoch_us <= ?
                  AND (p.compression_retry_after = ''
                       OR p.compression_retry_after <= ?)
                  AND b.query_visible = 1
                  AND b.state = 'done'
                ORDER BY p.max_event_epoch_us, p.min_event_epoch_us, p.path
                LIMIT ?
                """,
                (int(cutoff_epoch_us), now, max(int(limit), 0)),
            ).fetchall()
        return [dict(row) for row in rows]

    def pending_archive_parts(self, limit: int = 1_000_000) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT p.*, b.log_file_name, b.log_begin_utc, b.log_end_utc
                FROM parquet_parts p
                JOIN binlog_files b ON b.id = p.binlog_id
                WHERE p.oss_key = ''
                  AND b.state IN ('stored', 'done')
                ORDER BY p.min_event_epoch_us, p.max_event_epoch_us, p.path
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        return [dict(row) for row in rows]

    def mark_part_archived(
        self,
        path: str,
        *,
        oss_key: str,
        oss_etag: str,
        oss_offset: int = 0,
        oss_length: int = 0,
        oss_object_sha256: str = "",
    ) -> None:
        self.mark_parts_archived(
            [
                {
                    "path": path,
                    "oss_key": oss_key,
                    "oss_etag": oss_etag,
                    "oss_offset": oss_offset,
                    "oss_length": oss_length,
                    "oss_object_sha256": oss_object_sha256,
                }
            ]
        )

    def mark_parts_archived(self, parts: list[dict[str, Any]]) -> None:
        """Commit one bounded upload result batch in a single transaction."""

        if not parts:
            return
        now = utc_now_text()
        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for part in parts:
                    path = str(part.get("path") or "")
                    cursor = conn.execute(
                        """
                        UPDATE parquet_parts
                        SET oss_key = ?, oss_etag = ?, oss_offset = ?, oss_length = ?,
                            oss_object_sha256 = ?, oss_uploaded_at = ?,
                            oss_verified_at = ?, updated_at = ?
                        WHERE path = ?
                        """,
                        (
                            str(part.get("oss_key") or ""),
                            str(part.get("oss_etag") or ""),
                            max(int(part.get("oss_offset") or 0), 0),
                            max(int(part.get("oss_length") or 0), 0),
                            str(part.get("oss_object_sha256") or ""),
                            now,
                            now,
                            now,
                            path,
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise RuntimeError(
                            f"Archived part metadata is missing: {path}"
                        )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def mark_part_compression_level(
        self,
        path: str,
        *,
        expected_object_sha256: str,
        compression_level: int,
    ) -> None:
        now = utc_now_text()
        with self._write_lock, self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE parquet_parts
                SET compression_level = ?, compression_updated_at = ?,
                    compression_error = '', compression_retry_after = '',
                    updated_at = ?
                WHERE path = ? AND object_sha256 = ?
                """,
                (
                    int(compression_level),
                    now,
                    now,
                    str(path),
                    str(expected_object_sha256),
                ),
            )
        if cursor.rowcount != 1:
            raise RuntimeError("Parquet compression state changed concurrently")

    def record_part_compression_failure(
        self,
        path: str,
        *,
        expected_object_sha256: str,
        error: str,
        retry_seconds: int = 300,
    ) -> None:
        retry_after = (
            datetime.now(UTC) + timedelta(seconds=max(int(retry_seconds), 1))
        ).isoformat(timespec="seconds").replace("+00:00", "Z")
        with self._write_lock, self.connection() as conn:
            conn.execute(
                """
                UPDATE parquet_parts
                SET compression_error = ?, compression_retry_after = ?,
                    updated_at = ?
                WHERE path = ? AND object_sha256 = ?
                """,
                (
                    str(error)[:1000],
                    retry_after,
                    utc_now_text(),
                    str(path),
                    str(expected_object_sha256),
                ),
            )

    def commit_part_encoding(
        self,
        path: str,
        *,
        expected_object_sha256: str,
        object_sha256: str,
        size_bytes: int,
        compression_level: int,
        oss_key: str,
        oss_etag: str,
        oss_offset: int = 0,
        oss_length: int = 0,
        oss_object_sha256: str = "",
        retire_grace_seconds: int = 3600,
    ) -> None:
        now = utc_now_text()
        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                current = conn.execute(
                    "SELECT * FROM parquet_parts WHERE path = ?",
                    (str(path),),
                ).fetchone()
                if (
                    current is None
                    or str(current["object_sha256"])
                    != str(expected_object_sha256)
                ):
                    raise RuntimeError("Parquet encoding changed concurrently")
                old_key = str(current["oss_key"] or "")
                if old_key and old_key != str(oss_key):
                    self._retire_oss_object(
                        conn,
                        current,
                        grace_seconds=retire_grace_seconds,
                    )
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO parquet_part_identity_aliases(
                        object_sha256, logical_part_id, created_at
                    ) VALUES(?, ?, ?)
                    """,
                    [
                        (
                            str(expected_object_sha256),
                            str(current["logical_part_id"]),
                            now,
                        ),
                        (
                            str(object_sha256),
                            str(current["logical_part_id"]),
                            now,
                        ),
                    ],
                )
                cursor = conn.execute(
                    """
                    UPDATE parquet_parts
                    SET size_bytes = ?, sha256 = ?, object_sha256 = ?,
                        compression_level = ?, compression_updated_at = ?,
                        compression_error = '', compression_retry_after = '',
                        oss_key = ?, oss_etag = ?, oss_offset = ?, oss_length = ?,
                        oss_object_sha256 = ?, oss_uploaded_at = ?,
                        oss_verified_at = ?, local_last_access_at = ?, updated_at = ?
                    WHERE path = ? AND object_sha256 = ?
                    """,
                    (
                        int(size_bytes),
                        str(object_sha256),
                        str(object_sha256),
                        int(compression_level),
                        now,
                        str(oss_key),
                        str(oss_etag),
                        max(int(oss_offset), 0),
                        max(int(oss_length), 0),
                        str(oss_object_sha256 or object_sha256),
                        now,
                        now,
                        now,
                        now,
                        str(path),
                        str(expected_object_sha256),
                    ),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError("Parquet encoding changed concurrently")
                for table in (
                    "parquet_part_catalog",
                    "parquet_negative_probes",
                    "parquet_positive_probes",
                ):
                    conn.execute(
                        f"UPDATE {table} SET sha256 = ? WHERE path = ?",
                        (str(object_sha256), str(path)),
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def retired_oss_objects_ready(self, limit: int = 16) -> list[dict[str, Any]]:
        now = utc_now_text()
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT retired.*
                FROM retired_oss_objects retired
                WHERE retired.delete_after <= ?
                  AND (retired.retry_after = '' OR retired.retry_after <= ?)
                  AND NOT EXISTS(
                      SELECT 1 FROM parquet_parts current
                      WHERE current.oss_key = retired.oss_key
                  )
                ORDER BY retired.delete_after, retired.oss_key
                LIMIT ?
                """,
                (now, now, max(int(limit), 0)),
            ).fetchall()
        return [dict(row) for row in rows]

    def forget_retired_oss_object(self, oss_key: str) -> None:
        with self._write_lock, self.connection() as conn:
            conn.execute(
                "DELETE FROM retired_oss_objects WHERE oss_key = ?",
                (str(oss_key),),
            )

    def record_retired_oss_failure(
        self,
        oss_key: str,
        error: str,
        *,
        retry_seconds: int = 300,
    ) -> None:
        retry_after = (
            datetime.now(UTC) + timedelta(seconds=max(int(retry_seconds), 1))
        ).isoformat(timespec="seconds").replace("+00:00", "Z")
        with self._write_lock, self.connection() as conn:
            conn.execute(
                """
                UPDATE retired_oss_objects
                SET last_error = ?, retry_after = ?
                WHERE oss_key = ?
                """,
                (str(error)[:1000], retry_after, str(oss_key)),
            )

    def touch_local_parts(self, paths: list[str]) -> None:
        if not paths:
            return
        now = utc_now_text()
        with self._write_lock, self.connection() as conn:
            conn.executemany(
                "UPDATE parquet_parts SET local_last_access_at = ? WHERE path = ?",
                [(now, path) for path in paths],
            )

    def file_archive_complete(self, file_id: str) -> bool:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS total,
                       SUM(CASE WHEN oss_key <> '' THEN 1 ELSE 0 END) AS archived
                FROM parquet_parts
                WHERE binlog_id = ?
                """,
                (file_id,),
            ).fetchone()
        total = int(row["total"] or 0) if row else 0
        archived = int(row["archived"] or 0) if row else 0
        return total == archived

    def update_part(self, path: str, values: dict[str, Any]) -> None:
        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                existing = conn.execute(
                    "SELECT sha256, row_count, min_event_epoch_us, "
                    "max_event_epoch_us, size_bytes, content_revision "
                    "FROM parquet_parts WHERE path = ?",
                    (path,),
                ).fetchone()
                content_changed = bool(existing) and (
                    str(existing["sha256"]) != str(values["sha256"])
                    or any(
                        int(existing[column]) != int(values[column])
                        for column in (
                            "row_count",
                            "min_event_epoch_us",
                            "max_event_epoch_us",
                            "size_bytes",
                        )
                    )
                )
                content_revision = (
                    self._next_content_revision(conn)
                    if content_changed
                    else int(existing["content_revision"] if existing else 0)
                )
                if existing and str(existing["sha256"]) != str(values["sha256"]):
                    conn.execute(
                        "DELETE FROM parquet_part_catalog WHERE path = ?",
                        (path,),
                    )
                    conn.execute(
                        "DELETE FROM parquet_negative_probes WHERE path = ?",
                        (path,),
                    )
                    conn.execute(
                        "DELETE FROM parquet_positive_probes WHERE path = ?",
                        (path,),
                    )
                conn.execute(
                    """
                    UPDATE parquet_parts
                    SET row_count = ?, min_event_epoch_us = ?, max_event_epoch_us = ?,
                        size_bytes = ?,
                        oss_key = CASE WHEN sha256 = ? THEN oss_key ELSE '' END,
                        oss_etag = CASE WHEN sha256 = ? THEN oss_etag ELSE '' END,
                        oss_offset = CASE WHEN sha256 = ? THEN oss_offset ELSE 0 END,
                        oss_length = CASE WHEN sha256 = ? THEN oss_length ELSE 0 END,
                        oss_object_sha256 = CASE
                            WHEN sha256 = ? THEN oss_object_sha256 ELSE '' END,
                        oss_uploaded_at = CASE
                            WHEN sha256 = ? THEN oss_uploaded_at ELSE '' END,
                        oss_verified_at = CASE
                            WHEN sha256 = ? THEN oss_verified_at ELSE '' END,
                        sha256 = ?, content_revision = ?,
                        local_last_access_at = ?, updated_at = ?
                    WHERE path = ?
                    """,
                    (
                        values["row_count"],
                        values["min_event_epoch_us"],
                        values["max_event_epoch_us"],
                        values["size_bytes"],
                        values["sha256"],
                        values["sha256"],
                        values["sha256"],
                        values["sha256"],
                        values["sha256"],
                        values["sha256"],
                        values["sha256"],
                        values["sha256"],
                        content_revision,
                        utc_now_text(),
                        utc_now_text(),
                        path,
                    ),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def delete_part(self, path: str) -> None:
        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                current = conn.execute(
                    "SELECT * FROM parquet_parts WHERE path = ?",
                    (str(path),),
                ).fetchone()
                if current is not None:
                    self._retire_oss_object(conn, current)
                    conn.execute(
                        "DELETE FROM parquet_parts WHERE path = ?",
                        (str(path),),
                    )
                    conn.execute(
                        "DELETE FROM parquet_part_identity_aliases "
                        "WHERE logical_part_id = ?",
                        (str(current["logical_part_id"]),),
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def create_job(
        self,
        kind: str,
        instance_id: str,
        message: str = "",
        *,
        requested_start_utc: str = "",
        requested_end_utc: str = "",
    ) -> str:
        job_id = uuid.uuid4().hex
        with self._write_lock, self.connection() as conn:
            conn.execute(
                """
                INSERT INTO jobs(
                    id, kind, status, project_id, instance_id, started_at,
                    requested_start_utc, requested_end_utc, message
                ) VALUES(?, ?, 'running', ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    kind,
                    "",
                    instance_id,
                    utc_now_text(),
                    requested_start_utc,
                    requested_end_utc,
                    message,
                ),
            )
        return job_id

    def reconcile_interrupted_jobs(self) -> int:
        now = utc_now_text()
        message = "服务曾中断；任务已安全暂停，可从本地文件断点继续"
        with self._write_lock, self.connection() as conn:
            rows = conn.execute(
                "SELECT id FROM jobs WHERE status = 'running'"
            ).fetchall()
            if not rows:
                return 0
            job_ids = [str(row["id"]) for row in rows]
            conn.executemany(
                """
                UPDATE jobs
                SET status = 'paused', finished_at = ?, message = ?,
                    error_code = 'SERVICE_RESTARTED', current_file = ''
                WHERE id = ? AND status = 'running'
                """,
                [(now, message, job_id) for job_id in job_ids],
            )
            conn.executemany(
                """
                INSERT INTO job_events(job_id, level, code, message, created_at)
                VALUES(?, 'warning', 'SERVICE_RESTARTED', ?, ?)
                """,
                [(job_id, message, now) for job_id in job_ids],
            )
        return len(job_ids)

    def update_job(self, job_id: str, **values: Any) -> None:
        allowed = {
            "status",
            "finished_at",
            "current_file",
            "total_files",
            "completed_files",
            "failed_files",
            "discovered_files",
            "message",
            "error_code",
        }
        items = [(key, value) for key, value in values.items() if key in allowed]
        if not items:
            return
        sql = "UPDATE jobs SET " + ", ".join(f"{key} = ?" for key, _ in items)
        params = [value for _, value in items] + [job_id]
        with self._write_lock, self.connection() as conn:
            conn.execute(sql + " WHERE id = ?", params)

    def finish_job(
        self, job_id: str, status: str, message: str, error_code: str = ""
    ) -> None:
        self.update_job(
            job_id,
            status=status,
            finished_at=utc_now_text(),
            message=message,
            error_code=error_code,
            current_file="",
        )

    def add_job_event(
        self, job_id: str, level: str, code: str, message: str
    ) -> None:
        with self._write_lock, self.connection() as conn:
            conn.execute(
                """
                INSERT INTO job_events(job_id, level, code, message, created_at)
                VALUES(?, ?, ?, ?, ?)
                """,
                (job_id, level, code, message, utc_now_text()),
            )

    def jobs(self, limit: int = 50, instance_id: str = "") -> list[dict[str, Any]]:
        """任务列表。instance_id 为空时不过滤(保持单实例时期的行为)。"""
        with self.connection() as conn:
            if instance_id:
                rows = conn.execute(
                    "SELECT * FROM jobs WHERE instance_id = ? "
                    "ORDER BY started_at DESC, rowid DESC LIMIT ?",
                    (str(instance_id), int(limit)),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM jobs ORDER BY started_at DESC, rowid DESC LIMIT ?",
                    (int(limit),),
                ).fetchall()
            result = [dict(row) for row in rows]
            events_by_job: dict[str, list[dict[str, Any]]] = {
                str(item["id"]): [] for item in result
            }
            if events_by_job:
                placeholders = ",".join("?" for _ in events_by_job)
                events = conn.execute(
                    f"""
                    SELECT job_id, level, code, message, created_at
                    FROM (
                        SELECT id, job_id, level, code, message, created_at,
                               ROW_NUMBER() OVER (
                                   PARTITION BY job_id ORDER BY id DESC
                               ) AS event_rank
                        FROM job_events
                        WHERE job_id IN ({placeholders})
                    )
                    WHERE event_rank <= 20
                    ORDER BY job_id, id
                    """,
                    tuple(events_by_job),
                ).fetchall()
                for event in events:
                    event_item = dict(event)
                    job_id = str(event_item.pop("job_id"))
                    events_by_job[job_id].append(event_item)
            for item in result:
                item.pop("project_id", None)
                item["events"] = events_by_job[str(item["id"])]
        return result

    def latest_job(self, instance_id: str = "") -> dict[str, Any] | None:
        jobs = self.jobs(limit=1, instance_id=instance_id)
        return jobs[0] if jobs else None

    @staticmethod
    def _query_task_item(
        row: sqlite3.Row | dict[str, Any],
        *,
        include_result: bool,
    ) -> dict[str, Any]:
        item = dict(row)
        try:
            item["query"] = json.loads(str(item.pop("query_json") or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            item.pop("query_json", None)
            item["query"] = {}
        result_path = str(item.pop("result_path") or "")
        item["has_result"] = bool(result_path)
        if include_result:
            item["_result_path"] = result_path
        item["cancel_requested"] = bool(item.get("cancel_requested"))
        return item

    def create_query_task(self, query: dict[str, Any]) -> str:
        task_id = uuid.uuid4().hex
        with self._write_lock, self.connection() as conn:
            conn.execute(
                """
                INSERT INTO query_tasks(id, status, query_json, created_at, message)
                VALUES(?, 'queued', ?, ?, '等待查询工作线程')
                """,
                (task_id, json_dumps(query), utc_now_text()),
            )
        return task_id

    def start_query_task(self, task_id: str) -> bool:
        with self._write_lock, self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE query_tasks
                SET status = 'running', started_at = ?, message = '正在规划查询'
                WHERE id = ? AND status = 'queued' AND cancel_requested = 0
                """,
                (utc_now_text(), task_id),
            )
        return bool(cursor.rowcount)

    def update_query_task(self, task_id: str, **values: Any) -> None:
        allowed = {
            "status",
            "started_at",
            "finished_at",
            "current_file",
            "total_parts",
            "completed_parts",
            "candidate_parts",
            "indexed_parts",
            "unknown_parts",
            "estimated_bytes",
            "scanned_bytes",
            "cancel_requested",
            "message",
            "error_code",
        }
        items = [(key, value) for key, value in values.items() if key in allowed]
        if not items:
            return
        sql = "UPDATE query_tasks SET " + ", ".join(
            f"{key} = ?" for key, _ in items
        )
        params = [value for _, value in items] + [task_id]
        with self._write_lock, self.connection() as conn:
            conn.execute(sql + " WHERE id = ?", params)

    def finish_query_task(
        self,
        task_id: str,
        status: str,
        message: str,
        *,
        error_code: str = "",
        result_path: str = "",
        result_bytes: int = 0,
    ) -> None:
        with self._write_lock, self.connection() as conn:
            conn.execute(
                """
                UPDATE query_tasks
                SET status = ?, finished_at = ?, current_file = '', message = ?,
                    error_code = ?, result_path = ?, result_bytes = ?
                WHERE id = ?
                """,
                (
                    status,
                    utc_now_text(),
                    message,
                    error_code,
                    str(result_path),
                    max(int(result_bytes), 0),
                    task_id,
                ),
            )

    def request_query_task_cancel(self, task_id: str) -> dict[str, Any] | None:
        terminal = {"succeeded", "failed", "cancelled"}
        now = utc_now_text()
        with self._write_lock, self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM query_tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
            if row is None:
                return None
            status = str(row["status"])
            requested = status not in terminal
            if status == "queued":
                conn.execute(
                    """
                    UPDATE query_tasks
                    SET status = 'cancelled', cancel_requested = 1,
                        finished_at = ?, current_file = '',
                        message = '查询已在排队阶段取消',
                        error_code = 'QUERY_CANCELLED'
                    WHERE id = ? AND status = 'queued'
                    """,
                    (now, task_id),
                )
            elif status in {"running", "cancelling"}:
                conn.execute(
                    """
                    UPDATE query_tasks
                    SET status = 'cancelling', cancel_requested = 1,
                        message = '正在停止查询；已发出的单次读取完成后退出'
                    WHERE id = ? AND status IN ('running', 'cancelling')
                    """,
                    (task_id,),
                )
            updated = conn.execute(
                "SELECT * FROM query_tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
        item = self._query_task_item(updated, include_result=True)
        item["requested"] = requested
        return item

    def query_task(
        self,
        task_id: str,
        *,
        include_result: bool = True,
    ) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM query_tasks WHERE id = ?",
                (task_id,),
            ).fetchone()
        return (
            self._query_task_item(row, include_result=include_result)
            if row is not None
            else None
        )

    def query_tasks(
        self,
        limit: int = 50,
        *,
        include_result: bool = False,
    ) -> list[dict[str, Any]]:
        bounded = min(max(int(limit), 1), 100)
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM query_tasks
                ORDER BY created_at DESC, rowid DESC LIMIT ?
                """,
                (bounded,),
            ).fetchall()
        return [
            self._query_task_item(row, include_result=include_result)
            for row in rows
        ]

    def reconcile_interrupted_query_tasks(self) -> int:
        now = utc_now_text()
        with self._write_lock, self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE query_tasks
                SET status = 'cancelled', cancel_requested = 1,
                    finished_at = ?, current_file = '',
                    message = '服务重启，未完成的查询已停止',
                    error_code = 'SERVICE_RESTARTED'
                WHERE status IN ('queued', 'running', 'cancelling')
                """,
                (now,),
            )
        return max(int(cursor.rowcount), 0)

    def prune_query_tasks(self, keep: int = 100) -> list[str]:
        bounded = min(max(int(keep), 10), 1000)
        terminal = ("succeeded", "failed", "cancelled")
        with self._write_lock, self.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, result_path FROM query_tasks
                WHERE status IN (?, ?, ?)
                ORDER BY created_at DESC, rowid DESC
                LIMIT -1 OFFSET ?
                """,
                (*terminal, bounded),
            ).fetchall()
            if rows:
                conn.executemany(
                    "DELETE FROM query_tasks WHERE id = ?",
                    [(str(row["id"]),) for row in rows],
                )
        return [str(row["result_path"] or "") for row in rows]

    @staticmethod
    def _utc_datetime(value: str) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    @classmethod
    def _median_interval_seconds(cls, values: list[str]) -> tuple[float | None, int]:
        timestamps = sorted(
            value
            for value in (cls._utc_datetime(item) for item in values)
            if value is not None
        )
        intervals = [
            (current - previous).total_seconds()
            for previous, current in zip(timestamps, timestamps[1:])
            if current > previous
        ]
        if len(intervals) < 3:
            return None, len(timestamps)
        return float(median(intervals)), len(timestamps)

    @staticmethod
    def _median_duration_seconds(
        values: list[float | int | str],
    ) -> tuple[float | None, int]:
        durations: list[float] = []
        for value in values:
            try:
                duration = float(value)
            except (TypeError, ValueError):
                continue
            if isfinite(duration) and duration > 0:
                durations.append(duration)
        if len(durations) < 4:
            return None, len(durations)
        return float(median(durations)), len(durations)

    @classmethod
    def estimate_sync_performance(
        cls,
        *,
        processing_durations: list[float | int | str],
        source_times: list[str],
        known_remaining_files: int,
        latest_source_end_utc: str,
        running: bool,
        active_files: int = 0,
        workload_ready: bool = True,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        current = (now or datetime.now(UTC)).astimezone(UTC)
        seconds_per_file, completion_sample_size = cls._median_duration_seconds(
            processing_durations
        )
        source_seconds_per_file, source_sample_size = cls._median_interval_seconds(
            source_times
        )
        known_remaining = max(0, int(known_remaining_files))
        active_count = min(known_remaining, max(0, int(active_files)))
        queued_remaining = max(0, known_remaining - active_count)
        result: dict[str, Any] = {
            "state": "warming_up",
            "seconds_per_file": (
                round(seconds_per_file, 3)
                if seconds_per_file is not None
                else None
            ),
            "source_seconds_per_file": (
                round(source_seconds_per_file, 3)
                if source_seconds_per_file is not None
                else None
            ),
            "processing_files_per_hour": None,
            "source_files_per_hour": None,
            "completion_sample_size": completion_sample_size,
            "source_sample_size": source_sample_size,
            "known_remaining_files": known_remaining,
            "active_files": active_count,
            "queued_remaining_files": queued_remaining,
            "estimated_unseen_files": 0.0,
            "estimated_backlog_files": float(known_remaining),
            "estimated_remaining_seconds": None,
            "estimated_catch_up_at_utc": "",
        }
        if seconds_per_file is not None:
            result["processing_files_per_hour"] = round(
                3600.0 / seconds_per_file, 3
            )
        if source_seconds_per_file is not None:
            result["source_files_per_hour"] = round(
                3600.0 / source_seconds_per_file, 3
            )

        if known_remaining == 0:
            result["state"] = "checking_latest" if running else "caught_up"
            return result
        if running and active_count > 0 and queued_remaining == 0:
            result["state"] = "live_following"
            result["estimated_backlog_files"] = 0.0
            return result
        if not workload_ready or seconds_per_file is None:
            return result

        # ETA is scoped to Completed Binlogs already confirmed by the latest
        # RDS API inventory.  Future or still-open Binlogs are new input, not
        # hidden backlog, and must not turn an already-caught-up service into a
        # false "cannot catch up" state.
        backlog = float(known_remaining)
        result["estimated_backlog_files"] = backlog
        remaining_seconds = backlog * seconds_per_file
        catch_up_at = current + timedelta(seconds=remaining_seconds)
        result.update(
            {
                "state": "available",
                "estimated_remaining_seconds": round(remaining_seconds, 1),
                "estimated_catch_up_at_utc": catch_up_at.isoformat(
                    timespec="seconds"
                ).replace("+00:00", "Z"),
            }
        )
        return result

    def sync_performance(
        self,
        job: dict[str, Any],
        *,
        now: datetime | None = None,
        completion_limit: int = 21,
        source_limit: int = 61,
        active_files: int = 0,
    ) -> dict[str, Any]:
        instance_id = str(job.get("instance_id") or "")
        with self.connection() as conn:
            completion_rows = conn.execute(
                """
                SELECT processing_seconds
                FROM binlog_files
                WHERE instance_id = ? AND state = 'done' AND processing_seconds > 0
                ORDER BY completed_at DESC
                LIMIT ?
                """,
                (instance_id, max(4, int(completion_limit))),
            ).fetchall()
            source_rows = conn.execute(
                """
                SELECT log_begin_utc, log_end_utc
                FROM binlog_files
                WHERE instance_id = ? AND LOWER(remote_status) = 'completed'
                ORDER BY log_begin_utc DESC
                LIMIT ?
                """,
                (instance_id, max(4, int(source_limit))),
            ).fetchall()
        latest_source_end_utc = (
            str(source_rows[0]["log_end_utc"]) if source_rows else ""
        )
        running = str(job.get("status") or "") == "running"
        workload_ready = (
            not running
            or int(job.get("total_files") or 0) > 0
            or int(job.get("discovered_files") or 0) > 0
        )
        return self.estimate_sync_performance(
            processing_durations=[
                float(row["processing_seconds"]) for row in completion_rows
            ],
            source_times=[str(row["log_begin_utc"]) for row in source_rows],
            known_remaining_files=(
                int(job.get("total_files") or 0)
                - int(job.get("completed_files") or 0)
            ),
            latest_source_end_utc=latest_source_end_utc,
            running=running,
            active_files=active_files,
            workload_ready=workload_ready,
            now=now,
        )

    def known_instances(self) -> list[str]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT DISTINCT instance_id FROM binlog_files "
                "WHERE instance_id <> '' ORDER BY instance_id"
            ).fetchall()
        return [str(row[0]) for row in rows]

    def rebuild_storage_file_stats(self) -> dict[str, int | bool]:
        """Explicitly materialize capacity totals without startup-path backfill.

        `BEGIN IMMEDIATE` gives the rebuild a consistent part snapshot and makes
        concurrent writers wait; after commit, database triggers maintain every
        subsequent insert, content/archive/compression update, and deletion.
        """

        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute("DELETE FROM parquet_file_stats")
                conn.execute(
                    """
                    INSERT INTO parquet_file_stats(
                        binlog_id, event_count, parquet_bytes, archived_bytes,
                        min_event_epoch_us, max_event_epoch_us, part_count,
                        archived_part_count, zstd1_part_count, zstd9_part_count
                    )
                    SELECT binlog_id,
                           COALESCE(SUM(row_count), 0),
                           COALESCE(SUM(size_bytes), 0),
                           COALESCE(SUM(CASE WHEN oss_key <> ''
                               THEN size_bytes ELSE 0 END), 0),
                           COALESCE(MIN(min_event_epoch_us), 0),
                           COALESCE(MAX(max_event_epoch_us), 0),
                           COUNT(*),
                           COALESCE(SUM(CASE WHEN oss_key <> ''
                               THEN 1 ELSE 0 END), 0),
                           COALESCE(SUM(CASE WHEN compression_level < 9
                               THEN 1 ELSE 0 END), 0),
                           COALESCE(SUM(CASE WHEN compression_level >= 9
                               THEN 1 ELSE 0 END), 0)
                    FROM parquet_parts
                    GROUP BY binlog_id
                    """
                )
                conn.execute(
                    "UPDATE parquet_file_stats_state "
                    "SET complete = 1, updated_at = ? WHERE singleton = 1",
                    (utc_now_text(),),
                )
                files = int(
                    conn.execute("SELECT COUNT(*) FROM parquet_file_stats").fetchone()[0]
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return {"complete": True, "files": files}

    def storage_metadata_stats(self) -> dict[str, Any]:
        with self.connection() as conn:
            state = conn.execute(
                "SELECT complete FROM parquet_file_stats_state "
                "WHERE singleton = 1"
            ).fetchone()
            if state and bool(state["complete"]):
                row = conn.execute(
                    """
                    SELECT
                        COALESCE(SUM(fs.event_count), 0) AS event_count,
                        COALESCE(SUM(fs.parquet_bytes), 0) AS parquet_bytes,
                        COALESCE(SUM(fs.archived_bytes), 0) AS archived_bytes,
                        MIN(fs.min_event_epoch_us) AS oldest_epoch_us,
                        MAX(fs.max_event_epoch_us) AS latest_epoch_us,
                        COALESCE(SUM(fs.part_count), 0) AS part_count,
                        COALESCE(SUM(fs.archived_part_count), 0)
                            AS archived_part_count,
                        COALESCE(SUM(fs.zstd1_part_count), 0)
                            AS zstd1_part_count,
                        COALESCE(SUM(fs.zstd9_part_count), 0)
                            AS zstd9_part_count
                    FROM parquet_file_stats fs
                    JOIN binlog_files b ON b.id = fs.binlog_id
                    WHERE b.query_visible = 1
                    """
                ).fetchone()
            else:
                # Rollback-compatible fallback until the explicit one-time
                # migration builds the serving rollup.
                row = conn.execute(
                    """
                    SELECT
                        COALESCE(SUM(row_count), 0) AS event_count,
                        COALESCE(SUM(size_bytes), 0) AS parquet_bytes,
                        COALESCE(SUM(CASE WHEN oss_key <> ''
                            THEN size_bytes ELSE 0 END), 0) AS archived_bytes,
                        MIN(min_event_epoch_us) AS oldest_epoch_us,
                        MAX(max_event_epoch_us) AS latest_epoch_us,
                        COUNT(*) AS part_count,
                        COALESCE(SUM(CASE WHEN oss_key <> ''
                            THEN 1 ELSE 0 END), 0) AS archived_part_count,
                        COALESCE(SUM(CASE WHEN compression_level < 9
                            THEN 1 ELSE 0 END), 0) AS zstd1_part_count,
                        COALESCE(SUM(CASE WHEN compression_level >= 9
                            THEN 1 ELSE 0 END), 0) AS zstd9_part_count
                    FROM parquet_parts p
                    JOIN binlog_files b ON b.id = p.binlog_id
                    WHERE b.query_visible = 1
                    """
                ).fetchone()
            files = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN state = 'done' THEN 1 ELSE 0 END) AS done,
                    SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) AS failed
                FROM binlog_files
                """
            ).fetchone()
        result = dict(row) if row else {}
        result["files"] = dict(files) if files else {}
        return result
