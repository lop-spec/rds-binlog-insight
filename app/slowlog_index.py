"""Dedicated slow-log query and analytics index.

Parquet/OSS remains the immutable source of truth.  This SQLite database is a
rebuildable serving index: every part occurrence is retained, while reads
de-duplicate overlapping parts by event identity.  Part identity and a
persistent retry queue make live ingest and historical backfill idempotent.
"""

from __future__ import annotations

import json
import re
import sqlite3
import threading
import time
import zlib
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping

import pyarrow as pa
import pyarrow.parquet as pq

from .sql_fingerprint import FINGERPRINT_FORMAT_VERSION, statement_profile


SLOWLOG_INDEX_VERSION = 8
REQUIRED_RUNTIME_INDEXES = frozenset(
    {
        "idx_slowlog_event_instance_node_time",
        "idx_slowlog_event_node_time",
    }
)
SLOWLOG_EVENT_TYPE = "SLOW_LOG"
BUCKET_US = 5 * 60 * 1_000_000
MISSING_SAMPLE_LIMIT = 20
_COLLECTOR_PREFIX = re.compile(r"^\s*/\*.*?\*/\s*", re.DOTALL)

SQL_ORDERS = {
    "executions": "executions DESC, scan_rows DESC, fingerprint DESC",
    "events": "executions DESC, scan_rows DESC, fingerprint DESC",
    "row_events": "rows_sent DESC, executions DESC, fingerprint DESC",
    "payload_bytes": "sql_bytes DESC, executions DESC, fingerprint DESC",
    "exec_time": "query_time_ms_total DESC, executions DESC, fingerprint DESC",
    "recent": "last_epoch_us DESC, executions DESC, fingerprint DESC",
    "scan_rows": "scan_rows DESC, executions DESC, fingerprint DESC",
}
SLOWLOG_ORDER_KEYS = {
    "executions": ("executions", "scan_rows"),
    "events": ("executions", "scan_rows"),
    "row_events": ("rows_sent", "executions"),
    "payload_bytes": ("sql_bytes", "executions"),
    "exec_time": ("query_time_ms_total", "executions"),
    "recent": ("last_epoch_us", "executions"),
    "scan_rows": ("scan_rows", "executions"),
}


def slowlog_order_key(
    row: Mapping[str, Any],
    keys: tuple[str, ...],
) -> tuple[int | str, ...]:
    """Match SQLite's numeric order plus its deterministic fingerprint tie."""

    return tuple(int(row.get(key) or 0) for key in keys) + (
        str(row.get("fingerprint") or ""),
    )


CLICKHOUSE_SLOWLOG_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_epoch_us", pa.int64()),
        ("event_date", pa.date32()),
        ("instance_id", pa.string()),
        ("node_id", pa.string()),
        ("operation", pa.string()),
        ("database_name", pa.string()),
        ("table_name", pa.string()),
        ("fingerprint", pa.string()),
        ("sql_id", pa.string()),
        ("action", pa.string()),
        ("normalized_sql", pa.string()),
        ("sample_sql", pa.string()),
        ("sql_bytes", pa.uint32()),
        ("query_time_ms", pa.uint64()),
        ("lock_time_ms", pa.uint64()),
        ("rows_examined", pa.uint64()),
        ("rows_sent", pa.uint64()),
        ("database_account", pa.string()),
        ("client_ip", pa.string()),
        ("thread_id", pa.int64()),
        ("source_file_name", pa.string()),
        ("_source_part_path", pa.string()),
        ("_source_part_id", pa.string()),
        ("_source_part_sha256", pa.string()),
        ("_content_revision", pa.uint64()),
    ]
)


SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=15000;

CREATE TABLE IF NOT EXISTS slowlog_parts (
    part_path TEXT PRIMARY KEY,
    logical_part_id TEXT NOT NULL,
    object_sha256 TEXT NOT NULL,
    content_revision INTEGER NOT NULL,
    instance_id TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    indexed_rows INTEGER NOT NULL,
    min_event_epoch_us INTEGER NOT NULL,
    max_event_epoch_us INTEGER NOT NULL,
    format_version INTEGER NOT NULL,
    indexed_at_us INTEGER NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS slowlog_queue (
    part_path TEXT PRIMARY KEY,
    logical_part_id TEXT NOT NULL,
    object_sha256 TEXT NOT NULL,
    content_revision INTEGER NOT NULL,
    min_event_epoch_us INTEGER NOT NULL,
    max_event_epoch_us INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_us INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    enqueued_at_us INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_slowlog_queue_ready
ON slowlog_queue(next_retry_us, max_event_epoch_us DESC, part_path);

CREATE INDEX IF NOT EXISTS idx_slowlog_part_min_event
ON slowlog_parts(min_event_epoch_us);

CREATE INDEX IF NOT EXISTS idx_slowlog_part_max_event
ON slowlog_parts(max_event_epoch_us DESC);

CREATE TABLE IF NOT EXISTS slowlog_reconcile_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    after_path TEXT NOT NULL DEFAULT '',
    complete INTEGER NOT NULL DEFAULT 0,
    updated_at_us INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS slowlog_events (
    occurrence_id INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL,
    part_path TEXT NOT NULL,
    event_epoch_us INTEGER NOT NULL,
    instance_id TEXT NOT NULL,
    node_id TEXT NOT NULL DEFAULT '',
    operation TEXT NOT NULL,
    database_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    sql_id TEXT NOT NULL,
    sql_bytes INTEGER NOT NULL,
    query_time_ms INTEGER NOT NULL,
    lock_time_ms INTEGER NOT NULL,
    rows_examined INTEGER NOT NULL,
    rows_sent INTEGER NOT NULL,
    database_account TEXT NOT NULL,
    client_ip TEXT NOT NULL,
    thread_id INTEGER NOT NULL,
    source_file_name TEXT NOT NULL,
    is_canonical INTEGER NOT NULL DEFAULT 0 CHECK(is_canonical IN (0, 1)),
    UNIQUE(event_id, part_path)
);

-- SQL text is the largest field in the serving database and is only needed by
-- event lists/details (or explicit keyword searches).  Keeping it out of the
-- time-series fact table prevents analytics range scans from faulting hundreds
-- of megabytes of SQL body pages into cache.
CREATE TABLE IF NOT EXISTS slowlog_event_details (
    event_id TEXT NOT NULL,
    part_path TEXT NOT NULL,
    sql_text_z BLOB NOT NULL,
    PRIMARY KEY(event_id, part_path)
) WITHOUT ROWID;

CREATE UNIQUE INDEX IF NOT EXISTS idx_slowlog_event_canonical
ON slowlog_events(instance_id, event_id) WHERE is_canonical = 1;

CREATE INDEX IF NOT EXISTS idx_slowlog_event_time
ON slowlog_events(event_epoch_us DESC, event_id) WHERE is_canonical = 1;

-- Storage/status only needs an exact recent-event count.  Keep this index
-- intentionally narrow so a cold status request reads integer keys rather
-- than the wide analytics projection or event rows.
CREATE INDEX IF NOT EXISTS idx_slowlog_event_recent_count
ON slowlog_events(event_epoch_us) WHERE is_canonical = 1;

CREATE INDEX IF NOT EXISTS idx_slowlog_event_instance_time
ON slowlog_events(instance_id, event_epoch_us DESC, event_id)
WHERE is_canonical = 1;

CREATE INDEX IF NOT EXISTS idx_slowlog_event_instance_node_time
ON slowlog_events(instance_id, node_id, event_epoch_us DESC, event_id)
WHERE is_canonical = 1 AND node_id <> '';

CREATE INDEX IF NOT EXISTS idx_slowlog_event_node_time
ON slowlog_events(node_id, event_epoch_us DESC, event_id)
WHERE is_canonical = 1 AND node_id <> '';

-- primary-db is the normal analytics route.  This deliberately wide but still
-- body-free index covers the complete fact projection, so cold insight queries
-- never have to visit the SQL-text table or the main event rows.
CREATE INDEX IF NOT EXISTS idx_slowlog_event_analytics
ON slowlog_events(
    is_canonical, instance_id, event_epoch_us, event_id, operation,
    database_name, table_name, fingerprint, sql_id,
    query_time_ms, lock_time_ms, rows_examined, rows_sent, sql_bytes
);

CREATE INDEX IF NOT EXISTS idx_slowlog_event_object_time
ON slowlog_events(
    instance_id, database_name, table_name, event_epoch_us DESC, event_id
) WHERE is_canonical = 1;

CREATE INDEX IF NOT EXISTS idx_slowlog_event_object_nocase_time
ON slowlog_events(
    instance_id, database_name COLLATE NOCASE, table_name COLLATE NOCASE,
    event_epoch_us DESC, event_id
) WHERE is_canonical = 1;

CREATE INDEX IF NOT EXISTS idx_slowlog_event_fingerprint_time
ON slowlog_events(instance_id, fingerprint, event_epoch_us DESC)
WHERE is_canonical = 1;

CREATE INDEX IF NOT EXISTS idx_slowlog_event_part
ON slowlog_events(part_path);

CREATE TABLE IF NOT EXISTS slowlog_statements (
    instance_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    sql_id TEXT NOT NULL,
    action TEXT NOT NULL,
    normalized_sql TEXT NOT NULL,
    sample_sql TEXT NOT NULL,
    first_seen_us INTEGER NOT NULL,
    last_seen_us INTEGER NOT NULL,
    PRIMARY KEY(instance_id, fingerprint)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS slowlog_counters (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    indexed_parts INTEGER NOT NULL DEFAULT 0,
    indexed_rows INTEGER NOT NULL DEFAULT 0,
    unique_events INTEGER NOT NULL DEFAULT 0
);

INSERT OR IGNORE INTO slowlog_counters(
    singleton, indexed_parts, indexed_rows, unique_events
) VALUES(1, 0, 0, 0);

CREATE TRIGGER IF NOT EXISTS slowlog_part_count_insert
AFTER INSERT ON slowlog_parts
BEGIN
    UPDATE slowlog_counters
    SET indexed_parts = indexed_parts + 1,
        indexed_rows = indexed_rows + NEW.indexed_rows
    WHERE singleton = 1;
END;

CREATE TRIGGER IF NOT EXISTS slowlog_part_count_update
AFTER UPDATE OF indexed_rows ON slowlog_parts
BEGIN
    UPDATE slowlog_counters
    SET indexed_rows = indexed_rows + NEW.indexed_rows - OLD.indexed_rows
    WHERE singleton = 1;
END;

CREATE TRIGGER IF NOT EXISTS slowlog_part_count_delete
AFTER DELETE ON slowlog_parts
BEGIN
    UPDATE slowlog_counters
    SET indexed_parts = indexed_parts - 1,
        indexed_rows = indexed_rows - OLD.indexed_rows
    WHERE singleton = 1;
END;

CREATE TRIGGER IF NOT EXISTS slowlog_event_count_insert
AFTER INSERT ON slowlog_events
BEGIN
    UPDATE slowlog_counters
    SET unique_events = unique_events + CASE WHEN NOT EXISTS (
        SELECT 1 FROM slowlog_events
        WHERE instance_id = NEW.instance_id
          AND event_id = NEW.event_id AND part_path <> NEW.part_path
    ) THEN 1 ELSE 0 END
    WHERE singleton = 1;
END;

CREATE TRIGGER IF NOT EXISTS slowlog_event_count_delete
AFTER DELETE ON slowlog_events
BEGIN
    UPDATE slowlog_counters
    SET unique_events = unique_events - CASE WHEN NOT EXISTS (
        SELECT 1 FROM slowlog_events
        WHERE instance_id = OLD.instance_id AND event_id = OLD.event_id
    ) THEN 1 ELSE 0 END
    WHERE singleton = 1;
END;

CREATE TRIGGER IF NOT EXISTS slowlog_event_promote_after_delete
AFTER DELETE ON slowlog_events
WHEN OLD.is_canonical = 1
BEGIN
    UPDATE slowlog_events SET is_canonical = 1
    WHERE instance_id = OLD.instance_id AND event_id = OLD.event_id
      AND part_path = (
          SELECT MIN(part_path) FROM slowlog_events
          WHERE instance_id = OLD.instance_id AND event_id = OLD.event_id
      );
END;
"""


class SlowLogIndexError(RuntimeError):
    def __init__(self, message: str, code: str = "SLOWLOG_INDEX_ERROR") -> None:
        super().__init__(message)
        self.code = code


def _runtime_schema_gaps(conn: sqlite3.Connection) -> list[str]:
    columns = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(slowlog_events)")
    }
    indexes = {
        str(row["name"])
        for row in conn.execute("PRAGMA index_list(slowlog_events)")
    }
    gaps: list[str] = []
    if "node_id" not in columns:
        gaps.append("column:slowlog_events.node_id")
    gaps.extend(
        f"index:{name}"
        for name in sorted(REQUIRED_RUNTIME_INDEXES - indexes)
    )
    return gaps


def _now_us() -> int:
    return int(time.time() * 1_000_000)


def _identity(part: dict[str, Any]) -> str:
    return str(part.get("logical_part_id") or part.get("sha256") or "")


def _object_sha(part: dict[str, Any]) -> str:
    return str(
        part.get("oss_object_sha256")
        or part.get("object_sha256")
        or part.get("sha256")
        or ""
    )


def _integer(value: Any) -> int:
    try:
        return int(float(value or 0))
    except (TypeError, ValueError, OverflowError):
        return 0


def _metrics(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(str(value or ""))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _clean_sql(value: Any) -> str:
    return _COLLECTOR_PREFIX.sub("", str(value or ""), count=1).strip()


def _compress_sql(value: str) -> bytes:
    return zlib.compress(str(value).encode("utf-8"), level=6)


def _decompress_sql(value: Any) -> str:
    if value is None:
        return ""
    raw = bytes(value)
    try:
        return zlib.decompress(raw).decode("utf-8")
    except (zlib.error, UnicodeDecodeError):
        return ""


def _trend_width(start_us: int, end_us: int, max_points: int = 480) -> int:
    span = max(int(end_us) - int(start_us), BUCKET_US)
    buckets = max(span // BUCKET_US, 1)
    factor = max(int((buckets + max_points - 1) // max_points), 1)
    return BUCKET_US * factor


def _empty_transactions() -> dict[str, Any]:
    return {
        "ddl": [],
        "multi_table": [],
        "totals": {
            "transactions": 0,
            "row_events": 0,
            "payload_bytes": 0,
            "ddl_transactions": 0,
            "multi_table_transactions": 0,
            "cross_second_transactions": 0,
            "max_dependency_depth": 0,
            "avg_dependency_depth": 0,
            "txn_length_bytes": 0,
            "max_duration_us": 0,
            "max_row_events": 0,
            "avg_duration_us": 0,
        },
        "row_histogram": [],
        "duration_histogram": [],
        "longest": [],
        "largest": [],
        "trend": [],
    }


def slowlog_trend_width(
    start_epoch_us: int,
    end_epoch_us: int,
    max_points: int = 480,
) -> int:
    return _trend_width(start_epoch_us, end_epoch_us, max_points)


def slowlog_empty_transactions() -> dict[str, Any]:
    return _empty_transactions()


def slowlog_statement_result(row: Mapping[str, Any]) -> dict[str, Any]:
    executions = int(row["executions"] or 0)
    scan_rows = int(row["scan_rows"] or 0)
    rows_sent = int(row["rows_sent"] or 0)
    return {
        **dict(row),
        "events": executions,
        "row_events": rows_sent,
        "payload_bytes": int(row["sql_bytes"] or 0),
        "slow_events": executions,
        "est_rows_per_exec": round(scan_rows / executions) if executions else 0,
        "est_scan_total": scan_rows,
        "est_db": str(row["database_name"] or ""),
        "est_full_scan": 0,
        "scan_rows_per_exec": round(scan_rows / executions)
        if executions
        else 0,
        "scan_source": "actual",
        "source_kind": "slowlog",
    }


def _migrate_v7_to_v8(conn: sqlite3.Connection) -> None:
    """Add optional node identity without rebuilding immutable source parts."""

    columns = {
        str(row["name"])
        for row in conn.execute("PRAGMA table_info(slowlog_events)")
    }
    conn.execute("BEGIN IMMEDIATE")
    try:
        if "node_id" not in columns:
            conn.execute(
                "ALTER" " TABLE slowlog_events "
                "ADD COLUMN node_id TEXT NOT NULL DEFAULT ''"
            )
        # Keep the v7 analytics covering index in place.  Rebuilding that wide
        # index would scan the entire serving database during a small upgrade.
        # Node-filtered reads use the new narrow partial indexes.  Fresh v8
        # databases use the same stable body-free covering definition so a
        # rollback never has to rebuild or reinterpret this wide index.
        conn.execute(
            "UPDATE slowlog_parts SET format_version = ?",
            (SLOWLOG_INDEX_VERSION,),
        )
        conn.execute("COMMIT")
    except BaseException:
        conn.execute("ROLLBACK")
        raise


class SlowLogIndex:
    """Rebuildable slow-log serving index with a persistent part queue."""

    def __init__(self, path: Path, *, run_migrations: bool = True) -> None:
        self.path = Path(path)
        self._anchor_lock = threading.Lock()
        self._wal_anchor: sqlite3.Connection | None = None
        if not run_migrations and not self.path.is_file():
            raise SlowLogIndexError(
                "slow-log index schema has not been initialized",
                "SLOWLOG_INDEX_SCHEMA_MISSING",
            )
        if run_migrations:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        else:
            # Runtime operations intentionally use short connections.  Keep
            # one read-only connection alive so closing each writer does not
            # become SQLite's "last connection" and force a checkpoint plus
            # WAL/shm unlink for every indexed part.
            self._open_wal_anchor()
        try:
            with self.connection() as conn:
                version_row = conn.execute("PRAGMA user_version").fetchone()
                version = int(version_row[0] if version_row else 0)
                if not run_migrations:
                    if version != SLOWLOG_INDEX_VERSION:
                        raise SlowLogIndexError(
                            f"slow-log index schema {version} is not supported by worker",
                            "SLOWLOG_INDEX_SCHEMA_MISMATCH",
                        )
                    gaps = _runtime_schema_gaps(conn)
                    if gaps:
                        raise SlowLogIndexError(
                            "slow-log index schema is incomplete: "
                            + ", ".join(gaps),
                            "SLOWLOG_INDEX_SCHEMA_INCOMPLETE",
                        )
                    return
                if version == 7:
                    _migrate_v7_to_v8(conn)
                    version = SLOWLOG_INDEX_VERSION
                if version not in {0, SLOWLOG_INDEX_VERSION}:
                    raise SlowLogIndexError(
                        f"slow-log index schema {version} is not supported by "
                        f"version {SLOWLOG_INDEX_VERSION}",
                        "SLOWLOG_INDEX_SCHEMA_UNSUPPORTED",
                    )
                conn.executescript(SCHEMA)
                conn.execute(
                    "INSERT OR IGNORE INTO slowlog_reconcile_state"
                    "(singleton, after_path, complete, updated_at_us) VALUES(1, '', 0, 0)"
                )
                conn.execute(f"PRAGMA user_version = {SLOWLOG_INDEX_VERSION}")
                gaps = _runtime_schema_gaps(conn)
                if gaps:
                    raise SlowLogIndexError(
                        "slow-log index migration is incomplete: "
                        + ", ".join(gaps),
                        "SLOWLOG_INDEX_SCHEMA_INCOMPLETE",
                    )
        except Exception:
            self.close()
            raise

    def _open_wal_anchor(self) -> None:
        """Keep WAL coordination files stable for this index's lifetime."""

        with self._anchor_lock:
            if self._wal_anchor is not None:
                return
            connection = sqlite3.connect(
                self.path,
                timeout=15,
                isolation_level=None,
                check_same_thread=False,
            )
            try:
                connection.row_factory = sqlite3.Row
                connection.execute("PRAGMA busy_timeout=15000")
                connection.execute("PRAGMA query_only=ON")
                journal_mode = str(
                    connection.execute("PRAGMA journal_mode").fetchone()[0]
                ).lower()
                if journal_mode != "wal":
                    raise SlowLogIndexError(
                        "slow-log WAL anchor requires journal_mode=wal, "
                        f"got {journal_mode}",
                        "SLOWLOG_INDEX_JOURNAL_MODE_MISMATCH",
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

    @contextmanager
    def connection(
        self,
        *,
        control: Any | None = None,
        temp_store_memory: bool = False,
    ) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=15000")
        if temp_store_memory:
            # Full-window parity verification builds two connection-local TEMP
            # tables.  Keep those verifier-only tables off the shared production
            # disk; normal serving connections retain SQLite's default policy.
            conn.execute("PRAGMA temp_store=MEMORY")
        conn.create_function(
            "slowlog_sql_text",
            1,
            _decompress_sql,
            deterministic=True,
        )
        cancelled: list[BaseException] = []
        if control is not None:
            def check_cancelled() -> int:
                try:
                    control.check_cancelled()
                except BaseException as exc:
                    cancelled.append(exc)
                    return 1
                return 0

            conn.set_progress_handler(check_cancelled, 2000)
        try:
            yield conn
        except sqlite3.OperationalError:
            if cancelled:
                raise cancelled[0]
            raise
        finally:
            if control is not None:
                conn.set_progress_handler(None, 0)
            conn.close()

    # -------------------------------------------------------------- queue/coverage

    def enqueue_parts(self, parts: Iterable[dict[str, Any]]) -> int:
        values = [part for part in parts if str(part.get("path") or "")]
        if not values:
            return 0
        now = _now_us()
        queued = 0
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for part in values:
                    path = str(part["path"])
                    identity = _identity(part)
                    current = conn.execute(
                        "SELECT logical_part_id, format_version FROM slowlog_parts "
                        "WHERE part_path = ?",
                        (path,),
                    ).fetchone()
                    if (
                        current
                        and str(current["logical_part_id"]) == identity
                        and int(current["format_version"]) == SLOWLOG_INDEX_VERSION
                    ):
                        conn.execute(
                            "DELETE FROM slowlog_queue WHERE part_path = ?",
                            (path,),
                        )
                        continue
                    conn.execute(
                        """
                        INSERT INTO slowlog_queue(
                            part_path, logical_part_id, object_sha256,
                            content_revision, min_event_epoch_us,
                            max_event_epoch_us, row_count, attempts,
                            next_retry_us, last_error, enqueued_at_us, updated_at_us
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, 0, 0, '', ?, ?)
                        ON CONFLICT(part_path) DO UPDATE SET
                            logical_part_id = excluded.logical_part_id,
                            object_sha256 = excluded.object_sha256,
                            content_revision = excluded.content_revision,
                            min_event_epoch_us = excluded.min_event_epoch_us,
                            max_event_epoch_us = excluded.max_event_epoch_us,
                            row_count = excluded.row_count,
                            attempts = CASE
                                WHEN slowlog_queue.logical_part_id <> excluded.logical_part_id
                                THEN 0 ELSE slowlog_queue.attempts END,
                            next_retry_us = CASE
                                WHEN slowlog_queue.logical_part_id <> excluded.logical_part_id
                                THEN 0 ELSE slowlog_queue.next_retry_us END,
                            last_error = CASE
                                WHEN slowlog_queue.logical_part_id <> excluded.logical_part_id
                                THEN '' ELSE slowlog_queue.last_error END,
                            enqueued_at_us = CASE
                                WHEN slowlog_queue.logical_part_id <> excluded.logical_part_id
                                THEN excluded.enqueued_at_us
                                ELSE slowlog_queue.enqueued_at_us END,
                            updated_at_us = excluded.updated_at_us
                        """,
                        (
                            path,
                            identity,
                            _object_sha(part),
                            _integer(part.get("content_revision")),
                            _integer(part.get("min_event_epoch_us")),
                            _integer(part.get("max_event_epoch_us")),
                            _integer(part.get("row_count")),
                            now,
                            now,
                        ),
                    )
                    queued += 1
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return queued

    def record_failure(self, part: dict[str, Any], error: BaseException | str) -> None:
        self.enqueue_parts([part])
        now = _now_us()
        message = str(error).replace("\x00", " ")[:1000]
        with self.connection() as conn:
            row = conn.execute(
                "SELECT attempts FROM slowlog_queue "
                "WHERE part_path = ? AND logical_part_id = ?",
                (str(part["path"]), _identity(part)),
            ).fetchone()
            attempts = int(row["attempts"] or 0) + 1 if row else 1
            backoff_seconds = min(3600, 2 ** min(attempts, 10))
            conn.execute(
                """
                UPDATE slowlog_queue
                SET attempts = attempts + 1,
                    next_retry_us = ?, last_error = ?, updated_at_us = ?
                WHERE part_path = ? AND logical_part_id = ?
                """,
                (
                    now + backoff_seconds * 1_000_000,
                    message,
                    now,
                    str(part["path"]),
                    _identity(part),
                ),
            )

    def remove_path(self, path: str) -> None:
        """Remove a deleted/non-slow part from the rebuildable index and queue."""

        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute("DELETE FROM slowlog_queue WHERE part_path = ?", (str(path),))
                conn.execute(
                    "DELETE FROM slowlog_event_details WHERE part_path = ?",
                    (str(path),),
                )
                conn.execute("DELETE FROM slowlog_events WHERE part_path = ?", (str(path),))
                conn.execute("DELETE FROM slowlog_parts WHERE part_path = ?", (str(path),))
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    def ready_paths(self, limit: int = 32) -> list[str]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT part_path FROM slowlog_queue
                WHERE next_retry_us <= ?
                -- A due retry must not starve behind a continuous stream of
                -- ordinary work. Among ordinary parts, drain the oldest event
                -- first so continuous live arrivals cannot starve historical
                -- reconciliation. Exponential next_retry_us backoff still
                -- keeps a repeatedly failing part from monopolizing the worker.
                ORDER BY CASE WHEN attempts > 0 THEN 0 ELSE 1 END,
                         next_retry_us,
                         max_event_epoch_us ASC,
                         part_path
                LIMIT ?
                """,
                (_now_us(), max(int(limit), 0)),
            ).fetchall()
        return [str(row["part_path"]) for row in rows]

    def _covered(self, parts: list[dict[str, Any]]) -> dict[str, str]:
        paths = [str(part["path"]) for part in parts]
        covered: dict[str, str] = {}
        if not paths:
            return covered
        with self.connection() as conn:
            for start in range(0, len(paths), 400):
                chunk = paths[start : start + 400]
                marks = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"SELECT part_path, logical_part_id FROM slowlog_parts "
                    f"WHERE part_path IN ({marks}) AND format_version = ?",
                    (*chunk, SLOWLOG_INDEX_VERSION),
                ).fetchall()
                for row in rows:
                    covered[str(row["part_path"])] = str(row["logical_part_id"])
        return covered

    def missing_parts(
        self, parts: list[dict[str, Any]], *, limit: int = 32
    ) -> list[dict[str, Any]]:
        covered = self._covered(parts)
        candidates = [
            part
            for part in parts
            if covered.get(str(part["path"])) != _identity(part)
        ]
        if not candidates:
            return []
        queued: dict[str, str] = {}
        paths = [str(part["path"]) for part in candidates]
        with self.connection() as conn:
            for start in range(0, len(paths), 400):
                chunk = paths[start : start + 400]
                marks = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"SELECT part_path, logical_part_id FROM slowlog_queue "
                    f"WHERE part_path IN ({marks})",
                    chunk,
                ).fetchall()
                for row in rows:
                    queued[str(row["part_path"])] = str(row["logical_part_id"])
        return [
            part
            for part in candidates
            if queued.get(str(part["path"])) != _identity(part)
        ][: max(int(limit), 0)]

    def coverage(self, parts: list[dict[str, Any]]) -> dict[str, Any]:
        covered = self._covered(parts)
        missing = [
            str(part["path"])
            for part in parts
            if covered.get(str(part["path"])) != _identity(part)
        ]
        covered_rows = sum(
            _integer(part.get("row_count"))
            for part in parts
            if covered.get(str(part["path"])) == _identity(part)
        )
        return {
            "complete": not missing,
            "total_parts": len(parts),
            "covered_parts": len(parts) - len(missing),
            "covered_rows": covered_rows,
            "missing_parts_total": len(missing),
            "missing_parts": missing[:MISSING_SAMPLE_LIMIT],
            "missing_parts_truncated": len(missing) > MISSING_SAMPLE_LIMIT,
        }

    def reconcile_state(self) -> dict[str, Any]:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT after_path, complete, updated_at_us "
                "FROM slowlog_reconcile_state WHERE singleton = 1"
            ).fetchone()
        return {
            "after_path": str(row["after_path"] or ""),
            "complete": bool(row["complete"]),
            "updated_at_us": int(row["updated_at_us"] or 0),
        }

    def advance_reconcile(self, *, after_path: str, complete: bool) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                UPDATE slowlog_reconcile_state
                SET after_path = ?, complete = ?, updated_at_us = ?
                WHERE singleton = 1
                """,
                (str(after_path), 1 if complete else 0, _now_us()),
            )

    # --------------------------------------------------------------------- build

    @staticmethod
    def _read_events(source: Path) -> list[dict[str, Any]]:
        parquet = pq.ParquetFile(source)
        available = set(parquet.schema_arrow.names)
        wanted = [
            name
            for name in (
                "event_id",
                "event_epoch_us",
                "instance_id",
                "host_instance_id",
                "source_file_name",
                "raw_event_type",
                "operation",
                "database_name",
                "table_name",
                "thread_id",
                "execution_time_ms",
                "sql_kind",
                "sql_text",
                "columns_json",
                "connection_name",
                "database_account",
            )
            if name in available
        ]
        if not {"event_id", "event_epoch_us"}.issubset(available):
            raise SlowLogIndexError(
                "slow-log Parquet is missing event identity columns",
                "SLOWLOG_INDEX_COLUMNS_MISSING",
            )
        rows: list[dict[str, Any]] = []
        for batch in parquet.iter_batches(
            batch_size=8192,
            columns=wanted,
            use_threads=False,
        ):
            rows.extend(batch.to_pylist())
        return rows

    def build_part(
        self,
        part: dict[str, Any],
        source: Path,
        *,
        queue_if_missing: bool = True,
    ) -> dict[str, Any]:
        path = Path(source)
        if not path.is_file():
            raise SlowLogIndexError(
                "slow-log source part is unavailable",
                "SLOWLOG_INDEX_SOURCE_MISSING",
            )
        if queue_if_missing:
            self.enqueue_parts([part])
        desired_identity = _identity(part)
        raw_rows = self._read_events(path)
        events: list[tuple[Any, ...]] = []
        details: list[tuple[str, str, bytes]] = []
        statements: dict[tuple[str, str], dict[str, Any]] = {}
        seen_event_ids: set[str] = set()
        for row in raw_rows:
            if str(row.get("raw_event_type") or SLOWLOG_EVENT_TYPE).upper() != SLOWLOG_EVENT_TYPE:
                continue
            event_id = str(row.get("event_id") or "")
            epoch_us = _integer(row.get("event_epoch_us"))
            if not event_id or epoch_us <= 0 or event_id in seen_event_ids:
                continue
            seen_event_ids.add(event_id)
            instance = str(row.get("instance_id") or part.get("instance_id") or "")
            operation = str(row.get("operation") or "OTHER").upper()
            database = str(row.get("database_name") or "")
            table = str(row.get("table_name") or "")
            sql_text = _clean_sql(row.get("sql_text"))
            metric = _metrics(row.get("columns_json"))
            supplied_profile = metric.get("statement_profile")
            if (
                isinstance(supplied_profile, dict)
                and int(supplied_profile.get("format_version") or 0)
                == FINGERPRINT_FORMAT_VERSION
                and len(str(supplied_profile.get("fingerprint") or "")) == 32
            ):
                profile = {
                    name: str(supplied_profile.get(name) or "")
                    for name in (
                        "fingerprint",
                        "normalized",
                        "source",
                        "action",
                        "sample",
                    )
                }
            else:
                profile = statement_profile(
                    sql_kind=str(row.get("sql_kind") or "ORIGINAL"),
                    sql_text=sql_text,
                    row_query="",
                    operation=operation,
                    database=database,
                    table=table,
                )
            node_id = str(metric.get("node_id") or "")
            examined = _integer(metric.get("rows_examined"))
            sent = _integer(metric.get("rows_sent"))
            lock_ms = _integer(metric.get("lock_time_ms"))
            query_ms = _integer(row.get("execution_time_ms"))
            sql_id = str(metric.get("sql_id") or "")
            account = str(row.get("database_account") or "")
            client_ip = str(row.get("connection_name") or "")
            thread_id = _integer(row.get("thread_id"))
            part_path = str(part["path"])
            events.append(
                (
                    event_id,
                    part_path,
                    epoch_us,
                    instance,
                    node_id,
                    operation,
                    database,
                    table,
                    profile["fingerprint"],
                    sql_id,
                    len(sql_text.encode("utf-8")),
                    query_ms,
                    lock_ms,
                    examined,
                    sent,
                    account,
                    client_ip,
                    thread_id,
                    str(row.get("source_file_name") or part.get("log_file_name") or ""),
                )
            )
            details.append((event_id, part_path, _compress_sql(sql_text)))
            statement_key = (instance, profile["fingerprint"])
            known = statements.get(statement_key)
            if known is None:
                statements[statement_key] = {
                    "sql_id": sql_id,
                    "action": profile["action"],
                    "normalized_sql": profile["normalized"],
                    "sample_sql": profile["sample"],
                    "first_seen_us": epoch_us,
                    "last_seen_us": epoch_us,
                }
            else:
                known["first_seen_us"] = min(known["first_seen_us"], epoch_us)
                known["last_seen_us"] = max(known["last_seen_us"], epoch_us)
                if sql_id:
                    known["sql_id"] = sql_id

        now = _now_us()
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                queued = conn.execute(
                    "SELECT logical_part_id FROM slowlog_queue WHERE part_path = ?",
                    (str(part["path"]),),
                ).fetchone()
                if queued is None and not queue_if_missing:
                    conn.execute("ROLLBACK")
                    return {"built": 0, "indexed_rows": 0, "skipped": "missing-queue"}
                if queued and str(queued["logical_part_id"]) != desired_identity:
                    conn.execute("ROLLBACK")
                    return {"built": 0, "indexed_rows": 0, "skipped": "stale-queue"}
                current = conn.execute(
                    "SELECT logical_part_id, content_revision, format_version "
                    "FROM slowlog_parts WHERE part_path = ?",
                    (str(part["path"]),),
                ).fetchone()
                if (
                    current
                    and str(current["logical_part_id"]) == desired_identity
                    and int(current["format_version"]) == SLOWLOG_INDEX_VERSION
                ):
                    conn.execute(
                        "DELETE FROM slowlog_queue WHERE part_path = ? "
                        "AND logical_part_id = ?",
                        (str(part["path"]), desired_identity),
                    )
                    conn.execute("COMMIT")
                    return {"built": 0, "indexed_rows": 0, "skipped": "current"}
                incoming_revision = _integer(part.get("content_revision"))
                if current and int(current["content_revision"]) > incoming_revision:
                    conn.execute("ROLLBACK")
                    return {"built": 0, "indexed_rows": 0, "skipped": "stale-revision"}

                conn.execute(
                    "DELETE FROM slowlog_event_details WHERE part_path = ?",
                    (str(part["path"]),),
                )
                conn.execute(
                    "DELETE FROM slowlog_events WHERE part_path = ?",
                    (str(part["path"]),),
                )
                if events:
                    conn.executemany(
                        """
                        INSERT INTO slowlog_events(
                            event_id, part_path, event_epoch_us, instance_id,
                            node_id, operation, database_name, table_name, fingerprint,
                            sql_id, sql_bytes, query_time_ms, lock_time_ms,
                            rows_examined, rows_sent, database_account,
                            client_ip, thread_id, source_file_name, is_canonical
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                        ON CONFLICT(event_id, part_path) DO UPDATE SET
                            event_epoch_us = excluded.event_epoch_us,
                            instance_id = excluded.instance_id,
                            node_id = excluded.node_id,
                            operation = excluded.operation,
                            database_name = excluded.database_name,
                            table_name = excluded.table_name,
                            fingerprint = excluded.fingerprint,
                            sql_id = excluded.sql_id,
                            sql_bytes = excluded.sql_bytes,
                            query_time_ms = excluded.query_time_ms,
                            lock_time_ms = excluded.lock_time_ms,
                            rows_examined = excluded.rows_examined,
                            rows_sent = excluded.rows_sent,
                            database_account = excluded.database_account,
                            client_ip = excluded.client_ip,
                            thread_id = excluded.thread_id,
                            source_file_name = excluded.source_file_name
                        """,
                        events,
                    )
                    conn.executemany(
                        """
                        INSERT INTO slowlog_event_details(event_id, part_path, sql_text_z)
                        VALUES(?, ?, ?)
                        ON CONFLICT(event_id, part_path) DO UPDATE SET
                            sql_text_z = excluded.sql_text_z
                        """,
                        details,
                    )
                    # Canonical identity is (instance,event), not event_id alone:
                    # historical collectors produced a small number of equal IDs
                    # in different instances.  Re-elect by MIN(part_path) every
                    # time so concurrent backfill order cannot change results.
                    conn.execute(
                        """
                        UPDATE slowlog_events SET is_canonical = 0
                        WHERE (instance_id, event_id) IN (
                            SELECT instance_id, event_id FROM slowlog_events
                            WHERE part_path = ?
                        )
                        """,
                        (str(part["path"]),),
                    )
                    conn.execute(
                        """
                        UPDATE slowlog_events SET is_canonical = 1
                        WHERE (instance_id, event_id) IN (
                            SELECT instance_id, event_id FROM slowlog_events
                            WHERE part_path = ?
                        )
                          AND part_path = (
                              SELECT MIN(candidate.part_path)
                              FROM slowlog_events AS candidate
                              WHERE candidate.instance_id = slowlog_events.instance_id
                                AND candidate.event_id = slowlog_events.event_id
                          )
                        """,
                        (str(part["path"]),),
                    )
                for (instance, fingerprint), item in statements.items():
                    conn.execute(
                        """
                        INSERT INTO slowlog_statements(
                            instance_id, fingerprint, sql_id, action,
                            normalized_sql, sample_sql, first_seen_us, last_seen_us
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(instance_id, fingerprint) DO UPDATE SET
                            sql_id = CASE WHEN excluded.sql_id <> ''
                                THEN excluded.sql_id ELSE slowlog_statements.sql_id END,
                            action = excluded.action,
                            normalized_sql = excluded.normalized_sql,
                            sample_sql = CASE
                                WHEN excluded.last_seen_us >= slowlog_statements.last_seen_us
                                THEN excluded.sample_sql ELSE slowlog_statements.sample_sql END,
                            first_seen_us = MIN(
                                slowlog_statements.first_seen_us, excluded.first_seen_us
                            ),
                            last_seen_us = MAX(
                                slowlog_statements.last_seen_us, excluded.last_seen_us
                            )
                        """,
                        (
                            instance,
                            fingerprint,
                            item["sql_id"],
                            item["action"],
                            item["normalized_sql"],
                            item["sample_sql"],
                            item["first_seen_us"],
                            item["last_seen_us"],
                        ),
                    )
                conn.execute(
                    """
                    INSERT INTO slowlog_parts(
                        part_path, logical_part_id, object_sha256,
                        content_revision, instance_id, row_count, indexed_rows,
                        min_event_epoch_us, max_event_epoch_us,
                        format_version, indexed_at_us
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(part_path) DO UPDATE SET
                        logical_part_id = excluded.logical_part_id,
                        object_sha256 = excluded.object_sha256,
                        content_revision = excluded.content_revision,
                        instance_id = excluded.instance_id,
                        row_count = excluded.row_count,
                        indexed_rows = excluded.indexed_rows,
                        min_event_epoch_us = excluded.min_event_epoch_us,
                        max_event_epoch_us = excluded.max_event_epoch_us,
                        format_version = excluded.format_version,
                        indexed_at_us = excluded.indexed_at_us
                    """,
                    (
                        str(part["path"]),
                        desired_identity,
                        _object_sha(part),
                        incoming_revision,
                        str(part.get("instance_id") or (events[0][3] if events else "")),
                        _integer(part.get("row_count")),
                        len(events),
                        _integer(part.get("min_event_epoch_us")),
                        _integer(part.get("max_event_epoch_us")),
                        SLOWLOG_INDEX_VERSION,
                        now,
                    ),
                )
                conn.execute(
                    "DELETE FROM slowlog_queue WHERE part_path = ? "
                    "AND logical_part_id = ?",
                    (str(part["path"]), desired_identity),
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return {"built": 1, "indexed_rows": len(events), "stat_groups": 0}

    def export_clickhouse_parts(
        self,
        parts: Iterable[dict[str, Any]],
        destination: Path,
    ) -> dict[str, Any]:
        """Export current occurrence rows for one bounded ClickHouse batch.

        The SQLite slow-log index is already the place where historical
        Parquet rows are fingerprinted and normalized.  Reusing that exact
        derived representation avoids a second OSS download and, more
        importantly, guarantees that SQLite fallback and ClickHouse group by
        the same fingerprint version.
        """

        requested = {
            str(part["path"]): {
                "identity": _identity(part),
                "sha256": _object_sha(part),
                "content_revision": max(
                    _integer(part.get("content_revision")), 0
                ),
            }
            for part in parts
            if str(part.get("path") or "") and _identity(part)
        }
        destination = Path(destination)
        destination.parent.mkdir(parents=True, exist_ok=True)
        current: dict[str, dict[str, Any]] = {}
        part_rows: dict[str, int] = {}
        writer: pq.ParquetWriter | None = None
        try:
            with self.connection() as conn:
                paths = list(requested)
                for offset in range(0, len(paths), 400):
                    chunk = paths[offset : offset + 400]
                    if not chunk:
                        continue
                    placeholders = ",".join("?" for _ in chunk)
                    rows = conn.execute(
                        "SELECT part_path,logical_part_id,object_sha256,"
                        "content_revision FROM slowlog_parts "
                        f"WHERE part_path IN ({placeholders})",
                        chunk,
                    ).fetchall()
                    for row in rows:
                        path = str(row["part_path"])
                        expected = requested.get(path)
                        if expected is None:
                            continue
                        if str(row["logical_part_id"]) != expected["identity"]:
                            continue
                        current[path] = {
                            "identity": str(row["logical_part_id"]),
                            "sha256": str(
                                row["object_sha256"] or expected["sha256"]
                            ),
                            "content_revision": max(
                                int(row["content_revision"] or 0), 0
                            ),
                        }
                        part_rows[path] = 0

                current_paths = list(current)
                for offset in range(0, len(current_paths), 200):
                    chunk = current_paths[offset : offset + 200]
                    if not chunk:
                        continue
                    placeholders = ",".join("?" for _ in chunk)
                    cursor = conn.execute(
                        """
                        SELECT e.event_id,e.event_epoch_us,e.instance_id,
                               e.node_id,e.operation,e.database_name,
                               e.table_name,e.fingerprint,e.sql_id,
                               COALESCE(st.action, '') AS action,
                               COALESCE(st.normalized_sql, '') AS normalized_sql,
                               COALESCE(st.sample_sql, '') AS sample_sql,
                               e.sql_bytes,e.query_time_ms,e.lock_time_ms,
                               e.rows_examined,e.rows_sent,e.database_account,
                               e.client_ip,e.thread_id,e.source_file_name,
                               e.part_path
                        FROM slowlog_events e
                        LEFT JOIN slowlog_statements st
                          ON st.instance_id=e.instance_id
                         AND st.fingerprint=e.fingerprint
                        WHERE e.part_path IN ("""
                        + placeholders
                        + ") ORDER BY e.part_path,e.event_epoch_us,e.event_id",
                        chunk,
                    )
                    while True:
                        rows = cursor.fetchmany(8192)
                        if not rows:
                            break
                        payload: dict[str, list[Any]] = {
                            name: [] for name in CLICKHOUSE_SLOWLOG_SCHEMA.names
                        }
                        for row in rows:
                            path = str(row["part_path"])
                            identity = current[path]
                            epoch_us = int(row["event_epoch_us"] or 0)
                            for name in (
                                "event_id",
                                "instance_id",
                                "node_id",
                                "operation",
                                "database_name",
                                "table_name",
                                "fingerprint",
                                "sql_id",
                                "action",
                                "normalized_sql",
                                "sample_sql",
                                "database_account",
                                "client_ip",
                                "source_file_name",
                            ):
                                payload[name].append(str(row[name] or ""))
                            payload["event_epoch_us"].append(epoch_us)
                            payload["event_date"].append(
                                datetime.fromtimestamp(
                                    epoch_us / 1_000_000, UTC
                                ).date()
                            )
                            payload["sql_bytes"].append(
                                min(max(int(row["sql_bytes"] or 0), 0), 2**32 - 1)
                            )
                            for name in (
                                "query_time_ms",
                                "lock_time_ms",
                                "rows_examined",
                                "rows_sent",
                            ):
                                payload[name].append(max(int(row[name] or 0), 0))
                            payload["thread_id"].append(int(row["thread_id"] or 0))
                            payload["_source_part_path"].append(path)
                            payload["_source_part_id"].append(identity["identity"])
                            payload["_source_part_sha256"].append(identity["sha256"])
                            payload["_content_revision"].append(
                                identity["content_revision"]
                            )
                            part_rows[path] += 1
                        if writer is None:
                            writer = pq.ParquetWriter(
                                destination,
                                CLICKHOUSE_SLOWLOG_SCHEMA,
                                compression="zstd",
                                compression_level=1,
                            )
                        writer.write_table(
                            pa.Table.from_pydict(
                                payload,
                                schema=CLICKHOUSE_SLOWLOG_SCHEMA,
                            )
                        )
        finally:
            if writer is not None:
                writer.close()
        if writer is None:
            # A zero-row part is still a valid, coverable source occurrence.
            pq.write_table(
                pa.Table.from_pylist([], schema=CLICKHOUSE_SLOWLOG_SCHEMA),
                destination,
                compression="zstd",
                compression_level=1,
            )
        return {
            "part_rows": part_rows,
            "exported_parts": len(part_rows),
            "exported_rows": sum(part_rows.values()),
            "missing_parts": [
                path for path in requested if path not in current
            ],
        }

    def statement_profiles(
        self,
        keys: Iterable[tuple[str, str]],
    ) -> dict[tuple[str, str], dict[str, Any]]:
        """Load only requested SQL text dimensions by their covering key.

        ClickHouse performs the numeric range aggregation.  The normalized and
        sample SQL bodies remain in this compact dimension table and are read
        only for fingerprints that can appear in one of the returned top-N
        orders.  Chunking keeps SQLite's bound-parameter count predictable.
        """

        requested = sorted(
            {
                (str(instance), str(fingerprint))
                for instance, fingerprint in keys
                if str(instance) and str(fingerprint)
            }
        )
        profiles: dict[tuple[str, str], dict[str, Any]] = {}
        if not requested:
            return profiles
        with self.connection() as conn:
            for offset in range(0, len(requested), 200):
                chunk = requested[offset : offset + 200]
                predicate = " OR ".join(
                    "(instance_id = ? AND fingerprint = ?)" for _key in chunk
                )
                parameters = [value for key in chunk for value in key]
                rows = conn.execute(
                    "SELECT instance_id,fingerprint,sql_id,action,"
                    "normalized_sql,sample_sql,first_seen_us,last_seen_us "
                    "FROM slowlog_statements WHERE " + predicate,
                    parameters,
                ).fetchall()
                for row in rows:
                    key = (str(row["instance_id"]), str(row["fingerprint"]))
                    profiles[key] = dict(row)
        return profiles

    # --------------------------------------------------------------------- query

    @staticmethod
    def _event_where(
        query: dict[str, Any], start_epoch_us: int, end_epoch_us: int
    ) -> tuple[str, list[Any]]:
        clauses = ["event_epoch_us >= ?", "event_epoch_us <= ?"]
        params: list[Any] = [int(start_epoch_us), int(end_epoch_us)]
        instance = str(query.get("instance") or "").strip()
        if instance:
            clauses.append("instance_id = ?")
            params.append(instance)
        node_id = str(query.get("node_id") or query.get("nodeId") or "").strip()
        if node_id:
            clauses.append("node_id <> ''")
            clauses.append("node_id = ?")
            params.append(node_id)
        fingerprint = str(query.get("fingerprint") or "").strip()
        if fingerprint:
            clauses.append("fingerprint = ?")
            params.append(fingerprint)
        for key, column in (("database", "database_name"), ("table", "table_name")):
            value = str(query.get(key) or "").strip().lower()
            if value:
                clauses.append(f"{column} = ? COLLATE NOCASE")
                params.append(value)
        for key, column in (("account", "database_account"), ("connection", "client_ip")):
            value = str(query.get(key) or "").strip().lower()
            if value:
                clauses.append(f"lower({column}) LIKE ?")
                params.append(f"%{value}%")
        operations = [
            str(value).upper()
            for value in (query.get("operations") or [])
            if str(value).strip()
        ]
        if operations:
            clauses.append("operation IN (" + ",".join("?" for _ in operations) + ")")
            params.extend(operations)
        keyword = str(query.get("keyword") or "").strip().lower()
        if keyword:
            terms = [term for term in keyword.split() if term]
            joiner = " OR " if str(query.get("keyword_mode") or "AND").upper() == "OR" else " AND "
            predicates: list[str] = []
            for term in terms:
                predicates.append(
                    "(lower(slowlog_sql_text(sql_text_z)) LIKE ? "
                    "OR lower(database_name) LIKE ? "
                    "OR lower(table_name) LIKE ? OR lower(sql_id) LIKE ?)"
                )
                params.extend([f"%{term}%"] * 4)
            if predicates:
                clauses.append("(" + joiner.join(predicates) + ")")
        return " AND ".join(clauses), params

    @staticmethod
    def _event_index_hint(query: dict[str, Any]) -> str:
        """Choose the narrow serving index instead of the analytics cover."""

        instance = str(query.get("instance") or "").strip()
        node_id = str(query.get("node_id") or query.get("nodeId") or "").strip()
        if instance and str(query.get("fingerprint") or "").strip():
            return " INDEXED BY idx_slowlog_event_fingerprint_time"
        if instance and node_id:
            return " INDEXED BY idx_slowlog_event_instance_node_time"
        if instance and str(query.get("database") or "").strip():
            return " INDEXED BY idx_slowlog_event_object_nocase_time"
        if instance:
            return " INDEXED BY idx_slowlog_event_instance_time"
        if node_id:
            return " INDEXED BY idx_slowlog_event_node_time"
        return " INDEXED BY idx_slowlog_event_time"

    @staticmethod
    def _event_result(row: sqlite3.Row, *, detail: bool = False) -> dict[str, Any]:
        epoch_us = int(row["event_epoch_us"] or 0)
        result = {
            "event_id": str(row["event_id"]),
            "event_epoch_us": epoch_us,
            "event_time_utc": datetime.fromtimestamp(
                epoch_us / 1_000_000, UTC
            ).isoformat().replace("+00:00", "Z"),
            "instance_id": str(row["instance_id"]),
            "node_id": str(row["node_id"]),
            "operation": str(row["operation"]),
            "database_name": str(row["database_name"]),
            "table_name": str(row["table_name"]),
            "fingerprint": str(row["fingerprint"]),
            "transaction_id": "",
            "gtid": "",
            "xid": "",
            "sql_kind": "ORIGINAL",
            "sql_text": _decompress_sql(row["sql_text_z"]),
            "before_json": "",
            "after_json": "",
            "source_file_name": str(row["source_file_name"]),
            "host_instance_id": "slow-log",
            "server_id": 0,
            "thread_id": int(row["thread_id"] or 0),
            "start_position": 0,
            "end_position": 0,
            "row_index": 0,
            "execution_time_ms": int(row["query_time_ms"] or 0),
            "query_time_ms": int(row["query_time_ms"] or 0),
            "error_code": 0,
            "row_query": "",
            "raw_event_type": SLOWLOG_EVENT_TYPE,
            "connection_id": "",
            "connection_name": str(row["client_ip"]),
            "client_ip": str(row["client_ip"]),
            "database_account": str(row["database_account"]),
            "execution_status": "success",
            "error_message": "",
            "affected_rows": int(row["rows_sent"] or 0),
            "started_epoch_us": epoch_us,
            "finished_epoch_us": epoch_us + int(row["query_time_ms"] or 0) * 1000,
            "batch_id": "",
            "statement_index": -1,
            "transaction_context_id": "",
            "locator": "",
            "rows_examined": int(row["rows_examined"] or 0),
            "rows_sent": int(row["rows_sent"] or 0),
            "lock_time_ms": int(row["lock_time_ms"] or 0),
            "sql_id": str(row["sql_id"]),
        }
        if detail:
            result["columns_json"] = json.dumps(
                {
                    "rows_examined": int(row["rows_examined"] or 0),
                    "rows_sent": int(row["rows_sent"] or 0),
                    "lock_time_ms": int(row["lock_time_ms"] or 0),
                    "sql_id": str(row["sql_id"]),
                    "node_id": str(row["node_id"]),
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
        return result

    def query_events(
        self,
        query: dict[str, Any],
        *,
        start_epoch_us: int,
        end_epoch_us: int,
    ) -> dict[str, Any]:
        limit = min(max(_integer(query.get("limit") or 100), 1), 1000)
        offset = min(max(_integer(query.get("offset")), 0), 100_000)
        where, params = self._event_where(query, start_epoch_us, end_epoch_us)
        index_hint = self._event_index_hint(query)
        with self.connection() as conn:
            rows = conn.execute(
                f"SELECT e.*, d.sql_text_z FROM slowlog_events e{index_hint} "
                "JOIN slowlog_event_details d "
                "ON d.event_id = e.event_id AND d.part_path = e.part_path "
                f"WHERE e.is_canonical = 1 AND {where} "
                "ORDER BY e.event_epoch_us DESC, e.event_id DESC LIMIT ? OFFSET ?",
                (*params, limit + 1, offset),
            ).fetchall()
        return {
            "rows": [self._event_result(row) for row in rows[:limit]],
            "has_more": len(rows) > limit,
            "limit": limit,
            "offset": offset,
            "coverage_found": True,
            "tiers_used": ["slowlog-index"],
            "local_parts_read": 0,
            "oss_parts_read": 0,
            "range_start_epoch_us": int(start_epoch_us),
            "range_end_epoch_us": int(end_epoch_us),
        }

    def existing_event_ids(
        self,
        event_ids: Iterable[str],
        instance: str = "",
    ) -> set[str]:
        values = sorted({str(value) for value in event_ids if str(value)})
        if not values:
            return set()
        instance = str(instance or "").strip()
        found: set[str] = set()
        with self.connection() as conn:
            for position in range(0, len(values), 400):
                chunk = values[position : position + 400]
                placeholders = ",".join("?" for _ in chunk)
                instance_clause = " AND instance_id = ?" if instance else ""
                params: tuple[Any, ...] = (
                    (*chunk, instance) if instance else tuple(chunk)
                )
                rows = conn.execute(
                    "SELECT event_id FROM slowlog_events "
                    f"WHERE is_canonical = 1 AND event_id IN ({placeholders})"
                    f"{instance_clause}",
                    params,
                ).fetchall()
                found.update(str(row["event_id"]) for row in rows)
        return found

    def _event_details_with_connection(
        self,
        conn: sqlite3.Connection,
        event_ids: Iterable[str],
        instance: str = "",
    ) -> dict[str, dict[str, Any]]:
        values = sorted({str(value) for value in event_ids if str(value)})
        if not values:
            return {}
        instance = str(instance or "").strip()
        results: dict[str, dict[str, Any]] = {}
        for position in range(0, len(values), 300):
            chunk = values[position : position + 300]
            placeholders = ",".join("?" for _ in chunk)
            instance_clause = " AND e.instance_id = ?" if instance else ""
            params: tuple[Any, ...] = (
                (*chunk, instance) if instance else tuple(chunk)
            )
            rows = conn.execute(
                "SELECT e.*, d.sql_text_z FROM slowlog_events e "
                "JOIN slowlog_event_details d "
                "ON d.event_id = e.event_id AND d.part_path = e.part_path "
                f"WHERE e.event_id IN ({placeholders}) "
                f"AND e.is_canonical = 1{instance_clause} "
                "ORDER BY e.event_id, e.instance_id",
                params,
            ).fetchall()
            for row in rows:
                event_id = str(row["event_id"])
                results.setdefault(event_id, self._event_result(row, detail=True))
        return results

    def event_details(
        self,
        event_ids: Iterable[str],
        instance: str = "",
    ) -> dict[str, dict[str, Any]]:
        with self.connection() as conn:
            return self._event_details_with_connection(conn, event_ids, instance)

    def event_detail(
        self,
        event_id: str,
        instance: str = "",
    ) -> dict[str, Any] | None:
        instance = str(instance or "").strip()
        instance_clause = " AND e.instance_id = ?" if instance else ""
        params: tuple[Any, ...] = (str(event_id), instance) if instance else (str(event_id),)
        with self.connection() as conn:
            row = conn.execute(
                "SELECT e.*, d.sql_text_z FROM slowlog_events e "
                "JOIN slowlog_event_details d "
                "ON d.event_id = e.event_id AND d.part_path = e.part_path "
                f"WHERE e.event_id = ? AND e.is_canonical = 1{instance_clause} "
                "ORDER BY e.instance_id LIMIT 1",
                params,
            ).fetchone()
        return self._event_result(row, detail=True) if row else None

    # ------------------------------------------------------------------ analytics

    @staticmethod
    def _analytics_filters(
        *,
        start_epoch_us: int,
        end_epoch_us: int,
        instance: str,
        node_id: str,
        database: str,
        table: str,
        operation: str,
    ) -> tuple[str, dict[str, Any]]:
        clauses = [
            "e.event_epoch_us >= :start_epoch_us",
            "e.event_epoch_us <= :end_epoch_us",
        ]
        params: dict[str, Any] = {
            "start_epoch_us": int(start_epoch_us),
            "end_epoch_us": int(end_epoch_us),
        }
        if node_id:
            clauses.append("e.node_id <> ''")
            clauses.append("e.node_id = :node_id")
            params["node_id"] = str(node_id)
        for value, column, name in (
            (instance, "e.instance_id", "instance"),
            (database, "e.database_name", "database"),
            (table, "e.table_name", "table_name"),
            (operation.upper() if operation else "", "e.operation", "operation"),
        ):
            if value:
                if name in {"database", "table_name"}:
                    clauses.append(f"{column} = :{name} COLLATE NOCASE")
                else:
                    clauses.append(f"{column} = :{name}")
                params[name] = str(value)
        return " AND ".join(clauses), params

    @staticmethod
    def _statement_row(row: sqlite3.Row) -> dict[str, Any]:
        return slowlog_statement_result(row)

    @staticmethod
    def _attach_extreme_event_ids(
        conn: sqlite3.Connection,
        rows: list[dict[str, Any]],
    ) -> None:
        fingerprints = sorted(
            {str(row.get("fingerprint") or "") for row in rows}
            - {""}
        )
        for row in rows:
            row["max_scan_event_id"] = ""
            row["max_query_event_id"] = ""
        if not fingerprints:
            return
        placeholders = ",".join("?" for _ in fingerprints)
        extremes = conn.execute(
            "SELECT s.fingerprint, "
            "COALESCE((SELECT e.event_id FROM temp.slow_events_scope e "
            "WHERE e.fingerprint = s.fingerprint "
            "ORDER BY e.rows_examined DESC, e.query_time_ms DESC, "
            "e.event_epoch_us DESC, e.event_id DESC LIMIT 1), '') "
            "AS max_scan_event_id, "
            "COALESCE((SELECT e.event_id FROM temp.slow_events_scope e "
            "WHERE e.fingerprint = s.fingerprint "
            "ORDER BY e.query_time_ms DESC, e.rows_examined DESC, "
            "e.event_epoch_us DESC, e.event_id DESC LIMIT 1), '') "
            "AS max_query_event_id "
            "FROM temp.slow_scope s "
            f"WHERE s.fingerprint IN ({placeholders})",
            tuple(fingerprints),
        ).fetchall()
        by_fingerprint = {str(row["fingerprint"]): row for row in extremes}
        for row in rows:
            extreme = by_fingerprint.get(str(row.get("fingerprint") or ""))
            if extreme is None:
                continue
            row["max_scan_event_id"] = str(extreme["max_scan_event_id"] or "")
            row["max_query_event_id"] = str(extreme["max_query_event_id"] or "")

    def summarize(
        self,
        *,
        start_epoch_us: int,
        end_epoch_us: int,
        instance: str = "",
        node_id: str = "",
        database: str = "",
        table: str = "",
        operation: str = "",
        limit: int = 50,
        order: str = "executions",
        control: Any | None = None,
        temp_store_memory: bool = False,
    ) -> dict[str, Any]:
        if control is not None:
            control.check_cancelled()
        limit = min(max(int(limit), 1), 500)
        order = order if order in SQL_ORDERS else "executions"
        where, params = self._analytics_filters(
            start_epoch_us=start_epoch_us,
            end_epoch_us=end_epoch_us,
            instance=instance,
            node_id=node_id,
            database=database,
            table=table,
            operation=operation,
        )
        width = _trend_width(start_epoch_us, end_epoch_us)
        if node_id:
            analytics_index_hint = (
                " INDEXED BY idx_slowlog_event_instance_node_time"
                if instance
                else " INDEXED BY idx_slowlog_event_node_time"
            )
        else:
            analytics_index_hint = (
                " INDEXED BY idx_slowlog_event_analytics" if instance else ""
            )
        with self.connection(
            control=control,
            temp_store_memory=temp_store_memory,
        ) as conn:
            conn.execute("DROP TABLE IF EXISTS temp.slow_events_scope")
            conn.execute("DROP TABLE IF EXISTS temp.slow_scope")
            conn.execute(
                f"""
                CREATE TEMP TABLE slow_events_scope AS
                SELECT e.event_id, e.event_epoch_us, e.instance_id,
                       e.operation, e.database_name, e.table_name,
                       e.fingerprint, e.sql_id, e.query_time_ms,
                       e.lock_time_ms, e.rows_examined, e.rows_sent,
                       e.sql_bytes
                FROM slowlog_events e{analytics_index_hint}
                WHERE e.is_canonical = 1 AND {where}
                """,
                params,
            )
            conn.execute(
                """
                CREATE TEMP TABLE slow_scope AS
                SELECT e.fingerprint AS fingerprint,
                       COUNT(*) AS executions,
                       SUM(e.rows_examined) AS scan_rows,
                       MAX(e.rows_examined) AS scan_rows_max,
                       SUM(e.rows_sent) AS rows_sent,
                       MAX(e.rows_sent) AS rows_sent_max,
                       SUM(e.query_time_ms) AS query_time_ms_total,
                       MAX(e.query_time_ms) AS query_time_ms_max,
                       SUM(e.lock_time_ms) AS lock_time_ms_total,
                       MAX(e.lock_time_ms) AS lock_time_ms_max,
                       SUM(e.sql_bytes) AS sql_bytes,
                       MIN(e.event_epoch_us) AS first_epoch_us,
                       MAX(e.event_epoch_us) AS last_epoch_us,
                       COUNT(DISTINCT e.database_name || X'1f' || e.table_name) AS objects,
                       MIN(e.instance_id) AS instance_id,
                       MIN(e.database_name) AS database_name,
                       MIN(e.table_name) AS table_name,
                       MIN(e.operation) AS operation,
                       MAX(e.sql_id) AS sql_id,
                       MIN(e.event_id) AS sample_event_id,
                       MIN(st.action) AS action,
                       MIN(st.normalized_sql) AS normalized_sql,
                       MIN(st.sample_sql) AS sample_sql
                FROM temp.slow_events_scope e
                LEFT JOIN slowlog_statements st
                  ON st.instance_id = e.instance_id
                 AND st.fingerprint = e.fingerprint
                GROUP BY e.fingerprint
                """,
            )
            totals = conn.execute(
                """
                SELECT COALESCE(SUM(executions), 0) AS executions,
                       COALESCE(SUM(scan_rows), 0) AS scan_rows,
                       COALESCE(SUM(rows_sent), 0) AS rows_sent,
                       COALESCE(SUM(query_time_ms_total), 0) AS query_time_ms_total,
                       COALESCE(MAX(query_time_ms_max), 0) AS query_time_ms_max,
                       COALESCE(SUM(lock_time_ms_total), 0) AS lock_time_ms_total,
                       COALESCE(MAX(lock_time_ms_max), 0) AS lock_time_ms_max,
                       COALESCE(SUM(sql_bytes), 0) AS sql_bytes,
                       COUNT(*) AS fingerprints,
                       COALESCE(SUM(objects), 0) AS objects
                FROM temp.slow_scope
                """
            ).fetchone()
            object_total = conn.execute(
                """
                SELECT COUNT(*) AS objects FROM (
                    SELECT DISTINCT database_name, table_name
                    FROM temp.slow_events_scope
                )
                """,
            ).fetchone()
            orders: dict[str, list[dict[str, Any]]] = {}
            for key, clause in SQL_ORDERS.items():
                rows = conn.execute(
                    f"SELECT * FROM temp.slow_scope ORDER BY {clause} LIMIT ?",
                    (limit,),
                ).fetchall()
                orders[key] = [self._statement_row(row) for row in rows]
            objects = conn.execute(
                """
                SELECT database_name, table_name,
                       COUNT(*) AS events,
                       SUM(sql_bytes) AS payload_bytes,
                       COUNT(DISTINCT fingerprint) AS fingerprints,
                       SUM(rows_examined) AS scan_rows,
                       SUM(rows_sent) AS rows_sent,
                       SUM(query_time_ms) AS query_time_ms_total
                FROM temp.slow_events_scope
                GROUP BY database_name, table_name
                ORDER BY scan_rows DESC, events DESC LIMIT ?
                """,
                (limit,),
            ).fetchall()
            operations = conn.execute(
                """
                SELECT operation, COUNT(*) AS events,
                       SUM(sql_bytes) AS payload_bytes,
                       SUM(rows_examined) AS scan_rows
                FROM temp.slow_events_scope
                GROUP BY operation ORDER BY events DESC
                """,
            ).fetchall()
            trend = conn.execute(
                """
                SELECT (event_epoch_us / :width) * :width AS ts,
                       COUNT(*) AS events,
                       SUM(query_time_ms) AS query_time_ms_total,
                       SUM(rows_examined) AS scan_rows,
                       SUM(rows_sent) AS rows_sent
                FROM temp.slow_events_scope
                GROUP BY ts ORDER BY ts
                """,
                {"width": width},
            ).fetchall()
            self._attach_extreme_event_ids(conn, orders[order])
            sample_ids = {
                str(row.get(key) or "")
                for row in orders[order]
                for key in ("max_scan_event_id", "max_query_event_id")
                if str(row.get(key) or "")
            }
            sample_events = self._event_details_with_connection(
                conn,
                sample_ids,
                instance,
            )
        if control is not None:
            control.check_cancelled()
        executions = int(totals["executions"] or 0)
        scan_rows = int(totals["scan_rows"] or 0)
        rows_sent = int(totals["rows_sent"] or 0)
        sql_bytes = int(totals["sql_bytes"] or 0)
        return {
            "window": {
                "start_epoch_us": int(start_epoch_us),
                "end_epoch_us": int(end_epoch_us),
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
                    "fingerprints": int(totals["fingerprints"] or 0),
                    "objects": int(object_total["objects"] or 0),
                    "boundary_events": 0,
                    "scan_rows": scan_rows,
                    "actual_scan_rows": scan_rows,
                    "est_scan_rows": scan_rows,
                    "scan_covered_executions": executions,
                    "est_covered_executions": executions,
                    "query_time_ms_total": int(totals["query_time_ms_total"] or 0),
                    "query_time_ms_max": int(totals["query_time_ms_max"] or 0),
                    "lock_time_ms_total": int(totals["lock_time_ms_total"] or 0),
                    "lock_time_ms_max": int(totals["lock_time_ms_max"] or 0),
                },
                "statements": orders[order],
                "orders": orders,
                "sample_events": sample_events,
                "objects": [dict(row) for row in objects],
                "operations": [dict(row) for row in operations],
                "trend": [dict(row) for row in trend],
            },
            "transactions": _empty_transactions(),
            "locks": {
                "long_transactions": [],
                "large_transactions": [],
                "row_hotspots": [],
                "table_hotspots": [],
                "ddl_windows": [],
                "risk": {},
            },
        }

    # --------------------------------------------------------------------- stats

    def stats(self) -> dict[str, Any]:
        now = _now_us()
        with self.connection() as conn:
            parts = conn.execute(
                """
                SELECT COALESCE((
                           SELECT min_event_epoch_us FROM slowlog_parts
                           ORDER BY min_event_epoch_us LIMIT 1
                       ), 0) AS min_us,
                       COALESCE((
                           SELECT max_event_epoch_us FROM slowlog_parts
                           ORDER BY max_event_epoch_us DESC LIMIT 1
                       ), 0) AS max_us
                """
            ).fetchone()
            counters = conn.execute(
                "SELECT indexed_parts, indexed_rows, unique_events "
                "FROM slowlog_counters WHERE singleton = 1"
            ).fetchone()
            queue = conn.execute(
                """
                SELECT COUNT(*) AS pending,
                       COALESCE(SUM(CASE WHEN last_error <> '' THEN 1 ELSE 0 END), 0)
                           AS failed,
                       COALESCE(MIN(enqueued_at_us), 0) AS oldest_us,
                       COALESCE(MAX(attempts), 0) AS max_attempts
                FROM slowlog_queue
                """
            ).fetchone()
            recent = conn.execute(
                """
                SELECT COUNT(*) AS events_60m,
                       COALESCE(SUM(CASE WHEN event_epoch_us >= ? THEN 1 ELSE 0 END), 0)
                           AS events_10m
                FROM slowlog_events INDEXED BY idx_slowlog_event_recent_count
                WHERE is_canonical = 1 AND event_epoch_us >= ?
                """,
                (now - 600 * 1_000_000, now - 3600 * 1_000_000),
            ).fetchone()
        database_bytes = sum(
            candidate.stat().st_size
            for candidate in (
                self.path,
                Path(str(self.path) + "-wal"),
                Path(str(self.path) + "-shm"),
            )
            if candidate.exists()
        )
        oldest = int(queue["oldest_us"] or 0)
        reconcile = self.reconcile_state()
        indexed_rows = int(counters["indexed_rows"] or 0)
        unique_events = int(counters["unique_events"] or 0)
        return {
            "schema_version": SLOWLOG_INDEX_VERSION,
            "indexed_parts": int(counters["indexed_parts"] or 0),
            "indexed_rows": indexed_rows,
            "unique_events": unique_events,
            "duplicate_occurrences": max(indexed_rows - unique_events, 0),
            "pending_parts": int(queue["pending"] or 0),
            "failed_parts": int(queue["failed"] or 0),
            "max_attempts": int(queue["max_attempts"] or 0),
            "oldest_pending_age_seconds": max((now - oldest) // 1_000_000, 0)
            if oldest
            else 0,
            "indexed_watermark_us": int(parts["max_us"] or 0),
            "indexed_start_us": int(parts["min_us"] or 0),
            "indexed_events_last_hour": int(recent["events_60m"] or 0),
            "indexed_events_last_10_minutes": int(recent["events_10m"] or 0),
            "events_per_minute_60m": round(int(recent["events_60m"] or 0) / 60, 3),
            "events_per_minute_10m": round(int(recent["events_10m"] or 0) / 10, 3),
            "index_bytes": int(database_bytes),
            "reconcile_complete": bool(reconcile["complete"]),
            "reconcile_after_path": str(reconcile["after_path"]),
        }

    def prune_orphans(self, known_paths: Iterable[str]) -> int:
        known = {str(path) for path in known_paths}
        with self.connection() as conn:
            rows = conn.execute("SELECT part_path FROM slowlog_parts").fetchall()
        stale = [str(row["part_path"]) for row in rows if str(row["part_path"]) not in known]
        if not stale:
            return 0
        removed = 0
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for path in stale:
                    conn.execute(
                        "DELETE FROM slowlog_event_details WHERE part_path = ?",
                        (path,),
                    )
                    conn.execute("DELETE FROM slowlog_events WHERE part_path = ?", (path,))
                    removed += int(
                        conn.execute(
                            "DELETE FROM slowlog_parts WHERE part_path = ?", (path,)
                        ).rowcount
                        or 0
                    )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return removed
