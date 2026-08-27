from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable


SCHEMA_VERSION = 1
ACTIVE_LOAD_STATES = ("pending", "loading", "ready", "load_failed")
ACTIVE_DELETE_STATES = ("delete_pending", "deleting", "delete_failed")
SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;

CREATE TABLE IF NOT EXISTS clickhouse_schema (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    version INTEGER NOT NULL
);

INSERT OR IGNORE INTO clickhouse_schema(singleton, version) VALUES(1, 1);

CREATE TABLE IF NOT EXISTS clickhouse_parts (
    part_path TEXT NOT NULL,
    logical_part_id TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    content_revision INTEGER NOT NULL,
    min_event_epoch_us INTEGER NOT NULL,
    max_event_epoch_us INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    size_bytes INTEGER NOT NULL,
    status TEXT NOT NULL CHECK(status IN (
        'pending', 'loading', 'ready', 'load_failed',
        'delete_pending', 'deleting', 'delete_failed', 'retired'
    )),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_us INTEGER NOT NULL DEFAULT 0,
    inserted_rows INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    first_seen_us INTEGER NOT NULL,
    updated_at_us INTEGER NOT NULL,
    ready_at_us INTEGER NOT NULL DEFAULT 0,
    seen_generation INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(part_path, logical_part_id)
);

CREATE INDEX IF NOT EXISTS idx_clickhouse_parts_queue
ON clickhouse_parts(status, next_retry_us, max_event_epoch_us DESC);

CREATE INDEX IF NOT EXISTS idx_clickhouse_parts_identity
ON clickhouse_parts(logical_part_id, status);

CREATE INDEX IF NOT EXISTS idx_clickhouse_parts_coverage
ON clickhouse_parts(part_path, logical_part_id, status);

CREATE TABLE IF NOT EXISTS clickhouse_reconcile_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    generation INTEGER NOT NULL DEFAULT 0,
    start_epoch_us INTEGER NOT NULL DEFAULT 0,
    end_epoch_us INTEGER NOT NULL DEFAULT 0,
    source_parts INTEGER NOT NULL DEFAULT 0,
    eligible_parts INTEGER NOT NULL DEFAULT 0,
    completed_at_us INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT ''
);

INSERT OR IGNORE INTO clickhouse_reconcile_state(singleton) VALUES(1);
"""


class ClickHouseManifestError(RuntimeError):
    pass


def part_identity(part: dict[str, Any]) -> str:
    return str(part.get("logical_part_id") or part.get("sha256") or "")


class ClickHouseManifest:
    def __init__(self, path: Path, *, run_migrations: bool = False):
        self.path = Path(path)
        if run_migrations:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.connection() as connection:
                connection.executescript(SCHEMA)
        self._verify_schema()

    @contextmanager
    def connection(self) -> Iterable[Any]:
        connection = sqlite3.connect(str(self.path), timeout=30)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 30000")
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def _verify_schema(self) -> None:
        if not self.path.is_file():
            raise ClickHouseManifestError(
                f"ClickHouse manifest is not migrated: {self.path}"
            )
        with self.connection() as connection:
            row = connection.execute(
                "SELECT version FROM clickhouse_schema WHERE singleton = 1"
            ).fetchone()
        if row is None or int(row["version"]) != SCHEMA_VERSION:
            raise ClickHouseManifestError(
                "ClickHouse manifest schema version is unsupported"
            )

    def recover_loading(self) -> int:
        now_us = time.time_ns() // 1000
        with self.connection() as connection:
            load = connection.execute(
                """
                UPDATE clickhouse_parts
                SET status = 'pending', next_retry_us = 0,
                    last_error = 'worker restarted during ingestion',
                    updated_at_us = ?
                WHERE status = 'loading'
                """,
                (now_us,),
            ).rowcount
            delete = connection.execute(
                """
                UPDATE clickhouse_parts
                SET status = 'delete_pending', next_retry_us = 0,
                    last_error = 'worker restarted during deletion',
                    updated_at_us = ?
                WHERE status = 'deleting'
                """,
                (now_us,),
            ).rowcount
        return int(load) + int(delete)

    def part_status(self, part_path: str, logical_part_id: str) -> str | None:
        """Return one exact durable job state without changing its claim."""

        with self.connection() as connection:
            row = connection.execute(
                "SELECT status FROM clickhouse_parts "
                "WHERE part_path = ? AND logical_part_id = ?",
                (str(part_path), str(logical_part_id)),
            ).fetchone()
        return str(row["status"]) if row is not None else None

    def reconcile(
        self,
        parts: list[dict[str, Any]],
        *,
        start_epoch_us: int,
        end_epoch_us: int,
        source_parts: int | None = None,
        sweep_unseen: bool = True,
        preserve_reconcile_state: bool = False,
    ) -> dict[str, int]:
        now_us = time.time_ns() // 1000
        with self.connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            state = connection.execute(
                "SELECT generation FROM clickhouse_reconcile_state "
                "WHERE singleton = 1"
            ).fetchone()
            generation = int(state["generation"] if state else 0) + 1
            queued = 0
            replacements = 0
            for part in parts:
                identity = part_identity(part)
                if not identity:
                    continue
                path = str(part["path"])
                existing = connection.execute(
                    "SELECT status FROM clickhouse_parts "
                    "WHERE part_path = ? AND logical_part_id = ?",
                    (path, identity),
                ).fetchone()
                if existing is None or str(existing["status"]) not in {
                    "pending", "loading", "ready", "load_failed"
                }:
                    queued += 1
                connection.execute(
                    """
                    INSERT INTO clickhouse_parts(
                        part_path, logical_part_id, sha256, content_revision,
                        min_event_epoch_us, max_event_epoch_us, row_count,
                        size_bytes, status, attempts, next_retry_us,
                        inserted_rows, last_error, first_seen_us,
                        updated_at_us, ready_at_us, seen_generation
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, 0,
                             0, '', ?, ?, 0, ?)
                    ON CONFLICT(part_path, logical_part_id) DO UPDATE SET
                        sha256 = excluded.sha256,
                        content_revision = excluded.content_revision,
                        min_event_epoch_us = excluded.min_event_epoch_us,
                        max_event_epoch_us = excluded.max_event_epoch_us,
                        row_count = excluded.row_count,
                        size_bytes = excluded.size_bytes,
                        status = CASE WHEN clickhouse_parts.status IN
                            ('delete_pending', 'deleting', 'delete_failed', 'retired')
                            THEN 'pending' ELSE clickhouse_parts.status END,
                        attempts = CASE WHEN clickhouse_parts.status IN
                            ('delete_pending', 'deleting', 'delete_failed', 'retired')
                            THEN 0 ELSE clickhouse_parts.attempts END,
                        next_retry_us = CASE WHEN clickhouse_parts.status IN
                            ('delete_pending', 'deleting', 'delete_failed', 'retired')
                            THEN 0 ELSE clickhouse_parts.next_retry_us END,
                        inserted_rows = CASE WHEN clickhouse_parts.status IN
                            ('delete_pending', 'deleting', 'delete_failed', 'retired')
                            THEN 0 ELSE clickhouse_parts.inserted_rows END,
                        last_error = CASE WHEN clickhouse_parts.status IN
                            ('delete_pending', 'deleting', 'delete_failed', 'retired')
                            THEN '' ELSE clickhouse_parts.last_error END,
                        ready_at_us = CASE WHEN clickhouse_parts.status IN
                            ('delete_pending', 'deleting', 'delete_failed', 'retired')
                            THEN 0 ELSE clickhouse_parts.ready_at_us END,
                        updated_at_us = excluded.updated_at_us,
                        seen_generation = excluded.seen_generation
                    """,
                    (
                        path,
                        identity,
                        str(part.get("sha256") or ""),
                        int(part.get("content_revision") or 0),
                        int(part.get("min_event_epoch_us") or 0),
                        int(part.get("max_event_epoch_us") or 0),
                        int(part.get("row_count") or 0),
                        int(part.get("size_bytes") or 0),
                        now_us,
                        now_us,
                        generation,
                    ),
                )
                cursor = connection.execute(
                    """
                    UPDATE clickhouse_parts
                    SET status = CASE WHEN status = 'deleting'
                                      THEN status ELSE 'delete_pending' END,
                        next_retry_us = CASE WHEN status = 'deleting'
                                             THEN next_retry_us ELSE 0 END,
                        last_error = CASE WHEN status = 'deleting'
                                          THEN last_error ELSE '' END,
                        updated_at_us = ?
                    WHERE part_path = ? AND logical_part_id != ?
                      AND status != 'retired'
                    """,
                    (now_us, path, identity),
                )
                replacements += int(cursor.rowcount)

            # A full sweep owns deletion/retirement. Incremental reconciles use
            # a short moving window and must never interpret unscanned history
            # as deleted.
            deleted = 0
            aged_out = 0
            if sweep_unseen:
                deleted = connection.execute(
                    """
                    UPDATE clickhouse_parts
                    SET status = CASE WHEN status = 'deleting'
                                      THEN status ELSE 'delete_pending' END,
                        next_retry_us = CASE WHEN status = 'deleting'
                                             THEN next_retry_us ELSE 0 END,
                        updated_at_us = ?
                    WHERE seen_generation != ? AND max_event_epoch_us >= ?
                      AND status != 'retired'
                    """,
                    (now_us, generation, int(start_epoch_us)),
                ).rowcount
                aged_out = connection.execute(
                    """
                    UPDATE clickhouse_parts
                    SET status = 'retired', next_retry_us = 0,
                        last_error = '', updated_at_us = ?
                    WHERE seen_generation != ? AND max_event_epoch_us < ?
                      AND status != 'retired'
                    """,
                    (now_us, generation, int(start_epoch_us)),
                ).rowcount
            if preserve_reconcile_state:
                connection.execute(
                    """
                    UPDATE clickhouse_reconcile_state
                    SET generation = ?, last_error = ''
                    WHERE singleton = 1
                    """,
                    (generation,),
                )
            else:
                connection.execute(
                    """
                    UPDATE clickhouse_reconcile_state
                    SET generation = ?, start_epoch_us = ?, end_epoch_us = ?,
                        source_parts = ?, eligible_parts = ?,
                        completed_at_us = ?, last_error = ''
                    WHERE singleton = 1
                    """,
                    (
                        generation,
                        int(start_epoch_us),
                        int(end_epoch_us),
                        int(
                            source_parts
                            if source_parts is not None
                            else len(parts)
                        ),
                        len(parts),
                        now_us,
                    ),
                )
        return {
            "generation": generation,
            "eligible_parts": len(parts),
            "queued_parts": queued,
            "replacement_deletes": replacements,
            "missing_deletes": int(deleted),
            "aged_out_parts": int(aged_out),
        }

    def record_reconcile_error(self, error: str) -> None:
        with self.connection() as connection:
            connection.execute(
                "UPDATE clickhouse_reconcile_state SET last_error = ? "
                "WHERE singleton = 1",
                (str(error)[:2000],),
            )

    def claim_next(self, *, prefer_newest: bool = False) -> dict[str, Any] | None:
        """Claim one durable job without letting a moving hot window run away.

        Deletes and retries always retain priority.  Callers performing an
        initial bulk backfill may request newest-first loads so the queryable
        head converges before older rows approach the table TTL.  The default
        remains oldest-first for small steady-state queues.
        """
        now_us = time.time_ns() // 1000
        event_order = "DESC" if prefer_newest else "ASC"
        with self.connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                f"""
                SELECT * FROM clickhouse_parts
                WHERE status IN ('pending', 'load_failed',
                                 'delete_pending', 'delete_failed')
                  AND next_retry_us <= ?
                ORDER BY CASE WHEN status IN ('delete_pending', 'delete_failed')
                              THEN 0 ELSE 1 END,
                         CASE WHEN status IN ('load_failed', 'delete_failed')
                              THEN 0 ELSE 1 END,
                         next_retry_us,
                         max_event_epoch_us {event_order},
                         part_path ASC
                LIMIT 1
                """,
                (now_us,),
            ).fetchone()
            if row is None:
                return None
            deleting = str(row["status"]) in {
                "delete_pending", "delete_failed"
            }
            status = "deleting" if deleting else "loading"
            cursor = connection.execute(
                """
                UPDATE clickhouse_parts SET status = ?, updated_at_us = ?
                WHERE part_path = ? AND logical_part_id = ? AND status = ?
                """,
                (
                    status,
                    now_us,
                    str(row["part_path"]),
                    str(row["logical_part_id"]),
                    str(row["status"]),
                ),
            )
            if not cursor.rowcount:
                return None
        result = dict(row)
        result["job_kind"] = "delete" if deleting else "load"
        return result

    def mark_ready(
        self,
        part_path: str,
        logical_part_id: str,
        inserted_rows: int,
    ) -> bool:
        now_us = time.time_ns() // 1000
        with self.connection() as connection:
            cursor = connection.execute(
                """
                UPDATE clickhouse_parts
                SET status = 'ready', inserted_rows = ?, last_error = '',
                    next_retry_us = 0, ready_at_us = ?, updated_at_us = ?
                WHERE part_path = ? AND logical_part_id = ?
                  AND status IN ('loading', 'pending', 'load_failed')
                """,
                (
                    max(int(inserted_rows), 0),
                    now_us,
                    now_us,
                    str(part_path),
                    str(logical_part_id),
                ),
            )
        return bool(cursor.rowcount)

    def release_pending(
        self,
        part_path: str,
        logical_part_id: str,
        *,
        reason: str = "",
        delay_seconds: int = 0,
    ) -> bool:
        now_us = time.time_ns() // 1000
        with self.connection() as connection:
            row = connection.execute(
                "SELECT status FROM clickhouse_parts "
                "WHERE part_path = ? AND logical_part_id = ?",
                (str(part_path), str(logical_part_id)),
            ).fetchone()
            if row is None:
                return False
            pending = (
                "delete_pending"
                if str(row["status"]) in ACTIVE_DELETE_STATES
                else "pending"
            )
            cursor = connection.execute(
                """
                UPDATE clickhouse_parts
                SET status = ?, next_retry_us = ?, last_error = ?,
                    updated_at_us = ?
                WHERE part_path = ? AND logical_part_id = ?
                """,
                (
                    pending,
                    now_us + max(int(delay_seconds), 0) * 1_000_000,
                    str(reason)[:2000],
                    now_us,
                    str(part_path),
                    str(logical_part_id),
                ),
            )
        return bool(cursor.rowcount)

    def mark_failed(
        self,
        part_path: str,
        logical_part_id: str,
        error: str,
    ) -> bool:
        now_us = time.time_ns() // 1000
        with self.connection() as connection:
            row = connection.execute(
                "SELECT attempts, status FROM clickhouse_parts "
                "WHERE part_path = ? AND logical_part_id = ?",
                (str(part_path), str(logical_part_id)),
            ).fetchone()
            if row is None:
                return False
            attempts = int(row["attempts"] or 0) + 1
            delay_seconds = min(5 * (2 ** min(attempts - 1, 8)), 900)
            failed = (
                "delete_failed"
                if str(row["status"]) in ACTIVE_DELETE_STATES
                else "load_failed"
            )
            cursor = connection.execute(
                """
                UPDATE clickhouse_parts
                SET status = ?, attempts = ?, next_retry_us = ?,
                    last_error = ?, updated_at_us = ?
                WHERE part_path = ? AND logical_part_id = ?
                """,
                (
                    failed,
                    attempts,
                    now_us + delay_seconds * 1_000_000,
                    str(error)[:2000],
                    now_us,
                    str(part_path),
                    str(logical_part_id),
                ),
            )
        return bool(cursor.rowcount)

    def mark_retired(self, part_path: str, logical_part_id: str) -> bool:
        now_us = time.time_ns() // 1000
        with self.connection() as connection:
            cursor = connection.execute(
                """
                UPDATE clickhouse_parts
                SET status = 'retired', next_retry_us = 0,
                    inserted_rows = 0, last_error = '', updated_at_us = ?
                WHERE part_path = ? AND logical_part_id = ?
                """,
                (now_us, str(part_path), str(logical_part_id)),
            )
        return bool(cursor.rowcount)

    def queue_missing_paths(self, paths: Iterable[str]) -> int:
        """Queue every active identity for removed or no-longer-visible paths."""

        values = sorted({str(path) for path in paths if str(path)})
        if not values:
            return 0
        now_us = time.time_ns() // 1000
        changed = 0
        with self.connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            for offset in range(0, len(values), 400):
                chunk = values[offset : offset + 400]
                placeholders = ",".join("?" for _ in chunk)
                changed += int(
                    connection.execute(
                        """
                        UPDATE clickhouse_parts
                        SET status = CASE WHEN status = 'deleting'
                                          THEN status ELSE 'delete_pending' END,
                            next_retry_us = CASE WHEN status = 'deleting'
                                                 THEN next_retry_us ELSE 0 END,
                            last_error = CASE WHEN status = 'deleting'
                                              THEN last_error ELSE '' END,
                            updated_at_us = ?
                        """
                        f"WHERE part_path IN ({placeholders}) "
                        "AND status != 'retired'",
                        (now_us, *chunk),
                    ).rowcount
                )
        return changed

    def global_coverage(
        self,
        *,
        source_complete: bool,
        source_pending: bool,
    ) -> dict[str, Any]:
        """Return an O(log N) all-history serving gate.

        Source-side SQLite triggers make every mutation durable before it is
        visible to readers. Once that queue is empty, any remaining non-ready
        manifest state can be found with point seeks on the status-leading
        queue index. Exact per-part reconciliation remains an offline verifier,
        not a cost paid by every user query.
        """

        with self.connection() as connection:
            state = connection.execute(
                "SELECT * FROM clickhouse_reconcile_state WHERE singleton = 1"
            ).fetchone()
            unready = connection.execute(
                """
                SELECT status FROM clickhouse_parts
                WHERE status IN (
                    'pending', 'loading', 'load_failed',
                    'delete_pending', 'deleting', 'delete_failed'
                )
                LIMIT 1
                """
            ).fetchone()
        reconciled = bool(state and int(state["completed_at_us"] or 0))
        manifest_error = str(state["last_error"] or "") if state else ""
        complete = bool(
            source_complete
            and not source_pending
            and reconciled
            and not manifest_error
            and unready is None
        )
        total_parts = int(state["eligible_parts"] or 0) if state else 0
        return {
            "complete": complete,
            "total_parts": total_parts,
            "covered_parts": total_parts if complete else 0,
            "covered_rows": 0,
            "missing_parts": [],
            "source_complete": bool(source_complete),
            "source_pending": bool(source_pending),
            "manifest_pending": unready is not None,
            "reconcile_start_epoch_us": int(state["start_epoch_us"] or 0)
            if state
            else 0,
            "reconcile_end_epoch_us": int(state["end_epoch_us"] or 0)
            if state
            else 0,
            "reconcile_completed_at_us": int(state["completed_at_us"] or 0)
            if state
            else 0,
        }

    def coverage(self, parts: list[dict[str, Any]]) -> dict[str, Any]:
        expected = {
            str(part["path"]): part_identity(part)
            for part in parts
            if part_identity(part)
        }
        rows: dict[str, list[Any]] = {}
        with self.connection() as connection:
            paths = list(expected)
            for offset in range(0, len(paths), 400):
                chunk = paths[offset : offset + 400]
                if not chunk:
                    continue
                placeholders = ",".join("?" for _ in chunk)
                for row in connection.execute(
                    "SELECT part_path, logical_part_id, status, inserted_rows "
                    f"FROM clickhouse_parts WHERE part_path IN ({placeholders})",
                    chunk,
                ).fetchall():
                    rows.setdefault(str(row["part_path"]), []).append(row)
            state = connection.execute(
                "SELECT * FROM clickhouse_reconcile_state WHERE singleton = 1"
            ).fetchone()
        missing: list[str] = []
        covered_rows = 0
        for path, identity in expected.items():
            current = None
            stale_active = False
            for row in rows.get(path, []):
                if str(row["logical_part_id"]) == identity:
                    current = row
                elif str(row["status"]) != "retired":
                    stale_active = True
            if (
                current is None
                or str(current["status"]) != "ready"
                or stale_active
            ):
                missing.append(path)
            else:
                covered_rows += int(current["inserted_rows"] or 0)
        reconciled = bool(state and int(state["completed_at_us"] or 0))
        reconcile_start = int(state["start_epoch_us"] or 0) if state else 0
        reconcile_end = int(state["end_epoch_us"] or 0) if state else 0
        return {
            "complete": reconciled and not missing,
            "total_parts": len(expected),
            "covered_parts": len(expected) - len(missing),
            "covered_rows": covered_rows,
            "missing_parts": missing,
            "reconcile_start_epoch_us": reconcile_start,
            "reconcile_end_epoch_us": reconcile_end,
            "reconcile_completed_at_us": int(state["completed_at_us"] or 0)
            if state
            else 0,
        }

    def stats(self) -> dict[str, Any]:
        now_us = time.time_ns() // 1000
        with self.connection() as connection:
            counts = connection.execute(
                """
                SELECT
                    COUNT(*) AS tracked_parts,
                    SUM(status = 'ready') AS ready_parts,
                    SUM(status IN ('pending', 'loading', 'delete_pending',
                                   'deleting')) AS pending_parts,
                    SUM(status IN ('load_failed', 'delete_failed')) AS failed_parts,
                    SUM(status IN ('delete_pending', 'deleting',
                                   'delete_failed')) AS delete_parts,
                    COALESCE(SUM(CASE WHEN status = 'ready'
                                      THEN inserted_rows ELSE 0 END), 0)
                        AS ready_rows,
                    COALESCE(SUM(CASE WHEN status = 'ready'
                                      THEN size_bytes ELSE 0 END), 0)
                        AS ready_source_bytes,
                    COALESCE(MIN(CASE WHEN status IN
                        ('pending', 'loading', 'load_failed', 'delete_pending',
                         'deleting', 'delete_failed') THEN first_seen_us END), 0)
                        AS oldest_pending_us,
                    COALESCE(MAX(CASE WHEN status = 'ready'
                                      THEN max_event_epoch_us END), 0)
                        AS ready_watermark_us
                FROM clickhouse_parts
                """
            ).fetchone()
            state = connection.execute(
                "SELECT * FROM clickhouse_reconcile_state WHERE singleton = 1"
            ).fetchone()
        oldest = int(counts["oldest_pending_us"] or 0)
        return {
            "schema_version": SCHEMA_VERSION,
            "tracked_parts": int(counts["tracked_parts"] or 0),
            "ready_parts": int(counts["ready_parts"] or 0),
            "pending_parts": int(counts["pending_parts"] or 0),
            "failed_parts": int(counts["failed_parts"] or 0),
            "delete_parts": int(counts["delete_parts"] or 0),
            "ready_rows": int(counts["ready_rows"] or 0),
            "ready_source_bytes": int(counts["ready_source_bytes"] or 0),
            "oldest_pending_age_seconds": max((now_us - oldest) // 1_000_000, 0)
            if oldest
            else 0,
            "ready_watermark_us": int(counts["ready_watermark_us"] or 0),
            "reconcile_generation": int(state["generation"] or 0)
            if state
            else 0,
            "reconcile_start_epoch_us": int(state["start_epoch_us"] or 0)
            if state
            else 0,
            "reconcile_end_epoch_us": int(state["end_epoch_us"] or 0)
            if state
            else 0,
            "reconcile_completed_at_us": int(state["completed_at_us"] or 0)
            if state
            else 0,
            "last_error": str(state["last_error"] or "") if state else "",
        }
