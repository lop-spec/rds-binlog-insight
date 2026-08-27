from __future__ import annotations

import json
import sqlite3
import threading
import zlib
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator


CATALOG_STORE_SCHEMA_VERSION = 1
REQUIRED_RUNTIME_TABLES = frozenset({"backfill_state", "catalogs"})

CATALOG_STORE_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS catalogs (
    path TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL,
    content_revision INTEGER NOT NULL,
    payload_zlib BLOB NOT NULL,
    indexed_at TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS backfill_state (
    singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
    after_path TEXT NOT NULL DEFAULT '',
    complete INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL DEFAULT ''
);

INSERT OR IGNORE INTO backfill_state(singleton) VALUES(1);
"""


class CatalogStore:
    """Rebuildable compressed catalog data, isolated from metadata.sqlite3."""

    def __init__(self, path: Path, *, run_migrations: bool = True):
        self.path = path
        self._write_lock = threading.RLock()
        if not run_migrations:
            self._require_schema_version()
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as conn:
            row = conn.execute("PRAGMA user_version").fetchone()
            version = int(row[0] if row else 0)
            if version == CATALOG_STORE_SCHEMA_VERSION:
                conn.executescript(CATALOG_STORE_SCHEMA)
                return
            if version > CATALOG_STORE_SCHEMA_VERSION:
                raise RuntimeError(
                    f"Catalog schema version {version} is newer than supported "
                    f"version {CATALOG_STORE_SCHEMA_VERSION}"
                )
            conn.executescript(CATALOG_STORE_SCHEMA)
            conn.execute(
                f"PRAGMA user_version = {CATALOG_STORE_SCHEMA_VERSION}"
            )

    def _require_schema_version(self) -> None:
        if not self.path.is_file():
            raise RuntimeError(
                f"Catalog schema version {CATALOG_STORE_SCHEMA_VERSION} is "
                "required; the database does not exist"
            )
        with self.connection() as conn:
            row = conn.execute("PRAGMA user_version").fetchone()
            tables = {
                str(item["name"])
                for item in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                )
            }
        version = int(row[0] if row else 0)
        if version != CATALOG_STORE_SCHEMA_VERSION:
            raise RuntimeError(
                f"Catalog schema version {version} is not supported by this "
                f"runtime; migrate to version {CATALOG_STORE_SCHEMA_VERSION} "
                "first"
            )
        gaps = sorted(REQUIRED_RUNTIME_TABLES - tables)
        if gaps:
            raise RuntimeError(
                "Catalog schema is incomplete: "
                + ", ".join(f"table:{name}" for name in gaps)
                + "; run app.metadata_migrate first"
            )

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _encode(catalog: dict[str, Any]) -> bytes:
        payload = {
            "databases": sorted(set(catalog.get("databases") or [])),
            "tables": sorted(set(catalog.get("tables") or [])),
            "operations": sorted(set(catalog.get("operations") or [])),
        }
        raw = json.dumps(
            payload,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        return zlib.compress(raw, level=6)

    @staticmethod
    def _decode(path: str, payload: bytes) -> dict[str, Any]:
        try:
            value = json.loads(zlib.decompress(payload).decode("utf-8"))
        except (
            OSError,
            zlib.error,
            UnicodeDecodeError,
            json.JSONDecodeError,
        ) as exc:
            raise RuntimeError(f"Catalog payload is corrupt for {path}: {exc}") from exc
        return {
            "databases": list(value.get("databases") or []),
            "tables": list(value.get("tables") or []),
            "operations": list(value.get("operations") or []),
        }

    @classmethod
    def _encoded_rows(
        cls,
        entries: list[dict[str, Any]],
    ) -> list[tuple[str, str, int, bytes, str]]:
        return [
            (
                str(entry["path"]),
                str(entry["sha256"]),
                int(entry["content_revision"]),
                cls._encode(dict(entry["catalog"])),
                str(entry["indexed_at"]),
            )
            for entry in entries
        ]

    @staticmethod
    def _upsert_encoded(
        conn: sqlite3.Connection,
        rows: list[tuple[str, str, int, bytes, str]],
    ) -> None:
        if not rows:
            return
        conn.executemany(
            """
            INSERT INTO catalogs(
                path, sha256, content_revision, payload_zlib, indexed_at
            ) VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(path) DO UPDATE SET
                sha256 = excluded.sha256,
                content_revision = excluded.content_revision,
                payload_zlib = excluded.payload_zlib,
                indexed_at = excluded.indexed_at
            WHERE excluded.content_revision > catalogs.content_revision
               OR (
                   excluded.content_revision = catalogs.content_revision
                   AND excluded.indexed_at >= catalogs.indexed_at
               )
            """,
            rows,
        )

    def upsert_many(self, entries: list[dict[str, Any]]) -> None:
        rows = self._encoded_rows(entries)
        if not rows:
            return
        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._upsert_encoded(conn, rows)
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def catalogs(self, paths: list[str]) -> dict[str, dict[str, Any]]:
        if not paths:
            return {}
        result: dict[str, dict[str, Any]] = {}
        with self.connection() as conn:
            for offset in range(0, len(paths), 400):
                chunk = paths[offset : offset + 400]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    "SELECT path, sha256, content_revision, payload_zlib, "
                    "indexed_at FROM catalogs "
                    f"WHERE path IN ({placeholders})",
                    chunk,
                ).fetchall()
                for row in rows:
                    path = str(row["path"])
                    result[path] = {
                        "sha256": str(row["sha256"]),
                        "content_revision": int(row["content_revision"]),
                        **self._decode(path, bytes(row["payload_zlib"])),
                        "indexed_at": str(row["indexed_at"]),
                    }
        return result

    def apply_backfill_page(
        self,
        entries: list[dict[str, Any]],
        *,
        after_path: str,
        complete: bool,
        updated_at: str,
    ) -> None:
        rows = self._encoded_rows(entries)
        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                self._upsert_encoded(conn, rows)
                conn.execute(
                    "UPDATE backfill_state "
                    "SET after_path = ?, complete = ?, updated_at = ? "
                    "WHERE singleton = 1",
                    (after_path, 1 if complete else 0, updated_at),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def backfill_state(self) -> dict[str, Any]:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT after_path, complete, updated_at "
                "FROM backfill_state WHERE singleton = 1"
            ).fetchone()
        if not row:
            raise RuntimeError("Catalog backfill state is missing")
        return {
            "after_path": str(row["after_path"] or ""),
            "complete": bool(row["complete"]),
            "updated_at": str(row["updated_at"] or ""),
        }

    def update_sha(
        self,
        path: str,
        old_sha256: str,
        new_sha256: str,
        indexed_at: str,
    ) -> None:
        with self._write_lock, self.connection() as conn:
            conn.execute(
                "UPDATE catalogs SET sha256 = ?, indexed_at = ? "
                "WHERE path = ? AND sha256 = ?",
                (new_sha256, indexed_at, path, old_sha256),
            )

    def delete_paths(self, paths: list[str]) -> None:
        if not paths:
            return
        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for offset in range(0, len(paths), 400):
                    chunk = paths[offset : offset + 400]
                    placeholders = ",".join("?" for _ in chunk)
                    conn.execute(
                        f"DELETE FROM catalogs WHERE path IN ({placeholders})",
                        chunk,
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def stats(self) -> dict[str, int | bool]:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT COUNT(*) AS entries, "
                "COALESCE(SUM(length(payload_zlib)), 0) AS payload_bytes "
                "FROM catalogs"
            ).fetchone()
            state = conn.execute(
                "SELECT complete FROM backfill_state WHERE singleton = 1"
            ).fetchone()
        return {
            "entries": int(row["entries"] or 0),
            "payload_bytes": int(row["payload_bytes"] or 0),
            "backfill_complete": bool(state and state["complete"]),
        }
