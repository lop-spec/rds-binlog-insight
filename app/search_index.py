from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import threading
import time
from collections.abc import Iterable
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


INDEX_SCHEMA_VERSION = 2
KEYWORD_COLUMNS = (
    "sql_text",
    "before_json",
    "after_json",
    "transaction_id",
    "source_file_name",
)
INDEX_COLUMNS = (
    "event_epoch_us",
    "database_name",
    "table_name",
    "operation",
    *KEYWORD_COLUMNS,
)
_SEPARATOR = "\n\u241f\n"
_TOKEN_PATTERN = re.compile(r"\w+", re.UNICODE)

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;
PRAGMA busy_timeout=10000;

CREATE TABLE IF NOT EXISTS indexed_parts (
    path TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL,
    logical_part_id TEXT NOT NULL DEFAULT '',
    row_group_count INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    indexed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS structural_parts (
    path TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL,
    logical_part_id TEXT NOT NULL DEFAULT '',
    row_group_count INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    schema_version INTEGER NOT NULL,
    indexed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS blocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    part_path TEXT NOT NULL,
    part_sha256 TEXT NOT NULL,
    logical_part_id TEXT NOT NULL DEFAULT '',
    row_group_id INTEGER NOT NULL,
    min_event_epoch_us INTEGER NOT NULL,
    max_event_epoch_us INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    databases_json TEXT NOT NULL,
    tables_json TEXT NOT NULL,
    operations_json TEXT NOT NULL,
    UNIQUE(part_path, part_sha256, row_group_id)
);

CREATE INDEX IF NOT EXISTS idx_blocks_time
ON blocks(max_event_epoch_us DESC, min_event_epoch_us DESC);

CREATE INDEX IF NOT EXISTS idx_blocks_part
ON blocks(part_path, part_sha256, row_group_id);

CREATE TABLE IF NOT EXISTS index_failures (
    path TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL,
    logical_part_id TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL,
    retry_after_epoch REAL NOT NULL,
    failed_at TEXT NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS keyword_fts USING fts5(
    text,
    content='',
    contentless_delete=1,
    detail=none,
    tokenize='trigram'
);

CREATE VIRTUAL TABLE IF NOT EXISTS token_fts USING fts5(
    text,
    content='',
    contentless_delete=1,
    detail=none,
    tokenize='unicode61'
);

CREATE VIRTUAL TABLE IF NOT EXISTS database_fts USING fts5(
    text,
    content='',
    contentless_delete=1,
    detail=none,
    tokenize='trigram'
);

CREATE VIRTUAL TABLE IF NOT EXISTS table_fts USING fts5(
    text,
    content='',
    contentless_delete=1,
    detail=none,
    tokenize='trigram'
);
"""


def _utc_text() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_values(values: Iterable[str]) -> str:
    return json.dumps(
        sorted({str(value).lower() for value in values if str(value).strip()}),
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _query_trigrams(value: str) -> list[str]:
    normalized = value.lower()
    if len(normalized) < 3:
        return []
    return list(dict.fromkeys(normalized[index : index + 3] for index in range(len(normalized) - 2)))


def _quote_fts(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _token_key(value: str) -> str:
    digest = hashlib.blake2b(
        value.encode("utf-8"),
        digest_size=6,
        person=b"rdsidxv2",
    ).hexdigest()
    return "x" + digest


def _fts_expression(value: str) -> str:
    return " AND ".join(_quote_fts(value) for value in _query_trigrams(value))


class SearchIndex:
    """Minimal local index mapping predicates to immutable Parquet row groups."""

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._write_lock = threading.RLock()
        with self.connection() as conn:
            existing = conn.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE type = 'table' AND name = 'keyword_fts'"
            ).fetchone()
            if existing and "contentless_delete=1" not in str(existing[0]).replace(
                " ",
                "",
            ):
                conn.executescript(
                    """
                    DROP TABLE IF EXISTS keyword_fts;
                    DROP TABLE IF EXISTS token_fts;
                    DROP TABLE IF EXISTS database_fts;
                    DROP TABLE IF EXISTS table_fts;
                    DELETE FROM blocks;
                    DELETE FROM indexed_parts;
                    """
                )
            conn.executescript(SCHEMA)
            for table in (
                "indexed_parts",
                "structural_parts",
                "blocks",
                "index_failures",
            ):
                columns = {
                    str(row["name"])
                    for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
                }
                if "logical_part_id" not in columns:
                    conn.execute(
                        f"ALTER TABLE {table} ADD COLUMN logical_part_id "
                        "TEXT NOT NULL DEFAULT ''"
                    )
            conn.execute(
                """
                INSERT OR REPLACE INTO structural_parts(
                    path, sha256, logical_part_id, row_group_count, row_count,
                    schema_version, indexed_at
                )
                SELECT path, sha256, logical_part_id, row_group_count, row_count,
                       schema_version, indexed_at
                FROM indexed_parts
                """
            )

    @contextmanager
    def connection(self) -> Iterable[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=10000")
        try:
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _identity(part: dict[str, Any]) -> str:
        return str(part.get("logical_part_id") or part.get("sha256") or "")

    @classmethod
    def _identity_matches(
        cls,
        part: dict[str, Any],
        stored_logical_part_id: Any,
        stored_sha256: Any,
    ) -> bool:
        stored_logical = str(stored_logical_part_id or "")
        if stored_logical:
            return stored_logical == cls._identity(part)
        return str(stored_sha256 or "") == str(part.get("sha256") or "")

    def rebind_logical_parts(self, parts: list[dict[str, Any]]) -> int:
        values = [
            (
                self._identity(part),
                str(part["path"]),
            )
            for part in parts
            if self._identity(part)
        ]
        if not values:
            return 0
        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for table, path_column in (
                    ("indexed_parts", "path"),
                    ("structural_parts", "path"),
                    ("blocks", "part_path"),
                    ("index_failures", "path"),
                ):
                    conn.executemany(
                        f"UPDATE {table} SET logical_part_id = ? "
                        f"WHERE {path_column} = ? AND logical_part_id <> ?",
                        [(identity, path, identity) for identity, path in values],
                    )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return len(values)

    def is_current(self, part: dict[str, Any]) -> bool:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT sha256, logical_part_id, row_group_count, schema_version
                FROM indexed_parts
                WHERE path = ?
                """,
                (str(part["path"]),),
            ).fetchone()
        return bool(
            row
            and self._identity_matches(
                part, row["logical_part_id"], row["sha256"]
            )
            and int(row["row_group_count"]) > 0
            and int(row["schema_version"]) == INDEX_SCHEMA_VERSION
        )

    def is_structural_current(self, part: dict[str, Any]) -> bool:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT sha256, logical_part_id, row_group_count, schema_version
                FROM structural_parts
                WHERE path = ?
                """,
                (str(part["path"]),),
            ).fetchone()
        return bool(
            row
            and self._identity_matches(
                part, row["logical_part_id"], row["sha256"]
            )
            and int(row["row_group_count"]) > 0
            and int(row["schema_version"]) == INDEX_SCHEMA_VERSION
        )

    @staticmethod
    def _column_values(table: Any, name: str) -> list[Any]:
        if name not in table.column_names:
            return []
        return table.column(name).to_pylist()

    @staticmethod
    def _delete_fts_rows(
        conn: sqlite3.Connection,
        block_ids: Iterable[int],
    ) -> None:
        values = [(int(block_id),) for block_id in block_ids]
        if not values:
            return
        for table in (
            "keyword_fts",
            "token_fts",
            "database_fts",
            "table_fts",
        ):
            conn.executemany(f"DELETE FROM {table} WHERE rowid = ?", values)

    def _structural_blocks(
        self,
        part: dict[str, Any],
        source: str | Path | Any,
    ) -> list[dict[str, Any]]:
        parquet = (
            source
            if isinstance(source, pq.ParquetFile)
            else pq.ParquetFile(source)
        )
        available = set(parquet.schema_arrow.names)
        columns = (
            "event_epoch_us",
            "database_name",
            "table_name",
            "operation",
        )
        if not set(columns).issubset(available):
            raise ValueError("Parquet 缺少结构索引列")
        blocks: list[dict[str, Any]] = []
        total_rows = 0
        for row_group_id in range(parquet.num_row_groups):
            table = parquet.read_row_group(
                row_group_id,
                columns=list(columns),
                use_threads=False,
            )
            epochs = [
                int(value)
                for value in self._column_values(table, "event_epoch_us")
                if value is not None
            ]
            if len(epochs) != int(table.num_rows) or not epochs:
                raise ValueError("结构索引事件时间列存在空值")
            databases = {
                str(value).lower()
                for value in self._column_values(table, "database_name")
                if str(value or "").strip()
            }
            tables = {
                str(value).lower()
                for value in self._column_values(table, "table_name")
                if str(value or "").strip()
            }
            operations = {
                str(value).upper()
                for value in self._column_values(table, "operation")
                if str(value or "").strip()
            }
            total_rows += int(table.num_rows)
            blocks.append(
                {
                    "row_group_id": row_group_id,
                    "min_event_epoch_us": min(epochs),
                    "max_event_epoch_us": max(epochs),
                    "row_count": int(table.num_rows),
                    "databases": databases,
                    "tables": tables,
                    "operations": operations,
                }
            )
        if not blocks or total_rows != int(part["row_count"]):
            raise ValueError("结构索引 Row Group 总行数与 Parquet 元数据不一致")
        return blocks

    def upsert_structural_blocks(
        self,
        part: dict[str, Any],
        blocks: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if self.is_structural_current(part):
            return {"indexed": 0, "row_groups": 0, "rows": 0}
        total_rows = sum(int(block["row_count"]) for block in blocks)
        if not blocks or total_rows != int(part["row_count"]):
            raise ValueError("结构索引 Row Group 总行数与分区元数据不一致")
        now = _utc_text()
        path = str(part["path"])
        sha256 = str(part["sha256"])
        identity = self._identity(part)
        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                full = conn.execute(
                    "SELECT sha256, logical_part_id, schema_version "
                    "FROM indexed_parts WHERE path = ?",
                    (path,),
                ).fetchone()
                if (
                    full
                    and self._identity_matches(
                        part, full["logical_part_id"], full["sha256"]
                    )
                    and int(full["schema_version"]) == INDEX_SCHEMA_VERSION
                ):
                    conn.execute("ROLLBACK")
                    return {"indexed": 0, "row_groups": 0, "rows": 0}
                old_ids = [
                    int(row[0])
                    for row in conn.execute(
                        "SELECT id FROM blocks WHERE part_path = ?",
                        (path,),
                    ).fetchall()
                ]
                self._delete_fts_rows(conn, old_ids)
                conn.execute("DELETE FROM blocks WHERE part_path = ?", (path,))
                conn.execute("DELETE FROM indexed_parts WHERE path = ?", (path,))
                conn.execute("DELETE FROM structural_parts WHERE path = ?", (path,))
                for block in blocks:
                    cursor = conn.execute(
                        """
                        INSERT INTO blocks(
                            part_path, part_sha256, logical_part_id, row_group_id,
                            min_event_epoch_us, max_event_epoch_us, row_count,
                            databases_json, tables_json, operations_json
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            path,
                            sha256,
                            identity,
                            int(block["row_group_id"]),
                            int(block["min_event_epoch_us"]),
                            int(block["max_event_epoch_us"]),
                            int(block["row_count"]),
                            _json_values(block["databases"]),
                            _json_values(block["tables"]),
                            json.dumps(
                                sorted(block["operations"]),
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ),
                        ),
                    )
                    block_id = int(cursor.lastrowid)
                    conn.execute(
                        "INSERT INTO keyword_fts(rowid, text) VALUES(?, '')",
                        (block_id,),
                    )
                    conn.execute(
                        "INSERT INTO token_fts(rowid, text) VALUES(?, '')",
                        (block_id,),
                    )
                    conn.execute(
                        "INSERT INTO database_fts(rowid, text) VALUES(?, ?)",
                        (block_id, _SEPARATOR.join(sorted(block["databases"]))),
                    )
                    conn.execute(
                        "INSERT INTO table_fts(rowid, text) VALUES(?, ?)",
                        (block_id, _SEPARATOR.join(sorted(block["tables"]))),
                    )
                conn.execute(
                    """
                    INSERT INTO structural_parts(
                        path, sha256, logical_part_id, row_group_count, row_count,
                        schema_version, indexed_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        path,
                        sha256,
                        identity,
                        len(blocks),
                        total_rows,
                        INDEX_SCHEMA_VERSION,
                        now,
                    ),
                )
                conn.execute("DELETE FROM index_failures WHERE path = ?", (path,))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return {
            "indexed": 1,
            "row_groups": len(blocks),
            "rows": total_rows,
            "catalog": {
                "databases": sorted(
                    {value for block in blocks for value in block["databases"]}
                ),
                "tables": sorted(
                    {value for block in blocks for value in block["tables"]}
                ),
                "operations": sorted(
                    {value for block in blocks for value in block["operations"]}
                ),
            },
        }

    def index_structural_parquet(
        self,
        part: dict[str, Any],
        source: str | Path | Any,
    ) -> dict[str, Any]:
        if self.is_structural_current(part):
            return {"indexed": 0, "row_groups": 0, "rows": 0}
        return self.upsert_structural_blocks(
            part,
            self._structural_blocks(part, source),
        )

    def index_parquet(
        self,
        part: dict[str, Any],
        source: str | Path | Any,
    ) -> dict[str, Any]:
        if self.is_current(part):
            return {"indexed": 0, "row_groups": 0, "rows": 0}
        parquet = pq.ParquetFile(source)
        available = set(parquet.schema_arrow.names)
        required = {"event_epoch_us"}
        if not required.issubset(available):
            raise ValueError("Parquet 缺少索引所需事件时间列")
        columns = [name for name in INDEX_COLUMNS if name in available]
        blocks: list[dict[str, Any]] = []
        total_rows = 0
        for row_group_id in range(parquet.num_row_groups):
            table = parquet.read_row_group(
                row_group_id,
                columns=columns,
                # Parallelism is managed across immutable Parquet parts by the
                # background scheduler. Per-column Arrow threads here would
                # multiply into hundreds of runnable threads and starve the
                # HTTP query/status path.
                use_threads=False,
            )
            epochs = [
                int(value)
                for value in self._column_values(table, "event_epoch_us")
                if value is not None
            ]
            if not epochs:
                continue
            databases = {
                str(value).lower()
                for value in self._column_values(table, "database_name")
                if str(value or "").strip()
            }
            tables = {
                str(value).lower()
                for value in self._column_values(table, "table_name")
                if str(value or "").strip()
            }
            operations = {
                str(value).upper()
                for value in self._column_values(table, "operation")
                if str(value or "").strip()
            }
            keyword_values: list[str] = []
            for name in KEYWORD_COLUMNS:
                keyword_values.extend(
                    str(value)
                    for value in self._column_values(table, name)
                    if str(value or "")
                )
            keyword_tokens = {
                token
                for value in keyword_values
                for token in _TOKEN_PATTERN.findall(value.lower())
                if 6 <= len(token) <= 128
            }
            row_count = len(epochs)
            total_rows += row_count
            blocks.append(
                {
                    "row_group_id": row_group_id,
                    "min_event_epoch_us": min(epochs),
                    "max_event_epoch_us": max(epochs),
                    "row_count": row_count,
                    "databases": databases,
                    "tables": tables,
                    "operations": operations,
                    "keyword_text": _SEPARATOR.join(keyword_values),
                    "token_text": " ".join(
                        sorted(_token_key(token) for token in keyword_tokens)
                    ),
                    "database_text": _SEPARATOR.join(sorted(databases)),
                    "table_text": _SEPARATOR.join(sorted(tables)),
                }
            )
        if not blocks or total_rows != int(part["row_count"]):
            raise ValueError("索引 Row Group 总行数与 Parquet 元数据不一致")
        now = _utc_text()
        path = str(part["path"])
        sha256 = str(part["sha256"])
        identity = self._identity(part)
        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                old_ids = [
                    int(row[0])
                    for row in conn.execute(
                        "SELECT id FROM blocks WHERE part_path = ?",
                        (path,),
                    ).fetchall()
                ]
                self._delete_fts_rows(conn, old_ids)
                conn.execute("DELETE FROM blocks WHERE part_path = ?", (path,))
                conn.execute("DELETE FROM indexed_parts WHERE path = ?", (path,))
                conn.execute("DELETE FROM structural_parts WHERE path = ?", (path,))
                for block in blocks:
                    cursor = conn.execute(
                        """
                        INSERT INTO blocks(
                            part_path, part_sha256, logical_part_id, row_group_id,
                            min_event_epoch_us, max_event_epoch_us, row_count,
                            databases_json, tables_json, operations_json
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            path,
                            sha256,
                            identity,
                            block["row_group_id"],
                            block["min_event_epoch_us"],
                            block["max_event_epoch_us"],
                            block["row_count"],
                            _json_values(block["databases"]),
                            _json_values(block["tables"]),
                            json.dumps(
                                sorted(block["operations"]),
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ),
                        ),
                    )
                    block_id = int(cursor.lastrowid)
                    conn.execute(
                        "INSERT INTO keyword_fts(rowid, text) VALUES(?, ?)",
                        (block_id, block["keyword_text"]),
                    )
                    conn.execute(
                        "INSERT INTO token_fts(rowid, text) VALUES(?, ?)",
                        (block_id, block["token_text"]),
                    )
                    conn.execute(
                        "INSERT INTO database_fts(rowid, text) VALUES(?, ?)",
                        (block_id, block["database_text"]),
                    )
                    conn.execute(
                        "INSERT INTO table_fts(rowid, text) VALUES(?, ?)",
                        (block_id, block["table_text"]),
                    )
                conn.execute(
                    """
                    INSERT INTO indexed_parts(
                        path, sha256, logical_part_id, row_group_count, row_count,
                        schema_version, indexed_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        path,
                        sha256,
                        identity,
                        len(blocks),
                        total_rows,
                        INDEX_SCHEMA_VERSION,
                        now,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO structural_parts(
                        path, sha256, logical_part_id, row_group_count, row_count,
                        schema_version, indexed_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        path,
                        sha256,
                        identity,
                        len(blocks),
                        total_rows,
                        INDEX_SCHEMA_VERSION,
                        now,
                    ),
                )
                conn.execute("DELETE FROM index_failures WHERE path = ?", (path,))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return {
            "indexed": 1,
            "row_groups": len(blocks),
            "rows": total_rows,
            "catalog": {
                "databases": sorted(
                    {
                        value
                        for block in blocks
                        for value in block["databases"]
                    }
                ),
                "tables": sorted(
                    {
                        value
                        for block in blocks
                        for value in block["tables"]
                    }
                ),
                "operations": sorted(
                    {
                        value
                        for block in blocks
                        for value in block["operations"]
                    }
                ),
            },
        }

    def record_failure(
        self,
        part: dict[str, Any],
        error: Exception,
        *,
        retry_seconds: int = 300,
    ) -> None:
        with self._write_lock, self.connection() as conn:
            conn.execute(
                """
                INSERT INTO index_failures(
                    path, sha256, logical_part_id, error,
                    retry_after_epoch, failed_at
                ) VALUES(?, ?, ?, ?, ?, ?)
                ON CONFLICT(path) DO UPDATE SET
                    sha256 = excluded.sha256,
                    logical_part_id = excluded.logical_part_id,
                    error = excluded.error,
                    retry_after_epoch = excluded.retry_after_epoch,
                    failed_at = excluded.failed_at
                """,
                (
                    str(part["path"]),
                    str(part["sha256"]),
                    self._identity(part),
                    str(error)[:1000],
                    time.time() + max(int(retry_seconds), 1),
                    _utc_text(),
                ),
            )

    @staticmethod
    def _part_map(parts: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        return {str(part["path"]): part for part in parts}

    def _coverage_table(
        self,
        parts: list[dict[str, Any]],
        table: str,
    ) -> tuple[set[str], set[str]]:
        if not parts:
            return set(), set()
        expected = {str(part["path"]): part for part in parts}
        covered: set[str] = set()
        with self.connection() as conn:
            paths = list(expected)
            for offset in range(0, len(paths), 400):
                chunk = paths[offset : offset + 400]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"SELECT path, sha256, logical_part_id, schema_version FROM {table} "
                    f"WHERE path IN ({placeholders})",
                    chunk,
                ).fetchall()
                for row in rows:
                    path = str(row["path"])
                    if (
                        expected.get(path) is not None
                        and self._identity_matches(
                            expected[path],
                            row["logical_part_id"],
                            row["sha256"],
                        )
                        and int(row["schema_version"]) == INDEX_SCHEMA_VERSION
                    ):
                        covered.add(path)
        return covered, set(expected) - covered

    def _coverage(
        self,
        parts: list[dict[str, Any]],
    ) -> tuple[set[str], set[str]]:
        return self._coverage_table(parts, "indexed_parts")

    def _structural_coverage(
        self,
        parts: list[dict[str, Any]],
    ) -> tuple[set[str], set[str]]:
        return self._coverage_table(parts, "structural_parts")

    @staticmethod
    def _fts_ids(
        conn: sqlite3.Connection,
        table: str,
        value: str,
    ) -> set[int] | None:
        expression = _fts_expression(value)
        if not expression:
            return None
        rows = conn.execute(
            f"SELECT rowid FROM {table} WHERE {table} MATCH ?",
            (expression,),
        ).fetchall()
        return {int(row[0]) for row in rows}

    @staticmethod
    def _token_ids(
        conn: sqlite3.Connection,
        value: str,
    ) -> set[int] | None:
        normalized = value.strip().lower()
        if (
            not 6 <= len(normalized) <= 128
            or _TOKEN_PATTERN.fullmatch(normalized) is None
        ):
            return None
        rows = conn.execute(
            "SELECT rowid FROM token_fts WHERE token_fts MATCH ?",
            (_token_key(normalized),),
        ).fetchall()
        return {int(row[0]) for row in rows}

    def candidate_blocks(
        self,
        parts: list[dict[str, Any]],
        query: dict[str, Any],
        *,
        start_epoch_us: int,
        end_epoch_us: int,
    ) -> dict[str, Any]:
        part_map = self._part_map(parts)
        full_covered, _full_unknown = self._coverage(parts)
        structural_covered, _structural_unknown = self._structural_coverage(parts)
        covered = full_covered | structural_covered
        unknown = set(part_map) - covered
        if not covered:
            return {
                "entries": [],
                "covered_paths": covered,
                "full_covered_paths": full_covered,
                "structural_covered_paths": set(),
                "unknown_paths": unknown,
                "skipped_parts": 0,
            }
        database = str(query.get("database") or "").strip().lower()
        table = str(query.get("table") or "").strip().lower()
        operations = {
            str(value).strip().upper()
            for value in (query.get("operations") or [])
            if str(value).strip()
        }
        terms = [
            value.lower()
            for value in str(query.get("keyword") or "").split()
            if value
        ][:20]
        mode_or = bool(
            terms and str(query.get("keyword_mode") or "").upper() == "OR"
        )
        with self.connection() as conn:
            structural_ids: set[int] | None = None

            def intersect_structural(value: set[int] | None) -> None:
                nonlocal structural_ids
                if value is None:
                    return
                structural_ids = (
                    value
                    if structural_ids is None
                    else structural_ids & value
                )

            intersect_structural(
                self._fts_ids(conn, "database_fts", database)
                if database
                else None
            )
            intersect_structural(
                self._fts_ids(conn, "table_fts", table) if table else None
            )
            full_ids = (
                set(structural_ids) if structural_ids is not None else None
            )

            def intersect_full(value: set[int] | None) -> None:
                nonlocal full_ids
                if value is None:
                    return
                full_ids = value if full_ids is None else full_ids & value

            if terms:
                term_sets: list[set[int] | None] = []
                for term in terms:
                    token_ids = self._token_ids(conn, term)
                    trigram_ids = self._fts_ids(conn, "keyword_fts", term)
                    if token_ids is None:
                        term_sets.append(trigram_ids)
                    elif trigram_ids is None:
                        term_sets.append(token_ids)
                    else:
                        # Token lookup is an accelerator, not a semantic
                        # substitute: the public query contract is arbitrary
                        # substring matching, including a term embedded inside
                        # a longer token.
                        term_sets.append(token_ids | trigram_ids)
                if mode_or:
                    if all(value is not None for value in term_sets):
                        combined: set[int] = set()
                        for value in term_sets:
                            combined.update(value or set())
                        intersect_full(combined)
                else:
                    for value in term_sets:
                        intersect_full(value)
            rows = conn.execute(
                """
                SELECT *
                FROM blocks
                WHERE max_event_epoch_us >= ?
                  AND min_event_epoch_us <= ?
                ORDER BY max_event_epoch_us DESC,
                         min_event_epoch_us DESC,
                         id DESC
                """,
                (int(start_epoch_us), int(end_epoch_us)),
            ).fetchall()
        entries: list[dict[str, Any]] = []
        candidate_paths: set[str] = set()
        for row in rows:
            block_id = int(row["id"])
            path = str(row["part_path"])
            part = part_map.get(path)
            if (
                path not in covered
                or part is None
                or not self._identity_matches(
                    part,
                    row["logical_part_id"],
                    row["part_sha256"],
                )
            ):
                continue
            complete = path in full_covered
            allowed_ids = full_ids if complete else structural_ids
            if allowed_ids is not None and block_id not in allowed_ids:
                continue
            databases = json.loads(str(row["databases_json"]))
            tables = json.loads(str(row["tables_json"]))
            row_operations = set(json.loads(str(row["operations_json"])))
            if database and not any(database in str(value) for value in databases):
                continue
            if table and not any(table in str(value) for value in tables):
                continue
            if operations and operations.isdisjoint(row_operations):
                continue
            candidate_paths.add(path)
            entries.append(
                {
                    "path": path,
                    "part": part,
                    "row_group_id": int(row["row_group_id"]),
                    "min_event_epoch_us": int(row["min_event_epoch_us"]),
                    "max_event_epoch_us": int(row["max_event_epoch_us"]),
                    "complete": complete,
                }
            )
        return {
            "entries": entries,
            "covered_paths": covered,
            "full_covered_paths": full_covered,
            "structural_covered_paths": covered - full_covered,
            "unknown_paths": unknown,
            "skipped_parts": len(covered - candidate_paths),
        }

    def missing_parts(
        self,
        parts: list[dict[str, Any]],
        *,
        limit: int = 1,
    ) -> list[dict[str, Any]]:
        covered, unknown = self._coverage(parts)
        if not unknown:
            return []
        blocked: set[str] = set()
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT path, sha256, logical_part_id FROM index_failures "
                "WHERE retry_after_epoch > ?",
                (time.time(),),
            ).fetchall()
            expected = self._part_map(parts)
            for row in rows:
                path = str(row["path"])
                part = expected.get(path)
                if part and self._identity_matches(
                    part, row["logical_part_id"], row["sha256"]
                ):
                    blocked.add(path)
        candidates = [
            part
            for part in parts
            if str(part["path"]) in unknown
            and str(part["path"]) not in covered
            and str(part["path"]) not in blocked
        ]
        candidates.sort(
            key=lambda part: (
                0 if Path(str(part["path"])).is_file() else 1,
                -int(part["max_event_epoch_us"]),
                str(part["path"]),
            )
        )
        return candidates[: max(int(limit), 0)]

    def missing_structural_parts(
        self,
        parts: list[dict[str, Any]],
        *,
        limit: int = 1,
    ) -> list[dict[str, Any]]:
        covered, unknown = self._structural_coverage(parts)
        if not unknown:
            return []
        blocked: set[str] = set()
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT path, sha256, logical_part_id FROM index_failures "
                "WHERE retry_after_epoch > ?",
                (time.time(),),
            ).fetchall()
            expected = self._part_map(parts)
            for row in rows:
                path = str(row["path"])
                part = expected.get(path)
                if part and self._identity_matches(
                    part, row["logical_part_id"], row["sha256"]
                ):
                    blocked.add(path)
        candidates = [
            part
            for part in parts
            if str(part["path"]) in unknown
            and str(part["path"]) not in covered
            and str(part["path"]) not in blocked
        ]
        candidates.sort(
            key=lambda part: (
                0 if Path(str(part["path"])).is_file() else 1,
                -int(part["max_event_epoch_us"]),
                str(part["path"]),
            )
        )
        return candidates[: max(int(limit), 0)]

    def remove_part(self, path: str) -> None:
        with self._write_lock, self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                block_ids = [
                    int(row[0])
                    for row in conn.execute(
                        "SELECT id FROM blocks WHERE part_path = ?",
                        (path,),
                    ).fetchall()
                ]
                self._delete_fts_rows(conn, block_ids)
                conn.execute("DELETE FROM blocks WHERE part_path = ?", (path,))
                conn.execute("DELETE FROM indexed_parts WHERE path = ?", (path,))
                conn.execute("DELETE FROM structural_parts WHERE path = ?", (path,))
                conn.execute("DELETE FROM index_failures WHERE path = ?", (path,))
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def stats(self) -> dict[str, Any]:
        with self.connection() as conn:
            part_count = int(
                conn.execute("SELECT count(*) FROM indexed_parts").fetchone()[0]
            )
            structural_part_count = int(
                conn.execute("SELECT count(*) FROM structural_parts").fetchone()[0]
            )
            block_count = int(conn.execute("SELECT count(*) FROM blocks").fetchone()[0])
            row_count = int(
                conn.execute(
                    "SELECT coalesce(sum(row_count), 0) FROM indexed_parts"
                ).fetchone()[0]
            )
            last_indexed = str(
                conn.execute(
                    "SELECT coalesce(max(indexed_at), '') FROM indexed_parts"
                ).fetchone()[0]
            )
            last_structural_indexed = str(
                conn.execute(
                    "SELECT coalesce(max(indexed_at), '') FROM structural_parts"
                ).fetchone()[0]
            )
        size_bytes = 0
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(self.path) + suffix)
            if candidate.is_file():
                size_bytes += candidate.stat().st_size
        return {
            "schema_version": INDEX_SCHEMA_VERSION,
            "part_count": part_count,
            "structural_part_count": structural_part_count,
            "block_count": block_count,
            "row_count": row_count,
            "size_bytes": size_bytes,
            "last_indexed_at": last_indexed,
            "last_structural_indexed_at": last_structural_indexed,
        }
