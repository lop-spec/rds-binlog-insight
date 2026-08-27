from __future__ import annotations

import base64
import hashlib
import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Iterator

import pyarrow.parquet as pq
import pyarrow.compute as pc


EXACT_INDEX_FORMAT_VERSION = 1
SCHEMA_REGISTRY_FORMAT_VERSION = 1
DEFAULT_SCHEMA_REGISTRY = Path(__file__).with_name("exact_schema_registry.json")
ROW_OPERATIONS = {"INSERT", "UPDATE", "DELETE"}
INTEGER_TYPE_IDS = {1, 2, 3, 8, 9, 13}
BINARY_TYPE_IDS = {16, 249, 250, 251, 252}
SUPPORTED_TYPE_IDS = INTEGER_TYPE_IDS | BINARY_TYPE_IDS


MANIFEST_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=10000;

CREATE TABLE IF NOT EXISTS segments (
    id TEXT PRIMARY KEY,
    file_name TEXT NOT NULL UNIQUE,
    sha256 TEXT NOT NULL,
    registry_sha256 TEXT NOT NULL,
    format_version INTEGER NOT NULL,
    min_event_epoch_us INTEGER NOT NULL,
    max_event_epoch_us INTEGER NOT NULL,
    part_count INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    exact_doc_count INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS segment_parts (
    part_path TEXT PRIMARY KEY,
    segment_id TEXT NOT NULL REFERENCES segments(id),
    logical_part_id TEXT NOT NULL,
    object_sha256 TEXT NOT NULL,
    row_layout_fingerprint TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    min_event_epoch_us INTEGER NOT NULL,
    max_event_epoch_us INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_exact_segment_parts_identity
ON segment_parts(logical_part_id);

CREATE INDEX IF NOT EXISTS idx_exact_segment_parts_segment
ON segment_parts(segment_id);

CREATE TABLE IF NOT EXISTS part_tables (
    part_path TEXT NOT NULL,
    segment_id TEXT NOT NULL REFERENCES segments(id),
    logical_part_id TEXT NOT NULL,
    database_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    schema_version_id TEXT NOT NULL,
    column_ordinal INTEGER NOT NULL,
    column_name TEXT NOT NULL,
    type_id INTEGER NOT NULL,
    coverage_state TEXT NOT NULL,
    row_events INTEGER NOT NULL,
    indexed_events INTEGER NOT NULL,
    PRIMARY KEY(
        part_path, database_name, table_name, schema_version_id,
        column_ordinal, type_id
    )
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS idx_exact_part_tables_lookup
ON part_tables(database_name, table_name, part_path, coverage_state);
"""


SEGMENT_SCHEMA = """
PRAGMA journal_mode=OFF;
PRAGMA synchronous=OFF;
PRAGMA temp_store=MEMORY;
PRAGMA locking_mode=EXCLUSIVE;

CREATE TABLE metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;

CREATE TABLE docs (
    doc_id INTEGER PRIMARY KEY,
    event_id TEXT NOT NULL,
    event_epoch_us INTEGER NOT NULL,
    operation TEXT NOT NULL,
    database_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    gtid TEXT NOT NULL,
    source_file_name TEXT NOT NULL,
    host_instance_id TEXT NOT NULL,
    server_id INTEGER NOT NULL,
    thread_id INTEGER NOT NULL,
    start_position INTEGER NOT NULL,
    end_position INTEGER NOT NULL,
    row_index INTEGER NOT NULL,
    raw_event_type TEXT NOT NULL,
    logical_part_id TEXT NOT NULL,
    row_group_id INTEGER NOT NULL,
    row_offset INTEGER NOT NULL,
    schema_version_id TEXT NOT NULL,
    primary_before_json TEXT NOT NULL,
    primary_after_json TEXT NOT NULL
);

CREATE INDEX idx_exact_docs_order
ON docs(event_epoch_us DESC, source_file_name DESC, end_position DESC, row_index DESC);

CREATE TABLE exact_values (
    database_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    schema_version_id TEXT NOT NULL,
    column_ordinal INTEGER NOT NULL,
    type_id INTEGER NOT NULL,
    value_hash BLOB NOT NULL,
    value_key BLOB NOT NULL,
    doc_id INTEGER NOT NULL,
    PRIMARY KEY(
        database_name, table_name, schema_version_id, column_ordinal,
        type_id, value_hash, value_key, doc_id
    )
) WITHOUT ROWID;

CREATE TABLE part_tables (
    part_path TEXT NOT NULL,
    logical_part_id TEXT NOT NULL,
    database_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    schema_version_id TEXT NOT NULL,
    column_ordinal INTEGER NOT NULL,
    column_name TEXT NOT NULL,
    type_id INTEGER NOT NULL,
    coverage_state TEXT NOT NULL,
    row_events INTEGER NOT NULL,
    indexed_events INTEGER NOT NULL,
    PRIMARY KEY(
        part_path, database_name, table_name, schema_version_id,
        column_ordinal, type_id
    )
) WITHOUT ROWID;
"""


_MISSING = object()


def _utc_text() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _json(value: Any) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _identity(part: dict[str, Any]) -> str:
    return str(part.get("logical_part_id") or part.get("sha256") or "")


def _object_sha(part: dict[str, Any]) -> str:
    return str(
        part.get("oss_object_sha256")
        or part.get("object_sha256")
        or part.get("sha256")
        or ""
    )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _schema_version(
    row: dict[str, Any],
    columns: list[dict[str, Any]],
) -> str:
    explicit = str(row.get("schema_version_id") or "").strip()
    if explicit:
        return explicit
    return _schema_signature(row, columns)


def _schema_signature(
    row: dict[str, Any],
    columns: list[dict[str, Any]],
) -> str:
    payload = {
        "database": str(row.get("database_name") or "").lower(),
        "table": str(row.get("table_name") or "").lower(),
        "columns": columns,
    }
    return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()


def _load_schema_registry(
    path: Path | None,
) -> tuple[dict[tuple[str, str, str], dict[str, Any]], str]:
    configured_path = os.environ.get("RDS_BINLOG_EXACT_SCHEMA_REGISTRY", "").strip()
    registry_path = (
        Path(path)
        if path is not None
        else Path(configured_path)
        if configured_path
        else DEFAULT_SCHEMA_REGISTRY
    )
    if configured_path and path is None and not registry_path.is_file():
        raise FileNotFoundError(
            f"Configured exact schema registry does not exist: {registry_path}"
        )
    if registry_path.is_file():
        payload = json.loads(registry_path.read_text(encoding="utf-8"))
    else:
        payload = {"format_version": SCHEMA_REGISTRY_FORMAT_VERSION, "mappings": []}
    if int(payload.get("format_version") or 0) != SCHEMA_REGISTRY_FORMAT_VERSION:
        raise ValueError("精确索引 schema registry 版本不受支持")
    mappings: dict[tuple[str, str, str], dict[str, Any]] = {}
    for raw in payload.get("mappings") or []:
        if not isinstance(raw, dict) or not isinstance(raw.get("primary_key"), dict):
            raise ValueError("精确索引 schema registry 映射格式无效")
        database = str(raw.get("database_name") or "").strip().lower()
        table = str(raw.get("table_name") or "").strip().lower()
        signature = str(raw.get("schema_signature") or "").strip().lower()
        primary = dict(raw["primary_key"])
        if not database or not table or len(signature) != 64:
            raise ValueError("精确索引 schema registry 缺少完整绑定")
        try:
            ordinal = int(primary["column_ordinal"])
            type_id = int(primary["type_id"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("精确索引 schema registry 主键定义无效") from exc
        if ordinal < 0 or type_id not in SUPPORTED_TYPE_IDS:
            raise ValueError("精确索引 schema registry 主键类型不安全")
        mapping = {
            "column_ordinal": ordinal,
            "column_name": str(primary.get("column_name") or "").strip(),
            "row_image_key": str(primary.get("row_image_key") or "").strip(),
            "type_id": type_id,
            "evidence": str(raw.get("evidence") or "").strip(),
        }
        if not mapping["column_name"] or not mapping["row_image_key"]:
            raise ValueError("精确索引 schema registry 主键名称不完整")
        key = (database, table, signature)
        if key in mappings:
            raise ValueError("精确索引 schema registry 存在重复映射")
        mappings[key] = mapping
    canonical = {
        "format_version": SCHEMA_REGISTRY_FORMAT_VERSION,
        "mappings": [
            {"database": key[0], "table": key[1], "signature": key[2], **value}
            for key, value in sorted(mappings.items())
        ],
    }
    digest = hashlib.sha256(_json(canonical).encode("utf-8")).hexdigest()
    return mappings, digest


def _parse_columns(value: Any) -> list[dict[str, Any]] | None:
    if isinstance(value, list):
        rows = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            rows = json.loads(text)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
    if not isinstance(rows, list) or not all(isinstance(item, dict) for item in rows):
        return None
    return [dict(item) for item in rows]


def _parse_image(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return dict(value)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return dict(parsed) if isinstance(parsed, dict) else None


def _canonical_binary(value: Any, *, query: bool) -> bytes | None:
    if isinstance(value, dict) and "$binary_base64" in value:
        try:
            return base64.b64decode(str(value["$binary_base64"]), validate=True)
        except (ValueError, TypeError):
            return None
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if query and isinstance(value, str):
        text = value.strip()
        if text.lower().startswith("base64:"):
            try:
                return base64.b64decode(text[7:], validate=True)
            except (ValueError, TypeError):
                return None
        if text.lower().startswith("0x"):
            try:
                return bytes.fromhex(text[2:])
            except ValueError:
                return None
    return None


def canonical_value(value: Any, type_id: int, *, query: bool = False) -> bytes | None:
    if type_id in INTEGER_TYPE_IDS:
        if isinstance(value, bool):
            integer = int(value)
        elif isinstance(value, int):
            integer = value
        elif query and isinstance(value, str):
            try:
                integer = int(value.strip(), 10)
            except ValueError:
                return None
        else:
            return None
        return b"i:" + str(integer).encode("ascii")
    if type_id in BINARY_TYPE_IDS:
        binary = _canonical_binary(value, query=query)
        return b"b:" + binary if binary is not None else None
    return None


def _primary_context(
    row: dict[str, Any],
    mapping: dict[str, Any] | None = None,
    columns: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    operation = str(row.get("operation") or "").upper()
    if operation not in ROW_OPERATIONS:
        return {"state": "irrelevant"}
    columns = columns if columns is not None else _parse_columns(row.get("columns_json"))
    if columns is None:
        return {"state": "unknown", "reason": "columns-missing"}
    primary = [item for item in columns if bool(item.get("primary_key"))]
    if len(primary) != 1 and mapping is None:
        return {
            "state": "unknown",
            "reason": "primary-key-not-single",
            "schema_version_id": _schema_version(row, columns),
        }
    if mapping is not None:
        ordinal_value = int(mapping["column_ordinal"])
        if ordinal_value >= len(columns):
            return {
                "state": "unknown",
                "reason": "registry-column-out-of-range",
                "schema_version_id": _schema_version(row, columns),
            }
        column = columns[ordinal_value]
        try:
            observed_ordinal = int(column.get("index"))
            observed_type = int(column.get("type_id"))
        except (TypeError, ValueError):
            observed_ordinal = -1
            observed_type = -1
        observed_key = str(column.get("name") or f"@{ordinal_value + 1}")
        if (
            observed_ordinal != ordinal_value
            or observed_type != int(mapping["type_id"])
            or observed_key != str(mapping["row_image_key"])
        ):
            return {
                "state": "unknown",
                "reason": "registry-schema-mismatch",
                "schema_version_id": _schema_version(row, columns),
            }
    else:
        column = primary[0]
    try:
        ordinal = int(column.get("index"))
        type_id = int(column.get("type_id"))
    except (TypeError, ValueError):
        return {
            "state": "unknown",
            "reason": "primary-key-metadata-invalid",
            "schema_version_id": _schema_version(row, columns),
        }
    image_key = str(column.get("name") or f"@{ordinal + 1}")
    name = str(mapping["column_name"]) if mapping is not None else image_key
    context = {
        "state": "supported" if type_id in SUPPORTED_TYPE_IDS else "unknown",
        "reason": "" if type_id in SUPPORTED_TYPE_IDS else "primary-key-type-unsupported",
        "schema_version_id": _schema_version(row, columns),
        "column_ordinal": ordinal,
        "column_name": name,
        "image_key": image_key,
        "type_id": type_id,
        "before": _MISSING,
        "after": _MISSING,
    }
    if context["state"] != "supported":
        return context
    before = _parse_image(row.get("before_json"))
    after = _parse_image(row.get("after_json"))
    if before is not None and image_key in before:
        context["before"] = before[image_key]
    if after is not None and image_key in after:
        context["after"] = after[image_key]
    required = (
        ("after",)
        if operation == "INSERT"
        else ("before",)
        if operation in {"DELETE", "UPDATE"}
        else ()
    )
    canonical: list[tuple[str, Any, bytes]] = []
    for image in required:
        raw = context[image]
        if raw is _MISSING:
            return {**context, "state": "unknown", "reason": f"{image}-primary-key-missing"}
        encoded = canonical_value(raw, type_id)
        if encoded is None:
            return {**context, "state": "unknown", "reason": f"{image}-primary-key-invalid"}
        canonical.append((image, raw, encoded))
    if operation == "UPDATE" and context["after"] is not _MISSING:
        raw = context["after"]
        encoded = canonical_value(raw, type_id)
        if encoded is None:
            return {**context, "state": "unknown", "reason": "after-primary-key-invalid"}
        canonical.append(("after", raw, encoded))
    context["values"] = canonical
    return context


def primary_key_match(row: dict[str, Any], value: Any) -> bool | None:
    context = _primary_context(row)
    if context.get("state") == "irrelevant":
        return False
    if context.get("state") != "supported":
        return None
    expected = canonical_value(value, int(context["type_id"]), query=True)
    if expected is None:
        return None
    return any(item[2] == expected for item in context.get("values") or [])


def _row_layout_fingerprint(parquet: pq.ParquetFile) -> str:
    payload = {
        "schema": [
            {"name": field.name, "type": str(field.type), "nullable": field.nullable}
            for field in parquet.schema_arrow
        ],
        "row_groups": [
            int(parquet.metadata.row_group(index).num_rows)
            for index in range(parquet.num_row_groups)
        ],
    }
    return hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()


def _catalog_excludes(
    catalog: dict[str, Any] | None,
    part: dict[str, Any],
    database: str,
    table: str,
) -> bool:
    if not catalog:
        return False
    if str(catalog.get("sha256") or "") != str(part.get("sha256") or ""):
        return False
    databases = {str(value).lower() for value in (catalog.get("databases") or [])}
    tables = {str(value).lower() for value in (catalog.get("tables") or [])}
    return database not in databases or table not in tables


class ExactIndex:
    def __init__(self, root: Path, *, registry_path: Path | None = None) -> None:
        self.root = Path(root)
        self.segments_dir = self.root / "segments"
        self.manifest_path = self.root / "manifest.sqlite3"
        self.schema_registry, self.registry_sha256 = _load_schema_registry(
            registry_path
        )
        self.registered_tables = frozenset(
            (database, table)
            for database, table, _signature in self.schema_registry
        )
        self.segments_dir.mkdir(parents=True, exist_ok=True)
        with self.connection() as conn:
            conn.executescript(MANIFEST_SCHEMA)
            columns = {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(segments)").fetchall()
            }
            if "registry_sha256" not in columns:
                conn.execute(
                    "ALTER TABLE segments ADD COLUMN registry_sha256 "
                    "TEXT NOT NULL DEFAULT ''"
                )

    def _mapping_for_row(
        self,
        row: dict[str, Any],
        columns: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        columns = columns if columns is not None else _parse_columns(row.get("columns_json"))
        if columns is None:
            return None
        key = (
            str(row.get("database_name") or "").strip().lower(),
            str(row.get("table_name") or "").strip().lower(),
            _schema_signature(row, columns),
        )
        return self.schema_registry.get(key)

    def _primary_context(
        self,
        row: dict[str, Any],
        schema_cache: dict[str, tuple[list[dict[str, Any]] | None, dict[str, Any] | None]] | None = None,
    ) -> dict[str, Any]:
        cache_key = "\x00".join(
            (
                str(row.get("database_name") or "").lower(),
                str(row.get("table_name") or "").lower(),
                str(row.get("schema_version_id") or ""),
                str(row.get("columns_json") or ""),
            )
        )
        cached = schema_cache.get(cache_key) if schema_cache is not None else None
        if cached is None:
            columns = _parse_columns(row.get("columns_json"))
            mapping = self._mapping_for_row(row, columns)
            if schema_cache is not None:
                schema_cache[cache_key] = (columns, mapping)
        else:
            columns, mapping = cached
        return _primary_context(row, mapping, columns)

    def primary_key_match(
        self,
        row: dict[str, Any],
        value: Any,
        *,
        schema_cache: dict[str, tuple[list[dict[str, Any]] | None, dict[str, Any] | None]] | None = None,
    ) -> bool | None:
        context = self._primary_context(row, schema_cache)
        if context.get("state") == "irrelevant":
            return False
        if context.get("state") != "supported":
            return None
        expected = canonical_value(value, int(context["type_id"]), query=True)
        if expected is None:
            return None
        return any(item[2] == expected for item in context.get("values") or [])

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.manifest_path, timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=10000")
        try:
            yield conn
        finally:
            conn.close()

    @staticmethod
    def _segment_connection(path: Path) -> sqlite3.Connection:
        uri = path.resolve().as_uri() + "?mode=ro&immutable=1"
        conn = sqlite3.connect(uri, uri=True, timeout=5, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA query_only=ON")
        return conn

    def missing_parts(
        self,
        parts: list[dict[str, Any]],
        *,
        limit: int = 8,
    ) -> list[dict[str, Any]]:
        target = max(int(limit), 0)
        if not target or not parts:
            return []
        paths = [str(part["path"]) for part in parts]
        placeholders = ",".join("?" for _ in paths)
        with self.connection() as conn:
            rows = conn.execute(
                f"SELECT p.part_path, p.logical_part_id, s.registry_sha256 "
                f"FROM segment_parts p JOIN segments s ON s.id = p.segment_id "
                f"WHERE part_path IN ({placeholders})",
                paths,
            ).fetchall()
        covered = {
            str(row["part_path"]): str(row["logical_part_id"])
            for row in rows
            if str(row["registry_sha256"]) == self.registry_sha256
        }
        return [
            part
            for part in parts
            if covered.get(str(part["path"])) != _identity(part)
        ][:target]

    def catalog_relevant_parts(
        self,
        parts: Iterable[dict[str, Any]],
        catalogs: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Use complete L0 catalogs to prioritize only registered tables.

        A missing or stale catalog remains relevant so catalog uncertainty can
        never create a false negative. With no explicit registry, embedded
        single-column primary-key metadata remains supported and every part is
        therefore retained.
        """
        values = list(parts)
        if not self.registered_tables:
            return values
        relevant: list[dict[str, Any]] = []
        for part in values:
            catalog = catalogs.get(str(part["path"]))
            if any(
                not _catalog_excludes(catalog, part, database, table)
                for database, table in self.registered_tables
            ):
                relevant.append(part)
        return relevant

    def _registered_indices(self, table: Any) -> Any:
        if not self.registered_tables or not table.num_rows:
            return None
        databases = pc.utf8_lower(table.column("database_name"))
        tables = pc.utf8_lower(table.column("table_name"))
        mask = None
        for database, table_name in sorted(self.registered_tables):
            pair = pc.and_(
                pc.equal(databases, database),
                pc.equal(tables, table_name),
            )
            mask = pair if mask is None else pc.or_(mask, pair)
        return pc.indices_nonzero(pc.fill_null(mask, False))

    def build_segment(
        self,
        part_sources: Iterable[tuple[dict[str, Any], Any]],
        *,
        progress: Any | None = None,
    ) -> dict[str, Any]:
        values = list(part_sources)
        if not values:
            return {"built": 0, "part_count": 0, "row_count": 0, "exact_docs": 0}
        segment_id = uuid.uuid4().hex
        temporary = self.segments_dir / f".{segment_id}.sqlite.tmp"
        conn = sqlite3.connect(temporary, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        summaries: list[dict[str, Any]] = []
        table_summaries: list[dict[str, Any]] = []
        exact_docs = 0
        total_rows = 0
        try:
            conn.executescript(SEGMENT_SCHEMA)
            conn.execute("BEGIN IMMEDIATE")
            for position, (part, source) in enumerate(values, start=1):
                summary, coverage, docs = self._add_part(conn, part, source)
                summaries.append(summary)
                table_summaries.extend(coverage)
                exact_docs += docs
                total_rows += int(summary["row_count"])
                if progress is not None:
                    progress(position, len(values), part, summary)
            conn.executemany(
                "INSERT INTO metadata(key, value) VALUES(?, ?)",
                [
                    ("format_version", str(EXACT_INDEX_FORMAT_VERSION)),
                    ("registry_sha256", self.registry_sha256),
                    ("segment_id", segment_id),
                    ("created_at", _utc_text()),
                    ("part_count", str(len(summaries))),
                    ("row_count", str(total_rows)),
                    ("exact_doc_count", str(exact_docs)),
                ],
            )
            conn.execute("COMMIT")
            integrity = conn.execute("PRAGMA integrity_check").fetchone()
            if not integrity or str(integrity[0]).lower() != "ok":
                raise RuntimeError(f"exact sidecar integrity check failed: {integrity}")
            conn.execute("PRAGMA optimize")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            conn.close()
            temporary.unlink(missing_ok=True)
            raise
        conn.close()
        digest = _sha256_file(temporary)
        final = self.segments_dir / f"segment-{digest}.sqlite"
        if final.exists():
            temporary.unlink()
        else:
            os.replace(temporary, final)
        min_epoch = min(int(item["min_event_epoch_us"]) for item in summaries)
        max_epoch = max(int(item["max_event_epoch_us"]) for item in summaries)
        try:
            with self.connection() as manifest:
                manifest.execute("BEGIN IMMEDIATE")
                for summary in summaries:
                    manifest.execute(
                        "DELETE FROM part_tables WHERE part_path = ?",
                        (summary["part_path"],),
                    )
                    manifest.execute(
                        "DELETE FROM segment_parts WHERE part_path = ?",
                        (summary["part_path"],),
                    )
                manifest.execute(
                    """
                    INSERT OR REPLACE INTO segments(
                        id, file_name, sha256, registry_sha256, format_version,
                        min_event_epoch_us, max_event_epoch_us,
                        part_count, row_count, exact_doc_count, created_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        segment_id,
                        final.name,
                        digest,
                        self.registry_sha256,
                        EXACT_INDEX_FORMAT_VERSION,
                        min_epoch,
                        max_epoch,
                        len(summaries),
                        total_rows,
                        exact_docs,
                        _utc_text(),
                    ),
                )
                manifest.executemany(
                    """
                    INSERT INTO segment_parts(
                        part_path, segment_id, logical_part_id, object_sha256,
                        row_layout_fingerprint, row_count,
                        min_event_epoch_us, max_event_epoch_us
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            item["part_path"],
                            segment_id,
                            item["logical_part_id"],
                            item["object_sha256"],
                            item["row_layout_fingerprint"],
                            item["row_count"],
                            item["min_event_epoch_us"],
                            item["max_event_epoch_us"],
                        )
                        for item in summaries
                    ],
                )
                manifest.executemany(
                    """
                    INSERT INTO part_tables(
                        part_path, segment_id, logical_part_id, database_name,
                        table_name, schema_version_id, column_ordinal,
                        column_name, type_id, coverage_state,
                        row_events, indexed_events
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        (
                            item["part_path"],
                            segment_id,
                            item["logical_part_id"],
                            item["database_name"],
                            item["table_name"],
                            item["schema_version_id"],
                            item["column_ordinal"],
                            item["column_name"],
                            item["type_id"],
                            item["coverage_state"],
                            item["row_events"],
                            item["indexed_events"],
                        )
                        for item in table_summaries
                    ],
                )
                manifest.execute("COMMIT")
        except BaseException:
            final.unlink(missing_ok=True)
            raise
        return {
            "built": 1,
            "segment_id": segment_id,
            "path": str(final),
            "sha256": digest,
            "part_count": len(summaries),
            "row_count": total_rows,
            "exact_docs": exact_docs,
        }

    def _add_part(
        self,
        conn: sqlite3.Connection,
        part: dict[str, Any],
        source: Any,
    ) -> tuple[dict[str, Any], list[dict[str, Any]], int]:
        parquet = pq.ParquetFile(source)
        available = set(parquet.schema_arrow.names)
        required = {
            "event_id",
            "event_epoch_us",
            "operation",
            "database_name",
            "table_name",
            "before_json",
            "after_json",
            "columns_json",
        }
        if not required.issubset(available):
            missing = ", ".join(sorted(required - available))
            raise ValueError(f"Parquet 缺少精确索引列：{missing}")
        columns = [
            name
            for name in (
                "event_id",
                "event_epoch_us",
                "operation",
                "database_name",
                "table_name",
                "transaction_id",
                "gtid",
                "source_file_name",
                "host_instance_id",
                "server_id",
                "thread_id",
                "start_position",
                "end_position",
                "row_index",
                "raw_event_type",
                "before_json",
                "after_json",
                "columns_json",
                "schema_version_id",
                "table_map_id",
            )
            if name in available
        ]
        part_path = str(part["path"])
        logical = _identity(part)
        coverage: dict[tuple[Any, ...], dict[str, Any]] = {}
        schema_cache: dict[
            str,
            tuple[list[dict[str, Any]] | None, dict[str, Any] | None],
        ] = {}
        doc_count = 0
        scanned_rows = 0
        for row_group_id in range(parquet.num_row_groups):
            if self.registered_tables:
                selector = parquet.read_row_group(
                    row_group_id,
                    columns=["database_name", "table_name"],
                    use_threads=False,
                )
                scanned_rows += selector.num_rows
                indices = self._registered_indices(selector)
                if indices is None or not len(indices):
                    continue
                table = parquet.read_row_group(
                    row_group_id,
                    columns=columns,
                    use_threads=False,
                ).take(indices)
                row_offsets: Iterable[int] = (
                    int(value.as_py()) for value in indices
                )
            else:
                table = parquet.read_row_group(
                    row_group_id,
                    columns=columns,
                    use_threads=False,
                )
                scanned_rows += table.num_rows
                row_offsets = range(table.num_rows)
            rows = table.to_pylist()
            for row_offset, row in zip(row_offsets, rows):
                operation = str(row.get("operation") or "").upper()
                if operation not in ROW_OPERATIONS:
                    continue
                database = str(row.get("database_name") or "").strip().lower()
                table_name = str(row.get("table_name") or "").strip().lower()
                if not database or not table_name:
                    continue
                context = self._primary_context(row, schema_cache)
                schema_version_id = str(
                    context.get("schema_version_id")
                    or hashlib.sha256(
                        f"{database}\x00{table_name}\x00unknown".encode("utf-8")
                    ).hexdigest()
                )
                ordinal = int(context.get("column_ordinal", -1))
                column_name = str(context.get("column_name") or "")
                type_id = int(context.get("type_id", -1))
                key = (
                    part_path,
                    database,
                    table_name,
                    schema_version_id,
                    ordinal,
                    column_name,
                    type_id,
                )
                state = coverage.setdefault(
                    key,
                    {
                        "part_path": part_path,
                        "logical_part_id": logical,
                        "database_name": database,
                        "table_name": table_name,
                        "schema_version_id": schema_version_id,
                        "column_ordinal": ordinal,
                        "column_name": column_name,
                        "type_id": type_id,
                        "coverage_state": "complete",
                        "row_events": 0,
                        "indexed_events": 0,
                    },
                )
                state["row_events"] += 1
                if context.get("state") != "supported":
                    state["coverage_state"] = "unknown"
                    continue
                values = list(context.get("values") or [])
                before_values = [item[1] for item in values if item[0] == "before"]
                after_values = [item[1] for item in values if item[0] == "after"]
                cursor = conn.execute(
                    """
                    INSERT INTO docs(
                        event_id, event_epoch_us, operation, database_name,
                        table_name, transaction_id, gtid, source_file_name,
                        host_instance_id, server_id, thread_id,
                        start_position, end_position, row_index, raw_event_type,
                        logical_part_id, row_group_id, row_offset,
                        schema_version_id, primary_before_json,
                        primary_after_json
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(row.get("event_id") or ""),
                        int(row.get("event_epoch_us") or 0),
                        operation,
                        database,
                        table_name,
                        str(row.get("transaction_id") or ""),
                        str(row.get("gtid") or ""),
                        str(row.get("source_file_name") or ""),
                        str(row.get("host_instance_id") or ""),
                        int(row.get("server_id") or 0),
                        int(row.get("thread_id") or 0),
                        int(row.get("start_position") or 0),
                        int(row.get("end_position") or 0),
                        int(row.get("row_index") or 0),
                        str(row.get("raw_event_type") or ""),
                        logical,
                        row_group_id,
                        row_offset,
                        schema_version_id,
                        _json({column_name: before_values[0]}) if before_values else "",
                        _json({column_name: after_values[0]}) if after_values else "",
                    ),
                )
                doc_id = int(cursor.lastrowid)
                seen: set[bytes] = set()
                for _image, _raw, encoded in values:
                    if encoded in seen:
                        continue
                    seen.add(encoded)
                    conn.execute(
                        """
                        INSERT INTO exact_values(
                            database_name, table_name, schema_version_id,
                            column_ordinal, type_id, value_hash,
                            value_key, doc_id
                        ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            database,
                            table_name,
                            schema_version_id,
                            ordinal,
                            type_id,
                            hashlib.blake2b(encoded, digest_size=16).digest(),
                            encoded,
                            doc_id,
                        ),
                    )
                state["indexed_events"] += 1
                doc_count += 1
        expected_rows = int(part.get("row_count") or scanned_rows)
        if scanned_rows != expected_rows:
            raise ValueError(
                f"精确索引行数与分区元数据不一致：{scanned_rows}/{expected_rows}"
            )
        for item in coverage.values():
            if int(item["indexed_events"]) != int(item["row_events"]):
                item["coverage_state"] = "unknown"
            conn.execute(
                """
                INSERT INTO part_tables(
                    part_path, logical_part_id, database_name, table_name,
                    schema_version_id, column_ordinal, column_name, type_id,
                    coverage_state, row_events, indexed_events
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item["part_path"],
                    item["logical_part_id"],
                    item["database_name"],
                    item["table_name"],
                    item["schema_version_id"],
                    item["column_ordinal"],
                    item["column_name"],
                    item["type_id"],
                    item["coverage_state"],
                    item["row_events"],
                    item["indexed_events"],
                ),
            )
        summary = {
            "part_path": part_path,
            "logical_part_id": logical,
            "object_sha256": _object_sha(part),
            "row_layout_fingerprint": _row_layout_fingerprint(parquet),
            "row_count": scanned_rows,
            "min_event_epoch_us": int(part["min_event_epoch_us"]),
            "max_event_epoch_us": int(part["max_event_epoch_us"]),
        }
        return summary, list(coverage.values()), doc_count

    def coverage(
        self,
        parts: list[dict[str, Any]],
        *,
        catalogs: dict[str, dict[str, Any]],
        database: str,
        table: str,
    ) -> dict[str, Any]:
        database = str(database or "").strip().lower()
        table = str(table or "").strip().lower()
        if self.registered_tables and (database, table) not in self.registered_tables:
            return {
                "complete": False,
                "covered_parts": 0,
                "missing_parts": [],
                "unknown_parts": [str(part["path"]) for part in parts],
                "segment_ids": [],
                "reason": "schema-unregistered",
            }
        missing: list[str] = []
        unknown: list[str] = []
        covered: list[str] = []
        segment_ids: set[str] = set()
        with self.connection() as conn:
            for part in parts:
                path = str(part["path"])
                identity = _identity(part)
                segment = conn.execute(
                    "SELECT p.segment_id, p.logical_part_id, s.registry_sha256 "
                    "FROM segment_parts p JOIN segments s ON s.id = p.segment_id "
                    "WHERE p.part_path = ?",
                    (path,),
                ).fetchone()
                if (
                    segment is None
                    or str(segment["logical_part_id"]) != identity
                    or str(segment["registry_sha256"]) != self.registry_sha256
                ):
                    if _catalog_excludes(catalogs.get(path), part, database, table):
                        covered.append(path)
                    else:
                        missing.append(path)
                    continue
                table_rows = conn.execute(
                    """
                    SELECT coverage_state
                    FROM part_tables
                    WHERE part_path = ? AND logical_part_id = ?
                      AND database_name = ? AND table_name = ?
                    """,
                    (path, identity, database, table),
                ).fetchall()
                if not table_rows:
                    covered.append(path)
                    continue
                if any(str(row["coverage_state"]) != "complete" for row in table_rows):
                    unknown.append(path)
                    continue
                covered.append(path)
                segment_ids.add(str(segment["segment_id"]))
        return {
            "complete": not missing and not unknown,
            "covered_parts": len(covered),
            "missing_parts": missing,
            "unknown_parts": unknown,
            "segment_ids": sorted(segment_ids),
        }

    def lookup(
        self,
        parts: list[dict[str, Any]],
        *,
        catalogs: dict[str, dict[str, Any]],
        database: str,
        table: str,
        value: Any,
        start_epoch_us: int,
        end_epoch_us: int,
        operations: Iterable[str],
        limit: int,
        offset: int,
    ) -> dict[str, Any]:
        database = str(database or "").strip().lower()
        table = str(table or "").strip().lower()
        coverage = self.coverage(
            parts,
            catalogs=catalogs,
            database=database,
            table=table,
        )
        if not coverage["complete"]:
            return {
                **coverage,
                "rows": [],
                "has_more": False,
                "segments": 0,
                "oss_gets": 0,
                "oss_bytes": 0,
            }
        current_identities = {_identity(part) for part in parts}
        operation_set = {
            str(item).strip().upper() for item in operations if str(item).strip()
        }
        with self.connection() as conn:
            segment_rows = []
            if coverage["segment_ids"]:
                placeholders = ",".join("?" for _ in coverage["segment_ids"])
                segment_rows = conn.execute(
                    f"SELECT id, file_name FROM segments WHERE id IN ({placeholders})",
                    coverage["segment_ids"],
                ).fetchall()
        rows: list[dict[str, Any]] = []
        for segment in segment_rows:
            path = self.segments_dir / str(segment["file_name"])
            if not path.is_file():
                return {
                    **coverage,
                    "complete": False,
                    "rows": [],
                    "has_more": False,
                    "segments": 0,
                    "oss_gets": 0,
                    "oss_bytes": 0,
                    "unknown_parts": sorted(
                        set(coverage["unknown_parts"]) | {str(path)}
                    ),
                }
            segment_conn = self._segment_connection(path)
            try:
                schemas = segment_conn.execute(
                    """
                    SELECT DISTINCT schema_version_id, column_ordinal, type_id
                    FROM part_tables
                    WHERE database_name = ? AND table_name = ?
                      AND coverage_state = 'complete'
                    """,
                    (database, table),
                ).fetchall()
                doc_ids: set[int] = set()
                for schema in schemas:
                    type_id = int(schema["type_id"])
                    encoded = canonical_value(value, type_id, query=True)
                    if encoded is None:
                        continue
                    matches = segment_conn.execute(
                        """
                        SELECT doc_id FROM exact_values
                        WHERE database_name = ? AND table_name = ?
                          AND schema_version_id = ? AND column_ordinal = ?
                          AND type_id = ? AND value_hash = ? AND value_key = ?
                        """,
                        (
                            database,
                            table,
                            str(schema["schema_version_id"]),
                            int(schema["column_ordinal"]),
                            type_id,
                            hashlib.blake2b(encoded, digest_size=16).digest(),
                            encoded,
                        ),
                    ).fetchall()
                    doc_ids.update(int(item["doc_id"]) for item in matches)
                if not doc_ids:
                    continue
                placeholders = ",".join("?" for _ in doc_ids)
                docs = segment_conn.execute(
                    f"SELECT * FROM docs WHERE doc_id IN ({placeholders})",
                    sorted(doc_ids),
                ).fetchall()
                for doc in docs:
                    if str(doc["logical_part_id"]) not in current_identities:
                        continue
                    epoch = int(doc["event_epoch_us"])
                    operation = str(doc["operation"])
                    if epoch < int(start_epoch_us) or epoch > int(end_epoch_us):
                        continue
                    if operation_set and operation not in operation_set:
                        continue
                    rows.append(self._result_row(doc))
            finally:
                segment_conn.close()
        unique: dict[tuple[str, str], dict[str, Any]] = {}
        for row in rows:
            unique[(str(row["event_id"]), str(row["locator"]))] = row
        ordered = sorted(
            unique.values(),
            key=lambda row: (
                int(row["event_epoch_us"]),
                str(row["source_file_name"]),
                int(row["end_position"]),
                int(row["row_index"]),
            ),
            reverse=True,
        )
        limit = max(int(limit), 1)
        offset = max(int(offset), 0)
        page = ordered[offset : offset + limit]
        return {
            **coverage,
            "rows": page,
            "has_more": len(ordered) > offset + limit,
            "segments": len(segment_rows),
            "oss_gets": 0,
            "oss_bytes": 0,
        }

    @staticmethod
    def _result_row(doc: sqlite3.Row) -> dict[str, Any]:
        epoch = int(doc["event_epoch_us"])
        event_time = datetime.fromtimestamp(epoch / 1_000_000, UTC).isoformat(
            timespec="microseconds"
        ).replace("+00:00", "Z")
        return {
            "event_id": str(doc["event_id"]),
            "event_epoch_us": epoch,
            "event_time_utc": event_time,
            "operation": str(doc["operation"]),
            "database_name": str(doc["database_name"]),
            "table_name": str(doc["table_name"]),
            "transaction_id": str(doc["transaction_id"]),
            "gtid": str(doc["gtid"]),
            "sql_kind": "EXACT_INDEX",
            "sql_text": "",
            "before_json": str(doc["primary_before_json"]),
            "after_json": str(doc["primary_after_json"]),
            "source_file_name": str(doc["source_file_name"]),
            "host_instance_id": str(doc["host_instance_id"]),
            "server_id": int(doc["server_id"]),
            "thread_id": int(doc["thread_id"]),
            "start_position": int(doc["start_position"]),
            "end_position": int(doc["end_position"]),
            "row_index": int(doc["row_index"]),
            "execution_time_ms": 0,
            "error_code": 0,
            "row_query": "",
            "raw_event_type": str(doc["raw_event_type"]),
            "locator": (
                f"{doc['logical_part_id']}:{int(doc['row_group_id'])}"
            ),
        }

    def stats(self) -> dict[str, Any]:
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT count(*) AS segments,
                       coalesce(sum(part_count), 0) AS segment_part_entries,
                       coalesce(sum(row_count), 0) AS rows,
                       coalesce(sum(exact_doc_count), 0) AS exact_docs
                FROM segments
                """
            ).fetchone()
            current_parts = int(
                conn.execute("SELECT count(*) FROM segment_parts").fetchone()[0]
            )
            complete_tables = int(
                conn.execute(
                    "SELECT count(*) FROM part_tables WHERE coverage_state = 'complete'"
                ).fetchone()[0]
            )
            unknown_tables = int(
                conn.execute(
                    "SELECT count(*) FROM part_tables WHERE coverage_state <> 'complete'"
                ).fetchone()[0]
            )
        size_bytes = 0
        for path in [self.manifest_path, *self.segments_dir.glob("segment-*.sqlite")]:
            try:
                if path.is_file():
                    size_bytes += path.stat().st_size
            except OSError:
                continue
        return {
            "format_version": EXACT_INDEX_FORMAT_VERSION,
            "registry_sha256": self.registry_sha256,
            "registry_mappings": len(self.schema_registry),
            "registered_tables": len(self.registered_tables),
            "segments": int(row["segments"]),
            "segment_part_entries": int(row["segment_part_entries"]),
            "part_count": current_parts,
            "row_count": int(row["rows"]),
            "exact_docs": int(row["exact_docs"]),
            "complete_table_coverage": complete_tables,
            "unknown_table_coverage": unknown_tables,
            "size_bytes": size_bytes,
        }
