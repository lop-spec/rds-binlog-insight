"""全量 SQL、事务与锁争用推断的预聚合索引。

设计边界（必须始终对用户可见）：

1. Binlog **不包含** InnoDB 锁等待、死锁、MDL 等待和事务的真实起止时刻。本模块
   的「锁分析」全部是**基于 binlog 的争用风险推断**，不是真实锁监控。
2. 事务持续时间取自同一事务内首末事件的时间戳，是**持锁时长的下界估计**：
   binlog 只在提交时落盘，事务开始等待和空闲阶段都看不到。
3. 行热点冲突依赖行镜像中的主键值。表没有可识别主键时降级为表级热点，并如实
   标注 `key_kind='table'`。

正文长期只存在于 OSS，因此聚合在后台索引流水线里按分区一次性完成，查询只读本
地 SQLite。未覆盖的区间由调用方回退为即时扫描，覆盖度随结果一起返回，绝不用
「没查到」冒充「没有」。
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Iterator

from .sql_fingerprint import (
    FINGERPRINT_FORMAT_VERSION,
    fingerprint_of,
    normalize_sql,
    sql_action,
    statement_profile,
)

# 4：sql_stat 增加真实执行次数，txn_top/txn_bucket 增加事务提交依赖与体量。
ANALYTICS_FORMAT_VERSION = 4

# 5 分钟时间桶：与 Parquet 分片内的聚簇粒度一致，既能画趋势又不放大索引体积。
BUCKET_US = 5 * 60 * 1_000_000

# 每个分区保留的事务明细上限。事务总数可达百万级，全量保留会让本地索引失控；
# 直方图保留全量统计，明细只保留最值得排查的部分，另加无条件保留的跨分区事务。
TXN_TOP_PER_PART = 200
HOT_OBJECT_TOP_PER_PART = 400

# 语句级中间结果的硬上限。超过则退化为「按库表+操作」的模板聚合，并在覆盖信息
# 里标注 degraded，不允许无声截断。
MAX_STATEMENT_GROUPS = 400_000

# 单次热点行提取覆盖的表数量上限，按事件量取前 N 张表。
MAX_HOT_TABLES = 64

# 语句榜单允许的排序口径。白名单直接拼进 ORDER BY，绝不接受外部字符串。
# 注意没有「扫描行数」：binlog 只记录被修改的行，扫描行数是慢日志的口径。
SQL_ORDERS = {
    "executions": "executions DESC, row_events DESC",
    "events": "events DESC, row_events DESC",
    "row_events": "row_events DESC, events DESC",
    "payload_bytes": "payload_bytes DESC, events DESC",
    "exec_time": "exec_time_ms_total DESC, slow_events DESC, events DESC",
    "recent": "last_epoch_us DESC, events DESC",
    # SELECT 扫描行数（EXPLAIN 估算 × 窗口内执行次数），见 select_explain.py。
    "scan_rows": "est_scan_total DESC, executions DESC",
}
DEFAULT_SQL_ORDER = "executions"

# Binlog 事件头时间戳是**秒级**（parser 用 header.Timestamp × 1e6），因此事务
# 时长的分辨率是 1 秒：同一秒内完成的事务时长恒为 0。毫秒级分档在这里永远只有
# 第一格有值，没有信息量，所以按秒分档。
DURATION_BUCKET_BOUNDS_US = (0, 1_000_000, 5_000_000, 30_000_000)
DURATION_BUCKET_LABELS = ("同秒完成", "≤1 秒", "2–5 秒", "6–30 秒", ">30 秒")
ROW_BUCKET_BOUNDS = (1, 10, 100, 1_000)
ROW_BUCKET_LABELS = ("1 行", "2–10 行", "11–100 行", "101–1000 行", ">1000 行")


MANIFEST_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=10000;

CREATE TABLE IF NOT EXISTS analytics_parts (
    part_path TEXT PRIMARY KEY,
    logical_part_id TEXT NOT NULL,
    object_sha256 TEXT NOT NULL,
    format_version INTEGER NOT NULL,
    fingerprint_version INTEGER NOT NULL,
    event_date TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    min_event_epoch_us INTEGER NOT NULL,
    max_event_epoch_us INTEGER NOT NULL,
    txn_count INTEGER NOT NULL,
    statement_groups INTEGER NOT NULL,
    sql_mode TEXT NOT NULL,
    hot_mode TEXT NOT NULL,
    hot_tables INTEGER NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS statements (
    fingerprint TEXT PRIMARY KEY,
    action TEXT NOT NULL,
    source_kind TEXT NOT NULL,
    normalized_sql TEXT NOT NULL,
    sample_sql TEXT NOT NULL,
    first_seen_us INTEGER NOT NULL,
    last_seen_us INTEGER NOT NULL
) WITHOUT ROWID;

-- 2026-08-07：这里原本有 10 个二级索引，全部删除。
-- 依据：拦截 analytics_summary() 实际发出的 180 条 SELECT 逐条取 EXPLAIN QUERY
-- PLAN，这 10 个索引的使用次数全部为 0。查询已改成 `FROM scope_parts p CROSS
-- JOIN <表>` 固定连接顺序，一律走各表主键（首列 part_path）seek，用不到它们。
-- 而它们实测占全库 58.6%（11.8 GiB），其中 61%~76% 的条目负载是 WITHOUT ROWID
-- 表被迫携带的完整 6 列主键副本。回滚 DDL 见 data/scratch/ops/rollback-indexes.sql。
-- 加索引前请先确认 EXPLAIN QUERY PLAN 里真的出现了它，否则只是纯成本。

CREATE TABLE IF NOT EXISTS sql_stat (
    part_path TEXT NOT NULL,
    bucket_epoch_us INTEGER NOT NULL,
    database_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    events INTEGER NOT NULL,
    executions INTEGER NOT NULL,
    row_events INTEGER NOT NULL,
    payload_bytes INTEGER NOT NULL,
    exec_time_ms_total INTEGER NOT NULL,
    exec_time_ms_max INTEGER NOT NULL,
    slow_events INTEGER NOT NULL,
    first_epoch_us INTEGER NOT NULL,
    last_epoch_us INTEGER NOT NULL,
    sample_event_id TEXT NOT NULL,
    PRIMARY KEY(
        part_path, bucket_epoch_us, database_name, table_name,
        operation, fingerprint
    )
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS txn_bucket (
    part_path TEXT NOT NULL,
    bucket_epoch_us INTEGER NOT NULL,
    txn_count INTEGER NOT NULL,
    row_events INTEGER NOT NULL,
    payload_bytes INTEGER NOT NULL,
    ddl_txn_count INTEGER NOT NULL,
    multi_table_txn_count INTEGER NOT NULL,
    cross_second_txn_count INTEGER NOT NULL,
    dependency_depth_total INTEGER NOT NULL,
    max_dependency_depth INTEGER NOT NULL,
    txn_length_bytes INTEGER NOT NULL,
    total_duration_us INTEGER NOT NULL,
    max_duration_us INTEGER NOT NULL,
    max_row_events INTEGER NOT NULL,
    rows_b1 INTEGER NOT NULL,
    rows_b2 INTEGER NOT NULL,
    rows_b3 INTEGER NOT NULL,
    rows_b4 INTEGER NOT NULL,
    rows_b5 INTEGER NOT NULL,
    dur_b1 INTEGER NOT NULL,
    dur_b2 INTEGER NOT NULL,
    dur_b3 INTEGER NOT NULL,
    dur_b4 INTEGER NOT NULL,
    dur_b5 INTEGER NOT NULL,
    PRIMARY KEY(part_path, bucket_epoch_us)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS txn_top (
    part_path TEXT NOT NULL,
    txn_key TEXT NOT NULL,
    gtid TEXT NOT NULL,
    xid TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    thread_id INTEGER NOT NULL,
    server_id INTEGER NOT NULL,
    start_epoch_us INTEGER NOT NULL,
    end_epoch_us INTEGER NOT NULL,
    duration_us INTEGER NOT NULL,
    commit_epoch_us INTEGER NOT NULL,
    first_header_us INTEGER NOT NULL,
    row_events INTEGER NOT NULL,
    payload_bytes INTEGER NOT NULL,
    txn_length_bytes INTEGER NOT NULL,
    dependency_depth INTEGER NOT NULL,
    table_count INTEGER NOT NULL,
    tables_json TEXT NOT NULL,
    ops_json TEXT NOT NULL,
    has_ddl INTEGER NOT NULL,
    multi_table INTEGER NOT NULL,
    boundary_open INTEGER NOT NULL,
    PRIMARY KEY(part_path, txn_key)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS hot_object (
    part_path TEXT NOT NULL,
    bucket_epoch_us INTEGER NOT NULL,
    database_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    key_kind TEXT NOT NULL,
    row_key TEXT NOT NULL,
    txn_count INTEGER NOT NULL,
    event_count INTEGER NOT NULL,
    update_count INTEGER NOT NULL,
    delete_count INTEGER NOT NULL,
    first_epoch_us INTEGER NOT NULL,
    last_epoch_us INTEGER NOT NULL,
    PRIMARY KEY(
        part_path, bucket_epoch_us, database_name, table_name,
        key_kind, row_key
    )
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS ddl_event (
    part_path TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_epoch_us INTEGER NOT NULL,
    database_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    sample_sql TEXT NOT NULL,
    PRIMARY KEY(part_path, event_id)
) WITHOUT ROWID;

-- SELECT 指纹的扫描行数估算（EXPLAIN 口径，每指纹一行、算过即存），由
-- select_explain.py 在索引 worker 周期内增量回填。不进 DATA_TABLES：
-- 它不是分区派生数据，格式迁移时无需随分区重建。
CREATE TABLE IF NOT EXISTS select_explain (
    fingerprint TEXT PRIMARY KEY,
    sample_sha TEXT NOT NULL,
    database_name TEXT NOT NULL,
    db_source TEXT NOT NULL,
    est_rows_examined INTEGER NOT NULL,
    full_scan INTEGER NOT NULL,
    plan_json TEXT NOT NULL,
    status TEXT NOT NULL,
    error TEXT NOT NULL,
    updated_utc TEXT NOT NULL
) WITHOUT ROWID;
"""


def _decode_txn_rows(rows: Iterable[sqlite3.Row]) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for key in ("tables_json", "ops_json"):
            try:
                item[key.replace("_json", "")] = json.loads(str(item.pop(key)))
            except (TypeError, ValueError, json.JSONDecodeError):
                item[key.replace("_json", "")] = {}
        result.append(item)
    return result


def _utc_text() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _identity(part: dict[str, Any]) -> str:
    return str(part.get("logical_part_id") or part.get("sha256") or "")


def _object_sha(part: dict[str, Any]) -> str:
    return str(
        part.get("oss_object_sha256")
        or part.get("object_sha256")
        or part.get("sha256")
        or ""
    )


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _primary_key_image_names(columns_json: Any) -> list[str]:
    """取行镜像里的主键键名。

    解析器写行镜像时用的键就是列名，没有列名元数据时退化为 `@序号`
    （见 parser-go 的 rowObject/columnNames），columns_json 里的 `name`
    与之同源，因此可以直接用作 JSON 路径。
    """

    text = columns_json if isinstance(columns_json, list) else str(columns_json or "")
    if isinstance(text, str):
        if not text.strip():
            return []
        try:
            parsed = json.loads(text)
        except (TypeError, ValueError, json.JSONDecodeError):
            return []
    else:
        parsed = text
    if not isinstance(parsed, list):
        return []
    names: list[str] = []
    for item in parsed:
        if not isinstance(item, dict) or not item.get("primary_key"):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            index = item.get("index")
            if not isinstance(index, int):
                return []
            name = f"@{index + 1}"
        names.append(name)
    return names


class AnalyticsIndexError(RuntimeError):
    def __init__(self, message: str, code: str = "ANALYTICS_INDEX_ERROR"):
        super().__init__(message)
        self.code = code


class AnalyticsIndex:
    """按 Parquet 分区预聚合的 SQL / 事务 / 锁争用推断索引。"""

    def __init__(self, root: Path, *, connect_duckdb: Any = None) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.manifest_path = self.root / "manifest.sqlite3"
        self._connect_duckdb = connect_duckdb
        # DuckDB/OSS 阶段可以并行，但 SQLite manifest 只有一个写者。让同一进程
        # 的工作线程先在 Python 层排队，避免多个 BEGIN IMMEDIATE 各自消耗
        # busy_timeout，最终把正常的长提交误报成 database is locked。
        self._manifest_write_lock = threading.Lock()
        # 必须在建表脚本之前迁移：CREATE TABLE IF NOT EXISTS 对已存在的旧表是
        # 空操作，缺列的旧表不会被自动补齐，后续写入才会报错。
        self._migrate_schema()
        with self.connection() as conn:
            conn.executescript(MANIFEST_SCHEMA)
        self._drop_stale_format()

    # 每张表在当前格式下必须具备的列。缺任意一个都说明表结构来自旧版本。
    REQUIRED_COLUMNS = {
        "sql_stat": {
            "row_events",
            "executions",
            "exec_time_ms_total",
            "exec_time_ms_max",
            "slow_events",
        },
        "txn_top": {
            "multi_table",
            "txn_length_bytes",
            "dependency_depth",
            "commit_epoch_us",
            "first_header_us",
        },
        "txn_bucket": {
            "cross_second_txn_count",
            "dependency_depth_total",
            "txn_length_bytes",
        },
        "analytics_parts": {"fingerprint_version", "sql_mode", "hot_mode"},
    }

    DATA_TABLES = (
        "sql_stat",
        "txn_bucket",
        "txn_top",
        "hot_object",
        "ddl_event",
        "statements",
        "analytics_parts",
    )

    def _migrate_schema(self) -> int:
        """结构过期时整体重建。

        这些表都是可从 Parquet 重新聚合的派生索引，没有原始数据，因此升级策略
        是丢弃重建而不是逐列 ALTER——后者在多次版本迭代后极易出现半迁移状态。
        """

        if not self.manifest_path.exists():
            return 0
        with self.connection() as conn:
            existing = {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            stale = False
            for table, needed in self.REQUIRED_COLUMNS.items():
                if table not in existing:
                    continue
                columns = {
                    str(row["name"])
                    for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
                }
                if not needed.issubset(columns):
                    stale = True
                    break
            if not stale:
                return 0
            conn.execute("BEGIN IMMEDIATE")
            try:
                for table in self.DATA_TABLES:
                    conn.execute(f"DROP TABLE IF EXISTS {table}")
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return 1

    def _drop_stale_format(self) -> int:
        """删除格式或指纹版本已过期的聚合。

        版本升级会改变桶语义与指纹口径。旧行如果留在表里，即使覆盖检查把它们
        判为「未覆盖」，查询照样能读到，从而把旧口径的数据混进新结果。
        """

        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT part_path FROM analytics_parts
                WHERE format_version <> ? OR fingerprint_version <> ?
                """,
                (ANALYTICS_FORMAT_VERSION, FINGERPRINT_FORMAT_VERSION),
            ).fetchall()
        stale = [str(row["part_path"]) for row in rows]
        if not stale:
            return 0
        return self.prune_parts(stale)

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.manifest_path, timeout=15, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=15000")
        try:
            yield conn
        finally:
            conn.close()

    # ---------------------------------------------------------------- 覆盖度

    def missing_parts(
        self,
        parts: list[dict[str, Any]],
        *,
        limit: int = 4,
    ) -> list[dict[str, Any]]:
        target = max(int(limit), 0)
        if not target or not parts:
            return []
        covered = self._covered_identities([str(part["path"]) for part in parts])
        return [
            part
            for part in parts
            if covered.get(str(part["path"])) != _identity(part)
        ][:target]

    def _covered_identities(self, paths: list[str]) -> dict[str, str]:
        if not paths:
            return {}
        covered: dict[str, str] = {}
        with self.connection() as conn:
            for chunk_start in range(0, len(paths), 400):
                chunk = paths[chunk_start : chunk_start + 400]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""
                    SELECT part_path, logical_part_id
                    FROM analytics_parts
                    WHERE part_path IN ({placeholders})
                      AND format_version = ?
                      AND fingerprint_version = ?
                    """,
                    (*chunk, ANALYTICS_FORMAT_VERSION, FINGERPRINT_FORMAT_VERSION),
                ).fetchall()
                for row in rows:
                    covered[str(row["part_path"])] = str(row["logical_part_id"])
        return covered

    def coverage(self, parts: list[dict[str, Any]]) -> dict[str, Any]:
        """返回覆盖度。未覆盖的分区必须由调用方扫描兜底或如实标注。"""

        paths = [str(part["path"]) for part in parts]
        covered_map = self._covered_identities(paths)
        covered: list[str] = []
        missing: list[str] = []
        for part in parts:
            path = str(part["path"])
            if covered_map.get(path) == _identity(part):
                covered.append(path)
            else:
                missing.append(path)
        # 只有语句指纹被迫退化成模板才算「降级」。hot_mode 反映的是目标表本身
        # 有没有可识别主键，那是数据的客观属性，不是索引质量问题。
        degraded: list[str] = []
        hot_modes: dict[str, int] = {}
        keyed_tables = 0
        if covered:
            with self.connection() as conn:
                for chunk_start in range(0, len(covered), 400):
                    chunk = covered[chunk_start : chunk_start + 400]
                    placeholders = ",".join("?" for _ in chunk)
                    rows = conn.execute(
                        f"""
                        SELECT part_path, sql_mode, hot_mode, hot_tables
                        FROM analytics_parts
                        WHERE part_path IN ({placeholders})
                        """,
                        chunk,
                    ).fetchall()
                    for row in rows:
                        if str(row["sql_mode"]) != "statement":
                            degraded.append(str(row["part_path"]))
                        mode = str(row["hot_mode"] or "")
                        hot_modes[mode] = hot_modes.get(mode, 0) + 1
                        keyed_tables += int(row["hot_tables"] or 0)
        return {
            "complete": not missing,
            "total_parts": len(parts),
            "covered_parts": len(covered),
            "missing_parts": missing,
            "degraded_parts": sorted(set(degraded)),
            # 行级锁归因是否可用：需要行镜像里带主键元数据
            # （binlog_row_metadata=FULL）。全是 table-only 说明只能做表级热点。
            "hot_modes": hot_modes,
            "primary_key_tables": keyed_tables,
        }

    def stats(self) -> dict[str, Any]:
        with self.connection() as conn:
            parts = conn.execute(
                """
                SELECT COUNT(*) AS parts,
                       COALESCE(SUM(row_count), 0) AS rows,
                       COALESCE(SUM(txn_count), 0) AS txns,
                       COALESCE(MIN(min_event_epoch_us), 0) AS min_us,
                       COALESCE(MAX(max_event_epoch_us), 0) AS max_us
                FROM analytics_parts
                """
            ).fetchone()
            statements = conn.execute(
                "SELECT COUNT(*) AS total FROM statements"
            ).fetchone()
            degraded = conn.execute(
                "SELECT COUNT(*) AS total FROM analytics_parts "
                "WHERE sql_mode <> 'statement'"
            ).fetchone()
        size = self.manifest_path.stat().st_size if self.manifest_path.exists() else 0
        return {
            "parts": int(parts["parts"] or 0),
            "rows": int(parts["rows"] or 0),
            "transactions": int(parts["txns"] or 0),
            "statements": int(statements["total"] or 0),
            "degraded_parts": int(degraded["total"] or 0),
            "min_event_epoch_us": int(parts["min_us"] or 0),
            "max_event_epoch_us": int(parts["max_us"] or 0),
            "index_bytes": int(size),
        }

    def prune_parts(self, paths: Iterable[str]) -> int:
        with self._manifest_write_lock:
            return self._prune_parts_locked(paths)

    def _prune_parts_locked(self, paths: Iterable[str]) -> int:
        values = [str(path) for path in paths if str(path)]
        if not values:
            return 0
        removed = 0
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for path in values:
                    for table in (
                        "sql_stat",
                        "txn_bucket",
                        "txn_top",
                        "hot_object",
                        "ddl_event",
                    ):
                        conn.execute(
                            f"DELETE FROM {table} WHERE part_path = ?",
                            (path,),
                        )
                    cursor = conn.execute(
                        "DELETE FROM analytics_parts WHERE part_path = ?",
                        (path,),
                    )
                    removed += int(cursor.rowcount or 0)
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
        return removed

    def prune_orphans(self, known_paths: Iterable[str]) -> int:
        """删除已不在分区清单里的聚合，避免保留期外的数据长期滞留。"""

        known = {str(path) for path in known_paths}
        with self.connection() as conn:
            rows = conn.execute("SELECT part_path FROM analytics_parts").fetchall()
        stale = [str(row["part_path"]) for row in rows if str(row["part_path"]) not in known]
        return self.prune_parts(stale)

    # ------------------------------------------------------------------ 构建

    def build_part(self, part: dict[str, Any], source: Path) -> dict[str, Any]:
        """对单个 Parquet 分区做一次完整聚合并原子提交。

        DuckDB 需要真实文件路径，且聚合要读几乎所有列，Range 读没有优势，因此
        调用方负责把正文准备成本地文件（本地正文或 OSS 临时下载）。
        """

        if self._connect_duckdb is None:
            raise AnalyticsIndexError(
                "分析索引缺少 DuckDB 连接工厂",
                "ANALYTICS_DUCKDB_UNAVAILABLE",
            )
        path = Path(source)
        if not path.is_file():
            raise AnalyticsIndexError(
                "分析索引源文件不存在",
                "ANALYTICS_SOURCE_MISSING",
            )
        conn = self._connect_duckdb()
        try:
            payload = self._collect(conn, path)
        finally:
            conn.close()
        self._commit(part, payload)
        return {
            "built": 1,
            "part_path": str(part["path"]),
            "row_count": payload["row_count"],
            "txn_count": payload["txn_count"],
            "statement_groups": len(payload["sql_rows"]),
            "sql_mode": payload["sql_mode"],
            "hot_mode": payload["hot_mode"],
        }

    @staticmethod
    def _available_columns(conn: Any, path: Path) -> set[str]:
        rows = conn.execute(
            "SELECT name FROM parquet_schema(" + _sql_literal(str(path)) + ")"
        ).fetchall()
        return {str(row[0]) for row in rows}

    def _create_view(self, conn: Any, path: Path) -> None:
        available = self._available_columns(conn, path)

        def column(name: str, fallback: str) -> str:
            return name if name in available else f"{fallback} AS {name}"

        required = {"event_epoch_us", "operation", "database_name", "table_name"}
        if not required.issubset(available):
            raise AnalyticsIndexError(
                "Parquet 缺少分析所需的基础列",
                "ANALYTICS_COLUMNS_MISSING",
            )
        select = ",\n            ".join(
            [
                column("event_id", "''"),
                "event_epoch_us",
                "operation",
                "database_name",
                "table_name",
                column("sql_kind", "''"),
                column("sql_text", "''"),
                column("row_query", "''"),
                column("transaction_id", "''"),
                column("gtid", "''"),
                column("xid", "''"),
                column("thread_id", "0"),
                column("server_id", "0"),
                column("schema_version_id", "''"),
                column("columns_json", "''"),
                column("before_json", "''"),
                column("after_json", "''"),
                column("execution_time_ms", "0"),
                column("header_epoch_us", "0"),
                column("commit_epoch_us", "0"),
                column("txn_last_committed", "0"),
                column("txn_sequence_number", "0"),
                column("txn_length_bytes", "0"),
                column("start_position", "0"),
            ]
        )
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW raw_events AS
            SELECT
            {select}
            FROM read_parquet({_sql_literal(str(path))})
            """
        )
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW ev AS
            SELECT
                event_id,
                event_epoch_us,
                (event_epoch_us / {BUCKET_US})::BIGINT * {BUCKET_US}
                    AS bucket_epoch_us,
                UPPER(COALESCE(operation, '')) AS operation,
                COALESCE(database_name, '') AS database_name,
                COALESCE(table_name, '') AS table_name,
                UPPER(COALESCE(sql_kind, '')) AS sql_kind,
                COALESCE(sql_text, '') AS sql_text,
                COALESCE(row_query, '') AS row_query,
                COALESCE(transaction_id, '') AS transaction_id,
                COALESCE(gtid, '') AS gtid,
                COALESCE(xid, '') AS xid,
                CAST(COALESCE(thread_id, 0) AS BIGINT) AS thread_id,
                CAST(COALESCE(server_id, 0) AS BIGINT) AS server_id,
                COALESCE(schema_version_id, '') AS schema_version_id,
                COALESCE(columns_json, '') AS columns_json,
                COALESCE(before_json, '') AS before_json,
                COALESCE(after_json, '') AS after_json,
                -- QueryEvent 头部的 exec_time，**秒级**且只有 QueryEvent 有值；
                -- 行事件恒为 0。只能用来标记跨秒的慢语句，不能当耗时分布用。
                CAST(COALESCE(execution_time_ms, 0) AS BIGINT) AS execution_time_ms,
                -- 解析器 v2 字段；旧 Parquet 缺列时由上面的 column() 补 0。
                CAST(COALESCE(header_epoch_us, 0) AS BIGINT) AS header_epoch_us,
                CAST(COALESCE(commit_epoch_us, 0) AS BIGINT) AS commit_epoch_us,
                CAST(COALESCE(txn_last_committed, 0) AS BIGINT) AS txn_last_committed,
                CAST(COALESCE(txn_sequence_number, 0) AS BIGINT)
                    AS txn_sequence_number,
                CAST(COALESCE(txn_length_bytes, 0) AS BIGINT) AS txn_length_bytes,
                -- 同一个 RowsEvent 的所有行共享起止位置，因此 distinct 位置数就是
                -- 语句被执行的次数（近似：超大批量会被 MySQL 拆成多个 RowsEvent）。
                CAST(COALESCE(start_position, 0) AS BIGINT) AS start_position,
                CAST(
                    LENGTH(COALESCE(sql_text, ''))
                    + LENGTH(COALESCE(before_json, ''))
                    + LENGTH(COALESCE(after_json, ''))
                    + LENGTH(COALESCE(row_query, ''))
                    AS BIGINT
                ) AS payload_bytes,
                CASE
                    WHEN COALESCE(gtid, '') <> '' THEN 'g:' || gtid
                    WHEN COALESCE(xid, '') <> ''
                        THEN 'x:' || xid || ':' || CAST(COALESCE(server_id, 0) AS VARCHAR)
                    WHEN COALESCE(transaction_id, '') <> ''
                        THEN 't:' || transaction_id
                    ELSE ''
                END AS txn_key,
                (
                    UPPER(COALESCE(sql_kind, '')) = 'ORIGINAL'
                    AND regexp_matches(
                        UPPER(LTRIM(COALESCE(sql_text, ''))),
                        '^(CREATE|ALTER|DROP|RENAME|TRUNCATE)[ \\t(]'
                    )
                ) AS is_ddl
            FROM raw_events
            WHERE event_epoch_us IS NOT NULL
            """
        )

    def _collect(self, conn: Any, path: Path) -> dict[str, Any]:
        self._create_view(conn, path)
        overview = conn.execute(
            """
            SELECT COUNT(*) AS rows,
                   COALESCE(MIN(event_epoch_us), 0) AS min_us,
                   COALESCE(MAX(event_epoch_us), 0) AS max_us
            FROM ev
            """
        ).fetchone()
        row_count = int(overview[0] or 0)
        part_min = int(overview[1] or 0)
        part_max = int(overview[2] or 0)
        if not row_count:
            return {
                "row_count": 0,
                "min_event_epoch_us": 0,
                "max_event_epoch_us": 0,
                "txn_count": 0,
                "sql_mode": "statement",
                "hot_mode": "empty",
                "hot_tables": 0,
                "sql_rows": [],
                "statements": {},
                "txn_buckets": [],
                "txn_tops": [],
                "hot_rows": [],
                "ddl_rows": [],
            }

        sql_rows, statements, sql_mode = self._collect_statements(conn)
        txn_buckets, txn_tops, txn_count = self._collect_transactions(
            conn,
            part_min=part_min,
            part_max=part_max,
        )
        hot_rows, hot_mode, hot_tables = self._collect_hot_objects(conn)
        ddl_rows = self._collect_ddl(conn, statements)
        return {
            "row_count": row_count,
            "min_event_epoch_us": part_min,
            "max_event_epoch_us": part_max,
            "txn_count": txn_count,
            "sql_mode": sql_mode,
            "hot_mode": hot_mode,
            "hot_tables": hot_tables,
            "sql_rows": sql_rows,
            "statements": statements,
            "txn_buckets": txn_buckets,
            "txn_tops": txn_tops,
            "hot_rows": hot_rows,
            "ddl_rows": ddl_rows,
        }

    # ------------------------------------------------------- 语句级聚合

    def _collect_statements(
        self,
        conn: Any,
    ) -> tuple[list[tuple[Any, ...]], dict[str, dict[str, Any]], str]:
        conn.execute(
            """
            CREATE OR REPLACE TEMP TABLE stmt_groups AS
            SELECT
                bucket_epoch_us,
                database_name,
                table_name,
                operation,
                sql_kind,
                sql_text,
                row_query,
                COUNT(*) AS events,
                COUNT(DISTINCT start_position) AS executions,
                COUNT(*) FILTER (
                    WHERE operation IN ('INSERT', 'UPDATE', 'DELETE')
                ) AS row_events,
                SUM(payload_bytes) AS payload_bytes,
                SUM(execution_time_ms) AS exec_time_ms_total,
                MAX(execution_time_ms) AS exec_time_ms_max,
                COUNT(*) FILTER (WHERE execution_time_ms > 0) AS slow_events,
                MIN(event_epoch_us) AS first_epoch_us,
                MAX(event_epoch_us) AS last_epoch_us,
                MIN(event_id) AS sample_event_id
            FROM ev
            GROUP BY 1, 2, 3, 4, 5, 6, 7
            """
        )
        total = int(
            conn.execute("SELECT COUNT(*) FROM stmt_groups").fetchone()[0] or 0
        )
        mode = "statement"
        if total > MAX_STATEMENT_GROUPS:
            # 语句实例过多（通常是未参数化的高基数 SQL）。退化为「库表+操作」
            # 模板聚合，行数与字节数仍然精确，只是不再区分语句文本。
            mode = "template"
            conn.execute(
                """
                CREATE OR REPLACE TEMP TABLE stmt_groups AS
                SELECT
                    bucket_epoch_us,
                    database_name,
                    table_name,
                    operation,
                    sql_kind,
                    '' AS sql_text,
                    '' AS row_query,
                    SUM(events) AS events,
                    SUM(executions) AS executions,
                    SUM(row_events) AS row_events,
                    SUM(payload_bytes) AS payload_bytes,
                    SUM(exec_time_ms_total) AS exec_time_ms_total,
                    MAX(exec_time_ms_max) AS exec_time_ms_max,
                    SUM(slow_events) AS slow_events,
                    MIN(first_epoch_us) AS first_epoch_us,
                    MAX(last_epoch_us) AS last_epoch_us,
                    MIN(sample_event_id) AS sample_event_id
                FROM stmt_groups
                GROUP BY 1, 2, 3, 4, 5, 6, 7
                """
            )

        aggregated: dict[tuple[Any, ...], list[Any]] = {}
        statements: dict[str, dict[str, Any]] = {}
        cursor = conn.execute(
            """
            SELECT bucket_epoch_us, database_name, table_name, operation,
                   sql_kind, sql_text, row_query, events, executions, row_events,
                   payload_bytes, exec_time_ms_total, exec_time_ms_max,
                   slow_events, first_epoch_us, last_epoch_us, sample_event_id
            FROM stmt_groups
            """
        )
        while True:
            batch = cursor.fetchmany(20_000)
            if not batch:
                break
            for row in batch:
                (
                    bucket,
                    database,
                    table,
                    operation,
                    sql_kind,
                    sql_text,
                    row_query,
                    events,
                    executions,
                    row_events,
                    payload_bytes,
                    exec_total,
                    exec_max,
                    slow_events,
                    first_us,
                    last_us,
                    sample_event_id,
                ) = row
                profile = statement_profile(
                    sql_kind=str(sql_kind or ""),
                    sql_text=str(sql_text or ""),
                    row_query=str(row_query or ""),
                    operation=str(operation or ""),
                    database=str(database or ""),
                    table=str(table or ""),
                )
                fingerprint = profile["fingerprint"]
                key = (
                    int(bucket),
                    str(database or ""),
                    str(table or ""),
                    str(operation or ""),
                    fingerprint,
                )
                entry = aggregated.get(key)
                if entry is None:
                    aggregated[key] = [
                        int(events or 0),
                        int(executions or 0),
                        int(row_events or 0),
                        int(payload_bytes or 0),
                        int(exec_total or 0),
                        int(exec_max or 0),
                        int(slow_events or 0),
                        int(first_us or 0),
                        int(last_us or 0),
                        str(sample_event_id or ""),
                    ]
                else:
                    entry[0] += int(events or 0)
                    entry[1] += int(executions or 0)
                    entry[2] += int(row_events or 0)
                    entry[3] += int(payload_bytes or 0)
                    entry[4] += int(exec_total or 0)
                    entry[5] = max(entry[5], int(exec_max or 0))
                    entry[6] += int(slow_events or 0)
                    entry[7] = min(entry[7], int(first_us or 0))
                    entry[8] = max(entry[8], int(last_us or 0))
                known = statements.get(fingerprint)
                if known is None:
                    statements[fingerprint] = {
                        "action": profile["action"],
                        "source_kind": profile["source"],
                        "normalized_sql": profile["normalized"],
                        "sample_sql": profile["sample"],
                        "first_seen_us": int(first_us or 0),
                        "last_seen_us": int(last_us or 0),
                    }
                else:
                    known["first_seen_us"] = min(
                        known["first_seen_us"], int(first_us or 0)
                    )
                    known["last_seen_us"] = max(
                        known["last_seen_us"], int(last_us or 0)
                    )
        conn.execute("DROP TABLE IF EXISTS stmt_groups")
        rows = [(*key, *value) for key, value in aggregated.items()]
        return rows, statements, mode

    # --------------------------------------------------------- 事务聚合

    def _collect_transactions(
        self,
        conn: Any,
        *,
        part_min: int,
        part_max: int,
    ) -> tuple[list[tuple[Any, ...]], list[tuple[Any, ...]], int]:
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE txn AS
            SELECT
                txn_key,
                MIN(event_epoch_us) AS start_epoch_us,
                MAX(event_epoch_us) AS end_epoch_us,
                -- 事务耗时：提交时刻（微秒）减去事务内最早事件的时间戳（秒级）。
                -- 起点被截断到秒，因此只有跨秒事务的结果有意义；同秒完成的事务
                -- 算出来是「commit 在该秒内的偏移」，是噪声，必须置 0。
                CASE
                    WHEN MAX(commit_epoch_us) > 0
                     AND MIN(NULLIF(header_epoch_us, 0)) > 0
                     AND MAX(commit_epoch_us) - MIN(NULLIF(header_epoch_us, 0))
                         >= 1000000
                    THEN MAX(commit_epoch_us) - MIN(NULLIF(header_epoch_us, 0))
                    ELSE 0
                END AS duration_us,
                MAX(commit_epoch_us) AS commit_epoch_us,
                MIN(NULLIF(header_epoch_us, 0)) AS first_header_us,
                MAX(txn_length_bytes) AS txn_length_bytes,
                MAX(txn_sequence_number) AS sequence_number,
                MAX(txn_last_committed) AS last_committed,
                CASE
                    WHEN MAX(txn_sequence_number) > 0
                    THEN MAX(txn_sequence_number) - MAX(txn_last_committed)
                    ELSE 0
                END AS dependency_depth,
                (MIN(event_epoch_us) / {BUCKET_US})::BIGINT * {BUCKET_US}
                    AS bucket_epoch_us,
                COUNT(*) FILTER (
                    WHERE operation IN ('INSERT', 'UPDATE', 'DELETE')
                ) AS row_events,
                COUNT(*) AS events,
                SUM(payload_bytes) AS payload_bytes,
                MAX(thread_id) AS thread_id,
                MAX(server_id) AS server_id,
                MAX(gtid) AS gtid,
                MAX(xid) AS xid,
                MAX(transaction_id) AS transaction_id,
                COUNT(DISTINCT
                    CASE WHEN table_name <> ''
                    THEN database_name || '.' || table_name END
                ) AS table_count,
                MIN(
                    CASE WHEN table_name <> ''
                    THEN database_name || '.' || table_name END
                ) AS first_table,
                COUNT(*) FILTER (WHERE operation = 'INSERT') AS insert_events,
                COUNT(*) FILTER (WHERE operation = 'UPDATE') AS update_events,
                COUNT(*) FILTER (WHERE operation = 'DELETE') AS delete_events,
                COUNT(*) FILTER (WHERE is_ddl) AS ddl_events
            FROM ev
            WHERE txn_key <> ''
            GROUP BY txn_key
            """
        )
        txn_count = int(conn.execute("SELECT COUNT(*) FROM txn").fetchone()[0] or 0)
        if not txn_count:
            conn.execute("DROP TABLE IF EXISTS txn")
            return [], [], 0

        rows_case = self._histogram_case("row_events", ROW_BUCKET_BOUNDS, "rows_b")
        dur_case = self._histogram_case(
            "duration_us",
            DURATION_BUCKET_BOUNDS_US,
            "dur_b",
        )
        buckets = conn.execute(
            f"""
            SELECT
                bucket_epoch_us,
                COUNT(*) AS txn_count,
                SUM(row_events) AS row_events,
                SUM(payload_bytes) AS payload_bytes,
                COUNT(*) FILTER (WHERE ddl_events > 0) AS ddl_txn_count,
                COUNT(*) FILTER (WHERE table_count > 1) AS multi_table_txn_count,
                COUNT(*) FILTER (WHERE duration_us > 0) AS cross_second_txn_count,
                SUM(dependency_depth) AS dependency_depth_total,
                MAX(dependency_depth) AS max_dependency_depth,
                SUM(txn_length_bytes) AS txn_length_bytes,
                SUM(duration_us) AS total_duration_us,
                MAX(duration_us) AS max_duration_us,
                MAX(row_events) AS max_row_events,
                {rows_case},
                {dur_case}
            FROM txn
            GROUP BY bucket_epoch_us
            """
        ).fetchall()
        bucket_rows = [
            (int(row[0]), *[int(value or 0) for value in row[1:]])
            for row in buckets
        ]

        top = conn.execute(
            """
            SELECT txn_key, gtid, xid, transaction_id, thread_id, server_id,
                   start_epoch_us, end_epoch_us, duration_us, row_events,
                   payload_bytes, table_count, first_table,
                   insert_events, update_events, delete_events, ddl_events,
                   txn_length_bytes, dependency_depth,
                   commit_epoch_us, first_header_us
            FROM txn
            WHERE txn_key IN (
                SELECT txn_key FROM (
                    SELECT txn_key FROM txn ORDER BY row_events DESC LIMIT ?
                )
                UNION
                SELECT txn_key FROM (
                    SELECT txn_key FROM txn ORDER BY duration_us DESC LIMIT ?
                )
                UNION
                SELECT txn_key FROM txn WHERE ddl_events > 0
                UNION
                SELECT txn_key FROM (
                    SELECT txn_key FROM txn
                    WHERE table_count > 1
                    ORDER BY table_count DESC, row_events DESC
                    LIMIT ?
                )
                UNION
                SELECT txn_key FROM txn
                WHERE start_epoch_us <= ? OR end_epoch_us >= ?
            )
            """,
            [
                TXN_TOP_PER_PART,
                TXN_TOP_PER_PART,
                TXN_TOP_PER_PART,
                part_min,
                part_max,
            ],
        ).fetchall()
        top_rows: list[tuple[Any, ...]] = []
        for row in top:
            (
                txn_key,
                gtid,
                xid,
                transaction_id,
                thread_id,
                server_id,
                start_us,
                end_us,
                duration_us,
                row_events,
                payload_bytes,
                table_count,
                first_table,
                insert_events,
                update_events,
                delete_events,
                ddl_events,
                txn_length_bytes,
                dependency_depth,
                commit_epoch_us,
                first_header_us,
            ) = row
            boundary_open = int(
                int(start_us or 0) <= part_min or int(end_us or 0) >= part_max
            )
            top_rows.append(
                (
                    str(txn_key),
                    str(gtid or ""),
                    str(xid or ""),
                    str(transaction_id or ""),
                    int(thread_id or 0),
                    int(server_id or 0),
                    int(start_us or 0),
                    int(end_us or 0),
                    int(duration_us or 0),
                    int(commit_epoch_us or 0),
                    int(first_header_us or 0),
                    int(row_events or 0),
                    int(payload_bytes or 0),
                    int(txn_length_bytes or 0),
                    int(dependency_depth or 0),
                    int(table_count or 0),
                    _json(
                        {
                            "first": str(first_table or ""),
                            "count": int(table_count or 0),
                        }
                    ),
                    _json(
                        {
                            "INSERT": int(insert_events or 0),
                            "UPDATE": int(update_events or 0),
                            "DELETE": int(delete_events or 0),
                            "DDL": int(ddl_events or 0),
                        }
                    ),
                    1 if int(ddl_events or 0) else 0,
                    1 if int(table_count or 0) > 1 else 0,
                    boundary_open,
                )
            )
        conn.execute("DROP TABLE IF EXISTS txn")
        return bucket_rows, top_rows, txn_count

    @staticmethod
    def _histogram_case(column: str, bounds: tuple[int, ...], prefix: str) -> str:
        parts: list[str] = []
        previous: int | None = None
        for position, bound in enumerate(bounds, start=1):
            condition = (
                f"{column} <= {bound}"
                if previous is None
                else f"{column} > {previous} AND {column} <= {bound}"
            )
            parts.append(f"COUNT(*) FILTER (WHERE {condition}) AS {prefix}{position}")
            previous = bound
        parts.append(
            f"COUNT(*) FILTER (WHERE {column} > {bounds[-1]}) "
            f"AS {prefix}{len(bounds) + 1}"
        )
        return ", ".join(parts)

    # ----------------------------------------------------- 行热点（锁推断）

    def _collect_hot_objects(
        self,
        conn: Any,
    ) -> tuple[list[tuple[Any, ...]], str, int]:
        """按主键统计被多个事务反复改写的行，作为行锁争用热点的推断依据。

        只统计 UPDATE / DELETE：它们必然持有已存在行的行锁，是争用的主要来源。
        表没有可识别主键时降级为表级热点，并如实标注 key_kind。
        """

        tables = conn.execute(
            f"""
            SELECT database_name, table_name,
                   ANY_VALUE(columns_json) AS columns_json,
                   COUNT(*) AS events
            FROM ev
            WHERE operation IN ('UPDATE', 'DELETE')
              AND (before_json <> '' OR after_json <> '')
            GROUP BY 1, 2
            ORDER BY events DESC
            LIMIT {MAX_HOT_TABLES}
            """
        ).fetchall()
        if not tables:
            return [], "empty", 0

        cases: list[str] = []
        keyed_tables = 0
        for database, table, columns_json, _events in tables:
            key_names = _primary_key_image_names(columns_json)
            if not key_names:
                continue
            keyed_tables += 1
            image = (
                "CASE WHEN before_json <> '' THEN before_json ELSE after_json END"
            )
            extracts = [
                "COALESCE(json_extract_string("
                + image
                + ", "
                + _sql_literal('$."' + name.replace('"', '\\"') + '"')
                + "), '\\x00')"
                for name in key_names
            ]
            # 复合主键按列序拼接，单列主键退化为单次提取。
            value = extracts[0] if len(extracts) == 1 else (
                "concat_ws('\\x1f', " + ", ".join(extracts) + ")"
            )
            cases.append(
                "WHEN database_name = "
                + _sql_literal(str(database or ""))
                + " AND table_name = "
                + _sql_literal(str(table or ""))
                + " THEN "
                + value
            )

        hot_rows: list[tuple[Any, ...]] = []
        if cases:
            key_expression = "CASE " + " ".join(cases) + " ELSE NULL END"
            rows = conn.execute(
                f"""
                SELECT bucket_epoch_us, database_name, table_name, row_key,
                       COUNT(DISTINCT txn_key) AS txn_count,
                       COUNT(*) AS event_count,
                       COUNT(*) FILTER (WHERE operation = 'UPDATE') AS updates,
                       COUNT(*) FILTER (WHERE operation = 'DELETE') AS deletes,
                       MIN(event_epoch_us) AS first_epoch_us,
                       MAX(event_epoch_us) AS last_epoch_us
                FROM (
                    SELECT bucket_epoch_us, database_name, table_name,
                           operation, txn_key, event_epoch_us,
                           {key_expression} AS row_key
                    FROM ev
                    WHERE operation IN ('UPDATE', 'DELETE')
                      AND (before_json <> '' OR after_json <> '')
                )
                WHERE row_key IS NOT NULL
                GROUP BY 1, 2, 3, 4
                HAVING COUNT(DISTINCT txn_key) > 1
                ORDER BY txn_count DESC, event_count DESC
                LIMIT {HOT_OBJECT_TOP_PER_PART}
                """
            ).fetchall()
            hot_rows.extend(
                (
                    int(row[0]),
                    str(row[1] or ""),
                    str(row[2] or ""),
                    "pk",
                    str(row[3]),
                    int(row[4] or 0),
                    int(row[5] or 0),
                    int(row[6] or 0),
                    int(row[7] or 0),
                    int(row[8] or 0),
                    int(row[9] or 0),
                )
                for row in rows
            )

        # 表级写热点始终生成：既覆盖没有可识别主键的表，也给出「哪张表写压力最
        # 大」这一与主键无关的结论。
        table_rows = conn.execute(
            f"""
            SELECT bucket_epoch_us, database_name, table_name,
                   COUNT(DISTINCT txn_key) AS txn_count,
                   COUNT(*) AS event_count,
                   COUNT(*) FILTER (WHERE operation = 'UPDATE') AS updates,
                   COUNT(*) FILTER (WHERE operation = 'DELETE') AS deletes,
                   MIN(event_epoch_us) AS first_epoch_us,
                   MAX(event_epoch_us) AS last_epoch_us
            FROM ev
            WHERE operation IN ('INSERT', 'UPDATE', 'DELETE')
              AND table_name <> ''
            GROUP BY 1, 2, 3
            ORDER BY event_count DESC
            LIMIT {HOT_OBJECT_TOP_PER_PART}
            """
        ).fetchall()
        hot_rows.extend(
            (
                int(row[0]),
                str(row[1] or ""),
                str(row[2] or ""),
                "table",
                "",
                int(row[3] or 0),
                int(row[4] or 0),
                int(row[5] or 0),
                int(row[6] or 0),
                int(row[7] or 0),
                int(row[8] or 0),
            )
            for row in table_rows
        )
        if not keyed_tables:
            mode = "table-only"
        elif keyed_tables < len(tables):
            mode = "mixed"
        else:
            mode = "primary-key"
        return hot_rows, mode, keyed_tables

    def _collect_ddl(
        self,
        conn: Any,
        statements: dict[str, dict[str, Any]],
    ) -> list[tuple[Any, ...]]:
        rows = conn.execute(
            """
            SELECT event_id, event_epoch_us, database_name, table_name, sql_text
            FROM ev
            WHERE is_ddl
            ORDER BY event_epoch_us
            LIMIT 5000
            """
        ).fetchall()
        result: list[tuple[Any, ...]] = []
        for event_id, epoch_us, database, table, sql_text in rows:
            normalized = normalize_sql(str(sql_text or ""))
            fingerprint = fingerprint_of(f"original\x00{normalized}")
            if fingerprint and fingerprint not in statements:
                statements[fingerprint] = {
                    "action": sql_action(normalized) or "DDL",
                    "source_kind": "original",
                    "normalized_sql": normalized,
                    "sample_sql": str(sql_text or "")[:2048],
                    "first_seen_us": int(epoch_us or 0),
                    "last_seen_us": int(epoch_us or 0),
                }
            result.append(
                (
                    str(event_id or ""),
                    int(epoch_us or 0),
                    str(database or ""),
                    str(table or ""),
                    fingerprint,
                    str(sql_text or "")[:2048],
                )
            )
        return result

    # ------------------------------------------------------------- 提交

    def _commit(self, part: dict[str, Any], payload: dict[str, Any]) -> None:
        with self._manifest_write_lock:
            self._commit_locked(part, payload)

    def _commit_locked(self, part: dict[str, Any], payload: dict[str, Any]) -> None:
        part_path = str(part["path"])
        event_date = str(part.get("event_date") or "")
        with self.connection() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                for table in (
                    "sql_stat",
                    "txn_bucket",
                    "txn_top",
                    "hot_object",
                    "ddl_event",
                ):
                    conn.execute(
                        f"DELETE FROM {table} WHERE part_path = ?",
                        (part_path,),
                    )
                conn.executemany(
                    """
                    INSERT INTO statements(
                        fingerprint, action, source_kind, normalized_sql,
                        sample_sql, first_seen_us, last_seen_us
                    ) VALUES(?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(fingerprint) DO UPDATE SET
                        first_seen_us = MIN(first_seen_us, excluded.first_seen_us),
                        last_seen_us = MAX(last_seen_us, excluded.last_seen_us)
                    """,
                    [
                        (
                            fingerprint,
                            value["action"],
                            value["source_kind"],
                            value["normalized_sql"],
                            value["sample_sql"],
                            int(value["first_seen_us"]),
                            int(value["last_seen_us"]),
                        )
                        for fingerprint, value in payload["statements"].items()
                        if fingerprint
                    ],
                )
                conn.executemany(
                    """
                    INSERT INTO sql_stat(
                        part_path, bucket_epoch_us, database_name, table_name,
                        operation, fingerprint, events, executions, row_events,
                        payload_bytes, exec_time_ms_total, exec_time_ms_max,
                        slow_events, first_epoch_us, last_epoch_us,
                        sample_event_id
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [(part_path, *row) for row in payload["sql_rows"]],
                )
                conn.executemany(
                    """
                    INSERT INTO txn_bucket(
                        part_path, bucket_epoch_us, txn_count, row_events,
                        payload_bytes, ddl_txn_count, multi_table_txn_count,
                        cross_second_txn_count, dependency_depth_total,
                        max_dependency_depth, txn_length_bytes,
                        total_duration_us, max_duration_us, max_row_events,
                        rows_b1, rows_b2, rows_b3, rows_b4, rows_b5,
                        dur_b1, dur_b2, dur_b3, dur_b4, dur_b5
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [(part_path, *row) for row in payload["txn_buckets"]],
                )
                conn.executemany(
                    """
                    INSERT INTO txn_top(
                        part_path, txn_key, gtid, xid, transaction_id,
                        thread_id, server_id, start_epoch_us, end_epoch_us,
                        duration_us, commit_epoch_us, first_header_us,
                        row_events, payload_bytes,
                        txn_length_bytes, dependency_depth, table_count,
                        tables_json, ops_json, has_ddl, multi_table,
                        boundary_open
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [(part_path, *row) for row in payload["txn_tops"]],
                )
                conn.executemany(
                    """
                    INSERT INTO hot_object(
                        part_path, bucket_epoch_us, database_name, table_name,
                        key_kind, row_key, txn_count, event_count,
                        update_count, delete_count, first_epoch_us, last_epoch_us
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [(part_path, *row) for row in payload["hot_rows"]],
                )
                conn.executemany(
                    """
                    INSERT INTO ddl_event(
                        part_path, event_id, event_epoch_us, database_name,
                        table_name, fingerprint, sample_sql
                    ) VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    [(part_path, *row) for row in payload["ddl_rows"]],
                )
                conn.execute(
                    """
                    INSERT OR REPLACE INTO analytics_parts(
                        part_path, logical_part_id, object_sha256,
                        format_version, fingerprint_version, event_date,
                        row_count, min_event_epoch_us, max_event_epoch_us,
                        txn_count, statement_groups, sql_mode, hot_mode,
                        hot_tables, created_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        part_path,
                        _identity(part),
                        _object_sha(part),
                        ANALYTICS_FORMAT_VERSION,
                        FINGERPRINT_FORMAT_VERSION,
                        event_date,
                        int(payload["row_count"]),
                        int(payload["min_event_epoch_us"]),
                        int(payload["max_event_epoch_us"]),
                        int(payload["txn_count"]),
                        len(payload["sql_rows"]),
                        str(payload["sql_mode"]),
                        str(payload["hot_mode"]),
                        int(payload["hot_tables"]),
                        _utc_text(),
                    ),
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    # ------------------------------------------------------------- 查询

    @staticmethod
    def _install_scope(conn: sqlite3.Connection, part_paths: Iterable[str]) -> int:
        """把待查分区放进临时表，避免超长 IN 列表触碰变量上限。"""

        # 下面所有查询都写成 `FROM scope_parts p CROSS JOIN <表>`：CROSS JOIN
        # 在 SQLite 里会固定连接顺序，强制以这张小临时表为外层、按各表主键
        # 首列 part_path 做 seek。库里从未跑过 ANALYZE(无 sqlite_stat1)，普通
        # JOIN 会被优化器选成全扫 sql_stat(近千万行/十几 GB)再回探临时表，
        # 单次查询从秒级劣化到分钟级超时。改写顺序即可，勿退回普通 JOIN。
        conn.execute("DROP TABLE IF EXISTS temp.scope_parts")
        conn.execute(
            "CREATE TEMP TABLE scope_parts(part_path TEXT PRIMARY KEY) WITHOUT ROWID"
        )
        values = sorted({str(path) for path in part_paths if str(path)})
        conn.executemany(
            "INSERT OR IGNORE INTO scope_parts(part_path) VALUES(?)",
            [(value,) for value in values],
        )
        return len(values)

    @staticmethod
    def _trend_width(start_us: int, end_us: int, max_points: int = 480) -> int:
        span = max(int(end_us) - int(start_us), BUCKET_US)
        buckets = max(span // BUCKET_US, 1)
        factor = max(int((buckets + max_points - 1) // max_points), 1)
        return BUCKET_US * factor

    def summarize(
        self,
        part_paths: list[str],
        *,
        start_epoch_us: int,
        end_epoch_us: int,
        database: str = "",
        table: str = "",
        operation: str = "",
        limit: int = 50,
        order: str = DEFAULT_SQL_ORDER,
        rollup_width: int = 0,
        instance: str = "",
    ) -> dict[str, Any]:
        """在已覆盖分区上做一次跨分区归并，返回三类分析结果。

        `rollup_width` 非 0 时改从时间桶 rollup 读（长窗口用，扫描量与分区数
        无关）；此时实例过滤走 rollup 自带的 instance_id 列，不再依赖分区归属。
        """

        start_us = int(start_epoch_us)
        end_us = int(end_epoch_us)
        limit = min(max(int(limit), 1), 500)
        order = str(order) if str(order) in SQL_ORDERS else DEFAULT_SQL_ORDER
        rollup_width = int(rollup_width or 0)
        filters: list[str] = []
        params: dict[str, Any] = {"start_us": start_us, "end_us": end_us}
        if rollup_width:
            params.update(
                {
                    "rollup_width": rollup_width,
                    # 桶按宽度对齐：起点向下取整，终点向上取整，保证窗口内
                    # 任何一刻所属的桶都被纳入。
                    "bucket_start": (start_us // rollup_width) * rollup_width,
                    "bucket_end": (end_us // rollup_width) * rollup_width
                    + rollup_width,
                }
            )
            if instance:
                params["instance"] = str(instance)
        if database:
            filters.append("AND s.database_name = :database")
            params["database"] = str(database)
        if table:
            filters.append("AND s.table_name = :table")
            params["table"] = str(table)
        if operation:
            filters.append("AND s.operation = :operation")
            params["operation"] = str(operation).upper()
        # 每项自带尾随空格：rollup 路径会在 object_filter 后面继续拼
        # "AND s.is_boundary = 0"，用 " ".join 时末项没有尾随空格，会粘成
        # ":operationAND s.is_boundary" —— SQLite 把 :operationAND 当成合法参数名，
        # 于是报 near "s": syntax error。只有长窗口(走 rollup)且选了库/表/操作
        # 任一筛选时才触发，短窗口不走 rollup 所以一直没暴露。
        object_filter = "".join(f"{item} " for item in filters)
        width = self._trend_width(start_us, end_us)

        with self.connection() as conn:
            if rollup_width:
                # rollup 路径的每一段都按 (width, bucket) 主键 seek，完全不碰
                # scope_parts；长窗口下这张临时表要插三万多行，白花 1 秒。
                scope_size = len(part_paths)
            else:
                scope_size = self._install_scope(conn, part_paths)
            if not scope_size:
                return self._empty_summary(start_us, end_us, width)
            if rollup_width:
                # 趋势粒度不能细于桶宽，否则每个桶只有一个点、其余为空。
                width = max(width, rollup_width)
            sql_section = self._sql_section(
                conn,
                params=params,
                object_filter=object_filter,
                limit=limit,
                width=width,
                order=order,
                rollup_width=rollup_width,
                instance=instance,
            )
            txn_section = self._transaction_section(
                conn,
                params=params,
                limit=limit,
                width=width,
                rollup_width=rollup_width,
                instance=instance,
            )
            lock_section = self._lock_section(
                conn,
                params=params,
                object_filter=object_filter,
                limit=limit,
                rollup_width=rollup_width,
                instance=instance,
            )
        return {
            "window": {
                "start_epoch_us": start_us,
                "end_epoch_us": end_us,
                # 实际统计粒度：走 rollup 时就是桶宽，不再是 5 分钟。
                "bucket_us": rollup_width or BUCKET_US,
                "trend_bucket_us": width,
                "rollup_width_us": rollup_width,
            },
            "sql": sql_section,
            "transactions": txn_section,
            "locks": lock_section,
        }

    @staticmethod
    def _empty_summary(start_us: int, end_us: int, width: int) -> dict[str, Any]:
        return {
            "window": {
                "start_epoch_us": start_us,
                "end_epoch_us": end_us,
                "bucket_us": BUCKET_US,
                "trend_bucket_us": width,
            },
            "sql": {
                "order": DEFAULT_SQL_ORDER,
                "totals": {
                    "events": 0,
                    "executions": 0,
                    "row_events": 0,
                    "payload_bytes": 0,
                    "slow_events": 0,
                    "fingerprints": 0,
                    "objects": 0,
                    "boundary_events": 0,
                    "est_scan_rows": 0,
                    "est_covered_executions": 0,
                },
                "statements": [],
                "orders": {},
                "objects": [],
                "operations": [],
                "trend": [],
            },
            "transactions": {
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
            },
            "locks": {
                "long_transactions": [],
                "large_transactions": [],
                "row_hotspots": [],
                "table_hotspots": [],
                "ddl_windows": [],
                "risk": {},
            },
        }

    def _sql_section(
        self,
        conn: sqlite3.Connection,
        *,
        params: dict[str, Any],
        object_filter: str,
        limit: int,
        width: int,
        order: str = DEFAULT_SQL_ORDER,
        rollup_width: int = 0,
        instance: str = "",
    ) -> dict[str, Any]:
        # 事务边界（BEGIN/COMMIT）与事务数同量级，混进语句统计会把业务 SQL 完全
        # 挤出榜单，因此在 SQL 维度排除，单独计数，由事务维度负责解读。
        if rollup_width:
            # 长窗口走时间桶 rollup：主键首列是 (bucket_width_us,
            # bucket_epoch_us)，直接按时间范围 seek，扫描量与分区数无关。
            # boundary 在 rollup 时已固化成列，省掉每行一次 statements seek。
            base = (
                "FROM sql_rollup s "
                "WHERE s.bucket_width_us = :rollup_width "
                "AND s.bucket_epoch_us >= :bucket_start "
                "AND s.bucket_epoch_us < :bucket_end "
                "AND s.last_epoch_us >= :start_us AND s.first_epoch_us <= :end_us "
                + ("AND s.instance_id = :instance " if instance else "")
                + object_filter
            )
            window = base + "AND s.is_boundary = 0 "
            boundary_window = base + "AND s.is_boundary = 1 "
            # 先把窗口内的行压成「对象×指纹」一层快照：合计、对象分布、操作
            # 分布、语句榜单全部从它再聚合，rollup 表只扫这一次（另加趋势一次）。
            # 保留库表与操作维度，同一指纹落在多张分表时对象分布才不会失真。
            conn.execute("DROP TABLE IF EXISTS temp.sql_obj")
            conn.execute(
                f"""
                CREATE TEMP TABLE sql_obj AS
                SELECT s.database_name AS database_name,
                       s.table_name AS table_name,
                       s.operation AS operation,
                       s.fingerprint AS fingerprint,
                       SUM(s.events) AS events,
                       SUM(s.executions) AS executions,
                       SUM(s.row_events) AS row_events,
                       SUM(s.payload_bytes) AS payload_bytes,
                       SUM(s.exec_time_ms_total) AS exec_time_ms_total,
                       MAX(s.exec_time_ms_max) AS exec_time_ms_max,
                       SUM(s.slow_events) AS slow_events,
                       MIN(s.first_epoch_us) AS first_epoch_us,
                       MAX(s.last_epoch_us) AS last_epoch_us,
                       MIN(s.sample_event_id) AS sample_event_id
                {window}
                GROUP BY s.database_name, s.table_name, s.operation, s.fingerprint
                """,
                params,
            )
            # 趋势要按时间桶分组，sql_obj 已经把时间维度压没了，它仍走原表。
            trend_window = window
            window = "FROM temp.sql_obj s WHERE 1=1 "
        else:
            window = (
                "FROM scope_parts p CROSS JOIN sql_stat s "
                "ON s.part_path = p.part_path "
                "JOIN statements st ON st.fingerprint = s.fingerprint "
                "WHERE s.last_epoch_us >= :start_us AND s.first_epoch_us <= :end_us "
                "AND st.source_kind <> 'boundary' "
                + object_filter
            )
            boundary_window = window.replace(
                "AND st.source_kind <> 'boundary' ",
                "AND st.source_kind = 'boundary' ",
            )
            trend_window = window
        totals = conn.execute(
            f"""
            SELECT COALESCE(SUM(s.events), 0) AS events,
                   COALESCE(SUM(s.executions), 0) AS executions,
                   COALESCE(SUM(s.row_events), 0) AS row_events,
                   COALESCE(SUM(s.payload_bytes), 0) AS payload_bytes,
                   COALESCE(SUM(s.slow_events), 0) AS slow_events,
                   COUNT(DISTINCT s.fingerprint) AS fingerprints,
                   COUNT(DISTINCT s.database_name || '.' || s.table_name) AS objects
            {window}
            """,
            params,
        ).fetchone()
        boundary = conn.execute(
            f"""
            SELECT COALESCE(SUM(s.events), 0) AS events
            {boundary_window}
            """,
            params,
        ).fetchone()
        # select_explain 是指纹级小表（主键 seek），LEFT JOIN 只加常数成本；
        # est_scan_total = EXPLAIN 估算的单次扫描行数 × 窗口内执行次数。
        explain_window = window.replace(
            "WHERE ",
            "LEFT JOIN select_explain se "
            "ON se.fingerprint = s.fingerprint AND se.status = 'ok' "
            "WHERE ",
            1,
        )
        # 指纹级聚合只扫一遍窗口，落进临时表；六种排序、扫描行数合计都从
        # 这份快照取。响应里带全部排序的 TopN，前端换排序零请求零扫描。
        conn.execute("DROP TABLE IF EXISTS temp.sql_grouped")
        conn.execute(
            f"""
            CREATE TEMP TABLE sql_grouped AS
            SELECT s.fingerprint AS fingerprint,
                   SUM(s.events) AS events,
                   SUM(s.executions) AS executions,
                   SUM(s.row_events) AS row_events,
                   SUM(s.payload_bytes) AS payload_bytes,
                   SUM(s.exec_time_ms_total) AS exec_time_ms_total,
                   MAX(s.exec_time_ms_max) AS exec_time_ms_max,
                   SUM(s.slow_events) AS slow_events,
                   MIN(s.first_epoch_us) AS first_epoch_us,
                   MAX(s.last_epoch_us) AS last_epoch_us,
                   COUNT(DISTINCT s.database_name || '.' || s.table_name) AS objects,
                   MIN(s.database_name) AS database_name,
                   MIN(s.table_name) AS table_name,
                   MIN(s.operation) AS operation,
                   MIN(s.sample_event_id) AS sample_event_id,
                   MIN(se.est_rows_examined) AS est_rows_per_exec,
                   MIN(se.database_name) AS est_db,
                   MAX(se.full_scan) AS est_full_scan,
                   SUM(s.executions) * COALESCE(MIN(se.est_rows_examined), 0)
                       AS est_scan_total
            {explain_window}
            GROUP BY s.fingerprint
            """,
            params,
        )
        orders_rows: dict[str, list[sqlite3.Row]] = {}
        for order_key, order_clause in SQL_ORDERS.items():
            orders_rows[order_key] = conn.execute(
                f"""
                SELECT * FROM temp.sql_grouped
                ORDER BY {order_clause}
                LIMIT :limit
                """,
                {"limit": limit},
            ).fetchall()
        statements = orders_rows[order]
        est_totals = conn.execute(
            """
            SELECT COALESCE(SUM(est_scan_total), 0) AS est_scan_rows,
                   COALESCE(SUM(
                       CASE WHEN est_rows_per_exec IS NOT NULL
                            THEN executions ELSE 0 END
                   ), 0) AS est_covered
            FROM temp.sql_grouped
            """
        ).fetchone()
        detail_fingerprints = list(
            dict.fromkeys(
                str(row["fingerprint"])
                for rows in orders_rows.values()
                for row in rows
            )
        )
        detail = self._statement_details(conn, detail_fingerprints)
        # 注意：不要改成从 temp.sql_grouped 再聚合。那张表按 fingerprint 分组、
        # 库表列取的是 MIN，同一指纹落在多张表（分表）时对象分布会失真。
        objects = conn.execute(
            f"""
            SELECT s.database_name AS database_name,
                   s.table_name AS table_name,
                   SUM(s.events) AS events,
                   SUM(s.payload_bytes) AS payload_bytes,
                   COUNT(DISTINCT s.fingerprint) AS fingerprints
            {window}
            GROUP BY s.database_name, s.table_name
            ORDER BY events DESC
            LIMIT :limit
            """,
            {**params, "limit": limit},
        ).fetchall()
        operations = conn.execute(
            f"""
            SELECT s.operation AS operation,
                   SUM(s.events) AS events,
                   SUM(s.payload_bytes) AS payload_bytes
            {window}
            GROUP BY s.operation
            ORDER BY events DESC
            """,
            params,
        ).fetchall()
        trend = conn.execute(
            f"""
            SELECT (s.bucket_epoch_us / :width) * :width AS ts,
                   SUM(s.events) AS events,
                   SUM(s.payload_bytes) AS payload_bytes
            {trend_window}
            GROUP BY ts
            ORDER BY ts
            """,
            {**params, "width": width},
        ).fetchall()
        return {
            "order": order,
            "totals": {
                "events": int(totals["events"] or 0),
                "executions": int(totals["executions"] or 0),
                "row_events": int(totals["row_events"] or 0),
                "payload_bytes": int(totals["payload_bytes"] or 0),
                "slow_events": int(totals["slow_events"] or 0),
                "fingerprints": int(totals["fingerprints"] or 0),
                "objects": int(totals["objects"] or 0),
                "boundary_events": int(boundary["events"] or 0),
                "est_scan_rows": int(est_totals["est_scan_rows"] or 0),
                "est_covered_executions": int(est_totals["est_covered"] or 0),
            },
            "statements": [
                {
                    **{key: row[key] for key in row.keys()},
                    **detail.get(str(row["fingerprint"]), {}),
                }
                for row in statements
            ],
            # 全部排序的 TopN 快照：前端换排序直接本地切换，不再发请求。
            "orders": {
                order_key: [
                    {
                        **{key: row[key] for key in row.keys()},
                        **detail.get(str(row["fingerprint"]), {}),
                    }
                    for row in rows
                ]
                for order_key, rows in orders_rows.items()
            },
            "objects": [dict(row) for row in objects],
            "operations": [dict(row) for row in operations],
            "trend": [dict(row) for row in trend],
        }

    @staticmethod
    def _statement_details(
        conn: sqlite3.Connection,
        fingerprints: list[str],
    ) -> dict[str, dict[str, Any]]:
        if not fingerprints:
            return {}
        result: dict[str, dict[str, Any]] = {}
        for start in range(0, len(fingerprints), 400):
            chunk = fingerprints[start : start + 400]
            placeholders = ",".join("?" for _ in chunk)
            rows = conn.execute(
                f"""
                SELECT fingerprint, action, source_kind, normalized_sql, sample_sql
                FROM statements
                WHERE fingerprint IN ({placeholders})
                """,
                chunk,
            ).fetchall()
            for row in rows:
                result[str(row["fingerprint"])] = {
                    "action": str(row["action"]),
                    "source_kind": str(row["source_kind"]),
                    "normalized_sql": str(row["normalized_sql"]),
                    "sample_sql": str(row["sample_sql"]),
                }
        return result

    def _transaction_section(
        self,
        conn: sqlite3.Connection,
        *,
        params: dict[str, Any],
        limit: int,
        width: int,
        rollup_width: int = 0,
        instance: str = "",
    ) -> dict[str, Any]:
        if rollup_width:
            bucket_window = (
                "FROM txn_rollup b "
                "WHERE b.bucket_width_us = :rollup_width "
                "AND b.bucket_epoch_us >= :bucket_start "
                "AND b.bucket_epoch_us < :bucket_end"
                + (" AND b.instance_id = :instance" if instance else "")
            )
        else:
            bucket_window = (
                "FROM scope_parts p CROSS JOIN txn_bucket b "
                "ON b.part_path = p.part_path "
                "WHERE b.bucket_epoch_us + :bucket_us >= :start_us "
                "AND b.bucket_epoch_us <= :end_us"
            )
        bucket_params = {**params, "bucket_us": BUCKET_US}
        totals = conn.execute(
            f"""
            SELECT COALESCE(SUM(b.txn_count), 0) AS transactions,
                   COALESCE(SUM(b.row_events), 0) AS row_events,
                   COALESCE(SUM(b.payload_bytes), 0) AS payload_bytes,
                   COALESCE(SUM(b.ddl_txn_count), 0) AS ddl_transactions,
                   COALESCE(SUM(b.multi_table_txn_count), 0)
                       AS multi_table_transactions,
                   COALESCE(SUM(b.cross_second_txn_count), 0)
                       AS cross_second_transactions,
                   COALESCE(SUM(b.dependency_depth_total), 0)
                       AS dependency_depth_total,
                   COALESCE(MAX(b.max_dependency_depth), 0)
                       AS max_dependency_depth,
                   COALESCE(SUM(b.txn_length_bytes), 0) AS txn_length_bytes,
                   COALESCE(MAX(b.max_duration_us), 0) AS max_duration_us,
                   COALESCE(MAX(b.max_row_events), 0) AS max_row_events,
                   COALESCE(SUM(b.total_duration_us), 0) AS total_duration_us
            {bucket_window}
            """,
            bucket_params,
        ).fetchone()
        transactions = int(totals["transactions"] or 0)
        histogram = conn.execute(
            f"""
            SELECT COALESCE(SUM(b.rows_b1), 0) AS rows_b1,
                   COALESCE(SUM(b.rows_b2), 0) AS rows_b2,
                   COALESCE(SUM(b.rows_b3), 0) AS rows_b3,
                   COALESCE(SUM(b.rows_b4), 0) AS rows_b4,
                   COALESCE(SUM(b.rows_b5), 0) AS rows_b5,
                   COALESCE(SUM(b.dur_b1), 0) AS dur_b1,
                   COALESCE(SUM(b.dur_b2), 0) AS dur_b2,
                   COALESCE(SUM(b.dur_b3), 0) AS dur_b3,
                   COALESCE(SUM(b.dur_b4), 0) AS dur_b4,
                   COALESCE(SUM(b.dur_b5), 0) AS dur_b5
            {bucket_window}
            """,
            bucket_params,
        ).fetchone()
        trend = conn.execute(
            f"""
            SELECT (b.bucket_epoch_us / :width) * :width AS ts,
                   SUM(b.txn_count) AS transactions,
                   SUM(b.row_events) AS row_events,
                   MAX(b.max_duration_us) AS max_duration_us
            {bucket_window}
            GROUP BY ts
            ORDER BY ts
            """,
            {**bucket_params, "width": width},
        ).fetchall()
        # 四类下钻共用一次请求：界面上点概览数字即可切换，不再往返。
        top_args = {"rollup_width": rollup_width, "instance": instance}
        longest = self._top_transactions(
            conn,
            params=params,
            limit=limit,
            order="duration_us DESC, row_events DESC",
            **top_args,
        )
        largest = self._top_transactions(
            conn,
            params=params,
            limit=limit,
            order="row_events DESC",
            **top_args,
        )
        ddl = self._top_transactions(
            conn,
            params=params,
            limit=limit,
            order="start_epoch_us DESC",
            having="MAX(t.has_ddl) = 1",
            **top_args,
        )
        multi_table = self._top_transactions(
            conn,
            params=params,
            limit=limit,
            order="table_count DESC, row_events DESC",
            having="MAX(t.multi_table) = 1",
            **top_args,
        )
        return {
            "ddl": ddl,
            "multi_table": multi_table,
            "totals": {
                "transactions": transactions,
                "row_events": int(totals["row_events"] or 0),
                "payload_bytes": int(totals["payload_bytes"] or 0),
                "ddl_transactions": int(totals["ddl_transactions"] or 0),
                "multi_table_transactions": int(
                    totals["multi_table_transactions"] or 0
                ),
                "cross_second_transactions": int(
                    totals["cross_second_transactions"] or 0
                ),
                "max_dependency_depth": int(totals["max_dependency_depth"] or 0),
                "avg_dependency_depth": (
                    round(int(totals["dependency_depth_total"] or 0) / transactions, 1)
                    if transactions
                    else 0
                ),
                "txn_length_bytes": int(totals["txn_length_bytes"] or 0),
                "max_duration_us": int(totals["max_duration_us"] or 0),
                "max_row_events": int(totals["max_row_events"] or 0),
                "avg_duration_us": (
                    int(int(totals["total_duration_us"] or 0) / transactions)
                    if transactions
                    else 0
                ),
            },
            "row_histogram": _histogram_rows(
                histogram,
                "rows_b",
                ROW_BUCKET_LABELS,
            ),
            "duration_histogram": _histogram_rows(
                histogram,
                "dur_b",
                DURATION_BUCKET_LABELS,
            ),
            "longest": longest,
            "largest": largest,
            "trend": [dict(row) for row in trend],
        }

    @staticmethod
    def _top_transactions(
        conn: sqlite3.Connection,
        *,
        params: dict[str, Any],
        limit: int,
        order: str,
        having: str = "",
        rollup_width: int = 0,
        instance: str = "",
    ) -> list[dict[str, Any]]:
        if rollup_width:
            # 桶级 TopN 再选 TopN：duration 取各桶最大（同一事务不跨桶重复
            # 计时），row_events 跨桶累加。属于近似——只在长窗口生效。
            rows = conn.execute(
                f"""
                SELECT t.txn_key AS txn_key,
                       MAX(t.gtid) AS gtid, MAX(t.xid) AS xid,
                       MAX(t.transaction_id) AS transaction_id,
                       MAX(t.thread_id) AS thread_id,
                       MAX(t.server_id) AS server_id,
                       MIN(t.start_epoch_us) AS start_epoch_us,
                       MAX(t.end_epoch_us) AS end_epoch_us,
                       MAX(t.duration_us) AS duration_us,
                       SUM(t.row_events) AS row_events,
                       SUM(t.payload_bytes) AS payload_bytes,
                       MAX(t.txn_length_bytes) AS txn_length_bytes,
                       MAX(t.dependency_depth) AS dependency_depth,
                       MAX(t.table_count) AS table_count,
                       MAX(t.tables_json) AS tables_json,
                       MAX(t.ops_json) AS ops_json,
                       MAX(t.has_ddl) AS has_ddl,
                       MAX(t.multi_table) AS multi_table,
                       MAX(t.boundary_open) AS boundary_open,
                       SUM(t.part_fragments) AS part_fragments
                FROM txn_top_rollup t
                WHERE t.bucket_width_us = :rollup_width
                  AND t.bucket_epoch_us >= :bucket_start
                  AND t.bucket_epoch_us < :bucket_end
                  AND t.end_epoch_us >= :start_us AND t.start_epoch_us <= :end_us
                  {"AND t.instance_id = :instance" if instance else ""}
                GROUP BY t.txn_key
                {"HAVING " + having if having else ""}
                ORDER BY {order}
                LIMIT :limit
                """,
                {**params, "limit": limit},
            ).fetchall()
            return _decode_txn_rows(rows)
        rows = conn.execute(
            f"""
            SELECT t.txn_key AS txn_key,
                   MAX(t.gtid) AS gtid,
                   MAX(t.xid) AS xid,
                   MAX(t.transaction_id) AS transaction_id,
                   MAX(t.thread_id) AS thread_id,
                   MAX(t.server_id) AS server_id,
                   MIN(t.start_epoch_us) AS start_epoch_us,
                   MAX(t.end_epoch_us) AS end_epoch_us,
                   -- 用提交时刻与最早 header 重算：事务可能跨分区，取各分区
                   -- duration 的 MAX 会漏掉跨分区的真实跨度。start/end 都是
                   -- commit 时刻，相减恒为 0，不能用它们。
                   CASE
                       WHEN MAX(t.commit_epoch_us) > 0
                        AND MIN(NULLIF(t.first_header_us, 0)) > 0
                        AND MAX(t.commit_epoch_us)
                            - MIN(NULLIF(t.first_header_us, 0)) >= 1000000
                       THEN MAX(t.commit_epoch_us)
                            - MIN(NULLIF(t.first_header_us, 0))
                       ELSE MAX(t.duration_us)
                   END AS duration_us,
                   SUM(t.row_events) AS row_events,
                   SUM(t.payload_bytes) AS payload_bytes,
                   MAX(t.txn_length_bytes) AS txn_length_bytes,
                   MAX(t.dependency_depth) AS dependency_depth,
                   MAX(t.table_count) AS table_count,
                   MAX(t.tables_json) AS tables_json,
                   MAX(t.ops_json) AS ops_json,
                   MAX(t.has_ddl) AS has_ddl,
                   MAX(t.multi_table) AS multi_table,
                   MAX(t.boundary_open) AS boundary_open,
                   COUNT(*) AS part_fragments
            FROM scope_parts p CROSS JOIN txn_top t ON t.part_path = p.part_path
            WHERE t.end_epoch_us >= :start_us AND t.start_epoch_us <= :end_us
            GROUP BY t.txn_key
            {"HAVING " + having if having else ""}
            ORDER BY {order}
            LIMIT :limit
            """,
            {**params, "limit": limit},
        ).fetchall()
        return _decode_txn_rows(rows)

    def _lock_section(
        self,
        conn: sqlite3.Connection,
        *,
        params: dict[str, Any],
        object_filter: str,
        limit: int,
        rollup_width: int = 0,
        instance: str = "",
    ) -> dict[str, Any]:
        # hot_object 没有 operation 维度：把 operation 条件从对象过滤里剥掉，
        # 否则 operation 筛选会生成不存在的 h.operation 列而报错。
        hot_filter = (
            object_filter.replace("AND s.operation = :operation", "")
            .replace("s.", "h.")
            .strip()
        )
        if rollup_width:
            hot_source = (
                "FROM hot_rollup h "
                "WHERE h.bucket_width_us = :rollup_width "
                "AND h.bucket_epoch_us >= :bucket_start "
                "AND h.bucket_epoch_us < :bucket_end "
                + ("AND h.instance_id = :instance " if instance else "")
            )
        else:
            hot_source = (
                "FROM scope_parts p CROSS JOIN hot_object h "
                "ON h.part_path = p.part_path WHERE "
            )
        row_hotspots = conn.execute(
            f"""
            SELECT h.database_name AS database_name,
                   h.table_name AS table_name,
                   h.row_key AS row_key,
                   SUM(h.txn_count) AS txn_count,
                   SUM(h.event_count) AS event_count,
                   SUM(h.update_count) AS update_count,
                   SUM(h.delete_count) AS delete_count,
                   MIN(h.first_epoch_us) AS first_epoch_us,
                   MAX(h.last_epoch_us) AS last_epoch_us
            {hot_source}
              {"AND" if rollup_width else ""} h.key_kind = 'pk'
              AND h.last_epoch_us >= :start_us AND h.first_epoch_us <= :end_us
              {hot_filter}
            GROUP BY h.database_name, h.table_name, h.row_key
            ORDER BY txn_count DESC, event_count DESC
            LIMIT :limit
            """,
            {**params, "limit": limit},
        ).fetchall()
        table_hotspots = conn.execute(
            f"""
            SELECT h.database_name AS database_name,
                   h.table_name AS table_name,
                   SUM(h.txn_count) AS txn_count,
                   SUM(h.event_count) AS event_count,
                   SUM(h.update_count) AS update_count,
                   SUM(h.delete_count) AS delete_count
            {hot_source}
              {"AND" if rollup_width else ""} h.key_kind = 'table'
              AND h.last_epoch_us >= :start_us AND h.first_epoch_us <= :end_us
              {hot_filter}
            GROUP BY h.database_name, h.table_name
            ORDER BY event_count DESC
            LIMIT :limit
            """,
            {**params, "limit": limit},
        ).fetchall()
        # DDL 走 rollup 时用 ddl_rollup（按事件时刻建主键），否则仍按分区 seek。
        if rollup_width:
            ddl_rows = conn.execute(
                f"""
                SELECT event_id, event_epoch_us, database_name, table_name,
                       sample_sql
                FROM ddl_rollup
                WHERE event_epoch_us >= :start_us AND event_epoch_us <= :end_us
                  {"AND instance_id = :instance" if instance else ""}
                ORDER BY event_epoch_us DESC
                LIMIT :limit
                """,
                {**params, "limit": limit},
            ).fetchall()
        else:
            ddl_rows = conn.execute(
                """
                SELECT d.event_id AS event_id,
                       d.event_epoch_us AS event_epoch_us,
                       d.database_name AS database_name,
                       d.table_name AS table_name,
                       d.sample_sql AS sample_sql
                FROM scope_parts p CROSS JOIN ddl_event d
                ON d.part_path = p.part_path
                WHERE d.event_epoch_us >= :start_us AND d.event_epoch_us <= :end_us
                ORDER BY d.event_epoch_us DESC
                LIMIT :limit
                """,
                {**params, "limit": limit},
            ).fetchall()
        ddl_windows = [
            {
                **dict(row),
                "concurrent_dml_events": self._dml_around(
                    conn,
                    database=str(row["database_name"]),
                    table=str(row["table_name"]),
                    center_us=int(row["event_epoch_us"]),
                    rollup_width=rollup_width,
                    instance=instance,
                ),
            }
            for row in ddl_rows
        ]
        long_transactions = self._top_transactions(
            conn,
            params=params,
            limit=limit,
            order="duration_us DESC",
            rollup_width=rollup_width,
            instance=instance,
        )
        large_transactions = self._top_transactions(
            conn,
            params=params,
            limit=limit,
            order="row_events DESC",
            rollup_width=rollup_width,
            instance=instance,
        )
        risk = {
            "row_hotspot_max_txn": max(
                (int(row["txn_count"] or 0) for row in row_hotspots),
                default=0,
            ),
            "longest_transaction_us": max(
                (int(item["duration_us"] or 0) for item in long_transactions),
                default=0,
            ),
            "largest_transaction_rows": max(
                (int(item["row_events"] or 0) for item in large_transactions),
                default=0,
            ),
            "ddl_events": len(ddl_windows),
        }
        return {
            "long_transactions": long_transactions,
            "large_transactions": large_transactions,
            "row_hotspots": [dict(row) for row in row_hotspots],
            "table_hotspots": [dict(row) for row in table_hotspots],
            "ddl_windows": ddl_windows,
            "risk": risk,
        }

    @staticmethod
    def _dml_around(
        conn: sqlite3.Connection,
        *,
        database: str,
        table: str,
        center_us: int,
        radius_us: int = 5 * 60 * 1_000_000,
        rollup_width: int = 0,
        instance: str = "",
    ) -> int:
        """DDL 前后同表 DML 事件量，用于估计 MDL 阻塞影响面。

        这个方法对**每条 DDL 各调一次**，走分区路径时每次都要扫遍窗口内所有
        分区的 sql_stat（7 天 = 2670 万行 × 50 条 DDL）——2026-08-11 实测，
        这是长窗口分析最大的单点开销。rollup 路径按 (width, bucket) 主键
        前缀 seek，只碰 DDL 时刻前后那一两个桶。
        """

        start_us = int(center_us) - int(radius_us)
        end_us = int(center_us) + int(radius_us)
        if rollup_width:
            row = conn.execute(
                f"""
                SELECT COALESCE(SUM(events), 0) AS events
                FROM sql_rollup
                WHERE bucket_width_us = :width
                  AND bucket_epoch_us >= :b0 AND bucket_epoch_us <= :b1
                  AND database_name = :database AND table_name = :table
                  AND operation IN ('INSERT', 'UPDATE', 'DELETE')
                  AND last_epoch_us >= :start_us AND first_epoch_us <= :end_us
                  {"AND instance_id = :instance" if instance else ""}
                """,
                {
                    "width": rollup_width,
                    "b0": (start_us // rollup_width) * rollup_width,
                    "b1": (end_us // rollup_width) * rollup_width,
                    "database": database,
                    "table": table,
                    "start_us": start_us,
                    "end_us": end_us,
                    **({"instance": instance} if instance else {}),
                },
            ).fetchone()
            return int(row["events"] or 0)
        row = conn.execute(
            """
            SELECT COALESCE(SUM(s.events), 0) AS events
            FROM scope_parts p CROSS JOIN sql_stat s ON s.part_path = p.part_path
            WHERE s.database_name = :database AND s.table_name = :table
              AND s.operation IN ('INSERT', 'UPDATE', 'DELETE')
              AND s.last_epoch_us >= :start_us AND s.first_epoch_us <= :end_us
            """,
            {
                "database": database,
                "table": table,
                "start_us": start_us,
                "end_us": end_us,
            },
        ).fetchone()
        return int(row["events"] or 0)


def _histogram_rows(
    row: Any,
    prefix: str,
    labels: tuple[str, ...],
) -> list[dict[str, Any]]:
    return [
        {
            "label": label,
            "count": int(row[f"{prefix}{position}"] or 0) if row is not None else 0,
        }
        for position, label in enumerate(labels, start=1)
    ]
