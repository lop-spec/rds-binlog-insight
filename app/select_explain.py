"""SELECT 扫描行数估算：按指纹用真实样本 EXPLAIN，结果固定持久化。

general log 只有语句文本，没有任何行数口径（binlog 只有写入行数、慢日志只覆盖
超阈值语句）。这里对「全量 SQL」里的每个 SELECT 指纹取真实参数样本（Execute 型
记录参数已绑定），到源实例上跑一次只读 EXPLAIN，把优化器估算的扫描行数落进
manifest（select_explain 表）。每个指纹只算一次（样本变了才重算），查询时按
时间窗用 executions 加权，符合「已有数据固定保存、选时间只过滤」的产品口径。

安全边界：
- 只处理 action='SELECT'；剥前导注释后必须以 SELECT/WITH 开头，否则跳过；
- 连接账号是 readonly_user（SELECT, SHOW VIEW ON *.*），与采集共用凭据；
- 只发 EXPLAIN（生成计划、不执行），绝不用 EXPLAIN ANALYZE；
- 每轮有预算（条数与耗时双上限），跑不完下轮继续，不阻塞索引主流程。
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import time
from typing import Any

LOGGER = logging.getLogger(__name__)

_SCHEMA = """
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

# 估算值上限：优化器在深连接上会给出天文数字，截断避免溢出与刷屏。
_EST_CAP = 10**15

_COMMENT_RE = re.compile(r"^\s*(?:/\*.*?\*/\s*|--[^\n]*\n\s*)+", re.S)
_DB_COMMENT_RE = re.compile(r"/\*[^*]*\bdb=([A-Za-z0-9_$]+)[^*]*\*/")
_QUALIFIED_RE = re.compile(
    r"\b(?:from|join)\s+`?([A-Za-z0-9_$]+)`?\s*\.\s*`?[A-Za-z0-9_$]+`?",
    re.I,
)
_FIRST_TABLE_RE = re.compile(
    r"\b(?:from|join)\s+`?([A-Za-z0-9_$]+)`?(?!\s*\.)",
    re.I,
)


def _utc_now_text() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.environ.get(name, str(default)))
    except ValueError:
        value = default
    return min(max(value, minimum), maximum)


class SelectExplainConfig:
    def __init__(self) -> None:
        self.enabled = _env("RDS_BINLOG_SELECT_EXPLAIN_ENABLED", "0") == "1"
        # 复用 general log 采集的只读凭据：同一实例、同一 readonly_user 账号。
        self.host = _env("RDS_BINLOG_GLOG_HOST")
        self.port = _env_int("RDS_BINLOG_GLOG_PORT", 3306, 1, 65535)
        self.user = _env("RDS_BINLOG_GLOG_USER")
        self.password = os.environ.get("RDS_BINLOG_GLOG_PASSWORD", "")
        self.batch = _env_int("RDS_BINLOG_SELECT_EXPLAIN_BATCH", 300, 1, 5000)
        self.budget_seconds = _env_int(
            "RDS_BINLOG_SELECT_EXPLAIN_BUDGET_SECONDS", 90, 5, 600
        )
        # 同名表存在于多个库时的选择顺序（实测 biz_* 等核心表在 6 个库同名，
        # 快照库是历史副本，业务流量在这些库）。
        self.db_priority = tuple(
            item.strip()
            for item in _env(
                "RDS_BINLOG_SELECT_EXPLAIN_DB_PRIORITY",
                "example_source,example_target,example_app,example_warehouse",
            ).split(",")
            if item.strip()
        )
        # 错误指纹的重试间隔：库结构变化（建表/加库）后能自愈，又不会每轮
        # 重复打无解的语句。
        self.retry_hours = _env_int(
            "RDS_BINLOG_SELECT_EXPLAIN_RETRY_HOURS", 24, 1, 24 * 14
        )

    @property
    def ready(self) -> bool:
        return bool(self.enabled and self.host and self.user and self.password)


def strip_leading_comments(sql: str) -> str:
    return _COMMENT_RE.sub("", sql, count=1).strip()


def estimate_examined_rows(plan_rows: list[dict[str, Any]]) -> int:
    """按嵌套循环语义累计各深度的估算检查行数。

    同一 select_id 内：第 i 张表被检查 rows_i × Π_{j<i}(rows_j × filtered_j) 次；
    不同 select_id（子查询/派生表/UNION 分支）之间相加。这是优化器口径的估算，
    与真实 rows_examined 有偏差，但量级与相对排序可用。
    """

    total = 0.0
    by_select: dict[str, list[dict[str, Any]]] = {}
    for row in plan_rows:
        by_select.setdefault(str(row.get("id") or "1"), []).append(row)
    for rows in by_select.values():
        prefix = 1.0
        for row in rows:
            est = float(row.get("rows") or 0)
            filtered = float(row.get("filtered") or 100.0) / 100.0
            total += prefix * est
            prefix *= max(est * filtered, 1.0)
            if total > _EST_CAP:
                return _EST_CAP
    return int(min(total, _EST_CAP))


class SelectExplainWorker:
    """在索引 worker 周期末尾运行的增量估算器。"""

    def __init__(self, manifest_path: str, config: SelectExplainConfig | None = None):
        self.manifest_path = str(manifest_path)
        self.config = config or SelectExplainConfig()
        self._table_db_cache: dict[str, list[str]] | None = None
        self._conn = None

    # ---------------- MySQL ----------------

    def _mysql(self):
        if self._conn is None:
            import pymysql

            self._conn = pymysql.connect(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                connect_timeout=10,
                read_timeout=20,
                write_timeout=20,
                charset="utf8mb4",
                autocommit=True,
            )
        return self._conn

    def _close_mysql(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001 - 收尾清理不影响结果
                pass
            self._conn = None

    def _table_dbs(self, table: str) -> list[str]:
        """table -> 含有该表的库列表（单次全量拉取后缓存）。"""

        if self._table_db_cache is None:
            cache: dict[str, list[str]] = {}
            with self._mysql().cursor() as cursor:
                cursor.execute(
                    "SELECT TABLE_NAME, TABLE_SCHEMA FROM information_schema.TABLES"
                    " WHERE TABLE_SCHEMA NOT IN"
                    " ('mysql','sys','information_schema','performance_schema')"
                )
                for name, schema in cursor.fetchall():
                    cache.setdefault(str(name).lower(), []).append(str(schema))
            self._table_db_cache = cache
        return self._table_db_cache.get(table.lower(), [])

    # ---------------- 定库 ----------------

    def resolve_databases(self, sample_sql: str, body: str) -> tuple[list[str], str]:
        """返回（候选库有序列表, 来源标记）。"""

        # 1) 采集器经 Init DB 还原的连接当前库（嵌在注释里）。
        m = _DB_COMMENT_RE.search(sample_sql)
        if m:
            return [m.group(1)], "init_db"
        # 2) 语句里带库名前缀。
        qualified = list(dict.fromkeys(_QUALIFIED_RE.findall(body)))
        if len(qualified) == 1:
            return [qualified[0]], "qualified"
        if len(qualified) > 1:
            # 跨库语句：任选其一作默认库即可（引用都是限定的）。
            return [qualified[0]], "qualified"
        # 3) 不带前缀：按首个表名查唯一归属，多库同名走优先级列表。
        m = _FIRST_TABLE_RE.search(body)
        if not m:
            # 语句根本不引用表（SELECT 1 / SELECT @@var / SELECT NOW()），
            # 扫描行数就是 0，由调用方直接记 ok，不算定库失败。
            return [], "no_table"
        dbs = self._table_dbs(m.group(1))
        if len(dbs) == 1:
            return [dbs[0]], "unique_table"
        if len(dbs) > 1:
            ordered = [db for db in self.config.db_priority if db in dbs]
            ordered += [db for db in dbs if db not in ordered]
            return ordered[:3], "priority"
        return [], "none"

    # ---------------- EXPLAIN ----------------

    def explain_one(self, sample_sql: str) -> dict[str, Any]:
        body = strip_leading_comments(sample_sql).rstrip().rstrip(";")
        lowered = body.lower()
        if not lowered.startswith(("select", "with")):
            return {"status": "skip", "error": "非 SELECT 主体"}
        if ";" in body:
            return {"status": "skip", "error": "疑似多语句"}
        candidates, source = self.resolve_databases(sample_sql, body)
        if source == "no_table":
            return {
                "status": "ok",
                "database": "",
                "db_source": source,
                "est_rows": 0,
                "full_scan": 0,
                "plan": [],
            }
        if not candidates:
            return {"status": "error", "error": "无法定库", "db_source": source}
        last_error = ""
        for database in candidates:
            try:
                with self._mysql().cursor() as cursor:
                    cursor.execute(f"USE `{database}`")
                    cursor.execute("EXPLAIN " + body)
                    columns = [desc[0].lower() for desc in cursor.description]
                    plan = [
                        dict(zip(columns, row)) for row in cursor.fetchall()
                    ]
                compact = [
                    {
                        "id": str(row.get("id") or ""),
                        "table": str(row.get("table") or ""),
                        "type": str(row.get("type") or ""),
                        "key": str(row.get("key") or ""),
                        "rows": int(row.get("rows") or 0),
                        "filtered": float(row.get("filtered") or 0.0),
                        "extra": str(row.get("extra") or ""),
                    }
                    for row in plan
                ]
                return {
                    "status": "ok",
                    "database": database,
                    "db_source": source,
                    "est_rows": estimate_examined_rows(plan),
                    "full_scan": int(
                        any(row["type"].upper() in ("ALL", "INDEX") for row in compact)
                    ),
                    "plan": compact,
                }
            except Exception as exc:  # noqa: BLE001 - 单条失败入库继续
                last_error = f"{type(exc).__name__}: {exc}"
                # 连接级错误直接中断本轮，避免逐条超时把预算烧完。
                if "Lost connection" in last_error or "timed out" in last_error:
                    self._close_mysql()
                    raise
        return {"status": "error", "error": last_error[:500], "db_source": source}

    # ---------------- 主入口 ----------------

    def run_topup(self) -> dict[str, int]:
        """跑一轮增量估算；返回统计。索引 worker 每周期调用一次。"""

        stats = {"scanned": 0, "ok": 0, "error": 0, "skip": 0}
        if not self.config.ready:
            return stats
        conn = sqlite3.connect(self.manifest_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=15000")
        started = time.monotonic()
        try:
            conn.executescript(_SCHEMA)
            retry_cutoff = time.strftime(
                "%Y-%m-%dT%H:%M:%SZ",
                time.gmtime(time.time() - self.config.retry_hours * 3600),
            )
            # 待办 = 没算过的 + 到期重试的错误项；ok 项永不重算（固定保存）。
            rows = conn.execute(
                """
                SELECT st.fingerprint, st.sample_sql
                FROM statements st
                LEFT JOIN select_explain se ON se.fingerprint = st.fingerprint
                WHERE st.action = 'SELECT'
                  AND (
                        se.fingerprint IS NULL
                        OR (se.status = 'error' AND se.updated_utc < :cutoff)
                  )
                LIMIT :limit
                """,
                {"cutoff": retry_cutoff, "limit": self.config.batch},
            ).fetchall()
            for row in rows:
                if time.monotonic() - started > self.config.budget_seconds:
                    break
                stats["scanned"] += 1
                sample = str(row["sample_sql"] or "")
                sha = hashlib.sha256(sample.encode("utf-8", "replace")).hexdigest()
                try:
                    result = self.explain_one(sample)
                except Exception as exc:  # noqa: BLE001 - 连接级失败，下轮再试
                    LOGGER.warning("select explain 本轮中断: %s", exc)
                    break
                status = result.get("status") or "error"
                stats["ok" if status == "ok" else ("skip" if status == "skip" else "error")] += 1
                conn.execute(
                    """
                    INSERT OR REPLACE INTO select_explain(
                        fingerprint, sample_sha, database_name, db_source,
                        est_rows_examined, full_scan, plan_json,
                        status, error, updated_utc
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(row["fingerprint"]),
                        sha,
                        str(result.get("database") or ""),
                        str(result.get("db_source") or ""),
                        int(result.get("est_rows") or 0),
                        int(result.get("full_scan") or 0),
                        json.dumps(
                            result.get("plan") or [], ensure_ascii=False
                        ),
                        "ok" if status == "ok" else status,
                        str(result.get("error") or ""),
                        _utc_now_text(),
                    ),
                )
            conn.commit()
        finally:
            conn.close()
            self._close_mysql()
        if stats["scanned"]:
            LOGGER.info(
                "select explain 增量: scanned=%s ok=%s error=%s skip=%s",
                stats["scanned"],
                stats["ok"],
                stats["error"],
                stats["skip"],
            )
        return stats
