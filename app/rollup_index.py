"""跨分区时间桶 rollup：让长窗口分析与分区数量脱钩。

背景（2026-08-11 实测）：analytics 的预聚合是**按分区**存的，7 天窗口要归并
35,422 个分区、2,670 万行 sql_stat，`_sql_section` 一段就 >8 分钟。分区平均只
覆盖 17 秒，窗口一长，跨分区归并必然爆炸——这是粒度问题，查询调优解决不了。

这里加一层与分区解耦的时间桶聚合：把 sql_stat / txn_bucket / txn_top /
hot_object 按「小时桶 × 实例 × 对象 × 指纹」预先合并，主键首列是
(bucket_width_us, bucket_epoch_us)，查询按时间范围直接 seek，扫描量只与窗口
长度相关，与分区数无关。实测小时桶压缩 32.7×（1 天：381 万行 → 11.7 万行）。

幂等：不做增量累加（分区会重建，累加无法回退），而是**按桶整体重算**——桶的
值永远等于「该桶时间范围内所有分区的 sql_stat 之和」，重算多少次结果都一样。
rollup_state 只用来找出「哪些桶需要重算」。
"""

from __future__ import annotations

import logging
import sqlite3
import time
from typing import Any, Iterable

LOGGER = logging.getLogger(__name__)

HOUR_US = 3600 * 1_000_000
DAY_US = 24 * HOUR_US
# 每桶保留的 TopN：长窗口的下钻榜单从各桶的 TopN 里再选，是近似值。
TOP_PER_BUCKET = 50
HOT_PER_BUCKET = 200
# 允许 rollup 落后实时多久仍走 rollup 路径（见 covered 注释）。
LAG_TOLERANCE_US = 3 * HOUR_US

ROLLUP_SCHEMA = """
CREATE TABLE IF NOT EXISTS rollup_state (
    part_path TEXT PRIMARY KEY,
    sha256 TEXT NOT NULL,
    rolled_at TEXT NOT NULL
) WITHOUT ROWID;

-- 回填水位：已完成的小时桶推进到哪。当前小时那个桶永远重算（分区还在增），
-- 所以水位只记「已封存」的桶。
CREATE TABLE IF NOT EXISTS rollup_watermark (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    hour_bucket_us INTEGER NOT NULL,
    updated_utc TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sql_rollup (
    bucket_width_us INTEGER NOT NULL,
    bucket_epoch_us INTEGER NOT NULL,
    instance_id TEXT NOT NULL,
    database_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    is_boundary INTEGER NOT NULL,
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
        bucket_width_us, bucket_epoch_us, instance_id,
        database_name, table_name, operation, fingerprint
    )
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS txn_rollup (
    bucket_width_us INTEGER NOT NULL,
    bucket_epoch_us INTEGER NOT NULL,
    instance_id TEXT NOT NULL,
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
    rows_b1 INTEGER NOT NULL, rows_b2 INTEGER NOT NULL, rows_b3 INTEGER NOT NULL,
    rows_b4 INTEGER NOT NULL, rows_b5 INTEGER NOT NULL,
    dur_b1 INTEGER NOT NULL, dur_b2 INTEGER NOT NULL, dur_b3 INTEGER NOT NULL,
    dur_b4 INTEGER NOT NULL, dur_b5 INTEGER NOT NULL,
    PRIMARY KEY(bucket_width_us, bucket_epoch_us, instance_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS txn_top_rollup (
    bucket_width_us INTEGER NOT NULL,
    bucket_epoch_us INTEGER NOT NULL,
    instance_id TEXT NOT NULL,
    txn_key TEXT NOT NULL,
    gtid TEXT NOT NULL,
    xid TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    thread_id INTEGER NOT NULL,
    server_id INTEGER NOT NULL,
    start_epoch_us INTEGER NOT NULL,
    end_epoch_us INTEGER NOT NULL,
    duration_us INTEGER NOT NULL,
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
    part_fragments INTEGER NOT NULL,
    PRIMARY KEY(bucket_width_us, bucket_epoch_us, instance_id, txn_key)
) WITHOUT ROWID;

-- DDL 事件不做时间桶聚合（本来就稀少），只是换成按事件时刻建主键，
-- 免得长窗口下为了几十条 DDL 去 seek 三万多个分区。
CREATE TABLE IF NOT EXISTS ddl_rollup (
    event_epoch_us INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    instance_id TEXT NOT NULL,
    database_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    sample_sql TEXT NOT NULL,
    PRIMARY KEY(event_epoch_us, event_id)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS hot_rollup (
    bucket_width_us INTEGER NOT NULL,
    bucket_epoch_us INTEGER NOT NULL,
    instance_id TEXT NOT NULL,
    key_kind TEXT NOT NULL,
    database_name TEXT NOT NULL,
    table_name TEXT NOT NULL,
    row_key TEXT NOT NULL,
    txn_count INTEGER NOT NULL,
    event_count INTEGER NOT NULL,
    update_count INTEGER NOT NULL,
    delete_count INTEGER NOT NULL,
    first_epoch_us INTEGER NOT NULL,
    last_epoch_us INTEGER NOT NULL,
    PRIMARY KEY(
        bucket_width_us, bucket_epoch_us, instance_id,
        key_kind, database_name, table_name, row_key
    )
) WITHOUT ROWID;
"""

# 同一事务可能跨分区，duration 要用「提交时刻 - 最早 header」重算，
# 与 analytics_index._top_transactions 保持同一口径。
_TXN_DURATION_EXPR = """
    CASE
        WHEN MAX(t.commit_epoch_us) > 0
         AND MIN(NULLIF(t.first_header_us, 0)) > 0
         AND MAX(t.commit_epoch_us) - MIN(NULLIF(t.first_header_us, 0)) >= 1000000
        THEN MAX(t.commit_epoch_us) - MIN(NULLIF(t.first_header_us, 0))
        ELSE MAX(t.duration_us)
    END
"""


def bucket_floor(epoch_us: int, width_us: int) -> int:
    return (int(epoch_us) // int(width_us)) * int(width_us)


def buckets_for_span(start_us: int, end_us: int, width_us: int) -> list[int]:
    first = bucket_floor(start_us, width_us)
    last = bucket_floor(end_us, width_us)
    return list(range(first, last + width_us, width_us))


class RollupIndex:
    """时间桶 rollup 的构建与查询。与 AnalyticsIndex 共用 manifest.sqlite3。"""

    def __init__(self, manifest_path: str) -> None:
        self.manifest_path = str(manifest_path)

    def connection(self, *, readonly: bool = False) -> sqlite3.Connection:
        if readonly:
            conn = sqlite3.connect(
                f"file:{self.manifest_path}?mode=ro", uri=True, timeout=30
            )
        else:
            conn = sqlite3.connect(
                self.manifest_path, timeout=30, isolation_level=None
            )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def ensure_schema(self) -> None:
        conn = self.connection()
        try:
            conn.executescript(ROLLUP_SCHEMA)
        finally:
            conn.close()

    # ------------------------------------------------------------ 构建

    @staticmethod
    def _install_scope(
        conn: sqlite3.Connection, parts: Iterable[dict[str, Any]]
    ) -> int:
        """(part_path, instance_id) 临时表；查询一律 CROSS JOIN 它做主键 seek。"""

        conn.execute("DROP TABLE IF EXISTS temp.rollup_scope")
        conn.execute(
            "CREATE TEMP TABLE rollup_scope("
            "part_path TEXT PRIMARY KEY, instance_id TEXT NOT NULL"
            ") WITHOUT ROWID"
        )
        rows = sorted(
            {
                (str(part["path"]), str(part.get("instance_id") or ""))
                for part in parts
                if part.get("path")
            }
        )
        conn.executemany(
            "INSERT OR IGNORE INTO rollup_scope(part_path, instance_id) VALUES(?, ?)",
            rows,
        )
        return len(rows)

    def pending_parts(self, parts: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """返回尚未并入 rollup（或正文已变）的分区。"""

        if not parts:
            return []
        conn = self.connection()
        try:
            conn.executescript(ROLLUP_SCHEMA)
            done = {
                str(row["part_path"]): str(row["sha256"])
                for row in conn.execute(
                    "SELECT part_path, sha256 FROM rollup_state"
                )
            }
        finally:
            conn.close()
        return [
            part
            for part in parts
            if done.get(str(part["path"])) != str(part.get("sha256") or "")
        ]

    def rebuild_bucket(
        self,
        conn: sqlite3.Connection,
        *,
        bucket_us: int,
        width_us: int,
        parts: list[dict[str, Any]],
    ) -> None:
        """按「该桶时间范围内的全部分区」整体重算一个桶，天然幂等。"""

        end_us = bucket_us + width_us
        scope = self._install_scope(conn, parts)
        for table in ("sql_rollup", "txn_rollup", "txn_top_rollup", "hot_rollup"):
            conn.execute(
                f"DELETE FROM {table} "
                "WHERE bucket_width_us = ? AND bucket_epoch_us = ?",
                (width_us, bucket_us),
            )
        # DDL 没有桶维度，按事件时刻清理本桶覆盖的区间。
        if width_us == HOUR_US:
            conn.execute(
                "DELETE FROM ddl_rollup "
                "WHERE event_epoch_us >= ? AND event_epoch_us < ?",
                (bucket_us, end_us),
            )
        if not scope:
            return
        window = {"bucket": bucket_us, "end": end_us, "width": width_us}

        conn.execute(
            """
            INSERT OR REPLACE INTO sql_rollup(
                bucket_width_us, bucket_epoch_us, instance_id, database_name,
                table_name, operation, fingerprint, is_boundary, events,
                executions, row_events, payload_bytes, exec_time_ms_total,
                exec_time_ms_max, slow_events, first_epoch_us, last_epoch_us,
                sample_event_id
            )
            SELECT :width, :bucket, p.instance_id, s.database_name, s.table_name,
                   s.operation, s.fingerprint,
                   MAX(CASE WHEN st.source_kind = 'boundary' THEN 1 ELSE 0 END),
                   SUM(s.events), SUM(s.executions), SUM(s.row_events),
                   SUM(s.payload_bytes), SUM(s.exec_time_ms_total),
                   MAX(s.exec_time_ms_max), SUM(s.slow_events),
                   MIN(s.first_epoch_us), MAX(s.last_epoch_us),
                   MIN(s.sample_event_id)
            -- INNER JOIN，与精确路径 (_sql_section 非 rollup 分支) 同口径：
            -- statements 与 sql_stat 由 _commit 在同一事务写入，缺记录的行两
            -- 边都不该统计。用 LEFT JOIN 会让两条路径的合计对不上。
            FROM rollup_scope p CROSS JOIN sql_stat s ON s.part_path = p.part_path
            JOIN statements st ON st.fingerprint = s.fingerprint
            WHERE s.bucket_epoch_us >= :bucket AND s.bucket_epoch_us < :end
            GROUP BY p.instance_id, s.database_name, s.table_name,
                     s.operation, s.fingerprint
            """,
            window,
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO txn_rollup(
                bucket_width_us, bucket_epoch_us, instance_id, txn_count,
                row_events, payload_bytes, ddl_txn_count, multi_table_txn_count,
                cross_second_txn_count, dependency_depth_total,
                max_dependency_depth, txn_length_bytes, total_duration_us,
                max_duration_us, max_row_events,
                rows_b1, rows_b2, rows_b3, rows_b4, rows_b5,
                dur_b1, dur_b2, dur_b3, dur_b4, dur_b5
            )
            SELECT :width, :bucket, p.instance_id,
                   SUM(b.txn_count), SUM(b.row_events), SUM(b.payload_bytes),
                   SUM(b.ddl_txn_count), SUM(b.multi_table_txn_count),
                   SUM(b.cross_second_txn_count), SUM(b.dependency_depth_total),
                   MAX(b.max_dependency_depth), SUM(b.txn_length_bytes),
                   SUM(b.total_duration_us), MAX(b.max_duration_us),
                   MAX(b.max_row_events),
                   SUM(b.rows_b1), SUM(b.rows_b2), SUM(b.rows_b3),
                   SUM(b.rows_b4), SUM(b.rows_b5),
                   SUM(b.dur_b1), SUM(b.dur_b2), SUM(b.dur_b3),
                   SUM(b.dur_b4), SUM(b.dur_b5)
            FROM rollup_scope p CROSS JOIN txn_bucket b ON b.part_path = p.part_path
            WHERE b.bucket_epoch_us >= :bucket AND b.bucket_epoch_us < :end
            GROUP BY p.instance_id
            """,
            window,
        )
        # 事务下钻：每桶保留四类各 TopN，长窗口从这些桶级榜单里再选。
        for order_clause, having in (
            ("duration_us DESC, row_events DESC", ""),
            ("row_events DESC, duration_us DESC", ""),
            ("start_epoch_us DESC", "HAVING MAX(t.has_ddl) = 1"),
            ("table_count DESC, row_events DESC", "HAVING MAX(t.multi_table) = 1"),
        ):
            conn.execute(
                f"""
                INSERT OR IGNORE INTO txn_top_rollup(
                    bucket_width_us, bucket_epoch_us, instance_id, txn_key,
                    gtid, xid, transaction_id, thread_id, server_id,
                    start_epoch_us, end_epoch_us, duration_us, row_events,
                    payload_bytes, txn_length_bytes, dependency_depth,
                    table_count, tables_json, ops_json, has_ddl, multi_table,
                    boundary_open, part_fragments
                )
                SELECT :width, :bucket, p.instance_id, t.txn_key,
                       MAX(t.gtid), MAX(t.xid), MAX(t.transaction_id),
                       MAX(t.thread_id), MAX(t.server_id),
                       MIN(t.start_epoch_us), MAX(t.end_epoch_us),
                       {_TXN_DURATION_EXPR} AS duration_us,
                       SUM(t.row_events) AS row_events, SUM(t.payload_bytes),
                       MAX(t.txn_length_bytes), MAX(t.dependency_depth),
                       MAX(t.table_count) AS table_count, MAX(t.tables_json),
                       MAX(t.ops_json), MAX(t.has_ddl), MAX(t.multi_table),
                       MAX(t.boundary_open), COUNT(*)
                FROM rollup_scope p CROSS JOIN txn_top t ON t.part_path = p.part_path
                WHERE t.end_epoch_us >= :bucket AND t.start_epoch_us < :end
                GROUP BY p.instance_id, t.txn_key
                {having}
                ORDER BY {order_clause}
                LIMIT :top
                """,
                {**window, "top": TOP_PER_BUCKET},
            )
        if width_us == HOUR_US:
            conn.execute(
                """
                INSERT OR REPLACE INTO ddl_rollup(
                    event_epoch_us, event_id, instance_id, database_name,
                    table_name, sample_sql
                )
                SELECT d.event_epoch_us, d.event_id, p.instance_id,
                       d.database_name, d.table_name, d.sample_sql
                FROM rollup_scope p CROSS JOIN ddl_event d
                ON d.part_path = p.part_path
                WHERE d.event_epoch_us >= :bucket AND d.event_epoch_us < :end
                """,
                window,
            )
        conn.execute(
            """
            INSERT OR REPLACE INTO hot_rollup(
                bucket_width_us, bucket_epoch_us, instance_id, key_kind,
                database_name, table_name, row_key, txn_count, event_count,
                update_count, delete_count, first_epoch_us, last_epoch_us
            )
            SELECT * FROM (
                SELECT :width, :bucket, p.instance_id, h.key_kind,
                       h.database_name, h.table_name, h.row_key,
                       SUM(h.txn_count), SUM(h.event_count) AS event_count,
                       SUM(h.update_count), SUM(h.delete_count),
                       MIN(h.first_epoch_us), MAX(h.last_epoch_us)
                FROM rollup_scope p
                CROSS JOIN hot_object h ON h.part_path = p.part_path
                WHERE h.last_epoch_us >= :bucket AND h.first_epoch_us < :end
                GROUP BY p.instance_id, h.key_kind, h.database_name,
                         h.table_name, h.row_key
                ORDER BY event_count DESC
                LIMIT :top
            )
            """,
            {**window, "top": HOT_PER_BUCKET},
        )

    def rebuild_day(self, conn: sqlite3.Connection, day_bucket_us: int) -> None:
        """天桶直接从小时桶聚合，不再回读 sql_stat。"""

        window = {
            "day": day_bucket_us,
            "end": day_bucket_us + DAY_US,
            "hour": HOUR_US,
            "width": DAY_US,
        }
        for table in ("sql_rollup", "txn_rollup", "txn_top_rollup", "hot_rollup"):
            conn.execute(
                f"DELETE FROM {table} "
                "WHERE bucket_width_us = ? AND bucket_epoch_us = ?",
                (DAY_US, day_bucket_us),
            )
        conn.execute(
            """
            INSERT OR REPLACE INTO sql_rollup(
                bucket_width_us, bucket_epoch_us, instance_id, database_name,
                table_name, operation, fingerprint, is_boundary, events,
                executions, row_events, payload_bytes, exec_time_ms_total,
                exec_time_ms_max, slow_events, first_epoch_us, last_epoch_us,
                sample_event_id
            )
            SELECT :width, :day, instance_id, database_name, table_name,
                   operation, fingerprint, MAX(is_boundary), SUM(events),
                   SUM(executions), SUM(row_events), SUM(payload_bytes),
                   SUM(exec_time_ms_total), MAX(exec_time_ms_max),
                   SUM(slow_events), MIN(first_epoch_us), MAX(last_epoch_us),
                   MIN(sample_event_id)
            FROM sql_rollup
            WHERE bucket_width_us = :hour
              AND bucket_epoch_us >= :day AND bucket_epoch_us < :end
            GROUP BY instance_id, database_name, table_name, operation, fingerprint
            """,
            window,
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO txn_rollup(
                bucket_width_us, bucket_epoch_us, instance_id, txn_count,
                row_events, payload_bytes, ddl_txn_count, multi_table_txn_count,
                cross_second_txn_count, dependency_depth_total,
                max_dependency_depth, txn_length_bytes, total_duration_us,
                max_duration_us, max_row_events,
                rows_b1, rows_b2, rows_b3, rows_b4, rows_b5,
                dur_b1, dur_b2, dur_b3, dur_b4, dur_b5
            )
            SELECT :width, :day, instance_id, SUM(txn_count), SUM(row_events),
                   SUM(payload_bytes), SUM(ddl_txn_count),
                   SUM(multi_table_txn_count), SUM(cross_second_txn_count),
                   SUM(dependency_depth_total), MAX(max_dependency_depth),
                   SUM(txn_length_bytes), SUM(total_duration_us),
                   MAX(max_duration_us), MAX(max_row_events),
                   SUM(rows_b1), SUM(rows_b2), SUM(rows_b3), SUM(rows_b4),
                   SUM(rows_b5), SUM(dur_b1), SUM(dur_b2), SUM(dur_b3),
                   SUM(dur_b4), SUM(dur_b5)
            FROM txn_rollup
            WHERE bucket_width_us = :hour
              AND bucket_epoch_us >= :day AND bucket_epoch_us < :end
            GROUP BY instance_id
            """,
            window,
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO txn_top_rollup(
                bucket_width_us, bucket_epoch_us, instance_id, txn_key, gtid,
                xid, transaction_id, thread_id, server_id, start_epoch_us,
                end_epoch_us, duration_us, row_events, payload_bytes,
                txn_length_bytes, dependency_depth, table_count, tables_json,
                ops_json, has_ddl, multi_table, boundary_open, part_fragments
            )
            SELECT * FROM (
                SELECT :width, :day, instance_id, txn_key, MAX(gtid), MAX(xid),
                       MAX(transaction_id), MAX(thread_id), MAX(server_id),
                       MIN(start_epoch_us), MAX(end_epoch_us),
                       MAX(duration_us) AS duration_us,
                       SUM(row_events) AS row_events, SUM(payload_bytes),
                       MAX(txn_length_bytes), MAX(dependency_depth),
                       MAX(table_count), MAX(tables_json), MAX(ops_json),
                       MAX(has_ddl), MAX(multi_table), MAX(boundary_open),
                       SUM(part_fragments)
                FROM txn_top_rollup
                WHERE bucket_width_us = :hour
                  AND bucket_epoch_us >= :day AND bucket_epoch_us < :end
                GROUP BY instance_id, txn_key
                ORDER BY duration_us DESC, row_events DESC
                LIMIT :top
            )
            """,
            {**window, "top": TOP_PER_BUCKET * 4},
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO hot_rollup(
                bucket_width_us, bucket_epoch_us, instance_id, key_kind,
                database_name, table_name, row_key, txn_count, event_count,
                update_count, delete_count, first_epoch_us, last_epoch_us
            )
            SELECT * FROM (
                SELECT :width, :day, instance_id, key_kind, database_name,
                       table_name, row_key, SUM(txn_count),
                       SUM(event_count) AS event_count, SUM(update_count),
                       SUM(delete_count), MIN(first_epoch_us), MAX(last_epoch_us)
                FROM hot_rollup
                WHERE bucket_width_us = :hour
                  AND bucket_epoch_us >= :day AND bucket_epoch_us < :end
                GROUP BY instance_id, key_kind, database_name, table_name, row_key
                ORDER BY event_count DESC
                LIMIT :top
            )
            """,
            {**window, "top": HOT_PER_BUCKET},
        )

    def mark_parts(
        self, conn: sqlite3.Connection, parts: list[dict[str, Any]]
    ) -> None:
        stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        conn.executemany(
            "INSERT OR REPLACE INTO rollup_state(part_path, sha256, rolled_at) "
            "VALUES(?, ?, ?)",
            [
                (str(part["path"]), str(part.get("sha256") or ""), stamp)
                for part in parts
            ],
        )

    # ------------------------------------------------------------ 水位

    def watermark(self, conn: sqlite3.Connection) -> int:
        row = conn.execute(
            "SELECT hour_bucket_us FROM rollup_watermark WHERE id = 1"
        ).fetchone()
        return int(row["hour_bucket_us"]) if row else 0

    def set_watermark(self, conn: sqlite3.Connection, hour_bucket_us: int) -> None:
        conn.execute(
            "INSERT OR REPLACE INTO rollup_watermark(id, hour_bucket_us, updated_utc)"
            " VALUES(1, ?, ?)",
            (
                int(hour_bucket_us),
                time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            ),
        )

    def stats(self) -> dict[str, Any]:
        conn = self.connection(readonly=True)
        try:
            result: dict[str, Any] = {}
            for width, label in ((HOUR_US, "hour"), (DAY_US, "day")):
                row = conn.execute(
                    "SELECT COUNT(*) c, MIN(bucket_epoch_us) lo,"
                    " MAX(bucket_epoch_us) hi FROM sql_rollup"
                    " WHERE bucket_width_us = ?",
                    (width,),
                ).fetchone()
                result[label] = {
                    "rows": int(row["c"] or 0),
                    "first_bucket_us": int(row["lo"] or 0),
                    "last_bucket_us": int(row["hi"] or 0),
                }
            result["parts"] = int(
                conn.execute("SELECT COUNT(*) FROM rollup_state").fetchone()[0]
            )
            result["watermark_us"] = self.watermark(conn)
            return result
        except sqlite3.OperationalError:
            return {}
        finally:
            conn.close()

    # ------------------------------------------------------------ 覆盖度

    def covered(self, parts: list[dict[str, Any]]) -> tuple[bool, int]:
        """判断能否走 rollup，并返回滞后未并入的分区数。

        **当前小时的分区不参与判定**：general log 每 2 分钟就落一个新分区，
        要求 100% 并入的话 pending 永远回不到 0，rollup 路径就永远用不上。
        当前小时桶每轮都整体重算，所以真实滞后只有一轮索引周期，代价是窗口
        末尾可能少最近一两分钟的数据——由调用方如实标注，不静默吞掉。
        """

        if not parts:
            return False, 0
        # 容忍最近几个小时的滞后，而不是只容忍当前小时：indexer 每轮只推进
        # 有限个桶，刚跨过整点时上一小时往往还没并入，判据卡太死会让同一个
        # 窗口一会儿秒级、一会儿退回精确路径跑几分钟——那种不稳定比少几分钟
        # 数据更难用。滞后量随结果一起报出。
        current_hour = bucket_floor(
            int(time.time() * 1_000_000) - LAG_TOLERANCE_US, HOUR_US
        )
        conn = self.connection(readonly=True)
        try:
            done = {
                str(row["part_path"]): str(row["sha256"])
                for row in conn.execute("SELECT part_path, sha256 FROM rollup_state")
            }
        except sqlite3.OperationalError:
            return False, 0
        finally:
            conn.close()
        if not done:
            return False, 0
        lag = 0
        for part in parts:
            if done.get(str(part["path"])) == str(part.get("sha256") or ""):
                continue
            # 落在当前小时的分区允许滞后；更早的分区缺失说明 rollup 还没追上
            # 这段历史，必须退回精确路径。
            if int(part.get("min_event_epoch_us") or 0) >= current_hour:
                lag += 1
                continue
            return False, lag
        return True, lag
