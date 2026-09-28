"""Binlog write analytics served from ClickHouse aggregates of the table-ordered row store.

Since the raw-archive switch (2026-09-22) prod binlog is no longer converted to Parquet, so the Parquet
analytics index has no prod input; the rows are parsed straight into ``binlog_rows_v1``. The rows worker
aggregates every committed file from its buffer into two small tables (5-minute buckets, see
``binlog_rows.aggregate_statements``); this module answers the analytics page from them in the same shape as
``AnalyticsIndex.summarize`` so the page is unchanged.

Rows ingested before the aggregates existed were backfilled per table and operation only (fingerprint 0,
no statement text): their statements are shown as synthetic templates, and the coverage notice says so.
Per-transaction figures (duration, size, dependency depth, multi-table) and row-level hotspots need
per-transaction storage and are reported as unavailable instead of zero.

ClickHouse aliases are query-global: an aggregate aliased to a column name changes every other reference to
that column in the query ("sum(events) AS events, sum(events) AS row_events" nests aggregates). Every alias
here is therefore distinct from the table's columns (a_*, db_name, ...); the ranking query renames the source
columns first so its output can keep the Parquet contract's names.
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from typing import Any

from .analytics_index import BUCKET_US, DEFAULT_SQL_ORDER, SQL_ORDERS, AnalyticsIndex
from .binlog_rows import AGG_BUCKET_US, AGG_TABLE, AGG_TXN_TABLE, ALL_TABLES, ch_array, coverage_note

MODE = "binlog-rows"
# fp = 0: no statement text (history before the aggregates, or row events without a Rows_query event).
FINGERPRINT_EXPR = "if(fp = 0, concat('t:', operation, ':', database_name, '.', table_name), toString(fp))"
QUERY_SETTINGS = {"max_threads": 4, "max_execution_time": 60, "max_memory_usage": 1_200_000_000,
                  "log_comment": "binlog-agg-query"}
DDL_WINDOW_ROWS = 20
ALL_ROWS = f"database_name = '{ALL_TABLES}' AND table_name = '{ALL_TABLES}'"
PARALLEL_QUERIES = 4
UNAVAILABLE_TXN_FIELDS = ("max_duration_us", "max_row_events", "cross_second_transactions",
                          "avg_dependency_depth", "max_dependency_depth", "multi_table_transactions",
                          "txn_length_bytes", "avg_duration_us")
assert AGG_BUCKET_US == BUCKET_US, "aggregate buckets must match the analytics trend unit"


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return int(float(value or 0))


class BinlogRowsAnalytics:
    def __init__(self, rows: Any):
        self.rows = rows
        self.ch = rows.ch

    def _q(self, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        return self.ch.rows(sql, params=params, settings=QUERY_SETTINGS, timeout=75)

    def summarize(self, query: dict[str, Any], start_us: int, end_us: int, *, control: Any | None = None
                  ) -> dict[str, Any] | None:
        """None when the row store holds nothing of this instance in the window (caller falls back)."""
        instance = str(query.get("instance") or "").strip()
        if not instance:
            return None
        cov = self.rows.coverage(instance, start_us, end_us)
        if not cov.get("indexedFiles"):
            return None
        limit = min(max(int(query.get("limit") or 50), 1), 500)
        order = str(query.get("order") or DEFAULT_SQL_ORDER)
        order = order if order in SQL_ORDERS else DEFAULT_SQL_ORDER
        start_us, end_us = int(start_us), int(end_us)
        width = AnalyticsIndex._trend_width(start_us, end_us)
        params: dict[str, Any] = {
            "i": instance, "lo": (start_us // AGG_BUCKET_US) * AGG_BUCKET_US, "hi": end_us,
            "d0": datetime.fromtimestamp(start_us / 1e6, UTC).date().isoformat(),
            "d1": datetime.fromtimestamp(end_us / 1e6, UTC).date().isoformat(), "w": width, "n": limit,
        }
        window = ("instance_id = {i:String} AND event_date BETWEEN {d0:Date} AND {d1:Date} "
                  "AND bucket_us BETWEEN {lo:Int64} AND {hi:Int64}")
        objects = window
        for key, column in (("database", "database_name"), ("table", "table_name"), ("operation", "operation")):
            value = str(query.get(key) or "").strip()
            if value:
                params[key] = value.upper() if key == "operation" else value
                objects += f" AND {column} = {{{key}:String}}"
        if control is not None:
            control.check_cancelled()
        # Phase 1: every independent query at once; phase 2: those needing phase-1 keys. The page's wall time
        # is then about the slowest query instead of the sum of ~12 (2.5 s -> ~0.5 s on a 6-hour window).
        with ThreadPoolExecutor(max_workers=PARALLEL_QUERIES, thread_name_prefix="binlog-agg") as pool:
            run = {name: pool.submit(self._q, sql, params) for name, sql in self._phase_one(objects, window).items()}
            ranked = run["ranked"].result()
            texts = pool.submit(self._statement_texts, objects, params,
                                sorted({_int(r["fp_max"]) for r in ranked} - {0}))
            hotspots = [
                {"database_name": r["db_name"], "table_name": r["tbl_name"], "event_count": _int(r["a_events"]),
                 "update_count": _int(r["a_updates"]), "delete_count": _int(r["a_deletes"])}
                for r in run["hotspots"].result()]
            txn_counts = pool.submit(self._table_txns, hotspots, params, window)
            ddl_rows = run["ddl"].result()
            # DDL rows carry no table name in the row store: same-table DML is unknown, not zero
            dml = [pool.submit(self._q, self._ddl_dml_sql(), {
                "i": params["i"], "db": row["db_name"], "tbl": row["tbl_name"],
                "a": _int(row["a_bucket"]) - AGG_BUCKET_US, "b": _int(row["a_bucket"]) + AGG_BUCKET_US})
                if row["tbl_name"] else None for row in ddl_rows]
            if control is not None:
                control.check_cancelled()
            sql, synthetic = self._sql_section(run, ranked, texts.result(), params["i"], order, limit)
            counts = txn_counts.result()
            ddl = [{"event_epoch_us": _int(row["a_first"]), "database_name": row["db_name"],
                    "table_name": row["tbl_name"], "sample_sql": str(row["a_sample"] or "") or "（历史数据未记录语句）",
                    "concurrent_dml_events": (None if f is None
                                              else _int((f.result() or [{}])[0].get("a_events")))}
                   for row, f in zip(ddl_rows, dml)]
            txn_total, txn_trend = run["txn_total"].result(), run["txn_trend"].result()
            all_rows = run["all_rows"].result()[0]
        for item in hotspots:
            item["txn_count"] = counts.get((item["database_name"], item["table_name"]), 0)
        return {
            "mode": MODE,
            "window": {"start_epoch_us": start_us, "end_epoch_us": end_us, "bucket_us": AGG_BUCKET_US,
                       "trend_bucket_us": width, "rollup_width_us": 0},
            "sql": sql,
            "transactions": {
                "mode": MODE, "unavailable": list(UNAVAILABLE_TXN_FIELDS),
                "ddl": [], "multi_table": [], "row_histogram": [], "duration_histogram": [], "longest": [],
                "largest": [],
                "totals": {"transactions": _int(txn_total[0]["a_txns"]) if txn_total else 0,
                           "row_events": _int(all_rows["a_events"]), "payload_bytes": _int(all_rows["a_payload"]),
                           "ddl_transactions": len(ddl), **{key: None for key in UNAVAILABLE_TXN_FIELDS}},
                "trend": [{"ts": _int(r["ts"]), "transactions": _int(r["a_txns"])} for r in txn_trend],
            },
            "locks": {
                "mode": MODE, "long_transactions": [], "large_transactions": [], "row_hotspots": [],
                "table_hotspots": hotspots, "ddl_windows": ddl,
                "risk": {"ddl_events": len(ddl), "row_hotspot_max_txn": None, "longest_transaction_us": None,
                         "largest_transaction_rows": None},
            },
            "coverage": self._coverage(cov, synthetic),
            "evidence": {
                "source": "binlog", "engine": "clickhouse", "lock_analysis": "inferred",
                "notes": [
                    "按表存储（binlog_rows_v1）入库时按 5 分钟桶聚合；执行次数按 RowsEvent 计（每个事件的首行）。",
                    "语句指纹 = normalizeQuery(原始 SQL 前 4096 字符)；没有原始 SQL 的行按操作与对象合成模板。",
                    "事务数按 5 分钟桶精确去重后累计（跨桶事务计两次）；逐事务明细与行级热点需要逐事务存储，本路径不提供。",
                ],
            },
        }

    @staticmethod
    def _ddl_dml_sql() -> str:
        """Same-table DML in the DDL's bucket +-1 (built per call, like every other statement here)."""
        return (f"SELECT sum(events) AS a_events FROM {AGG_TABLE} WHERE instance_id = {{i:String}} "
                "AND database_name = {db:String} AND table_name = {tbl:String} AND operation != 'DDL' "
                "AND bucket_us BETWEEN {a:Int64} AND {b:Int64}")

    @staticmethod
    def _phase_one(objects: str, window: str) -> dict[str, str]:
        rank_columns = ", ".join(f"row_number() OVER (ORDER BY {clause}) AS r_{key}"
                                 for key, clause in SQL_ORDERS.items())
        keep = " OR ".join(f"r_{key} <= {{n:UInt32}}" for key in SQL_ORDERS)
        return {
            "totals": f"SELECT sum(events) AS a_events, sum(executions) AS a_executions, "
                      f"sum(payload_bytes) AS a_payload, sum(slow_events) AS a_slow, "
                      f"uniqExact({FINGERPRINT_EXPR}) AS a_fingerprints, "
                      f"uniqExact(database_name, table_name) AS a_objects, countIf(fp = 0) AS a_synthetic "
                      f"FROM {AGG_TABLE} WHERE {objects}",
            # src renames the columns so the grouped output can use the contract names (events, executions, ...)
            "ranked": f"""SELECT * FROM (SELECT g.*, {rank_columns} FROM (
SELECT if(fp = 0, concat('t:', op, ':', db, '.', tbl), toString(fp)) AS fingerprint, max(fp) AS fp_max,
 sum(e) AS events, sum(x) AS executions, sum(e) AS row_events, sum(pb) AS payload_bytes,
 sum(et) AS exec_time_ms_total, max(em) AS exec_time_ms_max, sum(se) AS slow_events, min(fe) AS first_epoch_us,
 max(le) AS last_epoch_us, uniqExact(db, tbl) AS objects, min(db) AS db_name, min(tbl) AS tbl_name,
 min(op) AS op_name, toInt64(0) AS est_scan_total
FROM (SELECT fp, database_name AS db, table_name AS tbl, operation AS op, events AS e, executions AS x,
       payload_bytes AS pb, exec_time_ms_total AS et, exec_time_ms_max AS em, slow_events AS se,
       first_epoch_us AS fe, last_epoch_us AS le
      FROM {AGG_TABLE} WHERE {objects}) AS src
GROUP BY fingerprint) AS g) WHERE {keep}""",
            "objects": f"SELECT database_name AS db_name, table_name AS tbl_name, sum(events) AS a_events, "
                       f"sum(payload_bytes) AS a_payload, uniqExact({FINGERPRINT_EXPR}) AS a_fingerprints "
                       f"FROM {AGG_TABLE} WHERE {objects} GROUP BY db_name, tbl_name ORDER BY a_events DESC "
                       "LIMIT {n:UInt32}",
            "operations": f"SELECT operation AS op_name, sum(events) AS a_events, sum(payload_bytes) AS a_payload "
                          f"FROM {AGG_TABLE} WHERE {objects} GROUP BY op_name ORDER BY a_events DESC",
            "trend": f"SELECT intDiv(bucket_us, {{w:Int64}}) * {{w:Int64}} AS ts, sum(events) AS a_events, "
                     f"sum(payload_bytes) AS a_payload FROM {AGG_TABLE} WHERE {objects} GROUP BY ts ORDER BY ts",
            "hotspots": f"SELECT database_name AS db_name, table_name AS tbl_name, sum(events) AS a_events, "
                        f"sumIf(events, operation = 'UPDATE') AS a_updates, "
                        f"sumIf(events, operation = 'DELETE') AS a_deletes FROM {AGG_TABLE} WHERE {objects} "
                        "GROUP BY db_name, tbl_name ORDER BY a_events DESC LIMIT {n:UInt32}",
            # grouped: parts of an AggregatingMergeTree merge lazily, so one key can appear once per file
            "ddl": f"SELECT bucket_us AS a_bucket, database_name AS db_name, table_name AS tbl_name, fp AS a_fp, "
                   f"min(first_epoch_us) AS a_first, any(sample_sql) AS a_sample FROM {AGG_TABLE} "
                   f"WHERE {objects} AND operation = 'DDL' GROUP BY a_bucket, db_name, tbl_name, a_fp "
                   f"ORDER BY a_first DESC LIMIT {DDL_WINDOW_ROWS}",
            "txn_total": f"SELECT sum(txns) AS a_txns FROM {AGG_TXN_TABLE} WHERE {window} AND {ALL_ROWS}",
            "txn_trend": f"SELECT intDiv(bucket_us, {{w:Int64}}) * {{w:Int64}} AS ts, sum(txns) AS a_txns "
                         f"FROM {AGG_TXN_TABLE} WHERE {window} AND {ALL_ROWS} GROUP BY ts ORDER BY ts",
            "all_rows": f"SELECT sum(events) AS a_events, sum(payload_bytes) AS a_payload FROM {AGG_TABLE} "
                        f"WHERE {window}",
        }

    @staticmethod
    def _sql_section(run: dict[str, Any], ranked: list[dict[str, Any]], texts: dict[int, tuple[str, str]],
                     instance: str, order: str, limit: int) -> tuple[dict[str, Any], int]:
        totals = run["totals"].result()[0]
        statements: list[tuple[dict[str, Any], dict[str, int]]] = []
        for row in ranked:
            fp = _int(row["fp_max"])
            item = {
                "fingerprint": str(row["fingerprint"]),
                **{k: _int(row[k]) for k in ("events", "executions", "row_events", "payload_bytes",
                                             "exec_time_ms_total", "exec_time_ms_max", "slow_events",
                                             "first_epoch_us", "last_epoch_us", "objects", "est_scan_total")},
                "database_name": row["db_name"], "table_name": row["tbl_name"],
                "operation": row["op_name"], "action": row["op_name"], "instance_id": instance,
                "sample_event_id": None, "est_rows_per_exec": None, "est_db": None, "est_full_scan": None,
            }
            if fp:
                normalized, sample = texts.get(fp, ("", ""))
                item.update(source_kind="rows-query", normalized_sql=normalized, sample_sql=sample)
            else:
                target = f"{row['db_name']}.{row['tbl_name']}" if row["tbl_name"] else row["db_name"]
                item.update(source_kind="synthetic", normalized_sql=f"{row['op_name']} {target}".strip(),
                            sample_sql="")
            statements.append((item, {k: _int(row[f"r_{k}"]) for k in SQL_ORDERS}))
        orders = {key: [item for item, ranks in sorted(statements, key=lambda s: s[1][key]) if ranks[key] <= limit]
                  for key in SQL_ORDERS}
        section = {
            "mode": MODE,
            "order": order,
            "totals": {
                "events": _int(totals["a_events"]), "executions": _int(totals["a_executions"]),
                "row_events": _int(totals["a_events"]), "payload_bytes": _int(totals["a_payload"]),
                "slow_events": _int(totals["a_slow"]), "fingerprints": _int(totals["a_fingerprints"]),
                "objects": _int(totals["a_objects"]), "boundary_events": 0, "est_scan_rows": 0,
                "est_covered_executions": 0,
            },
            "statements": orders[order],
            "orders": orders,
            "objects": [{"database_name": r["db_name"], "table_name": r["tbl_name"], "events": _int(r["a_events"]),
                         "payload_bytes": _int(r["a_payload"]), "fingerprints": _int(r["a_fingerprints"])}
                        for r in run["objects"].result()],
            "operations": [{"operation": r["op_name"], "events": _int(r["a_events"]),
                            "payload_bytes": _int(r["a_payload"])} for r in run["operations"].result()],
            "trend": [{"ts": _int(r["ts"]), "events": _int(r["a_events"]), "payload_bytes": _int(r["a_payload"])}
                      for r in run["trend"].result()],
        }
        return section, _int(totals["a_synthetic"])

    def _statement_texts(self, objects: str, params: dict[str, Any], fps: list[int]) -> dict[int, tuple[str, str]]:
        if not fps:
            return {}
        rows = self._q(
            f"SELECT fp AS a_fp, any(normalized_sql) AS a_normalized, any(sample_sql) AS a_sample FROM {AGG_TABLE} "
            f"WHERE {objects} AND fp IN {{fps:Array(UInt64)}} GROUP BY a_fp",
            {**params, "fps": "[" + ",".join(str(fp) for fp in fps) + "]"})
        return {_int(r["a_fp"]): (str(r["a_normalized"]), str(r["a_sample"])) for r in rows}

    def _table_txns(self, hotspots: list[dict[str, Any]], params: dict[str, Any], window: str
                    ) -> dict[tuple[str, str], int]:
        if not hotspots:
            return {}
        rows = self._q(
            f"SELECT database_name AS db_name, table_name AS tbl_name, sum(txns) AS a_txns "
            f"FROM {AGG_TXN_TABLE} WHERE {window} AND NOT ({ALL_ROWS}) AND table_name IN {{names:Array(String)}} "
            "GROUP BY db_name, tbl_name",
            {**params, "names": ch_array(sorted({str(r["table_name"]) for r in hotspots}))})
        return {(r["db_name"], r["tbl_name"]): _int(r["a_txns"]) for r in rows}

    @staticmethod
    def _coverage(cov: dict[str, Any], synthetic_rows: int) -> dict[str, Any]:
        indexed, pending, missing = (_int(cov.get(k)) for k in ("indexedFiles", "pendingFiles", "missingFiles"))
        notes = [coverage_note(cov)]
        if synthetic_rows:
            notes.append("部分语句无原始 SQL（按表存储上线聚合前的历史，或行事件未带 RowsQuery），按「操作 + 对象」合成展示")
        return {
            "unit": "files", "complete": not cov.get("gaps"), "total_parts": indexed + pending + missing,
            "covered_parts": indexed, "missing_parts": [], "missing_parts_total": pending + missing,
            "missing_parts_truncated": False, "degraded_parts": 0, "hot_modes": {}, "primary_key_tables": 0,
            "scanned_parts": [], "scan_errors": [], "pending_parts": pending, "rollup_width_us": 0,
            "rollup_lag_parts": 0, "note": "；".join(n for n in notes if n),
        }
