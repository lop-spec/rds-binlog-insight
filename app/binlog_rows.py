"""Binlog rows stored by table in ClickHouse (``binlog_rows_v1``).

One parse per binlog file at ingest; queries read only the requested table's
granules (partition bucket + primary key) and use a text index for keywords.
Coverage is proven from the ingest manifest against the collector catalog, so
a query always reports which parts of the requested window were not searched
(source binlog missing vs. not yet ingested) instead of returning a silent
zero.
"""
from __future__ import annotations

import base64
import http.client
import json
import logging
import os
import re
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import urlencode

from .binlog_lite import RawBinlogError

LOGGER = logging.getLogger(__name__)

DATABASE = "insight"
ROWS_TABLE = f"{DATABASE}.binlog_rows_v1"
STATEMENTS_TABLE = f"{DATABASE}.binlog_statements_v1"
STAGE_TABLE = f"{DATABASE}.binlog_rows_stage_v1"
FILES_TABLE = f"{DATABASE}.binlog_rows_files_v1"
BUFFER_TABLE = f"{DATABASE}.binlog_rows_buf_v1"
# v2 worker buffers are sorted by table bucket: each per-bucket move reads only its own granules
# (8 full scans per binlog before, ~56 s of the ~130 s per file).
WORKER_BUFFER_TABLE = f"{DATABASE}.binlog_rows_buf_worker_v2"
WORKER_LANES_MAX = 8
STORAGE_POLICY = "binlog_rows"
BUCKETS = 8
BUCKET_EXPR = f"cityHash64(lower(database_name), lower(table_name)) % {BUCKETS}"
RETENTION_DAYS = 62
STATEMENT_MAX_BYTES = 65536
TIER = "clickhouse-binlog-rows"
# Lower-cased searchable text; exact-sequence checks run on it.
RAW_EXPR = "lower(concat(before_json, ' ', after_json, ' ', transaction_id, ' ', source_file_name))"
# Index text: non-ASCII runs become spaces so a token is exactly a maximal [a-z0-9] run (splitByNonAlpha alone
# glues digits to adjacent CJK characters: '订单157683' would not contain the token '157683').
# Must stay byte-identical to the ``search`` index expression, otherwise the text index is silently not used.
SEARCH_EXPR = "replaceRegexpAll(" + RAW_EXPR + ", '[^\\\\x00-\\\\x7f]+', ' ')"
NON_BINLOG_HOSTS = ("slow-log", "tabularis", "general-log")
QUERY_DEADLINE_SECONDS = 100
PAGE_DEPTH_LIMIT = 2000
DAY_US = 86_400_000_000
# Newest-first reads merge every part that overlaps the slice at once (~26 MB per part on wide row images).
# While backfill and live lanes write the same day, merged parts mix hours, so a heavy table's day can need
# ~1 GB; a slice that hits the cap is split in halves (newest first) down to QUERY_SPLIT_MIN_US instead of
# failing, and the lower cap keeps a query from pushing ingestion over the server memory limit.
QUERY_MEMORY_BYTES = 800_000_000
QUERY_SPLIT_MIN_US = 10 * 60 * 1_000_000
DATE_TEXT = re.compile(r"\d{4}-\d{2}-\d{2}")


def day_slices(start: int, end: int) -> list[tuple[int, int]]:
    """UTC day slices (partitions are per event_date), newest first, covering [start, end] exactly."""
    slices: list[tuple[int, int]] = []
    hi = int(end)
    while hi >= start:
        lo = max(int(start), (hi // DAY_US) * DAY_US)
        slices.append((lo, hi))
        hi = lo - 1
    return slices
# Rows kept in the table store: anything bound to a table, plus DDL (no table name, kept for audit).
ROW_STORE_FILTER = "(table_name != '' OR operation = 'DDL')"
TOKEN = re.compile(r"[a-z0-9]+")

STAGE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("event_id", "String"), ("event_epoch_us", "Int64"), ("event_time_utc", "DateTime64(6, 'UTC')"),
    ("event_date", "Date"), ("instance_id", "String"), ("host_instance_id", "String"),
    ("source_file_name", "String"), ("raw_event_type", "String"), ("operation", "String"),
    ("database_name", "String"), ("table_name", "String"), ("server_id", "Int64"), ("thread_id", "Int64"),
    ("transaction_id", "String"), ("gtid", "String"), ("start_position", "Int64"), ("end_position", "Int64"),
    ("row_index", "Int32"), ("execution_time_ms", "Int64"), ("error_code", "Int32"), ("sql_kind", "String"),
    ("before_json", "String"), ("after_json", "String"), ("row_query", "LowCardinality(String)"),
    ("connection_id", "String"), ("connection_name", "String"), ("database_account", "String"),
    ("execution_status", "String"), ("error_message", "String"), ("affected_rows", "Int64"),
    ("started_epoch_us", "Int64"), ("finished_epoch_us", "Int64"), ("batch_id", "String"),
    ("statement_index", "Int32"), ("transaction_context_id", "String"), ("_source_part_key", "String"),
)
STAGE_COLUMN_NAMES = tuple(name for name, _ in STAGE_COLUMNS)

RESULT_COLUMNS = (
    "event_id", "event_epoch_us", "instance_id", "host_instance_id", "source_file_name", "raw_event_type",
    "operation", "database_name", "table_name", "server_id", "thread_id", "transaction_id", "gtid",
    "start_position", "end_position", "row_index", "execution_time_ms", "error_code", "sql_kind",
    "before_json", "after_json", "row_query_hash", "connection_id", "connection_name", "database_account",
    "execution_status", "error_message", "affected_rows", "batch_id", "statement_index",
    "transaction_context_id",
)

# Parser NDJSON -> stage row; mirrors storage.ingest_ndjson_file (DuckDB) so the
# same file yields the same event_id through either the Parquet or the raw path.
PARSER_INPUT_STRUCTURE = ", ".join((
    "event_id Nullable(String)", "event_epoch_us Nullable(Int64)", "raw_event_type Nullable(String)",
    "operation Nullable(String)", "database_name Nullable(String)", "table_name Nullable(String)",
    "server_id Nullable(Int64)", "thread_id Nullable(Int64)", "transaction_id Nullable(String)",
    "gtid Nullable(String)", "start_position Nullable(Int64)", "end_position Nullable(Int64)",
    "row_index Nullable(Int32)", "execution_time_ms Nullable(Int64)", "error_code Nullable(Int32)",
    "sql_kind Nullable(String)", "sql_text Nullable(String)", "before_json Nullable(String)", "after_json Nullable(String)",
    "row_query Nullable(String)", "connection_id Nullable(String)", "connection_name Nullable(String)",
    "database_account Nullable(String)", "execution_status Nullable(String)", "error_message Nullable(String)",
    "affected_rows Nullable(Int64)", "started_epoch_us Nullable(Int64)", "finished_epoch_us Nullable(Int64)",
    "batch_id Nullable(String)", "statement_index Nullable(Int32)", "transaction_context_id Nullable(String)",
))


def parser_select_sql() -> str:
    """SELECT list turning parser NDJSON (``input()``) into stage columns.

    Uses ``{file_id}``/``{instance_id}``/``{host_instance_id}``/``{source_file_name}``
    query parameters; the fallback event id is DuckDB's
    ``sha256(file_id || chr(31) || start || chr(31) || end || chr(31) || row || chr(31) || op)``.
    """
    sep = "char(31)"
    fallback = (
        "lower(hex(SHA256(concat({file_id:String}, " + sep + ", toString(coalesce(start_position, 0)), "
        + sep + ", toString(coalesce(end_position, 0)), " + sep + ", toString(coalesce(row_index, 0)), "
        + sep + ", coalesce(operation, '')))))"
    )
    epoch = "coalesce(event_epoch_us, 0)"
    return ", ".join((
        f"if(coalesce(event_id, '') = '', {fallback}, event_id) AS event_id",
        f"{epoch} AS event_epoch_us",
        f"fromUnixTimestamp64Micro({epoch}, 'UTC') AS event_time_utc",
        f"toDate(fromUnixTimestamp64Micro({epoch}, 'UTC')) AS event_date",
        "{instance_id:String} AS instance_id",
        "{host_instance_id:String} AS host_instance_id",
        "{source_file_name:String} AS source_file_name",
        "coalesce(raw_event_type, '') AS raw_event_type",
        "upper(if(coalesce(operation, '') = '', 'OTHER', operation)) AS operation",
        "coalesce(database_name, '') AS database_name",
        "coalesce(table_name, '') AS table_name",
        "coalesce(server_id, 0) AS server_id",
        "coalesce(thread_id, 0) AS thread_id",
        "coalesce(transaction_id, '') AS transaction_id",
        "coalesce(gtid, '') AS gtid",
        "coalesce(start_position, 0) AS start_position",
        "coalesce(end_position, 0) AS end_position",
        "coalesce(row_index, 0) AS row_index",
        "coalesce(execution_time_ms, 0) AS execution_time_ms",
        "coalesce(error_code, 0) AS error_code",
        "coalesce(sql_kind, '') AS sql_kind",
        "coalesce(before_json, '') AS before_json",
        "coalesce(after_json, '') AS after_json",
        "toLowCardinality(if(coalesce(row_query, '') = '' AND coalesce(sql_kind, '') = 'ORIGINAL', "
        "coalesce(sql_text, ''), coalesce(row_query, ''))) AS row_query",
        "coalesce(connection_id, '') AS connection_id",
        "coalesce(connection_name, '') AS connection_name",
        "coalesce(database_account, '') AS database_account",
        "coalesce(execution_status, '') AS execution_status",
        "coalesce(error_message, '') AS error_message",
        "coalesce(affected_rows, 0) AS affected_rows",
        "coalesce(started_epoch_us, 0) AS started_epoch_us",
        "coalesce(finished_epoch_us, 0) AS finished_epoch_us",
        "coalesce(batch_id, '') AS batch_id",
        "coalesce(statement_index, -1) AS statement_index",
        "coalesce(transaction_context_id, '') AS transaction_context_id",
        "concat('raw:', {file_id:String}) AS _source_part_key",
    ))


def build_schema() -> list[str]:
    """Idempotent DDL. The ``binlog_rows`` storage policy lives in the server config."""
    stage_cols = ",\n ".join(f"{name} {ctype}" for name, ctype in STAGE_COLUMNS)
    rows_cols = """
 event_id String CODEC(ZSTD(1)), event_epoch_us Int64 CODEC(DoubleDelta, ZSTD(1)),
 event_time_utc DateTime64(6, 'UTC') CODEC(DoubleDelta, ZSTD(1)), event_date Date, tbl_bucket UInt8,
 instance_id LowCardinality(String), host_instance_id LowCardinality(String), source_file_name LowCardinality(String),
 raw_event_type LowCardinality(String), operation LowCardinality(String), database_name LowCardinality(String),
 table_name LowCardinality(String), server_id Int64 CODEC(Delta(8), ZSTD(1)), thread_id Int64 CODEC(Delta(8), ZSTD(1)),
 transaction_id String CODEC(ZSTD(1)), gtid String CODEC(ZSTD(1)), start_position Int64 CODEC(Delta(8), ZSTD(1)),
 end_position Int64 CODEC(Delta(8), ZSTD(1)), row_index Int32 CODEC(Delta(4), ZSTD(1)),
 execution_time_ms Int64 CODEC(ZSTD(1)), error_code Int32 CODEC(ZSTD(1)), sql_kind LowCardinality(String),
 before_json String CODEC(ZSTD(3)), after_json String CODEC(ZSTD(3)), row_query_hash UInt64 CODEC(ZSTD(1)),
 connection_id String CODEC(ZSTD(1)), connection_name String CODEC(ZSTD(1)), database_account String CODEC(ZSTD(1)),
 execution_status LowCardinality(String), error_message String CODEC(ZSTD(1)), affected_rows Int64 CODEC(ZSTD(1)),
 started_epoch_us Int64 CODEC(ZSTD(1)), finished_epoch_us Int64 CODEC(ZSTD(1)), batch_id String CODEC(ZSTD(1)),
 statement_index Int32 CODEC(ZSTD(1)), transaction_context_id String CODEC(ZSTD(1)), _source_part_key String CODEC(ZSTD(1)),
 INDEX search """ + SEARCH_EXPR + " TYPE text(tokenizer = 'splitByNonAlpha')"
    event_select = []
    for name in STAGE_COLUMN_NAMES:
        if name == "row_query":
            event_select.append(f"if(row_query = '', 0, cityHash64(leftUTF8(row_query, {STATEMENT_MAX_BYTES}))) AS row_query_hash")
            continue
        event_select.append(name)
        if name == "event_date":
            event_select.append(f"toUInt8({BUCKET_EXPR}) AS tbl_bucket")
    return [
        f"""CREATE TABLE IF NOT EXISTS {ROWS_TABLE} ({rows_cols}
) ENGINE = MergeTree
PARTITION BY (event_date, tbl_bucket)
ORDER BY (instance_id, database_name, table_name, event_epoch_us)
TTL event_date + INTERVAL {RETENTION_DAYS} DAY
SETTINGS storage_policy = '{STORAGE_POLICY}', index_granularity = 8192,
 max_bytes_to_merge_at_max_space_in_pool = 2147483648, min_bytes_for_wide_part = 268435456, ttl_only_drop_parts = 1""",
        f"""CREATE TABLE IF NOT EXISTS {STATEMENTS_TABLE} (event_date Date, row_query_hash UInt64, row_query String CODEC(ZSTD(3)))
ENGINE = ReplacingMergeTree PARTITION BY event_date ORDER BY row_query_hash TTL event_date + INTERVAL {RETENTION_DAYS} DAY
SETTINGS storage_policy = '{STORAGE_POLICY}', index_granularity = 8192,
 max_bytes_to_merge_at_max_space_in_pool = 1073741824, ttl_only_drop_parts = 1""",
        f"CREATE TABLE IF NOT EXISTS {STAGE_TABLE} (\n {stage_cols}\n) ENGINE = Null",
        f"CREATE TABLE IF NOT EXISTS {BUFFER_TABLE} AS {STAGE_TABLE} ENGINE = MergeTree ORDER BY tuple() "
        "SETTINGS min_bytes_for_wide_part = 1073741824",
        *(f"CREATE TABLE IF NOT EXISTS {WORKER_BUFFER_TABLE}_{lane} AS {STAGE_TABLE} ENGINE = MergeTree "
          f"ORDER BY ({BUCKET_EXPR}) SETTINGS min_bytes_for_wide_part = 1073741824" for lane in range(WORKER_LANES_MAX)),
        f"""CREATE TABLE IF NOT EXISTS {FILES_TABLE} (
 instance_id LowCardinality(String), file_id String, host_instance_id LowCardinality(String), source_file_name String,
 lo_us Int64, hi_us Int64, rows UInt64, source LowCardinality(String), rq_complete UInt8,
 ingested_at DateTime64(3, 'UTC'), version UInt64
) ENGINE = ReplacingMergeTree(version) ORDER BY (instance_id, file_id)""",
        f"""CREATE MATERIALIZED VIEW IF NOT EXISTS {DATABASE}.binlog_rows_mv_events_v1 TO {ROWS_TABLE} AS
SELECT {", ".join(event_select)}
FROM {STAGE_TABLE}""",
        f"""CREATE MATERIALIZED VIEW IF NOT EXISTS {DATABASE}.binlog_rows_mv_statements_v1 TO {STATEMENTS_TABLE} AS
SELECT event_date, cityHash64(q) AS row_query_hash, q AS row_query
FROM (SELECT any(event_date) AS event_date, leftUTF8(row_query, {STATEMENT_MAX_BYTES}) AS q FROM {STAGE_TABLE}
 WHERE row_query != '' GROUP BY q)""",
    ]


# --------------------------------------------------------------------------- coverage

def partition_filter(partitions: Iterable[Iterable[Any]]) -> str:
    """``event_date IN (...) AND tbl_bucket IN (...)`` for (event_date, bucket) pairs; values are validated."""
    dates, buckets = set(), set()
    for day, bucket in partitions:
        day, bucket = str(day), int(bucket)
        if not DATE_TEXT.fullmatch(day) or not 0 <= bucket < BUCKETS:
            raise ValueError(f"bad partition {day!r}/{bucket!r}")
        dates.add(day)
        buckets.add(bucket)
    return (f"event_date IN ({', '.join(repr(d) for d in sorted(dates))}) "
            f"AND tbl_bucket IN ({', '.join(str(b) for b in sorted(buckets))})")


def purge_file_rows(ch: Any, part_keys: list[str], lo_us: int | None = None, hi_us: int | None = None, *,
                    partitions: list[tuple[str, int]] | None = None, reason: str = "") -> None:
    """Delete one file's rows from the row store.

    The mutation still visits every part of the table (and waits behind merges for a pool slot), but parts
    outside the predicate's partitions are skipped without reading data. ``partitions`` (the
    (event_date, bucket) pairs the move could have written, from the buffer) leave ~40-150 parts whose
    _source_part_key is read, against ~380 for the file's day span with one day of slack each side (the
    fallback for markers without partitions). An empty list means nothing could have been written.
    Unbounded, every part is read; that is kept only for spans that cannot be determined and is logged.
    """
    where = "_source_part_key IN (SELECT arrayJoin(JSONExtract({k:String}, 'Array(String)')))"
    params: dict[str, Any] = {"k": json.dumps(part_keys)}
    if partitions is not None:
        if not partitions:
            LOGGER.info("BINLOG_ROWS_PURGE_SKIPPED parts=%s reason=%s (no partition written)", len(part_keys),
                        reason)
            return
        where = partition_filter(partitions) + " AND " + where
    elif lo_us and hi_us:
        params["d0"] = (datetime.fromtimestamp(int(lo_us) / 1e6, UTC).date() - timedelta(days=1)).isoformat()
        params["d1"] = (datetime.fromtimestamp(int(hi_us) / 1e6, UTC).date() + timedelta(days=1)).isoformat()
        where = "event_date BETWEEN {d0:Date} AND {d1:Date} AND " + where
    else:
        LOGGER.warning("BINLOG_ROWS_PURGE_UNBOUNDED parts=%s reason=%s", len(part_keys), reason or "no time span")
    ch.execute(f"DELETE FROM {ROWS_TABLE} WHERE {where}", params=params,
               settings={"lightweight_deletes_sync": 2, "max_execution_time": 1800}, timeout=1900)


def _epoch_us(text: str) -> int:
    return int(datetime.fromisoformat(str(text).replace("Z", "+00:00")).timestamp() * 1_000_000)


def coverage_runs(files: Iterable[dict[str, Any]], ingested: set[str],
                  start: int | None = None, end: int | None = None) -> dict[str, Any]:
    """Consecutive ingested files form one covered interval; any other file is a gap.

    ``files`` carry ``id, lo, hi, state``. Gaps are merged per reason:
    ``source_missing`` (collector could not obtain the binlog) or ``pending``
    (collected but not yet in ClickHouse). Output is clipped to [start, end].
    """
    ordered = sorted(files, key=lambda f: (f["lo"], f["hi"], f["id"]))
    intervals: list[list[int]] = []
    gaps: list[dict[str, Any]] = []
    run: list[int] | None = None
    previous_was_gap = False
    for item in ordered:
        good = item["id"] in ingested
        if good:
            if run is None:
                run = [item["lo"], item["hi"]]
            else:
                run[1] = max(run[1], item["hi"])
            previous_was_gap = False
            continue
        if run is not None:
            intervals.append(run)
            run = None
        reason = "source_missing" if item.get("state") == "unavailable" else "pending"
        # Only neighbouring missing files merge; a covered run in between always splits gaps.
        if previous_was_gap and gaps[-1]["reason"] == reason:
            gaps[-1]["end"] = max(gaps[-1]["end"], item["hi"])
            gaps[-1]["files"] += 1
        else:
            gaps.append({"start": item["lo"], "end": item["hi"], "reason": reason, "files": 1})
        previous_was_gap = True
    if run is not None:
        intervals.append(run)
    # A covered run wins where it overlaps a neighbouring gap's boundary second.
    if start is not None and end is not None:
        intervals = [[max(lo, start), min(hi, end)] for lo, hi in intervals if max(lo, start) <= min(hi, end)]
        clipped = []
        for gap in gaps:
            lo, hi = max(gap["start"], start), min(gap["end"], end)
            if lo < hi:
                clipped.append({**gap, "start": lo, "end": hi})
        gaps = clipped
        covered = sum(hi - lo for lo, hi in intervals)
        latest = max((f["hi"] for f in ordered), default=None)
        if latest is None:
            gaps.append({"start": start, "end": end, "reason": "no_data", "files": 0})
        elif latest < end and max(latest, start) < end:
            gaps.append({"start": max(latest, start), "end": end, "reason": "not_collected_yet", "files": 0})
        return {"intervals": intervals, "gaps": gaps, "covered_us": covered, "requested_us": max(end - start, 0)}
    return {"intervals": intervals, "gaps": gaps}


GAP_LABELS = {
    "source_missing": "源 Binlog 缺失",
    "pending": "未入库",
    "not_collected_yet": "尚未采集",
    "no_data": "无数据",
}


def coverage_note(summary: dict[str, Any]) -> str:
    gaps = summary.get("gaps") or []
    requested = summary.get("requested_us") or 0
    covered = summary.get("covered_us") or 0
    if not gaps:
        return "所选区间已完整入库并检索"
    counts: dict[str, int] = {}
    for gap in gaps:
        counts[gap["reason"]] = counts.get(gap["reason"], 0) + 1
    parts = "、".join(f"{GAP_LABELS.get(k, k)} {v} 段" for k, v in counts.items())
    pct = f"{covered / requested * 100:.1f}%" if requested else "0%"
    return f"已检索所选时长的 {pct}；未覆盖 {len(gaps)} 段（{parts}），这些时段没有被检索"


# --------------------------------------------------------------------------- query building

def ch_array(values: Iterable[str]) -> str:
    """ClickHouse Array(String) literal for a typed query parameter (constant for index analysis)."""
    escaped = (str(v).replace("\\", "\\\\").replace("'", "\\'") for v in values)
    return "[" + ",".join("'" + v + "'" for v in escaped) + "]"


def keyword_conditions(query: dict[str, Any], params: dict[str, Any]) -> str:
    """Terms are whitespace separated (max 20), case-insensitive, AND/OR by keyword_mode.

    - pure [a-z0-9] term: whole-token match served by the text index;
    - term with separators or non-ASCII characters that still contains [a-z0-9] runs
      (``shop_id``, ``订单157683``): index prefilter on those tokens plus the exact sequence;
    - term without any [a-z0-9] run (e.g. pure Chinese): bounded substring scan.
    """
    terms = str(query.get("keyword") or "").strip().lower().split()[:20]
    if not terms:
        return ""
    clauses = []
    for index, term in enumerate(terms):
        name = f"kw{index}"
        params[name] = term
        tokens = TOKEN.findall(term)
        if TOKEN.fullmatch(term):
            clauses.append(f"hasToken({SEARCH_EXPR}, {{{name}:String}})")
        elif tokens:
            params[name + "t"] = ch_array(tokens)
            clauses.append(
                f"(hasAllTokens({SEARCH_EXPR}, {{{name}t:Array(String)}}) "
                f"AND position({RAW_EXPR}, {{{name}:String}}) > 0)")
        else:
            clauses.append(f"position({RAW_EXPR}, {{{name}:String}}) > 0")
    joiner = " OR " if str(query.get("keyword_mode") or "").upper() == "OR" else " AND "
    return "(" + joiner.join(clauses) + ")"


def memory_limited(exc: BaseException) -> bool:
    """ClickHouse MEMORY_LIMIT_EXCEEDED (per query or server total)."""
    text = str(exc)
    return "Code: 241" in text or "MEMORY_LIMIT_EXCEEDED" in text


def keyword_uses_scan(query: dict[str, Any]) -> bool:
    return any(not TOKEN.findall(t) for t in str(query.get("keyword") or "").strip().lower().split()[:20])


def primary_key_condition(query: dict[str, Any], params: dict[str, Any], row_image_key: str | None) -> str:
    exact = query.get("exact")
    if not isinstance(exact, dict) or str(exact.get("kind") or "").upper() != "PRIMARY_KEY":
        return ""
    if not row_image_key:
        raise RawBinlogError("该表未登记主键列，不能按主键精确查询；可改用关键词", "INDEX_QUERY_UNSUPPORTED")
    value = str(exact.get("value") or "").strip()
    if not value:
        raise RawBinlogError("主键值不能为空", "INVALID_QUERY")
    params.update(pk_key=row_image_key, pk_raw=value, pk_quoted=json.dumps(value, ensure_ascii=False))
    cond = ("(JSONExtractRaw(after_json, {pk_key:String}) IN ({pk_raw:String}, {pk_quoted:String}) "
            "OR JSONExtractRaw(before_json, {pk_key:String}) IN ({pk_raw:String}, {pk_quoted:String}))")
    low = value.lower()
    if TOKEN.fullmatch(low):
        params["pk_token"] = low
        cond = f"(hasToken({SEARCH_EXPR}, {{pk_token:String}}) AND {cond})"
    return cond


def build_query(query: dict[str, Any], start: int, end: int, *, row_image_key: str | None = None
                ) -> tuple[str, dict[str, Any], int, int]:
    instance = str(query.get("instance") or "").strip()
    database = str(query.get("database") or "").strip()
    table = str(query.get("table") or "").strip()
    if not (instance and database and table):
        raise RawBinlogError("按表查询需要单实例、完整库名和表名", "INDEX_QUERY_SCOPE_REQUIRED")
    if str(query.get("source") or "binlog") != "binlog" or query.get("fingerprint"):
        raise RawBinlogError("此条件不支持按表查询", "INDEX_QUERY_UNSUPPORTED")
    limit = min(max(int(query.get("limit") or 100), 1), 1000)
    offset = int(query.get("offset") or 0)
    if offset < 0 or offset + limit + 1 > PAGE_DEPTH_LIMIT:
        raise RawBinlogError(f"分页深度超过{PAGE_DEPTH_LIMIT}，请缩小区间或加关键词", "RAW_QUERY_PAGE_LIMIT")
    params: dict[str, Any] = {"instance": instance, "db": database, "tbl": table, "lo": int(start), "hi": int(end),
                              "d0": datetime.fromtimestamp(start / 1e6, UTC).date().isoformat(),
                              "d1": datetime.fromtimestamp(end / 1e6, UTC).date().isoformat()}
    where = [
        "instance_id = {instance:String}",
        f"tbl_bucket = cityHash64(lower({{db:String}}), lower({{tbl:String}})) % {BUCKETS}",
        "database_name = {db:String}", "table_name = {tbl:String}",
        "event_date BETWEEN toDate({d0:String}) AND toDate({d1:String})",
        "event_epoch_us BETWEEN {lo:Int64} AND {hi:Int64}",
    ]
    ops = [str(v).upper() for v in query.get("operations") or [] if str(v).strip()]
    if ops:
        params["ops"] = ch_array(ops)
        where.append("has({ops:Array(String)}, operation)")
    transaction = str(query.get("transaction") or "").strip()
    if transaction:
        params["txn"] = transaction
        where.append("(transaction_id = {txn:String} OR gtid = {txn:String})")
    for clause in (keyword_conditions(query, params), primary_key_condition(query, params, row_image_key)):
        if clause:
            where.append(clause)
    sql = (f"SELECT {', '.join(RESULT_COLUMNS)} FROM {ROWS_TABLE} WHERE " + " AND ".join(where)
           + " ORDER BY event_epoch_us DESC, end_position DESC, row_index DESC")
    return sql, params, limit, offset


# --------------------------------------------------------------------------- presentation

def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        return json.dumps(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def render_sql(row: dict[str, Any]) -> str:
    """Readable row-image statement (``sql_kind`` PSEUDO); the UI prefers row_query when present."""
    target = f"`{row.get('database_name', '')}`.`{row.get('table_name', '')}`"
    def image(text: Any) -> dict[str, Any]:
        try:
            value = json.loads(text) if text else {}
        except (TypeError, ValueError):
            return {}
        return value if isinstance(value, dict) else {}
    before, after = image(row.get("before_json")), image(row.get("after_json"))
    op = str(row.get("operation") or "").upper()
    pairs = lambda data, sep: sep.join(f"`{k}` = {_literal(v)}" for k, v in data.items())
    if op == "INSERT":
        return (f"INSERT INTO {target} (" + ", ".join(f"`{k}`" for k in after) + ") VALUES ("
                + ", ".join(_literal(v) for v in after.values()) + ")")
    if op == "UPDATE":
        return f"UPDATE {target} SET {pairs(after, ', ')}" + (f" WHERE {pairs(before, ' AND ')}" if before else "")
    if op == "DELETE":
        return f"DELETE FROM {target} WHERE {pairs(before, ' AND ')}"
    return ""


def encode_locator(row: dict[str, Any]) -> str:
    payload = json.dumps([row["instance_id"], row["database_name"], row["table_name"],
                          int(row["event_epoch_us"]), row["event_id"]], separators=(",", ":"), ensure_ascii=False)
    return "ch:" + base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")


def decode_locator(locator: str) -> tuple[str, str, str, int, str] | None:
    if not str(locator or "").startswith("ch:"):
        return None
    text = locator[3:]
    try:
        value = json.loads(base64.urlsafe_b64decode(text + "=" * (-len(text) % 4)))
        instance, database, table, epoch, event_id = value
        return str(instance), str(database), str(table), int(epoch), str(event_id)
    except (ValueError, TypeError, json.JSONDecodeError):
        return None


def present(row: dict[str, Any], statements: dict[int, str]) -> dict[str, Any]:
    out = dict(row)
    epoch = int(out.get("event_epoch_us") or 0)
    out["event_epoch_us"] = epoch
    out["event_time_utc"] = datetime.fromtimestamp(epoch / 1e6, UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    rq_hash = int(out.pop("row_query_hash", 0) or 0)
    out["row_query"] = statements.get(rq_hash, "") if rq_hash else ""
    out["sql_text"] = render_sql(out)
    if not out.get("sql_kind"):
        out["sql_kind"] = "PSEUDO"
    out["event_locator"] = encode_locator(out)
    return out


# --------------------------------------------------------------------------- ClickHouse access

class ChHttp:
    """Minimal HTTP client with per-call settings, parameters and a request body."""

    def __init__(self, host: str, port: int, user: str, password: str):
        self.host, self.port, self.user, self.password = host, int(port), user, password

    @classmethod
    def from_env(cls) -> "ChHttp | None":
        user = os.environ.get("CLICKHOUSE_USER", "").strip()
        password = os.environ.get("CLICKHOUSE_PASSWORD", "")
        if not user:
            return None
        return cls(os.environ.get("RDS_BINLOG_CLICKHOUSE_HOST", "rds-binlog-insight-clickhouse").strip(),
                   int(os.environ.get("RDS_BINLOG_CLICKHOUSE_PORT", "8123") or 8123), user, password)

    def execute(self, sql: str, *, params: dict[str, Any] | None = None, settings: dict[str, Any] | None = None,
                body: bytes | Any = b"", timeout: float = 120, query_id: str = "") -> str:
        values: dict[str, Any] = {"query": sql, "use_query_cache": 0}
        values.update(settings or {})
        if query_id:
            values["query_id"] = query_id
        for key, value in (params or {}).items():
            values[f"param_{key}"] = value
        conn = http.client.HTTPConnection(self.host, self.port, timeout=timeout, blocksize=1024 * 1024)
        try:
            conn.request("POST", "/?" + urlencode(values), body=body,
                         headers={"X-ClickHouse-User": self.user, "X-ClickHouse-Key": self.password})
            response = conn.getresponse()
            text = response.read().decode("utf-8", errors="replace")
            if response.status != 200:
                raise RawBinlogError(f"ClickHouse 返回 {response.status}: {text[:300]}", "CLICKHOUSE_BINLOG_ROWS_UNAVAILABLE")
            return text
        except (OSError, http.client.HTTPException) as exc:
            raise RawBinlogError(f"ClickHouse 不可用：{exc}", "CLICKHOUSE_BINLOG_ROWS_UNAVAILABLE") from exc
        finally:
            conn.close()

    def rows(self, sql: str, **kwargs: Any) -> list[dict[str, Any]]:
        text = self.execute(sql + " FORMAT JSONEachRow", **kwargs)
        return [json.loads(line) for line in text.splitlines() if line]


def serving_enabled() -> bool:
    return os.environ.get("RDS_BINLOG_ROWS_SERVING", "0").strip() == "1"


class BinlogRows:
    """Query/coverage/detail over ``binlog_rows_v1``. Construction is side-effect free."""

    def __init__(self, metadata: Any, http_client: ChHttp, *, registry: Callable[[str, str], str | None] | None = None):
        self.metadata = metadata
        self.ch = http_client
        self.registry = registry or (lambda _db, _tbl: None)
        self._ingested_cache: dict[str, tuple[float, set[str]]] = {}

    @classmethod
    def from_env(cls, metadata: Any, *, registry: Callable[[str, str], str | None] | None = None) -> "BinlogRows | None":
        if not serving_enabled():
            return None
        client = ChHttp.from_env()
        if client is None:
            LOGGER.warning("BINLOG_ROWS_DISABLED reason=missing-clickhouse-credentials")
            return None
        return cls(metadata, client, registry=registry)

    # -- coverage -------------------------------------------------------------
    def _files(self, instance: str, start: int | None, end: int | None) -> list[dict[str, Any]]:
        sql = ("SELECT id, host_instance_id, log_file_name, log_begin_utc, log_end_utc, state FROM binlog_files "
               "WHERE instance_id = ?")
        args: list[Any] = [instance]
        if start is not None and end is not None:
            lo = datetime.fromtimestamp(start / 1e6 - 86400, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            hi = datetime.fromtimestamp(end / 1e6, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            sql += " AND log_begin_utc >= ? AND log_begin_utc <= ?"
            args += [lo, hi]
        out = []
        with self.metadata.connection() as conn:
            for row in conn.execute(sql, args):
                if row["host_instance_id"] in NON_BINLOG_HOSTS or "/" in str(row["log_file_name"] or ""):
                    continue
                try:
                    lo_us, hi_us = _epoch_us(row["log_begin_utc"]), _epoch_us(row["log_end_utc"])
                except ValueError:
                    continue
                if start is not None and end is not None and (hi_us < start or lo_us > end):
                    continue
                out.append({"id": row["id"], "lo": lo_us, "hi": hi_us, "state": row["state"]})
        return out

    def ingested(self, instance: str, *, max_age: float = 15.0) -> set[str]:
        cached = self._ingested_cache.get(instance)
        if cached and time.monotonic() - cached[0] < max_age:
            return cached[1]
        text = self.ch.execute(f"SELECT file_id FROM {FILES_TABLE} FINAL WHERE instance_id = {{i:String}} FORMAT TSV",
                               params={"i": instance}, settings={"max_execution_time": 30}, timeout=40)
        ids = set(text.split())
        self._ingested_cache[instance] = (time.monotonic(), ids)
        return ids

    def coverage(self, instance: str, start: int | None = None, end: int | None = None) -> dict[str, Any]:
        if not instance:
            return {"intervals": [], "gaps": [], "reason": "请选择一个实例", "mode": "binlog-rows"}
        files = self._files(instance, start, end)
        ingested = self.ingested(instance)
        summary = coverage_runs(files, ingested, start, end)
        summary.update(mode="binlog-rows", indexedFiles=sum(1 for f in files if f["id"] in ingested),
                       pendingFiles=sum(1 for f in files if f["id"] not in ingested and f["state"] != "unavailable"),
                       missingFiles=sum(1 for f in files if f["state"] == "unavailable"))
        summary["reason"] = "" if summary["intervals"] else "该实例尚无已入库的 Binlog 区间"
        return summary

    # -- query ----------------------------------------------------------------
    def query(self, query: dict[str, Any], start: int, end: int, *, control: Any | None = None) -> dict[str, Any]:
        database, table = str(query.get("database") or "").strip(), str(query.get("table") or "").strip()
        sql, params, limit, offset = build_query(query, start, end, row_image_key=self.registry(database, table))
        if control is not None:
            control.check_cancelled()
        started = time.monotonic()
        summary = self.coverage(str(query.get("instance") or "").strip(), start, end)
        # Newest-first day slices: each touches one day's partitions (bounded memory) and the scan stops as
        # soon as offset + limit + 1 rows exist, so common keywords over long ranges return quickly.
        wanted = offset + limit + 1
        rows: list[dict[str, Any]] = []
        deadline = time.monotonic() + QUERY_DEADLINE_SECONDS
        pending = day_slices(start, end)
        try:
            while pending:
                lo, hi = pending.pop(0)
                remaining = deadline - time.monotonic()
                if remaining <= 1:
                    raise RawBinlogError("TIMEOUT_EXCEEDED across day slices", "CLICKHOUSE_BINLOG_ROWS_UNAVAILABLE")
                if control is not None:
                    control.check_cancelled()
                slice_params = dict(params, lo=lo, hi=hi,
                                    d0=datetime.fromtimestamp(lo / 1e6, UTC).date().isoformat(),
                                    d1=datetime.fromtimestamp(hi / 1e6, UTC).date().isoformat())
                # Wide row images: reading in-order across many parts keeps one block per part in memory;
                # 8192-row blocks and no per-part read-ahead buffering (read_in_order_use_buffering) keep it
                # near 26 MB per overlapping part.
                settings = {"max_threads": 4, "max_block_size": 8192, "max_execution_time": max(1, int(remaining)),
                            "max_memory_usage": QUERY_MEMORY_BYTES, "read_in_order_use_buffering": 0,
                            "timeout_overflow_mode": "throw", "log_comment": "binlog-rows-query"}
                try:
                    rows += self.ch.rows(sql + f" LIMIT {wanted - len(rows)}", params=slice_params,
                                         settings=settings, timeout=int(remaining) + 15,
                                         query_id=f"binlog-rows-{int(time.time() * 1000)}-{os.getpid()}")
                except RawBinlogError as exc:
                    if not memory_limited(exc) or hi - lo < 2 * QUERY_SPLIT_MIN_US:
                        raise
                    # a failed slice returned nothing; its halves keep the newest-first order
                    mid = lo + (hi - lo) // 2
                    LOGGER.warning("BINLOG_ROWS_QUERY_SPLIT table=%s.%s lo=%s hi=%s reason=%s",
                                   database, table, lo, hi, "total" if "(total)" in str(exc) else "query")
                    pending[0:0] = [(mid + 1, hi), (lo, mid)]
                    continue
                if len(rows) >= wanted:
                    break
        except RawBinlogError as exc:
            if "TIMEOUT_EXCEEDED" in str(exc) or "Timeout exceeded" in str(exc):
                raise RawBinlogError(
                    f"查询超过{QUERY_DEADLINE_SECONDS}秒，未返回不完整结果；"
                    + ("中文/无英文数字的关键词只能逐行扫描，请缩短时间或改用 ID/英文关键词" if keyword_uses_scan(query)
                       else "请缩短时间范围"), "QUERY_DEADLINE_EXCEEDED") from exc
            raise
        rows = rows[offset:]
        has_more = len(rows) > limit
        rows = rows[:limit]
        statements = self._statements(rows)
        presented = [present(r, statements) for r in rows]
        return {
            "rows": presented, "has_more": has_more, "limit": limit, "offset": offset,
            "tiers_used": [TIER], "range_requests": 0, "range_bytes": 0,
            "exact_index_complete": not summary["gaps"], "coverage_found": bool(summary["intervals"]),
            "range_start_epoch_us": start, "range_end_epoch_us": end,
            "covered_intervals": summary["intervals"], "coverage_gaps": summary["gaps"][:200],
            "coverage_gap_count": len(summary["gaps"]), "covered_us": summary.get("covered_us", 0),
            "requested_us": summary.get("requested_us", 0), "coverage_note": coverage_note(summary),
            "query_ms": int((time.monotonic() - started) * 1000),
        }

    def _statements(self, rows: list[dict[str, Any]]) -> dict[int, str]:
        hashes = sorted({int(r.get("row_query_hash") or 0) for r in rows} - {0})
        if not hashes:
            return {}
        days = sorted({datetime.fromtimestamp(int(r["event_epoch_us"]) / 1e6, UTC).date().isoformat() for r in rows})
        found = self.ch.rows(
            f"SELECT row_query_hash, any(row_query) AS row_query FROM {STATEMENTS_TABLE} "
            "WHERE event_date IN (SELECT arrayJoin(JSONExtract({days:String}, 'Array(Date)'))) "
            "AND row_query_hash IN (SELECT arrayJoin(JSONExtract({h:String}, 'Array(UInt64)'))) GROUP BY row_query_hash",
            params={"days": json.dumps(days), "h": json.dumps(hashes)},
            settings={"max_execution_time": 30, "max_threads": 4}, timeout=40)
        return {int(r["row_query_hash"]): r["row_query"] for r in found}

    # -- detail ---------------------------------------------------------------
    def detail(self, locator: str) -> dict[str, Any] | None:
        decoded = decode_locator(locator)
        if decoded is None:
            return None
        instance, database, table, epoch, event_id = decoded
        rows = self.ch.rows(
            f"SELECT {', '.join(RESULT_COLUMNS)} FROM {ROWS_TABLE} WHERE instance_id = {{i:String}} "
            f"AND tbl_bucket = cityHash64(lower({{db:String}}), lower({{tbl:String}})) % {BUCKETS} "
            "AND database_name = {db:String} AND table_name = {tbl:String} "
            "AND event_date = toDate(fromUnixTimestamp64Micro({e:Int64}, 'UTC')) "
            "AND event_epoch_us = {e:Int64} AND event_id = {id:String} LIMIT 1",
            params={"i": instance, "db": database, "tbl": table, "e": epoch, "id": event_id},
            settings={"max_execution_time": 30, "max_threads": 4}, timeout=40)
        if not rows:
            return None
        return present(rows[0], self._statements(rows))


# --------------------------------------------------------------------------- ingestion

class RowsIngestor:
    """Ingest one collected binlog file into binlog_rows_v1 through a local buffer.

    buffer -> row-count verification -> stage (materialized views) -> manifest.
    A failed load only truncates the buffer; a failed move purges the file's rows.
    """

    def __init__(self, http_client: ChHttp, *, buffer_table: str = WORKER_BUFFER_TABLE, oss_base: str = ""):
        self.ch = http_client
        self.buffer = buffer_table
        self.oss_base = oss_base

    LOAD_SETTINGS = {"max_threads": 1, "max_insert_threads": 1, "max_memory_usage": 1_200_000_000,
                     "min_insert_block_size_rows": 16384, "min_insert_block_size_bytes": 16777216,
                     "max_insert_block_size": 16384, "max_execution_time": 0,
                     "max_insert_delayed_streams_for_parallel_write": 1, "input_format_null_as_default": 1,
                     "input_format_skip_unknown_fields": 1,
                     # ClickHouse lets aliases shadow columns; DuckDB (the Parquet path) does not.
                     "prefer_column_name_to_alias": 1}

    def truncate(self) -> None:
        self.ch.execute(f"TRUNCATE TABLE {self.buffer}", settings={"max_execution_time": 300}, timeout=320)

    def load_ndjson_chunk(self, path: Path, entry: dict[str, Any]) -> None:
        sql = (f"INSERT INTO {self.buffer} ({', '.join(STAGE_COLUMN_NAMES)}) SELECT {parser_select_sql()} "
               f"FROM input('{PARSER_INPUT_STRUCTURE}') FORMAT JSONEachRow")
        with path.open("rb") as handle:
            self.ch.execute(sql, params={"file_id": entry["file_id"], "instance_id": entry["instance_id"],
                                         "host_instance_id": entry["host_instance_id"],
                                         "source_file_name": entry["source_file_name"]},
                            settings={**self.LOAD_SETTINGS, "log_comment": "binlog-rows-raw-load"},
                            body=handle, timeout=1800)

    def load_ndjson_stream(self, stream: Any, entry: dict[str, Any]) -> None:
        """Stream parser stdout straight into the buffer (chunked HTTP body, no staging on disk)."""
        sql = (f"INSERT INTO {self.buffer} ({', '.join(STAGE_COLUMN_NAMES)}) SELECT {parser_select_sql()} "
               f"FROM input('{PARSER_INPUT_STRUCTURE}') FORMAT JSONEachRow")
        self.ch.execute(sql, params={"file_id": entry["file_id"], "instance_id": entry["instance_id"],
                                     "host_instance_id": entry["host_instance_id"],
                                     "source_file_name": entry["source_file_name"]},
                        settings={**self.LOAD_SETTINGS, "log_comment": "binlog-rows-raw-load"},
                        body=stream, timeout=3600)

    def buffered_rows(self) -> int:
        return int(self.ch.execute(f"SELECT count() FROM {self.buffer} FORMAT TSV",
                                   settings={"max_execution_time": 120}, timeout=140).strip() or 0)

    def partitions(self) -> list[tuple[str, int]]:
        """(event_date, bucket) pairs the buffered rows will land in; the purge bound for a failed move."""
        text = self.ch.execute(
            f"SELECT DISTINCT toString(event_date), toUInt8({BUCKET_EXPR}) FROM {self.buffer} "
            f"WHERE {ROW_STORE_FILTER} ORDER BY 1, 2 FORMAT TSV", settings={"max_execution_time": 120}, timeout=140)
        pairs = []
        for line in text.splitlines():
            if line.strip():
                day, bucket = line.split("\t")
                pairs.append((day, int(bucket)))
        return pairs

    def move(self, part_keys: list[str], tag: str, *, lo_us: int | None = None, hi_us: int | None = None,
             partitions: list[tuple[str, int]] | None = None) -> None:
        """Buffer -> stage, one table bucket per INSERT.

        A mixed block splits into up to BUCKETS partitions, one tiny part each (~220 parts of ~480 KiB per
        500 MB binlog: an OSS object set each, then merges). Per-bucket blocks land as one part (~33 per
        binlog, ~2.9 MiB) with lower peak memory; bigger blocks instead exceed the 1.2 GB query limit.
        A failed move purges only the partitions of the buckets it attempted (``partitions`` from the buffer).
        """
        bucket = -1
        try:
            for bucket in range(BUCKETS):
                self.ch.execute(
                    f"INSERT INTO {STAGE_TABLE} SELECT * FROM {self.buffer} "
                    f"WHERE {ROW_STORE_FILTER} AND {BUCKET_EXPR} = {bucket}",
                    settings={"max_threads": 1, "max_insert_threads": 1, "max_block_size": 16384,
                              "min_insert_block_size_rows": 16384, "min_insert_block_size_bytes": 16777216,
                              "max_insert_block_size": 16384, "max_memory_usage": 1_200_000_000,
                              "max_execution_time": 0, "log_comment": tag + "-move"}, timeout=3600)
        except RawBinlogError:
            attempted = None if partitions is None else [p for p in partitions if p[1] <= bucket]
            purge_file_rows(self.ch, part_keys, lo_us, hi_us, partitions=attempted, reason="move-failed")
            raise

    def record(self, entry: dict[str, Any], rows: int, source: str, rq_complete: bool) -> None:
        now = time.time()
        self.ch.execute(
            f"INSERT INTO {FILES_TABLE} (instance_id, file_id, host_instance_id, source_file_name, lo_us, hi_us, rows, "
            "source, rq_complete, ingested_at, version) VALUES ({i:String}, {f:String}, {h:String}, {n:String}, "
            "{lo:Int64}, {hi:Int64}, {r:UInt64}, {s:String}, {q:UInt8}, fromUnixTimestamp64Milli({ms:Int64}), {ms:UInt64})",
            params={"i": entry["instance_id"], "f": entry["file_id"], "h": entry["host_instance_id"],
                    "n": entry["source_file_name"], "lo": entry["lo"], "hi": entry["hi"], "r": rows, "s": source,
                    "q": 1 if rq_complete else 0, "ms": int(now * 1000)},
            settings={"max_execution_time": 60}, timeout=80)
