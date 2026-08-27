from __future__ import annotations

import argparse
import datetime as dt
import http.client
import json
import re
import sqlite3
import tempfile
import time
import uuid
from pathlib import Path
from urllib.parse import urlencode

import pyarrow as pa
import pyarrow.parquet as pq

from app.clickhouse_client import ClickHouseClient, ClickHouseConfig


TYPE_MAP = {
    "event_id": "String",
    "event_epoch_us": "Int64",
    "event_date": "Date",
    "instance_id": "String",
    "operation": "String",
    "database_name": "String",
    "table_name": "String",
    "fingerprint": "String",
    "sql_id": "String",
    "sql_bytes": "UInt32",
    "query_time_ms": "UInt64",
    "lock_time_ms": "UInt64",
    "rows_examined": "UInt64",
    "rows_sent": "UInt64",
}
SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("event_epoch_us", pa.int64()),
        ("event_date", pa.date32()),
        ("instance_id", pa.string()),
        ("operation", pa.string()),
        ("database_name", pa.string()),
        ("table_name", pa.string()),
        ("fingerprint", pa.string()),
        ("sql_id", pa.string()),
        ("sql_bytes", pa.uint32()),
        ("query_time_ms", pa.uint64()),
        ("lock_time_ms", pa.uint64()),
        ("rows_examined", pa.uint64()),
        ("rows_sent", pa.uint64()),
    ]
)
TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")


def _insert_parquet(
    client: ClickHouseClient,
    table: str,
    path: Path,
) -> None:
    columns = ", ".join(SCHEMA.names)
    input_schema = ", ".join(
        f"{name} {TYPE_MAP[name]}" for name in SCHEMA.names
    )
    sql = (
        f"INSERT INTO {table} ({columns}) SELECT {columns} "
        f"FROM input('{input_schema}') FORMAT Parquet"
    )
    values: dict[str, str | int] = {
        "query": sql,
        "max_threads": 2,
        "max_insert_threads": 1,
        "input_format_parallel_parsing": 0,
        "input_format_parquet_use_native_reader_v3": 0,
        "wait_end_of_query": 1,
        "max_execution_time": 120,
    }
    connection = http.client.HTTPConnection(
        client.config.host,
        client.config.port,
        timeout=150,
    )
    try:
        connection.putrequest("POST", "/?" + urlencode(values))
        for name, value in client._headers().items():
            connection.putheader(name, value)
        connection.putheader("Content-Type", "application/octet-stream")
        connection.putheader("Content-Length", str(path.stat().st_size))
        connection.endheaders()
        with path.open("rb") as source:
            while chunk := source.read(1024 * 1024):
                connection.send(chunk)
        response = connection.getresponse()
        body = response.read().decode("utf-8", errors="replace")
        if response.status != 200:
            raise RuntimeError(
                f"ClickHouse insert HTTP {response.status}: {body[:500]}"
            )
    finally:
        connection.close()


def _export_sqlite(
    source: Path,
    destination: Path,
    *,
    instance: str,
    start_us: int,
    end_us: int,
) -> int:
    connection = sqlite3.connect(
        f"file:{source}?mode=ro",
        uri=True,
        timeout=5,
    )
    connection.row_factory = sqlite3.Row
    cursor = connection.execute(
        """
        SELECT event_id,event_epoch_us,instance_id,operation,database_name,
               table_name,fingerprint,sql_id,sql_bytes,query_time_ms,
               lock_time_ms,rows_examined,rows_sent
        FROM slowlog_events INDEXED BY idx_slowlog_event_analytics
        WHERE is_canonical=1 AND instance_id=?
          AND event_epoch_us>=? AND event_epoch_us<=?
        ORDER BY event_epoch_us
        """,
        (instance, int(start_us), int(end_us)),
    )
    writer = pq.ParquetWriter(
        destination,
        SCHEMA,
        compression="zstd",
        compression_level=1,
    )
    loaded = 0
    try:
        while True:
            rows = cursor.fetchmany(8192)
            if not rows:
                break
            payload: dict[str, list[object]] = {
                name: [] for name in SCHEMA.names
            }
            for row in rows:
                epoch_us = int(row["event_epoch_us"])
                payload["event_id"].append(str(row["event_id"]))
                payload["event_epoch_us"].append(epoch_us)
                payload["event_date"].append(
                    dt.datetime.fromtimestamp(
                        epoch_us / 1_000_000,
                        dt.UTC,
                    ).date()
                )
                for name in (
                    "instance_id",
                    "operation",
                    "database_name",
                    "table_name",
                    "fingerprint",
                    "sql_id",
                ):
                    payload[name].append(str(row[name] or ""))
                for name in (
                    "sql_bytes",
                    "query_time_ms",
                    "lock_time_ms",
                    "rows_examined",
                    "rows_sent",
                ):
                    payload[name].append(max(int(row[name] or 0), 0))
            writer.write_table(pa.Table.from_pydict(payload, schema=SCHEMA))
            loaded += len(rows)
    finally:
        writer.close()
        connection.close()
    return loaded


def _queries(table: str) -> dict[str, str]:
    where = (
        "instance_id={instance:String} "
        "AND event_epoch_us>={start:Int64} "
        "AND event_epoch_us<={end:Int64}"
    )
    return {
        "statements": f"""
            SELECT fingerprint,count() executions,
                   sum(rows_examined) agg_rows_examined_total,
                   max(rows_examined) agg_rows_examined_max,
                   sum(rows_sent) agg_rows_sent_total,
                   max(rows_sent) agg_rows_sent_max,
                   sum(query_time_ms) agg_query_time_ms_total,
                   max(query_time_ms) agg_query_time_ms_max,
                   sum(lock_time_ms) agg_lock_time_ms_total,
                   max(lock_time_ms) agg_lock_time_ms_max,
                   sum(sql_bytes) agg_sql_bytes,
                   min(event_epoch_us) first_epoch_us,
                   max(event_epoch_us) last_epoch_us,
                   uniqExact(tuple(database_name,table_name)) objects,
                   min(instance_id) sample_instance_id,
                   min(database_name) sample_database_name,
                   min(table_name) sample_table_name,
                   min(operation) sample_operation,
                   max(sql_id) sample_sql_id,
                   min(event_id) sample_event_id
            FROM {table} WHERE {where} GROUP BY fingerprint
        """,
        "objects": f"""
            SELECT database_name,table_name,count() events,
                   sum(sql_bytes) payload_bytes,
                   uniqExact(fingerprint) fingerprints,
                   sum(rows_examined) scan_rows,
                   sum(rows_sent) sent_rows,
                   sum(query_time_ms) query_time_ms_total
            FROM {table} WHERE {where}
            GROUP BY database_name,table_name
        """,
        "operations": f"""
            SELECT operation,count() events,sum(sql_bytes) payload_bytes,
                   sum(rows_examined) scan_rows
            FROM {table} WHERE {where} GROUP BY operation
        """,
        "trend": f"""
            SELECT intDiv(event_epoch_us,{{width:Int64}})*{{width:Int64}} ts,
                   count() events,sum(query_time_ms) query_time_ms_total,
                   sum(rows_examined) scan_rows,sum(rows_sent) sent_rows
            FROM {table} WHERE {where} GROUP BY ts ORDER BY ts
        """,
    }


def run(args: argparse.Namespace) -> dict[str, object]:
    if not TABLE_RE.fullmatch(args.table):
        raise ValueError(f"unsafe ClickHouse table name: {args.table!r}")
    client = ClickHouseClient(ClickHouseConfig.from_env())
    ddl = f"""
        CREATE TABLE {args.table}
        (
            event_id String CODEC(ZSTD(1)),
            event_epoch_us Int64 CODEC(DoubleDelta,ZSTD(1)),
            event_date Date CODEC(Delta,ZSTD(1)),
            instance_id LowCardinality(String) CODEC(ZSTD(1)),
            operation LowCardinality(String) CODEC(ZSTD(1)),
            database_name LowCardinality(String) CODEC(ZSTD(1)),
            table_name LowCardinality(String) CODEC(ZSTD(1)),
            fingerprint FixedString(32) CODEC(ZSTD(1)),
            sql_id String CODEC(ZSTD(1)),
            sql_bytes UInt32 CODEC(T64,ZSTD(1)),
            query_time_ms UInt64 CODEC(T64,ZSTD(1)),
            lock_time_ms UInt64 CODEC(T64,ZSTD(1)),
            rows_examined UInt64 CODEC(T64,ZSTD(1)),
            rows_sent UInt64 CODEC(T64,ZSTD(1))
        )
        ENGINE=MergeTree
        PARTITION BY event_date
        PRIMARY KEY (instance_id,event_epoch_us)
        ORDER BY (instance_id,event_epoch_us,fingerprint,event_id)
        SETTINGS index_granularity=4096
    """
    temporary = (
        Path(tempfile.gettempdir())
        / f"slowlog-poc-{uuid.uuid4().hex}.parquet"
    )
    client.query(f"DROP TABLE IF EXISTS {args.table}", timeout=30)
    client.query(ddl, timeout=30)
    try:
        started = time.monotonic()
        loaded = _export_sqlite(
            args.source,
            temporary,
            instance=args.instance,
            start_us=args.start_us,
            end_us=args.end_us,
        )
        export_seconds = time.monotonic() - started
        started = time.monotonic()
        _insert_parquet(client, args.table, temporary)
        insert_seconds = time.monotonic() - started
        parameters = {
            "instance": args.instance,
            "start": args.start_us,
            "end": args.end_us,
            "width": 300_000_000,
        }
        timings: list[float] = []
        first: dict[str, list[dict[str, object]]] = {}
        for repetition in range(args.repeat):
            started = time.monotonic()
            for name, sql in _queries(args.table).items():
                rows = client.json_rows(
                    sql,
                    parameters=parameters,
                    settings={"max_threads": 2, "max_execution_time": 20},
                    timeout=25,
                )
                if repetition == 0:
                    first[name] = rows
            timings.append(round(time.monotonic() - started, 6))
        groups = first["statements"]
        totals = {
            "executions": sum(int(row["executions"]) for row in groups),
            "fingerprints": len(groups),
            "rows_examined": sum(
                int(row["agg_rows_examined_total"]) for row in groups
            ),
            "rows_sent": sum(
                int(row["agg_rows_sent_total"]) for row in groups
            ),
            "query_time_ms_total": sum(
                int(row["agg_query_time_ms_total"]) for row in groups
            ),
            "query_time_ms_max": max(
                (int(row["agg_query_time_ms_max"]) for row in groups),
                default=0,
            ),
            "lock_time_ms_total": sum(
                int(row["agg_lock_time_ms_total"]) for row in groups
            ),
            "lock_time_ms_max": max(
                (int(row["agg_lock_time_ms_max"]) for row in groups),
                default=0,
            ),
        }
        database, table_name = args.table.split(".", 1)
        storage = client.json_rows(
            "SELECT sum(rows) rows,sum(bytes_on_disk) bytes_on_disk "
            "FROM system.parts WHERE active AND database={database:String} "
            "AND table={table:String}",
            parameters={"database": database, "table": table_name},
            timeout=10,
        )
        return {
            "loaded_rows": loaded,
            "parquet_bytes": temporary.stat().st_size,
            "export_seconds": round(export_seconds, 6),
            "insert_seconds": round(insert_seconds, 6),
            "query_seconds": timings,
            "totals": totals,
            "objects": len(first["objects"]),
            "operations": len(first["operations"]),
            "trend_points": len(first["trend"]),
            "clickhouse_storage": storage[0] if storage else {},
        }
    finally:
        temporary.unlink(missing_ok=True)
        if not args.keep_table:
            client.query(f"DROP TABLE IF EXISTS {args.table}", timeout=30)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--instance", required=True)
    parser.add_argument("--start-us", type=int, required=True)
    parser.add_argument("--end-us", type=int, required=True)
    parser.add_argument(
        "--table",
        default="insight.slowlog_events_poc",
    )
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--keep-table", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run(args), ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
