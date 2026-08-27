from __future__ import annotations

import argparse
import json
import re
from datetime import date

from app.clickhouse_client import ClickHouseClient, ClickHouseConfig


QUERY_COLUMNS = (
    "event_id",
    "event_epoch_us",
    "event_time_utc",
    "event_date",
    "instance_id",
    "host_instance_id",
    "source_file_name",
    "raw_event_type",
    "operation",
    "database_name",
    "table_name",
    "server_id",
    "thread_id",
    "transaction_id",
    "gtid",
    "start_position",
    "end_position",
    "row_index",
    "execution_time_ms",
    "error_code",
    "sql_kind",
    "sql_text",
    "before_json",
    "after_json",
    "row_query",
    "connection_id",
    "connection_name",
    "database_account",
    "execution_status",
    "error_message",
    "affected_rows",
    "started_epoch_us",
    "finished_epoch_us",
    "batch_id",
    "statement_index",
    "transaction_context_id",
    "_source_part_key",
    "_content_revision",
)

_QUALIFIED_IDENTIFIER = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$"
)


def _table(value: str) -> str:
    if not _QUALIFIED_IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe qualified ClickHouse table: {value!r}")
    return value


def _day(value: str) -> str:
    parsed = date.fromisoformat(value)
    if parsed.isoformat() != value:
        raise ValueError(f"Invalid ISO date: {value!r}")
    return value


def _projection(value: str) -> str:
    if value and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Unsafe ClickHouse projection: {value!r}")
    return value


def _partition_stats(
    client: ClickHouseClient,
    table: str,
    day: str,
    min_epoch_us: int,
) -> dict[str, object]:
    rows = client.json_rows(
        f"""
        SELECT count() AS rows,
               min(event_epoch_us) AS min_epoch_us,
               max(event_epoch_us) AS max_epoch_us,
               uniqExact(_source_part_key) AS source_parts
        FROM {table}
        WHERE event_date = {{day:Date}}
          AND event_epoch_us >= {{min_epoch_us:Int64}}
        """,
        parameters={"day": day, "min_epoch_us": min_epoch_us},
        settings={"max_execution_time": 600, "max_threads": 1},
        timeout=610,
    )
    return dict(rows[0])


def _range_stats(
    client: ClickHouseClient,
    table: str,
    day: str,
    start_us: int,
    end_us: int,
    *,
    source_projection: str = "",
) -> dict[str, object]:
    hash_expression = "cityHash64(" + ",".join(QUERY_COLUMNS) + ")"
    settings: dict[str, str | int] = {
        "max_execution_time": 600,
        "max_threads": 1,
    }
    if source_projection:
        settings["preferred_optimize_projection_name"] = source_projection
    rows = client.json_rows(
        f"""
        SELECT count() AS rows,
               min(event_epoch_us) AS min_epoch_us,
               max(event_epoch_us) AS max_epoch_us,
               sum({hash_expression}) AS hash_sum,
               groupBitXor({hash_expression}) AS hash_xor
        FROM {table}
        WHERE event_date = {{day:Date}}
          AND event_epoch_us >= {{start:Int64}}
          AND event_epoch_us < {{end:Int64}}
        """,
        parameters={"day": day, "start": start_us, "end": end_us},
        settings=settings,
        timeout=610,
    )
    return dict(rows[0])


def _emit(payload: dict[str, object]) -> None:
    print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), flush=True)


def _create_view(
    client: ClickHouseClient,
    source: str,
    target: str,
    view: str,
) -> None:
    columns = ",".join(QUERY_COLUMNS)
    client.query(
        f"""
        CREATE MATERIALIZED VIEW IF NOT EXISTS {view}
        TO {target}
        AS SELECT {columns} FROM {source}
        """,
        settings={"max_execution_time": 120},
        timeout=130,
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build one reverse-query partition in bounded ranges, verify it, "
            "and atomically install it."
        )
    )
    parser.add_argument("--day", required=True, type=_day)
    parser.add_argument("--source", default="insight.events", type=_table)
    parser.add_argument("--target", default="insight.events_query", type=_table)
    parser.add_argument(
        "--stage", default="insight.events_query_stage", type=_table
    )
    parser.add_argument(
        "--view", default="insight.events_query_mv", type=_table
    )
    parser.add_argument("--bucket-seconds", type=int, default=300)
    parser.add_argument("--min-epoch-us", type=int, default=0)
    parser.add_argument("--source-projection", type=_projection, default="")
    parser.add_argument("--reset-stage", action="store_true")
    parser.add_argument("--replace-partition", action="store_true")
    parser.add_argument("--create-view", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.bucket_seconds <= 3600:
        raise ValueError("--bucket-seconds must be between 1 and 3600")

    client = ClickHouseClient(ClickHouseConfig.from_env())
    min_epoch_us = max(int(args.min_epoch_us), 0)
    source_stats = _partition_stats(
        client, args.source, args.day, min_epoch_us
    )
    source_rows = int(source_stats["rows"] or 0)
    stage_all_stats = _partition_stats(client, args.stage, args.day, 0)
    if args.reset_stage and int(stage_all_stats["rows"] or 0):
        client.query(
            f"ALTER TABLE {args.stage} DROP PARTITION '{args.day}'",
            settings={"max_execution_time": 120},
            timeout=130,
        )
    stage_stats = _partition_stats(client, args.stage, args.day, min_epoch_us)
    if int(stage_stats["rows"] or 0):
        raise RuntimeError(
            "Stage partition is not empty; pass --reset-stage to rebuild it"
        )

    _emit({"state": "start", "day": args.day, "source": source_stats})
    source_ranges: list[tuple[int, int, dict[str, object]]] = []
    hash_sum = 0
    hash_xor = 0
    if source_rows:
        start_us = int(source_stats["min_epoch_us"])
        stop_us = int(source_stats["max_epoch_us"]) + 1
        bucket_us = args.bucket_seconds * 1_000_000
        cursor = (start_us // bucket_us) * bucket_us
        columns = ",".join(QUERY_COLUMNS)
        bucket = 0
        while cursor < stop_us:
            next_cursor = min(cursor + bucket_us, stop_us)
            expected = _range_stats(
                client,
                args.source,
                args.day,
                cursor,
                next_cursor,
                source_projection=args.source_projection,
            )
            if int(expected["rows"] or 0):
                insert_settings: dict[str, str | int] = {
                    "max_execution_time": 600,
                    "max_threads": 1,
                    "max_insert_threads": 1,
                    "max_block_size": 32768,
                    "max_insert_block_size": 32768,
                    "min_insert_block_size_rows": 0,
                    "min_insert_block_size_bytes": 0,
                }
                if args.source_projection:
                    insert_settings["preferred_optimize_projection_name"] = (
                        args.source_projection
                    )
                client.query(
                    f"""
                    INSERT INTO {args.stage} ({columns})
                    SELECT {columns} FROM {args.source}
                    WHERE event_date = {{day:Date}}
                      AND event_epoch_us >= {{start:Int64}}
                      AND event_epoch_us < {{end:Int64}}
                    """,
                    parameters={
                        "day": args.day,
                        "start": cursor,
                        "end": next_cursor,
                    },
                    settings=insert_settings,
                    timeout=610,
                )
            actual = _range_stats(
                client, args.stage, args.day, cursor, next_cursor
            )
            if actual != expected:
                raise RuntimeError(
                    "Stage/source range parity mismatch: "
                    + json.dumps(
                        {
                            "start": cursor,
                            "end": next_cursor,
                            "source": expected,
                            "stage": actual,
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                )
            source_ranges.append((cursor, next_cursor, expected))
            hash_sum = (
                hash_sum + int(expected["hash_sum"] or 0)
            ) & ((1 << 64) - 1)
            hash_xor ^= int(expected["hash_xor"] or 0)
            cursor = next_cursor
            bucket += 1
            if bucket == 1 or bucket % 12 == 0 or cursor >= stop_us:
                _emit(
                    {
                        "state": "progress",
                        "day": args.day,
                        "buckets": bucket,
                        "through_epoch_us": cursor,
                    }
                )

    source_stats["hash_sum"] = hash_sum
    source_stats["hash_xor"] = hash_xor
    stage_stats = _partition_stats(client, args.stage, args.day, min_epoch_us)
    if stage_stats != {
        key: source_stats[key]
        for key in ("rows", "min_epoch_us", "max_epoch_us", "source_parts")
    }:
        raise RuntimeError(
            "Stage/source parity mismatch: "
            + json.dumps(
                {"source": source_stats, "stage": stage_stats},
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
    if args.replace_partition:
        client.query(
            f"""
            ALTER TABLE {args.target}
            REPLACE PARTITION '{args.day}' FROM {args.stage}
            """,
            settings={"max_execution_time": 600},
            timeout=610,
        )
        target_stats = _partition_stats(
            client, args.target, args.day, min_epoch_us
        )
        if target_stats != stage_stats:
            raise RuntimeError(
                "Installed target/source parity mismatch: "
                + json.dumps(
                    {"source": stage_stats, "target": target_stats},
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            )
        for start_us, end_us, expected in source_ranges:
            actual = _range_stats(
                client, args.target, args.day, start_us, end_us
            )
            if actual != expected:
                raise RuntimeError(
                    "Installed target/source range parity mismatch: "
                    + json.dumps(
                        {
                            "start": start_us,
                            "end": end_us,
                            "source": expected,
                            "target": actual,
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                )
        client.query(
            f"ALTER TABLE {args.stage} DROP PARTITION '{args.day}'",
            settings={"max_execution_time": 120},
            timeout=130,
        )
    if args.create_view:
        _create_view(client, args.source, args.target, args.view)
    _emit(
        {
            "state": "complete",
            "day": args.day,
            "source": source_stats,
            "installed": bool(args.replace_partition),
            "view_created": bool(args.create_view),
        }
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
