from __future__ import annotations

import argparse
import hashlib
import http.client
import json
import os
import statistics
import time
from typing import Any
from urllib.parse import urlencode, urlsplit


TOP_COLUMNS = (
    "event_id",
    "event_epoch_us",
    "source_file_name",
    "end_position",
    "row_index",
)


def _canonical_hash(rows: list[dict[str, Any]]) -> str:
    canonical = [
        [
            str(row.get("event_id") or ""),
            int(row.get("event_epoch_us") or 0),
            str(row.get("source_file_name") or ""),
            int(row.get("end_position") or 0),
            int(row.get("row_index") or 0),
        ]
        for row in rows
    ]
    payload = json.dumps(
        canonical,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _request(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    body: bytes | None = None,
    timeout: int = 30,
) -> bytes:
    parsed = urlsplit(url)
    connection = http.client.HTTPConnection(
        parsed.hostname,
        parsed.port or 80,
        timeout=timeout,
    )
    try:
        path = parsed.path or "/"
        if parsed.query:
            path += "?" + parsed.query
        connection.request(method, path, body=body, headers=headers or {})
        response = connection.getresponse()
        response_body = response.read()
        if response.status != 200:
            raise RuntimeError(
                f"HTTP {response.status} from {parsed.hostname}: "
                + response_body[:500].decode("utf-8", errors="replace")
            )
        return response_body
    finally:
        connection.close()


def _current_api_once(
    base_url: str,
    host_header: str,
    query: dict[str, str | int],
    timeout_seconds: int = 60,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    body = _request(
        base_url.rstrip("/") + "/api/events?" + urlencode(query),
        headers={"Host": host_header} if host_header else {},
        timeout=max(int(timeout_seconds), 1),
    )
    payload = json.loads(body.decode("utf-8"))
    if not bool(payload.get("ok")):
        raise RuntimeError("Current API returned ok=false")
    data = dict(payload["data"])
    rows = [dict(row) for row in data.pop("rows")]
    details = {
        key: data.get(key)
        for key in (
            "tiers_used",
            "query_certificate_hit",
            "query_certificate_recorded",
            "query_certificate_part_count",
            "indexed_parts",
            "structural_indexed_parts",
            "oss_range_parts_read",
            "range_bytes",
            "unavailable_parts",
        )
    }
    return rows, details


def _clickhouse_once(
    host: str,
    port: int,
    user: str,
    password: str,
    table: str,
    query: dict[str, str | int],
) -> list[dict[str, Any]]:
    sql = f"""
        SELECT {', '.join(TOP_COLUMNS)} FROM {table}
        WHERE event_epoch_us >= {{start:Int64}}
          AND event_epoch_us <= {{end:Int64}}
          AND event_date >= toDate(
              fromUnixTimestamp64Micro({{start:Int64}}), 'UTC'
          )
          AND event_date <= toDate(
              fromUnixTimestamp64Micro({{end:Int64}}), 'UTC'
          )
          AND lowerUTF8(instance_id) = lowerUTF8({{instance:String}})
          AND positionCaseInsensitiveUTF8(
              database_name, {{database:String}}
          ) > 0
          AND positionCaseInsensitiveUTF8(
              table_name, {{table_name:String}}
          ) > 0
          AND raw_event_type NOT IN ('TABULARIS_AUDIT', 'SLOW_LOG')
        ORDER BY event_epoch_us DESC, source_file_name DESC,
                 end_position DESC, row_index DESC, event_id DESC,
                 _content_revision DESC, _source_part_key DESC
        LIMIT {{raw_limit:UInt64}} OFFSET {{raw_offset:UInt64}}
        FORMAT JSONEachRow
    """
    wanted = int(query["limit"])
    batch_size = max(wanted * 4, 256)
    raw_offset = 0
    seen: set[str] = set()
    unique_rows: list[dict[str, Any]] = []
    while len(unique_rows) < wanted:
        parameters: dict[str, str | int] = {
            "query": sql,
            "use_query_cache": 0,
            "max_threads": 2,
            "param_start": query["startEpochUs"],
            "param_end": query["endEpochUs"],
            "param_instance": query["instance"],
            "param_database": query["database"],
            "param_table_name": query["table"],
            "param_raw_limit": batch_size,
            "param_raw_offset": raw_offset,
        }
        connection = http.client.HTTPConnection(host, port, timeout=30)
        try:
            connection.request(
                "POST",
                "/?" + urlencode(parameters),
                body=b"",
                headers={
                    "X-ClickHouse-User": user,
                    "X-ClickHouse-Key": password,
                },
            )
            response = connection.getresponse()
            body = response.read().decode("utf-8", errors="replace")
            if response.status != 200:
                raise RuntimeError(
                    f"ClickHouse HTTP {response.status}: {body[:500]}"
                )
        finally:
            connection.close()
        batch = [json.loads(line) for line in body.splitlines() if line]
        for row in batch:
            event_id = str(row.get("event_id") or "")
            if event_id in seen:
                continue
            seen.add(event_id)
            unique_rows.append(row)
            if len(unique_rows) >= wanted:
                break
        raw_offset += len(batch)
        if len(batch) < batch_size:
            break
    return unique_rows[:wanted]


def _measure(callback, repeats: int) -> tuple[list[float], list[list[dict[str, Any]]], list[dict[str, Any]]]:
    durations: list[float] = []
    row_sets: list[list[dict[str, Any]]] = []
    metadata: list[dict[str, Any]] = []
    for _ in range(repeats):
        started = time.perf_counter()
        result = callback()
        durations.append(time.perf_counter() - started)
        if isinstance(result, tuple):
            rows, details = result
            metadata.append(dict(details))
        else:
            rows = result
            metadata.append({})
        row_sets.append(rows)
    return durations, row_sets, metadata


def _summary(
    durations: list[float],
    row_sets: list[list[dict[str, Any]]],
    metadata: list[dict[str, Any]],
) -> dict[str, Any]:
    hashes = [_canonical_hash(rows) for rows in row_sets]
    if len(set(hashes)) != 1 or len({len(rows) for rows in row_sets}) != 1:
        raise RuntimeError("Benchmark result changed between repetitions")
    return {
        "durations_seconds": [round(value, 6) for value in durations],
        "first_seconds": round(durations[0], 6),
        "p50_seconds": round(statistics.median(durations), 6),
        "rows": len(row_sets[0]),
        "top100_hash": hashes[0],
        "metadata": metadata,
    }


def evaluate_benchmark(
    current_summary: dict[str, Any],
    clickhouse_summary: dict[str, Any],
    *,
    minimum_speedup: float,
) -> dict[str, Any]:
    exact_match = bool(
        int(current_summary.get("rows") or 0)
        == int(clickhouse_summary.get("rows") or 0)
        and str(current_summary.get("top100_hash") or "")
        == str(clickhouse_summary.get("top100_hash") or "")
    )
    current_first = float(current_summary.get("first_seconds") or 0.0)
    target_first = float(clickhouse_summary.get("first_seconds") or 0.0)
    current_p50 = float(current_summary.get("p50_seconds") or 0.0)
    target_p50 = float(clickhouse_summary.get("p50_seconds") or 0.0)
    first_speedup = current_first / target_first if target_first > 0 else 0.0
    p50_speedup = current_p50 / target_p50 if target_p50 > 0 else 0.0
    minimum = max(float(minimum_speedup), 0.0)
    speedup_passed = bool(target_p50 > 0 and p50_speedup >= minimum)
    return {
        "ok": exact_match and speedup_passed,
        "exact_match": exact_match,
        "minimum_speedup": minimum,
        "speedup_passed": speedup_passed,
        "first_speedup": round(first_speedup, 3),
        "p50_speedup": round(p50_speedup, 3),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--current-api", required=True)
    parser.add_argument("--current-host-header", default="")
    parser.add_argument("--clickhouse-host", required=True)
    parser.add_argument("--clickhouse-port", type=int, default=8123)
    parser.add_argument("--clickhouse-table", default="insight_poc.events_poc")
    parser.add_argument("--start-epoch-us", type=int, required=True)
    parser.add_argument("--end-epoch-us", type=int, required=True)
    parser.add_argument("--instance", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--min-speedup", type=float, default=2.0)
    args = parser.parse_args()
    user = os.environ.get("CLICKHOUSE_USER", "").strip()
    password = os.environ.get("CLICKHOUSE_PASSWORD", "")
    if not user or not password:
        raise RuntimeError("CLICKHOUSE_USER and CLICKHOUSE_PASSWORD are required")

    query: dict[str, str | int] = {
        "source": "binlog",
        "instance": args.instance,
        "database": args.database,
        "table": args.table,
        "startEpochUs": args.start_epoch_us,
        "endEpochUs": args.end_epoch_us,
        "limit": 100,
    }
    current = _measure(
        lambda: _current_api_once(
            args.current_api,
            args.current_host_header,
            query,
        ),
        args.repeats,
    )
    clickhouse = _measure(
        lambda: _clickhouse_once(
            args.clickhouse_host,
            args.clickhouse_port,
            user,
            password,
            args.clickhouse_table,
            query,
        ),
        args.repeats,
    )
    current_summary = _summary(*current)
    clickhouse_summary = _summary(*clickhouse)
    gate = evaluate_benchmark(
        current_summary,
        clickhouse_summary,
        minimum_speedup=args.min_speedup,
    )
    result = {
        "window": query,
        "current": current_summary,
        "clickhouse": clickhouse_summary,
        **gate,
    }
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0 if gate["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
