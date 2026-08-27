from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from app.clickhouse_query import ClickHouseQueryBackend
from app.metadata import MetadataStore
from tools.clickhouse_poc_benchmark import (
    _current_api_once,
    _measure,
    _summary,
    evaluate_benchmark,
)


def _raw_once(
    backend: ClickHouseQueryBackend,
    query: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    gate_started = time.monotonic()
    deadline = gate_started + 30.0
    gate_retries = 0
    while True:
        result = backend.query_events(
            query,
            retention_days=3650,
            limit_cap=1000,
        )
        if result is not None:
            break
        if time.monotonic() >= deadline:
            source = backend.metadata.clickhouse_change_tracking_state()
            raise RuntimeError(
                "Raw OSS coverage gate stayed closed for 30 seconds: "
                + json.dumps(source, sort_keys=True)
            )
        gate_retries += 1
        time.sleep(0.5)
    coverage = dict(result.get("clickhouse_coverage") or {})
    return [dict(row) for row in result["rows"]], {
        "tiers_used": list(result.get("tiers_used") or []),
        "candidate_parts": int(result.get("indexed_parts") or 0),
        "has_more": bool(result.get("has_more")),
        "coverage_complete": bool(coverage.get("complete")),
        "source_complete": bool(coverage.get("source_complete")),
        "source_pending": bool(coverage.get("source_pending")),
        "gate_retries": gate_retries,
        "gate_wait_seconds": round(gate_retries * 0.5, 3),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Compare the current SQL Insight OSS path with the exact-key "
            "ClickHouse raw-OSS backend using identical Top-N semantics."
        )
    )
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--current-api", required=True)
    parser.add_argument("--current-host-header", default="")
    parser.add_argument("--start-epoch-us", type=int, required=True)
    parser.add_argument("--end-epoch-us", type=int, required=True)
    parser.add_argument("--instance", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--min-speedup", type=float, default=2.0)
    parser.add_argument("--current-timeout-seconds", type=int, default=180)
    args = parser.parse_args()

    repeats = max(int(args.repeats), 1)
    current_query: dict[str, str | int] = {
        "source": "binlog",
        "instance": args.instance,
        "database": args.database,
        "table": args.table,
        "startEpochUs": args.start_epoch_us,
        "endEpochUs": args.end_epoch_us,
        "limit": 100,
    }
    raw_query: dict[str, Any] = {
        "source": "binlog",
        "instance": args.instance,
        "database": args.database,
        "table": args.table,
        "start_epoch_us": args.start_epoch_us,
        "end_epoch_us": args.end_epoch_us,
        "limit": 100,
    }

    metadata = MetadataStore(
        args.data_dir / "metadata.sqlite3",
        run_migrations=False,
    )
    try:
        backend = ClickHouseQueryBackend.from_env(metadata, args.data_dir)
        if backend is None or not backend.raw_serving:
            raise RuntimeError("ClickHouse raw-OSS serving backend is unavailable")
        current = _measure(
            lambda: _current_api_once(
                args.current_api,
                args.current_host_header,
                current_query,
                args.current_timeout_seconds,
            ),
            repeats,
        )
        raw = _measure(lambda: _raw_once(backend, raw_query), repeats)
    finally:
        metadata.close()

    current_summary = _summary(*current)
    raw_summary = _summary(*raw)
    gate = evaluate_benchmark(
        current_summary,
        raw_summary,
        minimum_speedup=args.min_speedup,
    )
    result = {
        "window": current_query,
        "current": current_summary,
        "clickhouse_raw_oss": raw_summary,
        **gate,
    }
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0 if bool(gate["ok"]) else 2


if __name__ == "__main__":
    raise SystemExit(main())
