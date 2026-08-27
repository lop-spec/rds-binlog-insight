from __future__ import annotations

import argparse
import copy
import json
import statistics
import time
from pathlib import Path
from typing import Any, Callable


DAY_US = 24 * 60 * 60 * 1_000_000
ORDER_KEYS: dict[str, tuple[str, ...]] = {
    "executions": ("executions", "scan_rows"),
    "events": ("executions", "scan_rows"),
    "row_events": ("rows_sent", "executions"),
    "payload_bytes": ("sql_bytes", "executions"),
    "exec_time": ("query_time_ms_total", "executions"),
    "recent": ("last_epoch_us", "executions"),
    "scan_rows": ("scan_rows", "executions"),
}


def _record_key(row: dict[str, Any]) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def canonical_summary(summary: dict[str, Any]) -> dict[str, Any]:
    """Remove backend-only evidence and canonicalize unordered result sets."""
    result = copy.deepcopy(summary)
    result.pop("clickhouse_slowlog_coverage", None)
    sql = result.get("sql")
    if not isinstance(sql, dict):
        return result
    statements = sql.get("statements")
    if isinstance(statements, list):
        for row in statements:
            if isinstance(row, dict):
                # The SQLite fallback stores one mutable latest literal sample
                # per fingerprint, so a sample can change from events beyond a
                # frozen --end-us.  Counts, normalized SQL, metrics and order
                # are snapshot semantics; this representative literal is not.
                row.pop("sample_sql", None)
        sql["statements"] = sorted(statements, key=_record_key)
    orders = sql.get("orders")
    if isinstance(orders, dict):
        normalized_orders: dict[str, Any] = {}
        for name, rows in sorted(orders.items()):
            if isinstance(rows, list):
                for row in rows:
                    if isinstance(row, dict):
                        row.pop("sample_sql", None)
                normalized_orders[str(name)] = sorted(rows, key=_record_key)
            else:
                normalized_orders[str(name)] = rows
        sql["orders"] = normalized_orders
    for name in ("objects", "operations", "trend"):
        rows = sql.get(name)
        if isinstance(rows, list):
            sql[name] = sorted(rows, key=_record_key)
    return result


def _preview(value: Any) -> Any:
    text = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return text if len(text) <= 500 else text[:497] + "..."


def _first_difference(
    expected: Any,
    actual: Any,
    path: str = "$",
) -> dict[str, Any] | None:
    if type(expected) is not type(actual):
        return {
            "path": path,
            "expected": _preview(expected),
            "actual": _preview(actual),
        }
    if isinstance(expected, dict):
        expected_keys = set(expected)
        actual_keys = set(actual)
        if expected_keys != actual_keys:
            return {
                "path": path,
                "expected_keys": sorted(expected_keys),
                "actual_keys": sorted(actual_keys),
            }
        for key in sorted(expected):
            difference = _first_difference(
                expected[key], actual[key], f"{path}.{key}"
            )
            if difference is not None:
                return difference
        return None
    if isinstance(expected, list):
        if len(expected) != len(actual):
            return {
                "path": path,
                "expected_length": len(expected),
                "actual_length": len(actual),
            }
        for index, (left, right) in enumerate(zip(expected, actual)):
            difference = _first_difference(left, right, f"{path}[{index}]")
            if difference is not None:
                return difference
        return None
    if expected != actual:
        return {
            "path": path,
            "expected": _preview(expected),
            "actual": _preview(actual),
        }
    return None


def compare_summaries(
    source: dict[str, Any],
    target: dict[str, Any],
) -> dict[str, Any]:
    difference = _first_difference(
        canonical_summary(source),
        canonical_summary(target),
    )
    return {"exact": difference is None, "difference": difference}


def _numeric_order_key(row: dict[str, Any], keys: tuple[str, ...]) -> tuple[int, ...]:
    return tuple(int(row.get(key) or 0) for key in keys)


def order_violations(summary: dict[str, Any]) -> list[str]:
    sql = summary.get("sql") or {}
    orders = sql.get("orders") or {}
    violations: list[str] = []
    for name, keys in ORDER_KEYS.items():
        rows = orders.get(name)
        if rows is None:
            continue
        if not isinstance(rows, list):
            violations.append(f"sql.orders.{name} is not a list")
            continue
        previous: tuple[int, ...] | None = None
        for position, row in enumerate(rows):
            if not isinstance(row, dict):
                violations.append(f"sql.orders.{name}[{position}] is not an object")
                break
            current = _numeric_order_key(row, keys)
            if previous is not None and current > previous:
                violations.append(
                    f"sql.orders.{name}[{position}]={current} exceeds previous={previous}"
                )
                break
            previous = current
    selected = str(sql.get("order") or "executions")
    statements = sql.get("statements")
    selected_rows = orders.get(selected)
    if isinstance(statements, list) and isinstance(selected_rows, list):
        if statements != selected_rows:
            violations.append(f"sql.statements does not equal sql.orders.{selected}")
    return violations


def evaluate_gates(
    *,
    source_ready: bool,
    target_ready: bool,
    parity: bool,
    ordered: bool,
    source_seconds: list[float],
    target_seconds: list[float],
    minimum_speedup: float,
) -> dict[str, Any]:
    source_median = statistics.median(source_seconds) if source_seconds else 0.0
    target_median = statistics.median(target_seconds) if target_seconds else 0.0
    speedup = source_median / target_median if target_median > 0 else 0.0
    speedup = round(speedup, 6)
    speedup_passed = bool(target_seconds) and speedup >= float(minimum_speedup)
    return {
        "ok": bool(
            source_ready and target_ready and parity and ordered and speedup_passed
        ),
        "source_ready": bool(source_ready),
        "target_ready": bool(target_ready),
        "parity": bool(parity),
        "ordered": bool(ordered),
        "minimum_speedup": float(minimum_speedup),
        "source_median_seconds": round(source_median, 6),
        "target_median_seconds": round(target_median, 6),
        "speedup": speedup,
        "speedup_passed": speedup_passed,
    }


def fixed_window_readiness(
    source_stats: dict[str, Any],
    target_stats: dict[str, Any],
) -> tuple[bool, bool]:
    """Judge store health without chasing a live tail past ``--end-us``.

    Per-scenario coverage below proves that every source part intersecting the
    fixed window is present in both stores.  Global pending counts may increase
    after that watermark while collection continues, so they are evidence for
    live-tail freshness, not validity of the frozen parity snapshot.
    """

    source_ready = bool(
        source_stats.get("reconcile_complete")
        and int(source_stats.get("failed_parts") or 0) == 0
    )
    target_ready = bool(
        int(target_stats.get("failed_parts") or 0) == 0
        and int(target_stats.get("delete_parts") or 0) == 0
        and int(target_stats.get("reconcile_completed_at_us") or 0) > 0
    )
    return source_ready, target_ready


def fixed_window_slices(
    start_us: int,
    end_us: int,
    *,
    width_us: int = DAY_US,
) -> list[tuple[int, int]]:
    """Split an inclusive fixed watermark into non-overlapping bounded reads."""

    start = int(start_us)
    end = int(end_us)
    width = max(int(width_us), 1)
    if start <= 0 or end < start:
        return []
    result: list[tuple[int, int]] = []
    cursor = start
    while cursor <= end:
        slice_end = min(end, cursor + width - 1)
        result.append((cursor, slice_end))
        cursor = slice_end + 1
    return result


def _timed(call: Callable[[], dict[str, Any]]) -> tuple[dict[str, Any], float]:
    started = time.perf_counter()
    result = call()
    return result, round(time.perf_counter() - started, 6)


def _run_pairs(
    source_call: Callable[[], dict[str, Any]],
    target_call: Callable[[], dict[str, Any]],
    repeat: int,
) -> tuple[dict[str, Any], dict[str, Any], list[float], list[float]]:
    source_result: dict[str, Any] | None = None
    target_result: dict[str, Any] | None = None
    source_seconds: list[float] = []
    target_seconds: list[float] = []
    for repetition in range(max(int(repeat), 1)):
        if repetition % 2:
            target_result, elapsed = _timed(target_call)
            target_seconds.append(elapsed)
            source_result, elapsed = _timed(source_call)
            source_seconds.append(elapsed)
        else:
            source_result, elapsed = _timed(source_call)
            source_seconds.append(elapsed)
            target_result, elapsed = _timed(target_call)
            target_seconds.append(elapsed)
    assert source_result is not None and target_result is not None
    return source_result, target_result, source_seconds, target_seconds


def _source_summary(source: Any, query: dict[str, Any]) -> dict[str, Any]:
    return source.summarize(
        start_epoch_us=int(query["start_epoch_us"]),
        end_epoch_us=int(query["end_epoch_us"]),
        instance=str(query.get("instance") or ""),
        node_id=str(query.get("node_id") or ""),
        database=str(query.get("database") or ""),
        table=str(query.get("table") or ""),
        operation=str(query.get("operation") or ""),
        limit=int(query.get("limit") or 500),
        order=str(query.get("order") or "executions"),
        temp_store_memory=True,
    )


def _target_summary(
    target: Any,
    query: dict[str, Any],
    retention_days: int,
    parts: list[dict[str, Any]],
) -> dict[str, Any]:
    result = target.summarize(
        query,
        retention_days=retention_days,
        parts=parts,
    )
    if result is None:
        raise RuntimeError("ClickHouse slow-log backend declined a covered query")
    return result


def _query_parts(metadata: Any, query: dict[str, Any]) -> list[dict[str, Any]]:
    return metadata.parts_in_range(
        start_epoch_us=int(query["start_epoch_us"]),
        end_epoch_us=int(query["end_epoch_us"]),
        source="slowlog",
        instance=str(query.get("instance") or ""),
    )


def _coverage(
    metadata: Any,
    source: Any,
    target_manifest: Any,
    query: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    parts = _query_parts(metadata, query)
    return parts, source.coverage(parts), target_manifest.coverage(parts)


def _recent_dimensions(source: Any, start_us: int, end_us: int) -> dict[str, str]:
    with source.connection() as connection:
        recent = connection.execute(
            """
            SELECT instance_id,node_id,database_name,table_name,operation
            FROM slowlog_events
            WHERE is_canonical = 1
              AND event_epoch_us >= ? AND event_epoch_us <= ?
            ORDER BY event_epoch_us DESC,event_id DESC
            LIMIT 1
            """,
            (int(start_us), int(end_us)),
        ).fetchone()
        busiest = connection.execute(
            """
            SELECT instance_id,COUNT(*) AS events
            FROM slowlog_events
            WHERE is_canonical = 1
              AND event_epoch_us >= ? AND event_epoch_us <= ?
            GROUP BY instance_id ORDER BY events DESC LIMIT 1
            """,
            (int(start_us), int(end_us)),
        ).fetchone()
    result = {
        "instance": str(busiest["instance_id"] or "") if busiest else "",
        "node_id": str(recent["node_id"] or "") if recent else "",
        "database": str(recent["database_name"] or "") if recent else "",
        "table": str(recent["table_name"] or "") if recent else "",
        "operation": str(recent["operation"] or "") if recent else "",
    }
    return result


def _scenario(
    name: str,
    start_us: int,
    end_us: int,
    limit: int,
    **filters: str,
) -> tuple[str, dict[str, Any]]:
    query: dict[str, Any] = {
        "source": "slowlog",
        "start_epoch_us": int(start_us),
        "end_epoch_us": int(end_us),
        "limit": int(limit),
        "order": "executions",
    }
    query.update({key: value for key, value in filters.items() if value})
    return name, query


def _run_runtime(args: argparse.Namespace) -> dict[str, Any]:
    # Lazy imports keep pure verifier tests independent from PyArrow and runtime
    # credentials. Production execution still uses the exact product classes.
    from app.clickhouse_manifest import ClickHouseManifest
    from app.clickhouse_slowlog import ClickHouseSlowLogQueryBackend
    from app.config import ensure_data_dirs
    from app.metadata import MetadataStore
    from app.slowlog_index import SlowLogIndex

    data_dir = Path(args.data_dir).resolve()
    paths = ensure_data_dirs(data_dir)
    metadata = MetadataStore(data_dir / "metadata.sqlite3", run_migrations=False)
    source = SlowLogIndex(
        paths["index"] / "slowlog.sqlite3",
        run_migrations=False,
    )
    target_manifest = ClickHouseManifest(
        paths["index"] / "clickhouse" / "slowlog-manifest.sqlite3",
        run_migrations=False,
    )
    target = ClickHouseSlowLogQueryBackend(
        metadata,
        data_dir,
        manifest=target_manifest,
        statement_index=source,
        serving_enabled=True,
    )
    settings = metadata.load_settings()
    retention_days = max(int(settings.retention_days), 1)
    source_stats = source.stats()
    target_stats = target_manifest.stats()
    start_us = int(args.start_us or source_stats.get("indexed_start_us") or 0)
    end_us = int(args.end_us or source_stats.get("indexed_watermark_us") or 0)
    if start_us <= 0 or end_us < start_us:
        raise RuntimeError(
            f"invalid indexed slow-log window: start={start_us}, end={end_us}"
        )
    source_ready, target_ready = fixed_window_readiness(
        source_stats,
        target_stats,
    )

    dimensions = _recent_dimensions(source, start_us, end_us)
    recent_start = max(start_us, end_us - int(args.performance_hours) * 60 * 60 * 1_000_000)
    scenarios: list[tuple[str, dict[str, Any], int]] = []
    # Full-window aggregation exceeded the production ClickHouse query-memory
    # ceiling even though the underlying 79万 rows were healthy.  Prove the
    # same full coverage as inclusive, non-overlapping daily slices; exact
    # parity for every slice implies exact parity for their union and keeps
    # each validation query inside the production resource envelope.
    for position, (slice_start, slice_end) in enumerate(
        fixed_window_slices(
            start_us,
            end_us,
            width_us=int(args.slice_hours) * 60 * 60 * 1_000_000,
        )
    ):
        name, query = _scenario(
            f"all_slice_{position:03d}",
            slice_start,
            slice_end,
            args.limit,
        )
        scenarios.append((name, query, 1))
    name, query = _scenario(
        "instance_recent",
        recent_start,
        end_us,
        args.limit,
        instance=dimensions["instance"],
    )
    scenarios.append((name, query, args.repeat))
    name, query = _scenario(
        "object_recent",
        recent_start,
        end_us,
        args.limit,
        database=dimensions["database"],
        table=dimensions["table"],
    )
    scenarios.append((name, query, 1))
    name, query = _scenario(
        "operation_recent",
        recent_start,
        end_us,
        args.limit,
        operation=dimensions["operation"],
    )
    scenarios.append((name, query, 1))
    if dimensions["node_id"]:
        name, query = _scenario(
            "node_recent",
            recent_start,
            end_us,
            args.limit,
            node_id=dimensions["node_id"],
        )
        scenarios.append((name, query, 1))

    results: list[dict[str, Any]] = []
    all_parity = True
    all_ordered = True
    performance_source: list[float] = []
    performance_target: list[float] = []
    for name, query, repeat in scenarios:
        parts, source_coverage, target_coverage = _coverage(
            metadata, source, target_manifest, query
        )
        covered = bool(
            parts
            and source_coverage.get("complete")
            and target_coverage.get("complete")
        )
        scenario_result: dict[str, Any] = {
            "name": name,
            "query": query,
            "source_coverage": source_coverage,
            "target_coverage": {
                **target_coverage,
                "missing_parts": list(target_coverage.get("missing_parts") or [])[:20],
            },
            "covered": covered,
        }
        if not covered:
            scenario_result.update(
                {
                    "parity": False,
                    "order_violations": ["coverage incomplete or query has no source parts"],
                    "source_seconds": [],
                    "target_seconds": [],
                }
            )
            all_parity = False
            all_ordered = False
            results.append(scenario_result)
            continue
        source_result, target_result, source_seconds, target_seconds = _run_pairs(
            lambda q=query: _source_summary(source, q),
            lambda q=query, p=parts: _target_summary(
                target, q, retention_days, p
            ),
            repeat,
        )
        comparison = compare_summaries(source_result, target_result)
        violations = order_violations(source_result) + order_violations(target_result)
        all_parity = all_parity and bool(comparison["exact"])
        all_ordered = all_ordered and not violations
        scenario_result.update(
            {
                "parity": bool(comparison["exact"]),
                "difference": comparison["difference"],
                "order_violations": violations,
                "source_seconds": source_seconds,
                "target_seconds": target_seconds,
                "source_executions": int(
                    source_result.get("sql", {}).get("totals", {}).get("executions") or 0
                ),
                "target_executions": int(
                    target_result.get("sql", {}).get("totals", {}).get("executions") or 0
                ),
            }
        )
        if name == "instance_recent":
            performance_source = source_seconds
            performance_target = target_seconds
        results.append(scenario_result)

    gates = evaluate_gates(
        source_ready=source_ready,
        target_ready=target_ready,
        parity=all_parity,
        ordered=all_ordered,
        source_seconds=performance_source,
        target_seconds=performance_target,
        minimum_speedup=float(args.min_speedup),
    )
    final_source_stats = source.stats()
    final_target_stats = target_manifest.stats()
    if int(final_source_stats.get("failed_parts") or 0) > 0:
        gates["ok"] = False
        gates["source_ready"] = False
    if int(final_target_stats.get("failed_parts") or 0) > 0:
        gates["ok"] = False
        gates["target_ready"] = False
    return {
        "ok": bool(gates["ok"]),
        "window": {"start_epoch_us": start_us, "end_epoch_us": end_us},
        "dimensions": dimensions,
        "gates": gates,
        "source_stats_before": source_stats,
        "source_stats_after": final_source_stats,
        "target_stats_before": target_stats,
        "target_stats_after": final_target_stats,
        "scenarios": results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify full slow-log ClickHouse coverage, parity and speed."
    )
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--start-us", type=int, default=0)
    parser.add_argument("--end-us", type=int, default=0)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--performance-hours", type=int, default=24)
    parser.add_argument("--slice-hours", type=int, default=6)
    parser.add_argument("--min-speedup", type=float, default=2.0)
    args = parser.parse_args()
    args.limit = min(max(int(args.limit), 1), 500)
    args.repeat = min(max(int(args.repeat), 1), 9)
    args.performance_hours = min(max(int(args.performance_hours), 1), 24 * 61)
    args.slice_hours = min(max(int(args.slice_hours), 1), 24)
    args.min_speedup = max(float(args.min_speedup), 0.0)
    try:
        result = _run_runtime(args)
    except Exception as exc:
        result = {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
        }
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0 if result.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
