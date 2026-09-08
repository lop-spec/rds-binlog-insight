"""Descriptive, zero-lag correlations of the exact collected slow-log scope.

No sampling, interpolation, smoothing, lag search or cumulative counts. Both
engines call this implementation with integer, canonical event counts from the
same snapshot as their trend. Collection completeness is NOT implied by index
coverage: these coefficients describe the collected logs, not unseen traffic.
"""
from __future__ import annotations

import logging
import math
from collections import Counter
from typing import Any, Mapping

LOGGER = logging.getLogger(__name__)
MIN_BUCKETS = 6


def pearson(x: list[int], y: list[int]) -> tuple[float | None, str]:
    """Integer sufficient statistics avoid catastrophic cancellation.

    Only the final division/square root use floating point. In particular,
    adding a large baseline to every count must not change the coefficient.
    """
    if len(x) != len(y):
        raise ValueError("correlation vectors must be aligned")
    n = len(x)
    if n < 2:
        return None, "insufficient_buckets"
    sx, sy = sum(x), sum(y)
    xx = n * sum(v * v for v in x) - sx * sx
    yy = n * sum(v * v for v in y) - sy * sy
    if not xx:
        return None, "constant_sql"
    if not yy:
        return None, "constant_total"
    xy = n * sum(a * b for a, b in zip(x, y)) - sx * sy
    r = xy / math.sqrt(xx * yy)
    return max(-1.0, min(1.0, r)), "ok"


def differences(values: list[int]) -> list[int]:
    return [b - a for a, b in zip(values, values[1:])]


def attach_correlations(
    orders: Mapping[str, list[dict[str, Any]]],
    sparse_trend: list[dict[str, Any]],
    series: Mapping[str, Mapping[int, int]],
    *,
    start_us: int,
    end_us: int,
    width_us: int,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if width_us <= 0 or end_us < start_us:
        raise ValueError("invalid correlation window")
    # The API window is inclusive. Plot partial edge buckets, but exclude them
    # from coefficients so a 1-second edge is not compared to a 5-minute bin.
    first = start_us // width_us * width_us
    last = end_us // width_us * width_us
    indexed = {int(row["ts"]): dict(row) for row in sparse_trend}
    trend = []
    for ts in range(first, last + 1, width_us):
        row = {"ts": ts, "events": 0, "scan_rows": 0,
               "rows_sent": 0, "query_time_ms_total": 0, **indexed.get(ts, {})}
        row["partial"] = ts < start_us or ts + width_us > end_us + 1
        trend.append(row)
    full = [row for row in trend if not row["partial"]]
    times = [int(row["ts"]) for row in full]
    y = [int(row["events"]) for row in full]
    dy = differences(y)
    meta = {
        "method": "pearson_first_difference_v1",
        "scope": "filtered_collected_slowlog",
        "bucket_us": width_us,
        "complete_buckets": len(full),
        "difference_pairs": max(len(full) - 1, 0),
        "minimum_buckets": MIN_BUCKETS,
        "excluded_partial_buckets": len(trend) - len(full),
        "start_epoch_us": times[0] if times else None,
        "end_epoch_us_exclusive": times[-1] + width_us if times else None,
        "total_events": sum(y),
    }
    results: dict[str, dict[str, Any]] = {}
    for rows in orders.values():
        for row in rows:
            fp = str(row.get("fingerprint") or "")
            if fp not in results:
                counts = series.get(fp)
                x = [int((counts or {}).get(ts, 0)) for ts in times]
                invalid = counts is None or any(a < 0 or a > b for a, b in zip(x, y))
                status = "series_unavailable" if invalid else (
                    "insufficient_buckets" if len(full) < MIN_BUCKETS else "ok")
                r = level = rest = None
                level_status = rest_status = status
                if status == "ok":
                    r, status = pearson(differences(x), dy)
                    level, level_status = pearson(x, y)
                    rest, rest_status = pearson(
                        differences(x), differences([b - a for a, b in zip(x, y)]))
                results[fp] = {
                    "value": r, "status": status,
                    "level_value": level, "level_status": level_status,
                    "without_self_value": rest, "without_self_status": rest_status,
                    "events": sum(x),
                    "event_share": sum(x) / sum(y) if sum(y) and not invalid else None,
                    "active_buckets": sum(v > 0 for v in x),
                    "counts": x if not invalid else [],
                }
            row["correlation"] = results[fp]
    unavailable = Counter(r["status"] for r in results.values() if r["status"] != "ok")
    if unavailable:
        LOGGER.warning("Slow-log correlation unavailable: %s; full_buckets=%d",
                       dict(unavailable), len(full))
    return trend, meta
