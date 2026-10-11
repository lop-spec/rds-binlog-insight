"""Where a requested analysis window lands when the index does not fully cover it.

Two repairs, in this order:

1. *truncated* — only the tail is missing (the newest data is still being indexed): keep the requested start, cut the
   end back to the covered edge. The analysed time point stays the one that was asked for.
2. *shifted* — the start itself is not covered: the nearest same-length window that is, earlier before later at equal
   distance (the 09-30 "snap to the nearest indexed period" rule).

This module is the pure part for sources whose coverage is a list of covered intervals (the Binlog row store). The slow-log
rise investigation reads part spans instead and applies the same two repairs in ``slowlog_impact_service``; both report the
change in one shape (``adjusted_payload``) so the page announces it the same way.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Iterable

LOGGER = logging.getLogger(__name__)

MINUTE_US = 60_000_000
HOUR_US = 60 * MINUTE_US
DAY_US = 24 * HOUR_US
SEARCH_REACH_US = 6 * HOUR_US  # how far from the requested window to look for a covered one


def floor_to(value_us: int, step_us: int) -> int:
    return value_us // step_us * step_us


def adjusted_payload(kind: str, reason: str, requested_start_us: int, requested_end_us: int,
                     start_us: int, end_us: int) -> dict[str, Any]:
    payload: dict[str, Any] = {"kind": kind, "reason": reason, "requested_start_us": int(requested_start_us),
                               "requested_end_us": int(requested_end_us)}
    if kind == "truncated":
        payload["truncated_us"] = int(requested_end_us) - int(end_us)
    else:
        payload["shift_us"] = int(start_us) - int(requested_start_us)
    return payload


def _inside(intervals: list[tuple[int, int]], low: int, high: int) -> bool:
    return any(a <= low and high <= b for a, b in intervals)


def ready_edge(intervals: Iterable[Iterable[int]], now_us: int) -> int | None:
    """The newest covered instant not in the future, or None when nothing is covered."""
    edges = [int(b) for _a, b in intervals if int(b) <= now_us]
    return max(edges) if edges else None


def align_to_intervals(start_us: int, end_us: int, intervals: Iterable[Iterable[int]], *, now_us: int,
                       step_us: int = 5 * MINUTE_US, min_span_us: int | None = None,
                       reach_us: int = SEARCH_REACH_US, floor_us: int = 0) -> dict[str, Any] | None:
    """None when [start, end] lies inside one covered interval; otherwise the repaired window.

    ``{"kind": "truncated" | "shifted", "start_us", "end_us"}``, or ``{"kind": "none"}`` when no covered window of
    that length exists within reach (the caller then runs the requested window and reports its own coverage).
    Repaired edges sit on ``step_us`` boundaries; a truncated window keeps at least ``min_span_us`` (one step by default).
    """
    covered = sorted((int(a), int(b)) for a, b in intervals if int(b) >= int(a))
    if _inside(covered, start_us, end_us):
        return None
    length = end_us - start_us
    minimum = step_us if min_span_us is None else min_span_us
    for a, b in covered:
        if a <= start_us <= b:
            cut = floor_to(min(end_us, b, now_us), step_us)
            if cut - start_us >= minimum:
                return {"kind": "truncated", "start_us": start_us, "end_us": cut}
            break
    base = floor_to(start_us, step_us)
    for step in range(1, reach_us // step_us + 1):
        for low in (base - step * step_us, base + step * step_us):
            high = low + length
            if low >= floor_us and high <= now_us and _inside(covered, low, high):
                return {"kind": "shifted", "start_us": low, "end_us": high}
    return {"kind": "none"}


def align_binlog_query(binlog_rows: Any, query: dict[str, Any], retention_days: int,
                       now_us: int | None = None) -> tuple[dict[str, Any] | None, int | None]:
    """Repair an analytics window the Binlog row store does not fully cover; returns (adjusted, ready_through_us).

    Same two repairs as the slow-log rise investigation. ``query`` is updated in place. Fail-open: when coverage cannot be
    read, or no covered window of that length exists within reach, the requested window runs unchanged and the reason is
    logged (the result then carries its own coverage report). Needs a single instance: coverage is per instance.
    """
    instance = str(query.get("instance") or "")
    if binlog_rows is None or not instance:
        return None, None
    now_us = now_us if now_us is not None else time.time_ns() // 1000
    floor_us = now_us - max(int(retention_days), 1) * DAY_US
    start_us = max(int(query.get("start_epoch_us") or floor_us), floor_us)
    end_us = min(int(query.get("end_epoch_us") or now_us), now_us)
    if end_us <= start_us:
        return None, None
    try:
        coverage = binlog_rows.coverage(instance, max(start_us - SEARCH_REACH_US, floor_us),
                                        min(end_us + SEARCH_REACH_US, now_us))
    except Exception:  # noqa: BLE001 - a coverage hiccup must not take the analysis down
        LOGGER.warning("BINLOG_WINDOW_ALIGN_SKIPPED instance=%s: coverage unavailable; running the requested window",
                       instance, exc_info=True)
        return None, None
    intervals = coverage.get("intervals") or []
    edge = ready_edge(intervals, now_us)
    fixed = align_to_intervals(start_us, end_us, intervals, now_us=now_us, floor_us=floor_us)
    if fixed is None:
        return None, edge
    if fixed["kind"] == "none":
        LOGGER.warning("BINLOG_WINDOW_ALIGN_NONE instance=%s: no covered window of this length within reach; "
                       "running the requested window", instance)
        return None, edge
    adjusted = adjusted_payload(fixed["kind"], "incomplete_binlog_coverage", start_us, end_us,
                                fixed["start_us"], fixed["end_us"])
    LOGGER.warning("BINLOG_WINDOW_ALIGNED instance=%s kind=%s start=%s end=%s -> start=%s end=%s",
                   instance, fixed["kind"], start_us, end_us, fixed["start_us"], fixed["end_us"])
    query["start_epoch_us"], query["end_epoch_us"] = fixed["start_us"], fixed["end_us"]
    return adjusted, edge
