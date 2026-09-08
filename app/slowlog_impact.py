"""Deterministic resource-overlap ranking, never a causal attribution model.

The ranking uses exact logged execution intervals, not an estimated per-second
scan rate. CMS sample t is paired with the preceding period (t-period, t].
Metric gaps are not zero-filled. No SQL text, incident label, LLM, lag search,
or fitted coefficient participates in the calculation.
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from decimal import Decimal
from fractions import Fraction
from typing import Any

from .slowlog_correlation import pearson

LOGGER = logging.getLogger(__name__)
PERIOD_US = 60_000_000
MAX_EVENTS = 250_000
DAY_US = 86400 * 1_000_000


def family_key(event: dict[str, Any]) -> str:
    """DAS SQL IDs can intentionally normalize numeric physical table suffixes."""
    sql_id = str(event.get("sql_id") or "")
    return json.dumps([str(event.get("database_name") or ""),
                       "sql_id" if sql_id else "fingerprint",
                       sql_id or str(event["fingerprint"])], separators=(",", ":"))


def rank_performance_growth(events: list[dict[str, Any]], baseline_events: list[dict[str, Any]],
                            points: list[dict[str, Any]], *, start_us: int, end_us: int,
                            index_complete: bool, baseline_complete: bool) -> dict[str, Any]:
    """Resource overlap discounted by the non-growing fraction of execution time.

    Identical full-minute windows exactly 24 hours apart. All SQL families are
    treated identically. There is no incident-specific threshold or SQL allowlist.
    Fraction preserves the exact ordering before conversion to display shares.
    """
    if not baseline_complete or len(baseline_events) > MAX_EVENTS:
        reason = "incomplete_baseline_index" if not baseline_complete else "baseline_event_limit_exceeded"
        LOGGER.warning("slowlog_performance_growth unavailable: %s", reason)
        return {"status": reason, "nodes": []}
    families = [{**e, "fingerprint": family_key(e)} for e in events]
    previous = [{**e, "fingerprint": family_key(e)} for e in baseline_events]
    missing_ids = sum(not e.get("sql_id") for e in events + baseline_events)
    if missing_ids:
        LOGGER.warning("slowlog_performance_growth missing SQL IDs: %s events use physical fingerprint", missing_ids)
    result = rank_resource_overlap(families, points, start_us=start_us, end_us=end_us,
                                   index_complete=index_complete)
    previous_series = execution_series(previous, start_us - DAY_US, end_us - DAY_US)
    members: dict[tuple[str, str], set[str]] = defaultdict(set)
    ids = {}
    for event in events:
        key = str(event.get("node_id") or ""), family_key(event)
        members[key].add(str(event["fingerprint"]))
        ids[key] = str(event.get("sql_id") or "")
    for node in result.get("nodes", []):
        rows = node.get("statements", [])
        scores = {}
        for row in rows:
            key = node["node_id"], row["fingerprint"]
            before = sum(previous_series.get(key, {}).values())
            current = row["runtime_us_total"]
            delta = max(0, current - before)
            scores[row["fingerprint"]] = Fraction(row["score_numerator"] * delta, current or 1)
            row.update(raw_overlap_rank=row["rank"], baseline_runtime_us_total=before,
                       runtime_delta_us=current - before, sql_id=ids[key],
                       member_fingerprints=sorted(members[key]))
        rows.sort(key=lambda row: (-scores[row["fingerprint"]], row["fingerprint"]))
        total = sum(scores.values(), Fraction())
        for rank, row in enumerate(rows, 1):
            score = scores[row["fingerprint"]]
            row.update(rank=rank if score else None,
                       growth_score_numerator=str(score.numerator),
                       growth_score_denominator=str(score.denominator),
                       growth_share=float(score / total) if total else None)
        if rows and not total:
            node["status"] = "no_growth_overlap"
            LOGGER.warning("slowlog_performance_growth unavailable: node=%s no growth overlap", node["node_id"])
    if result.get("nodes"):
        result["status"] = "ok" if all(n["status"] == "ok" for n in result["nodes"]) else "partial"
    result.update(method="baseline_adjusted_resource_overlap_v2", rank_unit="database_and_cloud_sql_id",
                  baseline_start_us=start_us - DAY_US, baseline_end_us=end_us - DAY_US)
    return result


def execution_series(events: list[dict[str, Any]], start_us: int, end_us: int,
                     period_us: int = PERIOD_US) -> dict[tuple[str, str], dict[int, int]]:
    """Sum exact microseconds intersecting each complete sampling interval.

    Only events starting in the supplied source scope are included. Executions
    beginning before that scope cannot be inferred from a missing record.
    Sparse keys are period END timestamps; simultaneous executions sum.
    """
    if period_us <= 0 or end_us < start_us:
        raise ValueError("invalid execution window")
    first = ((start_us + period_us - 1) // period_us) * period_us
    stop = ((end_us + 1) // period_us) * period_us
    result: dict[tuple[str, str], dict[int, int]] = defaultdict(lambda: defaultdict(int))
    seen: set[tuple[str, str]] = set()
    for event in events:
        node, fp = str(event.get("node_id") or ""), str(event["fingerprint"])
        identity = (str(event.get("instance_id") or ""), str(event.get("event_id") or ""))
        if identity[1]:
            if identity in seen:
                raise ValueError("duplicate event: canonicalize the input before ranking")
            seen.add(identity)
        begin = int(event["start_us"])
        duration = int(event["duration_ms"]) * 1000
        if duration < 0:
            raise ValueError("negative execution duration")
        if not start_us <= begin <= end_us:
            raise ValueError("event starts outside the declared source scope")
        finish = min(begin + duration, stop)
        values = result[node, fp]
        for bucket in range(max(first, begin // period_us * period_us), finish, period_us):
            overlap = max(0, min(finish, bucket + period_us) - max(begin, bucket))
            if overlap:
                values[bucket + period_us] += overlap
    return dict(result)


def rank_resource_overlap(events: list[dict[str, Any]], points: list[dict[str, Any]], *,
                          start_us: int, end_us: int, index_complete: bool,
                          period_us: int = PERIOD_US) -> dict[str, Any]:
    """Rank each node independently by sum(runtime_us * positive metric excess).

    Baseline is the lower empirical 20th percentile of the SAME selected node
    window, not a fitted quiet-period detector. Shares sum to 1 within a node;
    they are overlap shares, NOT shares of IOPS caused by a SQL statement.
    """
    def unavailable(reason: str) -> dict[str, Any]:
        LOGGER.warning("slowlog_resource_overlap unavailable: %s", reason)
        return {"status": reason, "nodes": []}

    if not index_complete:
        return unavailable("incomplete_index")
    if len(events) > MAX_EVENTS:
        return unavailable("event_limit_exceeded")
    series = execution_series(events, start_us, end_us, period_us)
    first = ((start_us + period_us - 1) // period_us) * period_us
    stop = ((end_us + 1) // period_us) * period_us
    times = list(range(first + period_us, stop + 1, period_us))
    if len(times) < 6:
        return unavailable("insufficient_complete_periods")
    time_set = set(times)
    metrics: dict[str, dict[int, Decimal]] = defaultdict(dict)
    for point in points:
        node = str(point.get("nodeId") or "")
        t = int(point["timestamp"]) * 1000
        if t not in time_set:
            continue
        value = Decimal(str(point["Average"]))
        if not value.is_finite() or value < 0 or abs(value.as_tuple().exponent) > 18:
            return unavailable("invalid_metric_value")
        if t in metrics[node] and metrics[node][t] != value:
            return unavailable("conflicting_metric_samples")
        metrics[node][t] = value
    nodes = []
    for node in sorted({n for n, _ in series} | set(metrics)):
        samples = metrics.get(node, {})
        if not node or len(samples) != len(times):
            LOGGER.warning("slowlog_resource_overlap metric gaps: node=%s actual=%s expected=%s",
                           node or "unknown", len(samples), len(times))
            nodes.append({"node_id": node, "status": "metric_gaps", "statements": [],
                          "expected_points": len(times), "actual_points": len(samples)})
            continue
        scale = 10 ** max(0, max(-v.as_tuple().exponent for v in samples.values()))
        y = [int(samples[t] * scale) for t in times]
        baseline = sorted(y)[(len(y) - 1) // 5]
        excess = [max(v - baseline, 0) for v in y]
        statements = []
        for (event_node, fingerprint), sparse in series.items():
            if event_node != node:
                continue
            runtime = [sparse.get(t, 0) for t in times]
            correlation, reason = pearson(runtime, y)
            score = sum(a * b for a, b in zip(runtime, excess))
            statements.append({"fingerprint": fingerprint, "score_numerator": score,
                               "resource_r": correlation, "correlation_status": reason,
                               "runtime_us": runtime, "runtime_us_total": sum(runtime)})
        total = sum(row["score_numerator"] for row in statements)
        statements.sort(key=lambda row: (-row["score_numerator"], row["fingerprint"]))
        for rank, row in enumerate(statements, 1):
            row["rank"] = rank if total else None
            row["overlap_share"] = row["score_numerator"] / total if total else None
        status = "ok" if total else "no_excess_overlap"
        if status != "ok":
            LOGGER.warning("slowlog_resource_overlap unavailable: node=%s reason=%s", node, status)
        nodes.append({"node_id": node, "status": status, "baseline": baseline / scale,
                      "peak": max(y) / scale, "metric_scale": scale,
                      "metric_values": y, "statements": statements})
    if not nodes:
        return unavailable("no_metric_or_execution_data")
    return {"status": "ok" if all(n["status"] == "ok" for n in nodes) else "partial",
            "method": "execution_resource_overlap_v1", "period_us": period_us,
            "sample_end_us": times, "nodes": nodes,
            "source_scope": "collected slow executions starting inside the selected window",
            "warning": "Index coverage is not collection completeness. Runtime includes waits; "
                       "resource overlap and Pearson r are neither causation nor IOPS contribution."}
