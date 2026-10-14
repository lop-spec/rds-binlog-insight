"""Deterministic resource-overlap ranking, never a causal attribution model.

The ranking uses exact logged execution intervals, not an estimated per-second
scan rate. CMS sample t is paired with the preceding period (t-period, t].
Metric gaps are not zero-filled. No SQL text, incident label, LLM, lag search,
or fitted coefficient participates in the calculation.

Order within a node: growth score (overlap with the metric excess x growth of
non-waiting execution time over the same window yesterday), then measured scan
growth only to separate equal scores. Scan counts are evidence, not a gate: DAS
can record almost no rows for a statement that ran for minutes (materialized
subqueries), so "scanned more rows than yesterday" must not outrank a statement
that held the node for fourteen minutes.
"""
from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from decimal import Decimal
from fractions import Fraction
from typing import Any

from .slowlog_correlation import pearson
from .resource_evidence import metric_windows, metric_comparison

LOGGER = logging.getLogger(__name__)
PERIOD_US = 60_000_000
MAX_EVENTS = 250_000
DAY_US = 86400 * 1_000_000


def family_key(event: dict[str, Any]) -> str:
    """DAS SQL IDs can intentionally normalize numeric physical table suffixes.

    A statement ranked against another node's metric (replication lag: the primary's writes against a replica's lag)
    carries ``origin`` so it never merges with the same SQL running on the node itself; without it the key is unchanged.
    """
    sql_id = str(event.get("sql_id") or "")
    key = [str(event.get("database_name") or ""), "sql_id" if sql_id else "fingerprint", sql_id or str(event["fingerprint"])]
    if event.get("origin"):
        key.append(str(event["origin"]))
    return json.dumps(key, separators=(",", ":"))


def clipped_runtime_net_of_lock(events: list[dict[str, Any]], start_us: int, end_us: int,
                                period_us: int = PERIOD_US) -> dict[tuple[str, str], Fraction]:
    """Per (node, family): execution time inside the complete minutes of the window, minus the lock wait the slow log recorded.

    Same clipping as ``execution_series`` (executions starting in scope, whole minutes only). Each execution keeps the
    share of its time that was not lock wait, spread evenly over its run. A missing or zero lock time leaves the
    execution whole, so with no recorded waits this equals the plain runtime total.
    """
    first = ((start_us + period_us - 1) // period_us) * period_us
    stop = ((end_us + 1) // period_us) * period_us
    totals: dict[tuple[str, str], Fraction] = defaultdict(Fraction)
    for event in events:
        begin = int(event["start_us"])
        duration = int(event["duration_ms"]) * 1000
        clipped = max(0, min(begin + duration, stop) - max(begin, first))
        if not clipped or duration <= 0:
            continue
        lock = event.get("lock_time_ms")
        known = isinstance(lock, (int, float)) and not isinstance(lock, bool) and math.isfinite(lock) and lock > 0
        waited = min(int(lock) * 1000, duration) if known else 0
        totals[str(event.get("node_id") or ""), str(event["fingerprint"])] += Fraction(clipped * (duration - waited), duration)
    return dict(totals)


def rank_performance_growth(events: list[dict[str, Any]], baseline_events: list[dict[str, Any]],
                            points: list[dict[str, Any]], *, start_us: int, end_us: int,
                            index_complete: bool, baseline_complete: bool,
                            baseline_points: list[dict[str, Any]] | None = None,
                            score_points: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Resource overlap discounted by the non-growing fraction of execution time.

    Identical full-minute windows exactly 24 hours apart. All SQL families are
    treated identically. There is no incident-specific threshold or SQL allowlist.
    Growth is measured on execution time without the lock wait the slow log
    recorded, so a statement that only waits longer scores nothing. Fraction
    preserves the exact ordering before conversion to display shares.
    """
    if end_us - start_us >= DAY_US:
        LOGGER.warning('slowlog_performance_growth unavailable: overlapping_baseline')
        return {'status':'overlapping_baseline','nodes':[]}
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
                                   index_complete=index_complete, score_points=score_points)
    previous_series = execution_series(previous, start_us - DAY_US, end_us - DAY_US)
    net_now = clipped_runtime_net_of_lock(families, start_us, end_us)
    net_before = clipped_runtime_net_of_lock(previous, start_us - DAY_US, end_us - DAY_US)
    members: dict[tuple[str, str], set[str]] = defaultdict(set)
    ids = {}
    for event in events:
        key = str(event.get("node_id") or ""), family_key(event)
        members[key].add(str(event["fingerprint"]))
        ids[key] = str(event.get("sql_id") or "")
    def costs(source):
        result = {}
        for event in source:
            key = str(event.get('node_id') or ''), family_key(event)
            entry = result.setdefault(key, dict(count=0, rows_examined=0, lock_time_ms=0, rows_examined_known=0, lock_time_ms_known=0))
            entry['count'] += 1
            for field in ('rows_examined', 'lock_time_ms'):
                value = event.get(field)
                if value is not None and not isinstance(value, bool) and isinstance(value, (int,float)) and value >= 0 and math.isfinite(value):
                    entry[field] += value; entry[field+'_known'] += 1
        return result
    now_costs, before_costs = costs(events), costs(baseline_events)
    def metric_input(source):
        return [dict(p, role=p.get('nodeId',''), node=p.get('nodeId',''), metric='IOPS', value=p['Average']) for p in source]
    current_metrics, _ = metric_windows(metric_input(points),'IOPS',start_us,end_us+1)
    previous_metrics, _ = metric_windows(metric_input(baseline_points or []),'IOPS',start_us-DAY_US,end_us-DAY_US+1)
    for node in result.get("nodes", []):
        rows = node.get("statements", [])
        node['resource_comparison'] = metric_comparison(current_metrics.get(node['node_id']),previous_metrics.get(node['node_id']))
        scores = {}
        for row in rows:
            key = node["node_id"], row["fingerprint"]
            before = sum(previous_series.get(key, {}).values())
            current = row["runtime_us_total"]
            # Elapsed growth minus the lock wait inside it: time spent waiting is not work the statement added.
            grown = max(Fraction(0), net_now.get(key, Fraction(0)) - net_before.get(key, Fraction(0)))
            scores[row["fingerprint"]] = Fraction(row["score_numerator"]) * grown / (current or 1)
            row.update(raw_overlap_rank=row["rank"], baseline_runtime_us_total=before,
                       runtime_delta_us=current - before, sql_id=ids[key],
                       lock_wait_excluded_us=int(max(Fraction(0), max(0, current - before) - grown)),
                       member_fingerprints=sorted(members[key]))
            now = now_costs[key]; old = before_costs.get(key, dict(count=0,rows_examined=0,lock_time_ms=0,rows_examined_known=0,lock_time_ms_known=0))
            evidence = dict(status='elapsed_overlap_only', causal=False, scope='collected_slow_records',
                            current_count=now['count'],baseline_count=old['count'],
                            warnings=['runtime_includes_waits','scans_not_physical_iops','index_coverage_not_source_completeness'])
            for field in ('rows_examined','lock_time_ms'):
                complete = now[field+'_known']==now['count'] and old[field+'_known']==old['count']
                evidence[field] = dict(current=now[field] if now[field+'_known'] else None,
                                       baseline=old[field] if old[field+'_known'] or not old['count'] else None,
                                       delta=now[field]-old[field] if complete else None,
                                       known=now[field+'_known'],total=now['count'],
                                       baseline_known=old[field+'_known'],baseline_total=old['count'])
            scan_delta = evidence['rows_examined']['delta']
            if scan_delta is not None:
                evidence['status'] = 'scan_growth_related_evidence' if scan_delta>0 else 'no_scan_growth'
            if (evidence['lock_time_ms']['delta'] or 0)>0:
                evidence['warnings'].append('lock_wait_increased_possible_victim')
            if node['resource_comparison'].get('delta') is not None and node['resource_comparison']['delta']<=0:
                evidence['warnings'].append('resource_window_mean_not_increased')
            row['attribution'] = evidence
        # Growth score first; measured scan growth only separates equal scores (the zero-score tail). Neither is IOPS attribution.
        rows.sort(key=lambda row: (-scores[row["fingerprint"]],
                                   -max(0,row['attribution']['rows_examined']['delta'] or 0),
                                   row["fingerprint"]))
        total = sum(scores.values(), Fraction())
        for rank, row in enumerate(rows, 1):
            score = scores[row["fingerprint"]]
            row.update(rank=rank if score or (row['attribution']['rows_examined']['delta'] or 0)>0 else None,
                       growth_score_numerator=str(score.numerator),
                       growth_score_denominator=str(score.denominator),
                       growth_share=float(score / total) if total else None)
        if any((row['attribution']['rows_examined']['delta'] or 0)>0 for row in rows):
            node['status'] = 'ok'
        elif rows and not total:
            node["status"] = "no_growth_overlap"
            LOGGER.warning("slowlog_performance_growth unavailable: node=%s no growth overlap", node["node_id"])
    if result.get("nodes"):
        result["status"] = "ok" if all(n["status"] == "ok" for n in result["nodes"]) else "partial"
    result.update(method="growth_overlap_net_of_lock_v4", rank_unit="database_and_cloud_sql_id",
                  ranking_warning='Growth share is overlap with the metric excess, not physical IOPS contribution; '
                                  'recorded lock wait is excluded, other waits are not; DAS scan counts can be far below the real scan.',
                  baseline_start_us=start_us - DAY_US, baseline_end_us=end_us - DAY_US)
    return result


def order_by_correlation(result: dict[str, Any]) -> None:
    """Rank each node's SQL families by signed Pearson r (descending) between execution time and the metric.

    The rule of the MongoDB workspace: uncomputable coefficients (constant series, too few minutes) follow the
    computable ones in stable identity order; there is no hidden cost tie-break. Cost increments stay visible as
    columns. Correlation is a timing association, not a measurement of per-SQL resource consumption.
    """
    for node in result.get("nodes", []):
        rows = node.get("statements", [])
        rows.sort(key=lambda row: (row.get("resource_r") is None, -(row.get("resource_r") or 0.0), row["fingerprint"]))
        for position, row in enumerate(rows, 1):
            row["correlation_rank"] = position if row.get("resource_r") is not None else None
            row["active_minutes"] = sum(1 for value in row.get("runtime_us", []) if value)
    result["order"] = "correlation"


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


def _node_metrics(points: list[dict[str, Any]], time_set: set[int]) -> tuple[dict[str, dict[int, Decimal]] | None, str]:
    """Per node, the samples at the complete period ends; ``(None, reason)`` for a value that cannot be used."""
    metrics: dict[str, dict[int, Decimal]] = defaultdict(dict)
    for point in points:
        node = str(point.get("nodeId") or "")
        t = int(point["timestamp"]) * 1000
        if t not in time_set:
            continue
        value = Decimal(str(point["Average"]))
        if not value.is_finite() or value < 0 or abs(value.as_tuple().exponent) > 18:
            return None, "invalid_metric_value"
        if t in metrics[node] and metrics[node][t] != value:
            return None, "conflicting_metric_samples"
        metrics[node][t] = value
    return metrics, ""


def _scaled(samples: dict[int, Decimal], times: list[int]) -> tuple[int, list[int]]:
    scale = 10 ** max(0, max(-v.as_tuple().exponent for v in samples.values()))
    return scale, [int(samples[t] * scale) for t in times]


def rank_resource_overlap(events: list[dict[str, Any]], points: list[dict[str, Any]], *,
                          start_us: int, end_us: int, index_complete: bool,
                          period_us: int = PERIOD_US,
                          score_points: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    """Rank each node independently by sum(runtime_us * positive metric excess).

    Baseline is the lower empirical 20th percentile of the SAME selected node
    window, not a fitted quiet-period detector. Shares sum to 1 within a node;
    they are overlap shares, NOT shares of IOPS caused by a SQL statement.

    ``score_points`` (replication lag) replaces the series the excess and the coefficient are taken from; the node
    still reports ``points`` as its metric. A lag level is an accumulated state: what a statement can be related to is
    the minutes in which the lag grew, not the minutes in which it merely stayed high.
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
    metrics, reason = _node_metrics(points, time_set)
    if metrics is None:
        return unavailable(reason)
    score_metrics = metrics
    if score_points is not None:
        score_metrics, reason = _node_metrics(score_points, time_set)
        if score_metrics is None:
            return unavailable(reason)
    nodes = []
    for node in sorted({n for n, _ in series} | set(metrics)):
        samples = metrics.get(node, {})
        scored = score_metrics.get(node, {})
        if not node or len(samples) != len(times) or len(scored) != len(times):
            LOGGER.warning("slowlog_resource_overlap metric gaps: node=%s actual=%s expected=%s",
                           node or "unknown", min(len(samples), len(scored)), len(times))
            nodes.append({"node_id": node, "status": "metric_gaps", "statements": [],
                          "expected_points": len(times), "actual_points": min(len(samples), len(scored))})
            continue
        scale, y = _scaled(samples, times)
        score_scale, ys = (scale, y) if scored is samples else _scaled(scored, times)
        baseline = sorted(ys)[(len(ys) - 1) // 5]
        excess = [max(v - baseline, 0) for v in ys]
        statements = []
        for (event_node, fingerprint), sparse in series.items():
            if event_node != node:
                continue
            runtime = [sparse.get(t, 0) for t in times]
            correlation, reason = pearson(runtime, ys)
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
        entry = {"node_id": node, "status": status, "baseline": sorted(y)[(len(y) - 1) // 5] / scale,
                 "peak": max(y) / scale, "metric_scale": scale,
                 "metric_values": y, "statements": statements}
        if scored is not samples:
            entry.update(score_values=ys, score_scale=score_scale, score_baseline=baseline / score_scale)
        nodes.append(entry)
    if not nodes:
        return unavailable("no_metric_or_execution_data")
    return {"status": "ok" if all(n["status"] == "ok" for n in nodes) else "partial",
            "method": "execution_resource_overlap_v1", "period_us": period_us,
            "sample_end_us": times, "nodes": nodes,
            "source_scope": "collected slow executions starting inside the selected window",
            "warning": "Index coverage is not collection completeness. Runtime includes waits; "
                       "resource overlap and Pearson r are neither causation nor IOPS contribution."}
