"""Opt-in, bounded, read-only resource analysis; normal analytics stays unchanged."""
from __future__ import annotations

import json
import logging
import time
from bisect import bisect_right
from collections import Counter
from dataclasses import replace
from typing import Any

from .clickhouse_manifest import part_identity
from .credentials import load_credential
from .slow_log_collector import DasRpcClient
from .slowlog_impact import DAY_US, MAX_EVENTS, PERIOD_US, family_key, order_by_correlation, rank_performance_growth
from .window_align import adjusted_payload

LOGGER = logging.getLogger(__name__)


class CmsRpcClient(DasRpcClient):
    VERSION = "2019-01-01"


# The metrics to investigate on the cluster edition (CMS namespace acs_rds_dashboard, per node). CPU and IOPS are the
# usual complaints; rows read and row-lock time are the InnoDB signals that separate scan storms from lock pile-ups.
RDS_METRICS = {
    "cpu": ("Cluster_CpuUsage", "CPU 使用率 %"),
    "iops": ("Cluster_IOPSUsage", "IOPS 使用率 %"),
    "rows_read": ("Cluster_InnoDBRowRead", "InnoDB 读取行数 / 秒"),
    "row_lock": ("Cluster_InnoDBRowLockTimePs", "行锁等待 ms / 秒"),
    "threads": ("Cluster_ThreadsRunning", "活跃线程数"),
}
DEFAULT_METRIC = "iops"
TOP_PER_NODE = 10
TOP_PER_NODE_CORRELATION = 20
# How far from the requested window to look for one whose index is complete.
SEARCH_REACH_US = 6 * 3600 * 1_000_000
# A window cut back to the indexed edge still needs the ranker's six complete minutes.
MIN_TRUNCATED_US = 6 * PERIOD_US
# Windows ending this close to now report how far behind the index is (older windows are long since indexed).
FRESHNESS_WINDOW_US = 30 * PERIOD_US


def load_metric_points(settings: Any, start_us: int, end_us: int, instance: str,
                       node: str = "", credential_loader: Any = load_credential,
                       metric_name: str = RDS_METRICS[DEFAULT_METRIC][0]) -> list[dict[str, Any]]:
    credential = credential_loader(settings.credential_target)
    if credential is None:
        raise RuntimeError("cloud_credential_unavailable")
    client = CmsRpcClient(replace(settings, db_instance_id=instance,
                                 endpoint=f"https://metrics.{settings.region_id}.aliyuncs.com"),
                          credential, timeout=8)
    dimensions = {"instanceId": instance}
    if node:
        dimensions["nodeId"] = node
    params = {"Namespace": "acs_rds_dashboard", "MetricName": metric_name,
              "Dimensions": json.dumps([dimensions]), "StartTime": str(start_us // 1000),
              "EndTime": str((end_us + 1) // 1000), "Period": "60", "Length": "1440"}
    result = []
    tokens = set()
    deadline = time.monotonic() + 20
    for _ in range(48):
        if time.monotonic() > deadline:
            raise RuntimeError("metric_deadline_exceeded")
        response = client.call("DescribeMetricList", params)
        if str(response.get("Code")) != "200":
            raise RuntimeError(f"metric_api_error:{response.get('Code')}")
        points = json.loads(response.get("Datapoints") or "[]")
        if not isinstance(points, list):
            raise RuntimeError("invalid_metric_points")
        for point in points:
            if point.get("instanceId") != instance or (node and point.get("nodeId") != node):
                raise RuntimeError("metric_identity_mismatch")
        result.extend(points)
        token = response.get("NextToken")
        if not token:
            return result
        if token in tokens:
            raise RuntimeError("metric_pagination_cycle")
        tokens.add(token)
        params["NextToken"] = token
    raise RuntimeError("metric_page_limit_exceeded")


load_iops_points = load_metric_points  # the pre-1.29.21 name; same signature, IOPS is the default metric


def _merge(spans: list[tuple[int, int]]) -> tuple[list[int], list[int]]:
    merged: list[list[int]] = []
    for low, high in sorted(spans):
        if merged and low <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], high)
        else:
            merged.append([low, high])
    return [item[0] for item in merged], [item[1] for item in merged]


def _touches(merged: tuple[list[int], list[int]], low: int, high: int) -> bool:
    """Whether [low, high] overlaps a merged span; both ends inclusive, like parts_in_range."""
    lows, highs = merged
    index = bisect_right(lows, high) - 1
    return index >= 0 and highs[index] >= low


def _index_map(metadata: Any, backend: Any, instance: str, low: int, high: int) -> dict[str, Any] | None:
    """Ready and not-ready slow-log part spans over [low, high]; None while the index has never been reconciled."""
    parts = metadata.parts_in_range(start_epoch_us=low, end_epoch_us=high, source="slowlog", instance=instance)
    coverage = backend._manifest_coverage(parts)
    if not coverage.get("reconcile_completed_at_us"):
        return None
    missing = set(coverage.get("missing_parts") or [])
    ready: list[tuple[int, int]] = []
    blocked: list[tuple[int, int]] = []
    for part in parts:
        if not part_identity(part):
            continue  # coverage ignores parts it cannot identify, so they prove nothing either way
        span = (int(part["min_event_epoch_us"]), int(part["max_event_epoch_us"]))
        (blocked if str(part.get("path") or "") in missing else ready).append(span)
    return {"ready": _merge(ready), "blocked": _merge(blocked),
            "first": min((span[0] for span in ready), default=None),
            "last": max((span[1] for span in ready), default=None)}


def _indexed(index: dict[str, Any], low: int, high: int) -> bool:
    # The window must sit inside indexed time (not run past the newest ready part) and touch no unready part.
    return (index["first"] is not None and index["first"] <= low and high <= index["last"]
            and _touches(index["ready"], low, high) and not _touches(index["blocked"], low, high))


def nearest_indexed_window(metadata: Any, backend: Any, instance: str, start_us: int, end_us: int,
                           retention_days: int, reach_us: int = SEARCH_REACH_US) -> tuple[int, int] | None:
    """The same-length window closest to the requested one whose index and day-before baseline index are both complete.

    Steps a minute at a time, earlier before later at equal distance; never ends in the future and keeps the baseline
    day inside retention. Two part listings (current range, baseline range) answer every candidate in memory.
    """
    length = end_us - start_us
    now_us = time.time_ns() // 1000
    lowest = max(start_us - reach_us, now_us - (max(int(retention_days), 1) - 1) * DAY_US)
    highest = min(start_us + reach_us, now_us - length)
    if highest < lowest:
        return None
    current = _index_map(metadata, backend, instance, lowest, highest + length)
    baseline = _index_map(metadata, backend, instance, lowest - DAY_US, highest + length - DAY_US)
    if current is None or baseline is None:
        return None
    for step in range(1, reach_us // PERIOD_US + 1):
        for low in (start_us - step * PERIOD_US, start_us + step * PERIOD_US):
            if (lowest <= low <= highest and _indexed(current, low, low + length)
                    and _indexed(baseline, low - DAY_US, low + length - DAY_US)):
                return low, low + length
    return None


def _node_roles(roles: Any, instance: str) -> dict[str, dict[str, str]]:
    """Roles are a label on the answer, never a reason to fail it: a broken lookup is logged and the nodes stay unlabeled."""
    if roles is None:
        return {}
    try:
        return roles(instance) or {}
    except Exception:  # noqa: BLE001
        LOGGER.warning("slowlog_resource_query node roles unavailable for %s; nodes are shown without a role", instance, exc_info=True)
        return {}


def truncate_to_ready(metadata: Any, backend: Any, instance: str, start_us: int, end_us: int,
                      min_span_us: int = MIN_TRUNCATED_US) -> int | None:
    """The end of the longest prefix of [start, end] the index fully covers, on a whole minute.

    Only a missing tail can be cut: the newest slow-log parts are still being indexed, so the requested start stays and the
    analysis simply stops at the last clean minute. None when the start itself is not indexed, or the prefix left is
    shorter than ``min_span_us`` (the caller then looks for another window).
    """
    index = _index_map(metadata, backend, instance, start_us, end_us)
    if index is None or index["first"] is None or index["first"] > start_us:
        return None
    limit = min(end_us, index["last"])
    for low, high in zip(*index["blocked"]):
        if high < start_us:
            continue
        if low > limit:
            break
        if low <= start_us:
            return None
        limit = low - 1  # the first part still waiting for the index starts a gap
        break
    cut = limit // PERIOD_US * PERIOD_US
    return cut if cut - start_us >= min_span_us else None


def ready_through(metadata: Any, backend: Any, instance: str, end_us: int, now_us: int) -> int | None:
    """Newest event the index serves, for a window that ends close to now (None for older windows)."""
    if end_us < now_us - FRESHNESS_WINDOW_US:
        return None
    index = _index_map(metadata, backend, instance, end_us - FRESHNESS_WINDOW_US, now_us)
    return None if index is None else index["last"]


def query_resource_overlap(metadata: Any, backend: Any, query: dict[str, Any],
                           settings: Any, metrics_loader: Any = load_metric_points,
                           roles: Any = None) -> dict[str, Any]:
    def unavailable(reason: str) -> dict[str, Any]:
        LOGGER.warning("slowlog_resource_query unavailable: %s", reason)
        return {"status": reason, "nodes": []}

    if backend is None or not backend.serving_enabled:
        return unavailable("clickhouse_backend_required")
    if query.get("source") != "slowlog":
        return unavailable("slowlog_source_required")
    instance = str(query.get("instance") or "")
    if not instance:
        return unavailable("single_instance_required")
    metric_id = str(query.get("metric") or DEFAULT_METRIC)
    if metric_id not in RDS_METRICS:
        return unavailable("unsupported_metric")
    metric_name, metric_label = RDS_METRICS[metric_id]
    # Only a non-default metric passes the name, so a caller-supplied loader for IOPS keeps its old signature.
    loader_options = {} if metric_id == DEFAULT_METRIC else {"metric_name": metric_name}
    by_correlation = str(query.get("impact_order") or "") == "correlation"
    window = backend._window(query, settings.retention_days)
    if window is None:
        return unavailable("invalid_window")
    start_us, end_us = window
    if end_us - start_us >= DAY_US:
        return unavailable("overlapping_baseline")
    def index_state(low: int, high: int) -> tuple[dict[str, Any], dict[str, Any] | None]:
        """Coverage of the window and of its day-before baseline (None when the baseline is outside retention)."""
        found = backend._manifest_coverage(metadata.parts_in_range(start_epoch_us=low, end_epoch_us=high,
                                                                   source="slowlog", instance=instance))
        before = (low - DAY_US, high - DAY_US)
        if backend._window({**query, "start_epoch_us": before[0], "end_epoch_us": before[1]}, settings.retention_days) != before:
            return found, None
        return found, backend._manifest_coverage(metadata.parts_in_range(start_epoch_us=before[0], end_epoch_us=before[1],
                                                                         source="slowlog", instance=instance))

    def usable(found: dict[str, Any] | None) -> bool:
        return bool(found and found.get("complete") and found.get("total_parts"))

    coverage, baseline_coverage = index_state(start_us, end_us)
    adjusted = None
    if not usable(coverage) or (baseline_coverage is not None and not usable(baseline_coverage)):
        reason = "incomplete_index" if not usable(coverage) else "incomplete_baseline_index"
        requested = (start_us, end_us)
        # A window that ends close to now usually misses only its tail (the newest parts are still being indexed): keep the
        # requested start and stop at the last indexed minute rather than moving the whole window off the time point asked for.
        cut = None if usable(coverage) else truncate_to_ready(metadata, backend, instance, start_us, end_us)
        if cut is not None:
            cut_coverage, cut_baseline = index_state(start_us, cut)
            if usable(cut_coverage) and (cut_baseline is None or usable(cut_baseline)):
                coverage, baseline_coverage = cut_coverage, cut_baseline
                adjusted = adjusted_payload("truncated", reason, *requested, start_us, cut)
                LOGGER.warning("slowlog_resource_query window truncated to the indexed edge: reason=%s cut_min=%s instance=%s",
                               reason, adjusted["truncated_us"] // PERIOD_US, instance)
                end_us = cut
        if adjusted is None:
            shifted = nearest_indexed_window(metadata, backend, instance, start_us, end_us, settings.retention_days)
            if shifted is None:
                return unavailable(reason)
            coverage, baseline_coverage = index_state(*shifted)
            if not usable(coverage) or not usable(baseline_coverage):
                LOGGER.warning("slowlog_resource_query window shift not confirmed by the manifest: %s", reason)
                return unavailable(reason)
            adjusted = adjusted_payload("shifted", reason, *requested, *shifted)
            LOGGER.warning("slowlog_resource_query window shifted: reason=%s shift_min=%s instance=%s",
                           reason, adjusted["shift_us"] // PERIOD_US, instance)
            start_us, end_us = shifted
    def read_events(start: int, end: int) -> list[dict[str, Any]]:
        scope, parameters = backend._scope_sql(query, start, end)
        return backend._rows("SELECT scope_event_id AS event_id, "
                             "metric_event_epoch_us AS start_us, metric_node_id AS node_id, "
                             "metric_database_name AS database_name, "
                             "metric_fingerprint AS fingerprint, metric_sql_id AS sql_id, "
                             # Legacy index maps unreported costs to 0: zero lacks presence proof.
                             "metric_query_time_ms AS duration_ms, nullIf(metric_rows_examined,0) AS rows_examined, "
                             "nullIf(metric_lock_time_ms,0) AS lock_time_ms, "
                             "metric_database_account AS account FROM (" + scope + ") "
                             f"LIMIT {MAX_EVENTS + 1}", parameters, None)
    rows = read_events(start_us, end_us)
    if len(rows) > MAX_EVENTS:
        return unavailable("event_limit_exceeded")
    if not rows:
        return unavailable("no_collected_executions")
    baseline_start, baseline_end = start_us - DAY_US, end_us - DAY_US
    if baseline_coverage is None:
        return unavailable("baseline_outside_retention")
    baseline = read_events(baseline_start, baseline_end)
    if len(baseline) > MAX_EVENTS:
        return unavailable("baseline_event_limit_exceeded")
    try:
        points = metrics_loader(settings, start_us, end_us, instance,
                                str(query.get("node_id") or ""), **loader_options)
        try:
            baseline_points = metrics_loader(settings, baseline_start, baseline_end, instance, str(query.get('node_id') or ''), **loader_options)
        except (RuntimeError, ValueError, KeyError, TypeError, OSError):
            LOGGER.exception('slowlog_resource_query baseline metrics unavailable; not inferring resource growth')
            baseline_points = []
        unknown=sum(event.get('rows_examined') is None or event.get('lock_time_ms') is None for event in rows+baseline)
        if unknown:
            LOGGER.warning('slowlog_cost_provenance incomplete: %s records contain legacy zero or absent costs; not treating as measured zero',unknown)
        result = rank_performance_growth(rows, baseline, points, start_us=start_us, end_us=end_us,
                                         index_complete=True, baseline_complete=True, baseline_points=baseline_points)
    except (RuntimeError, ValueError, KeyError, TypeError, OSError):
        LOGGER.exception("slowlog_resource_query metric or input validation failed")
        return unavailable("metric_or_input_unavailable")
    if by_correlation:
        order_by_correlation(result)
    top = TOP_PER_NODE_CORRELATION if by_correlation else TOP_PER_NODE
    counts = Counter((row["node_id"], family_key(row)) for row in rows)
    accounts: dict[tuple[str, str], Counter] = {}
    for event in rows:
        if event.get("account"):
            accounts.setdefault((event["node_id"], family_key(event)), Counter())[str(event["account"])] += 1
    samples = {}
    for event in rows:
        key = event["node_id"], family_key(event)
        previous = samples.get(key)
        if previous is None or (int(event["duration_ms"]), event["event_id"]) > (int(previous["duration_ms"]), previous["event_id"]):
            samples[key] = event
    selected = {(instance, samples[node["node_id"], row["fingerprint"]]["fingerprint"])
                for node in result.get("nodes", []) for row in node.get("statements", [])[:top]}
    profiles = backend.statement_index.statement_profiles(selected)
    node_roles = _node_roles(roles, instance)
    for node in result.get("nodes", []):
        node["total_fingerprints"] = len(node.get("statements", []))
        node["statements"] = node.get("statements", [])[:top]
        known = node_roles.get(node["node_id"]) or {}
        node["role"], node["zone"] = known.get("role", ""), known.get("zone", "")
        for row in node["statements"]:
            key = node["node_id"], row["fingerprint"]
            row.update(profiles.get((instance, samples[key]["fingerprint"]), {}))
            row["executions"] = counts[key]
            # Who ran it: the accounts with the most executions in the window (at most three) and how many there are.
            ranked = sorted(accounts.get(key, {}).items(), key=lambda item: (-item[1], item[0]))
            row["accounts"] = [name for name, _ in ranked[:3]]
            row["account_count"] = len(ranked)
            row["sql_id"] = samples[key]["sql_id"]
            row["sample_event_id"] = samples[key]["event_id"]
            # Preserve exact sufficient statistics across JavaScript's 53-bit boundary.
            row["score_numerator"] = str(row["score_numerator"])
    result.update(instance_id=instance, start_us=start_us, end_us=end_us,
                  ready_through_us=ready_through(metadata, backend, instance, end_us, time.time_ns() // 1000),
                  coverage=coverage, baseline_coverage=baseline_coverage, adjusted=adjusted,
                  executions=len(rows), baseline_executions=len(baseline), metric=metric_name,
                  metric_id=metric_id, metric_label=metric_label,
                  calculation_scope="Independent read-only snapshot; all fingerprints ranked before Top 10")
    return result
