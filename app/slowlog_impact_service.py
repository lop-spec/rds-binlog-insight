"""Opt-in, bounded, read-only resource analysis; normal analytics stays unchanged."""
from __future__ import annotations

import json
import logging
import time
from collections import Counter
from dataclasses import replace
from typing import Any

from .credentials import load_credential
from .slow_log_collector import DasRpcClient
from .slowlog_impact import DAY_US, MAX_EVENTS, family_key, order_by_correlation, rank_performance_growth

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


def query_resource_overlap(metadata: Any, backend: Any, query: dict[str, Any],
                           settings: Any, metrics_loader: Any = load_metric_points) -> dict[str, Any]:
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
    parts = metadata.parts_in_range(start_epoch_us=start_us, end_epoch_us=end_us,
                                   source="slowlog", instance=instance)
    coverage = backend._manifest_coverage(parts)
    if not coverage.get("complete") or not coverage.get("total_parts"):
        return unavailable("incomplete_index")
    def read_events(start: int, end: int) -> list[dict[str, Any]]:
        scope, parameters = backend._scope_sql(query, start, end)
        return backend._rows("SELECT scope_event_id AS event_id, "
                             "metric_event_epoch_us AS start_us, metric_node_id AS node_id, "
                             "metric_database_name AS database_name, "
                             "metric_fingerprint AS fingerprint, metric_sql_id AS sql_id, "
                             # Legacy index maps unreported costs to 0: zero lacks presence proof.
                             "metric_query_time_ms AS duration_ms, nullIf(metric_rows_examined,0) AS rows_examined, "
                             "nullIf(metric_lock_time_ms,0) AS lock_time_ms FROM (" + scope + ") "
                             f"LIMIT {MAX_EVENTS + 1}", parameters, None)
    rows = read_events(start_us, end_us)
    if len(rows) > MAX_EVENTS:
        return unavailable("event_limit_exceeded")
    if not rows:
        return unavailable("no_collected_executions")
    baseline_start, baseline_end = start_us - DAY_US, end_us - DAY_US
    if backend._window({**query, "start_epoch_us": baseline_start, "end_epoch_us": baseline_end}, settings.retention_days) != (baseline_start, baseline_end):
        return unavailable("baseline_outside_retention")
    baseline_parts = metadata.parts_in_range(start_epoch_us=baseline_start, end_epoch_us=baseline_end,
                                            source="slowlog", instance=instance)
    baseline_coverage = backend._manifest_coverage(baseline_parts)
    if not baseline_coverage.get("complete") or not baseline_coverage.get("total_parts"):
        return unavailable("incomplete_baseline_index")
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
    samples = {}
    for event in rows:
        key = event["node_id"], family_key(event)
        previous = samples.get(key)
        if previous is None or (int(event["duration_ms"]), event["event_id"]) > (int(previous["duration_ms"]), previous["event_id"]):
            samples[key] = event
    selected = {(instance, samples[node["node_id"], row["fingerprint"]]["fingerprint"])
                for node in result.get("nodes", []) for row in node.get("statements", [])[:top]}
    profiles = backend.statement_index.statement_profiles(selected)
    for node in result.get("nodes", []):
        node["total_fingerprints"] = len(node.get("statements", []))
        node["statements"] = node.get("statements", [])[:top]
        for row in node["statements"]:
            key = node["node_id"], row["fingerprint"]
            row.update(profiles.get((instance, samples[key]["fingerprint"]), {}))
            row["executions"] = counts[key]
            row["sql_id"] = samples[key]["sql_id"]
            row["sample_event_id"] = samples[key]["event_id"]
            # Preserve exact sufficient statistics across JavaScript's 53-bit boundary.
            row["score_numerator"] = str(row["score_numerator"])
    result.update(instance_id=instance, start_us=start_us, end_us=end_us,
                  coverage=coverage, baseline_coverage=baseline_coverage,
                  executions=len(rows), baseline_executions=len(baseline), metric=metric_name,
                  metric_id=metric_id, metric_label=metric_label,
                  calculation_scope="Independent read-only snapshot; all fingerprints ranked before Top 10")
    return result
