"""Opt-in, bounded, read-only resource analysis; normal analytics stays unchanged."""
from __future__ import annotations

import json
import logging
import time
from bisect import bisect_right
from collections import Counter, defaultdict
from dataclasses import replace
from decimal import Decimal
from typing import Any, Callable

from .clickhouse_manifest import part_identity
from .credentials import load_credential
from .lag_origin import describe_lag
from .lag_peers import LOOKBACK_US, load_peer_series, peer_health
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
    # Replication lag of a replica node, seconds. A level that accumulates, so it is handled differently: see LAG_METRIC.
    "replication_lag": ("Cluster_DataDelay", "复制延迟 秒"),
}
DEFAULT_METRIC = "iops"
# The lag of a replica is produced on the primary (what it wrote) or on the replica's applier (what it is stuck on), never by
# the replica's own readers. So for this metric the candidates are the primary's slow statements, the score uses the minutes
# in which the lag grew (not the minutes in which it stayed high), and each node gets a `lag_origin` block.
LAG_METRIC = "replication_lag"
TOP_PER_NODE = 10
TOP_PER_NODE_CORRELATION = 20
# How far from the requested window to look for one whose index is complete.
SEARCH_REACH_US = 6 * 3600 * 1_000_000
# A window cut back to the indexed edge still needs the ranker's six complete minutes.
MIN_TRUNCATED_US = 6 * PERIOD_US
# Windows ending this close to now report how far behind the index is (older windows are long since indexed).
FRESHNESS_WINDOW_US = 30 * PERIOD_US
# What the primary wrote is looked up from this long before a lag episode's first sample (the writes come before their lag), on
# the binlog aggregates' five-minute buckets (so the episodes of different nodes, which differ by a few minutes, share one lookup),
# and for at most this many distinct windows per answer.
WRITES_LEAD_US = 5 * PERIOD_US
WRITES_BUCKET_US = 5 * PERIOD_US
MAX_WRITE_LOOKUPS = 12


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


def lag_increase_points(points: list[dict[str, Any]], period_us: int = PERIOD_US) -> list[dict[str, Any]]:
    """Per node, the seconds of lag each minute added: ``max(lag(t) - lag(t - 1 min), 0)``.

    A minute whose predecessor is missing has no value (a gap is never read as zero growth), which is why the loader
    asks for one sample before the window.
    """
    period_ms = period_us // 1000
    by_node: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
    for point in points:
        by_node[str(point.get("nodeId") or "")][int(point["timestamp"])] = point
    out = []
    for samples in by_node.values():
        for stamp, point in samples.items():
            before = samples.get(stamp - period_ms)
            if before is not None:
                out.append({**point, "Average": max(Decimal(str(point["Average"])) - Decimal(str(before["Average"])), Decimal(0))})
    return out


def _lag_series(points: list[dict[str, Any]], node: str) -> list[tuple[int, float]]:
    return sorted((int(point["timestamp"]) * 1000, float(point["Average"])) for point in points
                  if str(point.get("nodeId") or "") == node)


def lag_candidates(events: list[dict[str, Any]], primaries: set[str], replicas: list[str]) -> list[dict[str, Any]]:
    """The primary's statements, once per replica whose lag they are ranked against.

    Each copy keeps the real event id in ``source_event_id`` (the detail drawer needs it) and is marked ``origin`` so its
    family never merges with the same SQL on the replica itself.
    """
    out = []
    for event in events:
        if event.get("node_id") not in primaries:
            continue
        for node in replicas:
            out.append({**event, "node_id": node, "event_id": f"{event['event_id']}@{node}",
                        "source_event_id": event["event_id"], "source_node_id": event["node_id"], "origin": "primary"})
    return out


def _add_peer_health(nodes: list[dict[str, Any]], replicas: list[str], settings: Any, instance: str,
                     peer_loader: Callable[..., list[dict[str, Any]]] | None) -> None:
    """Compare every node that has lag episodes with its siblings (``lag_peers``).

    One set of per-node series is read for all of them. This is evidence next to the lag card, never a reason to fail the
    analysis: a failure is logged and the nodes simply carry no ``peer_health`` block (or an ``unavailable`` one).
    """
    lagging = [node for node in nodes if (node.get("lag_origin") or {}).get("episodes")]
    if peer_loader is None or not lagging:
        return
    episodes = [episode for node in lagging for episode in node["lag_origin"]["episodes"]]
    high = max(int(episode["last_us"]) for episode in episodes)
    low = min(int(episode["first_us"]) for episode in episodes) - LOOKBACK_US      # a window is under a day, so under 27 h
    try:
        series, failed = load_peer_series(peer_loader, settings, low, high, instance)
        for node in lagging:
            block = node["lag_origin"]
            if "slow" in failed or "reads" in failed:
                block["peer_health"] = {"status": "unavailable"}
                continue
            block["peer_health"] = peer_health(node["node_id"], block["episodes"], series,
                                               [other for other in replicas if other != node["node_id"]])
    except Exception:  # noqa: BLE001 - evidence next to the lag card, never a reason to fail the analysis
        LOGGER.warning("slowlog_resource_query peer health unavailable: instance=%s", instance, exc_info=True)


def _add_write_hotspots(nodes: list[dict[str, Any]], instance: str,
                        writes_lookup: Callable[[str, int, int], dict[str, Any] | None] | None) -> None:
    """Attach to every lag episode what the primary wrote while it lasted (the busiest tables of the binlog row store).

    A stall is left out: an applier stuck on one event says nothing about the volume written, and the page does not show it.
    """
    if writes_lookup is None:
        return
    found: dict[tuple[int, int], dict[str, Any] | None] = {}
    for node in nodes:
        for episode in (node.get("lag_origin") or {}).get("episodes", []):
            if episode.get("kind") == "stall":
                continue
            window = ((int(episode["first_us"]) - WRITES_LEAD_US) // WRITES_BUCKET_US * WRITES_BUCKET_US,
                      -(-int(episode["last_us"]) // WRITES_BUCKET_US) * WRITES_BUCKET_US)
            if window not in found:
                if len(found) >= MAX_WRITE_LOOKUPS:
                    LOGGER.warning("slowlog_resource_query write hotspots skipped for later episodes: instance=%s limit=%d", instance, MAX_WRITE_LOOKUPS)
                    found[window] = None
                else:
                    try:
                        found[window] = writes_lookup(instance, *window)
                    except Exception:  # noqa: BLE001 - same: evidence, not a reason to fail
                        LOGGER.warning("slowlog_resource_query write hotspots unavailable: instance=%s window=%s", instance, window, exc_info=True)
                        found[window] = {"status": "unavailable"}
            if found[window]:
                episode["writes"] = found[window]


def tail_plan(metadata: Any, backend: Any, instance: str, start_us: int, end_us: int, view: dict[str, Any] | None,
              index_state: Callable[[int, int], tuple[dict[str, Any], dict[str, Any] | None]],
              usable: Callable[[dict[str, Any] | None], bool]) -> dict[str, Any] | None:
    """The window the live tail (``slowlog_tail``) can complete, or None when it cannot help.

    ClickHouse must be complete wherever the tail does not reach: the parts still on their way in may only lie inside the
    stretch the tail covers, and the day-before baseline (read from ClickHouse as always) must be complete. The window then
    ends on the last whole minute the source has finished publishing, which is later than the indexed edge by the time the
    collector, the indexer and the copy into ClickHouse take.
    """
    if view is None or end_us < time.time_ns() // 1000 - FRESHNESS_WINDOW_US:
        return None
    cut = min(end_us, int(view["complete_through_us"])) // PERIOD_US * PERIOD_US
    if cut - start_us < MIN_TRUNCATED_US:
        return None
    index = _index_map(metadata, backend, instance, start_us, cut)
    if index is None:
        return None
    covered_from = int(view["covered_from_us"])
    # Before the tail begins ClickHouse must have everything, the start of the window included; from there on the tail has it all,
    # so a part that is still missing may only reach into the window from where the tail covers.
    if covered_from > start_us and (index["first"] is None or index["first"] > start_us):
        return None
    if any(high >= start_us and max(low, start_us) < covered_from for low, high in zip(*index["blocked"])):
        return None
    coverage, baseline = index_state(start_us, cut)
    if not coverage.get("total_parts") or (baseline is not None and not usable(baseline)):
        return None
    pending = len(coverage.get("missing_parts") or [])
    return {"end_us": cut, "covered_from_us": covered_from, "newest_us": int(view["newest_us"]), "pending_parts": pending,
            "coverage": {**coverage, "complete": True, "tail_parts": pending}, "baseline_coverage": baseline}


def query_resource_overlap(metadata: Any, backend: Any, query: dict[str, Any],
                           settings: Any, metrics_loader: Any = load_metric_points,
                           roles: Any = None,
                           ddl_lookup: Callable[[str, int, int], list[dict[str, Any]]] | None = None,
                           peer_loader: Callable[..., list[dict[str, Any]]] | None = None,
                           writes_lookup: Callable[[str, int, int], dict[str, Any] | None] | None = None,
                           tail: Any = None) -> dict[str, Any]:
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
    lag_mode = metric_id == LAG_METRIC
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
    tail_used: dict[str, Any] | None = None
    if tail is not None and end_us >= time.time_ns() // 1000 - FRESHNESS_WINDOW_US:
        # A window ending close to now: ClickHouse lacks the newest parts, whether they are still on their way in or the collector has
        # not produced them yet (a complete index says nothing about that); the live tail may hold exactly that stretch.
        try:
            tail_used = tail_plan(metadata, backend, instance, start_us, end_us, tail.view(instance), index_state, usable)
        except Exception:  # noqa: BLE001 - extra evidence, never a reason to fail: without it the window is cut at the indexed edge
            LOGGER.warning("slowlog_resource_query live tail unavailable: instance=%s", instance, exc_info=True)
        if tail_used is not None:
            requested_end = end_us
            coverage, baseline_coverage, end_us = tail_used["coverage"], tail_used["baseline_coverage"], tail_used["end_us"]
            if end_us < requested_end:
                adjusted = adjusted_payload("truncated", "source_delay", start_us, requested_end, start_us, end_us)
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
    if tail_used is not None:
        known = {row["event_id"] for row in rows}
        fresh = [row for row in tail.rows(instance, max(start_us, tail_used["covered_from_us"]), end_us, query)
                 if row["event_id"] not in known]
        rows.extend(fresh)
        tail_used["events"] = len(fresh)
        LOGGER.info("slowlog_resource_query window completed by the live tail: instance=%s events=%d pending_parts=%d end_behind_s=%d",
                    instance, len(fresh), tail_used["pending_parts"], max(time.time_ns() // 1000 - end_us, 0) // 1_000_000)
    if len(rows) > MAX_EVENTS:
        return unavailable("event_limit_exceeded")
    # A lag episode can be explained by the lag series and the binlog alone, so an empty slow log does not end that analysis.
    if not rows and not lag_mode:
        return unavailable("no_collected_executions")
    baseline_start, baseline_end = start_us - DAY_US, end_us - DAY_US
    if baseline_coverage is None:
        return unavailable("baseline_outside_retention")
    baseline = read_events(baseline_start, baseline_end)
    if len(baseline) > MAX_EVENTS:
        return unavailable("baseline_event_limit_exceeded")
    node_roles = _node_roles(roles, instance)
    try:
        # One sample before the window, so the first minute's growth can be told for the lag metric.
        lead = PERIOD_US if lag_mode else 0
        points = metrics_loader(settings, start_us - lead, end_us, instance,
                                str(query.get("node_id") or ""), **loader_options)
        try:
            baseline_points = metrics_loader(settings, baseline_start - lead, baseline_end, instance, str(query.get('node_id') or ''), **loader_options)
        except (RuntimeError, ValueError, KeyError, TypeError, OSError):
            LOGGER.exception('slowlog_resource_query baseline metrics unavailable; not inferring resource growth')
            baseline_points = []
        ranked_rows, ranked_baseline, score_points, replicas = rows, baseline, None, []
        if lag_mode:
            with_lag = sorted({str(point.get("nodeId") or "") for point in points} - {""})
            primaries = {node for node, info in node_roles.items() if info.get("role") == "primary"}
            if not primaries:
                LOGGER.warning("slowlog_resource_query lag candidates unavailable: primary node unknown for %s", instance)
            replicas = [node for node in with_lag if node not in primaries]
            ranked_rows = lag_candidates(rows, primaries, replicas)
            ranked_baseline = lag_candidates(baseline, primaries, replicas)
            if len(ranked_rows) > MAX_EVENTS or len(ranked_baseline) > MAX_EVENTS:
                return unavailable("event_limit_exceeded")
            score_points = lag_increase_points(points)
        unknown=sum(event.get('rows_examined') is None or event.get('lock_time_ms') is None for event in ranked_rows+ranked_baseline)
        if unknown:
            LOGGER.warning('slowlog_cost_provenance incomplete: %s records contain legacy zero or absent costs; not treating as measured zero',unknown)
        result = rank_performance_growth(ranked_rows, ranked_baseline, points, start_us=start_us, end_us=end_us,
                                         index_complete=True, baseline_complete=True, baseline_points=baseline_points,
                                         score_points=score_points)
    except (RuntimeError, ValueError, KeyError, TypeError, OSError):
        LOGGER.exception("slowlog_resource_query metric or input validation failed")
        return unavailable("metric_or_input_unavailable")
    if lag_mode:
        # The primary (and any node without lag samples) has no lag to explain: leave it out instead of listing it as a gap.
        kept = [node for node in result.get("nodes", []) if node["node_id"] in replicas]
        if kept:
            result["nodes"] = kept
            result["status"] = "ok" if all(node["status"] == "ok" for node in kept) else "partial"
        elif not replicas:
            LOGGER.warning("slowlog_resource_query no replica lag samples: instance=%s node=%s", instance, query.get("node_id") or "")
            result["nodes"], result["status"] = [], "no_replica_lag_samples"
    if by_correlation:
        order_by_correlation(result)
    top = TOP_PER_NODE_CORRELATION if by_correlation else TOP_PER_NODE
    counts = Counter((row["node_id"], family_key(row)) for row in ranked_rows)
    accounts: dict[tuple[str, str], Counter] = {}
    for event in ranked_rows:
        if event.get("account"):
            accounts.setdefault((event["node_id"], family_key(event)), Counter())[str(event["account"])] += 1
    samples = {}
    for event in ranked_rows:
        key = event["node_id"], family_key(event)
        previous = samples.get(key)
        if previous is None or (int(event["duration_ms"]), event["event_id"]) > (int(previous["duration_ms"]), previous["event_id"]):
            samples[key] = event
    selected = {(instance, samples[node["node_id"], row["fingerprint"]]["fingerprint"])
                for node in result.get("nodes", []) for row in node.get("statements", [])[:top]}
    profiles = backend.statement_index.statement_profiles(selected)
    if tail_used is not None:
        # A fingerprint first seen in the last minutes is not in the SQLite statement table yet; the tail kept its text.
        profiles.update(tail.profiles(selected - set(profiles)))
    for node in result.get("nodes", []):
        node["total_fingerprints"] = len(node.get("statements", []))
        node["statements"] = node.get("statements", [])[:top]
        known = node_roles.get(node["node_id"]) or {}
        node["role"], node["zone"] = known.get("role", ""), known.get("zone", "")
        for row in node["statements"]:
            key = node["node_id"], row["fingerprint"]
            row.update(profiles.get((instance, samples[key]["fingerprint"]), {}))
            row["executions"] = counts[key]
            # Minutes with any execution time: the page shows it next to every ranking, not only the correlation order's.
            row["active_minutes"] = sum(1 for value in row.get("runtime_us", []) if value)
            # Who ran it: the accounts with the most executions in the window (at most three) and how many there are.
            ranked = sorted(accounts.get(key, {}).items(), key=lambda item: (-item[1], item[0]))
            row["accounts"] = [name for name, _ in ranked[:3]]
            row["account_count"] = len(ranked)
            row["sql_id"] = samples[key]["sql_id"]
            row["sample_event_id"] = samples[key].get("source_event_id") or samples[key]["event_id"]
            if lag_mode:
                row["origin"], row["source_node_id"] = "primary", samples[key].get("source_node_id", "")
            # Preserve exact sufficient statistics across JavaScript's 53-bit boundary.
            row["score_numerator"] = str(row["score_numerator"])
    if lag_mode:
        peaks = {node: max((value for stamp, value in _lag_series(points, node) if start_us <= stamp <= end_us), default=0.0)
                 for node in replicas}
        own = defaultdict(list)
        for event in rows:
            own[event["node_id"]].append(event)
        for node in result.get("nodes", []):
            node["lag_origin"] = describe_lag(node["node_id"], _lag_series(points, node["node_id"]), events=own[node["node_id"]],
                                              other_peaks={other: peak for other, peak in peaks.items() if other != node["node_id"]},
                                              instance=instance, ddl_lookup=ddl_lookup)
        _add_peer_health(result.get("nodes", []), replicas, settings, instance, peer_loader)
        _add_write_hotspots(result.get("nodes", []), instance, writes_lookup)
        result.update(candidate_scope="primary_statements", primary_nodes=sorted(
            node for node, info in node_roles.items() if info.get("role") == "primary"))
    ready = ready_through(metadata, backend, instance, end_us, time.time_ns() // 1000)
    if tail_used is not None:
        ready = max(ready or 0, tail_used["newest_us"])
    result.update(instance_id=instance, start_us=start_us, end_us=end_us,
                  ready_through_us=ready,
                  tail=None if tail_used is None else {"from_us": max(start_us, tail_used["covered_from_us"]), "through_us": end_us,
                                                       "newest_us": tail_used["newest_us"], "events": tail_used.get("events", 0),
                                                       "pending_parts": tail_used["pending_parts"]},
                  coverage=coverage, baseline_coverage=baseline_coverage, adjusted=adjusted,
                  executions=len(rows), baseline_executions=len(baseline), metric=metric_name,
                  metric_id=metric_id, metric_label=metric_label,
                  calculation_scope="Independent read-only snapshot; all fingerprints ranked before Top 10")
    return result
