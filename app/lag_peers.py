"""How a lagging replica compares with the other replicas of its cluster.

A replica lags either because the primary wrote more than the appliers can absorb, or because this one node became slower
than its siblings. The siblings replay the same binlog, so the two are told apart by comparing the node with them, minute by
minute, on what Cloud Monitor already records per node: slow statements and SELECTs per second, running threads, CPU, IOPS.
There is no model and no threshold learned from data: a node is called slower when it logs several times the siblings'
slow statements per 1000 reads in the minutes where it was serving reads. The estimated time per read (running threads over
reads per second) is shown beside it but never decides: the running threads include the replication threads themselves, so
a node whose applier is merely busy would look slow by it.

Two things keep the comparison honest. The proxy takes a lagging node out of read routing (its SELECT rate collapses while
the siblings keep theirs), and the numbers of those minutes say nothing about the node's health: they are listed as their
own intervals and left out of every ratio. And a ratio of two tiny numbers is noise, so a node is only called slower when
it has logged enough slow statements to be more than a handful.

Everything is arithmetic on minute samples stamped with the END of the minute, like the lag series in ``lag_origin``.
"""
from __future__ import annotations

import logging
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from statistics import median
from typing import Any, Callable

LOGGER = logging.getLogger(__name__)

PERIOD_US = 60_000_000
LOOKBACK_US = 3 * 3600 * 1_000_000      # how far before the first episode the node is compared, to find when it diverged

# Cloud Monitor metric (namespace acs_rds_dashboard, per node) behind each comparison row.
METRIC_NAMES = {
    "slow": "Cluster_SlowQueries",        # statements over long_query_time, per second
    "reads": "Cluster_ComSelect",         # SELECT per second
    "threads": "Cluster_ThreadsRunning",  # statements executing now (replication threads included)
    "cpu": "Cluster_CpuUsage",            # %
    "iops": "Cluster_IOPSUsage",          # % of the instance's IOPS limit
}

MIN_PEER_READS = 50.0        # SELECT/s the siblings must serve for a minute to be comparable at all
REMOVED_SHARE = 0.05         # below this share of the siblings' read rate the proxy has taken the node out of routing
MIN_REMOVED_MINUTES = 3      # shorter dips are not reported as a removal
MAX_REMOVED_LISTED = 8       # the most recent read-less intervals that are listed
MIN_READ_MINUTES = 5         # usable minutes needed in the lagging period to judge the node
# Slow statements per 1000 reads, node / siblings. Over five usable minutes the ratio between two healthy replicas stayed
# within 0.85-1.8 on every node of a production cluster (a quiet morning, and an evening where all replicas lagged).
SLOW_RATIO = 3.0
MIN_SLOW_COUNT = 60          # ... and the node must have logged at least this many slow statements in the minutes compared
SLOW_PEER_FLOOR = 0.05       # siblings' rate below this is read as this, so a spotless sibling does not inflate every ratio
SMOOTH_MINUTES = 5           # the onset search looks at five usable minutes at a time
ONSET_GAP = 2                # good minutes tolerated inside a run of divergent ones
ONSET_REACH = 5              # the divergent run must reach within this many usable minutes of the end of the first episode

Series = dict[str, dict[str, dict[int, float]]]   # metric key -> node -> {minute end (us): value}


def series_from_points(points: list[dict[str, Any]]) -> dict[str, dict[int, float]]:
    """Cloud Monitor datapoints of one metric (every node of the instance) as ``node -> {minute end us: value}``."""
    out: dict[str, dict[int, float]] = {}
    for point in points:
        node = str(point.get("nodeId") or "")
        try:
            stamp, value = int(point["timestamp"]) * 1000, float(point["Average"])
        except (KeyError, TypeError, ValueError):
            continue
        if node:
            out.setdefault(node, {})[stamp] = value
    return out


def load_peer_series(loader: Callable[..., list[dict[str, Any]]], settings: Any, start_us: int, end_us: int,
                     instance: str) -> tuple[Series, dict[str, str]]:
    """The five health series of every node over [start, end]: one loader call per metric, in parallel.

    A metric that cannot be read is reported in the second result (and logged) instead of failing the others.
    """
    found: Series = {}
    failed: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=len(METRIC_NAMES), thread_name_prefix="lag-peers") as pool:
        futures = {key: pool.submit(loader, settings, start_us, end_us, instance, "", metric_name=name)
                   for key, name in METRIC_NAMES.items()}
        for key, future in futures.items():
            try:
                found[key] = series_from_points(future.result())
            except (RuntimeError, ValueError, KeyError, TypeError, OSError) as exc:
                failed[key] = str(exc)[:120] or type(exc).__name__
                LOGGER.warning("lag_peers metric unavailable: metric=%s instance=%s reason=%s", METRIC_NAMES[key], instance, failed[key])
    return found, failed


def _at(series: Series, key: str, node: str, stamp: int) -> float | None:
    return series.get(key, {}).get(node, {}).get(stamp)


def _peer_median(series: Series, key: str, peers: list[str], stamp: int) -> float | None:
    values = [value for peer in peers if (value := _at(series, key, peer, stamp)) is not None]
    return median(values) if values else None


def _classify(series: Series, node: str, peers: list[str], stamp: int) -> str:
    """``usable`` (both sides serve reads), ``removed`` (siblings do, the node does not) or ``unknown`` (no way to tell)."""
    peer_reads = _peer_median(series, "reads", peers, stamp)
    own = _at(series, "reads", node, stamp)
    if peer_reads is None or peer_reads < MIN_PEER_READS or own is None:
        return "unknown"       # a missing sample is not a removal
    return "removed" if own < REMOVED_SHARE * peer_reads else "usable"


def _ratio_of_sums(series: Series, top: str, bottom: str, node: str, stamps: list[int], scale: float) -> float | None:
    """sum(top) / sum(bottom) * scale over the minutes where the node has both samples."""
    upper = lower = 0.0
    for stamp in stamps:
        a, b = _at(series, top, node, stamp), _at(series, bottom, node, stamp)
        if a is not None and b is not None:
            upper, lower = upper + a, lower + b
    return upper / lower * scale if lower > 0 else None


def _peers_ratio(series: Series, top: str, bottom: str, peers: list[str], stamps: list[int], scale: float) -> float | None:
    values = [value for peer in peers if (value := _ratio_of_sums(series, top, bottom, peer, stamps, scale)) is not None]
    return median(values) if values else None


def _slow_count(series: Series, node: str, stamps: list[int]) -> float:
    """Slow statements the node logged in those minutes (the metric is a per-second average of each minute)."""
    return sum((_at(series, "slow", node, stamp) or 0.0) * 60 for stamp in stamps)


def _mean(series: Series, key: str, node: str, stamps: list[int]) -> float | None:
    values = [value for stamp in stamps if (value := _at(series, key, node, stamp)) is not None]
    return sum(values) / len(values) if values else None


def _peers_mean(series: Series, key: str, peers: list[str], stamps: list[int]) -> float | None:
    values = [value for peer in peers if (value := _mean(series, key, peer, stamps)) is not None]
    return median(values) if values else None


def _row(node_value: float | None, peer_value: float | None, digits: int = 2) -> dict[str, Any] | None:
    if node_value is None or peer_value is None:
        return None
    ratio = node_value / peer_value if peer_value > 0 else None
    return {"node": round(node_value, digits), "peers": round(peer_value, digits),
            "ratio": None if ratio is None else round(ratio, 2)}


def _removed_intervals(stamps: list[int], states: dict[int, str]) -> list[dict[str, int]]:
    """Runs of ``removed`` minutes (one missing or unknown minute inside a run does not split it), at least MIN_REMOVED_MINUTES long."""
    runs: list[list[int]] = []
    for stamp in stamps:
        if states[stamp] != "removed":
            continue
        if runs and stamp - runs[-1][-1] <= 2 * PERIOD_US:
            runs[-1].append(stamp)
        else:
            runs.append([stamp])
    listed = [{"start_us": run[0] - PERIOD_US, "end_us": run[-1], "minutes": len(run)} for run in runs if len(run) >= MIN_REMOVED_MINUTES]
    return listed[-MAX_REMOVED_LISTED:]


def _diverged_since(series: Series, node: str, peers: list[str], usable: list[int], end_us: int) -> int | None:
    """Start of the run of divergent minutes that is still going on at ``end_us``; None when there is none.

    A minute is divergent when, over the last five usable minutes, the node logged at least MIN_SLOW_COUNT slow statements
    and its rate per 1000 reads is SLOW_RATIO times the siblings'. A run tolerates ONSET_GAP good minutes inside it and must reach the end (the
    end of the last episode), so the answer is "since when has it been like this", which may be well before the first one.
    """
    window: deque[int] = deque(maxlen=SMOOTH_MINUTES)
    flags: list[tuple[int, bool]] = []
    for stamp in usable:
        if stamp > end_us:
            break
        window.append(stamp)
        recent = list(window)
        own = _ratio_of_sums(series, "slow", "reads", node, recent, 1000.0)
        reference = _peers_ratio(series, "slow", "reads", peers, recent, 1000.0)
        bad = (own is not None and reference is not None and _slow_count(series, node, recent) >= MIN_SLOW_COUNT
               and own >= SLOW_RATIO * max(reference, SLOW_PEER_FLOOR))
        flags.append((stamp, bad))
    start = last_bad = None
    good = 0
    runs: list[tuple[int, int]] = []
    for index, (_stamp, bad) in enumerate(flags):
        if bad:
            if start is None:
                start = index
            last_bad, good = index, 0
        elif start is not None:
            good += 1
            if good > ONSET_GAP:
                runs.append((start, last_bad))
                start = last_bad = None
    if start is not None:
        runs.append((start, last_bad))
    if not runs:
        return None
    first, last = runs[-1]
    if len(flags) - 1 - last > ONSET_REACH:
        return None
    return flags[first][0] - PERIOD_US


def peer_health(node: str, episodes: list[dict[str, Any]], series: Series, peers: list[str]) -> dict[str, Any]:
    """Compare ``node`` with ``peers`` over the minutes of its lag episodes.

    ``episodes`` are the node's ``lag_origin`` episodes (``first_us``/``last_us`` are minute ends); ``series`` holds the five
    health series of every node; ``peers`` are the other replicas. The block says whether the node was slower than its
    siblings while it lagged (``diverged``), since when (``diverged_since_us``) and over which read-less intervals the proxy
    had taken it out of routing (``reads_removed``, which may reach back into the lookback before the first episode).
    """
    spans = [(int(e["first_us"]), int(e["last_us"])) for e in episodes if "first_us" in e and "last_us" in e]
    if not spans:
        return {"status": "no_episodes"}
    own_reads = series.get("reads", {}).get(node)
    peers = [peer for peer in peers if peer != node and series.get("reads", {}).get(peer)]
    base: dict[str, Any] = {"peers": len(peers)}
    if not own_reads:
        return {**base, "status": "no_data"}
    if not peers:
        return {**base, "status": "no_peers"}
    low, high = min(first for first, _ in spans), max(last for _, last in spans)
    stamps = sorted(stamp for stamp in own_reads if stamp <= high)
    states = {stamp: _classify(series, node, peers, stamp) for stamp in stamps}
    removed = _removed_intervals(stamps, states)
    in_episode = [stamp for stamp in stamps if any(first <= stamp <= last for first, last in spans)]
    if not in_episode:
        return {**base, "status": "no_data"}       # the series do not reach the episodes at all
    period = [stamp for stamp in in_episode if states[stamp] == "usable"]
    result: dict[str, Any] = {**base, "period": {"start_us": low, "end_us": high, "minutes": len(in_episode), "read_minutes": len(period)},
                              "reads_removed": removed}
    if len(period) < MIN_READ_MINUTES:
        return {**result, "status": "no_reads"}
    own_slow = _ratio_of_sums(series, "slow", "reads", node, period, 1000.0)
    peer_slow = _peers_ratio(series, "slow", "reads", peers, period, 1000.0)
    own_latency = _ratio_of_sums(series, "threads", "reads", node, period, 1000.0)
    peer_latency = _peers_ratio(series, "threads", "reads", peers, period, 1000.0)
    slow_row = _row(own_slow, None if peer_slow is None else max(peer_slow, SLOW_PEER_FLOOR))
    latency_row = _row(own_latency, peer_latency)
    slower = bool(slow_row and _slow_count(series, node, period) >= MIN_SLOW_COUNT and slow_row["ratio"] is not None
                  and slow_row["ratio"] >= SLOW_RATIO)
    metrics = {"slow_per_1000_reads": slow_row, "ms_per_read": latency_row,
               "reads_per_s": _row(_mean(series, "reads", node, period), _peers_mean(series, "reads", peers, period), 0),
               "cpu_pct": _row(_mean(series, "cpu", node, period), _peers_mean(series, "cpu", peers, period), 1),
               "iops_pct": _row(_mean(series, "iops", node, period), _peers_mean(series, "iops", peers, period), 1)}
    if slow_row:
        slow_row["peers"] = round(peer_slow, 2)       # the real sibling rate, not the floored one used for the ratio
    usable = [stamp for stamp in stamps if states[stamp] == "usable"]
    since = _diverged_since(series, node, peers, usable, high) if slower else None
    return {**result, "status": "ok", "diverged": slower, "diverged_since_us": since,
            "metrics": {key: value for key, value in metrics.items() if value is not None}}
