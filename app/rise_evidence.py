"""Evidence next to the rise ranking that the slow log alone cannot give.

1. Template families. DAS can give one statement several SQL IDs (the same upsert on numbered shard tables
   ``order_record_38`` / ``_563`` came out as two IDs, the same statement on its unsharded table as two more), which
   splits one cause over several rows and lets a steady statement outrank it. Families whose normalized SQL is the same
   once the numeric shard suffixes are removed are merged inside a database.
2. Statement counters per node (Cloud Monitor ``Cluster_Com*`` / ``Cluster_InnoDBRow*``), window mean against the same
   window yesterday. They count every statement, fast ones included.
3. How much of the rise the slow log can explain: the execution time the slow log added over yesterday, as an average
   concurrency, against the growth of ``Cluster_ThreadsRunning``. A load made of short statements below the slow-log
   threshold (10-21: order upserts tripled after a four-minute stall) leaves the ranking with little to rank; this says so
   instead of letting a steady statement look like the cause.

Everything here is evidence: a failure is logged and the block says ``unavailable``; the ranking never depends on it.
"""
from __future__ import annotations

import hashlib
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

LOGGER = logging.getLogger(__name__)
DAY_US = 86400 * 1_000_000

# Identifier ending in a shard number: order_record_563, task_queue_9. Numeric literals are already ``?`` in the
# normalized SQL, so only names are touched.
_SHARD = re.compile(r"(?<![0-9A-Za-z_])([A-Za-z][0-9A-Za-z_]*?)_\d+(?![0-9A-Za-z_])")
_SPACE = re.compile(r"\s+")
# INSERT / REPLACE up to and including VALUES: table and column list.
_INSERT_HEAD = re.compile(r"^((?:INSERT|REPLACE)\s+(?:IGNORE\s+)?INTO\s+\S+\s*\([^)]*\)\s*VALUES?)\b", re.IGNORECASE)


def sql_template(normalized_sql: str) -> str:
    """The normalized SQL with numeric shard suffixes replaced and whitespace collapsed; '' when there is no text.

    An INSERT / REPLACE is cut after its column list and VALUES: a multi-row batch keeps literals in the slow log's text
    and is cut off by its length limit (10-21: one upsert came as a 1,243-character single row and as a 417-character
    truncated batch), so what follows VALUES is not a stable identity.
    """
    text = _SPACE.sub(" ", str(normalized_sql or "")).strip()
    if not text:
        return ""
    head = _INSERT_HEAD.match(text)
    if head:
        text = head.group(1)
    return _SHARD.sub(r"\1_#", text)


def assign_template_families(events: list[dict[str, Any]], profiles: Callable[[set[str]], dict[str, dict[str, Any]]],
                             family_key: Callable[[dict[str, Any]], str]) -> dict[str, int]:
    """Give events of families that share a template a common ``family``; returns {merged family: SQL-ID families merged}.

    ``profiles`` maps fingerprints to their statement profile (``normalized_sql``); one representative fingerprint per
    family is looked up. Families without text, or alone in their template, keep their own key.
    """
    keys = [family_key(event) for event in events]
    representative: dict[str, tuple[str, str]] = {}
    for key, event in zip(keys, events):
        representative.setdefault(key, (str(event.get("fingerprint") or ""), str(event.get("database_name") or "")))
    found = profiles({fp for fp, _db in representative.values() if fp})
    by_template: dict[tuple[str, str], list[str]] = {}
    for key, (fingerprint, database) in representative.items():
        text = sql_template((found.get(fingerprint) or {}).get("normalized_sql") or "")
        if text:
            by_template.setdefault((database, text), []).append(key)
    target: dict[str, str] = {}
    merged: dict[str, int] = {}
    for (database, text), members in by_template.items():
        if len(members) < 2:
            continue
        name = hashlib.sha1(f"{database}\n{text}".encode("utf-8")).hexdigest()[:16]
        for key in members:
            target[key] = name
        merged[name] = len(members)
    for key, event in zip(keys, events):
        name = target.get(key)
        if name:
            event["family"] = name
    return merged


# Statement counters: every statement counts, not only slow ones. Order is the order on the page.
COUNTER_METRICS = [
    ("com_select", "Cluster_ComSelect", "SELECT 次数 / 秒", "SELECT"),
    ("com_insert", "Cluster_ComInsert", "INSERT 次数 / 秒", "INSERT"),
    ("com_update", "Cluster_ComUpdate", "UPDATE 次数 / 秒", "UPDATE"),
    ("com_delete", "Cluster_ComDelete", "DELETE 次数 / 秒", "DELETE"),
    ("com_replace", "Cluster_ComReplace", "REPLACE 次数 / 秒", "REPLACE"),
    ("row_read", "Cluster_InnoDBRowRead", "InnoDB 读取行 / 秒", ""),
    ("row_insert", "Cluster_InnoDBRowInsert", "InnoDB 插入行 / 秒", ""),
    ("row_update", "Cluster_InnoDBRowUpdate", "InnoDB 更新行 / 秒", ""),
    ("row_delete", "Cluster_InnoDBRowDelete", "InnoDB 删除行 / 秒", ""),
]
THREADS_METRIC = "Cluster_ThreadsRunning"
# Below this growth of running threads there is nothing to explain.
MIN_THREADS_DELTA = 1.0
# The slow log explains less than this share of the growth: the page says the load is mostly statements it cannot see.
LOW_EXPLAINED = 0.3


def _means(points: list[dict[str, Any]], start_us: int, end_us: int) -> dict[str, float]:
    """Per node, the mean of the samples whose minute ends inside (start, end]."""
    sums: dict[str, list[float]] = {}
    for point in points:
        stamp = int(point["timestamp"]) * 1000
        if not start_us < stamp <= end_us:
            continue
        try:
            value = float(point["Average"])
        except (KeyError, TypeError, ValueError):
            continue
        sums.setdefault(str(point.get("nodeId") or ""), []).append(value)
    return {node: sum(values) / len(values) for node, values in sums.items() if values}


def load_counters(loader: Callable[..., list[dict[str, Any]]], settings: Any, instance: str, node: str,
                  start_us: int, end_us: int, threads_points: tuple[list[dict[str, Any]], list[dict[str, Any]]] | None = None,
                  workers: int = 8) -> dict[str, Any]:
    """Window and day-before means of every counter, per node: ``{"status", "nodes": {node: {key: (now, before)}}}``.

    ``threads_points`` reuses the running-thread series the ranking already read (when the selected metric is threads).
    """
    wanted = [(key, name) for key, name, _label, _op in COUNTER_METRICS]
    if threads_points is None:
        wanted.append(("threads", THREADS_METRIC))
    jobs = [(key, name, offset) for key, name in wanted for offset in (0, DAY_US)]

    def fetch(job: tuple[str, str, int]) -> tuple[str, int, list[dict[str, Any]] | Exception]:
        key, name, offset = job
        try:
            return key, offset, loader(settings, start_us - offset, end_us - offset, instance, node, metric_name=name)
        except Exception as exc:  # noqa: BLE001 - evidence: a counter that cannot be read is left out
            return key, offset, exc

    with ThreadPoolExecutor(max_workers=max(1, min(workers, len(jobs)))) as pool:
        fetched = list(pool.map(fetch, jobs))
    series: dict[str, dict[int, list[dict[str, Any]]]] = {}
    failed = []
    for key, offset, points in fetched:
        if isinstance(points, Exception):
            failed.append(f"{key}{'@-1d' if offset else ''}: {str(points)[:120]}")
        else:
            series.setdefault(key, {})[offset] = points
    if failed:
        LOGGER.warning("rise_counters unavailable for instance=%s: %s", instance, "; ".join(failed))
    if threads_points is not None:
        series["threads"] = {0: threads_points[0], DAY_US: threads_points[1]}
    nodes: dict[str, dict[str, tuple[float | None, float | None]]] = {}
    for key, by_offset in series.items():
        now = _means(by_offset.get(0, []), start_us, end_us)
        before = _means(by_offset.get(DAY_US, []), start_us - DAY_US, end_us - DAY_US)
        for node_id in set(now) | set(before):
            nodes.setdefault(node_id, {})[key] = (now.get(node_id), before.get(node_id))
    if not nodes:
        return {"status": "unavailable", "nodes": {}}
    return {"status": "ok", "nodes": nodes}


def _change(now: float | None, before: float | None) -> dict[str, Any]:
    delta = None if now is None or before is None else now - before
    ratio = None if delta is None or not before else delta / before
    return {"current": now, "baseline": before, "delta": delta, "ratio": ratio}


def node_evidence(counters: dict[str, tuple[float | None, float | None]] | None, slowlog_added_us: int,
                  window_us: int, families: list[dict[str, Any]]) -> dict[str, Any]:
    """The block one node shows above its table.

    ``families`` are all the node's ranked rows (before the top-N cut) with ``action``, ``executions``,
    ``baseline_executions``. The explained share is the slow log's added execution time as an average concurrency
    over the growth of running threads; both include waiting, so it is a rough comparison, never an attribution.
    """
    if not counters:
        return {"status": "unavailable"}
    items = []
    for key, _name, label, operation in COUNTER_METRICS:
        if key in counters:
            items.append({"key": key, "label": label, "operation": operation, **_change(*counters[key])})
    threads = _change(*counters["threads"]) if "threads" in counters else None
    added = max(int(slowlog_added_us), 0) / window_us if window_us > 0 else 0.0
    explained = None
    if threads and threads["delta"] is not None and threads["delta"] >= MIN_THREADS_DELTA:
        explained = min(added / threads["delta"], 1.0)
    # The statement type that grew most (by its own share of growth, at least 20% and +50 per second).
    grown = [item for item in items if item["operation"] and item["delta"] is not None and item["ratio"] is not None
             and item["ratio"] >= 0.2 and item["delta"] >= 50]
    grown.sort(key=lambda item: -item["delta"])
    lead = grown[0] if grown else None
    related = []
    if lead:
        same = [row for row in families if str(row.get("action") or "").upper() == lead["operation"]]
        same.sort(key=lambda row: -(int(row.get("executions") or 0) - int(row.get("baseline_executions") or 0)))
        related = [row["fingerprint"] for row in same[:3]
                   if int(row.get("executions") or 0) > int(row.get("baseline_executions") or 0)]
    return {"status": "ok", "items": items, "threads": threads, "slowlog_added_concurrency": added,
            "explained_ratio": explained, "low_explained": explained is not None and explained < LOW_EXPLAINED,
            "lead": lead, "related_families": related}
