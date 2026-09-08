"""Read-only replay. Exit 1 for missing truth records or rank regressions.

Inputs stay private: numeric canonical execution snapshots, CMS samples and an
independent incident specification. Never add ground-truth rows to the source.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.slowlog_correlation import attach_correlations
from app.slowlog_impact import rank_resource_overlap, rank_performance_growth
from app.slowlog_index import slowlog_trend_width


def baseline_ranks(events, start, end):
    width = slowlog_trend_width(start, end)
    series = defaultdict(Counter)
    total = Counter()
    for event in events:
        bucket = int(event["start_us"]) // width * width
        series[event["fingerprint"]][bucket] += 1
        total[bucket] += 1
    groups = [{"fingerprint": fp} for fp in series]
    attach_correlations({"all": groups}, [{"ts": t, "events": n} for t, n in total.items()],
                        series, start_us=start, end_us=end, width_us=width)
    valid = sorted((g for g in groups if g["correlation"]["value"] is not None),
                   key=lambda g: (-g["correlation"]["value"], g["fingerprint"]))
    return {g["fingerprint"]: i for i, g in enumerate(valid, 1)}


def backtest(cases, metrics, specification, baselines=None):
    by_case = {case["name"]: case for case in cases}
    by_metric = {case["name"]: case for case in metrics}
    by_baseline = {case["name"]: case for case in (baselines or [])}
    output = []
    for spec in specification:
        case = by_case[spec["case"]]
        events = [e for e in case["events"] if e["node_id"] == spec["node"]]
        points = [p for p in by_metric[spec["case"]]["points"] if p["nodeId"] == spec["node"]]
        if baselines is not None:
            baseline = by_baseline[spec["case"]]
            result = rank_performance_growth(events, [e for e in baseline["events"] if e["node_id"] == spec["node"]],
                                             points, start_us=case["start_us"], end_us=case["end_us"],
                                             index_complete=bool(case["coverage"]["complete"]),
                                             baseline_complete=bool(baseline["coverage"]["complete"]))
        else:
            result = rank_resource_overlap(events, points, start_us=case["start_us"],
                                           end_us=case["end_us"], index_complete=bool(case["coverage"]["complete"]))
        node = next((n for n in result["nodes"] if n["node_id"] == spec["node"]), {})
        statements = node.get("statements", [])
        actual = {r["fingerprint"]: r for r in statements}
        old = baseline_ranks(events, case["start_us"], case["end_us"])
        checks = []
        if "ordered_sql_ids" in spec:
            expected_order = spec["ordered_sql_ids"]
            actual_order = [r.get("sql_id") for r in statements if r.get("rank") is not None][:len(expected_order)]
            checks.append({"expected_order": expected_order, "actual_order": actual_order,
                           "pass": bool(expected_order) and actual_order == expected_order})
        for expected in spec.get("expected", []):
            fingerprints = {e["fingerprint"] for e in events if e["sql_id"] == expected["sql_id"]}
            ranks = [actual[fp]["rank"] for fp in fingerprints if fp in actual and actual[fp]["rank"] is not None]
            ranks_old = [old[fp] for fp in fingerprints if fp in old]
            rank = min(ranks) if ranks else None
            checks.append({"sql_id": expected["sql_id"], "present": bool(fingerprints),
                           "old_count_r_rank": min(ranks_old) if ranks_old else None,
                           "resource_overlap_rank": rank, "target_max_rank": expected["max_rank"],
                           "pass": rank is not None and rank <= expected["max_rank"]})
        if "peak_below" in spec:
            checks.append({"normal_window_peak": node.get("peak"), "threshold": spec["peak_below"],
                           "pass": node.get("peak") is not None and node["peak"] < spec["peak_below"]})
        source_ids = {e["fingerprint"]: e["sql_id"] for e in events}
        output.append({"case": spec["case"], "node": spec["node"], "window": [case["start"], case["end"]],
                       "executions": len(events), "fingerprints": len(actual), "index_coverage": case["coverage"],
                       "metric_status": node.get("status"), "checks": checks,
                       "reference_confidence": spec.get("reference_confidence"),
                       "rank_scope": spec.get("rank_scope"), "point": spec.get("point"),
                       "pass": bool(checks) and node.get("status") == "ok" and all(c["pass"] for c in checks),
                       "top3": [{"rank": r["rank"], "fingerprint": r["fingerprint"],
                                 "sql_id": r.get("sql_id") or source_ids[r["fingerprint"]], "resource_r": r["resource_r"],
                                 "overlap_share": r["overlap_share"], "growth_share": r.get("growth_share")} for r in statements[:3]]})
    return {"passed": sum(c["pass"] for c in output), "total": len(output), "cases": output,
            "warning": "Retrospective known-case replay, not independent holdout or a general causal guarantee."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", required=True, type=Path)
    parser.add_argument("--metrics", required=True, type=Path)
    parser.add_argument("--truth", required=True, type=Path)
    parser.add_argument("--baseline", type=Path, help="Same-scope execution snapshots exactly 24h earlier")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    read = lambda p: json.loads(p.read_text(encoding="utf-8"))
    result = backtest(read(args.events), read(args.metrics), read(args.truth),
                      read(args.baseline) if args.baseline else None)
    # Refuse to overwrite an existing report: input snapshots and results are assets.
    with args.output.open("x", encoding="utf-8") as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    print(json.dumps({"passed": result["passed"], "total": result["total"],
                      "failures": [{"case": c["case"], "checks": c["checks"]}
                                   for c in result["cases"] if not c["pass"]]}, ensure_ascii=False))
    return 0 if result["passed"] == result["total"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
