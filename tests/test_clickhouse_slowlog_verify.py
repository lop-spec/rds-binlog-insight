from __future__ import annotations

import unittest

from tools.clickhouse_slowlog_verify import (
    canonical_summary,
    compare_summaries,
    evaluate_gates,
    fixed_window_readiness,
    fixed_window_slices,
    order_violations,
)


def _statement(fingerprint: str, executions: int, scan_rows: int) -> dict[str, object]:
    return {
        "fingerprint": fingerprint,
        "executions": executions,
        "scan_rows": scan_rows,
        "rows_sent": executions,
        "sql_bytes": executions * 10,
        "query_time_ms_total": executions * 2,
        "last_epoch_us": executions,
    }


def _summary(rows: list[dict[str, object]]) -> dict[str, object]:
    ordered = sorted(
        rows,
        key=lambda row: (int(row["executions"]), int(row["scan_rows"])),
        reverse=True,
    )
    return {
        "window": {"start_epoch_us": 1, "end_epoch_us": 2},
        "sql": {
            "mode": "slowlog",
            "scan_source": "actual",
            "order": "executions",
            "totals": {"executions": sum(int(row["executions"]) for row in rows)},
            "statements": list(ordered),
            "orders": {
                "executions": list(ordered),
                "events": list(ordered),
            },
            "objects": [
                {"database_name": "biz", "table_name": "orders", "events": 3}
            ],
            "operations": [{"operation": "SELECT", "events": 3}],
            "trend": [{"ts": 1, "events": 3}],
        },
        "transactions": {},
        "locks": {},
    }


class ClickHouseSlowLogVerifyTest(unittest.TestCase):
    def test_canonical_summary_ignores_coverage_and_nondeterministic_ties(self):
        source = _summary([_statement("b", 2, 20), _statement("a", 2, 20)])
        target = _summary([_statement("a", 2, 20), _statement("b", 2, 20)])
        target["clickhouse_slowlog_coverage"] = {"complete": True}
        for row in source["sql"]["statements"]:
            row["sample_sql"] = "SELECT * FROM t WHERE id=1"
        for row in target["sql"]["statements"]:
            row["sample_sql"] = "SELECT * FROM t WHERE id=2"

        self.assertEqual(canonical_summary(source), canonical_summary(target))
        self.assertTrue(compare_summaries(source, target)["exact"])

    def test_compare_reports_first_semantic_difference(self):
        source = _summary([_statement("a", 3, 30)])
        target = _summary([_statement("a", 3, 31)])

        result = compare_summaries(source, target)

        self.assertFalse(result["exact"])
        self.assertIn("scan_rows", result["difference"]["path"])

    def test_order_validation_detects_descending_key_violation(self):
        result = _summary([_statement("a", 3, 30), _statement("b", 2, 20)])
        result["sql"]["orders"]["executions"] = list(
            reversed(result["sql"]["orders"]["executions"])
        )

        violations = order_violations(result)

        self.assertTrue(any("executions" in value for value in violations))

    def test_gate_requires_coverage_parity_order_and_two_x_speedup(self):
        passed = evaluate_gates(
            source_ready=True,
            target_ready=True,
            parity=True,
            ordered=True,
            source_seconds=[4.0, 5.0, 6.0],
            target_seconds=[1.0, 2.0, 3.0],
            minimum_speedup=2.0,
        )
        failed = evaluate_gates(
            source_ready=True,
            target_ready=True,
            parity=True,
            ordered=True,
            source_seconds=[3.0],
            target_seconds=[2.0],
            minimum_speedup=2.0,
        )

        self.assertTrue(passed["ok"])
        self.assertEqual(passed["speedup"], 2.5)
        self.assertFalse(failed["ok"])
        self.assertFalse(failed["speedup_passed"])

    def test_fixed_window_readiness_does_not_chase_newer_live_tail(self):
        source_ready, target_ready = fixed_window_readiness(
            {
                "reconcile_complete": True,
                "pending_parts": 7,
                "failed_parts": 0,
            },
            {
                "reconcile_completed_at_us": 1,
                "pending_parts": 5,
                "failed_parts": 0,
                "delete_parts": 0,
            },
        )
        self.assertTrue(source_ready)
        self.assertTrue(target_ready)

    def test_fixed_window_slices_are_inclusive_without_gaps_or_overlap(self):
        self.assertEqual(
            fixed_window_slices(10, 25, width_us=6),
            [(10, 15), (16, 21), (22, 25)],
        )


if __name__ == "__main__":
    unittest.main()
