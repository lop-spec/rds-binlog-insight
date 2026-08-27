from __future__ import annotations

import unittest

from tools.clickhouse_poc_benchmark import evaluate_benchmark


class ClickHousePocBenchmarkTests(unittest.TestCase):
    def test_gate_requires_exact_rows_hash_and_two_x_p50(self):
        current = {
            "rows": 100,
            "top100_hash": "same",
            "first_seconds": 6.0,
            "p50_seconds": 5.0,
        }
        target = {
            "rows": 100,
            "top100_hash": "same",
            "first_seconds": 2.0,
            "p50_seconds": 2.0,
        }

        result = evaluate_benchmark(
            current,
            target,
            minimum_speedup=2.0,
        )

        self.assertTrue(result["ok"])
        self.assertTrue(result["exact_match"])
        self.assertEqual(result["p50_speedup"], 2.5)

    def test_gate_rejects_fast_but_inexact_or_less_than_two_x(self):
        current = {
            "rows": 100,
            "top100_hash": "source",
            "first_seconds": 6.0,
            "p50_seconds": 3.0,
        }
        inexact = {
            "rows": 100,
            "top100_hash": "target",
            "first_seconds": 1.0,
            "p50_seconds": 1.0,
        }
        slow = {
            "rows": 100,
            "top100_hash": "source",
            "first_seconds": 2.0,
            "p50_seconds": 2.0,
        }

        self.assertFalse(
            evaluate_benchmark(current, inexact, minimum_speedup=2.0)["ok"]
        )
        self.assertFalse(
            evaluate_benchmark(current, slow, minimum_speedup=2.0)["ok"]
        )


if __name__ == "__main__":
    unittest.main()
