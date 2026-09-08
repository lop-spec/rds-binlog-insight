from __future__ import annotations

import math
import random
import statistics
import unittest

from app.slowlog_correlation import attach_correlations, differences, pearson


class CorrelationTests(unittest.TestCase):
    def summarize(self, x, y, *, start=0, end=None, sparse=False):
        rows = [{"fingerprint": "a", "executions": sum(x)}]
        orders = {"executions": rows, "scan_rows": [dict(rows[0])]}
        trend = [{"ts": i * 10, "events": v} for i, v in enumerate(y) if v or not sparse]
        dense, meta = attach_correlations(orders, trend, {"a": {i * 10: v for i, v in enumerate(x) if v}},
                                         start_us=start, end_us=len(y) * 10 - 1 if end is None else end, width_us=10)
        self.assertEqual(orders['executions'][0]['correlation'], orders['scan_rows'][0]['correlation'])
        return rows[0]['correlation'], dense, meta

    def test_exact_positive_negative_and_zero(self):
        self.assertEqual(pearson([0, 1, 2], [4, 6, 8]), (1.0, 'ok'))
        self.assertEqual(pearson([0, 1, 2], [8, 6, 4]), (-1.0, 'ok'))
        self.assertEqual(pearson([-1, 0, 1], [1, -2, 1]), (0.0, 'ok'))

    def test_large_baselines_do_not_cancel(self):
        x, y = [1, 7, 4, 0, 8, 3], [9, 5, 6, 2, 9, 4]
        self.assertEqual(pearson(x, y), pearson([v + 10**18 for v in x], [v + 10**19 for v in y]))

    def test_independent_reference_random_vectors(self):
        random.seed(53)
        for _ in range(100):
            x = [random.randrange(10000) for _ in range(30)]
            y = [random.randrange(10000) for _ in range(30)]
            self.assertAlmostEqual(pearson(x, y)[0], statistics.correlation(x, y), delta=1e-14)

    def test_uses_first_differences_not_cumulative_or_levels(self):
        x, y = [1, 3, 2, 8, 4, 7], [4, 5, 8, 10, 10, 13]
        c, _, _ = self.summarize(x, y)
        self.assertAlmostEqual(c['value'], statistics.correlation(differences(x), differences(y)), delta=1e-14)
        self.assertNotAlmostEqual(c['value'], c['level_value'])

    def test_fills_zero_buckets_before_differencing(self):
        x, y = [1, 0, 3, 0, 0, 2], [3, 0, 4, 0, 0, 6]
        c, dense, meta = self.summarize(x, y, sparse=True)
        self.assertEqual([row['events'] for row in dense], y)
        self.assertEqual(c['counts'], x)
        self.assertEqual(meta['difference_pairs'], 5)
        self.assertEqual(c['value'], pearson(differences(x), differences(y))[0])

    def test_partial_edges_excluded_without_removing_plot_points(self):
        c, dense, meta = self.summarize([99, 1, 2, 0, 3, 1, 5, 99], [100, 3, 5, 0, 6, 8, 7, 100], start=1, end=74)
        self.assertEqual(c['counts'], [1, 2, 0, 3, 1, 5])
        self.assertEqual(meta['excluded_partial_buckets'], 2)
        self.assertEqual(len(dense), 8)
        self.assertEqual(meta['complete_buckets'], 6)

    def test_no_false_scores_for_short_or_constant_series(self):
        for x, y, status in [([1, 2, 4], [2, 4, 8], 'insufficient_buckets'),
                             ([1] * 6, [2, 4, 3, 9, 2, 6], 'constant_sql'),
                             ([1, 3, 1, 5, 2, 6], [6] * 6, 'constant_total'),
                             ([0] * 6, [0] * 6, 'constant_sql')]:
            with self.subTest(status=status), self.assertLogs('app.slowlog_correlation', level='WARNING'):
                c, _, _ = self.summarize(x, y)
            self.assertEqual(c['status'], status)
            self.assertIsNone(c['value'])

    def test_linear_ramp_has_no_growth_variance(self):
        with self.assertLogs('app.slowlog_correlation', level='WARNING'):
            c, _, _ = self.summarize([1, 2, 3, 4, 5, 6], [2, 4, 6, 8, 10, 12])
        self.assertIsNone(c['value'])
        self.assertEqual(c['level_value'], 1.0)

    def test_total_includes_non_top_sql_and_self_is_disclosed(self):
        x = [1, 4, 0, 3, 0, 9]
        c, _, _ = self.summarize(x, [v + 10 for v in x])
        self.assertEqual(c['value'], 1.0)
        self.assertIsNone(c['without_self_value'])
        self.assertEqual(c['without_self_status'], 'constant_total')
        self.assertAlmostEqual(c['event_share'], sum(x) / (sum(x) + 60))

    def test_missing_series_is_not_zero(self):
        rows = [{"fingerprint": "absent"}]
        with self.assertLogs('app.slowlog_correlation', level='WARNING'):
            attach_correlations({'executions': rows}, [], {}, start_us=0, end_us=99, width_us=10)
        self.assertEqual(rows[0]['correlation']['status'], 'series_unavailable')
        self.assertIsNone(rows[0]['correlation']['value'])

    def test_invalid_component_counts_do_not_produce_score(self):
        with self.assertLogs('app.slowlog_correlation', level='WARNING'):
            c, _, _ = self.summarize([100, 1, 2, 1, 2, 1], [2] * 6)
        self.assertEqual(c['status'], 'series_unavailable')
        self.assertIsNone(c['event_share'])

    def test_rejects_misaligned_vectors(self):
        with self.assertRaises(ValueError):
            pearson([1, 2], [1])


if __name__ == '__main__':
    unittest.main()
