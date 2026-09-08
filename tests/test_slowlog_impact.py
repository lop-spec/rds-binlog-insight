import unittest
from unittest.mock import Mock, patch

from app.config import Settings
from app.slowlog_impact import DAY_US, execution_series, rank_resource_overlap, rank_performance_growth
from app.slowlog_impact_service import load_iops_points, query_resource_overlap

W = 60_000_000


def event(fp, start, duration, node="a"):
    return dict(event_id=fp, fingerprint=fp, start_us=start,
                duration_ms=duration, node_id=node, sql_id=fp)


def points(values, node="a"):
    return [dict(timestamp=(i + 1) * 60000, nodeId=node, Average=y)
            for i, y in enumerate(values)]


class ImpactTests(unittest.TestCase):
    def rank(self, events, values, **kw):
        return rank_resource_overlap(events, points(values), start_us=0,
                                     end_us=6 * W - 1, index_complete=True, **kw)

    def test_single_long_execution_beats_short_coincident_execution(self):
        rows = [event("long", 3 * W, 180000), event("short", 3 * W, 1000)]
        result = self.rank(rows, [10, 10, 10, 90, 90, 90])["nodes"][0]
        self.assertEqual(result["statements"][0]["fingerprint"], "long")
        self.assertEqual(result["statements"][0]["runtime_us_total"], 180_000_000)
        self.assertEqual(result["statements"][0]["resource_r"], 1.0)
        self.assertAlmostEqual(sum(r["overlap_share"] for r in result["statements"]), 1)

    def test_cross_bucket_overlap_and_partial_edges_are_exact(self):
        data = execution_series([event("f", W // 2, 150000)], 1, 4 * W - 2)
        self.assertEqual(dict(data["a", "f"]), {2 * W: W, 3 * W: W})

    def test_concurrency_sums_and_never_estimates_scan_rate(self):
        rows = [event("one", 0, 60000), dict(event("two", 0, 60000), fingerprint="one")]
        result = execution_series(rows, 0, 6 * W - 1)
        self.assertEqual(result["a", "one"][W], 2 * W)

    def test_duplicates_negative_durations_and_wrong_scope_rejected(self):
        for rows in ([event("x", 0, -1)], [event("x", -1, 1)],
                     [event("x", 0, 1), event("x", 0, 1)]):
            with self.assertRaises(ValueError):
                execution_series(rows, 0, 6 * W - 1)

    def test_missing_resource_sample_never_zero_filled(self):
        with self.assertLogs("app.slowlog_impact", "WARNING"):
            result = rank_resource_overlap([event("x", W, 1)], points([1, 2, 3, 4, 5]),
                                           start_us=0, end_us=6 * W - 1, index_complete=True)
        self.assertEqual(result["nodes"][0]["status"], "metric_gaps")
        self.assertEqual(result["nodes"][0]["statements"], [])

    def test_node_identity_is_not_pooled(self):
        with self.assertLogs("app.slowlog_impact", "WARNING"):
            result = self.rank([event("x", W, 1, "other")], [1, 2, 3, 4, 5, 6])
        wrong = next(n for n in result["nodes"] if n["node_id"] == "other")
        self.assertEqual(wrong["status"], "metric_gaps")

    def test_constant_resource_has_no_score(self):
        with self.assertLogs("app.slowlog_impact", "WARNING"):
            result = self.rank([event("x", W, 1000)], [10] * 6)["nodes"][0]
        self.assertEqual(result["status"], "no_excess_overlap")
        self.assertIsNone(result["statements"][0]["overlap_share"])
        self.assertIsNone(result["statements"][0]["rank"])

    def test_decimal_values_and_large_baseline_preserved(self):
        rows = [event("x", 3 * W, 180000)]
        first = self.rank(rows, [".001"] * 3 + [".002"] * 3)
        second = self.rank(rows, ["100000000000.001"] * 3 + ["100000000000.002"] * 3)
        a, b = first["nodes"][0]["statements"][0], second["nodes"][0]["statements"][0]
        self.assertEqual(a["score_numerator"], b["score_numerator"])
        self.assertEqual(a["resource_r"], b["resource_r"])

    def test_incomplete_source_rejected_before_calculation(self):
        with self.assertLogs("app.slowlog_impact", "WARNING"):
            result = rank_resource_overlap([], [], start_us=0, end_us=W, index_complete=False)
        self.assertEqual(result["status"], "incomplete_index")

    def test_nan_and_conflicting_metric_points_rejected(self):
        for extra, status in [([dict(timestamp=60000, nodeId="a", Average="NaN")], "invalid_metric_value"),
                              ([dict(timestamp=60000, nodeId="a", Average=999)], "conflicting_metric_samples")]:
            with self.assertLogs("app.slowlog_impact", "WARNING"):
                result = rank_resource_overlap([event("x", W, 1)], points([1, 2, 3, 4, 5, 6]) + extra,
                                               start_us=0, end_us=6 * W - 1, index_complete=True)
            self.assertEqual(result["status"], status)


class GrowthTests(unittest.TestCase):
    def rank(self, current, before):
        previous = [{**e, 'start_us': e['start_us'] - DAY_US} for e in before]
        return rank_performance_growth(current, previous, points([10, 10, 10, 90, 90, 90]),
                                       start_us=0, end_us=6*W-1, index_complete=True, baseline_complete=True)

    def test_large_unchanged_load_does_not_outrank_growing_load(self):
        steady = event('steady', 3*W, 180000)
        growing = event('growing', 3*W, 60000)
        rows = self.rank([steady, growing], [steady])['nodes'][0]['statements']
        self.assertEqual(rows[0]['sql_id'], 'growing')
        self.assertEqual(rows[0]['growth_share'], 1)
        self.assertIsNone(rows[1]['rank'])

    def test_native_sql_family_merges_shards_but_retains_actual_fingerprints(self):
        one = dict(event('physical-one', 3*W, 60000), sql_id='family')
        two = dict(event('physical-two', 4*W, 60000), sql_id='family')
        rows = self.rank([one,two], [])['nodes'][0]['statements']
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]['member_fingerprints'], ['physical-one','physical-two'])
        self.assertEqual(rows[0]['runtime_us_total'], 120000000)
        self.assertIsInstance(rows[0]['growth_score_numerator'], str)

    def test_database_identity_prevents_wrong_family_merge(self):
        one = dict(event('one',3*W,60000),sql_id='family',database_name='db1')
        two = dict(event('two',4*W,60000),sql_id='family',database_name='db2')
        self.assertEqual(len(self.rank([one,two],[])['nodes'][0]['statements']),2)

    def test_no_growth_is_not_silently_replaced_by_absolute_load(self):
        e=event('steady',3*W,180000)
        with self.assertLogs('app.slowlog_impact','WARNING'):
            node=self.rank([e],[e])['nodes'][0]
        self.assertEqual(node['status'],'no_growth_overlap')
        self.assertIsNone(node['statements'][0]['growth_share'])

    def test_missing_baseline_cannot_be_treated_as_zero(self):
        with self.assertLogs('app.slowlog_impact','WARNING'):
            result=rank_performance_growth([],[],[],start_us=0,end_us=W,index_complete=True,baseline_complete=False)
        self.assertEqual(result['status'],'incomplete_baseline_index')


class ServiceTests(unittest.TestCase):
    def test_pagination_and_node_identity(self):
        client = Mock()
        client.call.side_effect = [dict(Code="200", Datapoints='[]', NextToken="next"),
                                  dict(Code="200", Datapoints='[]')]
        with patch("app.slowlog_impact_service.CmsRpcClient", return_value=client):
            self.assertEqual(load_iops_points(Settings(), 0, 6 * W - 1, "instance", "node", lambda _: object()), [])
        self.assertEqual(client.call.call_count, 2)
        self.assertEqual(client.call.call_args.args[1]["NextToken"], "next")
        self.assertEqual(client.call.call_args.args[1]["EndTime"], "360000")

    def test_foreign_instance_not_accepted(self):
        client = Mock()
        client.call.return_value = dict(Code="200", Datapoints='[{"instanceId":"wrong"}]')
        with patch("app.slowlog_impact_service.CmsRpcClient", return_value=client):
            with self.assertRaisesRegex(RuntimeError, "identity"):
                load_iops_points(Settings(), 0, W, "instance", credential_loader=lambda _: object())

    def test_missing_backend_logged_without_fallback_scan(self):
        loader = Mock()
        with self.assertLogs("app.slowlog_impact_service", "WARNING"):
            result = query_resource_overlap(Mock(), None, {}, Settings(), loader)
        self.assertEqual(result["status"], "clickhouse_backend_required")
        loader.assert_not_called()

    def test_bound_enforced_before_cloud_call(self):
        backend, metadata, loader = Mock(), Mock(), Mock()
        backend._window.return_value = (0, 6 * W - 1)
        backend._manifest_coverage.return_value = {"complete": True, "total_parts": 1}
        backend._scope_sql.return_value = ("SELECT 1", {})
        backend._rows.return_value = [None] * 250001
        with self.assertLogs("app.slowlog_impact_service", "WARNING"):
            result = query_resource_overlap(metadata, backend, {"source": "slowlog", "instance": "x"}, Settings(), loader)
        self.assertEqual(result["status"], "event_limit_exceeded")
        loader.assert_not_called()


if __name__ == "__main__":
    unittest.main()
