import unittest
from unittest.mock import Mock, patch

from app.config import Settings
from app.slowlog_impact import DAY_US, execution_series, order_by_correlation, rank_resource_overlap, rank_performance_growth
from app.slowlog_impact_service import (RDS_METRICS, load_iops_points, load_metric_points,
                                       nearest_indexed_window, query_resource_overlap)

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
        sql=backend._rows.call_args.args[0]
        self.assertIn('nullIf(metric_rows_examined,0)',sql)
        self.assertIn('nullIf(metric_lock_time_ms,0)',sql)


def runs(name, starts, duration=30_000):
    """Executions of one SQL family, each with its own event id (the ranker rejects duplicates)."""
    return [dict(event(name, start, duration), event_id=f"{name}-{start}") for start in starts]


class CorrelationOrderTests(unittest.TestCase):
    def ranked(self, current, before, values):
        previous = [{**e, "start_us": e["start_us"] - DAY_US} for e in before]
        return rank_performance_growth(current, previous, points(values), start_us=0, end_us=6 * W - 1,
                                       index_complete=True, baseline_complete=True)

    def test_orders_by_signed_r_descending_not_by_cost_or_evidence(self):
        # metric rises 10 -> 60. "ramp" runs longer each minute (r = +1), "inverse" shorter (r = -1),
        # "big" runs the whole window at a constant rate: the largest cost, but its coefficient is undefined.
        ramp = [dict(event("ramp", i * W, (i + 1) * 10_000), event_id=f"ramp-{i}") for i in range(6)]
        inverse = [dict(event("inverse", i * W, (6 - i) * 10_000), event_id=f"inverse-{i}") for i in range(6)]
        big = [event("big", 0, 360_000)]
        result = self.ranked(big + inverse + ramp, [], [10, 20, 30, 40, 50, 60])
        order_by_correlation(result)
        rows = result["nodes"][0]["statements"]
        self.assertEqual([r["sql_id"] for r in rows], ["ramp", "inverse", "big"])
        by = {r["sql_id"]: r for r in rows}
        self.assertGreater(by["ramp"]["resource_r"], 0.99)
        self.assertLess(by["inverse"]["resource_r"], -0.99)
        self.assertIsNone(by["big"]["resource_r"])
        self.assertGreater(by["big"]["runtime_us_total"], by["ramp"]["runtime_us_total"])
        self.assertEqual([r["correlation_rank"] for r in rows], [1, 2, None])
        self.assertEqual(by["ramp"]["active_minutes"], 6)
        self.assertEqual(result["order"], "correlation")

    def test_uncomputable_coefficients_follow_computable_ones_in_stable_order(self):
        flat = [event(name, 0, 360_000) for name in ("b-flat", "a-flat")]
        ramp = [dict(event("ramp", i * W, (i + 1) * 10_000), event_id=f"ramp-{i}") for i in range(6)]
        result = self.ranked(flat + ramp, [], [1, 2, 3, 4, 5, 6])
        order_by_correlation(result)
        rows = result["nodes"][0]["statements"]
        self.assertEqual(rows[0]["sql_id"], "ramp")
        tail = [r for r in rows if r["resource_r"] is None]
        self.assertEqual([r["sql_id"] for r in tail], ["a-flat", "b-flat"])
        self.assertTrue(all(r["correlation_rank"] is None for r in tail))


def json_key(sql_id):
    import json
    return json.dumps(["", "sql_id", sql_id], separators=(",", ":"))


class MetricSelectionTests(unittest.TestCase):
    def test_registry_covers_the_main_rds_metrics(self):
        self.assertEqual(list(RDS_METRICS), ["cpu", "iops", "rows_read", "row_lock", "threads"])
        self.assertEqual(RDS_METRICS["cpu"][0], "Cluster_CpuUsage")
        self.assertEqual(RDS_METRICS["iops"][0], "Cluster_IOPSUsage")

    def test_loader_requests_the_named_cms_metric(self):
        client = Mock()
        client.call.return_value = dict(Code="200", Datapoints="[]")
        with patch("app.slowlog_impact_service.CmsRpcClient", return_value=client):
            load_metric_points(Settings(), 0, W, "instance", credential_loader=lambda _: object(), metric_name="Cluster_CpuUsage")
            load_iops_points(Settings(), 0, W, "instance", credential_loader=lambda _: object())
        names = [c.args[1]["MetricName"] for c in client.call.call_args_list]
        self.assertEqual(names, ["Cluster_CpuUsage", "Cluster_IOPSUsage"])

    def test_unsupported_metric_is_reported_before_any_read(self):
        backend, loader = Mock(), Mock()
        backend.serving_enabled = True
        with self.assertLogs("app.slowlog_impact_service", "WARNING"):
            result = query_resource_overlap(Mock(), backend, {"source": "slowlog", "instance": "x", "metric": "nope"},
                                            Settings(), loader)
        self.assertEqual(result["status"], "unsupported_metric")
        backend._window.assert_not_called()
        loader.assert_not_called()

    def test_default_metric_keeps_the_old_loader_signature_and_other_metrics_pass_the_name(self):
        for metric, expect in (("", {}), ("cpu", {"metric_name": "Cluster_CpuUsage"})):
            backend, metadata, loader = Mock(), Mock(), Mock(return_value=[])
            backend.serving_enabled = True
            # the baseline window must echo the requested range or the service reports baseline_outside_retention
            backend._window.side_effect = lambda q, retention: ((q["start_epoch_us"], q["end_epoch_us"])
                                                                 if "start_epoch_us" in q else (0, 6 * W - 1))
            backend._manifest_coverage.return_value = {"complete": True, "total_parts": 1}
            backend._scope_sql.return_value = ("SELECT 1", {})
            backend._rows.return_value = [dict(event_id="e", start_us=W, node_id="a", database_name="d", fingerprint="f",
                                               sql_id="s", duration_ms=1000, rows_examined=1, lock_time_ms=1)]
            query_resource_overlap(metadata, backend, {"source": "slowlog", "instance": "x", "metric": metric},
                                   Settings(), loader)
            self.assertEqual(loader.call_args_list[0].kwargs, expect)
            self.assertEqual(loader.call_args_list[1].kwargs, expect)  # the baseline day uses the same metric

MIN = 60_000_000


class IndexedWindowTests(unittest.TestCase):
    """A window whose slow-log index (or day-before baseline index) is incomplete moves to the nearest one that is."""
    NOW = 1_790_000_040_000_000  # a whole minute
    STEP = 10 * MIN
    FIRST = NOW - NOW % STEP - 3 * DAY_US

    def setUp(self):
        count = (self.NOW - self.FIRST) // self.STEP + 2
        self.parts = [dict(path=f"p{k}", logical_part_id=f"id{k}", min_event_epoch_us=self.FIRST + k * self.STEP,
                           max_event_epoch_us=self.FIRST + (k + 1) * self.STEP) for k in range(count)]
        patcher = patch("app.slowlog_impact_service.time.time_ns", return_value=self.NOW * 1000)
        patcher.start()
        self.addCleanup(patcher.stop)

    def overlapping(self, low, high):
        return [p for p in self.parts if p["max_event_epoch_us"] >= low and p["min_event_epoch_us"] <= high]

    def fixture(self, not_ready=(), reconciled=True):
        metadata, backend = Mock(), Mock()
        backend.serving_enabled = True
        metadata.parts_in_range.side_effect = lambda *, start_epoch_us, end_epoch_us, source, instance, **_: (
            self.overlapping(start_epoch_us, end_epoch_us) if (source, instance) == ("slowlog", "x") else [])

        def coverage(parts):
            expected = [p for p in parts if p.get("logical_part_id")]
            missing = [p["path"] for p in expected if p["path"] in not_ready]
            return dict(complete=reconciled and not missing, total_parts=len(expected), covered_parts=len(expected) - len(missing),
                        missing_parts=missing, reconcile_completed_at_us=1 if reconciled else 0)
        backend._manifest_coverage.side_effect = coverage
        backend._window.side_effect = lambda q, days: (max(q["start_epoch_us"], self.NOW - days * DAY_US), min(q["end_epoch_us"], self.NOW))
        backend._scope_sql.side_effect = lambda q, low, high: ("SELECT 1", {"low": low, "high": high})
        backend._rows.side_effect = lambda sql, params, _: [dict(event_id=f"e{params['low']}", start_us=params["low"] + MIN, node_id="a",
                                                                database_name="d", fingerprint="f", sql_id="s", duration_ms=1000,
                                                                rows_examined=1, lock_time_ms=1)]
        return metadata, backend

    def test_window_running_past_the_ready_index_moves_earlier_to_the_last_indexed_minute(self):
        ready_until = (self.NOW - 25 * MIN) // self.STEP * self.STEP
        metadata, backend = self.fixture({p["path"] for p in self.parts if p["max_event_epoch_us"] > ready_until})
        found = nearest_indexed_window(metadata, backend, "x", self.NOW - 60 * MIN, self.NOW, 60)
        # a window ending on the first unready part's start would still touch it, so the last clean minute is one earlier
        self.assertEqual(found, (ready_until - 61 * MIN, ready_until - MIN))

    def test_incomplete_baseline_day_moves_to_the_nearer_side_of_the_hole(self):
        start, end = self.NOW - 5 * 3600 * 1_000_000, self.NOW - 4 * 3600 * 1_000_000
        hole = self.overlapping(self.NOW - DAY_US - 290 * MIN, self.NOW - DAY_US - 265 * MIN)
        metadata, backend = self.fixture({p["path"] for p in hole})
        early = min(p["min_event_epoch_us"] for p in hole) + DAY_US - end - MIN
        late = max(p["max_event_epoch_us"] for p in hole) + DAY_US - start + MIN
        shift = early if abs(early) <= abs(late) else late
        self.assertEqual(nearest_indexed_window(metadata, backend, "x", start, end, 60), (start + shift, end + shift))
        # served through the analysis, the move is reported with its reason and the baseline stays a day behind
        loader = Mock(return_value=[])
        result = query_resource_overlap(metadata, backend, {"source": "slowlog", "instance": "x", "start_epoch_us": start,
                                                            "end_epoch_us": end}, Settings(), loader)
        self.assertEqual(result["adjusted"], dict(reason="incomplete_baseline_index", requested_start_us=start,
                                                  requested_end_us=end, shift_us=shift))
        self.assertEqual((result["start_us"], result["end_us"]), (start + shift, end + shift))
        self.assertEqual(loader.call_args_list[0].args[1:3], (start + shift, end + shift))
        self.assertEqual(loader.call_args_list[1].args[1:3], (start + shift - DAY_US, end + shift - DAY_US))
        self.assertTrue(result["coverage"]["complete"] and result["baseline_coverage"]["complete"])

    def test_indexed_window_is_left_alone(self):
        metadata, backend = self.fixture()
        start, end = self.NOW - 3 * 3600 * 1_000_000, self.NOW - 2 * 3600 * 1_000_000
        loader = Mock(return_value=[])
        result = query_resource_overlap(metadata, backend, {"source": "slowlog", "instance": "x", "start_epoch_us": start,
                                                            "end_epoch_us": end}, Settings(), loader)
        self.assertIsNone(result["adjusted"])
        self.assertEqual((result["start_us"], result["end_us"]), (start, end))
        self.assertEqual(metadata.parts_in_range.call_count, 2)  # the window and its baseline, no search

    def test_nothing_indexed_within_reach_is_reported_not_guessed(self):
        metadata, backend = self.fixture({p["path"] for p in self.parts})
        start, end = self.NOW - 3 * 3600 * 1_000_000, self.NOW - 2 * 3600 * 1_000_000
        self.assertIsNone(nearest_indexed_window(metadata, backend, "x", start, end, 60))
        with self.assertLogs("app.slowlog_impact_service", "WARNING") as logs:
            result = query_resource_overlap(metadata, backend, {"source": "slowlog", "instance": "x", "start_epoch_us": start,
                                                                "end_epoch_us": end}, Settings(), Mock())
        self.assertEqual(result["status"], "incomplete_index")
        self.assertIn("incomplete_index", logs.output[0])

    def test_unreconciled_index_never_yields_a_window(self):
        metadata, backend = self.fixture(reconciled=False)
        self.assertIsNone(nearest_indexed_window(metadata, backend, "x", self.NOW - 3 * 3600 * 1_000_000, self.NOW - 2 * 3600 * 1_000_000, 60))

    def test_baseline_day_must_stay_inside_retention(self):
        metadata, backend = self.fixture()
        start, end = self.NOW - 30 * 3600 * 1_000_000, self.NOW - 29 * 3600 * 1_000_000
        # plenty of retention: the neighbouring minute is already indexed
        self.assertEqual(nearest_indexed_window(metadata, backend, "x", start, end, 60), (start - MIN, end - MIN))
        # two days of retention: an earlier window's baseline day would be expired, so only the far later side qualifies
        self.assertEqual(nearest_indexed_window(metadata, backend, "x", start, end, 2), (self.NOW - DAY_US, self.NOW - DAY_US + 3600 * 1_000_000))


if __name__ == "__main__":
    unittest.main()
