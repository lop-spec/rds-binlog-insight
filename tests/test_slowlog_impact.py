import unittest
from fractions import Fraction
from unittest.mock import Mock, patch

from app.config import Settings
from app.slowlog_impact import DAY_US, execution_series, order_by_correlation, rank_resource_overlap, rank_performance_growth
from app.lag_peers import LOOKBACK_US, METRIC_NAMES
from app.slowlog_impact_service import (RDS_METRICS, WRITES_LEAD_US, lag_candidates, lag_increase_points, load_iops_points,
                                       load_metric_points, nearest_indexed_window, query_resource_overlap, truncate_to_ready)

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

    def test_a_long_run_with_almost_no_recorded_scan_outranks_many_small_scan_growers(self):
        # DAS can log a few rows for a statement that ran for minutes (materialized subqueries). "Scanned more rows than
        # yesterday" is evidence, not a gate: thirty statements that scanned a million rows each must not bury it.
        long_run = dict(event('long', 3*W, 180000), rows_examined=9)
        noise = [dict(event(f'noise-{i}', 3*W, 1000), rows_examined=1_000_000 + i) for i in range(30)]
        rows = self.rank([long_run, *noise], [])['nodes'][0]['statements']
        self.assertEqual((rows[0]['sql_id'], rows[0]['rank']), ('long', 1))
        self.assertAlmostEqual(rows[0]['growth_share'], 180 / 210)
        self.assertEqual(rows[0]['attribution']['status'], 'scan_growth_related_evidence')   # its own few rows still count as evidence
        self.assertTrue(all(row['rank'] > 1 for row in rows[1:]))

    def test_measured_scan_growth_only_separates_statements_with_equal_scores(self):
        before = [dict(event('small', 3*W, 60000), rows_examined=10), dict(event('big', 3*W, 60000), rows_examined=10)]
        now = [dict(event('small', 3*W, 60000), rows_examined=110), dict(event('big', 3*W, 60000), rows_examined=5010)]
        node = self.rank(now, before)['nodes'][0]
        self.assertEqual([row['sql_id'] for row in node['statements']], ['big', 'small'])
        self.assertEqual([row['rank'] for row in node['statements']], [1, 2])
        self.assertTrue(all(row['growth_share'] is None for row in node['statements']))   # no runtime grew: no share is invented
        self.assertEqual(node['status'], 'ok')

    def test_growth_that_is_only_recorded_lock_wait_scores_nothing(self):
        before = [dict(event('waits', 3*W, 10000), lock_time_ms=0), event('works', 3*W, 10000)]
        now = [dict(event('waits', 3*W, 70000), lock_time_ms=60000), dict(event('works', 3*W, 70000), lock_time_ms=None)]
        by = {row['sql_id']: row for row in self.rank(now, before)['nodes'][0]['statements']}
        self.assertEqual(by['works']['rank'], 1)
        self.assertEqual(by['waits']['growth_share'], 0)
        self.assertIsNone(by['waits']['rank'])
        self.assertEqual(by['waits']['lock_wait_excluded_us'], 60_000_000)   # the page can say how much was taken out
        self.assertEqual(by['works']['lock_wait_excluded_us'], 0)            # an unreported lock time is no evidence of waiting
        self.assertIn('lock_wait_increased_possible_victim', by['waits']['attribution']['warnings'])

    def test_without_lock_time_the_score_is_exactly_the_plain_runtime_growth(self):
        steady, growing = event('steady', 3*W, 180000), event('growing', 3*W, 90000)
        rows = self.rank([steady, growing], [steady, event('growing', 3*W, 30000)])['nodes'][0]['statements']
        self.assertEqual(len(rows), 2)
        for row in rows:
            plain = Fraction(row['score_numerator'] * max(0, row['runtime_us_total'] - row['baseline_runtime_us_total']),
                             row['runtime_us_total'])
            self.assertEqual(Fraction(int(row['growth_score_numerator']), int(row['growth_score_denominator'])), plain)
            self.assertEqual(row['lock_wait_excluded_us'], 0)


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

    def test_statements_carry_the_accounts_that_ran_them(self):
        backend, metadata = Mock(), Mock()
        backend.serving_enabled = True
        backend._window.side_effect = lambda q, retention: ((q["start_epoch_us"], q["end_epoch_us"])
                                                             if "start_epoch_us" in q else (0, 6 * W - 1))
        backend._manifest_coverage.return_value = {"complete": True, "total_parts": 1}
        backend._scope_sql.side_effect = lambda q, low, high: ("SELECT 1", {"low": low})
        backend.statement_index.statement_profiles.return_value = {}

        def make(i, sql_id, account):
            return dict(event_id=f"{sql_id}-{i}", start_us=3 * W + i * 1000, node_id="a", database_name="d", fingerprint=sql_id,
                        sql_id=sql_id, duration_ms=30000, rows_examined=1, lock_time_ms=None, account=account)
        current = [make(i, "mixed", "analyst") for i in range(3)] + [make(3, "mixed", "root")] + [make(i, "plain", "") for i in range(2)]
        backend._rows.side_effect = lambda sql, params, _: (
            [dict(e, start_us=e["start_us"] - DAY_US) for e in current[:1]] if params["low"] < 0 else current)
        loader = Mock(return_value=points([10, 10, 10, 90, 90, 90]))
        result = query_resource_overlap(metadata, backend, {"source": "slowlog", "instance": "x"}, Settings(), loader)
        rows = {row["sql_id"]: row for row in result["nodes"][0]["statements"]}
        self.assertEqual((rows["mixed"]["accounts"], rows["mixed"]["account_count"]), (["analyst", "root"], 2))   # most executions first
        self.assertEqual((rows["plain"]["accounts"], rows["plain"]["account_count"]), ([], 0))                    # unknown stays unknown
        self.assertIn("metric_database_account AS account", backend._rows.call_args_list[0].args[0])

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
        self.assertEqual(list(RDS_METRICS), ["cpu", "iops", "rows_read", "row_lock", "threads", "replication_lag"])
        self.assertEqual(RDS_METRICS["replication_lag"][0], "Cluster_DataDelay")
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
        self.assertEqual(result["adjusted"], dict(kind="shifted", reason="incomplete_baseline_index", requested_start_us=start,
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
        self.assertIsNone(result["ready_through_us"])  # an old window says nothing about how far behind the index is
        self.assertEqual((result["start_us"], result["end_us"]), (start, end))
        self.assertEqual(metadata.parts_in_range.call_count, 2)  # the window and its baseline, no search

    def query(self, metadata, backend, start, end, **kw):
        loader = Mock(return_value=[])
        result = query_resource_overlap(metadata, backend, {"source": "slowlog", "instance": "x", "start_epoch_us": start,
                                                            "end_epoch_us": end}, Settings(), loader, **kw)
        return result, loader

    def test_window_ending_now_is_cut_back_to_the_indexed_edge_and_keeps_its_start(self):
        ready_until = (self.NOW - 12 * MIN) // self.STEP * self.STEP
        metadata, backend = self.fixture({p["path"] for p in self.parts if p["max_event_epoch_us"] > ready_until})
        start, end = self.NOW - 60 * MIN, self.NOW
        result, loader = self.query(metadata, backend, start, end)
        cut = ready_until - MIN  # the first unready part starts at ready_until, so the last clean minute is one earlier
        self.assertEqual(result["adjusted"], dict(kind="truncated", reason="incomplete_index", requested_start_us=start,
                                                  requested_end_us=end, truncated_us=end - cut))
        self.assertEqual((result["start_us"], result["end_us"]), (start, cut))
        self.assertEqual(loader.call_args_list[0].args[1:3], (start, cut))
        self.assertEqual(loader.call_args_list[1].args[1:3], (start - DAY_US, cut - DAY_US))  # the baseline day is cut the same
        self.assertTrue(result["coverage"]["complete"] and result["baseline_coverage"]["complete"])
        self.assertEqual(result["ready_through_us"], ready_until)  # how far behind the index is, for the page

    def test_a_cut_shorter_than_six_minutes_moves_the_window_instead(self):
        ready_until = (self.NOW - 12 * MIN) // self.STEP * self.STEP
        metadata, backend = self.fixture({p["path"] for p in self.parts if p["max_event_epoch_us"] > ready_until})
        start = ready_until - 4 * MIN  # only 3 clean minutes would be left
        result, _loader = self.query(metadata, backend, start, self.NOW)
        self.assertEqual(result["adjusted"]["kind"], "shifted")
        self.assertLessEqual(result["end_us"], ready_until - MIN)
        self.assertEqual(result["end_us"] - result["start_us"], self.NOW - start)

    def test_truncate_to_ready_needs_an_indexed_start_and_stops_before_the_first_unready_part(self):
        hole = self.overlapping(self.NOW - 40 * MIN, self.NOW - 31 * MIN)
        metadata, backend = self.fixture({p["path"] for p in hole})
        hole_low = min(p["min_event_epoch_us"] for p in hole)
        self.assertEqual(truncate_to_ready(metadata, backend, "x", self.NOW - 90 * MIN, self.NOW - 20 * MIN), hole_low - MIN)
        # the start inside the hole, or an index that was never reconciled: nothing to keep
        self.assertIsNone(truncate_to_ready(metadata, backend, "x", hole_low + 3 * MIN, self.NOW - 20 * MIN))
        unreconciled, backend2 = self.fixture(reconciled=False)
        self.assertIsNone(truncate_to_ready(unreconciled, backend2, "x", self.NOW - 90 * MIN, self.NOW - 20 * MIN))

    def test_nodes_carry_their_role_and_a_broken_lookup_only_drops_the_label(self):
        metadata, backend = self.fixture()
        start, end = self.NOW - 3 * 3600 * 1_000_000, self.NOW - 2 * 3600 * 1_000_000
        seen = []
        result, _loader = self.query(metadata, backend, start, end, roles=lambda instance: (
            seen.append(instance) or {"a": {"role": "primary", "zone": "cn-chengdu-a", "status": "active"}}))
        self.assertEqual(seen, ["x"])
        self.assertEqual([(n["node_id"], n["role"], n["zone"]) for n in result["nodes"]], [("a", "primary", "cn-chengdu-a")])

        def broken(_instance):
            raise RuntimeError("lookup down")
        with self.assertLogs("app.slowlog_impact_service", "WARNING") as logs:
            result, _loader = self.query(metadata, backend, start, end, roles=broken)
        self.assertTrue(any("node roles unavailable" in line for line in logs.output))
        self.assertEqual([(n["role"], n["zone"]) for n in result["nodes"]], [("", "")])

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


def lag_points(levels, node="r", first_ms=0):
    """Minute samples of a lag level; the first one sits before the window, like the loader's extra minute."""
    return [dict(timestamp=first_ms + i * 60000, nodeId=node, Average=value) for i, value in enumerate(levels)]


class LagMetricTests(unittest.TestCase):
    """Replication lag: the candidates are the primary's statements, scored on the minutes in which the lag grew."""

    ROLES = {"p": {"role": "primary", "zone": "a"}, "r": {"role": "secondary", "zone": "b"}}

    def test_increase_points_are_the_seconds_each_minute_added_and_a_gap_is_not_zero(self):
        levels = lag_points([0, 0, 30, 90, 90, 60])
        grown = {p["timestamp"]: p["Average"] for p in lag_increase_points(levels)}
        self.assertEqual(grown, {60000: 0, 120000: 30, 180000: 60, 240000: 0, 300000: 0})   # the first sample has no predecessor
        gap = [p for p in levels if p["timestamp"] != 180000]
        stamps = {p["timestamp"] for p in lag_increase_points(gap)}
        self.assertNotIn(180000, stamps)
        self.assertNotIn(240000, stamps)                                                       # its predecessor is the missing one

    def test_the_score_uses_the_growth_series_while_the_node_still_reports_the_level(self):
        # the lag level climbs 0 -> 180 in the last three minutes
        levels = [0, 0, 0, 0, 60, 120, 180]
        runs_late = [dict(event("late", 3 * W, 180000), event_id="late")]
        runs_always = [dict(event("always", i * W, 60000), event_id=f"always-{i}") for i in range(6)]
        result = rank_resource_overlap(runs_late + runs_always, lag_points(levels, "a"), start_us=0, end_us=6 * W - 1,
                                       index_complete=True, score_points=lag_increase_points(lag_points(levels, "a")))
        node = result["nodes"][0]
        self.assertEqual(node["peak"], 180)
        self.assertEqual([v / node["metric_scale"] for v in node["metric_values"]], [0, 0, 0, 60, 120, 180])
        self.assertEqual([v / node["score_scale"] for v in node["score_values"]], [0, 0, 0, 60, 60, 60])
        self.assertEqual(next(r for r in node["statements"] if r["fingerprint"] == "late")["resource_r"], 1.0)   # runs exactly in the growth minutes
        by = {row["fingerprint"]: row for row in node["statements"]}
        self.assertIsNone(by["always"]["resource_r"])                                           # constant runtime: no coefficient
        # Without a separate score series the same call ranks on the level itself (the metric of every other mode)
        plain = rank_resource_overlap(runs_late + runs_always, lag_points(levels, "a"), start_us=0, end_us=6 * W - 1, index_complete=True)
        self.assertNotIn("score_values", plain["nodes"][0])
        self.assertLess(next(r for r in plain["nodes"][0]["statements"] if r["fingerprint"] == "late")["resource_r"], 1.0)   # a level is not the growth

    def test_candidates_are_copied_per_replica_with_their_own_identity(self):
        rows = [dict(event_id="e1", node_id="p", fingerprint="f", sql_id="s", start_us=0, duration_ms=1),
                dict(event_id="e2", node_id="r", fingerprint="f", sql_id="s", start_us=0, duration_ms=1)]
        copies = lag_candidates(rows, {"p"}, ["r", "r2"])
        self.assertEqual([(c["node_id"], c["event_id"], c["source_event_id"], c["origin"]) for c in copies],
                         [("r", "e1@r", "e1", "primary"), ("r2", "e1@r2", "e1", "primary")])
        from app.slowlog_impact import family_key
        self.assertNotEqual(family_key(copies[0]), family_key(rows[1]))                       # never merges with the replica's own SQL
        self.assertEqual(family_key(rows[0]), family_key({k: v for k, v in copies[0].items() if k != "origin"}))

    def fixture(self):
        backend, metadata = Mock(), Mock()
        backend.serving_enabled = True
        backend._window.side_effect = lambda q, retention: ((q["start_epoch_us"], q["end_epoch_us"])
                                                             if "start_epoch_us" in q else (0, 6 * W - 1))
        backend._manifest_coverage.return_value = {"complete": True, "total_parts": 1}
        backend._scope_sql.side_effect = lambda q, low, high: ("SELECT 1", {"low": low})
        backend.statement_index.statement_profiles.return_value = {}

        def make(event_id, node, fingerprint, start, duration, account="app"):
            return dict(event_id=event_id, start_us=start, node_id=node, database_name="shop", fingerprint=fingerprint,
                        sql_id=fingerprint, duration_ms=duration, rows_examined=1, lock_time_ms=None, account=account)
        current = [make("w1", "p", "bulk-write", 4 * W, 180000, "batch"),                       # the primary's write, in the growth minutes
                   make("w2", "p", "steady-write", 0, 360000),
                   make("v1", "r", "queued-read", 4 * W, 180000)]                               # the replica's own reader: a victim
        backend._rows.side_effect = lambda sql, params, _: (
            [dict(e, start_us=e["start_us"] - DAY_US) for e in current[1:2]] if params["low"] < 0 else current)
        return metadata, backend

    def run_lag(self, **kw):
        metadata, backend = self.fixture()
        loader = Mock(side_effect=lambda settings, start, end, instance, node="", **o: lag_points([0, 0, 0, 0, 60, 120, 180]))
        result = query_resource_overlap(metadata, backend, {"source": "slowlog", "instance": "x", "metric": "replication_lag"},
                                        Settings(), loader, roles=lambda _i: self.ROLES, **kw)
        return result, loader

    def test_a_replica_is_ranked_with_the_primarys_statements_and_the_primary_is_not_listed(self):
        result, loader = self.run_lag()
        self.assertEqual([n["node_id"] for n in result["nodes"]], ["r"])
        node = result["nodes"][0]
        self.assertEqual([r["sql_id"] for r in node["statements"]][0], "bulk-write")
        self.assertTrue(all(r["origin"] == "primary" and r["source_node_id"] == "p" for r in node["statements"]))
        self.assertNotIn("queued-read", [r["sql_id"] for r in node["statements"]])            # the replica's readers are evidence, not candidates
        self.assertEqual({r["sample_event_id"] for r in node["statements"]} - {"w1", "w2"}, set())   # the detail drawer gets real event ids
        self.assertEqual((result["metric_id"], result["metric"], result["candidate_scope"]),
                         ("replication_lag", "Cluster_DataDelay", "primary_statements"))
        self.assertEqual(result["primary_nodes"], ["p"])
        self.assertIn("lag_origin", node)
        # one extra minute is requested ahead of the window (and of its baseline day) so the first growth can be told
        self.assertEqual([c.args[1] for c in loader.call_args_list], [-W, -W - DAY_US])

    def test_active_minutes_come_with_the_growth_order_too(self):
        # the page prints "n / total minutes with executions" next to every statement; only the correlation order used to set n
        result, _loader = self.run_lag()
        rows = result["nodes"][0]["statements"]
        self.assertTrue(rows and all(row["active_minutes"] == sum(1 for value in row["runtime_us"] if value) for row in rows))
        self.assertGreater(rows[0]["active_minutes"], 0)

    def test_the_lag_origin_gets_the_replicas_own_events_and_the_other_replicas_peaks(self):
        seen = {}
        with patch("app.slowlog_impact_service.describe_lag",
                   lambda node, samples, **kw: seen.update(node=node, samples=samples, **kw) or {"episodes": []}):
            self.run_lag(ddl_lookup=lambda *_: [])
        self.assertEqual(seen["node"], "r")
        self.assertEqual([e["event_id"] for e in seen["events"]], ["v1"])
        self.assertEqual(seen["other_peaks"], {})
        self.assertEqual(seen["instance"], "x")
        self.assertEqual(seen["samples"][-1], (6 * W, 180.0))

    def test_unknown_primary_still_gives_the_lag_and_says_why_there_are_no_candidates(self):
        metadata, backend = self.fixture()
        loader = Mock(return_value=lag_points([0, 0, 0, 0, 60, 120, 180]))
        with self.assertLogs("app.slowlog_impact_service", "WARNING") as logs:
            result = query_resource_overlap(metadata, backend, {"source": "slowlog", "instance": "x", "metric": "replication_lag"},
                                            Settings(), loader, roles=lambda _i: {})
        self.assertTrue(any("primary node unknown" in line for line in logs.output))
        self.assertEqual(result["nodes"][0]["statements"], [])
        self.assertIn("lag_origin", result["nodes"][0])

    def test_no_replica_samples_is_its_own_status(self):
        metadata, backend = self.fixture()
        loader = Mock(return_value=lag_points([0, 0, 0, 0, 60, 120, 180], node="p"))          # only the primary answers: nothing to explain
        with self.assertLogs("app.slowlog_impact_service", "WARNING"):
            result = query_resource_overlap(metadata, backend, {"source": "slowlog", "instance": "x", "metric": "replication_lag"},
                                            Settings(), loader, roles=lambda _i: self.ROLES)
        self.assertEqual((result["status"], result["nodes"]), ("no_replica_lag_samples", []))


class LagPeerEvidenceTests(unittest.TestCase):
    """The evidence next to the lag card: the lagging node against its siblings, and what the primary wrote meanwhile."""

    ROLES = {"p": {"role": "primary", "zone": "a"}, "r": {"role": "secondary", "zone": "b"},
             "r2": {"role": "secondary", "zone": "b"}, "r3": {"role": "secondary", "zone": "a"}}
    MINUTES = 21
    LEVELS = [0] * 3 + [40 + 2 * i for i in range(14)] + [0] * 4                      # r lags 40-66 s for 14 minutes; r2 and r3 do not
    HEALTHY = dict(Cluster_ComSelect=5000.0, Cluster_SlowQueries=0.4, Cluster_ThreadsRunning=3.0, Cluster_CpuUsage=29.0, Cluster_IOPSUsage=40.0)
    SLOWER = dict(Cluster_ComSelect=2300.0, Cluster_SlowQueries=7.0, Cluster_ThreadsRunning=9.0, Cluster_CpuUsage=17.0, Cluster_IOPSUsage=29.0)

    def lag_loader(self, levels=None):
        levels = self.LEVELS if levels is None else levels
        everyone = lag_points(levels, "r") + lag_points([0] * self.MINUTES, "r2") + lag_points([0] * self.MINUTES, "r3")
        return Mock(side_effect=lambda settings, start, end, instance, node="", **o: everyone)

    def peer_loader(self, fail=()):
        def load(settings, start, end, instance, node="", metric_name=""):
            if metric_name in fail:
                raise RuntimeError("metric_api_error:500")
            return [dict(timestamp=i * 60000, nodeId=name, Average=(self.SLOWER if name == "r" else self.HEALTHY)[metric_name])
                    for name in ("r", "r2", "r3") for i in range(self.MINUTES)]
        return Mock(side_effect=load)

    def analyse(self, lag_loader=None, **kw):
        metadata, backend = LagMetricTests.fixture(self)
        query = {"source": "slowlog", "instance": "x", "metric": "replication_lag", "start_epoch_us": 0, "end_epoch_us": (self.MINUTES - 1) * W - 1}
        result = query_resource_overlap(metadata, backend, query, Settings(), lag_loader or self.lag_loader(), roles=lambda _i: self.ROLES, **kw)
        return {node["node_id"]: node for node in result["nodes"]}, result

    def test_a_node_that_lags_is_compared_with_its_siblings_and_the_others_are_not(self):
        peers = self.peer_loader()
        nodes, result = self.analyse(peer_loader=peers)
        health = nodes["r"]["lag_origin"]["peer_health"]
        self.assertEqual((health["status"], health["diverged"], health["peers"]), ("ok", True, 2))
        self.assertGreater(health["metrics"]["slow_per_1000_reads"]["ratio"], 20)
        self.assertEqual(nodes["r"]["lag_origin"]["episodes"][0]["node_local"], True)
        self.assertNotIn("peer_health", nodes["r2"]["lag_origin"])                  # no episode, nothing to explain
        self.assertEqual(sorted(c.kwargs["metric_name"] for c in peers.call_args_list), sorted(METRIC_NAMES.values()))
        self.assertTrue(all(c.args[3:5] == ("x", "") for c in peers.call_args_list))   # one call per metric, every node at once
        first = nodes["r"]["lag_origin"]["episodes"][0]["first_us"]
        self.assertTrue(all(c.args[1] == first - LOOKBACK_US for c in peers.call_args_list))   # looks back before the episode
        self.assertEqual(result["nodes"][0]["statements"][0]["sql_id"], "bulk-write")   # the ranking itself is untouched

    def test_without_a_loader_or_without_a_lag_episode_nothing_is_read(self):
        nodes, _ = self.analyse()
        self.assertTrue(all("peer_health" not in node["lag_origin"] for node in nodes.values()))
        peers = self.peer_loader()
        quiet, _ = self.analyse(lag_loader=self.lag_loader([0] * self.MINUTES), peer_loader=peers)
        peers.assert_not_called()
        self.assertTrue(all(not node["lag_origin"]["episodes"] for node in quiet.values()))

    def test_a_metric_that_cannot_be_read_is_logged_and_the_card_says_so_without_failing_the_analysis(self):
        with self.assertLogs("app.lag_peers", "WARNING"):
            nodes, result = self.analyse(peer_loader=self.peer_loader(fail=(METRIC_NAMES["slow"],)))
        self.assertEqual(nodes["r"]["lag_origin"]["peer_health"], {"status": "unavailable"})
        self.assertEqual(result["status"], "ok")
        self.assertTrue(nodes["r"]["statements"])
        # a metric that only decorates the table (CPU) leaves the verdict standing
        with self.assertLogs("app.lag_peers", "WARNING"):
            nodes, _ = self.analyse(peer_loader=self.peer_loader(fail=(METRIC_NAMES["cpu"],)))
        health = nodes["r"]["lag_origin"]["peer_health"]
        self.assertEqual((health["status"], health["diverged"]), ("ok", True))
        self.assertNotIn("cpu_pct", health["metrics"])

    def test_what_the_primary_wrote_is_attached_to_each_episode_once_per_window(self):
        asked = []
        table = {"database_name": "shop", "table_name": "t", "events": 10, "deletes": 6, "inserts": 4, "updates": 0}
        nodes, _ = self.analyse(writes_lookup=lambda instance, low, high: asked.append((instance, low, high)) or {"status": "ok", "tables": [table]})
        episode = nodes["r"]["lag_origin"]["episodes"][0]
        self.assertEqual(asked, [("x", episode["first_us"] - WRITES_LEAD_US, episode["last_us"])])
        self.assertEqual(episode["writes"]["tables"], [table])
        self.assertTrue(all("writes" not in e for n in ("r2", "r3") for e in nodes[n]["lag_origin"]["episodes"]))

    def test_a_failing_or_absent_row_store_is_logged_and_never_fails_the_analysis(self):
        def broken(*_args):
            raise RuntimeError("clickhouse down")
        with self.assertLogs("app.slowlog_impact_service", "WARNING") as logs:
            nodes, result = self.analyse(writes_lookup=broken)
        self.assertEqual(nodes["r"]["lag_origin"]["episodes"][0]["writes"], {"status": "unavailable"})
        self.assertTrue(any("write hotspots unavailable" in line for line in logs.output))
        self.assertEqual(result["status"], "ok")
        nodes, _ = self.analyse(writes_lookup=lambda *_: None)                                   # the server returns None without the store
        self.assertNotIn("writes", nodes["r"]["lag_origin"]["episodes"][0])


if __name__ == "__main__":
    unittest.main()
