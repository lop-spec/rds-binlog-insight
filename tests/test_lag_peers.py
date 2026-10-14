import unittest
from unittest.mock import Mock

from app.lag_peers import (METRIC_NAMES, MIN_READ_MINUTES, PERIOD_US, load_peer_series, peer_health, series_from_points)

BASE = 1_790_000_000_000_000 // PERIOD_US * PERIOD_US


def stamp(minute):
    """The end of minute ``minute`` after BASE, which is how Cloud Monitor stamps a sample."""
    return BASE + minute * PERIOD_US


HEALTHY = dict(reads=5000.0, slow=0.4, threads=3.0, cpu=29.0, iops=40.0)       # a sibling on an ordinary afternoon
SLOWER = dict(reads=2300.0, slow=7.0, threads=9.0, cpu=17.0, iops=29.0)        # the node that fell behind: fewer reads, many more slow ones
REMOVED = dict(reads=6.0, slow=0.0, threads=3.0, cpu=12.0, iops=15.0)          # the proxy took it out of read routing


def build(minutes, profiles):
    """Series for ``minutes`` minutes: ``profiles[node](minute)`` is that node's values (a dict, or None for no sample)."""
    series = {key: {node: {} for node in profiles} for key in METRIC_NAMES}
    for node, profile in profiles.items():
        for minute in range(1, minutes + 1):
            values = profile(minute)
            for key, value in (values or {}).items():
                series[key][node][stamp(minute)] = value
    return series


def episode(first, last, local=True):
    return {"first_us": stamp(first), "last_us": stamp(last), "node_local": local, "kind": "behind"}


PEERS = ["p1", "p2", "p3", "p4"]


def siblings(profile=lambda minute: HEALTHY):
    return {name: profile for name in PEERS}


class PeerHealthTests(unittest.TestCase):
    def test_a_node_that_became_slower_is_called_out_with_the_time_it_diverged(self):
        series = build(160, {**siblings(), "n": lambda m: HEALTHY if m < 60 else SLOWER})
        out = peer_health("n", [episode(120, 150)], series, PEERS)
        self.assertEqual((out["status"], out["diverged"], out["peers"]), ("ok", True, 4))
        # the ratio is about the rate per 1000 reads: 7/2300 against 0.4/5000
        self.assertGreater(out["metrics"]["slow_per_1000_reads"]["ratio"], 20)
        self.assertAlmostEqual(out["metrics"]["slow_per_1000_reads"]["node"], 3.04, places=1)
        self.assertAlmostEqual(out["metrics"]["slow_per_1000_reads"]["peers"], 0.08, places=2)
        self.assertLess(out["metrics"]["cpu_pct"]["ratio"], 1)               # busier it is not: it is waiting
        self.assertLess(out["metrics"]["iops_pct"]["ratio"], 1)
        self.assertGreater(out["metrics"]["ms_per_read"]["ratio"], 3)
        # the divergence began at minute 60, well before the episode; the five-minute window may blur it by a few minutes
        self.assertLessEqual(abs(out["diverged_since_us"] - stamp(60)), 5 * PERIOD_US)
        self.assertEqual(out["period"], {"start_us": stamp(120), "end_us": stamp(150), "minutes": 31, "read_minutes": 31})

    def test_every_replica_lagging_together_is_not_blamed_on_the_node(self):
        # all five look alike (with a little noise): the primary wrote more than any of them could absorb
        noisy = lambda shift: (lambda m: dict(HEALTHY, slow=0.4 + 0.05 * ((m + shift) % 3), reads=5000.0 + 40 * ((m * shift) % 7)))
        series = build(160, {**{name: noisy(i) for i, name in enumerate(PEERS)}, "n": noisy(9)})
        out = peer_health("n", [episode(100, 150, local=False)], series, PEERS)
        self.assertEqual((out["status"], out["diverged"], out["diverged_since_us"]), ("ok", False, None))
        self.assertLess(out["metrics"]["slow_per_1000_reads"]["ratio"], 1.5)

    def test_minutes_without_reads_are_listed_and_left_out_of_the_ratios(self):
        def node(m):
            if 100 <= m < 120:
                return REMOVED                      # the proxy took the node out of routing for 20 minutes
            return SLOWER if m >= 60 else HEALTHY
        series = build(160, {**siblings(), "n": node})
        out = peer_health("n", [episode(90, 150)], series, PEERS)
        self.assertEqual([(r["start_us"], r["end_us"], r["minutes"]) for r in out["reads_removed"]], [(stamp(99), stamp(119), 20)])
        self.assertEqual(out["period"]["minutes"], 61)
        self.assertEqual(out["period"]["read_minutes"], 41)                  # 61 minutes of episode minus the 20 removed ones
        self.assertAlmostEqual(out["metrics"]["slow_per_1000_reads"]["node"], 3.04, places=1)   # not diluted by the removed minutes
        self.assertTrue(out["diverged"])
        self.assertLessEqual(abs(out["diverged_since_us"] - stamp(60)), 5 * PERIOD_US)   # a removal inside the run does not restart it

    def test_a_node_that_served_no_reads_in_the_episode_cannot_be_judged(self):
        series = build(160, {**siblings(), "n": lambda m: HEALTHY if m < 100 else REMOVED})
        out = peer_health("n", [episode(110, 150)], series, PEERS)
        self.assertEqual(out["status"], "no_reads")
        self.assertEqual(out["period"]["read_minutes"], 0)
        self.assertEqual([r["minutes"] for r in out["reads_removed"]], [51])          # minutes 100..150, up to the end of the episode
        self.assertNotIn("diverged", out)
        # a handful of minutes is not enough either
        few = build(160, {**siblings(), "n": lambda m: SLOWER if 110 <= m < 110 + MIN_READ_MINUTES - 1 else REMOVED})
        self.assertEqual(peer_health("n", [episode(110, 150)], few, PEERS)["status"], "no_reads")

    def test_a_few_slow_statements_are_not_a_difference_however_large_the_ratio(self):
        # 400 reads/s and 0.08 slow/s is 0.2 per 1000 against a spotless sibling, but only 48 slow statements in ten minutes
        quiet = dict(reads=400.0, slow=0.08, threads=3.0, cpu=5.0, iops=5.0)
        spotless = dict(HEALTHY, slow=0.0)
        series = build(60, {**siblings(lambda m: spotless), "n": lambda m: quiet})
        out = peer_health("n", [episode(40, 49)], series, PEERS)
        self.assertEqual((out["status"], out["diverged"]), ("ok", False))
        self.assertGreater(out["metrics"]["slow_per_1000_reads"]["ratio"], 3)

    def test_siblings_the_node_has_to_share_nothing_with_are_not_counted_and_a_missing_node_is_no_data(self):
        series = build(160, {**siblings(), "n": lambda m: SLOWER})
        self.assertEqual(peer_health("n", [episode(100, 150)], series, [])["status"], "no_peers")
        self.assertEqual(peer_health("n", [episode(100, 150)], series, ["n", "ghost"])["status"], "no_peers")   # itself and unknown nodes are not siblings
        self.assertEqual(peer_health("ghost", [episode(100, 150)], series, PEERS)["status"], "no_data")
        self.assertEqual(peer_health("n", [], series, PEERS)["status"], "no_episodes")
        self.assertEqual(peer_health("n", [episode(500, 520)], series, PEERS)["status"], "no_data")    # the series stop before the episode

    def test_missing_samples_are_neither_zero_nor_a_removal(self):
        # the node's slow-statement samples are missing for ten minutes in the middle: they drop out of the sums
        def node(m):
            if 100 <= m < 110:
                return {key: value for key, value in SLOWER.items() if key != "slow"}
            return SLOWER if m >= 60 else HEALTHY
        series = build(160, {**siblings(), "n": node})
        out = peer_health("n", [episode(90, 150)], series, PEERS)
        self.assertEqual(out["reads_removed"], [])
        self.assertAlmostEqual(out["metrics"]["slow_per_1000_reads"]["node"], 3.04, places=1)
        self.assertTrue(out["diverged"])
        # a minute without the node's read sample at all is unknown, not removed
        gap = build(160, {**siblings(), "n": lambda m: None if 100 <= m < 120 else SLOWER})
        self.assertEqual(peer_health("n", [episode(90, 150)], gap, PEERS)["reads_removed"], [])

    def test_a_divergence_that_ended_before_the_lag_has_no_start_to_report(self):
        series = build(160, {**siblings(), "n": lambda m: SLOWER if 20 <= m < 40 else HEALTHY})
        out = peer_health("n", [episode(25, 38)], series, PEERS)
        self.assertTrue(out["diverged"])
        # the episode sits inside the slow stretch, so the stretch started before it
        self.assertLessEqual(abs(out["diverged_since_us"] - stamp(20)), 5 * PERIOD_US)
        later = peer_health("n", [episode(120, 150)], series, PEERS)             # healthy again by then
        self.assertEqual((later["diverged"], later["diverged_since_us"]), (False, None))


class LoadingTests(unittest.TestCase):
    def test_points_become_per_node_minute_series_and_bad_points_are_skipped(self):
        points = [{"nodeId": "a", "timestamp": 60000, "Average": "1.5"}, {"nodeId": "a", "timestamp": 120000, "Average": 2},
                  {"nodeId": "b", "timestamp": 60000, "Average": 3}, {"timestamp": 60000, "Average": 9},
                  {"nodeId": "c", "Average": 1}, {"nodeId": "c", "timestamp": "x", "Average": 1}]
        self.assertEqual(series_from_points(points), {"a": {60_000_000: 1.5, 120_000_000: 2.0}, "b": {60_000_000: 3.0}})

    def test_one_call_per_metric_for_all_nodes_and_a_failed_metric_is_reported_not_raised(self):
        calls = []

        def loader(settings, start, end, instance, node="", metric_name=""):
            calls.append((start, end, instance, node, metric_name))
            if metric_name == METRIC_NAMES["cpu"]:
                raise RuntimeError("metric_api_error:403")
            return [{"nodeId": "a", "timestamp": 60000, "Average": 1}]

        with self.assertLogs("app.lag_peers", "WARNING") as logs:
            series, failed = load_peer_series(loader, object(), 5, 9, "rm-x")
        self.assertEqual(sorted(call[4] for call in calls), sorted(METRIC_NAMES.values()))
        self.assertTrue(all(call[:4] == (5, 9, "rm-x", "") for call in calls))          # no node filter: every node answers
        self.assertEqual(set(series), set(METRIC_NAMES) - {"cpu"})
        self.assertEqual(set(failed), {"cpu"})
        self.assertIn("Cluster_CpuUsage", logs.output[0])
        broken = Mock(side_effect=ValueError("bad"))
        with self.assertLogs("app.lag_peers", "WARNING"):
            self.assertEqual(load_peer_series(broken, object(), 5, 9, "rm-x")[0], {})


if __name__ == "__main__":
    unittest.main()
