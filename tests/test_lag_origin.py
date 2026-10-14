import unittest
from datetime import UTC, datetime

from app.lag_origin import MATCH_US, PERIOD_US, RELEASE_US, describe_lag, lag_episodes

BASE = int(datetime(2026, 10, 14, 3, 0, tzinfo=UTC).timestamp() * 1_000_000)
ONSET = BASE + (17 * 60 + 57) * 1_000_000          # the primary logged an ALTER at 03:17:57Z (11:17:57 Beijing)
RELEASE = BASE + (85 * 60 + 54) * 1_000_000        # the replica's applier let go at 04:25:54Z


def stuck_then_recovering(onset=ONSET, release=RELEASE, catch_up=0.5, minutes=130):
    """Cloud Monitor minute samples of a replica whose applier sat on one event from ``onset`` to ``release``.

    A sample stamped t describes the minute before it, so it carries the lag in the middle of that minute.
    """
    out = []
    peak = (release - onset) / 1e6
    for minute in range(1, minutes):
        end = BASE + minute * PERIOD_US
        mid = end - PERIOD_US // 2
        if mid < onset:
            lag = 0.0
        elif mid < release:
            lag = (mid - onset) / 1e6
        else:
            lag = max(0.0, peak - catch_up * (mid - release) / 1e6)
        out.append((end, lag))
    return out


def victim(event_id, start, end, account="root"):
    return dict(event_id=event_id, start_us=start, duration_ms=(end - start) // 1000, sql_id="prepare",
                fingerprint="p", database_name="shop", account=account)


class EpisodeTests(unittest.TestCase):
    def test_an_applier_stuck_on_one_event_is_found_with_the_time_of_that_event(self):
        episodes = lag_episodes(stuck_then_recovering())
        self.assertEqual(len(episodes), 1)
        episode = episodes[0]
        self.assertEqual(episode["kind"], "stall")
        self.assertAlmostEqual(episode["peak_s"], (RELEASE - ONSET) / 1e6, delta=60)
        self.assertGreaterEqual(episode["stall"]["minutes"], 60)
        self.assertLessEqual(abs(episode["stall"]["origin_us"] - ONSET), 5_000_000)   # the event it is stuck on
        self.assertLessEqual(abs(episode["stall"]["end_us"] - RELEASE), PERIOD_US)
        self.assertLessEqual(abs(episode["stall"]["release_us"] - RELEASE), RELEASE_US)   # within the match distance of the queued statements
        self.assertAlmostEqual(episode["growth_rate"], 1.0, delta=0.05)
        self.assertAlmostEqual(episode["recovery_rate"], 0.5, delta=0.05)

    def test_a_replica_that_is_only_too_slow_has_no_stall_and_a_moving_origin(self):
        samples = [(BASE + m * PERIOD_US, 18.0 * m if m <= 20 else max(0.0, 360 - 12.0 * (m - 20))) for m in range(1, 60)]
        episode = lag_episodes(samples)[0]
        self.assertEqual(episode["kind"], "behind")
        self.assertIsNone(episode["stall"])
        self.assertAlmostEqual(episode["growth_rate"], 0.3, delta=0.02)

    def test_a_short_blip_is_a_spike_and_below_the_floor_nothing_is_reported(self):
        blip = [(BASE + m * PERIOD_US, lag) for m, lag in enumerate([0, 0, 40, 55, 20, 0, 0], 1)]
        self.assertEqual(lag_episodes(blip)[0]["kind"], "spike")
        quiet = [(BASE + m * PERIOD_US, 12.0) for m in range(1, 30)]
        self.assertEqual(lag_episodes(quiet), [])

    def test_a_missing_minute_never_makes_a_stall(self):
        samples = [s for s in stuck_then_recovering() if s[0] % (2 * PERIOD_US) == 0 or s[0] < ONSET]
        # every other minute is gone: no two consecutive samples, so no slope can be taken
        self.assertTrue(all(e["stall"] is None for e in lag_episodes(samples)))

    def test_two_episodes_are_listed_separately(self):
        first = stuck_then_recovering(minutes=60)
        second = [(end + 200 * PERIOD_US, lag) for end, lag in stuck_then_recovering(minutes=60)]
        self.assertEqual(len(lag_episodes(first + second)), 2)


# A real incident (2026-10-14): one replica of a production cluster sat 4077 s on a replicated ALTER TABLE (logged by the
# primary at 11:17:57 Beijing = 03:17:57Z). These are Cloud Monitor's instance-level ReplicationDelay minutes (labelled with
# the minute start) from 03:16Z: the mean over five replicas, of which only that one lagged, times five. It is the noisy
# version of the node series: the other four add a little, and a burst of deletes at 04:23Z moves the tail.
REAL_MEAN_OF_FIVE = [
    0.01, 0.01, 6.5, 18.5, 30.5, 42.51, 54.5, 66.5, 78.5, 90.5, 102.5, 114.5, 126.5, 138.5, 150.5, 162.5, 174.5, 186.5, 198.5,
    210.5, 222.5, 234.5, 246.53, 258.5, 270.5, 282.5, 294.5, 306.5, 318.5, 330.5, 342.5, 354.5, 366.5, 378.51, 390.5, 402.5,
    414.59, 426.5, 438.51, 450.5, 462.5, 474.52, 486.51, 498.5, 510.5, 522.5, 534.5, 546.5, 558.5, 570.5, 582.5, 594.51,
    606.51, 618.54, 630.5, 642.5, 654.5, 666.5, 678.5, 690.5, 702.5, 714.5, 726.5, 738.5, 750.5, 762.6, 774.5, 795.84, 802.25,
    810.5, 813.36, 799.97, 785.2, 766.55, 745.87, 725.15, 706.21, 685.38, 665.69, 643.57, 619.27, 593.62, 568.39, 544.68,
    517.11, 490.57, 460.14, 429.67, 401.45, 367.52, 331.77, 303.6, 296.27, 284.76, 268.66, 249.81, 225.62, 202.67, 174.55,
    142.94, 114.4, 86.58, 57.95, 33.28, 6.7, 0.0, 0.0, 0.0, 0.0]


# The same incident, the lagging replica's own Cloud Monitor series (Cluster_DataDelay, one point a minute, stamped with the end
# of the minute) from 03:10Z to 05:10Z: 0 until the ALTER, then +60 s every minute for 67 minutes, then the catch-up.
REAL_NODE_SERIES = [
    0, 0, 0, 0, 0, 0, 0, 0, 0.083, 33.499, 93.5, 153.499, 213.5, 273.499, 333.5, 393.5, 453.5, 513.499, 573.499, 633.499,
    693.499, 753.5, 813.5, 873.5, 933.499, 993.5, 1053.499, 1113.5, 1173.499, 1233.499, 1293.5, 1353.5, 1413.499, 1473.5,
    1533.5, 1593.499, 1653.499, 1713.499, 1773.499, 1833.5, 1893.5, 1953.5, 2013.499, 2073.5, 2133.499, 2193.5, 2253.5,
    2313.5, 2373.499, 2433.5, 2493.5, 2553.5, 2613.5, 2673.499, 2733.499, 2793.5, 2853.499, 2913.5, 2973.499, 3033.499,
    3093.499, 3153.5, 3213.5, 3273.5, 3333.5, 3393.5, 3453.499, 3513.499, 3573.499, 3633.5, 3693.499, 3753.499, 3813.499,
    3873.5, 3933.499, 3993.5, 4053.483, 4066.033, 3998.733, 3924.549, 3831.1, 3727.533, 3624.233, 3529.333, 3425.233,
    3326.716, 3215.899, 3093.916, 2965.95, 2840.099, 2720.999, 2583.5, 2450.399, 2298.066, 2145.999, 2004.666, 1834.599,
    1655.966, 1516.433, 1480.866, 1422.399, 1342.116, 1246.983, 1126.216, 1011.333, 870.016, 712.299, 569.783, 430.416,
    287.466, 164.366, 31.883, 0, 0, 0, 0, 0, 0, 0, 0, 0
]
REAL_NODE_FIRST = int(datetime(2026, 10, 14, 3, 10, tzinfo=UTC).timestamp() * 1_000_000)


class RealNodeSeriesTests(unittest.TestCase):
    def test_the_recorded_series_puts_the_origin_on_the_alter_within_a_second_and_the_release_within_a_minute(self):
        samples = [(REAL_NODE_FIRST + index * PERIOD_US, value) for index, value in enumerate(REAL_NODE_SERIES)]
        episodes = lag_episodes(samples)
        self.assertEqual(len(episodes), 1)
        episode = episodes[0]
        self.assertEqual(episode["kind"], "stall")
        self.assertEqual(episode["peak_s"], 4066.03)
        self.assertAlmostEqual(episode["growth_rate"], 1.0, delta=0.01)
        self.assertEqual(episode["stall"]["minutes"], 67)
        self.assertLessEqual(abs(episode["stall"]["origin_us"] - ONSET), 1_000_000)     # the ALTER was logged at 03:17:57Z
        self.assertLessEqual(abs(episode["stall"]["release_us"] - RELEASE), 15_000_000)   # the queued statements ended at 04:25:54Z
        queued = [victim("q1", ONSET, RELEASE), victim("q2", ONSET + 40 * 60 * 1_000_000, RELEASE)]
        block = describe_lag("rn-lagging", samples, events=queued, other_peaks={"rn-other": 16.6}, instance="rm-x",
                             ddl_lookup=lambda *_: [dict(event_epoch_us=ONSET, database_name="", table_name="", sample_sql="ALTER TABLE shop.shop_jobs ADD INDEX i (a)")])
        shown = block["episodes"][0]
        self.assertEqual((shown["trigger"]["type"], abs(shown["trigger"]["offset_s"]) < 1.0), ("ddl", True))
        self.assertEqual((shown["queued"]["count"], shown["holders"]["count"], shown["node_local"]), (2, 0, True))


class RealIncidentTests(unittest.TestCase):
    def test_the_recorded_replica_series_gives_the_alter_as_the_origin_of_one_stall(self):
        first = BASE + 16 * PERIOD_US
        samples = [(first + index * PERIOD_US, value * 5) for index, value in enumerate(REAL_MEAN_OF_FIVE)]
        episodes = lag_episodes(samples)
        self.assertEqual(len(episodes), 1)
        stall = episodes[0]["stall"]
        self.assertEqual(episodes[0]["kind"], "stall")
        self.assertGreaterEqual(stall["minutes"], 55)
        self.assertLessEqual(abs(stall["origin_us"] - ONSET), 90_000_000)          # inside the DDL match distance
        block = describe_lag("rn-lagging", samples, events=[], other_peaks={"rn-other": 0.0}, instance="rm-x", ddl_lookup=lambda *_: [
            dict(event_epoch_us=ONSET, database_name="", table_name="",
                 sample_sql="ALTER TABLE shop.shop_jobs ADD INDEX idx_shop_created (shop_id, created_at), ALGORITHM=INPLACE, LOCK=NONE")])
        self.assertEqual(block["episodes"][0]["trigger"]["type"], "ddl")
        self.assertIn("idx_shop_created", block["episodes"][0]["trigger"]["sql"])
        # the climbing run ends at 04:23Z in this noisy series, but the lag peaks at 04:26Z: that is the release, and the
        # statements the primary's reader log shows ending at 04:25:54Z are matched to it
        self.assertLessEqual(abs(stall["release_us"] - RELEASE), RELEASE_US)
        held = [victim(f"q{i}", ONSET + i * 1_000_000, RELEASE) for i in range(3)]
        again = describe_lag("rn-lagging", samples, events=held, other_peaks={}, instance="rm-x", ddl_lookup=None)
        self.assertEqual(again["episodes"][0]["queued"]["count"], 3)


class OriginTests(unittest.TestCase):
    def describe(self, **kw):
        defaults = dict(events=[], other_peaks={"other": 0.4}, instance="rm-x")
        return describe_lag("rn-lagging", stuck_then_recovering(), **{**defaults, **kw})

    def test_the_ddl_at_the_origin_is_named_with_its_offset(self):
        calls = []

        def lookup(instance, low, high):
            calls.append((instance, low, high))
            return [dict(event_epoch_us=ONSET, database_name="", table_name="",
                         sample_sql="ALTER TABLE shop.shop_jobs ADD INDEX idx_shop_created (shop_id, created_at)"),
                    dict(event_epoch_us=ONSET - 30 * 60 * 1_000_000, database_name="", table_name="", sample_sql="far away")]
        block = self.describe(ddl_lookup=lookup)
        episode = block["episodes"][0]
        self.assertEqual(episode["trigger"]["type"], "ddl")
        self.assertIn("shop_jobs", episode["trigger"]["sql"])
        self.assertLessEqual(abs(episode["trigger"]["offset_s"]), 5)
        self.assertEqual(calls[0][0], "rm-x")
        self.assertLess(calls[0][1], ONSET)
        self.assertGreater(calls[0][2], ONSET)
        self.assertTrue(episode["node_local"])

    def test_without_a_ddl_at_the_origin_the_stall_is_reported_as_unexplained_not_guessed(self):
        far = [dict(event_epoch_us=ONSET + MATCH_US + 60_000_000, database_name="", table_name="", sample_sql="late")]
        episode = self.describe(ddl_lookup=lambda *_: far)["episodes"][0]
        self.assertEqual(episode["trigger"], {"type": "none"})

    def test_a_failing_ddl_lookup_leaves_the_rest_of_the_card_and_is_logged(self):
        def broken(*_):
            raise OSError("clickhouse down")
        with self.assertLogs("app.lag_origin", "WARNING"):
            episode = self.describe(ddl_lookup=broken)["episodes"][0]
        self.assertEqual(episode["trigger"], {"type": "unavailable"})
        self.assertEqual(episode["kind"], "stall")

    def test_statements_that_let_go_with_the_stall_are_queued_victims_or_holder_candidates(self):
        queued = [victim("q1", ONSET, RELEASE), victim("q2", ONSET + 21_000_000, RELEASE + 1_000_000, "suanfa"),
                  victim("q3", ONSET + 40 * 60 * 1_000_000, RELEASE - 1_000_000)]
        holder = victim("h1", ONSET - 30 * 60 * 1_000_000, RELEASE)
        unrelated = [victim("u1", ONSET + 5_000_000, ONSET + 3 * 60 * 1_000_000),          # ended long before the release
                     victim("u2", RELEASE - 10_000_000, RELEASE + 1_000_000)]              # too short to have been held
        block = self.describe(events=queued + [holder] + unrelated)
        episode = block["episodes"][0]
        self.assertEqual(episode["queued"]["count"], 3)
        self.assertEqual([s["event_id"] for s in episode["queued"]["samples"]], ["q1", "q2", "q3"])   # longest first
        self.assertEqual([s["event_id"] for s in episode["holders"]["samples"]], ["h1"])

    def test_no_holder_in_the_slow_log_is_visible_as_zero_holders(self):
        episode = self.describe(events=[victim("q1", ONSET, RELEASE)])["episodes"][0]
        self.assertEqual(episode["holders"], {"count": 0, "samples": []})

    def test_other_replicas_that_lagged_too_mean_the_cause_is_not_node_local(self):
        self.assertFalse(self.describe(other_peaks={"b": 31.0})["episodes"][0]["node_local"])
        self.assertIsNone(self.describe(other_peaks={})["episodes"][0]["node_local"])

    def test_a_series_without_an_episode_has_an_empty_block(self):
        quiet = [(BASE + m * PERIOD_US, 0.0) for m in range(1, 20)]
        self.assertEqual(describe_lag("n", quiet, events=[], other_peaks={}, instance="i")["episodes"], [])


if __name__ == "__main__":
    unittest.main()
