import unittest

from unittest.mock import Mock

from app.window_align import (HOUR_US, MINUTE_US, adjusted_payload, align_binlog_query, align_to_intervals,
                              floor_to, ready_edge)

STEP = 5 * MINUTE_US
NOW = floor_to(1_790_000_100_000_000, STEP) + 2 * MINUTE_US  # deliberately off a 5-minute boundary


def ceil_to(value, step):
    return (value + step - 1) // step * step


class AlignTests(unittest.TestCase):
    def align(self, start, end, intervals, **kw):
        return align_to_intervals(start, end, intervals, now_us=NOW, **kw)

    def test_a_covered_window_is_left_alone(self):
        start = NOW - 3 * HOUR_US
        self.assertIsNone(self.align(start, start + HOUR_US, [(start - HOUR_US, NOW)]))

    def test_missing_tail_is_cut_back_not_moved(self):
        start = NOW - 2 * HOUR_US
        edge = NOW - 12 * MINUTE_US  # indexed up to here
        found = self.align(start, NOW, [(start - HOUR_US, edge)])
        self.assertEqual(found, {"kind": "truncated", "start_us": start, "end_us": floor_to(edge, STEP)})

    def test_truncation_never_cuts_past_now(self):
        start = NOW - 2 * HOUR_US
        found = self.align(start, NOW + HOUR_US, [(start - HOUR_US, NOW + 30 * MINUTE_US)])
        self.assertEqual(found, {"kind": "truncated", "start_us": start, "end_us": floor_to(NOW, STEP)})

    def test_a_prefix_shorter_than_one_step_falls_back_to_a_covered_window(self):
        start = NOW - 30 * MINUTE_US
        covered = [(start - 5 * HOUR_US, start + 2 * MINUTE_US)]  # only 2 minutes of the window survive
        found = self.align(start, NOW, covered)
        self.assertEqual(found["kind"], "shifted")
        self.assertEqual(found["end_us"] - found["start_us"], NOW - start)
        self.assertLessEqual(found["end_us"], start + 2 * MINUTE_US)
        self.assertEqual(found["start_us"] % STEP, 0)

    def test_a_hole_at_the_start_shifts_to_the_nearer_side(self):
        start = NOW - 4 * HOUR_US
        hole = (start - 10 * MINUTE_US, start + 20 * MINUTE_US)
        found = self.align(start, start + 30 * MINUTE_US, [(start - 3 * HOUR_US, hole[0]), (hole[1], NOW)])
        self.assertEqual(found, {"kind": "shifted", "start_us": ceil_to(hole[1], STEP),
                                 "end_us": ceil_to(hole[1], STEP) + 30 * MINUTE_US})

    def test_equal_distance_prefers_the_earlier_window(self):
        start = floor_to(NOW, STEP) - 4 * HOUR_US
        covered = [(start - 35 * MINUTE_US, start - 5 * MINUTE_US), (start + 35 * MINUTE_US, start + 65 * MINUTE_US)]
        found = self.align(start, start + 30 * MINUTE_US, covered)
        self.assertEqual(found, {"kind": "shifted", "start_us": start - 35 * MINUTE_US, "end_us": start - 5 * MINUTE_US})

    def test_nothing_covered_within_reach_is_reported_not_guessed(self):
        start = NOW - 20 * HOUR_US
        self.assertEqual(self.align(start, start + HOUR_US, [(NOW - 2 * HOUR_US, NOW)]), {"kind": "none"})
        self.assertEqual(self.align(start, start + HOUR_US, []), {"kind": "none"})

    def test_candidates_stay_above_the_retention_floor_and_below_now(self):
        covered = [(NOW - 40 * MINUTE_US, NOW + HOUR_US)]
        self.assertIsNone(self.align(NOW - 30 * MINUTE_US, NOW, covered))
        found = self.align(NOW - 120 * MINUTE_US, NOW - 90 * MINUTE_US, covered, floor_us=NOW - 40 * MINUTE_US)
        self.assertEqual(found["kind"], "shifted")
        self.assertGreaterEqual(found["start_us"], NOW - 40 * MINUTE_US)
        self.assertLessEqual(found["end_us"], NOW)
        below = self.align(NOW - 120 * MINUTE_US, NOW - 90 * MINUTE_US, covered, floor_us=NOW)
        self.assertEqual(below, {"kind": "none"})

    def test_ready_edge_ignores_the_future(self):
        self.assertEqual(ready_edge([(0, 10), (20, 30)], 25), 10)
        self.assertEqual(ready_edge([(0, 10), (20, 30)], 99), 30)
        self.assertIsNone(ready_edge([], 99))

    def test_the_two_repairs_report_one_shape(self):
        self.assertEqual(adjusted_payload("truncated", "incomplete_index", 100, 900, 100, 600),
                         {"kind": "truncated", "reason": "incomplete_index", "requested_start_us": 100,
                          "requested_end_us": 900, "truncated_us": 300})
        self.assertEqual(adjusted_payload("shifted", "incomplete_index", 100, 900, 40, 840),
                         {"kind": "shifted", "reason": "incomplete_index", "requested_start_us": 100,
                          "requested_end_us": 900, "shift_us": -60})


class BinlogQueryTests(unittest.TestCase):
    """The analytics endpoint hands the Binlog window to the row store's coverage and gets back a repaired one."""

    def rows(self, intervals):
        rows = Mock()
        rows.coverage.return_value = {"intervals": [list(i) for i in intervals]}
        return rows

    def test_covered_window_is_untouched_and_reports_the_ready_edge(self):
        start = NOW - 3 * HOUR_US
        query = {"instance": "rm-x", "start_epoch_us": start, "end_epoch_us": start + HOUR_US}
        adjusted, edge = align_binlog_query(self.rows([(start - HOUR_US, NOW - 8 * MINUTE_US)]), query, 60, now_us=NOW)
        self.assertEqual((adjusted, edge), (None, NOW - 8 * MINUTE_US))
        self.assertEqual((query["start_epoch_us"], query["end_epoch_us"]), (start, start + HOUR_US))

    def test_missing_tail_updates_the_query_and_is_reported(self):
        start = NOW - 2 * HOUR_US
        edge = NOW - 12 * MINUTE_US
        rows = self.rows([(start - HOUR_US, edge)])
        query = {"instance": "rm-x", "start_epoch_us": start, "end_epoch_us": NOW}
        with self.assertLogs("app.window_align", "WARNING") as logs:
            adjusted, ready = align_binlog_query(rows, query, 60, now_us=NOW)
        cut = floor_to(edge, STEP)
        self.assertEqual(adjusted, adjusted_payload("truncated", "incomplete_binlog_coverage", start, NOW, start, cut))
        self.assertEqual(ready, edge)
        self.assertEqual((query["start_epoch_us"], query["end_epoch_us"]), (start, cut))
        self.assertIn("BINLOG_WINDOW_ALIGNED", logs.output[0])
        # coverage is read once, around the window, never beyond now or the retention floor
        low, high = rows.coverage.call_args.args[1:3]
        self.assertGreaterEqual(low, NOW - 60 * 86_400_000_000)
        self.assertLessEqual(high, NOW)

    def test_window_without_an_instance_or_row_store_runs_as_asked(self):
        rows = self.rows([])
        self.assertEqual(align_binlog_query(rows, {"start_epoch_us": 1, "end_epoch_us": 2}, 60, now_us=NOW), (None, None))
        rows.coverage.assert_not_called()
        self.assertEqual(align_binlog_query(None, {"instance": "rm-x"}, 60, now_us=NOW), (None, None))

    def test_unreadable_coverage_is_logged_and_the_window_is_left_alone(self):
        rows = Mock()
        rows.coverage.side_effect = RuntimeError("clickhouse down")
        query = {"instance": "rm-x", "start_epoch_us": NOW - HOUR_US, "end_epoch_us": NOW}
        with self.assertLogs("app.window_align", "WARNING") as logs:
            self.assertEqual(align_binlog_query(rows, query, 60, now_us=NOW), (None, None))
        self.assertIn("BINLOG_WINDOW_ALIGN_SKIPPED", logs.output[0])
        self.assertEqual((query["start_epoch_us"], query["end_epoch_us"]), (NOW - HOUR_US, NOW))

    def test_nothing_covered_within_reach_is_logged_and_the_window_is_left_alone(self):
        start = NOW - 20 * HOUR_US
        query = {"instance": "rm-x", "start_epoch_us": start, "end_epoch_us": start + HOUR_US}
        with self.assertLogs("app.window_align", "WARNING") as logs:
            adjusted, edge = align_binlog_query(self.rows([(NOW - 2 * HOUR_US, NOW - 8 * MINUTE_US)]), query, 60, now_us=NOW)
        self.assertEqual((adjusted, edge), (None, NOW - 8 * MINUTE_US))
        self.assertIn("BINLOG_WINDOW_ALIGN_NONE", logs.output[0])
        self.assertEqual((query["start_epoch_us"], query["end_epoch_us"]), (start, start + HOUR_US))


if __name__ == "__main__":
    unittest.main()
