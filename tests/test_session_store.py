"""History of abnormal sessions: merged across snapshots, bounded, and honest about gaps in collection."""
import tempfile
import unittest
from pathlib import Path

from app.session_store import MAX_HISTORY_ROWS, SessionStore
from app.session_view import build_session_view

SEC = 1_000_000
T0 = 1_790_000_000 * SEC
MDL = "Waiting for table metadata lock"


def item(session_id, command="Sleep", seconds=0, user="app", client="10.0.0.1", database="shop", state="", sql="", trx=None, trx_seconds=0):
    row = dict(SessionId=session_id, User=user, Client=client, DbName=database, Command=command, Time=seconds, State=state, SqlText=sql)
    if trx is not None:
        row.update(TrxId=trx, TrxDuration=trx_seconds)
    return row


def view_at(ts_us, *sessions):
    data = {"IsFinish": True, "SessionData": {"SessionList": list(sessions), "TotalSessionCount": len(sessions) + 100, "TimeStamp": ts_us // 1000}}
    return build_session_view(data, ts_us)


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = SessionStore(Path(self.tmp.name) / "sessions.sqlite3")

    def test_one_session_abnormal_for_an_hour_is_one_row_with_its_span(self):
        for minute in range(0, 61):
            ts = T0 + minute * 60 * SEC
            self.store.record("rm-x", "rn-1", view_at(ts, item(312, "Sleep", 11 + minute * 60, trx="9", trx_seconds=600 + minute * 60, sql="")))
        history = self.store.history("rm-x", "rn-1", T0, T0 + 3600 * SEC)
        self.assertEqual(len(history["sessions"]), 1)
        row = history["sessions"][0]
        self.assertEqual((row["session_id"], row["first_us"], row["last_us"], row["seen"]), (312, T0, T0 + 3600 * SEC, 61))
        self.assertEqual((row["max_seconds"], row["max_trx_seconds"], row["kinds"]), (11 + 3600, 600 + 3600, ["long_trx", "idle_trx"]))
        self.assertEqual(history["coverage"][0]["snapshots"], 61)

    def test_a_long_gap_or_another_owner_is_another_row(self):
        self.store.record("rm-x", "rn-1", view_at(T0, item(5, "Sleep", 60, trx="1", trx_seconds=60)))
        self.store.record("rm-x", "rn-1", view_at(T0 + 3600 * SEC, item(5, "Sleep", 60, trx="2", trx_seconds=60)))                  # an hour later
        self.store.record("rm-x", "rn-1", view_at(T0 + 3600 * SEC + 60 * SEC, item(5, "Sleep", 60, user="etl", trx="3", trx_seconds=60)))   # same id, other user
        self.store.record("rm-x", "rn-2", view_at(T0, item(5, "Sleep", 60, trx="1", trx_seconds=60)))                                 # same id, other node
        rows = self.store.history("rm-x", "", T0 - SEC, T0 + 7200 * SEC)["sessions"]
        self.assertEqual(len(rows), 4)

    def test_kinds_join_the_state_and_statement_follow_the_latest_and_a_holder_stays_a_holder(self):
        self.store.record("rm-x", "rn-1", view_at(T0, item(7, "Sleep", 15, trx="4", trx_seconds=15)))
        self.store.record("rm-x", "rn-1", view_at(T0 + 60 * SEC, item(7, "Query", 70, state=MDL, sql="SELECT * FROM t", trx="4", trx_seconds=75)))
        row = self.store.history("rm-x", "rn-1", T0, T0 + 60 * SEC)["sessions"][0]
        self.assertEqual((row["command"], row["state"], row["sql"]), ("Query", MDL, "SELECT * FROM t"))
        self.assertEqual(row["kinds"], ["long_running", "lock_wait", "long_trx", "idle_trx"])
        holder_view = view_at(T0 + 120 * SEC, item(1, "Query", 100, state=MDL, sql="ALTER TABLE t ADD INDEX i (a)"),
                              item(2, "Sleep", 3, trx="8", trx_seconds=500))
        self.store.record("rm-x", "rn-1", holder_view)
        later = view_at(T0 + 180 * SEC, item(2, "Sleep", 63, trx="8", trx_seconds=560))
        self.store.record("rm-x", "rn-1", later)
        holders = [r for r in self.store.history("rm-x", "rn-1", T0 + 120 * SEC, T0 + 180 * SEC)["sessions"] if r["session_id"] == 2]
        self.assertEqual((len(holders), holders[0]["holder"], holders[0]["holder_basis"], holders[0]["seen"]), (1, True, "transaction", 2))

    def test_the_summary_row_counts_every_session_but_only_abnormal_ones_are_stored(self):
        written = self.store.record("rm-x", "rn-1", view_at(T0, item(1, "Sleep", 3), item(2, "Query", 1, sql="SELECT 1"), item(3, "Query", 40, sql="SELECT slow")))
        self.assertEqual(written, 1)
        stats = self.store.stats()
        self.assertEqual((stats["snapshots"], stats["abnormal_sessions"]), (1, 1))
        coverage = self.store.history("rm-x", "rn-1", T0, T0)["coverage"][0]
        self.assertEqual((coverage["snapshots"], coverage["most_abnormal"]), (1, 1))

    def test_history_overlap_node_filter_order_and_truncation(self):
        self.store.record("rm-x", "rn-1", view_at(T0, item(1, "Query", 99, sql="a")))
        self.store.record("rm-x", "rn-2", view_at(T0 + 600 * SEC, item(2, "Query", 99, sql="b")))
        self.store.record("rm-x", "rn-1", view_at(T0 + 1200 * SEC, item(3, "Query", 99, sql="c")))
        every = self.store.history("rm-x", "", T0, T0 + 1200 * SEC)["sessions"]
        self.assertEqual([row["session_id"] for row in every], [3, 2, 1])                          # newest first
        self.assertEqual([row["session_id"] for row in self.store.history("rm-x", "rn-1", T0, T0 + 1200 * SEC)["sessions"]], [3, 1])
        self.assertEqual([row["session_id"] for row in self.store.history("rm-x", "", T0 + 300 * SEC, T0 + 900 * SEC)["sessions"]], [2])   # only the one seen then
        self.assertEqual(self.store.history("rm-y", "", T0, T0 + 1200 * SEC)["sessions"], [])        # another instance
        limited = self.store.history("rm-x", "", T0, T0 + 1200 * SEC, limit=2)
        self.assertEqual((len(limited["sessions"]), limited["truncated"]), (2, True))
        self.assertEqual(self.store.history("rm-x", "", T0, T0 + 1200 * SEC, limit=10_000)["truncated"], False)
        self.assertLessEqual(MAX_HISTORY_ROWS, 500)

    def test_lock_holder_candidates_come_first_whatever_their_age(self):
        self.store.record("rm-x", "rn-1", view_at(T0, item(1, "Query", 100, state=MDL, sql="ALTER TABLE t ADD INDEX i (a)"), item(2, "Sleep", 3, trx="8", trx_seconds=500)))
        self.store.record("rm-x", "rn-1", view_at(T0 + 600 * SEC, item(3, "Query", 99, sql="a later slow query")))
        rows = self.store.history("rm-x", "rn-1", T0, T0 + 600 * SEC)["sessions"]
        self.assertEqual([(row["session_id"], row["holder"]) for row in rows], [(2, True), (3, False), (1, False)])

    def test_a_window_without_snapshots_says_so_instead_of_looking_quiet(self):
        history = self.store.history("rm-x", "rn-1", T0, T0 + 3600 * SEC)
        self.assertEqual((history["sessions"], history["coverage"]), ([], []))

    def test_purge_forgets_what_was_last_seen_before_the_cutoff(self):
        self.store.record("rm-x", "rn-1", view_at(T0, item(1, "Query", 99, sql="old")))
        self.store.record("rm-x", "rn-1", view_at(T0 + 100 * 86400 * SEC, item(2, "Query", 99, sql="new")))
        removed = self.store.purge(T0 + 50 * 86400 * SEC)
        self.assertEqual(removed, 2)                                   # one abnormal row and one summary row
        self.assertEqual([row["session_id"] for row in self.store.history("rm-x", "", 0, T0 + 200 * 86400 * SEC)["sessions"]], [2])

    def test_a_session_without_an_id_is_not_stored(self):
        written = self.store.record("rm-x", "rn-1", view_at(T0, {"Command": "Query", "Time": 99, "State": "x", "User": "u"}))
        self.assertEqual(written, 0)

    def test_the_same_snapshot_twice_changes_nothing_but_the_seen_count(self):
        snapshot = view_at(T0, item(5, "Sleep", 60, trx="1", trx_seconds=60))
        self.store.record("rm-x", "rn-1", snapshot)
        self.store.record("rm-x", "rn-1", snapshot)
        history = self.store.history("rm-x", "rn-1", T0, T0)
        self.assertEqual(len(history["sessions"]), 1)
        self.assertEqual(history["coverage"][0]["snapshots"], 1)

    def test_a_reopened_store_keeps_its_rows(self):
        self.store.record("rm-x", "rn-1", view_at(T0, item(1, "Query", 99, sql="kept")))
        again = SessionStore(self.store.path)
        self.assertEqual(again.history("rm-x", "", T0, T0)["sessions"][0]["sql"], "kept")


if __name__ == "__main__":
    unittest.main()
