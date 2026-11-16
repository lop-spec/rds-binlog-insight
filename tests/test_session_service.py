"""The session management endpoints without the HTTP server: who may be asked, partial answers, history windows, KILL text."""
import tempfile
import unittest
from pathlib import Path

from app.live_sessions import LiveSessionsError
from app.session_collector import DEFAULT_CONFIG, SessionCollector
from app.session_service import MAX_HISTORY_DAYS, SessionRequestError, SessionService
from app.session_store import SessionStore

SEC = 1_000_000
T0 = 1_790_000_000 * SEC
ROLES = {"rn-p": {"role": "primary", "zone": "cn-a"}, "rn-b": {"role": "secondary", "zone": "cn-b"}, "rn-a": {"role": "secondary", "zone": "cn-a"}}


def das(*sessions):
    return {"IsFinish": True, "SessionData": {"SessionList": list(sessions), "TotalSessionCount": len(sessions), "TimeStamp": T0 // 1000}}


def idle_trx(session_id, seconds=60):
    return dict(SessionId=session_id, User="app", Client="10.0.0.1", DbName="shop", Command="Sleep", Time=seconds, State="", SqlText="",
                TrxId="9", TrxDuration=seconds)


class FakeLive:
    def __init__(self, answers):
        self.answers, self.calls = answers, []

    def view(self, instance, node, build, source="auto"):
        self.calls.append((instance, node))
        self.sources = getattr(self, "sources", []) + [source]
        answer = self.answers[instance, node]
        if isinstance(answer, BaseException):
            raise answer
        return {**build(answer, 5), "instance_id": instance, "node_id": node, "cached": False}

    def direct_status(self):
        return {"enabled": False, "instances": [], "fallbacks": []}


class ServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = SessionStore(Path(self.tmp.name) / "s.sqlite3")
        self.answers = {("rm-x", "rn-p"): das(idle_trx(1)), ("rm-x", "rn-a"): das(), ("rm-x", "rn-b"): das(idle_trx(2, 90))}
        self.live = FakeLive(self.answers)
        pairs = [("rm-x", "rn-b"), ("rm-x", "rn-p"), ("rm-x", "rn-a"), ("rm-other", "rn-z"), ("rm-x", "")]
        self.collector = SessionCollector(self.live, self.store, config_loader=lambda: dict(DEFAULT_CONFIG), nodes=lambda: pairs,
                                          default_instances=lambda: ["rm-x"], wall_us=lambda: T0)
        self.service = SessionService(self.live, self.store, self.collector, nodes=lambda: pairs, roles=lambda instance: ROLES if instance == "rm-x" else {},
                                      wall_us=lambda: T0 + 60 * SEC)

    def test_every_collected_node_is_read_primary_first_with_its_role_and_zone(self):
        result = self.service.overview("rm-x", "", {"blocking_s": 30, "trx_s": 30, "idle_trx_s": 10})
        self.assertEqual([(n["node_id"], n["role"], n["zone"], n["status"]) for n in result["nodes"]],
                         [("rn-p", "primary", "cn-a", "ok"), ("rn-a", "secondary", "cn-a", "ok"), ("rn-b", "secondary", "cn-b", "ok")])
        self.assertEqual(sorted(self.live.calls), [("rm-x", "rn-a"), ("rm-x", "rn-b"), ("rm-x", "rn-p")])
        self.assertFalse(result["partial"])
        self.assertEqual(result["nodes"][0]["overview"]["abnormal"], 1)
        self.assertEqual(result["generated_us"], T0 + 60 * SEC)

    def test_thresholds_reach_the_classification(self):
        loose = self.service.overview("rm-x", "rn-b", {"blocking_s": 30, "trx_s": 600, "idle_trx_s": 600})
        self.assertEqual(loose["nodes"][0]["overview"]["abnormal"], 0)
        self.assertEqual(loose["nodes"][0]["thresholds"]["idle_trx_s"], 600)

    def test_one_node_failing_leaves_the_others_and_says_why(self):
        self.answers["rm-x", "rn-a"] = LiveSessionsError("das_throttled", "Throttling.User")
        result = self.service.overview("rm-x", "", {"blocking_s": 30, "trx_s": 30, "idle_trx_s": 10})
        self.assertTrue(result["partial"])
        failed = next(n for n in result["nodes"] if n["node_id"] == "rn-a")
        self.assertEqual((failed["status"], failed["code"], failed["role"]), ("error", "DAS_THROTTLED", "secondary"))
        self.assertEqual([n["status"] for n in result["nodes"] if n["node_id"] != "rn-a"], ["ok", "ok"])

    def test_only_collected_instances_and_nodes_can_be_asked(self):
        for instance, node, code in (("rm-nope", "", "SESSIONS_UNKNOWN_INSTANCE"), ("rm-x", "rn-z", "SESSIONS_UNKNOWN_NODE"), ("rm-other", "rn-p", "SESSIONS_UNKNOWN_NODE")):
            with self.assertRaises(SessionRequestError) as caught:
                self.service.overview(instance, node, {"blocking_s": 30, "trx_s": 30, "idle_trx_s": 10})
            self.assertEqual(caught.exception.code, code)
        self.assertEqual(self.live.calls, [])      # nothing was spent on a node nobody collects

    def test_a_broken_role_lookup_only_drops_the_labels(self):
        service = SessionService(self.live, self.store, self.collector, nodes=lambda: [("rm-x", "rn-p")], roles=lambda instance: 1 / 0)
        with self.assertLogs("app.session_service", "WARNING"):
            result = service.overview("rm-x", "", {"blocking_s": 30, "trx_s": 30, "idle_trx_s": 10})
        self.assertEqual((result["nodes"][0]["status"], result["nodes"][0]["role"], result["nodes"][0]["zone"]), ("ok", "", ""))

    def test_history_returns_the_window_with_node_labels_and_the_retention_state(self):
        self.collector.read_one("rm-x", "rn-b", dict(DEFAULT_CONFIG))
        history = self.service.history("rm-x", "", T0 - 60 * SEC, T0 + 60 * SEC, 100)
        self.assertEqual([(row["node_id"], row["session_id"]) for row in history["sessions"]], [("rn-b", 2)])
        self.assertEqual([n["node_id"] for n in history["nodes"]], ["rn-a", "rn-b", "rn-p"])
        self.assertTrue(history["retention"]["enabled"])
        self.assertEqual(history["coverage"][0]["snapshots"], 1)
        self.assertEqual(self.service.history("rm-x", "rn-p", T0 - 60 * SEC, T0 + 60 * SEC, 100)["sessions"], [])

    def test_history_windows_are_bounded_and_nodes_checked(self):
        for start, end in ((T0, T0), (T0 + 1, T0), (T0, T0 + (MAX_HISTORY_DAYS * 86400 + 1) * SEC)):
            with self.assertRaises(SessionRequestError) as caught:
                self.service.history("rm-x", "", start, end, 10)
            self.assertEqual(caught.exception.code, "SESSIONS_INVALID_WINDOW")
        with self.assertRaises(SessionRequestError):
            self.service.history("rm-x", "rn-z", T0, T0 + SEC, 10)

    def payload(self, **extra):
        rows = [dict(session_id=2, user="app", client="10.0.0.1", database="shop", command="Sleep", seconds=90, trx_seconds=90, kinds=["idle_trx"])]
        return {"instance": "rm-x", "node_id": "rn-b", "mode": "connection", "snapshot_us": T0, "sessions": rows, **extra}

    def test_the_source_reaches_the_reader_and_the_status_says_how_sessions_are_read(self):
        self.service.overview("rm-x", "rn-b", {"blocking_s": 30, "trx_s": 30, "idle_trx_s": 10})
        self.service.overview("rm-x", "rn-b", {"blocking_s": 30, "trx_s": 30, "idle_trx_s": 10}, "das")
        self.assertEqual(self.live.sources, ["auto", "das"])
        self.assertEqual(self.service.status()["direct"], {"enabled": False, "instances": [], "fallbacks": []})

    def test_kill_statements_are_written_never_run_and_leave_a_log_line(self):
        self.service._wall_us = lambda: T0+10*SEC
        with self.assertLogs("app.session_service", "INFO") as logs:
            out = self.service.kill_statements(self.payload())
        self.assertEqual(out["statements"], ["/*FORCE_NODE='rn-b'*/ KILL 2;"])
        self.assertEqual(out["text"], "/*FORCE_NODE='rn-b'*/ KILL 2;\n")             # the KILL and nothing else
        self.assertEqual([(node["node_id"], node["role"], node["hint"], node["snapshot_us"]) for node in out["nodes"]], [("rn-b", "secondary", "FORCE_NODE='rn-b'", T0)])
        self.assertIn("nothing was executed", logs.output[0])
        self.assertIn("nodes=rn-b mode=connection sessions=1", logs.output[0])
        self.assertEqual(self.live.calls, [('rm-x', 'rn-b')])  # one authoritative read; no KILL execution

    def test_a_lock_waiters_field_from_an_older_page_is_ignored(self):
        self.service._wall_us = lambda: T0+10*SEC
        for value in (3, 0, -1, True, "2", None):
            out = self.service.kill_statements(self.payload(lock_waiters=value))
            self.assertEqual(out["text"], "/*FORCE_NODE='rn-b'*/ KILL 2;\n")

    def test_a_snapshot_the_node_has_moved_past_still_works_when_the_sessions_are_the_same_connections(self):
        # the node was read again after the page's snapshot (a database read is a few milliseconds old, not 10 s): same sessions, newer time
        self.service._wall_us = lambda: T0+25*SEC
        self.answers["rm-x", "rn-b"]["SessionData"]["TimeStamp"] = (T0 + 20 * SEC) // 1000
        out = self.service.kill_statements(self.payload())
        self.assertEqual(out["statements"], ["/*FORCE_NODE='rn-b'*/ KILL 2;"])

    def test_a_session_that_is_gone_or_became_another_connection_is_not_killed(self):
        self.service._wall_us = lambda: T0+10*SEC
        self.answers["rm-x", "rn-b"] = das(idle_trx(3, 90))
        with self.assertRaises(SessionRequestError) as caught:
            self.service.kill_statements(self.payload())
        self.assertEqual(caught.exception.code, "SESSIONS_CHANGED")
        other_user = dict(idle_trx(2, 90), User="someone-else")
        other_client = dict(idle_trx(2, 90), Client="10.9.9.9")
        for changed in (other_user, other_client):
            self.answers["rm-x", "rn-b"] = das(changed)
            with self.assertRaises(SessionRequestError) as caught:
                self.service.kill_statements(self.payload())
            self.assertEqual(caught.exception.code, "SESSIONS_CHANGED")
            self.assertIn("2", str(caught.exception))

    def test_a_protected_session_is_skipped_even_when_the_page_called_it_something_else(self):
        # the page says "app", the node says a platform account: no statement, and it is reported as skipped, not as a changed session
        self.service._wall_us = lambda: T0+10*SEC
        platform = dict(idle_trx(2, 90), User="rdsinternal")
        self.answers["rm-x", "rn-b"] = das(platform)
        out = self.service.kill_statements(self.payload())
        self.assertEqual((out["statements"], out["text"], out["session_ids"]), ([], "", []))
        self.assertEqual([row["session_id"] for row in out["skipped"]], [2])

    def test_a_page_snapshot_older_than_thirty_seconds_or_from_the_future_is_refused(self):
        self.service._wall_us = lambda: T0+31*SEC
        with self.assertRaises(SessionRequestError) as caught:
            self.service.kill_statements(self.payload())
        self.assertEqual(caught.exception.code, "SESSIONS_STALE_SNAPSHOT")
        self.service._wall_us = lambda: T0-6*SEC
        with self.assertRaises(SessionRequestError):
            self.service.kill_statements(self.payload())
        self.assertEqual(self.live.calls, [])                                  # refused before the node is asked

    def test_sessions_on_several_nodes_are_one_request_and_one_text_of_kills_primary_first(self):
        self.service._wall_us = lambda: T0+10*SEC
        self.answers['rm-x','rn-b'] = das(idle_trx(2),idle_trx(3))
        self.answers['rm-x','rn-p'] = das(idle_trx(2))
        self.answers['rm-x','rn-p']['SessionData']['TimeStamp'] = (T0+SEC)//1000
        rows = lambda *ids: [dict(session_id=i, user="app", client="10.0.0.1", database="shop", command="Sleep", seconds=90) for i in ids]
        out = self.service.kill_statements({"instance": "rm-x", "mode": "query", "nodes": [
            {"node_id": "rn-b", "snapshot_us": T0, "lock_waiters": 1, "sessions": rows(2, 3)},
            {"node_id": "rn-p", "snapshot_us": T0 + SEC, "sessions": rows(2)}]})
        self.assertEqual(out["text"].splitlines(), ["/*FORCE_NODE='rn-p'*/ KILL QUERY 2;", "/*FORCE_NODE='rn-b'*/ KILL QUERY 2;", "/*FORCE_NODE='rn-b'*/ KILL QUERY 3;"])
        self.assertEqual(out["statements"], out["text"].splitlines())
        self.assertEqual(out["session_ids"], [2, 2, 3])                                # the same id on two nodes is two sessions
        self.assertEqual([(node["node_id"], node["role"], node["snapshot_us"], node["session_ids"]) for node in out["nodes"]],
                         [("rn-p", "primary", T0 + SEC, [2]), ("rn-b", "secondary", T0, [2, 3])])
        self.assertEqual((out["mode"], out["generated_us"], out["skipped"]), ("query", T0 + 10 * SEC, []))
        one = {"node_id": "rn-b", "snapshot_us": T0, "sessions": rows(1)}
        for bad in ({"instance": "rm-x", "nodes": []}, {"instance": "rm-x", "nodes": "x"}, {"instance": "rm-x", "nodes": [1]},
                    {"instance": "rm-x", "nodes": [one, dict(one, sessions=rows(2))]}, {"instance": "rm-x", "nodes": [one, dict(one, node_id="rn-p", sessions=[])]},
                    {"instance": "rm-x", "nodes": [one, dict(one, node_id="rn-p", snapshot_us=0)]}):
            with self.assertRaises(ValueError):
                self.service.kill_statements(bad)
        with self.assertRaises(SessionRequestError):
            self.service.kill_statements({"instance": "rm-x", "nodes": [one, dict(one, node_id="rn-z")]})

    def test_kill_statement_requests_are_checked(self):
        for bad in (self.payload(node_id="rn-z"), self.payload(instance="rm-other"), self.payload(node_id="")):
            with self.assertRaises(SessionRequestError):
                self.service.kill_statements(bad)
        for bad in (self.payload(sessions="x"), self.payload(sessions=[1]), self.payload(snapshot_us=True), self.payload(snapshot_us="1"),
                    self.payload(snapshot_us=0), self.payload(mode="drop"), self.payload(sessions=[])):
            with self.assertRaises(ValueError):
                self.service.kill_statements(bad)


if __name__ == "__main__":
    unittest.main()
