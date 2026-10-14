"""Running statements of one node, read on demand from DAS: what is listed, how it is read, and how it fails."""
import threading
import unittest

from app.live_sessions import (ACTION, DEADLINE_SECONDS, MAX_SESSIONS, POLL_SECONDS, SQL_LIMIT, LiveSessions,
                               LiveSessionsError, classify_failure, normalize_sessions)


def session(user, command, seconds, sql="SELECT 1", client="10.0.0.1", database="app", state="executing", session_id=1):
    return dict(User=user, Command=command, Time=seconds, SqlText=sql, Client=client, DbName=database, State=state,
                SessionId=session_id)


def finished(sessions, total=None, stamp=1_790_000_000_000):
    return {"Success": True, "Data": {"IsFinish": True, "State": "SUCCESS", "ResultId": "r1",
                                      "SessionData": {"SessionList": sessions, "TotalSessionCount": total or len(sessions),
                                                      "TimeStamp": stamp}}}


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class Client:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def call(self, action, params):
        self.calls.append((action, dict(params)))
        reply = self.responses.pop(0) if len(self.responses) > 1 else self.responses[0]
        if isinstance(reply, BaseException):
            raise reply
        return reply


def service(client, clock=None, **kwargs):
    clock = clock or Clock()
    return LiveSessions(lambda _instance: client, clock=clock, sleep=clock.sleep, wall_us=lambda: 5, **kwargs), clock


class NormalizeTests(unittest.TestCase):
    def test_idle_and_replication_sessions_are_dropped_and_the_longest_comes_first(self):
        data = finished([session("root", "Sleep", 900, "", session_id=1), session("repl", "Binlog Dump GTID", 5_000_000, "", session_id=2),
                         session("root", "Daemon", 3, "", session_id=3), session("analyst", "Query", 885, "SELECT slow", "10.0.0.8", session_id=4),
                         session("root", "Execute", 0, "SELECT fast", session_id=5), session("root", "Execute", 2, "SELECT mid", session_id=6)],
                        total=1066)["Data"]
        result = normalize_sessions(data, now_us=7)
        self.assertEqual([row["session_id"] for row in result["sessions"]], [4, 6, 5])
        self.assertEqual((result["total_sessions"], result["running_sessions"], result["longest_running_seconds"]), (1066, 3, 885))
        first = result["sessions"][0]
        self.assertEqual((first["user"], first["client"], first["database"], first["command"], first["state"], first["seconds"]),
                         ("analyst", "10.0.0.8", "app", "Query", "executing", 885))
        self.assertEqual(result["observed_at_us"], 1_790_000_000_000_000)   # the snapshot's own time, not ours
        self.assertFalse(result["truncated"])

    def test_statement_text_is_cut_and_its_length_kept(self):
        long_sql = "SELECT " + "x" * 5000
        row = normalize_sessions(finished([session("a", "Query", 1, long_sql)])["Data"], 0)["sessions"][0]
        self.assertEqual(len(row["sql"]), SQL_LIMIT)
        self.assertEqual(row["sql_chars"], len(long_sql))

    def test_a_long_list_is_capped_and_says_so(self):
        many = [session("a", "Query", i, session_id=i) for i in range(MAX_SESSIONS + 5)]
        result = normalize_sessions(finished(many)["Data"], 0)
        self.assertEqual(len(result["sessions"]), MAX_SESSIONS)
        self.assertEqual(result["running_sessions"], MAX_SESSIONS + 5)
        self.assertTrue(result["truncated"])
        self.assertEqual(result["sessions"][0]["seconds"], MAX_SESSIONS + 4)

    def test_missing_fields_do_not_break_the_list(self):
        result = normalize_sessions({"SessionData": {"SessionList": [{"Command": "Query", "Time": "oops"}]}}, now_us=9)
        self.assertEqual(result["sessions"][0]["seconds"], 0)
        self.assertEqual(result["sessions"][0]["user"], "")
        self.assertEqual(result["observed_at_us"], 9)


class ReadTests(unittest.TestCase):
    def test_the_async_snapshot_is_polled_with_its_result_id_until_it_finishes(self):
        client = Client({"Success": True, "Data": {"IsFinish": False, "State": "RUNNING", "ResultId": "async-1"}},
                        finished([session("analyst", "Query", 30)]))
        live, clock = service(client)
        result = live.snapshot("rm-x", "rn-1")
        self.assertEqual(result["status"], "ok")
        self.assertFalse(result["cached"])
        self.assertEqual((result["instance_id"], result["node_id"]), ("rm-x", "rn-1"))
        self.assertEqual(client.calls[0], (ACTION, {"InstanceId": "rm-x", "NodeId": "rn-1"}))
        self.assertEqual(client.calls[1], (ACTION, {"InstanceId": "rm-x", "NodeId": "rn-1", "ResultId": "async-1"}))
        self.assertEqual(clock.now, POLL_SECONDS)

    def test_a_snapshot_that_never_finishes_times_out_with_a_reason(self):
        client = Client({"Success": True, "Data": {"IsFinish": False, "State": "RUNNING", "ResultId": "async-1"}})
        live, clock = service(client)
        with self.assertLogs("app.live_sessions", "WARNING"), self.assertRaises(LiveSessionsError) as caught:
            live.snapshot("rm-x", "rn-1")
        self.assertEqual(caught.exception.code, "das_timeout")
        self.assertGreaterEqual(clock.now, DEADLINE_SECONDS)

    def test_a_failed_snapshot_is_reported(self):
        client = Client({"Success": True, "Data": {"IsFinish": False, "Fail": True, "State": "FAILED"}})
        live, _ = service(client)
        with self.assertLogs("app.live_sessions", "WARNING"), self.assertRaises(LiveSessionsError) as caught:
            live.snapshot("rm-x", "rn-1")
        self.assertEqual(caught.exception.code, "das_failed")
        self.assertIn("FAILED", str(caught.exception))

    def test_failures_keep_their_reason_and_a_missing_permission_is_told_apart(self):
        for error, code in ((RuntimeError("DAS HTTP 403: Forbidden.RAM not authorized"), "das_permission_denied"),
                            (RuntimeError("DAS 调用失败：Throttling.User: Request was denied due to user flow control"), "das_throttled"),
                            (RuntimeError("DAS HTTP 429: Too Many Requests"), "das_throttled"),
                            (RuntimeError("DAS 网络错误：timed out"), "das_unreachable"),
                            (RuntimeError("DAS 调用失败：InvalidNode"), "das_failed")):
            self.assertEqual(classify_failure(error), code)
            live, _ = service(Client(error))
            with self.assertLogs("app.live_sessions", "WARNING") as logs, self.assertRaises(LiveSessionsError) as caught:
                live.snapshot("rm-x", "rn-1")
            self.assertEqual(caught.exception.code, code)
            self.assertIn(code, logs.output[0])   # a failure always leaves a log line with its code

    def test_no_credential_is_reported_without_any_call(self):
        def factory(_instance):
            raise RuntimeError("读不到凭据: target=x")
        live = LiveSessions(factory)
        with self.assertLogs("app.live_sessions", "WARNING"), self.assertRaises(LiveSessionsError) as caught:
            live.snapshot("rm-x", "rn-1")
        self.assertEqual(caught.exception.code, "das_failed")


class ViewTests(unittest.TestCase):
    def test_every_view_of_a_node_shares_one_read_and_one_cache(self):
        client = Client(finished([session("a", "Query", 7), session("b", "Sleep", 90)]))
        live, clock = service(client)
        running = live.snapshot("rm-x", "rn-1")
        shaped = live.view("rm-x", "rn-1", lambda data, now_us: {"status": "ok", "listed": len(data["SessionData"]["SessionList"]), "now": now_us})
        self.assertEqual((running["cached"], shaped["cached"]), (False, True))
        self.assertEqual((shaped["listed"], shaped["now"], shaped["instance_id"], shaped["node_id"]), (2, 5, "rm-x", "rn-1"))
        self.assertEqual(len(client.calls), 1)
        self.assertEqual([row["session_id"] for row in running["sessions"]], [1])   # the running view is unchanged by the refactor


class CacheTests(unittest.TestCase):
    def test_a_repeat_within_the_cache_window_does_not_ask_DAS_again(self):
        client = Client(finished([session("a", "Query", 1)]))
        live, clock = service(client, cache_seconds=10)
        first = live.snapshot("rm-x", "rn-1")
        clock.now += 9
        second = live.snapshot("rm-x", "rn-1")
        self.assertEqual((first["cached"], second["cached"]), (False, True))
        self.assertEqual(len(client.calls), 1)
        clock.now += 2
        third = live.snapshot("rm-x", "rn-1")
        self.assertFalse(third["cached"])
        self.assertEqual(len(client.calls), 2)

    def test_nodes_are_cached_separately(self):
        client = Client(finished([session("a", "Query", 1)]))
        live, _ = service(client)
        live.snapshot("rm-x", "rn-1")
        live.snapshot("rm-x", "rn-2")
        self.assertEqual([call[1]["NodeId"] for call in client.calls], ["rn-1", "rn-2"])

    def test_two_people_clicking_at_once_cost_one_read(self):
        release, entered = threading.Event(), threading.Event()

        class Slow(Client):
            def call(self, action, params):
                entered.set()
                release.wait(5)
                return super().call(action, params)

        client = Slow(finished([session("a", "Query", 1)]))
        live, _ = service(client)
        results = []
        threads = [threading.Thread(target=lambda: results.append(live.snapshot("rm-x", "rn-1"))) for _ in range(2)]
        threads[0].start()
        entered.wait(5)
        threads[1].start()
        release.set()
        for thread in threads:
            thread.join(5)
        self.assertEqual(len(client.calls), 1)
        self.assertEqual(sorted(item["cached"] for item in results), [False, True])


if __name__ == "__main__":
    unittest.main()
