"""The retention loop: what it reads, how it backs off, and what it says when DAS refuses."""
import json
import tempfile
import threading
import unittest
from pathlib import Path

from app.live_sessions import LiveSessionsError
from app.session_collector import (CONFIG_NAME, DEFAULT_CONFIG, MAX_BACKOFF_SECONDS, PURGE_EVERY_SECONDS, SessionCollector, load_config)
from app.session_store import SessionStore

SEC = 1_000_000
T0 = 1_790_000_000 * SEC


def das(*sessions, stamp=T0 // 1000):
    return {"IsFinish": True, "SessionData": {"SessionList": list(sessions), "TotalSessionCount": len(sessions), "TimeStamp": stamp}}


def idle_trx(session_id=5, seconds=60):
    return dict(SessionId=session_id, User="app", Client="10.0.0.1", DbName="shop", Command="Sleep", Time=seconds, State="", SqlText="",
                TrxId="9", TrxDuration=seconds)


class FakeLive:
    def __init__(self, answers):
        self.answers = answers          # (instance, node) -> DAS Data, or an exception to raise
        self.calls = []

    def view(self, instance, node, build):
        self.calls.append((instance, node))
        answer = self.answers[instance, node]
        if isinstance(answer, BaseException):
            raise answer
        payload = build(answer, 5)
        payload.update(instance_id=instance, node_id=node)
        return {**payload, "cached": False}


class Clock:
    def __init__(self):
        self.now = T0

    def __call__(self):
        return self.now


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)

    def write(self, value):
        (self.dir / CONFIG_NAME).write_text(value if isinstance(value, str) else json.dumps(value), encoding="utf-8")

    def test_without_the_file_the_loop_is_on_with_its_defaults(self):
        self.assertEqual(load_config(self.dir), DEFAULT_CONFIG)
        self.assertEqual(DEFAULT_CONFIG, {"enabled": True, "intervalSeconds": 60, "retentionDays": 7, "instances": []})

    def test_the_file_overrides_what_it_names_and_switches_the_loop_off(self):
        self.write({"enabled": False})
        self.assertEqual(load_config(self.dir), {**DEFAULT_CONFIG, "enabled": False})
        self.write({"intervalSeconds": 120, "retentionDays": 14, "instances": ["rm-x"], "ignored": 1})
        self.assertEqual(load_config(self.dir), {"enabled": True, "intervalSeconds": 120, "retentionDays": 14, "instances": ["rm-x"]})

    def test_a_file_that_cannot_be_used_is_reported_and_the_defaults_apply(self):
        for bad in ("{not json", "[]", {"enabled": "yes"}, {"intervalSeconds": 5}, {"intervalSeconds": True}, {"retentionDays": 0}, {"instances": "rm-x"}, {"instances": [1]}):
            self.write(bad)
            with self.assertLogs("app.session_collector", "WARNING") as logs:
                self.assertEqual(load_config(self.dir), DEFAULT_CONFIG)
            self.assertIn("无法使用", logs.output[0])


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = SessionStore(Path(self.tmp.name) / "s.sqlite3")
        self.clock = Clock()
        self.answers = {("rm-p", "rn-1"): das(idle_trx()), ("rm-p", "rn-2"): das(), ("rm-t", "rn-9"): das(idle_trx(8))}
        self.live = FakeLive(self.answers)
        self.collector = SessionCollector(self.live, self.store, config_loader=lambda: dict(DEFAULT_CONFIG),
                                          nodes=lambda: [("rm-p", "rn-1"), ("rm-p", "rn-2"), ("rm-t", "rn-9"), ("rm-p", "")],
                                          default_instances=lambda: ["rm-p"], wall_us=self.clock)

    def test_the_primary_instance_is_read_by_default_and_only_collected_nodes_with_an_id(self):
        self.assertEqual(self.collector.targets(dict(DEFAULT_CONFIG)), [("rm-p", "rn-1"), ("rm-p", "rn-2")])
        self.assertEqual(self.collector.targets({**DEFAULT_CONFIG, "instances": ["rm-t"]}), [("rm-t", "rn-9")])
        self.assertEqual(self.collector.targets({**DEFAULT_CONFIG, "instances": ["rm-nope"]}), [])

    def test_a_pass_stores_the_abnormal_sessions_and_reports_each_node(self):
        config = dict(DEFAULT_CONFIG)
        self.assertEqual(self.collector.run_pass(config, self.collector.targets(config)), 2)
        history = self.store.history("rm-p", "", T0, T0)
        self.assertEqual([(row["node_id"], row["session_id"], row["kinds"]) for row in history["sessions"]], [("rn-1", 5, ["long_trx", "idle_trx"])])
        self.assertEqual([item["node_id"] for item in history["coverage"]], ["rn-1", "rn-2"])
        status = self.collector.status()
        self.assertEqual([(n["node_id"], n["reads"], n["consecutive_failures"], n["abnormal_now"]) for n in status["nodes"]],
                         [("rn-1", 1, 0, 1), ("rn-2", 1, 0, 0)])
        self.assertEqual(status["store"]["snapshots"], 2)

    def test_a_refusal_is_logged_once_per_streak_backed_off_and_forgotten_on_recovery(self):
        config = dict(DEFAULT_CONFIG)
        self.answers["rm-p", "rn-1"] = LiveSessionsError("das_throttled", "Throttling.User: flow control")
        with self.assertLogs("app.session_collector", "WARNING") as logs:
            self.assertFalse(self.collector.read_one("rm-p", "rn-1", config))
        self.assertEqual(len(logs.output), 1)
        self.assertIn("code=das_throttled", logs.output[0])
        self.assertIn("rn-1", logs.output[0])
        # inside the back-off the node is not asked again (the other node still is)
        calls = len(self.live.calls)
        self.assertFalse(self.collector.read_one("rm-p", "rn-1", config))
        self.assertEqual(len(self.live.calls), calls)
        self.assertTrue(self.collector.read_one("rm-p", "rn-2", config))
        node = self.collector.status()["nodes"][0]
        self.assertEqual((node["node_id"], node["consecutive_failures"], node["last_error"]["code"]), ("rn-1", 1, "das_throttled"))
        # after the delay it is asked again; a second failure is not logged again, a success is
        self.clock.now += 3600 * SEC
        self.assertFalse(self.collector.read_one("rm-p", "rn-1", config))
        self.assertEqual(self.collector.status()["nodes"][0]["consecutive_failures"], 2)
        self.answers["rm-p", "rn-1"] = das(idle_trx())
        self.clock.now += 3600 * SEC
        with self.assertLogs("app.session_collector", "INFO") as logs:
            self.assertTrue(self.collector.read_one("rm-p", "rn-1", config))
        self.assertTrue(any("recovered" in line and "after 2 failed reads" in line for line in logs.output))
        self.assertEqual(self.collector.status()["nodes"][0]["consecutive_failures"], 0)

    def test_the_delay_doubles_with_each_failure_up_to_ten_minutes(self):
        config = dict(DEFAULT_CONFIG)
        self.answers["rm-p", "rn-1"] = LiveSessionsError("das_failed", "boom")
        waited = []
        for _ in range(8):
            self.clock.now += 3600 * SEC
            with self.assertLogs("app.session_collector", "WARNING") if not waited else self.assertNoLogs("app.session_collector", "WARNING"):
                self.collector.read_one("rm-p", "rn-1", config)
            waited.append((self.collector._state["rm-p", "rn-1"]["next_us"] - self.clock.now) // SEC)
        self.assertEqual(waited[:4], [120, 240, 480, MAX_BACKOFF_SECONDS])
        self.assertEqual(set(waited[3:]), {MAX_BACKOFF_SECONDS})

    def test_a_bug_in_one_read_does_not_stop_the_others(self):
        config = dict(DEFAULT_CONFIG)
        self.answers["rm-p", "rn-1"] = RuntimeError("unexpected")
        with self.assertLogs("app.session_collector", "WARNING"):
            self.assertEqual(self.collector.run_pass(config, self.collector.targets(config)), 1)
        self.assertEqual(self.collector.status()["nodes"][0]["last_error"]["code"], "unexpected")

    def test_the_purge_runs_at_most_once_an_hour_with_the_retention_cutoff(self):
        removed = []
        self.store.purge = lambda before: removed.append(before) or 0
        config = {**DEFAULT_CONFIG, "retentionDays": 3}
        self.collector.purge_if_due(config)
        self.collector.purge_if_due(config)
        self.assertEqual(removed, [T0 - 3 * 86400 * SEC])
        self.clock.now += (PURGE_EVERY_SECONDS + 1) * SEC
        self.collector.purge_if_due(config)
        self.assertEqual(len(removed), 2)

    def test_a_failed_purge_is_logged_and_retried_later(self):
        def broken(_before):
            raise OSError("disk")
        self.store.purge = broken
        with self.assertLogs("app.session_collector", "ERROR"):
            self.collector.purge_if_due(dict(DEFAULT_CONFIG))

    def test_the_loop_reads_every_target_then_stops_when_asked_and_a_disabled_loop_reads_nothing(self):
        waits = []

        class Stop:
            """Event stand-in: stops the loop at the end of the first pass."""
            def __init__(self, limit):
                self.limit = limit

            def is_set(self):
                return len(waits) >= self.limit

            def wait(self, seconds):
                waits.append(seconds)
                return self.is_set()

            def set(self):
                self.limit = 0

        self.collector._stop = Stop(2)
        self.collector._loop()
        self.assertEqual(self.live.calls, [("rm-p", "rn-1"), ("rm-p", "rn-2")])
        self.assertEqual(waits[:2], [30.0, 30.0])         # two nodes in a 60 s interval: one every 30 s
        self.live.calls.clear()
        waits.clear()
        off = SessionCollector(self.live, self.store, config_loader=lambda: {**DEFAULT_CONFIG, "enabled": False},
                               nodes=lambda: [("rm-p", "rn-1")], default_instances=lambda: ["rm-p"], wall_us=self.clock)
        off._stop = Stop(1)
        off._loop()
        self.assertEqual(self.live.calls, [])
        self.assertEqual(waits, [30.0])                   # idles and looks at the file again
        self.assertFalse(off.status()["enabled"] and off.status()["running"])

    def test_start_runs_a_daemon_thread_and_shutdown_stops_it(self):
        collector = SessionCollector(self.live, self.store, config_loader=lambda: {**DEFAULT_CONFIG, "enabled": False},
                                     nodes=lambda: [], default_instances=lambda: [], wall_us=self.clock)
        collector.start()
        collector.start()                                  # a second start is a no-op
        self.assertTrue(collector.status()["running"])
        self.assertEqual(sum(t.name == "session-retention" for t in threading.enumerate()), 1)
        collector.shutdown()
        collector._thread.join(5)
        self.assertFalse(collector._thread.is_alive())


if __name__ == "__main__":
    unittest.main()
