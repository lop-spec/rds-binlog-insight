"""Reading a node's sessions from the database itself: the one statement, the DAS-shaped answer, what is distrusted, and the fallback."""
import json
import re
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from app.direct_sessions import (CONFIG_NAME, INTERNAL_USERS, DirectSessionError, DirectSessionReader, build_data, load_config)
from app.live_sessions import DIRECT_RETRY_SECONDS, LiveSessions, LiveSessionsError
from app.session_view import build_session_view
from tests.test_live_sessions import Client, Clock, finished, session

WALL = 1_790_000_000.0
PRIMARY_HOST = "rm-x.mysql.example.invalid"
PROXY_HOST = "proxy.rwlb.example.invalid"
ROLES = {"rn-p": {"role": "primary", "zone": "cn-a"}, "rn-a": {"role": "secondary", "zone": "cn-a"}, "rn-b": {"role": "secondary", "zone": "cn-b"}}
CONFIG = {"enabled": True, "instances": {"rm-x": {"primaryHost": PRIMARY_HOST}}}


def row(ident, user="app", host="10.0.0.1:51000", db="shop", command="Sleep", seconds=3, state="", info=None, trx_id=None, trx_s=None, sid=11, ro=1):
    return (sid, ro, ident, user, host, db, command, seconds, state, info, trx_id, trx_s)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, args=None):
        self.connection.statements.append(sql)
        self.connection.arguments.append(args)
        if self.connection.fail_query:
            raise OSError("lost connection")

    def fetchall(self):
        return self.connection.rows


class FakeConnection:
    def __init__(self, rows, fail_query=False):
        self.rows, self.fail_query, self.statements, self.arguments, self.closed = rows, fail_query, [], [], False

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = True


class FakeServer:
    """What ``pymysql.connect`` is replaced with: records every connection and answers from ``answers[(host, hint node)]``."""

    def __init__(self, answers, connect_error=None, fail_query=False):
        self.answers, self.connect_error, self.fail_query = answers, connect_error, fail_query
        self.connections, self.kwargs = [], []

    def __call__(self, **kwargs):
        self.kwargs.append(kwargs)
        if self.connect_error:
            raise self.connect_error
        connection = FakeServerConnection(self, kwargs["host"])
        self.connections.append(connection)
        return connection


class FakeServerConnection(FakeConnection):
    def __init__(self, server, host):
        super().__init__([], server.fail_query)
        self.server, self.host = server, host

    def cursor(self):
        outer = self

        class Cursor(FakeCursor):
            def execute(self, sql, args=None):
                match = re.match(r"/\*FORCE_NODE='([^']+)'\*/", sql)
                outer.rows = outer.server.answers[(outer.host, match.group(1) if match else "")]
                super().execute(sql, args)

        return Cursor(self)


def reader(tmp, server, *, config=CONFIG, roles=ROLES, instances=None):
    if config is not None:
        (Path(tmp) / CONFIG_NAME).write_text(json.dumps(config), encoding="utf-8")
    credential = SimpleNamespace(instance_id="rm-x", host=PROXY_HOST, port=3306, user="reader", password="not-a-real-password")
    return DirectSessionReader(Path(tmp), instances=instances or (lambda: [credential]), roles=lambda _instance: roles, connect=server, wall=lambda: WALL)


class DirectReaderCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)

    def standby_server(self):
        return FakeServer({(PROXY_HOST, "rn-a"): [row(44, command="Query", seconds=70, state="executing", info="SELECT 1", sid=21, ro=1)],
                           (PROXY_HOST, "rn-b"): [row(7, sid=22, ro=1)], (PRIMARY_HOST, ""): [row(9, sid=20, ro=0)]})


class StatementTests(DirectReaderCase):
    def test_one_select_with_the_nodes_hint_and_no_platform_accounts(self):
        server = self.standby_server()
        reader(self.tmp.name, server).read("rm-x", "rn-a")
        connection = server.connections[0]
        self.assertEqual(len(connection.statements), 1)                       # one statement, one round trip
        sql = connection.statements[0]
        self.assertTrue(sql.startswith("/*FORCE_NODE='rn-a'*/ SELECT /*+ MAX_EXECUTION_TIME(5000) */ "))
        for part in ("information_schema.PROCESSLIST", "information_schema.INNODB_TRX", "trx_autocommit_non_locking = 0", "p.ID <> CONNECTION_ID()", "@@read_only", "@@server_id"):
            self.assertIn(part, sql)
        arguments = connection.arguments[0]
        self.assertEqual(arguments, list(INTERNAL_USERS))                       # the platform accounts are bound parameters
        self.assertEqual(sql.count("%s"), len(arguments))
        for name in INTERNAL_USERS:
            self.assertNotIn(name, sql)
        self.assertNotRegex(sql, r"(?i)\b(KILL|SET|UPDATE|INSERT|DELETE|DROP|ALTER|CREATE|GRANT|FLUSH)\b")
        self.assertTrue(connection.closed)
        kwargs = server.kwargs[0]
        self.assertEqual((kwargs["host"], kwargs["port"], kwargs["user"], kwargs["connect_timeout"], kwargs["read_timeout"]), (PROXY_HOST, 3306, "reader", 3, 8))

    def test_accounts_a_deployment_adds_are_left_out_too_and_cannot_break_the_statement(self):
        server = self.standby_server()
        extra = ["platform-agent", "x'); DROP TABLE t; --", "platform-agent"]
        direct = reader(self.tmp.name, server, config={**CONFIG, "excludeUsers": extra})
        direct.read("rm-x", "rn-a")
        connection = server.connections[0]
        self.assertEqual(connection.arguments[0], [*INTERNAL_USERS, "platform-agent", "x'); DROP TABLE t; --"])   # once each, as parameters
        self.assertNotIn("DROP", connection.statements[0])

    def test_the_primary_is_read_through_its_own_address_without_a_hint(self):
        server = self.standby_server()
        data = reader(self.tmp.name, server).read("rm-x", "rn-p")
        self.assertEqual(server.kwargs[0]["host"], PRIMARY_HOST)
        self.assertTrue(server.connections[0].statements[0].startswith("SELECT /*+ MAX_EXECUTION_TIME(5000) */ "))
        self.assertEqual(data["Direct"]["read_only"], 0)

    def test_the_answer_has_the_shape_of_a_das_snapshot_and_the_session_view_reads_it(self):
        rows = [row(44, user="etl", host="10.0.0.9:51234", db=None, command="Query", seconds=70, state="executing", info="SELECT * FROM big"),
                row(312, trx_id=99, trx_s=905, seconds=905), row(7, host="localhost", db="shop", seconds=3)]
        data, server_id, read_only = build_data(rows, WALL)
        self.assertEqual((server_id, read_only, data["Source"], data["IsFinish"], data["Fail"]), (11, 1, "mysql", True, False))
        snapshot = data["SessionData"]
        self.assertEqual((snapshot["TotalSessionCount"], snapshot["TimeStamp"]), (3, 1_790_000_000_000))
        first, second, third = snapshot["SessionList"]
        self.assertEqual(first, {"SessionId": 44, "User": "etl", "Client": "10.0.0.9", "DbName": "", "Command": "Query", "State": "executing",
                                 "Time": 70, "SqlText": "SELECT * FROM big"})          # no transaction fields for a session in none
        self.assertEqual((second["TrxId"], second["TrxDuration"], second["SqlText"]), ("99", 905, ""))
        self.assertEqual(third["Client"], "localhost")
        view = build_session_view(data, 5)
        flagged = {item["session_id"]: item["kinds"] for item in view["sessions"] if item["kinds"]}
        self.assertEqual(flagged, {44: ["long_running"], 312: ["long_trx", "idle_trx"]})
        self.assertEqual((view["observed_at_us"], view["overview"]["total"]), (1_790_000_000_000_000, 3))

    def test_a_long_statement_is_kept_whole(self):
        long_sql = "SELECT '" + "商品🧪" * 5000 + "' LIMIT 50;"
        data, _, _ = build_data([row(5, command="Query", seconds=1, info=long_sql)], WALL)
        self.assertEqual(data["SessionData"]["SessionList"][0]["SqlText"], long_sql)


class DistrustTests(DirectReaderCase):
    def test_a_node_that_is_not_the_one_asked_for_is_refused(self):
        server = FakeServer({(PROXY_HOST, "rn-a"): [row(1, ro=0)], (PRIMARY_HOST, ""): [row(1, ro=1)]})
        direct = reader(self.tmp.name, server)
        for node in ("rn-a", "rn-p"):                                         # a standby that is writable, a primary that is read only
            with self.assertRaises(DirectSessionError) as caught:
                direct.read("rm-x", node)
            self.assertEqual(caught.exception.code, "direct_wrong_node")

    def test_a_server_id_that_belongs_to_another_node_means_the_hint_was_not_followed(self):
        server = FakeServer({(PROXY_HOST, "rn-a"): [row(1, sid=21)], (PROXY_HOST, "rn-b"): [row(1, sid=21)]})
        direct = reader(self.tmp.name, server)
        direct.read("rm-x", "rn-a")
        direct.read("rm-x", "rn-a")                                           # the same node answering again is fine
        with self.assertRaises(DirectSessionError) as caught:
            direct.read("rm-x", "rn-b")
        self.assertEqual(caught.exception.code, "direct_wrong_node")
        self.assertIn("rn-a", str(caught.exception))

    def test_a_node_that_was_rebuilt_gets_a_new_server_id_without_a_complaint(self):
        server = FakeServer({(PROXY_HOST, "rn-a"): [row(1, sid=21)], (PROXY_HOST, "rn-b"): [row(1, sid=23)]})
        direct = reader(self.tmp.name, server)
        direct.read("rm-x", "rn-a")
        server.answers[(PROXY_HOST, "rn-a")] = [row(1, sid=22)]
        direct.read("rm-x", "rn-a")
        server.answers[(PROXY_HOST, "rn-b")] = [row(1, sid=21)]               # the old id is free again
        direct.read("rm-x", "rn-b")

    def test_no_role_no_credential_and_a_node_id_that_cannot_sit_in_a_hint_never_reach_the_database(self):
        server = self.standby_server()
        cases = [(dict(roles={}), "rm-x", "rn-a", "direct_role_unknown"),
                 (dict(roles={"rn-a": {"role": "", "stale": True}}), "rm-x", "rn-a", "direct_role_unknown"),
                 (dict(instances=lambda: []), "rm-x", "rn-a", "direct_no_credential"),
                 (dict(instances=lambda: 1 / 0), "rm-x", "rn-a", "direct_no_credential"),
                 (dict(), "rm-x", "rn-1'*/ KILL 1; /*", "direct_bad_node")]
        for kwargs, instance, node, code in cases:
            with self.assertRaises(DirectSessionError) as caught:
                reader(self.tmp.name, server, **kwargs).read(instance, node)
            self.assertEqual(caught.exception.code, code, node)
        self.assertEqual(server.kwargs, [])

    def test_connect_and_query_failures_say_why_and_the_connection_is_closed(self):
        with self.assertRaises(DirectSessionError) as caught:
            reader(self.tmp.name, FakeServer({}, connect_error=OSError("timed out"))).read("rm-x", "rn-a")
        self.assertEqual(caught.exception.code, "direct_connect_failed")
        self.assertIn("timed out", str(caught.exception))
        failing = FakeServer({(PROXY_HOST, "rn-a"): []}, fail_query=True)
        with self.assertRaises(DirectSessionError) as caught:
            reader(self.tmp.name, failing).read("rm-x", "rn-a")
        self.assertEqual(caught.exception.code, "direct_query_failed")
        self.assertTrue(failing.connections[0].closed)
        empty = FakeServer({(PROXY_HOST, "rn-a"): []})
        with self.assertRaises(DirectSessionError) as caught:
            reader(self.tmp.name, empty).read("rm-x", "rn-a")
        self.assertEqual(caught.exception.code, "direct_empty")


class ConfigTests(DirectReaderCase):
    def test_without_the_file_or_with_one_that_cannot_be_used_nothing_is_read_directly(self):
        self.assertFalse(reader(self.tmp.name, self.standby_server(), config=None).enabled_for("rm-x"))
        for bad in ("not json", "[]", json.dumps({"enabled": "yes", "instances": {}}), json.dumps({"enabled": True, "instances": []}),
                    json.dumps({"enabled": True, "instances": {"rm-x": {}}}), json.dumps({"enabled": True, "instances": {"rm-x": {"primaryHost": "a b;c"}}}),
                    json.dumps({"enabled": True, "instances": {"rm-x": {"primaryHost": PRIMARY_HOST}}, "cacheSeconds": 999}),
                    json.dumps({**CONFIG, "excludeUsers": "root"}), json.dumps({**CONFIG, "excludeUsers": [""]}), json.dumps({**CONFIG, "excludeUsers": [1]}),
                    json.dumps({**CONFIG, "excludeUsers": ["x"] * 51})):
            (Path(self.tmp.name) / CONFIG_NAME).write_text(bad, encoding="utf-8")
            with self.assertLogs("app.direct_sessions", "WARNING"):
                self.assertFalse(load_config(Path(self.tmp.name))["enabled"], bad)

    def test_only_the_listed_instances_are_read_directly_and_the_file_is_read_again_when_it_changes(self):
        direct = reader(self.tmp.name, self.standby_server())
        self.assertTrue(direct.enabled_for("rm-x"))
        self.assertFalse(direct.enabled_for("rm-other"))
        self.assertEqual(direct.cache_seconds(), 3.0)
        (Path(self.tmp.name) / CONFIG_NAME).write_text(json.dumps({"enabled": False, "instances": CONFIG["instances"], "cacheSeconds": 5}), encoding="utf-8")
        self.assertFalse(direct.enabled_for("rm-x"))                          # switched off without a restart
        self.assertEqual(direct.cache_seconds(), 5.0)


class LiveSessionsFallbackTests(DirectReaderCase):
    def live(self, server, das=None, **kwargs):
        clock = Clock()
        direct = reader(self.tmp.name, server, **kwargs)
        client = das or Client(finished([session("a", "Query", 7)]))
        live = LiveSessions(lambda _instance: client, clock=clock, sleep=clock.sleep, wall_us=lambda: 5, direct=direct)
        return live, clock, client, direct

    def test_a_direct_read_is_one_statement_and_never_asks_das(self):
        server = self.standby_server()
        live, clock, client, _ = self.live(server)
        first = live.snapshot("rm-x", "rn-a")
        self.assertEqual((first["source"], first["cached"], [r["session_id"] for r in first["sessions"]]), ("mysql", False, [44]))
        self.assertNotIn("fallback", first)
        self.assertEqual(client.calls, [])
        clock.now += 2.9
        self.assertTrue(live.snapshot("rm-x", "rn-a")["cached"])               # the short database cache
        clock.now += 0.2
        self.assertFalse(live.snapshot("rm-x", "rn-a")["cached"])
        self.assertEqual(len(server.connections), 2)

    def test_six_nodes_are_not_held_to_the_two_reads_of_das(self):
        server = FakeServer({(PROXY_HOST, "rn-a"): [row(1, sid=21)], (PROXY_HOST, "rn-b"): [row(1, sid=22)], (PRIMARY_HOST, ""): [row(1, sid=20, ro=0)]})
        live, _, client, _ = self.live(server)
        for node in ("rn-p", "rn-a", "rn-b"):
            live.view("rm-x", node, lambda data, now: {"status": "ok", "total": data["SessionData"]["TotalSessionCount"]})
        self.assertEqual(client.calls, [])

    def test_das_can_be_asked_for_on_purpose_and_is_cached_apart(self):
        server = self.standby_server()
        live, _, client, _ = self.live(server)
        direct = live.snapshot("rm-x", "rn-a")
        das = live.view("rm-x", "rn-a", lambda data, now: {"status": "ok"}, "das")
        self.assertEqual((direct["source"], das["source"], das["cached"]), ("mysql", "das", False))
        self.assertEqual(len(client.calls), 1)
        again = live.view("rm-x", "rn-a", lambda data, now: {"status": "ok"}, "auto")      # the database answer is still cached, DAS was not asked again
        self.assertEqual((again["source"], again["cached"], len(client.calls)), ("mysql", True, 1))

    def test_a_direct_read_that_fails_falls_back_to_das_says_so_and_is_not_retried_at_once(self):
        server = FakeServer({}, connect_error=OSError("refused"))
        live, clock, client, _ = self.live(server)
        with self.assertLogs("app.live_sessions", "WARNING") as logs:
            first = live.snapshot("rm-x", "rn-a")
        self.assertEqual((first["source"], first["fallback"], len(client.calls)), ("das", "direct_connect_failed", 1))
        self.assertIn("direct_connect_failed", logs.output[0])
        self.assertIn("retry_in=30s", logs.output[0])
        clock.now += 11                                                       # the DAS answer is cached for 10 s; the direct reader is still resting
        second = live.snapshot("rm-x", "rn-a")
        self.assertEqual((second["source"], second["fallback"], len(server.kwargs), len(client.calls)), ("das", "direct_connect_failed", 1, 2))
        status = live.direct_status()
        self.assertEqual((status["enabled"], status["instances"]), (True, ["rm-x"]))
        self.assertEqual([(f["node_id"], f["code"], f["failures"]) for f in status["fallbacks"]], [("rn-a", "direct_connect_failed", 1)])
        clock.now += DIRECT_RETRY_SECONDS
        with self.assertLogs("app.live_sessions", "WARNING") as logs:
            live.snapshot("rm-x", "rn-a")
        self.assertEqual(len(server.kwargs), 2)                               # tried again once the rest is over, and rests twice as long
        self.assertIn("retry_in=60s", logs.output[0])

    def test_a_direct_read_that_works_again_clears_the_fallback(self):
        server = FakeServer({}, connect_error=OSError("refused"))
        live, clock, _, _ = self.live(server)
        with self.assertLogs("app.live_sessions", "WARNING"):
            live.snapshot("rm-x", "rn-a")
        server.connect_error = None
        server.answers = {(PROXY_HOST, "rn-a"): [row(44, command="Query", seconds=70, sid=21)]}
        clock.now += DIRECT_RETRY_SECONDS + 1
        back = live.snapshot("rm-x", "rn-a")
        self.assertEqual((back["source"], "fallback" in back, live.direct_status()["fallbacks"]), ("mysql", False, []))

    def test_when_the_fallback_fails_too_the_das_reason_is_what_the_caller_gets(self):
        server = FakeServer({}, connect_error=OSError("refused"))
        live, _, _, _ = self.live(server, das=Client(RuntimeError("DAS HTTP 403: Forbidden")))
        with self.assertLogs("app.live_sessions", "WARNING"), self.assertRaises(LiveSessionsError) as caught:
            live.snapshot("rm-x", "rn-a")
        self.assertEqual(caught.exception.code, "das_permission_denied")

    def test_instances_not_switched_on_and_mongo_members_are_read_from_das(self):
        server = self.standby_server()
        live, _, client, _ = self.live(server)
        self.assertEqual(live.snapshot("rm-other", "rn-a")["source"], "das")
        live.view("dds-x", "1001", lambda data, now: {"status": "ok"})
        self.assertEqual(server.kwargs, [])
        self.assertEqual(len(client.calls), 2)

    def test_without_a_reader_or_with_it_switched_off_everything_stays_on_das(self):
        server = self.standby_server()
        clock = Clock()
        client = Client(finished([session("a", "Query", 7)]))
        plain = LiveSessions(lambda _instance: client, clock=clock, sleep=clock.sleep, wall_us=lambda: 5)
        self.assertEqual((plain.snapshot("rm-x", "rn-a")["source"], plain.direct_status()), ("das", {"enabled": False, "instances": [], "fallbacks": []}))
        live, _, client, _ = self.live(server, config={"enabled": False, "instances": CONFIG["instances"]})
        self.assertEqual(live.snapshot("rm-x", "rn-a")["source"], "das")
        self.assertEqual(server.kwargs, [])


if __name__ == "__main__":
    unittest.main()
