"""MongoDB sessions read straight from the members ($currentOp): the documents as a DAS snapshot, what is distrusted, the
dispatch from the shared reader, and the stored snapshot a member without a direct path may be served from."""
import json
import os
import tempfile
import unittest
from pathlib import Path

from app.direct_mongo_sessions import APP_NAME, PIPELINE, DirectMongoReader, build_data
from app.direct_sessions import CONFIG_NAME, DirectSessionError, DirectSessionReader
from app.live_sessions import LiveSessions
from app.mongo_sessions import build_mongo_session_view
from tests.test_live_sessions import Client, Clock

INSTANCE = "dds-example0000000"
HOST1 = "dds-example00000001.mongodb.cn-example.rds.aliyuncs.com"
HOST2 = "dds-example00000002.mongodb.cn-example.rds.aliyuncs.com"
ROLES = {"1001": {"role": "primary", "zone": "a", "host": HOST1}, "1002": {"role": "secondary", "zone": "a", "host": HOST2},
         "1003": {"role": "hidden", "zone": "a", "host": ""}}
ENTRY = {"instanceId": INSTANCE, "enabled": True, "credentialsFile": ".credentials/readonly.json", "readonlyUsername": "reader", "port": 3717, "tls": False}
WALL = 1_792_681_739.0
THRESHOLDS = {"blocking_s": 30, "trx_s": 30, "idle_trx_s": 10}


def doc(desc, opid=None, active=False, op="", ns="", secs=0, command=None, client="10.0.0.2:53784", driver=("mongo-go-driver", "2.4.0"), app=""):
    item = {"desc": desc, "client": client, "active": active, "appName": app, "connectionId": int(desc[4:]) if desc[4:].isdigit() else 0,
            "clientMetadata": {"driver": {"name": driver[0], "version": driver[1]}} if driver else {}}
    if opid is not None:
        item.update(opid=opid, op=op, ns=ns, secs_running=secs)
    if command is not None:
        item["command"] = command
    return item


DOCS = [
    doc("conn1"),                                                                                         # an idle connection
    doc("conn10", 77, True, "command", "appdb.orders", 45, {"aggregate": "orders", "pipeline": [{"$match": {"shop_id": 7}}], "$db": "appdb"}),
    doc("conn11", 78, True, "command", "", 120, {"hello": 1, "topologyVersion": {"counter": 5}, "$db": "admin"}, driver=("mongo-java-driver|sync", "5.1.3")),
    doc("conn12", 79, True, "getmore", "local.oplog.rs", 40, {"getMore": 123, "collection": "oplog.rs"}, driver=("NetworkInterfaceTL-ReplNetwork", "8.0.10")),  # replication tailing
    doc("BackgroundSync", 5, True, "none", "local.oplog.rs", 9000),                                       # an internal thread
    doc("conn99", 91, True, "command", "admin.$cmd.aggregate", 0, {"aggregate": 1, "pipeline": [{"$currentOp": {"allUsers": True}}]}, app=APP_NAME),
]


class FakeAdmin:
    def __init__(self, client):
        self.client = client

    def command(self, command, *args, **kwargs):
        name = command if isinstance(command, str) else next(iter(command))
        self.client.commands.append(name)
        if name == "connectionStatus":
            return self.client.identity
        if name == "hello":
            return self.client.hello
        raise AssertionError(f"unexpected command {name}")

    def aggregate(self, pipeline, **kwargs):
        self.client.pipelines.append((pipeline, kwargs))
        if self.client.fail_aggregate:
            raise OSError("connection reset")
        return iter(self.client.docs)


class FakeClient:
    def __init__(self, kwargs, spec):
        self.kwargs, self.closed = kwargs, False
        self.commands, self.pipelines = [], []
        self.identity = spec.get("identity") or {"authInfo": {"authenticatedUsers": [{"user": "reader", "db": "admin"}],
                                                              "authenticatedUserRoles": [{"role": "read", "db": "admin"}, {"role": "clusterMonitor", "db": "admin"}]}}
        self.hello, self.docs, self.fail_aggregate = spec["hello"], spec.get("docs", DOCS), spec.get("fail_aggregate", False)
        self.admin = FakeAdmin(self)

    def close(self):
        self.closed = True


class FakeFactory:
    """``pymongo.MongoClient`` replaced: one fake member per host, every client made is kept."""

    def __init__(self, members):
        self.members, self.made = members, []

    def __call__(self, **kwargs):
        client = FakeClient(kwargs, self.members[kwargs["host"]])
        self.made.append(client)
        return client


def primary(me="p0:27017"):
    return {"hello": {"isWritablePrimary": True, "secondary": False, "me": me}}


def secondary(me="s0:27017"):
    return {"hello": {"isWritablePrimary": False, "secondary": True, "me": me}}


class ReaderCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data = Path(self.tmp.name)
        (self.data / ".credentials").mkdir()
        self.secret = self.data / ".credentials" / "readonly.json"
        self.secret.write_text(json.dumps({"username": "reader", "password": "not-a-real-password", "authSource": "admin"}), encoding="utf-8")
        os.chmod(self.secret, 0o600)
        (self.data / CONFIG_NAME).write_text(json.dumps({"enabled": True, "instances": {INSTANCE: {}}}), encoding="utf-8")
        self.main = DirectSessionReader(self.data, instances=lambda: [], roles=lambda _instance: {})

    def reader(self, members, entries=None, roles=ROLES):
        self.factory = FakeFactory(members)
        direct = DirectMongoReader(self.data, entries=lambda: [ENTRY] if entries is None else entries, roles=lambda _instance: roles,
                                   config=self.main.config, client_factory=self.factory, wall=lambda: WALL)
        self.main.attach_mongo(direct)
        return direct


class BuildDataTests(unittest.TestCase):
    def test_connections_become_a_das_snapshot_without_internal_threads_our_own_operations_or_probes_as_running_work(self):
        data = build_data(DOCS, WALL)
        self.assertEqual((data["Source"], data["Timestamp"], data["SessionStat"]), ("mongo", 1_792_681_739_000, {"ActiveCount": 1}))
        items = {item["Desc"]: item for item in data["SessionList"]}
        self.assertEqual(sorted(items), ["conn1", "conn10", "conn11", "conn12"])             # BackgroundSync and our own aggregation are not listed
        idle, work, probe = items["conn1"], items["conn10"], items["conn11"]
        self.assertEqual((items["conn12"]["Active"], items["conn12"]["Ns"]), (False, "local.oplog.rs"))   # replication tailing is a connection, not work
        self.assertEqual((idle["OpId"], idle["Active"], idle["Driver"], idle["Client"]), (None, False, "mongo-go-driver@2.4.0", "10.0.0.2:53784"))
        self.assertEqual((work["OpId"], work["Active"], work["SecsRunning"], work["Ns"], work["Op"]), (77, True, 45, "appdb.orders", "command"))
        self.assertEqual(json.loads(work["Command"])["aggregate"], "orders")
        self.assertEqual((probe["OpId"], probe["Active"], probe["Driver"]), (78, False, "mongo-java-driver|sync@5.1.3"))   # a monitor stream, not work

    def test_the_mongo_view_flags_only_the_real_long_operation(self):
        view = build_mongo_session_view(build_data(DOCS, WALL), WALL * 1_000_000, THRESHOLDS)
        self.assertEqual((view["overview"]["total"], view["overview"]["running"], view["overview"]["abnormal"]), (4, 1, 1))
        flagged = [row for row in view["sessions"] if row["kinds"]]
        self.assertEqual([(row["session_id"], row["connection"], row["kinds"]) for row in flagged], [(77, "conn10", ["long_running"])])
        probe = next(row for row in view["sessions"] if row["connection"] == "conn11")
        self.assertEqual((probe["kinds"], probe["running"], probe["protected"]), ([], False, "驱动探活 / 认证"))      # 120 s of a hello stream is not abnormal

    def test_a_truncated_command_and_missing_fields_do_not_break_the_snapshot(self):
        data = build_data([doc("conn5", 9, True, "query", "a.b", 1, {"$truncated": "{find: x, filter: ..."}, driver=None), {"desc": "conn6"}], WALL)
        first, second = data["SessionList"]
        self.assertEqual(json.loads(first["Command"]), {"$truncated": "{find: x, filter: ..."})
        self.assertEqual((first["Driver"], second["OpId"], second["Command"], second["SecsRunning"]), ("", None, "", 0))


class ReadTests(ReaderCase):
    def test_a_member_is_read_with_the_approved_identity_over_a_direct_connection_and_the_client_is_kept(self):
        direct = self.reader({HOST1: primary(), HOST2: secondary()})
        data = direct.read(INSTANCE, "1001")
        client = self.factory.made[0]
        kwargs = client.kwargs
        self.assertEqual((kwargs["host"], kwargs["port"], kwargs["username"], kwargs["authSource"], kwargs["directConnection"], kwargs["tls"], kwargs["appname"]),
                         (HOST1, 3717, "reader", "admin", True, False, APP_NAME))
        self.assertEqual((kwargs["serverSelectionTimeoutMS"], kwargs["socketTimeoutMS"]), (3000, 8000))
        self.assertEqual(client.commands, ["connectionStatus", "hello"])
        pipeline, options = client.pipelines[0]
        self.assertEqual(pipeline, PIPELINE)
        self.assertEqual(pipeline[0], {"$currentOp": {"allUsers": True, "idleConnections": True}})
        self.assertEqual(options, {"maxTimeMS": 5000})
        self.assertEqual((data["Direct"]["role"], data["Direct"]["documents"], len(data["SessionList"])), ("primary", 6, 4))
        direct.read(INSTANCE, "1001")
        self.assertEqual((len(self.factory.made), client.commands.count("connectionStatus")), (1, 1))         # authenticated once, then reused

    def test_every_read_leaves_only_reads(self):
        direct = self.reader({HOST1: primary(), HOST2: secondary()})
        direct.read(INSTANCE, "1001")
        direct.read(INSTANCE, "1002")
        for client in self.factory.made:
            self.assertTrue(set(client.commands) <= {"connectionStatus", "hello"})
            self.assertTrue(all(stage.keys() <= {"$currentOp", "$project"} for pipeline, _ in client.pipelines for stage in pipeline))

    def test_an_identity_that_is_not_the_approved_one_is_refused_and_not_kept(self):
        for identity in ({"authInfo": {"authenticatedUsers": [{"user": "someone", "db": "admin"}], "authenticatedUserRoles": [{"role": "read", "db": "admin"}]}},
                         {"authInfo": {"authenticatedUsers": [{"user": "reader", "db": "admin"}], "authenticatedUserRoles": [{"role": "read", "db": "admin"}, {"role": "dbAdmin", "db": "admin"}]}},
                         {"authInfo": {"authenticatedUsers": [{"user": "reader", "db": "admin"}], "authenticatedUserRoles": []}}):
            direct = self.reader({HOST1: {**primary(), "identity": identity}})
            with self.assertRaises(DirectSessionError) as caught:
                direct.read(INSTANCE, "1001")
            self.assertEqual(caught.exception.code, "direct_wrong_identity")
            self.assertTrue(self.factory.made[0].closed)
            self.assertEqual(self.factory.made[0].pipelines, [])                                           # nothing was read with it

    def test_a_member_whose_real_role_is_not_the_registered_one_is_refused(self):
        direct = self.reader({HOST1: secondary(), HOST2: primary()})
        for node in ("1001", "1002"):                                                                       # roles swapped since the node book was read
            with self.assertRaises(DirectSessionError) as caught:
                direct.read(INSTANCE, node)
            self.assertEqual(caught.exception.code, "direct_wrong_node")

    def test_two_nodes_answering_as_one_member_is_a_routing_failure(self):
        direct = self.reader({HOST1: primary("same:27017"), HOST2: secondary("same:27017")})
        direct.read(INSTANCE, "1001")
        direct.read(INSTANCE, "1001")
        with self.assertRaises(DirectSessionError) as caught:
            direct.read(INSTANCE, "1002")
        self.assertEqual(caught.exception.code, "direct_wrong_node")

    def test_a_failed_read_drops_the_client_and_the_next_read_connects_again(self):
        direct = self.reader({HOST1: {**primary(), "fail_aggregate": True}})
        with self.assertRaises(DirectSessionError) as caught:
            direct.read(INSTANCE, "1001")
        self.assertEqual(caught.exception.code, "direct_query_failed")
        self.assertTrue(self.factory.made[0].closed)
        self.factory.members[HOST1] = primary()
        direct.read(INSTANCE, "1001")
        self.assertEqual(len(self.factory.made), 2)

    def test_nothing_listed_is_not_a_snapshot(self):
        direct = self.reader({HOST1: {**primary(), "docs": []}})
        with self.assertRaises(DirectSessionError) as caught:
            direct.read(INSTANCE, "1001")
        self.assertEqual(caught.exception.code, "direct_empty")

    def test_the_hidden_member_and_members_without_an_address_have_no_direct_path(self):
        direct = self.reader({HOST1: primary(), HOST2: secondary()})
        self.assertEqual([direct.supports(INSTANCE, node) for node in ("1001", "1002", "1003", "9999")], [True, True, False, False])
        with self.assertRaises(DirectSessionError) as caught:
            direct.read(INSTANCE, "1003")
        self.assertEqual(caught.exception.code, "direct_no_address")
        self.assertEqual(self.factory.made, [])

    def test_the_account_must_be_the_registered_read_only_one(self):
        direct = self.reader({HOST1: primary()})
        self.secret.write_text(json.dumps({"username": "someone-else", "password": "x"}), encoding="utf-8")
        with self.assertRaises(DirectSessionError) as caught:
            direct.read(INSTANCE, "1001")
        self.assertEqual(caught.exception.code, "direct_no_credential")
        self.secret.unlink()
        with self.assertRaises(DirectSessionError) as caught:
            direct.read(INSTANCE, "1001")
        self.assertEqual(caught.exception.code, "direct_no_credential")
        with self.assertRaises(DirectSessionError) as caught:
            self.reader({HOST1: primary()}, entries=[]).read(INSTANCE, "1001")
        self.assertEqual(caught.exception.code, "direct_no_credential")
        self.assertEqual(self.factory.made, [])

    @unittest.skipIf(os.name == "nt", "file modes are not meaningful on Windows")
    def test_a_credential_file_others_can_read_is_refused(self):
        direct = self.reader({HOST1: primary()})
        os.chmod(self.secret, 0o644)
        with self.assertRaises(DirectSessionError) as caught:
            direct.read(INSTANCE, "1001")
        self.assertEqual(caught.exception.code, "direct_no_credential")


class DispatchTests(ReaderCase):
    def test_the_shared_reader_hands_dds_instances_to_the_mongo_reader(self):
        self.assertFalse(self.main.enabled_for(INSTANCE))                                                   # no Mongo reader attached yet
        self.reader({HOST1: primary(), HOST2: secondary()})
        self.assertTrue(self.main.enabled_for(INSTANCE))
        self.assertFalse(self.main.enabled_for("dds-other"))
        self.assertEqual([self.main.supports(INSTANCE, node) for node in ("1001", "1003")], [True, False])
        self.assertTrue(self.main.supports("rm-any", "rn-any"))                                              # MySQL nodes always have the path
        self.assertEqual(self.main.read(INSTANCE, "1002")["Source"], "mongo")

    def test_a_dds_entry_needs_no_address_and_other_entries_still_do(self):
        from app.direct_sessions import load_config
        (self.data / CONFIG_NAME).write_text(json.dumps({"enabled": True, "instances": {INSTANCE: {}, "rm-x": {}}}), encoding="utf-8")
        with self.assertLogs("app.direct_sessions", "WARNING"):
            self.assertFalse(load_config(self.data)["enabled"])
        (self.data / CONFIG_NAME).write_text(json.dumps({"enabled": True, "instances": {INSTANCE: {}, "rm-x": {"primaryHost": "rm-x.mysql.example.invalid"}}}), encoding="utf-8")
        self.assertTrue(load_config(self.data)["enabled"])

    def test_switching_direct_reads_off_closes_the_connections_to_the_members(self):
        self.reader({HOST1: primary(), HOST2: secondary()})
        self.main.read(INSTANCE, "1001")
        client = self.factory.made[0]
        self.assertFalse(client.closed)
        (self.data / CONFIG_NAME).write_text(json.dumps({"enabled": False, "instances": {INSTANCE: {}}, "cacheSeconds": 4}), encoding="utf-8")
        self.assertFalse(self.main.enabled_for(INSTANCE))
        self.assertTrue(client.closed)


class LiveSessionsTests(ReaderCase):
    def live(self, members, das=None):
        self.reader(members)
        clock = Clock()
        client = das or Client({"Success": True, "Data": {"SessionList": [], "Timestamp": 1}})
        return LiveSessions(lambda _instance: client, clock=clock, sleep=clock.sleep, wall_us=lambda: 5, direct=self.main), clock, client

    def test_a_member_is_read_directly_and_says_so(self):
        live, _, das = self.live({HOST1: primary(), HOST2: secondary()})
        out = live.view(INSTANCE, "1001", lambda data, now: build_mongo_session_view(data, now, THRESHOLDS))
        self.assertEqual((out["source"], out["cached"], out["overview"]["abnormal"]), ("mongo", False, 1))
        self.assertEqual(das.calls, [])
        status = live.direct_status()
        self.assertEqual((status["enabled"], status["instances"]), (True, [INSTANCE]))

    def test_the_hidden_member_is_read_from_das_without_a_warning(self):
        das = Client({"Success": True, "Data": {"SessionList": [{"Desc": "TTLMonitor", "Active": False, "ConnectionId": 0}], "Timestamp": 1}})
        live, _, das = self.live({HOST1: primary(), HOST2: secondary()}, das)
        with self.assertNoLogs("app.live_sessions", "WARNING"):
            out = live.view(INSTANCE, "1003", lambda data, now: build_mongo_session_view(data, now, THRESHOLDS))
        self.assertEqual((out["source"], len(das.calls), live.direct_supports(INSTANCE, "1003"), live.direct_supports(INSTANCE, "1001")), ("das", 1, False, True))
        self.assertTrue(live.direct_enabled(INSTANCE))
        self.assertFalse(live.direct_enabled("rm-x"))

    def test_a_stored_snapshot_is_served_when_the_caller_accepts_its_age_and_only_then(self):
        das = Client({"Success": True, "Data": {"SessionList": [], "Timestamp": 1}})
        live, clock, das = self.live({HOST1: primary(), HOST2: secondary()}, das)
        build = lambda data, now: {"status": "ok", "n": len(data["SessionList"])}  # noqa: E731
        first = live.view(INSTANCE, "1003", build)
        self.assertEqual((first["cached"], "stale_seconds" in first, len(das.calls)), (False, False, 1))
        clock.now += 40                                                                                      # DAS answers are cached for 10 s
        strict = live.view(INSTANCE, "1003", build)
        self.assertEqual((strict["cached"], len(das.calls)), (False, 2))                                      # no max_age: a fresh read
        clock.now += 40
        relaxed = live.view(INSTANCE, "1003", build, "auto", 90.0)
        self.assertEqual((relaxed["cached"], relaxed["stale_seconds"], len(das.calls)), (True, 40, 2))        # the 40 s old snapshot, no DAS call
        clock.now += 60
        too_old = live.view(INSTANCE, "1003", build, "auto", 90.0)
        self.assertEqual((too_old["cached"], "stale_seconds" in too_old, len(das.calls)), (False, False, 3))  # 100 s old: read again


if __name__ == "__main__":
    unittest.main()
