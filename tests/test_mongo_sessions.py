"""MongoDB session management: members from the cloud, one view per GetMongoDBCurrentOp answer, the guarded killOp text, and
the RDS service / reader / retention loop routing a dds- instance to them. Payloads follow real DAS / DDS answers (2026-10-22)."""
import json
import tempfile
import unittest
from pathlib import Path

from app.live_sessions import LiveSessions, LiveSessionsError
from app.mongo_sessions import (MongoNodeBook, build_mongo_kill_statements, build_mongo_session_view, command_text, parse_members,
                                protected_reason)
from app.session_collector import DEFAULT_CONFIG, SessionCollector
from app.session_service import SessionRequestError, SessionService
from app.session_store import SessionStore

SEC = 1_000_000
T0 = 1_792_681_739_190_000
INSTANCE = "dds-example0000000"
HOST1 = "dds-example00000001.mongodb.cn-example.rds.aliyuncs.com"
HOST2 = "dds-example00000002.mongodb.cn-example.rds.aliyuncs.com"
THRESHOLDS = {"blocking_s": 30, "trx_s": 30, "idle_trx_s": 10}

ZONES = {"ZoneInfos": {"ZoneInfo": [
    {"ZoneId": "cn-example-a", "RoleType": "Secondary", "NodeType": "normal", "InsName": INSTANCE, "RoleId": "100000002"},
    {"ZoneId": "cn-example-a", "RoleType": "Hidden", "NodeType": "normal", "InsName": INSTANCE, "RoleId": "100000003"},
    {"ZoneId": "cn-example-a", "RoleType": "Primary", "NodeType": "normal", "InsName": INSTANCE, "RoleId": "100000001"}]}}
REPLICA_SETS = {"DBInstanceId": INSTANCE, "ReplicaSets": {"ReplicaSet": [
    {"ReplicaSetRole": "Primary", "ConnectionDomain": HOST1, "NetworkType": "VPC", "RoleId": "100000001"},
    {"ReplicaSetRole": "Secondary", "ConnectionDomain": "dds-example00000002452-pub.mongodb.cn-example.rds.aliyuncs.com",
     "NetworkType": "Public", "RoleId": "100000002"},
    {"ReplicaSetRole": "Secondary", "ConnectionDomain": HOST2, "NetworkType": "VPC", "RoleId": "100000002"}]}}
MEMBERS = parse_members(ZONES, REPLICA_SETS, INSTANCE)


def op(opid, seconds=0, *, desc="conn1791249", ns="appdb.events_54", active=True, command=None,
       driver="mongo-go-driver@2.4.0", client="10.0.0.2:53784", plan=""):
    item = {"Op": "command", "Ns": ns, "Driver": driver, "Desc": desc, "Active": active, "ConnectionId": 1791249, "OpId": str(opid),
            "Client": client, "Command": json.dumps(command if command is not None else {
                "lsid": {"id": {"$binary": {"base64": "DNZ", "subType": "04"}}}, "$db": "appdb", "$clusterTime": {"signature": {}},
                "aggregate": "orders_70", "pipeline": [{"$match": {"shop_id": 38870}}]})}
    if seconds:
        item["SecsRunning"] = seconds
    if plan:
        item["PlanSummary"] = plan
    return item


def das(*ops):
    return {"SessionStat": {"TotalCount": len(ops), "ActiveCount": sum(1 for o in ops if o.get("Active"))}, "Timestamp": T0 // 1000,
            "SessionList": list(ops)}


class MembersTests(unittest.TestCase):
    def test_roles_zones_and_only_the_vpc_address(self):
        self.assertEqual(MEMBERS, {
            "100000001": {"role": "primary", "zone": "cn-example-a", "host": HOST1},
            "100000002": {"role": "secondary", "zone": "cn-example-a", "host": HOST2},
            "100000003": {"role": "hidden", "zone": "cn-example-a", "host": ""}})

    def test_an_answer_for_another_instance_never_labels_these_members(self):
        with self.assertRaises(ValueError):
            parse_members(ZONES, {**REPLICA_SETS, "DBInstanceId": "dds-other"}, INSTANCE)
        with self.assertRaises(ValueError):
            parse_members(ZONES, REPLICA_SETS, "dds-other")

    def test_the_book_lists_configured_instances_primary_first(self):
        calls = []

        class Client:
            def __init__(self, settings, credential, timeout):
                calls.append((settings.db_instance_id, settings.region_id, settings.endpoint))

            def call(self, action, params):
                return ZONES if action == "DescribeRoleZoneInfo" else REPLICA_SETS

        from dataclasses import dataclass

        @dataclass
        class S:
            credential_target: str = "ecs"
            db_instance_id: str = ""
            region_id: str = ""
            endpoint: str = ""

        entries = [{"instanceId": INSTANCE, "label": "demo-mongo", "region": "cn-example", "enabled": True},
                   {"instanceId": "dds-off", "region": "cn-example", "enabled": False}]
        book = MongoNodeBook(lambda: S(), lambda target: object(), lambda: entries, Client)
        book.refresh()
        self.assertEqual(calls, [(INSTANCE, "cn-example", "https://mongodb.aliyuncs.com")])
        listed = book.instances()
        self.assertEqual([(item["instance_id"], item["label"]) for item in listed], [(INSTANCE, "demo-mongo")])
        self.assertEqual([(node["node_id"], node["role"]) for node in listed[0]["nodes"]],
                         [("100000001", "primary"), ("100000002", "secondary"), ("100000003", "hidden")])
        self.assertEqual(sorted(book.pairs()), sorted((INSTANCE, node) for node in MEMBERS))


class ViewTests(unittest.TestCase):
    def test_operations_become_session_rows(self):
        view = build_mongo_session_view(das(op(503368306, 45, plan="IXSCAN { shop_id: 1 }"), op(-1177661433, 218, desc="conn2224778"),
                                            op(1, 0)), T0, THRESHOLDS)
        self.assertEqual((view["engine"], view["observed_at_us"]), ("mongo", T0 // 1000 * 1000))
        self.assertEqual(view["overview"]["abnormal"], 2)
        self.assertEqual(view["overview"]["max_active_seconds"], 218)
        first = view["sessions"][0]
        self.assertEqual((first["session_id"], first["connection"], first["seconds"], first["kinds"]), (-1177661433, "conn2224778", 218, ["long_running"]))
        row = next(r for r in view["sessions"] if r["session_id"] == 503368306)
        self.assertEqual((row["user"], row["client"], row["client_port"], row["database"], row["state"], row["command_name"]),
                         ("mongo-go-driver@2.4.0", "10.0.0.2", "53784", "appdb.events_54", "IXSCAN { shop_id: 1 }", "aggregate"))
        self.assertNotIn("lsid", row["sql"])
        self.assertNotIn("$clusterTime", row["sql"])
        self.assertEqual(view["stats"]["client"][0], {"key": "10.0.0.2", "total": 3, "active": 3, "abnormal": 2})

    def test_internal_threads_and_probes_are_protected_and_never_abnormal(self):
        view = build_mongo_session_view(das({"Desc": "TTLMonitor", "Active": False, "ConnectionId": 0},
                                            op(7, 120, command={"hello": 1, "maxAwaitTimeMS": 10000}),
                                            op(8, 120, ns="local.oplog.rs"),
                                            op(9, 120, driver="NetworkInterfaceTL-ReplNetwork")), T0, THRESHOLDS)
        reasons = {row["session_id"]: row["protected"] for row in view["sessions"]}
        self.assertEqual(reasons, {None: "没有 opid", 7: "驱动探活 / 认证", 8: "副本集内部连接（复制 / 心跳）", 9: "副本集内部连接（复制 / 心跳）"})
        self.assertEqual([row["session_id"] for row in view["sessions"] if row["kinds"]], [8, 9])
        self.assertEqual(view["overview"]["total"], 4)
        self.assertEqual(view["overview"]["active"], 3)

    def test_truncated_commands_keep_their_text_and_name(self):
        text, name = command_text(json.dumps({"$truncated": '{ aggregate: "orders_70", pipeline: [ ...'}))
        self.assertTrue(text.startswith("{ aggregate:"))
        self.assertEqual(name, "aggregate")
        self.assertEqual(command_text("not json"), ("not json", ""))

    def test_opids_outside_32_bits_are_not_opids(self):
        view = build_mongo_session_view(das(op("shard01:123", 50), op(2**31, 50), op(-(2**31), 50)), T0, THRESHOLDS)
        self.assertEqual(sorted(str(row["session_id"]) for row in view["sessions"]), sorted(["None", "None", str(-(2**31))]))


class KillTextTests(unittest.TestCase):
    def node(self, role_id):
        return {"node_id": role_id, **MEMBERS[role_id]}

    def rows(self, *pairs):
        return [{"session_id": opid, "connection": desc, "database": "appdb.x", "user": "mongo-go-driver", "command_name": "find"}
                for opid, desc in pairs]

    def test_one_check_and_one_guarded_kill_line_per_member(self):
        result = build_mongo_kill_statements(self.node("100000002"), self.rows((-1177661433, "conn2224778"), (5, "conn9"), (5, "conn9")),
                                             snapshot_us=T0, generated_us=T0 + SEC)
        self.assertEqual(result["session_ids"], [-1177661433, 5])
        self.assertEqual(len(result["checks"]), 1)
        self.assertIn("{$match: {opid: {$in: [-1177661433, 5]}}}", result["checks"][0])
        self.assertEqual(len(result["statements"]), 1)
        kill = result["statements"][0]
        self.assertTrue(kill.startswith('[[-1177661433, "conn2224778"], [5, "conn9"]].forEach('))
        self.assertIn("{$match: {opid: op, desc: desc}}", kill)
        self.assertIn("admin.killOp(op)", kill)
        self.assertEqual(result["host"], HOST2)
        self.assertIn(f"mongodb://{HOST2}:3717/admin?directConnection=true", result["connect"])

    def test_nothing_a_client_sent_reaches_the_text(self):
        with self.assertRaises(ValueError):
            build_mongo_kill_statements(self.node("100000001"), [{"session_id": "1); db.dropDatabase(", "connection": "conn1"}],
                                        snapshot_us=T0, generated_us=T0)
        result = build_mongo_kill_statements(self.node("100000001"), self.rows((3, 'conn1"]); db.dropDatabase(); (["')),
                                             snapshot_us=T0, generated_us=T0)
        self.assertEqual((result["text"], result["skipped"]), ("", [{"session_id": 3, "reason": "内部线程"}]))

    def test_protected_operations_and_the_hidden_member_get_no_kill(self):
        rows = self.rows((1, "conn1")) + [{"session_id": 2, "connection": "conn2", "database": "local.oplog.rs", "user": "", "command_name": "getMore"}]
        result = build_mongo_kill_statements(self.node("100000001"), rows, snapshot_us=T0, generated_us=T0)
        self.assertEqual(result["session_ids"], [1])
        self.assertEqual(result["skipped"], [{"session_id": 2, "reason": "副本集内部连接（复制 / 心跳）"}])
        hidden = build_mongo_kill_statements(self.node("100000003"), self.rows((1, "conn1")), snapshot_us=T0, generated_us=T0)
        self.assertEqual(hidden["text"], "")
        self.assertIn("DAS 控制台", hidden["skipped"][0]["reason"])
        self.assertEqual(protected_reason(1, "conn1", "appdb.a", "go", "saslStart"), "驱动探活 / 认证")


class FakeClient:
    def __init__(self, answers):
        self.answers, self.calls = answers, []

    def call(self, action, params):
        self.calls.append((action, dict(params)))
        answer = self.answers[params["NodeId"]]
        if isinstance(answer, BaseException):
            raise answer
        return answer


class RoutingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.client = FakeClient({"100000001": {"Data": das(op(11, 40)), "Success": True, "Code": 200},
                                  "100000002": {"Data": das(op(12, 218, desc="conn2224778")), "Success": True},
                                  "100000003": {"Data": das({"Desc": "TTLMonitor", "Active": False, "ConnectionId": 0}), "Success": True}})
        self.live = LiveSessions(lambda instance: self.client, sleep=lambda s: None)
        self.store = SessionStore(Path(self.tmp.name) / "s.sqlite3")
        pairs = [("rm-x", "rn-p")] + [(INSTANCE, node) for node in MEMBERS]
        roles = lambda instance: MEMBERS if instance == INSTANCE else {"rn-p": {"role": "primary", "zone": "a"}}  # noqa: E731
        self.collector = SessionCollector(self.live, self.store, config_loader=lambda: dict(DEFAULT_CONFIG), nodes=lambda: pairs,
                                          default_instances=lambda: ["rm-x", INSTANCE], wall_us=lambda: T0)
        self.service = SessionService(self.live, self.store, self.collector, nodes=lambda: pairs, roles=roles,
                                      mongo_instances=lambda: [{"instance_id": INSTANCE, "nodes": []}], wall_us=lambda: T0)

    def test_the_reader_asks_one_member_synchronously(self):
        data = self.live._read(INSTANCE, "100000002")
        self.assertEqual(self.client.calls, [("GetMongoDBCurrentOp", {"InstanceId": INSTANCE, "NodeId": "100000002"})])
        self.assertEqual(data["SessionList"][0]["OpId"], "12")

    def test_a_refused_read_is_an_error_with_a_reason(self):
        self.client.answers["100000002"] = RuntimeError("Throttling.User: Request was denied due to user flow control.")
        with self.assertRaises(LiveSessionsError) as caught:
            self.live._read(INSTANCE, "100000002")
        self.assertEqual(caught.exception.code, "das_throttled")
        self.client.answers["100000002"] = {"Success": False, "Message": "InvalidParam"}
        with self.assertRaises(LiveSessionsError) as caught:
            self.live._read(INSTANCE, "100000002")
        self.assertEqual(caught.exception.code, "das_failed")

    def test_overview_reads_every_member_primary_first_with_the_mongo_view(self):
        result = self.service.overview(INSTANCE, "", THRESHOLDS)
        self.assertEqual(result["engine"], "mongo")
        self.assertEqual([(n["node_id"], n["role"], n["host"]) for n in result["nodes"]],
                         [("100000001", "primary", HOST1), ("100000002", "secondary", HOST2), ("100000003", "hidden", "")])
        self.assertEqual([n["overview"]["abnormal"] for n in result["nodes"]], [1, 1, 0])

    def test_an_unknown_member_is_refused_before_anything_is_read(self):
        with self.assertRaises(SessionRequestError):
            self.service.overview(INSTANCE, "999999999", THRESHOLDS)
        self.assertEqual(self.client.calls, [])

    def test_kill_text_for_two_members_keeps_them_apart(self):
        result = self.service.kill_statements({"instance": INSTANCE, "mode": "connection", "nodes": [
            {"node_id": "100000002", "snapshot_us": T0, "sessions": [{"session_id": 12, "connection": "conn2224778", "database": "appdb.a"}]},
            {"node_id": "100000001", "snapshot_us": T0, "sessions": [{"session_id": 11, "connection": "conn1791249", "database": "appdb.a"}]}]})
        self.assertEqual((result["engine"], result["mode"], result["kills"]), ("mongo", "killop", 2))
        self.assertEqual([(n["node_id"], n["host"]) for n in result["nodes"]], [("100000001", HOST1), ("100000002", HOST2)])
        self.assertTrue(result["text"].startswith(f"// primary 100000001: mongosh \"mongodb://{HOST1}"))
        self.assertTrue(all("forEach" in n["text"] and "$currentOp" in n["text"] for n in result["nodes"]))

    def test_status_lists_the_mongo_instances(self):
        self.assertEqual(self.service.status()["mongo"], [{"instance_id": INSTANCE, "nodes": []}])
        broken = SessionService(self.live, self.store, self.collector, nodes=lambda: [], roles=lambda i: {}, mongo_instances=lambda: 1 / 0)
        self.assertEqual(broken.status()["mongo"], [])

    def test_retention_keeps_abnormal_operations_of_every_member(self):
        read = self.collector.run_pass(dict(DEFAULT_CONFIG), [pair for pair in self.collector.targets(dict(DEFAULT_CONFIG)) if pair[0] == INSTANCE])
        self.assertEqual(read, 3)
        history = self.store.history(INSTANCE, "", T0 - 60 * SEC, T0 + 60 * SEC)
        self.assertEqual(sorted((row["node_id"], row["session_id"], row["kinds"]) for row in history["sessions"]),
                         [("100000001", 11, ["long_running"]), ("100000002", 12, ["long_running"])])
        self.assertEqual(len(history["coverage"]), 3)


if __name__ == "__main__":
    unittest.main()
