"""Session management view: DAS's three abnormal-session kinds, statistics, lock-holder candidates and KILL statements."""
import unittest
from datetime import UTC, datetime

from app.session_view import (DEFAULT_THRESHOLDS, HOLDER_LIMIT, LIST_ACTIVE_LIMIT, MAX_KILL_SESSIONS, build_kill_statements,
                              build_session_view, is_lock_wait, parse_thresholds, protected_reason, route_hint)

MDL = "Waiting for table metadata lock"


def item(session_id, command="Sleep", seconds=0, user="app", client="10.0.0.1", database="shop", state="", sql="",
         trx=None, trx_seconds=0):
    row = dict(SessionId=session_id, User=user, Client=client, DbName=database, Command=command, Time=seconds, State=state, SqlText=sql)
    if trx is not None:
        row.update(TrxId=trx, TrxDuration=trx_seconds)
    return row


def snapshot(*sessions, total=None, stamp=1_790_000_000_000):
    return {"IsFinish": True, "SessionData": {"SessionList": list(sessions), "TotalSessionCount": total or len(sessions), "TimeStamp": stamp}}


def view(*sessions, total=None, **kw):
    return build_session_view(snapshot(*sessions, total=total), 7, **kw)


def by_id(result):
    return {row["session_id"]: row for row in result["sessions"]}


class AbnormalKindTests(unittest.TestCase):
    def test_the_three_das_kinds_with_their_thresholds(self):
        result = view(
            item(1, "Query", 31, sql="SELECT slow"),                                            # a statement over 30 s
            item(2, "Query", 30, sql="SELECT edge"),                                            # exactly 30 s is not over
            item(3, "Sleep", 0, trx="9", trx_seconds=31),                                       # a transaction over 30 s
            item(4, "Sleep", 11, trx="8", trx_seconds=11),                                      # open, silent for more than 10 s
            item(5, "Sleep", 10, trx="7", trx_seconds=10),                                      # not yet
            item(6, "Sleep", 900),                                                              # idle without a transaction: fine
            item(7, "Query", 5, sql="SELECT quick"))
        rows = by_id(result)
        self.assertEqual({sid: row["kinds"] for sid, row in rows.items() if row["kinds"]},
                         {1: ["long_running"], 3: ["long_trx"], 4: ["idle_trx"]})
        self.assertEqual(result["overview"]["abnormal"], 3)
        self.assertEqual(result["overview"]["kinds"], {"long_running": 1, "lock_wait": 0, "long_trx": 1, "idle_trx": 1})

    def test_a_session_can_be_abnormal_in_several_ways(self):
        row = by_id(view(item(1, "Sleep", 600, trx="5", trx_seconds=900)))[1]
        self.assertEqual(row["kinds"], ["long_trx", "idle_trx"])
        waiting = by_id(view(item(2, "Query", 40, state=MDL, sql="ALTER TABLE t ADD INDEX i (a)")))[2]
        self.assertEqual(waiting["kinds"], ["long_running", "lock_wait"])

    def test_thresholds_can_be_changed_and_are_reported(self):
        result = view(item(1, "Query", 12), thresholds={"blocking_s": 10, "trx_s": 30, "idle_trx_s": 10})
        self.assertEqual(by_id(result)[1]["kinds"], ["long_running"])
        self.assertEqual(result["thresholds"]["blocking_s"], 10)
        self.assertEqual(view(item(1, "Query", 12))["thresholds"], DEFAULT_THRESHOLDS)

    def test_a_transaction_id_of_zero_or_nothing_is_no_transaction(self):
        rows = by_id(view(item(1, "Sleep", 50, trx="0", trx_seconds=50), item(2, "Sleep", 50, trx="", trx_seconds=50),
                          item(3, "Sleep", 50, trx=None)))
        for row in rows.values():
            self.assertEqual((row["kinds"], row["trx_id"], row["trx_seconds"], row["active"]), ([], "", 0, False))

    def test_active_is_a_running_command_or_an_open_transaction(self):
        rows = by_id(view(item(1, "Query", 0), item(2, "Execute", 0), item(3, "Sleep", 1, trx="4", trx_seconds=1), item(4, "Sleep", 1),
                          item(5, "Binlog Dump GTID", 99999)))
        self.assertEqual({sid: row["active"] for sid, row in rows.items()}, {1: True, 2: True, 3: True, 4: False, 5: False})

    def test_lock_wait_states(self):
        for state in (MDL, "Waiting for global read lock", "Waiting for backup lock", "Waiting for table flush", "waiting for schema metadata lock"):
            self.assertTrue(is_lock_wait(state), state)
        for state in ("executing", "Sending data", "Waiting for handler commit", "", "starting"):
            self.assertFalse(is_lock_wait(state), state)

    def test_a_snapshot_without_transaction_fields_means_no_transaction_not_a_missing_feature(self):
        # DAS omits TrxId / TrxDuration for a session in no transaction; a whole node without them is simply a quiet node
        rows = by_id(view(item(1, "Query", 1), item(2, "Sleep", 5)))
        self.assertEqual([row["kinds"] for row in rows.values()], [[], []])


class StatisticsTests(unittest.TestCase):
    def test_totals_actives_and_abnormals_by_user_client_and_database(self):
        result = view(item(1, "Query", 40, user="etl", client="10.0.0.2", database="dw"), item(2, "Sleep", 3, user="app", client="10.0.0.1"),
                      item(3, "Query", 1, user="app", client="10.0.0.1"), item(4, "Sleep", 50, user="app", client="10.0.0.3", trx="3", trx_seconds=50),
                      total=1066)
        users = {row["key"]: row for row in result["stats"]["user"]}
        self.assertEqual((users["app"]["total"], users["app"]["active"], users["app"]["abnormal"]), (3, 2, 1))
        self.assertEqual((users["etl"]["total"], users["etl"]["active"], users["etl"]["abnormal"]), (1, 1, 1))
        self.assertEqual([row["key"] for row in result["stats"]["user"]], ["app", "etl"])   # abnormal first, then active, then total
        self.assertEqual({row["key"] for row in result["stats"]["client"]}, {"10.0.0.1", "10.0.0.2", "10.0.0.3"})
        self.assertEqual({row["key"] for row in result["stats"]["database"]}, {"dw", "shop"})
        overview = result["overview"]
        self.assertEqual((overview["total"], overview["listed_total"], overview["active"], overview["running"]), (1066, 4, 3, 2))
        self.assertEqual(overview["max_active_seconds"], 40)

    def test_statistics_are_cut_to_the_top_and_say_how_many_more(self):
        many = [item(i, "Sleep", 1, user=f"u{i:03d}") for i in range(1, 50)]
        result = view(*many)
        self.assertEqual(len(result["stats"]["user"]), 30)
        self.assertEqual(result["stats"]["more"]["user"], 19)


class HolderTests(unittest.TestCase):
    def test_sessions_old_enough_to_hold_the_lock_are_candidates_and_always_listed(self):
        result = view(
            item(1, "Query", 3600, state=MDL, sql="ALTER TABLE t ADD INDEX i (a)"),             # the stuck DDL
            item(2, "Query", 1500, state=MDL, sql="SELECT * FROM t"),                           # queued behind it
            item(3, "Sleep", 5, trx="11", trx_seconds=4000),                                    # open transaction older than the DDL's wait
            item(4, "Sleep", 5, trx="12", trx_seconds=100),                                     # opened later: cannot be the holder
            item(5, "Query", 3700, sql="SELECT long_scan"),                                     # a running statement older than the wait
            item(6, "Sleep", 5000))                                                             # idle, no transaction: holds nothing
        rows = by_id(result)
        self.assertEqual({sid for sid, row in rows.items() if row["holder"]}, {3, 5})
        self.assertEqual((rows[3]["holder_basis"], rows[3]["holder_rank"], rows[5]["holder_basis"], rows[5]["holder_rank"]),
                         ("transaction", 1, "statement", 2))   # the oldest transaction or statement first
        self.assertEqual(result["holders"], {"waiters": 2, "oldest_wait_s": 3600, "candidates": 2})
        self.assertFalse(rows[1]["holder"] or rows[2]["holder"])          # the waiters themselves are victims
        self.assertIn(4, rows)                                              # the rest stays listed as an ordinary active session

    def test_no_waiter_means_no_candidates(self):
        result = view(item(1, "Sleep", 5, trx="4", trx_seconds=4000), item(2, "Query", 90, sql="SELECT 1"))
        self.assertEqual(result["holders"], {"waiters": 0, "oldest_wait_s": 0, "candidates": 0})
        self.assertFalse(any(row["holder"] for row in result["sessions"]))

    def test_candidates_are_capped(self):
        sessions = [item(1, "Query", 100, state=MDL, sql="ALTER TABLE t DROP COLUMN c")]
        sessions += [item(100 + i, "Sleep", 1, trx=str(i + 1), trx_seconds=500 + i) for i in range(HOLDER_LIMIT + 5)]
        result = view(*sessions)
        self.assertEqual(sum(row["holder"] for row in result["sessions"]), HOLDER_LIMIT)
        self.assertEqual(result["holders"]["candidates"], HOLDER_LIMIT)


class ListingTests(unittest.TestCase):
    def test_a_long_node_lists_all_abnormal_sessions_and_caps_the_rest(self):
        actives = [item(1000 + i, "Query", 1 + i % 20, sql=f"SELECT {i}") for i in range(LIST_ACTIVE_LIMIT + 50)]
        abnormal = [item(1, "Sleep", 60, trx="3", trx_seconds=60), item(2, "Query", 99, sql="SELECT slow")]
        result = view(*actives, *abnormal)
        listed = {row["session_id"] for row in result["sessions"]}
        self.assertTrue({1, 2} <= listed)
        self.assertTrue(result["truncated"])
        self.assertEqual(result["overview"]["listed_total"], LIST_ACTIVE_LIMIT + 52)
        self.assertEqual([row["session_id"] for row in result["sessions"][:2]], [2, 1])    # abnormal first, longest first

    def test_a_short_list_is_complete_and_sql_is_cut(self):
        long_sql = "SELECT " + "x" * 5000
        result = view(item(1, "Query", 1, sql=long_sql))
        row = result["sessions"][0]
        self.assertFalse(result["truncated"])
        self.assertEqual((len(row["sql"]), row["sql_chars"]), (2000, len(long_sql)))

    def test_the_snapshot_time_is_the_snapshots_own(self):
        self.assertEqual(view(item(1))["observed_at_us"], 1_790_000_000_000_000)
        self.assertEqual(build_session_view({"SessionData": {"SessionList": []}}, 77)["observed_at_us"], 77)

    def test_a_missing_session_id_is_listed_but_protected(self):
        rows = view({"Command": "Query", "Time": "oops"})["sessions"]
        self.assertEqual((rows[0]["session_id"], rows[0]["seconds"], rows[0]["protected"]), (None, 0, "没有会话号"))


class ThresholdParsingTests(unittest.TestCase):
    def test_defaults_overrides_and_refusals(self):
        self.assertEqual(parse_thresholds({}), DEFAULT_THRESHOLDS)
        self.assertEqual(parse_thresholds({"blockingSeconds": "60", "idleTrxSeconds": " 5 "}), {"blocking_s": 60, "trx_s": 30, "idle_trx_s": 5})
        for bad in ("0", "-1", "86401", "abc", "1.5"):
            with self.assertRaises(ValueError):
                parse_thresholds({"trxSeconds": bad})


class ProtectionTests(unittest.TestCase):
    def test_replication_system_and_cloud_internal_sessions_are_never_killable(self):
        self.assertEqual(protected_reason("repl", "Binlog Dump GTID", 5), "复制线程（Binlog Dump）")
        self.assertEqual(protected_reason("system user", "Query", 5), "复制 / 系统线程")
        self.assertEqual(protected_reason("root", "Daemon", 5), "复制 / 系统线程")
        self.assertEqual(protected_reason("event_scheduler", "Daemon", 5), "复制 / 系统线程")   # the command is checked first
        self.assertEqual(protected_reason("event_scheduler", "Query", 5), "事件调度线程")
        self.assertEqual(protected_reason("aliyun_root", "Query", 5), "云内部账号")
        self.assertEqual(protected_reason("aliyun_dba_x", "Sleep", 5), "云内部账号")
        self.assertEqual(protected_reason("root", "Sleep", 5), "")          # the application's own account is an ordinary one
        self.assertEqual(protected_reason("app", "Query", None), "没有会话号")


class KillStatementTests(unittest.TestCase):
    NODE = {"node_id": "rn-lagging", "role": "secondary", "zone": "cn-chengdu-b"}

    def rows(self):
        return [dict(session_id=312, user="app", client="10.0.0.1", database="shop", command="Sleep", seconds=905, trx_seconds=905,
                     kinds=["long_trx", "idle_trx"]),
                dict(session_id=44, user="etl", client="10.0.0.9", database="dw", command="Query", seconds=70, trx_seconds=0, kinds=["long_running"])]

    def test_the_text_checks_first_then_kills_in_id_order_with_a_comment_per_session(self):
        out = build_kill_statements(self.NODE, self.rows(), mode="connection", snapshot_us=1_790_000_000_000_000, generated_us=1_790_000_060_000_000)
        self.assertEqual(out["statements"], ["/*FORCE_NODE='rn-lagging'*/ KILL 44;", "/*FORCE_NODE='rn-lagging'*/ KILL 312;"])
        self.assertEqual(out["session_ids"], [44, 312])
        text = out["text"]
        self.assertIn("备节点 rn-lagging · cn-chengdu-b", text)
        self.assertIn("WHERE ID IN (44, 312);", text)
        self.assertIn("sys.schema_table_lock_waits", text)
        self.assertLess(text.index("PROCESSLIST"), text.index("KILL 44;"))                       # the check comes before any KILL
        self.assertIn("KILL 312;  -- app@10.0.0.1 db=shop Sleep 905s 事务 905s 事务过长/未提交事务", text)
        self.assertIn("KILL 44;  -- etl@10.0.0.9 db=dw Query 70s 语句运行过久", text)
        self.assertIn(f"生成于 {datetime.fromtimestamp(1_790_000_060, UTC):%Y-%m-%d %H:%M:%S} UTC · 会话快照 {datetime.fromtimestamp(1_790_000_000, UTC):%Y-%m-%d %H:%M:%S} UTC", text)

    def test_query_mode_writes_kill_query(self):
        out = build_kill_statements(self.NODE, self.rows(), mode="query", snapshot_us=1, generated_us=2)
        self.assertEqual(out["statements"], ["/*FORCE_NODE='rn-lagging'*/ KILL QUERY 44;", "/*FORCE_NODE='rn-lagging'*/ KILL QUERY 312;"])
        self.assertIn("空闲会话不受影响", out["text"])

    def test_protected_sessions_are_skipped_with_the_reason_and_duplicates_collapse(self):
        rows = self.rows() + [dict(session_id=9, user="repl", command="Binlog Dump GTID", seconds=9999),
                              dict(session_id=10, user="aliyun_root", command="Query", seconds=70), dict(self.rows()[0])]
        out = build_kill_statements(self.NODE, rows, mode="connection", snapshot_us=1, generated_us=2)
        self.assertEqual(out["session_ids"], [44, 312])
        self.assertEqual(out["skipped"], [{"session_id": 9, "reason": "复制线程（Binlog Dump）"}, {"session_id": 10, "reason": "云内部账号"}])
        self.assertIn("-- 跳过 9：复制线程（Binlog Dump），不生成 KILL", out["text"])
        self.assertNotIn("KILL 9;", out["text"])
        self.assertNotIn("KILL 10;", out["text"])

    def test_only_protected_sessions_leave_no_statement_at_all(self):
        out = build_kill_statements(self.NODE, [dict(session_id=9, user="repl", command="Binlog Dump", seconds=1)], mode="connection", snapshot_us=1, generated_us=2)
        self.assertEqual((out["statements"], out["session_ids"]), ([], []))
        self.assertEqual([line for line in out["text"].splitlines() if not line.startswith("--")], [])   # comments only: nothing to run
        self.assertNotIn("PROCESSLIST", out["text"])

    def test_a_user_name_cannot_put_a_statement_into_the_text(self):
        evil = dict(session_id=5, user="x\nKILL 1; --", client="10.0.0.1 */ DROP TABLE t; /*", database="d\r\nSELECT 1;", command="Query KILL 2;",
                    seconds=99, kinds=[])
        out = build_kill_statements({"node_id": "rn-1\nKILL 3;", "role": "primary", "zone": "z\nKILL 4;"}, [evil], mode="connection", snapshot_us=1, generated_us=2)
        allowed = ("", "/*FORCE_MASTER*/ SELECT @@server_id AS server_id, @@read_only AS read_only;", "/*FORCE_MASTER*/ SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 80) AS INFO FROM information_schema.PROCESSLIST WHERE ID IN (5);",
                   "/*FORCE_MASTER*/ SELECT * FROM sys.schema_table_lock_waits;", "/*FORCE_MASTER*/ KILL 5;")
        for line in out["text"].splitlines():
            if not line.startswith("--"):
                self.assertIn(line.split("  --")[0], allowed)
        self.assertEqual(out["statements"], ["/*FORCE_MASTER*/ KILL 5;"])
        bare = out["text"].replace("/*FORCE_MASTER*/", "")                  # the hint is the only comment the text may open
        self.assertNotIn("*/", bare)
        self.assertNotIn("/*", bare)

    def test_a_node_id_that_cannot_sit_in_a_hint_leaves_the_hint_out(self):
        out = build_kill_statements({"node_id": "rn-1'*/ KILL 3; /*", "role": "secondary", "zone": "z"}, self.rows()[:1], mode="connection", snapshot_us=1, generated_us=2)
        self.assertEqual(out["statements"], ["KILL 312;"])
        self.assertNotIn("/*", out["text"])
        self.assertNotIn("*/", out["text"])
        self.assertNotIn("FORCE_", out["text"])                            # no hint, and so no primary guard either: nothing to guard
        self.assertIn("请连到这个节点执行", out["text"])

    def test_a_standby_is_self_checked_then_checked_and_killed_with_its_node_hint(self):
        out = build_kill_statements(self.NODE, self.rows(), mode="connection", snapshot_us=1, generated_us=2)
        hint = "/*FORCE_NODE='rn-lagging'*/ "
        code = [line for line in out["text"].splitlines() if not line.startswith("--")]
        self.assertEqual(code[0], hint + "SELECT @@server_id AS server_id, @@read_only AS read_only;")
        self.assertEqual(code[1], hint + "SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 80) AS INFO FROM information_schema.PROCESSLIST WHERE ID IN (44, 312);")
        self.assertEqual(code[2], hint + "SELECT * FROM sys.schema_table_lock_waits;")
        self.assertTrue(code[3].startswith(hint + "KILL 44;") and code[4].startswith(hint + "KILL 312;"))
        self.assertEqual(len(code), 5)
        text = out["text"]
        self.assertIn("每条语句都带 Hint FORCE_NODE='rn-lagging'", text)
        self.assertIn("-- 0. 先自检路由：read_only 应为 1（备节点）", text)
        self.assertIn("FORCE_SLAVE", text)                                  # the way out when the proxy does not know the node id
        self.assertIn("-- 3. 结束会话", text)
        self.assertIn("执行后重跑第 1 步", text)
        self.assertNotIn("FORCE_MASTER", text)                              # a standby's statements never go to the primary
        self.assertLess(text.index("-- 0."), text.index("-- 1."))           # the self-check comes before the check and any KILL

    def test_a_primary_uses_force_master_and_expects_read_only_off(self):
        out = build_kill_statements({"node_id": "rn-main", "role": "primary", "zone": "cn-chengdu-a"}, self.rows(), mode="query", snapshot_us=1, generated_us=2)
        self.assertEqual(out["statements"], ["/*FORCE_MASTER*/ KILL QUERY 44;", "/*FORCE_MASTER*/ KILL QUERY 312;"])
        self.assertIn("每条语句都带 Hint FORCE_MASTER", out["text"])
        self.assertIn("-- 0. 先自检路由：read_only 应为 0（主节点）", out["text"])
        self.assertNotIn("FORCE_SLAVE", out["text"])
        self.assertEqual(len([line for line in out["text"].splitlines() if line.startswith("/*FORCE_MASTER*/ SELECT")]), 3)   # self-check, check, lock waits

    def test_route_hint_takes_only_plain_node_ids(self):
        self.assertEqual(route_hint({"node_id": "rn-2abc3def4ghi5jk01", "role": "secondary"}), ("FORCE_NODE='rn-2abc3def4ghi5jk01'", "/*FORCE_NODE='rn-2abc3def4ghi5jk01'*/ "))
        self.assertEqual(route_hint({"node_id": "rn-x"}), ("FORCE_NODE='rn-x'", "/*FORCE_NODE='rn-x'*/ "))     # an unknown role still goes to its node
        self.assertEqual(route_hint({"node_id": "anything'", "role": "primary"}), ("FORCE_MASTER", "/*FORCE_MASTER*/ "))
        for bad in ("", None, "rn-1'*/", "rn 1", "rn-1\n", "-rn", "r" * 65, "节点"):
            self.assertEqual(route_hint({"node_id": bad, "role": "secondary"}), ("", ""), repr(bad))

    def test_inputs_are_checked(self):
        for kwargs, rows in ((dict(mode="drop"), self.rows()), (dict(mode="connection"), []),
                             (dict(mode="connection"), [dict(session_id="12", user="a", command="Query")]),
                             (dict(mode="connection"), [dict(session_id=True, user="a", command="Query")]),
                             (dict(mode="connection"), [dict(session_id=i, user="a", command="Query") for i in range(1, MAX_KILL_SESSIONS + 2)])):
            with self.assertRaises(ValueError):
                build_kill_statements(self.NODE, rows, snapshot_us=1, generated_us=2, **kwargs)

    def test_an_unknown_node_role_is_said_not_guessed(self):
        out = build_kill_statements({"node_id": "rn-x"}, self.rows(), mode="connection", snapshot_us=1, generated_us=2)
        self.assertIn("节点角色未知 rn-x", out["text"])
        self.assertIn("返回的 server_id 应是目标节点的", out["text"])


if __name__ == "__main__":
    unittest.main()
