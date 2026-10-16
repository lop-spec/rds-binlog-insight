"""Session management view: DAS's three abnormal-session kinds, statistics, lock-holder candidates and KILL statements."""
import unittest
from datetime import UTC, datetime

from app.session_view import (DEFAULT_THRESHOLDS, HOLDER_LIMIT, LIST_ACTIVE_LIMIT, MAX_KILL_SESSIONS, build_kill_statements, combine_kill_statements,
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
    HINT = "/*FORCE_NODE='rn-lagging'*/ "
    CHECK = "SELECT @@read_only AS ro, ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 80) AS INFO FROM information_schema.PROCESSLIST WHERE ID IN (44, 312);"

    def rows(self):
        return [dict(session_id=312, user="app", client="10.0.0.1", database="shop", command="Sleep", seconds=905, trx_seconds=905,
                     kinds=["long_trx", "idle_trx"]),
                dict(session_id=44, user="etl", client="10.0.0.9", database="dw", command="Query", seconds=70, trx_seconds=0, kinds=["long_running"])]

    def test_a_standby_gets_one_check_and_one_kill_per_session_and_nothing_else(self):
        out = build_kill_statements(self.NODE, self.rows(), mode="connection", snapshot_us=1_790_000_000_000_000, generated_us=1_790_000_060_000_000)
        self.assertEqual(out["text"].splitlines(), [self.HINT + self.CHECK, self.HINT + "KILL 44;", self.HINT + "KILL 312;"])
        self.assertEqual(out["statements"], [self.HINT + "KILL 44;", self.HINT + "KILL 312;"])
        self.assertEqual((out["session_ids"], out["hint"], out["snapshot_us"]), ([44, 312], "FORCE_NODE='rn-lagging'", 1_790_000_000_000_000))
        self.assertNotIn("--", out["text"])                                   # no comments: the page says what to look at

    def test_the_lock_check_is_written_only_when_sessions_on_the_node_wait_for_a_lock(self):
        quiet = build_kill_statements(self.NODE, self.rows(), mode="connection", snapshot_us=1, generated_us=2)
        self.assertNotIn("schema_table_lock_waits", quiet["text"])
        busy = build_kill_statements(self.NODE, self.rows(), mode="connection", snapshot_us=1, generated_us=2, lock_waiters=3)
        self.assertEqual(busy["text"].splitlines(), [self.HINT + self.CHECK, self.HINT + "SELECT * FROM sys.schema_table_lock_waits;",
                                                     self.HINT + "KILL 44;", self.HINT + "KILL 312;"])
        for bad in (-1, True, "2", 1.5, None):
            with self.assertRaises(ValueError):
                build_kill_statements(self.NODE, self.rows(), mode="connection", snapshot_us=1, generated_us=2, lock_waiters=bad)

    def test_query_mode_writes_kill_query(self):
        out = build_kill_statements(self.NODE, self.rows(), mode="query", snapshot_us=1, generated_us=2)
        self.assertEqual(out["statements"], [self.HINT + "KILL QUERY 44;", self.HINT + "KILL QUERY 312;"])

    def test_a_primary_uses_force_master_and_its_check_carries_read_only(self):
        out = build_kill_statements({"node_id": "rn-main", "role": "primary", "zone": "cn-chengdu-a"}, self.rows(), mode="connection", snapshot_us=1, generated_us=2)
        self.assertEqual(out["text"].splitlines(), ["/*FORCE_MASTER*/ " + self.CHECK, "/*FORCE_MASTER*/ KILL 44;", "/*FORCE_MASTER*/ KILL 312;"])
        self.assertEqual(out["hint"], "FORCE_MASTER")

    def test_protected_sessions_are_skipped_with_the_reason_and_duplicates_collapse(self):
        rows = self.rows() + [dict(session_id=9, user="repl", command="Binlog Dump GTID", seconds=9999),
                              dict(session_id=10, user="aliyun_root", command="Query", seconds=70), dict(self.rows()[0])]
        out = build_kill_statements(self.NODE, rows, mode="connection", snapshot_us=1, generated_us=2)
        self.assertEqual(out["session_ids"], [44, 312])
        self.assertEqual(out["skipped"], [{"session_id": 9, "reason": "复制线程（Binlog Dump）"}, {"session_id": 10, "reason": "云内部账号"}])
        self.assertEqual(out["text"].splitlines()[0], self.HINT + self.CHECK)       # the skipped ids are not in the text at all
        self.assertNotIn("KILL 9;", out["text"])
        self.assertNotIn("KILL 10;", out["text"])

    def test_only_protected_sessions_leave_no_statement_at_all(self):
        out = build_kill_statements(self.NODE, [dict(session_id=9, user="repl", command="Binlog Dump", seconds=1)], mode="connection", snapshot_us=1, generated_us=2)
        self.assertEqual((out["statements"], out["session_ids"], out["text"]), ([], [], ""))

    def test_a_user_name_cannot_put_a_statement_into_the_text(self):
        evil = dict(session_id=5, user="x\nKILL 1; --", client="10.0.0.1 */ DROP TABLE t; /*", database="d\r\nSELECT 1;", command="Query KILL 2;",
                    seconds=99, kinds=[])
        out = build_kill_statements({"node_id": "rn-1\nKILL 3;", "role": "primary", "zone": "z\nKILL 4;"}, [evil], mode="connection", snapshot_us=1, generated_us=2)
        self.assertEqual(out["text"].splitlines(), ["/*FORCE_MASTER*/ SELECT @@read_only AS ro, ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 80) AS INFO "
                                                    "FROM information_schema.PROCESSLIST WHERE ID IN (5);", "/*FORCE_MASTER*/ KILL 5;"])
        self.assertEqual(out["statements"], ["/*FORCE_MASTER*/ KILL 5;"])

    def test_one_text_for_several_nodes_puts_every_check_before_every_kill(self):
        primary = build_kill_statements({"node_id": "rn-main", "role": "primary", "zone": "cn-chengdu-a"}, self.rows()[:1], mode="connection", snapshot_us=1, generated_us=2)
        standby = build_kill_statements(self.NODE, self.rows() + [dict(session_id=9, user="repl", command="Binlog Dump GTID", seconds=1)],
                                        mode="connection", snapshot_us=3, generated_us=2, lock_waiters=2)
        self.assertEqual(primary["checks"], ["/*FORCE_MASTER*/ " + self.CHECK.replace("(44, 312)", "(312)")])
        self.assertEqual((primary["node_id"], primary["role"], primary["zone"]), ("rn-main", "primary", "cn-chengdu-a"))
        out = combine_kill_statements([primary, standby], mode="connection", generated_us=2)
        self.assertEqual(out["text"].splitlines(), [
            "/*FORCE_MASTER*/ " + self.CHECK.replace("(44, 312)", "(312)"),
            self.HINT + self.CHECK, self.HINT + "SELECT * FROM sys.schema_table_lock_waits;",
            "/*FORCE_MASTER*/ KILL 312;", self.HINT + "KILL 44;", self.HINT + "KILL 312;"])
        self.assertEqual(out["checks"], out["text"].splitlines()[:3])
        self.assertEqual(out["statements"], out["text"].splitlines()[3:])
        self.assertEqual(out["session_ids"], [312, 44, 312])                      # the same id on two nodes is two sessions
        self.assertEqual(out["skipped"], [{"node_id": "rn-lagging", "session_id": 9, "reason": "复制线程（Binlog Dump）"}])
        self.assertEqual([(n["node_id"], n["role"], n["hint"], n["snapshot_us"], n["session_ids"], len(n["statements"]), len(n["skipped"])) for n in out["nodes"]],
                         [("rn-main", "primary", "FORCE_MASTER", 1, [312], 1, 0), ("rn-lagging", "secondary", "FORCE_NODE='rn-lagging'", 3, [44, 312], 2, 1)])
        self.assertEqual((out["mode"], out["generated_us"]), ("connection", 2))
        self.assertEqual(combine_kill_statements([], mode="connection", generated_us=2)["text"], "")

    def test_a_node_id_that_cannot_sit_in_a_hint_leaves_the_hint_out(self):
        out = build_kill_statements({"node_id": "rn-1'*/ KILL 3; /*", "role": "secondary", "zone": "z"}, self.rows()[:1], mode="connection", snapshot_us=1, generated_us=2)
        self.assertEqual(out["hint"], "")                                      # the page then asks for a direct connection to the node
        self.assertEqual(out["statements"], ["KILL 312;"])
        self.assertEqual(out["text"].splitlines(), ["SELECT ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 80) AS INFO FROM information_schema.PROCESSLIST WHERE ID IN (312);",
                                                    "KILL 312;"])

    def test_inputs_are_checked(self):
        for kwargs, rows in ((dict(mode="drop"), self.rows()), (dict(mode="connection"), []),
                             (dict(mode="connection"), [dict(session_id="12", user="a", command="Query")]),
                             (dict(mode="connection"), [dict(session_id=True, user="a", command="Query")]),
                             (dict(mode="connection"), [dict(session_id=i, user="a", command="Query") for i in range(1, MAX_KILL_SESSIONS + 2)])):
            with self.assertRaises(ValueError):
                build_kill_statements(self.NODE, rows, snapshot_us=1, generated_us=2, **kwargs)

    def test_an_unknown_node_role_still_goes_to_its_node(self):
        out = build_kill_statements({"node_id": "rn-x"}, self.rows(), mode="connection", snapshot_us=1, generated_us=2)
        self.assertEqual(out["hint"], "FORCE_NODE='rn-x'")
        self.assertTrue(all(line.startswith("/*FORCE_NODE='rn-x'*/ ") for line in out["text"].splitlines()))

    def test_route_hint_takes_only_plain_node_ids(self):
        self.assertEqual(route_hint({"node_id": "rn-2abc3def4ghi5jk01", "role": "secondary"}), ("FORCE_NODE='rn-2abc3def4ghi5jk01'", "/*FORCE_NODE='rn-2abc3def4ghi5jk01'*/ "))
        self.assertEqual(route_hint({"node_id": "rn-x"}), ("FORCE_NODE='rn-x'", "/*FORCE_NODE='rn-x'*/ "))     # an unknown role still goes to its node
        self.assertEqual(route_hint({"node_id": "anything'", "role": "primary"}), ("FORCE_MASTER", "/*FORCE_MASTER*/ "))
        for bad in ("", None, "rn-1'*/", "rn 1", "rn-1\n", "-rn", "r" * 65, "节点"):
            self.assertEqual(route_hint({"node_id": bad, "role": "secondary"}), ("", ""), repr(bad))


if __name__ == "__main__":
    unittest.main()
