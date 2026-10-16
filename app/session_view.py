"""Session management: what the DAS session page shows, rebuilt from one ``GetMySQLAllSessionAsync`` snapshot.

DAS's page has an instance card (abnormal / active sessions, longest execution), session statistics by user, client and
database, a session list, and three kinds of abnormal session on RDS MySQL: a statement running longer than 30 s (a
blocking one, e.g. waiting for a table metadata lock), a transaction open longer than 30 s, and a transaction that has run
nothing for 10 s (a COMMIT probably forgotten). All of them are derivable from the fields the snapshot carries (Command,
Time, State, TrxId, TrxDuration), so this module computes them itself and adds one thing DAS does not show: when sessions
wait for a metadata lock, the sessions that can be holding it.

Ending a session is not done here. The service has a read-only cloud identity and no database account, so "kill" is a
text: ``build_kill_statements`` writes the KILL statements (and the statements to check first) for a person to run on the
node. Nothing in this module talks to a database.

A cluster-edition instance has no address per node: a person connects to the read/write splitting address, where a plain
statement lands on a node the proxy picks and the id of a session is only meaningful on its own node. Each statement is
therefore written with the proxy's routing hint (``/*FORCE_NODE='<node id>'*/`` for a standby, ``/*FORCE_MASTER*/`` for the
primary); on a direct connection the hint is only a comment.
"""
from __future__ import annotations

import re
from collections import defaultdict
from datetime import UTC, datetime
from typing import Any

# DAS's own definition for RDS MySQL (session management > abnormal sessions); a caller may pass others.
DEFAULT_THRESHOLDS = {"blocking_s": 30, "trx_s": 30, "idle_trx_s": 10}
# DAS: a session is active when its command is Query or Execute, or when it sits in an unfinished transaction.
RUNNING_COMMANDS = frozenset({"query", "execute"})
SQL_LIMIT = 2000
LIST_ACTIVE_LIMIT = 1000     # rows of one node's list beyond the flagged ones are capped (a node holds ~1200 sessions)
LIST_OTHER_LIMIT = 1500
STATS_LIMIT = 30
HOLDER_LIMIT = 20
MAX_KILL_SESSIONS = 200
# What a node id must look like to be written inside a hint (they come from the node registry; this is the second lock).
NODE_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
# Waits that are about a lock held by somebody else's transaction or statement (the victims of a stuck DDL).
LOCK_WAIT_WORDS = ("metadata lock", "global read lock", "backup lock", "commit lock", "table flush")
INTERNAL_USERS = frozenset({"system user", "event_scheduler", "aliyun_root"})
KIND_LABELS = {
    "long_running": "语句运行过久",
    "lock_wait": "等待锁",
    "long_trx": "事务过长",
    "idle_trx": "未提交事务",
}


def _seconds(value: Any) -> int:
    try:
        return max(int(float(value)), 0)
    except (TypeError, ValueError):
        return 0


def _session_id(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value or "").strip()
    return int(text) if text.isdigit() else None


def parse_thresholds(query: dict[str, str]) -> dict[str, int]:
    """Thresholds from ``blockingSeconds`` / ``trxSeconds`` / ``idleTrxSeconds``; a value that is not 1..86400 is refused."""
    names = {"blocking_s": "blockingSeconds", "trx_s": "trxSeconds", "idle_trx_s": "idleTrxSeconds"}
    result = dict(DEFAULT_THRESHOLDS)
    for key, name in names.items():
        raw = str(query.get(name) or "").strip()
        if not raw:
            continue
        try:
            value = int(raw)
        except ValueError as exc:
            raise ValueError(f"{name} 必须是整数") from exc
        if not 1 <= value <= 86400:
            raise ValueError(f"{name} 必须在 1 到 86400 之间")
        result[key] = value
    return result


def is_lock_wait(state: str) -> bool:
    text = state.strip().lower()
    return text.startswith("waiting for") and any(word in text for word in LOCK_WAIT_WORDS)


def protected_reason(user: str, command: str, session_id: Any) -> str:
    """Why a KILL statement is never written for this session ("" when it may be)."""
    lowered_command, lowered_user = command.strip().lower(), user.strip().lower()
    if not isinstance(session_id, int) or session_id <= 0:
        return "没有会话号"
    if lowered_command.startswith("binlog dump"):
        return "复制线程（Binlog Dump）"
    if lowered_user == "system user" or lowered_command in {"daemon", "connect"}:
        return "复制 / 系统线程"
    if lowered_user == "event_scheduler":
        return "事件调度线程"
    if lowered_user in INTERNAL_USERS or lowered_user.startswith("aliyun_"):
        return "云内部账号"
    return ""


def _session(item: dict[str, Any], thresholds: dict[str, int]) -> dict[str, Any]:
    # DAS leaves TrxId / TrxDuration out of the answer for a session that is in no transaction (checked on a live node).
    command = str(item.get("Command") or "").strip()
    state = str(item.get("State") or "")
    sql = str(item.get("SqlText") or "")
    seconds = _seconds(item.get("Time"))
    trx_id = str(item.get("TrxId") or "").strip()
    in_trx = trx_id not in ("", "0", "None", "null")
    trx_seconds = _seconds(item.get("TrxDuration")) if in_trx else 0
    running = command.lower() in RUNNING_COMMANDS
    session_id = _session_id(item.get("SessionId"))
    waiting = running and is_lock_wait(state)
    kinds = []
    if running and seconds > thresholds["blocking_s"]:
        kinds.append("long_running")
    if waiting and seconds > thresholds["blocking_s"]:
        kinds.append("lock_wait")
    if in_trx and trx_seconds > thresholds["trx_s"]:
        kinds.append("long_trx")
    if in_trx and command.lower() == "sleep" and seconds > thresholds["idle_trx_s"]:
        kinds.append("idle_trx")
    user = str(item.get("User") or "")
    return {
        "session_id": session_id, "user": user, "client": str(item.get("Client") or ""), "database": str(item.get("DbName") or ""),
        "command": command, "state": state, "seconds": seconds, "trx_id": trx_id if in_trx else "", "trx_seconds": trx_seconds,
        "sql": sql[:SQL_LIMIT], "sql_chars": len(sql), "active": running or in_trx, "running": running, "waiting": waiting,
        "kinds": kinds, "protected": protected_reason(user, command, session_id), "holder": False,
    }


def _holders(sessions: list[dict[str, Any]]) -> dict[str, Any]:
    """Sessions that can be holding the lock the waiting sessions queue for.

    Only reasoning from the snapshot: whoever holds a metadata lock got it before the oldest waiter started waiting, so a
    candidate is a session whose transaction (or running statement) is at least that old and is not itself waiting. DAS does
    not name the holder; the exact one needs ``performance_schema.metadata_locks`` on the node (the check statements do).
    """
    waiters = [row for row in sessions if row["waiting"]]
    if not waiters:
        return {"waiters": 0, "oldest_wait_s": 0, "candidates": 0}
    oldest = max(row["seconds"] for row in waiters)
    candidates = []
    for row in sessions:
        if row["waiting"]:
            continue
        if row["trx_id"] and row["trx_seconds"] >= oldest:
            candidates.append((row["trx_seconds"], "transaction", row))
        elif row["running"] and row["seconds"] >= oldest:
            candidates.append((row["seconds"], "statement", row))
    candidates.sort(key=lambda item: (-item[0], item[2]["session_id"] or 0))
    for rank, (_, basis, row) in enumerate(candidates[:HOLDER_LIMIT], 1):
        row.update(holder=True, holder_basis=basis, holder_rank=rank)
    return {"waiters": len(waiters), "oldest_wait_s": oldest, "candidates": min(len(candidates), HOLDER_LIMIT)}


def _stats(sessions: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    counts: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
    for row in sessions:
        entry = counts[row[field]]
        entry[0] += 1
        entry[1] += int(row["active"])
        entry[2] += int(bool(row["kinds"]))
    ranked = sorted(counts.items(), key=lambda item: (-item[1][2], -item[1][1], -item[1][0], item[0]))
    return [{"key": key, "total": total, "active": active, "abnormal": abnormal} for key, (total, active, abnormal) in ranked]


def build_session_view(data: dict[str, Any], now_us: int, thresholds: dict[str, int] | None = None) -> dict[str, Any]:
    """The session management payload of one finished snapshot (``Data`` of ``GetMySQLAllSessionAsync``)."""
    thresholds = thresholds or DEFAULT_THRESHOLDS
    snapshot = data.get("SessionData") or {}
    listed = snapshot.get("SessionList") or []
    sessions = [_session(item, thresholds) for item in listed if isinstance(item, dict)]
    holders = _holders(sessions)
    running = [row for row in sessions if row["running"]]
    kinds = {kind: sum(kind in row["kinds"] for row in sessions) for kind in KIND_LABELS}
    abnormal = [row for row in sessions if row["kinds"]]
    # Abnormal sessions and lock-holder candidates are always listed; the rest is capped per node (the statistics cover all).
    flagged = [row for row in sessions if row["kinds"] or row["holder"]]
    actives = sorted((row for row in sessions if row["active"] and not row["kinds"] and not row["holder"]), key=lambda row: (-row["seconds"], row["session_id"] or 0))
    others = sorted((row for row in sessions if not row["active"] and not row["kinds"] and not row["holder"]), key=lambda row: (-row["seconds"], row["session_id"] or 0))
    chosen = flagged + actives[:LIST_ACTIVE_LIMIT] + others[:LIST_OTHER_LIMIT]
    chosen.sort(key=lambda row: (not row["kinds"], not row["holder"], -row["seconds"], row["session_id"] or 0))
    observed_ms = snapshot.get("TimeStamp") or data.get("Timestamp")
    try:
        observed_us = int(observed_ms) * 1000
    except (TypeError, ValueError):
        observed_us = now_us
    return {
        "status": "ok", "observed_at_us": observed_us, "thresholds": dict(thresholds),
        "overview": {
            "total": int(snapshot.get("TotalSessionCount") or len(sessions)), "listed_total": len(sessions),
            "active": sum(row["active"] for row in sessions), "running": len(running),
            "max_active_seconds": max((row["seconds"] for row in running), default=0),
            "abnormal": len(abnormal), "kinds": kinds,
        },
        "holders": holders,
        "stats": {"user": _stats(sessions, "user")[:STATS_LIMIT], "client": _stats(sessions, "client")[:STATS_LIMIT],
                  "database": _stats(sessions, "database")[:STATS_LIMIT],
                  "more": {name: max(len({row[field] for row in sessions}) - STATS_LIMIT, 0)
                           for name, field in (("user", "user"), ("client", "client"), ("database", "database"))}},
        "sessions": chosen, "truncated": len(chosen) < len(sessions),
    }


def route_hint(node: dict[str, Any]) -> tuple[str, str]:
    """The read/write splitting hint that sends a statement to this node, as (name for a comment, text for the statement).

    Empty when the node id cannot be written inside a hint safely: the text then asks for a connection to the node itself.
    """
    if str(node.get("role") or "") == "primary":
        return "FORCE_MASTER", "/*FORCE_MASTER*/ "
    node_id = str(node.get("node_id") or "")
    if NODE_ID_PATTERN.fullmatch(node_id):
        return f"FORCE_NODE='{node_id}'", f"/*FORCE_NODE='{node_id}'*/ "
    return "", ""


def build_kill_statements(node: dict[str, Any], sessions: list[dict[str, Any]], *, mode: str, snapshot_us: int,
                          generated_us: int, lock_waiters: int = 0) -> dict[str, Any]:
    """The statements to end the given sessions on one node: one check of all of them, the lock check when sessions on the node
    wait for a lock (``lock_waiters`` > 0), and one KILL per session. Code only, no comments: the page says what to look at.

    ``sessions`` are rows of a snapshot as the page displayed them; only their integer ids reach the text (and the hint, built
    from a node id that matched ``NODE_ID_PATTERN``), so a user name cannot smuggle a statement in. The check carries
    ``@@read_only`` (1 on a standby, 0 on the primary): on the primary's own address a hint is only a comment and the statements
    run on the primary, and this column is how that shows. Only ``KILL <id>`` and ``KILL QUERY <id>`` are written.
    """
    if mode not in ("connection", "query"):
        raise ValueError("mode 必须是 connection 或 query")
    if not sessions:
        raise ValueError("没有选中会话")
    if len(sessions) > MAX_KILL_SESSIONS:
        raise ValueError(f"一次最多 {MAX_KILL_SESSIONS} 个会话")
    if isinstance(lock_waiters, bool) or not isinstance(lock_waiters, int) or lock_waiters < 0:
        raise ValueError("lock_waiters 必须是非负整数")
    keep: dict[int, dict[str, Any]] = {}
    skipped: list[dict[str, Any]] = []
    for row in sessions:
        session_id = row.get("session_id")
        if isinstance(session_id, bool) or not isinstance(session_id, int):
            raise ValueError("会话号必须是整数")
        if session_id in keep or any(item["session_id"] == session_id for item in skipped):
            continue
        reason = protected_reason(str(row.get("user") or ""), str(row.get("command") or ""), session_id)
        if reason:
            skipped.append({"session_id": session_id, "reason": reason})
        else:
            keep[session_id] = row
    verb = "KILL" if mode == "connection" else "KILL QUERY"
    hint_name, hint = route_hint(node)
    ids = sorted(keep)
    checks: list[str] = []
    statements: list[str] = []
    if ids:
        listing = ", ".join(str(i) for i in ids)
        read_only = "@@read_only AS ro, " if hint else ""
        checks.append(hint + f"SELECT {read_only}ID, USER, HOST, DB, COMMAND, TIME, STATE, LEFT(INFO, 80) AS INFO "
                      f"FROM information_schema.PROCESSLIST WHERE ID IN ({listing});")
        if lock_waiters:
            checks.append(hint + "SELECT * FROM sys.schema_table_lock_waits;")
        statements.extend(f"{hint}{verb} {session_id};" for session_id in ids)
    lines = checks + statements
    return {"text": ("\n".join(lines) + "\n") if lines else "", "checks": checks, "statements": statements, "skipped": skipped,
            "mode": mode, "session_ids": ids, "snapshot_us": snapshot_us, "generated_us": generated_us, "hint": hint_name,
            "node_id": str(node.get("node_id") or ""), "role": str(node.get("role") or ""), "zone": str(node.get("zone") or "")}


def combine_kill_statements(parts: list[dict[str, Any]], *, mode: str, generated_us: int) -> dict[str, Any]:
    """One text for sessions picked on several nodes: every node's checks first, then every node's KILLs, so a person runs
    the checks, reads them, then runs the KILLs, whatever node each session is on. Every line keeps its node's hint, so
    the whole text goes to one connection on the read/write splitting address. ``parts`` are ``build_kill_statements``
    answers, one per node, in the order the text should follow (the primary first).
    """
    checks = [line for part in parts for line in part["checks"]]
    statements = [line for part in parts for line in part["statements"]]
    lines = checks + statements
    nodes = [{key: part[key] for key in ("node_id", "role", "zone", "hint", "snapshot_us", "session_ids", "statements", "skipped")}
             for part in parts]
    return {"text": ("\n".join(lines) + "\n") if lines else "", "checks": checks, "statements": statements,
            "skipped": [{"node_id": part["node_id"], **row} for part in parts for row in part["skipped"]],
            "session_ids": [session_id for part in parts for session_id in part["session_ids"]],
            "mode": mode, "generated_us": generated_us, "nodes": nodes}
