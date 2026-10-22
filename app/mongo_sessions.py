"""Session management for MongoDB: what the DAS session page shows for an ApsaraDB for MongoDB replica set, rebuilt from one
``GetMongoDBCurrentOp`` answer per node, and the mongosh text that ends the operations a person picked.

The page, the retention loop and the history are the ones RDS uses (``session_service`` / ``session_collector`` /
``session_store``); this module only supplies what differs:

* the node list. DAS names a replica-set member by its role id (``DescribeRoleZoneInfo``: primary, secondary and the hidden
  member that has no address). ``GetMongoDBCurrentOp`` answers for the **primary** when it does not know the ``NodeId`` it was
  given (checked 2026-10-22 with an id that does not exist: the primary's 22 operations came back, no error), so only ids
  from this book are ever sent. ``DescribeReplicaSetRole`` gives each role id its VPC address, written into the text;
* the view. One row per operation (``currentOp``), shaped like an RDS session row so the page, CSV and history keep one
  shape: ``session_id`` = opid, ``user`` = driver, ``database`` = namespace, ``command`` = op, ``state`` = plan summary,
  ``sql`` = the command document. DAS gives no user, no lock wait and no transaction fields for MongoDB, so the only
  abnormal kind is an operation running longer than the threshold;
* the text. ``killOp`` works on the node the operation runs on, and an opid is a per-node counter: the same number on
  another member is somebody else's operation. Each node therefore gets its own block for a direct connection to that
  member, and the kill line only kills an opid whose ``desc`` (the connection, ``connNNN``) still matches the snapshot.

Nothing here talks to a database; the service has a read-only cloud identity and no MongoDB account that may kill.
"""
from __future__ import annotations

import json
import re
from collections import defaultdict
from typing import Any

from .node_roles import NodeRoleBook
from .session_view import DEFAULT_THRESHOLDS, LIST_ACTIVE_LIMIT, LIST_OTHER_LIMIT, MAX_KILL_SESSIONS, STATS_LIMIT

ACTION = "GetMongoDBCurrentOp"
DDS_ENDPOINT = "https://mongodb.aliyuncs.com"
SQL_LIMIT = 2000
ROLE_NAMES = {"primary": "primary", "secondary": "secondary", "hidden": "hidden"}
CONNECTION_PATTERN = re.compile(r"conn\d{1,12}")
HOST_PATTERN = re.compile(r"dds-[a-z0-9-]+\.mongodb\.[a-z0-9-]+\.rds\.aliyuncs\.com")
# Command fields that say nothing about what the operation does (session, gossip and signature), dropped from the text shown.
NOISE_FIELDS = ("lsid", "$clusterTime", "$db", "txnNumber", "$readPreference", "autocommit", "startTransaction")
PROBE_COMMANDS = frozenset({"hello", "ismaster", "ping", "buildinfo", "saslstart", "saslcontinue", "endsessions"})
INTERNAL_DRIVERS = ("NetworkInterfaceTL", "MongoDB Internal Client")
KIND_LABELS = {"long_running": "操作运行过久"}


def is_mongo(instance_id: str) -> bool:
    """MongoDB instances are the ``dds-…`` ones (``mongo_collector.load_instances`` refuses any other id)."""
    return str(instance_id or "").startswith("dds-")


# -- the node book --------------------------------------------------------------

def parse_members(zones: dict[str, Any], replica_sets: dict[str, Any], instance_id: str) -> dict[str, dict[str, str]]:
    """``{role_id: {"role", "zone", "host"}}`` from ``DescribeRoleZoneInfo`` and ``DescribeReplicaSetRole``.

    ``host`` is the member's VPC address (the hidden member has none). A response for another instance never labels these.
    """
    raw = (zones.get("ZoneInfos") or {}).get("ZoneInfo") or []
    if isinstance(raw, dict):
        raw = [raw]
    members: dict[str, dict[str, str]] = {}
    for item in raw:
        if str(item.get("InsName") or "") != instance_id:
            raise ValueError("role zone info does not describe the requested instance")
        role_id = str(item.get("RoleId") or "").strip()
        if role_id.isdigit():
            role = str(item.get("RoleType") or "").strip().lower()
            members[role_id] = {"role": ROLE_NAMES.get(role, role), "zone": str(item.get("ZoneId") or "").strip(), "host": ""}
    if str(replica_sets.get("DBInstanceId") or instance_id) != instance_id:
        raise ValueError("replica set roles do not describe the requested instance")
    sets = (replica_sets.get("ReplicaSets") or {}).get("ReplicaSet") or []
    if isinstance(sets, dict):
        sets = [sets]
    for item in sets:
        role_id = str(item.get("RoleId") or "").strip()
        host = str(item.get("ConnectionDomain") or "").strip()
        if role_id in members and str(item.get("NetworkType") or "").upper() == "VPC" and HOST_PATTERN.fullmatch(host):
            members[role_id]["host"] = host
    return members


class MongoNodeBook(NodeRoleBook):
    """Members of the configured MongoDB instances, refreshed off the request path like the RDS node roles."""

    def __init__(self, settings_loader, credential_loader, entries, client_factory, refresh_seconds: float = 300) -> None:
        self._entries = entries
        super().__init__(settings_loader, credential_loader, lambda: [item["instanceId"] for item in self._entries() if item.get("enabled")],
                         client_factory, refresh_seconds)

    def _fetch(self, instance: str) -> dict[str, dict[str, str]]:
        from dataclasses import replace

        entry = next(item for item in self._entries() if item["instanceId"] == instance)
        settings = self._settings_loader()
        credential = self._credential_loader(settings.credential_target)
        if credential is None:
            raise RuntimeError("cloud_credential_unavailable")
        client = self._client_factory(replace(settings, db_instance_id=instance, region_id=entry["region"], endpoint=DDS_ENDPOINT),
                                      credential, timeout=15)
        return parse_members(client.call("DescribeRoleZoneInfo", {"DBInstanceId": instance}),
                             client.call("DescribeReplicaSetRole", {"DBInstanceId": instance}), instance)

    def pairs(self) -> list[tuple[str, str]]:
        with self._lock:
            return [(instance, node) for instance, nodes in self._roles.items() for node in nodes]

    def instances(self) -> list[dict[str, Any]]:
        """What the page lists: every configured instance with its members (empty until the first refresh succeeded)."""
        order = {"primary": 0, "secondary": 1, "hidden": 2}
        result = []
        for entry in self._entries():
            if not entry.get("enabled"):
                continue
            members = self.lookup(entry["instanceId"])
            result.append({"instance_id": entry["instanceId"], "label": entry.get("label") or entry["instanceId"],
                           "nodes": [{"node_id": node, **info} for node, info in
                                     sorted(members.items(), key=lambda item: (order.get(item[1]["role"], 3), item[0]))]})
        return result


# -- the view -------------------------------------------------------------------

def _seconds(value: Any) -> int:
    try:
        return max(int(float(value)), 0)
    except (TypeError, ValueError):
        return 0


def _opid(value: Any) -> int | None:
    """A replica-set opid is a 32-bit integer (DAS sends it as text and it can be negative); anything else is not one."""
    if isinstance(value, bool):
        return None
    text = str(value if value is not None else "").strip()
    if not re.fullmatch(r"-?\d{1,10}", text):
        return None
    number = int(text)
    return number if -2**31 <= number < 2**31 else None


def command_text(raw: Any) -> tuple[str, str]:
    """(text shown, command name) of the command document DAS returns as JSON text (``{"$truncated": "…"}`` when long)."""
    if raw in (None, ""):
        return "", ""
    try:
        doc = json.loads(raw) if isinstance(raw, str) else raw
    except ValueError:
        return str(raw), ""
    if not isinstance(doc, dict):
        return str(raw), ""
    if isinstance(doc.get("$truncated"), str):
        text = doc["$truncated"]
        name = re.match(r"\{\s*\$?([A-Za-z]+)\s*:", text)
        return text, (name.group(1) if name else "")
    kept = {key: value for key, value in doc.items() if key not in NOISE_FIELDS}
    name = next((key for key in kept if not key.startswith("$")), "")
    return json.dumps(kept, ensure_ascii=False, separators=(", ", ": ")), name


def protected_reason(opid: Any, connection: str, namespace: str, driver: str, command_name: str) -> str:
    """Why no kill line is written for this operation ("" when it may be ended)."""
    if not isinstance(opid, int) or isinstance(opid, bool):
        return "没有 opid"
    if not CONNECTION_PATTERN.fullmatch(connection):
        return "内部线程"
    if namespace.startswith("local.") or any(word in driver for word in INTERNAL_DRIVERS):
        return "副本集内部连接（复制 / 心跳）"
    if command_name.lower() in PROBE_COMMANDS:
        return "驱动探活 / 认证"
    return ""


def _row(item: dict[str, Any], thresholds: dict[str, int]) -> dict[str, Any]:
    text, name = command_text(item.get("Command"))
    opid = _opid(item.get("OpId"))
    connection = str(item.get("Desc") or "")
    namespace = str(item.get("Ns") or "")
    driver = str(item.get("Driver") or "")
    seconds = _seconds(item.get("SecsRunning"))
    active = bool(item.get("Active"))
    client = str(item.get("Client") or "")
    protected = protected_reason(opid, connection, namespace, driver, name)
    kinds = ["long_running"] if active and seconds > thresholds["blocking_s"] and protected != "驱动探活 / 认证" else []
    return {
        "session_id": opid, "connection": connection, "connection_id": item.get("ConnectionId"),
        "user": driver, "client": client.rsplit(":", 1)[0] if client.count(":") == 1 else client, "client_port": client.rsplit(":", 1)[-1] if client.count(":") == 1 else "",
        "database": namespace, "command": str(item.get("Op") or ""), "command_name": name, "state": str(item.get("PlanSummary") or ""),
        "seconds": seconds, "trx_id": "", "trx_seconds": 0, "sql": text[:SQL_LIMIT], "sql_chars": len(text),
        "active": active, "running": active, "waiting": False, "kinds": kinds, "protected": protected, "holder": False,
    }


def _stats(rows: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    counts: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
    for row in rows:
        entry = counts[row[field]]
        entry[0] += 1
        entry[1] += int(row["active"])
        entry[2] += int(bool(row["kinds"]))
    ranked = sorted(counts.items(), key=lambda item: (-item[1][2], -item[1][1], -item[1][0], item[0]))
    return [{"key": key, "total": total, "active": active, "abnormal": abnormal} for key, (total, active, abnormal) in ranked]


def build_mongo_session_view(data: dict[str, Any], now_us: int, thresholds: dict[str, int] | None = None) -> dict[str, Any]:
    """The session management payload of one ``GetMongoDBCurrentOp`` answer (its ``Data``), in the RDS payload's shape."""
    thresholds = thresholds or DEFAULT_THRESHOLDS
    listed = [item for item in (data.get("SessionList") or []) if isinstance(item, dict)]
    rows = [_row(item, thresholds) for item in listed]
    running = [row for row in rows if row["running"]]
    abnormal = [row for row in rows if row["kinds"]]
    flagged = abnormal
    actives = sorted((row for row in rows if row["active"] and not row["kinds"]), key=lambda row: (-row["seconds"], row["session_id"] or 0))
    others = sorted((row for row in rows if not row["active"]), key=lambda row: (-row["seconds"], row["session_id"] or 0))
    chosen = flagged + actives[:LIST_ACTIVE_LIMIT] + others[:LIST_OTHER_LIMIT]
    chosen.sort(key=lambda row: (not row["kinds"], -row["seconds"], row["session_id"] or 0))
    try:
        observed_us = int(data.get("Timestamp")) * 1000
    except (TypeError, ValueError):
        observed_us = now_us
    stat = data.get("SessionStat") or {}
    return {
        "status": "ok", "engine": "mongo", "observed_at_us": observed_us, "thresholds": dict(thresholds),
        "overview": {
            "total": len(rows), "listed_total": len(rows), "active": len(running), "running": len(running),
            "max_active_seconds": max((row["seconds"] for row in running), default=0), "abnormal": len(abnormal),
            "kinds": {"long_running": len(abnormal)}, "das_active": _seconds(stat.get("ActiveCount")),
        },
        "holders": {"waiters": 0, "oldest_wait_s": 0, "candidates": 0},
        "stats": {"user": _stats(rows, "user")[:STATS_LIMIT], "client": _stats(rows, "client")[:STATS_LIMIT],
                  "database": _stats(rows, "database")[:STATS_LIMIT],
                  "more": {name: max(len({row[name] for row in rows}) - STATS_LIMIT, 0) for name in ("user", "client", "database")}},
        "sessions": chosen, "truncated": len(chosen) < len(rows),
    }


# -- the text -------------------------------------------------------------------

def connect_command(host: str) -> str:
    return f'mongosh "mongodb://{host}:3717/admin?directConnection=true" -u <账号> -p'


def build_mongo_kill_statements(node: dict[str, Any], sessions: list[dict[str, Any]], *, snapshot_us: int,
                                generated_us: int) -> dict[str, Any]:
    """The mongosh lines that end the given operations on one member: one check of all of them, then one guarded kill line.

    Only integers (opids) and ``connNNN`` names reach the text, so nothing a client sent can become code. The kill line looks
    every pair up in ``$currentOp`` on the member it is run on and calls ``killOp`` only for an opid whose connection still
    matches: run on the wrong member, or after the operation ended and the number was reused, it kills nothing and says so.
    """
    if not sessions:
        raise ValueError("没有选中会话")
    if len(sessions) > MAX_KILL_SESSIONS:
        raise ValueError(f"一次最多 {MAX_KILL_SESSIONS} 个会话")
    keep: dict[int, str] = {}
    skipped: list[dict[str, Any]] = []
    for row in sessions:
        opid = row.get("session_id")
        if isinstance(opid, bool) or not isinstance(opid, int):
            raise ValueError("opid 必须是整数")
        if opid in keep or any(item["session_id"] == opid for item in skipped):
            continue
        reason = protected_reason(opid, str(row.get("connection") or ""), str(row.get("database") or ""), str(row.get("user") or ""),
                                  str(row.get("command_name") or ""))
        if reason:
            skipped.append({"session_id": opid, "reason": reason})
        else:
            keep[opid] = str(row["connection"])
    host = str(node.get("host") or "")
    if keep and not HOST_PATTERN.fullmatch(host):
        reason = "隐藏节点没有连接地址，请在 DAS 控制台的会话管理里结束" if node.get("role") == "hidden" else "不知道这个节点的连接地址"
        skipped.extend({"session_id": opid, "reason": reason} for opid in sorted(keep))
        keep = {}
    ids = sorted(keep)
    checks: list[str] = []
    statements: list[str] = []
    if ids:
        listing = ", ".join(str(opid) for opid in ids)
        pairs = ", ".join(f'[{opid}, "{keep[opid]}"]' for opid in ids)
        checks.append('db.getSiblingDB("admin").aggregate([{$currentOp: {allUsers: true}}, {$match: {opid: {$in: [' + listing + ']}}}, '
                      '{$project: {_id: 0, opid: 1, desc: 1, op: 1, ns: 1, secs_running: 1, client: 1, planSummary: 1}}])')
        statements.append('[' + pairs + '].forEach(([op, desc]) => { const admin = db.getSiblingDB("admin"); '
                          'if (admin.aggregate([{$currentOp: {allUsers: true}}, {$match: {opid: op, desc: desc}}]).toArray().length) '
                          'printjson({opid: op, result: admin.killOp(op)}); else print("opid " + op + " 不在本节点（或已结束），未执行"); })')
    lines = checks + statements
    return {"text": ("\n".join(lines) + "\n") if lines else "", "checks": checks, "statements": statements, "skipped": skipped,
            "mode": "killop", "session_ids": ids, "snapshot_us": snapshot_us, "generated_us": generated_us, "hint": "",
            "host": host if ids else "", "connect": connect_command(host) if ids else "",
            "node_id": str(node.get("node_id") or ""), "role": str(node.get("role") or ""), "zone": str(node.get("zone") or "")}


def combine_mongo_kill_statements(parts: list[dict[str, Any]], *, generated_us: int) -> dict[str, Any]:
    """Every member's block, kept apart: each one runs on its own direct connection. ``text`` joins them for the API only."""
    blocks = [f"// {part['role'] or 'node'} {part['node_id']}: {part['connect']}\n{part['text']}" for part in parts if part["text"]]
    nodes = [{key: part[key] for key in ("node_id", "role", "zone", "hint", "host", "connect", "text", "snapshot_us", "session_ids",
                                         "statements", "checks", "skipped")} for part in parts]
    return {"engine": "mongo", "text": "\n".join(blocks), "checks": [line for part in parts for line in part["checks"]],
            "statements": [line for part in parts for line in part["statements"]],
            "kills": sum(len(part["session_ids"]) for part in parts),
            "skipped": [{"node_id": part["node_id"], **row} for part in parts for row in part["skipped"]],
            "session_ids": [opid for part in parts for opid in part["session_ids"]],
            "mode": "killop", "generated_us": generated_us, "nodes": nodes}
