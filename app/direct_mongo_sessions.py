"""Operations of one MongoDB replica-set member read straight from the member (``$currentOp``), shaped like the DAS answer.

DAS answers ``GetMongoDBCurrentOp`` in 1.5-2.2 s per member (1 s for the hidden one). The same facts are one aggregation on
the member itself: ``$currentOp`` with ``idleConnections`` answers in 20-70 ms for ~2000 connections, so the page reads the
members directly and keeps DAS as the fallback (and for the hidden member, which has no address).

The account is the read-only one the MongoDB collector already uses (``mongo-instances.json``: ``credentialsFile`` and
``readonlyUsername``); its roles (``read``, ``readAnyDatabase``, ``clusterMonitor``) include the ``inprog`` action ``$currentOp``
needs. The same checks as the collector run when a client is made: the authenticated user must be the approved one and hold
nothing but the approved roles. Only reads are issued (``hello``, ``connectionStatus``, one aggregation).

What comes back is the ``Data`` dict ``mongo_sessions.build_mongo_session_view`` reads from DAS (``SessionList`` with ``OpId``,
``Desc``, ``Client``, ``Driver``, ``Ns``, ``Op``, ``Active``, ``SecsRunning``, ``PlanSummary``, ``Command``, ``ConnectionId``)
so the page, the retention loop and the kill text keep one shape. Differences from DAS that are on purpose:

* every ``connNNN`` connection is listed (the member's own count, ``serverStatus`` agrees; DAS lists about 20 % fewer);
* a driver's monitoring command (``hello`` / ``isMaster`` streams, probes) and the replica set's own tailing of the oplog are
  connections, not running operations, so they are never "active" (DAS shows the same: 1-8 running operations where 230
  monitor streams are open);
* the member's internal threads (``BackgroundSync``, ``Checkpointer`` ...) and this reader's own operations are left out.

A role that is not the member's real role, an identity that is not the approved one, a refused connection or a failed read
raises ``DirectSessionError`` and the caller reads DAS instead.
"""
from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Any, Callable

from .direct_sessions import DirectSessionError
from .mongo_sessions import HOST_PATTERN, INTERNAL_DRIVERS, PROBE_COMMANDS

LOGGER = logging.getLogger(__name__)

APP_NAME = "sql-insight-sessions"
CONNECTION_PATTERN = re.compile(r"conn\d{1,12}")
APPROVED_ROLES = frozenset({"read", "readAnyDatabase", "clusterMonitor"})
DEFAULT_PORT = 3717
# Only what the page shows: the stage after $currentOp keeps a ~2000-document answer small.
PIPELINE: list[dict[str, Any]] = [
    {"$currentOp": {"allUsers": True, "idleConnections": True}},
    {"$project": {"_id": 0, "opid": 1, "desc": 1, "client": 1, "clientMetadata.driver": 1, "appName": 1, "ns": 1, "op": 1, "active": 1,
                  "secs_running": 1, "planSummary": 1, "command": 1, "connectionId": 1}},
]


def _command_name(command: Any) -> str:
    if isinstance(command, dict):
        return next((str(key) for key in command if not str(key).startswith("$")), "")
    return ""


def _command_text(command: Any) -> str:
    """The command document as JSON text (extended JSON for BSON types); ``$truncated`` documents stay as the member sent them."""
    if not isinstance(command, dict):
        return ""
    try:
        try:
            from bson import json_util
        except ImportError:  # the driver is installed wherever this runs for real
            return json.dumps(command, ensure_ascii=False, default=str)
        return json_util.dumps(command, json_options=json_util.RELAXED_JSON_OPTIONS, ensure_ascii=False)
    except Exception:  # noqa: BLE001 - a command that cannot be written is shown by its name only
        return json.dumps({_command_name(command): "…"}, ensure_ascii=False)


def _seconds(value: Any) -> int:
    try:
        return max(int(float(value)), 0)
    except (TypeError, ValueError):
        return 0


def build_data(docs: list[dict[str, Any]], wall_s: float) -> dict[str, Any]:
    """The DAS-shaped ``Data`` of one member from its ``$currentOp`` documents."""
    items = []
    active_total = 0
    for doc in docs:
        desc = str(doc.get("desc") or "")
        if not CONNECTION_PATTERN.fullmatch(desc) or doc.get("appName") == APP_NAME:
            continue
        command = doc.get("command")
        name = _command_name(command)
        driver = ((doc.get("clientMetadata") or {}).get("driver") or {})
        driver_text = f"{driver.get('name')}@{driver.get('version')}" if driver.get("name") else ""
        namespace = str(doc.get("ns") or "")
        internal = namespace.startswith("local.") or any(word in driver_text for word in INTERNAL_DRIVERS)
        active = bool(doc.get("active")) and name.lower() not in PROBE_COMMANDS and not internal
        active_total += int(active)
        item = {"OpId": doc.get("opid"), "Desc": desc, "ConnectionId": doc.get("connectionId"), "Client": str(doc.get("client") or ""),
                "Driver": driver_text, "Ns": namespace,
                "Op": str(doc.get("op") or ""), "Active": active, "SecsRunning": _seconds(doc.get("secs_running")),
                "PlanSummary": str(doc.get("planSummary") or ""), "Command": _command_text(command)}
        items.append(item)
    return {"Source": "mongo", "SessionList": items, "Timestamp": int(wall_s * 1000), "SessionStat": {"ActiveCount": active_total}}


class DirectMongoReader:
    def __init__(self, data_dir: Path, *, entries: Callable[[], list[dict[str, Any]]], roles: Callable[[str], dict[str, dict[str, str]]],
                 config: Callable[[], dict[str, Any]], client_factory: Callable[..., Any] | None = None, wall: Callable[[], float] = time.time) -> None:
        """``entries()`` lists the ``mongo-instances.json`` entries, ``roles(instance)`` the members ``{role id: {role, zone, host}}``,
        ``config()`` the shared ``session-direct.json`` settings; ``client_factory`` is ``pymongo.MongoClient``."""
        self._data_dir, self._entries, self._roles, self._config, self._factory, self._wall = Path(data_dir), entries, roles, config, client_factory, wall
        self._lock = threading.Lock()
        self._clients: dict[tuple[str, str], Any] = {}
        self._owner: dict[tuple[str, str], str] = {}        # (instance, the member's own name) -> the role id that answered with it

    # -- what can be read directly ------------------------------------------------
    def supports(self, instance_id: str, node_id: str) -> bool:
        """A primary or secondary with an address; the hidden member has none and stays on DAS."""
        member = (self._roles(instance_id) or {}).get(node_id) or {}
        return member.get("role") in ("primary", "secondary") and bool(HOST_PATTERN.fullmatch(str(member.get("host") or "")))

    def _entry(self, instance_id: str) -> dict[str, Any]:
        entry = next((item for item in self._entries() if item.get("instanceId") == instance_id and item.get("enabled")), None)
        if entry is None or not entry.get("credentialsFile") or not entry.get("readonlyUsername"):
            raise DirectSessionError("direct_no_credential", "mongo-instances.json 里没有这个实例的只读账号")
        return entry

    def _credentials(self, entry: dict[str, Any]) -> dict[str, Any]:
        path = Path(entry["credentialsFile"])
        path = path if path.is_absolute() else self._data_dir / path
        try:
            if os.name != "nt" and path.stat().st_mode & 0o077:
                raise DirectSessionError("direct_no_credential", "只读账号文件权限必须是 0600")
            auth = json.loads(path.read_text(encoding="utf-8"))
        except DirectSessionError:
            raise
        except (OSError, ValueError) as exc:
            raise DirectSessionError("direct_no_credential", f"只读账号文件无法读取：{type(exc).__name__}") from exc
        if auth.get("username") != entry["readonlyUsername"] or auth.get("authSource", "admin") != "admin" or not auth.get("password"):
            raise DirectSessionError("direct_no_credential", "只读账号文件与登记的只读身份不一致")
        return auth

    def _client(self, instance_id: str, host: str, entry: dict[str, Any]) -> Any:
        key = (instance_id, host)
        with self._lock:
            client = self._clients.get(key)
        if client is not None:
            return client
        auth = self._credentials(entry)
        config = self._config()
        factory = self._factory
        if factory is None:
            from pymongo import MongoClient
            factory = MongoClient
        client = factory(host=host, port=int(entry.get("port") or DEFAULT_PORT), username=auth["username"], password=auth["password"],
                         tls=bool(entry.get("tls", False)), authSource="admin", authMechanism="SCRAM-SHA-256", directConnection=True,
                         appname=APP_NAME, retryWrites=False, tz_aware=True, maxPoolSize=2,
                         serverSelectionTimeoutMS=int(config["connectTimeoutSeconds"] * 1000), connectTimeoutMS=int(config["connectTimeoutSeconds"] * 1000),
                         socketTimeoutMS=int(config["readTimeoutSeconds"] * 1000))
        try:
            identity = client.admin.command({"connectionStatus": 1}).get("authInfo", {})
            users = identity.get("authenticatedUsers", [])
            roles = {item.get("role") for item in identity.get("authenticatedUserRoles", [])}
            if not any(item.get("user") == entry["readonlyUsername"] and item.get("db") == "admin" for item in users):
                raise DirectSessionError("direct_wrong_identity", "认证身份不是登记的只读账号")
            if not roles or not roles <= APPROVED_ROLES:
                raise DirectSessionError("direct_wrong_identity", "只读账号带有未批准的角色")
        except DirectSessionError:
            client.close()
            raise
        except Exception as exc:  # noqa: BLE001 - every failure is a reason the caller logs, then it reads DAS
            client.close()
            raise DirectSessionError("direct_connect_failed", f"{type(exc).__name__}: {str(exc)[:200]}") from exc
        with self._lock:
            self._clients[key] = client
        return client

    def _drop(self, instance_id: str, host: str) -> None:
        with self._lock:
            client = self._clients.pop((instance_id, host), None)
        if client is not None:
            try:
                client.close()
            except Exception:  # noqa: BLE001 - closing a dead client is not a second failure
                pass

    def close(self) -> None:
        with self._lock:
            clients, self._clients = list(self._clients.values()), {}
        for client in clients:
            try:
                client.close()
            except Exception:  # noqa: BLE001
                pass

    # -- one member ------------------------------------------------------------------
    def read(self, instance_id: str, node_id: str) -> dict[str, Any]:
        """The DAS-shaped ``Data`` of one member, read now; ``DirectSessionError`` when it cannot be trusted."""
        member = (self._roles(instance_id) or {}).get(node_id) or {}
        role, host = str(member.get("role") or ""), str(member.get("host") or "")
        if role not in ("primary", "secondary") or not HOST_PATTERN.fullmatch(host):
            raise DirectSessionError("direct_no_address", f"成员没有可直连的地址（角色 {role or '未知'}）")
        entry = self._entry(instance_id)
        max_ms = int(self._config()["maxExecutionMs"])
        started = self._wall()
        client = self._client(instance_id, host, entry)
        try:
            hello = client.admin.command("hello")
            docs = list(client.admin.aggregate(PIPELINE, maxTimeMS=max_ms))
        except Exception as exc:  # noqa: BLE001
            self._drop(instance_id, host)
            raise DirectSessionError("direct_query_failed", f"{type(exc).__name__}: {str(exc)[:200]}") from exc
        finished = self._wall()
        if not docs:
            raise DirectSessionError("direct_empty", "没有读到任何操作（至少应有本服务自己的连接）")
        primary = bool(hello.get("isWritablePrimary")) and not hello.get("secondary")
        secondary = bool(hello.get("secondary"))
        if (role == "primary" and not primary) or (role == "secondary" and not secondary):
            raise DirectSessionError("direct_wrong_node", f"成员实际角色与登记的 {role} 不符（主备切换了？）")
        name = str(hello.get("me") or host)
        with self._lock:
            owner = self._owner.get((instance_id, name))
            if owner is not None and owner != node_id:
                raise DirectSessionError("direct_wrong_node", f"成员 {name} 已属于节点 {owner}，不是 {node_id}")
            for key in [key for key, value in self._owner.items() if key[0] == instance_id and value == node_id]:
                self._owner.pop(key)
            self._owner[(instance_id, name)] = node_id
        data = build_data(docs, (started + finished) / 2)
        data["Direct"] = {"member": name, "role": role, "elapsed_ms": round((finished - started) * 1000, 1), "documents": len(docs)}
        return data
