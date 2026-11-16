"""Sessions of one RDS MySQL node read straight from the database, shaped like the DAS snapshot.

DAS answers ``GetMySQLAllSessionAsync`` asynchronously (start, then poll every 1.5 s), about 2-3 s per node and 7 s for a
six-node instance. The same facts are one ``information_schema`` statement away: ``PROCESSLIST`` joined with ``INNODB_TRX``
answers in tens of milliseconds, so a page that needs "who is running / holding a transaction right now" reads the nodes
itself and keeps DAS as the fallback.

This module only reads. The statement is one ``SELECT`` (no ``KILL``, no DDL, no ``SET``); the account is the read-only
account that ``schema-instances.json`` already holds for the schema comparison (``SELECT, PROCESS, SHOW VIEW``). A
read/write-splitting address has no per-node port, so a standby is reached with the proxy's routing hint
``/*FORCE_NODE='<node id>'*/`` and the primary through the instance's own address (the hint is refused for the primary).

What comes back is the ``Data`` dict ``live_sessions`` caches (``SessionData.SessionList`` with ``SessionId/User/Client/
DbName/Command/State/Time/SqlText`` and ``TrxId/TrxDuration`` only for a session inside a transaction), so the running
statements view, the session management view, the retention loop and the KILL text all work unchanged.

Anything that cannot be trusted raises ``DirectSessionError`` and the caller reads DAS instead: no credential, the node's
role not known, a landing node that is not the one asked for (``@@read_only`` against the role, a ``@@server_id`` that
another node of the instance already owns), a refused or timed out connection.

Switched on per instance by ``data/session-direct.json`` (read again whenever its modification time changes, no restart)::

    {"enabled": true, "instances": {"<instance id>": {"primaryHost": "<the instance's own address>"}}, "excludeUsers": ["<extra platform account>"]}

Without the file nothing is read directly.
"""
from __future__ import annotations

import json
import logging
import re
import threading
import time
from pathlib import Path
from typing import Any, Callable

LOGGER = logging.getLogger(__name__)

CONFIG_NAME = "session-direct.json"
DEFAULT_CONFIG: dict[str, Any] = {"enabled": False, "instances": {}, "cacheSeconds": 3.0, "connectTimeoutSeconds": 3, "readTimeoutSeconds": 8,
                                  "maxExecutionMs": 5000, "excludeUsers": []}
# Accounts of the RDS platform itself. DAS does not list them (checked against DAS on a production instance: the replica
# threads, the event scheduler, the platform agent and replication accounts are absent from its counts); listing them here
# would show a replica's SQL threads as "running" sessions and the primary's replication threads as sessions to kill.
# ``excludeUsers`` of the config file adds the accounts of one deployment (the platform has more than are common to all).
INTERNAL_USERS = ("system user", "event_scheduler", "aliyun_root", "replicator", "rdsadmin", "rdsinternal", "rdsrepl")
NODE_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
HOST_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9.-]{0,252}")
# One statement, one round trip: the thread list of the node, each thread's InnoDB transaction (if any), and who answered.
# Our own thread is left out. Only a read-write transaction counts as one: InnoDB starts every transaction read only and
# upgrades it on the first write or locking read, so a single autocommit SELECT, or a pooled application connection that ran
# a SELECT after BEGIN and sits idle, holds nothing and is no transaction somebody forgot to commit. DAS does not report those
# either (checked against it on a production instance: about ten such idle read-only transactions on the standbys, none in DAS).
SESSION_SQL = (
    "SELECT /*+ MAX_EXECUTION_TIME({max_ms}) */ @@server_id AS sid, @@read_only AS ro, "
    "p.ID, p.USER, p.HOST, p.DB, p.COMMAND, p.TIME, p.STATE, p.INFO, t.trx_id, "
    "TIMESTAMPDIFF(SECOND, t.trx_started, NOW()) AS trx_s "
    "FROM information_schema.PROCESSLIST p "
    "LEFT JOIN information_schema.INNODB_TRX t ON t.trx_mysql_thread_id = p.ID AND t.trx_is_read_only = 0 "
    "WHERE p.ID <> CONNECTION_ID() AND p.USER NOT IN ({users})"
)
MAX_EXCLUDED = 50


class DirectSessionError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def load_config(data_dir: Path) -> dict[str, Any]:
    """The direct-read settings: off without the file, off (with a warning) for a file that cannot be used."""
    path = Path(data_dir) / CONFIG_NAME
    if not path.exists():
        return dict(DEFAULT_CONFIG)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("要求 JSON 对象")
        config = {**DEFAULT_CONFIG, **{key: raw[key] for key in DEFAULT_CONFIG if key in raw}}
        if not isinstance(config["enabled"], bool):
            raise ValueError("enabled 必须是 true/false")
        instances = config["instances"]
        if not isinstance(instances, dict):
            raise ValueError("instances 必须是对象")
        for instance_id, item in instances.items():
            primary = (item or {}).get("primaryHost") if isinstance(item, dict) else None
            if not isinstance(primary, str) or not HOST_PATTERN.fullmatch(primary):
                raise ValueError(f"{instance_id} 缺少合法的 primaryHost")
        excluded = config["excludeUsers"]
        if not (isinstance(excluded, list) and len(excluded) <= MAX_EXCLUDED and all(isinstance(name, str) and 0 < len(name) <= 64 for name in excluded)):
            raise ValueError("excludeUsers 必须是不超过 50 个、每个 1 到 64 字符的账号名数组")
        for key, low, high in (("cacheSeconds", 0, 60), ("connectTimeoutSeconds", 1, 30), ("readTimeoutSeconds", 1, 60), ("maxExecutionMs", 500, 30000)):
            value = config[key]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not low <= value <= high:
                raise ValueError(f"{key} 必须在 {low} 到 {high} 之间")
        return config
    except (OSError, ValueError) as exc:
        LOGGER.warning("会话直连配置 %s 无法使用，按关闭处理（读取走 DAS）：%s", path, exc)
        return dict(DEFAULT_CONFIG)


def _seconds(value: Any) -> int:
    try:
        return max(int(float(value)), 0)
    except (TypeError, ValueError):
        return 0


def _client(host: Any) -> str:
    """``ip:port`` of a connection's host column as DAS prints it: the address only."""
    text = str(host or "")
    head, sep, tail = text.rpartition(":")
    return head if sep and tail.isdigit() else text


def build_data(rows: list[tuple], wall_s: float) -> tuple[dict[str, Any], int, int]:
    """The DAS-shaped ``Data`` of one node from the statement's rows, with the answering ``server_id`` and ``read_only``."""
    sessions = []
    server_id = read_only = -1
    for sid, ro, ident, user, host, database, command, seconds, state, info, trx_id, trx_s in rows:
        server_id, read_only = int(sid), int(ro)
        item = {"SessionId": int(ident), "User": str(user or ""), "Client": _client(host), "DbName": str(database or ""),
                "Command": str(command or ""), "State": str(state or ""), "Time": _seconds(seconds), "SqlText": str(info or "")}
        if trx_id is not None:
            item["TrxId"] = str(trx_id)
            item["TrxDuration"] = _seconds(trx_s)
        sessions.append(item)
    data = {"IsFinish": True, "Fail": False, "State": "SUCCESS", "Source": "mysql",
            "SessionData": {"SessionList": sessions, "TotalSessionCount": len(sessions), "TimeStamp": int(wall_s * 1000)}}
    return data, server_id, read_only


class DirectSessionReader:
    def __init__(self, data_dir: Path, *, instances: Callable[[], list[Any]], roles: Callable[[str], dict[str, dict[str, str]]],
                 connect: Callable[..., Any] | None = None, wall: Callable[[], float] = time.time) -> None:
        """``instances()`` lists the instance configs of ``schema-instances.json`` (``instance_id, host, port, user, password``);
        ``roles(instance)`` maps node id to ``{"role": "primary" | "secondary", ...}``; ``connect`` is ``pymysql.connect``."""
        self._data_dir, self._instances, self._roles, self._connect, self._wall = Path(data_dir), instances, roles, connect, wall
        self._config: dict[str, Any] = dict(DEFAULT_CONFIG)
        self._config_stamp: tuple[int, int] | None = None
        self._lock = threading.Lock()
        self._owner: dict[tuple[str, int], str] = {}          # (instance, server_id) -> the node that answered with it
        self._answered: dict[tuple[str, str], int] = {}       # (instance, node) -> its server_id

    # -- settings -------------------------------------------------------------
    def config(self) -> dict[str, Any]:
        path = self._data_dir / CONFIG_NAME
        try:
            stat = path.stat()
            stamp: tuple[int, int] | None = (stat.st_mtime_ns, stat.st_size)
        except OSError:
            stamp = None
        with self._lock:
            if stamp != self._config_stamp:
                self._config, self._config_stamp = load_config(self._data_dir), stamp
            return self._config

    def cache_seconds(self) -> float:
        return float(self.config()["cacheSeconds"])

    def enabled_for(self, instance_id: str) -> bool:
        config = self.config()
        return bool(config["enabled"] and instance_id in config["instances"])

    # -- one node -------------------------------------------------------------
    def _endpoint(self, instance_id: str, node_id: str) -> tuple[str, str, int, str, str]:
        """(route hint, host, port, user, password) for a node; the hint is empty for the primary."""
        if not NODE_ID_PATTERN.fullmatch(node_id):
            raise DirectSessionError("direct_bad_node", "节点 ID 格式不能写进路由 Hint")
        try:
            item = next((entry for entry in self._instances() if getattr(entry, "instance_id", "") == instance_id), None)
        except Exception as exc:  # noqa: BLE001 - an unreadable or invalid schema-instances.json is "no credential"
            raise DirectSessionError("direct_no_credential", f"schema-instances.json 无法读取：{type(exc).__name__}: {str(exc)[:160]}") from exc
        if item is None or not getattr(item, "user", "") or not getattr(item, "host", ""):
            raise DirectSessionError("direct_no_credential", "schema-instances.json 里没有这个实例的只读账号")
        role = str((self._roles(instance_id).get(node_id) or {}).get("role") or "")
        if role == "primary":
            return "", self.config()["instances"][instance_id]["primaryHost"], int(item.port), str(item.user), str(item.password)
        if role == "secondary":
            return f"/*FORCE_NODE='{node_id}'*/ ", str(item.host), int(item.port), str(item.user), str(item.password)
        raise DirectSessionError("direct_role_unknown", f"节点角色未知（{role or '空'}），不能决定走哪个地址")

    def read(self, instance_id: str, node_id: str) -> dict[str, Any]:
        """The DAS-shaped ``Data`` of one node, read now; ``DirectSessionError`` when it cannot be trusted."""
        config = self.config()
        hint, host, port, user, password = self._endpoint(instance_id, node_id)
        expected_read_only = 0 if not hint else 1
        users = list(dict.fromkeys([*INTERNAL_USERS, *config["excludeUsers"]]))
        sql = hint + SESSION_SQL.format(max_ms=int(config["maxExecutionMs"]), users=", ".join(["%s"] * len(users)))
        connect = self._connect
        if connect is None:
            import pymysql
            connect = pymysql.connect
        started = self._wall()
        try:
            connection = connect(host=host, port=port, user=user, password=password, charset="utf8mb4", autocommit=True,
                                 connect_timeout=config["connectTimeoutSeconds"], read_timeout=config["readTimeoutSeconds"],
                                 write_timeout=config["readTimeoutSeconds"])
        except Exception as exc:  # noqa: BLE001 - every failure becomes a reason the caller logs, then it reads DAS
            raise DirectSessionError("direct_connect_failed", f"{type(exc).__name__}: {str(exc)[:200]}") from exc
        try:
            with connection.cursor() as cursor:
                cursor.execute(sql, users)                       # the account names are bound parameters, never part of the text
                rows = list(cursor.fetchall())
        except Exception as exc:  # noqa: BLE001
            raise DirectSessionError("direct_query_failed", f"{type(exc).__name__}: {str(exc)[:200]}") from exc
        finally:
            try:
                connection.close()
            except Exception:  # noqa: BLE001 - closing a dead connection is not a second failure
                pass
        finished = self._wall()
        data, server_id, read_only = build_data(rows, (started + finished) / 2)
        if not rows:
            raise DirectSessionError("direct_empty", "没有读到任何会话（至少应有本服务之外的业务连接）")
        if read_only != expected_read_only:
            raise DirectSessionError("direct_wrong_node", f"落点 read_only={read_only}，该节点角色应为 {expected_read_only}（路由或角色变了）")
        with self._lock:
            owner = self._owner.get((instance_id, server_id))
            if owner is not None and owner != node_id:
                raise DirectSessionError("direct_wrong_node", f"server_id {server_id} 已属于节点 {owner}，Hint 没有把语句送到 {node_id}")
            previous = self._answered.get((instance_id, node_id))
            if previous is not None and previous != server_id:
                self._owner.pop((instance_id, previous), None)
            self._owner[(instance_id, server_id)] = node_id
            self._answered[(instance_id, node_id)] = server_id
        data["Direct"] = {"server_id": server_id, "read_only": read_only, "elapsed_ms": round((finished - started) * 1000, 1)}
        return data
