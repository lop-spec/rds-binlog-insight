"""Roles (primary / secondary) of RDS cluster-edition nodes.

Cloud Monitor and DAS identify a cluster node by an ``rn-…`` id that carries no role. The role lives in
``DBClusterNodes.DBClusterNode[]`` of ``DescribeDBInstanceAttribute`` (``NodeRole`` primary|secondary and
``NodeZoneId``) — the same read-only call the service already makes to verify an instance, so no new permission.
(``DescribeDBInstanceHAConfig`` is not usable for this: it names the same nodes with numeric ids.)

Roles are the *current* ones: a failover swaps them. They are refreshed by a background thread and only ever read
from memory on a request path, because the HTTP server is single-threaded and ``/api/status`` is polled every few
seconds. A refresh that fails keeps the last known roles and is logged once per failure streak.
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

LOGGER = logging.getLogger(__name__)

REFRESH_SECONDS = 300
_PRIMARY = {"primary", "master"}
_SECONDARY = {"secondary", "slave", "standby", "follower"}


def normalize_role(value: Any) -> str:
    """``primary`` / ``secondary``; anything else is kept lower-case so an unexpected value stays visible."""
    role = str(value or "").strip().lower()
    if role in _PRIMARY:
        return "primary"
    if role in _SECONDARY:
        return "secondary"
    return role


def parse_cluster_nodes(payload: dict[str, Any], instance_id: str) -> dict[str, dict[str, str]]:
    """``{node_id: {"role", "zone", "status"}}`` from a DescribeDBInstanceAttribute response.

    Empty for instances that are not cluster edition (no ``DBClusterNodes``). The attribute entry must be the
    requested instance: a response for another instance never labels these nodes.
    """
    items = (payload.get("Items") or {}).get("DBInstanceAttribute") or []
    if isinstance(items, dict):
        items = [items]
    item = next((entry for entry in items if str(entry.get("DBInstanceId") or "") == instance_id), None)
    if item is None:
        raise ValueError("instance attribute does not describe the requested instance")
    raw = (item.get("DBClusterNodes") or {}).get("DBClusterNode") or []
    if isinstance(raw, dict):
        raw = [raw]
    nodes: dict[str, dict[str, str]] = {}
    for node in raw:
        node_id = str(node.get("NodeId") or "").strip()
        if node_id:
            nodes[node_id] = {"role": normalize_role(node.get("NodeRole")),
                              "zone": str(node.get("NodeZoneId") or "").strip(),
                              "status": str(node.get("Status") or "").strip()}
    return nodes


class NodeRoleBook:
    def __init__(self, settings_loader: Callable[[], Any], credential_loader: Callable[[Any], Any],
                 instances: Callable[[], list[str]], client_factory: Callable[..., Any],
                 refresh_seconds: float = REFRESH_SECONDS) -> None:
        self._settings_loader = settings_loader
        self._credential_loader = credential_loader
        self._instances = instances
        self._client_factory = client_factory
        self._refresh_seconds = refresh_seconds
        self._lock = threading.Lock()
        self._roles: dict[str, dict[str, dict[str, str]]] = {}
        self._failing: set[str] = set()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def lookup(self, instance_id: str) -> dict[str, dict[str, str]]:
        """Last known roles of one instance; never touches the network."""
        with self._lock:
            return {node: dict(info) for node, info in self._roles.get(str(instance_id or ""), {}).items()}

    def refresh(self) -> None:
        for instance in sorted(set(self._instances())):
            if self._stop.is_set():
                return
            try:
                nodes = self._fetch(instance)
            except Exception as exc:  # noqa: BLE001 - a failed refresh must leave the last known roles in place
                if instance not in self._failing:
                    LOGGER.warning("node roles unavailable for %s (%s: %s); keeping the last known roles",
                                   instance, type(exc).__name__, exc)
                self._failing.add(instance)
                continue
            self._failing.discard(instance)
            with self._lock:
                previous = self._roles.get(instance)
                self._roles[instance] = nodes
            if previous is None:
                LOGGER.info("node roles loaded for %s: %s", instance,
                            ", ".join(f"{node}={info['role']}" for node, info in sorted(nodes.items())) or "(none)")
            else:
                for node, info in sorted(nodes.items()):
                    was = (previous.get(node) or {}).get("role")
                    if was != info["role"]:
                        LOGGER.warning("node role changed for %s %s: %s -> %s", instance, node, was or "(new)", info["role"])

    def _fetch(self, instance: str) -> dict[str, dict[str, str]]:
        settings = self._settings_loader()
        credential = self._credential_loader(settings.credential_target)
        if credential is None:
            raise RuntimeError("cloud_credential_unavailable")
        client = self._client_factory(settings, credential, timeout=15)
        return parse_cluster_nodes(client.call("DescribeDBInstanceAttribute", {"DBInstanceId": instance}), instance)

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="node-roles", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                self.refresh()
            except Exception:  # noqa: BLE001 - the loop must survive any single bad pass
                LOGGER.exception("node role refresh pass failed")
            self._stop.wait(max(self._refresh_seconds - (time.monotonic() - started), 5.0))

    def shutdown(self) -> None:
        self._stop.set()
