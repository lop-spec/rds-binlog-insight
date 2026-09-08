"""Resolve only observed Pod-IP bindings, never infer a historical Pod from IP.

Inventory is a metadata-only file supplied by an authorized Kubernetes reader.
No SSH, sudo, kube credentials, workload env vars or Docker socket enters Web.
"""
from __future__ import annotations

import ipaddress
import json
import logging
import os
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)
MAX_INVENTORY_BYTES = 8 * 1024 * 1024


def resolve_pod(client: str, event_us: int, path: Path, instance: str) -> dict[str, Any]:
    def unknown(reason: str, candidates: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        LOGGER.warning("slowlog_pod_lookup unavailable: reason=%s", reason)
        return {"status": reason, "pods": candidates or []}

    try:
        address = str(ipaddress.ip_address(client.strip()))
    except ValueError:
        return unknown("invalid_client_ip")
    try:
        with path.open("rb") as handle:
            raw = handle.read(MAX_INVENTORY_BYTES + 1)
        if len(raw) > MAX_INVENTORY_BYTES:
            return unknown("inventory_too_large")
        inventory = json.loads(raw)
        if inventory.get("schema_version") != 1 or not isinstance(inventory.get("bindings"), list):
            return unknown("invalid_inventory")
        if not instance or instance not in inventory.get("instance_ids", []):
            return unknown("instance_unconfigured")
        candidates = []
        observed = []
        for binding in inventory["bindings"]:
            if str(ipaddress.ip_address(binding["ip"])) != address:
                continue
            created = int(binding["created_us"])
            first, last = int(binding["first_seen_us"]), int(binding["last_seen_us"])
            if not binding.get("uid") or not binding.get("cluster") or not created <= first <= last:
                return unknown("invalid_inventory")
            pod = {key: str(binding.get(key) or "") for key in
                   ("cluster", "namespace", "name", "uid", "node", "host_ip", "owner")}
            pod.update(first_seen_us=first, last_seen_us=last)
            if created <= event_us:
                candidates.append(pod)
            if first <= event_us <= last:
                observed.append(pod)
        if len(observed) > 1:
            return unknown("ambiguous_ip", observed)
        if observed:
            return {"status": "observed_interval", "pods": observed,
                    "note": "Observed binding interval; polling cannot rule out changes between samples."}
        if candidates:
            return unknown("historical_binding_unverified", candidates)
        return unknown("ip_not_observed")
    except FileNotFoundError:
        return unknown("inventory_unconfigured")
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return unknown("invalid_inventory")


def attach_pod_detail(row: dict[str, Any], data_dir: Path) -> dict[str, Any]:
    if row.get("raw_event_type") == "SLOW_LOG":
        path = Path(os.environ.get("RDS_BINLOG_POD_INVENTORY", str(data_dir / "pod-inventory.json")))
        row["pod"] = resolve_pod(str(row.get("connection_name") or ""),
                                 int(row.get("event_epoch_us") or 0), path,
                                 str(row.get("instance_id") or ""))
    return row
