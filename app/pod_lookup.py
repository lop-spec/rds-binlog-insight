"""Resolve only observed Pod-IP bindings, never infer a historical Pod from IP.

Inventory is a metadata-only file written by ``pod_inventory`` (read-only ACK / ECS calls under the service's cloud
identity). No SSH, sudo, workload env vars or Docker socket is involved; ``instance_ids`` ``["*"]`` applies it to every
instance (the clusters' VPC ranges do not overlap).
"""
from __future__ import annotations

import ipaddress
import json
import logging
import os
import threading
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)
MAX_INVENTORY_BYTES = 32 * 1024 * 1024
_CACHE: dict[str, Any] = {}
_CACHE_LOCK = threading.Lock()


def load_inventory(path: Path) -> dict[str, Any]:
    """Parsed inventory, re-read only when the file changes (the refresher rewrites it once a minute)."""
    stat = path.stat()
    key = (str(path), stat.st_mtime_ns, stat.st_size)
    with _CACHE_LOCK:
        if _CACHE.get("key") == key:
            return _CACHE["data"]
    if stat.st_size > MAX_INVENTORY_BYTES:
        raise OverflowError("inventory_too_large")
    data = json.loads(path.read_bytes())
    if data.get("schema_version") != 1 or not isinstance(data.get("bindings"), list):
        raise ValueError("invalid_inventory")
    by_ip: dict[str, list[dict[str, Any]]] = {}
    for binding in data["bindings"]:
        by_ip.setdefault(str(ipaddress.ip_address(binding["ip"])), []).append(binding)
    data["_by_ip"] = by_ip
    data["_hosts"] = {str(ipaddress.ip_address(host["ip"])): host for host in data.get("hosts") or []}
    with _CACHE_LOCK:
        _CACHE.update(key=key, data=data)
    return data


def resolve_pod(client: str, event_us: int, path: Path, instance: str) -> dict[str, Any]:
    def unknown(reason: str, candidates: list[dict[str, Any]] | None = None) -> dict[str, Any]:
        LOGGER.warning("slowlog_pod_lookup unavailable: reason=%s", reason)
        return {"status": reason, "pods": candidates or []}

    try:
        address = str(ipaddress.ip_address(client.strip()))
    except ValueError:
        return unknown("invalid_client_ip")
    try:
        inventory = load_inventory(path)
        if not instance or not ({instance, "*"} & set(inventory.get("instance_ids", []))):
            return unknown("instance_unconfigured")
        candidates = []
        observed = []
        for binding in inventory["_by_ip"].get(address, []):
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
    except OverflowError:
        return unknown("inventory_too_large")
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return unknown("invalid_inventory")


def attach_pod_detail(row: dict[str, Any], data_dir: Path) -> dict[str, Any]:
    if row.get("raw_event_type") == "SLOW_LOG":
        path = inventory_path(data_dir)
        row["pod"] = resolve_pod(str(row.get("connection_name") or ""),
                                 int(row.get("event_epoch_us") or 0), path,
                                 str(row.get("instance_id") or ""))
    return row


def inventory_path(data_dir: Path) -> Path:
    return Path(os.environ.get("RDS_BINLOG_POD_INVENTORY", str(data_dir / "pod-inventory.json")))


def name_ips(ips: list[str], at_us: int, path: Path) -> dict[str, dict[str, str]]:
    """Short names for the page: the Pod that held the IP at ``at_us`` (only when exactly one did), else the ECS
    instance that has the address now. IPs with neither are left out."""
    try:
        inventory = load_inventory(path)
    except (OSError, ValueError, TypeError, KeyError, OverflowError):
        return {}
    out: dict[str, dict[str, str]] = {}
    for value in ips:
        try:
            address = str(ipaddress.ip_address(str(value).strip()))
        except ValueError:
            continue
        pods = [binding for binding in inventory["_by_ip"].get(address, [])
                if int(binding["first_seen_us"]) <= at_us <= int(binding["last_seen_us"]) + 120_000_000]
        if len(pods) == 1:
            pod = pods[0]
            out[value] = {"kind": "pod", "name": str(pod.get("name") or ""), "namespace": str(pod.get("namespace") or ""),
                          "cluster": str(pod.get("cluster") or ""), "node": str(pod.get("node") or "")}
        elif address in inventory["_hosts"]:
            host = inventory["_hosts"][address]
            out[value] = {"kind": "ecs", "name": str(host.get("name") or ""), "instance_id": str(host.get("instance_id") or "")}
    return out
