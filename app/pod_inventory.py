"""Which Pod or ECS instance a client IP belongs to, kept as a running inventory of observed bindings.

Session lists, the in-flight table and Mongo slow-log sources only know a client IP. A background thread lists the
Pods of every ACK cluster the service's cloud identity can see (``DescribeClustersV1`` → a 15-minute
``DescribeClusterUserKubeconfig`` kept in memory → ``GET /api/v1/pods``) once a minute, and the region's ECS
instances (private, public and EIP addresses) every ten minutes, and writes ``pod-inventory.json`` in the schema
``pod_lookup`` reads. A Pod keeps its IP from start to deletion, so a binding's interval starts at the Pod's
``startTime`` and ends at the last poll that still saw it; bindings older than ``RETENTION_DAYS`` are dropped.
Pods on the host network (Pod IP = node IP) are skipped: that IP is the node, named by the ECS list.

Only read calls are made; the kubeconfig is a temporary client certificate that lives in memory, its key touches
disk only inside a private temporary directory for the moment ``ssl`` needs a file, and is removed right after.
"""
from __future__ import annotations

import base64
import gzip
import hashlib
import hmac
import ipaddress
import json
import logging
import os
import re
import ssl
import tempfile
import threading
import time
import urllib.parse
import urllib.request
import uuid
from dataclasses import replace
from datetime import datetime
from email.utils import formatdate
from pathlib import Path
from typing import Any, Callable

from .rds_api import RdsRpcClient, shared_ssl_context

LOGGER = logging.getLogger(__name__)

POD_REFRESH_SECONDS = 60
ECS_REFRESH_SECONDS = 600
CLUSTER_REFRESH_SECONDS = 600
RETENTION_DAYS = 30
CS_VERSION = "2015-12-15"


class EcsClient(RdsRpcClient):
    VERSION = "2014-05-26"


def _epoch_us(text: Any) -> int:
    value = str(text or "").strip()
    if not value:
        return 0
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1_000_000)


def _ip(value: Any) -> str:
    try:
        return str(ipaddress.ip_address(str(value or "").strip()))
    except ValueError:
        return ""


def parse_pods(payload: dict[str, Any], cluster: str) -> list[dict[str, Any]]:
    """Bindings of the Pods in one ``PodList`` page that own their IP (not host network, IP assigned)."""
    out = []
    for item in payload.get("items") or []:
        meta, spec, status = item.get("metadata") or {}, item.get("spec") or {}, item.get("status") or {}
        host_ip = _ip(status.get("hostIP"))
        if spec.get("hostNetwork") or not meta.get("uid"):
            continue
        ips = {_ip(entry.get("ip")) for entry in status.get("podIPs") or []} | {_ip(status.get("podIP"))}
        ips.discard("")
        ips.discard(host_ip)
        created = _epoch_us(meta.get("creationTimestamp"))
        started = _epoch_us(status.get("startTime")) or created
        owner = next((ref for ref in meta.get("ownerReferences") or [] if ref.get("controller")), None)
        for ip in sorted(ips):
            out.append({"ip": ip, "cluster": cluster, "namespace": str(meta.get("namespace") or ""),
                        "name": str(meta.get("name") or ""), "uid": str(meta["uid"]),
                        "node": str(spec.get("nodeName") or ""), "host_ip": host_ip,
                        "owner": f"{owner.get('kind')}/{owner.get('name')}" if owner else "",
                        "created_us": created, "started_us": max(started, created)})
    return out


def parse_ecs(payload: dict[str, Any]) -> list[dict[str, str]]:
    """``{ip, name, instance_id}`` for every private, public and EIP address of one DescribeInstances page."""
    out = []
    for item in (payload.get("Instances") or {}).get("Instance") or []:
        name, instance_id = str(item.get("InstanceName") or item.get("InstanceId") or ""), str(item.get("InstanceId") or "")
        addresses = set((item.get("VpcAttributes") or {}).get("PrivateIpAddress", {}).get("IpAddress") or [])
        addresses |= set((item.get("PublicIpAddress") or {}).get("IpAddress") or [])
        addresses |= set((item.get("InnerIpAddress") or {}).get("IpAddress") or [])
        addresses.add((item.get("EipAddress") or {}).get("IpAddress") or "")
        for eni in (item.get("NetworkInterfaces") or {}).get("NetworkInterface") or []:
            for entry in (eni.get("PrivateIpSets") or {}).get("PrivateIpSet") or []:
                addresses.add(entry.get("PrivateIpAddress") or "")
        for address in sorted(filter(None, map(_ip, addresses))):
            out.append({"ip": address, "name": name, "instance_id": instance_id})
    return out


def parse_kubeconfig(text: str) -> dict[str, str]:
    """server / CA / client certificate / key of the single-cluster kubeconfig ACK returns."""
    def field(name: str) -> str:
        match = re.search(rf"^\s*{re.escape(name)}:\s*(\S+)\s*$", text, re.MULTILINE)
        return match.group(1).strip("'\"") if match else ""
    config = {"server": field("server"), "ca": field("certificate-authority-data"),
              "cert": field("client-certificate-data"), "key": field("client-key-data")}
    if not config["server"].startswith("https://") or not all(config.values()):
        raise ValueError("kubeconfig lacks server or client certificate")
    return config


def merge_bindings(previous: list[dict[str, Any]], seen: list[dict[str, Any]], now_us: int,
                   retention_us: int) -> list[dict[str, Any]]:
    """Extend the bindings seen again, add new ones, drop those last seen before the retention horizon."""
    merged: dict[tuple[str, str], dict[str, Any]] = {(item["uid"], item["ip"]): dict(item) for item in previous}
    for item in seen:
        key = (item["uid"], item["ip"])
        entry = merged.get(key)
        if entry is None:
            entry = {k: v for k, v in item.items() if k != "started_us"}
            entry["first_seen_us"] = min(item["started_us"] or now_us, now_us)
            merged[key] = entry
        entry["last_seen_us"] = now_us
    horizon = now_us - retention_us
    return sorted((item for item in merged.values() if item["last_seen_us"] >= horizon),
                  key=lambda item: (item["ip"], item["first_seen_us"]))


class PodInventory:
    def __init__(self, settings_loader: Callable[[], Any], credential_loader: Callable[[Any], Any], path: Path) -> None:
        self._settings_loader = settings_loader
        self._credential_loader = credential_loader
        self.path = path
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._clusters: list[dict[str, str]] = []
        self._clusters_at = 0.0
        self._kube: dict[str, tuple[float, dict[str, str]]] = {}
        self._ecs_at = 0.0
        self._failing: set[str] = set()
        self.state: dict[str, Any] = {"last_ok_us": 0, "errors": {}}

    # ---------- cloud calls ----------

    def _settings_and_credential(self) -> tuple[Any, Any]:
        settings = self._settings_loader()
        credential = self._credential_loader(settings.credential_target)
        if credential is None:
            raise RuntimeError("cloud_credential_unavailable")
        return settings, credential

    def _cs_get(self, path: str, query: dict[str, str]) -> dict[str, Any]:
        settings, credential = self._settings_and_credential()
        cred = credential.current()
        host = os.environ.get("RDS_BINLOG_CS_ENDPOINT", f"cs-vpc.{settings.region_id}.aliyuncs.com")
        headers = {"Accept": "application/json", "Date": formatdate(usegmt=True),
                   "x-acs-signature-method": "HMAC-SHA1", "x-acs-signature-nonce": str(uuid.uuid4()),
                   "x-acs-signature-version": "1.0", "x-acs-version": CS_VERSION}
        if cred.security_token:
            headers["x-acs-security-token"] = cred.security_token
        acs = "".join(f"{key}:{headers[key]}\n" for key in sorted(headers) if key.startswith("x-acs-"))
        resource = path + ("?" + "&".join(f"{key}={query[key]}" for key in sorted(query)) if query else "")
        signed = f"GET\n{headers['Accept']}\n\n\n{headers['Date']}\n{acs}{resource}"
        digest = hmac.new(cred.access_key_secret.encode(), signed.encode(), hashlib.sha1).digest()
        headers["Authorization"] = f"acs {cred.access_key_id}:{base64.b64encode(digest).decode()}"
        url = f"https://{host}{path}" + ("?" + urllib.parse.urlencode(query) if query else "")
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=20,
                                    context=shared_ssl_context()) as response:
            return json.loads(response.read().decode("utf-8"))

    def _clusters_list(self) -> list[dict[str, str]]:
        if self._clusters and time.monotonic() - self._clusters_at < CLUSTER_REFRESH_SECONDS:
            return self._clusters
        settings, _ = self._settings_and_credential()
        payload = self._cs_get("/api/v1/clusters", {"region_id": settings.region_id, "page_size": "100"})
        self._clusters = [{"id": str(item["cluster_id"]), "name": str(item.get("name") or item["cluster_id"])}
                          for item in payload.get("clusters") or [] if item.get("state") == "running"]
        self._clusters_at = time.monotonic()
        return self._clusters

    def _kubeconfig(self, cluster_id: str) -> dict[str, str]:
        hit = self._kube.get(cluster_id)
        if hit and hit[0] - time.time() > 120:
            return hit[1]
        payload = self._cs_get(f"/k8s/{cluster_id}/user_config",
                               {"PrivateIpAddress": "true", "TemporaryDurationMinutes": "15"})
        config = parse_kubeconfig(str(payload.get("config") or ""))
        expires = _epoch_us(payload.get("expiration")) / 1_000_000 or time.time() + 600
        self._kube[cluster_id] = (expires, config)
        return config

    @staticmethod
    def _tls(config: dict[str, str]) -> ssl.SSLContext:
        context = ssl.create_default_context(cadata=base64.b64decode(config["ca"]).decode())
        with tempfile.TemporaryDirectory(prefix="kube-") as folder:
            cert, key = Path(folder) / "c.pem", Path(folder) / "k.pem"
            for target, value in ((cert, config["cert"]), (key, config["key"])):
                descriptor = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(descriptor, "wb") as handle:
                    handle.write(base64.b64decode(value))
            context.load_cert_chain(str(cert), str(key))
        return context

    def _pods(self, cluster: dict[str, str]) -> list[dict[str, Any]]:
        config = self._kubeconfig(cluster["id"])
        context = self._tls(config)
        out, token = [], ""
        while True:
            query = {"limit": "500", "fieldSelector": "status.phase!=Succeeded,status.phase!=Failed"}
            if token:
                query["continue"] = token
            request = urllib.request.Request(f"{config['server']}/api/v1/pods?{urllib.parse.urlencode(query)}",
                                             headers={"Accept": "application/json", "Accept-Encoding": "gzip"})
            with urllib.request.urlopen(request, timeout=30, context=context) as response:
                body = response.read()
                if response.headers.get("Content-Encoding") == "gzip":
                    body = gzip.decompress(body)
            payload = json.loads(body)
            out.extend(parse_pods(payload, cluster["name"]))
            token = str((payload.get("metadata") or {}).get("continue") or "")
            if not token:
                return out

    def _ecs(self) -> list[dict[str, str]]:
        settings, credential = self._settings_and_credential()
        endpoint = os.environ.get("RDS_BINLOG_ECS_ENDPOINT", f"https://ecs-vpc.{settings.region_id}.aliyuncs.com")
        client = EcsClient(replace(settings, endpoint=endpoint), credential, timeout=20)
        out, page = [], 1
        while True:
            payload = client.call("DescribeInstances", {"RegionId": settings.region_id, "PageSize": 100, "PageNumber": page})
            out.extend(parse_ecs(payload))
            if page * 100 >= int(payload.get("TotalCount") or 0):
                return out
            page += 1

    # ---------- refresh ----------

    def _load(self) -> dict[str, Any]:
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if data.get("schema_version") == 1:
                return data
        except (OSError, ValueError):
            pass
        return {"schema_version": 1, "instance_ids": ["*"], "bindings": [], "hosts": []}

    def _failed(self, scope: str, exc: Exception) -> None:
        self.state["errors"][scope] = f"{type(exc).__name__}: {exc}"[:300]
        if scope not in self._failing:
            LOGGER.warning("pod inventory: %s unavailable (%s: %s); keeping the last known bindings",
                           scope, type(exc).__name__, exc)
        self._failing.add(scope)

    def _ok(self, scope: str) -> None:
        self.state["errors"].pop(scope, None)
        self._failing.discard(scope)

    def refresh(self) -> None:
        data = self._load()
        now_us = time.time_ns() // 1000
        seen: list[dict[str, Any]] = []
        try:
            clusters = self._clusters_list()
            self._ok("clusters")
        except Exception as exc:  # noqa: BLE001 - a failed listing keeps the last inventory
            self._failed("clusters", exc)
            clusters = []
        for cluster in clusters:
            if self._stop.is_set():
                return
            try:
                seen.extend(self._pods(cluster))
                self._ok(cluster["name"])
            except Exception as exc:  # noqa: BLE001 - one unreadable cluster must not hide the others
                self._kube.pop(cluster["id"], None)
                self._failed(cluster["name"], exc)
        data["bindings"] = merge_bindings(data.get("bindings") or [], seen, now_us, RETENTION_DAYS * 86_400_000_000)
        if time.monotonic() - self._ecs_at >= ECS_REFRESH_SECONDS or not data.get("hosts"):
            try:
                data["hosts"] = self._ecs()
                self._ecs_at = time.monotonic()
                self._ok("ecs")
            except Exception as exc:  # noqa: BLE001
                self._failed("ecs", exc)
        data.update(schema_version=1, instance_ids=["*"], generated_us=now_us)
        temporary = self.path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        os.replace(temporary, self.path)
        if seen:
            self.state["last_ok_us"] = now_us
        self.state["pods_seen"] = len(seen)

    def start(self) -> None:
        if self._thread is None:
            self._thread = threading.Thread(target=self._run, name="pod-inventory", daemon=True)
            self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            try:
                self.refresh()
            except Exception:  # noqa: BLE001 - the loop must survive any single bad pass
                LOGGER.exception("pod inventory refresh failed")
            self._stop.wait(max(POD_REFRESH_SECONDS - (time.monotonic() - started), 5.0))

    def shutdown(self) -> None:
        self._stop.set()
