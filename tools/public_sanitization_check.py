"""Fail CI when tracked files or Git history contain release-sensitive material."""

from __future__ import annotations

import argparse
import hashlib
import ipaddress
import re
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
TOKEN_RE = re.compile(rb"[A-Za-z0-9_.-]+")
IPV4_RE = re.compile(rb"(?<![0-9.])(?:[0-9]{1,3}\.){3}[0-9]{1,3}(?![0-9.])")
RDS_ID_RE = re.compile(rb"\brm-[a-z0-9]{16,}\b", re.IGNORECASE)
HOME_PATH_RE = re.compile(rb"/home/([A-Za-z0-9._-]+)/")
OSS_BUCKET_HOST_RE = re.compile(
    rb"(?:https?://)?([a-z0-9][a-z0-9.-]{1,62})\.oss-[a-z0-9.-]+\.aliyuncs\.com",
    re.IGNORECASE,
)
SECRET_PATTERNS = {
    "private-key": re.compile(rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
    "github-token": re.compile(rb"\bgh[pousr]_[A-Za-z0-9]{30,}\b"),
    "cloud-access-key": re.compile(rb"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b|\bLTAI[A-Za-z0-9]{12,24}\b"),
    "jwt": re.compile(rb"\beyJ[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\b"),
}

# SHA-256 digests let the gate reject known private identifiers without
# publishing the identifiers themselves in this public repository.
BANNED_TOKEN_DIGESTS = {
    "41cc8488a0db5c0cf9073619063346ecff9d3ec1310ba768c4905f78e8f67bb7",
    "c27338c453067b437471afbce792704d816112c6031bb996b62c698ea599ef80",
    "60d9c3b1ca51ade7db9fc61e92ed236429e360ce0f2542450e91a408157487a4",
    "8e721ed9ee1c69c9a323440de29cbefa09bc0b600247fc7753bdcee9c5d6722d",
    "290abd3147a14f4d0c3ccfae9aaa921c223ac755e6bbee01d535ec2e808f6146",
    "700ab86bfeead33ad06430d68af54876510bc4d73843cac030348c4075f09a3f",
    "d98986c4d25cbaa93318437abec0be5249292b9e2642cdb823d287d6a02be1fe",
    "9fa269721f7f10662e7e528dadc9d6a6cf2d496786cba5085d7356e36d281cc7",
    "6863335d75ae1caa8586ab6d8ae46233babae2520e2e7df2a0853a9fb02df47a",
    "71925c0dd73e00091ec2bb900f4f5bd600f33743e862876711f611d81ee9ab4c",
    "8b4c01949831b91b2b8160c4acafb9df8668db536ac80f5dc5f29e996ccc58b3",
    "d3836bdeb5151d5758ea41378f5c18144ac9f2d4d3fb6de3b25318c6ae13d41e",
    "9b89025ce7a6d932b28f6e15132a70d402f723874a425e9b4c7cc3b179fa66ce",
    "c8e241b6c67d4f98c76ee4d8c54215d8e7df6a1227b50da89370d53c9b05c178",
    "4f2322145e6627328d13b6e71706787442f79d3047ea1530ecadd751f9f6cffb",
    "f75142913198a8e0ae79d7b076aa43641ddd9deaec247942c474ec7c76967d1a",
}
SENSITIVE_NAME_RE = re.compile(
    r"(^|/)(?:\.env(?:\..*)?|id_(?:rsa|dsa|ecdsa|ed25519)|.*\.(?:pem|p12|pfx|key|sqlite3?))(?:$|/)",
    re.IGNORECASE,
)


def _git(*args: str) -> bytes:
    return subprocess.check_output(["git", *args], cwd=ROOT)


def _allowed_public_ip(value: str) -> bool:
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return True
    documentation = (
        ipaddress.ip_network("192.0.2.0/24"),
        ipaddress.ip_network("198.51.100.0/24"),
        ipaddress.ip_network("203.0.113.0/24"),
    )
    return bool(
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_unspecified
        or any(address in network for network in documentation)
    )


def _scan(label: str, data: bytes) -> list[str]:
    if b"\0" in data[:8192]:
        return []
    findings: list[str] = []
    for name, pattern in SECRET_PATTERNS.items():
        if pattern.search(data):
            findings.append(name)
    for raw in IPV4_RE.findall(data):
        value = raw.decode("ascii", "ignore")
        if not _allowed_public_ip(value):
            findings.append("public-ipv4")
            break
    for raw in RDS_ID_RE.findall(data):
        value = raw.lower()
        if not any(marker in value for marker in (b"example", b"sample", b"test", b"fake", b"demo")):
            findings.append("rds-instance-id")
            break
    for user in HOME_PATH_RE.findall(data):
        if user.lower() not in {b"example", b"app", b"service"}:
            findings.append("user-home-path")
            break
    for bucket in OSS_BUCKET_HOST_RE.findall(data):
        if not bucket.lower().startswith((b"example", b"sample", b"test", b"demo")):
            findings.append("oss-bucket-host")
            break
    if any(hashlib.sha256(token).hexdigest() in BANNED_TOKEN_DIGESTS for token in TOKEN_RE.findall(data)):
        findings.append("private-identifier")
    return [f"{label}: {item}" for item in sorted(set(findings))]


def _working_tree() -> list[tuple[str, bytes]]:
    rows: list[tuple[str, bytes]] = []
    for raw in _git("ls-files", "-z").split(b"\0"):
        if not raw:
            continue
        name = raw.decode("utf-8", "surrogateescape")
        path = ROOT / name
        if path.is_file():
            rows.append((name, path.read_bytes()))
    return rows


def _history() -> list[tuple[str, bytes]]:
    rows: list[tuple[str, bytes]] = []
    seen: set[str] = set()
    for line in _git("rev-list", "--objects", "--all").decode("utf-8", "replace").splitlines():
        object_id, separator, name = line.partition(" ")
        if not separator or not name or object_id in seen:
            continue
        kind = _git("cat-file", "-t", object_id).strip()
        if kind != b"blob":
            continue
        seen.add(object_id)
        rows.append((f"history:{object_id[:12]}:{name}", _git("cat-file", "blob", object_id)))
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--history", action="store_true")
    args = parser.parse_args()
    findings: list[str] = []
    rows = _working_tree()
    if args.history:
        rows.extend(_history())
    for name, data in rows:
        if SENSITIVE_NAME_RE.search(name) and not name.endswith(".env.example"):
            findings.append(f"{name}: sensitive-file-name")
        findings.extend(_scan(name, data))
    if findings:
        for finding in sorted(set(findings)):
            print(finding)
        print(f"sanitization failed: {len(set(findings))} finding(s)", file=sys.stderr)
        return 1
    print(f"sanitization passed: {len(rows)} tracked blob(s) scanned")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
