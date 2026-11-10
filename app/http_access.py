"""Network containment until an authenticated public entry is explicitly configured.

Host/Origin are NOT identities. Only the TCP peer is checked; forwarding headers
never confer trust. Deployments may list their isolated worker network in the
owner-controlled data/http-access.json. Missing policy means loopback only.
"""
from __future__ import annotations

import ipaddress
import json
from pathlib import Path
from typing import Any


def load_networks(root: Path) -> tuple[Any, ...]:
    path = Path(root) / 'http-access.json'
    raw = json.loads(path.read_text('utf-8')) if path.exists() else {}
    entries = raw.get('trustedNetworks', [])
    if not isinstance(entries, list) or len(entries) > 32:
        raise ValueError('http-access trustedNetworks must be a bounded array')
    networks = [ipaddress.ip_network('127.0.0.0/8'), ipaddress.ip_network('::1/128')]
    for entry in entries:
        network = ipaddress.ip_network(entry, strict=True)
        # Network containment must never silently become a public allow-all.
        if not network.is_private or network.prefixlen == 0:
            raise ValueError('http-access accepts explicit private networks only')
        networks.append(network)
    return tuple(networks)


def peer_allowed(peer: str, networks: tuple[Any, ...]) -> bool:
    try:
        address = ipaddress.ip_address(peer)
        if getattr(address, 'ipv4_mapped', None):
            address = address.ipv4_mapped
        return any(address in network for network in networks)
    except ValueError:
        return False


def validate_event_query(result: dict[str, Any]) -> dict[str, Any]:
    result['source'] = str(result.get('source') or '').strip().lower()
    result['keyword_mode'] = str(result.get('keyword_mode') or 'AND').upper()
    if result['source'] not in {'', 'all', 'audit', 'database', 'slowlog', 'binlog'}:
        raise ValueError('source 取值不受支持')
    if result['keyword_mode'] not in {'AND', 'OR'}:
        raise ValueError('keywordMode 只能是 AND 或 OR')
    if not 1 <= result['limit'] <= 1000 or not 0 <= result['offset'] <= 100000:
        raise ValueError('limit 必须在 1..1000，offset 必须在 0..100000')
    return result
