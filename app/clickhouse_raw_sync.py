from __future__ import annotations

from typing import Any

from .clickhouse_client import ClickHouseClient
from .clickhouse_manifest import ClickHouseManifest
from .clickhouse_raw_oss import build_raw_oss_manifest_rows
from .config import Settings
from .metadata import MetadataStore


ACTIVE_PACK_STATES = frozenset({"pending", "loading", "ready", "load_failed"})


def _active_archived(part: dict[str, Any]) -> bool:
    return bool(
        part.get("exists")
        and int(part.get("query_visible") or 0)
        and str(part.get("oss_key") or "")
    )


def _ranged_ready(
    manifest: ClickHouseManifest,
    part: dict[str, Any],
) -> bool:
    return bool(manifest.coverage([part]).get("complete"))


def apply_pending_raw_oss_changes(
    metadata: MetadataStore,
    pack_manifest: ClickHouseManifest,
    client: ClickHouseClient,
    settings: Settings,
    *,
    manifest_table: str,
    limit: int = 256,
    max_rounds: int = 4,
) -> dict[str, int]:
    """Apply a bounded, version-acknowledged source queue page.

    A ranged part remains unacknowledged until its native exception rows are
    verified. Its active-manifest row is inserted only once: the durable pack
    job proves the preceding ClickHouse manifest insert committed, so retry
    loops cannot bloat the ReplacingMergeTree.
    """

    batch_limit = min(max(int(limit), 1), 2048)
    rounds_limit = max(int(max_rounds), 1)
    result = {
        "rounds": 0,
        "scanned": 0,
        "inserted": 0,
        "acknowledged": 0,
        "deferred": 0,
    }
    for _round in range(rounds_limit):
        changes = metadata.pending_clickhouse_changes(limit=batch_limit)
        if not changes:
            break
        result["rounds"] += 1
        result["scanned"] += len(changes)
        applicable = [
            part
            for part in changes
            if not (
                part.get("exists")
                and int(part.get("query_visible") or 0)
                and not str(part.get("oss_key") or "")
            )
        ]
        result["deferred"] += len(changes) - len(applicable)
        catalogs = metadata.catalog_store.catalogs(
            [str(part["path"]) for part in applicable if _active_archived(part)]
        )

        states: dict[str, str | None] = {}
        manifest_changes: list[dict[str, Any]] = []
        for part in applicable:
            path = str(part["path"])
            state: str | None = None
            if _active_archived(part) and int(part.get("oss_length") or 0) > 0:
                state = pack_manifest.part_status(
                    path,
                    str(part.get("logical_part_id") or ""),
                )
            states[path] = state
            if state not in ACTIVE_PACK_STATES:
                manifest_changes.append(part)

        rows = build_raw_oss_manifest_rows(
            settings,
            manifest_changes,
            catalogs=catalogs,
        )
        if rows:
            result["inserted"] += client.insert_json_rows(
                manifest_table,
                rows,
                timeout=120,
            )

        acknowledgements: list[tuple[str, int]] = []
        for part in applicable:
            path = str(part["path"])
            version = int(part.get("change_version") or 0)
            if not _active_archived(part):
                pack_manifest.queue_missing_paths([path])
                acknowledgements.append((path, version))
                continue
            if int(part.get("oss_length") or 0) > 0:
                if states.get(path) not in ACTIVE_PACK_STATES:
                    pack_manifest.reconcile(
                        [part],
                        start_epoch_us=0,
                        end_epoch_us=int(part.get("max_event_epoch_us") or 0),
                        source_parts=1,
                        sweep_unseen=False,
                        preserve_reconcile_state=True,
                    )
                if _ranged_ready(pack_manifest, part):
                    acknowledgements.append((path, version))
                else:
                    result["deferred"] += 1
            else:
                pack_manifest.queue_missing_paths([path])
                acknowledgements.append((path, version))
        result["acknowledged"] += metadata.ack_clickhouse_changes(
            acknowledgements
        )
        if not acknowledgements:
            break
    return result
