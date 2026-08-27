from __future__ import annotations

from typing import Any

from .clickhouse_manifest import ClickHouseManifest, part_identity
from .metadata import MetadataStore, TABULARIS_AUDIT_FILE_PREFIX


def reconcile_pending_source_changes(
    metadata: MetadataStore,
    manifest: ClickHouseManifest,
    *,
    limit: int = 2048,
) -> dict[str, int]:
    """Hand durable source mutations to the manifest before acknowledging.

    The two SQLite databases cannot share one transaction. Ordering the commits
    manifest-first and acknowledging with the source change version makes every
    crash mode conservative: a retry may repeat an idempotent reconcile, but a
    user query can never observe a clean source gate before the manifest has the
    corresponding load/delete state.
    """

    changes = metadata.pending_clickhouse_changes(limit=max(int(limit), 0))
    eligible: list[dict[str, Any]] = []
    removed: list[str] = []
    acknowledgements: list[tuple[str, int]] = []
    deferred = 0
    for change in changes:
        path = str(change.get("path") or "")
        version = int(change.get("change_version") or 0)
        current_database_part = bool(
            change.get("exists")
            and int(change.get("query_visible") or 0)
            and not str(change.get("log_file_name") or "").startswith(
                TABULARIS_AUDIT_FILE_PREFIX
            )
        )
        if current_database_part:
            if str(change.get("oss_key") or "") and part_identity(change):
                eligible.append(change)
                acknowledgements.append((path, version))
            else:
                # The source row is visible but its verified OSS object is not
                # ready yet. Keep the durable gate closed until archival emits
                # a newer coalesced change.
                deferred += 1
        else:
            removed.append(path)
            acknowledgements.append((path, version))

    queued = 0
    if eligible:
        result = manifest.reconcile(
            eligible,
            start_epoch_us=min(
                int(part.get("min_event_epoch_us") or 0) for part in eligible
            ),
            end_epoch_us=max(
                int(part.get("max_event_epoch_us") or 0) for part in eligible
            ),
            sweep_unseen=False,
            preserve_reconcile_state=True,
        )
        queued = int(result.get("queued_parts") or 0)
    removed_states = manifest.queue_missing_paths(removed)
    acknowledged = metadata.ack_clickhouse_changes(acknowledgements)
    return {
        "scanned": len(changes),
        "queued": queued,
        "removed": len(removed),
        "removed_states": int(removed_states),
        "deferred": deferred,
        "acknowledged": int(acknowledged),
    }
