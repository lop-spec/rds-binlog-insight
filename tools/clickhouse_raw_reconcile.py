from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from app.clickhouse_client import ClickHouseClient, ClickHouseConfig
from app.clickhouse_manifest import ClickHouseManifest
from app.clickhouse_raw_oss import (
    ClickHouseRawOssConfig,
    build_raw_oss_manifest_rows,
)
from app.metadata import MetadataStore


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


def _audit_cut_gate(
    metadata: MetadataStore,
    pack_manifest: ClickHouseManifest,
    client: ClickHouseClient,
    *,
    manifest_table: str,
) -> dict[str, Any]:
    source = metadata.clickhouse_source_stats()
    tracking = metadata.clickhouse_change_tracking_state()
    pack = pack_manifest.stats()
    rows = client.json_rows(
        f"""
        SELECT count() AS source_parts,
               countIf(oss_length > 0) AS ranged_parts,
               countIf(catalog_ready = 0) AS catalog_unknown,
               sum(row_count) AS source_rows,
               sum(size_bytes) AS source_bytes
        FROM {manifest_table} FINAL
        WHERE is_deleted = 0
        """,
        timeout=60,
    )
    active = rows[0] if rows else {}
    reasons: list[str] = []
    if bool(tracking.get("pending")):
        reasons.append("source queue is pending")
    if int(source["archived_parts"]) != int(source["source_parts"]):
        reasons.append("visible source contains unarchived parts")
    for key in ("source_parts", "ranged_parts", "source_rows", "source_bytes"):
        if int(active.get(key) or 0) != int(source[key]):
            reasons.append(
                f"active manifest {key}={int(active.get(key) or 0)} "
                f"!= source {int(source[key])}"
            )
    if int(pack.get("ready_parts") or 0) != int(source["ranged_parts"]):
        reasons.append(
            "packed ready parts do not match ranged source parts"
        )
    for key in ("pending_parts", "failed_parts", "delete_parts"):
        if int(pack.get(key) or 0):
            reasons.append(f"packed manifest {key}={int(pack[key])}")
    return {
        "ready": not reasons,
        "reasons": reasons,
        "source": source,
        "tracking": tracking,
        "active_manifest": active,
        "packed_manifest": pack,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Apply the durable SQLite source-change queue to the ClickHouse "
            "raw OSS manifest."
        )
    )
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--limit", type=int, default=2048)
    parser.add_argument("--max-rounds", type=int, default=1000)
    parser.add_argument("--mark-complete", action="store_true")
    args = parser.parse_args()
    limit = min(max(int(args.limit), 1), 2048)
    max_rounds = max(int(args.max_rounds), 1)

    config = ClickHouseConfig.from_env()
    raw_config = ClickHouseRawOssConfig.from_env()
    if not raw_config.enabled:
        raise RuntimeError("RDS_BINLOG_CLICKHOUSE_RAW_OSS_ENABLED=1 is required")
    metadata = MetadataStore(
        args.data_dir / "metadata.sqlite3",
        run_migrations=False,
    )
    settings = metadata.load_settings()
    client = ClickHouseClient(config)
    table = f"{config.database}.{raw_config.manifest_table}"
    pack_manifest = ClickHouseManifest(
        args.data_dir
        / "index"
        / "clickhouse"
        / "raw-oss-packed-manifest.sqlite3",
        run_migrations=False,
    )
    started = time.monotonic()
    scanned = 0
    inserted = 0
    acknowledged = 0
    deferred = 0
    rounds = 0
    for _round in range(max_rounds):
        changes = metadata.pending_clickhouse_changes(limit=limit)
        if not changes:
            break
        rounds += 1
        scanned += len(changes)
        applicable = [
            part
            for part in changes
            if not (
                part.get("exists")
                and int(part.get("query_visible") or 0)
                and not str(part.get("oss_key") or "")
            )
        ]
        deferred += len(changes) - len(applicable)
        catalogs = metadata.catalog_store.catalogs(
            [str(part["path"]) for part in applicable if _active_archived(part)]
        )
        rows = build_raw_oss_manifest_rows(
            settings,
            applicable,
            catalogs=catalogs,
        )
        if rows:
            inserted += client.insert_json_rows(table, rows, timeout=120)
        acknowledgements: list[tuple[str, int]] = []
        for part in applicable:
            path = str(part["path"])
            version = int(part.get("change_version") or 0)
            if not _active_archived(part):
                pack_manifest.queue_missing_paths([path])
                acknowledgements.append((path, version))
                continue
            if int(part.get("oss_length") or 0) > 0:
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
                    deferred += 1
            else:
                pack_manifest.queue_missing_paths([path])
                acknowledgements.append((path, version))
        acknowledged += metadata.ack_clickhouse_changes(acknowledgements)
        # If the leading page consists only of an unarchived or not-yet-loaded
        # ranged part, another tight loop cannot make progress.
        if not acknowledgements:
            break
    audit = _audit_cut_gate(
        metadata,
        pack_manifest,
        client,
        manifest_table=table,
    )
    marked_complete = False
    if args.mark_complete and bool(audit["ready"]):
        metadata.mark_clickhouse_change_tracking_complete()
        marked_complete = True
        audit = _audit_cut_gate(
            metadata,
            pack_manifest,
            client,
            manifest_table=table,
        )
    result = {
        "state": "ready" if bool(audit["ready"]) else "pending",
        "rounds": rounds,
        "scanned": scanned,
        "inserted": inserted,
        "acknowledged": acknowledged,
        "deferred": deferred,
        "marked_complete": marked_complete,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "audit": audit,
    }
    print(
        json.dumps(result, ensure_ascii=False, separators=(",", ":")),
        flush=True,
    )
    metadata.close()
    return 0 if bool(audit["ready"]) else 3


if __name__ == "__main__":
    raise SystemExit(main())
