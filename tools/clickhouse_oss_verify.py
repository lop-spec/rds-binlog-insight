from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from app.clickhouse_client import (
    MAX_PART_STATE_IDENTITIES,
    ClickHouseClient,
    ClickHouseConfig,
    validate_part_state_batch_size,
)
from app.clickhouse_manifest import ClickHouseManifest, part_identity
from app.clickhouse_oss import (
    ClickHouseOssConfig,
    history_start_epoch_us,
    split_direct_and_ranged_parts,
)
from app.clickhouse_stage import StageTables, staged_remote_matches
from app.config import ensure_data_dirs
from app.maintenance_status import write_json_status
from app.metadata import MetadataStore


SOURCE_LIMIT = 1_000_001
STATUS_NAME = "clickhouse-oss-verify-status.json"


def _status_name(manifest_name: str) -> str:
    if str(manifest_name) == "oss-all-manifest.sqlite3":
        return STATUS_NAME
    stem = Path(str(manifest_name)).stem.removesuffix("-manifest")
    return f"clickhouse-{stem}-verify-status.json"


def inventory_snapshot(parts: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a stable, credential-free identity for one source inventory."""

    digest = hashlib.sha256()
    identities: set[str] = set()
    duplicate_identities: list[str] = []
    rows = 0
    source_bytes = 0
    for part in sorted(
        parts,
        key=lambda item: (part_identity(item), str(item.get("path") or "")),
    ):
        identity = part_identity(part)
        if identity in identities and len(duplicate_identities) < 20:
            duplicate_identities.append(identity)
        identities.add(identity)
        row_count = max(int(part.get("row_count") or 0), 0)
        size_bytes = max(int(part.get("size_bytes") or 0), 0)
        rows += row_count
        source_bytes += size_bytes
        fields = (
            identity,
            str(part.get("path") or ""),
            str(part.get("sha256") or ""),
            str(max(int(part.get("content_revision") or 0), 0)),
            str(row_count),
            str(part.get("oss_key") or ""),
            str(max(int(part.get("oss_offset") or 0), 0)),
            str(max(int(part.get("oss_length") or 0), 0)),
        )
        digest.update("\x1f".join(fields).encode("utf-8"))
        digest.update(b"\n")
    direct, ranged = split_direct_and_ranged_parts(parts)
    return {
        "parts": len(parts),
        "rows": rows,
        "source_bytes": source_bytes,
        "direct_parts": len(direct),
        "ranged_parts": len(ranged),
        "identity_sha256": digest.hexdigest(),
        "duplicate_identities": duplicate_identities,
    }


def _state_error(part: dict[str, Any], state: dict[str, Any]) -> str:
    if staged_remote_matches(part, state):
        return ""
    identity = part_identity(part)
    expected_rows = max(int(part.get("row_count") or 0), 0)
    return (
        f"{identity}: rows={int(state.get('rows') or 0)}/{expected_rows}, "
        f"name_rows={int(state.get('name_rows') or 0)}/{expected_rows}, "
        f"sha={str(state.get('sha256') or '')[:12]}/"
        f"{str(part.get('sha256') or '')[:12]}, "
        f"name_sha={str(state.get('name_sha256') or '')[:12]}/"
        f"{str(part.get('sha256') or '')[:12]}, "
        f"revision={int(state.get('min_revision') or 0)}.."
        f"{int(state.get('max_revision') or 0)}/"
        f"{int(part.get('content_revision') or 0)}, "
        f"name_revision={int(state.get('name_min_revision') or 0)}.."
        f"{int(state.get('name_max_revision') or 0)}/"
        f"{int(part.get('content_revision') or 0)}"
    )


def verify_remote_parts(
    client: Any,
    parts: list[dict[str, Any]],
    *,
    time_table: str,
    name_table: str,
    batch_size: int = MAX_PART_STATE_IDENTITIES,
    mismatch_limit: int = 20,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Verify every source identity against both physical query tables."""

    size = validate_part_state_batch_size(batch_size)
    mismatches: list[str] = []
    mismatch_count = 0
    checked_rows = 0
    for offset in range(0, len(parts), size):
        batch = parts[offset : offset + size]
        states = client.paired_part_states_for_tables(
            [part_identity(part) for part in batch],
            time_table=time_table,
            name_table=name_table,
        )
        for part in batch:
            error = _state_error(part, states.get(part_identity(part), {}))
            if error:
                mismatch_count += 1
                if len(mismatches) < max(int(mismatch_limit), 1):
                    mismatches.append(error)
            checked_rows += max(int(part.get("row_count") or 0), 0)
        if progress is not None:
            progress(
                {
                    "checked_parts": min(offset + len(batch), len(parts)),
                    "total_parts": len(parts),
                    "checked_rows": checked_rows,
                    "mismatch_parts": mismatch_count,
                }
            )
    return {
        "checked_parts": len(parts),
        "checked_rows": checked_rows,
        "mismatch_parts": mismatch_count,
        "mismatches": mismatches,
        "exact": mismatch_count == 0,
    }


def evaluate_full_gate(
    *,
    source_inventory: dict[str, Any],
    source_parts: int,
    manifest_coverage: dict[str, Any],
    manifest_stats: dict[str, Any],
    remote: dict[str, Any],
    time_summary: dict[str, Any],
    name_summary: dict[str, Any],
    stage_time_summary: dict[str, Any],
    stage_name_summary: dict[str, Any],
    object_statuses: dict[str, dict[str, Any]],
    incremental_mv_enabled: bool,
    journal_exists: bool,
    source_stable: bool,
) -> dict[str, Any]:
    expected_parts = int(source_inventory.get("parts") or 0)
    expected_rows = int(source_inventory.get("rows") or 0)
    checks = {
        "source_inventory_nonempty": expected_parts > 0,
        "source_inventory_not_truncated": int(source_parts) < SOURCE_LIMIT,
        "all_source_parts_archived": int(source_parts) == expected_parts,
        "source_identities_unique": not bool(
            source_inventory.get("duplicate_identities")
        ),
        "manifest_coverage_complete": bool(manifest_coverage.get("complete")),
        "manifest_parts_complete": (
            int(manifest_coverage.get("total_parts") or 0) == expected_parts
            and int(manifest_coverage.get("covered_parts") or 0) == expected_parts
        ),
        "manifest_rows_complete": (
            int(manifest_coverage.get("covered_rows") or 0) == expected_rows
        ),
        "manifest_queue_clean": (
            int(manifest_stats.get("ready_parts") or 0) == expected_parts
            and int(manifest_stats.get("pending_parts") or 0) == 0
            and int(manifest_stats.get("failed_parts") or 0) == 0
            and int(manifest_stats.get("delete_parts") or 0) == 0
            and not str(manifest_stats.get("last_error") or "")
        ),
        "remote_parts_exact": (
            bool(remote.get("exact"))
            and int(remote.get("checked_parts") or 0) == expected_parts
            and int(remote.get("checked_rows") or 0) == expected_rows
        ),
        "time_table_rows_exact": int(time_summary.get("rows") or 0)
        == expected_rows,
        "name_table_rows_exact": int(name_summary.get("rows") or 0)
        == expected_rows,
        "staging_empty": (
            int(stage_time_summary.get("rows") or 0) == 0
            and int(stage_name_summary.get("rows") or 0) == 0
        ),
        "physical_tables_exist": all(
            bool(object_statuses.get(name, {}).get("exists"))
            and str(object_statuses.get(name, {}).get("engine") or "")
            == "MergeTree"
            for name in ("time", "name", "stage_time", "stage_name")
        ),
        "incremental_mv_ready": bool(incremental_mv_enabled)
        and bool(object_statuses.get("materialized_view", {}).get("exists"))
        and str(
            object_statuses.get("materialized_view", {}).get("engine") or ""
        )
        == "MaterializedView",
        "journal_absent": not journal_exists,
        "source_snapshot_stable": bool(source_stable),
    }
    return {"ok": all(checks.values()), "checks": checks}


def evaluate_ready_pilot_gate(
    *,
    source_parts: list[dict[str, Any]],
    source_identity_errors: list[str],
    manifest_stats: dict[str, Any],
    remote: dict[str, Any],
    time_summary: dict[str, Any],
    name_summary: dict[str, Any],
    stage_time_summary: dict[str, Any],
    stage_name_summary: dict[str, Any],
    journal_exists: bool,
) -> dict[str, Any]:
    ready_parts = int(manifest_stats.get("ready_parts") or 0)
    ready_rows = int(manifest_stats.get("ready_rows") or 0)
    checks = {
        "ready_inventory_nonempty": bool(source_parts),
        "ready_source_identities_current": not source_identity_errors,
        "selected_ready_parts_exact": int(remote.get("checked_parts") or 0)
        == len(source_parts),
        "selected_ready_parts_remote_exact": bool(remote.get("exact")),
        "selected_within_ready_manifest": len(source_parts) <= ready_parts,
        "time_table_rows_equal_ready_manifest": int(
            time_summary.get("rows") or 0
        )
        == ready_rows,
        "name_table_rows_equal_ready_manifest": int(
            name_summary.get("rows") or 0
        )
        == ready_rows,
        "staging_empty": (
            int(stage_time_summary.get("rows") or 0) == 0
            and int(stage_name_summary.get("rows") or 0) == 0
        ),
        "journal_absent": not journal_exists,
        "manifest_has_no_failures": (
            int(manifest_stats.get("failed_parts") or 0) == 0
            and int(manifest_stats.get("delete_parts") or 0) == 0
            and not str(manifest_stats.get("last_error") or "")
        ),
    }
    return {"ok": all(checks.values()), "checks": checks}


def _eligible_parts(
    metadata: MetadataStore,
    *,
    start_us: int,
    end_us: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    source_parts = metadata.parts_in_range(
        start_epoch_us=start_us,
        end_epoch_us=end_us,
        limit=SOURCE_LIMIT,
        source="database",
    )
    eligible = [
        part
        for part in source_parts
        if str(part.get("oss_key") or "") and part_identity(part)
    ]
    return source_parts, eligible


def _run_ready_pilot(args: argparse.Namespace) -> dict[str, Any]:
    paths = ensure_data_dirs(Path(args.data_dir))
    config = ClickHouseConfig.from_env()
    oss_config = ClickHouseOssConfig.from_env()
    if not oss_config.enabled or config.ingest_mode != "query":
        raise RuntimeError("ClickHouse OSS query-mode verification is not enabled")
    tables = StageTables.from_configs(config, oss_config)
    metadata = MetadataStore(
        Path(args.data_dir) / "metadata.sqlite3",
        run_migrations=False,
    )
    manifest = ClickHouseManifest(
        paths["index"] / "clickhouse" / oss_config.manifest_name,
        run_migrations=False,
    )
    client = ClickHouseClient(config)
    with manifest.connection() as connection:
        rows = connection.execute(
            """
            SELECT part_path, logical_part_id
            FROM clickhouse_parts
            WHERE status = 'ready'
            ORDER BY max_event_epoch_us DESC, part_path DESC
            LIMIT ?
            """,
            (int(args.ready_pilot),),
        ).fetchall()
    source_parts: list[dict[str, Any]] = []
    source_identity_errors: list[str] = []
    for row in rows:
        path = str(row["part_path"])
        identity = str(row["logical_part_id"])
        current = metadata.part_by_path(path)
        if current is None or part_identity(current) != identity:
            if len(source_identity_errors) < 20:
                source_identity_errors.append(identity)
            continue
        source_parts.append(current)
    remote = verify_remote_parts(
        client,
        source_parts,
        time_table=tables.final_time,
        name_table=tables.final_name,
        batch_size=args.batch_size,
        mismatch_limit=args.mismatch_limit,
    )
    manifest_stats = manifest.stats()
    time_summary = client.table_storage_summary(tables.final_time)
    name_summary = client.table_storage_summary(tables.final_name)
    stage_time_summary = client.table_storage_summary(tables.stage_time)
    stage_name_summary = client.table_storage_summary(tables.stage_name)
    journal_path = manifest.path.with_suffix(".stage-journal.json")
    gate = evaluate_ready_pilot_gate(
        source_parts=source_parts,
        source_identity_errors=source_identity_errors,
        manifest_stats=manifest_stats,
        remote=remote,
        time_summary=time_summary,
        name_summary=name_summary,
        stage_time_summary=stage_time_summary,
        stage_name_summary=stage_name_summary,
        journal_exists=journal_path.exists(),
    )
    return {
        "ok": bool(gate["ok"]),
        "mode": "ready-pilot",
        "cutover_eligible": False,
        "requested_ready_parts": int(args.ready_pilot),
        "source_identity_errors": source_identity_errors,
        "manifest_stats": manifest_stats,
        "remote": remote,
        "tables": {
            "time": time_summary,
            "name": name_summary,
            "stage_time": stage_time_summary,
            "stage_name": stage_name_summary,
        },
        "journal_exists": journal_path.exists(),
        "gate": gate,
    }


def _run(args: argparse.Namespace) -> dict[str, Any]:
    paths = ensure_data_dirs(Path(args.data_dir))
    config = ClickHouseConfig.from_env()
    oss_config = ClickHouseOssConfig.from_env()
    if not oss_config.enabled or config.ingest_mode != "query":
        raise RuntimeError("ClickHouse OSS query-mode verification is not enabled")
    tables = StageTables.from_configs(config, oss_config)
    metadata = MetadataStore(
        Path(args.data_dir) / "metadata.sqlite3",
        run_migrations=False,
    )
    manifest = ClickHouseManifest(
        paths["index"] / "clickhouse" / oss_config.manifest_name,
        run_migrations=False,
    )
    client = ClickHouseClient(config)
    initial_manifest_stats = manifest.stats()
    end_us = int(
        args.end_us
        or initial_manifest_stats.get("reconcile_end_epoch_us")
        or 0
    )
    if end_us <= 0:
        raise RuntimeError(
            "A completed manifest reconciliation watermark or --end-us is required"
        )
    start_us = int(
        args.start_us
        if args.start_us is not None
        else history_start_epoch_us(datetime.now(UTC), oss_config.history_days)
    )
    source_parts, eligible = _eligible_parts(
        metadata,
        start_us=start_us,
        end_us=end_us,
    )
    source_before = inventory_snapshot(eligible)
    coverage = manifest.coverage(eligible)
    stats = manifest.stats()
    status_path = paths["logs"] / _status_name(oss_config.manifest_name)

    def progress(value: dict[str, Any]) -> None:
        write_json_status(
            status_path,
            {
                "state": "running",
                "window": {"start_epoch_us": start_us, "end_epoch_us": end_us},
                "inventory": source_before,
                **value,
            },
        )

    remote = verify_remote_parts(
        client,
        eligible,
        time_table=tables.final_time,
        name_table=tables.final_name,
        batch_size=args.batch_size,
        mismatch_limit=args.mismatch_limit,
        progress=progress,
    )
    time_summary = client.table_storage_summary(tables.final_time)
    name_summary = client.table_storage_summary(tables.final_name)
    stage_time_summary = client.table_storage_summary(tables.stage_time)
    stage_name_summary = client.table_storage_summary(tables.stage_name)
    object_statuses = {
        "time": client.table_status(tables.final_time),
        "name": client.table_status(tables.final_name),
        "stage_time": client.table_status(tables.stage_time),
        "stage_name": client.table_status(tables.stage_name),
        "materialized_view": client.table_status(
            f"{config.database}.{oss_config.materialized_view}"
        ),
    }
    final_source_parts, final_eligible = _eligible_parts(
        metadata,
        start_us=start_us,
        end_us=end_us,
    )
    source_after = inventory_snapshot(final_eligible)
    source_stable = bool(
        len(source_parts) == len(final_source_parts)
        and source_before == source_after
    )
    journal_path = manifest.path.with_suffix(".stage-journal.json")
    gate = evaluate_full_gate(
        source_inventory=source_before,
        source_parts=len(source_parts),
        manifest_coverage=coverage,
        manifest_stats=stats,
        remote=remote,
        time_summary=time_summary,
        name_summary=name_summary,
        stage_time_summary=stage_time_summary,
        stage_name_summary=stage_name_summary,
        object_statuses=object_statuses,
        incremental_mv_enabled=oss_config.incremental_mv_enabled,
        journal_exists=journal_path.exists(),
        source_stable=source_stable,
    )
    result = {
        "ok": bool(gate["ok"]),
        "window": {"start_epoch_us": start_us, "end_epoch_us": end_us},
        "watermark_source": (
            "explicit" if int(args.end_us or 0) > 0 else "manifest-reconcile"
        ),
        "source_parts": len(source_parts),
        "inventory_before": source_before,
        "inventory_after": source_after,
        "manifest_coverage": {
            **coverage,
            "missing_parts": list(coverage.get("missing_parts") or [])[:20],
            "missing_parts_total": len(coverage.get("missing_parts") or []),
        },
        "manifest_stats": stats,
        "remote": remote,
        "tables": {
            "time": time_summary,
            "name": name_summary,
            "stage_time": stage_time_summary,
            "stage_name": stage_name_summary,
        },
        "objects": object_statuses,
        "journal": str(journal_path),
        "journal_exists": journal_path.exists(),
        "gate": gate,
    }
    write_json_status(status_path, {"state": "complete", **result})
    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Verify full OSS ClickHouse coverage in both query tables."
    )
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--start-us", type=int, default=None)
    parser.add_argument("--end-us", type=int, default=0)
    parser.add_argument(
        "--batch-size", type=int, default=MAX_PART_STATE_IDENTITIES
    )
    parser.add_argument("--mismatch-limit", type=int, default=20)
    parser.add_argument(
        "--ready-pilot",
        type=int,
        default=0,
        help=(
            "Verify up to N manifest-ready parts without claiming full "
            "cutover eligibility."
        ),
    )
    args = parser.parse_args()
    args.batch_size = validate_part_state_batch_size(args.batch_size)
    args.mismatch_limit = min(max(int(args.mismatch_limit), 1), 1000)
    args.ready_pilot = min(max(int(args.ready_pilot), 0), 1000)
    try:
        result = _run_ready_pilot(args) if args.ready_pilot else _run(args)
    except Exception as exc:
        result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0 if result.get("ok") else 2


if __name__ == "__main__":
    raise SystemExit(main())
