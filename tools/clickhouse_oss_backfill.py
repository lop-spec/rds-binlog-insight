from __future__ import annotations

import argparse
import logging
import threading
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Iterable

from app.clickhouse_client import (
    ClickHouseClient,
    ClickHouseConfig,
    validate_part_state_batch_size,
)
from app.clickhouse_ingest import (
    IngestPaused,
    _probe_capacity,
    _probe_health,
    _probe_io_pressure,
    _worker_lock,
)
from app.clickhouse_manifest import ClickHouseManifest, part_identity
from app.clickhouse_oss import (
    ClickHouseOssConfig,
    build_direct_s3_insert_sql,
    history_start_epoch_us,
    split_direct_and_ranged_parts,
)
from app.clickhouse_stage import StagedBatchLoader
from app.config import ensure_data_dirs
from app.maintenance_status import write_json_status
from app.metadata import MetadataStore


LOGGER = logging.getLogger(__name__)
STATUS_NAME = "clickhouse-oss-backfill-status.json"
_MANIFEST_WRITE_LOCK = threading.Lock()


def _status_name(manifest_name: str) -> str:
    if str(manifest_name) == "oss-all-manifest.sqlite3":
        return STATUS_NAME
    stem = Path(str(manifest_name)).stem.removesuffix("-manifest")
    return f"clickhouse-{stem}-backfill-status.json"


def _wait_for_admission(
    config: ClickHouseConfig,
    capacity_path: Path,
    *,
    on_pause: Callable[[str], None],
) -> None:
    """Wait out transient production safety fuses instead of exiting."""

    while True:
        try:
            _probe_io_pressure(config)
            _probe_health(config)
            _probe_capacity(capacity_path, config)
            return
        except IngestPaused as exc:
            on_pause(str(exc))
            time.sleep(max(float(config.idle_seconds), 5.0))


def _remote_matches(part: dict[str, Any], state: dict[str, Any]) -> bool:
    expected_rows = int(part.get("row_count") or 0)
    rows = int(state.get("rows") or 0)
    name_rows = int(state.get("name_rows") or 0)
    if expected_rows == 0:
        return rows == 0 and name_rows == 0
    revision = int(part.get("content_revision") or 0)
    return bool(
        rows == expected_rows
        and name_rows == expected_rows
        and int(state.get("sha_count") or 0) == 1
        and str(state.get("sha256") or "") == str(part.get("sha256") or "")
        and int(state.get("min_revision") or 0) == revision
        and int(state.get("max_revision") or 0) == revision
        and int(state.get("name_sha_count") or 0) == 1
        and str(state.get("name_sha256") or "")
        == str(part.get("sha256") or "")
        and int(state.get("name_min_revision") or 0) == revision
        and int(state.get("name_max_revision") or 0) == revision
    )


def _batches(
    parts: list[dict[str, Any]],
    *,
    max_parts: int,
    max_bytes: int,
    max_rows: int,
) -> Iterable[list[dict[str, Any]]]:
    batch: list[dict[str, Any]] = []
    size = 0
    rows = 0
    for part in parts:
        part_size = max(int(part.get("size_bytes") or 0), 1)
        part_rows = max(int(part.get("row_count") or 0), 0)
        if batch and (
            len(batch) >= max_parts
            or size + part_size > max_bytes
            or rows + part_rows > max_rows
        ):
            yield batch
            batch = []
            size = 0
            rows = 0
        batch.append(part)
        size += part_size
        rows += part_rows
    if batch:
        yield batch


def _cleanup_partial_batch(
    client: ClickHouseClient,
    parts: list[dict[str, Any]],
) -> list[str]:
    """Delete incomplete rows left by one failed INSERT in one mutation batch."""

    identities = [part_identity(part) for part in parts]
    states = client.paired_part_states(identities)
    stale_identities: list[str] = []
    for part in parts:
        identity = part_identity(part)
        state = states.get(identity, {})
        if _remote_matches(part, state):
            continue
        if int(state.get("rows") or 0) or int(state.get("name_rows") or 0):
            stale_identities.append(identity)
    if stale_identities:
        client.delete_parts(stale_identities)
    return stale_identities


def _load_batch(
    client: ClickHouseClient,
    manifest: ClickHouseManifest,
    settings: Any,
    config: ClickHouseConfig,
    parts: list[dict[str, Any]],
) -> tuple[int, int]:
    """Load a direct batch, bisecting failures to isolate a bad object."""

    identities = [part_identity(part) for part in parts]
    states = client.paired_part_states(identities)
    missing: list[dict[str, Any]] = []
    zero_ready: list[dict[str, Any]] = []
    stale_identities: list[str] = []
    ready = 0
    for part in parts:
        identity = part_identity(part)
        state = states.get(identity, {})
        if _remote_matches(part, state):
            with _MANIFEST_WRITE_LOCK:
                manifest.mark_ready(
                    str(part["path"]), identity, int(part["row_count"])
                )
            ready += 1
            continue
        if int(state.get("rows") or 0) or int(state.get("name_rows") or 0):
            stale_identities.append(identity)
        if int(part.get("row_count") or 0) == 0:
            zero_ready.append(part)
        else:
            missing.append(part)
    if stale_identities:
        client.delete_parts(stale_identities)
    for part in zero_ready:
        identity = part_identity(part)
        with _MANIFEST_WRITE_LOCK:
            marked_ready = manifest.mark_ready(str(part["path"]), identity, 0)
        if not marked_ready:
            raise RuntimeError(
                f"OSS manifest identity changed before zero-row commit: {identity}"
            )
        ready += 1
    if not missing:
        return ready, 0
    try:
        oss_config = ClickHouseOssConfig.from_env()
        sql = build_direct_s3_insert_sql(
            settings,
            database=config.database,
            table=config.table,
            parts=missing,
        )
        query_memory = max(
            750_000_000,
            3_000_000_000 // max(int(oss_config.backfill_workers), 1),
        )
        client.query(
            sql,
            settings={
                "max_threads": oss_config.backfill_threads,
                "max_insert_threads": oss_config.backfill_insert_threads,
                "max_execution_time": 1800,
                "max_memory_usage": query_memory,
                "wait_end_of_query": 1,
            },
            timeout=1800,
        )
        verified = client.paired_part_states(
            [part_identity(part) for part in missing]
        )
        for part in missing:
            identity = part_identity(part)
            state = verified.get(identity, {})
            if not _remote_matches(part, state):
                raise RuntimeError(
                    "OSS batch verification failed: "
                    f"part={identity} rows={state.get('rows')}/"
                    f"{part.get('row_count')}"
                )
            with _MANIFEST_WRITE_LOCK:
                marked_ready = manifest.mark_ready(
                    str(part["path"]), identity, int(state["rows"])
                )
            if not marked_ready:
                raise RuntimeError(
                    f"OSS manifest identity changed before commit: {identity}"
                )
        return ready + len(missing), 0
    except Exception as exc:
        if len(missing) > 1:
            stale_identities = _cleanup_partial_batch(client, missing)
            LOGGER.warning(
                "ClickHouse OSS batch failed; bisecting parts=%s "
                "stale=%s error=%s",
                len(missing),
                len(stale_identities),
                exc,
            )
            midpoint = len(missing) // 2
            left_ready, left_failed = _load_batch(
                client, manifest, settings, config, missing[:midpoint]
            )
            right_ready, right_failed = _load_batch(
                client, manifest, settings, config, missing[midpoint:]
            )
            return ready + left_ready + right_ready, left_failed + right_failed
        part = missing[0]
        identity = part_identity(part)
        with _MANIFEST_WRITE_LOCK:
            manifest.mark_failed(str(part["path"]), identity, str(exc))
        LOGGER.exception("ClickHouse OSS direct object failed: %s", identity)
        return ready, 1


def run(
    data_dir: Path,
    *,
    batch_parts: int,
    batch_bytes: int,
    batch_rows: int,
    max_parts: int,
) -> int:
    batch_parts = validate_part_state_batch_size(batch_parts)
    paths = ensure_data_dirs(data_dir)
    config = ClickHouseConfig.from_env()
    oss_config = ClickHouseOssConfig.from_env()
    status_path = paths["logs"] / _status_name(oss_config.manifest_name)
    if not oss_config.enabled or config.ingest_mode != "query":
        raise RuntimeError("ClickHouse OSS query-mode ingestion is not enabled")
    if config.table != oss_config.query_table:
        raise RuntimeError("ClickHouse OSS ingest target does not match query table")
    metadata = MetadataStore(data_dir / "metadata.sqlite3", run_migrations=False)
    settings = metadata.load_settings()
    manifest = ClickHouseManifest(
        paths["index"] / "clickhouse" / oss_config.manifest_name,
        run_migrations=False,
    )
    client = ClickHouseClient(config)
    staged_loader: StagedBatchLoader | None = None
    if oss_config.staged_backfill_enabled:
        staged_loader = StagedBatchLoader(
            client=client,
            manifest=manifest,
            settings=settings,
            config=config,
            oss_config=oss_config,
            journal_path=manifest.path.with_suffix(".stage-journal.json"),
        )
        staged_loader.recover()
    now = datetime.now(UTC)
    start_us = history_start_epoch_us(now, oss_config.history_days)
    end_us = int(now.timestamp() * 1_000_000)
    source_parts = metadata.parts_in_range(
        start_epoch_us=start_us,
        end_epoch_us=end_us,
        source="database",
    )
    eligible = [
        part
        for part in source_parts
        if str(part.get("oss_key") or "") and part_identity(part)
    ]
    reconcile = manifest.reconcile(
        eligible,
        start_epoch_us=start_us,
        end_epoch_us=end_us,
        source_parts=len(source_parts),
    )
    if len(source_parts) == len(eligible):
        # The explicit metadata migration installs durable mutation triggers
        # before this full snapshot. Concurrent changes remain queued, while
        # this flag records that pre-existing history has been reconciled.
        metadata.mark_clickhouse_change_tracking_complete()
    manifest.recover_loading()
    direct, ranged = split_direct_and_ranged_parts(eligible)
    with manifest.connection() as connection:
        ready_rows = connection.execute(
            "SELECT part_path, logical_part_id FROM clickhouse_parts "
            "WHERE status = 'ready'"
        ).fetchall()
    ready = {
        (str(row["part_path"]), str(row["logical_part_id"]))
        for row in ready_rows
    }
    pending = [
        part
        for part in direct
        if (str(part["path"]), part_identity(part)) not in ready
    ]
    pending.sort(
        key=lambda part: (
            int(part.get("max_event_epoch_us") or 0),
            str(part.get("path") or ""),
        ),
        reverse=True,
    )
    if max_parts:
        pending = pending[:max_parts]
    planned = len(pending)
    planned_source_bytes = sum(
        max(int(part.get("size_bytes") or 0), 0) for part in pending
    )
    completed = 0
    failures = 0
    started = time.monotonic()
    initial_stats = manifest.stats()
    initial_ready_source_bytes = int(
        initial_stats.get("ready_source_bytes") or 0
    )
    write_json_status(
        status_path,
        {
            "state": "starting",
            "reconcile": reconcile,
            "directParts": len(direct),
            "rangedParts": len(ranged),
            "plannedParts": planned,
            "plannedSourceBytes": planned_source_bytes,
            "workers": oss_config.backfill_workers,
            "stagedBackfill": bool(staged_loader),
            "batchParts": batch_parts,
            "batchBytes": max(int(batch_bytes), 1),
            "batchRows": max(int(batch_rows), 1),
        },
    )

    batches = list(
        _batches(
            pending,
            max_parts=batch_parts,
            max_bytes=max(int(batch_bytes), 1),
            max_rows=max(int(batch_rows), 1),
        )
    )

    def load_one(batch: list[dict[str, Any]]) -> tuple[int, int]:
        _wait_for_admission(
            config,
            paths["root"],
            on_pause=lambda error: write_json_status(
                status_path,
                {
                    "state": "paused",
                    "phase": "safety-fuse",
                    "lastError": error,
                    "completedParts": completed,
                    "failedParts": failures,
                    "plannedParts": planned,
                    "stagedBackfill": bool(staged_loader),
                    "stats": manifest.stats(),
                },
            ),
        )
        if staged_loader is not None:
            return staged_loader.load(batch)
        return _load_batch(client, manifest, settings, config, batch)

    def record_batch(batch_ready: int, batch_failed: int) -> None:
        nonlocal completed, failures
        completed += batch_ready
        failures += batch_failed
        elapsed = max(time.monotonic() - started, 0.001)
        stats = manifest.stats()
        completed_source_bytes = max(
            int(stats.get("ready_source_bytes") or 0)
            - initial_ready_source_bytes,
            0,
        )
        source_bytes_per_second = completed_source_bytes / elapsed
        remaining_source_bytes = max(
            planned_source_bytes - completed_source_bytes, 0
        )
        parts_per_second = completed / elapsed
        remaining_direct_parts = max(
            planned - completed - failures,
            0,
        )
        eta_by_parts = (
            remaining_direct_parts / parts_per_second
            if parts_per_second > 0
            else None
        )
        eta_by_bytes = (
            remaining_source_bytes / source_bytes_per_second
            if source_bytes_per_second > 0
            else None
        )
        write_json_status(
            status_path,
            {
                "state": "running",
                "completedParts": completed,
                "failedParts": failures,
                "plannedParts": planned,
                "remainingDirectParts": remaining_direct_parts,
                "rangedPartsForVerifiedStream": len(ranged),
                "partsPerSecond": round(parts_per_second, 3),
                "plannedSourceBytes": planned_source_bytes,
                "completedSourceBytes": completed_source_bytes,
                "sourceBytesPerSecond": round(source_bytes_per_second, 3),
                # Early history consists of many tiny recent parts. A byte-rate
                # projection can therefore overstate the ETA by weeks before
                # representative large files are reached. Keep both estimates
                # explicit and use observed part throughput as the UI headline.
                "estimatedRemainingSeconds": round(eta_by_parts, 3)
                if eta_by_parts is not None
                else None,
                "estimatedRemainingSecondsByParts": round(eta_by_parts, 3)
                if eta_by_parts is not None
                else None,
                "estimatedRemainingSecondsByBytes": round(eta_by_bytes, 3)
                if eta_by_bytes is not None
                else None,
                "etaBasis": "observed-parts",
                "workers": oss_config.backfill_workers,
                "stagedBackfill": bool(staged_loader),
                "stats": stats,
            },
        )
        _probe_health(config)
        time.sleep(max(float(config.idle_seconds), 0.1))

    if oss_config.backfill_workers == 1:
        for batch in batches:
            batch_ready, batch_failed = load_one(batch)
            record_batch(batch_ready, batch_failed)
    else:
        batch_iterator = iter(batches)
        with ThreadPoolExecutor(
            max_workers=oss_config.backfill_workers,
            thread_name_prefix="oss-backfill",
        ) as executor:
            in_flight: dict[Future[tuple[int, int]], list[dict[str, Any]]] = {}

            def submit_next() -> bool:
                try:
                    batch = next(batch_iterator)
                except StopIteration:
                    return False
                in_flight[executor.submit(load_one, batch)] = batch
                return True

            for _ in range(oss_config.backfill_workers):
                if not submit_next():
                    break
            while in_flight:
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for future in done:
                    in_flight.pop(future)
                    batch_ready, batch_failed = future.result()
                    record_batch(batch_ready, batch_failed)
                    submit_next()
    final_stats = manifest.stats()
    elapsed_seconds = max(time.monotonic() - started, 0.001)
    completed_source_bytes = max(
        int(final_stats.get("ready_source_bytes") or 0)
        - initial_ready_source_bytes,
        0,
    )
    final = {
        "state": "complete" if not failures else "complete-with-errors",
        "completedParts": completed,
        "failedParts": failures,
        "plannedParts": planned,
        "rangedPartsForVerifiedStream": len(ranged),
        "elapsedSeconds": round(elapsed_seconds, 3),
        "plannedSourceBytes": planned_source_bytes,
        "completedSourceBytes": completed_source_bytes,
        "sourceBytesPerSecond": round(
            completed_source_bytes / elapsed_seconds, 3
        ),
        "workers": oss_config.backfill_workers,
        "stagedBackfill": bool(staged_loader),
        "stats": final_stats,
    }
    write_json_status(status_path, final)
    print(final)
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill standalone OSS Parquet objects into OSS MergeTree."
    )
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--batch-parts", type=int, default=64)
    parser.add_argument("--batch-bytes", type=int, default=512 * 1024 * 1024)
    parser.add_argument("--batch-rows", type=int, default=1_000_000)
    parser.add_argument("--max-parts", type=int, default=0)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    paths = ensure_data_dirs(args.data_dir)
    lock_path = paths["index"] / "clickhouse" / "oss-ingester.lock"
    with _worker_lock(lock_path):
        return run(
            args.data_dir,
            batch_parts=args.batch_parts,
            batch_bytes=args.batch_bytes,
            batch_rows=args.batch_rows,
            max_parts=max(int(args.max_parts), 0),
        )


if __name__ == "__main__":
    raise SystemExit(main())
