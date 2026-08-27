from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import signal
import shutil
import sqlite3
import tempfile
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, BinaryIO, Callable, Iterator
from urllib.request import Request, urlopen

try:
    import fcntl
except ImportError:  # pragma: no cover - production runs on Linux
    fcntl = None

from .clickhouse_client import ClickHouseClient, ClickHouseConfig
from .clickhouse_change_queue import reconcile_pending_source_changes
from .clickhouse_manifest import ClickHouseManifest, part_identity
from .clickhouse_oss import ClickHouseOssConfig, history_start_epoch_us
from .clickhouse_slowlog import SourceIndexLagPaused, SourceIndexPriorityGate
from .config import ensure_data_dirs
from .credentials import load_credential
from .io_pressure import (
    IoPressurePaused,
    probe_io_pressure,
    require_io_recovery,
)
from .maintenance_status import SLOWLOG_WORKER_STATUS_NAME, write_json_status
from .metadata import MetadataStore
from .oss_store import OssArchive


LOGGER = logging.getLogger(__name__)
STATUS_NAME = "clickhouse-worker-status.json"
OSS_STATUS_NAME = "clickhouse-oss-worker-status.json"
BACKLOG_RECONCILE_SECONDS = 600.0
BULK_BACKLOG_PARTS = 1000
BULK_BACKLOG_RECONCILE_SECONDS = 3600.0
OSS_FULL_RECONCILE_SECONDS = 24 * 3600
OSS_INCREMENTAL_RECONCILE_HOURS = 6
# A successful serving-path probe may be reused only within the same short
# ingest step.  Five seconds let several small parts pass after one fast sample
# while the shared filesystem was already entering sustained journal pressure.
HEALTH_CANARY_CACHE_SECONDS = 1.0
# Normal production parts are below this ceiling, so OSS verification stays in
# memory and avoids the old download -> local SHA scan -> local upload chain.
# Oversized future parts automatically roll to TMPDIR instead of risking OOM.
PARQUET_STREAM_MEMORY_BYTES = 128 * 1024 * 1024


# Capacity, serving-health and PSI backpressure all have the same durable
# queue semantics: release the claim without recording a failed source part.
IngestPaused = IoPressurePaused


class IngestPartError(RuntimeError):
    def __init__(self, part_path: str, logical_part_id: str, cause: Exception):
        super().__init__(str(cause))
        self.part_path = part_path
        self.logical_part_id = logical_part_id


class MergeGovernor:
    """Ensure bounded background merges are enabled once per worker process.

    SYSTEM STOP MERGES cancels in-flight merges. Calling START/STOP around
    transient PSI boundaries creates read amplification.  The container I/O
    cap and the table's 128 MiB merge ceiling bound merge work; the safety fuse
    pauses only new ingestion and lets the current merge finish.
    """

    def __init__(self, client: ClickHouseClient):
        self.client = client
        self._started = False

    def resume(self) -> None:
        if self._started:
            return
        self.client.start_merges()
        self._started = True


def _reconcile_interval(
    config: ClickHouseConfig,
    stats: dict[str, Any],
) -> float:
    """Avoid repeatedly rescanning metadata while a known backlog remains."""

    base = float(config.reconcile_seconds)
    pending = int(stats.get("pending_parts") or 0)
    if pending > BULK_BACKLOG_PARTS:
        return max(base, BULK_BACKLOG_RECONCILE_SECONDS)
    if pending > 0:
        return max(base, BACKLOG_RECONCILE_SECONDS)
    return base


def _initial_reconcile_monotonic(
    config: ClickHouseConfig,
    stats: dict[str, Any],
    *,
    now_monotonic: float | None = None,
    now_epoch_us: int | None = None,
) -> float:
    """Restore the durable reconcile age instead of resetting it on restart."""

    pending = int(stats.get("pending_parts") or 0)
    completed_at_us = int(stats.get("reconcile_completed_at_us") or 0)
    if pending <= 0 or completed_at_us <= 0:
        return 0.0
    monotonic_now = (
        time.monotonic() if now_monotonic is None else float(now_monotonic)
    )
    epoch_now_us = (
        time.time_ns() // 1000 if now_epoch_us is None else int(now_epoch_us)
    )
    elapsed = max((epoch_now_us - completed_at_us) / 1_000_000.0, 0.0)
    if elapsed >= _reconcile_interval(config, stats):
        return 0.0
    return max(monotonic_now - elapsed, 0.000001)


def _require_ingest_recovery(
    config: ClickHouseConfig,
    pressure: float,
    *,
    paused: bool,
) -> None:
    """Apply hysteresis without changing explicit PSI-fuse disablement."""

    require_io_recovery(
        float(config.io_pressure_max_full_avg10),
        pressure,
        paused=paused,
        recovery_ratio=float(config.io_pressure_recovery_ratio),
    )


def _metadata_part_by_path(
    metadata: MetadataStore,
    path: str,
    *,
    attempts: int = 3,
) -> dict[str, Any] | None:
    """Read one source part through short, bounded SQLite lock contention."""

    for attempt in range(max(int(attempts), 1)):
        try:
            return metadata.part_by_path(path)
        except sqlite3.OperationalError as exc:
            locked = "locked" in str(exc).lower()
            if not locked or attempt + 1 >= attempts:
                raise
            delay = 0.25 * (2**attempt)
            LOGGER.warning(
                "Metadata read was locked; retrying part %s in %.2fs (%s/%s)",
                path,
                delay,
                attempt + 1,
                attempts,
            )
            time.sleep(delay)
    raise AssertionError("metadata retry loop did not return")


@contextmanager
def _worker_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("a+", encoding="utf-8")
    try:
        if fcntl is not None:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("another ClickHouse ingester owns the lock") from exc
        yield
    finally:
        if fcntl is not None:
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError:
                pass
        handle.close()


def _probe_capacity(path: Path, config: ClickHouseConfig) -> int:
    free = int(shutil.disk_usage(path).free)
    required = int(config.min_free_gb) * 1024**3
    if free < required:
        raise IngestPaused(
            f"free disk below safety floor: {free} < {required} bytes"
        )
    return free


def _probe_health(config: ClickHouseConfig) -> float:
    if not config.health_url:
        return 0.0
    request = Request(
        config.health_url,
        headers={"Host": config.health_host_header}
        if config.health_host_header
        else {},
    )
    started = time.monotonic()
    try:
        with urlopen(
            request,
            timeout=max(config.health_max_seconds + 1.0, 2.0),
        ) as response:
            # /api/storage is the serving-path canary. Its bounded status payload
            # is currently about 270 KiB, so a 16 KiB read would truncate valid
            # JSON and falsely fail every probe. One MiB remains a strict ceiling.
            body = response.read(1024 * 1024)
            status = int(response.status)
        payload = json.loads(body.decode("utf-8"))
    except (OSError, ValueError) as exc:
        # A slow or malformed production canary is backpressure, not evidence
        # that the claimed source part is corrupt. Keep it pending so the
        # persistent queue retries without creating a transient failed item.
        raise IngestPaused(f"production health probe failed: {exc}") from exc
    elapsed = time.monotonic() - started
    if status != 200 or not bool(payload.get("ok")):
        raise IngestPaused(f"production health failed: HTTP {status}")
    if elapsed > config.health_max_seconds:
        raise IngestPaused(
            f"production health exceeded {config.health_max_seconds:.3f}s: "
            f"{elapsed:.3f}s"
        )
    return elapsed


class HealthCanary:
    """Bound repeated serving-path probes while keeping fail-closed behavior."""

    def __init__(
        self,
        config: ClickHouseConfig,
        *,
        cache_seconds: float = HEALTH_CANARY_CACHE_SECONDS,
    ) -> None:
        self.config = config
        self.cache_seconds = max(float(cache_seconds), 0.0)
        self._checked_at = float("-inf")
        self._elapsed = 0.0

    def probe(self, *, force: bool = False) -> float:
        now = time.monotonic()
        if not force and now - self._checked_at < self.cache_seconds:
            return self._elapsed
        # Failures are deliberately not cached: the worker pauses and the
        # next iteration must observe a fresh serving-path result.
        elapsed = _probe_health(self.config)
        self._elapsed = elapsed
        self._checked_at = time.monotonic()
        return elapsed


def _probe_io_pressure(
    config: ClickHouseConfig,
    *,
    path: Path = Path("/proc/pressure/io"),
) -> float:
    """Pause ingestion before it adds I/O to an already saturated host."""

    return probe_io_pressure(
        float(config.io_pressure_max_full_avg10),
        path=path,
    )


def _admit_io_pressure(
    config: ClickHouseConfig,
    health_canary: HealthCanary,
    *,
    paused: bool,
) -> tuple[bool, IoPressurePaused | None]:
    """Use the serving canary to reject false host-wide PSI positives.

    Linux charges time deliberately spent behind another container's cgroup
    I/O throttle to host PSI.  A healthy production serving probe therefore
    permits one independently rate-limited ingest iteration.  The caller keeps
    the PSI hysteresis state until the host metric genuinely recovers.
    """

    try:
        pressure = _probe_io_pressure(config)
        _require_ingest_recovery(config, pressure, paused=paused)
    except IoPressurePaused as exc:
        health_canary.probe(force=True)
        return True, exc
    return False, None


def _ingest_pacing_seconds(config: ClickHouseConfig) -> float:
    """Prevent fast small parts from replacing the removed io.max throttle."""

    return max(float(config.idle_seconds), 1.0)


def reconcile_once(
    metadata: MetadataStore,
    manifest: ClickHouseManifest,
    config: ClickHouseConfig,
    *,
    now: datetime | None = None,
    allow_high_io_pressure: bool = False,
    oss_config: ClickHouseOssConfig | None = None,
) -> dict[str, int]:
    if not allow_high_io_pressure:
        _probe_io_pressure(config)
    current = now or datetime.now(UTC)
    resolved_oss = oss_config or ClickHouseOssConfig.from_env()
    object_mode = bool(
        resolved_oss.enabled and config.ingest_mode == "query"
    )
    change_result = {
        "scanned": 0,
        "queued": 0,
        "removed": 0,
        "removed_states": 0,
        "deferred": 0,
        "acknowledged": 0,
    }
    tracking_state = {"complete": True, "pending": False}
    if object_mode:
        tracking_state = metadata.clickhouse_change_tracking_state()
        change_result = reconcile_pending_source_changes(metadata, manifest)
    full_scan = True
    if object_mode:
        stats = manifest.stats()
        completed_at_us = int(stats.get("reconcile_completed_at_us") or 0)
        current_us = int(current.timestamp() * 1_000_000)
        full_scan = bool(
            not bool(tracking_state.get("complete"))
            or not completed_at_us
            or current_us - completed_at_us
            >= OSS_FULL_RECONCILE_SECONDS * 1_000_000
        )
    if object_mode and full_scan:
        start_us = history_start_epoch_us(
            current, resolved_oss.history_days
        )
    else:
        history = (
            timedelta(hours=OSS_INCREMENTAL_RECONCILE_HOURS)
            if object_mode
            else timedelta(hours=config.hot_hours)
        )
        start_us = int((current - history).timestamp() * 1_000_000)
    end_us = int(current.timestamp() * 1_000_000)
    parts = metadata.parts_in_range(
        start_epoch_us=start_us,
        end_epoch_us=end_us,
        source="database",
    )
    eligible = [
        part
        for part in parts
        if str(part.get("oss_key") or "") and part_identity(part)
    ]
    result = manifest.reconcile(
        eligible,
        start_epoch_us=start_us,
        end_epoch_us=end_us,
        source_parts=len(parts),
        sweep_unseen=full_scan,
        preserve_reconcile_state=object_mode and not full_scan,
    )
    if object_mode and full_scan and len(parts) == len(eligible):
        metadata.mark_clickhouse_change_tracking_complete()
    result["full_scan"] = int(full_scan)
    result.update(
        {
            f"source_changes_{key}": int(value)
            for key, value in change_result.items()
        }
    )
    return result


@contextmanager
def _verified_part_stream(
    archive: OssArchive,
    part: dict[str, Any],
) -> Iterator[BinaryIO]:
    """Verify one OSS object before exposing it to ClickHouse's HTTP insert."""
    expected_size = max(int(part.get("size_bytes") or 0), 0)
    expected_sha = str(part.get("sha256") or "")
    if not expected_sha:
        raise RuntimeError("source part is missing SHA-256")
    digest = hashlib.sha256()
    total = 0
    with tempfile.SpooledTemporaryFile(
        max_size=PARQUET_STREAM_MEMORY_BYTES,
        mode="w+b",
    ) as payload:
        with archive.open_part_reader(part) as source:
            while chunk := source.read(4 * 1024 * 1024):
                total += len(chunk)
                if total > expected_size:
                    raise RuntimeError(
                        "OSS stream size exceeds source metadata: "
                        f"bytes={total}/{expected_size}"
                    )
                digest.update(chunk)
                payload.write(chunk)
        if total != expected_size:
            raise RuntimeError(
                "OSS stream size does not match source metadata: "
                f"bytes={total}/{expected_size}"
            )
        actual_sha = digest.hexdigest()
        if actual_sha != expected_sha:
            raise RuntimeError(
                "OSS stream SHA-256 does not match source metadata: "
                f"sha={actual_sha}/{expected_sha}"
            )
        payload.seek(0)
        yield payload


def _ingest_part_state(
    client: ClickHouseClient,
    identity: str,
    *,
    verify_name_table: bool,
) -> dict[str, Any]:
    if not verify_name_table:
        return client.part_state(identity)
    return client.paired_part_states([identity]).get(identity, {})


def _ingest_state_has_rows(
    state: dict[str, Any],
    *,
    verify_name_table: bool,
) -> bool:
    return bool(
        int(state.get("rows") or 0)
        or (verify_name_table and int(state.get("name_rows") or 0))
    )


def _ingest_state_matches(
    state: dict[str, Any],
    *,
    expected_rows: int,
    expected_sha: str,
    expected_revision: int,
    verify_name_table: bool,
) -> bool:
    time_matches = bool(
        int(state.get("rows") or 0) == expected_rows
        and (
            expected_rows == 0
            or (
                int(state.get("sha_count") or 0) == 1
                and str(state.get("sha256") or "") == expected_sha
                and int(state.get("min_revision") or 0) == expected_revision
                and int(state.get("max_revision") or 0) == expected_revision
            )
        )
    )
    if not verify_name_table:
        return time_matches
    return bool(
        time_matches
        and int(state.get("name_rows") or 0) == expected_rows
        and (
            expected_rows == 0
            or (
                int(state.get("name_sha_count") or 0) == 1
                and str(state.get("name_sha256") or "") == expected_sha
                and int(state.get("name_min_revision") or 0)
                == expected_revision
                and int(state.get("name_max_revision") or 0)
                == expected_revision
            )
        )
    )


def ingest_one(
    metadata: MetadataStore,
    manifest: ClickHouseManifest,
    client: ClickHouseClient,
    archive: OssArchive,
    config: ClickHouseConfig,
    scratch: Path,
    capacity_path: Path,
    health_probe: Callable[[], float] | None = None,
    prefer_newest: bool = False,
    allow_high_io_pressure: bool = False,
    verify_name_table: bool = False,
) -> dict[str, Any] | None:
    probe_health = health_probe or (lambda: _probe_health(config))
    if not allow_high_io_pressure:
        _probe_io_pressure(config)
    claimed = manifest.claim_next(prefer_newest=prefer_newest)
    if claimed is None:
        return None
    path = str(claimed["part_path"])
    identity = str(claimed["logical_part_id"])
    try:
        health_seconds = probe_health()
        free_bytes = _probe_capacity(capacity_path, config)
        if str(claimed.get("job_kind") or "") == "delete":
            client.delete_part(identity)
            if int(client.part_state(identity).get("rows") or 0):
                raise IngestPaused(
                    "ClickHouse delete mutation is still applying"
                )
            if not manifest.mark_retired(path, identity):
                raise RuntimeError("manifest delete claim changed before commit")
            return {
                "state": "retired",
                "part_path": path,
                "free_bytes": free_bytes,
                "health_seconds": probe_health(),
            }

        current = _metadata_part_by_path(metadata, path)
        if current is None or part_identity(current) != identity:
            # A source deletion/change can race the periodic reconciliation.
            # Delete the claimed identity now so it cannot remain query-visible.
            client.delete_part(identity)
            if _ingest_state_has_rows(
                _ingest_part_state(
                    client,
                    identity,
                    verify_name_table=verify_name_table,
                ),
                verify_name_table=verify_name_table,
            ):
                raise IngestPaused(
                    "ClickHouse stale delete mutation is still applying"
                )
            manifest.mark_retired(path, identity)
            return {"state": "source-changed", "part_path": path}
        if not str(current.get("oss_key") or ""):
            raise RuntimeError("source part is not archived in OSS")

        expected_rows = int(current.get("row_count") or 0)
        expected_sha = str(current.get("sha256") or "")
        expected_revision = int(current.get("content_revision") or 0)
        remote = _ingest_part_state(
            client,
            identity,
            verify_name_table=verify_name_table,
        )
        remote_rows = int(remote.get("rows") or 0)
        remote_matches = _ingest_state_matches(
            remote,
            expected_rows=expected_rows,
            expected_sha=expected_sha,
            expected_revision=expected_revision,
            verify_name_table=verify_name_table,
        )
        if remote_matches:
            if not manifest.mark_ready(path, identity, remote_rows):
                raise RuntimeError("manifest load claim changed before commit")
            return {
                "state": "already-ready",
                "part_path": path,
                "rows": remote_rows,
                "free_bytes": free_bytes,
                "health_seconds": health_seconds,
            }
        if _ingest_state_has_rows(
            remote,
            verify_name_table=verify_name_table,
        ):
            client.delete_part(identity)
            if _ingest_state_has_rows(
                _ingest_part_state(
                    client,
                    identity,
                    verify_name_table=verify_name_table,
                ),
                verify_name_table=verify_name_table,
            ):
                raise IngestPaused(
                    "ClickHouse source delete mutation is still applying"
                )

        with _verified_part_stream(archive, current) as payload:
            latest = _metadata_part_by_path(metadata, path)
            if latest is None or part_identity(latest) != identity:
                client.delete_part(identity)
                manifest.mark_retired(path, identity)
                return {"state": "source-changed", "part_path": path}
            probe_health()
            _probe_capacity(capacity_path, config)
            if not allow_high_io_pressure:
                _probe_io_pressure(config)
            client.insert_parquet_stream(
                payload,
                content_length=int(current.get("size_bytes") or 0),
                part_key=identity,
                sha256=expected_sha,
                content_revision=expected_revision,
            )

        verified = _ingest_part_state(
            client,
            identity,
            verify_name_table=verify_name_table,
        )
        inserted_rows = int(verified.get("rows") or 0)
        verified_matches = _ingest_state_matches(
            verified,
            expected_rows=expected_rows,
            expected_sha=expected_sha,
            expected_revision=expected_revision,
            verify_name_table=verify_name_table,
        )
        if not verified_matches:
            raise RuntimeError(
                "ClickHouse row/hash/revision verification failed: "
                f"rows={inserted_rows}/{expected_rows}, "
                f"sha={verified.get('sha256')}/{expected_sha}, "
                f"revision={verified.get('min_revision')}:"
                f"{verified.get('max_revision')}/{expected_revision}, "
                f"name_rows={verified.get('name_rows')}/{expected_rows}, "
                f"name_sha={verified.get('name_sha256')}/{expected_sha}, "
                f"name_revision={verified.get('name_min_revision')}:"
                f"{verified.get('name_max_revision')}/{expected_revision}"
            )
        final_health = probe_health()
        if not manifest.mark_ready(path, identity, inserted_rows):
            raise RuntimeError("source identity changed before manifest commit")
        return {
            "state": "ready",
            "part_path": path,
            "rows": inserted_rows,
            "bytes": int(current.get("size_bytes") or 0),
            "free_bytes": _probe_capacity(capacity_path, config),
            "health_seconds": final_health,
        }
    except IngestPaused as exc:
        manifest.release_pending(
            path,
            identity,
            reason=str(exc),
            delay_seconds=30,
        )
        raise
    except Exception as exc:
        manifest.mark_failed(path, identity, str(exc))
        raise IngestPartError(path, identity, exc) from exc


def _run_worker_locked(
    data_dir: Path,
    config: ClickHouseConfig,
    paths: dict[str, Path],
    *,
    once: bool = False,
    max_parts: int = 0,
) -> int:
    oss_config = ClickHouseOssConfig.from_env()
    object_mode = bool(
        oss_config.enabled and config.ingest_mode == "query"
    )
    if object_mode:
        if oss_config.staged_backfill_enabled:
            raise RuntimeError(
                "The incremental OSS ingester cannot run in staged backfill mode"
            )
        if not oss_config.incremental_mv_enabled:
            raise RuntimeError(
                "The incremental OSS ingester requires the name materialized view"
            )
        if (
            config.table != oss_config.query_table
            or config.query_table != oss_config.query_table
            or config.name_query_table != oss_config.name_query_table
        ):
            raise RuntimeError(
                "The incremental OSS ingester target does not match OSS tables"
            )
    status_path = paths["logs"] / (
        OSS_STATUS_NAME if object_mode else STATUS_NAME
    )
    metadata = MetadataStore(data_dir / "metadata.sqlite3", run_migrations=False)
    manifest = ClickHouseManifest(
        paths["index"]
        / "clickhouse"
        / (oss_config.manifest_name if object_mode else "manifest.sqlite3"),
        run_migrations=False,
    )
    client = ClickHouseClient(config)
    settings = metadata.load_settings()
    archive = OssArchive(
        settings,
        credential=load_credential(settings.credential_target),
    )
    recovered = manifest.recover_loading()
    version = client.ping()
    merge_governor = MergeGovernor(client)
    merge_governor.resume()
    health_canary = HealthCanary(config)
    source_gate = SourceIndexPriorityGate(
        paths["index"] / SLOWLOG_WORKER_STATUS_NAME,
        block_paused_state=False,
    )
    stopping = False
    io_pressure_paused = False
    io_pressure_override_active = False

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    initial_stats = manifest.stats()
    # Preserve only the remaining durable delay across a restart.  Treating
    # every restart as a fresh reconcile would hide newly published hot parts
    # forever when a backlogged worker is recreated more often than hourly.
    last_reconcile = _initial_reconcile_monotonic(
        config,
        initial_stats,
    )
    completed = 0
    write_json_status(
        status_path,
        {
            "state": "starting",
            "clickhouseVersion": version,
            "recoveredParts": recovered,
            "stats": initial_stats,
        },
    )
    while not stopping:
        try:
            source_gate.check()
        except SourceIndexLagPaused as exc:
            write_json_status(
                status_path,
                {
                    "state": "paused",
                    "phase": "source-priority",
                    "lastError": str(exc),
                    "stats": manifest.stats(),
                },
            )
            if once:
                return 2
            time.sleep(max(config.idle_seconds, 5.0))
            continue
        try:
            io_pressure_canary_override, pressure_exc = _admit_io_pressure(
                config,
                health_canary,
                paused=io_pressure_paused,
            )
            if io_pressure_canary_override:
                io_pressure_paused = True
                if not io_pressure_override_active:
                    LOGGER.warning(
                        "Host I/O PSI is above the generic ClickHouse "
                        "ceiling but the serving canary is healthy; "
                        "allowing one bounded iteration: %s",
                        pressure_exc,
                    )
                io_pressure_override_active = True
            else:
                io_pressure_paused = False
                if io_pressure_override_active:
                    LOGGER.info(
                        "Host I/O PSI recovered below the generic "
                        "ClickHouse ceiling"
                    )
                io_pressure_override_active = False
        except IoPressurePaused as exc:
            io_pressure_paused = True
            write_json_status(
                status_path,
                {
                    "state": "paused",
                    "phase": "safety-fuse",
                    "lastError": str(exc),
                    "stats": manifest.stats(),
                },
            )
            if once:
                return 2
            time.sleep(max(config.idle_seconds, 5.0))
            continue
        now = time.monotonic()
        current_stats = manifest.stats()
        reconcile_interval = _reconcile_interval(config, current_stats)
        if not last_reconcile or now - last_reconcile >= reconcile_interval:
            try:
                reconcile = reconcile_once(
                    metadata,
                    manifest,
                    config,
                    allow_high_io_pressure=io_pressure_canary_override,
                    oss_config=oss_config,
                )
                last_reconcile = now
            except IngestPaused as exc:
                if isinstance(exc, IoPressurePaused):
                    io_pressure_paused = True
                write_json_status(
                    status_path,
                    {
                        "state": "paused",
                        "phase": "safety-fuse",
                        "lastError": str(exc),
                        "stats": manifest.stats(),
                    },
                )
                if once:
                    return 2
                time.sleep(max(config.idle_seconds, 5.0))
                continue
            except Exception as exc:
                manifest.record_reconcile_error(str(exc))
                write_json_status(
                    status_path,
                    {
                        "state": "error",
                        "phase": "reconcile",
                        "lastError": str(exc),
                        "stats": manifest.stats(),
                    },
                )
                LOGGER.exception("ClickHouse reconciliation failed")
                if once:
                    return 1
                time.sleep(config.idle_seconds)
                continue
        else:
            reconcile = {}
        try:
            if not io_pressure_canary_override:
                _probe_io_pressure(config)
            health_canary.probe(force=io_pressure_canary_override)
            prefer_newest = (
                int(current_stats.get("pending_parts") or 0)
                > BULK_BACKLOG_PARTS
            )
            health_probe = (
                (lambda: health_canary.probe(force=True))
                if io_pressure_canary_override
                else health_canary.probe
            )
            result = ingest_one(
                metadata,
                manifest,
                client,
                archive,
                config,
                paths["scratch"] / "clickhouse-ingest",
                paths["root"],
                health_probe=health_probe,
                prefer_newest=prefer_newest,
                allow_high_io_pressure=io_pressure_canary_override,
                verify_name_table=object_mode,
            )
        except IngestPaused as exc:
            if isinstance(exc, IoPressurePaused):
                io_pressure_paused = True
            write_json_status(
                status_path,
                {
                    "state": "paused",
                    "phase": "safety-fuse",
                    "lastError": str(exc),
                    "stats": manifest.stats(),
                },
            )
            if once:
                return 2
            time.sleep(max(config.idle_seconds, 5.0))
            continue
        except IngestPartError as exc:
            LOGGER.exception("ClickHouse ingestion failed")
            write_json_status(
                status_path,
                {
                    "state": "error",
                    "phase": "ingest",
                    "partPath": exc.part_path,
                    "lastError": str(exc),
                    "stats": manifest.stats(),
                },
            )
            if once:
                return 1
            time.sleep(config.idle_seconds)
            continue

        if result is None:
            write_json_status(
                status_path,
                {
                    "state": "idle",
                    "reconcile": reconcile,
                    "completedThisRun": completed,
                    "ioPressureCanaryOverride": io_pressure_canary_override,
                    "stats": manifest.stats(),
                },
            )
            if once:
                return 0
            time.sleep(config.idle_seconds)
            continue
        completed += 1
        write_json_status(
            status_path,
            {
                "state": "running",
                "phase": "ingest",
                "lastResult": result,
                "queueOrder": (
                    "newest-first" if prefer_newest else "oldest-first"
                ),
                "completedThisRun": completed,
                "ioPressureCanaryOverride": io_pressure_canary_override,
                "stats": manifest.stats(),
            },
        )
        if max_parts and completed >= max_parts:
            write_json_status(
                status_path,
                {
                    "state": "limit-reached",
                    "completedThisRun": completed,
                    "stats": manifest.stats(),
                },
            )
            return 0
        time.sleep(_ingest_pacing_seconds(config))
    write_json_status(
        status_path,
        {
            "state": "stopped",
            "completedThisRun": completed,
            "stats": manifest.stats(),
        },
    )
    return 0


def run_worker(
    data_dir: Path,
    *,
    once: bool = False,
    max_parts: int = 0,
) -> int:
    config = ClickHouseConfig.from_env()
    if not config.enabled:
        LOGGER.info("ClickHouse ingestion is disabled")
        return 0
    paths = ensure_data_dirs(data_dir)
    oss_config = ClickHouseOssConfig.from_env()
    object_mode = bool(
        oss_config.enabled and config.ingest_mode == "query"
    )
    lock_path = paths["index"] / "clickhouse" / (
        "oss-ingester.lock" if object_mode else "ingester.lock"
    )
    with _worker_lock(lock_path):
        return _run_worker_locked(
            data_dir,
            config,
            paths,
            once=once,
            max_parts=max(int(max_parts), 0),
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--max-parts", type=int, default=0)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return run_worker(
        args.data_dir,
        once=args.once,
        max_parts=args.max_parts,
    )


if __name__ == "__main__":
    raise SystemExit(main())
