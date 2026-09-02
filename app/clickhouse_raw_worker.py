from __future__ import annotations

import argparse
import logging
import os
import signal
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

from .clickhouse_client import ClickHouseClient, ClickHouseConfig
from .clickhouse_ingest import (
    HealthCanary,
    IngestPartError,
    IngestPaused,
    _admit_io_pressure,
    _worker_lock,
    ingest_one,
)
from .clickhouse_manifest import ClickHouseManifest
from .clickhouse_raw_oss import ClickHouseRawOssConfig
from .clickhouse_raw_sync import apply_pending_raw_oss_changes
from .config import ensure_data_dirs
from .credentials import load_credential
from .maintenance_status import write_json_status
from .metadata import MetadataStore
from .oss_store import OssArchive


LOGGER = logging.getLogger(__name__)
STATUS_NAME = "clickhouse-raw-oss-worker-status.json"


def run_worker(data_dir: Path, *, once: bool = False) -> int:
    base_config = ClickHouseConfig.from_env()
    raw_config = ClickHouseRawOssConfig.from_env()
    if not base_config.enabled or not raw_config.enabled:
        LOGGER.info("ClickHouse raw OSS synchronization is disabled")
        return 0
    packed_config = replace(
        base_config,
        table=raw_config.packed_table,
        query_table=raw_config.packed_table,
        name_query_table=raw_config.packed_table,
        ingest_mode="query",
    )
    paths = ensure_data_dirs(Path(data_dir))
    idle_seconds = max(
        float(os.environ.get("RDS_BINLOG_CLICKHOUSE_RAW_OSS_IDLE_SECONDS", "2")),
        0.2,
    )
    lock_path = paths["index"] / "clickhouse" / "raw-oss-worker.lock"
    with _worker_lock(lock_path):
        metadata = MetadataStore(
            Path(data_dir) / "metadata.sqlite3",
            run_migrations=False,
        )
        pack_manifest = ClickHouseManifest(
            paths["index"]
            / "clickhouse"
            / "raw-oss-packed-manifest.sqlite3",
            run_migrations=False,
        )
        client = ClickHouseClient(packed_config)
        settings = metadata.load_settings()
        archive = OssArchive(
            settings,
            credential=load_credential(settings.credential_target),
        )
        scratch = paths["scratch"] / "clickhouse-raw-packed"
        scratch.mkdir(parents=True, exist_ok=True)
        status_path = paths["logs"] / STATUS_NAME
        canary = HealthCanary(packed_config)
        recovered = pack_manifest.recover_loading()
        stopping = False
        io_pressure_paused = False
        io_pressure_override_active = False

        def stop(_signum: int, _frame: Any) -> None:
            nonlocal stopping
            stopping = True

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        write_json_status(
            status_path,
            {
                "state": "starting",
                "recoveredParts": recovered,
                "clickhouseVersion": client.ping(),
                "source": metadata.clickhouse_change_tracking_state(),
                "pack": pack_manifest.stats(),
            },
        )
        try:
            while not stopping:
                try:
                    io_pressure_canary_override, pressure_exc = (
                        _admit_io_pressure(
                            packed_config,
                            canary,
                            paused=io_pressure_paused,
                        )
                    )
                    if io_pressure_canary_override:
                        io_pressure_paused = True
                        if not io_pressure_override_active:
                            LOGGER.warning(
                                "Host I/O PSI is above the raw OSS worker "
                                "ceiling but the serving canary is healthy; "
                                "allowing one bounded part: %s",
                                pressure_exc,
                            )
                        io_pressure_override_active = True
                    else:
                        io_pressure_paused = False
                        if io_pressure_override_active:
                            LOGGER.info(
                                "Host I/O PSI recovered below the raw OSS "
                                "worker ceiling"
                            )
                        io_pressure_override_active = False
                    before = apply_pending_raw_oss_changes(
                        metadata,
                        pack_manifest,
                        client,
                        settings,
                        manifest_table=(
                            f"{base_config.database}.{raw_config.manifest_table}"
                        ),
                    )
                    ingested = ingest_one(
                        metadata,
                        pack_manifest,
                        client,
                        archive,
                        packed_config,
                        scratch,
                        paths["root"],
                        health_probe=canary.probe,
                        prefer_newest=False,
                        allow_high_io_pressure=io_pressure_canary_override,
                        verify_name_table=False,
                    )
                    after = apply_pending_raw_oss_changes(
                        metadata,
                        pack_manifest,
                        client,
                        settings,
                        manifest_table=(
                            f"{base_config.database}.{raw_config.manifest_table}"
                        ),
                        max_rounds=1,
                    )
                    state = "running" if ingested or before["scanned"] else "idle"
                    write_json_status(
                        status_path,
                        {
                            "state": state,
                            "reconcileBefore": before,
                            "lastPart": ingested or {},
                            "reconcileAfter": after,
                            "ioPressureCanaryOverride": (
                                io_pressure_canary_override
                            ),
                            "source": metadata.clickhouse_change_tracking_state(),
                            "pack": pack_manifest.stats(),
                        },
                    )
                    if once:
                        return 0
                    if not ingested and not before["scanned"]:
                        time.sleep(idle_seconds)
                except IngestPaused as exc:
                    was_paused = io_pressure_paused
                    io_pressure_paused = True
                    if not was_paused:
                        LOGGER.warning("Raw OSS worker paused: %s", exc)
                    io_pressure_override_active = False
                    write_json_status(
                        status_path,
                        {
                            "state": "paused",
                            "lastError": str(exc),
                            "source": metadata.clickhouse_change_tracking_state(),
                            "pack": pack_manifest.stats(),
                        },
                    )
                    if once:
                        return 2
                    time.sleep(max(idle_seconds, 5.0))
                except IngestPartError as exc:
                    LOGGER.error("Raw OSS packed part failed: %s", exc)
                    write_json_status(
                        status_path,
                        {
                            "state": "error",
                            "lastError": str(exc),
                            "source": metadata.clickhouse_change_tracking_state(),
                            "pack": pack_manifest.stats(),
                        },
                    )
                    if once:
                        return 3
                    time.sleep(idle_seconds)
                except Exception as exc:
                    LOGGER.exception("Raw OSS synchronization failed")
                    write_json_status(
                        status_path,
                        {
                            "state": "error",
                            "lastError": str(exc),
                            "source": metadata.clickhouse_change_tracking_state(),
                            "pack": pack_manifest.stats(),
                        },
                    )
                    if once:
                        return 1
                    time.sleep(max(idle_seconds, 5.0))
        finally:
            metadata.close()
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return run_worker(args.data_dir, once=args.once)


if __name__ == "__main__":
    raise SystemExit(main())
