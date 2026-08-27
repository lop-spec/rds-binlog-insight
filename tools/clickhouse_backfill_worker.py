from __future__ import annotations

import argparse
import logging
import re
import signal
import time
from pathlib import Path
from typing import Any

from app.clickhouse_client import ClickHouseClient, ClickHouseConfig
from app.clickhouse_ingest import IngestPartError, IngestPaused, ingest_one
from app.clickhouse_manifest import ClickHouseManifest
from app.config import ensure_data_dirs
from app.credentials import load_credential
from app.maintenance_status import write_json_status
from app.metadata import MetadataStore
from app.oss_store import OssArchive


LOGGER = logging.getLogger(__name__)
SAFE_ID = re.compile(r"^[a-z0-9_-]{1,32}$")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "One explicit migration-only ClickHouse backfill lane. "
            "The normal ingester remains the sole reconciler."
        )
    )
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--idle-exit-seconds", type=float, default=30.0)
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    if not SAFE_ID.fullmatch(args.worker_id):
        raise ValueError("worker-id must match [a-z0-9_-]{1,32}")
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    config = ClickHouseConfig.from_env()
    if not config.enabled:
        raise RuntimeError("ClickHouse ingestion is disabled")
    paths = ensure_data_dirs(args.data_dir)
    metadata = MetadataStore(
        args.data_dir / "metadata.sqlite3",
        run_migrations=False,
    )
    manifest = ClickHouseManifest(
        paths["index"] / "clickhouse" / "manifest.sqlite3",
        run_migrations=False,
    )
    client = ClickHouseClient(config)
    client.ping()
    settings = metadata.load_settings()
    archive = OssArchive(
        settings,
        credential=load_credential(settings.credential_target),
    )
    status_path = (
        paths["logs"] / f"clickhouse-backfill-{args.worker_id}.json"
    )
    stopping = False

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    completed = 0
    errors = 0
    idle_started = 0.0
    while not stopping:
        try:
            result = ingest_one(
                metadata,
                manifest,
                client,
                archive,
                config,
                paths["scratch"] / f"clickhouse-backfill-{args.worker_id}",
                paths["root"],
            )
        except IngestPaused as exc:
            write_json_status(
                status_path,
                {
                    "state": "paused",
                    "lastError": str(exc),
                    "completed": completed,
                    "errors": errors,
                },
            )
            return 2
        except IngestPartError as exc:
            errors += 1
            LOGGER.exception("Backfill part failed")
            write_json_status(
                status_path,
                {
                    "state": "error",
                    "partPath": exc.part_path,
                    "lastError": str(exc),
                    "completed": completed,
                    "errors": errors,
                },
            )
            time.sleep(config.idle_seconds)
            continue
        if result is None:
            if not idle_started:
                idle_started = time.monotonic()
            if time.monotonic() - idle_started >= max(
                float(args.idle_exit_seconds), 1.0
            ):
                write_json_status(
                    status_path,
                    {
                        "state": "complete",
                        "completed": completed,
                        "errors": errors,
                        "stats": manifest.stats(),
                    },
                )
                return 0
            time.sleep(config.idle_seconds)
            continue
        idle_started = 0.0
        completed += 1
        write_json_status(
            status_path,
            {
                "state": "running",
                "lastResult": result,
                "completed": completed,
                "errors": errors,
            },
        )
    write_json_status(
        status_path,
        {
            "state": "stopped",
            "completed": completed,
            "errors": errors,
        },
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
