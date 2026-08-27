from __future__ import annotations

import argparse
import json
import time
from dataclasses import replace
from pathlib import Path

from app.clickhouse_client import ClickHouseClient, ClickHouseConfig
from app.clickhouse_ingest import (
    HealthCanary,
    IngestPartError,
    ingest_one,
)
from app.clickhouse_manifest import ClickHouseManifest
from app.clickhouse_raw_oss import ClickHouseRawOssConfig
from app.credentials import load_credential
from app.metadata import MetadataStore
from app.oss_store import OssArchive


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Load only custom ranged .parquet-pack members into the raw-OSS "
            "ClickHouse exception table."
        )
    )
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--max-parts", type=int, default=0)
    parser.add_argument("--idle-seconds", type=float, default=0.1)
    parser.add_argument(
        "--skip-recovery",
        action="store_true",
        help=(
            "Do not reset loading claims. Use only for an additional worker "
            "after one primary worker has completed startup recovery."
        ),
    )
    parser.add_argument(
        "--allow-high-io-pressure",
        action="store_true",
        help="Rely on the production HTTP canary instead of host-wide PSI.",
    )
    args = parser.parse_args()
    max_parts = max(int(args.max_parts), 0)
    idle_seconds = max(float(args.idle_seconds), 0.0)

    base_config = ClickHouseConfig.from_env()
    raw_config = ClickHouseRawOssConfig.from_env()
    if not raw_config.enabled:
        raise RuntimeError("RDS_BINLOG_CLICKHOUSE_RAW_OSS_ENABLED=1 is required")
    packed_config = replace(
        base_config,
        table=raw_config.packed_table,
        query_table=raw_config.packed_table,
        name_query_table=raw_config.packed_table,
        ingest_mode="query",
    )
    metadata = MetadataStore(
        args.data_dir / "metadata.sqlite3",
        run_migrations=False,
    )
    manifest = ClickHouseManifest(
        args.data_dir
        / "index"
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
    scratch = args.data_dir / "tmp" / "clickhouse-raw-packed"
    scratch.mkdir(parents=True, exist_ok=True)
    recovered = 0 if args.skip_recovery else manifest.recover_loading()
    canary = HealthCanary(packed_config)
    completed = 0
    failed = 0
    started = time.monotonic()
    print(
        json.dumps(
            {
                "state": "starting",
                "recovered": recovered,
                "clickhouse_version": client.ping(),
                "stats": manifest.stats(),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        flush=True,
    )
    while not max_parts or completed < max_parts:
        try:
            result = ingest_one(
                metadata,
                manifest,
                client,
                archive,
                packed_config,
                scratch,
                args.data_dir,
                health_probe=canary.probe,
                prefer_newest=False,
                allow_high_io_pressure=bool(args.allow_high_io_pressure),
                verify_name_table=False,
            )
        except IngestPartError as exc:
            failed += 1
            print(
                json.dumps(
                    {
                        "state": "part-failed",
                        "part_path": exc.part_path,
                        "error": str(exc),
                        "failed_this_run": failed,
                        "stats": manifest.stats(),
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                flush=True,
            )
            continue
        if result is None:
            break
        completed += 1
        if completed == 1 or completed % 10 == 0:
            print(
                json.dumps(
                    {
                        "state": "running",
                        "completed_this_run": completed,
                        "failed_this_run": failed,
                        "last": result,
                        "elapsed_seconds": round(
                            time.monotonic() - started, 3
                        ),
                        "stats": manifest.stats(),
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                flush=True,
            )
        if idle_seconds:
            time.sleep(idle_seconds)
    stats = manifest.stats()
    state = (
        "complete"
        if not int(stats.get("pending_parts") or 0)
        and not int(stats.get("failed_parts") or 0)
        and not int(stats.get("delete_parts") or 0)
        else "incomplete"
    )
    print(
        json.dumps(
            {
                "state": state,
                "completed_this_run": completed,
                "failed_this_run": failed,
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "stats": stats,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        flush=True,
    )
    metadata.close()
    return 0 if state == "complete" else 3


if __name__ == "__main__":
    raise SystemExit(main())
