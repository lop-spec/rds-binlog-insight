from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from pathlib import Path

from app.clickhouse_client import ClickHouseClient, ClickHouseConfig
from app.clickhouse_manifest import ClickHouseManifest
from app.clickhouse_raw_oss import (
    ClickHouseRawOssConfig,
    build_raw_oss_manifest_rows,
)
from app.metadata import MetadataStore


def _resume_after_path(
    client: ClickHouseClient,
    *,
    table: str,
) -> str:
    rows = client.json_rows(
        f"""
        SELECT max(part_path) AS after_path
        FROM {table} FINAL
        WHERE change_version = 0 AND is_deleted = 0
        """,
        timeout=30,
    )
    return str(rows[0].get("after_path") or "") if rows else ""


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Load the small active-object/catalog manifest used by direct "
            "ClickHouse queries over the original OSS Parquet."
        )
    )
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--batch-size", type=int, default=256)
    parser.add_argument("--max-pages", type=int, default=0)
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Explicitly truncate the non-serving manifest before a full reload.",
    )
    args = parser.parse_args()
    batch_size = min(max(int(args.batch_size), 1), 2048)
    max_pages = max(int(args.max_pages), 0)

    config = ClickHouseConfig.from_env()
    raw_config = ClickHouseRawOssConfig.from_env()
    if not raw_config.enabled:
        raise RuntimeError("RDS_BINLOG_CLICKHOUSE_RAW_OSS_ENABLED=1 is required")
    client = ClickHouseClient(config)
    table = f"{config.database}.{raw_config.manifest_table}"
    metadata = MetadataStore(
        args.data_dir / "metadata.sqlite3",
        run_migrations=False,
    )
    settings = metadata.load_settings()
    if args.reset:
        if raw_config.serving_enabled:
            raise RuntimeError("Refusing to reset a serving raw OSS manifest")
        client.query(f"TRUNCATE TABLE {table}", timeout=120)
    after_path = _resume_after_path(client, table=table)
    started = time.monotonic()
    pages = 0
    inserted = 0
    catalog_unknown = 0
    while not max_pages or pages < max_pages:
        parts = metadata.clickhouse_source_parts_page(
            after_path=after_path,
            limit=batch_size,
        )
        if not parts:
            break
        paths = [str(part["path"]) for part in parts]
        catalogs = metadata.catalog_store.catalogs(paths)
        rows = build_raw_oss_manifest_rows(
            settings,
            parts,
            catalogs=catalogs,
        )
        client.insert_json_rows(table, rows, timeout=120)
        after_path = paths[-1]
        pages += 1
        inserted += len(rows)
        catalog_unknown += sum(
            not int(row.get("catalog_ready") or 0) for row in rows
        )
        if pages == 1 or pages % 25 == 0:
            print(
                json.dumps(
                    {
                        "state": "running",
                        "pages": pages,
                        "inserted_this_run": inserted,
                        "after_path": after_path,
                        "catalog_unknown_this_run": catalog_unknown,
                        "elapsed_seconds": round(time.monotonic() - started, 3),
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                flush=True,
            )
        if len(parts) < batch_size:
            break

    complete = not metadata.clickhouse_source_parts_page(
        after_path=after_path,
        limit=1,
    )
    totals = client.json_rows(
        f"""
        SELECT count() AS rows,
               countIf(catalog_ready = 0) AS catalog_unknown,
               countIf(oss_length > 0) AS packed_parts,
               sum(row_count) AS source_rows,
               sum(size_bytes) AS source_bytes
        FROM {table} FINAL
        WHERE is_deleted = 0
        """,
        timeout=60,
    )
    packed_manifest_path = (
        args.data_dir
        / "index"
        / "clickhouse"
        / "raw-oss-packed-manifest.sqlite3"
    )
    packed_manifest = ClickHouseManifest(
        packed_manifest_path,
        run_migrations=True,
    )
    pack_reconcile: dict[str, int] = {}
    ranged_parts: list[dict[str, object]] = []
    if complete:
        ranged_after = ""
        while True:
            page = metadata.clickhouse_ranged_source_parts_page(
                after_path=ranged_after,
                limit=2048,
            )
            if not page:
                break
            ranged_parts.extend(page)
            ranged_after = str(page[-1]["path"])
        now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
        pack_reconcile = packed_manifest.reconcile(
            ranged_parts,
            start_epoch_us=0,
            end_epoch_us=now_us,
            source_parts=len(ranged_parts),
            sweep_unseen=True,
        )
    result = {
        "state": "complete" if complete else "paused",
        "complete": complete,
        "pages_this_run": pages,
        "inserted_this_run": inserted,
        "after_path": after_path,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "manifest_table": table,
        "packed_manifest": str(packed_manifest_path),
        "packed_parts": len(ranged_parts),
        "pack_reconcile": pack_reconcile,
        "totals": totals[0] if totals else {},
        "source_tracking": metadata.clickhouse_change_tracking_state(),
        "checked_at": datetime.now(UTC).isoformat(),
    }
    print(
        json.dumps(result, ensure_ascii=False, separators=(",", ":")),
        flush=True,
    )
    metadata.close()
    return 0 if complete else 3


if __name__ == "__main__":
    raise SystemExit(main())
