from __future__ import annotations

import argparse
import json
from pathlib import Path

import sqlparse

from .clickhouse_client import ClickHouseClient, ClickHouseConfig
from .clickhouse_manifest import ClickHouseManifest
from .clickhouse_oss import ClickHouseOssConfig, build_oss_schema
from .clickhouse_raw_oss import (
    ClickHouseRawOssConfig,
    build_raw_oss_schema,
)
from .metadata import MetadataStore


def _split_schema_statements(schema: str) -> list[str]:
    statements: list[str] = []
    for candidate in sqlparse.split(schema, strip_semicolon=True):
        statement = candidate.strip()
        executable = sqlparse.format(
            statement,
            strip_comments=True,
        ).strip()
        if statement and executable:
            statements.append(statement)
    return statements


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Explicitly migrate the ClickHouse hot serving layer."
    )
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument(
        "--schema",
        type=Path,
        action="append",
        default=None,
        help="Schema file to apply; repeat to override the ordered defaults.",
    )
    parser.add_argument("--manifest-only", action="store_true")
    parser.add_argument(
        "--oss-object-tables",
        action="store_true",
        help=(
            "Explicitly create the all-history OSS-backed query tables and "
            "their separate durable manifest."
        ),
    )
    parser.add_argument(
        "--raw-oss-tables",
        action="store_true",
        help=(
            "Explicitly create the active source manifest and the small "
            "packed-object exception table used for direct raw OSS queries."
        ),
    )
    args = parser.parse_args()

    manifest_path = args.data_dir / "index" / "clickhouse" / "manifest.sqlite3"
    slowlog_manifest_path = (
        args.data_dir
        / "index"
        / "clickhouse"
        / "slowlog-manifest.sqlite3"
    )
    ClickHouseManifest(manifest_path, run_migrations=True)
    ClickHouseManifest(slowlog_manifest_path, run_migrations=True)
    result: dict[str, object] = {
        "manifest": str(manifest_path),
        "slowlog_manifest": str(slowlog_manifest_path),
        "manifest_schema": 1,
    }
    metadata: MetadataStore | None = None
    if args.oss_object_tables or args.raw_oss_tables:
        metadata = MetadataStore(
            args.data_dir / "metadata.sqlite3",
            run_migrations=True,
        )
        result["source_change_tracking"] = (
            metadata.clickhouse_change_tracking_state()
        )
    if args.oss_object_tables:
        oss_config = ClickHouseOssConfig.from_env()
        if not oss_config.enabled:
            raise RuntimeError(
                "RDS_BINLOG_CLICKHOUSE_OSS_ENABLED=1 is required for "
                "--oss-object-tables"
            )
        oss_manifest_path = (
            args.data_dir
            / "index"
            / "clickhouse"
            / oss_config.manifest_name
        )
        ClickHouseManifest(oss_manifest_path, run_migrations=True)
        result["oss_manifest"] = str(oss_manifest_path)
    raw_config: ClickHouseRawOssConfig | None = None
    if args.raw_oss_tables:
        raw_config = ClickHouseRawOssConfig.from_env()
        if not raw_config.enabled:
            raise RuntimeError(
                "RDS_BINLOG_CLICKHOUSE_RAW_OSS_ENABLED=1 is required for "
                "--raw-oss-tables"
            )
    if not args.manifest_only:
        schemas = args.schema or [
            Path("/app/clickhouse/002_events.sql"),
            Path("/app/clickhouse/003_slowlog_events.sql"),
            Path("/app/clickhouse/005_events_query.sql"),
        ]
        config = ClickHouseConfig.from_env()
        client = ClickHouseClient(config)
        for schema_path in schemas:
            schema = schema_path.read_text(encoding="utf-8")
            for statement in _split_schema_statements(schema):
                client.query(statement, timeout=120)
        if args.oss_object_tables:
            assert metadata is not None
            settings = metadata.load_settings()
            object_schema = build_oss_schema(
                settings,
                oss_config,
                database=config.database,
            )
            for statement in _split_schema_statements(object_schema):
                client.query(statement, timeout=120)
            result["oss_tables"] = {
                "query": f"{config.database}.{oss_config.query_table}",
                "name_query": (
                    f"{config.database}.{oss_config.name_query_table}"
                ),
            }
            if oss_config.staged_backfill_enabled:
                result["oss_tables"].update(
                    {
                        "stage_query": (
                            f"{config.database}."
                            f"{oss_config.stage_query_table}"
                        ),
                        "stage_name_query": (
                            f"{config.database}."
                            f"{oss_config.stage_name_query_table}"
                        ),
                    }
                )
            if oss_config.incremental_mv_enabled:
                result["oss_tables"]["materialized_view"] = (
                    f"{config.database}.{oss_config.materialized_view}"
                )
        if args.raw_oss_tables:
            assert metadata is not None and raw_config is not None
            settings = metadata.load_settings()
            raw_schema = build_raw_oss_schema(
                settings,
                raw_config,
                database=config.database,
            )
            for statement in _split_schema_statements(raw_schema):
                client.query(statement, timeout=120)
            result["raw_oss_tables"] = {
                "manifest": (
                    f"{config.database}.{raw_config.manifest_table}"
                ),
                "packed": f"{config.database}.{raw_config.packed_table}",
            }
        result.update(
            {
                "clickhouse_version": client.ping(),
                "table": config.qualified_table,
                "schemas": [str(path) for path in schemas],
            }
        )
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
