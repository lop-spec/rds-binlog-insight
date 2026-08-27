from __future__ import annotations

import os
import io
import hashlib
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from unittest.mock import patch

from app.config import Settings
from app.clickhouse_client import (
    ClickHouseClient,
    ClickHouseConfig,
    validate_part_state_batch_size,
)
from app.clickhouse_manifest import ClickHouseManifest
from app.clickhouse_query import ClickHouseQueryBackend
from app.clickhouse_raw_oss import (
    ClickHouseRawOssConfig,
    build_exact_raw_oss_source_sql,
    build_raw_oss_manifest_rows,
    build_raw_oss_candidate_sql,
    build_raw_oss_schema,
    raw_oss_day_windows,
)
from app.clickhouse_raw_sync import apply_pending_raw_oss_changes
from app.metadata import MetadataStore
from app.clickhouse_oss import (
    ClickHouseOssConfig,
    build_direct_s3_insert_sql,
    build_oss_schema,
    history_start_epoch_us,
    split_direct_and_ranged_parts,
)
from app.rds_api import RemoteBinlog
from app.storage import EventStorage, StorageError


class ClickHouseOssConfigTests(unittest.TestCase):
    def test_raw_oss_schema_is_small_manifest_plus_packed_exception_table(self):
        settings = Settings(
            oss_enabled=True,
            oss_bucket="example-binlog-bucket",
            oss_region_id="cn-hangzhou",
            oss_endpoint="oss-cn-hangzhou-internal.aliyuncs.com",
            oss_prefix="mysql-binlog/rm-test/",
        )
        config = ClickHouseRawOssConfig(
            enabled=True,
            serving_enabled=False,
            manifest_table="oss_active_parts_v1",
            packed_table="events_query_packed_v1",
            prefix="sql-insight-clickhouse/raw-v1/",
            cache_gb=20,
        )

        schema = build_raw_oss_schema(settings, config, database="insight")

        self.assertIn("CREATE TABLE IF NOT EXISTS insight.oss_active_parts_v1", schema)
        self.assertIn("ReplacingMergeTree(change_version, is_deleted)", schema)
        self.assertIn("ORDER BY part_path", schema)
        self.assertIn("database_names Array(String)", schema)
        self.assertIn("catalog_ready UInt8", schema)
        self.assertIn("CREATE TABLE IF NOT EXISTS insight.events_query_packed_v1", schema)
        self.assertIn("/raw-v1/packed-events/", schema)
        self.assertIn("use_environment_credentials=true", schema)
        self.assertNotIn("access_key", schema.lower())
        self.assertNotIn("secret", schema.lower())

    def test_raw_oss_uses_manifest_candidates_then_exact_object_keys(self):
        settings = Settings(
            oss_enabled=True,
            oss_bucket="example-binlog-bucket",
            oss_region_id="cn-hangzhou",
            oss_endpoint="oss-cn-hangzhou-internal.aliyuncs.com",
            oss_prefix="mysql-binlog/rm-test/",
        )
        config = ClickHouseRawOssConfig(
            enabled=True,
            serving_enabled=True,
            manifest_table="oss_active_parts_v1",
            packed_table="events_query_packed_v1",
            prefix="sql-insight-clickhouse/raw-v1/",
            cache_gb=20,
        )
        start_us = int(datetime(2026, 8, 25, 23, 0, tzinfo=UTC).timestamp() * 1_000_000)
        end_us = int(datetime(2026, 8, 26, 1, 0, tzinfo=UTC).timestamp() * 1_000_000)

        candidate_sql, parameters = build_raw_oss_candidate_sql(
            config,
            database="insight",
            query={
                "source": "database",
                "instance": "rm-test",
                "database": "orders",
                "table": "items",
                "operations": ["INSERT"],
            },
            start_epoch_us=start_us,
            end_epoch_us=end_us,
            limit=16,
        )

        self.assertIn("FROM insight.oss_active_parts_v1 FINAL", candidate_sql)
        self.assertIn("ORDER BY max_event_epoch_us DESC, part_path DESC", candidate_sql)
        self.assertIn("oss_length = 0", candidate_sql)
        self.assertIn("oss_length > 0", candidate_sql)
        self.assertIn("catalog_ready = 0 OR", candidate_sql)
        self.assertNotIn("FROM s3(", candidate_sql)
        self.assertEqual(parameters["raw_instance"], "rm-test")
        self.assertEqual(parameters["raw_database"], "orders")
        self.assertEqual(parameters["raw_table"], "items")

        exact_sql, exact_parameters = build_exact_raw_oss_source_sql(
            settings,
            config,
            database="insight",
            candidates=[
                {
                    "oss_path": "example-binlog-bucket/mysql-binlog/rm-test/a.parquet",
                    "oss_key": "mysql-binlog/rm-test/a.parquet",
                    "oss_length": 0,
                    "logical_part_id": "part-a",
                    "sha256": "a" * 64,
                    "content_revision": 3,
                },
                {
                    "oss_path": "example-binlog-bucket/mysql-slow-log/rm-test/b.parquet",
                    "oss_key": "mysql-slow-log/rm-test/b.parquet",
                    "oss_length": 0,
                    "logical_part_id": "part-b",
                    "sha256": "b" * 64,
                    "content_revision": 4,
                },
                {
                    "oss_path": "example-binlog-bucket/mysql-binlog/rm-test/p.parquet-pack",
                    "oss_key": "mysql-binlog/rm-test/p.parquet-pack",
                    "oss_length": 100,
                    "logical_part_id": "part-packed",
                    "sha256": "c" * 64,
                    "content_revision": 5,
                },
            ],
        )

        self.assertIn(
            "/{mysql-binlog/rm-test/a.parquet,"
            "mysql-slow-log/rm-test/b.parquet}",
            exact_sql,
        )
        self.assertIn("mapContains(raw_part_keys, _path)", exact_sql)
        self.assertIn("FROM insight.events_query_packed_v1", exact_sql)
        self.assertNotIn("**", exact_sql)
        self.assertNotIn("oss_active_parts_v1", exact_sql)
        self.assertNotIn(".parquet-pack}", exact_sql)
        self.assertNotIn("access_key", exact_sql.lower())
        self.assertNotIn("secret", exact_sql.lower())
        self.assertEqual(
            exact_parameters["raw_pack_identity_0"], "part-packed"
        )

    def test_raw_oss_day_windows_are_newest_first_and_utc_bounded(self):
        start_us = int(datetime(2026, 8, 24, 23, 30, tzinfo=UTC).timestamp() * 1_000_000)
        end_us = int(datetime(2026, 8, 26, 0, 30, tzinfo=UTC).timestamp() * 1_000_000)

        windows = raw_oss_day_windows(start_us, end_us)

        self.assertEqual([item[0] for item in windows], ["2026-08-26", "2026-08-25", "2026-08-24"])
        self.assertEqual(windows[0][1], int(datetime(2026, 8, 26, tzinfo=UTC).timestamp() * 1_000_000))
        self.assertEqual(windows[0][2], end_us)
        self.assertEqual(windows[-1][1], start_us)

    def test_raw_oss_manifest_rows_are_versioned_and_catalog_safe(self):
        settings = Settings(
            oss_enabled=True,
            oss_bucket="example-binlog-bucket",
            oss_region_id="cn-hangzhou",
            oss_endpoint="oss-cn-hangzhou-internal.aliyuncs.com",
            oss_prefix="mysql-binlog/rm-test/",
        )
        part = {
            "path": "/data/a.parquet",
            "logical_part_id": "logical-a",
            "sha256": "a" * 64,
            "content_revision": 7,
            "event_date": "2026-08-26",
            "min_event_epoch_us": 10,
            "max_event_epoch_us": 20,
            "row_count": 30,
            "size_bytes": 40,
            "oss_key": "mysql-binlog/rm-test/parquet/event_date=2026-08-26/a.parquet",
            "oss_offset": 0,
            "oss_length": 0,
            "instance_id": "rm-test",
            "change_version": 11,
            "exists": True,
            "query_visible": 1,
        }
        rows = build_raw_oss_manifest_rows(
            settings,
            [part],
            catalogs={
                part["path"]: {
                    "sha256": part["sha256"],
                    "content_revision": 7,
                    "databases": ["orders"],
                    "tables": ["items"],
                    "operations": ["INSERT"],
                }
            },
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["oss_path"], "example-binlog-bucket/" + part["oss_key"])
        self.assertEqual(rows[0]["change_version"], 11)
        self.assertEqual(rows[0]["catalog_ready"], 1)
        self.assertEqual(rows[0]["database_names"], ["orders"])
        self.assertEqual(rows[0]["is_deleted"], 0)
        self.assertRegex(
            rows[0]["updated_at"],
            r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}$",
        )

    def test_direct_backfill_batch_is_bounded_by_verifier_limit(self):
        self.assertEqual(validate_part_state_batch_size(256), 256)
        with self.assertRaisesRegex(ValueError, "at most 256"):
            validate_part_state_batch_size(257)

    def test_incremental_manifest_reconcile_preserves_full_history_state(self):
        with tempfile.TemporaryDirectory() as temp:
            manifest = ClickHouseManifest(
                Path(temp) / "manifest.sqlite3", run_migrations=True
            )
            now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
            old = {
                "path": "/data/old.parquet",
                "logical_part_id": "old",
                "sha256": "a" * 64,
                "content_revision": 1,
                "min_event_epoch_us": now_us - 30 * 86400 * 1_000_000,
                "max_event_epoch_us": now_us - 30 * 86400 * 1_000_000,
                "row_count": 1,
                "size_bytes": 1,
            }
            recent = {
                **old,
                "path": "/data/recent.parquet",
                "logical_part_id": "recent",
                "max_event_epoch_us": now_us,
                "min_event_epoch_us": now_us,
            }
            full_start = now_us - 61 * 86400 * 1_000_000
            manifest.reconcile(
                [old, recent],
                start_epoch_us=full_start,
                end_epoch_us=now_us,
            )
            first = manifest.stats()

            manifest.reconcile(
                [recent],
                start_epoch_us=now_us - 6 * 3600 * 1_000_000,
                end_epoch_us=now_us + 1,
                sweep_unseen=False,
                preserve_reconcile_state=True,
            )
            second = manifest.stats()

            self.assertEqual(second["reconcile_start_epoch_us"], full_start)
            self.assertEqual(
                second["reconcile_completed_at_us"],
                first["reconcile_completed_at_us"],
            )
            with manifest.connection() as connection:
                status = connection.execute(
                    "SELECT status FROM clickhouse_parts "
                    "WHERE logical_part_id = 'old'"
                ).fetchone()["status"]
            self.assertEqual(status, "pending")

    def test_object_backend_serves_older_manifest_covered_history(self):
        now = datetime.now(UTC)
        event_us = int((now - timedelta(days=30)).timestamp() * 1_000_000)
        part = {
            "path": "/data/events/old.parquet",
            "logical_part_id": "old-part",
            "sha256": hashlib.sha256(b"old").hexdigest(),
            "content_revision": 1,
            "min_event_epoch_us": event_us,
            "max_event_epoch_us": event_us,
            "row_count": 1,
            "size_bytes": 3,
            "oss_key": "mysql-binlog/old.parquet",
        }

        class _Metadata:
            @staticmethod
            def clickhouse_change_tracking_state():
                return {"complete": True, "pending": False}

            def parts_in_range(self, **_kwargs):
                raise AssertionError(
                    "all-history object serving must not enumerate source parts"
                )

        class _Client:
            def __init__(self):
                self.sql = ""

            def json_rows(self, sql, **_kwargs):
                self.sql = sql
                return [{"event_id": "old-event", "event_epoch_us": event_us}]

        base_config = ClickHouseConfig(
            enabled=True,
            serving_enabled=False,
            host="clickhouse",
            port=8123,
            database="insight",
            table="events",
            user="test",
            password="test",
            hot_hours=27,
            serving_hours=25,
            reconcile_seconds=30,
            idle_seconds=1,
            health_url="",
            health_host_header="",
            health_max_seconds=1,
            min_free_gb=20,
            io_pressure_max_full_avg10=10,
        )
        oss_config = ClickHouseOssConfig(
            enabled=True,
            serving_enabled=True,
            prefix="sql-insight/v1/",
            cache_gb=40,
            query_table="events_query_oss",
            name_query_table="events_query_by_name_oss",
            materialized_view="events_query_oss_to_name_mv",
            manifest_name="oss-manifest.sqlite3",
            history_days=61,
        )
        with tempfile.TemporaryDirectory() as temp:
            manifest = ClickHouseManifest(
                Path(temp) / "oss-manifest.sqlite3", run_migrations=True
            )
            manifest.reconcile(
                [part],
                start_epoch_us=int(
                    (now - timedelta(days=61)).timestamp() * 1_000_000
                ),
                end_epoch_us=int(now.timestamp() * 1_000_000),
            )
            manifest.claim_next()
            manifest.mark_ready(str(part["path"]), "old-part", 1)
            client = _Client()
            backend = ClickHouseQueryBackend(
                _Metadata(),
                Path(temp),
                base_config,
                client=client,
                manifest=manifest,
                oss_config=oss_config,
            )

            result = backend.query_events(
                {
                    "source": "binlog",
                    "start_epoch_us": event_us - 1,
                    "end_epoch_us": event_us + 1,
                    "limit": 100,
                },
                retention_days=60,
                limit_cap=1000,
            )

        self.assertEqual(result["tiers_used"], ["clickhouse-oss"])
        self.assertIn("FROM insight.events_query_oss", client.sql)

    def test_object_backend_falls_back_while_source_change_is_pending(self):
        class _Metadata:
            @staticmethod
            def clickhouse_change_tracking_state():
                return {"complete": True, "pending": True}

            def parts_in_range(self, **_kwargs):
                raise AssertionError("pending source gate must fail before enumeration")

        class _Client:
            def json_rows(self, *_args, **_kwargs):
                raise AssertionError("pending source gate must not query ClickHouse")

        config = ClickHouseConfig(
            enabled=True,
            serving_enabled=False,
            host="clickhouse",
            port=8123,
            database="insight",
            table="events",
            user="test",
            password="test",
            hot_hours=27,
            serving_hours=25,
            reconcile_seconds=30,
            idle_seconds=1,
            health_url="",
            health_host_header="",
            health_max_seconds=1,
            min_free_gb=20,
            io_pressure_max_full_avg10=10,
        )
        oss_config = ClickHouseOssConfig(
            enabled=True,
            serving_enabled=True,
            prefix="sql-insight/v1/",
            cache_gb=40,
            query_table="events_query_oss",
            name_query_table="events_query_by_name_oss",
            materialized_view="events_query_oss_to_name_mv",
            manifest_name="oss-manifest.sqlite3",
            history_days=0,
        )
        with tempfile.TemporaryDirectory() as temp:
            manifest = ClickHouseManifest(
                Path(temp) / "oss-manifest.sqlite3", run_migrations=True
            )
            manifest.reconcile([], start_epoch_us=0, end_epoch_us=1)
            backend = ClickHouseQueryBackend(
                _Metadata(),
                Path(temp),
                config,
                client=_Client(),
                manifest=manifest,
                oss_config=oss_config,
            )

            result = backend.query_events(
                {
                    "source": "binlog",
                    "start_epoch_us": 0,
                    "end_epoch_us": 1,
                },
                retention_days=60,
                limit_cap=1000,
            )

        self.assertIsNone(result)

    def test_raw_backend_serves_original_oss_without_source_part_enumeration(self):
        event_us = int(datetime(2026, 8, 26, 0, 5, tzinfo=UTC).timestamp() * 1_000_000)

        class _Metadata:
            @staticmethod
            def clickhouse_change_tracking_state():
                return {"complete": True, "pending": False}

            @staticmethod
            def load_settings():
                return Settings(
                    oss_enabled=True,
                    oss_bucket="example-binlog-bucket",
                    oss_region_id="cn-hangzhou",
                    oss_endpoint="oss-cn-hangzhou-internal.aliyuncs.com",
                    oss_prefix="mysql-binlog/rm-test/",
                )

            def parts_in_range(self, **_kwargs):
                raise AssertionError("raw OSS serving must never enumerate metadata parts")

        class _Client:
            def __init__(self):
                self.sql: list[str] = []
                self.calls: list[tuple[str, dict]] = []

            def json_rows(self, sql, **_kwargs):
                self.sql.append(sql)
                self.calls.append((sql, dict(_kwargs)))
                if "FROM insight.oss_active_parts_v1 FINAL" in sql:
                    return [
                        {
                            "part_path": "/data/raw.parquet",
                            "logical_part_id": "raw-part",
                            "sha256": "a" * 64,
                            "content_revision": 1,
                            "min_event_epoch_us": event_us - 1,
                            "max_event_epoch_us": event_us + 1,
                            "row_count": 1,
                            "size_bytes": 4,
                            "oss_path": "example-binlog-bucket/mysql-binlog/rm-test/raw.parquet",
                            "oss_key": "mysql-binlog/rm-test/raw.parquet",
                            "oss_offset": 0,
                            "oss_length": 0,
                        }
                    ]
                return [{"event_id": "raw-event", "event_epoch_us": event_us}]

        config = ClickHouseConfig(
            enabled=True,
            serving_enabled=False,
            host="clickhouse",
            port=8123,
            database="insight",
            table="events",
            user="test",
            password="test",
            hot_hours=27,
            serving_hours=25,
            reconcile_seconds=30,
            idle_seconds=1,
            health_url="",
            health_host_header="",
            health_max_seconds=1,
            min_free_gb=20,
            io_pressure_max_full_avg10=10,
        )
        raw_config = ClickHouseRawOssConfig(
            enabled=True,
            serving_enabled=True,
            manifest_table="oss_active_parts_v1",
            packed_table="events_query_packed_v1",
            prefix="sql-insight-clickhouse/raw-v1/",
            cache_gb=20,
        )
        with tempfile.TemporaryDirectory() as temp:
            pack_manifest = ClickHouseManifest(
                Path(temp) / "raw-pack-manifest.sqlite3", run_migrations=True
            )
            pack_manifest.reconcile(
                [], start_epoch_us=0, end_epoch_us=event_us, source_parts=0
            )
            client = _Client()
            backend = ClickHouseQueryBackend(
                _Metadata(),
                Path(temp),
                config,
                client=client,
                raw_config=raw_config,
                raw_pack_manifest=pack_manifest,
            )

            result = backend.query_events(
                {
                    "source": "binlog",
                    "start_epoch_us": event_us - 1,
                    "end_epoch_us": event_us + 1,
                    "limit": 100,
                },
                retention_days=60,
                limit_cap=1000,
            )

        self.assertEqual(result["tiers_used"], ["clickhouse-raw-oss"])
        self.assertTrue(any("FROM s3(" in sql for sql in client.sql))
        self.assertFalse(any("**" in sql for sql in client.sql))
        self.assertFalse(any("_path GLOBAL IN" in sql for sql in client.sql))
        raw_calls = [
            kwargs
            for sql, kwargs in client.calls
            if "AS raw_oss_source" in sql
        ]
        self.assertEqual(len(raw_calls), 1)
        raw_settings = raw_calls[0]["settings"]
        self.assertEqual(raw_settings["max_threads"], 1)
        self.assertEqual(raw_settings["max_download_threads"], 1)
        self.assertEqual(raw_settings["max_parsing_threads"], 1)
        self.assertEqual(raw_settings["input_format_parquet_max_block_size"], 1024)
        self.assertEqual(
            raw_settings["input_format_parquet_prefer_block_bytes"],
            8 * 1024 * 1024,
        )
        self.assertEqual(
            raw_settings["input_format_max_block_size_bytes"],
            32 * 1024 * 1024,
        )
        self.assertEqual(
            raw_settings["input_format_parquet_enable_row_group_prefetch"], 0
        )
        self.assertEqual(raw_settings["max_block_size"], 1024)

    def test_raw_backend_failure_never_falls_back_to_parquet_in_web_process(self):
        class _FailingRawBackend:
            raw_serving = True

            @staticmethod
            def query_events(*_args, **_kwargs):
                raise RuntimeError("simulated ClickHouse memory limit")

        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata = MetadataStore(root / "metadata.sqlite3")
            event_us = int(datetime.now(UTC).timestamp() * 1_000_000)
            with patch.dict(
                os.environ,
                {"RDS_BINLOG_CLICKHOUSE_ENABLED": "0"},
                clear=False,
            ):
                storage = EventStorage(metadata, root)
            storage.clickhouse_backend = _FailingRawBackend()
            query = {
                "source": "binlog",
                "start_epoch_us": event_us - 1,
                "end_epoch_us": event_us + 1,
                "limit": 20,
            }
            with (
                patch.object(
                    metadata,
                    "storage_metadata_stats",
                    return_value={
                        "oldest_epoch_us": event_us - 1,
                        "latest_epoch_us": event_us + 1,
                    },
                ),
                patch.object(
                    metadata,
                    "complete_query_certificate",
                    return_value=({"part_count": 0}, None),
                ),
                patch.object(
                    metadata,
                    "parts_in_range",
                    side_effect=AssertionError("Parquet fallback must not run"),
                ) as parts_in_range,
                patch("app.storage.LOGGER.exception") as log_exception,
            ):
                with self.assertRaises(StorageError) as raised:
                    storage._query_events_tiered_impl(
                        query,
                        Settings(retention_days=60),
                        None,
                    )

            self.assertEqual(
                raised.exception.code,
                "CLICKHOUSE_RAW_OSS_QUERY_UNAVAILABLE",
            )
            parts_in_range.assert_not_called()
            log_exception.assert_called_once()

    def test_source_change_queue_is_durable_coalesced_and_version_acked(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata = MetadataStore(root / "metadata.sqlite3")
            settings = Settings(db_instance_id="rm-prod")
            remote = RemoteBinlog(
                log_file_name="mysql-bin.000001",
                log_begin_utc="2026-08-26T00:00:00Z",
                log_end_utc="2026-08-26T01:00:00Z",
                file_size=1,
                checksum_crc64="",
                download_link="",
                intranet_download_link="",
                link_expired_utc="",
                remote_status="Completed",
                host_instance_id="mysql",
            )
            file_id, _ = metadata.upsert_remote(settings, remote)
            source = root / "part.parquet"
            digest = hashlib.sha256(b"part").hexdigest()
            metadata.replace_parts(
                file_id,
                [
                    {
                        "path": str(source),
                        "event_date": "2026-08-26",
                        "row_count": 1,
                        "min_event_epoch_us": 10,
                        "max_event_epoch_us": 20,
                        "size_bytes": 4,
                        "sha256": digest,
                    }
                ],
            )
            metadata.set_file_state(file_id, "done", event_count=1)
            page = metadata.clickhouse_source_parts_page(limit=1)
            self.assertEqual([item["path"] for item in page], [str(source)])
            self.assertEqual(page[0]["instance_id"], "rm-prod")
            self.assertTrue(page[0]["query_visible"])
            first = metadata.pending_clickhouse_changes(limit=10)
            self.assertEqual([item["path"] for item in first], [str(source)])
            first_version = int(first[0]["change_version"])
            self.assertEqual(first[0]["oss_key"], "")

            metadata.mark_part_archived(
                str(source),
                oss_key="mysql-binlog/rm-prod/part.parquet",
                oss_etag="etag",
            )
            second = metadata.pending_clickhouse_changes(limit=10)
            self.assertEqual(len(second), 1)
            self.assertGreater(int(second[0]["change_version"]), first_version)
            self.assertEqual(
                second[0]["oss_key"], "mysql-binlog/rm-prod/part.parquet"
            )
            self.assertEqual(
                metadata.ack_clickhouse_changes([(str(source), first_version)]),
                0,
            )

            metadata.mark_clickhouse_change_tracking_complete()
            self.assertEqual(
                metadata.clickhouse_change_tracking_state(),
                {"complete": True, "pending": True},
            )
            self.assertEqual(
                metadata.ack_clickhouse_changes(
                    [(str(source), int(second[0]["change_version"]))]
                ),
                1,
            )
            self.assertEqual(
                metadata.clickhouse_change_tracking_state(),
                {"complete": True, "pending": False},
            )

            with metadata.connection() as connection:
                connection.execute(
                    "UPDATE parquet_parts SET oss_offset = 10, oss_length = 4 "
                    "WHERE path = ?",
                    (str(source),),
                )
            ranged = metadata.pending_clickhouse_changes(limit=10)
            self.assertEqual([item["path"] for item in ranged], [str(source)])
            self.assertEqual(int(ranged[0]["oss_offset"]), 10)
            self.assertEqual(int(ranged[0]["oss_length"]), 4)
            self.assertEqual(
                metadata.ack_clickhouse_changes(
                    [(str(source), int(ranged[0]["change_version"]))]
                ),
                1,
            )

            metadata.delete_part(str(source))
            deleted = metadata.pending_clickhouse_changes(limit=10)
            self.assertEqual(len(deleted), 1)
            self.assertEqual(deleted[0]["path"], str(source))
            self.assertFalse(deleted[0]["exists"])

    def test_ranged_source_page_excludes_standalone_objects(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            metadata = MetadataStore(root / "metadata.sqlite3")
            settings = Settings(db_instance_id="rm-prod")
            remote = RemoteBinlog(
                log_file_name="mysql-bin.000002",
                log_begin_utc="2026-08-26T00:00:00Z",
                log_end_utc="2026-08-26T01:00:00Z",
                file_size=1,
                checksum_crc64="",
                download_link="",
                intranet_download_link="",
                link_expired_utc="",
                remote_status="Completed",
                host_instance_id="mysql",
            )
            file_id, _ = metadata.upsert_remote(settings, remote)
            digest = hashlib.sha256(b"pack-member").hexdigest()
            path = str(root / "member.parquet")
            metadata.replace_parts(
                file_id,
                [{
                    "path": path,
                    "event_date": "2026-08-26",
                    "row_count": 1,
                    "min_event_epoch_us": 10,
                    "max_event_epoch_us": 20,
                    "size_bytes": 11,
                    "sha256": digest,
                }],
            )
            metadata.set_file_state(file_id, "done", event_count=1)
            metadata.mark_part_archived(
                path,
                oss_key="mysql-binlog/rm-prod/packs/a.parquet-pack",
                oss_etag="etag",
                oss_offset=123,
                oss_length=11,
            )

            rows = metadata.clickhouse_ranged_source_parts_page(limit=10)

            self.assertEqual([row["path"] for row in rows], [path])
            self.assertEqual(rows[0]["oss_offset"], 123)
            self.assertEqual(rows[0]["oss_length"], 11)
            self.assertEqual(
                metadata.clickhouse_source_stats(),
                {
                    "source_parts": 1,
                    "archived_parts": 1,
                    "ranged_parts": 1,
                    "source_rows": 1,
                    "source_bytes": 11,
                    "min_event_epoch_us": 10,
                    "max_event_epoch_us": 20,
                },
            )

    def test_raw_worker_does_not_repeat_ranged_manifest_insert_while_loading(self):
        part = {
            "path": "/data/member.parquet",
            "logical_part_id": "packed-part",
            "sha256": "a" * 64,
            "content_revision": 1,
            "event_date": "2026-08-26",
            "min_event_epoch_us": 10,
            "max_event_epoch_us": 20,
            "row_count": 1,
            "size_bytes": 4,
            "oss_key": "mysql-binlog/rm-test/a.parquet-pack",
            "oss_offset": 100,
            "oss_length": 4,
            "instance_id": "rm-test",
            "change_version": 7,
            "exists": True,
            "query_visible": 1,
        }

        class _Catalog:
            @staticmethod
            def catalogs(_paths):
                return {}

        class _Metadata:
            catalog_store = _Catalog()

            def __init__(self):
                self.pending = True

            def pending_clickhouse_changes(self, **_kwargs):
                return [part] if self.pending else []

            def ack_clickhouse_changes(self, changes):
                if changes == [(part["path"], 7)]:
                    self.pending = False
                    return 1
                return 0

        class _Manifest:
            def __init__(self):
                self.status = None

            def part_status(self, *_args):
                return self.status

            def reconcile(self, *_args, **_kwargs):
                self.status = "pending"

            def coverage(self, _parts):
                return {"complete": self.status == "ready"}

            def queue_missing_paths(self, _paths):
                raise AssertionError("active ranged part must not be deleted")

        class _Client:
            def __init__(self):
                self.calls = 0

            def insert_json_rows(self, _table, rows, **_kwargs):
                self.calls += 1
                return len(rows)

        metadata = _Metadata()
        manifest = _Manifest()
        client = _Client()
        settings = Settings(oss_enabled=True, oss_bucket="example-binlog-bucket")

        first = apply_pending_raw_oss_changes(
            metadata,
            manifest,
            client,
            settings,
            manifest_table="insight.oss_active_parts_v1",
            max_rounds=1,
        )
        second = apply_pending_raw_oss_changes(
            metadata,
            manifest,
            client,
            settings,
            manifest_table="insight.oss_active_parts_v1",
            max_rounds=1,
        )
        manifest.status = "ready"
        third = apply_pending_raw_oss_changes(
            metadata,
            manifest,
            client,
            settings,
            manifest_table="insight.oss_active_parts_v1",
            max_rounds=1,
        )

        self.assertEqual(first["inserted"], 1)
        self.assertEqual(second["inserted"], 0)
        self.assertEqual(third["acknowledged"], 1)
        self.assertEqual(client.calls, 1)

    def test_global_manifest_gate_tracks_load_and_delete_states(self):
        part = {
            "path": "/data/part.parquet",
            "logical_part_id": "part-v1",
            "sha256": "a" * 64,
            "content_revision": 1,
            "min_event_epoch_us": 10,
            "max_event_epoch_us": 20,
            "row_count": 1,
            "size_bytes": 4,
        }
        with tempfile.TemporaryDirectory() as temp:
            manifest = ClickHouseManifest(
                Path(temp) / "manifest.sqlite3", run_migrations=True
            )
            manifest.reconcile(
                [part], start_epoch_us=0, end_epoch_us=100, source_parts=1
            )
            self.assertFalse(
                manifest.global_coverage(
                    source_complete=True, source_pending=False
                )["complete"]
            )
            manifest.claim_next()
            manifest.mark_ready(part["path"], part["logical_part_id"], 1)
            self.assertTrue(
                manifest.global_coverage(
                    source_complete=True, source_pending=False
                )["complete"]
            )
            manifest.queue_missing_paths([part["path"]])
            self.assertFalse(
                manifest.global_coverage(
                    source_complete=True, source_pending=False
                )["complete"]
            )

    def test_query_mode_ingest_writes_only_object_query_columns(self):
        captured: dict[str, object] = {}

        class _Response:
            status = 200

            @staticmethod
            def read() -> bytes:
                return b""

        class _Connection:
            def __init__(self, _host, _port, timeout):
                captured["timeout"] = timeout

            def putrequest(self, _method, target):
                captured["target"] = target

            def putheader(self, *_args):
                return None

            def endheaders(self):
                return None

            def send(self, payload):
                captured["payload"] = payload

            def getresponse(self):
                return _Response()

            def close(self):
                return None

        config = ClickHouseConfig(
            enabled=True,
            serving_enabled=False,
            host="clickhouse",
            port=8123,
            database="insight",
            table="events_query_oss",
            user="test",
            password="test",
            hot_hours=27,
            serving_hours=25,
            reconcile_seconds=30,
            idle_seconds=1,
            health_url="",
            health_host_header="",
            health_max_seconds=1,
            min_free_gb=20,
            io_pressure_max_full_avg10=10,
            query_table="events_query_oss",
            name_query_table="events_query_by_name_oss",
            ingest_mode="query",
        )
        with patch(
            "app.clickhouse_client.http.client.HTTPConnection", _Connection
        ):
            ClickHouseClient(config).insert_parquet_stream(
                io.BytesIO(b"PAR1"),
                content_length=4,
                part_key="part-a",
                sha256="a" * 64,
                content_revision=1,
            )

        query = parse_qs(urlsplit(str(captured["target"])).query)["query"][0]
        destination = query.split("SELECT", 1)[0]
        self.assertIn("INSERT INTO insight.events_query_oss", destination)
        self.assertIn("_source_part_sha256", destination)
        self.assertNotIn("source_file_id", destination)
        self.assertNotIn("columns_json", destination)

    def test_json_row_insert_keeps_data_in_post_body(self):
        captured: dict[str, object] = {}

        class _Response:
            status = 200

            @staticmethod
            def read() -> bytes:
                return b""

        class _Connection:
            def __init__(self, _host, _port, timeout):
                captured["timeout"] = timeout

            def request(self, method, target, body, headers):
                captured.update(
                    {"method": method, "target": target, "body": body, "headers": headers}
                )

            def getresponse(self):
                return _Response()

            def close(self):
                return None

        config = ClickHouseConfig(
            enabled=True,
            serving_enabled=False,
            host="clickhouse",
            port=8123,
            database="insight",
            table="events",
            user="test",
            password="test",
            hot_hours=27,
            serving_hours=25,
            reconcile_seconds=30,
            idle_seconds=1,
            health_url="",
            health_host_header="",
            health_max_seconds=1,
            min_free_gb=20,
            io_pressure_max_full_avg10=10,
        )
        rows = [
            {"part_path": "/data/a.parquet", "change_version": 1},
            {"part_path": "/data/b.parquet", "change_version": 2},
        ]
        with patch(
            "app.clickhouse_client.http.client.HTTPConnection", _Connection
        ):
            ClickHouseClient(config).insert_json_rows(
                "insight.oss_active_parts_v1", rows
            )

        self.assertEqual(captured["method"], "POST")
        self.assertIn("INSERT+INTO+insight.oss_active_parts_v1", captured["target"])
        self.assertNotIn("%2Fdata%2Fa.parquet", captured["target"])
        self.assertIn(b'"part_path":"/data/a.parquet"', captured["body"])
        self.assertNotIn(b"test", captured["body"])

    def test_compose_mounts_protected_credentials_without_embedding_values(self):
        root = Path(__file__).resolve().parents[1]
        compose = (root / "compose.yaml").read_text(encoding="utf-8")
        entrypoint = (root / "clickhouse" / "oss-entrypoint.sh").read_text(
            encoding="utf-8"
        )

        self.assertIn(
            'entrypoint: ["sh", "/opt/sql-insight/oss-entrypoint.sh"]',
            compose,
        )
        self.assertIn("clickhouse_oss_credentials:", compose)
        self.assertIn("RDS_BINLOG_OSS_CREDENTIAL_FILE", compose)
        self.assertIn("RDS_BINLOG_CLICKHOUSE_OSS_QUERY_TABLE", compose)
        self.assertIn("RDS_BINLOG_CLICKHOUSE_OSS_NAME_QUERY_TABLE", compose)
        self.assertIn("RDS_BINLOG_CLICKHOUSE_OSS_MANIFEST", compose)
        self.assertIn("AWS_ACCESS_KEY_ID", entrypoint)
        self.assertIn("exec /entrypoint.sh", entrypoint)
        self.assertNotIn("set -x", entrypoint)
        self.assertNotIn("access_key_id=", compose.lower())

    def test_object_schema_uses_separate_remote_prefixes_and_bounded_cache(self):
        settings = Settings(
            oss_enabled=True,
            oss_bucket="example-binlog-bucket",
            oss_region_id="cn-hangzhou",
            oss_endpoint="oss-cn-hangzhou-internal.aliyuncs.com",
            oss_prefix="mysql-binlog/rm-test/",
        )
        with patch.dict(
            os.environ,
            {
                "RDS_BINLOG_CLICKHOUSE_OSS_ENABLED": "1",
                "RDS_BINLOG_CLICKHOUSE_OSS_PREFIX": "sql-insight/v1/",
                "RDS_BINLOG_CLICKHOUSE_OSS_CACHE_GB": "40",
                "RDS_BINLOG_CLICKHOUSE_OSS_HISTORY_DAYS": "61",
            },
            clear=False,
        ):
            config = ClickHouseOssConfig.from_env()

        schema = build_oss_schema(settings, config, database="insight")

        self.assertTrue(config.enabled)
        self.assertEqual(config.cache_gb, 40)
        self.assertIn(
            "https://example-binlog-bucket.oss-cn-hangzhou-internal.aliyuncs.com/"
            "sql-insight/v1/events-query/",
            schema,
        )
        self.assertIn(
            "https://example-binlog-bucket.oss-cn-hangzhou-internal.aliyuncs.com/"
            "sql-insight/v1/events-query-by-name/",
            schema,
        )
        self.assertIn("type=cache", schema)
        self.assertIn("/var/lib/clickhouse/caches/sql-insight/time/", schema)
        self.assertIn("max_size='20Gi'", schema)
        self.assertIn("metadata_type=local", schema)
        self.assertIn("use_environment_credentials=true", schema)
        self.assertEqual(schema.count("INTERVAL 61 DAY DELETE"), 2)
        self.assertIn("CREATE MATERIALIZED VIEW IF NOT EXISTS", schema)
        self.assertNotIn("access_key", schema.lower())
        self.assertNotIn("secret", schema.lower())

    def test_unbounded_history_scans_from_epoch_and_omits_table_ttl(self):
        settings = Settings(
            oss_enabled=True,
            oss_bucket="example-binlog-bucket",
            oss_region_id="cn-hangzhou",
            oss_endpoint="oss-cn-hangzhou-internal.aliyuncs.com",
            oss_prefix="mysql-binlog/rm-test/",
        )
        with patch.dict(
            os.environ,
            {
                "RDS_BINLOG_CLICKHOUSE_OSS_ENABLED": "1",
                "RDS_BINLOG_CLICKHOUSE_OSS_PREFIX": "sql-insight/v1/",
                "RDS_BINLOG_CLICKHOUSE_OSS_HISTORY_DAYS": "0",
                "RDS_BINLOG_CLICKHOUSE_OSS_BACKFILL_THREADS": "4",
                "RDS_BINLOG_CLICKHOUSE_OSS_BACKFILL_INSERT_THREADS": "2",
                "RDS_BINLOG_CLICKHOUSE_OSS_BACKFILL_WORKERS": "2",
            },
            clear=False,
        ):
            config = ClickHouseOssConfig.from_env()

        schema = build_oss_schema(settings, config, database="insight")

        self.assertEqual(config.history_days, 0)
        self.assertEqual(config.backfill_threads, 4)
        self.assertEqual(config.backfill_insert_threads, 2)
        self.assertEqual(config.backfill_workers, 2)
        self.assertEqual(history_start_epoch_us(datetime(2026, 8, 26, tzinfo=UTC), 0), 0)
        self.assertNotIn("\nTTL ", schema)

    def test_staged_schema_clones_both_tables_without_incremental_mv(self):
        settings = Settings(
            oss_enabled=True,
            oss_bucket="example-binlog-bucket",
            oss_region_id="cn-hangzhou",
            oss_endpoint="oss-cn-hangzhou-internal.aliyuncs.com",
            oss_prefix="mysql-binlog/rm-test/",
        )
        config = ClickHouseOssConfig(
            enabled=True,
            serving_enabled=False,
            prefix="sql-insight/v3/",
            cache_gb=40,
            query_table="events_query_oss_all_v3",
            name_query_table="events_query_by_name_oss_all_v3",
            materialized_view="events_query_oss_all_v3_to_name_mv",
            manifest_name="oss-all-v3-manifest.sqlite3",
            history_days=0,
            staged_backfill_enabled=True,
            incremental_mv_enabled=False,
            stage_query_table="events_query_oss_all_v3_stage",
            stage_name_query_table="events_query_by_name_oss_all_v3_stage",
        )

        schema = build_oss_schema(settings, config, database="insight")

        self.assertIn(
            "CREATE TABLE IF NOT EXISTS insight.events_query_oss_all_v3_stage "
            "AS insight.events_query_oss_all_v3",
            schema,
        )
        self.assertIn(
            "CREATE TABLE IF NOT EXISTS "
            "insight.events_query_by_name_oss_all_v3_stage "
            "AS insight.events_query_by_name_oss_all_v3",
            schema,
        )
        self.assertNotIn("CREATE MATERIALIZED VIEW", schema)
        self.assertEqual(schema.count("PROJECTION source_part_state_v1"), 2)
        self.assertNotIn("ADD PROJECTION", schema)

    def test_schema_rejects_non_aliyun_or_credential_bearing_endpoint(self):
        config = ClickHouseOssConfig(
            enabled=True,
            serving_enabled=False,
            prefix="sql-insight/v1/",
            cache_gb=40,
            query_table="events_query_oss",
            name_query_table="events_query_by_name_oss",
            materialized_view="events_query_oss_to_name_mv",
            manifest_name="oss-manifest.sqlite3",
            history_days=61,
        )
        for endpoint in (
            "https://user:password@example.com",
            "https://oss.example.com/?token=secret",
        ):
            settings = Settings(
                oss_enabled=True,
                oss_bucket="example-binlog-bucket",
                oss_region_id="cn-hangzhou",
                oss_endpoint=endpoint,
                oss_prefix="mysql-binlog/rm-test/",
            )
            with self.subTest(endpoint=endpoint):
                with self.assertRaises(ValueError):
                    build_oss_schema(settings, config, database="insight")

    def test_ranged_pack_members_are_never_sent_to_naked_s3(self):
        parts = [
            {"path": "standalone", "oss_key": "one.parquet", "oss_offset": 0, "oss_length": 0},
            {"path": "first-pack-member", "oss_key": "many.parquet-pack", "oss_offset": 0, "oss_length": 100},
            {"path": "later-pack-member", "oss_key": "many.parquet-pack", "oss_offset": 100, "oss_length": 200},
            {"path": "missing", "oss_key": "", "oss_offset": 0, "oss_length": 0},
        ]

        direct, ranged = split_direct_and_ranged_parts(parts)

        self.assertEqual([part["path"] for part in direct], ["standalone"])
        self.assertEqual(
            [part["path"] for part in ranged],
            ["first-pack-member", "later-pack-member"],
        )

    def test_direct_insert_is_exactly_mapped_and_contains_no_credentials(self):
        settings = Settings(
            oss_enabled=True,
            oss_bucket="example-binlog-bucket",
            oss_region_id="cn-hangzhou",
            oss_endpoint="oss-cn-hangzhou-internal.aliyuncs.com",
            oss_prefix="mysql-binlog/rm-test/",
        )
        parts = [
            {
                "oss_key": "mysql-binlog/rm-test/a.parquet",
                "logical_part_id": "part-a",
                "sha256": "a" * 64,
                "content_revision": 3,
                "row_count": 10,
                "oss_length": 0,
            },
            {
                "oss_key": "mysql-binlog/rm-test/b.parquet",
                "logical_part_id": "part-b",
                "sha256": "b" * 64,
                "content_revision": 4,
                "row_count": 20,
                "oss_length": 0,
            },
        ]

        sql = build_direct_s3_insert_sql(
            settings,
            database="insight",
            table="events_query_oss",
            parts=parts,
        )

        self.assertIn("/{mysql-binlog/rm-test/a.parquet,mysql-binlog/rm-test/b.parquet}", sql)
        self.assertIn("example-binlog-bucket/mysql-binlog/rm-test/a.parquet", sql)
        self.assertIn("part-a", sql)
        self.assertIn("mapContains(part_keys, _path)", sql)
        self.assertIn("input_format_parquet_max_block_size = 1024", sql)
        self.assertIn("input_format_max_block_size_bytes = 33554432", sql)
        self.assertIn("input_format_parquet_enable_row_group_prefetch = 0", sql)
        self.assertIn("max_insert_block_size = 1024", sql)
        self.assertIn("min_insert_block_size_bytes = 8388608", sql)
        self.assertNotIn("access_key", sql.lower())
        self.assertNotIn("secret", sql.lower())

        with self.assertRaises(ValueError):
            build_direct_s3_insert_sql(
                settings,
                database="insight",
                table="events_query_oss",
                parts=[{**parts[0], "oss_length": 1}],
            )


if __name__ == "__main__":
    unittest.main()
