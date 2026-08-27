from __future__ import annotations

import json
import hashlib
import io
import multiprocessing
import os
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
import urllib.request
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import oss2
import pyarrow as pa
import pyarrow.parquet as pq

from app import __version__
from app.analytics_index import AnalyticsIndex
import app.storage as storage_module
from app.checksum import crc64_xz_update
from app.config import (
    APP_VERSION,
    Settings,
    load_slow_log_instances,
    parse_settings_payload,
)
from app.credentials import (
    CloudCredential,
    credential_status,
    delete_credential,
    load_credential,
    save_credential,
)
from app.downloader import DownloadError, DownloadResult, download_file
from app.general_log_collector import GeneralLogCollector, GeneralLogConfig
from app.metadata import MetadataStore
from app.oss_store import OssArchive, OssArchiveError, OssRangeReader
from app.parser_bridge import (
    NativeChecksumResult,
    ParserError,
    parse_ndjson_chunks,
    parse_ndjson_chunks_buffered,
    parse_native_ndjson_chunks,
)
from app.maintenance_status import SUPERVISOR_STATUS_NAME, write_json_status
from app.pipeline import (
    DOWNLOAD_PIPELINE_WORKERS,
    FILE_PIPELINE_WORKERS,
    OSS_ARCHIVE_BACKLOG_PER_FILE,
    OSS_ARCHIVE_WORKERS,
    TRANSFORM_PIPELINE_WORKERS,
    PreparedBinlog,
    PipelineError,
    SyncManager,
    parse_sync_window,
)
from app.rds_api import RdsApiError, RdsRpcClient, RemoteBinlog
from app.server import (
    Application,
    AppHTTPServer,
    RequestHandler,
    _allowed_hosts,
    _host_allowed,
    _origin_allowed,
)
from app.service_supervisor import ServiceSupervisor
from app.slow_log_collector import SlowLogCollector, SlowLogConfig
from app.slowlog_worker import SlowLogQueueWorker, drain_slowlog_queue_once
from app.storage import EventStorage, StorageError


def remote(
    name: str,
    begin: str,
    *,
    end: str | None = None,
    link: str = "https://example.invalid/signed",
    intranet_link: str | None = None,
    host: str = "host-a",
) -> RemoteBinlog:
    return RemoteBinlog(
        log_file_name=name,
        log_begin_utc=begin,
        log_end_utc=end or begin,
        file_size=123,
        checksum_crc64="456",
        download_link=link,
        intranet_download_link=link + "-vpc" if intranet_link is None else intranet_link,
        link_expired_utc="2026-07-27T13:00:00Z",
        remote_status="Completed",
        host_instance_id=host,
    )


def oss_settings(**values) -> Settings:
    defaults = {
        "db_instance_id": "rm-test000001",
        "retention_days": 60,
        "oss_enabled": True,
        "oss_bucket": "example-binlog-bucket",
        "oss_region_id": "cn-hangzhou",
        "oss_endpoint": "oss-cn-hangzhou-internal.aliyuncs.com",
        "oss_prefix": "mysql-binlog/rm-test000001/",
        "oss_auth_mode": "ecs_ram_role",
        "oss_retention_days": 60,
    }
    defaults.update(values)
    return Settings(**defaults)


class FakeArchive:
    def __init__(self):
        self.objects: dict[str, bytes] = {}
        self.uploaded: list[str] = []
        self.downloaded: list[str] = []
        self.ranged: list[str] = []

    def upload_part(self, part, *, fresh=False):
        path = Path(part["path"])
        key = f"fake/{path.name}/{part['sha256']}"
        self.objects[key] = path.read_bytes()
        self.uploaded.append(str(path))
        return {"oss_key": key, "oss_etag": part["sha256"][:16]}

    def download_part(self, part, destination):
        key = str(part["oss_key"])
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(self.objects[key])
        self.downloaded.append(key)
        return destination

    def open_part_reader(self, part):
        key = str(part["oss_key"])
        self.ranged.append(key)

        class Reader(io.BytesIO):
            def __init__(self, payload):
                super().__init__(payload)
                self.requests = 0
                self.byte_count = 0

            def read(self, size=-1):
                payload = super().read(size)
                self.requests += 1
                self.byte_count += len(payload)
                return payload

            def stats(self):
                return {
                    "range_requests": self.requests,
                    "range_bytes": self.byte_count,
                }

        return Reader(self.objects[key])


class ConfigAndChecksumTests(unittest.TestCase):
    def test_package_and_runtime_versions_stay_consistent(self) -> None:
        self.assertEqual(__version__, APP_VERSION)
        index_html = (
            Path(__file__).resolve().parents[1] / "web" / "index.html"
        ).read_text("utf-8")
        self.assertIn(f"/assets/app.css?v={APP_VERSION}", index_html)
        self.assertIn(f"/assets/app.js?v={APP_VERSION}", index_html)

    def test_body_cache_settings_are_removed(self) -> None:
        settings = Settings()
        updated = parse_settings_payload(
            {
                "localCacheBytes": 1024**3,
                "queryCacheBytes": 10 * 1024**3,
            },
            settings,
        )
        self.assertFalse(hasattr(updated, "local_cache_bytes"))
        self.assertFalse(hasattr(updated, "query_cache_bytes"))
        self.assertNotIn("localCacheBytes", updated.public_dict())
        self.assertNotIn("queryCacheBytes", updated.public_dict())

    def test_container_healthcheck_uses_constant_time_liveness_endpoint(self) -> None:
        root = Path(__file__).resolve().parents[1]
        dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
        self.assertIn("/healthz", dockerfile)
        self.assertNotIn("/api/status", dockerfile)

    def test_python_base_image_is_digest_pinned(self) -> None:
        root = Path(__file__).resolve().parents[1]
        dockerfile = (root / "Dockerfile").read_text(encoding="utf-8")
        base = (
            "FROM python:3.12-slim-bookworm@sha256:"
            "b64e9d3a71eddaa1b3f80c04abf292b3139e3b7c4dd272d19c31dc1f91194d1b"
        )
        self.assertEqual(dockerfile.count(base), 2)

    def test_docker_build_context_excludes_clickhouse_runtime_artifacts(self) -> None:
        root = Path(__file__).resolve().parents[1]
        ignored = set(
            (root / ".dockerignore").read_text(encoding="utf-8").splitlines()
        )
        self.assertTrue(
            {
                "clickhouse/data",
                "clickhouse/log",
                "clickhouse/config-validate-*",
                "clickhouse/data.failed-*",
                "clickhouse/log.failed-*",
                "clickhouse/runtime.env",
            }.issubset(ignored)
        )

    def test_liveness_response_never_waits_for_access_logging(self) -> None:
        handler = object.__new__(RequestHandler)
        handler.path = "/healthz?probe=supervisor"
        handler.address_string = lambda: "127.0.0.1"  # type: ignore[method-assign]

        with patch("app.server.LOGGER.info") as log_info:
            handler.log_message('"%s" %s', "GET /healthz", 200)
            log_info.assert_not_called()

            handler.path = "/api/status"
            handler.log_message('"%s" %s', "GET /api/status", 200)
            log_info.assert_called_once()

    def test_error_response_ignores_a_disconnected_client(self) -> None:
        handler = object.__new__(RequestHandler)
        with patch.object(
            handler,
            "_json",
            side_effect=BrokenPipeError("client disconnected"),
        ):
            handler._error(500, "INTERNAL_ERROR", "failed")

    def test_client_disconnect_does_not_emit_a_second_http_error(self) -> None:
        handler = object.__new__(RequestHandler)
        handler.path = "/healthz"
        handler._valid_host = lambda: True  # type: ignore[method-assign]
        handler._json = lambda *_args, **_kwargs: (_ for _ in ()).throw(  # type: ignore[method-assign]
            BrokenPipeError("client closed")
        )

        with (
            patch.object(handler, "_error") as send_error,
            patch("app.server.LOGGER.exception") as log_exception,
        ):
            handler.do_GET()

        send_error.assert_not_called()
        log_exception.assert_not_called()

    def test_query_scan_parallelism_is_bounded_for_the_5_gib_service(self) -> None:
        root = Path(__file__).resolve().parents[1]
        compose = (root / "compose.yaml").read_text(encoding="utf-8")

        self.assertLessEqual(storage_module.QUERY_SCAN_WORKERS, 4)
        self.assertIn("RDS_BINLOG_QUERY_SCAN_WORKERS", compose)

    def test_container_runs_server_under_an_external_health_supervisor(self) -> None:
        root = Path(__file__).resolve().parents[1]
        compose = (root / "compose.yaml").read_text(encoding="utf-8")
        supervisor = (root / "app" / "service_supervisor.py")
        self.assertTrue(supervisor.is_file())
        self.assertIn("app.service_supervisor", compose)
        self.assertIn("RDS_BINLOG_SERVICE_PROBE_TIMEOUT", compose)
        self.assertIn("RDS_BINLOG_SERVICE_FAILURE_LIMIT", compose)

    def test_all_services_use_bounded_nonblocking_container_logging(self) -> None:
        compose = (
            Path(__file__).resolve().parents[1] / "compose.yaml"
        ).read_text(encoding="utf-8")

        self.assertIn("driver: local", compose)
        self.assertIn("mode: non-blocking", compose)
        self.assertIn('max-buffer-size: "4m"', compose)
        self.assertEqual(
            compose.count("logging: *runtime-logging"),
            compose.count("restart: unless-stopped"),
        )

    def test_browser_status_polling_is_single_flight(self) -> None:
        source = (
            Path(__file__).resolve().parents[1] / "web" / "app.js"
        ).read_text(encoding="utf-8")
        self.assertIn("statusRefreshInFlight", source)
        self.assertIn("if (state.statusRefreshInFlight) return", source)
        self.assertIn("state.statusRefreshInFlight = false", source)

    def test_sync_page_renders_full_chain_speed_and_catch_up_eta_states(
        self,
    ) -> None:
        root = Path(__file__).resolve().parents[1]
        html = (root / "web" / "index.html").read_text(encoding="utf-8")
        script = (root / "web" / "app.js").read_text(encoding="utf-8")
        stylesheet = (root / "web" / "app.css").read_text(encoding="utf-8")
        self.assertIn('id="active-job-speed"', html)
        self.assertIn('id="active-job-eta"', html)
        self.assertIn("全链路速度", html)
        self.assertIn("预计追平时间", html)
        self.assertIn('id="app-version"', html)
        self.assertNotIn("v1.8.5 · 1 CPU", html)
        self.assertIn("data.version", script)
        self.assertIn("function renderSyncPerformance", script)
        for state in (
            "warming_up",
            "available",
            "not_catching_up",
            "checking_latest",
            "live_following",
            "caught_up",
        ):
            self.assertIn(state, script)
        self.assertIn("等待新的 Completed Binlog", script)
        self.assertIn("正在处理最新 Binlog", script)
        self.assertIn("active-job-metrics", stylesheet)

    def test_parquet_hot_path_uses_fastest_zstd_level(self) -> None:
        storage = (
            Path(__file__).resolve().parents[1] / "app" / "storage.py"
        ).read_text(encoding="utf-8")
        self.assertEqual(storage.count("COMPRESSION_LEVEL 1"), 2)
        self.assertNotIn("COMPRESSION_LEVEL 9", storage)

    def test_parser_staging_uses_a_dedicated_bounded_tmpfs(self) -> None:
        compose = (
            Path(__file__).resolve().parents[1] / "compose.yaml"
        ).read_text(encoding="utf-8")
        insight, indexer = compose.split("  indexer:\n", maxsplit=1)
        self.assertIn("- /data/staging:size=1g,mode=1777", insight)
        self.assertNotIn("/data/staging:size=", indexer)

    def test_deployer_targets_actual_previous_version_and_reasserts_pause(
        self,
    ) -> None:
        script = (
            Path(__file__).resolve().parents[1] / "deploy" / "deploy-v1.8.6.py"
        ).read_text(encoding="utf-8")
        self.assertIn('CURRENT_VERSION = "1.8.5"', script)
        self.assertIn("wait_health(CURRENT_VERSION)", script)
        self.assertNotIn('wait_health("1.5.0")', script)
        self.assertIn('current["sync"].get("pauseRequested")', script)
        self.assertIn('"unhealthyRecovery": True', script)
        self.assertIn("parallel pipeline status mismatch", script)
        self.assertIn("one-CPU limit was not applied", script)
        self.assertIn("persistent spill directory was not applied", script)
        self.assertIn("bounded staging path was not applied", script)
        self.assertIn("staging_tmpfs_bytes != 1024**3", script)
        self.assertIn("os.statvfs('/data/staging')", script)
        self.assertNotIn('inspect.get("Mounts")', script)
        self.assertIn("Python per-event parser bridge", script)
        self.assertIn("candidate parser staging bound probe failed", script)
        self.assertIn("except BrokenPipeError:", script)
        self.assertIn("atomic result file is authoritative", script.lower())
        self.assertIn("fast ZSTD/direct OSS or body-version gate", script)
        self.assertIn("candidate body lock probe failed", script)
        self.assertIn("candidate app version probe failed", script)
        self.assertIn("candidate package version probe failed", script)
        self.assertIn("candidate compose image mismatch", script)
        self.assertIn("candidate container image mismatch", script)
        self.assertIn("main service supervisor was not applied", script)
        self.assertIn('"stop", "--time", "30", "rds-binlog-insight-indexer"', script)
        self.assertIn('"stop", "--time", "30", "rds-binlog-insight"', script)
        self.assertIn('["docker", "rm", "rds-binlog-insight-indexer"]', script)
        self.assertIn('["docker", "rm", "rds-binlog-insight"]', script)
        self.assertIn('inspect["Image"] != image_id', script)

    def test_oss_access_key_mode_is_supported_without_exposing_secret(self) -> None:
        settings = oss_settings(oss_auth_mode="access_key")
        settings.validate(require_identity=True)
        public = settings.public_dict()
        self.assertEqual(public["ossAuthMode"], "access_key")
        self.assertNotIn("accessKeyId", public)
        self.assertNotIn("accessKeySecret", public)

    def test_project_id_is_not_required_or_exposed(self) -> None:
        settings = Settings(db_instance_id="rm-test000001")
        settings.validate(require_identity=True)
        self.assertNotIn("projectId", settings.public_dict())
        self.assertFalse(hasattr(settings, "project_id"))

        updated = parse_settings_payload(
            {
                "projectId": "must-be-ignored",
                "dbInstanceId": "rm-test000001",
            },
            Settings(),
        )
        self.assertEqual(updated.db_instance_id, "rm-test000001")
        self.assertFalse(hasattr(updated, "project_id"))

        root = Path(__file__).resolve().parents[1]
        for relative in ("web/index.html", "web/app.js", "README.md"):
            text = (root / relative).read_text(encoding="utf-8")
            self.assertNotIn("projectId", text, relative)

    def test_oss_crc64_vector_and_incremental_update(self) -> None:
        expected = 0x995DC9BBDF1939FA
        self.assertEqual(crc64_xz_update(b"123456789"), expected)
        self.assertEqual(
            crc64_xz_update(b"6789", crc64_xz_update(b"12345")),
            expected,
        )

    def test_boolean_payload_is_not_truthy_string_coercion(self) -> None:
        current = Settings()
        updated = parse_settings_payload(
            {"autoSync": "false", "preferIntranetDownload": "1"},
            current,
        )
        self.assertFalse(updated.auto_sync)
        self.assertTrue(updated.prefer_intranet_download)
        with self.assertRaises(ValueError):
            parse_settings_payload({"autoSync": "not-a-boolean"}, current)

    def test_intranet_download_cannot_be_disabled_by_legacy_settings(self) -> None:
        self.assertTrue(Settings().prefer_intranet_download)
        self.assertTrue(
            Settings.from_mapping(
                {"prefer_intranet_download": False}
            ).prefer_intranet_download
        )
        self.assertTrue(
            parse_settings_payload(
                {"preferIntranetDownload": False},
                Settings(),
            ).prefer_intranet_download
        )

    def test_sync_window_requires_two_aware_ordered_timestamps(self) -> None:
        start, end = parse_sync_window(
            "2026-07-27T01:00:00Z",
            "2026-07-27T02:00:00+00:00",
        )
        self.assertEqual(start, datetime(2026, 7, 27, 1, tzinfo=UTC))
        self.assertEqual(end, datetime(2026, 7, 27, 2, tzinfo=UTC))

        invalid = (
            ("2026-07-27T01:00:00Z", ""),
            ("", "2026-07-27T02:00:00Z"),
            ("2026-07-27T01:00:00", "2026-07-27T02:00:00Z"),
            ("2026-07-27T02:00:00Z", "2026-07-27T01:00:00Z"),
        )
        for values in invalid:
            with self.subTest(values=values), self.assertRaises(PipelineError):
                parse_sync_window(*values)

    def test_oss_settings_are_normalized_and_exposed_without_credentials(self) -> None:
        settings = parse_settings_payload(
            {
                "ossEnabled": True,
                "ossBucket": "example-binlog-bucket",
                "ossRegionId": "cn-hangzhou",
                "ossEndpoint": "oss-cn-hangzhou-internal.aliyuncs.com",
                "ossPrefix": "/mysql-binlog/rm-test000001",
                "ossAuthMode": "ecs_ram_role",
                "ossRetentionDays": 60,
            },
            Settings(),
        )
        settings.validate()
        public = settings.public_dict()
        self.assertEqual(
            public["ossPrefix"],
            "mysql-binlog/rm-test000001/",
        )
        self.assertEqual(public["ossRetentionDays"], 60)
        self.assertNotIn("localCacheBytes", public)
        self.assertNotIn("accessKey", json.dumps(public))

        with self.assertRaisesRegex(ValueError, "内网 Endpoint"):
            parse_settings_payload(
                {
                    "ossEnabled": True,
                    "ossBucket": "example-binlog-bucket",
                    "ossRegionId": "cn-hangzhou",
                    "ossEndpoint": "oss-cn-hangzhou.aliyuncs.com",
                    "ossPrefix": "mysql-binlog/rm-test000001/",
                },
                Settings(),
            )


class HostValidationTests(unittest.TestCase):
    def test_public_host_requires_explicit_allowlist(self) -> None:
        local_only = _allowed_hosts("")
        self.assertTrue(_host_allowed("127.0.0.1:8769", local_only))
        self.assertFalse(_host_allowed("192.0.2.10:8769", local_only))

        public = _allowed_hosts("192.0.2.10")
        self.assertTrue(_host_allowed("192.0.2.10:8769", public))
        self.assertTrue(
            _origin_allowed("http://192.0.2.10:8769", public, 8769)
        )

    def test_neighbor_hosts_and_origins_stay_blocked(self) -> None:
        allowed = _allowed_hosts("192.0.2.10")
        self.assertFalse(_host_allowed("example.invalid:8769", allowed))
        self.assertFalse(
            _origin_allowed("http://192.0.2.10:8770", allowed, 8769)
        )
        self.assertFalse(
            _origin_allowed("https://192.0.2.10:8769", allowed, 8769)
        )


class CredentialBackendTests(unittest.TestCase):
    def test_linux_file_backend_round_trip_and_delete(self) -> None:
        target = "RDS-Binlog-Insight/default"
        credential = CloudCredential(
            "test-access-key-id",
            "not-a-real-secret",
            "test-token",
        )
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("app.credentials._uses_windows_credential_manager", return_value=False),
            patch.dict(
                os.environ,
                {
                    "RDS_BINLOG_CREDENTIAL_DIR": directory,
                    "ALIBABA_CLOUD_ACCESS_KEY_ID": "",
                    "ALIBABA_CLOUD_ACCESS_KEY_SECRET": "",
                    "ALIBABA_CLOUD_SECURITY_TOKEN": "",
                },
                clear=False,
            ),
        ):
            save_credential(target, credential)

            self.assertEqual(load_credential(target), credential)
            status = credential_status(target)
            self.assertTrue(status["present"])
            self.assertEqual(status["source"], "protected-file")
            self.assertNotIn(credential.access_key_secret, json.dumps(status))

            files = list(Path(directory).glob("*.json"))
            self.assertEqual(len(files), 1)
            self.assertNotIn(credential.access_key_id, files[0].name)
            self.assertFalse(list(Path(directory).glob("*.tmp")))

            self.assertTrue(delete_credential(target))
            self.assertIsNone(load_credential(target))
            self.assertFalse(delete_credential(target))

    def test_environment_credentials_keep_priority(self) -> None:
        target = "RDS-Binlog-Insight/default"
        with (
            tempfile.TemporaryDirectory() as directory,
            patch("app.credentials._uses_windows_credential_manager", return_value=False),
            patch.dict(
                os.environ,
                {
                    "RDS_BINLOG_CREDENTIAL_DIR": directory,
                    "ALIBABA_CLOUD_ACCESS_KEY_ID": "",
                    "ALIBABA_CLOUD_ACCESS_KEY_SECRET": "",
                    "ALIBABA_CLOUD_SECURITY_TOKEN": "",
                },
                clear=False,
            ),
        ):
            save_credential(target, CloudCredential("file-id", "file-secret"))
            with patch.dict(
                os.environ,
                {
                    "ALIBABA_CLOUD_ACCESS_KEY_ID": "environment-id",
                    "ALIBABA_CLOUD_ACCESS_KEY_SECRET": "environment-secret",
                    "ALIBABA_CLOUD_SECURITY_TOKEN": "",
                },
                clear=False,
            ):
                loaded = load_credential(target)
                self.assertIsNotNone(loaded)
                self.assertEqual(loaded.access_key_id, "environment-id")
                self.assertEqual(
                    credential_status(target)["source"],
                    "environment",
                )


class RdsClientTests(unittest.TestCase):
    def test_download_url_never_falls_back_to_public(self) -> None:
        public_only = remote(
            "mysql-bin.000001",
            "2026-07-27T01:00:00Z",
            intranet_link="",
        )
        with self.assertRaises(RdsApiError) as raised:
            public_only.selected_url(True)
        self.assertEqual(raised.exception.code, "INTRANET_DOWNLOAD_LINK_MISSING")

        with_intranet = remote(
            "mysql-bin.000002",
            "2026-07-27T02:00:00Z",
        )
        self.assertEqual(
            with_intranet.selected_url(False),
            "https://example.invalid/signed-vpc",
        )

    def test_binlog_pages_are_sorted_stably(self) -> None:
        settings = Settings(
            db_instance_id="rm-test000001",
            page_size=30,
        )

        class FakeClient(RdsRpcClient):
            def call(self, action, params):
                self.assert_action = action
                if params["PageNumber"] == 1:
                    return {
                        "TotalRecordCount": 2,
                        "Items": {
                            "BinLogFile": [
                                {
                                    "LogFileName": "mysql-bin.000002",
                                    "LogBeginTime": "2026-07-27T02:00:00Z",
                                    "LogEndTime": "2026-07-27T03:00:00Z",
                                    "FileSize": 2,
                                    "Checksum": "2",
                                    "DownloadLink": "https://example.invalid/2",
                                    "RemoteStatus": "Completed",
                                }
                            ]
                        },
                    }
                return {
                    "TotalRecordCount": 2,
                    "Items": {
                        "BinLogFile": [
                            {
                                "LogFileName": "mysql-bin.000001",
                                "LogBeginTime": "2026-07-27T01:00:00Z",
                                "LogEndTime": "2026-07-27T02:00:00Z",
                                "FileSize": 1,
                                "Checksum": "1",
                                "DownloadLink": "https://example.invalid/1",
                                "RemoteStatus": "Completed",
                            }
                        ]
                    },
                }

        client = FakeClient(settings, CloudCredential("test-id", "test-secret"))
        items = client.list_binlogs(
            "2026-07-27T00:00:00Z", "2026-07-27T04:00:00Z"
        )
        self.assertEqual(
            [item.log_file_name for item in items],
            ["mysql-bin.000001", "mysql-bin.000002"],
        )
        self.assertEqual(client.assert_action, "DescribeBinlogFiles")

    def test_primary_host_is_resolved_from_ha_config(self) -> None:
        settings = Settings(db_instance_id="rm-test000001")

        class FakeClient(RdsRpcClient):
            def call(self, action, params):
                self.assert_action = action
                if params["DBInstanceId"] != settings.db_instance_id:
                    raise AssertionError(params)
                return {
                    "HostInstanceInfos": {
                        "NodeInfo": [
                            {"NodeId": "host-b", "NodeType": "Slave"},
                            {"NodeId": "host-a", "NodeType": "Master"},
                        ]
                    }
                }

        client = FakeClient(settings, CloudCredential("test-id", "test-secret"))
        self.assertEqual(client.primary_host_instance_id(), "host-a")
        self.assertEqual(client.assert_action, "DescribeDBInstanceHAConfig")


class MetadataSecurityTests(unittest.TestCase):
    def test_visibility_migration_hides_interrupted_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metadata.sqlite3"
            store = MetadataStore(path)
            settings = Settings(db_instance_id="rm-test000001")
            done = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            interrupted = remote(
                "mysql-bin.000002",
                "2026-07-29T01:05:00Z",
            )
            done_id, _ = store.upsert_remote(settings, done)
            interrupted_id, _ = store.upsert_remote(settings, interrupted)
            store.set_file_state(done_id, "done")
            store.set_file_state(interrupted_id, "parsing")
            with store.connection() as conn:
                conn.execute("DROP INDEX idx_binlog_visibility")
                conn.execute("DROP INDEX idx_binlog_slowlog_source")
                conn.execute("DROP INDEX idx_binlog_visibility_id")
                conn.execute(
                    "ALTER TABLE binlog_files DROP COLUMN query_visible"
                )
                conn.execute("PRAGMA user_version = 0")

            migrated = MetadataStore(path)
            self.assertEqual(migrated.file_record(done_id)["query_visible"], 1)
            self.assertEqual(
                migrated.file_record(interrupted_id)["query_visible"],
                0,
            )

    def test_download_hashes_bytes_inline_without_second_file_pass(self) -> None:
        payload = b"streamed-binlog-payload"
        updates: list[bytes] = []

        class Response(io.BytesIO):
            status = 200

            def getcode(self):
                return self.status

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                self.close()

        class ChecksumStream:
            def update(self, value):
                updates.append(bytes(value))

            def finish(self):
                data = b"".join(updates)
                return NativeChecksumResult(
                    len(data),
                    hashlib.sha256(data).hexdigest(),
                    str(crc64_xz_update(data)),
                )

            def abort(self):
                return None

        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / "streamed.binlog"
            with (
                patch(
                    "app.downloader.urllib.request.urlopen",
                    return_value=Response(payload),
                ),
                patch("app.downloader.NativeChecksumStream", ChecksumStream),
                patch(
                    "app.downloader.verify_file",
                    side_effect=AssertionError("unexpected second checksum pass"),
                ),
            ):
                result = download_file(
                    "https://vpc.example.invalid/binlog",
                    destination,
                    expected_size=len(payload),
                    expected_crc64=str(crc64_xz_update(payload)),
                )
            self.assertEqual(b"".join(updates), payload)
            self.assertEqual(result.sha256, hashlib.sha256(payload).hexdigest())
            self.assertEqual(destination.read_bytes(), payload)

    def test_http_404_refreshes_stale_intranet_link_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory)
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            settings = Settings(
                db_instance_id="rm-test000001",
                retention_days=60,
                initial_lookback_days=60,
            )
            stale = remote(
                "mysql-bin.000001",
                "2026-07-22T10:00:00Z",
                intranet_link="https://stale-vpc.example.invalid/signed",
            )
            refreshed = remote(
                "mysql-bin.000001",
                "2026-07-22T10:00:00Z",
                intranet_link="https://fresh-vpc.example.invalid/signed",
            )
            file_id, _ = store.upsert_remote(settings, stale)
            result_path = storage.paths["downloads"] / f"{file_id}.binlog"
            manager = SyncManager(store, storage)
            client = SimpleNamespace()
            try:
                with (
                    patch(
                        "app.pipeline.download_file",
                        side_effect=[
                            DownloadError("signed URL expired", "HTTP_404"),
                            DownloadResult(result_path, 123, "sha256", "456"),
                        ],
                    ) as mocked_download,
                    patch.object(
                        manager, "_refresh_item", return_value=refreshed
                    ) as mocked_refresh,
                    patch.object(manager, "_event") as mocked_event,
                ):
                    result = manager._download(
                        "job-test", client, settings, file_id, stale
                    )
            finally:
                manager.shutdown()

            self.assertEqual(result, (result_path, "sha256"))
            self.assertEqual(mocked_download.call_count, 2)
            self.assertEqual(
                mocked_download.call_args_list[0].args[0],
                "https://stale-vpc.example.invalid/signed",
            )
            self.assertEqual(
                mocked_download.call_args_list[1].args[0],
                "https://fresh-vpc.example.invalid/signed",
            )
            mocked_refresh.assert_called_once_with(client, settings, stale)
            mocked_event.assert_any_call(
                "job-test",
                "warning",
                "DOWNLOAD_LINK_REFRESH",
                "mysql-bin.000001 下载链接不可用，刷新一次",
            )
            self.assertEqual(mocked_event.call_count, 2)
            self.assertEqual(
                mocked_event.call_args_list[1].args[:3],
                ("job-test", "info", "FILE_DOWNLOADED"),
            )

    def test_expired_partial_is_preserved_without_blocking_later_binlog(self) -> None:
        class Client:
            @staticmethod
            def verify_instance():
                return {
                    "dbInstanceId": "rm-test000001",
                    "engine": "MySQL",
                    "engineVersion": "8.0",
                }

            @staticmethod
            def primary_host_instance_id():
                return "host-a"

            @staticmethod
            def list_binlogs(_start_utc, _end_utc):
                return [later]

        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory)
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            settings = Settings(
                db_instance_id="rm-test000001",
                retention_days=60,
                initial_lookback_days=60,
            )
            expired = remote(
                "mysql-bin.000001",
                "2026-07-22T10:00:00Z",
                end="2026-07-22T10:03:00Z",
            )
            later = remote(
                "mysql-bin.000002",
                "2026-07-22T10:03:00Z",
                end="2026-07-22T10:06:00Z",
            )
            expired_id, _ = store.upsert_remote(settings, expired)
            store.set_file_state(expired_id, "failed")
            partial = storage.paths["downloads"] / f"{expired_id}.binlog.part"
            partial.write_bytes(b"preserve-me")
            job_id = store.create_job("sync", settings.db_instance_id)
            processed: list[str] = []

            manager = SyncManager(
                store,
                storage,
                client_factory=lambda _settings, _credential: Client(),
            )

            def process_one(
                _job_id,
                _client,
                _settings,
                file_id,
                item,
                _prior_state,
                _flavor,
                _archive,
                *,
                prepared_download=None,
                defer_commit=False,
                query_visible_event=None,
                archive_submitter=None,
                transform_submitter=None,
            ):
                if item.log_file_name == expired.log_file_name:
                    raise DownloadError(
                        "刷新下载链接后未找到 mysql-bin.000001",
                        "DOWNLOAD_LINK_REFRESH_MISSING",
                    )
                processed.append(item.log_file_name)
                return PreparedBinlog(
                    file_id=file_id,
                    item=item,
                    raw_path=prepared_download[0],
                    event_count=0,
                    parse_seconds=0.01,
                )

            def commit(_job_id, _settings, prepared):
                store.set_file_state(
                    prepared.file_id,
                    "done",
                    raw_deleted=True,
                )

            try:
                with (
                    patch.object(
                        manager,
                        "_process_one",
                        side_effect=process_one,
                    ),
                    patch.object(
                        manager,
                        "_commit_prepared",
                        side_effect=commit,
                    ),
                    patch.object(
                        manager,
                        "_download",
                        side_effect=lambda *_args: (
                            storage.paths["downloads"] / "prefetched.binlog",
                            "sha256",
                        ),
                    ),
                ):
                    manager._run(
                        job_id,
                        settings,
                        CloudCredential("test-id", "test-secret"),
                    )
            finally:
                manager.shutdown()

            expired_record = store.file_record(expired_id)
            job = store.jobs()[0]
            self.assertEqual(processed, ["mysql-bin.000002"])
            self.assertTrue(partial.is_file())
            self.assertEqual(partial.read_bytes(), b"preserve-me")
            self.assertEqual(expired_record["state"], "unavailable")
            self.assertEqual(
                expired_record["error_code"], "INSTANCE_BINLOG_NOT_FOUND"
            )
            self.assertEqual(job["status"], "warning")
            self.assertEqual(job["error_code"], "INSTANCE_BINLOG_NOT_FOUND")
            self.assertEqual(job["completed_files"], 1)
            self.assertEqual(job["failed_files"], 1)
            self.assertTrue(
                any(
                    event["code"] == "INSTANCE_BINLOG_NOT_FOUND"
                    and "实例未找到 2026-07-22T10:00:00Z 至 "
                    "2026-07-22T10:03:00Z 范围的 Binlog"
                    in event["message"]
                    for event in job["events"]
                )
            )

    def test_partial_download_is_recovered_before_later_remote_file(self) -> None:
        class Client:
            def __init__(self, items):
                self.items = items

            def list_binlogs(self, _start, _end):
                return list(self.items)

        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory)
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            settings = Settings(
                db_instance_id="rm-test000001",
                retention_days=60,
                initial_lookback_days=60,
            )
            older = remote("mysql-bin.000001", "2026-07-22T10:00:00Z")
            later = remote("mysql-bin.000002", "2026-07-22T11:00:00Z")
            older_id, _ = store.upsert_remote(settings, older)
            store.set_file_state(older_id, "downloading")
            partial = storage.paths["downloads"] / f"{older_id}.binlog.part"
            partial.write_bytes(b"partial")

            manager = SyncManager(store, storage)
            try:
                pending = manager._discover(
                    Client([later]),
                    settings,
                    primary_host_instance_id="host-a",
                )
            finally:
                manager.shutdown()

            self.assertEqual(
                [entry[1].log_file_name for entry in pending],
                ["mysql-bin.000001", "mysql-bin.000002"],
            )

    def test_legacy_project_setting_is_ignored_and_not_exposed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = MetadataStore(Path(directory) / "metadata.sqlite3")
            with store.connection() as conn:
                conn.execute(
                    "UPDATE app_settings SET value_json = ? WHERE singleton = 1",
                    (
                        '{"project_id":"legacy-project","region_id":"cn-hangzhou",'
                        '"db_instance_id":"rm-test000001"}',
                    ),
                )

            settings = store.load_settings()
            self.assertEqual(settings.db_instance_id, "rm-test000001")
            self.assertFalse(hasattr(settings, "project_id"))

            job_id = store.create_job("sync", settings.db_instance_id, "compat")
            jobs = store.jobs()
            self.assertEqual(jobs[0]["id"], job_id)
            self.assertNotIn("project_id", jobs[0])

    def test_job_records_requested_sync_window(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = MetadataStore(Path(directory) / "metadata.sqlite3")
            job_id = store.create_job(
                "sync",
                "rm-test000001",
                "range",
                requested_start_utc="2026-07-27T01:00:00Z",
                requested_end_utc="2026-07-27T02:00:00Z",
            )
            job = store.jobs()[0]
            self.assertEqual(job["id"], job_id)
            self.assertEqual(job["requested_start_utc"], "2026-07-27T01:00:00Z")
            self.assertEqual(job["requested_end_utc"], "2026-07-27T02:00:00Z")

    def test_jobs_loads_recent_events_in_one_bounded_query(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = MetadataStore(Path(directory) / "metadata.sqlite3")
            job_ids = [
                store.create_job("sync", "rm-test000001", f"job-{index}")
                for index in range(3)
            ]
            for job_id in job_ids:
                for event_index in range(25):
                    store.add_job_event(
                        job_id,
                        "info",
                        f"EVENT_{event_index:02d}",
                        f"event {event_index}",
                    )

            statements: list[str] = []
            original_connection = store.connection

            @contextmanager
            def traced_connection():
                with original_connection() as connection:
                    connection.set_trace_callback(statements.append)
                    yield connection

            with patch.object(store, "connection", traced_connection):
                jobs = store.jobs(limit=3)

            event_reads = [
                statement
                for statement in statements
                if statement.lstrip().upper().startswith("SELECT")
                and "FROM JOB_EVENTS" in statement.upper()
            ]
            self.assertEqual(len(event_reads), 1)
            self.assertEqual(len(jobs), 3)
            for job in jobs:
                self.assertEqual(len(job["events"]), 20)
                self.assertEqual(job["events"][0]["code"], "EVENT_05")
                self.assertEqual(job["events"][-1]["code"], "EVENT_24")

    def test_sync_performance_uses_recorded_full_chain_duration_and_confirmed_backlog(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = MetadataStore(Path(directory) / "metadata.sqlite3")
            settings = Settings(db_instance_id="rm-test000001")
            source_start = datetime(2026, 8, 1, 23, 25, tzinfo=UTC)
            completed_start = datetime(2026, 8, 1, 23, 50, tzinfo=UTC)
            for index in range(7):
                begin = source_start + timedelta(minutes=5 * index)
                end = begin + timedelta(minutes=5)
                file_id, _ = store.upsert_remote(
                    settings,
                    remote(
                        f"mysql-bin.{index + 1:06d}",
                        begin.isoformat().replace("+00:00", "Z"),
                        end=end.isoformat().replace("+00:00", "Z"),
                    ),
                )
                if index < 6:
                    store.set_file_state(file_id, "done", raw_deleted=True)
                    # Completion timestamps deliberately include long gaps.  The
                    # speed must come from active full-chain duration, not from
                    # idle time between periodic sync jobs.
                    completed_at = completed_start + timedelta(minutes=10 * index)
                    with store.connection() as conn:
                        conn.execute(
                            "UPDATE binlog_files "
                            "SET completed_at = ?, processing_seconds = ? WHERE id = ?",
                            (
                                completed_at.isoformat().replace("+00:00", "Z"),
                                float(40 + (index % 3) * 10),
                                file_id,
                            ),
                        )

            job_id = store.create_job("sync", settings.db_instance_id)
            store.update_job(job_id, total_files=100, completed_files=20)
            job = store.latest_job()
            self.assertIsNotNone(job)

            performance = store.sync_performance(
                job,
                now=datetime(2026, 8, 2, 0, 5, tzinfo=UTC),
            )

            self.assertEqual(performance["state"], "available")
            self.assertEqual(performance["completion_sample_size"], 6)
            self.assertAlmostEqual(performance["seconds_per_file"], 50.0)
            self.assertAlmostEqual(performance["source_seconds_per_file"], 300.0)
            self.assertAlmostEqual(performance["processing_files_per_hour"], 72.0)
            self.assertAlmostEqual(performance["source_files_per_hour"], 12.0)
            self.assertEqual(performance["known_remaining_files"], 80)
            self.assertAlmostEqual(performance["estimated_unseen_files"], 0.0)
            self.assertAlmostEqual(performance["estimated_backlog_files"], 80.0)
            self.assertAlmostEqual(performance["estimated_remaining_seconds"], 4000.0)
            self.assertEqual(
                performance["estimated_catch_up_at_utc"],
                "2026-08-02T01:11:40Z",
            )

    def test_file_state_records_active_full_chain_duration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = MetadataStore(Path(directory) / "metadata.sqlite3")
            settings = Settings(db_instance_id="rm-test000001")
            file_id, _ = store.upsert_remote(
                settings,
                remote(
                    "mysql-bin.000001",
                    "2026-08-02T00:00:00Z",
                    end="2026-08-02T00:05:00Z",
                ),
            )
            with patch(
                "app.metadata.utc_now_text",
                side_effect=[
                    "2026-08-02T00:10:00Z",
                    "2026-08-02T00:11:15Z",
                ],
            ):
                store.set_file_state(
                    file_id,
                    "downloading",
                    increment_attempt=True,
                )
                store.set_file_state(file_id, "done", raw_deleted=True)
            record = store.file_record(file_id)
            self.assertEqual(
                record["processing_started_at"],
                "2026-08-02T00:10:00Z",
            )
            self.assertAlmostEqual(record["processing_seconds"], 75.0, places=2)

    def test_sync_performance_reports_checking_and_caught_up_states(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = MetadataStore(Path(directory) / "metadata.sqlite3")
            job_id = store.create_job("sync", "rm-test000001")
            store.update_job(job_id, total_files=20, completed_files=1)
            job = store.latest_job()
            self.assertIsNotNone(job)
            warming = store.sync_performance(
                job,
                now=datetime(2026, 8, 2, tzinfo=UTC),
            )
            self.assertEqual(warming["state"], "warming_up")
            self.assertIsNone(warming["seconds_per_file"])

            active_backlog = store.estimate_sync_performance(
                processing_durations=[400.0, 400.0, 400.0, 400.0],
                source_times=[
                    "2026-08-01T23:40:00Z",
                    "2026-08-01T23:45:00Z",
                    "2026-08-01T23:50:00Z",
                    "2026-08-01T23:55:00Z",
                ],
                known_remaining_files=19,
                latest_source_end_utc="2026-08-01T23:55:00Z",
                running=True,
                active_files=1,
                now=datetime(2026, 8, 2, tzinfo=UTC),
            )
            self.assertEqual(active_backlog["state"], "available")
            self.assertEqual(active_backlog["known_remaining_files"], 19)
            self.assertEqual(active_backlog["queued_remaining_files"], 18)
            self.assertAlmostEqual(
                active_backlog["estimated_remaining_seconds"],
                7600.0,
            )

            live_following = store.estimate_sync_performance(
                processing_durations=[50.0, 50.0, 50.0, 50.0],
                source_times=[],
                known_remaining_files=1,
                latest_source_end_utc="",
                running=True,
                active_files=1,
                now=datetime(2026, 8, 2, tzinfo=UTC),
            )
            self.assertEqual(live_following["state"], "live_following")
            self.assertEqual(live_following["known_remaining_files"], 1)
            self.assertEqual(live_following["queued_remaining_files"], 0)
            self.assertIsNone(live_following["estimated_remaining_seconds"])

            discovering = store.estimate_sync_performance(
                processing_durations=[50.0, 50.0, 50.0, 50.0],
                source_times=[
                    "2026-08-01T23:40:00Z",
                    "2026-08-01T23:45:00Z",
                    "2026-08-01T23:50:00Z",
                    "2026-08-01T23:55:00Z",
                ],
                known_remaining_files=0,
                latest_source_end_utc="2026-08-01T23:55:00Z",
                running=True,
                workload_ready=False,
                now=datetime(2026, 8, 2, tzinfo=UTC),
            )
            self.assertEqual(discovering["state"], "checking_latest")
            self.assertEqual(discovering["seconds_per_file"], 50.0)
            self.assertIsNone(discovering["estimated_remaining_seconds"])

            caught_up = store.estimate_sync_performance(
                processing_durations=[50.0, 50.0, 50.0, 50.0],
                source_times=[],
                known_remaining_files=0,
                latest_source_end_utc="",
                running=False,
                now=datetime(2026, 8, 2, tzinfo=UTC),
            )
            self.assertEqual(caught_up["state"], "caught_up")

    def test_sync_status_exposes_latest_job_performance(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory)
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            store.create_job("sync", "rm-test000001")
            manager = SyncManager(store, storage, start_scheduler=False)
            try:
                status = manager.status()
            finally:
                manager.shutdown()
            self.assertIn("performance", status["latestJob"])
            self.assertEqual(
                status["latestJob"]["performance"]["state"],
                "checking_latest",
            )

    def test_pause_blocks_the_auto_scheduler_until_an_explicit_start(self) -> None:
        class TwoPassShutdown:
            def __init__(self) -> None:
                self.calls = 0

            def wait(self, _seconds: float) -> bool:
                self.calls += 1
                return self.calls > 1

        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory)
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            settings = Settings(
                db_instance_id="rm-test000001",
                auto_sync=True,
                poll_minutes=1,
            )
            manager = SyncManager(
                store,
                storage,
                start_scheduler=False,
                settings_loader=lambda: settings,
            )
            original_shutdown = manager._shutdown
            manager._pause_after_current.set()
            manager._shutdown = TwoPassShutdown()  # type: ignore[assignment]
            try:
                with (
                    patch.object(manager, "_run_retention_cleanup_if_due"),
                    patch.object(manager, "start") as mocked_start,
                ):
                    manager._scheduler_loop()
                mocked_start.assert_not_called()
                self.assertTrue(manager.status()["pauseRequested"])
            finally:
                manager._shutdown = original_shutdown
                manager.shutdown()

    def test_completed_file_scrubs_signed_download_capabilities(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = MetadataStore(Path(directory) / "metadata.sqlite3")
            settings = Settings(
                db_instance_id="rm-test000001",
            )
            item = remote("mysql-bin.000001", "2026-07-27T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            store.set_file_state(file_id, "done", raw_deleted=True)
            record = store.file_record(file_id)
            self.assertIsNotNone(record)
            self.assertEqual(record["download_link"], "")
            self.assertEqual(record["intranet_download_link"], "")
            self.assertEqual(record["link_expired_utc"], "")

            # A later retained-window scan must not put the signed URL back.
            store.upsert_remote(
                settings,
                remote(
                    "mysql-bin.000001",
                    "2026-07-27T01:00:00Z",
                    link="https://example.invalid/refreshed",
                ),
            )
            record = store.file_record(file_id)
            self.assertEqual(record["download_link"], "")

    def test_cached_failed_file_survives_remote_retention_window(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = Settings(db_instance_id="rm-test000001")
            item = remote("mysql-bin.000001", "2026-07-20T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            store.set_file_state(file_id, "failed", error_code="PARSER_FAILED")
            storage = EventStorage(store, data_root)
            (storage.paths["downloads"] / f"{file_id}.binlog").write_bytes(b"cached")

            class EmptyClient:
                @staticmethod
                def list_binlogs(_start_utc, _end_utc):
                    return []

            manager = SyncManager(store, storage)
            try:
                pending = manager._discover(EmptyClient(), settings)
            finally:
                manager.shutdown()

            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0][0], file_id)
            self.assertEqual(pending[0][1].log_file_name, item.log_file_name)
            self.assertEqual(pending[0][2], "failed")

    def test_discovery_keeps_only_primary_host(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = Settings(db_instance_id="rm-test000001")
            storage = EventStorage(store, data_root)
            master = remote(
                "mysql-bin.000010",
                "2026-07-27T01:00:00Z",
                host="host-master",
            )
            replica = remote(
                "mysql-bin.000011",
                "2026-07-27T01:00:00Z",
                host="host-replica",
            )

            class MultiNodeClient:
                @staticmethod
                def list_binlogs(_start_utc, _end_utc):
                    return [replica, master]

            manager = SyncManager(store, storage)
            try:
                pending = manager._discover(
                    MultiNodeClient(),
                    settings,
                    primary_host_instance_id="host-master",
                )
            finally:
                manager.shutdown()

            self.assertEqual(len(pending), 1)
            self.assertEqual(pending[0][1].host_instance_id, "host-master")

    def test_discovery_uses_and_enforces_requested_window(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = Settings(db_instance_id="rm-test000001")
            storage = EventStorage(store, data_root)
            before = remote(
                "mysql-bin.000009",
                "2026-07-27T00:00:00Z",
                end="2026-07-27T00:59:59Z",
            )
            overlap = remote(
                "mysql-bin.000010",
                "2026-07-27T00:59:00Z",
                end="2026-07-27T01:30:00Z",
            )
            inside = remote(
                "mysql-bin.000011",
                "2026-07-27T01:30:00Z",
                end="2026-07-27T02:00:00Z",
            )
            after = remote(
                "mysql-bin.000012",
                "2026-07-27T02:00:01Z",
                end="2026-07-27T03:00:00Z",
            )

            class WindowClient:
                calls: list[tuple[str, str]] = []

                @classmethod
                def list_binlogs(cls, start_utc, end_utc):
                    cls.calls.append((start_utc, end_utc))
                    return [before, overlap, inside, after]

            manager = SyncManager(store, storage)
            try:
                pending = manager._discover(
                    WindowClient(),
                    settings,
                    start_utc=datetime(2026, 7, 27, 1, tzinfo=UTC),
                    end_utc=datetime(2026, 7, 27, 2, tzinfo=UTC),
                )
            finally:
                manager.shutdown()

            self.assertEqual(
                WindowClient.calls,
                [("2026-07-27T01:00:00Z", "2026-07-27T02:00:00Z")],
            )
            self.assertEqual(
                [entry[1].log_file_name for entry in pending],
                ["mysql-bin.000010", "mysql-bin.000011"],
            )

    def test_existing_verified_download_does_not_require_live_url(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cached.binlog"
            path.write_bytes(b"cached")
            result = download_file(
                "",
                path,
                expected_size=6,
                expected_crc64="",
            )
            self.assertEqual(result.path, path)
            self.assertEqual(result.size_bytes, 6)

    def test_service_restart_reconciles_running_job(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = MetadataStore(Path(directory) / "metadata.sqlite3")
            job_id = store.create_job("sync", "rm-test000001", "running")
            self.assertEqual(store.reconcile_interrupted_jobs(), 1)
            job = store.latest_job()
            self.assertIsNotNone(job)
            self.assertEqual(job["id"], job_id)
            self.assertEqual(job["status"], "paused")
            self.assertEqual(job["error_code"], "SERVICE_RESTARTED")
            self.assertTrue(job["finished_at"])

    def test_retention_cleanup_runs_hourly_even_when_auto_sync_is_disabled(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = Settings(auto_sync=False, retention_days=30)
            storage = EventStorage(store, data_root)
            manager = SyncManager(store, storage)
            cleanup_result = {
                "deleted_parts": 0,
                "rewritten_parts": 0,
                "removed_rows": 0,
                "errors": [],
            }
            try:
                with patch.object(storage, "cleanup", return_value=cleanup_result) as cleanup:
                    manager._run_retention_cleanup_if_due(settings)
                    manager._run_retention_cleanup_if_due(settings)
                    self.assertEqual(cleanup.call_count, 1)
                    manager._last_retention_cleanup -= 60 * 60 + 1
                    manager._run_retention_cleanup_if_due(settings)
                    self.assertEqual(cleanup.call_count, 2)
            finally:
                manager.shutdown()


class StorageBulkIngestTests(unittest.TestCase):
    def test_metadata_publish_reuses_same_transaction_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = Settings(db_instance_id="rm-test000001")
            item = remote("mysql-bin.000001", "2026-08-24T15:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000)
            ndjson_path = storage.paths["staging"] / "publish-lock.ndjson"
            ndjson_path.write_text(
                json.dumps(
                    {
                        "event_id": "event-publish-lock",
                        "event_epoch_us": epoch_us,
                        "operation": "INSERT",
                        "database_name": "audit_db",
                        "table_name": "orders",
                    },
                    separators=(",", ":"),
                )
                + "\n",
                encoding="utf-8",
            )

            with patch.object(
                store,
                "part_by_path",
                side_effect=sqlite3.OperationalError("database is locked"),
            ) as second_read:
                count, parts = storage.ingest_ndjson_file(
                    file_id=file_id,
                    instance_id=settings.db_instance_id,
                    host_instance_id=item.host_instance_id,
                    source_file_name=item.log_file_name,
                    ndjson_path=ndjson_path,
                    part_key="publish-lock",
                    append=True,
                )

            self.assertEqual(count, 1)
            second_read.assert_not_called()
            self.assertTrue(Path(parts[0]["path"]).is_file())
            with store.connection() as conn:
                committed = conn.execute(
                    "SELECT sha256 FROM parquet_parts WHERE path = ?",
                    (parts[0]["path"],),
                ).fetchone()
            self.assertEqual(str(committed["sha256"]), parts[0]["sha256"])

    def test_pending_archive_excludes_in_progress_files(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = MetadataStore(root / "metadata.sqlite3")
            settings = Settings(db_instance_id="rm-test000001")
            parsing_id, _ = store.upsert_remote(
                settings,
                remote("mysql-bin.000001", "2026-08-24T15:00:00Z"),
            )
            done_id, _ = store.upsert_remote(
                settings,
                remote("mysql-bin.000002", "2026-08-24T15:05:00Z"),
            )
            parsing_path = str(root / "parsing.parquet")
            done_path = str(root / "done.parquet")

            def part(path: str, epoch_us: int) -> dict[str, object]:
                return {
                    "path": path,
                    "event_date": "2026-08-24",
                    "row_count": 1,
                    "min_event_epoch_us": epoch_us,
                    "max_event_epoch_us": epoch_us,
                    "size_bytes": 10,
                    "sha256": hashlib.sha256(path.encode()).hexdigest(),
                }

            store.replace_parts(parsing_id, [part(parsing_path, 1)])
            store.replace_parts(done_id, [part(done_path, 2)])
            store.set_file_state(parsing_id, "parsing", query_visible=False)
            store.set_file_state(done_id, "done")

            pending_paths = {
                str(item["path"]) for item in store.pending_archive_parts()
            }
            self.assertNotIn(parsing_path, pending_paths)
            self.assertIn(done_path, pending_paths)

    def test_detached_transform_publishes_metadata_only_in_parent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = Settings(db_instance_id="rm-test000001")
            item = remote("mysql-bin.000001", "2026-07-27T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000)
            ndjson_path = storage.paths["staging"] / "detached.ndjson"
            ndjson_path.write_text(
                json.dumps(
                    {
                        "event_id": "event-detached",
                        "event_epoch_us": epoch_us,
                        "operation": "INSERT",
                        "database_name": "audit_db",
                        "table_name": "orders",
                        "start_position": 1,
                        "end_position": 2,
                    },
                    separators=(",", ":"),
                )
                + "\n",
                encoding="utf-8",
            )

            payload = {
                "data_dir": str(data_root),
                "file_id": file_id,
                "instance_id": settings.db_instance_id,
                "host_instance_id": item.host_instance_id,
                "source_file_name": item.log_file_name,
                "ndjson_path": str(ndjson_path),
                "part_key": "000000",
            }
            with ProcessPoolExecutor(
                max_workers=1,
                mp_context=multiprocessing.get_context("spawn"),
            ) as executor:
                count, parts = executor.submit(
                    storage_module.ingest_ndjson_file_detached,
                    payload,
                ).result(timeout=30)

            self.assertEqual(count, 1)
            self.assertEqual(
                store.parts_in_range(
                    start_epoch_us=epoch_us - 1,
                    end_epoch_us=epoch_us + 1,
                ),
                [],
            )
            storage.publish_ingested_parts(file_id, parts, append=True)
            self.assertEqual(
                len(
                    store.parts_in_range(
                        start_epoch_us=epoch_us - 1,
                        end_epoch_us=epoch_us + 1,
                    )
                ),
                1,
            )

    def test_duckdb_spills_use_persistent_data_scratch(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metadata = MetadataStore(root / "metadata.sqlite3")
            storage = EventStorage(metadata, root / "data")

            conn = storage._duckdb_connect()
            try:
                configured = str(
                    conn.execute(
                        "SELECT current_setting('temp_directory')"
                    ).fetchone()[0]
                )
            finally:
                conn.close()

            self.assertEqual(
                Path(configured).resolve(),
                storage.paths["scratch"].resolve(),
            )

        compose = (
            Path(__file__).resolve().parents[1] / "compose.yaml"
        ).read_text(encoding="utf-8")
        # Every Python service with the persistent data volume spills only
        # inside that same filesystem; adding a worker must preserve the rule.
        self.assertEqual(
            compose.count("TMPDIR: /data/scratch"),
            compose.count("RDS_BINLOG_DATA_DIR: /data"),
        )
        self.assertEqual(
            compose.count("RDS_BINLOG_STAGING_DIR: /data/staging"),
            2,
        )

    def test_parser_output_is_published_in_bounded_chunks(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.binlog"
            source.write_bytes(b"test")
            command = [
                sys.executable,
                "-c",
                (
                    "import json\n"
                    "for value in range(5):\n"
                    " print(json.dumps({'value': value}), flush=True)\n"
                ),
            ]
            with patch("app.parser_bridge._parser_command", return_value=command):
                paths = list(
                    parse_ndjson_chunks(
                        source,
                        "file-stream",
                        root / "staging",
                        max_lines=2,
                        max_bytes=1024 * 1024,
                    )
                )

            self.assertEqual(
                [len(path.read_text(encoding="utf-8").splitlines()) for path in paths],
                [2, 2, 1],
            )
            self.assertTrue(all(not path.name.endswith(".part") for path in paths))

    def test_buffered_parser_preserves_chunk_order_and_bounds(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.binlog"
            source.write_bytes(b"test")
            staging = root / "staging"
            command = [
                sys.executable,
                "-c",
                (
                    "import json, os, sys\n"
                    "from pathlib import Path\n"
                    f"root = Path({str(staging.resolve())!r})\n"
                    "root.mkdir(parents=True, exist_ok=True)\n"
                    "for index, values in enumerate(([0, 1], [2, 3], [4])):\n"
                    " final = root / f'file-buffered-{index:06d}.ndjson'\n"
                    " partial = Path(str(final) + '.part')\n"
                    " payload = ''.join(json.dumps({'value': value}) + '\\n' for value in values).encode()\n"
                    " partial.write_bytes(payload)\n"
                    " os.replace(partial, final)\n"
                    " print(json.dumps({'path': str(final.resolve()), 'rows': len(values), 'bytes': len(payload)}), flush=True)\n"
                    " if sys.stdin.readline().strip() != 'ok': raise SystemExit(2)\n"
                ),
            ]
            with patch("app.parser_bridge._parser_command", return_value=command):
                paths = list(
                    parse_ndjson_chunks_buffered(
                        source,
                        "file-buffered",
                        staging,
                        max_lines=2,
                        max_bytes=1024 * 1024,
                        max_prefetch=1,
                    )
                )
            self.assertEqual(
                [
                    [json.loads(line)["value"] for line in path.read_text().splitlines()]
                    for path in paths
                ],
                [[0, 1], [2, 3], [4]],
            )

    def test_native_parser_rejects_manifest_outside_staging(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.binlog"
            source.write_bytes(b"keep")
            outside = root / "file-outside-000000.ndjson"
            payload = b'{"value":1}\n'
            outside.write_bytes(payload)
            command = [
                sys.executable,
                "-c",
                (
                    "import json\n"
                    f"print(json.dumps({{'path': {str(outside.resolve())!r}, "
                    f"'rows': 1, 'bytes': {len(payload)}}}), flush=True)\n"
                    "input()\n"
                ),
            ]
            with patch("app.parser_bridge._parser_command", return_value=command):
                with self.assertRaises(ParserError) as raised:
                    list(
                        parse_native_ndjson_chunks(
                            source,
                            "file-outside",
                            root / "staging",
                        )
                    )
            self.assertEqual(raised.exception.code, "PARSER_CHUNK_PATH_INVALID")
            self.assertEqual(source.read_bytes(), b"keep")

    def test_each_ingested_chunk_is_immediately_queryable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = Settings(db_instance_id="rm-test000001")
            item = remote("mysql-bin.000001", "2026-07-27T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000)

            for index in range(2):
                ndjson_path = storage.paths["staging"] / f"chunk-{index}.ndjson"
                ndjson_path.write_text(
                    json.dumps(
                        {
                            "event_id": f"event-{index + 1}",
                            "event_epoch_us": epoch_us + index,
                            "operation": "INSERT",
                            "database_name": "audit_db",
                            "table_name": "orders",
                            "start_position": 100 + index,
                            "end_position": 101 + index,
                        },
                        separators=(",", ":"),
                    )
                    + "\n",
                    encoding="utf-8",
                )
                count, parts = storage.ingest_ndjson_file(
                    file_id=file_id,
                    instance_id=settings.db_instance_id,
                    host_instance_id=item.host_instance_id,
                    source_file_name=item.log_file_name,
                    ndjson_path=ndjson_path,
                    part_key=f"{index:06d}",
                    append=True,
                )
                self.assertEqual(count, 1)
                self.assertEqual(len(parts), 1)
                page = storage.query_events(
                    {"database": "audit_db", "limit": 10},
                    settings.retention_days,
                )
                self.assertEqual(len(page["rows"]), index + 1)

            self.assertEqual(
                [row["event_id"] for row in page["rows"]],
                ["event-2", "event-1"],
            )

    def test_ndjson_bulk_ingest_preserves_exact_rows_and_queryability(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = Settings(db_instance_id="rm-test000001")
            item = remote("mysql-bin.000001", "2026-07-27T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000)
            records = [
                {
                    "event_id": "event-1",
                    "event_epoch_us": epoch_us,
                    "raw_event_type": "WriteRowsEventV2",
                    "operation": "INSERT",
                    "database_name": "audit_db",
                    "table_name": "orders",
                    "server_id": 123,
                    "thread_id": 456,
                    "transaction_id": "trx-1",
                    "gtid": "uuid:1",
                    "xid": "9",
                    "start_position": 100,
                    "end_position": 200,
                    "row_index": 0,
                    "execution_time_ms": 0,
                    "error_code": 0,
                    "sql_kind": "",
                    "sql_text": "",
                    "sql_bytes_base64": "",
                    "before_json": "",
                    "after_json": '{"id":1,"amount":"12.3400"}',
                    "columns_json": '[{"name":"amount","type":"DECIMAL"}]',
                    "row_query": "",
                },
                {
                    "event_id": "event-2",
                    "event_epoch_us": epoch_us + 1,
                    "raw_event_type": "QueryEvent",
                    "operation": "DDL",
                    "database_name": "audit_db",
                    "table_name": "",
                    "server_id": 123,
                    "thread_id": 456,
                    "transaction_id": "trx-2",
                    "gtid": "uuid:2",
                    "xid": "",
                    "start_position": 201,
                    "end_position": 300,
                    "row_index": 0,
                    "execution_time_ms": 7,
                    "error_code": 0,
                    "sql_kind": "ALTER",
                    "sql_text": "ALTER TABLE orders ADD COLUMN note VARCHAR(20)",
                    "sql_bytes_base64": "",
                    "before_json": "",
                    "after_json": "",
                    "columns_json": "",
                    "row_query": "",
                },
            ]
            ndjson_path = storage.paths["staging"] / f"{file_id}.ndjson"
            ndjson_path.write_text(
                "".join(
                    json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n"
                    for record in records
                ),
                encoding="utf-8",
            )

            count, parts = storage.ingest_ndjson_file(
                file_id=file_id,
                instance_id=settings.db_instance_id,
                host_instance_id=item.host_instance_id,
                source_file_name=item.log_file_name,
                ndjson_path=ndjson_path,
            )

            self.assertEqual(count, 2)
            self.assertEqual(sum(part["row_count"] for part in parts), 2)
            self.assertTrue(all(Path(part["path"]).is_file() for part in parts))
            page = storage.query_events(
                {"database": "audit_db", "limit": 10}, settings.retention_days
            )
            self.assertEqual([row["event_id"] for row in page["rows"]], ["event-2", "event-1"])
            self.assertEqual(page["rows"][1]["after_json"], '{"id":1,"amount":"12.3400"}')


class OssTieringTests(unittest.TestCase):
    def test_event_detail_streams_batches_instead_of_materializing_row_groups(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-08-03T00:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000)
            part = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-streamed-detail",
                epoch_us=epoch_us,
                part_key="streamed-detail",
            )
            real_factory = pq.ParquetFile
            batch_calls: list[dict] = []

            class GuardedParquetFile:
                def __init__(self, source):
                    self.inner = real_factory(source)

                def __getattr__(self, name):
                    return getattr(self.inner, name)

                def read_row_groups(self, *_args, **_kwargs):
                    raise AssertionError("event detail must not materialize row groups")

                def iter_batches(self, *args, **kwargs):
                    batch_calls.append(dict(kwargs))
                    return self.inner.iter_batches(*args, **kwargs)

            with (
                patch("app.storage.pq.ParquetFile", GuardedParquetFile),
                patch.object(
                    storage,
                    "_read_part_table",
                    side_effect=AssertionError(
                        "event detail must not use the bulk query reader"
                    ),
                ),
            ):
                detail = storage.event_detail_tiered(
                    "event-streamed-detail",
                    settings,
                    None,
                    f"{part['sha256']}:*",
                )

            self.assertIsNotNone(detail)
            self.assertEqual(detail["event_id"], "event-streamed-detail")
            self.assertGreaterEqual(len(batch_calls), 2)
            self.assertTrue(
                all(call.get("use_threads") is False for call in batch_calls)
            )

    def test_oss_range_reader_reuses_bounded_read_ahead_and_checks_etag(
        self,
    ) -> None:
        payload = b"0123456789abcdef0123456789abcdef"

        class FakeBucket:
            def __init__(self) -> None:
                self.calls: list[tuple[str, tuple[int, int]]] = []
                self.etag = '"range-etag"'

            def get_object(self, key, byte_range):
                self.calls.append((key, byte_range))
                start, end = byte_range
                return SimpleNamespace(
                    read=lambda: payload[start : end + 1],
                    headers={"ETag": self.etag},
                )

        bucket = FakeBucket()
        reader = OssRangeReader(
            bucket,
            "cold/part.parquet",
            len(payload),
            "range-etag",
            read_ahead_bytes=8,
        )
        self.assertEqual(reader.seek(4), 4)
        self.assertEqual(reader.read(5), b"45678")
        self.assertEqual(bucket.calls, [("cold/part.parquet", (0, 15))])
        self.assertEqual(reader.seek(6), 6)
        self.assertEqual(reader.read(4), b"6789")
        self.assertEqual(bucket.calls, [("cold/part.parquet", (0, 15))])
        self.assertEqual(reader.stats(), {"range_requests": 1, "range_bytes": 16})

        bucket.etag = '"different-etag"'
        reader.seek(20)
        with self.assertRaises(OssArchiveError) as raised:
            reader.read(1)
        self.assertEqual(raised.exception.code, "OSS_RANGE_VERIFY_FAILED")

    def test_oss_range_reader_retries_transient_transport_failure(self) -> None:
        payload = b"retry-range-payload"

        class FlakyBucket:
            def __init__(self) -> None:
                self.calls = 0

            def get_object(self, _key, byte_range):
                self.calls += 1
                if self.calls < 3:
                    raise RuntimeError("transient TLS connect failure")
                start, end = byte_range
                return SimpleNamespace(
                    read=lambda: payload[start : end + 1],
                    headers={"ETag": '"retry-etag"'},
                )

        bucket = FlakyBucket()
        reader = OssRangeReader(
            bucket,
            "cold/retry.parquet",
            len(payload),
            "retry-etag",
            read_ahead_bytes=1,
            fetch_attempts=3,
            retry_delay_seconds=0,
        )

        self.assertEqual(reader.read(), payload)
        self.assertEqual(bucket.calls, 3)
        self.assertEqual(reader.stats(), {"range_requests": 1, "range_bytes": len(payload)})

    def test_unindexed_part_prunes_row_groups_with_structural_predicates(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            parquet_path = data_root / "events" / "candidate.parquet"
            parquet_path.parent.mkdir(parents=True, exist_ok=True)
            table = pa.table(
                {
                    "event_epoch_us": [10, 11, 12, 13],
                    "database_name": ["audit", "audit", "audit", "audit"],
                    "table_name": ["other", "other", "target_table", "target_table"],
                    "operation": ["UPDATE", "UPDATE", "UPDATE", "UPDATE"],
                    "sql_text": ["first", "second", "third", "fourth"],
                }
            )
            pq.write_table(table, parquet_path, row_group_size=2, compression="zstd")
            digest = hashlib.sha256(parquet_path.read_bytes()).hexdigest()
            part = {
                "path": str(parquet_path),
                "row_count": 4,
                "min_event_epoch_us": 10,
                "max_event_epoch_us": 13,
                "size_bytes": parquet_path.stat().st_size,
                "sha256": digest,
            }

            selected, tier, stats = storage._read_part_table(
                part,
                None,
                None,
                columns=("event_epoch_us", "table_name", "sql_text"),
                predicate_query={
                    "start_epoch_us": 10,
                    "end_epoch_us": 13,
                    "table": "target",
                    "operations": ["UPDATE"],
                },
            )

            self.assertEqual(tier, "local-index")
            self.assertEqual(selected.num_rows, 2)
            self.assertEqual(selected.column("table_name").to_pylist(), ["target_table"] * 2)
            self.assertEqual(stats["predicate_row_groups_scanned"], 2)
            self.assertEqual(stats["predicate_row_groups_selected"], 1)
            self.assertFalse(storage.search_index.is_structural_current(part))
            storage.search_index.index_structural_parquet(part, parquet_path)
            self.assertTrue(storage.search_index.is_structural_current(part))
            plan = storage.search_index.candidate_blocks(
                [part],
                {
                    "table": "target",
                    "keyword": "keyword_not_in_structural_index",
                },
                start_epoch_us=10,
                end_epoch_us=13,
            )
            self.assertEqual(
                [entry["row_group_id"] for entry in plan["entries"]],
                [1],
            )
            self.assertFalse(plan["entries"][0]["complete"])
            self.assertEqual(plan["unknown_paths"], set())

    def test_identical_concurrent_queries_share_one_scan(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            settings = oss_settings()
            started = threading.Event()
            release = threading.Event()
            call_count = 0
            call_lock = threading.Lock()

            def slow_query(_query, _settings, _archive, *, limit_cap=1000):
                nonlocal call_count
                with call_lock:
                    call_count += 1
                started.set()
                self.assertTrue(release.wait(2))
                return {"rows": [], "limit_cap": limit_cap}

            storage._query_events_tiered_impl = slow_query
            query = {"start_epoch_us": 10, "end_epoch_us": 20, "limit": 100}
            with ThreadPoolExecutor(max_workers=2) as executor:
                first = executor.submit(
                    storage.query_events_tiered, query, settings, None
                )
                self.assertTrue(started.wait(1))
                second = executor.submit(
                    storage.query_events_tiered, query, settings, None
                )
                time.sleep(0.05)
                release.set()
                self.assertEqual(first.result(timeout=2)["rows"], [])
                self.assertEqual(second.result(timeout=2)["rows"], [])
            self.assertEqual(call_count, 1)

    def test_packed_parts_share_one_object_and_keep_exact_ranges(self) -> None:
        class MissingObject(Exception):
            status = 404

        class Body:
            def __init__(self, payload: bytes, etag: str):
                self.payload = payload
                self.position = 0
                self.headers = {"ETag": f'"{etag}"'}

            def read(self, size: int = -1) -> bytes:
                if size < 0:
                    size = len(self.payload) - self.position
                start = self.position
                self.position = min(self.position + size, len(self.payload))
                return self.payload[start : self.position]

        class FakeBucket:
            def __init__(self):
                self.objects: dict[str, dict] = {}
                self.ranges: list[tuple[int, int]] = []
                self.put_calls = 0

            def head_object(self, key):
                if key not in self.objects:
                    raise MissingObject()
                value = self.objects[key]
                return SimpleNamespace(
                    headers=value["headers"],
                    content_length=len(value["body"]),
                    etag=value["etag"],
                )

            def put_object_from_file(self, key, filename, headers=None):
                self.put_calls += 1
                self.objects[key] = {
                    "body": Path(filename).read_bytes(),
                    "headers": dict(headers or {}),
                    "etag": "pack-etag",
                }

            def get_object(self, key, byte_range):
                self.ranges.append(byte_range)
                start, end = byte_range
                value = self.objects[key]
                return Body(value["body"][start : end + 1], value["etag"])

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            payloads = [b"first-parquet-body", b"second-parquet-body-longer"]
            parts = []
            for index, payload in enumerate(payloads):
                path = root / f"part-{index}.parquet"
                path.write_bytes(payload)
                parts.append(
                    {
                        "path": str(path),
                        "event_date": "2026-07-29",
                        "row_count": 1,
                        "min_event_epoch_us": index,
                        "max_event_epoch_us": index,
                        "size_bytes": len(payload),
                        "sha256": hashlib.sha256(payload).hexdigest(),
                    }
                )
            bucket = FakeBucket()
            archive = OssArchive(oss_settings(), bucket=bucket)
            results = archive.upload_parts(
                parts,
                scratch_dir=root,
                target_bytes=1024,
            )

            self.assertEqual(bucket.put_calls, 1)
            self.assertEqual(results[0]["oss_key"], results[1]["oss_key"])
            self.assertEqual(results[0]["oss_offset"], 0)
            self.assertEqual(results[0]["oss_length"], len(payloads[0]))
            self.assertEqual(results[1]["oss_offset"], len(payloads[0]))
            self.assertEqual(results[1]["oss_length"], len(payloads[1]))

            packed_part = {**parts[1], **results[1]}
            reader = archive.open_part_reader(packed_part)
            self.assertEqual(reader.read(), payloads[1])
            expected_range = (
                len(payloads[0]),
                len(payloads[0]) + len(payloads[1]) - 1,
            )
            self.assertEqual(bucket.ranges[-1], expected_range)

            destination = root / "downloaded.parquet"
            archive.download_part(packed_part, destination)
            self.assertEqual(destination.read_bytes(), payloads[1])
            self.assertEqual(bucket.ranges[-1], expected_range)

            fresh_bucket = FakeBucket()
            fresh_archive = OssArchive(oss_settings(), bucket=fresh_bucket)
            fresh_results = fresh_archive.upload_parts(
                parts,
                scratch_dir=root,
                target_bytes=1024,
                fresh=True,
            )
            self.assertEqual(fresh_bucket.put_calls, 2)
            self.assertNotEqual(
                fresh_results[0]["oss_key"],
                fresh_results[1]["oss_key"],
            )
            self.assertTrue(
                all(int(item.get("oss_offset") or 0) == 0 for item in fresh_results)
            )
            self.assertTrue(
                all(int(item.get("oss_length") or 0) == 0 for item in fresh_results)
            )

    def test_pyarrow_reads_one_row_group_through_oss_ranges(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            parquet_path = Path(directory) / "range.parquet"
            row_count = 25_000
            table = pa.table(
                {
                    "event_epoch_us": list(range(row_count)),
                    "payload": [
                        hashlib.sha256(str(index).encode("ascii")).hexdigest()
                        for index in range(row_count)
                    ],
                }
            )
            pq.write_table(
                table,
                parquet_path,
                compression="zstd",
                compression_level=9,
                row_group_size=8192,
            )
            payload = parquet_path.read_bytes()

            class FakeBucket:
                def get_object(self, _key, byte_range):
                    start, end = byte_range
                    return SimpleNamespace(
                        read=lambda: payload[start : end + 1],
                        headers={"ETag": '"range-etag"'},
                    )

            reader = OssRangeReader(
                FakeBucket(),
                "cold/range.parquet",
                len(payload),
                "range-etag",
            )
            parquet = pq.ParquetFile(reader)
            selected = parquet.read_row_group(1, columns=["event_epoch_us"])
            self.assertEqual(selected.num_rows, 8192)
            self.assertGreater(reader.request_count, 0)
            self.assertLess(reader.bytes_read, len(payload))

    def test_access_key_mode_uses_protected_cloud_credential(self) -> None:
        settings = oss_settings(oss_auth_mode="access_key")
        credential = CloudCredential("test-access-id", "test-access-secret", "token")
        with (
            patch("app.oss_store.oss2.ProviderAuthV4") as provider_auth,
            patch("app.oss_store.oss2.Bucket") as bucket,
        ):
            provider_auth.return_value = object()
            OssArchive(settings, credential=credential)

        provider = provider_auth.call_args.args[0]
        resolved = provider.get_credentials()
        self.assertEqual(resolved.get_access_key_id(), "test-access-id")
        self.assertEqual(resolved.get_access_key_secret(), "test-access-secret")
        self.assertEqual(resolved.get_security_token(), "token")
        bucket.assert_called_once()

    def test_sync_manager_passes_protected_credential_to_oss(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory)
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            settings = oss_settings(oss_auth_mode="access_key")
            credential = CloudCredential("test-access-id", "test-access-secret")
            with patch("app.pipeline.OssArchive") as archive:
                manager = SyncManager(
                    store,
                    storage,
                    credential_loader=lambda _target: credential,
                )
                try:
                    manager.archive_for_settings(settings)
                finally:
                    manager.shutdown()
            archive.assert_called_once_with(settings, credential=credential)

    def test_sync_manager_shutdown_joins_catalog_stats_backfill(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory)
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            started = threading.Event()
            release = threading.Event()
            shutdown_done = threading.Event()

            def delayed_stats():
                started.set()
                release.wait(5)
                return {"cataloged_parts": 0, "total_parts": 0}

            with patch.object(store, "part_catalog_stats", delayed_stats):
                manager = SyncManager(store, storage)
                self.assertTrue(started.wait(1))
                shutdown = threading.Thread(
                    target=lambda: (manager.shutdown(), shutdown_done.set())
                )
                shutdown.start()
                try:
                    self.assertFalse(shutdown_done.wait(0.1))
                finally:
                    release.set()
                    shutdown.join(2)
            self.assertTrue(shutdown_done.is_set())

    @staticmethod
    def _ingest_one(
        storage: EventStorage,
        file_id: str,
        item: RemoteBinlog,
        *,
        event_id: str,
        epoch_us: int,
        part_key: str,
    ) -> dict:
        path = storage.paths["staging"] / f"{event_id}.ndjson"
        path.write_text(
            json.dumps(
                {
                    "event_id": event_id,
                    "event_epoch_us": epoch_us,
                    "operation": "INSERT",
                    "database_name": "archive_db",
                    "table_name": "orders",
                    "start_position": epoch_us % 1000,
                    "end_position": epoch_us % 1000 + 1,
                },
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
        _, parts = storage.ingest_ndjson_file(
            file_id=file_id,
            instance_id="rm-test000001",
            host_instance_id=item.host_instance_id,
            source_file_name=item.log_file_name,
            ndjson_path=path,
            part_key=part_key,
            append=True,
        )
        part = parts[0]
        storage.ensure_part_index(part, None)
        return part

    def test_metadata_schema_tracks_archive_without_losing_local_path(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = MetadataStore(Path(directory) / "metadata.sqlite3")
            with store.connection() as conn:
                columns = {
                    str(row["name"])
                    for row in conn.execute(
                        "PRAGMA table_info(parquet_parts)"
                    ).fetchall()
                }
            self.assertTrue(
                {
                    "oss_key",
                    "oss_etag",
                    "oss_offset",
                    "oss_length",
                    "oss_object_sha256",
                    "oss_uploaded_at",
                    "oss_verified_at",
                    "local_last_access_at",
                }.issubset(columns)
            )

    def test_ingest_records_part_filter_catalog(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            part = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-catalog",
                epoch_us=int(time.time() * 1_000_000),
                part_key="000001",
            )

            catalog = store.part_catalogs([part["path"]])[part["path"]]
            self.assertEqual(catalog["databases"], ["archive_db"])
            self.assertEqual(catalog["tables"], ["orders"])
            self.assertEqual(catalog["operations"], ["INSERT"])
            self.assertEqual(
                store.part_catalog_stats(),
                {"total_parts": 1, "cataloged_parts": 1},
            )

            with store.connection() as conn:
                conn.execute(
                    "DELETE FROM parquet_part_catalog WHERE path = ?",
                    (part["path"],),
                )
            self.assertEqual(
                [value["path"] for value in store.missing_part_catalogs()],
                [part["path"]],
            )
            result = storage.ensure_part_catalog(part, None)
            self.assertEqual(result["cataloged"], 1)
            self.assertEqual(result["rows"], 1)
            self.assertEqual(
                store.part_catalogs([part["path"]])[part["path"]]["tables"],
                ["orders"],
            )

            with store.connection() as conn:
                conn.execute(
                    "DELETE FROM parquet_part_catalog WHERE path = ?",
                    (part["path"],),
                )
            storage.search_index.remove_part(part["path"])
            storage.ensure_part_index(part, None)
            self.assertEqual(
                store.part_catalogs([part["path"]])[part["path"]]["tables"],
                ["orders"],
            )

    def test_archived_body_is_released_before_background_index(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000)
            source = storage.paths["staging"] / "async-index.ndjson"
            source.write_text(
                json.dumps(
                    {
                        "event_id": "event-async-index",
                        "event_epoch_us": epoch_us,
                        "operation": "INSERT",
                        "database_name": "archive_db",
                        "table_name": "orders",
                        "after_json": "{\"id\":2571634}",
                    },
                    separators=(",", ":"),
                )
                + "\n",
                encoding="utf-8",
            )
            _, parts = storage.ingest_ndjson_file(
                file_id=file_id,
                instance_id=settings.db_instance_id,
                host_instance_id=item.host_instance_id,
                source_file_name=item.log_file_name,
                ndjson_path=source,
                part_key="async-index",
                append=True,
            )
            part = parts[0]
            local_path = Path(part["path"])
            archive = FakeArchive()
            uploaded = archive.upload_part(part)
            store.mark_part_archived(
                part["path"],
                oss_key=uploaded["oss_key"],
                oss_etag=uploaded["oss_etag"],
            )
            archived_part = {
                **part,
                "oss_key": uploaded["oss_key"],
                "oss_etag": uploaded["oss_etag"],
            }

            self.assertTrue(local_path.is_file())
            self.assertFalse(storage.search_index.is_current(part))
            self.assertGreater(storage.release_archived_body(archived_part), 0)
            self.assertFalse(local_path.exists())
            page = storage.query_events_tiered(
                {
                    "start_epoch_us": epoch_us,
                    "end_epoch_us": epoch_us,
                    "table": "orders",
                    "keyword": "2571634",
                    "limit": 10,
                },
                settings,
                archive,
            )
            self.assertEqual(page["rows"][0]["event_id"], "event-async-index")
            self.assertEqual(page["oss_range_parts_read"], 1)
            self.assertEqual(archive.downloaded, [])

            storage.ensure_part_index(archived_part, archive)
            self.assertTrue(storage.search_index.is_current(part))

    def test_stale_archived_snapshot_cannot_delete_rewritten_body(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000)

            first_source = storage.paths["staging"] / "release-old.ndjson"
            first_source.write_text(
                json.dumps(
                    {
                        "event_id": "event-release-old",
                        "event_epoch_us": epoch_us,
                        "operation": "INSERT",
                        "database_name": "archive_db",
                        "table_name": "orders",
                        "after_json": "{\"id\":1}",
                    },
                    separators=(",", ":"),
                )
                + "\n",
                encoding="utf-8",
            )
            _, first_parts = storage.ingest_ndjson_file(
                file_id=file_id,
                instance_id=settings.db_instance_id,
                host_instance_id=item.host_instance_id,
                source_file_name=item.log_file_name,
                ndjson_path=first_source,
                part_key="release-version-race",
                append=True,
            )
            first_part = first_parts[0]
            store.mark_part_archived(
                first_part["path"],
                oss_key="fake/old-version.parquet",
                oss_etag="old-etag",
            )
            stale_archived_part = {
                **first_part,
                "oss_key": "fake/old-version.parquet",
                "oss_etag": "old-etag",
            }

            second_source = storage.paths["staging"] / "release-new.ndjson"
            second_source.write_text(
                json.dumps(
                    {
                        "event_id": "event-release-new",
                        "event_epoch_us": epoch_us + 1,
                        "operation": "UPDATE",
                        "database_name": "archive_db",
                        "table_name": "orders",
                        "after_json": "{\"id\":2}",
                    },
                    separators=(",", ":"),
                )
                + "\n",
                encoding="utf-8",
            )
            _, second_parts = storage.ingest_ndjson_file(
                file_id=file_id,
                instance_id=settings.db_instance_id,
                host_instance_id=item.host_instance_id,
                source_file_name=item.log_file_name,
                ndjson_path=second_source,
                part_key="release-version-race",
                append=True,
                publish_metadata=False,
            )
            second_part = second_parts[0]
            self.assertEqual(first_part["path"], second_part["path"])
            self.assertNotEqual(first_part["sha256"], second_part["sha256"])
            self.assertTrue(Path(second_part["path"]).is_file())
            self.assertEqual(
                store.part_by_path(second_part["path"])["sha256"],
                first_part["sha256"],
            )

            self.assertEqual(
                storage.release_archived_body(stale_archived_part),
                0,
            )
            self.assertTrue(Path(second_part["path"]).is_file())
            storage.publish_ingested_parts(
                file_id,
                second_parts,
                append=True,
            )
            self.assertEqual(
                store.part_by_path(second_part["path"])["sha256"],
                second_part["sha256"],
            )

    def test_identical_retry_reuses_committed_archive_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            payload = (
                json.dumps(
                    {
                        "event_id": "event-identical-retry",
                        "event_epoch_us": int(time.time() * 1_000_000),
                        "operation": "INSERT",
                        "database_name": "archive_db",
                        "table_name": "orders",
                        "after_json": "{\"id\":2571634}",
                    },
                    separators=(",", ":"),
                )
                + "\n"
            )

            first_source = storage.paths["staging"] / "identical-first.ndjson"
            first_source.write_text(payload, encoding="utf-8")
            _, first_parts = storage.ingest_ndjson_file(
                file_id=file_id,
                instance_id=settings.db_instance_id,
                host_instance_id=item.host_instance_id,
                source_file_name=item.log_file_name,
                ndjson_path=first_source,
                part_key="identical-retry",
                append=True,
            )
            first_part = first_parts[0]
            archive = FakeArchive()
            uploaded = archive.upload_part(first_part)
            store.mark_part_archived(
                first_part["path"],
                oss_key=uploaded["oss_key"],
                oss_etag=uploaded["oss_etag"],
            )
            archived_part = {**first_part, **uploaded}
            self.assertGreater(storage.release_archived_body(archived_part), 0)

            retry_source = storage.paths["staging"] / "identical-retry.ndjson"
            retry_source.write_text(payload, encoding="utf-8")
            _, retry_parts = storage.ingest_ndjson_file(
                file_id=file_id,
                instance_id=settings.db_instance_id,
                host_instance_id=item.host_instance_id,
                source_file_name=item.log_file_name,
                ndjson_path=retry_source,
                part_key="identical-retry",
                append=True,
            )
            retry_part = retry_parts[0]

            self.assertEqual(retry_part["sha256"], first_part["sha256"])
            self.assertEqual(retry_part["oss_key"], uploaded["oss_key"])
            self.assertEqual(retry_part["oss_etag"], uploaded["oss_etag"])
            self.assertEqual(
                [part for part in retry_parts if not part.get("oss_key")],
                [],
            )

    def test_release_waits_for_active_local_index_reader(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            part = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-release-race",
                epoch_us=int(time.time() * 1_000_000),
                part_key="release-race",
            )
            storage.search_index.remove_part(part["path"])
            archived_part = {**part, "oss_key": "fake/release-race.parquet"}
            store.mark_part_archived(
                part["path"],
                oss_key=archived_part["oss_key"],
                oss_etag="release-race-etag",
            )
            index_started = threading.Event()
            allow_index_finish = threading.Event()
            release_finished = threading.Event()
            released: list[int] = []

            def hold_index(_part, source):
                self.assertEqual(Path(source), Path(part["path"]))
                index_started.set()
                self.assertTrue(allow_index_finish.wait(timeout=2))
                return {"indexed": 1, "row_groups": 1, "rows": 1}

            def release_body():
                released.append(storage.release_archived_body(archived_part))
                release_finished.set()

            with patch.object(
                storage.search_index,
                "index_parquet",
                side_effect=hold_index,
            ):
                index_thread = threading.Thread(
                    target=storage.ensure_part_index,
                    args=(part, None),
                )
                release_thread = threading.Thread(target=release_body)
                index_thread.start()
                self.assertTrue(index_started.wait(timeout=2))
                release_thread.start()
                self.assertFalse(release_finished.wait(timeout=0.05))
                allow_index_finish.set()
                index_thread.join(timeout=2)
                release_thread.join(timeout=2)

            self.assertFalse(index_thread.is_alive())
            self.assertFalse(release_thread.is_alive())
            self.assertTrue(release_finished.is_set())
            self.assertGreater(released[0], 0)
            self.assertFalse(Path(part["path"]).exists())

    def test_search_index_deletes_contentless_fts_rows(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            part = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-index-delete",
                epoch_us=int(time.time() * 1_000_000),
                part_key="000001",
            )
            with storage.search_index.connection() as conn:
                self.assertGreater(
                    int(conn.execute("SELECT count(*) FROM keyword_fts").fetchone()[0]),
                    0,
                )

            storage.search_index.remove_part(part["path"])
            with storage.search_index.connection() as conn:
                for table in (
                    "keyword_fts",
                    "token_fts",
                    "database_fts",
                    "table_fts",
                ):
                    self.assertEqual(
                        int(conn.execute(f"SELECT count(*) FROM {table}").fetchone()[0]),
                        0,
                    )
                self.assertEqual(
                    int(conn.execute("SELECT count(*) FROM blocks").fetchone()[0]),
                    0,
                )

    def test_table_clustering_and_exact_token_locate_one_row_group(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000)
            row_count = 18_000
            target_value = 10_000_000 + row_count - 1
            source = storage.paths["staging"] / "cluster.ndjson"
            source.write_text(
                "".join(
                    json.dumps(
                        {
                            "event_id": f"cluster-{index}",
                            "event_epoch_us": epoch_us + index,
                            "operation": "UPDATE",
                            "database_name": "example_app",
                            "table_name": "alpha" if index < 9_000 else "beta",
                            "after_json": json.dumps(
                                {"id": 10_000_000 + index},
                                separators=(",", ":"),
                            ),
                            "start_position": index,
                            "end_position": index + 1,
                        },
                        separators=(",", ":"),
                    )
                    + "\n"
                    for index in range(row_count)
                ),
                encoding="utf-8",
            )
            _, parts = storage.ingest_ndjson_file(
                file_id=file_id,
                instance_id=settings.db_instance_id,
                host_instance_id=item.host_instance_id,
                source_file_name=item.log_file_name,
                ndjson_path=source,
                part_key="cluster",
                append=True,
            )
            part = parts[0]
            storage.ensure_part_index(part, None)
            parquet = pq.ParquetFile(part["path"])
            self.assertEqual(
                [
                    parquet.metadata.row_group(index).num_rows
                    for index in range(parquet.num_row_groups)
                ],
                [8192, 8192, 1616],
            )
            parquet.close()
            plan = storage.search_index.candidate_blocks(
                [part],
                {
                    "database": "example_app",
                    "table": "beta",
                    "keyword": str(target_value),
                    "operations": ["UPDATE"],
                },
                start_epoch_us=epoch_us,
                end_epoch_us=epoch_us + row_count,
            )
            # Exact-token postings keep the target block first, while trigram
            # postings may conservatively retain another block whose different
            # values collectively contain the same trigrams. The row filter is
            # still exact and arbitrary substring matches cannot be dropped.
            self.assertEqual(plan["entries"][0]["row_group_id"], 2)
            self.assertEqual(
                {entry["row_group_id"] for entry in plan["entries"]},
                {1, 2},
            )
            substring_plan = storage.search_index.candidate_blocks(
                [part],
                {
                    "database": "example_app",
                    "table": "beta",
                    "keyword": str(target_value)[1:],
                    "operations": ["UPDATE"],
                },
                start_epoch_us=epoch_us,
                end_epoch_us=epoch_us + row_count,
            )
            self.assertEqual(substring_plan["entries"][0]["row_group_id"], 2)
            self.assertIn(2, {entry["row_group_id"] for entry in substring_plan["entries"]})
            miss = storage.search_index.candidate_blocks(
                [part],
                {
                    "database": "example_app",
                    "table": "beta",
                    "keyword": "999999999999",
                },
                start_epoch_us=epoch_us,
                end_epoch_us=epoch_us + row_count,
            )
            # Repeated trigrams are a conservative candidate at block level;
            # only the exact Arrow row predicate may prove this substring miss.
            self.assertTrue(miss["entries"])

    def test_oss_upload_is_content_addressed_and_lifecycle_merge_preserves_rules(
        self,
    ) -> None:
        class MissingObject(Exception):
            status = 404

        class FakeBucket:
            def __init__(self):
                self.objects = {}
                self.head_calls = 0
                self.put_calls = 0
                self.lifecycle = oss2.models.BucketLifecycle(
                    [
                        oss2.models.LifecycleRule(
                            "keep-other",
                            "other/",
                            expiration=oss2.models.LifecycleExpiration(days=7),
                        )
                    ]
                )

            def head_object(self, key):
                self.head_calls += 1
                if key not in self.objects:
                    raise MissingObject()
                value = self.objects[key]
                return SimpleNamespace(
                    headers=value["headers"],
                    content_length=len(value["body"]),
                    etag=value["etag"],
                )

            def put_object_from_file(self, key, filename, headers=None):
                self.put_calls += 1
                self.objects[key] = {
                    "body": Path(filename).read_bytes(),
                    "headers": dict(headers or {}),
                    "etag": "test-etag",
                }

            def get_bucket_lifecycle(self):
                return self.lifecycle

            def put_bucket_lifecycle(self, lifecycle):
                self.lifecycle = lifecycle

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "part.parquet"
            path.write_bytes(b"parquet-test")
            sha256 = hashlib.sha256(path.read_bytes()).hexdigest()
            part = {
                "path": str(path),
                "event_date": "2026-07-29",
                "row_count": 1,
                "min_event_epoch_us": 1,
                "max_event_epoch_us": 2,
                "size_bytes": path.stat().st_size,
                "sha256": sha256,
            }
            bucket = FakeBucket()
            archive = OssArchive(oss_settings(), bucket=bucket)
            uploaded = archive.upload_part(part, fresh=True)
            self.assertIn(sha256[:16], uploaded["oss_key"])
            self.assertEqual(bucket.head_calls, 1)
            self.assertEqual(bucket.put_calls, 1)
            self.assertEqual(archive.upload_part(part), uploaded)
            self.assertEqual(bucket.head_calls, 2)

            lifecycle = archive.ensure_lifecycle()
            self.assertEqual(lifecycle["expirationDays"], 60)
            self.assertEqual(
                [rule.id for rule in bucket.lifecycle.rules],
                ["keep-other", archive.lifecycle_rule_id],
            )

    def test_oss_upload_retries_only_transient_failures_idempotently(self) -> None:
        class MissingObject(Exception):
            status = 404

        class RequestError(Exception):
            status = -2

        class AccessDenied(Exception):
            status = 403

        class FakeBucket:
            def __init__(self, failure, *, commit_before_failure=False):
                self.failure = failure
                self.commit_before_failure = commit_before_failure
                self.objects = {}
                self.put_calls = 0

            def head_object(self, key):
                if key not in self.objects:
                    raise MissingObject()
                value = self.objects[key]
                return SimpleNamespace(
                    headers=value["headers"],
                    content_length=len(value["body"]),
                    etag=value["etag"],
                )

            def put_object_from_file(self, key, filename, headers=None):
                self.put_calls += 1
                if self.put_calls == 1 and self.commit_before_failure:
                    self.objects[key] = {
                        "body": Path(filename).read_bytes(),
                        "headers": dict(headers or {}),
                        "etag": "committed-before-eof",
                    }
                if self.put_calls == 1:
                    raise self.failure
                self.objects[key] = {
                    "body": Path(filename).read_bytes(),
                    "headers": dict(headers or {}),
                    "etag": "retry-etag",
                }

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "retry.parquet"
            path.write_bytes(b"retry-parquet")
            part = {
                "path": str(path),
                "event_date": "2026-08-24",
                "row_count": 1,
                "min_event_epoch_us": 1,
                "max_event_epoch_us": 2,
                "size_bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }

            transient = FakeBucket(RequestError("tls eof"))
            archive = OssArchive(
                oss_settings(),
                bucket=transient,
                upload_attempts=3,
                upload_retry_delay_seconds=0,
            )
            uploaded = archive.upload_part(part, fresh=True)
            self.assertEqual(transient.put_calls, 2)
            self.assertEqual(uploaded["oss_etag"], "retry-etag")

            ambiguous = FakeBucket(
                RequestError("tls eof after commit"),
                commit_before_failure=True,
            )
            archive = OssArchive(
                oss_settings(),
                bucket=ambiguous,
                upload_attempts=3,
                upload_retry_delay_seconds=0,
            )
            uploaded = archive.upload_part(part, fresh=True)
            self.assertEqual(ambiguous.put_calls, 1)
            self.assertEqual(uploaded["oss_etag"], "committed-before-eof")

            denied = FakeBucket(AccessDenied("denied"))
            archive = OssArchive(
                oss_settings(),
                bucket=denied,
                upload_attempts=3,
                upload_retry_delay_seconds=0,
            )
            with self.assertRaises(OssArchiveError):
                archive.upload_part(part, fresh=True)
            self.assertEqual(denied.put_calls, 1)

    def test_query_uses_local_then_oss_and_deduplicates(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000)
            older = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-older",
                epoch_us=epoch_us,
                part_key="000001",
            )
            self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-newer",
                epoch_us=epoch_us + 1,
                part_key="000002",
            )
            archive = FakeArchive()
            uploaded = archive.upload_part(older)
            store.mark_part_archived(
                older["path"],
                oss_key=uploaded["oss_key"],
                oss_etag=uploaded["oss_etag"],
            )
            Path(older["path"]).unlink()

            result = storage.query_events_tiered(
                {
                    "start_epoch_us": epoch_us - 1,
                    "end_epoch_us": epoch_us + 1,
                    "limit": 10,
                },
                settings,
                archive,
            )
            self.assertEqual(
                [row["event_id"] for row in result["rows"]],
                ["event-newer", "event-older"],
            )
            self.assertEqual(result["tiers_used"], ["local-index", "oss-range"])
            self.assertEqual(len(archive.ranged), 1)
            self.assertEqual(archive.downloaded, [])

    def test_query_scans_independent_parts_concurrently(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            for index in range(8):
                self._ingest_one(
                    storage,
                    file_id,
                    item,
                    event_id=f"event-parallel-{index}",
                    epoch_us=epoch_us + index,
                    part_key=f"parallel-{index}",
                )

            original_read = storage._read_part_table
            state_lock = threading.Lock()
            active = 0
            max_active = 0

            def delayed_read(*args, **kwargs):
                nonlocal active, max_active
                with state_lock:
                    active += 1
                    max_active = max(max_active, active)
                try:
                    time.sleep(0.03)
                    return original_read(*args, **kwargs)
                finally:
                    with state_lock:
                        active -= 1

            storage._read_part_table = delayed_read
            result = storage.query_events_tiered(
                {
                    "start_epoch_us": epoch_us,
                    "end_epoch_us": epoch_us + 7,
                    "limit": 100,
                },
                settings,
                None,
            )

            self.assertEqual(len(result["rows"]), 8)
            self.assertGreater(max_active, 1)

    def test_query_never_builds_structural_index_inline(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            part = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-query-read-only-index",
                epoch_us=epoch_us,
                part_key="query-read-only-index",
            )
            storage.search_index.remove_part(str(part["path"]))

            with patch.object(
                storage,
                "ensure_part_structural_index",
                side_effect=AssertionError("query attempted an index write"),
            ) as inline_index, patch.object(
                storage.search_index,
                "index_structural_parquet",
                side_effect=AssertionError("scan attempted an index write"),
            ) as inline_scan_index:
                result = storage.query_events_tiered(
                    {
                        "start_epoch_us": epoch_us,
                        "end_epoch_us": epoch_us,
                        "table": "orders",
                        "limit": 10,
                    },
                    settings,
                    None,
                )

            self.assertEqual(
                [row["event_id"] for row in result["rows"]],
                ["event-query-read-only-index"],
            )
            inline_index.assert_not_called()
            inline_scan_index.assert_not_called()

    def test_query_keeps_workers_busy_behind_a_slow_part(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            part_paths = {}
            for index in range(9):
                part = self._ingest_one(
                    storage,
                    file_id,
                    item,
                    event_id=f"event-streaming-{index}",
                    epoch_us=epoch_us + index,
                    part_key=f"streaming-{index}",
                )
                part_paths[index] = str(part["path"])

            original_read = storage._read_part_table
            state_lock = threading.Lock()
            slow_finished = False
            oldest_started_before_slow_finished = False
            oldest_started = threading.Event()
            path_indexes = {path: index for index, path in part_paths.items()}
            call_order = []

            def delayed_read(part, *args, **kwargs):
                nonlocal slow_finished, oldest_started_before_slow_finished
                path = str(part["path"])
                with state_lock:
                    call_order.append(path_indexes[path])
                if path == part_paths[8]:
                    try:
                        oldest_started.wait(timeout=5)
                        return original_read(part, *args, **kwargs)
                    finally:
                        with state_lock:
                            slow_finished = True
                else:
                    if path == part_paths[0]:
                        with state_lock:
                            oldest_started_before_slow_finished = not slow_finished
                        oldest_started.set()
                    time.sleep(0.01)
                    return original_read(part, *args, **kwargs)

            storage._read_part_table = delayed_read
            result = storage.query_events_tiered(
                {
                    "start_epoch_us": epoch_us,
                    "end_epoch_us": epoch_us + 8,
                    "limit": 100,
                },
                settings,
                None,
            )

            self.assertEqual(len(result["rows"]), 9)
            self.assertTrue(
                oldest_started_before_slow_finished,
                f"read order={call_order}",
            )

    def test_range_query_does_not_persist_body_cache(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            part = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-query-lru",
                epoch_us=epoch_us,
                part_key="000001",
            )
            archive = FakeArchive()
            uploaded = archive.upload_part(part)
            store.mark_part_archived(
                part["path"],
                oss_key=uploaded["oss_key"],
                oss_etag=uploaded["oss_etag"],
            )
            Path(part["path"]).unlink()
            storage.search_index.remove_part(part["path"])
            query = {
                "start_epoch_us": epoch_us,
                "end_epoch_us": epoch_us,
                "limit": 10,
            }

            first = storage.query_events_tiered(query, settings, archive)
            second = storage.query_events_tiered(query, settings, archive)

            self.assertEqual(first["rows"][0]["event_id"], "event-query-lru")
            self.assertEqual(second["rows"][0]["event_id"], "event-query-lru")
            self.assertEqual(len(archive.ranged), 1)
            self.assertEqual(archive.downloaded, [])
            self.assertTrue(second["query_certificate_hit"])
            self.assertEqual(second["query_cache_parts_read"], 0)
            self.assertEqual(storage.query_cache_stats()["part_count"], 0)
            self.assertEqual(
                list(storage.paths["legacy_query_cache"].glob("*.parquet")),
                [],
            )

    def test_part_catalog_skips_impossible_oss_download(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            part = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-catalog-skip",
                epoch_us=epoch_us,
                part_key="000001",
            )
            archive = FakeArchive()
            uploaded = archive.upload_part(part)
            store.mark_part_archived(
                part["path"],
                oss_key=uploaded["oss_key"],
                oss_etag=uploaded["oss_etag"],
            )
            Path(part["path"]).unlink()
            storage.search_index.remove_part(part["path"])

            result = storage.query_events_tiered(
                {
                    "start_epoch_us": epoch_us,
                    "end_epoch_us": epoch_us,
                    "table": "table_not_in_this_part",
                    "limit": 10,
                },
                settings,
                archive,
            )

            self.assertEqual(result["rows"], [])
            self.assertEqual(result["catalog_skipped_parts"], 1)
            self.assertEqual(archive.ranged, [])
            self.assertEqual(archive.downloaded, [])

    def test_unknown_part_negative_probe_avoids_repeated_oss_range(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            part = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-migration-probe",
                epoch_us=epoch_us,
                part_key="000001",
            )
            archive = FakeArchive()
            uploaded = archive.upload_part(part)
            store.mark_part_archived(
                part["path"],
                oss_key=uploaded["oss_key"],
                oss_etag=uploaded["oss_etag"],
            )
            Path(part["path"]).unlink()
            storage.search_index.remove_part(part["path"])
            query = {
                "start_epoch_us": epoch_us,
                "end_epoch_us": epoch_us,
                "keyword": "missing_migration_value",
                "limit": 10,
            }

            first = storage.query_events_tiered(query, settings, archive)
            second = storage.query_events_tiered(query, settings, archive)

            self.assertEqual(first["rows"], [])
            self.assertEqual(second["rows"], [])
            self.assertEqual(len(archive.ranged), 1)
            self.assertTrue(second["query_certificate_hit"])

    def test_positive_probe_reuses_exact_rows_without_oss_range(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            part = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-positive-probe",
                epoch_us=epoch_us,
                part_key="000001",
            )
            archive = FakeArchive()
            uploaded = archive.upload_part(part)
            store.mark_part_archived(
                part["path"],
                oss_key=uploaded["oss_key"],
                oss_etag=uploaded["oss_etag"],
            )
            Path(part["path"]).unlink()
            storage.search_index.remove_part(part["path"])
            query = {
                "start_epoch_us": epoch_us,
                "end_epoch_us": epoch_us,
                "table": "orders",
                "limit": 10,
            }

            first = storage.query_events_tiered(query, settings, archive)
            first_range_count = len(archive.ranged)
            second = storage.query_events_tiered(query, settings, archive)

            self.assertEqual(
                [row["event_id"] for row in first["rows"]],
                ["event-positive-probe"],
            )
            self.assertEqual(second["rows"], first["rows"])
            self.assertGreater(first_range_count, 0)
            self.assertEqual(len(archive.ranged), first_range_count)
            self.assertTrue(second["query_certificate_hit"])
            self.assertEqual(second["range_requests"], 0)

    def test_complete_query_certificate_skips_part_enumeration(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-query-certificate",
                epoch_us=epoch_us,
                part_key="000001",
            )
            query = {
                "start_epoch_us": epoch_us,
                "end_epoch_us": epoch_us,
                "table": "orders",
                "limit": 10,
            }

            first = storage.query_events_tiered(query, settings, None)
            with patch.object(
                store,
                "parts_in_range",
                side_effect=AssertionError("certificate must precede enumeration"),
            ):
                second = storage.query_events_tiered(query, settings, None)

            self.assertEqual(second["rows"], first["rows"])
            self.assertTrue(second["query_certificate_hit"])
            self.assertEqual(second["query_cache_parts_read"], 0)

    def test_query_certificate_invalidates_for_new_part_in_window(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-certificate-before",
                epoch_us=epoch_us,
                part_key="000001",
            )
            query = {
                "start_epoch_us": epoch_us - 1,
                "end_epoch_us": epoch_us,
                "table": "orders",
                "limit": 10,
            }
            storage.query_events_tiered(query, settings, None)

            self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-certificate-after",
                epoch_us=epoch_us - 1,
                part_key="000002",
            )
            result = storage.query_events_tiered(query, settings, None)

            self.assertFalse(result["query_certificate_hit"])
            self.assertEqual(
                {row["event_id"] for row in result["rows"]},
                {"event-certificate-before", "event-certificate-after"},
            )

    def test_query_certificate_survives_new_part_outside_window(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-certificate-window",
                epoch_us=epoch_us,
                part_key="000001",
            )
            query = {
                "start_epoch_us": epoch_us,
                "end_epoch_us": epoch_us,
                "table": "orders",
                "limit": 10,
            }
            first = storage.query_events_tiered(query, settings, None)

            self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-certificate-outside",
                epoch_us=epoch_us + 1,
                part_key="000002",
            )
            with patch.object(
                store,
                "parts_in_range",
                side_effect=AssertionError("outside part must not invalidate"),
            ):
                second = storage.query_events_tiered(query, settings, None)

            self.assertEqual(second["rows"], first["rows"])
            self.assertTrue(second["query_certificate_hit"])

    def test_structural_part_negative_probe_avoids_repeated_oss_range(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            part = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-structural-probe",
                epoch_us=epoch_us,
                part_key="000001",
            )
            archive = FakeArchive()
            uploaded = archive.upload_part(part)
            store.mark_part_archived(
                part["path"],
                oss_key=uploaded["oss_key"],
                oss_etag=uploaded["oss_etag"],
            )
            Path(part["path"]).unlink()
            storage.search_index.remove_part(part["path"])
            query = {
                "start_epoch_us": epoch_us,
                "end_epoch_us": epoch_us,
                "table": "orders",
                "keyword": "missing_structural_value",
                "limit": 10,
            }

            first = storage.query_events_tiered(query, settings, archive)
            first_range_count = len(archive.ranged)
            second = storage.query_events_tiered(query, settings, archive)

            self.assertEqual(first["rows"], [])
            self.assertEqual(second["rows"], [])
            self.assertFalse(storage.search_index.is_structural_current(part))
            self.assertGreater(first_range_count, 0)
            self.assertEqual(len(archive.ranged), first_range_count)
            self.assertTrue(second["query_certificate_hit"])

    def test_full_index_false_positive_negative_probe_avoids_repeated_oss_range(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            _, parts = storage.ingest_file(
                file_id=file_id,
                instance_id="rm-test000001",
                host_instance_id=item.host_instance_id,
                source_file_name=item.log_file_name,
                events=[
                    {
                        "event_id": "event-table-only",
                        "event_epoch_us": epoch_us,
                        "database_name": "app",
                        "table_name": "target_table",
                        "operation": "UPDATE",
                        "sql_text": "UPDATE target_table SET value = 1",
                    },
                    {
                        "event_id": "event-keyword-only",
                        "event_epoch_us": epoch_us,
                        "database_name": "app",
                        "table_name": "other_table",
                        "operation": "UPDATE",
                        "sql_text": "UPDATE other_table SET value = 2571634",
                    },
                ],
            )
            part = parts[0]
            storage.search_index.index_parquet(part, part["path"])
            self.assertTrue(storage.search_index.is_current(part))
            archive = FakeArchive()
            uploaded = archive.upload_part(part)
            store.mark_part_archived(
                part["path"],
                oss_key=uploaded["oss_key"],
                oss_etag=uploaded["oss_etag"],
            )
            Path(part["path"]).unlink()
            query = {
                "start_epoch_us": epoch_us,
                "end_epoch_us": epoch_us,
                "table": "target_table",
                "keyword": "2571634",
                "keyword_mode": "AND",
                "limit": 10,
            }

            with (
                patch.object(
                    storage.metadata,
                    "record_negative_probes",
                    wraps=storage.metadata.record_negative_probes,
                ) as record_many,
                patch.object(
                    storage.metadata,
                    "record_negative_probe",
                    side_effect=AssertionError("per-part negative probe commit"),
                ),
            ):
                first = storage.query_events_tiered(query, settings, archive)
                first_range_count = len(archive.ranged)
                second = storage.query_events_tiered(query, settings, archive)

            self.assertEqual(first["rows"], [])
            self.assertEqual(second["rows"], [])
            self.assertGreater(first_range_count, 0)
            self.assertEqual(len(archive.ranged), first_range_count)
            self.assertTrue(second["query_certificate_hit"])
            self.assertEqual(record_many.call_count, 1)

    def test_trigram_index_skips_no_hit_without_oss_read(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            part = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-empty-probe",
                epoch_us=epoch_us,
                part_key="000001",
            )
            archive = FakeArchive()
            uploaded = archive.upload_part(part)
            store.mark_part_archived(
                part["path"],
                oss_key=uploaded["oss_key"],
                oss_etag=uploaded["oss_etag"],
            )
            Path(part["path"]).unlink()
            query = {
                "start_epoch_us": epoch_us,
                "end_epoch_us": epoch_us,
                "keyword": "definitely_missing_keyword",
                "keyword_mode": "AND",
                "limit": 10,
            }

            first = storage.query_events_tiered(query, settings, archive)
            second = storage.query_events_tiered(query, settings, archive)

            self.assertEqual(first["rows"], [])
            self.assertEqual(second["rows"], [])
            self.assertEqual(archive.ranged, [])
            self.assertEqual(archive.downloaded, [])
            self.assertEqual(second["index_skipped_parts"], 1)

    def test_empty_probe_is_not_reused_for_a_wider_time_slice(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            _, parts = storage.ingest_file(
                file_id=file_id,
                instance_id="rm-test000001",
                host_instance_id=item.host_instance_id,
                source_file_name=item.log_file_name,
                events=[
                    {
                        "event_id": "event-before-match",
                        "event_epoch_us": epoch_us,
                        "operation": "QUERY",
                        "sql_text": "SELECT 1",
                    },
                    {
                        "event_id": "event-wider-match",
                        "event_epoch_us": epoch_us + 1,
                        "operation": "QUERY",
                        "sql_text": "SELECT wider_time_match",
                    },
                ],
            )
            part = parts[0]
            archive = FakeArchive()
            uploaded = archive.upload_part(part)
            store.mark_part_archived(
                part["path"],
                oss_key=uploaded["oss_key"],
                oss_etag=uploaded["oss_etag"],
            )
            Path(part["path"]).unlink()

            first = storage.query_events_tiered(
                {
                    "start_epoch_us": epoch_us,
                    "end_epoch_us": epoch_us,
                    "keyword": "wider_time_match",
                    "limit": 10,
                },
                settings,
                archive,
            )
            second = storage.query_events_tiered(
                {
                    "start_epoch_us": epoch_us,
                    "end_epoch_us": epoch_us + 1,
                    "keyword": "wider_time_match",
                    "limit": 10,
                },
                settings,
                archive,
            )

            self.assertEqual(first["rows"], [])
            self.assertEqual(
                [row["event_id"] for row in second["rows"]],
                ["event-wider-match"],
            )
            self.assertEqual(len(archive.ranged), 2)
            self.assertEqual(second["negative_probe_skipped_parts"], 0)

    def test_legacy_query_cache_is_fully_removed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            older = storage.paths["legacy_query_cache"] / ("a" * 64 + ".parquet")
            newer = storage.paths["legacy_query_cache"] / ("b" * 64 + ".parquet")
            older.write_bytes(b"a" * 128)
            newer.write_bytes(b"b" * 128)
            os.utime(older, (1, 1))
            os.utime(newer, (2, 2))

            result = storage.enforce_query_cache_limit(0)

            self.assertEqual(result["evicted_parts"], 2)
            self.assertFalse(older.exists())
            self.assertFalse(newer.exists())

    def test_query_rejects_end_after_latest_before_reading_oss(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            part = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-latest",
                epoch_us=epoch_us,
                part_key="000001",
            )
            archive = FakeArchive()
            uploaded = archive.upload_part(part)
            store.mark_part_archived(
                part["path"],
                oss_key=uploaded["oss_key"],
                oss_etag=uploaded["oss_etag"],
            )
            Path(part["path"]).unlink()

            with self.assertRaises(StorageError) as raised:
                storage.query_events_tiered(
                    {
                        "start_epoch_us": epoch_us - 1,
                        "end_epoch_us": epoch_us + 1,
                        "limit": 10,
                    },
                    settings,
                    archive,
                )
            self.assertEqual(raised.exception.code, "QUERY_END_AFTER_LATEST")
            self.assertIn("当前已解析数据只到", str(raised.exception))
            self.assertEqual(archive.downloaded, [])

    def test_query_reads_newest_oss_parts_first_and_stops_after_page_is_full(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000) - 60_000_000
            archive = FakeArchive()
            for index in range(3):
                part = self._ingest_one(
                    storage,
                    file_id,
                    item,
                    event_id=f"event-{index}",
                    epoch_us=epoch_us + index,
                    part_key=f"{index:06d}",
                )
                uploaded = archive.upload_part(part)
                store.mark_part_archived(
                    part["path"],
                    oss_key=uploaded["oss_key"],
                    oss_etag=uploaded["oss_etag"],
                )
                Path(part["path"]).unlink()

            result = storage.query_events_tiered(
                {
                    "start_epoch_us": epoch_us,
                    "end_epoch_us": epoch_us + 2,
                    "limit": 1,
                },
                settings,
                archive,
            )
            self.assertEqual(
                [row["event_id"] for row in result["rows"]],
                ["event-2"],
            )
            self.assertTrue(result["has_more"])
            self.assertEqual(len(archive.ranged), 2)
            self.assertEqual(archive.downloaded, [])

    def test_csv_export_uses_oss_when_local_part_was_evicted(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000)
            part = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-from-oss",
                epoch_us=epoch_us,
                part_key="000001",
            )
            archive = FakeArchive()
            uploaded = archive.upload_part(part)
            store.mark_part_archived(
                part["path"],
                oss_key=uploaded["oss_key"],
                oss_etag=uploaded["oss_etag"],
            )
            Path(part["path"]).unlink()

            export_path, count = storage.export_csv_tiered(
                {
                    "start_epoch_us": epoch_us - 1,
                    "end_epoch_us": epoch_us,
                },
                settings,
                archive,
            )
            self.assertEqual(count, 1)
            self.assertIn(
                "event-from-oss",
                export_path.read_text(encoding="utf-8-sig"),
            )
            self.assertEqual(len(archive.ranged), 1)
            self.assertEqual(archive.downloaded, [])

    def test_local_body_release_requires_archive_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(time.time() * 1_000_000)
            older = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-older",
                epoch_us=epoch_us,
                part_key="000001",
            )
            newer = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-newer",
                epoch_us=epoch_us + 1,
                part_key="000002",
            )
            archive = FakeArchive()
            uploaded = archive.upload_part(older)
            store.mark_part_archived(
                older["path"],
                oss_key=uploaded["oss_key"],
                oss_etag=uploaded["oss_etag"],
            )

            result = storage.enforce_local_cache_limit(0)
            self.assertEqual(result["evicted_parts"], 1)
            self.assertFalse(Path(older["path"]).exists())
            self.assertTrue(Path(newer["path"]).exists())
            self.assertTrue(
                any(
                    part["path"] == older["path"]
                    for part in store.parts_for_file(file_id)
                )
            )
            second = storage.enforce_local_cache_limit(0)
            self.assertEqual(second["evicted_parts"], 0)
            self.assertTrue(second["blocked_unarchived"])

    def test_local_cache_limit_does_not_scan_all_part_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            part = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-local-only-scan",
                epoch_us=int(time.time() * 1_000_000),
                part_key="local-only",
            )
            archive = FakeArchive()
            uploaded = archive.upload_part(part)
            store.mark_part_archived(
                part["path"],
                oss_key=uploaded["oss_key"],
                oss_etag=uploaded["oss_etag"],
            )

            with patch.object(
                store,
                "list_parts",
                side_effect=AssertionError("cache enforcement performed a full scan"),
            ):
                result = storage.enforce_local_cache_limit(0)

            self.assertEqual(result["evicted_parts"], 1)
            self.assertFalse(Path(part["path"]).exists())

    def test_unindexed_local_body_is_bounded_handoff_not_query_lru(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            part = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-index-handoff",
                epoch_us=int(time.time() * 1_000_000),
                part_key="handoff",
            )
            storage.search_index.remove_part(part["path"])
            archive = FakeArchive()
            uploaded = archive.upload_part(part)
            store.mark_part_archived(
                part["path"],
                oss_key=uploaded["oss_key"],
                oss_etag=uploaded["oss_etag"],
            )

            kept = storage.enforce_local_cache_limit(part["size_bytes"])
            self.assertEqual(kept["evicted_parts"], 0)
            self.assertEqual(kept["blocked_unindexed"], part["size_bytes"])
            self.assertTrue(Path(part["path"]).is_file())

            evicted = storage.enforce_local_cache_limit(0)
            self.assertEqual(evicted["evicted_parts"], 1)
            self.assertEqual(evicted["blocked_unindexed"], 0)
            self.assertFalse(Path(part["path"]).exists())

    def test_retention_removes_expired_oss_only_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-05-01T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            epoch_us = int(
                (datetime.now(UTC) - timedelta(days=61)).timestamp()
                * 1_000_000
            )
            part = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-expired",
                epoch_us=epoch_us,
                part_key="000001",
            )
            archive = FakeArchive()
            uploaded = archive.upload_part(part)
            store.mark_part_archived(
                part["path"],
                oss_key=uploaded["oss_key"],
                oss_etag=uploaded["oss_etag"],
            )
            Path(part["path"]).unlink()

            result = storage.cleanup(60, archive_enabled=True)
            self.assertEqual(result["deleted_parts"], 1)
            self.assertEqual(store.parts_for_file(file_id), [])
            self.assertIn(uploaded["oss_key"], archive.objects)

    def test_raw_binlog_is_kept_until_every_part_is_archived(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            part = self._ingest_one(
                storage,
                file_id,
                item,
                event_id="event-1",
                epoch_us=int(time.time() * 1_000_000),
                part_key="000001",
            )
            raw = storage.paths["downloads"] / f"{file_id}.binlog"
            raw.write_bytes(b"raw")
            store.set_file_state(file_id, "stored", event_count=1)
            manager = SyncManager(store, storage)
            try:
                with self.assertRaises(StorageError):
                    manager._finish_stored_file(file_id, raw, settings)
                self.assertTrue(raw.exists())
                archive = FakeArchive()
                uploaded = archive.upload_part(part)
                store.mark_part_archived(
                    part["path"],
                    oss_key=uploaded["oss_key"],
                    oss_etag=uploaded["oss_etag"],
                )
                manager._finish_stored_file(file_id, raw, settings)
                self.assertFalse(raw.exists())
                self.assertEqual(store.file_record(file_id)["state"], "done")
            finally:
                manager.shutdown()

    def test_missing_rds_range_returns_explicit_message(self) -> None:
        class EmptyRdsClient:
            @staticmethod
            def verify_instance():
                return {"dbInstanceId": "rm-test000001"}

            @staticmethod
            def primary_host_instance_id():
                return "host-a"

            @staticmethod
            def list_binlogs(_start, _end):
                return []

        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            store.save_settings(oss_settings(oss_enabled=False))
            storage = EventStorage(store, data_root)
            manager = SyncManager(
                store,
                storage,
                client_factory=lambda _settings, _credential: EmptyRdsClient(),
                credential_loader=lambda _target: CloudCredential("id", "secret"),
            )
            try:
                with self.assertRaises(PipelineError) as raised:
                    manager.request_backfill_for_range(
                        1_775_000_000_000_000,
                        1_775_003_600_000_000,
                    )
                self.assertEqual(
                    raised.exception.code,
                    "BINLOG_RANGE_NOT_FOUND",
                )
                self.assertIn("实例未找到", str(raised.exception))
                self.assertIn("范围的 Binlog", str(raised.exception))
            finally:
                manager.shutdown()


class QueryIsolationTests(unittest.TestCase):
    def test_analytics_manifest_commit_is_single_writer(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            index = AnalyticsIndex(Path(directory) / "analytics")
            guard = threading.Lock()
            first_entered = threading.Event()
            second_entered = threading.Event()
            active_writers = 0
            max_active_writers = 0
            begin_count = 0
            errors: list[BaseException] = []

            class FakeConnection:
                def execute(self, sql: str, _params: object = ()) -> "FakeConnection":
                    nonlocal active_writers, max_active_writers, begin_count
                    statement = " ".join(sql.split())
                    if statement == "BEGIN IMMEDIATE":
                        with guard:
                            begin_count += 1
                            position = begin_count
                            active_writers += 1
                            max_active_writers = max(
                                max_active_writers,
                                active_writers,
                            )
                        if position == 1:
                            first_entered.set()
                            second_entered.wait(0.5)
                        else:
                            second_entered.set()
                    elif statement in {"COMMIT", "ROLLBACK"}:
                        with guard:
                            active_writers -= 1
                    return self

                def executemany(
                    self,
                    _sql: str,
                    _rows: object,
                ) -> "FakeConnection":
                    return self

            @contextmanager
            def fake_connection():
                yield FakeConnection()

            part = {
                "path": "events/test.parquet",
                "logical_part_id": "logical-test",
                "object_sha256": "sha-test",
                "event_date": "2026-08-26",
            }
            payload = {
                "statements": {},
                "sql_rows": [],
                "txn_buckets": [],
                "txn_tops": [],
                "hot_rows": [],
                "ddl_rows": [],
                "row_count": 1,
                "min_event_epoch_us": 1,
                "max_event_epoch_us": 2,
                "txn_count": 1,
                "sql_mode": "statement",
                "hot_mode": "primary-key",
                "hot_tables": 1,
            }

            def commit() -> None:
                try:
                    index._commit(part, payload)
                except BaseException as exc:
                    errors.append(exc)

            with patch.object(index, "connection", side_effect=fake_connection):
                first = threading.Thread(target=commit)
                second = threading.Thread(target=commit)
                first.start()
                self.assertTrue(first_entered.wait(1))
                second.start()
                first.join(3)
                second.join(3)

            self.assertFalse(first.is_alive())
            self.assertFalse(second.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(max_active_writers, 1)

    def test_public_status_never_runs_full_storage_or_index_scans(self) -> None:
        settings = Settings(retention_days=30, oss_retention_days=60)

        class Metadata:
            @staticmethod
            def load_settings():
                return settings

            @staticmethod
            def storage_metadata_stats():
                return {
                    "event_count": 123,
                    "parquet_bytes": 456,
                    "archived_bytes": 400,
                    "oldest_epoch_us": 10,
                    "latest_epoch_us": 20,
                    "part_count": 10,
                    "archived_part_count": 9,
                    "files": {},
                }

        class Storage:
            @staticmethod
            def stats(_retention_days):
                raise AssertionError("public status must not run a full storage scan")

        class Sync:
            @staticmethod
            def status():
                return {
                    "running": True,
                    "latestJob": {},
                    "index": {
                        "part_count": 8,
                        "block_count": 16,
                        "size_bytes": 2048,
                        "localBodyBytes": 1024,
                        "catalog": {
                            "catalogedParts": 7,
                            "totalParts": 10,
                        },
                    },
                }

        application = Application.__new__(Application)
        application.metadata = Metadata()
        application.storage = Storage()
        application.sync = Sync()
        with patch("app.server.credential_status", return_value={"configured": True}):
            result = application.public_status()
        self.assertEqual(result["summary"]["eventCount"], 123)
        self.assertEqual(result["summary"]["localBodyBytes"], 1024)
        self.assertEqual(result["summary"]["indexedParts"], 8)
        self.assertEqual(result["summary"]["indexCoverage"], 0.8)
        self.assertEqual(result["summary"]["catalogCoverage"], 0.7)

    def test_status_local_size_does_not_stat_every_oss_only_part(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            local = storage.paths["events"] / "event_date=2026-07-30" / "local.parquet"
            local.parent.mkdir(parents=True, exist_ok=True)
            local.write_bytes(b"local")
            with (
                patch.object(
                    store,
                    "storage_metadata_stats",
                    return_value={
                        "event_count": 1,
                        "parquet_bytes": 5,
                        "archived_bytes": 5,
                        "oldest_epoch_us": 1,
                        "latest_epoch_us": 1,
                        "part_count": 201,
                        "archived_part_count": 201,
                        "files": {},
                    },
                ),
                patch.object(store, "list_parts", return_value=[]),
                patch.object(
                    store,
                    "part_paths",
                    side_effect=AssertionError("must not scan all metadata paths"),
                ),
            ):
                result = storage.stats(60)
            self.assertEqual(result["local_parquet_bytes"], 5)
            self.assertEqual(result["local_part_count"], 1)

    def test_storage_uses_persisted_index_stats_while_writer_is_busy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            status_path = (
                storage.paths["index"] / "index-worker-status.json"
            )
            status_path.write_text(
                json.dumps(
                    {
                        "updatedAt": "2026-08-21T07:00:00Z",
                        "index": {
                            "schema_version": 2,
                            "part_count": 12,
                            "structural_part_count": 34,
                            "block_count": 56,
                            "row_count": 78,
                            "size_bytes": 90,
                            "last_indexed_at": "2026-08-21T06:59:00Z",
                            "last_structural_indexed_at": (
                                "2026-08-21T06:58:00Z"
                            ),
                        },
                        "analytics": {
                            "parts": 34,
                            "rows": 56,
                            "transactions": 78,
                            "statements": 90,
                            "degraded_parts": 0,
                            "min_event_epoch_us": 1,
                            "max_event_epoch_us": 2,
                            "index_bytes": 123,
                        },
                    }
                ),
                encoding="utf-8",
            )
            with patch.object(
                storage.search_index,
                "stats",
                side_effect=AssertionError(
                    "HTTP storage path must not count the busy FTS database"
                ),
            ), patch.object(
                storage.analytics_index,
                "stats",
                side_effect=AssertionError(
                    "HTTP storage path must not count the busy analytics database"
                ),
            ):
                result = storage.stats(60)
            self.assertEqual(result["index"]["part_count"], 12)
            self.assertEqual(
                result["index"]["stats_source"], "index-worker-status"
            )
            self.assertEqual(
                result["index"]["stats_updated_at"],
                "2026-08-21T07:00:00Z",
            )
            self.assertEqual(result["analytics"]["parts"], 34)
            self.assertEqual(
                result["analytics"]["stats_source"],
                "index-worker-status",
            )

    def test_storage_stats_returns_stale_snapshot_during_single_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            previous = {"part_count": 7, "parts": [{"part_id": "old"}]}
            storage._publish_storage_stats_snapshot(
                previous,
                retention_days=60,
                persist=False,
            )
            storage._storage_stats_refreshed_monotonic = time.monotonic() - 60
            entered = threading.Event()
            release = threading.Event()
            calls: list[int] = []

            def refresh(retention_days: int) -> dict:
                calls.append(retention_days)
                entered.set()
                self.assertTrue(release.wait(2))
                return {"part_count": 8, "parts": [{"part_id": "new"}]}

            with patch.object(
                storage,
                "_build_storage_stats_snapshot",
                side_effect=refresh,
            ):
                started = time.perf_counter()
                first = storage.stats(60)
                elapsed = time.perf_counter() - started
                self.assertLess(elapsed, 0.2)
                self.assertEqual(first, previous)
                self.assertIsNot(first, previous)
                self.assertTrue(entered.wait(1))
                self.assertEqual(storage.stats(60), previous)
                self.assertEqual(calls, [60])
                release.set()
                deadline = time.monotonic() + 2
                while storage._storage_stats_refreshing and time.monotonic() < deadline:
                    time.sleep(0.01)
                self.assertFalse(storage._storage_stats_refreshing)
                self.assertEqual(storage.stats(60)["part_count"], 8)

    def test_storage_stats_loads_fresh_persisted_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            first = EventStorage(store, data_root)
            expected = {"part_count": 11, "parts": []}
            first._publish_storage_stats_snapshot(
                expected,
                retention_days=30,
                persist=True,
            )

            reloaded = EventStorage(store, data_root)
            with patch.object(
                reloaded,
                "_build_storage_stats_snapshot",
                side_effect=AssertionError("fresh snapshot must be served directly"),
            ):
                actual = reloaded.stats(30)
            self.assertEqual(actual, expected)
            self.assertIsNot(actual, expected)

    def test_storage_stats_refresh_failure_keeps_last_good_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            expected = {"part_count": 13, "parts": []}
            storage._publish_storage_stats_snapshot(
                expected,
                retention_days=60,
                persist=False,
            )
            storage._storage_stats_refreshed_monotonic = time.monotonic() - 60

            with patch.object(
                storage,
                "_build_storage_stats_snapshot",
                side_effect=RuntimeError("locked for test"),
            ), self.assertLogs("app.storage", level="ERROR") as captured:
                self.assertEqual(storage.stats(60), expected)
                deadline = time.monotonic() + 2
                while storage._storage_stats_refreshing and time.monotonic() < deadline:
                    time.sleep(0.01)
            self.assertFalse(storage._storage_stats_refreshing)
            storage._storage_stats_refreshed_monotonic = time.monotonic()
            self.assertEqual(storage.stats(60), expected)
            self.assertTrue(
                any("继续提供最后一次成功快照" in line for line in captured.output)
            )

    def test_index_supervisor_persists_analytics_and_slowlog_snapshots(self) -> None:
        from app.index_supervisor import IndexSupervisor

        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            supervisor = IndexSupervisor(
                data_root,
                no_progress_seconds=600,
                idle_seconds=15,
            )
            supervisor._publish(
                "generation-a",
                {
                    "state": "completed",
                    "phase": "analytics",
                    "result": {
                        "analyticsStats": {
                            "parts": 12,
                            "index_bytes": 34,
                        },
                        "slowlogIndexStats": {
                            "indexed_parts": 56,
                            "pending_parts": 0,
                        },
                    },
                },
            )
            reloaded = IndexSupervisor(
                data_root,
                no_progress_seconds=600,
                idle_seconds=15,
            )
            reloaded._publish(
                "generation-b",
                {"state": "starting", "phase": "", "result": {}},
            )
            status = json.loads(reloaded.status_path.read_text("utf-8"))
            self.assertEqual(status["analytics"]["parts"], 12)
            self.assertEqual(status["analytics"]["index_bytes"], 34)
            self.assertEqual(status["slowlog"]["indexed_parts"], 56)
            self.assertEqual(status["slowlog"]["pending_parts"], 0)

    def test_index_supervisor_defers_heavy_worker_until_slowlog_source_ready(self) -> None:
        from app.index_supervisor import IndexSupervisor

        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            supervisor = IndexSupervisor(
                data_root,
                no_progress_seconds=600,
                idle_seconds=15,
                source_priority_enabled=True,
            )
            write_json_status(
                supervisor.source_status_path,
                {
                    "running": True,
                    "state": "paused",
                    "updatedAt": datetime.now(UTC).isoformat(),
                    "stats": {
                        "pending_parts": 0,
                        "failed_parts": 0,
                        "oldest_pending_age_seconds": 0,
                        "reconcile_complete": False,
                    },
                },
            )
            supervisor._sleep_interruptibly = lambda _seconds: setattr(
                supervisor, "stopping", True
            )
            with patch("app.index_supervisor.subprocess.Popen") as spawn:
                supervisor.run()
            spawn.assert_not_called()

            supervisor.stopping = False
            write_json_status(
                supervisor.source_status_path,
                {
                    "running": True,
                    "state": "idle",
                    "updatedAt": datetime.now(UTC).isoformat(),
                    "stats": {
                        "pending_parts": 0,
                        "failed_parts": 0,
                        "oldest_pending_age_seconds": 0,
                        "reconcile_complete": True,
                    },
                },
            )
            self.assertEqual(supervisor._source_priority_error(), "")

    def test_events_without_existing_coverage_never_starts_backfill(self) -> None:
        settings = oss_settings(oss_enabled=False)

        class Metadata:
            @staticmethod
            def load_settings():
                return settings

        class Storage:
            @staticmethod
            def query_events_tiered(_query, _settings, _archive):
                return {
                    "rows": [],
                    "has_more": False,
                    "limit": 100,
                    "offset": 0,
                    "coverage_found": False,
                    "tiers_used": [],
                }

        class Sync:
            def __init__(self):
                self.backfill_calls = 0

            @staticmethod
            def archive_for_settings(_settings):
                return None

            def request_backfill_for_range(self, _start_us, _end_us):
                self.backfill_calls += 1
                return {"message": "unexpected backfill"}

        sync = Sync()
        application = SimpleNamespace(
            metadata=Metadata(),
            storage=Storage(),
            sync=sync,
        )
        httpd = AppHTTPServer(("127.0.0.1", 0), application)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            port = httpd.server_address[1]
            url = (
                f"http://127.0.0.1:{port}/api/events"
                "?startEpochUs=1775000000000000"
                "&endEpochUs=1775003600000000"
            )
            with urllib.request.urlopen(url, timeout=2) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertTrue(payload["ok"])
            self.assertNotIn("backfill", payload["data"])
            self.assertEqual(sync.backfill_calls, 0)
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=2)

    def test_complete_slowlog_analytics_does_not_initialize_oss(self) -> None:
        settings = oss_settings(oss_enabled=True)

        class Metadata:
            @staticmethod
            def load_settings():
                return settings

        class Storage:
            @staticmethod
            def slowlog_query_coverage(_query, _settings):
                return {"complete": True}

            @staticmethod
            def analytics_summary(_query, _settings, archive, **_kwargs):
                if archive is not None:
                    raise AssertionError("complete slow-log insight must stay local")
                return {"coverage": {"complete": True}}

        class Sync:
            @staticmethod
            def archive_for_settings(_settings):
                raise AssertionError("OSS must be initialized lazily")

        application = SimpleNamespace(
            metadata=Metadata(), storage=Storage(), sync=Sync()
        )
        httpd = AppHTTPServer(("127.0.0.1", 0), application)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            port = httpd.server_address[1]
            url = (
                f"http://127.0.0.1:{port}/api/analytics"
                "?source=slowlog&instance=rm-test"
                "&startEpochUs=1775000000000000"
                "&endEpochUs=1775003600000000"
            )
            with urllib.request.urlopen(url, timeout=2) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertTrue(payload["ok"])
            self.assertTrue(payload["data"]["coverage"]["complete"])
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=2)

    def test_complete_slowlog_events_do_not_initialize_oss(self) -> None:
        settings = oss_settings(oss_enabled=True)

        class Metadata:
            @staticmethod
            def load_settings():
                return settings

        class Storage:
            @staticmethod
            def slowlog_query_coverage(_query, _settings):
                return {"complete": True}

            @staticmethod
            def query_events_tiered(_query, _settings, archive):
                if archive is not None:
                    raise AssertionError("complete slow-log query must stay local")
                return {"rows": [], "tiers_used": ["slowlog-index"]}

        class Sync:
            @staticmethod
            def archive_for_settings(_settings):
                raise AssertionError("OSS must be initialized lazily")

        application = SimpleNamespace(
            metadata=Metadata(), storage=Storage(), sync=Sync()
        )
        httpd = AppHTTPServer(("127.0.0.1", 0), application)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            port = httpd.server_address[1]
            url = (
                f"http://127.0.0.1:{port}/api/events"
                "?source=slowlog&instance=rm-test"
                "&startEpochUs=1775000000000000"
                "&endEpochUs=1775003600000000"
            )
            with urllib.request.urlopen(url, timeout=2) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["data"]["tiers_used"], ["slowlog-index"])
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=2)

    def test_slowlog_event_detail_does_not_initialize_oss(self) -> None:
        settings = oss_settings(oss_enabled=True)

        class Metadata:
            @staticmethod
            def load_settings():
                return settings

        class Storage:
            @staticmethod
            def local_execution_event_detail(_event_id, _instance):
                return None

            @staticmethod
            def slowlog_event_detail(event_id, _settings, instance):
                return {
                    "event_id": event_id,
                    "instance_id": instance,
                    "tiers_used": ["slowlog-index"],
                }

            @staticmethod
            def event_detail_tiered(*_args, **_kwargs):
                raise AssertionError("covered detail must not enter fallback")

        class Sync:
            @staticmethod
            def archive_for_settings(_settings):
                raise AssertionError("OSS must be initialized lazily")

        application = SimpleNamespace(
            metadata=Metadata(), storage=Storage(), sync=Sync()
        )
        httpd = AppHTTPServer(("127.0.0.1", 0), application)
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            port = httpd.server_address[1]
            url = f"http://127.0.0.1:{port}/api/event?id=slow-1&instance=rm-test"
            with urllib.request.urlopen(url, timeout=2) as response:
                payload = json.loads(response.read().decode("utf-8"))
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["data"]["instance_id"], "rm-test")
        finally:
            httpd.shutdown()
            httpd.server_close()
            thread.join(timeout=2)


class ArchiveMetadataBatchTests(unittest.TestCase):
    def test_archive_upload_commits_metadata_as_one_batch(self) -> None:
        class Metadata:
            def __init__(self) -> None:
                self.batches: list[list[dict[str, object]]] = []

            def mark_parts_archived(self, rows):
                self.batches.append([dict(row) for row in rows])

            def mark_part_archived(self, *_args, **_kwargs):
                raise AssertionError("per-part metadata commits must not be used")

        class Archive:
            @staticmethod
            def pack_batches(parts, _target_bytes):
                return [parts]

            @staticmethod
            def upload_parts(parts, **_kwargs):
                return [
                    {
                        "oss_key": f"archive/{index}",
                        "oss_etag": f"etag-{index}",
                        "oss_offset": index * 10,
                        "oss_length": 10,
                        "oss_object_sha256": "pack-sha",
                    }
                    for index, _part in enumerate(parts)
                ]

        with tempfile.TemporaryDirectory() as directory:
            metadata = Metadata()
            archived_paths: list[str] = []
            storage = SimpleNamespace(
                paths={"scratch": Path(directory)},
                note_part_archived=(
                    lambda path, **_values: archived_paths.append(path)
                ),
                search_index=SimpleNamespace(
                    is_current=lambda _part: (_ for _ in ()).throw(
                        AssertionError("fresh archive must not open the search index")
                    )
                ),
                release_archived_body=lambda _part: (_ for _ in ()).throw(
                    AssertionError("fresh archive must not release before indexing")
                ),
            )
            manager = SyncManager.__new__(SyncManager)
            manager.metadata = metadata
            manager.storage = storage
            parts = [
                {
                    "path": str(Path(directory) / f"part-{index}.parquet"),
                    "event_date": "2026-08-25",
                    "size_bytes": 10,
                }
                for index in range(2)
            ]

            archived = manager._archive_parts(
                "",
                Settings(),
                Archive(),
                parts,
                event_code="TEST_ARCHIVE",
                fresh=True,
            )

            self.assertEqual(archived, 2)
            self.assertEqual(len(metadata.batches), 1)
            self.assertEqual(len(metadata.batches[0]), 2)
            self.assertEqual(archived_paths, [part["path"] for part in parts])


class PipelinePrefetchTests(unittest.TestCase):
    def test_pipeline_keeps_cpu_heavy_work_to_one_lane(self) -> None:
        self.assertEqual(FILE_PIPELINE_WORKERS, 1)
        self.assertEqual(DOWNLOAD_PIPELINE_WORKERS, 3)
        self.assertEqual(TRANSFORM_PIPELINE_WORKERS, 1)

    def test_archive_pool_and_per_file_backlog_are_bounded(self) -> None:
        self.assertEqual(OSS_ARCHIVE_WORKERS, 4)
        self.assertEqual(OSS_ARCHIVE_BACKLOG_PER_FILE, 5)

    def test_fresh_archive_never_scans_the_global_search_handoff(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            store.save_settings(settings)
            item = remote("mysql-bin.000001", "2026-08-24T16:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            storage = EventStorage(store, data_root)
            path = storage.paths["events"] / "fresh-archive.parquet"
            path.write_bytes(b"fresh-archive")
            part = {
                "path": str(path),
                "event_date": "2026-08-24",
                "row_count": 1,
                "min_event_epoch_us": 1,
                "max_event_epoch_us": 1,
                "size_bytes": path.stat().st_size,
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            }
            store.replace_parts(file_id, [part])
            manager = SyncManager(store, storage, start_scheduler=False)
            archive = FakeArchive()
            try:
                with (
                    patch.object(
                        storage.search_index,
                        "is_current",
                        side_effect=AssertionError(
                            "fresh archive must not open the search index"
                        ),
                    ) as index_probe,
                    patch.object(
                        storage,
                        "enforce_local_cache_limit",
                        side_effect=AssertionError(
                            "fresh archive must not scan the global handoff"
                        ),
                    ) as global_scan,
                    patch.object(
                        manager,
                        "_prefixed_archive",
                        return_value=archive,
                    ),
                ):
                    archived = manager.archive_parts_now(
                        [part],
                        "mysql-slow-log/rm-test/node/",
                    )
                self.assertEqual(archived, 1)
                index_probe.assert_not_called()
                global_scan.assert_not_called()
                self.assertTrue(store.part_by_path(str(path))["oss_key"])
                self.assertTrue(path.is_file())
            finally:
                manager.shutdown()

    def test_discovery_batches_metadata_and_keeps_done_rows_read_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = oss_settings()
            store.save_settings(settings)
            storage = EventStorage(store, data_root)
            manager = SyncManager(store, storage, start_scheduler=False)
            items = [
                remote(
                    f"mysql-bin.{index:06d}",
                    "2026-08-24T16:00:00Z",
                )
                for index in range(200)
            ]
            client = SimpleNamespace(list_binlogs=lambda _start, _end: items)
            try:
                with patch.object(
                    store,
                    "upsert_remote",
                    side_effect=AssertionError(
                        "discovery must not open one transaction per file"
                    ),
                ) as single_upsert:
                    discovered = manager._discover(client, settings)
                self.assertEqual(len(discovered), len(items))
                single_upsert.assert_not_called()

                with store.connection() as connection:
                    connection.execute(
                        "UPDATE binlog_files SET state = 'done', "
                        "download_link = '', intranet_download_link = '', "
                        "link_expired_utc = '', updated_at = 'sentinel'"
                    )
                with patch.object(
                    store,
                    "upsert_remote",
                    side_effect=AssertionError(
                        "discovery must not open one transaction per file"
                    ),
                ) as single_upsert:
                    discovered = manager._discover(client, settings)
                self.assertEqual(discovered, [])
                single_upsert.assert_not_called()
                with store.connection() as connection:
                    unchanged = connection.execute(
                        "SELECT COUNT(*) FROM binlog_files "
                        "WHERE state = 'done' AND updated_at = 'sentinel'"
                    ).fetchone()[0]
                self.assertEqual(unchanged, len(items))
            finally:
                manager.shutdown()

    def test_deferred_file_keeps_raw_until_ordered_commit(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            settings = Settings(
                db_instance_id="rm-test000001",
                auto_sync=False,
            )
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, _ = store.upsert_remote(settings, item)
            raw_path = storage.paths["downloads"] / f"{file_id}.binlog"
            raw_path.write_bytes(b"verified-binlog")
            store.set_file_state(file_id, "stored", event_count=7)
            job_id = store.create_job("sync", settings.db_instance_id)
            manager = SyncManager(
                store,
                storage,
                start_scheduler=False,
            )
            try:
                prepared = manager._process_one(
                    job_id,
                    SimpleNamespace(),
                    settings,
                    file_id,
                    item,
                    "stored",
                    "mysql",
                    None,
                    defer_commit=True,
                    query_visible_event=threading.Event(),
                )
                self.assertIsInstance(prepared, PreparedBinlog)
                self.assertTrue(raw_path.is_file())
                self.assertEqual(store.file_record(file_id)["state"], "stored")

                manager._commit_prepared(job_id, settings, prepared)
                self.assertFalse(raw_path.exists())
                self.assertEqual(store.file_record(file_id)["state"], "done")
            finally:
                manager.shutdown()

    def test_oss_archive_queue_does_not_block_next_parquet_chunk(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            settings = Settings(
                db_instance_id="rm-test000001",
                auto_sync=False,
            )
            item = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            file_id, state = store.upsert_remote(settings, item)
            raw_path = storage.paths["downloads"] / f"{file_id}.binlog"
            raw_path.write_bytes(b"verified-binlog")
            ndjson_paths = [
                storage.paths["staging"] / f"chunk-{index}.ndjson"
                for index in range(2)
            ]
            for path in ndjson_paths:
                path.write_text("{}\n", encoding="utf-8")
            first_archive = Future()
            second_archive = Future()
            second_archive.set_result(1)
            second_ingested = threading.Event()
            archive_calls = 0
            ingest_calls = 0
            manager = SyncManager(
                store,
                storage,
                start_scheduler=False,
            )

            def ingest(**_kwargs):
                nonlocal ingest_calls
                ingest_calls += 1
                if ingest_calls == 2:
                    second_ingested.set()
                return (
                    1,
                    [
                        {
                            "path": str(
                                (
                                    data_root
                                    / f"part-{ingest_calls}.parquet"
                                ).resolve()
                            ),
                            "size_bytes": 1,
                        }
                    ],
                )

            def submit_archive(_parts):
                nonlocal archive_calls
                archive_calls += 1
                return first_archive if archive_calls == 1 else second_archive

            try:
                with (
                    patch(
                        "app.pipeline.parse_ndjson_chunks_buffered",
                        return_value=iter(ndjson_paths),
                    ),
                    patch.object(
                        storage,
                        "ingest_ndjson_file",
                        side_effect=ingest,
                    ),
                    patch.object(
                        storage,
                        "finalize_file_parts",
                        return_value=0,
                    ),
                    ThreadPoolExecutor(max_workers=1) as executor,
                ):
                    result = executor.submit(
                        manager._process_one,
                        "job-test",
                        SimpleNamespace(),
                        settings,
                        file_id,
                        item,
                        state,
                        "mysql",
                        FakeArchive(),
                        prepared_download=(raw_path, "sha256"),
                        defer_commit=True,
                        query_visible_event=threading.Event(),
                        archive_submitter=submit_archive,
                    )
                    self.assertTrue(second_ingested.wait(timeout=2))
                    self.assertFalse(result.done())
                    first_archive.set_result(1)
                    prepared = result.result(timeout=2)
                self.assertIsInstance(prepared, PreparedBinlog)
                self.assertEqual(ingest_calls, 2)
                self.assertEqual(archive_calls, 2)
                self.assertTrue(raw_path.is_file())
            finally:
                if not first_archive.done():
                    first_archive.set_result(1)
                manager.shutdown()

    def test_single_cpu_files_commit_in_source_order(self) -> None:
        class Client:
            @staticmethod
            def verify_instance():
                return {
                    "dbInstanceId": "rm-test000001",
                    "engine": "MySQL",
                    "engineVersion": "8.0",
                }

            @staticmethod
            def primary_host_instance_id():
                return "host-a"

        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            settings = Settings(
                db_instance_id="rm-test000001",
                auto_sync=False,
            )
            entries = [
                remote(
                    f"mysql-bin.{index:06d}",
                    f"2026-07-29T01:{index:02d}:00Z",
                )
                for index in range(1, 4)
            ]
            pending = [
                (*store.upsert_remote(settings, item),)
                for item in entries
            ]
            pending = [
                (file_id, item, state)
                for (file_id, state), item in zip(pending, entries, strict=True)
            ]
            names = [item.log_file_name for item in entries]
            started: set[str] = set()
            started_lock = threading.Lock()
            all_started = threading.Event()
            visibility: dict[str, threading.Event] = {}
            committed: list[str] = []
            job_id = store.create_job("sync", settings.db_instance_id)
            manager = SyncManager(
                store,
                storage,
                client_factory=lambda *_args: Client(),
                start_scheduler=False,
            )

            def download(_job, _client, _settings, file_id, item):
                path = storage.paths["downloads"] / f"{file_id}.binlog"
                path.write_bytes(item.log_file_name.encode())
                return path, "sha256"

            def process(
                _job,
                _client,
                _settings,
                file_id,
                item,
                _prior_state,
                _flavor,
                _archive,
                *,
                prepared_download=None,
                defer_commit=False,
                query_visible_event=None,
                archive_submitter=None,
                transform_submitter=None,
            ):
                self.assertTrue(defer_commit)
                self.assertIsNotNone(prepared_download)
                self.assertIsNotNone(query_visible_event)
                visibility[item.log_file_name] = query_visible_event
                with started_lock:
                    started.add(item.log_file_name)
                    if len(started) == len(entries):
                        all_started.set()
                time.sleep(
                    {
                        names[0]: 0.05,
                        names[1]: 0.02,
                        names[2]: 0.0,
                    }[item.log_file_name]
                )
                return PreparedBinlog(
                    file_id=file_id,
                    item=item,
                    raw_path=prepared_download[0],
                    event_count=1,
                    parse_seconds=0.01,
                )

            def commit(_job, _settings, prepared):
                position = names.index(prepared.item.log_file_name)
                self.assertTrue(visibility[prepared.item.log_file_name].is_set())
                for later in names[position + 1 :]:
                    if later in visibility:
                        self.assertFalse(visibility[later].is_set())
                committed.append(prepared.item.log_file_name)
                store.set_file_state(
                    prepared.file_id,
                    "done",
                    event_count=prepared.event_count,
                    raw_deleted=True,
                )

            discover_calls = 0

            def discover(*_args, **_kwargs):
                nonlocal discover_calls
                discover_calls += 1
                if discover_calls == 1:
                    return pending
                latest = store.latest_job()
                self.assertEqual(latest["current_file"], "")
                self.assertIn("正在确认", latest["message"])
                return []

            try:
                with (
                    patch.object(manager, "_discover", side_effect=discover),
                    patch.object(manager, "_download", side_effect=download),
                    patch.object(manager, "_process_one", side_effect=process),
                    patch.object(manager, "_commit_prepared", side_effect=commit),
                    patch.object(
                        storage,
                        "cleanup",
                        return_value={
                            "deleted_parts": 0,
                            "rewritten_parts": 0,
                            "removed_rows": 0,
                            "errors": [],
                        },
                    ),
                ):
                    manager._run(
                        job_id,
                        settings,
                        CloudCredential("test-id", "test-secret"),
                    )
            finally:
                manager.shutdown()
            self.assertEqual(started, set(names))
            self.assertEqual(committed, names)
            self.assertEqual(store.latest_job()["completed_files"], 3)

    def test_hidden_parallel_parts_do_not_advance_queries_or_latest_time(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            settings = Settings(db_instance_id="rm-test000001", auto_sync=False)
            first = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            second = remote("mysql-bin.000002", "2026-07-29T01:05:00Z")
            first_id, _ = store.upsert_remote(settings, first)
            second_id, _ = store.upsert_remote(settings, second)

            def part(path: Path, minimum: int, maximum: int) -> dict:
                return {
                    "path": str(path.resolve()),
                    "event_date": "2026-07-29",
                    "row_count": 1,
                    "min_event_epoch_us": minimum,
                    "max_event_epoch_us": maximum,
                    "size_bytes": 1,
                    "sha256": f"{maximum:064x}",
                }

            store.upsert_parts(
                first_id,
                [part(data_root / "first.parquet", 100, 100)],
            )
            store.upsert_parts(
                second_id,
                [part(data_root / "second.parquet", 200, 200)],
            )
            store.set_file_visibility(first_id, True)
            store.set_file_visibility(second_id, False)
            self.assertEqual(
                [
                    Path(value["path"]).name
                    for value in store.parts_in_range(
                        start_epoch_us=0,
                        end_epoch_us=1_000,
                    )
                ],
                ["first.parquet"],
            )
            self.assertEqual(store.storage_metadata_stats()["latest_epoch_us"], 100)

            store.set_file_visibility(second_id, True)
            self.assertEqual(
                len(
                    store.parts_in_range(
                        start_epoch_us=0,
                        end_epoch_us=1_000,
                    )
                ),
                2,
            )
            self.assertEqual(store.storage_metadata_stats()["latest_epoch_us"], 200)

    def test_next_download_starts_before_current_parse_finishes(self) -> None:
        class Client:
            @staticmethod
            def verify_instance():
                return {
                    "dbInstanceId": "rm-test000001",
                    "engine": "MySQL",
                    "engineVersion": "8.0",
                }

            @staticmethod
            def primary_host_instance_id():
                return "host-a"

        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            settings = Settings(
                db_instance_id="rm-test000001",
                auto_sync=False,
            )
            first = remote("mysql-bin.000001", "2026-07-29T01:00:00Z")
            second = remote("mysql-bin.000002", "2026-07-29T01:05:00Z")
            first_id, first_state = store.upsert_remote(settings, first)
            second_id, second_state = store.upsert_remote(settings, second)
            pending = [
                (first_id, first, first_state),
                (second_id, second, second_state),
            ]
            second_started = threading.Event()
            release_second = threading.Event()
            observed: list[str] = []
            job_id = store.create_job("sync", settings.db_instance_id)
            manager = SyncManager(
                store,
                storage,
                client_factory=lambda *_args: Client(),
            )

            def download(_job, _client, _settings, file_id, item):
                if file_id == second_id:
                    second_started.set()
                    self.assertTrue(release_second.wait(timeout=2))
                path = storage.paths["downloads"] / f"{file_id}.binlog"
                path.write_bytes(item.log_file_name.encode())
                return path, "sha256"

            def process(
                _job,
                _client,
                _settings,
                file_id,
                item,
                _prior_state,
                _flavor,
                _archive,
                *,
                prepared_download=None,
                defer_commit=False,
                query_visible_event=None,
                archive_submitter=None,
                transform_submitter=None,
            ):
                self.assertIsNotNone(prepared_download)
                if file_id == first_id:
                    self.assertTrue(second_started.wait(timeout=2))
                    release_second.set()
                observed.append(item.log_file_name)
                return PreparedBinlog(
                    file_id=file_id,
                    item=item,
                    raw_path=prepared_download[0],
                    event_count=0,
                    parse_seconds=0.01,
                )

            def commit(_job, _settings, prepared):
                store.set_file_state(
                    prepared.file_id,
                    "done",
                    raw_deleted=True,
                )

            try:
                with (
                    patch.object(manager, "_discover", side_effect=[pending, []]),
                    patch.object(manager, "_download", side_effect=download),
                    patch.object(manager, "_process_one", side_effect=process),
                    patch.object(manager, "_commit_prepared", side_effect=commit),
                    patch.object(
                        storage,
                        "cleanup",
                        return_value={
                            "deleted_parts": 0,
                            "rewritten_parts": 0,
                            "removed_rows": 0,
                            "errors": [],
                        },
                    ),
                ):
                    manager._run(
                        job_id,
                        settings,
                        CloudCredential("test-id", "test-secret"),
                    )
            finally:
                release_second.set()
                manager.shutdown()
            self.assertEqual(
                observed,
                ["mysql-bin.000001", "mysql-bin.000002"],
            )


class ProcessIsolationRecoveryTests(unittest.TestCase):
    def test_slowlog_worker_paces_successful_parts_at_idle_interval(self):
        class _Stopping:
            def __init__(self) -> None:
                self.stopped = False
                self.delays: list[float] = []

            def is_set(self) -> bool:
                return self.stopped

            def set(self) -> None:
                self.stopped = True

            def wait(self, delay: float) -> bool:
                self.delays.append(delay)
                self.stopped = True
                return True

        with tempfile.TemporaryDirectory() as directory:
            worker = SlowLogQueueWorker(Path(directory), idle_seconds=5.0)
            stopping = _Stopping()
            worker.stopping = stopping
            gate = SimpleNamespace(check=lambda: 0.0)
            canary_probes: list[bool] = []
            canary = SimpleNamespace(
                probe=lambda *, force=False: canary_probes.append(bool(force))
            )
            slowlog_index = SimpleNamespace(
                advance_reconcile=lambda **_kwargs: None,
                stats=lambda: {"pending_parts": 1},
            )
            storage = SimpleNamespace(slowlog_index=slowlog_index)
            metadata = SimpleNamespace(
                load_settings=lambda: SimpleNamespace(oss_enabled=False)
            )

            def drain_one(*_args, admission_check=None, **_kwargs):
                self.assertIsNotNone(admission_check)
                admission_check()
                return {"parts": 1, "rows": 2, "failedParts": 0}

            with (
                patch("app.slowlog_worker._bound_arrow"),
                patch(
                    "app.slowlog_worker.ensure_data_dirs",
                    return_value={"index": Path(directory)},
                ),
                patch("app.slowlog_worker.MetadataStore", return_value=metadata),
                patch("app.slowlog_worker.EventStorage", return_value=storage),
                patch(
                    "app.slowlog_worker.IoPressureGate.from_env",
                    return_value=gate,
                ),
                patch(
                    "app.slowlog_worker.HealthCanary",
                    return_value=canary,
                ),
                patch("app.slowlog_worker.write_json_status"),
                patch(
                    "app.slowlog_worker.drain_slowlog_queue_once",
                    side_effect=drain_one,
                ),
            ):
                self.assertEqual(worker.run(), 0)

            self.assertEqual(stopping.delays, [5.0])
            self.assertEqual(canary_probes, [False, True, True])

    def test_slowlog_worker_serving_canary_pauses_before_queue_work(self):
        with tempfile.TemporaryDirectory() as directory:
            worker = SlowLogQueueWorker(Path(directory), idle_seconds=0.2)
            gate = SimpleNamespace(check=lambda: 0.0)

            def pause_once(*, force: bool = False) -> float:
                del force
                worker.stopping.set()
                from app.io_pressure import IoPressurePaused

                raise IoPressurePaused("production health probe failed: timed out")

            canary = SimpleNamespace(probe=pause_once)
            slowlog_index = SimpleNamespace(
                advance_reconcile=lambda **_kwargs: None,
                stats=lambda: {"pending_parts": 1},
            )
            storage = SimpleNamespace(slowlog_index=slowlog_index)
            metadata = SimpleNamespace(
                load_settings=lambda: SimpleNamespace(oss_enabled=False)
            )
            with (
                patch("app.slowlog_worker._bound_arrow"),
                patch(
                    "app.slowlog_worker.ensure_data_dirs",
                    return_value={"index": Path(directory)},
                ),
                patch("app.slowlog_worker.MetadataStore", return_value=metadata),
                patch("app.slowlog_worker.EventStorage", return_value=storage),
                patch(
                    "app.slowlog_worker.IoPressureGate.from_env",
                    return_value=gate,
                ),
                patch(
                    "app.slowlog_worker.HealthCanary",
                    return_value=canary,
                ),
                patch("app.slowlog_worker.write_json_status") as status,
                patch("app.slowlog_worker.drain_slowlog_queue_once") as drain,
            ):
                self.assertEqual(worker.run(), 0)
            drain.assert_not_called()
            states = [call.args[1]["state"] for call in status.call_args_list]
            self.assertIn("paused", states)
            self.assertEqual(states[-1], "stopped")

    def test_slowlog_worker_uses_serving_canary_to_override_false_io_pressure(self):
        with tempfile.TemporaryDirectory() as directory:
            worker = SlowLogQueueWorker(Path(directory), idle_seconds=0.2)
            gate = SimpleNamespace()

            def pause_for_pressure() -> float:
                from app.io_pressure import IoPressurePaused

                worker.stopping.set()
                raise IoPressurePaused(
                    "host I/O pressure exceeded safety ceiling"
                )

            gate.check = pause_for_pressure
            canary_probes: list[bool] = []
            canary = SimpleNamespace(
                probe=lambda *, force=False: canary_probes.append(bool(force))
            )
            slowlog_index = SimpleNamespace(
                advance_reconcile=lambda **_kwargs: None,
                stats=lambda: {"pending_parts": 1},
            )
            storage = SimpleNamespace(slowlog_index=slowlog_index)
            metadata = SimpleNamespace(
                load_settings=lambda: SimpleNamespace(oss_enabled=False)
            )

            def drain_one(*_args, admission_check=None, **_kwargs):
                self.assertIsNotNone(admission_check)
                admission_check()
                worker.stopping.set()
                return {"parts": 1, "rows": 2, "failedParts": 0}

            with (
                patch("app.slowlog_worker._bound_arrow"),
                patch(
                    "app.slowlog_worker.ensure_data_dirs",
                    return_value={"index": Path(directory)},
                ),
                patch("app.slowlog_worker.MetadataStore", return_value=metadata),
                patch("app.slowlog_worker.EventStorage", return_value=storage),
                patch(
                    "app.slowlog_worker.IoPressureGate.from_env",
                    return_value=gate,
                ),
                patch(
                    "app.slowlog_worker.HealthCanary",
                    return_value=canary,
                ),
                patch("app.slowlog_worker.write_json_status") as status,
                patch(
                    "app.slowlog_worker.drain_slowlog_queue_once",
                    side_effect=drain_one,
                ) as drain,
            ):
                self.assertEqual(worker.run(), 0)

            drain.assert_called_once()
            self.assertEqual(canary_probes, [True, True, True])
            completed = [
                call.args[1]
                for call in status.call_args_list
                if call.args[1]["state"] == "completed"
            ]
            self.assertEqual(len(completed), 1)
            self.assertTrue(
                completed[0]["result"]["ioPressureCanaryOverride"]
            )

    def test_slowlog_worker_yields_when_io_and_serving_canary_are_saturated(self):
        with tempfile.TemporaryDirectory() as directory:
            worker = SlowLogQueueWorker(Path(directory), idle_seconds=0.2)
            gate = SimpleNamespace()

            def pause_once() -> float:
                worker.stopping.set()
                from app.io_pressure import IoPressurePaused

                raise IoPressurePaused("host I/O pressure exceeded safety ceiling")

            gate.check = pause_once
            slowlog_index = SimpleNamespace(
                advance_reconcile=lambda **_kwargs: None,
                stats=lambda: {"pending_parts": 0},
            )
            storage = SimpleNamespace(slowlog_index=slowlog_index)
            metadata = SimpleNamespace(
                load_settings=lambda: SimpleNamespace(oss_enabled=False)
            )

            def reject_override(*, force: bool = False) -> float:
                self.assertTrue(force)
                worker.stopping.set()
                from app.io_pressure import IoPressurePaused

                raise IoPressurePaused(
                    "production health probe failed: timed out"
                )

            canary = SimpleNamespace(probe=reject_override)
            with (
                patch.dict(
                    os.environ,
                    {"RDS_BINLOG_SLOWLOG_IO_RECOVERY_RATIO": "0.8"},
                ),
                patch("app.slowlog_worker._bound_arrow"),
                patch(
                    "app.slowlog_worker.ensure_data_dirs",
                    return_value={"index": Path(directory)},
                ),
                patch("app.slowlog_worker.MetadataStore", return_value=metadata),
                patch("app.slowlog_worker.EventStorage", return_value=storage),
                patch(
                    "app.slowlog_worker.IoPressureGate.from_env",
                    return_value=gate,
                ) as gate_factory,
                patch(
                    "app.slowlog_worker.HealthCanary",
                    return_value=canary,
                ),
                patch("app.slowlog_worker.write_json_status") as status,
                patch("app.slowlog_worker.drain_slowlog_queue_once") as drain,
            ):
                self.assertEqual(worker.run(), 0)
            drain.assert_not_called()
            gate_factory.assert_called_once_with(
                "RDS_BINLOG_SLOWLOG_IO_FULL_AVG10_MAX",
                default=10.0,
                recovery_ratio=0.8,
            )
            states = [call.args[1]["state"] for call in status.call_args_list]
            self.assertIn("paused", states)
            self.assertEqual(states[-1], "stopped")

    def test_main_service_opens_metadata_without_runtime_migrations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch(
                "app.server.MetadataStore",
                side_effect=RuntimeError("migration-owner-probe"),
            ) as metadata_store:
                with self.assertRaisesRegex(RuntimeError, "migration-owner-probe"):
                    Application(root)

            metadata_store.assert_called_once_with(
                root / "metadata.sqlite3",
                run_migrations=False,
            )

    def test_slowlog_node_configs_are_distinct_and_build_node_request(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            data_root.mkdir(parents=True)
            (data_root / "slow-log-instances.json").write_text(
                json.dumps(
                    {
                        "instances": [
                            {
                                "instanceId": "rm-test",
                                "nodeId": "pi-node-a",
                                "enabled": True,
                            },
                            {
                                "instanceId": "rm-test",
                                "nodeId": "pi-node-b",
                                "enabled": True,
                            },
                        ]
                    }
                ),
                "utf-8",
            )
            configured = load_slow_log_instances(data_root)
            self.assertEqual(
                [item["nodeId"] for item in configured],
                ["pi-node-a", "pi-node-b"],
            )

            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            collector = SlowLogCollector(
                store,
                storage,
                SlowLogConfig(
                    {
                        "instanceId": "rm-test",
                        "nodeId": "pi-node-a",
                        "enabled": True,
                    }
                ),
                credential_loader=lambda _target: None,
            )
            self.assertEqual(collector.config.node_id, "pi-node-a")
            self.assertEqual(
                collector._request_params(1000, 2000, 3)["NodeId"],
                "pi-node-a",
            )

    def test_slowlog_collectors_use_unique_staging_files_for_same_window(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            collectors = [
                SlowLogCollector(
                    store,
                    storage,
                    SlowLogConfig(
                        {
                            "instanceId": "rm-test",
                            "nodeId": node_id,
                            "enabled": True,
                        }
                    ),
                    credential_loader=lambda _target: None,
                )
                for node_id in ("pi-node-a", "pi-node-b")
            ]
            batches = [
                collector._build_events(
                    [
                        {
                            "Timestamp": 1_787_297_600_000,
                            "SQLText": "SELECT 1",
                            "SqlType": "select",
                            "DBName": "biz",
                            "QueryTime": 10,
                            "RowsExamined": 1,
                            "RowsSent": 1,
                            "SqlId": f"sql-{collector.config.node_id}",
                            "NodeId": collector.config.node_id,
                        }
                    ]
                )
                for collector in collectors
            ]
            staging_paths: list[Path] = []

            def capture_staging_path(**values):
                path = Path(values["ndjson_path"])
                self.assertTrue(path.is_file())
                staging_paths.append(path)
                return 1, []

            with patch.object(
                storage,
                "ingest_ndjson_file",
                side_effect=capture_staging_path,
            ):
                for collector, events in zip(collectors, batches):
                    collector._ingest_batch(events)

            self.assertEqual(len(staging_paths), 2)
            self.assertEqual(len(set(staging_paths)), 2)
            self.assertTrue(all(not path.exists() for path in staging_paths))

    def test_slowlog_collector_only_enqueues_index_work(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            collector = SlowLogCollector(
                store,
                storage,
                SlowLogConfig(
                    {
                        "instanceId": "rm-test",
                        "nodeId": "pi-node-a",
                        "enabled": True,
                        "ossPrefix": "mysql-slow-log/rm-test/",
                    }
                ),
                credential_loader=lambda _target: None,
            )
            events = collector._build_events(
                [
                    {
                        "Timestamp": 1_787_297_600_000,
                        "SQLText": "SELECT * FROM orders WHERE id = 1",
                        "SqlType": "select",
                        "DBName": "biz",
                        "TableName": "orders",
                        "QueryTime": 1200,
                        "RowsExamined": 42,
                        "RowsSent": 1,
                        "SqlId": "sql-test",
                        "NodeId": "pi-node-a",
                    }
                ]
            )
            self.assertEqual(len(events), 1)
            self.assertEqual(
                json.loads(events[0]["columns_json"])["node_id"],
                "pi-node-a",
            )
            metrics = json.loads(events[0]["columns_json"])
            profile = metrics["statement_profile"]
            self.assertEqual(profile["format_version"], 2)
            self.assertEqual(profile["action"], "SELECT")
            self.assertNotEqual(profile["fingerprint"], metrics["sql_id"])
            with patch.object(storage, "ensure_slowlog_part") as build:
                collector._ingest_batch(events)
            build.assert_not_called()
            stats = storage.slowlog_index.stats()
            self.assertEqual(stats["pending_parts"], 1)
            self.assertEqual(stats["indexed_parts"], 0)
            with patch.object(
                storage.slowlog_index,
                "enqueue_parts",
                wraps=storage.slowlog_index.enqueue_parts,
            ) as redundant_enqueue:
                result = drain_slowlog_queue_once(store, storage, None)
            redundant_enqueue.assert_not_called()
            self.assertEqual(result["parts"], 1)
            stats = storage.slowlog_index.stats()
            self.assertEqual(stats["pending_parts"], 0)
            self.assertEqual(stats["indexed_parts"], 1)

    def test_slowlog_incomplete_batch_is_not_query_visible(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            collector = SlowLogCollector(
                store,
                storage,
                SlowLogConfig(
                    {
                        "instanceId": "rm-test",
                        "nodeId": "pi-node-a",
                        "enabled": True,
                    }
                ),
                credential_loader=lambda _target: None,
            )
            events = collector._build_events(
                [
                    {
                        "Timestamp": 1_787_297_600_000,
                        "SQLText": "SELECT 1",
                        "SqlType": "select",
                        "DBName": "biz",
                        "QueryTime": 10,
                        "RowsExamined": 1,
                        "RowsSent": 1,
                        "SqlId": "sql-incomplete",
                        "NodeId": "pi-node-a",
                    }
                ]
            )

            with patch.object(
                storage,
                "ingest_ndjson_file",
                side_effect=sqlite3.OperationalError("database is locked"),
            ):
                with self.assertRaisesRegex(
                    sqlite3.OperationalError,
                    "database is locked",
                ):
                    collector._ingest_batch(events)

            with store.connection() as conn:
                record = conn.execute(
                    "SELECT state, query_visible FROM binlog_files "
                    "WHERE host_instance_id = 'slow-log'",
                ).fetchone()
            self.assertEqual(str(record["state"]), "parsing")
            self.assertEqual(int(record["query_visible"]), 0)

    def test_general_log_incomplete_batch_is_not_query_visible(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            collector = GeneralLogCollector(
                store,
                storage,
                GeneralLogConfig(
                    {
                        "instanceId": "rm-test",
                        "enabled": True,
                    }
                ),
            )
            events = [
                {
                    "event_id": "glog-incomplete",
                    "event_epoch_us": 1_787_297_600_000_000,
                    "operation": "SELECT",
                    "database_name": "biz",
                    "sql_kind": "ORIGINAL",
                    "sql_text": "SELECT 1",
                }
            ]

            with patch.object(
                storage,
                "ingest_ndjson_file",
                side_effect=sqlite3.OperationalError("database is locked"),
            ):
                with self.assertRaisesRegex(
                    sqlite3.OperationalError,
                    "database is locked",
                ):
                    collector._ingest_batch(events)

            with store.connection() as conn:
                record = conn.execute(
                    "SELECT state, query_visible FROM binlog_files "
                    "WHERE host_instance_id = 'general-log'",
                ).fetchone()
            self.assertEqual(str(record["state"]), "parsing")
            self.assertEqual(int(record["query_visible"]), 0)

    def test_service_supervisor_restarts_an_unhealthy_child(self) -> None:
        supervisor = ServiceSupervisor(
            [sys.executable, "-c", "import time; time.sleep(30)"],
            "http://127.0.0.1:1/healthz",
            probe_interval=0.05,
            probe_timeout=0.05,
            failure_limit=1,
            startup_grace=0,
        )
        thread = threading.Thread(target=supervisor.run, daemon=True)
        thread.start()
        deadline = time.monotonic() + 5
        try:
            while supervisor.restart_count < 1 and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertGreaterEqual(supervisor.restart_count, 1)
        finally:
            supervisor.request_stop()
            thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        self.assertTrue(supervisor.last_health_error)
        self.assertGreaterEqual(supervisor.last_health_seconds, 0)

    def test_main_sync_manager_never_starts_historical_index_threads(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            data_root = Path(directory) / "data"
            store = MetadataStore(data_root / "metadata.sqlite3")
            storage = EventStorage(store, data_root)
            write_json_status(
                storage.paths["index"] / SUPERVISOR_STATUS_NAME,
                {
                    "supervisor": {
                        "running": True,
                        "watchdogRestarts": 2,
                    },
                    "index": {
                        "external": True,
                        "running": True,
                        "currentPart": "part-test.parquet",
                    },
                    "catalog": {
                        "external": True,
                        "running": False,
                        "currentParts": [],
                    },
                },
            )
            manager = SyncManager(store, storage, start_scheduler=False)
            try:
                self.assertIsNone(manager._indexer)
                self.assertIsNone(manager._cataloger)
                self.assertIsNone(manager._scheduler)
                with patch.object(
                    storage.search_index,
                    "stats",
                    side_effect=AssertionError(
                        "HTTP status must use the external cached snapshot"
                    ),
                ):
                    status = manager.status()["index"]
                self.assertTrue(status["external"])
                self.assertEqual(status["currentPart"], "part-test.parquet")
                self.assertEqual(status["supervisor"]["watchdogRestarts"], 2)
            finally:
                manager.shutdown()

    def test_stalled_native_parser_is_terminated_and_raw_file_is_kept(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "source.binlog"
            source.write_bytes(b"keep-for-retry")
            command = [
                sys.executable,
                "-c",
                "import time; time.sleep(30)",
            ]
            started = time.monotonic()
            with patch("app.parser_bridge._parser_command", return_value=command):
                with self.assertRaises(ParserError) as raised:
                    list(
                        parse_native_ndjson_chunks(
                            source,
                            "stalled-parser",
                            root / "staging",
                            no_progress_seconds=0.1,
                        )
                    )
            self.assertEqual(raised.exception.code, "PARSER_NO_PROGRESS")
            self.assertLess(time.monotonic() - started, 5)
            self.assertEqual(source.read_bytes(), b"keep-for-retry")
            self.assertEqual(list((root / "staging").glob("*.part")), [])

    def test_compose_runs_indexer_as_a_bounded_separate_service(self) -> None:
        compose = (
            Path(__file__).resolve().parents[1] / "compose.yaml"
        ).read_text(encoding="utf-8")
        self.assertIn("  indexer:\n", compose)
        insight = compose.split("  insight:\n", 1)[1].split("  indexer:\n", 1)[0]
        indexer = compose.split("  indexer:\n", 1)[1]
        self.assertIn("app.index_supervisor", compose)
        self.assertIn("mem_limit: 5g", compose)
        self.assertIn('cpus: "2.0"', compose)
        self.assertIn("blkio_config:", insight)
        self.assertIn("weight: 500", insight)
        self.assertIn("mem_limit: 8g", indexer)
        self.assertIn('cpus: "1.0"', indexer)
        self.assertIn("blkio_config:", indexer)
        self.assertIn("weight: 10", indexer)
        # Docker io.max throttling on containers sharing one ext4 filesystem
        # can block cgroup writeback ownership changes and accumulate runc
        # health checks in D state. Keep relative weights and application-level
        # pacing, but never recreate the per-device hard limits.
        self.assertNotIn("device_read_bps:", compose)
        self.assertNotIn("device_write_bps:", compose)
        self.assertNotIn("device_read_iops:", compose)
        self.assertNotIn("device_write_iops:", compose)
        self.assertIn("      - ionice\n", indexer)
        self.assertIn('      - "3"\n', indexer)
        self.assertIn("      - nice\n", indexer)
        self.assertIn('      - "10"\n', indexer)
        self.assertIn("RDS_BINLOG_SLOWLOG_BATCH", compose)
        self.assertIn("RDS_BINLOG_SLOWLOG_WORKERS", compose)
        self.assertIn("RDS_BINLOG_PARSER_NO_PROGRESS_SECONDS", compose)


@unittest.skipUnless(os.name == "nt", "Windows launcher test")
class WindowsLauncherTests(unittest.TestCase):
    def test_chromium_app_browser_is_discoverable(self) -> None:
        from app.launcher import _browser_path

        browser = _browser_path()
        self.assertIsNotNone(browser)
        self.assertTrue(browser.is_file())


if __name__ == "__main__":
    unittest.main()
