"""Direct Binlog downloads must distinguish list access from download access."""
from __future__ import annotations

import tempfile
import threading
import unittest
import urllib.error
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app.config import Settings
from app.credentials import CloudCredential
from app.downloader import DownloadError
from app.metadata import MetadataStore
from app.pipeline import SyncManager
from app.storage import EventStorage
from app.rds_api import RdsApiError, RemoteBinlog
from app.server import RequestHandler


def settings():
    return Settings(db_instance_id="rm-fixture0001")


def remote(url="https://fixture.invalid/internal?signature=fixture-link"):
    return RemoteBinlog(
        "mysql-bin.000001", "2026-09-01T01:00:00Z", "2026-09-01T01:01:00Z",
        123, "456", "https://fixture.invalid/public", url, "", "Completed",
        "fixture-host", "fixture-request",
    )


def manager_stub():
    manager = SyncManager.__new__(SyncManager)
    manager._settings = Mock(return_value=settings())
    manager.credential_loader = Mock(return_value=CloudCredential("fixture-id", "fixture-secret"))
    manager.metadata = Mock()
    manager._client_refresh_lock = threading.Lock()
    manager._event = Mock()
    manager.storage = SimpleNamespace(paths={"downloads": Path("fixture-downloads")})
    return manager


class IntranetDownloadTests(unittest.TestCase):
    def test_invalid_and_denied_links_never_reach_network_or_public_fallback(self):
        cases = [
            ("", "INTRANET_DOWNLOAD_LINK_MISSING"),
            ("sub account not auth permission", "BINLOG_DOWNLOAD_FORBIDDEN"),
            ("file:///tmp/fixture", "INTRANET_DOWNLOAD_LINK_INVALID"),
            ("https:///fixture", "INTRANET_DOWNLOAD_LINK_INVALID"),
            ("https://user:fixture-secret@fixture.invalid/", "INTRANET_DOWNLOAD_LINK_INVALID"),
            ("https://fixture.invalid/path\n?signature=fixture-secret", "INTRANET_DOWNLOAD_LINK_INVALID"),
        ]
        with patch("app.rds_api.urllib.request.build_opener") as opener:
            for url, code in cases:
                with self.subTest(code=code), self.assertLogs("app.rds_api", "WARNING") as logs, self.assertRaises(RdsApiError) as raised:
                    remote(url).probe_intranet_download()
                self.assertEqual(raised.exception.code, code)
                self.assertEqual(raised.exception.request_id, "fixture-request")
                self.assertNotIn("fixture-secret", str(logs.output) + str(raised.exception))
            opener.assert_not_called()

    def test_probe_reads_only_binlog_header_and_disables_proxy_redirects(self):
        with patch("app.rds_api.urllib.request.build_opener") as build:
            response = build.return_value.open.return_value.__enter__.return_value
            response.getcode.return_value = 206
            response.read.return_value = b"\xfebin"
            result = remote().probe_intranet_download()
            response.read.assert_called_once_with(4)
            req = build.return_value.open.call_args.args[0]
            self.assertEqual(req.get_header("Range"), "bytes=0-3")
            self.assertEqual(result["bytesRead"], 4)
            self.assertTrue(result["verified"])
            self.assertNotIn("signature", str(result))
            proxy, redirect, _https = build.call_args.args
            self.assertEqual(proxy.proxies, {})
            with self.assertRaises(RdsApiError) as raised:
                redirect.redirect_request(req, None, 302, "redirect", {}, "https://fixture.invalid/public")
            self.assertEqual(raised.exception.code, "INTRANET_DOWNLOAD_REDIRECT")

    def test_http_success_with_wrong_body_is_not_download_success(self):
        with patch("app.rds_api.urllib.request.build_opener") as build:
            response = build.return_value.open.return_value.__enter__.return_value
            response.getcode.return_value = 200
            response.read.return_value = b"<htm"
            with self.assertRaises(RdsApiError) as raised:
                remote().probe_intranet_download()
            self.assertEqual(raised.exception.code, "BINLOG_DOWNLOAD_PROBE_INVALID")

    def test_probe_network_error_does_not_disclose_link(self):
        with patch("app.rds_api.urllib.request.build_opener") as build:
            build.return_value.open.side_effect = urllib.error.URLError("sensitive-fixture-link")
            with self.assertRaises(RdsApiError) as raised:
                remote().probe_intranet_download()
            self.assertNotIn("sensitive-fixture-link", str(raised.exception))


class PipelineDownloadTests(unittest.TestCase):
    def test_backfill_does_not_claim_available_when_download_denied(self):
        manager = manager_stub()
        manager.client_factory = Mock()
        client = manager.client_factory.return_value
        client.primary_host_instance_id.return_value = "fixture-host"
        client.list_binlogs.return_value = [remote("sub account not auth permission")]
        manager.start = Mock()
        begin = int(datetime(2026, 9, 1, 1, tzinfo=UTC).timestamp() * 1_000_000)
        with self.assertLogs("app.rds_api", "WARNING"), self.assertRaises(RdsApiError) as raised:
            manager.request_backfill_for_range(begin, begin + 60_000_000)
        self.assertEqual(raised.exception.code, "BINLOG_DOWNLOAD_FORBIDDEN")
        manager.start.assert_not_called()

    def test_expired_url_refresh_uses_existing_pipeline_and_checksums(self):
        manager = manager_stub()
        refreshed = replace(remote(), intranet_download_link="https://fixture.invalid/internal?signature=renewed-fixture")
        manager._refresh_item = Mock(return_value=refreshed)
        result = SimpleNamespace(path=Path("fixture.binlog"), size_bytes=123, sha256="fixture-sha")
        with patch("app.pipeline.download_file", side_effect=[DownloadError("expired", "LINK_EXPIRED"), result]) as download:
            path, sha = manager._download("fixture-job", Mock(), settings(), "fixture-id", remote())
        self.assertEqual((path, sha), (result.path, result.sha256))
        self.assertEqual(download.call_count, 2)
        self.assertEqual(download.call_args.args[0], refreshed.intranet_download_link)
        self.assertEqual(download.call_args.kwargs["expected_crc64"], "456")
        self.assertEqual(download.call_args.kwargs["expected_size"], 123)
        manager._refresh_item.assert_called_once()

    def test_missing_url_refreshes_once(self):
        manager = manager_stub()
        manager._refresh_item = Mock(return_value=remote())
        result = SimpleNamespace(path=Path("fixture.binlog"), size_bytes=123, sha256="fixture-sha")
        with patch("app.pipeline.download_file", return_value=result) as download, self.assertLogs("app.rds_api", "WARNING"):
            manager._download("fixture-job", Mock(), settings(), "fixture-id", remote(""))
        download.assert_called_once()
        manager._refresh_item.assert_called_once()

    def test_cached_denial_refreshes_once_and_preserves_download_validation(self):
        manager = manager_stub()
        stale = remote("sub account not auth permission")
        manager._refresh_item = Mock(return_value=remote())
        result = SimpleNamespace(path=Path("fixture.binlog"), size_bytes=123, sha256="fixture-sha")
        with patch("app.pipeline.download_file", return_value=result) as download, self.assertLogs("app.rds_api", "WARNING"):
            self.assertEqual(manager._download("fixture-job", Mock(), settings(), "fixture-id", stale),
                             (result.path, result.sha256))
        manager._refresh_item.assert_called_once()
        download.assert_called_once()
        self.assertEqual(download.call_args.args[0], remote().intranet_download_link)
        self.assertEqual(download.call_args.kwargs["expected_size"], 123)
        self.assertEqual(download.call_args.kwargs["expected_crc64"], "456")
        manager._event.assert_any_call("fixture-job", "warning", "DOWNLOAD_LINK_REFRESH",
                                      "mysql-bin.000001 下载链接不可用，刷新一次")

    def test_fresh_permission_failure_stops_after_one_refresh_without_downloading(self):
        manager = manager_stub()
        denied = remote("sub account not auth permission")
        manager._refresh_item = Mock(return_value=denied)
        with patch("app.pipeline.download_file") as download, self.assertLogs("app.rds_api", "WARNING"), self.assertRaises(RdsApiError) as raised:
            manager._download("fixture-job", Mock(), settings(), "fixture-id", denied)
        self.assertEqual(raised.exception.code, "BINLOG_DOWNLOAD_FORBIDDEN")
        self.assertEqual(raised.exception.request_id, "fixture-request")
        download.assert_not_called()
        manager._refresh_item.assert_called_once()

    def test_refresh_api_failure_is_not_misclassified_as_missing(self):
        manager = manager_stub()
        manager._refresh_item = Mock(side_effect=RdsApiError("API denied", code="Forbidden.RAM", request_id="fixture-request"))
        with patch("app.pipeline.download_file") as download, self.assertLogs("app.rds_api", "WARNING"), self.assertRaises(RdsApiError) as raised:
            manager._download("fixture-job", Mock(), settings(), "fixture-id", remote("sub account not auth permission"))
        self.assertEqual(raised.exception.code, "Forbidden.RAM")
        download.assert_not_called()
        manager._refresh_item.assert_called_once()

    def test_cached_denial_missing_at_source_preserves_partial_and_continues_raw_lane(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metadata = MetadataStore(root / "metadata.sqlite3")
            storage = EventStorage(metadata, root)
            manager = SyncManager(metadata, storage)
            stale = remote("sub account not auth permission")
            later = replace(remote(), log_file_name="mysql-bin.000002",
                            log_begin_utc="2026-09-01T01:01:00Z", log_end_utc="2026-09-01T01:02:00Z")
            old_id, _ = metadata.upsert_remote(settings(), stale)
            new_id, _ = metadata.upsert_remote(settings(), later)
            partial = storage.paths["downloads"] / f"{old_id}.binlog.part"
            partial.write_bytes(b"preserve-checkpoint")
            job = metadata.create_job("sync", settings().db_instance_id)
            client = Mock()
            client.list_binlogs.return_value = [later]
            result = SimpleNamespace(path=root / "fixture.binlog", size_bytes=123, sha256="fixture-sha")
            processed = []

            def archive(_job, _client, _settings, file_id, item, *_args, **_kwargs):
                processed.append(item.log_file_name)
                metadata.set_file_state(file_id, "done")

            try:
                with patch("app.pipeline.download_file", return_value=result) as download, \
                     patch.object(manager, "archive_for_settings", return_value=Mock()), \
                     patch.object(manager, "_process_one", side_effect=archive), \
                     self.assertLogs("app.rds_api", "WARNING"):
                    counts = manager._run_pending_raw(job, client, settings(),
                        [(old_id, stale, "discovered"), (new_id, later, "discovered")],
                        "mysql", Mock(), completed=0, unavailable=0)
                self.assertEqual(counts, (1, 1, False))
                client.list_binlogs.assert_called_once()
                download.assert_called_once()
                self.assertEqual(processed, [later.log_file_name])
                self.assertEqual(metadata.file_record(old_id)["state"], "unavailable")
                self.assertEqual(metadata.file_record(old_id)["error_code"], "INSTANCE_BINLOG_NOT_FOUND")
                self.assertEqual(metadata.file_record(new_id)["state"], "done")
                self.assertEqual(partial.read_bytes(), b"preserve-checkpoint")
            finally:
                manager.shutdown()


class SettingsDownloadProbeTests(unittest.TestCase):
    def handler(self):
        handler = RequestHandler.__new__(RequestHandler)
        application = Mock()
        application.metadata.load_settings.return_value = settings()
        handler.server = SimpleNamespace(application=application)
        handler.path = "/api/settings/test"
        handler._valid_host = Mock(return_value=True)
        handler._valid_origin = Mock(return_value=True)
        handler._body_json = Mock(return_value={})
        handler._json = Mock()
        handler._error = Mock()
        return handler

    def test_settings_test_requires_actual_intranet_read(self):
        handler = self.handler()
        with patch("app.server.load_credential", return_value=CloudCredential("fixture-id", "fixture-secret")), patch("app.server.RdsRpcClient") as factory, patch.object(RemoteBinlog, "probe_intranet_download", return_value={"verified": True, "bytesRead": 4}) as probe:
            factory.return_value.primary_host_instance_id.return_value = "fixture-host"
            factory.return_value.verify_instance.return_value = {"dbInstanceId": settings().db_instance_id}
            factory.return_value.list_binlogs.return_value = [remote()]
            handler.do_POST()
        probe.assert_called_once()
        handler._error.assert_not_called()
        result = handler._json.call_args.args[0]
        self.assertTrue(result["data"]["intranetDownload"]["verified"])
        self.assertEqual(result["data"]["binlogWindowMinutes"], 10)

    def test_settings_test_reports_denial_with_request_id(self):
        handler = self.handler()
        with patch("app.server.load_credential", return_value=CloudCredential("fixture-id", "fixture-secret")), patch("app.server.RdsRpcClient") as factory, self.assertLogs(level="WARNING"):
            factory.return_value.primary_host_instance_id.return_value = "fixture-host"
            factory.return_value.list_binlogs.return_value = [remote("sub account not auth permission")]
            handler.do_POST()
        handler._json.assert_not_called()
        self.assertEqual(handler._error.call_args.args[1], "BINLOG_DOWNLOAD_FORBIDDEN")
        self.assertIn("fixture-request", handler._error.call_args.args[2])
        handler.app.sync.archive_for_settings.assert_not_called()

    def test_no_completed_binlog_is_unverified_not_success(self):
        handler = self.handler()
        with patch("app.server.load_credential", return_value=CloudCredential("fixture-id", "fixture-secret")), patch("app.server.RdsRpcClient") as factory, self.assertLogs("app.server", "ERROR"):
            factory.return_value.list_binlogs.return_value = []
            handler.do_POST()
        self.assertEqual(handler._error.call_args.args[1], "BINLOG_DOWNLOAD_PROBE_UNAVAILABLE")
        handler._json.assert_not_called()


if __name__ == "__main__":
    unittest.main()
