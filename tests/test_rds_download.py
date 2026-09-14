"""Direct Binlog downloads must distinguish list access from download access."""
from __future__ import annotations

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
from app.pipeline import SyncManager
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

    def test_permission_failure_stops_instead_of_retrying_or_downloading(self):
        manager = manager_stub()
        manager._refresh_item = Mock()
        with patch("app.pipeline.download_file") as download, self.assertLogs("app.rds_api", "WARNING"), self.assertRaises(RdsApiError):
            manager._download("fixture-job", Mock(), settings(), "fixture-id", remote("sub account not auth permission"))
        download.assert_not_called()
        manager._refresh_item.assert_not_called()


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
