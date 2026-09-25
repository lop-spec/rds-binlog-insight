"""Direct Binlog downloads must distinguish list access from download access."""
from __future__ import annotations

import hashlib
import io
import multiprocessing
import sys
import tempfile
import threading
import unittest
import urllib.error
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app.config import Settings
from app.credentials import CloudCredential
from app.downloader import DownloadError, download_file, download_raw_cache
from app.parser_bridge import NativeChecksumResult, ParserError
from app.pipeline import SyncManager
from app.raw_cache import RawCacheWriter, inspect_cache, iter_raw
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


class _ChecksumStream:
    def __init__(self):
        self.data = bytearray()
        self.aborted = False

    def update(self, data):
        self.data.extend(data)

    def finish(self):
        return NativeChecksumResult(
            len(self.data), hashlib.sha256(self.data).hexdigest(), "fixture-crc"
        )

    def abort(self):
        self.aborted = True


def _response(data: bytes, status: int = 200, headers: dict[str, str] | None = None):
    response = io.BytesIO(data)
    response.status = status
    response.headers = headers or {}
    return response


def _process_raw_cache_download(payload):
    destination_text, marker_text, raw, source_id = payload
    destination = Path(destination_text)
    marker = Path(marker_text)

    def response(*_args, **_kwargs):
        with marker.open("ab") as handle:
            handle.write(b"network\n")
        return _response(raw)

    with patch(
        "app.downloader.urllib.request.urlopen", side_effect=response
    ), patch("app.downloader.NativeChecksumStream", _ChecksumStream):
        result = download_raw_cache(
            "https://fixture.invalid/source",
            destination,
            source_id=source_id,
            expected_size=len(raw),
            expected_crc64="fixture-crc",
            physical_budget=len(raw),
        )
    return result.sha256


def manager_stub():
    manager = SyncManager.__new__(SyncManager)
    manager._settings = Mock(return_value=settings())
    manager.credential_loader = Mock(return_value=CloudCredential("fixture-id", "fixture-secret"))
    manager.metadata = Mock()
    manager._client_refresh_lock = threading.Lock()
    manager._event = Mock()
    manager.storage = SimpleNamespace(paths={"downloads": Path("fixture-downloads")})
    return manager


class CachedChecksumFailureTests(unittest.TestCase):
    def test_checksum_tool_failures_preserve_cached_bytes_and_original_error(self):
        for code in ('PARSER_EXECUTABLE_MISSING', 'CHECKSUM_PROCESS_FAILED', 'CHECKSUM_OUTPUT_INVALID'):
            with self.subTest(code=code), tempfile.TemporaryDirectory() as root:
                path = Path(root) / 'cached.binlog'
                path.write_bytes(b'cached')
                with patch('app.downloader.checksum_file', side_effect=ParserError('fixture tool failure', code)), patch('app.downloader.urllib.request.urlopen') as network:
                    with self.assertRaises(DownloadError) as raised:
                        download_file('', path, expected_size=6, expected_crc64='')
                self.assertEqual(raised.exception.code, code)
                self.assertEqual(path.read_bytes(), b'cached')
                self.assertEqual(list(Path(root).iterdir()), [path])
                network.assert_not_called()

    def test_actual_corruption_is_quarantined_not_mistaken_for_tool_failure(self):
        for size, crc, expected_code in ((5, '456', 'SIZE_MISMATCH'), (6, '123', 'CRC64_MISMATCH')):
            with self.subTest(code=expected_code), tempfile.TemporaryDirectory() as root:
                path = Path(root) / 'cached.binlog'; path.write_bytes(b'cached')
                with patch('app.downloader.checksum_file', return_value=NativeChecksumResult(size, 'a'*64, crc)), self.assertLogs('app.downloader', 'WARNING') as logged:
                    with self.assertRaises(DownloadError) as raised:
                        download_file('', path, expected_size=6, expected_crc64='456')
                self.assertEqual(raised.exception.code, 'DOWNLOAD_LINK_MISSING')
                self.assertIn(expected_code, str(logged.output))
                self.assertFalse(path.exists())
                self.assertEqual(list(Path(root).glob('*.corrupt-*'))[0].read_bytes(), b'cached')

    def test_416_checksum_tool_failure_preserves_partial(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root) / 'cached.binlog'
            partial = path.with_suffix('.binlog.part'); partial.write_bytes(b'cached')
            error = urllib.error.HTTPError('https://fixture.invalid/', 416, 'range', {}, None)
            with patch('app.downloader.urllib.request.urlopen', side_effect=error), patch('app.downloader.checksum_file', side_effect=ParserError('tool failed', 'CHECKSUM_PROCESS_FAILED')):
                with self.assertRaises(DownloadError) as raised:
                    download_file('https://fixture.invalid/', path, expected_size=6, expected_crc64='')
            self.assertEqual(raised.exception.code, 'CHECKSUM_PROCESS_FAILED')
            self.assertEqual(partial.read_bytes(), b'cached')
            self.assertFalse(path.exists())

    def test_stream_start_failure_closes_http_response(self):
        with tempfile.TemporaryDirectory() as root:
            response = Mock(status=200, headers={})
            with patch('app.downloader.urllib.request.urlopen', return_value=response), patch('app.downloader.NativeChecksumStream', side_effect=ParserError('missing', 'PARSER_EXECUTABLE_MISSING')):
                with self.assertRaises(DownloadError) as raised:
                    download_file('https://fixture.invalid/', Path(root)/'cache', expected_size=6, expected_crc64='')
            self.assertEqual(raised.exception.code, 'PARSER_EXECUTABLE_MISSING')
            response.close.assert_called_once()

    def test_progress_failure_aborts_native_stream_and_keeps_partial(self):
        with tempfile.TemporaryDirectory() as root:
            response = io.BytesIO(b'cached'); response.status = 200
            path = Path(root)/'cache'
            with patch('app.downloader.urllib.request.urlopen', return_value=response), patch('app.downloader.NativeChecksumStream') as stream:
                with self.assertRaisesRegex(RuntimeError, 'progress fixture failure'):
                    download_file('https://fixture.invalid/', path, expected_size=6, expected_crc64='',
                                  progress=Mock(side_effect=RuntimeError('progress fixture failure')))
            stream.return_value.abort.assert_called_once()
            self.assertTrue(response.closed)
            self.assertEqual(path.with_suffix('.part').read_bytes(), b'cached')
            self.assertFalse(path.exists())


@unittest.skipUnless(sys.platform.startswith('linux'), 'shipped native checksum executable requires Linux; exercised in cloud CI')
class NativeDownloadChecksumTests(unittest.TestCase):
    def test_cached_file_is_really_verified_without_network_or_live_url(self):
        with tempfile.TemporaryDirectory() as root:
            path = Path(root)/'cache'; path.write_bytes(b'123456789')
            with patch('app.downloader.urllib.request.urlopen') as network:
                # Independent CRC64/ECMA check vector used by OSS (Go hash/crc64).
                result = download_file('', path, expected_size=9, expected_crc64='11051210869376104954')
            self.assertEqual(result.sha256, hashlib.sha256(b'123456789').hexdigest())
            self.assertEqual(result.size_bytes, 9)
            network.assert_not_called()


class RawCacheDownloadTests(unittest.TestCase):
    SOURCE_ID = "a" * 64

    def test_fresh_download_streams_checksums_and_publishes_complete_cache(self):
        raw = (b"compressible-binlog-source-" * 8000)
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root) / "source.rawcache"
            progress = Mock()
            with patch("app.downloader.urllib.request.urlopen", return_value=_response(raw)), patch(
                "app.downloader.NativeChecksumStream", _ChecksumStream
            ):
                result = download_raw_cache(
                    "https://fixture.invalid/source",
                    destination,
                    source_id=self.SOURCE_ID,
                    expected_size=len(raw),
                    expected_crc64="fixture-crc",
                    physical_budget=len(raw),
                    progress=progress,
                )
            self.assertEqual(result.path, destination)
            self.assertEqual(result.sha256, hashlib.sha256(raw).hexdigest())
            self.assertEqual(
                b"".join(iter_raw(destination, source_id=self.SOURCE_ID, expected_size=len(raw))),
                raw,
            )
            state = inspect_cache(destination, source_id=self.SOURCE_ID, expected_size=len(raw))
            self.assertTrue(state.complete)
            self.assertLess(state.physical_bytes, len(raw))
            self.assertEqual(progress.call_args.args[0], len(raw))
            self.assertEqual(list(Path(root).glob("*.part-*")), [])

    def test_concurrent_calls_share_one_bounded_publication(self):
        raw = b"concurrent-source" * 8000
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root) / "source.rawcache"

            def response(*_args, **_kwargs):
                return _response(raw)

            def download():
                return download_raw_cache(
                    "https://fixture.invalid/source",
                    destination,
                    source_id=self.SOURCE_ID,
                    expected_size=len(raw),
                    expected_crc64="fixture-crc",
                    physical_budget=len(raw),
                )

            with patch(
                "app.downloader.urllib.request.urlopen", side_effect=response
            ) as opened, patch(
                "app.downloader.NativeChecksumStream", _ChecksumStream
            ), ThreadPoolExecutor(max_workers=2) as workers:
                results = list(workers.map(lambda _index: download(), range(2)))
            self.assertEqual(opened.call_count, 1)
            self.assertEqual([result.sha256 for result in results], [
                hashlib.sha256(raw).hexdigest(),
                hashlib.sha256(raw).hexdigest(),
            ])
            self.assertEqual(list(Path(root).glob("*.part-*")), [])

    def test_processes_share_one_cross_process_publication(self):
        raw = b"cross-process-source" * 8000
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root) / "source.rawcache"
            marker = Path(root) / "network-calls.log"
            payload = (str(destination), str(marker), raw, self.SOURCE_ID)
            with ProcessPoolExecutor(
                max_workers=2,
                mp_context=multiprocessing.get_context("spawn"),
            ) as workers:
                results = [
                    worker.result(timeout=30)
                    for worker in [
                        workers.submit(_process_raw_cache_download, payload),
                        workers.submit(_process_raw_cache_download, payload),
                    ]
                ]
            self.assertEqual(
                results,
                [hashlib.sha256(raw).hexdigest(), hashlib.sha256(raw).hexdigest()],
            )
            self.assertEqual(marker.read_text(encoding="ascii").splitlines(), ["network"])
            self.assertEqual(list(Path(root).glob("*.part-*")), [])

    def test_resume_uses_verified_raw_offset_and_exact_content_range(self):
        prefix = b"a" * (64 * 1024)
        suffix = b"b" * 7000
        raw = prefix + suffix
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root) / "source.rawcache"
            partial = Path(root) / (destination.name + ".part-old")
            writer = RawCacheWriter(
                partial,
                source_id=self.SOURCE_ID,
                expected_size=len(raw),
                physical_budget=len(raw) * 2,
                frame_bytes=64 * 1024,
            )
            writer.write(prefix)
            writer.checkpoint()
            writer.close()
            response = _response(
                suffix,
                206,
                {
                    "Content-Range": f"bytes {len(prefix)}-{len(raw)-1}/{len(raw)}",
                    "Content-Length": str(len(suffix)),
                },
            )
            with patch("app.downloader.urllib.request.urlopen", return_value=response) as opened, patch(
                "app.downloader.NativeChecksumStream", _ChecksumStream
            ):
                result = download_raw_cache(
                    "https://fixture.invalid/source",
                    destination,
                    source_id=self.SOURCE_ID,
                    expected_size=len(raw),
                    expected_crc64="fixture-crc",
                    physical_budget=len(raw) * 2,
                )
            request = opened.call_args.args[0]
            self.assertEqual(request.get_header("Range"), f"bytes={len(prefix)}-")
            self.assertEqual(result.sha256, hashlib.sha256(raw).hexdigest())
            self.assertEqual(
                b"".join(iter_raw(destination, source_id=self.SOURCE_ID, expected_size=len(raw))),
                raw,
            )
            self.assertFalse(partial.exists())

    def test_wrong_content_range_fails_closed_without_mutating_recovery(self):
        prefix = b"a" * (64 * 1024)
        raw_size = len(prefix) + 10
        cases = {
            "start": {"Content-Range": f"bytes 0-9/{raw_size}"},
            "end": {
                "Content-Range": f"bytes {len(prefix)}-{raw_size - 2}/{raw_size}",
                "Content-Length": "9",
            },
            "total": {
                "Content-Range": (
                    f"bytes {len(prefix)}-{raw_size - 1}/{raw_size + 1}"
                ),
                "Content-Length": "10",
            },
            "length": {
                "Content-Range": (
                    f"bytes {len(prefix)}-{raw_size - 1}/{raw_size}"
                ),
                "Content-Length": "11",
            },
        }
        for name, headers in cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as root:
                destination = Path(root) / "source.rawcache"
                partial = Path(root) / (destination.name + ".part-old")
                writer = RawCacheWriter(
                    partial,
                    source_id=self.SOURCE_ID,
                    expected_size=raw_size,
                    physical_budget=raw_size * 2,
                    frame_bytes=64 * 1024,
                )
                writer.write(prefix)
                writer.checkpoint()
                writer.close()
                before = partial.read_bytes()
                response = _response(b"b" * 10, 206, headers)
                with patch(
                    "app.downloader.urllib.request.urlopen",
                    return_value=response,
                ), patch(
                    "app.downloader.NativeChecksumStream", _ChecksumStream
                ):
                    with self.assertRaises(DownloadError) as raised:
                        download_raw_cache(
                            "https://fixture.invalid/source",
                            destination,
                            source_id=self.SOURCE_ID,
                            expected_size=raw_size,
                            expected_crc64="fixture-crc",
                            physical_budget=raw_size * 2,
                        )
                self.assertEqual(raised.exception.code, "RANGE_RESPONSE_INVALID")
                self.assertEqual(partial.read_bytes(), before)
                self.assertFalse(destination.exists())
                self.assertEqual(list(Path(root).glob("*.part-*")), [partial])

    def test_wrong_complete_content_length_fails_before_cache_creation(self):
        raw = b"complete-response"
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root) / "source.rawcache"
            response = _response(
                raw,
                200,
                {"Content-Length": str(len(raw) + 1)},
            )
            with patch(
                "app.downloader.urllib.request.urlopen",
                return_value=response,
            ), patch("app.downloader.NativeChecksumStream", _ChecksumStream):
                with self.assertRaises(DownloadError) as raised:
                    download_raw_cache(
                        "https://fixture.invalid/source",
                        destination,
                        source_id=self.SOURCE_ID,
                        expected_size=len(raw),
                        expected_crc64="fixture-crc",
                        physical_budget=len(raw) * 20,
                    )
            self.assertEqual(raised.exception.code, "RANGE_RESPONSE_INVALID")
            self.assertTrue(response.closed)
            self.assertFalse(destination.exists())
            self.assertEqual(list(Path(root).glob("*.part-*")), [])

    def test_http_416_never_promotes_an_incomplete_recovery(self):
        prefix = b"a" * (64 * 1024)
        raw_size = len(prefix) + 10
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root) / "source.rawcache"
            partial = Path(root) / (destination.name + ".part-old")
            writer = RawCacheWriter(
                partial,
                source_id=self.SOURCE_ID,
                expected_size=raw_size,
                physical_budget=raw_size * 2,
                frame_bytes=64 * 1024,
            )
            writer.write(prefix)
            writer.checkpoint()
            writer.close()
            before = partial.read_bytes()
            error = urllib.error.HTTPError(
                "https://fixture.invalid/source", 416, "range", {}, None
            )
            with patch(
                "app.downloader.urllib.request.urlopen", side_effect=error
            ), patch("app.downloader.NativeChecksumStream", _ChecksumStream):
                with self.assertRaises(DownloadError) as raised:
                    download_raw_cache(
                        "https://fixture.invalid/source",
                        destination,
                        source_id=self.SOURCE_ID,
                        expected_size=raw_size,
                        expected_crc64="fixture-crc",
                        physical_budget=raw_size * 2,
                    )
            self.assertEqual(raised.exception.code, "HTTP_416")
            self.assertEqual(partial.read_bytes(), before)
            self.assertFalse(destination.exists())

    def test_complete_verified_frames_can_publish_without_a_live_url(self):
        raw = b"complete-prefix" * 6000
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root) / "source.rawcache"
            partial = Path(root) / (destination.name + ".part-old")
            writer = RawCacheWriter(
                partial,
                source_id=self.SOURCE_ID,
                expected_size=len(raw),
                physical_budget=len(raw) * 2,
                frame_bytes=64 * 1024,
            )
            writer.write(raw)
            writer.checkpoint()
            writer.close()
            self.assertFalse(
                inspect_cache(
                    partial,
                    source_id=self.SOURCE_ID,
                    expected_size=len(raw),
                ).complete
            )
            with patch("app.downloader.urllib.request.urlopen") as network, patch(
                "app.downloader.NativeChecksumStream", _ChecksumStream
            ):
                result = download_raw_cache(
                    "",
                    destination,
                    source_id=self.SOURCE_ID,
                    expected_size=len(raw),
                    expected_crc64="fixture-crc",
                    physical_budget=len(raw) * 2,
                )
            self.assertEqual(result.sha256, hashlib.sha256(raw).hexdigest())
            self.assertEqual(
                b"".join(
                    iter_raw(
                        destination,
                        source_id=self.SOURCE_ID,
                        expected_size=len(raw),
                    )
                ),
                raw,
            )
            self.assertFalse(partial.exists())
            network.assert_not_called()

    def test_recovery_asset_count_is_bounded_before_network(self):
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root) / "source.rawcache"
            for index in range(16):
                Path(root, f"{destination.name}.part-{index:02d}").write_bytes(b"")
            with patch("app.downloader.urllib.request.urlopen") as network:
                with self.assertRaises(DownloadError) as raised:
                    download_raw_cache(
                        "https://fixture.invalid/source",
                        destination,
                        source_id=self.SOURCE_ID,
                        expected_size=1000,
                        expected_crc64="fixture-crc",
                        physical_budget=2000,
                    )
            self.assertEqual(raised.exception.code, "RAW_CACHE_RECOVERY_ASSET_LIMIT")
            network.assert_not_called()

    def test_forensic_bytes_exhaust_budget_before_network(self):
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root) / "source.rawcache"
            destination.write_bytes(b"corrupt" * 32)
            with patch("app.downloader.urllib.request.urlopen") as network, patch(
                "app.downloader.NativeChecksumStream", _ChecksumStream
            ):
                with self.assertLogs("app.downloader", "WARNING"):
                    with self.assertRaises(DownloadError) as raised:
                        download_raw_cache(
                            "https://fixture.invalid/source",
                            destination,
                            source_id=self.SOURCE_ID,
                            expected_size=1000,
                            expected_crc64="fixture-crc",
                            physical_budget=128,
                        )
            self.assertEqual(raised.exception.code, "RAW_CACHE_BUDGET_EXCEEDED")
            self.assertFalse(destination.exists())
            self.assertEqual(len(list(Path(root).glob("*.corrupt-*"))), 1)
            network.assert_not_called()

    def test_publish_directory_sync_failure_is_recoverable_and_bounded(self):
        raw = b"publish-sync-source" * 8000
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root) / "source.rawcache"
            with patch(
                "app.downloader.urllib.request.urlopen", return_value=_response(raw)
            ), patch(
                "app.downloader.NativeChecksumStream", _ChecksumStream
            ), patch(
                "app.downloader._sync_directory",
                side_effect=OSError("injected directory fsync failure"),
            ):
                with self.assertRaises(DownloadError) as raised:
                    download_raw_cache(
                        "https://fixture.invalid/source",
                        destination,
                        source_id=self.SOURCE_ID,
                        expected_size=len(raw),
                        expected_crc64="fixture-crc",
                        physical_budget=len(raw),
                    )
            self.assertEqual(raised.exception.code, "DOWNLOAD_INTERRUPTED")
            self.assertTrue(destination.is_file())
            self.assertEqual(len(list(Path(root).glob("*.part-*"))), 1)
            with patch("app.downloader.urllib.request.urlopen") as network, patch(
                "app.downloader.NativeChecksumStream", _ChecksumStream
            ):
                recovered = download_raw_cache(
                    "",
                    destination,
                    source_id=self.SOURCE_ID,
                    expected_size=len(raw),
                    expected_crc64="fixture-crc",
                    physical_budget=len(raw),
                )
            self.assertEqual(recovered.sha256, hashlib.sha256(raw).hexdigest())
            self.assertEqual(list(Path(root).glob("*.part-*")), [])
            network.assert_not_called()

    def test_link_failure_retains_complete_private_cache_for_retry(self):
        raw = b"publish-link-source" * 8000
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root) / "source.rawcache"
            with patch(
                "app.downloader.urllib.request.urlopen", return_value=_response(raw)
            ), patch(
                "app.downloader.NativeChecksumStream", _ChecksumStream
            ), patch(
                "app.downloader._publish_cache_exclusive",
                side_effect=OSError("injected link failure"),
            ):
                with self.assertRaises(DownloadError) as raised:
                    download_raw_cache(
                        "https://fixture.invalid/source",
                        destination,
                        source_id=self.SOURCE_ID,
                        expected_size=len(raw),
                        expected_crc64="fixture-crc",
                        physical_budget=len(raw),
                    )
            self.assertEqual(raised.exception.code, "DOWNLOAD_INTERRUPTED")
            self.assertFalse(destination.exists())
            attempts = list(Path(root).glob("*.part-*"))
            self.assertEqual(len(attempts), 1)
            self.assertTrue(
                inspect_cache(
                    attempts[0],
                    source_id=self.SOURCE_ID,
                    expected_size=len(raw),
                ).complete
            )
            with patch("app.downloader.urllib.request.urlopen") as network, patch(
                "app.downloader.NativeChecksumStream", _ChecksumStream
            ):
                recovered = download_raw_cache(
                    "",
                    destination,
                    source_id=self.SOURCE_ID,
                    expected_size=len(raw),
                    expected_crc64="fixture-crc",
                    physical_budget=len(raw),
                )
            self.assertEqual(recovered.sha256, hashlib.sha256(raw).hexdigest())
            self.assertEqual(list(Path(root).glob("*.part-*")), [])
            network.assert_not_called()

    def test_existing_complete_cache_is_reverified_without_network(self):
        raw = b"cached-source" * 1000
        with tempfile.TemporaryDirectory() as root:
            destination = Path(root) / "source.rawcache"
            with RawCacheWriter(
                destination,
                source_id=self.SOURCE_ID,
                expected_size=len(raw),
                physical_budget=len(raw) * 2,
                frame_bytes=64 * 1024,
            ) as writer:
                writer.write(raw)
                writer.finish(expected_sha256=hashlib.sha256(raw).hexdigest())
            with patch("app.downloader.urllib.request.urlopen") as network, patch(
                "app.downloader.NativeChecksumStream", _ChecksumStream
            ):
                result = download_raw_cache(
                    "",
                    destination,
                    source_id=self.SOURCE_ID,
                    expected_size=len(raw),
                    expected_crc64="fixture-crc",
                    physical_budget=len(raw) * 2,
                )
            self.assertEqual(result.size_bytes, len(raw))
            network.assert_not_called()


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

    def test_opt_in_raw_cache_binds_file_identity_budget_and_path(self):
        manager = manager_stub()
        result = SimpleNamespace(
            path=Path("fixture-downloads/fixture-id.rawcache"),
            size_bytes=123,
            sha256="fixture-sha",
        )
        with patch.dict(
            "os.environ",
            {
                "RDS_BINLOG_RAW_CACHE_ENABLED": "1",
                "RDS_BINLOG_RAW_CACHE_MAX_RATIO": "2",
            },
        ), patch("app.pipeline.download_raw_cache", return_value=result) as cached, patch(
            "app.pipeline.download_file"
        ) as ordinary:
            path, sha = manager._download(
                "fixture-job", Mock(), settings(), "fixture-id", remote()
            )
        self.assertEqual((path, sha), (result.path, result.sha256))
        ordinary.assert_not_called()
        self.assertEqual(cached.call_args.args[1], result.path)
        self.assertEqual(cached.call_args.kwargs["source_id"], "fixture-id")
        self.assertEqual(cached.call_args.kwargs["physical_budget"], 246)

    def test_flag_change_reuses_existing_asset_without_migration(self):
        cases = (
            ("0", ".rawcache", "download_raw_cache"),
            ("1", ".binlog", "download_file"),
        )
        for enabled, suffix, expected_call in cases:
            with self.subTest(enabled=enabled), tempfile.TemporaryDirectory() as root:
                manager = manager_stub()
                manager.storage.paths["downloads"] = Path(root)
                existing = Path(root) / f"fixture-id{suffix}"
                existing.write_bytes(b"existing")
                result = SimpleNamespace(
                    path=existing, size_bytes=123, sha256="fixture-sha"
                )
                with patch.dict(
                    "os.environ",
                    {
                        "RDS_BINLOG_RAW_CACHE_ENABLED": enabled,
                        "RDS_BINLOG_RAW_CACHE_MAX_RATIO": "2",
                    },
                ), patch(
                    "app.pipeline.download_raw_cache", return_value=result
                ) as cached, patch(
                    "app.pipeline.download_file", return_value=result
                ) as ordinary:
                    path, sha = manager._download(
                        "fixture-job", Mock(), settings(), "fixture-id", remote()
                    )
                self.assertEqual((path, sha), (existing, "fixture-sha"))
                if expected_call == "download_raw_cache":
                    cached.assert_called_once()
                    ordinary.assert_not_called()
                else:
                    ordinary.assert_called_once()
                    cached.assert_not_called()

    def test_async_progress_failure_prevents_downloaded_state(self):
        manager = manager_stub()
        manager.metadata.update_download_progress.side_effect = RuntimeError(
            "durable progress failed"
        )
        result = SimpleNamespace(
            path=Path("fixture.binlog"), size_bytes=123, sha256="fixture-sha"
        )

        def downloaded(_url, _path, **kwargs):
            kwargs["progress"](123)
            return result

        with patch("app.pipeline.download_file", side_effect=downloaded), self.assertLogs(
            "app.pipeline", "ERROR"
        ):
            with self.assertRaisesRegex(RuntimeError, "durable progress failed"):
                manager._download(
                    "fixture-job", Mock(), settings(), "fixture-id", remote()
                )
        states = [call.args[1] for call in manager.metadata.set_file_state.call_args_list]
        self.assertEqual(states, ["downloading"])


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
