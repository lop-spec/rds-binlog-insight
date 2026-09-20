"""Deterministic concurrent admission, failure isolation and prefetch contracts."""
from __future__ import annotations

import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import Mock, patch
from dataclasses import replace
from types import SimpleNamespace

from app import parser_bridge as parser
from app import pipeline
from app.config import Settings
from app.pipeline import PreparedBinlog, SyncManager
from tests.test_core import remote


class ParserCapacityTests(unittest.TestCase):
    def test_two_lanes_fit_existing_tmpfs(self):
        self.assertLessEqual(2 * parser.NATIVE_CHUNK_MAX_BYTES *
                             parser.NATIVE_CHUNK_MAX_OUTSTANDING,
                             parser.NATIVE_STAGING_BUDGET_BYTES)
        self.assertLess(parser.NATIVE_STAGING_BUDGET_BYTES, 1024**3)

    def test_failed_lane_does_not_clean_live_lane(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first_entered = threading.Event()
            release = threading.Event()
            live = root / 'live-000000.ndjson'

            def fake(_path, file_id, staging, *_args, **_kwargs):
                chunk = staging / f'{file_id}-000000.ndjson'
                chunk.write_text('{}\n')
                if file_id == 'live':
                    first_entered.set()
                    if not release.wait(3):
                        raise AssertionError('second parser remained serialized')
                    yield chunk
                else:
                    self.assertTrue(live.is_file())
                    raise parser.ParserError('forced failure')

            def consume(file_id):
                return list(parser.parse_ndjson_chunks_buffered(
                    root / 'input', file_id, root))

            with patch.object(parser, '_parse_ndjson_chunks_buffered', fake), \
                    ThreadPoolExecutor(max_workers=2) as executor:
                first = executor.submit(consume, 'live')
                try:
                    self.assertTrue(first_entered.wait(2))
                    second = executor.submit(consume, 'failed')
                    with self.assertRaisesRegex(parser.ParserError, 'forced failure'):
                        second.result(timeout=2)
                    self.assertTrue(live.is_file())
                    self.assertFalse((root / 'failed-000000.ndjson').exists())
                finally:
                    release.set()
                self.assertEqual(first.result(timeout=2), [live])
            # Failure released its capacity; a retry does not hang.
            with patch.object(parser, '_parse_ndjson_chunks_buffered', return_value=iter(())):
                self.assertEqual(consume('failed'), [])

    def _check_blocked(self, *, same_id=False, byte_limited=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            release = threading.Event()
            entered = [threading.Event() for _ in range(3)]
            next_index = iter(range(3))
            lock = threading.Lock()

            def fake(*_args, **_kwargs):
                with lock:
                    index = next(next_index)
                entered[index].set()
                if not release.wait(4):
                    raise AssertionError('test failed to release active parsers')
                yield from ()

            def consume(index):
                return list(parser.parse_ndjson_chunks_buffered(
                    root / 'input', 'same' if same_id else f'file-{index}', root,
                    max_bytes=(384 if byte_limited else 128) * 1024**2))

            admitted = 1 if same_id or byte_limited else 2
            with patch.object(parser, '_parse_ndjson_chunks_buffered', fake), \
                    ThreadPoolExecutor(max_workers=3) as executor:
                futures = [executor.submit(consume, i) for i in range(admitted)]
                try:
                    for event in entered[:admitted]:
                        self.assertTrue(event.wait(2))
                    futures.append(executor.submit(consume, admitted))
                    time.sleep(0.05)
                    self.assertFalse(entered[admitted].is_set())
                finally:
                    release.set()
                for future in futures:
                    future.result(timeout=2)
                self.assertTrue(entered[admitted].is_set())

    def test_third_lane_waits_for_capacity(self):
        self._check_blocked()

    def test_same_file_waits_for_existing_owner(self):
        self._check_blocked(same_id=True)

    def test_byte_budget_can_serialize_large_chunks(self):
        self._check_blocked(byte_limited=True)

    def test_impossible_reservation_fails_before_parser_start(self):
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(parser, '_parse_ndjson_chunks_buffered') as native:
            with self.assertRaises(parser.ParserError) as raised:
                list(parser.parse_ndjson_chunks_buffered(
                    Path(directory) / 'input', 'file', Path(directory),
                    max_bytes=1024**3))
            self.assertEqual(raised.exception.code, 'PARSER_STAGING_BUDGET')
            native.assert_not_called()


class DownloadCapacityTests(unittest.TestCase):
    def run_pipeline(self, sizes, *, pause=False, wait_for_ahead=False,
                     slow_first_download=False, unavailable_first=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manager = SyncManager.__new__(SyncManager)
            manager.metadata = Mock()
            manager.metadata.file_record.return_value = {'event_count': 1}
            manager.storage = SimpleNamespace(paths={'downloads': root})
            manager._pause_after_current = threading.Event()
            manager._shutdown = threading.Event()
            manager._update_pipeline_status = Mock()
            manager._event = Mock()
            pending = [(f'file-{i}', replace(remote(f'bin.{i}', '2026-07-29T01:00:00Z'),
                                           file_size=size), 'discovered')
                       for i, size in enumerate(sizes)]
            lock = threading.Lock()
            ahead = threading.Event()
            second_parsing = threading.Event()
            first_two = threading.Barrier(2)
            downloaded, processed, committed = [], [], []
            retained, peaks = {}, []

            def download(_job, _client, _settings, file_id, item):
                with lock:
                    downloaded.append(file_id)
                    retained[file_id] = item.file_size
                    peaks.append(sum(retained.values()))
                    if len(downloaded) >= 4:
                        ahead.set()
                if slow_first_download and file_id == 'file-0':
                    self.assertTrue(second_parsing.wait(2), 'first download blocked the second parser lane')
                if unavailable_first and file_id == 'file-0':
                    from app.downloader import DownloadError
                    with lock:
                        retained.pop(file_id)
                    raise DownloadError('fixture missing from source', 'DOWNLOAD_LINK_REFRESH_MISSING')
                path = root / file_id
                path.write_bytes(b'fixture')
                return path, 'sha256'

            def process(_job, _client, _settings, file_id, item, *_args, **kwargs):
                with lock:
                    processed.append(file_id)
                if file_id == 'file-1':
                    second_parsing.set()
                if wait_for_ahead and file_id in {'file-0', 'file-1'}:
                    self.assertTrue(ahead.wait(2), 'independent prefetch did not run')
                    first_two.wait(timeout=2)
                if file_id == 'file-0':
                    time.sleep(0.03)  # deliberately complete the second lane first
                if pause:
                    manager._pause_after_current.set()
                return PreparedBinlog(file_id, item, kwargs['prepared_download'][0], 1, .01)

            def commit(_job, _settings, prepared):
                with lock:
                    committed.append(prepared.file_id)
                    retained.pop(prepared.file_id)
                prepared.raw_path.unlink()

            manager._download = download
            manager._process_one = process
            manager._commit_prepared = commit
            result = manager._run_pending_parallel(
                'job', object(), Settings(), pending, 'mysql', None,
                completed=0, unavailable=0)
            return result, downloaded, processed, committed, max(peaks)

    def test_slow_first_download_does_not_block_second_parser_or_change_commit_order(self):
        result, _, processed, committed, peak = self.run_pipeline(
            [100] * 5, slow_first_download=True)
        self.assertEqual(result, (5, 0, False))
        self.assertEqual(processed[0], 'file-1')
        self.assertEqual(committed, [f'file-{i}' for i in range(5)])
        self.assertLessEqual(peak, pipeline.DOWNLOAD_PREFETCH_FILES * 100)

    def test_missing_first_download_is_accounted_without_losing_later_files(self):
        result, _, processed, committed, peak = self.run_pipeline(
            [100] * 5, slow_first_download=True, unavailable_first=True)
        self.assertEqual(result, (4, 1, False))
        self.assertNotIn('file-0', processed)
        self.assertEqual(committed, [f'file-{i}' for i in range(1, 5)])
        self.assertLessEqual(peak, pipeline.DOWNLOAD_PREFETCH_FILES * 100)

    def test_prefetch_runs_ahead_of_both_parsers_and_commit_remains_ordered(self):
        result, downloaded, processed, committed, peak = self.run_pipeline(
            [100] * 7, wait_for_ahead=True)
        self.assertEqual(result, (7, 0, False))
        self.assertEqual(len(downloaded), 7)
        self.assertEqual(len(processed), 7)
        self.assertEqual(committed, [f'file-{i}' for i in range(7)])
        self.assertLessEqual(peak, pipeline.DOWNLOAD_PREFETCH_FILES * 100)

    def test_prefetch_byte_budget_shrinks_window(self):
        with patch.object(pipeline, 'DOWNLOAD_PREFETCH_BYTES', 250):
            result, _, _, committed, peak = self.run_pipeline([100] * 7)
        self.assertEqual(result, (7, 0, False))
        self.assertEqual(len(committed), 7)
        self.assertLessEqual(peak, 250)

    def test_oversized_file_is_admitted_alone_without_deadlock(self):
        with patch.object(pipeline, 'DOWNLOAD_PREFETCH_BYTES', 250), \
                self.assertLogs(pipeline.LOGGER, level='WARNING'):
            result, _, _, _, peak = self.run_pipeline([400, 100, 100, 100])
        self.assertEqual(result, (4, 0, False))
        self.assertEqual(peak, 400)

    def test_pause_finishes_admitted_batch_without_parsing_prefetched_files(self):
        result, _, processed, committed, peak = self.run_pipeline([100] * 8, pause=True)
        self.assertEqual(result, (2, 0, True))
        self.assertEqual(set(processed), {'file-0', 'file-1'})
        self.assertEqual(committed, ['file-0', 'file-1'])
        self.assertLessEqual(peak, 400)


if __name__ == '__main__':
    unittest.main()
