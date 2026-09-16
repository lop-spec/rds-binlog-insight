from __future__ import annotations

import json
import tempfile
import threading
import unittest
from concurrent.futures import Future
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app import pipeline
from app.config import Settings
from app.pipeline import PreparedBinlog, SyncManager
from tests.test_core import remote


class PipelineStageTimingTests(unittest.TestCase):
    def run_fixture(self, *, detached=True, empty=False, archive_error=False,
                    visible=False, progress_error=False):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        raw = root / 'fixture.binlog'
        raw.write_bytes(b'original raw must survive until commit')
        paths = []
        for index in range(0 if empty else 3):
            path = root / f'chunk-{index}.ndjson'
            path.write_bytes(b'{"fixture":1}\n')
            paths.append(path)
        ndjson_bytes = sum(p.stat().st_size for p in paths)
        manager = SyncManager.__new__(SyncManager)
        manager.metadata = Mock()
        if progress_error:
            manager.metadata.record_file_chunk_progress.side_effect = RuntimeError('progress commit failed')
        manager.storage = SimpleNamespace(
            paths={'downloads': root, 'staging': root, 'root': root},
            ingest_ndjson_file=Mock(return_value=(1, [])),
            publish_ingested_parts=Mock(), finalize_file_parts=Mock())
        manager._event = Mock()
        manager._commit_prepared = Mock()
        manager._archive_parts = Mock()
        self.manager, self.raw = manager, raw
        self.stage_log = Mock()
        item = remote('mysql-bin.fixture', '2026-07-29T01:00:00Z')
        sequence = iter(range(1, 1000))

        def transform(payload):
            future = Future()
            future.set_result((1, [{'path': payload['part_key'], 'size_bytes': 10}]))
            return future

        def archive(_parts):
            future = Future()
            if archive_error:
                future.set_exception(RuntimeError('fixture upload failed'))
            else:
                future.set_result(1)
            return future

        visible_event = threading.Event()
        if visible:
            visible_event.set()
        with patch.object(pipeline, 'parse_ndjson_chunks_buffered', return_value=iter(paths)), \
                patch.object(pipeline.LOGGER, 'info', self.stage_log), \
                patch.object(pipeline.time, 'monotonic', side_effect=lambda: next(sequence) / 100):
            prepared = manager._process_one(
                'job', Mock(), Settings(db_instance_id='rm-fixture'),
                'file', item, 'pending', 'mysql', Mock() if detached else None,
                prepared_download=(raw, 'verified-fixture'), defer_commit=True,
                query_visible_event=visible_event,
                transform_submitter=transform if detached else None,
                archive_submitter=archive if detached else None)
        rows = [json.loads(c.args[1]) for c in self.stage_log.call_args_list
                if c.args[0] == 'FILE_STAGE_TIMINGS %s']
        self.assertEqual(len(rows), 1)
        metrics = rows[0]
        self.assertEqual(metrics['chunks'], len(paths))
        self.assertEqual(metrics['ndjson_bytes'], ndjson_bytes)
        self.assertEqual(metrics['events'], len(paths))
        phases = [value for name, value in metrics.items()
                  if name.endswith('_seconds') and name != 'prepared_seconds']
        self.assertTrue(all(value >= 0 for value in phases))
        self.assertAlmostEqual(sum(phases), metrics['prepared_seconds'], places=5)
        self.assertEqual(metrics['prepared_seconds'], prepared.parse_seconds)
        self.assertIsInstance(prepared, PreparedBinlog)
        self.assertTrue(raw.exists())
        self.assertTrue(all(not p.exists() for p in paths))
        manager._commit_prepared.assert_not_called()
        manager.metadata.set_file_state.assert_called_with('file', 'stored', event_count=len(paths))
        return metrics

    def test_detached_phases_are_disjoint_and_keep_commit_boundary(self):
        metrics = self.run_fixture()
        self.assertEqual(metrics['transform_mode'], 'detached')
        for name in ('native_wait_seconds', 'transform_wait_seconds',
                     'publish_seconds', 'archive_wait_seconds', 'metadata_seconds'):
            self.assertGreater(metrics[name], 0)
        self.assertEqual(self.manager.storage.publish_ingested_parts.call_count, 3)

    def test_visible_chunks_record_each_progress_and_log_exactly_once(self):
        self.run_fixture(visible=True)
        calls = self.manager.metadata.record_file_chunk_progress.call_args_list
        self.assertEqual([c.args for c in calls], [('file', 1), ('file', 2), ('file', 3)])
        logs = [c for c in self.stage_log.call_args_list
                if len(c.args) == 4 and c.args[2] == 'FILE_CHUNK_PUBLISHED']
        self.assertEqual(len(logs), 3)
        for index, (call, log) in enumerate(zip(calls, logs), 1):
            self.assertEqual(call.kwargs, {
                'job_id': 'job',
                'message': f'mysql-bin.fixture 已发布 {index} 条事件；这些事件现在即可查询',
                'event_message': f'mysql-bin.fixture 第 {index} 批：1 条事件已原子发布',
            })
            self.assertEqual(log.args[1:], ('job', 'FILE_CHUNK_PUBLISHED', call.kwargs['event_message']))
        self.assertFalse(any(c.args[2] == 'FILE_CHUNK_PUBLISHED'
                             for c in self.manager._event.call_args_list))

    def test_background_chunks_do_not_change_visible_job_or_log_publication(self):
        self.run_fixture()
        calls = self.manager.metadata.record_file_chunk_progress.call_args_list
        self.assertEqual(len(calls), 3)
        self.assertTrue(all(c.kwargs['job_id'] == '' for c in calls))
        self.assertFalse(any(len(c.args) == 4 and c.args[2] == 'FILE_CHUNK_PUBLISHED'
                             for c in self.stage_log.call_args_list))

    def test_progress_commit_failure_preserves_raw_and_never_logs_success(self):
        with self.assertRaisesRegex(RuntimeError, 'progress commit failed'):
            self.run_fixture(visible=True, progress_error=True)
        self.assertTrue(self.raw.exists())
        self.manager.storage.finalize_file_parts.assert_not_called()
        self.manager._commit_prepared.assert_not_called()
        self.stage_log.assert_not_called()

    def test_inline_publish_is_explicitly_accounted_with_transform(self):
        metrics = self.run_fixture(detached=False)
        self.assertEqual(metrics['transform_mode'], 'inline-with-publish')
        self.assertEqual(metrics['publish_seconds'], 0)
        self.assertEqual(self.manager.storage.ingest_ndjson_file.call_count, 3)

    def test_empty_file_has_a_complete_zero_chunk_observation(self):
        self.assertEqual(self.run_fixture(empty=True)['events'], 0)

    def test_archive_failure_never_reports_prepared_or_deletes_raw(self):
        with self.assertRaisesRegex(RuntimeError, 'fixture upload failed'):
            self.run_fixture(archive_error=True)
        self.assertTrue(self.raw.exists())
        self.manager.storage.finalize_file_parts.assert_not_called()
        self.manager._commit_prepared.assert_not_called()
        self.stage_log.assert_not_called()


if __name__ == '__main__':
    unittest.main()
