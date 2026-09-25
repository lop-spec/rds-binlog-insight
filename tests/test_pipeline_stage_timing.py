from __future__ import annotations

import json
import tempfile
import threading
import unittest
from concurrent.futures import Future
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app import pipeline
from app.config import Settings
from app.parser_bridge import ParserChunk
from app.pipeline import PreparedBinlog, SyncManager
from tests.test_core import remote


class PipelineStageTimingTests(unittest.TestCase):
    def run_fixture(self, *, detached=True, empty=False, archive_error=False,
                    visible=False, progress_error=False, transport="ndjson",
                    persisted_rows=1, cleanup_error=False, raw_cache=False):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        root = Path(directory.name)
        raw = root / ('fixture.rawcache' if raw_cache else 'fixture.binlog')
        raw.write_bytes(b'original raw must survive until commit')
        paths = []
        for index in range(0 if empty else 3):
            path = root / f'chunk-{index}.{transport}'
            path.write_bytes(b'{"fixture":1}\n')
            paths.append(path)
        transport_bytes = sum(p.stat().st_size for p in paths)
        chunks = [
            ParserChunk(path=path, transport_format=transport, sequence=index,
                        rows=1, size_bytes=path.stat().st_size,
                        decoded_bytes=32 if transport == "arrow" else None)
            for index, path in enumerate(paths)
        ]
        manager = SyncManager.__new__(SyncManager)
        manager.metadata = Mock()
        publish = Mock()
        if progress_error:
            publish.side_effect = RuntimeError('progress commit failed')
        manager.storage = SimpleNamespace(
            paths={'downloads': root, 'staging': root, 'root': root},
            ingest_ndjson_file=Mock(return_value=(persisted_rows, [])),
            ingest_arrow_file=Mock(return_value=(persisted_rows, [])),
            publish_ingested_parts=publish, finalize_file_parts=Mock())
        manager._event = Mock()
        manager._commit_prepared = Mock()
        manager._archive_parts = Mock()
        self.manager, self.raw, self.chunk_paths = manager, raw, paths
        self.stage_log = Mock()
        self.transform_payloads = []
        item = remote('mysql-bin.fixture', '2026-07-29T01:00:00Z')
        sequence = iter(range(1, 1000))

        def transform(payload):
            self.transform_payloads.append(payload)
            future = Future()
            future.set_result((persisted_rows, [{'path': payload['part_key'], 'size_bytes': 10}]))
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
        unlink_context = (
            patch.object(Path, 'unlink', side_effect=OSError('fixture cleanup failed'))
            if cleanup_error else nullcontext()
        )
        with patch.object(pipeline, 'parse_parser_chunks_buffered', return_value=iter(chunks)) as parser, \
                patch.object(pipeline.LOGGER, 'info', self.stage_log), \
                patch.object(pipeline.time, 'monotonic', side_effect=lambda: next(sequence) / 100), \
                unlink_context:
            prepared = manager._process_one(
                'job', Mock(), Settings(db_instance_id='rm-fixture'),
                'file', item, 'pending', 'mysql', Mock() if detached else None,
                prepared_download=(raw, 'verified-fixture'), defer_commit=True,
                query_visible_event=visible_event,
                transform_submitter=transform if detached else None,
                archive_submitter=archive if detached else None)
        self.parser_call = parser
        rows = [json.loads(c.args[1]) for c in self.stage_log.call_args_list
                if c.args[0] == 'FILE_STAGE_TIMINGS %s']
        self.assertEqual(len(rows), 1)
        metrics = rows[0]
        self.assertEqual(metrics['chunks'], len(paths))
        self.assertEqual(
            metrics['ndjson_bytes'],
            transport_bytes if transport == 'ndjson' else 0,
        )
        self.assertEqual(metrics['parser_transport_bytes'], transport_bytes)
        self.assertEqual(metrics['parser_transport'], transport if paths else 'empty')
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

    def test_raw_cache_parser_input_is_bound_to_original_remote_size(self):
        self.run_fixture(raw_cache=True)
        self.assertEqual(
            self.parser_call.call_args.kwargs["raw_cache_expected_size"],
            remote('mysql-bin.fixture', '2026-07-29T01:00:00Z').file_size,
        )

    def test_detached_phases_are_disjoint_and_keep_commit_boundary(self):
        metrics = self.run_fixture()
        self.assertEqual(metrics['transform_mode'], 'detached')
        for name in ('native_wait_seconds', 'transform_wait_seconds',
                     'publish_seconds', 'archive_wait_seconds', 'metadata_seconds'):
            self.assertGreater(metrics[name], 0)
        self.assertEqual(self.manager.storage.publish_ingested_parts.call_count, 3)

    def test_visible_chunks_merge_parts_catalog_and_progress_once(self):
        self.run_fixture(visible=True)
        calls = self.manager.storage.publish_ingested_parts.call_args_list
        self.assertEqual([c.args[0] for c in calls], ['file', 'file', 'file'])
        logs = [c for c in self.stage_log.call_args_list
                if len(c.args) == 4 and c.args[2] == 'FILE_CHUNK_PUBLISHED']
        self.assertEqual(len(logs), 3)
        for index, (call, log) in enumerate(zip(calls, logs), 1):
            self.assertEqual(call.kwargs, {
                'append': True,
                'progress': (
                    index,
                    'job',
                    f'mysql-bin.fixture 已发布 {index} 条事件；这些事件现在即可查询',
                    f'mysql-bin.fixture 第 {index} 批：1 条事件已原子发布',
                ),
            })
            self.assertEqual(
                log.args[1:],
                ('job', 'FILE_CHUNK_PUBLISHED', call.kwargs['progress'][3]),
            )
        self.manager.metadata.record_file_chunk_progress.assert_not_called()
        self.assertFalse(any(c.args[2] == 'FILE_CHUNK_PUBLISHED'
                             for c in self.manager._event.call_args_list))

    def test_background_chunks_do_not_change_visible_job_or_log_publication(self):
        self.run_fixture()
        calls = self.manager.storage.publish_ingested_parts.call_args_list
        self.assertEqual(len(calls), 3)
        self.assertTrue(all(c.kwargs['progress'][1] == '' for c in calls))
        self.manager.metadata.record_file_chunk_progress.assert_not_called()
        self.assertFalse(any(len(c.args) == 4 and c.args[2] == 'FILE_CHUNK_PUBLISHED'
                             for c in self.stage_log.call_args_list))

    def test_progress_commit_failure_preserves_raw_and_never_logs_success(self):
        with self.assertRaisesRegex(RuntimeError, 'progress commit failed'):
            self.run_fixture(visible=True, progress_error=True)
        self.assertTrue(self.raw.exists())
        self.manager.storage.finalize_file_parts.assert_not_called()
        self.manager._commit_prepared.assert_not_called()
        self.stage_log.assert_not_called()

    def test_inline_private_transform_has_merged_publish_phase(self):
        metrics = self.run_fixture(detached=False)
        self.assertEqual(
            metrics['transform_mode'], 'inline-private-then-merged-publish'
        )
        self.assertGreater(metrics['publish_seconds'], 0)
        self.assertEqual(self.manager.storage.ingest_ndjson_file.call_count, 3)
        self.assertTrue(all(
            call.kwargs['publish_metadata'] is False
            for call in self.manager.storage.ingest_ndjson_file.call_args_list
        ))

    def test_arrow_inline_uses_columnar_ingest_and_transport_metrics(self):
        metrics = self.run_fixture(detached=False, transport='arrow')
        self.assertEqual(metrics['parser_transport'], 'arrow')
        self.assertEqual(metrics['ndjson_bytes'], 0)
        self.assertEqual(self.manager.storage.ingest_arrow_file.call_count, 3)
        self.manager.storage.ingest_ndjson_file.assert_not_called()

    def test_arrow_detached_payload_keeps_format_path_and_row_contract(self):
        self.run_fixture(detached=True, transport='arrow')
        self.assertEqual(len(self.transform_payloads), 3)
        for index, payload in enumerate(self.transform_payloads):
            self.assertEqual(payload['parser_format'], 'arrow')
            self.assertEqual(payload['expected_rows'], 1)
            self.assertEqual(Path(payload['parser_path']), self.chunk_paths[index])
            self.assertNotIn('ndjson_path', payload)

    def test_row_count_mismatch_cleans_chunk_and_preserves_raw(self):
        with self.assertRaises(pipeline.ParserError) as raised:
            self.run_fixture(persisted_rows=0)
        self.assertEqual(raised.exception.code, 'PARSER_CHUNK_ROW_COUNT_MISMATCH')
        self.assertTrue(self.raw.exists())
        self.assertFalse(self.chunk_paths[0].exists())
        self.manager.metadata.record_file_chunk_progress.assert_not_called()
        self.manager.storage.publish_ingested_parts.assert_not_called()
        self.manager.storage.finalize_file_parts.assert_not_called()

    def test_chunk_cleanup_failure_is_logged_and_aborts_before_progress(self):
        with self.assertRaises(pipeline.ParserError) as raised:
            self.run_fixture(cleanup_error=True)
        self.assertEqual(raised.exception.code, 'PARSER_CHUNK_CLEANUP_FAILED')
        self.assertTrue(self.raw.exists())
        self.assertTrue(any(path.exists() for path in self.chunk_paths))
        self.manager.metadata.record_file_chunk_progress.assert_not_called()
        self.manager.storage.finalize_file_parts.assert_not_called()

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
