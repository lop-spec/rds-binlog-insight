from __future__ import annotations

import json
import random
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from app.binlog_lite import RawBinlogError
from app.config import Settings
from app.query_tasks import QueryBatchControl, QueryCancelled, QueryControl, QueryTaskManager
from app.raw_binlog import MAX_BYTES, MAX_FILES
from app.storage import EventStorage, StorageError
from tests import test_raw_binlog as fixtures


class BatchQueryTests(unittest.TestCase):
    setUp = fixtures.ManifestTests.setUp
    add_raw = fixtures.ManifestTests.add_raw

    def storage(self):
        storage = EventStorage(self.metadata, Path(self.temp.name))
        self.addCleanup(storage.slowlog_index.close)
        clock = patch.object(storage, '_query_window', return_value=(100, 200))
        clock.start()
        self.addCleanup(clock.stop)
        return storage

    def rows(self, count=35):
        rng = random.Random(42)
        rows = {}
        for i in range(count):
            file_id = self.add_raw(i)
            rows[file_id] = [dict(event_id=f'{i}:{j}', event_epoch_us=rng.randrange(100, 201),
                source_file_name=f'mysql-bin.{i:06}', end_position=j, row_index=0,
                operation='UPDATE', database_name='db', table_name='one') for j in range(3)]
        return rows

    def test_35_files_three_batches_match_global_order_and_every_page(self):
        storage = self.storage()
        data = self.rows()
        expected = sorted([r for rows in data.values() for r in rows],
                          key=storage._row_sort_key, reverse=True)
        visited = []

        def decode(_storage, _archive, entry, *args):
            visited.append(entry['file_id'])
            yield from data[entry['file_id']]

        pages = []
        with patch('app.raw_binlog_query.decode', side_effect=decode):
            for offset in range(0, 110, 10):
                visited.clear()
                result = storage.query_events_tiered(
                    {'source': 'binlog', 'offset': offset, 'limit': 10}, Settings(), Mock())
                self.assertEqual(result['rows'], expected[offset:offset+10])
                self.assertEqual(result['has_more'], len(expected) > offset+10)
                self.assertEqual(result['query_batches'], 3)
                self.assertEqual(result['candidate_binlogs'], 35)
                self.assertEqual(set(visited), set(data))
                self.assertEqual(len(visited), 35)
                pages.extend(result['rows'])
        self.assertEqual(pages, expected)

    def test_budget_is_per_batch_not_whole_task(self):
        storage = self.storage()
        data = self.rows(17)
        budgets = []

        def decode(_storage, _archive, entry, _query, _start, _end, budget):
            if not budgets or budgets[-1] is not budget:
                budgets.append(budget)
            budget.add(MAX_BYTES // MAX_FILES)
            yield from data[entry['file_id']]

        with patch('app.raw_binlog_query.decode', side_effect=decode):
            result = storage.query_events_tiered({'source': 'binlog'}, Settings(), Mock())
        self.assertEqual(len(budgets), 2)
        self.assertTrue(all(b.bytes <= MAX_BYTES for b in budgets))
        self.assertGreater(result['range_bytes'], MAX_BYTES)
        self.assertTrue(all(b.cancel.is_set() for b in budgets))

    def test_plan_spills_and_closes_before_any_decode_and_after_failure(self):
        self.rows(35)
        real_spool = tempfile.SpooledTemporaryFile
        opened = []

        def spool(**kwargs):
            kwargs['max_size'] = 1
            result = real_spool(**kwargs)
            opened.append(result)
            return result

        with patch('app.raw_binlog.tempfile.SpooledTemporaryFile', side_effect=spool):
            with self.assertRaisesRegex(RuntimeError, 'stop'):
                with self.store.plan({}, 100, 200) as plan:
                    self.assertTrue(opened[0]._rolled)
                    batches = plan['batches']
                    self.assertEqual(next(batches)['candidate_files'], MAX_FILES)
                    # No open metadata read snapshot survives candidate planning.
                    with self.metadata.connection() as conn:
                        busy, frames, checkpointed = conn.execute('PRAGMA wal_checkpoint(PASSIVE)').fetchone()
                    self.assertEqual(busy, 0)
                    self.assertEqual(frames, checkpointed)
                    raise RuntimeError('stop')
        self.assertTrue(opened[0].closed)

    def test_cancel_after_first_batch_stops_next_batch_and_releases_lock(self):
        storage = self.storage()
        data = self.rows()
        task_id = self.metadata.create_query_task({})
        control = QueryControl(task_id, self.metadata)
        visited = []
        begin = control.begin_batch

        def begin_batch(number, total):
            if number == 2:
                control.cancel()
            begin(number, total)

        def decode(_storage, _archive, entry, *args):
            visited.append(entry['file_id'])
            yield from data[entry['file_id']]

        with patch.object(control, 'begin_batch', side_effect=begin_batch), \
             patch('app.raw_binlog_query.decode', side_effect=decode):
            with self.assertRaises(QueryCancelled):
                storage.query_events_tiered({'source': 'binlog'}, Settings(), Mock(), control=control)
        self.assertEqual(len(visited), MAX_FILES)
        self.assertTrue(storage.raw_binlogs.query_lock.acquire(blocking=False))
        storage.raw_binlogs.query_lock.release()
        control.flush()
        record = self.metadata.query_task(task_id)
        self.assertEqual(record['completed_parts'], MAX_FILES)
        self.assertEqual(record['total_parts'], 35)

    def test_late_batch_failure_never_writes_successful_partial_result(self):
        storage = self.storage()
        data = self.rows()
        visited = []
        closed = []

        def decode(_storage, _archive, entry, *args):
            visited.append(entry['file_id'])
            try:
                if len(visited) == MAX_FILES+1:
                    raise RawBinlogError('fixture CRC mismatch', 'OSS_OBJECT_VERIFY_FAILED')
                yield from data[entry['file_id']]
            finally:
                closed.append(entry['file_id'])

        manager = QueryTaskManager(self.metadata, storage, settings_loader=Settings,
                                   archive_loader=lambda _: Mock())
        self.addCleanup(manager.shutdown)
        with patch('app.raw_binlog_query.decode', side_effect=decode):
            task_id = manager.submit({'source': 'binlog'})
            deadline = time.monotonic()+10
            while time.monotonic() < deadline:
                record = self.metadata.query_task(task_id)
                if record['status'] in {'failed', 'succeeded', 'cancelled'}:
                    break
                time.sleep(.01)
        self.assertEqual(record['status'], 'failed')
        self.assertEqual(record['error_code'], 'OSS_OBJECT_VERIFY_FAILED')
        self.assertFalse(record.get('result_path'))
        self.assertEqual(list(manager.result_dir.glob('*.json.gz')), [])
        self.assertEqual(len(visited), MAX_FILES+1)
        self.assertEqual(closed, visited)

    def test_oversized_file_fails_before_decoder_or_oss_access(self):
        storage = self.storage()
        self.rows()
        self.add_raw(100, size=MAX_BYTES+1)
        archive = Mock()
        with patch('app.raw_binlog_query.decode') as decode:
            with self.assertRaises(RawBinlogError) as caught:
                storage.query_events_tiered({'source': 'binlog'}, Settings(), archive)
        self.assertEqual(caught.exception.code, 'QUERY_BINLOG_BYTE_LIMIT')
        decode.assert_not_called()
        self.assertEqual(archive.mock_calls, [])

    def test_child_deadline_and_parent_cancellation_are_not_suppressed(self):
        child = QueryBatchControl()
        child.until = 0
        with self.assertRaises(RawBinlogError) as caught:
            child.check_cancelled()
        self.assertEqual(caught.exception.code, 'QUERY_DEADLINE_EXCEEDED')
        parent = Mock()
        parent.check_cancelled.side_effect = QueryCancelled('cancelled')
        with self.assertRaises(QueryCancelled):
            QueryBatchControl(parent).advance()

    def test_unknown_schema_closes_decoder_without_suppressing_failure(self):
        storage = self.storage()
        self.add_raw(0)
        closed = []

        def decode(*args):
            try:
                yield dict(event_id='one', event_epoch_us=150, operation='UPDATE')
            finally:
                closed.append(True)

        with patch('app.raw_binlog_query.decode', side_effect=decode), \
             patch.object(storage.exact_index, 'primary_key_match', return_value=None):
            with self.assertRaises(RawBinlogError) as caught:
                storage.query_events_tiered({'source': 'binlog', 'exact': {'value': '1'}}, Settings(), Mock())
        self.assertEqual(caught.exception.code, 'EXACT_SCHEMA_UNKNOWN')
        self.assertEqual(closed, [True])

    def test_legacy_only_batches_merge_audit_and_slowlog_once(self):
        storage = self.storage()
        base = int(time.time()*1_000_000)-1_000_000
        storage._query_window.return_value = (base+100, base+200)
        expected = set()
        for index in range(19):
            file_id = self.add_raw(index)
            name = f'mysql-bin.{index:06}'
            kind = 'WriteRowsEventV2'
            if index == 17:
                name, kind = 'tabularis-audit-batch-fixture', 'TABULARIS_AUDIT'
            elif index == 18:
                name, kind = 'slow-log/batch-fixture', 'SLOW_LOG'
            with self.metadata.connection() as conn:
                conn.execute('DELETE FROM raw_binlog_archives WHERE file_id=?', (file_id,))
                conn.execute('UPDATE binlog_files SET log_file_name=? WHERE id=?', (name, file_id))
            event_id = f'legacy-{index}'
            expected.add(event_id)
            path = Path(self.temp.name)/'fixture.ndjson'
            path.write_text(json.dumps(dict(event_id=event_id, event_epoch_us=base+100+index,
                raw_event_type=kind, operation='INSERT', database_name='db', table_name='one',
                after_json='{"id":1}'))+'\n', encoding='utf-8')
            storage.ingest_ndjson_file(file_id=file_id, instance_id='test', host_instance_id='node',
                source_file_name=name, ndjson_path=path)
        pages = []
        with patch('app.raw_binlog_query.decode') as decode:
            for offset in (0, 7, 14):
                result = storage.query_events_tiered({'source': 'all', 'limit': 7, 'offset': offset,
                    'start_epoch_us': base+100, 'end_epoch_us': base+118}, Settings(), None)
                self.assertEqual(result['candidate_binlogs'], 17)
                self.assertEqual(result['query_batches'], 2)
                self.assertEqual(result['has_more'], offset+7 < 19)
                pages.extend(result['rows'])
        decode.assert_not_called()
        self.assertEqual({r['event_id'] for r in pages}, expected)
        self.assertEqual(len(pages), 19)
        self.assertEqual(pages, sorted(pages, key=storage._row_sort_key, reverse=True))

    def test_planning_cancellation_never_starts_body_reads(self):
        storage = self.storage()
        self.rows()
        control = Mock()
        control.check_cancelled.side_effect = QueryCancelled('cancel plan')
        with patch('app.raw_binlog_query.decode') as decode:
            with self.assertRaises(QueryCancelled):
                storage.query_events_tiered({'source': 'binlog'}, Settings(), Mock(), control=control)
        decode.assert_not_called()

    def test_cross_batch_result_memory_limit_still_fails_closed(self):
        storage = self.storage()
        self.rows(17)
        with self.store.plan({}, 100, 200) as plan:
            batches = list(plan['batches'])
        large_ids = {b['file_ids'][0] for b in batches}
        text = 'x'*(17*1024**2)

        def decode(_storage, _archive, entry, *args):
            if entry['file_id'] in large_ids:
                yield dict(event_id=entry['file_id'], event_epoch_us=150,
                           operation='UPDATE', sql_text=text)

        with patch('app.raw_binlog_query.decode', side_effect=decode):
            with self.assertRaises(RawBinlogError) as caught:
                storage.query_events_tiered({'source': 'binlog', 'limit': 10}, Settings(), Mock())
        self.assertEqual(caught.exception.code, 'RAW_QUERY_RESULT_LIMIT')

    def test_missing_legacy_batch_is_not_a_successful_empty_page(self):
        storage = self.storage()
        storage._query_window.return_value = (100, 200)
        with patch.object(self.metadata, 'storage_metadata_stats', return_value={'latest_epoch_us': 200}), \
             patch.object(self.metadata, 'parts_in_range', return_value=[]):
            with self.assertRaises(StorageError) as caught:
                storage._query_events_tiered_impl({'source': 'binlog', '_binlog_batch_ids': ['missing']},
                                                 Settings(), None)
        self.assertEqual(caught.exception.code, 'QUERY_BATCH_INCOMPLETE')

    def test_real_legacy_and_raw_union_scopes_reads_and_keeps_raw_authoritative(self):
        storage = self.storage()
        data = self.rows(25)
        base = int(time.time()*1_000_000)-1_000_000
        storage._query_window.return_value = (base+100, base+200)
        for rows in data.values():
            for row in rows:
                row['event_epoch_us'] += base
        expected = [r for rows in data.values() for r in rows]
        legacy_ids = []
        for index in range(25, 34):
            file_id = self.add_raw(index)
            legacy_ids.append(file_id)
            with self.metadata.connection() as conn:
                conn.execute('DELETE FROM raw_binlog_archives WHERE file_id=?', (file_id,))
            row = dict(event_id=f'legacy-{index}', event_epoch_us=base+110+index,
                       raw_event_type='WriteRowsEventV2', operation='INSERT',
                       database_name='db', table_name='one', after_json='{"id":1}')
            path = Path(self.temp.name)/'fixture.ndjson'
            path.write_text(json.dumps(row)+'\n', encoding='utf-8')
            storage.ingest_ndjson_file(file_id=file_id, instance_id='test', host_instance_id='node',
                source_file_name=f'mysql-bin.{index:06}', ndjson_path=path)
        # A Parquet copy of a raw-authoritative file must never be re-read.
        raw_id = next(iter(data))
        path = Path(self.temp.name)/'fixture.ndjson'
        path.write_text(json.dumps(dict(event_id='stale-parquet-copy', event_epoch_us=base+150,
            raw_event_type='WriteRowsEventV2', operation='INSERT'))+'\n', encoding='utf-8')
        storage.ingest_ndjson_file(file_id=raw_id, instance_id='test', host_instance_id='node',
            source_file_name='mysql-bin.000000', ndjson_path=path)

        with self.metadata.connection() as conn:
            conn.execute("UPDATE raw_binlog_archives SET lo=?,hi=?,summary=json_set(summary,'$.lo',?,'$.hi',?)",
                         (base+100, base+200, base+100, base+200))

        def decode(_storage, _archive, entry, *args):
            yield from data[entry['file_id']]

        with patch('app.raw_binlog_query.decode', side_effect=decode), \
             patch.object(self.metadata, 'parts_in_range', wraps=self.metadata.parts_in_range) as parts:
            result = storage.query_events_tiered({'source': 'binlog', 'limit': 100,
                'start_epoch_us': base+100, 'end_epoch_us': base+200}, Settings(), Mock())
        ids = [r['event_id'] for r in result['rows']]
        self.assertEqual(set(ids), {r['event_id'] for r in expected} | {f'legacy-{i}' for i in range(25, 34)})
        self.assertEqual(len(ids), 84)
        self.assertEqual(result['rows'], sorted(result['rows'], key=storage._row_sort_key, reverse=True))
        self.assertFalse(result['has_more'])
        selected = [c.kwargs.get('binlog_batch_ids') for c in parts.call_args_list]
        self.assertTrue(selected)
        self.assertTrue(all(s is not None and len(s) <= MAX_FILES for s in selected))
        self.assertEqual({f for s in selected for f in s}, set(legacy_ids))
        self.assertNotIn(raw_id, {f for s in selected for f in s})


if __name__ == '__main__':
    unittest.main()
