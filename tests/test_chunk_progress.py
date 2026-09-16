from __future__ import annotations

import sqlite3
import subprocess
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from app.config import Settings
from app.metadata import MetadataStore
from app.rds_api import RemoteBinlog


class ChunkProgressTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / 'metadata.sqlite3'
        migration_owner = MetadataStore(self.path)
        migration_owner.close()
        self.store = MetadataStore(self.path, run_migrations=False)
        self.addCleanup(self.store.close)
        item = RemoteBinlog(
            log_file_name='mysql-bin.fixture', log_begin_utc='2026-08-21T00:00:00Z',
            log_end_utc='2026-08-21T01:00:00Z', file_size=42,
            checksum_crc64='', download_link='', intranet_download_link='',
            link_expired_utc='', remote_status='Completed', host_instance_id='host-a')
        self.file_id, _ = self.store.upsert_remote(Settings(db_instance_id='rm-fixture'), item)
        self.job_id = self.store.create_job('sync', 'rm-fixture', 'original progress')
        self.store.set_file_state(self.file_id, 'parsing', event_count=7,
                                  error_code='old-error', error_message='old-message')

    def state(self):
        with self.store.connection() as conn:
            file = dict(conn.execute('SELECT * FROM binlog_files WHERE id = ?', (self.file_id,)).fetchone())
            job = dict(conn.execute('SELECT * FROM jobs WHERE id = ?', (self.job_id,)).fetchone())
            events = [dict(r) for r in conn.execute('SELECT * FROM job_events WHERE job_id = ? ORDER BY id', (self.job_id,))]
            return file, job, events

    def record(self, count=11):
        self.store.record_file_chunk_progress(self.file_id, count, job_id=self.job_id,
                                              message='new progress', event_message='chunk published')

    def test_matches_previous_visible_updates_with_one_durable_commit(self):
        connection = self.store.connection
        opened, statements = [], []

        @contextmanager
        def traced():
            with connection() as conn:
                opened.append(conn)
                self.assertEqual(conn.execute('PRAGMA synchronous').fetchone()[0], 2)
                self.assertEqual(conn.execute('PRAGMA wal_autocheckpoint').fetchone()[0], 1000)
                conn.set_trace_callback(lambda sql: statements.append((sql, conn.in_transaction)))
                yield conn

        # The previous path opened three autocommit connections for these writes.
        with patch.object(self.store, 'connection', traced), patch('app.metadata.utc_now_text', return_value='2026-08-21T02:00:00Z'):
            self.store.set_file_state(self.file_id, 'parsing', event_count=11)
            self.store.update_job(self.job_id, message='new progress')
            self.store.add_job_event(self.job_id, 'info', 'FILE_CHUNK_PUBLISHED', 'chunk published')
        self.assertEqual(len(opened), 3)
        expected = self.state()
        with self.store.connection() as conn:
            conn.execute('DELETE FROM job_events WHERE job_id = ?', (self.job_id,))
        opened.clear()
        statements.clear()
        with patch.object(self.store, 'connection', traced), patch('app.metadata.utc_now_text', return_value='2026-08-21T02:00:00Z'):
            self.record()
        self.assertEqual(len(opened), 1)
        self.assertEqual(sum(sql == 'BEGIN IMMEDIATE' for sql, _ in statements), 1)
        self.assertEqual(sum(sql == 'COMMIT' for sql, _ in statements), 1)
        writes = [(sql, in_transaction) for sql, in_transaction in statements if sql.startswith(('UPDATE ', 'INSERT '))]
        self.assertEqual(len(writes), 3)
        self.assertTrue(all(in_transaction for _, in_transaction in writes))
        actual = self.state()
        for event in expected[2] + actual[2]:
            event.pop('id')
        self.assertEqual(actual, expected)
        self.assertFalse(self.store._wal_anchor.in_transaction)

    def test_background_progress_preserves_job_visibility_and_raw_boundary(self):
        before = self.state()
        self.store.record_file_chunk_progress(self.file_id, 15)
        file, job, events = self.state()
        self.assertEqual(file['state'], 'parsing')
        self.assertEqual(file['event_count'], 15)
        self.assertEqual((file['error_code'], file['error_message']), ('', ''))
        for name in ['query_visible', 'completed_at', 'raw_deleted_at', 'processing_started_at', 'processing_seconds']:
            self.assertEqual(file[name], before[0][name])
        self.assertEqual((job, events), before[1:])

    def test_failed_event_insert_rolls_back_file_and_job_and_allows_retry(self):
        before = self.state()
        with self.store.connection() as conn:
            conn.execute("CREATE TRIGGER fail_event BEFORE INSERT ON job_events BEGIN SELECT RAISE(ABORT, 'injected event failure'); END")
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'injected event failure'):
            self.record()
        self.assertEqual(self.state(), before)
        with self.store.connection() as conn:
            conn.execute('DROP TRIGGER fail_event')
        self.record()
        file, job, events = self.state()
        self.assertEqual((file['event_count'], job['message'], len(events)), (11, 'new progress', 1))

    def test_missing_job_foreign_key_failure_rolls_back_file(self):
        before = self.state()
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.record_file_chunk_progress(self.file_id, 99, job_id='missing',
                                                  message='invalid', event_message='invalid')
        self.assertEqual(self.state(), before)

    def run_crash_child(self, mode):
        self.store.close()
        source = '''
import os, sys
from contextlib import contextmanager
from pathlib import Path
from app.metadata import MetadataStore
store = MetadataStore(Path(sys.argv[1]), run_migrations=False)
if sys.argv[4] == 'during':
    connection = store.connection
    @contextmanager
    def crash_connection():
        with connection() as conn:
            conn.create_function('crash_before_commit', 0, lambda: os._exit(74))
            conn.execute('CREATE TEMP TRIGGER crash_event BEFORE INSERT ON job_events BEGIN SELECT crash_before_commit(); END')
            yield conn
    store.connection = crash_connection
store.record_file_chunk_progress(sys.argv[2], 19, job_id=sys.argv[3], message='durable progress', event_message='durable event')
os._exit(73)
'''
        result = subprocess.run(
            [sys.executable, '-c', source, str(self.path), self.file_id, self.job_id, mode],
            capture_output=True, text=True, timeout=15,
            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
        self.assertEqual(result.returncode, 74 if mode == 'during' else 73, result.stderr)
        self.store = MetadataStore(self.path, run_migrations=False)
        self.addCleanup(self.store.close)

    def test_committed_progress_survives_abrupt_process_exit(self):
        self.run_crash_child('after')
        file, job, events = self.state()
        self.assertEqual((file['event_count'], job['message'], len(events)), (19, 'durable progress', 1))
        self.assertEqual(events[0]['message'], 'durable event')
        self.assertEqual((file['state'], file['completed_at'], file['raw_deleted_at']), ('parsing', '', ''))

    def test_abrupt_exit_before_commit_leaves_no_partial_progress(self):
        before = self.state()
        self.run_crash_child('during')
        self.assertEqual(self.state(), before)
        self.record()
        self.assertEqual(self.state()[0]['event_count'], 11)

    def test_commit_failure_rolls_back_and_is_not_swallowed(self):
        before = self.state()
        connection = self.store.connection

        class CommitFailure:
            def __init__(self, conn):
                self.conn = conn

            def __getattr__(self, name):
                return getattr(self.conn, name)

            def commit(self):
                raise sqlite3.OperationalError('injected commit failure')

        @contextmanager
        def failing_connection():
            with connection() as conn:
                yield CommitFailure(conn)

        with patch.object(self.store, 'connection', failing_connection):
            with self.assertRaisesRegex(sqlite3.OperationalError, 'injected commit failure'):
                self.record()
        self.assertEqual(self.state(), before)


if __name__ == '__main__':
    unittest.main()
