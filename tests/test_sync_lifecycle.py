from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app.config import Settings
from app.credentials import CloudCredential
from app.pipeline import PipelineError, SyncManager
from app.server import RequestHandler, run_server
from app.sync_lifecycle import CollectorLease, PauseControl, sync_health


class PauseTests(unittest.TestCase):
    def test_maintenance_deadline_survives_restart_and_expires(self):
        with tempfile.TemporaryDirectory() as root:
            with patch('app.sync_lifecycle.time.time', return_value=1000):
                control = PauseControl(Path(root), 'fixture')
                control.pause(60)
            reopened = PauseControl(Path(root), 'fixture')
            with patch('app.sync_lifecycle.time.time', return_value=1059):
                self.assertFalse(reopened.expire())
            with patch('app.sync_lifecycle.time.time', return_value=1060):
                self.assertTrue(reopened.expire())
            self.assertEqual(PauseControl(Path(root), 'fixture').state['mode'], 'none')

    def test_manual_pause_never_expires(self):
        with tempfile.TemporaryDirectory() as root:
            control = PauseControl(Path(root), 'fixture')
            control.pause()
            control = PauseControl(Path(root), 'fixture')
            with patch('app.sync_lifecycle.time.time', return_value=1e12):
                self.assertFalse(control.expire())
            self.assertEqual(control.state['mode'], 'manual')
            control.clear()
            self.assertEqual(PauseControl(Path(root), 'fixture').state['mode'], 'none')

    def test_invalid_file_fails_closed_until_explicit_clear(self):
        with tempfile.TemporaryDirectory() as root:
            control = PauseControl(Path(root), 'fixture')
            control.pause(10)
            control.path.write_text('{invalid')
            with self.assertLogs('app.sync_lifecycle', 'ERROR'):
                control = PauseControl(Path(root), 'fixture')
            self.assertEqual(control.state['mode'], 'manual')
            self.assertFalse(control.expire())
            control.clear()
            self.assertEqual(PauseControl(Path(root), 'fixture').state['mode'], 'none')

    def test_failed_persistence_does_not_change_live_state(self):
        with tempfile.TemporaryDirectory() as root:
            control = PauseControl(Path(root), 'fixture')
            with patch('app.sync_lifecycle.os.replace', side_effect=OSError('fixture failure')):
                with self.assertRaises(OSError):
                    control.pause(10)
            self.assertEqual(control.state['mode'], 'none')
            self.assertEqual(list(control.path.parent.glob('*.tmp')), [])

    def test_instance_scope_and_path_traversal_safe(self):
        with tempfile.TemporaryDirectory() as root:
            first = PauseControl(Path(root), '../one')
            first.pause(10)
            second = PauseControl(Path(root), 'two')
            self.assertEqual(first.path.parent, Path(root) / 'sync-control')
            self.assertEqual(second.state['mode'], 'none')

    def test_invalid_durations_never_write(self):
        with tempfile.TemporaryDirectory() as root:
            control = PauseControl(Path(root), 'fixture')
            for value in [0, -1, 86401, True, '60', 1.5]:
                with self.subTest(value=value), self.assertRaises(ValueError):
                    control.pause(value)
            self.assertFalse(control.path.exists())


class LeaseTests(unittest.TestCase):
    def test_excludes_second_owner_and_releases_without_unlink(self):
        with tempfile.TemporaryDirectory() as root:
            with CollectorLease(Path(root)):
                with self.assertLogs('app.sync_lifecycle', 'ERROR'), self.assertRaises(RuntimeError):
                    with CollectorLease(Path(root)):
                        self.fail('second owner entered')
            with CollectorLease(Path(root)) as lease:
                self.assertTrue(lease.path.exists())
            self.assertTrue((Path(root) / 'collector.lock').exists())

    def test_second_process_cannot_acquire_live_owner_lock(self):
        with tempfile.TemporaryDirectory() as root, CollectorLease(Path(root)):
            script = 'from app.sync_lifecycle import CollectorLease; from pathlib import Path; import sys;\ntry: CollectorLease(Path(sys.argv[1])).__enter__()\nexcept RuntimeError: sys.exit(4)\nsys.exit(0)'
            result = subprocess.run([sys.executable, '-c', script, root], timeout=10, capture_output=True,
                                    **({'creationflags': subprocess.CREATE_NO_WINDOW} if os.name == 'nt' else {}))
            self.assertEqual(result.returncode, 4, result.stderr)
            self.assertIn(b'COLLECTOR_OWNER_CONFLICT', result.stderr)

    def test_lock_released_by_kernel_after_process_exit(self):
        with tempfile.TemporaryDirectory() as root:
            script = 'from app.sync_lifecycle import CollectorLease; from pathlib import Path; import os,sys; lease=CollectorLease(Path(sys.argv[1])); lease.__enter__(); os._exit(0)'
            result = subprocess.run([sys.executable, '-c', script, root], timeout=10, capture_output=True,
                                    **({'creationflags': subprocess.CREATE_NO_WINDOW} if os.name == 'nt' else {}))
            self.assertEqual(result.returncode, 0, result.stderr)
            with CollectorLease(Path(root)):
                pass

    def test_duplicate_server_rejected_before_initializing_application(self):
        with tempfile.TemporaryDirectory() as root, CollectorLease(Path(root)):
            with patch('app.server.Application') as application:
                with self.assertLogs('app.sync_lifecycle', 'ERROR'), self.assertRaises(RuntimeError):
                    run_server(0, root=Path(root))
                application.assert_not_called()


class ManagerTests(unittest.TestCase):
    def manager(self, root, auto=True):
        m = SyncManager.__new__(SyncManager)
        m.scope_instance_id = 'fixture'
        m.metadata = SimpleNamespace(path=Path(root) / 'metadata.sqlite3', latest_job=Mock(return_value=None))
        m._settings_loader = lambda: Settings(db_instance_id='rm-fixture001', auto_sync=auto)
        m._state_lock = threading.RLock()
        m._cold_boundary_lock = threading.Lock()
        m._pause_after_current = threading.Event()
        m._pause_controls = {}
        m._worker = None
        m._last_auto_start = float('-inf')
        m._last_health_log = ('', 0)
        m._last_auto_error = ''
        m.role = 'secondary'
        m.credential_loader = lambda _: CloudCredential('fixture', 'fixture')
        return m

    def one_tick(self, m):
        m._shutdown = SimpleNamespace(wait=Mock(side_effect=[False, True]))
        # A freshly booted Linux runner may have uptime below pollMinutes.
        with patch.object(m, 'start') as start, patch('app.pipeline.time.monotonic', return_value=1):
            m._scheduler_loop()
        return start

    def test_timed_idle_pause_is_persisted_and_scheduler_resumes_after_expiry(self):
        with tempfile.TemporaryDirectory() as root:
            m = self.manager(root)
            with patch('app.sync_lifecycle.time.time', return_value=1000):
                self.assertFalse(m.request_pause(resume_after_seconds=5))
                self.one_tick(m).assert_not_called()
            with patch('app.sync_lifecycle.time.time', return_value=1005):
                self.one_tick(m).assert_called_once_with(reason='auto')
            self.assertFalse(m._pause_after_current.is_set())
            self.assertTrue(m._settings().auto_sync)

    def test_expiry_never_reenables_disabled_auto_sync(self):
        with tempfile.TemporaryDirectory() as root:
            m = self.manager(root)
            with patch('app.sync_lifecycle.time.time', return_value=1000):
                m.request_pause(resume_after_seconds=5)
            m._settings_loader = lambda: Settings(db_instance_id='rm-fixture001', auto_sync=False)
            with patch('app.sync_lifecycle.time.time', return_value=1010):
                self.one_tick(m).assert_not_called()
            self.assertFalse(m._settings().auto_sync)

    def test_timed_pause_rejects_preexisting_disabled_setting(self):
        with tempfile.TemporaryDirectory() as root:
            m = self.manager(root, auto=False)
            with self.assertRaises(PipelineError) as raised:
                m.request_pause(resume_after_seconds=5)
            self.assertEqual(raised.exception.code, 'AUTO_SYNC_DISABLED')
            self.assertFalse(m._pause_after_current.is_set())

    def test_manual_pause_blocks_scheduler_and_auto_start_race(self):
        with tempfile.TemporaryDirectory() as root:
            m = self.manager(root)
            m._worker = SimpleNamespace(is_alive=lambda: True)
            m.request_pause()
            m._worker = None
            self.one_tick(m).assert_not_called()
            with self.assertRaises(PipelineError) as raised:
                m.start(reason='auto')
            self.assertEqual(raised.exception.code, 'SYNC_PAUSED')
            self.assertEqual(m._pause_control().state['mode'], 'manual')

    def test_start_failure_without_a_job_is_reported_unhealthy(self):
        with tempfile.TemporaryDirectory() as root:
            m = self.manager(root)
            m._shutdown = SimpleNamespace(wait=Mock(side_effect=[False, True]))
            with patch.object(m, 'start', side_effect=PipelineError('fixture', 'CREDENTIAL_REQUIRED')), patch('app.pipeline.time.monotonic', return_value=1):
                with self.assertLogs('app.pipeline', 'WARNING'):
                    m._scheduler_loop()
            self.assertEqual(m._collection_health(False, None)['state'], 'scheduler_error')

    def test_real_manager_reloads_manual_pause_after_restart(self):
        from app.metadata import MetadataStore
        from app.storage import EventStorage
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            store = MetadataStore(root / 'metadata.sqlite3')
            store.save_settings(Settings(db_instance_id='rm-fixture001'))
            storage = EventStorage(store, root)
            PauseControl(root, 'rm-fixture001').pause()
            m = SyncManager(store, storage, start_scheduler=False)
            try:
                self.assertTrue(m.status()['pauseRequested'])
                self.assertEqual(m.status()['health']['state'], 'paused')
            finally:
                m.shutdown()

    def test_start_clears_manual_pause_but_preserves_auto_sync_setting(self):
        with tempfile.TemporaryDirectory() as root:
            m = self.manager(root, auto=False)
            m._pause_control().pause()
            m._pause_after_current.set()
            m.metadata.create_job = Mock(return_value='fixture-job')
            with patch('app.pipeline.threading.Thread') as thread:
                self.assertEqual(m.start(), 'fixture-job')
            thread.return_value.start.assert_called_once()
            self.assertEqual(m._pause_control().state['mode'], 'none')
            self.assertFalse(m._settings().auto_sync)


class HealthTests(unittest.TestCase):
    def test_disabled_or_paused_cannot_report_caught_up(self):
        settings = Settings(db_instance_id='rm-fixture001')
        latest = {'status': 'success', 'performance': {'state': 'caught_up'}}
        for configured, pause, expected in [(replace(settings, auto_sync=False), {'mode': 'none'}, 'disabled'),
                                            (settings, {'mode': 'manual'}, 'paused'),
                                            (settings, {'mode': 'maintenance'}, 'maintenance')]:
            result = sync_health(configured, False, False, pause, latest)
            self.assertFalse(result['ok'])
            self.assertEqual(result['state'], expected)

    def test_stale_running_task_is_unhealthy_without_claiming_data_gap_size(self):
        with patch('app.sync_lifecycle.time.time', return_value=2000):
            result = sync_health(Settings(db_instance_id='rm-fixture001'), True, False, {'mode':'none'},
                                 {'started_at':'1970-01-01T00:00:01Z'})
        self.assertEqual(result['state'], 'stalled')
        self.assertFalse(result['ok'])

    def test_progress_event_prevents_false_stall(self):
        with patch('app.sync_lifecycle.time.time', return_value=2000):
            result = sync_health(Settings(db_instance_id='rm-fixture001'), True, False, {'mode':'none'},
                                 {'started_at':'1970-01-01T00:00:01Z', 'events':[{'created_at':'1970-01-01T00:33:19Z'}]})
        self.assertTrue(result['ok'])
        self.assertEqual(result['state'], 'running')


class ApiTests(unittest.TestCase):
    def handler(self, path, payload):
        h = RequestHandler.__new__(RequestHandler)
        app = Mock()
        app.metadata.load_settings.return_value = Settings(db_instance_id='rm-fixture001')
        h.server = SimpleNamespace(application=app)
        h.path, h.headers = path, {}
        h._valid_host = h._valid_origin = Mock(return_value=True)
        h._body_json = Mock(return_value=payload)
        h._json, h._error = Mock(), Mock()
        return h

    def test_accidental_disable_is_rejected_before_any_write(self):
        h = self.handler('/api/settings', {'autoSync': False})
        with self.assertLogs('app.server', 'ERROR'):
            h.do_POST()
        self.assertEqual(h._error.call_args.args[1], 'AUTO_SYNC_DISABLE_CONFIRMATION_REQUIRED')
        h.app.metadata.save_settings.assert_not_called()

    def test_confirmed_disable_is_saved_and_audited(self):
        h = self.handler('/api/settings', {'autoSync': False, 'confirmDisableAutoSync': True})
        h.app.storage.enforce_query_cache_limit.return_value = {'errors': []}
        with patch('app.server.credential_status', return_value={}), self.assertLogs('app.server', 'WARNING') as logs:
            h.do_POST()
        h._error.assert_not_called()
        self.assertFalse(h.app.metadata.save_settings.call_args.args[0].auto_sync)
        self.assertTrue(any('AUTO_SYNC_CHANGED' in line for line in logs.output))

    def test_timed_pause_is_routed_to_selected_instance(self):
        h = self.handler('/api/sync/pause', {'instanceId': 'rm-secondary', 'resumeAfterSeconds': 900})
        h.app.secondary_sync.return_value._pause_control.return_value.state = {'mode': 'maintenance'}
        h.do_POST()
        h._error.assert_not_called()
        h.app.secondary_sync.return_value.request_pause.assert_called_once_with(resume_after_seconds=900)
        h.app.sync.request_pause.assert_not_called()

    def test_invalid_duration_does_not_request_any_pause(self):
        for value in [None, 0, 86401, True, '60', 1.5]:
            h = self.handler('/api/sync/pause', {'resumeAfterSeconds': value})
            h.do_POST()
            h._error.assert_called_once()
            h.app.sync.request_pause.assert_not_called()

    def test_unknown_instance_does_not_pause_primary(self):
        h = self.handler('/api/sync/pause', {'instanceId': 'rm-unknown'})
        h.app.secondary_sync.return_value = None
        h.app.sync._job_scope.return_value = 'rm-fixture001'
        with self.assertLogs('app.server', 'ERROR'):
            h.do_POST()
        self.assertEqual(h._error.call_args.args[1], 'SYNC_INSTANCE_NOT_FOUND')
        h.app.sync.request_pause.assert_not_called()

    def test_liveness_independent_of_collection_readiness(self):
        h = self.handler('/api/sync/health', {})
        h.app.secondary_syncs = []
        h.app.sync.status.return_value = {'health': {'ok': False, 'state': 'disabled'}}
        h.do_GET()
        self.assertEqual(h._json.call_args.args[1], 503)
        h.path = '/healthz'
        h._json.reset_mock()
        h.do_GET()
        self.assertTrue(h._json.call_args.args[0]['ok'])


if __name__ == '__main__':
    unittest.main()
