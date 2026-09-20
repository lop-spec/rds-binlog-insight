"""Stale-ready regression: native loss must neither serve empty nor stay idle."""
from __future__ import annotations

import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from app.clickhouse_manifest import ClickHouseManifest
from app.clickhouse_query import ClickHouseQueryBackend, ClickHouseRawOssUnavailable
from app.clickhouse_raw_oss import (
    ClickHouseRawOssConfig, PACKED_VERIFY_SETTINGS, build_packed_state_sql,
    packed_state_matches,
)
from app.clickhouse_raw_sync import audit_ready_packed_parts
from app.config import Settings


def part(index=0):
    return dict(path=f"/fixture/{index:04d}.parquet", part_path=f"/fixture/{index:04d}.parquet",
                logical_part_id=f"{index:064x}", sha256="b" * 64,
                content_revision=1, row_count=100_000, size_bytes=1024,
                min_event_epoch_us=100, max_event_epoch_us=200,
                oss_key="fixture/pack", oss_length=1024, oss_offset=100)


def native(item):
    return dict(part_key=item['logical_part_id'], rows=item['row_count'], sha_count=1,
                sha256=item['sha256'], min_revision=item['content_revision'],
                max_revision=item['content_revision'])


def raw_config():
    return replace(ClickHouseRawOssConfig.from_env(), enabled=True,
                   serving_enabled=True, packed_table='events_query_packed_v1')


class PackedIntegrityTests(unittest.TestCase):
    def backend(self):
        backend = ClickHouseQueryBackend.__new__(ClickHouseQueryBackend)
        backend.config = SimpleNamespace(database='insight')
        backend.raw_config = raw_config()
        backend.metadata = SimpleNamespace(
            clickhouse_change_tracking_state=lambda: dict(complete=True, pending=False),
            load_settings=lambda: Settings(),
        )
        backend.raw_pack_manifest = SimpleNamespace(window_coverage=lambda **kw: dict(complete=True, covered_parts=1))
        return backend

    def test_full_query_rejects_missing_native_part_despite_empty_queue_and_ready_flag(self):
        backend = self.backend()
        calls = []
        def query(sql, parameters, control, **kwargs):
            calls.append(sql)
            if ' AS part_key' in sql:
                return []
            if ' FINAL' in sql:
                return [part()]
            self.fail('must not read/publish an incomplete native body')
        backend._query_with_cancel = query
        with self.assertLogs('app.clickhouse_query', level='ERROR'), self.assertRaises(ClickHouseRawOssUnavailable):
            backend._query_raw_events({'source': 'binlog'}, start_us=100, end_us=200,
                                      limit_cap=1000, control=None)
        self.assertEqual(len(calls), 2)

    def test_loss_during_empty_result_read_is_detected_before_return(self):
        backend = self.backend()
        checks = []
        def query(sql, parameters, control, **kwargs):
            if ' AS part_key' in sql:
                checks.append(1)
                return [native(part())] if len(checks) == 1 else []
            if ' FINAL' in sql:
                return [part()]
            return []
        backend._query_with_cancel = query
        with self.assertLogs('app.clickhouse_query', level='ERROR'), self.assertRaises(ClickHouseRawOssUnavailable):
            backend._query_raw_events({'source': 'binlog'}, start_us=100, end_us=200,
                                      limit_cap=1000, control=None)
        self.assertEqual(len(checks), 2)

    def test_exact_native_state_required(self):
        expected = part()
        self.assertTrue(packed_state_matches(expected, native(expected)))
        for mismatch in ({}, {'rows': 99999}, {'rows': 100001}, {'sha_count': 2},
                         {'sha256': 'c' * 64}, {'min_revision': 0}, {'max_revision': 2}):
            state = {**native(expected), **mismatch} if mismatch else {}
            self.assertFalse(packed_state_matches(expected, state), mismatch)

    def test_direct_objects_skip_native_verification(self):
        backend = self.backend()
        backend._query_with_cancel = Mock()
        backend._verify_raw_packed_candidates([{**part(), 'oss_length': 0}], None)
        backend._query_with_cancel.assert_not_called()

    def test_verification_is_cancel_aware_and_budgeted(self):
        backend = self.backend()
        backend._query_with_cancel = Mock(return_value=[native(part())])
        control = object()
        backend._verify_raw_packed_candidates([part()], control)
        call = backend._query_with_cancel.call_args
        self.assertIs(call.args[2], control)
        self.assertEqual(call.kwargs['settings'], PACKED_VERIFY_SETTINGS)
        backend._query_with_cancel.side_effect = TimeoutError('cancelled or over budget')
        with self.assertRaises(TimeoutError):
            backend._verify_raw_packed_candidates([part()], control)

    def test_builder_binds_keys_and_caps_page(self):
        sql, parameters = build_packed_state_sql(raw_config(), database='insight', parts=[part()])
        self.assertNotIn(part()['logical_part_id'], sql)
        self.assertIn(part()['logical_part_id'], parameters.values())
        for parts in ([], [part(i) for i in range(65)]):
            with self.assertRaises(ValueError):
                build_packed_state_sql(raw_config(), database='insight', parts=parts)

    def manifest(self, root, count=1):
        manifest = ClickHouseManifest(root / 'manifest.sqlite3', run_migrations=True)
        manifest.reconcile([part(i) for i in range(count)], start_epoch_us=0, end_epoch_us=300)
        for i in range(count):
            self.assertTrue(manifest.mark_ready(part(i)['path'], part(i)['logical_part_id'], 100_000))
        return manifest

    def test_audit_requeues_only_corrupt_parts_then_existing_lane_claims_them(self):
        with tempfile.TemporaryDirectory() as root:
            manifest = self.manifest(Path(root), 3)
            client = SimpleNamespace(json_rows=Mock(return_value=[native(part(0)), {**native(part(2)), 'rows': 1}]))
            with self.assertLogs('app.clickhouse_raw_sync', level='ERROR'):
                result = audit_ready_packed_parts(manifest, client, raw_config(), database='insight')
            self.assertEqual((result['checked'], result['invalidated']), (3, 2))
            self.assertEqual(manifest.part_status(part(0)['path'], part(0)['logical_part_id']), 'ready')
            self.assertEqual(manifest.claim_next()['job_kind'], 'load')
            self.assertEqual(client.json_rows.call_args.kwargs['timeout'], 12)
            self.assertEqual(client.json_rows.call_args.kwargs['settings'], PACKED_VERIFY_SETTINGS)

    def test_audit_failure_never_changes_ready_state(self):
        with tempfile.TemporaryDirectory() as root:
            manifest = self.manifest(Path(root))
            client = SimpleNamespace(json_rows=Mock(side_effect=RuntimeError('network failed')))
            with self.assertRaisesRegex(RuntimeError, 'network failed'):
                audit_ready_packed_parts(manifest, client, raw_config(), database='insight')
            self.assertEqual(manifest.part_status(part()['path'], part()['logical_part_id']), 'ready')

    def test_cursor_has_no_skip_and_completes_without_whole_table_fetch(self):
        with tempfile.TemporaryDirectory() as root:
            manifest = self.manifest(Path(root), 70)
            seen, cursor = [], ('', '')
            for _ in range(3):
                page = manifest.ready_parts_page(after=cursor)
                self.assertLessEqual(len(page), 64)
                seen.extend(p['logical_part_id'] for p in page)
                if not page:
                    break
                cursor = (page[-1]['part_path'], page[-1]['logical_part_id'])
            self.assertEqual(seen, [part(i)['logical_part_id'] for i in range(70)])
            result = audit_ready_packed_parts(manifest, Mock(), raw_config(), database='insight', after=cursor)
            self.assertTrue(result['cycleComplete'])
            self.assertEqual(result['after'], ('', ''))

    def test_audit_cas_does_not_revive_deletion_or_invalidate_new_generation(self):
        with tempfile.TemporaryDirectory() as root:
            manifest = self.manifest(Path(root), 3)
            audited = manifest.ready_parts_page()
            manifest.queue_missing_paths([part(0)['path']])
            with manifest.connection() as conn:
                conn.execute("UPDATE clickhouse_parts SET content_revision=2 WHERE part_path=?", (part(1)['path'],))
                conn.execute("UPDATE clickhouse_parts SET ready_at_us=ready_at_us+1 WHERE part_path=?", (part(2)['path'],))
            self.assertEqual(manifest.invalidate_ready_parts(audited), 0)
            self.assertEqual(manifest.part_status(part(0)['path'], part(0)['logical_part_id']), 'delete_pending')


if __name__ == '__main__':
    unittest.main()
