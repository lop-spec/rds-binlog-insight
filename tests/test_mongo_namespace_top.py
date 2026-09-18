import unittest
from unittest.mock import Mock

from app.mongo_collector import MongoCollector, load_instances
from app.mongo_insight import collection_interval, summarize_namespace_intervals


def totals(read_count=0, read_us=0, write_count=0, write_us=0):
    return dict(read_count=read_count, read_us=read_us, write_count=write_count, write_us=write_us)


def sample(time_us, collections, *, node='node-a:3000', epoch='1:100', role='Primary'):
    return dict(node=node, epoch=epoch, role=role, time_us=time_us, timestamp=time_us // 1000,
                collections=collections)


class NamespaceInterval(unittest.TestCase):
    def test_first_sample_has_no_measurable_interval(self):
        result = collection_interval(None, sample(2_000_000, {'db.a': totals(read_count=5)}), [])
        self.assertEqual(result['status'], 'initial_sample')
        self.assertEqual(result['collections'], [])

    def test_difference_reports_only_active_namespaces(self):
        before = sample(1_000_000, {'db.a': totals(10, 500), 'db.b': totals(3, 30)})
        after = sample(61_000_000, {'db.a': totals(40, 2_500), 'db.b': totals(3, 30)})
        result = collection_interval(before, after, [])
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(result['interval_seconds'], 60.0)
        self.assertEqual([r['namespace'] for r in result['collections']], ['db.a'])
        self.assertEqual(result['collections'][0]['read_count'], 30)
        self.assertEqual(result['collections'][0]['read_us'], 2_000)

    def test_process_restart_is_not_differenced(self):
        before = sample(1_000_000, {'db.a': totals(10, 500)})
        after = sample(61_000_000, {'db.a': totals(40, 2_500)}, epoch='2:200')
        self.assertEqual(collection_interval(before, after, [])['status'], 'process_restarted')

    def test_node_or_role_change_is_not_differenced(self):
        before = sample(1_000_000, {'db.a': totals(10, 500)})
        self.assertEqual(collection_interval(before, sample(61_000_000, {'db.a': totals(40, 2_500)}, node='node-b:3000'), [])['status'],
                         'node_changed')
        self.assertEqual(collection_interval(before, sample(61_000_000, {'db.a': totals(40, 2_500)}, role='Secondary'), [])['status'],
                         'role_changed')

    def test_counter_reset_discards_the_whole_interval(self):
        before = sample(1_000_000, {'db.a': totals(10, 500), 'db.b': totals(9, 90)})
        after = sample(61_000_000, {'db.a': totals(40, 2_500), 'db.b': totals(1, 10)})
        result = collection_interval(before, after, [])
        self.assertEqual(result['status'], 'counter_reset')
        self.assertEqual(result['collections'], [])
        self.assertEqual(result['families'], [])

    def test_namespace_absent_from_the_earlier_sample_is_skipped(self):
        before = sample(1_000_000, {'db.a': totals(10, 500)})
        after = sample(61_000_000, {'db.a': totals(10, 500), 'db.new': totals(7, 70)})
        self.assertEqual(collection_interval(before, after, [])['collections'], [])

    def test_families_stay_complete_while_namespaces_truncate(self):
        before = sample(1_000_000, {f'db.msg_{i}': totals(0, 0) for i in range(10)})
        after = sample(61_000_000, {f'db.msg_{i}': totals(i + 1, (i + 1) * 100) for i in range(10)})
        result = collection_interval(before, after, ['msg'], limit=3)
        self.assertTrue(result['truncated'])
        self.assertEqual(result['observed_namespaces'], 10)
        self.assertEqual(len(result['collections']), 3)
        self.assertEqual([f['family'] for f in result['families']], ['db.msg_*'])
        self.assertEqual(result['families'][0]['shards'], 10)
        self.assertEqual(result['families'][0]['read_us'], sum((i + 1) * 100 for i in range(10)))

    def test_ranking_uses_lock_time_not_operation_count(self):
        before = sample(1_000_000, {'db.busy': totals(0, 0), 'db.slow': totals(0, 0)})
        after = sample(61_000_000, {'db.busy': totals(1_000, 1_000), 'db.slow': totals(2, 900_000)})
        result = collection_interval(before, after, [])
        self.assertEqual(result['collections'][0]['namespace'], 'db.slow')


class NamespaceWindowSummary(unittest.TestCase):
    def point(self, start_us, end_us, rows, *, families=(), status='ok', role='Primary', truncated=False):
        return dict(status=status, role=role, start_us=start_us, end_us=end_us,
                    interval_seconds=(end_us - start_us) / 1e6, collections=rows,
                    families=list(families), truncated=truncated)

    def test_interval_crossing_the_boundary_is_dropped_not_scaled(self):
        inside = self.point(10, 20, [dict(namespace='db.a', **totals(5, 50))])
        crossing = self.point(15, 40, [dict(namespace='db.a', **totals(100, 100_000))])
        result = summarize_namespace_intervals([inside, crossing], 0, 30)
        self.assertEqual(result['intervals'], 1)
        self.assertEqual(result['collections'][0]['read_us'], 50)

    def test_failed_intervals_do_not_contribute(self):
        failed = self.point(10, 20, [dict(namespace='db.a', **totals(5, 50))], status='counter_reset')
        result = summarize_namespace_intervals([failed], 0, 30)
        self.assertEqual(result['intervals'], 0)
        self.assertEqual(result['collections'], [])

    def test_role_filter_selects_matching_intervals(self):
        primary = self.point(10, 20, [dict(namespace='db.a', **totals(5, 50))])
        secondary = self.point(10, 20, [dict(namespace='db.a', **totals(9, 90))], role='Secondary')
        result = summarize_namespace_intervals([primary, secondary], 0, 30, role='Secondary')
        self.assertEqual(result['intervals'], 1)
        self.assertEqual(result['collections'][0]['read_us'], 90)

    def test_truncation_is_propagated_to_the_window(self):
        truncated = self.point(10, 20, [dict(namespace='db.a', **totals(5, 50))], truncated=True)
        self.assertTrue(summarize_namespace_intervals([truncated], 0, 30)['truncated_intervals'])

    def test_families_accumulate_across_intervals(self):
        rows = [dict(namespace='db.msg_1', **totals(1, 10))]
        families = [dict(family='db.msg_*', shards=4, **totals(1, 10))]
        first = self.point(0, 60_000_000, rows, families=families)
        second = self.point(60_000_000, 120_000_000, rows, families=families)
        result = summarize_namespace_intervals([first, second], 0, 180_000_000)
        self.assertEqual(result['families'][0]['read_us'], 20)
        self.assertEqual(result['families'][0]['shards'], 4)
        self.assertEqual(result['observed_seconds'], 120.0)


class TopCollection(unittest.TestCase):
    def collector(self):
        return MongoCollector(Mock(), {'instanceId': 'dds-example', 'region': 'cn-example-1', 'families': []}, lambda: None)

    def test_top_failure_degrades_without_breaking_the_native_lane(self):
        collector = self.collector()
        client = Mock()
        client.admin.command.side_effect = RuntimeError('not authorized on admin')
        collector.clients['host-a'] = client
        self.assertEqual(collector.namespace_totals('host-a'), {})
        self.assertEqual(collector.state['top:host-a'], 'RuntimeError')

    def test_totals_are_normalized_and_note_is_dropped(self):
        collector = self.collector()
        client = Mock()
        client.admin.command.return_value = {'totals': {
            'note': 'provided for backwards compatibility',
            'db.a': {'readLock': {'count': 3, 'time': 30}, 'writeLock': {'count': 1, 'time': 10}}}}
        collector.clients['host-a'] = client
        self.assertEqual(collector.namespace_totals('host-a'),
                         {'db.a': dict(read_count=3, read_us=30, write_count=1, write_us=10)})

    def test_publish_is_skipped_when_totals_are_unavailable(self):
        collector = self.collector()
        collector.publish_namespaces('host-a', dict(sample(1_000_000, {}), collections={}))
        collector.store.telemetry.assert_not_called()

    def test_publish_emits_one_point_per_successful_interval(self):
        collector = self.collector()
        collector.before['host-a'] = sample(1_000_000, {'db.a': totals(10, 500)})
        collector.publish_namespaces('host-a', sample(61_000_000, {'db.a': totals(40, 2_500)}))
        instance, kind, points = collector.store.telemetry.call_args[0]
        self.assertEqual((instance, kind), ('dds-example', 'namespaces'))
        self.assertEqual(points[0]['metric'], 'top')
        self.assertEqual(points[0]['period'], 60)
        self.assertEqual(points[0]['collections'][0]['read_count'], 30)

    def test_limit_outside_the_supported_range_is_rejected(self):
        import json
        import tempfile
        from pathlib import Path
        entry = {'instanceId': 'dds-example', 'region': 'cn-example-1', 'families': [], 'topNamespaceLimit': 0}
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'mongo-instances.json').write_text(json.dumps([entry]), encoding='utf-8')
            with self.assertRaises(ValueError):
                load_instances(root)
            entry['topNamespaceLimit'] = 200
            (root / 'mongo-instances.json').write_text(json.dumps([entry]), encoding='utf-8')
            self.assertEqual(load_instances(root)[0]['topNamespaceLimit'], 200)


if __name__ == '__main__':
    unittest.main()
