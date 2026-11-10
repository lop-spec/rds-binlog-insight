import json
import os
import tempfile
import threading
import unittest
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.config import Settings
from app.server import AppHTTPServer
from app.starrocks_metrics import StarRocksMetrics, load_instances


NOW = 1_900_000_000_000
INSTANCE = 'c-example01'


class FakeClient:
    def __init__(self, pages):
        self.pages = list(pages)
        self.calls = []

    def call(self, action, params):
        self.calls.append((action, dict(params)))
        if not self.pages:
            raise AssertionError('unexpected remote request')
        return self.pages.pop(0)


def point(timestamp=NOW-60_000, node='frontend-0', identity=INSTANCE, value=4.5):
    return {'timestamp': timestamp, 'podName': node, 'clusterId': identity, 'Average': value}


def page(points, token=None):
    result = {'Code': '200', 'Datapoints': json.dumps(points)}
    if token is not None:
        result['NextToken'] = token
    return result


class StarRocksMetricsTest(unittest.TestCase):
    def setUp(self):
        self.mode = patch.dict(os.environ, {'RDS_BINLOG_CLOUD_AUTH_MODE': 'ecs_ram_role'})
        self.mode.start()
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.config = self.root / 'starrocks-instances.json'
        self.settings = Settings(db_instance_id='rm-example', region_id='cn-chengdu')
        self.credential_calls = []
        self.client_settings = []
        self.client = FakeClient([])
        self.write_config([{'instanceId': INSTANCE, 'region': 'cn-hangzhou', 'label': 'fixture', 'enabled': True}])

    def tearDown(self):
        self.temp.cleanup()
        self.mode.stop()

    def write_config(self, entries):
        self.config.write_text(json.dumps(entries), encoding='utf-8')

    def service(self):
        def credentials(target):
            self.credential_calls.append(target)
            return object()
        def factory(settings, credential):
            self.client_settings.append(settings)
            return self.client
        return StarRocksMetrics(self.root, lambda: self.settings, credentials,
                                client_factory=factory, now_ms=lambda: NOW)

    def query(self, **overrides):
        params = {'instance': INSTANCE, 'metric': 'fe_qps', 'startEpochUs': str((NOW-600_000)*1000),
                  'endEpochUs': str(NOW*1000)}
        params.update(overrides)
        return params

    def test_static_cloud_credentials_are_never_loaded(self):
        service = self.service()
        with patch.dict(os.environ, {'RDS_BINLOG_CLOUD_AUTH_MODE': 'access_key'}):
            with self.assertRaisesRegex(RuntimeError, 'requires_ecs_role'):
                service.query(self.query())
        self.assertEqual(self.credential_calls, [])
        self.assertEqual(self.client.calls, [])

    def test_status_is_non_network_and_audit_not_connected(self):
        service = self.service()
        status = service.status()
        self.assertEqual(status['status'], 'ok')
        self.assertEqual(status['audit']['status'], 'not_connected')
        self.assertEqual(status['running_queries']['status'], 'not_connected')
        self.assertEqual(self.credential_calls, [])
        self.assertEqual(self.client.calls, [])

    def test_missing_or_disabled_config_is_not_available(self):
        self.config.unlink()
        self.assertEqual(self.service().status()['status'], 'not_configured')
        self.write_config([{'instanceId': INSTANCE, 'region': 'cn-hangzhou', 'enabled': False}])
        self.assertEqual(self.service().status()['status'], 'not_configured')
        with self.assertRaisesRegex(ValueError, 'configured_starrocks_instance_required'):
            self.service().query(self.query())
        self.assertEqual(self.credential_calls, [])

    def test_unknown_instance_and_metric_fail_before_credential(self):
        service = self.service()
        with self.assertRaisesRegex(ValueError, 'configured_starrocks_instance_required'):
            service.query(self.query(instance='c-other01'))
        with self.assertRaisesRegex(ValueError, 'unsupported_starrocks_metric'):
            service.query(self.query(metric='arbitrary'))
        self.assertEqual(self.credential_calls, [])

    def test_instances_are_validated_and_duplicates_refused(self):
        for entry in [
            {'instanceId': '../c-example', 'region': 'cn-hangzhou', 'enabled': True},
            {'instanceId': INSTANCE, 'region': 'evil.example', 'enabled': True},
            {'instanceId': INSTANCE, 'region': 'cn-hangzhou', 'enabled': 'false'},
        ]:
            self.write_config([entry])
            self.assertEqual(self.service().status()['status'], 'unavailable')
        entry = {'instanceId': INSTANCE, 'region': 'cn-hangzhou', 'enabled': True}
        self.write_config([entry, entry])
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            load_instances(self.root)

    def test_cross_region_paginates_and_preserves_nodes(self):
        self.client.pages = [page([point(node='frontend-0')], 'next'), page([point(node='frontend-1')])]
        result = self.service().query(self.query())
        self.assertEqual(result['nodes'], ['frontend-0', 'frontend-1'])
        self.assertEqual(len(result['points']), 2)
        self.assertFalse(result['complete'])
        self.assertEqual(result['audit_status'], 'not_connected')
        self.assertEqual(self.client_settings[0].endpoint, 'https://metrics.cn-hangzhou.aliyuncs.com')
        self.assertEqual(self.client_settings[0].region_id, 'cn-hangzhou')
        self.assertEqual(self.client.calls[0][1]['Period'], '60')
        self.assertEqual(self.client.calls[1][1]['NextToken'], 'next')
        self.assertEqual(json.loads(self.client.calls[0][1]['Dimensions']), {'clusterId': INSTANCE})

    def test_longer_window_uses_five_minute_period(self):
        self.client.pages = [page([])]
        service = self.service()
        result = service.query(self.query(startEpochUs=str((NOW-2*86400*1000)*1000)))
        self.assertEqual(result['period'], 300)
        self.assertEqual(self.client.calls[0][1]['Period'], '300')
        self.assertEqual(result['points'], [])
        self.assertFalse(result['complete'])

    def test_window_and_duplicate_query_parameter_rejected(self):
        service = self.service()
        for overrides in [
            {'startEpochUs': 'bad'},
            {'startEpochUs': str(NOW*1000)},
            {'startEpochUs': str((NOW-8*86400*1000)*1000)},
            {'endEpochUs': str((NOW+120_000)*1000)},
            {'instance': [INSTANCE, INSTANCE]},
        ]:
            with self.assertRaises(ValueError):
                service.query(self.query(**overrides))
        self.assertFalse(self.credential_calls)

    def test_wrong_cluster_rejected_instead_of_misattributed(self):
        self.client.pages = [page([point(identity='c-other01')])]
        with self.assertRaisesRegex(RuntimeError, 'identity_mismatch'):
            self.service().query(self.query())

    def test_missing_node_nan_bool_and_time_outside_window_rejected(self):
        for row in [point(node=''), point(value=float('nan')), point(value=True), point(timestamp=NOW-900_000)]:
            self.client.pages = [page([row])]
            with self.assertRaisesRegex(RuntimeError, 'invalid_metric_point'):
                self.service().query(self.query())

    def test_duplicate_points_rejected(self):
        self.client.pages = [page([point(), point(value=99)])]
        with self.assertRaisesRegex(RuntimeError, 'conflicting_metric_points'):
            self.service().query(self.query())

    def test_pagination_cycle_rejected(self):
        self.client.pages = [page([], 'repeat'), page([], 'repeat')]
        with self.assertRaisesRegex(RuntimeError, 'pagination_cycle'):
            self.service().query(self.query())

    def test_failed_response_does_not_become_zero(self):
        self.client.pages = [{'Code': '403', 'Datapoints': '[]'}]
        with self.assertRaisesRegex(RuntimeError, 'cms_error'):
            self.service().query(self.query())

    def test_concurrent_request_gate(self):
        service = self.service()
        self.assertTrue(service._gate.acquire(blocking=False))
        self.assertTrue(service._gate.acquire(blocking=False))
        try:
            with self.assertRaisesRegex(RuntimeError, 'metrics_busy'):
                service.query(self.query())
        finally:
            service._gate.release()
            service._gate.release()
        self.assertFalse(self.credential_calls)

    def test_http_routes_reach_real_service_and_reject_other_cluster(self):
        self.client.pages = [page([point()])]
        server = AppHTTPServer(('127.0.0.1', 0), SimpleNamespace(starrocks=self.service()))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        base = 'http://127.0.0.1:' + str(server.server_address[1])
        try:
            with urllib.request.urlopen(base + '/api/starrocks/status', timeout=5) as response:
                self.assertEqual(json.load(response)['data']['audit']['status'], 'not_connected')
            query = urllib.parse.urlencode(self.query())
            with urllib.request.urlopen(base + '/api/starrocks/metrics?' + query, timeout=5) as response:
                self.assertEqual(json.load(response)['data']['nodes'], ['frontend-0'])
            query = urllib.parse.urlencode(self.query(instance='c-other01'))
            with self.assertRaises(urllib.error.HTTPError) as raised:
                urllib.request.urlopen(base + '/api/starrocks/metrics?' + query, timeout=5)
            self.assertEqual(raised.exception.code, 400)
            raised.exception.close()
            with urllib.request.urlopen(base + '/assets/starrocks.js', timeout=5) as response:
                self.assertIn(b'runStarRocksMetrics', response.read())
            self.assertEqual(len(self.client.calls), 1)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


if __name__ == '__main__':
    unittest.main()
