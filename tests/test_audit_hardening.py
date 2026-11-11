from __future__ import annotations
import gzip
import io
import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from app.audit_redaction import redact_secrets
from app.config import Settings, parse_settings_payload
from app.http_access import load_networks, peer_allowed
from app.metadata import MetadataStore
from app.server import AppHTTPServer, _event_query
from app.tabularis_audit import TabularisAuditIngest
from app.slowlog_tail import NodeTail
from app.slow_log_collector import SlowLogConfig, DasRpcClient
from app.credentials import CloudCredential


def event(identity='a', **changes):
    return {**dict(event_id=identity, instance_id='rm-fixture', execution_status='success',
                   started_epoch_us=1, finished_epoch_us=2, sql_text='SELECT 1'), **changes}


class AuditDurability(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = MetadataStore(Path(self.temp.name) / 'meta.db')
        self.ingest = TabularisAuditIngest(self.store, Mock(), archiver=Mock(), token='fixture', allowed_instances=['rm-fixture'])

    def tearDown(self):
        self.ingest.shutdown()
        self.temp.cleanup()

    def test_ack_is_durable_even_without_oss_or_parquet(self):
        result = self.ingest.ingest({'events': [event()]})
        self.assertEqual(result['acknowledged_event_ids'], ['a'])
        self.ingest.storage.ingest_file.assert_not_called()
        self.ingest.archiver.archive_parts_now.assert_not_called()
        self.assertEqual(self.store.local_execution_event_detail('a')['sql_text'], 'SELECT 1')
        self.assertEqual(self.ingest.acknowledged(['a', 'missing']), ['a'])

    def test_replay_same_id_is_immutable_and_conflict_is_explicit(self):
        self.ingest.ingest({'events': [event()]})
        result = self.ingest.ingest({'events': [event(sql_text='SELECT 2')]})
        self.assertEqual(result['acknowledged_event_ids'], [])
        self.assertEqual(result['rejected'][0]['code'], 'AUDIT_ID_CONFLICT')
        self.assertEqual(self.store.local_execution_event_detail('a')['sql_text'], 'SELECT 1')

    def test_same_content_retry_is_acknowledged_without_overwrite(self):
        self.ingest.ingest({'events': [event()]})
        result = self.ingest.ingest({'events': [event()]})
        self.assertEqual((result['duplicates'], result['accepted']), (1, 0))
        self.assertEqual(result['acknowledged_event_ids'], ['a'])

    def test_one_invalid_row_does_not_block_good_rows(self):
        result = self.ingest.ingest({'events': [None, event('b'), {'event_id': 'bad'}]})
        self.assertEqual(result['acknowledged_event_ids'], ['b'])
        self.assertEqual(len(result['rejected']), 2)
        self.assertFalse(result['acknowledged'])

    def test_duplicate_changed_content_within_one_batch_is_rejected(self):
        result = self.ingest.ingest({'events': [event(), event(sql_text='DROP TABLE x')]})
        self.assertEqual(result['accepted'], 1)
        self.assertEqual(result['rejected'][0]['code'], 'AUDIT_ID_CONFLICT')

    def test_index_backfill_cannot_replace_an_event(self):
        self.store.record_tabularis_audit_log([event()])
        self.store.record_tabularis_audit_log([event(sql_text='UPDATE x SET y=1')])
        self.assertEqual(self.store.local_execution_event_detail('a')['sql_text'], 'SELECT 1')

    def test_health_survives_new_ingest_object(self):
        self.ingest.ingest({'events': [event()]})
        restarted = TabularisAuditIngest(self.store, Mock(), archiver=Mock(), allowed_instances=['rm-fixture'])
        self.assertEqual(restarted.status()['states'], {'accepted': 1})
        self.assertTrue(restarted.status()['lastAcceptedUtc'])

    def test_wildcards_are_literal(self):
        self.ingest.ingest({'events': [event('a', sql_text='SELECT sample_shop'), event('b', sql_text='SELECT sampleXshop')]})
        found = self.store.query_tabularis_audit_log({'keyword': 'sample_shop'})
        self.assertEqual([row['event_id'] for row in found['rows']], ['a'])

    def test_server_redacts_untrusted_sender(self):
        self.ingest.ingest({'events': [event(sql_text="CREATE USER x IDENTIFIED BY 'secret\\\'tail'")]})
        text = self.store.local_execution_event_detail('a')['sql_text']
        self.assertNotIn('secret', text)
        self.assertNotIn('tail', text)
        self.assertIn('<redacted>', text)


class AccessBoundary(unittest.TestCase):
    def test_default_policy_rejects_public_peer_and_header_spoofing(self):
        with tempfile.TemporaryDirectory() as root:
            networks = load_networks(Path(root))
        self.assertTrue(peer_allowed('127.0.0.1', networks))
        self.assertFalse(peer_allowed('203.0.113.10', networks))

    def test_public_allow_all_configuration_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            Path(root, 'http-access.json').write_text('{"trustedNetworks":["0.0.0.0/0"]}')
            with self.assertRaises(ValueError):
                load_networks(Path(root))

    def test_public_access_requires_explicit_owner_boolean(self):
        with tempfile.TemporaryDirectory() as root:
            policy=Path(root,'http-access.json')
            for value in ['true','false',1,0,None]:
                policy.write_text(json.dumps({'allowPublic':value}))
                with self.subTest(value=value),self.assertRaises(ValueError):load_networks(Path(root))
            policy.write_text(json.dumps({'allowPublic':True,'trustedNetworks':['172.18.0.0/16']}))
            networks=load_networks(Path(root))
            for peer in ['203.0.113.10','2001:db8::1','::ffff:203.0.113.10']:
                self.assertTrue(peer_allowed(peer,networks),peer)
            self.assertFalse(peer_allowed('not-an-address',networks))
            policy.write_text(json.dumps({'allowPublic':False}))
            self.assertFalse(peer_allowed('203.0.113.10',load_networks(Path(root))))

    def test_public_mode_serves_api_but_keeps_ingest_token_and_origin_checks(self):
        with tempfile.TemporaryDirectory() as root:
            Path(root,'http-access.json').write_text('{"allowPublic":true}')
            ingest=Mock()
            app=SimpleNamespace(data_dir=Path(root),public_status=lambda:{'available':True},audit_ingest=ingest)
            server=AppHTTPServer(('127.0.0.1',0),app)
            thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
            try:
                self.assertTrue(peer_allowed('203.0.113.10',server.trusted_networks))
                base=f'http://127.0.0.1:{server.server_port}'
                with urllib.request.urlopen(base+'/api/status',timeout=3) as response:
                    self.assertTrue(json.load(response)['data']['available'])
                probes=[('/api/ingest/tabularis-audit',{},401,'AUDIT_AUTH_REQUIRED'),
                        ('/api/settings',{'Origin':'https://untrusted.invalid'},403,'LOCAL_ORIGIN_REQUIRED')]
                for route,headers,status,code in probes:
                    with self.assertRaises(urllib.error.HTTPError) as raised:
                        urllib.request.urlopen(urllib.request.Request(base+route,data=b'{}',headers=headers),timeout=3)
                    self.assertEqual(raised.exception.code,status)
                    value=json.load(raised.exception);raised.exception.close()
                    self.assertEqual(value['error']['code'],code)
                ingest.ingest.assert_not_called()
            finally:server.shutdown();server.server_close();thread.join(3)

    def test_get_query_validation_matches_post_bounds(self):
        for query in [{'limit': ['-1']}, {'limit': ['999999']}, {'offset': ['-1']}, {'keywordMode': ['oops']}, {'source': ['typo']}]:
            with self.subTest(query=query), self.assertRaises(ValueError):
                _event_query(query)

    def test_sensitive_routes_reject_before_touching_application(self):
        with tempfile.TemporaryDirectory() as root:
            app = SimpleNamespace(data_dir=Path(root))
            server = AppHTTPServer(('127.0.0.1', 0), app)
            server.trusted_networks = ()  # simulate every TCP peer being outside the boundary
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            base = f'http://127.0.0.1:{server.server_port}'
            try:
                for route in ['/api/settings','/api/query-task?id=a','/api/ip-names','/api/redis/investigate']:
                    with self.subTest(route=route), self.assertRaises(urllib.error.HTTPError) as error:
                        urllib.request.urlopen(urllib.request.Request(base+route, headers={'X-Forwarded-For':'127.0.0.1','Origin':base}), timeout=3)
                    self.assertEqual(error.exception.code, 403)
                for route in ['/api/system/stop','/api/settings','/api/storage/cleanup','/api/sync/pause','/api/ingest/tabularis-audit']:
                    with self.subTest(route=route), self.assertRaises(urllib.error.HTTPError) as error:
                        urllib.request.urlopen(urllib.request.Request(base+route, data=b'{}'), timeout=3)
                    self.assertEqual(error.exception.code, 403)
                self.assertTrue(json.load(urllib.request.urlopen(base+'/healthz', timeout=3))['ok'])
            finally:
                server.shutdown(); server.server_close(); thread.join(3)

    def test_json_compression_is_negotiated_and_roundtrips(self):
        with tempfile.TemporaryDirectory() as root:
            app = SimpleNamespace(data_dir=Path(root), public_status=lambda: {'value': 'a'*4000})
            server = AppHTTPServer(('127.0.0.1', 0), app)
            thread = threading.Thread(target=server.serve_forever, daemon=True); thread.start()
            try:
                request = urllib.request.Request(f'http://127.0.0.1:{server.server_port}/api/status', headers={'Accept-Encoding':'gzip'})
                with urllib.request.urlopen(request, timeout=3) as response:
                    self.assertEqual(response.headers['Content-Encoding'], 'gzip')
                    self.assertEqual(json.loads(gzip.decompress(response.read()))['data']['value'], 'a'*4000)
            finally:
                server.shutdown(); server.server_close(); thread.join(3)


class StatusTruth(unittest.TestCase):
    def test_inconsistent_catalog_counts_are_unknown_not_full(self):
        from app.server import Application
        app = object.__new__(Application)
        app.metadata = SimpleNamespace(load_settings=lambda: Settings(db_instance_id='rm-fixture'),
            storage_metadata_stats=lambda: {'part_count': 1}, known_instances=lambda: [])
        app.sync = SimpleNamespace(status=lambda: {'index': {'catalog': {'catalogedParts': 2}}})
        with patch('app.server.credential_status', return_value={}):
            result = app.public_status()['summary']
        self.assertIsNone(result['catalogCoverage'])
        self.assertIn('catalog_count_exceeds_parts', result['coverageIssues'])


class CollectorSafety(unittest.TestCase):
    def test_full_page_without_total_does_not_end_tail_pagination(self):
        client = Mock()
        client.call.side_effect = [{'Data': {'Logs': [{}]*100}}, {'Data': {'Logs': [{}]}}]
        tail = NodeTail(SlowLogConfig({'instanceId': 'rm-fixture', 'enabled': True}), lambda: client)
        rows, pages, capped = tail._read(0, 100000)
        self.assertEqual((len(rows), pages, capped), (101, 2, False))

    def test_das_error_without_success_does_not_look_like_empty_data(self):
        client = DasRpcClient(Settings(db_instance_id='rm-fixture'), CloudCredential('fixture','fixture'))
        with patch.object(client, '_signed_params', return_value={}), patch('app.slow_log_collector.urllib.request.urlopen', return_value=io.BytesIO(b'{"Code":"Throttling","Message":"quota"}')):
            with self.assertRaisesRegex(RuntimeError, 'Throttling'):
                client.call('DescribeSlowLogRecords', {})

    def test_redis_rpc_success_has_request_id_but_no_das_success_flag(self):
        from app.redis_service import KvClient
        client = KvClient(Settings(db_instance_id='rm-fixture'), CloudCredential('fixture', 'fixture'))
        body = b'{"RequestId":"fixture","Accounts":{"Account":[]},"PageNumber":1,"PageSize":30,"TotalCount":0}'
        with patch.object(client, '_signed_params', return_value={}), patch('app.slow_log_collector.urllib.request.urlopen', return_value=io.BytesIO(body)):
            self.assertEqual(client.call('DescribeAccounts', {})['RequestId'], 'fixture')

    def test_explicit_failure_overrides_success_code(self):
        client = DasRpcClient(Settings(db_instance_id='rm-fixture'), CloudCredential('fixture', 'fixture'))
        with patch.object(client, '_signed_params', return_value={}), patch('app.slow_log_collector.urllib.request.urlopen', return_value=io.BytesIO(b'{"Success":false,"Code":"200"}')):
            with self.assertRaises(RuntimeError):
                client.call('DescribeSlowLogRecords', {})

    def test_column_suffixes_never_merge_as_shard_tables(self):
        from app.rise_evidence import sql_template
        self.assertNotEqual(sql_template('SELECT col_1 FROM t'), sql_template('SELECT col_2 FROM t'))
        self.assertEqual(sql_template('SELECT col_1 FROM t_1'), sql_template('SELECT col_1 FROM t_2'))

    def test_absent_baseline_family_is_unknown_not_growth(self):
        from app.slowlog_impact import rank_performance_growth
        result = rank_performance_growth([dict(node_id='n',fingerprint='f',sql_id='s',event_id='e',start_us=180000000,duration_ms=60000)], [],
            [dict(nodeId='n',timestamp=i*60000,Average=i*10) for i in range(1,7)], start_us=0,end_us=359999999,index_complete=True,baseline_complete=True)
        row = result['nodes'][0]['statements'][0]
        self.assertIsNone(row['runtime_delta_us'])
        self.assertIsNone(row['attribution']['baseline_count'])
        self.assertIn('missing_slow_baseline_not_zero',row['attribution']['warnings'])


class Redaction(unittest.TestCase):
    def test_escaped_literals_and_uri_and_jwt_are_removed(self):
        for value, secret in [("ALTER USER x IDENTIFIED WITH p AS 'hashvalue'", 'hashvalue'),
                              ('mysql://user:secret@host/db', 'secret'),
                              ('Authorization: Bearer eyJabc.def.ghi', 'eyJabc'),
                              ("INSERT INTO auth (name,password) VALUES ('user','secret')", 'secret')]:
            with self.subTest(value=value):
                text, changed = redact_secrets(value)
                self.assertTrue(changed)
                self.assertNotIn(secret, text)

if __name__ == '__main__':
    unittest.main()
