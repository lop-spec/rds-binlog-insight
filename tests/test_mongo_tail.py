import itertools
import json
import random
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from app.mongo_collector import MongoCollector, SlowWindowOverCapacity, das_record
from app.mongo_insight import MINUTE, WINDOW, parse_counts
from app.mongo_service import MongoService
from app.mongo_store import MongoStore
from app.mongo_tail import MongoSlowTail, merge_rows
from tests.test_mongo_insight import T

SECOND = 1_000_000
SEQUENCE = itertools.count(1)


def dds(at_us, ns='demo.messages_1', ms=120, role='PRIMARY', client='10.0.0.1', shape=1):
    """A DDS slow record with every field DDS returns; ``lsid`` makes each execution distinct, as in production."""
    n = next(SEQUENCE)
    q = dict(op='command', ns=ns, command={'find': ns.split('.')[1], 'filter': {'x' * shape: 42}}, millis=ms,
             replRole={'stateStr': role}, cpuNanos=ms * 1000, lsid={'id': n})
    return {'AccountName': 'app@demo', 'DBName': 'demo', 'DocsExamined': 10,
            'ExecutionStartTime': datetime.fromtimestamp(at_us // SECOND, timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'HostAddress': client, 'KeysExamined': 11, 'QueryTimes': ms, 'ReturnRowCounts': 1,
            'SQLText': json.dumps(q), 'TableName': ns.split('.')[1]}


def das_log(record):
    """The DAS entry DAS returns for a DDS record (field names and string counts as seen on 2026-10-28)."""
    at = int(datetime.fromisoformat(record['ExecutionStartTime'].replace('Z', '+00:00')).timestamp() * 1000)
    return {'Timestamp': at, 'QueryStartTime': record['ExecutionStartTime'], 'QueryTime': record['QueryTimes'],
            # Microsecond durations: the fraction keeps executions with the same milliseconds apart, as they nearly always are.
            'QueryTimeSeconds': round(record['QueryTimes'] / 1000 + json.loads(record['SQLText'])['lsid']['id'] % 997 * 1e-6, 6),
            'Command': record['SQLText'], 'DBName': record['DBName'],
            'TableName': record['TableName'], 'Namespace': record['DBName'] + '.' + record['TableName'],
            'AccountName': record['AccountName'], 'HostAddress': record['HostAddress'],
            'DocsExamined': str(record['DocsExamined']), 'KeysExamined': str(record['KeysExamined']),
            'ReturnNum': str(record['ReturnRowCounts']), 'QueryId': 'q%d' % at, 'OpType': 'command'}


class FakeDas:
    """DescribeSlowLogRecords over a fixed set: [StartTime, EndTime) in ms, pages ordered by QueryTimeSeconds.

    Like DAS, a page above 100 records cuts the command text to 2,048 characters. ``unstable_above`` reorders every page of a
    slice holding more records than that, as an unstable sort would.
    """

    def __init__(self, records, unstable_above=None):
        self.logs = [das_log(r) for r in records]
        self.calls = []
        self.unstable_above = unstable_above

    def call(self, action, params):
        assert action == 'DescribeSlowLogRecords'
        self.calls.append(params)
        hits = sorted((x for x in self.logs if params['StartTime'] <= x['Timestamp'] < params['EndTime']),
                      key=lambda x: x['QueryTimeSeconds'])
        if self.unstable_above is not None and len(hits) > self.unstable_above:
            random.Random(len(self.calls)).shuffle(hits)
        size, page = params['PageSize'], params['PageNumber']
        logs = hits[(page - 1) * size:page * size]
        if size > 100:
            logs = [dict(x, Command=x['Command'][:2048]) for x in logs]
        return {'Success': 'true', 'Data': {'TotalRecords': len(hits), 'Logs': logs}}


def collector(store=None, records=(), source='das'):
    store = store or Mock()
    if isinstance(store, Mock):
        store.publish.side_effect = lambda instance, start, end, records, prefixes, archive=None: {'records': len(records), 'end': end}
    c = MongoCollector(store, {'instanceId': 'dds-example', 'region': 'cn-example-1', 'families': [], 'slowlogSource': source},
                       lambda: None)
    c.DAS_REQUEST_INTERVAL = 0
    fake = FakeDas(records)
    c.das_rpc = lambda: fake
    return c, fake


class DasSource(unittest.TestCase):
    def test_das_entry_maps_to_the_dds_record_field_for_field(self):
        record = dds(T + 7 * SECOND)
        self.assertEqual(das_record(das_log(record)), record)

    def test_dense_window_is_read_in_whole_second_slices_of_at_most_twenty_pages(self):
        records = [dds(T + (i * 300 // 5000) * SECOND) for i in range(5000)]
        c, fake = collector(records=records)
        c.slow_window(T, T + WINDOW)
        published = c.store.publish.call_args.args[3]
        self.assertEqual(sorted(json.dumps(r) for r in published), sorted(json.dumps(r) for r in records))
        self.assertTrue(all(p['PageSize'] == 100 and p['OrderBy'] == 'QueryTimeSeconds' for p in fake.calls))
        self.assertTrue(all(p['StartTime'] % 1000 == 0 and p['EndTime'] % 1000 == 0 for p in fake.calls))
        self.assertLessEqual(max(p['PageNumber'] for p in fake.calls), 20)
        # 50 pages of records: one look at the window, then its slices; no slice is read twice
        self.assertLess(len(fake.calls), 70)
        self.assertEqual(c.state['slowlog_source'], 'das')

    def test_pages_never_ask_for_more_than_one_hundred_records(self):
        # Above 100 DAS cuts the command text to 2,048 characters, which does not parse (the first 1.34.8 build).
        long = dds(T, shape=3000)
        c, fake = collector(records=[long])
        c.slow_window(T, T + WINDOW)
        self.assertEqual(c.store.publish.call_args.args[3], [long])
        self.assertGreater(len(long['SQLText']), 2048)

    def test_das_records_that_do_not_parse_are_read_again_from_dds(self):
        long = dds(T, shape=3000)
        with tempfile.TemporaryDirectory() as td:
            c, _ = collector(store=MongoStore(Path(td), backend='parquet'), records=[long])
            c.DAS_PAGE = 200  # what the first 1.34.8 build asked for: DAS cuts the text
            client = Mock()
            client.call.return_value = {'TotalRecordCount': 1, 'Items': {'LogRecords': [long]}}
            c.rpc = lambda cms=False: client
            with self.assertLogs('app.mongo_collector', 'WARNING') as logs:
                manifest = c.slow_window(T, T + WINDOW)
        self.assertEqual(manifest['records'], 1)
        self.assertIn('JSONDecodeError', c.state['slowlog_source'])
        self.assertTrue(any('reading DDS instead' in line for line in logs.output))

    def test_a_busy_second_is_paged_and_checked(self):
        records = [dds(T + 5 * SECOND) for _ in range(250)]
        c, fake = collector(records=records)
        c.slow_window(T, T + WINDOW)
        self.assertEqual(sorted(json.dumps(r) for r in c.store.publish.call_args.args[3]), sorted(json.dumps(r) for r in records))

    def test_unstable_pages_split_the_slice_and_a_second_that_stays_unstable_reads_dds(self):
        records = [dds(T + (i % 4) * SECOND) for i in range(400)]
        c, fake = collector(records=records)
        c.das_rpc = lambda: fake
        fake.unstable_above = 150
        with self.assertLogs('app.mongo_collector', 'WARNING') as logs:
            c.slow_window(T, T + WINDOW)
        self.assertTrue(any('das_pages_unstable' in line and 'splitting' in line for line in logs.output))
        # every second holds 100 records, so after splitting every slice is a single page and stable
        self.assertEqual(len(c.store.publish.call_args.args[3]), 400)
        self.assertEqual(c.state['slowlog_source'], 'das')
        busy, _ = collector(records=[dds(T + 5 * SECOND) for _ in range(250)])
        busy.das_rpc().unstable_above = 150
        client = Mock()
        client.call.return_value = {'TotalRecordCount': 1, 'Items': {'LogRecords': [dds(T)]}}
        busy.rpc = lambda cms=False: client
        with self.assertLogs('app.mongo_collector', 'WARNING'):
            busy.slow_window(T, T + WINDOW)
        self.assertIn('das_pages_unstable', busy.state['slowlog_source'])

    def test_window_over_the_record_cap_is_over_capacity_and_never_published(self):
        c, fake = collector(records=[dds(T + i * SECOND) for i in range(30)])
        c.SLOWLOG_MAX_RECORDS = 20
        with self.assertRaises(SlowWindowOverCapacity):
            c.slow_window(T, T + WINDOW)
        self.assertEqual(len(fake.calls), 1)
        c.store.publish.assert_not_called()

    def test_das_failure_reads_the_same_window_from_dds(self):
        c, _ = collector()
        c.das_rpc = Mock(side_effect=RuntimeError('DAS 调用失败：denied'))
        client = Mock()
        record = dds(T)
        client.call.return_value = {'TotalRecordCount': 1, 'Items': {'LogRecords': [record]}}
        c.rpc = lambda cms=False: client
        with self.assertLogs('app.mongo_collector', 'WARNING') as logs:
            c.slow_window(T, T + WINDOW)
        self.assertIn('reading DDS instead', logs.output[0])
        self.assertTrue(c.state['slowlog_source'].startswith('dds_fallback'))
        self.assertEqual(c.store.publish.call_args.args[3], [record])


class LiveTail(unittest.TestCase):
    def tail(self, records, published=T):
        c, fake = collector(records=records)
        tail = MongoSlowTail(c, lambda: published)
        return tail, fake

    def test_scan_reads_unpublished_minutes_and_vouches_up_to_a_minute_ago(self):
        records = [dds(T + i * 10 * SECOND) for i in range(45)]  # T .. T+7:20
        tail, fake = self.tail(records)
        now = T / SECOND + 7 * 60 + 30
        self.assertEqual(tail.scan(now), 45)
        self.assertEqual(fake.calls[0]['StartTime'], T // 1000)
        self.assertEqual(tail.covered_from, T)
        self.assertEqual(tail.complete_through, T + 6 * MINUTE)
        through, rows = tail.coverage([T, T + WINDOW], T, now)
        self.assertEqual(through, T + 6 * MINUTE)
        self.assertEqual(sum(r['count'] for r in rows[T]), 30)
        self.assertEqual(sum(r['count'] for r in rows[T + WINDOW]), 6)  # T+5:00 .. T+5:50, before T+6:00

    def test_next_scan_rereads_only_what_it_did_not_vouch_for(self):
        tail, fake = self.tail([dds(T + i * 10 * SECOND) for i in range(45)])
        now = T / SECOND + 7 * 60 + 30
        tail.scan(now)
        tail.scan(now + 20)
        self.assertEqual(fake.calls[-1]['StartTime'], (T + 6 * MINUTE) // 1000)
        self.assertEqual(tail.covered_from, T)
        self.assertEqual(sum(n for _, n in tail.minutes.values()), 45)

    def test_stale_tail_or_one_that_starts_too_late_vouches_for_nothing(self):
        tail, _ = self.tail([dds(T)])
        now = T / SECOND + 7 * 60
        tail.scan(now)
        self.assertIsNone(tail.coverage([T], T, now + 91))
        self.assertIsNone(tail.coverage([T - WINDOW, T], T - WINDOW, now))

    def test_published_minutes_leave_the_buffer_and_its_coverage(self):
        published = [T]
        tail, _ = self.tail([dds(T + i * 10 * SECOND) for i in range(45)], published=None)
        tail.published_through = lambda: published[0]
        now = T / SECOND + 7 * 60 + 30
        tail.scan(now)
        published[0] = T + WINDOW
        tail.scan(now + 20)
        self.assertEqual(tail.covered_from, T + WINDOW)
        self.assertTrue(all(m >= T + WINDOW for m in tail.minutes))
        # A window before the run (published late or skipped) cannot be stood in for.
        self.assertIsNone(tail.coverage([T, T + WINDOW], T, now + 20))


class TailMatchesPublishedWindows(unittest.TestCase):
    RECORDS = [dds(T + i * 7 * SECOND, ns='demo.messages_%d' % (i % 3), ms=(i % 5) * 400 + 100,
                   role='SECONDARY' if i % 4 == 0 else 'PRIMARY', client='10.0.0.%d' % (i % 2), shape=1 + i % 2)
               for i in range(85)]  # T .. T+9:48, some statements run into the next minute

    @staticmethod
    def comparable(rows):
        out = []
        for row in rows:
            value = {k: (int(v) if isinstance(v, (int, float)) else v) for k, v in row.items() if k not in ('clients', 'accounts')}
            value.update(clients=dict(parse_counts(row.get('clients'))), accounts=dict(parse_counts(row.get('accounts'))))
            out.append(json.dumps(value, sort_keys=True))
        return sorted(out)

    def test_tail_rows_equal_the_rows_of_the_published_windows(self):
        with tempfile.TemporaryDirectory() as a, tempfile.TemporaryDirectory() as b:
            published = MongoStore(Path(a), backend='parquet')
            for start in (T, T + WINDOW):
                published.publish('dds-example', start, start + WINDOW,
                                  [r for r in self.RECORDS if start <= datetime.fromisoformat(r['ExecutionStartTime'].replace('Z', '+00:00')).timestamp() * SECOND < start + WINDOW], [])
            live = MongoStore(Path(b), backend='parquet')
            c, _ = collector(store=live, records=self.RECORDS)
            tail = MongoSlowTail(c, lambda: T)
            now = T / SECOND + 12 * 60
            tail.scan(now)
            for lo, hi, width in ((T + MINUTE, T + 10 * MINUTE, MINUTE), (T, T + 10 * MINUTE, 3 * MINUTE), (T + 2 * MINUTE + 5 * SECOND, T + 8 * MINUTE, MINUTE)):
                for role in ('', 'Primary'):
                    expected, full = published.read('dds-example', lo, hi, width=width, role=role)
                    _, rows = tail.coverage([T, T + WINDOW], lo, now)
                    actual, coverage = live.read('dds-example', lo, hi, width=width, role=role, tail=rows)
                    self.assertTrue(full['complete'] and coverage['complete'] and len(expected) > 10)
                    self.assertEqual(coverage['tail_windows'], len(range(lo // WINDOW * WINDOW, hi, WINDOW)))
                    self.assertEqual(self.comparable(actual), self.comparable(expected))
            # One window published, the next one from the tail: still the same rows, nothing counted twice.
            first = [r for r in self.RECORDS if r['ExecutionStartTime'] < '2027-01-15T08:05:00Z']
            live.publish('dds-example', T, T + WINDOW, first, [])
            _, rows = tail.coverage([T, T + WINDOW], T, now)
            actual, coverage = live.read('dds-example', T, T + 10 * MINUTE, width=MINUTE, tail=rows)
            expected, _ = published.read('dds-example', T, T + 10 * MINUTE, width=MINUTE)
            self.assertEqual(coverage['tail_windows'], 1)
            self.assertEqual(self.comparable(actual), self.comparable(expected))

    def test_merge_keeps_the_longest_sample_and_adds_counts(self):
        base = dict(bucket=T, role='Primary', group_id='g', profile='{}', sample='short', max_us=10, first_us=T + 5, last_us=T + 5,
                    clients='{"a":1}', accounts='', **{k: 1 for k in ('count', 'failed', 'collscan', 'spill', 'overlap_us')})
        from app.mongo_tail import SUMS
        for k in SUMS:
            base.setdefault(k, 1)
        other = dict(base, bucket=T + MINUTE, sample='long', max_us=20, first_us=T + MINUTE, last_us=T + MINUTE, clients='{"b":2}')
        merged = merge_rows([dict(base)], [other], width=2 * MINUTE, lo=T, hi=T + 2 * MINUTE)
        self.assertEqual(len(merged), 1)
        row = merged[0]
        self.assertEqual((row['count'], row['sample'], row['max_us'], row['first_us'], row['last_us']), (2, 'long', 20, T + 5, T + MINUTE))
        self.assertEqual(dict(parse_counts(row['clients'])), {'a': 1, 'b': 2})


class ServiceUsesTheTail(unittest.TestCase):
    def service(self, td, records):
        Path(td, 'mongo-instances.json').write_text(json.dumps([{'instanceId': 'dds-example', 'region': 'cn-chengdu', 'backend': 'parquet',
                                                                  'families': [], 'enabled': True}]), encoding='utf-8')
        service = MongoService(Path(td), lambda: None, start=False)
        self.assertEqual(service.error, '')
        c = service.collectors[0]
        c.DAS_REQUEST_INTERVAL = 0
        fake = FakeDas(records)
        c.das_rpc = lambda: fake
        return service, service.tails['dds-example']

    def test_recent_window_ends_a_minute_ago_instead_of_moving_back(self):
        records = [dds(T + i * 10 * SECOND) for i in range(60)]
        with tempfile.TemporaryDirectory() as td:
            service, tail = self.service(td, records)
            store = service.stores['dds-example']
            store.publish('dds-example', T - WINDOW, T, [], [])
            now = T / SECOND + 9 * 60 + 30
            tail.scan(now)  # vouches through T+8:00
            with patch('app.mongo_tail.time.time', return_value=now):
                result = service.query(dict(instance='dds-example', startEpochUs=T, endEpochUs=T + 9 * MINUTE,
                                            baselineStart=T - 9 * MINUTE, metric='CPUUtilization', order='correlation'))
                status = service.status()['collectors'][0]
        self.assertEqual(result['adjusted']['reason'], 'source_delay')
        self.assertEqual(result['adjusted']['truncated_us'], MINUTE)
        self.assertEqual((result['start_us'], result['end_us']), (T, T + 8 * MINUTE))
        # The previous equal-length baseline stays adjacent to the shortened window.
        self.assertEqual((result['baseline_start'], result['baseline_end']), (T - 8 * MINUTE, T))
        self.assertTrue(result['coverage']['complete'])
        self.assertEqual(result['coverage']['tail_windows'], 2)
        self.assertEqual(status['slowlog_data_through'], T + 8 * MINUTE)
        self.assertTrue(status['slowlog_tail']['fresh'])

    def test_without_a_fresh_tail_the_old_nearest_window_search_runs(self):
        with tempfile.TemporaryDirectory() as td:
            service, tail = self.service(td, [])
            store = service.stores['dds-example']
            store.nearest_complete = Mock(return_value=None)
            with self.assertLogs('app.mongo_service', 'WARNING'):
                result = service.query(dict(instance='dds-example', startEpochUs=T, endEpochUs=T + 5 * MINUTE,
                                            baselineStart=T - 5 * MINUTE))
        store.nearest_complete.assert_called_once()
        self.assertIsNone(result['adjusted'])


if __name__ == '__main__':
    unittest.main()
