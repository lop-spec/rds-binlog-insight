from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path

from app.clickhouse_slowlog import ClickHouseSlowLogQueryBackend
from app.slowlog_index import BUCKET_US, SlowLogIndex
from tests.test_slowlog_index import _event, _part
from tests.test_clickhouse_slowlog import _Client, _Metadata, _Manifest, _StatementIndex


class EngineCorrelationTests(unittest.TestCase):
    def test_sqlite_deduplicates_aligns_and_applies_node_scope(self):
        start = int(datetime.now(UTC).timestamp() * 1_000_000) // BUCKET_US * BUCKET_US - 12 * BUCKET_US
        a = [1, 4, 0, 3, 0, 2, 1, 5]
        b = [3, 1, 0, 4, 0, 1, 6, 2]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            index = SlowLogIndex(root / 'slowlog.sqlite3')
            events = []
            for table, values in [('orders', a), ('other', b)]:
                for bucket, n in enumerate(values):
                    for i in range(n):
                        events.append(_event(f'{table}-{bucket}-{i}', start + bucket * BUCKET_US + i,
                                             rows_examined=7, rows_sent=1, query_ms=4, table=table))
            events.append(_event('other-node', start + 1, rows_examined=999, rows_sent=1, query_ms=5, node_id='node-b'))
            index.build_part(_part(root / 'p1.parquet', 'one', events), root / 'p1.parquet')
            index.build_part(_part(root / 'p2.parquet', 'two', events), root / 'p2.parquet')
            result = index.summarize(start_epoch_us=start, end_epoch_us=start + len(a) * BUCKET_US - 1,
                                     instance='rm-prod', node_id='pi-node-a', limit=1)
            sql = result['sql']
            self.assertEqual(sql['totals']['executions'], sum(a) + sum(b))
            self.assertEqual([p['events'] for p in sql['trend']], [x + y for x, y in zip(a, b)])
            self.assertEqual(sql['correlation']['complete_buckets'], 8)
            row = sql['statements'][0]
            self.assertEqual(row['correlation']['counts'], b)
            self.assertAlmostEqual(row['correlation']['event_share'], sum(b) / (sum(a) + sum(b)))
            self.assertEqual(row['correlation']['status'], 'ok')
            self.assertEqual(len(sql['statements']), 1)  # denominator still includes BOTH SQLs
            filtered = index.summarize(start_epoch_us=start, end_epoch_us=start + len(a) * BUCKET_US - 1,
                                       instance='rm-prod', node_id='pi-node-a', table='orders')
            c = filtered['sql']['statements'][0]['correlation']
            self.assertEqual(c['counts'], a)
            self.assertEqual(c['value'], 1.0)
            self.assertIsNone(c['without_self_value'])

    def test_clickhouse_sum_map_is_parsed_in_single_canonical_snapshot(self):
        start = int(datetime.now(UTC).timestamp() * 1_000_000) // BUCKET_US * BUCKET_US - 12 * BUCKET_US
        values = [1, 4, 0, 3, 0, 2, 1, 5]
        class MapClient(_Client):
            def json_rows(self, sql, **kwargs):
                rows = super().json_rows(sql, **kwargs)
                statement = rows[0]
                statement.update(executions=sum(values), bucket_counts=[
                    [str(start + i * BUCKET_US) for i, v in enumerate(values) if v],
                    [str(v) for v in values if v]])
                return rows[:3] + [dict(rows[3], group_ts=start + i * BUCKET_US, executions=v)
                                   for i, v in enumerate(values) if v]
        with tempfile.TemporaryDirectory() as tmp:
            client = MapClient()
            part = {'path': 'p', 'logical_part_id': 'p1'}
            backend = ClickHouseSlowLogQueryBackend(_Metadata([part]), Path(tmp), client=client,
                manifest=_Manifest(True), statement_index=_StatementIndex(),
                table='insight.slowlog_events', serving_enabled=True)
            sql = backend.summarize({'source': 'slowlog', 'start_epoch_us': start,
                                     'end_epoch_us': start + len(values) * BUCKET_US - 1}, retention_days=60)['sql']
            self.assertEqual(len(client.queries), 1)
            self.assertIn('sumMap([metric_bucket], [toUInt64(1)])', client.queries[0])
            self.assertEqual([p['events'] for p in sql['trend']], values)
            for rows in sql['orders'].values():
                self.assertEqual(rows[0]['correlation']['counts'], values)
                self.assertEqual(rows[0]['correlation']['value'], 1.0)
                self.assertIsNone(rows[0]['correlation']['without_self_value'])


if __name__ == '__main__':
    unittest.main()
