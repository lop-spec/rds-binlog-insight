"""JOIN table membership must agree across SQLite and the isolated CI ClickHouse."""
from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from app.clickhouse_client import ClickHouseClient, ClickHouseConfig
from app.clickhouse_slowlog import ClickHouseSlowLogQueryBackend
from app.slowlog_index import SlowLogIndex
from tests.test_slowlog_index import _event, _part

T = 1_787_286_000_000_000
TABLES = {
    'single': 'orders',
    'first': 'orders,customers',
    'last': 'customers,orders',
    'upper': 'CUSTOMERS,ORDERS,items',
    'spaced': 'customers, orders ,items',
    'tabbed': 'customers,\torders\r\n,items',
    'prefix': 'orders_archive,customers',
    'suffix': 'preorders,customers',
    'wildcard': 'customers,orders%',
    'quoted': 'customers,o"rders',
    'backslash': 'customers,o\\rders',
    'unrelated': 'customers,items',
}
CASES = [
    ('orders', {'single', 'first', 'last', 'upper', 'spaced', 'tabbed'}),
    (' ORDERS ', {'single', 'first', 'last', 'upper', 'spaced', 'tabbed'}),
    ('orders,customers', {'first'}),  # Existing combination selections stay exact.
    ('customers,orders', {'last'}),
    ('orders%', {'wildcard'}),
    ('o"rders', {'quoted'}),
    ('o\\rders', {'backslash'}),
    ("orders' OR 1=1 --", set()),
    ('order', set()),
]

def records():
    return [_event(key, T + number, table=table, rows_examined=100 + number,
                   rows_sent=number, query_ms=200 + number)
            for number, (key, table) in enumerate(TABLES.items())]


class SlowLogTableFilters(unittest.TestCase):
    def test_sqlite_analytics_and_details_include_join_members_without_double_counting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            index = SlowLogIndex(root / 'slowlog.sqlite3')
            rows = records()
            for name, values in [('main', rows), ('duplicate', [rows[1]])]:
                source = root / f'{name}.parquet'
                index.build_part(_part(source, name, values), source)
            for table, expected in CASES:
                with self.subTest(table=table):
                    page = index.query_events({'instance': 'rm-prod', 'database': 'example_app',
                        'table': table, 'limit': 100}, start_epoch_us=T-1, end_epoch_us=T+100)
                    self.assertEqual({r['event_id'] for r in page['rows']}, expected)
                    self.assertEqual(len(page['rows']), len(expected))
                    summary = index.summarize(start_epoch_us=T-1, end_epoch_us=T+100,
                        instance='rm-prod', database='example_app', table=table, limit=100)
                    self.assertEqual(summary['sql']['totals']['executions'], len(expected))
                    self.assertEqual(summary['sql']['totals']['scan_rows'],
                        sum(100 + number for number, key in enumerate(TABLES) if key in expected))

    def test_clickhouse_scope_uses_parameterized_member_match_and_exact_combination(self):
        backend = object.__new__(ClickHouseSlowLogQueryBackend)
        backend.table = 'fixture.events'
        sql, parameters = backend._scope_sql({'table': "orders' OR 1=1 --"}, T-1, T+100)
        self.assertIn("splitByChar(',', table_name)", sql)
        self.assertIn('trimBoth(lowerUTF8(name))', sql)
        self.assertNotIn("orders' OR 1=1", sql)
        self.assertIn("orders' OR 1=1 --", parameters.values())
        sql, _ = backend._scope_sql({'table': 'orders,customers'}, T-1, T+100)
        self.assertNotIn('splitByChar', sql)
        self.assertIn('lowerUTF8(table_name) = lowerUTF8(', sql)


@unittest.skipUnless(os.environ.get('SLOWLOG_CI_FIXTURE') == '1', 'isolated CI ClickHouse fixture only')
class SlowLogTableFiltersClickHouse(unittest.TestCase):
    def test_real_scope_matches_sqlite_membership_and_deduplication(self):
        config = ClickHouseConfig.from_env()
        self.assertEqual(config.database, 'mongo_ci_fixture')
        self.assertEqual(config.host, '127.0.0.1')
        self.assertEqual(config.port, 18123)
        client = ClickHouseClient(config)
        table = 'mongo_ci_fixture.slowlog_table_filter_fixture'
        client.query(f'''CREATE TABLE {table} (
            instance_id String, event_epoch_us Int64, event_id String,
            node_id String, operation String, database_name String, table_name String,
            fingerprint String, sql_id String, action String, sql_bytes UInt64,
            query_time_ms UInt64, lock_time_ms UInt64, rows_examined UInt64, rows_sent UInt64,
            _source_part_path String, _source_part_id String
        ) ENGINE=Memory''')
        fixture = []
        for number, (event_id, table_name) in enumerate(TABLES.items()):
            fixture.append(dict(instance_id='rm-prod', event_epoch_us=T+number,
                event_id=event_id, node_id='fixture-node', operation='SELECT',
                database_name='example_app', table_name=table_name, fingerprint=event_id,
                sql_id=event_id, action='SELECT', sql_bytes=100, query_time_ms=200+number,
                lock_time_ms=7, rows_examined=100+number, rows_sent=number,
                _source_part_path='main', _source_part_id='main'))
        fixture.append({**fixture[1], '_source_part_path': 'duplicate', '_source_part_id': 'duplicate'})
        try:
            client.insert_json_rows(table, fixture)
            backend = object.__new__(ClickHouseSlowLogQueryBackend)
            backend.table = table
            for value, expected in CASES:
                with self.subTest(table=value):
                    sql, parameters = backend._scope_sql({'instance': 'rm-prod',
                        'database': 'example_app', 'table': value}, T-1, T+100)
                    rows = client.json_rows(sql, parameters=parameters)
                    self.assertEqual({row['scope_event_id'] for row in rows}, expected)
                    self.assertEqual(len(rows), len(expected))
                    self.assertEqual(sum(int(row['metric_rows_examined']) for row in rows),
                        sum(100 + number for number, key in enumerate(TABLES) if key in expected))
        finally:
            client.query(f'DROP TABLE {table}')

if __name__ == '__main__':
    unittest.main()
