"""Fixed, synthetic SQLite fixtures; no network, production data or schema changes.

Run: python -m unittest tests.test_sqlite_serving_performance -v
Hard gates: identical ordered rows/schema; index seeks; >=40% fewer VM steps
for the reported recent-window and instance-scoped lookup workloads. Wall times
are interleaved warm-cache medians, not production latency promises.
"""
from __future__ import annotations

import json
import statistics
import tempfile
import time
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from app.metadata import MetadataStore, TABULARIS_AUDIT_FILE_PREFIX
from app.slowlog_index import SlowLogIndex

DAY = 86_400_000_000
BASE = 1_780_000_000_000_000


def insert_fixture(conn, table, values):
    """Populate required columns of the real schema, without altering it."""
    columns = conn.execute(f"PRAGMA table_info({table})").fetchall()
    defaults = {
        row[1]: (0 if 'INT' in row[2] else '')
        for row in columns if row[3] and row[4] is None
    }
    rows = [{**defaults, **value} for value in values]
    names = list(rows[0])
    conn.executemany(
        f"INSERT INTO {table} ({','.join(names)}) VALUES ({','.join('?' for _ in names)})",
        [tuple(row[name] for name in names) for row in rows],
    )


def measure(conn, sql):
    steps = 0

    def progress():
        nonlocal steps
        steps += 100
        return 0

    conn.set_progress_handler(progress, 100)
    try:
        rows = [tuple(row) for row in conn.execute(sql)]
    finally:
        conn.set_progress_handler(None, 0)
    return rows, steps


def compare(test, conn, label, before, after, *, improvement=False):
    old_rows, old_steps = measure(conn, before)
    new_rows, new_steps = measure(conn, after)
    test.assertEqual(old_rows, new_rows, label)
    timings = {"before": [], "after": []}
    for cycle in range(6):
        pairs = [("before", before), ("after", after)]
        for name, sql in (pairs if cycle % 2 else pairs[::-1]):
            started = time.perf_counter()
            conn.execute(sql).fetchall()
            timings[name].append((time.perf_counter() - started) * 1000)
    result = {
        "case": label, "rows": len(new_rows),
        "before_ms": round(statistics.median(timings["before"]), 3),
        "after_ms": round(statistics.median(timings["after"]), 3),
        "before_steps": old_steps, "after_steps": new_steps,
        "plan": [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + after)],
    }
    print(json.dumps(result), flush=True)
    if improvement:
        test.assertLessEqual(new_steps, old_steps * 0.6, result)
    return result


class SQLiteServingPerformanceTests(unittest.TestCase):
    def test_existing_ids_fixed_fixture_is_exact_and_uses_key_seeks(self):
        with tempfile.TemporaryDirectory() as directory:
            index = SlowLogIndex(Path(directory) / "slowlog.sqlite3")
            self.addCleanup(index.close)
            with index.connection() as conn:
                schema = list(conn.execute("SELECT name, sql FROM sqlite_master ORDER BY name"))
                conn.execute("BEGIN IMMEDIATE")
                insert_fixture(conn, "slowlog_events", (
                    {"event_id": f"event-{i:06d}", "part_path": "fixture",
                     "instance_id": "rm-a", "is_canonical": 1,
                     "event_epoch_us": BASE + i * 1000}
                    for i in range(40_000)
                ))
                insert_fixture(conn, "slowlog_events", [
                    {"event_id": "other-only", "part_path": "other", "instance_id": "rm-b", "is_canonical": 1},
                    {"event_id": "not-canonical", "part_path": "duplicate", "instance_id": "rm-a", "is_canonical": 0},
                ])
                conn.commit()
                traced = []

                @contextmanager
                def connection():
                    yield conn

                with patch.object(index, "connection", connection):
                    conn.set_trace_callback(traced.append)
                    ids = [f"event-{i:06d}" for i in range(39_600, 40_000)]
                    self.assertEqual(index.existing_event_ids(ids, " rm-a "), set(ids))
                    conn.set_trace_callback(None)
                    sql = next(sql for sql in traced if sql.startswith("SELECT event_id FROM"))
                    baseline = sql.replace(" INDEXED BY idx_slowlog_event_canonical", "")
                    result = compare(self, conn, "existing-400-of-40000", baseline, sql, improvement=True)
                    self.assertTrue(any("idx_slowlog_event_canonical" in p and "event_id=?" in p for p in result["plan"]))
                    # Multiple chunks, duplicates, missing ids and instance isolation.
                    more = [f"event-{i:06d}" for i in range(1100)]
                    self.assertEqual(index.existing_event_ids(more * 2 + ["other-only", "not-canonical", "missing", ""], "rm-a"), set(more))
                    self.assertEqual(index.existing_event_ids(["other-only"], "rm-b"), {"other-only"})
                    self.assertEqual(index.existing_event_ids(["other-only", "not-canonical"]), {"other-only"})
                    self.assertEqual(index.existing_event_ids([], "rm-a"), set())
                self.assertEqual(schema, list(conn.execute("SELECT name, sql FROM sqlite_master ORDER BY name")))

    def test_parts_fixed_fixture_preserves_all_overlap_and_filter_semantics(self):
        with tempfile.TemporaryDirectory() as directory:
            store = MetadataStore(Path(directory) / "metadata.sqlite3")
            with store.connection() as conn:
                schema = list(conn.execute("SELECT name, sql FROM sqlite_master ORDER BY name"))
                for source in ("", "slowlog", "binlog", "database", "audit"):
                    self.assertEqual(store.parts_in_range(start_epoch_us=BASE, end_epoch_us=BASE + DAY,
                                                         source=source), [])
                oldest_plan = [row[3] for row in conn.execute(
                    "EXPLAIN QUERY PLAN SELECT MIN(min_event_epoch_us) FROM parquet_parts")]
                self.assertTrue(any("idx_part_time" in row for row in oldest_plan), oldest_plan)
                files, parts = [], []
                for i in range(12_000):
                    kind = ("slowlog", "binlog", "audit")[i % 3]
                    prefix = {"slowlog": "slow-log/", "binlog": "mysql-bin.", "audit": TABULARIS_AUDIT_FILE_PREFIX}[kind]
                    files.append({"id": f"file-{i}", "instance_id": "rm-a" if i % 7 else "rm-b",
                                  "log_file_name": prefix + str(i), "host_instance_id": "slow-log" if kind == "slowlog" else "db",
                                  "query_visible": 0 if i % 101 == 0 else 1})
                    start = BASE + i * DAY // 200
                    # Long/imported intervals, exact boundaries, identical timestamps.
                    end = start + DAY // 1000
                    if i % 997 == 0:
                        end += 400 * DAY
                    parts.append({"path": f"part-{i:06d}", "logical_part_id": f"logical-{i}",
                                  "binlog_id": f"file-{i}", "min_event_epoch_us": start,
                                  "max_event_epoch_us": end, "row_count": 1,
                                  "compression_level": (-7, 1, 9, 99)[i % 4]})
                # Several parts per file and identical sort keys exercise LIMIT ties.
                for i in range(3):
                    parts.append({**parts[-1], "path": f"tie-{i}", "logical_part_id": f"tie-id-{i}"})
                conn.execute("BEGIN IMMEDIATE")
                insert_fixture(conn, "binlog_files", files)
                insert_fixture(conn, "parquet_parts", parts)
                conn.commit()

                @contextmanager
                def connection(*, control=None):
                    # This fixture owns the connection/progress instrumentation;
                    # cancellation behavior is exercised with real connections.
                    self.assertIsNone(control)
                    yield conn

                windows = [(BASE + 59 * DAY, BASE + 60 * DAY),
                           (BASE, BASE + 500 * DAY),
                           (BASE - DAY, BASE),
                           (BASE + 450 * DAY, BASE + 451 * DAY),
                           (parts[997]["max_event_epoch_us"], parts[997]["max_event_epoch_us"])]
                with patch.object(store, "connection", connection):
                    for source in ("", "slowlog", "binlog", "database", "audit"):
                        for instance in ("", "rm-a", "rm-b", "missing"):
                            for start, end in windows:
                                for limit in (2, 100_000):
                                    traces = []
                                    conn.set_trace_callback(traces.append)
                                    actual = store.parts_in_range(start_epoch_us=start, end_epoch_us=end,
                                                                  source=source, instance=instance, limit=limit)
                                    conn.set_trace_callback(None)
                                    sql = next(sql for sql in traces if "SELECT p.*" in sql)
                                    baseline = "SELECT p.*" + sql.split("SELECT p.*", 1)[1]
                                    baseline = baseline.replace(
                                        "levels CROSS JOIN parquet_parts p INDEXED BY idx_part_cold_compression "
                                        "ON p.compression_level = levels.level CROSS JOIN binlog_files b",
                                        "parquet_parts p JOIN binlog_files b",
                                    )
                                    expected = [dict(row) for row in conn.execute(baseline)]
                                    self.assertEqual(actual, expected, (source, instance, start, end, limit))
                                    if source != "audit" and start - BASE > max(end - start, 0):
                                        plan = [row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + sql)]
                                        self.assertTrue(any("idx_part_cold_compression" in p
                                                            and "compression_level=? AND max_event_epoch_us>?" in p
                                                            for p in plan), plan)
                                    if limit == 100_000 and (not instance or (source == "slowlog" and instance == "rm-b")):
                                        if start == windows[0][0]:
                                            suffix = f"-{instance}" if instance else ""
                                            compare(self, conn, f"recent-parts-{source or 'all'}{suffix}", baseline, sql,
                                                    improvement=source != "audit")
                                        elif source == "slowlog" and not instance and start in (BASE, BASE - DAY):
                                            label = "wide-500-days" if start == BASE else "earliest-boundary"
                                            result = compare(self, conn, f"parts-{label}", baseline, sql)
                                            self.assertLessEqual(abs(result["before_steps"] - result["after_steps"]), 100)
                self.assertEqual(schema, list(conn.execute("SELECT name, sql FROM sqlite_master ORDER BY name")))


if __name__ == "__main__":
    unittest.main()
