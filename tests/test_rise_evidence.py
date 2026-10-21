import unittest

from app.rise_evidence import (
    COUNTER_METRICS, DAY_US, assign_template_families, load_counters, node_evidence, sql_template,
)
from app.slowlog_impact import family_key

MIN = 60_000_000
UPSERT = "INSERT INTO `order_record_{n}` (`id`,`tenant_id`) VALUES (?..) ON DUPLICATE KEY UPDATE `updated_at`=VALUES(updated_at)"


class TemplateTests(unittest.TestCase):
    def test_shard_numbers_in_names_go_and_literals_stay(self):
        self.assertEqual(sql_template(UPSERT.format(n=38)), sql_template(UPSERT.format(n=563)))
        self.assertIn("`order_record_#`", sql_template(UPSERT.format(n=1)))
        self.assertEqual(sql_template("SELECT * FROM t WHERE a = ? LIMIT 10"), "SELECT * FROM t WHERE a = ? LIMIT 10")
        self.assertEqual(sql_template("select  *\n from task_queue_9"), "select * from task_queue_#")
        self.assertEqual(sql_template(""), "")

    def test_a_truncated_multi_row_batch_has_the_template_of_its_single_row_insert(self):
        single = "INSERT INTO `orders` (`a`,`b`) VALUES (?) ON DUPLICATE KEY UPDATE `a`=IF(a IN (?), VALUES(a), a)"
        batch = "INSERT INTO `orders` (`a`,`b`) VALUES (?),(?,'[{\\\"payment\\\":?,\\\"num\\\":?,"
        self.assertEqual(sql_template(single), sql_template(batch))
        self.assertNotEqual(sql_template(single), sql_template("INSERT INTO `orders` (`a`) VALUES (?)"))
        self.assertNotEqual(sql_template(single), sql_template("INSERT INTO `other` (`a`,`b`) VALUES (?)"))

    def test_sql_ids_of_one_statement_on_shard_tables_become_one_family(self):
        events = [
            dict(fingerprint="f38", sql_id="A", database_name="order_record", node_id="n"),
            dict(fingerprint="f563", sql_id="B", database_name="order_record", node_id="n"),
            dict(fingerprint="f1", sql_id="A", database_name="order_record", node_id="n"),
            # the same text in another database stays apart
            dict(fingerprint="g", sql_id="C", database_name="main_db", node_id="n"),
            # a different statement on the same tables stays apart
            dict(fingerprint="h", sql_id="D", database_name="order_record", node_id="n"),
        ]
        texts = {"f38": UPSERT.format(n=38), "f563": UPSERT.format(n=563), "f1": UPSERT.format(n=1),
                 "g": UPSERT.format(n=7), "h": "DELETE FROM `order_record_5` WHERE id = ?"}
        asked = []

        def profiles(fps):
            asked.append(set(fps))
            return {fp: {"normalized_sql": texts[fp]} for fp in fps}

        merged = assign_template_families(events, profiles, family_key)
        self.assertEqual(len(asked), 1)
        self.assertEqual(asked[0], {"f38", "f563", "g", "h"})  # one representative per SQL-ID family
        self.assertEqual(list(merged.values()), [2])
        keys = [family_key(event) for event in events]
        self.assertEqual(keys[0], keys[1])
        self.assertEqual(keys[0], keys[2])
        self.assertNotEqual(keys[3], keys[0])
        self.assertNotEqual(keys[4], keys[0])
        self.assertNotIn("family", events[3])

    def test_without_text_nothing_merges(self):
        events = [dict(fingerprint="a", sql_id="A", database_name="d"), dict(fingerprint="b", sql_id="B", database_name="d")]
        self.assertEqual(assign_template_families(events, lambda fps: {}, family_key), {})
        self.assertTrue(all("family" not in event for event in events))

    def test_a_lag_copy_keeps_its_origin_apart(self):
        base = dict(fingerprint="a", sql_id="A", database_name="d", family="x")
        self.assertNotEqual(family_key(base), family_key({**base, "origin": "primary"}))


def points(start, value, node="n1", minutes=10):
    return [dict(timestamp=(start + (i + 1) * MIN) // 1000, nodeId=node, Average=value) for i in range(minutes)]


class CounterTests(unittest.TestCase):
    START = 1_800_000_000_000_000 // MIN * MIN
    END = START + 10 * MIN

    def loader(self, now, before, fail=()):
        def load(settings, start, end, instance, node, metric_name):
            if metric_name in fail:
                raise RuntimeError("boom")
            value = (now if start == self.START else before).get(metric_name, 0)
            return points(start, value)
        return load

    def test_counters_compare_the_window_with_the_same_window_yesterday(self):
        load = self.loader({"Cluster_ComInsert": 13000, "Cluster_ThreadsRunning": 20, "Cluster_ComSelect": 500},
                           {"Cluster_ComInsert": 4000, "Cluster_ThreadsRunning": 11, "Cluster_ComSelect": 480},
                           fail={"Cluster_ComReplace"})
        found = load_counters(load, None, "i", "", self.START, self.END)
        self.assertEqual(found["status"], "ok")
        node = found["nodes"]["n1"]
        self.assertEqual(node["com_insert"], (13000, 4000))
        self.assertEqual(node["threads"], (20, 11))
        self.assertNotIn("com_replace", node)  # a counter that cannot be read is left out

    def test_the_running_thread_series_of_the_ranking_is_reused(self):
        calls = []

        def load(settings, start, end, instance, node, metric_name):
            calls.append(metric_name)
            return points(start, 1)
        found = load_counters(load, None, "i", "", self.START, self.END,
                              threads_points=(points(self.START, 20), points(self.START - DAY_US, 10)))
        self.assertNotIn("Cluster_ThreadsRunning", calls)
        self.assertEqual(len(calls), 2 * len(COUNTER_METRICS))
        self.assertEqual(found["nodes"]["n1"]["threads"], (20, 10))

    def test_nothing_readable_is_unavailable(self):
        def load(*args, **kwargs):
            raise RuntimeError("down")
        self.assertEqual(load_counters(load, None, "i", "", self.START, self.END)["status"], "unavailable")


class NodeEvidenceTests(unittest.TestCase):
    def test_a_rise_the_slow_log_cannot_see_says_so_and_names_the_statement_type(self):
        counters = {"com_insert": (13000.0, 4200.0), "com_select": (500.0, 450.0), "com_update": (300.0, 290.0),
                    "threads": (20.0, 11.0)}
        families = [
            dict(fingerprint="todo", action="SELECT", executions=89, baseline_executions=76),
            dict(fingerprint="order", action="INSERT", executions=655, baseline_executions=0),
            dict(fingerprint="order2", action="INSERT", executions=237, baseline_executions=0),
            dict(fingerprint="flat", action="INSERT", executions=5, baseline_executions=9),
        ]
        # 30 minutes; the slow log added 400 seconds of execution time
        block = node_evidence(counters, 400_000_000, 30 * MIN, families)
        self.assertEqual(block["status"], "ok")
        self.assertAlmostEqual(block["slowlog_added_concurrency"], 400 / 1800)
        self.assertAlmostEqual(block["explained_ratio"], (400 / 1800) / 9)
        self.assertTrue(block["low_explained"])
        self.assertEqual(block["lead"]["key"], "com_insert")
        self.assertAlmostEqual(block["lead"]["ratio"], 8800 / 4200)
        self.assertEqual(block["related_families"], ["order", "order2"])

    def test_no_thread_growth_no_explained_share(self):
        block = node_evidence({"threads": (11.0, 10.5), "com_select": (100.0, 100.0)}, 10_000_000, 10 * MIN, [])
        self.assertIsNone(block["explained_ratio"])
        self.assertFalse(block["low_explained"])
        self.assertIsNone(block["lead"])

    def test_the_explained_share_is_capped(self):
        block = node_evidence({"threads": (12.0, 10.0)}, 3600_000_000, 10 * MIN, [])
        self.assertEqual(block["explained_ratio"], 1.0)

    def test_without_counters_unavailable(self):
        self.assertEqual(node_evidence(None, 0, MIN, []), {"status": "unavailable"})


if __name__ == "__main__":
    unittest.main()
