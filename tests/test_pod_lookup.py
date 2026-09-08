import json
import tempfile
import unittest
from pathlib import Path

from app.pod_lookup import attach_pod_detail, resolve_pod


class PodLookupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "pods.json"
        self.binding = dict(ip="192.0.2.1", cluster="cluster", namespace="ns", name="pod",
                            uid="uid", node="node", host_ip="192.0.2.2", owner="deployment/app",
                            created_us=10, first_seen_us=20, last_seen_us=30)
        self.save([self.binding])

    def save(self, bindings):
        self.path.write_text(json.dumps(dict(schema_version=1, instance_ids=["database"], bindings=bindings)))

    def lookup(self, timestamp=25):
        return resolve_pod("192.0.2.1", timestamp, self.path, "database")

    def test_observed_interval_retains_uid_and_provenance(self):
        result = self.lookup()
        self.assertEqual(result["status"], "observed_interval")
        self.assertEqual(result["pods"][0]["uid"], "uid")
        self.assertIn("polling", result["note"])

    def test_current_ip_is_not_past_identity(self):
        with self.assertLogs("app.pod_lookup", "WARNING"):
            before = self.lookup(15)
            recycled = self.lookup(5)
            stale = self.lookup(35)
        self.assertEqual(before["status"], "historical_binding_unverified")
        self.assertEqual(recycled["status"], "ip_not_observed")
        self.assertEqual(stale["status"], "historical_binding_unverified")

    def test_host_network_ambiguity_not_arbitrarily_chosen(self):
        self.save([self.binding, dict(self.binding, uid="other", name="other")])
        with self.assertLogs("app.pod_lookup", "WARNING"):
            result = self.lookup()
        self.assertEqual(result["status"], "ambiguous_ip")
        self.assertEqual(len(result["pods"]), 2)

    def test_instance_scope_prevents_cross_cluster_guess(self):
        with self.assertLogs("app.pod_lookup", "WARNING"):
            result = resolve_pod("192.0.2.1", 25, self.path, "other-database")
        self.assertEqual(result["status"], "instance_unconfigured")

    def test_missing_or_broken_file_is_explicit(self):
        for content, expected in [("not json", "invalid_inventory"), (None, "inventory_unconfigured")]:
            if content is None:
                self.path.unlink()
            else:
                self.path.write_text(content)
            with self.assertLogs("app.pod_lookup", "WARNING"):
                self.assertEqual(self.lookup()["status"], expected)

    def test_binlog_details_unchanged(self):
        row = {"raw_event_type": "ROWS_EVENT"}
        self.assertEqual(attach_pod_detail(row, self.path.parent), row)
        self.assertNotIn("pod", row)


if __name__ == "__main__":
    unittest.main()
