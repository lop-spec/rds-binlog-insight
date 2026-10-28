import json
import tempfile
import unittest
from pathlib import Path

from app.pod_inventory import merge_bindings, parse_ecs, parse_kubeconfig, parse_pods
from app.pod_lookup import name_ips, resolve_pod

POD_LIST = {"items": [
    {"metadata": {"name": "api-7d9c-abcde", "namespace": "default", "uid": "u1", "creationTimestamp": "2026-10-27T07:41:41Z",
                  "ownerReferences": [{"kind": "ReplicaSet", "name": "api-7d9c", "controller": True}]},
     "spec": {"nodeName": "node-a"},
     "status": {"hostIP": "192.0.2.10", "podIP": "198.51.100.5", "podIPs": [{"ip": "198.51.100.5"}], "startTime": "2026-10-27T07:41:42Z"}},
    {"metadata": {"name": "agent", "namespace": "kube-system", "uid": "u2", "creationTimestamp": "2026-10-27T07:00:00Z"},
     "spec": {"nodeName": "node-a", "hostNetwork": True},
     "status": {"hostIP": "192.0.2.10", "podIP": "192.0.2.10"}},
    {"metadata": {"name": "pending", "namespace": "default", "uid": "u3", "creationTimestamp": "2026-10-27T07:00:00Z"},
     "spec": {}, "status": {}},
]}


class PodInventoryTests(unittest.TestCase):
    def test_pods_skip_host_network_and_unassigned(self):
        pods = parse_pods(POD_LIST, "prod")
        self.assertEqual([(p["ip"], p["name"], p["owner"]) for p in pods], [("198.51.100.5", "api-7d9c-abcde", "ReplicaSet/api-7d9c")])
        self.assertEqual(pods[0]["started_us"], 1793086902000000)

    def test_merge_starts_at_pod_start_and_drops_expired(self):
        seen = parse_pods(POD_LIST, "prod")
        now = 1793087000000000
        first = merge_bindings([], seen, now, 10**12)
        self.assertEqual(first[0]["first_seen_us"], seen[0]["started_us"])
        self.assertEqual(first[0]["last_seen_us"], now)
        later = merge_bindings(first, [], now + 10**12 + 1, 10**12)
        self.assertEqual(later, [])

    def test_ecs_addresses(self):
        rows = parse_ecs({"Instances": {"Instance": [{"InstanceId": "i-1", "InstanceName": "host-b",
                          "VpcAttributes": {"PrivateIpAddress": {"IpAddress": ["192.0.2.112"]}},
                          "EipAddress": {"IpAddress": "203.0.113.69"}}]}})
        self.assertEqual({r["ip"] for r in rows}, {"192.0.2.112", "203.0.113.69"})

    def test_kubeconfig_fields(self):
        text = "clusters:\n- cluster:\n    server: https://192.0.2.1:6443\n    certificate-authority-data: Q0E=\nusers:\n- user:\n    client-certificate-data: Q0VSVA==\n    client-key-data: S0VZ\n"
        self.assertEqual(parse_kubeconfig(text)["server"], "https://192.0.2.1:6443")
        with self.assertRaises(ValueError):
            parse_kubeconfig("server: https://x\n")

    def test_names_and_slow_log_detail_from_written_inventory(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "pod-inventory.json"
            now = 1793087000000000
            bindings = merge_bindings([], parse_pods(POD_LIST, "prod"), now, 10**12)
            path.write_text(json.dumps({"schema_version": 1, "instance_ids": ["*"], "bindings": bindings,
                                        "hosts": [{"ip": "192.0.2.112", "name": "host-b", "instance_id": "i-1"}]}))
            names = name_ips(["198.51.100.5", "192.0.2.112", "203.0.113.1", "bad"], now, path)
            self.assertEqual(names["198.51.100.5"]["name"], "api-7d9c-abcde")
            self.assertEqual(names["192.0.2.112"], {"kind": "ecs", "name": "host-b", "instance_id": "i-1"})
            self.assertNotIn("203.0.113.1", names)
            self.assertNotIn("198.51.100.5", name_ips(["198.51.100.5"], bindings[0]["first_seen_us"] - 1, path))
            self.assertEqual(resolve_pod("198.51.100.5", now, path, "rm-any")["status"], "observed_interval")


if __name__ == "__main__":
    unittest.main()
