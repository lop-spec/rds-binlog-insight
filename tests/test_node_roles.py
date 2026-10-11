import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from app.node_roles import NodeRoleBook, normalize_role, parse_cluster_nodes

INSTANCE = "rm-example-cluster"


def attribute(instance=INSTANCE, nodes=None):
    entry = {"DBInstanceId": instance, "DBInstanceDescription": "demo-main"}
    if nodes is not None:
        entry["DBClusterNodes"] = {"DBClusterNode": nodes}
    return {"Items": {"DBInstanceAttribute": [entry]}}


def node(node_id, role, zone="cn-chengdu-a", status="active"):
    return {"NodeId": node_id, "NodeRole": role, "NodeZoneId": zone, "Status": status, "Cpu": "8", "Memory": "16384"}


CLUSTER = [node("rn-example-primary", "primary"),
           node("rn-example-standby-b", "secondary", "cn-chengdu-b"),
           node("rn-example-standby-a", "secondary")]


class ParseTests(unittest.TestCase):
    def test_cluster_nodes_carry_role_zone_and_status(self):
        nodes = parse_cluster_nodes(attribute(nodes=CLUSTER), INSTANCE)
        self.assertEqual(nodes["rn-example-primary"], {"role": "primary", "zone": "cn-chengdu-a", "status": "active"})
        self.assertEqual(nodes["rn-example-standby-b"]["role"], "secondary")
        self.assertEqual(nodes["rn-example-standby-b"]["zone"], "cn-chengdu-b")
        self.assertEqual(len(nodes), 3)

    def test_single_node_dict_and_unknown_role_are_kept_visible(self):
        payload = {"Items": {"DBInstanceAttribute": {"DBInstanceId": INSTANCE,
                   "DBClusterNodes": {"DBClusterNode": node("rn-x", "Witness")}}}}
        self.assertEqual(parse_cluster_nodes(payload, INSTANCE)["rn-x"]["role"], "witness")
        self.assertEqual([normalize_role(v) for v in ("Master", "PRIMARY", "Slave", "", None)],
                         ["primary", "primary", "secondary", "", ""])

    def test_not_a_cluster_edition_has_no_nodes(self):
        self.assertEqual(parse_cluster_nodes(attribute(), INSTANCE), {})

    def test_another_instance_never_labels_these_nodes(self):
        with self.assertRaises(ValueError):
            parse_cluster_nodes(attribute(instance="rm-other", nodes=CLUSTER), INSTANCE)
        with self.assertRaises(ValueError):
            parse_cluster_nodes({"Items": {}}, INSTANCE)


class BookTests(unittest.TestCase):
    def book(self, responses, instances=(INSTANCE,), credential=object()):
        client = Mock()
        client.call.side_effect = responses
        factory = Mock(return_value=client)
        book = NodeRoleBook(lambda: SimpleNamespace(credential_target="t"), lambda _target: credential,
                            lambda: list(instances), factory)
        return book, client, factory

    def test_refresh_loads_roles_with_the_instance_attribute_call(self):
        book, client, factory = self.book([attribute(nodes=CLUSTER)])
        self.assertEqual(book.lookup(INSTANCE), {})  # nothing is fetched on a request path
        book.refresh()
        self.assertEqual(client.call.call_args.args, ("DescribeDBInstanceAttribute", {"DBInstanceId": INSTANCE}))
        self.assertEqual(factory.call_args.kwargs, {"timeout": 15})
        roles = book.lookup(INSTANCE)
        self.assertEqual(roles["rn-example-primary"]["role"], "primary")
        roles["rn-example-primary"]["role"] = "tampered"  # lookup hands out copies
        self.assertEqual(book.lookup(INSTANCE)["rn-example-primary"]["role"], "primary")
        self.assertEqual(book.lookup("rm-unknown"), {})

    def test_failed_refresh_keeps_last_known_roles_and_logs_once_per_streak(self):
        book, _client, _factory = self.book([attribute(nodes=CLUSTER), RuntimeError("boom"), RuntimeError("boom"),
                                             attribute(nodes=CLUSTER), RuntimeError("again")])
        book.refresh()
        with self.assertLogs("app.node_roles", "WARNING") as logs:
            book.refresh()
            book.refresh()
        self.assertEqual(len(logs.output), 1)
        self.assertEqual(book.lookup(INSTANCE)["rn-example-standby-a"]["role"], "secondary")
        book.refresh()
        with self.assertLogs("app.node_roles", "WARNING") as logs:
            book.refresh()  # a new streak is logged again
        self.assertEqual(len(logs.output), 1)

    def test_failover_is_logged(self):
        swapped = [node("rn-example-primary", "secondary"), CLUSTER[1], node("rn-example-standby-a", "primary")]
        book, _client, _factory = self.book([attribute(nodes=CLUSTER), attribute(nodes=swapped)])
        book.refresh()
        with self.assertLogs("app.node_roles", "WARNING") as logs:
            book.refresh()
        self.assertEqual(sum("role changed" in line for line in logs.output), 2)
        self.assertEqual(book.lookup(INSTANCE)["rn-example-standby-a"]["role"], "primary")

    def test_missing_credential_is_reported_not_raised(self):
        book, client, _factory = self.book([], credential=None)
        with self.assertLogs("app.node_roles", "WARNING") as logs:
            book.refresh()
        self.assertIn("cloud_credential_unavailable", logs.output[0])
        client.call.assert_not_called()
        self.assertEqual(book.lookup(INSTANCE), {})


if __name__ == "__main__":
    unittest.main()
