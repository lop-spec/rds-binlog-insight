from __future__ import annotations

import unittest

from tools.clickhouse_oss_verify import (
    _status_name,
    evaluate_full_gate,
    evaluate_ready_pilot_gate,
    inventory_snapshot,
    verify_remote_parts,
)


SHA = "a" * 64


def _part(identity: str, rows: int = 3) -> dict[str, object]:
    return {
        "path": f"/data/index/{identity}.parquet",
        "logical_part_id": identity,
        "sha256": SHA,
        "content_revision": 7,
        "row_count": rows,
        "size_bytes": 100,
        "oss_key": f"mysql-binlog/{identity}.parquet",
        "oss_offset": 0,
        "oss_length": 0,
    }


def _state(rows: int = 3) -> dict[str, object]:
    return {
        "rows": rows,
        "sha_count": 1,
        "sha256": SHA,
        "min_revision": 7,
        "max_revision": 7,
        "name_rows": rows,
        "name_sha_count": 1,
        "name_sha256": SHA,
        "name_min_revision": 7,
        "name_max_revision": 7,
    }


class _Client:
    def __init__(self, states):
        self.states = states
        self.batches = []

    def paired_part_states_for_tables(
        self, identities, *, time_table, name_table
    ):
        self.batches.append((list(identities), time_table, name_table))
        return {identity: self.states.get(identity, {}) for identity in identities}


class ClickHouseOssVerifyTests(unittest.TestCase):
    def test_v3_status_file_does_not_collide_with_v2(self):
        self.assertEqual(
            _status_name("oss-all-manifest.sqlite3"),
            "clickhouse-oss-verify-status.json",
        )
        self.assertEqual(
            _status_name("oss-all-v3-manifest.sqlite3"),
            "clickhouse-oss-all-v3-verify-status.json",
        )

    def test_inventory_is_order_independent_and_detects_range_parts(self):
        direct = _part("direct")
        ranged = {**_part("ranged"), "oss_length": 123}

        left = inventory_snapshot([direct, ranged])
        right = inventory_snapshot([ranged, direct])

        self.assertEqual(left, right)
        self.assertEqual(left["parts"], 2)
        self.assertEqual(left["rows"], 6)
        self.assertEqual(left["direct_parts"], 1)
        self.assertEqual(left["ranged_parts"], 1)

    def test_remote_verifier_checks_both_tables_in_bounded_batches(self):
        parts = [_part("a"), _part("b")]
        client = _Client({"a": _state(), "b": _state()})

        result = verify_remote_parts(
            client,
            parts,
            time_table="insight.time",
            name_table="insight.name",
            batch_size=1,
        )

        self.assertTrue(result["exact"])
        self.assertEqual(result["checked_parts"], 2)
        self.assertEqual(len(client.batches), 2)
        self.assertTrue(all(batch[1:] == ("insight.time", "insight.name") for batch in client.batches))

    def test_remote_verifier_reports_name_table_mismatch(self):
        state = _state()
        state["name_rows"] = 2

        result = verify_remote_parts(
            _Client({"a": state}),
            [_part("a")],
            time_table="insight.time",
            name_table="insight.name",
        )

        self.assertFalse(result["exact"])
        self.assertEqual(result["mismatch_parts"], 1)
        self.assertIn("name_rows=2/3", result["mismatches"][0])

    def test_full_gate_rejects_partial_coverage_or_extra_target_rows(self):
        inventory = inventory_snapshot([_part("a")])
        common = {
            "source_inventory": inventory,
            "source_parts": 1,
            "manifest_coverage": {
                "complete": True,
                "total_parts": 1,
                "covered_parts": 1,
                "covered_rows": 3,
            },
            "manifest_stats": {
                "ready_parts": 1,
                "pending_parts": 0,
                "failed_parts": 0,
                "delete_parts": 0,
                "last_error": "",
            },
            "remote": {
                "exact": True,
                "checked_parts": 1,
                "checked_rows": 3,
            },
            "time_summary": {"rows": 3},
            "name_summary": {"rows": 3},
            "stage_time_summary": {"rows": 0},
            "stage_name_summary": {"rows": 0},
            "object_statuses": {
                "time": {"exists": True, "engine": "MergeTree"},
                "name": {"exists": True, "engine": "MergeTree"},
                "stage_time": {"exists": True, "engine": "MergeTree"},
                "stage_name": {"exists": True, "engine": "MergeTree"},
                "materialized_view": {
                    "exists": True,
                    "engine": "MaterializedView",
                },
            },
            "incremental_mv_enabled": True,
            "journal_exists": False,
            "source_stable": True,
        }

        self.assertTrue(evaluate_full_gate(**common)["ok"])
        partial = dict(common)
        partial["manifest_coverage"] = {
            **common["manifest_coverage"],
            "complete": False,
        }
        self.assertFalse(evaluate_full_gate(**partial)["ok"])
        extra = dict(common)
        extra["name_summary"] = {"rows": 4}
        self.assertFalse(evaluate_full_gate(**extra)["ok"])

    def test_ready_pilot_is_exact_but_never_a_full_cutover_claim(self):
        parts = [_part("a")]
        gate = evaluate_ready_pilot_gate(
            source_parts=parts,
            source_identity_errors=[],
            manifest_stats={
                "ready_parts": 1,
                "ready_rows": 3,
                "failed_parts": 0,
                "delete_parts": 0,
                "last_error": "",
            },
            remote={"checked_parts": 1, "exact": True},
            time_summary={"rows": 3},
            name_summary={"rows": 3},
            stage_time_summary={"rows": 0},
            stage_name_summary={"rows": 0},
            journal_exists=False,
        )
        missing_name = evaluate_ready_pilot_gate(
            source_parts=parts,
            source_identity_errors=[],
            manifest_stats={
                "ready_parts": 1,
                "ready_rows": 3,
                "failed_parts": 0,
                "delete_parts": 0,
                "last_error": "",
            },
            remote={"checked_parts": 1, "exact": True},
            time_summary={"rows": 3},
            name_summary={"rows": 2},
            stage_time_summary={"rows": 0},
            stage_name_summary={"rows": 0},
            journal_exists=False,
        )

        self.assertTrue(gate["ok"])
        self.assertFalse(missing_name["ok"])


if __name__ == "__main__":
    unittest.main()
