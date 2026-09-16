"""Version/key-prefilter regression; writes only to the isolated CI service."""
from __future__ import annotations

import os
import unittest
from dataclasses import replace

from app.clickhouse_client import ClickHouseClient, ClickHouseConfig
from app.clickhouse_raw_oss import (
    ClickHouseRawOssConfig,
    build_raw_oss_candidate_sql,
    build_raw_oss_schema,
)
from app.config import Settings


def fixture_rows():
    base = dict(
        part_path="", logical_part_id="", sha256="a" * 64,
        content_revision=1, source_kind="mysql-binlog", instance_id="rm-fixture",
        event_date="2026-01-01", min_event_epoch_us=120, max_event_epoch_us=180,
        row_count=3, size_bytes=40, oss_path="bucket/a.parquet", oss_key="a.parquet",
        oss_offset=0, oss_length=0, catalog_ready=1,
        database_names=["orders"], table_names=["items"], operations=["INSERT"],
        change_version=1, is_deleted=0, updated_at="2026-01-01 00:00:00.000",
    )
    rows = []

    def add(path, version=1, **changes):
        rows.append({**base, "part_path": path, "logical_part_id": path,
                     "change_version": version, "content_revision": version,
                     "oss_key": f"{path}-v{version}.parquet", **changes})

    add("deleted")
    add("deleted", 2, is_deleted=1, source_kind="", instance_id="",
        min_event_epoch_us=0, max_event_epoch_us=0, catalog_ready=0)
    add("changed-source")
    add("changed-source", 2, source_kind="mysql-slow-log")
    add("changed-instance")
    add("changed-instance", 2, instance_id="other-instance")
    add("outside-window")
    add("outside-window", 2, min_event_epoch_us=390, max_event_epoch_us=400)
    add("inside-window", min_event_epoch_us=390, max_event_epoch_us=400)
    add("inside-window", 2)
    add("changed-catalog")
    add("changed-catalog", 2, table_names=["unrelated"])
    add("changed-database")
    add("changed-database", 2, database_names=["unrelated"])
    add("changed-operation")
    add("changed-operation", 2, operations=["DELETE"])
    add("unknown-catalog", catalog_ready=0, database_names=[], table_names=[], operations=[])
    add("packed", oss_length=100, oss_offset=32, max_event_epoch_us=200)
    add("general", source_kind="mysql-general-log", max_event_epoch_us=190)
    add("tie-a", max_event_epoch_us=200)
    add("tie-b", max_event_epoch_us=200)
    add("revised")
    add("revised", 2, oss_length=80, oss_offset=40, sha256="b" * 64)
    add("irrelevant", source_kind="unrelated-source")
    return rows


def oracle(rows, query, start=100, end=200):
    """Independent latest-version evaluation, not a rewrite of generated SQL."""
    latest = {}
    for row in rows:
        previous = latest.get(row["part_path"])
        if previous is None or row["change_version"] > previous["change_version"]:
            latest[row["part_path"]] = row
    allowed = {"mysql-binlog", "mysql-general-log"}
    if query["source"] == "database":
        allowed.add("mysql-slow-log")
    result = []
    for row in latest.values():
        if row["is_deleted"] or row["source_kind"] not in allowed:
            continue
        if row["max_event_epoch_us"] < start or row["min_event_epoch_us"] > end:
            continue
        if query.get("instance") and row["instance_id"].lower() != query["instance"].lower():
            continue
        if row["catalog_ready"]:
            if any(query.get(key) and not any(query[key].lower() in name.lower()
                       for name in row[column]) for key, column in
                       (("database", "database_names"), ("table", "table_names"))):
                continue
            if query.get("operations") and not set(query["operations"]) & set(row["operations"]):
                continue
        result.append(row)
    return sorted(result, key=lambda row: (row["max_event_epoch_us"], row["part_path"]), reverse=True)


def fixture_manifest_ddl(raw, database):
    schema = build_raw_oss_schema(
        Settings(oss_enabled=True, oss_bucket="fixture-only",
                 oss_endpoint="oss-cn-hangzhou-internal.aliyuncs.com",
                 oss_region_id="cn-hangzhou"), raw, database=database,
    )
    table = f"{database}.{raw.manifest_table}"
    return next(s for s in schema.split(";")
                if f"CREATE TABLE IF NOT EXISTS {table}\n" in s)


class RawCandidateFixtureSetup(unittest.TestCase):
    def test_manifest_setup_needs_no_cloud_service_or_credential(self):
        raw = replace(ClickHouseRawOssConfig.from_env(),
                      manifest_table="raw_candidate_ci_fixture")
        ddl = fixture_manifest_ddl(raw, "mongo_ci_fixture")
        self.assertIn("ReplacingMergeTree(change_version, is_deleted)", ddl)
        self.assertIn("ORDER BY part_path", ddl)
        self.assertNotIn("packed-events", ddl)
        self.assertNotIn("disk =", ddl)


@unittest.skipUnless(os.environ.get("RAW_CANDIDATE_CI_FIXTURE") == "1",
                     "isolated CI ClickHouse fixture only")
class RawCandidateClickHouse(unittest.TestCase):
    def test_versions_tombstones_catalogs_and_cursor_match_independent_oracle(self):
        config = ClickHouseConfig.from_env()
        self.assertEqual(config.database, "mongo_ci_fixture")
        self.assertEqual(config.host, "127.0.0.1")
        self.assertEqual(config.port, 18123)
        client = ClickHouseClient(config)
        raw = replace(ClickHouseRawOssConfig.from_env(), manifest_table="raw_candidate_ci_fixture")
        table = f"{config.database}.{raw.manifest_table}"
        # Only the production manifest DDL; no S3 table, credentials or OSS writes.
        client.query(fixture_manifest_ddl(raw, config.database))
        try:
            # Keep versions physically separate so accidental pre-FINAL filtering
            # cannot be masked by an opportunistic background merge.
            client.query(f"SYSTEM STOP MERGES {table}")
            fixture = fixture_rows()
            for version in (1, 2):
                client.insert_json_rows(table, [r for r in fixture if r["change_version"] == version])
            for source in ("binlog", "database"):
                for filters in (
                    {},
                    {"instance": "RM-FIXTURE"},
                    {"instance": "rm-fixture", "database": "ORDER", "table": "item",
                     "operations": ["INSERT"]},
                    {"database": "not-present", "table": "not-present"},
                    {"operations": ["DELETE"]},
                ):
                    query = {"source": source, **filters}
                    with self.subTest(query=query):
                        expected = oracle(fixture, query)
                        actual = []
                        cursor = {}
                        for _ in range(len(fixture) + 1):
                            sql, parameters = build_raw_oss_candidate_sql(
                                raw, database=config.database, query=query,
                                start_epoch_us=100, end_epoch_us=200, limit=2, **cursor,
                            )
                            page = client.json_rows(sql, parameters=parameters)
                            if not page:
                                break
                            self.assertLessEqual(len(page), 2)
                            actual.extend(page)
                            cursor = dict(cursor_max_event_epoch_us=int(page[-1]["max_event_epoch_us"]),
                                          cursor_part_path=page[-1]["part_path"])
                        else:
                            self.fail("candidate cursor did not reach a terminal empty page")
                        self.assertEqual([r["part_path"] for r in actual], [r["part_path"] for r in expected])
                        for got, want in zip(actual, expected):
                            for name in ("part_path", "logical_part_id", "sha256", "oss_path", "oss_key"):
                                self.assertEqual(got[name], want[name], name)
                            for name in ("content_revision", "row_count", "size_bytes", "oss_length",
                                         "oss_offset", "min_event_epoch_us", "max_event_epoch_us"):
                                self.assertEqual(int(got[name]), want[name], name)
        finally:
            client.query(f"DROP TABLE {table}")


if __name__ == "__main__":
    unittest.main()
