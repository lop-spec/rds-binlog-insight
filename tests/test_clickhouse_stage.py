from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.clickhouse_client import ClickHouseConfig
from app.clickhouse_manifest import ClickHouseManifest
from app.clickhouse_oss import ClickHouseOssConfig
from app.clickhouse_stage import StagedBatchLoader
from app.config import Settings
from app.maintenance_status import write_json_status


SHA = "a" * 64


def _part() -> dict[str, object]:
    return {
        "path": "/data/index/part.parquet",
        "logical_part_id": "part-a",
        "sha256": SHA,
        "content_revision": 7,
        "row_count": 3,
        "size_bytes": 100,
        "oss_key": "mysql-binlog/rm-test/part.parquet",
        "oss_length": 0,
        "min_event_epoch_us": 1,
        "max_event_epoch_us": 2,
    }


def _config() -> ClickHouseConfig:
    return ClickHouseConfig(
        enabled=True,
        serving_enabled=False,
        host="clickhouse",
        port=8123,
        database="insight",
        table="events_query_oss_all_v3",
        user="test",
        password="test",
        hot_hours=27,
        serving_hours=25,
        reconcile_seconds=30,
        idle_seconds=1,
        health_url="",
        health_host_header="",
        health_max_seconds=1,
        min_free_gb=20,
        io_pressure_max_full_avg10=10,
        query_table="events_query_oss_all_v3",
        name_query_table="events_query_by_name_oss_all_v3",
        ingest_mode="query",
    )


def _oss_config() -> ClickHouseOssConfig:
    return ClickHouseOssConfig(
        enabled=True,
        serving_enabled=False,
        prefix="sql-insight-clickhouse/v3/",
        cache_gb=40,
        query_table="events_query_oss_all_v3",
        name_query_table="events_query_by_name_oss_all_v3",
        materialized_view="events_query_oss_all_v3_to_name_mv",
        manifest_name="oss-all-v3-manifest.sqlite3",
        history_days=0,
        staged_backfill_enabled=True,
        incremental_mv_enabled=False,
        stage_query_table="events_query_oss_all_v3_stage",
        stage_name_query_table="events_query_by_name_oss_all_v3_stage",
    )


def _settings() -> Settings:
    return Settings(
        oss_enabled=True,
        oss_bucket="example-binlog-bucket",
        oss_region_id="cn-hangzhou",
        oss_endpoint="oss-cn-hangzhou-internal.aliyuncs.com",
        oss_prefix="mysql-binlog/rm-test/",
    )


def _state(*, time_rows: bool, name_rows: bool) -> dict[str, object]:
    result: dict[str, object] = {
        "rows": 3 if time_rows else 0,
        "sha_count": 1 if time_rows else 0,
        "sha256": SHA if time_rows else "",
        "min_revision": 7 if time_rows else 0,
        "max_revision": 7 if time_rows else 0,
        "name_rows": 3 if name_rows else 0,
        "name_sha_count": 1 if name_rows else 0,
        "name_sha256": SHA if name_rows else "",
        "name_min_revision": 7 if name_rows else 0,
        "name_max_revision": 7 if name_rows else 0,
    }
    return result


class _StageClient:
    def __init__(self) -> None:
        self.stage_time = False
        self.stage_name = False
        self.final_time = False
        self.final_name = False
        self.extra_stage_time = False
        self.extra_stage_name = False
        self.inject_extra_stage_time = False
        self.fail_copy = False
        self.calls: list[str] = []
        self.all_state_calls: list[str] = []
        self.summary_calls: list[str] = []

    def paired_part_states_for_tables(
        self, identities, *, time_table, name_table
    ):
        staged = time_table.endswith("_stage")
        return {
            identity: _state(
                time_rows=self.stage_time if staged else self.final_time,
                name_rows=self.stage_name if staged else self.final_name,
            )
            for identity in identities
        }

    def table_part_summary(self, table):
        self.summary_calls.append(table)
        if table.endswith("name_oss_all_v3_stage"):
            present = self.stage_name
            extra = self.extra_stage_name
        elif table.endswith("v3_stage"):
            present = self.stage_time
            extra = self.extra_stage_time
        else:
            present = False
            extra = False
        count = int(present) + int(extra)
        return {"rows": 3 * count, "part_count": count}

    def table_storage_summary(self, table):
        if table.endswith("name_oss_all_v3_stage"):
            present = self.stage_name
            extra = self.extra_stage_name
        elif table.endswith("v3_stage"):
            present = self.stage_time
            extra = self.extra_stage_time
        else:
            present = False
            extra = False
        count = int(present) + int(extra)
        return {"rows": 3 * count, "active_parts": count, "partitions": count}

    def all_part_states_for_table(self, table):
        self.all_state_calls.append(table)
        if table.endswith("name_oss_all_v3_stage"):
            present = self.stage_name
            extra = self.extra_stage_name
        else:
            present = self.stage_time
            extra = self.extra_stage_time
        states = {}
        if present:
            states["part-a"] = {
                key: value
                for key, value in _state(time_rows=True, name_rows=False).items()
                if not key.startswith("name_")
            }
        if extra:
            states["unexpected-part"] = {
                key: value
                for key, value in _state(time_rows=True, name_rows=False).items()
                if not key.startswith("name_")
            }
        return states

    def query(self, sql, **_kwargs):
        self.calls.append(sql)
        if "INSERT INTO insight.events_query_oss_all_v3_stage" in sql:
            self.stage_time = True
            if self.inject_extra_stage_time:
                self.extra_stage_time = True
        return ""

    def copy_table(self, *, source, destination):
        self.calls.append(f"COPY {source} {destination}")
        if self.fail_copy:
            raise RuntimeError("copy failed")
        if not self.stage_time:
            raise RuntimeError("time stage is empty")
        self.stage_name = True

    def active_partitions(self, table):
        if table.endswith("name_oss_all_v3_stage"):
            present = self.stage_name
        else:
            present = self.stage_time
        return ["2026-08-24"] if present else []

    def move_partitions(self, *, source, destination, partitions):
        self.calls.append(f"MOVE {source} {destination} {partitions}")
        if source.endswith("name_oss_all_v3_stage"):
            self.stage_name = False
            self.final_name = True
        else:
            self.stage_time = False
            self.final_time = True

    def truncate_table(self, table):
        self.calls.append(f"TRUNCATE {table}")
        if table.endswith("name_oss_all_v3_stage"):
            self.stage_name = False
        else:
            self.stage_time = False


class ClickHouseStageTests(unittest.TestCase):
    def _loader(self, temp: str, client: _StageClient):
        part = _part()
        manifest = ClickHouseManifest(
            Path(temp) / "manifest.sqlite3", run_migrations=True
        )
        manifest.reconcile(
            [part], start_epoch_us=0, end_epoch_us=10, source_parts=1
        )
        journal = Path(temp) / "stage-journal.json"
        loader = StagedBatchLoader(
            client=client,
            manifest=manifest,
            settings=_settings(),
            config=_config(),
            oss_config=_oss_config(),
            journal_path=journal,
        )
        return part, manifest, journal, loader

    def test_staged_batch_commits_both_tables_before_manifest(self):
        with tempfile.TemporaryDirectory() as temp:
            client = _StageClient()
            part, manifest, journal, loader = self._loader(temp, client)

            ready, failed = loader.load([part])

            self.assertEqual((ready, failed), (1, 0))
            self.assertTrue(client.final_time)
            self.assertTrue(client.final_name)
            self.assertFalse(client.stage_time)
            self.assertFalse(client.stage_name)
            self.assertFalse(journal.exists())
            self.assertEqual(manifest.stats()["ready_parts"], 1)
            self.assertLess(
                client.calls.index(
                    "MOVE insight.events_query_by_name_oss_all_v3_stage "
                    "insight.events_query_by_name_oss_all_v3 ['2026-08-24']"
                ),
                client.calls.index(
                    "MOVE insight.events_query_oss_all_v3_stage "
                    "insight.events_query_oss_all_v3 ['2026-08-24']"
                ),
            )

    def test_hot_path_reads_each_complete_stage_state_once(self):
        with tempfile.TemporaryDirectory() as temp:
            client = _StageClient()
            part, _manifest, _journal, loader = self._loader(temp, client)

            ready, failed = loader.load([part])

            self.assertEqual((ready, failed), (1, 0))
            self.assertEqual(
                client.all_state_calls,
                [
                    "insight.events_query_oss_all_v3_stage",
                    "insight.events_query_by_name_oss_all_v3_stage",
                ],
            )
            self.assertEqual(client.summary_calls, [])

    def test_unexpected_stage_part_is_rejected_before_final_move(self):
        with tempfile.TemporaryDirectory() as temp:
            client = _StageClient()
            client.inject_extra_stage_time = True
            part, manifest, journal, loader = self._loader(temp, client)

            ready, failed = loader.load([part])

            self.assertEqual((ready, failed), (0, 1))
            self.assertFalse(client.final_time)
            self.assertFalse(client.final_name)
            self.assertFalse(client.stage_time)
            self.assertFalse(client.stage_name)
            self.assertFalse(journal.exists())
            self.assertEqual(manifest.stats()["failed_parts"], 1)

    def test_failed_stage_copy_is_discarded_without_final_rows(self):
        with tempfile.TemporaryDirectory() as temp:
            client = _StageClient()
            client.fail_copy = True
            part, manifest, journal, loader = self._loader(temp, client)

            ready, failed = loader.load([part])

            self.assertEqual((ready, failed), (0, 1))
            self.assertFalse(client.final_time)
            self.assertFalse(client.final_name)
            self.assertFalse(client.stage_time)
            self.assertFalse(client.stage_name)
            self.assertFalse(journal.exists())
            self.assertEqual(manifest.stats()["failed_parts"], 1)

    def test_recovery_finishes_time_move_after_name_move(self):
        with tempfile.TemporaryDirectory() as temp:
            client = _StageClient()
            client.stage_time = True
            client.final_name = True
            part, manifest, journal, loader = self._loader(temp, client)
            write_json_status(
                journal,
                {
                    "schema": 1,
                    "state": "name-moved",
                    "parts": [
                        {
                            "path": part["path"],
                            "logical_part_id": part["logical_part_id"],
                            "sha256": part["sha256"],
                            "content_revision": part["content_revision"],
                            "row_count": part["row_count"],
                        }
                    ],
                },
            )

            recovered = loader.recover()

            self.assertEqual(recovered, 1)
            self.assertTrue(client.final_time)
            self.assertTrue(client.final_name)
            self.assertFalse(journal.exists())
            self.assertEqual(manifest.stats()["ready_parts"], 1)


if __name__ == "__main__":
    unittest.main()
