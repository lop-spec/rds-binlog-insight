from __future__ import annotations

import tempfile
import unittest
from contextlib import nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from app.clickhouse_client import ClickHouseConfig
from app.clickhouse_raw_oss import ClickHouseRawOssConfig
from app.clickhouse_raw_worker import _sync_progressed, run_worker
from app.io_pressure import IoPressurePaused


def _clickhouse_config() -> ClickHouseConfig:
    return ClickHouseConfig(
        enabled=True,
        serving_enabled=False,
        host="clickhouse",
        port=8123,
        database="insight",
        table="events",
        user="test",
        password="secret",
        hot_hours=27,
        serving_hours=25,
        reconcile_seconds=30,
        idle_seconds=1.0,
        health_url="http://insight:8769/api/storage",
        health_host_header="",
        health_max_seconds=1.0,
        min_free_gb=20,
        io_pressure_max_full_avg10=10.0,
    )


def _raw_config() -> ClickHouseRawOssConfig:
    return ClickHouseRawOssConfig(
        enabled=True,
        serving_enabled=False,
        manifest_table="oss_active_parts_v1",
        packed_table="events_query_packed_v1",
        prefix="sql-insight-clickhouse/raw-v1/",
        cache_gb=20,
    )


class RawOssWorkerAdmissionTest(unittest.TestCase):
    def test_deferred_source_scan_without_applied_changes_is_idle(self):
        deferred = {
            "rounds": 1,
            "scanned": 6,
            "inserted": 0,
            "acknowledged": 0,
            "deferred": 6,
        }
        self.assertFalse(_sync_progressed(None, deferred, deferred))
        self.assertTrue(_sync_progressed({"state": "ready"}, deferred, deferred))
        self.assertTrue(
            _sync_progressed(None, deferred, {**deferred, "acknowledged": 1})
        )

    def test_healthy_serving_canary_overrides_host_psi_for_one_bounded_part(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            paths = {
                "root": root,
                "index": root / "index",
                "scratch": root / "scratch",
                "logs": root / "logs",
            }
            for path in paths.values():
                path.mkdir(parents=True, exist_ok=True)
            metadata = SimpleNamespace(
                load_settings=lambda: SimpleNamespace(credential_target="test"),
                clickhouse_change_tracking_state=lambda: {
                    "complete": True,
                    "pending": True,
                },
                close=lambda: None,
            )
            manifest = SimpleNamespace(
                recover_loading=lambda: 0,
                stats=lambda: {"pending_parts": 1},
            )
            client = SimpleNamespace(ping=lambda: "test")
            status_updates: list[dict[str, object]] = []
            pressure = IoPressurePaused(
                "host I/O pressure exceeded safety ceiling"
            )
            reconcile_result = {"scanned": 0}

            with (
                patch(
                    "app.clickhouse_raw_worker.ClickHouseConfig.from_env",
                    return_value=_clickhouse_config(),
                ),
                patch(
                    "app.clickhouse_raw_worker.ClickHouseRawOssConfig.from_env",
                    return_value=_raw_config(),
                ),
                patch(
                    "app.clickhouse_raw_worker.ensure_data_dirs",
                    return_value=paths,
                ),
                patch(
                    "app.clickhouse_raw_worker._worker_lock",
                    return_value=nullcontext(),
                ),
                patch(
                    "app.clickhouse_raw_worker.MetadataStore",
                    return_value=metadata,
                ),
                patch(
                    "app.clickhouse_raw_worker.ClickHouseManifest",
                    return_value=manifest,
                ),
                patch(
                    "app.clickhouse_raw_worker.ClickHouseClient",
                    return_value=client,
                ),
                patch(
                    "app.clickhouse_raw_worker.OssArchive",
                    return_value=SimpleNamespace(),
                ),
                patch(
                    "app.clickhouse_raw_worker.load_credential",
                    return_value=SimpleNamespace(),
                ),
                patch(
                    "app.clickhouse_raw_worker.HealthCanary",
                    return_value=SimpleNamespace(probe=lambda *args, **kwargs: 0.01),
                ),
                patch(
                    "app.clickhouse_raw_worker._admit_io_pressure",
                    return_value=(True, pressure),
                ),
                patch(
                    "app.clickhouse_raw_worker.apply_pending_raw_oss_changes",
                    return_value=reconcile_result,
                ),
                patch("app.clickhouse_raw_worker.ingest_one") as ingest,
                patch(
                    "app.clickhouse_raw_worker.write_json_status",
                    side_effect=lambda _path, payload: status_updates.append(payload),
                ),
            ):
                ingest.return_value = {"state": "ready", "part_path": "part-1"}
                result = run_worker(root, once=True)

            self.assertEqual(result, 0)
            self.assertEqual(ingest.call_count, 1)
            self.assertTrue(ingest.call_args.kwargs["allow_high_io_pressure"])
            self.assertTrue(status_updates[-1]["ioPressureCanaryOverride"])


if __name__ == "__main__":
    unittest.main()
