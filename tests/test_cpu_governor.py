from __future__ import annotations

import unittest
from pathlib import Path
from unittest.mock import Mock

from deploy.cpu_governor import (
    LIMITED_CPU_LIMIT,
    NO_CPU_LIMIT,
    CpuTimes,
    DockerCgroupReader,
    DockerCpuLimit,
    DockerCpuUsage,
    cpu_usage_percent,
    external_cpu_usage_percent,
    limit_after_probe_error,
    parse_args,
    target_limit,
)


class CpuUsageTests(unittest.TestCase):
    def test_cpu_usage_uses_host_deltas(self) -> None:
        before = CpuTimes(total=1_000, idle=800)
        after = CpuTimes(total=1_100, idle=890)

        self.assertEqual(cpu_usage_percent(before, after), 10.0)

    def test_exact_threshold_stays_fail_safe(self) -> None:
        self.assertEqual(target_limit([19.9, 19.9, 19.9], 20.0), NO_CPU_LIMIT)
        self.assertEqual(
            target_limit([20.0, 20.0, 20.0], 20.0), LIMITED_CPU_LIMIT
        )
        self.assertEqual(
            target_limit([20.1, 20.1, 20.1], 20.0), LIMITED_CPU_LIMIT
        )

    def test_external_usage_excludes_both_insight_containers(self) -> None:
        before = CpuTimes(total=1_000, idle=800)
        after = CpuTimes(total=2_000, idle=1_300)
        before_managed = {
            "rds-binlog-insight": 1_000_000,
            "rds-binlog-insight-indexer": 2_000_000,
        }
        after_managed = {
            "rds-binlog-insight": 1_800_000,
            "rds-binlog-insight-indexer": 2_200_000,
        }

        external = external_cpu_usage_percent(
            before,
            after,
            before_managed,
            after_managed,
            clock_ticks=100,
        )

        self.assertEqual(external, 40.0)
        self.assertEqual(
            target_limit([external] * 3, 40.0), LIMITED_CPU_LIMIT
        )

    def test_insight_cpu_alone_does_not_trigger_limit(self) -> None:
        before = CpuTimes(total=1_000, idle=800)
        after = CpuTimes(total=2_000, idle=1_300)
        before_managed = {
            "rds-binlog-insight": 1_000_000,
            "rds-binlog-insight-indexer": 2_000_000,
        }
        after_managed = {
            "rds-binlog-insight": 4_000_000,
            "rds-binlog-insight-indexer": 3_000_000,
        }

        external = external_cpu_usage_percent(
            before,
            after,
            before_managed,
            after_managed,
            clock_ticks=100,
        )

        self.assertEqual(external, 10.0)
        self.assertEqual(target_limit([external] * 3, 40.0), NO_CPU_LIMIT)

    def test_container_set_changes_are_safe_for_one_sample(self) -> None:
        before = CpuTimes(total=1_000, idle=800)
        after = CpuTimes(total=2_000, idle=1_300)
        external = external_cpu_usage_percent(
            before,
            after,
            {"stable": 1_000_000, "stopped": 5_000_000},
            {"stable": 2_000_000, "started": 1_000_000},
            clock_ticks=100,
        )
        self.assertEqual(external, 40.0)

    def test_probe_error_preserves_the_last_applied_limit(self) -> None:
        self.assertEqual(
            limit_after_probe_error(NO_CPU_LIMIT), NO_CPU_LIMIT
        )
        self.assertEqual(
            limit_after_probe_error(LIMITED_CPU_LIMIT), LIMITED_CPU_LIMIT
        )


class DockerCgroupReaderTests(unittest.TestCase):
    def test_reads_host_cgroup_without_entering_busy_container(self) -> None:
        run = Mock()
        run.side_effect = [
            "1234",
            "0::/system.slice/docker-abc.scope",
            "usage_usec 5678\nuser_usec 5000\nsystem_usec 678",
        ]
        reader = DockerCgroupReader(run=run)

        self.assertIn("usage_usec 5678", reader.read("insight", "cpu.stat"))
        calls = [item.args[0] for item in run.call_args_list]
        self.assertEqual(
            calls[0],
            ["docker", "inspect", "--format", "{{.State.Pid}}", "insight"],
        )
        self.assertEqual(calls[1], ["cat", "/proc/1234/cgroup"])
        self.assertEqual(
            calls[2],
            ["cat", "/sys/fs/cgroup/system.slice/docker-abc.scope/cpu.stat"],
        )
        self.assertFalse(any(call[:2] == ["docker", "exec"] for call in calls))


class DockerCpuUsageTests(unittest.TestCase):
    def test_reads_usage_usec_for_each_excluded_container(self) -> None:
        reader = Mock()
        reader.read.side_effect = [
            "usage_usec 1234\nuser_usec 1000\nsystem_usec 234",
            "usage_usec 5678\nuser_usec 5000\nsystem_usec 678",
        ]
        usage = DockerCpuUsage(
            ["rds-binlog-insight", "rds-binlog-insight-indexer"],
            reader=reader,
        )

        self.assertEqual(
            usage.read(),
            {
                "rds-binlog-insight": 1234,
                "rds-binlog-insight-indexer": 5678,
            },
        )

    def test_prefix_auto_discovers_the_whole_application(self) -> None:
        run = Mock(
            return_value=(
                "rds-binlog-insight\n"
                "rds-binlog-insight-indexer\n"
                "rds-binlog-insight-clickhouse-poc"
            )
        )
        reader = Mock()
        reader.read.side_effect = [
            "usage_usec 1",
            "usage_usec 2",
            "usage_usec 3",
        ]
        usage = DockerCpuUsage(
            [],
            prefix="rds-binlog-insight",
            run=run,
            reader=reader,
        )

        self.assertEqual(
            usage.read(),
            {
                "rds-binlog-insight": 1,
                "rds-binlog-insight-clickhouse-poc": 2,
                "rds-binlog-insight-indexer": 3,
            },
        )


class DockerCpuLimitTests(unittest.TestCase):
    def test_reconcile_releases_and_restores_only_when_needed(self) -> None:
        run = Mock()
        reader = Mock()
        reader.read.side_effect = ["200000 100000", "max 100000"]
        limit = DockerCpuLimit(
            "rds-binlog-insight", run=run, reader=reader
        )

        self.assertTrue(limit.reconcile(NO_CPU_LIMIT))
        self.assertTrue(limit.reconcile(LIMITED_CPU_LIMIT))

        self.assertEqual(
            run.call_args_list[0].args[0],
            [
                "docker",
                "update",
                "--cpu-quota",
                "-1",
                "rds-binlog-insight",
            ],
        )
        self.assertEqual(
            run.call_args_list[1].args[0],
            ["docker", "update", "--cpus", "2.0", "rds-binlog-insight"],
        )

    def test_reconcile_is_idempotent(self) -> None:
        run = Mock()
        reader = Mock()
        reader.read.return_value = "200000 100000"
        limit = DockerCpuLimit(
            "rds-binlog-insight", run=run, reader=reader
        )

        self.assertFalse(limit.reconcile(LIMITED_CPU_LIMIT))
        run.assert_not_called()


class CpuGovernorServiceTests(unittest.TestCase):
    def test_probe_supports_the_host_python_310_runtime(self) -> None:
        source = (
            Path(__file__).resolve().parents[1] / "deploy" / "cpu_governor.py"
        ).read_text(encoding="utf-8")

        self.assertNotIn("from datetime import UTC", source)
        self.assertIn("timezone.utc", source)

    def test_service_is_restart_safe_and_fails_back_to_two_cpus(self) -> None:
        unit = (
            Path(__file__).resolve().parents[1]
            / "deploy"
            / "rds-binlog-insight-cpu-governor.service"
        ).read_text(encoding="utf-8")

        self.assertIn("User=yy", unit)
        self.assertIn("Restart=always", unit)
        self.assertIn("--threshold 40", unit)
        self.assertIn("--exclude-prefix rds-binlog-insight", unit)
        self.assertIn("--window-samples 3", unit)
        self.assertIn("--minimum-state-seconds 30", unit)
        self.assertIn(
            "ExecStopPost=-/usr/bin/docker update --cpus 2.0 rds-binlog-insight",
            unit,
        )

    def test_default_probe_excludes_the_whole_application(self) -> None:
        args = parse_args([])

        self.assertEqual(args.threshold, 40.0)
        self.assertEqual(args.exclude_containers, [])
        self.assertEqual(args.exclude_prefix, "rds-binlog-insight")


if __name__ == "__main__":
    unittest.main()
