from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Callable, Sequence


NO_CPU_LIMIT = 0
LIMITED_CPU_LIMIT = 2_000_000_000


@dataclass(frozen=True)
class CpuTimes:
    total: int
    idle: int


def read_cpu_times(path: Path = Path("/proc/stat")) -> CpuTimes:
    fields = path.read_text(encoding="ascii").splitlines()[0].split()
    if not fields or fields[0] != "cpu" or len(fields) < 9:
        raise RuntimeError(f"unexpected CPU counters in {path}")
    values = [int(value) for value in fields[1:]]
    idle = values[3] + values[4]
    non_idle = sum(values[index] for index in (0, 1, 2, 5, 6, 7))
    return CpuTimes(total=idle + non_idle, idle=idle)


def cpu_usage_percent(before: CpuTimes, after: CpuTimes) -> float:
    total_delta = after.total - before.total
    idle_delta = after.idle - before.idle
    if total_delta <= 0 or idle_delta < 0:
        raise RuntimeError("host CPU counters did not advance monotonically")
    busy = max(0, min(total_delta, total_delta - idle_delta))
    return round(busy * 100.0 / total_delta, 3)


def external_cpu_usage_percent(
    before: CpuTimes,
    after: CpuTimes,
    before_managed_usage_usec: dict[str, int],
    after_managed_usage_usec: dict[str, int],
    *,
    clock_ticks: int | None = None,
) -> float:
    total_delta = after.total - before.total
    idle_delta = after.idle - before.idle
    if total_delta <= 0 or idle_delta < 0:
        raise RuntimeError("host CPU counters did not advance monotonically")
    ticks = int(clock_ticks or os.sysconf("SC_CLK_TCK"))
    if ticks <= 0:
        raise RuntimeError("host clock tick rate must be positive")
    managed_ticks = 0.0
    stable_containers = (
        before_managed_usage_usec.keys() & after_managed_usage_usec.keys()
    )
    for container in stable_containers:
        before_usec = before_managed_usage_usec[container]
        after_usec = after_managed_usage_usec[container]
        # A recreated container starts a new cgroup counter. Treat that one
        # interval as zero excluded CPU, which is conservative and recovers on
        # the next sample instead of permanently pinning the service to 1 CPU.
        delta_usec = max(0, after_usec - before_usec)
        managed_ticks += delta_usec * ticks / 1_000_000

    host_busy_ticks = total_delta - idle_delta
    external_busy_ticks = max(0.0, host_busy_ticks - managed_ticks)
    external_busy_ticks = min(float(total_delta), external_busy_ticks)
    return round(external_busy_ticks * 100.0 / total_delta, 3)


def target_limit(samples: Sequence[float], threshold: float) -> int:
    if not samples:
        raise ValueError("at least one CPU sample is required")
    average = sum(samples) / len(samples)
    return LIMITED_CPU_LIMIT if average >= threshold else NO_CPU_LIMIT


def limit_after_probe_error(active_limit: int) -> int:
    """Observation failure must not make an already-busy service weaker."""

    if active_limit not in (NO_CPU_LIMIT, LIMITED_CPU_LIMIT):
        raise ValueError(f"unsupported active CPU limit: {active_limit}")
    return active_limit


def run_command(argv: Sequence[str]) -> str:
    completed = subprocess.run(
        list(argv),
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=10,
    )
    if completed.returncode != 0:
        output = completed.stdout.strip()
        raise RuntimeError(
            f"command failed ({completed.returncode}): {list(argv)!r}: {output}"
        )
    return completed.stdout.strip()


class DockerCgroupReader:
    """Read cgroup v2 metrics from the host, never via docker exec."""

    def __init__(
        self,
        *,
        run: Callable[[Sequence[str]], str] = run_command,
    ) -> None:
        self.run = run
        self._paths: dict[str, PurePosixPath] = {}

    def _resolve(self, container: str) -> PurePosixPath:
        pid_text = self.run(
            ["docker", "inspect", "--format", "{{.State.Pid}}", container]
        ).strip()
        try:
            pid = int(pid_text)
        except ValueError as exc:
            raise RuntimeError(
                f"unexpected container PID for {container}: {pid_text!r}"
            ) from exc
        if pid <= 0:
            raise RuntimeError(f"container is not running: {container}")
        membership = self.run(["cat", f"/proc/{pid}/cgroup"])
        relative = ""
        for line in membership.splitlines():
            fields = line.split(":", 2)
            if len(fields) == 3 and fields[0] == "0" and fields[1] == "":
                relative = fields[2]
                break
        if not relative.startswith("/") or ".." in PurePosixPath(relative).parts:
            raise RuntimeError(
                f"unexpected cgroup v2 membership for {container}: "
                f"{membership!r}"
            )
        path = PurePosixPath("/sys/fs/cgroup") / relative.lstrip("/")
        self._paths[container] = path
        return path

    def read(self, container: str, filename: str) -> str:
        if filename not in {"cpu.max", "cpu.stat"}:
            raise ValueError(f"unsupported cgroup file: {filename}")
        for attempt in range(2):
            path = self._paths.get(container) or self._resolve(container)
            try:
                return self.run(["cat", str(path / filename)])
            except RuntimeError:
                self._paths.pop(container, None)
                if attempt:
                    raise
        raise AssertionError("cgroup read retry did not return")


class DockerCpuLimit:
    def __init__(
        self,
        container: str,
        *,
        run: Callable[[Sequence[str]], str] = run_command,
        reader: DockerCgroupReader | None = None,
    ) -> None:
        self.container = container
        self.run = run
        self.reader = reader or DockerCgroupReader(run=run)

    def current(self) -> int:
        value = self.reader.read(self.container, "cpu.max")
        fields = value.split()
        if len(fields) != 2:
            raise RuntimeError(f"unexpected cgroup v2 cpu.max value: {value!r}")
        quota, period = fields
        if quota == "max":
            return NO_CPU_LIMIT
        period_value = int(period)
        if period_value <= 0:
            raise RuntimeError(f"unexpected cgroup CPU period: {period!r}")
        return round(int(quota) * 1_000_000_000 / period_value)

    def reconcile(self, desired: int) -> bool:
        if desired not in (NO_CPU_LIMIT, LIMITED_CPU_LIMIT):
            raise ValueError(f"unsupported NanoCpus target: {desired}")
        if self.current() == desired:
            return False
        if desired == NO_CPU_LIMIT:
            self.run(
                [
                    "docker",
                    "update",
                    "--cpu-quota",
                    "-1",
                    self.container,
                ]
            )
        else:
            self.run(
                ["docker", "update", "--cpus", "2.0", self.container]
            )
        return True


class DockerCpuUsage:
    def __init__(
        self,
        containers: Sequence[str],
        *,
        prefix: str = "",
        run: Callable[[Sequence[str]], str] = run_command,
        reader: DockerCgroupReader | None = None,
    ) -> None:
        self.containers = tuple(dict.fromkeys(containers))
        self.prefix = str(prefix or "").strip()
        if not self.containers and not self.prefix:
            raise ValueError("an excluded container or prefix is required")
        self.run = run
        self.reader = reader or DockerCgroupReader(run=run)

    def _running_containers(self) -> list[str]:
        containers = set(self.containers)
        if self.prefix:
            output = self.run(
                [
                    "docker",
                    "ps",
                    "--filter",
                    f"name=^/{self.prefix}",
                    "--format",
                    "{{.Names}}",
                ]
            )
            containers.update(
                name
                for raw_name in output.splitlines()
                if (name := raw_name.strip()).startswith(self.prefix)
            )
        return sorted(containers)

    def read(self) -> dict[str, int]:
        usage: dict[str, int] = {}
        for container in self._running_containers():
            value = self.reader.read(container, "cpu.stat")
            fields = {
                key: raw_value
                for line in value.splitlines()
                if len(parts := line.split(maxsplit=1)) == 2
                for key, raw_value in [parts]
            }
            try:
                usage[container] = int(fields["usage_usec"])
            except (KeyError, ValueError) as exc:
                raise RuntimeError(
                    f"unexpected cgroup v2 cpu.stat value for {container}: {value!r}"
                ) from exc
        return usage


def emit(event: str, **values: object) -> None:
    print(
        json.dumps(
            {
                "time": datetime.now(timezone.utc).isoformat(),
                "event": event,
                **values,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        flush=True,
    )


def run_governor(args: argparse.Namespace) -> None:
    stopping = False

    def request_stop(_signum: int, _frame: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    cgroups = DockerCgroupReader()
    limiter = DockerCpuLimit(args.container, reader=cgroups)
    usage_reader = DockerCpuUsage(
        [args.container, *args.exclude_containers],
        prefix=args.exclude_prefix,
        reader=cgroups,
    )
    samples: deque[float] = deque(maxlen=args.window_samples)
    active_limit = LIMITED_CPU_LIMIT
    last_change = time.monotonic()
    previous = read_cpu_times()
    previous_managed_usage = usage_reader.read()
    last_error = ""

    try:
        try:
            limiter.reconcile(LIMITED_CPU_LIMIT)
        except Exception as exc:
            last_error = str(exc)
            emit("fail_safe_pending", error=last_error)
        emit(
            "started",
            container=args.container,
            excludedContainers=list(args.exclude_containers),
            excludedPrefix=args.exclude_prefix,
            threshold=args.threshold,
            intervalSeconds=args.interval_seconds,
            windowSamples=args.window_samples,
            minimumStateSeconds=args.minimum_state_seconds,
        )

        while not stopping:
            time.sleep(args.interval_seconds)
            if stopping:
                break
            try:
                current = read_cpu_times()
                current_managed_usage = usage_reader.read()
                usage = external_cpu_usage_percent(
                    previous,
                    current,
                    previous_managed_usage,
                    current_managed_usage,
                )
                previous = current
                previous_managed_usage = current_managed_usage
                samples.append(usage)

                desired = active_limit
                average = sum(samples) / len(samples)
                if len(samples) == args.window_samples:
                    desired = target_limit(samples, args.threshold)

                now = time.monotonic()
                state_may_change = (
                    desired == active_limit
                    or now - last_change >= args.minimum_state_seconds
                )
                target = desired if state_may_change else active_limit
                changed = limiter.reconcile(target)
                if target != active_limit:
                    active_limit = target
                    last_change = now
                if changed:
                    emit(
                        "limit_changed",
                        mode=(
                            "unlimited" if target == NO_CPU_LIMIT else "two_cpu"
                        ),
                        averageExternalCpuPercent=round(average, 3),
                        samples=list(samples),
                    )
                last_error = ""
            except Exception as exc:
                error = str(exc)
                samples.clear()
                active_limit = limit_after_probe_error(active_limit)
                if error != last_error:
                    emit("probe_error", error=error)
                    last_error = error
    finally:
        try:
            limiter.reconcile(LIMITED_CPU_LIMIT)
            emit("stopped", mode="two_cpu")
        except Exception as exc:
            emit("stop_fail_safe_failed", error=str(exc))


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--container", default="rds-binlog-insight")
    parser.add_argument("--threshold", type=float, default=40.0)
    parser.add_argument(
        "--exclude-container",
        action="append",
        dest="exclude_containers",
        default=[],
    )
    parser.add_argument(
        "--exclude-prefix",
        default="rds-binlog-insight",
    )
    parser.add_argument("--interval-seconds", type=float, default=5.0)
    parser.add_argument("--window-samples", type=int, default=3)
    parser.add_argument("--minimum-state-seconds", type=float, default=30.0)
    args = parser.parse_args(argv)
    args.exclude_prefix = str(args.exclude_prefix or "").strip()
    if not 0 < args.threshold <= 100:
        parser.error("--threshold must be in (0, 100]")
    if args.interval_seconds <= 0:
        parser.error("--interval-seconds must be positive")
    if args.window_samples < 1:
        parser.error("--window-samples must be at least 1")
    if args.minimum_state_seconds < 0:
        parser.error("--minimum-state-seconds cannot be negative")
    return args


if __name__ == "__main__":
    run_governor(parse_args())
