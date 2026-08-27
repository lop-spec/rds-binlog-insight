from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path


LOGGER = logging.getLogger(__name__)


def _positive_env_float(name: str, default: float, minimum: float) -> float:
    try:
        return max(float(os.environ.get(name, default)), minimum)
    except ValueError:
        return default


def _positive_env_int(name: str, default: int, minimum: int) -> int:
    try:
        return max(int(os.environ.get(name, default)), minimum)
    except ValueError:
        return default


def _hidden_process_options() -> tuple[int, subprocess.STARTUPINFO | None]:
    creation_flags = 0
    startupinfo = None
    if os.name == "nt":
        creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = 0
    return creation_flags, startupinfo


class ServiceSupervisor:
    """Keep the HTTP/sync process recoverable from native or GIL stalls."""

    def __init__(
        self,
        command: list[str],
        health_url: str,
        *,
        probe_interval: float,
        probe_timeout: float,
        failure_limit: int,
        startup_grace: float,
    ) -> None:
        self.command = command
        self.health_url = health_url
        self.probe_interval = max(float(probe_interval), 0.2)
        self.probe_timeout = max(float(probe_timeout), 0.1)
        self.failure_limit = max(int(failure_limit), 1)
        self.startup_grace = max(float(startup_grace), 0.0)
        self.stopping = False
        self.child: subprocess.Popen[bytes] | None = None
        self.restart_count = 0
        self.last_health_error = ""
        self.last_health_seconds = 0.0

    def request_stop(self, *_args: object) -> None:
        self.stopping = True

    def _spawn(self) -> subprocess.Popen[bytes]:
        creation_flags, startupinfo = _hidden_process_options()
        child = subprocess.Popen(
            self.command,
            stdin=subprocess.DEVNULL,
            creationflags=creation_flags,
            startupinfo=startupinfo,
            start_new_session=os.name != "nt",
        )
        self.child = child
        LOGGER.info("Started service child pid=%s", child.pid)
        return child

    def _health_ok(self) -> bool:
        started = time.monotonic()
        try:
            with urllib.request.urlopen(
                self.health_url,
                timeout=self.probe_timeout,
            ) as response:
                body = response.read(4096)
                healthy = response.status == 200 and bool(body)
                self.last_health_error = (
                    ""
                    if healthy
                    else f"status={response.status} bodyBytes={len(body)}"
                )
                return healthy
        except (OSError, TimeoutError) as exc:
            self.last_health_error = f"{type(exc).__name__}: {exc}"
            return False
        finally:
            self.last_health_seconds = time.monotonic() - started

    def _request_stack_dump(self, child: subprocess.Popen[bytes]) -> None:
        diagnostic_signal = getattr(signal, "SIGUSR1", None)
        if diagnostic_signal is None or child.poll() is not None:
            return
        try:
            os.kill(child.pid, diagnostic_signal)
        except OSError:
            return

    def _terminate_child(self) -> None:
        child = self.child
        if child is None or child.poll() is not None:
            return
        try:
            if os.name == "nt":
                child.terminate()
            else:
                os.killpg(child.pid, signal.SIGTERM)
            child.wait(timeout=12)
            return
        except (OSError, subprocess.TimeoutExpired):
            pass
        try:
            if os.name == "nt":
                child.kill()
            else:
                os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=5)
        except (OSError, subprocess.TimeoutExpired):
            LOGGER.exception("Failed to stop service child pid=%s", child.pid)

    def _sleep_interruptibly(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while not self.stopping and time.monotonic() < deadline:
            time.sleep(min(0.2, max(deadline - time.monotonic(), 0.0)))

    def run(self) -> int:
        try:
            while not self.stopping:
                child = self._spawn()
                started = time.monotonic()
                became_healthy = False
                failures = 0
                restart_for_health = False
                while not self.stopping and child.poll() is None:
                    if self._health_ok():
                        became_healthy = True
                        failures = 0
                    elif became_healthy or time.monotonic() - started >= self.startup_grace:
                        failures += 1
                        LOGGER.warning(
                            "Service health probe failed pid=%s failures=%s/%s "
                            "elapsed=%.3fs reason=%s",
                            child.pid,
                            failures,
                            self.failure_limit,
                            self.last_health_seconds,
                            self.last_health_error or "invalid-response",
                        )
                        if failures >= self.failure_limit:
                            restart_for_health = True
                            self._request_stack_dump(child)
                            self._sleep_interruptibly(0.5)
                            self._terminate_child()
                            break
                    self._sleep_interruptibly(self.probe_interval)
                if self.stopping:
                    break
                exit_code = child.poll()
                self.restart_count += 1
                LOGGER.error(
                    "Restarting service child exit=%s healthFailure=%s restarts=%s",
                    exit_code,
                    restart_for_health,
                    self.restart_count,
                )
                self.child = None
                self._sleep_interruptibly(min(1.0 + self.restart_count, 5.0))
        finally:
            self._terminate_child()
            self.child = None
        return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", choices=("127.0.0.1", "0.0.0.0"), default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8769)
    parser.add_argument("--data-dir", type=Path)
    args = parser.parse_args()
    command = [
        sys.executable,
        "-m",
        "app.server",
        "--host",
        args.host,
        "--port",
        str(args.port),
    ]
    if args.data_dir is not None:
        command.extend(("--data-dir", str(args.data_dir.resolve())))
    supervisor = ServiceSupervisor(
        command,
        f"http://127.0.0.1:{args.port}/healthz",
        probe_interval=_positive_env_float(
            "RDS_BINLOG_SERVICE_PROBE_INTERVAL", 5.0, 0.2
        ),
        probe_timeout=_positive_env_float(
            "RDS_BINLOG_SERVICE_PROBE_TIMEOUT", 2.0, 0.1
        ),
        failure_limit=_positive_env_int(
            "RDS_BINLOG_SERVICE_FAILURE_LIMIT", 3, 1
        ),
        startup_grace=_positive_env_float(
            "RDS_BINLOG_SERVICE_STARTUP_GRACE", 30.0, 0.0
        ),
    )
    signal.signal(signal.SIGTERM, supervisor.request_stop)
    signal.signal(signal.SIGINT, supervisor.request_stop)
    return supervisor.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    raise SystemExit(main())
