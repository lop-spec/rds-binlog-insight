from __future__ import annotations

import argparse
import logging
import os
import signal
import threading
import time
import traceback
import uuid
from pathlib import Path
from typing import Any

from .clickhouse_client import ClickHouseConfig
from .clickhouse_ingest import HealthCanary
from .config import data_root, ensure_data_dirs
from .credentials import load_credential
from .index_worker import _advance_slowlog_index, _bound_arrow
from .io_pressure import (
    IoPressureGate,
    IoPressurePaused,
    io_recovery_ratio_from_env,
)
from .maintenance_status import SLOWLOG_WORKER_STATUS_NAME, write_json_status
from .metadata import MetadataStore
from .oss_store import OssArchive
from .storage import EventStorage


LOGGER = logging.getLogger(__name__)


def drain_slowlog_queue_once(
    metadata: MetadataStore,
    storage: EventStorage,
    archive: OssArchive | None,
    *,
    publish: Any | None = None,
    generation: str = "slowlog-worker",
    admission_check: Any | None = None,
) -> dict[str, Any]:
    reporter = publish or (lambda *_args, **_kwargs: None)
    return _advance_slowlog_index(
        metadata,
        storage,
        archive,
        publish=reporter,
        generation=generation,
        admission_check=admission_check,
    )


class SlowLogQueueWorker:
    def __init__(self, data_dir: Path, *, idle_seconds: float = 2.0) -> None:
        self.data_dir = data_dir
        self.idle_seconds = max(float(idle_seconds), 0.2)
        self.stopping = threading.Event()

    def request_stop(self, *_args: object) -> None:
        self.stopping.set()

    def run(self) -> int:
        _bound_arrow()
        paths = ensure_data_dirs(self.data_dir)
        status_path = paths["index"] / SLOWLOG_WORKER_STATUS_NAME
        metadata = MetadataStore(
            self.data_dir / "metadata.sqlite3",
            run_migrations=False,
        )
        storage = EventStorage(metadata, self.data_dir)
        settings = metadata.load_settings()
        archive: OssArchive | None = None
        if settings.oss_enabled:
            archive = OssArchive(
                settings,
                credential=load_credential(settings.credential_target),
            )
        generation = uuid.uuid4().hex
        last_error = ""
        io_gate = IoPressureGate.from_env(
            "RDS_BINLOG_SLOWLOG_IO_FULL_AVG10_MAX",
            default=10.0,
            recovery_ratio=io_recovery_ratio_from_env(
                "RDS_BINLOG_SLOWLOG_IO_RECOVERY_RATIO",
                0.5,
            ),
        )
        health_canary = HealthCanary(ClickHouseConfig.from_env())
        io_pressure_override_active = False

        # The source part and the dedicated queue live in separate SQLite
        # databases, so a SIGKILL can land after the source commit but before
        # enqueue.  Force a bounded reconciliation on every worker start;
        # subsequent idle sweeps keep the same gap bounded by the configured
        # refresh interval.
        storage.slowlog_index.advance_reconcile(after_path="", complete=False)

        def publish(
            state: str,
            *,
            phase: str = "",
            part: dict[str, Any] | None = None,
            token: str = "",
            error: str = "",
            result: dict[str, Any] | None = None,
        ) -> None:
            nonlocal last_error
            if error:
                last_error = error
            write_json_status(
                status_path,
                {
                    "running": not self.stopping.is_set(),
                    "state": state,
                    "phase": phase or "slowlog",
                    "generation": generation,
                    "pid": os.getpid(),
                    "currentPart": Path(str((part or {}).get("path") or "")).name,
                    "progressToken": token,
                    "lastError": last_error,
                    "result": result or {},
                    "stats": storage.slowlog_index.stats(),
                },
            )

        publish("starting", token=f"{generation}:starting")
        try:
            while not self.stopping.is_set():
                try:
                    io_pressure_canary_override = False
                    try:
                        io_gate.check()
                    except IoPressurePaused as pressure_exc:
                        # Host PSI includes time spent behind every container's
                        # cgroup throttle.  A deliberately rate-limited live
                        # collector can therefore keep system full avg10 high
                        # even while the physical device and serving path are
                        # healthy.  The source queue is already bounded to one
                        # part and a small I/O cgroup, so use the real serving
                        # canary as the deciding signal in that case.  A failed
                        # canary still raises IoPressurePaused and stops work.
                        health_canary.probe(force=True)
                        io_pressure_canary_override = True
                        if not io_pressure_override_active:
                            LOGGER.warning(
                                "Host I/O PSI is above the source-worker "
                                "ceiling but the serving canary is healthy; "
                                "allowing one bounded slow-log part: %s",
                                pressure_exc,
                            )
                        io_pressure_override_active = True
                    else:
                        if io_pressure_override_active:
                            LOGGER.info(
                                "Host I/O PSI recovered below the "
                                "source-worker ceiling"
                            )
                        io_pressure_override_active = False
                        # One cached preflight per loop keeps an idle worker
                        # cheap when the PSI gate is already healthy.
                        health_canary.probe()
                    # The forced check passed into the drain runs immediately
                    # before a Parquet build, after any reconciliation page.
                    totals = drain_slowlog_queue_once(
                        metadata,
                        storage,
                        archive,
                        publish=publish,
                        generation=generation,
                        admission_check=lambda: health_canary.probe(force=True),
                    )
                    if totals["parts"] or totals.get("reconcileAdvanced"):
                        # A fresh postflight observes the actual impact of this
                        # single bounded unit instead of trusting a stale probe.
                        health_canary.probe(force=True)
                    last_error = ""
                    publish(
                        "idle" if not totals["parts"] else "completed",
                        token=f"{generation}:idle:{time.time_ns()}",
                        result={
                            **totals,
                            "ioPressureCanaryOverride": (
                                io_pressure_canary_override
                            ),
                        },
                    )
                    # full avg10 is a ten-second moving signal.  Pace even a
                    # successful part at the configured interval so several
                    # fast parts cannot outrun the next PSI admission check.
                    if io_pressure_canary_override:
                        delay = max(
                            self.idle_seconds,
                            1.0 if totals["parts"] else 5.0,
                        )
                    else:
                        delay = self.idle_seconds
                except IoPressurePaused as exc:
                    last_error = str(exc)
                    publish(
                        "paused",
                        phase="safety-fuse",
                        token=f"{generation}:io-pressure:{time.time_ns()}",
                        error=last_error,
                    )
                    delay = max(self.idle_seconds, 5.0)
                except Exception as exc:  # durable queue owns retries
                    traceback.print_exc()
                    last_error = f"{type(exc).__name__}: {exc}"
                    publish(
                        "error",
                        token=f"{generation}:error:{time.time_ns()}",
                        error=last_error,
                    )
                    delay = min(self.idle_seconds, 5.0)
                self.stopping.wait(delay)
        finally:
            publish("stopped", token=f"{generation}:stopped")
        return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=data_root())
    parser.add_argument("--idle-seconds", type=float, default=2.0)
    args = parser.parse_args()
    worker = SlowLogQueueWorker(args.data_dir.resolve(), idle_seconds=args.idle_seconds)
    signal.signal(signal.SIGTERM, worker.request_stop)
    signal.signal(signal.SIGINT, worker.request_stop)
    return worker.run()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    raise SystemExit(main())
