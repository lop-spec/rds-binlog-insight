#!/usr/bin/env python3
"""Create verified online backups of selected SQLite databases.

The source databases are opened read-only and copied through SQLite's backup
API, so committed WAL content is included without stopping the live service.
Existing destinations are never overwritten.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sqlite3
import sys
import time
from contextlib import closing
from pathlib import Path


class BackupPaused(RuntimeError):
    """The backup stopped at a safe page boundary."""


FULL_AVG10 = re.compile(r"^full\s+.*\bavg10=([0-9]+(?:\.[0-9]+)?)\b")


def _relative_database(value: str) -> Path:
    relative = Path(value)
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise argparse.ArgumentTypeError(
            "database must be a relative path without '..'"
        )
    return relative


def _readonly_uri(path: Path) -> str:
    return path.resolve().as_uri() + "?mode=ro"


def _check_io_pressure(path: Path, limit: float) -> float | None:
    if limit <= 0 or not path.is_file():
        return None
    for line in path.read_text(encoding="utf-8").splitlines():
        match = FULL_AVG10.search(line)
        if not match:
            continue
        value = float(match.group(1))
        if value > limit:
            raise BackupPaused(
                f"I/O pressure full avg10 {value:.2f} exceeds {limit:.2f}"
            )
        return value
    raise RuntimeError(f"I/O pressure file has no full avg10 value: {path}")


def backup_database(
    source: Path,
    destination: Path,
    *,
    io_pressure_path: Path,
    io_pressure_limit: float,
    step_pages: int,
    step_sleep_seconds: float,
) -> dict[str, object]:
    if not source.is_file():
        raise FileNotFoundError(f"source database does not exist: {source}")
    if destination.exists():
        raise FileExistsError(f"refusing to overwrite {destination}")

    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = destination.with_name(
        f".{destination.name}.partial-{os.getpid()}"
    )
    if temporary.exists():
        raise FileExistsError(f"refusing to overwrite {temporary}")

    started = time.perf_counter()
    _check_io_pressure(io_pressure_path, io_pressure_limit)
    source_connection: sqlite3.Connection | None = None
    target_connection: sqlite3.Connection | None = None
    try:
        source_connection = sqlite3.connect(
            _readonly_uri(source), uri=True, timeout=60
        )
        target_connection = sqlite3.connect(temporary, timeout=60)
        def progress(_status: int, remaining: int, _total: int) -> None:
            _check_io_pressure(io_pressure_path, io_pressure_limit)
            if remaining > 0 and step_sleep_seconds > 0:
                time.sleep(step_sleep_seconds)

        source_connection.backup(
            target_connection,
            pages=max(int(step_pages), 1),
            progress=progress,
            sleep=0.05,
        )
        target_connection.close()
        target_connection = None
        source_connection.close()
        source_connection = None

        with closing(
            sqlite3.connect(_readonly_uri(temporary), uri=True, timeout=60)
        ) as verification:
            quick_check = str(
                verification.execute("PRAGMA quick_check").fetchone()[0]
            )
            page_count = int(
                verification.execute("PRAGMA page_count").fetchone()[0]
            )
            page_size = int(
                verification.execute("PRAGMA page_size").fetchone()[0]
            )
        if quick_check != "ok":
            raise RuntimeError(
                f"quick_check failed for {destination}: {quick_check}"
            )

        os.chmod(temporary, 0o600)
        os.replace(temporary, destination)
        return {
            "source": str(source),
            "destination": str(destination),
            "bytes": destination.stat().st_size,
            "page_count": page_count,
            "page_size": page_size,
            "quick_check": quick_check,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
        }
    except BaseException:
        if target_connection is not None:
            target_connection.close()
        if source_connection is not None:
            source_connection.close()
        temporary.unlink(missing_ok=True)
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Create verified online backups of SQLite databases."
    )
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--destination-root", type=Path, required=True)
    parser.add_argument(
        "--database",
        type=_relative_database,
        action="append",
        required=True,
        help="Relative database path; repeat for each database.",
    )
    parser.add_argument(
        "--io-pressure-file",
        type=Path,
        default=Path("/proc/pressure/io"),
    )
    parser.add_argument("--io-full-avg10-max", type=float, default=10.0)
    parser.add_argument("--step-pages", type=int, default=128)
    parser.add_argument("--step-sleep-seconds", type=float, default=0.1)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.step_pages < 1 or args.step_sleep_seconds < 0:
        print("step-pages must be positive and step-sleep-seconds non-negative", file=sys.stderr)
        return 1
    source_root = args.source_root.resolve()
    destination_root = args.destination_root.resolve()
    destination_root.mkdir(parents=True, exist_ok=True, mode=0o700)

    def request_stop(signum: int, _frame: object) -> None:
        raise BackupPaused(f"backup interrupted by signal {signum}")

    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)
    try:
        for relative in args.database:
            result = backup_database(
                source_root / relative,
                destination_root / relative,
                io_pressure_path=args.io_pressure_file,
                io_pressure_limit=float(args.io_full_avg10_max),
                step_pages=int(args.step_pages),
                step_sleep_seconds=float(args.step_sleep_seconds),
            )
            print(json.dumps(result, separators=(",", ":")), flush=True)
    except BackupPaused as exc:
        print(f"BackupPaused: {exc}", file=sys.stderr, flush=True)
        return 2
    except Exception as exc:
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
