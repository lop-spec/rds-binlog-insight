from __future__ import annotations

import logging
import os
from pathlib import Path


LOGGER = logging.getLogger(__name__)
DEFAULT_PRESSURE_PATH = Path("/proc/pressure/io")


class IoPressurePaused(RuntimeError):
    """A background task must yield until host I/O pressure recovers."""


def _bounded_float(value: object, default: float) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        parsed = float(default)
    return min(max(parsed, 0.0), 100.0)


def io_limit_from_env(name: str, default: float) -> float:
    return _bounded_float(os.environ.get(name), default)


def io_recovery_ratio_from_env(name: str, default: float = 0.5) -> float:
    return min(_bounded_float(os.environ.get(name), default), 1.0)


def read_full_avg10(path: Path = DEFAULT_PRESSURE_PATH) -> float:
    try:
        lines = Path(path).read_text(encoding="utf-8").splitlines()
    except FileNotFoundError:
        return 0.0
    except OSError as exc:
        LOGGER.warning("Unable to read host I/O pressure from %s: %s", path, exc)
        return 0.0
    for line in lines:
        fields = line.split()
        if not fields or fields[0] != "full":
            continue
        values = {
            key: value
            for field in fields[1:]
            if "=" in field
            for key, value in (field.split("=", 1),)
        }
        try:
            return float(values["avg10"])
        except (KeyError, ValueError):
            break
    LOGGER.warning("Host I/O pressure file has no full avg10 value: %s", path)
    return 0.0


def probe_io_pressure(
    limit: float,
    *,
    path: Path = DEFAULT_PRESSURE_PATH,
) -> float:
    ceiling = _bounded_float(limit, 0.0)
    if ceiling <= 0:
        return 0.0
    current = read_full_avg10(path)
    if current > ceiling:
        raise IoPressurePaused(
            "host I/O pressure exceeded safety ceiling: "
            f"full avg10={current:.2f}% > {ceiling:.2f}%"
        )
    return current


def require_io_recovery(
    limit: float,
    pressure: float,
    *,
    paused: bool,
    recovery_ratio: float = 0.5,
) -> None:
    ceiling = _bounded_float(limit, 0.0)
    if not paused or ceiling <= 0:
        return
    recovery_ceiling = ceiling * min(max(float(recovery_ratio), 0.0), 1.0)
    if float(pressure) > recovery_ceiling:
        raise IoPressurePaused(
            "host I/O pressure has not reached ingest recovery ceiling: "
            f"full avg10={float(pressure):.2f}% > {recovery_ceiling:.2f}%"
        )


class IoPressureGate:
    """Stateful PSI gate with hysteresis for low-priority background work."""

    def __init__(
        self,
        *,
        limit: float,
        recovery_ratio: float = 0.5,
        path: Path = DEFAULT_PRESSURE_PATH,
    ) -> None:
        self.limit = _bounded_float(limit, 0.0)
        self.recovery_ratio = min(max(float(recovery_ratio), 0.0), 1.0)
        self.path = Path(path)
        self.paused = False

    @classmethod
    def from_env(
        cls,
        name: str,
        *,
        default: float,
        recovery_ratio: float = 0.5,
        path: Path = DEFAULT_PRESSURE_PATH,
    ) -> "IoPressureGate":
        return cls(
            limit=io_limit_from_env(name, default),
            recovery_ratio=recovery_ratio,
            path=path,
        )

    def check(self) -> float:
        try:
            pressure = probe_io_pressure(self.limit, path=self.path)
            require_io_recovery(
                self.limit,
                pressure,
                paused=self.paused,
                recovery_ratio=self.recovery_ratio,
            )
        except IoPressurePaused:
            self.paused = True
            raise
        self.paused = False
        return pressure
