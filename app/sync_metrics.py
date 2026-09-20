"""Bounded wall-clock observations, not a throughput benchmark or coverage proof."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from math import isfinite
from statistics import median
from typing import Any

WINDOW_SECONDS = 2 * 3600
SAMPLE_LIMIT = 4096


def utc_datetime(value: str | datetime) -> datetime | None:
    try:
        value = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    except (ValueError, TypeError, OverflowError):
        return None


def source_kind(row: dict[str, Any]) -> str:
    name = str(row.get("log_file_name") or "")
    if name.startswith("slow-log/") or row.get("host_instance_id") == "slow-log":
        return "slowlog"
    if name.startswith("tabularis-audit-"):
        return "audit"
    return "binlog"


def estimate_sync_performance(
    *, completion_rows: list[dict[str, Any]], source_rows: list[dict[str, Any]],
    known_remaining_files: int, running: bool, active_files: int = 0,
    workload_ready: bool = True, failed_files: int = 0,
    host_instance_id: str | None = None, now: datetime | None = None,
    completion_truncated: bool = False, source_truncated: bool = False,
) -> dict[str, Any]:
    current = utc_datetime(now or datetime.now(UTC))
    if current is None:
        raise ValueError("invalid observation clock")
    start = current - timedelta(seconds=WINDOW_SECONDS)
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    invalid_completion_times = invalid_source_times = 0

    def group(row):
        key = (source_kind(row), str(row.get("host_instance_id") or ""))
        return groups.setdefault(key, {
            "source": key[0], "host_instance_id": key[1],
            "completed_files": 0, "completed_bytes": 0,
            "observed_source_files": 0, "observed_source_bytes": 0,
            "durations": [],
        })

    for row in completion_rows:
        stamp = utc_datetime(row.get("completed_at") or "")
        if stamp is None:
            invalid_completion_times += 1
            continue
        if not start < stamp <= current:
            continue
        item = group(row)
        item["completed_files"] += 1
        item["completed_bytes"] += max(0, int(row.get("file_size") or 0))
        try:
            duration = float(row.get("processing_seconds") or 0)
        except (TypeError, ValueError):
            duration = 0.0
        if isfinite(duration) and duration > 0:
            item["durations"].append(duration)

    for row in source_rows:
        if str(row.get("remote_status") or "").lower() != "completed":
            continue
        stamp = utc_datetime(row.get("log_end_utc") or "")
        if stamp is None:
            invalid_source_times += 1
            continue
        if not start < stamp <= current:
            continue
        item = group(row)
        item["observed_source_files"] += 1
        item["observed_source_bytes"] += max(0, int(row.get("file_size") or 0))

    completion_valid = not completion_truncated and not invalid_completion_times
    # Even a capped inventory is a valid observed lower bound, never an
    # estimate of the complete source generation rate.
    source_valid = not invalid_source_times
    for item in groups.values():
        durations = item.pop("durations")
        item["duration_sample_size"] = len(durations)
        item["seconds_per_file"] = round(median(durations), 3) if len(durations) >= 4 else None
        for prefix, count, size, valid in (
            ("processing", "completed_files", "completed_bytes", completion_valid),
            ("source", "observed_source_files", "observed_source_bytes", source_valid),
        ):
            item[prefix + "_files_per_hour"] = item[count] * 3600 / WINDOW_SECONDS if valid else None
            item[prefix + "_bytes_per_hour"] = item[size] * 3600 / WINDOW_SECONDS if valid else None

    # A single observed host is useful for displaying observations, but it is
    # not an authoritative assignment of the job's backlog to that host.
    observed_hosts = {host for (kind, host), item in groups.items()
                      if kind == "binlog" and item["completed_files"]}
    selected_host = host_instance_id or (next(iter(observed_hosts)) if len(observed_hosts) == 1 else None)
    selected = groups.get(("binlog", selected_host)) if selected_host is not None else None
    known_remaining = max(0, int(known_remaining_files))
    active_count = min(known_remaining, max(0, int(active_files)))
    failed_count = max(0, int(failed_files))
    result: dict[str, Any] = {
        "state": "warming_up", "window_start_utc": start.isoformat().replace("+00:00", "Z"),
        "window_end_utc": current.isoformat().replace("+00:00", "Z"), "window_seconds": WINDOW_SECONDS,
        "rate_basis": "recorded_done_per_wall_clock_window",
        "source_rate_basis": "observed_completed_inventory_by_log_end_utc",
        "source_inventory_complete": False,
        "source_rate_reason": "sample_limit_observed_lower_bound" if source_truncated else "invalid_timestamp" if invalid_source_times else "inventory_coverage_unproven",
        "processing_rate_reason": "sample_limit" if completion_truncated else "invalid_timestamp" if invalid_completion_times else "" if selected else "host_sample_unavailable",
        "host_instance_id": selected_host, "host_authoritative": bool(host_instance_id),
        "groups": sorted(groups.values(), key=lambda g: (g["source"], g["host_instance_id"])),
        "seconds_per_file": selected["seconds_per_file"] if selected else None,
        "completion_sample_size": selected["completed_files"] if selected else 0,
        "source_sample_size": selected["observed_source_files"] if selected else 0,
        "source_seconds_per_file": None,
        "processing_files_per_hour": selected["processing_files_per_hour"] if selected else None,
        "processing_bytes_per_hour": selected["processing_bytes_per_hour"] if selected else None,
        "source_files_per_hour": selected["source_files_per_hour"] if selected else None,
        "source_bytes_per_hour": selected["source_bytes_per_hour"] if selected else None,
        "known_remaining_files": known_remaining, "active_files": active_count,
        "queued_remaining_files": max(0, known_remaining - active_count), "failed_files": failed_count,
        "estimated_unseen_files": None, "estimated_backlog_files": float(known_remaining),
        "estimated_remaining_seconds": None, "estimated_catch_up_at_utc": "",
        "inventory_clear_at_utc": "", "inventory_remaining_seconds": None,
        "continuous_state": "unknown", "continuous_reason": "source_inventory_coverage_unproven",
        "estimated_net_remaining_seconds": None,
    }
    process_rate, source_rate = result["processing_files_per_hour"], result["source_files_per_hour"]
    if (selected_host and process_rate is not None and source_rate is not None
            and source_rate > 0 and process_rate <= source_rate):
        # The observed source is a lower bound, so this negative conclusion is
        # safe; a positive difference cannot establish actual net catch-up.
        result.update(continuous_state="not_catching_up", continuous_reason="completion_not_above_observed_source")

    if failed_count:
        result["state"] = "blocked"
    elif not running and not workload_ready:
        result["state"] = "warming_up"
    elif known_remaining == 0:
        result["state"] = "checking_latest" if running else "caught_up"
    elif running and active_count and result["queued_remaining_files"] == 0:
        result["state"] = "live_following"
    elif (running and workload_ready and host_instance_id and process_rate
          and result["completion_sample_size"] >= 4):
        seconds = known_remaining * 3600 / process_rate
        result.update(
            state="available", inventory_remaining_seconds=round(seconds, 1),
            inventory_clear_at_utc=(current + timedelta(seconds=seconds)).isoformat(timespec="seconds").replace("+00:00", "Z"),
        )
    return result
