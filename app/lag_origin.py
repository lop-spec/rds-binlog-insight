"""Where a replication-lag episode began, read off the lag series of one replica node.

An applier that is stuck on one event (a DDL waiting for a metadata lock, one huge transaction) falls behind one second
per second, and the primary-side timestamp of the event it is stuck on does not move: ``origin(t) = t - lag(t)``. A
replica that is only too slow for the write rate falls behind by less than a second per second and its origin keeps
moving. Everything here is arithmetic on that identity plus lookups of what happened at the origin timestamp; there is
no model, no sampling and no SQL text matching.

A Cloud Monitor minute sample is stamped with the END of the minute it describes (checked against a recorded incident: the
sample stamped 11:19:00 held the lag of 11:18:30, and the ALTER that caused it was logged at 11:17:57), so the lag is taken
as the lag of the middle of that minute. The origin carries about +-30 s of slack in general; a stuck applier's samples all
point at the same event, so for a stall it is much closer.
"""
from __future__ import annotations

import logging
import statistics
from typing import Any, Callable

LOGGER = logging.getLogger(__name__)

PERIOD_US = 60_000_000
FLOOR_S = 30.0               # the cloud alert rule: lag >= 30 s
STALL_SLOPE = 0.85           # lag seconds gained per second; a stuck applier gains 1.0
MIN_RUN_MINUTES = 3          # shorter climbs are noise, not an episode phase
TAIL_FLOOR_S = 0.5           # an episode ends when the lag is back under this
MATCH_US = 90_000_000        # how far a binlog event may sit from the estimated origin
RELEASE_US = 90_000_000      # how far a statement's end may sit from the moment the stall let go
MIN_BLOCKED_US = 60_000_000  # a statement shorter than this was not held by a minutes-long stall
PEAK_REACH_US = 5 * PERIOD_US  # how far past the climbing run the peak may be and still count as the release
MAX_EPISODES = 3
MAX_SAMPLES = 5


def _origin_us(stamp_us: int, lag_s: float, period_us: int = PERIOD_US) -> float:
    """Primary-side time of the event being applied: the middle of the sample's minute minus the lag."""
    return stamp_us - period_us / 2 - lag_s * 1_000_000


def _runs(flags: list[bool]) -> list[tuple[int, int]]:
    """Index ranges [first, last] of consecutive true flags."""
    out: list[tuple[int, int]] = []
    start = None
    for index, flag in enumerate(flags):
        if flag and start is None:
            start = index
        elif not flag and start is not None:
            out.append((start, index - 1))
            start = None
    if start is not None:
        out.append((start, len(flags) - 1))
    return out


def lag_episodes(samples: list[tuple[int, float]], period_us: int = PERIOD_US, floor_s: float = FLOOR_S) -> list[dict[str, Any]]:
    """Episodes where the lag reached ``floor_s``: onset, peak, end, and whether the applier was stuck on one event.

    ``samples`` are ``(period_end_us, lag_seconds)`` in time order. A missing minute splits the series: a slope is only
    taken between two consecutive minutes, never across a gap.
    """
    pts = sorted(samples)
    episodes: list[dict[str, Any]] = []
    i, n = 0, len(pts)
    while i < n:
        if pts[i][1] < floor_s:
            i += 1
            continue
        last_over = i
        while last_over + 1 < n and pts[last_over + 1][1] >= floor_s:
            last_over += 1
        first = i
        while first > 0 and TAIL_FLOOR_S < pts[first - 1][1] < pts[first][1] and pts[first][0] - pts[first - 1][0] == period_us:
            first -= 1
        last = last_over
        while last + 1 < n and pts[last + 1][1] > TAIL_FLOOR_S and pts[last + 1][0] - pts[last][0] == period_us:
            last += 1
        episodes.append(_describe(pts[first:last + 1], period_us))
        i = last + 1
    return episodes


def _describe(pts: list[tuple[int, float]], period_us: int) -> dict[str, Any]:
    period_s = period_us / 1_000_000
    peak_index = max(range(len(pts)), key=lambda k: (pts[k][1], -k))
    slopes: list[float | None] = []
    for (t0, l0), (t1, l1) in zip(pts, pts[1:]):
        slopes.append((l1 - l0) / period_s if t1 - t0 == period_us else None)
    # The stuck phase: the longest run of ~1 s/s climbing, before the peak, whose origin timestamp stays put. A sample
    # that straddles the onset or the release (or noise from elsewhere) moves a few samples' origin away from the
    # rest: those are cut off the run instead of discarding it, so only a run that is mostly stable is kept.
    climbing = [slope is not None and slope >= STALL_SLOPE and index < peak_index for index, slope in enumerate(slopes)]
    stall = None
    best = 0
    for low, high in _runs(climbing):
        window = pts[low:high + 2]
        origins = [_origin_us(t, lag, period_us) for t, lag in window]
        centre = statistics.median(origins)
        for first, last in _runs([abs(origin - centre) <= period_us for origin in origins]):
            if last - first >= MIN_RUN_MINUTES and last - first > best:
                kept = origins[first:last + 1]
                best = last - first
                stall = {"start_us": window[first][0], "end_us": window[last][0], "minutes": last - first,
                         "origin_us": int(statistics.median(kept)), "origin_spread_s": round((max(kept) - min(kept)) / 1_000_000, 1)}
    if stall:
        # The stuck applier lets go somewhere inside the minute after its last constant-origin sample, so that sample's stamp is
        # the estimate. When the run ended well before the peak (samples bent by something else), the peak is the better one.
        gap_us = pts[peak_index][0] - stall["end_us"]
        stall["release_us"] = pts[peak_index][0] if period_us < gap_us <= PEAK_REACH_US else stall["end_us"]
    rising = [slope for index, slope in enumerate(slopes) if slope is not None and index < peak_index and slope > 0]
    falling = [-slope for index, slope in enumerate(slopes) if slope is not None and index >= peak_index and slope < 0]
    kind = "stall" if stall else ("behind" if len(rising) >= MIN_RUN_MINUTES else "spike")
    return {"first_us": pts[0][0], "peak_us": pts[peak_index][0], "peak_s": round(pts[peak_index][1], 2),
            "last_us": pts[-1][0], "kind": kind, "stall": stall,
            "growth_rate": round(statistics.median(rising), 2) if rising else None,
            "recovery_rate": round(statistics.median(falling), 2) if falling else None}


def _statement(event: dict[str, Any]) -> dict[str, Any]:
    return {"event_id": event.get("event_id"), "sql_id": event.get("sql_id") or "", "fingerprint": event.get("fingerprint") or "",
            "database_name": event.get("database_name") or "", "account": event.get("account") or "",
            "start_us": int(event["start_us"]), "duration_ms": int(event["duration_ms"])}


def _held_by_the_stall(events: list[dict[str, Any]], origin_us: int, release_us: int) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Statements of the lagging node that let go together with the stall: those that started after it began were queued
    behind it; those that started before it and finished with it are candidates for the holder of the lock."""
    queued: list[dict[str, Any]] = []
    holders: list[dict[str, Any]] = []
    for event in events:
        start, duration = int(event["start_us"]), int(event["duration_ms"]) * 1000
        if duration < MIN_BLOCKED_US or abs(start + duration - release_us) > RELEASE_US:
            continue
        (queued if start >= origin_us - MATCH_US else holders).append(event)
    queued.sort(key=lambda e: (-int(e["duration_ms"]), str(e.get("event_id") or "")))
    holders.sort(key=lambda e: (-int(e["duration_ms"]), str(e.get("event_id") or "")))
    return queued, holders


def describe_lag(node_id: str, samples: list[tuple[int, float]], *, events: list[dict[str, Any]],
                 other_peaks: dict[str, float], instance: str,
                 ddl_lookup: Callable[[str, int, int], list[dict[str, Any]]] | None = None,
                 period_us: int = PERIOD_US) -> dict[str, Any]:
    """The ``lag_origin`` block of one replica node: its episodes and, for a stuck applier, what sat at the origin."""
    episodes = lag_episodes(samples, period_us)
    others_quiet = all(peak < FLOOR_S for peak in other_peaks.values()) if other_peaks else None
    for episode in episodes:
        episode["node_local"] = others_quiet
        stall = episode["stall"]
        if not stall:
            continue
        origin, release = stall["origin_us"], stall["release_us"]
        episode["trigger"] = {"type": "none"}
        if ddl_lookup is not None:
            try:
                found = ddl_lookup(instance, origin - 2 * MATCH_US, origin + 2 * MATCH_US)
            except Exception:  # noqa: BLE001 - the origin card is evidence, never a reason to fail the analysis
                LOGGER.warning("lag_origin ddl lookup failed: instance=%s node=%s origin_us=%s", instance, node_id, origin, exc_info=True)
                episode["trigger"] = {"type": "unavailable"}
                found = []
            near = [row for row in found if abs(int(row["event_epoch_us"]) - origin) <= MATCH_US]
            if near:
                best = min(near, key=lambda row: abs(int(row["event_epoch_us"]) - origin))
                episode["trigger"] = {"type": "ddl", "event_us": int(best["event_epoch_us"]),
                                      "sql": str(best.get("sample_sql") or "")[:1000],
                                      "database": str(best.get("database_name") or ""), "table": str(best.get("table_name") or ""),
                                      "offset_s": round((int(best["event_epoch_us"]) - origin) / 1_000_000, 1)}
        queued, holders = _held_by_the_stall(events, origin, release)
        episode["queued"] = {"count": len(queued), "samples": [_statement(e) for e in queued[:MAX_SAMPLES]]}
        episode["holders"] = {"count": len(holders), "samples": [_statement(e) for e in holders[:MAX_SAMPLES]]}
    episodes.sort(key=lambda e: (-e["peak_s"], e["first_us"]))
    if len(episodes) > MAX_EPISODES:
        LOGGER.warning("lag_origin episodes truncated: node=%s total=%d", node_id, len(episodes))
    return {"floor_s": FLOOR_S, "episodes": episodes[:MAX_EPISODES], "episode_count": len(episodes),
            "origin_slack_s": period_us / 2_000_000}
