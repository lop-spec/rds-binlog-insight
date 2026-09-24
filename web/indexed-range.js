"use strict";

// Pure interval selection shared by the UI and Node tests. Never bridge a hole
// or round the selected boundaries outside the certified microsecond interval.
function indexedQuickRange(intervals, durationMs) {
  if (!Number.isFinite(durationMs) || durationMs <= 0) return null;
  for (const [lo, hi] of [...intervals].sort((a, b) => b[1] - a[1])) {
    const start = Math.ceil(Number(lo) / 1_000_000) * 1000;
    const end = Math.floor(Number(hi) / 1_000_000) * 1000;
    if (!Number.isFinite(start) || !Number.isFinite(end) || start > end) continue;
    const selectedStart = Math.max(start, end - durationMs);
    return { start: selectedStart, end, clipped: end - selectedStart < durationMs };
  }
  return null;
}

// Narrow a custom request inside its own bounds; never silently move it to a
// different date or bridge an unindexed hole. The form shows the returned range.
function indexedCustomRange(intervals, requestedStart, requestedEnd) {
  if (!Number.isFinite(requestedStart) || !Number.isFinite(requestedEnd) || requestedStart > requestedEnd) return null;
  for (const [lo, hi] of [...intervals].sort((a, b) => b[1] - a[1])) {
    const start = Math.max(requestedStart, Math.ceil(Number(lo) / 1_000_000) * 1000);
    const end = Math.min(requestedEnd, Math.floor(Number(hi) / 1_000_000) * 1000);
    if (!Number.isFinite(start) || !Number.isFinite(end) || start > end) continue;
    return { start, end, clipped: start !== requestedStart || end !== requestedEnd };
  }
  return null;
}

// Table-row mode: end at the newest ingested second and keep the requested
// duration. Holes inside the range are reported by the server, never bridged
// silently and never used to shrink the user's range.
function rowsQuickRange(intervals, durationMs) {
  if (!Number.isFinite(durationMs) || durationMs <= 0 || !intervals || !intervals.length) return null;
  const newest = Math.max(...intervals.map((interval) => Number(interval[1])));
  if (!Number.isFinite(newest)) return null;
  const end = Math.floor(newest / 1_000_000) * 1000;
  return { start: end - durationMs, end };
}

// Count uncovered segments (microsecond bounds) overlapping [startMs, endMs].
function gapsWithin(gaps, startMs, endMs) {
  const counts = {};
  let total = 0;
  for (const gap of gaps || []) {
    const lo = Number(gap.start) / 1000, hi = Number(gap.end) / 1000;
    if (!Number.isFinite(lo) || !Number.isFinite(hi) || hi < startMs || lo > endMs) continue;
    counts[gap.reason] = (counts[gap.reason] || 0) + 1;
    total += 1;
  }
  return { total, counts };
}

if (typeof module !== "undefined") module.exports = { indexedQuickRange, indexedCustomRange, rowsQuickRange, gapsWithin };
