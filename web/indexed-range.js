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

if (typeof module !== "undefined") module.exports = { indexedQuickRange, indexedCustomRange };
