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

if (typeof module !== "undefined") module.exports = { indexedQuickRange };
