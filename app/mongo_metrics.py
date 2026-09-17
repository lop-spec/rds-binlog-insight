"""Workspace metrics derived only from already collected telemetry."""
from collections import Counter, defaultdict
import logging
import math

LOGGER = logging.getLogger(__name__)
ANALYSIS_METRICS = ('CPUUtilization', 'IOPSUtilization', 'ScannedDocs', 'LockWaits', 'AvgRt')
MINUTE_MS = 60_000


def _count(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float) and math.isfinite(value) and value >= 0 and value.is_integer():
        return int(value)
    return None


def _queue_count(sample):
    lock = sample.get('global_lock')
    queue = lock.get('currentQueue') if isinstance(lock, dict) else None
    if not isinstance(queue, dict):
        return None
    readers, writers = _count(queue.get('readers')), _count(queue.get('writers'))
    if queue.get('total') is None:
        return readers + writers if readers is not None and writers is not None else None
    total = _count(queue['total'])
    if readers is not None and writers is not None and total != readers + writers:
        return None
    return total


def lock_wait_points(samples, start, end):
    """Latest native lock-queue gauge per minute, never a wait-duration proxy.

    Output timestamp is the minute end in epoch milliseconds, matching cloud
    metrics. Query start/end use epoch microseconds, as in the analytics API.
    Distinct nodes of the same role in a minute are ambiguous, not averaged.
    Missing snapshots/fields are omitted, not forward-filled or replaced by zero.
    """
    buckets = defaultdict(list)
    reasons = Counter()
    for sample in samples:
        try:
            timestamp = int(sample['timestamp'])
            if not start < timestamp * 1000 <= end:
                continue
            bucket = (timestamp + MINUTE_MS - 1) // MINUTE_MS * MINUTE_MS
            if bucket * 1000 > end:
                continue
            role, node = sample['role'], sample['node']
            if not isinstance(node, str) or not node or role not in ('Primary', 'Secondary'):
                raise ValueError('invalid_native_identity')
            buckets[role, bucket].append(sample)
        except (KeyError, ValueError, TypeError, OverflowError):
            reasons['invalid_native_identity_or_time'] += 1
    points = []
    for (role, bucket), rows in sorted(buckets.items()):
        if len({r['node'] for r in rows}) != 1:
            reasons['ambiguous_role_nodes'] += 1
            continue
        latest = max(int(r['timestamp']) for r in rows)
        gauges = []
        for row in rows:
            if int(row['timestamp']) != latest:
                continue
            gauges.append(_queue_count(row))
        if not gauges or None in gauges:
            reasons['lock_queue_not_reported'] += 1
            continue
        if len(set(gauges)) != 1:
            reasons['conflicting_native_samples'] += 1
            continue
        points.append(dict(timestamp=bucket, role=role, node=rows[0]['node'], metric='LockWaits',
                           value=gauges[0], period=60, sampled_at=latest,
                           source='serverStatus.globalLock.currentQueue', aggregation='latest_snapshot_per_minute',
                           unit='queued_operations'))
    if reasons or not points:
        LOGGER.warning('mongo_lock_wait unavailable: %s', dict(reasons) or {'no_native_samples': 1})
    return points
