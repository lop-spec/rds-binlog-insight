"""Read-only StarRocks CMS metrics; no data-plane connection or business writes.

Each configured cluster is an explicit allowlist. CMS timestamps represent cloud
points, not SQL execution timestamps; missing nodes and pages are never zeros.
"""
from __future__ import annotations

import json
import logging
import math
import os
import re
import threading
import time
from dataclasses import replace
from pathlib import Path
from typing import Any, Callable

from .slow_log_collector import DasRpcClient

LOGGER = logging.getLogger(__name__)
CONFIG_FILE = 'starrocks-instances.json'
MAX_WINDOW_MS = 7 * 86400 * 1000
METRICS = {
    'fe_qps': {'name': 'serverless_starrocks_fe_qps', 'label': 'FE 查询 / 秒', 'unit': '次/秒', 'kind': 'FE'},
    'fe_pending': {'name': 'serverless_starrocks_fe_query_queue_pending', 'label': 'FE 排队查询', 'unit': '个', 'kind': 'FE'},
    'fe_up': {'name': 'serverless_starrocks_fe_up', 'label': 'FE 在线状态', 'unit': '', 'kind': 'FE'},
    'be_up': {'name': 'serverless_starrocks_be_up', 'label': 'BE 在线状态', 'unit': '', 'kind': 'BE'},
    'fe_cpu': {'name': 'serverless_starrocks_fe_cpu_util', 'label': 'FE CPU 使用率', 'unit': '%', 'kind': 'FE'},
    'be_cpu': {'name': 'serverless_starrocks_be_cpu_util', 'label': 'BE CPU 使用率', 'unit': '%', 'kind': 'BE'},
    'fe_memory': {'name': 'serverless_starrocks_fe_mem_util', 'label': 'FE 内存使用率', 'unit': '%', 'kind': 'FE'},
    'be_memory': {'name': 'serverless_starrocks_be_process_mem_bytes', 'label': 'BE 进程内存', 'unit': '字节', 'kind': 'BE'},
}


class CmsClient(DasRpcClient):
    VERSION = '2019-01-01'


def load_instances(root: Path) -> list[dict[str, Any]]:
    path = Path(root) / CONFIG_FILE
    if not path.exists():
        return []
    value = json.loads(path.read_text(encoding='utf-8'))
    entries = value.get('instances') if isinstance(value, dict) else value
    if not isinstance(entries, list) or len(entries) > 50:
        raise ValueError('starrocks_instances_array_required')
    result, seen = [], set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError('invalid_starrocks_instance')
        identity = entry.get('instanceId')
        region = entry.get('region')
        enabled = entry.get('enabled', False)
        if not isinstance(identity, str) or not re.fullmatch(r'c-[a-z0-9]{4,64}', identity) or identity in seen:
            raise ValueError('invalid_or_duplicate_starrocks_instance')
        if not isinstance(region, str) or not re.fullmatch(r'[a-z]+-[a-z]+(?:-\d+)?', region):
            raise ValueError('invalid_starrocks_region')
        if not isinstance(enabled, bool):
            raise ValueError('invalid_starrocks_enabled')
        label = entry.get('label', identity)
        if not isinstance(label, str) or not label or len(label) > 128:
            raise ValueError('invalid_starrocks_label')
        seen.add(identity)
        result.append({'instanceId': identity, 'region': region, 'enabled': enabled, 'label': label})
    return result


def _one(params: dict[str, Any], name: str, default: str = '') -> str:
    value = params.get(name, default)
    if isinstance(value, list):
        if len(value) != 1:
            raise ValueError('duplicate_parameter')
        value = value[0]
    return str(value)


class StarRocksMetrics:
    """Bounded, on-demand CMS queries. Credentials use the app's existing loader."""

    def __init__(self, root: Path, settings_loader: Callable, credential_loader: Callable, *,
                 client_factory: Callable | None = None, now_ms: Callable | None = None):
        self._settings_loader = settings_loader
        self._credential_loader = credential_loader
        self._client_factory = client_factory or (lambda settings, credential: CmsClient(settings, credential, timeout=15))
        self._now_ms = now_ms or (lambda: time.time_ns() // 1_000_000)
        self._gate = threading.BoundedSemaphore(2)
        self.error = ''
        try:
            self.entries = load_instances(Path(root))
        except (ValueError, OSError, TypeError) as exc:
            LOGGER.warning('starrocks config unavailable: %s', type(exc).__name__)
            self.entries = []
            self.error = type(exc).__name__

    def status(self) -> dict[str, Any]:
        active = [item for item in self.entries if item['enabled']]
        return {
            'status': 'unavailable' if self.error else 'ok' if active else 'not_configured',
            'reason': self.error,
            'instances': [{'id': item['instanceId'], 'label': item['label']} for item in active],
            'metrics': [{'id': key, **item} for key, item in METRICS.items()],
            'audit': {'status': 'not_connected', 'message': '审计与慢查询尚未接入'},
            'running_queries': {'status': 'not_connected', 'message': '全局在途查询尚未接入'},
        }

    def query(self, params: dict[str, Any]) -> dict[str, Any]:
        identity = _one(params, 'instance')
        entry = next((item for item in self.entries if item['enabled'] and item['instanceId'] == identity), None)
        if entry is None:
            raise ValueError('configured_starrocks_instance_required')
        metric_id = _one(params, 'metric')
        if metric_id not in METRICS:
            raise ValueError('unsupported_starrocks_metric')
        try:
            start_us, end_us = int(_one(params, 'startEpochUs')), int(_one(params, 'endEpochUs'))
        except (TypeError, ValueError):
            raise ValueError('invalid_starrocks_time') from None
        start_ms, end_ms = start_us // 1000, end_us // 1000
        if start_ms < 0 or end_ms <= start_ms or end_ms - start_ms > MAX_WINDOW_MS or end_ms > self._now_ms() + 60_000:
            raise ValueError('starrocks_window_must_be_up_to_seven_days')
        period = 60 if end_ms - start_ms <= 86400 * 1000 else 300
        if not self._gate.acquire(blocking=False):
            raise RuntimeError('starrocks_metrics_busy')
        try:
            return self._query(entry, metric_id, start_ms, end_ms, period)
        finally:
            self._gate.release()

    def _query(self, entry: dict[str, Any], metric_id: str, start_ms: int, end_ms: int, period: int) -> dict[str, Any]:
        settings = self._settings_loader()
        if os.environ.get('RDS_BINLOG_CLOUD_AUTH_MODE', '').strip() != 'ecs_ram_role':
            raise RuntimeError('starrocks_requires_ecs_role')
        credential = self._credential_loader(settings.credential_target)
        if credential is None:
            raise RuntimeError('cloud_credential_unavailable')
        region, identity = entry['region'], entry['instanceId']
        # The application may run in another region; its VPC endpoint is not assumed reachable.
        scoped = replace(settings, region_id=region, db_instance_id=identity, endpoint=f'https://metrics.{region}.aliyuncs.com')
        client = self._client_factory(scoped, credential)
        params = {
            'Namespace': 'acs_emr', 'MetricName': METRICS[metric_id]['name'],
            'Dimensions': json.dumps({'clusterId': identity}, separators=(',', ':')),
            'StartTime': str(start_ms), 'EndTime': str(end_ms), 'Period': str(period), 'Length': '1000',
        }
        points, seen_tokens, token = [], set(), None
        for _ in range(32):
            if token:
                params['NextToken'] = token
            response = client.call('DescribeMetricList', params)
            if str(response.get('Code', '')) != '200':
                raise RuntimeError('starrocks_cms_error')
            raw = response.get('Datapoints')
            rows = json.loads(raw) if isinstance(raw, str) else raw
            if not isinstance(rows, list):
                raise RuntimeError('starrocks_invalid_datapoints')
            for row in rows:
                if not isinstance(row, dict) or row.get('clusterId') != identity:
                    raise RuntimeError('starrocks_metric_identity_mismatch')
                node = row.get('podName')
                value = row.get('Average')
                timestamp = row.get('timestamp')
                if (not isinstance(node, str) or not node or len(node) > 256
                        or isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
                        or isinstance(timestamp, bool) or not isinstance(timestamp, int)
                        or not start_ms < timestamp <= end_ms):
                    raise RuntimeError('starrocks_invalid_metric_point')
                points.append({'timestamp': timestamp, 'node': node, 'value': value, 'period': period})
            token = response.get('NextToken')
            if not token:
                break
            if not isinstance(token, str) or token in seen_tokens:
                raise RuntimeError('starrocks_metric_pagination_cycle')
            seen_tokens.add(token)
        else:
            raise RuntimeError('starrocks_metric_page_limit')
        point_keys = [(p['node'], p['timestamp']) for p in points]
        if len(set(point_keys)) != len(point_keys):
            raise RuntimeError('starrocks_conflicting_metric_points')
        points.sort(key=lambda p: (p['node'], p['timestamp']))
        return {
            'instance': identity, 'metric': metric_id, 'label': METRICS[metric_id]['label'],
            'unit': METRICS[metric_id]['unit'], 'kind': METRICS[metric_id]['kind'],
            'start_ms': start_ms, 'end_ms': end_ms, 'period': period, 'points': points,
            'nodes': sorted({point['node'] for point in points}), 'source': 'aliyun_cms',
            'complete': False,
            'warning': '仅列云监控返回的节点和采样点；无法仅凭返回结果确认节点全覆盖，缺失不当作零。',
            'audit_status': 'not_connected', 'running_queries_status': 'not_connected',
        }
