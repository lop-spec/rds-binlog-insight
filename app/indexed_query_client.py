"""Client for the opt-in, hard-isolated indexed Parquet execution service."""
from __future__ import annotations

import json
import logging
import time
import uuid

import requests

LOGGER = logging.getLogger(__name__)
MAX_PLAN_BYTES = 16 * 1024 * 1024


class IndexedQueryError(RuntimeError):
    def __init__(self, message, code='INDEXED_QUERY_FAILED'):
        super().__init__(message)
        self.code = code


class DeadlineControl:
    def __init__(self, control, seconds=60):
        self.control = control
        self.deadline = time.monotonic() + seconds

    def check_cancelled(self):
        if self.control is not None:
            self.control.check_cancelled()
        if time.monotonic() >= self.deadline:
            raise IndexedQueryError('索引检索超过总时限', 'QUERY_DEADLINE_EXCEEDED')

    def __getattr__(self, name):
        if self.control is not None:
            return getattr(self.control, name)
        return lambda *args, **kwargs: None


def execute(url, payload, control):
    control.check_cancelled()
    job_id = uuid.uuid4().hex
    payload = {**payload, 'id': job_id,
               'seconds': min(55, max(0.1, control.deadline - time.monotonic() - 2))}
    body = json.dumps(payload, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    if len(body) > MAX_PLAN_BYTES:
        raise IndexedQueryError('候选计划超过16 MiB，未启动正文扫描', 'QUERY_PLAN_BUDGET_EXCEEDED')
    session = requests.Session()
    # The worker is an internal service, never a request through an ambient proxy.
    session.trust_env = False
    response = None
    terminal = False
    try:
        response = session.post(url.rstrip('/') + '/query', data=body,
                                headers={'Content-Type': 'application/json'},
                                stream=True, timeout=(3, 5))
        if response.status_code != 200:
            # HTTP status alone cannot prove that an upstream never admitted
            # the request. Fence/cancel this id before reporting the rejection.
            raise IndexedQueryError(f'查询执行单元拒绝请求：HTTP {response.status_code}',
                                    'QUERY_WORKER_UNAVAILABLE')
        outcome = None
        for line in response.iter_lines(chunk_size=None):
            control.check_cancelled()
            if not line:
                continue
            message = json.loads(line)
            if 'progress' in message:
                progress = message['progress']
                control.advance(current_file=progress.get('file', ''),
                                scanned_bytes=int(progress.get('bytes', 0)))
            if 'result' in message or 'error' in message:
                terminal = True
                outcome = message
        # Consume the terminating HTTP chunk before closing a successful stream.
        control.check_cancelled()
        if outcome is not None:
            if 'error' in outcome:
                error = outcome['error']
                raise IndexedQueryError(error['message'], error['code'])
            return outcome['result']
        raise IndexedQueryError('执行单元连接结束但没有查询终态', 'QUERY_WORKER_LOST')
    except BaseException:
        if not terminal:
            try:
                cancelled = session.post(url.rstrip('/') + '/cancel',
                                         json={'id': job_id}, timeout=(3, 5))
                cancelled.raise_for_status()
                if not cancelled.json().get('terminal'):
                    raise RuntimeError('native process still active')
            except Exception as exc:
                LOGGER.error('Indexed query cancellation NOT confirmed: job=%s reason=%s',
                             job_id, type(exc).__name__)
                raise IndexedQueryError('无法确认独立查询进程已终止；未回退主服务扫描',
                                        'QUERY_CANCEL_UNCONFIRMED') from exc
        LOGGER.warning('Indexed query failed/cancelled; no CK or in-process fallback: job=%s', job_id)
        raise
    finally:
        if response is not None:
            response.close()
        session.close()
