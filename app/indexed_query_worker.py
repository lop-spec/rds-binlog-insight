"""Internal single-flight supervisor. Each query owns one disposable native process."""
from __future__ import annotations

import json
import logging
import math
import os
import queue
import re
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOGGER = logging.getLogger(__name__)
MAX_PLAN_BYTES = 16 * 1024 * 1024
MAX_MESSAGE_BYTES = 34 * 1024 * 1024


def stop_owned_process(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)


class QueryServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, *, scratch='/scratch'):
        super().__init__(address, Handler)
        self.scratch = scratch
        self.job_lock = threading.Lock()
        self.active = None
        self.cancelled = {}


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, format, *args):
        # Never log request bodies, settings, SQL or row contents.
        LOGGER.info(format, *args)

    def json_response(self, status, value):
        data = json.dumps(value).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.send_header('Connection', 'close')
        self.close_connection = True
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        self.json_response(200 if self.path == '/healthz' else 404,
                           {'status': 'ok' if self.path == '/healthz' else 'not found'})

    def do_POST(self):
        self.connection.settimeout(5)
        try:
            length = int(self.headers.get('Content-Length', '0'))
            if not 0 < length <= MAX_PLAN_BYTES:
                raise ValueError('invalid request size')
            body = self.rfile.read(length)
            if len(body) != length:
                raise ValueError('incomplete request')
            payload = json.loads(body)
            job_id = payload['id']
            if not re.fullmatch('[a-f0-9]{32}', job_id):
                raise ValueError('invalid job id')
        except (ValueError, KeyError, TypeError, TimeoutError):
            self.close_connection = True
            self.json_response(400, {'error': 'invalid request'})
            return
        if self.path == '/cancel':
            with self.server.job_lock:
                job = self.server.active
                if job is None or job['id'] != job_id:
                    # A timed-out POST can still be reading its body. Fence that
                    # id before acknowledging cancellation; it must never start later.
                    now = time.monotonic()
                    self.server.cancelled = {k: v for k, v in self.server.cancelled.items() if v > now}
                    accepted = len(self.server.cancelled) < 128 or job_id in self.server.cancelled
                    if accepted:
                        self.server.cancelled[job_id] = now + 65
                    self.json_response(200, {'terminal': accepted})
                    return
                job['cancel'].set()
            self.json_response(200, {'terminal': job['done'].wait(4)})
            return
        if self.path != '/query':
            self.json_response(404, {'error': 'not found'})
            return
        try:
            seconds = min(max(float(payload.get('seconds', 55)), 0.1), 55)
            if not math.isfinite(seconds):
                raise ValueError('invalid deadline')
            if not isinstance(payload['work'], list) or not isinstance(payload['settings'], dict):
                raise ValueError('invalid plan')
            # AccessKey material is never sent here. ECS RAM role resolution is local.
            if payload['settings'].get('oss_auth_mode', 'ecs_ram_role') != 'ecs_ram_role':
                raise ValueError('worker requires ECS RAM role')
        except (ValueError, KeyError, TypeError):
            self.json_response(400, {'error': 'invalid plan'})
            return
        with self.server.job_lock:
            if self.server.cancelled.get(job_id, 0) > time.monotonic():
                LOGGER.warning('Indexed query refused: job was cancelled before admission')
                self.json_response(409, {'error': 'cancelled before admission'})
                return
            if self.server.active is not None:
                LOGGER.warning('Indexed query refused: worker busy')
                self.json_response(429, {'error': 'worker busy'})
                return
            job = {'id': job_id, 'cancel': threading.Event(), 'done': threading.Event()}
            self.server.active = job
        process = None
        pump = None
        terminal = None
        last_usage = {}
        started = time.monotonic()
        deadline = started + seconds
        def send(message):
            data = json.dumps(message, ensure_ascii=False, separators=(',', ':')).encode('utf-8') + b'\n'
            self.wfile.write(f'{len(data):X}\r\n'.encode('ascii') + data + b'\r\n')
            self.wfile.flush()

        try:
            self.send_response(200)
            self.send_header('Content-Type', 'application/x-ndjson')
            self.send_header('Transfer-Encoding', 'chunked')
            self.send_header('Connection', 'close')
            self.close_connection = True
            self.end_headers()
            with tempfile.TemporaryDirectory(prefix='query-', dir=self.server.scratch) as scratch:
                env = {**os.environ, 'TMPDIR': scratch, 'OMP_NUM_THREADS': '1',
                       'OPENBLAS_NUM_THREADS': '1', 'ARROW_NUM_THREADS': '1',
                       'PYTHONIOENCODING': 'utf-8', 'PYTHONUNBUFFERED': '1'}
                process = subprocess.Popen(
                    [sys.executable, '-m', 'app.indexed_query_native'],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, env=env,
                    creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
                messages = queue.Queue(maxsize=32)
                stopping = threading.Event()

                def read_output():
                    try:
                        while not stopping.is_set():
                            line = process.stdout.readline(MAX_MESSAGE_BYTES + 1)
                            if not line:
                                return
                            if len(line) > MAX_MESSAGE_BYTES:
                                LOGGER.error('Indexed query native output exceeds message budget: job=%s', job_id)
                                job['cancel'].set()
                                return
                            message = json.loads(line)
                            while not stopping.is_set():
                                try:
                                    messages.put(message, timeout=0.1)
                                    break
                                except queue.Full:
                                    pass
                    except (ValueError, OSError) as exc:
                        LOGGER.error('Indexed query native output invalid/closed: job=%s reason=%s',
                                     job_id, type(exc).__name__)
                        if not stopping.is_set():
                            job['cancel'].set()

                pump = threading.Thread(target=read_output, daemon=True)
                pump.start()

                def write_input():
                    try:
                        with process.stdin:
                            process.stdin.write(body)
                    except OSError as exc:
                        LOGGER.warning('Indexed query input closed: job=%s cancelled=%s reason=%s',
                                       job_id, job['cancel'].is_set(), type(exc).__name__)
                        if not stopping.is_set():
                            job['cancel'].set()

                writer = threading.Thread(target=write_input, daemon=True)
                writer.start()
                try:
                    while True:
                        if job['cancel'].is_set() or time.monotonic() >= deadline:
                            code = 'QUERY_CANCELLED' if job['cancel'].is_set() else 'QUERY_DEADLINE_EXCEEDED'
                            terminal = {'error': {'code': code, 'message': '独立查询进程已终止'}}
                            break
                        try:
                            message = messages.get(timeout=0.25)
                        except queue.Empty:
                            if process.poll() is not None and not pump.is_alive():
                                terminal = {'error': {'code': 'QUERY_WORKER_EXITED',
                                                      'message': f'查询进程退出：{process.returncode}'}}
                                break
                            send({'heartbeat': True})
                            continue
                        if 'result' in message or 'error' in message:
                            terminal = message
                            try:
                                process.wait(timeout=min(2, max(0.01, deadline - time.monotonic())))
                            except subprocess.TimeoutExpired:
                                terminal = {'error': {'code': 'QUERY_TERMINATION_FAILED',
                                                      'message': '查询未正常退出，已强制终止自身进程'}}
                            if 'result' in terminal and process.poll() != 0:
                                terminal = {'error': {'code': 'QUERY_WORKER_EXITED',
                                                      'message': '查询进程未成功退出'}}
                            break
                        if 'usage' in message:
                            last_usage = message['usage']
                        else:
                            send(message)
                finally:
                    stopping.set()
                    stop_owned_process(process)
                    pump.join(timeout=2)
                    writer.join(timeout=2)
                    process.stdout.close()
            job['done'].set()  # Native terminal, independent of a slow HTTP consumer.
            if job['cancel'].is_set():
                terminal = {'error': {'code': 'QUERY_CANCELLED', 'message': '独立查询进程已终止'}}
            if 'error' in terminal:
                terminal['error']['last_reported_usage'] = last_usage
            send(terminal)
            self.wfile.write(b'0\r\n\r\n')
            self.wfile.flush()
        except Exception as exc:
            LOGGER.log(logging.INFO if job['cancel'].is_set() else logging.ERROR,
                       'Indexed query connection ended: job=%s cancelled=%s reason=%s',
                       job_id, job['cancel'].is_set(), type(exc).__name__)
            self.close_connection = True
        finally:
            if process is not None:
                stop_owned_process(process)
            confirmed = process is None or process.poll() is not None
            LOGGER.info('Indexed query terminal: job=%s confirmed=%s elapsed=%.3f result=%s usage=%s',
                        job_id, confirmed, time.monotonic() - started,
                        'success' if terminal and 'result' in terminal else 'failed/cancelled', last_usage)
            with self.server.job_lock:
                if confirmed:
                    self.server.active = None
                    job['done'].set()


def main():
    logging.basicConfig(level=logging.INFO)
    # A container/process-tree limit is deployment's responsibility, not DuckDB's.
    QueryServer(('0.0.0.0', 8770), scratch=os.environ.get('TMPDIR', '/scratch')).serve_forever()


if __name__ == '__main__':
    main()
