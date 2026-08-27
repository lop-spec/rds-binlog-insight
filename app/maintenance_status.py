from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from .config import utc_now_text


WORKER_PROGRESS_NAME = "index-worker-progress.json"
SUPERVISOR_STATUS_NAME = "index-worker-status.json"
SLOWLOG_WORKER_STATUS_NAME = "slowlog-worker-status.json"
CLICKHOUSE_SLOWLOG_WORKER_STATUS_NAME = "clickhouse-slowlog-worker-status.json"


def read_json_status(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def write_json_status(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {**value, "updatedAt": utc_now_text()}
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
