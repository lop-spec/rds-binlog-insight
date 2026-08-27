from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from app.clickhouse_query import ClickHouseQueryBackend
from app.metadata import MetadataStore
from tools.clickhouse_poc_benchmark import _request
from tools.clickhouse_raw_benchmark import _raw_once


IDENTITY_COLUMNS = (
    "event_id",
    "event_epoch_us",
    "source_file_name",
    "end_position",
    "row_index",
)


def _identity(row: dict[str, Any]) -> list[str]:
    return [str(row.get(column) or "") for column in IDENTITY_COLUMNS]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Verify that a ClickHouse raw-OSS search result can be resolved "
            "by the existing SQL Insight event-detail endpoint."
        )
    )
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--current-api", required=True)
    parser.add_argument("--current-host-header", default="")
    parser.add_argument("--start-epoch-us", type=int, required=True)
    parser.add_argument("--end-epoch-us", type=int, required=True)
    parser.add_argument("--instance", required=True)
    parser.add_argument("--database", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--timeout-seconds", type=int, default=60)
    args = parser.parse_args()

    metadata = MetadataStore(
        args.data_dir / "metadata.sqlite3",
        run_migrations=False,
    )
    try:
        backend = ClickHouseQueryBackend.from_env(metadata, args.data_dir)
        if backend is None or not backend.raw_serving:
            raise RuntimeError("ClickHouse raw-OSS serving backend is unavailable")
        rows, coverage = _raw_once(
            backend,
            {
                "source": "binlog",
                "instance": args.instance,
                "database": args.database,
                "table": args.table,
                "start_epoch_us": args.start_epoch_us,
                "end_epoch_us": args.end_epoch_us,
                "limit": 1,
            },
        )
    finally:
        metadata.close()

    if not rows:
        raise RuntimeError("ClickHouse raw-OSS query returned no event")
    search_row = rows[0]
    event_id = str(search_row.get("event_id") or "")
    locator = str(search_row.get("locator") or "")
    if not event_id or not locator:
        raise RuntimeError("ClickHouse result is missing event_id or locator")

    url = args.current_api.rstrip("/") + "/api/event?" + urlencode(
        {
            "id": event_id,
            "locator": locator,
            "instance": args.instance,
        }
    )
    body = _request(
        url,
        headers=(
            {"Host": args.current_host_header}
            if args.current_host_header
            else {}
        ),
        timeout=max(int(args.timeout_seconds), 1),
    )
    payload = json.loads(body.decode("utf-8"))
    detail = dict(payload.get("data") or {})
    exact_match = _identity(search_row) == _identity(detail)
    identity_hash = hashlib.sha256(
        json.dumps(
            _identity(search_row),
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    result = {
        "ok": bool(payload.get("ok")) and exact_match,
        "exact_match": exact_match,
        "identity_hash": identity_hash,
        "locator_kind": "logical-part" if len(locator.split(":", 1)[0]) == 64 else "other",
        "coverage": coverage,
    }
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
