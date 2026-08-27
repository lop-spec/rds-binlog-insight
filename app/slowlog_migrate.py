from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from .slowlog_index import (
    REQUIRED_RUNTIME_INDEXES,
    SLOWLOG_INDEX_VERSION,
    SlowLogIndex,
    _runtime_schema_gaps,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Explicitly migrate the dedicated slow-log serving index."
    )
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--path", type=Path)
    parser.add_argument("--quick-check", action="store_true")
    args = parser.parse_args()

    path = args.path or args.data_dir / "index" / "slowlog.sqlite3"
    index = SlowLogIndex(path, run_migrations=True)
    with index.connection() as conn:
        version = int(conn.execute("PRAGMA user_version").fetchone()[0])
        gaps = _runtime_schema_gaps(conn)
        indexes = {
            str(row["name"])
            for row in conn.execute("PRAGMA index_list(slowlog_events)")
        }
        integrity = (
            str(conn.execute("PRAGMA quick_check").fetchone()[0])
            if args.quick_check
            else "not-requested"
        )
    if version != SLOWLOG_INDEX_VERSION or gaps:
        raise RuntimeError(
            f"slow-log migration did not converge: version={version}, gaps={gaps}"
        )
    result = {
        "path": str(path),
        "schema_version": version,
        "required_indexes": sorted(REQUIRED_RUNTIME_INDEXES),
        "required_indexes_present": sorted(REQUIRED_RUNTIME_INDEXES & indexes),
        "size_bytes": path.stat().st_size,
        "quick_check": integrity,
        "sqlite_version": sqlite3.sqlite_version,
    }
    print(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
