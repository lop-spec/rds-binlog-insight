from __future__ import annotations

import argparse
import json
from pathlib import Path

from .metadata import MetadataStore


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run explicit, rollback-compatible metadata serving migrations."
    )
    parser.add_argument("--data-dir", default="/data")
    parser.add_argument(
        "--rebuild-storage-file-stats",
        action="store_true",
        help="materialize per-file capacity totals after additive schema setup",
    )
    args = parser.parse_args()
    store = MetadataStore(Path(args.data_dir) / "metadata.sqlite3")
    result: dict[str, object] = {"schema_version": 1}
    if args.rebuild_storage_file_stats:
        result["storage_file_stats"] = store.rebuild_storage_file_stats()
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
