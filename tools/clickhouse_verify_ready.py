from __future__ import annotations

import argparse
import hashlib
import json
import uuid
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from app.clickhouse_client import ClickHouseClient, ClickHouseConfig
from app.clickhouse_manifest import ClickHouseManifest, part_identity
from app.config import ensure_data_dirs
from app.credentials import load_credential
from app.metadata import MetadataStore
from app.oss_store import OssArchive


COLUMNS = (
    "event_id",
    "event_epoch_us",
    "source_file_name",
    "end_position",
    "row_index",
)


def _tuple(row: dict[str, Any]) -> tuple[str, int, str, int, int]:
    return (
        str(row.get("event_id") or ""),
        int(row.get("event_epoch_us") or 0),
        str(row.get("source_file_name") or ""),
        int(row.get("end_position") or 0),
        int(row.get("row_index") or 0),
    )


def _hash(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(
        sorted(_tuple(row) for row in rows),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(4 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", type=Path, default=Path("/data"))
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--part-id", default="")
    args = parser.parse_args()
    paths = ensure_data_dirs(args.data_dir)
    metadata = MetadataStore(
        args.data_dir / "metadata.sqlite3",
        run_migrations=False,
    )
    manifest = ClickHouseManifest(
        paths["index"] / "clickhouse" / "manifest.sqlite3",
        run_migrations=False,
    )
    client = ClickHouseClient(ClickHouseConfig.from_env())
    settings = metadata.load_settings()
    archive = OssArchive(
        settings,
        credential=load_credential(settings.credential_target),
    )
    with manifest.connection() as connection:
        if args.part_id:
            ready = connection.execute(
                """
                SELECT part_path, logical_part_id, row_count, sha256,
                       content_revision, max_event_epoch_us
                FROM clickhouse_parts
                WHERE status = 'ready' AND logical_part_id = ?
                LIMIT 1
                """,
                (args.part_id,),
            ).fetchall()
        else:
            ready = connection.execute(
                """
            SELECT part_path, logical_part_id, row_count, sha256,
                   content_revision, max_event_epoch_us
            FROM clickhouse_parts
            WHERE status = 'ready'
            ORDER BY max_event_epoch_us DESC, part_path DESC
            LIMIT ?
            """,
                (min(max(int(args.limit), 1), 100),),
            ).fetchall()
    results: list[dict[str, Any]] = []
    exact = True
    scratch = paths["scratch"] / "clickhouse-verify"
    scratch.mkdir(parents=True, exist_ok=True)
    for row in ready:
        path_text = str(row["part_path"])
        identity = str(row["logical_part_id"])
        current = metadata.part_by_path(path_text)
        if current is None or part_identity(current) != identity:
            results.append(
                {
                    "logical_part_id": identity,
                    "error": "source identity is no longer current",
                }
            )
            exact = False
            continue
        source_path = Path(path_text)
        temporary: Path | None = None
        if not source_path.is_file():
            temporary = scratch / f".{uuid.uuid4().hex}.parquet"
            archive.download_part(current, temporary)
            source_path = temporary
        try:
            source_rows = pq.read_table(
                source_path,
                columns=list(COLUMNS),
            ).to_pylist()
            target_rows = client.json_rows(
                f"""
                SELECT {', '.join(COLUMNS)}
                FROM {client.config.qualified_table}
                WHERE _source_part_key = {{part_key:String}}
                """,
                parameters={"part_key": identity},
                timeout=60,
            )
            source_hash = _hash(source_rows)
            target_hash = _hash(target_rows)
            source_sha = _file_sha256(source_path)
            matched = (
                len(source_rows) == len(target_rows)
                and len(source_rows) == int(row["row_count"] or 0)
                and source_hash == target_hash
                and source_sha == str(row["sha256"] or "")
            )
            exact = exact and matched
            results.append(
                {
                    "logical_part_id": identity,
                    "source_rows": len(source_rows),
                    "target_rows": len(target_rows),
                    "source_hash": source_hash,
                    "target_hash": target_hash,
                    "source_sha256_match": source_sha
                    == str(row["sha256"] or ""),
                    "exact_match": matched,
                }
            )
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)
    print(
        json.dumps(
            {
                "checked_parts": len(results),
                "exact_match": exact and bool(results),
                "parts": results,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
    )
    return 0 if exact and results else 2


if __name__ == "__main__":
    raise SystemExit(main())
