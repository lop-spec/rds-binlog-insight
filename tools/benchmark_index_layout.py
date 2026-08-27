from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq

from app.oss_store import OssRangeReader
from app.search_index import SearchIndex


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000)
    args = parser.parse_args()
    row_count = max(args.rows, 10_000)
    base_epoch_us = 1_785_340_000_000_000
    target_row = row_count * 7 // 8
    order = sorted(
        range(row_count),
        key=lambda index: (
            f"table_{index % 10:02d}",
            "UPDATE",
            (base_epoch_us + index * 100_000) // 300_000_000,
            base_epoch_us + index * 100_000,
        ),
    )
    columns: dict[str, list[object]] = {
        "event_epoch_us": [],
        "database_name": [],
        "table_name": [],
        "operation": [],
        "sql_text": [],
        "before_json": [],
        "after_json": [],
        "transaction_id": [],
        "source_file_name": [],
    }
    for index in order:
        columns["event_epoch_us"].append(base_epoch_us + index * 100_000)
        columns["database_name"].append("example_app")
        columns["table_name"].append(f"table_{index % 10:02d}")
        columns["operation"].append("UPDATE")
        columns["sql_text"].append(
            f"UPDATE table_{index % 10:02d} "
            f"SET value={index % 997} WHERE id={index}"
        )
        columns["before_json"].append(
            f'{{"id":{index},"value":{index % 997}}}'
        )
        needle = 2_571_634 if index == target_row else index % 10_000
        columns["after_json"].append(
            f'{{"id":{index},"value":{(index + 1) % 997},"needle":"{needle}"}}'
        )
        columns["transaction_id"].append(f"tx-{index // 5}")
        columns["source_file_name"].append("mysql-bin.050000")

    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        parquet_path = root / "part.parquet"
        table = pa.table(columns)
        started = time.perf_counter()
        pq.write_table(
            table,
            parquet_path,
            compression="zstd",
            compression_level=9,
            row_group_size=8192,
        )
        write_seconds = time.perf_counter() - started
        payload = parquet_path.read_bytes()
        part = {
            "path": str(parquet_path),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "row_count": row_count,
            "min_event_epoch_us": min(columns["event_epoch_us"]),
            "max_event_epoch_us": max(columns["event_epoch_us"]),
        }
        index = SearchIndex(root / "search.sqlite3")
        started = time.perf_counter()
        index.index_parquet(part, parquet_path)
        index_seconds = time.perf_counter() - started

        query = {
            "database": "example_app",
            "table": f"table_{target_row % 10:02d}",
            "keyword": "2571634",
            "operations": ["UPDATE"],
        }
        started = time.perf_counter()
        hit = index.candidate_blocks(
            [part],
            query,
            start_epoch_us=part["min_event_epoch_us"],
            end_epoch_us=part["max_event_epoch_us"],
        )
        hit_ms = (time.perf_counter() - started) * 1000
        started = time.perf_counter()
        miss = index.candidate_blocks(
            [part],
            {**query, "keyword": "999999999999"},
            start_epoch_us=part["min_event_epoch_us"],
            end_epoch_us=part["max_event_epoch_us"],
        )
        miss_ms = (time.perf_counter() - started) * 1000

        class Bucket:
            def get_object(self, _key: str, byte_range: tuple[int, int]):
                start, end = byte_range
                return SimpleNamespace(
                    read=lambda: payload[start : end + 1],
                    headers={"ETag": '"benchmark-etag"'},
                )

        reader = OssRangeReader(
            Bucket(),
            "cold/part.parquet",
            len(payload),
            "benchmark-etag",
        )
        parquet = pq.ParquetFile(reader)
        groups = [entry["row_group_id"] for entry in hit["entries"]]
        selected = parquet.read_row_groups(
            groups,
            columns=["event_epoch_us", "table_name", "after_json"],
        )
        print(
            json.dumps(
                {
                    "rows": row_count,
                    "parquet_bytes": len(payload),
                    "row_groups": parquet.num_row_groups,
                    "index_bytes": index.stats()["size_bytes"],
                    "write_seconds": round(write_seconds, 3),
                    "index_seconds": round(index_seconds, 3),
                    "hit_ms": round(hit_ms, 3),
                    "hit_blocks": len(groups),
                    "miss_ms": round(miss_ms, 3),
                    "miss_blocks": len(miss["entries"]),
                    "range_requests": reader.request_count,
                    "range_bytes": reader.bytes_read,
                    "range_ratio": round(reader.bytes_read / len(payload), 4),
                    "selected_rows": selected.num_rows,
                },
                ensure_ascii=False,
            )
        )


if __name__ == "__main__":
    main()
