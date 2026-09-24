"""Strict Arrow IPC input for the shared event normalization path.

Only producer-owned, uncompressed IPC files belong here; the isolated producer
must also enforce its process-tree memory cap. File size is not a replacement
for that cap. This adapter does not claim to decode binlog or remove the legacy
producer's JSON serialization by itself.
"""
from __future__ import annotations

from contextlib import ExitStack
from pathlib import Path

import pyarrow as pa

MAX_COLUMNAR_BYTES = 128 * 1024 * 1024
TYPES = {"VARCHAR": pa.string(), "BIGINT": pa.int64(),
         "UBIGINT": pa.uint64(), "INTEGER": pa.int32()}


def parser_schema(columns: dict[str, str]) -> pa.Schema:
    return pa.schema([(name, TYPES[kind]) for name, kind in columns.items()])


def open_parser_arrow(resources: ExitStack, path: Path, columns: dict[str, str],
                      *, max_bytes: int = MAX_COLUMNAR_BYTES) -> pa.RecordBatchReader:
    if not 0 < path.stat().st_size <= max_bytes:
        raise ValueError("Arrow parser file exceeds input byte budget or is empty")
    mapped = resources.enter_context(pa.memory_map(str(path), "r"))
    source = pa.ipc.open_file(mapped)
    schema = parser_schema(columns)
    if not source.schema.equals(schema, check_metadata=True):
        raise ValueError(
            "Arrow parser schema must match the exact ordered 41-field contract"
        )
    if source.num_record_batches > 4096:
        raise ValueError("Arrow parser file exceeds record-batch count budget")

    def batches():
        consumed = 0
        for index in range(source.num_record_batches):
            batch = source.get_batch(index)
            consumed += batch.nbytes
            if consumed > max_bytes:
                raise ValueError("Arrow parser decoded columns exceed byte budget")
            yield batch

    return pa.RecordBatchReader.from_batches(schema, batches())
