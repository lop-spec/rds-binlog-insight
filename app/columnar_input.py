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
    names = source.schema.names
    if len(set(names)) != len(names) or set(names) - set(columns):
        raise ValueError("Arrow parser schema contains duplicate or unknown fields")
    if source.num_record_batches > 4096:
        raise ValueError("Arrow parser file exceeds record-batch count budget")
    for fld in source.schema:
        if fld.type != schema.field(fld.name).type:
            raise ValueError(f"Arrow parser field type mismatch: {fld.name}")

    def batches():
        consumed = 0
        for index in range(source.num_record_batches):
            batch = source.get_batch(index)
            # Missing columns have exactly the legacy read_json NULL semantics.
            arrays = [batch.column(batch.schema.get_field_index(f.name))
                      if f.name in names else pa.nulls(batch.num_rows, type=f.type)
                      for f in schema]
            normalized = pa.RecordBatch.from_arrays(arrays, schema=schema)
            consumed += normalized.nbytes
            if consumed > max_bytes:
                raise ValueError("Arrow parser decoded columns exceed byte budget")
            yield normalized

    return pa.RecordBatchReader.from_batches(schema, batches())
