# Reproducible native binlog decoder candidate

This directory is an unpromoted decoder candidate. It is not wired into the
production image or service.

Source recovery baseline:

- repository branch: `feat/binlog-rows`
- first source commit: `abd2619a0d1471feaed56c42afd0d5a2db557cd9`
- recovered `parser-slim/main.go` SHA-256: `49070c794c1745fec183d678e7d1a43b243ab1e038790806bfa9f59ac30bcaa8`
- recovered `go.mod` SHA-256: `1c712a24ec54844116ad73ed7903c51fd66491ed067bd739633a341f55f73ff7`
- recovered `go.sum` SHA-256: `c39582031d8010dddf869d5eae6ea0f65ef09334d71d7bc2be5045751bc92acf`

The recovered source is a later slim derivative, not a byte-for-byte copy of
the legacy full decoder. The full-output fields omitted by that derivative are
restored here and are checked against frozen legacy output. The exact
`schema_version_id` formula was recovered from the legacy binary symbol
`main.schemaVersionID` and independently confirmed against the frozen ROW
fixture:

```
sha256(lower(database_name) + NUL + lower(table_name) + NUL + columns_json)
```

The event reader intentionally does not use `BinlogParser.ParseReader` or
`ParseSingleEvent`: it reads every 19-byte header and complete body with
`io.ReadFull`, verifies size and position, then calls `BinlogParser.Parse`.
Checksum verification remains enabled in go-mysql. FDE must be first, unknown
event types and duplicate FDE are rejected, and each rows event must reference
an observed TableMap ID. Contract runs additionally require GTID and (for ROW)
TableMap coverage.

`--arrow-output PATH` bypasses the NDJSON encoder and writes the parser's exact
41-field transport schema as an uncompressed Arrow IPC file. The eleven
collector/audit-only fields are typed nulls; the storage layer still adds the
six source/time fields and applies the same normalization to produce the final
47-field rows. Record batches are bounded by row count and estimated decoded
bytes (defaults: 4096 rows and 32 MiB), while the complete IPC file has a hard
128 MiB default and maximum matching the current reader contract. Unsigned
values that cannot fit a signed transport field fail closed.

The Arrow file is written exclusively to `PATH.part`, file-synced, then published without overwrite by a same-directory hard link; Linux directory metadata is synced before and after staging-link removal. Parse, checksum, context, bound, close, or publication failure removes staging output and never publishes a partial final file. `--arrow-output` deliberately produces one bounded IPC file for candidate contracts.

The collector interface is `--output-dir DIR --chunk-format arrow`. It writes one independently readable Arrow IPC file per chunk and emits exactly one `parser-chunk-v1` JSON manifest after atomic publication:

```json
{"protocol":"parser-chunk-v1","format":"arrow-ipc-file-v1","sequence":0,"path":"/absolute/staging/source-000000.arrow","rows":4096,"bytes":123456,"decoded_bytes":789012}
```

The parser waits for an exact sequence-bound ACK before advancing:

```json
{"protocol":"parser-chunk-ack-v1","sequence":0,"status":"ok"}
```

Rows, decoded estimate and physical IPC size are independently limited; the configured maximum cannot exceed 128 MiB. Manifests and ACKs reject missing, unknown or trailing JSON fields/data. Each chunk uses exclusive `.part` creation, file sync and no-overwrite publication. Manifest/ACK failure removes the unaccepted final; parse, bound or close failure removes partial output. Existing final or `.part` paths fail closed rather than being deleted or overwritten.

`--chunk-format ndjson` remains an explicit rollback transport. It now uses the same no-overwrite publication and emits versioned `ndjson-v1` manifests, while retaining line ACKs for the existing rollback reader. A complete encoded NDJSON record is checked before writing, so neither one oversized record nor a boundary crossing can publish an over-limit chunk.

The collector owns an ACKed file and may prefetch only within its separate outstanding-slot budget; parser ACK does not mean Parquet publication, metadata progress or archive completion. This candidate protocol is selected by default only in this branch and is not deployed in production.

Build and test with the digest-pinned builder without publishing an image:

```sh
docker build --file parser/Dockerfile \
  --output type=local,dest=parser/build parser
```

Only the existing CI may produce release images or packages. Passing the
synthetic parser contract proves decoder compatibility for that fixture; it is
not production deployment, throughput, recovery, or 30-day query acceptance.
