# Lossless cache and columnar ingestion candidate

Status: **mechanism/contract implementation, not enabled in the collector, not a tenfold result**.
The default parser/download/publish pipeline, Docker parser binary and production settings are unchanged.

## Implemented entry points

- `app.raw_cache.RawCacheWriter`: exclusive creation, bounded independent ZSTD1/LZ4 frames, per-frame and full raw SHA256, a complete footer, file/directory sync on Linux and explicit physical budget. `resume(old, new, ...)` copies only verified frames into a new exclusive file. Its budget includes the retained old cache. It never truncates or deletes old data.
- `inspect_cache`: read-only verification. Only a truncated trailing frame/footer is recoverable; bad framing, size, codec, identity or SHA is an error.
- `iter_raw`: bounded verified-frame replay. Full consumption is necessary to verify the footer. Cache completion is **not** RDS CRC verification, parser completeness, archive completion or query visibility.
- `EventStorage.ingest_arrow_file`: Arrow IPC file input to exactly the same normalization, sort, 47-field Parquet writer and catalog calculation as `ingest_ndjson_file`. Unknown/duplicate fields and incompatible types fail closed. Missing fields receive the old JSON reader's NULL/default semantics. The collector does not select this entry point yet.
- `tools.benchmark_raw_cache`: frozen local input, exclusive outputs, complete decompressed SHA oracle, per-codec write/replay CPU and wall time. CLI restricted to disposable cloud fixtures pending real-input authorization/resource gates. Results are cache mechanism measurements only; file length is not an end-to-end physical I/O measurement.
- `tools.parser_contract_fixture`: real synthetic MySQL STATEMENT/ROW binlogs, independent event framing/CRC32 checks, original SQL, complete legacy/candidate output, and corrupt-stream rejection evidence. Uses owned CI containers only, never RDS or production credentials. Keeps failed evidence.
- `parser/`: unpromoted, reproducible Go decoder candidate recovered from the `feat/binlog-rows` slim-source lineage. Its digest-pinned build, source hashes and provenance are in `parser/README.md`; it is not copied into the application image or selected by the collector.

## Native decoder compatibility gate

The original full custom Go wrapper source is still absent from known history. A later, reproducible `parser-slim` derivative was recovered from commit `abd2619`; its module, Go 1.26.5 version and go-mysql v1.16.0 dependency match the legacy executable build identity. The derivative omitted seven full-output fields. The candidate restores those fields, including the exact independently recovered `schema_version_id` formula, and tests them against frozen legacy output rather than treating source similarity as compatibility proof.

Before replacement, the candidate must preserve event identities, raw SQL bytes, all emitted fields, transaction/GTID/XID context, row images, decimals, unsigned values, null/empty/binary/JSON/time values, schema identity and error behavior. Use legacy differential checks plus independent binlog fixtures; a confirmed legacy defect is not a correct golden value. In go-mysql v1.16.0, both `ParseReader` and `ParseSingleEvent` suppress missing-table-map failure. `ParseSingleEvent` also treats a partial header returning EOF as completion. The candidate instead uses bounded `io.ReadFull` framing followed by `Parse(raw)`, validates event size/position and CRC through the parser, requires first-event FDE, rejects unknown/duplicate-FDE input, and checks every rows event against an observed TableMap ID. Contract flags additionally require GTID and ROW TableMap coverage. Partial headers/bodies, bad CRC/size/position, missing FDE/TableMap/GTID and unknown events must all exit nonzero without publishing a chunk.

The Arrow adapter alone does not remove the legacy parser's JSON encoding. Do not pipe legacy JSON through an extra converter and call that the completed native-columnar architecture.

## Confirmed legacy value-loss blocker (2026-09-20)

The owned MySQL 8.0 fixture in run `35509934369` (commit `19ab949`) contains
`VARBINARY X'00FFFE'`. The unchanged native binary emits that value as a JSON
string containing replacement characters: UTF-8 bytes `00efbfbdefbfbd`, not
`00fffe`. It occurs in the INSERT after-image and both UPDATE images. Source
binlog SHA and both compressed-cache round trips still match; this loss is
inside the legacy parser output, not the new cache or Arrow ingestion.

`tools.parser_contract_oracle` now independently checks all known mutation
images, including binary bytes, unsigned integers, decimal/float/time tags,
JSON values and NULLs. It also ingests the real legacy outputs through both
NDJSON and Arrow and compares all 47 fields, order, types and catalogs. **Both
transports agree while the independent byte oracle fails.** The fixture job
must fail at this separate gate until a lossless native decoder fixes it;
there is no expected-failure exemption or success downgrade. Failed reports,
raw input and outputs are retained. A binary-safe candidate may represent
bytes as `{"$bytes_base64":"AP/+"}` or the length-bearing recovered-source
form `{"$binary_base64":"AP/+","$length":3}`.

The current candidate detects binary columns from TableMap collation metadata
(and also fails safe for invalid UTF-8 strings), preserving arbitrary bytes in
row JSON and rendering pseudo SQL with `FROM_BASE64(...)`. On the frozen
`35510454952` fixture, a local Go 1.26.5 rebuild passed the independent value
oracle: all six ROW records materialized through the 47-field storage schema;
all identity, transaction, schema, position and catalog fields equaled legacy;
only `before_json`, `after_json` and derived `sql_text` changed; STATEMENT output
was fully equal. All nine corrupt/context variants exited nonzero with zero
published files. This is local candidate evidence only until the owned cloud
MySQL contract produces a retained passing artifact; it does not alter the
existing binary or enable a production parser.

The synthetic STATEMENT/ROW files are only 785/1604 bytes. Measured ZSTD cache
ratios were 0.743/0.507 and LZ4 0.811/0.596, with full SHA round trips. These
small-file results neither pass the <0.385 resource gate nor characterize
production compression; do not extrapolate throughput from them.

## Resource and durability gates

1. Freeze real file identities, raw SHA, input sizes and the deployed baseline before same-cohort measurements. CI synthetic fixtures do not represent production compression/CPU cost.
2. At the earlier 83.11 MB/s target and unchanged 32 MB/s parent write cap, **all** source cache, Parquet, index/merge/cache/WAL and background physical writes must fit. A raw-cache-only ratio below 0.385 is necessary, not sufficient.
3. Enforce CPU, process-tree memory, I/O, total space and network request/byte limits externally. The cache has its own per-frame/input/output checks; these do not bound DuckDB, the decoder, OS page cache, or the whole service tree.
4. IPC input is producer-owned and must be uncompressed/bounded at the producer. Reader file/decoded-byte checks are not protection from every malformed IPC allocation; keep the producer/reader inside the enforced memory group.
5. Do not publish pre-CRC frames. Preserve ordered retirement, source generation/revision fencing, `_part_body_lock`, `file_archive_complete()`, ACK/pause and FINAL/tombstone semantics. Do not release the durable source until the existing archive gate permits it.
6. Ordinary unit crash/tail tests are not whole-service or host power-loss acceptance. Windows unit tests do not certify Linux durability.

## Verification

Local contracts:

```text
(cd parser && go test ./... -v)
python -m unittest tests.test_raw_cache tests.test_columnar_input tests.test_parser_contract_fixture tests.test_parser_contract_oracle -v
```

The cloud-only parser job builds the candidate with Go 1.26.5, saves build
identity, generates both legacy and candidate output from the same owned MySQL
files, runs the independent value/47-field differential, and retains all
negative-stream stderr and hashes.

Use the existing `build-image.yml` dispatch with `parser_contract=true` and all other inputs false for credential-free cloud fixture generation. It never publishes images/releases. The normal release regression also includes the cache/columnar tests.

Continue implementation at the strict source decoder/columnar producer and pipeline wiring; then coalesce durable metadata commits without removing mirror/index service completion gates. Real ECS pressure/deployment still requires the agreed isolation budget, stop thresholds and explicit authorization. Only same-cohort end-to-end >10x, sustained net catch-up, full body/page oracles, 30-day 19/20 and whole-service recovery satisfy the final goal.
