# Lossless cache and columnar ingestion candidate

Status: **mechanism/contract implementation, not enabled in the collector, not a tenfold result**.
The default parser/download/publish pipeline, Docker parser binary and production settings are unchanged.

## Implemented entry points

- `app.raw_cache.RawCacheWriter`: exclusive creation, bounded independent ZSTD1/LZ4 frames, per-frame and full raw SHA256, a complete footer, file/directory sync on Linux and explicit physical budget. `resume(old, new, ...)` copies only verified frames into a new exclusive file. Its budget includes the retained old cache. It never truncates or deletes old data.
- `inspect_cache`: read-only verification. Only a truncated trailing frame/footer is recoverable; bad framing, size, codec, identity or SHA is an error.
- `iter_raw`: bounded verified-frame replay. Full consumption is necessary to verify the footer. Cache completion is **not** RDS CRC verification, parser completeness, archive completion or query visibility.
- `EventStorage.ingest_arrow_file`: Arrow IPC file input to exactly the same normalization, sort, 47-field Parquet writer and catalog calculation as `ingest_ndjson_file`. Unknown/duplicate fields and incompatible types fail closed. Missing fields receive the old JSON reader's NULL/default semantics. The collector does not select this entry point yet.
- `tools.benchmark_raw_cache`: frozen local input, exclusive outputs, complete decompressed SHA oracle, per-codec write/replay CPU and wall time. CLI restricted to disposable cloud fixtures pending real-input authorization/resource gates. Results are cache mechanism measurements only; file length is not an end-to-end physical I/O measurement.
- `tools.parser_contract_fixture`: real synthetic MySQL STATEMENT/ROW binlogs, independent event framing/CRC32 checks, original SQL, and complete legacy native output. Uses owned CI containers only, never RDS or production credentials. Keeps failed evidence.

## Native decoder compatibility gate

The original custom Go wrapper source is not present in the known trees/history/archive. The checked-in native executable remains the baseline, not a reproducible source build.

Before replacing it, the new source decoder must match event identities, raw SQL bytes, all emitted fields, transaction/GTID/XID context, row images, decimals, unsigned values, null/empty/binary/JSON/time values, schema identity and error behavior against both the legacy executable and independent binlog fixtures. Upstream `ParseReader` silently continues after its internal missing-table-map error; use a strict loop and explicit coverage checks. Unsupported events must not become apparent success.

The Arrow adapter alone does not remove the legacy parser's JSON encoding. Do not pipe legacy JSON through an extra converter and call that the completed native-columnar architecture.

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
python -m unittest tests.test_raw_cache tests.test_columnar_input -v
```

Use the existing `build-image.yml` dispatch with `parser_contract=true` and all other inputs false for credential-free cloud fixture generation. It never publishes images/releases. The normal release regression also includes the cache/columnar tests.

Continue implementation at the strict source decoder/columnar producer and pipeline wiring; then coalesce durable metadata commits without removing mirror/index service completion gates. Real ECS pressure/deployment still requires the agreed isolation budget, stop thresholds and explicit authorization. Only same-cohort end-to-end >10x, sustained net catch-up, full body/page oracles, 30-day 19/20 and whole-service recovery satisfy the final goal.
