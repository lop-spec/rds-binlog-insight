# Lossless cache and columnar ingestion candidate

Status: **candidate collector implementation, not deployed and not a tenfold result**.
The branch collector defaults to bounded native Arrow chunks with an explicit NDJSON rollback setting. It also contains a default-off compressed raw-cache download/replay path and durable merged chunk publication. The deployed service, production settings and release image are unchanged.

## Implemented entry points

- `app.raw_cache.RawCacheWriter`: exclusive creation, bounded independent ZSTD1/LZ4 frames, per-frame and full raw SHA256, a complete footer, file/directory sync on Linux and explicit physical budget. `resume(old, new, ...)` copies only verified frames into a new exclusive file. Its budget includes the retained old cache. It never truncates or deletes old data.
- `inspect_cache`: read-only verification. Only a truncated trailing frame/footer is recoverable; bad framing, size, codec, identity or SHA is an error.
- `iter_raw`: bounded verified-frame replay. Full consumption is necessary to verify the footer. `download_raw_cache` feeds each network chunk once to `RawCacheWriter` and the native CRC64/SHA stream, resumes only from complete verified frames using an original-byte HTTP Range, and publishes with a no-overwrite hard link. An open-ended `206` must start at the requested offset and end at the known EOF with matching total size and usable `Content-Length`; a complete `200` with a usable `Content-Length` must match the known original size. Invalid response framing fails before a cache is created or mutated. Complete, recovery and forensic assets share a count and physical budget. Cache completion is **not** RDS CRC verification, parser completeness, archive completion or query visibility.
- `parser/raw_cache_reader.go`: strict `io.ReaderAt` over complete `RDSRAW1` caches. It validates header identity/declared original size, frame bounds and SHA, footer count/final SHA and trailing bytes before parsing; ZSTD and LZ4 expansion are bounded to one declared frame. Raw/gzip/zstd/tar inputs use the existing stream detector and ZIP keeps bounded random access. Raw-cache mode is selected by actual flag presence: `--raw-cache-source-id` and `--raw-cache-expected-size` are mandatory together, size must be non-negative, and identity is bound to the ordinary source ID.
- `EventStorage.ingest_arrow_file`: Arrow IPC file input to exactly the same normalization, sort, 47-field Parquet writer and catalog calculation as `ingest_ndjson_file`. The IPC schema must equal the ordered 41-field `PARSER_JSON_COLUMNS` contract, including types, nullability and metadata; missing, reordered, unknown, duplicate or incompatible fields fail closed. Producer-unavailable audit fields remain present as typed NULL columns. Each collector chunk supplies an expected row count, which is checked against the decoded input before Parquet publication. Under `_part_body_lock`, all private Parquet bodies are replaced and file-synced, their body directories and the events directory are synced, and only then is the matching `.version` file and directory made durable. The bodies and version fence are rechecked before part rows, SQLite catalog truth, file event count and job/event progress commit in one SQLite transaction. A failed transaction publishes none of them, and identical restart/retry is idempotent. The compressed catalog store remains a recoverable SHA/revision-fenced mirror, not a second atomically committed database.
- `tools.benchmark_raw_cache`: frozen local input, exclusive outputs, complete decompressed SHA oracle, per-codec write/replay CPU and wall time. CLI restricted to disposable cloud fixtures pending real-input authorization/resource gates. Results are cache mechanism measurements only; file length is not an end-to-end physical I/O measurement.
- `tools.parser_contract_fixture`: real synthetic MySQL STATEMENT/ROW binlogs, independent event framing/CRC32 checks, original SQL, complete legacy/candidate output, strict Arrow chunk manifest/ACK transcripts and corrupt-stream rejection evidence. It replays valid ZSTD/LZ4 raw caches through NDJSON, direct Arrow and Arrow chunk transports, and mutates header/magic/codec/identity/declared size/expected-size argument/frame marker/frame SHA/footer marker/footer length/final SHA/trailing bytes. Every invalid cache must fail before NDJSON/Arrow publication or the first chunk ACK. Uses owned CI containers only, never RDS or production credentials. Keeps failed evidence.
- `parser/`: unpromoted, reproducible Go decoder candidate recovered from the `feat/binlog-rows` slim-source lineage. It has both a direct bounded 41-field Arrow IPC file mode and a bounded Arrow chunk/manifest/ACK mode. Its digest-pinned build, source hashes and provenance are in `parser/README.md`; the branch Dockerfile now tests and builds this source into a candidate image instead of copying the legacy binary. No release image has been generated or selected in production.

## Native decoder compatibility gate

The original full custom Go wrapper source is still absent from known history. A later, reproducible `parser-slim` derivative was recovered from commit `abd2619`; its module, Go 1.26.5 version and go-mysql v1.16.0 dependency match the legacy executable build identity. The derivative omitted seven full-output fields. The candidate restores those fields, including the exact independently recovered `schema_version_id` formula, and tests them against frozen legacy output rather than treating source similarity as compatibility proof.

Before replacement, the candidate must preserve event identities, raw SQL bytes, all emitted fields, transaction/GTID/XID context, row images, decimals, unsigned values, null/empty/binary/JSON/time values, schema identity and error behavior. Use legacy differential checks plus independent binlog fixtures; a confirmed legacy defect is not a correct golden value. In go-mysql v1.16.0, both `ParseReader` and `ParseSingleEvent` suppress missing-table-map failure. `ParseSingleEvent` also treats a partial header returning EOF as completion. The candidate instead uses bounded `io.ReadFull` framing followed by `Parse(raw)`, validates event size/position and CRC through the parser, requires first-event FDE, rejects unknown/duplicate-FDE input, and checks every rows event against an observed TableMap ID. Contract flags additionally require GTID and ROW TableMap coverage. Partial headers/bodies, bad CRC/size/position, missing FDE/TableMap/GTID and unknown events must all exit nonzero without publishing a chunk.

The Python Arrow adapter alone did not remove the parser's JSON encoding. The Go candidate supports `--arrow-output`, which writes typed columns directly from `outputEvent` without invoking `encoding/json` for the transport. Embedded `before_json`, `after_json` and `columns_json` remain contract fields by design. The single IPC file is uncompressed, row/estimated-byte batched, hard-limited to 128 MiB, staged exclusively, file-synced and published without overwrite; all parse or publication failures remove staging output.

The branch collector now uses the same native encoder through `--output-dir --chunk-format arrow`. Each IPC chunk is bounded by rows, decoded estimate and physical bytes. The strict `parser-chunk-v1` manifest carries sequence, path, rows, physical bytes and decoded bytes; the collector verifies the exact field set, staging ownership, name, order, size and bounds before sending a sequence-bound `parser-chunk-ack-v1` ACK. ACK means that the collector has taken ownership inside a bounded outstanding slot, not that Parquet metadata or source progress is durable. With the default prefetch of one, at most two chunks are owned; a slot is released only after the caller finishes that chunk's body. Failure or cancellation terminates the parser and removes owned lane artifacts. The source chunk is deleted only after transform/publication succeeds; deletion failure is logged and aborts the file. Restart admission removes only orphaned artifacts for the same inactive lane.

`RDS_BINLOG_PARSER_TRANSPORT=ndjson` explicitly rolls back to the retained NDJSON chunk path. The bridge omits the new `--chunk-format` flag in this mode so an older deployed parser can still run. New parser builds emit versioned and bounded NDJSON manifests; the collector temporarily accepts only the exact old three-field NDJSON manifest for binary rollback compatibility.

`RDS_BINLOG_RAW_CACHE_ENABLED=1` selects compressed cache only for a new download that has no reusable `.rawcache`, raw-cache recovery, `.binlog` or `.binlog.part` asset. The default is `0`; flag changes never migrate old assets. `RDS_BINLOG_RAW_CACHE_MAX_RATIO` defaults to `0.385` and is a hard cache/recovery/forensic physical budget, not an observed compression ratio. Async download progress coalesces SQLite writes, but `finish()` propagates any persistence failure and prevents the file state from advancing to downloaded. None of these candidate paths is enabled in production.

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
row JSON and rendering pseudo SQL with `FROM_BASE64(...)`. Owned-cloud run
`36055924930`, job `107822690187`, commit `e93b3f3` passed the independent
source-value and complete 47-field decoder contract. Artifact `10832806078`
(SHA-256 `b2c8dc27fdd7b6f944e66d8ec0052a0387fa7c85d432a9da3faa483e88038d4e`)
was separately rechecked without importing the application/contract code: all
framing/CRC, known values, identity, transaction, schema, position, type and
catalog gates passed; only the three intended body/pseudo-SQL fields differed
from legacy. All nine corrupt/context variants exited nonzero with zero
published NDJSON files. That run predates the direct Go Arrow producer and its
artifact does not contain the candidate binary, so it cannot prove either of
those newer claims.

Against the retained `36055924930` source binlogs, the Go 1.26.5 producer emitted 5 STATEMENT and 6 ROW records as direct Arrow. Its exact 41-field values/order matched the same candidate's NDJSON, and both paths materialized identical 47-field Parquet rows and catalogs; the ROW independent value oracle had zero failures. Each of the nine corrupt/context variants was also run through both NDJSON chunk and direct Arrow modes: all 18 attempts failed nonzero and published zero files. Isolated cloud run `36061087704` reproduced these gates at commit `0c1f2af`; independent artifact verification also rehashed the included Linux binary and checked Arrow/Parquet values, schema, order, catalog and failure artifacts without importing application code. That run predates the collector Arrow chunk protocol in this working tree and therefore does not verify the new protocol.

The synthetic STATEMENT/ROW files are only 785/1604 bytes. Measured ZSTD cache
ratios were 0.743/0.507 and LZ4 0.811/0.596, with full SHA round trips. These
small-file results neither pass the <0.385 resource gate nor characterize
production compression; do not extrapolate throughput from them.

## Resource and durability gates

1. Freeze real file identities, raw SHA, input sizes and the deployed baseline before same-cohort measurements. CI synthetic fixtures do not represent production compression/CPU cost.
2. At the earlier 83.11 MB/s target and unchanged 32 MB/s parent write cap, **all** source cache, Parquet, index/merge/cache/WAL and background physical writes must fit. A raw-cache-only ratio below 0.385 is necessary, not sufficient.
3. Enforce CPU, process-tree memory, I/O, total space and network request/byte limits externally. The cache has its own per-frame/input/output checks; these do not bound DuckDB, the decoder, OS page cache, or the whole service tree.
4. IPC input is producer-owned and must be uncompressed/bounded at the producer. Reader file/decoded-byte checks are not protection from every malformed IPC allocation; keep the producer/reader inside the enforced memory group.
5. Do not publish pre-CRC frames. Preserve ordered retirement, source generation/revision fencing, `_part_body_lock`, `file_archive_complete()`, ACK/pause and FINAL/tombstone semantics. Parser ACK is only bounded transfer ownership; chunk row-count validation, body and `.version` durability, merged SQLite publication, archive completion and ordered source progress remain later gates. Do not release the durable source until the existing archive gate permits it.
6. Directory-sync, version-replace, metadata rollback, catalog-mirror fallback, concurrent body-lock and restart-idempotency unit tests cover specific boundaries only. They are not whole-service or host power-loss acceptance. Windows unit tests do not certify Linux durability.

## Verification

Local contracts:

```text
(cd parser && go test ./... -v)
python -m unittest tests.test_raw_cache tests.test_columnar_input tests.test_parser_buffering tests.test_pipeline_capacity tests.test_pipeline_stage_timing tests.test_parser_contract_fixture tests.test_parser_contract_oracle tests.test_public_config -v
```

The cloud-only parser job builds the candidate with Go 1.26.5, saves the exact
binary plus hashes for every Go source file, and builds the root Dockerfile only
through its non-publishing `parser-builder` target. Both paths use identical
`-trimpath -buildvcs=false` flags so repository metadata cannot make the comparison
spuriously differ. It byte-compares that staged binary with the directly built
candidate and runs the checksum smoke probe. The
job then generates legacy NDJSON and candidate NDJSON/direct Arrow from the same
owned MySQL files, and runs the
41-field transport, 47-field storage, value and decoder differential gates.
Historical run `36074182917` emitted three-row native Arrow chunks, retained the exact strict manifests, sequence-bound ACKs, completion stderr and chunk bytes, and replayed them through the 47-field oracle. It retained all nine negative-stream stderr files and zero-publication checks for NDJSON chunk, direct Arrow and Arrow chunk modes, but predates the raw-cache Go reader, compressed-download pipeline and merged durable publication.

The current source is bound to commit `475625bd7669a2fd3c809e82cc0a3a5c3ae89b9f` by successful non-publishing workflow run [`36084140215`](https://github.com/lop-spec/rds-binlog-insight/actions/runs/36084140215), job `107912177351`, artifact `10842658055`. The downloaded ZIP is 4,194,179 bytes with 343 members and 11,384,547 expanded bytes; SHA256 is `a17a9194e1176f6387a9daff261cb585a066f090cdf279a7844d9ac3ee78b74c`. Its 10,469,538-byte Linux amd64 candidate SHA256 is `5e193982dfc1a6b58218b9952cc84046c0611c8f4b4726bb670bdae1d71cd80b`, byte-identical to the root Docker builder output and the final local Linux build.

An independent verifier that does not import application, parser, fixture or oracle implementations reports `PASS`: verifier SHA256 `33896a4c630cc4470773a5fc5f4eec02e7f299cdd758fe00a6bca3346babca55`, report SHA256 `bfac53c74dfffb991e78523b77aef4cebb1f7d48b4d5bcfe52cef9cec44af867`. It rehashed all members and committed parser sources, checked the ELF/build identity, strict 41-field transport and 47-field storage/catalog/`.version`/body-lock seals, completed all 12 STATEMENT/ROW × ZSTD/LZ4 × transport raw-cache replays, and independently reconstructed all 12 malformed-cache cases across three transports: 36/36 exited nonzero with zero publication and zero ACK.

Use the existing `build-image.yml` dispatch with `parser_contract=true` and all other inputs false for credential-free cloud fixture generation. It never publishes images/releases. The normal release regression also includes the cache/columnar tests.

The commit-bound candidate has compressed download integration, native Go raw-cache replay and bounded durable metadata commits. Local Go 1.26.5 formatting, module verification, tests and vet pass. Rebuilt Windows/Linux amd64 binaries have SHA256 `6f0a526a7ad936cbedd93fd5d47ce6f1f1c20c6b39c2309f915669bd48fc347f` and `5e193982dfc1a6b58218b9952cc84046c0611c8f4b4726bb670bdae1d71cd80b`; the cloud artifact independently confirms the Linux digest, while neither binary is a release artifact. The final Python discovery run passed 877 tests in 154.085 seconds with six existing skips; log SHA256 is `4940870694bef0d0de4cc0abdb5d38cc310eb73fb33f5c90c8a649e3ada4e048`.

This evidence is a small synthetic semantic/mechanism gate, not throughput or production coverage. No image/release was published and no deployment, restart, configuration change or pressure experiment occurred. Real ECS pressure/deployment still requires the agreed isolation budget, stop thresholds and explicit authorization. Only same-cohort end-to-end >10x, sustained net catch-up, full body/page oracles, 30-day 19/20 and whole-service recovery satisfy the final goal.
