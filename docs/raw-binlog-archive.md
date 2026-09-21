# Original binlog archive (v1.28.49)

Raw downloads persist display progress at most once every five seconds (plus
final completion). Resume still uses the actual partial-file length; streaming
CRC64/SHA-256, fsync, and the verified downloaded-state commit are unchanged.
This bounds unnecessary FULL SQLite progress commits; it is not itself a
claim that metadata persistence is the dominant throughput bottleneck.

The collector may run with `RDS_BINLOG_RAW_ARCHIVE=1`. The existing OSS credential,
retention policy and file-discovery/retry state remain authoritative. No business
MySQL write or DDL is required. The mode requires OSS; it never silently falls
back to full JSON expansion when archive verification fails.

## Durable states

1. Download with source size/CRC64 validation and local SHA256.
2. Traverse v4 event envelopes and TableMap events. Do not decode row images.
3. Upload immutable original and gzip sidecar, each content-addressed. Verify
   remote size, SHA256 metadata and OSS CRC64. Read queries verify sidecar SHA256.
4. Commit `raw_binlog_archives` under the existing SQLite FULL durability policy.
5. Verify both objects before releasing the downloaded source and marking done.

`done` now can mean either legacy parsed/archived Parquet or original+lite index.
The UI distinguishes these. Header-event counts are NOT reported as decoded row
counts. A crash before manifest commit reuses verified objects; a crash after it
verifies the manifest before cleanup. Never delete the legacy Parquet/indices.

Per collector, two archive lanes and eight download lanes share a rolling
admission budget of eight source files / 4 GiB (one oversized file alone). The legacy expansion pipeline
keeps its original three download lanes and four files / 2 GiB budget. A slow earlier download does not
block a ready later file. Each archive lane runs the unchanged lightweight scan
in a short-lived child process, avoiding contention with download threads for
the service's Python interpreter. Children inherit the existing container CPU
and memory limits; timeout is 180 seconds, and temporary JSON is capped at
256 MiB per lane. Worker errors preserve the source and never commit a manifest;
conservative-index diagnostics are relayed to the service log. No row expansion,
checksum, OSS validation, or query budget is weakened.

Pause stops new admission; downloaded files remain
recoverable. Discovery continues through the existing retained-file scheduler.

## Query safety and compatibility

- Union legacy physical source IDs and eligible raw IDs BEFORE enqueue and again
  before execution: at most **16 files and 8 GiB**. No implicit full scan on refusal.
- Raw queries: one concurrent decoder, 180-second deadline, 512 MiB staging file,
  32 MiB retained result payload, pagination depth at most 2,000.
- Sidecars select whole GTID transaction regions, including FDE/table-map/context.
  Statement SQL and unknown/compressed payloads remain conservative candidates.
  Anonymous/no-GTID and unusual prefix files retain a whole-file range.
- Table predicates use paired database/table names, not a cross-product. Include
  both header and immediate/original GTID commit timestamps in time envelopes.
- Range GET must return the exact Content-Range/length. No full-object fallback.
- Existing Parquet event IDs and links stay valid. The native parser's ID includes
  a global output ordinal, which is unsafe after pruning. New raw events have a
  versioned ID derived from source file + GTID + transaction-local output ordinal;
  their locator identifies the original transaction range. IDs stay stable across
  query narrowing and event-detail lookup. The legacy subquery excludes raw-owned
  source files to avoid returning both ID schemes for a partially converted file.
- Missing primary-key schema is an explicit error, not an empty result.

## Verification

```
python -m unittest tests.test_raw_binlog tests.test_pipeline_capacity tests.test_query_preflight -v
python tools/raw_binlog_probe.py /path/to/existing/closed.binlog
python tools/raw_binlog_acceptance.py http://127.0.0.1:8769/api/status
```

The native full-prefix oracle checks all emitted data fields against range decode,
with explicit raw-v1 identity normalization and a separate narrow detail lookup.
It prints counts/hashes only. `raw_binlog_archive_benchmark.py` additionally tests
one immutable OSS roundtrip without changing serving metadata or deleting input;
its timing EXCLUDES source download and is not an end-to-end capacity claim.

Delivery requires at least 15 minutes / 100 completed original-file samples and
an estimate <=24 hours after subtracting ongoing source production and retaining
20% throughput headroom. Source production uses the larger of the busiest recent
complete UTC day and the latest hour. The acceptance command also checks capacity
including known unavailable files, but this does NOT demonstrate recoverability.
Unavailable sources and unregistered historical date gaps remain separate data
completeness gates; directory `done` or a capacity forecast cannot close them.

## Rollback

Turn `RDS_BINLOG_RAW_ARCHIVE=0` on this version to resume the existing full parser;
keep the raw-aware query code deployed. Do not downgrade to an old image that
cannot read the newly archived original objects. Do not delete manifests, old
Parquet, credentials, or original OSS objects during rollback.
