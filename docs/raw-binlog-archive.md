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

## Asynchronous indexed queries

With the archive switch enabled on the existing `indexer`, each worker pass also
indexes one committed raw file, newest first. The collector does not call or wait
for this path. The worker reads OSS sequentially, verifies size/SHA256/CRC64, then
uses the existing native decoder. Collection-owned downloads are not retained.
No new queue, service, business database index, or CPU allocation is introduced.

`index/raw-events-v1.sqlite3` is a rebuildable derived store. Ordered keys support
an exact instance/database/table/time scope and the existing schema-verified
single-column primary-key types. Before and after primary-key values both point
to the same immutable event. A registry or source-descriptor change invalidates
coverage. Missing/unsupported historical schema remains an error for key lookup.
Event identities and all returned fields match the raw decoder; compressed
payload blocks share repeated content across at most 256 rows / 4 MiB, with a
32 MiB single-event ceiling. Only page rows are returned, not truncated fields.
This is a covering index, not a keys-only format: budget its measured disk growth.

The single lane inherits the indexer's hard limits and low scheduling priority.
It adjusts its duty cycle to measured host CPU idle time, yields for queued/running
queries, I/O or memory pressure, and reserves 20 GiB of free disk (plus source
staging space before a download). Pressure recovery uses hysteresis. Unavailable
pressure measurements pause rather than assume spare capacity; each reason is
logged and exposed in the existing worker status. A pause beyond five minutes
releases the work unit; incomplete files remain invisible and retry later.
These controls do not establish a measured zero-impact guarantee.

Publication occurs only after EOF and source revalidation. Committed intermediate
blocks are not queryable; restart retries reclaim them in bounded batches. Source
retirement reclaims derived rows/blocks only, without deleting source manifests
or archives. Registered missing files and incomplete overlapping files subtract
from coverage. Envelope-proven irrelevant tables can be pruned. Opaque files use
decoded timestamp bounds only after complete indexing, never epoch-to-infinity.
The certificate covers the registered source catalog, not undiscovered history.
Query-time catalog reads exclude raw manifests whose known bounds do not overlap
the requested interval before fetching their summary/descriptor JSON. Legacy and
unknown bounds are retained conservatively; invalid raw bounds fail closed.
Reversed or unknown legacy timestamps may use the existing completed Parquet
manifest for **time pruning only**: `done` without error, source SHA256 and fully
downloaded size, unique nonempty part identities, valid part hashes/ranges, and
summed part rows equal to the positive completed source count are all required.
Absent any evidence, retain an all-time unknown gap. This neither reads Parquet/OSS
nor serves those legacy rows, modifies the original timestamps, or certifies
undiscovered sources. Each recovery/unknown-gap decision logs its reason.
The interval selector still reads the full registered catalog; no TTL cache or
stale coverage certificate is introduced.

The recommended UI requires one instance and complete database/table names.
Primary-key history is the most selective lookup. Time shortcuts end at the most
recent continuous certified interval, clip to it without crossing holes, and
never substitute wall-clock time when no interval exists. A custom interval is
narrowed before submission to the most recent continuous certified segment
**inside the requested window**, with the actual dates written back into the form
and the old/new ranges explained. No overlap blocks submission, not a move to a
different date. Execution revalidates coverage; no raw-scan fallback is allowed.
Result captions identify the executed interval and state that other dates were
not searched, including when the result is empty.
Binlog keyword, status, account and connection filters read the same covering
index, with the raw decoder's matching semantics for available fields. A requested
execution-status filter requires a stored status certificate for every structural
candidate (scope/time/operation/transaction/PK), including candidates beyond the
requested page. Missing status or a legacy index without this certificate raises
`INDEX_FILTER_UNAVAILABLE` before decoding payloads, not a successful empty page.
The API does not silently remove an explicitly submitted filter. Following the
user-authorized fast-query policy, the Binlog UI clears and hides execution-status,
account and connection inputs before both GET and POST serialization, and visibly
states that it searches recorded row changes without these unavailable audit
fields. Other sources retain their filters. No binlog row is inferred to have
`execution_status=success`, and keywords are never guessed to be primary keys.
Existing index files remain readable without the status filter; a write/rebuild
adds the nullable status column without fabricating values for old rows.
Ordered payload cursors stop after a page plus one lookahead; they do not count all
matches or build another keyword index.
Two query-local decoded blocks are cached (at most 64 MiB), discarded at request
end; misses load the committed SQLite payload, never an archive. Broad negative
keywords may still traverse all scoped indexed rows and require measurement.

The UI has no raw-scan mode. Binlog GET and POST default to indexed-only, and POST
rejects explicit `indexedOnly=false`. Other source types retain their existing route.
Indexed queries and cached details do not initialize OSS. Execution has a
50-second deadline and tasks a 55-second submission budget, leaving room for
result delivery within the **60-second acceptance target**. Busy execution slots
reject new indexed tasks instead of queueing them behind long scans. A timeout
or busy response is a failure, not evidence that the target was achieved. The
32 MiB result budget and pagination depth 2,000 remain; failure returns no partial
page. No historical source data, checksums or legacy indexes are removed.

Before production adoption, measure original-file throughput and tail latency
with/without the worker under identical limits, index bytes/event, ongoing source
rate, and backlog. For the current acceptance target, measure submission through
visible result delivery within 60 seconds, retaining the user's filters and
restricting queries to certified indexed intervals. Include positive, negative,
pagination and concurrent requests. Small fixtures and deadline settings do not
establish this production target. Earlier 100× / 1 TB measurements likewise must
not be claimed from the fixture.
Disable the archive switch **only on the indexer** to stop this new work without
changing collection. The older raw-aware query image can still read every original
archive; keep the derived file for investigation/reuse, rather than deleting data.

## Legacy raw engine safety and compatibility

The following bounded engine remains for internal compatibility and recovery;
it is not the Binlog GET/POST indexed-only query route or a UI scan option.

- Snapshot the union of legacy physical source IDs and eligible raw IDs once at
  execution. One task reads sequential batches of at most **16 files and 8 GiB**;
  more candidates no longer cause refusal. A single file over 8 GiB still fails.
  The temporary plan spills after 1 MiB and releases its metadata reader before I/O.
- Each batch retains the 180-second deadline and one concurrent raw decoder;
  raw staging remains limited to 512 MiB. Batched queries retain at most 32 MiB
  of result payload and pagination depth 2,000 across the whole task. Merge by
  event ID and global descending order before slicing the requested page.
  Cancellation or any batch failure prevents a successful partial result.
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
python -m unittest tests.test_raw_binlog tests.test_binlog_query_batches tests.test_raw_event_index tests.test_index_worker tests.test_pipeline_capacity tests.test_query_preflight -v
node --test tests/test_indexed_range.cjs
python -m tools.benchmark_index_layout --raw-event-index --rows 16384
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
