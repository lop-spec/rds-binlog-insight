# MongoDB analytics adapter

MongoDB is a source inside the existing **分析洞察** workspace. MySQL routes, facts and ranking versions are unchanged. The adapter combines typed command shapes, fixed-window slow-log snapshots, native command counters and role-matched performance series. Rankings are descriptive candidates, never a claim that correlation proves causality.

## Data and query contracts

- DDS slow records use `QueryTimes` in milliseconds and `ExecutionStartTime` in UTC. Values are normalized using BSON types. Command identification does not depend on JSON key ordering.
- `command` and `suboperation` are independent populations. A top-level update and its internal operation must never be added as independent requests. getMore is a separate command.
- Missing CPU, read bytes and waits remain unknown. Cost deltas are calculated only when the field is complete in both observed populations. Slow-log counts are not full workload counts. Measured counts, durations, scanned documents, CPU and bytes remain visible even without a complete baseline, with field coverage for partial values. Missing source/baseline is never labeled “no growth”.
- The workspace offers CPU, IOPS utilization, **update-cache utilization**, scanned documents, lock waits and average response. Update-cache utilization is a stock (not per-command memory ownership): its curve is shown while causal/cost ranking stays unavailable without component evidence; use the dedicated cache/ticket investigation. Selecting a metric always requests `order=correlation`; there is no independent ordering control. It orders by signed Pearson r (descending) between each command's minute execution-time series and the selected role-matched metric. This is a timing association, not a measurement of per-command CPU/IO resource consumption. A complete baseline or cost growth is not required. Uncomputable coefficients remain null, after valid coefficients; ties use stable identities, never a hidden cost ranking. If none are computable, `ranking.available=false` and a reason are returned and logged, without changing the metric, scope or order.
- Lock waits (`LockWaits`) use existing native `serverStatus.globalLock.currentQueue.total`: queued operations waiting to acquire a lock, **not wait duration**, concurrent read/write tickets, or write-concern delay. The documented `readers + writers` sum is used only when both are present and total is absent. Use the last native snapshot within each minute; missing fields/minutes, conflicting same-time readings and ambiguous same-role nodes remain gaps. Do not backfill or extrapolate native history. Reference: [MongoDB serverStatus globalLock](https://www.mongodb.com/docs/manual/reference/command/serverStatus/#mongodb-serverstatus-serverstatus.globalLock.currentQueue).
- Legacy API-only cost/growth order values remain compatible. If a growth ranking has no usable comparison, the API explicitly returns `requested_order`, effective `order` and `order_reason`, and logs the change. It ranks measured cost (or count if that field is absent), not an arbitrary fingerprint hash. The response discloses this change without silently changing the query window; the simplified workspace never requests these legacy orders. Current-window correlation needs complete current source/metric coverage, not a historical baseline.
- Native `metrics.commands` deltas are cut across processes, roles and counter resets. Counters use actual observation intervals; missing historical minute counts cannot be reconstructed from slow logs. The panel shows command QPS timelines and baseline rate changes only when both windows contain at least two valid intervals. Coverage is shown separately; observed rates are not extrapolated into whole-window counts.
- Normalized facts and minute rollups are immutable ZSTD Parquet files under the existing data root. The single visibility pointer is a window manifest. Original source records retain legitimate duplicate multiplicity. ClickHouse is the serving index; `FINAL` resolves retry duplicates, and each read checks revision row counts against the manifests before claiming completeness.
- The highest-duration representative is selected with its actual record, not an unrelated lexicographic sample. The longest-operation panel is independent of growth Top N, so a late, non-growing slow query remains available as a counterexample.
- Quick time ranges align to the collector's latest completed slow-log window; explicitly selected custom/replay ranges are never shifted. Unsupported CMS metrics are logged and retried after six hours without discarding supported series. TCMalloc known-free memory supports native field variants and excludes unmapped pages.
- The native full-count panel and optional connected-service client aggregate panel remain separate from slow-log rankings. No application coverage is implied when the latter is empty.
- Namespace lock counters come from the native `top` command in the same node sample as the command counters and are differenced across the same process epoch, node and role. They are scoped to collections, not statements: they answer which collection was busy, never which statement, and their microseconds are lock hold time including waits, not CPU time. A negative delta discards the whole interval as a counter reset. Intervals that do not fall entirely inside the requested window are dropped rather than scaled, and interval count and observed seconds are reported as coverage instead of being extrapolated. The per-namespace list is truncated by lock time with the truncation disclosed; sharded family totals stay complete. A `top` failure skips the namespace lane for that cycle and leaves the command counter lane intact. Cumulative `top` totals are held in memory for differencing only and are never written into the persisted native payload.

## Cache/ticket investigation (1.36.0)

The Mongo metric menu adds **更新缓存占比 %** (`WtCacheUpdatesUsage`). The existing metrics lane also fetches DDS update-memory, application-thread eviction, read/write ticket gauges, queues and disk/network gauges using each response's `ValueFormat`; it does not guess a CMS metric name. These are persisted under telemetry kind `pressure`, without a new table or business-database DDL. History fetch markers have a metric-set version so existing bounded hourly recovery revisits old hours for the added series. A completed fetch is not a guarantee that every metric was available.

`GET /api/mongo/investigate` uses a configured instance, an exact whole-minute window of at most 30 minutes, a disjoint equal-length baseline and an optional configured `logSource`. It never shifts the requested incident window. The explicit UI button reads five-second DDS performance, bounded checkpoint/election/shutdown records, audit status, retained slow-record source costs, historical native snapshots and observed Pod-IP bindings. It does not connect to the business database, enable profiling/audit, change a parameter, kill an operation or scan collections. Pod bindings are **not** Pod CPU/OOM/restart or network history, and no such exclusion is claimed.

The timeline separates update memory, total cache and dirty memory; application evictions; used/available write tickets; available read tickets; waiting writes; latency; CPU, IOPS, disk/network utilization and request rate. Ticket capacity is observed used+available, not a fixed 128. Continuous zero availability **and positive queue** over at least two adjacent samples is saturation evidence. Earlier eviction plus rising update memory is a mechanism candidate, never proof of a configured 10% threshold or of the initial producer. Same-bucket events cannot establish ordering. DDS samples mark period start; the ordinary minute analysis shifts them to period end explicitly, without mixing CMS period-end gauges.

Retained raw reads are capped at 50,000 records, 64MiB and 12 seconds per current/baseline population; immutable Parquet is streamed in batches. Missing windows, unreadable records and budget exhaustion invalidate cost deltas. Outer commands and internal suboperations are not added together. Read bytes, CPU, cache waits, execution queues and dirty bytes retain field coverage. Read bytes are neither physical cloud-disk reads nor net cache growth. Slow records do not cover fast writes; audit-disabled history, old snapshots and hidden nodes remain evidence gaps. A 90-second task budget, at most 14 control-plane calls, one investigation at a time and a 60-second/eight-entry result cache bound repeated clicks. A failed optional lane stays a gap, not a normal/zero reading.

SLS is opt-in per instance using an additive `logSources` array in the existing private `mongo-instances.json`. No credentials go in it and the API accepts only its source IDs, never arbitrary project, logstore or query strings. Examples (deployment values stay private):

```json
"logSources": [
  {"id":"app","label":"应用超时","project":"fixture-project","logstore":"app-logs",
   "endpointTokens":["fixture.invalid:3717"]},
  {"id":"http","label":"接口并发","project":"fixture-project","logstore":"app-stdout",
   "kind":"http","pathToken":"getMsgList","maxConcurrent":3}
]
```

The SLS adapter uses signed **GET `/logstores/{logstore}`** (the `/shards/lb` endpoint is for ingestion and rejects this GET), rejects redirects, reads at most five 100-row sample pages per investigation and 4MiB per response. For timeout sources it additionally runs two bounded system-field SQL aggregates (totals and five-second histogram, maximum 400 rows), without requiring `content.doc_value`. Aggregate completeness, raw-sample completeness and pagination completeness are separate; exact totals can be available while raw details remain sampled. It parses bounded raw logs instead of assuming `content` has SQL `doc_value`; only counts, timestamps, source labels and sanitized diagnostics leave the adapter. No customer body, URL query text or credentials are returned. HTTP overlap subtracts a second from both ends to provide a conservative observed concurrency lower bound; incomplete/unfinished requests cannot be reconstructed. More overlap than configured concurrency demonstrates an observed protection gap, not its code-level cause. With no mapping the UI explicitly says SLS was not queried; no source is auto-discovered across projects.

`storage.data.txnBytesDirty` is a **signed net dirty-byte change** and may be a negative BSON Long when an operation frees update memory; it is neither a malformed slow record nor gross bytes written. 1.36.1 preserves this field's sign without weakening other nonnegative cost guards. It fixes a live-acceptance incompatibility that rejected such raw records and a legacy SLS query-path mismatch.

Release validation must cover signed BSON dirty-byte changes, the official SLS GET path, independently complete aggregates/partial samples, period semantics, missing gauges, conflicting nodes/epochs, constant expensive work versus growth, waiting victims, truncated sources, fixed SLS scopes, raw-body redaction, HTTP overlap and frontend stale-result invalidation. Use credential-free fixtures; do not publish incident logs or production identifiers. No new migration is needed, and rolling back only the application image keeps additive telemetry readable by the previous version.

## Deployment

Use the cloud-built release image. Do not create a local release image. Install only the additive Insight tables before enabling the configuration:

```bash
python -m app.mongo_store --data-dir /data --migrate
```

This creates `mongo_rollups` and `mongo_telemetry` in the existing configured ClickHouse database. It does not create any MongoDB collection/index, enable profiling/audit or modify a MongoDB parameter. The web process checks the schema; it does not install it implicitly. Missing configuration/schema is logged and leaves MySQL working.

Create `/data/mongo-instances.json` with the existing deployment's instance/region and approved read-only node list. Preserve all other configuration files.

```json
[
  {
    "instanceId": "dds-example",
    "label": "MongoDB",
    "region": "cn-example-1",
    "enabled": true,
    "backend": "clickhouse",
    "nodes": ["dds-example1.mongodb.cn-example-1.rds.aliyuncs.com"],
    "port": 3717,
    "readonlyUsername": "example_monitor",
    "credentialsFile": ".credentials/mongo-readonly.json",
    "families": ["messages", "request_records"],
    "topNamespaceLimit": 150,
    "slowlogSource": "das"
  }
]
```

The credential file is separate, mode 0600, with `username`, `password`, `authSource`. This deployment adapter requires the privately configured approved read-only username in `admin`, authenticates it before sampling, and rejects reported roles outside read/monitor permissions. It constructs a direct, read-only node connection from separate fields, never accepts an arbitrary URI, and never falls back outside the configured instance host allowlist. Credentials are not returned by the status API.

Three bounded realtime lanes handle slow-log windows, CMS points and native samples; the native lane additionally differences the `top` command into per-namespace lock intervals. Two additional recovery lanes fill recent 48-hour DDS/CMS history, with realtime progress taking priority: at most one five-minute DDS window per 60-second cycle and one CMS hour per 120-second cycle, per configured instance. Recovery discovers immutable missing windows and persists hourly CMS fetch markers. It never rewinds the live slow-log checkpoint or invents historical native snapshots. The initial lower time bound remains pinned while recovery is pending, so a slow repair cannot age out the requested baseline; the recovery budget is capped at seven days with an explicit expiry log. Failed windows/hours back off for ten minutes while other gaps can progress. `/api/mongo/status` and the query response expose recovery progress and failures; completed CMS fetches do not imply every metric was available. Slow windows are five minutes, read 90 seconds after they end (DAS since 1.34.8, see below), checkpointed only after successful publication. A window may hold up to 2,000 pages (200,000 records); a larger one is skipped by the live lane (`mongo_slowlog live_skip reason=over_capacity`, listed under `slowlog_skipped` in `/api/mongo/status`) instead of blocking every later window, and a live lane more than six hours behind jumps to the present (`reason=lag_exceeds_limit`). Skipped windows are not marked done: the bounded history lane sees them as missing and retries them with backoff. Recently closed windows are refreshed for provider delay. Pagination changes, timeout, parser error, missing metric, node failure and disabled branches are visible in normal logs and `/api/mongo/status`. A transient archive failure preserves the local source and does not advance its publication pointer.

The ClickHouse tables retain seven days from ingestion. Parquet source versions are retained as recovery assets; automatic local source eviction is not enabled in this release. Monitor their disk use through the deployment inventory. This limitation does not permit deleting unarchived source files. Existing OSS lifecycle applies to objects uploaded under the existing archive prefix; no new bucket lifecycle is installed.

## Endpoints

- `GET /api/mongo/status`: configured instances, collector lane status, metric capability list and optional private replay definitions.
- `GET /api/mongo/analytics`: `instance`, `startEpochUs`, `endEpochUs` (exclusive), default baseline `[start-(end-start), start)`; optional `baselineStart` for an explicit equal-length disjoint baseline, `role`, `kind`, `command`, `namespace`, `metric`, `order`, `limit`.
- Default/workspace order: `correlation` (the judgement control under "更多条件" defaults to it; `attribution` remains selectable). The result page shows the metric curve and the ranking table; notes and all supporting panels (longest commands, native counters, memory, namespace lock time, client aggregates, coverage) sit in one folded "更多明细". Legacy API orders: `duration_growth`, `count_growth`, `scan_growth`, `cpu_growth`, `read_growth`, `performance`, `count`, `max_latency`, `duration_total`, `scan_total`, `cpu_total`, `read_total`.
- `namespace_top` in the analytics response aggregates namespace lock intervals for windows up to 180 minutes, the same grain already used for cost buckets. Longer windows report `unavailable: window_exceeds_180_minutes` rather than a partial or coarser total. `topNamespaceLimit` (1–1000, default 150) bounds the per-sample namespace list; family totals are unaffected by it.
- `POST /api/ingest/mongo-commands`: bearer token required from `/data/.credentials/mongo-ingest-token`. Accepts bounded, idempotent connected-service command aggregates; no raw query text is accepted by this contract.

Client aggregate envelope fields are `instance`, `service`, `process_epoch`, stable `batch_id`, `lost`, and `rows`. Each row has `start_us`, `end_us`, `namespace`, `command`, optional normalized `fingerprint`, `count`, `failed`, and `duration_us`. Counts represent driver command attempts in the connected service, not business transactions. Whole-batch retries must reuse the same batch ID. Driver monitoring must be registered in the application's common MongoClient creation path; the database collector cannot install a listener in another service. This release supplies the ingestion contract, not a claim that every application is connected.

## Fixed replay

Keep real deployment identifiers and raw logs outside the public repository. The replay tool reads private fixture and case files and uses the same normalizer, source publication, serving adapter and ranking code as the UI:

```bash
python tools/backtest_mongo_insight.py --source-dir /private/fixtures --cases-file /private/cases.json --data-dir /private/replay-data --backend parquet --output /private/replay-result.json
```

Use `--backend clickhouse --archive` only with the intended deployment environment. The tool does not query the business database. `/data/mongo-replays.json` may contain private UI replay entries with `id`, `label`, `instance`, `start_us`, `end_us`, `baseline_start_us`, `metric`, `order`; the algorithm contains no incident-specific names or time thresholds.

Assertions cover duplicate multiplicity, BSON types, order-sensitive sorts, command/suboperation separation, missing metrics, counter resets, late queries, highest-duration sample provenance, source/index completeness and positive case Top 3 targets. A case whose root cause was not closed remains explicitly unresolved after a technically successful replay.

For queries longer than 180 minutes, cost/count statistics use coarser buckets. Minute correlation is unavailable at that grain rather than interpolated. Narrow the window for minute resource overlap. The computed runtime scope is the executions represented in the fetched source windows; executions starting before those source windows cannot be inferred. Index and collection coverage are distinct from having all business requests.

## Cache diagnosis workspace (1.36.2)

Selecting `WtCacheUpdatesUsage` makes the existing **Start investigation** button run the incident lane directly. It does not produce the unsupported memory correlation ranking. The exact whole-minute window and disjoint equal baseline are retained (maximum 30 minutes); an episode reaching the selected end is labelled clipped, not recovered. Five-second charts preserve missing-point breaks.

`mongo_workload.indexed_workload` reuses the existing complete minute rollups to show family totals, command templates, read/CPU costs, field coverage, source IPs and historical Pod bindings. Bounded raw records enrich representative samples only: exhausted raw sampling cannot erase complete indexed totals or suppress their comparison. `observed_delta` compares recorded values when both indexed windows are complete; `delta` is available only when both cost fields are fully known. Incomplete fields are expressly not total workload growth or cache ownership. The default business list is ordered by recorded read bytes, never called a per-command cache contribution score.

The first result screen shows the cache/ticket mechanism, business family costs and command/source rows, application log summaries, and unresolved audit/producer scope. Clicking a command opens the existing detail drawer with redacted shape, projection/paging metadata, sample time and unique observed historical Pod binding; candidates or today's Pod are not historical proof.

The SLS selector defaults to explicit **Auto-link configured logs**, querying only instance-registered sources. Choosing **No logs** performs no SLS reads. Each source retains separate coverage and population; HTTP conservative overlaps are clipped to the selected time interval, not allowed to borrow concurrency from before it. A shorter window must not inherit a later peak from a longer incident window.

Acceptance must operate the actual workspace primary button with a real incident's instance, role, exact current/baseline windows and cache metric. Verify visible five-second saturation, business costs and source drilldown; separately test no-log selection and configured-log selection, and a longer window covering recovery. API responses or a menu option alone do not meet acceptance. No real incident IDs, customer literals or timestamps are baked into this algorithm.

## Rollback

Disable the Mongo entry and roll back the web image to the retained previous image/container. Keep source Parquet, manifests and additive ClickHouse tables. There are no business-database rollback commands. Do not replace the existing MySQL collectors or re-create unrelated containers during this rollout.

## Window alignment (1.29.22)

When a slow-log source window overlapping the requested period has no published manifest, the query moves to the nearest same-length period whose windows all exist, within 6 hours either way (5-minute steps, earlier first at equal distance, never ending in the future). The baseline moves by the same offset. A period whose baseline is also complete is preferred; when none exists the nearest one with a complete current period is used and `adjusted.baseline_complete` is `false`. A request whose current period is already complete is never moved, even when its baseline is incomplete.

The response carries `adjusted = {reason, requested_start_us, requested_end_us, requested_baseline_start_us, shift_us, baseline_complete}` (`null` when nothing moved) and the effective `start_us`, `end_us`, `baseline_start`. When no complete period exists nearby the request is served as before (partial coverage, unchanged notice) and a warning is logged. The page rewrites the time fields and the baseline start and switches the range selector to custom.

## DAS source and live tail (1.34.8–1.34.11)

On 2026-10-28 an incident window held 31,663 slow records in five minutes (about 105 per second). DDS `DescribeSlowLogRecords` allows 30 requests a minute of 100 records, so the live lane needed about eleven minutes per five-minute window, fell behind for good, and every analysis ending "now" was moved back by 15 minutes or more. Nothing in the pipeline could fix that while the source was DDS.

**Source.** DAS `DescribeSlowLogRecords` (`InstanceId`=the dds instance, `StartTime`/`EndTime` in ms) serves the same records: 61 records, then a whole window of 2,677, compared field for field with DDS (command text included) were equal after `das_record`, which also keeps DDS's field order (a revision hashes the JSON text; 1.34.10 had another order, so windows it read got a new revision once), so a window keeps its revision whichever API read it, and both APIs returned equal totals 3 and 9 days back. A request takes about 0.3 s; 30 back-to-back requests saw no throttling. **Pages stay at 100 records in DAS's default order**: DAS accepts up to 5,000 a page and an `OrderBy`, but either one makes it cut every command to 2,048 characters, which does not parse (1.34.8 asked for 2,000 records, 1.34.9 ordered by `QueryTimeSeconds`; both failed every DAS read until 1.34.10, and such a failure now reads the window from DDS). The default order is newest second first; paging two seconds of 910 and 938 records in pages of 10 returned every record once. `QueryId` names the statement shape and repeats, so a slice is accepted only when its distinct records (time, duration, client, text) add up to DAS's `TotalRecords`; otherwise, or when `TotalRecords` changes between pages (records still arriving in the newest seconds), it is split in halves (`mongo_slowlog das_pages_unstable … splitting`, INFO) and a single second is read up to three times before the window reads DDS. A slice of more than 20 pages is first split at whole seconds. `[StartTime, EndTime)` is exclusive at the end at second resolution (two adjacent one-second slices add up to the two-second slice). A window above 200,000 records is over capacity exactly as before. Any other DAS failure reads the window from DDS instead (`mongo_slowlog das unavailable … reading DDS instead`, `slowlog_source: dds_fallback: …` in the status). The live and history lanes share one request pace, the live tail has its own (at most one request per 0.1 s each). `"slowlogSource": "dds"` in `mongo-instances.json` keeps the old source and turns the tail off.

**Freshness.** Measured the same day: the newest record DAS shows started 2–12 s earlier, and a minute's count stops changing about 20 s after the minute ends (statements are stamped with their start, so a long one arrives after it finished). The live lane therefore reads a window 90 s after it ends (was 180 s), re-reads one of the last three closed windows every tick for late long statements, runs every 20 s and catches up several windows per tick after a restart.

**Live tail** (`app/mongo_tail.py`, one thread per instance, `MONGO_SLOWLOG_TAIL=0` turns it off). Every 20 s it reads from the first minute it does not vouch for yet up to now (on start: the last 15 minutes, never minutes the live lane has published), normalizes the records like `publish` and keeps the rollups of each source minute in memory (no raw records, nothing on disk). It vouches for every minute before `complete_through` = scan start − 60 s, down to the minute; statements that ran longer than about 40 s may come only with the published window. A tail whose last scan is more than 90 s old, or that does not reach back to the first minute an analysis needs, is ignored.

**Query.** When windows of the requested period have no manifest and the tail covers all of them, they are read from the tail and the rest from ClickHouse (a window published in between is read from ClickHouse only). The start stays; an end after `complete_through` is cut back to it with `adjusted = {reason: "source_delay", requested_start_us, requested_end_us, requested_baseline_start_us, truncated_us, shift_us: 0, baseline_complete: null}`, and a baseline that ended where the period started (前一个等长窗口) is moved to stay adjacent and equally long. Coverage counts such windows as collected and adds `tail_windows`/`tail_records`; the page says how many windows came from the live read. Only when the tail cannot cover the period does the 1.29.22 nearest-window search run. `/api/mongo/status` gives each collector `slowlog_tail` (fresh, `complete_through_us`, minutes, records, failures) and `slowlog_data_through` (the tail's end when fresh, else the last published window), which the quick ranges now end at.
