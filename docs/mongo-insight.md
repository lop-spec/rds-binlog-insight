# MongoDB analytics adapter

MongoDB is a source inside the existing **分析洞察** workspace. MySQL routes, facts and ranking versions are unchanged. The adapter combines typed command shapes, fixed-window slow-log snapshots, native command counters and role-matched performance series. Rankings are descriptive candidates, never a claim that correlation proves causality.

## Data and query contracts

- DDS slow records use `QueryTimes` in milliseconds and `ExecutionStartTime` in UTC. Values are normalized using BSON types. Command identification does not depend on JSON key ordering.
- `command` and `suboperation` are independent populations. A top-level update and its internal operation must never be added as independent requests. getMore is a separate command.
- Missing CPU, read bytes and waits remain unknown. Cost deltas are calculated only when the field is complete in both observed populations. Slow-log counts are not full workload counts.
- Native `metrics.commands` deltas are cut across processes, roles and counter resets. Counters use actual observation intervals; missing historical minute counts cannot be reconstructed from slow logs.
- Normalized facts and minute rollups are immutable ZSTD Parquet files under the existing data root. The single visibility pointer is a window manifest. Original source records retain legitimate duplicate multiplicity. ClickHouse is the serving index; `FINAL` resolves retry duplicates, and each read checks revision row counts against the manifests before claiming completeness.
- The highest-duration representative is selected with its actual record, not an unrelated lexicographic sample. The longest-operation panel is independent of growth Top N, so a late, non-growing slow query remains available as a counterexample.
- The native full-count panel and optional connected-service client aggregate panel remain separate from slow-log rankings. No application coverage is implied when the latter is empty.

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
    "families": ["messages", "request_records"]
  }
]
```

The credential file is separate, mode 0600, with `username`, `password`, `authSource`. This deployment adapter requires the privately configured approved read-only username in `admin`, authenticates it before sampling, and rejects reported roles outside read/monitor permissions. It constructs a direct, read-only node connection from separate fields, never accepts an arbitrary URI, and never falls back outside the configured instance host allowlist. Credentials are not returned by the status API.

Three bounded background lanes handle slow-log windows, CMS points and native samples. Slow windows are five minutes with a three-minute lag, checkpointed only after successful publication. Recently closed windows are refreshed for provider delay. Pagination changes, timeout, parser error, missing metric, node failure and disabled branches are visible in normal logs and `/api/mongo/status`. A transient archive failure preserves the local source and does not advance its publication pointer.

The ClickHouse tables retain seven days from ingestion. Parquet source versions are retained as recovery assets; automatic local source eviction is not enabled in this release. Monitor their disk use through the deployment inventory. This limitation does not permit deleting unarchived source files. Existing OSS lifecycle applies to objects uploaded under the existing archive prefix; no new bucket lifecycle is installed.

## Endpoints

- `GET /api/mongo/status`: configured instances, collector lane status, metric capability list and optional private replay definitions.
- `GET /api/mongo/analytics`: `instance`, `startEpochUs`, `endEpochUs` (exclusive), optional `baselineStart` for an equal-length disjoint baseline, `role`, `kind`, `command`, `namespace`, `metric`, `order`, `limit`.
- Supported orders: `duration_growth`, `count_growth`, `scan_growth`, `cpu_growth`, `read_growth`, `performance`, `count`, `max_latency`, `duration_total`.
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

## Rollback

Disable the Mongo entry and roll back the web image to the retained previous image/container. Keep source Parquet, manifests and additive ClickHouse tables. There are no business-database rollback commands. Do not replace the existing MySQL collectors or re-create unrelated containers during this rollout.
