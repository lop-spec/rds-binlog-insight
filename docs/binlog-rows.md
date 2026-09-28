# Binlog rows by table (1.29.0)

Binlog table queries (`source=binlog` + database + table, optional keyword / operations / primary key)
are served from ClickHouse `insight.binlog_rows_v1` when `RDS_BINLOG_ROWS_SERVING=1` on `insight`.

## Layout

| Object | Role |
|---|---|
| `binlog_rows_v1` | Row events, `PARTITION BY (event_date, cityHash64(lower(db), lower(table)) % 8)`, `ORDER BY (instance, db, table, time)`, one `text` index on `lower(concat(before_json, ' ', after_json, ' ', transaction_id, ' ', source_file_name))`, 62-day TTL, storage policy `binlog_rows` (OSS + 12 GiB local cache). |
| `binlog_statements_v1` | Original SQL (`row_query`) once per distinct text (first 64 KiB), keyed by `cityHash64`. Rows only keep `row_query_hash`. |
| `binlog_rows_stage_v1` + two materialized views | Null-engine entry point feeding both tables. |
| `binlog_rows_buf_*` | Local per-lane buffers: load → verify row count → move → manifest. A failed load only truncates the buffer. |
| `binlog_rows_files_v1` | Ingest manifest. Coverage = consecutive ingested files; every other file inside a requested window is reported as a gap (`source_missing` or `pending`), plus `not_collected_yet` after the newest collected file. |

The `binlog_rows` storage policy is defined in the ClickHouse server config (`config.d/low-resource.xml`),
so the tables stay ALTER-able (inline dynamic disks cannot be altered on 26.3). Create tables with
`python -m app.clickhouse_migrate --binlog-rows-tables`.

## Query semantics

- Keywords: whitespace-separated, case-insensitive, AND/OR. Pure `[a-z0-9]` terms are whole-token
  matches served by the text index; ASCII terms with separators (`shop_id`) use the index for their
  tokens and then check the exact substring; terms without any ASCII token (Chinese) scan the table's
  rows in the window and fail explicitly after 100 s instead of returning a partial result.
- Primary key: needs the table in the exact-index schema registry (row image key, e.g. `@1`).
- Results carry `covered_intervals`, `coverage_gaps` and `coverage_note`; the UI keeps the user's range
  and lists uncovered segments instead of narrowing to one interval.
- Details use `ch:` locators (instance, db, table, event time, event id) resolved by a primary-key read.

## Ingestion (`binlog-rows-worker`)

`python -m app.binlog_rows_worker` (compose profile `binlog-rows`).

- `RDS_BINLOG_ROWS_LIVE_LANES` (default 2): newest-first lanes; each re-ranks after every file.
- `RDS_BINLOG_ROWS_BACKFILL_WINDOWS`: `instance|startISO|endISO;...`, one extra lane per window (max 4 lanes total).
- `RDS_BINLOG_ROWS_EXTERNAL_PARQUET_WINDOWS`: Parquet-backed files in these windows are left to an external backfill.
- Raw archives: 4-way ranged OSS download (~210 MB/s), SHA256/CRC64 check, native parser stdout streamed
  into ClickHouse (`input()` + JSONEachRow, same field mapping and fallback `event_id` as the DuckDB path).
- Parquet parts: `s3()` into the buffer, per-part row counts must equal the metadata.
- Pauses (always logged as `BINLOG_ROWS_PAUSED`): ClickHouse memory above `RDS_BINLOG_ROWS_CH_MEMORY_LIMIT_GIB`,
  and for window lanes also collector lag above 20 minutes. Status: `data/index/binlog-rows-worker-status.json`.

Rows without a table (transaction boundaries: XID / BEGIN / COMMIT, ~41% of production records) are not
kept; DDL is kept and its original text goes through the statements table (`sql_kind = ORIGINAL`).

### One parser, two modes (`parser-go/`, v3 since 1.29.6)

The image builds `/app/tools/binlog-parser` from `parser-go/`. The collector runs it in full mode; the rows
worker runs the same binary with `--slim` (override the path with `RDS_BINLOG_ROWS_PARSER`).

- Full mode is byte-identical to the former committed v2 binary (sha256 `b1b2fc7d…`): table identity
  (`table_map_id`, `schema_version_id`) and transaction timing (`header_epoch_us`, `commit_epoch_us`,
  `txn_*`) feed the exact-index registry and the analytics page.
- `--slim` leaves those two groups out (embedded nil pointers, so JSON omits them), omits pseudo SQL, column
  metadata and base64 SQL, cuts `row_query` to 65536 code points (what the store keeps) and does not write
  transaction-boundary records while still advancing the output sequence, so every emitted `event_id`, row
  image, position and `row_query` prefix is identical to full mode.
- Acceptance (3 production binlogs, 524 MB each): full output sha256 equal to v2, `--slim` output sha256
  equal to the 1.29.1–1.29.5 slim parser. Parser CPU in slim mode ≈ 60% of full mode.

Measured on the production host (4 vCPU): one 524 MB binlog ≈ 56 s end to end with the slim parser
(74 s full parser streamed, 132 s with on-disk chunks); download 2.5 s with 4 ranged streams.

## Commit safety (1.29.16)

Each file is moved behind an inflight marker (`data/index/binlog-rows-inflight/<file_id>.json`, with the
buffer's `(event_date, bucket)` partitions). A marker still present when the same file is committed again
means an earlier attempt may have left rows: `commit()` purges them first (the start-up recovery does the
same). A failed per-bucket INSERT is stopped on the server (`KILL QUERY ... SYNC` on its query id) before the
purge; a purge that starts while the INSERT still runs misses its later parts (`mysql-bin.095753` kept 77,876
duplicate rows that way before 1.29.16).

## Analytics (1.29.16)

Prod binlog has been archived raw since 2026-09-22, so the Parquet analytics index has no prod input. The
analytics page (`/api/analytics`, source binlog) is served from two ClickHouse aggregates when the row store
holds files of the requested instance in the window; otherwise (other instances, older days) the Parquet
index answers as before, and a ClickHouse error falls back to it with `BINLOG_ROWS_ANALYTICS_FAILED` logged.

| Object | Key | Content |
|---|---|---|
| `binlog_agg_5m_v1` | instance, 5-minute bucket, db, table, operation, `fp` | events, executions (RowsEvents: `row_index = 1`), payload bytes, slow events / exec time (QueryEvent `exec_time`), first/last time, normalized and sample SQL |
| `binlog_agg_txn_5m_v1` | instance, bucket, db, table | `uniqCombined64` state of `transaction_id` |

- Written by the worker after the move, from the buffer (`RowsIngestor.aggregate`), with
  `insert_deduplication_token = agg:<file_id>` / `aggtxn:<file_id>` and `non_replicated_deduplication_window`
  on both tables: a retried or re-ingested file is aggregated once, without keying the aggregates by file.
- `fp = normalizedQueryHash(leftUTF8(row_query, 4096))`; the displayed template is `normalizeQuery` of the same
  prefix. Identifiers with three or more digits are normalized too, so sharded tables share one fingerprint.
  `fp = 0` (no statement text) is shown as a synthetic `OPERATION db.table` template.
- Rows ingested before 1.29.16 were backfilled per table and operation only (`aggregate_statements(...,
  with_text=False)`: fingerprint 0, no payload bytes); statement fingerprints and sample SQL start with 1.29.16.
- Not available from the row store (the page says so instead of showing zeros): transaction duration and
  size distributions, dependency depth, longest / largest / multi-table transactions, row-level hotspots.
- ClickHouse aliases are query-global: no alias in `binlog_rows_analytics` may equal a column name (a test
  checks every emitted statement).
