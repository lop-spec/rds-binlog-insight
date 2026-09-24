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

### Slim parser (`parser-slim/`, 1.29.1)

The collector keeps using `tools/binlog-parser`. The rows worker uses `/app/tools/binlog-parser-slim --slim`
(built from `parser-slim/` in the image; override with `RDS_BINLOG_ROWS_PARSER`, missing binary falls back to
the full parser with a `BINLOG_ROWS_SLIM_PARSER_MISSING` log line). Slim mode omits pseudo SQL, column
metadata and base64 SQL, cuts `row_query` to 65536 code points (what the store keeps) and does not write
transaction-boundary records while still advancing the output sequence, so every emitted `event_id`,
row image, position and `row_query` prefix is identical to the full parser. Verified on a production binlog:
174,443 non-boundary records, 0 field differences; parser CPU 29.3 s → 17.9 s.

Measured on the production host (4 vCPU): one 524 MB binlog ≈ 56 s end to end with the slim parser
(74 s full parser streamed, 132 s with on-disk chunks); download 2.5 s with 4 ranged streams.
