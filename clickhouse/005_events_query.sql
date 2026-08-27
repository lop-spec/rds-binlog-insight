-- Query-serving copy for reverse-chronological Top-N reads. The wide
-- insight.events table remains the ingestion and rollback truth; this table
-- intentionally contains only API result/filter columns plus part identity.
CREATE TABLE IF NOT EXISTS insight.events_query
(
    event_id String CODEC(ZSTD(1)),
    event_epoch_us Int64 CODEC(DoubleDelta, ZSTD(1)),
    event_time_utc DateTime64(6, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    event_date Date CODEC(Delta, ZSTD(1)),
    instance_id LowCardinality(String) CODEC(ZSTD(1)),
    host_instance_id LowCardinality(String) CODEC(ZSTD(1)),
    source_file_name String CODEC(ZSTD(1)),
    raw_event_type LowCardinality(String) CODEC(ZSTD(1)),
    operation LowCardinality(String) CODEC(ZSTD(1)),
    database_name LowCardinality(String) CODEC(ZSTD(1)),
    table_name LowCardinality(String) CODEC(ZSTD(1)),
    server_id Int64 CODEC(Delta, ZSTD(1)),
    thread_id Int64 CODEC(Delta, ZSTD(1)),
    transaction_id String CODEC(ZSTD(1)),
    gtid String CODEC(ZSTD(1)),
    start_position Int64 CODEC(Delta, ZSTD(1)),
    end_position Int64 CODEC(Delta, ZSTD(1)),
    row_index Int32 CODEC(Delta, ZSTD(1)),
    execution_time_ms Int64 CODEC(Delta, ZSTD(1)),
    error_code Int32 CODEC(Delta, ZSTD(1)),
    sql_kind LowCardinality(String) CODEC(ZSTD(1)),
    sql_text String CODEC(ZSTD(1)),
    before_json String CODEC(ZSTD(3)),
    after_json String CODEC(ZSTD(3)),
    row_query String CODEC(ZSTD(1)),
    connection_id String CODEC(ZSTD(1)),
    connection_name String CODEC(ZSTD(1)),
    database_account String CODEC(ZSTD(1)),
    execution_status LowCardinality(String) CODEC(ZSTD(1)),
    error_message String CODEC(ZSTD(1)),
    affected_rows Int64 CODEC(Delta, ZSTD(1)),
    started_epoch_us Int64 CODEC(DoubleDelta, ZSTD(1)),
    finished_epoch_us Int64 CODEC(DoubleDelta, ZSTD(1)),
    batch_id String CODEC(ZSTD(1)),
    statement_index Int32 CODEC(Delta, ZSTD(1)),
    transaction_context_id String CODEC(ZSTD(1)),
    _source_part_key String CODEC(ZSTD(1)),
    _content_revision UInt64 CODEC(Delta, ZSTD(1)),
    INDEX event_id_bloom event_id TYPE bloom_filter(0.001) GRANULARITY 4,
    INDEX source_part_bloom _source_part_key TYPE bloom_filter(0.001) GRANULARITY 4,
    INDEX transaction_bloom transaction_id TYPE bloom_filter(0.001) GRANULARITY 4,
    INDEX gtid_bloom gtid TYPE bloom_filter(0.001) GRANULARITY 4,
    INDEX operation_set operation TYPE set(64) GRANULARITY 4,
    INDEX source_type_set raw_event_type TYPE set(64) GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY event_date
PRIMARY KEY event_epoch_us
ORDER BY
(
    event_epoch_us DESC,
    source_file_name DESC,
    end_position DESC,
    row_index DESC,
    event_id DESC,
    _content_revision DESC,
    _source_part_key DESC
)
TTL event_time_utc + INTERVAL 30 HOUR DELETE
SETTINGS
    allow_experimental_reverse_key = 1,
    index_granularity = 8192,
    max_bytes_to_merge_at_max_space_in_pool = 134217728,
    non_replicated_deduplication_window = 100000;

-- Backfill is assembled here in bounded time ranges and atomically installed
-- one partition at a time with REPLACE PARTITION.
CREATE TABLE IF NOT EXISTS insight.events_query_stage AS insight.events_query;

-- Fuzzy API names are first resolved on the tiny hourly projection below.
-- The resulting exact (instance, database, table) tuples are served from this
-- second physical sort order.  A normal projection cannot express the mixed
-- ascending name prefix and descending event order required by this query.
CREATE TABLE IF NOT EXISTS insight.events_query_by_name AS insight.events_query
ENGINE = MergeTree
PARTITION BY event_date
PRIMARY KEY (instance_id, database_name, table_name, event_epoch_us)
ORDER BY
(
    instance_id ASC,
    database_name ASC,
    table_name ASC,
    event_epoch_us DESC,
    source_file_name DESC,
    end_position DESC,
    row_index DESC,
    event_id DESC,
    _content_revision DESC,
    _source_part_key DESC
)
TTL event_time_utc + INTERVAL 30 HOUR DELETE
SETTINGS
    allow_experimental_reverse_key = 1,
    index_granularity = 8192,
    max_bytes_to_merge_at_max_space_in_pool = 134217728,
    non_replicated_deduplication_window = 100000;

CREATE TABLE IF NOT EXISTS insight.events_query_by_name_stage
AS insight.events_query_by_name;

-- CREATE ... AS may inherit projections from an already-upgraded time table.
-- They are redundant on the physical name-sorted copy and would more than
-- double its disk footprint, so remove only those derived structures.
ALTER TABLE insight.events_query_by_name
    DROP PROJECTION IF EXISTS by_exact_name_v1;
ALTER TABLE insight.events_query_by_name
    DROP PROJECTION IF EXISTS names_hourly_v1;
ALTER TABLE insight.events_query_by_name_stage
    DROP PROJECTION IF EXISTS by_exact_name_v1;
ALTER TABLE insight.events_query_by_name_stage
    DROP PROJECTION IF EXISTS names_hourly_v1;

-- Existing installations need the same additive structures on both the live
-- time table and its bounded backfill stage. Keeping their schemas identical
-- is required by REPLACE PARTITION. Historical parts are materialized only by
-- the explicit production migration procedure, never by service startup.
ALTER TABLE insight.events_query
    ADD INDEX IF NOT EXISTS database_name_ngram lowerUTF8(database_name)
    TYPE ngrambf_v1(3, 2048, 3, 0) GRANULARITY 1;
ALTER TABLE insight.events_query
    ADD INDEX IF NOT EXISTS table_name_ngram lowerUTF8(table_name)
    TYPE ngrambf_v1(3, 2048, 3, 0) GRANULARITY 1;
ALTER TABLE insight.events_query
    ADD PROJECTION IF NOT EXISTS names_hourly_v1
    (
        SELECT toStartOfHour(event_time_utc) AS event_hour,
               instance_id, database_name, table_name, count()
        GROUP BY event_hour, instance_id, database_name, table_name
    );

ALTER TABLE insight.events_query_stage
    ADD INDEX IF NOT EXISTS database_name_ngram lowerUTF8(database_name)
    TYPE ngrambf_v1(3, 2048, 3, 0) GRANULARITY 1;
ALTER TABLE insight.events_query_stage
    ADD INDEX IF NOT EXISTS table_name_ngram lowerUTF8(table_name)
    TYPE ngrambf_v1(3, 2048, 3, 0) GRANULARITY 1;
ALTER TABLE insight.events_query_stage
    ADD PROJECTION IF NOT EXISTS names_hourly_v1
    (
        SELECT toStartOfHour(event_time_utc) AS event_hour,
               instance_id, database_name, table_name, count()
        GROUP BY event_hour, instance_id, database_name, table_name
    );
