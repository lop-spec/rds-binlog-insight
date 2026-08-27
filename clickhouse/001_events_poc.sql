CREATE DATABASE IF NOT EXISTS insight_poc;

CREATE TABLE IF NOT EXISTS insight_poc.events_poc
(
    event_id String CODEC(ZSTD(1)),
    event_epoch_us Int64 CODEC(DoubleDelta, ZSTD(1)),
    event_time_utc DateTime64(6, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
    event_date Date CODEC(Delta, ZSTD(1)),
    instance_id LowCardinality(String) CODEC(ZSTD(1)),
    host_instance_id LowCardinality(String) CODEC(ZSTD(1)),
    source_file_id String CODEC(ZSTD(1)),
    source_file_name String CODEC(ZSTD(1)),
    raw_event_type LowCardinality(String) CODEC(ZSTD(1)),
    operation LowCardinality(String) CODEC(ZSTD(1)),
    database_name LowCardinality(String) CODEC(ZSTD(1)),
    table_name LowCardinality(String) CODEC(ZSTD(1)),
    table_map_id UInt64 CODEC(Delta, ZSTD(1)),
    schema_version_id String CODEC(ZSTD(1)),
    server_id Int64 CODEC(Delta, ZSTD(1)),
    thread_id Int64 CODEC(Delta, ZSTD(1)),
    transaction_id String CODEC(ZSTD(1)),
    gtid String CODEC(ZSTD(1)),
    xid String CODEC(ZSTD(1)),
    start_position Int64 CODEC(Delta, ZSTD(1)),
    end_position Int64 CODEC(Delta, ZSTD(1)),
    row_index Int32 CODEC(Delta, ZSTD(1)),
    execution_time_ms Int64 CODEC(Delta, ZSTD(1)),
    error_code Int32 CODEC(Delta, ZSTD(1)),
    sql_kind LowCardinality(String) CODEC(ZSTD(1)),
    sql_text String CODEC(ZSTD(1)),
    sql_bytes_base64 String CODEC(ZSTD(1)),
    before_json String CODEC(ZSTD(3)),
    after_json String CODEC(ZSTD(3)),
    columns_json String CODEC(ZSTD(3)),
    row_query String CODEC(ZSTD(1)),
    header_epoch_us Int64 CODEC(DoubleDelta, ZSTD(1)),
    commit_epoch_us Int64 CODEC(DoubleDelta, ZSTD(1)),
    txn_last_committed Int64 CODEC(Delta, ZSTD(1)),
    txn_sequence_number Int64 CODEC(Delta, ZSTD(1)),
    txn_length_bytes Int64 CODEC(Delta, ZSTD(1)),
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
    _source_part_key String DEFAULT '',
    _source_part_sha256 String DEFAULT '',
    _content_revision UInt64 DEFAULT 0,
    _ingested_at DateTime64(3, 'UTC') DEFAULT now64(3),
    INDEX event_id_bloom event_id TYPE bloom_filter(0.001) GRANULARITY 4,
    INDEX source_part_bloom _source_part_key TYPE bloom_filter(0.001) GRANULARITY 4,
    INDEX operation_set operation TYPE set(64) GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY event_date
PRIMARY KEY (instance_id, database_name, table_name, event_epoch_us)
ORDER BY
(
    instance_id,
    database_name,
    table_name,
    event_epoch_us,
    source_file_name,
    end_position,
    row_index,
    event_id
)
SETTINGS
    index_granularity = 8192,
    non_replicated_deduplication_window = 100000;
