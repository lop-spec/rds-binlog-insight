CREATE DATABASE IF NOT EXISTS insight;

-- Slow-log analytics has a different access path from binlog event search:
-- almost every request fixes instance_id and a time range, then aggregates by
-- fingerprint/object/operation.  Keep this fact table narrow and time-first;
-- the generic events table deliberately has a different ordering key.
CREATE TABLE IF NOT EXISTS insight.slowlog_events
(
    event_id String CODEC(ZSTD(1)),
    event_epoch_us Int64 CODEC(DoubleDelta, ZSTD(1)),
    event_date Date CODEC(Delta, ZSTD(1)),
    instance_id LowCardinality(String) CODEC(ZSTD(1)),
    node_id LowCardinality(String) CODEC(ZSTD(1)),
    operation LowCardinality(String) CODEC(ZSTD(1)),
    database_name LowCardinality(String) CODEC(ZSTD(1)),
    table_name LowCardinality(String) CODEC(ZSTD(1)),
    fingerprint FixedString(32) CODEC(ZSTD(1)),
    sql_id String CODEC(ZSTD(1)),
    action LowCardinality(String) CODEC(ZSTD(1)),
    normalized_sql String CODEC(ZSTD(3)),
    sample_sql String CODEC(ZSTD(3)),
    sql_bytes UInt32 CODEC(T64, ZSTD(1)),
    query_time_ms UInt64 CODEC(T64, ZSTD(1)),
    lock_time_ms UInt64 CODEC(T64, ZSTD(1)),
    rows_examined UInt64 CODEC(T64, ZSTD(1)),
    rows_sent UInt64 CODEC(T64, ZSTD(1)),
    database_account String CODEC(ZSTD(1)),
    client_ip String CODEC(ZSTD(1)),
    thread_id Int64 CODEC(Delta, ZSTD(1)),
    source_file_name String CODEC(ZSTD(1)),
    _source_part_path String CODEC(ZSTD(1)),
    _source_part_id String CODEC(ZSTD(1)),
    _source_part_sha256 String CODEC(ZSTD(1)),
    _content_revision UInt64 CODEC(Delta, ZSTD(1)),
    _ingested_at DateTime64(3, 'UTC') DEFAULT now64(3),
    INDEX source_part_path_bloom _source_part_path
        TYPE bloom_filter(0.001) GRANULARITY 4,
    INDEX source_part_id_bloom _source_part_id
        TYPE bloom_filter(0.001) GRANULARITY 4,
    INDEX node_set node_id TYPE set(256) GRANULARITY 4,
    INDEX operation_set operation TYPE set(64) GRANULARITY 4
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
PRIMARY KEY (instance_id, event_epoch_us)
ORDER BY
(
    instance_id,
    event_epoch_us,
    fingerprint,
    event_id,
    _source_part_path,
    _source_part_id
)
TTL event_date + INTERVAL 61 DAY DELETE
SETTINGS
    index_granularity = 4096,
    max_bytes_to_merge_at_max_space_in_pool = 134217728;

ALTER TABLE insight.slowlog_events MODIFY SETTING
    max_bytes_to_merge_at_max_space_in_pool = 134217728;
