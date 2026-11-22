#!/bin/sh
# Synthetic-only diagnostic for non-replicated MergeTree mutation metadata.
set -eu
p=/tmp/ingest-diagnostic
mkdir -m 700 "$p"
expected_version=$(printf '%s.%s' '26.3.17' '110')
clickhouse local --version | grep -F "$expected_version" >/dev/null
clickhouse local --path "$p" --max_threads 1 --max_memory_usage 268435456 \
 --background_schedule_pool_size=4 --background_pool_size=4 --multiquery <<'SQL'
CREATE TABLE lw (event_date Date,tbl_bucket UInt8,event_id UInt64,_source_part_key String)
 ENGINE=MergeTree PARTITION BY (event_date,tbl_bucket) ORDER BY event_id
 SETTINGS old_parts_lifetime=1,number_of_free_entries_in_pool_to_execute_mutation=2,
 number_of_free_entries_in_pool_to_lower_max_size_of_merge=2,
 number_of_free_entries_in_pool_to_execute_optimize_entire_partition=2;
INSERT INTO lw VALUES ('2026-10-29',0,1,'raw:a'),('2026-10-29',0,2,'raw:b'),
 ('2026-10-29',3,3,'raw:a'),('2026-10-30',0,4,'raw:a');
SELECT 'light-before',partition_id,name,data_version,rows FROM system.parts WHERE database=currentDatabase()
 AND table='lw' AND active ORDER BY partition_id FORMAT TSV;
DELETE FROM lw IN PARTITION ('2026-10-29',0)
 WHERE event_date=toDate('2026-10-29') AND tbl_bucket=0 AND _source_part_key='raw:a'
 SETTINGS lightweight_deletes_sync=2;
SELECT 'light-after',partition_id,name,data_version,rows FROM system.parts WHERE database=currentDatabase()
 AND table='lw' AND active ORDER BY partition_id FORMAT TSV;
SELECT 'light-values',arraySort(groupArray(event_id)) FROM lw FORMAT TSV;
CREATE TABLE hw (event_date Date,tbl_bucket UInt8,event_id UInt64,_source_part_key String)
 ENGINE=MergeTree PARTITION BY (event_date,tbl_bucket) ORDER BY event_id
 SETTINGS old_parts_lifetime=1,number_of_free_entries_in_pool_to_execute_mutation=2,
 number_of_free_entries_in_pool_to_lower_max_size_of_merge=2,
 number_of_free_entries_in_pool_to_execute_optimize_entire_partition=2;
INSERT INTO hw VALUES ('2026-10-29',0,1,'raw:a'),('2026-10-29',0,2,'raw:b'),
 ('2026-10-29',3,3,'raw:a'),('2026-10-30',0,4,'raw:a');
SELECT 'heavy-before',partition_id,name,data_version,rows FROM system.parts WHERE database=currentDatabase()
 AND table='hw' AND active ORDER BY partition_id FORMAT TSV;
ALTER TABLE hw DELETE IN PARTITION ('2026-10-29',0)
 WHERE event_date=toDate('2026-10-29') AND tbl_bucket=0 AND _source_part_key='raw:a'
 SETTINGS mutations_sync=2;
SELECT 'heavy-after',partition_id,name,data_version,rows FROM system.parts WHERE database=currentDatabase()
 AND table='hw' AND active ORDER BY partition_id FORMAT TSV;
SELECT 'heavy-values',arraySort(groupArray(event_id)) FROM hw FORMAT TSV;
SQL

# Deliberate capability test: forced mode must not silently fall back to the
# table-wide mutation. Its exit is reported, not used as a release pass.
q=/tmp/ingest-forced-mode
mkdir -m 700 "$q"
if clickhouse local --path "$q" --max_threads 1 --max_memory_usage 268435456 \
 --background_schedule_pool_size=4 --background_pool_size=4 --multiquery <<'SQL'
CREATE TABLE forced (event_date Date,tbl_bucket UInt8,event_id UInt64,_source_part_key String)
 ENGINE=MergeTree PARTITION BY (event_date,tbl_bucket) ORDER BY event_id
 SETTINGS old_parts_lifetime=1,number_of_free_entries_in_pool_to_execute_mutation=2,
 number_of_free_entries_in_pool_to_lower_max_size_of_merge=2,
 number_of_free_entries_in_pool_to_execute_optimize_entire_partition=2;
INSERT INTO forced VALUES ('2026-10-29',0,1,'raw:a'),('2026-10-29',0,2,'raw:b'),
 ('2026-10-29',3,3,'raw:a'),('2026-10-30',0,4,'raw:a');
SELECT 'forced-before',partition_id,name,data_version,rows FROM system.parts WHERE database=currentDatabase()
 AND table='forced' AND active ORDER BY partition_id FORMAT TSV;
DELETE FROM forced IN PARTITION ('2026-10-29',0)
 WHERE event_date=toDate('2026-10-29') AND tbl_bucket=0 AND _source_part_key='raw:a'
 SETTINGS lightweight_delete_mode='lightweight_update_force', lightweight_deletes_sync=2;
SELECT 'forced-after',partition_id,name,data_version,rows FROM system.parts WHERE database=currentDatabase()
 AND table='forced' AND active ORDER BY partition_id FORMAT TSV;
SELECT 'forced-values',arraySort(groupArray(event_id)) FROM forced FORMAT TSV;
SQL
then
 echo 'forced_lightweight_update_accepted'
else
 echo 'forced_lightweight_update_rejected'
fi
