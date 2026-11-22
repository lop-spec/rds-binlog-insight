#!/bin/sh
# Synthetic rows, local temporary storage, no network or production mounts.
set -eu
base=/tmp/ingest-purge-fixture
mkdir -m 700 "$base"
expected_version=$(printf '%s.%s' '26.3.17' '110')
clickhouse local --version | grep -F "$expected_version" >/dev/null
clickhouse local --path "$base" --max_threads 1 --max_memory_usage 268435456 \
  --background_schedule_pool_size=4 --background_pool_size=4 --multiquery <<'SQL'
CREATE TABLE fixture (
 event_date Date, tbl_bucket UInt8, instance_id String, event_id UInt64, _source_part_key String, value String
) ENGINE=MergeTree PARTITION BY (event_date,tbl_bucket) ORDER BY (instance_id,event_id)
SETTINGS old_parts_lifetime=1, number_of_free_entries_in_pool_to_execute_mutation=2,
 number_of_free_entries_in_pool_to_lower_max_size_of_merge=2,
 number_of_free_entries_in_pool_to_execute_optimize_entire_partition=2;
INSERT INTO fixture VALUES
 ('2026-10-29',0,'prod',1,'raw:a','delete-me'),
 ('2026-10-29',0,'prod',2,'raw:b','keep-other-file'),
 ('2026-10-29',0,'test',3,'raw:c','keep-other-instance'),
 ('2026-10-29',3,'prod',4,'raw:a','keep-other-bucket'),
 ('2026-10-30',0,'prod',5,'raw:a','keep-other-day');
DELETE FROM fixture WHERE event_date IN ('2026-10-29') AND tbl_bucket IN (0)
 AND _source_part_key IN (SELECT arrayJoin(JSONExtract('["raw:a"]', 'Array(String)')))
 SETTINGS lightweight_deletes_sync=2;
SELECT throwIf(arraySort(groupArray((event_id,value))) != [(2,'keep-other-file'),(3,'keep-other-instance'),
 (4,'keep-other-bucket'),(5,'keep-other-day')],'delete_scope_mismatch') FROM fixture FORMAT Null;
SELECT throwIf(count()!=1,'unexpected_mutation_count') FROM system.mutations
 WHERE database=currentDatabase() AND table='fixture' FORMAT Null;
SELECT 'single_mutation_and_logical_scope_passed' AS result;
SQL
