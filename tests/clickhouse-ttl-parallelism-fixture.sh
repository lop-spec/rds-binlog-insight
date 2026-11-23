#!/bin/sh
# Only synthetic tables in a disposable, network-isolated ClickHouse container.
# Test the scope and rollback of a MergeTree setting, not its production benefit.
set -eu
base=/tmp/ttl-parallelism-fixture
mkdir -m 700 "$base"
expected_version=$(printf '%s.%s' '26.3.17' '110')
clickhouse local --version | grep -F "$expected_version" >/dev/null
clickhouse local --path "$base" --max_threads 1 --max_memory_usage 268435456 \
  --background_schedule_pool_size=4 --background_pool_size=4 --multiquery <<'SQL'
CREATE TABLE target (d Date, id UInt64) ENGINE=MergeTree ORDER BY id
 TTL d + INTERVAL 62 DAY DELETE
 SETTINGS ttl_only_drop_parts=1, number_of_free_entries_in_pool_to_execute_mutation=2,
 number_of_free_entries_in_pool_to_lower_max_size_of_merge=2,
 number_of_free_entries_in_pool_to_execute_optimize_entire_partition=2;
CREATE TABLE control AS target;
INSERT INTO target VALUES ('2099-01-01',1),('2099-01-01',2);
INSERT INTO control VALUES ('2099-01-01',3);
CREATE TABLE before_ddl (name String, ddl String) ENGINE=Memory;
INSERT INTO before_ddl SELECT name,create_table_query FROM system.tables
 WHERE database=currentDatabase() AND name IN ('target','control');
SELECT throwIf(count()!=2,'incomplete_fixture') FROM before_ddl FORMAT Null;
SELECT throwIf(toUInt64(value)!=2,'unexpected_default') FROM system.merge_tree_settings
 WHERE name='max_number_of_merges_with_ttl_in_pool' FORMAT Null;
ALTER TABLE target MODIFY SETTING max_number_of_merges_with_ttl_in_pool=1;
SELECT throwIf(count()!=1,'target_setting_or_ttl_changed') FROM system.tables
 WHERE database=currentDatabase() AND name='target'
 AND extract(create_table_query,'max_number_of_merges_with_ttl_in_pool\\s*=\\s*([0-9]+)')='1'
 AND position(create_table_query,'TTL ')>0
 AND splitByString('SETTINGS',create_table_query)[1]
     =(SELECT splitByString('SETTINGS',ddl)[1] FROM before_ddl WHERE name='target') FORMAT Null;
SELECT throwIf(count()!=1,'control_ddl_changed') FROM system.tables
 WHERE database=currentDatabase() AND name='control'
 AND create_table_query=(SELECT ddl FROM before_ddl WHERE name='control') FORMAT Null;
SELECT throwIf(groupArray(id)!=[1,2] AND arraySort(groupArray(id))!=[1,2],'target_rows_changed') FROM target FORMAT Null;
ALTER TABLE target RESET SETTING max_number_of_merges_with_ttl_in_pool;
SELECT throwIf(count()!=1,'target_ddl_not_restored') FROM system.tables
 WHERE database=currentDatabase() AND name='target'
 AND create_table_query=(SELECT ddl FROM before_ddl WHERE name='target') FORMAT Null;
SELECT throwIf(arraySort(groupArray(id))!=[1,2],'target_rows_changed_on_reset') FROM target FORMAT Null;
SELECT throwIf(groupArray(id)!=[3],'control_rows_changed') FROM control FORMAT Null;
SELECT 'ttl_parallelism_setting_scope_and_rollback_passed' AS result;
SQL
