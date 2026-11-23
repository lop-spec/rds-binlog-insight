#!/bin/sh
# Bounded, networkless CI fixture. No production credentials, data or mounts.
set -eu
base=/tmp/sql-insight-ttl-batch
mkdir -m 700 "$base"
cat > "$base/config.xml" <<'XML'
<clickhouse>
  <logger><level>warning</level><console>true</console></logger>
  <listen_host>127.0.0.1</listen_host><tcp_port>19000</tcp_port>
  <path>/tmp/sql-insight-ttl-batch/data/</path>
  <tmp_path>/tmp/sql-insight-ttl-batch/tmp/</tmp_path>
  <user_files_path>/tmp/sql-insight-ttl-batch/files/</user_files_path>
  <format_schema_path>/tmp/sql-insight-ttl-batch/schemas/</format_schema_path>
  <mark_cache_size>8388608</mark_cache_size><uncompressed_cache_size>8388608</uncompressed_cache_size>
  <background_pool_size>4</background_pool_size><background_schedule_pool_size>4</background_schedule_pool_size>
  <background_buffer_flush_schedule_pool_size>2</background_buffer_flush_schedule_pool_size>
  <background_message_broker_schedule_pool_size>2</background_message_broker_schedule_pool_size>
  <background_distributed_schedule_pool_size>2</background_distributed_schedule_pool_size>
  <merge_tree>
    <number_of_free_entries_in_pool_to_execute_mutation>2</number_of_free_entries_in_pool_to_execute_mutation>
    <number_of_free_entries_in_pool_to_lower_max_size_of_merge>2</number_of_free_entries_in_pool_to_lower_max_size_of_merge>
    <number_of_free_entries_in_pool_to_execute_optimize_entire_partition>2</number_of_free_entries_in_pool_to_execute_optimize_entire_partition>
  </merge_tree>
  <users><default><password/><networks><ip>127.0.0.1</ip></networks><profile>default</profile><quota>default</quota></default></users>
  <profiles><default><max_threads>1</max_threads><max_memory_usage>134217728</max_memory_usage><max_execution_time>5</max_execution_time></default></profiles>
  <quotas><default/></quotas>
  <part_log><database>system</database><table>part_log</table><flush_interval_milliseconds>300</flush_interval_milliseconds></part_log>
</clickhouse>
XML
p=0
trap 'if [ "$p" -gt 0 ]; then kill "$p" 2>/dev/null || :; wait "$p" || :; fi; if [ -f "$base/server.log" ]; then tail -25 "$base/server.log"; fi' EXIT
clickhouse server --config-file="$base/config.xml" > "$base/server.log" 2>&1 &
p=$!
sql() { clickhouse client --host 127.0.0.1 --port 19000 --query "$1"; }
tries=0
until sql 'SELECT 1' >/dev/null 2>&1; do
  tries=$((tries+1)); test "$tries" -lt 30; kill -0 "$p"; sleep 1
done
expected=$(printf '%s.%s' '26.3.17' '110')
test "$(sql 'SELECT version()')" = "$expected"
sql 'CREATE DATABASE fixture'

observe() {
  table=$1 cap=$2
  sql "CREATE TABLE fixture.$table (d Date, id UInt64, v String) ENGINE=MergeTree PARTITION BY d ORDER BY id TTL d + INTERVAL 1 DAY DELETE SETTINGS ttl_only_drop_parts=1, max_number_of_merges_with_ttl_in_pool=1, max_parts_to_merge_at_once=$cap, old_parts_lifetime=1"
  sql "SYSTEM STOP MERGES fixture.$table"
  sql "SYSTEM STOP TTL MERGES fixture.$table"
  i=0
  while [ "$i" -lt 8 ]; do
    sql "INSERT INTO fixture.$table SELECT toDate('2000-01-01'),$i*32+number,repeat('x',50) FROM numbers(32)"
    i=$((i+1))
  done
  test "$(sql "SELECT count(),sum(rows) FROM system.parts WHERE database='fixture' AND table='$table' AND active")" = "$(printf '8\t256')"
  sql "SYSTEM START TTL MERGES fixture.$table"
  sql "SYSTEM START MERGES fixture.$table"
  tries=0
  until [ "$(sql "SELECT count() FROM fixture.$table")" = 0 ]; do
    tries=$((tries+1)); test "$tries" -lt 30; sleep 1
  done
  sql 'SYSTEM FLUSH LOGS'
  sql "SELECT countIf(error=0),max(length(merged_from)),countIf(error!=0) FROM system.part_log WHERE database='fixture' AND table='$table' AND event_type='MergeParts' AND merge_reason='TTLDropMerge'"
}
control=$(observe control 100)
limited=$(observe limited 3)
printf 'control\t%s\nlimited\t%s\n' "$control" "$limited"
# The control must group all eight; the limited run must use multiple batches.
[ "$control" = "$(printf '1\t8\t0')" ]
printf '%s\n' "$limited" | awk -F '\t' '$1 >= 3 && $2 == 3 && $3 == 0 {ok=1} END {exit !ok}'
sql "SELECT throwIf(count()!=0) FROM fixture.control"
sql "SELECT throwIf(count()!=0) FROM fixture.limited"
printf '%s\n' ttl_drop_batch_cap_observed
