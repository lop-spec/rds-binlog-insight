"""Provider-dependent INFO fields: missing stays missing, percentiles are not deltas."""
import math
import re

SECTIONS=('server','commandstats','cpu','stats','memory','clients','persistence','keyspace','latencystats','errorstats')
COUNTERS=set('total_commands_processed total_connections_received total_net_input_bytes total_net_output_bytes rejected_connections expired_keys evicted_keys keyspace_hits keyspace_misses total_reads_processed total_writes_processed total_io_reads_processed total_io_writes_processed eventloop_cycles eventloop_duration_sum eventloop_duration_cmd_sum expired_time_cap_reached_count expire_cycle_cpu_milliseconds total_error_replies aof_delayed_fsync'.split())
GAUGES=set('instantaneous_ops_per_sec instantaneous_input_kbps instantaneous_output_kbps latest_fork_usec expired_stale_perc used_memory used_memory_rss used_memory_peak used_memory_overhead used_memory_startup used_memory_dataset used_memory_lua used_memory_scripts used_memory_vm_eval used_memory_vm_functions number_of_cached_scripts number_of_functions number_of_libraries mem_clients_normal mem_clients_slaves mem_total_replication_buffers mem_replication_backlog mem_aof_buffer mem_fragmentation_ratio mem_fragmentation_bytes allocator_frag_ratio allocator_frag_bytes allocator_rss_ratio allocator_rss_bytes rss_overhead_ratio rss_overhead_bytes lazyfree_pending_objects maxmemory connected_clients blocked_clients tracking_clients pubsub_clients client_longest_output_list client_biggest_input_buf rdb_bgsave_in_progress rdb_current_bgsave_time_sec rdb_last_bgsave_time_sec aof_rewrite_in_progress aof_current_rewrite_time_sec aof_last_rewrite_time_sec aof_pending_bio_fsync aof_buffer_length aof_rewrite_buffer_length aof_enabled master_repl_offset repl_backlog_size repl_backlog_histlen'.split())


def finite(v):
    try:
        n=float(v)
        return n if math.isfinite(n) else None
    except (ValueError,TypeError):return None


def sections_of(raw):
    out={};name='default'
    for line in str(raw).splitlines():
        if line.startswith('# '):name=line[2:].strip().lower();out.setdefault(name,{})
        elif ':' in line:
            k,v=line.split(':',1);out.setdefault(name,{})[k]=v
    return out


def enrich(sections):
    counters={};gauges={};percentiles={};errors={};keyspace={}
    for values in sections.values():
        for k,v in values.items():
            if k in COUNTERS|GAUGES:
                n=finite(v)
                if n is not None:(counters if k in COUNTERS else gauges)[k]=n
    for key,value in sections.get('latencystats',{}).items():
        cmd=key.removeprefix('latency_percentiles_usec_')
        if not re.fullmatch(r'[a-z0-9_|-]{1,96}',cmd):continue
        p={}
        for pair in value.split(','):
            k,_,v=pair.partition('=')
            if k in ('p50','p99','p99.9') and finite(v) is not None:p[k]=float(v)
        if p:percentiles[cmd]=p
    for key,value in sections.get('errorstats',{}).items():
        if re.fullmatch(r'errorstat_[A-Z0-9_]{1,40}',key):
            m=re.search(r'(?:^|,)count=(\d+)',value)
            if m:errors[key[10:]]=int(m[1])
    for key,value in sections.get('keyspace',{}).items():
        if not re.fullmatch(r'db\d{1,3}',key):continue
        keyspace[key]={k:float(v) for item in value.split(',') for k,sep,v in [item.partition('=')] if sep and k in ('keys','expires','avg_ttl') and finite(v) is not None}
    return {'counters':counters,'gauges':gauges,'percentiles':percentiles,'error_counts':errors,'keyspace':keyspace,
            'present_sections':list(sections),'missing_sections':[s for s in SECTIONS if not sections.get(s)],
            'missing_fields':sorted((COUNTERS|GAUGES)-set(counters)-set(gauges)),
            'percentile_scope':'provider_cumulative_histogram_snapshot_not_window_quantiles',
            'command_optional_support':{name:any(re.search(r'(?:^|,)'+name+r'=\d+(?:,|$)',v) for v in sections.get('commandstats',{}).values()) for name in ('rejected_calls','failed_calls')}}


def counter_delta(old,new):
    return {k:new[k]-old[k] for k in old.keys()&new.keys() if new[k]>=old[k]}


def derived(previous,current,seconds,process_cpu):
    old=previous.get('telemetry',{});now=current.get('telemetry',{})
    changes=counter_delta(old.get('counters',{}),now.get('counters',{}))
    cpu_seconds=process_cpu/100*seconds if process_cpu is not None else None
    commands=changes.get('total_commands_processed')
    return {'counter_deltas':changes,'error_deltas':counter_delta(old.get('error_counts',{}),now.get('error_counts',{})),
            'process_cpu_seconds':cpu_seconds,
            'cpu_usec_per_processed_command':cpu_seconds*1e6/commands if cpu_seconds is not None and commands and commands>0 else None,
            'cpu_usec_per_io_cycle':None,
            'non_command_cpu_seconds':None,
            'non_command_reason':'nested_command_timing_scope_not_proven; difference is not network CPU proof',
            'cpu_units':'process CPU vs server commands, not client round trips',
            'user_cpu_seconds':current['cpu'].get('used_cpu_user',0)-previous['cpu'].get('used_cpu_user',0) if 'used_cpu_user' in current['cpu'] and 'used_cpu_user' in previous['cpu'] else None,
            'sys_cpu_seconds':current['cpu'].get('used_cpu_sys',0)-previous['cpu'].get('used_cpu_sys',0) if 'used_cpu_sys' in current['cpu'] and 'used_cpu_sys' in previous['cpu'] else None}


def config_numbers(text):
    """Extract only literal numeric/boolean whitelist fields under followorder.worker.

    Not a YAML loader: anchors, expressions, quoted/flow values are unknown, never
    evaluated. No other configuration or address/credential may leave memory.
    """
    allowed={'enable','scan_count','interval_ms','batch','shop_concurrency','shop_inner_concurrency','ack_timeout_ms','retry_delay_ms'}
    stack=[];out={}
    for raw in str(text).splitlines():
        if '\t' in raw:continue
        m=re.fullmatch(r'( *)([a-z_]+):(?:\s*(.*))?',raw)
        if not m:continue
        indent=len(m[1]);key=m[2];value=(m[3] or '').split('#',1)[0].strip()
        while stack and stack[-1][0]>=indent:stack.pop()
        path=[p for _,p in stack]+[key]
        if not value:stack.append((indent,key));continue
        if len(path)>=3 and path[:2]==['followorder','worker'] and key in allowed:
            field='.'.join(path)
            if value in ('true','false'):out[field]=value=='true'
            elif re.fullmatch(r'\d{1,10}',value):out[field]=int(value)
    return out
