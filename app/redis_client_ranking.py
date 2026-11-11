"""Association of received client shape counts to full-instance observed CPU."""
from collections import defaultdict
from .redis_ranking import association, HIGH_R


def client_key_ranking(service,identity,start,end,cpu):
    aggregate=service.aggregates
    if aggregate is None or aggregate.client is None or not aggregate.schema_ready:return [],{'status':'not_connected'}
    client=aggregate.client;db=client.config.database
    # A CPU node is never substituted for instance-wide client counts. All
    # observed nodes must contribute the same minute, with independent epochs.
    expected={n['node'] for n in service.meta.get(identity,[])}
    if not expected or set(cpu)!=expected:return [],{'status':'instance_cpu_nodes_incomplete'}
    times=set.intersection(*(set(cpu[n]) for n in expected))
    resource={t:sum(cpu[n][t] for n in expected) for t in times}
    try:
        rows=client.json_rows(f'''SELECT deployment,command,shape,path,script,toUnixTimestamp(toStartOfMinute(window_end)) minute,
            sum(calls) calls,uniqExact(process) processes,count() reports
            FROM {db}.redis_client_counts FINAL
            WHERE instance={{instance:String}} AND ts>=toDateTime({int(start)}) AND window_end<toDateTime({int(end)})
            AND command!='heartbeat' GROUP BY deployment,command,shape,path,script,minute
            ORDER BY minute LIMIT 20001''',parameters={'instance':identity},settings={'max_execution_time':8,'max_memory_usage':128000000,'max_result_bytes':8000000,'result_overflow_mode':'throw'},timeout=10)
    except Exception as exc:return [],{'status':'unavailable','error':type(exc).__name__}
    if len(rows)>20000:return [],{'status':'series_truncated_no_ranking'}
    groups=defaultdict(dict)
    for row in rows:groups[tuple(row[k] for k in ('deployment','command','shape','path','script'))][int(row['minute'])]=int(row['calls'])/60
    ranked=[]
    for (deployment,command,shape,path,script),values in groups.items():
        relation=association(values,resource)
        if relation['r'] is not None and relation['r']>=HIGH_R:
            ranked.append({'node':'','key':shape,'command':command,'deployment':deployment,'path':path,'script':script,'source':'connected_client_counts',
                           'correlation':relation,'qps_observed':sum(values.values())/len(values),'delta_qps_observed':None,'assessment':'high_correlation_only',
                           'scope':'received_batches_only; full_instance_CPU_not_shard; missing_reports_not_zero','causal':False})
    return ranked,{'status':'observed' if rows else 'not_connected_or_no_reports','series':len(groups),'complete':False}
