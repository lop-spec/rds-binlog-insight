"""Five-layer Redis evidence, with no causal score or fabricated missing coverage."""
import statistics
from collections import defaultdict
from .redis_history import decode


def unit_costs(current):
    by_command=defaultdict(list)
    for (node,command),cost in current.items():
        if cost['calls']>=100 and cost['seconds']>0:by_command[command].append((node,cost))
    rows=[];node_ratios=defaultdict(list)
    for cmd,parts in by_command.items():
        if len(parts)<3:continue
        times=[p['seconds'] for _,p in parts]
        if min(times)<max(times)*.95:continue
        median=statistics.median(p['usec']/p['calls'] for _,p in parts)
        if median<=0:continue
        for node,c in parts:
            value=c['usec']/c['calls'];ratio=value/median
            rows.append({'node':node,'command':cmd,'avg_us':value,'median_us':median,'index':ratio,'peer_count':len(parts),'calls':c['calls']})
            node_ratios[node].append(ratio)
    summary=[{'node':node,'median_command_index':statistics.median(ratios),'comparable_commands':len(ratios),'scope':'same_command_observed_cost_not_host_cause'} for node,ratios in sorted(node_ratios.items())]
    return {'rows':sorted(rows,key=lambda r:(r['command'],r['node']))[:300],'nodes':summary,'warning':'只比较本窗覆盖相近且至少100次的相同命令；Key大小、命令参数与节点因素均可能影响单次耗时，不能单凭指数确认宿主机。'}


def poll_model(source,configs):
    if not source:return {'status':'no_source_snapshot','predicted_calls':None}
    deployments=[]
    for d in source['data'].get('deployments',[]):
        config=configs.get(d['deployment']);values=config['data'].get('values',{}) if config else {}
        enabled=values.get('followorder.worker.enable')
        intervals={name:values.get('followorder.worker.'+name+'.interval_ms') for name in ('deliver','sent_timeout','archive')}
        pod_count=sum(p.get('ready',False) and not p.get('terminating') for p in d.get('pods',[]))
        prediction=None
        if enabled is False:prediction={'eval_per_second':0,'zrangebyscore_per_second':0}
        elif enabled is True and all(isinstance(v,int) and v>0 for v in intervals.values()):
            prediction={'eval_per_second':pod_count*64*2*1000/intervals['deliver'],'zrangebyscore_per_second':pod_count*64*(1000/intervals['sent_timeout']+1000/intervals['archive'])}
        deployments.append({'deployment':d['deployment'],'ready_pods':pod_count,'worker_enable_config':enabled,'interval_ms':intervals,'prediction':prediction,'observed_at':source['time']})
    return {'status':'configured_model_not_runtime_proof','deployments':deployments,'model':'64 buckets; two deliver EVAL, one sent_timeout ZRANGEBYSCORE, one archive ZRANGEBYSCORE per tick',
            'warning':'ConfigMap值不保证运行进程已加载；空值不假设默认开关；Pod就绪不等于worker已启动。模型吻合属于机制证据，不凭比例拟合定根因。'}


def diagnosis(service,entry,start,end,node,current,baseline):
    identity=entry['instanceId'];history=service.evidence
    if history is None:return {}
    peers=unit_costs(current)
    nodes=defaultdict(lambda:{'process_cpu_seconds':0,'process_cpu_known_seconds':0,'counter_deltas':{},'cost_numerator_cpu_seconds':0,'cost_denominator_commands':0,'cost_matched_seconds':0})
    latest={};initial={};latencies=[]
    filt,args=(' AND node=?',[node]) if node else ('',[])
    with service.db() as c:
        records=c.execute('SELECT node,start,end,payload,reason FROM intervals WHERE instance=? AND start>=? AND end<=?'+filt+' ORDER BY end LIMIT 50001',[identity,start,end]+args).fetchall()
        if len(records)>50000:raise ValueError('redis_diagnosis_row_budget')
        for r in records:
            if r['reason']:continue
            value=decode(r['payload']);d=value.get('derived',{});n=nodes[r['node']];seconds=r['end']-r['start']
            if d.get('process_cpu_seconds') is not None:n['process_cpu_seconds']+=d['process_cpu_seconds'];n['process_cpu_known_seconds']+=seconds
            processed=d.get('counter_deltas',{}).get('total_commands_processed')
            if processed is not None and d.get('process_cpu_seconds') is not None:
                n['cost_numerator_cpu_seconds']+=d['process_cpu_seconds'];n['cost_denominator_commands']+=processed;n['cost_matched_seconds']+=seconds
            for field,count in d.get('counter_deltas',{}).items():
                item=n['counter_deltas'].setdefault(field,{'value':0,'seconds':0});item['value']+=count;item['seconds']+=seconds
        for n in service.meta.get(identity,[]):
            if node and n['node']!=node:continue
            last=c.execute('SELECT ts,payload FROM snapshots WHERE instance=? AND node=? AND ts>=? AND ts<=? ORDER BY ts DESC LIMIT 1',(identity,n['node'],start,end)).fetchone()
            first=c.execute('SELECT ts,payload FROM snapshots WHERE instance=? AND node=? AND ts>=? AND ts<=? ORDER BY ts LIMIT 1',(identity,n['node'],start,end)).fetchone()
            if last:
                raw=decode(last['payload']);tel=raw.get('telemetry',{});latest[n['node']]={'time':last['ts'],'telemetry':tel,'run_id':raw.get('run_id'),'physical_node':raw.get('node_id'),'identity_verified':bool(raw.get('node_id'))}
                for cmd,p in tel.get('percentiles',{}).items():latencies.append({'node':n['node'],'command':cmd,**p,'scope':'cumulative_histogram_snapshot'})
            if first:initial[n['node']]={'time':first['ts'],'telemetry':decode(first['payload']).get('telemetry',{})}
    for n,value in nodes.items():
        processed=value['cost_denominator_commands']
        value['cpu_usec_per_processed_command']=value['cost_numerator_cpu_seconds']*1e6/processed if processed else None
        value['non_command_cpu_seconds']=None
        value['non_command_reason']='Lua/EXEC timing nesting and server计数口径未核实，不能把差值定为网络或往返开销'
        value['snapshot']=latest.get(n);value['initial_snapshot']=initial.get(n)
        known=value['process_cpu_known_seconds'];value['average_process_cpu_percent']=value['process_cpu_seconds']/known*100 if known else None
    source=history.latest(identity,'sources',before=end)
    configs={}
    if source:
        for dep in source['data'].get('deployments',[]):configs[dep['deployment']]=history.latest(identity,'config',before=end,node=dep['deployment'])
    events=history.rows(identity,'event',start-600,min(service.clock()+1,end+600),limit=200)
    cloud_events=history.rows(identity,'cloud_event',start-600,min(service.clock()+1,end+600),limit=100)
    aggregates=service.aggregates.query(identity,start,end) if service.aggregates else {'status':'not_connected','rows':[],'reported_calls':{}}
    server_calls=defaultdict(int)
    for (_,cmd),cost in current.items():server_calls[cmd]+=cost['calls']
    reconciliation=[]
    for cmd in sorted(set(server_calls)|set(aggregates.get('reported_calls',{}))):
        server=server_calls.get(cmd);client=aggregates.get('reported_calls',{}).get(cmd)
        reconciliation.append({'command':cmd,'server_observed_calls':server,'client_reported_calls':client,
            'numeric_difference':server-client if server is not None and client is not None else None,
            'uncovered_fraction':None,'scope_comparable':False,
            'reason':'server可能含Lua内部、事务控制、采集器及连接命令；客户端为逻辑调用，不能把差数直接称未覆盖比例'})
    evidence=[]
    # These are triage classifications only. Full root-cause attribution requires
    # matching source execution counters, exact epoch/window and a mechanism.
    memory=[]
    for n,last in latest.items():
        old=initial.get(n,{}).get('telemetry',{}).get('gauges',{});new=last['telemetry'].get('gauges',{})
        memory.append({'node':n,'fields':{k:{'first':old.get(k),'last':new.get(k),'delta':new[k]-old[k] if k in old and k in new else None} for k in sorted(set(old)|set(new)) if k.startswith(('used_memory','mem_','allocator_','rss_','lazyfree','aof_','rdb_'))},'scope':'overlapping_components_not_additive'})
    if source and events['rows']:evidence.append({'grade':'时间相关候选','kind':'workload_or_configuration_change','message':'窗口附近有副本、镜像、HPA或Pod变化；需与命令阶跃和运行计数核对。'})
    if any(n['median_command_index']>1.2 and n['comparable_commands']>=5 for n in peers['nodes']):evidence.append({'grade':'时间相关候选','kind':'higher_observed_unit_cost','message':'同窗多个相同命令单次成本偏高；不能据此排除Key参数差异或确认宿主机争抢。'})
    evidence.append({'grade':'未覆盖','kind':'unattributed','message':'未接入的客户端、内部命令口径差异、未记录历史均不默认为零；不自动关闭其他候选。'})
    return {'five_layers':{'nodes':[{'node':n,**v} for n,v in sorted(nodes.items())], 'unit_costs':peers,'latency_snapshots':latencies[:300],
              'memory':memory,'sources':source,'worker_configs':configs,'polling_model':poll_model(source,configs),
              'events':events,'cloud_events':cloud_events,
              'keys':history.rows(identity,'keys',start,end,node,100),'queues':history.rows(identity,'queues',start,end,limit=30),
              'profiles':history.rows(identity,'profile',start,end,node,60),'night_baselines':history.rows(identity,'night',start-3*86400,end,node,30),
              'client_aggregates':aggregates,'coverage_reconciliation':reconciliation,'findings':evidence},
            'collector_state':dict(service.context.state) if service.context else {},'aggregate_delivery_state':dict(service.aggregates.state) if service.aggregates else {},'causal':False,'workflow':['已观测节点范围','前置等长与凌晨基线','命令量/成本','来源与事件','单位成本','Key/队列','内存分项','三级证据与缺口']}
