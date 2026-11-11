"""Zero-lag association on observed intervals, never CPU attribution or causation."""
from collections import defaultdict
import math
from .slowlog_correlation import pearson, differences
from .redis_history import decode

MIN_POINTS=6
HIGH_R=.7


def association(x, y):
    times=sorted(x.keys()&y.keys())
    if len(times)<MIN_POINTS:return {'r':None,'change_r':None,'points':len(times),'status':'insufficient_points'}
    a=[round(x[t]*1000000) for t in times];b=[round(y[t]*1000000) for t in times]
    r,status=pearson(a,b)
    pairs=[i for i in range(1,len(times)) if times[i]-times[i-1]==60]
    dr,_=pearson([a[i]-a[i-1] for i in pairs],[b[i]-b[i-1] for i in pairs])
    return {'r':r,'change_r':dr,'points':len(times),'status':status}


def interval_series(records,start,end):
    cpu=defaultdict(dict);commands=defaultdict(dict);parts=defaultdict(list);broken=set()
    for row in records:
        if row['reason']:
            if row['reason'] not in ('baseline',):broken.add(row['node'])
            continue
        lo,hi=row['start'],row['end'];bucket=int(hi//60)*60
        # Whole calendar minutes; interval-end grouping, never proportional split.
        if bucket<math.ceil(start/60)*60 or bucket+60>end:continue
        parts[row['node'],bucket].append((hi-lo,decode(row['payload'])))
    for (node,t),rows in parts.items():
        seconds=sum(sec for sec,_ in rows)
        if not 48<=seconds<=72:continue
        known=sum(sec for sec,d in rows if d.get('process_cpu_percent') is not None)
        if known<48:continue
        cpu[node][t]=sum(sec*d['process_cpu_percent'] for sec,d in rows if d.get('process_cpu_percent') is not None)/known
        names=set().union(*(d['commands'].keys() for _,d in rows))
        for cmd in names:
            valid=[(sec,d['commands'][cmd]) for sec,d in rows if cmd in d['commands']]
            covered=sum(sec for sec,_ in valid)
            if covered<48:continue
            commands[node,cmd][t]={'calls':sum(c['calls'] for _,c in valid)/covered,'usec':sum(c['usec'] for _,c in valid)/covered}
    return cpu,commands,broken


def resource_comparison(now,before):
    a=list(now.values());b=list(before.values())
    if len(a)<MIN_POINTS or len(b)<MIN_POINTS:return {'current':sum(a)/len(a) if a else None,'baseline':sum(b)/len(b) if b else None,'delta':None,'status':'insufficient_points'}
    return {'current':sum(a)/len(a),'baseline':sum(b)/len(b),'delta':sum(a)/len(a)-sum(b)/len(b),'status':'observed_minutes_not_full_window'}


def ranked_evidence(service,identity,start,end,base_start,base_end,records,before,rows,node=''):
    cpu,series,broken=interval_series(records,start,end)
    old_cpu,_,old_broken=interval_series(before,base_start,base_end)
    node_changes={n:resource_comparison(v,old_cpu.get(n,{})) for n,v in cpu.items()}
    ranked=[];counts=defaultdict(int)
    # Epoch changes include topology flips between windows, not merely resets in a window.
    filt,args=(' AND node=?',[node]) if node else ('',[])
    epochs=defaultdict(set)
    with service.db() as c:
        for snap in c.execute('SELECT node,payload FROM snapshots WHERE instance=? AND ts>=? AND ts<=?'+filt,(identity,base_start,end,*args)):
            s=decode(snap['payload']);epochs[snap['node']].add((s.get('node_id'),s.get('run_id')))
    discontinuous=broken|old_broken|{n for n,values in epochs.items() if len(values)>1}
    for n in discontinuous:
        if n in node_changes:node_changes[n].update(delta=None,status='node_epoch_or_collection_changed')
    for row in rows:
        n,cmd=row['node'],row['command'];timeline=series.get((n,cmd),{})
        calls=association({t:v['calls'] for t,v in timeline.items()},cpu.get(n,{}))
        cost=association({t:v['usec'] for t,v in timeline.items()},cpu.get(n,{}))
        selected=max([('calls',calls),('usec',cost)],key=lambda item:item[1]['r'] if item[1]['r'] is not None else -2)
        relation={**selected[1],'basis':selected[0],'calls_r':calls['r'],'cost_r':cost['r'],'available_cpu_minutes':len(cpu.get(n,{})),'window_minutes':int((end-start)//60)}
        if n in discontinuous:relation.update(r=None,status='node_epoch_or_collection_changed')
        growth=row.get('delta_calls') is not None and row.get('delta_service_seconds') is not None and (row['delta_calls']>0 or row['delta_service_seconds']>0)
        row.update(correlation=relation,resource_comparison=node_changes.get(n,{}),causal=False)
        high=relation['r'] is not None and relation['r']>=HIGH_R
        grade='high_correlation_with_growth' if high and growth else 'high_correlation_baseline_missing' if high and row.get('delta_calls') is None else 'high_correlation_no_growth' if high else 'not_high_or_unavailable'
        row['assessment']=grade;counts[grade]+=1
        if high:ranked.append(row)
    ranked.sort(key=lambda r:(r['assessment']!='high_correlation_with_growth',-(r['correlation']['r']),-(r.get('delta_service_seconds') or 0),r['node'],r['command']))
    grouped={}
    for row in ranked:
        if row['assessment']!='high_correlation_with_growth':continue
        group=grouped.setdefault(row['command'],{'command':row['command'],'nodes':[],'calls':0,'baseline_calls':0,'delta_calls':0,'delta_service_seconds':0,'r_max':-1,'r_min':1,'nested':row['nested']})
        group['nodes'].append(row['node'])
        for key in ('calls','delta_calls','delta_service_seconds'):group[key]+=row[key]
        group['baseline_calls']+=row['baseline']['calls'];group['r_max']=max(group['r_max'],row['correlation']['r']);group['r_min']=min(group['r_min'],row['correlation']['r'])
    command_groups=sorted(grouped.values(),key=lambda row:(-max(0,row['delta_service_seconds']),-row['r_max'],row['command']))
    keys,key_meta=hot_key_ranking(service,identity,start,end,base_start,base_end,cpu,discontinuous,node)
    from .redis_client_ranking import client_key_ranking
    client_keys,client_meta=client_key_ranking(service,identity,start,end,cpu) if not node and not discontinuous else ([],{'status':'node_filter_or_epoch_change'})
    keys.extend(client_keys);keys.sort(key=lambda r:-(r['correlation']['r']))
    key_meta['client']=client_meta
    return {'command_groups':command_groups[:20],'command_groups_truncated':len(command_groups)>20,'commands':ranked[:30],'commands_truncated':len(ranked)>30,'command_count':len(ranked),'assessments':dict(counts),
            'keys':keys[:30],'keys_truncated':len(keys)>30,'key_status':key_meta,
            'nodes':[{'node':n,**v} for n,v in sorted(node_changes.items(),key=lambda item:int(item[0].split('-')[-1]))],
            'method':'signed_pearson_zero_lag_observed_minute_rates','minimum_points':MIN_POINTS,'high_r':HIGH_R,
            'scope':'same_node_interval_end_minutes; no interpolation, missing data or absent top keys not zero-filled',
            'counterevidence':'同涨同落不证明因果；Lua/EXEC可能嵌套，成本不相加算CPU；高相关但未增长的命令不能解释窗口上涨。','causal':False}


def hot_key_ranking(service,identity,start,end,base_start,base_end,cpu,broken,node=''):
    filt,args=(' AND node=?',[node]) if node else ('',[])
    timelines=[defaultdict(dict),defaultdict(dict)];truncated=False;invalid=0
    with service.db() as c:
        for side,(lo,hi) in enumerate(((start,end),(base_start,base_end))):
            records=c.execute('SELECT node,ts,payload FROM redis_evidence WHERE instance=? AND kind=\'keys\' AND ts>=? AND ts<?'+filt+' ORDER BY ts LIMIT 20001',(identity,lo,hi,*args)).fetchall()
            truncated|=len(records)>20000
            for record in records[:20000]:
                t=int(record['ts']//60)*60
                for key in decode(record['payload']).get('items',[]):
                    # Big-key size or rank position is not a request-rate observation.
                    if key.get('kind')!='HotKeys':continue
                    try:qps=float(key['qps'])
                    except (ValueError,TypeError,KeyError):invalid+=1;continue
                    if not math.isfinite(qps) or qps<0:invalid+=1;continue
                    k=(record['node'],str(key.get('db')),key['key'])
                    timelines[side][k][t]=qps
    rows=[];uncomputable=0
    for (n,db,key),values in timelines[0].items():
        r=association(values,cpu.get(n,{}))
        if n in broken:r.update(r=None,status='node_epoch_or_collection_changed')
        if r['r'] is None:uncomputable+=1;continue
        if r['r']<HIGH_R:continue
        old=timelines[1].get((n,db,key),{})
        # Compare only paired relative minutes; disappearance from a Top list is unknown.
        shift=start-base_start;paired=[t for t in values if t-shift in old]
        delta=sum(values[t]-old[t-shift] for t in paired)/len(paired) if len(paired)>=MIN_POINTS and not truncated else None
        rows.append({'node':n,'db':db,'key':key,'source':'threshold_hot_key_qps','correlation':r,'qps_observed':sum(values.values())/len(values),
                     'delta_qps_observed':delta,'baseline_paired_points':len(paired),'assessment':'high_correlation_with_growth' if delta is not None and delta>0 else 'high_correlation_only',
                     'scope':'threshold_top_key_snapshots_not_full_key_traffic','causal':False})
    rows.sort(key=lambda r:(r['assessment']!='high_correlation_with_growth',-r['correlation']['r'],r['node'],r['key']))
    return rows,{'source':'threshold_hot_key_qps','observed_shapes':len(timelines[0]),'uncomputable_shapes':uncomputable,'truncated':truncated,'invalid_points':invalid,
                 'reason':'没有足够连续热点QPS点或业务全量计数时，无法确定高相关Key；不拿大Key大小/队列长度冒充命令调用量。'}
