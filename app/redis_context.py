"""Scoped Kubernetes context + cloud/key evidence for the existing Redis worker."""
import hashlib
import json
import re
import time
import urllib.parse
import urllib.request
import urllib.error
from .redis_telemetry import config_numbers
from .redis_protocol import safe_key,RedisTelemetryError,key_shape,error_code


class WorkloadReader:
    def __init__(self, inventory):self.inventory=inventory

    def get(self,cluster,namespace,kind,name=''):
        prefix={'deployments':'/apis/apps/v1','replicasets':'/apis/apps/v1','horizontalpodautoscalers':'/apis/autoscaling/v2','pods':'/api/v1','configmaps':'/api/v1'}.get(kind)
        if not prefix or not re.fullmatch(r'c[a-z0-9]{32}',cluster) or not re.fullmatch(r'[a-z0-9-]{1,63}',namespace):raise ValueError('redis_context_scope_invalid')
        if name and not re.fullmatch(r'[a-z0-9.-]{1,253}',name):raise ValueError('redis_context_name_invalid')
        if kind=='configmaps' and not name:raise ValueError('redis_configmap_name_required')
        config=self.inventory._kubeconfig(cluster);tls=self.inventory._tls(config);rows=[];token='';seen=set()
        for _ in range(10):
            query={'limit':'500'} if not name else {}
            if token:query['continue']=token
            path=f'{prefix}/namespaces/{namespace}/{kind}'+('/'+name if name else '')
            request=urllib.request.Request(config['server']+path+('?' + urllib.parse.urlencode(query) if query else ''),headers={'Accept':'application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1' if kind=='replicasets' and not name else 'application/json'})
            with urllib.request.urlopen(request,timeout=10,context=tls) as r:
                raw=r.read(8*1024*1024+1)
            if len(raw)>8*1024*1024:raise RuntimeError('redis_context_byte_limit')
            body=json.loads(raw)
            if name:return body
            rows.extend(body.get('items',[]));token=body.get('metadata',{}).get('continue','')
            if not token:return rows
            if token in seen:raise RuntimeError('redis_context_page_cycle')
            seen.add(token)
        raise RuntimeError('redis_context_page_limit')

    def snapshot(self,source):
        cluster,namespace=source['clusterId'],source.get('namespace','default')
        names=source.get('deployments',[])
        if not isinstance(names,list) or not 1<=len(names)<=30:raise ValueError('redis_workloads_required')
        wanted=[]
        for name in names:
            try:wanted.append(self.get(cluster,namespace,'deployments',name))
            except urllib.error.HTTPError as exc:
                if exc.code!=404:raise
        hpas=self.get(cluster,namespace,'horizontalpodautoscalers')
        pods=self.get(cluster,namespace,'pods');replicasets=self.get(cluster,namespace,'replicasets')
        rs_owners={r['metadata']['uid']:next((o['uid'] for o in r['metadata'].get('ownerReferences',[]) if o.get('kind')=='Deployment'),'') for r in replicasets}
        rs_names={r['metadata']['uid']:r['metadata']['name'] for r in replicasets}
        result=[];configs={};missing=[]
        for name in names:
            d=next((d for d in wanted if d['metadata']['name']==name),None)
            if d is None:missing.append(name);continue
            uid=d['metadata']['uid'];spec=d.get('spec',{});template=spec.get('template',{}).get('spec',{})
            seen=[]
            for p in pods:
                owner=next((o['uid'] for o in p['metadata'].get('ownerReferences',[]) if o.get('kind')=='ReplicaSet'),'')
                if rs_owners.get(owner)!=uid:continue
                st=p.get('status',{});ready=any(c.get('type')=='Ready' and c.get('status')=='True' for c in st.get('conditions',[]))
                seen.append({'pod':p['metadata']['name'],'uid':p['metadata']['uid'],'start':st.get('startTime'),'ready':ready,'replicaset':rs_names.get(owner),'terminating':bool(p['metadata'].get('deletionTimestamp'))})
            hpa=next((h for h in hpas if h.get('spec',{}).get('scaleTargetRef',{}).get('name')==name and h.get('spec',{}).get('scaleTargetRef',{}).get('kind')=='Deployment'),None)
            h=None if hpa is None else {'name':hpa['metadata']['name'],'min':hpa['spec'].get('minReplicas'),'max':hpa['spec'].get('maxReplicas'),'current':hpa.get('status',{}).get('currentReplicas'),'desired':hpa.get('status',{}).get('desiredReplicas'),'last_scale':hpa.get('status',{}).get('lastScaleTime')}
            cm_names=set(v['configMap']['name'] for v in template.get('volumes',[]) if v.get('configMap',{}).get('name'))
            for container in template.get('containers',[]):
                cm_names.update(x['configMapRef']['name'] for x in container.get('envFrom',[]) if x.get('configMapRef',{}).get('name'))
            worker={};provenance=[]
            for cm_name in sorted(cm_names):
                cm=self.get(cluster,namespace,'configmaps',cm_name)
                for key,text in cm.get('data',{}).items():
                    if not isinstance(text,str) or len(text)>1024*1024:continue
                    numbers=config_numbers(text)
                    if numbers:
                        worker.update(numbers);provenance.append({'configmap':cm_name,'field':key})
            configs[name]={'values':worker,'source':provenance,'scope':'configmap_literals_not_effective_runtime','missing_or_dynamic':not bool(worker)}
            result.append({'deployment':name,'uid':uid,'replicas':spec.get('replicas'),'ready':d.get('status',{}).get('readyReplicas',0),'available':d.get('status',{}).get('availableReplicas',0),'images':sorted(c.get('image','') for c in template.get('containers',[])),'hpa':h,'pods':sorted(seen,key=lambda p:p['pod'])})
        return {'cluster':cluster,'namespace':namespace,'deployments':result,'missing_deployments':missing,'source_complete':not missing},configs


class RedisContext:
    def __init__(self,service,reader=None):
        self.service=service;self.history=service.evidence;self.reader=reader;self.next={};self.cursors={};self.state={}

    def due(self,entry,kind,seconds):
        key=(entry['instanceId'],kind);now=self.service.clock()
        if now<self.next.get(key,0):return False
        self.next[key]=now+seconds;return True

    def run(self,entry,nodes):
        identity=entry['instanceId'];now=self.service.clock();errors={}
        tasks=[]
        if self.due(entry,'source',60) and entry.get('sources'):tasks.append(('sources',lambda:self.sources(entry,now)))
        if self.due(entry,'keys',60):tasks.append(('keys',lambda:self.keys(entry,nodes,now)))
        if self.due(entry,'queues',60):tasks.append(('queues',lambda:self.queues(entry,now)))
        if self.due(entry,'events',300):tasks.append(('cloud_events',lambda:self.events(entry,now)))
        if entry.get('prefixProfile',False) and self.due(entry,'profile',20):tasks.append(('profile',lambda:self.profile(entry,nodes,now)))
        if self.due(entry,'night',3600):tasks.append(('night',lambda:self.night(entry,nodes,now)))
        for name,fn in tasks:
            if self.service.stop_event.is_set():break
            try:fn();self.state[name]={'last_success':self.service.clock()}
            except Exception as exc:
                prior=self.state.get(name,{})
                errors[name]=error_code(exc);self.state[name]={'last_attempt':now,'last_success':prior.get('last_success'),'first_failure':prior.get('first_failure',now),'failures':prior.get('failures',0)+1,'error':error_code(exc)}
        self.history.compact()
        return errors

    def sources(self,e,now):
        if self.reader is None:raise RuntimeError('redis_workload_reader_not_configured')
        snapshot,configs=self.reader.snapshot(e['sources'])
        self.history.changed(e['instanceId'],'sources','',now,snapshot,'workload_replicas_image_hpa_or_pods_changed')
        for name,value in configs.items():self.history.changed(e['instanceId'],'config',name,now,value,'worker_configuration_changed')
        self.history.put(e['instanceId'],'capability','sources',now,{'complete':snapshot['source_complete'],'config_scope':'numeric_boolean_whitelist_only; no env/secret stored'})

    def keys(self,e,nodes,now):
        for n in nodes:
            r=self.service.rpc(e,'DescribeHotBigKeys',{'NodeId':n['node_id'].split('#')[0]},das=True)
            data=r.get('Data',{});items=[];unexpected=0
            for group,array in [('HotKeys','HotKey'),('BigKeys','BigKey'),('HighTrafficKeys','HighTrafficKey'),('LargeKeys','LargeKey')]:
                for item in data.get(group,{}).get(array,[])[:100]:
                    if item.get('NodeId') and item['NodeId']!=n['node_id'].split('#')[0]:unexpected+=1;continue
                    items.append({'kind':group,'key':safe_key(item.get('Key','')),'key_type':item.get('KeyType'),'db':item.get('Db'),'size':item.get('Size'),'qps':item.get('Hot'),'bytes':item.get('DataSize'),'node_id':item.get('NodeId')})
            capability={group:('unsupported_or_error' if data.get(message) else 'available' if group in data else 'not_returned') for group,message in [('HotKeys','HotKeyMsg'),('BigKeys','BigKeyMsg'),('HighTrafficKeys','HighTrafficKeyMsg'),('LargeKeys','LargeKeyMsg')]}
            self.history.put(e['instanceId'],'keys',n['node'],now,{'items':items,'capability':capability,'unexpected_node_items':unexpected,'scope':'threshold_top_keys_not_all_keys','empty_is_not_no_activity':True})

    def queues(self,e,now):
        environments=e.get('queueEnvironments',['pre'])
        if not isinstance(environments,list) or len(environments)>3 or any(v not in ('','pre','alpha') for v in environments):raise ValueError('redis_queue_environment')
        registered=[]
        for env in environments:
            prefix=env+':' if env else ''
            for b in range(64):
                for stage in ('','processing:','sent_timeout:','acked:'):
                    registered.append((prefix+'follow_order:outbox:'+stage+'zset:{fo:'+str(b)+'}','ZCARD'))
                for kind in ('hash','ver'):
                    registered.append((prefix+'follow_order:outbox:'+kind+':{fo:'+str(b)+'}','HLEN'))
        extras=e.get('queues',[])
        if len(extras)>64:raise ValueError('redis_queue_budget')
        for q in extras:
            if set(q)!={'key','command'} or q['command'] not in ('ZCARD','HLEN','LLEN') or not re.fullmatch(r'(?:pre:|alpha:)?(?:ws:pending(?::index)?|order_buyer_match:pending)(?::[a-zA-Z0-9_{}:.-]{1,200})?',q['key']):raise ValueError('redis_unregistered_queue')
            registered.append((q['key'],q['command']))
        w=self.service.wire_factory(e);rows=[]
        try:
            for offset in range(0,len(registered),512):
                page=registered[offset:offset+512];w.allow_metadata([key for key,_ in page])
                for key,cmd in page:
                    value=w.send(cmd,key);rows.append({'key':safe_key(key),'command':cmd,'length':value})
            self.history.put(e['instanceId'],'queues','',now,{'items':rows,'registered':len(registered),'scope':'registered_queues_only','ws_pending_unregistered':not bool(extras)})
        finally:w.close()

    def events(self,e,now):
        for action in ('DescribeHistoryTasks','DescribeHistoryEvents'):
            r=self.service.rpc(e,action,{'RegionId':e['region'],'FromStartTime':self.service.iso_time(now-86400),'ToStartTime':self.service.iso_time(now),'PageSize':100,'PageNumber':1,**({'ArchiveStatus':'All'} if action.endswith('Events') else {})})
            items=r.get('Items',[])
            # No signed URL, detail blob, request parameters or unknown strings persisted.
            clean=[]
            for item in items[:100]:
                clean.append({k:v for k,v in item.items() if k in ('TaskId','TaskType','Status','EventId','EventType','EventStatus','EventCategory','StartTime','EndTime','TaskStartTime','TaskEndTime','EventStartTime','EventEndTime','EventTime') and isinstance(v,(str,int,float,bool))})
            self.history.put(e['instanceId'],'cloud_event',action,now,{'items':clean,'total':r.get('TotalCount'),'truncated':r.get('TotalCount',0)>len(items),'source':action,'scope':'cloud_visible_events_only'})

    def profile(self,e,nodes,now):
        # One shard/page per pass, at most 16 keys and 48 metadata reads.
        if not nodes:return
        ix=int(now//20)%len(nodes);node=nodes[ix];key=(e['instanceId'],node['node_id']);cursor=self.cursors.get(key,'0')
        w=self.service.wire_factory(e)
        try:
            reply=w.send('ISCAN',str(node['index']),cursor,'COUNT','16')
            next_cursor=str(reply[0]);keys=reply[1]
            if not next_cursor.isdigit() or not isinstance(keys,list):raise RuntimeError('redis_iscan_reply_invalid')
            accepted=[k for k in keys[:16] if isinstance(k,str) and len(k)<=512];w.allow_metadata(accepted)
            groups={}
            for k in accepted:
                typ=w.send('TYPE',k);ttl=w.send('TTL',k)
                if typ=='none':continue
                memory=None
                try:memory=w.send('MEMORY','USAGE',k,'SAMPLES','5')
                except RedisTelemetryError as exc:
                    if str(exc)!='redis_permission_or_command_rejected':raise
                prefix=key_shape(k);g=groups.setdefault(prefix,{'observed_keys':0,'memory_bytes_observed':0,'memory_known':0,'ttl_none':0,'ttl_expiring':0,'ttl_seconds_observed':[],'types':{}})
                g['observed_keys']+=1;g['types'][typ]=g['types'].get(typ,0)+1
                if isinstance(memory,int):g['memory_bytes_observed']+=memory;g['memory_known']+=1
                if ttl==-1:g['ttl_none']+=1
                elif isinstance(ttl,int) and ttl>=0:g['ttl_expiring']+=1;g['ttl_seconds_observed'].append(ttl)
            self.cursors[key]=next_cursor
            snapshot={'groups':groups,'cursor':cursor,'next_cursor':next_cursor,'cycle_complete':next_cursor=='0','returned_keys':len(keys),'sampled_keys':len(accepted),'truncated_page':len(keys)>16,'scope':'bounded_cursor_sample_not_uniform_not_total_prefix_cardinality','duplicate_keys_across_pages_possible':True,'memory_scope':'MEMORY USAGE SAMPLES 5; estimated object memory','db':0}
            self.history.put(e['instanceId'],'profile',node['node'],now,snapshot)
            self.service.aggregates.profile(e['instanceId'],node['node'],now,snapshot)
        finally:w.close()

    def night(self,e,nodes,now):
        # Reproducible prior local day 03:00–05:00 cloud rates, not invented usec.
        local_day=int((now+8*3600)//86400)*86400-8*3600
        start=local_day+3*3600
        if now<local_day+5*3600+300:start-=86400
        for window in (start-86400,start):
            for n in nodes:
                last=self.history.latest(e['instanceId'],'night',node=n['node'],before=window+1)
                if last and last['time']==window:continue
                r=self.service.rpc(e,'DescribeHistoryMonitorValues',{'NodeId':n['node_id'],'StartTime':self.service.iso_time(window),'EndTime':self.service.iso_time(window+7200),'IntervalForHistory':'01m','MonitorKeys':'CpuUsage,TotalQps,Redis_Keys_Monitor,Redis_Zset_Monitor,Redis_Scripting_Monitor'})
                h=json.loads(r.get('MonitorHistory') or '{}');points=[]
                from datetime import datetime
                for t,v in h.items():
                    ts=datetime.fromisoformat(t.replace('Z','+00:00')).timestamp()
                    if window<=ts<window+7200:points.append((ts,v))
                avg={k:sum(float(v[k]) for _,v in points if k in v)/sum(k in v for _,v in points) for k in ('CpuUsage','TotalQps','del','eval','evalsha','zrangebyscore') if any(k in v for _,v in points)}
                local=self.history.rollups(e['instanceId'],window,window+7200,3600,n['node'])
                commands={};local_seconds=0
                for part in local['rows']:
                    local_seconds+=part['seconds']
                    for cmd,cost in part['commands'].items():
                        target=commands.setdefault(cmd,{'calls':0,'usec':0,'seconds':0})
                        for k in target:target[k]+=cost[k]
                self.history.put(e['instanceId'],'night',n['node'],window,{'start':window,'end':window+7200,'averages':avg,'points':len(points),'complete':len(points)==120,'scope':'cloud_rate_points_not_command_duration','local_commands':commands,'local_coverage_seconds':local_seconds,'local_scope':'hour_rollups_grouped_by_interval_end; no missing commands invented'})
