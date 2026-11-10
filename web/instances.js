/* Engine-aware main instance controls. No credentials or caller SQL. */
let srAuditRequest=0,srAuditDetails=new Map(),srAuditUi=false,srMainBindings=false;
function registeredInstance(id){return state.status?.instanceRegistry?.find(x=>x.id===id);}
function isStarRocksInstance(id){return registeredInstance(id)?.engine==='starrocks';}
function isStarRocksAudit(){return isStarRocksInstance($('#filter-instance')?.value);}
function srAuditRange(range){
  ++indexedRangeRequest;++srAuditRequest;
  const spans={'15m':900000,'1h':3600000,'24h':86400000,'7d':604800000,'30d':2592000000};
  if(!Object.hasOwn(spans,range))return;
  const end=new Date();end.setSeconds(0,0);
  $('#filter-start').value=toLocalInput(new Date(end.getTime()-spans[range]));
  $('#filter-end').value=toLocalInput(end);$('#filter-end').setCustomValidity('');
  $('#indexed-range-hint').textContent='SR 源审计按查询开始时间筛选；每次最多一小时，不依赖Binlog索引。';
}
function syncStarRocksAuditControls(){
  const sr=isStarRocksAudit();
  if(!sr&&!srAuditUi)return false;
  const changed=sr!==srAuditUi;srAuditUi=sr;
  const source=$('#filter-source');
  if(sr){
    if(![...source.options].some(o=>o.value==='starrocks')){const o=document.createElement('option');o.value='starrocks';o.textContent='StarRocks SQL 审计';source.appendChild(o);}
    source.value='starrocks';source.disabled=true;
  }else{source.querySelector('option[value="starrocks"]')?.remove();source.value='binlog';source.disabled=false;}
  $('#filter-query-mode').disabled=sr;
  if(sr)$('#filter-query-mode').value='keyword';
  $('#filter-table').disabled=sr;$('#filter-table').closest('label').hidden=sr;
  const fields=$('#view-audit .operation-filter');if(fields)fields.hidden=sr;
  const tasks=$('#view-audit .query-tasks');if(tasks)tasks.hidden=sr;
  const notice=$('#indexed-range-hint').closest('.notice');if(notice)notice.hidden=sr;
  $('#starrocks-audit-note').hidden=!sr;
  $('#audit-query-help').textContent=sr?'StarRocks 源审计直接只读查询，可按SQL关键词、数据库、账号和状态筛选；不是逐行变更。':'Binlog 按完整库表＋关键词查行变更，或按已知主键精确定位。只读完整索引，不扫描原档。';
  if(changed){++srAuditRequest;srAuditDetails.clear();renderEvents({rows:[],has_more:false,tiers_used:[]});$('#result-meta').textContent=sr?'已连接 StarRocks 源审计，点击查询读取记录。':'等待查询';}
  if(!sr)return false;
  for(const id of ['connection','account','status']){const e=$('#filter-'+id);e.disabled=false;$('#filter-'+id+'-field').hidden=false;}
  const cancelled=$('#filter-status option[value="cancelled"]');if(cancelled)cancelled.disabled=true;
  if($('#filter-status').value==='cancelled')$('#filter-status').value='';
  $('#filter-keyword').disabled=false;$('#filter-value-field').hidden=false;
  $('#filter-keyword-mode').disabled=true;$('#filter-keyword-mode-field').hidden=true;
  $('#filter-value-label').textContent='SQL关键词';$('#filter-keyword').placeholder='SQL原文包含的片段（原样匹配）';
  $('#filter-database').placeholder='完整数据库名';$('#filter-account').placeholder='完整账号';
  $('#filter-connection').placeholder='完整客户端IP:端口';$('#binlog-query-policy').hidden=true;
  $('#query-export').disabled=true;$('#query-export').title='SR为有界源端快照，暂不支持导出';
  if(changed&&$('#audit-range').value!=='custom'){$('#audit-range').value='15m';srAuditRange('15m');}
  return true;
}
function resetStarRocksAuditControls(){
  if(syncStarRocksAuditControls())return true;
  const cancelled=$('#filter-status option[value="cancelled"]');if(cancelled)cancelled.disabled=false;
  $('#filter-keyword-mode-field').hidden=false;
  return false;
}
function srAuditEvent(row,instance){
  return {event_id:row.id,instance_id:instance,event_time_utc:new Date(row.event_us/1000).toISOString(),
    database_name:row.db||'',table_name:'',host_instance_id:row.feIp||'',raw_event_type:'STARROCKS_AUDIT',
    sql_kind:'ORIGINAL',sql_text:row.stmt||'',operation:sql_action_ui(row.stmt),
    database_account:row.user||'',connection_name:row.clientIp||'',connection_id:row.queryId||'',
    execution_status:row.state==='ERR'?'failed':['OK','EOF'].includes(row.state)?'success':'unknown',
    source_file_name:'StarRocks 源审计',start_position:'—',end_position:'—'};
}
function sql_action_ui(sql){return /^(?:\s*\/\*[\s\S]*?\*\/)*\s*(\w+)/.exec(sql||'')?.[1]?.toUpperCase()||'OTHER';}
async function runStarRocksAudit(){
  if(state.queryPromise)return state.queryPromise;
  const instance=$('#filter-instance').value,request=++srAuditRequest;
  const start=new Date($('#filter-start').value).getTime()*1000,end=new Date($('#filter-end').value).getTime()*1000;
  if(!Number.isFinite(start)||!Number.isFinite(end)||end<=start||end-start>3600e6)throw new Error('SR 审计每次请选择一小时内的有效时间。');
  state.queryPromise=withBusy($('#query-submit'),async()=>{
    $('#result-meta').textContent='正在读取 StarRocks 源审计…';srAuditDetails.clear();
    const q=new URLSearchParams({instance,mode:'audit',startEpochUs:String(start),endEpochUs:String(end),limit:'100',
      database:$('#filter-database').value.trim(),account:$('#filter-account').value.trim(),keyword:$('#filter-keyword').value.trim(),
      connection:$('#filter-connection').value.trim(),status:$('#filter-status').value});
    const data=await api('/api/starrocks/query?'+q);
    if(request!==srAuditRequest||!isStarRocksAudit()||$('#filter-instance').value!==instance)return;
    state.queryOffset=0;
    const rows=(data.rows||[]).map(r=>{srAuditDetails.set(r.id,r);return srAuditEvent(r,instance);});
    renderEvents({rows,has_more:false,tiers_used:['StarRocks 源审计']});
    $('#page-label').textContent='有界快照';$('#page-prev').disabled=true;$('#page-next').disabled=true;
    $('#result-meta').textContent=`starrocks-prod · ${rows.length} 条已返回记录${data.coverage?.truncated?' · 达到上限，部分结果':''}；长查询与插件晚到可能补入，不等于源端全量。`;
    $('#starrocks-audit-note').textContent=data.warning||data.coverage?.warning||'SR源审计，不是逐行变更。';
    return data;
  },'读取中…');
  try{return await state.queryPromise;}catch(error){
    if(request===srAuditRequest&&isStarRocksAudit()){renderEvents({rows:[],has_more:false,tiers_used:[]});$('#result-meta').textContent='SR 读取失败：'+error.message;}
    throw error;
  }finally{state.queryPromise=null;}
}
function openStarRocksAuditDetail(id){
  const row=srAuditDetails.get(id);if(!row)return false;
  $('#detail-title').textContent='StarRocks SQL 审计';$('#detail-body').innerHTML=detailBlock('源审计记录（生产写命令仅供核对，勿重放）',JSON.stringify(row,null,2));showDetailDrawer();return true;
}
async function selectAnalyticsInstance(){
  const id=$('#analytics-instance').value,entry=registeredInstance(id);
  const source=entry?.engine==='starrocks'?'starrocks':entry?.engine==='mongodb'?'mongodb':'rds';
  if($('#analytics-source').value!==source){$('#analytics-source').value=source;await syncAnalyticsMode();}
  if(source==='starrocks')$('#starrocks-instance').value=id;
  else if(source==='mongodb')$('#mongo-instance').value=id;
}
async function runStarRocksSessions(){
  const instance=$('#sessions-instance').value;
  $('#sessions-live').hidden=false;$('#sessions-history').hidden=true;
  for(const id of ['sessions-holders','sessions-stats','sessions-list'])$('#'+id).innerHTML='';
  $('#sessions-summary').innerHTML='';$('#sessions-retention').hidden=true;
  if(sessionsState.mode==='history'){
    $('#sessions-meta').textContent='StarRocks 未留存在途快照历史';
    $('#sessions-summary').textContent='源审计可查已完成请求；不能用完成审计补造历史在途查询。';return;
  }
  $('#sessions-meta').textContent='正在读取 StarRocks 在途查询…';
  const data=await api('/api/starrocks/query?'+new URLSearchParams({instance,mode:'running'}));
  if($('#sessions-instance').value!==instance)return;
  $('#sessions-meta').textContent=data.status==='permission_denied'?'StarRocks · 权限不足，未取得全局在途查询':'StarRocks · 当前只读快照';
  $('#sessions-summary').innerHTML=detailBlock('StarRocks 在途查询',data.status==='permission_denied'?data.warning:JSON.stringify(data.rows||[],null,2));
}
function bindMainInstanceControls(){
  if(srMainBindings)return;srMainBindings=true;
  $('#analytics-instance').addEventListener('change',()=>selectAnalyticsInstance().catch(e=>toast(e.message,'error')));
  $('#query-form').addEventListener('input',()=>{if(isStarRocksAudit())++srAuditRequest;});
  $('#query-form').addEventListener('change',()=>{if(isStarRocksAudit())++srAuditRequest;});
}
