/* Read-only StarRocks adapter inside the existing analytics workspace. */
let starrocksRequest=0,starrocksStatus=null,starrocksBindings=false,starrocksTimer=null,starrocksRows=[];
const SR_NAMES={metrics:'节点监控',audit:'SQL 审计',slow:'慢查询',workload:'SQL 负载',loads:'导入记录',running:'在途查询'};
const SR_ERRORS={starrocks_audit_window_max_one_hour:'审计、负载和导入每次最多一小时，请缩小时间。',starrocks_audit_outside_source_retention:'开始时间超出源审计默认30天保留期。',starrocks_database_busy:'已有 SR 查询运行，请稍后重试。',starrocks_audit_not_configured:'此实例未配置审计只读身份。',starrocks_source_timezone_unverified:'源时间口径未通过校验，已停止查询。',starrocks_identity_not_readonly:'账号不再满足只读权限校验，已停止连接。'};
function srMode(){return $('#starrocks-mode').value;}
function srVisible(){return $('#analytics-source').value==='starrocks';}
function srFields(){
  const mode=srMode();
  document.querySelectorAll('[data-sr-metrics]').forEach(x=>x.hidden=mode!=='metrics');
  document.querySelectorAll('[data-sr-workload]').forEach(x=>x.hidden=mode!=='workload');
  document.querySelectorAll('[data-sr-audit]').forEach(x=>x.hidden=!['audit','slow','workload'].includes(mode));
}
function srStopTimer(){if(starrocksTimer){clearTimeout(starrocksTimer);starrocksTimer=null;}}
function srSchedule(){
  srStopTimer();
  if(!srVisible()||!Number($('#starrocks-refresh').value)||document.hidden)return;
  starrocksTimer=setTimeout(async()=>{
    if(!srVisible()||document.hidden)return;
    // Only a preset follows now. Custom incident windows are never moved.
    if($('#analytics-range').value!=='custom')setAnalyticsRange($('#analytics-range').value);
    try{await runStarRocks();}catch(error){toast(error.message,'error');}
  },30000);
}
async function syncStarRocksMode(enabled){
  const controls=$('#starrocks-controls');if(!controls)return;
  controls.hidden=!enabled;++starrocksRequest;srStopTimer();
  if(!enabled)return;
  for(const id of ['analytics-instance','analytics-filters']){
    const element=$('#'+id);if(element)(id==='analytics-instance'?element.closest('label'):element).hidden=true;
  }
  $('#analytics-tabs').hidden=true;$('#analytics-lock-warning').hidden=true;$('#analytics-node-field').hidden=true;
  if(!starrocksBindings){
    starrocksBindings=true;
    $('#starrocks-controls').addEventListener('change',()=>{
      ++starrocksRequest;starrocksRows=[];srFields();srStopTimer();
      $('#analytics-panel-sql').innerHTML='';$('#analytics-empty').hidden=false;
      $('#analytics-meta').textContent='条件已变更，请重新排查';
      if($('#starrocks-refresh').value!=='0')srSchedule();
    });
    $('#analytics-form').addEventListener('change',event=>{
      if(!srVisible()||event.target.closest('#starrocks-controls'))return;
      ++starrocksRequest;srStopTimer();starrocksRows=[];
      $('#analytics-panel-sql').innerHTML='';$('#analytics-empty').hidden=false;
      $('#analytics-meta').textContent='时间范围已变更，请重新排查';
    });
    $('#analytics-panel-sql').addEventListener('click',event=>{
      const button=event.target.closest('[data-sr-row]');if(!button)return;
      const row=starrocksRows[Number(button.dataset.srRow)];if(!row)return;
      $('#detail-title').textContent=String(row.queryId||row.id||'StarRocks 审计');
      $('#detail-body').innerHTML=detailBlock('只读审计记录（生产写命令仅供核对，勿重放）',JSON.stringify(row,null,2));
      showDetailDrawer();
    });
    document.addEventListener('visibilitychange',()=>{if(document.hidden)srStopTimer();else srSchedule();});
  }
  if($('#analytics-range').value!=='custom')setAnalyticsRange('5m');
  $('#analytics-panel-sql').innerHTML='';$('#analytics-empty').hidden=false;
  $('#analytics-meta').textContent='正在读取 StarRocks 配置…';
  $('#analytics-coverage').textContent='读取源审计和云监控；不把未读到的记录当作零。';
  switchAnalyticsTab('sql');srFields();
  const request=starrocksRequest;
  try{
    const data=await api('/api/starrocks/status');
    if(request!==starrocksRequest||!srVisible())return;
    starrocksStatus=data;
    $('#starrocks-instance').innerHTML=(data.instances||[]).map(x=>`<option value="${escapeHtml(x.id)}">${escapeHtml(x.label)}</option>`).join('');
    $('#starrocks-metric').innerHTML=(data.metrics||[]).map(x=>`<option value="${escapeHtml(x.id)}">${escapeHtml(x.label)}</option>`).join('');
    $('#analytics-meta').textContent=data.status==='ok'?'选择内容后点击开始排查':'StarRocks 未登记或配置不可用';
  }catch(error){if(request===starrocksRequest&&srVisible())$('#analytics-meta').textContent=`StarRocks 配置读取失败：${error.message}`;}
}
function starrocksNumber(value,metric){
  if(value==null||typeof value==='boolean'||value===''||!Number.isFinite(Number(value)))return '—';
  const n=metric==='be_memory'?Number(value)/1024**3:Number(value);
  return n.toLocaleString('zh-CN',{maximumFractionDigits:2});
}
function starrocksGraph(rows,metric,label,unit){
  if(rows.length<2)return '<p>不足两个监控点，无法绘制趋势。</p>';
  const first=rows[0].timestamp,last=rows.at(-1).timestamp;if(last<=first)return '<p>时间冲突，不能绘图。</p>';
  const scale=metric==='be_memory'?1024**3:1,max=Math.max(...rows.map(x=>Number(x.value)/scale),1);
  const segments=[];let current=[];
  rows.forEach((p,i)=>{if(i&&p.timestamp-rows[i-1].timestamp>p.period*1500){segments.push(current);current=[];}
    current.push(`${((p.timestamp-first)/(last-first)*900).toFixed(2)},${(110-Number(p.value)/scale/max*100).toFixed(2)}`);});segments.push(current);
  return `<div class="spark"><svg viewBox="0 0 920 125" role="img" aria-label="${escapeHtml(label)}，缺失时断线">${segments.map(line=>`<polyline points="${line.join(' ')}" fill="none" stroke="var(--accent,#216e57)" stroke-width="2"/>`).join('')}</svg></div><p class="analytics-note">${escapeHtml(formatTime(first*1000))} — ${escapeHtml(formatTime(last*1000))} · 范围 ${starrocksNumber(Math.min(...rows.map(x=>x.value)),metric)} — ${starrocksNumber(Math.max(...rows.map(x=>x.value)),metric)} ${escapeHtml(unit)}；断线表示缺点。</p>`;
}
function srRenderMetrics(data){
  const unit=data.metric==='be_memory'?'GiB':data.unit;
  const groups=(data.nodes||[]).map(node=>({node,points:data.points.filter(p=>p.node===node)}));
  const rows=groups.map(g=>{const p=g.points,last=p.at(-1);return [escapeHtml(g.node),escapeHtml(formatTime(last.timestamp*1000)),starrocksNumber(last.value,data.metric),starrocksNumber(Math.min(...p.map(x=>x.value)),data.metric),starrocksNumber(Math.max(...p.map(x=>x.value)),data.metric),String(p.length)];});
  $('#analytics-meta').textContent=`${data.label} · ${groups.length} 个返回节点 · ${data.period}秒粒度`;
  $('#analytics-coverage').textContent=data.warning;
  $('#analytics-panel-sql').innerHTML=`<section class="detail-block"><h3>${escapeHtml(data.label)} · ${escapeHtml(unit)}</h3>${analyticsTable(['节点','最新时间','最新','最小','最大','采样点'],rows,'没有返回监控点，不能视为零或节点正常。')}</section>`+groups.map(g=>`<section class="detail-block"><h4>${escapeHtml(g.node)}</h4>${starrocksGraph(g.points,data.metric,data.label,unit)}</section>`).join('');
}
function srCost(group,key,divisor=1){
  const c=group.costs?.[key];return !c||c.value==null?'未上报':`${starrocksNumber(c.value/divisor)}${c.known<group.count?`<br><small>字段 ${c.known}/${group.count}</small>`:''}`;
}
function srRenderData(data,mode){
  const coverage=data.coverage||{},partial=coverage.truncated||data.truncated;
  $('#analytics-coverage').textContent=`${data.status==='permission_denied'?'未取得查询数据。':partial?'部分结果：达到读取上限。':'仅已观察记录：未证明源端全量。'} ${data.warning||coverage.warning||''}`;
  $('#analytics-meta').textContent=data.status==='permission_denied'?'在途查询 · 权限不足，未读取':`${SR_NAMES[mode]} · ${mode==='workload'?`${data.observed_executions} 次已观察执行 / ${data.total_groups} 个模板`:`${data.rows?.length||0} 条返回记录`}`;
  let output='';
  if(mode==='workload'){
    const rows=(data.groups||[]).map(g=>[
      `<details><summary>${escapeHtml(g.database||'未指明库')} · ${escapeHtml(g.action)} · SQL 模板</summary><pre>${escapeHtml(g.sql)}</pre>${g.truncated_sql?'<p>正文不完整，不与完整模板合并。</p>':''}</details>`,
      (g.accounts||[]).map(x=>escapeHtml(x.name)).join('<br>'),(g.clients||[]).map(x=>`${escapeHtml(x.name)} ×${x.count}`).join('<br>'),
      `${g.count}<br><small>失败 ${g.failed} / ≥1秒 ${g.slow}</small>`,srCost(g,'cpuCostNs',1e9),srCost(g,'scanBytes',1024**2),srCost(g,'queryTime',1000),starrocksNumber(g.peak_memory==null?null:g.peak_memory/1024**2)]);
    output=`<h3>已观察 SQL 成本</h3><p>按所选成本排序，不是资源上涨根因榜。扫描字节不是物理IO；语句总成本不重复归到JOIN各表；内存只列单次峰值。</p>${analyticsTable(['SQL 模板','账号','来源IP','次数','CPU秒','扫描MiB','耗时秒','峰值MiB'],rows,'窗口内没有返回已完成审计，不能视为无请求。')}<p>原始记录 ${data.source_records}，合并重复 ${data.duplicate_records}，成本冲突 ${data.duplicate_cost_conflicts}；当前列出 ${(data.groups||[]).length}/${data.total_groups} 个模板。</p>`;
  }else if(mode==='audit'||mode==='slow'){
    starrocksRows=data.rows||[];
    const rows=starrocksRows.map((r,i)=>[escapeHtml(formatTime(r.event_us)),`<button type="button" class="button ghost compact" data-sr-row="${i}" title="查看SQL和审计详情">查看SQL</button><br>${escapeHtml(r.db||'未指明库')}`,escapeHtml(r.user||'未上报'),escapeHtml(r.clientIp||'未上报'),escapeHtml(r.state||'未知'),starrocksNumber(r.queryTime),starrocksNumber(r.scanRows),starrocksNumber(r.cpuCostNs==null?null:r.cpuCostNs/1e6)]);
    output=`<h3>${SR_NAMES[mode]}</h3>${analyticsTable(['开始时间','SQL / 对象','账号','来源IP','状态','耗时ms','扫描行','CPUms'],rows,'没有返回审计记录，不能视为源端没有请求。')}<p>新到旧有界快照，不用OFFSET跨变化结果翻页；需要更多记录时缩小时间范围。原始SQL最多32KiB，超长会标记。</p>`;
  }else if(mode==='loads'){
    const rows=(data.rows||[]).map(r=>[escapeHtml(r.CREATE_TIME||''),escapeHtml(r.DB_NAME||''),escapeHtml(r.TABLE_NAME||''),escapeHtml(r.USER||''),escapeHtml(r.TYPE||''),escapeHtml(r.STATE||''),starrocksNumber(r.SINK_ROWS),starrocksNumber(r.SCAN_BYTES==null?null:r.SCAN_BYTES/1024**2)]);
    output=`<h3>导入作业</h3>${analyticsTable(['开始时间','数据库','表','账号','类型','状态','导入行','扫描MiB'],rows,'没有返回导入记录，不证明没有写入。')}`;
  }else if(data.status==='permission_denied'){
    output=`<h3>无法读取全局在途查询</h3><p>${escapeHtml(data.warning)}</p><p>不能用只显示当前账号的PROCESSLIST冒充全集群实时请求。</p>`;
  }else{
    output=`<h3>在途查询快照</h3>${detailBlock('返回数据',JSON.stringify(data.rows||[],null,2))}`;
  }
  $('#analytics-panel-sql').innerHTML=`<section class="detail-block">${output}</section>`;
}
async function runStarRocks(){
  const instance=$('#starrocks-instance').value,mode=srMode();
  if(!instance||starrocksStatus?.status!=='ok')throw new Error('StarRocks 实例未登记');
  const start=new Date($('#analytics-start').value).getTime()*1000,end=new Date($('#analytics-end').value).getTime()*1000;
  if(mode!=='running'&&(!Number.isFinite(start)||!Number.isFinite(end)||end<=start))throw new Error('请选择有效时间。');
  if(mode!=='running'&&mode!=='metrics'&&end-start>3600e6)throw new Error('审计、负载和导入每次最多一小时，请缩小时间。');
  const request=++starrocksRequest;srStopTimer();starrocksRows=[];
  $('#analytics-meta').textContent=`正在读取${SR_NAMES[mode]}…`;$('#analytics-panel-sql').innerHTML='';$('#analytics-empty').hidden=true;
  try{
    const query=new URLSearchParams({instance,mode,metric:$('#starrocks-metric').value,startEpochUs:String(start),endEpochUs:String(end),database:$('#starrocks-database').value,account:$('#starrocks-account').value,order:$('#starrocks-order').value,limit:mode==='workload'?'50':'100'});
    const data=await api('/api/starrocks/'+(mode==='metrics'?'metrics':'query')+'?'+query);
    if(request!==starrocksRequest||!srVisible())return;
    if(mode==='metrics')srRenderMetrics(data);else srRenderData(data,mode);
    switchAnalyticsTab('sql');
  }catch(error){
    if(request!==starrocksRequest||!srVisible())return;
    const message=SR_ERRORS[error.message]||error.message;
    $('#analytics-meta').textContent='读取未完成：'+message;$('#analytics-coverage').textContent='读取失败，不把缺失结果当作正常。';
    $('#analytics-panel-sql').innerHTML='';$('#analytics-empty').hidden=false;
    throw new Error(message);
  }finally{if(request===starrocksRequest)srSchedule();}
}
// Kept for callers of the initial cloud-only draft; uses the same safe path.
async function runStarRocksMetrics(){return runStarRocks();}
