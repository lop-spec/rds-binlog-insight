/* Mongo is an engine adapter inside the existing analytics workspace. */
let mongoStatus = null, mongoResult = null, mongoRequest = 0, mongoBindings = false;
const MONGO_METRICS = {CPUUtilization:'CPU 使用率 %',MemoryUtilization:'内存使用率 %',ConnectionAmount:'连接数',IOPSUtilization:'IOPS 使用率 %',ReplicationLag:'复制延迟 s',QPS:'QPS',ScannedDocs:'扫描文档 / 秒',ScannedKeys:'扫描索引 / 秒',ReadIops:'读 IOPS',WriteIops:'写 IOPS',AvgRt:'平均响应 μs',ReadAvgRt:'读取平均响应 μs',WriteAvgRt:'写入平均响应 μs',WtCacheUsage:'WT 缓存使用率 %',WtCacheDirtyUsage:'WT 脏页使用率 %',ConcurrentReads:'读等待',ConcurrentWrites:'写等待',CentralCacheFree:'TCMalloc 中央空闲 MiB',TcmallocCacheMemRatio:'TCMalloc 内存碎片率 %'};
function mongoNumber(v, digits=2) { return v == null || !Number.isFinite(Number(v)) ? '—' : Number(v).toLocaleString('zh-CN',{maximumFractionDigits:digits}); }
function mongoAllocatorFree(value) {
  const t=value?.tcmalloc||{},groups=[['pageheap_free_bytes'],['central_cache_free','central_cache_free_bytes'],['transfer_cache_free','transfer_cache_free_bytes'],['thread_cache_free','thread_cache_free_bytes'],['cpu_free'],['sharded_transfer_cache_free']];
  const known=groups.map(keys=>keys.map(k=>t[k]).find(v=>v!=null&&Number.isFinite(Number(v)))).filter(v=>v!=null);
  return known.length?known.reduce((s,v)=>s+Number(v),0):null;
}
function mongoScale(v, divisor) { return v == null ? null : v/divisor; }
function mongoDelta(v) { return v == null ? '基线或字段未完整' : `${v>0?'+':''}${mongoNumber(v)}`; }
function mongoOptions(rows) { return rows.map(([v,t])=>`<option value="${escapeHtml(v)}">${escapeHtml(t)}</option>`).join(''); }

async function syncMongoMode(enabled) {
  const controls = $('#mongo-controls'); if (!controls) return;
  controls.hidden = !enabled;
  for (const id of ['analytics-instance','analytics-filters']) {
    const element = $('#'+id); if (element) (id==='analytics-instance'?element.closest('label'):element).hidden=enabled;
  }
  if (!enabled) return;
  $('#analytics-tabs').hidden=true; $('#analytics-lock-warning').hidden=true; $('#analytics-node-field').hidden=true;
  state.analyticsTab='sql'; switchAnalyticsTab('sql');
  if (!mongoBindings) {
    mongoBindings=true;
    $('#mongo-replay').addEventListener('change',()=>{
      const c=mongoStatus?.replays?.find(x=>x.id===$('#mongo-replay').value); if (!c) return;
      $('#mongo-instance').value=c.instance;$('#analytics-start').value=toLocalInput(new Date(c.start_us/1000));
      $('#analytics-end').value=toLocalInput(new Date(c.end_us/1000));$('#analytics-range').value='custom';
      $('#mongo-baseline').value='custom';$('#mongo-baseline-start').value=toLocalInput(new Date(c.baseline_start_us/1000));
      $('#mongo-role').value=c.role||'Primary';$('#mongo-order').value=c.order||'duration_growth';
      $('#mongo-metric').value=c.metric||'CPUUtilization';$('#mongo-kind').value='command';$('#mongo-command').value='';$('#mongo-namespace').value='';
    });
    $('#analytics-panel-sql').addEventListener('click',e=>{
      const button=e.target.closest('[data-mongo-group]'); if (button) openMongoDetail(button.dataset.mongoGroup,button.dataset.mongoRole);
    });
  }
  if (mongoStatus) return;
  try {
    mongoStatus=await api('/api/mongo/status');
    $('#mongo-instance').innerHTML=mongoOptions((mongoStatus.instances||[]).map(x=>[x.id,x.label]));
    $('#mongo-metric').innerHTML=mongoOptions((mongoStatus.metrics||[]).map(x=>[x,MONGO_METRICS[x]||x]));
    $('#mongo-replay').innerHTML=mongoOptions([['','选择已固定的异常与基线'],...(mongoStatus.replays||[]).map(x=>[x.id,x.label])]);
    if (mongoStatus.status!=='ok') $('#analytics-meta').textContent=`MongoDB ${mongoStatus.reason||mongoStatus.status}`;
  } catch(e) { $('#analytics-meta').textContent=`MongoDB 配置读取失败：${e.message}`; }
}

async function runMongoAnalytics() {
  const request=++mongoRequest;
  let start=new Date($('#analytics-start').value).getTime()*1000,end=new Date($('#analytics-end').value).getTime()*1000;
  if ($('#analytics-range').value!=='custom') {
    const current=await api('/api/mongo/status');
    if(request!==mongoRequest||$('#analytics-source').value!=='mongodb')return;
    const watermark=current.collectors?.find(x=>x.instanceId===$('#mongo-instance').value)?.slowlog_window;
    if(Number.isFinite(watermark)&&watermark>0){const span=end-start;end=watermark;start=end-span;$('#analytics-start').value=toLocalInput(new Date(start/1000));$('#analytics-end').value=toLocalInput(new Date(end/1000));}
  }
  if (!Number.isFinite(start)||!Number.isFinite(end)||end<=start) throw new Error('请选择有效的异常窗口');
  let baseline=$('#mongo-baseline').value==='previous'?start-(end-start):start-86400*1e6;
  if ($('#mongo-baseline').value==='custom') baseline=new Date($('#mongo-baseline-start').value).getTime()*1000;
  if (!Number.isFinite(baseline)) throw new Error('请选择有效的基线开始时间');
  const params=new URLSearchParams({instance:$('#mongo-instance').value,startEpochUs:String(start),endEpochUs:String(end),baselineStart:String(baseline),
    role:$('#mongo-role').value,kind:$('#mongo-kind').value,metric:$('#mongo-metric').value,order:$('#mongo-order').value,
    command:$('#mongo-command').value,namespace:$('#mongo-namespace').value,limit:$('#analytics-limit').value});
  $('#analytics-meta').textContent='正在读取 MongoDB 既有汇总、计数与性能点…';
  const data=await api('/api/mongo/analytics?'+params);
  if (request!==mongoRequest||$('#analytics-source').value!=='mongodb') return;
  mongoResult=data;renderMongoAnalytics(data);
}

function mongoSparkline(points) {
  const rows=[...points].sort((a,b)=>a.timestamp-b.timestamp); if (rows.length<2) return '<p>性能序列不足；不补造缺失数据。</p>';
  const lo=rows[0].timestamp,hi=rows.at(-1).timestamp,max=Math.max(...rows.map(x=>Number(x.value)),1);
  // Separate segments at missing minutes instead of drawing through gaps.
  const segments=[];let current=[];
  rows.forEach((p,i)=>{if(i&&p.timestamp-rows[i-1].timestamp>90000){segments.push(current);current=[];}current.push(`${((p.timestamp-lo)/(hi-lo)*900).toFixed(2)},${(110-Number(p.value)/max*100).toFixed(2)}`);});segments.push(current);
  return `<svg viewBox="0 0 920 125" role="img" aria-label="${escapeHtml(MONGO_METRICS[rows[0].metric]||rows[0].metric)}性能曲线，断点保留缺口" width="100%" height="160">${segments.map(x=>`<polyline points="${x.join(' ')}" fill="none" stroke="var(--accent,#216e57)" stroke-width="2"/>`).join('')}</svg>`;
}

function renderMongoAnalytics(data) {
  $('#analytics-empty').hidden=true;
  const coverage=data.coverage||{},baseline=data.baseline_coverage||{};
  $('#analytics-coverage').innerHTML=`<span>MongoDB · 当前 ${coverage.collected_windows??0}/${coverage.expected_windows??0} 窗口，基线 ${baseline.collected_windows??0}/${baseline.expected_windows??0} 窗口 · ${coverage.complete&&baseline.complete?'窗口采集完整（仍是慢记录子集）':'存在缺口，增长结论不可用'} · 分桶 ${mongoNumber(data.bucket_width/6e7)} 分钟</span>`;
  const totals=(data.totals||[]).map(x=>`${escapeHtml(x.role)} ${escapeHtml(x.kind)}：${mongoNumber(x.baseline_count)} → ${mongoNumber(x.count)}`).join('；');
  $('#analytics-meta').textContent=`${data.total_groups} 个命令模板 · ${data.status} · 基线 ${formatTime(data.baseline_start)} → ${formatTime(data.baseline_end)}`;
  const rows=(data.statements||[]).map(x=>[
    `<button type="button" class="button ghost compact" data-mongo-group="${escapeHtml(x.group_id)}" data-mongo-role="${escapeHtml(x.role)}">${escapeHtml(x.namespace)}<br><strong>${escapeHtml(x.command)}</strong> · ${escapeHtml(x.role)}</button>`,
    `${mongoNumber(x.baseline_count)} → ${mongoNumber(x.count)}<br>${mongoDelta(x.count_delta)}`,
    `${mongoNumber(x.avg_us/1000)} ms / ${mongoNumber(x.max_us/1000)} ms`,
    `${mongoDelta(x.costs.duration_us.delta==null?null:x.costs.duration_us.delta/1e6)} s`,
    mongoDelta(x.costs.docs.delta),
    `${mongoNumber(x.evidence.difference_r,3)}<br>${escapeHtml(x.evidence.metric_status)}`,
    `${x.conclusion==='candidate'?'增量候选（未证明因果）':'证据不足 / 无成本增长'}${x.new_slow_shape?'<br>新出现于慢日志':''}`,
  ]);
  const performance=(data.metric_points||[]).filter(x=>!$('#mongo-role').value||x.role===$('#mongo-role').value);
  const roles=[...new Set(performance.map(x=>x.role))];
  const outlierRows=(data.outliers||[]).map(x=>[`<button class="button ghost compact" data-mongo-group="${escapeHtml(x.group_id)}" data-mongo-role="${escapeHtml(x.role)}">${escapeHtml(x.namespace)} · ${escapeHtml(x.command)}</button>`,mongoNumber(x.max_us/1000)+' ms',formatTime(x.sample.start_us),(x.exclusions||[]).map(escapeHtml).join('；')||'没有足够反证，不等于已证明因果']);
  const counterRows=(data.native_counters||[]).map(x=>[escapeHtml(x.node),escapeHtml(x.role),`<details><summary>${escapeHtml(x.command)} · 分钟趋势</summary>${mongoSparkline((x.intervals||[]).map(p=>({timestamp:p.end_us/1000,value:p.qps,metric:'QPS'})))}</details>`,mongoNumber(x.count),`${mongoNumber(x.baseline_qps)} → ${mongoNumber(x.qps)}`,mongoDelta(x.qps_delta),`${mongoNumber(x.coverage_seconds)} / ${mongoNumber(x.window_seconds)} 秒；基线 ${mongoNumber(x.baseline_coverage_seconds)} 秒`,mongoNumber(x.failed)]);
  const memory=(data.native_latest||[]).map(x=>{
    const g=x.tcmalloc?.generic||{},c=x.wt_cache||{},free=mongoAllocatorFree(x.tcmalloc);
    return [escapeHtml(x.node),escapeHtml(x.role),formatTime(x.timestamp*1000),mongoNumber(x.mem?.resident/1024),mongoNumber(c['bytes currently in the cache']/2**30),mongoNumber(c['maximum bytes configured']/2**30),mongoNumber(c['tracked dirty bytes in the cache']/2**30),mongoNumber(g.current_allocated_bytes/2**30),mongoNumber(mongoScale(free,2**30)),mongoNumber(x.connections?.current),mongoNumber(x.cursor?.open?.total),`${mongoNumber(x.global_lock?.currentQueue?.readers)} / ${mongoNumber(x.global_lock?.currentQueue?.writers)}`];
  });
  const clients=(data.client_aggregates||[]).map(x=>[escapeHtml(x.service),escapeHtml(x.namespace),escapeHtml(x.command),mongoNumber(x.count),mongoNumber(x.failed),mongoNumber(x.lost)]);
  $('#analytics-panel-sql').innerHTML=`<section class="detail-block"><h3>慢命令增量与性能关联</h3><p>${totals}</p><p class="analytics-note">${escapeHtml(data.warning)} · 只对所选层级计算。缺 CPU/读取字段显示未知，不补零。快捷时间对齐最新完整慢日志窗口；自定义时间不改写。</p>
    ${roles.map(role=>`<h4>${escapeHtml(role)} · ${escapeHtml(MONGO_METRICS[data.metric]||data.metric)}</h4>${mongoSparkline(performance.filter(x=>x.role===role))}`).join('')||'<p>没有匹配的性能数据。</p>'}
    ${analyticsTable(['集合族 / 命令','慢记录次数与增量','平均 / 最大耗时','累计耗时增量','扫描文档增量','性能差分 r','判断'],rows)}
    </section><details class="detail-block"><summary>最长慢命令与反证（独立于增量榜）</summary>${analyticsTable(['命令','最长耗时','代表样本开始','排除项'],outlierRows)}</details><details class="detail-block"><summary>节点命令总次数（不是慢日志计数）</summary><p>按服务器原生计数器的连续区间相减；缺失和跨进程区间不计。QPS 使用已覆盖秒数，不外推完整窗口。基线与当前各至少两个有效区间才比较观测 QPS；不等于全窗口次数增长。</p>${analyticsTable(['节点','角色','命令','计数增量','基线 → 当前 QPS','观测 QPS 变化','当前 / 窗口；基线覆盖','失败'],counterRows,'该历史窗口未采集原生命令计数，不能从慢日志补出来。')}${detailBlock('计数缺口',JSON.stringify({current:data.native_gaps||[],baseline:data.native_baseline_gaps||[]}))}</details>
    <details class="detail-block"><summary>内存组成与当前节点状态</summary>${analyticsTable(['节点','角色','样本时间','RSS GiB','WT GiB','WT 上限 GiB','WT 脏页 GiB','实际分配 GiB','已知空闲 GiB（不含 unmapped）','连接','打开游标','读 / 写排队'],memory,'该窗口无原生内存快照。')}</details>
    <details class="detail-block"><summary>集合 / 模板全量次数（接入服务范围）</summary><p>仅统计注册 Command Monitoring 的服务；未接入时不显示虚构的全量次数。</p>${analyticsTable(['服务','集合','命令','尝试次数','失败','丢失'],clients,'尚无应用命令聚合接入。')}</details>
    <details class="detail-block"><summary>可复算数据与缺口</summary>${detailBlock('数据覆盖',JSON.stringify({coverage,baseline,optional:data.optional_unavailable},null,2))}</details>`;
  switchAnalyticsTab('sql');
}

function openMongoDetail(id,role) {
  const x=[...(mongoResult?.statements||[]),...(mongoResult?.outliers||[])].find(x=>x.group_id===id&&x.role===role); if(!x)return;
  $('#detail-title').textContent=`${x.namespace} · ${x.command}`;
  const costRows=Object.entries(x.costs).map(([k,v])=>[escapeHtml(k),mongoNumber(v.baseline),mongoNumber(v.observed),mongoDelta(v.delta),`${v.known}/${v.total}`]);
  $('#detail-body').innerHTML=`<section class="detail-block"><h3>异常增量与反证</h3><p>${x.conclusion==='candidate'?'该模板有成本或慢记录增量，是排查候选，不等于已经证明性能根因。':'当前证据不能确认该模板导致异常。'}</p><p>${(x.exclusions||[]).map(escapeHtml).join('；')||'未触发时序排除项，仍需直接成本和业务验证。'}</p>
    <p>次数 ${mongoNumber(x.baseline_count)} → ${mongoNumber(x.count)}；频次成本项 ${mongoNumber(mongoScale(x.frequency_cost_delta_us,1e6))} s；单次成本项 ${mongoNumber(mongoScale(x.per_call_cost_delta_us,1e6))} s。基线为零时不能计算该分解。</p>
    ${analyticsTable(['指标（原始单位）','基线已记录值','当前已记录值','完整字段增量','当前字段覆盖'],costRows)}
    ${detailBlock('规范化命令（脱敏）',JSON.stringify(x.shape,null,2))}${detailBlock('代表慢记录与成本',JSON.stringify(x.sample,null,2))}
    ${detailBlock('时序关联口径',JSON.stringify(x.evidence,null,2))}${detailBlock('分钟 / 汇总桶序列',JSON.stringify(x.trend,null,2))}</section>`;
  showDetailDrawer();
}
