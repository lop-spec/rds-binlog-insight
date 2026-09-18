/* Mongo is an engine adapter inside the existing analytics workspace. */
let mongoStatus = null, mongoResult = null, mongoRequest = 0, mongoBindings = false;
const MONGO_METRICS = {CPUUtilization:'CPU 使用率 %',IOPSUtilization:'IOPS 使用率 %',ScannedDocs:'扫描文档 / 秒',LockWaits:'锁等待（操作数）',AvgRt:'平均响应 μs'};
function mongoNumber(v, digits=2) { return v == null || !Number.isFinite(Number(v)) ? '—' : Number(v).toLocaleString('zh-CN',{maximumFractionDigits:digits}); }
function mongoAllocatorFree(value) {
  const t=value?.tcmalloc||{},groups=[['pageheap_free_bytes'],['central_cache_free','central_cache_free_bytes'],['transfer_cache_free','transfer_cache_free_bytes'],['thread_cache_free','thread_cache_free_bytes'],['cpu_free'],['sharded_transfer_cache_free']];
  const known=groups.map(keys=>keys.map(k=>t[k]).find(v=>v!=null&&Number.isFinite(Number(v)))).filter(v=>v!=null);
  return known.length?known.reduce((s,v)=>s+Number(v),0):null;
}
function mongoScale(v, divisor) { return v == null ? null : v/divisor; }
function mongoDelta(v) { return v == null ? '不可比较' : `${v>0?'+':''}${mongoNumber(v)}`; }
const MONGO_ORDERS={correlation:'所选指标相关度',duration_growth:'累计耗时增量',count_growth:'慢记录次数增量',scan_growth:'扫描文档增量',cpu_growth:'CPU 增量',read_growth:'读取量增量',performance:'性能重合',count:'已采集慢记录数',max_latency:'最长单次耗时',duration_total:'已记录累计耗时',scan_total:'已记录扫描文档',cpu_total:'已记录 CPU',read_total:'已记录读取量'};
const MONGO_REASONS={correlation_unavailable:'数据不足，暂不能计算相关度',no_slow_records:'没有可分析的慢命令',incomplete_source:'当前采集有缺口',incomplete_baseline:'基线采集有缺口',incomplete_source_or_baseline:'当前或基线采集有缺口',metric_gaps:'性能点有缺口',coarse_grain_zoom_required:'请缩至 3 小时内计算分钟关联',memory_requires_component_deltas:'内存需看组成变化',insufficient_buckets:'不足 6 个完整分钟',insufficient_points:'有效点不足',constant_sql:'命令时序无变化',constant_total:'性能时序无变化',ranking_evidence_unavailable:'排序所需证据不足',ranking_field_unavailable:'排序字段未上报',ok:'当前窗口关联（非因果）'};
function mongoReason(code){return MONGO_REASONS[code]||'当前关联不可计算';}
function mongoCost(v,scale=1,unit='') {
  if(!v||v.observed==null)return '—';
  const n=v.observed/scale,shown=n>0&&n<0.01?'＜0.01':mongoNumber(n);
  const field=v.known<v.total?`<br><small>字段覆盖 ${v.known}/${v.total}；仅已记录值</small>`:'';
  const delta=v.delta==null?'':`<br><small>增量 ${mongoDelta(v.delta/scale)} ${unit}</small>`;
  return `<strong>${shown} ${unit}</strong>${field}${delta}`;
}
function mongoCorrelation(x){
  const e=x.evidence||{},r=e.pearson;
  if(r==null)return `<span>暂不可计算</span><br><small>${escapeHtml(mongoReason(e.metric_status))}</small>`;
  return `<strong title="命令每分钟执行时长与所选性能指标的 Pearson 相关系数；相关不等于因果">r ${mongoNumber(r,3)}</strong><br><small>${r>0?'正相关':r<0?'负相关':'无线性相关'}</small>`;
}
function mongoMetricChanged(){
  ++mongoRequest;mongoResult=null;
  $('#analytics-panel-sql').innerHTML='';$('#analytics-empty').hidden=false;
  $('#analytics-meta').textContent=`已选择 ${MONGO_METRICS[$('#mongo-metric').value]}；点击开始分析，按相关度排序`;
  $('#analytics-coverage').textContent='';
}
function mongoAssessment(x){
  const names={incomplete_source:'仅列已采集成本；不判断增长',incomplete_baseline:'基线待补齐；不判断增长',incomplete_command:'命令正文不完整',after_peak:'首次出现晚于性能峰值',candidate:'增量候选（未证明因果）',insufficient_cost_fields:'部分成本未上报，不能排除增长',no_observed_growth:'已记录成本未增长'};
  return names[x.assessment]||(x.conclusion==='candidate'?names.candidate:'当前证据不足');
}
function mongoCoverageText(c){
  const ranges=[...(c.missing_ranges||[]),...(c.index_missing_ranges||[])];
  return `${c.collected_windows??0}/${c.expected_windows??0} 个五分钟窗口${c.complete?'，完整':`，缺 ${c.missing_count??'?'} 个${c.index_missing_count?`，另有 ${c.index_missing_count} 个索引缺口`:''}`}`+
    (ranges.length?`；缺口 ${ranges.slice(0,3).map(x=>`${formatTime(x.start_us)}—${formatTime(x.end_us)}`).join('；')}${ranges.length>3?` 等 ${ranges.length} 段`:''}`:'');
}
function mongoOptions(rows) { return rows.map(([v,t])=>`<option value="${escapeHtml(v)}">${escapeHtml(t)}</option>`).join(''); }

function mongoBaselineWindow(start,end,mode,custom) {
  const day=86400*1e6,span=end-start;
  if (!Number.isFinite(start)||!Number.isFinite(end)||span<=0) throw new Error('请选择有效的分析时间，结束时间必须晚于开始时间');
  if (span>7*day) throw new Error('MongoDB 单次最多分析 7 天，请缩小时间范围');
  let adjustment='';
  if (mode==='yesterday'&&span>day) {
    mode='previous';adjustment='分析范围超过 24 小时，昨日同窗会重叠，已改为前一个等长窗口；分析时间不变。';
  }
  const baseline=mode==='custom'?custom:mode==='previous'?start-span:start-day;
  if (!Number.isFinite(baseline)) throw new Error('请选择有效的基线开始时间');
  if (baseline+span>start) throw new Error('基线与分析时间重叠：请选择更早的基线开始时间，或改选“前一个等长窗口”');
  return {start:baseline,end:baseline+span,mode,adjustment};
}
function syncMongoBaseline(start=new Date($('#analytics-start').value).getTime()*1000,end=new Date($('#analytics-end').value).getTime()*1000,strict=false) {
  const select=$('#mongo-baseline'),input=$('#mongo-baseline-start'),hint=$('#mongo-baseline-hint');
  input.disabled=select.value!=='custom';
  try {
    const plan=mongoBaselineWindow(start,end,select.value,new Date(input.value).getTime()*1000);
    if (plan.adjustment) {
      select.value=plan.mode;
      console.warn('mongo_baseline adjusted: yesterday overlaps current window; using previous equal-length window');
      toast(plan.adjustment,'info',8000);
    }
    hint.textContent=`${plan.adjustment}基线：${formatTime(plan.start)} → ${formatTime(plan.end)}（等长、不重叠）`;
    return plan;
  } catch(error) {
    hint.textContent=error.message;
    if(strict)throw error;
    return null;
  }
}
function mongoAnalysisError(error) {
  const messages={baseline_must_not_overlap_current_window:'基线与分析时间重叠，请改选“前一个等长窗口”或更早的基线开始时间',mongo_window_exceeds_seven_days:'MongoDB 单次最多分析 7 天，请缩小时间范围'};
  return messages[error.message]||error.message;
}

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
      $('#mongo-role').value=c.role||'Primary';
      const metric=c.metric||'CPUUtilization';
      if(!Object.hasOwn(MONGO_METRICS,metric)){
        console.warn(`mongo_replay metric changed: ${metric} is outside workspace metrics; using CPUUtilization correlation`);
        toast('此回放的旧指标已移出工作台；保留时间范围，改按 CPU 相关度分析','info',8000);
      }
      $('#mongo-metric').value=Object.hasOwn(MONGO_METRICS,metric)?metric:'CPUUtilization';$('#mongo-kind').value='command';$('#mongo-command').value='';$('#mongo-namespace').value='';
      mongoMetricChanged();
    });
    $('#mongo-metric').addEventListener('change',mongoMetricChanged);
    $('#analytics-form').addEventListener('change',()=>{
      if($('#analytics-source').value==='mongodb')syncMongoBaseline();
    });
    $('#analytics-panel-sql').addEventListener('click',e=>{
      const button=e.target.closest('[data-mongo-group]'); if (button) openMongoDetail(button.dataset.mongoGroup,button.dataset.mongoRole);
      if(e.target.closest('[data-mongo-recent]')){setAnalyticsRange('1h');$('#mongo-baseline').value='previous';runMongoAnalytics().catch(err=>toast(err.message,'error'));}
    });
  }
  syncMongoBaseline();
  if (mongoStatus) return;
  try {
    mongoStatus=await api('/api/mongo/status');
    $('#mongo-instance').innerHTML=mongoOptions((mongoStatus.instances||[]).map(x=>[x.id,x.label]));
    $('#mongo-metric').innerHTML=mongoOptions(Object.entries(MONGO_METRICS));
    $('#mongo-replay').innerHTML=mongoOptions([['','选择已固定的异常与基线'],...(mongoStatus.replays||[]).map(x=>[x.id,x.label])]);
    if (mongoStatus.status!=='ok') $('#analytics-meta').textContent=`MongoDB ${mongoStatus.reason||mongoStatus.status}`;
  } catch(e) { $('#analytics-meta').textContent=`MongoDB 配置读取失败：${e.message}`; }
}

async function runMongoAnalytics() {
  const request=++mongoRequest;
  mongoResult=null;
  $('#analytics-panel-sql').innerHTML='';
  $('#analytics-empty').hidden=false;
  $('#analytics-coverage').textContent='正在检查 MongoDB 分析时间与基线…';
  try {
    let start=new Date($('#analytics-start').value).getTime()*1000,end=new Date($('#analytics-end').value).getTime()*1000;
    let baseline=syncMongoBaseline(start,end,true);
    if ($('#analytics-range').value!=='custom') {
      const current=await api('/api/mongo/status');
      if(request!==mongoRequest||$('#analytics-source').value!=='mongodb')return;
      const watermark=current.collectors?.find(x=>x.instanceId===$('#mongo-instance').value)?.slowlog_window;
      if(Number.isFinite(watermark)&&watermark>0){const span=end-start;end=watermark;start=end-span;$('#analytics-start').value=toLocalInput(new Date(start/1000));$('#analytics-end').value=toLocalInput(new Date(end/1000));}
      baseline=syncMongoBaseline(start,end,true);
    }
    const params=new URLSearchParams({instance:$('#mongo-instance').value,startEpochUs:String(start),endEpochUs:String(end),baselineStart:String(baseline.start),
      role:$('#mongo-role').value,kind:$('#mongo-kind').value,metric:$('#mongo-metric').value,order:'correlation',
      command:$('#mongo-command').value,namespace:$('#mongo-namespace').value,limit:$('#analytics-limit').value});
    $('#analytics-meta').textContent='正在读取 MongoDB 既有汇总、计数与性能点…';
    $('#analytics-coverage').textContent=$('#mongo-baseline-hint').textContent;
    const data=await api('/api/mongo/analytics?'+params);
    if (request!==mongoRequest||$('#analytics-source').value!=='mongodb') return;
    mongoResult=data;renderMongoAnalytics(data);
  } catch(error) {
    if(request!==mongoRequest||$('#analytics-source').value!=='mongodb')return;
    const message=mongoAnalysisError(error);
    $('#analytics-meta').textContent=`MongoDB 分析未完成：${message}`;
    $('#analytics-coverage').textContent=message;
    $('#analytics-panel-sql').innerHTML='';
    $('#analytics-empty').hidden=false;
    mongoResult=null;
    throw new Error(message);
  }
}

function mongoSparkline(points) {
  const rows=[...points].sort((a,b)=>a.timestamp-b.timestamp); if (rows.length<2) return '<p>性能序列不足；不补造缺失数据。</p>';
  const lo=rows[0].timestamp,hi=rows.at(-1).timestamp,max=Math.max(...rows.map(x=>Number(x.value)),1);
  // Separate segments at missing minutes instead of drawing through gaps.
  const segments=[];let current=[];
  rows.forEach((p,i)=>{if(i&&p.timestamp-rows[i-1].timestamp>90000){segments.push(current);current=[];}current.push(`${((p.timestamp-lo)/(hi-lo)*900).toFixed(2)},${(110-Number(p.value)/max*100).toFixed(2)}`);});segments.push(current);
  return `<div class="spark"><svg viewBox="0 0 920 125" role="img" aria-label="${escapeHtml(MONGO_METRICS[rows[0].metric]||rows[0].metric)}性能曲线，断点保留缺口">${segments.map(x=>`<polyline points="${x.join(' ')}" fill="none" stroke="var(--accent,#216e57)" stroke-width="2"/>`).join('')}</svg></div><p class="analytics-note">${formatTime(lo*1000)} — ${formatTime(hi*1000)} · 已有点范围 ${mongoNumber(Math.min(...rows.map(x=>Number(x.value))))} — ${mongoNumber(Math.max(...rows.map(x=>Number(x.value))))}；断线表示缺点，不补零。</p>`;
}

function renderMongoAnalytics(data) {
  $('#analytics-empty').hidden=true;
  const coverage=data.coverage||{},baseline=data.baseline_coverage||{};
  $('#analytics-coverage').innerHTML=`<span>MongoDB · 当前 ${escapeHtml(mongoCoverageText(coverage))}<br>基线 ${escapeHtml(mongoCoverageText(baseline))}</span>`;
  const totals=(data.totals||[]).map(x=>`${escapeHtml(x.role)} ${x.kind==='suboperation'?'内部操作':'外层命令'}：当前已采集 ${mongoNumber(x.count)} 条${x.baseline_count==null?'':`，基线 ${mongoNumber(x.baseline_count)} 条`}`).join('；');
  const automatic=data.order==='correlation';
  $('#analytics-meta').textContent=automatic?`${data.total_groups} 个命令模板 · 按 ${MONGO_METRICS[data.metric]||data.metric} 相关度从高到低${data.ranking?.available===false?' · 暂无可计算结果':''}`:`${data.total_groups} 个命令模板 · 按${MONGO_ORDERS[data.order]||data.order}排序`;
  const history=data.collection_status||{},slow=history.history_slow_progress,metricProgress=history.history_metric_progress;
  const recovery=history.enabled?`后台近 48 小时回补：慢日志 ${slow?`${slow.collected_windows}/${slow.expected_windows} 窗口`:'等待调度'}；云指标 ${metricProgress?`${metricProgress.collected_hours}/${metricProgress.expected_hours} 小时已抓取`:'等待调度'}。实时采集优先，不倒退水位；历史原生命令计数无法补造。${history.history_pause?' 当前暂停，优先追赶实时水位。':''}`:'历史回补未启用。';
  const historyErrors=['history_slowlog','history_metrics'].filter(k=>history[k]&&history[k]!=='ok').map(k=>`${k}：${history[k]}`).join('；');
  const scopeNotice=automatic?(!coverage.complete?`当前窗口：${mongoCoverageText(coverage)}。数据尚未完整，相关度暂不可计算；下方命令信息仅供参考。`:!baseline.complete?'对照窗口数据尚未完整，不展示增减。相关度只依赖当前窗口，不受对照窗口缺口影响。':''):((!coverage.complete||!baseline.complete)?`当前窗口：${mongoCoverageText(coverage)}。基线：${mongoCoverageText(baseline)}。下面仍展示已采集记录的成本；不外推全窗口，不判断增减。`:'');
  const orderNotice=automatic?(data.ranking?.available===false?`所选指标相关度暂不可计算：${mongoReason(data.ranking.reason)}。保留当前指标，不改按耗时或次数排序。`:''):(data.order_reason?`原排序“${MONGO_ORDERS[data.requested_order]||data.requested_order}”不可用（${mongoReason(data.order_reason)}），已明确改按“${MONGO_ORDERS[data.order]||data.order}”排序，不是异常增量榜。`:'');
  const rows=(data.statements||[]).map(x=>[
    `<button type="button" class="button ghost compact" data-mongo-group="${escapeHtml(x.group_id)}" data-mongo-role="${escapeHtml(x.role)}">${escapeHtml(x.namespace)}${x.scope==='database'?' · 库级命令':''}<br><strong>${['unknown','command'].includes(x.command)?'命令类型未识别':escapeHtml(x.command)}</strong> · ${escapeHtml(x.role)}</button>${x.incomplete?'<br><small>正文不完整，详情保留已知成本</small>':''}`,
    mongoCorrelation(x),
    `<strong>${mongoNumber(x.count)} 条</strong>${x.baseline_count==null?'':`<br>基线 ${mongoNumber(x.baseline_count)} 条`}${x.count_delta==null?'':`<br>增量 ${mongoDelta(x.count_delta)}`}`,
    `${mongoCost(x.costs.duration_us,1e6,'s')}<br>平均 ${mongoNumber(x.avg_us/1000)} ms<br>最大 ${mongoNumber(x.max_us/1000)} ms`,
    `扫描 ${mongoCost(x.costs.docs)}<br>读取 ${mongoCost(x.costs.bytes_read,2**20,'MiB')}`,
    `CPU ${mongoCost(x.costs.cpu_ns,1e9,'s')}<br>写关注等待 ${mongoCost(x.costs.write_wait_us,1e6,'s')}`,
  ]);
  const performance=(data.metric_points||[]).filter(x=>!$('#mongo-role').value||x.role===$('#mongo-role').value);
  const roles=[...new Set(performance.map(x=>x.role))];
  const outlierRows=(data.outliers||[]).map(x=>[`<button class="button ghost compact" data-mongo-group="${escapeHtml(x.group_id)}" data-mongo-role="${escapeHtml(x.role)}">${escapeHtml(x.namespace)}${x.scope==='database'?' · 库级命令':''} · ${escapeHtml(x.command)}</button>`,mongoNumber(x.max_us/1000)+' ms',formatTime(x.sample.start_us),(x.exclusions||[]).map(escapeHtml).join('；')||'没有足够反证，不等于已证明因果']);
  const counterRows=(data.native_counters||[]).map(x=>[escapeHtml(x.node),escapeHtml(x.role),`<details><summary>${escapeHtml(x.command)} · 分钟趋势</summary>${mongoSparkline((x.intervals||[]).map(p=>({timestamp:p.end_us/1000,value:p.qps,metric:'QPS'})))}</details>`,mongoNumber(x.count),`${mongoNumber(x.baseline_qps)} → ${mongoNumber(x.qps)}`,mongoDelta(x.qps_delta),`${mongoNumber(x.coverage_seconds)} / ${mongoNumber(x.window_seconds)} 秒；基线 ${mongoNumber(x.baseline_coverage_seconds)} 秒`,mongoNumber(x.failed)]);
  const memory=(data.native_latest||[]).map(x=>{
    const g=x.tcmalloc?.generic||{},c=x.wt_cache||{},free=mongoAllocatorFree(x.tcmalloc);
    return [escapeHtml(x.node),escapeHtml(x.role),formatTime(x.timestamp*1000),mongoNumber(x.mem?.resident/1024),mongoNumber(c['bytes currently in the cache']/2**30),mongoNumber(c['maximum bytes configured']/2**30),mongoNumber(c['tracked dirty bytes in the cache']/2**30),mongoNumber(g.current_allocated_bytes/2**30),mongoNumber(mongoScale(free,2**30)),mongoNumber(x.connections?.current),mongoNumber(x.cursor?.open?.total),`${mongoNumber(x.global_lock?.currentQueue?.readers)} / ${mongoNumber(x.global_lock?.currentQueue?.writers)}`];
  });
  const clients=(data.client_aggregates||[]).map(x=>[escapeHtml(x.service),escapeHtml(x.namespace),escapeHtml(x.command),mongoNumber(x.count),mongoNumber(x.failed),mongoNumber(x.lost)]);
  const namespaceTop=data.namespace_top||{};
  const lockSeconds=x=>mongoNumber(mongoScale((x.read_us||0)+(x.write_us||0),1e6));
  const namespaceRows=(namespaceTop.collections||[]).map(x=>[escapeHtml(x.namespace),mongoNumber(x.read_count),mongoNumber(mongoScale(x.read_us,1e6)),mongoNumber(x.write_count),mongoNumber(mongoScale(x.write_us,1e6)),lockSeconds(x)]);
  const familyRows=(namespaceTop.families||[]).map(x=>[escapeHtml(x.family),mongoNumber(x.shards,0),mongoNumber(x.read_count),mongoNumber(mongoScale(x.read_us,1e6)),mongoNumber(x.write_count),mongoNumber(mongoScale(x.write_us,1e6)),lockSeconds(x)]);
  const namespaceNote=namespaceTop.unavailable==='window_exceeds_180_minutes'
    ? '窗口超过 180 分钟时不做该聚合；缩小窗口后可见。'
    : `仅统计完整落在窗口内的区间：${mongoNumber(namespaceTop.intervals,0)} 个区间、覆盖 ${mongoNumber(namespaceTop.observed_seconds)} 秒。${namespaceTop.truncated_intervals?'单次采样的集合列表按锁时间截断，分表族合计不受截断影响。':''}`;
  $('#analytics-panel-sql').innerHTML=`<section class="detail-block"><h3>慢命令成本与性能关联</h3><p>${totals}</p>${scopeNotice||orderNotice?`<div class="notice"><div><strong>${escapeHtml(orderNotice||'窗口数据尚未完整')}</strong><p>${escapeHtml(scopeNotice)}</p><button type="button" class="button secondary compact" data-mongo-recent>改查最近 1 小时，对比前 1 小时</button></div></div>`:''}<p class="analytics-note">${escapeHtml(recovery)} ${escapeHtml(historyErrors)} 重新分析可查看回补后的结果。</p><p class="analytics-note">${escapeHtml(data.warning)} · 只对所选层级计算。字段未上报不补零；部分字段展示覆盖条数。快捷时间对齐最新完整慢日志窗口；自定义时间不改写。</p>
    ${roles.map(role=>`<h4>${escapeHtml(role)} · ${escapeHtml(MONGO_METRICS[data.metric]||data.metric)}</h4>${mongoSparkline(performance.filter(x=>x.role===role))}`).join('')||'<p>没有匹配的性能数据。</p>'}
    ${analyticsTable(['集合族 / 命令','所选指标相关度','慢命令次数','累计 / 平均 / 最大耗时','扫描文档 / 读取量','CPU / 写关注等待'],rows)}
    </section><details class="detail-block"><summary>最长慢命令与反证（补充排查）</summary>${analyticsTable(['命令','最长耗时','代表样本开始','排除项'],outlierRows)}</details><details class="detail-block"><summary>节点命令总次数（不是慢日志计数）</summary><p>按服务器原生计数器的连续区间相减；缺失和跨进程区间不计。QPS 使用已覆盖秒数，不外推完整窗口。基线与当前各至少两个有效区间才比较观测 QPS；不等于全窗口次数增长。</p>${analyticsTable(['节点','角色','命令','计数增量','基线 → 当前 QPS','观测 QPS 变化','当前 / 窗口；基线覆盖','失败'],counterRows,'该历史窗口未采集原生命令计数，不能从慢日志补出来。')}${detailBlock('计数缺口',JSON.stringify({current:data.native_gaps||[],baseline:data.native_baseline_gaps||[]}))}</details>
    <details class="detail-block"><summary>内存组成与当前节点状态</summary>${analyticsTable(['节点','角色','样本时间','RSS GiB','WT GiB','WT 上限 GiB','WT 脏页 GiB','实际分配 GiB','已知空闲 GiB（不含 unmapped）','连接','打开游标','读 / 写排队'],memory,'该窗口无原生内存快照。')}</details>
    <details class="detail-block"><summary>集合占用（服务器原生 top，不是慢日志）</summary><p>按 namespace 的读写锁次数与锁持有时间相减得到：它回答"哪张表忙"，不区分具体语句；微秒是锁持有时间（含等待），不是 CPU 时间。${escapeHtml(namespaceNote)}</p>${analyticsTable(['分表族','分表数','读次数','读锁 s','写次数','写锁 s','合计锁 s'],familyRows,'该窗口没有完整的集合区间。')}${analyticsTable(['集合','读次数','读锁 s','写次数','写锁 s','合计锁 s'],namespaceRows,'该窗口没有完整的集合区间。')}</details>
    <details class="detail-block"><summary>集合 / 模板全量次数（接入服务范围）</summary><p>仅统计注册 Command Monitoring 的服务；未接入时不显示虚构的全量次数。</p>${analyticsTable(['服务','集合','命令','尝试次数','失败','丢失'],clients,'尚无应用命令聚合接入。')}</details>
    <details class="detail-block"><summary>可复算数据与缺口</summary>${detailBlock('数据覆盖',JSON.stringify({coverage,baseline,optional:data.optional_unavailable},null,2))}</details>`;
  switchAnalyticsTab('sql');
}

function openMongoDetail(id,role) {
  const x=[...(mongoResult?.statements||[]),...(mongoResult?.outliers||[])].find(x=>x.group_id===id&&x.role===role); if(!x)return;
  $('#detail-title').textContent=`${x.namespace}${x.scope==='database'?' · 库级命令':''} · ${x.command}`;
  const costRows=Object.entries(x.costs).map(([k,v])=>[escapeHtml(k),mongoNumber(v.baseline),mongoNumber(v.observed),v.delta==null?'不可比较（见窗口与字段覆盖）':mongoDelta(v.delta),`${v.known}/${v.total}；基线 ${v.baseline_known??'?'}/${v.baseline_total??'?'}`]);
  $('#detail-body').innerHTML=`<section class="detail-block"><h3>命令详情与时序核查</h3><p>${mongoCorrelation(x)} · ${escapeHtml(MONGO_METRICS[x.evidence?.metric]||x.evidence?.metric||'')}。统计范围为慢日志，相关不等于因果。</p><p>${(x.exclusions||[]).map(escapeHtml).join('；')||'未触发时序排除项，仍需直接成本和业务验证。'}</p>
    <p>次数 ${mongoNumber(x.baseline_count)} → ${mongoNumber(x.count)}；频次成本项 ${mongoNumber(mongoScale(x.frequency_cost_delta_us,1e6))} s；单次成本项 ${mongoNumber(mongoScale(x.per_call_cost_delta_us,1e6))} s。基线为零时不能计算该分解。</p>
    ${analyticsTable(['指标（原始单位）','对照窗口','当前窗口','增减（数据完整时）','数据覆盖'],costRows)}
    ${detailBlock('规范化命令（脱敏）',JSON.stringify(x.shape,null,2))}${detailBlock('代表慢记录与成本',JSON.stringify(x.sample,null,2))}
    ${detailBlock('时序关联口径',JSON.stringify(x.evidence,null,2))}${detailBlock('分钟 / 汇总桶序列',JSON.stringify(x.trend,null,2))}</section>`;
  showDetailDrawer();
}
