/* Redis command counters are evidence, not an additive CPU attribution model. */
let redisRequest=0,redisStatus=null,redisBound=false;
function redisVisible(){return $('#analytics-source').value==='redis';}
function redisNumber(x){return x==null||typeof x==='boolean'||!Number.isFinite(Number(x))?'—':Number(x).toLocaleString('zh-CN',{maximumFractionDigits:3});}
async function syncRedisMode(enabled){
  const controls=$('#redis-controls');if(!controls)return;
  controls.hidden=!enabled;++redisRequest;
  if(!enabled)return;
  for(const id of ['analytics-instance','analytics-filters']){const n=$('#'+id);if(n)(id==='analytics-instance'?n.closest('label'):n).hidden=true;}
  $('#analytics-tabs').hidden=true;$('#analytics-lock-warning').hidden=true;$('#analytics-node-field').hidden=true;
  $('#analytics-panel-sql').innerHTML='';$('#analytics-empty').hidden=false;switchAnalyticsTab('sql');
  if(!redisBound){
    redisBound=true;
    $('#analytics-form').addEventListener('change',()=>{if(!redisVisible())return;++redisRequest;$('#analytics-panel-sql').innerHTML='';$('#analytics-empty').hidden=false;$('#analytics-meta').textContent='条件已变更，请重新排查';});
    $('#redis-instance').addEventListener('change',redisNodes);
  }
  const request=redisRequest;
  try{
    const data=await api('/api/redis/status');if(request!==redisRequest||!redisVisible())return;
    redisStatus=data;
    $('#redis-instance').innerHTML=(data.instances||[]).map(x=>`<option value="${escapeHtml(x.id)}">${escapeHtml(x.label)}</option>`).join('');
    redisNodes();
    const item=data.instances?.[0];
    const stale=!item?.latest || Date.now()/1000-item.latest>90;
    const failed=stale||item?.health?.errors?.length||item?.health?.last_error;
    $('#analytics-meta').textContent=data.status==='ok'?`${failed?'采集待恢复':'持续记录'} · ${item?.interval_seconds||20}秒 · 本机保留${item?.retention_days||30}天`:'Redis 尚未登记或配置不可用';
    $('#analytics-coverage').textContent=data.warning||'未连接，不把缺失数据当作零。';
  }catch(error){if(request===redisRequest&&redisVisible())$('#analytics-meta').textContent='Redis状态读取失败：'+error.message;}
}
function redisNodes(){
  const item=redisStatus?.instances?.find(x=>x.id===$('#redis-instance').value);
  $('#redis-node').innerHTML='<option value="">全部分片</option>'+(item?.nodes||[]).map(x=>`<option value="${escapeHtml(x.node)}">${escapeHtml(x.node)}</option>`).join('');
}
function renderRedis(data){
  $('#analytics-meta').textContent=`命令上涨排查 · ${(data.rows||[]).length}项${data.rows_truncated?'（部分）':''} · 昨日同窗对照`;
  $('#analytics-coverage').textContent=data.warning;
  const coverage=(data.coverage||[]).map(x=>[escapeHtml(x.node),`${redisNumber(x.seconds)} / ${redisNumber(x.expected_seconds)}秒`,`${redisNumber(x.baseline_seconds)} / ${redisNumber(x.expected_seconds)}秒`]);
  const commands=(data.rows||[]).map(x=>[escapeHtml(x.node),escapeHtml(x.command)+(x.nested?'<br><small>含嵌套，不相加</small>':''),redisNumber(x.calls),redisNumber(x.qps),redisNumber(x.service_seconds),redisNumber(x.avg_us),x.delta_service_seconds==null?'基线不足':redisNumber(x.delta_service_seconds)]);
  let output=`<section class="detail-block"><h3>记录覆盖</h3>${analyticsTable(['分片','本窗已记录','昨日已记录'],coverage,'尚无覆盖记录。')}<p>只汇总完整落在所选窗口内的差分段；边界、断采和切换缺口不外推。两窗命令覆盖均≥95%才计算增量。</p></section><section class="detail-block"><h3>命令执行成本</h3><p>优先按昨日同窗耗时增量排序；缺基线时仅列已记录耗时，不宣称上涨根因。</p>${analyticsTable(['分片','命令','调用次数','次/秒','累计执行秒','平均微秒','较昨日执行秒'],commands,'没有已记录命令；开始采集前的历史耗时无法恢复。')}</section>`;
  const rates=(data.rate_rows||[]).map(x=>[escapeHtml(x.node),escapeHtml(x.command),redisNumber(x.qps),redisNumber(x.baseline_qps),`${x.samples} / ${x.baseline_samples}`]);
  output+=`<section class="detail-block"><h3>云监控命令QPS</h3><p>各自已返回采样点的速率均值，不是逐次调用记录；不含命令执行耗时，采样不足不外推。</p>${analyticsTable(['分片','命令','本窗次/秒','昨日次/秒','本窗/昨日点数'],rates,'尚无命令速率记录。')}</section>`;
  const points=(data.metrics||[]).filter(x=>x.CpuUsage!=null);
  const nodes=[...new Set(points.map(x=>x.node))];
  output+=nodes.map(node=>`<section class="detail-block"><h3>${escapeHtml(node)} · CPU %</h3>${starrocksGraph(points.filter(x=>x.node===node).map(x=>({timestamp:x.timestamp,value:x.CpuUsage,period:5})),'cpu','云监控CPU','%')}</section>`).join('');
  const buckets=data.buckets;
  if(buckets){
    const groups={};(buckets.rows||[]).forEach(x=>(groups[x.node_index]??=[]).push(x.bucket));
    output+=`<section class="detail-block"><h3>64桶归属</h3><p>最近已记录快照：${escapeHtml(formatTime(buckets.time*1e6))}。不是窗口内每一刻的槽位证明。</p>${analyticsTable(['节点索引','桶数','桶号'],Object.entries(groups).map(([n,b])=>[escapeHtml(n),String(b.length),escapeHtml(b.join(', '))]))}</section>`;
  }else output+='<section class="detail-block"><h3>64桶归属</h3><p>窗口结束前未记录快照，不用未来快照补历史。</p></section>';
  const slow=(data.slow_events||[]).map(x=>[escapeHtml(formatTime(x.time*1e6)),escapeHtml(x.node),escapeHtml(x.command),redisNumber(x.usec),escapeHtml((x.keys||[]).join(' · ')),escapeHtml(x.client||'')]);
  output+=`<section class="detail-block"><h3>已记录慢事件</h3><p>仅阈值以上事件；Key已脱敏，不保存Value、ARGV或Lua正文。${data.slow_events_truncated?'显示达到100条上限。':''}</p>${analyticsTable(['时间','分片','命令','执行微秒','Key形状','来源IP'],slow,'窗口内未记录到慢事件；不代表命令耗时为零。')}</section>`;
  const samples=(data.key_samples||[]).map(x=>[escapeHtml(x.command),escapeHtml((x.keys||[]).join(' · ')),escapeHtml(x.category||x.script||'未分类'),escapeHtml(x.pod||'未上报'),redisNumber(x.sample_rate),redisNumber(x.client_us)]);
  output+=`<section class="detail-block"><h3>键级采样（实例）</h3><p>${samples.length?'实例级客户端采样，未自动归属到选中分片；不外推服务端CPU，不与Lua内部命令重复累加。':'未接入或本窗无样本，无法归因到具体Key/新增Pod；需业务Hook发版。'}${data.key_samples_truncated?' 显示达到100条上限。':''}</p>${analyticsTable(['命令','Key形状','脚本/任务','Pod','采样率','客户端微秒'],samples,'没有键级样本。')}</section>`;
  if(data.resets?.length)output+=`<section class="detail-block"><h3>采集断点</h3>${analyticsTable(['分片','时间','原因'],data.resets.map(x=>[escapeHtml(x.node),escapeHtml(formatTime(x.time*1e6)),escapeHtml(x.reason)]))}</section>`;
  $('#analytics-panel-sql').innerHTML=output;
}
async function runRedis(){
  const instance=$('#redis-instance').value;
  if(!instance||redisStatus?.status!=='ok')throw new Error('Redis实例尚未登记');
  const start=new Date($('#analytics-start').value).getTime()*1000,end=new Date($('#analytics-end').value).getTime()*1000;
  if(!Number.isFinite(start)||!Number.isFinite(end)||end<=start||end-start>86400e6)throw new Error('Redis每次请选择一天以内的有效窗口');
  const request=++redisRequest;$('#analytics-panel-sql').innerHTML='';$('#analytics-empty').hidden=true;$('#analytics-meta').textContent='正在读取Redis记录…';
  try{
    const query=new URLSearchParams({instance,node:$('#redis-node').value,startEpochUs:String(start),endEpochUs:String(end)});
    const data=await api('/api/redis/investigate?'+query);if(request!==redisRequest||!redisVisible())return;
    renderRedis(data);switchAnalyticsTab('sql');
  }catch(error){if(request===redisRequest&&redisVisible()){$('#analytics-meta').textContent='Redis读取失败：'+error.message;$('#analytics-coverage').textContent='读取失败，不把缺失当正常。';$('#analytics-panel-sql').innerHTML='';$('#analytics-empty').hidden=false;}throw error;}
}
