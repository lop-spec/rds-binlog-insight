/* All derived diagnostics preserve the scope of their evidence source. */
function redisEvidence(data){
 const f=data.five_layers;if(!f)return '';
 const table=(headers,rows,empty='尚未记录该层证据，不能当作零。')=>analyticsTable(headers,rows,empty);
 const section=(title,body,open=false)=>`<details class="detail-block"${open?' open':''}><summary>${escapeHtml(title)}</summary>${body}</details>`;
 const scopes=new Map((f.nodes||[]).map(n=>[n.node,n]));
 const ranges=(data.coverage||[]).map(c=>{const n=scopes.get(c.node)||{},v=n.counter_deltas?.total_commands_processed;return [escapeHtml(c.node),redisNumber(n.average_process_cpu_percent),redisNumber(v?.seconds?v.value/v.seconds:null),`${redisNumber(n.process_cpu_known_seconds)}秒`,`${redisNumber(c.seconds)} / ${redisNumber(c.expected_seconds)}`,`${redisNumber(c.baseline_seconds)} / ${redisNumber(c.expected_seconds)}`];});
 let output=section('分片范围',table(['分片','已观察CPU%','处理命令/秒','CPU覆盖','本窗覆盖秒','昨日覆盖秒'],ranges)+`<p>先分清六片同涨还是单片偏高；未记录的历史不补零，部分CPU覆盖不冒充整窗平均。</p>`,true);
 const findings=(f.findings||[]).map(x=>[escapeHtml(x.grade),escapeHtml(x.message)]);
 output+=section('证据结论',table(['等级','事实与缺口'],findings),true);
 const source=f.sources?.data;
 const model=new Map((f.polling_model?.deployments||[]).map(x=>[x.deployment,x]));
 const workloads=(source?.deployments||[]).map(d=>{const m=model.get(d.deployment);return [escapeHtml(d.deployment),redisNumber(d.replicas),redisNumber(d.ready),d.hpa?`${redisNumber(d.hpa.min)}–${redisNumber(d.hpa.max)} / ${redisNumber(d.hpa.current)}`:'未返回',escapeHtml((d.images||[]).join('\n')),m?.worker_enable_config===true?'配置开启':m?.worker_enable_config===false?'配置关闭':'配置未明',m?.prediction?`${redisNumber(m.prediction.eval_per_second)} / ${redisNumber(m.prediction.zrangebyscore_per_second)}`:'不能推算'];});
 output+=section('来源与副本',`<p>${source?'快照观察时间 '+escapeHtml(formatTime(f.sources.time*1e6)):'窗口结束前没有来源快照。'}；仅保存ConfigMap数字/布尔白名单，不读取Secret，不保证运行进程已加载配置。</p>${table(['工作负载','副本','就绪','HPA限额/当前','镜像','worker','预测EVAL/读'],workloads)}<p>${escapeHtml(f.polling_model?.warning||'未采集来源层，不能由Redis单独推断来源。')}</p>`,true);
 const ev=(f.events?.rows||[]).slice(0,30).map(x=>[escapeHtml(formatTime(x.time*1e6)),escapeHtml(x.node||'工作负载'),escapeHtml(x.data.type),escapeHtml(x.data.exact_change_time===false?'快照间观察到变更，非精确发生时刻':'源事件')]);
 const cloud=(f.cloud_events?.rows||[]).flatMap(x=>(x.data.items||[]).map(y=>[escapeHtml(formatTime(x.time*1e6)),escapeHtml(y.TaskType||y.EventType||'云事件'),escapeHtml(y.StartTime||y.TaskStartTime||y.EventTime||''),escapeHtml(y.Status||y.EventStatus||'')]));
 output+=section('事件对齐',table(['观察时间','对象','事件','时间口径'],ev)+table(['查询观察','云侧类型','源端时间','状态'],cloud,'云侧未返回该窗口事件；不证明没有未记录变更。')+`<p>${f.events?.truncated||f.cloud_events?.truncated?'事件列表达到上限，部分结果。':''}</p>`);
 const costs=(f.unit_costs?.nodes||[]).map(n=>[escapeHtml(n.node),redisNumber(n.median_command_index),String(n.comparable_commands)]);
 const commandCosts=(f.unit_costs?.rows||[]).filter(x=>['eval','exec','get','set','zadd','select','expire'].includes(x.command)).map(x=>[escapeHtml(x.node),escapeHtml(x.command),redisNumber(x.avg_us),redisNumber(x.median_us),redisNumber(x.index)]);
 output+=section('分片单位成本',table(['分片','中位成本指数','可比命令数'],costs)+`<p>${escapeHtml(f.unit_costs?.warning||'')}</p>`+`<details><summary>命令对标明细</summary>${table(['分片','命令','单次微秒','分片中位微秒','指数'],commandCosts)}</details>`,true);
 const counters=(f.nodes||[]).map(n=>{const d=n.counter_deltas||{},g=n.snapshot?.telemetry?.gauges||{};const rate=k=>d[k]?d[k].value/d[k].seconds:null;return [escapeHtml(n.node),redisNumber(n.average_process_cpu_percent),redisNumber(n.process_cpu_known_seconds),redisNumber(n.cpu_usec_per_processed_command),redisNumber(rate('total_connections_received')),redisNumber(rate('expired_keys')),redisNumber(rate('evicted_keys')),redisNumber(g.blocked_clients),redisNumber(g.used_memory==null?null:g.used_memory/1024**3),redisNumber(g.used_memory_rss==null?null:g.used_memory_rss/1024**3)];});
 output+=section('节点分项',table(['分片','进程CPU%','CPU覆盖秒','CPU微秒/命令','新连接/秒','过期/秒','淘汰/秒','阻塞数','已用GiB','常驻GiB'],counters)+`<p>分母是服务端已处理命令，不是客户端往返次数；Lua/EXEC嵌套口径未核实，不把CPU减命令耗时的差值定性为网络开销。</p>`);
 const memory=(f.memory||[]).flatMap(n=>Object.entries(n.fields||{}).map(([name,v])=>[escapeHtml(n.node),escapeHtml(name),redisNumber(v.first),redisNumber(v.last),redisNumber(v.delta)]));
 output+=section('内存变化',`<p>首末已观察快照差值，单位按Redis字段（字节/比值/状态）保存。分项存在包含关系，不相加；没有字段就显示—。</p>${table(['分片','字段','首值','末值','变化'],memory)}`);
 const percentiles=(f.latency_snapshots||[]).filter(x=>['eval','exec','get','set','del','zrangebyscore'].includes(x.command)).map(x=>[escapeHtml(x.node),escapeHtml(x.command),redisNumber(x.p50),redisNumber(x.p99),redisNumber(x['p99.9'])]);
 output+=section('命令耗时分位',`<p>供应商累计直方图的最新快照，不是本窗分位数；分位数不可相减或平均。</p>${table(['分片','命令','P50微秒','P99微秒','P99.9微秒'],percentiles)}`);
 const night=(f.night_baselines?.rows||[]).map(x=>[escapeHtml(formatTime(x.data.start*1e6)),escapeHtml(x.node),redisNumber(x.data.averages?.CpuUsage),redisNumber(x.data.averages?.eval),redisNumber(x.data.averages?.zrangebyscore),`${x.data.points}点 / ${x.data.complete?'齐全':'不完整'}`]);
 output+=section('凌晨基线',`<p>北京时间03:00–05:00，各自云端速率点均值；不是服务端耗时。</p>${table(['开始时间','分片','CPU%','EVAL/秒','范围读/秒','覆盖'],night)}`);
 const top=(f.keys?.rows||[]).flatMap(x=>(x.data.items||[]).map(k=>[escapeHtml(formatTime(x.time*1e6)),escapeHtml(x.node),escapeHtml(k.kind),escapeHtml(k.key),escapeHtml(k.key_type||''),escapeHtml(String(k.qps??'—')),redisNumber(k.size)]));
 output+=section('热Key与大Key',`<p>仅超阈值或Top榜，不是全部Key；列表为空不等于没有热点。Key按形状和摘要脱敏；Size按云端类型定义，不等于真实内存。</p>${table(['观察时间','分片','榜单','Key形状','类型','QPS','Size'],top)}`);
 const queue=f.queues?.rows?.[0];const lengths=queue?.data?.items||[];const totals={};lengths.forEach(q=>{const name=q.key.replace(/\{fo:\d+\}/,'{fo:*}');totals[name]=(totals[name]||0)+Number(q.length||0);});
 output+=section('登记队列',`<p>${queue?'快照 '+escapeHtml(formatTime(queue.time*1e6)):'本窗尚无登记队列快照'}；只计登记键，聚合64桶元素数，不读取队列内容。ws/order_buyer等动态队列未登记时不宣称覆盖。</p>${table(['队列形状','元素总数'],Object.entries(totals).map(([k,v])=>[escapeHtml(k),redisNumber(v)]))}`);
 const profiles=(f.profiles?.rows||[]).slice(0,12).flatMap(x=>Object.entries(x.data.groups||{}).map(([k,g])=>[escapeHtml(x.node),escapeHtml(x.data.source==='offline_rdb'?(x.data.complete?'离线声明完整':'离线部分'):'游标样本'),escapeHtml(k),redisNumber(g.observed_keys),redisNumber(g.memory_known?g.memory_bytes_observed:null),redisNumber(g.ttl_none),redisNumber(g.ttl_expiring)]));
 output+=section('前缀画像抽样',`<p>低速游标页抽样，不是均匀采样；可重复、可漏项，不外推全库Key数、占用和增长。TTL=-1才计无过期，过期已删除的键不计。</p>${table(['分片','来源','前缀形状','观察键数','估算字节','无过期','带过期'],profiles)}`);
 const a=f.client_aggregates||{};const ar=(a.rows||[]).slice(0,100).map(r=>[escapeHtml(r.deployment),escapeHtml(r.command),escapeHtml(r.shape),escapeHtml(r.path),redisNumber(r.calls),redisNumber(r.delta_calls_observed),redisNumber(r.errors),redisNumber(r.client_us),`${redisNumber(r.request_bytes==null?null:Number(r.request_bytes)+Number(r.response_bytes))} / ${redisNumber(r.bytes_known_calls)}`,redisNumber(r.reported_pods)]);
 output+=section('客户端全量计数',`<p>状态：${escapeHtml(a.status||'未连接')}。实例范围、不按分片过滤；统计已收到批次的逻辑调用，500形状上限之外计入other；网络重试、未接入客户端和已丢弃批次不消失。客户端耗时含网络排队，Pipeline不分摊。</p>${table(['工作负载','命令','形状','代码路径','调用','已报变化','错误','客户端微秒','估算字节/覆盖次','上报Pod'],ar)}<p>已上报丢弃调用：${redisNumber(a.dropped_calls_reported)}；Pipeline：${redisNumber(a.pipeline_count)}次 / ${redisNumber(a.pipeline_client_us)}微秒。已报变化不是完整业务增量；数据不完整时不造完整榜。</p>`,true);
 const gaps=(f.coverage_reconciliation||[]).filter(r=>['del','eval','evalsha','zrangebyscore','get','set','other'].includes(r.command)).map(r=>[escapeHtml(r.command),redisNumber(r.server_observed_calls),redisNumber(r.client_reported_calls),redisNumber(r.numeric_difference),'口径未对齐，不计算比例']);
 output+=section('调用覆盖核对',table(['命令','服务器观察','客户端上报','数值差','未覆盖比例'],gaps)+`<p>服务器可能计入Lua内部及采集器命令，客户端Hook记录逻辑调用而非每次网络尝试；缺少可比范围时不得直接相减宣布覆盖率。</p>`);
 const capabilities=(f.nodes||[]).map(n=>{const t=n.snapshot?.telemetry||{},o=t.command_optional_support||{};return [escapeHtml(n.node),escapeHtml(t.present_sections?.length?((t.missing_sections||[]).join('、')||'无缺失分组'):'该历史未记录分组'),o.rejected_calls?'有字段':'未提供',o.failed_calls?'有字段':'未提供',escapeHtml((t.missing_fields||[]).filter(k=>['total_reads_processed','total_writes_processed','eventloop_cycles','eventloop_duration_sum','expire_cycle_cpu_milliseconds','expired_stale_perc','mem_clients_normal','mem_replication_backlog','mem_aof_buffer'].includes(k)).join('、'))];});
 output+=section('能力与缺口',table(['分片','空白分组','拒绝计数','失败计数','重点缺失字段'],capabilities)+`<p>字段未提供不记零；persistence/errorstats空白、事件循环或主动过期字段缺失时，相关机制不能由该记录单独验证。connected_clients在Proxy兼容实现中可能不代表真实连接总数。</p>`);
 return output;
}
function renderRedisHistory(data){
 $('#analytics-meta').textContent=`长期汇总趋势 · ${data.period===86400?'日视图':'小时视图'} · ${(data.points||[]).length}点`;
 $('#analytics-coverage').textContent=(data.truncated||data.rows_truncated?'达到读取上限，结果不完整。 ':'')+'由已记录分钟/小时生成；不扫描20秒明细，不补缺口，不把累计执行时间当CPU，也不把长期总量当上涨根因。';
 const points=data.points||[],nodes=[...new Set(points.map(p=>p.node))];
 let output=nodes.map(node=>`<section class="detail-block"><h3>${escapeHtml(node)} · 汇总CPU</h3>${starrocksGraph(points.filter(p=>p.node===node),'cpu','已观察进程CPU','%')}</section>`).join('');
 const rows=(data.rows||[]).map(r=>[escapeHtml(r.node),escapeHtml(r.command),redisNumber(r.calls),redisNumber(r.usec/1e6),redisNumber(r.calls?r.usec/r.calls:null),redisNumber(r.seconds)]);
 output+=`<section class="detail-block"><h3>已记录成本</h3><p>按差分段结束时间归入汇总桶；不按比例拆到真实秒。Lua/EXEC与内部命令可能嵌套，不相加算总CPU。</p>${analyticsTable(['分片','命令','调用次数','执行秒','单次微秒','覆盖秒'],rows,'该时间尚无汇总记录；开始记录前的历史不能恢复。')}</section>`;
 $('#analytics-panel-sql').innerHTML=output;
}
