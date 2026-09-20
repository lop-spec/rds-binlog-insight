const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync('web/mongo.js','utf8');
const escape=s=>String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
function context(){return vm.createContext({console,escapeHtml:escape,URLSearchParams,Date,Number,Math,formatTime:v=>new Date(v/1000).toISOString()});}
test('Mongo source is inside existing workspace, not a second application',()=>{
 const html=fs.readFileSync('web/index.html','utf8'),app=fs.readFileSync('web/app.js','utf8');
 assert.match(html,/value="mongodb"/);assert.match(html,/id="mongo-kind"/);assert.match(html,/id="mongo-baseline"/);
 assert.match(app,/return runMongoAnalytics\(\)/);assert.match(html,/assets\/mongo.js/);
});
test('missing values are not rendered as zero',()=>{
 const c=context();vm.runInContext(source,c);assert.equal(vm.runInContext('mongoNumber(null)',c),'—');
});
test('allocator free supports native field versions and excludes unmapped pages',()=>{
 const c=context();vm.runInContext(source,c);
 assert.equal(vm.runInContext('mongoAllocatorFree(null)',c),null);
 assert.equal(vm.runInContext('mongoAllocatorFree({tcmalloc:{central_cache_free:3,pageheap_free_bytes:5,pageheap_unmapped_bytes:900}})',c),8);
 assert.equal(vm.runInContext('mongoAllocatorFree({tcmalloc:{central_cache_free_bytes:3,pageheap_free_bytes:5}})',c),8);
});
test('Mongo charts obey existing CSP and native memory includes queue/cursor evidence',()=>{
 assert.doesNotMatch(source,/style="/);
 assert.match(source,/<div class="spark"><svg/);
 assert.match(fs.readFileSync('web/app.css','utf8'),/\.spark svg\s*\{[^}]*width:\s*100%/);
 assert.match(source,/tracked dirty bytes in the cache/);
 assert.match(source,/x\.cursor\?\.open\?\.total/);
 assert.match(source,/currentQueue/);
});
test('native command rate changes retain observed-coverage disclosure',()=>{
 assert.match(source,/qps_delta/);assert.match(source,/baseline_coverage_seconds/);assert.match(source,/不等于全窗口次数增长/);
});
test('performance curve does not bridge missing minutes',()=>{
 const c=context();vm.runInContext(source,c);
 const html=vm.runInContext(`mongoSparkline([{timestamp:0,value:1,metric:'CPUUtilization'},{timestamp:60000,value:2,metric:'CPUUtilization'},{timestamp:240000,value:3,metric:'CPUUtilization'}])`,c);
 assert.equal((html.match(/<polyline/g)||[]).length,2);
});
test('missing baseline does not hide measured costs or imply no growth',()=>{
 const c=context();vm.runInContext(source,c);
 const cost=vm.runInContext('mongoCost({observed:527430000,known:931,total:931,delta:null},1e6,"s")',c);
 assert.match(cost,/527\.43 s/);assert.doesNotMatch(cost,/不可比较|未完整/);
 assert.match(vm.runInContext('mongoCost({observed:2000,known:50,total:931,delta:null})',c),/50\/931/);
 assert.equal(vm.runInContext('mongoCost({observed:null,known:0,total:931})',c),'—');
 assert.match(vm.runInContext('mongoAssessment({assessment:"incomplete_baseline"})',c),/不判断增长/);
 assert.doesNotMatch(source,/证据不足 \/ 无成本增长/);
 assert.match(source,/data\.order_reason/);assert.match(source,/missing_ranges/);
});
test('full incomplete-window page contains measured totals, scope and effective sorting',()=>{
 const c=context(),nodes={};c.$=selector=>nodes[selector]??=(selector==='#mongo-role'?{value:'Primary'}:{});
 c.analyticsTable=(headers,rows)=>'<table>'+headers.join('|')+rows.map(r=>r.join('|')).join('\n')+'</table>';
 c.detailBlock=(a,b)=>a+b;c.switchAnalyticsTab=()=>{};
 vm.runInContext(source,c);
 const cost={observed:527430000,known:931,total:931,delta:null};
 c.data={status:'incomplete_source',order:'duration_total',requested_order:'duration_growth',order_reason:'incomplete_source',coverage:{complete:false,collected_windows:177,expected_windows:288,missing_count:111},baseline_coverage:{complete:false,collected_windows:17,expected_windows:288,missing_count:271},baseline_start:1800000000000000,baseline_end:1800000300000000,collection_status:{enabled:true},totals:[{role:'Primary',kind:'command',count:931,baseline_count:null}],statements:[{namespace:'demo.events',command:'insert',role:'Primary',group_id:'x',count:931,baseline_count:null,count_delta:null,avg_us:567000,max_us:2008000,costs:{duration_us:cost,docs:{...cost,observed:12},cpu_ns:{...cost,observed:20,known:50},bytes_read:{...cost,observed:null},write_wait_us:{...cost,observed:1000}},evidence:{metric_status:'incomplete_source'},assessment:'incomplete_source'}]};
 vm.runInContext('renderMongoAnalytics(data)',c);
 const html=nodes['#analytics-panel-sql'].innerHTML;
 assert.match(html,/527\.43 s/);assert.match(html,/177\/288/);assert.match(html,/17\/288/);
 assert.match(html,/已明确改按/);assert.match(html,/不是异常增量榜/);assert.match(html,/50\/931/);
 assert.doesNotMatch(html,/suboperation|无成本增长|基线或字段未完整/);
});
function requestContext(values={}) {
 const c=context(),nodes={},calls=[],notices=[];
 const defaults={'#analytics-source':'mongodb','#analytics-range':'custom','#analytics-start':'2026-09-16T09:40:29Z','#analytics-end':'2026-09-17T09:59:29Z','#mongo-baseline':'yesterday','#mongo-baseline-start':'','#mongo-instance':'dds-example','#mongo-role':'Primary','#mongo-kind':'command','#mongo-metric':'CPUUtilization','#mongo-order':'duration_growth','#analytics-limit':'50'};
 c.$=s=>nodes[s]??={value:values[s]??defaults[s]??'',textContent:'',innerHTML:'',hidden:false};
 c.toast=(...args)=>notices.push(args);c.console={...console,warn:()=>{}};
 c.api=async path=>{calls.push(path);return {total_groups:0};};
 vm.runInContext(source,c);vm.runInContext('renderMongoAnalytics=data=>{$("#analytics-meta").textContent="分析完成";}',c);
 return {c,nodes,calls,notices,run:()=>vm.runInContext('runMongoAnalytics()',c)};
}
test('24h19m yesterday selection becomes an explicit non-overlapping equal baseline without changing scope',async()=>{
 const {c,nodes,calls,notices,run}=requestContext();await run();
 const q=new URL(calls[0],'http://fixture').searchParams,start=Date.parse('2026-09-16T09:40:29Z')*1000,end=Date.parse('2026-09-17T09:59:29Z')*1000;
 assert.equal(Number(q.get('startEpochUs')),start);assert.equal(Number(q.get('endEpochUs')),end);
 assert.equal(Number(q.get('baselineStart')),start-(end-start));assert.equal(nodes['#mongo-baseline'].value,'previous');
 assert.equal(notices.length,1);assert.match(notices[0][0],/已改为前一个等长窗口/);
 assert.match(nodes['#mongo-baseline-hint'].textContent,/等长、不重叠/);assert.equal(nodes['#mongo-baseline-start'].disabled,true);
 assert.equal(nodes['#analytics-meta'].textContent,'分析完成');
});
test('exactly 24 hours preserves yesterday; 24h plus one second and seven days use previous',()=>{
 const c=context();vm.runInContext(source,c);c.start=Date.parse('2026-09-16T09:40:29Z')*1000;
 for(const hours of [1,24,24+1/3600,168]) {
  c.span=Math.round(hours*3600*1e6);const p=vm.runInContext('mongoBaselineWindow(start,start+span,"yesterday",NaN)',c);
  assert.equal(p.mode,hours<=24?'yesterday':'previous');assert.ok(p.end<=c.start);
 }
});
test('invalid, reversed and over-seven-day windows fail locally without a loading or stale result',async()=>{
 for(const values of [{'#analytics-start':''},{'#analytics-end':'2026-09-16T09:40:29Z'},{'#analytics-end':'2026-09-24T09:40:29Z'}]) {
  const {c,nodes,calls,run}=requestContext(values);c.$('#analytics-panel-sql').innerHTML='stale';
  await assert.rejects(run());assert.equal(calls.length,0);assert.match(nodes['#analytics-meta'].textContent,/分析未完成/);
  assert.equal(nodes['#analytics-panel-sql'].innerHTML,'');assert.equal(vm.runInContext('mongoResult',c),null);
 }
});
test('explicit overlapping or invalid custom baseline is rejected, never silently changed',async()=>{
 for(const baseline of ['2026-09-15T09:40:29Z','']) {
  const {nodes,calls,run}=requestContext({'#mongo-baseline':'custom','#mongo-baseline-start':baseline});
  await assert.rejects(run(),/基线/);assert.equal(calls.length,0);assert.equal(nodes['#mongo-baseline'].value,'custom');
  assert.equal(nodes['#mongo-baseline-start'].value,baseline);assert.equal(nodes['#mongo-baseline-start'].disabled,false);
 }
});
test('valid custom baseline remains exact',async()=>{
 const {calls,run}=requestContext({'#mongo-baseline':'custom','#mongo-baseline-start':'2026-09-14T09:40:29Z'});await run();
 assert.equal(Number(new URL(calls[0],'http://fixture').searchParams.get('baselineStart')),Date.parse('2026-09-14T09:40:29Z')*1000);
});
test('backend and network errors terminate loading, clear stale data and allow retry',async()=>{
 for(const message of ['baseline_must_not_overlap_current_window','mongo_window_exceeds_seven_days','网络连接失败']) {
  const {c,nodes,run}=requestContext();c.api=async()=>{throw new Error(message);};
  await assert.rejects(run());assert.match(nodes['#analytics-meta'].textContent,/分析未完成/);
  assert.doesNotMatch(nodes['#analytics-meta'].textContent,/正在读取|baseline_must|mongo_window/);
  assert.equal(nodes['#analytics-panel-sql'].innerHTML,'');assert.equal(nodes['#analytics-empty'].hidden,false);
  c.api=async()=>({});await run();assert.equal(nodes['#analytics-meta'].textContent,'分析完成');
 }
});
test('an older rejected request cannot overwrite a newer result',async()=>{
 const {c,nodes,run}=requestContext();let reject;
 c.api=()=>new Promise((_,r)=>{reject=r;});const old=run();
 c.api=async()=>({});await run();reject(new Error('old failure'));await old;
 assert.equal(nodes['#analytics-meta'].textContent,'分析完成');
});
test('source switching suppresses late Mongo errors',async()=>{
 const {c,nodes,run}=requestContext();let reject;c.api=()=>new Promise((_,r)=>{reject=r;});const pending=run();
 c.$('#analytics-source').value='slowlog';c.$('#analytics-meta').textContent='MySQL';reject(new Error('late'));await pending;
 assert.equal(nodes['#analytics-meta'].textContent,'MySQL');
});
test('database-scoped statements, outliers and details are labeled without a fabricated collection',()=>{
 assert.equal((source.match(/x\.scope==='database'\?' · 库级命令':''/g)||[]).length,3);
});
test('workspace offers exactly the five requested metrics and no independent sort selector',()=>{
 const c=context();vm.runInContext(source,c);
 assert.deepEqual(Array.from(vm.runInContext('Object.keys(MONGO_METRICS)',c)),['CPUUtilization','IOPSUtilization','ScannedDocs','LockWaits','AvgRt']);
 assert.doesNotMatch(fs.readFileSync('web/index.html','utf8'),/id="mongo-order"/);
 assert.doesNotMatch(source,/\$\('#mongo-order'\)/);
});
test('each resource defaults to cost evidence and explicit correlation remains available',async()=>{
 for(const metric of ['CPUUtilization','IOPSUtilization','ScannedDocs','LockWaits','AvgRt']) {
  for(const mode of ['attribution','correlation']) {
   const {calls,run}=requestContext({'#mongo-metric':metric,'#mongo-order':'cpu_growth','#mongo-analysis-mode':mode});await run();
   const q=new URL(calls[0],'http://fixture').searchParams;assert.equal(q.get('metric'),metric);assert.equal(q.get('order'),mode);
  }
 }
 const {calls,run}=requestContext();await run();assert.equal(new URL(calls[0],'http://fixture').searchParams.get('order'),'attribution');
});
test('resource evidence exposes waiting counterexample and missing deltas without a causal score',()=>{
 const c=context();vm.runInContext(source,c);
 const html=vm.runInContext('mongoEvidence({attribution:{status:"no_resource_cost_growth",cost_field:"cpu_ns",delta:null,reasons:["elapsed_growth_without_resource_cost_growth","write_concern_wait_increased"]}})',c);
 assert.match(html,/未增长/);assert.match(html,/等待/);assert.match(html,/不可比较/);assert.doesNotMatch(html,/增量 0|根因概率.*%/);
});
test('switching metrics invalidates displayed and in-flight results',async()=>{
 const {c,nodes,run}=requestContext();let resolve;c.api=()=>new Promise(r=>{resolve=r;});const old=run();
 c.$('#analytics-panel-sql').innerHTML='old CPU result';c.$('#mongo-metric').value='AvgRt';vm.runInContext('mongoMetricChanged()',c);
 resolve({});await old;
 assert.equal(nodes['#analytics-panel-sql'].innerHTML,'');assert.match(nodes['#analytics-meta'].textContent,/平均响应/);
 assert.equal(vm.runInContext('mongoResult',c),null);
});
test('automatic ranking page distinguishes unavailable correlation without changing order',()=>{
 const c=context(),nodes={};c.$=s=>nodes[s]??={value:s==='#mongo-role'?'Primary':''};
 c.analyticsTable=(headers,rows)=>headers.join('|')+rows.map(r=>r.join('|')).join('\n');
 c.detailBlock=(a,b)=>a+b;c.switchAnalyticsTab=()=>{};vm.runInContext(source,c);
 c.data={order:'correlation',metric:'LockWaits',total_groups:0,coverage:{complete:true},baseline_coverage:{complete:false},ranking:{available:true}};
 vm.runInContext('renderMongoAnalytics(data)',c);
 assert.match(nodes['#analytics-meta'].textContent,/锁等待.*相关度从高到低/);
 assert.match(nodes['#analytics-panel-sql'].innerHTML,/不受对照窗口缺口影响/);
 assert.doesNotMatch(nodes['#analytics-panel-sql'].innerHTML,/已明确改按|增量榜/);
 c.data.ranking={available:false,reason:'metric_gaps'};vm.runInContext('renderMongoAnalytics(data)',c);
 assert.match(nodes['#analytics-panel-sql'].innerHTML,/相关度暂不可计算/);
 assert.match(nodes['#analytics-panel-sql'].innerHTML,/不改按耗时或次数排序/);
 assert.match(nodes['#analytics-meta'].textContent,/暂无可计算结果/);
});
test('correlation renders signed real coefficients and missing is not zero',()=>{
 const c=context();vm.runInContext(source,c);
 assert.match(vm.runInContext('mongoCorrelation({evidence:{pearson:-0.8}})',c),/r -0.8.*负相关/);
 const missing=vm.runInContext('mongoCorrelation({evidence:{pearson:null,metric_status:"metric_gaps"}})',c);
 assert.match(missing,/暂不可计算/);assert.doesNotMatch(missing,/r 0/);
});
test('no causal claim or fabricated full collection counts',()=>{
 assert.match(source,/相关与|因果/);assert.match(source,/尚无应用命令聚合接入/);
 assert.match(source,/不是慢日志计数/);assert.match(source,/未采集原生命令计数/);
 assert.match(source,/escapeHtml\(x.namespace\)/);assert.match(source,/字段覆盖/);
});
