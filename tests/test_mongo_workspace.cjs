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
 assert.equal(vm.runInContext('mongoCost({observed:null,known:0,total:931})',c),'未上报');
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
test('no causal claim or fabricated full collection counts',()=>{
 assert.match(source,/相关与|因果/);assert.match(source,/尚无应用命令聚合接入/);
 assert.match(source,/不是慢日志计数/);assert.match(source,/未采集原生命令计数/);
 assert.match(source,/escapeHtml\(x.namespace\)/);assert.match(source,/字段覆盖/);
});
