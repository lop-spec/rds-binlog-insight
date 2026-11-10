const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync('web/instances.js','utf8'),app=fs.readFileSync('web/app.js','utf8');
function context(){
 const nodes={},requests=[];const defaults={'#filter-instance':'c-example01','#filter-start':'2026-11-10T09:00:00Z','#filter-end':'2026-11-10T09:15:00Z','#filter-keyword':'select',' #analytics-source':'rds','#analytics-instance':'c-example01'};
 const state={status:{instanceRegistry:[{id:'c-example01',label:'example-sr',engine:'starrocks'}]},queryPromise:null};
 const c=vm.createContext({console,URLSearchParams,Date,Map,Number,Object,state,document:{hidden:true},
  $:id=>nodes[id]??={value:defaults[id]||'',innerHTML:'',textContent:'',hidden:false,disabled:false,setCustomValidity(){},closest(){return {hidden:false};}},
  withBusy:async(b,fn)=>fn(),renderEvents:data=>{c.rendered=data;},detailBlock:(a,b)=>a+b,showDetailDrawer:()=>{c.detailOpened=true;},toast(){},
  api:async url=>{requests.push(url);return {rows:[{id:'event1',event_us:1794301200000000,queryId:'qid',user:'reader',stmt:'SELECT 1',state:'EOF'}],coverage:{truncated:true},warning:'late events possible'};}
 });vm.runInContext(source,c);return {c,nodes,requests};
}
test('registered SR is a source read-only engine, not a guessed MySQL ID prefix',()=>{
 const {c}=context();assert.equal(vm.runInContext('isStarRocksAudit()',c),true);
 c.state.status.instanceRegistry=[];assert.equal(vm.runInContext('isStarRocksAudit()',c),false);
});
test('main SR audit issues GET to real SR endpoint, not query-tasks',async()=>{
 const {c,nodes,requests}=context();await vm.runInContext('runStarRocksAudit()',c);
 assert.equal(requests.length,1);assert.match(requests[0],/^\/api\/starrocks\/query\?/);
 const q=new URL(requests[0],'http://fixture').searchParams;assert.equal(q.get('instance'),'c-example01');assert.equal(q.get('keyword'),'select');
 assert.equal(c.rendered.rows[0].raw_event_type,'STARROCKS_AUDIT');assert.equal(c.rendered.rows[0].database_account,'reader');
 assert.match(nodes['#result-meta'].textContent,/部分结果/);assert.equal(nodes['#page-next'].disabled,true);
 assert.equal(vm.runInContext('openStarRocksAuditDetail("event1")',c),true);assert.equal(c.detailOpened,true);
 assert.equal(vm.runInContext('openStarRocksAuditDetail("unknown")',c),false);
});
test('main SR invalid window is rejected before network without auto moving it',async()=>{
 const {c,nodes,requests}=context();c.$('#filter-end').value='2026-11-10T12:00:00Z';
 await assert.rejects(vm.runInContext('runStarRocksAudit()',c),/一小时/);assert.equal(requests.length,0);
 assert.equal(nodes['#filter-end'].value,'2026-11-10T12:00:00Z');
});
test('main SR source switch discards a late response',async()=>{
 const {c,nodes}=context();let resolve;c.api=()=>new Promise(r=>resolve=r);const pending=vm.runInContext('runStarRocksAudit()',c);
 c.$('#filter-instance').value='rm-example01';c.$('#result-meta').textContent='MySQL result';resolve({rows:[]});await pending;
 assert.equal(nodes['#result-meta'].textContent,'MySQL result');assert.equal(c.state.queryPromise,null);
});
test('main SR network failure clears records and is never a zero-success response',async()=>{
 const {c,nodes}=context();c.api=async()=>{throw new Error('source down');};await assert.rejects(vm.runInContext('runStarRocksAudit()',c));
 assert.equal(c.rendered.rows.length,0);assert.match(nodes['#result-meta'].textContent,/读取失败/);
});
test('main analytics instance auto-selects SR source instead of RDS',async()=>{
 const {c,nodes}=context();c.$('#analytics-source').value='rds';let called=0;c.syncAnalyticsMode=async()=>{called++;};
 await vm.runInContext('selectAnalyticsInstance()',c);assert.equal(nodes['#analytics-source'].value,'starrocks');assert.equal(nodes['#starrocks-instance'].value,'c-example01');assert.equal(called,1);
});
test('app prevents legacy task polling and Binlog watermarks for SR audit',()=>{
 assert.match(app,/isStarRocksAudit\(\)\) return runStarRocksAudit\(\)/);
 assert.match(app,/isStarRocksAudit\(\)\) return false/);
 const chunk=app.slice(app.indexOf('async function refreshQueryTasks()'),app.indexOf('async function runQuery('));
 assert.equal((chunk.match(/isStarRocksAudit/g)||[]).length,4);
 assert.match(app,/openStarRocksAuditDetail\(eventId\)/);
});
