const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('web/starrocks.js', 'utf8');
const app = fs.readFileSync('web/app.js', 'utf8');
const html = fs.readFileSync('web/index.html', 'utf8');
const escapeHtml = s => String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
function context() {
  const nodes = {};
  const defaults = {'#analytics-source':'starrocks', '#analytics-range':'custom',
    '#analytics-start':'2026-11-10T06:00:00Z', '#analytics-end':'2026-11-10T07:00:00Z',
    '#starrocks-instance':'c-example01', '#starrocks-metric':'fe_qps', '#starrocks-mode':'metrics', '#starrocks-order':'cpuCostNs', '#starrocks-refresh':'0'};
  const c = vm.createContext({console, Date, Number, Math, URLSearchParams, escapeHtml, document:{hidden:true}, clearTimeout(){}, setTimeout(){},
    detailBlock:(label,value)=>escapeHtml(label)+escapeHtml(value),
    formatTime: value => new Date(value/1000).toISOString(),
    analyticsTable: (headers, rows, empty='') => `<table>${headers.join('|')}${rows.length ? rows.map(r=>r.join('|')).join('\n') : empty}</table>`,
    switchAnalyticsTab: () => {}, setAnalyticsRange: () => {},
    $: selector => nodes[selector] ??= {value:defaults[selector] || '', hidden:false, innerHTML:'', textContent:'', closest:()=>({hidden:false})},
  });
  vm.runInContext(source, c);
  return {c,nodes};
}

test('StarRocks lives in the existing workspace and is dispatched separately', () => {
  assert.match(html, /value="starrocks"/);
  assert.match(html, /assets\/starrocks\.js/);
  assert.match(app, /return runStarRocks\(\)/);
  assert.match(app, /\.value === "rds" \? Number\(state\.status/);
  assert.match(source, /无法读取全局在途查询/);
});

test('missing values never become zero', () => {
  const {c}=context();
  for (const value of ['null','undefined','false','""','NaN']) assert.equal(vm.runInContext(`starrocksNumber(${value}, "fe_qps")`,c),'—');
  assert.equal(vm.runInContext('starrocksNumber(0,"fe_qps")',c),'0');
  assert.equal(vm.runInContext('starrocksNumber(1073741824,"be_memory")',c),'1');
});

test('chart breaks over missing points and escapes its label', () => {
  const {c}=context();
  const result=vm.runInContext(`starrocksGraph([{timestamp:0,value:1,period:60},{timestamp:60000,value:2,period:60},{timestamp:240000,value:3,period:60}],"fe_qps","<script>","次")`,c);
  assert.equal((result.match(/<polyline/g)||[]).length,2);
  assert.match(result,/&lt;script&gt;/);assert.doesNotMatch(result,/<script>/);
});

test('no configured instance fails before any API query',async()=>{
  const {c}=context();vm.runInContext('starrocksStatus={status:"not_configured"}',c);
  c.api=async()=>{throw new Error('must not call');};
  await assert.rejects(vm.runInContext('runStarRocksMetrics()',c),/未登记/);
});

test('renders points per node without pretending source coverage',async()=>{
  const {c,nodes}=context();vm.runInContext('starrocksStatus={status:"ok"}',c);
  let called='';c.api=async url=>{called=url;return {label:'FE 查询 / 秒',kind:'FE',metric:'fe_qps',unit:'次/秒',period:60,
    nodes:['<node>'],points:[{node:'<node>',timestamp:1794294000000,value:3,period:60},{node:'<node>',timestamp:1794294060000,value:5,period:60}],warning:'缺失不当作零'};};
  await vm.runInContext('runStarRocksMetrics()',c);
  assert.match(called,/^\/api\/starrocks\/metrics\?/);
  assert.match(nodes['#analytics-panel-sql'].innerHTML,/&lt;node&gt;/);
  assert.doesNotMatch(nodes['#analytics-panel-sql'].innerHTML,/<node>/);
  assert.match(nodes['#analytics-coverage'].textContent,/缺失不当作零/);
  assert.equal(nodes['#analytics-empty'].hidden,true);
});

test('empty points do not become a zero or normal result',async()=>{
  const {c,nodes}=context();vm.runInContext('starrocksStatus={status:"ok"}',c);
  c.api=async()=>({label:'FE 查询 / 秒',metric:'fe_qps',unit:'次/秒',period:60,nodes:[],points:[],warning:'缺失不当作零'});
  await vm.runInContext('runStarRocksMetrics()',c);
  assert.match(nodes['#analytics-panel-sql'].innerHTML,/没有返回监控点，不能视为零或节点正常/);
});

test('a stale StarRocks response cannot overwrite another source',async()=>{
  const {c,nodes}=context();vm.runInContext('starrocksStatus={status:"ok"}',c);
  let resolve;c.api=()=>new Promise(r=>{resolve=r;});
  const pending=vm.runInContext('runStarRocksMetrics()',c);
  c.$('#analytics-source').value='mongodb';c.$('#analytics-meta').textContent='Mongo';
  resolve({label:'stale',nodes:[],points:[]});await pending;
  assert.equal(nodes['#analytics-meta'].textContent,'Mongo');
});

test('network error clears the stale graph and does not say normal',async()=>{
  const {c,nodes}=context();vm.runInContext('starrocksStatus={status:"ok"}',c);
  c.$('#analytics-panel-sql').innerHTML='old result';c.api=async()=>{throw new Error('network');};
  await assert.rejects(vm.runInContext('runStarRocksMetrics()',c));
  assert.equal(nodes['#analytics-panel-sql'].innerHTML,'');
  assert.match(nodes['#analytics-coverage'].textContent,/读取失败/);
});

test('permission denial is never displayed as zero active queries',()=>{
  const {c,nodes}=context();c.data={status:'permission_denied',rows:[],warning:'缺少 SYSTEM OPERATE'};
  vm.runInContext('srRenderData(data,"running")',c);
  assert.match(nodes['#analytics-meta'].textContent,/权限不足/);
  assert.doesNotMatch(nodes['#analytics-meta'].textContent,/0 条/);
  assert.match(nodes['#analytics-panel-sql'].innerHTML,/不能用只显示当前账号/);
});

test('bounded workload keeps partial scope and escapes SQL and sources',()=>{
  const {c,nodes}=context();c.data={observed_executions:3,total_groups:1,source_records:3,duplicate_records:0,duplicate_cost_conflicts:0,coverage:{truncated:true},groups:[{database:'<db>',action:'SELECT',sql:'SELECT <script>',accounts:[{name:'<user>'}],clients:[{name:'<ip>',count:3}],count:3,failed:0,slow:0,costs:{cpuCostNs:{value:100,known:1}},peak_memory:null}]};
  vm.runInContext('srRenderData(data,"workload")',c);
  assert.match(nodes['#analytics-coverage'].textContent,/部分结果/);
  assert.match(nodes['#analytics-panel-sql'].innerHTML,/&lt;script&gt;/);
  assert.doesNotMatch(nodes['#analytics-panel-sql'].innerHTML,/<script>/);
  assert.match(nodes['#analytics-panel-sql'].innerHTML,/字段 1\/3/);
});

test('audit and load modes use the database API, not the CMS API',async()=>{
  const {c}=context();vm.runInContext('starrocksStatus={status:"ok"}',c);
  let called='';c.$('#starrocks-mode').value='audit';
  c.api=async url=>{called=url;return {rows:[],coverage:{truncated:false}};};
  await vm.runInContext('runStarRocks()',c);
  assert.match(called,/^\/api\/starrocks\/query\?/);
  assert.match(called,/mode=audit/);
});
