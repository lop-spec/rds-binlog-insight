const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
function context() {
  const c = vm.createContext({console, URLSearchParams, Date, document:{addEventListener(){}}});
  for (const file of ['workspace.js','app.js']) vm.runInContext(fs.readFileSync(path.join(__dirname,'../web',file),'utf8'), c);
  return c;
}

test('Pod history uncertainty is explicit and metadata escaped', () => {
  const c = context();
  c.pod = {status:'historical_binding_unverified',pods:[{name:'<script>bad</script>',namespace:'ns',uid:'u',cluster:'c'}]};
  const html = vm.runInContext('podDetail(pod)',c);
  assert.match(html,/历史绑定未证实/);
  assert.match(html,/Pod UID/);
  assert.doesNotMatch(html,/<script>/);
  assert.match(vm.runInContext('podDetail()',c),/未接入生产 Pod 元数据/);
});

test('resource overlap never claims IOPS contribution or causal certainty', () => {
  const c=context();
  c.result={executions:3,instance_id:'i',nodes:[{node_id:'node',status:'ok',baseline:10,peak:90,
    statements:[{rank:1,fingerprint:'f',sql_id:'sql-id',normalized_sql:'SELECT <img onerror=alert(1)>',
      sample_event_id:'event',growth_share:0.75,resource_r:0.98765,runtime_us_total:1000000,executions:1}]}]};
  const html=vm.runInContext('renderResourceAnalysis(result)',c);
  assert.match(html,/不调用模型/); assert.match(html,/不是 IOPS 贡献率/);
  assert.match(html,/75\.0000%/); assert.match(html,/0\.9877/);
  assert.match(html,/执行样本 \/ Pod/); assert.doesNotMatch(html,/<img/);
});

test('unavailable resource metrics have a reason and no fake score', () => {
  const c=context();
  const html=vm.runInContext('renderResourceAnalysis({status:"metric_or_input_unavailable",nodes:[]})',c);
  assert.match(html,/云监控或输入不可用/);
  assert.doesNotMatch(html,/0\.0000%/);
});

test('performance sorting reuses the computed response without a second request', async () => {
  const c=context();let requests=0;
  c.fetchResult=()=>{requests++;return Promise.resolve({status:'ok',nodes:[]});};
  vm.runInContext('api=fetchResult',c);
  await vm.runInContext('loadPerformanceAnalysis("source=slowlog&instance=i&order=scan_rows&limit=50")',c);
  await vm.runInContext('loadPerformanceAnalysis("source=slowlog&instance=i&order=executions&limit=20")',c);
  assert.equal(requests,1);
});

test('late resource response cannot reopen or overwrite a closed drawer', async () => {
  const c=context(); let resolve;
  c.response=new Promise(r=>{resolve=r;});
  vm.runInContext(`const fakeBody={innerHTML:''}; const fakeTitle={textContent:''};
    document.querySelector=s=>s==='#detail-body'?fakeBody:fakeTitle;
    analyticsQueryString=()=>'';showDetailDrawer=()=>{};api=()=>response;`,c);
  const request=vm.runInContext('openResourceAnalysis()',c);
  vm.runInContext("state.detailRequest++;fakeBody.innerHTML='closed'",c);
  resolve({status:'metric_or_input_unavailable',nodes:[]});
  await request;
  assert.equal(vm.runInContext('fakeBody.innerHTML',c),'closed');
});
