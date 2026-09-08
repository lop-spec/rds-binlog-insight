const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const root = path.join(__dirname, '..');
const app = fs.readFileSync(path.join(root, 'web/app.js'), 'utf8');
const workspace = fs.readFileSync(path.join(root, 'web/workspace.js'), 'utf8');
const html = fs.readFileSync(path.join(root, 'web/index.html'), 'utf8');
function context() {
  const panel = {innerHTML: ''};
  const c = vm.createContext({console, URLSearchParams, Date, document: {addEventListener() {}, querySelector() {return panel;}}});
  vm.runInContext(workspace + '\n' + app, c);
  vm.runInContext(`state.analytics={coverage:{complete:true,total_parts:2},sql:{}}`, c);
  return c;
}
const item = {fingerprint:'f', sql_id:'das-id', normalized_sql:'SELECT <script>alert(1)</script>',
  executions:12, scan_rows:600, rows_sent:20, query_time_ms_total:500, sample_event_id:'e',
  max_scan_event_id:'max-scan', max_query_event_id:'max-time',
  correlation:{value:0, status:'ok', level_value:0.1, without_self_value:-0.4, without_self_status:'ok',
    counts:[1,2,3,0,4,2], event_share:0.1}};

test('slow table has seven columns, correlation replaces ID, source badge occurs once', () => {
  const c=context(); c.item=item;
  const rendered=vm.runInContext('renderAnalyticsSlowlogSql({statements:[item],order:"scan_rows"})',c);
  assert.match(rendered, /相关度 r/);
  assert.doesNotMatch(rendered, /<th>SQL ID<\/th>/);
  assert.equal((rendered.match(/<th>/g)||[]).length,7);
  assert.equal((rendered.match(/慢日志实测/g)||[]).length,1);
  assert.match(rendered,/0\.0000/);
  assert.match(rendered,/data-slow-event-id="max-scan"/);
  assert.doesNotMatch(rendered, /<script>alert/);
});

test('detail retains metrics, formula and every aligned bucket', () => {
  const c=context(); c.item=item;
  const rendered=vm.runInContext('sqlAggregateDetail(item, {correlation:{complete_buckets:6, bucket_us:300000000}, trend: item.correlation.counts.map((n,i)=>({ts:i*300000000,events:n+10}))})',c);
  assert.match(rendered,/SQL ID/); assert.match(rendered,/das-id/);
  assert.match(rendered,/单次/); assert.match(rendered,/最大耗时/);
  assert.match(rendered,/corr\(Δx, Δy\)/); assert.match(rendered,/剔除自身/);
  assert.equal((rendered.match(/<tbody><tr>|<\/tr><tr>/g)||[]).length,6);
  assert.doesNotMatch(rendered,/<script>alert/);
});

test('incomplete coverage and unavailable coefficients never display a fabricated score', () => {
  const c=context(); c.item={...item,correlation:{value:0.999, status:'ok'}};
  vm.runInContext('state.analytics.coverage.complete=false',c);
  assert.doesNotMatch(vm.runInContext('correlationCell(item)',c),/0\.999/);
  assert.match(vm.runInContext('correlationCell(item)',c),/日志索引未完整/);
  vm.runInContext('state.analytics.coverage.complete=true',c);
  c.item={...item,correlation:{value:null,status:'constant_sql'}};
  assert.match(vm.runInContext('correlationCell(item)',c),/SQL 增量无变化/);
  assert.doesNotMatch(vm.runInContext('correlationCell(item)',c),/NaN|0\.0000/);
});

test('sort uses response snapshot, keeps coefficient, does not make a request', async () => {
  const c=context(); c.item=item;
  vm.runInContext('state.analytics.sql={mode:"slowlog",order:"executions",orders:{scan_rows:[item]}}',c);
  await vm.runInContext('changeSqlOrder({value:"scan_rows"})',c);
  assert.equal(vm.runInContext('state.analytics.sql.order',c),'scan_rows');
  assert.equal(vm.runInContext('state.analytics.sql.statements[0].correlation.value',c),0);
});

test('zero curve reports zero peak and partial edges are identified', () => {
  const c=context();
  const rendered=vm.runInContext('sparkline([{ts:0,events:0},{ts:300000000,events:0,partial:true}])',c);
  assert.match(rendered,/峰值 0/); assert.match(rendered,/不完整桶，不参与相关度/);
});

test('form controls preserved and native disclosures are closed initially', () => {
  for (const id of ['analytics-filters','audit-filters']) {
    assert.match(html,new RegExp(`<details id="${id}" class="[^"]+">`));
  }
  const ids=[...html.matchAll(/\bid="([^"]+)"/g)].map(x=>x[1]);
  assert.equal(new Set(ids).size,ids.length,'duplicate DOM IDs');
  for(const id of ['analytics-source','analytics-instance','analytics-node','analytics-database','analytics-table',
    'analytics-operation','analytics-limit','query-reset','query-export','analytics-reset','filter-query-mode']) {
    assert.ok(ids.includes(id),id);
  }
  assert.doesNotMatch(html,/data-analytics-range=|data-range=/);
  assert.match(html,/workspace\.js/); assert.match(html,/workspace\.css/);
});
