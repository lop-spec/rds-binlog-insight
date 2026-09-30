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


// ---- 1.29.21: RDS = 上涨排查 (metric + window -> SQL ranked by correlation); the four detail views load on demand ----
const rise = fs.readFileSync(path.join(root, 'web/rise.js'), 'utf8');
function rdsContext(api) {
  const nodes = {};
  const node = (selector) => (nodes[selector] ??= {innerHTML: '', textContent: '', hidden: false, value: '', className: '',
    classList: {toggle() {}}, getAttribute: () => 'false', setAttribute() {}, options: [],
    selectedOptions: [{textContent: selector === '#analytics-source' ? 'RDS' : '全部实例'}]});
  const c = vm.createContext({console, URLSearchParams, Date, Promise, Number, Object, Math, JSON, Infinity, String,
    document: {addEventListener() {}, querySelector: node, querySelectorAll: () => []}});
  vm.runInContext(workspace + '\n' + app + '\n' + rise, c);
  Object.assign(nodes, {'#analytics-source': {value: 'rds', selectedOptions: [{textContent: 'RDS'}]},
    '#analytics-start': {value: '2026-09-28T10:00'}, '#analytics-end': {value: '2026-09-28T12:00'},
    '#analytics-instance': {value: 'rm-main', selectedOptions: [{textContent: 'mysql-main'}], options: [{value: 'rm-main'}]},
    '#rds-metric': {value: 'cpu', options: [1]}, '#rds-controls': {hidden: true},
    '#analytics-node': {value: ''}, '#analytics-database': {value: ''}, '#analytics-table': {value: ''},
    '#analytics-operation': {value: ''}, '#analytics-limit': {value: '50'}, '#analytics-filter-summary': {textContent: ''},
    '#analytics-lock-warning': {hidden: true}, '#analytics-tabs': {hidden: false}, '#analytics-empty': {hidden: false},
    '#analytics-meta': {textContent: ''}, '#analytics-coverage': {className: '', innerHTML: ''}});
  const calls = [], toasts = [];
  Object.assign(c, {api: async (url) => { calls.push(url); return api(url); }, toast: (m, k) => toasts.push([k, m]),
    renderAnalyticsSql: (sql) => `SQL[${sql.mode || 'binlog'}:${sql.tag}]`, renderAnalyticsTransactions: () => 'TXN',
    renderAnalyticsLocks: () => 'LOCKS', renderAnalyticsCoverage: (cov, w, kind) => { nodes.notice = kind + ':' + cov.total_parts; },
    renderTxnDrill() {}});
  return {c, nodes, calls, toasts};
}
const slowResult = {evidence: {source: 'slowlog'}, sql: {mode: 'slowlog', tag: 's', orders: {}}, coverage: {total_parts: 4272}};
const binlogResult = {evidence: {source: 'binlog'}, sql: {tag: 'b', orders: {}}, transactions: {}, locks: {},
  coverage: {total_parts: 1084, unit: 'files', pending_parts: 2}};
const stmt = (sqlId, r, now, before, extra = {}) => ({fingerprint: `["d","sql_id","${sqlId}"]`, sql_id: sqlId, normalized_sql: `SELECT ${sqlId} FROM t`,
  resource_r: r, correlation_status: r === null ? 'constant_x' : 'ok', runtime_us_total: now, baseline_runtime_us_total: before,
  runtime_delta_us: now - before, executions: 4, active_minutes: 3, runtime_us: [0, 1, 2], sample_event_id: `ev-${sqlId}`,
  attribution: {baseline_count: 1, current_count: 4, rows_examined: {delta: 1200}, lock_time_ms: {delta: 0}}, ...extra});
const impactResult = {status: 'ok', metric_id: 'cpu', metric: 'Cluster_CpuUsage', order: 'correlation', instance_id: 'rm-main',
  start_us: 1790000000000000, end_us: 1790007200000000, executions: 10, baseline_executions: 8,
  sample_end_us: [1790000060000000, 1790000120000000, 1790000180000000],
  coverage: {total_parts: 12, covered_parts: 12, complete: true}, baseline_coverage: {total_parts: 12, covered_parts: 12, complete: true},
  nodes: [
    {node_id: 'rn-quiet', status: 'ok', peak: 30, metric_scale: 1, metric_values: [10, 11, 12], resource_comparison: {baseline_mean: 10, current_mean: 11, delta: 1},
      total_fingerprints: 1, statements: [stmt('quiet', 0.3, 5_000_000, 5_000_000)]},
    {node_id: 'rn-hot', status: 'ok', peak: 99, metric_scale: 1000, metric_values: [30000, 60000, 90000],
      resource_comparison: {baseline_mean: 30, current_mean: 60, delta: 30}, total_fingerprints: 3,
      statements: [stmt('ramp', 0.97, 180_000_000, 60_000_000), stmt('steady', 0.4, 90_000_000, 90_000_000),
                   stmt('flat', null, 60_000_000, 0)]},
  ]};

test('RDS run asks for one correlation-ordered impact analysis and nothing else', async () => {
  const {c, nodes, calls, toasts} = rdsContext(async () => impactResult);
  await vm.runInContext('runAnalytics()', c);
  assert.equal(calls.length, 1);
  const url = new URL(calls[0], 'http://fixture');
  assert.equal(url.pathname, '/api/slowlog-impact');
  assert.equal(url.searchParams.get('source'), 'slowlog');
  assert.equal(url.searchParams.get('metric'), 'cpu');
  assert.equal(url.searchParams.get('order'), 'correlation');
  assert.equal(url.searchParams.get('instance'), 'rm-main');
  assert.ok(toasts.some(([kind]) => kind === 'success'));
  assert.match(nodes['#analytics-meta'].textContent, /^RDS · mysql-main · CPU 使用率 %/);
  // detail views were not requested
  assert.equal(nodes['#analytics-panel-sql']?.innerHTML ?? '', '');
});

test('the hottest node comes first and its SQL are listed in the order the API returned (correlation), with cost increments', async () => {
  const {c, nodes} = rdsContext(async () => impactResult);
  await vm.runInContext('runAnalytics()', c);
  const htmlOut = nodes['#analytics-panel-rise'].innerHTML;
  assert.ok(htmlOut.indexOf('rn-hot') < htmlOut.indexOf('rn-quiet'), 'node with the larger metric rise first');
  assert.ok(htmlOut.indexOf('SELECT ramp') < htmlOut.indexOf('SELECT steady') && htmlOut.indexOf('SELECT steady') < htmlOut.indexOf('SELECT flat'));
  assert.match(htmlOut, /\+0\.97/);                       // signed correlation
  assert.match(htmlOut, /\+2 分/);                         // ramp: 60 s -> 180 s = +120 s
  assert.match(htmlOut, /昨日同窗无/);                       // flat: no baseline executions
  assert.match(htmlOut, /title="constant_x|每分钟耗时恒定/);  // uncomputable r explained, not shown as 0
  assert.match(htmlOut, /窗口均值 <strong>60<\/strong>/);
  assert.match(htmlOut, /class="is-flat"/);                // steady/quiet rows: no growth -> dimmed
});

test('unavailable impact analysis says why instead of an empty table', async () => {
  const {c, nodes, toasts} = rdsContext(async () => ({status: 'overlapping_baseline', nodes: []}));
  await vm.runInContext('runAnalytics()', c);
  assert.match(nodes['#analytics-panel-rise'].innerHTML, /24 小时以内/);
  assert.ok(toasts.some(([kind, m]) => kind === 'error' && /无法排查/.test(m)));
  const none = rdsContext(async () => impactResult);
  none.nodes['#analytics-instance'].value = '';
  await assert.rejects(vm.runInContext('runAnalytics()', none.c), /请先选择一个 RDS 实例/);
  assert.equal(none.calls.length, 0);
});

test('a window the server moved to the nearest indexed period is applied to the form and the detail query, and announced', async () => {
  const moved = {...impactResult, adjusted: {reason: 'incomplete_index', requested_start_us: 1789990000000000,
    requested_end_us: 1789997200000000, shift_us: -10_000_000_000}};
  const {c, nodes, toasts} = rdsContext(async () => moved);
  await vm.runInContext('runAnalytics()', c);
  const local = (us) => vm.runInContext(`toLocalInput(new Date(${us / 1000}))`, c);
  assert.equal(nodes['#analytics-start'].value, local(moved.start_us));
  assert.equal(nodes['#analytics-end'].value, local(moved.end_us));
  assert.equal(nodes['#analytics-range'].value, 'custom');
  const query = new URLSearchParams(vm.runInContext('state.rise.query', c));
  assert.equal(query.get('startEpochUs'), String(moved.start_us));   // the on-demand detail views read the same window
  assert.equal(query.get('endEpochUs'), String(moved.end_us));
  assert.equal(query.get('metric'), 'cpu');
  const html = nodes['#analytics-panel-rise'].innerHTML;
  assert.match(html, /已自动改查最近的有索引时段（早 2 小时 47 分钟）/);
  assert.match(html, /慢日志索引还不完整/);
  assert.ok(toasts.some(([kind, m]) => kind === 'info' && /已改查/.test(m)));
  // the baseline-day reason is named as such
  moved.adjusted.reason = 'incomplete_baseline_index';
  vm.runInContext('renderRise(state.rise.data)', c);
  assert.match(nodes['#analytics-panel-rise'].innerHTML, /昨日对照日索引还不完整/);
});

test('an indexed window is neither moved nor announced; a missing index names the 6-hour search', async () => {
  const {c, nodes} = rdsContext(async () => ({...impactResult, adjusted: null}));
  await vm.runInContext('runAnalytics()', c);
  assert.doesNotMatch(nodes['#analytics-panel-rise'].innerHTML, /已自动改查/);
  assert.equal(nodes['#analytics-start'].value, '2026-09-28T10:00');
  const none = rdsContext(async () => ({status: 'incomplete_index', nodes: []}));
  await vm.runInContext('runAnalytics()', none.c);
  assert.match(none.nodes['#analytics-panel-rise'].innerHTML, /前后 6 小时内没有索引完整/);
});

test('detail views load once, on demand, from the same conditions as the investigation', async () => {
  const {c, nodes, calls} = rdsContext(async (url) => (url.includes('/api/slowlog-impact') ? impactResult
    : url.includes('source=slowlog') ? slowResult : binlogResult));
  await vm.runInContext('runAnalytics()', c);
  assert.equal(calls.length, 1);
  await vm.runInContext('openAnalyticsTab("writes")', c);
  assert.equal(calls.length, 3);
  const detail = calls.slice(1).map((u) => new URL(u, 'http://fixture'));
  assert.deepEqual(detail.map((u) => u.searchParams.get('source')).sort(), ['binlog', 'slowlog']);
  assert.ok(detail.every((u) => u.pathname === '/api/analytics' && !u.searchParams.has('metric') && u.searchParams.get('instance') === 'rm-main'));
  assert.equal(nodes['#analytics-panel-sql'].innerHTML, 'SQL[slowlog:s]');
  assert.equal(nodes['#analytics-panel-writes'].innerHTML, 'SQL[binlog:b]');
  assert.equal(nodes['#analytics-panel-transactions'].innerHTML, 'TXN');
  await vm.runInContext('openAnalyticsTab("locks")', c);
  await vm.runInContext('openAnalyticsTab("sql")', c);
  assert.equal(calls.length, 3, 'switching between detail tabs never requests again');
  assert.equal(vm.runInContext('state.analytics.sql.tag', c), 's');
  assert.equal(nodes['#analytics-lock-warning'].hidden, true);
  vm.runInContext('switchAnalyticsTab("locks")', c);
  assert.equal(vm.runInContext('state.analytics.sql.tag', c), 'b');
  assert.equal(nodes['#analytics-lock-warning'].hidden, false);
  // a new investigation invalidates the loaded details
  await vm.runInContext('runAnalytics()', c);
  await vm.runInContext('openAnalyticsTab("sql")', c);
  assert.equal(calls.length, 3 + 1 + 2);
});

test('one failed detail kind keeps the other usable and is reported', async () => {
  const {c, nodes, toasts} = rdsContext(async (url) => {
    if (url.includes('/api/slowlog-impact')) return impactResult;
    if (url.includes('source=binlog')) throw new Error('clickhouse down');
    return slowResult;
  });
  await vm.runInContext('runAnalytics()', c);
  await vm.runInContext('openAnalyticsTab("writes")', c);
  assert.equal(nodes['#analytics-panel-sql'].innerHTML, 'SQL[slowlog:s]');
  assert.match(nodes['#analytics-panel-writes'].innerHTML, /Binlog 写入分析失败.*clickhouse down/);
  assert.match(nodes['#analytics-panel-locks'].innerHTML, /clickhouse down/);
  assert.ok(toasts.some(([k, m]) => k === 'error' && /Binlog 写入明细加载失败/.test(m)));
});

test('sorting a Binlog result re-renders the writes panel and keeps orders per kind', () => {
  const {c, nodes} = rdsContext(async () => binlogResult);
  vm.runInContext('state.analytics={evidence:{source:"binlog"},sql:{order:"executions",orders:{events:[{}]},tag:"b"}}', c);
  vm.runInContext('changeSqlOrder({value:"events"})', c);
  assert.equal(nodes['#analytics-panel-writes'].innerHTML, 'SQL[binlog:b]');
  assert.equal(vm.runInContext('state.sqlOrders.binlog', c), 'events');
  assert.equal(vm.runInContext('state.sqlOrders.slowlog', c), undefined);
  assert.match(vm.runInContext('analyticsQueryString("", "binlog")', c), /order=events/);
  assert.match(vm.runInContext('analyticsQueryString("", "slowlog")', c), /order=executions/);
});

test('data source is exactly RDS and MongoDB; the primary tab is 上涨排查 and the four detail views sit behind 更多视图', () => {
  const options = [...html.matchAll(/<select id="analytics-source"[^>]*>(.*?)<\/select>/gs)][0][1];
  assert.deepEqual([...options.matchAll(/value="([^"]+)"/g)].map((m) => m[1]), ['rds', 'mongodb']);
  const tabs = [...html.matchAll(/<button data-analytics-tab="([^"]+)"([^>]*)>/g)].map((m) => [m[1], /is-detail/.test(m[2]) && /hidden/.test(m[2])]);
  assert.deepEqual(tabs, [['rise', false], ['sql', true], ['writes', true], ['transactions', true], ['locks', true]]);
  assert.ok(html.includes('id="analytics-more-views"'));
  for (const id of ['rise', 'sql', 'writes', 'transactions', 'locks']) assert.ok(html.includes(`id="analytics-panel-${id}"`), id);
  assert.match(html, /rise\.js/);
  const metrics = [...rise.matchAll(/\["(\w+)", "[^"]+"\]/g)].map((m) => m[1]).slice(0, 5);
  assert.deepEqual(metrics, ['cpu', 'iops', 'rows_read', 'row_lock', 'threads']);
});
