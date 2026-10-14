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
  max_scan_event_id:'max-scan', max_query_event_id:'max-time', accounts:['analyst','root'], account_count:2,
  correlation:{value:0, status:'ok', level_value:0.1, without_self_value:-0.4, without_self_status:'ok',
    counts:[1,2,3,0,4,2], event_share:0.1}};

test('slow table has eight columns incl. the user, correlation replaces ID, source badge occurs once', () => {
  const c=context(); c.item=item;
  const rendered=vm.runInContext('renderAnalyticsSlowlogSql({statements:[item],order:"scan_rows"})',c);
  assert.match(rendered, /相关度 r/);
  assert.doesNotMatch(rendered, /<th>SQL ID<\/th>/);
  assert.equal((rendered.match(/<th>/g)||[]).length,8);
  assert.match(rendered, /<th>SQL<\/th><th>用户<\/th><th>相关度 r<\/th>/);
  assert.match(rendered, /title="共 2 个账号：analyst、root">analyst <span class="muted">\+1<\/span>/);
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


// ---- 1.30.0: RDS = 上涨排查 (slow log, ranked by growth share) + Binlog 写入, both selectable evidence sources ----
const rise = fs.readFileSync(path.join(root, 'web/rise.js'), 'utf8');
function rdsContext(api, {slowlog = true, binlog = false, start = '2026-09-28T10:00', end = '2026-09-28T12:00'} = {}) {
  const nodes = {};
  const node = (selector) => (nodes[selector] ??= {innerHTML: '', textContent: '', hidden: false, value: '', className: '',
    classList: {toggle() {}}, getAttribute: () => 'false', setAttribute() {}, options: [],
    selectedOptions: [{textContent: selector === '#analytics-source' ? 'RDS' : '全部实例'}]});
  const c = vm.createContext({console, URLSearchParams, Date, Promise, Number, Object, Math, JSON, Infinity, String,
    document: {addEventListener() {}, querySelector: node, querySelectorAll: () => []}});
  vm.runInContext(workspace + '\n' + app + '\n' + rise, c);
  Object.assign(nodes, {'#analytics-source': {value: 'rds', selectedOptions: [{textContent: 'RDS'}]},
    '#analytics-start': {value: start}, '#analytics-end': {value: end}, '#analytics-range': {value: '24h'},
    '#analytics-instance': {value: 'rm-main', selectedOptions: [{textContent: 'mysql-main'}], options: [{value: 'rm-main'}]},
    '#analytics-use-slowlog': {checked: slowlog}, '#analytics-use-binlog': {checked: binlog},
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
const local = (c, us) => vm.runInContext(`toLocalInput(new Date(${us / 1000}))`, c);
const slowResult = {evidence: {source: 'slowlog'}, sql: {mode: 'slowlog', tag: 's', orders: {}}, coverage: {total_parts: 4272}};
const binlogResult = {evidence: {source: 'binlog'}, sql: {tag: 'b', orders: {}}, transactions: {}, locks: {},
  coverage: {total_parts: 1084, unit: 'files', pending_parts: 2}};
const stmt = (sqlId, r, now, before, extra = {}) => ({fingerprint: `["d","sql_id","${sqlId}"]`, sql_id: sqlId, normalized_sql: `SELECT ${sqlId} FROM t`,
  resource_r: r, correlation_status: r === null ? 'constant_x' : 'ok', runtime_us_total: now, baseline_runtime_us_total: before,
  runtime_delta_us: now - before, executions: 4, active_minutes: 3, runtime_us: [0, 1, 2], sample_event_id: `ev-${sqlId}`, growth_share: null,
  attribution: {baseline_count: 1, current_count: 4, rows_examined: {delta: 0}, lock_time_ms: {delta: 0}}, ...extra});
const growing = (delta) => ({attribution: {baseline_count: 1, current_count: 4, rows_examined: {delta}, lock_time_ms: {delta: 0}}});
const impactResult = {status: 'ok', metric_id: 'cpu', metric: 'Cluster_CpuUsage', order: 'growth', instance_id: 'rm-main',
  start_us: 1790000000000000, end_us: 1790007200000000, executions: 10, baseline_executions: 8,
  sample_end_us: [1790000060000000, 1790000120000000, 1790000180000000],
  coverage: {total_parts: 12, covered_parts: 12, complete: true}, baseline_coverage: {total_parts: 12, covered_parts: 12, complete: true},
  nodes: [
    {node_id: 'rn-quiet', role: 'secondary', zone: 'cn-chengdu-b', status: 'ok', peak: 30, metric_scale: 1, metric_values: [10, 11, 12],
      resource_comparison: {baseline_mean: 10, current_mean: 11, delta: 1}, total_fingerprints: 1, statements: [stmt('quiet', 0.3, 5_000_000, 5_000_000)]},
    {node_id: 'rn-hot', role: 'primary', zone: 'cn-chengdu-a', status: 'ok', peak: 99, metric_scale: 1000, metric_values: [30000, 60000, 90000],
      resource_comparison: {baseline_mean: 30, current_mean: 60, delta: 30}, total_fingerprints: 3,
      statements: [stmt('ramp', 0.97, 180_000_000, 60_000_000, {...growing(1200), growth_share: 0.8, accounts: ['analyst'], account_count: 1, lock_wait_excluded_us: 3_000_000}),
                   stmt('steady', 0.4, 90_000_000, 90_000_000, {accounts: ['root', 'report', 'etl'], account_count: 4}),
                   stmt('flat', null, 60_000_000, 0, {growth_share: 0.2})]},
  ]};

test('slow log only: one growth-ordered impact analysis and nothing else', async () => {
  const {c, nodes, calls, toasts} = rdsContext(async () => impactResult);
  await vm.runInContext('runAnalytics()', c);
  assert.equal(calls.length, 1);
  const url = new URL(calls[0], 'http://fixture');
  assert.equal(url.pathname, '/api/slowlog-impact');
  assert.equal(url.searchParams.get('source'), 'slowlog');
  assert.equal(url.searchParams.get('metric'), 'cpu');
  assert.equal(url.searchParams.get('order'), 'growth');   // the old 对齐节点 IOPS ranking is the default now; r is only a column
  assert.equal(url.searchParams.get('instance'), 'rm-main');
  assert.ok(toasts.some(([kind]) => kind === 'success'));
  assert.match(nodes['#analytics-meta'].textContent, /^RDS · mysql-main · CPU 使用率 %/);
  assert.equal(nodes['#analytics-panel-sql']?.innerHTML ?? '', '');   // the slow-log detail view was not requested
  assert.equal(vm.runInContext('state.analyticsTab', c), 'rise');
});

test('the hottest node comes first; SQL keep the API order and show share, r and cost increments; nodes carry their role', async () => {
  const {c, nodes} = rdsContext(async () => impactResult);
  await vm.runInContext('runAnalytics()', c);
  const htmlOut = nodes['#analytics-panel-rise'].innerHTML;
  assert.ok(htmlOut.indexOf('rn-hot') < htmlOut.indexOf('rn-quiet'), 'node with the larger metric rise first');
  assert.ok(htmlOut.indexOf('SELECT ramp') < htmlOut.indexOf('SELECT steady') && htmlOut.indexOf('SELECT steady') < htmlOut.indexOf('SELECT flat'));
  assert.match(htmlOut, /<strong title="增长份额[^"]*">80%<\/strong>/);   // growth share is the ranking column
  assert.match(htmlOut, /<strong title="增长份额[^"]*">20%<\/strong>/);
  assert.match(htmlOut, /\+0\.97/);                       // signed correlation stays as a check column
  assert.match(htmlOut, /\+2 分/);                         // ramp: 60 s -> 180 s = +120 s
  assert.match(htmlOut, /昨日同窗无/);                       // flat: no baseline executions
  assert.match(htmlOut, /title="constant_x|每分钟耗时恒定/);  // uncomputable r explained, not shown as 0
  assert.match(htmlOut, /窗口均值 <strong>60<\/strong>/);
  assert.match(htmlOut, /class="is-flat"/);                // steady/quiet rows: neither runtime nor scans grew -> dimmed
  assert.equal((htmlOut.match(/<th>/g) || []).length, 16, 'eight columns in each of the two node tables');
  assert.match(htmlOut, /<th>SQL（按增长份额排序）<\/th><th>用户<\/th><th>增长份额<\/th>/);
  assert.match(htmlOut, /<span class="account-cell" title="analyst">analyst<\/span>/);                       // one account: just the name
  assert.match(htmlOut, /title="共 4 个账号：root、report、etl …">root <span class="muted">\+3<\/span>/);   // more accounts than listed: +N and a hint
  assert.match(htmlOut, /title="慢日志没有记录这条 SQL 的执行账号">—/);                                       // no account recorded: a dash with the reason
  assert.match(htmlOut, /已扣除慢日志记录的锁等待增长 3\.00 s/);                                              // the lock wait taken out of the growth is disclosed
  assert.equal((htmlOut.match(/data-live-node=/g) || []).length, 2, 'every node can show what is running now');
  assert.match(htmlOut, /共 3 个 SQL 家族，显示前 3 个，合计占本节点增长份额 100%/);
  // the node's database role and zone sit next to its id
  assert.match(htmlOut, /<strong>rn-hot<\/strong> <span class="chip role-primary"[^>]*>主节点<\/span><span class="muted">可用区 A<\/span>/);
  assert.match(htmlOut, /<strong>rn-quiet<\/strong> <span class="chip role-secondary"[^>]*>备节点<\/span><span class="muted">可用区 B<\/span>/);
  assert.match(htmlOut, /排序：按增长份额（该 SQL 在指标超额分钟里跑了多久/);
  assert.doesNotMatch(htmlOut, /先看扫描行比昨日增加的 SQL/);   // scan growth is no longer the first key
});

test('running statements are read on demand for one node and shown with their user, source and runtime', async () => {
  const live = {status: 'ok', instance_id: 'rm-main', node_id: 'rn-hot', observed_at_us: 1790000100000000, cached: false, total_sessions: 1066,
    running_sessions: 2, longest_running_seconds: 885, truncated: false, sessions: [
      {session_id: 1, user: 'analyst', client: '10.0.0.8', database: 'app', command: 'Query', state: 'executing', seconds: 885, sql: 'SELECT <b>x</b>\n FROM t', sql_chars: 24},
      {session_id: 2, user: 'root', client: '10.0.1.1', database: 'app', command: 'Execute', state: 'statistics', seconds: 0, sql: 'SELECT 1', sql_chars: 8}]};
  const {c, calls} = rdsContext(async (url) => (url.startsWith('/api/live-sessions') ? live : impactResult));
  await vm.runInContext('runAnalytics()', c);
  const result = {hidden: true, innerHTML: ''};
  const button = {disabled: false, textContent: '看正在跑的查询', closest: () => ({querySelector: () => result})};
  c.button = button;
  await vm.runInContext('loadLiveSessions("rn-hot", button)', c);
  const url = new URL(calls[calls.length - 1], 'http://fixture');
  assert.equal(url.pathname, '/api/live-sessions');
  assert.equal(url.searchParams.get('instance'), 'rm-main');
  assert.equal(url.searchParams.get('nodeId'), 'rn-hot');
  assert.equal(result.hidden, false);
  assert.equal(button.disabled, false);
  assert.equal(button.textContent, '刷新正在跑的查询');
  assert.match(result.innerHTML, /共 1,066 个会话，2 个在执行，最长已运行 885 秒/);
  assert.match(result.innerHTML, /<th>用户<\/th><th>来源 IP<\/th><th>库<\/th><th>已运行<\/th><th>状态<\/th><th>SQL<\/th>/);
  assert.match(result.innerHTML, /<td>analyst<\/td><td>10\.0\.0\.8<\/td><td>app<\/td><td><strong>14 分 45 秒<\/strong><\/td>/);
  assert.doesNotMatch(result.innerHTML, /<b>x<\/b>/);                  // statement text is escaped
  assert.match(result.innerHTML, /SELECT &lt;b&gt;x&lt;\/b&gt; FROM t/);   // and its whitespace folded
});

test('a failed live read says why and re-enables the button', async () => {
  const {c} = rdsContext(async (url) => { if (url.startsWith('/api/live-sessions')) throw new Error('DAS 会话快照超时未完成'); return impactResult; });
  await vm.runInContext('runAnalytics()', c);
  const result = {hidden: true, innerHTML: ''};
  const button = {disabled: false, textContent: '看正在跑的查询', closest: () => ({querySelector: () => result})};
  c.button = button;
  await vm.runInContext('loadLiveSessions("rn-hot", button)', c);
  assert.match(result.innerHTML, /读取失败。<\/strong>DAS 会话快照超时未完成/);
  assert.equal(button.disabled, false);
  assert.equal(button.textContent, '看正在跑的查询');
});

test('a node without a known role shows no chip', async () => {
  const bare = {...impactResult, nodes: impactResult.nodes.map(({role, zone, ...rest}) => rest)};
  const {c, nodes} = rdsContext(async () => bare);
  await vm.runInContext('runAnalytics()', c);
  assert.doesNotMatch(nodes['#analytics-panel-rise'].innerHTML, /class="chip role-/);
});

// ---- 1.31.0: replication lag as a main metric, with the origin of the lag episode ----
const lagEpisode = {first_us: 1790000060000000, peak_us: 1790004000000000, peak_s: 4079.6, last_us: 1790006000000000,
  kind: 'stall', growth_rate: 1, recovery_rate: 0.5, node_local: true,
  stall: {start_us: 1790000120000000, end_us: 1790003990000000, minutes: 64, origin_us: 1790000050000000, origin_spread_s: 0},
  trigger: {type: 'ddl', event_us: 1790000051000000, offset_s: 1, database: '', table: '', sql: 'ALTER TABLE shop.shop_jobs ADD INDEX idx_x (a) <b>x</b>'},
  queued: {count: 16, samples: [{event_id: 'ev-q1', account: 'root', start_us: 1790000052000000, duration_ms: 4077000}]},
  holders: {count: 0, samples: []}};
const lagResult = (episode, extra = {}) => ({...impactResult, metric_id: 'replication_lag', metric: 'Cluster_DataDelay', candidate_scope: 'primary_statements',
  nodes: [{node_id: 'rn-lag', role: 'secondary', zone: 'cn-chengdu-b', status: 'ok', peak: 4079.6, metric_scale: 10, metric_values: [0, 100, 40796],
    resource_comparison: {baseline_mean: 0, current_mean: 900, delta: 900}, total_fingerprints: 1,
    lag_origin: {floor_s: 30, episode_count: episode ? 1 : 0, origin_slack_s: 30, episodes: episode ? [episode] : []},
    statements: [stmt('bulk', 0.9, 60_000_000, 0, {...growing(5), growth_share: 1, origin: 'primary', source_node_id: 'rn-p'})]}], ...extra});

test('replication lag is a selectable metric and its request is the ordinary growth request', async () => {
  const {c, nodes, calls} = rdsContext(async () => lagResult(lagEpisode));
  assert.ok(vm.runInContext('RISE_METRICS.map(([id]) => id)', c).includes('replication_lag'));
  nodes['#rds-metric'].value = 'replication_lag';
  await vm.runInContext('runAnalytics()', c);
  const url = new URL(calls[0], 'http://fixture');
  assert.equal(url.searchParams.get('metric'), 'replication_lag');
  assert.equal(url.searchParams.get('order'), 'growth');
});

test('replication lag: the origin card names the stuck event, the DDL at it, the queued statements and the missing holder', async () => {
  const {c, nodes} = rdsContext(async () => lagResult(lagEpisode));
  await vm.runInContext('runAnalytics()', c);
  const html = nodes['#analytics-panel-rise'].innerHTML;
  assert.match(html, /复制延迟溯源/);
  assert.match(html, /<strong>卡在同一个事件上<\/strong>/);
  assert.match(html, /<strong>DDL<\/strong>/);
  assert.match(html, /ALTER TABLE shop\.shop_jobs ADD INDEX idx_x \(a\) &lt;b&gt;x&lt;\/b&gt;/);   // the statement text is escaped
  assert.doesNotMatch(html, /<b>x<\/b>/);
  assert.match(html, /16 条<\/strong>语句开始于卡顿之后/);
  assert.match(html, /data-lag-event="ev-q1"/);
  assert.match(html, /持锁的不在慢日志里/);
  assert.match(html, /其他备节点的延迟都没到阈值/);
  assert.match(html, /SQL（主库执行，按增长份额排序）/);
  assert.match(html, /主库上的慢 SQL/);               // the page says why the candidates are the primary's statements
  assert.doesNotMatch(html, /窗口 .* 对照昨日同窗。排序：按增长份额/);   // not the generic metric paragraph
  assert.match(html, /data-live-node="rn-lag"/);      // the running-query button stays: it helps while a stall is on
});

test('replication lag: holders found in the slow log are listed, other lagging replicas change the verdict, unexplained stalls say so', async () => {
  const holder = {...lagEpisode, node_local: false, trigger: {type: 'none'},
    holders: {count: 1, samples: [{event_id: 'ev-h', account: 'etl', start_us: 1790000000000000, duration_ms: 4100000}]}};
  const {c, nodes} = rdsContext(async () => lagResult(holder));
  await vm.runInContext('runAnalytics()', c);
  const html = nodes['#analytics-panel-rise'].innerHTML;
  assert.match(html, /持锁者候选/);
  assert.match(html, /data-lag-event="ev-h"/);
  assert.doesNotMatch(html, /持锁的不在慢日志里/);
  assert.match(html, /其他备节点也出现了延迟/);
  assert.match(html, /没有 DDL<\/strong>/);
  const unavailable = rdsContext(async () => lagResult({...lagEpisode, trigger: {type: 'unavailable'}}));
  await vm.runInContext('runAnalytics()', unavailable.c);
  assert.match(unavailable.nodes['#analytics-panel-rise'].innerHTML, /未能核对那一刻有没有 DDL/);
});

test('replication lag: a slow replica and a blip are described as what they are, a quiet node says nothing happened', async () => {
  const behind = rdsContext(async () => lagResult({...lagEpisode, kind: 'behind', stall: null, trigger: undefined, growth_rate: 0.3}));
  await vm.runInContext('runAnalytics()', behind.c);
  assert.match(behind.nodes['#analytics-panel-rise'].innerHTML, /回放速度跟不上主库的写入/);
  assert.doesNotMatch(behind.nodes['#analytics-panel-rise'].innerHTML, /<strong>卡在同一个事件上<\/strong>/);
  const blip = rdsContext(async () => lagResult({...lagEpisode, kind: 'spike', stall: null}));
  await vm.runInContext('runAnalytics()', blip.c);
  assert.match(blip.nodes['#analytics-panel-rise'].innerHTML, /短时尖峰/);
  const quiet = rdsContext(async () => lagResult(null));
  await vm.runInContext('runAnalytics()', quiet.c);
  assert.match(quiet.nodes['#analytics-panel-rise'].innerHTML, /没有达到 30 秒（告警阈值）/);
});

test('replication lag without any statement still shows the origin; a primary-only answer has its own message', async () => {
  const bare = lagResult(lagEpisode);
  bare.nodes[0].statements = [];
  const {c, nodes} = rdsContext(async () => bare);
  await vm.runInContext('runAnalytics()', c);
  assert.match(nodes['#analytics-panel-rise'].innerHTML, /<strong>卡在同一个事件上<\/strong>/);
  assert.match(nodes['#analytics-panel-rise'].innerHTML, /主库没有采集到可与延迟增长对照的慢 SQL/);
  const none = rdsContext(async () => ({status: 'no_replica_lag_samples', nodes: []}));
  await vm.runInContext('runAnalytics()', none.c);
  assert.match(none.nodes['#analytics-panel-rise'].innerHTML, /没有取到备节点的复制延迟指标/);
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
    requested_end_us: 1789997200000000, shift_us: -10_000_000_000}};   // no "kind": older servers mean a shift
  const {c, nodes, toasts} = rdsContext(async () => moved);
  await vm.runInContext('runAnalytics()', c);
  assert.equal(nodes['#analytics-start'].value, local(c, moved.start_us));
  assert.equal(nodes['#analytics-end'].value, local(c, moved.end_us));
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

test('a window cut back to the indexed edge keeps its start, says where it ends and how far behind the index is', async () => {
  const behindUs = Date.now() * 1000 - 4 * 60_000_000;
  const cut = {...impactResult, ready_through_us: behindUs, adjusted: {kind: 'truncated', reason: 'incomplete_index',
    requested_start_us: impactResult.start_us, requested_end_us: impactResult.end_us + 300_000_000, truncated_us: 300_000_000}};
  const {c, nodes, toasts} = rdsContext(async () => cut);
  await vm.runInContext('runAnalytics()', c);
  const html = nodes['#analytics-panel-rise'].innerHTML;
  assert.match(html, /已截到最近已入库的时刻（比所选结束早 5 分钟）/);
  assert.match(html, /起点不变/);
  assert.doesNotMatch(html, /已自动改查/);
  assert.match(html, /数据截至 <strong>[^<]+<\/strong>（最新已入库的慢日志，落后现在约 [45] 分钟）/);
  assert.equal(nodes['#analytics-start'].value, local(c, cut.start_us));
  assert.equal(nodes['#analytics-end'].value, local(c, cut.end_us));   // the form shows the window that was really analysed
  assert.ok(toasts.some(([kind, m]) => kind === 'info' && /已截到/.test(m)));
});

test('an indexed window is neither moved nor announced; a missing index names the 6-hour search', async () => {
  const {c, nodes} = rdsContext(async () => ({...impactResult, adjusted: null}));
  await vm.runInContext('runAnalytics()', c);
  assert.doesNotMatch(nodes['#analytics-panel-rise'].innerHTML, /已自动改查|已截到/);
  assert.doesNotMatch(nodes['#analytics-panel-rise'].innerHTML, /数据截至/);   // an old window says nothing about freshness
  assert.equal(nodes['#analytics-start'].value, '2026-09-28T10:00');
  const none = rdsContext(async () => ({status: 'incomplete_index', nodes: []}));
  await vm.runInContext('runAnalytics()', none.c);
  assert.match(none.nodes['#analytics-panel-rise'].innerHTML, /前后 6 小时内没有索引完整/);
});

test('both sources ticked: one impact request and one Binlog request; the Binlog pages fill at once from that response', async () => {
  const {c, nodes, calls} = rdsContext(async (url) => (url.includes('/api/slowlog-impact') ? impactResult : binlogResult), {binlog: true});
  await vm.runInContext('runAnalytics()', c);
  assert.equal(calls.length, 2);
  const byPath = Object.fromEntries(calls.map((u) => [new URL(u, 'http://fixture').pathname, new URL(u, 'http://fixture')]));
  assert.equal(byPath['/api/slowlog-impact'].searchParams.get('order'), 'growth');
  const binlog = byPath['/api/analytics'];
  assert.equal(binlog.searchParams.get('source'), 'binlog');
  assert.ok(!binlog.searchParams.has('metric') && !binlog.searchParams.has('nodeId'));
  assert.equal(binlog.searchParams.get('instance'), 'rm-main');
  assert.equal(nodes['#analytics-panel-writes'].innerHTML, 'SQL[binlog:b]');
  assert.equal(nodes['#analytics-panel-transactions'].innerHTML, 'TXN');
  assert.equal(nodes['#analytics-panel-locks'].innerHTML, 'LOCKS');
  assert.equal(vm.runInContext('state.analyticsRuns.binlog.result.sql.tag', c), 'b');
  // switching tabs never requests again
  await vm.runInContext('openAnalyticsTab("writes")', c);
  await vm.runInContext('openAnalyticsTab("locks")', c);
  assert.equal(calls.length, 2);
  assert.equal(nodes['#analytics-lock-warning'].hidden, false);
  vm.runInContext('switchAnalyticsTab("writes")', c);
  assert.equal(nodes['#analytics-lock-warning'].hidden, true);
});

test('only Binlog ticked: no impact request, no instance or metric needed, the writes page opens', async () => {
  const {c, nodes, calls} = rdsContext(async () => binlogResult, {slowlog: false, binlog: true});
  nodes['#analytics-instance'].value = '';
  await vm.runInContext('runAnalytics()', c);
  assert.equal(calls.length, 1);
  const url = new URL(calls[0], 'http://fixture');
  assert.equal(url.pathname, '/api/analytics');
  assert.equal(url.searchParams.get('source'), 'binlog');
  assert.ok(!url.searchParams.has('instance'));
  assert.equal(vm.runInContext('state.analyticsTab', c), 'writes');
  assert.equal(nodes['#analytics-panel-rise']?.innerHTML ?? '', '');
  assert.ok(!/CPU/.test(nodes['#analytics-meta'].textContent), 'no metric in the scope line without the slow log');
});

test('nothing ticked is refused before any request', async () => {
  const {c, calls} = rdsContext(async () => impactResult, {slowlog: false, binlog: false});
  await assert.rejects(vm.runInContext('runAnalytics()', c), /请至少勾选一个证据来源/);
  assert.equal(calls.length, 0);
});

test('one failed source keeps the other usable and is reported; every ticked source failing is an error', async () => {
  const {c, nodes, toasts} = rdsContext(async (url) => {
    if (url.includes('/api/slowlog-impact')) return impactResult;
    throw new Error('clickhouse down');
  }, {binlog: true});
  await vm.runInContext('runAnalytics()', c);
  assert.match(nodes['#analytics-panel-rise'].innerHTML, /rn-hot/);
  assert.match(nodes['#analytics-panel-writes'].innerHTML, /Binlog 写入分析失败.*clickhouse down/);
  assert.match(nodes['#analytics-panel-locks'].innerHTML, /clickhouse down/);
  assert.ok(toasts.some(([k, m]) => k === 'error' && /Binlog 写入分析失败：clickhouse down/.test(m)));
  assert.ok(!toasts.some(([k]) => k === 'success'), 'a partial failure is not announced as success');
  const both = rdsContext(async () => { throw new Error('boom'); }, {binlog: true});
  await assert.rejects(vm.runInContext('runAnalytics()', both.c), /boom/);
});

test('the slow-log detail loads once, on demand, from the investigation window; a new investigation invalidates it', async () => {
  const {c, nodes, calls} = rdsContext(async (url) => (url.includes('/api/slowlog-impact') ? impactResult : slowResult));
  await vm.runInContext('runAnalytics()', c);
  assert.equal(calls.length, 1);
  await vm.runInContext('openAnalyticsTab("sql")', c);
  assert.equal(calls.length, 2);
  const detail = new URL(calls[1], 'http://fixture');
  assert.equal(detail.pathname, '/api/analytics');
  assert.equal(detail.searchParams.get('source'), 'slowlog');
  assert.ok(!detail.searchParams.has('metric') && detail.searchParams.get('instance') === 'rm-main');
  assert.equal(nodes['#analytics-panel-sql'].innerHTML, 'SQL[slowlog:s]');
  assert.equal(vm.runInContext('state.analytics.sql.tag', c), 's');
  await vm.runInContext('openAnalyticsTab("sql")', c);
  assert.equal(calls.length, 2, 'opening it again never requests again');
  await vm.runInContext('runAnalytics()', c);
  await vm.runInContext('openAnalyticsTab("sql")', c);
  assert.equal(calls.length, 2 + 1 + 1);
});

test('a failed slow-log detail is reported on its own page', async () => {
  const {c, nodes, toasts} = rdsContext(async (url) => {
    if (url.includes('/api/slowlog-impact')) return impactResult;
    throw new Error('slow-log index busy');
  });
  await vm.runInContext('runAnalytics()', c);
  await vm.runInContext('openAnalyticsTab("sql")', c);
  assert.match(nodes['#analytics-panel-sql'].innerHTML, /慢日志分析失败.*slow-log index busy/);
  assert.ok(toasts.some(([k, m]) => k === 'error' && /慢日志明细加载失败/.test(m)));
});

test('a Binlog window the server repaired is announced on its page and, without the slow log, applied to the form', async () => {
  const requestedEnd = 1790007200000000 + 600_000_000;
  const repaired = {...binlogResult, ready_through_us: Date.now() * 1000 - 3 * 60_000_000, adjusted: {kind: 'truncated',
    reason: 'incomplete_binlog_coverage', requested_start_us: 1790000000000000, requested_end_us: requestedEnd, truncated_us: 600_000_000}};
  const {c, nodes, toasts} = rdsContext(async () => repaired, {slowlog: false, binlog: true});
  await vm.runInContext('runAnalytics()', c);
  const html = nodes['#analytics-panel-writes'].innerHTML;
  assert.match(html, /Binlog 已截到最近已入库的时刻（比所选结束早 10 分钟）/);
  assert.match(html, /Binlog 已入库到 <strong>[^<]+<\/strong>（落后现在约 [34] 分钟）/);
  assert.match(html, /SQL\[binlog:b\]$/);
  assert.equal(nodes['#analytics-end'].value, local(c, 1790007200000000));
  assert.equal(nodes['#analytics-range'].value, 'custom');
  assert.ok(toasts.some(([kind, m]) => kind === 'info' && /Binlog 窗口已对齐/.test(m)));
  const moved = {...repaired, adjusted: {...repaired.adjusted, kind: 'shifted', shift_us: -3_600_000_000, truncated_us: undefined}};
  const shifted = rdsContext(async () => moved, {slowlog: false, binlog: true});
  await vm.runInContext('runAnalytics()', shifted.c);
  assert.match(shifted.nodes['#analytics-panel-writes'].innerHTML, /Binlog 已自动改查最近的已入库时段（早 1 小时）/);
  assert.equal(shifted.nodes['#analytics-start'].value, local(shifted.c, 1790000000000000 - 3_600_000_000));
});

test('the 24-hour default is clamped to 23 h 55 m before it can be rejected, and the form follows', async () => {
  const {c, nodes, calls} = rdsContext(async () => impactResult, {start: '2026-09-27T12:00', end: '2026-09-28T12:00'});
  await vm.runInContext('runAnalytics()', c);
  const url = new URL(calls[0], 'http://fixture');
  const end = Number(url.searchParams.get('endEpochUs')), start = Number(url.searchParams.get('startEpochUs'));
  assert.equal(end - start, (24 * 60 - 5) * 60_000_000);
  assert.equal(nodes['#analytics-start'].value, local(c, start));
  assert.equal(nodes['#analytics-range'].value, 'custom');
  assert.match(nodes['#analytics-panel-rise'].innerHTML, /时间框已调整[\s\S]*23 小时 55 分/);
});

test('start and end are rounded down to whole minutes (5 minutes when only Binlog is asked) and written back', async () => {
  const slow = rdsContext(async () => impactResult, {start: '2026-09-28T10:00:40', end: '2026-09-28T12:07:59'});
  await vm.runInContext('runAnalytics()', slow.c);
  const query = new URL(slow.calls[0], 'http://fixture').searchParams;
  assert.equal(Number(query.get('startEpochUs')) % 60_000_000, 0);
  assert.equal(Number(query.get('endEpochUs')) % 60_000_000, 0);
  assert.equal(slow.nodes['#analytics-start'].value, '2026-09-28T10:00:00');
  assert.equal(slow.nodes['#analytics-end'].value, '2026-09-28T12:07:00');
  assert.equal(slow.nodes['#analytics-range'].value, '24h', 'rounding alone does not turn the preset into a custom range');
  const only = rdsContext(async () => binlogResult, {slowlog: false, binlog: true, start: '2026-09-28T10:03:00', end: '2026-09-28T12:07:59'});
  await vm.runInContext('runAnalytics()', only.c);
  const binlog = new URL(only.calls[0], 'http://fixture').searchParams;
  assert.equal(Number(binlog.get('startEpochUs')) % 300_000_000, 0);
  assert.equal(Number(binlog.get('endEpochUs')) % 300_000_000, 0);
  assert.equal(only.nodes['#analytics-start'].value, '2026-09-28T10:00:00');
  assert.equal(only.nodes['#analytics-end'].value, '2026-09-28T12:05:00');
  // a window that is already whole minutes is left exactly as typed (no write-back while the user is editing)
  const same = rdsContext(async () => impactResult);
  await vm.runInContext('runAnalytics()', same.c);
  assert.equal(same.nodes['#analytics-start'].value, '2026-09-28T10:00');
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

test('data source is exactly RDS and MongoDB; evidence sources are two ticked-by-default options; 写入 SQL is a first-level page', () => {
  const options = [...html.matchAll(/<select id="analytics-source"[^>]*>(.*?)<\/select>/gs)][0][1];
  assert.deepEqual([...options.matchAll(/value="([^"]+)"/g)].map((m) => m[1]), ['rds', 'mongodb']);
  for (const id of ['analytics-use-slowlog', 'analytics-use-binlog']) {
    assert.match(html, new RegExp(`<input id="${id}" type="checkbox" checked>`), id);
  }
  assert.match(html, /<fieldset id="rds-sources"[^>]*>\s*<legend>证据来源<\/legend>/);
  const tabs = [...html.matchAll(/<button data-analytics-tab="([^"]+)"([^>]*)>/g)].map((m) => [m[1], /is-detail/.test(m[2]) && /hidden/.test(m[2])]);
  assert.deepEqual(tabs, [['rise', false], ['sql', true], ['writes', false], ['transactions', true], ['locks', true]]);
  assert.ok(html.includes('id="analytics-more-views"'));
  for (const id of ['rise', 'sql', 'writes', 'transactions', 'locks']) assert.ok(html.includes(`id="analytics-panel-${id}"`), id);
  assert.match(html, /rise\.js/);
  const metrics = [...rise.matchAll(/\["(\w+)", "[^"]+"\]/g)].map((m) => m[1]).slice(0, 5);
  assert.deepEqual(metrics, ['cpu', 'iops', 'rows_read', 'row_lock', 'threads']);
});

test('the 对齐节点 IOPS entry is gone: one investigation, one ranking', () => {
  const code = (text) => text.replace(/\/\*[\s\S]*?\*\//g, '');   // the history note in rise.js's header may name the old entry
  for (const text of [html, workspace, app, rise]) assert.doesNotMatch(code(text), /对齐节点 IOPS|resource-analysis|openResourceAnalysis|renderResourceAnalysis|showPerformanceRanking/);
  assert.doesNotMatch(app + workspace, /性能关联（昨日基线）|'performance'/);
  assert.match(html, /<details class="action-menu"><summary>更多操作<\/summary><div class="menu-content"><button id="analytics-reset"[^>]*>重置条件<\/button><\/div><\/details>/);
});
