const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const web = (name) => fs.readFileSync(path.join(__dirname, '../web', name), 'utf8');
const sources = ['workspace.js', 'app.js', 'sessions.js'].map(web).join('\n');
const html = web('index.html');

function context({status = null, api = async () => ({})} = {}) {
  const nodes = {}, lists = {}, calls = [], toasts = [], downloads = [];
  const node = (selector) => (nodes[selector] ??= {innerHTML: '', textContent: '', hidden: false, value: '', className: '', options: [],
    classList: {toggle() {}}, addEventListener() {}, dataset: {}});
  class Blob { constructor(parts, options) { this.parts = parts; this.type = options?.type; } }
  const c = vm.createContext({console, URLSearchParams, Date, Promise, Number, Object, Math, JSON, Infinity, String, Set, Map, Blob,
    URL: {createObjectURL: (blob) => { downloads.push(blob); return 'blob:x'; }, revokeObjectURL() {}}, setTimeout: () => 0,
    document: {addEventListener() {}, querySelector: node, querySelectorAll: (selector) => lists[selector] || [],
      createElement: () => ({click() { downloads.push('clicked'); }, remove() {}}), body: {append() {}}}});
  vm.runInContext(sources, c);
  c.api = async (url, options) => { calls.push([url, options]); return api(url, options); };
  c.toast = (message, kind) => toasts.push([kind, message]);
  c.showDetailDrawer = () => { nodes.drawerShown = true; };
  if (status) vm.runInContext(`state.status = ${JSON.stringify(status)}`, c);
  return {c, nodes, lists, calls, toasts, downloads, run: (code) => vm.runInContext(code, c)};
}

const SNAP = 1_790_000_000_000_000;
const row = (id, extra = {}) => ({session_id: id, user: 'app', client: '10.0.0.1', database: 'shop', command: 'Sleep', state: '', seconds: 0, trx_id: '', trx_seconds: 0,
  sql: '', sql_chars: 0, active: false, running: false, waiting: false, kinds: [], protected: '', holder: false, ...extra});
const node = (id, role, sessions, extra = {}) => ({node_id: id, role, zone: 'cn-chengdu-b', status: 'ok', cached: false, observed_at_us: SNAP, instance_id: 'rm-x',
  thresholds: {blocking_s: 30, trx_s: 30, idle_trx_s: 10}, truncated: false,
  overview: {total: 1170, listed_total: sessions.length, active: sessions.filter((r) => r.active).length, running: sessions.filter((r) => r.running).length,
    max_active_seconds: Math.max(0, ...sessions.filter((r) => r.running).map((r) => r.seconds)), abnormal: sessions.filter((r) => r.kinds.length).length,
    kinds: {long_running: sessions.filter((r) => r.kinds.includes('long_running')).length, lock_wait: 0, long_trx: sessions.filter((r) => r.kinds.includes('long_trx')).length,
      idle_trx: sessions.filter((r) => r.kinds.includes('idle_trx')).length}},
  holders: {waiters: 0, oldest_wait_s: 0, candidates: 0},
  stats: {user: [{key: 'app', total: 1100, active: 5, abnormal: 1}, {key: 'etl', total: 70, active: 2, abnormal: 1}],
    client: [{key: '10.0.0.1', total: 900, active: 4, abnormal: 1}, {key: '10.0.0.9', total: 270, active: 3, abnormal: 1}],
    database: [{key: 'shop', total: 1000, active: 5, abnormal: 1}, {key: 'dw', total: 170, active: 2, abnormal: 1}], more: {}},
  sessions, ...extra});
const idle = row(312, {command: 'Sleep', seconds: 905, trx_id: '9', trx_seconds: 905, active: true, kinds: ['long_trx', 'idle_trx'], holder: true, holder_basis: 'transaction'});
const slow = row(44, {user: 'etl', client: '10.0.0.9', database: 'dw', command: 'Query', state: 'executing', seconds: 70, sql: 'SELECT * FROM big', sql_chars: 17, active: true, running: true, kinds: ['long_running']});
const repl = row(9, {user: 'repl', command: 'Binlog Dump GTID', seconds: 99999, protected: '复制线程（Binlog Dump）'});
const quiet = row(7, {seconds: 3});
const payload = (extra = {}) => ({instance_id: 'rm-x', thresholds: {blocking_s: 30, trx_s: 30, idle_trx_s: 10}, partial: false, generated_us: SNAP,
  nodes: [node('rn-primary00000001', 'primary', [slow, quiet, repl]), node('rn-secondary0000b', 'secondary', [idle, row(8, {seconds: 1})])], ...extra});

test('the page is wired: nav item, view, assets, no inline script or style', () => {
  assert.match(html, /<button class="nav-item" data-view="sessions"/);
  assert.match(html, /<section id="view-sessions" class="view">/);
  assert.match(html, /<script src="\/assets\/sessions\.js\?v=[^"]+" defer><\/script>/);
  const ids = [...html.matchAll(/\bid="([^"]+)"/g)].map((m) => m[1]);
  assert.equal(new Set(ids).size, ids.length, 'duplicate DOM IDs');
  for (const id of ['sessions-form', 'sessions-instance', 'sessions-node', 'sessions-refresh', 'sessions-summary', 'sessions-stats', 'sessions-list', 'sessions-history', 'sessions-retention']) {
    assert.ok(ids.includes(id), id);
  }
  // the service sends style-src 'self': a generated style attribute would be dropped by the browser
  assert.doesNotMatch(web('sessions.js'), /style="|onclick=|onchange=/);
  assert.match(web('app.js'), /sessions: \["SESSION CONTROL", "会话管理"\]/);
});

test('the summary adds the nodes up and names each abnormal kind; a failed node says why and does not hide the others', () => {
  const {run, c} = context();
  const data = payload();
  data.nodes.push({node_id: 'rn-third0000000003', role: 'secondary', zone: 'cn-chengdu-a', status: 'error', code: 'DAS_THROTTLED', message: 'Throttling.User'});
  data.partial = true;
  c.data = data;
  const out = run('sessionSummaryHtml(data)');
  assert.match(out, /<span>异常会话<\/span><strong>2<\/strong><em>语句运行过久 1 · 事务过长 1 · 未提交事务 1<\/em>/);
  assert.match(out, /<span>运行中会话<\/span><strong>1<\/strong>/);
  assert.match(out, /<span>会话总数<\/span><strong>2,340<\/strong><em>2 个节点<\/em>/);
  assert.match(out, /<span>运行中最长<\/span><strong>1 分 10 秒<\/strong>/);
  assert.match(out, /DAS 限流，稍后再刷新/);
  assert.match(out, /<code title="rn-primary00000001">…00000001<\/code>/);   // node ids are shortened, the full id stays in the title
  assert.match(out, /title="rn-primary00000001"/);
  assert.match(out, /chip role-primary/);
});

test('sessions waiting for a lock are called out per node with the oldest wait; a quiet node shows nothing', () => {
  const {run, c} = context();
  const data = payload();
  data.nodes[1].holders = {waiters: 16, oldest_wait_s: 4077, candidates: 1};
  c.data = data;
  const out = run('sessionHolderHtml(data)');
  assert.match(out, /有会话在等锁/);
  assert.match(out, /16 个会话在等锁，最久已等 <strong>1 小时 8 分<\/strong>/);
  assert.match(out, /列表里标了「疑似持锁」/);
  assert.match(out, /先杀持锁者，不是等待者/);
  data.nodes[1].holders = {waiters: 3, oldest_wait_s: 90, candidates: 0};
  assert.match(run('sessionHolderHtml(data)'), /持锁者可能已结束或不在这个节点上/);
  data.nodes[1].holders = {waiters: 0, oldest_wait_s: 0, candidates: 0};
  assert.equal(run('sessionHolderHtml(data)'), '');
});

test('statistics merge the nodes, put abnormal first, and a click filters the list (a second click clears it)', () => {
  const {run, c} = context();
  c.data = payload();
  run('sessionsState.data = data');
  const out = run('sessionStatsHtml(data)');
  assert.match(out, /<h3>按用户<\/h3>/);
  assert.match(out, /<h3>按访问来源<\/h3>/);
  assert.match(out, /<h3>按数据库<\/h3>/);
  assert.match(out, /data-session-filter="user" data-session-value="app"[^>]*>\s*<td>app<\/td><td>2,200<\/td><td>10<\/td><td><strong>2<\/strong><\/td>/);
  run('sessionsFilterBy("user", "etl")');
  assert.deepEqual(JSON.parse(run('JSON.stringify(sessionsState.filters)')), {q: '', user: 'etl', client: '', database: '', minSeconds: 0});
  assert.match(run('sessionStatsHtml(data)'), /session-stat-row is-picked" data-session-filter="user" data-session-value="etl"/);
  assert.deepEqual(JSON.parse(run('JSON.stringify(sessionVisible().map((r) => r.session_id))')), [44]);   // etl's slow query is abnormal: it is on the default tab
  run('sessionsState.tab = "all"');
  assert.deepEqual(JSON.parse(run('JSON.stringify(sessionVisible().map((r) => r.session_id))')), [44]);
  run('sessionsFilterBy("user", "etl")');
  assert.equal(run('sessionsState.filters.user'), '');
});

test('tabs count what they hold and the abnormal one lists flagged and lock-holder sessions first', () => {
  const {run, c} = context();
  c.data = payload();
  run('sessionsState.data = data; sessionsState.tab = "abnormal"');
  const out = run('sessionListHtml()');
  assert.match(out, /data-session-tab="abnormal" class="is-active">异常会话 2</);
  assert.match(out, /data-session-tab="active" class="">活跃会话 2</);
  assert.match(out, /data-session-tab="all" class="">全部会话 5</);
  assert.deepEqual(JSON.parse(run('JSON.stringify(sessionVisible().map((r) => r.session_id))')), [312, 44]);     // the holder first, then the slow one
  run('sessionsState.tab = "all"');
  assert.deepEqual(JSON.parse(run('JSON.stringify(sessionVisible().map((r) => r.session_id))')), [312, 44, 9, 7, 8]);
  run('sessionsState.sort = "id"; sessionsState.tab = "all"');
  assert.deepEqual(JSON.parse(run('JSON.stringify(sessionVisible().map((r) => r.session_id))')), [312, 44, 9, 8, 7]);   // flagged stay on top, then by id
});

test('rows show the reasons, escape what the server sent, and a protected session cannot be picked', () => {
  const {run, c} = context();
  const evil = row(5, {user: '<img onerror=alert(1)>', sql: 'SELECT "<script>x</script>" FROM t', sql_chars: 33, command: 'Query', seconds: 80, active: true, running: true, kinds: ['long_running']});
  const data = payload();
  data.nodes[0].sessions.push(evil);
  c.data = data;
  run('sessionsState.data = data; sessionsState.tab = "all"');
  const out = run('sessionListHtml()');
  assert.doesNotMatch(out, /<img onerror|<script>x/);
  assert.match(out, /&lt;img onerror=alert\(1\)&gt;/);
  assert.match(out, /chip warn" title="语句执行超过阈值（默认 30 秒）">语句运行过久/);
  assert.match(out, /chip session-holder"[^>]*>疑似持锁/);
  assert.match(out, /title="[^"]*未结束的事务[^"]*">疑似持锁/);
  assert.match(out, /<input type="checkbox" data-session-pick="rn-primary00000001:9" aria-label="选择会话 9" disabled title="复制线程（Binlog Dump）：不生成 KILL 语句">/);
  assert.match(out, /受保护/);
  assert.doesNotMatch(out, /data-session-pick="rn-secondary0000b:312" aria-label="选择会话 312" disabled/);
});

test('filters: keyword over user, source, database, SQL and state; minimum seconds; pagination keeps the page in range', () => {
  const {run, c} = context();
  const many = Array.from({length: 250}, (_, i) => row(1000 + i, {seconds: i, user: i % 2 ? 'odd' : 'even', sql: i === 3 ? 'SELECT needle' : ''}));
  const data = payload();
  data.nodes = [node('rn-primary00000001', 'primary', many)];
  c.data = data;
  run('sessionsState.data = data; sessionsState.tab = "all"');
  assert.equal(run('sessionVisible().length'), 250);
  run('sessionsState.filters.q = "NEEDLE"');
  assert.deepEqual(JSON.parse(run('JSON.stringify(sessionVisible().map((r) => r.session_id))')), [1003]);
  run('sessionsState.filters.q = ""; sessionsState.filters.minSeconds = 240');
  assert.equal(run('sessionVisible().length'), 10);
  run('sessionsState.filters.minSeconds = 0; sessionsState.filters.q = "odd"');
  assert.equal(run('sessionVisible().length'), 125);
  run('sessionsState.filters.q = ""; sessionsState.page = 9');
  const out = run('sessionListHtml()');
  assert.match(out, /第 3 \/ 3 页 · 共 250 条/);                                          // page 9 was clamped to the last
  assert.equal(run('sessionsState.page'), 2);
  assert.match(out, /data-session-page="1" disabled/);
});

test('picking sessions enables the KILL button with the count; a refresh drops selections that are gone', () => {
  const {run, c} = context();
  c.data = payload();
  run('sessionsState.data = data; sessionsState.tab = "abnormal"');
  assert.match(run('sessionListHtml()'), /data-session-kill disabled/);
  run('sessionsChange({target: {dataset: {sessionPick: "rn-primary00000001:44"}, checked: true}})');
  run('sessionsChange({target: {dataset: {sessionPick: "rn-gone:1"}, checked: true}})');
  assert.match(run('sessionListHtml()'), /data-session-kill title="只生成语句，不在本页执行">生成 KILL 语句（1）/);
  assert.match(run('sessionListHtml()'), /session-row is-flagged is-picked/);
  run('sessionsChange({target: {dataset: {sessionPick: "rn-primary00000001:44"}, checked: false}})');
  assert.match(run('sessionListHtml()'), /data-session-kill disabled/);
});

test('the header checkbox picks every session on the page that may be ended, and clears them; protected rows are left out; half-picked is marked', () => {
  const {run, c} = context();
  c.data = payload();
  run('sessionsState.data = data; sessionsState.tab = "all"; sessionsState.selected = new Set()');
  let out = run('sessionListHtml()');
  assert.match(out, /<th class="session-check"><input type="checkbox" data-session-pick-all aria-label="全选本页可结束的会话" title="全选 \/ 取消本页 4 个可结束的会话"><\/th>/);
  run('sessionsChange({target: {dataset: {sessionPickAll: ""}, checked: true}})');
  assert.deepEqual(JSON.parse(run('JSON.stringify([...sessionsState.selected].sort())')),
    ['rn-primary00000001:44', 'rn-primary00000001:7', 'rn-secondary0000b:312', 'rn-secondary0000b:8']);   // 9 is protected: never picked
  out = run('sessionListHtml()');
  assert.match(out, /data-session-pick-all [^>]*" checked>/);
  assert.doesNotMatch(out, /data-indeterminate/);
  assert.match(out, /生成 KILL 语句（4）/);
  run('sessionsChange({target: {dataset: {sessionPick: "rn-primary00000001:7"}, checked: false}})');
  out = run('sessionListHtml()');
  assert.doesNotMatch(out, /data-session-pick-all [^>]*" checked>/);
  assert.match(out, /data-session-pick-all [^>]*" data-indeterminate>/);
  run('sessionsChange({target: {dataset: {sessionPickAll: ""}, checked: false}})');
  assert.equal(run('sessionsState.selected.size'), 0);
  // on the abnormal tab only its rows are on the page
  run('sessionsState.tab = "abnormal"');
  run('sessionsChange({target: {dataset: {sessionPickAll: ""}, checked: true}})');
  assert.deepEqual(JSON.parse(run('JSON.stringify([...sessionsState.selected].sort())')), ['rn-primary00000001:44', 'rn-secondary0000b:312']);
  // only protected rows on the page: the box is disabled
  run('sessionsState.tab = "all"; sessionsState.filters.q = "repl"');
  assert.match(run('sessionListHtml()'), /data-session-pick-all [^>]*title="本页没有可结束的会话" disabled>/);
});

test('a pick belongs to the list on screen: switching tab drops the picks the new tab does not show, so the count and the KILL request never include them', async () => {
  const {run, c, calls} = context();
  c.data = payload();
  run('sessionsState.data = data; sessionsState.selected = new Set(); sessionsState.tab = "all"');
  run('sessionsChange({target: {dataset: {sessionPick: "rn-primary00000001:7"}, checked: true}})');   // quiet session 7: on the all tab only
  assert.match(run('sessionListHtml()'), /生成 KILL 语句（1）/);
  run('sessionsState.tab = "abnormal"');
  assert.match(run('sessionListHtml()'), /data-session-kill disabled/);
  assert.equal(run('sessionsState.selected.size'), 0);
  await assert.rejects(run('generateSessionKill()'), /请先勾选要结束的会话/);
  assert.equal(calls.length, 0);
  run('sessionsState.tab = "all"');
  assert.match(run('sessionListHtml()'), /data-session-kill disabled/);                             // the pick did not come back
});

test('a refresh that moves a picked session out of the list drops the pick', () => {
  const {run, c} = context();
  c.data = payload();
  run('sessionsState.data = data; sessionsState.selected = new Set(); sessionsState.tab = "abnormal"');
  run('sessionsChange({target: {dataset: {sessionPick: "rn-primary00000001:44"}, checked: true}})');
  assert.match(run('sessionListHtml()'), /生成 KILL 语句（1）/);
  const later = payload();                                                                          // the next snapshot: 44 is no longer abnormal
  later.nodes[0].sessions[0] = {...later.nodes[0].sessions[0], kinds: [], holder: false};
  c.data = later;
  run('sessionsState.data = data; renderSessionsList()');
  assert.equal(run('sessionsState.selected.size'), 0);
  assert.match(run('sessionListHtml()'), /data-session-kill disabled/);
});

test('KILL statements: one request for every node with what the page showed, each node\'s snapshot time and waiters, and the mode; one text, escaped, copied, regenerated per mode', async () => {
  const written = (url, options) => {
    const body = JSON.parse(options.body);
    const nodes = body.nodes.map((n) => ({node_id: n.node_id, role: n.node_id.startsWith('rn-primary') ? 'primary' : 'secondary', zone: 'cn-chengdu-b',
      hint: n.node_id.startsWith('rn-primary') ? 'FORCE_MASTER' : '', snapshot_us: n.snapshot_us, session_ids: n.sessions.map((s) => s.session_id),
      statements: n.sessions.map((s) => `KILL ${s.session_id};`), skipped: []}));
    const statements = nodes.flatMap((n) => n.statements);
    return {text: `-- ${body.mode}\n${statements.join('\n')}\n-- <b>x</b>\n`, checks: [], statements, mode: body.mode, session_ids: [], generated_us: SNAP, nodes,
      skipped: [{node_id: 'rn-primary00000001', session_id: 9, reason: '复制线程（Binlog Dump）'}]};
  };
  const {run, c, calls, nodes, toasts} = context({api: async (url, options) => written(url, options)});
  c.data = payload();
  c.data.nodes[1].holders = {waiters: 16, oldest_wait_s: 4077, candidates: 1};            // sessions wait for a lock on the standby only
  // on the all tab every pick is on screen; a pick the page does not show is not sent (see the picks tests above)
  run('sessionsState.data = data; sessionsState.tab = "all"; sessionsState.selected = new Set(["rn-primary00000001:44", "rn-primary00000001:9", "rn-secondary0000b:312"])');
  await run('generateSessionKill()');
  assert.equal(calls.length, 1);                                                           // every node in one request: one text comes back
  const [url, options] = calls[0];
  assert.equal(url, '/api/sessions/kill-statements');
  assert.equal(options.method, 'POST');
  const body = JSON.parse(options.body);
  assert.deepEqual(Object.keys(body).sort(), ['instance', 'mode', 'nodes']);
  assert.equal(body.instance, 'rm-x');
  assert.equal(body.mode, 'connection');
  const primary = body.nodes.find((n) => n.node_id === 'rn-primary00000001'), standby = body.nodes.find((n) => n.node_id === 'rn-secondary0000b');
  assert.equal(body.nodes.length, 2);
  assert.equal(primary.snapshot_us, SNAP);
  assert.equal(primary.lock_waiters, 0);                                                   // each node's own waiters decide whether its lock check is written
  assert.equal(standby.lock_waiters, 16);
  assert.deepEqual(primary.sessions.map((s) => s.session_id).sort((a, b) => a - b), [9, 44]);
  assert.deepEqual(standby.sessions.map((s) => s.session_id), [312]);
  assert.deepEqual(Object.keys(primary.sessions[0]).sort(), ['client', 'command', 'database', 'kinds', 'seconds', 'session_id', 'trx_seconds', 'user']);
  assert.equal(nodes.drawerShown, true);
  const html = nodes['#detail-body'].innerHTML;
  assert.equal(nodes['#detail-title'].textContent, 'KILL 语句');
  assert.match(html, /只生成语句，不执行。所有节点的语句合在一起/);
  assert.match(html, /含<strong>主节点<\/strong>的会话：杀错会话会直接影响线上写入/);
  assert.match(html, /跳过 1 个受保护会话：9（复制线程（Binlog Dump））/);
  assert.match(html, /节点 rn-secondary0000b 的 ID 不能写进 Hint/);                         // only the node whose answer had no hint
  assert.doesNotMatch(html, /节点 rn-primary00000001 的 ID/);
  assert.equal(html.match(/<textarea class="session-kill-text"/g).length, 1);            // one text and one copy button, whatever the nodes
  assert.equal(html.match(/data-session-copy/g).length, 1);
  assert.match(html, /3 条 KILL · 2 个节点/);
  assert.match(html, /<code title="rn-primary00000001">…00000001<\/code> <span class="chip role-primary">主节点<\/span>[^]*?<span class="muted">2 条 · 快照 /);
  assert.match(html, /<code title="rn-secondary0000b">…ary0000b<\/code>[^]*?<span class="muted">1 条 · 快照 /);
  assert.match(html, /-- &lt;b&gt;x&lt;\/b&gt;/);
  assert.doesNotMatch(html, /<b>x<\/b>/);
  assert.match(html, /data-session-killmode="connection" class="is-active"/);
  // KILL QUERY regenerates the text with the other mode
  await run('regenerateSessionKill("query")');
  assert.equal(JSON.parse(calls.at(-1)[1].body).mode, 'query');
  assert.match(nodes['#detail-body'].innerHTML, /data-session-killmode="query" class="is-active"/);
  // copy: the clipboard gets exactly the generated text
  let copied = '';
  c.navigator = {clipboard: {writeText: async (text) => { copied = text; }}};
  run('copySessionKill()');
  assert.match(copied, /^-- query\n(KILL \d+;\n)+-- <b>x<\/b>\n$/);
  assert.deepEqual(copied.split('\n').filter((line) => line.startsWith('KILL ')).sort(), ['KILL 312;', 'KILL 44;', 'KILL 9;']);
  await new Promise((resolve) => setImmediate(resolve));
  assert.ok(toasts.some(([kind, message]) => kind === 'success' && /已复制/.test(message)));
});

test('nothing selected is a message, not a request', async () => {
  const {run, c, calls} = context();
  c.data = payload();
  run('sessionsState.data = data; sessionsState.selected = new Set()');
  await assert.rejects(run('generateSessionKill()'), /请先勾选要结束的会话/);
  assert.equal(calls.length, 0);
});

test('CSV export quotes, keeps a spreadsheet from running SQL as a formula, and lists only active sessions', () => {
  const {run, c, downloads} = context();
  assert.equal(run('sessionCsvCell("a,b")'), '"a,b"');
  assert.equal(run('sessionCsvCell(\'say "hi"\')'), '"say ""hi"""');
  assert.equal(run('sessionCsvCell("=cmd|\' /C calc\'!A0")'), "'=cmd|' /C calc'!A0");
  assert.equal(run('sessionCsvCell("+1")'), "'+1");
  assert.equal(run('sessionCsvCell(null)'), '');
  const data = payload();
  data.nodes[0].sessions.push(row(66, {command: 'Query', seconds: 5, active: true, running: true, sql: 'SELECT 1,\n2', user: '=evil'}));
  c.data = data;
  run('sessionsState.data = data');
  run('exportActiveSessions()');
  assert.equal(downloads.at(-1), 'clicked');
  const blob = downloads.find((item) => item && item.parts);
  const text = blob.parts[1];
  assert.equal(blob.parts[0], '﻿');
  const lines = text.split('\r\n');
  assert.equal(lines[0], '节点,角色,会话ID,用户,来源,数据库,命令,状态,执行秒,事务秒,事务ID,异常原因,SQL');
  assert.equal(lines.length, 1 + 3);                                                      // 44, 66 and 312 are active; quiet sessions are left out
  assert.ok(lines.some((line) => line.includes("'=evil")));
  assert.ok(lines.some((line) => line.includes('"SELECT 1, 2"')));
  assert.ok(lines.every((line) => !/\n/.test(line)));
});

test('history: coverage per node with a warning for a thin one, rows with their span and "same time" buttons, empty and truncated states', () => {
  const {run, c} = context();
  const hour = 3600 * 1_000_000;
  const data = {instance_id: 'rm-x', start_us: SNAP, end_us: SNAP + hour, truncated: true,
    retention: {interval_seconds: 60, nodes: []}, nodes: [{node_id: 'rn-primary00000001', role: 'primary', zone: 'cn-chengdu-a'}, {node_id: 'rn-secondary0000b', role: 'secondary', zone: 'cn-chengdu-b'}],
    coverage: [{node_id: 'rn-primary00000001', snapshots: 59, first_us: SNAP, last_us: SNAP + hour, most_abnormal: 3}, {node_id: 'rn-secondary0000b', snapshots: 10, first_us: SNAP, last_us: SNAP + 600_000_000, most_abnormal: 1}],
    sessions: [{node_id: 'rn-secondary0000b', session_id: 312, first_us: SNAP + 60_000_000, last_us: SNAP + 3_600_000_000, seen: 60, user: 'app', client: '10.0.0.1', database: 'shop',
      command: 'Sleep', state: '', max_seconds: 3600, max_trx_seconds: 3700, kinds: ['long_trx', 'idle_trx'], holder: true, holder_basis: 'transaction', sql: ''}]};
  c.data = data;
  const out = run('sessionHistoryHtml(data)');
  assert.match(out, /59 次快照（这个窗口里应有约 60 次）/);
  assert.match(out, /10 次快照（这个窗口里应有约 60 次） <span class="chip warn" title="空白不等于没有异常">留存有缺口/);
  assert.match(out, /快照少于应有数量的 70%/);
  assert.match(out, /疑似持锁/);
  assert.match(out, /→ [^<]+（59 分钟）|→ [^<]+（59 分 0 秒）|→ [^<]+（1 小时 0 分）|→ [^<]+（59 分/);
  assert.match(out, /data-session-around="1790000060000000"/);
  assert.match(out, /只列出最近的一部分/);
  data.sessions = []; data.truncated = false; data.coverage = [];
  const silent = run('sessionHistoryHtml(data)');
  assert.match(silent, /0 次快照（这个窗口里应有约 60 次） <span class="chip warn"/);        // a node with no snapshot at all is a gap, not a quiet node
  data.nodes = [];
  const empty = run('sessionHistoryHtml(data)');
  assert.match(empty, /这个窗口里没有任何快照/);
  assert.match(empty, /这个窗口里没有留存到异常会话/);
});

test('the retention strip says on, off, or failing, and what to do', () => {
  const {run, c} = context();
  const on = {enabled: true, interval_seconds: 60, retention_days: 7, nodes: [{node_id: 'a', last_ok_us: SNAP, consecutive_failures: 0}, {node_id: 'b', last_ok_us: SNAP, consecutive_failures: 0}], store: {abnormal_sessions: 12, snapshots: 3400}};
  c.on = on;
  let view = run('sessionRetentionHtml(on)');
  assert.equal(view.kind, 'info');
  assert.match(view.html, /留存已开启/);
  assert.match(view.html, /每 60 秒读一次每个节点，只存异常会话，保留 7 天 · 2 个节点读取正常/);
  assert.match(view.html, /已存 12 条异常会话、3,400 次快照/);
  on.nodes[1] = {node_id: 'b', last_ok_us: SNAP, consecutive_failures: 3, last_error: {code: 'das_throttled'}};
  view = run('sessionRetentionHtml(on)');
  assert.equal(view.kind, 'warning');
  assert.match(view.html, /1 \/ 2 个节点最近读取失败（DAS 限流，稍后再刷新）/);
  c.off = {...on, enabled: false};
  view = run('sessionRetentionHtml(off)');
  assert.match(view.html, /留存已关闭/);
  assert.match(view.html, /data\/session-retention\.json/);
  assert.equal(run('sessionRetentionHtml(null)'), null);
});

test('the instance and node lists come from the collected nodes, primary first, defaulting to the primary instance', () => {
  const status = {primaryInstance: {instanceId: 'rm-prod'}, slowLogs: [
    {instanceId: 'rm-test', nodeId: 'rn-t1', label: 'demo-test', role: 'primary', zone: 'cn-chengdu-a'},
    {instanceId: 'rm-prod', nodeId: 'rn-b', label: 'demo-main', role: 'secondary', zone: 'cn-chengdu-b'},
    {instanceId: 'rm-prod', nodeId: 'rn-p', label: 'demo-main', role: 'primary', zone: 'cn-chengdu-a'},
    {instanceId: 'rm-prod', nodeId: 'rn-b', label: 'demo-main', role: 'secondary', zone: 'cn-chengdu-b'},
    {instanceId: 'rm-prod', nodeId: '', label: 'demo-main'}]};
  const {run, nodes} = context({status});
  nodes['#sessions-instance'] = {innerHTML: '', value: '', options: []};
  nodes['#sessions-node'] = {innerHTML: '', value: ''};
  run('syncSessionControls()');
  assert.equal(nodes['#sessions-instance'].value, 'rm-prod');
  assert.match(nodes['#sessions-instance'].innerHTML, /<option value="rm-test">demo-test<\/option>/);
  const options = [...nodes['#sessions-node'].innerHTML.matchAll(/<option value="([^"]*)">([^<]*)<\/option>/g)].map((m) => [m[1], m[2]]);
  assert.deepEqual(options, [['', '全部节点'], ['rn-p', '主节点 · 可用区 A · rn-p'], ['rn-b', '备节点 · 可用区 B · rn-b']]);
});

test('the request carries the instance, the node and only the thresholds that were filled in; no instance is an error', async () => {
  const {run, nodes, calls} = context({api: async () => payload()});
  nodes['#sessions-instance'] = {value: 'rm-x'};
  nodes['#sessions-node'] = {value: 'rn-secondary0000b'};
  nodes['#sessions-blocking'] = {value: '45'};
  nodes['#sessions-trx'] = {value: ''};
  nodes['#sessions-idle'] = {value: ' 5 '};
  await run('loadSessions()');
  assert.equal(new URL(calls[0][0], 'http://x').pathname, '/api/sessions');
  const params = new URL(calls[0][0], 'http://x').searchParams;
  assert.deepEqual([params.get('instance'), params.get('nodeId'), params.get('blockingSeconds'), params.get('trxSeconds'), params.get('idleTrxSeconds')], ['rm-x', 'rn-secondary0000b', '45', null, '5']);
  assert.match(nodes['#sessions-summary'].innerHTML, /异常会话/);
  assert.match(nodes['#sessions-meta'].textContent, /快照时间/);
  nodes['#sessions-instance'].value = '';
  assert.throws(() => run('sessionsQueryString()'), /请先选择一个 RDS 实例/);
});

test('delegated clicks: tabs, stats rows, SQL expand and the mode switch work from data attributes', () => {
  const {run, c, nodes} = context();
  nodes['#sessions-history'] = {hidden: true, innerHTML: ''};
  c.data = payload();
  run('sessionsState.data = data');
  const target = (attrs) => ({closest: (selector) => (Object.keys(attrs).some((k) => selector.includes(k)) ? {dataset: attrs[Object.keys(attrs).find((k) => selector.includes(k))]} : null)});
  run('sessionsClick').call(null, {target: target({'data-session-tab': {sessionTab: 'all'}})});
  assert.equal(run('sessionsState.tab'), 'all');
  run('sessionsClick')({target: target({'data-session-filter': {sessionFilter: 'database', sessionValue: 'dw'}})});
  assert.equal(run('sessionsState.filters.database'), 'dw');
  run('sessionsClick')({target: target({'data-session-clear': {sessionClear: 'database'}})});
  assert.equal(run('sessionsState.filters.database'), '');
  run('sessionsClick')({target: target({'data-session-sql': {sessionSql: 'rn-primary00000001:44'}})});
  assert.equal(run('sessionsState.expanded.has("rn-primary00000001:44")'), true);
  assert.match(run('sessionListHtml()'), /sql-cell is-open/);
  run('sessionsClick')({target: target({'data-session-sql': {sessionSql: 'rn-primary00000001:44'}})});
  assert.equal(run('sessionsState.expanded.has("rn-primary00000001:44")'), false);
  run('sessionsClick')({target: target({'data-session-mode': {sessionMode: 'history'}})});
  assert.equal(run('sessionsState.mode'), 'history');
});

test('opened before the first status answer: the lists fill and the first read happens when it arrives; later polls leave the dropdowns and the data alone', async () => {
  const status = {primaryInstance: {instanceId: 'rm-prod'}, slowLogs: [
    {instanceId: 'rm-test', nodeId: 'rn-t1', label: 'demo-test', role: 'primary', zone: 'cn-chengdu-a'},
    {instanceId: 'rm-prod', nodeId: 'rn-p', label: 'demo-main', role: 'primary', zone: 'cn-chengdu-a'},
    {instanceId: 'rm-prod', nodeId: 'rn-b', label: 'demo-main', role: 'secondary', zone: 'cn-chengdu-b'}]};
  const retention = {enabled: true, interval_seconds: 60, retention_days: 7, nodes: [], store: {}};
  const {run, nodes, calls} = context({api: async (url) => (url.startsWith('/api/sessions?') ? payload() : retention)});
  nodes['#sessions-instance'] = {innerHTML: '', value: '', options: []};
  nodes['#sessions-node'] = {innerHTML: '', value: ''};
  const reads = () => calls.filter(([url]) => url.startsWith('/api/sessions?'));
  const settle = () => new Promise((resolve) => setImmediate(resolve));

  run('state.view = "sessions"');
  run('initSessionsView()');                                                    // the page is open, the status has not been answered yet
  await settle();
  assert.equal(reads().length, 0);
  assert.equal(nodes['#sessions-instance'].value, '');

  run(`state.status = ${JSON.stringify(status)}; onSessionsStatus()`);          // the first answer
  await settle();
  assert.equal(nodes['#sessions-instance'].value, 'rm-prod');
  assert.match(nodes['#sessions-node'].innerHTML, /rn-p[^]*rn-b/);
  assert.equal(reads().length, 1);
  assert.equal(new URL(reads()[0][0], 'http://x').searchParams.get('instance'), 'rm-prod');

  nodes['#sessions-instance'].value = 'rm-test';                                // the user picks another instance
  run('sessionsChange({target: {id: "sessions-instance"}})');
  assert.match(nodes['#sessions-node'].innerHTML, /rn-t1/);
  assert.doesNotMatch(nodes['#sessions-node'].innerHTML, /rn-p/);

  nodes['#sessions-instance'].innerHTML = 'untouched-instances';                // the poll comes every few seconds: nothing changed, nothing is rewritten
  nodes['#sessions-node'].innerHTML = 'untouched-nodes';
  run('onSessionsStatus()');
  await settle();
  assert.equal(nodes['#sessions-instance'].innerHTML, 'untouched-instances');
  assert.equal(nodes['#sessions-node'].innerHTML, 'untouched-nodes');
  assert.equal(nodes['#sessions-instance'].value, 'rm-test');
  assert.equal(reads().length, 1);                                              // and it does not read again

  nodes['#sessions-node'].value = 'rn-t1';                                      // a node is added: that list is rewritten and the picked node stays picked
  status.slowLogs.push({instanceId: 'rm-test', nodeId: 'rn-t2', label: 'demo-test', role: 'secondary', zone: 'cn-chengdu-b'});
  run(`state.status = ${JSON.stringify(status)}; onSessionsStatus()`);
  assert.match(nodes['#sessions-node'].innerHTML, /rn-t1[^]*rn-t2/);
  assert.equal(nodes['#sessions-node'].value, 'rn-t1');
  assert.equal(nodes['#sessions-instance'].innerHTML, 'untouched-instances');
  assert.equal(reads().length, 1);

  status.slowLogs = status.slowLogs.filter((item) => item.instanceId !== 'rm-test');   // the picked instance disappears: back to the primary instance
  run(`state.status = ${JSON.stringify(status)}; onSessionsStatus()`);
  assert.equal(nodes['#sessions-instance'].value, 'rm-prod');
  assert.equal(nodes['#sessions-node'].value, '');
});

test('status answered while another page is open: the lists are ready and the first visit reads once; the history mode never auto-reads', async () => {
  const status = {primaryInstance: {instanceId: 'rm-prod'}, slowLogs: [{instanceId: 'rm-prod', nodeId: 'rn-p', label: 'demo-main', role: 'primary', zone: 'cn-chengdu-a'}]};
  const retention = {enabled: true, interval_seconds: 60, retention_days: 7, nodes: [], store: {}};
  const make = () => {
    const made = context({api: async (url) => (url.startsWith('/api/sessions?') ? payload() : retention)});
    made.nodes['#sessions-instance'] = {innerHTML: '', value: '', options: []};
    made.nodes['#sessions-node'] = {innerHTML: '', value: ''};
    made.reads = () => made.calls.filter(([url]) => url.startsWith('/api/sessions?'));
    return made;
  };
  const settle = () => new Promise((resolve) => setImmediate(resolve));

  const away = make();
  away.run('state.view = "audit"');
  away.run(`state.status = ${JSON.stringify(status)}; onSessionsStatus()`);
  await settle();
  assert.equal(away.nodes['#sessions-instance'].value, 'rm-prod');
  assert.equal(away.reads().length, 0);                                         // nobody is looking at the page
  away.run('state.view = "sessions"; initSessionsView()');
  await settle();
  assert.equal(away.reads().length, 1);
  away.run('state.view = "audit"; state.view = "sessions"; initSessionsView(); onSessionsStatus()');
  await settle();
  assert.equal(away.reads().length, 1);                                         // coming back to the page does not read again

  const history = make();
  history.run('state.view = "sessions"; initSessionsView(); sessionsState.mode = "history"');
  history.run(`state.status = ${JSON.stringify(status)}; onSessionsStatus()`);
  await settle();
  assert.equal(history.reads().length, 0);
});
