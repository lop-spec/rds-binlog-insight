const { test } = require('node:test');
const assert = require('node:assert/strict');
const { indexedQuickRange, indexedCustomRange, rowsQuickRange, gapsWithin } = require('../web/indexed-range.js');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');

function uiFixture() {
  const nodes = new Map();
  const document = { addEventListener() {}, querySelectorAll() { return []; },
    createElement() { return {dataset: {}, events: {}, addEventListener(name, handler) { this.events[name] = handler; }}; },
    querySelector(key) {
    if (!nodes.has(key)) nodes.set(key, { value: '', textContent: '', disabled: false,
      querySelector: key => document.querySelector(key),
      classList: { toggle() {} }, children: [], append(child) { this.children.push(child); },
      setCustomValidity(message) { this.error = message; } });
    return nodes.get(key);
  }};
  const requests = [];
  const ctx = vm.createContext({ document, console, URLSearchParams, Date, indexedQuickRange, indexedCustomRange, rowsQuickRange, gapsWithin,
    fetch(url, options) { return new Promise(resolve => requests.push({ url, options, resolve: data => resolve({ok: true, json: async () => ({ok: true, data})}) })); }
  });
  vm.runInContext(fs.readFileSync(path.join(__dirname, '../web/app.js'), 'utf8'), ctx);
  const run = code => vm.runInContext(code, ctx);
  const node = key => document.querySelector('#'+key);
  node('filter-query-mode').value = 'indexed-time';
  node('filter-instance').value = 'instance';
  node('filter-database').value = 'db';
  node('filter-table').value = 'table';
  run('syncQueryMode()');
  return {run, node, requests};
}

test('actual row click and keyboard preserve raw and legacy detail locators', () => {
  const {run, node} = uiFixture();
  run(`globalThis.details = []; openDetail = (...args) => details.push(args);
    renderEvents({rows: [
      {event_id: 'raw-id', event_locator: 'raw:file:4', instance_id: 'fixture'},
      {event_id: 'legacy-id', locator: 'legacy-part', instance_id: 'fixture'}]});`);
  for (const row of node('event-rows').children) {
    row.events.click();
    row.events.keydown({key: 'Enter'});
  }
  assert.deepEqual(JSON.parse(run('JSON.stringify(details)')), [
    ['raw-id', 'raw:file:4', 'fixture'], ['raw-id', 'raw:file:4', 'fixture'],
    ['legacy-id', 'legacy-part', 'fixture'], ['legacy-id', 'legacy-part', 'fixture']]);
});

test('fast scope is visible without opening advanced filters', () => {
  const html = fs.readFileSync(path.join(__dirname, '../web/index.html'), 'utf8');
  const visibleForm = html.slice(html.indexOf('<form id="query-form"'), html.indexOf('<details id="audit-filters"'));
  for (const field of ['filter-query-mode', 'filter-instance', 'filter-database', 'filter-table']) {
    assert.ok(visibleForm.includes(`id="${field}"`), `${field} must be outside collapsed filters`);
  }
  const {run, node} = uiFixture();
  assert.equal(node('filter-value-field').hidden, true);
  node('filter-query-mode').value = 'primary-key';
  run('syncQueryMode()');
  assert.equal(node('filter-value-field').hidden, false);
});

test('actual form aligns to index, clips, and submits strict binlog-only fast query', async () => {
  const {run, node, requests} = uiFixture();
  const pending = run('setQuickRange("24h")');
  assert.equal(node('filter-end').value, '');
  requests[0].resolve({intervals: [[1_790_000_000_000_000, 1_790_000_010_000_000]], pendingFiles: 2});
  await pending;
  const payload = run('eventQueryPayload()');
  assert.equal(payload.indexedOnly, true);
  assert.equal(payload.source, 'binlog');
  assert.equal(payload.startEpochUs, 1_790_000_000_000_000);
  assert.equal(payload.endEpochUs, 1_790_000_010_000_000);
  assert.match(node('indexed-range-hint').textContent, /区间不足/);
});

test('no coverage clears stale dates and refuses submission', async () => {
  const {run, node, requests} = uiFixture();
  node('filter-start').value = '2026-09-20T00:00:00';
  const pending = run('setQuickRange("24h")');
  requests[0].resolve({intervals: [], reason: '索引未完成'});
  await pending;
  assert.equal(node('filter-start').value, '');
  assert.equal(node('filter-end').value, '');
  assert.match(node('filter-end').error, /索引未完成/);
  assert.throws(() => run('eventQueryPayload()'), /已索引区间/);
});

test('stale interval response cannot overwrite a newer scope or custom input', async () => {
  const {run, node, requests} = uiFixture();
  const first = run('setQuickRange("24h")');
  const second = run('setQuickRange("1h")');
  const coverage = {intervals: [[1_790_000_000_000_000, 1_790_000_010_000_000]], pendingFiles: 0};
  requests[1].resolve(coverage);
  await second;
  const current = node('filter-end').value;
  requests[0].resolve({intervals: [], reason: 'old request'});
  await first;
  assert.equal(node('filter-end').value, current);
  const third = run('setQuickRange("1h")');
  node('audit-range').value = 'custom';
  node('filter-end').value = '2026-09-19T01:02:03';
  requests[2].resolve(coverage);
  await third;
  assert.equal(node('filter-end').value, '2026-09-19T01:02:03');
});

test('primary-key mode forbids scan fallback and removes unavailable audit filters visibly', async () => {
  const {run, node, requests} = uiFixture();
  node('filter-query-mode').value = 'primary-key';
  node('filter-keyword').value = '5917';
  node('filter-account').value = 'old account';
  run('syncQueryMode()');
  assert.equal(node('filter-account').value, '');
  assert.equal(node('filter-account').disabled, true);
  assert.equal(node('filter-account-field').hidden, true);
  assert.equal(node('binlog-query-policy').hidden, false);
  const pending = run('setQuickRange("24h")');
  requests[0].resolve({intervals: [[1_790_000_000_000_000, 1_790_000_010_000_000]], pendingFiles: 0});
  await pending;
  assert.equal(run('eventQueryPayload().exact.fallback'), 'error');
});

test('keyword query keeps the keyword but clears unsupported audit filters in form and request', async () => {
  const {run, node, requests} = uiFixture();
  node('filter-query-mode').value = 'keyword';
  node('filter-keyword').value = '157683';
  node('filter-status').value = 'success';
  run('syncQueryMode()');
  const pending = run('setQuickRange("24h")');
  requests[0].resolve({intervals: [[1_790_000_000_000_000, 1_790_000_010_000_000]], pendingFiles: 0});
  await pending;
  const payload = run('eventQueryPayload()');
  assert.equal(payload.indexedOnly, true);
  assert.equal(payload.source, 'binlog');
  assert.equal(payload.keyword, '157683');
  assert.equal(payload.status, '');
  assert.equal(node('filter-status').value, '');
  assert.equal(node('filter-status').disabled, true);
  assert.equal(node('filter-status-field').hidden, true);
  assert.equal(node('filter-keyword-mode').disabled, false);
  assert.equal(node('filter-value-field').hidden, false);
  const html = fs.readFileSync(path.join(__dirname, '../web/index.html'), 'utf8');
  assert.ok(!html.includes('可能扫描原档'));
});

test('other source keyword searches retain their own query route', () => {
  const {run, node} = uiFixture();
  node('filter-query-mode').value = 'keyword';
  node('filter-source').value = 'slowlog';
  node('filter-keyword').value = 'SELECT';
  node('filter-status').value = 'success';
  node('filter-account').value = 'reader';
  node('filter-connection').value = 'prod';
  run('syncQueryMode()');
  const payload = run('eventQueryPayload()');
  assert.equal(payload.source, 'slowlog');
  assert.equal(payload.keyword, 'SELECT');
  assert.equal(payload.indexedOnly, undefined);
  assert.equal(payload.status, 'success');
  assert.equal(payload.account, 'reader');
  assert.equal(payload.connection, 'prod');
  assert.equal(node('filter-status').disabled, false);
  assert.equal(node('filter-status-field').hidden, false);
  assert.equal(node('binlog-query-policy').hidden, true);
});

test('custom ranges select one intersecting segment, never cross holes or shift dates', () => {
  const intervals = [[1_000_001, 5_999_999], [10_000_000, 20_000_000]];
  assert.deepEqual(indexedCustomRange(intervals, 1000, 15000), {start: 10000, end: 15000, clipped: true});
  assert.deepEqual(indexedCustomRange(intervals, 3000, 4000), {start: 3000, end: 4000, clipped: false});
  assert.deepEqual(indexedCustomRange(intervals, 1000, 9000), {start: 2000, end: 5000, clipped: true});
  assert.deepEqual(indexedCustomRange(intervals, 10000, 10000), {start: 10000, end: 10000, clipped: false});
  assert.equal(indexedCustomRange(intervals, 6000, 9000), null);
  assert.equal(indexedCustomRange(intervals, NaN, 9000), null);
  assert.equal(indexedCustomRange(intervals, 9000, 1000), null);
});

function customForm(fixture) {
  const {run, node} = fixture;
  node('filter-query-mode').value = 'keyword';
  node('filter-keyword').value = '157683';
  node('filter-status').value = 'success';
  node('filter-account').value = 'stale account';
  node('filter-connection').value = 'stale connection';
  node('audit-range').value = 'custom';
  node('filter-start').value = run('toLocalInput(new Date(1789006589000))');
  node('filter-end').value = run('toLocalInput(new Date(1789956989000))');
}

test('actual submit converts the broad custom form before POST and keeps the keyword', async () => {
  const fixture = uiFixture();
  const {run, node, requests} = fixture;
  customForm(fixture);
  run('refreshQueryTasks = async () => {}; state.status = {summary: {latestEpochUs: 1}};');
  const pending = run('runQuery()');
  assert.match(requests[0].url, /^\/api\/index-coverage\?/);
  requests[0].resolve({intervals: [[1789956949000000, 1789956986999999]], pendingFiles: 100});
  await new Promise(resolve => setImmediate(resolve));
  assert.equal(requests[1].url, '/api/query-tasks');
  const payload = JSON.parse(requests[1].options.body);
  assert.equal(payload.startEpochUs, 1789956949000000);
  assert.equal(payload.endEpochUs, 1789956986000000);
  assert.equal(payload.keyword, '157683');
  assert.equal(payload.exact, undefined);
  assert.equal(payload.source, 'binlog');
  assert.equal(payload.indexedOnly, true);
  for (const key of ['status', 'account', 'connection']) {
    assert.equal(payload[key], '');
    assert.equal(node('filter-' + key).value, '');
    assert.equal(node('filter-' + key).disabled, true);
  }
  assert.equal(new Date(node('filter-start').value).getTime() * 1000, payload.startEpochUs);
  assert.equal(new Date(node('filter-end').value).getTime() * 1000, payload.endEpochUs);
  assert.match(node('indexed-range-hint').textContent, /原选.*已收窄.*实际查询.*未检索/);
  requests[1].resolve({taskId: 'fixture-task'});
  await pending;
});

test('custom submit without intersecting coverage creates no task and keeps requested dates', async () => {
  const fixture = uiFixture();
  const {run, node, requests} = fixture;
  customForm(fixture);
  const before = node('filter-start').value;
  const pending = run('runQuery()');
  const rejected = assert.rejects(pending, /没有完整可查区间/);
  requests[0].resolve({intervals: [[1000000, 9000000]], pendingFiles: 100});
  await rejected;
  assert.equal(requests.length, 1);
  assert.equal(node('filter-start').value, before);
});

test('custom coverage cannot overwrite edited scope or dates', async () => {
  for (const field of ['database', 'start']) {
    const fixture = uiFixture();
    const {run, node, requests} = fixture;
    customForm(fixture);
    const pending = run('alignCustomIndexedRange()');
    const rejected = assert.rejects(pending, /条件已变化/);
    node('filter-' + field).value = 'changed';
    requests[0].resolve({intervals: [[1789956949000000, 1789956986999999]]});
    await rejected;
    assert.equal(node('filter-' + field).value, 'changed');
  }
});

test('GET serialization also clears restored unsupported filters, not only POST', () => {
  const fixture = uiFixture();
  customForm(fixture);
  const params = new URLSearchParams(fixture.run('eventQueryString()'));
  for (const key of ['status', 'account', 'connection']) assert.equal(params.has(key), false);
  assert.equal(params.get('keyword'), '157683');
});

test('empty indexed results identify the executed interval, not all history', () => {
  const {run, node} = uiFixture();
  run(`state.activeQuery = {start_epoch_us: 1789956949000000, end_epoch_us: 1789956986000000};
    renderEvents({rows: [], tiers_used: ['raw-event-index']});`);
  assert.match(node('result-meta').textContent, /0 条.*仅查询.*其他时间未检索/);
  assert.ok(node('result-meta').textContent.includes(run('formatTime(1789956949000000)')));
  assert.equal(node('event-empty strong').textContent, '所查区间没有匹配记录');
  assert.match(node('event-empty span').textContent, /其他历史尚未查询/);
  assert.ok(!node('event-empty span').textContent.includes('同步'));
});

test('empty coverage never substitutes wall clock', () => {
  assert.equal(indexedQuickRange([], 3600000), null);
});
test('latest continuous interval wins, gaps are not bridged', () => {
  assert.deepEqual(indexedQuickRange([[1000000, 9000000], [20000000, 30000000]], 60000),
    { start: 20000, end: 30000, clipped: true });
});
test('rounding stays inside certified interval', () => {
  assert.deepEqual(indexedQuickRange([[1000001, 5999999]], 1000),
    { start: 4000, end: 5000, clipped: false });
});
test('too short subsecond interval is not rounded outside coverage', () => {
  assert.equal(indexedQuickRange([[1000001, 1999999]], 60000), null);
});
test('duration is preserved when the latest interval is long enough', () => {
  assert.deepEqual(indexedQuickRange([[1000000, 3601000000]], 60000),
    { start: 3541000, end: 3601000, clipped: false });
});

test('rows quick range keeps the requested duration and ends at the newest ingested second', () => {
  const hour = 3600_000;
  const range = rowsQuickRange([[1_000_000_000_000_000, 1_000_000_500_000_000], [1_000_001_000_000_000, 1_000_002_000_123_456]], hour);
  assert.deepEqual(range, { start: 1_000_002_000_000 - hour, end: 1_000_002_000_000 });
  assert.equal(rowsQuickRange([], hour), null);
  assert.equal(rowsQuickRange([[1, 2]], 0), null);
});

test('gaps within a range are counted per reason and never outside the range', () => {
  const gaps = [
    { start: 10_000_000, end: 20_000_000, reason: 'source_missing' },
    { start: 30_000_000, end: 40_000_000, reason: 'pending' },
    { start: 90_000_000, end: 95_000_000, reason: 'pending' },
  ];
  assert.deepEqual(gapsWithin(gaps, 15_000, 35_000), { total: 2, counts: { source_missing: 1, pending: 1 } });
  assert.deepEqual(gapsWithin(gaps, 50_000, 60_000), { total: 0, counts: {} });
});
