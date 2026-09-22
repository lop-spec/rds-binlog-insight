const { test } = require('node:test');
const assert = require('node:assert/strict');
const { indexedQuickRange } = require('../web/indexed-range.js');
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
  const ctx = vm.createContext({ document, console, URLSearchParams, Date, indexedQuickRange,
    fetch(url) { return new Promise(resolve => requests.push({ url, resolve: data => resolve({ok: true, json: async () => ({ok: true, data})}) })); }
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

test('primary-key mode forbids scan fallback and disables unrelated filters', async () => {
  const {run, node, requests} = uiFixture();
  node('filter-query-mode').value = 'primary-key';
  node('filter-keyword').value = '5917';
  node('filter-account').value = 'old account';
  run('syncQueryMode()');
  assert.equal(node('filter-account').value, '');
  assert.equal(node('filter-account').disabled, true);
  const pending = run('setQuickRange("24h")');
  requests[0].resolve({intervals: [[1_790_000_000_000_000, 1_790_000_010_000_000]], pendingFiles: 0});
  await pending;
  assert.equal(run('eventQueryPayload().exact.fallback'), 'error');
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
