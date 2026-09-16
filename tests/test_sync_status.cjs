const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

function fixture() {
  const nodes = new Map();
  const element = () => ({hidden: false, textContent: '', outerHTML: '', style: {}, children: [],
    set innerHTML(v) { this.html = v; this.children = []; }, get innerHTML() { return this.html || ''; },
    append(v) { this.children.push(v); }});
  const document = {addEventListener() {}, createElement: element, querySelector(s) {
    if (!nodes.has(s)) nodes.set(s, element());
    return nodes.get(s);
  }};
  const c = vm.createContext({console, URLSearchParams, Date, document});
  vm.runInContext(fs.readFileSync(path.join(__dirname, '../web/app.js'), 'utf8'), c);
  c.status = {primaryInstance: {instanceId: 'prod'}, generalLogs: [{instanceId: 'prod', label: 'primary-fixture'}, {instanceId: 'test', label: 'secondary-fixture'}],
    sync: {running: true, latestJob: {id: 'primary-long-running', instance_id: 'prod', kind: 'sync', status: 'running',
      current_file: 'mysql-bin.000673', completed_files: 673, total_files: 9772, message: 'production progress', performance: {state: 'available', seconds_per_file: 88}}}};
  c.jobs = Array.from({length: 50}, (_, i) => ({id: `test-${i}`, instance_id: 'test', kind: 'sync', status: i ? 'success' : 'running',
    current_file: '', completed_files: 0, total_files: 0, message: 'checking test'}));
  vm.runInContext('state.status = status', c);
  return {c, nodes, run: s => vm.runInContext(s, c)};
}

test('primary card survives 50 newer secondary jobs and a late history response', () => {
  const {c, nodes, run} = fixture();
  run('renderPrimarySync(status); renderJobs(jobs)');
  assert.equal(nodes.get('#active-job').hidden, false);
  assert.equal(nodes.get('#active-job-file').textContent, 'primary-fixture · mysql-bin.000673');
  assert.equal(nodes.get('#active-job-count').textContent, '673 / 9772');
  assert.equal(nodes.get('#active-job-message').textContent, 'production progress');
  assert.equal(nodes.get('#active-job-speed').textContent, '88.0 秒 / Binlog');
  c.status.sync.latestJob.current_file = 'mysql-bin.000674';
  run('renderPrimarySync(status); renderJobs(jobs)');
  assert.equal(nodes.get('#active-job-file').textContent, 'primary-fixture · mysql-bin.000674');
});

test('primary latest job is retained in history with instance labels, without duplicates', () => {
  const {c, nodes, run} = fixture();
  run('renderJobs(jobs)');
  let items = nodes.get('#job-list').children;
  assert.equal(items.length, 51);
  assert.match(items[0].innerHTML, /primary-fixture/);
  assert.match(items[1].innerHTML, /secondary-fixture/);
  c.jobs.unshift(c.status.sync.latestJob);
  run('renderJobs(jobs)');
  assert.equal(nodes.get('#job-list').children.length, 51);
  assert.equal(c.jobs.length, 51);
});

test('paused or failed primary retains its file with the correct status', () => {
  const {c, nodes, run} = fixture();
  for (const [status, label] of [['paused', '已暂停'], ['failed', '失败'], ['success', '完成']]) {
    c.status.sync.running = false;
    c.status.sync.latestJob.status = status;
    run('renderPrimarySync(status)');
    assert.equal(nodes.get('#active-job').hidden, false);
    assert.match(nodes.get('#active-job .status-chip').outerHTML, new RegExp(label));
    assert.match(nodes.get('#active-job-file').textContent, /mysql-bin\.000673/);
  }
});

test('empty primary never borrows a running secondary job', () => {
  const {c, nodes, run} = fixture();
  c.status.sync.latestJob = null;
  run('renderPrimarySync(status); renderJobs(jobs)');
  assert.equal(nodes.get('#active-job').hidden, true);
});

test('primary checking state remains explicitly assigned to its instance', () => {
  const {c, nodes, run} = fixture();
  c.status.sync.latestJob.current_file = '';
  run('renderPrimarySync(status)');
  assert.match(nodes.get('#active-job-file').textContent, /^primary-fixture · 核验 RDS 最新 Completed Binlog$/);
});

test('history escapes instance labels and falls back to the actual instance ID', () => {
  const {c, nodes, run} = fixture();
  c.status.generalLogs[0].label = '<img src=x onerror=alert(1)>';
  c.jobs[0].instance_id = 'other-instance';
  run('renderJobs(jobs)');
  const items = nodes.get('#job-list').children;
  assert.match(items[0].innerHTML, /&lt;img/);
  assert.doesNotMatch(items[0].innerHTML, /<img/);
  assert.match(items[1].innerHTML, /other-instance/);
});
