const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
function context() {
  const nodes = {};
  const node = (selector) => (nodes[selector] ??= {innerHTML: ''});
  const c = vm.createContext({console, URLSearchParams, Date, document: {addEventListener() {}, querySelector: node}});
  for (const file of ['workspace.js','app.js']) vm.runInContext(fs.readFileSync(path.join(__dirname,'../web',file),'utf8'), c);
  return {c, nodes};
}

test('Pod history uncertainty is explicit and metadata escaped', () => {
  const {c} = context();
  c.pod = {status:'historical_binding_unverified',pods:[{name:'<script>bad</script>',namespace:'ns',uid:'u',cluster:'c'}]};
  const html = vm.runInContext('podDetail(pod)',c);
  assert.match(html,/历史绑定未证实/);
  assert.match(html,/Pod UID/);
  assert.doesNotMatch(html,/<script>/);
  assert.match(vm.runInContext('podDetail()',c),/未接入生产 Pod 元数据/);
});

test('every resource-analysis status has a reason the page can show', () => {
  const {c} = context();
  for (const status of ['incomplete_baseline_index','baseline_outside_retention','no_growth_overlap','incomplete_index','single_instance_required',
    'overlapping_baseline','metric_gaps','no_excess_overlap','metric_or_input_unavailable','insufficient_complete_periods']) {
    assert.ok(vm.runInContext(`RESOURCE_STATUS[${JSON.stringify(status)}]`, c), status);
  }
});

test('node options name the database role and zone, primary first, and stay in a stable order', () => {
  const {c, nodes} = context();
  c.slowLogs = [
    {instanceId: 'rm-prod', nodeId: 'rn-b', label: 'demo-main', role: 'secondary', zone: 'cn-chengdu-b'},
    {instanceId: 'rm-prod', nodeId: 'rn-a', label: 'demo-main', role: 'secondary', zone: 'cn-chengdu-a'},
    {instanceId: 'rm-prod', nodeId: 'rn-p', label: 'demo-main', role: 'primary', zone: 'cn-chengdu-a'},
    {instanceId: 'rm-test', nodeId: 'rn-t', label: 'demo-test', role: '', zone: ''},
    {instanceId: 'rm-prod', nodeId: 'rn-p', label: 'demo-main', role: 'primary', zone: 'cn-chengdu-a'},   // listed twice: one option
  ];
  vm.runInContext('syncSlowLogNodeOptions(slowLogs)', c);
  const options = [...nodes['#analytics-node-options'].innerHTML.matchAll(/<option value="([^"]+)" label="([^"]+)"><\/option>/g)].map((m) => [m[1], m[2]]);
  assert.deepEqual(options, [
    ['rn-p', '主节点 · 可用区 A · demo-main'],
    ['rn-a', '备节点 · 可用区 A · demo-main'],
    ['rn-b', '备节点 · 可用区 B · demo-main'],
    ['rn-t', '角色未知 · demo-test'],
  ]);
  const again = nodes['#analytics-node-options'].innerHTML;
  c.slowLogs.reverse();
  vm.runInContext('syncSlowLogNodeOptions(slowLogs)', c);
  assert.equal(nodes['#analytics-node-options'].innerHTML, again, 'the status refresh every 3 s must not reshuffle the list');
});

test('role and zone labels escape what the server sends', () => {
  const {c, nodes} = context();
  c.slowLogs = [{instanceId: 'i', nodeId: 'rn-x', label: '<img onerror=alert(1)>', role: '<b>', zone: ''}];
  vm.runInContext('syncSlowLogNodeOptions(slowLogs)', c);
  assert.doesNotMatch(nodes['#analytics-node-options'].innerHTML, /<img|<b>/);
  assert.equal(vm.runInContext('nodeRoleText("primary")', c), '主节点');
  assert.equal(vm.runInContext('nodeRoleText("")', c), '角色未知');
  assert.equal(vm.runInContext('nodeZoneText("cn-chengdu-c")', c), '可用区 C');
  assert.equal(vm.runInContext('nodeZoneText("")', c), '');
});
