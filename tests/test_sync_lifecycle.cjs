const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const app = fs.readFileSync(path.join(__dirname, '../web/app.js'), 'utf8');
const html = fs.readFileSync(path.join(__dirname, '../web/index.html'), 'utf8');
function context(data) {
  const nodes = new Map();
  const c = vm.createContext({console, Date, URLSearchParams, fixture: data, document: {
    addEventListener() {},
    querySelector(id) {
      if (!nodes.has(id)) nodes.set(id, {textContent:'', innerHTML:'', style:{}});
      return nodes.get(id);
    },
  }});
  vm.runInContext(app, c);
  vm.runInContext('api=async()=>fixture; syncInstanceOptions=()=>{}; syncSlowLogNodeOptions=()=>{}; renderSyncPerformance=()=>{}; state.view="overview";', c);
  return {c, nodes};
}
for (const [state, label] of [['disabled','自动同步已关闭'], ['paused','手动暂停未恢复'], ['maintenance','维护暂停中'], ['stalled','同步停滞'], ['scheduler_error','自动采集启动失败']]) {
  test(`collection state ${state} overrides a stale caught-up label`, async () => {
    const {c, nodes} = context({configured:true, sync:{running:false, health:{ok:false,state,message:'fixture-reason'}, latestJob:{status:'success',performance:{state:'caught_up'}}}});
    await vm.runInContext('refreshStatus()', c);
    assert.equal(nodes.get('#summary-sync').textContent, label);
    assert.match(nodes.get('#service-meta').textContent, /fixture-reason/);
    assert.equal(nodes.get('#start-sync').disabled, false);
  });
}
test('secondary pause is visible and does not display stale caught-up success', () => {
  const {c, nodes} = context({});
  vm.runInContext('renderSecondarySyncs([{instanceId:"fixture",sync:{health:{ok:false,message:"手动暂停未恢复"},latestJob:{performance:{state:"caught_up"}}}}])', c);
  assert.match(nodes.get('#secondary-list').innerHTML, /手动暂停未恢复/);
  assert.doesNotMatch(nodes.get('#secondary-list').innerHTML, /已追平/);
});
test('maintenance and indefinite controls are distinct and setting disable requires explicit confirmation', () => {
  assert.match(html, /id="maintenance-sync"/);
  assert.match(html, /手动暂停（不自动恢复）/);
  assert.match(app, /resumeAfterSeconds: 900/);
  const save = app.slice(app.indexOf('async function saveSettings'), app.indexOf('async function downloadExport'));
  assert.match(save, /window\.confirm/);
  assert.match(save, /confirmDisableAutoSync = true/);
  assert.match(save, /已取消关闭自动同步；设置未更改/);
});
