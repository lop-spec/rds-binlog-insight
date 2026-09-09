const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm');
const source=fs.readFileSync('web/mongo.js','utf8');
const escape=s=>String(s).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;');
function context(){return vm.createContext({console,escapeHtml:escape,URLSearchParams,Date,Number,Math});}
test('Mongo source is inside existing workspace, not a second application',()=>{
 const html=fs.readFileSync('web/index.html','utf8'),app=fs.readFileSync('web/app.js','utf8');
 assert.match(html,/value="mongodb"/);assert.match(html,/id="mongo-kind"/);assert.match(html,/id="mongo-baseline"/);
 assert.match(app,/return runMongoAnalytics\(\)/);assert.match(html,/assets\/mongo.js/);
});
test('missing values are not rendered as zero',()=>{
 const c=context();vm.runInContext(source,c);assert.equal(vm.runInContext('mongoNumber(null)',c),'—');
});
test('performance curve does not bridge missing minutes',()=>{
 const c=context();vm.runInContext(source,c);
 const html=vm.runInContext(`mongoSparkline([{timestamp:0,value:1,metric:'CPUUtilization'},{timestamp:60000,value:2,metric:'CPUUtilization'},{timestamp:240000,value:3,metric:'CPUUtilization'}])`,c);
 assert.equal((html.match(/<polyline/g)||[]).length,2);
});
test('no causal claim or fabricated full collection counts',()=>{
 assert.match(source,/相关与|因果/);assert.match(source,/尚无应用命令聚合接入/);
 assert.match(source,/不是慢日志计数/);assert.match(source,/未采集原生命令计数/);
 assert.match(source,/escapeHtml\(x.namespace\)/);assert.match(source,/字段覆盖/);
});
