"use strict";

/* 上涨排查（RDS）：选一个时间窗和一个主要指标，看窗口内哪些 SQL 的执行时间与该指标曲线最相关，
   以及它们比昨日同窗多花了多少。排序固定为相关度（带符号皮尔逊 r，从高到低），与 MongoDB 工作台同一规则；
   相关只是时序关联，不是对资源消耗的度量，也不是因果。 */

const RISE_METRICS = [
  ["cpu", "CPU 使用率 %"],
  ["iops", "IOPS 使用率 %"],
  ["rows_read", "InnoDB 读取行数 / 秒"],
  ["row_lock", "行锁等待 ms / 秒"],
  ["threads", "活跃线程数"],
];

const RISE_STATUS = {
  unsupported_metric: "不支持该指标",
  no_collected_executions: "该窗口没有采集到慢 SQL 执行记录",
  metric_or_input_unavailable: "云监控指标或输入校验失败，未计算（详见服务日志）",
  insufficient_complete_periods: "窗口不足 6 个完整分钟，不计算相关度",
  no_metric_or_execution_data: "没有指标或执行数据",
};

const RISE_CORRELATION_REASON = {
  insufficient_buckets: "分钟数不足",
  constant_x: "该 SQL 每分钟耗时恒定，无法计算",
  constant_y: "指标在窗口内恒定，无法计算",
  constant_series: "序列恒定，无法计算",
};

function riseMessage(status) {
  return RISE_STATUS[status] || (typeof RESOURCE_STATUS === "object" && RESOURCE_STATUS[status]) || `不可用：${status}`;
}

function initRdsControls() {
  const select = $("#rds-metric");
  if (select && !select.options.length) {
    select.innerHTML = RISE_METRICS.map(([id, label]) => `<option value="${id}">${escapeHtml(label)}</option>`).join("");
  }
}

function syncRdsControls(enabled) {
  const box = $("#rds-controls");
  if (box) box.hidden = !enabled;
  if (!enabled) return;
  initRdsControls();
  ensureRiseInstance();
}

// 上涨排查需要单个实例（云监控指标按实例取）：没选时落到主实例。状态加载完成前选不到，所以提交时会再试一次。
function ensureRiseInstance() {
  const select = $("#analytics-instance");
  const primary = state.status?.primaryInstance?.instanceId;
  if (select && !select.value && primary && [...(select.options || [])].some((option) => option.value === primary)) {
    select.value = primary;
  }
  return select?.value || "";
}

function riseQueryString() {
  if (!ensureRiseInstance()) throw new Error("请先选择一个 RDS 实例再排查上涨");
  const params = new URLSearchParams(analyticsQueryString("", "slowlog"));
  params.set("metric", $("#rds-metric").value || "cpu");
  params.set("order", "correlation");
  return params.toString();
}

function riseMetricLabel(id) {
  return (RISE_METRICS.find(([key]) => key === id) || [id, id])[1];
}

function riseNumber(value, digits = 1) {
  const number = Number(value);
  if (!Number.isFinite(number)) return "—";
  return number.toLocaleString("zh-CN", { maximumFractionDigits: digits });
}

function riseSigned(value, formatter = riseNumber) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return "—";
  const number = Number(value);
  return `${number > 0 ? "+" : number < 0 ? "−" : ""}${formatter(Math.abs(number))}`;
}

function riseCorrelation(row) {
  if (row.resource_r === null || row.resource_r === undefined) {
    const reason = RISE_CORRELATION_REASON[row.correlation_status] || row.correlation_status || "无法计算";
    return `<span class="muted" title="${escapeHtml(reason)}">—</span>`;
  }
  const value = Number(row.resource_r);
  const level = value >= 0.8 ? "strong" : value >= 0.5 ? "medium" : value > 0 ? "weak" : "negative";
  return `<strong class="rise-r is-${level}" title="相关度 r：该 SQL 每分钟执行耗时与所选指标的皮尔逊相关系数；相关不等于因果">${riseSigned(value, (v) => v.toFixed(2))}</strong>`;
}

function riseCost(row) {
  const now = Number(row.runtime_us_total || 0);
  const before = Number(row.baseline_runtime_us_total || 0);
  const delta = now - before;
  const percent = before > 0 ? ` <span class="muted">${riseSigned((delta / before) * 100, (v) => `${riseNumber(v, 0)}%`)}</span>` : (now > 0 ? ' <span class="chip warn">昨日同窗无</span>' : "");
  return `<strong>${riseSigned(delta, humanMicros)}</strong>${percent}<br><span class="muted">${humanMicros(before)} → ${humanMicros(now)}</span>`;
}

function riseWork(row) {
  const attribution = row.attribution || {};
  const scan = attribution.rows_examined || {};
  const lock = attribution.lock_time_ms || {};
  const scanText = scan.delta === null || scan.delta === undefined ? '<span class="muted" title="部分执行未上报该字段，不按 0 计算">扫描行 —</span>'
    : `扫描行 ${riseSigned(scan.delta, (v) => humanCount(v))}`;
  const lockText = lock.delta ? `<br>锁等待 ${riseSigned(lock.delta, (v) => `${humanCount(v)} ms`)}` : "";
  return `${scanText}${lockText}`;
}

function riseStatement(node, row, position) {
  const sql = row.normalized_sql || row.sample_sql || row.sql_id || row.fingerprint;
  const growing = Number(row.runtime_delta_us || 0) > 0;
  const minutes = (row.runtime_us || []).length;
  return `<tr class="${growing ? "" : "is-flat"}">
    <td class="rise-rank">${position}</td>
    <td><button class="slow-sql-link" type="button" data-rise-node="${escapeHtml(node.node_id)}" data-rise-fp="${escapeHtml(row.fingerprint)}" title="查看样本执行与逐分钟序列"><code class="sql-cell">${escapeHtml(sql)}</code></button></td>
    <td>${riseCorrelation(row)}<br><span class="muted" title="窗口内有该 SQL 执行的分钟数">${humanCount(row.active_minutes)} / ${humanCount(minutes)} 分钟有执行</span></td>
    <td>${riseCost(row)}</td>
    <td>${humanCount(row.attribution?.baseline_count)} → <strong>${humanCount(row.executions)}</strong></td>
    <td>${riseWork(row)}</td>
  </tr>`;
}

function riseNodeSpark(data, node) {
  const scale = Number(node.metric_scale) || 1;
  const times = data.sample_end_us || [];
  const points = (node.metric_values || []).map((value, index) => ({ ts: times[index], value: Number(value) / scale }));
  return sparkline(points, { valueKey: "value" });
}

function riseNodeSummary(data, node) {
  const comparison = node.resource_comparison || {};
  const label = riseMetricLabel(data.metric_id);
  const change = comparison.delta === null || comparison.delta === undefined
    ? "昨日同窗指标不完整，不比较"
    : `昨日同窗 ${riseNumber(comparison.baseline_mean, 2)}，${riseSigned(comparison.delta, (v) => riseNumber(v, 2))}`;
  return `<strong>${escapeHtml(node.node_id || "未知节点")}</strong> · ${escapeHtml(label)}：窗口均值 <strong>${riseNumber(comparison.current_mean, 2)}</strong>（${change}）· 峰值 ${riseNumber(node.peak, 2)}`;
}

function riseNodeBody(data, node) {
  if (node.status === "metric_gaps") {
    return `<p class="analytics-unavailable">该节点云监控指标缺点（${humanCount(node.actual_points)} / ${humanCount(node.expected_points)}），不补零、不排名。</p>`;
  }
  const rows = node.statements || [];
  if (!rows.length) return '<p class="analytics-empty">窗口内该节点没有采集到慢 SQL。</p>';
  const note = node.status && node.status !== "ok" && RESOURCE_STATUS[node.status]
    ? `<p class="analytics-note">${escapeHtml(RESOURCE_STATUS[node.status])}；下表按相关度列出，成本增量仅供核对。</p>` : "";
  return `${riseNodeSpark(data, node)}${note}
    <div class="table-wrap"><table class="data-table analytics-table rise-table">
      <thead><tr><th>#</th><th>SQL（按相关度 r 从高到低）</th><th>相关度 r</th><th>耗时增量（昨日同窗 → 窗口）</th><th>执行次数（昨日 → 窗口）</th><th>扫描 / 锁等待增量</th></tr></thead>
      <tbody>${rows.map((row, index) => riseStatement(node, row, index + 1)).join("")}</tbody>
    </table></div>
    <p class="analytics-note">共 ${humanCount(node.total_fingerprints)} 个 SQL 家族，显示相关度最高的 ${humanCount(rows.length)} 个。灰显行的耗时没有比昨日同窗增加。</p>`;
}

function renderRise(data) {
  const panel = $("#analytics-panel-rise");
  $("#analytics-empty").hidden = true;
  const metric = riseMetricLabel(data.metric_id);
  const nodes = [...(data.nodes || [])].sort((a, b) => {
    const rise = (node) => (node.resource_comparison?.delta ?? -Infinity);
    return rise(b) - rise(a) || Number(b.peak || 0) - Number(a.peak || 0) || String(a.node_id).localeCompare(String(b.node_id));
  });
  if (!nodes.length) {
    panel.innerHTML = `<p class="analytics-unavailable"><strong>无法排查。</strong>${escapeHtml(riseMessage(data.status))}</p>`;
    return;
  }
  const many = nodes.length > 3;
  panel.innerHTML = `
    <p class="analytics-note">${escapeHtml(metric)}：窗口 ${escapeHtml(formatTime(data.start_us))} → ${escapeHtml(formatTime(data.end_us))}，对照昨日同窗；SQL 只统计已采集的慢日志（含等待时间），相关度是时序关联，不是资源消耗的度量，也不是因果。</p>
    ${nodes.map((node, index) => `<details class="rise-node" ${!many || index === 0 ? "open" : ""}>
      <summary>${riseNodeSummary(data, node)}</summary>${riseNodeBody(data, node)}</details>`).join("")}`;
}

function renderRiseCoverage(data) {
  const node = $("#analytics-coverage");
  const cover = (label, coverage) => coverage
    ? `${label} ${humanCount(coverage.covered_parts ?? coverage.total_parts)}/${humanCount(coverage.total_parts)} 个分区${coverage.complete ? "" : "（未完整）"}` : "";
  const complete = data.coverage?.complete !== false && data.baseline_coverage?.complete !== false;
  node.className = `notice ${complete ? "info" : "warning"} compact-notice`;
  const text = [cover("慢日志", data.coverage), cover("对照日", data.baseline_coverage),
    `${humanCount(data.executions)} 条执行 / 昨日 ${humanCount(data.baseline_executions)} 条`].filter(Boolean).join(" · ");
  node.innerHTML = `<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M12 11v6M12 7h.01"/></svg><span>${escapeHtml(text)}</span>`;
}

async function runRise() {
  const query = riseQueryString();
  const data = await api(`/api/slowlog-impact?${query}`);
  state.rise = { data, query };
  state.analyticsRuns = {}; // 明细视图按新的窗口重新加载
  state.analyticsRunsQuery = "";
  renderRise(data);
  renderRiseCoverage(data);
  const scope = [$("#analytics-source").selectedOptions[0].textContent, $("#analytics-instance").selectedOptions[0].textContent,
    riseMetricLabel(data.metric_id), `${$("#analytics-start").value.replace("T", " ")} → ${$("#analytics-end").value.replace("T", " ")}`]
    .filter(Boolean).join(" · ");
  $("#analytics-meta").textContent = scope;
  switchAnalyticsTab(state.rdsTab === "rise" || !state.rdsTab ? "rise" : state.rdsTab);
  if (!data.nodes?.length) toast(`无法排查：${riseMessage(data.status)}`, "error", 6000);
  else toast("排查完成", "success");
}

function riseDetailSeries(data, node, row) {
  const scale = Number(node.metric_scale) || 1;
  const times = data.sample_end_us || [];
  return times.map((end, index) => [
    escapeHtml(formatTime(end)),
    riseNumber((node.metric_values || [])[index] / scale, 2),
    humanMicros((row.runtime_us || [])[index] || 0),
  ]);
}

async function openRiseDetail(nodeId, fingerprint) {
  const data = state.rise?.data;
  const node = (data?.nodes || []).find((item) => item.node_id === nodeId);
  const row = (node?.statements || []).find((item) => item.fingerprint === fingerprint);
  if (!row) return;
  const opened = row.sample_event_id ? await openDetail(row.sample_event_id, "", data.instance_id || "") : false;
  if (!opened) {
    $("#detail-title").textContent = "SQL 明细";
    $("#detail-body").innerHTML = '<p class="analytics-note">原始执行样本不可用。</p>';
    showDetailDrawer();
  }
  const members = (row.member_fingerprints || []).length;
  $("#detail-body").insertAdjacentHTML("afterbegin", `<section class="detail-block"><h3>排查依据</h3>
    <p>${riseCorrelation(row)} · ${escapeHtml(riseMetricLabel(data.metric_id))} · 节点 ${escapeHtml(node.node_id)}</p>
    <p>耗时 ${humanMicros(row.baseline_runtime_us_total)} → ${humanMicros(row.runtime_us_total)}；执行 ${humanCount(row.attribution?.baseline_count)} → ${humanCount(row.executions)} 次${members > 1 ? `；合并 ${members} 个物理指纹（分表族）` : ""}。</p>
    ${analyticsTable(["分钟结束", `${riseMetricLabel(data.metric_id)}`, "该 SQL 当分钟执行耗时"], riseDetailSeries(data, node, row))}
    <p class="analytics-note">相关度是这两列的皮尔逊系数，只说明时间上同涨同落，不证明因果，也不衡量该 SQL 消耗了多少资源。</p></section>`);
}
