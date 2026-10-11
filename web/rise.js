"use strict";

/* 上涨排查（RDS）：选一个时间窗和一个主要指标，看窗口内哪些慢 SQL 比昨日同窗多花了成本、并与该指标曲线重合。
   排序是服务端的「扫描增量证据优先，再按增长份额」——原「对齐节点 IOPS（性能关联）」，现为默认；相关度 r 只是时序关联的
   核对列，不是排序依据，也不是资源消耗的度量或因果。
   证据来源可勾选：慢日志（上涨排查）与 Binlog 写入（写入 SQL / 事务 / 锁争用推断），只请求勾选的。 */

const RISE_METRICS = [
  ["cpu", "CPU 使用率 %"],
  ["iops", "IOPS 使用率 %"],
  ["rows_read", "InnoDB 读取行数 / 秒"],
  ["row_lock", "行锁等待 ms / 秒"],
  ["threads", "活跃线程数"],
];

const RISE_STATUS = {
  incomplete_index: "所选时段及前后 6 小时内没有索引完整的同长度时段（索引仍在追平，或该实例暂无采集）",
  incomplete_baseline_index: "所选时段的昨日对照日索引不完整，前后 6 小时内也没有可用的时段",
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

// 上涨排查要和昨日同窗不重叠，窗口必须小于 24 小时；留 5 分钟余量，取整后也不会碰线。
const RISE_MAX_WINDOW_MS = 24 * 3_600_000 - 300_000;

function riseMessage(status) {
  return RISE_STATUS[status] || (typeof RESOURCE_STATUS === "object" && RESOURCE_STATUS[status]) || `不可用：${status}`;
}

function initRdsControls() {
  const select = $("#rds-metric");
  if (select && !select.options.length) {
    select.innerHTML = RISE_METRICS.map(([id, label]) => `<option value="${id}">${escapeHtml(label)}</option>`).join("");
  }
}

// 证据来源勾选：缺少勾选框（旧页面、测试桩）按已勾选处理。
function riseUses(kind) {
  const box = $(kind === "slowlog" ? "#analytics-use-slowlog" : "#analytics-use-binlog");
  return !box || box.checked !== false;
}

function syncRdsControls(enabled) {
  const sources = $("#rds-sources");
  if (sources) sources.hidden = !enabled;
  const box = $("#rds-controls");
  if (box) box.hidden = !enabled;
  if (!enabled) return;
  initRdsControls();
  ensureRiseInstance();
  syncAnalyticsSources();
}

// 性能指标和 Node ID 只属于慢日志；页签跟着勾选走。
function syncAnalyticsSources() {
  const slowlog = riseUses("slowlog");
  const metric = $("#rds-controls");
  if (metric) metric.hidden = !slowlog;
  const nodeField = $("#analytics-node-field");
  const nodeInput = $("#analytics-node");
  if (nodeField) nodeField.hidden = !slowlog;
  if (nodeInput) nodeInput.disabled = !slowlog;
  syncAnalyticsTabs();
}

function syncAnalyticsTabs() {
  const slowlog = riseUses("slowlog");
  const binlog = riseUses("binlog");
  const open = $("#analytics-more-views")?.getAttribute("aria-expanded") === "true";
  const wanted = { rise: slowlog, writes: binlog, sql: slowlog && open, transactions: binlog && open, locks: binlog && open };
  $$("[data-analytics-tab]").forEach((button) => { button.hidden = !wanted[button.dataset.analyticsTab]; });
  const current = state.analyticsTab || "rise";
  if (!wanted[current] && $("#analytics-source").value !== "mongodb") switchAnalyticsTab(slowlog ? "rise" : "writes");
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
  params.set("order", "growth");
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
  return `<strong class="rise-r is-${level}" title="相关度 r：该 SQL 每分钟执行耗时与所选指标的皮尔逊相关系数，只是核对列；相关不等于因果">${riseSigned(value, (v) => v.toFixed(2))}</strong>`;
}

// 增长份额：该 SQL 在本节点「超额指标 × 耗时增长」里占的比例，份额不是资源贡献率。
function riseShare(row) {
  if (row.growth_share === null || row.growth_share === undefined) {
    return '<span class="muted" title="没有相比昨日同窗的正向耗时增长">—</span>';
  }
  const percent = Number(row.growth_share) * 100;
  const text = percent >= 10 ? riseNumber(percent, 0) : percent >= 1 ? riseNumber(percent, 1) : riseNumber(percent, 2);
  return `<strong title="增长份额：每分钟「执行耗时 × 高于窗口 P20 基线的指标超额」乘以相比昨日同窗的正向耗时增长比例，在本节点内的占比；不是资源贡献率">${text}%</strong>`;
}

function riseCost(row) {
  const now = Number(row.runtime_us_total || 0);
  const before = Number(row.baseline_runtime_us_total || 0);
  const delta = now - before;
  const percent = before > 0 ? ` <span class="muted">${riseSigned((delta / before) * 100, (v) => `${riseNumber(v, 0)}%`)}</span>` : (now > 0 ? ' <span class="chip warn">昨日同窗无</span>' : "");
  return `<strong>${riseSigned(delta, humanMicros)}</strong>${percent}<br><span class="muted">${humanMicros(before)} → ${humanMicros(now)}</span>`;
}

function riseScanDelta(row) {
  const scan = row.attribution?.rows_examined || {};
  return scan.delta === null || scan.delta === undefined ? null : Number(scan.delta);
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
  const growing = Number(row.runtime_delta_us || 0) > 0 || (riseScanDelta(row) || 0) > 0;
  const minutes = (row.runtime_us || []).length;
  return `<tr class="${growing ? "" : "is-flat"}">
    <td class="rise-rank">${position}</td>
    <td><button class="slow-sql-link" type="button" data-rise-node="${escapeHtml(node.node_id)}" data-rise-fp="${escapeHtml(row.fingerprint)}" title="查看样本执行与逐分钟序列"><code class="sql-cell">${escapeHtml(sql)}</code></button></td>
    <td>${riseShare(row)}</td>
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

// 主 / 备角色取自云上实例的节点信息，是「当前」角色；主备切换后事故当时的角色可能不同。
function riseRoleChip(node) {
  if (!node.role && !node.zone) return "";
  const role = typeof nodeRoleText === "function" ? nodeRoleText(node.role) : node.role;
  const zone = typeof nodeZoneText === "function" ? nodeZoneText(node.zone) : node.zone;
  return `<span class="chip role-${escapeHtml(node.role || "unknown")}" title="节点当前角色，取自云上实例的节点信息；主备切换后会变">${escapeHtml(role)}</span>${zone ? `<span class="muted">${escapeHtml(zone)}</span> ` : ""}`;
}

function riseNodeSummary(data, node) {
  const comparison = node.resource_comparison || {};
  const label = riseMetricLabel(data.metric_id);
  const change = comparison.delta === null || comparison.delta === undefined
    ? "昨日同窗指标不完整，不比较"
    : `昨日同窗 ${riseNumber(comparison.baseline_mean, 2)}，${riseSigned(comparison.delta, (v) => riseNumber(v, 2))}`;
  return `<strong>${escapeHtml(node.node_id || "未知节点")}</strong> ${riseRoleChip(node)}· ${escapeHtml(label)}：窗口均值 <strong>${riseNumber(comparison.current_mean, 2)}</strong>（${change}）· 峰值 ${riseNumber(node.peak, 2)}`;
}

function riseNodeBody(data, node) {
  if (node.status === "metric_gaps") {
    return `<p class="analytics-unavailable">该节点云监控指标缺点（${humanCount(node.actual_points)} / ${humanCount(node.expected_points)}），不补零、不排名。</p>`;
  }
  const rows = node.statements || [];
  if (!rows.length) return '<p class="analytics-empty">窗口内该节点没有采集到慢 SQL。</p>';
  const note = node.status && node.status !== "ok" && RESOURCE_STATUS[node.status]
    ? `<p class="analytics-note">${escapeHtml(RESOURCE_STATUS[node.status])}；下表按扫描增量和增长份额列出，仅供核对。</p>` : "";
  const shown = rows.reduce((sum, row) => sum + (Number(row.growth_share) || 0), 0);
  const covered = shown > 0 ? `，合计占本节点增长份额 ${riseNumber(shown * 100, 0)}%` : "";
  return `${riseNodeSpark(data, node)}${note}
    <div class="table-wrap"><table class="data-table analytics-table rise-table">
      <thead><tr><th>#</th><th>SQL（先看扫描行比昨日增加的，再按增长份额）</th><th>增长份额</th><th>相关度 r（核对）</th><th>耗时增量（昨日同窗 → 窗口）</th><th>执行次数（昨日 → 窗口）</th><th>扫描 / 锁等待增量</th></tr></thead>
      <tbody>${rows.map((row, index) => riseStatement(node, row, index + 1)).join("")}</tbody>
    </table></div>
    <p class="analytics-note">共 ${humanCount(node.total_fingerprints)} 个 SQL 家族，显示前 ${humanCount(rows.length)} 个${covered}。灰显行的耗时和扫描都没有比昨日同窗增加。</p>`;
}

function riseSpan(us) {
  const minutes = Math.round(Math.abs(us) / 60_000_000);
  const hours = Math.floor(minutes / 60);
  return hours ? `${hours} 小时${minutes % 60 ? ` ${minutes % 60} 分钟` : ""}` : `${minutes} 分钟`;
}

function riseNotice(title, text) {
  return `<div class="notice info compact-notice"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M12 11v6M12 7h.01"/></svg><div>
    <strong>${title}</strong>
    <span>${text}</span></div></div>`;
}

// 服务端发现所选时段（或它的昨日对照日）的慢日志索引不完整时有两种处理：
// 只缺末尾（最新的慢日志还在入库）→ 保留起点、截到最近已入库的整分钟；起点就没入库 → 改查最近的同长度、索引完整的时段。
function riseAdjustNotice(data) {
  const moved = data.adjusted;
  if (!moved) return "";
  const why = moved.reason === "incomplete_baseline_index" ? "所选时段对应的昨日对照日索引还不完整" : "所选时段的慢日志索引还不完整";
  const asked = `${escapeHtml(formatTime(moved.requested_start_us))} → ${escapeHtml(formatTime(moved.requested_end_us))}`;
  if (moved.kind === "truncated") {
    return riseNotice(`已截到最近已入库的时刻（比所选结束早 ${riseSpan(moved.truncated_us)}）`,
      `${why}（${asked}）：最新的慢日志还在入库，起点不变，结果只到 ${escapeHtml(formatTime(data.end_us))}；过几分钟再排查即可补上末尾。`);
  }
  const shift = Number(moved.shift_us) < 0 ? `早 ${riseSpan(moved.shift_us)}` : `晚 ${riseSpan(moved.shift_us)}`;
  return riseNotice(`已自动改查最近的有索引时段（${shift}）`,
    `${why}（${asked}），以下为 ${escapeHtml(formatTime(data.start_us))} → ${escapeHtml(formatTime(data.end_us))} 的结果；时间框已同步。`);
}

// 离现在不远的窗口才报「数据截至」：更老的窗口早已入库，落后多少与它无关。
function riseFreshness(data) {
  if (!data.ready_through_us) return "";
  const behind = Math.max(0, Math.round((Date.now() - Number(data.ready_through_us) / 1000) / 60_000));
  return `<p class="analytics-note">数据截至 <strong>${escapeHtml(formatTime(data.ready_through_us))}</strong>（最新已入库的慢日志，落后现在约 ${behind} 分钟）；慢日志在语句结束后才出现，窗口末尾仍在执行的长查询暂未计入。</p>`;
}

// 表单和之后按需加载的明细都跟着改到实际查询的窗口。
function applyRiseAdjustment(data, query) {
  if (!data.adjusted) return query;
  const params = new URLSearchParams(query);
  params.set("startEpochUs", String(data.start_us));
  params.set("endEpochUs", String(data.end_us));
  applyWindowToForm(data.start_us, data.end_us);
  return params.toString();
}

function applyWindowToForm(startUs, endUs) {
  $("#analytics-start").value = toLocalInput(new Date(startUs / 1000));
  $("#analytics-end").value = toLocalInput(new Date(endUs / 1000));
  $("#analytics-range").value = "custom";
}

function renderRise(data) {
  const panel = $("#analytics-panel-rise");
  $("#analytics-empty").hidden = true;
  const metric = riseMetricLabel(data.metric_id);
  const nodes = [...(data.nodes || [])].sort((a, b) => {
    const rise = (node) => (node.resource_comparison?.delta ?? -Infinity);
    return rise(b) - rise(a) || Number(b.peak || 0) - Number(a.peak || 0) || String(a.node_id).localeCompare(String(b.node_id));
  });
  const prefix = `${(state.riseWindowNotes || []).map((text) => riseNotice("时间框已调整", escapeHtml(text))).join("")}${riseAdjustNotice(data)}`;
  if (!nodes.length) {
    panel.innerHTML = `${prefix}<p class="analytics-unavailable"><strong>无法排查。</strong>${escapeHtml(riseMessage(data.status))}</p>`;
    return;
  }
  const many = nodes.length > 3;
  panel.innerHTML = `${prefix}
    <p class="analytics-note">${escapeHtml(metric)}：窗口 ${escapeHtml(formatTime(data.start_us))} → ${escapeHtml(formatTime(data.end_us))}，对照昨日同窗。排序：先看扫描行比昨日增加的 SQL（按增加量），再按增长份额；相关度 r 只是时序关联的核对列，不是排序依据。份额不是资源贡献率，耗时含等待，也不是因果。SQL 只统计已采集的慢日志，看不到写入型和不够慢的查询负载，可勾选「Binlog 写入」对照。</p>
    ${riseFreshness(data)}
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

/* ---------------------------- 时间框取整与 24 小时上限 ---------------------------- */

// 开始/结束取整到整分钟（只勾 Binlog 时取整到 5 分钟，Binlog 按 5 分钟聚合）；上涨排查的窗口压到 24 小时以内。
// 返回要告诉用户的调整说明；时间框没变就不回写，避免打断正在输入。
function normalizeAnalyticsWindow(forRise) {
  const step = forRise ? 60_000 : 300_000;
  let start = new Date($("#analytics-start").value).getTime();
  let end = new Date($("#analytics-end").value).getTime();
  if (!Number.isFinite(start) || !Number.isFinite(end)) return [];
  const notes = [];
  const rounded = [Math.floor(start / step) * step, Math.floor(end / step) * step];
  const changed = rounded[0] !== start || rounded[1] !== end;
  [start, end] = rounded;
  if (forRise && end - start >= RISE_MAX_WINDOW_MS + 300_000) {
    start = end - RISE_MAX_WINDOW_MS;
    notes.push("上涨排查要和昨日同窗不重叠，窗口已压到最近 23 小时 55 分。");
  }
  if (end <= start) return notes;
  if (changed || notes.length) {
    $("#analytics-start").value = toLocalInput(new Date(start));
    $("#analytics-end").value = toLocalInput(new Date(end));
    if (notes.length) $("#analytics-range").value = "custom";
  }
  return notes;
}

/* ---------------------------- 两类证据 ---------------------------- */

async function runRiseSlowlog() {
  const requested = riseQueryString();
  const data = await api(`/api/slowlog-impact?${requested}`);
  const query = applyRiseAdjustment(data, requested);
  state.rise = { data, query };
  renderRise(data);
  renderRiseCoverage(data);
  return data;
}

// Binlog 写入：沿用「写入 SQL / 事务 / 锁争用推断」三页，窗口缺口由服务端对齐并在 adjusted 里回报。
function binlogAdjustedWindow(result) {
  const moved = result.adjusted;
  if (!moved) return null;
  return moved.kind === "truncated"
    ? [moved.requested_start_us, moved.requested_end_us - moved.truncated_us]
    : [moved.requested_start_us + moved.shift_us, moved.requested_end_us + moved.shift_us];
}

function binlogNotice(result) {
  const moved = result?.adjusted;
  let html = "";
  if (moved) {
    const asked = `${escapeHtml(formatTime(moved.requested_start_us))} → ${escapeHtml(formatTime(moved.requested_end_us))}`;
    const [start, end] = binlogAdjustedWindow(result);
    html += moved.kind === "truncated"
      ? riseNotice(`Binlog 已截到最近已入库的时刻（比所选结束早 ${riseSpan(moved.truncated_us)}）`,
        `所选时段（${asked}）的 Binlog 还没有全部入库，起点不变，结果只到 ${escapeHtml(formatTime(end))}。`)
      : riseNotice(`Binlog 已自动改查最近的已入库时段（${Number(moved.shift_us) < 0 ? "早" : "晚"} ${riseSpan(moved.shift_us)}）`,
        `所选时段（${asked}）没有完整入库，以下为 ${escapeHtml(formatTime(start))} → ${escapeHtml(formatTime(end))} 的结果。`);
  }
  if (result?.ready_through_us) {
    const behind = Math.max(0, Math.round((Date.now() - Number(result.ready_through_us) / 1000) / 60_000));
    html += `<p class="analytics-note">Binlog 已入库到 <strong>${escapeHtml(formatTime(result.ready_through_us))}</strong>（落后现在约 ${behind} 分钟）。</p>`;
  }
  return html;
}

async function runBinlogWrites() {
  const query = analyticsQueryString("", "binlog");
  const result = await api(`/api/analytics?${query}`);
  state.analyticsRuns.binlog = { query, result };
  renderAnalytics(result);
  return result;
}

const RISE_SOURCE_LABEL = { slowlog: "慢日志", binlog: "Binlog 写入" };

async function runRise() {
  const useSlowlog = riseUses("slowlog");
  const useBinlog = riseUses("binlog");
  if (!useSlowlog && !useBinlog) throw new Error("请至少勾选一个证据来源（慢日志 / Binlog 写入）");
  if (useSlowlog && !ensureRiseInstance()) throw new Error("请先选择一个 RDS 实例再排查上涨");
  state.riseWindowNotes = normalizeAnalyticsWindow(useSlowlog);
  state.analyticsRuns = {}; // 明细视图按新的窗口重新加载
  state.analyticsRunsQuery = "";
  const jobs = [];
  if (useSlowlog) jobs.push(["slowlog", runRiseSlowlog()]);
  if (useBinlog) jobs.push(["binlog", runBinlogWrites()]);
  const settled = await Promise.allSettled(jobs.map(([, job]) => job));
  const failures = settled.map((outcome, index) => (outcome.status === "rejected" ? [jobs[index][0], outcome.reason] : null)).filter(Boolean);
  if (failures.length === jobs.length) throw failures[0][1]; // 勾选的来源都失败，交给提交处理器弹错
  for (const [kind, error] of failures) {
    const message = error?.message || String(error);
    if (kind === "slowlog") $("#analytics-panel-rise").innerHTML = `<p class="analytics-unavailable"><strong>上涨排查失败。</strong>${escapeHtml(message)}；另一类证据不受影响，可稍后重新分析。</p>`;
    else renderAnalyticsFailure("binlog", error);
    toast(`${RISE_SOURCE_LABEL[kind]}分析失败：${message}`, "error", 6000);
  }
  const outcome = Object.fromEntries(jobs.map(([kind], index) => [kind, settled[index]]));
  const binlog = outcome.binlog?.status === "fulfilled" ? outcome.binlog.value : null;
  if (binlog?.adjusted && !useSlowlog) {
    const [start, end] = binlogAdjustedWindow(binlog);
    applyWindowToForm(start, end);
  }
  if (binlog) {
    // Binlog 页签顶部：窗口调整与入库水位。
    $("#analytics-panel-writes").innerHTML = binlogNotice(binlog) + $("#analytics-panel-writes").innerHTML;
  }
  const data = outcome.slowlog?.status === "fulfilled" ? outcome.slowlog.value : null;
  const scope = [$("#analytics-source").selectedOptions[0].textContent, $("#analytics-instance").selectedOptions[0].textContent,
    data ? riseMetricLabel(data.metric_id) : "", `${$("#analytics-start").value.replace("T", " ")} → ${$("#analytics-end").value.replace("T", " ")}`]
    .filter(Boolean).join(" · ");
  $("#analytics-meta").textContent = scope;
  // 留在用户当前的页签（它所属的来源仍勾选时），否则回到第一个来源的主页签。
  const shown = { rise: useSlowlog, writes: useBinlog, sql: useSlowlog, transactions: useBinlog, locks: useBinlog };
  const target = shown[state.rdsTab] ? state.rdsTab : (useSlowlog ? "rise" : "writes");
  switchAnalyticsTab(target);
  if (target === "sql") await loadAnalyticsDetails();
  if (data) {
    if (!data.nodes?.length) toast(`无法排查：${riseMessage(data.status)}`, "error", 6000);
    else if (data.adjusted?.kind === "truncated") toast(`所选时段末尾还没入库，已截到 ${formatTime(data.end_us)}`, "info", 6000);
    else if (data.adjusted) toast("所选时段没有完整索引，已改查最近的有索引时段", "info", 6000);
    else if (!failures.length) toast("排查完成", "success");
  } else if (binlog && !failures.length) {
    toast(binlog.adjusted ? "Binlog 窗口已对齐到最近的已入库时段" : "分析完成", binlog.adjusted ? "info" : "success", 6000);
  }
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
    <p>增长份额 ${riseShare(row)} · 相关度 ${riseCorrelation(row)} · ${escapeHtml(riseMetricLabel(data.metric_id))} · 节点 ${escapeHtml(node.node_id)} ${riseRoleChip(node)}</p>
    <p>耗时 ${humanMicros(row.baseline_runtime_us_total)} → ${humanMicros(row.runtime_us_total)}；执行 ${humanCount(row.attribution?.baseline_count)} → ${humanCount(row.executions)} 次${members > 1 ? `；合并 ${members} 个物理指纹（分表族）` : ""}。</p>
    ${analyticsTable(["分钟结束", `${riseMetricLabel(data.metric_id)}`, "该 SQL 当分钟执行耗时"], riseDetailSeries(data, node, row))}
    <p class="analytics-note">增长份额 = 每分钟「执行耗时 × 高于窗口 P20 基线的指标超额」乘以相比昨日同窗的正向耗时增长比例，在本节点内的占比，不是资源贡献率；相关度是上表两列的皮尔逊系数，只说明时间上同涨同落，不证明因果，也不衡量该 SQL 消耗了多少资源。</p></section>`);
}
