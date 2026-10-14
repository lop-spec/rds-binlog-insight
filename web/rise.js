"use strict";

/* 上涨排查（RDS）：选一个时间窗和一个主要指标，看窗口内哪些慢 SQL 比昨日同窗多花了成本、并与该指标曲线重合。
   排序是服务端的「增长份额」：该 SQL 在指标超额分钟里跑了多久，乘以比昨日同窗多出来的执行时间（已扣除慢日志记录的锁等待）；
   扫描行增量只在份额相同时决定先后；相关度 r 只是时序关联的核对列，不是排序依据，也不是资源消耗的度量或因果。
   慢日志要等语句结束才出现，正在跑的查询由「看正在跑的查询」按需读取 DAS 实时会话。
   证据来源可勾选：慢日志（上涨排查）与 Binlog 写入（写入 SQL / 事务 / 锁争用推断），只请求勾选的。 */

const RISE_METRICS = [
  ["cpu", "CPU 使用率 %"],
  ["iops", "IOPS 使用率 %"],
  ["rows_read", "InnoDB 读取行数 / 秒"],
  ["row_lock", "行锁等待 ms / 秒"],
  ["threads", "活跃线程数"],
  ["replication_lag", "复制延迟 秒（备节点）"],
];

const RISE_STATUS = {
  incomplete_index: "所选时段及前后 6 小时内没有索引完整的同长度时段（索引仍在追平，或该实例暂无采集）",
  incomplete_baseline_index: "所选时段的昨日对照日索引不完整，前后 6 小时内也没有可用的时段",
  unsupported_metric: "不支持该指标",
  no_collected_executions: "该窗口没有采集到慢 SQL 执行记录",
  metric_or_input_unavailable: "云监控指标或输入校验失败，未计算（详见服务日志）",
  insufficient_complete_periods: "窗口不足 6 个完整分钟，不计算相关度",
  no_metric_or_execution_data: "没有指标或执行数据",
  no_replica_lag_samples: "没有取到备节点的复制延迟指标（主节点没有复制延迟；所选节点若是主节点请换成备节点）",
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
    return '<span class="muted" title="没有相比昨日同窗的正向增长（只因锁等待变长的不计入）">—</span>';
  }
  const percent = Number(row.growth_share) * 100;
  const text = percent >= 10 ? riseNumber(percent, 0) : percent >= 1 ? riseNumber(percent, 1) : riseNumber(percent, 2);
  const lock = Number(row.lock_wait_excluded_us) > 0 ? `；已扣除慢日志记录的锁等待增长 ${humanMicros(row.lock_wait_excluded_us)}` : "";
  return `<strong title="增长份额：每分钟「执行耗时 × 高于窗口 P20 基线的指标超额」乘以相比昨日同窗的正向增长比例（已扣除慢日志记录的锁等待），在本节点内的占比；不是资源贡献率${lock}">${text}%</strong>`;
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
    <td>${accountCell(row)}</td>
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
  const lag = data.metric_id === "replication_lag";
  const origin = lag ? riseLagOrigin(data, node) : "";
  if (!rows.length) {
    return `${origin}<p class="analytics-empty">${lag ? "窗口内主库没有采集到可与延迟增长对照的慢 SQL。" : "窗口内该节点没有采集到慢 SQL。"}</p>`;
  }
  const note = node.status && node.status !== "ok" && RESOURCE_STATUS[node.status]
    ? `<p class="analytics-note">${escapeHtml(RESOURCE_STATUS[node.status])}；下表按扫描增量和增长份额列出，仅供核对。</p>` : "";
  // A stuck applier is not caused by what the primary wrote: say so right above the table that would suggest otherwise.
  const stalled = lag && (node.lag_origin?.episodes || []).some((episode) => episode.kind === "stall");
  const stallNote = stalled ? '<p class="analytics-note"><strong>这个节点的延迟是回放线程卡住，不是主库写得太多</strong>：下表只是同期主库上跑得久的慢 SQL，份额和相关度说明不了原因，原因看上面的溯源。</p>' : "";
  // The same for a node that is simply slower than its siblings while they replay the same writes without lagging.
  const slower = lag && riseNodeSlower(node.lag_origin);
  const slowerNote = slower && !stalled ? '<p class="analytics-note"><strong>这个节点的延迟是它自己比其他备节点慢，不是主库写得太多</strong>：下表只是同期主库上跑得久的慢 SQL，份额和相关度说明不了原因，原因看上面「和其他备节点比」。</p>' : "";
  const shown = rows.reduce((sum, row) => sum + (Number(row.growth_share) || 0), 0);
  const covered = shown > 0 ? `，合计占本节点增长份额 ${riseNumber(shown * 100, 0)}%` : "";
  return `${riseLiveBox(node)}${riseNodeSpark(data, node)}${origin}${note}${stallNote}${slowerNote}
    <div class="table-wrap"><table class="data-table analytics-table rise-table">
      <thead><tr><th>#</th><th>${lag ? "SQL（主库执行，按增长份额排序）" : "SQL（按增长份额排序）"}</th><th>用户</th><th>增长份额</th><th>相关度 r（核对）</th><th>耗时增量（昨日同窗 → 窗口）</th><th>执行次数（昨日 → 窗口）</th><th>扫描 / 锁等待增量</th></tr></thead>
      <tbody>${rows.map((row, index) => riseStatement(node, row, index + 1)).join("")}</tbody>
    </table></div>
    <p class="analytics-note">共 ${humanCount(node.total_fingerprints)} 个 SQL 家族，显示前 ${humanCount(rows.length)} 个${covered}。灰显行的耗时和扫描都没有比昨日同窗增加。</p>`;
}

/* ---------------------------- 复制延迟溯源 ---------------------------- */

// 备节点的延迟不是它自己的读查询造成的：要么是主库写了什么，要么是备节点的回放线程卡在某个事件上。
function riseLagNote(data) {
  return `<p class="analytics-note">复制延迟：窗口 ${escapeHtml(formatTime(data.start_us))} → ${escapeHtml(formatTime(data.end_us))}，对照昨日同窗。
    延迟出在主库写入或备节点的回放线程，不在备节点自己的读查询，所以下表列的是<strong>主库上的慢 SQL</strong>，按它们在「延迟增长的分钟」里跑了多久排序（延迟本身是累积量，只在涨的那几分钟里算重合），再乘以比昨日同窗多出来的部分。
    表上方的「复制延迟溯源」先回答更关键的问题：延迟是从主库的哪个时刻开始的、是不是回放线程卡在同一个事件上、那一刻 binlog 里有没有 DDL。份额和相关度只是时间上的同涨同落，不是因果；
    慢日志只含已执行完且够慢的语句，一条几毫秒就执行完的 DDL 不会出现在表里，要看溯源。</p>`;
}

function riseLagWho(item) {
  const id = escapeHtml(item.event_id || "");
  const detail = item.event_id ? ` <button class="slow-sql-link" type="button" data-lag-event="${id}" title="查看这条慢日志的明细">明细</button>` : "";
  return `<li>${escapeHtml(item.account || "用户未知")} · ${escapeHtml(formatTime(item.start_us, true))} 开始 · 运行 <strong>${escapeHtml(humanMicros(Number(item.duration_ms || 0) * 1000))}</strong>${detail}</li>`;
}

function riseLagList(block, text) {
  const items = block?.samples || [];
  const more = Number(block?.count || 0) - items.length;
  return `<p>${text}</p><ul class="rise-lag-list">${items.map(riseLagWho).join("")}${more > 0 ? `<li class="muted">另有 ${humanCount(more)} 条</li>` : ""}</ul>`;
}

function riseLagTrigger(episode) {
  const trigger = episode.trigger || {};
  if (trigger.type === "ddl") {
    const where = [trigger.database, trigger.table].filter(Boolean).join(".");
    const offset = Number(trigger.offset_s);
    return `<p>主库 binlog 在这个时刻有一条 <strong>DDL</strong>（${escapeHtml(formatTime(trigger.event_us))}，与估算起点相差 ${riseNumber(Math.abs(offset), 0)} 秒）${where ? `，对象 ${escapeHtml(where)}` : ""}：<code class="sql-cell">${escapeHtml(trigger.sql || "（未记录语句）")}</code><br>
      DDL 在备节点回放时要拿到这张表的元数据锁：所有先开始、还没结束的查询或事务放手之前它一直等，复制线程停在它身上；它排在前面时，这张表上随后到的查询也会排在它后面一起等。</p>`;
  }
  if (trigger.type === "unavailable") return "<p>Binlog 行存储这时读不到 DDL 记录（详见服务日志），未能核对那一刻有没有 DDL。</p>";
  return "<p>主库 binlog 在这个时刻附近<strong>没有 DDL</strong>：卡住的是同一个事件的回放（超大事务，或回放时等锁）。行存储没有逐事务明细，需要看那一刻前后主库提交的大事务。</p>";
}

// Minute resolution: Cloud Monitor samples and the five-minute binlog aggregates say nothing about seconds.
function riseStamp(us) {
  return formatTime(us, true).replace(/:\d{2}$/, "");
}

function riseClock(us) {
  const date = new Date(Number(us) / 1000);
  return Number.isNaN(date.getTime()) ? "—" : date.toLocaleTimeString("zh-CN", { hour: "2-digit", minute: "2-digit", hour12: false });
}

// What the primary wrote while the lag lasted: its busiest tables in the binlog row store, and whether they were busy before.
function riseLagWrites(episode) {
  const writes = episode.writes;
  if (!writes) return "";
  if (writes.status === "unavailable") return '<p class="muted">同期主库的写入热点读不到（详见服务日志）。</p>';
  const tables = (writes.tables || []).filter((table) => Number(table.events) > 0).slice(0, 3);
  if (!tables.length) return '<p class="muted">Binlog 行存储里这段时间没有这个实例的写入记录（可能还没入库）。</p>';
  const items = tables.map((table) => {
    const name = [table.database_name, table.table_name].filter(Boolean).join(".");
    const parts = [["删", table.deletes], ["插", table.inserts], ["改", table.updates]].filter(([, count]) => Number(count) > 0)
      .map(([label, count]) => `${label} ${humanCount(count)}`).join(" / ");
    const share = Number(table.share) > 0 ? `，占全部行事件 ${riseNumber(table.share * 100, 0)}%` : "";
    const before = Number(table.before_events) > 0
      ? `；前一段同长时间 ${humanCount(table.before_events)}（这一段是 ${riseNumber(table.events / table.before_events, 1)} 倍）`
      : "；前一段同长时间几乎没有写入";
    return `<li><code>${escapeHtml(name)}</code>：${humanCount(table.events)} 行事件${parts ? `（${parts}）` : ""}${share}${before}</li>`;
  });
  return `<p>同期主库写得最多的表（Binlog 行事件，${escapeHtml(riseStamp(writes.start_us))} → ${escapeHtml(riseStamp(writes.end_us))}）：</p><ul class="rise-lag-list">${items.join("")}</ul>`;
}

// The node is slower than its siblings and only it lagged: the verdict that moves the blame off the primary's writes.
function riseNodeSlower(block) {
  const episodes = block?.episodes || [];
  return block?.peer_health?.diverged === true && episodes.length > 0 && episodes.every((episode) => episode.node_local === true);
}

function riseReadsRemoved(health) {
  const spans = (health.reads_removed || []).map((span) => `${riseClock(span.start_us)}–${riseClock(span.end_us)}`);
  if (!spans.length) return "";
  return `<p class="analytics-note">读流量被摘除的时段：${spans.join("、")}。读写分离在备节点延迟越过阈值后会把它从读路由里摘掉，这些分钟里它几乎没有读，上表没有计入。</p>`;
}

function riseLagPeerRow(label, row, digits, flag = false, hint = "") {
  if (!row) return "";
  const ratio = row.ratio === null || row.ratio === undefined ? "—" : `×${riseNumber(row.ratio, 1)}`;
  const title = hint ? ` title="${escapeHtml(hint)}"` : "";
  return `<tr><td${title}>${escapeHtml(label)}</td><td>${riseNumber(row.node, digits)}</td><td>${riseNumber(row.peers, digits)}</td><td>${flag ? `<strong>${ratio}</strong>` : ratio}</td></tr>`;
}

// The node against the other replicas of the same instance, on what Cloud Monitor records per node.
function riseLagPeers(block) {
  const health = block.peer_health;
  if (!health) return "";
  const title = "<h4>和其他备节点比</h4>";
  if (health.status === "unavailable") return `${title}<p class="muted">读不到云监控的节点指标（详见服务日志），没能和其他备节点比较。</p>`;
  if (health.status === "no_peers" || health.status === "no_data") return `${title}<p class="muted">这个实例里没有可比较的其他备节点，或云监控缺少这个节点的读指标。</p>`;
  if (health.status === "no_reads") {
    return `${title}<p>延迟期间这个节点几乎没有读流量（只有 ${humanCount(health.period?.read_minutes)} 分钟可比，不足以判断它自己是不是变慢了）。</p>${riseReadsRemoved(health)}`;
  }
  const episodes = block.episodes || [];
  const metrics = health.metrics || {};
  const slow = metrics.slow_per_1000_reads;
  const local = episodes.length > 0 && episodes.every((episode) => episode.node_local === true);
  const shared = episodes.some((episode) => episode.node_local === false);
  const rates = slow ? `每 1000 次读有 ${riseNumber(slow.node, 2)} 条慢查询，其他备节点是 ${riseNumber(slow.peers, 2)}（×${riseNumber(slow.ratio, 1)}）` : "";
  let verdict;
  if (health.diverged) {
    const first = Math.min(...episodes.map((episode) => Number(episode.first_us)));
    const since = health.diverged_since_us
      ? `，从约 ${escapeHtml(riseStamp(health.diverged_since_us))} 起一直如此${first > health.diverged_since_us ? `（比第一段延迟早 ${riseSpan(first - health.diverged_since_us)}）` : ""}` : "";
    const scope = local ? "问题在这个节点自己：其他备节点回放同样的写入，延迟都没到阈值。"
      : shared ? "其他备节点也出现了延迟，主库写入是共同原因，这个节点另外还比它们慢。" : "";
    verdict = `<p><strong>这个节点自己比其他备节点慢</strong>：延迟期间${rates}${since}。${scope}</p>`;
    const idle = Number(metrics.cpu_pct?.ratio) <= 1.2 && Number(metrics.iops_pct?.ratio) <= 1.2;
    if (idle) verdict += "<p>CPU 和 IOPS 都没有比它们高，却更慢：多半是在等（存储、锁或宿主机），不是在算。这里看不到盘延迟，要到这个节点上看复制线程状态和 IO 延迟，或向云厂商查盘。</p>";
  } else {
    const scope = local ? "延迟出在回放线程本身而不是读请求；这里没有它的复制线程状态。"
      : shared ? "其他备节点也延迟了，主库写入是共同原因，看上面的写入热点。" : "";
    verdict = `<p><strong>这个节点的读表现和其他备节点一致</strong>${rates ? `（${rates}）` : ""}。${scope}</p>`;
  }
  const table = `<table class="rise-peers"><thead><tr><th>指标（延迟期间均值）</th><th>这个节点</th><th>其他备节点（中位数）</th><th>倍数</th></tr></thead><tbody>
    ${riseLagPeerRow("慢查询 / 1000 次读", slow, 2, health.diverged)}
    ${riseLagPeerRow("读 QPS", metrics.reads_per_s, 0)}
    ${riseLagPeerRow("单次读耗时 ms（粗估）", metrics.ms_per_read, 2, false, "并发线程数 ÷ 读速率；线程数里含复制线程，只作参考")}
    ${riseLagPeerRow("CPU %", metrics.cpu_pct, 1)}
    ${riseLagPeerRow("IOPS 使用率 %", metrics.iops_pct, 1)}
  </tbody></table>`;
  const basis = `<p class="analytics-note">比较的是延迟段内两边都有读流量的 ${humanCount(health.period?.read_minutes)} 分钟（延迟段共 ${humanCount(health.period?.minutes)} 分钟），对象是同实例其他 ${humanCount(health.peers)} 个备节点的中位数。慢查询按各节点的 long_query_time 记，同一实例各节点相同。</p>`;
  return `${title}${verdict}${table}${basis}${riseReadsRemoved(health)}`;
}

function riseLagEpisode(block, episode) {
  const when = `${escapeHtml(formatTime(episode.first_us, true))} → ${escapeHtml(formatTime(episode.last_us, true))}`;
  const head = `<p><strong>${when}</strong>：延迟峰值 <strong>${riseNumber(episode.peak_s, 0)} 秒</strong>（${escapeHtml(formatTime(episode.peak_us, true))}）${episode.recovery_rate ? `，之后以每秒追 ${riseNumber(episode.recovery_rate, 2)} 秒的速度赶回` : ""}。</p>`;
  if (episode.kind === "behind") {
    const where = episode.node_local === true ? "同一时段其他备节点的延迟都没到阈值：只有这个节点落后。"
      : episode.node_local === false ? "同一时段其他备节点也出现了延迟。" : "";
    return `${head}<p>延迟以每秒约 ${riseNumber(episode.growth_rate, 2)} 秒的速度增长而不是一口气卡住：备节点的回放速度跟不上主库的写入。${where}下表是同期主库上的慢语句。</p>${riseLagWrites(episode)}`;
  }
  if (episode.kind !== "stall") {
    return `${head}<p>短时尖峰，既没有持续卡住也没有持续落后，多半是一次性的大事务。</p>${riseLagWrites(episode)}`;
  }
  const stall = episode.stall || {};
  const slack = Number(block.origin_slack_s) || 30;
  let body = `<p>延迟每秒涨 ${riseNumber(episode.growth_rate, 2)} 秒、持续 ${humanCount(stall.minutes)} 分钟：这个节点的复制线程<strong>卡在同一个事件上</strong>，回放没有前进。该事件在主库的时间约 <strong>${escapeHtml(formatTime(stall.origin_us))}</strong>（±${riseNumber(slack, 0)} 秒），放行在 ${escapeHtml(formatTime(stall.release_us || stall.end_us, true))} 前后。</p>`;
  body += riseLagTrigger(episode);
  if (episode.node_local === true) body += "<p>同一时段其他备节点的延迟都没到阈值：问题只在这个节点上，不是主库写入量引起的。</p>";
  else if (episode.node_local === false) body += "<p>同一时段其他备节点也出现了延迟：除了这个节点自己，也要看主库当时的写入。</p>";
  if (episode.queued?.count) body += riseLagList(episode.queued, `这个节点上有 <strong>${humanCount(episode.queued.count)} 条</strong>语句开始于卡顿之后、又恰好随卡顿一起结束（被堵在同一把锁后面的受害者，运行时间最长的在前）：`);
  if (episode.holders?.count) body += riseLagList(episode.holders, "开始于卡顿之前、随卡顿一起结束的语句（持锁者候选）：");
  else if (episode.holders) body += "<p>没有任何语句开始于卡顿之前、又随卡顿一起结束：<strong>持锁的不在慢日志里</strong>。常见原因是一个没提交的空闲事务或连接（慢日志要等语句结束才出现，空闲会话不会出现）。再出现时，卡顿进行中可点本节点的「看正在跑的查询」；但空闲事务不在其中，要到该节点上查元数据锁。</p>";
  return `${head}${body}`;
}

function riseLagOrigin(data, node) {
  const block = node.lag_origin;
  if (!block) return "";
  const episodes = block.episodes || [];
  if (!episodes.length) {
    return `<div class="rise-lag"><p><strong>复制延迟溯源</strong>：本窗口内该节点的延迟没有达到 ${riseNumber(block.floor_s, 0)} 秒（告警阈值），没有可溯源的延迟段。</p></div>`;
  }
  const more = Number(block.episode_count || 0) - episodes.length;
  return `<div class="rise-lag"><h4>复制延迟溯源</h4>${episodes.map((episode) => riseLagEpisode(block, episode)).join("")}
    ${riseLagPeers(block)}
    ${more > 0 ? `<p class="muted">还有 ${humanCount(more)} 段较小的延迟未列出。</p>` : ""}
    <p class="analytics-note">起点由「样本分钟的中点 − 延迟」推出：回放线程卡在一个事件上时，它正在回放的那个事件在主库的时间不动。云监控是一分钟一个点，所以起点有 ±${riseNumber(block.origin_slack_s, 0)} 秒的误差；受害者与持锁者只统计窗口内开始且已执行完的慢日志。</p></div>`;
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
  return `<p class="analytics-note">数据截至 <strong>${escapeHtml(formatTime(data.ready_through_us))}</strong>（最新已入库的慢日志，落后现在约 ${behind} 分钟）；慢日志在语句结束后才出现，窗口末尾仍在执行的长查询暂未计入，可点节点里的「看正在跑的查询」。</p>`;
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
  const lag = data.metric_id === "replication_lag";
  panel.innerHTML = `${prefix}
    ${lag ? riseLagNote(data) : `<p class="analytics-note">${escapeHtml(metric)}：窗口 ${escapeHtml(formatTime(data.start_us))} → ${escapeHtml(formatTime(data.end_us))}，对照昨日同窗。排序：按增长份额（该 SQL 在指标超额分钟里跑了多久，乘以比昨日同窗多出来的部分，已扣除慢日志记录的锁等待）；扫描行增量只在份额相同时决定先后，相关度 r 只是时序关联的核对列。份额不是资源贡献率，除锁等待外的等待仍算在耗时里，也不是因果；慢日志的扫描行可能远小于真实扫描（含物化子查询的语句）。SQL 只统计已采集的慢日志，看不到写入型和不够慢的查询负载，可勾选「Binlog 写入」对照；语句结束后才会进慢日志，正在跑的点节点里的「看正在跑的查询」。</p>`}
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

/* ---------------------------- 正在跑的查询（按需读取 DAS 实时会话） ---------------------------- */

function riseLiveBox(node) {
  const id = escapeHtml(node.node_id);
  return `<div class="rise-live" data-live-box="${id}">
    <button class="button ghost compact" type="button" data-live-node="${id}" title="实时读取该节点的会话列表（DAS，只读，约 2–3 秒；10 秒内重复点击用缓存）">看正在跑的查询</button>
    <span class="muted">慢日志要等语句结束才出现，事故进行中点这里</span>
    <div class="rise-live-result" hidden></div>
  </div>`;
}

function liveSessionsHtml(data) {
  const rows = data.sessions || [];
  const observed = data.observed_at_us ? formatTime(data.observed_at_us, true) : "—";
  const head = `<p class="analytics-note">${escapeHtml(observed)} 的会话快照${data.cached ? "（10 秒内缓存）" : ""}：共 ${humanCount(data.total_sessions)} 个会话，${humanCount(data.running_sessions)} 个在执行，最长已运行 ${humanCount(data.longest_running_seconds)} 秒。${data.truncated ? "只列运行最久的 50 个。" : ""}</p>`;
  if (!rows.length) return `${head}<p class="analytics-empty">此刻没有在执行的语句。</p>`;
  return head + analyticsTable(["用户", "来源 IP", "库", "已运行", "状态", "SQL"], rows.map((row) => [
    escapeHtml(row.user || "—"),
    escapeHtml(row.client || "—"),
    escapeHtml(row.database || "—"),
    `<strong>${escapeHtml(humanMicros(Number(row.seconds || 0) * 1_000_000))}</strong>`,
    escapeHtml(row.state || row.command || "—"),
    `<code class="sql-cell" title="${escapeHtml(row.sql || "")}">${escapeHtml(String(row.sql || "").replace(/\s+/g, " "))}</code>`,
  ]));
}

async function loadLiveSessions(nodeId, button) {
  const box = button.closest("[data-live-box]");
  const result = box?.querySelector(".rise-live-result");
  const instance = state.rise?.data?.instance_id || "";
  if (!result || !instance) return;
  button.disabled = true;
  result.hidden = false;
  result.innerHTML = '<p class="analytics-note">正在读取该节点的实时会话…</p>';
  try {
    const data = await api(`/api/live-sessions?instance=${encodeURIComponent(instance)}&nodeId=${encodeURIComponent(nodeId)}`);
    result.innerHTML = liveSessionsHtml(data);
    button.textContent = "刷新正在跑的查询";
  } catch (error) {
    result.innerHTML = `<p class="analytics-unavailable"><strong>读取失败。</strong>${escapeHtml(error.message)}</p>`;
  } finally {
    button.disabled = false;
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
    <p class="analytics-note">增长份额 = 每分钟「执行耗时 × 高于窗口 P20 基线的指标超额」乘以相比昨日同窗的正向增长比例（扣除慢日志记录的锁等待），在本节点内的占比，不是资源贡献率；相关度是上表两列的皮尔逊系数，只说明时间上同涨同落，不证明因果，也不衡量该 SQL 消耗了多少资源。</p></section>`);
}
