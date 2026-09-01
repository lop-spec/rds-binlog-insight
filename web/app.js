"use strict";

const state = {
  view: "audit",
  queryOffset: 0,
  queryLimit: 100,
  hasMore: false,
  status: null,
  settings: null,
  statusTimer: null,
  queryTaskTimer: null,
  statusRefreshInFlight: false,
  queryTaskRefreshInFlight: false,
  queryPromise: null,
  queryTasks: [],
  activeQueryTaskId: "",
  activeQuery: null,
  renderedQueryTaskId: "",
  notifiedQueryTasks: new Set(),
  lastBackfillMessage: "",
  analytics: null,
  analyticsTab: "sql",
  analyticsInitialized: false,
  sqlOrder: "executions",
  txnDrill: "longest",
  // 事务钻取：从分析洞察的事务榜单点进来时带上 GTID，审计查询按它精确过滤。
  txnFilter: "",
  schemaInitialized: false,
  schemaInstances: [],
  schemaScope: "target",
  schemaFilter: "diff",
  schemaResult: null,
};

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

const icons = {
  detail: '<svg viewBox="0 0 24 24"><path d="m9 18 6-6-6-6"/></svg>',
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function humanBytes(input) {
  const value = Number(input || 0);
  if (!Number.isFinite(value) || value <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  const index = Math.min(Math.floor(Math.log(value) / Math.log(1024)), units.length - 1);
  return `${(value / (1024 ** index)).toFixed(index < 2 ? 0 : 2)} ${units[index]}`;
}

function humanCount(input) {
  const value = Number(input || 0);
  return new Intl.NumberFormat("zh-CN").format(value);
}

function formatTime(value, compact = false) {
  if (!value) return "—";
  let date;
  if (typeof value === "number") date = new Date(value / 1000);
  else date = new Date(String(value).replace(" ", "T"));
  if (Number.isNaN(date.getTime())) return String(value);
  return new Intl.DateTimeFormat("zh-CN", {
    year: compact ? undefined : "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(date).replaceAll("/", "-");
}

function toLocalInput(date) {
  const offset = date.getTimezoneOffset() * 60_000;
  return new Date(date.getTime() - offset).toISOString().slice(0, 19);
}

function toast(message, kind = "info", timeout = 3600) {
  const item = document.createElement("div");
  item.className = `toast ${kind}`;
  item.textContent = message;
  $("#toast-region").append(item);
  window.setTimeout(() => item.remove(), timeout);
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    cache: "no-store",
    headers: { "Content-Type": "application/json", ...(options.headers || {}) },
    ...options,
  });
  let payload;
  try {
    payload = await response.json();
  } catch {
    throw new Error(`本地服务返回了无效响应（HTTP ${response.status}）`);
  }
  if (!response.ok || payload.ok === false) {
    const error = new Error(payload?.error?.message || `请求失败（HTTP ${response.status}）`);
    error.code = payload?.error?.code || "REQUEST_FAILED";
    throw error;
  }
  return payload.data;
}

async function withBusy(button, work, busyText = "处理中…") {
  const original = button.innerHTML;
  button.disabled = true;
  button.textContent = busyText;
  try {
    return await work();
  } finally {
    button.disabled = false;
    button.innerHTML = original;
  }
}

const viewMeta = {
  audit: ["AUDIT EXPLORER", "审计查询"],
  analytics: ["WORKLOAD ANALYTICS", "分析洞察"],
  schema: ["SCHEMA COMPARE", "结构对比"],
  jobs: ["SEQUENTIAL PIPELINE", "同步任务"],
  storage: ["PHYSICAL DATASET", "物理存储"],
  settings: ["SERVICE CONTROL", "服务设置"],
};

function switchView(name) {
  if (!viewMeta[name]) return;
  state.view = name;
  $$(".nav-item").forEach((item) => item.classList.toggle("is-active", item.dataset.view === name));
  $$(".view").forEach((view) => view.classList.toggle("is-active", view.id === `view-${name}`));
  $("#page-eyebrow").textContent = viewMeta[name][0];
  $("#page-title").textContent = viewMeta[name][1];
  if (name === "jobs") refreshJobs();
  if (name === "storage") refreshStorage();
  if (name === "settings") refreshSettings();
  if (name === "analytics" && !state.analyticsInitialized) {
    state.analyticsInitialized = true;
    setAnalyticsRange("24h");
  }
  if (name === "schema" && !state.schemaInitialized) {
    state.schemaInitialized = true;
    initSchemaView();
  }
  window.scrollTo({ top: 0, behavior: "smooth" });
}

function setQuickRange(range) {
  const units = { "15m": 15 * 60_000, "1h": 60 * 60_000, "24h": 24 * 60 * 60_000, "7d": 7 * 24 * 60 * 60_000, "30d": 30 * 24 * 60 * 60_000 };
  const latestEpochUs = Number(state.status?.summary?.latestEpochUs || 0);
  const end = latestEpochUs > 0 ? new Date(latestEpochUs / 1000) : new Date();
  const endInput = $("#filter-end");
  endInput.value = toLocalInput(end);
  endInput.setCustomValidity("");
  $("#filter-start").value = toLocalInput(new Date(end.getTime() - units[range]));
  $$("[data-range]").forEach((button) => button.classList.toggle("is-active", button.dataset.range === range));
}

function setDefaultSyncWindow() {
  if ($("#sync-start-time").value && $("#sync-end-time").value) return;
  const now = new Date();
  $("#sync-end-time").value = toLocalInput(now);
  $("#sync-start-time").value = toLocalInput(new Date(now.getTime() - 60 * 60_000));
}

function syncWindowPayload() {
  const startValue = $("#sync-start-time").value;
  const endValue = $("#sync-end-time").value;
  if (!startValue || !endValue) throw new Error("请同时填写 Binlog 开始时间和结束时间");
  const start = new Date(startValue);
  const end = new Date(endValue);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) throw new Error("同步时间格式无效");
  if (start >= end) throw new Error("Binlog 结束时间必须晚于开始时间");
  return { startTimeUtc: start.toISOString(), endTimeUtc: end.toISOString() };
}

function eventQueryString(includePage = true) {
  const params = new URLSearchParams();
  const start = $("#filter-start").value;
  const end = $("#filter-end").value;
  if (start) params.set("startEpochUs", String(new Date(start).getTime() * 1000));
  if (end) params.set("endEpochUs", String(new Date(end).getTime() * 1000));
  if ($("#filter-instance").value) params.set("instance", $("#filter-instance").value);
  if ($("#filter-query-mode").value === "keyword" && $("#filter-keyword").value.trim()) params.set("keyword", $("#filter-keyword").value.trim());
  params.set("keywordMode", $("#filter-keyword-mode").value);
  if ($("#filter-database").value.trim()) params.set("database", $("#filter-database").value.trim());
  if ($("#filter-table").value.trim()) params.set("table", $("#filter-table").value.trim());
  if ($("#filter-source").value) params.set("source", $("#filter-source").value);
  if (state.txnFilter) params.set("transaction", state.txnFilter);
  if ($("#filter-connection").value.trim()) params.set("connection", $("#filter-connection").value.trim());
  if ($("#filter-account").value.trim()) params.set("account", $("#filter-account").value.trim());
  if ($("#filter-status").value) params.set("status", $("#filter-status").value);
  const operations = $$(".operation-filter input:checked").map((item) => item.value);
  if (operations.length) params.set("operation", operations.join(","));
  if (includePage) {
    params.set("limit", String(state.queryLimit));
    params.set("offset", String(state.queryOffset));
  } else {
    params.set("limit", "1000");
    params.set("offset", "0");
  }
  return params.toString();
}

function eventQueryPayload() {
  const params = new URLSearchParams(eventQueryString(true));
  const payload = {
    keyword: params.get("keyword") || "",
    keywordMode: params.get("keywordMode") || "AND",
    instance: params.get("instance") || "",
    database: params.get("database") || "",
    table: params.get("table") || "",
    connection: params.get("connection") || "",
    account: params.get("account") || "",
    status: params.get("status") || "",
    operation: params.get("operation") || "",
    // source 之前漏在这里，选了「来源」走 POST 查询会被丢掉；transaction 是事务钻取。
    source: params.get("source") || "",
    transaction: params.get("transaction") || "",
    limit: Number(params.get("limit") || state.queryLimit),
    offset: Number(params.get("offset") || state.queryOffset),
  };
  if (params.has("startEpochUs")) payload.startEpochUs = Number(params.get("startEpochUs"));
  if (params.has("endEpochUs")) payload.endEpochUs = Number(params.get("endEpochUs"));
  if ($("#filter-query-mode").value === "primary-key") {
    const value = $("#filter-keyword").value.trim();
    if (!payload.database || !payload.table) throw new Error("主键精确查询必须填写完整数据库名和表名");
    if (!value) throw new Error("请填写主键值");
    payload.keyword = "";
    payload.exact = { kind: "PRIMARY_KEY", value, fallback: "scan" };
  }
  return payload;
}

function syncQueryMode() {
  const exact = $("#filter-query-mode").value === "primary-key";
  $("#filter-value-label").textContent = exact ? "主键值" : "关键词";
  $("#filter-keyword").placeholder = exact ? "例如 3521" : "SQL、行值、GTID 或文件名";
  $("#filter-keyword-mode").disabled = exact;
  $("#filter-keyword-mode-field").classList.toggle("is-disabled", exact);
  $("#filter-database").placeholder = exact ? "完整数据库名" : "支持片段匹配";
  $("#filter-table").placeholder = exact ? "完整表名" : "支持片段匹配";
  $("#query-export").disabled = exact;
  $("#query-export").title = exact ? "主键精确结果请在任务列表中查看" : "导出 CSV";
}

function validateEventRange() {
  const startInput = $("#filter-start");
  const endInput = $("#filter-end");
  endInput.setCustomValidity("");
  const start = startInput.value ? new Date(startInput.value) : null;
  const end = endInput.value ? new Date(endInput.value) : null;
  if (start && end && start > end) {
    throw new Error("结束时间必须晚于或等于开始时间");
  }
  const latestEpochUs = Number(state.status?.summary?.latestEpochUs || 0);
  if (end && latestEpochUs > 0 && end.getTime() * 1000 > latestEpochUs) {
    const latestText = formatTime(latestEpochUs);
    const message = `结束时间超出已有数据范围；当前已解析数据只到 ${latestText}`;
    endInput.setCustomValidity(message);
    endInput.reportValidity();
    const error = new Error(message);
    error.code = "QUERY_END_AFTER_LATEST";
    throw error;
  }
}

function operationClass(value) {
  const key = String(value || "other").toLowerCase();
  return ["insert", "update", "delete", "ddl", "query", "transaction"].includes(key) ? key : "query";
}

function executionStatusLabel(value) {
  return ({ success: "成功", failed: "失败", cancelled: "已取消", unknown: "结果未知" })[value] || value || "";
}

function executionStatusClass(value) {
  return ["success", "failed", "cancelled"].includes(value) ? value : "";
}

function epochMicrosText(value) {
  const micros = Number(value || 0);
  return micros > 0 ? formatTime(new Date(micros / 1000).toISOString(), true) : "—";
}

function rowSql(row) {
  // 开了 binlog_rows_query_log_events 之后，行事件会带上产生它的原始语句
  // (RowsQueryLogEvent)，存在 row_query 里。此时 sql_text 是按行镜像重建的
  // `SET @1 = ...` 占位形态，可读性远不如原文，而且操作类型可能与真实语句不符
  // （INSERT ... ON DUPLICATE KEY UPDATE 会记成 UpdateRows）。有原文就用原文。
  // 注意：一条语句影响多行时，这些行事件共用同一条 row_query——列表是行事件
  // 明细，出现重复文本是正常的，执行次数以聚合页的 RowsEvent 边界口径为准。
  if (row.row_query && String(row.sql_kind || "").toUpperCase() !== "ORIGINAL") {
    return row.row_query;
  }
  if (row.sql_text) return row.sql_text;
  if (row.after_json) return row.after_json;
  if (row.before_json) return row.before_json;
  return row.raw_event_type || "Binlog event";
}

function renderEvents(result) {
  const tbody = $("#event-rows");
  tbody.innerHTML = "";
  const rows = result.rows || [];
  $("#event-empty").hidden = rows.length > 0;
  $(".data-table", $("#view-audit")).hidden = rows.length === 0;
  for (const row of rows) {
    const tr = document.createElement("tr");
    tr.tabIndex = 0;
    tr.dataset.eventId = row.event_id;
    const sql = rowSql(row);
    const object = [row.database_name, row.table_name].filter(Boolean).join(".") || "—";
    const transaction = row.transaction_context_id || row.gtid || row.transaction_id || "—";
    const auditEvent = row.raw_event_type === "TABULARIS_AUDIT";
    const sourceLabel = auditEvent ? "本地执行日志" : (row.source_file_name || "—");
    // 来源明细按「有没有」判断，不按事件类型判断：慢日志的 raw_event_type 是
    // SLOW_LOG，之前落到 else 分支去显示 binlog 的 start/end_position（恒为
    // 0 → 0），把已经采到的客户端 IP 与账号盖住了。binlog 事件三个字段都空，
    // 仍然显示位置，行为不变。
    const sourceOrigin =
      [row.connection_name, row.database_account].filter(Boolean).join(" · ") ||
      row.connection_id ||
      "";
    const sourceMeta =
      sourceOrigin || (auditEvent ? "—" : `${row.start_position} → ${row.end_position}`);
    const sqlKind = String(row.sql_kind || "").toUpperCase();
    // 行事件带了 RowsQuery 原文时展示的就是原文，标签不能再写「重建」。
    const hasRowQuery = Boolean(row.row_query) && sqlKind !== "ORIGINAL";
    const sqlLabel = auditEvent
      ? "执行"
      : hasRowQuery
      ? "原始"
      : sqlKind === "PSEUDO"
      ? "重建"
      : sqlKind === "ORIGINAL"
      ? "原始"
      : "事件";
    const status = executionStatusLabel(row.execution_status);
    tr.innerHTML = `
      <td><span class="primary-cell mono">${escapeHtml(formatTime(row.event_time_utc, true))}</span><span class="secondary-cell mono">${escapeHtml(String(row.event_time_utc || "").slice(0, 10))}</span></td>
      <td><span class="primary-cell sql-preview"><span class="kind-label">${escapeHtml(sqlLabel)}</span>${escapeHtml(sql)}</span><span class="secondary-cell">${escapeHtml(row.raw_event_type || "")}</span></td>
      <td><span class="primary-cell">${escapeHtml(object)}</span><span class="secondary-cell">${escapeHtml(row.host_instance_id || "主/备编号未知")}</span></td>
      <td><span class="operation-chip ${operationClass(row.operation)}">${escapeHtml(row.operation || "OTHER")}</span>${status ? `<span class="status-chip audit-${executionStatusClass(row.execution_status)}">${escapeHtml(status)}</span>` : ""}</td>
      <td><span class="primary-cell mono">${escapeHtml(transaction)}</span><span class="secondary-cell">${auditEvent ? escapeHtml(row.connection_id || "连接 ID 未知") : row.thread_id ? `thread ${escapeHtml(row.thread_id)}` : "无连接身份字段"}</span></td>
      <td><span class="primary-cell mono">${escapeHtml(sourceLabel)}</span><span class="secondary-cell mono">${escapeHtml(sourceMeta)}</span></td>
      <td><button class="icon-button row-detail" title="查看详情" aria-label="查看事件详情" type="button">${icons.detail}</button></td>`;
    tr.addEventListener("click", () => openDetail(row.event_id, row.locator, row.instance_id));
    tr.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") openDetail(row.event_id, row.locator, row.instance_id);
    });
    tbody.append(tr);
  }
  state.hasMore = Boolean(result.has_more);
  const page = Math.floor(state.queryOffset / state.queryLimit) + 1;
  $("#page-label").textContent = `第 ${page} 页`;
  $("#page-prev").disabled = state.queryOffset === 0;
  $("#page-next").disabled = !state.hasMore;
  const tierNames = (result.tiers_used || []).map((tier) => (
    tier === "exact-index"
      ? "主键精确索引 · 0 OSS"
      : tier === "slowlog-index"
      ? "慢日志专用索引 · 0 OSS"
      : tier === "local-index"
      ? "本地索引"
      : tier === "oss-range"
      ? "OSS Range"
      : tier === "oss-temporary"
      ? "OSS 临时回退"
      : tier
  ));
  const tierCopy = tierNames.length ? ` · ${tierNames.join(" + ")}` : "";
  if (result.backfill?.message) {
    $("#result-meta").textContent = result.backfill.message;
    if (state.lastBackfillMessage !== result.backfill.message) {
      state.lastBackfillMessage = result.backfill.message;
      toast(result.backfill.message, "info", 6000);
    }
  } else {
    state.lastBackfillMessage = "";
    const exactCopy = result.exact_index_complete === false
      ? " · 精确索引回填中，本次已准确扫描"
      : "";
    $("#result-meta").textContent = rows.length
      ? `本页 ${rows.length} 条 · ${state.queryLimit} 条/页${tierCopy}${exactCopy}`
      : `0 条${tierCopy}${exactCopy}`;
  }
}

function queryTaskStatusLabel(status) {
  return ({
    queued: "排队中",
    running: "查询中",
    cancelling: "停止中",
    succeeded: "已完成",
    failed: "失败",
    cancelled: "已停止",
  })[status] || status || "未知";
}

function queryTaskTitle(task) {
  const query = task.query || {};
  const object = [query.database, query.table].filter(Boolean).join(".");
  if (object && query.exact?.kind === "PRIMARY_KEY") return `${object} · 主键=${query.exact.value}`;
  if (object && query.keyword) return `${object} · ${query.keyword}`;
  return object || query.keyword || "全部事件";
}

function renderQueryTasks(tasks) {
  const list = $("#query-task-list");
  const visible = tasks.slice(0, 8);
  const activeCount = tasks.filter((task) => ["queued", "running", "cancelling"].includes(task.status)).length;
  $("#query-task-summary").textContent = activeCount
    ? `${activeCount} 个进行中 · 最多并行 2 个`
    : tasks.length ? `最近 ${Math.min(tasks.length, 8)} 个` : "暂无任务";
  $("#query-task-empty").hidden = visible.length > 0;
  list.innerHTML = visible.map((task) => {
    const total = Number(task.total_parts || 0);
    const completed = Number(task.completed_parts || 0);
    const percent = total > 0
      ? Math.min(100, Math.round((completed / total) * 100))
      : task.status === "succeeded" ? 100 : 0;
    const running = ["queued", "running", "cancelling"].includes(task.status);
    const estimate = Number(task.estimated_bytes || 0);
    const progress = total > 0
      ? `${humanCount(completed)} / ${humanCount(total)} 分区`
      : task.status === "queued"
      ? "等待工作线程"
      : task.status === "succeeded"
      ? "0 个候选分区"
      : task.status === "running"
      ? "正在生成查询计划"
      : task.message || "未读取分区";
    const scan = estimate > 0 ? `预计最多 ${humanBytes(estimate)}` : "本地索引/缓存路径";
    return `
      <article class="query-task ${task.id === state.activeQueryTaskId ? "is-active" : ""}" data-status="${escapeHtml(task.status)}">
        <button class="query-task-main" type="button" data-query-task-open="${escapeHtml(task.id)}">
          <span class="query-task-copy"><strong>${escapeHtml(queryTaskTitle(task))}</strong><small>${escapeHtml(formatTime(task.created_at))} · ${escapeHtml(progress)} · ${escapeHtml(scan)}</small></span>
          <span class="query-task-state"><span class="status-chip query-${escapeHtml(task.status)}">${escapeHtml(queryTaskStatusLabel(task.status))}</span><small>${percent}%</small></span>
          <progress class="query-task-progress" value="${percent}" max="100" aria-label="查询进度 ${percent}%"></progress>
        </button>
        ${running ? `<button class="button danger compact query-task-cancel" data-query-task-cancel="${escapeHtml(task.id)}" type="button">停止</button>` : ""}
      </article>`;
  }).join("");
  $$('[data-query-task-open]', list).forEach((button) => button.addEventListener("click", () => loadQueryTask(button.dataset.queryTaskOpen)));
  $$('[data-query-task-cancel]', list).forEach((button) => button.addEventListener("click", () => cancelQueryTask(button.dataset.queryTaskCancel, button)));
}

async function loadQueryTask(taskId) {
  state.activeQueryTaskId = taskId;
  renderQueryTasks(state.queryTasks);
  const task = await api(`/api/query-task?id=${encodeURIComponent(taskId)}`);
  state.activeQuery = task.query || null;
  state.queryOffset = Number(task.query?.offset || 0);
  state.queryLimit = Number(task.query?.limit || 100);
  if (task.status === "succeeded" && task.result) {
    renderEvents(task.result);
    state.renderedQueryTaskId = task.id;
    return;
  }
  const total = Number(task.total_parts || 0);
  const completed = Number(task.completed_parts || 0);
  const progress = total > 0 ? `${humanCount(completed)} / ${humanCount(total)} 分区` : "等待查询计划";
  $("#result-meta").textContent = `${queryTaskStatusLabel(task.status)} · ${progress}${task.message ? ` · ${task.message}` : ""}`;
}

async function cancelQueryTask(taskId, button) {
  if (button) button.disabled = true;
  try {
    const result = await api("/api/query-task/cancel", {
      method: "POST",
      body: JSON.stringify({ id: taskId }),
    });
    toast(result.requested ? "已发送停止请求" : "任务已经结束", "info");
    await refreshQueryTasks();
  } catch (error) {
    toast(error.message, "error");
  } finally {
    if (button?.isConnected) button.disabled = false;
  }
}

async function refreshQueryTasks() {
  if (state.queryTaskRefreshInFlight) return;
  state.queryTaskRefreshInFlight = true;
  try {
    const tasks = await api("/api/query-tasks?limit=50");
    state.queryTasks = tasks;
    if (!state.activeQueryTaskId && tasks.length) state.activeQueryTaskId = tasks[0].id;
    renderQueryTasks(tasks);
    const active = tasks.find((task) => task.id === state.activeQueryTaskId);
    if (!active) return;
    if (active.status === "succeeded" && state.renderedQueryTaskId !== active.id) {
      await loadQueryTask(active.id);
    } else if (["queued", "running", "cancelling"].includes(active.status)) {
      const total = Number(active.total_parts || 0);
      const completed = Number(active.completed_parts || 0);
      const progress = total > 0 ? `${humanCount(completed)} / ${humanCount(total)} 分区` : "等待查询计划";
      $("#result-meta").textContent = `${queryTaskStatusLabel(active.status)} · ${progress}${Number(active.estimated_bytes || 0) > 0 ? ` · 预计最多读取 ${humanBytes(active.estimated_bytes)}` : ""}`;
    } else if (["failed", "cancelled"].includes(active.status)) {
      $("#result-meta").textContent = `${queryTaskStatusLabel(active.status)} · ${active.message || "未返回结果"}`;
      if (!state.notifiedQueryTasks.has(active.id)) {
        state.notifiedQueryTasks.add(active.id);
        toast(active.message || queryTaskStatusLabel(active.status), active.status === "failed" ? "error" : "info");
      }
    }
  } catch (error) {
    $("#query-task-summary").textContent = "任务状态读取失败";
  } finally {
    state.queryTaskRefreshInFlight = false;
  }
}

async function runQuery(queryOverride = null) {
  if (state.queryPromise) return state.queryPromise;
  if (!queryOverride) validateEventRange();
  const button = $("#query-submit");
  state.queryPromise = withBusy(button, async () => {
    const query = queryOverride || eventQueryPayload();
    const created = await api("/api/query-tasks", {
      method: "POST",
      body: JSON.stringify(query),
    });
    state.activeQueryTaskId = created.taskId;
    state.activeQuery = query;
    state.renderedQueryTaskId = "";
    renderEvents({ rows: [], has_more: false, tiers_used: [] });
    $("#result-meta").textContent = `任务 ${created.taskId.slice(0, 8)} 已创建 · 等待查询计划`;
    await refreshQueryTasks();
    return created;
  }, "创建中…");
  try {
    return await state.queryPromise;
  } finally {
    state.queryPromise = null;
  }
}

function prettyJson(value) {
  if (!value) return "—";
  try { return JSON.stringify(JSON.parse(value), null, 2); } catch { return String(value); }
}

async function openDetail(eventId, locator = "", instance = "") {
  const drawer = $("#detail-drawer");
  $("#detail-body").innerHTML = '<div class="empty-state"><strong>正在读取事件…</strong></div>';
  drawer.classList.add("is-open");
  drawer.setAttribute("aria-hidden", "false");
  $("#drawer-backdrop").hidden = false;
  try {
    const row = await api(`/api/event?id=${encodeURIComponent(eventId)}&locator=${encodeURIComponent(locator)}&instance=${encodeURIComponent(instance)}`);
    $("#detail-title").textContent = `${row.operation || "EVENT"} · ${row.database_name || "—"}.${row.table_name || "—"}`;
    const auditEvent = row.raw_event_type === "TABULARIS_AUDIT";
    const slowEvent = row.raw_event_type === "SLOW_LOG";
    const normalizedKind = String(row.sql_kind || "").toUpperCase();
    const sqlKind = auditEvent ? "本地执行原始文本" : slowEvent ? "RDS 慢日志原始 SQL" : normalizedKind === "PSEUDO" ? "根据行镜像重建，不可直接回放" : normalizedKind === "ORIGINAL" ? "Binlog 携带的原始文本" : "无 SQL 文本";
    const auditDetails = auditEvent ? `
      <div class="detail-grid audit-detail-grid">
        ${detailMeta("执行状态", executionStatusLabel(row.execution_status))}
        ${detailMeta("连接", row.connection_name || row.connection_id)}
        ${detailMeta("连接 ID", row.connection_id)}
        ${detailMeta("数据库账号", row.database_account)}
        ${detailMeta("开始时间", epochMicrosText(row.started_epoch_us))}
        ${detailMeta("结束时间", epochMicrosText(row.finished_epoch_us))}
        ${detailMeta("执行耗时", `${Number(row.execution_time_ms || 0).toLocaleString()} ms`)}
        ${detailMeta("影响行数", Number(row.affected_rows || 0).toLocaleString())}
        ${detailMeta("Batch ID", row.batch_id)}
        ${detailMeta("语句序号", Number(row.statement_index) >= 0 ? Number(row.statement_index).toLocaleString() : "—")}
        ${detailMeta("事务上下文", row.transaction_context_id)}
        ${detailMeta("来源", "本地执行日志")}
      </div>
      ${row.error_message ? detailBlock("错误信息", row.error_message) : ""}
    ` : "";
    const slowDetails = slowEvent ? `
      <div class="detail-grid audit-detail-grid">
        ${detailMeta("实际扫描行数", Number(row.rows_examined || 0).toLocaleString())}
        ${detailMeta("返回行数", Number(row.rows_sent || 0).toLocaleString())}
        ${detailMeta("查询耗时", millisText(row.execution_time_ms))}
        ${detailMeta("锁等待", millisText(row.lock_time_ms))}
        ${detailMeta("SQL ID", row.sql_id)}
        ${detailMeta("Node ID", row.node_id)}
        ${detailMeta("数据库账号", row.database_account)}
        ${detailMeta("客户端 IP", row.connection_name)}
        ${detailMeta("Thread", row.thread_id ? String(row.thread_id) : "—")}
        ${detailMeta("来源", "RDS 慢日志")}
      </div>
    ` : "";
    $("#detail-body").innerHTML = `
      <div class="detail-grid">
        ${detailMeta("事件时间 UTC", row.event_time_utc)}
        ${detailMeta("操作", row.operation)}
        ${detailMeta("GTID / 事务", row.transaction_context_id || row.gtid || row.transaction_id || "—")}
        ${detailMeta("XID", row.xid || "—")}
        ${detailMeta("Server / Thread", auditEvent ? "本地客户端" : slowEvent ? `RDS 慢日志 / ${row.thread_id || "—"}` : `${row.server_id || 0} / ${row.thread_id || 0}`)}
        ${detailMeta("位置", auditEvent ? "客户端执行事件" : slowEvent ? (row.source_file_name || "RDS 慢日志") : `${row.source_file_name || "—"} : ${row.start_position} → ${row.end_position}`)}
      </div>
      ${auditDetails}
      ${slowDetails}
      ${detailBlock(`SQL · ${sqlKind}`, row.sql_text || row.row_query || "—")}
      ${auditEvent || slowEvent ? "" : detailBlock("变更前 BEFORE", prettyJson(row.before_json))}
      ${auditEvent || slowEvent ? "" : detailBlock("变更后 AFTER", prettyJson(row.after_json))}
      ${auditEvent || slowEvent ? "" : detailBlock("列信息", prettyJson(row.columns_json))}
      ${detailBlock("事件 ID", row.event_id)}
    `;
  } catch (error) {
    $("#detail-body").innerHTML = `<div class="empty-state"><strong>读取失败</strong><span>${escapeHtml(error.message)}</span></div>`;
  }
}

function detailMeta(label, value) {
  return `<div class="detail-meta"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value || "—")}</strong></div>`;
}

function detailBlock(title, content) {
  return `<section class="detail-block"><h3>${escapeHtml(title)}</h3><pre class="code-block">${escapeHtml(content || "—")}</pre></section>`;
}

function closeDetail() {
  $("#detail-drawer").classList.remove("is-open");
  $("#detail-drawer").setAttribute("aria-hidden", "true");
  $("#drawer-backdrop").hidden = true;
}

function renderJobStatus(job) {
  const value = job?.status || "unknown";
  const labels = { running: "运行中", success: "完成", failed: "失败", paused: "已暂停", warning: "有警告" };
  return `<span class="status-chip ${escapeHtml(value)}">${labels[value] || escapeHtml(value)}</span>`;
}

function humanDuration(input) {
  const seconds = Math.max(0, Number(input || 0));
  if (!Number.isFinite(seconds)) return "—";
  const minutes = Math.max(1, Math.round(seconds / 60));
  const days = Math.floor(minutes / 1440);
  const hours = Math.floor((minutes % 1440) / 60);
  const restMinutes = minutes % 60;
  if (days) return `${days} 天 ${hours} 小时`;
  if (hours) return `${hours} 小时 ${restMinutes} 分钟`;
  return `${restMinutes} 分钟`;
}

function rateText(value) {
  const rate = Number(value);
  return Number.isFinite(rate) ? rate.toFixed(1) : "—";
}

function renderSyncPerformance(performance, running = false) {
  const speed = Number(performance?.seconds_per_file);
  const sampleSize = Number(performance?.completion_sample_size || 0);
  const processRate = Number(performance?.processing_files_per_hour);
  const sourceRate = Number(performance?.source_files_per_hour);
  const stateValue = performance?.state || "warming_up";
  const knownStates = ["warming_up", "available", "not_catching_up", "checking_latest", "live_following", "caught_up"];
  const metricState = knownStates.includes(stateValue) ? stateValue : "warming_up";

  if (Number.isFinite(speed) && speed > 0) {
    $("#active-job-speed").textContent = `${speed < 100 ? speed.toFixed(1) : Math.round(speed)} 秒 / Binlog`;
    $("#active-job-speed-note").textContent =
      `近 ${humanCount(sampleSize)} 个完整发布样本 · ${rateText(processRate)} 个/小时`;
  } else {
    $("#active-job-speed").textContent = "计算中";
    $("#active-job-speed-note").textContent = "至少完成 4 个 Binlog 后生成稳定速度";
  }

  if (metricState === "available") {
    const backlog = Math.ceil(Number(performance.estimated_backlog_files || 0));
    $("#active-job-eta").textContent = formatTime(performance.estimated_catch_up_at_utc);
    $("#active-job-eta-note").textContent =
      `约 ${humanDuration(performance.estimated_remaining_seconds)} · 已确认待处理 ${humanCount(backlog)} 个 Completed Binlog`;
  } else if (metricState === "not_catching_up") {
    $("#active-job-eta").textContent = "按当前速度无法追平";
    $("#active-job-eta-note").textContent =
      `当前处理 ${rateText(processRate)} 个/小时，实例新增 ${rateText(sourceRate)} 个/小时`;
  } else if (metricState === "caught_up") {
    $("#active-job-eta").textContent = "已追平";
    $("#active-job-eta-note").textContent = "等待新的 Completed Binlog";
  } else if (metricState === "live_following") {
    $("#active-job-eta").textContent = "已追平 · 跟随最新";
    $("#active-job-eta-note").textContent = "正在处理最新 Binlog；队列无历史积压，完成后等待新的 Completed Binlog";
  } else if (metricState === "checking_latest") {
    $("#active-job-eta").textContent = "正在确认最新文件";
    $("#active-job-eta-note").textContent = "当前清单已处理完，正在向 RDS API 确认是否有新的 Completed Binlog";
  } else {
    $("#active-job-eta").textContent = "计算中";
    $("#active-job-eta-note").textContent = running
      ? "正在积累完整发布与实例生成速率样本"
      : "同步启动后开始估算";
  }
}

function renderJobs(jobs) {
  const list = $("#job-list");
  list.innerHTML = "";
  $("#job-empty").hidden = jobs.length > 0;
  for (const job of jobs) {
    const item = document.createElement("article");
    item.className = "job-item";
    const events = (job.events || []).map((event) => `<li><span>${escapeHtml(formatTime(event.created_at, true))}</span> ${escapeHtml(event.message)}</li>`).join("");
    const requestedRange = job.requested_start_utc && job.requested_end_utc
      ? ` · ${formatTime(job.requested_start_utc, true)} → ${formatTime(job.requested_end_utc, true)}`
      : "";
    item.innerHTML = `
      <div class="job-main">${renderJobStatus(job)}<span>${escapeHtml(job.kind)} · ${escapeHtml(job.id.slice(0, 8))}${escapeHtml(requestedRange)}</span></div>
      <div class="job-time"><strong>${escapeHtml(formatTime(job.started_at))}</strong><br>${job.finished_at ? escapeHtml(formatTime(job.finished_at)) : "尚未结束"}</div>
      <details class="job-message"><summary>${escapeHtml(job.message || "—")}</summary>${events ? `<ul class="job-events">${events}</ul>` : ""}</details>
      <div class="job-count">${humanCount(job.completed_files)} / ${humanCount(job.total_files)} 文件${job.failed_files ? `<br><span class="danger-text">${job.failed_files} 失败</span>` : ""}</div>`;
    list.append(item);
  }
  const active = jobs.find((job) => job.status === "running");
  $("#active-job").hidden = !active;
  if (active) {
    $("#active-job-file").textContent = active.current_file || "核验 RDS 最新 Completed Binlog";
    $("#active-job-count").textContent = `${active.completed_files} / ${active.total_files}`;
    const percent = active.total_files ? Math.min(100, (active.completed_files / active.total_files) * 100) : 8;
    $("#active-job-progress").style.width = `${percent}%`;
    $("#active-job-message").textContent = active.message || "正在运行";
    renderSyncPerformance(state.status?.sync?.latestJob?.performance, true);
  }
}

async function refreshJobs() {
  try { renderJobs(await api("/api/jobs")); } catch (error) { toast(error.message, "error"); }
}

function renderStorage(data) {
  const index = data.index || {};
  const catalog = data.catalog || {};
  $("#storage-local").textContent = humanBytes(data.local_parquet_bytes);
  $("#storage-oss").textContent = data.oss_enabled ? humanBytes(data.archived_bytes) : "未启用";
  $("#storage-catalog").textContent = data.part_count
    ? `${((Number(catalog.cataloged_parts || 0) / Number(data.part_count)) * 100).toFixed(2)}%`
    : "100%";
  $("#storage-cap").textContent = data.part_count
    ? `${((Number(index.part_count || 0) / Number(data.part_count)) * 100).toFixed(2)}%`
    : "100%";
  $("#storage-query-cache").textContent = humanCount(index.block_count || 0);
  $("#storage-metadata").textContent = humanBytes(data.metadata_bytes);
  $("#storage-downloads").textContent = humanBytes(data.download_bytes);
  $("#storage-parts").textContent = humanCount(data.part_count);
  $("#storage-retention-copy").textContent = data.oss_enabled
    ? `本地索引 ${humanBytes(index.size_bytes || 0)}；正文不做 LRU；OSS 对象保留 ${data.oss_retention_days} 天。`
    : `本地数据保留 ${data.retention_days} 天；OSS 归档未启用。`;
  const tbody = $("#storage-rows");
  tbody.innerHTML = "";
  const parts = data.parts || [];
  $("#storage-empty").hidden = parts.length > 0;
  $(".data-table", $("#view-storage")).hidden = parts.length === 0;
  for (const part of parts) {
    const tr = document.createElement("tr");
    const location = part.local_present && part.archive_present
      ? "本地 + OSS"
      : part.local_present
      ? "仅本地"
      : part.archive_present
      ? "仅 OSS"
      : "不可用";
    const locationClass = part.local_present || part.archive_present ? "success" : "failed";
    tr.innerHTML = `
      <td><span class="primary-cell mono">${escapeHtml(part.event_date)}</span></td>
      <td><span class="primary-cell mono">${escapeHtml(part.log_file_name)}</span><span class="secondary-cell mono">${escapeHtml(String(part.path).split(/[\\/]/).pop())}</span></td>
      <td>${humanCount(part.row_count)}</td>
      <td>${humanBytes(part.size_bytes)}</td>
      <td><span class="primary-cell">${escapeHtml(formatTime(Number(part.min_event_epoch_us)))}</span><span class="secondary-cell">至 ${escapeHtml(formatTime(Number(part.max_event_epoch_us)))}</span></td>
      <td><span class="status-chip ${locationClass}">${escapeHtml(location)}</span></td>
      <td><span class="primary-cell mono">${escapeHtml(part.sha256)}</span></td>`;
    tbody.append(tr);
  }
}

async function refreshStorage() {
  try { renderStorage(await api("/api/storage")); } catch (error) { toast(error.message, "error"); }
}

function fillSettings(data) {
  state.settings = data;
  $("#setting-instance").value = data.dbInstanceId || "";
  $("#setting-region").value = data.regionId || "";
  $("#setting-endpoint").value = data.endpoint || "";
  $("#setting-lookback").value = data.initialLookbackDays || 60;
  $("#setting-retention").value = data.retentionDays || 60;
  $("#setting-poll").value = data.pollMinutes || 5;
  $("#setting-auto").checked = Boolean(data.autoSync);
  $("#setting-intranet").checked = true;
  $("#setting-intranet").disabled = true;
  $("#setting-oss-enabled").checked = Boolean(data.ossEnabled);
  $("#setting-oss-bucket").value = data.ossBucket || "";
  $("#setting-oss-region").value = data.ossRegionId || "";
  $("#setting-oss-endpoint").value = data.ossEndpoint || "";
  $("#setting-oss-prefix").value = data.ossPrefix || "";
  $("#setting-oss-auth").value = data.ossAuthMode || "ecs_ram_role";
  $("#setting-oss-role").value = data.ossRoleName || "";
  $("#setting-oss-retention").value = data.ossRetentionDays || 60;
  const credential = data.credential || {};
  $("#credential-state").innerHTML = credential.present
    ? `<span class="state-dot"></span><span>${escapeHtml(credential.maskedAccessKeyId)} · ${escapeHtml(credential.source)}</span>`
    : '<span class="state-dot neutral"></span><span>未检测到凭据</span>';
}

async function refreshSettings() {
  try { fillSettings(await api("/api/settings")); } catch (error) { toast(error.message, "error"); }
}

function settingsPayload() {
  return {
    dbInstanceId: $("#setting-instance").value.trim(),
    regionId: $("#setting-region").value.trim(),
    endpoint: $("#setting-endpoint").value.trim(),
    initialLookbackDays: Number($("#setting-lookback").value),
    retentionDays: Number($("#setting-retention").value),
    pollMinutes: Number($("#setting-poll").value),
    autoSync: $("#setting-auto").checked,
    preferIntranetDownload: true,
    ossEnabled: $("#setting-oss-enabled").checked,
    ossBucket: $("#setting-oss-bucket").value.trim(),
    ossRegionId: $("#setting-oss-region").value.trim(),
    ossEndpoint: $("#setting-oss-endpoint").value.trim(),
    ossPrefix: $("#setting-oss-prefix").value.trim(),
    ossAuthMode: $("#setting-oss-auth").value,
    ossRoleName: $("#setting-oss-role").value.trim(),
    ossRetentionDays: Number($("#setting-oss-retention").value),
    accessKeyId: $("#setting-ak-id").value.trim(),
    accessKeySecret: $("#setting-ak-secret").value,
    securityToken: $("#setting-token").value,
  };
}

async function saveSettings() {
  const button = $("#save-settings");
  await withBusy(button, async () => {
    const data = await api("/api/settings", { method: "POST", body: JSON.stringify(settingsPayload()) });
    $("#setting-ak-id").value = "";
    $("#setting-ak-secret").value = "";
    $("#setting-token").value = "";
    fillSettings(data);
    toast("设置已保存；阿里云凭据未写入配置文件", "success");
    await refreshStatus();
  }, "保存中…");
}

async function downloadExport() {
  if ($("#filter-query-mode").value === "primary-key") {
    throw new Error("主键精确结果请直接在查询任务中查看详情");
  }
  const button = $("#query-export");
  await withBusy(button, async () => {
    const response = await fetch(`/api/export?${eventQueryString(false)}`, {
      cache: "no-store",
    });
    if (!response.ok) {
      let message = `导出失败（HTTP ${response.status}）`;
      try {
        const payload = await response.json();
        message = payload?.error?.message || message;
      } catch {
        // Keep the HTTP fallback when the server did not return JSON.
      }
      throw new Error(message);
    }
    const blob = await response.blob();
    const disposition = response.headers.get("Content-Disposition") || "";
    const match = disposition.match(/filename\*=UTF-8''([^;]+)/i);
    const filename = match
      ? decodeURIComponent(match[1])
      : `binlog-events-${new Date().toISOString().slice(0, 10)}.csv`;
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename;
    document.body.append(anchor);
    anchor.click();
    anchor.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 1000);
    toast(`CSV 已生成：${filename}`, "success");
  }, "导出中…");
}

async function startSync(button = $("#start-sync")) {
  await withBusy(button, async () => {
    const range = syncWindowPayload();
    const result = await api("/api/sync/start", { method: "POST", body: JSON.stringify(range) });
    toast(`同步任务已启动：${result.jobId.slice(0, 8)}`, "success");
    switchView("jobs");
    await Promise.all([refreshJobs(), refreshStatus()]);
  }, "启动中…");
}

function renderSecondarySyncs(items) {
  const section = $("#secondary-section");
  const list = $("#secondary-list");
  if (!section || !list) return;
  if (!items.length) {
    section.hidden = true;
    list.textContent = "";
    return;
  }
  section.hidden = false;
  list.innerHTML = items
    .map((item) => {
      const sync = item.sync || {};
      const job = sync.latestJob || {};
      const perf = job.performance || {};
      const running = Boolean(sync.running);
      const state = perf.state || "";
      const failed = job.status === "failed";
      const chip = failed
        ? '<span class="status-chip query-failed">失败</span>'
        : running
        ? '<span class="status-chip running">同步中</span>'
        : state === "caught_up" || state === "live_following"
        ? '<span class="status-chip query-succeeded">已追平</span>'
        : '<span class="status-chip query-queued">待命</span>';
      const remaining = Number(perf.known_remaining_files || 0);
      const meta = [
        escapeHtml(item.instanceId || ""),
        remaining > 0 ? `剩余 ${humanCount(remaining)} 个 Binlog` : "",
        job.started_at ? `最近任务 ${escapeHtml(formatTime(job.started_at))}` : "",
      ].filter(Boolean).join(" · ");
      return `<div class="secondary-item">
        <div class="secondary-head">${chip}<strong>${escapeHtml(item.label || item.instanceId || "—")}</strong></div>
        <div class="secondary-meta">${meta}</div>
        <div class="secondary-message">${escapeHtml((job.message || "").slice(0, 160) || "尚未运行")}</div>
      </div>`;
    })
    .join("");
}

function instanceLabels(status) {
  const labels = new Map();
  const sources = [
    ...(status.generalLogs || []),
    ...(status.slowLogs || []),
    ...(status.secondarySyncs || []),
  ];
  for (const item of sources) {
    const id = String(item.instanceId || item.instance_id || "").trim();
    const label = String(item.label || "").trim();
    if (id && label) labels.set(id, label);
  }
  return labels;
}

function syncInstanceOptions(instances, labels = new Map()) {
  // 分析洞察与审计查询共用同一份实例来源，有选有切换。
  [$("#analytics-instance"), $("#filter-instance")].forEach((select) => {
    if (!select) return;
    const known = new Set(Array.from(select.options).map((item) => item.value));
    instances.forEach((id) => {
      if (known.has(id)) return;
      const option = document.createElement("option");
      option.value = id;
      option.textContent = labels.get(id) || id;
      select.appendChild(option);
    });
  });
}

function syncSlowLogNodeOptions(slowLogs) {
  const list = $("#analytics-node-options");
  if (!list) return;
  const nodes = new Map();
  for (const item of slowLogs || []) {
    const nodeId = String(item.nodeId || "").trim();
    if (!nodeId || nodes.has(nodeId)) continue;
    nodes.set(nodeId, item.label || item.instanceId || nodeId);
  }
  list.innerHTML = [...nodes.entries()]
    .map(([nodeId, label]) => `<option value="${escapeHtml(nodeId)}" label="${escapeHtml(label)}"></option>`)
    .join("");
}

async function refreshStatus() {
  if (state.statusRefreshInFlight) return;
  state.statusRefreshInFlight = true;
  try {
    const data = await api("/api/status");
    state.status = data;
    syncInstanceOptions(data.instances || [], instanceLabels(data));
    syncSlowLogNodeOptions(data.slowLogs || []);
    $("#app-version").textContent =
      `v${data.version || "—"} · DYNAMIC CPU + FAST ZSTD + SAFE DIRECT OSS`;
    const summary = data.summary || {};
    $("#summary-events").textContent = humanCount(summary.eventCount);
    $("#summary-latest").textContent = summary.latestEpochUs ? formatTime(Number(summary.latestEpochUs), true) : "暂无数据";
    $("#summary-storage").textContent = humanBytes(summary.indexBytes);
    const running = Boolean(data.sync?.running);
    const latest = data.sync?.latestJob;
    const performanceState = latest?.performance?.state || "warming_up";
    const caughtUp = performanceState === "caught_up";
    const checkingLatest = performanceState === "checking_latest";
    const liveFollowing = performanceState === "live_following";
    renderSyncPerformance(latest?.performance, running);
    $("#summary-sync").textContent = running
      ? liveFollowing ? "已追平" : checkingLatest ? "核验最新" : "正在同步"
      : latest?.status === "failed" ? "失败待处理"
      : caughtUp ? "已追平"
      : data.configured ? "已到断点" : "待配置";
    $("#summary-sync").style.color = latest?.status === "failed" ? "var(--danger)" : running ? "var(--accent)" : "var(--teal)";
    $("#running-dot").hidden = !running;
    $("#setup-banner").hidden = data.configured;
    $("#sync-top").disabled = running;
    $("#start-sync").disabled = running;
    $("#pause-sync").disabled = !running;
    $("#service-meta").textContent = running
      ? liveFollowing ? "正在处理最新 Binlog，队列无历史积压"
      : checkingLatest ? "正在确认最新 Completed Binlog" : "后台顺序任务运行中"
      : caughtUp ? "等待新的 Completed Binlog"
      : `本地索引 · ${humanCount(summary.indexBlocks || 0)} 块`;
    $("#retention-top").textContent =
      `OSS ${summary.ossRetentionDays || 60} 天 · 目录 ${((Number(summary.catalogCoverage || 0)) * 100).toFixed(1)}% · 索引 ${((Number(summary.indexCoverage || 0)) * 100).toFixed(1)}%`;
    renderSecondarySyncs(data.secondarySyncs || []);
    if (state.view === "jobs") refreshJobs();
  } catch (error) {
    $("#service-indicator").style.background = "var(--danger)";
    $("#service-label").textContent = "本地服务异常";
    $("#service-meta").textContent = error.message;
  } finally {
    state.statusRefreshInFlight = false;
  }
}

function humanMicros(input) {
  const micros = Math.max(0, Number(input || 0));
  if (!Number.isFinite(micros) || micros === 0) return "0 ms";
  if (micros < 1000) return `${micros} µs`;
  if (micros < 1_000_000) return `${(micros / 1000).toFixed(micros < 10_000 ? 2 : 0)} ms`;
  const seconds = micros / 1_000_000;
  if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 2 : 1)} s`;
  // 先把秒取整再拆分，否则 239.952 秒会显示成「3 分 60 秒」。
  const totalSeconds = Math.round(seconds);
  const minutes = Math.floor(totalSeconds / 60);
  if (minutes < 60) return `${minutes} 分 ${totalSeconds % 60} 秒`;
  const totalMinutes = Math.round(totalSeconds / 60);
  return `${Math.floor(totalMinutes / 60)} 小时 ${totalMinutes % 60} 分`;
}

function statTiles(items) {
  return `<div class="stat-tiles">${items
    .map((item) => {
      const drill = item.drill ? ` data-txn-drill="${escapeHtml(item.drill)}"` : "";
      const tag = item.drill ? "button" : "div";
      const cls = item.drill ? "stat-tile is-clickable" : "stat-tile";
      const attrs = item.drill ? ' type="button"' : "";
      return `<${tag} class="${cls}"${drill}${attrs}><span>${escapeHtml(item.label)}</span><strong>${escapeHtml(item.value)}</strong>${item.hint ? `<em>${escapeHtml(item.hint)}</em>` : ""}${item.drill ? '<i class="drill-hint">查看明细 ›</i>' : ""}</${tag}>`;
    })
    .join("")}</div>`;
}

function barList(rows, { valueKey = "count", labelKey = "label", format = humanCount } = {}) {
  const max = rows.reduce((peak, row) => Math.max(peak, Number(row[valueKey] || 0)), 0);
  if (!rows.length) return '<p class="analytics-empty">暂无数据</p>';
  return `<div class="bar-list">${rows
    .map((row) => {
      const value = Number(row[valueKey] || 0);
      const width = max > 0 ? Math.max((value / max) * 100, value > 0 ? 2 : 0) : 0;
      return `<div class="bar-row"><span class="bar-label">${escapeHtml(row[labelKey])}</span><span class="bar-track"><span class="bar-fill" style="width:${width.toFixed(2)}%"></span></span><span class="bar-value">${escapeHtml(format(value))}</span></div>`;
    })
    .join("")}</div>`;
}

function sparkline(points, { valueKey = "events" } = {}) {
  const values = points.map((point) => Number(point[valueKey] || 0));
  if (values.length < 2) return '<p class="analytics-empty">时间点不足，无法绘制趋势</p>';
  const max = Math.max(...values, 1);
  const width = 1000;
  const height = 120;
  const step = width / (values.length - 1);
  const coords = values.map((value, index) => [index * step, height - (value / max) * (height - 12) - 6]);
  const line = coords.map(([x, y], index) => `${index === 0 ? "M" : "L"}${x.toFixed(1)} ${y.toFixed(1)}`).join(" ");
  const area = `${line} L${width} ${height} L0 ${height} Z`;
  const first = points[0];
  const last = points[points.length - 1];
  return `<div class="spark">
    <svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" role="img" aria-label="时间趋势">
      <path class="spark-area" d="${area}"></path>
      <path class="spark-line" d="${line}"></path>
    </svg>
    <div class="spark-axis"><span>${escapeHtml(formatTime(Number(first.ts), true))}</span><span>峰值 ${escapeHtml(humanCount(max))}</span><span>${escapeHtml(formatTime(Number(last.ts), true))}</span></div>
  </div>`;
}

function analyticsTable(headers, rows, emptyText = "暂无数据") {
  if (!rows.length) return `<p class="analytics-empty">${escapeHtml(emptyText)}</p>`;
  return `<div class="table-wrap"><table class="data-table analytics-table">
    <thead><tr>${headers.map((item) => `<th>${escapeHtml(item)}</th>`).join("")}</tr></thead>
    <tbody>${rows.map((cells) => `<tr>${cells.map((cell) => `<td>${cell}</td>`).join("")}</tr>`).join("")}</tbody>
  </table></div>`;
}

const SOURCE_LABELS = {
  original: ["原始 SQL", "QueryEvent 携带的真实语句文本"],
  "rows-query": ["原始 SQL", "RowsQueryEvent 携带的真实语句文本"],
  reconstructed: ["重建 SQL", "按行镜像生成，仅供阅读，不是真实语句文本，也不能用于回放"],
  synthetic: ["合成模板", "行事件既无语句文本也无 RowsQuery，按操作与对象合成"],
  boundary: ["事务边界", "BEGIN / COMMIT 等边界事件"],
  slowlog: ["慢日志实测", "RDS 慢日志中的原始 SQL 与实际执行指标"],
};

function sourceChip(kind) {
  const [label, hint] = SOURCE_LABELS[kind] || ["未知", ""];
  const className = kind === "original" || kind === "rows-query" ? "chip" : "chip warn";
  return `<span class="${className}" title="${escapeHtml(hint)}">${escapeHtml(label)}</span>`;
}

function objectLabel(database, table) {
  const value = [database, table].filter(Boolean).join(".");
  return value || "—";
}

const SQL_ORDER_LABELS = [
  ["executions", "执行次数"],
  ["row_events", "影响行数"],
  ["scan_rows", "扫描行数(估)"],
  ["events", "变更事件数"],
  ["exec_time", "慢语句耗时"],
  ["recent", "最近出现"],
];

function sourceMark(kind) {
  // 来源不再单独占一列，但重建/合成的语句必须仍然可辨认，否则会被误读成
  // 数据库真实执行过的语句文本。
  if (kind === "original" || kind === "rows-query") return "";
  const [label, hint] = SOURCE_LABELS[kind] || ["未知", ""];
  return `<span class="src-mark" title="${escapeHtml(hint)}">${escapeHtml(label)}</span>`;
}

function shareBar(value, total) {
  const ratio = Number(total) > 0 ? (Number(value) / Number(total)) * 100 : 0;
  return `<span class="share"><span class="share-track"><span class="share-fill" style="width:${Math.min(ratio, 100).toFixed(2)}%"></span></span><span class="share-text">${ratio.toFixed(ratio < 10 ? 1 : 0)}%</span></span>`;
}

const SLOWLOG_ORDER_LABELS = [
  ["executions", "执行次数"],
  ["scan_rows", "实际扫描行数"],
  ["row_events", "返回行数"],
  ["exec_time", "累计耗时"],
  ["recent", "最近出现"],
];

function millisText(value) {
  return humanMicros(Math.max(0, Number(value || 0)) * 1000);
}

function slowSqlDetailCell(item, order) {
  const sql = item.normalized_sql || item.sample_sql || "—";
  const body = `${sourceMark("slowlog")}${escapeHtml(sql)}`;
  const eventId = order === "scan_rows"
    ? (item.max_scan_event_id || item.sample_event_id)
    : (order === "exec_time"
      ? (item.max_query_event_id || item.sample_event_id)
      : item.sample_event_id);
  const sampleLabel = order === "scan_rows"
    ? "最大扫描样本"
    : (order === "exec_time" ? "最大耗时样本" : "代表样本");
  if (!eventId) {
    return `<code class="sql-cell" title="${escapeHtml(item.sample_sql || "")}">${body}</code>`;
  }
  return `<button class="slow-sql-link" type="button" data-slow-event-id="${escapeHtml(eventId)}" data-slow-instance="${escapeHtml(item.instance_id || "")}" title="查看该慢 SQL 的${sampleLabel}明细" aria-label="查看慢 SQL ${sampleLabel}：${escapeHtml(sql)}"><code class="sql-cell">${body}</code></button>`;
}

function renderAnalyticsSlowlogSql(data) {
  const totals = data.totals || {};
  const order = data.order || "executions";
  const executions = Number(totals.executions || 0);
  const averageMs = executions > 0 ? Number(totals.query_time_ms_total || 0) / executions : 0;
  const tiles = statTiles([
    { label: "慢 SQL 次数", value: humanCount(executions), hint: "RDS 慢日志实际记录数" },
    { label: "实际扫描行数", value: humanCount(totals.actual_scan_rows), hint: "RowsExamined 原始值求和" },
    { label: "返回行数", value: humanCount(totals.rows_sent), hint: "RowsSent 原始值求和" },
    { label: "累计查询耗时", value: millisText(totals.query_time_ms_total), hint: "QueryTime 原始值求和" },
    { label: "平均查询耗时", value: millisText(averageMs), hint: "累计 QueryTime / 执行次数" },
    { label: "最大查询耗时", value: millisText(totals.query_time_ms_max), hint: "窗口内单次最大 QueryTime" },
    { label: "累计锁等待", value: millisText(totals.lock_time_ms_total), hint: "LockTime 原始值求和" },
    { label: "SQL 指纹", value: humanCount(totals.fingerprints), hint: "参数归一后的慢 SQL" },
  ]);
  const sorter = `<div class="segmented sort-bar" aria-label="慢 SQL 排序">${SLOWLOG_ORDER_LABELS.map(
    ([key, label]) => `<button type="button" data-sql-order="${key}"${key === order ? ' class="is-active"' : ""}>${escapeHtml(label)}</button>`
  ).join("")}</div>`;
  const statements = analyticsTable(
    ["SQL", "SQL ID", "执行次数", "实际扫描", "单次扫描", "返回行数", "扫描/返回", "累计耗时", "平均耗时", "最大耗时", "锁等待", "最近出现"],
    (data.statements || []).map((item) => {
      const count = Number(item.executions || 0);
      const scanned = Number(item.scan_rows || 0);
      const sent = Number(item.rows_sent || 0);
      return [
        slowSqlDetailCell(item, order),
        `<span class="mono">${escapeHtml(item.sql_id || "—")}</span>`,
        `<strong>${humanCount(count)}</strong>`,
        `<strong>${humanCount(scanned)}</strong>`,
        count > 0 ? humanCount(Math.round(scanned / count)) : "—",
        humanCount(sent),
        sent > 0 ? `${(scanned / sent).toFixed(scanned / sent < 100 ? 1 : 0)}×` : (scanned > 0 ? "∞" : "—"),
        millisText(item.query_time_ms_total),
        count > 0 ? millisText(Number(item.query_time_ms_total || 0) / count) : "—",
        millisText(item.query_time_ms_max),
        millisText(item.lock_time_ms_total),
        escapeHtml(formatTime(Number(item.last_epoch_us))),
      ];
    }),
    "该时间窗内没有匹配的慢 SQL"
  );
  const objects = analyticsTable(
    ["对象", "慢 SQL 次数", "实际扫描", "返回行数", "累计耗时", "SQL 指纹"],
    (data.objects || []).map((item) => [
      escapeHtml(objectLabel(item.database_name, item.table_name)),
      humanCount(item.events),
      humanCount(item.scan_rows),
      humanCount(item.rows_sent),
      millisText(item.query_time_ms_total),
      humanCount(item.fingerprints),
    ]),
    "该时间窗内没有慢 SQL 对象"
  );
  const operations = barList(
    (data.operations || []).map((item) => ({ label: item.operation || "—", count: item.events })),
  );
  return `${tiles}
    <div class="analytics-block"><h3>慢 SQL 趋势</h3>${sparkline(data.trend || [])}</div>
    <div class="analytics-block">
      <div class="block-head"><h3>Top 慢 SQL</h3>${sorter}</div>
      <p class="analytics-note"><strong>实际扫描行数</strong>来自 RDS 慢日志 <code>RowsExamined</code>，<strong>返回行数</strong>来自 <code>RowsSent</code>，查询耗时与锁等待分别来自 <code>QueryTime</code> 和 <code>LockTime</code>；这里不使用 EXPLAIN 估算。按扫描或耗时排序时，点击 SQL 会打开该指纹的最大扫描或最大耗时原始执行，包含账号、客户端 IP、线程、时间和完整 SQL。扫描/返回比高通常意味着索引选择性不足或缺少合适索引，仍需结合 SQL、过滤条件与执行计划确认。</p>
      ${statements}
    </div>
    <div class="analytics-split">
      <div class="analytics-block"><h3>对象分布</h3>${objects}</div>
      <div class="analytics-block"><h3>操作分布</h3>${operations}</div>
    </div>`;
}

function renderAnalyticsSql(data) {
  if (data.mode === "slowlog") return renderAnalyticsSlowlogSql(data);
  const totals = data.totals || {};
  const order = data.order || "executions";
  const estCoverage = Number(totals.executions) > 0
    ? ((Number(totals.est_covered_executions) / Number(totals.executions)) * 100).toFixed(0)
    : "0";
  const tiles = statTiles([
    { label: "执行次数", value: humanCount(totals.executions), hint: "语句被执行的次数" },
    { label: "影响行数", value: humanCount(totals.row_events), hint: "被修改的行，非扫描行数" },
    { label: "扫描行数(估)", value: humanCount(totals.est_scan_rows), hint: `EXPLAIN 估算×执行次数，已覆盖 ${estCoverage}% 的执行` },
    { label: "语句指纹", value: humanCount(totals.fingerprints), hint: "参数已归一" },
    { label: "涉及对象", value: humanCount(totals.objects), hint: "库.表" },
    { label: "慢语句事件", value: humanCount(totals.slow_events), hint: "exec_time>0，即执行跨秒" },
    { label: "事务边界事件", value: humanCount(totals.boundary_events), hint: "BEGIN/COMMIT，已从榜单排除" },
  ]);
  const sorter = `<div class="segmented sort-bar" aria-label="语句排序">${SQL_ORDER_LABELS.map(
    ([key, label]) => `<button type="button" data-sql-order="${key}"${key === order ? ' class="is-active"' : ""}>${escapeHtml(label)}</button>`
  ).join("")}</div>`;
  const scanCell = (item) => {
    if (item.est_rows_per_exec === null || item.est_rows_per_exec === undefined) {
      return '<span class="muted">—</span>';
    }
    const tip = `EXPLAIN 估算：单次约 ${humanCount(item.est_rows_per_exec)} 行 × ${humanCount(item.executions)} 次` +
      (item.est_db ? `（库 ${item.est_db}）` : "");
    const mark = Number(item.est_full_scan) > 0
      ? ' <span class="chip warn" title="执行计划含全表/全索引扫描">全扫</span>'
      : "";
    return `<span title="${escapeHtml(tip)}">${humanCount(item.est_scan_total)}</span>${mark}`;
  };
  const statements = analyticsTable(
    ["语句", "动作", "执行次数", "次数占比", "影响行数", "行数占比", "每次行数", "扫描行数(估)", "慢语句", "最近出现"],
    (data.statements || []).map((item) => [
      `<code class="sql-cell" title="${escapeHtml(item.sample_sql || "")}">${sourceMark(item.source_kind)}${escapeHtml(item.normalized_sql || "—")}</code>`,
      `<span class="operation-chip ${escapeHtml(operationClass(item.action))}">${escapeHtml(item.action || "—")}</span>`,
      `<strong>${humanCount(item.executions)}</strong>`,
      shareBar(item.executions, totals.executions),
      humanCount(item.row_events),
      shareBar(item.row_events, totals.row_events),
      Number(item.executions) > 0
        ? (Number(item.row_events) / Number(item.executions)).toFixed(1)
        : "—",
      scanCell(item),
      Number(item.slow_events) > 0
        ? `<span class="chip warn" title="exec_time 最大 ${humanCount(item.exec_time_ms_max)} ms">${humanCount(item.slow_events)} 次</span>`
        : '<span class="muted">—</span>',
      escapeHtml(formatTime(Number(item.last_epoch_us))),
    ]),
    "该时间窗内没有匹配的语句"
  );
  const objects = analyticsTable(
    ["对象", "事件数", "体量", "语句指纹"],
    (data.objects || []).map((item) => [
      escapeHtml(objectLabel(item.database_name, item.table_name)),
      humanCount(item.events),
      humanBytes(item.payload_bytes),
      humanCount(item.fingerprints),
    ]),
    "该时间窗内没有写入对象"
  );
  const operations = barList(
    (data.operations || []).map((item) => ({ label: item.operation || "—", count: item.events })),
  );
  return `${tiles}
    <div class="analytics-block"><h3>写入趋势</h3>${sparkline(data.trend || [])}</div>
    <div class="analytics-block">
      <div class="block-head"><h3>Top 语句</h3>${sorter}</div>
      <p class="analytics-note"><strong>执行次数</strong>按 RowsEvent 边界统计（同一次语句执行产生的多行算一次），<strong>影响行数</strong>是被实际修改的行数——binlog 不记录扫描行数。<strong>扫描行数(估)</strong>是对 SELECT 指纹用真实参数样本跑 <code>EXPLAIN</code> 得到的优化器估算 × 窗口内执行次数：量级与相对排序可用，不是精确值；带「全扫」标记的执行计划含全表/全索引扫描。「慢语句」来自 QueryEvent 的 <code>exec_time</code>（秒级，仅 QueryEvent 有值）。</p>
      ${statements}
    </div>
    <div class="analytics-split">
      <div class="analytics-block"><h3>对象分布</h3>${objects}</div>
      <div class="analytics-block"><h3>操作分布</h3>${operations}</div>
    </div>`;
}

function renderTxnDrillChip() {
  const chip = $("#txn-drill-chip");
  if (!chip) return;
  if (!state.txnFilter) {
    chip.hidden = true;
    chip.innerHTML = "";
    return;
  }
  chip.hidden = false;
  chip.innerHTML = `<span class="chip">事务 <code>${escapeHtml(state.txnFilter)}</code></span>
    <button id="txn-drill-clear" class="button ghost" type="button">取消事务过滤</button>`;
}

function clearTransactionDrill(rerun) {
  if (!state.txnFilter) return;
  state.txnFilter = "";
  renderTxnDrillChip();
  if (rerun) {
    state.queryOffset = 0;
    runQuery().catch((error) => toast(error.message, "error"));
  }
}

async function openTransactionDrill(identity, startEpochUs, endEpochUs) {
  const txn = String(identity || "").trim();
  if (!txn) return;
  state.txnFilter = txn;
  // 清掉关键词：GTID 走 transaction 精确匹配，再叠一次 8 列 LIKE 只会拖慢。
  $("#filter-keyword").value = "";
  $("#filter-query-mode").value = "keyword";
  syncQueryMode();
  if (startEpochUs > 0 && endEpochUs > 0) {
    $("#filter-start").value = toLocalInput(new Date(startEpochUs / 1000));
    $("#filter-end").value = toLocalInput(new Date(endEpochUs / 1000));
  }
  $$(".operation-filter input").forEach((item) => { item.checked = false; });
  state.queryOffset = 0;
  renderTxnDrillChip();
  switchView("audit");
  try {
    await runQuery();
  } catch (error) {
    toast(error.message, "error");
  }
}

function txnDrillCell(item) {
  // 事务标识做成钻取入口：点开 = 带 GTID 与该事务时间窗跳到审计查询。
  // 精确匹配的是 transaction_id / gtid / xid，所以三者取第一个非空的即可。
  const identity = item.gtid || item.xid || item.transaction_id || "";
  const label = identity || item.txn_key || "—";
  if (!identity) return `<code>${escapeHtml(label)}</code>`;
  // 时间窗按事务首末事件各留 2 秒余量：窗口越窄，命中的分区越少、越快。
  const start = Math.max(Number(item.start_epoch_us || 0) - 2_000_000, 0);
  const end = Number(item.end_epoch_us || item.start_epoch_us || 0) + 2_000_000;
  return `<button type="button" class="txn-drill" data-txn="${escapeHtml(identity)}"
    data-txn-start="${start}" data-txn-end="${end}"
    title="点开看这个事务的完整经过">${escapeHtml(label)}</button>`;
}

function transactionRows(items) {
  if (!items.length) return [];
  return items.map((item) => [
    txnDrillCell(item),
    escapeHtml(formatTime(Number(item.start_epoch_us))),
    escapeHtml(humanMicros(item.duration_us)),
    humanCount(item.row_events),
    humanBytes(Number(item.txn_length_bytes) || Number(item.payload_bytes)),
    Number(item.dependency_depth) > 0 ? humanCount(item.dependency_depth) : '<span class="muted">—</span>',
    `${escapeHtml(String((item.tables && item.tables.first) || "—"))}${Number(item.table_count) > 1 ? ` <span class="muted">+${Number(item.table_count) - 1}</span>` : ""}`,
    `${item.has_ddl ? '<span class="chip warn">含 DDL</span>' : ""}${item.multi_table ? '<span class="chip">跨表</span>' : ""}${item.boundary_open ? '<span class="chip" title="事务跨越分区边界，时长为窗口内可见部分">跨分区</span>' : ""}` || "—",
  ]);
}

const TXN_DRILLS = {
  longest: ["最长事务", "按首末事件时间差排序；分辨率 1 秒，同秒完成的显示 0 秒"],
  largest: ["最大事务", "按事务内行事件数排序，行数越多锁范围越大、回滚代价越高"],
  ddl: ["含 DDL 事务", "包含 CREATE / ALTER / DROP / RENAME / TRUNCATE 的事务，按时间倒序"],
  multi_table: ["跨表事务", "同一事务修改了多张表，按涉及表数排序"],
};

function renderAnalyticsTransactions(data) {
  const totals = data.totals || {};
  const tiles = statTiles([
    { label: "事务数", value: humanCount(totals.transactions) },
    { label: "行事件", value: humanCount(totals.row_events) },
    { label: "最长事务", value: humanMicros(totals.max_duration_us), hint: "提交时刻 − 首事件时刻", drill: "longest" },
    { label: "最大事务", value: `${humanCount(totals.max_row_events)} 行`, drill: "largest" },
    { label: "跨秒事务", value: humanCount(totals.cross_second_transactions), hint: "耗时可测的事务", drill: "longest" },
    { label: "提交依赖深度", value: `${totals.avg_dependency_depth ?? 0} / ${humanCount(totals.max_dependency_depth)}`, hint: "均值/最大，越大越串行" },
    { label: "含 DDL 事务", value: humanCount(totals.ddl_transactions), drill: "ddl" },
    { label: "跨表事务", value: humanCount(totals.multi_table_transactions), drill: "multi_table" },
    { label: "数据体量", value: humanBytes(totals.payload_bytes) },
  ]);
  return `${tiles}
    <div class="analytics-block"><h3>提交趋势</h3>${sparkline(data.trend || [], { valueKey: "transactions" })}</div>
    <div class="analytics-split">
      <div class="analytics-block"><h3>事务大小分布</h3>${barList(data.row_histogram || [])}</div>
      <div class="analytics-block">
        <h3>事务时长分布</h3>
        <p class="analytics-note">耗时 = 事务提交时刻（微秒，来自 GTID）− 事务内最早事件的时间戳（秒级）。起点被截断到秒，因此<strong>只有跨秒事务的耗时可测</strong>；同秒完成的事务统一归入「同秒完成」并计 0，不是耗时为零。</p>
        ${barList(data.duration_histogram || [])}
      </div>
    </div>
    <div class="analytics-block">
      <div class="block-head">
        <h3 id="txn-drill-title">最长事务</h3>
        <div class="segmented sort-bar" aria-label="事务明细">${Object.entries(TXN_DRILLS)
          .map(([key, [label]]) => `<button type="button" data-txn-drill="${key}"${key === "longest" ? ' class="is-active"' : ""}>${escapeHtml(label)}</button>`)
          .join("")}</div>
      </div>
      <p class="analytics-note" id="txn-drill-note">${escapeHtml(TXN_DRILLS.longest[1])}</p>
      <div id="txn-drill-body"></div>
    </div>`;
}

const TXN_HEADERS = ["事务标识", "提交时间", "持续时长", "行事件", "体量", "依赖深度", "对象", "标记"];

function renderTxnDrill(kind) {
  const data = state.analytics?.transactions || {};
  const rows = data[kind] || [];
  const [title, note] = TXN_DRILLS[kind] || ["事务明细", ""];
  const empty = {
    longest: "该时间窗内没有事务",
    largest: "该时间窗内没有事务",
    ddl: "该时间窗内没有 DDL 事务",
    multi_table: "该时间窗内没有跨表事务",
  }[kind];
  const titleEl = $("#txn-drill-title");
  if (titleEl) titleEl.textContent = title;
  const noteEl = $("#txn-drill-note");
  if (noteEl) noteEl.textContent = note;
  const body = $("#txn-drill-body");
  if (body) body.innerHTML = analyticsTable(TXN_HEADERS, transactionRows(rows), empty);
  $$("[data-txn-drill]").forEach((el) => {
    if (el.tagName === "BUTTON" && el.closest(".sort-bar")) {
      el.classList.toggle("is-active", el.dataset.txnDrill === kind);
    }
  });
}

function renderAnalyticsLocks(data, coverage) {
  const risk = data.risk || {};
  const keyedTables = Number(coverage?.primary_key_tables || 0);
  const rowKeyUnavailable = !keyedTables && !(data.row_hotspots || []).length;
  const tiles = statTiles([
    { label: "最长持锁（推断）", value: humanMicros(risk.longest_transaction_us), hint: "下界估计" },
    { label: "最大锁范围（推断）", value: `${humanCount(risk.largest_transaction_rows)} 行` },
    { label: "最热行改写事务数", value: humanCount(risk.row_hotspot_max_txn), hint: "同一主键被多少事务改写" },
    { label: "DDL 事件", value: humanCount(risk.ddl_events), hint: "潜在 MDL 阻塞点" },
  ]);
  const rowHotspots = analyticsTable(
    ["对象", "主键值", "改写次数", "事务数*", "UPDATE", "DELETE", "首次", "最近"],
    (data.row_hotspots || []).map((item) => [
      escapeHtml(objectLabel(item.database_name, item.table_name)),
      `<code>${escapeHtml(item.row_key)}</code>`,
      `<strong>${humanCount(item.event_count)}</strong>`,
      humanCount(item.txn_count),
      humanCount(item.update_count),
      humanCount(item.delete_count),
      escapeHtml(formatTime(Number(item.first_epoch_us), true)),
      escapeHtml(formatTime(Number(item.last_epoch_us), true)),
    ]),
    "没有被多个事务重复改写的行；也可能是相关表没有可识别主键（见下方表级热点）"
  );
  const tableHotspots = analyticsTable(
    ["对象", "写事件数", "事务数*", "UPDATE", "DELETE"],
    (data.table_hotspots || []).map((item) => [
      escapeHtml(objectLabel(item.database_name, item.table_name)),
      humanCount(item.event_count),
      humanCount(item.txn_count),
      humanCount(item.update_count),
      humanCount(item.delete_count),
    ]),
    "该时间窗内没有写入"
  );
  const ddl = analyticsTable(
    ["时间", "对象", "语句", "同表 DML（±5 分钟）"],
    (data.ddl_windows || []).map((item) => [
      escapeHtml(formatTime(Number(item.event_epoch_us))),
      escapeHtml(objectLabel(item.database_name, item.table_name)),
      `<code class="sql-cell">${escapeHtml(item.sample_sql || "—")}</code>`,
      `${humanCount(item.concurrent_dml_events)}${Number(item.concurrent_dml_events) > 0 ? ' <span class="chip warn">有并发写</span>' : ""}`,
    ]),
    "该时间窗内没有 DDL"
  );
  const headers = ["事务标识", "开始时间", "持续时长", "行事件", "体量", "对象", "标记"];
  return `${tiles}
    <div class="analytics-block">
      <h3>行锁争用热点（推断）</h3>
      <p class="analytics-note">同一主键在窗口内被多个事务反复改写，改写次数越高越可能出现行锁等待。这是<strong>改写频次</strong>，不是实测锁等待。带 * 的事务数按 5 分钟桶累计，跨桶的同一事务会被重复计入，仅作量级参考。</p>
      ${rowKeyUnavailable
        ? `<p class="analytics-unavailable"><strong>行级归因当前不可用。</strong>本时间窗内的 Binlog 行镜像没有携带主键元数据（列名显示为 <code>@1</code>、<code>@2</code>），无法把改写归到具体某一行。这不代表没有行锁争用——请改看下方<strong>表级写热点</strong>。要启用行级归因，需要实例开启 <code>binlog_row_metadata=FULL</code>，之后新解析的 Binlog 才会带列名与主键标记。</p>`
        : rowHotspots}
    </div>
    <div class="analytics-block">
      <h3>表级写热点</h3>
      <p class="analytics-note">覆盖所有写入表，包含没有可识别主键、无法做行级归因的表。</p>
      ${tableHotspots}
    </div>
    <div class="analytics-block">
      <h3>长事务（持锁时长下界）</h3>
      <p class="analytics-note">事务在提交前一直持有已修改行的行锁，时长越长阻塞窗口越大。耗时由 GTID 的微秒级提交时刻减去事务首事件的秒级时间戳得到，<strong>只有跨秒事务可测</strong>，误差 &lt;1 秒。</p>
      ${analyticsTable(headers, transactionRows(data.long_transactions || []), "该时间窗内没有事务")}
    </div>
    <div class="analytics-block">
      <h3>大事务（锁范围）</h3>
      <p class="analytics-note">单个事务修改的行越多，持有的行锁越多，回滚代价与阻塞面越大。</p>
      ${analyticsTable(headers, transactionRows(data.large_transactions || []), "该时间窗内没有事务")}
    </div>
    <div class="analytics-block">
      <h3>DDL 与并发写窗口（MDL 风险）</h3>
      <p class="analytics-note">DDL 需要元数据锁；同一时间窗内该表仍有 DML，说明存在被阻塞的可能。Binlog 无法证明是否真的等待过。</p>
      ${ddl}
    </div>`;
}

function renderAnalyticsCoverage(coverage, window, source = "binlog", indexStats = null) {
  const node = $("#analytics-coverage");
  const total = Number(coverage?.total_parts || 0);
  const covered = Number(coverage?.covered_parts || 0);
  const pending = Number(coverage?.pending_parts || 0);
  const scanned = (coverage?.scanned_parts || []).length;
  const errors = coverage?.scan_errors || [];
  const complete = Boolean(coverage?.complete);
  // 0 个分区必须与「全覆盖且确实没有写入」区分开：前者是还没同步过这段
  // Binlog，空结果不能被读成「数据库没写入」。
  const nothingSynced = total === 0;
  node.className = `notice ${complete && !nothingSynced ? "info" : "warning"} compact-notice`;
  const parts = [];
  if (nothingSynced) {
    if (source === "slowlog") {
      parts.push("该时间窗内<strong>没有已采集的慢日志分区</strong>，空结果不代表没有慢 SQL；请检查慢日志采集水位与时间范围");
    } else {
      parts.push("该时间窗内<strong>没有已同步的 Binlog 分区</strong>，空结果不代表数据库没有写入；请先在“同步任务”里同步这段时间");
    }
  } else {
    parts.push(`分区覆盖 <strong>${covered}/${total}</strong>`);
    if (scanned) parts.push(`本次即时扫描 ${scanned} 个`);
    if (pending) parts.push(`<strong>${pending}</strong> 个待后台补齐，结果暂不含这些分区`);
    if (complete) parts.push("窗口内全部分区已覆盖");
  }
  if (errors.length) parts.push(`扫描失败 ${errors.length} 个：${escapeHtml(errors[0])}`);
  if (window?.trend_bucket_us) parts.push(`趋势粒度 ${humanMicros(window.trend_bucket_us)}`);
  if (source === "slowlog" && indexStats) {
    parts.push(`近 10 分钟采集/索引 <strong>${Number(indexStats.events_per_minute_10m || 0).toFixed(1)}</strong> 条/分钟`);
    parts.push(`索引队列 <strong>${humanCount(indexStats.pending_parts || 0)}</strong> 个`);
  }
  node.innerHTML = `<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M12 11v6M12 7h.01"/></svg><span>${parts.join(" · ")}</span>`;
}

function syncAnalyticsMode(slowSource) {
  const warning = $("#analytics-lock-warning");
  if (warning) warning.hidden = slowSource;
  const nodeField = $("#analytics-node-field");
  const nodeInput = $("#analytics-node");
  if (nodeField) nodeField.hidden = !slowSource;
  if (nodeInput) nodeInput.disabled = !slowSource;
  for (const name of ["transactions", "locks"]) {
    const button = $(`[data-analytics-tab="${name}"]`);
    if (button) {
      button.disabled = slowSource;
      button.title = slowSource ? "慢日志洞察不推断事务与锁争用，请切换到 Binlog 写入" : "";
    }
  }
  if (slowSource) {
    state.analyticsTab = "sql";
    switchAnalyticsTab("sql");
  }
}

function switchAnalyticsTab(name) {
  state.analyticsTab = name;
  $$("[data-analytics-tab]").forEach((button) => button.classList.toggle("is-active", button.dataset.analyticsTab === name));
  $$(".analytics-panel").forEach((panel) => panel.classList.toggle("is-active", panel.id === `analytics-panel-${name}`));
}

function renderAnalytics(result) {
  state.analytics = result;
  const slowSource = result.evidence?.source === "slowlog";
  const actualSlowlog = result.sql?.mode === "slowlog";
  syncAnalyticsMode(slowSource);
  $("#analytics-empty").hidden = true;
  $("#analytics-panel-sql").innerHTML = renderAnalyticsSql(result.sql || {});
  $("#analytics-panel-transactions").innerHTML = renderAnalyticsTransactions(result.transactions || {});
  $("#analytics-panel-locks").innerHTML = renderAnalyticsLocks(result.locks || {}, result.coverage);
  renderAnalyticsCoverage(result.coverage, result.window, slowSource ? "slowlog" : "binlog", result.slowlog_index);
  renderTxnDrill(state.txnDrill || "longest");
  const totals = result.sql?.totals || {};
  const txns = result.transactions?.totals || {};
  $("#analytics-meta").textContent = actualSlowlog
    ? `${humanCount(totals.executions)} 条慢 SQL · 实际扫描 ${humanCount(totals.actual_scan_rows)} 行 · 返回 ${humanCount(totals.rows_sent)} 行 · ${humanCount(totals.fingerprints)} 个指纹`
    : `${humanCount(totals.events)} 次执行 · ${humanCount(totals.row_events)} 行影响 · ${humanCount(txns.transactions)} 个事务 · ${humanCount(totals.fingerprints)} 个指纹`;
  switchAnalyticsTab(state.analyticsTab || "sql");
}

function setAnalyticsRange(range) {
  const units = { "1h": 60 * 60_000, "6h": 6 * 60 * 60_000, "24h": 24 * 60 * 60_000, "7d": 7 * 24 * 60 * 60_000, "30d": 30 * 24 * 60 * 60_000 };
  const latestEpochUs = Number(state.status?.summary?.latestEpochUs || 0);
  const end = latestEpochUs > 0 ? new Date(latestEpochUs / 1000) : new Date();
  $("#analytics-end").value = toLocalInput(end);
  $("#analytics-start").value = toLocalInput(new Date(end.getTime() - units[range]));
  $$("[data-analytics-range]").forEach((button) => button.classList.toggle("is-active", button.dataset.analyticsRange === range));
}

function analyticsQueryString(orderOverride = "") {
  const params = new URLSearchParams();
  const start = $("#analytics-start").value;
  const end = $("#analytics-end").value;
  if (!start || !end) throw new Error("请先选择分析的开始时间和结束时间");
  const startDate = new Date(start);
  const endDate = new Date(end);
  if (Number.isNaN(startDate.getTime()) || Number.isNaN(endDate.getTime())) throw new Error("分析时间格式无效");
  if (startDate >= endDate) throw new Error("结束时间必须晚于开始时间");
  params.set("startEpochUs", String(startDate.getTime() * 1000));
  params.set("endEpochUs", String(endDate.getTime() * 1000));
  const source = $("#analytics-source").value || "binlog";
  params.set("source", source);
  const instance = $("#analytics-instance").value;
  if (instance) params.set("instance", instance);
  const nodeId = $("#analytics-node").value.trim();
  if (source === "slowlog" && nodeId) params.set("nodeId", nodeId);
  const database = $("#analytics-database").value.trim();
  const table = $("#analytics-table").value.trim();
  const operation = $("#analytics-operation").value;
  if (database) params.set("database", database);
  if (table) params.set("table", table);
  if (operation) params.set("operation", operation);
  params.set("limit", $("#analytics-limit").value);
  // 未覆盖分区的即时补建由服务端自动决策（缺得少补全、缺得多补最新一批）。
  params.set("order", orderOverride || state.sqlOrder || "executions");
  return params.toString();
}

async function runAnalytics(orderOverride = "") {
  if (orderOverride) state.sqlOrder = orderOverride;
  // 换排序不需要重新扫描分区：已覆盖的聚合直接重排即可。
  const query = analyticsQueryString(orderOverride);
  const result = await api(`/api/analytics?${query}`);
  renderAnalytics(result);
  const pending = Number(result.coverage?.pending_parts || 0);
  if (pending) {
    toast(`分析完成；还有 ${pending} 个分区未建索引，后台补齐后重新分析可得到完整结果`, "info", 6000);
  } else {
    toast("分析完成", "success");
  }
}

/* ------------------------------ 结构对比 ------------------------------ */

const schemaStatusMeta = {
  changed: ["有差异", "warn"],
  missing_in_target: ["目标缺表", "info"],
  extra_in_target: ["目标独有", "muted"],
  same: ["一致", "ok"],
};

const schemaKindLabels = {
  table_added: "缺表",
  table_extra: "多表",
  column_added: "缺列",
  column_modified: "列定义",
  column_dropped: "多列",
  index_added: "缺索引",
  index_modified: "索引定义",
  index_dropped: "多索引",
  fk_added: "缺外键",
  fk_modified: "外键定义",
  fk_dropped: "多外键",
  option_engine: "引擎",
  option_collation: "排序规则",
  option_comment: "表注释",
  option_row_format: "行格式",
};

function schemaSelects() {
  return {
    sourceInstance: $("#schema-source-instance"),
    sourceDatabase: $("#schema-source-database"),
    targetInstance: $("#schema-target-instance"),
    targetDatabase: $("#schema-target-database"),
  };
}

async function initSchemaView() {
  const selects = schemaSelects();
  try {
    const data = await api("/api/schema/instances");
    state.schemaInstances = data.instances || [];
    if (!data.enabled || !state.schemaInstances.length) {
      $("#schema-meta").textContent = "未配置可对比实例（RDS_BINLOG_SCHEMA_INSTANCES）";
      [selects.sourceInstance, selects.targetInstance].forEach((select) => {
        select.innerHTML = '<option value="">未配置</option>';
        select.disabled = true;
      });
      return;
    }
    const options = state.schemaInstances
      .map((item) => `<option value="${escapeHtml(item.name)}">${escapeHtml(item.label)}（${escapeHtml(item.instanceId || item.name)}）</option>`)
      .join("");
    selects.sourceInstance.innerHTML = options;
    selects.targetInstance.innerHTML = options;

    // 预选值来自后端配置(defaultCompare)，没配才退回"第一个/第二个实例"
    const preset = data.defaultCompare || {};
    const known = new Set(state.schemaInstances.map((item) => item.name));
    selects.sourceInstance.value = known.has(preset.sourceInstance)
      ? preset.sourceInstance
      : state.schemaInstances[0].name;
    selects.targetInstance.value = known.has(preset.targetInstance)
      ? preset.targetInstance
      : (state.schemaInstances[1] || state.schemaInstances[0]).name;
    if (preset.scope) {
      state.schemaScope = preset.scope;
      $$("[data-schema-scope]").forEach((item) =>
        item.classList.toggle("is-active", item.dataset.schemaScope === preset.scope));
    }
    await Promise.all([loadSchemaDatabases("source"), loadSchemaDatabases("target")]);
    for (const [side, wanted] of [["source", preset.sourceDatabase], ["target", preset.targetDatabase]]) {
      if (!wanted) continue;
      const select = side === "source" ? selects.sourceDatabase : selects.targetDatabase;
      if ([...select.options].some((option) => option.value === wanted)) select.value = wanted;
    }
  } catch (error) {
    $("#schema-meta").textContent = error.message;
    toast(error.message, "error", 6000);
  }
}

async function loadSchemaDatabases(side) {
  const instanceSelect = side === "source" ? $("#schema-source-instance") : $("#schema-target-instance");
  const databaseSelect = side === "source" ? $("#schema-source-database") : $("#schema-target-database");
  const instance = instanceSelect.value;
  if (!instance) return;
  const previous = databaseSelect.value;
  databaseSelect.disabled = true;
  databaseSelect.innerHTML = '<option value="">加载中…</option>';
  try {
    const data = await api(`/api/schema/databases?instance=${encodeURIComponent(instance)}`);
    const databases = data.databases || [];
    if (!databases.length) {
      databaseSelect.innerHTML = '<option value="">该实例没有可见业务库</option>';
      return;
    }
    databaseSelect.innerHTML = databases
      .map((item) => `<option value="${escapeHtml(item.name)}">${escapeHtml(item.name)}（${humanCount(item.tableCount)} 表）</option>`)
      .join("");
    if (databases.some((item) => item.name === previous)) databaseSelect.value = previous;
    databaseSelect.disabled = false;
  } catch (error) {
    databaseSelect.innerHTML = `<option value="">${escapeHtml(error.message)}</option>`;
    toast(error.message, "error", 6000);
  }
}

function renderSchemaSummary(result) {
  const summary = result.summary;
  $("#schema-summary").hidden = false;
  $("#schema-stat-tables").textContent = humanCount(summary.tables);
  $("#schema-stat-same").textContent = humanCount(summary.same);
  $("#schema-stat-changed").textContent = humanCount(summary.changed);
  $("#schema-stat-missing").textContent = humanCount(summary.missingInTarget);
  $("#schema-stat-extra").textContent = humanCount(summary.extraInTarget);
  $("#schema-stat-changes").textContent = `${humanCount(summary.safeChanges)} / ${humanCount(summary.riskyChanges)}`;
  const source = result.source;
  const target = result.target;
  $("#schema-meta").textContent =
    `基准 ${source.label || source.instance}.${source.database}（${source.tableCount} 表）` +
    ` → 目标 ${target.label || target.instance}.${target.database}（${target.tableCount} 表）`;
  $("#schema-identity").textContent =
    `基准 ${source.identity.hostname || "-"}:${source.identity.port || "-"} ${source.identity.user || ""}` +
    ` ｜ 目标 ${target.identity.hostname || "-"}:${target.identity.port || "-"} ${target.identity.user || ""}` +
    ` ｜ read_only=${target.identity.readOnly}`;
}

function renderSchemaChange(change) {
  const label = schemaKindLabels[change.kind] || change.kind;
  const rows = [];
  if (change.source) rows.push(`<div class="schema-side"><span>基准</span><code>${escapeHtml(change.source)}</code></div>`);
  if (change.target) rows.push(`<div class="schema-side"><span>目标</span><code>${escapeHtml(change.target)}</code></div>`);
  const sql = change.sql
    ? `<pre class="schema-change-sql">${escapeHtml(change.sql)}</pre>`
    : "";
  return `
    <li class="schema-change ${change.risk === "risky" ? "is-risky" : ""}">
      <div class="schema-change-head">
        <span class="schema-badge ${change.risk === "risky" ? "warn" : "ok"}">${change.risk === "risky" ? "高危" : "安全"}</span>
        <strong>${escapeHtml(label)}</strong>
        <code>${escapeHtml(change.object)}</code>
        <span class="schema-change-detail">${escapeHtml(change.detail)}</span>
      </div>
      ${rows.join("")}
      ${sql}
    </li>`;
}

function renderSchemaDiffList() {
  const result = state.schemaResult;
  const container = $("#schema-diff-list");
  if (!result) {
    container.innerHTML = "";
    return;
  }
  const diffs = result.diffs.filter((item) => (state.schemaFilter === "all" ? true : item.status !== "same"));
  $("#schema-empty").hidden = diffs.length > 0;
  if (!diffs.length) {
    container.innerHTML = "";
    $("#schema-empty").querySelector("strong").textContent =
      state.schemaFilter === "all" ? "没有可显示的表" : "两侧结构完全一致";
    $("#schema-empty").querySelector("span").textContent =
      state.schemaFilter === "all" ? "换个库再试。" : "该范围内没有需要对齐的差异。";
    return;
  }
  container.innerHTML = diffs
    .map((item) => {
      const [statusText, statusTone] = schemaStatusMeta[item.status] || [item.status, "muted"];
      const counts = item.status === "same"
        ? ""
        : `<span class="schema-count">安全 ${item.safeCount} · 高危 ${item.riskyCount}</span>`;
      const body = item.changes.length
        ? `<ul class="schema-change-list">${item.changes.map(renderSchemaChange).join("")}</ul>`
        : '<p class="schema-note">无差异</p>';
      return `
        <details class="schema-table" data-table="${escapeHtml(item.table)}">
          <summary>
            <span class="schema-badge ${statusTone}">${escapeHtml(statusText)}</span>
            <code class="schema-table-name">${escapeHtml(item.table)}</code>
            ${counts}
          </summary>
          ${body}
        </details>`;
    })
    .join("");
}

function renderSchemaSql() {
  const result = state.schemaResult;
  if (!result) return;
  const includeRisky = $("#schema-include-risky").checked;
  const sql = includeRisky ? result.sqlWithRisky : result.sql;
  $("#schema-sql").textContent = sql;
  $("#schema-sql-section").hidden = false;
  $("#schema-sql-meta").textContent = includeRisky
    ? "已展开高危语句，执行前务必确认数据影响"
    : "高危语句已注释，仅安全语句可直接执行";
}

function schemaSqlFilename() {
  const result = state.schemaResult;
  const stamp = new Date().toISOString().slice(0, 10);
  return `schema-align-${result.target.instance}-${result.target.database}-${stamp}.sql`;
}

async function runSchemaDiff() {
  const selects = schemaSelects();
  const payload = {
    sourceInstance: selects.sourceInstance.value,
    sourceDatabase: selects.sourceDatabase.value,
    targetInstance: selects.targetInstance.value,
    targetDatabase: selects.targetDatabase.value,
    scope: state.schemaScope,
  };
  if (!payload.sourceDatabase || !payload.targetDatabase) {
    toast("请先选择基准库和目标库", "error");
    return;
  }
  await withBusy($("#schema-submit"), async () => {
    const result = await api("/api/schema/diff", { method: "POST", body: JSON.stringify(payload) });
    state.schemaResult = result;
    renderSchemaSummary(result);
    renderSchemaDiffList();
    renderSchemaSql();
    const summary = result.summary;
    toast(
      summary.changed || summary.missingInTarget
        ? `对比完成：${summary.changed} 张表有差异，安全语句 ${summary.safeChanges} 条`
        : "对比完成：所选范围内结构一致",
      summary.changed ? "info" : "success",
      5000,
    );
  }, "对比中…");
}

function bindSchemaEvents() {
  $("#schema-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await runSchemaDiff();
    } catch (error) {
      toast(error.message, "error", 6000);
    }
  });
  $("#schema-source-instance").addEventListener("change", () => loadSchemaDatabases("source"));
  $("#schema-target-instance").addEventListener("change", () => loadSchemaDatabases("target"));
  $$("[data-schema-scope]").forEach((button) => {
    button.addEventListener("click", () => {
      state.schemaScope = button.dataset.schemaScope;
      $$("[data-schema-scope]").forEach((item) => item.classList.toggle("is-active", item === button));
    });
  });
  $$("[data-schema-filter]").forEach((button) => {
    button.addEventListener("click", () => {
      state.schemaFilter = button.dataset.schemaFilter;
      $$("[data-schema-filter]").forEach((item) => item.classList.toggle("is-active", item === button));
      renderSchemaDiffList();
    });
  });
  $("#schema-swap").addEventListener("click", async () => {
    const selects = schemaSelects();
    const sourceInstance = selects.sourceInstance.value;
    const sourceDatabase = selects.sourceDatabase.value;
    selects.sourceInstance.value = selects.targetInstance.value;
    selects.targetInstance.value = sourceInstance;
    await Promise.all([loadSchemaDatabases("source"), loadSchemaDatabases("target")]);
    if ([...selects.sourceDatabase.options].some((option) => option.value === selects.targetDatabase.value)) {
      const previousTarget = selects.targetDatabase.value;
      selects.sourceDatabase.value = previousTarget;
      if ([...selects.targetDatabase.options].some((option) => option.value === sourceDatabase)) {
        selects.targetDatabase.value = sourceDatabase;
      }
    }
  });
  $("#schema-include-risky").addEventListener("change", renderSchemaSql);
  $("#schema-copy-sql").addEventListener("click", async () => {
    const text = $("#schema-sql").textContent || "";
    if (!text) return;
    try {
      await navigator.clipboard.writeText(text);
      toast("已复制到剪贴板", "success");
    } catch {
      toast("浏览器拒绝了剪贴板访问，请手动选中复制", "error");
    }
  });
  $("#schema-download-sql").addEventListener("click", () => {
    const text = $("#schema-sql").textContent || "";
    if (!text || !state.schemaResult) return;
    const blob = new Blob([text], { type: "application/sql;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = schemaSqlFilename();
    document.body.append(anchor);
    anchor.click();
    anchor.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 1000);
  });
}

function bindEvents() {
  bindSchemaEvents();
  $$(".nav-item").forEach((button) => button.addEventListener("click", () => switchView(button.dataset.view)));
  $$("[data-go]").forEach((button) => button.addEventListener("click", () => switchView(button.dataset.go)));
  $$("[data-range]").forEach((button) => button.addEventListener("click", () => setQuickRange(button.dataset.range)));
  $$("[data-analytics-range]").forEach((button) => button.addEventListener("click", () => setAnalyticsRange(button.dataset.analyticsRange)));
  $$("[data-analytics-tab]").forEach((button) => button.addEventListener("click", () => switchAnalyticsTab(button.dataset.analyticsTab)));
  $("#analytics-source").addEventListener("change", () => {
    syncAnalyticsMode($("#analytics-source").value === "slowlog");
  });
  $("#analytics-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try {
      await withBusy($("#analytics-submit"), runAnalytics, "分析中…");
    } catch (error) { toast(error.message, "error", 6000); }
  });
  // 排序与事务下钻都在重渲染后的节点上，用事件委托绑定一次。
  $("#view-analytics").addEventListener("click", async (event) => {
    const slowEvent = event.target.closest("[data-slow-event-id]");
    if (slowEvent) {
      await openDetail(
        slowEvent.dataset.slowEventId,
        "",
        slowEvent.dataset.slowInstance || "",
      );
      return;
    }
    const drill = event.target.closest("[data-txn-drill]");
    if (drill) {
      state.txnDrill = drill.dataset.txnDrill;
      if (!state.analytics) return;
      switchAnalyticsTab("transactions");
      renderTxnDrill(state.txnDrill);
      $("#txn-drill-title")?.scrollIntoView({ behavior: "smooth", block: "center" });
      return;
    }
    const sort = event.target.closest("[data-sql-order]");
    if (sort && state.analytics) {
      const key = sort.dataset.sqlOrder;
      const sql = state.analytics.sql || {};
      if (key === (sql.order || "executions")) return;
      state.sqlOrder = key;
      // 响应里带了全部排序的 TopN 快照：本地切换，零请求零扫描。
      if (sql.orders && sql.orders[key]) {
        sql.order = key;
        sql.statements = sql.orders[key];
        $("#analytics-panel-sql").innerHTML = renderAnalyticsSql(sql);
        return;
      }
      // 兜底：旧格式响应没有快照时退回重新请求。
      try {
        await withBusy(sort, () => runAnalytics(key), "排序中…");
      } catch (error) { toast(error.message, "error", 6000); }
    }
  });
  $("#analytics-reset").addEventListener("click", () => {
    $("#analytics-source").value = "binlog";
    syncAnalyticsMode(false);
    $("#analytics-node").value = "";
    $("#analytics-database").value = "";
    $("#analytics-table").value = "";
    $("#analytics-operation").value = "";
    $("#analytics-limit").value = "50";
    setAnalyticsRange("24h");
  });
  $("#query-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    state.queryOffset = 0;
    try { await runQuery(); } catch (error) { toast(error.message, "error"); }
  });
  $("#filter-end").addEventListener("input", () => {
    $("#filter-end").setCustomValidity("");
  });
  $("#filter-query-mode").addEventListener("change", syncQueryMode);
  document.addEventListener("click", (event) => {
    const trigger = event.target.closest(".txn-drill");
    if (trigger) {
      openTransactionDrill(
        trigger.dataset.txn,
        Number(trigger.dataset.txnStart),
        Number(trigger.dataset.txnEnd),
      );
      return;
    }
    if (event.target.closest("#txn-drill-clear")) clearTransactionDrill(true);
  });
  $("#query-reset").addEventListener("click", () => {
    clearTransactionDrill(false);
    $("#filter-instance").value = "";
    $("#filter-keyword").value = "";
    $("#filter-database").value = "";
    $("#filter-table").value = "";
    $("#filter-source").value = "";
    $("#filter-connection").value = "";
    $("#filter-account").value = "";
    $("#filter-status").value = "";
    $("#filter-keyword-mode").value = "AND";
    $("#filter-query-mode").value = "keyword";
    syncQueryMode();
    $$(".operation-filter input").forEach((item) => { item.checked = false; });
    setQuickRange("24h");
    state.queryOffset = 0;
    state.activeQuery = null;
  });
  $("#query-export").addEventListener("click", async () => {
    try { await downloadExport(); } catch (error) { toast(error.message, "error", 6000); }
  });
  $("#page-prev").addEventListener("click", async () => {
    state.queryOffset = Math.max(0, state.queryOffset - state.queryLimit);
    const query = state.activeQuery ? { ...state.activeQuery, offset: state.queryOffset } : null;
    try { await runQuery(query); } catch (error) { toast(error.message, "error"); }
  });
  $("#page-next").addEventListener("click", async () => {
    state.queryOffset += state.queryLimit;
    const query = state.activeQuery ? { ...state.activeQuery, offset: state.queryOffset } : null;
    try { await runQuery(query); } catch (error) { toast(error.message, "error"); }
  });
  $("#close-drawer").addEventListener("click", closeDetail);
  $("#drawer-backdrop").addEventListener("click", closeDetail);
  document.addEventListener("keydown", (event) => { if (event.key === "Escape") closeDetail(); });
  $("#start-sync").addEventListener("click", async () => { try { await startSync(); } catch (error) { toast(error.message, "error"); } });
  $("#sync-top").addEventListener("click", () => {
    switchView("jobs");
    setDefaultSyncWindow();
    $("#sync-start-time").focus();
  });
  $("#pause-sync").addEventListener("click", async () => {
    try {
      const data = await api("/api/sync/pause", { method: "POST", body: "{}" });
      toast(data.message, "info");
    } catch (error) { toast(error.message, "error"); }
  });
  $("#run-cleanup").addEventListener("click", async () => {
    const button = $("#run-cleanup");
    try {
      await withBusy(button, async () => {
        const data = await api("/api/storage/cleanup", { method: "POST", body: "{}" });
        toast(`清理完成：删除 ${data.deleted_parts} 分区，移除 ${humanCount(data.removed_rows)} 行`, data.errors?.length ? "error" : "success");
        await refreshStorage();
      }, "清理中…");
    } catch (error) { toast(error.message, "error"); }
  });
  $("#settings-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    try { await saveSettings(); } catch (error) { toast(error.message, "error"); }
  });
  $("#test-settings").addEventListener("click", async () => {
    const button = $("#test-settings");
    try {
      await withBusy(button, async () => {
        await saveSettings();
        const data = await api("/api/settings/test", { method: "POST", body: "{}" });
        toast(`${data.message}；近 24 小时 ${data.recentBinlogCount} 个文件`, "success", 5200);
      }, "核验中…");
    } catch (error) { toast(error.message, "error", 6000); }
  });
  $("#stop-service").addEventListener("click", async () => {
    if (!window.confirm("停止后台服务后，自动同步也会停止。确定继续吗？")) return;
    try {
      await api("/api/system/stop", { method: "POST", body: "{}" });
      document.body.innerHTML = '<div class="empty-state"><strong>后台服务已停止</strong><span>可关闭此窗口；双击启动器可再次打开。</span></div>';
    } catch (error) { toast(error.message, "error"); }
  });
}

async function bootstrap() {
  bindEvents();
  syncQueryMode();
  setDefaultSyncWindow();
  const initialView = window.location.hash.slice(1);
  if (viewMeta[initialView]) switchView(initialView);
  await Promise.allSettled([refreshStatus(), refreshSettings()]);
  setQuickRange("24h");
  await refreshQueryTasks();
  state.statusTimer = window.setInterval(refreshStatus, 3000);
  state.queryTaskTimer = window.setInterval(refreshQueryTasks, 1200);
}

document.addEventListener("DOMContentLoaded", bootstrap);
