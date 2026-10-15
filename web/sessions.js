"use strict";

/* 会话管理（RDS）：对齐阿里云 DAS 的会话管理——实例会话（异常 / 运行中 / 总数 / 最长运行）、会话统计（按用户 / 来源 / 数据库）、
   会话列表、三类异常会话（语句运行超过 30 秒、事务超过 30 秒、事务开启后 10 秒没再执行语句）；另加一项 DAS 没有的：有会话在等锁时，
   按快照推断「疑似持锁」的会话。实时部分每次点刷新才读 DAS；历史部分是服务后台每分钟留存的异常会话。
   「结束会话」换成 KILL 语句：服务只有云上只读身份、没有数据库账号，所以本页只生成语句（带核对语句和路由 Hint）让你执行，自己不执行任何 kill。 */

const SESSION_KIND_LABEL = { long_running: "语句运行过久", lock_wait: "等待锁", long_trx: "事务过长", idle_trx: "未提交事务" };
const SESSION_KIND_HINT = {
  long_running: "语句执行超过阈值（默认 30 秒）",
  lock_wait: "状态是在等元数据锁 / 全局读锁等，且超过阈值",
  long_trx: "事务持续超过阈值（默认 30 秒）",
  idle_trx: "事务开启后超过阈值（默认 10 秒）没再执行语句，可能漏了 COMMIT",
};
const SESSION_TABS = [["abnormal", "异常会话"], ["active", "活跃会话"], ["all", "全部会话"]];
const SESSION_PAGE_SIZE = 100;
const SESSION_ERROR_TEXT = {
  DAS_PERMISSION_DENIED: "没有读取 DAS 会话的权限",
  DAS_THROTTLED: "DAS 限流，稍后再刷新",
  DAS_TIMEOUT: "DAS 会话快照超时",
  DAS_UNREACHABLE: "DAS 不可达",
  DAS_FAILED: "DAS 读取失败",
};

const sessionsState = {
  initialized: false,
  autoLoaded: false,
  instanceHtml: "",
  nodeHtml: "",
  mode: "live",
  data: null,
  history: null,
  retention: null,
  tab: "abnormal",
  filters: { q: "", user: "", client: "", database: "", minSeconds: 0 },
  sort: "seconds",
  page: 0,
  selected: new Set(),
  expanded: new Set(),
  killMode: "connection",
  kill: [],
};

/* ---------------------------- 小工具 ---------------------------- */

function sessionSeconds(value) {
  const seconds = Math.max(0, Math.round(Number(value) || 0));
  if (seconds < 60) return `${seconds} 秒`;
  return humanMicros(seconds * 1_000_000);
}

function sessionKey(row) {
  return `${row.node_id}:${row.session_id}`;
}

function sessionTimeText(us) {
  return us ? formatTime(us, true) : "—";
}

function sessionNodeChip(node) {
  const role = node.role ? `<span class="chip role-${escapeHtml(node.role)}">${escapeHtml(nodeRoleText(node.role))}</span>` : "";
  const zone = nodeZoneText(node.zone);
  const id = String(node.node_id || "");
  return `<span class="session-node"><code title="${escapeHtml(id)}">…${escapeHtml(id.slice(-8))}</code> ${role}${zone ? ` <span class="muted">${escapeHtml(zone)}</span>` : ""}</span>`;
}

function sessionKindChips(row) {
  const chips = (row.kinds || []).map((kind) => `<span class="chip warn" title="${escapeHtml(SESSION_KIND_HINT[kind] || "")}">${escapeHtml(SESSION_KIND_LABEL[kind] || kind)}</span>`);
  if (row.holder) {
    const basis = row.holder_basis === "statement" ? "运行中的语句" : "未结束的事务";
    chips.push(`<span class="chip session-holder" title="有会话在等锁，这个会话的${basis}比最久的等待者更早开始，可能是持锁者；只是按快照推断，准确的用核对语句在节点上查">疑似持锁</span>`);
  }
  if (row.protected) chips.push(`<span class="chip" title="${escapeHtml(row.protected)}：不生成 KILL 语句">受保护</span>`);
  return chips.join(" ");
}

function sessionRows(data) {
  return (data?.nodes || []).filter((node) => node.status === "ok").flatMap((node) =>
    (node.sessions || []).map((row) => ({ ...row, node_id: node.node_id, role: node.role, zone: node.zone, observed_at_us: node.observed_at_us })));
}

function sessionInTab(row, tab) {
  if (tab === "abnormal") return (row.kinds || []).length > 0 || Boolean(row.holder);
  if (tab === "active") return Boolean(row.active);
  return true;
}

function sessionMatches(row, filters) {
  if (filters.user && row.user !== filters.user) return false;
  if (filters.client && row.client !== filters.client) return false;
  if (filters.database && row.database !== filters.database) return false;
  if (filters.minSeconds && Number(row.seconds || 0) < filters.minSeconds) return false;
  const q = String(filters.q || "").trim().toLowerCase();
  if (!q) return true;
  return [row.user, row.client, row.database, row.sql, row.state, row.command, row.session_id, row.node_id].some((value) => String(value ?? "").toLowerCase().includes(q));
}

function sessionSorted(rows, sort) {
  const by = { seconds: (r) => Number(r.seconds || 0), trx: (r) => Number(r.trx_seconds || 0), id: (r) => Number(r.session_id || 0) };
  const pick = by[sort] || by.seconds;
  return [...rows].sort((a, b) => (Boolean(b.holder) - Boolean(a.holder)) || ((b.kinds || []).length > 0) - ((a.kinds || []).length > 0) || pick(b) - pick(a)
    || String(a.node_id).localeCompare(String(b.node_id)) || Number(a.session_id || 0) - Number(b.session_id || 0));
}

function sessionVisible() {
  const rows = sessionRows(sessionsState.data).filter((row) => sessionInTab(row, sessionsState.tab) && sessionMatches(row, sessionsState.filters));
  return sessionSorted(rows, sessionsState.sort);
}

/* ---------------------------- 概览与统计 ---------------------------- */

function sessionTotals(data) {
  const ok = (data?.nodes || []).filter((node) => node.status === "ok");
  const sum = (pick) => ok.reduce((total, node) => total + Number(pick(node) || 0), 0);
  const kinds = {};
  for (const kind of Object.keys(SESSION_KIND_LABEL)) kinds[kind] = sum((node) => node.overview?.kinds?.[kind]);
  return {
    nodes: ok.length,
    total: sum((node) => node.overview?.total),
    running: sum((node) => node.overview?.running),
    active: sum((node) => node.overview?.active),
    abnormal: sum((node) => node.overview?.abnormal),
    longest: ok.reduce((peak, node) => Math.max(peak, Number(node.overview?.max_active_seconds || 0)), 0),
    kinds,
  };
}

function sessionSummaryHtml(data) {
  const totals = sessionTotals(data);
  const kindHint = Object.entries(totals.kinds).filter(([, count]) => count).map(([kind, count]) => `${SESSION_KIND_LABEL[kind]} ${humanCount(count)}`).join(" · ");
  const tiles = statTiles([
    { label: "异常会话", value: humanCount(totals.abnormal), hint: kindHint || "没有异常会话" },
    { label: "运行中会话", value: humanCount(totals.running), hint: `活跃（含未结束事务）${humanCount(totals.active)}` },
    { label: "会话总数", value: humanCount(totals.total), hint: `${humanCount(totals.nodes)} 个节点` },
    { label: "运行中最长", value: sessionSeconds(totals.longest) },
  ]);
  const rows = (data?.nodes || []).map((node) => {
    if (node.status !== "ok") {
      const reason = SESSION_ERROR_TEXT[node.code] || node.message || node.code || "读取失败";
      return [sessionNodeChip(node), '<span class="muted">—</span>', "—", "—", "—", "—", `<span class="chip warn" title="${escapeHtml(node.message || "")}">${escapeHtml(reason)}</span>`];
    }
    const overview = node.overview || {};
    const holders = node.holders || {};
    const waiting = holders.waiters ? ` <span class="chip warn" title="有会话在等锁，最久已等 ${escapeHtml(sessionSeconds(holders.oldest_wait_s))}">${humanCount(holders.waiters)} 个在等锁</span>` : "";
    return [sessionNodeChip(node), humanCount(overview.total), humanCount(overview.running), humanCount(overview.active),
      `<strong>${humanCount(overview.abnormal)}</strong>${waiting}`, sessionSeconds(overview.max_active_seconds),
      `${escapeHtml(sessionTimeText(node.observed_at_us))}${node.cached ? ' <span class="muted">（10 秒内缓存）</span>' : ""}`];
  });
  return `${tiles}${analyticsTable(["节点", "会话总数", "运行中", "活跃", "异常", "运行中最长", "快照时间 / 状态"], rows, "没有节点")}`;
}

function sessionHolderHtml(data) {
  const lines = (data?.nodes || []).filter((node) => node.status === "ok" && node.holders?.waiters).map((node) => {
    const holders = node.holders;
    return `<li>${sessionNodeChip(node)}：${humanCount(holders.waiters)} 个会话在等锁，最久已等 <strong>${escapeHtml(sessionSeconds(holders.oldest_wait_s))}</strong>；`
      + (holders.candidates ? `快照里有 ${humanCount(holders.candidates)} 个会话比最久的等待者更早开始，列表里标了「疑似持锁」。` : "快照里没有比最久等待者更早开始的事务或语句，持锁者可能已结束或不在这个节点上。")
      + "</li>";
  });
  if (!lines.length) return "";
  return `<div class="notice warning compact-notice session-holders"><svg viewBox="0 0 24 24"><path d="M12 3 2 21h20L12 3Z"/><path d="M12 9v5M12 18h.01"/></svg><div><strong>有会话在等锁</strong>
    <ul class="session-holder-list">${lines.join("")}</ul>
    <span>推断只用快照里的时长：持有元数据锁的会话一定早于最久的等待者。要确切知道是谁，生成 KILL 语句时附带的核对语句（performance_schema 的 metadata_locks）在节点上查一下。先杀持锁者，不是等待者。</span></div></div>`;
}

function sessionStatsHtml(data) {
  const merged = { user: new Map(), client: new Map(), database: new Map() };
  for (const node of (data?.nodes || []).filter((item) => item.status === "ok")) {
    for (const dimension of Object.keys(merged)) {
      for (const row of node.stats?.[dimension] || []) {
        const entry = merged[dimension].get(row.key) || { key: row.key, total: 0, active: 0, abnormal: 0 };
        entry.total += row.total; entry.active += row.active; entry.abnormal += row.abnormal;
        merged[dimension].set(row.key, entry);
      }
    }
  }
  const titles = { user: ["按用户", "user"], client: ["按访问来源", "client"], database: ["按数据库", "database"] };
  const blocks = Object.entries(titles).map(([dimension, [title, field]]) => {
    const rows = [...merged[dimension].values()].sort((a, b) => b.abnormal - a.abnormal || b.active - a.active || b.total - a.total || String(a.key).localeCompare(String(b.key))).slice(0, 12);
    const picked = sessionsState.filters[field];
    const body = rows.map((row) => `<tr class="session-stat-row${picked === row.key ? " is-picked" : ""}" data-session-filter="${field}" data-session-value="${escapeHtml(row.key)}" title="点击只看这个${title.slice(1)}">
      <td>${escapeHtml(row.key || "（空）")}</td><td>${humanCount(row.total)}</td><td>${humanCount(row.active)}</td><td>${row.abnormal ? `<strong>${humanCount(row.abnormal)}</strong>` : "0"}</td></tr>`).join("");
    return `<div class="session-stat"><h3>${title}</h3><div class="table-wrap"><table class="data-table analytics-table"><thead><tr><th>${escapeHtml(title.slice(1))}</th><th>总数</th><th>活跃</th><th>异常</th></tr></thead><tbody>${body || '<tr><td colspan="4" class="muted">无</td></tr>'}</tbody></table></div></div>`;
  });
  return `<div class="session-stats">${blocks.join("")}</div>`;
}

/* ---------------------------- 会话列表 ---------------------------- */

function sessionRowHtml(row) {
  const key = sessionKey(row);
  const picked = sessionsState.selected.has(key);
  const flagged = (row.kinds || []).length > 0 || row.holder;
  const sql = String(row.sql || "");
  const open = sessionsState.expanded.has(key);
  const shown = open ? sql : sql.replace(/\s+/g, " ").slice(0, 120);
  const more = sql.length > 120 || row.sql_chars > sql.length;
  const trx = row.trx_seconds ? `${escapeHtml(sessionSeconds(row.trx_seconds))}` : '<span class="muted">—</span>';
  return `<tr class="session-row${flagged ? " is-flagged" : ""}${picked ? " is-picked" : ""}">
    <td class="session-check"><input type="checkbox" data-session-pick="${escapeHtml(key)}" aria-label="选择会话 ${escapeHtml(row.session_id)}"${picked ? " checked" : ""}${row.protected ? ` disabled title="${escapeHtml(row.protected)}：不生成 KILL 语句"` : ""}></td>
    <td>${sessionNodeChip(row)}</td>
    <td>${escapeHtml(row.session_id ?? "—")}</td>
    <td>${escapeHtml(row.user)}</td><td>${escapeHtml(row.client)}</td><td>${escapeHtml(row.database)}</td>
    <td>${escapeHtml(row.command)}${row.state ? `<br><span class="muted">${escapeHtml(row.state)}</span>` : ""}</td>
    <td>${escapeHtml(sessionSeconds(row.seconds))}</td><td>${trx}</td>
    <td>${sessionKindChips(row) || '<span class="muted">—</span>'}</td>
    <td class="session-sql">${sql ? `<button type="button" class="slow-sql-link" data-session-sql="${escapeHtml(key)}" title="${more || open ? "点击展开 / 收起" : ""}"><code class="sql-cell${open ? " is-open" : ""}">${escapeHtml(shown)}</code></button>` : '<span class="muted">—</span>'}</td>
  </tr>`;
}

function sessionListHtml() {
  const all = sessionRows(sessionsState.data);
  const counts = Object.fromEntries(SESSION_TABS.map(([tab]) => [tab, all.filter((row) => sessionInTab(row, tab)).length]));
  const tabs = SESSION_TABS.map(([tab, label]) => `<button type="button" data-session-tab="${tab}" class="${sessionsState.tab === tab ? "is-active" : ""}">${label} ${humanCount(counts[tab])}</button>`).join("");
  const visible = sessionVisible();
  const pages = Math.max(1, Math.ceil(visible.length / SESSION_PAGE_SIZE));
  sessionsState.page = Math.min(sessionsState.page, pages - 1);
  const pageRows = visible.slice(sessionsState.page * SESSION_PAGE_SIZE, (sessionsState.page + 1) * SESSION_PAGE_SIZE);
  const filters = sessionsState.filters;
  const chips = ["user", "client", "database"].filter((field) => filters[field]).map((field) =>
    `<button type="button" class="chip session-filter-chip" data-session-clear="${field}" title="取消">${escapeHtml({ user: "用户", client: "来源", database: "库" }[field])}：${escapeHtml(filters[field])} ×</button>`).join(" ");
  const selected = [...sessionsState.selected].filter((key) => all.some((row) => sessionKey(row) === key)).length;
  const truncated = (sessionsState.data?.nodes || []).some((node) => node.truncated);
  const header = `<div class="section-heading session-list-head"><div class="segmented" id="sessions-tabs" aria-label="会话范围">${tabs}</div>
    <div class="session-actions">
      <label class="session-inline"><span>排序</span><select id="sessions-sort"><option value="seconds"${sessionsState.sort === "seconds" ? " selected" : ""}>执行时间</option><option value="trx"${sessionsState.sort === "trx" ? " selected" : ""}>事务时长</option><option value="id"${sessionsState.sort === "id" ? " selected" : ""}>会话 ID</option></select></label>
      <button type="button" class="button secondary compact" data-session-export title="导出当前活跃会话（CSV）">导出活跃会话</button>
      <button type="button" class="button primary compact" data-session-kill${selected ? "" : " disabled"} title="只生成语句，不在本页执行">生成 KILL 语句${selected ? `（${humanCount(selected)}）` : ""}</button>
    </div></div>
    <div class="session-filters"><label class="field"><span>关键词</span><input id="sessions-q" type="search" value="${escapeHtml(filters.q)}" placeholder="用户 / 来源 / 库 / SQL / 状态 / 会话号" autocomplete="off"></label>
      <label class="field"><span>执行时间 ≥（秒）</span><input id="sessions-min" type="number" min="0" step="1" value="${filters.minSeconds || ""}" placeholder="0"></label>
      <div class="session-filter-chips">${chips}</div></div>`;
  const body = pageRows.length ? `<div class="table-wrap"><table class="data-table analytics-table session-table"><thead><tr><th class="session-check"></th><th>节点</th><th>会话 ID</th><th>用户</th><th>来源</th><th>库</th><th>命令 / 状态</th><th>执行时间</th><th>事务时长</th><th>异常原因</th><th>SQL</th></tr></thead>
      <tbody>${pageRows.map(sessionRowHtml).join("")}</tbody></table></div>` : `<p class="analytics-empty">${sessionsState.tab === "abnormal" ? "当前快照里没有异常会话" : "没有符合条件的会话"}。</p>`;
  const pager = pages > 1 ? `<div class="session-pager"><button type="button" class="button secondary compact" data-session-page="-1"${sessionsState.page === 0 ? " disabled" : ""}>上一页</button>
      <span>第 ${sessionsState.page + 1} / ${pages} 页 · 共 ${humanCount(visible.length)} 条</span><button type="button" class="button secondary compact" data-session-page="1"${sessionsState.page >= pages - 1 ? " disabled" : ""}>下一页</button></div>` : "";
  const note = truncated ? '<p class="analytics-note">某些节点的会话太多，列表只保留异常会话、疑似持锁会话和各自最长的一部分；上面的统计覆盖全部会话。</p>' : "";
  return `${header}${body}${pager}${note}`;
}

/* ---------------------------- 实时页 ---------------------------- */

function sessionsPrimaryInstance() {
  return state.status?.primaryInstance?.instanceId || "";
}

function sessionInstances() {
  const seen = new Map();
  for (const item of state.status?.slowLogs || []) {
    if (item.instanceId && item.nodeId && !seen.has(item.instanceId)) seen.set(item.instanceId, item.label || item.instanceId);
  }
  return [...seen.entries()];
}

function sessionNodesOf(instanceId) {
  const nodes = new Map();
  for (const item of state.status?.slowLogs || []) {
    if (item.instanceId === instanceId && item.nodeId && !nodes.has(item.nodeId)) nodes.set(item.nodeId, item);
  }
  return [...nodes.values()].sort((a, b) => ((a.role === "primary" ? 0 : a.role === "secondary" ? 1 : 2) - (b.role === "primary" ? 0 : b.role === "secondary" ? 1 : 2)) || String(a.nodeId).localeCompare(String(b.nodeId)));
}

// The status poll calls this every few seconds: a list is only rewritten when its options changed, so an open dropdown stays open.
function syncSessionControls() {
  const instances = sessionInstances();
  const instance = $("#sessions-instance");
  if (instance) {
    const html = instances.map(([id, label]) => `<option value="${escapeHtml(id)}">${escapeHtml(label)}</option>`).join("");
    if (html !== sessionsState.instanceHtml) {
      const keep = instance.value || "";
      const primary = sessionsPrimaryInstance();
      sessionsState.instanceHtml = html;
      instance.innerHTML = html;
      instance.value = [keep, primary, instances[0]?.[0] || ""].find((id) => id && instances.some(([known]) => known === id)) || "";
    }
  }
  const picked = instance?.value || "";
  const node = $("#sessions-node");
  if (node) {
    const items = sessionNodesOf(picked);
    const html = `<option value="">全部节点</option>${items.map((item) =>
      `<option value="${escapeHtml(item.nodeId)}">${escapeHtml([nodeRoleText(item.role), nodeZoneText(item.zone), item.nodeId].filter(Boolean).join(" · "))}</option>`).join("")}`;
    if (html !== sessionsState.nodeHtml) {
      const keep = node.value || "";
      sessionsState.nodeHtml = html;
      node.innerHTML = html;
      node.value = items.some((item) => item.nodeId === keep) ? keep : "";
    }
  }
}

// The page may be opened before the first status answer arrives: the lists fill in and the first read happens when it does.
function maybeAutoLoadSessions() {
  if (sessionsState.autoLoaded || sessionsState.mode !== "live" || !($("#sessions-instance")?.value)) return;
  sessionsState.autoLoaded = true;
  withBusy($("#sessions-refresh"), runSessions, "读取中…").catch((error) => toast(error.message, "error", 6000));
}

// refreshStatus() calls this inside its own try block: a fault on this page must not turn the service indicator red.
function onSessionsStatus() {
  try {
    syncSessionControls();
    if (state.view === "sessions" && sessionsState.initialized) maybeAutoLoadSessions();
  } catch (error) {
    console.error("sessions: status sync failed", error);
  }
}

function sessionThresholdQuery() {
  const params = new URLSearchParams();
  for (const [id, name] of [["#sessions-blocking", "blockingSeconds"], ["#sessions-trx", "trxSeconds"], ["#sessions-idle", "idleTrxSeconds"]]) {
    const value = String($(id)?.value || "").trim();
    if (value) params.set(name, value);
  }
  return params;
}

function sessionsQueryString() {
  const instance = $("#sessions-instance")?.value || "";
  if (!instance) throw new Error("请先选择一个 RDS 实例");
  const params = sessionThresholdQuery();
  params.set("instance", instance);
  const node = $("#sessions-node")?.value || "";
  if (node) params.set("nodeId", node);
  return params.toString();
}

function renderSessionsLive() {
  const data = sessionsState.data;
  $("#sessions-live").hidden = false;
  $("#sessions-history").hidden = true;
  if (!data) return;
  const when = Math.min(...data.nodes.filter((node) => node.status === "ok").map((node) => Number(node.observed_at_us) || Infinity));
  $("#sessions-meta").textContent = Number.isFinite(when) ? `快照时间 ${formatTime(when)}${data.partial ? " · 有节点读取失败" : ""}` : "没有读到任何节点";
  $("#sessions-summary").innerHTML = sessionSummaryHtml(data);
  $("#sessions-holders").innerHTML = sessionHolderHtml(data);
  $("#sessions-stats").innerHTML = sessionStatsHtml(data);
  $("#sessions-list").innerHTML = sessionListHtml();
}

function renderSessionsList() {
  if (sessionsState.mode === "live") $("#sessions-list").innerHTML = sessionListHtml();
}

async function loadSessions() {
  const query = sessionsQueryString();
  sessionsState.data = await api(`/api/sessions?${query}`);
  const known = new Set(sessionRows(sessionsState.data).map(sessionKey));
  sessionsState.selected = new Set([...sessionsState.selected].filter((key) => known.has(key)));
  sessionsState.page = 0;
  renderSessionsLive();
  refreshSessionsRetention();
}

/* ---------------------------- 留存状态与历史 ---------------------------- */

function sessionRetentionHtml(status) {
  if (!status) return null;
  const nodes = status.nodes || [];
  const failing = nodes.filter((node) => node.consecutive_failures);
  const lastOk = Math.max(0, ...nodes.map((node) => Number(node.last_ok_us) || 0));
  const store = status.store || {};
  if (!status.enabled) {
    return { kind: "warning", html: "<strong>留存已关闭</strong><span>配置文件 data/session-retention.json 里 enabled 为 false：现在只有实时快照，事后看不到当时的异常会话。把它改回 true（或删掉该文件）即可，下个周期生效，不用重启。</span>" };
  }
  const state = failing.length
    ? `${humanCount(failing.length)} / ${humanCount(nodes.length)} 个节点最近读取失败（${escapeHtml(SESSION_ERROR_TEXT[String(failing[0].last_error?.code || "").toUpperCase()] || failing[0].last_error?.code || "未知原因")}），失败时服务自动退避重试`
    : `${humanCount(nodes.length)} 个节点读取正常`;
  return { kind: failing.length ? "warning" : "info", html: `<strong>留存已开启</strong><span>每 ${humanCount(status.interval_seconds)} 秒读一次每个节点，只存异常会话，保留 ${humanCount(status.retention_days)} 天 · ${state}${lastOk ? ` · 最近一次 ${escapeHtml(formatTime(lastOk, true))}` : ""} · 已存 ${humanCount(store.abnormal_sessions)} 条异常会话、${humanCount(store.snapshots)} 次快照</span>` };
}

async function refreshSessionsRetention() {
  try {
    sessionsState.retention = await api("/api/sessions/status");
  } catch (error) {
    sessionsState.retention = null;
    return;
  }
  const strip = $("#sessions-retention");
  const view = sessionRetentionHtml(sessionsState.retention);
  if (!strip || !view) return;
  strip.hidden = false;
  strip.className = `notice ${view.kind} compact-notice`;
  strip.innerHTML = `<svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M12 11v6M12 7h.01"/></svg><div>${view.html}</div>`;
}

function sessionHistoryWindow() {
  const start = new Date($("#sessions-start")?.value).getTime();
  const end = new Date($("#sessions-end")?.value).getTime();
  if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) throw new Error("请选择有效的开始和结束时间");
  return [start * 1000, end * 1000];
}

function setSessionHistoryWindow(startUs, endUs) {
  $("#sessions-start").value = toLocalInput(new Date(startUs / 1000));
  $("#sessions-end").value = toLocalInput(new Date(endUs / 1000));
  $("#sessions-window").value = "custom";
}

function applySessionWindow(hours) {
  const end = Date.now();
  setSessionHistoryWindow((end - hours * 3_600_000) * 1000, end * 1000);
  $("#sessions-window").value = String(hours);
}

function sessionHistoryHtml(data) {
  const interval = Number(data.retention?.interval_seconds) || 60;
  const expected = Math.max(1, Math.round((data.end_us - data.start_us) / (interval * 1_000_000)));
  const coverage = new Map((data.coverage || []).map((item) => [item.node_id, item]));
  const lines = (data.nodes || []).map((node) => {
    const item = coverage.get(node.node_id);
    const count = item ? item.snapshots : 0;
    const ratio = count / expected;
    return `<li>${sessionNodeChip(node)}：${humanCount(count)} 次快照（这个窗口里应有约 ${humanCount(expected)} 次）${ratio < 0.7 ? ' <span class="chip warn" title="空白不等于没有异常">留存有缺口</span>' : ""}</li>`;
  }).join("");
  const thin = (data.nodes || []).some((node) => (coverage.get(node.node_id)?.snapshots || 0) / expected < 0.7);
  const head = `<div class="notice ${thin ? "warning" : "info"} compact-notice"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="9"/><path d="M12 11v6M12 7h.01"/></svg><div><strong>留存覆盖</strong>
    <ul class="session-holder-list">${lines || "<li>这个窗口里没有任何快照。</li>"}</ul>${thin ? "<span>快照少于应有数量的 70%：留存刚开启，或者那段时间读取失败过。看不到异常不代表当时没有异常。</span>" : ""}</div></div>`;
  const rows = (data.sessions || []).map((row) => {
    const span = Math.max(0, Number(row.last_us) - Number(row.first_us));
    const kinds = (row.kinds || []).map((kind) => `<span class="chip warn" title="${escapeHtml(SESSION_KIND_HINT[kind] || "")}">${escapeHtml(SESSION_KIND_LABEL[kind] || kind)}</span>`).join(" ");
    const holder = row.holder ? ' <span class="chip session-holder" title="当时有会话在等锁，这个会话可能是持锁者（推断）">疑似持锁</span>' : "";
    const sql = String(row.sql || "").replace(/\s+/g, " ").slice(0, 160);
    return [sessionNodeChip(data.nodes.find((node) => node.node_id === row.node_id) || { node_id: row.node_id }), escapeHtml(row.session_id),
      escapeHtml(row.user), escapeHtml(row.client), escapeHtml(row.database),
      `${escapeHtml(sessionTimeText(row.first_us))}<br><span class="muted">→ ${escapeHtml(sessionTimeText(row.last_us))}（${escapeHtml(span ? sessionSeconds(span / 1_000_000) : "单次快照")}）</span>`,
      `${escapeHtml(sessionSeconds(row.max_seconds))}${row.max_trx_seconds ? `<br><span class="muted">事务 ${escapeHtml(sessionSeconds(row.max_trx_seconds))}</span>` : ""}`,
      `${kinds}${holder}`, `${escapeHtml(row.command)}${row.state ? `<br><span class="muted">${escapeHtml(row.state)}</span>` : ""}`,
      sql ? `<code class="sql-cell">${escapeHtml(sql)}</code>` : '<span class="muted">—</span>',
      `<button type="button" class="button ghost compact" data-session-around="${escapeHtml(String(row.first_us))}" title="以这一刻为中心看前后 2 分钟里的所有异常会话">同时段</button>`];
  });
  const table = `<div class="session-history-table">${analyticsTable(["节点", "会话 ID", "用户", "来源", "库", "首次 → 最后看到", "最长执行", "异常原因", "命令 / 状态", "SQL", ""], rows, "这个窗口里没有留存到异常会话")}</div>`;
  const more = data.truncated ? '<p class="analytics-note">只列出最近的一部分；缩小时间窗或选一个节点可以看到更多。</p>' : "";
  return `${head}${table}${more}`;
}

async function loadSessionHistory() {
  const instance = $("#sessions-instance")?.value || "";
  if (!instance) throw new Error("请先选择一个 RDS 实例");
  const [start, end] = sessionHistoryWindow();
  const params = new URLSearchParams({ instance, startEpochUs: String(start), endEpochUs: String(end), limit: "300" });
  const node = $("#sessions-node")?.value || "";
  if (node) params.set("nodeId", node);
  sessionsState.history = await api(`/api/sessions/history?${params}`);
  renderSessionsHistory();
  refreshSessionsRetention();
}

function renderSessionsHistory() {
  $("#sessions-live").hidden = true;
  $("#sessions-history").hidden = false;
  const data = sessionsState.history;
  $("#sessions-meta").textContent = data ? `${formatTime(data.start_us)} → ${formatTime(data.end_us)}` : "";
  $("#sessions-history").innerHTML = data ? sessionHistoryHtml(data) : "";
}

function setSessionsMode(mode) {
  sessionsState.mode = mode;
  for (const button of $$("[data-session-mode]")) button.classList?.toggle("is-active", button.dataset.sessionMode === mode);
  for (const field of $$("[data-session-live-only]")) field.hidden = mode !== "live";
  for (const field of $$("[data-session-history-only]")) field.hidden = mode !== "history";
  if (mode === "history" && !$("#sessions-start")?.value) applySessionWindow(1);
  if (mode === "live") renderSessionsLive(); else renderSessionsHistory();
}

async function runSessions() {
  if (sessionsState.mode === "history") await loadSessionHistory(); else await loadSessions();
}

/* ---------------------------- KILL 语句与导出 ---------------------------- */

function sessionSelectedRows() {
  const wanted = sessionsState.selected;
  return sessionRows(sessionsState.data).filter((row) => wanted.has(sessionKey(row)));
}

async function generateSessionKill() {
  const rows = sessionSelectedRows();
  if (!rows.length) throw new Error("请先勾选要结束的会话");
  const instance = sessionsState.data.instance_id;
  const byNode = new Map();
  for (const row of rows) byNode.set(row.node_id, [...(byNode.get(row.node_id) || []), row]);
  const results = [];
  for (const [nodeId, picked] of byNode) {
    const node = sessionsState.data.nodes.find((item) => item.node_id === nodeId);
    const written = await api("/api/sessions/kill-statements", {
      method: "POST",
      body: JSON.stringify({
        instance, node_id: nodeId, mode: sessionsState.killMode, snapshot_us: node.observed_at_us,
        sessions: picked.map((row) => ({ session_id: row.session_id, user: row.user, client: row.client, database: row.database, command: row.command,
          seconds: row.seconds, trx_seconds: row.trx_seconds, kinds: row.kinds })),
      }),
    });
    results.push({ node, ...written });
  }
  sessionsState.kill = results;
  showSessionKill();
}

function sessionKillHtml() {
  const mode = sessionsState.killMode;
  const modes = `<div class="segmented" id="sessions-kill-mode"><button type="button" data-session-killmode="connection" class="${mode === "connection" ? "is-active" : ""}" title="KILL：结束连接并回滚它没提交的事务">KILL（结束会话）</button><button type="button" data-session-killmode="query" class="${mode === "query" ? "is-active" : ""}" title="KILL QUERY：只终止当前语句，连接和事务还在，对空闲事务无效">KILL QUERY（只终止语句）</button></div>`;
  const blocks = sessionsState.kill.map((item, index) => {
    const primary = item.node.role === "primary";
    const warn = primary ? '<p class="session-kill-warn">这是<strong>主节点</strong>：杀错会话会直接影响线上写入，务必先跑核对语句。</p>' : "";
    const skipped = (item.skipped || []).length ? `<p class="analytics-note">跳过 ${humanCount(item.skipped.length)} 个受保护会话：${escapeHtml(item.skipped.map((row) => `${row.session_id}（${row.reason}）`).join("、"))}。</p>` : "";
    return `<section class="detail-block"><h3>${sessionNodeChip(item.node)}</h3>${warn}
      <textarea class="session-kill-text" readonly rows="${Math.min(24, Math.max(8, String(item.text).split("\n").length + 1))}" spellcheck="false" aria-label="KILL 语句">${escapeHtml(item.text)}</textarea>
      <div class="session-kill-actions"><button type="button" class="button primary compact" data-session-copy="${index}"${item.statements.length ? "" : " disabled"}>复制语句</button>
      <span class="muted">${humanCount(item.statements.length)} 条 KILL · 快照 ${escapeHtml(sessionTimeText(item.snapshot_us))}</span></div>${skipped}</section>`;
  }).join("");
  return `<p class="analytics-note">本页只生成语句，不执行任何 kill：服务只有云上只读身份、没有数据库账号。每条语句带读写分离的 Hint（备节点 FORCE_NODE、主节点 FORCE_MASTER），走读写分离（代理）地址的连接才会落到<strong>对应节点</strong>，在主地址的连接上它只是注释。先跑第 0 步自检，read_only 对不上就停；再跑第 1 条核对语句，确认会话还在、仍是快照里的那个，再杀，杀完重跑核对。</p>
    ${modes}${blocks}`;
}

function showSessionKill() {
  $("#detail-title").textContent = "KILL 语句";
  $("#detail-body").innerHTML = sessionKillHtml();
  showDetailDrawer();
}

async function regenerateSessionKill(mode) {
  sessionsState.killMode = mode;
  await generateSessionKill();
}

function copySessionKill(index) {
  const item = sessionsState.kill[index];
  if (!item) return;
  const done = () => toast("已复制 KILL 语句", "success");
  if (typeof navigator !== "undefined" && navigator.clipboard?.writeText) {
    navigator.clipboard.writeText(item.text).then(done, () => toast("复制失败，请手动选中文本复制", "error", 6000));
    return;
  }
  const area = $$("textarea.session-kill-text")[index];
  if (area) { area.select(); document.execCommand?.("copy"); done(); }
}

function sessionCsvCell(value) {
  let text = String(value ?? "");
  if (/^[=+\-@\t\r]/.test(text)) text = `'${text}`;   // a spreadsheet must not run a session's SQL as a formula
  return /[",\n\r]/.test(text) ? `"${text.replaceAll('"', '""')}"` : text;
}

function sessionsCsv(rows) {
  const head = ["节点", "角色", "会话ID", "用户", "来源", "数据库", "命令", "状态", "执行秒", "事务秒", "事务ID", "异常原因", "SQL"];
  const lines = rows.map((row) => [row.node_id, nodeRoleText(row.role), row.session_id, row.user, row.client, row.database, row.command, row.state, row.seconds,
    row.trx_seconds, row.trx_id, (row.kinds || []).map((kind) => SESSION_KIND_LABEL[kind] || kind).join("/"), String(row.sql || "").replace(/\s+/g, " ")]);
  return [head, ...lines].map((line) => line.map(sessionCsvCell).join(",")).join("\r\n");
}

function exportActiveSessions() {
  const rows = sessionSorted(sessionRows(sessionsState.data).filter((row) => row.active), "seconds");
  if (!rows.length) { toast("当前快照里没有活跃会话", "info"); return; }
  const blob = new Blob(["\uFEFF", sessionsCsv(rows)], { type: "text/csv;charset=utf-8" });
  const link = document.createElement("a");
  link.href = URL.createObjectURL(blob);
  link.download = `sessions-active-${new Date().toISOString().slice(0, 19).replace(/[:T]/g, "")}.csv`;
  document.body.append(link);
  link.click();
  link.remove();
  setTimeout(() => URL.revokeObjectURL(link.href), 1000);
}

/* ---------------------------- 事件 ---------------------------- */

function sessionsFilterBy(field, value) {
  sessionsState.filters[field] = sessionsState.filters[field] === value ? "" : value;
  sessionsState.page = 0;
  if (sessionsState.mode === "live") renderSessionsLive();
}

function sessionsClick(event) {
  const target = event.target;
  const find = (selector) => target.closest?.(selector);
  const mode = find("[data-session-mode]");
  if (mode) { setSessionsMode(mode.dataset.sessionMode); return; }
  const tab = find("[data-session-tab]");
  if (tab) { sessionsState.tab = tab.dataset.sessionTab; sessionsState.page = 0; renderSessionsList(); return; }
  const filter = find("[data-session-filter]");
  if (filter) { sessionsFilterBy(filter.dataset.sessionFilter, filter.dataset.sessionValue); return; }
  const clear = find("[data-session-clear]");
  if (clear) { sessionsState.filters[clear.dataset.sessionClear] = ""; renderSessionsLive(); return; }
  const sql = find("[data-session-sql]");
  if (sql) {
    const key = sql.dataset.sessionSql;
    if (sessionsState.expanded.has(key)) sessionsState.expanded.delete(key); else sessionsState.expanded.add(key);
    renderSessionsList();
    return;
  }
  const page = find("[data-session-page]");
  if (page) { sessionsState.page = Math.max(0, sessionsState.page + Number(page.dataset.sessionPage)); renderSessionsList(); return; }
  if (find("[data-session-export]")) { exportActiveSessions(); return; }
  if (find("[data-session-kill]")) { generateSessionKill().catch((error) => toast(error.message, "error", 6000)); return; }
  const killMode = find("[data-session-killmode]");
  if (killMode) { regenerateSessionKill(killMode.dataset.sessionKillmode).catch((error) => toast(error.message, "error", 6000)); return; }
  const copy = find("[data-session-copy]");
  if (copy) { copySessionKill(Number(copy.dataset.sessionCopy)); return; }
  const around = find("[data-session-around]");
  if (around) {
    const at = Number(around.dataset.sessionAround);
    setSessionHistoryWindow(at - 120_000_000, at + 120_000_000);
    loadSessionHistory().catch((error) => toast(error.message, "error", 6000));
  }
}

function sessionsChange(event) {
  const target = event.target;
  if (target.dataset?.sessionPick !== undefined) {
    const key = target.dataset.sessionPick;
    if (target.checked) sessionsState.selected.add(key); else sessionsState.selected.delete(key);
    renderSessionsList();
    return;
  }
  if (target.id === "sessions-sort") { sessionsState.sort = target.value; sessionsState.page = 0; renderSessionsList(); return; }
  if (target.id === "sessions-instance") { syncSessionControls(); return; }
  if (target.id === "sessions-window" && target.value !== "custom") applySessionWindow(Number(target.value));
}

function sessionsInput(event) {
  const target = event.target;
  if (target.id === "sessions-q") { sessionsState.filters.q = target.value; sessionsState.page = 0; renderSessionsList(); }
  if (target.id === "sessions-min") { sessionsState.filters.minSeconds = Math.max(0, Number(target.value) || 0); sessionsState.page = 0; renderSessionsList(); }
}

function initSessionsView() {
  if (sessionsState.initialized) { onSessionsStatus(); return; }
  sessionsState.initialized = true;
  syncSessionControls();
  const view = $("#view-sessions");
  view?.addEventListener("click", sessionsClick);
  view?.addEventListener("change", sessionsChange);
  view?.addEventListener("input", sessionsInput);
  $("#sessions-form")?.addEventListener("submit", async (event) => {
    event.preventDefault();
    try { await withBusy($("#sessions-refresh"), runSessions, "读取中…"); } catch (error) { toast(error.message, "error", 6000); }
  });
  setSessionsMode("live");
  refreshSessionsRetention();
  maybeAutoLoadSessions();
}
