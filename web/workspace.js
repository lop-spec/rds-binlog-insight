/* Progressive disclosure and SQL-level detail; existing form IDs/API stay intact. */
function setupWorkspaceControls() {
  // Node is an optional scope, not a second instance selector.
  document.querySelector('#analytics-filters .query-grid').prepend($('#analytics-node-field'));
  const maintenance = document.createElement('details');
  maintenance.className = 'nav-management';
  maintenance.innerHTML = '<summary>管理</summary>';
  for (const view of ['jobs', 'storage', 'settings']) maintenance.append($(`[data-view="${view}"]`));
  $('.nav-list').append(maintenance);
  for (const type of ['analytics', 'audit']) {
    const form = $(type === 'analytics' ? '#analytics-form' : '#query-form');
    const refresh = () => updateFilterSummary(type);
    form.addEventListener('input', refresh);
    form.addEventListener('change', refresh);
    form.addEventListener('click', (event) => {
      if (event.target.closest('[id$="-reset"]')) setTimeout(refresh, 0);
    });
    const prefix = type === 'analytics' ? 'analytics' : 'filter';
    for (const edge of ['start', 'end']) {
      $(`#${prefix}-${edge}`).addEventListener('input', () => { $(`#${type}-range`).value = 'custom'; });
    }
    refresh();
  }
  document.addEventListener('click', (event) => {
    for (const menu of $$('.action-menu[open]')) {
      if (!menu.contains(event.target) || event.target.closest('button')) menu.open = false;
    }
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') $$('.action-menu[open]').forEach(menu => { menu.open = false; menu.querySelector('summary').focus(); });
    if (event.key === 'Tab' && $('#detail-drawer').classList.contains('is-open')) {
      const nodes = [...$('#detail-drawer').querySelectorAll('button, summary, a, input, select')].filter(node => node.checkVisibility());
      const target = event.shiftKey ? nodes.at(-1) : nodes[0];
      if (document.activeElement === (event.shiftKey ? nodes[0] : nodes.at(-1))) { event.preventDefault(); target?.focus(); }
    }
  });
  syncAnalyticsMode();
}

function updateFilterSummary(type) {
  const disclosure = $(`#${type}-filters`);
  const chips = [];
  for (const input of disclosure.querySelectorAll('input, select')) {
    if (input.disabled || input.type === 'checkbox') continue;
    const value = input.value.trim();
    const defaultValue = { 'analytics-limit': '50', 'filter-query-mode': 'keyword', 'filter-keyword-mode': 'AND' }[input.id] || '';
    if (value === defaultValue) continue;
    const label = input.closest('label')?.querySelector('span')?.textContent || '';
    const text = input.tagName === 'SELECT' ? input.selectedOptions[0]?.textContent : value;
    chips.push(`${label}：${text}`);
  }
  for (const input of disclosure.querySelectorAll('input[type="checkbox"]:checked')) chips.push(input.value);
  disclosure.querySelector('.filter-count').textContent = chips.length ? `· ${chips.length} 项` : '';
  const summary = $(`#${type}-filter-summary`);
  summary.hidden = !chips.length;
  summary.textContent = chips.join(' · ');
}

function compactStatTiles(items) {
  return `<div class="compact-kpis">${statTiles(items.slice(0, 4))}<details class="analytics-more"><summary>更多指标</summary>${statTiles(items.slice(4))}</details></div>`;
}

function sqlSorter(labels, order) {
  return `<label class="range-picker">排序<select data-sql-sort aria-label="SQL 排序">${labels.map(([key, label]) => `<option value="${key}"${key === order ? ' selected' : ''}>${escapeHtml(label)}</option>`).join('')}</select></label>`;
}

async function changeSqlOrder(select) {
  const sql = state.analytics?.sql;
  if (!sql || select.value === sql.order) return;
  const key = select.value;
  if (!sql.orders?.[key]) {
    console.warn('SQL order snapshot unavailable; keeping existing result:', key);
    toast('当前结果缺少该排序快照，请重新分析', 'error');
    select.value = sql.order || 'executions';
    return;
  }
  const kind = analyticsKind(state.analytics);
  state.sqlOrders = {...state.sqlOrders, [kind]: key};
  sql.order = key;
  sql.statements = sql.orders[key];
  $(kind === 'slowlog' ? '#analytics-panel-sql' : '#analytics-panel-writes').innerHTML = renderAnalyticsSql(sql);
}

const CORRELATION_STATUS = {
  insufficient_buckets: '不足 6 个完整桶', constant_sql: 'SQL 增量无变化',
  constant_total: '总体增量无变化', series_unavailable: '时序数据缺失',
  incomplete_coverage: '日志索引未完整', unavailable: '相关度不可用',
};
function coefficient(value, digits = 4) {
  return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(digits) : '—';
}
function correlationCell(item) {
  const c = item.correlation || {};
  const valid = state.analytics?.coverage?.complete === true && state.analytics?.coverage?.total_parts > 0;
  const reason = !valid ? '日志索引未完整' : (CORRELATION_STATUS[c.status] || '相关度不可用');
  const title = valid && c.value != null ? `增量 Pearson r=${coefficient(c.value, 8)}；不是因果或贡献率，点击核对` : reason;
  return `<button class="correlation-cell" type="button" data-slow-fingerprint="${escapeHtml(item.fingerprint)}" title="${escapeHtml(title)}"><strong>${valid ? coefficient(c.value) : '—'}</strong><span>${valid && c.value != null ? '增量相关' : escapeHtml(reason)}</span></button>`;
}

const RESOURCE_STATUS = {
  overlapping_baseline: '窗口与昨日同窗重叠，请缩至 24 小时以内；不计算重叠样本的增长归因',
  incomplete_baseline_index: '昨日同窗索引不完整，不计算增长关联', baseline_outside_retention: '昨日同窗超出数据保留期',
  baseline_event_limit_exceeded: '昨日同窗超过 25 万条执行，请缩小窗口', no_growth_overlap: '没有可计算的增长负载重合',
  incomplete_index: '索引尚未完整', event_limit_exceeded: '超过 25 万条执行，请缩小窗口',
  single_instance_required: '请先选择一个实例', slowlog_source_required: '仅支持 RDS 慢日志',
  clickhouse_backend_required: '当前查询引擎不支持节点负载分析',
  window_exceeds_seven_days: '请选择不超过 7 天的窗口', invalid_window: '分析窗口无效',
  metric_gaps: '节点指标不完整，不排名', no_excess_overlap: '没有可计算的超基线负载重合',
  metric_or_input_unavailable: '云监控或输入不可用，请查服务日志',
  no_collected_executions: '窗口内没有已采集执行', insufficient_complete_periods: '不足 6 个完整分钟',
};

function podDetail(value) {
  const data = value || {status: 'inventory_unconfigured', pods: []};
  const labels = {inventory_unconfigured: '未接入生产 Pod 元数据', ip_not_observed: 'IP 未观测到（可能已销毁、宿主机或 NAT）',
    historical_binding_unverified: '历史绑定未证实；以下仅为候选', ambiguous_ip: 'IP 对应多个 Pod，不能唯一归属',
    instance_unconfigured: '此实例尚未绑定 Pod 元数据源', invalid_inventory: 'Pod 元数据无效', invalid_client_ip: '客户端地址不是有效 IP', inventory_too_large: 'Pod 元数据超过安全限额'};
  const pods = data.pods || [];
  return detailMeta('Pod 归属', data.status === 'observed_interval' ? '落在已观测绑定区间（轮询间变化仍不可排除）' : (labels[data.status] || data.status))
    + pods.map(pod => detailMeta('Pod / Namespace', `${pod.namespace} / ${pod.name}`)
      + detailMeta('集群 / 工作负载', `${pod.cluster} / ${pod.owner || '—'}`)
      + detailMeta('Node / Host IP', `${pod.node} / ${pod.host_ip}`)
      + detailMeta('Pod UID', pod.uid)
      + detailMeta('绑定观测区间', `${epochMicrosText(pod.first_seen_us)} → ${epochMicrosText(pod.last_seen_us)}`)).join('');
}

function sqlAggregateDetail(item, data) {
  const c = item.correlation || {};
  const meta = data.correlation || {};
  const valid = state.analytics?.coverage?.complete === true && state.analytics?.coverage?.total_parts > 0;
  const value = key => valid ? coefficient(c[key], 8) : '—';
  const count = Number(item.executions || 0), scanned = Number(item.scan_rows || 0), sent = Number(item.rows_sent || 0);
  const full = (data.trend || []).filter(row => !row.partial);
  const sequence = full.map((point, i) => [
    escapeHtml(formatTime(Number(point.ts))), humanCount(c.counts?.[i] ?? 0), humanCount(point.events),
    i && c.counts?.length ? String(c.counts[i] - c.counts[i - 1]) : '—',
    i ? String(point.events - full[i - 1].events) : '—',
  ]);
  return `<section class="detail-block sql-aggregate"><h3>SQL 明细 · 当前分析窗口汇总</h3>
    <div class="detail-grid">
      ${detailMeta('SQL ID', item.sql_id)}${detailMeta('执行次数', humanCount(count))}
      ${detailMeta('实际扫描 / 单次', `${humanCount(scanned)} / ${count ? humanCount(Math.round(scanned / count)) : '—'}`)}
      ${detailMeta('返回行数 / 扫描返回比', `${humanCount(sent)} / ${sent ? (scanned / sent).toFixed(2) + '×' : scanned ? '∞' : '—'}`)}
      ${detailMeta('累计 / 平均耗时', `${millisText(item.query_time_ms_total)} / ${count ? millisText(item.query_time_ms_total / count) : '—'}`)}
      ${detailMeta('最大耗时 / 累计锁等待', `${millisText(item.query_time_ms_max)} / ${millisText(item.lock_time_ms_total)}`)}
    </div>
    <h3>与慢 SQL 增长曲线的相关度</h3>
    <div class="detail-grid">
      ${detailMeta('增量 Pearson r', value('value'))}
      ${detailMeta('次数 Pearson r', value('level_value'))}
      ${detailMeta('剔除自身后的增量 r', value('without_self_value'))}
      ${detailMeta('完整桶内次数占比（非增长贡献）', valid && c.event_share != null ? `${(c.event_share * 100).toFixed(4)}%` : '—')}
      ${detailMeta('粒度 / 完整桶 / 增量对', `${humanMicros(meta.bucket_us || 0)} / ${meta.complete_buckets ?? 0} / ${meta.difference_pairs ?? 0}`)}
      ${detailMeta('活跃桶 / 排除首尾桶', `${c.active_buckets ?? 0} / ${meta.excluded_partial_buckets ?? 0}`)}
    </div>
    ${!valid || c.status !== 'ok' || c.without_self_status !== 'ok' ? `<p class="analytics-note">${!valid ? '日志索引未完整，不计算相关度。' : c.status !== 'ok' ? escapeHtml(CORRELATION_STATUS[c.status] || '相关度不可用') + '。' : ''}${c.without_self_status !== 'ok' ? '剔除自身系数不可用：' + escapeHtml(CORRELATION_STATUS[c.without_self_status] || '时序无变化') + '。' : ''}</p>` : ''}
    <p class="analytics-note">x = 该指纹每桶次数；y = 当前筛选范围内所有慢 SQL 每桶次数（不只 Top N，含自身）。主指标 r = corr(Δx, Δy)；剔除自身指标 = corr(Δx, Δ(y−x))。只取完整等宽桶，零记录补 0；相关不等于因果，高相关不等于增长贡献。单次尖峰和少量样本可能不稳定；6 桶是最低显示门槛，不是统计显著性保证。采集延迟、筛选范围和粒度都会影响结果。</p>
    <details class="analytics-more"><summary>核对计算序列（${full.length} 桶）</summary>${valid && c.counts?.length === full.length ? analyticsTable(['桶开始', '该 SQL', '全部慢 SQL', 'SQL 增量', '总体增量'], sequence) : '<p>完整序列不可用</p>'}</details>
    <details class="analytics-more"><summary>归一化 SQL / 指纹</summary>${detailBlock('归一化 SQL', item.normalized_sql)}${detailBlock('指纹', item.fingerprint)}</details>
    </section>`;
}

function showDetailDrawer() {
  const drawer = $('#detail-drawer');
  if (!drawer.classList.contains('is-open')) state.detailReturnFocus = document.activeElement;
  drawer.inert = false;
  drawer.classList.add('is-open');
  drawer.setAttribute('aria-hidden', 'false');
  $('#drawer-backdrop').hidden = false;
  $('#close-drawer').focus();
  $('.app-shell').inert = true;
}

async function openSlowSqlDetail(fingerprint) {
  const data = state.analytics?.sql || {};
  const item = (data.statements || []).find(row => row.fingerprint === fingerprint);
  if (!item) return;
  const eventId = data.order === 'scan_rows' ? item.max_scan_event_id || item.sample_event_id
    : data.order === 'exec_time' ? item.max_query_event_id || item.sample_event_id : item.sample_event_id;
  if (eventId) {
    if (!await openDetail(eventId, '', item.instance_id || '')) return;
  } else {
    $('#detail-title').textContent = 'SQL 明细';
    $('#detail-body').innerHTML = '<p class="analytics-note">原始执行样本不可用。</p>';
    showDetailDrawer();
  }
  $('#detail-body').insertAdjacentHTML('afterbegin', sqlAggregateDetail(item, data));
}

/* 来源 IP 的 Pod / ECS 名称：渲染时只放占位，页面上出现新占位就按时间点合并成一次 /api/ip-names 查询再填上。
   名称来自服务每分钟一次的 ACK Pod 列表和每 10 分钟一次的 ECS 列表；同一 IP 当时没有唯一 Pod、也不是 ECS 地址就留空。 */
const ipNames = new Map(); // `${ip}@${分钟或 now}` → {value, at}
const ipNamesPending = new Set();
const IP_NAME_LIVE_MS = 5 * 60 * 1000;

function ipOf(value) {
  const match = String(value ?? '').trim().match(/^\[?(\d{1,3}(?:\.\d{1,3}){3})\]?(?::\d+)?$/);
  return match ? match[1] : '';
}

function ipNameKey(ip, atUs) {
  return `${ip}@${atUs ? Math.floor(Number(atUs) / 60_000_000) : 'now'}`;
}

function ipNameText(hit) {
  if (!hit) return ['', ''];
  return hit.kind === 'pod'
    ? [hit.name, `Pod ${hit.namespace}/${hit.name} · 集群 ${hit.cluster} · 节点 ${hit.node}`]
    : [`ECS ${hit.name}`, `ECS 实例 ${hit.name}（${hit.instance_id}）`];
}

function ipNameCached(ip, atUs) {
  const entry = ipNames.get(ipNameKey(ip, atUs));
  if (!entry || (!atUs && Date.now() - entry.at > IP_NAME_LIVE_MS)) return undefined;
  return entry.value;
}

// 只放名称占位（块级，显示在 IP 下面）；atUs 为空表示「现在」。
function ipNameSlot(value, atUs = 0) {
  const ip = ipOf(value);
  if (!ip) return '';
  const [text, title] = ipNameText(ipNameCached(ip, atUs));
  return `<small class="ip-name" data-ip-name="${ip}" data-ip-at="${atUs ? Number(atUs) : ''}"${title ? ` title="${escapeHtml(title)}"` : ''}>${escapeHtml(text)}</small>`;
}

function ipCell(value, atUs = 0) {
  return `${escapeHtml(value || '—')}${ipNameSlot(value, atUs)}`;
}

async function fillIpNames() {
  const groups = new Map();
  for (const slot of document.querySelectorAll('[data-ip-name]:empty')) {
    const ip = slot.dataset.ipName, at = slot.dataset.ipAt || '';
    const key = ipNameKey(ip, at);
    if (ipNameCached(ip, at) !== undefined) {
      const [text, title] = ipNameText(ipNameCached(ip, at));
      if (text) { slot.textContent = text; slot.title = title; }
      continue;
    }
    if (ipNamesPending.has(key)) continue;
    ipNamesPending.add(key);
    if (!groups.has(at)) groups.set(at, new Set());
    groups.get(at).add(ip);
  }
  for (const [at, ips] of groups) {
    const list = [...ips];
    try {
      const params = new URLSearchParams({ ips: list.join(',') });
      if (at) params.set('atUs', at);
      const data = await api('/api/ip-names?' + params);
      for (const ip of list) ipNames.set(ipNameKey(ip, at), { value: data.names?.[ip] || null, at: Date.now() });
    } catch {
      // 查不到名称不影响页面，下一次渲染再试
    } finally {
      for (const ip of list) ipNamesPending.delete(ipNameKey(ip, at));
    }
  }
  if (groups.size) {
    for (const slot of document.querySelectorAll('[data-ip-name]:empty')) {
      const hit = ipNameCached(slot.dataset.ipName, slot.dataset.ipAt || '');
      const [text, title] = ipNameText(hit);
      if (text) { slot.textContent = text; slot.title = title; }
    }
  }
}

let ipNamesTimer = 0;
if (typeof MutationObserver === 'function' && typeof document !== 'undefined' && document.documentElement) {
  new MutationObserver(() => {
    if (!ipNamesTimer) ipNamesTimer = setTimeout(() => { ipNamesTimer = 0; fillIpNames(); }, 50);
  }).observe(document.documentElement, { childList: true, subtree: true });
}
