/* Progressive disclosure and SQL-level detail; existing form IDs/API stay intact. */
function setupWorkspaceControls() {
  $('#analytics-form .menu-content').insertAdjacentHTML('beforeend', '<button id="resource-analysis" class="button ghost" type="button">对齐节点 IOPS（性能关联）</button>');
  $('#resource-analysis').addEventListener('click', openResourceAnalysis);
  document.addEventListener('click', async event => {
    const button = event.target.closest('[data-resource-event]');
    if (button) await openDetail(button.dataset.resourceEvent, '', button.dataset.instance);
  });
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
  syncAnalyticsMode($('#analytics-source').value === 'slowlog');
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
  if (labels === SLOWLOG_ORDER_LABELS) labels = [['performance', '性能关联（昨日基线）'], ...labels];
  return `<label class="range-picker">排序<select data-sql-sort aria-label="SQL 排序">${labels.map(([key, label]) => `<option value="${key}"${key === order ? ' selected' : ''}>${escapeHtml(label)}</option>`).join('')}</select></label>`;
}

async function changeSqlOrder(select) {
  const sql = state.analytics?.sql;
  if (!sql || select.value === sql.order) return;
  const key = select.value;
  state.performanceRequest = (state.performanceRequest || 0) + 1;
  if (key === 'performance') { await showPerformanceRanking(); return; }
  if (!sql.orders?.[key]) {
    console.warn('SQL order snapshot unavailable; keeping existing result:', key);
    toast('当前结果缺少该排序快照，请重新分析', 'error');
    select.value = sql.order || 'executions';
    return;
  }
  state.sqlOrder = sql.order = key;
  sql.statements = sql.orders[key];
  $('#analytics-panel-sql').innerHTML = renderAnalyticsSql(sql);
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

function renderResourceAnalysis(result) {
  const nodes = result.nodes || [];
  const intro = '<p class="analytics-note">确定性性能关联排序，不调用模型，也不判定因果。先计算每分钟“执行微秒 × 高于窗口 P20 基线的 IOPS 使用率”，再乘以相比昨日同窗的正向耗时增长比例；常态慢而没有增长的 SQL 不冒充增长主因。同库同 SQL ID 的数字分表归为一个 SQL 家族，保留实际指纹。增长关联份额不是 IOPS 贡献率，耗时可能包含等待；r 仅供核对，不单独作为排序。分钟指标 t 对齐前一分钟，不搜索延迟。仅覆盖窗口内启动且已采集的慢 SQL；索引 100% 不代表采集无遗漏。</p>';
  if (!nodes.length) return intro + `<p>${escapeHtml(RESOURCE_STATUS[result.status] || result.status || '结果不可用')}</p>`;
  return intro + `<p>${epochMicrosText(result.start_us)} → ${epochMicrosText(result.end_us)} · 当前 / 昨日同窗 ${humanCount(result.executions)} / ${humanCount(result.baseline_executions)} 条执行 · 每节点所有 SQL 家族先排名，再展示前 10</p>` + nodes.map(node => {
    const rows = (node.statements || []).map(row => [String(row.rank ?? '—'),
      `<details><summary>${escapeHtml((row.normalized_sql || row.fingerprint).slice(0, 100))}</summary><p>${row.member_fingerprints?.length || 1} 个实际指纹；SQL 为最慢执行样本，不将家族总量归给样本表。</p>${detailBlock('实际指纹', (row.member_fingerprints || []).join('\n'))}${detailBlock('SQL', row.normalized_sql)}${detailBlock('SQL ID', row.sql_id)}${detailMeta('当前 / 昨日窗口耗时', `${millisText(row.runtime_us_total / 1000)} / ${millisText(row.baseline_runtime_us_total / 1000)}`)}${row.sample_event_id ? `<button type="button" class="button secondary compact" data-resource-event="${escapeHtml(row.sample_event_id)}" data-instance="${escapeHtml(result.instance_id)}">执行样本 / Pod</button>` : ''}</details>`,
      row.growth_share == null ? '—' : `${(row.growth_share * 100).toFixed(4)}%`, coefficient(row.resource_r, 4),
      millisText(row.runtime_us_total / 1000), humanCount(row.executions),
    ]);
    const audit = {period_us: result.period_us, sample_end_us: result.sample_end_us, metric_scale: node.metric_scale, metric_values: node.metric_values,
      statements: (node.statements || []).map(({fingerprint, runtime_us, score_numerator, baseline_runtime_us_total, growth_score_numerator, growth_score_denominator}) => ({fingerprint, runtime_us, score_numerator, baseline_runtime_us_total, growth_score_numerator, growth_score_denominator}))};
    return `<section class="detail-block"><h3>${escapeHtml(node.node_id || '未知节点')}</h3><p>IOPS 基线 ${node.baseline ?? '—'}% / 峰值 ${node.peak ?? '—'}%${node.status !== 'ok' ? ' · ' + escapeHtml(RESOURCE_STATUS[node.status] || node.status) : ''}</p>${analyticsTable(['排名', 'SQL', '增长关联份额', '时序 r', '窗口内执行耗时', '次数'], rows)}<details class="analytics-more"><summary>核对分钟计算序列</summary>${detailBlock('整数序列；metric_values ÷ metric_scale = IOPS 使用率 %', JSON.stringify(audit, null, 2))}</details></section>`;
  }).join('');
}

async function loadPerformanceAnalysis(query) {
  const params = new URLSearchParams(query || analyticsQueryString('executions'));
  params.set('order', 'executions'); params.delete('limit');
  const key = params.toString();
  if (state.performanceCache?.key === key) return state.performanceCache.promise;
  const promise = api(`/api/slowlog-impact?${key}`);
  state.performanceCache = {key, promise};
  try { return await promise; } catch (error) {
    if (state.performanceCache?.promise === promise) state.performanceCache = null;
    throw error;
  }
}

async function showPerformanceRanking() {
  const analytics = state.analytics, sql = analytics.sql;
  const request = state.performanceRequest;
  sql.order = 'performance';
  const header = `<div class="section-heading"><h3>慢 SQL · 性能关联排序</h3>${sqlSorter(SLOWLOG_ORDER_LABELS, 'performance')}</div>`;
  $('#analytics-panel-sql').innerHTML = header + '<p>正在读取当前 / 昨日同窗执行与节点 IOPS…</p>';
  try {
    const result = await loadPerformanceAnalysis(state.analyticsQuery);
    if (state.analytics !== analytics || request !== state.performanceRequest || sql.order !== 'performance') return;
    $('#analytics-panel-sql').innerHTML = header + renderResourceAnalysis(result);
  } catch (error) {
    if (state.analytics === analytics && request === state.performanceRequest && sql.order === 'performance')
      $('#analytics-panel-sql').innerHTML = header + `<p>${escapeHtml(error.message)}</p>`;
  }
}

async function openResourceAnalysis() {
  let query;
  try { query = analyticsQueryString(); } catch (error) { toast(error.message, 'error'); return; }
  const request = state.detailRequest = (state.detailRequest || 0) + 1;
  $('#detail-title').textContent = '节点 IOPS · 性能关联';
  $('#detail-body').innerHTML = '<p>正在读取已采集执行与云监控分钟指标…</p>';
  showDetailDrawer();
  try {
    const result = await loadPerformanceAnalysis(query);
    if (request !== state.detailRequest) return;
    $('#detail-body').innerHTML = renderResourceAnalysis(result);
  } catch (error) {
    if (request === state.detailRequest) $('#detail-body').innerHTML = `<p>${escapeHtml(error.message)}</p>`;
  }
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
