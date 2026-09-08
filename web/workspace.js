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
