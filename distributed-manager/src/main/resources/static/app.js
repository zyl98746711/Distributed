'use strict';

// 无构建依赖；所有动态内容使用 DOM 文本节点，不解释为 HTML。
const $ = id => document.getElementById(id);
const views = {
  overview: ['集群总览', '观察节点协作、Leader 选举与日志复制，让共识过程看得见。'],
  kv: ['KV 数据实验', '一次写入，观察数据如何在独立节点之间达成一致。'],
  logs: ['日志与成员', '沿着日志索引，理解复制、提交与联合共识的过程。'],
  guide: ['故障实验指南', '亲手暂停一个节点，观察系统如何恢复协作。']
};
const state = {
  view: 'overview', selected: '', cluster: null, stale: false, at: null,
  round: 0, generation: 0, refreshing: false, queued: false, timer: null,
  managementBusy: false, kvBusy: false, events: [], kv: new Map(), details: new Map()
};
const time = date => date ? new Date(date).toLocaleTimeString('zh-CN', {hour12: false}) : '尚无采样';
const live = node => Boolean(node.status) && node.reachable !== false;
const nodes = () => state.cluster?.nodes || [];
const activeNodes = () => nodes().filter(node => node.membership !== 'REMOVED');
const memberLabels = {MEMBER: 'MEMBER · 已登记', PENDING: 'PENDING · 待核实', REMOVED: 'REMOVED · 已移除'};
const processLabels = {RUNNING: '运行中', STOPPED: '已停止', STARTING: '启动中'};

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined && text !== null) node.textContent = String(text);
  return node;
}
function badge(text, kind = 'neutral') { return el('span', `badge ${kind}`, text); }
function empty(message) { return el('div', 'empty', message); }
function notice(id, message, kind = '') {
  const node = $(id);
  node.textContent = message;
  node.className = `notice ${kind}`;
  node.hidden = !message;
}
function event(message, kind = 'good') {
  state.events.unshift({message, kind, at: Date.now()});
  state.events.length = Math.min(state.events.length, 100);
  renderEvents();
}
function renderEvents() {
  $('events').replaceChildren(...state.events.map(item => {
    const row = el('li', `event ${item.kind}`);
    const body = el('div');
    body.append(el('p', 'event-message', item.message), el('time', '', time(item.at)));
    row.append(el('span', 'event-icon', item.kind === 'good' ? '↗' : '!'), body);
    return row;
  }));
  if (!state.events.length) $('events').append(el('li', 'empty', '暂无观测动态'));
}

async function api(path, {method = 'GET', body, timeout = 15000} = {}) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);
  try {
    const response = await fetch(path, {
      method, signal: controller.signal, cache: 'no-store',
      headers: body === undefined ? {Accept: 'application/json'} : {Accept: 'application/json', 'Content-Type': 'application/json'},
      body: body === undefined ? undefined : JSON.stringify(body)
    });
    let result;
    try { result = await response.json(); }
    catch { throw Object.assign(new Error(`接口未返回有效 JSON（HTTP ${response.status}）`), {status: response.status}); }
    if (!response.ok || result?.code !== 200) {
      throw Object.assign(new Error(result?.message || `请求失败（HTTP ${response.status}）`), {
        status: response.status, code: result?.code, data: result?.data
      });
    }
    return result.data;
  } catch (error) {
    if (error.name === 'AbortError') throw new Error('请求超时');
    if (error instanceof TypeError) throw new Error('无法连接服务，请检查 manager 是否运行');
    throw error;
  } finally { clearTimeout(timer); }
}
function errorText(error, mutation) {
  const unknown = mutation && (!error.status || error.status < 400 || error.status >= 500 || error.code >= 500);
  return `${error.message}${unknown ? '；操作结果可能未知，请刷新核实后再决定是否重试。' : ''}`;
}

function observeChanges(previous, current) {
  if (!previous) event(`已连接集群，发现 ${current.nodes.length} 个部署记录`);
  if (previous?.leaderId !== current.leaderId) {
    event(current.leaderId ? `发现 Leader：${current.leaderId}` : '未发现 Leader，等待选举或恢复多数派', current.leaderId ? 'good' : 'warning');
  }
  current.nodes.forEach(node => {
    const before = previous?.nodes.find(old => old.definition.id === node.definition.id);
    if (!before) { if (previous) event(`发现新部署：${node.definition.id}`); return; }
    if (live(before) !== live(node)) event(`${node.definition.id} ${live(node) ? '恢复可达' : 'HTTP 不可达'}`, live(node) ? 'good' : 'warning');
    if (live(before) && live(node) && before.status.role !== node.status.role) event(`${node.definition.id}：${before.status.role} → ${node.status.role}`);
    if (before.status?.term !== node.status?.term && live(before) && live(node)) event(`${node.definition.id} 任期 ${before.status.term} → ${node.status.term}`);
    if (before.membership !== node.membership) event(`${node.definition.id} 成员登记：${node.membership}`);
  });
}

function renderCluster() {
  const all = activeNodes();
  const leader = all.find(node => node.definition.id === state.cluster?.leaderId && live(node));
  $('metric-leader').textContent = state.cluster?.leaderId || '未发现';
  $('metric-term').textContent = leader?.status.term ?? '—';
  $('metric-online').textContent = all.filter(live).length;
  $('metric-total').textContent = ` / ${all.length}`;
  $('metric-members').textContent = all.filter(node => node.membership === 'MEMBER').length;
  const pending = all.filter(node => node.membership === 'PENDING').length;
  $('metric-pending').textContent = pending ? `${pending} 个 PENDING 待核实 · 请查询共识配置` : '已登记 MEMBER · 不代替共识配置';
  $('node-count').textContent = nodes().length;
  renderNodeCards();
  renderTopology();
  const select = $('detail-node');
  if (!nodes().some(node => node.definition.id === state.selected)) state.selected = leader?.definition.id || nodes()[0]?.definition.id || '';
  const signature = nodes().map(node => `${node.definition.id}:${node.membership}`).join('|');
  if (select.dataset.signature !== signature) {
    select.replaceChildren(...nodes().map(node => {
      const option = el('option', '', `${node.definition.id}${node.membership === 'REMOVED' ? '（已移除）' : ''}`);
      option.value = node.definition.id;
      return option;
    }));
    select.dataset.signature = signature;
  }
  select.value = state.selected;
}

function renderNodeCards() {
  const focused = document.activeElement;
  const focusKey = $('node-cards').contains(focused) ? [focused.dataset.id, focused.dataset.action] : null;
  const memberCount = activeNodes().filter(node => node.membership === 'MEMBER').length;
  const cards = nodes().map(node => {
    const id = node.definition.id;
    const online = live(node);
    const s = online ? node.status : {};
    const isLeader = online && s.role === 'LEADER';
    const card = el('article', `node-card${isLeader ? ' is-leader' : ''}${node.membership === 'REMOVED' ? ' is-removed' : ''}`);
    const head = el('div', 'node-header');
    const title = el('h3', 'node-title');
    title.append(el('span', 'node-icon', '▤'), document.createTextNode(id));
    head.append(title, badge(online ? s.role : '不可达', online ? (isLeader ? 'good' : 'neutral') : 'bad'));
    const definition = node.definition;
    const meta = el('p', 'node-meta mono', `${definition.host} · HTTP ${definition.httpPort} / RPC ${definition.rpcPort}\nPID ${s.pid ?? '—'} · Term ${s.term ?? '—'} · 日志 ${s.logSize ?? '—'} 条`);
    meta.style.whiteSpace = 'pre-line';
    const statuses = el('div', 'badge-row');
    statuses.append(badge(memberLabels[node.membership] || node.membership, node.membership === 'PENDING' ? 'warning' : 'neutral'));
    statuses.append(badge(`进程：${processLabels[node.state] || node.state}`, 'neutral'));
    if (state.stale) statuses.append(badge('历史采样', 'warning'));
    const stats = el('dl', 'node-stats');
    [['commitIndex', s.commitIndex], ['lastApplied', s.lastApplied], ['日志规模', s.logSize]].forEach(([label, value]) => {
      const item = el('div'); item.append(el('dt', '', label), el('dd', '', value ?? '—')); stats.append(item);
    });
    const progress = el('div', 'replication');
    const progressLabel = el('div', 'replication-label');
    const committed = Number(s.commitIndex) || 0;
    const applied = Number(s.lastApplied) || 0;
    progressLabel.append(el('span', '', '本地提交 → 应用'), el('span', 'mono', online ? `${applied} / ${committed}` : '等待连接'));
    const track = el('div', 'progress-track');
    const fill = el('div', 'progress-fill');
    fill.style.width = `${online ? (committed ? Math.max(0, Math.min(100, applied / committed * 100)) : 100) : 0}%`;
    track.append(fill); progress.append(progressLabel, track);
    const actions = el('div', 'node-actions');
    [['detail', '查看日志 ↗'], ['start', '启动'], ['stop', '停止'], ['remove', '移除']].forEach(([action, label]) => {
      const button = el('button', '', label); button.type = 'button';
      button.dataset.id = id; button.dataset.action = action;
      button.setAttribute('aria-label', `${id} ${label}`);
      if (action !== 'detail') {
        button.dataset.management = '';
        const forbidden = (action === 'start' && (node.membership === 'REMOVED' || online || node.state === 'STARTING'))
          || (action === 'stop' && !online && node.state === 'STOPPED')
          || (action === 'remove' && (node.membership === 'REMOVED' || (node.membership === 'MEMBER' && memberCount <= 1)));
        button.dataset.forbidden = String(forbidden);
        button.disabled = state.managementBusy || forbidden;
      }
      actions.append(button);
    });
    card.append(head, meta, statuses, stats, progress, actions);
    return card;
  });
  $('node-cards').replaceChildren(...(cards.length ? cards : [empty('部署清单中没有节点')]));
  if (focusKey) {
    const button = [...$('node-cards').querySelectorAll('button')].find(item => item.dataset.id === focusKey[0] && item.dataset.action === focusKey[1]);
    button?.focus({preventScroll: true});
  }
}

function svgElement(tag, attributes, text) {
  const node = document.createElementNS('http://www.w3.org/2000/svg', tag);
  Object.entries(attributes).forEach(([key, value]) => node.setAttribute(key, value));
  if (text !== undefined) node.textContent = String(text);
  return node;
}
function renderTopology() {
  const all = activeNodes();
  const width = Math.max(520, all.length * 135 + 40);
  const svg = svgElement('svg', {viewBox: `0 0 ${width} 245`, role: 'img', 'aria-label': `集群逻辑拓扑，${all.length} 个非移除节点`});
  svg.style.minWidth = `${Math.max(400, all.length * 115)}px`;
  const middle = width / 2;
  const rect = (x, y, w, h, fill, stroke) => svgElement('rect', {x, y, width: w, height: h, rx: 7, fill, stroke});
  const text = (x, y, value, color = '#65808e', size = 11) => svgElement('text', {x, y, fill: color, 'font-size': size, 'text-anchor': 'middle'}, value);
  svg.append(text(middle, 24, 'CONTROL PLANE', '#9aaaae', 8));
  svg.append(rect(middle - 72, 36, 144, 42, '#ffffff', '#d7e3e6'));
  svg.append(text(middle, 54, 'manager', '#466674', 12), text(middle, 68, `HTTP :${location.port || '80'} · 非投票节点`, '#97a9b1', 8));
  all.forEach((node, index) => {
    const x = width * (index + .5) / Math.max(1, all.length);
    const online = live(node);
    const leader = online && node.status.role === 'LEADER';
    svg.append(svgElement('path', {d: `M ${middle} 78 C ${middle} 111 ${x} 108 ${x} 140`, fill: 'none', stroke: leader ? '#75bdaa' : '#cfdbdf', 'stroke-width': 1.4, 'stroke-dasharray': '4 4'}));
    if (index < all.length - 1) {
      const next = width * (index + 1.5) / all.length;
      svg.append(svgElement('line', {x1: x + 51, y1: 166, x2: next - 51, y2: 166, stroke: '#a7c2c8', 'stroke-width': 1.4}));
    }
    svg.append(rect(x - 51, 140, 102, 53, leader ? '#eaf7f1' : '#fff', leader ? '#64b49d' : online ? '#d6e2e7' : '#e2c4bf'));
    svg.append(text(x, 161, node.definition.id, leader ? '#247863' : '#617f8c', 12));
    svg.append(text(x, 180, online ? node.status.role : '不可达', online ? '#91a5af' : '#b38078', 8));
    svg.append(text(x, 211, `RPC :${node.definition.rpcPort}`, '#92a5ae', 9));
  });
  svg.append(text(middle, 234, 'DATA PLANE / 节点间通过 Raft TCP 通信', '#9babb1', 8));
  $('topology').replaceChildren(all.length ? svg : empty('没有可展示的活动部署'));
}

function scheduleRefresh() {
  clearTimeout(state.timer);
  if ($('auto-refresh').checked && !document.hidden) state.timer = setTimeout(refresh, 2000);
}
async function refresh() {
  clearTimeout(state.timer);
  if (state.refreshing) { state.queued = true; return; }
  state.refreshing = true;
  $('refresh-button').disabled = true;
  const generation = state.generation;
  const view = state.view;
  const round = ++state.round;
  try {
    const cluster = await api('/manager/cluster');
    if (!cluster || !Array.isArray(cluster.nodes)) throw new Error('集群响应格式异常');
    if (state.stale) event('manager 连接恢复');
    observeChanges(state.cluster, cluster);
    state.cluster = cluster; state.stale = false; state.at = Date.now();
    $('connection-badge').className = 'badge good';
    $('connection-badge').replaceChildren(el('span', 'dot'), document.createTextNode('manager 已连接'));
    $('last-refresh').textContent = `最近采样 ${time(state.at)}`;
    notice('global-alert', cluster.leaderId ? '' : '尚未发现可用 Leader。集群可能正在选举或失去多数派；请观察节点状态，不要重复提交写入。', 'warning');
    renderCluster();
    if (generation === state.generation && !document.hidden) {
      if (view === 'kv') await sampleKv(generation, round);
      if (view === 'logs' && state.selected) await sampleDetails(generation);
    }
  } catch (error) {
    if (!state.stale) event(`集群状态读取失败：${error.message}`, 'error');
    state.stale = true;
    $('connection-badge').className = 'badge bad';
    $('connection-badge').replaceChildren(el('span', 'dot'), document.createTextNode('manager 连接异常'));
    notice('global-alert', `${error.message}。${state.at ? `当前显示 ${time(state.at)} 的历史数据，已过期。` : '尚无集群数据，请先启动 manager。'}`, 'error');
    if (state.cluster) renderCluster();
    if (state.view === 'kv') renderKvComparison();
    if (state.view === 'logs') renderDetails();
  } finally {
    state.refreshing = false; $('refresh-button').disabled = false;
    if (state.queued && !document.hidden) { state.queued = false; void refresh(); }
    else { state.queued = false; scheduleRefresh(); }
  }
}

// 每批最多三个读取任务；切换视图后不启动剩余任务，也不接纳旧响应。
async function limited(items, task, generation) {
  let index = 0;
  await Promise.all(Array.from({length: Math.min(3, items.length)}, async () => {
    while (index < items.length && generation === state.generation && !document.hidden) {
      const item = items[index++];
      await task(item);
    }
  }));
}
async function sampleKv(generation, round) {
  await limited(activeNodes(), async node => {
    const id = node.definition.id;
    let sample;
    try {
      if (!live(node)) throw new Error('节点 HTTP 不可达');
      const data = await api(`/manager/nodes/${encodeURIComponent(id)}/kv`);
      if (!data || typeof data !== 'object' || Array.isArray(data)) throw new Error('KV 响应格式异常');
      sample = {data, at: Date.now(), error: null, round};
    } catch (error) { sample = {...state.kv.get(id), error: error.message, round}; }
    if (generation === state.generation) state.kv.set(id, sample);
  }, generation);
  if (generation === state.generation) renderKvComparison();
}
function renderKvComparison() {
  const all = activeNodes();
  const table = el('table', 'data-table');
  const header = el('tr'); header.append(el('th', '', 'KEY'), el('th', '', '采样对比'));
  const fresh = id => { const sample = state.kv.get(id); return !state.stale && sample?.data && !sample.error && sample.round === state.round; };
  all.forEach(node => {
    const sample = state.kv.get(node.definition.id);
    const cell = el('th', 'mono', node.definition.id);
    cell.append(el('small', sample?.error ? 'unreachable' : '', sample?.error ? `${sample.error}${sample.at ? ` · 旧快照 ${time(sample.at)}` : ''}` : sample?.at ? `采样 ${time(sample.at)}${fresh(node.definition.id) ? '' : ' · 待刷新'}` : '等待采样'));
    header.append(cell);
  });
  const head = el('thead'); head.append(header); table.append(head);
  const body = el('tbody');
  const keys = [...new Set(all.flatMap(node => Object.keys(state.kv.get(node.definition.id)?.data || {})))].sort();
  keys.forEach(key => {
    const row = el('tr'); row.append(el('td', 'key-cell', key));
    const values = all.map(node => {
      const data = state.kv.get(node.definition.id)?.data;
      return data && Object.hasOwn(data, key) ? JSON.stringify(data[key]) : undefined;
    });
    const comparable = all.length > 0 && all.every(node => fresh(node.definition.id));
    const equal = comparable && values.every(value => value === values[0]);
    const result = el('td'); result.append(badge(!comparable ? '待确认' : equal ? '采样一致' : '存在差异', !comparable ? 'neutral' : equal ? 'good' : 'warning')); row.append(result);
    all.forEach((node, index) => {
      const sample = state.kv.get(node.definition.id);
      const cell = el('td');
      if (!fresh(node.definition.id)) {
        cell.className = 'unreachable';
        cell.textContent = sample?.error || '等待新采样';
        if (sample?.data && Object.hasOwn(sample.data, key)) cell.append(el('div', 'subtle', `历史值：${sample.data[key] === '' ? '""（空字符串）' : sample.data[key]}`));
      } else if (values[index] === undefined) { cell.className = 'missing'; cell.textContent = '缺失'; }
      else { cell.textContent = sample.data[key] === '' ? '""（空字符串）' : sample.data[key]; if (comparable && !equal) cell.className = 'difference'; }
      row.append(cell);
    });
    body.append(row);
  });
  table.append(body);
  $('kv-compare').replaceChildren(table);
  if (!keys.length) $('kv-compare').append(empty(all.every(node => fresh(node.definition.id)) && all.length ? '当前快照没有 Key，写入一条数据开始实验。' : '尚无可展示数据，请检查各节点的采样状态。'));
  $('kv-sample-time').textContent = `最近观测 ${time(Date.now())}`;
}

async function sampleDetails(generation) {
  const id = state.selected;
  const node = nodes().find(item => item.definition.id === id);
  await Promise.all(['log', 'members'].map(async kind => {
    const key = `${id}/${kind}`;
    let sample;
    try {
      if (!node || !live(node)) throw new Error('节点 HTTP 不可达');
      const data = await api(`/manager/nodes/${encodeURIComponent(id)}/${kind}`);
      if (!data || typeof data !== 'object') throw new Error('节点响应格式异常');
      sample = {data, at: Date.now(), error: null};
    } catch (error) { sample = {...state.details.get(key), error: error.message}; }
    if (generation === state.generation && id === state.selected) state.details.set(key, sample);
  }));
  if (generation === state.generation && id === state.selected) renderDetails();
}
function sampleDescription(sample) {
  if (!sample) return '等待采样';
  if (state.stale) return `manager 连接异常 · 历史数据 ${time(sample.at)}（已过期）`;
  return sample.error ? `${sample.error}${sample.at ? ` · 历史数据 ${time(sample.at)}（已过期）` : ''}` : `采样 ${time(sample.at)}`;
}
function renderDetails() {
  const log = state.details.get(`${state.selected}/log`);
  const members = state.details.get(`${state.selected}/members`);
  $('detail-status').textContent = sampleDescription(log);
  $('log-progress').replaceChildren();
  const data = log?.data;
  if (data) {
    [['lastIndex', data.lastIndex], ['commitIndex', data.commitIndex], ['lastApplied', data.lastApplied]].forEach(([key, value]) => {
      const item = el('span', '', key); item.append(el('strong', '', value ?? '—')); $('log-progress').append(item);
    });
    const entries = Array.isArray(data.entries) ? data.entries : [];
    const shown = $('show-all-logs').checked ? entries : entries.slice(-100);
    const table = el('table', 'data-table'); const head = el('thead'); const tr = el('tr');
    ['INDEX', 'TERM', 'TYPE', '状态', 'COMMAND'].forEach(label => tr.append(el('th', '', label)));
    head.append(tr); table.append(head); const body = el('tbody');
    shown.forEach(entry => {
      const row = el('tr');
      row.append(el('td', 'mono', entry.index), el('td', 'mono', entry.term), el('td', 'mono', entry.type));
      const status = el('td'); status.append(entry.index <= data.lastApplied ? badge('已应用', 'good') : entry.index <= data.commitIndex ? badge('已提交', 'neutral') : badge('待提交', 'warning'));
      row.append(status, el('td', 'mono', entry.command ?? '—')); body.append(row);
    });
    table.append(body); $('log-table').replaceChildren(table);
    if (!entries.length) $('log-table').append(empty('当前节点暂无日志'));
  } else $('log-table').replaceChildren(empty(sampleDescription(log)));
  $('membership-source').textContent = `来源 ${state.selected || '未选择'} · ${sampleDescription(members)}`;
  $('membership-flags').replaceChildren();
  $('membership-data').replaceChildren();
  if (members?.data) {
    const config = members.data;
    $('membership-flags').append(badge(config.joint ? 'JOINT 联合配置' : '稳定配置', config.joint ? 'warning' : 'good'), badge(config.changing ? '变更中' : '无进行中变更', config.changing ? 'warning' : 'neutral'));
    [['当前 / 新配置', config.members], ['旧配置', config.oldMembers], ['已提交成员', config.committedMembers]].forEach(([label, list]) => {
      const group = el('div', 'member-group'); group.append(el('h3', '', label));
      const entries = Array.isArray(list) ? list : Object.values(list || {});
      entries.forEach(item => group.append(el('p', '', `${item.id} · ${item.host}:${item.port ?? item.rpcPort ?? '—'}`)));
      if (!entries.length) group.append(el('p', 'subtle', '无'));
      $('membership-data').append(group);
    });
  } else $('membership-data').append(empty(sampleDescription(members)));
}

function updateBusy() {
  document.querySelectorAll('[data-management]').forEach(button => { button.disabled = state.managementBusy || button.dataset.forbidden === 'true'; });
  document.querySelectorAll('[data-kv]').forEach(button => { button.disabled = state.kvBusy; });
}
function confirmAction(title, message) {
  const dialog = $('confirm-dialog');
  if (dialog.open) return Promise.resolve(false);
  $('confirm-title').textContent = title; $('confirm-message').textContent = message;
  dialog.returnValue = 'cancel';
  return new Promise(resolve => {
    dialog.addEventListener('close', () => resolve(dialog.returnValue === 'confirm'), {once: true});
    dialog.showModal();
    dialog.querySelector('[value="cancel"]').focus();
  });
}
async function management(path, method, label, body) {
  if (state.managementBusy) return;
  state.managementBusy = true; updateBusy();
  notice('operation-feedback', `${label}：执行中，请等待。进程启动及配置提交可能需要数十秒。`);
  try {
    const data = await api(path, {method, body, timeout: 60000});
    const message = `${label}成功${data?.id ? ` · ${data.id}` : ''}`;
    notice('operation-feedback', message); event(message);
  } catch (error) {
    const message = `${label}：${errorText(error, true)}${error.data?.id ? ` 节点：${error.data.id}` : ''}`;
    notice('operation-feedback', message, 'warning'); event(message, 'warning');
  } finally { state.managementBusy = false; updateBusy(); void refresh(); }
}
async function nodeAction(id, action) {
  if (action === 'detail') {
    state.selected = id; state.generation++; $('detail-node').value = id;
    if (location.hash === '#logs') { renderDetails(); void refresh(); } else location.hash = 'logs';
    return;
  }
  if (state.managementBusy) return;
  const node = nodes().find(item => item.definition.id === id);
  if (!node) return;
  if (action === 'stop') {
    const leaderWarning = node.status?.role === 'LEADER' ? '该节点最近被观测为 Leader，停止后需要重新选举。' : '';
    if (!await confirmAction(`停止 ${id}？`, `${leaderWarning}停止只控制进程，不减少共识成员数。可能失去多数派并导致无法写入；全体停止并重启会丢失内存数据。`)) return;
  }
  if (action === 'remove' && !await confirmAction(`移除 ${id}？`, '这会通过 Raft 变更共识成员，确认提交后停止进程。被移除的节点不能直接重启，ID 与端口会保留；若结果未知，请先核实成员配置。')) return;
  const removing = action === 'remove';
  await management(`/manager/nodes/${encodeURIComponent(id)}${removing ? '' : `/${action}`}`, removing ? 'DELETE' : 'POST', `${{start: '启动', stop: '停止', remove: '移除'}[action]} ${id}`);
}

async function kvAction(method) {
  if (state.kvBusy) return;
  const input = $('kv-key'); const key = input.value;
  // 路径式接口不能可靠表示斜杠、点路径、矩阵参数和控制字符。
  const invalid = !key.trim() || /[\/\\;\u0000-\u001f\u007f]/.test(key) || key === '.' || key === '..';
  input.setCustomValidity(invalid ? 'Key 不能为空，且不能包含斜杠、反斜杠、分号、控制字符或单独的点路径。' : '');
  if (!input.reportValidity()) return;
  const value = $('kv-value').value;
  if (method === 'DELETE' && !await confirmAction(`删除 Key「${key}」？`, '删除操作会写入 Raft 日志并复制到各节点，不能撤销。')) return;
  if (state.kvBusy) return;
  state.kvBusy = true; updateBusy();
  const label = {PUT: '写入', GET: '查询', DELETE: '删除'}[method];
  $('kv-result-meta').textContent = `${method} · 请求中`;
  try {
    const query = method === 'PUT' ? `?${new URLSearchParams({value})}` : '';
    let data = await api(`/manager/kv/${encodeURIComponent(key)}${query}`, {method});
    if (method === 'GET' && key === 'all') {
      if (!data || !Object.hasOwn(data, key)) throw Object.assign(new Error('Key not found'), {status: 404, code: 404});
      data = {key, value: data[key], source: 'Leader 全量响应中的同名 Key'};
    }
    $('kv-result').textContent = JSON.stringify({code: 200, data}, null, 2);
    $('kv-result-meta').textContent = `${method} · ${time(Date.now())} · 成功`;
    event(`KV ${label}成功：${key}`);
  } catch (error) {
    const message = errorText(error, method !== 'GET');
    $('kv-result').textContent = JSON.stringify({code: error.code || error.status || 'NETWORK_ERROR', message}, null, 2);
    $('kv-result-meta').textContent = `${method} · ${time(Date.now())} · 未成功确认`;
    event(`KV ${label}：${message}`, 'warning');
  } finally { state.kvBusy = false; updateBusy(); void refresh(); }
}

function changeView() {
  const requested = location.hash.slice(1);
  state.view = Object.hasOwn(views, requested) ? requested : 'overview';
  state.generation++;
  const [title, description] = views[state.view];
  $('page-title').replaceChildren(document.createTextNode(title), el('span', 'title-dot'));
  $('page-description').textContent = description; $('breadcrumb-current').textContent = title;
  document.title = `${title} · Distributed Lab`;
  document.querySelectorAll('.view').forEach(view => { view.hidden = view.id !== `view-${state.view}`; });
  document.querySelectorAll('[data-view]').forEach(link => {
    const active = link.dataset.view === state.view;
    link.classList.toggle('active', active);
    if (active) link.setAttribute('aria-current', 'page'); else link.removeAttribute('aria-current');
  });
  if (state.view === 'logs') renderDetails();
  if (state.view === 'kv') renderKvComparison();
  void refresh();
}

$('server-origin').textContent = location.host;
$('refresh-button').addEventListener('click', refresh);
$('auto-refresh').addEventListener('change', () => { if ($('auto-refresh').checked) void refresh(); else clearTimeout(state.timer); });
$('clear-events').addEventListener('click', () => { state.events = []; renderEvents(); });
$('node-cards').addEventListener('click', event => {
  const button = event.target.closest('button[data-action]');
  if (button && !button.disabled) void nodeAction(button.dataset.id, button.dataset.action);
});
$('detail-node').addEventListener('change', () => { state.selected = $('detail-node').value; state.generation++; renderDetails(); void refresh(); });
$('show-all-logs').addEventListener('change', renderDetails);
$('add-node-button').addEventListener('click', () => { if (!state.managementBusy) { $('add-error').textContent = ''; $('add-dialog').showModal(); } });
$('cancel-add').addEventListener('click', () => $('add-dialog').close());
$('add-form').addEventListener('submit', event => {
  event.preventDefault();
  if (state.managementBusy) return;
  const httpPort = $('node-http-port').value ? Number($('node-http-port').value) : null;
  const rpcPort = $('node-rpc-port').value ? Number($('node-rpc-port').value) : null;
  if (httpPort !== null && httpPort === rpcPort) { $('add-error').textContent = 'HTTP 与 RPC 端口不能相同。'; return; }
  $('add-dialog').close();
  void management('/manager/nodes', 'POST', '新增节点', {host: $('node-host').value, httpPort, rpcPort});
});
$('kv-key').addEventListener('input', () => $('kv-key').setCustomValidity(''));
$('kv-form').addEventListener('submit', event => { event.preventDefault(); void kvAction('PUT'); });
$('kv-get').addEventListener('click', () => void kvAction('GET'));
$('kv-delete').addEventListener('click', () => void kvAction('DELETE'));
window.addEventListener('hashchange', changeView);
document.addEventListener('visibilitychange', () => {
  if (document.hidden) { clearTimeout(state.timer); state.generation++; }
  else if ($('auto-refresh').checked) void refresh();
});
changeView();
