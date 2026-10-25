const $ = s => document.querySelector(s);
const api = async (path, method = 'GET', body) => {
  const r = await fetch('/api' + path, {method, headers: body ? {'Content-Type': 'application/json'} : {}, body: body ? JSON.stringify(body) : undefined});
  if (!r.ok) { let m = r.statusText; try { m = (await r.json()).detail || m; } catch {} throw new Error(m); }
  return r.json();
};
const toast = (m, bad) => { const t = $('#toast'); t.textContent = m; t.style.borderColor = bad ? 'var(--bad)' : 'var(--line)'; t.style.display = 'block'; clearTimeout(t._t); t._t = setTimeout(() => t.style.display = 'none', 4000); };
const guard = fn => async (...a) => { try { await fn(...a); } catch (e) { toast(e.message, true); } };
const esc = s => String(s).replace(/[&<>"]/g, c => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;'}[c]));
const ansi = s => s.replace(/\x1b\[[0-9;?]*[A-Za-z]/g, '').replace(/\r(?!\n)/g, '\n');
const fmtTime = t => t ? new Date(t * 1000).toLocaleString() : '';
const fmtDur = (a, b) => b ? ((b - a) < 60 ? (b - a).toFixed(1) + 's' : Math.floor((b - a) / 60) + 'm ' + Math.round((b - a) % 60) + 's') : '...';

/* ---- streaming ---- */
function attach(runId, pre, onDone) {
  pre.textContent = '';
  const es = new EventSource(`/api/runs/${runId}/stream`);
  es.onmessage = e => {
    const stick = pre.scrollTop + pre.clientHeight >= pre.scrollHeight - 20;
    pre.append(ansi(JSON.parse(e.data)));
    if (stick) pre.scrollTop = pre.scrollHeight;
  };
  es.addEventListener('done', () => { es.close(); onDone && onDone(); });
  es.onerror = () => { es.close(); onDone && onDone(); };
  return es;
}

/* ---- nav ---- */
document.querySelectorAll('#nav button').forEach(b => b.onclick = () => show(b.dataset.v));
function show(v) {
  document.querySelectorAll('#nav button').forEach(b => b.classList.toggle('active', b.dataset.v === v));
  document.querySelectorAll('.view').forEach(s => s.classList.toggle('active', s.id === 'v-' + v));
  if (v === 'history') loadHistory();
  if (v === 'packages') loadPkgs();
  if (v === 'settings') loadSettings();
  if (v === 'scripts') cm.refresh();
}


/* ---- schedule builder ---- */
const DAYS = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun'];
const DAY_LABEL = {mon: 'Mon', tue: 'Tue', wed: 'Wed', thu: 'Thu', fri: 'Fri', sat: 'Sat', sun: 'Sun'};
const pad = n => String(n).padStart(2, '0');
const defaultSched = () => ({type: 'manual', n: 5, minute: 0, time: '09:00', days: ['mon', 'tue', 'wed', 'thu', 'fri'], dom: 1, cron: ''});
let sched = defaultSched(), tz = '';

function cronFromSched(s) {
  const [h, m] = s.time.split(':').map(Number);
  switch (s.type) {
    case 'manual': return '';
    case 'minutes': return `*/${s.n} * * * *`;
    case 'hours': return `${s.minute} */${s.n} * * *`;
    case 'daily': return `${m} ${h} * * *`;
    case 'weekly': return `${m} ${h} * * ${DAYS.filter(d => s.days.includes(d)).join(',')}`;
    case 'monthly': return `${m} ${h} ${s.dom} * *`;
    default: return s.cron.trim();
  }
}
function schedFromCron(cron) {
  const s = defaultSched();
  cron = (cron || '').trim();
  if (!cron) return s;
  const custom = () => { s.type = 'custom'; s.cron = cron; return s; };
  const f = cron.split(/\s+/);
  if (f.length !== 5) return custom();
  const [mi, h, d, mo, w] = f, num = /^\d+$/, step = /^\*\/\d+$/;
  if (mo !== '*') return custom();
  if ((mi === '*' || step.test(mi)) && h === '*' && d === '*' && w === '*') { s.type = 'minutes'; s.n = mi === '*' ? 1 : +mi.slice(2); return s; }
  if (num.test(mi) && step.test(h) && d === '*' && w === '*') { s.type = 'hours'; s.minute = +mi; s.n = +h.slice(2); return s; }
  if (num.test(mi) && h === '*' && d === '*' && w === '*') { s.type = 'hours'; s.minute = +mi; s.n = 1; return s; }
  if (num.test(mi) && num.test(h)) {
    s.time = `${pad(h)}:${pad(mi)}`;
    if (d === '*' && w === '*') { s.type = 'daily'; return s; }
    if (d === '*' && /^[a-z]{3}(,[a-z]{3})*$/i.test(w)) {
      const days = w.toLowerCase().split(',');
      if (days.every(x => DAYS.includes(x))) { s.type = 'weekly'; s.days = days; return s; }
    }
    if (num.test(d) && w === '*') { s.type = 'monthly'; s.dom = +d; return s; }
  }
  return custom();
}
function describeSched(s) {
  switch (s.type) {
    case 'manual': return 'Manual only';
    case 'minutes': return s.n === 1 ? 'Every minute' : `Every ${s.n} minutes`;
    case 'hours': return (s.n === 1 ? 'Every hour' : `Every ${s.n} hours`) + ` at :${pad(s.minute)}`;
    case 'daily': return `Every day at ${s.time}`;
    case 'weekly': {
      const d = DAYS.filter(x => s.days.includes(x)), key = d.join();
      const txt = key === 'mon,tue,wed,thu,fri' ? 'Weekdays' : key === 'sat,sun' ? 'Weekends' : d.length === 7 ? 'Every day' : d.map(x => DAY_LABEL[x]).join(', ');
      return `${txt} at ${s.time}`;
    }
    case 'monthly': return `Monthly on day ${s.dom} at ${s.time}`;
    default: return 'Custom: ' + s.cron;
  }
}
const describeCron = cron => describeSched(schedFromCron(cron));

$('#schDays').innerHTML = DAYS.map(d => `<button type="button" class="day" data-d="${d}">${DAY_LABEL[d]}</button>`).join('');
function writeSched() {
  $('#schType').value = sched.type; $('#schN').value = sched.n; $('#schMinute').value = sched.minute;
  $('#schTime').value = sched.time; $('#schDom').value = sched.dom; $('#schCron').value = sched.cron;
  document.querySelectorAll('#schDays .day').forEach(b => b.classList.toggle('on', sched.days.includes(b.dataset.d)));
  renderSched();
}
function readSched() {
  sched.type = $('#schType').value;
  sched.n = Math.max(1, +$('#schN').value || 1); sched.minute = Math.min(59, Math.max(0, +$('#schMinute').value || 0));
  sched.time = $('#schTime').value || '09:00'; sched.dom = Math.min(31, Math.max(1, +$('#schDom').value || 1));
  sched.cron = $('#schCron').value;
  sched.days = [...document.querySelectorAll('#schDays .day.on')].map(b => b.dataset.d);
  renderSched();
}
function renderSched() {
  const t = sched.type;
  const vis = {gEvery: t === 'minutes' || t === 'hours', gMinute: t === 'hours', gDays: t === 'weekly', gDom: t === 'monthly',
    gTime: ['daily', 'weekly', 'monthly'].includes(t), gCron: t === 'custom'};
  for (const [id, on] of Object.entries(vis)) $('#' + id).style.display = on ? '' : 'none';
  $('#schUnit').textContent = t === 'hours' ? 'hours' : 'minutes';
  $('#schN').max = t === 'hours' ? 23 : 59;
  $('#fEnabled').parentElement.style.display = t === 'manual' ? 'none' : '';
  const next = cur && cur.next_run && cur.enabled && cur.schedule === cronFromSched(sched) ? `  ·  next: ${fmtTime(cur.next_run)}` : '';
  $('#schSummary').textContent = t === 'manual' ? 'Only runs when you press Run' : `${describeSched(sched)}${tz ? ' (' + tz + ')' : ''}  ·  cron: ${cronFromSched(sched) || '-'}${next}`;
}
$('#schType').onchange = readSched;
document.querySelectorAll('#schN,#schMinute,#schTime,#schDom,#schCron').forEach(i => i.oninput = readSched);
document.querySelectorAll('#schDays .day').forEach(b => b.onclick = () => { b.classList.toggle('on'); readSched(); });

/* ---- scripts ---- */
const cm = CodeMirror.fromTextArea($('#code'), {
  mode: 'python', theme: 'material-darker', lineNumbers: true, indentUnit: 4, indentWithTabs: false,
  extraKeys: {
    Tab: c => c.somethingSelected() ? c.indentSelection('add') : c.replaceSelection('    '),
    'Ctrl-S': () => save(), 'Cmd-S': () => save(), 'Ctrl-Enter': () => run(), 'Cmd-Enter': () => run(),
  },
});
let scripts = [], cur = null, curEs = null, curRun = null;
const templates = {
  python: '#!/usr/bin/env python3\nprint("Hello from Pigeonhole")\n',
  bash: '#!/usr/bin/env bash\nset -euo pipefail\necho "Hello from Pigeonhole"\n',
};
const modeOf = l => l === 'python' ? 'python' : 'shell';

async function loadScripts() {
  scripts = await api('/scripts');
  renderList();
}
function renderList() {
  $('#scriptList').innerHTML = scripts.map(s => `<div class="item ${cur && cur.id === s.id ? 'active' : ''}" data-id="${s.id}">
    <span class="dot ${s.last_status || ''}"></span>
    <div class="meta"><span class="name">${esc(s.name)}</span>${s.schedule ? `<span class="sub ${s.enabled ? '' : 'paused'}" title="cron: ${esc(s.schedule)}">${s.enabled ? '' : 'Paused - '}${esc(describeCron(s.schedule))}</span>` : ''}</div>
    <span class="badge">${s.language === 'python' ? 'py' : 'sh'}</span></div>`).join('')
    || '<div class="hint" style="padding:12px">No scripts yet.</div>';
  document.querySelectorAll('.item').forEach(i => i.onclick = () => select(+i.dataset.id));
}
function select(id) {
  cur = id === 'new'
    ? {id: null, name: '', language: 'python', content: templates.python, schedule: '', enabled: true, timeout: 0, env: ''}
    : scripts.find(s => s.id === id);
  if (!cur) return;
  $('#noScript').style.display = 'none';
  $('#editorBody').style.display = 'flex';
  $('#fName').value = cur.name; $('#fLang').value = cur.language;
  sched = schedFromCron(cur.schedule); writeSched();
  $('#fTimeout').value = cur.timeout; $('#fEnabled').checked = cur.enabled;
  $('#fEnv').value = cur.env || ''; countEnv();
  $('#envBox').open = !!(cur.env || '').trim();
  cm.setOption('mode', modeOf(cur.language)); cm.setValue(cur.content); cm.clearHistory();
  setTimeout(() => cm.refresh(), 0);
  $('#delBtn').style.display = cur.id ? '' : 'none';
  showNext();
  renderList();
  if (curEs) { curEs.close(); curEs = null; }
  $('#out').textContent = '';
  setRunUI(null);
  if (cur.running_run) watch(cur.running_run);
}
function showNext() { renderSched(); }
function setRunUI(runId) {
  curRun = runId;
  $('#stopBtn').style.display = runId ? '' : 'none';
  $('#runBtn').disabled = !!runId;
  $('#runStatus').innerHTML = runId ? '<span class="s-running">running (run #' + runId + ')</span>' : '';
}
function watch(runId) {
  setRunUI(runId);
  curEs = attach(runId, $('#out'), async () => {
    setRunUI(null);
    await loadScripts();
    if (cur && cur.id) cur = scripts.find(s => s.id === cur.id) || cur;
    showNext();
  });
}
function countEnv() {
  const n = $('#fEnv').value.split('\n').filter(l => l.trim() && !l.trim().startsWith('#')).length;
  $('#envCount').textContent = n || '';
  $('#envCount').style.display = n ? '' : 'none';
}
$('#fEnv').oninput = countEnv;
$('#fLang').onchange = () => {
  const old = cm.getValue();
  cm.setOption('mode', modeOf($('#fLang').value));
  if (Object.values(templates).includes(old) || !old.trim()) cm.setValue(templates[$('#fLang').value]);
};
async function save() {
  if (!cur) return null;
  if (sched.type === 'weekly' && !sched.days.length) { toast('Pick at least one day', true); return null; }
  const body = {
    name: $('#fName').value, language: $('#fLang').value, content: cm.getValue(), schedule: cronFromSched(sched),
    enabled: $('#fEnabled').checked, timeout: +$('#fTimeout').value || 0, env: $('#fEnv').value,
  };
  try {
    const s = cur.id ? await api('/scripts/' + cur.id, 'PUT', body) : await api('/scripts', 'POST', body);
    await loadScripts();
    cur = scripts.find(x => x.id === s.id);
    renderList(); showNext();
    $('#delBtn').style.display = '';
    toast('Saved');
    return s.id;
  } catch (e) { toast(e.message, true); return null; }
}
async function run() {
  if (curRun) return;
  const id = await save();
  if (!id) return;
  try { const r = await api(`/scripts/${id}/run`, 'POST'); watch(r.run_id); } catch (e) { toast(e.message, true); }
}
$('#newBtn').onclick = () => select('new');
$('#saveBtn').onclick = save;
$('#runBtn').onclick = run;
$('#stopBtn').onclick = guard(() => api(`/runs/${curRun}/stop`, 'POST'));
$('#clearBtn').onclick = () => $('#out').textContent = '';
$('#delBtn').onclick = guard(async () => {
  if (!confirm(`Delete "${cur.name}"?`)) return;
  await api('/scripts/' + cur.id, 'DELETE');
  cur = null;
  $('#editorBody').style.display = 'none';
  $('#noScript').style.display = '';
  await loadScripts();
});

/* ---- history ---- */
let histEs = null, histSel = null, histTimer = null;
async function loadHistory() {
  const runs = await api('/runs?limit=200');
  $('#histBody').innerHTML = runs.map(r => `<tr class="click ${r.id === histSel ? 'sel' : ''}" data-id="${r.id}" data-t="${esc(r.title)}">
    <td>${r.id}</td><td>${esc(r.title)}</td><td>${r.trigger}</td>
    <td class="s-${r.status}">${r.status}${r.exit_code != null && r.status !== 'success' ? ' (' + r.exit_code + ')' : ''}</td>
    <td>${fmtTime(r.started)}</td><td>${fmtDur(r.started, r.finished)}</td></tr>`).join('');
  document.querySelectorAll('#histBody tr').forEach(tr => tr.onclick = () => {
    histSel = +tr.dataset.id;
    $('#histTitle').textContent = `Run #${histSel} - ${tr.dataset.t}`;
    if (histEs) histEs.close();
    histEs = attach(histSel, $('#histOut'), loadHistory);
    document.querySelectorAll('#histBody tr').forEach(x => x.classList.toggle('sel', x === tr));
  });
  clearTimeout(histTimer);
  if ($('#v-history').classList.contains('active') && runs.some(r => r.status === 'running')) histTimer = setTimeout(loadHistory, 3000);
}

/* ---- packages ---- */
let pkgEs = null;
function pkgJob(path, input) {
  return guard(async () => {
    const v = $(input).value.trim();
    if (!v) return;
    const r = await api(path, 'POST', {packages: v});
    $('#pkgTitle').textContent = 'Run #' + r.run_id;
    if (pkgEs) pkgEs.close();
    pkgEs = attach(r.run_id, $('#pkgOut'), loadPkgs);
    $(input).value = '';
  });
}
async function loadPkgs() {
  const [pip, apt] = await Promise.all([api('/pip'), api('/apt')]);
  $('#pipList').innerHTML = pip.map(p => `<div class="pk"><div>${esc(p.name)}<span class="v">${esc(p.version)}</span></div><button class="btn danger" data-p="${esc(p.name)}" style="padding:1px 8px">remove</button></div>`).join('') || '<div class="hint">Nothing installed.</div>';
  $('#aptList').innerHTML = apt.map(p => `<div class="pk"><div>${esc(p.name)}${p.installed ? '' : '<span class="v s-failed">not installed</span>'}</div><button class="btn danger" data-a="${esc(p.name)}" style="padding:1px 8px">remove</button></div>`).join('') || '<div class="hint">Nothing installed through Pigeonhole.</div>';
  document.querySelectorAll('[data-p]').forEach(b => b.onclick = () => { $('#pipIn').value = b.dataset.p; pkgJob('/pip/uninstall', '#pipIn')(); });
  document.querySelectorAll('[data-a]').forEach(b => b.onclick = () => { $('#aptIn').value = b.dataset.a; pkgJob('/apt/remove', '#aptIn')(); });
}
$('#pipBtn').onclick = pkgJob('/pip/install', '#pipIn');
$('#aptBtn').onclick = pkgJob('/apt/install', '#aptIn');
$('#pipIn').onkeydown = e => { if (e.key === 'Enter') $('#pipBtn').click(); };
$('#aptIn').onkeydown = e => { if (e.key === 'Enter') $('#aptBtn').click(); };

/* ---- settings ---- */
async function loadSettings() {
  const s = await api('/settings');
  $('#sNtfy').value = s.ntfy_url;
  $('#tzInfo').textContent = 'Schedules use timezone: ' + s.timezone;
}
const saveSettings = () => api('/settings', 'PUT', {ntfy_url: $('#sNtfy').value});
$('#sSave').onclick = guard(async () => { await saveSettings(); toast('Settings saved'); });
$('#sTest').onclick = guard(async () => { await saveSettings(); await api('/settings/test', 'POST'); toast('Test notification sent'); });

api('/settings').then(x => { tz = x.timezone; renderSched(); }).catch(() => {});
loadScripts().catch(e => toast(e.message, true));
setInterval(() => {
  if (!$('#v-scripts').classList.contains('active') || curRun) return;
  api('/scripts').then(s => {
    scripts = s;
    const n = cur && cur.id && s.find(x => x.id === cur.id);
    if (n) { cur.next_run = n.next_run; showNext(); if (n.running_run) watch(n.running_run); }
    renderList();
  }).catch(() => {});
}, 15000);
