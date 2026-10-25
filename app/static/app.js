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
  python: '#!/usr/bin/env python3\nprint("Hello from Scriptorium")\n',
  bash: '#!/usr/bin/env bash\nset -euo pipefail\necho "Hello from Scriptorium"\n',
};
const modeOf = l => l === 'python' ? 'python' : 'shell';

async function loadScripts() {
  scripts = await api('/scripts');
  renderList();
}
function renderList() {
  $('#scriptList').innerHTML = scripts.map(s => `<div class="item ${cur && cur.id === s.id ? 'active' : ''}" data-id="${s.id}">
    <span class="dot ${s.last_status || ''}"></span><span class="name">${esc(s.name)}</span>
    ${s.schedule && s.enabled ? '<span class="badge" title="' + esc(s.schedule) + '">cron</span>' : ''}<span class="badge">${s.language === 'python' ? 'py' : 'sh'}</span></div>`).join('')
    || '<div class="hint" style="padding:12px">No scripts yet.</div>';
  document.querySelectorAll('.item').forEach(i => i.onclick = () => select(+i.dataset.id));
}
function select(id) {
  cur = id === 'new'
    ? {id: null, name: '', language: 'python', content: templates.python, schedule: '', enabled: true, timeout: 0}
    : scripts.find(s => s.id === id);
  if (!cur) return;
  $('#noScript').style.display = 'none';
  $('#editorBody').style.display = 'flex';
  $('#fName').value = cur.name; $('#fLang').value = cur.language; $('#fSched').value = cur.schedule;
  $('#fTimeout').value = cur.timeout; $('#fEnabled').checked = cur.enabled;
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
function showNext() { $('#nextRun').textContent = cur && cur.next_run ? 'next: ' + fmtTime(cur.next_run) : ''; }
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
$('#fLang').onchange = () => {
  const old = cm.getValue();
  cm.setOption('mode', modeOf($('#fLang').value));
  if (Object.values(templates).includes(old) || !old.trim()) cm.setValue(templates[$('#fLang').value]);
};
async function save() {
  if (!cur) return null;
  const body = {
    name: $('#fName').value, language: $('#fLang').value, content: cm.getValue(), schedule: $('#fSched').value,
    enabled: $('#fEnabled').checked, timeout: +$('#fTimeout').value || 0,
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
  $('#aptList').innerHTML = apt.map(p => `<div class="pk"><div>${esc(p.name)}${p.installed ? '' : '<span class="v s-failed">not installed</span>'}</div><button class="btn danger" data-a="${esc(p.name)}" style="padding:1px 8px">remove</button></div>`).join('') || '<div class="hint">Nothing installed through Scriptorium.</div>';
  document.querySelectorAll('[data-p]').forEach(b => b.onclick = () => { $('#pipIn').value = b.dataset.p; pkgJob('/pip/uninstall', '#pipIn')(); });
  document.querySelectorAll('[data-a]').forEach(b => b.onclick = () => { $('#aptIn').value = b.dataset.a; pkgJob('/apt/remove', '#aptIn')(); });
}
$('#pipBtn').onclick = pkgJob('/pip/install', '#pipIn');
$('#aptBtn').onclick = pkgJob('/apt/install', '#aptIn');
$('#pipIn').onkeydown = e => e.key === 'Enter' && $('#pipBtn').click();
$('#aptIn').onkeydown = e => e.key === 'Enter' && $('#aptBtn').click();

/* ---- settings ---- */
async function loadSettings() {
  const s = await api('/settings');
  $('#sWebhook').value = s.webhook_url;
  $('#sNtfy').value = s.ntfy_url;
  $('#tzInfo').textContent = 'Cron schedules use timezone: ' + s.timezone;
}
const saveSettings = () => api('/settings', 'PUT', {webhook_url: $('#sWebhook').value, ntfy_url: $('#sNtfy').value});
$('#sSave').onclick = guard(async () => { await saveSettings(); toast('Settings saved'); });
$('#sTest').onclick = guard(async () => { await saveSettings(); await api('/settings/test', 'POST'); toast('Test notification sent'); });

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
