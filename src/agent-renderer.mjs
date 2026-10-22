import { stripVTControlCharacters } from 'node:util';
import { Text, truncateToWidth } from '@earendil-works/pi-tui';
import { statusLabel } from './task-status.mjs';

const safe = (value, limit = 160) => {
  const text = stripVTControlCharacters(String(value ?? '').slice(0, 65536)).replace(/[\x00-\x1f\x7f-\x9f\u2028-\u202e\u2066-\u2069]/g, ' ');
  return text.length > limit ? `${text.slice(0, limit)}…` : text;
};
const list = value => Array.isArray(value) ? value.filter(row => row && typeof row === 'object') : [];
const number = (value, fallback = '?') => Number.isSafeInteger(value) && value >= 0 ? value : fallback;
const attentionCount = row => list(row.attention).length;
const count = (n, noun) => `${n} ${noun}${n === 1 ? '' : 's'}`;
const shortId = value => truncateToWidth(safe(value, 80), 12, '…');
const ACTIONS = new Set(['catalog', 'spawn', 'status', 'wait', 'cancel', 'resume', 'archive', 'send', 'read']);
export function actionLabel(args = {}) {
  return ACTIONS.has(args?.action) ? args.action : '…';
}
export function callText(args = {}) {
  args ??= {};
  const action = actionLabel(args), parts = ['agent', action];
  if (action === 'spawn' && Array.isArray(args.tasks)) {
    const tasks = list(args.tasks);
    parts.push(count(args.tasks.length, 'task'));
    if (tasks.length === 1) parts.push(safe(tasks[0].name ?? tasks[0].role ?? args.defaults?.role, 32));
  } else if (action === 'read' && Array.isArray(args.requests)) {
    parts.push(count(args.requests.length, 'reference'));
    parts.push(safe([...new Set(list(args.requests).map(row => row.source ?? 'index'))].join('/'), 60));
  } else if (action === 'resume' && args.agentId) parts.push(shortId(args.agentId));
  else if (Array.isArray(args.agentIds)) parts.push(count(args.agentIds.length, 'agent'));
  if (action === 'send') parts.push(args.delivery === 'steer' ? 'steer' : 'followUp');
  if (action === 'wait') parts.push(args.mode === 'any' ? 'first settled' : 'all settled');
  return parts.filter(Boolean).join(' · ');
}

const problem = row => row.ok === false || row.outcome === 'unknown' || row.error || row.responseError || row.cancellationError || attentionCount(row) || ['failed', 'blocked', 'paused', 'interrupted'].includes(row.state);
function identity(row, rows, expanded) {
  const name = safe(row.name, 32);
  const duplicate = name && rows.some(other => other !== row && safe(other.name, 32) === name);
  const id = row.agentId ?? row.artifactId;
  if (expanded) return name ? `${name} · ${safe(id, 80)}` : safe(id, 80) || `item ${number(row.index, 0) + 1}`;
  return name ? `${name}${duplicate ? ` #${shortId(id)}` : ''}` : shortId(id) || `item ${number(row.index, 0) + 1}`;
}
function rowStatus(row) {
  if (row.ok === false) return `${row.outcome === 'unknown' ? '?' : '✕'} ${safe(row.outcome || 'error', 24)}`;
  if (row.outcome === 'unknown') return '? outcome unknown';
  if (row.cancellationError) return '? cancellation uncertain';
  if (row.archived) return '○ archived · files/mail retained';
  if (row.noop) return '○ no-op · prior state retained';
  if (row.messageId) return '○ message accepted (not completion)';
  if (row.cancellationRequested) return '○ cancellation requested';
  if (row.state === 'idle' && row.settled !== true) return '▶ finalizing';
  // A failed attempt can still be releasing its slot; never hide that failure.
  const status = statusLabel({ ...row, settled: true });
  return `${safe(status, 40)}${row.settled === false && !['running', 'queued'].includes(row.state) ? ' · finalizing' : ''}`;
}
function readRow(row, name) {
  if (row.ok === false) return `✕ error · ${name} · ${safe(row.error?.message, 120)}`;
  if (row.source === 'receipt') return `${row.outcome === 'unknown' ? '?' : '○'} receipt · ${name} · ${safe(row.outcome)} · ${safe(row.phase)} · not processing confirmation`;
  if (row.source === 'index') return `index · ${name} · ${count(list(row.records).length, 'response')}${row.omittedRecords ? ` · ${number(row.omittedRecords)} older omitted` : ''}`;
  return `${safe(row.source, 20)} · ${name} · ${number(row.bytesRead ?? 0)} bytes · ${row.eof === true ? 'end' : 'more available'}`;
}
function waitSummary(value, rows, args) {
  const settled = rows.filter(row => row.settled === true).length;
  const received = rows.filter(row => row.response).length, reused = rows.filter(row => row.responseReceived).length;
  const parts = [`${settled}/${rows.length} settled`];
  if (received) parts.push(`${count(received, 'result')} received`);
  if (reused) parts.push(`${reused} already received`);
  if (args.results === 'none') parts.push('metadata only');
  if (rows.some(row => row.response?.eof === false || row.responseComplete === false)) parts.push('more result text available');
  if (rows.some(row => row.responseError)) parts.push('result unavailable; inspect read reference');
  if (value.reason === 'timeout') parts.push('wait window ended'); // Historical/internal bounded waits.
  return parts.join(' · ');
}
function detailsText(value) {
  // Limit depth, visited nodes and output BEFORE pretty formatting. A tiny,
  // deeply nested JSON result must not allocate megabytes of indentation.
  let remaining = 12288, nodes = 0, clipped = false;
  const parts = [];
  const append = text => {
    if (text.length > remaining) clipped = true;
    parts.push(text.slice(0, remaining)); remaining = Math.max(0, remaining - text.length);
  };
  const visit = (item, depth) => {
    if (!remaining) { clipped = true; return; }
    if (++nodes > 512 || depth >= 10) { clipped = true; append('"[details omitted]"'); return; }
    if (item === null || typeof item !== 'object') { append(JSON.stringify(item)); return; }
    const array = Array.isArray(item), keys = Object.keys(item);
    append(array ? '[' : '{');
    for (const [index, key] of keys.entries()) {
      if (!remaining || nodes >= 512) { clipped = true; break; }
      append(`${index ? ',' : ''}\n${'  '.repeat(depth + 1)}`);
      if (!array) append(`${JSON.stringify(key)}: `);
      visit(item[key], depth + 1);
    }
    if (keys.length) append(`\n${'  '.repeat(depth)}`);
    append(array ? ']' : '}');
  };
  visit(value, 0);
  const text = parts.join('').split('\n').map(line => { if (line.length > 512) clipped = true; return safe(line, 512); }).join('\n');
  return `Details${clipped ? ' (preview)' : ''}\n${text}${clipped ? '\n… details clipped; use read/status for the remaining evidence.' : ''}`;
}
export function resultText(result, { expanded = false, isPartial = false, isError = false } = {}, args = {}) {
  args ??= {};
  if (isPartial) return args.action === 'wait' ? 'Waiting for agents…' : 'Working…';
  const raw = list(result?.content).find(part => part.type === 'text' && typeof part.text === 'string')?.text ?? '';
  if (isError || result?.isError) return `✕ error · ${safe(raw, expanded ? 2048 : 240)}`;
  let value;
  // Bound parsing and rendering independently from the model-facing response.
  if (raw.length <= 65536) { try { value = JSON.parse(raw); } catch {} }
  if (!value || typeof value !== 'object') return safe(raw, expanded ? 2048 : 200) || 'No text result';
  const rows = list(value.results ?? value.agents);
  const lines = [];
  if (value.stale) lines.push('! Stale snapshot · refresh status');
  if (value.reason === 'attention' || rows.some(attentionCount)) lines.push('! Question/blocker needs attention · read its reference');
  if (args.action === 'wait' && rows.length) lines.push(waitSummary(value, rows, args));
  if (rows.length) {
    // Keep actionable errors visible even when later in the batch. Expanded order is canonical.
    const ordered = expanded ? rows : [...rows.filter(problem), ...rows.filter(row => !problem(row))];
    const shown = ordered.slice(0, expanded ? 8 : 3);
    for (const row of shown) {
      const name = identity(row, rows, expanded);
      if (args.action === 'read') lines.push(readRow(row, name));
      else {
        const error = row.error?.message ?? row.cancellationError?.message ?? row.cancellationError ?? row.responseError?.message;
        lines.push(`${rowStatus(row)} · ${name}${error ? ` · ${safe(error, 120)}` : ''}${attentionCount(row) ? ` · ${attentionCount(row)} needs attention` : ''}`);
      }
    }
    if (rows.length > shown.length) {
      const hiddenProblems = ordered.slice(shown.length).filter(problem).length;
      lines.push(`+${rows.length - shown.length} more${hiddenProblems ? ` · ${hiddenProblems} need attention` : ''} · expand for details`);
    }
  } else if (Array.isArray(value.roles)) lines.push(`${count(value.roles.length, 'role')} · ${count(list(value.profiles).length, 'profile')}`);
  else if (Array.isArray(value.agents) || Array.isArray(value.results)) lines.push(args.action === 'read' ? 'No references' : 'No agents');
  else lines.push(safe(raw, expanded ? 2048 : 200) || 'No text result');
  if (expanded) lines.push(detailsText(value));
  return lines.join('\n');
}

export function renderAgentCall(args, theme, context = {}) {
  const component = context.lastComponent ?? new Text('', 0, 0);
  const text = callText(args), title = `agent · ${actionLabel(args)}`;
  component.setText(theme.fg('toolTitle', theme.bold(title)) + theme.fg('muted', text.slice(title.length)));
  return component;
}
export function renderAgentResult(result, options, theme, context = {}) {
  const component = context.lastComponent ?? new Text('', 0, 0);
  const text = resultText(result, { ...options, isError: context.isError ?? options?.isError }, context.args);
  const paint = (color, line) => theme?.fg ? theme.fg(color, line) : line;
  component.setText(text.split('\n').map(line => paint(line.startsWith('✕') ? 'error' : /^[!?⊘Ⅱ]/.test(line) ? 'warning' : line.startsWith('✓') ? 'success' : 'muted', line)).join('\n'));
  return component;
}
