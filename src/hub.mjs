import { stripVTControlCharacters } from 'node:util';
import { taskName } from './task-name.mjs';
import { taskStatus, statusLabel, statusIcon, nonCacheTokens, formatTokens } from './task-status.mjs';
import { Key, matchesKey, truncateToWidth, visibleWidth, wrapTextWithAnsi } from '@earendil-works/pi-tui';
import { inspectorSafetyLines } from './inspector-state.mjs';

export function displayText(value) {
  return stripVTControlCharacters(String(value ?? '—').slice(0, 16384)).replace(/[\x00-\x1f\x7f-\x9f\u2028\u2029\u202a-\u202e\u2066-\u2069]/g, ' ');
}
const clip = (text, width) => width > 0 ? truncateToWidth(text, width, '…') : '';
const metric = value => Number.isFinite(value) ? String(value) : '—';
function usageText(task) {
  const saved = task.usage, progress = task.progress?.usage;
  if (!saved && !progress) return 'Usage: — (not reported)';
  const n = key => !Number.isFinite(saved?.[key]) && !Number.isFinite(progress?.[key]) ? undefined : (saved?.[key] ?? 0) + (progress?.[key] ?? 0);
  return `Reported usage: ↑${metric(n('input'))} ↓${metric(n('output'))} cache ${metric(n('cacheRead'))}/${metric(n('cacheWrite'))} $${Number.isFinite(n('cost')) ? n('cost').toFixed(4) : '—'}`;
}
export function tailLines(tail) {
  if (!tail) return ['— No transcript recorded'];
  const lines = [];
  if (tail.truncated) lines.push(`… tail from byte ${tail.byteOffset} / ${tail.fileBytes}`);
  for (const raw of tail.text.split('\n').slice(-100)) {
    if (!raw) continue;
    try {
      const entry = JSON.parse(raw), m = entry.message ?? entry;
      const content = typeof m.content === 'string' ? m.content : (m.content ?? []).map(c => c.type === 'toolCall' ? `[tool ${c.name}] ${JSON.stringify(c.arguments)}` : c.text ?? c.thinking ?? `[${c.type}]`).join(' ');
      lines.push(displayText(`${m.role ?? 'event'}: ${content}`));
    } catch { lines.push(displayText(raw)); }
  }
  return lines.length ? lines : ['— Transcript empty'];
}
export class Hub {
  constructor({ read, isAlive = () => true, requestRender, done, theme, height = () => 24, intervalMs = 1000 }) {
    Object.assign(this, { read, isAlive, requestRender, done, theme, height, intervalMs });
    this.snapshot = { tasks: [], note: 'Loading read-only snapshot…' };
    this.panel = 'roster'; this.detailOffset = 0; this.revision = 0; this.closed = false; this.busy = false;
  }
  start() {
    if (this.closed || this.timer) return;
    void this.refresh(); this.timer = setInterval(() => { void this.refresh(); }, this.intervalMs); this.timer.unref?.();
  }
  async refresh() {
    if (this.closed) return;
    if (!this.isAlive()) { this.close(); return; }
    if (this.busy) { this.again = true; return; }
    this.busy = true; const revision = this.revision, selected = this.selectedId;
    try {
      const snapshot = await this.read(selected);
      if (this.closed || !this.isAlive() || revision !== this.revision) return;
      this.snapshot = snapshot; this.selectedId = snapshot.selectedId; this.error = undefined;
    } catch (e) {
      if (!this.closed && this.isAlive() && revision === this.revision) this.error = displayText(e.message);
    } finally {
      this.busy = false;
      if (!this.closed && this.isAlive()) {
        this.requestRender();
        if (this.again) { this.again = false; void this.refresh(); }
      }
    }
  }
  close() { if (this.closed) return; this.dispose(); this.done(); }
  dispose() { this.closed = true; this.revision++; clearInterval(this.timer); this.timer = undefined; }
  invalidate() {} // Render derives current theme and dimensions; no styled cache.
  handleInput(data) {
    if (this.closed) return;
    if (matchesKey(data, Key.escape) || matchesKey(data, Key.ctrl('c')) || data === 'q') { this.close(); return; }
    const tasks = this.snapshot.tasks, current = Math.max(0, tasks.findIndex(t => t.id === this.selectedId));
    let next = current;
    if (matchesKey(data, Key.up) || data === 'k') next = Math.max(0, current - 1);
    else if (matchesKey(data, Key.down) || data === 'j') next = Math.min(tasks.length - 1, current + 1);
    else if (matchesKey(data, Key.tab) || matchesKey(data, Key.shift('tab'))) this.panel = this.panel === 'roster' ? 'detail' : 'roster';
    else if (matchesKey(data, 'pageDown')) this.detailOffset = Math.min(this.detailMax ?? 0, this.detailOffset + 5);
    else if (matchesKey(data, 'pageUp')) this.detailOffset = Math.max(0, this.detailOffset - 5);
    else if (data === 'r') void this.refresh();
    if (next !== current && tasks[next]) {
      this.selectedId = tasks[next].id; this.detailOffset = 0; this.revision++;
      // Never display the prior task's tail during an asynchronous selection change.
      this.snapshot = { ...this.snapshot, tail: undefined, tailError: undefined };
      void this.refresh();
    }
    this.requestRender();
  }
  render(width) {
    if (width < 1) return [''];
    const requestedHeight = Math.floor(this.height());
    const h = Number.isFinite(requestedHeight) ? Math.max(1, Math.min(1000, requestedHeight)) : 24;
    const tasks = this.snapshot.tasks, selected = tasks.find(t => t.id === this.selectedId) ?? tasks[0];
    const count = state => tasks.filter(t => taskStatus(t) === state).length;
    const header = ' AGENT HUB';
    const counts = width < 70
      ? ` ▶ work ${count('working')} · ○ wait ${count('waiting')} · ✕ fail ${count('failed')} · ✓ done ${count('done')}${count('blocked') ? ` · ⊘ ${count('blocked')}` : ''}`
      : ` ▶ working ${count('working')}   ○ waiting ${count('waiting')}   ✕ failed ${count('failed')}   ✓ done ${count('done')}${count('blocked') ? `   ⊘ blocked ${count('blocked')}` : ''}`;
    const snapshotNote = this.snapshot.online ? '' : displayText(this.snapshot.note ?? '');
    const note = this.error ? ` STALE / read failed: ${this.error}` : ` ${snapshotNote}${this.snapshot.pressure ? ' · admission paused' : ''}`;
    const hint = width < 70 ? ' ↑↓ select · Tab view · Esc close' : ' ↑↓ select · Tab view · PgUp/PgDn scroll · r refresh · Esc close';
    const pad = (line, size) => { const value = clip(line, size); return value + ' '.repeat(Math.max(0, size - visibleWidth(value))); };
    // First draft is explicitly achromatic and opaque, independent of the user's
    // accent palette. Pad every terminal cell so the underlying Pi view cannot leak.
    const frame = lines => lines.slice(0, h).map(line => `\x1b[48;5;0m\x1b[38;5;252m${pad(line, width)}\x1b[0m`);
    if (h < 8) return frame(Array.from({ length: h }, (_, i) => [header, note, hint][i] ?? ''));
    const bodyHeight = h - 6, wide = width >= 96;
    const leftWidth = Math.max(28, Math.min(48, Math.floor(width * 0.34))), rightWidth = width - leftWidth - 3;
    const paneWidth = wide ? leftWidth : width;
    const index = Math.max(0, tasks.indexOf(selected)), visibleTasks = Math.max(1, Math.floor((bodyHeight - 2) / 2));
    const start = Math.max(0, Math.min(index - visibleTasks + 1, tasks.length - visibleTasks));
    const roster = [` ROSTER  ${tasks.length ? index + 1 : 0}/${tasks.length}`, ''];
    for (const t of tasks.slice(start, start + visibleTasks)) {
      const chosen = t.id === selected?.id;
      const state = taskStatus(t), nameWidth = Math.max(1, paneWidth - state.length - 7);
      const row = pad(`${chosen ? '›' : ' '} ${statusIcon(t)} ${clip(displayText(taskName(t)), nameWidth)}  ${state}`, paneWidth);
      roster.push(chosen ? `\x1b[7m${row}\x1b[27m` : row);
      roster.push(`   ${displayText(t.role)} · ${formatTokens(nonCacheTokens(t))} tokens`);
    }
    if (!tasks.length) roster.push(' No agents in this branch.', '', ' Ask the main agent to delegate.', ' This view never starts work.');
    const parentName = id => displayText(taskName(tasks.find(t => t.id === id) ?? { id, role: 'agent' }));
    const details = (selected ? [
      ` INSPECTOR  ${displayText(taskName(selected))}`, ` ${statusLabel(selected)}${this.snapshot.online ? '' : ' · saved state; liveness unknown'}`, '',
      ' TASK', ` ${displayText(selected.task)}`,
      ...(selected.error ? ['', ' ATTENTION', ` ${displayText(selected.error)}`] : []),
      ...inspectorSafetyLines(selected, this.snapshot),
      ...(selected.result ? ['', ' RESULT', ` ${displayText(selected.result)}`] : []),
      ...(selected.dependsOn?.length ? ['', ` DEPENDENCIES · ${displayText(selected.dependencyState)}`, ` ${selected.dependsOn.map(parentName).join(', ')}`] : []),
      ...(selected.blockedBy?.length ? [` Blocked by ${selected.blockedBy.map(parentName).join(', ')}. Repair predecessors before explicit resume.`] : []),
      '', ` ${displayText(selected.role)} · profile ${displayText(selected.profile)} · turns ${metric(selected.progress?.turns ?? selected.turns)}`,
      ` ${usageText(selected)}`,
      ` Model: ${displayText(selected.provider)}/${displayText(selected.model)}`,
      ` Workspace: ${displayText(selected.cwd)}`,
      ` agentId: ${displayText(selected.id)} · engine ${displayText(selected.state)}`,
      ` Saved: ${Number.isFinite(selected.updatedAt) && Math.abs(selected.updatedAt) < 8.64e15 ? new Date(selected.updatedAt).toISOString() : '—'}`,
      '', ' ACTIVITY / TRANSCRIPT TAIL · bounded 8 KiB',
      ...(this.snapshot.tailError ? [` Unavailable: ${displayText(this.snapshot.tailError)}`] : tailLines(this.snapshot.tail).map(line => ` ${line}`)),
    ] : [' INSPECTOR', '', ' Select an agent to inspect its task, result and activity.', '', ' Completed records stay here after their bottom entries disappear.', ' Opening /hub never starts a host or model.']).flatMap(line => wrapTextWithAnsi(displayText(line), Math.max(1, wide ? rightWidth : width)));
    this.detailMax = Math.max(0, details.length - bodyHeight); this.detailOffset = Math.min(this.detailOffset, this.detailMax);
    const detail = details.slice(this.detailOffset, this.detailOffset + bodyHeight);
    const lines = [header, counts, note, '─'.repeat(width)];
    for (let i = 0; i < bodyHeight; i++) {
      if (wide) lines.push(pad(roster[i] ?? '', leftWidth) + ' │ ' + clip(detail[i] ?? '', rightWidth));
      else lines.push(clip((this.panel === 'roster' ? roster : detail)[i] ?? '', width));
    }
    lines.push('─'.repeat(width), hint);
    return frame(lines);
  }
}
