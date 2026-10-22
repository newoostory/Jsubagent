import { stripVTControlCharacters } from 'node:util';
import { truncateToWidth } from '@earendil-works/pi-tui';
import { taskName } from './task-name.mjs';

const clean = value => stripVTControlCharacters(String(value ?? '').slice(0, 1024)).replace(/[\x00-\x1f\x7f-\x9f\u2028-\u202e\u2066-\u2069]/g, ' ');
const label = (value, width) => stripVTControlCharacters(truncateToWidth(clean(value), width, ''));
export function taskStatus(task) {
  if (task.state === 'queued') return 'waiting';
  if (task.state === 'running' || task.settled === false) return 'working';
  if (task.state === 'idle') return 'done';
  return ['failed', 'cancelled', 'paused', 'interrupted', 'blocked'].includes(task.state) ? task.state : 'unknown';
}
export const COMPLETED_VISIBLE_MS = 5000;
const ICONS = { working: '▶', waiting: '○', done: '✓', failed: '✕', cancelled: '■', paused: 'Ⅱ', interrupted: '!', blocked: '⊘', unknown: '?' };
export function statusIcon(task) { return ICONS[taskStatus(task)]; }
export function statusLabel(task) { return `${statusIcon(task)} ${taskStatus(task)}`; }
export function bottomVisible(task, now = Date.now()) {
  // Presentation only: never archive/delete records or hide actionable failures.
  return !(task.settled === true && ['done', 'cancelled'].includes(taskStatus(task)) && Number.isFinite(task.updatedAt) && now - task.updatedAt >= COMPLETED_VISIBLE_MS);
}
// Pi normalizes input separately from cacheRead/cacheWrite. Never subtract caches again.
export function nonCacheTokens(task) {
  const value = number => Number.isFinite(number) && number >= 0 ? number : 0;
  return [task.usage, task.progress?.usage].reduce((sum, usage) => sum + value(usage?.input) + value(usage?.output), 0);
}
export function formatTokens(tokens) {
  const n = Number.isFinite(tokens) && tokens > 0 ? tokens : 0;
  const divisor = n >= 1_000_000 ? 1_000_000 : 1000;
  // Truncate to one decimal so a sub-million total never renders as 1000k/1m.
  return `${Math.floor(n / divisor * 10) / 10}${divisor === 1000 ? 'k' : 'm'}`;
}
export function teamStatus(status, { now = Date.now() } = {}) {
  const priority = task => ({ working: 0, waiting: 1, blocked: 2, failed: 2, interrupted: 2, paused: 2, cancelled: 3, done: 4 }[taskStatus(task)] ?? 2);
  const tasks = status.tasks.filter(task => bottomVisible(task, now)).sort((a, b) => priority(a) - priority(b) || (b.updatedAt ?? 0) - (a.updatedAt ?? 0) || a.id.localeCompare(b.id));
  const shown = tasks.slice(0, 8), hidden = tasks.length - shown.length;
  const labels = shown.map(t => {
    const name = taskName(t), duplicate = tasks.some(other => other.id !== t.id && taskName(other) === name);
    return `${statusIcon(t)} ${label(name, 16)}${duplicate ? `#${label(t.id, 6)}` : ''} ${formatTokens(nonCacheTokens(t))}`;
  });
  const count = state => tasks.filter(t => taskStatus(t) === state).length;
  const counts = ['waiting', 'working', 'done'].map(state => `${state} ${count(state)}`).join(' ');
  const problems = ['blocked', 'failed', 'cancelled', 'paused', 'interrupted', 'unknown'].filter(state => count(state)).map(state => `${state} ${count(state)}`);
  const header = `${counts}${problems.length ? ` · ${problems.join(' ')}` : ''}${status.pressure ? ' · admission paused' : ''}`;
  const names = `${labels.join(' · ')}${hidden ? ` · +${hidden} /hub` : ''}`;
  return { footer: tasks.length ? header : '', lines: tasks.length ? [header, names] : [] };
}
