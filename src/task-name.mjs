import { fail } from './limits.mjs';

export const NAME_PATTERN = '^(?=.*\\S)[^\\u0000-\\u001f\\u007f-\\u009f\\u2028-\\u202e\\u2066-\\u2069]+$';
export function validateTaskName(name) {
  if (typeof name !== 'string' || !name.trim() || [...name].length > 32 || !new RegExp(NAME_PATTERN).test(name)) throw fail('name must be 1–32 visible characters on one line (no terminal/bidi controls)');
  return name.trim();
}
export function taskName(task) {
  return task.name || `${typeof task.role === 'string' ? task.role : task.role?.name ?? 'agent'}-${String(task.id).slice(0, 8)}`;
}
export function assignTaskName(requested, role, tasks) {
  if (requested !== undefined) return validateTaskName(requested);
  const used = new Set([...tasks].map(taskName));
  const prefix = role.slice(0, 24);
  for (let n = 1; ; n++) if (!used.has(`${prefix}-${n}`)) return `${prefix}-${n}`;
}
