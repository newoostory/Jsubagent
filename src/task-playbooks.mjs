import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { fail, hash } from './limits.mjs';
import { workflowAdapter } from './workflows/index.mjs';

export const TASK_PLAYBOOK_BYTES = 12 * 1024;
const require = createRequire(import.meta.url);
const ROLE_PHASE = { worker: 'execute', integrator: 'execute', reviewer: 'review', verifier: 'review', scout: 'plan', design: 'execute' };
const KEYS = ['loaded', 'prompt', 'route', 'sha256', 'version'];

export function taskPlaybookEligible(task) {
  if (!task?.role || task.workItem || task.role.hidden || task.profile?.hidden || task.role.requiredMode || task.profile?.requiredMode || task.role.inputMode || task.profile?.inputMode) return false;
  return workflowAdapter(task).id === 'generic';
}

async function installedRuntime() {
  let manifest;
  try { manifest = require.resolve('pi-super/package.json'); }
  catch (error) { if (error.code === 'MODULE_NOT_FOUND') return null; throw error; }
  // Once present, a broken installed runtime is an error, not a silent fallback.
  return import(pathToFileURL(join(dirname(manifest), 'src/runtime.js')).href);
}

export async function createTaskPlaybookSnapshot(task) {
  if (!taskPlaybookEligible(task)) return null;
  const runtime = await installedRuntime();
  if (!runtime) return null;
  const { routePrompt, PLAYBOOK_ROLES } = await import('pi-super/src/router.js');
  const role = PLAYBOOK_ROLES.includes(task.role.name) ? task.role.name : null;
  // Route the assigned goal, not unrelated words in the prohibition/handoff
  // fields. The complete assignment and actual ACL still govern execution.
  const goalLine = /^\s*(?:Goal|목표)\s*:\s*(.+)$/imu.exec(task.task ?? '')?.[1];
  const routingText = goalLine ?? task.task ?? '';
  const inferred = routePrompt(routingText, { phase: ROLE_PHASE[role] ?? 'plan' });
  const phase = ['reviewer', 'verifier', 'scout'].includes(role) ? ROLE_PHASE[role] : inferred.phase;
  const selected = await runtime.buildRuntimeContext(routingText, {}, { phase, ...(role ? { role } : {}) });
  if (Buffer.byteLength(selected.prompt, 'utf8') > TASK_PLAYBOOK_BYTES) throw fail('Task playbook exceeds its context budget', 'LIMIT');
  return { version: 1, route: selected.route, loaded: selected.loaded, prompt: selected.prompt, sha256: hash(selected.prompt) };
}

export function taskPlaybookPrompt(task) {
  const snapshot = task?.playbook;
  if (snapshot === undefined) return '';
  if (!taskPlaybookEligible(task) || !snapshot || typeof snapshot !== 'object' || Array.isArray(snapshot)
    || Object.keys(snapshot).sort().join('\0') !== KEYS.join('\0') || snapshot.version !== 1
    || typeof snapshot.prompt !== 'string' || !snapshot.prompt.trim() || Buffer.byteLength(snapshot.prompt, 'utf8') > TASK_PLAYBOOK_BYTES
    || snapshot.sha256 !== hash(snapshot.prompt) || !Array.isArray(snapshot.loaded) || snapshot.loaded.length > 6
    || !snapshot.route || typeof snapshot.route !== 'object' || Array.isArray(snapshot.route)
    || !['coding', 'research', 'agents', 'design', 'external', 'general'].includes(snapshot.route.primary)
    || !['plan', 'execute', 'review'].includes(snapshot.route.phase)
    || snapshot.route.role !== undefined && snapshot.route.role !== task.role.name
    || snapshot.loaded.some(path => typeof path !== 'string' || !/^(?:spine\.md|domains\/[a-z]+\/(?:plan|execute|review)\.md|modules\/[a-z]+\.md|roles\/[a-z]+\.md)$/u.test(path))) {
    throw fail('Invalid or changed task playbook snapshot', 'STORAGE');
  }
  return `\n\n${snapshot.prompt}`;
}
