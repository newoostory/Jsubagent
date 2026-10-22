import { join, relative, isAbsolute } from 'node:path';
import { realpath, stat } from 'node:fs/promises';
import { readJson } from './storage.mjs';
import { fail, hash } from './limits.mjs';

// Trusted local run registration, never a model-supplied activation field.
// Grants are specific to one host/branch, expire, and live outside candidate workspaces.
const RESERVED_ROLES = new Set(['bench-solo', 'bench-draft', 'bench-critique', 'bench-integrate']);
export const isBenchmark = (role, profile) => role?.requiredMode === 'benchmark' || profile?.requiredMode === 'benchmark' || RESERVED_ROLES.has(role?.name) || profile?.name === 'bench-camel-auto';
export const benchmarkGrantPath = (agentDir, hostRoot) => join(agentDir, 'pi-agent', 'benchmark-grants', `${hash(hostRoot)}.json`);
const inside = (root, path) => { const rel = relative(root, path); return rel === '' || (rel !== '..' && !rel.startsWith('../') && !isAbsolute(rel)); };
export async function loadBenchmarkGrant(agentDir, hostRoot, now = Date.now()) {
  let grant;
  try { grant = await readJson(benchmarkGrantPath(agentDir, hostRoot), 64 * 1024); }
  catch (error) { if (error.code === 'ENOENT') return null; throw error; }
  if (!grant || 'version' in grant || grant.hostRoot !== hostRoot || !Number.isSafeInteger(grant.expiresAt) || !Number.isSafeInteger(grant.createdAt) || grant.createdAt > now || grant.expiresAt <= now || grant.expiresAt - grant.createdAt > 4 * 60 * 60 * 1000) return null;
  const root = await realpath(grant.root);
  if (!(await stat(root)).isDirectory() || root !== grant.root || inside(root, await realpath(agentDir))) throw fail('Invalid benchmark workspace root', 'MODE');
  if (!Array.isArray(grant.jobs) || !grant.jobs.length || grant.jobs.length > 128) throw fail('Invalid benchmark grant jobs', 'MODE');
  for (const job of grant.jobs) {
    if (!job || typeof job.role !== 'string' || typeof job.profile !== 'string' || !/^[a-f0-9]{64}$/.test(job.taskSha256) || typeof job.directory !== 'string' || !inside(root, job.directory) || await realpath(job.directory) !== job.directory) throw fail('Invalid benchmark grant job', 'MODE');
  }
  return { ...grant, root };
}
export function assertBenchmarkContract(role, profile, active) {
  if (role?.requiredMode !== 'benchmark' || profile?.requiredMode !== 'benchmark' || role.hidden !== true || !active?.jobs || active.expiresAt <= Date.now()) throw fail('Benchmark requires a registered, unexpired benchmark role/profile pair', 'MODE');
  if (profile.provider !== 'camel-stream' || profile.model !== 'auto' || profile.instructions || !['readonly', 'shared'].includes(role.workspace) || !Number.isInteger(role.maxTurns) || role.maxTurns < 1 || role.maxTurns > 32 || role.tools.some(tool => !['read', 'write', 'edit', 'ls'].includes(tool))) throw fail('Benchmark permits only Camel Auto and confined file tools; no web, shell, mail, or artifact access', 'MODE');
}
export function authorizeBenchmarkJob(role, profile, grant, { directory, task, projectInstructions, dependsOn = [] }) {
  if (!isBenchmark(role, profile)) return;
  assertBenchmarkContract(role, profile, grant);
  if (projectInstructions || dependsOn.length || !grant.jobs.some(job => job.role === role.name && job.profile === profile.name && job.directory === directory && job.taskSha256 === hash(task))) throw fail('Task is not a preregistered benchmark job', 'MODE');
}
