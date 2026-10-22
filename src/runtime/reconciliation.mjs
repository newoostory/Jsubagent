import { join, relative, isAbsolute } from 'node:path';
import { realpath } from 'node:fs/promises';
import { readJson } from '../storage.mjs';
import { fail, hash } from '../limits.mjs';
import { effectRequiresReconciliation } from '../execution/failure-receipt.mjs';

export const reconciliationPath = (agentDir, root, id) => join(agentDir, 'pi-agent', 'reconciliations', `${hash(`${root}\0${id}`)}.json`);
export function recoveryIdentity(task) {
  return hash(JSON.stringify({ id: task.id, requestHash: task.requestHash, intent: task.executionIntent, effects: task.pendingEffects, workspace: task.worktreeIntent, usage: task.usage, turns: task.turns, usageUncertain: task.usageUncertain, ...(task.requestPermitUncertain ? { requestPermitUncertain: task.requestPermitUncertain } : {}), cancelled: task.cancelRequested, revoked: task.authorityRevoked }));
}
// Only a user-global, expiring confirmation can settle unknown outcomes. Neither
// tool arguments nor workspace files constitute this authority.
export async function readReconciliation(config, store, task) {
  const path = reconciliationPath(config.agentDir, store.root, task.id);
  let grant;
  try { grant = await readJson(path, 65536); } catch (error) { if (error.code === 'ENOENT') return null; throw error; }
  const directory = join(config.agentDir, 'pi-agent', 'reconciliations');
  if (await realpath(directory) !== directory) throw fail('Symlinked reconciliation directory', 'PERMISSION');
  const rel = relative(await realpath(task.sourceCwd ?? task.cwd), await realpath(directory));
  if (rel === '' || (!rel.startsWith(`..${process.platform === 'win32' ? '\\' : '/'}`) && rel !== '..' && !isAbsolute(rel))) throw fail('Reconciliation authority must be outside the task workspace', 'PERMISSION');
  if (!grant || Object.keys(grant).some(key => !['root', 'agentId', 'expectedIdentity', 'createdAt', 'expiresAt', 'decision', 'effects', 'usage', 'evidence'].includes(key)) || grant.root !== store.root || grant.agentId !== task.id || grant.expectedIdentity !== recoveryIdentity(task) || !['resume', 'abandon'].includes(grant.decision) || typeof grant.evidence !== 'string' || !grant.evidence.trim() || !Number.isSafeInteger(grant.createdAt) || !Number.isSafeInteger(grant.expiresAt) || grant.createdAt > Date.now() || grant.expiresAt <= Date.now() || grant.expiresAt - grant.createdAt > 3600000) throw fail('Stale or invalid reconciliation confirmation', 'PERMISSION');
  if ((task.cancelRequested || task.authorityRevoked) && grant.decision !== 'abandon') throw fail('Reconciliation cannot revoke cancellation or restore authority', 'PERMISSION');
  if (task.worktreeIntent && grant.decision !== 'abandon') throw fail('Uncertain workspace creation requires explicit abandonment', 'UNCERTAIN');
  if (task.requestPermitUncertain && grant.decision !== 'abandon') throw fail('Uncertain provider permit acknowledgement requires shared-ledger inspection; usage confirmation cannot release it', 'UNCERTAIN');
  const pending = (task.pendingEffects ?? []).filter(effectRequiresReconciliation);
  if (!Array.isArray(grant.effects) || grant.effects.length !== pending.length || new Set(grant.effects.map(row => row.stepId)).size !== pending.length) throw fail('Reconciliation must cover every uncertain effect exactly once', 'PERMISSION');
  const effects = [];
  for (const step of pending) {
    const row = grant.effects.find(row => row.stepId === step.stepId);
    if (!row || Object.keys(row).some(key => !['stepId', 'inputDigest', 'outcome', 'resultRef'].includes(key)) || row.inputDigest !== step.inputDigest || !['completed', 'not-executed', 'abandoned'].includes(row.outcome) || (row.outcome === 'abandoned' && grant.decision !== 'abandon')) throw fail('Effect confirmation does not bind its intent', 'PERMISSION');
    if (grant.decision === 'resume' && (!step.inputCanonical || (step.outcome === 'completed' && (row.outcome !== 'completed' || ['artifactId', 'bytes', 'sha256'].some(key => row.resultRef?.[key] !== step.resultRef?.[key]))))) throw fail('Durable completed outcomes cannot be downgraded or replaced', 'PERMISSION');
    if (row.outcome === 'completed') {
      const ref = row.resultRef, identity = ref && await store.artifactIdentity(ref.artifactId);
      if (!identity?.complete || identity.owner !== task.id || (await store.artifactInfo(ref.artifactId)).label !== 'tool-effect-outcome' || identity.bytes !== ref.bytes || identity.sha256 !== ref.sha256) throw fail('Reconciled outcome identity changed', 'STORAGE');
      const body = await readJson(join(store.root, 'artifacts', `${ref.artifactId}.bin`), store.limits.responseBytes + store.limits.inputBytes);
      if (!body || !Object.hasOwn(body, 'result') || Object.hasOwn(body, 'error')) throw fail('Completed effect requires a successful outcome receipt', 'STORAGE');
    }
    effects.push({ ...step, reconciled: true, outcome: row.outcome, ...(row.resultRef ? { resultRef: row.resultRef } : {}) });
  }
  // Lifetime accounting is optional evidence, never required for continuation.
  if (grant.usage !== undefined) {
    if (grant.usage && Object.keys(grant.usage).some(key => !['turns', 'state', 'input', 'output', 'cacheRead', 'cacheWrite', 'cost'].includes(key))) throw fail('Invalid usage confirmation fields', 'PERMISSION');
    if (!grant.usage || !Number.isSafeInteger(grant.usage.turns) || grant.usage.turns < (task.turns ?? 0) || grant.usage.state !== undefined && !['observed', 'estimated'].includes(grant.usage.state)) throw fail('Unknown usage requires confirmed lifetime accounting', 'PERMISSION');
    for (const key of ['input', 'output', 'cacheRead', 'cacheWrite', 'cost']) if (!Number.isFinite(grant.usage[key]) || grant.usage[key] < (task.usage?.[key] ?? 0)) throw fail('Usage confirmation cannot erase prior accounting', 'PERMISSION');
  }
  return { grant, effects };
}
