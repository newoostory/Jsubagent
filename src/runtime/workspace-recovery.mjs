import { isAbsolute } from 'node:path';
import { encodeJson, fail, hash, identifier } from '../limits.mjs';
import { processIdentity } from './ownership.mjs';

export const WORKSPACE_PREPARATION_INTENT_POLICY = 'future-workspace-pre-materialization-v1';
export const WORKSPACE_PREPARATION_RECOVERY_LIMIT = 1;

const KINDS = new Set(['directory-snapshot', 'git-worktree', 'result-product-snapshot']);
const INTENT_KEYS = new Set([
  'policy', 'intentId', 'taskId', 'attemptId', 'executionSegment', 'kind', 'base', 'inputSha256',
  'destination', 'inputRevision', 'snapshotRevision', 'authorityRevision', 'modelProfileFingerprint',
  'owner', 'preparedAt', 'materializationGeneration',
]);
const OWNER_KEYS = new Set(['pid', 'started', 'nonce', 'generation']);
const EXPECTED_KEYS = new Set([
  'kind', 'base', 'inputSha256', 'destination', 'inputRevision', 'snapshotRevision',
  'authorityRevision', 'modelProfileFingerprint',
]);
const AUDIT_KEYS = new Set([
  'policy', 'intentId', 'taskId', 'attemptId', 'inputSha256', 'destination', 'outcome',
  'preparedGeneration', 'recoveryGeneration', 'recoveredAt',
]);

function plain(value) {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value)
    && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null));
}

function exact(value, keys) {
  return plain(value) && Object.keys(value).length === keys.size && Object.keys(value).every(key => keys.has(key));
}

function digest(value) { return typeof value === 'string' && /^[a-f0-9]{64}$/.test(value); }
function base(value) { return typeof value === 'string' && /^[a-f0-9]{40,64}$/.test(value); }
function timestamp(value) { return Number.isSafeInteger(value) && value >= 0; }

function ownerIdentity(value) {
  if (!exact(value, OWNER_KEYS) || !Number.isSafeInteger(value.pid) || value.pid <= 0
    || typeof value.started !== 'string' || !value.started || Buffer.byteLength(value.started) > 256
    || typeof value.nonce !== 'string' || !value.nonce || Buffer.byteLength(value.nonce) > 128
    || !Number.isSafeInteger(value.generation) || value.generation < 1) return null;
  return value;
}

function sameOwner(left, right) {
  return Boolean(left && right && left.pid === right.pid && left.started === right.started
    && left.nonce === right.nonce && left.generation === right.generation);
}

function parsedIntent(value) {
  if (!exact(value, INTENT_KEYS) || value.policy !== WORKSPACE_PREPARATION_INTENT_POLICY) return null;
  try { identifier(value.intentId, 'workspace intent id'); identifier(value.taskId, 'workspace task id'); identifier(value.attemptId, 'workspace attempt id'); }
  catch { return null; }
  if (!Number.isSafeInteger(value.executionSegment) || value.executionSegment < 1 || !KINDS.has(value.kind)
    || !base(value.base) || !digest(value.inputSha256) || typeof value.destination !== 'string'
    || !isAbsolute(value.destination) || !value.destination || Buffer.byteLength(value.destination) > 4096
    || value.destination.includes('\0') || !digest(value.inputRevision) || !digest(value.snapshotRevision)
    || !digest(value.authorityRevision) || !digest(value.modelProfileFingerprint)
    || value.owner !== null && !ownerIdentity(value.owner) || !timestamp(value.preparedAt)
    || value.materializationGeneration !== null && (!Number.isSafeInteger(value.materializationGeneration) || value.materializationGeneration < 0)) return null;
  return value;
}

function parsedExpected(value) {
  if (!exact(value, EXPECTED_KEYS) || !KINDS.has(value.kind) || !base(value.base) || !digest(value.inputSha256)
    || typeof value.destination !== 'string' || !isAbsolute(value.destination) || !value.destination
    || Buffer.byteLength(value.destination) > 4096 || value.destination.includes('\0')
    || !digest(value.inputRevision) || !digest(value.snapshotRevision) || !digest(value.authorityRevision)
    || !digest(value.modelProfileFingerprint)) return null;
  return value;
}

function parsedAudit(value) {
  if (!exact(value, AUDIT_KEYS) || value.policy !== WORKSPACE_PREPARATION_INTENT_POLICY
    || value.outcome !== 'cleared_prepared_without_materialization_start') return null;
  try { identifier(value.intentId, 'workspace audit intent id'); identifier(value.taskId, 'workspace audit task id'); identifier(value.attemptId, 'workspace audit attempt id'); }
  catch { return null; }
  if (!digest(value.inputSha256) || typeof value.destination !== 'string' || !isAbsolute(value.destination)
    || Buffer.byteLength(value.destination) > 4096 || !Number.isSafeInteger(value.preparedGeneration)
    || value.preparedGeneration < 1 || !Number.isSafeInteger(value.recoveryGeneration)
    || value.recoveryGeneration <= value.preparedGeneration || !timestamp(value.recoveredAt)) return null;
  return value;
}

export function workspaceInputSha256(input) {
  return hash(encodeJson(input, 128 * 1024));
}

export function createPreparedWorkspaceIntent({
  intentId, taskId, attemptId, executionSegment, kind, base: pinnedBase, inputSha256,
  destination, inputRevision, snapshotRevision, authorityRevision, modelProfileFingerprint,
  owner, preparedAt = Date.now(),
}) {
  const value = {
    policy: WORKSPACE_PREPARATION_INTENT_POLICY, intentId, taskId, attemptId, executionSegment,
    kind, base: pinnedBase, inputSha256, destination, inputRevision, snapshotRevision,
    authorityRevision, modelProfileFingerprint, owner: owner ?? null, preparedAt,
    materializationGeneration: null,
  };
  if (!parsedIntent(value)) throw fail('Invalid prepared workspace intent', 'STORAGE');
  return value;
}

export function markWorkspaceMaterializationStarted(value, owner) {
  const intent = parsedIntent(value), current = ownerIdentity(owner);
  if (!intent || intent.materializationGeneration !== null) throw fail('Workspace materialization intent is not exactly prepared', 'STORAGE');
  if (intent.owner && (!current || !sameOwner(intent.owner, current))) throw fail('Workspace materialization ownership changed before start', 'STALE');
  return { ...intent, materializationGeneration: current?.generation ?? 0 };
}

export function workspaceIntentState(value) {
  const intent = parsedIntent(value);
  if (!intent) return 'legacy_or_ambiguous';
  return intent.materializationGeneration === null ? 'prepared' : 'started';
}

export function hasPreparedWorkspaceRecoveryAudit(task) {
  const audit = Array.isArray(task?.workspaceRecoveryAudit) && task.workspaceRecoveryAudit.length === WORKSPACE_PREPARATION_RECOVERY_LIMIT
    ? parsedAudit(task.workspaceRecoveryAudit[0]) : null;
  return Boolean(audit && audit.taskId === task.id);
}

export async function preparedWorkspaceRecoveryDecision(task, {
  ownership, expected, recordedExecutorsStopped = false, cancellationClear = false,
  usageClear = false, effectsClear = false, authorityClear = false, inputClear = false,
  retryClear = false,
} = {}) {
  const intent = parsedIntent(task?.worktreeIntent), current = ownerIdentity(ownership), wanted = parsedExpected(expected);
  const refuse = reason => ({ recover: false, reason });
  if (!intent) return refuse('Workspace intent is legacy or its materialization marker is ambiguous');
  if (intent.materializationGeneration !== null) return refuse('Workspace materialization has a durable start marker');
  if (!intent.owner) return refuse('Prepared intent has no exclusive ownership generation');
  if (!current || current.pid !== process.pid) return refuse('Current exclusive ownership identity is unavailable');
  if (current.generation <= intent.owner.generation || current.nonce === intent.owner.nonce) return refuse('Current ownership generation is not newer and distinct');
  if (!wanted || Object.keys(wanted).some(key => wanted[key] !== intent[key])) return refuse('Pinned workspace input, authority, or destination changed');
  if (task.id !== intent.taskId || task.state !== 'running' || task.worktree || task.executionIntent?.id !== intent.attemptId
    || task.executionIntent?.segment !== intent.executionSegment || task.executionSegment !== intent.executionSegment) return refuse('Workspace task or execution attempt identity changed');
  if (task.workspaceRecoveryAudit !== undefined && (!Array.isArray(task.workspaceRecoveryAudit) || task.workspaceRecoveryAudit.length >= WORKSPACE_PREPARATION_RECOVERY_LIMIT)) return refuse('Prepared workspace recovery budget is exhausted or ambiguous');
  if (!recordedExecutorsStopped) return refuse('A recorded executor or guardian is not positively stopped');
  if (![cancellationClear, usageClear, effectsClear, authorityClear, inputClear, retryClear].every(value => value === true)) return refuse('A cancellation, usage, effect, authority, input, or retry fence is not clear');
  try {
    if (await processIdentity(current.pid) !== current.started) return refuse('Current ownership process identity is not live');
    if (await processIdentity(intent.owner.pid) === intent.owner.started) return refuse('Prior owning host is not positively stopped');
  } catch {
    return refuse('Owning process identity could not be verified');
  }
  return { recover: true, reason: 'Exact prepared intent has no materialization start marker', intent };
}

export function clearPreparedWorkspaceIntentWithAudit(task, decision, ownership, recoveredAt = Date.now()) {
  const intent = parsedIntent(task?.worktreeIntent), current = ownerIdentity(ownership);
  if (!decision?.recover || !intent || intent !== decision.intent || !current
    || current.generation <= intent.owner?.generation || intent.materializationGeneration !== null) throw fail('Prepared workspace recovery decision is stale', 'STALE');
  if (task.workspaceRecoveryAudit !== undefined && (!Array.isArray(task.workspaceRecoveryAudit) || task.workspaceRecoveryAudit.length >= WORKSPACE_PREPARATION_RECOVERY_LIMIT)) throw fail('Prepared workspace recovery budget is exhausted', 'UNCERTAIN');
  const audit = {
    policy: WORKSPACE_PREPARATION_INTENT_POLICY, intentId: intent.intentId, taskId: intent.taskId,
    attemptId: intent.attemptId, inputSha256: intent.inputSha256, destination: intent.destination,
    outcome: 'cleared_prepared_without_materialization_start', preparedGeneration: intent.owner.generation,
    recoveryGeneration: current.generation, recoveredAt,
  };
  if (!parsedAudit(audit)) throw fail('Invalid prepared workspace recovery audit', 'STORAGE');
  task.workspaceRecoveryAudit = [audit];
  delete task.worktreeIntent;
  return audit;
}
