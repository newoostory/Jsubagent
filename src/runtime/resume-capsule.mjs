import { LIMITS, encodeJson, fail, hash, identifier, short } from '../limits.mjs';
import { assertProviderFault, assertUsageState, emptyUsageState, providerFaultNextEligible } from './provider-fault.mjs';
import { effectRequiresReconciliation } from '../execution/failure-receipt.mjs';

const HOLD_KINDS = new Set(['provider', 'memory', 'connection', 'request', 'effect', 'input', 'authority', 'admission']);
const HOLD_SOURCES = new Set(['provider_fault', 'runtime', 'recovery', 'authority']);
const HOLD_KEYS = new Set(['id', 'kind', 'source', 'since', 'detail', 'nextEligibleAt']);
const CAPSULE_KEYS = new Set([
  'type', 'stage', 'identity', 'goal', 'modelProfileFingerprint', 'snapshotRevision', 'inputRevision',
  'authorityRevision', 'requestState', 'effectState', 'usageState', 'checkpoint', 'holdReasons',
  'nextEligibleAt', 'cancellationIntent', 'nextAction', 'providerFault', 'pausedAt',
]);
const IDENTITY_KEYS = new Set(['agentId', 'resultEpoch', 'goalRevision', 'executionSegment']);
const GOAL_KEYS = new Set(['sha256', 'revision']);
const REQUEST_KEYS = new Set(['id', 'outcome', 'observedAt']);
const EFFECT_KEYS = new Set(['uncertain', 'pending']);
const EFFECT_ENTRY_KEYS = new Set(['stepId', 'kind', 'outcome', 'inputDigest', 'intentArtifactId', 'resultArtifactId']);
const CHECKPOINT_KEYS = new Set(['sha256', 'refs']);
const REF_KEYS = new Set(['kind', 'artifactId', 'bytes', 'sha256', 'complete']);
const OUTCOMES = new Set(['rejected_before_execution', 'completed', 'unknown']);
const NEXT_ACTIONS = new Set(['explicit_resume', 'reconcile_then_resume', 'inspect_or_cancel']);

function plain(value) {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value) && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null));
}

function exact(value, keys, name) {
  if (!plain(value)) throw fail(`Invalid ${name}`, 'STORAGE');
  for (const key of Object.keys(value)) if (!keys.has(key)) throw fail(`Unsupported ${name} field: ${key}`, 'STORAGE');
  return value;
}

function unsigned(value, name, { positive = false } = {}) {
  if (!Number.isSafeInteger(value) || value < (positive ? 1 : 0)) throw fail(`Invalid ${name}`, 'STORAGE');
  return value;
}

function digest(value, name) {
  if (typeof value !== 'string' || !/^[a-f0-9]{64}$/.test(value)) throw fail(`Invalid ${name}`, 'STORAGE');
  return value;
}

export function holdReason({ id, kind, source, since = Date.now(), detail, nextEligibleAt }) {
  identifier(id, 'hold reason id');
  if (!HOLD_KINDS.has(kind) || !HOLD_SOURCES.has(source)) throw fail('Invalid hold reason kind/source', 'STORAGE');
  unsigned(since, 'hold reason since');
  if (typeof detail !== 'string' || !detail || Buffer.byteLength(detail) > 512 || /[\r\n]/.test(detail)) throw fail('Invalid hold reason detail', 'STORAGE');
  if (nextEligibleAt !== undefined) unsigned(nextEligibleAt, 'hold reason nextEligibleAt');
  return { id, kind, source, since, detail, ...(nextEligibleAt === undefined ? {} : { nextEligibleAt }) };
}

export function assertHoldReasons(value) {
  if (!Array.isArray(value) || value.length > 8) throw fail('Invalid hold reasons', 'STORAGE');
  const ids = new Set();
  const rows = value.map(row => {
    exact(row, HOLD_KEYS, 'hold reason');
    const normalized = holdReason(row);
    if (ids.has(normalized.id)) throw fail('Duplicate hold reason', 'STORAGE');
    ids.add(normalized.id);
    return normalized;
  });
  encodeJson(rows, 8192);
  return rows;
}

export function mergeHoldReasons(current = [], additions = []) {
  const rows = new Map(assertHoldReasons(current).map(row => [row.id, row]));
  for (const row of assertHoldReasons(additions)) rows.set(row.id, row);
  return assertHoldReasons([...rows.values()].sort((a, b) => a.id.localeCompare(b.id)));
}

export function clearHoldKinds(current = [], kinds = []) {
  const removed = new Set(kinds);
  return assertHoldReasons(current).filter(row => !removed.has(row.kind));
}

export function providerHold(fault, poolId, since = Date.now(), minimumEligibleAt) {
  const normalized = assertProviderFault(fault), nextEligibleAt = minimumEligibleAt ?? providerFaultNextEligible(normalized);
  return holdReason({
    id: `provider-${hash(poolId).slice(0, 24)}`, kind: 'provider', source: 'provider_fault', since,
    detail: normalized.safeMessage, ...(nextEligibleAt === null ? {} : { nextEligibleAt }),
  });
}

function stableHash(value, name) {
  try { return hash(encodeJson(value, LIMITS.checkpointBytes)); }
  catch (error) { throw fail(`Cannot fingerprint ${name}: ${error.message}`, 'STORAGE'); }
}

export function goalRevisionHash(task) {
  return stableHash({ task: task.task, amendment: task.goalAmendment ?? '', revision: task.goalRevision ?? 1 }, 'goal revision');
}

export function modelProfileFingerprint(task) {
  return stableHash({ profile: task.profile, checkpointBinding: task.checkpoint?.binding ?? null }, 'model/profile');
}

export function snapshotRevision(task) {
  return stableHash({ sourceCwd: task.sourceCwd, inputSnapshot: task.inputSnapshot ?? null, worktreeBase: task.worktree?.base ?? null }, 'snapshot');
}

export function inputRevision(task) {
  return stableHash({
    ...(task.qualityContract ? { qualityContract: task.qualityContract } : {}),
    workItem: task.workItem ? { id: task.workItem.id, inputRefs: task.workItem.inputRefs, evidenceIds: task.workItem.evidenceIds ?? [], predecessors: task.workItem.predecessors ?? [] } : null,
    dependsOn: task.dependsOn ?? [], dependencyPolicy: task.dependencyPolicy ?? 'all_succeeded', dependencyHandoff: task.dependencyHandoff ?? 'summary',
    inputSnapshot: task.inputSnapshot ?? null,
    ...(task.resultInputDeclarations?.length ? { resultInputDeclarations: task.resultInputDeclarations } : {}),
    ...(task.resultBindings?.length ? { resultBindings: task.resultBindings } : {}),
    mailbox: (task.mailbox ?? []).map(row => row.id), delivering: (task.delivering ?? []).map(row => row.id), seen: (task.seen ?? []).map(row => row.id),
  }, 'input');
}

export function authorityRevision(task) {
  return stableHash({
    role: { name: task.role?.name, hash: task.role?.hash, workspace: task.role?.workspace, tools: task.role?.tools, requiredMode: task.role?.requiredMode },
    profile: { name: task.profile?.name, provider: task.profile?.provider, model: task.profile?.model, thinkingLevel: task.profile?.thinkingLevel, requiredMode: task.profile?.requiredMode },
    modeBindings: task.modeBindings ?? [], effectGrantHash: task.effectGrantHash ?? null, requestHash: task.requestHash,
  }, 'authority');
}

function normalizedRef(kind, value) {
  if (!value?.artifactId) return null;
  const row = { kind, artifactId: identifier(value.artifactId, 'capsule artifactId') };
  if (value.bytes !== undefined) row.bytes = unsigned(value.bytes, 'capsule ref bytes');
  if (value.sha256 !== undefined) row.sha256 = digest(value.sha256, 'capsule ref digest');
  if (value.complete !== undefined) {
    if (value.complete !== true) throw fail('Capsule cannot reference an incomplete artifact', 'STORAGE');
    row.complete = true;
  }
  return row;
}

function checkpointReferences(task) {
  const checkpoint = task.checkpoint ?? {};
  const refs = [];
  const append = row => { if (row && !refs.some(existing => existing.artifactId === row.artifactId && existing.kind === row.kind) && refs.length < 24) refs.push(row); };
  for (const record of (checkpoint.records ?? []).slice(-8)) append(normalizedRef('checkpoint_record', record.ref));
  append(normalizedRef('checkpoint_history', checkpoint.historyRef));
  append(normalizedRef('continuity_state', checkpoint.continuityRef));
  append(normalizedRef('compaction_envelope', checkpoint.compactionEnvelope?.ref));
  for (const section of (checkpoint.resultSections?.sections ?? []).slice(-8)) append(normalizedRef('result_section', section.ref));
  append(normalizedRef('canonical_result', task.resultRef));
  if (checkpoint.transcriptArtifactId) append({ kind: 'checkpoint_transcript', artifactId: identifier(checkpoint.transcriptArtifactId, 'capsule transcript artifactId') });
  if (task.transcript && task.transcript !== checkpoint.transcriptArtifactId) append({ kind: 'transcript', artifactId: identifier(task.transcript, 'capsule transcript artifactId') });
  if (task.latestReport) append({ kind: 'latest_report', artifactId: identifier(task.latestReport, 'capsule report artifactId') });
  return refs;
}

function requestState(task, fallbackOutcome = 'unknown', now = Date.now()) {
  const receipt = task.lastRequestReceipt ?? task.executionIntent?.requestReceipt;
  const outcome = OUTCOMES.has(receipt?.outcome) ? receipt.outcome : fallbackOutcome;
  return {
    ...(receipt?.id ? { id: identifier(receipt.id, 'request receipt id') } : {}),
    outcome,
    observedAt: unsigned(receipt?.observedAt ?? now, 'request receipt observedAt'),
  };
}

function effectState(task) {
  const pending = (task.pendingEffects ?? []).filter(effectRequiresReconciliation).slice(0, 8).map(step => ({
    stepId: identifier(step.stepId, 'effect stepId'), kind: short(step.kind ?? 'unknown', 64), outcome: short(step.outcome ?? 'unknown', 64), inputDigest: digest(step.inputDigest, 'effect input digest'),
    ...(step.intentRef ? { intentArtifactId: identifier(step.intentRef, 'effect intent artifactId') } : {}),
    ...(step.resultRef?.artifactId ? { resultArtifactId: identifier(step.resultRef.artifactId, 'effect result artifactId') } : {}),
  }));
  return { uncertain: pending.length > 0, pending };
}

// The legacy request-outcome hold was usage-only, not an effect/permit fence.
export function continuationHoldReasons(reasons = []) {
  return assertHoldReasons(reasons).filter(row => !(row.id === 'request-outcome' && row.kind === 'request' && row.source === 'recovery'));
}

export function resumeNextAction(task, fault = task.providerFault) {
  return task.worktreeIntent || task.requestPermitUncertain || effectState(task).uncertain ? 'reconcile_then_resume'
    : ['quota_unknown', 'billing_required', 'auth_required'].includes(fault?.kind) ? 'inspect_or_cancel' : 'explicit_resume';
}

export function createResumeCapsule(task, { fault, holdReasons = task.holdReasons ?? [], requestOutcome = 'unknown', now = Date.now() } = {}) {
  const reasons = continuationHoldReasons(holdReasons);
  const nextTimes = reasons.map(row => row.nextEligibleAt).filter(Number.isSafeInteger);
  const capsule = {
    type: 'usage-resume-capsule', stage: 'paused',
    identity: {
      agentId: identifier(task.id), resultEpoch: unsigned(task.resultEpoch ?? 1, 'result epoch', { positive: true }),
      goalRevision: unsigned(task.goalRevision ?? 1, 'goal revision', { positive: true }), executionSegment: unsigned(task.executionSegment ?? 0, 'execution segment'),
    },
    goal: { sha256: goalRevisionHash(task), revision: unsigned(task.goalRevision ?? 1, 'goal revision', { positive: true }) },
    modelProfileFingerprint: modelProfileFingerprint(task), snapshotRevision: snapshotRevision(task), inputRevision: inputRevision(task), authorityRevision: authorityRevision(task),
    requestState: requestState(task, requestOutcome, now), effectState: effectState(task), usageState: assertUsageState(task.lastUsageState ?? emptyUsageState(now)),
    checkpoint: { sha256: stableHash(task.checkpoint ?? {}, 'checkpoint'), refs: checkpointReferences(task) },
    holdReasons: reasons, nextEligibleAt: nextTimes.length ? Math.max(...nextTimes) : null,
    cancellationIntent: Boolean(task.cancelRequested || task.authorityRevoked),
    nextAction: resumeNextAction(task, fault),
    ...(fault ? { providerFault: assertProviderFault(fault) } : {}), pausedAt: unsigned(now, 'pause time'),
  };
  return assertResumeCapsule(capsule);
}

export function assertResumeCapsule(value) {
  exact(value, CAPSULE_KEYS, 'resume capsule');
  if (value.type !== 'usage-resume-capsule' || value.stage !== 'paused') throw fail('Unsupported resume capsule', 'STORAGE');
  exact(value.identity, IDENTITY_KEYS, 'resume capsule identity'); identifier(value.identity.agentId);
  unsigned(value.identity.resultEpoch, 'result epoch', { positive: true }); unsigned(value.identity.goalRevision, 'goal revision', { positive: true }); unsigned(value.identity.executionSegment, 'execution segment');
  exact(value.goal, GOAL_KEYS, 'resume capsule goal'); digest(value.goal.sha256, 'goal digest'); unsigned(value.goal.revision, 'goal revision', { positive: true });
  for (const [field, name] of [['modelProfileFingerprint', 'model/profile fingerprint'], ['snapshotRevision', 'snapshot revision'], ['inputRevision', 'input revision'], ['authorityRevision', 'authority revision']]) digest(value[field], name);
  exact(value.requestState, REQUEST_KEYS, 'resume capsule request state'); if (value.requestState.id !== undefined) identifier(value.requestState.id); if (!OUTCOMES.has(value.requestState.outcome)) throw fail('Invalid capsule request outcome', 'STORAGE'); unsigned(value.requestState.observedAt, 'request observedAt');
  exact(value.effectState, EFFECT_KEYS, 'resume capsule effect state');
  if (typeof value.effectState.uncertain !== 'boolean' || !Array.isArray(value.effectState.pending) || value.effectState.pending.length > 8) throw fail('Invalid capsule effect state', 'STORAGE');
  for (const entry of value.effectState.pending) { exact(entry, EFFECT_ENTRY_KEYS, 'capsule effect'); identifier(entry.stepId); digest(entry.inputDigest, 'effect input digest'); if (typeof entry.kind !== 'string' || typeof entry.outcome !== 'string') throw fail('Invalid capsule effect state', 'STORAGE'); if (entry.intentArtifactId) identifier(entry.intentArtifactId); if (entry.resultArtifactId) identifier(entry.resultArtifactId); }
  assertUsageState(value.usageState);
  exact(value.checkpoint, CHECKPOINT_KEYS, 'resume capsule checkpoint'); digest(value.checkpoint.sha256, 'checkpoint digest');
  if (!Array.isArray(value.checkpoint.refs) || value.checkpoint.refs.length > 24) throw fail('Invalid capsule checkpoint refs', 'STORAGE');
  for (const ref of value.checkpoint.refs) { exact(ref, REF_KEYS, 'capsule ref'); if (typeof ref.kind !== 'string' || Buffer.byteLength(ref.kind) > 64) throw fail('Invalid capsule ref kind', 'STORAGE'); identifier(ref.artifactId); if (ref.bytes !== undefined) unsigned(ref.bytes, 'capsule ref bytes'); if (ref.sha256 !== undefined) digest(ref.sha256, 'capsule ref digest'); if (ref.complete !== undefined && ref.complete !== true) throw fail('Invalid capsule ref completeness', 'STORAGE'); }
  assertHoldReasons(value.holdReasons);
  if (value.nextEligibleAt !== null) unsigned(value.nextEligibleAt, 'capsule nextEligibleAt');
  if (typeof value.cancellationIntent !== 'boolean' || !NEXT_ACTIONS.has(value.nextAction)) throw fail('Invalid capsule action/cancellation', 'STORAGE');
  if (value.providerFault !== undefined) assertProviderFault(value.providerFault);
  unsigned(value.pausedAt, 'capsule pausedAt');
  encodeJson(value, 32 * 1024);
  return JSON.parse(JSON.stringify(value));
}

export function assertCapsuleBinding(task, capsule) {
  const value = assertResumeCapsule(capsule);
  if (value.identity.agentId !== task.id || value.identity.resultEpoch !== (task.resultEpoch ?? 1) || value.identity.goalRevision !== (task.goalRevision ?? 1)
    || value.goal.sha256 !== goalRevisionHash(task) || value.modelProfileFingerprint !== modelProfileFingerprint(task)
    || value.snapshotRevision !== snapshotRevision(task) || value.inputRevision !== inputRevision(task) || value.authorityRevision !== authorityRevision(task)
    || value.checkpoint.sha256 !== stableHash(task.checkpoint ?? {}, 'checkpoint')) {
    throw fail('Resume capsule binding changed; inspect pinned goal/input/model/authority/checkpoint before a new epoch', 'STALE');
  }
  return value;
}
