import { providerFailureDecision } from './provider-fault.mjs';
import { effectRequiresReconciliation } from '../execution/failure-receipt.mjs';
import { fail } from '../limits.mjs';

export { providerFailureDecision };

export const USAGE_ONLY_TRANSPORT_RETRY_POLICY = 'future-model-transport-usage-only-v1';
const RETRY_STATES = new Set(['eligible', 'pending', 'active', 'exhausted']);
const RETRY_KEYS = new Set(['policy', 'acceptedUsageMismatch', 'retries', 'state', 'interruptions']);
const INTERRUPTION_KEYS = new Set(['requestId', 'observedAt', 'usageRisk', 'checkpointSha256', 'diagnosticType', 'transcriptArtifactId']);

function exact(value, keys, name) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).some(key => !keys.has(key))) throw fail(`Invalid ${name}`, 'STORAGE');
  return value;
}

export function createUsageOnlyTransportRetryPolicy() {
  return { policy: USAGE_ONLY_TRANSPORT_RETRY_POLICY, acceptedUsageMismatch: true, retries: 0, state: 'eligible', interruptions: [] };
}

export function assertUsageOnlyTransportRetryPolicy(value) {
  exact(value, RETRY_KEYS, 'usage-only transport retry policy');
  if (value.policy !== USAGE_ONLY_TRANSPORT_RETRY_POLICY || value.acceptedUsageMismatch !== true || !Number.isSafeInteger(value.retries) || value.retries < 0 || value.retries > 1 || !RETRY_STATES.has(value.state) || !Array.isArray(value.interruptions) || value.interruptions.length > 2) throw fail('Invalid usage-only transport retry policy', 'STORAGE');
  for (const row of value.interruptions) {
    exact(row, INTERRUPTION_KEYS, 'usage-only transport interruption');
    if (typeof row.requestId !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9_.:/-]{0,255}$/.test(row.requestId) || !Number.isSafeInteger(row.observedAt) || row.observedAt < 0 || row.usageRisk !== 'unknown' || typeof row.checkpointSha256 !== 'string' || !/^[a-f0-9]{64}$/.test(row.checkpointSha256) || row.diagnosticType !== 'provider_transport_failure') throw fail('Invalid usage-only transport interruption audit', 'STORAGE');
    if (row.transcriptArtifactId !== undefined && (typeof row.transcriptArtifactId !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9_.:/-]{0,255}$/.test(row.transcriptArtifactId))) throw fail('Invalid usage-only transport transcript audit reference', 'STORAGE');
  }
  if (value.interruptions.length < value.retries || value.state === 'eligible' && (value.retries !== 0 || value.interruptions.length) || ['pending', 'active', 'exhausted'].includes(value.state) && value.retries !== 1) throw fail('Inconsistent usage-only transport retry policy', 'STORAGE');
  return value;
}

export function usageOnlyTransportRetryDecision(task, evidence = {}) {
  let policy;
  try { policy = task?.usageOnlyTransportRetry === undefined ? createUsageOnlyTransportRetryPolicy() : assertUsageOnlyTransportRetryPolicy(task.usageOnlyTransportRetry); } catch { return { retry: false, reason: 'Invalid transport retry accounting' }; }
  if (policy.state !== 'eligible' || policy.retries !== 0) return { retry: false, reason: 'Usage-only transport retry budget is exhausted or already active' };
  if (evidence.fault?.kind !== 'transport_interruption' || evidence.fault?.source !== 'sdk_diagnostic' || evidence.fault?.diagnosticType !== 'provider_transport_failure' || evidence.fault?.transportPhase !== 'after_message_stream_start' || evidence.fault?.requestOutcome !== 'unknown') return { retry: false, reason: 'Trustworthy after-request SDK transport diagnostic is missing' };
  const required = [
    ['executorSettled', 'Prior worker execution is not fully settled'],
    ['checkpointConsistent', 'Last confirmed checkpoint is missing or inconsistent'],
    ['effectsSettled', 'A tool, worktree, web, or publication effect is not fully settled'],
    ['modelRequest', 'The interrupted request was not the ordinary model turn'],
    ['usageOnly', 'Uncertainty is not limited to model request usage'],
    ['quotaClear', 'Provider quota or recovery permit is held'],
    ['cancellationClear', 'Cancellation or authority revocation is durable'],
    ['memoryClear', 'Memory admission no longer permits execution'],
    ['admissionClear', 'Runtime admission or foreground connectivity is held'],
    ['authorityConsistent', 'Model or execution authority drifted'],
    ['inputConsistent', 'Pinned input or dependency state drifted'],
    ['budgetAvailable', 'Cumulative root turn/time budget is exhausted'],
    ['currentExecution', 'Execution fence is no longer current'],
  ];
  const blocked = required.find(([key]) => evidence[key] !== true);
  return blocked ? { retry: false, reason: blocked[1] } : { retry: true, reason: 'One fresh model request is allowed from the exact last confirmed checkpoint; prior request usage remains unknown' };
}

export function automaticRecoverySafe(task) {
  if (task.worktreeIntent) return false;
  // Missing usage is accepted for every task, including historical records.
  // Permit/storage uncertainty is not usage uncertainty and stays fenced.
  if (task.requestPermitUncertain) return false;
  return !(task.pendingEffects ?? []).some(effectRequiresReconciliation);
}

// Decisions are conservative: an expired lease alone never proves an executor
// dead. A caller may set executorStopped only after process/generation fencing.
export function recoveryDecision(task, { executorStopped = false } = {}) {
  if (task.cancelRequested || task.state === 'cancelled' || task.authorityRevoked) return { state: 'cancelled', retry: false, reason: 'Cancellation or authority revocation is durable' };
  if (task.state === 'queued' && !task.executionIntent) return { state: 'queued', retry: true, reason: 'Approved work has not started' };
  if (!['running', 'queued'].includes(task.state)) return { state: task.state, retry: false };
  if (!executorStopped) return { state: 'interrupted', retry: false, reason: 'Executor termination is not confirmed' };
  if (!automaticRecoverySafe(task)) return { state: 'interrupted', retry: false, reason: task.worktreeIntent ? 'Workspace creation needs reconciliation' : 'Uncheckpointed effect requires receipt/snapshot reconciliation; automatic replay refused' };
  if ((task.recoveryAttempts ?? 0) >= 2) return { state: 'paused', retry: false, reason: 'Recovery retry budget exhausted' };
  return { state: 'queued', retry: true, reason: 'Confirmed stopped executor; checkpointed tool results and exact known-failure receipts can be reused without re-executing their old calls' };
}
