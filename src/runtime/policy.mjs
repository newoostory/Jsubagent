import { effectRequiresReconciliation } from '../execution/failure-receipt.mjs';
import { fail } from '../limits.mjs';

function amount(value) { return Number.isFinite(value) && value >= 0 ? value : 0; }

// Node clamps oversized timeout delays to 1ms. Recheck the actual deadline
// between supported chunks instead of expiring a long opt-in budget early.
export function createExecutionBudgetTimer(remainingMs, abort, { now = Date.now, setTimer = setTimeout, clearTimer = clearTimeout } = {}) {
  if (remainingMs === null) return undefined;
  if (!Number.isSafeInteger(remainingMs) || remainingMs < 1) throw fail('Invalid execution-time budget timer', 'STORAGE');
  const deadline = now() + remainingMs; let timer, cancelled = false;
  const arm = () => {
    if (cancelled) return;
    const remaining = deadline - now();
    if (remaining <= 0) { cancelled = true; abort(fail('Root execution-time budget exhausted', 'ROOT_BUDGET')); return; }
    timer = setTimer(arm, Math.min(remaining, 2 ** 31 - 1)); timer?.unref?.();
  };
  arm();
  return { cancel() { cancelled = true; if (timer !== undefined) clearTimer(timer); } };
}

export function createRootBudget(limits) {
  return {
    maxTurns: limits.rootTurns, maxExecutionMs: limits.rootTimeMs,
    maxStagnantSegments: limits.rootStagnantSegments, executionMs: 0, segments: 0, stagnantSegments: 0,
  };
}

export function rootProgressMarker(task) {
  if (task.lastEffectProgress) return `effect:${task.lastEffectProgress}`;
  if (task.handoff) return `handoff:${task.handoff}`;
  const records = task.checkpoint?.records;
  if (Array.isArray(records) && records.length) {
    const last = records.at(-1);
    return `checkpoint:${last.contentSha256 ?? ''}:${last.contentBytes ?? ''}:${last.messageCount ?? ''}`;
  }
  return '';
}

export function settleRootSegment(task, { intentId, startedAt, finishedAt = Date.now(), progress = rootProgressMarker(task), trackProgress = true } = {}) {
  const budget = task.rootBudget;
  if (!budget || !intentId || budget.lastSettledIntentId === intentId) return false;
  const start = Number.isSafeInteger(startedAt) && startedAt >= 0 ? startedAt : finishedAt;
  budget.executionMs = amount(budget.executionMs) + Math.max(0, finishedAt - start);
  budget.segments = amount(budget.segments) + 1;
  budget.lastSettledIntentId = intentId;
  if (trackProgress) {
    if (budget.lastProgress === undefined) budget.stagnantSegments = 0;
    else budget.stagnantSegments = progress === budget.lastProgress ? amount(budget.stagnantSegments) + 1 : 0;
    budget.lastProgress = progress;
  }
  return true;
}

export function rootBudgetDecision(task, now = Date.now()) {
  const budget = task.rootBudget;
  if (!budget) return { allowed: false, reason: 'Root budget snapshot is missing; explicit recovery is required' };
  // Only explicit null disables a cumulative cap. Missing/corrupt durable fields
  // must not silently turn a historical finite budget into unlimited authority.
  const cap = value => value === null || Number.isSafeInteger(value) && value > 0;
  if (!cap(budget.maxTurns) || !cap(budget.maxExecutionMs) || !Number.isSafeInteger(budget.maxStagnantSegments) || budget.maxStagnantSegments < 1)
    return { allowed: false, reason: 'Root budget snapshot is invalid; explicit recovery is required' };
  if (budget.maxTurns !== null && (task.turns ?? 0) >= budget.maxTurns) return { allowed: false, reason: `Root turn budget exhausted (${task.turns ?? 0}/${budget.maxTurns}); usage is retained` };
  const liveMs = task.executionIntent && budget.lastSettledIntentId !== task.executionIntent.id ? Math.max(0, now - task.executionIntent.startedAt) : 0;
  if (budget.maxExecutionMs !== null && amount(budget.executionMs) + liveMs >= budget.maxExecutionMs) return { allowed: false, reason: `Root execution-time budget exhausted (${amount(budget.executionMs) + liveMs}/${budget.maxExecutionMs}ms); usage is retained` };
  if (amount(budget.stagnantSegments) >= budget.maxStagnantSegments) return { allowed: false, reason: `Root progress budget exhausted after ${budget.stagnantSegments} stagnant segments` };
  return { allowed: true, remainingMs: budget.maxExecutionMs === null ? null : Math.max(1, budget.maxExecutionMs - amount(budget.executionMs) - liveMs) };
}

export function continuationDecision(task, output) {
  const kind = output.continuation?.kind;
  if (!['slice', 'connection', 'worker_memory', 'system_memory'].includes(kind)) return { continue: false };
  if (task.cancelRequested || task.authorityRevoked) return { continue: false, reason: 'Authority cancelled or revoked' };
  const budget = rootBudgetDecision(task);
  if (!budget.allowed) return { continue: false, reason: budget.reason };
  if ((task.pendingEffects ?? []).some(effectRequiresReconciliation)) return { continue: false, reason: 'Uncertain tool effect requires reconciliation' };
  if (['worker_memory', 'system_memory'].includes(kind)) {
    if (task.requestPermitUncertain || task.executionIntent?.usageOperation || !task.checkpoint) return { continue: false, reason: 'Memory maintenance requires a confirmed checkpoint and no active compaction/permit fence' };
    const progress = rootProgressMarker(task);
    if (task.memoryMaintenance?.kind === kind && task.memoryMaintenance.progress === progress) return { continue: false, reason: 'Repeated memory maintenance without progress; change the input window before explicit recovery' };
  }
  return { continue: true, kind };
}
