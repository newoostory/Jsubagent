export const HOST_IDLE_TIMEOUT_MS = 120_000;
export const HOST_STARTUP_TIMEOUT_MS = 60_000;

// A disconnected run can still own approved background work or executors.
// Historical results and foreground work held for reconnection live on disk.
export function hostHasWork(engine) {
  if (!engine) return false;
  for (const key of ['running', 'recoveringExecutors', 'reconciling', 'settlingInputs', 'autoOverloadResumePending']) {
    if (engine[key]?.size) return true;
  }
  if (engine.runner?.pendingCleanup?.size) return true;
  for (const worker of engine.runner?.workers ?? []) if (worker.busy) return true;
  const metadata = typeof engine.tasks.rows === 'function';
  const rows = metadata ? engine.tasks.rows() : engine.tasks.values();
  for (const row of rows) {
    // Avoid loading historical conversation records on every health tick.
    if (row.profile?.allowBackground !== true || !['queued', 'running', 'paused'].includes(row.state)) continue;
    const task = metadata ? engine.tasks.get(row.id) : row;
    if (!task) return true; // Missing state is not proof that work stopped.
    if (task.profile?.allowBackground !== true || task.cancelRequested || task.authorityRevoked) continue;
    if (task.state === 'queued' || task.state === 'running') return true;
    const fault = task.resumeCapsule?.providerFault;
    if (task.state === 'paused' && fault?.kind === 'provider_overload' && fault.requestOutcome === 'rejected_before_execution') return true;
  }
  return false;
}

export class HostIdleGuard {
  constructor({ idleMs = HOST_IDLE_TIMEOUT_MS, startupMs = HOST_STARTUP_TIMEOUT_MS, now = Date.now() } = {}) {
    for (const value of [idleMs, startupMs]) if (!Number.isSafeInteger(value) || value < 1) throw new RangeError('Invalid host lifetime timeout');
    this.idleMs = idleMs; this.startupMs = startupMs; this.startedAt = now;
  }
  observe({ connected, initialized, busy, initializing, ending }, now = Date.now()) {
    if (ending || connected || busy || initializing) { this.idleSince = undefined; return null; }
    if (!initialized) return now - this.startedAt >= this.startupMs ? 'startup_timeout' : null;
    this.idleSince ??= now;
    return now - this.idleSince >= this.idleMs ? 'idle_timeout' : null;
  }
}
