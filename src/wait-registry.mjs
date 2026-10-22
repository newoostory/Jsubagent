import { identifier, fail } from './limits.mjs';
import { needsWaitAttention } from './wait-attention.mjs';

const MAX_WAITERS = 16;
const MAX_TIMEOUT_MS = 120_000;

function validateRequest(engine, input) {
  const waitId = identifier(input?.waitId, 'waitId');
  if (!Array.isArray(input?.ids) || input.ids.length < 1 || input.ids.length > 8) throw fail('wait ids: expected 1..8 task IDs');
  const ids = input.ids.map((id, index) => identifier(id, `wait ids[${index}]`));
  if (new Set(ids).size !== ids.length) throw fail('wait ids: expected unique task IDs');
  for (const id of ids) if (!engine.tasks.has(id)) throw fail(`Unknown task ${id}`, 'NOT_FOUND');
  const timeoutMs = input.timeoutMs;
  if (timeoutMs !== undefined && (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0 || timeoutMs > MAX_TIMEOUT_MS)) throw fail('wait timeoutMs: expected integer 0..120000');
  if (input.deadline !== undefined && (!Number.isSafeInteger(input.deadline) || input.deadline < 0)) throw fail('wait deadline: expected timestamp');
  const deadlineRemaining = input.deadline === undefined ? Infinity : Math.max(0, input.deadline - Date.now());
  const effectiveTimeoutMs = timeoutMs === undefined && input.deadline === undefined ? undefined : Math.min(timeoutMs ?? MAX_TIMEOUT_MS, deadlineRemaining, MAX_TIMEOUT_MS);
  const mode = input.mode ?? 'all';
  if (!['all', 'any'].includes(mode)) throw fail('wait mode: expected all or any');
  if (input.wakeOnMessage !== undefined && typeof input.wakeOnMessage !== 'boolean') throw fail('wait wakeOnMessage: expected boolean');
  if (input.includeMetadata !== undefined && typeof input.includeMetadata !== 'boolean') throw fail('wait includeMetadata: expected boolean');
  if (!Array.isArray(input.seenMessages ?? []) || (input.seenMessages ?? []).length > 1024) throw fail('wait seenMessages: expected up to 1024 artifact IDs');
  const seenMessages = (input.seenMessages ?? []).map((id, index) => identifier(id, `wait seenMessages[${index}]`));
  return { waitId, ids, timeoutMs: effectiveTimeoutMs, mode, wakeOnMessage: input.wakeOnMessage === true, includeMetadata: input.includeMetadata === true, seen: new Set(seenMessages) };
}

export class WaitRegistry {
  constructor(engine) { this.engine = engine; this.waiters = new Map(); this.closed = false; }
  wait(input) {
    if (this.closed) return Promise.reject(fail('Wait registry closed', 'CLOSED'));
    let request;
    try { request = validateRequest(this.engine, input); }
    catch (error) { return Promise.reject(error); }
    if (this.waiters.has(request.waitId)) return Promise.reject(fail('waitId already active', 'CONFLICT'));
    if (this.waiters.size >= MAX_WAITERS) return Promise.reject(fail('Too many pending waits', 'BUSY'));
    return new Promise((resolve, reject) => {
      const waiter = { ...request, resolve, reject, timer: null, unsubscribe: null, done: false, checking: false, checkAgain: false, rows: undefined };
      const finish = (fn, value) => {
        if (waiter.done) return;
        waiter.done = true; clearTimeout(waiter.timer); waiter.unsubscribe?.(); this.waiters.delete(waiter.waitId); fn(value);
      };
      const result = (reason, conditionMet) => waiter.includeMetadata
        ? { rows: waiter.rows ?? [], reason, conditionMet, snapshotAt: waiter.snapshotAt ?? Date.now(), stale: false }
        : (waiter.rows ?? []);
      const check = () => {
        if (waiter.done) return;
        if (waiter.checking) { waiter.checkAgain = true; return; }
        waiter.checking = true;
        try {
          do {
            waiter.checkAgain = false;
            const rows = [];
            for (const id of waiter.ids) {
              const task = this.engine.tasks.get(id);
              if (!task) throw fail(`Unknown task ${id}`, 'NOT_FOUND');
              rows.push(this.engine.summary(task));
            }
            waiter.rows = rows;
            waiter.snapshotAt = Date.now();
            const settled = waiter.mode === 'all' ? rows.every(row => row.settled === true) : rows.some(row => row.settled === true);
            const message = waiter.wakeOnMessage && rows.some(row => needsWaitAttention(row, waiter.seen));
            if (message) { finish(resolve, result('attention', settled)); return; }
            if (settled) { finish(resolve, result('condition', true)); return; }
          } while (waiter.checkAgain && !waiter.done);
        } catch (error) { finish(reject, error); }
        finally { waiter.checking = false; }
      };
      // Subscribe before the first snapshot so a mutation cannot land between them.
      waiter.unsubscribe = this.engine.subscribeChanges(check);
      this.waiters.set(waiter.waitId, waiter);
      check();
      if (!waiter.done && request.timeoutMs !== undefined) waiter.timer = setTimeout(() => {
        check();
        if (!waiter.done) finish(resolve, result('timeout', false));
      }, request.timeoutMs);
    });
  }
  cancel(waitId) {
    identifier(waitId, 'waitId');
    const waiter = this.waiters.get(waitId);
    if (!waiter) return { waitId, cancelled: false };
    waiter.done = true; clearTimeout(waiter.timer); waiter.unsubscribe?.(); this.waiters.delete(waitId);
    waiter.reject(fail('Wait cancelled', 'CANCELLED'));
    return { waitId, cancelled: true };
  }
  close() {
    if (this.closed) return;
    this.closed = true;
    for (const waiter of [...this.waiters.values()]) {
      waiter.done = true; clearTimeout(waiter.timer); waiter.unsubscribe?.(); this.waiters.delete(waiter.waitId);
      waiter.reject(fail('Host shutting down', 'CLOSED'));
    }
  }
}
