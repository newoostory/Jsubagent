import { validatePublicAgentParameters } from './public-api.mjs';
import { fail, identifier, safeError, short } from './limits.mjs';

const MAX_BATCHES = 8;
const UNKNOWN_CODES = new Set(['HOST', 'HOST_EXIT', 'STORAGE', 'TIMEOUT', 'CLOSED', 'UNKNOWN']);

function errorInfo(error) {
  const rawCode = typeof error?.code === 'string' && error.code ? error.code : 'UNKNOWN';
  return { code: short(rawCode, 64), message: safeError(error) };
}

function failureRow(index, id, error, fallback = 'rejected') {
  const details = errorInfo(error);
  const code = details.code;
  const outcome = UNKNOWN_CODES.has(code) ? 'unknown' : fallback;
  return { index, id, ok: false, outcome, error: details };
}

function stoppedRow(index, id) {
  return { index, id, ok: false, outcome: 'not_started', error: { code: 'CANCELLED', message: 'Batch admission stopped before this task started' } };
}

function dependencyRow(index, id, blockedBy) {
  const bounded = blockedBy.slice(0, 8);
  return { index, id, ok: false, outcome: 'not_started', blockedBy: bounded,
    error: { code: 'DEPENDENCY', message: short(`Batch dependency was not admitted: ${bounded.join(', ')}`, 512) } };
}

function invoke(operation) {
  try { return Promise.resolve(operation()); }
  catch (error) { return Promise.reject(error); }
}

function validateSpawn(batchId, tasks) {
  identifier(batchId, 'batchId');
  if (!Array.isArray(tasks) || tasks.length < 1 || tasks.length > 8) throw fail('Batch tasks: expected 1..8 items');
  const ids = new Set();
  const publicTasks = tasks.map((row) => {
    if (!row || typeof row !== 'object' || Array.isArray(row)) throw fail('Invalid batch task');
    const { id, ...params } = row;
    identifier(id, 'task id');
    if (ids.has(id)) throw fail('Duplicate batch task id', 'CONFLICT');
    ids.add(id);
    return params;
  });
  // The public task payload, excluding host-generated operation IDs, owns the
  // 64 KiB input budget. Canonical validation also performs UTF-8 checks.
  validatePublicAgentParameters({ action: 'spawn', tasks: publicTasks });
  return tasks.map((row, index) => ({ index, id: row.id, params: { ...publicTasks[index] } }));
}

export class BatchRunner {
  constructor(engine) {
    if (!engine || typeof engine.spawn !== 'function' || typeof engine.cancel !== 'function') throw fail('Invalid batch engine');
    this.engine = engine;
    this.active = new Map();
    this.closed = false;
  }

  async spawn({ batchId, tasks, mainModel }) {
    const rows = validateSpawn(batchId, tasks);
    if (this.closed) return Promise.reject(fail('Batch runner closed', 'CLOSED'));
    if (this.active.has(batchId)) return Promise.reject(fail(`Batch already active: ${batchId}`, 'CONFLICT'));
    if (this.active.size >= MAX_BATCHES) return Promise.reject(fail('Batch runner busy', 'BUSY'));

    const state = { stopped: false, accepted: new Set(), failedAdmissions: new Set(), compensations: new Map(), inFlight: null, ids: rows.map(row => row.id) };
    this.active.set(batchId, state);
    const prepared = this.engine.prepareResultBatch
      ? invoke(() => this.engine.prepareResultBatch(batchId, rows, mainModel)).then(() => this.run(rows, state, mainModel))
      : this.run(rows, state, mainModel);
    const run = prepared
      .then(results => ({ results }))
      .finally(() => this.active.delete(batchId));
    state.promise = run;
    return run;
  }

  async run(rows, state, mainModel) {
    const results = [];
    for (const row of rows) {
      if (state.stopped || this.closed) {
        results.push(stoppedRow(row.index, row.id));
        state.failedAdmissions.add(row.id);
        await this.engine.settleResultSlot?.(row.id, 'not_started');
        continue;
      }
      const blockedBy = (row.params.dependsOn ?? []).filter(id => state.failedAdmissions.has(id));
      if (blockedBy.length) {
        results.push(dependencyRow(row.index, row.id, blockedBy));
        state.failedAdmissions.add(row.id);
        await this.engine.settleResultSlot?.(row.id, 'not_started');
        continue;
      }
      try {
        const pending = this.engine.spawn({ ...row.params, id: row.id, mainModel });
        state.inFlight = row.id;
        const result = await pending;
        state.accepted.add(row.id);
        await this.engine.settleResultSlot?.(row.id, 'accepted');
        const accepted = { index: row.index, id: row.id, ok: true, result };
        if (state.stopped || this.closed) {
          accepted.cancellationRequested = true;
          try {
            const cancellationResult = await this.engine.cancel(row.id);
            state.compensations.set(row.id, { ok: true, result: cancellationResult });
          }
          catch (error) {
            accepted.cancellationError = errorInfo(error);
            accepted.cancellationOutcome = UNKNOWN_CODES.has(accepted.cancellationError.code) ? 'unknown' : 'rejected';
            state.compensations.set(row.id, { ok: false, outcome: accepted.cancellationOutcome, error: accepted.cancellationError });
          }
        }
        results.push(accepted);
      } catch (error) {
        const failure = failureRow(row.index, row.id, error);
        results.push(failure);
        await this.engine.settleResultSlot?.(row.id, failure.outcome);
        state.failedAdmissions.add(row.id);
      } finally {
        if (state.inFlight === row.id) state.inFlight = null;
      }
    }
    return results;
  }

  async cancel(batchId, ids) {
    identifier(batchId, 'batchId');
    if (!Array.isArray(ids) || ids.length < 1 || ids.length > 8) throw fail('Cancellation ids: expected 1..8 items');
    const unique = new Set();
    for (const id of ids) {
      identifier(id, 'task id');
      if (unique.has(id)) throw fail('Duplicate cancellation task id', 'CONFLICT');
      unique.add(id);
    }
    const state = this.active.get(batchId);
    if (state) state.stopped = true;
    // Start every cancellation before awaiting any one engine response. This is
    // also valid after settlement, when the caller supplies the stable task IDs.
    const settled = await Promise.allSettled(ids.map(id => invoke(() => this.engine.cancel(id))));
    const results = settled.map((entry, index) => entry.status === 'fulfilled'
      ? { index, id: ids[index], ok: true, result: entry.value }
      : failureRow(index, ids[index], entry.reason));
    if (state) {
      await state.promise;
      for (const row of results) {
        const compensation = state.compensations.get(row.id);
        if (!compensation) continue;
        if (compensation.ok) {
          row.ok = true; row.result = compensation.result;
          delete row.outcome; delete row.error;
        } else {
          row.ok = false; row.outcome = compensation.outcome; row.error = compensation.error;
          delete row.result;
        }
      }
    }
    return { results };
  }

  close() {
    this.closed = true;
    for (const state of this.active.values()) state.stopped = true;
  }
}
