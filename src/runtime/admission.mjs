import { randomUUID } from 'node:crypto';
import { join, resolve, sep } from 'node:path';
import { lstat, mkdir } from 'node:fs/promises';
import { fail, hash } from '../limits.mjs';
import { processIdentity } from './ownership.mjs';
import { assertProviderFault, overloadCooldownUntil } from './provider-fault.mjs';
import { initializeSharedMemory, updateSharedMemory, sharedMemoryDecision, renewSharedMemory } from './shared-memory.mjs';
import { initializeQuotaPermits, acquireQuotaPermit, settleQuotaPermit } from './quota-permits.mjs';

const PROCESS_TOOLS = new Set(['bash', 'grep', 'find']);
function resourcesConflict(a, b) {
  if (a === b) return true;
  if (!a.startsWith('workspace:') || !b.startsWith('workspace:')) return false;
  // macOS volumes commonly fold case and Unicode normalization. Conservatively
  // serialize equivalent spellings, including not-yet-created output paths.
  const canonical = value => process.platform === 'darwin' ? resolve(value).normalize('NFD').toLowerCase() : resolve(value);
  const left = canonical(a.slice(10)), right = canonical(b.slice(10));
  return left === right || left.startsWith(right.endsWith(sep) ? right : right + sep) || right.startsWith(left.endsWith(sep) ? left : left + sep);
}

function finiteInteger(value, fallback = 0) {
  return Number.isSafeInteger(value) && value >= 0 ? value : fallback;
}

export function admissionResources(task, limits) {
  const hostOwned = task.workItem?.execution === 'artifact-sha256';
  const account = hostOwned ? 'host-artifact-sha256' : task.profile.account ?? task.profile.provider;
  const memoryBytes = finiteInteger(task.profile.memoryBytes, limits.taskMemoryBytes);
  const defaultTools = task.role.tools?.some(tool => PROCESS_TOOLS.has(tool)) || task.role.workspace === 'worktree' ? 1 : 0;
  const toolProcesses = finiteInteger(task.profile.toolProcesses, defaultTools);
  // Workspace describes execution placement, not an exclusive resource claim.
  // Workers already have private worktrees; shared/in-place work is coordinated
  // by its caller. Only explicit resources acquire cross-run conflict fences.
  // Per-task effect/permit fences still prevent unsafe resume/replay; usage alone does not.
  const resources = new Set(task.ownedResources ?? []);
  return {
    taskId: task.id, account, model: hostOwned ? 'artifact-sha256' : task.profile.model, memoryBytes, toolProcesses, usesWorker: !hostOwned,
    resources: [...resources].sort(), acquiredAt: Date.now(),
    policyCaps: { concurrency: limits.concurrency, commandConcurrency: limits.commandConcurrency, memorySoftBytes: limits.memorySoftBytes, memoryStopBytes: limits.memoryStopBytes, aggregateBudgetMiB: limits.aggregateBudgetMiB ?? null },
  };
}

function activeResources({ reservations, running, tasks, limits }) {
  if (reservations) return [...reservations.values()];
  return [...running.keys()].map(id => tasks.get(id)).filter(Boolean).map(task => admissionResources(task, limits));
}

// This is a pure all-or-nothing decision. AdmissionController publishes a
// reservation only after every constraint passes, so no task can hold a subset
// of account/model/memory/tool/workspace capacity while waiting for another.
export function admissionDecision(task, {
  running = new Map(), tasks = new Map(), limits, pressure, connectionHeld,
  workers = limits.concurrency, localWorkerCount, memory, sharedMemory = false, quotaPools = new Map(), reservations, now = Date.now(),
}) {
  if (connectionHeld && task.profile.allowBackground !== true) return { admitted: false, reason: 'foreground_disconnected' };
  if (pressure) return { admitted: false, reason: 'memory_pressure' };
  const retained = activeResources({ reservations, running, tasks, limits });
  const active = retained.filter(row => !row.lockOnly);
  const effective = key => Math.min(limits[key] ?? Infinity, ...active.map(row => Object.hasOwn(row.policyCaps ?? {}, key) ? row.policyCaps[key] ?? Infinity : limits[key] ?? Infinity));
  const hostOwned = task.workItem?.execution === 'artifact-sha256';
  if (active.length >= effective('concurrency')) return { admitted: false, reason: 'execution_slots' };
  // Shared reservations consume global/account capacity, not this host's workers.
  const localWorkers = localWorkerCount ?? active.filter(row => row.usesWorker !== false).length;
  if (!hostOwned && localWorkers >= workers) return { admitted: false, reason: 'worker_slots' };
  const requested = admissionResources(task, limits);
  if (!hostOwned) {
    const pool = quotaPools.get(requested.account);
    if (pool?.held) return { admitted: false, reason: pool.reason ?? 'auth_pool_hold' };
    if (pool?.retryAt > now) return { admitted: false, reason: pool.reason ?? 'quota_pool_backoff', retryAt: pool.retryAt };
    const sameAccount = active.filter(row => row.account === requested.account);
    const accountCap = Math.min(task.profile.maxConcurrent ?? limits.concurrency, ...sameAccount.map(row => row.maxConcurrent ?? tasks.get(row.taskId)?.profile.maxConcurrent ?? limits.concurrency));
    if (sameAccount.length >= accountCap) return { admitted: false, reason: 'account_slots' };
    const sameModel = sameAccount.filter(row => row.model === requested.model);
    const modelCap = Math.min(task.profile.modelConcurrent ?? accountCap, ...sameModel.map(row => row.modelConcurrent ?? tasks.get(row.taskId)?.profile.modelConcurrent ?? tasks.get(row.taskId)?.profile.maxConcurrent ?? accountCap));
    if (sameModel.length >= modelCap) return { admitted: false, reason: 'model_slots' };
  }
  const activeTools = active.reduce((sum, row) => sum + finiteInteger(row.toolProcesses), 0);
  if (activeTools + requested.toolProcesses > effective('commandConcurrency')) return { admitted: false, reason: 'tool_process_slots' };
  const locked = new Set(retained.flatMap(row => row.resources ?? []));
  if (requested.resources.some(resource => [...locked].some(held => resourcesConflict(resource, held)))) return { admitted: false, reason: 'workspace_or_resource_lock' };
  if (!sharedMemory && Number.isFinite(effective('aggregateBudgetMiB'))) return { admitted: false, reason: 'memory_observation_unavailable' };
  if (!sharedMemory && memory?.supported) {
    const reservedBytes = active.reduce((sum, row) => sum + finiteInteger(row.memoryBytes), 0);
    if (memory.rssBytes >= effective('memoryStopBytes')) return { admitted: false, reason: 'memory_stop' };
    if (memory.rssBytes + reservedBytes + requested.memoryBytes > effective('memorySoftBytes')) return { admitted: false, reason: 'memory_reservation' };
  }
  return { admitted: true, reservation: { ...requested, maxConcurrent: task.profile.maxConcurrent, modelConcurrent: task.profile.modelConcurrent } };
}

class SharedAdmissionLedger {
  static async open(agentDir, runId, ownerNonce) {
    const directory = join(agentDir, 'pi-agent');
    await mkdir(directory, { recursive: true, mode: 0o700 });
    const info = await lstat(directory);
    if (!info.isDirectory() || info.isSymbolicLink()) throw fail('Unsafe shared admission directory', 'STORAGE');
    const path = join(directory, 'runtime-resources.sqlite');
    try { const file = await lstat(path); if (!file.isFile() || file.isSymbolicLink()) throw fail('Unsafe shared admission database', 'STORAGE'); }
    catch (error) { if (error.code !== 'ENOENT') throw error; }
    const { DatabaseSync } = await import('node:sqlite');
    const database = new DatabaseSync(path);
    const ledger = new SharedAdmissionLedger(database, runId, ownerNonce);
    try { ledger.ownerStarted = await processIdentity(process.pid); ledger.initialize(); return ledger; } catch (error) { try { database.close(); } catch {} throw error; }
  }
  constructor(database, runId, ownerNonce) { this.database = database; this.runId = runId; this.ownerNonce = ownerNonce; }
  initialize() {
    this.database.function('resource_conflicts', { deterministic: true }, (a, b) => Number(resourcesConflict(a, b)));
    // Unsupported stores are never upgraded or cleared: old reservations may
    // still fence live executors or unresolved effects. Require isolated state.
    const columns = new Set(this.database.prepare('PRAGMA table_info(reservations)').all().map(row => row.name));
    const required = ['reservation_id', 'owner_nonce', 'owner_pid', 'run_id', 'task_id', 'account', 'model', 'memory_bytes', 'tool_processes', 'resources_json', 'max_concurrent', 'model_concurrent', 'updated_at', 'policy_json', 'owner_started', 'executor_json', 'uncertain', 'execution_released'];
    if (columns.size && required.some(name => !columns.has(name))) throw fail('Unsupported shared admission schema; preserve existing state and use an isolated runtime state directory', 'STORAGE_SCHEMA');
    const quotaColumns = new Set(this.database.prepare('PRAGMA table_info(quota_pools)').all().map(row => row.name));
    if (quotaColumns.size && ['account', 'state', 'retry_at', 'reason', 'failures', 'updated_at'].some(name => !quotaColumns.has(name))) throw fail('Unsupported shared quota schema; existing state was preserved', 'STORAGE_SCHEMA');
    this.database.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = FULL;
      PRAGMA busy_timeout = 5000;
      PRAGMA trusted_schema = OFF;
      PRAGMA max_page_count = 8192;
      CREATE TABLE IF NOT EXISTS reservations (
        reservation_id TEXT PRIMARY KEY, owner_nonce TEXT NOT NULL, owner_pid INTEGER NOT NULL,
        run_id TEXT NOT NULL, task_id TEXT NOT NULL, account TEXT NOT NULL, model TEXT NOT NULL,
        memory_bytes INTEGER NOT NULL, tool_processes INTEGER NOT NULL, resources_json TEXT NOT NULL,
        max_concurrent INTEGER NOT NULL, model_concurrent INTEGER NOT NULL, updated_at INTEGER NOT NULL, policy_json TEXT NOT NULL,
        owner_started TEXT NOT NULL, executor_json TEXT, uncertain INTEGER NOT NULL DEFAULT 0, execution_released INTEGER NOT NULL DEFAULT 0
      );
      CREATE INDEX IF NOT EXISTS reservations_owner ON reservations(owner_nonce);
      CREATE TABLE IF NOT EXISTS quota_pools (
        account TEXT PRIMARY KEY, state TEXT NOT NULL, retry_at INTEGER, reason TEXT, failures INTEGER NOT NULL, updated_at INTEGER NOT NULL
      );
    `);
    // Forward-migrate only our known prior quota table by adding bounded safe metadata.
    // Unknown/missing legacy fence columns above still fail without mutation.
    this.database.exec('BEGIN IMMEDIATE');
    try {
      const columns = new Set(this.database.prepare('PRAGMA table_info(quota_pools)').all().map(row => row.name));
      if (!columns.has('fault_json')) this.database.exec('ALTER TABLE quota_pools ADD COLUMN fault_json TEXT');
      if (!columns.has('scope_json')) this.database.exec('ALTER TABLE quota_pools ADD COLUMN scope_json TEXT');
      initializeSharedMemory(this.database);
      initializeQuotaPermits(this.database);
      this.database.exec('COMMIT');
    } catch (error) { try { this.database.exec('ROLLBACK'); } catch {} throw error; }
    this.database.prepare('SELECT owner_started,executor_json,uncertain,policy_json,execution_released FROM reservations LIMIT 0').all();
  }
  transaction(operation) {
    this.database.exec('BEGIN IMMEDIATE');
    try { const result = operation(); this.database.exec('COMMIT'); return result; }
    catch (error) { try { this.database.exec('ROLLBACK'); } catch {} throw error; }
  }
  async reapStoppedOwners() {
    const rows = this.database.prepare('SELECT reservation_id,owner_nonce,owner_pid,owner_started,executor_json,uncertain FROM reservations WHERE execution_released=0 LIMIT 1025').all();
    if (rows.length > 1024) throw fail('Shared admission working set exceeded', 'BUSY');
    const identities = new Map();
    const current = async pid => { if (!identities.has(pid)) identities.set(pid, await processIdentity(pid)); return identities.get(pid); };
    for (const row of rows) {
      if (row.owner_nonce === this.ownerNonce) continue;
      if (await current(Number(row.owner_pid)) === row.owner_started) continue;
      const executor = row.executor_json && JSON.parse(row.executor_json);
      let alive = false;
      for (const identity of executor ? [executor, ...(executor.guardians ?? [])] : []) if (await current(identity.pid) === identity.started) { alive = true; break; }
      if (alive) continue;
      if (row.uncertain) this.database.prepare('UPDATE reservations SET execution_released=1 WHERE reservation_id=? AND owner_nonce=?').run(row.reservation_id, row.owner_nonce);
      else this.database.prepare('DELETE FROM reservations WHERE reservation_id=? AND owner_nonce=? AND uncertain=0').run(row.reservation_id, row.owner_nonce);
    }
    const observers = this.database.prepare('SELECT * FROM memory_observations LIMIT 129').all();
    if (observers.length > 128) throw fail('Shared memory observer working set exceeded', 'BUSY');
    for (const observer of observers) {
      if (observer.owner_nonce === this.ownerNonce || await current(Number(observer.owner_pid)) === observer.owner_started) continue;
      const observation = JSON.parse(observer.observation_json); let alive = false;
      for (const group of observation.groups) {
        if (group.shared) continue;
        for (const identity of group.identities) {
          const actual = await current(identity.pid);
          if (actual && actual.replace(/\s+/g, ' ') === identity.started.replace(/\s+/g, ' ')) { alive = true; break; }
        }
        if (alive) break;
      }
      if (!alive) this.database.prepare('DELETE FROM memory_observations WHERE owner_nonce=? AND owner_started=?').run(observer.owner_nonce, observer.owner_started);
    }
  }
  reservations(resources = []) {
    const rows = this.database.prepare(`SELECT * FROM reservations WHERE execution_released=0 OR EXISTS (
      SELECT 1 FROM json_each(resources_json) held JOIN json_each(?) requested ON resource_conflicts(held.value,requested.value)
    ) ORDER BY reservation_id LIMIT 1025`).all(JSON.stringify(resources));
    if (rows.length > 1024) throw fail('Shared admission ledger is full', 'BUSY');
    return new Map(rows.map(row => [row.reservation_id, {
      taskId: row.task_id, account: row.account, model: row.model, memoryBytes: Number(row.memory_bytes),
      toolProcesses: Number(row.tool_processes), resources: JSON.parse(row.resources_json),
      maxConcurrent: Number(row.max_concurrent), modelConcurrent: Number(row.model_concurrent),
      policyCaps: JSON.parse(row.policy_json), lockOnly: Boolean(row.execution_released),
    }]));
  }
  quotaPools(now = Date.now()) {
    const pools = new Map();
    for (const row of this.database.prepare("SELECT * FROM quota_pools WHERE state = 'held' OR (state = 'backoff' AND retry_at > ?)").all(now)) {
      let fault;
      if (row.fault_json) { try { fault = assertProviderFault(JSON.parse(row.fault_json)); } catch { throw fail('Corrupt shared provider fault metadata', 'STORAGE'); } }
      // Legacy held overloads become a bounded cooldown without clearing the
      // original task's unknown request/effect fence or its recovery receipt.
      const expiry = row.state === 'held' ? overloadCooldownUntil(fault, Number(row.updated_at), Number(row.retry_at) || 0) : null;
      const held = row.state === 'held' && expiry === null;
      const retryAt = expiry ?? (row.retry_at == null ? undefined : Number(row.retry_at));
      if (!held && !(retryAt > now)) continue;
      pools.set(row.account, { held, retryAt, reason: row.reason, failures: Number(row.failures), ...(fault ? { fault } : {}) });
    }
    return pools;
  }
  async reserve(task, limits, context) {
    await this.reapStoppedOwners();
    return this.transaction(() => {
      const quotaPools = this.quotaPools(context.now);
      const decision = admissionDecision(task, { ...context, limits, quotaPools, reservations: this.reservations(admissionResources(task, limits).resources) });
      if (!decision.admitted) return decision;
      if (context.sharedMemory) {
        if (!context.memoryObservationKnown) return { admitted: false, reason: 'memory_observation_unavailable' };
        const memory = sharedMemoryDecision(this.database, this, task, limits, context, decision.reservation);
        if (!memory.admitted) return memory;
        decision.reservation.memory = memory.memory; decision.reservation.memoryPlan = memory.memoryPlan; decision.accounting = memory.accounting;
      }
      const row = decision.reservation, taskKey = hash(`${this.ownerNonce}\0${this.runId}\0${task.id}`), id = `${this.ownerNonce}:${taskKey}`;
      this.database.prepare(`INSERT INTO reservations
        (reservation_id, owner_nonce, owner_pid, run_id, task_id, account, model, memory_bytes, tool_processes, resources_json, max_concurrent, model_concurrent, updated_at, policy_json, owner_started)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      ).run(id, this.ownerNonce, process.pid, this.runId, taskKey, row.account, row.model, row.memoryBytes, row.toolProcesses, JSON.stringify(row.resources), row.maxConcurrent ?? limits.concurrency, row.modelConcurrent ?? row.maxConcurrent ?? limits.concurrency, Date.now(), JSON.stringify(row.policyCaps), this.ownerStarted);
      if (row.memory) this.database.prepare('UPDATE reservations SET memory_json=? WHERE reservation_id=? AND owner_nonce=?').run(JSON.stringify(row.memory), id, this.ownerNonce);
      decision.reservation.fence = { id, ownerNonce: this.ownerNonce, runId: this.runId, taskKey };
      return decision;
    });
  }
  observeMemory(observation) { return this.transaction(() => updateSharedMemory(this.database, this, observation)); }
  renewMemory(task, limits, context) { return this.transaction(() => renewSharedMemory(this.database, this, this.taskKey(task.id), limits, { ...context, memoryBytes: admissionResources(task, limits).memoryBytes })); }
  requestPermit(task, requestId) {
    return this.transaction(() => {
      // The task's durable intent precedes this transaction. Retain its execution
      // fence with the claim so owner reaping cannot discard an unknown request.
      this.markUncertain(task.id, true);
      return acquireQuotaPermit(this.database, this, task, requestId);
    });
  }
  unsettledPermit(task) {
    const account = task.profile.account ?? task.profile.provider;
    // Derive the exact historical task key from the claim's owner, not the new
    // supervisor nonce or the optional task marker. Read at most two account rows.
    for (const table of ['quota_recovery_probes', 'quota_request_claims']) {
      const row = this.database.prepare(`SELECT * FROM ${table} WHERE account=?`).get(account);
      if (row?.run_id !== this.runId || row.task_key !== hash(`${row.owner_nonce}\0${this.runId}\0${task.id}`)) continue;
      if (!['pending', 'unknown'].includes(row.outcome) || !Number.isSafeInteger(row.created_at) || row.created_at < 0) throw fail('Invalid unresolved provider permit claim', 'STORAGE');
      return { requestId: row.request_id, observedAt: row.created_at };
    }
    return null;
  }
  settlePermit(task, requestId, outcome) { return this.transaction(() => settleQuotaPermit(this.database, this, task, requestId, outcome)); }
  taskKey(taskId) { return hash(`${this.ownerNonce}\0${this.runId}\0${taskId}`); }
  registerExecutor(taskId, executor) {
    if (!Number.isSafeInteger(executor?.pid) || typeof executor.started !== 'string' || !executor.nonce) throw fail('Invalid execution fence', 'STORAGE');
    const result = this.database.prepare('UPDATE reservations SET executor_json=? WHERE owner_nonce=? AND run_id=? AND task_id=?').run(JSON.stringify(executor), this.ownerNonce, this.runId, this.taskKey(taskId));
    if (result.changes !== 1) throw fail('Execution reservation disappeared before worker start', 'STORAGE');
  }
  markUncertain(taskId, uncertain) { this.database.prepare('UPDATE reservations SET uncertain=? WHERE owner_nonce=? AND run_id=? AND task_id=?').run(uncertain ? 1 : 0, this.ownerNonce, this.runId, this.taskKey(taskId)); }
  release(taskId) {
    this.transaction(() => {
      this.database.prepare('UPDATE reservations SET execution_released=1 WHERE owner_nonce=? AND run_id=? AND task_id=?').run(this.ownerNonce, this.runId, this.taskKey(taskId));
      this.database.prepare('DELETE FROM reservations WHERE owner_nonce=? AND run_id=? AND task_id=? AND uncertain=0').run(this.ownerNonce, this.runId, this.taskKey(taskId));
    });
  }
  async reconcile(fence, { preserveRequestClaims = false } = {}) {
    if (!fence || fence.runId !== this.runId) throw fail('Reconciliation reservation scope mismatch', 'PERMISSION');
    await this.reapStoppedOwners();
    this.transaction(() => {
      const row = this.database.prepare('SELECT * FROM reservations WHERE reservation_id=?').get(fence.id);
      if (!row) return;
      if (row.run_id !== fence.runId || row.owner_nonce !== fence.ownerNonce || row.task_id !== fence.taskKey || !row.execution_released) throw fail('Reservation is not safely settled for reconciliation', 'BUSY');
      // Automatic checkpoint/resource reconciliation is not evidence that a
      // shared provider claim settled. Only the existing explicit reconciliation
      // route may retire claims; accepting usage uncertainty never does.
      if (!preserveRequestClaims) {
        this.database.prepare('DELETE FROM quota_request_claims WHERE owner_nonce=? AND run_id=? AND task_key=?').run(fence.ownerNonce, fence.runId, fence.taskKey);
        this.database.prepare('DELETE FROM quota_recovery_probes WHERE owner_nonce=? AND run_id=? AND task_key=?').run(fence.ownerNonce, fence.runId, fence.taskKey);
      }
      this.database.prepare('DELETE FROM reservations WHERE reservation_id=?').run(fence.id);
    });
  }
  setQuotaPool(account, pool) {
    const state = pool?.held ? 'held' : pool?.retryAt > Date.now() ? 'backoff' : 'released';
    const fault = pool?.fault ? assertProviderFault(pool.fault) : undefined;
    const scopeJson = fault ? JSON.stringify(fault.quotaScopeIds) : null;
    this.transaction(() => {
      const prior = this.database.prepare('SELECT state,updated_at,retry_at,fault_json FROM quota_pools WHERE account=?').get(account);
      const priorFault = prior?.fault_json ? assertProviderFault(JSON.parse(prior.fault_json)) : null;
      const expiredOverload = prior?.state === 'held' && (overloadCooldownUntil(priorFault, Number(prior.updated_at), Number(prior.retry_at) || 0) ?? Infinity) <= Date.now();
      this.database.prepare(`INSERT INTO quota_pools (account, state, retry_at, reason, failures, updated_at, fault_json, scope_json, needs_probe, permit_generation) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1, 1)
        ON CONFLICT(account) DO UPDATE SET state=excluded.state, retry_at=excluded.retry_at, reason=excluded.reason, failures=excluded.failures, updated_at=excluded.updated_at, fault_json=excluded.fault_json, scope_json=excluded.scope_json, needs_probe=1, permit_generation=quota_pools.permit_generation+1
        WHERE quota_pools.state != 'held' OR excluded.state = 'held' OR ? = 1`
      ).run(account, state, pool?.retryAt ?? null, pool?.reason ?? null, pool?.failures ?? 0, Date.now(), fault ? JSON.stringify(fault) : null, scopeJson, !pool || Object.keys(pool).length === 0 || expiredOverload ? 1 : 0);
    });
  }
  close() {
    if (!this.database) return;
    this.transaction(() => {
      this.database.prepare('DELETE FROM reservations WHERE owner_nonce = ? AND uncertain=0 AND execution_released=1').run(this.ownerNonce);
    });
    this.database.close(); this.database = null;
  }
}

export class AdmissionController {
  constructor(limits) { this.limits = limits; this.reservations = new Map(); this.ownerNonce = randomUUID(); }
  async openShared({ agentDir, runId }) { this.shared = await SharedAdmissionLedger.open(agentDir, runId, this.ownerNonce); return this.shared.quotaPools(); }
  updateLimits(limits) { this.limits = limits; }
  decide(task, context = {}) {
    return admissionDecision(task, { ...context, localWorkerCount: [...this.reservations.values()].filter(row => row.usesWorker !== false).length, limits: this.limits, reservations: this.reservations });
  }
  reserve(task, context = {}) {
    if (this.reservations.has(task.id)) return { admitted: false, reason: 'duplicate_reservation' };
    const decision = this.decide(task, context);
    if (decision.admitted) this.reservations.set(task.id, decision.reservation);
    return decision;
  }
  async reserveShared(task, context = {}) {
    if (!this.shared) return this.reserve(task, context);
    if (this.reservations.has(task.id)) return { admitted: false, reason: 'duplicate_reservation' };
    const decision = await this.shared.reserve(task, this.limits, { ...context, localWorkerCount: [...this.reservations.values()].filter(row => row.usesWorker !== false).length });
    if (decision.admitted) this.reservations.set(task.id, decision.reservation);
    return decision;
  }
  release(taskId) { const found = this.reservations.get(taskId); this.reservations.delete(taskId); return found; }
  async releaseShared(taskId) { const found = this.release(taskId); this.shared?.release(taskId); return found; }
  registerExecutor(taskId, executor) { this.shared?.registerExecutor(taskId, executor); }
  markUncertain(taskId, uncertain) { this.shared?.markUncertain(taskId, uncertain); }
  observeMemory(observation) { this.shared?.observeMemory(observation); }
  renewMemory(task, context) { return this.shared ? this.shared.renewMemory(task, this.limits, context) : { admitted: false, reason: 'shared_memory_unavailable' }; }
  requestPermit(task, requestId) { return this.shared?.requestPermit(task, requestId); }
  unsettledPermit(task) { return this.shared?.unsettledPermit(task) ?? null; }
  settlePermit(task, requestId, outcome) { this.shared?.settlePermit(task, requestId, outcome); }
  async reconcile(fence, options) { if (fence) await this.shared?.reconcile(fence, options); }
  setQuotaPool(account, pool) { this.shared?.setQuotaPool(account, pool); }
  quotaPools() { return this.shared?.quotaPools() ?? new Map(); }
  close() { this.shared?.close(); this.shared = null; }
  get(taskId) { return this.reservations.get(taskId); }
}
