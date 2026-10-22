import { encodeJson, fail, hash } from '../limits.mjs';
import { memoryAdmission, projectedMemory } from './memory-policy.mjs';
import { projectSharedMemory } from './memory-accounting.mjs';

export function executorMemoryKey(identity) { return `m-${hash(JSON.stringify([identity.pid, identity.started, identity.nonce]))}`; }
export function initializeSharedMemory(database) {
  const columns = new Set(database.prepare('PRAGMA table_info(reservations)').all().map(row => row.name));
  if (!columns.has('memory_json')) database.exec('ALTER TABLE reservations ADD COLUMN memory_json TEXT');
  database.exec('CREATE TABLE IF NOT EXISTS memory_observations (owner_nonce TEXT PRIMARY KEY, owner_pid INTEGER NOT NULL, owner_started TEXT NOT NULL, observation_json TEXT NOT NULL)');
}
export function updateSharedMemory(database, owner, observation) {
  if (observation.ownerNonce !== owner.ownerNonce || !Array.isArray(observation.groups) || observation.groups.length > 1024) throw fail('Invalid owned memory observation', 'PROTOCOL');
  encodeJson(observation, 256 * 1024);
  const validation = projectedMemory({ groups: observation.groups.map(group => ({ ...group, envelopeBytes: 0 })), scope: observation.scope, now: observation.sampledAt });
  if (!validation.valid) throw fail(`Invalid memory observation: ${validation.reason}`, 'PROTOCOL');
  database.prepare('INSERT INTO memory_observations(owner_nonce,owner_pid,owner_started,observation_json) VALUES(?,?,?,?) ON CONFLICT(owner_nonce) DO UPDATE SET observation_json=excluded.observation_json').run(owner.ownerNonce, process.pid, owner.ownerStarted, JSON.stringify(observation));
  const rows = database.prepare('SELECT reservation_id,executor_json,memory_json FROM reservations WHERE owner_nonce=? AND execution_released=0 LIMIT 1025').all(owner.ownerNonce);
  if (rows.length > 1024) throw fail('Shared memory reservation working set exceeded', 'BUSY');
  for (const row of rows) {
    if (!row.memory_json || !row.executor_json) continue;
    const memory = JSON.parse(row.memory_json), executor = JSON.parse(row.executor_json);
    const worker = observation.groups.find(group => group.id === executorMemoryKey(executor));
    if (memory.workerPendingBytes && worker) {
      memory.memoryBindings.push({ groupId: worker.id, category: 'worker', envelopeBytes: memory.workerPendingBytes, processKeys: worker.processKeys });
      memory.pendingBytes -= memory.workerPendingBytes; memory.workerPendingBytes = 0;
    }
    const guardians = new Set((executor.guardians ?? []).map(executorMemoryKey));
    memory.memoryBindings = memory.memoryBindings.filter(binding => {
      if (binding.category !== 'tool' || guardians.has(binding.groupId) || observation.groups.some(group => group.id === binding.groupId)) return true;
      memory.pendingBytes += binding.envelopeBytes; memory.toolPendingBytes += binding.envelopeBytes; return false;
    });
    // Process membership can change inside a guardian group. The observer has
    // revalidated that complete group; refresh only its identity-bound binding.
    for (const binding of memory.memoryBindings) {
      const group = observation.groups.find(group => group.id === binding.groupId);
      if (group && group.category === 'tool') binding.processKeys = group.processKeys;
    }
    for (const guardian of executor.guardians ?? []) {
      const group = observation.groups.find(group => group.id === executorMemoryKey(guardian));
      if (!group || memory.memoryBindings.some(binding => binding.groupId === group.id) || memory.toolPendingBytes < memory.toolEnvelopeBytes) continue;
      memory.memoryBindings.push({ groupId: group.id, category: 'tool', envelopeBytes: memory.toolEnvelopeBytes, processKeys: group.processKeys });
      memory.pendingBytes -= memory.toolEnvelopeBytes; memory.toolPendingBytes -= memory.toolEnvelopeBytes;
    }
    database.prepare('UPDATE reservations SET memory_json=? WHERE reservation_id=? AND owner_nonce=?').run(JSON.stringify(memory), row.reservation_id, owner.ownerNonce);
  }
}
export function sharedMemoryDecision(database, owner, task, limits, context, requested) {
  const rows = database.prepare('SELECT * FROM memory_observations LIMIT 129').all();
  if (rows.length > 128) return { admitted: false, reason: 'memory_observer_limit' };
  const observations = rows.map(row => JSON.parse(row.observation_json));
  const active = database.prepare('SELECT owner_nonce,executor_json,memory_json,policy_json FROM reservations WHERE execution_released=0 LIMIT 1025').all();
  if (active.length > 1024) return { admitted: false, reason: 'memory_reservation_limit' };
  if (active.some(row => !row.memory_json)) return { admitted: false, reason: 'incompatible_shared_memory_reservation' };
  const reservations = active.map(row => ({ ...JSON.parse(row.memory_json), ownerNonce: row.owner_nonce }));
  const observation = observations.find(row => row.ownerNonce === owner.ownerNonce);
  if (!observation || !context.systemMemory) return { admitted: false, reason: 'memory_observation_unavailable' };
  if (context.systemMemory.scope !== observation.scope) return { admitted: false, reason: 'memory_scope_mismatch' };
  const claimed = new Set(reservations.flatMap(reservation => reservation.memoryBindings.map(binding => binding.groupId)));
  const workerGroups = observation.groups.filter(group => group.category === 'worker');
  const warm = workerGroups.find(group => group.available === true && group.configEpoch === context.configEpoch && !claimed.has(group.id));
  const hostOwned = task.workItem?.execution === 'artifact-sha256';
  if (!hostOwned && !warm && workerGroups.length >= (context.workers ?? limits.workerMax)) return { admitted: false, reason: 'memory_worker_not_ready' };
  // Maintenance/emergency RSS thresholds are not footprint reservations.
  // A warm group's hint is an absolute envelope; only cold starts add the
  // separately configured startup hint. Both are estimates, not hard limits.
  const envelopeBytes = requested.memoryBytes + (!hostOwned && !warm ? limits.workerStartupBytes : warm && task.memoryMaintenance?.growth ? warm.actualBytes : 0);
  const toolEnvelopeBytes = limits.taskMemoryBytes;
  const candidate = { ownerNonce: owner.ownerNonce, memoryBindings: warm && !hostOwned ? [{ groupId: warm.id, envelopeBytes, processKeys: warm.processKeys }] : [], pendingBytes: (warm && !hostOwned ? 0 : envelopeBytes) + requested.toolProcesses * toolEnvelopeBytes, workerPendingBytes: warm && !hostOwned ? 0 : envelopeBytes, toolPendingBytes: requested.toolProcesses * toolEnvelopeBytes, toolEnvelopeBytes };
  const accounting = projectSharedMemory({ observations, reservations, scope: observation.scope, candidate, now: context.now ?? Date.now() });
  const caps = key => Math.min(limits[key] ?? Infinity, ...active.map(row => JSON.parse(row.policy_json)[key] ?? Infinity));
  if (Number.isFinite(caps('aggregateBudgetMiB')) && observations.some(row => row.mainObserved !== true)) return { admitted: false, reason: 'main_memory_observation_unavailable' };
  const gate = memoryAdmission({ accounting, system: context.systemMemory, aggregateBudgetMiB: Number.isFinite(caps('aggregateBudgetMiB')) ? caps('aggregateBudgetMiB') : null, now: context.now ?? Date.now() });
  if (!gate.admitted) return gate;
  if (accounting.actualBytes >= caps('memoryStopBytes')) return { admitted: false, reason: 'memory_stop' };
  if (accounting.projectedManagedBytes > caps('memorySoftBytes')) return { admitted: false, reason: 'memory_reservation' };
  return { admitted: true, memory: candidate, memoryPlan: hostOwned ? undefined : { workerKey: warm?.id ?? null, cold: !warm }, accounting };
}

// Called at settled execution boundaries, never as a quota probe. Replace the
// existing reservation in one shared transaction; do not count it a second time.
export function renewSharedMemory(database, owner, taskKey, limits, context) {
  const rows = database.prepare('SELECT reservation_id,owner_nonce,task_id,memory_json,policy_json FROM reservations WHERE execution_released=0 LIMIT 1025').all();
  if (rows.length > 1024 || rows.some(row => !row.memory_json)) return { admitted: false, reason: 'incompatible_shared_memory_reservation' };
  const current = rows.find(row => row.owner_nonce === owner.ownerNonce && row.task_id === taskKey);
  if (!current) return { admitted: false, reason: 'memory_reservation_missing' };
  const raw = database.prepare('SELECT observation_json FROM memory_observations LIMIT 129').all();
  if (raw.length > 128) return { admitted: false, reason: 'memory_observer_limit' };
  const observations = raw.map(row => JSON.parse(row.observation_json)), observation = observations.find(row => row.ownerNonce === owner.ownerNonce);
  if (!context.memoryObservationKnown || !observation || !context.systemMemory || observation.scope !== context.systemMemory.scope) return { admitted: false, reason: 'memory_observation_unavailable' };
  const memory = JSON.parse(current.memory_json);
  for (const binding of memory.memoryBindings) {
    const group = observation.groups.find(row => row.id === binding.groupId);
    if (group && group.actualBytes >= binding.envelopeBytes) {
      const hint = binding.category === 'tool' ? memory.toolEnvelopeBytes : context.memoryBytes ?? limits.taskMemoryBytes;
      binding.envelopeBytes = group.actualBytes + hint;
    }
  }
  const reservations = rows.map(row => ({ ...(row === current ? memory : JSON.parse(row.memory_json)), ownerNonce: row.owner_nonce }));
  const accounting = projectSharedMemory({ observations, reservations, scope: observation.scope, now: context.now ?? Date.now() });
  const caps = key => Math.min(limits[key] ?? Infinity, ...rows.map(row => JSON.parse(row.policy_json)[key] ?? Infinity));
  if (Number.isFinite(caps('aggregateBudgetMiB')) && observations.some(row => row.mainObserved !== true)) return { admitted: false, reason: 'main_memory_observation_unavailable' };
  const gate = memoryAdmission({ accounting, system: context.systemMemory, aggregateBudgetMiB: Number.isFinite(caps('aggregateBudgetMiB')) ? caps('aggregateBudgetMiB') : null, now: context.now ?? Date.now() });
  if (!gate.admitted) return gate;
  if (accounting.actualBytes >= caps('memoryStopBytes')) return { admitted: false, reason: 'memory_stop' };
  if (accounting.projectedManagedBytes > caps('memorySoftBytes')) return { admitted: false, reason: 'memory_reservation' };
  database.prepare('UPDATE reservations SET memory_json=? WHERE reservation_id=? AND owner_nonce=?').run(JSON.stringify(memory), current.reservation_id, owner.ownerNonce);
  return { admitted: true, accounting };
}
