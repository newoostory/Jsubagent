import { totalmem } from 'node:os';
import { MiB, GiB, fail } from '../limits.mjs';

export const MEMORY_POLICY = Object.freeze({
  name: 'process-first', aggregateBudgetMiB: null,
  heapWarnRatio: 0.75, heapMaintenanceRatio: 0.85, heapRecoveryRatio: 0.65,
  maintenanceMs: 5000, recoveryMs: 10000, emergencySamples: 2, emergencyGraceMs: 10000,
  activeSampleMs: 1000, idleSampleMs: 5000, sampleMaxAgeMs: 10000,
  idleRetireMs: 30000, idleKeep: 0, systemRecoveryMs: 20000,
});
const bytes = n => Number.isSafeInteger(n) && n >= 0;
export function memoryPolicy(limits) {
  return { ...MEMORY_POLICY, aggregateBudgetMiB: limits.aggregateBudgetMiB ?? null,
    idleKeep: limits.workerIdleKeep ?? MEMORY_POLICY.idleKeep, idleRetireMs: limits.workerIdleRetireMs ?? MEMORY_POLICY.idleRetireMs,
    maintenanceRssBytes: limits.workerMemoryBytes ?? null, emergencyRssBytes: limits.workerEmergencyBytes ?? null };
}
export function systemReserveBytes(capacity) {
  if (!bytes(capacity) || capacity === 0) throw fail('Invalid effective memory capacity');
  return Math.min(2 * GiB, Math.max(512 * MiB, Math.ceil(capacity / 10)));
}
export function systemMemoryObservation(now = Date.now()) {
  const physical = totalmem(), constrained = process.constrainedMemory?.();
  const capacityBytes = bytes(constrained) && constrained > 0 ? Math.min(physical, constrained) : physical;
  let availableBytes;
  try { availableBytes = process.availableMemory?.(); } catch { /* Unknown is never zero. */ }
  const supported = bytes(availableBytes) && availableBytes <= capacityBytes;
  return { scope: `system:${process.platform}`, source: 'node.availableMemory', sampledAt: now,
    quality: supported ? (process.platform === 'linux' ? 'measured' : 'estimate') : 'unknown',
    capacityBytes, availableBytes: supported ? availableBytes : null, reserveBytes: systemReserveBytes(capacityBytes),
    note: 'Available-memory estimate is an admission signal, not authority to kill existing processes.' };
}
export function processKey(identity) {
  if (!Number.isSafeInteger(identity?.pid) || identity.pid <= 0 || !bytes(identity.startedAt) || typeof identity.nonce !== 'string' || !identity.nonce || identity.nonce.length > 80) throw fail('Invalid memory process identity', 'PROTOCOL');
  return `${identity.pid}:${identity.startedAt}:${identity.nonce}`;
}
export function validMemorySample(sample, { identity, scope, runToken, previousSequence = -1, now = Date.now(), maxAgeMs = MEMORY_POLICY.sampleMaxAgeMs } = {}) {
  try {
    if (processKey(sample.identity) !== processKey(identity) || sample.scope !== scope || sample.runToken !== (runToken ?? null)) return false;
    if (!Number.isSafeInteger(sample.sequence) || sample.sequence <= previousSequence || !bytes(sample.sampledAt) || sample.sampledAt > now + 1000 || now - sample.sampledAt > maxAgeMs) return false;
    if (sample.source !== 'node.memoryUsage' || sample.quality !== 'measured') return false;
    if (!['rss', 'heapUsed', 'heapTotal', 'heapLimit', 'external', 'arrayBuffers'].every(key => bytes(sample[key]))) return false;
    return sample.heapLimit > 0 && sample.heapUsed <= sample.heapTotal;
  } catch { return false; }
}

// Bounded state: no rolling arrays, accumulated samples or retained SDK references.
export class WorkerMemoryGuard {
  constructor(policy) { this.policy = policy; this.state = 'normal'; this.sequence = -1; this.emergencyCount = 0; }
  observe(sample, binding, now = Date.now()) {
    if (!validMemorySample(sample, { ...binding, previousSequence: this.sequence, now })) return { state: this.state, valid: false, reason: 'invalid_or_stale_sample' };
    if (this.lastSampleAt !== undefined && sample.sampledAt < this.lastSampleAt) return { state: this.state, valid: false, reason: 'regressing_sample_time' };
    const gap = this.lastSampleAt !== undefined && sample.sampledAt - this.lastSampleAt > this.policy.sampleMaxAgeMs;
    if (gap) { this.highSince = undefined; this.healthySince = undefined; this.emergencyCount = 0; }
    this.sequence = sample.sequence; this.lastSampleAt = sample.sampledAt;
    const ratio = sample.heapUsed / sample.heapLimit;
    // Native V8 heap pressure and system admission remain active without a fixed RSS ceiling.
    // Finite operator-declared RSS thresholds are opt-in, never inferred from null.
    const maintenance = this.policy.maintenanceRssBytes, emergency = this.policy.emergencyRssBytes;
    const high = ratio >= this.policy.heapMaintenanceRatio || (maintenance !== null && sample.rss >= maintenance);
    const healthy = ratio < this.policy.heapRecoveryRatio && (maintenance === null || sample.rss < maintenance * 0.8);
    this.emergencyCount = emergency !== null && sample.rss >= emergency ? this.emergencyCount + 1 : 0;
    if (this.emergencyCount >= this.policy.emergencySamples) this.state = 'emergency';
    else if (this.state !== 'emergency') {
      if (high) {
        this.highSince ??= sample.sampledAt; this.healthySince = undefined;
        this.state = this.state === 'yield_requested' || sample.sampledAt - this.highSince >= this.policy.maintenanceMs ? 'yield_requested' : 'warn';
      } else {
        this.highSince = undefined;
        if (healthy) this.healthySince ??= sample.sampledAt; else this.healthySince = undefined;
        if (this.state === 'yield_requested' && (!healthy || sample.sampledAt - this.healthySince < this.policy.recoveryMs)) { /* hysteresis */ }
        else this.state = ratio >= this.policy.heapWarnRatio ? 'warn' : 'normal';
      }
    }
    return { state: this.state, valid: true, heapRatio: ratio, sampledAt: sample.sampledAt };
  }
}

// Disjoint actual A / absolute envelope P / unbound starts U / headroom H.
// RSS already contains heap/external/arrayBuffers; those are not extra groups.
export function projectedMemory({ groups, pendingBytes = 0, headroomBytes = 0, scope, now = Date.now(), maxAgeMs = MEMORY_POLICY.sampleMaxAgeMs }) {
  if (!Array.isArray(groups) || groups.length > 1024 || !bytes(pendingBytes) || !bytes(headroomBytes)) return { valid: false, reason: 'invalid_accounting_input' };
  const identities = new Set();
  let actualBytes = 0, projectedManagedBytes = pendingBytes + headroomBytes, remainingGrowthBytes = pendingBytes;
  for (const group of groups) {
    if (!group || group.scope !== scope || !['measured', 'estimate'].includes(group.quality) || !bytes(group.sampledAt) || group.sampledAt > now + 1000 || now - group.sampledAt > maxAgeMs || !bytes(group.actualBytes) || !bytes(group.envelopeBytes) || !Array.isArray(group.processKeys) || group.processKeys.length === 0 || group.processKeys.length > 1024) return { valid: false, reason: 'incompatible_or_stale_group' };
    for (const key of group.processKeys) {
      if (typeof key !== 'string' || !key || key.length > 256 || identities.has(key)) return { valid: false, reason: 'overlapping_process_groups' };
      identities.add(key);
    }
    actualBytes += group.actualBytes;
    projectedManagedBytes += Math.max(group.actualBytes, group.envelopeBytes);
    remainingGrowthBytes += Math.max(0, group.envelopeBytes - group.actualBytes);
  }
  if (![actualBytes, projectedManagedBytes, remainingGrowthBytes].every(bytes)) return { valid: false, reason: 'accounting_overflow' };
  return { valid: true, actualBytes, projectedManagedBytes, remainingGrowthBytes, pendingBytes, headroomBytes, processCount: identities.size };
}
export function memoryAdmission({ accounting, system, aggregateBudgetMiB = null, now = Date.now() }) {
  if (!accounting?.valid) return { admitted: false, reason: accounting?.reason ?? 'memory_observation_unavailable' };
  if (aggregateBudgetMiB !== null && (!Number.isSafeInteger(aggregateBudgetMiB) || aggregateBudgetMiB < 1)) throw fail('Invalid aggregate memory budget');
  if (aggregateBudgetMiB !== null && accounting.projectedManagedBytes > aggregateBudgetMiB * MiB) return { admitted: false, reason: 'aggregate_memory_budget' };
  if (!system || !['measured', 'estimate'].includes(system.quality) || !bytes(system.sampledAt) || system.sampledAt > now + 1000 || now - system.sampledAt > MEMORY_POLICY.sampleMaxAgeMs || !bytes(system.availableBytes) || !bytes(system.reserveBytes)) return { admitted: false, reason: 'system_memory_unavailable' };
  if (system.availableBytes < accounting.remainingGrowthBytes + accounting.headroomBytes + system.reserveBytes) return { admitted: false, reason: 'system_memory_pressure' };
  return { admitted: true };
}
export class SystemMemoryGuard {
  constructor() { this.held = false; }
  observe(system, now = Date.now()) {
    const known = system && ['measured', 'estimate'].includes(system.quality) && bytes(system.sampledAt) && bytes(system.availableBytes) && bytes(system.reserveBytes) && now - system.sampledAt <= MEMORY_POLICY.sampleMaxAgeMs && system.sampledAt <= now + 1000 && (this.lastSampleAt === undefined || system.sampledAt >= this.lastSampleAt);
    if (this.lastSampleAt !== undefined && (!known || system.sampledAt - this.lastSampleAt > MEMORY_POLICY.sampleMaxAgeMs)) this.healthySince = undefined;
    if (known) this.lastSampleAt = system.sampledAt;
    if (!known || system.availableBytes < system.reserveBytes) { this.held = true; this.healthySince = undefined; }
    else if (this.held) { this.healthySince ??= system.sampledAt; if (system.sampledAt - this.healthySince >= MEMORY_POLICY.systemRecoveryMs) this.held = false; }
    return { held: this.held, known: Boolean(known), nextEligibleAt: this.held && this.healthySince !== undefined ? this.healthySince + MEMORY_POLICY.systemRecoveryMs : null };
  }
}
