import { projectedMemory, MEMORY_POLICY } from './memory-policy.mjs';

const amount = value => Number.isSafeInteger(value) && value >= 0;
// Join shared reservations to disjoint observed groups. A binding is installed by
// the ledger only after the executor identity appears in a valid sample, not at
// PID allocation. Unbound reservations remain U, including cold process starts.
export function projectSharedMemory({ observations, reservations, scope, candidate, now = Date.now() }) {
  if (!Array.isArray(observations) || observations.length > 128 || !Array.isArray(reservations) || reservations.length > 1024 || typeof scope !== 'string') return { valid: false, reason: 'invalid_shared_memory_input' };
  const groups = new Map(), owners = new Set(), claims = new Set();
  for (const observation of observations) {
    if (!observation || observation.scope !== scope || typeof observation.ownerNonce !== 'string' || owners.has(observation.ownerNonce) || !amount(observation.sampledAt) || observation.sampledAt > now + 1000 || now - observation.sampledAt > MEMORY_POLICY.sampleMaxAgeMs || !Array.isArray(observation.groups) || observation.groups.length > 1024) return { valid: false, reason: 'incompatible_or_stale_owner_observation' };
    owners.add(observation.ownerNonce);
    for (const group of observation.groups) {
      if (!group || typeof group.id !== 'string' || group.ownerNonce !== observation.ownerNonce || group.scope !== scope) return { valid: false, reason: 'overlapping_or_foreign_memory_group' };
      const previous = groups.get(group.id);
      if (previous) {
        if (!previous.shared || !group.shared || JSON.stringify(previous.processKeys) !== JSON.stringify(group.processKeys)) return { valid: false, reason: 'overlapping_or_foreign_memory_group' };
        if (previous.sampledAt >= group.sampledAt) continue;
      }
      groups.set(group.id, { ...group, envelopeBytes: 0 });
    }
  }
  let pendingBytes = 0;
  for (const reservation of [...reservations, ...(candidate ? [candidate] : [])]) {
    if (reservation.lockOnly) continue;
    if (!owners.has(reservation.ownerNonce) || !Array.isArray(reservation.memoryBindings) || reservation.memoryBindings.length > 128 || !amount(reservation.pendingBytes)) return { valid: false, reason: 'unobserved_memory_reservation' };
    pendingBytes += reservation.pendingBytes;
    for (const binding of reservation.memoryBindings) {
      const group = groups.get(binding.groupId);
      if (!group || group.shared || group.ownerNonce !== reservation.ownerNonce || !amount(binding.envelopeBytes) || claims.has(binding.groupId) || !Array.isArray(binding.processKeys) || binding.processKeys.length !== group.processKeys?.length || binding.processKeys.some((key, index) => key !== group.processKeys[index])) return { valid: false, reason: 'invalid_or_reused_memory_binding' };
      claims.add(binding.groupId); group.envelopeBytes = binding.envelopeBytes;
    }
  }
  return projectedMemory({ groups: [...groups.values()], pendingBytes, headroomBytes: 0, scope, now });
}

export function bindMemoryReservation(reservation, observation, { groupId, envelopeBytes, pendingBytes, now = Date.now() }) {
  if (!amount(pendingBytes) || pendingBytes > reservation.pendingBytes || !amount(envelopeBytes) || pendingBytes !== envelopeBytes) return { valid: false, reason: 'invalid_pending_transfer' };
  const group = observation?.groups?.find(row => row.id === groupId);
  if (!group || observation.ownerNonce !== reservation.ownerNonce || group.ownerNonce !== reservation.ownerNonce || (reservation.memoryBindings ?? []).some(binding => binding.groupId === groupId)) return { valid: false, reason: 'memory_binding_owner_or_identity' };
  const validation = projectedMemory({ groups: [{ ...group, envelopeBytes }], scope: observation.scope, now });
  if (!validation.valid) return validation;
  return { valid: true, reservation: { ...reservation, pendingBytes: reservation.pendingBytes - pendingBytes, memoryBindings: [...reservation.memoryBindings, { groupId, envelopeBytes, processKeys: [...group.processKeys] }] } };
}
