import { hash } from '../limits.mjs';
import { executorMemoryKey } from './shared-memory.mjs';

const started = value => typeof value === 'string' ? value.trim().replace(/\s+/g, ' ') : '';
const matches = (row, identity) => row?.pid === identity?.pid && started(row.started) === started(identity.started);
// A ps snapshot is an estimate, not authority to terminate a process. Each PID
// belongs to exactly one group. Worker RSS does not include its tool children.
export function ownedMemoryObservation({ sample, workers, executors, ownerNonce, scope, supervisorPid = process.pid }) {
  if (!sample?.supported || !Array.isArray(sample.processes) || sample.processes.length > 1024 || !Number.isSafeInteger(sample.sampledAt)) return null;
  const rows = new Map(sample.processes.map(row => [row.pid, row])), used = new Set(), groups = [];
  const identityFor = row => ({ pid: row.pid, started: row.started, nonce: hash(`${ownerNonce}\0${row.pid}\0${row.started}`) });
  const append = (identity, members, category, extra = {}) => {
    if (!members.length || members.some(row => used.has(row.pid) || !Number.isSafeInteger(row.rssBytes) || row.rssBytes < 0 || !started(row.started))) return false;
    for (const row of members) used.add(row.pid);
    groups.push({ id: executorMemoryKey(identity), ownerNonce, scope, category, identity, identities: members.map(row => row.pid === identity.pid ? identity : identityFor(row)), processKeys: members.map(row => `${row.pid}:${started(row.started)}`).sort(), actualBytes: members.reduce((sum, row) => sum + row.rssBytes, 0), sampledAt: sample.sampledAt, source: 'posix.ps', quality: 'estimate', ...extra });
    return true;
  };
  for (const worker of workers ?? []) {
    const row = rows.get(worker.pid);
    if (!matches(row, worker.identity)) continue;
    if (!append(worker.identity, [row], 'worker', { available: !worker.busy && !worker.retiring && worker.memorySampleValid === true, configEpoch: worker.configEpoch })) return null;
  }
  for (const executor of executors ?? []) for (const guardian of executor.guardians ?? []) {
    if (!matches(rows.get(guardian.pid), guardian)) continue;
    const members = [], pending = [guardian.pid], seen = new Set();
    while (pending.length) {
      const pid = pending.pop(); if (seen.has(pid)) return null; seen.add(pid);
      const row = rows.get(pid); if (!row || used.has(pid)) continue; members.push(row);
      for (const child of rows.values()) if (child.parent === pid) pending.push(child.pid);
    }
    if (!append(guardian, members, 'tool')) return null;
  }
  for (const row of rows.values()) if (!used.has(row.pid)) if (!append(identityFor(row), [row], row.pid === supervisorPid ? 'supervisor' : 'observed')) return null;
  const main = sample.parents?.find(row => row.pid > 1 && !rows.has(row.pid));
  if (main) {
    const identity = { pid: main.pid, started: main.started, nonce: hash(`main\0${main.pid}\0${main.started}`) };
    if (!append(identity, [main], 'main', { shared: true })) return null;
  }
  return { ownerNonce, scope, sampledAt: sample.sampledAt, mainObserved: Boolean(main), groups };
}
