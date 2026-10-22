const FIELDS = ['input', 'output', 'cacheRead', 'cacheWrite', 'cost'];
const DETAILS_TYPE = 'pi-agent-child-usage';
const DETAILS_VERSION = 1;
const RUN_BINDING_TYPE = 'pi-agent-run-binding';
const MAX_SNAPSHOTS = 128;

export const CHILD_USAGE_ROWS = Symbol('pi-agent-child-usage-rows');

function amount(value) {
  return Number.isFinite(value) && value >= 0 ? value : undefined;
}

function taskId(row) {
  const value = row?.agentId ?? row?.id;
  return typeof value === 'string' && value ? value : undefined;
}

function usageSnapshot(row) {
  const agentId = taskId(row);
  if (!agentId) return undefined;
  const saved = row?.usage, progress = row?.progress?.usage;
  const snapshot = { agentId };
  let present = 0;
  for (const key of FIELDS) {
    const committed = amount(saved?.[key]), running = amount(progress?.[key]);
    if (committed === undefined && running === undefined) continue;
    snapshot[key] = (committed ?? 0) + (running ?? 0); present++;
  }
  return present ? snapshot : undefined;
}

function mergeSnapshot(accounted, snapshot) {
  const prior = accounted.get(snapshot.agentId) ?? {};
  const next = { ...prior };
  for (const key of FIELDS) {
    const value = amount(snapshot[key]);
    if (value !== undefined) next[key] = Math.max(amount(prior[key]) ?? 0, value);
  }
  accounted.set(snapshot.agentId, next);
  return next;
}

function storedSnapshots(details) {
  if (!details || typeof details !== 'object' || Array.isArray(details) || details.type !== DETAILS_TYPE || details.version !== DETAILS_VERSION || !Array.isArray(details.snapshots) || details.snapshots.length > MAX_SNAPSHOTS) return [];
  const snapshots = [];
  for (const row of details.snapshots) {
    if (!row || typeof row !== 'object' || Array.isArray(row) || typeof row.agentId !== 'string' || !row.agentId) continue;
    const snapshot = { agentId: row.agentId };
    let present = 0;
    for (const key of FIELDS) {
      const value = amount(row[key]);
      if (value !== undefined) { snapshot[key] = value; present++; }
    }
    if (present) snapshots.push(snapshot);
  }
  return snapshots;
}

export function attachChildUsageRows(result, rows) {
  if (!result || typeof result !== 'object' || !Array.isArray(rows) || !rows.length) return result;
  Object.defineProperty(result, CHILD_USAGE_ROWS, { value: rows, enumerable: false, configurable: false, writable: false });
  return result;
}

export function childUsageRows(result) {
  return result?.[CHILD_USAGE_ROWS] ?? [];
}

export function restoreChildUsage(entries) {
  const accounted = new Map();
  for (const entry of entries ?? []) {
    if (entry?.type === 'custom' && entry.customType === RUN_BINDING_TYPE) { accounted.clear(); continue; }
    const message = entry?.type === 'message' ? entry.message : undefined;
    if (message?.role !== 'toolResult' || message.toolName !== 'agent') continue;
    for (const snapshot of storedSnapshots(message.details)) mergeSnapshot(accounted, snapshot);
  }
  return accounted;
}

export function accountChildUsage(rows, accounted = new Map()) {
  const byAgent = new Map();
  for (const row of rows ?? []) {
    const snapshot = usageSnapshot(row);
    if (!snapshot) continue;
    const existing = byAgent.get(snapshot.agentId);
    if (!existing) byAgent.set(snapshot.agentId, snapshot);
    else {
      for (const key of FIELDS) {
        const value = amount(snapshot[key]);
        if (value !== undefined) existing[key] = Math.max(amount(existing[key]) ?? 0, value);
      }
    }
  }

  const delta = Object.fromEntries(FIELDS.map(key => [key, 0]));
  const snapshots = [];
  for (const snapshot of byAgent.values()) {
    const prior = accounted.get(snapshot.agentId) ?? {};
    const merged = mergeSnapshot(accounted, snapshot);
    const stored = { agentId: snapshot.agentId };
    for (const key of FIELDS) {
      const current = amount(merged[key]);
      if (current === undefined) continue;
      stored[key] = current;
      const before = amount(prior[key]) ?? 0;
      if (current > before) delta[key] += current - before;
    }
    snapshots.push(stored);
  }

  const details = snapshots.length ? { type: DETAILS_TYPE, version: DETAILS_VERSION, snapshots } : undefined;
  if (!FIELDS.some(key => delta[key] > 0)) return { details };
  const totalTokens = delta.input + delta.output + delta.cacheRead + delta.cacheWrite;
  return {
    details,
    usage: {
      input: delta.input,
      output: delta.output,
      cacheRead: delta.cacheRead,
      cacheWrite: delta.cacheWrite,
      totalTokens,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: delta.cost },
    },
  };
}
