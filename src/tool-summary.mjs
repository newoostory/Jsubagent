import { short } from './limits.mjs';

// A model-facing allowlist. Operational metadata remains available through
// status(format:full) and /hub rather than expanding with future engine fields.
export function summarizeAgent(row) {
  const output = Object.fromEntries(['agentId', 'name', 'role', 'state', 'settled', 'turns', 'maxTurns'].filter(key => row[key] !== undefined).map(key => [key, row[key]]));
  for (const key of ['waitingFor', 'blockedBy', 'attention']) if (row[key]?.length) output[key] = row[key];
  for (const key of ['resultAvailable', 'errorAvailable', 'handoffAvailable', 'responseReceived', 'responseComplete']) if (row[key] !== undefined && row[key] !== false) output[key] = row[key];
  if (row.responseReceived && row.responseComplete === false) output.responseComplete = false;
  for (const key of ['bundleRef', 'stage', 'outputComplete', 'readiness', 'currentExecution', 'currentProductVerification', 'dataReadiness', 'resultRoutes', 'admission', 'quality']) if (row[key] !== undefined) output[key] = row[key];
  if (row.response) output.response = row.response;
  if (row.responseError) output.responseError = row.responseError;
  if (row.read) output.read = row.read;
  else if (!row.response && row.settled) {
    const current = row.currentReport && row.currentReport === row.latestReport;
    output.read = { action: 'read', requests: [{ agentId: row.agentId, ...(current ? { source: row.state !== 'idle' && row.errorAvailable ? 'error' : 'result', artifactId: row.currentReport } : { source: 'index' }) }] };
  }
  if (row.progress) {
    const progress = {};
    for (const key of ['turns', 'contextTokens', 'contextWindow', 'bytes', 'elapsedMs']) if (Number.isFinite(row.progress[key])) progress[key] = row.progress[key];
    if (typeof row.progress.command === 'string') progress.command = short(row.progress.command, 160);
    if (Object.keys(progress).length) output.progress = progress;
  }
  return output;
}

function selections(agents) {
  return {
    settledAgentIds: agents.filter(row => row.settled === true).map(row => row.agentId),
    remainingAgentIds: agents.filter(row => row.settled !== true).map(row => row.agentId),
    attentionAgentIds: agents.filter(row => row.attention?.length).map(row => row.agentId),
  };
}

export function summarizeStatus(value, { targeted = false } = {}) {
  const rows = value.agents ?? [value];
  const agents = rows.map(summarizeAgent);
  return { agents, ...selections(agents), ...(targeted || value.concurrency === undefined ? {} : { active: value.active, concurrency: value.concurrency, pressure: value.pressure }) };
}

export function summarizeWait(rows, metadata, params) {
  const agents = rows.map(summarizeAgent), selected = selections(agents);
  const output = { reason: metadata.reason, conditionMet: metadata.conditionMet, snapshotAt: metadata.snapshotAt, stale: metadata.stale, agents, ...selected };
  if (metadata.stale) {
    output.next = { action: 'status', agentIds: rows.map(row => row.agentId), format: 'summary' };
    return output;
  }
  // Resolve questions before recommending another wait on those same messages.
  const requests = [], seen = new Set();
  for (const row of rows) for (const item of row.attention ?? []) for (const read of item.read.requests) {
    const key = JSON.stringify(read);
    if (!seen.has(key) && requests.length < 8) { requests.push(read); seen.add(key); }
  }
  if (requests.length) output.next = { action: 'read', requests };
  else if (selected.remainingAgentIds.length) output.next = {
    action: 'wait', agentIds: selected.remainingAgentIds, format: 'summary',
    mode: params.mode ?? 'all', results: params.results ?? 'new', wakeOnMessage: params.wakeOnMessage ?? true,
  };
  return output;
}
