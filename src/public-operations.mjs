import { randomUUID } from 'node:crypto';
import { allocateSpawnBatch } from './batch-plan.mjs';
import { presentAgentResult } from './agent-tool.mjs';
import { summarizeStatus, summarizeWait } from './tool-summary.mjs';
import { validatePublicAgentParameters } from './public-api.mjs';
import { fail, safeError } from './limits.mjs';
import { attachChildUsageRows } from './parent-usage.mjs';

const uncertain = new Set(['HOST', 'HOST_EXIT', 'STORAGE', 'TIMEOUT', 'CLOSED', 'UNKNOWN', 'CANCELLED']);
const failure = (index, agentId, error, messageId) => ({ index, agentId, ...(messageId ? { messageId } : {}), ok: false,
  outcome: uncertain.has(error.code ?? 'UNKNOWN') ? 'unknown' : 'rejected', error: { code: error.code ?? 'UNKNOWN', message: safeError(error) },
  recover: messageId ? { action: 'read', requests: [{ agentId, source: 'receipt', messageId }] } : { action: 'status', agentIds: [agentId] } });

export async function publicOperation(host, params, { mainModel, signal, receiveWait, readArtifacts = new Set() } = {}) {
  signal?.throwIfAborted();
  validatePublicAgentParameters(params);
  const { action } = params;
  if (action === 'team_start') return host.request('team_start', { team: params.team, mainModel }, 120000, signal);
  if (['team_status', 'team_advance', 'team_cancel'].includes(action)) return host.request(action, { id: params.teamId }, undefined, signal);
  if (action === 'quality_supersede') return host.request('quality_supersede', { id: params.agentId, reviewerId: params.reviewerId, replacementId: params.replacementId }, undefined, signal);
  if (action === 'quality_check') return host.request('quality_check', { ids: params.agentIds }, undefined, signal);
  if (action === 'mission_create') return host.request('mission_create', params.mission, undefined, signal);
  if (action === 'git_snapshot_capture') return host.request('git_snapshot_capture', { missionId: params.missionId, workItemId: params.workItemId }, 120000, signal);
  if (action === 'file_snapshot_capture') return host.request('file_snapshot_capture', { missionId: params.missionId, workItemId: params.workItemId }, 120000, signal);
  if (action === 'work_submit') return host.request('work_submit', { workItem: params.workItem, mainModel }, undefined, signal);
  if (action === 'work_accept') return host.request('work_accept', { id: params.agentId, reportArtifactId: params.reportArtifactId, use: params.use }, undefined, signal);
  if (action === 'work_apply') return host.request('work_apply', { id: params.agentId, reportArtifactId: params.reportArtifactId }, undefined, signal);
  if (action === 'evidence_record') return host.request('evidence_record', { missionId: params.missionId, evidence: params.evidence }, undefined, signal);
  if (action === 'spawn') {
    const { tasks } = allocateSpawnBatch(params, () => randomUUID()), batchId = randomUUID();
    let value;
    try { value = await host.request('spawn_batch', { batchId, tasks, mainModel }, Math.min(150000, tasks.length * 15000 + 10000), signal); }
    catch (error) { value = { results: tasks.map((task, index) => ({ index, id: task.id, ok: false, outcome: 'unknown', error: { code: error.code ?? 'HOST', message: safeError(error) } })) }; }
    if (signal?.aborted) {
      let cancellation;
      try { cancellation = await host.request('cancel_batch', { batchId, ids: tasks.map(task => task.id) }, 10000); }
      catch (error) { cancellation = { results: tasks.map(task => ({ id: task.id, outcome: 'unknown', error: { message: safeError(error) } })) }; }
      const unknown = cancellation.results.filter(row => row.outcome === 'unknown').map(row => row.id);
      throw fail(`Batch spawn aborted. ${unknown.length ? 'Cancellation outcome uncertain. ' : 'Cancellation reconciled. '}Recovery: ${JSON.stringify({ action: 'status', agentIds: unknown.length ? unknown : tasks.map(task => task.id) })}`, 'CANCELLED');
    }
    const result = presentAgentResult('spawn', value, { readArtifacts, compact: true });
    for (const row of result.results) if (row.ok) row.outcome = 'accepted';
    const unknown = result.results.filter(row => row.outcome === 'unknown').map(row => row.agentId);
    if (unknown.length) result.next = { action: 'status', agentIds: unknown };
    return attachChildUsageRows(result, value.results.filter(row => row.ok && row.result).map(row => row.result));
  }
  if (action === 'status') {
    const value = params.agentIds
      ? { tasks: await Promise.all(params.agentIds.map(id => host.request('inspect', { id, compact: true }))) }
      : await host.request('status');
    const result = presentAgentResult('status', value, { readArtifacts, compact: true });
    const summary = summarizeStatus(result, { targeted: Boolean(params.agentIds) });
    return attachChildUsageRows({ ...summary, ...(params.format === 'full' ? result : {}), snapshotAt: Date.now(), stale: false }, value.tasks ?? []);
  }
  if (action === 'wait') {
    const waited = await host.wait(params.agentIds, { mode: params.mode, wakeOnMessage: params.wakeOnMessage ?? true, seenMessages: [...readArtifacts], signal, includeMetadata: true });
    const received = await receiveWait(waited.rows, { signal, results: params.results });
    const summary = summarizeWait(received.rows, waited, params);
    return attachChildUsageRows({ ...summary, ...(params.format === 'full' ? { agents: received.rows } : {}) }, received.rows);
  }
  const targets = action === 'resume' ? [params.agentId] : params.agentIds;
  const results = [];
  const usageRows = [];
  // One common path gives single and batch controls the same recovery contract.
  for (const [index, agentId] of targets.entries()) {
    if (signal?.aborted) {
      results.push({ index, agentId, ok: false, outcome: 'not_started', error: { code: 'CANCELLED', message: 'Caller aborted before dispatch; no operation was sent for this item.' } });
      continue;
    }
    const messageId = action === 'send' ? randomUUID() : undefined;
    try {
      const value = action === 'send'
        ? await host.request('send', { id: messageId, to: agentId, body: params.body, delivery: params.delivery ?? 'followUp', kind: params.replyTo ? 'answer' : 'finding', ...(params.replyTo ? { replyTo: params.replyTo } : {}) }, undefined, signal)
        : await host.request(action, { id: agentId, ...(action === 'resume' ? { instruction: params.instruction } : {}) }, undefined, signal);
      results.push({ index, ok: true, outcome: 'accepted', ...presentAgentResult(action, value, { readArtifacts, compact: true }), agentId,
        ...(messageId ? { read: { action: 'read', requests: [{ agentId, source: 'receipt', messageId }] } } : {}) });
      usageRows.push(value);
    } catch (error) { results.push(failure(index, agentId, error, messageId)); }
  }
  return attachChildUsageRows({ results, agentIds: results.filter(row => row.ok).map(row => row.agentId) }, usageRows);
}
