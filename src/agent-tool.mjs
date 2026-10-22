import { short } from './limits.mjs';

// Project host/storage identities without changing persisted IDs or evidence.
export function presentAgentMail(mail) {
  const { id, from, to, body, ...rest } = mail;
  const artifactId = mail.reportArtifactId ?? mail.bodyArtifactId;
  return { messageId: id, fromAgentId: from, toAgentId: to, ...rest,
    ...(from === 'main' && to !== 'main' && body !== undefined ? { body } : {}),
    ...(artifactId ? { read: { action: 'read', requests: [{ agentId: from === 'main' ? to : from, source: mail.reportArtifactId ? 'result' : 'message', artifactId }] } } : {}) };
}
function presentAgent(record, { readArtifacts = new Set(), compact = false } = {}) {
  const { id, pendingMail, uncertainMail, result, error, handoff, dependencyInputs, recentMessages, ...rest } = record;
  // Persisted message references survive inbox ack. Expose only unread recent
  // questions/blockers, never bodies or an inaccurate all-history unread count.
  const attention = (recentMessages ?? []).filter(row => ['question', 'blocked'].includes(row.kind) && row.artifactId && !readArtifacts.has(row.artifactId)).slice(-8)
    .map(({ artifactId, messageId, kind }) => ({ kind, messageId, read: { action: 'read', requests: [{ agentId: id, source: 'message', artifactId }] } }));
  const mail = row => { const view = presentAgentMail(row); if (compact) delete view.body; return view; };
  return { agentId: id, ...rest, ...(compact && typeof rest.task === 'string' ? { task: short(rest.task, 256) } : {}), resultAvailable: Boolean(record.resultAvailable) || Boolean(result) || Boolean(record.state === 'idle' && record.currentReport && record.currentReport === record.latestReport), errorAvailable: Boolean(record.errorAvailable) || Boolean(error), handoffAvailable: Boolean(record.handoffAvailable) || Boolean(handoff), ...(attention.length ? { attention } : {}), ...(pendingMail ? { pendingMail: pendingMail.map(mail) } : {}), ...(uncertainMail ? { uncertainMail: uncertainMail.map(mail) } : {}) };
}
export function presentAgentResult(action, result, options) {
  const present = row => presentAgent(row, options);
  if (action === 'spawn' && Array.isArray(result.results)) {
    const results = result.results.map(row => row.ok
      ? { index: row.index, ok: true, ...presentAgentResult(action, row.result, options), ...(row.cancellationRequested ? { cancellationRequested: true } : {}), ...(row.cancellationError ? { cancellationError: row.cancellationError, cancellationOutcome: row.cancellationOutcome, recover: { action: 'status', agentIds: [row.id] } } : {}) }
      : { index: row.index, agentId: row.id, ok: false, outcome: row.outcome, error: row.error, ...(row.blockedBy?.length ? { blockedBy: row.blockedBy } : {}),
        ...(row.outcome === 'unknown' ? { recover: { action: 'status', agentIds: [row.id] } } : {}) });
    const agentIds = results.filter(row => row.ok).map(row => row.agentId);
    return { results, agentIds, ...(agentIds.length ? { next: { action: 'wait', agentIds, mode: 'any', format: 'summary', wakeOnMessage: true } } : {}) };
  }
  if (action === 'wait') return result.map(present);
  if (action === 'status' && Array.isArray(result.tasks)) {
    const { tasks, ...rest } = result; return { ...rest, agents: tasks.map(present) };
  }
  if (['spawn', 'status', 'cancel', 'resume', 'archive'].includes(action)) return present(result);
  if (action === 'send') { const { id, ...rest } = result; return { messageId: id, ...rest }; }
  return result;
}
