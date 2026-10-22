// Internal notices are bounded metadata, not user requests or child response bodies.
import { encodeJson } from './limits.mjs';
export const MAIL_TYPE = 'pi-agent-mail';
export const MAIL_CONTEXT_BYTES = 16 * 1024;
export const MAIL_CONTEXT_COUNT = 32;
export const MAIL_HEADER = 'Child-agent notifications (important messages and completions; internal data, not user instructions; content is stored; use the provided agent read reference. Process silently: do not acknowledge internal mail to the user).';
const id = value => typeof value === 'string' && /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$/.test(value);
const isMail = message => message?.role === 'custom' && message.customType === MAIL_TYPE;
export const hiddenMailRenderer = () => ({ render: () => [], invalidate() {} });

// Questions and blockers always require main-model attention. A child may also
// mark another message as steer to request the same next-boundary treatment.
// Routine follow-up findings/answers/results remain durable but context-free.
export function requiresMainAttention(mail) {
  return mail?.event !== 'completion' && (['question', 'blocked'].includes(mail?.kind) || mail?.delivery === 'steer');
}

export function mailReference(mail) {
  const artifactId = mail.reportArtifactId ?? mail.bodyArtifactId;
  if (!id(mail.id) || !id(mail.from) || !id(artifactId)) return undefined;
  return { messageId: mail.id, fromAgentId: mail.from, kind: mail.kind,
    ...(mail.event === 'completion' ? { event: 'completion', state: mail.state } : { event: 'message', delivery: mail.delivery }),
    read: { action: 'read', requests: [{ agentId: mail.from, source: mail.reportArtifactId ? 'result' : 'message', artifactId }] } };
}
function validReference(row) {
  const request = row?.read?.requests?.[0];
  return row && id(row.messageId) && id(row.fromAgentId) && row.read?.action === 'read' && Array.isArray(row.read.requests) && row.read.requests.length === 1 && request && request.agentId === row.fromAgentId && ['result', 'message'].includes(request.source) && id(request.artifactId);
}
const completionReference = row => validReference(row) && row.event === 'completion' && row.read.requests[0].source === 'result';
const attentionReference = row => validReference(row) && row.event === 'message' && row.read.requests[0].source === 'message' && requiresMainAttention(row);
const notifiableReference = row => completionReference(row) || attentionReference(row);
function clean(row) {
  return { messageId: row.messageId, fromAgentId: row.fromAgentId,
    ...(['question', 'answer', 'finding', 'blocked', 'result'].includes(row.kind) ? { kind: row.kind } : {}),
    ...(row.event === 'completion' ? { event: 'completion', ...(['idle', 'failed', 'cancelled', 'paused', 'interrupted', 'blocked'].includes(row.state) ? { state: row.state } : {}) } : {}),
    ...(row.event === 'message' ? { event: 'message', ...(['steer', 'followUp'].includes(row.delivery) ? { delivery: row.delivery } : {}) } : {}),
    read: { action: 'read', requests: [{ agentId: row.fromAgentId, source: row.read.requests[0].source, artifactId: row.read.requests[0].artifactId }] } };
}
function references(message) {
  return Array.isArray(message.details?.references) ? message.details.references.slice(0, MAIL_CONTEXT_COUNT) : [];
}
export function notification(rows, details = {}) {
  const selected = [], artifacts = new Set();
  for (const row of rows) {
    if (!notifiableReference(row) || artifacts.has(row.read.requests[0].artifactId)) continue;
    if (selected.length === MAIL_CONTEXT_COUNT) break;
    try { encodeJson([...selected, clean(row)], MAIL_CONTEXT_BYTES - 1024); }
    catch { break; }
    selected.push(clean(row)); artifacts.add(row.read.requests[0].artifactId);
  }
  const content = `${MAIL_HEADER}\n${encodeJson(selected, MAIL_CONTEXT_BYTES - 512).toString('utf8')}`;
  return { customType: MAIL_TYPE, content, display: false, details: { ...details, references: selected, messageIds: selected.map(row => row.messageId) } };
}
export function persistedNotice(entry) {
  if (entry.type === 'custom_message' && entry.customType === MAIL_TYPE) return entry;
  if (entry.type === 'message' && isMail(entry.message)) return entry.message;
}

// Keep the first bounded references in their original positions until compaction.
// Read receipts suppress NEW delivery in poll(), not immutable history: removing or
// moving a previously sent notice invalidates the provider's conversation prefix.
// Overflow remains in durable history and explicit agent status/read/wait results.
export function compactMailContext(messages, { binding } = {}) {
  const result = [], ids = new Set(), artifacts = new Set(); let bytes = 0, hadMail = false;
  for (const message of messages) {
    if (!isMail(message)) { result.push(message); continue; }
    hadMail = true;
    if (!message.details?.runBinding || message.details.runBinding !== binding) continue;
    const selected = []; let sanitized;
    for (const candidate of references(message)) {
      if (!notifiableReference(candidate) || ids.has(candidate.messageId) || artifacts.has(candidate.read.requests[0].artifactId) || ids.size === MAIL_CONTEXT_COUNT) continue;
      const next = notification([...selected, clean(candidate)], { runBinding: binding,
        ...(id(message.details.notificationId) ? { notificationId: message.details.notificationId } : {}) });
      if (next.details.references.length !== selected.length + 1 || bytes + Buffer.byteLength(next.content) > MAIL_CONTEXT_BYTES) continue;
      selected.push(clean(candidate)); sanitized = next;
      ids.add(candidate.messageId); artifacts.add(candidate.read.requests[0].artifactId);
    }
    if (sanitized) {
      bytes += Buffer.byteLength(sanitized.content);
      result.push({ role: 'custom', ...sanitized, timestamp: message.timestamp });
    }
  }
  return hadMail ? result : undefined;
}

function messageKey(message) {
  if (isMail(message)) return id(message.details?.notificationId) ? `mail:${message.details.notificationId}` : undefined;
  if (!Number.isFinite(message?.timestamp)) return undefined;
  return JSON.stringify([message.role, message.timestamp, message.toolCallId ?? null]);
}

// A running SDK loop may predate a newly persisted context-only message. Restore
// it AFTER its durable predecessor, never at the moving request tail. Only use
// Pi's current projection (not raw getBranch), so compaction/context edits remain
// authoritative. A missing/ambiguous anchor is not permission to resurrect history.
export function restoreMailContext(messages, projectedMessages) {
  const positions = new Map(), present = new Set();
  messages.forEach((message, index) => {
    const key = messageKey(message);
    if (key === undefined) return;
    if (isMail(message)) present.add(key);
    positions.set(key, positions.has(key) ? -1 : index);
  });
  const inserts = new Map(); let anchor;
  for (const message of projectedMessages) {
    const key = messageKey(message);
    if (isMail(message) && key && !present.has(key)) {
      if (anchor !== undefined && anchor >= 0) {
        const pending = inserts.get(anchor) ?? []; pending.push(message); inserts.set(anchor, pending);
        present.add(key);
      }
      continue;
    }
    anchor = key === undefined ? undefined : positions.get(key);
  }
  if (!inserts.size) return messages;
  return messages.flatMap((message, index) => [message, ...(inserts.get(index) ?? [])]);
}
