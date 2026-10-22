import { SessionManager } from '@earendil-works/pi-coding-agent';
import { fail, LIMITS } from '../limits.mjs';
import { groupContextMessages, measureResidentContext } from './records.mjs';
import { restoreSessionMessages } from './restore.mjs';

// Only private, in-memory worker sessions. The complete record already lives in
// RunStore; a user-facing persisted Pi tree must never be reset by this helper.
export async function rebaseCompactedSession({ session, messages = session.messages, persist, model, thinkingLevel }) {
  const manager = session.sessionManager;
  if (!manager || manager.isPersisted() || typeof persist !== 'function') throw fail('Compaction rebase requires an in-memory session and durable commit', 'STORAGE');
  if (!messages.some(message => message.role === 'compactionSummary')) throw fail('Compaction rebase requires a compacted projection', 'STORAGE');
  groupContextMessages(messages);
  if (measureResidentContext(messages) > LIMITS.contextBytes) throw fail('Compacted projection exceeds the context budget', 'LIMIT');
  const candidate = SessionManager.inMemory(manager.getCwd());
  const append = (target, projection) => {
    if (model) target.appendModelChange(model.provider, model.id);
    if (thinkingLevel) target.appendThinkingLevelChange(thinkingLevel);
    restoreSessionMessages(target, projection);
  };
  append(candidate, messages);
  const projection = candidate.buildSessionContext().messages;
  // A failed write leaves the old manager and all of its references untouched.
  await persist(projection);
  const previousEntries = manager.getEntryCount(), id = manager.getSessionId();
  manager.newSession({ id });
  append(manager, projection);
  session.refreshContext();
  return { previousEntries, residentEntries: manager.getEntryCount(), sessionId: id };
}
