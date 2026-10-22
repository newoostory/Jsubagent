import { fail } from '../limits.mjs';

export function restoreSessionMessages(sessionManager, messages) {
  if (!sessionManager || typeof sessionManager.appendMessage !== 'function' || !Array.isArray(messages)) throw fail('Invalid checkpoint restore target', 'STORAGE');
  for (const message of messages) {
    if (message.role === 'compactionSummary') {
      sessionManager.appendCompaction(message.summary, null, message.tokensBefore);
    } else if (message.role === 'branchSummary') {
      sessionManager.branchWithSummary(sessionManager.getLeafId(), message.summary);
    } else {
      sessionManager.appendMessage(message);
    }
  }
}
