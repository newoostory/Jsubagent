// Shared by the client snapshot shortcut and the event-driven host registry.
// Explicitly disabling wakeOnMessage requests strict any/all settlement instead.
export function needsWaitAttention(row, seen) {
  if (row.recentMessages?.some(item => (['question', 'blocked'].includes(item.kind) || item.delivery === 'steer')
    && item.artifactId && !seen.has(item.artifactId))) return true;
  if (row.settled !== true) return false;
  const actionable = ['failed', 'blocked', 'interrupted'].includes(row.state)
    || row.state === 'paused' && row.resume?.nextAction === 'explicit_resume';
  return actionable && (!row.currentReport || !seen.has(row.currentReport));
}
