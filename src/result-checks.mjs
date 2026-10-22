import { encodeJson, fail, identifier, text } from './limits.mjs';
import { effectInputDigest } from './execution/tool-journal.mjs';

// These definitions come from the accepted main spawn request, not a producer
// report or verification-plan artifact. They grant no tools or automatic runs.
export function normalizeVerificationChecks(values = []) {
  if (!Array.isArray(values) || values.length > 16) throw fail('verificationChecks must contain at most sixteen checks');
  const ids = new Set(), digests = new Set();
  const checks = values.map(value => {
    if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).some(key => !['id', 'command', 'timeoutMs'].includes(key))) throw fail('Invalid verification check fields');
    const id = identifier(value.id, 'verification check'), command = text(value.command, 'verification command', 8192);
    if (value.timeoutMs !== undefined && (!Number.isSafeInteger(value.timeoutMs) || value.timeoutMs < 1 || value.timeoutMs > 86400000)) throw fail('Invalid verification check timeout');
    const check = { id, command, ...(value.timeoutMs === undefined ? {} : { timeoutMs: value.timeoutMs }) };
    const digest = checkInputDigest(check);
    if (ids.has(id) || digests.has(digest)) throw fail('Duplicate verification check identity or command');
    ids.add(id); digests.add(digest); return check;
  });
  encodeJson(checks, 24576); return checks;
}
export function checkInputDigest({ command, timeoutMs }) { return effectInputDigest({ command, ...(timeoutMs === undefined ? {} : { timeoutMs }) }); }
export function requiredCheckIdentities(task) { return (task.verificationChecks ?? []).map(check => ({ id: check.id, inputDigest: checkInputDigest(check) })); }
export function verificationAssignment(taskText, checks) {
  if (!checks.length) return taskText;
  return `${taskText}\n\nApproved required verification checks (exact bash arguments; no new tool authority):\n${JSON.stringify(checks.map(({ id, ...arguments_ }) => ({ id, tool: 'bash', arguments: arguments_ })))}\nRun against the runtime-materialized pinned product input in your assigned cwd. Do not repair product/tests to obtain PASS. Report failed, not_run or inconclusive checks honestly. A check receipt does not authorize merge/deploy or claim semantic acceptance.`;
}
