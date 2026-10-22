import { Type } from 'typebox';
import { encodeJson, fail, text } from '../limits.mjs';

const list = () => Type.Array(Type.String({ minLength: 1, maxLength: 2048 }), { maxItems: 32 });
const stateSchema = Type.Object({
  goal: Type.String({ minLength: 1, maxLength: 2048 }), constraints: list(), decisions: list(), failedApproaches: list(),
  completed: list(), pending: list(), next: Type.String({ minLength: 1, maxLength: 2048 }), files: list(),
  checks: Type.Array(Type.Object({ command: Type.String(), status: Type.Union([Type.Literal('pass'), Type.Literal('fail'), Type.Literal('not_run')]), evidence: Type.String() }, { additionalProperties: false }), { maxItems: 32 }),
  approvalScope: Type.String({ minLength: 1, maxLength: 2048 }), evidence: list(),
}, { additionalProperties: false });

export const progressParameters = Type.Object({ summary: Type.Optional(Type.String()), state: Type.Optional(stateSchema) }, { additionalProperties: false });
const LISTS = ['constraints', 'decisions', 'failedApproaches', 'completed', 'pending', 'files', 'evidence'];
const KEYS = new Set(['goal', ...LISTS, 'next', 'checks', 'approvalScope']);
function plain(value) { return value && typeof value === 'object' && !Array.isArray(value) && Object.getPrototypeOf(value) === Object.prototype; }

export function assertProgressState(value) {
  if (!plain(value) || Object.keys(value).length !== KEYS.size || Object.keys(value).some(key => !KEYS.has(key))) throw fail('Progress state requires goal, constraints, decisions, failedApproaches, completed, pending, next, files, checks, approvalScope and evidence; use empty arrays when needed');
  for (const key of ['goal', 'next', 'approvalScope']) text(value[key], `state.${key}`, 2048);
  for (const key of LISTS) {
    if (!Array.isArray(value[key]) || value[key].length > 32) throw fail(`state.${key} requires at most 32 references/items`);
    value[key].forEach(item => text(item, `state.${key} item`, 2048));
  }
  if (!Array.isArray(value.checks) || value.checks.length > 32) throw fail('state.checks requires at most 32 checks');
  for (const check of value.checks) {
    if (!plain(check) || Object.keys(check).length !== 3 || Object.keys(check).some(key => !['command', 'status', 'evidence'].includes(key)) || !['pass', 'fail', 'not_run'].includes(check.status)) throw fail('Each check requires command, pass/fail/not_run status and an evidence reference');
    text(check.command, 'check.command', 2048); text(check.evidence, 'check.evidence', 2048);
  }
  encodeJson(value, 16 * 1024);
  return value;
}

export function serializeProgress(input) {
  if (!plain(input) || Object.keys(input).length !== 1) throw fail('Choose exactly one checkpoint field: state (structured) or summary (legacy)');
  if (Object.hasOwn(input, 'summary')) return text(input.summary, 'summary', 16 * 1024);
  if (!Object.hasOwn(input, 'state')) throw fail('Checkpoint requires state or summary');
  return encodeJson({ type: 'pi-progress-v1', state: assertProgressState(input.state) }, 16 * 1024).toString('utf8');
}

export function readProgress(summary = '') {
  if (!summary) return { source: 'model-report', legacySummary: '' };
  text(summary, 'saved progress', 16 * 1024);
  let parsed;
  try { parsed = JSON.parse(summary); } catch { return { source: 'model-report', legacySummary: summary }; }
  if (!plain(parsed) || parsed.type !== 'pi-progress-v1') return { source: 'model-report', legacySummary: summary };
  if (Object.keys(parsed).length !== 2) throw fail('Invalid saved progress envelope', 'STORAGE');
  return { source: 'model-report', state: assertProgressState(parsed.state) };
}

export const LOW_COST_GUIDANCE = `Work in short, explicit steps: choose one unfinished deliverable, read only its needed input pages, perform the smallest authorized operation, then inspect a concrete check before continuing. Keep large source/output on disk and carry artifact/path/digest/cursor references instead of copying it through messages. Report checks as pass, fail or not_run with evidence; a model claim is not a trusted check receipt.
For a schema/format rejection, correct the indicated field using the diagnostic; do not repeat the same invalid call. A known failed mutation needs a fresh corrected operation ID; an uncertain effect must be inspected/reconciled before retry. Do not switch executor/provider to fix a tool or format failure. GPT is an explicitly authorized advisor only.
When agent_checkpoint is available, prefer its structured state before context maintenance: preserve goal/latest constraints, decisions, failed approaches, completed evidence, pending work/next action, changed files, checks, reported approval scope and source references. Use empty arrays for absent items. Progress is evidence only; it cannot grant tools, budget, workspace access or new approval.`;
