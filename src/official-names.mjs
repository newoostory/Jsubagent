import { Type } from 'typebox';
import { Check } from 'typebox/value';
import { fail } from './limits.mjs';
import { checkpointRecordCount } from './state/checkpoint.mjs';
import { modelGuidance } from './model-guidance.mjs';

export const OFFICIAL_ROLE = 'trans-official-names-researcher';
export const isOfficial = task => task.role.name === OFFICIAL_ROLE || task.role.inputMode === 'official-names';
export const isSourceResearch = task => ['trans-names-researcher', 'trans-context-researcher', 'trans-register-researcher', 'trans-voice-researcher', 'trans-structure-researcher'].includes(task.role.name);
export const isTrans = task => isOfficial(task) || isSourceResearch(task) || ['translator', 'translation-reviewer'].includes(task.role.name);
// Canonical result storage is lossless. Bounds apply to previews, provider turns
// and workflow section commits, never to a byte prefix relabeled as complete.
export function boundedTaskResult(task, raw) {
  if (typeof raw !== 'string') throw fail('Task result must be text', 'LIMIT');
  return raw;
}
const currentNamesReport = parent => parent && (Object.hasOwn(parent, 'currentReport') ? parent.currentReport : parent.latestReport);
function settledNames(parent, running) {
  const p = parent?.profile;
  const supported = p && ((p.provider === 'openai-codex' && p.model === 'gpt-5.6-luna' && p.thinkingLevel === 'max') || (p.provider === 'camel-stream' && p.model === 'auto' && p.thinkingLevel === 'high'));
  return parent && parent.role.name === 'trans-names-researcher' && p.name === 'trans-names-researcher' && supported && parent.state === 'idle' && currentNamesReport(parent) && currentNamesReport(parent) === parent.latestReport && !running.has(parent.id);
}
export function officialDependenciesFresh(task, tasks, running) {
  if (!isOfficial(task)) return true;
  // Bounded batches pin one Camel names report. Earlier Luna and two-Camel
  // snapshots remain readable; all current report pins must stay fresh.
  const ids = task.dependsOn ?? [];
  const pins = task.officialDependencyReports ?? (ids.length === 1 && task.officialDependencyReport ? [{ agentId: ids[0], reportArtifactId: task.officialDependencyReport }] : []);
  return ids.length > 0 && ids.length <= 2 && new Set(ids).size === ids.length && pins.length === ids.length && ids.every(id => {
    const pin = pins.filter(row => row.agentId === id), parent = tasks.get(id);
    return pin.length === 1 && settledNames(parent, running) && pin[0].reportArtifactId === currentNamesReport(parent);
  });
}
const clean = () => Type.String({ minLength: 1, maxLength: 160, pattern: '\\S' });
const candidateId = () => Type.String({ pattern: '^c[0-9]{4}$' });
const opaqueSegmentId = '[a-zA-Z][a-zA-Z0-9_-]{0,63}';
const encodedPathPart = '(?:[a-zA-Z0-9._~-]|%[0-9A-F]{2})+';
const sourceSegmentId = `${encodedPathPart}(?:/${encodedPathPart})*#seg-[0-9]{6,}`;
const segmentId = () => Type.String({ maxLength: 4096, pattern: `^(?:${opaqueSegmentId}|${sourceSegmentId})$` });
const CapsuleSchema = Type.Object({ candidates: Type.Array(Type.Object({
  candidate_id: candidateId(), source_form: clean(), entity_key: clean(), form_type: clean(),
  evidence: Type.Array(Type.Object({ segment_id: segmentId(), quote: clean() }, { additionalProperties: false }), { minItems: 1, maxItems: 10 }),
}, { additionalProperties: false }), { minItems: 1 }) }, { additionalProperties: false });

// Conservative rejection, not a content/DLP oracle: ambiguous inputs fail closed.
// Source binding/completeness and disclosure approval remain controller duties.
function cleanSurface(value) {
  if (value.trim() !== value || /[\\/\\\\:@<>%\u0000-\u001f\u007f-\u009f\u200b-\u200f\u202a-\u202e\u2066-\u2069]/u.test(value) || /[a-f0-9]{32,}|\.[a-z0-9]{2,8}\b|__[^ ]+__/iu.test(value)) throw fail('Official-name capsule contains forbidden or ambiguous metadata', 'PRIVACY');
}

function validateSegmentId(value) {
  if (value.trim() !== value) throw fail('Official-name capsule contains an invalid source segment locator', 'PRIVACY');
  if (!value.includes('#seg-')) return; // Short opaque IDs remain valid for non-EPUB sources.
  const encodedPath = value.slice(0, value.lastIndexOf('#seg-'));
  try {
    const parts = encodedPath.split('/').map(part => decodeURIComponent(part));
    if (/^[a-z][a-z0-9+.-]*:/i.test(parts[0])) throw new Error('absolute or URL-like source locator');
    for (const part of parts) {
      if (part === '.' || part === '..' || /[\/\\\u0000-\u001f\u007f-\u009f\u200b-\u200f\u202a-\u202e\u2066-\u2069]/u.test(part)) throw new Error('unsafe source locator');
    }
  } catch {
    throw fail('Official-name capsule contains an invalid source segment locator', 'PRIVACY');
  }
}
export function parseOfficialCapsule(taskText) {
  if (typeof taskText !== 'string' || Buffer.byteLength(taskText) > 24 * 1024) throw fail('Official-name capsule capacity exceeded (24576 bytes); one complete capsule required', 'LIMIT');
  let value; try { value = JSON.parse(taskText); } catch { throw fail('Official-name task must be only the strict candidate capsule JSON', 'PRIVACY'); }
  if (!Check(CapsuleSchema, value)) throw fail('Invalid minimized official-name capsule', 'PRIVACY');
  value.candidates.forEach((candidate, i) => {
    if (candidate.candidate_id !== `c${String(i + 1).padStart(4, '0')}`) throw fail('Candidate IDs must be source-ordered c0001..', 'PRIVACY');
    for (const field of ['source_form', 'entity_key', 'form_type']) cleanSurface(candidate[field]);
    for (const evidence of candidate.evidence) {
      validateSegmentId(evidence.segment_id);
      if (evidence.quote !== candidate.source_form) throw fail('Evidence quote must equal the clean observed form, never a full source row', 'PRIVACY');
    }
  });
  // Input bytes and strict schema already bound traversal. Measure canonical JSON
  // exactly: the generic encoder's conservative array-key estimate rejects fitting sets.
  if (Buffer.byteLength(JSON.stringify(value)) > 24 * 1024) throw fail('Official-name encoded capsule capacity exceeded (24576 bytes)', 'LIMIT');
  return value;
}
export function validateOfficialAdmission({ role, profile, task, dependsOn, dependencyPolicy, dependencyHandoff, projectInstructions }, tasks, running) {
  if (!isOfficial({ role })) return;
  if (role.name !== OFFICIAL_ROLE || role.inputMode !== 'official-names' || role.workspace !== 'readonly' || role.tools.length !== 2 || !['web_search', 'web_fetch'].every(tool => role.tools.includes(tool))) throw fail('Official-name role must use the isolated web-only input mode', 'PRIVACY');
  if (profile.name !== OFFICIAL_ROLE || profile.provider !== 'openai-codex' || profile.model !== 'gpt-5.6-luna' || profile.thinkingLevel !== 'max') throw fail('Official-name research requires the exact Luna/max profile; no fallback', 'MODEL');
  if (profile.instructions) throw fail('Official-name research forbids custom profile instructions; use only the static isolated role guidance', 'PRIVACY');
  if (projectInstructions || dependencyHandoff !== 'none' || dependencyPolicy !== 'all_succeeded' || ![1, 2].includes(dependsOn.length) || new Set(dependsOn).size !== dependsOn.length) throw fail('Official-name research requires one names dependency (or two legacy Camel dependencies), all_succeeded/none, and no project instructions', 'PRIVACY');
  for (const id of dependsOn) if (!settledNames(tasks.get(id), running)) throw fail('Completed, settled names-research dependencies required before capsule projection', 'CAPABILITY');
  // A single exact Camel report is the current route; retain one-Luna and
  // two-Camel compatibility without admitting mixed-provider legacy pairs.
  if (dependsOn.length === 2 && dependsOn.some(id => tasks.get(id).profile.provider !== 'camel-stream')) throw fail('Use one names report per batch; two Camel reports are legacy compatibility only', 'CAPABILITY');
  return parseOfficialCapsule(task);
}
export function assertFreshOfficial(task) {
  if (!isOfficial(task)) return;
  if (task.profile?.instructions) throw fail('Official-name context forbids custom profile instructions', 'PRIVACY');
  parseOfficialCapsule(task.task);
  if (task.dependencyHandoff !== 'none' || task.projectInstructions || task.instruction || task.handoff || task.mailbox?.length || task.delivering?.length || task.dependencyInputs?.length || task.resultInputDeclarations?.length || task.resultBindings?.length || checkpointRecordCount(task.checkpoint)) throw fail('Official-name context must be fresh and metadata-free; inspect then spawn a newly authorized capsule, never resume/mail', 'PRIVACY');
}
const FindingSchema = Type.Object({ findings: Type.Array(Type.Object({ candidate_id: candidateId(), status: Type.String({ enum: ['official', 'unresolved'] }), official_name: Type.Optional(clean()), source_url: Type.Optional(Type.String({ maxLength: 4096 })), quote: Type.Optional(Type.String({ minLength: 1, maxLength: 2000 })) }, { additionalProperties: false }), { minItems: 1 }) }, { additionalProperties: false });
// Cosmetic response wrappers must not trigger another research pass.
export function parseOfficialReport(raw) {
  const text = raw.trim();
  const fences = [...text.matchAll(/```(?:json)?\s*\n([\s\S]*?)```/g)];
  const body = fences.length === 1 ? fences[0][1].trim() : text;
  let value;
  try { value = JSON.parse(body); } catch {
    try { value = JSON.parse(body.slice(body.indexOf('{'), body.lastIndexOf('}') + 1)); }
    catch { throw fail('Official-name response needs a complete findings JSON object; reuse existing evidence to repair only formatting', 'EVIDENCE'); }
  }
  if (!value || !Array.isArray(value.findings)) throw fail('Invalid official-name findings', 'EVIDENCE');
  const fields = ['candidate_id', 'status', 'official_name', 'source_url', 'quote'];
  return { findings: value.findings.map(row => row && typeof row === 'object' && !Array.isArray(row) ? Object.fromEntries(fields.filter(k => Object.hasOwn(row, k)).map(k => [k, row[k]])) : row) };
}
function queryNamesForm(query, form) {
  if (typeof query !== 'string') return false;
  const escaped = form.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  return new RegExp(`(^|[^\\p{L}\\p{N}_])${escaped}($|[^\\p{L}\\p{N}_])`, 'u').test(query);
}
export function validateOfficialResult(task, raw, evidence) {
  boundedTaskResult(task, raw);
  const value = parseOfficialReport(raw);
  if (!Check(FindingSchema, value)) throw fail('Invalid official-name findings', 'EVIDENCE');
  const candidates = parseOfficialCapsule(task.task).candidates;
  if (value.findings.length !== candidates.length) {
    const missing = candidates.filter(c => !value.findings.some(f => f.candidate_id === c.candidate_id)).map(c => c.candidate_id);
    throw fail(`Official-name coverage requires ${candidates.length} findings, received ${value.findings.length}; missing IDs: ${missing.join(', ') || 'none (remove extras/duplicates)'}`, 'EVIDENCE');
  }
  const byId = new Map(value.findings.map(f => [f.candidate_id, f]));
  if (byId.size !== candidates.length || candidates.some(c => !byId.has(c.candidate_id))) throw fail('Official-name findings contain missing, duplicate or unknown candidate IDs', 'EVIDENCE');
  value.findings = candidates.map(c => byId.get(c.candidate_id));
  value.findings.forEach((finding, i) => {
    const sourceForm = candidates[i].source_form;
    if (finding.candidate_id !== candidates[i].candidate_id) throw fail(`Official-name finding at position ${i + 1} must use ${candidates[i].candidate_id}; keep capsule order without duplicates`, 'EVIDENCE');
    if (!evidence.some(row => row.kind === 'search' && queryNamesForm(row.input?.query, sourceForm) && row.outcome.ok && row.outcome.terminal === 'completed' && row.outcome.actions?.some(action => action.type === 'search' && action.status === 'completed'))) throw fail(`Official-name ${candidates[i].candidate_id} requires an actual completed native search action receipt`, 'EVIDENCE');
    if (finding.status === 'official') {
      if (!finding.official_name || !finding.source_url || !finding.quote || !evidence.some(row => row.kind === 'fetch' && row.outcome.ok && row.outcome.final_url === finding.source_url && row.outcome.content?.includes(finding.quote))) throw fail(`Official-name ${candidates[i].candidate_id} requires a literal citation in a directly fetched page`, 'EVIDENCE');
    } else if (finding.official_name || finding.source_url || finding.quote) throw fail(`Official-name ${candidates[i].candidate_id}: unresolved must not claim official evidence`, 'EVIDENCE');
  });
  // Literal provenance is verified, not linguistic correctness or authority quality.
  return value;
}
export function officialPrompt(task) {
  if (task.profile?.instructions) throw fail('Official-name prompt forbids custom profile instructions', 'PRIVACY');
  return `${modelGuidance(task.profile)}\n\n${task.role.prompt}`;
}
// ResourceLoader extension records are a public SDK contract. No default resource
// discovery, global hooks or filesystem-backed extension loading enters this child.
export function officialPromptExtension(task) {
  const prompt = officialPrompt(task);
  return { path: '<official-name-privacy>', resolvedPath: '<official-name-privacy>', sourceInfo: { path: '<official-name-privacy>', source: 'sdk', scope: 'temporary', origin: 'top-level' },
    handlers: new Map([['before_agent_start', [() => ({ systemPrompt: prompt })]]]), tools: new Map(), commands: new Map(), flags: new Map(), shortcuts: new Map(), messageRenderers: new Map() };
}
