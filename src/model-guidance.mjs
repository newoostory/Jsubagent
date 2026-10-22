// Method guidance only. Model/provider selection and role/tool permissions are unchanged.
import { text } from './limits.mjs';
export const PROFILE_INSTRUCTION_BYTES = 4096;
export function modelGuidanceKind(model) {
  const id = model?.id ?? model?.model;
  return ['openai', 'openai-codex'].includes(model?.provider) && typeof id === 'string' && id.trim() === id && /^gpt-6-astra(?:-\d{4}-\d{2}-\d{2})?$/.test(id) ? 'astra' : 'structured';
}
// Adapted from OpenAI's Astra prompting best practices; source/limits: docs/astra-guidance.md.
const GUIDANCE = {
  astra: `## Model working method: astra
Complete the assigned outcome within scope and existing authorization. Infer routine details from context; ask only when the answer materially changes the outcome or authority. Continue independent authorized work and prepare a reviewable result before necessary approval. Preserve required coverage even when work is long.
System/developer and role/tool restrictions remain binding; explicit user requirements override skill defaults. If a skill causes a pause, link the exact SKILL.md, quote its rule and separate it from your interpretation.
Run the smallest meaningful checks and all required checks. Broaden or repeat only for changed inputs, failures or concrete concerns; avoid tests that mirror trivial edits.
Lead with the result in plain concise prose. Use lists for useful comparison or sequence and proper spacing in peer messages. Preserve required output fields and evidence; avoid stock phrases and invented jargon.`,
  structured: `## Model working method: structured
1. Identify this assignment's requested outcome, supplied inputs, permitted actions, output and completion check. A bounded task may include implementation and its focused checks when assigned; do not invent phase handoffs. Do not infer missing source, permissions or schemas. The role/task contract takes precedence.
2. Work on one complete bounded unit at a time. Read the required evidence, perform the next allowed action, then compare its result with the expected result before continuing. Keep only a short progress checklist when useful; do not output private reasoning.
3. Before submission, check coverage and order against the assigned input, required fields and exact identifiers, and actual test/tool evidence. An example is a format example, not source data. Self-checking never replaces a required independent reviewer.
4. On missing, stale, truncated or conflicting input, identify the exact boundary and required next input. Do not guess, silently drop material, repeat an unchanged failure or declare success. Continue independent authorized work; request a smaller complete unit when capacity is the issue.`,
};
export function modelGuidance(model) { return GUIDANCE[modelGuidanceKind(model)]; }
export function profileGuidance(profile) {
  if (profile?.instructions === undefined) return '';
  const instructions = text(profile.instructions, 'profile instructions', PROFILE_INSTRUCTION_BYTES, { empty: true });
  if (!instructions) return '';
  return `\n\nProfile-specific working preferences (not new authority; role/tool/privacy restrictions and explicit task constraints remain binding):\n${instructions}`;
}
