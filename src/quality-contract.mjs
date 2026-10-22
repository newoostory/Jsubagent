import { join } from 'node:path';
import { readResultPage } from './responses.mjs';
import { encodeJson, fail, hash, identifier, text } from './limits.mjs';

const object = value => value && typeof value === 'object' && !Array.isArray(value);
function exact(value, keys, label) {
  if (!object(value) || Object.keys(value).some(key => !keys.includes(key))) throw fail(`Invalid ${label}`);
}
function unique(values, label) {
  if (new Set(values).size !== values.length) throw fail(`Duplicate ${label}`, 'CONFLICT');
  return values;
}
export function normalizeQualityContract(value) {
  if (value === undefined) return undefined;
  exact(value, ['domain', 'stage', 'criteria', 'preserve', 'unresolvedEffects', 'ownedPaths', 'review'], 'quality contract');
  if (!['general', 'translation', 'coding', 'research'].includes(value.domain)) throw fail('Invalid quality domain');
  const stage = value.stage ?? 'produce', review = value.review ?? 'optional';
  if (!['produce', 'review', 'verify', 'integrate'].includes(stage) || !['optional', 'required'].includes(review)) throw fail('Invalid quality stage/review');
  if (!Array.isArray(value.criteria) || value.criteria.length < 1 || value.criteria.length > 16) throw fail('Quality criteria require 1..16 items');
  const criteria = value.criteria.map(row => { exact(row, ['id', 'description'], 'quality criterion'); return { id: identifier(row.id), description: text(row.description, 'criterion description', 2048) }; });
  unique(criteria.map(row => row.id), 'criterion');
  const list = (key, convert) => {
    const rows = value[key] ?? [];
    if (!Array.isArray(rows) || rows.length > 16) throw fail(`Invalid quality ${key}`);
    return unique(rows.map(convert), key);
  };
  const preserve = list('preserve', item => text(item, 'preserved literal', 256));
  const unresolvedEffects = list('unresolvedEffects', item => identifier(item, 'unresolved effect'));
  const ownedPaths = list('ownedPaths', item => {
    text(item, 'owned path', 512);
    if (item !== '.' && (item.startsWith('/') || /[\\\x00-\x1f\x7f*?\[\]]/.test(item) || item.split('/').some(part => !part || part === '.' || part === '..' || ['.git', '.pi'].includes(part)))) throw fail('Owned paths must be literal workspace-relative paths');
    return item;
  });
  const result = { domain: value.domain, stage, criteria, preserve, unresolvedEffects, ownedPaths, review };
  encodeJson(result, 24 * 1024);
  return result;
}
export const qualityRequirementsHash = contract => hash(encodeJson({ domain: contract.domain, criteria: contract.criteria, preserve: contract.preserve, unresolvedEffects: contract.unresolvedEffects }));

export function assertQualityAdmission(contract, { role, workflow, workItem, resultInputs = [] }) {
  if (!contract) return;
  if (workItem || workflow !== 'generic') throw fail('Quality contracts currently require the ordinary generic workflow', 'UNSUPPORTED');
  if (['review', 'verify'].includes(contract.stage)) {
    if (role.tools.some(tool => ['write', 'edit'].includes(tool)) || contract.stage === 'review' && (role.workspace !== 'readonly' || role.tools.includes('bash'))) throw fail('Independent quality review cannot repair its subject', 'CAPABILITY');
    if (!role.tools.includes('agent_read') || !resultInputs.some(input => input.required && ['candidate', 'final'].includes(input.when))) throw fail('Independent quality review requires a required immutable result input', 'CAPABILITY');
  }
  if (role.workspace === 'shared' && role.tools.some(tool => ['write', 'edit', 'bash'].includes(tool)) && !contract.ownedPaths.length) throw fail('Shared quality work requires explicit ownedPaths', 'CAPABILITY');
}

const DOMAIN = {
  general: 'Check every requested deliverable, correction and boundary; identify missing evidence and unresolved uncertainty.',
  translation: 'Check meaning and omissions against the source; preserve numbers, units, terminology, placeholders and register. Separate format validation from semantic review.',
  coding: 'Check requested behavior, edge cases, regression risk and actual test evidence against the pinned product. A claimed PASS is not an execution receipt.',
  research: 'Check source support, citations, revised denominators and arithmetic; distinguish observation from causal inference, conflicts and missing coverage.',
};
export function qualityPrompt(contract) {
  if (!contract) return '';
  return `\n\nHost quality contract (no new authority):\n${JSON.stringify(contract)}\n${DOMAIN[contract.domain]}\nUse sufficient context and output for the task; do not optimize away required evidence. Return one JSON object with deliverable (the complete requested product, string or object), criteria (exactly one {id,status,evidence} per declared criterion; status met/unmet/inconclusive; evidence is a concrete locator or observation), and unresolvedEffects (exactly one {id,status:"needs_confirmation"} per declared effect). Keep every preserve literal verbatim in deliverable. Report unmet/inconclusive honestly; do not change criteria to fit the candidate. These are candidate claims, not semantic acceptance. For review/verify assess the required pinned result inputs independently; peer summaries are not a substitute. For large output use existing self-contained product sections and finish with a separate last section named quality whose content is {"kind":"quality_receipt","criteria":[...],"unresolvedEffects":[...]}; do not split a JSON object across sections. The quality section is metadata, not a replacement for the product.`;
}
export function qualitySectionPrompt(contract) {
  if (!contract) return '';
  return `\nHost quality contract: ${JSON.stringify(contract)}\n${DOMAIN[contract.domain]}\nContinue with the existing result_section envelope, not a top-level deliverable JSON envelope. Product sections remain complete logical sections. Declare quality as the last required section; its content is a separate small JSON object {"kind":"quality_receipt","criteria":[{"id":"declared criterion","status":"met|unmet|inconclusive","evidence":"concrete locator or observation"}],"unresolvedEffects":[{"id":"declared effect","status":"needs_confirmation"}]}. Preserve exact declared literals in product sections. The sidecar is metadata and cannot substitute for product content. Keep every criterion and unresolved effect identity, and do not claim semantic acceptance.`;
}

// Structural completeness and declared outcome only. This never proves semantic
// truth, grants an effect, or manufactures a command receipt.
export function inspectQualityResult(raw, contract) {
  let value;
  try { value = JSON.parse(raw); } catch { throw fail('Quality result must be a complete JSON object', 'QUALITY'); }
  const bad = message => { throw fail(message, 'QUALITY'); };
  if (!object(value) || Object.keys(value).some(key => !['deliverable', 'criteria', 'unresolvedEffects'].includes(key))) bad('Invalid quality result fields');
  if (!(typeof value.deliverable === 'string' && value.deliverable.trim() || object(value.deliverable) || Array.isArray(value.deliverable))) bad('Quality deliverable is missing');
  const values = [];
  const collect = item => { if (typeof item === 'string') values.push(item); else if (Array.isArray(item)) item.forEach(collect); else if (object(item)) Object.values(item).forEach(collect); };
  collect(value.deliverable);
  if (contract.preserve.some(literal => !values.some(part => part.includes(literal)))) bad('Quality deliverable omitted a preserved literal');
  if (!Array.isArray(value.criteria) || value.criteria.length !== contract.criteria.length) bad('Quality criterion coverage is incomplete');
  const ids = new Set(), outcomes = [];
  for (const row of value.criteria) {
    if (!object(row) || Object.keys(row).some(key => !['id', 'status', 'evidence'].includes(key)) || !contract.criteria.some(item => item.id === row.id) || ids.has(row.id) || !['met', 'unmet', 'inconclusive'].includes(row.status) || typeof row.evidence !== 'string' || !row.evidence.trim() || Buffer.byteLength(row.evidence) > 4096) bad('Invalid quality criterion outcome/evidence');
    ids.add(row.id); outcomes.push({ id: row.id, status: row.status });
  }
  if (!Array.isArray(value.unresolvedEffects) || value.unresolvedEffects.length !== contract.unresolvedEffects.length) bad('Unresolved effect identities must be retained');
  const effects = new Set();
  for (const row of value.unresolvedEffects) {
    if (!object(row) || Object.keys(row).sort().join() !== 'id,status' || row.status !== 'needs_confirmation' || !contract.unresolvedEffects.includes(row.id) || effects.has(row.id)) bad('Unresolved effect cannot be removed or reconciled by a model claim');
    effects.add(row.id);
  }
  return { contractSha256: hash(encodeJson(contract)), requirementsSha256: qualityRequirementsHash(contract), resultSha256: hash(raw), criteria: outcomes, unresolvedEffects: [...contract.unresolvedEffects], state: outcomes.every(row => row.status === 'met') ? 'candidate_ready' : 'needs_repair', acceptance: 'not_evaluated' };
}

export const qualityResultDigest = ref => ref?.type === 'segments-v1' ? ref.indexRef?.sha256 : ref?.sha256;

// Reuse immutable section blobs and their index. Only a small final sidecar is
// parsed; product pages and a short overlap window are released as we advance.
export async function inspectSectionQuality(store, owner, manifest, contract, resultRef) {
  const ordered = manifest.requiredSections.map(id => manifest.sections.find(row => row.sectionId === id));
  if (ordered.length < 2 || ordered.some(row => !row) || ordered.at(-1).sectionId !== 'quality') throw fail('Contracted sections require a separate final quality section', 'QUALITY');
  const sidecar = ordered.at(-1);
  if (sidecar.ref.bytes > 64 * 1024) throw fail('Quality sidecar is too large', 'QUALITY');
  const found = new Set(); let tail = '', receiptText = '';
  const overlap = Math.max(0, ...contract.preserve.map(literal => literal.length - 1));
  for (const row of ordered) {
    if (row.ref.codec !== 'utf8') throw fail('Contracted sections require UTF-8 section refs', 'QUALITY');
    const identity = await store.artifactIdentity(row.ref.artifactId);
    if (identity.owner !== owner || identity.sha256 !== row.ref.sha256 || identity.bytes !== row.ref.bytes || !identity.complete) throw fail('Quality section identity changed', 'STORAGE');
    let offset = 0;
    while (offset < row.ref.bytes) {
      const page = await readResultPage(join(store.root, 'artifacts', `${row.ref.artifactId}.bin`), offset, Math.min(16384, row.ref.bytes - offset), row.ref.bytes, row.ref.sha256);
      if (page.nextByteOffset <= offset) throw fail('Quality section read made no progress', 'STORAGE');
      offset = page.nextByteOffset;
      if (row === sidecar) receiptText += page.text;
      else {
        const window = tail + page.text;
        for (const literal of contract.preserve) if (window.includes(literal)) found.add(literal);
        tail = overlap ? window.slice(-overlap) : '';
      }
    }
    // The canonical segmented product inserts a newline between sections.
    if (row !== sidecar) {
      const window = tail + '\n';
      for (const literal of contract.preserve) if (window.includes(literal)) found.add(literal);
      tail = overlap ? window.slice(-overlap) : '';
    }
  }
  if (contract.preserve.some(literal => !found.has(literal))) throw fail('Segmented product omitted a preserved literal', 'QUALITY');
  let receipt; try { receipt = JSON.parse(receiptText); } catch { throw fail('Invalid quality sidecar JSON', 'QUALITY'); }
  if (!object(receipt) || Object.keys(receipt).sort().join() !== 'criteria,kind,unresolvedEffects' || receipt.kind !== 'quality_receipt') throw fail('Invalid quality sidecar fields', 'QUALITY');
  // Reuse the exact criterion/effect validator after independently proving the
  // product's literal coverage. The temporary deliverable is bounded metadata.
  const checked = inspectQualityResult(JSON.stringify({ deliverable: contract.preserve, criteria: receipt.criteria, unresolvedEffects: receipt.unresolvedEffects }), { ...contract });
  return { ...checked, resultSha256: qualityResultDigest(resultRef) };
}

function currentQuality(task) {
  return task.qualityContract && task.quality && task.quality.contractSha256 === hash(encodeJson(task.qualityContract)) && task.quality.resultSha256 === qualityResultDigest(task.resultRef) && task.quality.resultEpoch === (task.resultEpoch ?? 1) && task.quality.goalRevision === (task.goalRevision ?? 1);
}
export function bindQualityReceipt(receipt, contract, resultRef, task) {
  if (!object(receipt) || receipt.contractSha256 !== hash(encodeJson(contract)) || receipt.requirementsSha256 !== qualityRequirementsHash(contract) || receipt.resultSha256 !== qualityResultDigest(resultRef) || !Array.isArray(receipt.criteria) || receipt.criteria.length !== contract.criteria.length || !Array.isArray(receipt.unresolvedEffects) || receipt.unresolvedEffects.join('\0') !== contract.unresolvedEffects.join('\0') || receipt.acceptance !== 'not_evaluated') throw fail('Missing or unbound worker quality receipt', 'QUALITY');
  const ids = new Set();
  for (const row of receipt.criteria) {
    if (!contract.criteria.some(item => item.id === row.id) || ids.has(row.id) || !['met', 'unmet', 'inconclusive'].includes(row.status)) throw fail('Invalid worker quality outcome', 'QUALITY');
    ids.add(row.id);
  }
  const state = receipt.criteria.every(row => row.status === 'met') ? 'candidate_ready' : 'needs_repair';
  if (state !== receipt.state) throw fail('Contradictory worker quality outcome', 'QUALITY');
  return { contractSha256: receipt.contractSha256, requirementsSha256: receipt.requirementsSha256, resultSha256: receipt.resultSha256, criteria: receipt.criteria.map(({ id, status }) => ({ id, status })), unresolvedEffects: [...receipt.unresolvedEffects], state, acceptance: 'not_evaluated', resultEpoch: task.resultEpoch ?? 1, goalRevision: task.goalRevision ?? 1 };
}
export function qualityReviewVerdict(review, task, product, { settled, execution, unresolved = () => false, head }) {
  const declarations = (review.resultInputDeclarations ?? []).filter(input => input.required && input.sourceId === task.id);
  if (!declarations.length || review.id === task.id || !['review', 'verify'].includes(review.qualityContract?.stage)) return null;
  const bindings = (review.resultBindings ?? []).filter(input => input.agentId === task.id);
  const same = bindings.length && bindings.every(input => input.resultEpoch === (task.resultEpoch ?? 1) && product && (input.candidateSnapshot ? input.candidateSnapshot.productSha256 === product.candidateSnapshot?.productSha256 : input.revision === product.revision));
  if (!same || !task.qualityContract || qualityRequirementsHash(review.qualityContract) !== qualityRequirementsHash(task.qualityContract)) return 'stale';
  if (!settled(review)) return 'pending';
  if (head && head(review)?.canonicalResult !== true) return 'failed';
  if (review.qualityArchived || review.state !== 'idle' || !currentQuality(review) || review.quality.state !== 'candidate_ready' || review.quality.unresolvedEffects.length || unresolved(review) || review.usageUncertain || review.requestPermitUncertain || review.requiredResultChecks?.length && execution(review).verification !== 'passed') return 'failed';
  return 'passed';
}
export function qualityCompletion(tasks, allTasks, context) {
  const { settled, head, execution, unresolved = () => false, lookup = () => undefined } = context;
  const rows = tasks.map(task => {
    const reasons = [], reviews = { passed: 0, failed: 0, pending: 0, stale: 0, superseded: 0 }, reviewerIds = [];
    if (!task.qualityContract) reasons.push('no_quality_contract');
    if (!settled(task) || task.state !== 'idle') reasons.push('not_settled_success');
    if (!currentQuality(task)) reasons.push('missing_or_stale_quality_result');
    else {
      if (task.quality.state !== 'candidate_ready') reasons.push('unmet_or_inconclusive_criteria');
      if (task.quality.unresolvedEffects.length || unresolved(task) || task.usageUncertain || task.requestPermitUncertain) reasons.push('unresolved_effect_or_usage');
    }
    const checks = execution(task);
    if (task.requiredResultChecks?.length && checks.verification !== 'passed') reasons.push(`required_checks_${checks.verification}`);
    const product = head(task);
    if (product && product.canonicalResult === false) reasons.push('published_product_differs_from_canonical_result');
    for (const review of allTasks()) {
      if (!review || review.quarantined) {
        if (!reasons.includes('retained_evidence_integrity_unknown')) reasons.push('retained_evidence_integrity_unknown');
        continue;
      }
      const verdict = qualityReviewVerdict(review, task, product, context);
      if (!verdict) continue;
      const replacement = (task.qualityReviewSupersessions ?? []).findLast(row => row.reviewerId === review.id);
      const next = replacement && lookup(replacement.replacementId);
      if (next && replacement.replacementEpoch === next.resultEpoch && replacement.replacementResultSha256 === next.quality?.resultSha256 && qualityReviewVerdict(next, task, product, context) === 'passed') {
        reviews.superseded++; continue;
      }
      // Archived/negative/pending reviews remain visible until a main-authorized
      // replacement still proves a successful review of this current subject.
      if (reviewerIds.length < 4) reviewerIds.push(review.id);
      reviews[verdict]++;
    }
    if (reviews.failed) reasons.push('independent_review_failed');
    if (reviews.pending) reasons.push('independent_review_pending');
    if (reviews.stale) reasons.push('independent_review_stale');
    if (task.qualityContract?.review === 'required' && !reviews.passed) reasons.push('independent_review_required');
    return { agentId: task.id, state: reasons.length ? 'blocked' : 'ready_for_acceptance', reasons, reviews, reviewerIds, resultRef: task.resultRef, resultEpoch: task.resultEpoch ?? 1, acceptance: 'not_evaluated' };
  });
  return { scope: 'requested_agents', state: rows.every(row => row.state === 'ready_for_acceptance') ? 'ready_for_acceptance' : 'blocked', acceptance: 'not_evaluated', agents: rows, note: 'Structural coverage, current pinned independent claims and required runtime receipts only. Inspect semantic evidence and the actual destination before accepting the requested task. This query executes no work and grants no publication authority.' };
}
