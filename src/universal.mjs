import { encodeJson, fail, identifier, text, hash } from './limits.mjs';

const plain = value => value && typeof value === 'object' && !Array.isArray(value);
function exact(value, fields, label) {
  if (!plain(value) || Object.keys(value).some(key => !fields.includes(key))) throw fail(`Invalid ${label} fields`);
  encodeJson(value, 32 * 1024);
}

export const WORK_INPUT_BYTES = 2 * 1024 * 1024;

export async function boundedInputIdentity(store, ref, label = 'Input') {
  const metadata = await store.artifactInfo(ref.artifactId);
  if (!metadata || metadata.id !== ref.artifactId || metadata.complete !== true || !Number.isSafeInteger(metadata.bytes) || metadata.bytes < 0) throw fail(`${label} artifact metadata is invalid`, 'STORAGE');
  if (metadata.bytes > WORK_INPUT_BYTES) throw fail(`${label} exceeds the ${WORK_INPUT_BYTES}-byte pinned input limit`, 'LIMIT');
  if (metadata.owner !== ref.owner || metadata.bytes !== ref.bytes || metadata.sha256 && metadata.sha256 !== ref.sha256) throw fail(`${label} reference identity mismatch`, 'STORAGE');
  const identity = await store.artifactIdentity(ref.artifactId);
  if (identity.bytes > WORK_INPUT_BYTES) throw fail(`${label} exceeds the ${WORK_INPUT_BYTES}-byte pinned input limit`, 'LIMIT');
  if (!identity.complete || identity.owner !== ref.owner || identity.bytes !== ref.bytes || identity.sha256 !== ref.sha256) throw fail(`${label} identity changed`, 'STORAGE');
  return identity;
}
function strings(value, label, max = 8) {
  if (!Array.isArray(value) || value.length > max) throw fail(`Invalid ${label}`);
  const result = value.map(item => identifier(item, label));
  if (new Set(result).size !== result.length) throw fail(`Duplicate ${label}`);
  return result;
}
export function missionContract(value) {
  exact(value, ['id', 'request', 'goal', 'authority', 'completion'], 'mission');
  return { id: identifier(value.id), request: text(value.request, 'request', 24 * 1024), goal: text(value.goal, 'goal', 8192), authority: text(value.authority, 'authority', 8192), completion: text(value.completion, 'completion', 8192) };
}
export function workPredecessors(item) {
  return (item.predecessors ?? []).map(edge => typeof edge === 'string' ? { id: edge, use: 'inspect' } : edge);
}
function predecessorContract(value, intendedUse) {
  if (!Array.isArray(value) || value.length > 8) throw fail('Invalid predecessors');
  const edges = value.map(edge => {
    if (typeof edge === 'string') return identifier(edge, 'predecessor');
    exact(edge, ['id', 'use'], 'predecessor');
    if (!['inspect', 'applied'].includes(edge.use)) throw fail('Predecessor use must be inspect/applied');
    return { id: identifier(edge.id, 'predecessor'), use: edge.use };
  });
  const ids = edges.map(edge => typeof edge === 'string' ? edge : edge.id);
  if (new Set(ids).size !== ids.length) throw fail('Duplicate predecessor');
  if (intendedUse === 'publish' && edges.some(edge => typeof edge === 'string')) throw fail('Publish requires explicit predecessor edges; string IDs only mean inspect');
  return edges;
}
export function workItemContract(value) {
  exact(value, ['id', 'missionId', 'objective', 'inputRefs', 'evidenceIds', 'capabilities', 'resources', 'deliverable', 'acceptance', 'acceptanceProof', 'intendedUse', 'stop', 'predecessors', 'role', 'profile', 'route', 'directory', 'execution'], 'work item');
  const inputRefs = value.inputRefs ?? [];
  if (!Array.isArray(inputRefs) || inputRefs.length > 8) throw fail('Invalid inputRefs');
  for (const ref of inputRefs) { exact(ref, ['artifactId', 'owner', 'bytes', 'sha256'], 'input reference'); identifier(ref.artifactId); identifier(ref.owner); if (!Number.isSafeInteger(ref.bytes) || ref.bytes < 0 || ref.bytes > WORK_INPUT_BYTES || !/^[a-f0-9]{64}$/.test(ref.sha256)) throw fail(`Invalid input identity; pinned inputs must be <= ${WORK_INPUT_BYTES} bytes`); }
  if (new Set(inputRefs.map(ref => ref.artifactId)).size !== inputRefs.length) throw fail('Duplicate input reference');
  if (!['inspect', 'publish'].includes(value.intendedUse)) throw fail('Work intendedUse must be inspect/publish');
  if (value.execution !== undefined && !['model', 'artifact-sha256'].includes(value.execution)) throw fail('Unsupported work execution');
  if (value.route !== undefined && !['narrow', 'complex'].includes(value.route)) throw fail('Route must be narrow/complex');
  if (value.execution === 'artifact-sha256' && value.route !== undefined) throw fail('Deterministic execution cannot request a model route');
  if (value.execution === 'artifact-sha256' && inputRefs.length !== 1) throw fail('Deterministic artifact-sha256 requires exactly one pinned input');
  if (value.execution === 'artifact-sha256' && !value.acceptanceProof) throw fail('Deterministic execution requires a precommitted acceptance proof');
  if (value.acceptanceProof !== undefined) {
    exact(value.acceptanceProof, ['kind', 'sha256'], 'acceptance proof');
    if (value.acceptanceProof.kind !== 'exact-result-sha256' || !/^[a-f0-9]{64}$/.test(value.acceptanceProof.sha256)) throw fail('Unsupported acceptance proof');
  }
  const result = { id: identifier(value.id), missionId: identifier(value.missionId), objective: text(value.objective, 'objective', 16 * 1024), inputRefs, ...(value.evidenceIds !== undefined ? { evidenceIds: strings(value.evidenceIds, 'evidence ID') } : {}), capabilities: strings(value.capabilities ?? [], 'capability', 16), resources: strings(value.resources ?? [], 'resource', 16), deliverable: text(value.deliverable, 'deliverable', 8192), acceptance: text(value.acceptance, 'acceptance', 8192), ...(value.acceptanceProof ? { acceptanceProof: value.acceptanceProof } : {}), intendedUse: value.intendedUse, stop: text(value.stop, 'stop', 8192), predecessors: predecessorContract(value.predecessors ?? [], value.intendedUse), role: identifier(value.role), ...(value.profile ? { profile: identifier(value.profile) } : {}), ...(value.route ? { route: value.route } : {}), ...(value.directory ? { directory: text(value.directory, 'directory', 4096) } : {}), ...(value.execution ? { execution: value.execution } : {}) };
  if (workPredecessors(result).some(edge => edge.id === result.id)) throw fail('Work item cannot depend on itself');
  return result;
}
export function evidenceContract(value) {
  exact(value, ['id', 'question', 'source', 'locator', 'observation', 'coverage', 'missing', 'contradictions', 'priority', 'status', 'refs'], 'evidence');
  const refs = strings(value.refs, 'evidence reference', 8);
  if (!refs.length) throw fail('Evidence requires immutable references');
  const contradictions = strings(value.contradictions ?? [], 'contradiction', 8);
  if (value.missing !== undefined && (!Array.isArray(value.missing) || value.missing.length > 8)) throw fail('Invalid missing-evidence claims');
  const missing = value.missing?.map(claim => text(claim, 'missing-evidence claim', 160));
  if (missing && new Set(missing).size !== missing.length) throw fail('Duplicate missing-evidence claim');
  if (!['high', 'normal', 'low'].includes(value.priority) || !['supported', 'refuted', 'unresolved'].includes(value.status)) throw fail('Invalid evidence priority/status');
  return { id: identifier(value.id), question: text(value.question, 'question', 2048), source: text(value.source, 'source', 2048), locator: text(value.locator, 'locator', 2048), observation: text(value.observation, 'observation', 4096), coverage: text(value.coverage, 'coverage', 2048), ...(missing ? { missing } : {}), contradictions, priority: value.priority, status: value.status, refs };
}
// Pure decision: no mutation, no model calls, no reservations and no work stage assumption.
export function admitWork(item, { mission, tasks, running, inputIdentity, concurrency, appliedReceipts = new Map(), pressure = false }) {
  if (!mission || mission.id !== item.missionId) return { state: 'blocked', reason: 'Unknown mission' };
  if (item.intendedUse === 'publish' && item.execution !== 'artifact-sha256') return { state: 'blocked', reason: 'Only host-owned deterministic publication is supported' };
  for (const edge of workPredecessors(item)) {
    const predecessor = tasks.get(edge.id);
    if (!predecessor || predecessor.workItem?.missionId !== mission.id) return { state: 'blocked', reason: `Unknown predecessor ${edge.id}` };
    if (running.has(edge.id) || predecessor.state !== 'idle' || predecessor.acceptedReport !== predecessor.currentReport || !predecessor.acceptedReport) return { state: 'waiting', reason: `Predecessor ${edge.id} has no settled accepted current report` };
    if (edge.use === 'applied') {
      const receipt = appliedReceipts.get(edge.id);
      if (predecessor.workItem?.intendedUse !== 'publish' || predecessor.acceptedUse !== 'publish' || receipt?.state !== 'confirmed' || receipt.reportArtifactId !== predecessor.currentReport || receipt.grantHash !== predecessor.effectGrantHash) return { state: 'waiting', reason: `Predecessor ${edge.id} has no confirmed current effect` };
    }
  }
  for (const ref of item.inputRefs) {
    const info = inputIdentity.get(ref.artifactId);
    if (!info || !info.complete || ref.bytes > WORK_INPUT_BYTES || info.bytes > WORK_INPUT_BYTES || info.owner !== ref.owner || info.bytes !== ref.bytes || info.sha256 !== ref.sha256) return { state: 'blocked', reason: `Input identity changed or exceeds pinned input limit: ${ref.artifactId}` };
  }
  if (pressure || running.size >= concurrency) return { state: 'waiting', reason: 'Global capacity' };
  const active = [...running.keys()].map(id => tasks.get(id)).filter(Boolean);
  const candidate = tasks.get(item.id);
  if (!candidate) return { state: 'blocked', reason: 'Unknown work task' };
  if (item.execution !== 'artifact-sha256') {
    const sameProvider = active.filter(task => task.workItem?.execution !== 'artifact-sha256' && task.profile.provider === candidate.profile.provider);
    if (sameProvider.length >= Math.min(candidate.profile.maxConcurrent, ...sameProvider.map(t => t.profile.maxConcurrent))) return { state: 'waiting', reason: 'Provider capacity' };
    const sameModel = sameProvider.filter(task => task.profile.model === candidate.profile.model);
    if (sameModel.length >= Math.min(candidate.profile.modelConcurrent ?? candidate.profile.maxConcurrent, ...sameModel.map(t => t.profile.modelConcurrent ?? t.profile.maxConcurrent))) return { state: 'waiting', reason: 'Model capacity' };
  }
  if (active.some(task => (task.ownedResources ?? []).some(resource => candidate.ownedResources.includes(resource)))) return { state: 'waiting', reason: 'Resource owned' };
  // Explicitly routed independent items may share slots only if their raw inputs
  // and effectful workspace cannot overlap; the caller supplied the items, not a
  // router-generated DAG. Existing direct-profile jobs retain their policy.
  if (active.some(task => (item.route || task.workItem?.route) && (
    (task.workItem && item.inputRefs.some(ref => task.workItem.inputRefs.some(other => other.artifactId === ref.artifactId))) ||
    (item.intendedUse === 'publish' && task.workItem?.intendedUse === 'publish') ||
    (candidate.cwd === task.cwd && (candidate.role?.workspace === 'shared' || task.role?.workspace === 'shared'))
  ))) return { state: 'waiting', reason: 'Routed work input or effect scope overlaps an active item' };
  return { state: 'ready', resources: [...candidate.ownedResources] };
}
export function workPrompt(item, mission) {
  return `Mission request: ${mission.request}\nGoal: ${mission.goal}\nAuthority (does not grant new tools): ${mission.authority}\nCompletion: ${mission.completion}\nWork objective: ${item.objective}\nDeliverable: ${item.deliverable}\nAcceptance (candidate only until trusted checks): ${item.acceptance}\n${item.execution === 'artifact-sha256' ? 'Execution: host-owned artifact SHA-256, no model or product effect during execution.\n' : ''}${item.route ? `Caller-declared model route: ${item.route} (trusted profile mark required, not a quality guarantee).\n` : ''}Intended use: ${item.intendedUse}; publication requires a separate exact host grant, trusted acceptance and explicit work_apply.\n${item.predecessors.some(edge => typeof edge !== 'string') ? `Predecessor requirements (inspect accepted report vs applied verified effect): ${JSON.stringify(workPredecessors(item))}\n` : ''}${item.evidenceIds?.length ? `Evidence IDs (host checks mission and pinned sources before execution; these are claims, not facts): ${item.evidenceIds.join(', ')}\n` : ''}Stop: ${item.stop}\nInput artifact IDs: ${item.inputRefs.map(ref => ref.artifactId).join(', ') || '(none)'}\n`;
}
export function workIdentity(item) { return hash(encodeJson(item)); }
