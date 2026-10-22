import { encodeJson, fail, hash, identifier } from './limits.mjs';
import { resultBundleId, resultSelector } from './results.mjs';
import { subscription, registerSubscription, firstSubscriptionRevision } from './result-subscriptions.mjs';

export function normalizeResultInputs(inputs = [], { index, batchSize } = {}) {
  if (!Array.isArray(inputs) || inputs.length > 8) throw fail('resultInputs must contain at most eight inputs');
  return inputs.map(input => {
    if (!input || typeof input !== 'object' || Array.isArray(input) || Object.keys(input).some(key => !['from', 'view', 'files', 'kinds', 'when', 'revision', 'required'].includes(key))) throw fail('Invalid result input fields');
    const from = input.from;
    if (!from || typeof from !== 'object' || Array.isArray(from) || Object.keys(from).length !== 1) throw fail('Result source must be an agentId or earlier batch task');
    if (from.agentId !== undefined) identifier(from.agentId, 'result source');
    else if (!Number.isInteger(from.task) || index === undefined || from.task < 0 || from.task >= index || from.task >= batchSize) throw fail('Result input task index must refer to an earlier batch task');
    const selector = Object.fromEntries(['view', 'files', 'kinds'].filter(key => input[key] !== undefined).map(key => [key, input[key]]));
    resultSelector(selector);
    const when = input.when ?? 'final';
    if (!['published', 'candidate', 'final'].includes(when)) throw fail('Unknown result input milestone');
    if (input.revision !== undefined && (!Number.isSafeInteger(input.revision) || input.revision < 1)) throw fail('Invalid result input revision');
    if (input.required !== undefined && typeof input.required !== 'boolean') throw fail('Invalid required result input');
    return { from: { ...from }, ...Object.fromEntries(Object.entries(selector).map(([key, value]) => [key, Array.isArray(value) ? [...value].sort() : value])), when, ...(input.revision === undefined ? {} : { revision: input.revision }), required: input.required ?? true };
  });
}
const selectorOf = input => Object.fromEntries(['view', 'files', 'kinds'].filter(key => input[key] !== undefined).map(key => [key, input[key]]));
const revision = (store, bundleId, number) => store.metadata.get('result_revisions', store.scope, hash(`${bundleId}\0${number}`));
function coverage(store, manifest, selector) {
  const wanted = resultSelector(selector), db = store.metadata.database, args = [store.scope.runId, store.scope.ownerId, manifest.bundleId, manifest.revision, manifest.revision];
  const common = `e.run_id=? AND e.owner_id=? AND json_extract(e.data_json,'$.bundleId')=? AND json_extract(e.data_json,'$.revision')<=? AND json_extract(e.data_json,'$.revision')=(SELECT MAX(json_extract(n.data_json,'$.revision')) FROM result_entries n WHERE n.run_id=e.run_id AND n.owner_id=e.owner_id AND json_extract(n.data_json,'$.bundleId')=json_extract(e.data_json,'$.bundleId') AND json_extract(n.data_json,'$.fileId')=json_extract(e.data_json,'$.fileId') AND json_extract(n.data_json,'$.revision')<=?) AND COALESCE(json_extract(e.data_json,'$.deleted'),0)=0`;
  if (wanted.files) {
    const count = db.prepare(`SELECT COUNT(*) AS n FROM result_entries e WHERE ${common} AND json_extract(e.data_json,'$.logicalPath') IN (SELECT value FROM json_each(?))`).get(...args, JSON.stringify(wanted.files)).n;
    return Number(count) === wanted.files.length;
  }
  const requiredByView = { main: ['main_report'], reviewer: ['main_report', 'changed_files', 'patch_index'], verifier: ['main_report', 'test_inventory', 'verification_plan'], artifacts: ['main_report'] };
  const needed = selector.kinds ?? requiredByView[selector.view];
  const kinds = db.prepare(`SELECT DISTINCT json_extract(e.data_json,'$.kind') AS kind FROM result_entries e WHERE ${common} AND json_extract(e.data_json,'$.kind') IN (SELECT value FROM json_each(?)) LIMIT 32`).all(...args, JSON.stringify(needed));
  return needed.every(kind => kinds.some(row => row.kind === kind));
}
function eligible(store, manifest, input) { return manifest && (input.when === 'published' || input.when === 'candidate' && Boolean(manifest.candidateSnapshot) && ['candidate', 'final'].includes(manifest.stage) || input.when === 'final' && manifest.stage === 'final' && manifest.outputComplete) && coverage(store, manifest, input.selector); }
export { eligible as eligibleResultInput };
function binding(manifest, selector) { return { agentId: manifest.agentId, resultEpoch: manifest.resultEpoch, bundleId: manifest.bundleId, revision: manifest.revision, selector, ...(manifest.candidateSnapshot && ['reviewer', 'verifier'].includes(selector.view) ? { candidateSnapshot: manifest.candidateSnapshot } : {}) }; }

export class ResultInputs {
  constructor(store, tasks) { this.store = store; this.tasks = tasks; this.cursor = 0; }
  prepare(targetId, inputs, role) {
    inputs = normalizeResultInputs(inputs);
    if (inputs.length && !role.tools.includes('agent_read')) throw fail('resultInputs require the explicitly configured agent_read capability', 'CAPABILITY');
    const records = new Map();
    for (const input of inputs) {
      const sourceId = input.from.agentId;
      if (sourceId === targetId) throw fail('Result input cannot depend on itself', 'CONFLICT');
      const source = this.tasks.get(sourceId);
      if (input.when === 'candidate' && source?.resultContract !== 'code-change') throw fail('Candidate inputs require the code-change immutable snapshot backend', 'UNSUPPORTED');
      if (!source || source.workItem) throw fail('Result input source is not an accepted ordinary task', 'NOT_FOUND');
      const selector = selectorOf(input), resultEpoch = source.resultEpoch ?? 1, bundleId = resultBundleId(sourceId, resultEpoch);
      const head = this.store.metadata.get('result_heads', this.store.scope, bundleId);
      const explicit = input.revision === undefined ? null : revision(this.store, bundleId, input.revision);
      if (input.revision !== undefined && !explicit) throw fail('Explicit result revision does not exist', 'NOT_FOUND');
      const id = `ri-${hash(encodeJson({ targetId, sourceId, resultEpoch, selector, when: input.when, revision: input.revision }, 32 * 1024))}`;
      const row = { id, state: 'waiting', targetId, sourceId, resultEpoch, bundleId, selector, when: input.when, required: input.required, explicitRevision: input.revision ?? null, examinedRevision: Math.max(0, (explicit?.revision ?? head?.revision ?? 1) - 1) };
      if (explicit && !eligible(this.store, explicit, row)) throw fail('Explicit revision does not satisfy its immutable input contract', 'BLOCKED');
      const shared = subscription(this.store, row);
      if (shared?.revision) { row.state = 'bound'; row.binding = binding(revision(this.store, bundleId, shared.revision), selector); }
      else if (!shared && (explicit || eligible(this.store, head, row))) { row.state = 'bound'; row.binding = binding(explicit ?? head, selector); }
      const prior = records.get(id); if (prior?.required) row.required = true;
      records.set(id, row);
    }
    return [...records.values()];
  }
  register(rows) {
    this.store.metadata.transaction(() => { for (let row of rows) {
      const prior = this.store.metadata.get('result_bindings', this.store.scope, row.id);
      if (prior) {
        for (const key of ['targetId', 'sourceId', 'resultEpoch', 'bundleId', 'selector', 'when', 'required', 'explicitRevision']) if (JSON.stringify(prior[key]) !== JSON.stringify(row[key])) throw fail('Durable result input definition changed', 'STORAGE');
        continue;
      }
      const target = this.tasks.get(row.targetId), source = this.tasks.get(row.sourceId);
      const blocked = !target || target.cancelRequested || target.authorityRevoked || !source || source.cancelRequested || source.authorityRevoked;
      if (!blocked) {
        const shared = registerSubscription(this.store, row);
        if (shared.revision) { row = { ...row, state: 'bound', binding: binding(revision(this.store, row.bundleId, shared.revision), row.selector) }; }
      }
      this.store.metadata.put('result_bindings', this.store.scope, blocked ? { ...row, state: 'blocked', binding: undefined, reason: 'Publication/input authority is no longer current' } : row, { ifAbsent: true });
    } });
  }
  rows(targetId) {
    return this.store.metadata.database.prepare("SELECT data_json FROM result_bindings WHERE run_id=? AND owner_id=? AND json_extract(data_json,'$.targetId')=? ORDER BY seq LIMIT 17").all(this.store.scope.runId, this.store.scope.ownerId, targetId).map(row => JSON.parse(row.data_json));
  }
  advance(limit = 64) {
    const page = this.store.metadata.list('result_bindings', this.store.scope, { state: 'waiting', after: this.cursor, limit });
    this.cursor = page.nextCursor ?? 0;
    let more = Boolean(page.nextCursor);
    this.store.metadata.transaction(() => {
      for (const input of page.items) {
        const target = this.tasks.get(input.targetId), source = this.tasks.get(input.sourceId);
        if (!target || target.cancelRequested || target.authorityRevoked || target.state === 'archived' || !source || source.cancelRequested || source.authorityRevoked || ['cancelled', 'failed', 'blocked', 'archived'].includes(source.state) || (source.resultEpoch ?? 1) !== input.resultEpoch) {
          this.store.metadata.put('result_bindings', this.store.scope, { ...input, state: 'blocked', reason: 'Result producer/recipient cancelled, failed, blocked, unavailable, or changed epoch' }); continue;
        }
        const selected = firstSubscriptionRevision(this.store, input, eligible);
        more ||= selected.more;
        if (selected.manifest) this.store.metadata.put('result_bindings', this.store.scope, { ...input, state: 'bound', examinedRevision: selected.manifest.revision, binding: binding(selected.manifest, input.selector) });
        else if (!selected.more && source.state === 'idle' && this.store.metadata.get('result_heads', this.store.scope, input.bundleId)?.outputComplete) {
          // The current epoch is sealed and its full revision history was examined.
          // A missing required file/view cannot arrive without newly authorized work.
          this.store.metadata.put('result_bindings', this.store.scope, { ...input, state: 'blocked', reason: 'Result producer completed without required input coverage' });
        }
      }
    });
    return more;
  }
  readiness(task) {
    const rows = this.rows(task.id);
    if ((task.resultInputDeclarations ?? []).some(input => input.required && !rows.some(row => row.id === input.id))) return { ready: false, blocked: [], waiting: task.resultInputDeclarations.filter(input => input.required).map(input => input.sourceId), bindings: [] };
    if (rows.length > 16) throw fail('Result input binding working set exceeded', 'STORAGE');
    const required = rows.filter(row => row.required), blocked = required.filter(row => {
      const source = this.tasks.get(row.sourceId);
      return row.state === 'blocked' || !source || source.cancelRequested || source.authorityRevoked || ['cancelled', 'failed', 'blocked', 'archived'].includes(source.state);
    });
    return { ready: blocked.length === 0 && required.every(row => row.state === 'bound'), blocked: blocked.map(row => row.sourceId), waiting: required.filter(row => row.state === 'waiting').map(row => row.sourceId), bindings: rows.filter(row => row.state === 'bound').map(row => ({ ...row.binding, inputBindingId: row.id })) };
  }
  grant(task, request) {
    if (!task.role.tools.includes('agent_read') || task.cancelRequested || task.authorityRevoked) throw fail('Result read capability revoked', 'PERMISSION');
    if (request.agentId === task.id) return undefined;
    const rows = this.rows(task.id).filter(row => row.state === 'bound' && row.sourceId === request.agentId);
    const matches = rows.filter(row => (!request.inputBindingId || request.inputBindingId === row.id) && (!request.bundleId || request.bundleId === row.binding.bundleId) && (!request.revision || request.revision === row.binding.revision));
    if (matches.length !== 1) throw fail('Result read requires one exact durable input binding', 'PERMISSION');
    return { ...matches[0].binding, inputBindingId: matches[0].id };
  }
}
