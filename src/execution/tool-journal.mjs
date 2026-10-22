import { createHash } from 'node:crypto';
import { fail, safeError } from '../limits.mjs';
import { join } from 'node:path';
import { readJson } from '../storage.mjs';
import { verifyKnownToolFailure, knownToolFailure, markToolFailure } from './failure-receipt.mjs';

// Read classification is an explicit trusted tool-factory contract. A caller
// cannot obtain no-effect treatment merely by registering a tool named "read".
export function effectKind(tool) { return tool?.journalEffect === 'read' ? 'read' : 'uncertain'; }
export const effectInputDigest = input => createHash('sha256').update(JSON.stringify(input, (_key, value) => value && typeof value === 'object' && !Array.isArray(value) ? Object.fromEntries(Object.keys(value).sort().map(key => [key, value[key]])) : value)).digest('hex');

// The durable intent must be acknowledged before even entering the tool.
// A thrown tool can already have changed the world: failure is not non-execution.
export function journalTools(tools, { beginEffect, finishEffect, store, owner, observeEffect = () => {} }) {
  // Direct SDK fixtures/embedders still journal; they do not bypass durability.
  if (!beginEffect && !finishEffect && store?.journal && store?.scope) {
    beginEffect = async effect => { const intent = await store.journal.begin('tool-effect', store.scope, { owner, ...effect }); return { stepId: intent.intentId }; };
    finishEffect = async receipt => store.journal.complete(receipt.stepId, receipt);
  }
  if (!beginEffect || !finishEffect) throw fail('Durable tool journal unavailable', 'STORAGE');
  return tools.map(tool => ({ ...tool, execute: async (...args) => {
    const [toolCallId, input, signal] = args;
    signal?.throwIfAborted();
    const inputDigest = effectInputDigest(input);
    const kind = effectKind(tool);
    const intent = await beginEffect({ toolCallId, tool: tool.name, inputDigest, inputCanonical: true, kind });
    observeEffect({ phase: intent.replayRef ? 'replay' : 'intent', stepId: intent.stepId, toolCallId, tool: tool.name, inputDigest, kind });
    if (intent.replayRef) {
      const ref = intent.replayRef, identity = await store.artifactIdentity(ref.artifactId);
      const info = await store.artifactInfo(ref.artifactId);
      if (!identity.complete || ref.complete !== true || identity.owner !== owner || identity.sha256 !== ref.sha256 || identity.bytes !== ref.bytes || info.label !== 'tool-effect-outcome') throw fail('Reconciled tool receipt changed', 'STORAGE');
      const body = await readJson(join(store.root, 'artifacts', `${ref.artifactId}.bin`), store.limits.responseBytes + store.limits.inputBytes);
      if (!body || typeof body !== 'object' || Array.isArray(body)) throw fail('Reconciled tool receipt is invalid', 'STORAGE');
      if (Object.hasOwn(body, 'result') && !Object.hasOwn(body, 'error') && !Object.hasOwn(body, 'failure')) return body.result;
      if (!Object.hasOwn(body, 'result') && typeof body.error === 'string' && body.error && typeof body.code === 'string' && body.code && body.failure) {
        const failure = await verifyKnownToolFailure(store, owner, body.failure);
        throw markToolFailure(fail(body.error, body.code), failure);
      }
      throw fail('Reconciled tool receipt is not a completed result or known failure', 'STORAGE');
    }
    signal?.throwIfAborted();
    let result, error, failed = false;
    try { result = await tool.execute(...args); } catch (e) { failed = true; error = e; }
    const failure = failed && knownToolFailure(kind, error);
    const outcome = failed ? (failure ? 'failed' : 'uncertain') : 'completed';
    let artifact;
    try {
      artifact = await store.artifact(owner, 'tool-effect-outcome');
      const body = Buffer.from(JSON.stringify(failed ? { error: safeError(error) || 'Tool failed', code: typeof error?.code === 'string' && error.code ? error.code : 'TOOL_FAILED', ...(failure ? { failure } : {}) } : { result }));
      for (let offset = 0; offset < body.length; offset += 65536) await artifact.write(body.subarray(offset, offset + 65536));
      const sealed = await artifact.finish(true);
      await finishEffect({ stepId: intent.stepId, outcome, ...(failure ? { failure } : {}), resultRef: { artifactId: sealed.id, bytes: sealed.bytes, sha256: sealed.sha256, complete: sealed.complete } });
      observeEffect({ phase: 'receipt', stepId: intent.stepId, outcome, resultRef: { artifactId: sealed.id, bytes: sealed.bytes, sha256: sealed.sha256, complete: sealed.complete } });
    } catch (e) {
      if (artifact && !artifact.closed) await artifact.finish(false, safeError(e)).catch(() => {});
      throw fail(`Tool outcome could not be durably confirmed: ${safeError(e)}`, 'STORAGE');
    }
    if (failed) throw error;
    return result;
  } }));
}
