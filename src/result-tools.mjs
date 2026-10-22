import { Type } from 'typebox';
import { Check } from 'typebox/value';
import { encodeJson, fail, text } from './limits.mjs';
import { RESULT_KINDS } from './results.mjs';
import { importResultSource } from './result-source.mjs';

const ref = Type.Object({ artifactId: Type.String(), bytes: Type.Integer({ minimum: 0 }), sha256: Type.String({ pattern: '^[a-f0-9]{64}$' }), complete: Type.Literal(true) }, { additionalProperties: false });
const file = Type.Object({ logicalPath: Type.String({ minLength: 1, maxLength: 512 }), kind: Type.String({ enum: RESULT_KINDS }), fileComplete: Type.Boolean(), content: Type.Optional(Type.String({ maxLength: 16384 })), contentRef: Type.Optional(ref), source: Type.Optional(Type.Object({ path: Type.String({ minLength: 1, maxLength: 4096 }), expectedSha256: Type.String({ pattern: '^[a-f0-9]{64}$' }) }, { additionalProperties: false })), mediaType: Type.Optional(Type.String({ maxLength: 100 })) }, { additionalProperties: false });
const parameters = Type.Object({ action: Type.Literal('publish'), expectedHead: Type.Integer({ minimum: 0 }), stage: Type.String({ enum: ['progress', 'candidate', 'final'] }), files: Type.Array(file, { minItems: 1, maxItems: 16 }) }, { additionalProperties: false });

export function resultReportingTool({ task, store, publishResult }) {
  return { name: 'agent_result', label: 'Publish own result revision', description: 'Publish immutable report files from bounded inline UTF-8, your own sealed artifact refs, or a workspace-relative source path plus expectedSha256. Path imports require existing read/bash capability and reject links/private paths. expectedHead is CAS (0 initially); main.md is primary. candidate requires an accepted code-change contract and runtime-owned isolated worktree; runtime generates immutable product snapshot/diff/test inventory. Do not supply changes.jsonl, patches/index.jsonl, tests.jsonl, verification-plan.json or product snapshot files yourself. Candidate readiness is not verification/acceptance. final is irreversible for this result epoch. No external publication authority.', parameters,
    execute: async (toolCallId, input, signal) => {
      signal?.throwIfAborted();
      if (!Check(parameters, input)) throw fail('Invalid agent_result arguments');
      if (typeof publishResult !== 'function') throw fail('Result reporting capability unavailable', 'CAPABILITY');
      if (task.workItem) throw fail('Universal inspect-only tasks do not receive result reporting authority', 'PERMISSION');
      encodeJson(input, 65536);
      const files = [];
      for (const file of input.files) {
        if ([file.content, file.contentRef, file.source].filter(value => value !== undefined).length !== 1) throw fail('Exactly one of content/contentRef/source is required');
        let contentRef = file.contentRef;
        if (file.content !== undefined) {
          const body = text(file.content, 'inline result', 16384, { empty: true });
          const artifact = await store.artifact(task.id, 'result-file-utf8');
          try { signal?.throwIfAborted(); await artifact.write(Buffer.from(body, 'utf8')); const sealed = await artifact.finish(true); contentRef = { artifactId: sealed.id, bytes: sealed.bytes, sha256: sealed.sha256, complete: true }; }
          catch (error) { if (!artifact.closed) await artifact.finish(false, 'Result publication did not seal').catch(() => {}); throw error; }
        }
        if (file.source) contentRef = await importResultSource(store, task, file.source, signal);
        files.push({ logicalPath: file.logicalPath, kind: file.kind, fileComplete: file.fileComplete, contentRef, ...(file.mediaType ? { mediaType: file.mediaType } : {}) });
      }
      signal?.throwIfAborted();
      const bundleRef = await publishResult({ expectedHead: input.expectedHead, stage: input.stage, files }, toolCallId);
      return { content: [{ type: 'text', text: JSON.stringify({ publicationCommitted: true, bundleRef, note: 'Published bytes are not workflow acceptance or trusted verification.' }) }], details: { bundleRef } };
    } };
}

const readParameters = Type.Object({ agentId: Type.String(), source: Type.String({ enum: ['bundle', 'result_file'] }), inputBindingId: Type.Optional(Type.String()), bundleId: Type.Optional(Type.String()), revision: Type.Optional(Type.Integer({ minimum: 1 })), fileId: Type.Optional(Type.String()), view: Type.Optional(Type.String({ enum: ['main', 'reviewer', 'verifier', 'artifacts'] })), cursor: Type.Optional(Type.String({ maxLength: 4096 })), byteOffset: Type.Optional(Type.Integer({ minimum: 0 })), limitBytes: Type.Optional(Type.Integer({ minimum: 1, maximum: 16384 })) }, { additionalProperties: false });
export function resultReadingTool({ readResult }) {
  return { name: 'agent_read', label: 'Read granted result revision', description: 'Read your own published result or an exact durable resultInputs binding. Supply the inputBindingId from the assignment for peers. Views/cursors do not expand grants. Read only: no publication, send, spawn, cancel, acknowledgement or model wake. Follow returned agent_read arguments for paging.', parameters: readParameters,
    execute: async (_toolCallId, input, signal) => {
      signal?.throwIfAborted();
      if (!Check(readParameters, input) || typeof readResult !== 'function') throw fail('Result read capability/arguments unavailable', 'CAPABILITY');
      const page = await readResult(input); signal?.throwIfAborted();
      return { content: [{ type: 'text', text: encodeJson(page, 32768).toString('utf8') }], details: { source: input.source, read: page.read } };
    } };
}
