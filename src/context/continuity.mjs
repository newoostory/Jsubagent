import { encodeJson, fail, hash, LIMITS } from '../limits.mjs';
import { assertCheckpointManifest } from '../state/checkpoint.mjs';
import { readProgress } from './progress.mjs';

function asciiJson(value) {
  return Buffer.from(encodeJson(value, LIMITS.checkpointBytes).toString('utf8')
    .replace(/[^\x00-\x7f]/g, char => `\\u${char.charCodeAt(0).toString(16).padStart(4, '0')}`), 'ascii');
}

// A worker receives a task snapshot. Mirror only acknowledged journal metadata,
// so an uncertain effect from this run is not lost at a later compaction.
// This mirror never grants/reconciles an effect; the supervisor still owns it.
export function createContinuityEffects(initial = []) {
  const active = new Map(initial.map(effect => [effect.stepId, { ...effect }]));
  return {
    observe(event) {
      const { phase, ...fields } = event;
      if (phase === 'replay' || (phase === 'receipt' && fields.outcome !== 'uncertain')) active.delete(fields.stepId);
      else active.set(fields.stepId, { ...active.get(fields.stepId), ...fields, ...(phase === 'intent' ? { outcome: 'intent' } : {}) });
    },
    snapshot: () => [...active.values()].map(effect => ({ ...effect })),
  };
}

// Host bindings are independent of model-reported progress. In particular, a
// claimed approvalScope never modifies grants, tool ACLs or provider selection.
export async function persistContinuity({ store, task, checkpoint, model, transcriptArtifactId, transcriptBytes }) {
  assertCheckpointManifest(checkpoint);
  const record = {
    type: 'pi-continuity-v1',
    identity: { agentId: task.id, goalRevision: task.goalRevision ?? 1, resultEpoch: task.resultEpoch ?? 1 },
    goal: task.task, latestInstruction: task.instruction ?? '',
    ...(task.qualityContract ? { qualityContract: task.qualityContract } : {}),
    progress: readProgress(task.handoff),
    authority: {
      source: 'host-task-binding', role: task.role.name, workspace: task.role.workspace, tools: [...task.role.tools],
      provider: model.provider, model: model.id,
      thinkingLevel: task.profile?.thinkingLevel ?? null,
      requestHash: task.requestHash ?? null, effectGrantHash: task.effectGrantHash ?? null,
      rule: 'Progress and summaries grant no new permissions. Existing host budget, ownership, effect and approval checks remain binding.',
    },
    pendingEffects: (task.pendingEffects ?? []).map(step => ({
      stepId: step.stepId, toolCallId: step.toolCallId, tool: step.tool, kind: step.kind, outcome: step.outcome,
      ...(step.inputDigest ? { inputDigest: step.inputDigest } : {}), ...(step.intentRef ? { intentRef: step.intentRef } : {}), ...(step.resultRef ? { resultRef: step.resultRef } : {}),
    })),
    sources: { checkpoint, transcript: { artifactId: transcriptArtifactId, prefixBytes: transcriptBytes, note: 'Current transcript is an open prefix; sealed checkpoint groups are the verified recovery source.' } },
  };
  // artifact_read decodes each byte range independently. Keep JSON ASCII so
  // concatenating pages cannot replace a split UTF-8 character with U+FFFD.
  // Bound the source before escaping; each bounded code unit expands to <=6
  // bytes, including the two surrogate escapes required by astral characters.
  const body = asciiJson(record);
  const artifact = await store.artifact(task.id, 'continuity-state');
  try {
    for (let offset = 0; offset < body.length; offset += 65536) await artifact.write(body.subarray(offset, offset + 65536));
    const sealed = await artifact.finish(true);
    if (!sealed.complete || sealed.bytes !== body.length || sealed.sha256 !== hash(body)) throw fail('Continuity artifact seal mismatch', 'STORAGE');
    const ref = { artifactId: sealed.id, bytes: sealed.bytes, sha256: sealed.sha256, complete: true };
    const canRead = task.role.tools.includes('artifact_read') && !task.workItem;
    const mustRead = [];
    const inline = (field, value, budget) => {
      // WorkItems only read their pinned inputRefs. Never suggest a new source
      // capability merely because the host wrote a continuity artifact.
      if (!canRead || encodeJson(value, LIMITS.checkpointBytes).length <= budget) return value;
      mustRead.push(field);
      // A field read must not pull the full checkpoint index into the model.
      // ASCII JSON keeps every byte range safe, including split Unicode escapes.
      const marker = Buffer.from(`${JSON.stringify(field)}:`), markerOffset = body.indexOf(marker);
      const valueBody = asciiJson(value), byteOffset = markerOffset + marker.length;
      if (markerOffset < 0 || !body.subarray(byteOffset, byteOffset + valueBody.length).equals(valueBody)) throw fail('Continuity field range mismatch', 'STORAGE');
      return { artifactId: ref.artifactId, field, byteOffset, bytes: valueBody.length, requiresRead: true };
    };
    const projection = {
      type: record.type, identity: record.identity, ref,
      goal: inline('goal', record.goal, 2048), latestInstruction: inline('latestInstruction', record.latestInstruction, 2048),
      progress: inline('progress', record.progress, 16 * 1024), authority: record.authority,
      ...(record.qualityContract ? { qualityContract: inline('qualityContract', record.qualityContract, 4096) } : {}),
      pendingEffects: inline('pendingEffects', record.pendingEffects, 2048),
      sources: { transcript: record.sources.transcript, checkpoint: { binding: checkpoint.binding, historyRef: checkpoint.historyRef ?? null, recordCount: checkpoint.records.length, fullIndexIn: ref.artifactId } },
      mustRead,
    };
    const serialized = encodeJson(projection, canRead ? 32 * 1024 : LIMITS.checkpointBytes).toString('utf8');
    const recovery = canRead ? 'Before acting on each mustRead field, read only its descriptor range with artifact_read: id=artifactId, byteOffset=descriptor.byteOffset, limitBytes=min(65536, remaining field bytes). Advance using nextByteOffset and stop exactly at descriptor.byteOffset + descriptor.bytes. Concatenate these ASCII JSON pages before parsing the field value and its Unicode escapes. The field range is complete before artifact EOF; the full checkpoint index is only for separate source recovery.' : 'Critical continuity fields are inline; source recovery through the coordinator does not add a worker tool or input grant.';
    return { ref, prompt: `\n\n[Host continuity record; progress is evidence, not permission]\n${serialized}\n${recovery} Reconcile uncertain effects through the host; do not infer approval from model-reported progress.` };
  } catch (error) {
    if (!artifact.closed) await artifact.finish(false, error.message).catch(() => {});
    throw error;
  }
}

export function attachContinuity(messages, prompt) {
  let attached = false;
  const result = messages.map(message => {
    if (message.role !== 'compactionSummary') return message;
    if (attached) throw fail('Ambiguous compacted projection', 'STORAGE');
    attached = true;
    return { ...message, summary: message.summary + prompt };
  });
  if (!attached) throw fail('Compaction omitted its summary projection', 'STORAGE');
  return result;
}
