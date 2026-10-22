import { Type } from 'typebox';
import { Check } from 'typebox/value';
import { planSpawnBatch } from './batch-plan.mjs';
import { text, encodeJson, fail, LIMITS } from './limits.mjs';
import { PUBLIC_RESULT_FEATURES, PUBLIC_RESULT_FIELD_DESCRIPTIONS } from './public-contract.mjs';

const opt = Type.Optional;
const id = () => Type.String({ pattern: '^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$', maxLength: 80 });
const ids = () => Type.Array(id(), { minItems: 1, maxItems: 8, uniqueItems: true });
const string = (bytes, options = {}) => Type.String({ ...options, minLength: 1, maxLength: bytes, pattern: '\\S' });
const object = properties => Type.Object(properties, { additionalProperties: false });
const choice = (values, options = {}) => Type.String({ ...options, enum: [...values] });
const defaults = { role: opt(id()), profile: opt(id()), directory: opt(string(4096)), dependencyHandoff: opt(choice(['summary', 'none'])) };
const resultSelector = {
  view: opt(choice(PUBLIC_RESULT_FEATURES.selector.views)),
  files: opt(Type.Array(string(PUBLIC_RESULT_FEATURES.selector.fileMaxBytes), { minItems: 1, maxItems: PUBLIC_RESULT_FEATURES.selector.fileMaxItems, uniqueItems: true })),
  kinds: opt(Type.Array(string(PUBLIC_RESULT_FEATURES.selector.kindMaxBytes), { minItems: 1, maxItems: PUBLIC_RESULT_FEATURES.selector.kindMaxItems, uniqueItems: true })),
};
const resultInput = object({
  from: Type.Union([object({ agentId: id() }), object({ task: Type.Integer({ minimum: 0, maximum: 7 }) })]),
  ...resultSelector,
  when: opt(choice(PUBLIC_RESULT_FEATURES.milestone.values, { description: PUBLIC_RESULT_FIELD_DESCRIPTIONS.inputMilestone })),
  revision: opt(Type.Integer({ minimum: 1 })),
  required: opt(Type.Boolean()),
});
const resultRoute = object({
  to: Type.Union([object({ agentId: id() }), object({ task: Type.Integer({ minimum: 0, maximum: 7 }) })]),
  ...resultSelector,
  when: opt(choice(PUBLIC_RESULT_FEATURES.milestone.values, { description: PUBLIC_RESULT_FIELD_DESCRIPTIONS.routeMilestone })),
  follow: opt(choice(PUBLIC_RESULT_FEATURES.publishTo.followValues)),
});
const verificationCheck = object({
  id: id(),
  command: string(PUBLIC_RESULT_FEATURES.verificationChecks.commandMaxBytes, { description: PUBLIC_RESULT_FIELD_DESCRIPTIONS.verificationCommand }),
  timeoutMs: opt(Type.Integer({ minimum: PUBLIC_RESULT_FEATURES.verificationChecks.timeoutMinMs, maximum: PUBLIC_RESULT_FEATURES.verificationChecks.timeoutMaxMs, description: PUBLIC_RESULT_FIELD_DESCRIPTIONS.verificationTimeout })),
});
const qualityContract = object({
  domain: choice(['general', 'translation', 'coding', 'research']),
  stage: opt(choice(['produce', 'review', 'verify', 'integrate'])),
  criteria: Type.Array(object({ id: id(), description: string(2048) }), { minItems: 1, maxItems: 16 }),
  preserve: opt(Type.Array(string(256), { maxItems: 16, uniqueItems: true })),
  unresolvedEffects: opt(Type.Array(id(), { maxItems: 16, uniqueItems: true })),
  ownedPaths: opt(Type.Array(string(512), { maxItems: 16, uniqueItems: true })),
  review: opt(choice(['optional', 'required'])),
});
const task = object({
  ...defaults,
  name: opt(string(32)),
  task: string(24576),
  qualityContract: opt(qualityContract),
  resultContract: opt(choice(PUBLIC_RESULT_FEATURES.resultContract.values, { description: PUBLIC_RESULT_FIELD_DESCRIPTIONS.resultContract })),
  verificationChecks: opt(Type.Array(verificationCheck, { maxItems: PUBLIC_RESULT_FEATURES.verificationChecks.maxItems, description: PUBLIC_RESULT_FIELD_DESCRIPTIONS.verificationChecks })),
  resultInputs: opt(Type.Array(resultInput, { maxItems: PUBLIC_RESULT_FEATURES.resultInputs.maxItems, description: PUBLIC_RESULT_FIELD_DESCRIPTIONS.resultInputs })),
  publishTo: opt(Type.Array(resultRoute, { maxItems: PUBLIC_RESULT_FEATURES.publishTo.maxItems, description: PUBLIC_RESULT_FIELD_DESCRIPTIONS.publishTo })),
  dependsOn: opt(Type.Array(Type.Union([id(), Type.Number({ minimum: 0, maximum: 7, multipleOf: 1 })]), { maxItems: 8, uniqueItems: true })),
});
const range = { byteOffset: opt(Type.Integer({ minimum: 0 })), limitBytes: opt(Type.Integer({ minimum: 1, maximum: 32768 })) };
const request = object({ agentId: opt(id()), source: opt(choice(['index', 'result', 'error', 'handoff', 'transcript', 'message', 'artifact', 'receipt', 'bundle', 'result_file'])), artifactId: opt(id()), messageId: opt(id()), bundleId: opt(id()), revision: opt(Type.Integer({ minimum: 1 })), fileId: opt(id()), view: opt(choice(PUBLIC_RESULT_FEATURES.selector.views)), cursor: opt(string(4096)), ...range });
const format = opt(choice(['summary', 'full']));
const definitions = {
  catalog: {},
  spawn: { tasks: Type.Array(task, { minItems: 1, maxItems: 8 }), defaults: opt(object(defaults)) },
  status: { agentIds: opt(ids()), format },
  wait: { agentIds: ids(), mode: opt(choice(['all', 'any'])), format, results: opt(choice(['new', 'all', 'none'])), wakeOnMessage: opt(Type.Boolean({ description: 'Wake on unread questions/blockers (default true). No time limit; abort stops only this wait.' })) },
  send: { agentIds: ids(), body: string(8192), delivery: opt(choice(['steer', 'followUp'])), replyTo: opt(id()) },
  cancel: { agentIds: ids() },
  resume: { agentId: id(), instruction: string(16384) },
  archive: { agentIds: ids() },
  read: { requests: Type.Array(request, { minItems: 1, maxItems: 8 }) },
};
const properties = {};
for (const fields of Object.values(definitions)) for (const [key, value] of Object.entries(fields)) {
  if (properties[key] && JSON.stringify(properties[key]) !== JSON.stringify(value)) throw new Error(`Conflicting agent field: ${key}`);
  properties[key] = value;
}
export const PublicAgentParameters = Type.Object({ action: choice(Object.keys(definitions)), ...Object.fromEntries(Object.entries(properties).map(([key, value]) => [key, opt(value)])) }, {
  additionalProperties: false,
  allOf: [{ anyOf: Object.entries(definitions).map(([action, fields]) => {
    const schema = object({ action: choice([action]), ...fields });
    return { ...schema, properties: Object.fromEntries(Object.entries(schema.properties).map(([key, value]) => [key, key === 'action' ? value : Type.Unknown()])) };
  }) }],
  description: 'One action per call. spawn uses tasks (1–8) and optional defaults; status/send/cancel/archive use agentIds; resume uses agentId/instruction; read uses requests. Summary is the default. No unrelated fields.',
});

export function validatePublicAgentParameters(params) {
  if (!Check(PublicAgentParameters, params)) throw fail('Invalid agent action/fields. Use the canonical action-specific schema.');
  encodeJson(params, LIMITS.inputBytes);
  if (params.action === 'spawn') planSpawnBatch(params);
  if (params.action === 'send') {
    text(params.body, 'body', 8192);
    if (params.replyTo && params.agentIds.length !== 1) throw fail('replyTo requires exactly one recipient');
  }
  if (params.action === 'resume') text(params.instruction, 'instruction', 16384);
  if (params.action === 'read') {
    const seen = new Set();
    for (const p of params.requests) {
      const source = p.source ?? 'index';
      if (source !== 'artifact' && !p.agentId) throw fail('Read source requires agentId');
      if (['artifact', 'message'].includes(source) && !p.artifactId) throw fail(`${source} requires artifactId`);
      if (source === 'receipt' && !p.messageId) throw fail('Receipt requires messageId');
      if (source !== 'receipt' && p.messageId) throw fail('messageId is only valid for receipt');
      if (['index', 'receipt'].includes(source) && (p.artifactId !== undefined || p.byteOffset !== undefined || p.limitBytes !== undefined)) throw fail('Index/receipt accepts no artifact or range');
      if (p.byteOffset !== undefined && !Number.isSafeInteger(p.byteOffset)) throw fail('Invalid read byteOffset');
      const resultSource = ['bundle', 'result_file'].includes(source);
      if (!resultSource && ['bundleId', 'revision', 'fileId', 'view', 'cursor'].some(key => p[key] !== undefined)) throw fail('Result fields require bundle/result_file');
      if (resultSource && (p.artifactId !== undefined || p.messageId !== undefined)) throw fail('Bundle reads use logical identities, not artifact/message IDs');
      if (source === 'bundle' && (p.fileId !== undefined || p.byteOffset !== undefined || p.limitBytes !== undefined)) throw fail('Bundle listing accepts no file/range');
      if (p.revision !== undefined && (!p.bundleId || !Number.isSafeInteger(p.revision))) throw fail('Revision requires bundleId');
      if (p.cursor !== undefined && (source !== 'bundle' || p.view !== undefined || p.bundleId !== undefined || p.revision !== undefined)) throw fail('Cursor cannot be combined with another selector');
      if (source === 'result_file' && (!p.bundleId || !p.revision || !p.fileId || p.view !== undefined || p.cursor !== undefined)) throw fail('result_file requires bundleId/revision/fileId and a range only');
      const key = JSON.stringify([p.agentId, source, p.artifactId, p.messageId, p.bundleId, p.revision, p.fileId, p.view, p.cursor, p.byteOffset ?? 0, p.limitBytes ?? 4096]);
      if (seen.has(key)) throw fail('Duplicate read request'); seen.add(key);
    }
  }
  return params;
}
