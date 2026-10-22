import { createHash } from 'node:crypto';
import { encodeJson, fail, identifier, LIMITS } from '../limits.mjs';
import { describeContextGroup, groupContextMessages, jsonBytes, measureResidentContext } from '../context/records.mjs';

const CHECKPOINT_TYPE = 'checkpoint-manifest';
const CHECKPOINT_KEYS = new Set(['type', 'binding', 'records', 'residentRecordIds', 'completedToolCallIds', 'mailIds', 'historyRef', 'continuityRef', 'compactionEnvelope', 'resultSections', 'transcriptArtifactId', 'transcriptBytes', 'residentContextBytes']);
const BINDING_KEYS = new Set(['provider', 'model', 'configSha256']);
const ENVELOPE_KEYS = new Set(['type', 'ref']);
const RESULT_SECTION_KEYS = new Set(['type', 'requiredSections', 'sections', 'head', 'complete']);
const RESULT_SECTION_ENTRY_KEYS = new Set(['sectionId', 'revision', 'ref']);
const RECORD_KEYS = new Set(['type', 'messageCount', 'roles', 'toolCallIds', 'contentBytes', 'contentSha256', 'ref']);
const REF_KEYS = new Set(['artifactId', 'bytes', 'sha256', 'complete']);

function exactKeys(value, keys, name) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.getPrototypeOf(value) !== Object.prototype) throw fail(`Invalid ${name}`, 'STORAGE');
  for (const key of Object.keys(value)) if (!keys.has(key)) throw fail(`Unsupported ${name} field: ${key}`, 'STORAGE');
  return value;
}

function unsigned(value, name) {
  if (!Number.isSafeInteger(value) || value < 0) throw fail(`Invalid ${name}`, 'STORAGE');
  return value;
}

function digest(value) { return createHash('sha256').update(value).digest('hex'); }
function sha256(value) {
  if (typeof value !== 'string' || !/^[a-f0-9]{64}$/.test(value)) throw fail('Invalid checkpoint artifact digest', 'STORAGE');
  return value;
}

function sameArray(left, right) {
  if (left === undefined || right === undefined) return left === right;
  return Array.isArray(left) && Array.isArray(right) && left.length === right.length && left.every((value, index) => value === right[index]);
}

function validateBinding(binding, name = 'checkpoint binding') {
  exactKeys(binding, BINDING_KEYS, name);
  for (const key of ['provider', 'model']) if (typeof binding[key] !== 'string' || !binding[key] || Buffer.byteLength(binding[key]) > 256) throw fail(`Invalid ${name} ${key}`, 'STORAGE');
  if (typeof binding.configSha256 !== 'string' || !/^[a-f0-9]{64}$/.test(binding.configSha256)) throw fail(`Invalid ${name} configuration digest`, 'STORAGE');
  return binding;
}

function bindingMatches(left, right) {
  return left?.provider === right?.provider && left?.model === right?.model && left?.configSha256 === right?.configSha256;
}

export function assertCheckpointBinding(checkpoint, expected) {
  const manifest = assertCheckpointManifest(checkpoint);
  validateBinding(expected, 'expected checkpoint binding');
  if (!bindingMatches(manifest.binding, expected)) throw fail('Checkpoint provider/model/configuration binding mismatch; explicit fresh recovery is required', 'STORAGE');
  return manifest;
}

function validateRecord(record, index) {
  exactKeys(record, RECORD_KEYS, `checkpoint record ${index}`);
  if (!['message', 'tool'].includes(record.type)) throw fail('Invalid checkpoint record type', 'STORAGE');
  if (!Number.isSafeInteger(record.messageCount) || record.messageCount < 1) throw fail('Invalid checkpoint record message count', 'STORAGE');
  if (!Array.isArray(record.roles) || record.roles.length !== record.messageCount || record.roles.some(role => typeof role !== 'string' || !role)) throw fail('Invalid checkpoint record roles', 'STORAGE');
  if (record.type === 'message') {
    if (record.messageCount !== 1 || record.toolCallIds !== undefined) throw fail('Invalid single-message checkpoint record', 'STORAGE');
  } else if (!Array.isArray(record.toolCallIds) || !record.toolCallIds.length || record.toolCallIds.some(id => typeof id !== 'string' || !id) || new Set(record.toolCallIds).size !== record.toolCallIds.length || record.messageCount !== record.toolCallIds.length + 1) {
    throw fail('Invalid checkpoint tool record', 'STORAGE');
  }
  unsigned(record.contentBytes, 'checkpoint content bytes');
  sha256(record.contentSha256);
  const ref = exactKeys(record.ref, REF_KEYS, `checkpoint record ${index} reference`);
  identifier(ref.artifactId, 'checkpoint artifactId');
  unsigned(ref.bytes, 'checkpoint artifact bytes');
  sha256(ref.sha256);
  if (ref.complete !== true) throw fail('Checkpoint artifact is incomplete', 'STORAGE');
  return record;
}

function validateRef(value, name) {
  const ref = exactKeys(value, REF_KEYS, name);
  identifier(ref.artifactId, `${name} artifactId`); unsigned(ref.bytes, `${name} bytes`); sha256(ref.sha256);
  if (ref.complete !== true) throw fail(`Incomplete ${name}`, 'STORAGE');
  return ref;
}

function validateCompactionEnvelope(value) {
  exactKeys(value, ENVELOPE_KEYS, 'checkpoint compaction envelope');
  if (value.type !== 'opaque-compaction-envelope') throw fail('Unsupported checkpoint compaction envelope', 'STORAGE');
  validateRef(value.ref, 'checkpoint compaction envelope reference');
  return value;
}

export function assertResultSections(value) {
  exactKeys(value, RESULT_SECTION_KEYS, 'result section manifest');
  if (value.type !== 'result-section-manifest' || !Array.isArray(value.requiredSections) || !value.requiredSections.length || value.requiredSections.length > 256 || new Set(value.requiredSections).size !== value.requiredSections.length) throw fail('Invalid result section manifest coverage', 'STORAGE');
  value.requiredSections.forEach(id => identifier(id, 'result sectionId'));
  if (!Array.isArray(value.sections) || value.sections.length > value.requiredSections.length) throw fail('Invalid committed result sections', 'STORAGE');
  const ids = new Set();
  for (const row of value.sections) {
    exactKeys(row, RESULT_SECTION_ENTRY_KEYS, 'committed result section'); identifier(row.sectionId, 'result sectionId');
    if (!value.requiredSections.includes(row.sectionId) || ids.has(row.sectionId) || !Number.isSafeInteger(row.revision) || row.revision < 1) throw fail('Invalid committed result section identity', 'STORAGE');
    ids.add(row.sectionId);
    const { codec, ...ref } = row.ref ?? {};
    if (codec !== undefined && codec !== 'utf8') throw fail('Unsupported result section codec', 'STORAGE');
    validateRef(ref, 'result section reference');
  }
  if (typeof value.head !== 'string' || !/^[a-f0-9]{64}$/.test(value.head) || typeof value.complete !== 'boolean' || (value.complete && ids.size !== value.requiredSections.length)) throw fail('Invalid result section manifest head/completion', 'STORAGE');
  return value;
}

export function emptyCheckpoint(binding) {
  if (binding !== undefined) validateBinding(binding);
  return { type: CHECKPOINT_TYPE, ...(binding ? { binding: { ...binding } } : {}), records: [], residentRecordIds: [], completedToolCallIds: [], mailIds: [], transcriptBytes: 0, residentContextBytes: 0 };
}

export function assertCheckpointManifest(checkpoint, maxBytes = LIMITS.checkpointBytes) {
  exactKeys(checkpoint, CHECKPOINT_KEYS, 'checkpoint manifest');
  if (checkpoint.type !== CHECKPOINT_TYPE || !Array.isArray(checkpoint.records)) throw fail('Unsupported checkpoint schema; explicit fresh recovery is required', 'STORAGE');
  if (checkpoint.binding !== undefined) validateBinding(checkpoint.binding);
  checkpoint.records.forEach(validateRecord);
  const recordById = new Map();
  for (const record of checkpoint.records) {
    if (recordById.has(record.ref.artifactId)) throw fail('Duplicate checkpoint record artifact', 'STORAGE');
    recordById.set(record.ref.artifactId, record);
  }
  if (!Array.isArray(checkpoint.residentRecordIds) || checkpoint.residentRecordIds.some(id => typeof id !== 'string' || !recordById.has(id))) throw fail('Invalid resident checkpoint projection', 'STORAGE');
  if (!Array.isArray(checkpoint.completedToolCallIds) || checkpoint.completedToolCallIds.some(id => typeof id !== 'string' || !id) || new Set(checkpoint.completedToolCallIds).size !== checkpoint.completedToolCallIds.length) throw fail('Invalid completed tool call checkpoint acknowledgements', 'STORAGE');
  const residentRecords = checkpoint.residentRecordIds.map(id => recordById.get(id));
  const describedToolCalls = new Set(residentRecords.filter(record => record.type === 'tool').flatMap(record => record.toolCallIds));
  if (checkpoint.completedToolCallIds.some(id => !describedToolCalls.has(id)) || describedToolCalls.size !== checkpoint.completedToolCallIds.length) throw fail('Checkpoint tool acknowledgements do not match the resident durable tool groups', 'STORAGE');
  if (!Array.isArray(checkpoint.mailIds) || checkpoint.mailIds.length > 32 || checkpoint.mailIds.some(id => typeof id !== 'string')) throw fail('Invalid checkpoint mail acknowledgements', 'STORAGE');
  if (checkpoint.historyRef) { const ref = exactKeys(checkpoint.historyRef, REF_KEYS, 'checkpoint history reference'); identifier(ref.artifactId); unsigned(ref.bytes, 'history bytes'); sha256(ref.sha256); if (!ref.complete) throw fail('Incomplete checkpoint history', 'STORAGE'); }
  if (checkpoint.continuityRef) { if (!checkpoint.binding) throw fail('Continuity requires a provider/model/configuration binding', 'STORAGE'); validateRef(checkpoint.continuityRef, 'checkpoint continuity reference'); }
  if (checkpoint.compactionEnvelope) { if (!checkpoint.binding) throw fail('Opaque compaction envelope requires provider/model/configuration binding', 'STORAGE'); validateCompactionEnvelope(checkpoint.compactionEnvelope); }
  if (checkpoint.resultSections) { if (!checkpoint.binding) throw fail('Result sections require provider/model/configuration binding', 'STORAGE'); assertResultSections(checkpoint.resultSections); }
  if (checkpoint.transcriptArtifactId !== undefined) identifier(checkpoint.transcriptArtifactId, 'checkpoint transcriptArtifactId');
  unsigned(checkpoint.transcriptBytes, 'checkpoint transcriptBytes');
  unsigned(checkpoint.residentContextBytes, 'checkpoint residentContextBytes');
  encodeJson(checkpoint, maxBytes);
  return checkpoint;
}

export function checkpointRecordCount(checkpoint) {
  if (checkpoint === undefined) return 0;
  const manifest = assertCheckpointManifest(checkpoint), byId = new Map(manifest.records.map(record => [record.ref.artifactId, record]));
  return manifest.residentRecordIds.reduce((total, id) => total + byId.get(id).messageCount, 0);
}

function reuseIndex(previous) {
  const records = new Map();
  for (const record of previous?.records ?? []) {
    const key = `${record.contentSha256}:${record.contentBytes}:${record.messageCount}`;
    const available = records.get(key) ?? [];
    available.push(record);
    records.set(key, available);
  }
  return records;
}

async function persistBody(store, owner, body) {
  // RunStore range reads expose UTF-8 text and may split multibyte input. Store
  // an ASCII envelope so every bounded page can be reconstructed byte-exactly.
  const payloadHash = createHash('sha256'), payloadBytes = Math.ceil(body.length / 3) * 4;
  const artifact = await store.artifact(owner, 'checkpoint-context-group');
  try {
    // Base64 blocks end on three-byte boundaries; never allocate a second
    // full encoded copy of the resident context group.
    for (let offset = 0; offset < body.length; offset += 49152) {
      const chunk = Buffer.from(body.subarray(offset, offset + 49152).toString('base64'), 'ascii');
      payloadHash.update(chunk); await artifact.write(chunk);
    }
    const sealed = await artifact.finish(true);
    if (!sealed.complete || sealed.bytes !== payloadBytes || sealed.sha256 !== payloadHash.digest('hex')) throw fail('Checkpoint artifact seal mismatch', 'STORAGE');
    return { artifactId: sealed.id, bytes: sealed.bytes, sha256: sealed.sha256, complete: true };
  } catch (error) {
    if (!artifact.closed) await artifact.finish(false, error?.message).catch(() => {});
    throw error;
  }
}

export async function persistCompactionEnvelope({ store, owner, envelope }) {
  identifier(owner, 'checkpoint owner');
  if (!store || typeof store.artifact !== 'function') throw fail('Checkpoint artifact store unavailable', 'STORAGE');
  const body = encodeJson(envelope, Math.min(store.limits?.contextBytes ?? LIMITS.contextBytes, store.limits?.artifactBytes ?? LIMITS.artifactBytes));
  const artifact = await store.artifact(owner, 'checkpoint-provider-envelope');
  try {
    for (let offset = 0; offset < body.length; offset += 65536) await artifact.write(body.subarray(offset, offset + 65536));
    const sealed = await artifact.finish(true);
    if (!sealed.complete || sealed.bytes !== body.length || sealed.sha256 !== digest(body)) throw fail('Compaction envelope artifact seal mismatch', 'STORAGE');
    return { type: 'opaque-compaction-envelope', ref: { artifactId: sealed.id, bytes: sealed.bytes, sha256: sealed.sha256, complete: true } };
  } catch (error) {
    if (!artifact.closed) await artifact.finish(false, error?.message).catch(() => {});
    throw error;
  }
}

export async function persistCheckpoint({ store, owner, messages, previous, binding, continuityRef, compactionEnvelope, resultSections, mailIds, transcriptArtifactId, transcriptBytes = 0, maxManifestBytes = LIMITS.checkpointBytes }) {
  identifier(owner, 'checkpoint owner');
  if (!store || typeof store.artifact !== 'function') throw fail('Checkpoint artifact store unavailable', 'STORAGE');
  if (binding !== undefined) validateBinding(binding);
  if (continuityRef !== undefined) validateRef(continuityRef, 'checkpoint continuity reference');
  if (compactionEnvelope !== undefined) validateCompactionEnvelope(compactionEnvelope);
  if (resultSections !== undefined && resultSections !== null) assertResultSections(resultSections);
  const prior = previous === undefined ? emptyCheckpoint(binding) : assertCheckpointManifest(previous, maxManifestBytes);
  if (binding && prior.binding && !bindingMatches(binding, prior.binding)) throw fail('Checkpoint provider/model/configuration binding changed', 'STORAGE');
  if (binding && !prior.binding && (prior.records.length || prior.historyRef || prior.compactionEnvelope || prior.resultSections || prior.transcriptArtifactId)) throw fail('Unbound checkpoint records cannot enter a provider continuation', 'STORAGE');
  const groups = groupContextMessages(messages);
  const available = reuseIndex(prior), residentRecords = []; let records = [...prior.records], historyRef = prior.historyRef;
  const completedToolCallIds = [];
  for (const group of groups) {
    const description = describeContextGroup(group);
    const body = encodeJson(group, Math.min(store.limits?.contextBytes ?? LIMITS.contextBytes, store.limits?.artifactBytes ?? LIMITS.artifactBytes));
    const contentSha256 = digest(body);
    const key = `${contentSha256}:${body.length}:${group.length}`;
    const reused = available.get(key)?.shift();
    let record = reused;
    if (!record) {
      const ref = await persistBody(store, owner, body);
      record = { ...description, contentBytes: body.length, contentSha256, ref };
      records.push(record);
    }
    residentRecords.push(record);
    if (description.type === 'tool') completedToolCallIds.push(...description.toolCallIds);
  }
  if (records.length > 256) {
    const residentIds = new Set(residentRecords.map(record => record.ref.artifactId));
    const historical = records.filter(record => !residentIds.has(record.ref.artifactId));
    if (historical.length) {
      const index = await store.artifact(owner, 'checkpoint-history-index');
      try { const body = Buffer.from(JSON.stringify({ previous: historyRef ?? null, records: historical })); for (let offset = 0; offset < body.length; offset += 65536) await index.write(body.subarray(offset, offset + 65536)); const sealed = await index.finish(true); historyRef = { artifactId: sealed.id, bytes: sealed.bytes, sha256: sealed.sha256, complete: true }; }
      catch (error) { if (!index.closed) await index.finish(false, error.message).catch(() => {}); throw error; }
      records = records.filter(record => residentIds.has(record.ref.artifactId));
    }
  }
  const checkpoint = {
    type: CHECKPOINT_TYPE,
    ...((binding ?? prior.binding) ? { binding: { ...(binding ?? prior.binding) } } : {}),
    records,
    residentRecordIds: residentRecords.map(record => record.ref.artifactId),
    completedToolCallIds,
    mailIds: [...new Set(mailIds ?? prior.mailIds)].slice(-32),
    ...(historyRef ? { historyRef } : {}),
    ...((continuityRef ?? prior.continuityRef) ? { continuityRef: { ...(continuityRef ?? prior.continuityRef) } } : {}),
    ...((compactionEnvelope ?? prior.compactionEnvelope) ? { compactionEnvelope: { ...(compactionEnvelope ?? prior.compactionEnvelope), ref: { ...(compactionEnvelope ?? prior.compactionEnvelope).ref } } } : {}),
    ...((resultSections === null ? undefined : (resultSections ?? prior.resultSections)) ? { resultSections: JSON.parse(JSON.stringify(resultSections ?? prior.resultSections)) } : {}),
    ...(transcriptArtifactId === undefined ? {} : { transcriptArtifactId: identifier(transcriptArtifactId, 'checkpoint transcriptArtifactId') }),
    transcriptBytes: unsigned(transcriptBytes, 'checkpoint transcriptBytes'),
    residentContextBytes: measureResidentContext(messages),
  };
  return assertCheckpointManifest(checkpoint, maxManifestBytes);
}

async function readArtifactBody(store, record, owner, maxBytes) {
  const { ref } = record;
  const identity = await store.artifactIdentity(ref.artifactId);
  if (identity.owner !== owner || identity.bytes !== ref.bytes || identity.sha256 !== ref.sha256 || identity.complete !== true) throw fail('Checkpoint artifact identity mismatch', 'STORAGE');
  if (record.contentBytes > maxBytes || ref.bytes !== Math.ceil(record.contentBytes / 3) * 4) throw fail('Checkpoint context exceeds the resident byte budget', 'LIMIT');
  const body = Buffer.alloc(record.contentBytes), encodedHash = createHash('sha256');
  const window = Math.floor(Math.min(store.limits?.readBytes ?? LIMITS.readBytes, 65536) / 4) * 4;
  if (window < 4) throw fail('Checkpoint read window cannot fit one base64 group', 'LIMIT');
  let offset = 0, decodedOffset = 0;
  while (offset < ref.bytes) {
    const page = await store.readArtifact(ref.artifactId, offset, Math.min(window, ref.bytes - offset));
    if (page.byteOffset !== offset || !Number.isSafeInteger(page.bytesRead) || page.bytesRead < 1 || page.bytesRead % 4 || page.nextByteOffset !== offset + page.bytesRead || page.fileBytes !== ref.bytes || Buffer.byteLength(page.text, 'ascii') !== page.bytesRead) throw fail('Checkpoint artifact range mismatch', 'STORAGE');
    const final = page.nextByteOffset === ref.bytes;
    if (!(final ? /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/ : /^(?:[A-Za-z0-9+/]{4})*$/).test(page.text)) throw fail('Checkpoint artifact content mismatch', 'STORAGE');
    encodedHash.update(page.text, 'ascii');
    const chunk = Buffer.from(page.text, 'base64');
    if (chunk.toString('base64') !== page.text || decodedOffset + chunk.length > body.length) throw fail('Checkpoint artifact content mismatch', 'STORAGE');
    chunk.copy(body, decodedOffset); decodedOffset += chunk.length; offset = page.nextByteOffset;
  }
  if (encodedHash.digest('hex') !== ref.sha256 || decodedOffset !== record.contentBytes || digest(body) !== record.contentSha256) throw fail('Checkpoint record content mismatch', 'STORAGE');
  return body;
}

function compareDescription(record, messages) {
  const actual = describeContextGroup(messages);
  if (record.type !== actual.type || record.messageCount !== actual.messageCount || !sameArray(record.roles, actual.roles) || !sameArray(record.toolCallIds, actual.toolCallIds)) throw fail('Checkpoint manifest does not describe its artifact', 'STORAGE');
}

export async function restoreCheckpoint({ store, owner, checkpoint, expectedBinding, maxContextBytes = LIMITS.contextBytes, maxManifestBytes = LIMITS.checkpointBytes }) {
  identifier(owner, 'checkpoint owner');
  const manifest = assertCheckpointManifest(checkpoint, maxManifestBytes);
  if (expectedBinding !== undefined) { validateBinding(expectedBinding, 'expected checkpoint binding'); if (!bindingMatches(manifest.binding, expectedBinding)) throw fail('Checkpoint provider/model/configuration binding mismatch; explicit fresh recovery is required', 'STORAGE'); }
  if (!store || typeof store.readArtifact !== 'function' || typeof store.artifactIdentity !== 'function') throw fail('Checkpoint artifact store unavailable', 'STORAGE');
  const recordById = new Map(manifest.records.map(record => [record.ref.artifactId, record]));
  if (manifest.historyRef) { const identity = await store.artifactIdentity(manifest.historyRef.artifactId); if (identity.owner !== owner || identity.bytes !== manifest.historyRef.bytes || identity.sha256 !== manifest.historyRef.sha256 || !identity.complete) throw fail('Checkpoint history identity mismatch', 'STORAGE'); }
  if (manifest.continuityRef) { const ref = manifest.continuityRef, identity = await store.artifactIdentity(ref.artifactId); if (identity.owner !== owner || identity.bytes !== ref.bytes || identity.sha256 !== ref.sha256 || !identity.complete) throw fail('Checkpoint continuity identity mismatch', 'STORAGE'); }
  if (manifest.compactionEnvelope) { const ref = manifest.compactionEnvelope.ref, identity = await store.artifactIdentity(ref.artifactId); if (identity.owner !== owner || identity.bytes !== ref.bytes || identity.sha256 !== ref.sha256 || !identity.complete) throw fail('Checkpoint compaction envelope identity mismatch', 'STORAGE'); }
  if (manifest.resultSections) for (const section of manifest.resultSections.sections) { const ref = section.ref, identity = await store.artifactIdentity(ref.artifactId); if (identity.owner !== owner || identity.bytes !== ref.bytes || identity.sha256 !== ref.sha256 || !identity.complete) throw fail('Checkpoint result section identity mismatch', 'STORAGE'); }
  for (const record of manifest.records) {
    const identity = await store.artifactIdentity(record.ref.artifactId);
    if (identity.owner !== owner || identity.bytes !== record.ref.bytes || identity.sha256 !== record.ref.sha256 || identity.complete !== true) throw fail('Checkpoint artifact identity mismatch', 'STORAGE');
  }
  const messages = []; let contentBytes = 0;
  for (const id of manifest.residentRecordIds) {
    const record = recordById.get(id);
    if (!Number.isSafeInteger(contentBytes + record.contentBytes) || contentBytes + record.contentBytes > maxContextBytes + maxManifestBytes) throw fail('Checkpoint context exceeds the resident byte budget', 'LIMIT');
    contentBytes += record.contentBytes;
    const body = await readArtifactBody(store, record, owner, maxContextBytes + maxManifestBytes - (contentBytes - record.contentBytes));
    let group;
    try { group = JSON.parse(body.toString('utf8')); } catch { throw fail('Checkpoint artifact is not valid JSON', 'STORAGE'); }
    compareDescription(record, group);
    if (!jsonBytes(group, 'checkpoint artifact').equals(body)) throw fail('Checkpoint artifact is not canonical JSON', 'STORAGE');
    messages.push(...group);
  }
  groupContextMessages(messages);
  const residentContextBytes = measureResidentContext(messages);
  if (residentContextBytes !== manifest.residentContextBytes) throw fail('Checkpoint resident byte accounting mismatch', 'STORAGE');
  if (residentContextBytes > maxContextBytes) throw fail('Checkpoint context exceeds the resident byte budget', 'LIMIT');
  return { messages, transcriptBytes: manifest.transcriptBytes, residentContextBytes, transcriptArtifactId: manifest.transcriptArtifactId, compactionEnvelope: manifest.compactionEnvelope, resultSections: manifest.resultSections };
}
