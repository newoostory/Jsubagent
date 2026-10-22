import { encodeJson, fail, hash, identifier, LIMITS, safeError } from '../limits.mjs';
import { join } from 'node:path';
import { readResultPage } from '../responses.mjs';

const KEYS = new Set(['kind', 'sectionId', 'revision', 'expectedHead', 'requiredSections', 'content', 'complete']);

function exactObject(value, name) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.getPrototypeOf(value) !== Object.prototype) throw fail(`Invalid ${name}`, 'EVIDENCE');
  return value;
}

export function parseResultSection(raw) {
  if (typeof raw !== 'string') throw fail('Result section must be text', 'EVIDENCE');
  let value;
  try { value = JSON.parse(raw.trim()); } catch { return null; }
  if (value?.kind !== 'result_section') return null;
  exactObject(value, 'result section');
  for (const key of Object.keys(value)) if (!KEYS.has(key)) throw fail(`Unsupported result section field: ${key}`, 'EVIDENCE');
  identifier(value.sectionId, 'result sectionId');
  if (!Number.isSafeInteger(value.revision) || value.revision < 1) throw fail('Invalid result section revision', 'EVIDENCE');
  if (value.expectedHead !== null && (typeof value.expectedHead !== 'string' || !/^[a-f0-9]{64}$/.test(value.expectedHead))) throw fail('Invalid result section expectedHead', 'EVIDENCE');
  if (!Array.isArray(value.requiredSections) || !value.requiredSections.length || value.requiredSections.length > 256 || new Set(value.requiredSections).size !== value.requiredSections.length) throw fail('Invalid result section coverage declaration', 'EVIDENCE');
  value.requiredSections.forEach(id => identifier(id, 'required result section'));
  if (!value.requiredSections.includes(value.sectionId)) throw fail('Committed section is absent from required coverage', 'EVIDENCE');
  if (typeof value.content !== 'string') throw fail('Result section content must be text', 'EVIDENCE');
  if (typeof value.complete !== 'boolean') throw fail('Invalid result section completion marker', 'EVIDENCE');
  return value;
}

export class ResultSectionAssembler {
  constructor(manifest) {
    this.required = manifest?.requiredSections ? [...manifest.requiredSections] : undefined;
    this.sections = new Map((manifest?.sections ?? []).map(row => [row.sectionId, { sectionId: row.sectionId, revision: row.revision }]));
    this.head = manifest?.head ?? null; this.complete = manifest?.complete ?? false;
  }
  get active() { return this.required !== undefined; }
  commit(section) {
    if (this.complete) throw fail('Result section commit arrived after completed coverage', 'EVIDENCE');
    if (section.expectedHead !== this.head) throw fail('Result section expectedHead mismatch', 'CONFLICT');
    if (!this.required) this.required = [...section.requiredSections];
    else if (this.required.length !== section.requiredSections.length || this.required.some((id, index) => id !== section.requiredSections[index])) throw fail('Result section coverage declaration changed', 'EVIDENCE');
    const previous = this.sections.get(section.sectionId);
    if (section.revision !== (previous?.revision ?? 0) + 1) throw fail('Result section revision is not the next revision', 'CONFLICT');
    this.sections.set(section.sectionId, { sectionId: section.sectionId, revision: section.revision, content: section.content });
    const committed = this.required.filter(id => this.sections.has(id));
    if (section.complete && committed.length !== this.required.length) throw fail(`Result coverage incomplete; missing: ${this.required.filter(id => !this.sections.has(id)).join(', ')}`, 'EVIDENCE');
    const state = { previousHead: this.head, coverageSha256: hash(JSON.stringify(this.required)), sectionId: section.sectionId, revision: section.revision, contentSha256: hash(section.content) };
    this.head = hash(encodeJson(state));
    this.complete = section.complete;
    return { head: this.head, complete: this.complete, committed, remaining: this.required.filter(id => !this.sections.has(id)) };
  }
  contents() {
    if (!this.complete || !this.required) throw fail('Result section coverage is not complete', 'EVIDENCE');
    return this.required.map(id => this.sections.get(id).content);
  }
}

export function restoreResultSections(messages) {
  const assembler = new ResultSectionAssembler();
  for (const message of messages ?? []) {
    if (message?.role !== 'assistant' || message.stopReason !== 'stop' || !Array.isArray(message.content)) continue;
    const raw = message.content.filter(part => part?.type === 'text').map(part => part.text).join('\n');
    const section = parseResultSection(raw);
    if (section) assembler.commit(section);
  }
  return assembler;
}

export async function persistResultSection(store, owner, section) {
  const payload = Buffer.from(section.content, 'utf8');
  const artifact = await store.artifact(owner, 'agent-result-section');
  try {
    for (let offset = 0; offset < payload.length; offset += 65536) await artifact.write(payload.subarray(offset, offset + 65536));
    const sealed = await artifact.finish(true);
    if (!sealed.complete || sealed.bytes !== payload.length || sealed.sha256 !== hash(payload)) throw fail('Result section artifact seal mismatch', 'STORAGE');
    return { artifactId: sealed.id, bytes: sealed.bytes, sha256: sealed.sha256, complete: true, codec: 'utf8' };
  } catch (error) {
    if (!artifact.closed) await artifact.finish(false, safeError(error)).catch(() => {});
    throw error;
  }
}

export function resultSectionManifest(assembler, references) {
  if (!assembler.active || !assembler.head) return undefined;
  return {
    type: 'result-section-manifest', requiredSections: [...assembler.required],
    sections: assembler.required.filter(id => references.has(id)).map(id => { const section = assembler.sections.get(id); return { sectionId: id, revision: section.revision, ref: { ...references.get(id) } }; }),
    head: assembler.head, complete: assembler.complete,
  };
}

export async function readResultSections(store, manifest, owner) {
  const contents = [];
  for (const sectionId of manifest.requiredSections) {
    const row = manifest.sections.find(section => section.sectionId === sectionId);
    if (!row) throw fail(`Result section is missing: ${sectionId}`, 'STORAGE');
    const identity = await store.artifactIdentity(row.ref.artifactId);
    if (identity.owner !== owner || identity.bytes !== row.ref.bytes || identity.sha256 !== row.ref.sha256 || !identity.complete) throw fail('Result section artifact identity mismatch', 'STORAGE');
    const chunks = []; let offset = 0;
    while (offset < row.ref.bytes) {
      const limit = Math.min(store.limits?.readBytes ?? LIMITS.readBytes, row.ref.bytes - offset);
      const page = row.ref.codec === 'utf8' ? await readResultPage(join(store.root, 'artifacts', `${row.ref.artifactId}.bin`), offset, limit, row.ref.bytes, row.ref.sha256) : await store.readArtifact(row.ref.artifactId, offset, limit);
      if (page.nextByteOffset <= offset) throw fail('Result section read made no progress', 'STORAGE');
      chunks.push(page.text); offset = page.nextByteOffset;
    }
    const payload = chunks.join('');
    if (Buffer.byteLength(payload, row.ref.codec === 'utf8' ? 'utf8' : 'ascii') !== row.ref.bytes || hash(payload) !== row.ref.sha256) throw fail('Result section artifact content mismatch', 'STORAGE');
    const bytes = Buffer.from(payload, row.ref.codec === 'utf8' ? 'utf8' : 'base64');
    try { contents.push(new TextDecoder('utf-8', { fatal: true }).decode(bytes)); } catch { throw fail('Result section is not valid UTF-8', 'STORAGE'); }
  }
  return contents;
}

export function resultSectionPrompt(assembler) {
  const state = assembler.active ? { expectedHead: assembler.head, requiredSections: assembler.required, committed: assembler.required.filter(id => assembler.sections.has(id)), remaining: assembler.required.filter(id => !assembler.sections.has(id)) } : { expectedHead: null };
  return `The prior response reached an output-length boundary and is incomplete evidence only. Never append its raw fragment or repeat any tool call from it. Continue through committed result sections. Return exactly one complete JSON object and no wrapper: {"kind":"result_section","sectionId":"stable-id","revision":1,"expectedHead":${assembler.head ? '"<exact head below>"' : 'null'},"requiredSections":["all","required","ids"],"content":"one self-contained logical section, never a partial JSON or tool call","complete":false}. The first commit declares the complete ordered coverage list; later commits must keep it byte-for-byte, use the exact current head, and increment a replaced section revision. Mark complete:true only when every declared section is committed. Keep each envelope within the provider output limit. Current commit state: ${JSON.stringify(state)}`;
}
