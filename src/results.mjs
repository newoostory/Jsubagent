import { join } from 'node:path';
import { lstat, realpath } from 'node:fs/promises';
import { encodeJson, fail, hash, identifier } from './limits.mjs';
import { readJson } from './storage.mjs';
import { readResultPage } from './responses.mjs';
import { readTaskRecord } from './state/task-cache.mjs';

import { verifySegmentReference, readSegmentPage } from './result-segments.mjs';
import { executionReadiness } from './result-evidence.mjs';
import { productVerification } from './result-attestations.mjs';
import { productSnapshot } from './result-snapshots.mjs';
export const RESULT_KINDS = ['main_report', 'deliverable', 'changed_files', 'patch_index', 'test_inventory', 'verification_plan', 'review_findings', 'review_summary', 'verification_summary', 'evidence', 'log'];
export const RESULT_VIEWS = ['main', 'reviewer', 'verifier', 'artifacts'];
const recipes = { main: ['main_report', 'deliverable'], reviewer: ['main_report', 'changed_files', 'patch_index', 'evidence', 'review_findings', 'review_summary'], verifier: ['main_report', 'test_inventory', 'verification_plan', 'changed_files', 'verification_summary', 'evidence'] };
const object = value => value && typeof value === 'object' && !Array.isArray(value);
const integer = (value, min = 0) => Number.isSafeInteger(value) && value >= min;
function keys(value, allowed) { if (!object(value) || Object.keys(value).some(key => !allowed.includes(key))) throw fail('Invalid result fields'); }
function logicalPath(value) {
  if (typeof value !== 'string' || Buffer.byteLength(value) > 512 || !value || value.startsWith('/') || /[\\\x00-\x1f\x7f:]/.test(value) || value.split('/').some(part => !part || part === '.' || part === '..')) throw fail('Unsafe result logicalPath');
  return value;
}
export function advanceResultEpoch(task, goalChanged = false) {
  const epoch = (task.resultEpoch ?? 1) + 1, goal = (task.goalRevision ?? 1) + Number(goalChanged);
  if (!integer(epoch, 1) || !integer(goal, 1)) throw fail('Result epoch exhausted', 'LIMIT');
  task.resultEpoch = epoch; task.goalRevision = goal; delete task.bundleRef; delete task.executionReadiness;
}
export const resultBundleId = (agentId, epoch) => `b-${hash(`${identifier(agentId)}\0${epoch}`)}`;
const revisionId = (bundleId, revision) => hash(`${bundleId}\0${revision}`);
const publicationKey = (bundleId, publicationId) => hash(`${bundleId}\0${publicationId}`);
function queryEntry(database, scope, bundleId, fileId, revision) {
  const row = database.prepare(`SELECT data_json FROM result_entries WHERE run_id=? AND owner_id=? AND json_extract(data_json,'$.bundleId')=? AND json_extract(data_json,'$.fileId')=? AND json_extract(data_json,'$.revision')<=? ORDER BY json_extract(data_json,'$.revision') DESC LIMIT 1`).get(scope.runId, scope.ownerId, bundleId, fileId, revision);
  return row ? JSON.parse(row.data_json) : null;
}
function row(database, table, scope, id) {
  const found = database.prepare(`SELECT data_json FROM ${table} WHERE run_id=? AND owner_id=? AND id=?`).get(scope.runId, scope.ownerId, id);
  return found ? JSON.parse(found.data_json) : null;
}
export function resultHead(store, task) { return store.metadata.get('result_heads', store.scope, resultBundleId(task.id, task.resultEpoch ?? 1)); }
export function resultPrimaryRef(store, manifest) {
  return manifest ? queryEntry(store.metadata.database, store.scope, manifest.bundleId, manifest.primaryFileId, manifest.revision)?.contentRef : undefined;
}


// Own sealed artifacts only. The caller/runtime supplies the actor, attempt and publication ID.
// This service never imports a path, changes ownership, executes a command or confers acceptance.
export async function publishResultRevision(store, task, request, { publicationId, attemptId, assertCurrent, subject, generatedPaths = [], sourceDigest } = {}) {
  keys(request, ['expectedHead', 'stage', 'files', 'primaryFileId']);
  identifier(publicationId, 'publicationId'); identifier(attemptId, 'producerAttemptId');
  if (!['progress', 'candidate', 'final'].includes(request.stage) || request.stage === 'candidate' && (!subject || task.resultContract !== 'code-change')) throw fail('Candidate publication requires an immutable snapshot backend', 'UNSUPPORTED');
  if (subject) productSnapshot(store, subject, task.id);
  if (task.resultContract === 'code-change' && request.stage !== 'progress' && !subject) throw fail('Code-change publication requires a runtime product snapshot', 'BLOCKED');
  if (!integer(request.expectedHead) || !Array.isArray(request.files) || !request.files.length || request.files.length > 64) throw fail('Publication requires expectedHead and 1–64 file changes');
  const epoch = task.resultEpoch ?? 1, goalRevision = task.goalRevision ?? 1;
  if (!integer(epoch, 1) || !integer(goalRevision, 1)) throw fail('Invalid result epoch', 'STORAGE');
  const bundleId = resultBundleId(task.id, epoch), changes = [], paths = new Set();
  for (const input of request.files) {
    keys(input, ['logicalPath', 'kind', 'contentRef', 'fileComplete', 'mediaType', 'deleted']);
    const path = logicalPath(input.logicalPath), fileId = `f-${hash(path)}`;
    if (paths.has(path)) throw fail('Duplicate logicalPath'); paths.add(path);
    if (input.deleted === true) { changes.push({ fileId, logicalPath: path, deleted: true }); continue; }
    if (input.deleted !== undefined && input.deleted !== false) throw fail('Invalid result deletion');
    if (!RESULT_KINDS.includes(input.kind) || typeof input.fileComplete !== 'boolean') throw fail('Invalid result kind/completion');
    const ref = input.contentRef;
    assertCurrent?.();
    if (ref?.type === 'segments-v1') await verifySegmentReference(store, task.id, ref);
    else {
      keys(ref, ['artifactId', 'bytes', 'sha256', 'complete']); identifier(ref.artifactId);
      if (!integer(ref.bytes) || ref.complete !== true || !/^[a-f0-9]{64}$/.test(ref.sha256 ?? '')) throw fail('Result content must be a sealed immutable reference');
      const actual = await store.artifactIdentity(ref.artifactId);
      if (actual.owner !== task.id || actual.bytes !== ref.bytes || actual.sha256 !== ref.sha256 || !actual.complete) throw fail('Result artifact identity/owner mismatch', 'PERMISSION');
    }
    const mediaType = input.mediaType ?? 'text/plain; charset=utf-8';
    if (typeof mediaType !== 'string' || mediaType.length > 100 || /[\x00-\x1f\x7f]/.test(mediaType)) throw fail('Invalid result mediaType');
    changes.push({ fileId, logicalPath: path, kind: input.kind, contentRef: { ...ref }, bytes: ref.bytes, contentSha256: ref.sha256 ?? null, contentIdentity: ref.type === 'segments-v1' ? { type: 'segment-index-sha256', sha256: ref.indexRef.sha256 } : { type: 'sha256', sha256: ref.sha256 }, fileComplete: input.fileComplete, mediaType, provenance: { producerId: task.id, method: ref.type === 'segments-v1' ? 'owned-sealed-sections' : 'owned-sealed-artifact' } });
    if (generatedPaths.includes(path)) changes.at(-1).provenance.method = 'runtime-product-snapshot-collector';
  }
  const digest = hash(encodeJson({ request, epoch, goalRevision, attemptId, ...(subject ? { subject } : {}) }, 64 * 1024));
  return store.metadata.transaction(() => {
    assertCurrent?.();
    if ((task.resultEpoch ?? 1) !== epoch || (task.goalRevision ?? 1) !== goalRevision) throw fail('Publication epoch changed', 'STALE');
    const eventId = publicationKey(bundleId, publicationId);
    const duplicate = store.metadata.get('result_events', store.scope, eventId);
    if (duplicate) { if (duplicate.digest !== digest) throw fail('Publication ID reused with different content', 'CONFLICT'); return duplicate.bundleRef; }
    const head = resultHead(store, task);
    if ((head?.revision ?? 0) !== request.expectedHead) throw fail('Result head changed', 'CONFLICT');
    if (head?.stage === 'final') throw fail('Final result epoch is immutable; start a new authorized epoch', 'CONFLICT');
    const revision = request.expectedHead + 1;
    if (!integer(revision, 1)) throw fail('Result revision exhausted', 'LIMIT');
    let fileCount = head?.fileCount ?? 0, totalLogicalBytes = head?.totalLogicalBytes ?? 0, incompleteFiles = head?.incompleteFiles ?? 0;
    for (const change of changes) {
      const previous = queryEntry(store.metadata.database, store.scope, bundleId, change.fileId, request.expectedHead);
      if (previous && !previous.deleted) { fileCount--; totalLogicalBytes -= previous.bytes; if (!previous.fileComplete) incompleteFiles--; }
      if (!change.deleted) { fileCount++; totalLogicalBytes += change.bytes; if (!change.fileComplete) incompleteFiles++; }
      store.metadata.put('result_entries', store.scope, { id: hash(`${bundleId}\0${change.fileId}\0${revision}`), state: 'published', ...change, bundleId, agentId: task.id, resultEpoch: epoch, revision, fileRevision: revision }, { ifAbsent: true });
    }
    if (!integer(fileCount) || !integer(totalLogicalBytes) || !integer(incompleteFiles)) throw fail('Invalid result aggregate', 'LIMIT');
    const primaryFileId = request.primaryFileId ?? head?.primaryFileId ?? `f-${hash('main.md')}`;
    identifier(primaryFileId, 'primaryFileId');
    const primary = queryEntry(store.metadata.database, store.scope, bundleId, primaryFileId, revision);
    if (!primary || primary.deleted) throw fail('Publication must include a primary file');
    if (request.stage === 'final' && incompleteFiles) throw fail('Final publication has incomplete files', 'BLOCKED');
    if (subject) {
      for (const path of ['changes.jsonl', 'patches/index.jsonl', 'tests.jsonl', 'verification-plan.json', 'product/snapshot.bin', 'product/base-snapshot.bin']) {
        const entry = queryEntry(store.metadata.database, store.scope, bundleId, `f-${hash(path)}`, revision);
        if (!entry || entry.deleted || !entry.fileComplete || entry.provenance?.method !== 'runtime-product-snapshot-collector') throw fail('Incomplete code-change result contract', 'BLOCKED');
      }
    }
    const bundle = { schemaVersion: 2, agentId: task.id, bundleId, revision, parentRevision: head?.revision ?? null, resultEpoch: epoch, goalRevision, producerAttemptId: attemptId, stage: request.stage, outputComplete: request.stage === 'final', primaryFileId, entryIndexRef: { bundleId, revision }, fileCount, totalLogicalBytes, incompleteFiles, publishedAt: Date.now(), publicationId, readiness: { ...executionReadiness(store.metadata.database, store.scope, task.id, epoch), ...(task.executionReadiness?.evidenceIndexRef ? { evidenceIndexRef: task.executionReadiness.evidenceIndexRef } : {}) } };
    if (subject) { bundle.candidateSnapshot = subject; bundle.readiness.candidate = 'ready'; bundle.readiness.productSha256 = subject.productSha256; }
    const bundleRef = { agentId: task.id, bundleId, revision, resultEpoch: epoch };
    store.metadata.put('result_revisions', store.scope, { id: revisionId(bundleId, revision), state: 'published', ...bundle }, { ifAbsent: true });
    store.metadata.put('result_heads', store.scope, { id: bundleId, state: 'published', ...bundle });
    store.metadata.put('result_events', store.scope, { id: eventId, state: 'committed', publicationId, digest, ...(sourceDigest ? { sourceDigest } : {}), bundleRef, stage: request.stage }, { ifAbsent: true });
    return bundleRef;
  });
}

async function readDatabase(root, operation) {
  const base = await realpath(root), path = join(base, 'metadata.sqlite');
  for (const candidate of [path, `${path}-wal`, `${path}-shm`, `${path}-journal`]) {
    const info = await lstat(candidate).catch(error => error.code === 'ENOENT' ? null : Promise.reject(error));
    if (info && (!info.isFile() || info.isSymbolicLink())) throw fail('Unsafe result metadata path', 'STORAGE');
  }
  const { DatabaseSync } = await import('node:sqlite');
  const database = new DatabaseSync(path, { readOnly: true });
  try {
    if (!database.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name='result_heads'").get()) return { availability: 'not_published', reason: 'Legacy result metadata; use source=result for preserved reports.' };
    database.exec('BEGIN');
    try { const value = operation(database, { runId: hash(base), ownerId: 'host' }); database.exec('COMMIT'); return value; }
    catch (error) { database.exec('ROLLBACK'); throw error; }
  } finally { database.close(); }
}
function cursorValue(value) {
  if (typeof value !== 'string' || value.length > 4096) throw fail('Invalid result cursor');
  let result; try { result = JSON.parse(Buffer.from(value, 'base64url').toString('utf8')); } catch { throw fail('Invalid result cursor'); }
  keys(result, ['type', 'scope', 'agentId', 'resultEpoch', 'bundleId', 'revision', 'view', 'after']);
  if (result.type !== 'result-list-v2' || !integer(result.resultEpoch, 1) || !integer(result.revision, 1) || !RESULT_VIEWS.includes(result.view) || typeof result.after !== 'string' || !/^(f-[a-f0-9]{64})?$/.test(result.after)) throw fail('Invalid result cursor binding');
  identifier(result.agentId); identifier(result.bundleId); return result;
}

export function resultSelector(value) {
  keys(value, ['view', 'files', 'kinds']);
  if (Object.keys(value).length !== 1) throw fail('Exactly one result selector is required');
  if (value.view !== undefined) { if (!RESULT_VIEWS.includes(value.view)) throw fail('Unknown result view'); return { kinds: recipes[value.view] ?? RESULT_KINDS, files: null }; }
  if (value.files !== undefined) {
    if (!Array.isArray(value.files) || !value.files.length || value.files.length > 64 || new Set(value.files).size !== value.files.length) throw fail('Invalid result file selector');
    return { kinds: RESULT_KINDS, files: value.files.map(logicalPath).sort() };
  }
  if (!Array.isArray(value.kinds) || !value.kinds.length || value.kinds.length > RESULT_KINDS.length || value.kinds.some(kind => !RESULT_KINDS.includes(kind)) || new Set(value.kinds).size !== value.kinds.length) throw fail('Unknown or duplicate result kind');
  return { kinds: [...value.kinds].sort(), files: null };
}
// Main/offline adapter. Child grants are runtime-assigned and rechecked per page.
export async function readResultService(root, request, { pageSize = 16, grant } = {}) {
  if (!root) throw fail('No agent run in this branch', 'NOT_FOUND');
  const agentId = identifier(request.agentId), source = request.source;
  if (!['bundle', 'result_file'].includes(source) || !integer(pageSize, 1) || pageSize > 64) throw fail('Invalid result read');
  const { task, archived } = await readTaskRecord(root, agentId);
  const base = await realpath(root), scopeHash = hash(base);
  const cursor = request.cursor === undefined ? null : cursorValue(request.cursor);
  if (cursor && (source !== 'bundle' || request.bundleId !== undefined || request.revision !== undefined || request.view !== undefined || cursor.scope !== scopeHash || cursor.agentId !== agentId)) throw fail('Cursor scope/selector mismatch', 'PERMISSION');
  const pinned = await readDatabase(base, (database, scope) => {
    const bundleId = cursor?.bundleId ?? request.bundleId ?? resultBundleId(agentId, task.resultEpoch ?? 1);
    const revision = cursor?.revision ?? request.revision;
    const manifest = revision === undefined ? row(database, 'result_heads', scope, identifier(bundleId)) : row(database, 'result_revisions', scope, revisionId(identifier(bundleId), revision));
    if (!manifest) { if (revision !== undefined || request.bundleId) throw fail('No published result revision', 'NOT_FOUND'); return { availability: 'not_published', resultEpoch: task.resultEpoch ?? 1 }; }
    if (manifest.agentId !== agentId || manifest.bundleId !== bundleId || (cursor && cursor.resultEpoch !== manifest.resultEpoch)) throw fail('Result producer/epoch mismatch', 'PERMISSION');
    if (grant && (grant.agentId !== manifest.agentId || grant.bundleId !== manifest.bundleId || grant.revision !== manifest.revision || grant.resultEpoch !== manifest.resultEpoch)) throw fail('Result read exceeds the pinned input grant', 'PERMISSION');
    const allowed = grant ? resultSelector(grant.selector) : { kinds: RESULT_KINDS, files: null };
    const currentExecution = task.executionReadiness || task.requiredResultChecks?.length ? { resultEpoch: task.resultEpoch ?? 1, ...executionReadiness(database, scope, agentId, task.resultEpoch ?? 1), ...(task.executionReadiness?.evidenceIndexRef ? { evidenceIndexRef: task.executionReadiness.evidenceIndexRef } : {}) } : undefined;
    const currentProductHead = task.resultContract === 'code-change' ? row(database, 'result_heads', scope, resultBundleId(task.id, task.resultEpoch ?? 1)) : null;
    const currentProductVerification = currentProductHead ? productVerification(database, scope, task.id, task.resultEpoch ?? 1, currentProductHead.candidateSnapshot?.productSha256) : undefined;
    if (source === 'result_file') {
      if (!request.bundleId || !integer(request.revision, 1)) throw fail('result_file requires a pinned bundle revision');
      const entry = queryEntry(database, scope, bundleId, identifier(request.fileId), manifest.revision);
      if (!entry || entry.deleted) throw fail('File not present in this revision', 'NOT_FOUND');
      if (!allowed.kinds.includes(entry.kind) || allowed.files && !allowed.files.includes(entry.logicalPath)) throw fail('Result file is outside the input grant', 'PERMISSION');
      return { manifest, entry, currentExecution, currentProductVerification };
    }
    const view = cursor?.view ?? request.view ?? 'main';
    if (!RESULT_VIEWS.includes(view)) throw fail('Invalid result view');
    const kinds = (recipes[view] ?? RESULT_KINDS).filter(kind => allowed.kinds.includes(kind));
    const found = database.prepare(`SELECT e.data_json FROM result_entries e WHERE e.run_id=? AND e.owner_id=? AND json_extract(e.data_json,'$.bundleId')=? AND json_extract(e.data_json,'$.fileId')>? AND json_extract(e.data_json,'$.revision')<=? AND json_extract(e.data_json,'$.revision')=(SELECT MAX(json_extract(n.data_json,'$.revision')) FROM result_entries n WHERE n.run_id=e.run_id AND n.owner_id=e.owner_id AND json_extract(n.data_json,'$.bundleId')=json_extract(e.data_json,'$.bundleId') AND json_extract(n.data_json,'$.fileId')=json_extract(e.data_json,'$.fileId') AND json_extract(n.data_json,'$.revision')<=?) AND COALESCE(json_extract(e.data_json,'$.deleted'),0)=0 AND json_extract(e.data_json,'$.kind') IN (SELECT value FROM json_each(?)) AND (? IS NULL OR json_extract(e.data_json,'$.logicalPath') IN (SELECT value FROM json_each(?))) ORDER BY json_extract(e.data_json,'$.fileId') LIMIT ?`).all(scope.runId, scope.ownerId, bundleId, cursor?.after ?? '', manifest.revision, manifest.revision, JSON.stringify(kinds), allowed.files ? JSON.stringify(allowed.files) : null, allowed.files ? JSON.stringify(allowed.files) : null, pageSize + 1);
    const more = found.length > pageSize, entries = found.slice(0, pageSize).map(item => JSON.parse(item.data_json));
    const primaryEntry = queryEntry(database, scope, bundleId, manifest.primaryFileId, manifest.revision);
    const primary = primaryEntry && !primaryEntry.deleted && allowed.kinds.includes(primaryEntry.kind) && (!allowed.files || allowed.files.includes(primaryEntry.logicalPath))
      ? { fileId: primaryEntry.fileId, fileRevision: primaryEntry.fileRevision, contentSha256: primaryEntry.contentSha256, contentIdentity: primaryEntry.contentIdentity, bytes: primaryEntry.bytes, fileComplete: primaryEntry.fileComplete, read: { action: 'read', requests: [{ agentId, source: 'result_file', bundleId, revision: manifest.revision, fileId: primaryEntry.fileId, byteOffset: 0, limitBytes: 4096 }] } } : undefined;
    const files = entries.map(({ id, state, createdAt, updatedAt, ownerId, runId, ...entry }) => ({ ...entry, read: { action: 'read', requests: [{ agentId, source: 'result_file', bundleId, revision: manifest.revision, fileId: entry.fileId, byteOffset: 0, limitBytes: 4096 }] } }));
    const next = more ? Buffer.from(JSON.stringify({ type: 'result-list-v2', scope: scopeHash, agentId, resultEpoch: manifest.resultEpoch, bundleId, revision: manifest.revision, view, after: entries.at(-1).fileId })).toString('base64url') : null;
    const { id, state, createdAt, updatedAt, ownerId, runId, ...bundle } = manifest;
    return { availability: 'published', ...bundle, resultScope: scopeHash, view, primary, files, ...(currentExecution ? { currentExecution } : {}), ...(currentProductVerification ? { currentProductVerification } : {}), ...(next ? { read: { action: 'read', requests: [{ agentId, source: 'bundle', cursor: next }] } } : {}), listComplete: !more };
  });
  const common = { agentId, source, archived, currentState: task.state, ...(pinned.currentExecution ? { currentExecution: pinned.currentExecution } : {}), ...(pinned.currentProductVerification ? { currentProductVerification: pinned.currentProductVerification } : {}) };
  if (!pinned.entry) return { ...common, ...pinned };
  const { manifest, entry } = pinned, reference = entry.contentRef;
  if (!/^text\//.test(entry.mediaType) && !['application/json', 'application/x-ndjson'].includes(entry.mediaType)) throw fail('Binary logical file requires raw artifact/export reader', 'UNSUPPORTED');
  const dir = join(base, 'artifacts'), dirInfo = await lstat(dir);
  if (!dirInfo.isDirectory() || dirInfo.isSymbolicLink() || await realpath(dir) !== dir) throw fail('Unsafe result artifact directory', 'STORAGE');
  const offset = request.byteOffset ?? 0, limit = request.limitBytes ?? 4096;
  if (!integer(offset) || !integer(limit, 1) || limit > 32768) throw fail('Invalid result range');
  let page;
  if (reference.type === 'segments-v1') page = await readSegmentPage(dir, agentId, reference, offset, limit);
  else {
    const info = await readJson(join(dir, `${identifier(reference.artifactId)}.json`), 8192);
    if (info.id !== reference.artifactId || info.owner !== agentId || !info.complete || info.bytes !== reference.bytes || info.sha256 !== reference.sha256) throw fail('Result content identity mismatch', 'PERMISSION');
    page = await readResultPage(join(dir, `${info.id}.bin`), offset, limit, reference.bytes, reference.sha256);
    if (page.encoding !== 'utf8') throw fail('Range must begin at a UTF-8 boundary and fit at least one code point', 'INVALID_RANGE');
  }
  return { ...common, resultScope: scopeHash, bundleId: manifest.bundleId, revision: manifest.revision, resultEpoch: manifest.resultEpoch, fileId: entry.fileId, fileRevision: entry.fileRevision, contentSha256: entry.contentSha256, contentIdentity: entry.contentIdentity, fileComplete: entry.fileComplete, outputComplete: manifest.outputComplete, readiness: manifest.readiness, ...page };
}
