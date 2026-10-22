import { join } from 'node:path';
import { lstat, realpath } from 'node:fs/promises';
import { readJson, readPage } from './storage.mjs';
import { readAgentResponse } from './responses.mjs';
import { encodeJson, fail, hash, identifier, safeError } from './limits.mjs';
import { validatePublicAgentParameters } from './public-api.mjs';
import { readTaskRecord } from './state/task-cache.mjs';
import { readResultService } from './results.mjs';

async function directory(root, name) {
  if (!root) throw fail('No agent run in this branch', 'NOT_FOUND');
  const base = await realpath(root), path = join(base, name), info = await lstat(path);
  if (!info.isDirectory() || info.isSymbolicLink() || await realpath(path) !== path) throw fail('Reader refuses symlinked record directories');
  return path;
}
async function record(root, agentId) {
  identifier(agentId, 'record agentId');
  return readTaskRecord(root, agentId);
}

async function messageRecord(root, messageId) {
  const base = await realpath(root), path = join(base, 'metadata.sqlite');
  for (const candidate of [path, `${path}-wal`, `${path}-shm`, `${path}-journal`]) {
    const info = await lstat(candidate).catch(error => error.code === 'ENOENT' ? null : Promise.reject(error));
    if (info && (!info.isFile() || info.isSymbolicLink())) throw fail('Unsafe message metadata path', 'STORAGE');
  }
  const { DatabaseSync } = await import('node:sqlite');
  const database = new DatabaseSync(path, { readOnly: true });
  try {
    if (database.prepare('PRAGMA quick_check').get()?.quick_check !== 'ok') throw fail('Metadata integrity check failed', 'STORAGE');
    const found = database.prepare('SELECT data_json FROM messages WHERE id = ? AND run_id = ? AND owner_id = ?').get(identifier(messageId, 'messageId'), hash(base), 'host');
    if (!found) return undefined;
    try { return JSON.parse(found.data_json); } catch { throw fail('Corrupt message metadata row', 'STORAGE'); }
  } finally {
    database.close();
  }
}
async function raw(root, p, limitBytes) {
  const dir = await directory(root, 'artifacts'), artifactId = identifier(p.artifactId, 'artifactId');
  const info = await readJson(join(dir, `${artifactId}.json`), 8192);
  if (info.id !== artifactId || (p.agentId && info.owner !== p.agentId)) throw fail('Artifact owner/identity mismatch', 'PERMISSION');
  if (!Number.isSafeInteger(info.bytes) || info.bytes < 0 || (info.complete && !/^[a-f0-9]{64}$/.test(info.sha256 ?? ''))) throw fail('Invalid artifact identity', 'STORAGE');
  const { task, archived } = await record(root, info.owner);
  const page = await readPage(join(dir, `${info.id}.bin`), p.byteOffset ?? 0, limitBytes, 32768);
  if (page.fileBytes !== info.bytes) throw fail('Artifact size changed', 'STORAGE');
  const artifact = {
    artifactId: info.id, ownerAgentId: info.owner, label: info.label, bytes: info.bytes, complete: info.complete, createdAt: info.createdAt,
    ...(info.sha256 ? { sha256: info.sha256 } : {}),
    ...(info.responseType ? { responseType: info.responseType } : {}),
    ...(info.responseMetadata ? { responseMetadata: info.responseMetadata } : {}),
    ...(info.historyPreviousArtifactId ? { historyPreviousArtifactId: info.historyPreviousArtifactId, previous: { action: 'read', requests: [{ agentId: task.id, source: 'artifact', artifactId: info.historyPreviousArtifactId }] } } : {}),
  };
  return { agentId: task.id, source: 'artifact', artifactId: info.id, archived, currentState: task.state, artifact, ...page };
}
async function receipt(root, p) {
  const { task, archived } = await record(root, p.agentId);
  const message = await messageRecord(root, p.messageId);
  const owned = message && (message.to === task.id || message.from === task.id);
  const artifactId = owned ? message.bodyArtifactId : undefined;
  const phases = { pending: 'queued', delivering: 'injection_uncertain', acknowledged: 'retained_receipt', expired: 'expired' };
  const phase = owned ? (phases[message.state] ?? 'retained_receipt') : 'unknown';
  return { agentId: task.id, source: 'receipt', messageId: p.messageId, archived, currentState: task.state,
    outcome: owned ? 'accepted' : 'unknown', phase,
    ...(artifactId ? { read: { action: 'read', requests: [{ agentId: task.id, source: 'message', artifactId }] } } : {}),
    note: 'Acceptance/injection is not successful processing. Durable receipt absence is not proof of non-delivery.' };
}
async function one(root, p, limitBytes, pageSize) {
  if (['bundle', 'result_file'].includes(p.source)) return readResultService(root, { ...p, ...(p.source === 'result_file' ? { limitBytes } : {}) }, { pageSize });
  if (p.source === 'artifact') return raw(root, p, limitBytes);
  if (p.source === 'receipt') return receipt(root, p);
  return readAgentResponse(root, { ...p, ...((p.source ?? 'index') === 'index' ? {} : { limitBytes }) });
}
export async function publicRead(root, requests, { signal } = {}) {
  validatePublicAgentParameters({ action: 'read', requests });
  signal?.throwIfAborted();
  const budget = Math.floor((32768 - 512) / requests.length), results = [];
  for (const [index, request] of requests.entries()) {
    signal?.throwIfAborted();
    let limitBytes = Math.min(request.limitBytes ?? 4096, Math.max(1, budget - 1024));
    try {
      // Pin the first observed artifact across budget retries and continuation.
      let p = { ...request }, page, pageSize = 16;
      while (true) {
        page = await one(root, p, limitBytes, pageSize);
        if (page.artifactId) p.artifactId = page.artifactId;
        if (page.source === 'bundle' && page.bundleId && !p.cursor) { p.bundleId = page.bundleId; p.revision = page.revision; }
        if (page.source === 'index') {
          while (page.records?.length) {
            try { encodeJson({ index, ok: true, ...page }, budget); break; }
            catch { page.records.shift(); page.omittedRecords++; }
          }
        }
        const read = page.eof === false ? { action: 'read', requests: [{ ...p, byteOffset: page.nextByteOffset, limitBytes: request.limitBytes ?? 4096 }] } : page.read;
        const result = { index, ok: true, ...page, ...(read ? { read } : {}) };
        try { encodeJson(result, budget); results.push(result); break; }
        catch (error) {
          if (page.source === 'bundle') { if (pageSize === 1) throw error; pageSize = Math.max(1, Math.floor(pageSize / 2)); continue; }
          if (limitBytes === 1 || ['index', 'receipt'].includes(page.source)) throw error; limitBytes = Math.max(1, Math.floor(limitBytes / 2));
        }
      }
    } catch (error) {
      signal?.throwIfAborted();
      const row = { index, ok: false, agentId: request.agentId, source: request.source ?? 'index', error: { code: error.code ?? 'READ', message: safeError(error) } };
      // Escaped error text shares the same per-item budget as successful pages.
      while (true) {
        try { encodeJson(row, budget); break; }
        catch (limit) { if (limit.code !== 'LIMIT' || row.error.message.length <= 1) throw limit; row.error.message = row.error.message.slice(0, Math.floor(row.error.message.length / 2)); }
      }
      results.push(row);
    }
  }
  const result = { results }; encodeJson(result, 32768); return result;
}
