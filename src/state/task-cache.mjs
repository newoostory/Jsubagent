import { openSync, fstatSync, readSync, closeSync, constants } from 'node:fs';
import { join } from 'node:path';
import { hash, fail, identifier, LIMITS } from '../limits.mjs';
import { realpath, lstat } from 'node:fs/promises';

// Map-shaped synchronous access for the scheduler; only the working set is hot.
// Durable rows contain immutable record pointers, never conversation bodies.
function quarantinedTask(store, id, row, reason = row.quarantine?.reason ?? 'record identity unavailable') {
  const quarantine = row.quarantine ?? { recordId: row.recordId, bytes: row.bytes, sha256: row.sha256, reason };
  return { id, name: row.name ?? id, state: 'blocked', quarantined: true, quarantine, error: `Task record quarantined: ${reason}`, role: { name: row.roleName ?? '', tools: [], workspace: 'readonly' }, profile: row.profile ?? {}, task: '', cwd: store.root, dependsOn: [], checkpoint: {}, mailbox: [], delivering: [], seen: [], web: { mode: 'public' }, webUsed: { search: 0, fetch: 0 }, webReceipts: [] };
}

export class TaskCache {
  constructor(store, capacity = 128) { this.store = store; this.capacity = capacity; this.hot = new Map(); }
  row(id) { return this.store.metadata.get('work_items', this.store.scope, identifier(id)); }
  get size() { return Number(this.store.metadata.database.prepare("SELECT COUNT(*) AS count FROM work_items WHERE owner_id = ? AND run_id = ? AND state != 'archived'").get(this.store.scope.ownerId, this.store.scope.runId).count); }
  has(id) { const row = this.row(id); return Boolean(row && row.state !== 'archived'); }
  get(id, { includeArchived = false } = {}) {
    const cached = this.hot.get(id); if (cached) { this.hot.delete(id); this.hot.set(id, cached); return cached; }
    const row = this.row(id); if (!row || row.state === 'archived' && !includeArchived) return undefined;
    if (row.state === 'quarantined') { const value = quarantinedTask(this.store, id, row); this.set(id, value); return value; }
    let value, fd;
    try {
      fd = openSync(join(this.store.root, 'records', `${identifier(row.recordId)}.json`), constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
      const info = fstatSync(fd);
      if (!info.isFile() || info.size !== row.bytes || info.size > this.store.limits.stateBytes) throw fail('Task record size mismatch', 'STORAGE');
      const body = Buffer.alloc(info.size); let offset = 0;
      while (offset < body.length) { const n = readSync(fd, body, offset, body.length - offset, null); if (!n) break; offset += n; }
      if (offset !== body.length || hash(body) !== row.sha256) throw fail('Task record hash mismatch', 'STORAGE');
      value = JSON.parse(body.toString('utf8')); if (value.id !== id) throw fail('Task record owner mismatch', 'STORAGE');
    } catch (error) {
      if (typeof this.store.metadata.put !== 'function') throw fail(`Task record identity failure: ${error.message}`, 'STORAGE');
      const quarantine = { recordId: row.recordId, bytes: row.bytes, sha256: row.sha256, previousState: row.state, quarantinedAt: Date.now(), reason: String(error.message ?? error).slice(0, 1024) };
      this.store.metadata.put('work_items', this.store.scope, { ...row, state: 'quarantined', quarantine });
      value = quarantinedTask(this.store, id, { ...row, state: 'quarantined', quarantine }, quarantine.reason);
    } finally { if (fd !== undefined) closeSync(fd); }
    if (row.state !== 'archived') this.set(id, value);
    return row.state === 'archived' ? { ...value, qualityArchived: true } : value;
  }
  set(id, task) {
    this.hot.delete(id); this.hot.set(id, task);
    for (const [key, value] of this.hot) {
      if (this.hot.size <= this.capacity) break;
      if (key !== id && value.state !== 'running') this.hot.delete(key);
    }
    return this;
  }
  delete(id) { return this.hot.delete(id); }
  clear() { this.hot.clear(); }
  *rows({ includeArchived = false } = {}) {
    let after = 0;
    do {
      const page = this.store.metadata.list('work_items', this.store.scope, { after, limit: 100 });
      for (const row of page.items) if (includeArchived || row.state !== 'archived') yield row;
      if (page.nextCursor === null) return; after = page.nextCursor;
    } while (true);
  }
  *keys() { for (const row of this.rows()) yield row.id; }
  *values(options) { for (const row of this.rows(options)) yield this.get(row.id, options); }
  *entries() { for (const id of this.keys()) yield [id, this.get(id)]; }
  [Symbol.iterator]() { return this.entries(); }
}

export async function readTaskRecord(root, id) {
  const base = await realpath(root), path = join(base, 'metadata.sqlite');
  for (const candidate of [path, `${path}-wal`, `${path}-shm`, `${path}-journal`]) {
    const info = await lstat(candidate).catch(error => error.code === 'ENOENT' ? null : Promise.reject(error));
    if (info && (!info.isFile() || info.isSymbolicLink())) throw fail('Unsafe metadata path', 'STORAGE');
  }
  const records = join(base, 'records'), recordsInfo = await lstat(records);
  if (!recordsInfo.isDirectory() || recordsInfo.isSymbolicLink() || await realpath(records) !== records) throw fail('Unsafe records directory', 'STORAGE');
  const { DatabaseSync } = await import('node:sqlite');
  const db = new DatabaseSync(path, { readOnly: true });
  try {
    if (db.prepare('PRAGMA quick_check').get()?.quick_check !== 'ok') throw fail('Metadata integrity check failed', 'STORAGE');
    const found = db.prepare('SELECT data_json FROM work_items WHERE id = ? AND run_id = ? AND owner_id = ?').get(identifier(id), hash(base), 'host');
    if (!found) throw fail(`Unknown agentId: ${id}`, 'NOT_FOUND');
    let row;
    try { row = JSON.parse(found.data_json); } catch { throw fail('Corrupt task metadata row', 'STORAGE'); }
    const archived = row.state === 'archived';
    const cache = new TaskCache({ root: base, limits: LIMITS, metadata: { get: () => archived ? { ...row, state: 'idle' } : row }, scope: {} });
    return { task: cache.get(id), archived };
  } finally { db.close(); }
}

export async function listTaskRecords(root, { after = 0, limit = 128 } = {}) {
  if (!Number.isSafeInteger(after) || after < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 128) throw fail('Invalid task page');
  const base = await realpath(root), path = join(base, 'metadata.sqlite');
  for (const candidate of [path, `${path}-wal`, `${path}-shm`, `${path}-journal`]) {
    const info = await lstat(candidate).catch(error => error.code === 'ENOENT' ? null : Promise.reject(error));
    if (info && (!info.isFile() || info.isSymbolicLink())) throw fail('Unsafe metadata path', 'STORAGE');
  }
  const { DatabaseSync } = await import('node:sqlite');
  const db = new DatabaseSync(path, { readOnly: true });
  let rows;
  try {
    if (db.prepare('PRAGMA quick_check').get()?.quick_check !== 'ok') throw fail('Metadata integrity check failed', 'STORAGE');
    rows = db.prepare("SELECT seq, id FROM work_items WHERE run_id = ? AND owner_id = ? AND seq > ? AND state != 'archived' ORDER BY seq LIMIT ?").all(hash(base), 'host', after, limit + 1);
  } finally { db.close(); }
  const more = rows.length > limit, page = rows.slice(0, limit), items = [];
  for (const row of page) items.push((await readTaskRecord(base, row.id)).task);
  return { items, nextCursor: more ? Number(page.at(-1).seq) : null };
}
