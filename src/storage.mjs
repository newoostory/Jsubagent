import { constants } from 'node:fs';
import { open, mkdir, rename, unlink, readdir, stat, lstat, statfs, realpath, link, opendir } from 'node:fs/promises';
import { join, dirname } from 'node:path';
import { randomUUID, createHash } from 'node:crypto';
import { ArtifactStore } from './state/artifacts.mjs';
import { DurableJournal } from './state/journal.mjs';
import { LIMITS, MiB, encodeJson, jsonClone, identifier, fail, short, hash } from './limits.mjs';
import { MetadataStore } from './state/metadata.mjs';
import { TaskCache } from './state/task-cache.mjs';

export async function privateDir(path) { await mkdir(path, { recursive: true, mode: 0o700 }); return realpath(path); }
export async function readBounded(path, maxBytes) {
  const file = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
  try {
    const info = await file.stat();
    if (!info.isFile() || info.size > maxBytes) throw fail(`File must be regular and <= ${maxBytes} bytes: ${path}`, 'LIMIT');
    // A returned subarray keeps its whole backing allocation alive. Start at the
    // observed file size, not the limit (often 2MiB for a tiny config/tool read).
    // Keep one EOF probe byte and grow only if the file actually grows.
    let buf = Buffer.alloc(Math.min(maxBytes + 1, info.size + 1));
    let used = 0;
    while (true) {
      if (used === buf.length) {
        if (used > maxBytes) throw fail('File grew beyond read budget', 'LIMIT');
        const next = Buffer.alloc(Math.min(maxBytes + 1, Math.max(1, buf.length * 2)));
        buf.copy(next, 0, 0, used); buf = next;
      }
      const { bytesRead } = await file.read(buf, used, buf.length - used, null);
      if (!bytesRead) break;
      used += bytesRead;
    }
    if (used > maxBytes) throw fail('File grew beyond read budget', 'LIMIT');
    return buf.subarray(0, used);
  } finally { await file.close(); }
}
export async function readJson(path, maxBytes = LIMITS.stateBytes) {
  const bytes = await readBounded(path, maxBytes); const value = JSON.parse(bytes.toString('utf8')); encodeJson(value, maxBytes); return value;
}
export async function atomicJson(path, value, maxBytes = LIMITS.stateBytes) {
  const body = encodeJson(value, maxBytes);
  const temp = `${path}.${randomUUID()}.tmp`;
  let file;
  try {
    file = await open(temp, 'wx', 0o600); await file.writeFile(body); await file.sync(); await file.close(); file = null;
    await rename(temp, path);
    const dir = await open(dirname(path), 'r');
    try { await dir.sync(); } catch (e) { if (!['EINVAL', 'ENOTSUP'].includes(e.code)) throw e; } finally { await dir.close(); }
  } finally { await file?.close(); await unlink(temp).catch(e => { if (e.code !== 'ENOENT') throw e; }); }
}
async function fileIdentity(path) {
  const file = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
  try {
    const before = await file.stat(); if (!before.isFile()) throw fail('Record must be a regular file', 'STORAGE');
    const digest = createHash('sha256'), buffer = Buffer.allocUnsafe(64 * 1024); let offset = 0;
    while (offset < before.size) {
      const { bytesRead } = await file.read(buffer, 0, Math.min(buffer.length, before.size - offset), offset);
      if (!bytesRead) throw fail('Record changed during reconciliation', 'STORAGE');
      digest.update(buffer.subarray(0, bytesRead)); offset += bytesRead;
    }
    const after = await file.stat();
    if (after.size !== before.size || after.mtimeMs !== before.mtimeMs || after.ctimeMs !== before.ctimeMs) throw fail('Record changed during reconciliation', 'STORAGE');
    return { bytes: Number(before.size), sha256: digest.digest('hex') };
  } finally { await file.close(); }
}

export class TailBuffer {
  constructor(capacity = LIMITS.previewBytes) { this.buffer = Buffer.alloc(capacity); this.used = 0; this.position = 0; this.total = 0; }
  append(chunk) {
    this.total += chunk.length;
    if (chunk.length >= this.buffer.length) { chunk.copy(this.buffer, 0, chunk.length - this.buffer.length); this.used = this.buffer.length; this.position = 0; return; }
    const first = Math.min(chunk.length, this.buffer.length - this.position);
    chunk.copy(this.buffer, this.position, 0, first); if (first < chunk.length) chunk.copy(this.buffer, 0, first);
    this.position = (this.position + chunk.length) % this.buffer.length; this.used = Math.min(this.buffer.length, this.used + chunk.length);
  }
  bytes() {
    if (this.used < this.buffer.length) return Buffer.from(this.buffer.subarray(0, this.used));
    const result = Buffer.allocUnsafe(this.used); this.buffer.copy(result, 0, this.position); this.buffer.copy(result, this.used - this.position, 0, this.position); return result;
  }
  text() { const b = this.bytes(); let i = 0; if (this.total > this.used) while (i < b.length && (b[i] & 0xc0) === 0x80) i++; return b.subarray(i).toString('utf8'); }
}
export class RunStore {
  constructor(root, limits = LIMITS) { this.root = root; this.limits = limits; this.bytes = 0; this.checkAt = 0; this.diskCheck = null; this.missionCache = new Map(); }
  async init() {
    this.root = await privateDir(this.root);
    // Validate all storage namespaces before opening databases or publishing records.
    // O_NOFOLLOW on a file does not protect symlinked parent directories.
    const directories = ['tasks', 'missions', 'evidence', 'effects', 'artifacts', 'archive', 'records', 'content', 'worktrees'];
    for (const dir of directories) {
      const info = await lstat(join(this.root, dir)).catch(error => { if (error.code !== 'ENOENT') throw error; });
      if (info && (!info.isDirectory() || info.isSymbolicLink())) throw fail('Unsafe or symlinked record directories', 'STORAGE');
    }
    for (const dir of directories) await privateDir(join(this.root, dir));
    // These directories are reserved only to detect obsolete layouts. Current
    // mission, evidence, task, archive and publication-effect indexes live in transactional metadata.
    for (const name of ['tasks', 'evidence', 'effects', 'archive']) {
      if ((await readdir(join(this.root, name))).some(entry => entry.endsWith('.json'))) throw fail('Unsupported state layout; use a fresh runtime state directory', 'STORAGE');
    }
    this.scope = { ownerId: 'host', runId: hash(this.root) };
    try {
      this.metadata = await MetadataStore.open(join(this.root, 'metadata.sqlite'));
      const integrity = this.metadata.database.prepare('PRAGMA quick_check').get();
      if (integrity?.quick_check !== 'ok') throw fail('Metadata integrity check failed', 'STORAGE');
      if (!this.metadata.get('runs', this.scope, this.scope.runId)) this.metadata.put('runs', this.scope, { id: this.scope.runId, state: 'active' });
      this.taskCache = new TaskCache(this, this.limits.maxTasks);
      this.journal = await DurableJournal.open(join(this.root, 'effects.journal'));
      this.blobs = await ArtifactStore.open(join(this.root, 'content'), { metadata: this.metadata, journal: this.journal, diskBytes: this.limits.diskBytes, minFreeBytes: this.limits.minFreeBytes, maxArtifactBytes: this.limits.artifactBytes, maxRangeBytes: this.limits.readBytes });
      const artifacts = await opendir(join(this.root, 'artifacts'));
      for await (const entry of artifacts) {
        if (entry.isSymbolicLink()) throw fail('Unsafe artifact record link', 'STORAGE');
        if (entry.isFile() && (entry.name.endsWith('.bin') || entry.name.endsWith('.json'))) this.bytes += (await stat(join(this.root, 'artifacts', entry.name))).size;
      }
      await this.loadMissionRecords();
      await this.reconcileRecordFiles();
      await this.accountWorkspaces(join(this.root, 'worktrees'));
      if (this.bytes > this.limits.diskBytes) throw fail('Run storage quota is already exceeded', 'STORAGE');
      await this.checkDisk(true);
    } catch (error) {
      await this.journal?.close().catch(() => {});
      this.blobs?.close();
      this.metadata?.close();
      throw error;
    }
  }
  async accountWorkspaces(path, depth = 0) {
    if (depth > 130) throw fail('Workspace accounting depth exceeded', 'STORAGE');
    const entries = await opendir(path);
    for await (const entry of entries) {
      const child = join(path, entry.name), info = await lstat(child);
      if (info.isDirectory()) await this.accountWorkspaces(child, depth + 1);
      else this.reserve(info.size); // Never follow workspace symlinks.
    }
  }
  async loadMissionRecords() {
    this.missionCache.clear();
    const referenced = new Map(); let after = 0;
    do {
      const page = this.metadata.list('missions', this.scope, { after, limit: 128 });
      for (const row of page.items) {
        if (row.mission !== undefined) {
          if (row.recordId !== undefined || row.mission?.id !== row.id) throw fail('Mission metadata identity is invalid', 'STORAGE');
          this.missionCache.set(row.id, row.mission);
          continue;
        }
        if (typeof row.recordId !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$/.test(row.recordId)) throw fail('Invalid mission record metadata', 'STORAGE');
        const recordId = row.recordId;
        if (!Number.isSafeInteger(row.bytes) || row.bytes < 2 || row.bytes > 32 * 1024 || !/^[a-f0-9]{64}$/.test(row.sha256)) throw fail('Invalid mission record metadata', 'STORAGE');
        const name = `${recordId}.json`, path = join(this.root, 'missions', name);
        const identity = await fileIdentity(path);
        if (identity.bytes !== row.bytes || identity.sha256 !== row.sha256) throw fail('Mission record identity changed', 'STORAGE');
        const mission = await readJson(path, 32 * 1024);
        if (mission?.id !== row.id) throw fail('Mission record identity mismatch', 'STORAGE');
        referenced.set(name, row.id); this.missionCache.set(row.id, mission); this.bytes += identity.bytes;
      }
      if (page.nextCursor === null) break; after = page.nextCursor;
    } while (true);
    const entries = await opendir(join(this.root, 'missions'));
    for await (const entry of entries) {
      if (entry.isSymbolicLink() || !entry.isFile() || !referenced.has(entry.name)) throw fail('Unreferenced or unsafe mission record; explicit recovery required', 'STORAGE');
    }
  }

  async reconcileRecordFiles() {
    const entries = await opendir(join(this.root, 'records'));
    for await (const entry of entries) {
      if (entry.isSymbolicLink()) throw fail('Unsafe task record link', 'STORAGE');
      if (!entry.isFile()) continue;
      const path = join(this.root, 'records', entry.name), identity = await fileIdentity(path);
      this.bytes += identity.bytes;
      const base = entry.name.endsWith('.json') ? entry.name.slice(0, -5) : '';
      const recordId = /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$/.test(base) ? base : `orphan-${hash(entry.name).slice(0, 60)}`;
      if (!this.metadata.get('task_history', this.scope, recordId)) {
        this.metadata.put('task_history', this.scope, { id: recordId, state: 'orphaned', recordName: entry.name, bytes: identity.bytes, sha256: identity.sha256, orphanedAt: Date.now(), reason: 'record file has no committed metadata reference' }, { ifAbsent: true });
      }
    }
  }

  taskPath(id) { const row = this.metadata.get('work_items', this.scope, identifier(id)); if (!row) return join(this.root, 'records', `${identifier(id)}.json`); return join(this.root, 'records', `${row.recordId}.json`); }
  async saveTask(task) {
    const body = encodeJson(task, this.limits.stateBytes), recordId = randomUUID();
    const digest = hash(body), prior = this.metadata.get('work_items', this.scope, identifier(task.id));
    await this.checkDisk(); this.reserve(body.length);
    let published = false;
    try {
      // FS publication precedes both metadata references. If the transaction
      // fails, the accounted file remains an explicit orphan for reconciliation.
      await atomicJson(join(this.root, 'records', `${recordId}.json`), task, this.limits.stateBytes);
      published = true;
      this.metadata.transaction(() => {
        this.metadata.put('task_history', this.scope, { id: recordId, state: 'committed', taskId: task.id, taskState: task.state, recordId, bytes: body.length, sha256: digest, previousRecordId: prior?.recordId }, { ifAbsent: true });
        this.metadata.put('work_items', this.scope, { id: task.id, state: task.state, recordId, bytes: body.length, sha256: digest, name: task.name, roleName: task.role?.name, profile: task.profile, resultEpoch: task.resultEpoch ?? 1, ...(task.bundleRef ? { bundleRef: task.bundleRef } : {}), ...(task.requiredResultChecks?.length ? { requiredResultChecks: task.requiredResultChecks, verificationSources: (task.resultInputDeclarations ?? []).map(({ sourceId, resultEpoch }) => ({ sourceId, resultEpoch })), ...(task.productInput ? { verificationSubject: { ownerId: task.productInput.ownerId, productSha256: task.productInput.productSha256 } } : {}) } : {}), ...(task.quarantine ? { quarantine: task.quarantine } : {}) });
      });
    } catch (error) {
      if (!published) this.bytes -= body.length;
      throw error;
    }
    this.taskCache.set(task.id, task);
  }
  async loadTasks({ after = 0, limit = Math.min(this.limits.maxTasks, 128) } = {}) {
    const page = this.metadata.list('work_items', this.scope, { after, limit });
    const cache = new TaskCache(this, this.limits.maxTasks), result = [];
    for (const row of page.items) if (row.state !== 'archived') result.push(cache.get(row.id));
    return result;
  }
  taskRecords() { return { [Symbol.iterator]: () => this.taskCache.values() }; }
  missionPath(id) { const row = this.metadata.get('missions', this.scope, identifier(id)); return join(this.root, 'missions', `${row?.recordId ?? id}.json`); }
  getMission(id) { const mission = this.missionCache.get(identifier(id)); return mission && jsonClone(mission, 32 * 1024); }
  listMissions({ after = 0, limit = Math.min(this.limits.maxTasks, this.metadata.maxPageSize) } = {}) {
    const page = this.metadata.list('missions', this.scope, { after, limit });
    return { items: page.items.map(row => { const mission = this.missionCache.get(row.id); if (!mission) throw fail('Mission metadata has no verified record', 'STORAGE'); return jsonClone(mission, 32 * 1024); }), nextCursor: page.nextCursor };
  }
  async saveMission(mission) {
    const body = encodeJson(mission, 32 * 1024), cached = JSON.parse(body.toString('utf8')), inline = { id: cached.id, state: 'active', mission: cached };
    try {
      encodeJson({ ...inline, runId: this.scope.runId, ownerId: this.scope.ownerId, createdAt: Number.MAX_SAFE_INTEGER, updatedAt: Number.MAX_SAFE_INTEGER }, this.metadata.maxRowBytes);
      this.metadata.put('missions', this.scope, inline, { ifAbsent: true });
      this.missionCache.set(cached.id, cached);
      return;
    } catch (error) { if (error.code !== 'LIMIT') throw error; }
    const recordId = randomUUID(), path = join(this.root, 'missions', `${recordId}.json`);
    await this.checkDisk(); this.reserve(body.length); let published = false;
    try {
      await atomicJson(path, cached, 32 * 1024); published = true;
      this.metadata.put('missions', this.scope, { id: cached.id, state: 'active', recordId, bytes: body.length, sha256: hash(body) }, { ifAbsent: true });
      this.missionCache.set(cached.id, cached);
    } catch (error) { if (!published) this.bytes -= body.length; throw error; }
  }
  async loadMissions(options) { return this.listMissions(options).items; }
  evidencePath(id) { return join(this.root, 'evidence', `${identifier(id)}.json`); }
  getEvidence(id) {
    const row = this.metadata.get('evidence', this.scope, identifier(id));
    return row && { id: row.id, missionId: row.missionId, evidence: row.evidence, refIdentities: row.refIdentities };
  }
  listEvidence({ after = 0, limit = Math.min(this.limits.maxTasks, this.metadata.maxPageSize), missionId } = {}) {
    if (missionId === undefined) {
      const page = this.metadata.list('evidence', this.scope, { after, limit });
      return { items: page.items.map(row => ({ id: row.id, missionId: row.missionId, evidence: row.evidence, refIdentities: row.refIdentities })), nextCursor: page.nextCursor };
    }
    identifier(missionId, 'missionId');
    if (!Number.isSafeInteger(after) || after < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > this.metadata.maxPageSize) throw fail('Invalid evidence page');
    const rows = this.metadata.database.prepare("SELECT seq, data_json FROM evidence WHERE owner_id=? AND run_id=? AND seq>? AND json_extract(data_json,'$.missionId')=? ORDER BY seq LIMIT ?").all(this.scope.ownerId, this.scope.runId, after, missionId, limit + 1);
    const more = rows.length > limit, page = rows.slice(0, limit);
    return { items: page.map(({ data_json }) => { const row = JSON.parse(data_json); return { id: row.id, missionId: row.missionId, evidence: row.evidence, refIdentities: row.refIdentities }; }), nextCursor: more ? Number(page.at(-1).seq) : null };
  }
  *evidenceRecords() {
    let after = 0;
    do {
      const page = this.listEvidence({ after, limit: 128 });
      yield* page.items;
      if (page.nextCursor === null) return;
      after = page.nextCursor;
    } while (true);
  }
  evidenceCount() {
    return Number(this.metadata.database.prepare('SELECT COUNT(*) AS count FROM evidence WHERE owner_id=? AND run_id=?').get(this.scope.ownerId, this.scope.runId).count);
  }
  async saveEvidence(row) { this.metadata.put('evidence', this.scope, { id: row.id, state: 'active', missionId: row.missionId, evidence: row.evidence, refIdentities: row.refIdentities }, { ifAbsent: true }); }
  async loadEvidence(options) { return options === undefined ? [...this.evidenceRecords()] : this.listEvidence(options).items; }
  effectRecordId(id) { return `publication-${hash(identifier(id))}`; }
  async saveEffect(row) {
    identifier(row?.id); encodeJson(row, 8192);
    this.metadata.put('steps', this.scope, { id: this.effectRecordId(row.id), state: row.state, kind: 'universal-publication', publication: row });
  }
  async loadEffects() {
    const effects = []; let after = 0;
    do {
      const page = this.metadata.list('steps', this.scope, { after, limit: 128 });
      for (const record of page.items) if (record.kind === 'universal-publication') {
        const row = record.publication;
        if (!row || record.id !== this.effectRecordId(row.id)) throw fail('Invalid publication effect metadata', 'STORAGE');
        effects.push(row);
        if (effects.length > this.limits.maxTasks) throw fail('Too many publication effect journals', 'STORAGE');
      }
      after = page.nextCursor;
    } while (after !== null);
    return effects;
  }
  async archiveTask(id) { const row = this.metadata.require('work_items', this.scope, identifier(id)); this.metadata.put('work_items', this.scope, { ...row, state: 'archived' }); this.taskCache.delete(id); }
  async checkDisk(force = false) {
    if (!force && this.bytes < this.checkAt) return;
    this.diskCheck ??= (async () => {
      const info = await statfs(this.root);
      if (info.bavail * info.bsize < this.limits.minFreeBytes) throw fail('Low disk space; output capture stopped', 'STORAGE');
      this.checkAt = this.bytes + 32 * MiB;
    })().finally(() => { this.diskCheck = null; });
    await this.diskCheck;
  }
  reserve(bytes) {
    if (this.bytes + bytes > this.limits.diskBytes) throw fail('Run artifact disk quota exceeded; archive/delete an old run or increase diskBytes', 'STORAGE');
    this.bytes += bytes;
  }
  async artifact(owner, label, properties = {}) {
    if (!properties || typeof properties !== 'object' || Array.isArray(properties)) throw fail('Invalid artifact properties');
    const allowed = new Set(['historyPreviousArtifactId', 'responseType', 'responseMetadata']);
    if (Object.keys(properties).some(key => !allowed.has(key))) throw fail('Unsupported artifact property');
    if (properties.historyPreviousArtifactId !== undefined) identifier(properties.historyPreviousArtifactId, 'previous response artifact');
    if (properties.responseType !== undefined && !['report', 'message'].includes(properties.responseType)) throw fail('Invalid response artifact type');
    if (properties.responseMetadata !== undefined) {
      if (!properties.responseMetadata || typeof properties.responseMetadata !== 'object' || Array.isArray(properties.responseMetadata)) throw fail('Invalid response artifact metadata');
      encodeJson(properties.responseMetadata, 2048);
    }
    const id = randomUUID(); const path = join(this.root, 'artifacts', `${id}.bin`);
    const metadata = { id, owner: identifier(owner), label: short(label, 120), ...properties, bytes: 0, complete: false, createdAt: Date.now() };
    const metadataBytes = encodeJson(metadata, 8192).length;
    await this.checkDisk(); this.reserve(metadataBytes);
    let published = false, staged;
    try {
      staged = await this.blobs.begin(this.scope, { id, label: metadata.label });
      await atomicJson(join(this.root, 'artifacts', `${id}.json`), metadata, 8192); published = true;
      await link(staged.path, path);
      return new Artifact(this, staged, path, metadata, metadataBytes);
    } catch (error) {
      if (!published) this.bytes -= metadataBytes;
      if (staged && !staged.closed) await staged.abort(error.message).catch(cleanupError => { error.cleanupError = cleanupError; });
      throw error;
    }
  }
  async artifactInfo(id) { return readJson(join(this.root, 'artifacts', `${identifier(id)}.json`), 8192); }
  // Identity verification is streaming; response/UI windows never limit originals.
  async artifactIdentity(id) {
    const info = await this.artifactInfo(id);
    if (info.id !== id || !info.complete || !Number.isSafeInteger(info.bytes) || info.bytes < 0 || !/^[a-f0-9]{64}$/.test(info.sha256 ?? '')) throw fail('Immutable input artifact unavailable', 'STORAGE');
    const path = join(this.root, 'artifacts', `${identifier(id)}.bin`);
    const file = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
    try {
      const before = await file.stat();
      if (!before.isFile() || before.size !== info.bytes) throw fail('Artifact bytes changed after completion', 'STORAGE');
      const digest = createHash('sha256'), buffer = Buffer.alloc(65536); let bytes = 0;
      while (true) { const { bytesRead } = await file.read(buffer, 0, buffer.length, null); if (!bytesRead) break; bytes += bytesRead; if (bytes > info.bytes) throw fail('Artifact grew after completion', 'STORAGE'); digest.update(buffer.subarray(0, bytesRead)); }
      const after = await file.stat(), named = await lstat(path), sha256 = digest.digest('hex');
      // Sealed content-addressed blobs are shared through hard links. Publishing
      // another identical artifact changes nlink/ctime, not the bytes. Bind the
      // full content hash, stable content/ownership metadata and the ID path's
      // inode instead of treating legitimate link churn as content corruption.
      const stable = value => value.isFile() && ['dev', 'ino', 'size', 'mtimeMs', 'mode', 'uid', 'gid'].every(key => value[key] === before[key]);
      if (bytes !== info.bytes || !stable(after) || !stable(named) || sha256 !== info.sha256) throw fail('Artifact identity changed', 'STORAGE');
      return { id, owner: info.owner, bytes, complete: true, sha256 };
    } finally { await file.close(); }
  }
  async readArtifact(id, byteOffset = 0, limitBytes = this.limits.readBytes) {
    id = identifier(id);
    const info = await this.artifactInfo(id);
    if (info.id !== id || !Number.isSafeInteger(info.bytes) || info.bytes < 0 || (info.complete && !/^[a-f0-9]{64}$/.test(info.sha256 ?? ''))) throw fail('Invalid artifact identity', 'STORAGE');
    const page = await readPage(join(this.root, 'artifacts', `${id}.bin`), byteOffset, limitBytes, this.limits.readBytes);
    if (page.fileBytes !== info.bytes) throw fail('Artifact size does not match its durable metadata', 'STORAGE');
    return { ...page, artifact: info };
  }
  async close() { await this.journal?.close(); this.blobs?.close(); this.metadata?.close(); }
}
export class Artifact {
  constructor(store, staged, path, info, metadataBytes) { this.store = store; this.staged = staged; this.path = path; this.info = info; this.metadataBytes = metadataBytes; this.tail = new TailBuffer(store.limits.previewBytes); this.closed = false; this.busy = false; }
  async write(chunk) {
    if (this.closed || this.busy) throw fail('Artifact writes must be awaited and serialized', 'STORAGE');
    this.busy = true; let reserved = false; const before = this.staged.bytes;
    try {
      await this.store.checkDisk(); this.store.reserve(chunk.length); reserved = true;
      await this.staged.write(chunk); this.tail.append(chunk); this.info.bytes = this.staged.bytes;
    } finally {
      this.info.bytes = this.staged.bytes;
      if (reserved) this.store.bytes -= chunk.length - (this.staged.bytes - before);
      this.busy = false;
    }
  }
  async finish(complete = true, error) {
    if (this.finishing) return this.finishing;
    if (this.busy) throw fail('Cannot seal during an artifact write', 'STORAGE');
    this.closed = true;
    this.finishing = (async () => {
      let failure;
      try {
        if (complete) {
          const sealed = await this.staged.seal(); this.info.sha256 = sealed.sha256;
          // Publish an ID link to the sealed blob. This is the current public
          // read layout, not a second storage backend or schema fallback.
          const temp = `${this.path}.${randomUUID()}.tmp`;
          try { await link(this.store.blobs._blobPath(sealed.sha256), temp); await rename(temp, this.path); } finally { await unlink(temp).catch(e => { if (e.code !== 'ENOENT') throw e; }); }
        } else await this.staged.abort(error ?? 'Capture incomplete');
      } catch (e) { failure = e; complete = false; error ??= e.message; }
      this.info.bytes = this.staged.bytes; this.info.complete = complete; this.info.finishedAt = Date.now(); if (error) this.info.error = short(error, 1024);
      const nextMetadataBytes = encodeJson(this.info, 8192).length, delta = nextMetadataBytes - this.metadataBytes;
      if (delta > 0) { await this.store.checkDisk(); this.store.reserve(delta); }
      try { await atomicJson(join(this.store.root, 'artifacts', `${this.info.id}.json`), this.info, 8192); }
      catch (metadataError) { if (delta > 0) this.store.bytes -= delta; throw metadataError; }
      if (delta < 0) this.store.bytes += delta; this.metadataBytes = nextMetadataBytes;
      if (failure) throw failure;
      return this.summary();
    })();
    return this.finishing;
  }
  summary() { return { ...this.info, preview: this.tail.text(), truncated: this.info.bytes > this.tail.used }; }
}
export async function readPage(path, byteOffset = 0, limitBytes = LIMITS.readBytes, maxBytes = LIMITS.readBytes) {
  if (!Number.isSafeInteger(byteOffset) || byteOffset < 0 || !Number.isSafeInteger(limitBytes) || limitBytes < 1 || limitBytes > maxBytes) throw fail('Invalid byte range');
  const file = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
  try {
    const info = await file.stat(); if (!info.isFile()) throw fail('Only regular files can be read');
    const bytes = Buffer.alloc(limitBytes); const { bytesRead } = await file.read(bytes, 0, limitBytes, byteOffset);
    const body = bytes.subarray(0, bytesRead);
    return { text: body.toString('utf8'), byteOffset, bytesRead, nextByteOffset: byteOffset + bytesRead, fileBytes: info.size, eof: byteOffset + bytesRead >= info.size, encoding: 'utf8; byte ranges may split a character' };
  } finally { await file.close(); }
}
export { acquireOwnership as acquireLock } from './runtime/ownership.mjs';
