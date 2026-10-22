import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { chmod, link, lstat, mkdir, open, opendir, realpath, statfs, unlink } from 'node:fs/promises';
import { join } from 'node:path';
import { fail, short } from '../limits.mjs';

function artifactError(message, code = 'ARTIFACT') {
  return fail(message, code);
}

function identifier(value, name) {
  if (typeof value !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$/.test(value)) throw artifactError(`Invalid ${name}`, 'INVALID');
  return value;
}

function scopeValue(scope) {
  if (!scope || typeof scope !== 'object') throw artifactError('Artifact owner scope is required', 'OWNER_SCOPE');
  return { ownerId: identifier(scope.ownerId, 'ownerId'), runId: identifier(scope.runId, 'runId') };
}

function boundedText(value, name, maxBytes, { empty = false } = {}) {
  if (typeof value !== 'string' || (!empty && !value) || Buffer.byteLength(value) > maxBytes) throw artifactError(`Invalid ${name}`, 'INVALID');
  return value;
}

function integer(value, name, min = 0, max = Number.MAX_SAFE_INTEGER) {
  if (!Number.isSafeInteger(value) || value < min || value > max) throw artifactError(`Invalid ${name}`, 'INVALID');
  return value;
}

function sha256(value) {
  if (typeof value !== 'string' || !/^[a-f0-9]{64}$/.test(value)) throw artifactError('Invalid artifact sha256', 'STORAGE');
  return value;
}

async function syncDirectory(path) {
  const handle = await open(path, constants.O_RDONLY);
  try {
    await handle.sync();
  } catch (error) {
    if (!['EINVAL', 'ENOTSUP', 'EBADF'].includes(error.code)) throw error;
  } finally {
    await handle.close();
  }
}

async function preparePrivateDirectory(path) {
  try {
    const info = await lstat(path);
    if (!info.isDirectory() || info.isSymbolicLink()) throw artifactError(`Artifact path is not a private directory: ${path}`, 'STORAGE');
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
    await mkdir(path, { recursive: true, mode: 0o700 });
  }
  await chmod(path, 0o700);
  return realpath(path);
}

async function hashHandle(handle, expectedBytes) {
  const digest = createHash('sha256');
  const buffer = Buffer.allocUnsafe(128 * 1024);
  let offset = 0;
  while (offset < expectedBytes) {
    const want = Math.min(buffer.length, expectedBytes - offset);
    const { bytesRead } = await handle.read(buffer, 0, want, offset);
    if (!bytesRead) throw artifactError('Artifact ended before its recorded size', 'ARTIFACT_CORRUPT');
    digest.update(buffer.subarray(0, bytesRead));
    offset += bytesRead;
  }
  const extra = Buffer.allocUnsafe(1);
  if ((await handle.read(extra, 0, 1, offset)).bytesRead) throw artifactError('Artifact exceeds its recorded size', 'ARTIFACT_CORRUPT');
  return digest.digest('hex');
}
async function blobMatches(path, bytes, expectedHash) {
  let handle;
  try {
    handle = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
    const info = await handle.stat();
    return info.isFile() && Number(info.size) === bytes && await hashHandle(handle, bytes) === expectedHash;
  } catch {
    return false;
  } finally {
    await handle?.close().catch(() => {});
  }
}


async function* regularFiles(path, { suffix }) {
  const directory = await opendir(path);
  for await (const entry of directory) {
    if (entry.isSymbolicLink()) throw artifactError('Artifact store contains a symlink', 'STORAGE');
    const entryPath = join(path, entry.name);
    if (entry.isDirectory()) {
      const childDirectory = await opendir(entryPath);
      for await (const child of childDirectory) {
        if (child.isSymbolicLink()) throw artifactError('Artifact store contains a symlink', 'STORAGE');
        if (!child.isFile()) continue;
        if (!suffix || child.name.endsWith(suffix)) yield join(entryPath, child.name);
      }
    } else if (entry.isFile() && (!suffix || entry.name.endsWith(suffix))) {
      yield entryPath;
    }
  }
}

/** Content-addressed, immutable artifact storage with staged streaming writes. */
export class ArtifactStore {
  static async open(root, {
    metadata,
    journal = null,
    diskBytes = 32 * 1024 * 1024 * 1024,
    minFreeBytes = 512 * 1024 * 1024,
    maxArtifactBytes = 8 * 1024 * 1024 * 1024,
    maxRangeBytes = 64 * 1024,
    maxFiles = null,
    statfsFn = statfs,
  } = {}) {
    if (!metadata || typeof metadata.put !== 'function' || typeof metadata.require !== 'function') {
      throw artifactError('ArtifactStore requires a MetadataStore', 'INVALID');
    }
    const resolvedRoot = await preparePrivateDirectory(root);
    const stagingDir = await preparePrivateDirectory(join(resolvedRoot, 'staging'));
    const blobsDir = await preparePrivateDirectory(join(resolvedRoot, 'blobs'));
    const options = {
      diskBytes: integer(diskBytes, 'diskBytes', 1),
      minFreeBytes: integer(minFreeBytes, 'minFreeBytes'),
      maxArtifactBytes: integer(maxArtifactBytes, 'maxArtifactBytes', 1),
      maxRangeBytes: integer(maxRangeBytes, 'maxRangeBytes', 1, 16 * 1024 * 1024),
      maxFiles: maxFiles === null ? null : integer(maxFiles, 'maxFiles', 1, 10_000_000),
    };
    let usageBytes = 0, fileCount = 0;
    for (const [path, suffix] of [[stagingDir, '.stage'], [blobsDir, '.blob']]) {
      for await (const filePath of regularFiles(path, { suffix })) {
        if (options.maxFiles !== null && fileCount >= options.maxFiles) throw artifactError('Artifact file count budget exceeded', 'ARTIFACT_BUDGET');
        if (fileCount >= Number.MAX_SAFE_INTEGER) throw artifactError('Artifact file count exhausted', 'ARTIFACT_BUDGET');
        fileCount++; usageBytes += Number((await lstat(filePath)).size);
        if (!Number.isSafeInteger(usageBytes) || usageBytes > options.diskBytes) throw artifactError('Artifact disk byte budget is already exceeded', 'ARTIFACT_BUDGET');
      }
    }
    const store = new ArtifactStore(resolvedRoot, stagingDir, blobsDir, metadata, journal, { ...options, statfsFn, usageBytes, fileCount });
    await store._checkFreeSpace(0);
    return store;
  }

  constructor(root, stagingDir, blobsDir, metadata, journal, options) {
    this.root = root;
    this.stagingDir = stagingDir;
    this.blobsDir = blobsDir;
    this.metadata = metadata;
    this.journal = journal;
    this.diskBytes = options.diskBytes;
    this.minFreeBytes = options.minFreeBytes;
    this.maxArtifactBytes = options.maxArtifactBytes;
    this.maxRangeBytes = options.maxRangeBytes;
    this.maxFiles = options.maxFiles;
    this.statfsFn = options.statfsFn;
    this.usageBytes = options.usageBytes;
    this.reservedBytes = 0;
    this.fileCount = options.fileCount;
    this.reservedFiles = 0;
    this.closed = false;
  }

  _assertOpen() {
    if (this.closed) throw artifactError('Artifact store is closed', 'STORAGE');
  }

  async _checkFreeSpace(bytes) {
    const info = await this.statfsFn(this.root);
    const blockSize = typeof info.bsize === 'bigint' ? info.bsize : BigInt(info.bsize);
    const availableBlocks = typeof info.bavail === 'bigint' ? info.bavail : BigInt(info.bavail);
    const available = availableBlocks * blockSize;
    const required = BigInt(this.minFreeBytes + this.reservedBytes + bytes);
    if (available < required) throw artifactError('Insufficient free disk space for artifact write', 'ARTIFACT_BUDGET');
  }

  async _reserve(bytes) {
    this._assertOpen();
    await this._checkFreeSpace(bytes);
    if (this.usageBytes + this.reservedBytes + bytes > this.diskBytes) {
      throw artifactError('Artifact disk byte budget exceeded; no bytes were written', 'ARTIFACT_BUDGET');
    }
    this.reservedBytes += bytes;
  }

  _releaseReservation(bytes, committed = 0) {
    this.reservedBytes -= bytes;
    this.usageBytes += committed;
  }

  _reserveFile() {
    if (this.fileCount + this.reservedFiles >= Number.MAX_SAFE_INTEGER || (this.maxFiles !== null && this.fileCount + this.reservedFiles >= this.maxFiles)) throw artifactError('Artifact file count budget exceeded', 'ARTIFACT_BUDGET');
    this.reservedFiles++;
  }

  _releaseFileReservation(created = false) {
    this.reservedFiles--;
    if (created) this.fileCount++;
  }

  _stagePath(id) {
    return join(this.stagingDir, `${identifier(id, 'artifact id')}.stage`);
  }

  _blobPath(hash) {
    const identity = sha256(hash);
    return join(this.blobsDir, identity.slice(0, 2), `${identity}.blob`);
  }

  async begin(scopeInput, {
    id = randomUUID(),
    label,
    mediaType = 'application/octet-stream',
  } = {}) {
    this._assertOpen();
    const scope = scopeValue(scopeInput);
    identifier(id, 'artifact id');
    boundedText(label, 'artifact label', 256);
    boundedText(mediaType, 'artifact media type', 128);
    this.metadata.require('runs', { ownerId: scope.ownerId }, scope.runId);
    if (this.metadata.get('artifacts', scope, id)) throw artifactError(`Artifact already exists: ${id}`, 'CONFLICT');
    const stagePath = this._stagePath(id);
    let intent;
    let file;
    let fileSlot = false;
    try {
      this._reserveFile();
      fileSlot = true;
      if (this.journal) intent = await this.journal.begin('artifact-stage', scope, { artifactId: id });
      const record = this.metadata.put('artifacts', scope, {
        id,
        state: 'staging',
        label,
        mediaType,
        bytes: 0,
        stageName: `${id}.stage`,
      }, { ifAbsent: true });
      file = await open(stagePath, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | (constants.O_NOFOLLOW ?? 0), 0o600);
      this._releaseFileReservation(true);
      fileSlot = false;
      await syncDirectory(this.stagingDir);
      if (intent) await this.journal.complete(intent.intentId, { artifactId: id, state: 'staging' });
      return new ArtifactWriter(this, scope, file, stagePath, record);
    } catch (error) {
      if (fileSlot) this._releaseFileReservation(false);
      if (file) await file.close().catch(() => {});
      const current = this.metadata.get('artifacts', scope, id);
      if (current && ['staging', 'sealing'].includes(current.state)) {
        try { this.metadata.transition('artifacts', scope, id, { from: current.state, to: 'orphaned', patch: { error: short(error.message, 2048) } }); } catch {}
      }
      if (intent) await this.journal.fail(intent.intentId, error).catch(() => {});
      throw error;
    }
  }

  _sealed(scopeInput, idValue) {
    const scope = scopeValue(scopeInput);
    const id = identifier(idValue, 'artifact id');
    const record = this.metadata.require('artifacts', scope, id);
    if (record.state !== 'sealed') throw artifactError('Artifact is not sealed', 'ARTIFACT_INCOMPLETE');
    sha256(record.sha256);
    integer(record.bytes, 'artifact bytes');
    return { scope, record, path: this._blobPath(record.sha256) };
  }

  async _openSealed(scope, id) {
    const sealed = this._sealed(scope, id);
    const handle = await open(sealed.path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
    try {
      const info = await handle.stat();
      if (!info.isFile() || Number(info.size) !== sealed.record.bytes) throw artifactError('Sealed artifact size does not match metadata', 'ARTIFACT_CORRUPT');
      return { ...sealed, handle };
    } catch (error) {
      await handle.close();
      throw error;
    }
  }

  async readRange(scope, id, { offset = 0, length = this.maxRangeBytes } = {}) {
    this._assertOpen();
    integer(offset, 'range offset');
    integer(length, 'range length', 1, this.maxRangeBytes);
    const opened = await this._openSealed(scope, id);
    try {
      if (offset > opened.record.bytes) throw artifactError('Range offset exceeds artifact size', 'INVALID_RANGE');
      const size = Math.min(length, opened.record.bytes - offset);
      const bytes = Buffer.allocUnsafe(size);
      let used = 0;
      while (used < size) {
        const result = await opened.handle.read(bytes, used, size - used, offset + used);
        if (!result.bytesRead) break;
        used += result.bytesRead;
      }
      if (used !== size) throw artifactError('Sealed artifact changed during read', 'ARTIFACT_CORRUPT');
      return {
        bytes,
        offset,
        bytesRead: used,
        nextOffset: offset + used,
        size: opened.record.bytes,
        eof: offset + used >= opened.record.bytes,
        sha256: opened.record.sha256,
        encoding: 'raw',
      };
    } finally {
      await opened.handle.close();
    }
  }

  async readUtf8Range(scope, id, { offset = 0, maxBytes = this.maxRangeBytes } = {}) {
    this._assertOpen();
    integer(offset, 'range offset');
    integer(maxBytes, 'maxBytes', 1, this.maxRangeBytes);
    const opened = await this._openSealed(scope, id);
    try {
      const size = opened.record.bytes;
      if (offset > size) throw artifactError('Range offset exceeds artifact size', 'INVALID_RANGE');
      if (offset === size) return { text: '', offset, bytesRead: 0, nextOffset: offset, size, eof: true, sha256: opened.record.sha256, encoding: 'utf8' };
      const first = Buffer.allocUnsafe(1);
      if (!(await opened.handle.read(first, 0, 1, offset)).bytesRead) throw artifactError('Artifact changed during read', 'ARTIFACT_CORRUPT');
      if ((first[0] & 0xc0) === 0x80) throw artifactError('UTF-8 range offset splits a code point', 'INVALID_RANGE');
      const desired = Math.min(maxBytes, size - offset);
      const bytes = Buffer.allocUnsafe(desired);
      let used = 0;
      while (used < desired) {
        const result = await opened.handle.read(bytes, used, desired - used, offset + used);
        if (!result.bytesRead) break;
        used += result.bytesRead;
      }
      if (used !== desired) throw artifactError('Artifact changed during read', 'ARTIFACT_CORRUPT');
      const decoder = new TextDecoder('utf-8', { fatal: true });
      const minimum = Math.max(0, used - 3);
      for (let end = used; end >= minimum; end--) {
        try {
          const text = decoder.decode(bytes.subarray(0, end));
          if (!end && used) throw artifactError('maxBytes is too small for the next UTF-8 code point', 'RANGE_TOO_SMALL');
          return {
            text,
            offset,
            bytesRead: end,
            nextOffset: offset + end,
            size,
            eof: offset + end >= size,
            sha256: opened.record.sha256,
            encoding: 'utf8',
          };
        } catch (error) {
          if (error?.code === 'RANGE_TOO_SMALL') throw error;
        }
      }
      throw artifactError('Artifact range is not valid UTF-8', 'INVALID_UTF8');
    } finally {
      await opened.handle.close();
    }
  }

  async verify(scope, id) {
    this._assertOpen();
    const opened = await this._openSealed(scope, id);
    try {
      const actual = await hashHandle(opened.handle, opened.record.bytes);
      if (actual !== opened.record.sha256) throw artifactError('Artifact hash does not match metadata', 'ARTIFACT_CORRUPT');
      return { id: opened.record.id, bytes: opened.record.bytes, sha256: actual, verified: true };
    } finally {
      await opened.handle.close();
    }
  }

  /** Preserve and mark interrupted records; never removes unknown files. */
  async recoverOrphans(scopeInput, { after = 0, limit = Math.min(500, this.metadata.maxPageSize ?? 500) } = {}) {
    this._assertOpen();
    const scope = scopeValue(scopeInput);
    this.metadata.require('runs', { ownerId: scope.ownerId }, scope.runId);
    const page = this.metadata.list('artifacts', scope, { after, limit });
    const recovered = [];
    const damaged = [];
    for (const record of page.items) {
      if (['staging', 'sealing'].includes(record.state)) {
        const stageExists = await lstat(this._stagePath(record.id)).then(info => info.isFile()).catch(error => error.code === 'ENOENT' ? false : Promise.reject(error));
        let blobExists = false;
        if (record.sha256) blobExists = await lstat(this._blobPath(record.sha256)).then(info => info.isFile()).catch(error => error.code === 'ENOENT' ? false : Promise.reject(error));
        const next = this.metadata.transition('artifacts', scope, record.id, {
          from: record.state,
          to: 'orphaned',
          patch: { previousState: record.state, orphanedAt: Date.now(), stageExists, recoverableBlob: blobExists },
        });
        recovered.push(next);
      } else if (record.state === 'sealed') {
        const valid = await blobMatches(this._blobPath(record.sha256), record.bytes, record.sha256);
        if (!valid) {
          const next = this.metadata.transition('artifacts', scope, record.id, {
            from: 'sealed',
            to: 'orphaned',
            patch: { previousState: 'sealed', orphanedAt: Date.now(), error: 'sealed blob missing, wrong size, or hash mismatch' },
          });
          damaged.push(next);
        }
      }
    }
    return {
      recovered,
      damaged,
      nextCursor: page.nextCursor,
      pendingIntents: this.journal ? this.journal.recover(scope).pending : [],
    };
  }

  close() {
    this.closed = true;
  }
}

export class ArtifactWriter {
  constructor(store, scope, file, path, record) {
    this.store = store;
    this.scope = scope;
    this.file = file;
    this.path = path;
    this.record = record;
    this.bytes = 0;
    this.hash = createHash('sha256');
    this.closed = false;
    this.busy = false;
    this.finishing = null;
  }

  async write(value) {
    if (this.closed || this.busy) throw artifactError('Artifact writes must be awaited and serialized', 'ARTIFACT_STATE');
    let chunk;
    try {
      if (typeof value === 'string') chunk = Buffer.from(value);
      else if (Buffer.isBuffer(value) || ArrayBuffer.isView(value)) chunk = Buffer.from(value);
      else if (value instanceof ArrayBuffer) chunk = Buffer.from(value.slice(0));
      else throw new TypeError('Artifact chunks must be strings, Buffers, ArrayBuffers, or typed arrays');
    } catch (error) {
      throw artifactError(error.message, 'INVALID');
    }
    if (!chunk.length) return this.bytes;
    if (this.bytes + chunk.length > this.store.maxArtifactBytes) throw artifactError('Artifact byte quota exceeded; no bytes were written', 'ARTIFACT_BUDGET');
    this.busy = true;
    const before = this.bytes;
    let offset = 0;
    let reserved = false;
    try {
      await this.store._reserve(chunk.length);
      reserved = true;
      while (offset < chunk.length) {
        const result = await this.file.write(chunk, offset, chunk.length - offset, before + offset);
        if (!result.bytesWritten) throw artifactError('Zero-byte artifact write', 'STORAGE');
        offset += result.bytesWritten;
      }
      const nextBytes = before + chunk.length;
      this.record = this.store.metadata.put('artifacts', this.scope, { ...this.record, bytes: nextBytes, updatedAt: Date.now() });
      this.hash.update(chunk);
      this.bytes = nextBytes;
      this.store._releaseReservation(chunk.length, chunk.length);
      reserved = false;
      return this.bytes;
    } catch (error) {
      // A preflight budget rejection happens before touching the file and does
      // not poison the writer; callers may still seal the accepted prefix.
      if (!reserved) throw error;
      let committed = 0;
      try {
        await this.file.truncate(before);
        await this.file.sync();
      } catch (rollbackError) {
        error.rollbackError = rollbackError;
        const actual = await this.file.stat().then(info => Number(info.size)).catch(() => before + offset);
        committed = Math.max(0, actual - before);
      }
      this.store._releaseReservation(chunk.length, committed);
      reserved = false;
      this.closed = true;
      await this.file.close().catch(() => {});
      const current = this.store.metadata.get('artifacts', this.scope, this.record.id);
      if (current && ['staging', 'sealing'].includes(current.state)) {
        try { this.record = this.store.metadata.transition('artifacts', this.scope, current.id, { from: current.state, to: 'orphaned', patch: { bytes: before + committed, error: short(error.message, 2048) } }); } catch {}
      }
      throw error;
    } finally {
      if (reserved) this.store._releaseReservation(chunk.length, 0);
      this.busy = false;
    }
  }

  seal() {
    if (this.finishing) return this.finishing;
    if (this.closed || this.busy) return Promise.reject(artifactError('Artifact cannot be sealed in its current state', 'ARTIFACT_STATE'));
    this.closed = true;
    this.finishing = this._seal();
    return this.finishing;
  }

  async _seal() {
    let intent;
    let metadataSealed = false;
    let duplicate = false;
    const hash = this.hash.digest('hex');
    const blobDir = join(this.store.blobsDir, hash.slice(0, 2));
    const blobPath = this.store._blobPath(hash);
    try {
      await this.file.sync();
      await this.file.chmod(0o400);
      await this.file.sync();
      await this.file.close();
      if (this.store.journal) intent = await this.store.journal.begin('artifact-seal', this.scope, { artifactId: this.record.id, sha256: hash, bytes: this.bytes });
      this.record = this.store.metadata.transition('artifacts', this.scope, this.record.id, {
        from: 'staging',
        to: 'sealing',
        patch: { sha256: hash, bytes: this.bytes },
      });
      await mkdir(blobDir, { recursive: true, mode: 0o700 });
      await chmod(blobDir, 0o700);
      await syncDirectory(this.store.blobsDir);
      try {
        await link(this.path, blobPath);
        this.store.fileCount++;
      } catch (error) {
        if (error.code !== 'EEXIST') throw error;
        duplicate = true;
        const existing = await open(blobPath, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
        try {
          const info = await existing.stat();
          if (!info.isFile() || Number(info.size) !== this.bytes || await hashHandle(existing, this.bytes) !== hash) {
            throw artifactError('Existing content-addressed blob does not match its name', 'ARTIFACT_CORRUPT');
          }
        } finally {
          await existing.close();
        }
      }
      await syncDirectory(blobDir);
      this.record = this.store.metadata.transition('artifacts', this.scope, this.record.id, {
        from: 'sealing',
        to: 'sealed',
        patch: { sha256: hash, bytes: this.bytes, sealedAt: Date.now(), blobName: `${hash}.blob` },
      });
      metadataSealed = true;
      await unlink(this.path);
      this.store.fileCount--;
      await syncDirectory(this.store.stagingDir);
      if (duplicate) this.store.usageBytes -= this.bytes;
      if (intent) await this.store.journal.complete(intent.intentId, { artifactId: this.record.id, sha256: hash, bytes: this.bytes });
      return { ...this.record };
    } catch (error) {
      await this.file.close().catch(() => {});
      if (!metadataSealed) {
        const current = this.store.metadata.get('artifacts', this.scope, this.record.id);
        if (current && ['staging', 'sealing'].includes(current.state)) {
          try {
            this.record = this.store.metadata.transition('artifacts', this.scope, current.id, {
              from: current.state,
              to: 'orphaned',
              patch: { previousState: current.state, error: short(error.message, 2048), sha256: hash, bytes: this.bytes },
            });
          } catch {}
        }
      }
      if (intent) await this.store.journal.fail(intent.intentId, error).catch(() => {});
      throw error;
    }
  }

  async abort(reason = 'artifact capture aborted') {
    if (this.finishing) return this.finishing;
    if (this.closed) return { ...this.record };
    this.closed = true;
    try { await this.file.sync(); } finally { await this.file.close(); }
    const current = this.store.metadata.get('artifacts', this.scope, this.record.id);
    if (current && ['staging', 'sealing'].includes(current.state)) {
      this.record = this.store.metadata.transition('artifacts', this.scope, current.id, {
        from: current.state,
        to: 'orphaned',
        patch: { previousState: current.state, bytes: this.bytes, error: short(reason, 2048), orphanedAt: Date.now() },
      });
    }
    return { ...this.record };
  }
}

export async function openArtifactStore(root, options) {
  return ArtifactStore.open(root, options);
}
