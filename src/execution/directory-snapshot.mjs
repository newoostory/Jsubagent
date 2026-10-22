import { createHash, randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { lstat, mkdir, open, opendir, realpath, rm } from 'node:fs/promises';
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fail, identifier } from '../limits.mjs';

const FORMAT = 'pi-directory-snapshot-v1';
const EXCLUDED = new Set(['.git', '.pi', 'node_modules']);
const MAX_ENTRIES = 100_000;
const MAX_PATH_BYTES = 4096;
const MAX_PATH_TOTAL = 16 * 1024 * 1024;
const MAX_DEPTH = 128;
const MAX_RECORD_BYTES = 8192;
const DEFAULT_PAGE_BYTES = 64 * 1024;

function workspaceError(message, code = 'WORKSPACE') { return fail(message, code); }
function aborted(signal) { signal?.throwIfAborted(); }
function inside(root, path) {
  const rel = relative(root, path);
  return rel === '' || (rel !== '..' && !rel.startsWith(`..${sep}`) && !isAbsolute(rel));
}
function bigintStat(path) { return lstat(path, { bigint: true }); }
function kind(info) { return info.isDirectory() ? 'directory' : info.isFile() ? 'file' : info.isSymbolicLink() ? 'symlink' : 'special'; }
function mode(info) { return Number(info.mode & 0o777n); }
function identity(info) {
  return [info.dev, info.ino, info.size, info.mode, info.mtimeNs, info.ctimeNs].map(String).join(':');
}
function safeSize(info, path) {
  const value = Number(info.size);
  if (!Number.isSafeInteger(value) || value < 0) throw workspaceError(`Snapshot file is impractically large: ${path}`, 'LIMIT');
  return value;
}
function sameStat(expected, actual, message) {
  if (identity(expected) !== identity(actual)) throw workspaceError(message);
}
async function existing(path) {
  try { return await bigintStat(path); } catch (error) { if (error.code === 'ENOENT') return null; throw error; }
}
function jsonLine(value) { return Buffer.from(`${JSON.stringify(value)}\n`); }
function exactKeys(value, keys, message) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).sort().join('\0') !== [...keys].sort().join('\0')) throw workspaceError(message, 'STORAGE');
}
function record(line, message) {
  try { return JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(line)); }
  catch { throw workspaceError(message, 'STORAGE'); }
}
function validInteger(value, min, max, message) {
  if (!Number.isSafeInteger(value) || value < min || value > max) throw workspaceError(message, 'STORAGE');
  return value;
}
function pathParts(path) {
  if (typeof path !== 'string' || !path || Buffer.byteLength(path) > MAX_PATH_BYTES || path.includes('\\') || /[\x00-\x1f\x7f]/.test(path) || path.startsWith('/')) throw workspaceError('Unsafe path in directory snapshot', 'STORAGE');
  const parts = path.split('/');
  if (parts.some(part => !part || part === '.' || part === '..' || EXCLUDED.has(part))) throw workspaceError('Unsafe or excluded path in directory snapshot', 'STORAGE');
  return parts;
}

async function rejectGitWorkspace(root) {
  for (let cursor = root; ; cursor = dirname(cursor)) {
    if (await existing(join(cursor, '.git'))) throw workspaceError('Git workspaces must use the clean pinned Git backend; directory snapshot refused');
    const parent = dirname(cursor);
    if (parent === cursor) break;
  }
}

async function sourceBoundary(store, cwd, ownedRoot) {
  if (!store || typeof store.artifact !== 'function' || typeof store.artifactIdentity !== 'function') throw workspaceError('Directory snapshot requires the immutable run artifact store', 'STORAGE');
  const supplied = await bigintStat(cwd);
  if (supplied.isSymbolicLink() || !supplied.isDirectory()) throw workspaceError('Directory snapshot source must be a regular directory, not a symlink');
  const root = await realpath(cwd), rootInfo = await bigintStat(root);
  if (!rootInfo.isDirectory() || rootInfo.isSymbolicLink()) throw workspaceError('Directory snapshot source changed during acceptance');
  sameStat(supplied, rootInfo, 'Directory snapshot source alias changed during acceptance');
  if (!ownedRoot) {
    for (const component of root.split(sep)) if (EXCLUDED.has(component)) throw workspaceError('Excluded namespace cannot be a directory snapshot root');
    await rejectGitWorkspace(root);
  } else if (root !== ownedRoot) throw workspaceError('Product snapshot escaped its owned workspace', 'PERMISSION');
  const storeRoot = await realpath(store.root);
  if (!ownedRoot && inside(storeRoot, root)) throw workspaceError('Private run storage cannot be captured as a workspace');
  const privateRelative = inside(root, storeRoot) ? relative(root, storeRoot).split(sep).join('/') : null;
  return { root, rootInfo, privateRelative, source: cwd };
}
function excluded(path, privateRelative) {
  const name = path.slice(path.lastIndexOf('/') + 1);
  return EXCLUDED.has(name) || (privateRelative && (path === privateRelative || path.startsWith(`${privateRelative}/`)));
}
function addEntry(state, path) {
  state.entries++;
  state.pathBytes += Buffer.byteLength(path);
  if (state.entries > MAX_ENTRIES || state.pathBytes > MAX_PATH_TOTAL) throw workspaceError('Directory snapshot tree exceeds the explicit metadata budget', 'LIMIT');
}
async function assertSource(source, root, expected) {
  sameStat(expected, await bigintStat(source), 'Snapshot source alias or inode changed');
  if (await realpath(source) !== root) throw workspaceError('Snapshot source canonical path changed');
}

async function readFileStable(path, initial, signal, onChunk) {
  const handle = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
  const digest = createHash('sha256');
  try {
    const before = await handle.stat({ bigint: true });
    if (!before.isFile()) throw workspaceError(`Unsupported non-regular snapshot input: ${path}`);
    sameStat(initial, before, `Snapshot path or inode changed while opening: ${path}`);
    const bytes = safeSize(before, path), buffer = Buffer.allocUnsafe(DEFAULT_PAGE_BYTES);
    let offset = 0;
    while (offset < bytes) {
      aborted(signal);
      const want = Math.min(buffer.length, bytes - offset);
      const { bytesRead } = await handle.read(buffer, 0, want, offset);
      if (!bytesRead) throw workspaceError(`Snapshot file shortened while reading: ${path}`);
      const chunk = buffer.subarray(0, bytesRead);
      digest.update(chunk); await onChunk?.(chunk); offset += bytesRead;
    }
    if ((await handle.read(buffer, 0, 1, offset)).bytesRead) throw workspaceError(`Snapshot file grew while reading: ${path}`);
    const after = await handle.stat({ bigint: true });
    sameStat(before, after, `Snapshot file changed while reading: ${path}`);
    const pathAfter = await bigintStat(path);
    sameStat(before, pathAfter, `Snapshot path or inode changed while reading: ${path}`);
    return { bytes, sha256: digest.digest('hex') };
  } finally { await handle.close(); }
}

async function walkSource(root, rootInfo, privateRelative, signal, visitor) {
  async function walk(path, rel, depth, initial) {
    aborted(signal);
    if (depth > MAX_DEPTH) throw workspaceError('Directory snapshot exceeds the explicit depth budget', 'LIMIT');
    const before = await bigintStat(path);
    if (!before.isDirectory() || before.isSymbolicLink()) throw workspaceError(`Snapshot directory changed or became unsafe: ${path}`);
    sameStat(initial, before, `Snapshot directory path or inode changed: ${path}`);
    const guard = await open(path, constants.O_RDONLY | (constants.O_DIRECTORY ?? 0) | (constants.O_NOFOLLOW ?? 0));
    try {
      const opened = await guard.stat({ bigint: true }); sameStat(before, opened, `Snapshot directory changed while opening: ${path}`);
      const directory = await opendir(path); sameStat(opened, await bigintStat(path), `Snapshot directory changed while opening: ${path}`);
      for await (const item of directory) {
        aborted(signal); sameStat(opened, await bigintStat(path), `Snapshot directory changed while traversing: ${path}`);
        const childRel = rel ? `${rel}/${item.name}` : item.name;
        if (excluded(childRel, privateRelative)) continue;
        pathParts(childRel);
        const childPath = join(path, item.name), info = await bigintStat(childPath), type = kind(info);
        if (type === 'symlink') throw workspaceError(`Symlinks are not supported in directory snapshots: ${childRel}`);
        if (type === 'directory') { await visitor.directory(childRel, info); await walk(childPath, childRel, depth + 1, info); }
        else if (type === 'file') await visitor.file(childRel, childPath, info);
        else throw workspaceError(`Unsupported special file in directory snapshot: ${childRel}`);
        sameStat(opened, await bigintStat(path), `Snapshot directory changed while traversing: ${path}`);
      }
      sameStat(opened, await guard.stat({ bigint: true }), `Snapshot directory changed while traversing: ${path}`);
      sameStat(opened, await bigintStat(path), `Snapshot directory changed while traversing: ${path}`);
    } finally { await guard.close(); }
  }
  await walk(root, '', 0, rootInfo);
  sameStat(rootInfo, await bigintStat(root), 'Snapshot root changed while traversing');
}

/** Capture a non-Git directory into one sealed, binary-safe manifest artifact. */
export async function captureDirectory(store, owner, cwd, signal) { return captureTree(store, owner, cwd, signal); }

// Runtime-owned task only; callers cannot nominate arbitrary private paths or
// use this capability to capture another task's workspace or the artifact store.
export async function captureOwnedProductDirectory(store, task, signal) {
  identifier(task.id);
  const imported = task.productInput?.type === 'product-directory-v1' && task.worktree?.source === 'pinned result product snapshot';
  if (task.workItem || task.role?.workspace !== 'worktree' && !imported || !task.worktree?.path || task.cwd !== task.worktree.path || task.worktreeIntent || task.cancelRequested || task.authorityRevoked) throw workspaceError('Product snapshot requires a current owned worktree', 'PERMISSION');
  const parent = join(await realpath(store.root), 'worktrees'), info = await bigintStat(parent);
  if (!info.isDirectory() || info.isSymbolicLink() || await realpath(parent) !== parent) throw workspaceError('Unsafe product workspace parent', 'PERMISSION');
  const expected = join(parent, task.id);
  return captureTree(store, task.id, task.cwd, signal, expected);
}

async function captureTree(store, owner, cwd, signal, ownedRoot) {
  aborted(signal);
  const { root, rootInfo, privateRelative, source } = await sourceBoundary(store, cwd, ownedRoot);
  const state = { entries: 0, files: 0, payloadBytes: 0, pathBytes: 0 };
  // Product capture can overlap across active workers. Keep its first-pass
  // identity index on disk instead of one whole-tree Map per active task.
  const metadata = store.metadata, captureId = ownedRoot ? `capture-${randomUUID()}` : null;
  if (captureId && !metadata) throw workspaceError('Product capture requires durable metadata', 'STORAGE');
  const accepted = captureId ? {
    size: 0,
    set(path, entry) { metadata.put('result_snapshot_entries', store.scope, { id: createHash('sha256').update(`${captureId}\0${path}`).digest('hex'), state: 'capturing', snapshotId: captureId, path, entry }, { ifAbsent: true }); this.size++; },
    get(path) { return metadata.get('result_snapshot_entries', store.scope, createHash('sha256').update(`${captureId}\0${path}`).digest('hex'))?.entry; },
  } : new Map();
  let artifact;
  try {
    artifact = await store.artifact(owner, 'directory-snapshot-manifest');
    await artifact.write(jsonLine({ record: 'snapshot', format: FORMAT, version: 1, mode: mode(rootInfo) }));
    await assertSource(source, root, rootInfo);
    await walkSource(root, rootInfo, privateRelative, signal, {
      directory: async (path, info) => {
        addEntry(state, path);
        accepted.set(path, { type: 'directory', identity: identity(info), mode: mode(info) });
        await artifact.write(jsonLine({ record: 'entry', type: 'directory', path, mode: mode(info) }));
      },
      file: async (path, sourcePath, info) => {
        if (ownedRoot && info.nlink !== 1n) throw workspaceError('Hardlinks are not supported in product snapshots');
        addEntry(state, path); state.files++;
        const bytes = safeSize(info, sourcePath);
        if (!Number.isSafeInteger(state.payloadBytes + bytes)) throw workspaceError('Directory snapshot payload byte count overflow', 'LIMIT');
        state.payloadBytes += bytes;
        await artifact.write(jsonLine({ record: 'entry', type: 'file', path, mode: mode(info), bytes }));
        const captured = await readFileStable(sourcePath, info, signal, chunk => artifact.write(chunk));
        accepted.set(path, { type: 'file', identity: identity(info), mode: mode(info), ...captured });
        await artifact.write(jsonLine({ record: 'hash', sha256: captured.sha256 }));
      },
    });
    await assertSource(source, root, rootInfo);
    const seen = captureId ? null : new Set(); let seenCount = 0;
    await walkSource(root, rootInfo, privateRelative, signal, {
      directory: async (path, info) => {
        const expected = accepted.get(path);
        if (!expected || expected.type !== 'directory' || expected.identity !== identity(info) || expected.mode !== mode(info)) throw workspaceError(`Snapshot directory changed before acceptance: ${path}`);
        seen?.add(path); seenCount++;
      },
      file: async (path, sourcePath, info) => {
        const expected = accepted.get(path);
        if (!expected || expected.type !== 'file' || expected.identity !== identity(info) || expected.mode !== mode(info)) throw workspaceError(`Snapshot file changed before acceptance: ${path}`);
        const verified = await readFileStable(sourcePath, info, signal);
        if (verified.bytes !== expected.bytes || verified.sha256 !== expected.sha256) throw workspaceError(`Snapshot file hash changed before acceptance: ${path}`);
        seen?.add(path); seenCount++;
      },
    });
    await assertSource(source, root, rootInfo);
    if ((seen?.size ?? seenCount) !== accepted.size) throw workspaceError('Snapshot paths changed before acceptance');
    await artifact.write(jsonLine({ record: 'end', entries: state.entries, files: state.files, payloadBytes: state.payloadBytes }));
    aborted(signal);
    const sealed = await artifact.finish(true);
    if (!sealed.complete || !/^[a-f0-9]{64}$/.test(sealed.sha256 ?? '')) throw workspaceError('Directory snapshot did not seal immutably', 'STORAGE');
    // Only a fully verified sealed archive makes this temporary index redundant.
    // Failed/crashed capture rows remain inspectable; no orphan body is deleted.
    if (captureId) metadata.database.prepare("DELETE FROM result_snapshot_entries WHERE run_id=? AND owner_id=? AND json_extract(data_json,'$.snapshotId')=? AND state='capturing'").run(store.scope.runId, store.scope.ownerId, captureId);
    return { format: FORMAT, artifactId: sealed.id, bytes: sealed.bytes, sha256: sealed.sha256, complete: true };
  } catch (error) {
    if (artifact && !artifact.closed) await artifact.finish(false, error?.message ?? 'directory snapshot failed').catch(() => {});
    throw error;
  }
}

class ArtifactReader {
  constructor(store, ref, signal) {
    if (!store?.blobs || typeof store.blobs.readRange !== 'function' || !store.scope) throw workspaceError('Raw immutable artifact ranges are unavailable', 'STORAGE');
    this.store = store; this.ref = ref; this.signal = signal; this.buffer = Buffer.alloc(0); this.fetched = 0; this.consumed = 0;
    this.pageBytes = Math.max(1, Math.min(store.limits?.readBytes ?? DEFAULT_PAGE_BYTES, store.blobs.maxRangeBytes ?? DEFAULT_PAGE_BYTES));
    this.digest = createHash('sha256');
  }
  async fill() {
    aborted(this.signal);
    if (this.buffer.length || this.fetched >= this.ref.bytes) return;
    const length = Math.min(this.pageBytes, this.ref.bytes - this.fetched);
    const page = await this.store.blobs.readRange(this.store.scope, this.ref.artifactId, { offset: this.fetched, length });
    if (!Buffer.isBuffer(page.bytes) || page.offset !== this.fetched || page.bytesRead !== page.bytes.length || page.nextOffset !== this.fetched + page.bytes.length || page.size !== this.ref.bytes || page.sha256 !== this.ref.sha256 || page.bytes.length < 1) throw workspaceError('Directory snapshot artifact range identity mismatch', 'STORAGE');
    this.buffer = page.bytes; this.fetched = page.nextOffset;
  }
  take(length) {
    const chunk = this.buffer.subarray(0, length);
    this.buffer = this.buffer.subarray(length); this.consumed += chunk.length; this.digest.update(chunk);
    return chunk;
  }
  async line() {
    const chunks = []; let bytes = 0;
    while (true) {
      await this.fill();
      if (!this.buffer.length) throw workspaceError('Truncated directory snapshot record', 'STORAGE');
      const newline = this.buffer.indexOf(10);
      const count = newline < 0 ? this.buffer.length : newline;
      if (bytes + count > MAX_RECORD_BYTES) throw workspaceError('Directory snapshot record exceeds its byte budget', 'STORAGE');
      if (count) { chunks.push(this.take(count)); bytes += count; }
      if (newline >= 0) { this.take(1); return Buffer.concat(chunks, bytes); }
    }
  }
  async payload(bytes, onChunk) {
    let remaining = bytes;
    while (remaining) {
      await this.fill();
      if (!this.buffer.length) throw workspaceError('Truncated directory snapshot payload', 'STORAGE');
      const chunk = this.take(Math.min(remaining, this.buffer.length));
      await onChunk?.(chunk); remaining -= chunk.length;
    }
  }
  finish() {
    if (this.consumed !== this.ref.bytes || this.buffer.length || this.fetched !== this.ref.bytes || this.digest.digest('hex') !== this.ref.sha256) throw workspaceError('Directory snapshot artifact hash or length mismatch', 'STORAGE');
  }
}

async function writeAll(handle, chunk) {
  let offset = 0;
  while (offset < chunk.length) {
    const { bytesWritten } = await handle.write(chunk, offset, chunk.length - offset, null);
    if (!bytesWritten) throw workspaceError('Zero-byte directory materialization write', 'STORAGE');
    offset += bytesWritten;
  }
}
async function syncDirectory(path, finalMode) {
  const handle = await open(path, constants.O_RDONLY | (constants.O_DIRECTORY ?? 0) | (constants.O_NOFOLLOW ?? 0));
  try {
    if (finalMode !== undefined) await handle.chmod(finalMode);
    try { await handle.sync(); } catch (error) { if (!['EINVAL', 'ENOTSUP', 'EBADF'].includes(error.code)) throw error; }
  } finally { await handle.close(); }
}

async function parseArchive(store, ref, signal, destination = null, onEntry) {
  const reader = new ArtifactReader(store, ref, signal), header = record(await reader.line(), 'Invalid directory snapshot header');
  exactKeys(header, ['record', 'format', 'version', 'mode'], 'Invalid directory snapshot header');
  if (header.record !== 'snapshot' || header.format !== FORMAT || header.version !== 1) throw workspaceError('Unsupported directory snapshot format', 'STORAGE');
  validInteger(header.mode, 0, 0o777, 'Invalid directory snapshot root mode');
  const paths = new Map(), directories = []; let entries = 0, files = 0, payloadBytes = 0, pathBytes = 0;
  while (true) {
    aborted(signal);
    const entry = record(await reader.line(), 'Invalid directory snapshot entry');
    if (entry.record === 'end') {
      exactKeys(entry, ['record', 'entries', 'files', 'payloadBytes'], 'Invalid directory snapshot footer');
      if (entry.entries !== entries || entry.files !== files || entry.payloadBytes !== payloadBytes) throw workspaceError('Directory snapshot footer count mismatch', 'STORAGE');
      break;
    }
    if (entry.type === 'directory') exactKeys(entry, ['record', 'type', 'path', 'mode'], 'Invalid directory snapshot directory entry');
    else if (entry.type === 'file') exactKeys(entry, ['record', 'type', 'path', 'mode', 'bytes'], 'Invalid directory snapshot file entry');
    else throw workspaceError('Unsupported directory snapshot entry type', 'STORAGE');
    if (entry.record !== 'entry') throw workspaceError('Invalid directory snapshot entry', 'STORAGE');
    const parts = pathParts(entry.path);
    validInteger(entry.mode, 0, 0o777, 'Invalid directory snapshot mode');
    entries++; pathBytes += Buffer.byteLength(entry.path);
    if (entries > MAX_ENTRIES || pathBytes > MAX_PATH_TOTAL) throw workspaceError('Directory snapshot tree exceeds the explicit metadata budget', 'LIMIT');
    if (paths.has(entry.path)) throw workspaceError('Duplicate directory snapshot path', 'STORAGE');
    const parent = parts.length === 1 ? null : parts.slice(0, -1).join('/');
    if (parent && paths.get(parent) !== 'directory') throw workspaceError('Directory snapshot parent is missing or not a directory', 'STORAGE');
    paths.set(entry.path, entry.type);
    const outputPath = destination && join(destination, ...parts);
    if (entry.type === 'directory') {
      directories.push({ path: outputPath, mode: entry.mode });
      if (outputPath) await mkdir(outputPath, { mode: 0o700 });
      await onEntry?.({ path: entry.path, type: entry.type, mode: entry.mode });
      continue;
    }
    const bytes = validInteger(entry.bytes, 0, ref.bytes, 'Invalid directory snapshot file size');
    if (!Number.isSafeInteger(payloadBytes + bytes)) throw workspaceError('Directory snapshot payload byte count overflow', 'STORAGE');
    files++; payloadBytes += bytes;
    const digest = createHash('sha256'); let handle;
    try {
      if (outputPath) handle = await open(outputPath, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | (constants.O_NOFOLLOW ?? 0), 0o600);
      await reader.payload(bytes, async chunk => { digest.update(chunk); if (handle) { await store.checkDisk(true); await writeAll(handle, chunk); } });
      const hashRecord = record(await reader.line(), 'Invalid directory snapshot file hash');
      exactKeys(hashRecord, ['record', 'sha256'], 'Invalid directory snapshot file hash');
      const actual = digest.digest('hex');
      if (hashRecord.record !== 'hash' || !/^[a-f0-9]{64}$/.test(hashRecord.sha256 ?? '') || hashRecord.sha256 !== actual) throw workspaceError('Directory snapshot file hash mismatch', 'STORAGE');
      if (handle) { await handle.chmod(entry.mode); await handle.sync(); }
      await onEntry?.({ path: entry.path, type: entry.type, mode: entry.mode, bytes, sha256: actual });
    } finally { await handle?.close(); }
  }
  reader.finish();
  if (destination) {
    for (let index = directories.length - 1; index >= 0; index--) await syncDirectory(directories[index].path, directories[index].mode);
    await syncDirectory(destination, header.mode);
  }
  return { payloadBytes, rootMode: header.mode };
}

function validateRef(ref) {
  exactKeys(ref, ['format', 'artifactId', 'bytes', 'sha256', 'complete'], 'Invalid directory snapshot reference');
  if (ref.format !== FORMAT || ref.complete !== true || typeof ref.artifactId !== 'string' || !/^[a-f0-9]{64}$/.test(ref.sha256 ?? '')) throw workspaceError('Invalid directory snapshot reference', 'STORAGE');
  validInteger(ref.bytes, 1, Number.MAX_SAFE_INTEGER, 'Invalid directory snapshot byte length');
}

export async function inspectDirectorySnapshot(store, owner, ref, onEntry, signal) {
  aborted(signal); validateRef(ref);
  const actual = await store.artifactIdentity(ref.artifactId);
  if (actual.owner !== owner || !actual.complete || actual.bytes !== ref.bytes || actual.sha256 !== ref.sha256) throw workspaceError('Directory snapshot ownership or identity mismatch', 'STORAGE');
  return parseArchive(store, ref, signal, null, onEntry);
}

/** Materialize a sealed snapshot without consulting its mutable source directory. */
export async function materializeDirectory(store, owner, ref, destination, signal) {
  aborted(signal); validateRef(ref);
  if (!store || typeof store.artifactIdentity !== 'function' || typeof store.checkDisk !== 'function' || typeof store.reserve !== 'function') throw workspaceError('Directory snapshot artifact store unavailable', 'STORAGE');
  const immutable = await store.artifactIdentity(ref.artifactId);
  if (immutable.owner !== owner || immutable.bytes !== ref.bytes || immutable.sha256 !== ref.sha256 || !immutable.complete) throw workspaceError('Directory snapshot ownership or identity mismatch', 'STORAGE');
  if (typeof destination !== 'string' || !destination || basename(destination) === '.' || basename(destination) === '..') throw workspaceError('Invalid directory snapshot destination');
  const parent = await realpath(dirname(resolve(destination))), target = join(parent, basename(resolve(destination)));
  const parentInfo = await bigintStat(parent);
  if (!parentInfo.isDirectory() || parentInfo.isSymbolicLink()) throw workspaceError('Directory snapshot destination parent is unsafe');
  if (await existing(target)) throw workspaceError('Directory snapshot destination already exists');
  const { payloadBytes } = await parseArchive(store, ref, signal); // Validate before reservation or destination writes.
  await store.checkDisk(true); store.reserve(payloadBytes);
  aborted(signal);
  let created = false;
  try {
    sameStat(parentInfo, await bigintStat(parent), 'Snapshot destination parent changed');
    if (await realpath(parent) !== parent) throw workspaceError('Snapshot destination parent redirected');
    await mkdir(target, { mode: 0o700 }); created = true;
    await parseArchive(store, ref, signal, target);
    await syncDirectory(parent);
    return target;
  } catch (error) {
    if (created) await rm(target, { recursive: true, force: true }).catch(() => {});
    throw error;
  }
}
