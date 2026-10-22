import { constants } from 'node:fs';
import { lstat, open, realpath, stat } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fail, identifier } from './limits.mjs';

const MAX_BYTES = 64 * 1024;
const sha = data => createHash('sha256').update(data).digest('hex');
const unsafe = message => fail(message, 'UNSUPPORTED');
const stale = message => fail(message, 'CONFLICT');
const identity = stat => [stat.dev, stat.ino, stat.mode, stat.nlink, stat.size, stat.mtimeNs, stat.ctimeNs].join(':');
const within = (parent, child) => { const r = relative(parent, child); return r === '' || r !== '..' && !r.startsWith(`..${sep}`) && !isAbsolute(r); };

export function fileSnapshotGrant(value) {
  const fields = ['kind', 'missionId', 'workItemId', 'cwd', 'name', 'bytes', 'sha256'];
  if (!value || Object.getPrototypeOf(value) !== Object.prototype || Object.keys(value).sort().join() !== fields.sort().join() || value.kind !== 'file-input-snapshot') throw fail('Invalid trusted file snapshot grant', 'CAPABILITY');
  const { cwd, name, bytes, sha256 } = value;
  if (typeof cwd !== 'string' || !cwd || cwd.length > 4096 || !isAbsolute(cwd) || resolve(cwd) !== cwd || cwd.includes('\0')) throw fail('Invalid canonical file snapshot cwd', 'CAPABILITY');
  // An ASCII direct basename excludes traversal, platform separators and dotfiles.
  if (typeof name !== 'string' || name.length > 128 || !/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(name) || name.includes('..') || /^(?:\.env(?:\..*)?|\.npmrc|\.netrc|credentials?(?:\..*)?|secrets?(?:\..*)?|id_(?:rsa|dsa|ecdsa|ed25519)(?:\.pub)?|.*\.(?:pem|key|p12|pfx|kdbx))$/i.test(name)) throw fail('Unsafe or secret-like file snapshot basename', 'CAPABILITY');
  if (!Number.isSafeInteger(bytes) || bytes < 0 || bytes > MAX_BYTES || typeof sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(sha256)) throw fail('Invalid file snapshot size or SHA-256', 'CAPABILITY');
  return { kind: value.kind, missionId: identifier(value.missionId), workItemId: identifier(value.workItemId), cwd, name, bytes, sha256 };
}

function checkContent(data) {
  let text;
  try { text = new TextDecoder('utf-8', { fatal: true }).decode(data); }
  catch { throw unsafe('File snapshot must contain valid UTF-8'); }
  if (/[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]/.test(text) || text.charCodeAt(0) === 0xfeff) throw unsafe('File snapshot must contain plain UTF-8 text');
  if (text.includes('version https://git-lfs.github.com/spec/v1') || /-----BEGIN (?:[\w ]*PRIVATE KEY|OPENSSH PRIVATE KEY)-----|(?:^|\n)\s*(?:password|secret|api[_-]?key|access[_-]?token)\s*[:=]/i.test(text)) throw unsafe('Secret-like file snapshot content');
}

async function readStable(path, grant, first) {
  if (constants.O_NOFOLLOW === undefined) throw unsafe('No-follow file opening is unavailable');
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW | (constants.O_NONBLOCK ?? 0));
  try {
    const before = await file.stat({ bigint: true });
    if (!before.isFile() || before.nlink !== 1n || before.size !== BigInt(grant.bytes) || identity(before) !== identity(first)) throw stale('File snapshot identity or size changed');
    const data = Buffer.alloc(grant.bytes); let offset = 0;
    while (offset < data.length) {
      const { bytesRead } = await file.read(data, offset, data.length - offset, offset);
      if (!bytesRead) throw stale('File snapshot changed while reading');
      offset += bytesRead;
    }
    const after = await file.stat({ bigint: true });
    const last = await lstat(path, { bigint: true });
    if (identity(first) !== identity(after) || identity(first) !== identity(last) || !last.isFile()) throw stale('File snapshot changed while reading');
    checkContent(data);
    if (sha(data) !== grant.sha256) throw stale('File snapshot SHA-256 does not match grant');
    return data;
  } finally { await file.close(); }
}

// Called only after the host has fsynced an exclusive, non-retryable intent.
export async function captureFileSnapshot(rawGrant, store) {
  const grant = fileSnapshotGrant(rawGrant);
  if (!store || typeof store.artifact !== 'function' || typeof store.root !== 'string') throw fail('Private RunStore required', 'CAPABILITY');
  if (await realpath(grant.cwd) !== grant.cwd) throw unsafe('File snapshot cwd is not canonical');
  const storeRoot = await realpath(store.root);
  if (within(grant.cwd, storeRoot) || within(storeRoot, grant.cwd)) throw unsafe('Private artifact store must be outside source root');
  const privateInfo = await stat(storeRoot);
  if (!privateInfo.isDirectory() || process.getuid && (privateInfo.uid !== process.getuid() || (privateInfo.mode & 0o077) !== 0)) throw unsafe('Artifact store must be a private host-owned directory');
  const path = join(grant.cwd, grant.name);
  const first = await lstat(path, { bigint: true });
  if (!first.isFile() || first.isSymbolicLink() || first.nlink !== 1n || first.size !== BigInt(grant.bytes)) throw unsafe('File snapshot is symlink, hardlink, special or wrong size');
  const data = await readStable(path, grant, first);
  let artifact;
  try {
    artifact = await store.artifact(grant.workItemId, 'file-input-snapshot');
    await artifact.write(data);
    if (!(await readStable(path, grant, first)).equals(data)) throw stale('File snapshot content changed during capture');
    const info = await artifact.finish(true);
    if (!(await readStable(path, grant, first)).equals(data)) throw stale('File snapshot content changed after capture');
    return { artifactId: info.id, owner: info.owner, bytes: info.bytes, sha256: sha(data), name: grant.name };
  } catch (error) {
    if (artifact && !artifact.closed) await artifact.finish(false, 'File snapshot capture rejected').catch(() => {});
    throw error;
  }
}
