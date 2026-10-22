import { constants } from 'node:fs';
import { lstat, open, realpath } from 'node:fs/promises';
import { isAbsolute, join, relative, sep } from 'node:path';
import { createHash } from 'node:crypto';
import { fail } from './limits.mjs';

const stamp = stat => [stat.dev, stat.ino, stat.size, stat.mode, stat.mtimeNs, stat.ctimeNs].map(String).join(':');
export async function importResultSource(store, task, source, signal) {
  if (!task.role.tools.some(name => ['read', 'bash'].includes(name))) throw fail('Result path import requires an existing workspace read capability', 'CAPABILITY');
  const path = source.path;
  if (typeof path !== 'string' || !path || Buffer.byteLength(path) > 4096 || isAbsolute(path) || /[\\\x00-\x1f\x7f]/.test(path) || path.split('/').some(part => !part || ['.', '..', '.pi', '.git'].includes(part)) || !/^[a-f0-9]{64}$/.test(source.expectedSha256 ?? '')) throw fail('Unsafe result source path/hash');
  signal?.throwIfAborted();
  const root = await realpath(task.cwd), components = path.split('/'), guards = [];
  let candidate = root;
  for (const [n, component] of components.entries()) {
    candidate = join(candidate, component); const info = await lstat(candidate, { bigint: true });
    if (info.isSymbolicLink() || (n < components.length - 1 ? !info.isDirectory() : !info.isFile())) throw fail('Result source refuses symlinks and non-regular files', 'PERMISSION');
    guards.push({ path: candidate, identity: stamp(info) });
  }
  const privateRoot = await realpath(store.root), privateRelative = relative(privateRoot, candidate);
  if (privateRelative === '' || !privateRelative.startsWith(`..${sep}`) && privateRelative !== '..' && !isAbsolute(privateRelative)) throw fail('Private run artifacts cannot be imported as new source ownership', 'PERMISSION');
  const file = await open(candidate, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0)); let artifact;
  try {
    const before = await file.stat({ bigint: true });
    if (!before.isFile() || stamp(before) !== guards.at(-1).identity || before.nlink !== 1n) throw fail('Result source identity or link count changed', 'STORAGE');
    const bytes = Number(before.size); if (!Number.isSafeInteger(bytes) || bytes < 0) throw fail('Invalid result source size', 'LIMIT');
    artifact = await store.artifact(task.id, 'result-imported-file');
    const buffer = Buffer.allocUnsafe(65536), digest = createHash('sha256'); let offset = 0;
    while (offset < bytes) {
      signal?.throwIfAborted(); const { bytesRead } = await file.read(buffer, 0, Math.min(buffer.length, bytes - offset), offset);
      if (!bytesRead) throw fail('Result source shortened during import', 'STORAGE');
      const chunk = buffer.subarray(0, bytesRead); digest.update(chunk); await artifact.write(chunk); offset += bytesRead;
    }
    if ((await file.read(buffer, 0, 1, offset)).bytesRead || stamp(await file.stat({ bigint: true })) !== stamp(before)) throw fail('Result source changed during import', 'STORAGE');
    for (const guard of guards) if (stamp(await lstat(guard.path, { bigint: true })) !== guard.identity) throw fail('Result source path changed during import', 'STORAGE');
    if (await realpath(task.cwd) !== root || await realpath(candidate) !== candidate) throw fail('Result source boundary changed during import', 'STORAGE');
    if (digest.digest('hex') !== source.expectedSha256) throw fail('Result source hash mismatch', 'CONFLICT');
    signal?.throwIfAborted(); const sealed = await artifact.finish(true);
    if (!sealed.complete || sealed.bytes !== bytes || sealed.sha256 !== source.expectedSha256) throw fail('Result source did not seal', 'STORAGE');
    return { artifactId: sealed.id, bytes: sealed.bytes, sha256: sealed.sha256, complete: true };
  } catch (error) { if (artifact && !artifact.closed) await artifact.finish(false, 'Result source import failed').catch(() => {}); throw error; }
  finally { await file.close(); }
}
