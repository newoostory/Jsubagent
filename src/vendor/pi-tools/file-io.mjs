// SPDX-License-Identifier: MIT
// Copyright (c) 2026 newoostory
import { constants } from 'node:fs';
import { access, open, realpath, rename, unlink } from 'node:fs/promises';
import { resolve, dirname, join } from 'node:path';
import { homedir } from 'node:os';
import { randomUUID } from 'node:crypto';
import { failure, sha256 } from './hashline.mjs';

export const MAX_FILE_BYTES = 32 * 1024 * 1024;
export const MAX_EDIT_BYTES = 2 * 1024 * 1024;
export const MAX_OUTPUT_BYTES = 50 * 1024;
export async function localPath(cwd, raw) {
  if (typeof raw !== 'string' || !raw.trim() || Buffer.byteLength(raw) > 4096 || raw.includes('\0')) throw failure('Invalid path.');
  let path = raw.replace(/^@/, '');
  if (path === '~' || path.startsWith('~/')) path = homedir() + path.slice(1);
  return realpath(resolve(cwd, path));
}
const sameStat = (a, b) => a.dev === b.dev && a.ino === b.ino && a.size === b.size && a.mtimeNs === b.mtimeNs && a.ctimeNs === b.ctimeNs;
export async function readFileBounded(path, maxBytes = MAX_FILE_BYTES, signal) {
  signal?.throwIfAborted();
  const file = await open(path, constants.O_RDONLY | constants.O_NONBLOCK | (constants.O_NOFOLLOW ?? 0));
  try {
    signal?.throwIfAborted();
    const before = await file.stat({ bigint: true });
    if (!before.isFile()) throw failure('Only regular files are supported.');
    if (before.size > BigInt(maxBytes)) throw failure(`File exceeds ${maxBytes} bytes; use raw byte reads or a bounded shell command.`, 'LIMIT');
    const parts = []; let total = 0;
    while (true) {
      signal?.throwIfAborted();
      const buffer = Buffer.allocUnsafe(Math.min(64 * 1024, maxBytes + 1 - total));
      const { bytesRead } = await file.read(buffer, 0, buffer.length, total);
      signal?.throwIfAborted();
      if (!bytesRead) break;
      total += bytesRead;
      if (total > maxBytes) throw failure('File grew beyond its byte limit.', 'LIMIT');
      parts.push(buffer.subarray(0, bytesRead));
    }
    if (!sameStat(before, await file.stat({ bigint: true }))) throw failure('File changed during read; retry with a fresh read.', 'CONFLICT');
    return Buffer.concat(parts, total);
  } finally { await file.close(); }
}

// Caller holds the shared Pi file mutation queue over the entire read/modify/write.
export async function replaceSnapshot(path, original, content, maxBytes, signal) {
  signal?.throwIfAborted();
  if (Buffer.byteLength(content) > maxBytes) throw failure('Edited file exceeds byte limit.', 'LIMIT');
  await access(path, constants.R_OK | constants.W_OK);
  signal?.throwIfAborted();
  const target = await open(path, constants.O_RDONLY | constants.O_NONBLOCK | (constants.O_NOFOLLOW ?? 0));
  let mode;
  try { const stat = await target.stat(); if (!stat.isFile()) throw failure('Only regular files can be edited.'); mode = stat.mode & 0o777; }
  finally { await target.close(); }
  const temp = join(dirname(path), `.pi-hashline-${randomUUID()}.tmp`);
  try {
    const file = await open(temp, 'wx', mode);
    try { await file.writeFile(content, 'utf8'); await file.chmod(mode); await file.sync(); }
    finally { await file.close(); }
    signal?.throwIfAborted();
    const current = await readFileBounded(path, maxBytes, signal);
    if (sha256(current) !== sha256(original)) throw failure('File changed while staging edit; read again.', 'CONFLICT');
    signal?.throwIfAborted();
    await rename(temp, path);
  } finally { await unlink(temp).catch(error => { if (error.code !== 'ENOENT') throw error; }); }
}
