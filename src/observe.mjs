import { constants } from 'node:fs';
import { lstat, open, realpath } from 'node:fs/promises';
import { join } from 'node:path';
import { readJson } from './storage.mjs';
import { LIMITS, identifier, fail, short } from './limits.mjs';
import { taskName } from './task-name.mjs';
import { listTaskRecords } from './state/task-cache.mjs';

export const TAIL_BYTES = 8192;
const TASK_CAP = 128;
async function directory(root, name) {
  const base = await realpath(root), path = join(base, name), info = await lstat(path);
  if (!info.isDirectory() || info.isSymbolicLink() || await realpath(path) !== path) throw fail('Observer refuses symlinked record directories');
  return path;
}
export async function transcriptTail(root, task) {
  if (!root || !task?.transcript) return undefined;
  const dir = await directory(root, 'artifacts'), id = identifier(task.transcript);
  const metadata = await readJson(join(dir, `${id}.json`), 8192);
  if (metadata.owner !== task.id || metadata.id !== id) throw fail('Transcript owner mismatch');
  if (metadata.label !== 'sdk-transcript-jsonl' || !Number.isSafeInteger(metadata.bytes) || metadata.bytes < 0 || (metadata.complete && !/^[a-f0-9]{64}$/.test(metadata.sha256 ?? ''))) throw fail('Invalid transcript identity', 'STORAGE');
  const file = await open(join(dir, `${id}.bin`), constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
  try {
    const info = await file.stat(); if (!info.isFile() || info.size !== metadata.bytes) throw fail('Transcript size changed', 'STORAGE');
    const offset = Math.max(0, info.size - TAIL_BYTES), buffer = Buffer.alloc(TAIL_BYTES);
    const { bytesRead } = await file.read(buffer, 0, buffer.length, offset);
    let start = 0; while (start < bytesRead && (buffer[start] & 0xc0) === 0x80) start++;
    return { text: buffer.subarray(start, bytesRead).toString('utf8'), byteOffset: offset + start, fileBytes: info.size, truncated: offset > 0, artifactId: id, complete: metadata.complete, ...(metadata.sha256 ? { sha256: metadata.sha256 } : {}) };
  } finally { await file.close(); }
}
function savedSummary(t) {
  identifier(t.id);
  return { id: t.id, name: taskName(t), role: t.role?.name, profile: t.profile?.name, provider: t.profile?.provider, model: t.profile?.model,
    state: t.state, dependsOn: t.dependsOn ?? [], blockedBy: t.blockedBy ?? [], dependencyState: t.state === 'blocked' ? 'blocked' : t.dependsOn?.length ? 'saved' : 'ready', task: short(t.task ?? '', 256), cwd: t.cwd, result: short(t.result ?? '', 2048), error: short(t.error ?? '', 2048),
    bundleRef: t.bundleRef, executionReadiness: t.executionReadiness, dataReadiness: t.dataReadiness, holdReasons: t.holdReasons,
    ...(t.resumeCapsule ? { resume: { nextEligibleAt: t.resumeCapsule.nextEligibleAt, providerFault: { source: t.resumeCapsule.providerFault?.source } } } : {}),
    turns: t.turns, usage: t.usage, updatedAt: t.updatedAt, transcript: t.transcript };
}
async function savedTasks(root) {
  if (!root) return [];
  try { return (await listTaskRecords(root)).items.map(savedSummary); }
  catch (error) { if (error.code === 'ENOENT') return []; throw error; }
}
// Deliberately no ensure/start, RunStore.init, mailbox reads/acks, or model imports.
export async function observe({ client, root }, selectedId) {
  const online = Boolean(client?.info && !client.closed && !client.exited);
  const status = online ? await client.request('status', {}, 3000) : { tasks: await savedTasks(root) };
  if (!Array.isArray(status.tasks) || status.tasks.length > TASK_CAP) throw fail('Observer status task limit exceeded');
  const tasks = [...status.tasks].sort((a, b) => a.id.localeCompare(b.id));
  const selected = tasks.find(t => t.id === selectedId) ?? tasks[0];
  let tail, tailError;
  try { tail = await transcriptTail(root, selected); } catch (e) { tailError = short(e.message, 256); }
  return { ...status, tasks, selectedId: selected?.id, tail, tailError, online, observedAt: Date.now(),
    note: online ? 'Live host · read only' : 'Offline · saved states only; liveness unknown' };
}
