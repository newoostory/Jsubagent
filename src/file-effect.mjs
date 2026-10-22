import { constants } from 'node:fs';
import { link, lstat, open, realpath, unlink } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { isAbsolute, join, resolve } from 'node:path';
import { encodeJson, fail, hash, identifier, text } from './limits.mjs';
import { readBounded } from './storage.mjs';

// Only the trusted host's *global* configuration may supply this grant. Mission,
// model output, project configuration and public calls cannot create/alter it.
export function fileEffectGrant(value) {
  const fields = ['kind', 'missionId', 'workItemId', 'artifactId', 'owner', 'bytes', 'sha256', 'root', 'name'];
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).sort().join('\0') !== fields.sort().join('\0') || value.kind !== 'create-only-file') throw fail('Invalid trusted file-effect grant', 'CAPABILITY');
  const root = text(value.root, 'effect root', 4096);
  if (!isAbsolute(root) || resolve(root) !== root || root.includes('\0')) throw fail('Effect root must be an absolute normalized path', 'CAPABILITY');
  if (typeof value.name !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9._-]{0,119}$/.test(value.name) || value.name === '..') throw fail('Effect name must be one safe basename', 'CAPABILITY');
  if (!Number.isSafeInteger(value.bytes) || value.bytes < 0 || !/^[a-f0-9]{64}$/.test(value.sha256)) throw fail('Invalid effect input identity', 'CAPABILITY');
  return { kind: value.kind, missionId: identifier(value.missionId), workItemId: identifier(value.workItemId), artifactId: identifier(value.artifactId), owner: identifier(value.owner), bytes: value.bytes, sha256: value.sha256, root, name: value.name };
}

export async function fileEffectPlan(raw, item) {
  if (!raw) throw fail('No trusted host file-effect grant', 'UNSUPPORTED');
  const grant = fileEffectGrant(raw), ref = item.inputRefs[0];
  if (item.intendedUse !== 'publish' || item.execution !== 'artifact-sha256' || item.inputRefs.length !== 1 || item.missionId !== grant.missionId || item.id !== grant.workItemId || ref.artifactId !== grant.artifactId || ref.owner !== grant.owner || ref.bytes !== grant.bytes || ref.sha256 !== grant.sha256) throw fail('Work item does not match the trusted file-effect grant', 'UNSUPPORTED');
  await assertEffectRoot(grant.root);
  return { ...grant, grantHash: hash(encodeJson(grant)), path: join(grant.root, grant.name) };
}

export async function assertEffectRoot(root) {
  const info = await lstat(root);
  if (!info.isDirectory() || info.isSymbolicLink() || (info.mode & 0o077) !== 0 || (process.getuid && info.uid !== process.getuid()) || await realpath(root) !== root) throw fail('Effect root must remain a canonical host-owned private directory', 'PERMISSION');
}

export async function verifyPublished(plan, maxBytes) {
  await assertEffectRoot(plan.root);
  const info = await lstat(plan.path);
  if (!info.isFile() || info.isSymbolicLink() || info.nlink !== 1 || info.size !== plan.bytes) throw fail('Published file identity changed', 'STORAGE');
  const bytes = await readBounded(plan.path, maxBytes);
  if (bytes.length !== plan.bytes || hash(bytes) !== plan.sha256) throw fail('Published file digest changed', 'STORAGE');
  return { bytes: info.size, sha256: plan.sha256, destination: plan.path };
}

export async function assertVacant(plan) {
  await assertEffectRoot(plan.root);
  try { await lstat(plan.path); } catch (error) { if (error.code === 'ENOENT') return; throw error; }
  throw fail('Create-only destination already exists', 'CONFLICT');
}

// This is an observation, not an effect receipt: a matching file might have
// been created by someone else after the prepared journal was committed.
export async function observePublished(plan, maxBytes) {
  try { await verifyPublished(plan, maxBytes); return 'matching_unconfirmed'; }
  catch (error) { if (error.code === 'ENOENT') return 'absent_unconfirmed'; return 'different_or_unavailable'; }
}

export function effectJournal(plan, reportArtifactId) {
  return { id: plan.workItemId, missionId: plan.missionId, reportArtifactId: identifier(reportArtifactId), grantHash: plan.grantHash, artifactId: plan.artifactId, bytes: plan.bytes, sha256: plan.sha256, root: plan.root, name: plan.name, state: 'prepared' };
}

export function assertEffectJournal(value, plan, task) {
  const fields = ['id', 'missionId', 'reportArtifactId', 'grantHash', 'artifactId', 'bytes', 'sha256', 'root', 'name', 'state'];
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).sort().join('\0') !== fields.sort().join('\0') || !['prepared', 'confirmed'].includes(value.state)) throw fail('Incompatible effect journal; inspect before recovery', 'STORAGE');
  const expected = effectJournal(plan, task.acceptedReport);
  if (Object.entries(expected).some(([key, entry]) => key !== 'state' && value[key] !== entry) || task.currentReport !== task.acceptedReport || task.acceptedUse !== 'publish' || task.state !== 'idle') throw fail('Effect journal/accepted report/grant changed', 'STORAGE');
  return value;
}

async function syncRoot(root) {
  const dir = await open(root, 'r');
  try { await dir.sync(); } finally { await dir.close(); }
}

// Called ONLY after a durable prepared journal and checked acceptance. `link`
// publishes a fully synced regular temporary file atomically and fails if the
// destination (even a dangling symlink) exists; rename would replace it.
export async function createPublishedFile(plan, store) {
  await assertEffectRoot(plan.root);
  const content = await readBounded(join(store.root, 'artifacts', `${plan.artifactId}.bin`), store.limits.responseBytes);
  if (content.length !== plan.bytes || hash(content) !== plan.sha256) throw fail('Pinned effect source changed', 'STORAGE');
  const temp = join(plan.root, `.pi-agent-effect-${randomUUID()}.tmp`);
  let file;
  try {
    file = await open(temp, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | (constants.O_NOFOLLOW ?? 0), 0o600);
    await file.writeFile(content); await file.sync(); await file.close(); file = undefined;
    await assertEffectRoot(plan.root);
    await link(temp, plan.path);
    await syncRoot(plan.root);
    // The temporary staging name is still a second hardlink here. Remove it
    // before checking the published file's exclusive one-link identity.
  } finally {
    await file?.close();
    await unlink(temp).catch(error => { if (error.code !== 'ENOENT') throw error; });
  }
  await syncRoot(plan.root);
  return verifyPublished(plan, store.limits.responseBytes);
}
