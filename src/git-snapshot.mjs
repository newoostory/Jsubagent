import { spawn } from 'node:child_process';
import { constants } from 'node:fs';
import { lstat, open, realpath } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { isAbsolute, join, relative, resolve, sep } from 'node:path';
import { encodeJson, fail, hash, identifier } from './limits.mjs';

const COMMAND_BYTES = 8 * 1024 * 1024;
const FILE_BYTES = 64 * 1024;
const SNAPSHOT_BYTES = 1024 * 1024;
const MAX_PATHS = 8;
const sha = bytes => createHash('sha256').update(bytes).digest('hex');
const blobOid = (bytes, hexLength) => createHash(hexLength === 64 ? 'sha256' : 'sha1').update(`blob ${bytes.length}\0`).update(bytes).digest('hex');
const unsafe = message => fail(message, 'UNSUPPORTED');
const stale = message => fail(message, 'CONFLICT');
const gitEnv = () => {
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith('GIT_')));
  return { ...env, GIT_CONFIG_NOSYSTEM: '1', GIT_CONFIG_GLOBAL: process.platform === 'win32' ? 'NUL' : '/dev/null', GIT_ATTR_NOSYSTEM: '1', GIT_NO_REPLACE_OBJECTS: '1', GIT_NO_LAZY_FETCH: '1', GIT_OPTIONAL_LOCKS: '0', GIT_TERMINAL_PROMPT: '0', LC_ALL: 'C' };
};

// Never spawn a shell, aliases, hooks, filters, or a Git command which writes an
// index/ref/source. Bounded stdout AND stderr are required: truncation is failure.
function git(root, args, { input, allowed = [0], max = COMMAND_BYTES } = {}) {
  return new Promise((resolvePromise, reject) => {
    const child = spawn('git', ['--no-optional-locks', '-c', 'core.fsmonitor=false', '-c', 'core.hooksPath=/dev/null', '-c', 'core.untrackedCache=false', '-C', root, ...args], { shell: false, detached: process.platform !== 'win32', stdio: ['pipe', 'pipe', 'pipe'], env: gitEnv() });
    const out = [], err = []; let output = 0, errors = 0, settled = false;
    let timer;
    function stop(error) {
      if (settled) return;
      settled = true; clearTimeout(timer);
      // Close inherited pipes as well as killing the Git process group. A child
      // may have exited while a descendant still holds stdout/stderr open.
      if (process.platform !== 'win32' && child.pid) { try { process.kill(-child.pid, 'SIGKILL'); } catch {} }
      child.kill('SIGKILL');
      child.stdin?.destroy(); child.stdout?.destroy(); child.stderr?.destroy();
      reject(error);
    }
    timer = setTimeout(() => stop(unsafe('Git inspection timed out')), 10_000);
    child.on('error', stop);
    child.stdin.on('error', error => stop(unsafe(`Git inspection input failed: ${error.code ?? 'I/O error'}`)));
    child.stdout.on('data', data => { output += data.length; if (output > max) stop(unsafe('Git output exceeded bounded full-output limit')); else out.push(data); });
    child.stderr.on('data', data => { errors += data.length; if (errors > 4096) stop(unsafe('Git diagnostic exceeded bounded full-output limit')); else err.push(data); });
    child.on('close', code => {
      if (settled) return;
      settled = true; clearTimeout(timer);
      if (!allowed.includes(code)) return reject(unsafe(`Git inspection failed (${args[0]}, exit ${code})`));
      resolvePromise(Buffer.concat(out));
    });
    child.stdin.end(input);
  });
}
const line = bytes => bytes.toString('utf8').trimEnd();
function nul(bytes) {
  if (bytes.length && bytes.at(-1) !== 0) throw unsafe('Incomplete NUL Git output');
  return bytes.length ? bytes.subarray(0, -1).toString('utf8').split('\0') : [];
}
function pathName(value) {
  if (typeof value !== 'string' || !value || Buffer.byteLength(value) > 512 || !/^[\x20-\x7e]+$/.test(value) || value.includes('\\') || value.startsWith('/') || value.endsWith('/') || value.split('/').some(part => part === '' || part === '.' || part === '..' || part.toLowerCase() === '.git')) throw unsafe('Unsafe Git snapshot path');
  const parts = value.split('/');
  if (parts.some(part => /^(?:\.env(?:\..*)?|\.npmrc|\.netrc|credentials?(?:\..*)?|secrets?(?:\..*)?|id_(?:rsa|dsa|ecdsa|ed25519)(?:\.pub)?|.*\.(?:pem|key|p12|pfx|kdbx))$/i.test(part))) throw unsafe('Secret-like Git snapshot path');
  return value;
}
export function gitSnapshotGrant(value) {
  const keys = ['kind', 'missionId', 'workItemId', 'repoRoot', 'expectedHead', 'paths'];
  if (!value || Object.getPrototypeOf(value) !== Object.prototype || Object.keys(value).sort().join() !== keys.sort().join() || value.kind !== 'git-input-snapshot') throw fail('Invalid trusted Git snapshot grant', 'CAPABILITY');
  const root = value.repoRoot;
  if (typeof root !== 'string' || root.length > 4096 || !isAbsolute(root) || resolve(root) !== root || root.includes('\0')) throw fail('Invalid canonical repository root', 'CAPABILITY');
  if (!/^[a-f0-9]{40}(?:[a-f0-9]{24})?$/.test(value.expectedHead)) throw fail('Invalid expected HEAD', 'CAPABILITY');
  if (!Array.isArray(value.paths) || !value.paths.length || value.paths.length > MAX_PATHS) throw fail('Invalid bounded Git paths', 'CAPABILITY');
  const paths = value.paths.map(p => {
    if (!p || Object.getPrototypeOf(p) !== Object.prototype || Object.keys(p).sort().join() !== ['path', 'allowDirty', 'allowUntracked', 'allowDeleted', 'requireDirty'].sort().join() || ['allowDirty', 'allowUntracked', 'allowDeleted', 'requireDirty'].some(key => typeof p[key] !== 'boolean')) throw fail('Invalid Git path permission', 'CAPABILITY');
    return { path: pathName(p.path), allowDirty: p.allowDirty, allowUntracked: p.allowUntracked, allowDeleted: p.allowDeleted, requireDirty: p.requireDirty };
  });
  if (new Set(paths.map(p => p.path)).size !== paths.length || paths.some(p => paths.some(q => p !== q && q.path.startsWith(`${p.path}/`)))) throw fail('Duplicate/overlapping Git paths', 'CAPABILITY');
  return { kind: value.kind, missionId: identifier(value.missionId), workItemId: identifier(value.workItemId), repoRoot: root, expectedHead: value.expectedHead, paths };
}
function rows(bytes, type, paths) {
  const result = new Map();
  for (const row of nul(bytes)) {
    const tab = row.indexOf('\t');
    if (tab < 0) throw unsafe(`Malformed Git ${type} record`);
    const meta = row.slice(0, tab), path = row.slice(tab + 1);
    if (!paths.has(path) || result.has(path)) throw unsafe(`Unexpected or duplicate Git ${type} path`);
    let match;
    if (type === 'tree') match = /^(100644|100755|120000|160000) (blob|commit) ([a-f0-9]{40}|[a-f0-9]{64})$/.exec(meta);
    else match = /^(100644|100755|120000|160000) ([a-f0-9]{40}|[a-f0-9]{64}) ([0-3])$/.exec(meta);
    if (!match) throw unsafe(`Malformed Git ${type} metadata`);
    const mode = match[1], oid = type === 'tree' ? match[3] : match[2];
    if (type === 'tree' && match[2] !== 'blob' || !['100644', '100755'].includes(mode) || type === 'index' && match[3] !== '0') throw unsafe('Symlink, submodule, conflict, or special Git entry');
    result.set(path, { mode, oid });
  }
  return result;
}
async function readFileBounded(path, max, { rejectLink = false } = {}) {
  const file = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0) | (constants.O_NONBLOCK ?? 0));
  try {
    const before = await file.stat({ bigint: true });
    if (!before.isFile() || before.size > BigInt(max) || rejectLink && before.nlink !== 1n) throw unsafe('Unsafe or oversized file');
    const content = Buffer.alloc(Number(before.size)); let offset = 0;
    while (offset < content.length) {
      const { bytesRead } = await file.read(content, offset, content.length - offset, offset);
      if (!bytesRead) throw stale('File changed while reading');
      offset += bytesRead;
    }
    const after = await file.stat({ bigint: true });
    if (before.size !== after.size || before.mtimeNs !== after.mtimeNs || before.ctimeNs !== after.ctimeNs || before.ino !== after.ino || before.dev !== after.dev || before.mode !== after.mode) throw stale('File changed while reading');
    return { content, stat: after };
  } finally { await file.close(); }
}
async function workFile(root, path, oidLength) {
  let current = root;
  const parts = path.split('/');
  for (let i = 0; i < parts.length - 1; i++) {
    current = join(current, parts[i]);
    let stat;
    try { stat = await lstat(current); } catch (e) { if (e.code === 'ENOENT') return null; throw e; }
    if (!stat.isDirectory() || stat.isSymbolicLink()) throw unsafe('Symlink or special parent directory');
  }
  const target = join(root, ...parts);
  let first;
  try { first = await lstat(target, { bigint: true }); } catch (e) { if (e.code === 'ENOENT') return null; throw e; }
  if (!first.isFile() || first.isSymbolicLink() || first.nlink !== 1n || first.size > BigInt(FILE_BYTES)) throw unsafe('Symlink, hardlink, special or oversized working file');
  const read = await readFileBounded(target, FILE_BYTES, { rejectLink: true });
  const last = await lstat(target, { bigint: true });
  if (first.ino !== read.stat.ino || first.dev !== read.stat.dev || first.ino !== last.ino || first.dev !== last.dev || first.mtimeNs !== last.mtimeNs || first.ctimeNs !== last.ctimeNs || first.mode !== last.mode || BigInt(read.content.length) !== last.size) throw stale('Working file changed during capture');
  return { mode: (last.mode & 0o111n) !== 0n ? '100755' : '100644', oid: blobOid(read.content, oidLength), sha256: sha(read.content), bytes: read.content.length, base64: read.content.toString('base64') };
}
function checkContent(bytes) {
  if (bytes.length > FILE_BYTES) throw unsafe('Oversized Git blob');
  if (bytes.includes(Buffer.from('version https://git-lfs.github.com/spec/v1')) || /-----BEGIN (?:[\w ]*PRIVATE KEY|OPENSSH PRIVATE KEY)-----|(?:^|\n)\s*(?:password|secret|api[_-]?key|access[_-]?token)\s*[:=]/i.test(bytes.toString('utf8'))) throw unsafe('Secret-like or LFS pointer content');
}
async function gitBlob(root, { mode, oid }) {
  const data = await git(root, ['cat-file', 'blob', oid], { max: FILE_BYTES });
  checkContent(data);
  if (blobOid(data, oid.length) !== oid) throw stale('Git blob content does not match object identity');
  return { mode, oid, sha256: sha(data), bytes: data.length, base64: data.toString('base64') };
}
async function state(root, grant, indexPath) {
  const actualRoot = line(await git(root, ['rev-parse', '--show-toplevel'], { max: 4096 }));
  if (actualRoot !== root || await realpath(root) !== root) throw unsafe('Repository root changed or is not canonical');
  // Tree traversal can lazy-fetch missing promisor objects too; reject before
  // any object lookup, even if a Git installation ignores NO_LAZY_FETCH.
  const promisor = line(await git(root, ['config', '--get-regexp', 'promisor|partialclone'], { allowed: [0, 1], max: 4096 }));
  if (promisor) throw unsafe('Promisor or partial-clone repository unsupported');
  const head = line(await git(root, ['rev-parse', '--verify', 'HEAD'], { max: 128 }));
  if (head !== grant.expectedHead) throw stale('HEAD does not match trusted grant');
  try { await lstat(`${indexPath}.lock`); throw stale('Index update in progress'); } catch (error) { if (error.code !== 'ENOENT') throw error; }
  const index = await readFileBounded(indexPath, COMMAND_BYTES, { rejectLink: true });
  const names = new Set(grant.paths.map(p => p.path));
  const args = ['--', ...names];
  const treeRaw = await git(root, ['--literal-pathspecs', 'ls-tree', '-r', '-z', '--full-tree', 'HEAD', ...args]);
  const indexRaw = await git(root, ['--literal-pathspecs', 'ls-files', '--stage', '-z', ...args]);
  const flags = nul(await git(root, ['--literal-pathspecs', 'ls-files', '-v', '-z', ...args]));
  if (flags.length !== nul(indexRaw).length || flags.some(record => !/^H /.test(record) || !names.has(record.slice(2)))) throw unsafe('Sparse, assume-unchanged, or conflicted index entry');
  const sparse = line(await git(root, ['config', '--bool', '--get', 'core.sparseCheckout'], { allowed: [0, 1], max: 32 }));
  // Raw working bytes are compared with index bytes. Git may consider a CRLF
  // working copy clean after normalization; reject that state, not mislabel it.
  const autocrlf = line(await git(root, ['config', '--get-regexp', '^core\\.autocrlf$'], { allowed: [0, 1], max: 256 })).toLowerCase();
  if (autocrlf && autocrlf !== 'core.autocrlf false') throw unsafe('Core newline normalization unsupported');
  if (sparse === 'true') throw unsafe('Sparse checkout unsupported');
  const attributes = nul(await git(root, ['check-attr', '-z', '--all', '--stdin'], { input: Buffer.from([...names].join('\0') + '\0') }));
  if (attributes.length % 3) throw unsafe('Malformed attributes output');
  for (let i = 0; i < attributes.length; i += 3) if (!names.has(attributes[i]) || ['filter', 'working-tree-encoding', 'ident', 'text', 'eol', 'crlf'].includes(attributes[i + 1])) throw unsafe('Filters or normalization attributes on snapshot path');
  const ignored = nul(await git(root, ['check-ignore', '--no-index', '-z', '--stdin'], { input: Buffer.from([...names].join('\0') + '\0'), allowed: [0, 1] }));
  if (ignored.length) throw unsafe('Ignored snapshot path');
  return { head, indexSha256: sha(index.content), treeRaw, indexRaw, flags, attributes, ignored, tree: rows(treeRaw, 'tree', names), entries: rows(indexRaw, 'index', names) };
}
function sameState(left, right) {
  return left.head === right.head && left.indexSha256 === right.indexSha256 && left.treeRaw.equals(right.treeRaw) && left.indexRaw.equals(right.indexRaw) && JSON.stringify(left.flags) === JSON.stringify(right.flags) && JSON.stringify(left.attributes) === JSON.stringify(right.attributes);
}

// Caller must obtain grant from trusted host-global configuration, not work text.
// Store must be a private initialized RunStore. No public operation is installed.
export async function captureGitSnapshot(rawGrant, store) {
  const grant = gitSnapshotGrant(rawGrant), root = grant.repoRoot;
  if (!store || typeof store.artifact !== 'function' || typeof store.root !== 'string') throw fail('Private RunStore required', 'CAPABILITY');
  const storeRoot = await realpath(store.root);
  if (storeRoot === root || !relative(root, storeRoot).startsWith('..' + sep) && relative(root, storeRoot) !== '..' || !relative(storeRoot, root).startsWith('..' + sep) && relative(storeRoot, root) !== '..') throw unsafe('Artifact store must not be within repository or contain repository');
  const indexPath = line(await git(root, ['rev-parse', '--path-format=absolute', '--git-path', 'index'], { max: 4096 }));
  if (!isAbsolute(indexPath)) throw unsafe('Cannot identify index');
  const start = await state(root, grant, indexPath);
  const entries = [];
  for (const permission of grant.paths) {
    const path = permission.path, head = start.tree.get(path), staged = start.entries.get(path);
    const working = await workFile(root, path, grant.expectedHead.length);
    if (head && !staged && working) throw unsafe('Staged deletion with recreated untracked working path unsupported');
    if (!head && !staged && !working) throw unsafe('Authorized path is absent');
    const isUntracked = !head && !staged;
    if (isUntracked) {
      const visible = nul(await git(root, ['--literal-pathspecs', 'ls-files', '--others', '--exclude-standard', '-z', '--', path]));
      if (visible.length !== 1 || visible[0] !== path) throw unsafe('Untracked file is ignored or unavailable');
    }
    const headBlob = head ? await gitBlob(root, head) : null;
    const indexBlob = staged ? await gitBlob(root, staged) : head ? { state: 'deleted' } : null;
    if (working) checkContent(Buffer.from(working.base64, 'base64'));
    const work = working ?? { state: 'deleted' };
    const indexChanged = Boolean(head || staged) && (head?.oid !== staged?.oid || head?.mode !== staged?.mode);
    const workChanged = Boolean(staged) && (!working || staged.mode !== working.mode || indexBlob.sha256 !== working.sha256);
    const dirty = isUntracked || indexChanged || workChanged || Boolean(head && !working);
    if (isUntracked && !permission.allowUntracked || !isUntracked && dirty && !permission.allowDirty || dirty && !working && !permission.allowDeleted || permission.requireDirty && !dirty) throw unsafe('Dirty/untracked/deleted state not allowed or required dirty state missing');
    entries.push({ path, head: headBlob, index: indexBlob, work, dirty, untracked: isUntracked });
  }
  const manifest = { kind: 'git-input-snapshot', missionId: grant.missionId, workItemId: grant.workItemId, repoRoot: root, head: grant.expectedHead, indexSha256: start.indexSha256, entries };
  const data = encodeJson(manifest, SNAPSHOT_BYTES);
  const verified = await state(root, grant, indexPath);
  if (!sameState(start, verified)) throw stale('HEAD or index changed during capture');
  for (const entry of entries) if (JSON.stringify(entry.work) !== JSON.stringify(await workFile(root, entry.path, grant.expectedHead.length) ?? { state: 'deleted' })) throw stale('Working file changed during capture');
  let artifact;
  try {
    artifact = await store.artifact(grant.workItemId, 'git-input-snapshot');
    await artifact.write(data);
    const final = await state(root, grant, indexPath);
    if (!sameState(start, final)) throw stale('HEAD or index changed while storing snapshot');
    for (const entry of entries) if (JSON.stringify(entry.work) !== JSON.stringify(await workFile(root, entry.path, grant.expectedHead.length) ?? { state: 'deleted' })) throw stale('Working file changed while storing snapshot');
    const info = await artifact.finish(true);
    return { artifactId: info.id, owner: info.owner, bytes: info.bytes, sha256: hash(data), head: grant.expectedHead, paths: entries.map(e => e.path) };
  } catch (error) {
    if (artifact && !artifact.closed) await artifact.finish(false, 'Git snapshot capture rejected').catch(() => {});
    throw error;
  }
}
