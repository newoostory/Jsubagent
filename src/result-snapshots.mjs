import { createHash } from 'node:crypto';
import { encodeJson, fail, hash } from './limits.mjs';
import { captureOwnedProductDirectory, inspectDirectorySnapshot } from './execution/directory-snapshot.mjs';
import { effectRequiresReconciliation } from './execution/failure-receipt.mjs';

const policy = Object.freeze({ files: 'all-regular-including-untracked', excludedNames: ['.git', '.pi', 'node_modules'], symlinks: 'reject', hardlinks: 'reject', specialFiles: 'reject', rename: 'delete-plus-add', binary: 'exact-bytes', modes: 'posix-permissions' });
const line = value => Buffer.concat([encodeJson(value, 32768), Buffer.from('\n')]);
const refOf = info => ({ artifactId: info.id, bytes: info.bytes, sha256: info.sha256, complete: true });

export function productSnapshot(store, subject, owner) {
  if (!subject || subject.type !== 'product-directory-v1' || typeof subject.snapshotId !== 'string') throw fail('Missing immutable product snapshot', 'BLOCKED');
  const row = store.metadata.get('result_snapshots', store.scope, subject.snapshotId);
  if (!row || row.state !== 'sealed' || row.subject.ownerId !== owner || JSON.stringify(row.subject) !== JSON.stringify(subject)) throw fail('Product snapshot is not a trusted runtime capture', 'PERMISSION');
  return row.subject;
}

export function snapshotEntries(store, subject, after = '', limit = 32) {
  return store.metadata.database.prepare("SELECT data_json FROM result_snapshot_entries WHERE run_id=? AND owner_id=? AND json_extract(data_json,'$.snapshotId')=? AND json_extract(data_json,'$.path')>? ORDER BY json_extract(data_json,'$.path') LIMIT ?").all(store.scope.runId, store.scope.ownerId, subject.snapshotId, after, limit).map(row => JSON.parse(row.data_json).entry);
}

export async function captureProductSnapshot(store, task, { signal, assertCurrent = () => {} } = {}) {
  assertCurrent();
  const snapshotRef = await captureOwnedProductDirectory(store, task, signal);
  assertCurrent();
  const snapshotId = `ps-${hash(`${task.id}\0${snapshotRef.sha256}\0${hash(encodeJson(policy))}`)}`;
  const prior = store.metadata.get('result_snapshots', store.scope, snapshotId);
  if (prior) return productSnapshot(store, prior.subject, task.id);
  let files = 0, entries = 0;
  const { rootMode } = await inspectDirectorySnapshot(store, task.id, snapshotRef, entry => {
    assertCurrent(); entries++; if (entry.type === 'file') files++;
    const id = hash(`${snapshotId}\0${entry.path}`), existing = store.metadata.get('result_snapshot_entries', store.scope, id);
    if (existing) { if (existing.snapshotId !== snapshotId || JSON.stringify(existing.entry) !== JSON.stringify(entry)) throw fail('Immutable snapshot index changed', 'STORAGE'); }
    else store.metadata.put('result_snapshot_entries', store.scope, { id, state: 'sealed', snapshotId, path: entry.path, entry }, { ifAbsent: true });
  }, signal);
  // Canonical product identity ignores archive enumeration order and all report
  // revisions. It covers path/type/mode/file bytes, not a self-referential bundle.
  const digest = createHash('sha256'); digest.update(line({ type: 'product-directory-v1', policy, rootMode }));
  let after = '';
  for (;;) {
    const page = snapshotEntries(store, { snapshotId }, after);
    for (const entry of page) { digest.update(line(entry)); after = entry.path; }
    if (page.length < 32) break;
  }
  const subject = { type: 'product-directory-v1', snapshotId, ownerId: task.id, productSha256: digest.digest('hex'), snapshotRef, policy, rootMode, files, entries };
  assertCurrent();
  store.metadata.put('result_snapshots', store.scope, { id: snapshotId, state: 'sealed', subject }, { ifAbsent: true });
  return subject;
}

async function* entries(store, subject) {
  let after = '';
  for (;;) {
    const page = snapshotEntries(store, subject, after);
    for (const entry of page) { after = entry.path; yield entry; }
    if (page.length < 32) return;
  }
}

// Actual before/after snapshot metadata, not the model's remembered file list.
// Rename is explicitly delete+add, binary content is retained in snapshot refs.
export async function productChangeArtifacts(store, task, base, current, assertCurrent = () => {}) {
  productSnapshot(store, base, task.id); productSnapshot(store, current, task.id);
  const changes = await store.artifact(task.id, 'product-changes-index');
  const patches = await store.artifact(task.id, 'product-patch-index');
  try {
    const left = entries(store, base)[Symbol.asyncIterator](), right = entries(store, current)[Symbol.asyncIterator]();
    let a = await left.next(), b = await right.next(), count = 0;
    if (base.rootMode !== current.rootMode) {
      await changes.write(line({ root: true, change: 'mode', before: { mode: base.rootMode }, after: { mode: current.rootMode }, baseProductSha256: base.productSha256, productSha256: current.productSha256 }));
      await patches.write(line({ root: true, format: 'exact-before-after-snapshots', before: { snapshotId: base.snapshotId }, after: { snapshotId: current.snapshotId } })); count++;
    }
    while (!a.done || !b.done) {
      assertCurrent(); let before = null, after = null;
      const order = a.done ? 1 : b.done ? -1 : Buffer.compare(Buffer.from(a.value.path), Buffer.from(b.value.path));
      if (order < 0) { before = a.value; a = await left.next(); }
      else if (order > 0) { after = b.value; b = await right.next(); }
      else { before = a.value; after = b.value; a = await left.next(); b = await right.next(); }
      if (JSON.stringify(before) === JSON.stringify(after)) continue;
      const value = { path: after?.path ?? before.path, change: !before ? 'added' : !after ? 'deleted' : 'modified', before, after, baseProductSha256: base.productSha256, productSha256: current.productSha256 };
      await changes.write(line(value));
      await patches.write(line({ path: value.path, format: 'exact-before-after-snapshots', before: before ? { snapshotId: base.snapshotId, path: before.path, sha256: before.sha256 ?? null } : null, after: after ? { snapshotId: current.snapshotId, path: after.path, sha256: after.sha256 ?? null } : null }));
      count++;
    }
    assertCurrent();
    const changed = await changes.finish(true), patchIndex = await patches.finish(true);
    return { count, files: [
      { logicalPath: 'changes.jsonl', kind: 'changed_files', contentRef: refOf(changed), fileComplete: true, mediaType: 'application/x-ndjson' },
      { logicalPath: 'patches/index.jsonl', kind: 'patch_index', contentRef: refOf(patchIndex), fileComplete: true, mediaType: 'application/x-ndjson' },
    ] };
  } catch (error) {
    for (const artifact of [changes, patches]) if (!artifact.closed) await artifact.finish(false, 'product change collection failed').catch(() => {});
    throw error;
  }
}

export async function prepareCodeChangePublication(store, task, request, { assertCurrent = () => {}, signal } = {}) {
  if (task.resultContract !== 'code-change' || request.stage === 'progress') return { request };
  if (!task.productBase || task.worktreeIntent || task.requestPermitUncertain || (task.pendingEffects ?? []).some(step => step.tool !== 'agent_result' && step.outcome !== 'completed' && effectRequiresReconciliation(step))) throw fail('Uncertain product mutation/permit cannot become a candidate', 'UNCERTAIN');
  const reserved = new Set(['changes.jsonl', 'patches/index.jsonl', 'tests.jsonl', 'verification-plan.json', 'product/snapshot.bin', 'product/base-snapshot.bin']);
  if (request.files.some(file => reserved.has(file.logicalPath))) throw fail('Code-change provenance files are runtime-generated, not model declarations', 'PERMISSION');
  const subject = await captureProductSnapshot(store, task, { assertCurrent, signal });
  const changes = await productChangeArtifacts(store, task, task.productBase, subject, assertCurrent);
  const tests = await store.artifact(task.id, 'product-test-inventory');
  let inventory;
  try {
    for await (const entry of entries(store, subject)) if (entry.type === 'file' && /(?:^|\/)(?:test|tests|spec|specs)(?:\/|\.)|\.(?:test|spec)\./i.test(entry.path)) await tests.write(line({ ...entry, candidate: true, executed: false, productSha256: subject.productSha256 }));
    inventory = await tests.finish(true);
  } catch (error) { if (!tests.closed) await tests.finish(false, 'test inventory failed').catch(() => {}); throw error; }
  const plan = await store.artifact(task.id, 'product-verification-plan');
  let planInfo;
  try {
    await plan.write(line({ schemaVersion: 1, productSha256: subject.productSha256, baseProductSha256: task.productBase.productSha256, acceptance: 'not_evaluated', coverage: 'requires-approved-checker-contract', requiredChecks: [], reason: 'Discovered test files are candidates, not executed checks or proof of required coverage.', executionAuthority: 'Existing role tools only; this artifact grants no execution permission.' }));
    planInfo = await plan.finish(true);
  } catch (error) { if (!plan.closed) await plan.finish(false, 'verification plan failed').catch(() => {}); throw error; }
  const raw = ref => ({ artifactId: ref.artifactId, bytes: ref.bytes, sha256: ref.sha256, complete: true });
  const files = [...changes.files,
    { logicalPath: 'tests.jsonl', kind: 'test_inventory', contentRef: refOf(inventory), fileComplete: true, mediaType: 'application/x-ndjson' },
    { logicalPath: 'verification-plan.json', kind: 'verification_plan', contentRef: refOf(planInfo), fileComplete: true, mediaType: 'application/json' },
    { logicalPath: 'product/snapshot.bin', kind: 'evidence', contentRef: raw(subject.snapshotRef), fileComplete: true, mediaType: 'application/octet-stream' },
    { logicalPath: 'product/base-snapshot.bin', kind: 'evidence', contentRef: raw(task.productBase.snapshotRef), fileComplete: true, mediaType: 'application/octet-stream' },
  ];
  assertCurrent();
  return { request: { ...request, files: [...request.files, ...files] }, subject, generatedPaths: files.map(file => file.logicalPath) };
}
