import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, writeFile, readFile, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFileSync, spawn } from 'node:child_process';
import { once } from 'node:events';
import { randomUUID } from 'node:crypto';
import { Engine } from '../src/engine.mjs';
import { RunStore } from '../src/storage.mjs';
import { fakeConfig, eventually, providerFixture } from './support.mjs';
import { createWorkerPool } from '../src/runtime/worker-pool.mjs';
import { Semaphore } from '../src/limits.mjs';
import { processIdentity } from '../src/runtime/ownership.mjs';
import { createPreparedWorkspaceIntent, markWorkspaceMaterializationStarted } from '../src/runtime/workspace-recovery.mjs';

async function setup(t) {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'directory-engine-'))), source = join(root, 'source');
  await mkdir(source); await writeFile(join(source, 'input.bin'), Buffer.from([0, 255, 128, 1]));
  const config = fakeConfig(source); config.agentDir = join(root, 'agent'); await mkdir(config.agentDir);
  config.roles.writer = { ...config.roles.shared, name: 'writer', workspace: 'worktree' };
  const store = new RunStore(join(root, 'run'), config.limits); await store.init();
  let calls = 0;
  const runner = { async validate() {}, async run(task) { calls++; assert.deepEqual(await readFile(join(task.cwd, 'input.bin')), Buffer.from([0, 255, 128, 1])); await writeFile(join(task.cwd, 'output.txt'), 'isolated'); return { state: 'idle', result: 'done', turns: 1 }; } };
  let engine = new Engine(config, store, runner); await engine.init(); engine.pressure = true;
  t.after(async () => { await engine.shutdown(); await store.close(); await rm(root, { recursive: true, force: true }); });
  return { root, source, store, config, runner, get engine() { return engine; }, get calls() { return calls; }, async restart(ownershipIdentity, beforeInit) { await engine.shutdown(); engine = new Engine(config, store, runner, undefined, undefined, ownershipIdentity); engine.pressure = true; beforeInit?.(engine); await engine.init(); return engine; } };
}
const request = { id: 'directory-task', role: 'writer', task: 'Write only into the pinned workspace', mainModel: { provider: 'fixture', model: 'fixture' } };

test('non-Git spawn pins binary input, survives queued restart, and never writes source', async t => {
  const f = await setup(t);
  await f.engine.spawn(request);
  const snapshot = f.engine.get(request.id).inputSnapshot;
  assert.equal(snapshot.complete, true); assert.equal(typeof snapshot.artifactId, 'string');
  await writeFile(join(f.source, 'input.bin'), 'changed after acceptance');
  await f.restart();
  assert.deepEqual(f.engine.get(request.id).inputSnapshot, snapshot);
  f.engine.pressure = false; f.engine.kick();
  await eventually(() => f.engine.summary(f.engine.get(request.id)).settled).catch(error => { throw new Error(`${error.message}: ${JSON.stringify({ task: f.engine.get(request.id), fatal: f.engine.fatal, running: [...f.engine.running.keys()] })}`); });
  const task = f.engine.get(request.id);
  assert.equal(task.state, 'idle', task.error); assert.equal(f.calls, 1);
  assert.equal(task.worktree.source, 'pinned directory snapshot'); assert.equal(task.worktreeIntent, undefined);
  assert.equal(await readFile(join(task.cwd, 'output.txt'), 'utf8'), 'isolated');
  await assert.rejects(readFile(join(f.source, 'output.txt')), { code: 'ENOENT' });
  assert.equal(await readFile(join(f.source, 'input.bin'), 'utf8'), 'changed after acceptance');
});

test('malformed and unborn Git inputs cannot silently fall back to a directory copy', async t => {
  const f = await setup(t);
  await mkdir(join(f.source, '.git'));
  await assert.rejects(f.engine.spawn(request), /Git/);
  await rm(join(f.source, '.git'), { recursive: true });
  execFileSync('git', ['init', '--quiet', f.source]);
  await assert.rejects(f.engine.spawn(request), /HEAD/);
  assert.equal(f.calls, 0);
});

async function recoveryIdentities() {
  const started = await processIdentity(process.pid);
  return {
    old: { pid: process.pid, started: `replaced-${started}`, nonce: randomUUID(), generation: 1 },
    current: { pid: process.pid, started, nonce: randomUUID(), generation: 2 },
  };
}

async function ownedProcess(t, label) {
  const child = spawn(process.execPath, ['-e', 'setInterval(() => {}, 1000)'], { stdio: 'ignore' });
  await once(child, 'spawn');
  const closed = once(child, 'close');
  const identity = { pid: child.pid, started: await processIdentity(child.pid), nonce: `${label}-${randomUUID()}` };
  const stop = async () => { if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL'); await closed; };
  t.after(stop);
  return { child, identity, stop };
}

async function currentOwnership(generation = 2) {
  return { pid: process.pid, started: await processIdentity(process.pid), nonce: randomUUID(), generation };
}

async function stagePreparedDirectoryCrash(f, { ownership, admission = false } = {}) {
  const task = f.engine.get(request.id); ownership ??= await recoveryIdentities();
  task.state = 'running'; task.executionSegment = 1; task.dependencyInputs = [];
  task.executionIntent = { id: randomUUID(), startedAt: Date.now(), segment: 1 };
  if (admission) {
    const decision = await f.engine.admission.reserveShared(task, {
      running: f.engine.running, tasks: f.engine.tasks, pressure: false, connectionHeld: false,
      workers: f.engine.runner.maxWorkers ?? f.config.limits.concurrency, memory: f.engine.resourceObservation(),
      quotaPools: f.engine.quotaPools, sharedMemory: false, memoryObservationKnown: false, systemMemory: f.engine.systemMemory,
      configEpoch: f.engine.runner.configEpoch,
    });
    assert.equal(decision.admitted, true, decision.reason);
    f.engine.persistAdmissionReservation(task, decision.reservation);
  }
  const destination = join(f.store.root, 'worktrees', task.id);
  const expected = f.engine.workspacePreparationExpectation(task, {
    kind: 'directory-snapshot', base: task.inputSnapshot.sha256,
    input: { kind: 'directory-snapshot', ownerId: task.id, ref: task.inputSnapshot }, destination,
  });
  task.worktreeIntent = createPreparedWorkspaceIntent({
    intentId: randomUUID(), taskId: task.id, attemptId: task.executionIntent.id,
    executionSegment: task.executionSegment, ...expected, owner: ownership.old,
  });
  await f.store.saveTask(task);
  if (admission) { f.engine.admission.markUncertain(task.id, true); await f.engine.admission.releaseShared(task.id); }
  return { task, ownership };
}

test('production directory preparation durably saves its start marker before the first workspace filesystem operation', { timeout: 10_000 }, async t => {
  const f = await setup(t), owner = await currentOwnership(1); await f.restart(owner); await f.engine.spawn(request);
  const task = f.engine.get(request.id), parent = join(f.store.root, 'worktrees'), destination = join(parent, task.id);
  task.state = 'running'; task.executionSegment = 1; task.dependencyInputs = [];
  task.executionIntent = { id: randomUUID(), startedAt: Date.now(), segment: 1 };
  await rm(parent, { recursive: true, force: true });
  const originalSave = f.store.saveTask.bind(f.store);
  let markerSaved, releaseMarker;
  const saved = new Promise(resolve => { markerSaved = resolve; });
  const release = new Promise(resolve => { releaseMarker = resolve; });
  let observed = false;
  f.store.saveTask = async candidate => {
    await originalSave(candidate);
    if (!observed && candidate.id === task.id && candidate.worktreeIntent?.materializationGeneration !== null) { observed = true; markerSaved(); await release; }
  };
  let preparation;
  try {
    preparation = f.engine.prepareWorkspace(task, new AbortController().signal);
    await saved;
    const durable = JSON.parse(await readFile(f.store.taskPath(task.id), 'utf8'));
    assert.equal(durable.worktreeIntent.materializationGeneration, owner.generation);
    await assert.rejects(realpath(parent), { code: 'ENOENT' });
  } finally {
    releaseMarker();
    try { await preparation; } finally { f.store.saveTask = originalSave; }
  }
  assert.equal(task.worktreeIntent, undefined);
  assert.equal(await readFile(join(destination, 'input.bin')).then(value => value.equals(Buffer.from([0, 255, 128, 1]))), true);
});

test('prepared workspace recovery refuses while the prior owning process is actually live', async t => {
  const f = await setup(t), prior = await ownedProcess(t, 'live-owner'); await f.engine.spawn(request);
  const ownership = { old: { ...prior.identity, generation: 1 }, current: await currentOwnership(2) };
  try {
    const { task } = await stagePreparedDirectoryCrash(f, { ownership }), prepared = structuredClone(task.worktreeIntent);
    await f.restart(ownership.current);
    const restored = f.engine.get(task.id);
    assert.deepEqual(restored.worktreeIntent, prepared); assert.equal(restored.workspaceRecoveryAudit, undefined);
    assert.equal(restored.state, 'interrupted'); assert.equal(f.calls, 0);
  } finally { await prior.stop(); }
});

for (const liveFence of ['executor', 'guardian']) {
  test(`prepared workspace recovery refuses while its recorded ${liveFence} process is actually live`, async t => {
    const f = await setup(t), prior = await ownedProcess(t, `${liveFence}-owner`), live = await ownedProcess(t, `live-${liveFence}`);
    await f.engine.spawn(request); await prior.stop();
    const ownership = { old: { ...prior.identity, generation: 1 }, current: await currentOwnership(2) };
    try {
      const { task } = await stagePreparedDirectoryCrash(f, { ownership });
      task.executionIntent.executor = liveFence === 'executor' ? { ...live.identity } : { ...prior.identity, guardians: [{ ...live.identity }] };
      const prepared = structuredClone(task.worktreeIntent); await f.store.saveTask(task);
      await f.restart(ownership.current);
      const restored = f.engine.get(task.id);
      assert.deepEqual(restored.worktreeIntent, prepared); assert.equal(restored.workspaceRecoveryAudit, undefined);
      assert.equal(restored.recoveryPending, true); assert.equal(restored.state, 'interrupted'); assert.equal(f.calls, 0);
    } finally { await live.stop(); }
  });
}

for (const drift of ['input', 'authority']) {
  test(`prepared workspace recovery refuses pinned ${drift} drift`, async t => {
    const f = await setup(t), prior = await ownedProcess(t, `${drift}-owner`); await f.engine.spawn(request);
    let driftedInput;
    if (drift === 'input') {
      await writeFile(join(f.source, 'input.bin'), 'different valid pinned input');
      driftedInput = await f.engine.readWorktreeSnapshot(f.source, request.id);
    }
    await prior.stop();
    const ownership = { old: { ...prior.identity, generation: 1 }, current: await currentOwnership(2) };
    const { task } = await stagePreparedDirectoryCrash(f, { ownership }), prepared = structuredClone(task.worktreeIntent);
    if (drift === 'input') task.inputSnapshot = driftedInput;
    else task.role = { ...task.role, hash: 'drifted-authority' };
    await f.store.saveTask(task); await f.restart(ownership.current);
    const restored = f.engine.get(task.id);
    assert.deepEqual(restored.worktreeIntent, prepared); assert.equal(restored.workspaceRecoveryAudit, undefined);
    assert.equal(restored.state, 'interrupted'); assert.equal(f.calls, 0);
  });
}

test('a crash after workspace recovery audit persistence is repaired through that audit on the next admission startup', async t => {
  const f = await setup(t), prior = await ownedProcess(t, 'audit-owner'); await f.engine.spawn(request); await prior.stop();
  const ownership = { old: { ...prior.identity, generation: 1 }, current: await currentOwnership(2) };
  const { task } = await stagePreparedDirectoryCrash(f, { ownership, admission: true });
  const fence = structuredClone(task.admissionFence), intentId = task.worktreeIntent.intentId;
  let injected = false;
  await assert.rejects(f.restart(ownership.current, engine => {
    const reconcile = engine.admission.reconcile.bind(engine.admission);
    engine.admission.reconcile = async candidate => {
      if (!injected && candidate?.id === fence.id) { injected = true; throw new Error('injected crash after workspace recovery audit persistence'); }
      return reconcile(candidate);
    };
  }), /injected crash/);
  assert.equal(injected, true);
  const durable = JSON.parse(await readFile(f.store.taskPath(task.id), 'utf8'));
  assert.equal(durable.worktreeIntent, undefined); assert.equal(durable.workspaceRecoveryAudit[0].intentId, intentId);
  let reconciliations = 0;
  await f.restart(await currentOwnership(3), engine => {
    const reconcile = engine.admission.reconcile.bind(engine.admission);
    engine.admission.reconcile = async candidate => { if (candidate?.id === fence.id) reconciliations++; return reconcile(candidate); };
  });
  const restored = f.engine.get(task.id);
  assert.equal(reconciliations, 1); assert.equal(restored.state, 'queued');
  assert.equal(restored.workspaceRecoveryAudit[0].intentId, intentId); assert.equal(restored.worktreeIntent, undefined);
});

test('newer exclusive owner clears only an exact prepared/no-start workspace intent and performs one bounded retry', async t => {
  const f = await setup(t); await f.engine.spawn(request);
  const { task, ownership } = await stagePreparedDirectoryCrash(f);
  const intentId = task.worktreeIntent.intentId;
  await f.restart(ownership.current);
  const recovered = f.engine.get(task.id);
  assert.equal(recovered.state, 'queued'); assert.equal(recovered.worktreeIntent, undefined);
  assert.equal(recovered.recoveryAttempts, 1); assert.equal(recovered.workspaceRecoveryAudit.length, 1);
  assert.deepEqual({ intentId: recovered.workspaceRecoveryAudit[0].intentId, outcome: recovered.workspaceRecoveryAudit[0].outcome, preparedGeneration: recovered.workspaceRecoveryAudit[0].preparedGeneration, recoveryGeneration: recovered.workspaceRecoveryAudit[0].recoveryGeneration },
    { intentId, outcome: 'cleared_prepared_without_materialization_start', preparedGeneration: 1, recoveryGeneration: 2 });
  f.engine.pressure = false; f.engine.kick();
  await eventually(() => f.engine.summary(f.engine.get(task.id)).settled);
  assert.equal(f.engine.get(task.id).state, 'idle'); assert.equal(f.calls, 1);
  assert.equal(f.engine.get(task.id).workspaceRecoveryAudit.length, 1, 'the future-only retry budget remains durably consumed');
});

test('a durable started-before-call marker is never cleared even when the target is absent', async t => {
  const f = await setup(t); await f.engine.spawn(request);
  const { task, ownership } = await stagePreparedDirectoryCrash(f);
  task.worktreeIntent = markWorkspaceMaterializationStarted(task.worktreeIntent, ownership.old); await f.store.saveTask(task);
  await f.restart(ownership.current);
  const restored = f.engine.get(task.id);
  assert.equal(restored.worktreeIntent.materializationGeneration, 1); assert.equal(restored.workspaceRecoveryAudit, undefined);
  assert.equal(restored.state, 'interrupted'); assert.equal(f.calls, 0);
  await assert.rejects(realpath(join(f.store.root, 'worktrees', task.id)), { code: 'ENOENT' });
});

test('prepared workspace recovery refuses an execution-attempt identity mismatch', async t => {
  const f = await setup(t); await f.engine.spawn(request);
  const { task, ownership } = await stagePreparedDirectoryCrash(f);
  const prepared = structuredClone(task.worktreeIntent); task.executionIntent.id = randomUUID(); await f.store.saveTask(task);
  await f.restart(ownership.current);
  const restored = f.engine.get(task.id);
  assert.deepEqual(restored.worktreeIntent, prepared); assert.equal(restored.workspaceRecoveryAudit, undefined);
  assert.equal(restored.state, 'interrupted'); assert.equal(f.calls, 0);
});

test('prepared workspace recovery preserves an ambiguous materialization marker', async t => {
  const f = await setup(t); await f.engine.spawn(request);
  const { task, ownership } = await stagePreparedDirectoryCrash(f);
  task.worktreeIntent = { ...task.worktreeIntent, unrecognizedMarker: null }; const ambiguous = structuredClone(task.worktreeIntent); await f.store.saveTask(task);
  await f.restart(ownership.current);
  const restored = f.engine.get(task.id);
  assert.deepEqual(restored.worktreeIntent, ambiguous); assert.equal(restored.workspaceRecoveryAudit, undefined);
  assert.equal(restored.state, 'interrupted'); assert.equal(f.calls, 0);
});

test('uncertain directory creation stays fenced after restart without recapture or execution', async t => {
  const f = await setup(t); await f.engine.spawn(request);
  const task = f.engine.get(request.id);
  task.worktreeIntent = { path: join(f.store.root, 'worktrees', task.id), base: task.inputSnapshot.sha256 };
  await f.engine.persist(task); await f.restart((await recoveryIdentities()).current);
  f.engine.pressure = false; f.engine.kick(); await f.engine.tail;
  assert.ok(f.engine.get(task.id).worktreeIntent);
  assert.notEqual(f.engine.get(task.id).state, 'queued'); assert.equal(f.calls, 0);
});

test('installed SDK writes only to the pinned non-Git directory through real worker tools', { timeout: 45000 }, async t => {
  const root = await realpath(await mkdtemp(join(tmpdir(), 'directory-sdk-')));
  const fixture = await providerFixture(root), source = join(root, 'documents'); await mkdir(source);
  await writeFile(join(source, 'marker.txt'), 'accepted input');
  const config = { ...fakeConfig(source), agentDir: fixture.agentDir, authPath: join(fixture.agentDir, 'auth.json'), modelsPath: join(fixture.agentDir, 'models.json'), providerModules: [] };
  config.roles.writer = { ...config.roles.shared, name: 'writer', workspace: 'worktree' };
  const store = new RunStore(join(root, 'run'), config.limits); await store.init();
  const pool = await createWorkerPool(config, store, new Semaphore(config.limits.commandConcurrency));
  const engine = new Engine(config, store, pool); await engine.init();
  t.after(async () => { await engine.shutdown(); await store.close(); await fixture.close(); await rm(root, { recursive: true, force: true }); });
  await engine.spawn({ id: 'sdk-directory', role: 'writer', task: 'fixture-write: create isolated output', mainModel: fixture.profile });
  await eventually(() => engine.summary(engine.get('sdk-directory')).settled, 30000);
  const task = engine.get('sdk-directory'); assert.equal(task.state, 'idle', task.error);
  assert.equal(task.worktree.source, 'pinned directory snapshot');
  assert.equal(await readFile(join(task.cwd, 'created.txt'), 'utf8'), 'worktree-only-evidence');
  await assert.rejects(readFile(join(source, 'created.txt')), { code: 'ENOENT' });
  assert.equal(fixture.calls, 2); assert.equal(task.resultRef.complete, true);
  const steps = store.metadata.list('steps', store.scope).items;
  assert.equal(steps.length, 1); assert.equal(steps[0].tool, 'write'); assert.equal(steps[0].state, 'completed');
});

test('ambient Git routing cannot redirect non-Git capture or Git pinning and materialization', async t => {
  const f = await setup(t), other = join(f.root, 'outside'); await mkdir(other);
  const git = (cwd, args) => execFileSync('git', args, { cwd, env: Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith('GIT_'))) }).toString().trim();
  git(other, ['init', '--quiet']); await writeFile(join(other, 'outside.txt'), 'not admitted'); git(other, ['add', '.']);
  git(other, ['-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '--quiet', '-m', 'fixture']);
  const saved = { GIT_DIR: process.env.GIT_DIR, GIT_WORK_TREE: process.env.GIT_WORK_TREE };
  try {
    process.env.GIT_DIR = join(other, '.git'); process.env.GIT_WORK_TREE = other;
    const ref = await f.engine.readWorktreeSnapshot(f.source, 'non-git'); assert.equal(typeof ref, 'object');
    git(f.source, ['init', '--quiet']); git(f.source, ['add', '.']);
    git(f.source, ['-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '--quiet', '-m', 'accepted']);
    const expected = git(f.source, ['rev-parse', 'HEAD']);
    await f.engine.spawn(request); assert.equal(f.engine.get(request.id).inputSnapshot, expected);
    f.engine.pressure = false; f.engine.kick(); await eventually(() => f.engine.summary(f.engine.get(request.id)).settled);
    const task = f.engine.get(request.id); assert.equal(task.state, 'idle', task.error);
    assert.equal(git(task.cwd, ['rev-parse', 'HEAD']), expected);
    await assert.rejects(readFile(join(task.cwd, 'outside.txt')), { code: 'ENOENT' });
  } finally { for (const [key, value] of Object.entries(saved)) { if (value === undefined) delete process.env[key]; else process.env[key] = value; } }
});
