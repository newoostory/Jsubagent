import test from 'node:test';
import assert from 'node:assert/strict';
import { access, mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createBenchmarkExpiryTimer, Engine } from '../src/engine.mjs';
import { RunStore } from '../src/storage.mjs';
import { Semaphore } from '../src/limits.mjs';
import { WorkerPool } from '../src/runtime/worker-pool.mjs';
import { fakeConfig, eventually } from './support.mjs';

const model = { provider: 'fixture', model: 'fixture' };
const params = id => ({ id, role: 'scout', task: 'test', mainModel: model });

async function setup(t, run) {
  const root = await mkdtemp(join(tmpdir(), 'pi-agent-lifecycle-'));
  const config = fakeConfig(root);
  const store = new RunStore(join(root, 'run'), config.limits);
  await store.init();
  const runner = { validate: async () => {}, run };
  const engine = new Engine(config, store, runner);
  await engine.init();
  t.after(async () => {
    await engine.shutdown().catch(() => {});
    await rm(root, { recursive: true, force: true });
  });
  return { root, config, store, engine };
}

test('a stale cold worker reservation is readmitted without failing or executing the task', async t => {
  let pool, attempts = 0, executions = 0;
  const f = await setup(t, async task => {
    if (++attempts === 1) return pool.withWorker({ ...task, memoryPlan: { cold: true, workerKey: null } }, {}, undefined, () => { executions++; });
    return { state: 'idle', result: 'readmitted', checkpoint: { messages: [] }, usage: {}, turns: 1 };
  });
  pool = new WorkerPool(f.config, f.store, new Semaphore(1), { maxWorkers: 1, initialWorkers: 0 });
  // The prior ps snapshot selected cold startup; by dispatch, the existing
  // process is idle and fills the process budget. No worker has started this task.
  pool.workers.add({ busy: false, configEpoch: pool.configEpoch });
  t.after(() => clearInterval(pool.idleTimer));
  await f.engine.spawn(params('cold-readmission'));
  await eventually(() => f.engine.summary(f.engine.get('cold-readmission')).settled);
  const task = f.engine.get('cold-readmission');
  assert.equal(task.state, 'idle', task.error);
  assert.equal(task.result, 'readmitted');
  assert.equal(attempts, 2);
  assert.equal(executions, 0, 'the stale reservation must not start an executor');
  assert.equal(pool.workers.size, 1, 'readmission must not exceed the process cap');
  assert.equal(pool.slots.active, 0);
});

test('internal completion settles while the bounded public mutation queue is saturated', async t => {
  let finish;
  const f = await setup(t, async () => {
    await new Promise(resolve => { finish = resolve; });
    return { state: 'idle', result: 'done', checkpoint: { messages: [] }, usage: {}, turns: 1 };
  });
  await f.engine.spawn(params('saturated'));
  await eventually(() => finish);

  let releaseBarrier;
  const barrier = f.engine.mutate(() => new Promise(resolve => { releaseBarrier = resolve; }));
  await eventually(() => releaseBarrier);
  const queued = Array.from({ length: 63 }, () => f.engine.mutate(async () => {}));
  assert.equal(f.engine.publicPending, 64);
  await assert.rejects(f.engine.mutate(async () => {}), { code: 'BUSY' });

  finish();
  await eventually(() => f.engine.pending > f.engine.publicPending);
  releaseBarrier();
  await barrier;
  await Promise.all(queued);
  await eventually(() => f.engine.summary(f.engine.get('saturated')).settled);
  assert.equal(f.engine.get('saturated').state, 'idle');
  assert.equal((await f.store.loadTasks())[0].state, 'idle');
});

test('cancel re-resolves identity after a concurrent archive and cannot resurrect its task record', async t => {
  const f = await setup(t, async () => ({ state: 'idle', result: 'done', checkpoint: { messages: [] }, usage: {}, turns: 1 }));
  await f.engine.spawn(params('archived'));
  await eventually(() => f.engine.summary(f.engine.get('archived')).settled);

  let releaseBarrier;
  const barrier = f.engine.mutate(() => new Promise(resolve => { releaseBarrier = resolve; }));
  await eventually(() => releaseBarrier);
  const archived = f.engine.archive('archived');
  const cancelled = f.engine.cancel('archived');
  releaseBarrier();
  await barrier;
  await archived;
  await assert.rejects(cancelled, { code: 'NOT_FOUND' });

  assert.equal(f.engine.tasks.has('archived'), false);
  assert.equal((await f.store.loadTasks()).some(task => task.id === 'archived'), false);
  assert.equal(f.store.metadata.get('work_items', f.store.scope, 'archived').state, 'archived');
});

test('terminal persistence failure releases the slot and projects a failed result', async t => {
  let finish;
  const f = await setup(t, async () => {
    await new Promise(resolve => { finish = resolve; });
    return { state: 'idle', result: 'done', checkpoint: { messages: [] }, usage: {}, turns: 1 };
  });
  await f.engine.spawn(params('disk-failure'));
  await eventually(() => finish);
  const saveTask = f.store.saveTask.bind(f.store);
  f.store.saveTask = async task => {
    if (task.state !== 'running') throw new Error('fixture disk failure');
    return saveTask(task);
  };

  finish();
  await eventually(() => !f.engine.running.has('disk-failure'));
  const task = f.engine.get('disk-failure');
  assert.equal(f.engine.stopping, true);
  assert.match(f.engine.fatal, /persistence failed/i);
  assert.equal(task.state, 'failed');
  assert.equal(task.currentReport, null);
  assert.match(task.error, /persistence failed/i);

  f.store.saveTask = saveTask;
  const recovered = new Engine(f.config, f.store, { validate: async () => {}, run: async () => { throw new Error('must not replay'); } });
  await recovered.init();
  assert.equal(recovered.get('disk-failure').state, 'interrupted');
  await recovered.shutdown();
});

test('ordinary jobs ignore deprecated jobTimeoutMs while explicit cancellation remains effective', async t => {
  const completed = await setup(t, async () => {
    await new Promise(resolve => setTimeout(resolve, 30));
    return { state: 'idle', result: 'done', checkpoint: { messages: [] }, usage: {}, turns: 1 };
  });
  completed.config.limits.jobTimeoutMs = 1;
  await completed.engine.spawn(params('no-wall-clock-timeout'));
  await eventually(() => completed.engine.summary(completed.engine.get('no-wall-clock-timeout')).settled);
  assert.equal(completed.engine.get('no-wall-clock-timeout').state, 'idle');

  const cancelled = await setup(t, async (_task, signal) => {
    await new Promise((resolve, reject) => {
      signal.addEventListener('abort', () => reject(signal.reason), { once: true });
    });
  });
  cancelled.config.limits.jobTimeoutMs = 1;
  await cancelled.engine.spawn(params('explicit-cancel'));
  await eventually(() => cancelled.engine.running.has('explicit-cancel'));
  await cancelled.engine.cancel('explicit-cancel');
  await eventually(() => cancelled.engine.summary(cancelled.engine.get('explicit-cancel')).settled);
  assert.equal(cancelled.engine.get('explicit-cancel').state, 'cancelled');
});

test('benchmark grant expiry keeps its independent authorization timer', async () => {
  let reason;
  const fired = new Promise(resolve => {
    createBenchmarkExpiryTimer({ benchmark: { expiresAt: Date.now() + 10 } }, error => { reason = error; resolve(); });
  });
  await fired;
  assert.equal(reason.code, 'TIMEOUT');
  assert.match(reason.message, /Benchmark grant expired/);
  assert.equal(createBenchmarkExpiryTimer({}, () => assert.fail('ordinary job must not get a timer')), undefined);
});
