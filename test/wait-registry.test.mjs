import test from 'node:test';
import assert from 'node:assert/strict';
import { WaitRegistry } from '../src/wait-registry.mjs';
import { HostClient } from '../src/client.mjs';

function fixture(rows) {
  const listeners = new Set();
  const engine = { tasks: new Map(rows.map(row => [row.id, row])), summary: task => structuredClone(task), subscribeChanges(listener) { listeners.add(listener); return () => listeners.delete(listener); }, publish() { for (const listener of [...listeners]) listener(); } };
  return { engine, registry: new WaitRegistry(engine), listeners };
}
const running = id => ({ id, state: 'running', settled: false, recentMessages: [] });
test('wait all observes settlement only after the running-slot release event', async () => {
  const f = fixture([running('a'), running('b')]); const waiting = f.registry.wait({ waitId: 'wait-all', ids: ['a', 'b'], timeoutMs: 1000 });
  Object.assign(f.engine.tasks.get('a'), { state: 'idle', settled: true }); f.engine.publish();
  let resolved = false; waiting.then(() => { resolved = true; }); await Promise.resolve(); assert.equal(resolved, false);
  Object.assign(f.engine.tasks.get('b'), { state: 'idle', settled: false }); f.engine.publish(); await Promise.resolve(); assert.equal(resolved, false);
  f.engine.tasks.get('b').settled = true; f.engine.publish();
  assert.deepEqual((await waiting).map(row => row.id), ['a', 'b']); assert.equal(f.listeners.size, 0);
});
test('wait any returns all selected rows in request order', async () => {
  const f = fixture([running('a'), running('b')]); const waiting = f.registry.wait({ waitId: 'wait-any', ids: ['b', 'a'], timeoutMs: 1000, mode: 'any' });
  Object.assign(f.engine.tasks.get('a'), { state: 'failed', settled: true }); f.engine.publish();
  const rows = await waiting; assert.deepEqual(rows.map(row => row.id), ['b', 'a']); assert.equal(rows[0].settled, false); assert.equal(rows[1].settled, true);
});
test('message wake ignores seen artifacts and resolves on a new question', async () => {
  const f = fixture([running('a')]); f.engine.tasks.get('a').recentMessages = [{ artifactId: 'seen', kind: 'question' }];
  const waiting = f.registry.wait({ waitId: 'wait-message', ids: ['a'], timeoutMs: 1000, wakeOnMessage: true, seenMessages: ['seen'] });
  let resolved = false; waiting.then(() => { resolved = true; }); await Promise.resolve(); assert.equal(resolved, false);
  f.engine.tasks.get('a').recentMessages.push({ artifactId: 'new', kind: 'blocked' }); f.engine.publish();
  assert.equal((await waiting)[0].recentMessages.at(-1).artifactId, 'new');
});
test('metadata gives attention priority and reports a fresh server snapshot', async () => {
  const row = { id: 'a', state: 'idle', settled: true, recentMessages: [{ artifactId: 'question-a', kind: 'question' }] };
  const f = fixture([row]);
  const result = await f.registry.wait({ waitId: 'wait-metadata', ids: ['a'], timeoutMs: 1000, mode: 'all', wakeOnMessage: true, includeMetadata: true });
  assert.equal(result.reason, 'attention'); assert.equal(result.conditionMet, true);
  assert.equal(result.stale, false); assert.ok(Number.isSafeInteger(result.snapshotAt));
  assert.deepEqual(result.rows.map(item => item.id), ['a']);
});
test('timeout returns current unsettled snapshot without polling', async () => {
  const f = fixture([running('a')]); let summaries = 0; f.engine.summary = task => { summaries++; return structuredClone(task); };
  const rows = await f.registry.wait({ waitId: 'wait-timeout', ids: ['a'], timeoutMs: 2 });
  assert.equal(rows[0].settled, false); assert.ok(summaries <= 2); assert.equal(f.listeners.size, 0);
});
test('cancel affects only its waiter; deleted targets and shutdown reject explicitly', async () => {
  const f = fixture([running('a')]);
  const first = f.registry.wait({ waitId: 'wait-first', ids: ['a'], timeoutMs: 1000 }); const second = f.registry.wait({ waitId: 'wait-second', ids: ['a'], timeoutMs: 1000 });
  assert.deepEqual(f.registry.cancel('wait-first'), { waitId: 'wait-first', cancelled: true }); await assert.rejects(first, error => error.code === 'CANCELLED');
  f.engine.tasks.delete('a'); f.engine.publish(); await assert.rejects(second, error => error.code === 'NOT_FOUND');
  f.engine.tasks.set('a', running('a')); const closing = f.registry.wait({ waitId: 'wait-close', ids: ['a'], timeoutMs: 1000 });
  f.registry.close(); await assert.rejects(closing, error => error.code === 'CLOSED'); assert.equal(f.listeners.size, 0);
});
test('registration validates identity, selection, concurrency and duplicate wait IDs', async () => {
  const f = fixture([running('a')]);
  await assert.rejects(f.registry.wait({ waitId: 'bad id', ids: ['a'] }), /Invalid waitId/);
  await assert.rejects(f.registry.wait({ waitId: 'empty', ids: [] }), /1\.\.8/);
  await assert.rejects(f.registry.wait({ waitId: 'duplicate-target', ids: ['a', 'a'] }), /unique/);
  await assert.rejects(f.registry.wait({ waitId: 'missing-target', ids: ['missing'] }), error => error.code === 'NOT_FOUND');
  const waits = Array.from({ length: 16 }, (_, i) => f.registry.wait({ waitId: `wait-${i}`, ids: ['a'], timeoutMs: 1000 }));
  await assert.rejects(f.registry.wait({ waitId: 'wait-overflow', ids: ['a'], timeoutMs: 1000 }), error => error.code === 'BUSY');
  await assert.rejects(f.registry.wait({ waitId: 'wait-0', ids: ['a'], timeoutMs: 1000 }), error => error.code === 'CONFLICT');
  f.registry.close(); await Promise.allSettled(waits);
});
test('summary failures reject and unregister the waiter', async () => {
  const f = fixture([running('a')]); f.engine.summary = () => { throw new Error('summary failed'); };
  await assert.rejects(f.registry.wait({ waitId: 'wait-summary-error', ids: ['a'], timeoutMs: 1000 }), /summary failed/);
  assert.equal(f.listeners.size, 0); assert.equal(f.registry.waiters.size, 0);
});

test('omitted timeout has no timer and wakes only for events, cancel or shutdown', async t => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'] });
  const f = fixture([running('a')]);
  const waiting = f.registry.wait({ waitId: 'unbounded', ids: ['a'], wakeOnMessage: true, includeMetadata: true });
  assert.equal(f.registry.waiters.get('unbounded').timer, null);
  let resolved = false; waiting.then(() => { resolved = true; });
  t.mock.timers.tick(600_000); await Promise.resolve();
  assert.equal(resolved, false); assert.equal(f.listeners.size, 1);
  f.engine.tasks.get('a').recentMessages.push({ artifactId: 'question', kind: 'question' }); f.engine.publish();
  assert.equal((await waiting).reason, 'attention'); assert.equal(f.listeners.size, 0);
  const cancelled = f.registry.wait({ waitId: 'cancel-unbounded', ids: ['a'] });
  f.registry.cancel('cancel-unbounded'); await assert.rejects(cancelled, error => error.code === 'CANCELLED');
  assert.equal(f.engine.tasks.get('a').state, 'running');
  const closing = f.registry.wait({ waitId: 'close-unbounded', ids: ['a'] });
  f.registry.close(); await assert.rejects(closing, error => error.code === 'CLOSED');
  assert.equal(f.listeners.size, 0); assert.equal(f.registry.waiters.size, 0);
});

test('timer-free all/any waits still require settlement and return requested order', async () => {
  const f = fixture([running('a'), running('b')]);
  const all = f.registry.wait({ waitId: 'all', ids: ['a', 'b'], includeMetadata: true });
  const any = f.registry.wait({ waitId: 'any', ids: ['b', 'a'], mode: 'any', includeMetadata: true });
  Object.assign(f.engine.tasks.get('a'), { state: 'failed', settled: false }); f.engine.publish();
  assert.equal(f.registry.waiters.size, 2, 'terminal state before slot release is not settlement');
  f.engine.tasks.get('a').settled = true; f.engine.publish();
  assert.deepEqual((await any).rows.map(row => row.id), ['b', 'a']);
  assert.equal(f.registry.waiters.size, 1);
  Object.assign(f.engine.tasks.get('b'), { state: 'idle', settled: true }); f.engine.publish();
  assert.equal((await all).conditionMet, true); assert.equal(f.registry.waiters.size, 0);
});

test('all wait returns a settled actionable failure before unrelated work finishes', async () => {
  const f = fixture([running('a'), running('b')]);
  const waiting = f.registry.wait({ waitId: 'failure', ids: ['a', 'b'], wakeOnMessage: true, includeMetadata: true });
  Object.assign(f.engine.tasks.get('a'), { state: 'failed', settled: false, currentReport: 'failure-report' }); f.engine.publish();
  assert.equal(f.registry.waiters.size, 1, 'durable result and released slot are required');
  f.engine.tasks.get('a').settled = true; f.engine.publish();
  const result = await waiting;
  assert.equal(result.reason, 'attention'); assert.equal(result.conditionMet, false);
  assert.equal(result.rows[1].state, 'running');
  const seenWait = f.registry.wait({ waitId: 'seen-failure', ids: ['a', 'b'], wakeOnMessage: true, seenMessages: ['failure-report'], includeMetadata: true });
  assert.equal(f.registry.waiters.size, 1, 'a read failure report does not repeatedly interrupt all wait');
  f.registry.cancel('seen-failure'); await assert.rejects(seenWait, { code: 'CANCELLED' });
});

test('client snapshot uses the same actionable failure semantics as the host registry', async () => {
  const client = new HostClient({ cwd: '/fixture', agentDir: '/fixture', mainSessionId: 'fixture' });
  let rpc = 0;
  const rows = [{ ...running('a'), state: 'failed', settled: true }, running('b')];
  client.request = async operation => { rpc++; assert.equal(operation, 'status'); return { tasks: rows }; };
  const result = await client.wait(['a', 'b'], { wakeOnMessage: true, includeMetadata: true });
  assert.equal(result.reason, 'attention'); assert.equal(result.conditionMet, false); assert.equal(rpc, 1);
});

test('explicit steer wakes an all wait while ordinary progress does not', async () => {
  const f = fixture([running('a'), running('b')]);
  const waiting = f.registry.wait({ waitId: 'steer', ids: ['a', 'b'], wakeOnMessage: true, includeMetadata: true });
  f.engine.tasks.get('a').recentMessages.push({ artifactId: 'progress', kind: 'finding', delivery: 'followUp' }); f.engine.publish();
  assert.equal(f.registry.waiters.size, 1);
  f.engine.tasks.get('a').recentMessages.push({ artifactId: 'steer', kind: 'finding', delivery: 'steer' }); f.engine.publish();
  const result = await waiting;
  assert.equal(result.reason, 'attention'); assert.equal(result.conditionMet, false);
});
