import test from 'node:test';
import assert from 'node:assert/strict';
import { HostClient } from '../src/client.mjs';

const client = () => new HostClient({ cwd: '/fixture', agentDir: '/fixture', mainSessionId: 'wait' });
test('wait uses one host wait request and forwards event-wake options', async () => {
  const host = client(), calls = [];
  host.request = async (...args) => { calls.push(args); return args[0] === 'status' ? { tasks: [{ id: 'a', state: 'running', settled: false }] } : [{ id: 'a', state: 'idle', settled: true }]; };
  const rows = await host.wait(['a'], { timeoutMs: 1000, mode: 'any', wakeOnMessage: true, seenMessages: ['artifact-a'] });
  assert.deepEqual(rows, [{ id: 'a', state: 'idle', settled: true }]);
  assert.deepEqual(calls.map(call => call[0]), ['status', 'wait']);
  assert.deepEqual(calls[1][1], { waitId: calls[1][1].waitId, ids: ['a'], timeoutMs: calls[1][1].timeoutMs, deadline: calls[1][1].deadline, mode: 'any', wakeOnMessage: true, seenMessages: ['artifact-a'] });
  assert.ok(calls[1][1].timeoutMs <= 1000); assert.equal(calls[0][1].ids[0], 'a');
  assert.ok(calls[1][1].deadline >= Date.now() - 1000); assert.ok(calls[1][2] > calls[1][1].timeoutMs);
});
test('metadata describes an initial condition or attention wake without another RPC', async () => {
  const condition = client(), conditionCalls = [];
  condition.request = async (op, args) => { conditionCalls.push({ op, args }); return { tasks: [{ id: 'a', state: 'idle', settled: true, recentMessages: [] }] }; };
  const completed = await condition.wait(['a'], { mode: 'all', includeMetadata: true });
  assert.equal(completed.reason, 'condition'); assert.equal(completed.conditionMet, true); assert.equal(completed.stale, false);
  assert.deepEqual(conditionCalls.map(call => call.op), ['status']);

  const attention = client(), attentionCalls = [];
  attention.request = async (op, args) => { attentionCalls.push({ op, args }); return { tasks: [{ id: 'a', state: 'idle', settled: true, recentMessages: [{ artifactId: 'new-question', kind: 'question' }] }] }; };
  const noticed = await attention.wait(['a'], { mode: 'all', wakeOnMessage: true, includeMetadata: true });
  assert.equal(noticed.reason, 'attention'); assert.equal(noticed.conditionMet, true); assert.equal(noticed.stale, false);
  assert.deepEqual(attentionCalls.map(call => call.op), ['status']);
});
test('aborting wait cancels only its host waiter and retains the original reason', async () => {
  const host = client(), calls = [], controller = new AbortController();
  host.request = (op, args, _timeout, signal) => {
    calls.push({ op, args });
    if (op === 'status') return Promise.resolve({ tasks: [{ id: 'a', state: 'running', settled: false }] });
    if (op === 'wait_cancel') return Promise.resolve({ cancelled: true });
    return new Promise((_resolve, reject) => signal.addEventListener('abort', () => reject(signal.reason), { once: true }));
  };
  const waiting = host.wait(['a'], { signal: controller.signal }); await Promise.resolve(); await Promise.resolve(); controller.abort(new Error('stop waiting'));
  await assert.rejects(waiting, /stop waiting/); await new Promise(resolve => setImmediate(resolve));
  assert.deepEqual(calls.map(call => call.op), ['status', 'wait', 'wait_cancel']); assert.equal(calls[2].args.waitId, calls[1].args.waitId);
});
test('pre-aborted and closed clients send no wait or cancellation request', async () => {
  const host = client(), calls = []; host.request = async (...args) => { calls.push(args); return []; };
  const controller = new AbortController(); controller.abort(new Error('already stopped'));
  await assert.rejects(host.wait(['a'], { signal: controller.signal }), /already stopped/); assert.equal(calls.length, 0);
  const closed = client(); closed.closed = true; await assert.rejects(closed.wait(['a']), /Host not running/);
});
test('request abort removes its pending entry and ignores a late host reply', async () => {
  const host = client(), sent = []; host.writer = { send: async packet => { sent.push(packet); } };
  host.starting = Promise.resolve(); host.connected = true;
  const controller = new AbortController();
  const request = host.request('wait', { waitId: 'wait-a', ids: ['a'], timeoutMs: 10_000 }, 10_000, controller.signal);
  controller.abort(new Error('local wait ended')); await assert.rejects(request, /local wait ended/);
  assert.equal(host.pending.size, 0); assert.equal(sent.length, 1);
});
test('wait validates mode and strict server timeout bounds before transport', async () => {
  const host = client(), calls = []; host.request = async (...args) => { calls.push(args); return []; };
  await assert.rejects(host.wait(['a'], { mode: 'first' }), /all or any/);
  await assert.rejects(host.wait(['a'], { timeoutMs: -1 }), /0\.\.120000/);
  await assert.rejects(host.wait(['a'], { timeoutMs: 120_001 }), /0\.\.120000/); assert.equal(calls.length, 0);
});
test('a stalled initial snapshot obeys a tiny deadline with no timeout floor', async () => {
  const host = client(), budgets = [];
  host.request = (_op, _args, timeoutMs, signal) => { budgets.push(timeoutMs); return new Promise((_resolve, reject) => signal.addEventListener('abort', () => reject(signal.reason), { once: true })); };
  const started = Date.now(); await assert.rejects(host.wait(['a'], { timeoutMs: 2 }), error => error.code === 'TIMEOUT');
  assert.ok(Date.now() - started < 100); assert.ok(budgets.every(ms => ms <= 2));
});
test('transport timeout after a valid snapshot returns that snapshot and clears the host waiter', async () => {
  const host = client(), calls = [];
  host.request = async (op, args) => {
    calls.push({ op, args });
    if (op === 'status') return { tasks: [{ id: 'a', state: 'running', settled: false }] };
    if (op === 'wait') throw Object.assign(new Error('rpc deadline'), { code: 'TIMEOUT' });
    return { cancelled: true };
  };
  const rows = await host.wait(['a'], { timeoutMs: 1000 }); await Promise.resolve();
  assert.equal(rows[0].settled, false); assert.deepEqual(calls.map(call => call.op), ['status', 'wait', 'wait_cancel']);
  assert.equal(calls[2].args.waitId, calls[1].args.waitId);
});
test('metadata marks a transport-timeout fallback as stale', async () => {
  const host = client(), calls = [];
  host.request = async (op, args) => {
    calls.push({ op, args });
    if (op === 'status') return { tasks: [{ id: 'a', state: 'running', settled: false }] };
    if (op === 'wait') throw Object.assign(new Error('rpc deadline'), { code: 'TIMEOUT' });
    return { cancelled: true };
  };
  const result = await host.wait(['a'], { timeoutMs: 1000, includeMetadata: true }); await Promise.resolve();
  assert.equal(result.reason, 'transport_timeout'); assert.equal(result.conditionMet, false);
  assert.equal(result.stale, true); assert.ok(Number.isSafeInteger(result.snapshotAt));
  assert.equal(result.rows[0].settled, false);
  assert.equal(calls[1].args.includeMetadata, true);
});

test('omitted timeout registers one timer-free RPC beyond the former maximum window', async t => {
  t.mock.timers.enable({ apis: ['setTimeout', 'Date'] });
  const host = client(), calls = [];
  host.writer = { send: async frame => { calls.push(frame); } };
  host.starting = Promise.resolve(); host.connected = true;
  const request = host.request.bind(host);
  host.request = (op, args, ...rest) => op === 'status' ? Promise.resolve({ tasks: [{ id: 'a', state: 'running', settled: false }] }) : request(op, args, ...rest);
  const controller = new AbortController();
  const waiting = host.wait(['a'], { signal: controller.signal, includeMetadata: true });
  await Promise.resolve(); await Promise.resolve();
  assert.equal(calls.length, 1); assert.equal(calls[0].op, 'wait');
  assert.equal(Object.hasOwn(calls[0].args, 'timeoutMs'), false);
  assert.equal(Object.hasOwn(calls[0].args, 'deadline'), false);
  assert.equal(host.pending.size, 1); assert.equal([...host.pending.values()][0].timer, null);
  let settled = false; waiting.then(() => { settled = true; }, () => { settled = true; });
  t.mock.timers.tick(600_000); await Promise.resolve();
  assert.equal(settled, false); assert.equal(calls.length, 1);
  // A real response still resolves and cleans the request without a time budget.
  const entry = host.pending.get(calls[0].id); host.pending.delete(calls[0].id);
  entry.resolve({ rows: [{ id: 'a', state: 'idle', settled: true }], reason: 'condition', conditionMet: true, stale: false });
  assert.equal((await waiting).conditionMet, true); assert.equal(host.pending.size, 0);
  controller.abort(); assert.equal(calls.length, 1, 'no late cancellation after completion');
});

test('host loss rejects a timer-free wait and ordinary RPCs keep their deadline', async t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const host = client(); host.writer = { send: async () => {} };
  host.starting = Promise.resolve(); host.connected = true;
  const waiting = host.request('wait', { waitId: 'unbounded', ids: ['a'] }, null);
  const lost = Object.assign(new Error('host exited'), { code: 'HOST_EXIT' });
  host.exited = true; host.rejectAll(lost);
  await assert.rejects(waiting, error => error === lost); assert.equal(host.pending.size, 0);
  host.exited = false;
  const ordinary = host.request('status', {}, 50);
  t.mock.timers.tick(50);
  await assert.rejects(ordinary, error => error.code === 'TIMEOUT'); assert.equal(host.pending.size, 0);
});
