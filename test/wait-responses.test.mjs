import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { RunStore } from '../src/storage.mjs';
import { writeResponse, retainResponse, readAgentResponse } from '../src/responses.mjs';
import { receiveWaitResponses, WAIT_RESPONSE_BYTES } from '../src/wait-responses.mjs';
import { fakeConfig, responseTask } from './support.mjs';

async function setup(t) {
  const root = await mkdtemp(join(tmpdir(), 'pi-wait-response-'));
  const store = new RunStore(join(root, 'run'), fakeConfig(root).limits); await store.init();
  t.after(async () => { await rm(root, { recursive: true, force: true }); });
  const save = async (id, result = 'full response', state = 'idle', error = '') => {
    const task = responseTask(id, root, { state, result, error });
    const entry = await writeResponse(store, id, 'report', { agentId: id, state, result, error, savedAt: Date.now() });
    retainResponse(task, entry); await store.saveTask(task);
    return { agentId: id, state, settled: true, latestReport: entry.artifactId, currentReport: entry.artifactId, errorAvailable: Boolean(error) };
  };
  return { root: store.root, store, save };
}

test('wait returns complete ordinary responses and failure errors directly from immutable snapshots', async t => {
  const f = await setup(t), a = await f.save('a', '가나다 exact final result'), b = await f.save('b', 'partial', 'failed', 'specific failure');
  const out = await receiveWaitResponses(f.root, [a, b]);
  assert.equal(out.rows[0].response.text, '가나다 exact final result'); assert.equal(out.rows[0].response.eof, true);
  assert.equal(out.rows[1].response.source, 'error'); assert.equal(out.rows[1].response.text, 'specific failure');
  assert.deepEqual(out.received, [a.latestReport, b.latestReport]);
  assert.equal(out.rows[0].response.read.requests[0].artifactId, a.latestReport);
  await f.save('a', 'newer run result');
  assert.equal((await receiveWaitResponses(f.root, [a])).rows[0].response.text, '가나다 exact final result', 'a concurrent resume cannot replace the snapshot wait observed');
});

test('timeout/running rows never read an old report; missing/stale/corrupt outcomes stay explicit and do not count received', async t => {
  const f = await setup(t), a = await f.save('a'), b = await f.save('b');
  const running = { ...a, state: 'running', settled: false };
  assert.deepEqual(await receiveWaitResponses('/missing-run', [running]), { rows: [running], received: [] });
  await rm(join(f.root, 'artifacts', `${b.latestReport}.bin`));
  const out = await receiveWaitResponses(f.root, [{ ...a, state: 'interrupted' }, { ...a, currentReport: null }, b, { agentId: 'cancelled', state: 'cancelled', settled: true }]);
  assert.equal(out.received.length, 0);
  for (const row of out.rows) { assert.equal(row.response, undefined); assert.ok(row.responseError); assert.equal(row.read.action, 'read'); }
  const controller = new AbortController(); controller.abort(new Error('cancel wait'));
  await assert.rejects(receiveWaitResponses(f.root, [a], { signal: controller.signal }), /cancel wait/);
});

test('eight large responses obey a combined encoded32KiB budget and expose exact continuation offsets', async t => {
  const f = await setup(t), rows = [];
  for (let n = 0; n < 8; n++) rows.push(await f.save(`agent-${n}`, '\u0000'.repeat(24000)));
  const out = await receiveWaitResponses(f.root, rows);
  assert.equal(out.received.length, 8);
  assert.ok(out.rows.reduce((bytes, row) => bytes + Buffer.byteLength(JSON.stringify(row.response)), 0) <= WAIT_RESPONSE_BYTES);
  for (const row of out.rows) {
    assert.equal(row.response.eof, false); assert.equal(row.response.bytesRead, row.response.text.length);
    const rest = await readAgentResponse(f.root, row.response.read.requests[0]);
    assert.equal(row.response.text + rest.text, '\u0000'.repeat(24000));
  }
});

const receiptFor = response => ({
  agentId: response.agentId, artifactId: response.artifactId, state: response.state,
  source: response.source, nextByteOffset: response.nextByteOffset, eof: response.eof
});

test('new mode returns a pinned receipt marker without rereading a complete response', async t => {
  const f = await setup(t), row = await f.save('seen', 'already delivered');
  const first = await receiveWaitResponses(f.root, [row]);
  const receipts = new Map([[`${row.latestReport}:result`, receiptFor(first.rows[0].response)]]);
  await rm(join(f.root, 'artifacts', `${row.latestReport}.bin`));
  const out = await receiveWaitResponses(f.root, [row], { receipts });
  assert.deepEqual(out.received, []);
  assert.equal(out.rows[0].response, undefined);
  assert.equal(out.rows[0].responseReceived, true);
  assert.equal(out.rows[0].responseComplete, true);
  assert.deepEqual(out.rows[0].read, {
    action: 'read', requests: [{
      agentId: 'seen', source: 'result', artifactId: row.latestReport,
      byteOffset: 0, limitBytes: 32768
    }]
  });
  assert.equal(receipts.size, 1, 'caller receipt state is not mutated');
});

test('all rereads explicitly, none performs no I/O, and invalid current reports remain explicit', async t => {
  const f = await setup(t), allRow = await f.save('all', 'read again'), noneRow = await f.save('none', 'do not read');
  const first = await receiveWaitResponses(f.root, [allRow]);
  const receipts = new Map([[`${allRow.latestReport}:result`, receiptFor(first.rows[0].response)]]);
  const all = await receiveWaitResponses(f.root, [allRow], { results: 'all', receipts });
  assert.equal(all.rows[0].response.text, 'read again');
  assert.deepEqual(all.received, [allRow.latestReport]);

  await rm(join(f.root, 'artifacts', `${noneRow.latestReport}.bin`));
  const none = await receiveWaitResponses(f.root, [noneRow], { results: 'none', receipts });
  assert.deepEqual(none, { rows: [noneRow], received: [] });
  const invalid = await receiveWaitResponses(f.root, [{ ...noneRow, currentReport: null }], { results: 'none', receipts });
  assert.match(invalid.rows[0].responseError, /No immutable report/);
  assert.equal(invalid.rows[0].read.requests[0].artifactId, noneRow.latestReport);
});

test('partial receipts preserve continuation offsets while new artifacts and mismatched receipts are read', async t => {
  const f = await setup(t), row = await f.save('partial', 'x'.repeat(60000));
  const first = await receiveWaitResponses(f.root, [row]);
  assert.equal(first.rows[0].response.eof, false);
  const receipt = receiptFor(first.rows[0].response);
  const receipts = new Map([[`${row.latestReport}:result`, receipt]]);
  const seen = await receiveWaitResponses(f.root, [row], { receipts });
  assert.equal(seen.rows[0].responseReceived, true);
  assert.equal(seen.rows[0].responseComplete, false);
  assert.equal(seen.rows[0].read.requests[0].byteOffset, receipt.nextByteOffset);

  const newer = await f.save('partial', 'new attempt');
  const fresh = await receiveWaitResponses(f.root, [newer], { receipts });
  assert.equal(fresh.rows[0].response.text, 'new attempt');
  const mismatch = new Map([[`${newer.latestReport}:result`, { ...receiptFor(fresh.rows[0].response), source: 'error' }]]);
  const reread = await receiveWaitResponses(f.root, [newer], { receipts: mismatch });
  assert.equal(reread.rows[0].response.text, 'new attempt');
});

test('response budget is shared only by unread candidates', async t => {
  const f = await setup(t), rows = [];
  for (let n = 0; n < 8; n++) rows.push(await f.save(`mixed-${n}`, 'z'.repeat(50000)));
  const receipts = new Map();
  for (const row of rows.slice(0, 7)) receipts.set(`${row.latestReport}:result`, {
    agentId: row.agentId, artifactId: row.latestReport, state: row.state,
    source: 'result', nextByteOffset: 1234, eof: false
  });
  const out = await receiveWaitResponses(f.root, rows, { receipts });
  assert.equal(out.rows.filter(row => row.responseReceived).length, 7);
  assert.equal(out.received.length, 1);
  assert.ok(out.rows[7].response.bytesRead > 20000, 'seen rows do not shrink the only new page');
  assert.ok(Buffer.byteLength(JSON.stringify(out.rows[7].response)) <= WAIT_RESPONSE_BYTES);
});
