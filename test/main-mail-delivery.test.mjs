import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import extension from '../index.js';
import { HostClient } from '../src/client.mjs';
import { mailReference } from '../src/main-mail.mjs';

const mail = (n, delivery = 'followUp') => ({ id: `mail-${n}`, from: 'scout', kind: 'result', event: 'completion', state: 'idle', reportArtifactId: `body-${n}`, delivery, expiresAt: Date.now() + 60000 });
const intermediate = (n, kind = 'finding', delivery = 'followUp') => ({ ...mail(n), event: undefined, kind, delivery, reportArtifactId: undefined, bodyArtifactId: `body-${n}` });
async function harness(t) {
  const root = await mkdtemp(join(tmpdir(), 'pi-main-mail-'));
  const old = process.env.PI_CODING_AGENT_DIR; process.env.PI_CODING_AGENT_DIR = root;
  const hooks = new Map(), tools = new Map(), entries = [], sent = [], acked = [], warnings = [];
  let tick, inbox = [], busy = true, active = true, failAck = false, persistent = false;
  t.mock.method(globalThis, 'setInterval', fn => { tick = fn; return { unref() {} }; });
  t.mock.method(globalThis, 'clearInterval', () => {});
  t.mock.method(HostClient.prototype, 'start', async function () { this.info = { identity: { pid: 101, generation: 1, nonce: 'mail-fixture', started: 'fixture-start' } }; });
  t.mock.method(HostClient.prototype, 'monitor', async () => {});
  t.mock.method(HostClient.prototype, 'request', async (op, args) => {
    if (op === 'status') return { tasks: [], active: 0, concurrency: 8 };
    if (op === 'set_mode') return {};
    if (op === 'inbox') return structuredClone(inbox);
    if (op === 'ack') {
      if (failAck) throw new Error('injected ack persistence failure');
      acked.push(...args.ids); inbox = inbox.filter(m => !args.ids.includes(m.id)); return {};
    }
    throw new Error(`unexpected operation ${op}`);
  });
  const pi = {
    on(name, fn) {
      const prior = hooks.get(name);
      hooks.set(name, async (event, ctx) => {
        const first = await prior?.(event, ctx);
        const next = await fn(first ? { ...event, ...first } : event, ctx);
        return next === undefined ? first : { ...first, ...next };
      });
    },
    registerTool: tool => tools.set(tool.name, tool), registerCommand() {}, registerMessageRenderer() {},
    getActiveTools: () => active ? ['agent'] : [],
    appendEntry: (customType, data) => entries.push({ type: 'custom', customType, data }),
    sendMessage(message, options) { sent.push({ message, options }); },
  };
  const ctx = { cwd: root, hasUI: true, isIdle: () => !busy, isProjectTrusted: () => false,
    sessionManager: { getSessionId: () => 'mail-session', getBranch: () => entries, getEntries: () => entries, getSessionFile: () => persistent ? join(root, 'session.jsonl') : undefined,
      buildSessionProjection: () => ({ messages: entries.flatMap(e => e.type === 'message' ? [e.message] : e.type === 'custom_message' ? [{ ...e, role: 'custom', timestamp: e.timestamp ?? 2 }] : []) }) },
    ui: { notify: text => warnings.push(text), setWidget() {}, setStatus() {} } };
  extension(pi);
  const emit = name => hooks.get(name)?.({}, ctx);
  const connect = () => tools.get('agent').execute('mail-test', { action: 'status' }, undefined, undefined, ctx);
  t.after(async () => { await emit('session_shutdown'); if (old === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = old; await rm(root, { recursive: true, force: true }); });
  await emit('session_start'); await connect();
  return { sent, entries, acked, warnings, emit, connect,
    set inbox(value) { inbox = value; }, get inbox() { return inbox; },
    set busy(value) { busy = value; }, set active(value) { active = value; }, set failAck(value) { failAck = value; }, set persistent(value) { persistent = value; },
    async poll() { tick(); await new Promise(resolve => setImmediate(resolve)); },
    persist() { entries.push({ type: 'custom_message', ...sent.at(-1).message }); },
    context: messages => hooks.get('context')({ messages }, ctx),
  };
}

test('completions enter one next-boundary batch while routine progress stays stored-only', async t => {
  const h = await harness(t); h.inbox = Array.from({ length: 12 }, (_, n) => intermediate(n));
  await h.poll(); await h.poll(); assert.equal(h.sent.length, 0); assert.equal(h.inbox.length, 0); assert.equal(h.acked.length, 12);
  h.inbox = [mail(12), mail(13), mail(14, 'steer')];
  await h.poll(); await h.poll(); await h.poll();
  assert.equal(h.sent.length, 1); assert.equal(h.sent[0].options.triggerTurn, false);
  assert.equal(h.sent[0].options.deliverAs, 'nextTurn');
  assert.deepEqual(h.sent[0].message.details.messageIds, ['mail-12', 'mail-13', 'mail-14']);
  assert.equal(h.sent[0].message.display, false); assert.equal(h.acked.length, 12);
  const user = { role: 'user', content: 'ordinary request', timestamp: 1 };
  h.entries.push({ type: 'message', message: user }); h.persist();
  const current = await h.context([user]);
  assert.equal(current.messages[0], user);
  assert.equal(current.messages.at(-1).details.references.length, 3, 'persisted completion joins a running loop snapshot that lacks the custom entry');
  const assistant = { role: 'assistant', timestamp: 3, content: [] };
  h.entries.push({ type: 'message', message: assistant });
  const later = await h.context([user, assistant]);
  assert.deepEqual(later.messages.slice(0, current.messages.length), current.messages, 'stale SDK snapshots repair at the same durable boundary on later requests');
  h.failAck = true; await h.poll();
  assert.equal(h.inbox.length, 3); assert.equal(h.sent.length, 1);
  h.failAck = false; await h.poll(); assert.equal(h.acked.length, 15); assert.equal(h.inbox.length, 0);
  h.busy = false; await h.poll(); assert.equal(h.sent.length, 1);
  assert.equal(h.entries.filter(e => e.customType === 'pi-agent-delivered').length, 1);
  h.entries.push({ type: 'custom', customType: 'pi-agent-read-artifacts', data: { ids: ['body-12'] } });
  await h.emit('session_shutdown'); await h.emit('session_start'); await h.connect();
  assert.deepEqual((await h.context([user, assistant])).messages, later.messages, 'read receipts and reload do not rewrite the prefix');
});

test('an idle next-boundary batch waits without waking or periodic duplicate delivery', async t => {
  const h = await harness(t); h.inbox = [mail(1, 'steer')];
  await h.poll(); await h.poll(); assert.equal(h.sent.length, 1); assert.equal(h.acked.length, 0);
  // nextTurn intentionally stays queued until a user-started boundary.
  h.busy = false; await h.poll();
  assert.equal(h.sent.length, 1, 'idle polling must neither wake the model nor duplicate queued mail');
  assert.equal(h.acked.length, 0); h.persist(); await h.poll();
  assert.deepEqual(h.acked, ['mail-1']); assert.equal(h.inbox.length, 0);
});

test('questions, blockers and explicit steer reach the next model boundary; progress does not', async t => {
  const h = await harness(t);
  h.inbox = [
    intermediate(1, 'finding', 'followUp'),
    intermediate(2, 'answer', 'followUp'),
    intermediate(3, 'question', 'followUp'),
    intermediate(4, 'blocked', 'followUp'),
    intermediate(5, 'finding', 'steer'),
  ];
  await h.poll();
  assert.equal(h.sent.length, 1);
  assert.deepEqual(h.sent[0].options, { deliverAs: 'steer' });
  assert.deepEqual(h.sent[0].message.details.messageIds, ['mail-3', 'mail-4', 'mail-5']);
  assert.deepEqual(h.acked, ['mail-1', 'mail-2']);
  assert.equal(h.inbox.length, 3, 'attention mail remains durable until its notice reaches a model boundary');
  await h.poll(); assert.equal(h.sent.length, 1, 'pending next-boundary mail is not duplicated by polling');
  h.persist(); h.entries.push({ type: 'message', message: { role: 'assistant' } }); await h.poll();
  assert.deepEqual(new Set(h.acked), new Set(['mail-1', 'mail-2', 'mail-3', 'mail-4', 'mail-5']));
});

test('a steer dropped by abort is requeued once as idle nextTurn and remains durable until delivery', async t => {
  const h = await harness(t);
  h.inbox = [intermediate(1, 'question')];
  await h.poll();
  assert.equal(h.sent.length, 1); assert.deepEqual(h.sent[0].options, { deliverAs: 'steer' });
  assert.equal(h.acked.length, 0);

  // Model Pi interactive clearQueue + abort: the steer never enters session
  // history, and the main loop becomes idle. Polling must not wake the model;
  // it may only replace the lost steer with a queued nextTurn notice.
  h.busy = false; await h.poll();
  assert.equal(h.sent.length, 2);
  assert.deepEqual(h.sent[1].options, { triggerTurn: false, deliverAs: 'nextTurn' });
  assert.equal(h.acked.length, 0); assert.equal(h.inbox.length, 1);
  await h.poll(); await h.poll();
  assert.equal(h.sent.length, 2, 'legitimate idle nextTurn mail is not duplicated by polling');

  // A later user-started run persists the replacement exactly once.
  h.persist(); h.entries.push({ type: 'message', message: { role: 'assistant' } }); await h.poll();
  assert.deepEqual(h.acked, ['mail-1']); assert.equal(h.inbox.length, 0);
  await h.poll(); assert.equal(h.sent.length, 2);
});

test('a persisted steer awaiting the first durable assistant flush is not mistaken for an aborted steer', async t => {
  const h = await harness(t); h.persistent = true;
  h.inbox = [intermediate(1, 'blocked')];
  await h.poll(); h.persist(); h.busy = false;
  await h.poll(); await h.poll();
  assert.equal(h.sent.length, 1, 'in-memory persisted notice must not be requeued while awaiting JSONL flush');
  assert.equal(h.acked.length, 0);
  h.entries.push({ type: 'message', message: { role: 'assistant' } }); await h.poll();
  assert.deepEqual(h.acked, ['mail-1']);
});

test('reload restores delivered/read IDs; disabled tools retain unread mail; expiry does not delete artifacts', async t => {
  const h = await harness(t); h.inbox = [mail(1, 'steer'), mail(2), { ...mail(3), expiresAt: 0 }];
  await h.poll(); h.persist();
  h.entries.push({ type: 'custom', customType: 'pi-agent-read-artifacts', data: { ids: ['body-2'] } });
  await h.emit('session_shutdown'); await h.emit('session_start'); await h.connect();
  h.active = false; await h.poll();
  assert.equal(h.sent.length, 1); assert.equal(h.inbox.length, 0);
  assert.deepEqual(new Set(h.acked), new Set(['mail-1', 'mail-2', 'mail-3']));
  h.inbox = [mail(4)]; h.busy = false; await h.poll(); assert.equal(h.inbox.length, 1); assert.equal(h.sent.length, 1);
  h.active = true; await h.poll(); assert.equal(h.sent.length, 2);
  assert.ok(h.sent.every(item => item.options.triggerTurn === false && item.options.deliverAs === 'nextTurn'));
});

test('important mail while idle waits for the next user-started run', async t => {
  const h = await harness(t); h.busy = false;
  h.inbox = [intermediate(1, 'question')];
  await h.poll(); await h.poll();
  assert.equal(h.sent.length, 1);
  assert.deepEqual(h.sent[0].options, { triggerTurn: false, deliverAs: 'nextTurn' });
  assert.equal(h.acked.length, 0);
});

test('superseded completion is stored-only and a not-yet-settled current report waits without notifying', async t => {
  const h = await harness(t);
  h.inbox = [{ ...mail(1), superseded: true, completionReady: false }, { ...mail(2), completionReady: false }];
  await h.poll(); assert.deepEqual(h.acked, ['mail-1']); assert.equal(h.sent.length, 0);
  h.inbox[0].completionReady = true; await h.poll(); await h.poll();
  assert.equal(h.sent.length, 1); assert.deepEqual(h.sent[0].message.details.messageIds, ['mail-2']);
});

test('first custom message in a persistent Pi session is not acked before its initial assistant flush', async t => {
  const h = await harness(t); h.persistent = true; h.inbox = [mail(1, 'steer')];
  await h.poll(); h.persist(); // In Pi this exists in memory, but JSONL is not flushed yet.
  await h.poll(); await h.poll(); assert.equal(h.acked.length, 0); assert.equal(h.sent.length, 1);
  h.entries.push({ type: 'message', message: { role: 'assistant' } });
  await h.poll(); assert.deepEqual(h.acked, ['mail-1']);
});

test('legacy branch suppression survives a user-controlled reload after tree navigation', async t => {
  const h = await harness(t);
  const legacy = { role: 'custom', customType: 'pi-agent-mail', display: true, content: 'Team response notifications\n' + JSON.stringify([mailReference(mail(1))]) };
  assert.deepEqual((await h.context([legacy])).messages, [], 'legacy notices are unsupported even before tree navigation');
  await h.emit('session_tree'); assert.deepEqual((await h.context([legacy])).messages, []);
  await h.emit('session_shutdown'); await h.emit('session_start');
  assert.deepEqual((await h.context([legacy])).messages, [], 'reload must not admit notices from the old branch');
});
