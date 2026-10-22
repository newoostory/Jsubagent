import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import extension from '../index.js';
import { HostClient } from '../src/client.mjs';
import { closeProcessSessionHosts, processSessionHostCount, rememberSessionHost } from '../src/session-hosts.mjs';
import { processIdentity } from '../src/runtime/ownership.mjs';

test('quit closes current and process-owned detached hosts while replacement reasons only detach', async t => {
  const root = await mkdtemp(join(tmpdir(), 'pi-session-shutdown-'));
  const priorAgentDir = process.env.PI_CODING_AGENT_DIR;
  process.env.PI_CODING_AGENT_DIR = root;
  const hooks = new Map(), tools = new Map(), entries = [];
  const calls = { close: 0, detach: 0 };
  let connectError;
  t.mock.method(globalThis, 'setInterval', () => ({ unref() {} }));
  t.mock.method(globalThis, 'clearInterval', () => {});
  let hostIdentity = { pid: process.pid, generation: 1, nonce: 'shutdown-fixture', started: 'fixture-start' };
  t.mock.method(HostClient.prototype, 'start', async function () { this.info = { identity: hostIdentity }; });
  t.mock.method(HostClient.prototype, 'connectOnce', async function () { if (connectError) throw Object.assign(new Error(connectError), { code: connectError }); this.info = { identity: hostIdentity }; return this.info; });
  t.mock.method(HostClient.prototype, 'request', async op => {
    if (op === 'status') return { tasks: [], active: 0, concurrency: 8 };
    throw new Error(`unexpected ${op}`);
  });
  t.mock.method(HostClient.prototype, 'detach', async function () { calls.detach++; this.closed = true; });
  t.mock.method(HostClient.prototype, 'close', async function () { calls.close++; this.closed = true; return { saved: true, executorsStopped: true, stopped: true }; });
  const pi = {
    on(name, handler) { hooks.set(name, handler); },
    registerTool(tool) { tools.set(tool.name, tool); }, registerCommand() {}, registerMessageRenderer() {},
    getActiveTools: () => ['agent'], getAllTools: () => [{ name: 'agent' }], setActiveTools() {},
    appendEntry: (customType, data) => entries.push({ type: 'custom', customType, data }),
  };
  const sessionManager = {
    getSessionId: () => 'shutdown-session', getBranch: () => entries, getEntries: () => entries,
    getSessionFile: () => undefined, buildSessionProjection: () => ({ messages: [] }),
  };
  const ctx = { cwd: root, hasUI: false, isProjectTrusted: () => false, isIdle: () => true, sessionManager };
  const startAndConnect = async () => {
    await hooks.get('session_start')({ type: 'session_start' }, ctx);
    await tools.get('agent').execute('status', { action: 'status' }, undefined, undefined, ctx);
  };
  try {
    extension(pi);
    for (const reason of ['reload', 'new', 'resume', 'fork']) {
      await startAndConnect();
      await hooks.get('session_shutdown')({ type: 'session_shutdown', reason }, ctx);
    }
    assert.deepEqual(calls, { close: 0, detach: 4 });
    await startAndConnect();
    await hooks.get('session_shutdown')({ type: 'session_shutdown', reason: 'quit' }, ctx);
    assert.deepEqual(calls, { close: 1, detach: 4 });

    // Tree navigation forgets the current client in the extension instance, but
    // the process registry must still reconnect without spawning and close it.
    await startAndConnect();
    await hooks.get('session_tree')({ type: 'session_tree', newLeafId: 'next', oldLeafId: 'old' }, ctx);
    assert.deepEqual(calls, { close: 1, detach: 5 });
    await hooks.get('session_shutdown')({ type: 'session_shutdown', reason: 'quit' }, ctx);
    assert.deepEqual(calls, { close: 2, detach: 5 });

    // Extension reload replaces every closure; the global Symbol registry is
    // the only owner that survives and must close the detached prior run.
    await startAndConnect();
    await hooks.get('session_shutdown')({ type: 'session_shutdown', reason: 'reload' }, ctx);
    assert.deepEqual(calls, { close: 2, detach: 6 });
    extension(pi);
    await hooks.get('session_start')({ type: 'session_start' }, ctx);
    await hooks.get('session_shutdown')({ type: 'session_shutdown', reason: 'quit' }, ctx);
    assert.deepEqual(calls, { close: 3, detach: 6 });

    const detached = rootName => ({ options: { root: join(root, rootName), cwd: root, agentDir: root, owner: 'fixture', trusted: false }, info: { identity: hostIdentity } });
    rememberSessionHost(detached('already-gone'));
    connectError = 'ECONNREFUSED'; await closeProcessSessionHosts();
    assert.equal(processSessionHostCount(), 0, 'an already-gone owned host is cleared');

    const exited = { ...detached('current-already-gone'), exited: true, close: async () => ({ stopped: false }) };
    rememberSessionHost(exited);
    await closeProcessSessionHosts(exited);
    assert.equal(processSessionHostCount(), 0, 'an already-gone current host is reconciled through its verified binding');

    rememberSessionHost(detached('unknown-failure'));
    connectError = 'EACCES';
    await assert.rejects(closeProcessSessionHosts(), /Unable to confirm all owned supervisor shutdowns/);
    assert.equal(processSessionHostCount(), 1, 'unknown shutdown failure retains ownership for inspection/retry');
    connectError = 'ENOENT'; await closeProcessSessionHosts();
    assert.equal(processSessionHostCount(), 0);

    // A refused socket does not prove the recorded process has stopped.
    hostIdentity = { pid: process.pid, generation: 1, nonce: 'live-fixture', started: await processIdentity(process.pid) };
    rememberSessionHost(detached('socket-gone-process-alive'));
    await assert.rejects(closeProcessSessionHosts(), /remains alive/);
    assert.equal(processSessionHostCount(), 1, 'live recorded process must retain shutdown ownership');
    connectError = undefined; await closeProcessSessionHosts();
    assert.equal(processSessionHostCount(), 0);
  } finally {
    if (priorAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = priorAgentDir;
    await rm(root, { recursive: true, force: true });
  }
});
