import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, realpath, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager } from '@earendil-works/pi-coding-agent';
import extension from '../index.js';
import { providerFixture, eventually } from './support.mjs';
import { runRoot } from '../src/client.mjs';
import { readTaskRecord } from '../src/state/task-cache.mjs';

test('canonical send reaches the next child turn; aborting an unbounded wait does not cancel its child', { timeout: 30000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'pi-agent-control-'));
  let release, gated = false, session;
  const requests = [];
  const fixture = await providerFixture(root, { onRequest: data => requests.push(JSON.stringify(data)), gate(data, proceed) {
    if (!gated && JSON.stringify(data.messages).includes('fixture-control')) { gated = true; release = proceed; return true; }
    return false;
  } });
  const old = process.env.PI_CODING_AGENT_DIR; process.env.PI_CODING_AGENT_DIR = fixture.agentDir;
  try {
    const settingsManager = SettingsManager.inMemory({ compaction: { enabled: false } });
    const loader = new DefaultResourceLoader({ cwd: root, agentDir: fixture.agentDir, settingsManager, noExtensions: true, extensionFactories: [extension], noSkills: true, noContextFiles: true, noPromptTemplates: true, noThemes: true });
    await loader.reload(); assert.deepEqual(loader.getExtensions().errors, []);
    const runtime = await ModelRuntime.create({ authPath: join(fixture.agentDir, 'auth.json'), modelsPath: join(fixture.agentDir, 'models.json'), modelsStorePath: join(fixture.agentDir, 'models-store.json'), allowModelNetwork: false });
    ({ session } = await createAgentSession({ cwd: root, agentDir: fixture.agentDir, model: runtime.getModel(fixture.profile.provider, fixture.profile.model), modelRuntime: runtime, settingsManager, resourceLoader: loader, sessionManager: SessionManager.inMemory(root) }));
    await session.bindExtensions({ mode: 'print' });
    const runner = session.extensionRunner;
    const action = async (args, signal) => JSON.parse((await runner.getToolDefinition('agent').execute('control', args, signal, undefined, runner.createContext())).content[0].text);
    const agent = await action({ action: 'spawn', tasks: [{ role: 'scout', task: 'fixture-control fixture-read: inspect marker.txt' }] });
    assert.deepEqual(agent.results.map(row => row.index), [0]); assert.equal(agent.results[0].ok, true);
    const agentId = agent.agentIds[0];
    await eventually(() => release);
    const controller = new AbortController();
    const waiting = action({ action: 'wait', agentIds: [agentId] }, controller.signal);
    const abortTimer = setTimeout(() => controller.abort(new Error('stop waiting only')), 100);
    try { await assert.rejects(waiting, /stop waiting only/); } finally { clearTimeout(abortTimer); }
    const current = await action({ action: 'status', agentIds: [agentId] });
    assert.equal(current.agents[0].settled, false); assert.equal(current.agents[0].state, 'running');
    const steer = await action({ action: 'send', agentIds: [agentId], body: 'CONTROL_STEER_MARKER: report the bounded evidence', delivery: 'steer' });
    assert.deepEqual(steer.agentIds, [agentId]); assert.equal(steer.results[0].ok, true); assert.equal(steer.results[0].agentId, agentId); assert.ok(steer.results[0].messageId); assert.equal(requests.length, 1, 'steer cannot interrupt the held HTTP response');
    release(); release = undefined;
    assert.equal((await action({ action: 'wait', agentIds: [agentId] })).agents[0].state, 'idle');
    assert.ok(requests.slice(1).some(request => request.includes('CONTROL_STEER_MARKER')), 'next SDK turn receives steering');
    const hanging = await action({ action: 'spawn', tasks: [{ role: 'scout', task: 'fixture-hang' }] });
    const hangingId = hanging.agentIds[0];
    await eventually(() => requests.some(request => request.includes('fixture-hang')));
    const cancelled = await action({ action: 'cancel', agentIds: [hangingId] });
    assert.deepEqual(cancelled.agentIds, [hangingId]); assert.equal(cancelled.results[0].agentId, hangingId); assert.equal(cancelled.results[0].cancellationRequested, true);
    const done = (await action({ action: 'wait', agentIds: [hangingId] })).agents[0];
    assert.equal(done.state, 'cancelled'); assert.equal(done.settled, true);
    // Terminal cancellation through the agent tool is a no-op.
    await action({ action: 'cancel', agentIds: [agentId] });
    const terminal = (await action({ action: 'status', agentIds: [agentId], format: 'full' })).agents[0];
    assert.equal(terminal.state, 'idle'); assert.equal(terminal.settled, true);

    // Unlike a wait abort, quitting Pi must stop even a tree-detached active
    // supervisor and retain its durable checkpoint for inspection/recovery.
    const atQuit = await action({ action: 'spawn', tasks: [{ role: 'scout', task: 'fixture-hang QUIT_ACTIVE_CHILD' }] });
    await eventually(() => requests.some(request => request.includes('QUIT_ACTIVE_CHILD')));
    const hostPid = (await action({ action: 'status', format: 'full' })).pid;
    const ctx = runner.createContext();
    const token = ctx.sessionManager.getBranch().findLast(entry => entry.type === 'custom' && entry.customType === 'pi-agent-run-binding').data.token;
    const savedRoot = runRoot(await realpath(fixture.agentDir), await realpath(root), `${ctx.sessionManager.getSessionId()}:${token}`);
    await runner.emit({ type: 'session_tree', oldLeafId: 'old', newLeafId: 'new' });
    await runner.emit({ type: 'session_shutdown', reason: 'quit' });
    assert.throws(() => process.kill(hostPid, 0), { code: 'ESRCH' });
    const saved = await readTaskRecord(savedRoot, atQuit.agentIds[0]);
    assert.equal(saved.task.id, atQuit.agentIds[0]); assert.ok(saved.task.checkpoint);
  } finally {
    release?.();
    if (session) { await session.extensionRunner?.emit({ type: 'session_shutdown', reason: 'quit' }); await session.abort(); session.dispose(); }
    if (old === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = old;
    await (await import('./host-cleanup.mjs')).cleanupTestHosts(fixture.agentDir); await fixture.close(); await rm(root, { recursive: true, force: true });
  }
});
