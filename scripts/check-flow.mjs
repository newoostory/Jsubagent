import assert from 'node:assert/strict';
import { mkdtemp, mkdir, rm, readFile, writeFile, copyFile } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { HostClient } from '../src/client.mjs';
import { once } from 'node:events';
import { providerFixture, eventually } from '../test/support.mjs';

const root = await mkdtemp(join(tmpdir(), 'pi-agent-real-flow-'));
let providerRoot, provider, client;
try {
  // Keep provider credentials outside the Git workspace; the writer requires a clean tree.
  providerRoot = await mkdtemp(join(tmpdir(), 'pi-agent-real-flow-provider-'));
  provider = await providerFixture(providerRoot, { barrier: 4 });
  // This flow deliberately exercises queueing through four workers. Capacity
  // scaling is measured separately by bench-worker-scaling.mjs.
  await mkdir(join(provider.agentDir, 'pi-agent'), { recursive: true });
  await writeFile(join(provider.agentDir, 'pi-agent', 'config.json'), JSON.stringify({ limits: { workerMax: 4 } }));
  await copyFile(join(providerRoot, 'marker.txt'), join(root, 'marker.txt'));
  client = new HostClient({ cwd: root, agentDir: provider.agentDir, mainSessionId: 'real-flow' });
  const init = await client.start(); assert.equal(init.limits.concurrency, 8); assert.notEqual(init.pid, process.pid);
  const ids = Array.from({ length: 8 }, (_, i) => `sdk-${i}`);
  for (const id of ids) await client.request('spawn', { id, role: 'scout', task: 'fixture-read: inspect marker.txt and report evidence', mainModel: provider.profile });
  const finished = await client.wait(ids, { timeoutMs: 60_000 });
  assert.ok(finished.every(t => t.state === 'idle'), JSON.stringify({ tasks: finished.map(({ id, state, error }) => ({ id, state, error })), ...(finished.every(t => t.state === 'idle') ? {} : { diagnostic: await client.request('status') }) }));
  assert.equal(provider.maxActive, 4, 'eight approved logical tasks share four isolated worker slots');
  for (const id of ids) {
    const task = await client.request('inspect', { id }); assert.match(task.result, /fixture-marker-evidence/);
    let byteOffset = 0, transcriptText = '';
    for (;;) {
      const page = await client.request('artifact', { id: task.transcript, byteOffset, limitBytes: 4096 });
      transcriptText += page.text;
      if (page.eof) break;
      assert.ok(page.nextByteOffset > byteOffset); byteOffset = page.nextByteOffset;
    }
    assert.match(transcriptText, /"toolCall"/);
  }
  const mainMail = await client.request('inbox'); assert.equal(mainMail.length, 8);
  await client.request('ack', { ids: mainMail.map(m => m.id) }); assert.deepEqual(await client.request('inbox'), []);
  // Actual SDK agent calls agent_send; another idle SDK task auto-wakes on delivery.
  await client.request('spawn', { id: 'sender', role: 'scout', task: 'fixture-message recipient=sdk-0', mainModel: provider.profile });
  await client.wait(['sender'], { timeoutMs: 30_000 });
  await eventually(async () => (await client.request('inspect', { id: 'sdk-0' })).autoWakes === 1);
  await client.wait(['sdk-0'], { timeoutMs: 30_000 });
  const receiver = await client.request('inspect', { id: 'sdk-0' }); assert.equal(receiver.state, 'idle');
  // Registry/auth checks happen after admission inside the isolated worker.
  // Invalid explicit selection must fail without any implicit model fallback.
  await client.request('spawn', { id: 'bad-model', role: 'scout', task: 'test', mainModel: { provider: 'pi-agent-fixture', model: 'not-present' } });
  const [badModel] = await client.wait(['bad-model'], { timeoutMs: 15000 });
  assert.equal(badModel.state, 'failed'); assert.match(badModel.error, /not registered/);
  await client.request('spawn', { id: 'rate-limited', role: 'scout', task: 'fixture-rate-limit', mainModel: provider.profile });
  const [paused] = await client.wait(['rate-limited'], { timeoutMs: 15_000 });
  assert.equal(paused.state, 'paused', JSON.stringify(paused));
  assert.equal(paused.resume.nextAction, 'explicit_resume');
  assert.ok(Number.isSafeInteger(paused.resume.nextEligibleAt));
  const callsWhilePaused = provider.calls;
  await new Promise(resolve => setTimeout(resolve, Math.max(0, paused.resume.nextEligibleAt - Date.now()) + 50));
  assert.equal(provider.calls, callsWhilePaused, 'reset eligibility must not start an automatic provider retry');
  assert.equal((await client.request('status')).active, 0, 'usage pause releases the execution slot');
  await client.request('resume', { id: 'rate-limited' });
  const [retried] = await client.wait(['rate-limited'], { timeoutMs: 15_000 });
  assert.equal(retried.state, 'idle', JSON.stringify(retried)); assert.ok(provider.rateRetriedAfterMs >= 900, 'Retry-After must not be bypassed');
  // A real SDK writer operates in a detached Git worktree, never the original directory.
  const git = (args) => promisify(execFile)('git', args, { cwd: root, timeout: 10_000, maxBuffer: 64 * 1024 });
  await writeFile(join(root, '.gitignore'), 'agent/\n');
  await git(['init', '-q']); await git(['add', 'marker.txt', '.gitignore']);
  await git(['-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', '-c', 'commit.gpgsign=false', 'commit', '-qm', 'fixture']);
  await client.request('spawn', { id: 'writer', role: 'worker', task: 'fixture-write: create a file only in the assigned worktree', mainModel: provider.profile });
  const [written] = await client.wait(['writer'], { timeoutMs: 30_000 }); assert.equal(written.state, 'idle', JSON.stringify(written));
  assert.notEqual(written.cwd, root); assert.equal(await readFile(join(written.cwd, 'created.txt'), 'utf8'), 'worktree-only-evidence');
  await assert.rejects(readFile(join(root, 'created.txt')), { code: 'ENOENT' });
  await client.request('spawn', { id: 'interrupted', role: 'scout', task: 'fixture-hang', mainModel: provider.profile });
  await eventually(async () => (await client.request('inspect', { id: 'interrupted' })).state === 'running');
  const memory = await client.monitor();
  await client.request('cancel', { id: 'interrupted' });
  await client.wait(['interrupted'], { timeoutMs: 15000 });
  const runDir = client.options.root, pid = client.info.pid, calls = provider.calls;
  const exited = once(client.child, 'close'); await client.detach(); process.kill(pid, 'SIGKILL'); await exited;
  client = new HostClient({ cwd: root, agentDir: provider.agentDir, mainSessionId: 'real-flow' });
  await client.start(); assert.equal(client.options.root, runDir);
  const recovered = await client.request('inspect', { id: 'interrupted' }); assert.equal(recovered.state, 'cancelled');
  assert.equal((await client.request('status')).active, 0, 'restart must not revive cancellation');
  assert.equal(provider.calls, calls, 'no model replay after durable cancellation');
  console.log(JSON.stringify({ pass: true, flow: 'supervisor + 8 logical SDK tasks / 4 workers + isolated loopback provider + read/write/worktree + mail + cancel/kill/restart', maxConcurrentProviderRequests: provider.maxActive, requests: provider.calls, sampledTreeRssBytes: memory.rssBytes, liveProviders: 'not contacted' }));
} finally {
  try { await client?.close(); } finally {
    try { await provider?.close(); } finally {
      await Promise.all([rm(root, { recursive: true, force: true }), ...(providerRoot ? [rm(providerRoot, { recursive: true, force: true })] : [])]);
    }
  }
}
