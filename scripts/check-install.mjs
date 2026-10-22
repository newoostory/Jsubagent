import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { copyFile, mkdir, mkdtemp, rm, stat } from 'node:fs/promises';
import { dirname, join } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager } from '@earendil-works/pi-coding-agent';
import { providerFixture } from '../test/support.mjs';
import { cleanupTestHosts } from '../test/host-cleanup.mjs';

const source = fileURLToPath(new URL('../', import.meta.url));
const root = await mkdtemp(join(tmpdir(), 'jsubagent-install-'));
const installed = join(root, 'extension');
const report = JSON.parse(execFileSync('npm', ['pack', '--dry-run', '--json', '--ignore-scripts'], { cwd: source, encoding: 'utf8', maxBuffer: 4 * 1024 * 1024 }));
const pack = Array.isArray(report) ? report[0] : report.jsubagent;
const requests = [], errors = [];
const oldAgentDir = process.env.PI_CODING_AGENT_DIR;
let fixture, session;
try {
  for (const file of pack.files) {
    const target = join(installed, file.path);
    await mkdir(dirname(target), { recursive: true });
    await copyFile(join(source, file.path), target);
  }
  await assert.rejects(stat(join(installed, 'node_modules')), { code: 'ENOENT' });
  fixture = await providerFixture(root, {
    onRequest: data => requests.push(data),
    respond: ({ data, msg, stream }) => {
      if (!data.tools?.some(tool => tool.function.name === 'agent') || msg?.role !== 'tool' || !String(msg.tool_call_id).startsWith('spawn-')) return false;
      let accepted; try { accepted = JSON.parse(msg.content); } catch { accepted = null; }
      if (accepted?.agentIds?.length) return false;
      stream({ role: 'assistant', content: `fixture-install-failure: ${msg.content}` }); stream({}, 'stop'); return true;
    },
  });
  process.env.PI_CODING_AGENT_DIR = fixture.agentDir;
  const settingsManager = SettingsManager.inMemory({ compaction: { enabled: false }, retry: { enabled: false } });
  // Use Pi's real extension-path loader: peer aliases exist only in the main
  // process, exactly as with a managed git install that omits dev/peer packages.
  const loader = new DefaultResourceLoader({ cwd: root, agentDir: fixture.agentDir, settingsManager, noExtensions: true, additionalExtensionPaths: [join(installed, 'index.js')], noSkills: true, noContextFiles: true, noPromptTemplates: true, noThemes: true });
  await loader.reload(); assert.deepEqual(loader.getExtensions().errors, []);
  const runtime = await ModelRuntime.create({ authPath: join(fixture.agentDir, 'auth.json'), modelsPath: join(fixture.agentDir, 'models.json'), modelsStorePath: join(fixture.agentDir, 'models-store.json'), allowModelNetwork: false });
  ({ session } = await createAgentSession({ cwd: root, agentDir: fixture.agentDir, model: runtime.getModel(fixture.profile.provider, fixture.profile.model), modelRuntime: runtime, settingsManager, resourceLoader: loader, sessionManager: SessionManager.inMemory(root) }));
  await session.bindExtensions({ mode: 'print', onError: error => errors.push(error) });
  await session.prompt('fixture-coordinate: delegate a scout and wait for marker.txt evidence');
  const last = JSON.stringify(session.messages.at(-1));
  assert.match(last, /fixture-marker-evidence/, `Production installation failed: ${last.slice(0, 4096)}`);
  const main = requests.filter(request => request.tools?.some(tool => tool.function?.name === 'agent'));
  assert.equal(main.length, 3); assert.equal(requests.length - main.length, 2);
  assert.deepEqual(errors, []);
  await assert.rejects(stat(join(installed, 'node_modules')), { code: 'ENOENT' });
  console.log(JSON.stringify({ pass: true, installation: 'packed runtime files only, no extension node_modules', loader: 'Pi extension path', mainProviderRequests: main.length, childProviderRequests: requests.length - main.length, liveProviders: 'not contacted' }));
} finally {
  if (session) { await session.extensionRunner?.emit({ type: 'session_shutdown', reason: 'quit' }); await session.abort(); session.dispose(); }
  if (oldAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = oldAgentDir;
  if (fixture) { await cleanupTestHosts(fixture.agentDir); await fixture.close(); }
  await rm(root, { recursive: true, force: true });
}
