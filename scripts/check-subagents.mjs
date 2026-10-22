import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager } from '@earendil-works/pi-coding-agent';
import extension from '../index.js';
import { providerFixture } from '../test/support.mjs';
import { cleanupTestHosts } from '../test/host-cleanup.mjs';

const root = await mkdtemp(join(tmpdir(), 'pi-subagents-public-'));
const requests = [], errors = [];
const oldAgentDir = process.env.PI_CODING_AGENT_DIR;
let fixture, session;
try {
  fixture = await providerFixture(root, { onRequest: data => requests.push(data) });
  process.env.PI_CODING_AGENT_DIR = fixture.agentDir;
  const settingsManager = SettingsManager.inMemory({ compaction: { enabled: false } });
  const loader = new DefaultResourceLoader({ cwd: root, agentDir: fixture.agentDir, settingsManager, noExtensions: true, extensionFactories: [extension], noSkills: true, noContextFiles: true, noPromptTemplates: true, noThemes: true });
  await loader.reload(); assert.deepEqual(loader.getExtensions().errors, []);
  const runtime = await ModelRuntime.create({ authPath: join(fixture.agentDir, 'auth.json'), modelsPath: join(fixture.agentDir, 'models.json'), modelsStorePath: join(fixture.agentDir, 'models-store.json'), allowModelNetwork: false });
  ({ session } = await createAgentSession({ cwd: root, agentDir: fixture.agentDir, model: runtime.getModel(fixture.profile.provider, fixture.profile.model), modelRuntime: runtime, settingsManager, resourceLoader: loader, sessionManager: SessionManager.inMemory(root) }));
  await session.bindExtensions({ mode: 'print', onError: error => errors.push(error) });
  assert.equal(fixture.calls, 0, 'loading the extension is lazy');
  await session.prompt('MAIN_PRIVATE_CONTEXT: fixture-coordinate use a scout and wait for its bounded evidence');
  const main = requests.filter(request => request.tools?.some(tool => tool.function?.name === 'agent'));
  const child = requests.filter(request => !main.includes(request));
  assert.equal(main.length, 3, 'main runs exactly prompt -> spawn result -> wait result');
  assert.equal(child.length, 2, 'independent child runs prompt -> file-read result');
  assert.match(JSON.stringify(session.messages.at(-1)), /fixture-marker-evidence/);
  const agentSchema = main[0].tools.find(tool => tool.function.name === 'agent').function.parameters;
  assert.deepEqual(agentSchema.properties.action.enum, ['catalog', 'spawn', 'status', 'wait', 'send', 'cancel', 'resume', 'archive', 'read']);
  const text = value => typeof value === 'string' ? value : JSON.stringify(value);
  const mainSystem = text(main[0].messages.find(message => message.role === 'system')?.content);
  assert.match(mainSystem, /Main may work directly/);
  assert.doesNotMatch(mainSystem, /Default workflow preflight|implement-review-integrate-verify/);
  for (const request of child) {
    assert.doesNotMatch(JSON.stringify(request.messages), /MAIN_PRIVATE_CONTEXT/);
    assert.match(text(request.messages.find(message => message.role === 'system')?.content), /independent Pi subagent/);
    assert.equal(request.tools.some(tool => tool.function?.name === 'agent'), false);
  }
  const calls = fixture.calls;
  await new Promise(resolve => setTimeout(resolve, 1200));
  assert.equal(fixture.calls, calls, 'UI polling and a completion notice must not create extra main calls');
  assert.deepEqual(errors, []);
  console.log(JSON.stringify({ pass: true, mainProviderRequests: main.length, childProviderRequests: child.length, independentContext: true, defaultDirectWork: true, publicLifecycleActions: agentSchema.properties.action.enum.length, automaticFollowUpRequests: fixture.calls - calls, liveProviders: 'not contacted' }));
} finally {
  if (session) { await session.extensionRunner?.emit({ type: 'session_shutdown', reason: 'quit' }); await session.abort(); session.dispose(); }
  if (oldAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR; else process.env.PI_CODING_AGENT_DIR = oldAgentDir;
  if (fixture) { await cleanupTestHosts(fixture.agentDir); await fixture.close(); }
  await rm(root, { recursive: true, force: true });
}
