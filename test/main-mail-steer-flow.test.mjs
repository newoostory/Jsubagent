import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, rm } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { Type } from 'typebox';
import { createAgentSession, DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager } from '@earendil-works/pi-coding-agent';
import { providerFixture } from './support.mjs';

test('important mail steers an active Pi loop into its next automatic provider call', { timeout: 30_000 }, async () => {
  const root = await mkdtemp(join(tmpdir(), 'pi-main-mail-steer-'));
  const requests = [];
  const fixture = await providerFixture(root, {
    onRequest: data => requests.push(data),
    respond({ task, msg, stream, calls }) {
      if (!task.includes('fixture-main-mail-steer')) return false;
      if (msg?.role === 'user') {
        stream({ role: 'assistant', tool_calls: [{ index: 0, id: `probe-${calls}`, type: 'function', function: { name: 'attention_probe', arguments: '{}' } }] });
        stream({}, 'tool_calls');
      } else {
        stream({ role: 'assistant', content: 'attention observed' }); stream({}, 'stop');
      }
      return true;
    },
  });
  let session;
  const MAIL_TYPE = 'active-attention-fixture';
  const extension = pi => pi.registerTool({
    name: 'attention_probe', label: 'attention probe', description: 'fixture', parameters: Type.Object({}),
    execute: async () => {
      pi.sendMessage({ customType: MAIL_TYPE, content: 'QUESTION_REFERENCE', display: false }, { deliverAs: 'steer' });
      return { content: [{ type: 'text', text: 'probe complete' }] };
    },
  });
  try {
    const settingsManager = SettingsManager.inMemory({ compaction: { enabled: false } });
    const loader = new DefaultResourceLoader({ cwd: root, agentDir: fixture.agentDir, settingsManager, noExtensions: true, extensionFactories: [extension], noSkills: true, noContextFiles: true, noPromptTemplates: true, noThemes: true });
    await loader.reload(); assert.deepEqual(loader.getExtensions().errors, []);
    const runtime = await ModelRuntime.create({ authPath: join(fixture.agentDir, 'auth.json'), modelsPath: join(fixture.agentDir, 'models.json'), modelsStorePath: join(fixture.agentDir, 'models-store.json'), allowModelNetwork: false });
    ({ session } = await createAgentSession({ cwd: root, agentDir: fixture.agentDir, model: runtime.getModel(fixture.profile.provider, fixture.profile.model), modelRuntime: runtime, settingsManager, resourceLoader: loader, sessionManager: SessionManager.inMemory(root) }));
    await session.bindExtensions({ mode: 'print' });
    await session.prompt('fixture-main-mail-steer');
    assert.equal(requests.length, 2, 'one user prompt must produce the tool request and one automatic continuation');
    // Pi converts extension custom messages to provider-facing user messages.
    // The stable contract at this boundary is the exact opaque notification
    // content, not Pi's internal customType discriminator.
    const attention = requests[1].messages.filter(message => message.role === 'user' && JSON.stringify(message.content).includes('QUESTION_REFERENCE'));
    assert.equal(attention.length, 1, JSON.stringify(requests[1].messages));
    assert.equal(requests[1].messages.filter(message => message.role === 'user' && JSON.stringify(message.content).includes('fixture-main-mail-steer')).length, 1, 'no second user prompt was submitted');
  } finally {
    if (session) { await session.abort(); session.dispose(); }
    await fixture.close(); await rm(root, { recursive: true, force: true });
  }
});
