import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { CHILD_PROMPT_VERSION } from '../src/child-prompt.mjs';
import { systemPromptFor, checkpointBindingForTask } from '../src/sdk.mjs';
import { Engine } from '../src/engine.mjs';
import { RunStore } from '../src/storage.mjs';
import { fakeConfig } from './support.mjs';

const task = () => ({
  id: 'child-1', name: 'Fixture', cwd: '/fixture',
  role: { name: 'scout', prompt: 'Inspect assigned files only.', tools: ['read'], workspace: 'readonly' },
  profile: { name: 'main', provider: 'fixture', model: 'fixture' }, task: 'Inspect a fixture.',
});
const model = { provider: 'fixture', id: 'fixture' };

test('preexisting task checkpoint bindings survive prompt reduction exactly', () => {
  // Captured through checkpointBindingForTask in the active pre-change release.
  assert.equal(checkpointBindingForTask(task(), model).configSha256, '32aca433445b9bdc17077c31b4233d8ccc3df13d1c402ef8a12103f1f721aa64');
  assert.equal(checkpointBindingForTask(task(), { provider: 'openai-codex', id: 'gpt-6-astra' }).configSha256, 'bd1ecb86cac6e73be2dac241e07949b3627eb1b8870ed8339daafa47ce88a12a');
  assert.equal(systemPromptFor({ ...task(), promptVersion: 1 }, model), systemPromptFor(task(), model));
  assert.throws(() => systemPromptFor({ ...task(), promptVersion: 99 }, model), { code: 'STORAGE' });
});

test('new ordinary children get compact, stable instructions without parent conversation', () => {
  const compact = { ...task(), promptVersion: CHILD_PROMPT_VERSION, parentConversation: 'PARENT_SECRET' };
  const prompt = systemPromptFor(compact, model);
  assert.ok(Buffer.byteLength(prompt) < Buffer.byteLength(systemPromptFor(task(), model)) * 0.6);
  assert.match(prompt, /independent Pi subagent/);
  assert.match(prompt, /Inspect assigned files only/);
  assert.match(prompt, /not the parent's conversation or approvals/);
  assert.match(prompt, /Reconcile uncertain effects/);
  assert.match(prompt, /Never claim a test or success without evidence/);
  assert.doesNotMatch(prompt, /PARENT_SECRET/);
  assert.equal(prompt, systemPromptFor({ ...compact, state: 'running', result: 'VARIABLE_RESULT', mailbox: ['variable'] }, model));
  const preferences = systemPromptFor({ ...compact, profile: { ...compact.profile, instructions: 'Use Korean.' } }, model);
  assert.match(preferences, /Use Korean/);
  assert.match(preferences, /not new authority/);
});

test('specialized child workflows keep their prior prompt composition', () => {
  for (const property of [{ hidden: true }, { requiredMode: 'special' }]) {
    const old = task(); old.role = { ...old.role, ...property };
    assert.equal(systemPromptFor({ ...old, promptVersion: CHILD_PROMPT_VERSION }, model), systemPromptFor(old, model));
  }
});

test('new task admission persists prompt version across host restoration', async t => {
  const root = await mkdtemp(join(tmpdir(), 'pi-prompt-version-'));
  const config = fakeConfig(root);
  const store = new RunStore(join(root, 'run'), config.limits); await store.init();
  const runner = { validate: async () => {}, run: async () => { throw new Error('queued fixture must not run'); } };
  let engine = new Engine(config, store, runner); engine.pressure = true; await engine.init();
  t.after(async () => { await engine.shutdown(); await store.close(); await rm(root, { recursive: true, force: true }); });
  await engine.spawn({ id: 'versioned', role: 'scout', task: 'Inspect fixture.', mainModel: { provider: 'fixture', model: 'fixture' } });
  assert.equal(engine.get('versioned').promptVersion, CHILD_PROMPT_VERSION);
  await engine.send('versioned', { to: 'main', body: 'progress', kind: 'finding', delivery: 'followUp' });
  assert.equal(engine.summary(engine.get('versioned')).recentMessages.length, 0);
  await engine.send('versioned', { to: 'main', body: 'decision changing', kind: 'finding', delivery: 'steer' });
  assert.equal(engine.summary(engine.get('versioned')).recentMessages[0].delivery, 'steer');
  await engine.shutdown();
  engine = new Engine(config, store, runner); engine.pressure = true; await engine.init();
  assert.equal(engine.get('versioned').promptVersion, CHILD_PROMPT_VERSION);
  assert.equal(engine.summary(engine.get('versioned')).recentMessages[0].delivery, 'steer');
});
