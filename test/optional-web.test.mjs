import test from 'node:test';
import assert from 'node:assert/strict';
import { Type } from 'typebox';
import { createWebTools, hasWebRuntime, loadWebRuntime } from '../src/web-tools.mjs';

test('an absent optional web integration does not block ordinary child tasks', { skip: hasWebRuntime() }, async () => {
  assert.equal(await loadWebRuntime({ optional: true }), null);
  assert.deepEqual(await createWebTools({ task: { role: { name: 'scout' } } }), []);
});

test('an explicitly required web capability fails clearly when unavailable', { skip: hasWebRuntime() }, async () => {
  await assert.rejects(createWebTools({ task: { role: { requiredTools: ['web_fetch'] } } }), { code: 'CAPABILITY' });
});

test('an injected web runtime retains bounded receipt and cleanup behavior', async () => {
  const calls = [];
  const runtime = {
    WebSearchSchema: Type.Object({ query: Type.String() }),
    WebFetchSchema: Type.Object({ url: Type.String() }),
    BUDGETS: { searchDeadlineMs: 1000, fetchDeadlineMs: 1000 },
    createFetchSession: () => ({
      fetch: async input => ({ ok: true, text: `fixture for ${input.url}` }),
      dispose: () => calls.push('dispose'),
    }),
  };
  const hooks = {
    beginWeb: async (kind, input) => { calls.push([kind, input]); return 1; },
    finishWeb: async (kind, attempt, output) => {
      calls.push([kind, attempt, output.outcome]); return { artifactId: 'fixture-artifact' };
    },
  };
  const tools = await createWebTools({ task: { role: { name: 'scout' } }, hooks, runtime });
  const result = await tools.find(tool => tool.name === 'web_fetch').execute('fixture-call', { url: 'https://example.org/' });
  assert.deepEqual(JSON.parse(result.content[0].text), {
    ok: true, text: 'fixture for https://example.org/', receipt: 'web-fetch-1', artifactId: 'fixture-artifact',
  });
  tools.dispose();
  assert.deepEqual(calls, [
    ['fetch', { url: 'https://example.org/' }],
    ['fetch', 1, { ok: true, text: 'fixture for https://example.org/' }],
    'dispose',
  ]);
});
