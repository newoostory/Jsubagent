import { createServer } from 'node:http';
import { mkdir, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { LIMITS } from '../src/limits.mjs';

// Real loopback OpenAI-compatible SSE provider. No live network or credentials.
export async function providerFixture(root, { barrier = 0, onRequest = () => {}, gate = () => false, respond = () => false, reportUsage = false } = {}) {
  let active = 0, maxActive = 0, calls = 0, rateLimitedAt = 0, rateRetriedAfterMs = 0; const pending = new Set(); const sockets = new Set();
  const server = createServer(async (req, res) => {
    let bytes = 0, body = '';
    try {
      for await (const chunk of req) { bytes += chunk.length; if (bytes > 2 * 1024 * 1024) throw new Error('fixture request too large'); body += chunk.toString(); }
      const data = JSON.parse(body); onRequest(data); calls++; active++; maxActive = Math.max(active, maxActive); let counted = true;
      res.once('close', () => { if (counted) { active--; counted = false; } });
      const proceed = () => {
        pending.delete(proceed);
        if (res.destroyed) return;
        const msg = data.messages.at(-1);
        const user = [...data.messages].reverse().find(m => m.role === 'user');
        const task = typeof user?.content === 'string' ? user.content : JSON.stringify(user?.content ?? '');
        if (task.includes('fixture-rate-limit') && !rateLimitedAt) {
          rateLimitedAt = Date.now(); res.writeHead(429, { 'Content-Type': 'application/json', 'Retry-After': '1' }); res.end(JSON.stringify({ error: { message: 'Rate limit exceeded; retry after 1 second' } })); return;
        }
        if (task.includes('fixture-rate-limit')) rateRetriedAfterMs = Date.now() - rateLimitedAt;
        res.writeHead(200, { 'Content-Type': 'text/event-stream' });
        const stream = (delta, finish = null) => res.write(`data: ${JSON.stringify({ id: 'fixture', object: 'chat.completion.chunk', created: 1, model: 'fixture', choices: [{ index: 0, delta, finish_reason: finish }] })}\n\n`);
        if (respond({ data, task, msg, stream, calls })) { res.end('data: [DONE]\n\n'); return; }
        if (task.includes('fixture-hang')) { stream({ role: 'assistant' }); return; }
        if (task.includes('fixture-error') && !task.includes('fixture-repair')) { res.end('data: [DONE]\n\n'); return; }
        const tools = new Set((data.tools ?? []).map(t => t.function?.name));
      if (task.includes('fixture-coordinate') && tools.has('agent') && msg?.role === 'user') {
        stream({ role: 'assistant', tool_calls: [{ index: 0, id: `spawn-${calls}`, type: 'function', function: { name: 'agent', arguments: JSON.stringify({ action: 'spawn', tasks: [{ name: 'AuthScout', role: 'scout', task: 'fixture-read: investigate marker.txt and report evidence' }] }) } }] }); stream({}, 'tool_calls');
      } else if (task.includes('fixture-coordinate') && tools.has('agent') && msg?.role === 'tool' && String(msg.tool_call_id).startsWith('spawn-')) {
        const accepted = JSON.parse(msg.content), agentId = accepted.agentIds?.[0];
        if (!agentId) throw new Error('fixture coordinate expected the canonical spawn envelope');
        stream({ role: 'assistant', tool_calls: [{ index: 0, id: `wait-${calls}`, type: 'function', function: { name: 'agent', arguments: JSON.stringify({ action: 'wait', agentIds: [agentId], format: 'full' }) } }] }); stream({}, 'tool_calls');
      } else if (task.includes('fixture-coordinate') && tools.has('agent') && msg?.role === 'tool' && String(msg.tool_call_id).startsWith('wait-')) {
        const waited = JSON.parse(msg.content), done = waited.agents?.[0];
        if (!done) throw new Error('fixture coordinate expected the canonical wait envelope');
        stream({ role: 'assistant', content: `Verified wait response: ${done.response?.text ?? 'missing response'}` }); stream({}, 'stop');
        } else if (msg?.role === 'user' && task.includes('fixture-write')) {
          const properties = data.tools.find(t => t.function?.name === 'write')?.function.parameters.properties ?? {};
          const args = { path: 'created.txt', content: 'worktree-only-evidence', ...('expectedSha256' in properties ? { expectedSha256: 'missing' } : {}) };
          stream({ role: 'assistant', tool_calls: [{ index: 0, id: `write-${calls}`, type: 'function', function: { name: 'write', arguments: JSON.stringify(args) } }] }); stream({}, 'tool_calls');
        } else if (msg?.role === 'user' && task.includes('fixture-read')) {
          const properties = data.tools.find(t => t.function?.name === 'read')?.function.parameters.properties ?? {};
          const args = { path: 'marker.txt', ...('byteOffset' in properties ? { byteOffset: 0, limitBytes: 64 } : { offset: 1, limit: 1 }) };
          stream({ role: 'assistant', tool_calls: [{ index: 0, id: `read-${calls}`, type: 'function', function: { name: 'read', arguments: JSON.stringify(args) } }] }); stream({}, 'tool_calls');
        } else if (msg?.role === 'user' && task.includes('fixture-message')) {
          const to = /recipient=([a-zA-Z0-9_-]+)/.exec(task)?.[1] ?? 'main';
          stream({ role: 'assistant', tool_calls: [{ index: 0, id: `send-${calls}`, type: 'function', function: { name: 'agent_send', arguments: JSON.stringify({ to, body: 'Fixture peer finding', kind: 'finding', delivery: 'steer' }) } }] }); stream({}, 'tool_calls');
        } else {
          stream({ role: 'assistant', content: 'Verified fixture result: ' }); stream({ content: msg?.role === 'tool' ? String(msg.content).slice(0, 128) : 'done' }); stream({}, 'stop');
        }
        if (reportUsage) res.write(`data: ${JSON.stringify({ id: 'fixture', object: 'chat.completion.chunk', created: 1, model: 'fixture', choices: [], usage: { prompt_tokens: 5, completion_tokens: 2, total_tokens: 7 } })}\n\n`);
        res.end('data: [DONE]\n\n');
      };
      if (gate(data, proceed)) return; // The gate owns explicit response release.
      else if (barrier && calls <= barrier) { pending.add(proceed); if (pending.size === barrier) for (const start of [...pending]) start(); }
      else setTimeout(proceed, 100);
    } catch (e) { if (!res.headersSent) res.writeHead(500); res.end(String(e)); }
  });
  server.on('connection', s => { sockets.add(s); s.on('close', () => sockets.delete(s)); });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const agentDir = join(root, 'agent'); await mkdir(agentDir, { recursive: true });
  await writeFile(join(root, 'marker.txt'), 'fixture-marker-evidence');
  await writeFile(join(agentDir, 'models.json'), JSON.stringify({ providers: { 'pi-agent-fixture': { baseUrl: `http://127.0.0.1:${server.address().port}/v1`, api: 'openai-completions', apiKey: 'local-fixture-not-a-secret', compat: { supportsStore: false, supportsUsageInStreaming: false }, models: [{ id: 'fixture', name: 'Fixture', reasoning: false, input: ['text'], contextWindow: 32000, maxTokens: 1024 }] } } }));
  return { agentDir, profile: { provider: 'pi-agent-fixture', model: 'fixture', thinkingLevel: 'off' }, get maxActive() { return maxActive; }, get calls() { return calls; }, get rateRetriedAfterMs() { return rateRetriedAfterMs; }, close: async () => { for (const socket of sockets) socket.destroy(); await new Promise(resolve => server.close(resolve)); } };
}
export { writeTransConfig as currentConfigFixture } from './trans-config-fixture.mjs';
export function fakeConfig(root, overrides = {}) {
  return { cwd: root, agentDir: root, limits: { ...LIMITS, minFreeBytes: 1, ...overrides }, profiles: { main: { inherit: true } }, roles: { scout: { name: 'scout', description: 'test', profile: 'main', tools: ['read'], workspace: 'readonly', prompt: 'test', hash: 'fixture' }, shared: { name: 'shared', description: 'test', profile: 'main', tools: ['write'], workspace: 'shared', prompt: 'test', hash: 'fixture' } } };
}
export function responseTask(id, cwd, { state = 'idle', result = '', error = '' } = {}) {
  return { id, name: id, requestHash: 'fixture-request', state, result, error, task: 'Fixture evidence', sourceCwd: cwd, cwd,
    role: fakeConfig(cwd).roles.scout, profile: { name: 'main', provider: 'fixture', model: 'fixture', maxConcurrent: 8 },
    checkpoint: { messages: [] }, mailbox: [], delivering: [], seen: [], autoWakes: 0,
    web: { mode: 'public' }, webUsed: { search: 0, fetch: 0 }, webReceipts: [], updatedAt: Date.now() };
}
export async function eventually(fn, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) { const result = await fn(); if (result) return result; await new Promise(resolve => setTimeout(resolve, 20)); }
  throw new Error('eventually timed out');
}
