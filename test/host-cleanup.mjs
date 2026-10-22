import { readFile, readdir, realpath } from 'node:fs/promises';
import { join, sep } from 'node:path';
import { tmpdir } from 'node:os';
import { HostClient } from '../src/client.mjs';

// A UI shutdown deliberately detaches now. Tests must explicitly stop their
// temporary supervisors before deleting their durable roots. Never start one.
export async function cleanupTestHosts(agentDir) {
  const base = await realpath(agentDir), temp = await realpath(tmpdir());
  if (!base.startsWith(temp + sep)) throw new Error('Refusing host cleanup outside temporary fixtures');
  const runs = join(base, 'pi-agent', 'runtime', 'runs');
  let entries;
  try { entries = await readdir(runs); } catch (error) { if (error.code === 'ENOENT') return; throw error; }
  for (const name of entries) {
    if (!/^[a-f0-9]{64}$/.test(name)) continue;
    const root = join(runs, name); let binding;
    try { binding = JSON.parse(await readFile(join(root, 'bridge-binding.json'), 'utf8')); }
    catch (error) { if (error.code === 'ENOENT') continue; throw error; }
    if (binding.root !== root || binding.agentDir !== base) throw new Error('Fixture binding mismatch');
    const client = new HostClient({ cwd: binding.cwd, agentDir: base, root, mainSessionId: 'cleanup' });
    client.options = binding;
    client.starting = client.connectOnce(3000);
    try { await client.starting; await client.close(); }
    catch (error) {
      client.closed = true; client.socket?.destroy();
      if (!['ENOENT', 'ECONNREFUSED'].includes(error.code)) throw error;
    }
  }
}
