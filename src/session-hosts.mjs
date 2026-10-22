import { HostClient } from './client.mjs';
import { fail, safeError } from './limits.mjs';
import { waitOwnedExecutorStopped } from './runtime/ownership.mjs';

const REGISTRY_KEY = Symbol.for('pi-agent.process-owned-session-hosts.v1');
const GONE = new Set(['ENOENT', 'ECONNREFUSED']);

function registry() {
  if (!(globalThis[REGISTRY_KEY] instanceof Map)) globalThis[REGISTRY_KEY] = new Map();
  return globalThis[REGISTRY_KEY];
}

function identity(value) {
  if (!value || !Number.isSafeInteger(value.pid) || value.pid <= 0 || !Number.isSafeInteger(value.generation) || value.generation <= 0 || typeof value.nonce !== 'string' || !value.nonce || typeof value.started !== 'string' || !value.started) return undefined;
  return { pid: value.pid, generation: value.generation, nonce: value.nonce, started: value.started };
}

function sameIdentity(left, right) {
  return left && right && ['pid', 'generation', 'nonce', 'started'].every(key => left[key] === right[key]);
}

export function rememberSessionHost(client) {
  const hostIdentity = identity(client?.info?.identity), options = client?.options;
  if (!hostIdentity || !options || typeof options.root !== 'string' || !options.root || typeof options.cwd !== 'string' || typeof options.agentDir !== 'string') throw fail('Connected supervisor binding is incomplete', 'HOST');
  registry().set(options.root, { options: structuredClone(options), identity: hostIdentity });
}

async function closeDetached(binding) {
  const client = new HostClient({ cwd: binding.options.cwd, agentDir: binding.options.agentDir, root: binding.options.root, mainSessionId: 'process-quit' });
  client.options = structuredClone(binding.options);
  client.starting = client.connectOnce(3000);
  try {
    const info = await client.starting;
    if (!sameIdentity(identity(info?.identity), binding.identity)) throw fail('Owned supervisor identity changed before quit shutdown', 'HOST_EXIT');
    const receipt = await client.close();
    if (receipt?.stopped !== true || receipt?.saved !== true || receipt?.executorsStopped !== true) throw fail('Owned supervisor shutdown was not confirmed', 'SHUTDOWN_INCOMPLETE');
    return 'closed';
  } catch (error) {
    client.closed = true; client.socket?.destroy();
    if (GONE.has(error?.code)) {
      // A missing socket can precede executor cleanup. Confirm the recorded
      // PID/start identity is gone before retiring this process-owned binding.
      await waitOwnedExecutorStopped(binding.identity);
      return 'gone';
    }
    throw error;
  }
}

// A single Pi process may detach several session hosts during tree/new/reload.
// Quit closes exactly those bindings; it never scans runs or starts a host.
export async function closeProcessSessionHosts(current) {
  const owned = registry(), failures = [], currentRoot = current?.options?.root;
  if (current) {
    try {
      const receipt = await current.close();
      if (receipt?.stopped !== true || receipt?.saved !== true || receipt?.executorsStopped !== true) {
        // It may have disconnected or been detached during a startup/session
        // race. Only reconnect a verified binding, without starting any host.
        const binding = owned.get(currentRoot);
        if (!binding || !current.closed && !current.exited) throw fail('Current supervisor shutdown was not confirmed', 'SHUTDOWN_INCOMPLETE');
        await closeDetached(binding);
      }
      owned.delete(currentRoot);
    } catch (error) { failures.push(`${currentRoot ?? 'current'}: ${safeError(error)}`); }
  }
  for (const [root, binding] of [...owned]) {
    if (root === currentRoot) continue;
    try { await closeDetached(binding); owned.delete(root); }
    catch (error) { failures.push(`${root}: ${safeError(error)}`); }
  }
  if (failures.length) throw fail(`Unable to confirm all owned supervisor shutdowns: ${failures.join('; ')}`, 'SHUTDOWN_INCOMPLETE');
}

export const processSessionHostCount = () => registry().size;
