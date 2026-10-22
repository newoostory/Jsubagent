import { randomUUID } from 'node:crypto';
import { AsyncLocalStorage } from 'node:async_hooks';
import { FrameWriter, readFrames } from '../protocol.mjs';
import { fail, safeError, short } from '../limits.mjs';
import { createSdkRunner } from '../sdk.mjs';
import { VERSION as PI_SDK_VERSION } from '@earendil-works/pi-coding-agent';
import { providerErrorIpcFields, applyProviderErrorIpcFields } from './provider-fault.mjs';
import { WORKER_PROTOCOL, assertWorkerProtocol } from './worker-contract.mjs';
import { getHeapStatistics } from 'node:v8';
import { MEMORY_POLICY } from './memory-policy.mjs';
import { RUNTIME_PROVENANCE, captureLoadedSource, protocolMismatchDiagnostic } from './provenance.mjs';

const WORKER_LOADED_SOURCE = captureLoadedSource(import.meta.url, 'worker');
function runtimeIdentity() { return { provenance: RUNTIME_PROVENANCE, workerProtocol: WORKER_PROTOCOL, worker: { loaded: WORKER_LOADED_SOURCE }, sdk: { loadedVersion: PI_SDK_VERSION } }; }

const writer = new FrameWriter(process.stdout), pending = new Map();
let generation, runner, active, ending = false, scope, memoryYield, sampleSequence = 0, lastSampleAt = 0, sampling = false;
const startedAt = Date.now(), runContext = new AsyncLocalStorage();
function memorySample(runToken = active?.runToken ?? null) {
  const sample = { ...process.memoryUsage(), heapLimit: getHeapStatistics().heap_size_limit, identity: { pid: process.pid, startedAt, nonce: generation }, scope, runToken, sequence: ++sampleSequence, sampledAt: Date.now(), source: 'node.memoryUsage', quality: 'measured' };
  lastSampleAt = sample.sampledAt; return sample;
}
const memoryTimer = setInterval(() => {
  if (!sampling && generation && scope && !ending && Date.now() - lastSampleAt >= (active ? MEMORY_POLICY.activeSampleMs : MEMORY_POLICY.idleSampleMs)) {
    sampling = true;
    writer.send({ type: 'memory', generation, value: memorySample() }).catch(() => {}).finally(() => { sampling = false; });
  }
}, MEMORY_POLICY.activeSampleMs);
memoryTimer.unref();
const call = (op, ...args) => sessionCall(runContext.getStore(), op, ...args);
function sessionCall(runToken, op, ...args) {
  if (!runToken) return Promise.reject(fail('Worker call outside an owned run', 'STALE'));
  const id = randomUUID();
  return new Promise((resolve, reject) => {
    pending.set(id, { resolve, reject, runToken });
    writer.send({ type: 'call', generation, runToken, id, op, args }).catch(error => { pending.delete(id); reject(error); });
  });
}
function remoteStore(root, limits) {
  return { root, limits,
    artifactInfo: (...args) => call('artifactInfo', ...args),
    artifactIdentity: (...args) => call('artifactIdentity', ...args),
    readArtifact: (...args) => call('readArtifact', ...args),
    async artifact(owner, label) {
      const runToken = runContext.getStore();
      const call = (op, ...args) => sessionCall(runToken, op, ...args);
      const info = await call('artifact', owner, label);
      return { info, closed: false, async write(input) {
        const chunk = Buffer.isBuffer(input) ? input : Buffer.from(input);
        if (this.closed) throw fail('Artifact closed', 'STORAGE');
        for (let offset = 0; offset < chunk.length; offset += 65536) {
          const result = await call('artifactWrite', info.id, chunk.subarray(offset, offset + 65536).toString('base64')); info.bytes = result.bytes;
        }
      }, async finish(complete = true, error) {
        if (this.finishing) return this.finishing;
        this.closed = true;
        this.finishing = call('artifactFinish', info.id, complete, error).then(result => { Object.assign(info, result); return result; });
        return this.finishing;
      } };
    },
  };
}
let store;
async function execute(packet) {
  let result, error;
  try {
    if (packet.op === 'init') {
      if (runner) throw fail('Worker already initialized', 'PROTOCOL');
      try { assertWorkerProtocol(packet.args?.workerProtocol); }
      catch (error) {
        error.message = `${error.message} (${protocolMismatchDiagnostic({ expected: WORKER_PROTOCOL, received: packet.args?.workerProtocol, workerLoaded: WORKER_LOADED_SOURCE, supervisorLoaded: packet.args?.supervisorRuntime?.supervisor?.loaded, sdkVersion: PI_SDK_VERSION })})`;
        throw error;
      }
      const { config, root } = packet.args;
      scope = root;
      store = remoteStore(root, config.limits);
      const commands = {
        acquire: async signal => {
          signal?.throwIfAborted(); const runToken = runContext.getStore(); const id = await sessionCall(runToken, 'commandAcquire');
          return () => { const released = sessionCall(runToken, 'commandRelease', id); void released.catch(() => {}); return released; };
        },
        trackProcess: (owner, identity) => call('commandTrack', owner, identity),
        untrackProcess: (owner, identity) => call('commandUntrack', owner, identity),
      };
      runner = await createSdkRunner(config, store, commands); result = { pid: process.pid, startedAt, nonce: generation, workerProtocol: WORKER_PROTOCOL, runtime: runtimeIdentity() };
    } else if (packet.op === 'validate') {
      if (!runner || active) throw fail('Worker busy/uninitialized', 'BUSY');
      await runner.validate(packet.args.profile); result = true;
    } else if (packet.op === 'run') {
      if (!runner || active || ending) throw fail('Worker busy/uninitialized', 'BUSY');
      const runToken = packet.args.runToken;
      if (typeof runToken !== 'string' || !/^[a-zA-Z0-9_-]{1,80}$/.test(runToken)) throw fail('Missing run fence', 'PROTOCOL');
      const abort = new AbortController(); active = { abort, runToken };
      const hooks = Object.fromEntries(['send', 'inbox', 'takeSteering', 'checkpoint', 'saveCheckpoint', 'beginWeb', 'finishWeb', 'beginEffect', 'finishEffect', 'shouldHold', 'reserveUsage', 'settleUsage', 'publishResult', 'readResult'].map(name => [name, (...args) => sessionCall(runToken, name, ...args)]));
      hooks.shouldHold = async () => {
        const parent = await sessionCall(runToken, 'shouldHold'); if (parent) return parent;
        if (memoryYield !== runToken) return undefined;
        const recheck = await sessionCall(runToken, 'memoryRecheck', memorySample(runToken));
        if (recheck.state === 'normal' || recheck.state === 'warn') { memoryYield = undefined; return undefined; }
        return { kind: 'worker_memory', reason: 'Worker memory maintenance at a completed request/tool/checkpoint boundary.' };
      };
      hooks.progress = value => { writer.send({ type: 'progress', generation, runToken, value }).catch(e => abort.abort(e)); };
      try {
        result = await runContext.run(runToken, () => runner.run(packet.args.task, abort.signal, hooks));
        if (!result.resultRef?.complete) throw fail('SDK did not seal a canonical result', 'STORAGE');
        result = { ...result, result: short(result.result ?? '', 24 * 1024) };
      } finally { active = null; memoryYield = undefined; }
    } else throw fail('Unknown worker request', 'PROTOCOL');
  } catch (caught) {
    let e = caught, providerFields = {};
    try { providerFields = providerErrorIpcFields(e); }
    catch (metadataError) { e = metadataError; }
    error = { message: providerFields.providerFault?.safeMessage ?? short(safeError(e), 4096), code: e.code ?? 'WORKER', ...(e.checkpoint ? { checkpoint: e.checkpoint } : {}), ...(e.transcript ? { transcript: e.transcript } : {}), ...(e.usage ? { usage: e.usage } : {}), ...providerFields };
  }
  if (scope) await writer.send({ type: 'memory', generation, value: memorySample(packet.op === 'run' ? packet.args.runToken : null) });
  await writer.send({ type: 'reply', generation, ...(packet.op === 'run' ? { runToken: packet.args.runToken } : {}), id: packet.id, ok: !error, result, error });
}
const requests = new Set();
try {
  for await (const packet of readFrames(process.stdin)) {
    if (!generation && packet.type === 'request' && packet.op === 'init') generation = packet.generation;
    if (!generation || generation !== packet.generation) throw fail('Stale worker generation', 'PROTOCOL');
    if (packet.type === 'callReply') {
      const request = pending.get(packet.id); if (!request) continue; pending.delete(packet.id);
      if (packet.runToken !== request.runToken) { request.reject(fail('Stale worker hook reply', 'STALE')); continue; }
      if (packet.ok) request.resolve(packet.result); else request.reject(applyProviderErrorIpcFields(fail(packet.error.message, packet.error.code), packet.error));
    } else if (packet.type === 'memory_yield') { if (active?.runToken === packet.runToken) memoryYield = packet.runToken; }
    else if (packet.type === 'abort') { if (active?.runToken === packet.runToken) active.abort.abort(packet.reason === 'worker_memory' ? fail('Worker memory emergency; inspect last confirmed checkpoint and reconcile uncertain effects', 'WORKER_MEMORY_EMERGENCY') : fail('Worker operation cancelled', 'CANCELLED')); }
    else if (packet.type === 'request') {
      if (requests.size) throw fail('Overlapping worker requests', 'BUSY');
      const work = execute(packet); requests.add(work); work.finally(() => requests.delete(work)).catch(() => process.stdin.destroy());
    } else throw fail('Invalid worker packet', 'PROTOCOL');
  }
} catch (error) { process.stderr.write(`${safeError(error)}\n`); process.exitCode = 1; }
finally {
  ending = true; active?.abort.abort(fail('Supervisor disconnected', 'CLOSED'));
  clearInterval(memoryTimer);
  for (const request of pending.values()) request.reject(fail('Supervisor disconnected', 'CLOSED')); pending.clear();
  const timer = setTimeout(() => process.exit(1), 5000);
  await Promise.allSettled(requests); clearTimeout(timer);
  await writer.close().catch(() => {});
}
