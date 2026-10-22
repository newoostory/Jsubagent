import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { statSync } from 'node:fs';
import { FrameWriter, readFrames } from '../protocol.mjs';
import { LIMITS, MAX_RUNTIME_CONCURRENCY, integer, Semaphore, fail, safeError, short, hash } from '../limits.mjs';
import { TailBuffer } from '../storage.mjs';
import { processIdentity, waitOwnedExecutorStopped } from './ownership.mjs';
import { applyProviderErrorIpcFields, providerErrorIpcFields } from './provider-fault.mjs';
import { WORKER_PROTOCOL, assertWorkerProtocol } from './worker-contract.mjs';
import { WorkerMemoryGuard, memoryPolicy } from './memory-policy.mjs';
import { executorMemoryKey } from './shared-memory.mjs';
import { RUNTIME_PROVENANCE, observeSource, protocolMismatchDiagnostic, sourceState } from './provenance.mjs';

const workerPath = fileURLToPath(new URL('./worker.mjs', import.meta.url));
const peerBootstrap = fileURLToPath(new URL('./host-peers.mjs', import.meta.url));
const HOOKS = new Set(['send', 'inbox', 'takeSteering', 'checkpoint', 'saveCheckpoint', 'beginWeb', 'finishWeb', 'beginEffect', 'finishEffect', 'shouldHold', 'reserveUsage', 'settleUsage', 'publishResult', 'readResult']);

// One session per process is deliberate until co-location compatibility is measured.
// Every path, including validate/resume, uses this same bounded pool.
export async function createWorkerPool(config, store, commands) {
  const pool = new WorkerPool(config, store, commands);
  try { await pool.warm(); return pool; } catch (error) { await pool.close(); throw error; }
}

function fileVersion(path) {
  if (!path) return null;
  try { const info = statSync(path); return { size: info.size, modified: info.mtimeMs, inode: info.ino }; }
  catch (error) { return { missing: true, code: error?.code }; }
}

const WORKER_ERROR_KEYS = new Set(['message', 'code', 'checkpoint', 'transcript', 'usage', 'providerFault', 'requestOutcome', 'usageState']);
function restoreWorkerError(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).some(key => !WORKER_ERROR_KEYS.has(key))) throw fail('Invalid worker error envelope', 'PROTOCOL');
  if (typeof value.message !== 'string' || Buffer.byteLength(value.message) > 4096 || typeof value.code !== 'string' || !/^[A-Z0-9_]{1,64}$/.test(value.code)) throw fail('Invalid worker error identity', 'PROTOCOL');
  const error = fail(value.message, value.code);
  for (const key of ['checkpoint', 'transcript', 'usage']) if (value[key] !== undefined) error[key] = value[key];
  return applyProviderErrorIpcFields(error, value);
}

function normalizeWorkerRuntime(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || value.provenance !== RUNTIME_PROVENANCE) return null;
  const raw = value.worker?.loaded; let loaded = null;
  if (raw && typeof raw === 'object' && !Array.isArray(raw) && typeof raw.path === 'string' && Buffer.byteLength(raw.path) <= 4096) {
    const sha256 = typeof raw.sha256 === 'string' && /^[a-f0-9]{64}$/.test(raw.sha256) ? raw.sha256 : undefined;
    const bytes = Number.isSafeInteger(raw.bytes) && raw.bytes >= 0 && raw.bytes <= 2 * 1024 * 1024 ? raw.bytes : undefined;
    const capturedAt = Number.isSafeInteger(raw.capturedAt) && raw.capturedAt >= 0 ? raw.capturedAt : undefined;
    loaded = { component: 'worker', path: raw.path, ...(sha256 ? { sha256 } : {}), ...(bytes !== undefined ? { bytes } : {}), ...(capturedAt !== undefined ? { capturedAt } : {}), ...(!sha256 ? { unavailable: true, code: typeof raw.code === 'string' ? short(raw.code, 64) : 'SOURCE_UNAVAILABLE' } : {}) };
  }
  const loadedVersion = typeof value.sdk?.loadedVersion === 'string' && Buffer.byteLength(value.sdk.loadedVersion) <= 128 ? value.sdk.loadedVersion : null;
  return { provenance: RUNTIME_PROVENANCE, workerProtocol: Number.isSafeInteger(value.workerProtocol) ? value.workerProtocol : null, worker: { loaded }, sdk: { loadedVersion } };
}

function runtimeFingerprint(config) {
  return hash(JSON.stringify({
    workerProtocol: WORKER_PROTOCOL,
    cwd: config.cwd, agentDir: config.agentDir, authPath: config.authPath, authVersion: fileVersion(config.authPath),
    modelsPath: config.modelsPath, modelsVersion: fileVersion(config.modelsPath),
    providerModules: (config.providerModules ?? []).map(path => ({ path, version: fileVersion(path) })),
    limits: config.limits,
  }));
}

function pause(ms) {
  return new Promise(resolve => { const timer = setTimeout(resolve, ms); timer.unref?.(); });
}

export class WorkerPool {
  pendingCleanup = new Set();
  constructor(config, store, commands, options = {}) {
    this.config = config; this.store = store; this.commands = commands;
    this.maxWorkers = integer(options.maxWorkers ?? config.limits.workerMax ?? LIMITS.workerMax, 'workerMax', 1, MAX_RUNTIME_CONCURRENCY);
    // Internal fixtures/embedders may request lazy startup; public configuration
    // still requires at least one initial worker.
    this.initialWorkers = Math.min(integer(options.initialWorkers ?? config.limits.workerInitial ?? LIMITS.workerInitial, 'workerInitial', options.initialWorkers === undefined ? 1 : 0, MAX_RUNTIME_CONCURRENCY), this.maxWorkers);
    this.workerPath = options.worker ?? workerPath; this.workers = new Set(); this.slots = new Semaphore(this.maxWorkers); this.closed = false;
    this.supervisorRuntime = options.supervisorRuntime;
    this.configEpoch = runtimeFingerprint(config);
    this.startTimeoutMs = options.startTimeoutMs ?? 30000;
    if (!Number.isSafeInteger(this.startTimeoutMs) || this.startTimeoutMs < 1 || this.startTimeoutMs > 60000) throw fail('Invalid worker startup timeout');
    this.idleTimer = setInterval(() => { this.retireIdle().catch(() => {}); }, 5000); this.idleTimer.unref();
  }
  async warm() {
    if (this.closed) throw fail('Worker pool closed', 'CLOSED');
    const starts = [];
    const current = () => [...this.workers].filter(worker => !worker.retiring && worker.configEpoch === this.configEpoch).length;
    while (current() < this.initialWorkers && this.workers.size < this.maxWorkers) {
      const worker = new WorkerConnection(this, this.config, this.configEpoch); this.workers.add(worker); starts.push(worker.start());
    }
    const results = await Promise.allSettled(starts);
    const failed = results.find(result => result.status === 'rejected'); if (failed) throw failed.reason;
  }
  async validate(profile) {
    // Authentication/model availability is validated in run(), after the common
    // admission reservation. This check must never select a substitute model.
    if (!profile?.provider || !profile?.model) throw fail('Explicit configured model required', 'MODEL');
  }
  async run(task, signal, hooks) { return this.withWorker(task, hooks, signal, worker => worker.request('run', { task, runToken: worker.runToken }, worker.runToken)); }
  async withWorker(task, hooks, signal, action) {
    const release = await this.slots.acquire(signal);
    let worker;
    try {
      if (this.parking) await this.parking;
      if (this.replacing) await this.replacing;
      if (this.closed) throw fail('Worker pool closed', 'CLOSED');
      signal?.throwIfAborted(); this.parked = false;
      worker = task.memoryPlan?.cold ? undefined : [...this.workers].find(row => !row.busy && !row.dead && !row.closing && !row.retiring && row.configEpoch === this.configEpoch && (!task.memoryPlan?.workerKey || row.identity && executorMemoryKey(row.identity) === task.memoryPlan.workerKey));
      // Admission can yield to idle retirement while preparing the workspace.
      // Re-enter host memory admission; a warm reservation cannot fund a cold
      // replacement, and no executor/provider request has started here.
      if (task.memoryPlan?.workerKey && !worker) throw Object.assign(fail('Reserved memory worker is no longer available', 'WORKER_ADMISSION_STALE'), { requestOutcome: 'rejected_before_execution' });
      if (!worker) {
        if (this.workers.size >= this.maxWorkers) throw fail('Worker process budget unavailable after slot admission', 'BUSY');
        worker = new WorkerConnection(this, this.config, this.configEpoch); this.workers.add(worker);
      }
      worker.busy = true; worker.task = task; worker.hooks = hooks; worker.signal = signal; worker.runToken = randomUUID();
      worker.callsAbort = new AbortController();
      const abort = () => {
        if (!worker.ready) { worker.child?.kill('SIGKILL'); return; }
        worker.writer.send({ type: 'abort', generation: worker.generation, runToken: worker.runToken }).catch(() => {});
        worker.killTimer ??= setTimeout(() => worker.child.kill('SIGKILL'), 5000);
      };
      signal?.addEventListener('abort', abort, { once: true });
      try {
        signal?.throwIfAborted();
        try { await worker.start(); } catch (error) { await worker.close(); this.workers.delete(worker); throw error; }
        signal?.throwIfAborted();
        await hooks?.executorStarted?.(worker.identity);
        signal?.throwIfAborted();
        return await action(worker);
      } finally { signal?.removeEventListener('abort', abort); }
    } finally {
      try {
        if (worker) {
          clearTimeout(worker.killTimer); worker.killTimer = null;
          worker.runToken = null; worker.callsAbort?.abort(fail('Worker run settled', 'STALE'));
          let cleanupError; try { await worker.cleanup(); } catch (error) { cleanupError = error; }
          worker.task = null; worker.hooks = null; worker.signal = null; worker.busy = false;
          worker.idleSince = Date.now();
          const limits = worker.config.limits;
          if (!cleanupError && (this.parked || worker.retiring || ['yield_requested', 'emergency'].includes(worker.memoryGuard.state) || (limits.workerMaxRuns > 0 && worker.runs >= limits.workerMaxRuns))) await worker.close();
          if (cleanupError) throw cleanupError;
        }
      } finally { release(); }
    }
  }
  metrics() { return [...this.workers].map(w => ({ pid: w.child?.pid, identity: w.identity ?? { nonce: w.generation }, configEpoch: w.configEpoch, retiring: Boolean(w.retiring || w.closing), busy: w.busy, taskId: w.task?.id, memory: w.memory, memoryState: w.memoryGuard.state, memorySampleValid: Boolean(w.memorySampleValid && Date.now() - w.memory.sampledAt <= w.memoryGuard.policy.sampleMaxAgeMs) })); }
  runtimeProvenance() {
    const onDisk = observeSource(this.workerPath, 'worker');
    const processes = [...this.workers].map(worker => {
      const known = worker.runtime?.provenance === RUNTIME_PROVENANCE;
      const loaded = known ? worker.runtime.worker.loaded : null;
      return {
        pid: worker.identity?.pid ?? worker.child?.pid,
        loaded, onDisk, state: known ? sourceState(loaded, onDisk) : 'unknown_legacy',
        sdk: { loadedVersion: known ? worker.runtime.sdk.loadedVersion : null },
      };
    });
    const sdkVersions = [...new Set(processes.map(row => row.sdk.loadedVersion).filter(Boolean))];
    return { protocol: WORKER_PROTOCOL, onDisk, processes, sdkVersions, state: processes.length ? (processes.some(row => row.state === 'unknown_legacy') ? 'partially_unknown' : processes.some(row => row.state === 'drift') ? 'drift' : processes.some(row => row.state === 'unavailable') ? 'unavailable' : 'loaded') : 'not_loaded' };
  }
  async retireIdle(now = Date.now(), pressure = false) {
    if (this.closed || this.replacing || this.parking || this.slots.waiters.length) return;
    // acquire() reserves a slot before withWorker's continuation marks it busy.
    if (this.slots.active > [...this.workers].filter(worker => worker.busy).length) return;
    const policy = memoryPolicy(this.config.limits);
    const idle = [...this.workers].filter(worker => worker.ready && !worker.busy && !worker.retiring && !worker.closing && !worker.dead).sort((a, b) => b.idleSince - a.idleSince);
    await Promise.all(idle.slice(pressure ? 0 : policy.idleKeep).filter(worker => pressure || now - worker.idleSince >= policy.idleRetireMs).map(worker => worker.close()));
  }
  async park() {
    this.parked = true;
    if (this.parking) return this.parking;
    this.parking = Promise.all([...this.workers].filter(worker => !worker.busy).map(worker => worker.close())).finally(() => { this.parking = null; });
    return this.parking;
  }
  async reconfigure(config) {
    if (this.closed) throw fail('Worker pool closed', 'CLOSED');
    if (this.replacing) await this.replacing;
    const epoch = runtimeFingerprint(config);
    if (epoch === this.configEpoch) return { changed: false, configEpoch: epoch };
    const prior = { config: this.config, epoch: this.configEpoch, maxWorkers: this.maxWorkers, initialWorkers: this.initialWorkers };
    const nextMax = integer(config.limits.workerMax ?? LIMITS.workerMax, 'workerMax', 1, MAX_RUNTIME_CONCURRENCY), nextInitial = Math.min(integer(config.limits.workerInitial ?? LIMITS.workerInitial, 'workerInitial', 1, MAX_RUNTIME_CONCURRENCY), nextMax);
    // Fence every old process synchronously before the first await. Busy sessions
    // finish on their immutable config; idle sessions retire before replacements.
    for (const worker of this.workers) worker.retiring = true;
    this.config = config; this.configEpoch = epoch; this.maxWorkers = nextMax; this.initialWorkers = nextInitial;
    this.replacing = (async () => {
      try {
        await Promise.all([...this.workers].filter(worker => !worker.busy).map(worker => worker.close()));
        await this.warm();
        // Publish capacity only after replacement initialization succeeds. A
        // failed attempt must not grant permits which rollback cannot revoke.
        this.slots.resize(nextMax); this.commands.resize?.(config.limits.commandConcurrency);
      } catch (error) {
        await Promise.allSettled([...this.workers].filter(worker => worker.configEpoch === epoch && !worker.busy).map(worker => worker.close()));
        this.config = prior.config; this.configEpoch = prior.epoch; this.maxWorkers = prior.maxWorkers; this.initialWorkers = prior.initialWorkers;
        for (const worker of this.workers) if (worker.configEpoch === prior.epoch && !worker.closing && !worker.dead) worker.retiring = false;
        try { await this.warm(); } catch (restoreError) { throw new AggregateError([error, restoreError], 'Worker replacement and warm-floor restoration failed'); }
        throw error;
      }
    })().finally(() => { this.replacing = null; });
    await this.replacing;
    return { changed: true, configEpoch: epoch, retiring: [...this.workers].filter(worker => worker.retiring && worker.busy).length };
  }
  async shutdown() {
    if (this.shutdownPromise) return this.shutdownPromise;
    this.shutdownPromise = (async () => {
      const owned = new Set([...this.workers, ...this.pendingCleanup]), errors = [];
      try { await this.close(); } catch (error) { errors.push(error); }
      for (const worker of this.pendingCleanup) owned.add(worker);
      const guardians = new Map();
      for (const worker of owned) for (const { identity } of worker.guardians.values()) guardians.set(executorMemoryKey(identity), identity);
      const stopped = await Promise.allSettled([...guardians.values()].map(identity => waitOwnedExecutorStopped(identity)));
      for (const row of stopped) if (row.status === 'rejected') errors.push(row.reason);
      for (const worker of owned) {
        try { await worker.cleanup(); } catch (error) { errors.push(error); }
        if (worker.child && !worker.dead) errors.push(fail('Worker remains alive after shutdown', 'EXECUTOR_UNCERTAIN'));
      }
      if (errors.length) throw fail(`Worker shutdown incomplete: ${errors.map(safeError).join('; ')}`, 'EXECUTOR_UNCERTAIN');
    })();
    return this.shutdownPromise;
  }
  async close() {
    if (this.closing) return this.closing;
    this.closed = true;
    clearInterval(this.idleTimer);
    this.closing = Promise.allSettled([...this.workers].map(w => w.close())).then(results => {
      const failed = results.find(row => row.status === 'rejected' && row.reason?.code !== 'EXECUTOR_UNCERTAIN'); if (failed) throw failed.reason;
    });
    return this.closing;
  }
}
class WorkerConnection {
  constructor(pool, config = pool.config, configEpoch = pool.configEpoch) { this.pool = pool; this.config = config; this.configEpoch = configEpoch; this.generation = randomUUID(); this.pending = new Map(); this.artifacts = new Map(); this.reservations = new Map(); this.stderr = new TailBuffer(8192); this.runs = 0; this.memoryGuard = new WorkerMemoryGuard(memoryPolicy(config.limits)); }
  observeMemory(sample) {
    if (!this.identity) { this.initialSample = sample; return; }
    const decision = this.memoryGuard.observe(sample, { identity: this.identity, scope: this.pool.store.root, runToken: this.runToken });
    this.memorySampleValid = decision.valid;
    if (!decision.valid) return;
    this.memory = sample;
    if (decision.state === 'emergency') {
      this.retiring = true;
      if (this.busy && this.runToken && !this.memoryEmergency) {
        this.memoryEmergency = true;
        this.writer.send({ type: 'abort', generation: this.generation, runToken: this.runToken, reason: 'worker_memory' }).catch(() => {});
        this.killTimer ??= setTimeout(() => this.child.kill('SIGKILL'), this.memoryGuard.policy.emergencyGraceMs);
      } else if (!this.busy) this.close().catch(() => {});
    } else if (decision.state === 'yield_requested' && this.busy && this.runToken) {
      this.writer.send({ type: 'memory_yield', generation: this.generation, runToken: this.runToken }).catch(() => {});
    }
  }
  guardians = new Map();
  async start() {
    if (this.started) return this.started;
    this.started = (async () => {
      const heapMiB = this.config.limits.workerHeapMiB;
      const nodeArgs = heapMiB == null ? [] : [`--max-old-space-size=${heapMiB}`];
      this.child = spawn(process.execPath, [...nodeArgs, '--import', peerBootstrap, this.pool.workerPath], { cwd: this.config.cwd, env: { ...process.env, NODE_OPTIONS: '', PI_AGENT_WORKER: '1', PI_SKIP_VERSION_CHECK: '1', PI_TELEMETRY: '0' }, stdio: ['pipe', 'pipe', 'pipe'], windowsHide: true });
      this.writer = new FrameWriter(this.child.stdin);
      this.child.stderr.on('data', chunk => this.stderr.append(chunk));
      this.exited = new Promise(resolve => {
        this.child.once('error', error => this.reject(error));
        this.child.once('close', (code, signal) => {
          this.dead = true; this.pool.workers.delete(this);
          const error = fail(`Worker exited (${code ?? signal}); ${this.stderr.text()}`, this.memoryEmergency ? 'WORKER_MEMORY_EMERGENCY' : 'WORKER_EXIT');
          error.executorStopped = true; this.reject(error); resolve();
        });
      });
      this.reader = (async () => {
        try { for await (const packet of readFrames(this.child.stdout)) {
          if (packet.generation !== this.generation) throw fail('Stale worker generation', 'PROTOCOL');
          if (packet.type === 'reply') {
            const request = this.pending.get(packet.id); if (!request) continue;
            this.pending.delete(packet.id);
            if (request.runToken !== packet.runToken) { this.retiring = true; request.reject(fail('Stale worker run reply', 'STALE')); continue; }
            if (packet.ok) request.resolve(packet.result); else request.reject(restoreWorkerError(packet.error));
          } else if (packet.type === 'call') {
            // Calls may overlap (two tool output streams); their writes remain awaited.
            this.handle(packet).catch(error => { this.reject(error); this.child.kill('SIGTERM'); });
          } else if (packet.type === 'progress') {
            if (packet.runToken === this.runToken && this.task && !this.signal?.aborted) this.hooks?.progress?.(packet.value);
          } else if (packet.type === 'memory') this.observeMemory(packet.value);
          else throw fail('Unknown worker packet', 'PROTOCOL');
        } } catch (error) { this.reject(error); this.child.kill('SIGTERM'); }
      })();
      let timer, handshake;
      try {
        handshake = await Promise.race([this.request('init', { config: this.config, root: this.pool.store.root, workerProtocol: WORKER_PROTOCOL, supervisorRuntime: this.pool.supervisorRuntime }), new Promise((_, reject) => {
          timer = setTimeout(() => { this.child.kill('SIGKILL'); reject(fail('Worker initialization timed out', 'WORKER_START_TIMEOUT')); }, this.pool.startTimeoutMs);
        })]);
      } finally { clearTimeout(timer); }
      this.runtime = normalizeWorkerRuntime(handshake?.runtime);
      try { assertWorkerProtocol(handshake?.workerProtocol); }
      catch (error) {
        error.message = `${error.message} (${protocolMismatchDiagnostic({ expected: WORKER_PROTOCOL, received: handshake?.workerProtocol, workerLoaded: this.runtime?.worker?.loaded, supervisorLoaded: this.pool.supervisorRuntime?.supervisor?.loaded, sdkVersion: this.runtime?.sdk?.loadedVersion })})`;
        throw error;
      }
      this.identity = { pid: handshake?.pid, startedAt: handshake?.startedAt, nonce: handshake?.nonce, workerProtocol: handshake?.workerProtocol };
      if (this.identity.nonce !== this.generation || this.identity.pid !== this.child.pid || !Number.isSafeInteger(this.identity.startedAt)) throw fail('Worker identity handshake mismatch', 'PROTOCOL');
      this.identity.started = await processIdentity(this.child.pid);
      if (!this.identity.started) throw fail('Worker stopped before execution fence registration', 'WORKER_EXIT');
      this.ready = true;
      this.idleSince = Date.now();
      if (this.initialSample) { const sample = this.initialSample; this.initialSample = undefined; this.observeMemory(sample); }
    })();
    return this.started;
  }
  request(op, args, runToken) {
    if (this.dead) return Promise.reject(fail('Worker stopped', 'WORKER_EXIT'));
    const id = randomUUID();
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject, runToken });
      this.writer.send({ type: 'request', generation: this.generation, id, op, args }).catch(error => { this.pending.delete(id); reject(error); });
      if (op === 'run') this.runs++;
    });
  }
  reject(error) { for (const request of this.pending.values()) request.reject(error); this.pending.clear(); }
  async handle(packet) {
    let result, error;
    try {
      if (packet.runToken !== this.runToken) throw fail('Stale worker session token', 'STALE');
      if (this.dead || !this.task) throw fail('Worker has no active owned task', 'CLOSED');
      const { op, args = [] } = packet;
      if (op === 'memoryRecheck') {
        this.observeMemory(args[0]);
        if (!this.memorySampleValid) throw fail('Invalid memory recheck sample', 'PROTOCOL');
        result = { state: this.memoryGuard.state };
      } else if (HOOKS.has(op)) {
        if (this.signal?.aborted && !['finishEffect', 'saveCheckpoint', 'checkpoint', 'finishWeb', 'settleUsage'].includes(op)) this.signal.throwIfAborted();
        if (!this.hooks?.[op]) throw fail(`Worker hook unavailable: ${op}`, 'CAPABILITY');
        result = await this.hooks[op](...args);
      } else if (op === 'artifact') {
        if (args[0] !== this.task.id) throw fail('Artifact owner mismatch', 'PERMISSION');
        const artifact = await this.pool.store.artifact(...args);
        if (packet.runToken !== this.runToken || this.dead) { await artifact.finish(false, 'Originating worker session settled'); throw fail('Stale worker session', 'STALE'); }
        this.artifacts.set(artifact.info.id, artifact); result = artifact.info;
      } else if (op === 'artifactWrite') {
        const artifact = this.artifacts.get(args[0]); if (!artifact) throw fail('Unknown staged artifact', 'PERMISSION');
        const chunk = Buffer.from(args[1], 'base64'); if (chunk.length > 65536) throw fail('Worker write chunk exceeds budget', 'LIMIT');
        await artifact.write(chunk); result = { bytes: artifact.info.bytes };
      } else if (op === 'artifactFinish') {
        const artifact = this.artifacts.get(args[0]); if (!artifact) throw fail('Unknown staged artifact', 'PERMISSION');
        result = await artifact.finish(args[1], args[2]); this.artifacts.delete(args[0]);
      } else if (['artifactInfo', 'artifactIdentity', 'readArtifact'].includes(op)) {
        // Existing read grants are enforced by SDK safe/work-item tools, scoped to this run store.
        result = await this.pool.store[op](...args);
      } else if (op === 'commandAcquire') {
        const signals = [this.signal, this.callsAbort?.signal].filter(Boolean);
        const release = await this.pool.commands.acquire(signals.length ? AbortSignal.any(signals) : undefined);
        if (packet.runToken !== this.runToken || this.dead || !this.task) { release(); throw fail('Stale worker command reservation', 'STALE'); }
        const id = randomUUID(); this.reservations.set(id, release); result = id;
      } else if (op === 'commandTrack' || op === 'commandUntrack') {
        const [owner, identity] = args;
        if (owner !== this.task.id || !Number.isSafeInteger(identity?.pid) || typeof identity.started !== 'string' || typeof identity.nonce !== 'string') throw fail('Invalid command executor fence', 'PERMISSION');
        if (op === 'commandTrack') {
          if (!this.reservations.size || this.guardians.size >= this.config.limits.commandConcurrency) throw fail('Unreserved command executor', 'PERMISSION');
          this.guardians.set(identity.nonce, { owner, identity });
          await this.pool.commands.trackProcess?.(owner, identity);
        } else {
          const known = this.guardians.get(identity.nonce);
          if (!known || known.identity.pid !== identity.pid || known.identity.started !== identity.started) throw fail('Unknown command executor', 'STALE');
          if (await processIdentity(identity.pid) === identity.started) throw fail('Command executor is still alive', 'BUSY');
          await this.pool.commands.untrackProcess?.(owner, identity); this.guardians.delete(identity.nonce);
        }
        result = true;
      } else if (op === 'commandRelease') {
        if (this.guardians.size) throw fail('Command executor must stop before releasing capacity', 'BUSY');
        this.reservations.get(args[0])?.(); this.reservations.delete(args[0]); result = true;
      }
      else throw fail(`Unknown worker operation ${op}`, 'PROTOCOL');
    } catch (e) { error = { message: short(safeError(e), 4096), code: e.code ?? 'WORKER', ...providerErrorIpcFields(e) }; }
    await this.writer.send({ type: 'callReply', generation: this.generation, runToken: packet.runToken, id: packet.id, ok: !error, result, error });
  }
  async cleanup() {
    if (this.cleaning) return this.cleaning;
    const cleaning = (async () => {
      const artifacts = await Promise.allSettled([...this.artifacts.values()].map(a => a.finish(false, 'Worker did not seal artifact'))); this.artifacts.clear();
      const artifactFailure = artifacts.find(row => row.status === 'rejected');
      if (artifactFailure) { this.cleanupFailure ??= artifactFailure.reason; this.pool.pendingCleanup.add(this); }
      for (const [nonce, { owner, identity }] of this.guardians) {
        try {
          if (await processIdentity(identity.pid) === identity.started) continue;
          try { await this.pool.commands.untrackProcess?.(owner, identity); } catch (error) { this.cleanupFailure ??= error; this.pool.pendingCleanup.add(this); }
          this.guardians.delete(nonce);
        } catch {}
      }
      if (this.guardians.size) {
        this.retiring = true; this.pool.pendingCleanup.add(this); this.reconcileGuardians();
        const error = fail('Command guardian remains alive after bounded worker cleanup', 'EXECUTOR_UNCERTAIN'); error.executorStopped = false; throw error;
      }
      for (const release of this.reservations.values()) release(); this.reservations.clear();
      if (this.cleanupFailure) throw this.cleanupFailure;
      this.pool.pendingCleanup.delete(this);
    })();
    this.cleaning = cleaning;
    try { return await cleaning; } finally { if (this.cleaning === cleaning) this.cleaning = null; }
  }
  reconcileGuardians() {
    if (this.reconciling) return;
    this.reconciling = (async () => {
      while (this.guardians.size) {
        await pause(100);
        for (const [nonce, row] of this.guardians) {
          try {
            if (await processIdentity(row.identity.pid) === row.identity.started) continue;
            if (!this.pool.closed) try { await this.pool.commands.untrackProcess?.(row.owner, row.identity); } catch (error) { this.cleanupFailure ??= error; this.pool.pendingCleanup.add(this); }
            this.guardians.delete(nonce);
          } catch {}
        }
      }
      for (const release of this.reservations.values()) release(); this.reservations.clear();
      if (!this.cleanupFailure) this.pool.pendingCleanup.delete(this);
      if (!this.dead) await this.close().catch(() => {});
    })().catch(() => {});
  }
  async close() {
    if (this.closing) return this.closing;
    if (!this.child || this.dead) return;
    // Fence selection synchronously before yielding while the process exits.
    this.closing = (async () => {
      this.child.stdin.end();
      const kill = setTimeout(() => this.child.kill('SIGKILL'), 5000);
      try { await this.exited; } finally { clearTimeout(kill); await this.cleanup(); }
    })();
    return this.closing;
  }
}
