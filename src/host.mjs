import { createServer } from 'node:net';
import { chmod, lstat, unlink } from 'node:fs/promises';
import { dirname } from 'node:path';
import { FrameWriter, readFrames } from './protocol.mjs';
import { RunStore, acquireLock, privateDir } from './storage.mjs';
import { trustedAcceptance } from './trusted-acceptance.mjs';
import { loadConfiguration } from './config.mjs';
import { catalogFromConfiguration } from './coordinator.mjs';
import { WorkerPool } from './runtime/worker-pool.mjs';
import { TeamCoordinator } from './team.mjs';
import { Engine } from './engine.mjs';
import { boundedHostStatus } from './host-status.mjs';
import { WaitRegistry } from './wait-registry.mjs';
import { BatchRunner } from './batch.mjs';
import { LIMITS, Semaphore, encodeJson, identifier, integer, text, safeError, fail, short } from './limits.mjs';
import { bridgeSocketPath, isMutatingOperation, normalizeBinding, openRequestLedger, persistOrVerifyBinding, requestFingerprint } from './runtime/pi-bridge.mjs';
import { processTreeMemory } from './client.mjs';
import { systemMemoryObservation } from './runtime/memory-policy.mjs';
import { persistShutdown } from './runtime/shutdown.mjs';
import { HostIdleGuard, hostHasWork } from './runtime/host-lifecycle.mjs';
import { WORKER_PROTOCOL } from './runtime/worker-contract.mjs';
import { HOST_PROTOCOL, RUNTIME_PROVENANCE, captureLoadedSource, componentProvenance } from './runtime/provenance.mjs';

const SUPERVISOR_LOADED_SOURCE = captureLoadedSource(import.meta.url, 'supervisor');
const LOADED_SUPERVISOR_RUNTIME = Object.freeze({ provenance: RUNTIME_PROVENANCE, hostProtocol: HOST_PROTOCOL, supervisor: { loaded: SUPERVISOR_LOADED_SOURCE } });

let teamCoordinator;
let hostRoot, socketPath, server, engine, store, waitRegistry, batchRunner, unlock, ledger, bindingClaim, initialization, shutdownPromise, engineShutdown;
let initializingRunner, initializingEngine, initializationFailure;
let ending = false;
let disconnectTimer;
const RECONNECT_GRACE_MS = 1500;
let resourceSample, sampledAt = 0;
async function sampleResources() {
  if (resourceSample) return resourceSample;
  resourceSample = (async () => {
    let sample;
    try { sample = await processTreeMemory(process.pid); }
    catch { sample = { supported: false, sampledAt: Date.now() }; }
    sampledAt = Date.now();
    if (engine && !ending) engine.updateResources({ supported: sample.supported, rssBytes: sample.rssBytes ?? 0, sampledAt: sample.sampledAt ?? sampledAt });
    if (engine && !ending) engine.updateSystemMemory(systemMemoryObservation());
    if (engine && !ending) engine.updateMemorySample(sample);
  })().finally(() => { resourceSample = undefined; });
  return resourceSample;
}
const clients = new Set(), sockets = new Set(), activeRequests = new Map();
const initializers = new Set();
const idleGuard = new HostIdleGuard();
function runtimeStatus() {
  return {
    provenance: RUNTIME_PROVENANCE, hostProtocol: HOST_PROTOCOL,
    supervisor: componentProvenance(SUPERVISOR_LOADED_SOURCE, import.meta.url),
    worker: initializingRunner?.runtimeProvenance?.() ?? { protocol: WORKER_PROTOCOL, onDisk: null, processes: [], sdkVersions: [], state: 'not_loaded' },
  };
}


function closeRegistries() { waitRegistry?.close(); batchRunner?.close(); teamCoordinator?.stop(); }

function stopEngine() {
  if (!engineShutdown) engineShutdown = (async () => {
    closeRegistries();
    await teamCoordinator?.close();
    const ownedEngine = engine ?? initializingEngine;
    if (ownedEngine) await ownedEngine.shutdown(); else await initializingRunner?.shutdown();
  })();
  return engineShutdown;
}

function shutdown() {
  if (shutdownPromise) return shutdownPromise;
  ending = true; server?.close(); closeRegistries(); (engine ?? initializingEngine)?.requestShutdown();
  clearTimeout(disconnectTimer); disconnectTimer = undefined; clearInterval(health);
  shutdownPromise = (async () => {
    // Initialization may already own a warming pool. Let it publish/clean that
    // ownership before taking the final inventory; never strand late workers.
    await Promise.allSettled([...initializers]);
    await initialization?.catch(() => {});
    engine?.requestShutdown(); await resourceSample;
    const operations = {
      drain: async () => {
        const results = await Promise.allSettled([...activeRequests.values()].filter(row => row.operation !== 'shutdown').map(row => row.promise));
        const rejected = results.find(row => row.status === 'rejected'); if (rejected) throw rejected.reason;
        if (initializationFailure) throw initializationFailure;
      },
      stop: () => stopEngine(),
      closeStore: async () => { await store?.close(); },
      closeLedger: async () => { ledger?.close(); ledger = null; },
      release: async () => {
        // Unlink only while this generation owns the socket address.
        try { if (socketPath && unlock) await unlink(socketPath).catch(error => { if (error.code !== 'ENOENT') throw error; }); }
        finally { await unlock?.(); unlock = null; }
      },
    };
    if (hostRoot && unlock) return persistShutdown({ root: hostRoot, identity: unlock.identity, ...operations });
    // Startup failed before acquiring a durable run identity.
    try { await operations.stop(); } finally { try { await operations.closeStore(); } finally { await operations.closeLedger(); await operations.release(); } }
  })();
  return shutdownPromise;
}

async function terminate(code = 0) {
  ending = true;
  server?.close();
  await shutdown().catch(error => { process.stderr.write(`pi-agent host shutdown: ${safeError(error)}\n`); code = 1; });
  for (const socket of sockets) socket.destroy();
  clearInterval(health);
  process.exit(code);
}

process.on('SIGTERM', () => { void terminate(0); });
process.on('SIGINT', () => { void terminate(0); });

const health = setInterval(() => {
  let retirement;
  try {
    retirement = idleGuard.observe({ connected: clients.size > 0, initialized: Boolean(engine),
      busy: activeRequests.size > 0 || !ending && clients.size === 0 && hostHasWork(engine),
      initializing: initializers.size > 0 || sockets.size > clients.size, ending });
  } catch (error) {
    if (engine && !ending) engine.fatal = `Host idle observation failed: ${safeError(error)}`;
    void terminate(1); return;
  }
  // Decide synchronously before another scheduler tick or connection can admit
  // work. terminate() fences admission, drains writes and records shutdown.
  if (retirement && !engine?.scheduling) { void terminate(0); return; }
  if (engine && !ending) engine.kick();
  if (engine && !ending && Date.now() - sampledAt >= 1000) void sampleResources().catch(error => { engine.fatal = `Resource accounting failed: ${safeError(error)}`; });
  if (engine?.fatal && !ending) {
    process.stderr.write(`pi-agent fatal storage error: ${safeError(engine.fatal)}\n`);
    void terminate(1);
  }
}, 500);
health.unref();

function initialize(input) {
  if (ending) return Promise.reject(fail('Supervisor shutting down', 'CLOSED'));
  const work = initializeOwned(input); initializers.add(work);
  work.finally(() => initializers.delete(work)).catch(() => {});
  return work;
}
async function initializeOwned(input) {
  if (ending) throw fail('Supervisor shutting down', 'CLOSED');
  const binding = await normalizeBinding(input, hostRoot);
  if (ending) throw fail('Supervisor stopped during binding verification', 'CLOSED');
  bindingClaim ??= persistOrVerifyBinding(hostRoot, binding);
  await bindingClaim;
  await persistOrVerifyBinding(hostRoot, binding);
  if (ending) throw fail('Supervisor stopped before runtime initialization', 'CLOSED');
  if (!initialization) initialization = (async () => {
    const loadConfig = () => loadConfiguration({ cwd: binding.cwd, agentDir: binding.agentDir, trusted: binding.trusted, configPath: binding.configPath });
    const config = await loadConfig();
    const candidateStore = new RunStore(hostRoot, config.limits);
    store = candidateStore; await candidateStore.init();
    const commands = new Semaphore(config.limits.commandConcurrency);
    initializingRunner = new WorkerPool(config, store, commands, { supervisorRuntime: LOADED_SUPERVISOR_RUNTIME }); await initializingRunner.warm();
    initializingEngine = new Engine(config, store, initializingRunner, loadConfig, trustedAcceptance(store), unlock.identity);
    initializingEngine.connectionHeld = true;
    if (ending) initializingEngine.requestShutdown();
    await initializingEngine.init(); engine = initializingEngine;
    await sampleResources();
    waitRegistry = new WaitRegistry(engine); batchRunner = new BatchRunner(engine);
    teamCoordinator = new TeamCoordinator(engine); await teamCoordinator.init();
    const catalog = catalogFromConfiguration(config);
    return { pid: process.pid, identity: unlock.identity, root: store.root, catalog, roles: catalog.roles, profiles: Object.fromEntries(catalog.profiles.map(({ name, ...profile }) => [name, profile])), limits: config.limits, status: boundedHostStatus(engine), runtime: runtimeStatus() };
  })().catch(error => { initializationFailure = error; throw error; });
  return initialization;
}

async function dispatch(packet) {
  if (!engine) throw fail('Host is not initialized', 'PROTOCOL');
  if (ending && packet.op !== 'shutdown') throw fail('Host shutting down', 'CLOSED');
  const p = packet.args ?? {};
  switch (packet.op) {
    case 'team_start': return teamCoordinator.start(p.team, p.mainModel);
    case 'team_status': return teamCoordinator.status(p.id);
    case 'team_advance': return teamCoordinator.advance(p.id);
    case 'team_cancel': return teamCoordinator.cancel(p.id);
    case 'quality_supersede': return engine.qualitySupersede(p);
    case 'quality_check': return engine.qualityCheck(p.ids);
    case 'wait': return waitRegistry.wait(p);
    case 'wait_cancel': return waitRegistry.cancel(p.waitId);
    case 'spawn_batch': return batchRunner.spawn(p);
    case 'cancel_batch': return batchRunner.cancel(p.batchId, p.ids);
    case 'status': {
      if (p.ids === undefined) {
        if (p.after === undefined) return { ...boundedHostStatus(engine), runtime: runtimeStatus() };
        const after = integer(p.after, 'status after', 0, Number.MAX_SAFE_INTEGER);
        return { ...engine.status({ after }), runtime: runtimeStatus() };
      }
      if (!Array.isArray(p.ids) || p.ids.length < 1 || p.ids.length > 8) throw fail('status ids: expected 1..8 task IDs');
      const ids = p.ids.map((id, index) => identifier(id, `status ids[${index}]`));
      if (new Set(ids).size !== ids.length) throw fail('status ids: expected unique task IDs');
      const tasks = ids.map(id => engine.summary(engine.get(id)));
      return { pid: process.pid, uptimeMs: Date.now() - engine.started, active: engine.running.size, concurrency: engine.limits.concurrency, pressure: engine.pressure, memory: process.memoryUsage(), workers: engine.runner.metrics?.(), artifactBytes: engine.store.bytes, tasks, runtime: runtimeStatus() };
    }
    case 'catalog': { await engine.reloadRuntimeConfiguration(); return catalogFromConfiguration(engine.config); }
    case 'runtime_reload': return engine.reloadRuntimeConfiguration(); // Trusted local control, not a public agent action.
    case 'quota_release': return engine.releaseQuotaHold(p); // Explicit post-login control; never an implicit model fallback.
    case 'set_mode': return engine.setMode(p); // Trusted local control; absent from public/child tools.
    case 'mode_authority': return engine.mutate(async () => {
      if (!Array.isArray(p.names) || p.names.length > 16 || new Set(p.names).size !== p.names.length) throw fail('Invalid mode authority request', 'MODE');
      const names = p.names.map(name => identifier(name, 'mode'));
      // Parent-only observation. Caller-provided roots cannot authorize work.
      return Object.fromEntries(names.map(name => [name, engine.modes.get(name)?.root ? { root: engine.modes.get(name).root } : null]));
    });
    case 'spawn': return engine.spawn(p);
    case 'mission_create': return engine.createMission(p);
    case 'git_snapshot_capture': return engine.captureGitSnapshot(p); // Parent-only; no worker tool exposes this operation.
    case 'file_snapshot_capture': return engine.captureFileSnapshot(p); // Parent-only; never a worker tool.
    case 'work_submit': return engine.submitWorkItem(p.workItem, p.mainModel);
    case 'work_accept': return engine.acceptCandidate(p.id, { use: p.use, reportArtifactId: p.reportArtifactId });
    case 'work_apply': return engine.applyCandidate(p.id, { reportArtifactId: p.reportArtifactId });
    case 'evidence_record': return engine.recordEvidence(p.missionId, p.evidence);
    case 'send': return engine.send('main', p);
    case 'inbox': return engine.inboxMain();
    case 'ack': return engine.acknowledge(p.ids);
    case 'cancel': return engine.cancel(p.id);
    case 'resume': return engine.resume(p.id, p.instruction);
    case 'archive': return engine.archive(p.id);
    case 'inspect': {
      const task = engine.get(p.id);
      const mail = row => { if (!p.compact) return row; const { body, ...metadata } = row; return metadata; };
      return { ...engine.summary(task), ...(p.compact ? {} : { task: task.task, result: task.result, handoff: task.handoff, dependencyInputs: task.dependencyInputs ?? [] }),
        worktree: task.worktree, worktreeIntent: task.worktreeIntent, checkpoint: { omitted: task.checkpoint?.omitted, transcript: task.checkpoint?.transcript },
        pendingMail: task.mailbox.map(mail), uncertainMail: task.delivering.map(mail) };
    }
    case 'artifact': return engine.store.readArtifact(p.id, p.byteOffset, p.limitBytes);
    case 'pressure': {
      if (typeof p.paused !== 'boolean') throw fail('pressure paused: expected boolean', 'PROTOCOL');
      return engine.setForegroundPressure(p.paused);
    }
    case 'shutdown': return { stopping: true, ...await shutdown() };
    default: throw fail(`Unknown operation: ${packet.op}`, 'PROTOCOL');
  }
}

function boundedOutcome(outcome) {
  try { encodeJson(outcome, LIMITS.frameBytes); return outcome; }
  catch (error) {
    if (error.code !== 'LIMIT') throw error;
    return { ok: false, error: { code: 'LIMIT', message: 'Reply exceeds IPC byte budget; inspect an individual task or smaller artifact range.' } };
  }
}

async function execute(packet) {
  try { return boundedOutcome({ ok: true, result: await dispatch(packet) }); }
  catch (error) { return boundedOutcome({ ok: false, error: { code: error.code ?? 'HOST', message: short(safeError(error), 4096), ...(error.code === 'RUNTIME_MISMATCH' ? { runtime: runtimeStatus() } : {}) } }); }
}

async function acceptedOutcome(packet) {
  if (packet.recoveryOnly !== undefined && packet.recoveryOnly !== true) throw fail('Invalid recovery-only request marker', 'PROTOCOL');
  const recoveryOnly = packet.recoveryOnly === true, durable = isMutatingOperation(packet.op);
  if (recoveryOnly && !durable) throw fail('Recovery-only requests require a durably ledgered operation', 'PROTOCOL');
  const fingerprint = requestFingerprint(packet), current = activeRequests.get(packet.id);
  if (current) {
    if (current.fingerprint !== fingerprint || current.operation !== packet.op) throw fail('Request ID reused with different operation or arguments', 'PROTOCOL');
    return current.promise;
  }
  if (durable) {
    if (!ledger) throw fail('Shutdown closed the request ledger; prior acceptance is uncertain', 'UNCERTAIN');
    const saved = ledger.lookup(packet.id);
    if (saved) {
      if (saved.fingerprint !== fingerprint || saved.operation !== packet.op) throw fail('Request ID reused with different operation or arguments', 'PROTOCOL');
      if (saved.state === 'done') return saved.outcome;
      if (!recoveryOnly) throw fail('Pending mutation replay requires an explicit recovery-only request after verified supervisor replacement', 'UNCERTAIN');
      if (!ledger.canRecover(saved)) throw fail('Mutation was accepted by a prior supervisor but its outcome is uncertain; query the task/message ID instead of replaying it', 'UNCERTAIN');
      if (ending) throw fail('Supervisor shutting down before safe recovery dispatch', 'CLOSED');
      // The current ownership generation was acquired only after the prior
      // supervisor was confirmed stopped. CAS the durable dispatch marker first.
      ledger.recoverDispatch(packet.id, fingerprint, packet.op);
    } else {
      if (recoveryOnly) throw fail('Recovery request has no durable acceptance record; it was not dispatched as new work', 'UNCERTAIN');
      if (ending) throw fail('Supervisor shutting down; new request not accepted', 'CLOSED');
      ledger.accept(packet.id, fingerprint, packet.op);
      // Once this marker commits, any crash is uncertain even if execute() was
      // not reached. Only its durable absence is future-safe to recover.
      ledger.beginDispatch(packet.id, fingerprint, packet.op);
    }
  } else {
    if (ledger?.lookup(packet.id)) throw fail('Request ID overlaps a durable mutation record', 'PROTOCOL');
    if (ending && packet.op !== 'shutdown') throw fail('Supervisor shutting down', 'CLOSED');
  }
  const promise = execute(packet).then(outcome => { if (durable) ledger.finish(packet.id, fingerprint, packet.op, outcome); return outcome; });
  activeRequests.set(packet.id, { fingerprint, operation: packet.op, promise });
  promise.finally(() => activeRequests.delete(packet.id)).catch(() => {});
  return promise;
}

async function send(writer, packet, outcome) {
  await writer.send({ epoch: packet.epoch, id: packet.id, ...outcome });
}

async function handle(connection, packet) {
  let outcome;
  try { outcome = await acceptedOutcome(packet); }
  catch (error) { outcome = { ok: false, error: { code: error.code ?? 'HOST', message: short(safeError(error), 4096) } }; }
  try { await send(connection.writer, packet, boundedOutcome(outcome)); }
  finally { if (packet.op === 'shutdown') await terminate(outcome.ok ? 0 : 1); }
}

async function serve(socket) {
  if (sockets.size >= 8) { socket.destroy(); return; }
  sockets.add(socket); socket.setTimeout(10_000, () => { if (!connection.authorized) socket.destroy(); });
  const connection = { socket, writer: new FrameWriter(socket), authorized: false, epoch: undefined, inflight: new Set() };
  try {
    for await (const packet of readFrames(socket)) {
      if (!packet || typeof packet !== 'object' || Array.isArray(packet)) throw fail('Invalid host request', 'PROTOCOL');
      identifier(packet.id); identifier(packet.epoch, 'epoch');
      if (!connection.authorized) {
        if (packet.op !== 'init') throw fail('First socket request must initialize host', 'PROTOCOL');
        let outcome;
        try {
          const info = await initialize(packet.args);
          if (ending) throw fail('Supervisor stopped during initialization', 'CLOSED');
          connection.authorized = true; connection.epoch = packet.epoch; clients.add(connection); socket.setTimeout(0);
          clearTimeout(disconnectTimer); disconnectTimer = undefined;
          await engine.releaseConnectionHold();
          outcome = { ok: true, result: info };
        } catch (error) { outcome = { ok: false, error: { code: error.code ?? 'HOST', message: short(safeError(error), 4096), runtime: runtimeStatus() } }; }
        await send(connection.writer, packet, boundedOutcome(outcome));
        if (!outcome.ok && (!engine || initializationFailure)) setTimeout(() => { void terminate(1); }, 25).unref();
        continue;
      }
      if (packet.op === 'init' || packet.epoch !== connection.epoch) throw fail('Stale or invalid socket epoch', 'PROTOCOL');
      if (connection.inflight.size >= 32) throw fail('Too many pending host requests', 'PROTOCOL');
      const work = handle(connection, packet);
      connection.inflight.add(work);
      work.finally(() => connection.inflight.delete(work)).catch(() => socket.destroy());
    }
  } catch (error) {
    if (!ending && error.code !== 'CLOSED' && error.code !== 'ECONNRESET') process.stderr.write(`pi-agent host connection: ${safeError(error)}\n`);
  } finally {
    sockets.delete(socket); clients.delete(connection); socket.destroy();
    if (connection.authorized && clients.size === 0 && engine && !ending) {
      clearTimeout(disconnectTimer);
      disconnectTimer = setTimeout(() => {
        disconnectTimer = undefined;
        if (clients.size || ending) return;
        void engine.holdForeground().catch(error => { engine.fatal = `Foreground hold failed: ${safeError(error)}`; });
      }, RECONNECT_GRACE_MS);
      disconnectTimer.unref();
    }
  }
}

async function listen() {
  const requestedRoot = text(process.argv[2], 'run root', 4096);
  socketPath = bridgeSocketPath(requestedRoot);
  hostRoot = await privateDir(requestedRoot);
  await chmod(hostRoot, 0o700);
  unlock = await acquireLock(hostRoot); // Ownership precedes every state database open.
  ledger = await openRequestLedger(hostRoot, unlock.identity);
  await privateDir(dirname(socketPath));
  const socketDirectory = await lstat(dirname(socketPath));
  if (!socketDirectory.isDirectory() || socketDirectory.isSymbolicLink() || socketDirectory.uid !== process.getuid()) throw fail('Unsafe private socket directory', 'PERMISSION');
  await chmod(dirname(socketPath), 0o700);
  try {
    const info = await lstat(socketPath);
    if (!info.isSocket() || info.isSymbolicLink()) throw fail('Unsafe bridge socket path', 'STORAGE');
    await unlink(socketPath);
  } catch (error) { if (error.code !== 'ENOENT') throw error; }
  server = createServer(socket => { void serve(socket); });
  server.on('error', error => { process.stderr.write(`pi-agent host server: ${safeError(error)}\n`); void terminate(1); });
  await new Promise((resolve, reject) => {
    const error = caught => { server.off('listening', ready); reject(caught); };
    const ready = () => { server.off('error', error); resolve(); };
    server.once('error', error); server.once('listening', ready); server.listen(socketPath);
  });
  await chmod(socketPath, 0o600);
}

try { await listen(); }
catch (error) {
  process.stderr.write(`pi-agent host: ${safeError(error)}\n`);
  await shutdown().catch(() => {});
  clearInterval(health); process.exit(1);
}
