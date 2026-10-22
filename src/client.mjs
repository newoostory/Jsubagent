import { spawn, execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { createConnection } from 'node:net';
import { mkdir, realpath } from 'node:fs/promises';
import { FrameWriter, readFrames } from './protocol.mjs';
import { LIMITS, hash, fail, safeError, jsonClone } from './limits.mjs';
import { bridgeOwner, bridgeSocketPath, isMutatingOperation } from './runtime/pi-bridge.mjs';
import { processIdentity } from './runtime/ownership.mjs';
import { StartupDiagnosticBuffer, clientRuntimeView } from './runtime/provenance.mjs';
import { needsWaitAttention } from './wait-attention.mjs';

const execFileAsync = promisify(execFile);
const hostPath = fileURLToPath(new URL('./host.mjs', import.meta.url));
const peerBootstrap = fileURLToPath(new URL('./runtime/host-peers.mjs', import.meta.url));
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

export function runRoot(agentDir, cwd, mainSessionId) { return join(agentDir, 'pi-agent', 'runtime', 'runs', hash(`${cwd}\0${mainSessionId}`)); }

async function processForest(rootPids) {
  if (process.platform === 'win32') return { supported: false, reason: 'RSS tree sampling requires POSIX ps' };
  const { stdout } = await execFileAsync('ps', ['-axo', 'pid=,ppid=,rss=,lstart='], { maxBuffer: 2 * 1024 * 1024, timeout: 2000, env: { ...process.env, LC_ALL: 'C' } });
  const children = new Map(), processes = new Map();
  for (const line of stdout.trim().split('\n')) {
    const fields = line.trim().split(/\s+/), [pid, parent, rss] = fields.slice(0, 3).map(Number), started = fields.slice(3).join(' ');
    if (![pid, parent, rss].every(Number.isFinite) || !started) continue;
    processes.set(pid, { pid, parent, started, rssBytes: rss * 1024 });
    if (!children.has(parent)) children.set(parent, []); children.get(parent).push(pid);
  }
  const descendants = rootPid => {
    const todo = [rootPid], seen = new Set(), tree = [];
    while (todo.length) {
      const pid = todo.pop(); if (seen.has(pid)) continue; seen.add(pid);
      const row = processes.get(pid); if (row) tree.push(row);
      todo.push(...(children.get(pid) ?? []));
    }
    return tree;
  };
  const roots = [...new Set(rootPids)].map(pid => {
    const tree = descendants(pid);
    return { pid, rssBytes: tree.reduce((n, row) => n + row.rssBytes, 0), processes: tree };
  });
  const unique = new Map(); for (const root of roots) for (const row of root.processes) unique.set(row.pid, row);
  const tree = [...unique.values()];
  return { supported: true, rssBytes: tree.reduce((n, row) => n + row.rssBytes, 0), processes: tree, roots, parents: roots.map(root => processes.get(processes.get(root.pid)?.parent)).filter(row => row && row.pid > 1), sampledAt: Date.now(), note: 'sum of deduplicated RSS; shared pages may be counted multiple times; not a hard limit' };
}

export async function processTreeMemory(rootPid = process.pid) { return processForest([rootPid]); }

function socketConnection(path) {
  return new Promise((resolve, reject) => {
    const socket = createConnection(path);
    const failed = error => { socket.destroy(); reject(error); };
    socket.once('error', failed);
    socket.once('connect', () => { socket.off('error', failed); resolve(socket); });
  });
}

function hostError(frame, runtimeView) {
  const error = fail(frame.error?.message ?? 'Host error', frame.error?.code ?? 'HOST');
  Object.assign(error, frame.error ?? {});
  if (frame.error?.runtime) error.runtime = runtimeView(frame.error.runtime);
  error.bridgeReply = true; return error;
}

function sameHost(left, right) {
  if (!left || !right) return false;
  return left.pid === right.pid && left.generation === right.generation && left.nonce === right.nonce && left.started === right.started;
}

async function verifiedReplacement(previous, next, resultPid) {
  const valid = identity => identity && Number.isSafeInteger(identity.pid) && identity.pid > 0 && typeof identity.started === 'string' && identity.started && typeof identity.nonce === 'string' && identity.nonce && Number.isSafeInteger(identity.generation) && identity.generation > 0;
  if (!valid(previous) || !valid(next) || next.pid !== resultPid || next.generation <= previous.generation) return false;
  try {
    const priorNow = await processIdentity(previous.pid), replacementNow = await processIdentity(next.pid);
    return priorNow !== previous.started && replacementNow === next.started;
  } catch { return false; }
}

export class HostClient {
  constructor({ cwd, agentDir, mainSessionId, trusted = false, root, configPath, piPackageDir, onExit = () => {} }) {
    const run = root ?? runRoot(agentDir, cwd, mainSessionId);
    this.options = { cwd, agentDir, root: run, owner: bridgeOwner(cwd, mainSessionId), trusted, ...(configPath ? { configPath } : {}) };
    this.socketPath = bridgeSocketPath(run); this.pending = new Map(); this.onExit = onExit; this.hostPath = hostPath;
    this.closed = false; this.exited = false; this.connected = false; this.pressure = false;
    this.piPackageDir = piPackageDir;
  }

  runtimeView(remote) { return clientRuntimeView(remote, this.hostPath); }
  decorateResult(operation, result) {
    if (!result || typeof result !== 'object' || Array.isArray(result)) return result;
    if (operation === 'init' || operation === 'status') return { ...result, runtime: this.runtimeView(result.runtime) };
    return result;
  }

  async start() {
    if (this.starting) return this.starting;
    this.starting = this.boot(); return this.starting;
  }

  async boot() {
    if (this.closed) throw fail('Host client closed', 'CLOSED');
    await mkdir(this.options.root, { recursive: true, mode: 0o700 });
    this.options.root = await realpath(this.options.root); this.socketPath = bridgeSocketPath(this.options.root);
    let firstError;
    try { return await this.connectOnce(60_000); }
    catch (error) { firstError = error; if (error.bridgeReply || error.code === 'PERMISSION') throw error; }
    this.spawnHost();
    const deadline = Date.now() + 60_000;
    while (!this.closed && Date.now() < deadline) {
      try {
        if (this.spawnError) throw this.spawnError;
        return await this.connectOnce(Math.max(1, deadline - Date.now()));
      } catch (error) {
        if (error.bridgeReply || error.code === 'PERMISSION' || error === this.spawnError) throw error;
        firstError = error; await sleep(25);
      }
    }
    if (this.spawnError) throw this.spawnError;
    const stderr = this.startupStderr?.text();
    throw fail(`Supervisor did not open its private socket: ${firstError?.message ?? 'startup timed out'}${stderr ? `; startup stderr: ${stderr}` : ''}`, 'HOST_EXIT');
  }

  recordSpawnFailure(reason) {
    if (this.info || this.spawnError) return;
    const stderr = this.startupStderr?.text();
    this.spawnError = fail(`Supervisor failed during startup (${reason})${stderr ? `: ${stderr}` : ''}`, 'HOST_EXIT');
  }

  spawnHost(force = false) {
    if (this.spawned && !force) return;
    this.spawned = true; this.spawnError = undefined; this.startupStderr = new StartupDiagnosticBuffer();
    const child = spawn(process.execPath, ['--max-old-space-size=256', '--import', peerBootstrap, this.hostPath, this.options.root], {
      cwd: this.options.cwd,
      env: { ...process.env, ...(this.piPackageDir ? { JSUBAGENT_PI_PACKAGE_DIR: this.piPackageDir } : {}), NODE_OPTIONS: '', PI_AGENT_HOST: '1', PI_SKIP_VERSION_CHECK: '1', PI_TELEMETRY: '0' },
      stdio: ['ignore', 'ignore', 'pipe'], windowsHide: true, detached: process.platform !== 'win32',
    });
    this.child = child;
    child.stderr.on('data', chunk => this.startupStderr.append(chunk)); child.stderr.on('error', () => {}); child.stderr.unref?.();
    child.once('error', error => this.recordSpawnFailure(`spawn error: ${safeError(error)}`));
    child.once('close', (code, signal) => this.recordSpawnFailure(`exit ${code ?? signal ?? 'unknown'}`));
    child.unref();
  }

  async connectOnce(timeoutMs = 10_000) {
    if (this.closed) throw fail('Host client closed', 'CLOSED');
    const socket = await socketConnection(this.socketPath);
    const writer = new FrameWriter(socket), epoch = randomUUID(), id = randomUUID();
    const iterator = readFrames(socket)[Symbol.asyncIterator]();
    const timeout = setTimeout(() => socket.destroy(fail('Supervisor handshake timed out', 'TIMEOUT')), timeoutMs);
    let frame;
    try {
      await writer.send({ epoch, id, op: 'init', args: this.options });
      const next = await iterator.next();
      if (next.done) throw fail('Supervisor closed during handshake', 'CLOSED');
      frame = next.value;
    } finally { clearTimeout(timeout); }
    if (this.closed) { socket.destroy(); throw fail('Host client closed during handshake', 'CLOSED'); }
    if (frame.epoch !== epoch || frame.id !== id) { socket.destroy(); throw fail('Invalid supervisor handshake reply', 'PROTOCOL'); }
    if (!frame.ok) { socket.destroy(); throw hostError(frame, remote => this.runtimeView(remote)); }
    const runtime = this.runtimeView(frame.result?.runtime), previousIdentity = this.info?.identity;
    let replacement = false;
    if (previousIdentity && !sameHost(previousIdentity, frame.result?.identity)) {
      replacement = await verifiedReplacement(previousIdentity, frame.result?.identity, frame.result?.pid);
      if (!replacement) {
        socket.destroy(); const error = fail(`Supervisor identity changed without a verified replacement; pending mutation outcomes remain uncertain${runtime.diagnostic ? ` (${runtime.diagnostic})` : ''}`, 'HOST_EXIT'); error.runtime = runtime; throw error;
      }
    }
    const token = randomUUID();
    this.socket?.destroy(); this.socket = socket; this.writer = writer; this.epoch = epoch; this.connectionToken = token; this.connected = true; this.exited = false;
    this.info = { ...frame.result, runtime };
    this.reader = this.consume(iterator, socket, token);
    for (const entry of this.pending.values()) {
      if (replacement && isMutatingOperation(entry.op) && (entry.recoveryOnly || sameHost(entry.attemptedIdentity, previousIdentity))) entry.recoveryOnly = true;
      this.send(entry, token);
    }
    return this.info;
  }

  async consume(iterator, socket, token) {
    let error = fail('Supervisor socket closed', 'CLOSED');
    try {
      while (true) {
        const next = await iterator.next(); if (next.done) break;
        const frame = next.value;
        if (frame.epoch !== this.epoch) throw fail('Stale supervisor frame', 'PROTOCOL');
        const request = this.pending.get(frame.id); if (!request) continue;
        this.pending.delete(frame.id);
        if (frame.ok) request.resolve(this.decorateResult(request.op, frame.result)); else request.reject(hostError(frame, remote => this.runtimeView(remote)));
      }
    } catch (caught) { error = caught; }
    finally { this.connectionLost(token, socket, error); }
  }

  connectionLost(token, socket, error) {
    if (this.connectionToken !== token || this.socket !== socket) return;
    this.connected = false; this.writer = undefined; this.socket = undefined; socket.destroy();
    if (this.closed) return;
    // An explicit shutdown is not a crash. Reconnecting here can start a new
    // supervisor after the acknowledged generation dies and strand its run lock.
    if (this.closing) { this.rejectAll(fail('Supervisor socket closed during shutdown; acknowledgement may be uncertain', 'SHUTDOWN_INCOMPLETE')); return; }
    if (error?.code === 'PROTOCOL' || error?.code === 'PERMISSION') { this.hostExited(error); return; }
    if (!this.reconnecting) {
      this.reconnecting = this.reconnect().catch(caught => this.hostExited(caught)).finally(() => { this.reconnecting = undefined; });
    }
  }

  async reconnect() {
    const deadline = Date.now() + 10_000; let last, replacementStarted = false;
    while (!this.closed && !this.closing && Date.now() < deadline) {
      try { return await this.connectOnce(Math.max(1, Math.min(2000, deadline - Date.now()))); }
      catch (error) {
        if (this.closed || this.closing) return;
        if (error.bridgeReply || ['PERMISSION', 'PROTOCOL', 'HOST_EXIT'].includes(error.code)) throw error;
        last = error;
        if (!replacementStarted) {
          const identity = this.info?.identity;
          if (!identity?.started || !Number.isSafeInteger(identity.pid)) throw fail('Original supervisor identity is unavailable; replacement is not authorized', 'HOST_EXIT');
          let stopped;
          try { stopped = await processIdentity(identity.pid) !== identity.started; }
          catch (caught) { throw fail(`Original supervisor death could not be verified (${safeError(caught)})`, 'HOST_EXIT'); }
          if (this.closed || this.closing) return;
          if (stopped) { replacementStarted = true; this.spawnHost(true); }
        }
        await sleep(25);
      }
    }
    if (!this.closed) throw fail(`Supervisor connection was lost; mutation outcomes may be uncertain (${last?.message ?? 'socket unavailable'})`, 'HOST_EXIT');
  }

  hostExited(error) {
    if (this.closed || this.exited) return;
    this.exited = true; this.rejectAll(error?.code === 'HOST_EXIT' ? error : fail(`Supervisor exited; mutation outcomes may be uncertain (${error?.message ?? 'connection lost'})`, 'HOST_EXIT'));
    this.onExit({ code: error?.code ?? 'HOST_EXIT', signal: null });
  }

  request(op, args = {}, timeoutMs = LIMITS.rpcTimeoutMs, signal) {
    if (this.closed || this.exited || !this.starting) return Promise.reject(fail('Host not running', 'CLOSED'));
    if (this.pending.size >= 24) return Promise.reject(fail('Too many pending requests', 'BUSY'));
    if (signal?.aborted) return Promise.reject(signal.reason ?? fail(`RPC ${op} cancelled`, 'CANCELLED'));
    let pinnedArgs;
    try { pinnedArgs = jsonClone(args, LIMITS.frameBytes); }
    catch (error) { return Promise.reject(error); }
    const id = randomUUID();
    return new Promise((resolve, reject) => {
      let timer;
      const cleanup = () => { clearTimeout(timer); signal?.removeEventListener('abort', abort); };
      const settle = (fn, value) => { const entry = this.pending.get(id); if (!entry) return; this.pending.delete(id); cleanup(); fn(value); };
      timer = op === 'wait' && timeoutMs === null ? null : setTimeout(() => settle(reject, fail(`RPC ${op} timed out. Mutation outcome may be uncertain; query its task/message ID instead of blindly retrying.`, 'TIMEOUT')), timeoutMs ?? LIMITS.rpcTimeoutMs);
      const abort = () => settle(reject, signal.reason ?? fail(`RPC ${op} cancelled`, 'CANCELLED'));
      const entry = { id, op, args: pinnedArgs, resolve: value => { cleanup(); resolve(value); }, reject: error => { cleanup(); reject(error); }, timer };
      this.pending.set(id, entry); signal?.addEventListener('abort', abort, { once: true });
      if (signal?.aborted) { abort(); return; }
      if (this.connected) this.send(entry, this.connectionToken);
    });
  }

  send(entry, token) {
    if (!this.connected || token !== this.connectionToken) return;
    entry.attemptedIdentity = this.info?.identity ? { ...this.info.identity } : undefined;
    const writer = this.writer, socket = this.socket;
    writer.send({ epoch: this.epoch, id: entry.id, op: entry.op, args: entry.args, ...(entry.recoveryOnly ? { recoveryOnly: true } : {}) }).catch(error => {
      if (token === this.connectionToken) this.connectionLost(token, socket, error);
    });
  }

  rejectAll(error) { for (const request of this.pending.values()) request.reject(error); this.pending.clear(); }

  async monitor(rootPid = process.pid) {
    const roots = [rootPid, this.info?.pid].filter(Number.isSafeInteger);
    const sample = await processForest(roots);
    sample.mainMemory = process.memoryUsage(); sample.supervisorPid = this.info?.pid;
    sample.supervisor = sample.roots?.find(row => row.pid === this.info?.pid);
    this.memory = sample;
    if (!sample.supported || this.closed || this.exited) return sample;
    const limits = this.info?.limits ?? LIMITS;
    const stopped = limits.memoryStopBytes != null && sample.rssBytes >= limits.memoryStopBytes;
    const paused = stopped || (limits.memorySoftBytes != null && sample.rssBytes >= limits.memorySoftBytes);
    if (paused !== this.pressure) { await this.request('pressure', { paused }); this.pressure = paused; }
    if (stopped) throw fail('Process-tree memory stop threshold reached; new work and foreground continuation are held', 'MEMORY');
    return sample;
  }

  async wait(ids, { timeoutMs, signal, mode = 'all', wakeOnMessage = false, seenMessages = [], includeMetadata = false } = {}) {
    if (mode !== 'all' && mode !== 'any') throw fail('wait mode: expected all or any');
    if (timeoutMs !== undefined && (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0 || timeoutMs > 120_000)) throw fail('wait timeoutMs: expected integer 0..120000');
    if (typeof includeMetadata !== 'boolean') throw fail('wait includeMetadata: expected boolean');
    signal?.throwIfAborted();
    const deadline = timeoutMs === undefined ? Infinity : Date.now() + timeoutMs;
    const snapshotBudget = Math.min(LIMITS.rpcTimeoutMs, deadline - Date.now());
    if (snapshotBudget <= 0) throw fail('Wait timed out before task status could be read', 'TIMEOUT');
    const snapshotAbort = new AbortController();
    const snapshotTimedOut = fail('Wait timed out before task status could be read', 'TIMEOUT');
    const forwardAbort = () => snapshotAbort.abort(signal.reason ?? fail('wait cancelled', 'CANCELLED'));
    const snapshotTimer = setTimeout(() => snapshotAbort.abort(snapshotTimedOut), snapshotBudget);
    signal?.addEventListener('abort', forwardAbort, { once: true });
    let rows;
    try { rows = (await this.request('status', { ids }, snapshotBudget, snapshotAbort.signal)).tasks; }
    catch (error) { if (snapshotAbort.signal.reason === snapshotTimedOut || error?.code === 'TIMEOUT') throw snapshotTimedOut; throw error; }
    finally { clearTimeout(snapshotTimer); signal?.removeEventListener('abort', forwardAbort); }
    const snapshotAt = Date.now(), seen = new Set(seenMessages);
    const settled = mode === 'all' ? rows.every(row => row.settled === true) : rows.some(row => row.settled === true);
    const hasMessage = wakeOnMessage && rows.some(row => needsWaitAttention(row, seen));
    const localResult = (reason, conditionMet, stale = false) => includeMetadata ? { rows, reason, conditionMet, snapshotAt, stale } : rows;
    if (hasMessage) return localResult('attention', settled);
    if (settled) return localResult('condition', true);
    if (Date.now() >= deadline) return localResult('timeout', false);
    signal?.throwIfAborted();
    const waitId = randomUUID(), remaining = deadline - Date.now();
    if (remaining <= 0) return localResult('timeout', false);
    const headroom = Math.min(25, Math.floor(remaining / 4));
    const args = { waitId, ids, ...(timeoutMs === undefined ? {} : { timeoutMs: remaining - headroom, deadline: deadline - headroom }), mode, wakeOnMessage, seenMessages, ...(includeMetadata ? { includeMetadata: true } : {}) };
    let active = true, cancellationSent = false;
    const cancelWait = () => {
      if (!active || cancellationSent || this.closed || this.exited) return;
      cancellationSent = true; this.request('wait_cancel', { waitId }, 2000).catch(() => {});
    };
    const pending = this.request('wait', args, timeoutMs === undefined ? null : remaining, signal);
    signal?.addEventListener('abort', cancelWait, { once: true });
    try { return await pending; }
    catch (error) { cancelWait(); if (error?.code === 'TIMEOUT') return localResult('transport_timeout', false, true); throw error; }
    finally { active = false; signal?.removeEventListener('abort', cancelWait); }
  }

  async detach() {
    this.closed = true; this.connected = false;
    this.socket?.destroy(); this.socket = undefined; this.writer = undefined;
    this.rejectAll(fail('Client detached; logical jobs remain owned by their original run', 'CLOSED'));
    await this.reader?.catch(() => {});
  }

  async close() {
    if (this.closing) return this.closing;
    this.closing = (async () => {
      try {
        if (this.starting && !this.info && !this.closed) await this.starting;
        if (!this.info || this.exited || this.closed) return { stopped: false, saved: false, reason: 'No connected supervisor shutdown was acknowledged' };
        const identity = this.info.identity;
        if (!identity?.started || identity.pid !== this.info.pid) throw fail('Supervisor shutdown identity unavailable', 'SHUTDOWN_INCOMPLETE');
        const receipt = await this.request('shutdown', {}, 30000);
        if (receipt?.saved !== true || receipt.executorsStopped !== true) throw fail('Supervisor did not confirm durable executor shutdown', 'SHUTDOWN_INCOMPLETE');
        const deadline = Date.now() + 5000;
        while (await processIdentity(identity.pid) === identity.started) {
          if (Date.now() >= deadline) throw fail('Supervisor saved state but has not exited', 'SHUTDOWN_INCOMPLETE');
          await sleep(25);
        }
        return { ...receipt, stopped: true };
      } finally {
        this.closed = true; this.connected = false; this.socket?.destroy(); this.socket = undefined; this.writer = undefined;
        this.rejectAll(fail('Host closed', 'CLOSED'));
      }
    })();
    return this.closing;
  }
}
