import { createHash } from 'node:crypto';

export const KiB = 1024;
export const MiB = 1024 * KiB;
export const GiB = 1024 * MiB;
// Runtime capacity is distinct from the public tool's eight-item batch limit.
export const MAX_RUNTIME_CONCURRENCY = 16;
export const LIMITS = Object.freeze({
  concurrency: 8, commandConcurrency: 8, maxTasks: 128,
  frameBytes: MiB, queueBytes: 2 * MiB, previewBytes: 32 * KiB,
  readBytes: 64 * KiB, inputBytes: 64 * KiB, roleBytes: 32 * KiB,
  messageBytes: 8 * KiB, mailboxCount: 32, mailboxBytes: 256 * KiB,
  checkpointBytes: 512 * KiB, stateBytes: 2 * MiB,
  responseBytes: 2 * MiB, contextBytes: 16 * MiB,
  // maxTurns is a checkpointed scheduling slice, never a cumulative work cap.
  editBytes: 2 * MiB, maxTurns: 128, rootTurns: null, autoWakeLimit: null,
  rootTimeMs: null, rootStagnantSegments: 2,
  rateLimitBaseMs: 2_000, rateLimitMaxMs: 60_000, providerRetryLimit: 3,
  artifactBytes: 8 * GiB, diskBytes: 32 * GiB, minFreeBytes: 512 * MiB,
  commandTimeoutMs: 10 * 60_000, jobTimeoutMs: 60 * 60_000,
  rpcTimeoutMs: 30_000, memorySoftBytes: null, memoryStopBytes: null, aggregateBudgetMiB: null,
  taskMemoryBytes: 256 * MiB, workerStartupBytes: 256 * MiB, workerMemoryBytes: null, workerEmergencyBytes: null,
  workerInitial: 1, workerMax: 8, workerMaxRuns: 0, workerHeapMiB: null,
  workerIdleKeep: 0, workerIdleRetireMs: 30_000,
});
export function fail(message, code = 'INVALID') {
  const error = new Error(message); error.code = code; return error;
}
export function text(value, name, maxBytes, { empty = false } = {}) {
  if (typeof value !== 'string' || value.length > maxBytes || (!empty && !value.trim()) || Buffer.byteLength(value) > maxBytes)
    throw fail(`${name}: expected ${empty ? '' : 'nonempty '}text <= ${maxBytes} bytes`);
  return value;
}
export function integer(value, name, min, max) {
  if (!Number.isSafeInteger(value) || value < min || value > max) throw fail(`${name}: expected integer ${min}..${max}`);
  return value;
}
export function identifier(value, name = 'id') {
  if (typeof value !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$/.test(value)) throw fail(`Invalid ${name}`);
  return value;
}
export function hash(value) { return createHash('sha256').update(value).digest('hex'); }
export function short(value, bytes = 4096) {
  const s = typeof value === 'string' ? value : String(value ?? '');
  if (s.length <= bytes && Buffer.byteLength(s) <= bytes) return s;
  return Buffer.from(s.slice(0, bytes)).subarray(0, Math.max(0, bytes - 32)).toString('utf8') + '\n[truncated]';
}
export function safeError(error) {
  return short(error?.message ?? error, 2048)
    .replace(/Bearer\s+[^\s"',;]+/gi, 'Bearer [redacted]')
    .replace(/\bsk-[a-zA-Z0-9_-]{12,}/g, '[redacted-key]')
    .replace(/((?:access_token|refresh_token|api_key)\s*[=:]\s*)[^\s&"',;]+/gi, '$1[redacted]');
}
// Bound traversal BEFORE stringify; JSON escaping can use six bytes per code unit.
export function encodeJson(value, maxBytes = LIMITS.frameBytes) {
  let budget = maxBytes, nodes = 0;
  const seen = new Set();
  function walk(v, depth) {
    if (++nodes > 100_000 || depth > 32) throw fail('JSON complexity limit', 'LIMIT');
    if (v === null || typeof v === 'boolean' || typeof v === 'number') budget -= 24;
    else if (typeof v === 'string') {
      if (v.length > budget) throw fail('JSON byte budget exceeded', 'LIMIT');
      budget -= Buffer.byteLength(v) + 2;
      for (let i = 0; i < v.length; i++) if (v.charCodeAt(i) < 32) budget -= 5; else if (v[i] === '"' || v[i] === '\\') budget--;
    } else if (typeof v === 'object') {
      if (seen.has(v) || (!Array.isArray(v) && Object.getPrototypeOf(v) !== Object.prototype && Object.getPrototypeOf(v) !== null)) throw fail('JSON must be plain, acyclic data');
      seen.add(v); budget -= 2;
      for (const [key, item] of Object.entries(v)) {
        if (item === undefined) continue;
        walk(key, depth + 1); walk(item, depth + 1); budget -= 2;
        if (budget < 0) throw fail('JSON byte budget exceeded', 'LIMIT');
      }
      seen.delete(v);
    } else if (v !== undefined) throw fail('Unsupported JSON value');
    if (budget < 0) throw fail('JSON byte budget exceeded', 'LIMIT');
  }
  walk(value, 0);
  const result = Buffer.from(JSON.stringify(value));
  if (result.length > maxBytes) throw fail('JSON byte budget exceeded', 'LIMIT');
  return result;
}
export function jsonClone(value, maxBytes = LIMITS.stateBytes) { return JSON.parse(encodeJson(value, maxBytes)); }
export class Semaphore {
  constructor(max) { integer(max, 'semaphore maximum', 1, Number.MAX_SAFE_INTEGER); this.max = max; this.active = 0; this.waiters = []; }
  async acquire(signal) {
    signal?.throwIfAborted();
    if (this.active < this.max) { this.active++; return () => this.release(); }
    if (this.waiters.length >= 128) throw fail('Execution queue full', 'BUSY');
    return new Promise((resolve, reject) => {
      const waiter = { resolve, reject, signal };
      waiter.abort = () => { this.waiters = this.waiters.filter(w => w !== waiter); reject(signal.reason); };
      this.waiters.push(waiter); signal?.addEventListener('abort', waiter.abort, { once: true });
    });
  }
  resize(max) { integer(max, 'semaphore maximum', 1, Number.MAX_SAFE_INTEGER); this.max = max; this.drain(); }
  drain() {
    while (this.active < this.max && this.waiters.length) {
      const next = this.waiters.shift();
      next.signal?.removeEventListener('abort', next.abort);
      if (next.signal?.aborted) { next.reject(next.signal.reason); continue; }
      this.active++; next.resolve(() => this.release());
    }
  }
  release() {
    if (this.active <= 0) throw fail('Semaphore released without an active reservation', 'INTERNAL');
    this.active--; this.drain();
  }
}
