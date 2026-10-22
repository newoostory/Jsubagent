import { createHash } from 'node:crypto';
import { closeSync, fstatSync, openSync, readSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

export const RUNTIME_PROVENANCE = 1;
// Diagnostic identity only. Existing frame acceptance remains the compatibility policy.
export const HOST_PROTOCOL = 1;
const MAX_SOURCE_BYTES = 2 * 1024 * 1024;
const STARTUP_DIAGNOSTIC_BYTES = 8192;

function sourcePath(source) {
  if (source instanceof URL) return fileURLToPath(source);
  if (typeof source === 'string' && source.startsWith('file:')) return fileURLToPath(source);
  return String(source);
}

function unavailable(path, component, error, observedAt) {
  return { component, path, observedAt, unavailable: true, code: String(error?.code ?? 'SOURCE_UNAVAILABLE').slice(0, 64) };
}

/** A bounded observation of bytes on disk at one instant. It is never labelled loaded. */
export function observeSource(source, component = 'source') {
  const path = sourcePath(source), observedAt = Date.now();
  let fd;
  try {
    fd = openSync(path, 'r');
    const before = fstatSync(fd);
    if (!before.isFile()) return unavailable(path, component, { code: 'NOT_FILE' }, observedAt);
    if (!Number.isSafeInteger(before.size) || before.size < 0 || before.size > MAX_SOURCE_BYTES) return unavailable(path, component, { code: 'SOURCE_SIZE_LIMIT' }, observedAt);
    const bytes = Buffer.alloc(before.size); let offset = 0;
    while (offset < bytes.length) {
      const read = readSync(fd, bytes, offset, bytes.length - offset, offset);
      if (!read) break;
      offset += read;
    }
    const after = fstatSync(fd);
    if (offset !== before.size || after.size !== before.size || after.mtimeMs !== before.mtimeMs || after.ino !== before.ino) return unavailable(path, component, { code: 'SOURCE_CHANGED_DURING_READ' }, observedAt);
    return { component, path, sha256: createHash('sha256').update(bytes).digest('hex'), bytes: bytes.length, observedAt };
  } catch (error) { return unavailable(path, component, error, observedAt); }
  finally { if (fd !== undefined) try { closeSync(fd); } catch {} }
}

/** Capture once during module evaluation; callers must retain this object for process lifetime. */
export function captureLoadedSource(source, component) {
  const observation = observeSource(source, component);
  const { observedAt, ...identity } = observation;
  return Object.freeze({ ...identity, capturedAt: observedAt });
}

export function sourceState(loaded, onDisk) {
  if (!loaded?.sha256 || !onDisk?.sha256) return 'unavailable';
  return loaded.sha256 === onDisk.sha256 ? 'match' : 'drift';
}

export function componentProvenance(loaded, source = loaded?.path) {
  const onDisk = source ? observeSource(source, loaded?.component ?? 'source') : null;
  return { loaded: loaded ?? null, onDisk, state: sourceState(loaded, onDisk) };
}

function sourceLabel(value) {
  if (value?.sha256) return `${value.component ?? 'source'} sha256:${value.sha256}`;
  return `${value?.component ?? 'source'} unknown`;
}

export function protocolMismatchDiagnostic({ expected, received, workerLoaded, supervisorLoaded, sdkVersion }) {
  return `worker protocol expected ${expected}, received ${received ?? 'unknown'}; ${sourceLabel(workerLoaded)}; ${sourceLabel(supervisorLoaded)}; Pi SDK ${sdkVersion ?? 'unknown'}`;
}

/**
 * Add the client's current disk observation without relabelling it as remote loaded code.
 * A host without provenance is explicitly legacy/unknown.
 */
export function clientRuntimeView(remote, supervisorSource) {
  const observedSupervisorOnDisk = observeSource(supervisorSource, 'supervisor');
  const known = remote?.provenance === RUNTIME_PROVENANCE && remote?.supervisor?.loaded && typeof remote.supervisor.loaded === 'object';
  const hostProtocol = Number.isSafeInteger(remote?.hostProtocol) ? (remote.hostProtocol === HOST_PROTOCOL ? 'match' : 'mismatch') : 'unknown_legacy';
  const supervisor = known ? sourceState(remote.supervisor.loaded, observedSupervisorOnDisk) : 'unknown_legacy';
  const client = { hostProtocol: HOST_PROTOCOL, observedSupervisorOnDisk };
  if (!known) return {
    provenance: RUNTIME_PROVENANCE,
    legacy: true,
    hostProtocol: null,
    supervisor: { loaded: null, onDisk: null, state: 'unknown_legacy' },
    worker: { protocol: null, processes: [], state: 'unknown_legacy' },
    client,
    comparison: { hostProtocol, supervisor },
    diagnostic: 'Connected supervisor does not expose runtime provenance; its loaded supervisor, worker protocol, and SDK version are unknown/legacy. The client disk observation is not a loaded-build claim.',
  };
  const issues = [];
  if (hostProtocol === 'mismatch') issues.push(`host protocol client=${HOST_PROTOCOL} supervisor=${remote.hostProtocol}`);
  if (supervisor === 'drift') issues.push(`supervisor loaded ${remote.supervisor.loaded.sha256} but disk has ${observedSupervisorOnDisk.sha256}`);
  if (supervisor === 'unavailable') issues.push('supervisor source comparison unavailable');
  return { ...remote, client, comparison: { hostProtocol, supervisor }, diagnostic: issues.length ? `Runtime mismatch: ${issues.join('; ')}. No automatic restart or work replay was attempted.` : null };
}

function boundedAppend(existing, chunk, limit, keepTail) {
  if (!chunk.length) return existing;
  const joined = existing.length ? Buffer.concat([existing, chunk]) : Buffer.from(chunk);
  if (joined.length <= limit) return joined;
  return keepTail ? joined.subarray(joined.length - limit) : joined.subarray(0, limit);
}

function sanitizeStartupText(value) {
  return String(value)
    .replace(/\x1b\[[0-9;]*[A-Za-z]/g, '')
    .replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, '')
    .replace(/Bearer\s+[^\s"',;]+/gi, 'Bearer [redacted]')
    .replace(/\bsk-[a-zA-Z0-9_-]{12,}/g, '[redacted-key]')
    .replace(/((?:access[_-]?token|refresh[_-]?token|api[_-]?key|password|secret)\s*[=:]\s*)[^\s&"',;]+/gi, '$1[redacted]');
}

/** Retains only bounded head/tail bytes; rendering is redacted and capped again. */
export class StartupDiagnosticBuffer {
  constructor(limit = STARTUP_DIAGNOSTIC_BYTES) {
    this.limit = Math.max(1024, Math.min(64 * 1024, limit));
    this.half = Math.floor(this.limit / 2); this.head = Buffer.alloc(0); this.tail = Buffer.alloc(0); this.bytes = 0;
  }
  append(value) {
    const chunk = Buffer.isBuffer(value) ? value : Buffer.from(value);
    const before = this.bytes; this.bytes += chunk.length; let remainder = chunk;
    if (before < this.half) {
      const count = Math.min(this.half - before, chunk.length);
      this.head = boundedAppend(this.head, chunk.subarray(0, count), this.half, false);
      remainder = chunk.subarray(count);
    }
    if (remainder.length) this.tail = boundedAppend(this.tail, remainder, this.limit - this.half, true);
  }
  text(maxBytes = 2048) {
    const omittedBytes = Math.max(0, this.bytes - this.head.length - this.tail.length);
    const omitted = omittedBytes ? `\n[${omittedBytes} startup stderr bytes omitted]\n` : '';
    const combined = this.bytes <= this.head.length ? this.head.toString('utf8') : `${this.head.toString('utf8')}${omitted}${this.tail.toString('utf8')}`;
    const clean = sanitizeStartupText(combined).trim();
    const bytes = Buffer.from(clean);
    if (bytes.length <= maxBytes) return clean;
    const marker = Buffer.from('\n[rendered startup stderr truncated]\n');
    const side = Math.max(0, Math.floor((maxBytes - marker.length) / 2));
    return Buffer.concat([bytes.subarray(0, side), marker, bytes.subarray(bytes.length - side)]).toString('utf8');
  }
}
