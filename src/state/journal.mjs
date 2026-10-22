import { randomUUID } from 'node:crypto';
import { constants } from 'node:fs';
import { chmod, lstat, mkdir, open, rename, stat, unlink } from 'node:fs/promises';
import { dirname } from 'node:path';
import { encodeJson, fail, short } from '../limits.mjs';

function journalError(message, code = 'JOURNAL') {
  return fail(message, code);
}

function validIdentifier(value) {
  return typeof value === 'string' && /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$/.test(value);
}

function identifier(value, name) {
  if (!validIdentifier(value)) throw journalError(`Invalid ${name}`, 'INVALID');
  return value;
}

function positiveInteger(value, name, maximum = Number.MAX_SAFE_INTEGER) {
  if (!Number.isSafeInteger(value) || value < 1 || value > maximum) throw journalError(`Invalid ${name}`, 'INVALID');
  return value;
}

function validateScope(scope) {
  if (!scope || typeof scope !== 'object') throw journalError('Journal owner scope is required', 'OWNER_SCOPE');
  return { ownerId: identifier(scope.ownerId, 'ownerId'), runId: identifier(scope.runId, 'runId') };
}

async function syncDirectory(path) {
  const directory = await open(dirname(path), constants.O_RDONLY);
  try {
    await directory.sync();
  } catch (error) {
    if (!['EINVAL', 'ENOTSUP', 'EBADF'].includes(error.code)) throw error;
  } finally {
    await directory.close();
  }
}

function historyPush(history, value, maximum) {
  if (history.length === maximum) history.shift();
  history.push(value);
}

function scanEntry(state, line, lineOffset, fileBytes, limits) {
  if (line.length > limits.maxEntryBytes - 1) {
    state.tornTail = { offset: lineOffset, bytes: fileBytes - lineOffset, reason: 'oversized journal record' };
    return false;
  }
  let entry;
  try {
    entry = JSON.parse(line.toString('utf8'));
  } catch {
    state.tornTail = { offset: lineOffset, bytes: fileBytes - lineOffset, reason: 'invalid journal record' };
    return false;
  }
  if (!entry || typeof entry !== 'object' || !Number.isSafeInteger(entry.seq) || entry.seq !== state.lastSequence + 1) {
    state.tornTail = { offset: lineOffset, bytes: fileBytes - lineOffset, reason: 'non-contiguous journal sequence' };
    return false;
  }
  const validEnvelope = ['begin', 'complete', 'fail'].includes(entry.phase)
    && validIdentifier(entry.id) && validIdentifier(entry.intentId) && validIdentifier(entry.kind)
    && validIdentifier(entry.ownerId) && validIdentifier(entry.runId)
    && Number.isSafeInteger(entry.at) && entry.at >= 0
    && !state.entryIds.has(entry.id)
    && (entry.phase !== 'begin' || (entry.id === entry.intentId && !state.intentIds.has(entry.intentId)))
    && (entry.phase === 'begin' || state.pending.has(entry.intentId));
  if (!validEnvelope) {
    state.tornTail = { offset: lineOffset, bytes: fileBytes - lineOffset, reason: 'invalid journal envelope' };
    return false;
  }
  state.records++;
  if (state.records > limits.maxRecords) throw journalError('Journal record count budget exceeded', 'JOURNAL_BUDGET');
  state.entryIds.add(entry.id);
  state.lastSequence = entry.seq;
  if (entry.phase === 'begin') {
    if (state.pending.size >= limits.maxPending) throw journalError('Pending journal intent budget exceeded', 'JOURNAL_BUDGET');
    state.intentIds.add(entry.intentId);
    state.pending.set(entry.intentId, entry);
  } else {
    const begin = state.pending.get(entry.intentId);
    state.pending.delete(entry.intentId);
    const pair = { begin, terminal: entry };
    historyPush(entry.phase === 'complete' ? state.completed : state.failed, pair, limits.historyLimit);
  }
  return true;
}

async function scanJournal(path, fileBytes, limits) {
  const state = {
    pending: new Map(), completed: [], failed: [], entryIds: new Set(), intentIds: new Set(),
    records: 0, lastSequence: 0, validBytes: 0, tornTail: null,
  };
  if (!fileBytes) return state;
  const file = await open(path, constants.O_RDONLY | (constants.O_NOFOLLOW ?? 0));
  const readBuffer = Buffer.allocUnsafe(64 * 1024);
  let readOffset = 0;
  let carry = Buffer.alloc(0);
  let carryOffset = 0;
  try {
    while (readOffset < fileBytes && !state.tornTail) {
      const result = await file.read(readBuffer, 0, Math.min(readBuffer.length, fileBytes - readOffset), readOffset);
      if (!result.bytesRead) break;
      readOffset += result.bytesRead;
      const chunk = readBuffer.subarray(0, result.bytesRead);
      const data = carry.length ? Buffer.concat([carry, chunk]) : chunk;
      const baseOffset = carryOffset;
      let cursor = 0;
      while (!state.tornTail) {
        const newline = data.indexOf(0x0a, cursor);
        if (newline < 0) break;
        const lineOffset = baseOffset + cursor;
        const line = data.subarray(cursor, newline);
        cursor = newline + 1;
        state.validBytes = baseOffset + cursor;
        if (line.length && !scanEntry(state, line, lineOffset, fileBytes, limits)) break;
      }
      if (state.tornTail) break;
      carryOffset = baseOffset + cursor;
      carry = Buffer.from(data.subarray(cursor));
      if (carry.length >= limits.maxEntryBytes) {
        state.tornTail = { offset: carryOffset, bytes: fileBytes - carryOffset, reason: 'oversized or unterminated journal record' };
      }
    }
    if (!state.tornTail && carry.length) {
      state.tornTail = { offset: carryOffset, bytes: fileBytes - carryOffset, reason: 'unterminated journal record' };
    }
    if (!state.tornTail && readOffset !== fileBytes) {
      state.tornTail = { offset: state.validBytes, bytes: fileBytes - state.validBytes, reason: 'journal changed during scan' };
    }
  } finally {
    await file.close();
  }
  state.entryIds = undefined;
  state.intentIds = undefined;
  return state;
}

/**
 * Fsync-on-every-record append journal for cross-store intents. Startup scans
 * in fixed-size chunks and retains only pending intents plus bounded terminal
 * history. Atomic compaction rewrites only unresolved intents.
 */
export class DurableJournal {
  static async open(path, options = {}) {
    if (typeof path !== 'string' || !path) throw journalError('Journal path is required', 'INVALID');
    await mkdir(dirname(path), { recursive: true, mode: 0o700 });
    await chmod(dirname(path), 0o700);
    try {
      const existing = await lstat(path);
      if (!existing.isFile() || existing.isSymbolicLink()) throw journalError('Journal must be a regular non-symlink file', 'STORAGE');
    } catch (error) {
      if (error.code !== 'ENOENT') throw error;
    }
    const maxFileBytes = positiveInteger(options.maxFileBytes ?? 8 * 1024 * 1024, 'maxFileBytes');
    const maxEntryBytes = positiveInteger(options.maxEntryBytes ?? Math.min(32 * 1024, maxFileBytes), 'maxEntryBytes', maxFileBytes);
    const maxRecords = positiveInteger(options.maxRecords ?? 50_000, 'maxRecords', 1_000_000);
    const maxPending = positiveInteger(options.maxPending ?? 10_000, 'maxPending', maxRecords);
    const historyLimit = positiveInteger(options.historyLimit ?? 1_000, 'historyLimit', maxRecords);
    const maxRecoveryPage = positiveInteger(options.maxRecoveryPage ?? 1_000, 'maxRecoveryPage', maxPending);
    const info = await stat(path).catch(error => error.code === 'ENOENT' ? { size: 0 } : Promise.reject(error));
    if (info.size > maxFileBytes) throw journalError('Journal exceeds its configured byte budget', 'JOURNAL_BUDGET');
    const limits = { maxFileBytes, maxEntryBytes, maxRecords, maxPending, historyLimit, maxRecoveryPage };
    const scan = await scanJournal(path, info.size, limits);
    const file = await open(path, constants.O_CREAT | constants.O_APPEND | constants.O_RDWR | (constants.O_NOFOLLOW ?? 0), 0o600);
    try {
      await file.chmod(0o600);
      await syncDirectory(path);
      return new DurableJournal(path, file, { ...limits, ...scan, bytes: info.size });
    } catch (error) {
      await file.close().catch(() => {});
      throw error;
    }
  }

  constructor(path, file, state) {
    this.path = path;
    this.file = file;
    this.maxFileBytes = state.maxFileBytes;
    this.maxEntryBytes = state.maxEntryBytes;
    this.maxRecords = state.maxRecords;
    this.maxPending = state.maxPending;
    this.historyLimit = state.historyLimit;
    this.maxRecoveryPage = state.maxRecoveryPage;
    this.pending = state.pending;
    this.completed = state.completed;
    this.failed = state.failed;
    this.records = state.records;
    this.lastSequence = state.lastSequence;
    this.validBytes = state.validBytes;
    this.fileBytes = state.bytes;
    this.tornTail = state.tornTail;
    this.closed = false;
    this.faulted = false;
    this.queue = Promise.resolve();
  }

  _assertWritable() {
    if (this.closed) throw journalError('Journal is closed', 'STORAGE');
    if (this.tornTail || this.faulted) throw journalError('Journal recovery is required before appending', 'JOURNAL_RECOVERY_REQUIRED');
  }

  _enqueue(operation) {
    const result = this.queue.then(operation, operation);
    this.queue = result.catch(() => {});
    return result;
  }

  _encode(envelope) {
    const entry = { seq: this.lastSequence + 1, ...envelope, at: envelope.at ?? Date.now() };
    const body = encodeJson(entry, this.maxEntryBytes - 1);
    return { entry, line: Buffer.concat([body, Buffer.from('\n')]) };
  }

  _apply(entry) {
    this.records++;
    this.lastSequence = entry.seq;
    if (entry.phase === 'begin') this.pending.set(entry.intentId, entry);
    else {
      const begin = this.pending.get(entry.intentId);
      this.pending.delete(entry.intentId);
      historyPush(entry.phase === 'complete' ? this.completed : this.failed, { begin, terminal: entry }, this.historyLimit);
    }
  }

  async _append(envelope) {
    this._assertWritable();
    let encoded = this._encode(envelope);
    if (this.fileBytes + encoded.line.length > this.maxFileBytes || this.records >= this.maxRecords) {
      await this._compactInternal();
      encoded = this._encode(envelope);
    }
    if (this.fileBytes + encoded.line.length > this.maxFileBytes || this.records >= this.maxRecords) {
      throw journalError('Journal byte or record budget exceeded by unresolved intents', 'JOURNAL_BUDGET');
    }
    try {
      let offset = 0;
      while (offset < encoded.line.length) {
        const { bytesWritten } = await this.file.write(encoded.line, offset, encoded.line.length - offset);
        if (!bytesWritten) throw journalError('Zero-byte journal write', 'STORAGE');
        offset += bytesWritten;
      }
      await this.file.sync();
    } catch (error) {
      this.faulted = true;
      throw error;
    }
    this.fileBytes += encoded.line.length;
    this.validBytes = this.fileBytes;
    this._apply(encoded.entry);
    return encoded.entry;
  }

  begin(kindValue, scopeValue, payload = {}) {
    const kind = identifier(kindValue, 'intent kind');
    const scope = validateScope(scopeValue);
    const intentId = randomUUID();
    return this._enqueue(() => {
      if (this.pending.size >= this.maxPending) throw journalError('Pending journal intent budget exceeded', 'JOURNAL_BUDGET');
      return this._append({ id: intentId, intentId, phase: 'begin', kind, ownerId: scope.ownerId, runId: scope.runId, payload });
    });
  }

  _intent(intentIdValue) {
    const intentId = identifier(intentIdValue, 'intentId');
    const begin = this.pending.get(intentId);
    if (!begin) throw journalError(`Unknown or completed journal intent: ${intentId}`, 'NOT_FOUND');
    return begin;
  }

  complete(intentIdValue, result = {}) {
    return this._enqueue(() => {
      this._assertWritable();
      const begin = this._intent(intentIdValue);
      return this._append({ id: randomUUID(), intentId: begin.intentId, phase: 'complete', kind: begin.kind, ownerId: begin.ownerId, runId: begin.runId, result });
    });
  }

  fail(intentIdValue, error) {
    return this._enqueue(() => {
      this._assertWritable();
      const begin = this._intent(intentIdValue);
      return this._append({ id: randomUUID(), intentId: begin.intentId, phase: 'fail', kind: begin.kind, ownerId: begin.ownerId, runId: begin.runId, error: short(error?.message ?? error ?? 'intent failed', 2048) });
    });
  }

  recover(scopeValue, { after = 0, limit = this.maxRecoveryPage } = {}) {
    if (this.closed) throw journalError('Journal is closed', 'STORAGE');
    const scope = scopeValue === undefined ? null : validateScope(scopeValue);
    if (!Number.isSafeInteger(after) || after < 0) throw journalError('Invalid recovery cursor', 'INVALID');
    positiveInteger(limit, 'recovery page limit', this.maxRecoveryPage);
    const matches = value => !scope || (value.ownerId === scope.ownerId && value.runId === scope.runId);
    const candidates = [...this.pending.values()].filter(entry => entry.seq > after && matches(entry)).sort((a, b) => a.seq - b.seq);
    const hasMore = candidates.length > limit;
    const pending = candidates.slice(0, limit).map(entry => ({ ...entry }));
    return {
      pending,
      nextCursor: hasMore ? pending.at(-1).seq : null,
      completed: this.completed.filter(pair => matches(pair.begin)).map(pair => ({ begin: { ...pair.begin }, terminal: { ...pair.terminal } })),
      failed: this.failed.filter(pair => matches(pair.begin)).map(pair => ({ begin: { ...pair.begin }, terminal: { ...pair.terminal } })),
      tornTail: this.tornTail ? { ...this.tornTail } : null,
      validBytes: this.validBytes,
      fileBytes: this.fileBytes,
    };
  }

  async _compactInternal() {
    this._assertWritable();
    const tempPath = `${this.path}.compact-${randomUUID()}`;
    let temp;
    let replaced = false;
    let nextBytes = 0;
    let nextSequence = 0;
    const nextPending = new Map();
    try {
      temp = await open(tempPath, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY, 0o600);
      for (const begin of [...this.pending.values()].sort((a, b) => a.seq - b.seq)) {
        const entry = { ...begin, seq: ++nextSequence };
        const body = encodeJson(entry, this.maxEntryBytes - 1);
        const line = Buffer.concat([body, Buffer.from('\n')]);
        if (nextBytes + line.length > this.maxFileBytes) throw journalError('Unresolved intents exceed journal byte budget', 'JOURNAL_BUDGET');
        let offset = 0;
        while (offset < line.length) {
          const { bytesWritten } = await temp.write(line, offset, line.length - offset, nextBytes + offset);
          if (!bytesWritten) throw journalError('Zero-byte compacted journal write', 'STORAGE');
          offset += bytesWritten;
        }
        nextBytes += line.length;
        nextPending.set(entry.intentId, entry);
      }
      await temp.sync();
      await temp.close();
      temp = null;
      await this.file.close();
      this.file = null;
      await rename(tempPath, this.path);
      replaced = true;
      await syncDirectory(this.path);
      this.file = await open(this.path, constants.O_APPEND | constants.O_RDWR | (constants.O_NOFOLLOW ?? 0));
      this.pending = nextPending;
      this.completed = [];
      this.failed = [];
      this.records = nextPending.size;
      this.lastSequence = nextSequence;
      this.fileBytes = nextBytes;
      this.validBytes = nextBytes;
      return { pending: nextPending.size, fileBytes: nextBytes };
    } catch (error) {
      await temp?.close().catch(() => {});
      await unlink(tempPath).catch(cleanupError => { if (cleanupError.code !== 'ENOENT') error.cleanupError = cleanupError; });
      if (replaced) this.faulted = true;
      if (!this.file) {
        try { this.file = await open(this.path, constants.O_APPEND | constants.O_RDWR | (constants.O_NOFOLLOW ?? 0)); }
        catch (reopenError) { this.faulted = true; error.reopenError = reopenError; }
      }
      throw error;
    }
  }

  compact() {
    return this._enqueue(() => this._compactInternal());
  }

  /** Preserve an invalid tail before truncating only the validated prefix. */
  repairTornTail() {
    return this._enqueue(async () => {
      if (this.closed) throw journalError('Journal is closed', 'STORAGE');
      if (!this.tornTail) return null;
      const recoveryPath = `${this.path}.orphan-${randomUUID()}`;
      const recovery = await open(recoveryPath, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY, 0o600);
      const buffer = Buffer.allocUnsafe(64 * 1024);
      let sourceOffset = this.validBytes;
      let targetOffset = 0;
      try {
        while (sourceOffset < this.fileBytes) {
          const { bytesRead } = await this.file.read(buffer, 0, Math.min(buffer.length, this.fileBytes - sourceOffset), sourceOffset);
          if (!bytesRead) throw journalError('Journal tail changed during preservation', 'STORAGE');
          let written = 0;
          while (written < bytesRead) {
            const result = await recovery.write(buffer, written, bytesRead - written, targetOffset + written);
            if (!result.bytesWritten) throw journalError('Zero-byte recovery write', 'STORAGE');
            written += result.bytesWritten;
          }
          sourceOffset += bytesRead;
          targetOffset += bytesRead;
        }
        await recovery.sync();
      } finally {
        await recovery.close();
      }
      await syncDirectory(recoveryPath);
      await this.file.truncate(this.validBytes);
      await this.file.sync();
      await syncDirectory(this.path);
      this.fileBytes = this.validBytes;
      this.tornTail = null;
      this.faulted = false;
      return { recoveryPath, preservedBytes: targetOffset };
    });
  }

  async close() {
    if (this.closed) return;
    await this.queue;
    await this.file?.close();
    this.closed = true;
  }
}

export async function openJournal(path, options) {
  return DurableJournal.open(path, options);
}
