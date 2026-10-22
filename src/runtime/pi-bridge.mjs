import { chmod, lstat, realpath } from 'node:fs/promises';
import { join, resolve } from 'node:path';
import { atomicJson, readJson } from '../storage.mjs';
import { LIMITS, encodeJson, fail, hash, text } from '../limits.mjs';

const MUTATING = new Set([
  'team_start', 'team_advance', 'team_cancel', 'quality_supersede', 'catalog', 'set_mode', 'spawn', 'spawn_batch', 'cancel_batch',
  'mission_create', 'git_snapshot_capture', 'file_snapshot_capture', 'work_submit', 'work_accept', 'work_apply',
  'evidence_record', 'send', 'ack', 'cancel', 'resume', 'archive',
  'runtime_reload', 'quota_release',
]);
const BINDING_FILE = 'bridge-binding.json';
const LEDGER_FILE = 'bridge-requests.sqlite';
const MAX_LEDGER_BYTES = 64 * 1024 * 1024;
const RECOVERY_VERSION = 1;

export function bridgeSocketPath(root) {
  if (process.platform === 'win32' || !process.getuid) throw fail('Private local supervisor sockets require POSIX', 'UNSUPPORTED');
  // A short, per-uid 0700 directory avoids the small sockaddr_un path limit;
  // the run binding and ownership database remain in the canonical run root.
  const path = join('/tmp', `.pi-agent-runtime-${process.getuid()}`, hash(root).slice(0, 32));
  const max = process.platform === 'darwin' ? 103 : 107;
  if (Buffer.byteLength(path) > max) throw fail(`Runtime path is too long for a private local socket (${Buffer.byteLength(path)} > ${max})`, 'LIMIT');
  return path;
}

export function bridgeOwner(cwd, mainSessionId) {
  return hash(`${cwd}\0${String(mainSessionId)}`);
}

export function requestFingerprint(packet) {
  return hash(encodeJson({ op: packet.op, args: packet.args ?? {} }, LIMITS.frameBytes));
}

export function isMutatingOperation(op) { return MUTATING.has(op); }

export async function normalizeBinding(input, ownedRoot) {
  if (!input || typeof input !== 'object' || Array.isArray(input)) throw fail('Missing bridge binding', 'PERMISSION');
  const cwd = await realpath(text(input.cwd, 'cwd', 4096));
  const agentDir = await realpath(text(input.agentDir, 'agentDir', 4096));
  const requestedRoot = await realpath(text(input.root, 'root', 4096));
  if (requestedRoot !== ownedRoot) throw fail('Run root binding mismatch', 'PERMISSION');
  if (typeof input.owner !== 'string' || !/^[a-f0-9]{64}$/.test(input.owner)) throw fail('Invalid run owner binding', 'PERMISSION');
  if (input.trusted !== undefined && typeof input.trusted !== 'boolean') throw fail('Invalid trust binding', 'PERMISSION');
  const configPath = input.configPath === undefined ? undefined : resolve(cwd, text(input.configPath, 'configPath', 4096));
  return { cwd, agentDir, root: ownedRoot, owner: input.owner, trusted: input.trusted === true, ...(configPath ? { configPath } : {}) };
}

function sameBinding(left, right) {
  return ['cwd', 'agentDir', 'root', 'owner', 'trusted', 'configPath'].every(key => left?.[key] === right?.[key]);
}

export async function persistOrVerifyBinding(root, binding) {
  const path = join(root, BINDING_FILE);
  let saved;
  try { saved = await readJson(path, 32 * 1024); }
  catch (error) {
    if (error.code !== 'ENOENT') throw error;
    await atomicJson(path, binding, 32 * 1024);
    saved = binding;
  }
  if (!sameBinding(saved, binding)) throw fail('Run is bound to a different cwd, session, or owner', 'PERMISSION');
  return saved;
}

function storageFailure(action, error) {
  if (error?.code === 'STORAGE') return error;
  return fail(`Bridge request ledger ${action} failed: ${error?.message ?? String(error)}`, 'STORAGE');
}

export async function openRequestLedger(root, ownership) {
  const generation = ownership?.generation;
  if (!Number.isSafeInteger(generation) || generation < 1) throw fail('Bridge request ledger requires the current ownership generation', 'LOCKED');
  const path = join(root, LEDGER_FILE);
  try { const info = await lstat(path); if (!info.isFile() || info.isSymbolicLink()) throw fail('Unsafe bridge request ledger', 'STORAGE'); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  const { DatabaseSync } = await import('node:sqlite');
  // Reserve a bounded outcome before executing a mutation. Historical command
  // count is not an admission limit; disk exhaustion must never lose a receipt.
  const reservationBytes = LIMITS.frameBytes + 512;
  let db, lookupStatement, sizeStatement, insertStatement, dispatchStatement, recoverStatement, finishStatement;
  try {
    db = new DatabaseSync(path);
    await chmod(path, 0o600);
    db.exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=1000; CREATE TABLE IF NOT EXISTS requests (id TEXT PRIMARY KEY, fingerprint TEXT NOT NULL, operation TEXT NOT NULL, state TEXT NOT NULL CHECK(state IN ('pending','done')), outcome TEXT, accepted_at INTEGER NOT NULL, finished_at INTEGER, recovery_version INTEGER, accepted_generation INTEGER, dispatched_generation INTEGER)");
    const columns = new Set(db.prepare('PRAGMA table_info(requests)').all().map(row => row.name));
    for (const required of ['id', 'fingerprint', 'operation', 'state', 'outcome', 'accepted_at', 'finished_at']) if (!columns.has(required)) throw fail(`Bridge request ledger is missing required column: ${required}`, 'STORAGE');
    const additions = [
      ['recovery_version', 'ALTER TABLE requests ADD COLUMN recovery_version INTEGER'],
      ['accepted_generation', 'ALTER TABLE requests ADD COLUMN accepted_generation INTEGER'],
      ['dispatched_generation', 'ALTER TABLE requests ADD COLUMN dispatched_generation INTEGER'],
    ].filter(([name]) => !columns.has(name));
    if (additions.length) {
      db.exec('BEGIN IMMEDIATE');
      try { for (const [, statement] of additions) db.exec(statement); db.exec('COMMIT'); }
      catch (error) { try { db.exec('ROLLBACK'); } catch {} throw error; }
    }
    lookupStatement = db.prepare('SELECT fingerprint,operation,state,outcome,recovery_version AS recoveryVersion,accepted_generation AS acceptedGeneration,dispatched_generation AS dispatchedGeneration FROM requests WHERE id=?');
    sizeStatement = db.prepare(`SELECT COALESCE(SUM(CASE WHEN state='pending' THEN ${reservationBytes} ELSE length(CAST(outcome AS BLOB))+512 END),0) AS bytes FROM requests`);
    insertStatement = db.prepare("INSERT INTO requests(id,fingerprint,operation,state,outcome,accepted_at,finished_at,recovery_version,accepted_generation,dispatched_generation) VALUES(?,?,?,'pending',NULL,?,NULL,?,?,NULL)");
    dispatchStatement = db.prepare("UPDATE requests SET dispatched_generation=? WHERE id=? AND fingerprint=? AND operation=? AND state='pending' AND recovery_version=? AND accepted_generation=? AND dispatched_generation IS NULL");
    recoverStatement = db.prepare("UPDATE requests SET dispatched_generation=? WHERE id=? AND fingerprint=? AND operation=? AND state='pending' AND recovery_version=? AND accepted_generation<? AND dispatched_generation IS NULL");
    finishStatement = db.prepare("UPDATE requests SET state='done',outcome=?,finished_at=? WHERE id=? AND fingerprint=? AND operation=? AND state='pending' AND recovery_version=? AND dispatched_generation IS NOT NULL");
  } catch (error) {
    try { db?.close(); } catch {}
    throw storageFailure('open', error);
  }
  let closed = false;
  const opened = () => { if (closed) throw fail('Bridge request ledger closed', 'CLOSED'); };
  const mutation = (action, work) => {
    opened();
    try { return work(); } catch (error) { throw storageFailure(action, error); }
  };
  const mark = (statement, action, id, fingerprint, operation, tail) => mutation(action, () => {
    if (statement.run(generation, id, fingerprint, operation, RECOVERY_VERSION, tail).changes !== 1) throw fail('Bridge request acceptance changed before dispatch', 'STORAGE');
  });
  return {
    lookup(id) {
      opened();
      try {
        const row = lookupStatement.get(id);
        if (!row) return undefined;
        if (typeof row.fingerprint !== 'string' || !row.fingerprint || typeof row.operation !== 'string' || !row.operation || !['pending', 'done'].includes(row.state)) throw fail('Bridge request ledger row is malformed', 'STORAGE');
        if (row.state === 'pending') {
          if (row.outcome !== null) throw fail('Pending bridge request has an invalid saved outcome', 'STORAGE');
          return { ...row, outcome: undefined };
        }
        if (typeof row.outcome !== 'string') throw fail('Completed bridge request is missing its durable outcome', 'STORAGE');
        const outcome = JSON.parse(row.outcome);
        if (!outcome || typeof outcome !== 'object' || Array.isArray(outcome) || typeof outcome.ok !== 'boolean') throw fail('Completed bridge request outcome is malformed', 'STORAGE');
        return { ...row, outcome };
      } catch (error) { throw storageFailure('lookup', error); }
    },
    accept(id, fingerprint, operation) {
      mutation('acceptance', () => {
        const size = sizeStatement.get();
        if (size.bytes + reservationBytes > MAX_LEDGER_BYTES) throw fail('Bridge mutation ledger disk budget exhausted; preserved records require explicit retention management', 'STORAGE');
        insertStatement.run(id, fingerprint, operation, Date.now(), RECOVERY_VERSION, generation);
      });
    },
    canRecover(row) {
      opened();
      return row?.state === 'pending' && row.recoveryVersion === RECOVERY_VERSION && Number.isSafeInteger(row.acceptedGeneration) && row.acceptedGeneration > 0 && row.acceptedGeneration < generation && row.dispatchedGeneration === null;
    },
    beginDispatch(id, fingerprint, operation) { mark(dispatchStatement, 'dispatch marker', id, fingerprint, operation, generation); },
    recoverDispatch(id, fingerprint, operation) { mark(recoverStatement, 'recovery dispatch marker', id, fingerprint, operation, generation); },
    finish(id, fingerprint, operation, outcome) {
      const encoded = encodeJson(outcome, LIMITS.frameBytes).toString('utf8');
      mutation('completion', () => {
        if (finishStatement.run(encoded, Date.now(), id, fingerprint, operation, RECOVERY_VERSION).changes !== 1) throw fail('Bridge request acceptance changed before completion', 'STORAGE');
      });
    },
    close() {
      if (closed) return;
      closed = true;
      try { db.close(); } catch (error) { throw storageFailure('close', error); }
    },
  };
}
