import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';
import { join } from 'node:path';
import { lstat, chmod } from 'node:fs/promises';
import { fail } from '../limits.mjs';
const exec = promisify(execFile);

export async function processIdentity(pid) {
  if (!Number.isSafeInteger(pid) || pid <= 0) throw fail('Invalid owner pid', 'LOCKED');
  try { process.kill(pid, 0); } catch (error) { if (error.code === 'ESRCH') return null; throw error; }
  if (process.platform === 'win32') throw fail('Process-start ownership verification requires POSIX ps', 'UNSUPPORTED');
  try {
    const { stdout } = await exec('ps', ['-p', String(pid), '-o', 'lstart='], { timeout: 2000, maxBuffer: 4096, env: { ...process.env, LC_ALL: 'C' } });
    if (!stdout.trim()) throw fail('Cannot identify live owner process start', 'LOCKED');
    return stdout.trim();
  } catch (error) {
    try { process.kill(pid, 0); } catch (probe) { if (probe.code === 'ESRCH') return null; }
    throw error;
  }
}

export async function executorStopped(executor) {
  if (!executor?.started) return false;
  for (const identity of [executor, ...(executor.guardians ?? [])]) if (await processIdentity(identity.pid) === identity.started) return false;
  return true;
}

// Workers stop guardians through their owned IPC lifetime. For a detached or
// recovered executor we can observe death, but POSIX PID/start checks cannot
// atomically authorize a signal. Do not introduce a reused-PID kill race.
export async function waitOwnedExecutorStopped(identity, { timeoutMs = 2000 } = {}) {
  if (!identity?.started || !Number.isSafeInteger(identity.pid)) throw fail('Executor shutdown identity unavailable', 'EXECUTOR_UNCERTAIN');
  const deadline = Date.now() + timeoutMs;
  while (await processIdentity(identity.pid) === identity.started) {
    if (Date.now() >= deadline) throw fail('Recorded executor remains alive; shutdown cannot confirm cleanup', 'EXECUTOR_UNCERTAIN');
    await new Promise(resolve => setTimeout(resolve, 25));
  }
}

// SQLite serializes takeover; OS crash recovery replaces the orphanable gate file.
// A lease timeout is not proof of death and is never used to authorize takeover.
export async function acquireOwnership(root) {
  const path = join(root, 'ownership.sqlite');
  try { const info = await lstat(path); if (!info.isFile() || info.isSymbolicLink()) throw fail('Unsafe ownership store', 'LOCKED'); } catch (error) { if (error.code !== 'ENOENT') throw error; }
  const { DatabaseSync } = await import('node:sqlite');
  const db = new DatabaseSync(path); await chmod(path, 0o600);
  let held = false;
  try {
    db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=0; CREATE TABLE IF NOT EXISTS ownership (id INTEGER PRIMARY KEY CHECK(id=1), generation INTEGER NOT NULL, pid INTEGER, started TEXT, nonce TEXT)');
    const started = await processIdentity(process.pid), nonce = randomUUID();
    db.exec('BEGIN IMMEDIATE'); held = true;
    const old = db.prepare('SELECT * FROM ownership WHERE id=1').get();
    if (old?.pid && await processIdentity(old.pid) === old.started) throw fail(`Run already owned by PID ${old.pid}`, 'LOCKED');
    const generation = (old?.generation ?? 0) + 1;
    db.prepare('INSERT INTO ownership VALUES(1,?,?,?,?) ON CONFLICT(id) DO UPDATE SET generation=excluded.generation,pid=excluded.pid,started=excluded.started,nonce=excluded.nonce').run(generation, process.pid, started, nonce);
    db.exec('COMMIT'); held = false;
    let released = false;
    const release = async () => {
      if (released) return;
      db.exec('BEGIN IMMEDIATE');
      try { db.prepare('UPDATE ownership SET pid=NULL,started=NULL,nonce=NULL WHERE id=1 AND nonce=? AND generation=?').run(nonce, generation); db.exec('COMMIT'); released = true; }
      catch (error) { db.exec('ROLLBACK'); throw error; }
      finally { if (released) db.close(); }
    };
    release.identity = { pid: process.pid, started, nonce, generation };
    return release;
  } catch (error) {
    if (held) try { db.exec('ROLLBACK'); } catch {}
    db.close();
    if (/locked|busy/i.test(error.message)) throw fail('Ownership transaction busy; no takeover performed', 'LOCKED');
    throw error;
  }
}
