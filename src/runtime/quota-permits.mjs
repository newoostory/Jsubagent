import { fail, hash } from '../limits.mjs';
import { assertProviderFault, overloadCooldownUntil } from './provider-fault.mjs';

export function initializeQuotaPermits(db) {
  const columns = new Set(db.prepare('PRAGMA table_info(quota_pools)').all().map(row => row.name));
  if (!columns.has('needs_probe')) db.exec('ALTER TABLE quota_pools ADD COLUMN needs_probe INTEGER NOT NULL DEFAULT 0');
  if (!columns.has('permit_generation')) db.exec('ALTER TABLE quota_pools ADD COLUMN permit_generation INTEGER NOT NULL DEFAULT 0');
  db.exec("CREATE TABLE IF NOT EXISTS quota_request_claims (account TEXT PRIMARY KEY, owner_nonce TEXT NOT NULL, run_id TEXT NOT NULL, task_key TEXT NOT NULL, request_id TEXT NOT NULL, pool_generation INTEGER NOT NULL, created_at INTEGER NOT NULL, outcome TEXT NOT NULL DEFAULT 'pending')");
  const claimColumns = new Set(db.prepare('PRAGMA table_info(quota_request_claims)').all().map(row => row.name));
  if (!claimColumns.has('outcome')) db.exec("ALTER TABLE quota_request_claims ADD COLUMN outcome TEXT NOT NULL DEFAULT 'pending'");
  // Keep proven unknown prior probes fenced while admitting one different recovery request.
  db.exec("CREATE TABLE IF NOT EXISTS quota_recovery_probes (account TEXT PRIMARY KEY, owner_nonce TEXT NOT NULL, run_id TEXT NOT NULL, task_key TEXT NOT NULL, request_id TEXT NOT NULL, pool_generation INTEGER NOT NULL, created_at INTEGER NOT NULL, outcome TEXT NOT NULL DEFAULT 'pending')");
}
const accountOf = task => task.profile.account ?? task.profile.provider;
function denied(task, row, reason) {
  const profile = task.profile, now = Date.now();
  let previous; if (row?.fault_json) previous = JSON.parse(row.fault_json);
  const error = fail(reason, 'QUOTA_PERMIT_HELD');
  error.requestOutcome = 'rejected_before_execution';
  error.providerFault = { ...(previous ?? { provider: profile.provider, credentialProfileId: profile.name ?? profile.provider, quotaScopeIds: [`account:${hash(accountOf(task)).slice(0, 24)}`], kind: 'quota_unknown', source: 'typed_metadata' }), requestOutcome: 'rejected_before_execution', observedAt: now, safeMessage: reason };
  return error;
}
// Caller owns BEGIN IMMEDIATE. Admission is not a permit: every provider request
// rechecks shared holds. Only an explicitly resumed real task can acquire a probe.
export function acquireQuotaPermit(db, owner, task, requestId, now = Date.now()) {
  const account = accountOf(task), pool = db.prepare('SELECT * FROM quota_pools WHERE account=?').get(account);
  const fault = pool?.fault_json ? assertProviderFault(JSON.parse(pool.fault_json)) : null;
  const expiry = pool?.state === 'held' ? overloadCooldownUntil(fault, Number(pool.updated_at), Number(pool.retry_at) || 0) : null;
  const expiredOverload = expiry !== null && expiry <= now;
  const cooledOverload = fault?.kind === 'provider_overload' && (expiredOverload || pool.state === 'backoff' && Number(pool.retry_at) <= now);
  if (pool?.state === 'held' && !expiredOverload || pool?.state === 'backoff' && Number(pool.retry_at) > now) throw denied(task, pool, 'Shared provider pool is cooling down or held');
  if (!pool || !pool.needs_probe && pool.state !== 'backoff' && !expiredOverload) return { account, requestId, probe: false };
  const existing = db.prepare('SELECT * FROM quota_request_claims WHERE account=?').get(account);
  const replacement = db.prepare('SELECT * FROM quota_recovery_probes WHERE account=?').get(account);
  const taskKey = owner.taskKey(task.id);
  const same = row => row?.owner_nonce === owner.ownerNonce && row.run_id === owner.runId && row.task_key === taskKey && row.request_id === requestId;
  if (same(existing) || same(replacement)) {
    const claim = same(existing) ? existing : replacement;
    if (claim.outcome === 'pending') return { account, requestId, probe: true };
    throw denied(task, pool, 'Uncertain request cannot reuse its recovery permit');
  }
  if (replacement || existing && !(cooledOverload && existing.outcome === 'unknown' && existing.pool_generation !== pool.permit_generation)) {
    throw denied(task, pool, 'Another real request owns the shared recovery permit; no probe or automatic retry was sent');
  }
  const table = existing ? 'quota_recovery_probes' : 'quota_request_claims';
  db.prepare(`INSERT INTO ${table}(account,owner_nonce,run_id,task_key,request_id,pool_generation,created_at) VALUES(?,?,?,?,?,?,?)`).run(account, owner.ownerNonce, owner.runId, taskKey, requestId, pool.permit_generation, now);
  return { account, requestId, probe: true };
}
export function settleQuotaPermit(db, owner, task, requestId, outcome) {
  const account = accountOf(task), taskKey = owner.taskKey(task.id);
  for (const table of ['quota_request_claims', 'quota_recovery_probes']) {
    const claim = db.prepare(`SELECT * FROM ${table} WHERE account=?`).get(account);
    if (claim?.owner_nonce !== owner.ownerNonce || claim.run_id !== owner.runId || claim.task_key !== taskKey || claim.request_id !== requestId) continue;
    if (outcome === 'unknown') {
      db.prepare(`UPDATE ${table} SET outcome='unknown' WHERE account=? AND request_id=? AND owner_nonce=?`).run(account, requestId, owner.ownerNonce);
      return; // Never expire or replay this uncertain network request.
    }
    if (outcome === 'completed') {
      const pool = db.prepare('SELECT state,updated_at,retry_at,fault_json FROM quota_pools WHERE account=? AND permit_generation=?').get(account, claim.pool_generation);
      const fault = pool?.fault_json ? assertProviderFault(JSON.parse(pool.fault_json)) : null;
      const expiredOverload = pool?.state === 'held' && (overloadCooldownUntil(fault, Number(pool.updated_at), Number(pool.retry_at) || 0) ?? Infinity) <= Date.now();
      if (pool && (pool.state !== 'held' || expiredOverload)) db.prepare("UPDATE quota_pools SET needs_probe=0,state='released' WHERE account=? AND permit_generation=? AND state=? AND updated_at=?").run(account, claim.pool_generation, pool.state, pool.updated_at);
    }
    db.prepare(`DELETE FROM ${table} WHERE account=? AND request_id=? AND owner_nonce=?`).run(account, requestId, owner.ownerNonce);
    return;
  }
}
