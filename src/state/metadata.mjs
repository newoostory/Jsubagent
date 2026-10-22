import { lstat, mkdir } from 'node:fs/promises';
import { dirname } from 'node:path';
import { encodeJson, fail } from '../limits.mjs';

export const METADATA_TABLES = Object.freeze([
  'runs',
  'missions',
  'team_runs',
  'evidence',
  'work_items',
  'task_history',
  'attempts',
  'steps',
  'artifacts',
  'dependencies',
  'messages',
  'response_history',
  'usage_reservations',
  'worker_leases',
  'notification_outbox',
  'result_heads',
  'result_revisions',
  'result_entries',
  'result_events',
  'result_bindings',
  'result_slots',
  'result_routes',
  'result_deliveries',
  'result_evidence',
  'result_subscriptions',
  'result_snapshots',
  'result_snapshot_entries',
]);

const TABLE_SET = new Set(METADATA_TABLES);
const DEFAULT_STALE_STATES = Object.freeze({
  attempts: ['running'],
  steps: ['running'],
  artifacts: ['staging', 'sealing'],
  usage_reservations: ['reserved'],
  worker_leases: ['active'],
  notification_outbox: ['delivering'],
});

function stateError(message, code = 'INVALID') {
  return fail(message, code);
}

function identifier(value, name) {
  if (typeof value !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$/.test(value)) {
    throw stateError(`Invalid ${name}`);
  }
  return value;
}

function tableName(value) {
  if (!TABLE_SET.has(value)) throw stateError('Unknown metadata table');
  return value;
}

function positiveInteger(value, name, maximum = Number.MAX_SAFE_INTEGER) {
  if (!Number.isSafeInteger(value) || value < 1 || value > maximum) {
    throw stateError(`${name} must be an integer between 1 and ${maximum}`);
  }
  return value;
}

function timestamp(value, name) {
  if (!Number.isSafeInteger(value) || value < 0) throw stateError(`Invalid ${name}`);
  return value;
}

function scopeFor(table, scope) {
  if (!scope || typeof scope !== 'object') throw stateError('Metadata scope is required', 'OWNER_SCOPE');
  const ownerId = identifier(scope.ownerId, 'ownerId');
  if (table === 'runs') {
    if (scope.runId !== undefined) identifier(scope.runId, 'runId');
    return { ownerId, runId: scope.runId };
  }
  return { ownerId, runId: identifier(scope.runId, 'runId') };
}

function checkRecordScope(table, scope, record) {
  if (!record || typeof record !== 'object' || Array.isArray(record)) throw stateError('Metadata record must be an object');
  const id = identifier(record.id, 'record id');
  if (record.ownerId !== undefined && record.ownerId !== scope.ownerId) throw stateError('Record owner does not match scope', 'OWNER_SCOPE');
  const runId = table === 'runs' ? id : scope.runId;
  if (table === 'runs' && scope.runId !== undefined && scope.runId !== id) throw stateError('Run id does not match scope', 'OWNER_SCOPE');
  if (record.runId !== undefined && record.runId !== runId) throw stateError('Record run does not match scope', 'OWNER_SCOPE');
  return { id, runId };
}

function parseRow(row) {
  if (!row) return null;
  return JSON.parse(row.data_json);
}

function sqliteSupportError(cause) {
  const error = stateError(
    'SQLite support is required: use a Node.js build with node:sqlite (supported here on Node 22.13+), or inject a compatible DatabaseSync constructor',
    'SQLITE_SUPPORT_REQUIRED',
  );
  error.cause = cause;
  return error;
}

async function defaultDatabaseSync() {
  try {
    const sqlite = await import('node:sqlite');
    if (typeof sqlite.DatabaseSync !== 'function') throw new Error('DatabaseSync export is unavailable');
    return sqlite.DatabaseSync;
  } catch (error) {
    throw sqliteSupportError(error);
  }
}

function ensureSyncResult(result) {
  if (result && typeof result.then === 'function') throw stateError('Metadata transactions must use a synchronous callback', 'TRANSACTION');
  return result;
}

/**
 * Transactional store for bounded, queryable metadata. Full transcripts and
 * binary bodies belong in ArtifactStore; every JSON row is deliberately small.
 */
export class MetadataStore {
  static async open(path, options = {}) {
    if (typeof path !== 'string' || !path) throw stateError('Metadata database path is required');
    await mkdir(dirname(path), { recursive: true, mode: 0o700 });
    // SQLite opens sidecars itself. Validate every possible pre-existing path
    // before the database can follow a redirected WAL, journal, or SHM file.
    let hadDatabase = false;
    for (const candidate of [path, `${path}-wal`, `${path}-shm`, `${path}-journal`]) {
      try {
        const info = await lstat(candidate);
        if (candidate === path) hadDatabase = true;
        if (!info.isFile() || info.isSymbolicLink()) throw stateError('Metadata database and sidecars must be regular non-symlink files', 'STORAGE');
      } catch (error) {
        if (error.code !== 'ENOENT') throw error;
      }
    }
    const DatabaseSync = options.DatabaseSync ?? await defaultDatabaseSync();
    let database;
    try {
      database = new DatabaseSync(path);
    } catch (error) {
      if (error?.code === 'ERR_UNKNOWN_BUILTIN_MODULE') throw sqliteSupportError(error);
      const wrapped = stateError('Metadata database could not be opened safely', 'STORAGE'); wrapped.cause = error; throw wrapped;
    }
    const store = new MetadataStore(path, database, options);
    try {
      if (hadDatabase) store._assertCompatibleLayout();
      store._initialize();
      return store;
    } catch (error) {
      try { database.close(); } catch {}
      if (['STORAGE', 'SQLITE_SUPPORT_REQUIRED', 'INVALID'].includes(error?.code)) throw error;
      const wrapped = stateError('Metadata database could not be opened safely', 'STORAGE'); wrapped.cause = error; throw wrapped;
    }
  }

  constructor(path, database, {
    maxRowBytes = 32 * 1024,
    defaultPageSize = 100,
    maxPageSize = 500,
    maxRowsPerScope = null,
    maxDatabaseBytes = 1024 * 1024 * 1024,
    busyTimeoutMs = 5_000,
    walAutoCheckpointPages = 1_000,
    journalSizeLimitBytes = 16 * 1024 * 1024,
  } = {}) {
    this.path = path;
    this.database = database;
    this.maxRowBytes = positiveInteger(maxRowBytes, 'maxRowBytes', 1024 * 1024);
    this.defaultPageSize = positiveInteger(defaultPageSize, 'defaultPageSize', 10_000);
    this.maxPageSize = positiveInteger(maxPageSize, 'maxPageSize', 10_000);
    if (this.defaultPageSize > this.maxPageSize) throw stateError('defaultPageSize exceeds maxPageSize');
    this.maxRowsPerScope = maxRowsPerScope === null ? null : positiveInteger(maxRowsPerScope, 'maxRowsPerScope', 10_000_000);
    this.maxDatabaseBytes = positiveInteger(maxDatabaseBytes, 'maxDatabaseBytes');
    this.busyTimeoutMs = positiveInteger(busyTimeoutMs, 'busyTimeoutMs', 60_000);
    this.walAutoCheckpointPages = positiveInteger(walAutoCheckpointPages, 'walAutoCheckpointPages', 1_000_000);
    this.journalSizeLimitBytes = positiveInteger(journalSizeLimitBytes, 'journalSizeLimitBytes');
    this.closed = false;
    this.transactionDepth = 0;
    this.savepointCounter = 0;
  }

  _assertCompatibleLayout() {
    const names = this.database.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'").all().map(row => row.name);
    if (!names.length) return;
    const actual = new Set(names);
    const legacy = METADATA_TABLES.filter(name => !name.startsWith('result_'));
    const matches = expected => actual.size === expected.length && expected.every(name => actual.has(name));
    const beforeSnapshots = METADATA_TABLES.filter(name => !['result_snapshots', 'result_snapshot_entries'].includes(name));
    const supported = [METADATA_TABLES, beforeSnapshots, beforeSnapshots.filter(name => name !== 'result_subscriptions'), beforeSnapshots.filter(name => !['result_bindings', 'result_subscriptions'].includes(name)), legacy];
    if (!supported.some(tables => matches(tables) || matches(tables.filter(name => name !== 'team_runs')))) throw stateError('Unsupported metadata layout; use a fresh runtime state directory', 'STORAGE');
  }

  _initialize() {
    const db = this.database;
    db.exec(`
      PRAGMA journal_mode = WAL;
      PRAGMA synchronous = FULL;
      PRAGMA foreign_keys = ON;
      PRAGMA trusted_schema = OFF;
      PRAGMA busy_timeout = ${this.busyTimeoutMs};
      PRAGMA wal_autocheckpoint = ${this.walAutoCheckpointPages};
      PRAGMA journal_size_limit = ${this.journalSizeLimitBytes};
    `);
    const pageSizeRow = db.prepare('PRAGMA page_size').get();
    const pageSize = Number(pageSizeRow.page_size);
    const maxPages = Math.max(1, Math.floor(this.maxDatabaseBytes / pageSize));
    db.exec(`PRAGMA max_page_count = ${maxPages};`);
    db.exec('BEGIN IMMEDIATE');
    try {
      for (const table of METADATA_TABLES) {
        const foreignKey = table === 'runs' ? '' : ', FOREIGN KEY (run_id) REFERENCES runs(id) ON DELETE RESTRICT';
        db.exec(`
          CREATE TABLE IF NOT EXISTS ${table} (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            id TEXT NOT NULL UNIQUE,
            run_id TEXT NOT NULL,
            owner_id TEXT NOT NULL,
            state TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            data_bytes INTEGER NOT NULL CHECK (data_bytes >= 2 AND data_bytes <= ${this.maxRowBytes}),
            data_json TEXT NOT NULL
            ${foreignKey}
          );
          CREATE INDEX IF NOT EXISTS ${table}_scope_seq ON ${table}(owner_id, run_id, seq);
          CREATE INDEX IF NOT EXISTS ${table}_scope_state_seq ON ${table}(owner_id, run_id, state, seq);
          CREATE INDEX IF NOT EXISTS ${table}_scope_updated ON ${table}(owner_id, run_id, updated_at);
        `);
      }
      db.exec(`CREATE INDEX IF NOT EXISTS result_entries_version ON result_entries(run_id, owner_id, json_extract(data_json,'$.bundleId'), json_extract(data_json,'$.fileId'), json_extract(data_json,'$.revision'));`);
      db.exec("CREATE INDEX IF NOT EXISTS result_snapshot_entries_path ON result_snapshot_entries(run_id, owner_id, json_extract(data_json,'$.snapshotId'), json_extract(data_json,'$.path'))");
      db.exec('COMMIT');
    } catch (error) {
      try { db.exec('ROLLBACK'); } catch {}
      throw error;
    }
  }

  _assertOpen() {
    if (this.closed) throw stateError('Metadata store is closed', 'STORAGE');
  }

  _findUnscoped(table, id) {
    return this.database.prepare(`SELECT owner_id, run_id, data_json FROM ${table} WHERE id = ?`).get(id);
  }

  _assertExistingScope(table, expected, row) {
    if (!row) return;
    if (row.owner_id !== expected.ownerId || (table !== 'runs' && row.run_id !== expected.runId) || (table === 'runs' && expected.runId !== undefined && row.run_id !== expected.runId)) {
      throw stateError('Metadata record is outside the requested owner scope', 'OWNER_SCOPE');
    }
  }

  _boundedRecord(table, scope, record, existing) {
    const { id, runId } = checkRecordScope(table, scope, record);
    const now = Date.now();
    const state = identifier(record.state ?? existing?.state ?? 'pending', 'record state');
    const createdAt = timestamp(existing?.createdAt ?? record.createdAt ?? now, 'createdAt');
    const updatedAt = timestamp(record.updatedAt ?? now, 'updatedAt');
    if (updatedAt < createdAt) throw stateError('updatedAt precedes createdAt');
    const canonical = { ...record, id, runId, ownerId: scope.ownerId, state, createdAt, updatedAt };
    const body = encodeJson(canonical, this.maxRowBytes);
    return { canonical, body, id, runId, state, createdAt, updatedAt };
  }

  _checkInsertCount(table, scope) {
    if (this.maxRowsPerScope === null) return;
    const where = table === 'runs' ? 'owner_id = ?' : 'owner_id = ? AND run_id = ?';
    const args = table === 'runs' ? [scope.ownerId] : [scope.ownerId, scope.runId];
    const row = this.database.prepare(`SELECT COUNT(*) AS count FROM ${table} WHERE ${where}`).get(...args);
    if (Number(row.count) >= this.maxRowsPerScope) throw stateError(`Metadata row limit reached for ${table}`, 'METADATA_BUDGET');
  }

  put(tableValue, scopeValue, record, { ifAbsent = false } = {}) {
    this._assertOpen();
    const table = tableName(tableValue);
    const scope = scopeFor(table, scopeValue);
    const recordId = identifier(record?.id, 'record id');
    const existingRow = this._findUnscoped(table, recordId);
    this._assertExistingScope(table, scope, existingRow);
    const existing = parseRow(existingRow);
    if (ifAbsent && existing) throw stateError(`Metadata record already exists: ${recordId}`, 'CONFLICT');
    if (!existing) this._checkInsertCount(table, scope);
    const bounded = this._boundedRecord(table, scope, record, existing);
    let result;
    try {
      result = this.database.prepare(`
        INSERT INTO ${table} (id, run_id, owner_id, state, created_at, updated_at, data_bytes, data_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          state = excluded.state,
          updated_at = excluded.updated_at,
          data_bytes = excluded.data_bytes,
          data_json = excluded.data_json
        WHERE ${table}.owner_id = excluded.owner_id AND ${table}.run_id = excluded.run_id
      `).run(
        bounded.id,
        bounded.runId,
        scope.ownerId,
        bounded.state,
        bounded.createdAt,
        bounded.updatedAt,
        bounded.body.length,
        bounded.body.toString('utf8'),
      );
    } catch (error) {
      if (/full/i.test(error?.message ?? '')) throw stateError('Metadata database byte budget exceeded', 'METADATA_BUDGET');
      throw error;
    }
    if (Number(result.changes) !== 1) throw stateError('Metadata record scope conflict', 'OWNER_SCOPE');
    return bounded.canonical;
  }

  get(tableValue, scopeValue, idValue) {
    this._assertOpen();
    const table = tableName(tableValue);
    const scope = scopeFor(table, scopeValue);
    const id = identifier(idValue, 'record id');
    const row = this._findUnscoped(table, id);
    this._assertExistingScope(table, scope, row);
    return parseRow(row);
  }

  require(tableValue, scopeValue, idValue) {
    const value = this.get(tableValue, scopeValue, idValue);
    if (!value) throw stateError(`Metadata record not found: ${idValue}`, 'NOT_FOUND');
    return value;
  }

  list(tableValue, scopeValue, { after = 0, limit = this.defaultPageSize, state } = {}) {
    this._assertOpen();
    const table = tableName(tableValue);
    const scope = scopeFor(table, scopeValue);
    if (!Number.isSafeInteger(after) || after < 0) throw stateError('Invalid pagination cursor');
    positiveInteger(limit, 'page limit', this.maxPageSize);
    if (state !== undefined) identifier(state, 'record state');
    const filters = table === 'runs' ? ['owner_id = ?', 'seq > ?'] : ['owner_id = ?', 'run_id = ?', 'seq > ?'];
    const args = table === 'runs' ? [scope.ownerId, after] : [scope.ownerId, scope.runId, after];
    if (state !== undefined) { filters.push('state = ?'); args.push(state); }
    args.push(limit + 1);
    const rows = this.database.prepare(
      `SELECT seq, data_json FROM ${table} WHERE ${filters.join(' AND ')} ORDER BY seq ASC LIMIT ?`,
    ).all(...args);
    const hasMore = rows.length > limit;
    const page = hasMore ? rows.slice(0, limit) : rows;
    return {
      items: page.map(parseRow),
      nextCursor: hasMore ? Number(page.at(-1).seq) : null,
    };
  }

  transition(tableValue, scopeValue, idValue, { from, to, patch = {} }) {
    const table = tableName(tableValue);
    const scope = scopeFor(table, scopeValue);
    const id = identifier(idValue, 'record id');
    identifier(to, 'target state');
    const allowed = Array.isArray(from) ? from : [from];
    if (!allowed.length || allowed.some(value => typeof value !== 'string')) throw stateError('A source state is required');
    for (const value of allowed) identifier(value, 'source state');
    if (!patch || typeof patch !== 'object' || Array.isArray(patch)) throw stateError('Transition patch must be an object');
    return this.transaction(() => {
      const current = this.require(table, scope, id);
      if (!allowed.includes(current.state)) throw stateError(`Cannot transition ${table}/${id} from ${current.state} to ${to}`, 'CONFLICT');
      return this.put(table, scope, { ...current, ...patch, id, state: to, updatedAt: Date.now() });
    });
  }

  transaction(callback) {
    this._assertOpen();
    if (typeof callback !== 'function') throw stateError('Transaction callback is required');
    if (callback.constructor?.name === 'AsyncFunction') throw stateError('Metadata transactions require a synchronous callback', 'TRANSACTION');
    const outer = this.transactionDepth === 0;
    const savepoint = `state_${++this.savepointCounter}`;
    this.database.exec(outer ? 'BEGIN IMMEDIATE' : `SAVEPOINT ${savepoint}`);
    this.transactionDepth++;
    try {
      const result = ensureSyncResult(callback(this));
      this.database.exec(outer ? 'COMMIT' : `RELEASE SAVEPOINT ${savepoint}`);
      return result;
    } catch (error) {
      try { this.database.exec(outer ? 'ROLLBACK' : `ROLLBACK TO SAVEPOINT ${savepoint}; RELEASE SAVEPOINT ${savepoint}`); } catch {}
      throw error;
    } finally {
      this.transactionDepth--;
    }
  }

  /**
   * Mark stale in-flight rows orphaned while retaining their complete record.
   * The caller must provide the cutoff it has proven cannot still be live.
   */
  recoverOrphans(scopeValue, { before, states = DEFAULT_STALE_STATES, limit = this.maxPageSize } = {}) {
    this._assertOpen();
    timestamp(before, 'orphan cutoff');
    positiveInteger(limit, 'recovery limit', this.maxPageSize);
    const recovered = [];
    let truncated = false;
    this.transaction(() => {
      for (const [tableValue, tableStates] of Object.entries(states)) {
        const table = tableName(tableValue);
        if (table === 'runs') throw stateError('Run recovery state must be handled explicitly');
        const scope = scopeFor(table, scopeValue);
        if (!Array.isArray(tableStates) || !tableStates.length) throw stateError(`Recovery states missing for ${table}`);
        for (const value of tableStates) identifier(value, 'recovery state');
        const marks = tableStates.map(() => '?').join(', ');
        const remaining = limit - recovered.length;
        if (remaining <= 0) { truncated = true; break; }
        const rows = this.database.prepare(`
          SELECT data_json FROM ${table}
          WHERE owner_id = ? AND run_id = ? AND updated_at < ? AND state IN (${marks})
          ORDER BY seq ASC LIMIT ?
        `).all(scope.ownerId, scope.runId, before, ...tableStates, remaining + 1);
        if (rows.length > remaining) truncated = true;
        for (const row of rows.slice(0, remaining)) {
          const current = parseRow(row);
          const next = this.put(table, scope, {
            ...current,
            state: 'orphaned',
            orphanedAt: Date.now(),
            previousState: current.state,
          });
          recovered.push({ table, record: next });
        }
        if (truncated) break;
      }
    });
    return { recovered, truncated };
  }

  count(tableValue, scopeValue, { state } = {}) {
    this._assertOpen();
    const table = tableName(tableValue);
    const scope = scopeFor(table, scopeValue);
    if (state !== undefined) identifier(state, 'record state');
    const filters = table === 'runs' ? ['owner_id = ?'] : ['owner_id = ?', 'run_id = ?'];
    const args = table === 'runs' ? [scope.ownerId] : [scope.ownerId, scope.runId];
    if (state !== undefined) { filters.push('state = ?'); args.push(state); }
    return Number(this.database.prepare(`SELECT COUNT(*) AS count FROM ${table} WHERE ${filters.join(' AND ')}`).get(...args).count);
  }

  checkpoint(mode = 'PASSIVE') {
    this._assertOpen();
    if (!['PASSIVE', 'FULL', 'RESTART', 'TRUNCATE'].includes(mode)) throw stateError('Invalid WAL checkpoint mode');
    return this.database.prepare(`PRAGMA wal_checkpoint(${mode})`).get();
  }

  table(tableValue) {
    const name = tableName(tableValue);
    return Object.freeze({
      put: (scope, record, options) => this.put(name, scope, record, options),
      get: (scope, id) => this.get(name, scope, id),
      require: (scope, id) => this.require(name, scope, id),
      list: (scope, options) => this.list(name, scope, options),
      transition: (scope, id, transition) => this.transition(name, scope, id, transition),
    });
  }

  close() {
    if (this.closed) return;
    if (this.transactionDepth) throw stateError('Cannot close metadata store during a transaction', 'TRANSACTION');
    this.database.close();
    this.closed = true;
  }
}

export async function openMetadata(path, options) {
  return MetadataStore.open(path, options);
}
