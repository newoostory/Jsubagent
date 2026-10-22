import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { lstat, realpath } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';
import { encodeJson, fail, hash, identifier, LIMITS } from './limits.mjs';
import { RunStore, readBounded, readJson } from './storage.mjs';
import { TaskCache } from './state/task-cache.mjs';
import { verifySegmentReference } from './result-segments.mjs';
import { resultHead, resultPrimaryRef } from './results.mjs';
import { executionReadiness } from './result-evidence.mjs';
import { qualityCompletion } from './quality-contract.mjs';
import { effectInputDigest } from './execution/tool-journal.mjs';
import { effectRequiresReconciliation } from './execution/failure-receipt.mjs';
import { automaticRecoverySafe } from './runtime/recovery.mjs';

const MAX_TEAM_BYTES = 256 * 1024;
const TERMINAL = new Set(['idle', 'failed', 'blocked', 'cancelled', 'interrupted', 'paused']);
const require = createRequire(import.meta.url);
export async function saveTeam(store, team, ifAbsent = false) {
  const bytes = encodeJson(team, MAX_TEAM_BYTES), artifact = await store.artifact(team.id, 'team-state');
  await artifact.write(bytes); await artifact.finish();
  store.metadata.put('team_runs', store.scope, { id: team.id, state: team.state, revision: team.revision, contractHash: team.contractHash,
    ref: { artifactId: artifact.info.id, sha256: hash(bytes), bytes: bytes.length } }, { ifAbsent });
}
export async function loadTeam(store, id) {
  const row = store.metadata.get('team_runs', store.scope, identifier(id));
  if (!row?.ref) throw fail('Team state not found', 'NOT_FOUND');
  const info = await store.artifactInfo(row.ref.artifactId), actual = await store.artifactIdentity(row.ref.artifactId);
  if (info.label !== 'team-state' || actual.owner !== id || actual.sha256 !== row.ref.sha256 || actual.bytes !== row.ref.bytes || actual.bytes > MAX_TEAM_BYTES) throw fail('Team state identity changed', 'STORAGE');
  const bytes = await readBounded(join(store.root, 'artifacts', `${identifier(row.ref.artifactId)}.bin`), MAX_TEAM_BYTES);
  if (bytes.length !== row.ref.bytes || hash(bytes) !== row.ref.sha256) throw fail('Team state changed while reading', 'STORAGE');
  const team = JSON.parse(bytes.toString('utf8'));
  if (team.id !== id || team.state !== row.state || team.revision !== row.revision || team.contractHash !== row.contractHash || hash(encodeJson(team.spec)) !== team.contractHash) throw fail('Team record binding mismatch', 'STORAGE');
  return team;
}

/** Opens existing durable evidence only. Never creates a host, SDK client or account. */
export async function openTeamEvidence(runRoot) {
  const root = await realpath(runRoot);
  if (root !== runRoot) throw fail('Team run root must be canonical', 'STORAGE');
  const rootInfo = await lstat(root);
  if (!rootInfo.isDirectory() || rootInfo.isSymbolicLink() || (rootInfo.mode & 0o077)) throw fail('Team run root must be private', 'STORAGE');
  for (const directory of ['records', 'artifacts']) {
    const path = join(root, directory), info = await lstat(path);
    if (!info.isDirectory() || info.isSymbolicLink() || await realpath(path) !== path) throw fail('Unsafe team evidence namespace', 'STORAGE');
  }
  const path = join(root, 'metadata.sqlite');
  for (const candidate of [path, `${path}-wal`, `${path}-shm`, `${path}-journal`]) {
    const info = await lstat(candidate).catch(error => { if (error.code !== 'ENOENT') throw error; });
    if (info && (!info.isFile() || info.isSymbolicLink())) throw fail('Unsafe team metadata', 'STORAGE');
  }
  const { DatabaseSync } = await import('node:sqlite');
  const database = new DatabaseSync(path, { readOnly: true });
  try {
    if (database.prepare('PRAGMA quick_check').get()?.quick_check !== 'ok') throw fail('Team metadata integrity failed', 'STORAGE');
    const scope = { ownerId: 'host', runId: hash(root) };
    const allowed = new Set(['team_runs', 'work_items', 'result_heads']);
    const metadata = { database,
      get(table, _scope, id) {
        if (!allowed.has(table)) throw fail('Unsupported evidence table', 'STORAGE');
        const row = database.prepare(`SELECT data_json FROM ${table} WHERE owner_id=? AND run_id=? AND id=?`).get(scope.ownerId, scope.runId, identifier(id));
        return row ? JSON.parse(row.data_json) : null;
      },
      list(table, _scope, { after = 0, limit = 100 } = {}) {
        if (table !== 'work_items') throw fail('Unsupported evidence listing', 'STORAGE');
        const rows = database.prepare('SELECT seq,data_json FROM work_items WHERE owner_id=? AND run_id=? AND seq>? ORDER BY seq LIMIT ?').all(scope.ownerId, scope.runId, after, limit + 1);
        return { items: rows.slice(0, limit).map(row => JSON.parse(row.data_json)), nextCursor: rows.length > limit ? Number(rows[limit - 1].seq) : null };
      } };
    const store = Object.assign(Object.create(RunStore.prototype), { root, scope, limits: LIMITS, metadata });
    return { store, tasks: new TaskCache(store), close: () => database.close() };
  } catch (error) { database.close(); throw error; }
}

export function teamDeclarationDigest(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw fail('Actual team verification declaration is required', 'QUALITY');
  return effectInputDigest({ ...value, residualRisks: value.residualRisks ?? [], artifacts: value.artifacts ?? [] });
}
export async function verifyTeamPlanBinding(binding, { workspace, phase, declaration } = {}) {
  if (!['before_checks', 'before_close', 'evidence_read'].includes(phase)) throw fail('Unknown team completion phase', 'INVALID');
  if (await realpath(workspace) !== workspace || binding.workspace !== workspace) throw fail('Team destination mismatch', 'PERMISSION');
  const evidence = await openTeamEvidence(binding.team.runRoot);
  try {
    const { store, tasks } = evidence, team = await loadTeam(store, binding.team.teamId);
    const hostBinding = await readJson(join(store.root, 'bridge-binding.json'), 32 * 1024);
    if (hostBinding.cwd !== workspace || hostBinding.root !== store.root || !hostBinding.trusted) throw fail('Team has no trusted destination binding', 'PERMISSION');
    if (!['active', 'completed'].includes(team.state) || team.operation || team.repair || team.contractHash !== binding.team.contractHash || team.spec.directory !== workspace
      || team.spec.plan?.expectedRevision !== binding.originalRevision || JSON.stringify(team.plan?.binding) !== JSON.stringify(binding)) throw fail('Team policy/PLAN binding is not current', 'QUALITY');
    if (teamDeclarationDigest(declaration) !== teamDeclarationDigest(team.spec.plan.declaration)) throw fail('Actual verification declaration differs from original team approval', 'QUALITY');
    // Recompile policy rather than trusting a caller-selected subset or mutable member list.
    const { normalizeTeam, compileTeam, destinationCommand } = await import('./team.mjs');
    if (hash(encodeJson(normalizeTeam(team.spec))) !== team.contractHash) throw fail('Noncanonical team contract', 'STORAGE');
    const roles = {};
    for (const node of Object.values(team.nodes)) {
      const task = tasks.get(node.agentId); if (!task) throw fail('Required team member is missing', 'QUALITY');
      roles[node.role] = task.role;
    }
    const expected = compileTeam(team.spec, { roles });
    if (Object.keys(expected).sort().join('\0') !== Object.keys(team.nodes).sort().join('\0')) throw fail('Team coverage changed', 'QUALITY');
    const destination = team.nodes.destination;
    if (!destination) throw fail('Required destination verifier is missing', 'QUALITY');
    const members = [];
    for (const node of Object.values(team.nodes)) {
      const policy = expected[node.key], task = tasks.get(node.agentId);
      if (!policy || ['key', 'kind', 'assignment', 'role', 'source'].some(key => policy[key] !== node[key])
        || JSON.stringify(policy.prerequisites) !== JSON.stringify(node.prerequisites) || JSON.stringify(policy.inputs) !== JSON.stringify(node.inputs)
        || node.agentId !== `tm-${hash(`${team.id}:${node.key}:${node.generation}`).slice(0, 36)}`
        || task.role.name !== node.role || !node.requestHash || task.requestHash !== node.requestHash || !node.taskSha256 || hash(task.task) !== node.taskSha256 || task.sourceCwd !== workspace) throw fail('Team assignment identity changed', 'QUALITY');
      if (task.quarantined || task.qualityArchived || task.requestPermitUncertain || (node.kind !== 'destination' && !automaticRecoverySafe(task)) || task.cancelRequested || task.authorityRevoked || task.recoveryPending) throw fail('Team execution is uncertain or revoked', 'QUALITY');
      if (node.kind === 'destination') {
        if (!['running', 'idle'].includes(task.state) || task.usageUncertain || task.worktreeIntent || task.cwd !== workspace || task.role.workspace !== 'shared' || task.role.tools.some(tool => ['write', 'edit'].includes(tool))) throw fail('Destination verification is not current', 'QUALITY');
        const pending = (task.pendingEffects ?? []).filter(effectRequiresReconciliation);
        if (pending.length && !(task.state === 'running' && pending.length === 1 && pending[0].tool === 'bash' && pending[0].outcome === 'intent'
          && pending[0].attemptId === task.executionIntent?.id && pending[0].inputDigest === effectInputDigest({ command: destinationCommand(store.root, team.id) }))) throw fail('Destination has an unapproved or uncertain effect', 'QUALITY');
        continue;
      }
      const settled = row => TERMINAL.has(row.state) && !row.recoveryPending;
      const head = row => { const value = resultHead(store, row); return value && { ...value, canonicalResult: hash(encodeJson(resultPrimaryRef(store, value) ?? null)) === hash(encodeJson(row.resultRef ?? null)) }; };
      const gate = qualityCompletion([task], () => tasks.values({ includeArchived: true }), { settled, head, lookup: id => tasks.get(id, { includeArchived: true }),
        execution: row => executionReadiness(store.metadata.database, store.scope, row.id, row.resultEpoch ?? 1), unresolved: row => !automaticRecoverySafe(row) });
      if (!node.issued || gate.state !== 'ready_for_acceptance') throw fail(`Required team stage ${node.key} is not ready: ${gate.agents[0].reasons.join(',')}`, 'QUALITY');
      if (!task.resultRef) throw fail('Required canonical result missing', 'QUALITY');
      if (task.resultRef.type === 'segments-v1') await verifySegmentReference(store, task.id, task.resultRef);
      else {
        const identity = await store.artifactIdentity(task.resultRef.artifactId);
        if (identity.sha256 !== task.resultRef.sha256 || identity.bytes !== task.resultRef.bytes || identity.owner !== task.id) throw fail('Team result bytes changed', 'STORAGE');
      }
      members.push({ key: node.key, agentId: task.id, resultEpoch: task.resultEpoch, goalRevision: task.goalRevision, resultRef: task.resultRef, quality: task.quality, bindings: task.resultBindings ?? [] });
    }
    // The final check must run against a frozen destination, including unrelated writers.
    for (const task of tasks.values()) if (task.id !== destination.agentId && task.cwd === workspace && (!TERMINAL.has(task.state) || !automaticRecoverySafe(task))
      && task.role?.tools?.some(tool => ['bash', 'write', 'edit'].includes(tool))) throw fail('Destination writer is still active', 'BUSY');
    const verification = await import(pathToFileURL(join(dirname(require.resolve('pi-super/package.json')), 'src/verification.js')).href);
    const fresh = await verification.establishWorkspaceFreshness(workspace, undefined, team.spec.plan.declaration.inputScope);
    if (fresh.status !== 'established') throw fail('Destination freshness cannot be established', 'QUALITY');
    const proofSha256 = hash(encodeJson({ binding, contractHash: team.contractHash, revision: team.revision, members, destination: { agentId: destination.agentId, resultEpoch: tasks.get(destination.agentId).resultEpoch, goalRevision: tasks.get(destination.agentId).goalRevision, taskSha256: destination.taskSha256 }, freshness: fresh }, LIMITS.frameBytes));
    return { teamId: team.id, contractHash: team.contractHash, revision: team.revision, proofSha256, runRoot: store.root, planId: binding.planId };
  } finally { evidence.close(); }
}

/** A native failed command, rather than a model's failure assertion, permits bounded destination repair. */
export async function readTeamPlanFailure(team) {
  if (!team.plan) return null;
  const root = dirname(require.resolve('pi-super/package.json'));
  const plan = await import(pathToFileURL(join(root, 'src/plan/index.js')).href);
  const verification = await import(pathToFileURL(join(root, 'src/verification.js')).href);
  const state = await plan.executePlanAction('plan_status', {}, { cwd: team.spec.directory });
  if (state.state !== 'active' || state.metadata.planId !== team.plan.binding.planId) return null;
  const parsed = plan.parsePlan(state.content, 'active');
  if (teamDeclarationDigest(verification.parseVerificationDeclaration(parsed.sections.Verification)) !== teamDeclarationDigest(team.spec.plan.declaration)) throw fail('Failed check declaration differs from approved team policy', 'QUALITY');
  const match = /Evidence: `([^`]+)` \(sha256: `([0-9a-f]{64})`, bytes: (\d+)\)/u.exec(parsed.sections.Verification);
  if (!match || !/^\.pi\/plan\/verification\/[0-9a-f]{64}-[0-9a-f-]+\.json$/u.test(match[1])) return null;
  const pointer = { path: match[1], sha256: match[2], bytes: Number(match[3]) };
  const actual = await verification.hashArtifact(team.spec.directory, pointer);
  if (actual.sha256 !== pointer.sha256 || actual.bytes !== pointer.bytes) throw fail('Failed verification evidence changed', 'STORAGE');
  const summary = await readJson(join(team.spec.directory, pointer.path), verification.VERIFICATION_LIMITS.maxArtifactBytes);
  if (summary.schemaVersion !== verification.VERIFICATION_SCHEMA_VERSION || summary.pass !== false || summary.gateVerdict !== 'blocked'
    || summary.teamEvidence?.teamId !== team.id || summary.teamEvidence.contractHash !== team.contractHash || summary.teamEvidence.revision !== team.revision
    || summary.teamEvidence.runRoot !== team.plan.binding.team.runRoot || summary.teamEvidence.bindingSha256 !== hash(encodeJson(team.plan.binding))) return null;
  const checks = [...(summary.requiredChecks ?? []), ...(Array.isArray(summary.realFlow) ? summary.realFlow : summary.realFlow ? [summary.realFlow] : [])];
  if (!checks.some(check => check.pass === false && Number.isInteger(check.exitCode) && check.exitCode !== 0)) return null;
  return { evidence: pointer, failures: summary.failures, failedChecks: checks.filter(check => check.pass === false).map(check => ({ name: check.name, argv: check.argv, exitCode: check.exitCode })) };
}
