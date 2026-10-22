import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { realpath } from 'node:fs/promises';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { encodeJson, fail, hash, identifier, safeError, text } from './limits.mjs';
import { normalizeDependencies } from './dependencies.mjs';
import { normalizeResultInputs } from './result-inputs.mjs';
import { normalizeQualityContract } from './quality-contract.mjs';
import { assertProfilePin } from './config.mjs';
import { authorizeMainMutation } from './mode-access.mjs';
import { automaticRecoverySafe } from './runtime/recovery.mjs';
import { loadTeam, saveTeam, readTeamPlanFailure } from './team-evidence.mjs';

const require = createRequire(import.meta.url);
const ACTIVE = new Set(['active', 'replanning']);
const object = value => value && typeof value === 'object' && !Array.isArray(value);
function exact(value, allowed, label) { if (!object(value) || Object.keys(value).some(key => !allowed.includes(key))) throw fail(`Invalid ${label}`); }
export async function planRuntime() { return import(pathToFileURL(join(dirname(require.resolve('pi-super/package.json')), 'src/team-completion.js')).href); }
export function normalizeTeam(spec) {
  exact(spec, ['id', 'goal', 'directory', 'units', 'roles', 'profile', 'independentReview', 'integration', 'plan', 'maxRepairs', 'maxTotalTurns'], 'team');
  identifier(spec.id, 'team ID'); text(spec.goal, 'team goal', 4096); text(spec.directory, 'team directory', 4096);
  exact(spec.roles, ['produce', 'review', 'integrate', 'verify'], 'team roles');
  for (const role of Object.values(spec.roles)) identifier(role, 'team role');
  identifier(spec.roles.produce, 'producer role'); if (spec.profile !== undefined) identifier(spec.profile, 'team profile');
  if (!Array.isArray(spec.units) || spec.units.length < 1 || spec.units.length > 4) throw fail('Team units require 1..4 explicitly scoped items');
  const seen = new Set();
  const units = spec.units.map(unit => {
    exact(unit, ['id', 'task', 'dependsOn', 'qualityContract'], 'team unit');
    const id = identifier(unit.id); if (seen.has(id)) throw fail('Duplicate team unit');
    const dependsOn = unit.dependsOn ?? [];
    if (!Array.isArray(dependsOn) || new Set(dependsOn).size !== dependsOn.length || dependsOn.some(id => !seen.has(id))) throw fail('Team prerequisites must reference distinct earlier units');
    seen.add(id);
    const qualityContract = normalizeQualityContract(unit.qualityContract); if (!qualityContract || qualityContract.stage !== 'produce') throw fail('Team producer needs an explicit produce quality contract');
    return { id, task: text(unit.task, 'unit task', 8192), dependsOn: [...dependsOn], qualityContract };
  });
  const independentReview = spec.independentReview ?? units.length > 1;
  if (typeof independentReview !== 'boolean') throw fail('Invalid independentReview');
  if ((independentReview || units.some(unit => unit.qualityContract.review === 'required')) && !spec.roles.review) throw fail('Independent review role required');
  let integration;
  if (spec.integration) {
    exact(spec.integration, ['task', 'qualityContract', 'backup'], 'team integration');
    integration = { task: text(spec.integration.task, 'integration task', 8192), backup: text(spec.integration.backup, 'integration backup boundary', 4096), qualityContract: normalizeQualityContract(spec.integration.qualityContract) };
    if (!integration.qualityContract || integration.qualityContract.stage !== 'integrate' || !spec.roles.integrate) throw fail('Integration role and contract required');
    if (integration.qualityContract.review === 'required' && !spec.roles.review) throw fail('Integration review role required');
  }
  if (units.length > 1 && !integration) throw fail('Multiple deliverables require an explicit integration assignment');
  let plan;
  if (spec.plan) {
    exact(spec.plan, ['expectedRevision', 'declaration', 'completionSections'], 'team PLAN');
    text(spec.plan.expectedRevision, 'PLAN revision', 80);
    if (!/^[a-f0-9]{64}$/.test(spec.plan.expectedRevision) || !object(spec.plan.declaration)) throw fail('Invalid PLAN revision/declaration');
    exact(spec.plan.completionSections, ['Done Criteria', 'Milestones', 'Remaining Work', 'Next Action'], 'PLAN completion sections');
    if (!spec.roles.verify) throw fail('Destination verifier role required');
    plan = structuredClone(spec.plan);
  }
  const maxRepairs = spec.maxRepairs ?? null;
  if (maxRepairs !== null && (!Number.isSafeInteger(maxRepairs) || maxRepairs < 0 || maxRepairs > 2)) throw fail('Team maxRepairs must be null or 0..2');
  if (spec.maxTotalTurns !== undefined && spec.maxTotalTurns !== null && (!Number.isSafeInteger(spec.maxTotalTurns) || spec.maxTotalTurns < 1 || spec.maxTotalTurns > 4096)) throw fail('Invalid team turn budget');
  const result = { id: spec.id, goal: spec.goal, directory: spec.directory, units, roles: { ...spec.roles }, ...(spec.profile ? { profile: spec.profile } : {}), independentReview, ...(integration ? { integration } : {}), ...(plan ? { plan } : {}), maxRepairs, ...(spec.maxTotalTurns !== undefined ? { maxTotalTurns: spec.maxTotalTurns } : {}) };
  encodeJson(result, 32 * 1024); return result;
}
const producer = id => `produce:${id}`, reviewer = id => `review:${id}`;
export function compileTeam(spec, config) {
  const nodes = {};
  const role = name => { const value = config.roles[name]; if (!value || value.hidden) throw fail(`Unavailable explicit team role ${name}`, 'CAPABILITY'); return value; };
  const make = (key, kind, assignment, prerequisites, inputs, roleName, source = null) => {
    role(roleName);
    nodes[key] = { key, kind, assignment, prerequisites, inputs, role: roleName, source, generation: 0, agentId: `tm-${hash(`${spec.id}:${key}:0`).slice(0, 36)}`, issued: false };
  };
  const ends = {};
  for (const unit of spec.units) {
    const key = producer(unit.id), review = spec.independentReview || unit.qualityContract.review === 'required';
    const producerRole = role(spec.roles.produce);
    if (producerRole.workspace === 'shared' && producerRole.tools.some(tool => ['write', 'edit', 'bash'].includes(tool)) && !unit.qualityContract.ownedPaths.length) throw fail('Shared team writers require explicit ownedPaths', 'CAPABILITY');
    if (review) {
      const reviewerRole = role(spec.roles.review);
      if (reviewerRole.workspace === 'worktree' || reviewerRole.tools.some(tool => ['write', 'edit'].includes(tool))) throw fail('Independent reviewer must be readonly or tests-only', 'CAPABILITY');
    }
    if (role(spec.roles.produce).workspace === 'worktree' && !spec.integration) throw fail('Worktree products require explicitly authorized destination integration', 'CAPABILITY');
    make(key, 'produce', unit.id, unit.dependsOn.map(id => ends[id]), unit.dependsOn.map(producer), spec.roles.produce);
    if (review) { make(reviewer(unit.id), 'review', unit.id, [key], [key], spec.roles.review, key); ends[unit.id] = reviewer(unit.id); }
    else ends[unit.id] = key;
  }
  if (spec.integration) {
    if (role(spec.roles.integrate).workspace !== 'shared' || !spec.integration.qualityContract.ownedPaths.length) throw fail('Integration requires a shared destination and explicit ownedPaths', 'CAPABILITY');
    make('integration', 'integrate', 'integration', Object.values(ends), spec.units.map(unit => producer(unit.id)), spec.roles.integrate);
    if (spec.integration.qualityContract.review === 'required') make('integration-review', 'review', 'integration', ['integration'], ['integration'], spec.roles.review, 'integration');
  }
  if (spec.plan) {
    const verifier = role(spec.roles.verify);
    if (verifier.workspace !== 'shared' || !verifier.tools.includes('bash') || verifier.tools.some(tool => ['write', 'edit'].includes(tool))) throw fail('Destination verifier must be shared, bash-capable and tests-only', 'CAPABILITY');
    make('destination', 'destination', 'destination', spec.integration ? [nodes['integration-review'] ? 'integration-review' : 'integration'] : Object.values(ends), [], spec.roles.verify);
  }
  return nodes;
}
export function assertTeamGraph(team, config) {
  if (hash(encodeJson(normalizeTeam(team.spec))) !== team.contractHash) throw fail('Team contract changed', 'STORAGE');
  const expected = compileTeam(team.spec, config);
  if (!Number.isSafeInteger(team.round) || team.round < 0 || team.spec.maxRepairs !== null && team.round > team.spec.maxRepairs) throw fail('Team repair round changed', 'STORAGE');
  if (Object.keys(expected).sort().join('\0') !== Object.keys(team.nodes).sort().join('\0')) throw fail('Required team coverage changed', 'STORAGE');
  for (const node of Object.values(team.nodes)) {
    const policy = expected[node.key];
    if (!Number.isSafeInteger(node.generation) || node.generation < 0 || node.generation > team.round
      || team.spec.maxRepairs !== null && node.generation > team.spec.maxRepairs
      || ['key', 'kind', 'assignment', 'role', 'source'].some(key => node[key] !== policy[key])
      || JSON.stringify(node.prerequisites) !== JSON.stringify(policy.prerequisites) || JSON.stringify(node.inputs) !== JSON.stringify(policy.inputs)
      || node.agentId !== `tm-${hash(`${team.id}:${node.key}:${node.generation}`).slice(0, 36)}`) throw fail('Team member graph changed', 'STORAGE');
  }
  if (!Array.isArray(team.history)) throw fail('Team member history changed', 'STORAGE');
  const ids = new Set(Object.values(team.nodes).map(node => node.agentId));
  for (const row of team.history) {
    const current = team.nodes[row.key];
    if (!current || row.kind !== current.kind || row.source !== current.source || !Number.isSafeInteger(row.generation) || row.generation < 0 || row.generation >= current.generation
      || row.agentId !== `tm-${hash(`${team.id}:${row.key}:${row.generation}`).slice(0, 36)}` || ids.has(row.agentId)) throw fail('Team member history changed', 'STORAGE');
    ids.add(row.agentId);
  }
  for (const node of Object.values(team.nodes)) if (team.history.filter(row => row.key === node.key).length !== node.generation) throw fail('Team member history is incomplete', 'STORAGE');
  // Historical finite teams predate this audit; new continuous teams require it.
  if (team.repairFailures !== undefined || team.spec.maxRepairs === null) {
    if (!Array.isArray(team.repairFailures)) throw fail('Team repair progress audit missing', 'STORAGE');
    const fingerprints = new Set(); let round = 0;
    for (const row of team.repairFailures) {
      if (!team.nodes[row.root] || !['produce', 'integrate'].includes(team.nodes[row.root].kind) || !/^[a-f0-9]{64}$/.test(row.fingerprint)
        || !Number.isSafeInteger(row.round) || row.round <= round || row.round > team.round || fingerprints.has(row.fingerprint)) throw fail('Team repair progress audit changed', 'STORAGE');
      round = row.round; fingerprints.add(row.fingerprint);
    }
    if (team.spec.maxRepairs === null && team.repairFailures.length !== team.round) throw fail('Team repair progress audit is incomplete', 'STORAGE');
  }
}
// Match the existing Engine admission identity, including immutable contracts and input selectors.
export function teamRequestHash(params) {
  const dependsOn = normalizeDependencies(params.dependsOn, params.id), scope = {};
  const inputs = normalizeResultInputs(params.resultInputs);
  if (inputs.length) scope.resultInputs = inputs;
  if (params.resultContract) scope.resultContract = params.resultContract;
  if (params.qualityContract) scope.qualityContract = normalizeQualityContract(params.qualityContract);
  return hash(encodeJson({ role: params.role, task: params.task, profile: params.profile, directory: params.directory,
    mainModel: params.mainModel, projectInstructions: '', ...(dependsOn.length ? { dependsOn } : {}), ...scope }));
}
export function repairClosure(nodes, roots) {
  const affected = new Set(roots);
  let changed;
  do { changed = false; for (const node of Object.values(nodes)) if (!affected.has(node.key) && node.prerequisites.some(key => affected.has(key))) { affected.add(node.key); changed = true; } } while (changed);
  return [...affected];
}
export function destinationCommand(runRoot, teamId) {
  const payload = Buffer.from(JSON.stringify({ runRoot, teamId })).toString('base64');
  return ['node', fileURLToPath(new URL('./team-plan-verify.mjs', import.meta.url)), payload].map(quote).join(' ');
}
const quote = value => "'" + String(value).replaceAll("'", "'\\''") + "'";

export class TeamCoordinator {
  constructor(engine) {
    this.engine = engine; this.store = engine.store; this.lanes = new Map(); this.closed = false; this.scheduled = false;
    this.unsubscribe = engine.subscribeChanges(() => this.schedule());
  }
  async init() {
    for (const row of this.rows()) {
      const team = await loadTeam(this.store, row.id);
      if (team.state === 'starting' && !team.operation) { team.state = 'attention'; team.error = 'Interrupted PLAN binding/admission requires inspection of its durable CAS outcome'; await saveTeam(this.store, team); }
      if (team.operation) {
        const task = this.engine.tasks.get(team.operation.agentId);
        if (!task || task.role?.name !== team.nodes[team.operation.key].role || hash(task.task) !== team.operation.taskSha256 || !team.operation.requestHash || task.requestHash !== team.operation.requestHash || team.operation.kind === 'resume' && task.resultEpoch <= team.operation.beforeEpoch) { team.state = 'attention'; team.error = 'Interrupted admission/resume has no confirmed durable outcome; inspect before retry'; await saveTeam(this.store, team); }
        else { team.nodes[team.operation.key].issued = true; team.nodes[team.operation.key].taskSha256 = team.operation.taskSha256; team.nodes[team.operation.key].requestHash = team.operation.requestHash; if (team.operation.kind === 'resume' && team.repair?.phase === 'resume') { team.state = 'active'; delete team.repair; } delete team.operation; team.revision++; await saveTeam(this.store, team); }
      }
    }
    this.schedule();
  }
  *rows() { let after = 0; for (;;) { const page = this.store.metadata.list('team_runs', this.store.scope, { after, limit: 32 }); yield* page.items; if (page.nextCursor === null) return; after = page.nextCursor; } }
  async start(value, mainModel) {
    const spec = normalizeTeam(value), contractHash = hash(encodeJson(spec));
    if (this.store.metadata.get('team_runs', this.store.scope, spec.id)) { const prior = await loadTeam(this.store, spec.id); if (prior.contractHash !== contractHash) throw fail('Team ID reused for a different contract', 'CONFLICT'); return this.status(spec.id); }
    if ([...this.rows()].length >= 128) throw fail('Team history limit reached; use a fresh run', 'LIMIT');
    if ([...this.rows()].filter(row => !['completed', 'cancelled'].includes(row.state)).length >= 8) throw fail('At most eight unsettled teams', 'LIMIT');
    if (await realpath(spec.directory) !== spec.directory || spec.directory !== this.engine.config.cwd) throw fail('Team destination must be the canonical authorized host workspace', 'PERMISSION');
    const nodes = compileTeam(spec, this.engine.config);
    for (const node of Object.values(nodes)) {
      const role = this.engine.config.roles[node.role], profileName = spec.profile ?? role.profile, profile = this.engine.config.profiles[profileName];
      if (!profile) throw fail('Team explicit profile is unavailable', 'CAPABILITY');
      assertProfilePin(role, profile, profileName); authorizeMainMutation(role, profile, this.engine.modes ?? new Map());
    }
    const team = { id: spec.id, state: 'starting', spec, contractHash, mainModel, nodes, history: [], repairFailures: [], round: 0, revision: 1, maxTotalTurns: spec.maxTotalTurns ?? null, createdAt: Date.now() };
    await saveTeam(this.store, team, true);
    try {
      if (spec.plan) {
        const runtime = await planRuntime();
        const result = await runtime.bindTeamPlan({ cwd: spec.directory, expectedRevision: spec.plan.expectedRevision, team: { runRoot: this.store.root, teamId: team.id, contractHash } });
        team.plan = { binding: result.binding, revision: result.revision };
      }
      team.state = 'active'; await saveTeam(this.store, team); this.schedule();
    } catch (error) { team.state = 'attention'; team.error = safeError(error); await saveTeam(this.store, team); throw error; }
    return this.status(team.id);
  }
  schedule() {
    if (this.closed || this.scheduled) return; this.scheduled = true;
    setImmediate(() => { this.scheduled = false; for (const row of this.rows()) if (ACTIVE.has(row.state)) void this.advance(row.id).catch(() => {}); });
  }
  advance(id) {
    const previous = this.lanes.get(id) ?? Promise.resolve();
    const next = previous.catch(() => {}).then(() => this.advanceOwned(id));
    this.lanes.set(id, next); next.finally(() => { if (this.lanes.get(id) === next) this.lanes.delete(id); }).catch(() => {}); return next;
  }
  settled(task) { return task && this.engine.summary(task).settled; }
  safe(task) { return task && !task.cancelRequested && !task.authorityRevoked && !task.quarantined && !task.requestPermitUncertain && !task.usageUncertain && automaticRecoverySafe(task); }
  rawReady(task) { return this.settled(task) && task.state === 'idle' && task.quality?.state === 'candidate_ready' && this.safe(task); }
  async status(id) {
    const team = await loadTeam(this.store, identifier(id));
    let state = team.state, error = team.error, receipt = team.receipt ?? null;
    try {
      assertTeamGraph(team, this.engine.config);
      if (state === 'completed') {
        const required = Object.values(team.nodes);
        if (!receipt || receipt.members.length !== required.length || required.some(node => { const task = this.engine.tasks.get(node.agentId), prior = receipt.members.find(row => row.agentId === node.agentId); return !task || !prior || !this.settled(task) || task.state !== 'idle' || prior.resultEpoch !== task.resultEpoch || hash(encodeJson(prior.resultRef)) !== hash(encodeJson(task.resultRef ?? null)); })) throw fail('Retained team receipt no longer matches current members', 'QUALITY');
        for (const node of required.filter(row => row.kind !== 'destination')) if (this.engine.qualityCheck([node.agentId]).state !== 'ready_for_acceptance') throw fail('Current team quality evidence changed', 'QUALITY');
        if (team.plan && !await (await planRuntime()).readTeamPlanCompletion(team.plan.binding, { workspace: team.spec.directory })) throw fail('Current PLAN evidence is missing', 'QUALITY');
      }
    } catch (failure) { state = 'attention'; error = safeError(failure); receipt = null; }
    const members = Object.values(team.nodes).map(node => { const task = this.engine.tasks.get(node.agentId); return { key: node.key, kind: node.kind, agentId: node.agentId, generation: node.generation, issued: node.issued, state: task?.state ?? 'not_started', settled: task ? this.settled(task) : false }; });
    return { teamId: team.id, state, contractHash: team.contractHash, revision: team.revision, round: team.round, strategy: team.spec.units.length === 1 ? 'single_work_unit' : 'independent_work_units', requiredMembers: members, error, receipt, historyCount: team.history.length, acceptance: 'not_evaluated' };
  }
  priorReports(team, key) {
    return team.history.filter(row => row.key === key || row.source === key).map(row => this.engine.tasks.get(row.agentId, { includeArchived: true })).filter(Boolean).map(task => ({ agentId: task.id, report: task.currentReport, error: task.error ?? '' }));
  }
  params(team, node) {
    const unit = team.spec.units.find(unit => unit.id === node.assignment), config = this.engine.config;
    const contract = node.kind === 'destination' ? undefined : structuredClone(unit?.qualityContract ?? team.spec.integration.qualityContract);
    if (contract && node.kind === 'review') { contract.stage = config.roles[node.role].tools.includes('bash') ? 'verify' : 'review'; contract.review = 'optional'; }
    else if (contract && node.kind === 'produce' && team.nodes[reviewer(node.assignment)]) contract.review = 'required';
    const reports = this.priorReports(team, node.source ?? node.key);
    let assignment = node.kind === 'review' ? `Independently evaluate every original criterion on the required pinned product. Do not repair it. Prior findings to resolve: ${JSON.stringify(reports)}` : unit?.task ?? team.spec.integration?.task;
    if (node.kind === 'integrate') assignment += `\nUse the actual destination ${team.spec.directory}; backup/restore boundary: ${team.spec.integration.backup}. Apply only reviewed assigned products. Compare fresh destination hashes and preserve unrelated edits. No automatic whole-worktree copy, reset, merge, push or deployment. Input views deliberately do not replace your cwd. Stop on overlap or drift.`;
    if (node.kind === 'destination') {
      const command = destinationCommand(this.store.root, team.id);
      assignment = `Verify the actual destination, with all product writers frozen. Run this exact approved verification command with bash, inspect its real result, then report honestly. Do not repair code/tests or weaken the declaration. No candidate input may replace cwd.\n${command}`;
    }
    const task = `Goal: ${team.spec.goal}\nTeam ${team.id}; stage ${node.key}.\n${assignment}\nProhibited actions: no new authority, account/provider fallback, destructive cleanup or unassigned effects.\nOwned paths: ${JSON.stringify(contract?.ownedPaths ?? [])}\nInput revision/hash: runtime-pinned result inputs and admitted source snapshot.\nAcceptance/tests: ${JSON.stringify(contract?.criteria ?? team.spec.plan?.declaration)}\nStop: complete the assigned evidence or report the exact blocker; never claim tests or completion without evidence.\nPrior retained reports: ${JSON.stringify(reports)}`;
    text(task, 'compiled team assignment', 24 * 1024);
    const resultInputs = node.inputs.map(key => ({ from: { agentId: team.nodes[key].agentId }, when: 'final', required: true, view: node.kind === 'review' && this.engine.tasks.get(team.nodes[key].agentId)?.resultContract === 'code-change' ? config.roles[node.role].tools.includes('bash') ? 'verifier' : 'reviewer' : node.kind === 'review' ? 'main' : 'artifacts' }));
    const dependsOn = node.prerequisites.map(key => team.nodes[key].agentId);
    return { id: node.agentId, role: node.role, task, directory: team.spec.directory, mainModel: team.mainModel, ...(team.spec.profile ? { profile: team.spec.profile } : {}), ...(dependsOn.length ? { dependsOn } : {}), ...(resultInputs.length ? { resultInputs } : {}), ...(contract ? { qualityContract: contract } : {}), ...(node.kind === 'produce' && config.roles[node.role].workspace === 'worktree' ? { resultContract: 'code-change' } : {}) };
  }
  async dispatch(team, node, kind = 'spawn', instruction) {
    const ids = new Set([...Object.values(team.nodes), ...team.history].map(row => row.agentId));
    const turns = [...ids].reduce((sum, id) => sum + (this.engine.tasks.get(id, { includeArchived: true })?.turns ?? 0), 0);
    if (team.maxTotalTurns !== null && (!Number.isSafeInteger(team.maxTotalTurns) || team.maxTotalTurns < 1)) throw fail('Stored team turn budget is invalid', 'STORAGE');
    if (team.maxTotalTurns !== null && turns >= team.maxTotalTurns) throw fail('Cumulative team dispatch turn budget exhausted', 'LIMIT');
    const beforeEpoch = this.engine.tasks.get(node.agentId)?.resultEpoch ?? 0;
    const params = kind === 'spawn' ? this.params(team, node) : undefined;
    node.taskSha256 = params ? hash(params.task) : node.taskSha256 ?? hash(this.engine.get(node.agentId).task);
    node.requestHash = params ? teamRequestHash(params) : node.requestHash ?? this.engine.get(node.agentId).requestHash;
    team.operation = { requestHash: node.requestHash, key: node.key, kind, agentId: node.agentId, beforeEpoch, taskSha256: node.taskSha256, ...(params ? { requestSha256: hash(encodeJson(params)) } : {}) }; await saveTeam(this.store, team);
    if (kind === 'spawn') await this.engine.spawn(params);
    else await this.engine.resume(node.agentId, instruction);
    node.issued = true; delete team.operation; team.revision++; await saveTeam(this.store, team);
  }
  async reconcileReviews(team, node) {
    if (node.kind !== 'review') return;
    const source = this.engine.get(team.nodes[node.source].agentId), next = this.engine.get(node.agentId);
    if (!this.rawReady(next)) return;
    for (const previous of team.history.filter(row => row.kind === 'review' && row.source === node.source && row.agentId !== next.id)) {
      const old = this.engine.tasks.get(previous.agentId, { includeArchived: true });
      if (old?.resultInputDeclarations?.some(input => input.sourceId === source.id) && !(source.qualityReviewSupersessions ?? []).some(row => row.reviewerId === old.id && row.replacementId === next.id)) await this.engine.qualitySupersede({ id: source.id, reviewerId: old.id, replacementId: next.id });
    }
  }
  async beginRepair(team, root, reason, failure) {
    const node = team.nodes[root], task = this.engine.get(node.agentId);
    if (team.spec.maxRepairs !== null && team.round >= team.spec.maxRepairs || !this.safe(task) || ['paused', 'cancelled', 'interrupted'].includes(task.state)) throw fail('Team repair budget or recovery authority prevents automatic retry', 'BLOCKED');
    const contract = node.kind === 'integrate' ? team.spec.integration.qualityContract : team.spec.units.find(unit => unit.id === node.assignment).qualityContract;
    // Compare stable candidate/check outcomes, not report IDs, epoch counters,
    // timestamps or the wording of a fresh reviewer. Novel bytes are observable
    // evidence only; they are not proof of semantic improvement or acceptance.
    const fingerprint = hash(encodeJson({ root, contract, result: task.resultPrimaryRef?.sha256 ?? task.resultRef?.sha256 ?? task.quality?.resultSha256 ?? null, failure }));
    const failures = team.repairFailures ?? [];
    if (failures.some(row => row.fingerprint === fingerprint)) throw fail('Team repair made no observable progress: the same candidate and failed criteria/checks recurred', 'BLOCKED');
    if (!Number.isSafeInteger(team.round + 1)) throw fail('Team repair round exceeds durable integer capacity', 'STORAGE');
    team.round++; team.repairFailures = [...failures, { root, fingerprint, round: team.round }]; team.revision++; team.state = 'replanning'; team.repair = { root, affected: repairClosure(team.nodes, [root]), reason, phase: 'settling' };
    await saveTeam(this.store, team);
    return this.continueRepair(team);
  }
  async continueRepair(team) {
    const repair = team.repair;
    for (const key of repair.affected) {
      if (key === repair.root) continue;
      const task = this.engine.tasks.get(team.nodes[key].agentId);
      if (task && !this.settled(task)) await this.engine.cancel(task.id);
    }
    for (const key of repair.affected) {
      const task = this.engine.tasks.get(team.nodes[key].agentId);
      if (task && !this.settled(task)) return;
      if (task && (task.quarantined || task.requestPermitUncertain || !automaticRecoverySafe(task) || key === repair.root && !this.safe(task))) throw fail('Uncertain affected execution requires reconciliation', 'BLOCKED');
    }
    if (repair.phase === 'settling') {
      for (const key of repair.affected) {
        const node = team.nodes[key];
        if (key === repair.root) continue;
        team.history.push({ key, kind: node.kind, source: node.source, agentId: node.agentId, generation: node.generation });
        node.generation++; node.agentId = `tm-${hash(`${team.id}:${key}:${node.generation}`).slice(0, 36)}`; node.issued = false;
      }
      repair.phase = 'resume'; await saveTeam(this.store, team);
    }
    if (repair.phase === 'resume') {
      const node = team.nodes[repair.root];
      const task = this.engine.get(node.agentId);
      if (!this.safe(task)) throw fail('Unsafe repair owner', 'BLOCKED');
      await this.dispatch(team, node, 'resume', `Repair only your original scope and unchanged criteria. Retain valid work and correct these verified failures: ${repair.reason}. Prior evidence: ${JSON.stringify(this.priorReports(team, repair.root))}. Do not repeat uncertain effects or weaken checks.`);
      repair.phase = 'done'; team.state = 'active'; delete team.repair; await saveTeam(this.store, team);
    }
  }
  async advanceOwned(id) {
    if (this.closed) return;
    const team = await loadTeam(this.store, identifier(id)); if (!ACTIVE.has(team.state)) return this.status(id);
    try {
      assertTeamGraph(team, this.engine.config);
      if (team.state === 'replanning') { await this.continueRepair(team); return this.status(id); }
      for (const node of Object.values(team.nodes)) {
        const task = this.engine.tasks.get(node.agentId);
        if (!node.issued) {
          const ready = node.prerequisites.every(key => { const prior = this.engine.tasks.get(team.nodes[key].agentId); return this.rawReady(prior); });
          if (!ready) continue;
          // Before integration/destination require complete producer quality,
          // including every accepted independent reviewer, never a chosen subset.
          if (['integrate', 'destination'].includes(node.kind)) {
            const subjects = Object.values(team.nodes).filter(row => ['produce', 'integrate'].includes(row.kind) && row.key !== node.key).map(row => row.agentId);
            for (const subject of subjects) if (this.engine.qualityCheck([subject]).state !== 'ready_for_acceptance') throw fail('Required producer/review evidence is blocked; preserve the unresolved finding', 'BLOCKED');
          }
          await this.dispatch(team, node); continue;
        }
        if (!task || task.quarantined) throw fail('Team member evidence missing or quarantined', 'STORAGE');
        if (!this.settled(task)) continue;
        if (task.qualityFailure && ['produce', 'review', 'integrate'].includes(node.kind)) {
          const criteria = task.quality?.criteria?.map(({ id, status }) => ({ id, status })).sort((a, b) => a.id.localeCompare(b.id)) ?? null;
          await this.beginRepair(team, node.kind === 'review' ? node.source : node.key, JSON.stringify({ agentId: task.id, report: task.currentReport, error: task.error, criteria: task.quality?.criteria }), { kind: node.kind, criteria }); return this.status(id);
        }
        if (node.kind === 'destination' && ['failed', 'idle'].includes(task.state) && (team.spec.maxRepairs === null || team.round < team.spec.maxRepairs) && automaticRecoverySafe(task) && !task.requestPermitUncertain) {
          const failure = await readTeamPlanFailure(team);
          if (failure) {
            const owner = team.nodes.integration ? 'integration' : team.spec.units.length === 1 ? producer(team.spec.units[0].id) : null;
            if (!owner) throw fail('Native failure has no unambiguous scoped repair owner', 'BLOCKED');
            await this.beginRepair(team, owner, `Actual destination check failure; repair only your owned destination, preserve tests and declaration: ${JSON.stringify(failure)}`, { kind: 'destination', checks: failure.failedChecks }); return this.status(id);
          }
        }
        if (task.state !== 'idle') throw fail(`Team member ${node.key} needs attention: ${task.error ?? task.state}`, 'BLOCKED');
        if (node.kind === 'review') await this.reconcileReviews(team, node);
      }
      const current = Object.values(team.nodes).map(node => this.engine.tasks.get(node.agentId));
      if (current.every(task => this.settled(task) && task.state === 'idle')) {
        const subjects = Object.values(team.nodes).filter(node => node.kind !== 'destination').map(node => node.agentId);
        for (const subject of subjects) if (this.engine.qualityCheck([subject]).state !== 'ready_for_acceptance') throw fail('Required team quality/effect evidence is not ready', 'BLOCKED');
        let planReceipt;
        if (team.plan) {
          planReceipt = await (await planRuntime()).readTeamPlanCompletion(team.plan.binding, { workspace: team.spec.directory });
          if (!planReceipt) throw fail('Native PLAN completion evidence missing or stale', 'BLOCKED');
        }
        team.state = 'completed'; team.receipt = { contractHash: team.contractHash, revision: team.revision, members: current.map(task => ({ agentId: task.id, resultEpoch: task.resultEpoch, resultRef: task.resultRef })), ...(planReceipt ? { plan: planReceipt } : {}), acceptance: 'not_evaluated' }; await saveTeam(this.store, team);
      }
    } catch (error) { team.state = 'attention'; team.error = safeError(error); await saveTeam(this.store, team); }
    return this.status(id);
  }
  async cancel(id) {
    return this.advanceAfter(id, async team => {
      team.state = 'cancelled'; await saveTeam(this.store, team);
      for (const node of Object.values(team.nodes)) if (this.engine.tasks.has(node.agentId)) await this.engine.cancel(node.agentId);
    });
  }
  async advanceAfter(id, operation) { const prior = this.lanes.get(id) ?? Promise.resolve(); const result = prior.catch(() => {}).then(async () => { const team = await loadTeam(this.store, id); await operation(team); return this.status(id); }); this.lanes.set(id, result); return result; }
  stop() { this.closed = true; this.unsubscribe?.(); }
  async close() { this.stop(); await Promise.allSettled([...this.lanes.values()]); }
}
