import { normalizeQualityContract, assertQualityAdmission, bindQualityReceipt, qualityCompletion, qualityReviewVerdict } from './quality-contract.mjs';
import { randomUUID } from 'node:crypto';
import { basename, dirname, join, relative, isAbsolute } from 'node:path';
import { realpath, mkdir, access, stat, lstat, open } from 'node:fs/promises';
import { LIMITS, text, identifier, short, safeError, fail, jsonClone, encodeJson, Semaphore, hash } from './limits.mjs';
import { atomicJson, readJson } from './storage.mjs';
import { resolveProfile } from './config.mjs';
import { COMMAND_REGISTRATION_FAILURE_RECEIPT_VERSION, runProcess } from './process.mjs';
import { createTaskPlaybookSnapshot } from './task-playbooks.mjs';
import { CHILD_PROMPT_VERSION } from './child-prompt.mjs';
import { assignTaskName, taskName, validateTaskName } from './task-name.mjs';
import { normalizeDependencies, validateDependencyGraph, dependencyState, dependencyInputs } from './dependencies.mjs';
import { writeResponse, retainResponse, verifyResult } from './responses.mjs';
import { persistSectionPrimary } from './result-segments.mjs';
import { recordExecutionEvidence, executionReadiness, evidenceSummaryArtifact } from './result-evidence.mjs';
import { productVerification } from './result-attestations.mjs';
import { captureProductSnapshot, prepareCodeChangePublication, productSnapshot } from './result-snapshots.mjs';
import { normalizeVerificationChecks, requiredCheckIdentities, verificationAssignment } from './result-checks.mjs';
import { ownedMemoryObservation } from './runtime/memory-observer.mjs';
import { SystemMemoryGuard } from './runtime/memory-policy.mjs';
import { publishResultRevision, resultHead, resultPrimaryRef, advanceResultEpoch, readResultService, resultBundleId } from './results.mjs';
import { ResultInputs, normalizeResultInputs } from './result-inputs.mjs';
import { ResultNotifications, normalizePublishTo } from './result-notifications.mjs';
import { readGrantedResult } from './result-reader.mjs';
import { assertWebCall, validateWebState, enablePublicWeb, WEB_TOOLS, WEB_RECEIPT_INDEX } from './web-policy.mjs';
import { workflowAdapter, refreshWorkflowGrants, assertMutableWorkflowMode, createWorkflowExpiryTimer } from './workflows/index.mjs';
import { WEB_RESULT_BYTES } from './web-tools.mjs';
import { requiredModes, authorizeModes, authorizeMainMutation } from './mode-access.mjs';
import { assertUsageOnlyTransportRetryPolicy, automaticRecoverySafe, createUsageOnlyTransportRetryPolicy, recoveryDecision, usageOnlyTransportRetryDecision, USAGE_ONLY_TRANSPORT_RETRY_POLICY } from './runtime/recovery.mjs';
import { assertProviderFault, assertUsageState, emptyUsageState, overloadCooldownUntil, providerFailureDecision, usageStateFrom } from './runtime/provider-fault.mjs';
import { assertCapsuleBinding, assertHoldReasons, assertResumeCapsule, authorityRevision, clearHoldKinds, continuationHoldReasons, createResumeCapsule, holdReason, inputRevision, mergeHoldReasons, modelProfileFingerprint, providerHold, resumeNextAction, snapshotRevision } from './runtime/resume-capsule.mjs';
import { readReconciliation, recoveryIdentity, reconciliationPath } from './runtime/reconciliation.mjs';
import { Mailbox } from './state/mailbox.mjs';
import { AdmissionController } from './runtime/admission.mjs';
import { continuationDecision, createExecutionBudgetTimer, createRootBudget, rootBudgetDecision, rootProgressMarker, settleRootSegment } from './runtime/policy.mjs';
import { assertCheckpointManifest, emptyCheckpoint } from './state/checkpoint.mjs';
import { processIdentity, executorStopped } from './runtime/ownership.mjs';
import { captureDirectory, materializeDirectory } from './execution/directory-snapshot.mjs';
import { clearPreparedWorkspaceIntentWithAudit, createPreparedWorkspaceIntent, hasPreparedWorkspaceRecoveryAudit, markWorkspaceMaterializationStarted, preparedWorkspaceRecoveryDecision, workspaceInputSha256 } from './runtime/workspace-recovery.mjs';
import { effectRequiresReconciliation, isKnownFailedEffect, replayableEffect, verifyKnownToolFailure } from './execution/failure-receipt.mjs';

import { missionContract, workItemContract, workPredecessors, evidenceContract, admitWork, workPrompt, workIdentity, boundedInputIdentity } from './universal.mjs';
import { evidenceRows, evidenceIndex } from './evidence-index.mjs';
import { routeWork } from './work-router.mjs';
import { fileEffectPlan, effectJournal, assertEffectJournal, assertVacant, createPublishedFile, verifyPublished, observePublished } from './file-effect.mjs';
import { captureGitSnapshot } from './git-snapshot.mjs';
import { captureFileSnapshot } from './file-snapshot.mjs';
const TERMINAL = new Set(['idle', 'paused', 'failed', 'cancelled', 'interrupted', 'blocked']);
const workspaceGitEnv = () => ({ ...Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.startsWith('GIT_'))), LC_ALL: 'C' });
function commitUsage(task, running, usage, outcome = 'completed') {
  if (!usage) return;
  const normalized = usage.state ? assertUsageState(usage) : usageStateFrom(usage, { outcome });
  const observation = outcome === 'unknown' && normalized.state !== 'unknown' ? assertUsageState({ ...normalized, state: 'unknown' }) : normalized;
  const current = task.usage?.state ? assertUsageState(task.usage) : usageStateFrom(task.usage);
  const reported = task.executionIntent ? (task.executionIntent.reportedUsage ??= {}) : {};
  let present = 0;
  for (const key of ['input', 'output', 'cacheRead', 'cacheWrite', 'cost']) {
    const amount = observation[key];
    if (Number.isFinite(amount) && amount >= 0) {
      current[key] = (current[key] ?? 0) + Math.max(0, amount - (reported[key] ?? 0));
      reported[key] = Math.max(reported[key] ?? 0, amount); present++;
    }
  }
  current.state = current.state === 'unknown' || observation.state === 'unknown' ? 'unknown'
    : ['estimated'].includes(current.state) || observation.state === 'estimated' ? 'estimated'
      : observation.state === 'observed' && ['input', 'output', 'cacheRead', 'cacheWrite', 'cost'].every(key => current[key] !== null) ? 'observed'
        : ['input', 'output', 'cacheRead', 'cacheWrite', 'cost'].some(key => current[key] !== null) ? 'partial' : 'missing';
  current.observedAt = Math.max(current.observedAt, observation.observedAt);
  task.usage = assertUsageState(current); task.lastUsageState = observation;
  // The current run is now in the lifetime total; do not expose it twice while settling.
  delete running.progress.usage;
}
function pendingUsage(task, usage) {
  if (!usage || typeof usage !== 'object' || Array.isArray(usage)) return usage;
  const reported = task.executionIntent?.reportedUsage ?? {}, pending = { ...usage };
  for (const key of ['input', 'output', 'cacheRead', 'cacheWrite', 'cost']) {
    if (Number.isFinite(usage[key]) && usage[key] >= 0) pending[key] = Math.max(0, usage[key] - (Number.isFinite(reported[key]) ? reported[key] : 0));
  }
  return pending;
}
function commitTurns(task, turns = 0) {
  const intent = task.executionIntent, prior = intent?.reportedTurns ?? 0;
  if (!Number.isSafeInteger(turns) || turns < 0) throw fail('Invalid turn accounting', 'STORAGE');
  task.turns = (task.turns ?? 0) + Math.max(0, turns - prior);
  if (intent) intent.reportedTurns = Math.max(prior, turns);
}

export { createWorkflowExpiryTimer as createBenchmarkExpiryTimer };
function mailBatch(messages, maxCount = 8) {
  const batch = [];
  for (const message of messages.slice(0, maxCount)) {
    try { encodeJson([...batch, message], 64 * 1024); } catch { break; }
    batch.push(message);
  }
  return batch;
}
export class Engine {
  renewMemoryBoundary(task, reject = false) {
    if (!this.memoryAccountingEnabled) return { admitted: true };
    const decision = this.admission.renewMemory(task, { memoryObservationKnown: this.memoryObservationKnown, systemMemory: this.systemMemory });
    this.lastMemoryAdmission = { agentId: task.id, admitted: decision.admitted, reason: decision.reason, boundaryRenewal: true, sampledAt: Date.now() };
    if (decision.accounting) this.lastMemoryAccounting = decision.accounting;
    if (decision.admitted) delete task.memoryGrowthHold;
    else {
      task.memoryGrowthHold = { reason: decision.reason, since: Date.now() };
      if (reject) { const error = fail(`Memory growth renewal held: ${decision.reason}`, 'MEMORY_STAGE_HOLD'); error.requestOutcome = 'rejected_before_execution'; throw error; }
    }
    return decision;
  }
  updateMemorySample(sample) {
    this.memoryAccountingEnabled = true;
    const observation = ownedMemoryObservation({ sample, workers: this.runner.metrics?.() ?? [], executors: [...this.running.keys()].map(id => this.tasks.get(id)?.executionIntent?.executor).filter(Boolean), ownerNonce: this.admission.ownerNonce, scope: this.systemMemory?.scope ?? `system:${process.platform}` });
    this.memoryObservationKnown = Boolean(observation);
    if (observation) this.admission.observeMemory(observation);
  }
  systemMemoryGuard = new SystemMemoryGuard();
  updateSystemMemory(observation, now = Date.now()) {
    const decision = this.systemMemoryGuard.observe(observation, now);
    this.systemMemory = { ...observation, ...decision }; this.systemPressure = decision.held;
    if (decision.held) {
      void this.runner.retireIdle?.(now, true).catch(() => {});
      if (decision.known && observation.availableBytes < observation.reserveBytes) {
        this.systemLowSince ??= now;
        if (now - this.systemLowSince >= 5000 && !this.systemMemoryVictim) {
          const workers = (this.runner.metrics?.() ?? []).filter(worker => worker.busy && worker.taskId && worker.memorySampleValid).sort((a, b) => (b.memory?.rss ?? 0) - (a.memory?.rss ?? 0));
          this.systemMemoryVictim = workers.find(worker => this.running.has(worker.taskId))?.taskId;
        }
      } else this.systemLowSince = undefined;
    } else { this.systemLowSince = undefined; this.systemMemoryVictim = undefined; this.kick(); }
    return this.systemMemory;
  }
  constructor(config, store, runner, loadConfig, trustedChecks, ownershipIdentity) {
    this.loadConfig = loadConfig; this.trustedChecks = trustedChecks; this.ownershipIdentity = ownershipIdentity;
    this.config = config; this.limits = config.limits; this.store = store; this.runner = runner;
    this.tasks = store.taskCache ?? new Map(); this.running = new Map(); this.commands = runner?.commands ?? new Semaphore(this.limits.commandConcurrency);
    this.resultInputs = new ResultInputs(store, this.tasks);
    this.missions = { get: id => store.getMission(id), has: id => Boolean(store.getMission(id)) };
    this.evidence = { get: id => store.getEvidence(id), has: id => Boolean(store.getEvidence(id)), values: () => store.evidenceRecords() };
    this.effects = new Map(); this.effectObservations = new Map();
    this.main = { id: 'main', mailbox: [], seen: [] }; this.tail = Promise.resolve(); this.pending = 0; this.publicPending = 0; this.stopping = false; this.pressure = false; this.foregroundPressure = false; this.started = Date.now();
    this.mail = new Mailbox(store);
    this.resultNotifications = new ResultNotifications(store, this.tasks, this.mail, (source, target) => {
      for (const task of [source, target]) {
        if (!task || task.cancelRequested || task.authorityRevoked || task.quarantined || task.state === 'archived') throw fail('Result delivery authority is no longer current', 'PERMISSION');
        authorizeModes(task.role, task.profile, this.modes, task.modeBindings ?? []);
        workflowAdapter(task).assertMail();
      }
    });
    this.changeListeners = new Set();
    this.modes = new Map(); // Intentionally not restored from task records or model arguments.
    this.admission = new AdmissionController(this.limits); this.quotaPools = new Map(); this.admissionWake = null;
    this.autoOverloadResumePending = new Set(); this.autoOverloadResumeFailures = new Map();
    this.admissionWaits = new Map(); // Live diagnostics only; never recovery authority or persisted task state.
    this.settlingInputs = new Set(); // Terminal input failure is not settled until its report is durable.
    this.recoveringExecutors = new Set();
    this.reconciling = new Set();
    // A running host never silently adopts newly supplied capture authority.
    this.gitSnapshotGrantHash = config.trustedGitSnapshotGrant ? hash(encodeJson(config.trustedGitSnapshotGrant)) : null;
    this.gitSnapshotGrantChanged = false;
    this.fileSnapshotGrantHash = config.trustedFileSnapshotGrant ? hash(encodeJson(config.trustedFileSnapshotGrant)) : null;
    this.fileSnapshotGrantChanged = false;
    this.commands.trackProcess = (owner, identity) => this.trackCommandProcess(owner, identity, true);
    this.commands.untrackProcess = (owner, identity) => this.trackCommandProcess(owner, identity, false);
  }
  async init() {
    this.config.cwd = await realpath(this.config.cwd);
    // Reconstruct only from durable pointers, not a previous executor's mutable cache.
    this.tasks.clear();
    for (const [list, validate] of [
      [options => this.store.listMissions(options), raw => missionContract(raw)],
      [options => this.store.listEvidence(options), raw => {
        if (!this.missions.has(raw.missionId)) throw fail('Evidence has no mission', 'STORAGE');
        const evidence = evidenceContract(raw.evidence);
        if (evidence.id !== raw.id || !Array.isArray(raw.refIdentities) || raw.refIdentities.length !== evidence.refs.length || raw.refIdentities.some((ref, i) => ref.id !== evidence.refs[i] || !/^[a-f0-9]{64}$/.test(ref.sha256))) throw fail('Invalid stored evidence identity', 'STORAGE');
      }],
    ]) {
      let after = 0;
      do { const page = list({ after, limit: 128 }); for (const row of page.items) validate(row); after = page.nextCursor; } while (after !== null);
    }
    let evidenceAfter = 0;
    do {
      const page = this.store.listEvidence({ after: evidenceAfter, limit: 128 });
      for (const row of page.items) for (const id of row.evidence.contradictions) {
        if (this.evidence.get(id)?.missionId !== row.missionId) throw fail('Stored evidence contradiction crosses mission or is missing', 'STORAGE');
      }
      evidenceAfter = page.nextCursor;
    } while (evidenceAfter !== null);
    const records = this.store.taskRecords ? this.store.taskRecords() : await this.store.loadTasks();
    for (const task of records) {
      identifier(task.id);
      if (task.quarantined) continue;
      if ('version' in task || !task.name || !task.requestHash || !task.role?.name || !task.profile?.provider || !task.checkpoint || !Array.isArray(task.mailbox) || !Array.isArray(task.delivering) || !Array.isArray(task.seen) || !Array.isArray(task.webReceipts) || !task.webUsed) throw fail('Incompatible stored task; explicit recovery required', 'STORAGE');
      if (!['queued', 'running', ...TERMINAL].includes(task.state)) throw fail('Invalid stored task state');
      if (!this.store.taskCache && this.tasks.has(task.id)) throw fail('Duplicate stored agentId');
      if (task.workItem) {
        const item = workItemContract(task.workItem);
        if (this.workItemHasMail(task)) throw fail('Stored WorkItem contains forbidden mailbox state; inspect without acknowledging or delivering it', 'STORAGE');
        for (const ref of item.inputRefs) await boundedInputIdentity(this.store, ref, 'Stored input');
        await this.assertGitSnapshotInputs(item);
        await this.assertFileSnapshotInputs(item);
        if (!this.missions.has(item.missionId) || task.id !== item.id || task.role.name !== item.role || task.role.workspace !== 'readonly' || task.role.tools.some(tool => tool !== 'artifact_read' || !item.capabilities.includes(tool)) || item.capabilities.some(tool => tool !== 'artifact_read') || !Array.isArray(task.ownedResources) || task.ownedResources.join('\0') !== item.resources.join('\0') || !Array.isArray(task.attempts) || 'acceptedChecks' in task || 'appliedReport' in task || task.webUsed.search + task.webUsed.fetch !== 0 || task.webReceipts.length !== 0 || (task.acceptedReport && (task.acceptedUse !== item.intendedUse || task.acceptedReport !== task.currentReport))) throw fail('Incompatible stored work item; unverified effects cannot be recovered', 'STORAGE');
        if (item.intendedUse === 'publish') {
          if (!this.config.trustedEffectGrant) throw fail('Stored publication has no matching trusted host grant', 'STORAGE');
          const plan = await fileEffectPlan(this.config.trustedEffectGrant, item);
          if (task.effectGrantHash !== plan.grantHash) throw fail('Stored publication grant changed; no effect can be recovered', 'STORAGE');
        } else if ('effectGrantHash' in task) throw fail('Unexpected stored effect authority', 'STORAGE');
        if (item.evidenceIds?.length) await this.resolveEvidenceIndex(item);
        if (item.route) this.verifyWorkRoute(item, task.profile);
      }
      if (this.store.metadata.get('steps', this.store.scope, `cancel-${hash(task.id)}`)?.state === 'requested') task.cancelRequested = true;
      for (const step of task.pendingEffects ?? []) if (step.outcome === 'failed') await verifyKnownToolFailure(this.store, task.id, step.failure);
      validateWebState(task); this.tasks.set(task.id, task);
      if (task.qualityContract) {
        if (JSON.stringify(normalizeQualityContract(task.qualityContract)) !== JSON.stringify(task.qualityContract)) throw fail('Invalid stored quality contract', 'STORAGE');
      }
      if (task.holdReasons !== undefined) assertHoldReasons(task.holdReasons);
      if (task.resumeCapsule !== undefined) assertCapsuleBinding(task, task.resumeCapsule);
      if (task.lastResumeCapsule !== undefined) assertResumeCapsule(task.lastResumeCapsule);
      if (task.providerFault !== undefined) assertProviderFault(task.providerFault);
      if (task.usageOnlyTransportRetry !== undefined) assertUsageOnlyTransportRetryPolicy(task.usageOnlyTransportRetry);
    }
    validateDependencyGraph(this.tasks);
    for (const task of records) if (task.workItem) {
      const edges = workPredecessors(task.workItem);
      if (normalizeDependencies(edges.map(edge => edge.id), task.id).join('\0') !== (task.dependsOn ?? []).join('\0') || edges.some(edge => { const parent = this.tasks.get(edge.id); return parent?.workItem?.missionId !== task.workItem.missionId || (edge.use === 'applied' && parent?.workItem?.intendedUse !== 'publish'); })) throw fail('Incompatible stored work predecessor edge', 'STORAGE');
    }
    for (const task of records) if (!task.quarantined && (task.web?.mode !== 'public' || task.webReceipts.some(row => row.state === 'pending' && !row.reservationArtifactId))) throw fail('Incompatible stored web state; inspect before recovery', 'STORAGE');
    for (const row of await this.store.loadEffects()) {
      const task = this.tasks.get(row.id);
      if (!task?.workItem || task.workItem.intendedUse !== 'publish' || this.effects.has(row.id)) throw fail('Orphaned or duplicate effect journal; inspect before recovery', 'STORAGE');
      const plan = await fileEffectPlan(this.config.trustedEffectGrant, task.workItem);
      assertEffectJournal(row, plan, task);
      if (row.state === 'confirmed') await verifyPublished(plan, this.store.limits.responseBytes);
      else this.effectObservations.set(row.id, await observePublished(plan, this.store.limits.responseBytes));
      this.effects.set(row.id, row);
    }
    for (const task of records) if (task.acceptedReport) {
      if (!this.trustedChecks?.check) throw fail('Trusted acceptance checker unavailable on recovery', 'STORAGE');
      const report = await this.store.artifactInfo(task.acceptedReport);
      if (!await this.trustedChecks.check(task, report, { use: task.acceptedUse, reportArtifactId: task.acceptedReport })) throw fail('Stored acceptance no longer verifies', 'STORAGE');
    }
    this.quotaPools = await this.admission.openShared({ agentDir: this.config.agentDir, runId: this.store.scope?.runId ?? hash(this.store.root) });
    for (const task of records) if (!task.quarantined && this.restoreRequestPermitFence(task)) await this.store.saveTask(task);
    for (const task of records) if (!task.quarantined && (task.usageUncertain || task.resumeCapsule)) {
      await this.auditUsageUncertainty(task);
      if (task.resumeCapsule) {
        task.holdReasons = continuationHoldReasons(task.holdReasons ?? task.resumeCapsule.holdReasons);
        task.resumeCapsule = assertResumeCapsule({ ...task.resumeCapsule, holdReasons: continuationHoldReasons(task.resumeCapsule.holdReasons), nextAction: resumeNextAction(task, task.resumeCapsule.providerFault) });
      }
      await this.store.saveTask(task);
    }
    for (const task of records) if (task.reconciliationFence) await this.completeReconciliation(task);
    await this.recoverAdmissionState();
    for (const task of records) if (task.admissionFence && automaticRecoverySafe(task)) {
      const executor = task.executionIntent?.executor;
      const workspaceAudit = !task.executionIntent && task.state === 'queued' && hasPreparedWorkspaceRecoveryAudit(task);
      if (workspaceAudit || await executorStopped(executor)) await this.admission.reconcile(task.admissionFence, { preserveRequestClaims: true });
    }
    for (const task of records) if (task.state === 'running' || task.state === 'queued' || task.recoveryPending) {
      if (this.store.metadata.get('steps', this.store.scope, `cancel-${hash(task.id)}`)?.state === 'requested') task.cancelRequested = true;
      const executor = task.executionIntent?.executor;
      const recordedStopped = await executorStopped(executor);
      const workspaceRecovery = await this.preparedWorkspaceRecovery(task, { recordedExecutorsStopped: !executor || recordedStopped });
      const stopped = recordedStopped || Boolean(!executor && workspaceRecovery.recover);
      if (task.executionIntent && stopped) settleRootSegment(task, { intentId: task.executionIntent.id, startedAt: task.executionIntent.startedAt, progress: rootProgressMarker(task) });
      if (workspaceRecovery.recover) clearPreparedWorkspaceIntentWithAudit(task, workspaceRecovery, this.ownershipIdentity);
      const decision = recoveryDecision(task.recoveryPending ? { ...task, state: 'running' } : task, { executorStopped: stopped });
      task.recoveryPending = Boolean(executor && !stopped);
      if (task.recoveryPending) this.recoveringExecutors.add(task.id);
      task.state = decision.state; task.currentReport = null; task.error = decision.retry ? undefined : decision.reason;
      if (decision.retry) { if (task.executionIntent) task.recoveryAttempts = (task.recoveryAttempts ?? 0) + 1; delete task.executionIntent; if (task.checkpoint?.records?.length) task.continuationRequested = true; }
      const last = task.attempts?.at(-1); if (last?.state === 'running') { last.state = 'interrupted'; last.finishedAt = Date.now(); }
      await this.store.saveTask(task);
      if (workspaceRecovery.recover && decision.retry && task.admissionFence) await this.admission.reconcile(task.admissionFence, { preserveRequestClaims: true });
    }
    this.kick();
  }
  enqueueMutation(fn, isPublic) {
    this.pending++; if (isPublic) this.publicPending++;
    const work = this.tail.catch(() => {}).then(fn).finally(() => { this.pending--; if (isPublic) this.publicPending--; this.publishChange(); });
    this.tail = work.catch(() => {}); return work;
  }
  mutate(fn) {
    if (this.publicPending >= 64) return Promise.reject(fail('Mutation queue full', 'BUSY'));
    return this.enqueueMutation(fn, true);
  }
  mutateInternal(fn) { return this.enqueueMutation(fn, false); }
  subscribeChanges(listener) {
    if (typeof listener !== 'function') throw fail('Change listener must be a function');
    this.changeListeners.add(listener);
    return () => this.changeListeners.delete(listener);
  }
  publishChange() {
    for (const listener of [...this.changeListeners]) {
      try { listener(); } catch { /* Observers cannot break engine settlement. */ }
    }
  }
  get(id) { const task = this.tasks.get(identifier(id)); if (!task) throw fail(`Unknown task ${id}`, 'NOT_FOUND'); return task; }
  qualityHead(task) {
    const head = resultHead(this.store, task);
    return head && { ...head, canonicalResult: hash(encodeJson(resultPrimaryRef(this.store, head) ?? null)) === hash(encodeJson(task.resultRef ?? null)) };
  }
  async qualitySupersede({ id, reviewerId, replacementId }) {
    return this.mutate(async () => {
      const task = this.get(id), old = this.tasks.get(identifier(reviewerId), { includeArchived: true }), next = this.get(replacementId);
      if (!old || old.id === next.id || (task.qualityReviewSupersessions ?? []).some(row => row.reviewerId === replacementId)) throw fail('Invalid or chained review replacement', 'CONFLICT');
      const settled = row => TERMINAL.has(row.state) && !this.running.has(row.id) && !row.recoveryPending && !this.settlingInputs.has(row.id);
      if (!settled(task) || task.state !== 'idle' || !settled(old) || !settled(next)) throw fail('Review replacement requires settled subject and reviewers', 'BUSY');
      if (!old.currentReport || next.createdAt <= old.updatedAt || !next.task.includes(old.currentReport)) throw fail('Replacement assignment must postdate and explicitly reference the prior review report', 'QUALITY');
      authorizeMainMutation(task.role, task.profile, this.modes, task.modeBindings ?? []);
      const context = { settled, head: row => this.qualityHead(row), execution: row => executionReadiness(this.store.metadata.database, this.store.scope, row.id, row.resultEpoch ?? 1), unresolved: row => !automaticRecoverySafe(row) };
      const subject = this.qualityHead(task);
      if (!subject?.canonicalResult || !qualityReviewVerdict(old, task, subject, context) || qualityReviewVerdict(next, task, subject, context) !== 'passed') throw fail('Replacement must independently pass the unchanged current subject and criteria', 'QUALITY');
      if ((task.qualityReviewSupersessions ?? []).length >= 128) throw fail('Review replacement history limit reached', 'LIMIT');
      const receipt = { reviewerId: old.id, replacementId: next.id, replacementEpoch: next.resultEpoch ?? 1, replacementResultSha256: next.quality.resultSha256, sourceResultEpoch: task.resultEpoch ?? 1, sourceRevision: subject.revision, recordedAt: Date.now() };
      task.qualityReviewSupersessions = [...(task.qualityReviewSupersessions ?? []), receipt];
      await this.persist(task); return { agentId: id, receipt, acceptance: 'not_evaluated' };
    });
  }
  qualityCheck(ids) {
    if (!Array.isArray(ids) || ids.length < 1 || ids.length > 8 || new Set(ids).size !== ids.length) throw fail('quality_check requires 1..8 unique agent IDs');
    return qualityCompletion(ids.map(id => this.get(id)), () => this.tasks.values({ includeArchived: true }), {
      settled: task => TERMINAL.has(task.state) && !this.running.has(task.id) && !task.recoveryPending && !this.settlingInputs.has(task.id),
      head: task => this.qualityHead(task),
      lookup: id => this.tasks.get(id, { includeArchived: true }),
      execution: task => executionReadiness(this.store.metadata.database, this.store.scope, task.id, task.resultEpoch ?? 1),
      unresolved: task => !automaticRecoverySafe(task),
    });
  }
  workItemHasMail(task) {
    return Boolean(task.workItem && ((task.mailbox?.length ?? 0) || (task.delivering?.length ?? 0) || (task.seen?.length ?? 0) || (task.checkpoint?.mailIds?.length ?? 0) || this.mail.list(task.id, { state: 'pending', limit: 1 }).length || this.mail.list(task.id, { state: 'delivering', limit: 1 }).length));
  }
  summary(task) {
    const dependency = dependencyState(task, this.tasks, this.running, this.effects);
    const recentMessages = (task.responses ?? []).filter(row => row.type === 'message' && row.direction !== 'inbound' && (['question', 'blocked'].includes(row.kind) || row.delivery === 'steer')).slice(-8).map(({ artifactId, messageId, kind, delivery, createdAt }) => ({ artifactId, messageId, kind, ...(delivery ? { delivery } : {}), createdAt }));
    return { id: task.id, name: taskName(task), dependsOn: task.dependsOn ?? [], dependencyPolicy: task.dependencyPolicy ?? 'all_succeeded', dependencyHandoff: task.dependencyHandoff ?? 'summary',
      ...(task.web ? { webMode: 'public', webUsed: jsonClone(task.webUsed), webReceiptCount: task.webReceipts.length, webReceiptsOmitted: Math.max(0, task.webUsed.search + task.webUsed.fetch - task.webReceipts.length) } : {}),
      dependencyState: task.state === 'blocked' ? 'blocked' : task.state === 'queued' ? dependency.state : task.dependencyInputs ? 'ready' : (task.dependsOn?.length ? 'unknown' : 'ready'),
      waitingFor: task.state === 'queued' ? dependency.waitingFor : [], blockedBy: task.blockedBy ?? [], role: task.role.name, profile: task.profile.name, provider: task.profile.provider, model: task.profile.model,
      ...(task.state === 'queued' && this.admissionWaits.has(task.id) ? { admission: { ...this.admissionWaits.get(task.id) } } : {}),
      ...(task.workItem ? { missionId: task.workItem.missionId, workItemId: task.workItem.id, candidateReport: task.currentReport, acceptedReport: task.acceptedReport ?? null, acceptedUse: task.acceptedUse ?? null, appliedReport: this.effects.get(task.id)?.state === 'confirmed' ? this.effects.get(task.id).reportArtifactId : null, effect: this.effects.get(task.id)?.state === 'confirmed' ? 'applied' : this.effects.has(task.id) ? 'unknown' : 'not_applied', ...(this.effectObservations.has(task.id) ? { effectObservation: this.effectObservations.get(task.id) } : {}), attempts: task.attempts?.map(({ id, state, reportArtifactId, execution, provider, model }) => ({ id, state, reportArtifactId, execution, ...(provider ? { provider, model } : {}) })) } : {}),
      ...(task.qualityContract ? { quality: task.quality ?? { state: 'not_evaluated', acceptance: 'not_evaluated' } } : {}),
      state: task.state, settled: TERMINAL.has(task.state) && !this.running.has(task.id) && !task.recoveryPending && !this.settlingInputs.has(task.id), task: short(task.task, 256), cwd: task.cwd, result: short(task.result ?? '', 2048), error: task.error === undefined ? undefined : short(task.error, 2048),
      ...(task.playbook ? { playbook: { domain: task.playbook.route.primary, phase: task.playbook.route.phase, role: task.playbook.route.role ?? null, sha256: task.playbook.sha256, loaded: [...task.playbook.loaded] } } : {}),
      resultAvailable: Boolean(task.result), errorAvailable: Boolean(task.error), handoffAvailable: Boolean(task.handoff),
      resultRef: task.resultRef, preview: { text: short(task.result ?? '', 2048), truncated: Boolean(task.resultRef && task.resultRef.bytes > Buffer.byteLength(short(task.result ?? '', 2048))) },
      ...(task.resultRouteDeclarations?.length ? { resultRoutes: this.resultNotifications.rows(task.id).map(({ id, targetId, state, reason, deliveredRevision }) => ({ id, targetId, state, reason, deliveredRevision })) } : {}),
      ...(task.executionReadiness || task.requiredResultChecks?.length ? { currentExecution: { resultEpoch: task.resultEpoch ?? 1, ...executionReadiness(this.store.metadata.database, this.store.scope, task.id, task.resultEpoch ?? 1), ...(task.executionReadiness?.evidenceIndexRef ? { evidenceIndexRef: task.executionReadiness.evidenceIndexRef } : {}) } } : {}),
      ...(task.resultContract === 'code-change' ? { currentProductVerification: productVerification(this.store.metadata.database, this.store.scope, task.id, task.resultEpoch ?? 1, resultHead(this.store, task)?.candidateSnapshot?.productSha256) } : {}),
      ...(!automaticRecoverySafe(task) ? { recovery: { root: this.store.root, expectedIdentity: recoveryIdentity(task), confirmationPath: reconciliationPath(this.config.agentDir, this.store.root, task.id), effects: (task.pendingEffects ?? []).filter(effectRequiresReconciliation).map(({ stepId, toolCallId, tool, inputDigest, outcome, resultRef }) => ({ stepId, toolCallId, tool, inputDigest, outcome, resultRef })), usageUncertain: Boolean(task.usageUncertain), requestPermitUncertain: Boolean(task.requestPermitUncertain) } } : {}),
      ...(task.usageUncertain || task.usage?.state === 'unknown' || task.usageOnlyTransportRetry?.interruptions?.length ? { usageRisk: { state: 'unknown', acceptedMismatch: true, retryState: task.usageOnlyTransportRetry?.state, receipts: jsonClone(task.usageOnlyTransportRetry?.interruptions ?? []), ...(task.usageUncertaintyAudit ? { auditRef: jsonClone(task.usageUncertaintyAudit) } : {}) } } : {}),
      turns: task.turns ?? 0, maxTurns: task.rootBudget ? task.rootBudget.maxTurns : Math.min(task.role.maxTurns ?? this.limits.maxTurns, this.limits.maxTurns), usage: task.usage, rootBudget: task.rootBudget && { maxTurns: task.rootBudget.maxTurns, maxExecutionMs: task.rootBudget.maxExecutionMs, maxStagnantSegments: task.rootBudget.maxStagnantSegments, executionMs: task.rootBudget.executionMs, segments: task.rootBudget.segments, stagnantSegments: task.rootBudget.stagnantSegments }, updatedAt: task.updatedAt, mailbox: task.mailbox.length, transcript: task.transcript,
      ...(task.state === 'paused' && task.resumeCapsule ? { holdReasons: continuationHoldReasons(task.holdReasons ?? []), resume: { resultEpoch: task.resumeCapsule.identity.resultEpoch, goalRevision: task.resumeCapsule.identity.goalRevision, executionSegment: task.resumeCapsule.identity.executionSegment, nextEligibleAt: task.resumeCapsule.nextEligibleAt, nextAction: resumeNextAction(task, task.resumeCapsule.providerFault), cancellationIntent: task.resumeCapsule.cancellationIntent, usageState: jsonClone(task.resumeCapsule.usageState), providerFault: task.resumeCapsule.providerFault ? jsonClone(task.resumeCapsule.providerFault) : undefined } } : {}),
      latestReport: task.latestReport, currentReport: task.currentReport ?? null, bundleRef: task.bundleRef, dataReadiness: task.dataReadiness, responseCount: task.responseCount ?? 0, recentMessages, autoWakes: task.autoWakes, progress: this.running.get(task.id)?.progress };
  }
  status({ after = 0, missionAfter = 0, evidenceAfter = 0 } = {}) {
    let tasks, nextCursor = null;
    if (this.store.metadata) { const page = this.store.metadata.list('work_items', this.store.scope, { after, limit: 128 }); tasks = page.items.filter(row => row.state !== 'archived').map(row => this.summary(this.get(row.id))); nextCursor = page.nextCursor; }
    else { tasks = []; for (const task of this.tasks.values()) { if (tasks.length >= 128) break; tasks.push(this.summary(task)); } }
    const missions = this.store.listMissions({ after: missionAfter, limit: 128 }), evidence = this.store.listEvidence({ after: evidenceAfter, limit: 128 });
    return { pid: process.pid, uptimeMs: Date.now() - this.started, active: this.running.size, concurrency: this.limits.concurrency, pressure: Boolean(this.pressure || this.foregroundPressure || this.systemPressure), memory: process.memoryUsage(), treeMemory: this.resourceObservation(), systemMemory: this.systemMemory, memoryAdmission: this.lastMemoryAdmission, memoryAccounting: this.lastMemoryAccounting, workers: this.runner.metrics?.(), admissionReservations: this.admission.reservations.size, artifactBytes: this.store.bytes, missionIds: missions.items.map(row => row.id), evidenceIds: evidence.items.map(row => row.id), missionNextCursor: missions.nextCursor, evidenceNextCursor: evidence.nextCursor, tasks, nextCursor };
  }
  async releaseConnectionHold() {
    return this.mutateInternal(async () => {
      this.connectionHeld = false;
      for (const task of this.tasks.values()) if (task.heldForConnection && !task.cancelRequested && !task.authorityRevoked) { task.heldForConnection = false; task.state = 'queued'; delete task.executionIntent; await this.persist(task); }
      this.kick();
    });
  }
  async holdForeground() {
    await this.mutateInternal(async () => { this.connectionHeld = true; });
    await this.runner.park?.();
    return { held: true };
  }
  setForegroundPressure(paused) {
    if (typeof paused !== 'boolean') throw fail('Foreground pressure must be boolean');
    this.foregroundPressure = paused; if (!paused) this.kick();
    return { paused };
  }
  updateResources(observation) {
    if (!observation || typeof observation !== 'object' || Array.isArray(observation) || Object.keys(observation).some(key => !['supported', 'rssBytes', 'sampledAt'].includes(key))) throw fail('Invalid host resource observation');
    if (typeof observation.supported !== 'boolean' || !Number.isSafeInteger(observation.rssBytes) || observation.rssBytes < 0 || !Number.isSafeInteger(observation.sampledAt) || observation.sampledAt < 0 || observation.sampledAt > Date.now() + 1000) throw fail('Invalid host resource observation');
    if (!this.hostResources || observation.sampledAt >= this.hostResources.sampledAt) this.hostResources = { ...observation };
    const current = this.resourceObservation(); this.pressure = this.limits.memorySoftBytes != null && current.supported && current.rssBytes >= this.limits.memorySoftBytes;
    if (!this.pressure) this.kick();
    return current;
  }
  resourceObservation() {
    if (this.hostResources?.supported && Date.now() - this.hostResources.sampledAt <= 10_000) return { ...this.hostResources, source: 'host_process_tree' };
    const supervisor = process.memoryUsage();
    const workerRssBytes = (this.runner.metrics?.() ?? []).reduce((sum, worker) => sum + (Number.isFinite(worker.memory?.rss) ? worker.memory.rss : 0), 0);
    return { supported: true, rssBytes: supervisor.rss + workerRssBytes, sampledAt: Date.now(), supervisorRssBytes: supervisor.rss, workerRssBytes, source: 'runtime_fallback' };
  }
  reservationId(taskId) { return `admission-${hash(`${this.store.scope?.runId ?? ''}:${taskId}`).slice(0, 60)}`; }
  quotaPoolId(account) { return `quota-${hash(`${this.store.scope?.runId ?? ''}:${account}`).slice(0, 64)}`; }
  async recoverAdmissionState() {
    if (!this.store.metadata) return;
    let after = 0;
    do {
      const page = this.store.metadata.list('usage_reservations', this.store.scope, { after, limit: 128 });
      for (const row of page.items) {
        if (row.kind === 'quota_pool' && (row.state === 'held' || row.state === 'backoff' && row.retryAt > Date.now())) {
          const fault = row.fault ? assertProviderFault(row.fault) : undefined;
          // The shared ledger is authoritative for overload: a run-local historical
          // hold must not revive an expired shared cooldown after restart.
          const shared = this.admission.quotaPools().get(row.account);
          if (shared) this.quotaPools.set(row.account, shared);
          else if (fault?.kind !== 'provider_overload') this.quotaPools.set(row.account, { held: row.state === 'held', retryAt: row.retryAt, reason: row.reason, failures: row.failures ?? 0, ...(fault ? { fault } : {}) });
        } else if (row.kind === 'execution' && row.state === 'reserved') {
          this.store.metadata.put('usage_reservations', this.store.scope, { ...row, state: 'orphaned', releasedAt: Date.now(), releaseReason: 'supervisor_recovery' });
        }
      }
      if (page.nextCursor === null) break; after = page.nextCursor;
    } while (true);
  }
  persistAdmissionReservation(task, reservation) {
    task.admissionFence = reservation.fence;
    if (!this.store.metadata) return;
    this.store.metadata.put('usage_reservations', this.store.scope, { id: this.reservationId(task.id), state: 'reserved', kind: 'execution', agentId: task.id, executionIntentId: task.executionIntent.id, ...reservation });
  }
  async releaseAdmissionReservation(task, reason = 'settled') {
    if (task.recoveryPending) return;
    this.admission.markUncertain(task.id, !automaticRecoverySafe(task));
    await this.admission.releaseShared(task.id);
    if (!this.store.metadata) return;
    const id = this.reservationId(task.id), row = this.store.metadata.get('usage_reservations', this.store.scope, id);
    if (row?.state === 'reserved') this.store.metadata.put('usage_reservations', this.store.scope, { ...row, state: automaticRecoverySafe(task) ? 'released' : 'needs_attention', releasedAt: Date.now(), releaseReason: reason });
  }
  persistQuotaPool(account, pool, state) {
    try {
      this.admission.setQuotaPool(account, state === 'released' ? {} : pool);
      if (!this.store.metadata) return;
      const id = this.quotaPoolId(account), prior = this.store.metadata.get('usage_reservations', this.store.scope, id);
      this.store.metadata.put('usage_reservations', this.store.scope, { ...prior, id, state, kind: 'quota_pool', account, held: Boolean(pool?.held), retryAt: pool?.retryAt, reason: pool?.reason, failures: pool?.failures ?? prior?.failures ?? 0, ...(pool?.fault ? { fault: assertProviderFault(pool.fault) } : {}), ...(state === 'released' ? { releasedAt: Date.now() } : {}) });
    } catch (error) {
      this.fatal ??= `Quota pool persistence failed: ${safeError(error)}`; this.stopping = true;
      for (const running of this.running.values()) running.abort.abort(error);
      throw fail(this.fatal, 'STORAGE');
    }
  }
  scheduleAdmissionWake(at) {
    if (!Number.isSafeInteger(at) || at <= Date.now()) { this.kick(); return; }
    if (this.admissionWake?.at <= at) return;
    if (this.admissionWake) clearTimeout(this.admissionWake.timer);
    const timer = setTimeout(() => { this.admissionWake = null; this.kick(); }, Math.max(1, at - Date.now())); timer.unref?.();
    this.admissionWake = { at, timer };
  }
  async releaseQuotaHold({ account } = {}) {
    text(account, 'account', 120);
    return this.mutate(async () => {
      const pool = this.quotaPools.get(account) ?? this.admission.quotaPools().get(account);
      if (!pool?.held) return { account, released: false };
      this.quotaPools.delete(account); this.persistQuotaPool(account, pool, 'released');
      const awaitingExplicitResume = [...this.tasks.values()].filter(task => task.quotaHold?.account === account && task.state === 'paused').map(task => task.id);
      // Releasing a shared provider fence never schedules, probes, or resumes a task.
      return { account, released: true, awaitingExplicitResume };
    });
  }
  async persist(task) {
    task.updatedAt = Date.now();
    try { await this.store.saveTask(task); }
    catch (e) {
      this.fatal = `Task persistence failed: ${safeError(e)}`; this.stopping = true;
      for (const r of this.running.values()) r.abort.abort(e);
      throw fail(this.fatal, 'STORAGE');
    }
  }
  restoreRequestPermitFence(task) {
    const claim = this.admission.unsettledPermit(task);
    if (claim && !task.requestPermitUncertain) task.requestPermitUncertain = claim;
    return claim;
  }
  assertResultCurrent(task) {
    this.running.get(task.id)?.abort.signal.throwIfAborted();
    if (task.cancelRequested || task.authorityRevoked || this.store.metadata.get('steps', this.store.scope, `cancel-${hash(task.id)}`)?.state === 'requested') throw fail('Result publication cancelled or authority revoked', 'CANCELLED');
  }
  async auditUsageUncertainty(task) {
    if (!task.usageUncertain) return;
    task.usage = assertUsageState({ ...(task.usage?.state ? task.usage : usageStateFrom(task.usage)), state: 'unknown' });
    task.lastUsageState = assertUsageState({ ...(task.lastUsageState ?? emptyUsageState()), state: 'unknown' });
    const receipt = task.lastRequestReceipt;
    const requestId = receipt?.id ?? task.executionIntent?.activeRequestId ?? task.executionIntent?.id ?? `legacy-${task.resultEpoch ?? 1}-${task.executionSegment ?? 0}`;
    const id = `usage-unknown-${hash(`${task.id}\0${requestId}`)}`;
    let row = this.store.metadata.get('steps', this.store.scope, id);
    if (!row) {
      const artifact = await this.store.artifact(task.id, 'provider-usage-uncertainty');
      try {
        await artifact.write(encodeJson({ agentId: task.id, requestId, outcome: 'unknown', acceptedMismatch: true, observedAt: receipt?.observedAt ?? task.lastUsageState.observedAt, usage: task.lastUsageState, lifetimeUsage: task.usage }, 4096));
        const sealed = await artifact.finish(true);
        row = { id, agentId: task.id, kind: 'provider_usage_uncertainty', state: 'unknown', requestId, resultRef: { artifactId: sealed.id, bytes: sealed.bytes, sha256: sealed.sha256, complete: true } };
        this.store.metadata.put('steps', this.store.scope, row, { ifAbsent: true });
      } catch (error) { if (!artifact.closed) await artifact.finish(false, safeError(error)).catch(() => {}); throw error; }
    }
    task.usageUncertaintyAudit = row.resultRef;
  }
  async refreshConfiguration() {
    // Called only under mutate(). Validate fully before publishing; keep host-lifetime
    // runtime/limits/auth/provider state and every accepted task's snapshot intact.
    if (!this.loadConfig) return;
    const next = await this.loadConfig();
    this.config.profiles = next.profiles; this.config.roles = next.roles; this.config.routingProfiles = next.routingProfiles;
    this.config.sources = next.sources;
    this.config.trustedEffectGrant = next.trustedEffectGrant;
    const nextGitHash = next.trustedGitSnapshotGrant ? hash(encodeJson(next.trustedGitSnapshotGrant)) : null;
    if (nextGitHash !== this.gitSnapshotGrantHash) this.gitSnapshotGrantChanged = true;
    this.config.trustedGitSnapshotGrant = next.trustedGitSnapshotGrant;
    const nextFileHash = next.trustedFileSnapshotGrant ? hash(encodeJson(next.trustedFileSnapshotGrant)) : null;
    if (nextFileHash !== this.fileSnapshotGrantHash) this.fileSnapshotGrantChanged = true;
    this.config.trustedFileSnapshotGrant = next.trustedFileSnapshotGrant;
  }
  async reloadRuntimeConfiguration() {
    if (!this.loadConfig) return { changed: false, reason: 'configuration loader unavailable' };
    return this.mutate(async () => {
      if (this.stopping) throw fail('Host stopping', 'CLOSED');
      const next = await this.loadConfig(), deferredLimits = {};
      const applied = { ...next, limits: { ...next.limits } };
      for (const key of ['artifactBytes', 'diskBytes', 'minFreeBytes']) {
        applied.limits[key] = this.limits[key];
        if (next.limits[key] !== this.limits[key]) deferredLimits[key] = { configured: next.limits[key], active: this.limits[key] };
      }
      const replacement = await this.runner.reconfigure?.(applied) ?? { changed: false };
      this.config.profiles = applied.profiles; this.config.roles = applied.roles; this.config.routingProfiles = applied.routingProfiles; this.config.sources = applied.sources;
      this.config.trustedEffectGrant = applied.trustedEffectGrant;
      const nextGitHash = applied.trustedGitSnapshotGrant ? hash(encodeJson(applied.trustedGitSnapshotGrant)) : null;
      if (nextGitHash !== this.gitSnapshotGrantHash) this.gitSnapshotGrantChanged = true;
      this.config.trustedGitSnapshotGrant = applied.trustedGitSnapshotGrant;
      const nextFileHash = applied.trustedFileSnapshotGrant ? hash(encodeJson(applied.trustedFileSnapshotGrant)) : null;
      if (nextFileHash !== this.fileSnapshotGrantHash) this.fileSnapshotGrantChanged = true;
      this.config.trustedFileSnapshotGrant = applied.trustedFileSnapshotGrant;
      for (const key of ['providerModules', 'modelsPath', 'authPath']) this.config[key] = applied[key];
      this.config.limits = applied.limits; this.limits = applied.limits; this.admission.updateLimits(applied.limits); this.commands.resize?.(applied.limits.commandConcurrency);
      this.kick(); return { changed: true, workerReplacement: replacement, deferredLimits };
    });
  }
  async refreshBenchmarkMode() {
    return refreshWorkflowGrants(this.config, this.store, this.modes);
  }
  async setMode({ name, enabled, root }) {
    identifier(name, 'mode');
    if (typeof enabled !== 'boolean') throw fail('Mode enabled must be boolean');
    assertMutableWorkflowMode(name);
    return this.mutate(async () => {
      if (this.stopping) throw fail('Host stopping', 'CLOSED');
      let canonical = null;
      if (enabled && root != null) {
        canonical = await realpath(text(root, 'mode workspace', 4096));
        if (!(await stat(canonical)).isDirectory()) throw fail('Mode workspace must be a directory', 'MODE');
      }
      if (enabled) this.modes.set(name, { root: canonical }); else this.modes.delete(name);
      const revoked = [];
      for (const task of this.tasks.values()) {
        if (!requiredModes(task.role, task.profile).includes(name)) continue;
        try { authorizeModes(task.role, task.profile, this.modes, task.modeBindings ?? []); }
        catch (error) {
          if (task.state === 'queued' || this.running.has(task.id) || (task.state === 'paused' && task.resumeCapsule)) {
            revoked.push(task.id); task.authorityRevoked = { mode: name, at: Date.now() };
            if (['pending', 'active'].includes(task.usageOnlyTransportRetry?.state)) task.usageOnlyTransportRetry.state = 'exhausted';
            this.running.get(task.id)?.abort.abort(fail('Mode authority revoked', 'AUTHORITY_REVOKED'));
            if (!this.running.has(task.id)) {
              task.state = 'cancelled'; task.currentReport = null; task.error = 'Mode authority revocation is durable; create newly authorized work rather than reviving this execution.';
              if (task.resumeCapsule) task.resumeCapsule = assertResumeCapsule({ ...task.resumeCapsule, cancellationIntent: true });
            }
            await this.persist(task);
          }
        }
      }
      this.kick(); return { name, enabled, root: canonical, cancellationRequested: revoked };
    });
  }
  async createMission(input) {
    const mission = missionContract(input);
    return this.mutate(async () => {
      if (this.stopping) throw fail('Host stopping', 'CLOSED');
      const prior = this.missions.get(mission.id);
      if (prior) { if (workIdentity(prior) !== workIdentity(mission)) throw fail('Mission ID reused', 'CONFLICT'); return jsonClone(prior); }
      await this.store.saveMission(mission); return jsonClone(mission);
    });
  }
  snapshotPath(id, fileInput) { return join(this.store.root, `${fileInput ? 'file' : 'git'}-snapshot-${identifier(id)}.json`); }
  async snapshotReceipt(id, fileInput) {
    try { return await readJson(this.snapshotPath(id, fileInput), 8192); }
    catch (error) { if (error.code === 'ENOENT') return null; throw error; }
  }
  assertSnapshotGrantConflict(id) {
    if (this.config.trustedGitSnapshotGrant?.workItemId === id && this.config.trustedFileSnapshotGrant?.workItemId === id) throw fail('Git and file input grants overlap on one work ID', 'CAPABILITY');
  }
  async assertGitSnapshotInputs(item) { return this.assertSnapshotInputs(item, false); }
  async assertFileSnapshotInputs(item) { return this.assertSnapshotInputs(item, true); }
  async assertSnapshotInputs(item, fileInput) {
    if (fileInput) this.assertSnapshotGrantConflict(item.id);
    const title = fileInput ? 'File' : 'Git';
    const kind = fileInput ? 'file-input-snapshot' : 'git-input-snapshot';
    const grant = fileInput ? this.config.trustedFileSnapshotGrant : this.config.trustedGitSnapshotGrant;
    const receipt = await this.snapshotReceipt(item.id, fileInput);
    if (grant?.workItemId === item.id && (grant.missionId !== item.missionId || !receipt)) throw fail(`${title} snapshot capture for this exact mission/work must precede submission`, 'CAPABILITY');
    // A copied artifact without this run's matching receipt cannot grant reads.
    for (const ref of item.inputRefs) {
      const info = await this.store.artifactInfo(ref.artifactId);
      if (info.label === kind && (!receipt?.ref || receipt.ref.artifactId !== ref.artifactId)) throw fail(`${title} snapshot input lacks this work item capture receipt`, 'CAPABILITY');
    }
    if (!receipt) return;
    if (item.intendedUse !== 'inspect' || fileInput && (item.capabilities.length !== 1 || item.capabilities[0] !== 'artifact_read' || item.inputRefs.length !== 1)) throw fail(`${title} snapshot requires inspect-only pinned input`, 'CAPABILITY');
    if ((fileInput ? this.fileSnapshotGrantChanged : this.gitSnapshotGrantChanged) || receipt.kind !== kind || receipt.runRoot !== this.store.root || receipt.missionId !== item.missionId || receipt.workItemId !== item.id || !grant || grant.missionId !== item.missionId || grant.workItemId !== item.id || receipt.grantHash !== hash(encodeJson(grant))) throw fail(`${title} snapshot receipt authority changed or belongs to another run`, 'CAPABILITY');
    const fields = ['artifactId', 'owner', 'bytes', 'sha256'];
    if (!receipt.ref || Object.keys(receipt.ref).sort().join() !== fields.sort().join() || !item.inputRefs.some(ref => Object.keys(ref).length === 4 && fields.every(key => ref[key] === receipt.ref[key]))) throw fail(`Work item must explicitly pin exact ${title} snapshot inputRef`, 'CAPABILITY');
    const info = await boundedInputIdentity(this.store, receipt.ref, `${title} snapshot input`);
    if (info.owner !== item.id || (await this.store.artifactInfo(info.id)).label !== kind) throw fail(`${title} snapshot artifact identity changed`, 'STORAGE');
  }
  async captureFileSnapshot(input = {}) {
    if (!input || Object.getPrototypeOf(input) !== Object.prototype || Object.keys(input).sort().join() !== 'missionId,workItemId') throw fail('File snapshot capture accepts only missionId/workItemId', 'CAPABILITY');
    return this.captureInputSnapshot(input.missionId, input.workItemId, true);
  }
  async captureGitSnapshot({ missionId, workItemId } = {}) { return this.captureInputSnapshot(missionId, workItemId, false); }
  async captureInputSnapshot(missionId, workItemId, fileInput) {
    identifier(missionId, 'missionId'); identifier(workItemId, 'workItemId');
    return this.mutate(async () => {
      if (this.stopping) throw fail('Host stopping', 'CLOSED');
      await this.refreshConfiguration();
      this.assertSnapshotGrantConflict(workItemId);
      const title = fileInput ? 'file' : 'Git';
      const kind = fileInput ? 'file-input-snapshot' : 'git-input-snapshot';
      const grant = fileInput ? this.config.trustedFileSnapshotGrant : this.config.trustedGitSnapshotGrant;
      const grantHash = fileInput ? this.fileSnapshotGrantHash : this.gitSnapshotGrantHash;
      const changed = fileInput ? this.fileSnapshotGrantChanged : this.gitSnapshotGrantChanged;
      const path = this.snapshotPath(workItemId, fileInput);
      if (!grant || changed || hash(encodeJson(grant)) !== grantHash) throw fail(`Trusted ${title} snapshot grant absent or changed since host start`, 'CAPABILITY');
      if (!this.missions.has(missionId)) throw fail('Unknown mission', 'NOT_FOUND');
      if (grant.missionId !== missionId || grant.workItemId !== workItemId) throw fail(`${title} snapshot mission/work identity mismatch`, 'CAPABILITY');
      // The only source root is the canonical host workspace, never an action-supplied path.
      const root = fileInput ? grant.cwd : grant.repoRoot;
      if (root !== this.config.cwd || await realpath(this.config.cwd) !== root) throw fail(`${title} snapshot root is outside the authorized host workspace`, 'PERMISSION');
      if (this.tasks.has(workItemId)) throw fail('Work item already exists', 'CONFLICT');
      if (this.store.metadata?.get('work_items', this.store.scope, workItemId)) throw fail('Work item ID already used', 'CONFLICT');
      for (const folder of ['tasks', 'archive']) {
        try { await access(join(this.store.root, folder, `${workItemId}.json`)); throw fail('Work item ID already used', 'CONFLICT'); }
        catch (error) { if (error.code !== 'ENOENT') throw error; }
      }
      // Separate namespaces; an exclusive, fsynced intent survives every uncertain failure.
      let intent;
      try { intent = await open(path, 'wx', 0o600); }
      catch (error) { if (error.code === 'EEXIST') throw fail(`${title} snapshot capture already attempted; inspect the prior outcome`, 'CONFLICT'); throw error; }
      try { await intent.writeFile(encodeJson({ kind, missionId, workItemId, state: 'started' }, 8192)); await intent.sync(); }
      finally { await intent.close(); }
      const dir = await open(this.store.root, 'r');
      try { await dir.sync(); } catch (error) { if (!['EINVAL', 'ENOTSUP'].includes(error.code)) throw error; } finally { await dir.close(); }
      const result = await (fileInput ? captureFileSnapshot : captureGitSnapshot)(grant, this.store);
      const ref = { artifactId: result.artifactId, owner: result.owner, bytes: result.bytes, sha256: result.sha256 };
      await atomicJson(path, { kind, runRoot: this.store.root, missionId, workItemId, grantHash, ref }, 8192);
      return { inputRef: ref, ...result };
    });
  }
  async resolveEvidenceIndex(item) {
    const rows = evidenceRows(item, this.evidence);
    const index = evidenceIndex(item, rows); // Explicit inputRefs authorize every raw source; IDs alone do not.
    const checked = new Set();
    for (const row of rows) for (const ref of row.refIdentities) {
      if (checked.has(ref.id)) continue;
      await boundedInputIdentity(this.store, { artifactId: ref.id, owner: ref.owner, bytes: ref.bytes, sha256: ref.sha256 }, 'Evidence source');
      checked.add(ref.id);
    }
    return index;
  }
  verifyWorkRoute(item, selectedProfile, modeBindings) {
    const decision = routeWork(item, this.config);
    // The complete selected profile is an authority snapshot, not merely a
    // model label. In particular requiredMode and file protocol must not drift.
    const fields = ['name', 'provider', 'model', 'thinkingLevel', 'account', 'maxConcurrent', 'modelConcurrent', 'memoryBytes', 'toolProcesses', 'allowBackground', 'fileToolProtocol', 'requiredMode', 'instructions'];
    if (decision.state !== 'selected' || !selectedProfile || Object.keys(selectedProfile).some(field => !fields.includes(field)) || fields.some(field => decision.profile[field] !== selectedProfile[field])) throw fail(`Trusted ${item.route} route, profile or mode authority changed; inspect and submit new authorized work`, 'MODEL');
    // Recovery can precede mode activation. At actual start, check the freshly
    // configured role+profile against the task's original bound workspace.
    if (modeBindings !== undefined) authorizeModes(this.config.roles[item.role], decision.profile, this.modes, modeBindings);
  }
  async recordEvidence(missionId, input) {
    identifier(missionId); const evidence = evidenceContract(input);
    return this.mutate(async () => {
      if (!this.missions.has(missionId)) throw fail('Unknown mission', 'NOT_FOUND');
      const prior = this.evidence.get(evidence.id);
      if (prior) {
        if (prior.missionId !== missionId || workIdentity(prior.evidence) !== workIdentity(evidence)) throw fail('Evidence identity reused', 'CONFLICT');
        for (const ref of prior.refIdentities) { const current = await this.store.artifactIdentity(ref.id); if (current.owner !== ref.owner || current.bytes !== ref.bytes || current.sha256 !== ref.sha256) throw fail('Existing evidence source identity changed', 'STORAGE'); }
        return jsonClone(prior);
      }
      if (this.store.evidenceCount() >= this.limits.maxTasks * 8) throw fail('Evidence index full', 'LIMIT');
      const refIdentities = await Promise.all(evidence.refs.map(ref => this.store.artifactIdentity(ref)));
      for (const contradiction of evidence.contradictions) if (this.evidence.get(contradiction)?.missionId !== missionId) throw fail('Unknown or outside-mission contradictory evidence', 'BLOCKED');
      const row = { id: evidence.id, missionId, evidence, refIdentities };
      await this.store.saveEvidence(row); return jsonClone(row);
    });
  }
  async submitWorkItem(input, mainModel) {
    const workItem = workItemContract(input);
    const mission = this.missions.get(workItem.missionId); if (!mission) throw fail('Unknown mission', 'NOT_FOUND');
    const role = this.config.roles[workItem.role]; if (!role) throw fail('Unknown role');
    if (workItem.capabilities.some(cap => !role.tools.includes(cap))) throw fail('Role lacks requested capabilities', 'CAPABILITY');
    const edges = workPredecessors(workItem);
    for (const edge of edges) {
      const parent = this.tasks.get(edge.id);
      if (!parent || parent.workItem?.missionId !== mission.id || (edge.use === 'applied' && parent.workItem.intendedUse !== 'publish')) throw fail(`Incompatible predecessor ${edge.id} for ${edge.use}`, 'BLOCKED');
    }
    for (const ref of workItem.inputRefs) await boundedInputIdentity(this.store, ref);
    await this.assertFileSnapshotInputs(workItem);
    if (workItem.evidenceIds?.length) await this.resolveEvidenceIndex(workItem);
    return this.spawn({ id: workItem.id, role: workItem.role, profile: workItem.profile, directory: workItem.directory, task: workPrompt(workItem, mission), mainModel, dependsOn: edges.map(edge => edge.id), workItem });
  }
  async acceptCandidate(id, { use, reportArtifactId } = {}) {
    if (!this.trustedChecks?.check) throw fail('Trusted acceptance checker unavailable; candidate remains unaccepted', 'UNSUPPORTED');
    identifier(reportArtifactId, 'candidate report');
    if (!['inspect', 'publish'].includes(use)) throw fail('Unsupported acceptance use', 'UNSUPPORTED');
    return this.mutate(async () => {
      const task = this.get(id);
      if (!task.workItem || !TERMINAL.has(task.state) || this.running.has(id) || task.state !== 'idle' || task.currentReport !== reportArtifactId) throw fail('No settled current candidate report', 'BLOCKED');
      authorizeMainMutation(task.role, task.profile, this.modes, task.modeBindings ?? []);
      if (use === 'publish') {
        await this.refreshConfiguration();
        const plan = await fileEffectPlan(this.config.trustedEffectGrant, task.workItem);
        if (task.effectGrantHash !== plan.grantHash) throw fail('Publication grant changed', 'PERMISSION');
      }
      if (task.acceptedReport === reportArtifactId && task.acceptedUse === use) return this.summary(task);
      const report = await this.store.artifactInfo(reportArtifactId);
      if (!report.complete || report.owner !== id) throw fail('Candidate report is not immutable', 'STORAGE');
      if (!await this.trustedChecks.check(task, report, { use, reportArtifactId })) throw fail('Trusted check did not accept candidate', 'BLOCKED');
      task.acceptedReport = reportArtifactId; task.acceptedUse = use;
      await this.persist(task); this.kick(); return this.summary(task);
    });
  }
  async applyCandidate(id, { reportArtifactId } = {}) {
    identifier(reportArtifactId, 'accepted report');
    return this.mutate(async () => {
      const task = this.get(id);
      if (!task.workItem || task.workItem.intendedUse !== 'publish') throw fail('No trusted publication for this task; nothing applied', 'UNSUPPORTED');
      authorizeMainMutation(task.role, task.profile, this.modes, task.modeBindings ?? []);
      await this.refreshConfiguration();
      const plan = await fileEffectPlan(this.config.trustedEffectGrant, task.workItem);
      if (task.effectGrantHash !== plan.grantHash) throw fail('Publication grant changed; nothing applied', 'PERMISSION');
      if (task.state !== 'idle' || this.running.has(id) || task.currentReport !== reportArtifactId || task.acceptedReport !== reportArtifactId || task.acceptedUse !== 'publish') throw fail('Only the accepted current publish report may be applied', 'BLOCKED');
      const prior = this.effects.get(id);
      if (prior) {
        assertEffectJournal(prior, plan, task);
        if (prior.state === 'confirmed') {
          const report = await this.store.artifactInfo(reportArtifactId);
          if (!await this.trustedChecks?.check(task, report, { use: 'publish', reportArtifactId })) throw fail('Confirmed effect acceptance is no longer valid', 'STORAGE');
          await verifyPublished(plan, this.store.limits.responseBytes); return this.summary(task);
        }
        this.effectObservations.set(id, await observePublished(plan, this.store.limits.responseBytes));
        throw fail(`Effect intent is unresolved (${this.effectObservations.get(id)}); never retry automatically. Inspect the publication effect metadata and destination.`, 'UNKNOWN_EFFECT');
      }
      const report = await this.store.artifactInfo(reportArtifactId);
      if (!await this.trustedChecks?.check(task, report, { use: 'publish', reportArtifactId })) throw fail('Accepted proof or pinned source is stale; no effect was attempted', 'BLOCKED');
      await assertVacant(plan);
      const prepared = effectJournal(plan, reportArtifactId);
      // Fence retries in memory before the durability boundary: atomicJson can
      // fail after rename, when a prepared journal already exists on disk.
      this.effects.set(id, prepared);
      try { await this.store.saveEffect(prepared); }
      catch (error) {
        this.effectObservations.set(id, 'absent_unconfirmed');
        throw fail(`Effect intent persistence uncertain; no file was staged. Inspect publication effect metadata before any new attempt: ${safeError(error)}`, 'UNKNOWN_EFFECT');
      }
      try {
        await createPublishedFile(plan, this.store);
        const confirmed = { ...prepared, state: 'confirmed' };
        await this.store.saveEffect(confirmed); // Durable receipt before reporting applied.
        this.effects.set(id, confirmed);
        this.kick(); return this.summary(task);
      } catch (error) {
        this.effectObservations.set(id, await observePublished(plan, this.store.limits.responseBytes));
        throw fail(`Effect outcome unresolved (${this.effectObservations.get(id)}). Inspect publication effect metadata and destination; do not replay: ${safeError(error)}`, 'UNKNOWN_EFFECT');
      }
    });
  }
  async digestPinnedArtifact(task, signal) {
    signal.throwIfAborted();
    const ref = task.workItem.inputRefs[0];
    const info = await boundedInputIdentity(this.store, ref, 'Deterministic input');
    signal.throwIfAborted();
    return { state: 'idle', result: info.sha256, checkpoint: task.checkpoint, turns: 0 };
  }
  prepareResultBatch(batchId, rows, mainModel) {
    return this.mutate(async () => this.resultNotifications.prepareBatch(batchId, rows, mainModel));
  }
  settleResultSlot(id, state) {
    return this.mutate(async () => { this.resultNotifications.settleSlot(id, state); this.kick(); });
  }
  async spawn(params) {
    for (const field of Object.keys(params)) if (!['id', 'role', 'task', 'name', 'profile', 'dependsOn', 'directory', 'dependencyHandoff', 'mainModel', 'workItem', 'resultContract', 'resultInputs', 'publishTo', 'verificationChecks', 'qualityContract'].includes(field)) throw fail(`Unsupported spawn field: ${field}; use canonical task fields`);
    if (this.stopping) throw fail('Host stopping', 'CLOSED');
    // Acquire the command lane BEFORE the metadata mutation queue. Otherwise
    // spawn can hold that queue waiting for slots held by preparation guardians
    // whose durable track/untrack callbacks are queued behind this spawn.
    const releaseSnapshotSlot = this.config.roles[params.role]?.workspace === 'worktree' ? await this.commands.acquire() : undefined;
    const snapshotCommands = releaseSnapshotSlot ? {
      acquire: async signal => { signal?.throwIfAborted(); return () => {}; },
      trackProcess: this.commands.trackProcess, untrackProcess: this.commands.untrackProcess,
    } : undefined;
    try { return await this.mutate(async () => {
      if (this.stopping) throw fail('Host stopping', 'CLOSED');
      const id = identifier(params.id ?? randomUUID());
      const batchAdmissionHash = this.resultNotifications.checkAdmission(params);
      const taskText = text(params.task, 'task', 24 * 1024);
      const requestedName = params.name === undefined ? undefined : validateTaskName(params.name);
      const dependsOn = normalizeDependencies(params.dependsOn, id);
      const dependencyPolicy = 'all_succeeded', dependencyHandoff = params.dependencyHandoff ?? 'summary';
      if (!['summary', 'none'].includes(dependencyHandoff)) throw fail('Invalid dependency handoff');
      const scope = params.dependencyHandoff ? { dependencyHandoff } : {};
      if (params.resultContract !== undefined && !['primary', 'code-change'].includes(params.resultContract)) throw fail('Unsupported result contract', 'UNSUPPORTED');
      const resultInputSpecs = normalizeResultInputs(params.resultInputs);
      const publishTo = normalizePublishTo(params.publishTo);
      if (publishTo.some(route => route.when === 'candidate') && params.resultContract !== 'code-change') throw fail('Candidate routes require the code-change immutable snapshot backend', 'UNSUPPORTED');
      if (params.workItem && publishTo.length) throw fail('WorkItems cannot receive or publish result notifications', 'UNSUPPORTED');
      if (publishTo.length) scope.publishTo = publishTo;
      if (params.workItem && resultInputSpecs.length) throw fail('WorkItems use their immutable inputRefs contract', 'UNSUPPORTED');
      if (resultInputSpecs.length) scope.resultInputs = resultInputSpecs;
      if (params.resultContract) scope.resultContract = params.resultContract;
      const qualityContract = normalizeQualityContract(params.qualityContract);
      if (qualityContract) scope.qualityContract = qualityContract;
      const verificationChecks = normalizeVerificationChecks(params.verificationChecks);
      if (verificationChecks.length) scope.verificationChecks = verificationChecks;
      // Keep stable record hashes for unchanged canonical assignments, not old input aliases.
      const requestHash = hash(encodeJson({ role: params.role, task: taskText, profile: params.profile, directory: params.directory ?? this.config.cwd, mainModel: params.mainModel, projectInstructions: '', ...(requestedName === undefined ? {} : { name: requestedName }), ...(dependsOn.length ? { dependsOn } : {}), ...scope, ...(params.workItem ? { workItem: params.workItem } : {}) }));
      const existing = this.tasks.get(id);
      if (existing) { if (existing.requestHash !== requestHash) throw fail('Task ID reused for a different request', 'CONFLICT'); authorizeMainMutation(existing.role, existing.profile, this.modes, existing.modeBindings ?? []); return this.summary(existing); }
      for (const parent of dependsOn) if (!this.tasks.has(parent)) throw fail(`Unknown dependency agentId: ${parent}`);
      // New agents only reference existing nodes; edges cannot create a cycle.
      // Archive IDs cannot be reused after a late RPC retry.
      if (this.store.metadata?.get('work_items', this.store.scope, id)?.state === 'archived') throw fail('Task ID already archived', 'CONFLICT');
      await this.refreshConfiguration();
      if (!this.config.roles[params.role]) throw fail('Unknown role');
      const role = jsonClone(this.config.roles[params.role]);
      if (role.workspace === 'worktree' && !snapshotCommands) throw fail('Workspace policy changed before input snapshot admission; inspect and submit a fresh task', 'WORKSPACE');
      if (params.resultContract === 'code-change' && (params.workItem || role.workspace !== 'worktree')) throw fail('Code-change results require an isolated owned worktree', 'CAPABILITY');
      const missingTools = (role.requiredTools ?? []).filter(tool => ![...role.tools, ...WEB_TOOLS].includes(tool));
      if (missingTools.length) throw fail(`Role ${role.name} missing required tools: ${missingTools.join(', ')}. No model call or fallback; inspect this capability before delegation.`, 'CAPABILITY');
      let profile;
      if (params.workItem?.route !== undefined) {
        const proposed = workItemContract(params.workItem);
        // Check role rights, site pins and role mode before selecting a marked model.
        authorizeModes(role, {}, this.modes);
        const decision = routeWork(proposed, this.config);
        if (decision.state !== 'selected') throw fail(`${decision.reason}${decision.eligibleProfiles ? `; eligible direct profile choices (not quality-ranked): ${decision.eligibleProfiles.join(', ') || '(none)'}` : ''}`, decision.code ?? 'ROUTE');
        profile = decision.profile;
      } else profile = resolveProfile(this.config, params.role, params.profile, params.mainModel);
      let effectGrantHash;
      if (params.workItem) {
        const item = workItemContract(params.workItem), mission = this.missions.get(item.missionId);
        await this.assertGitSnapshotInputs(item);
        await this.assertFileSnapshotInputs(item);
        if (id !== item.id || !mission || role.name !== item.role || taskText !== workPrompt(item, mission) || item.capabilities.some(cap => !role.tools.includes(cap))) throw fail('Invalid work item admission', 'CAPABILITY');
        if (role.workspace !== 'readonly' || item.capabilities.some(tool => tool !== 'artifact_read')) throw fail('Universal work permits only pinned artifact_read; live files, web, mail and effects need trusted adapters', 'UNSUPPORTED');
        if (item.intendedUse === 'publish') {
          if (item.execution !== 'artifact-sha256') throw fail('Only host-owned deterministic publication is supported', 'UNSUPPORTED');
          const plan = await fileEffectPlan(this.config.trustedEffectGrant, item);
          effectGrantHash = plan.grantHash;
        }
        const edges = workPredecessors(item);
        if (normalizeDependencies(edges.map(edge => edge.id), item.id).join('\0') !== dependsOn.join('\0')) throw fail('Work predecessor edges do not match dependencies', 'BLOCKED');
        for (const edge of edges) {
          const parent = this.tasks.get(edge.id);
          if (parent?.workItem?.missionId !== item.missionId || (edge.use === 'applied' && parent.workItem.intendedUse !== 'publish')) throw fail('Unknown or incompatible work predecessor', 'BLOCKED');
        }
        for (const ref of item.inputRefs) await boundedInputIdentity(this.store, ref);
        if (item.evidenceIds?.length) await this.resolveEvidenceIndex(item);
        if ((role.requiredTools ?? []).some(tool => !item.capabilities.includes(tool))) throw fail('Work item omits required role capabilities', 'CAPABILITY');
        role.tools = role.tools.filter(tool => item.capabilities.includes(tool));
      }
      const workflow = workflowAdapter({ role, profile });
      assertQualityAdmission(qualityContract, { role, workflow: workflow.id, workItem: params.workItem, resultInputs: resultInputSpecs });
      if (verificationChecks.length && (params.workItem || workflow.id !== 'generic' || !role.tools.includes('bash') || !resultInputSpecs.some(input => input.required && ['candidate', 'final'].includes(input.when) && ['reviewer', 'verifier'].includes(input.view) && this.tasks.get(input.from.agentId)?.resultContract === 'code-change'))) throw fail('Required verification checks need explicit bash capability and a required immutable product input', 'CAPABILITY');
      await workflow.refreshMode?.(this.config, this.store, this.modes);
      const modeBindings = authorizeMainMutation(role, profile, this.modes);
      const workflowPatch = workflow.admit({ ...params, role, profile, task: taskText, dependsOn, dependencyPolicy, dependencyHandoff }, this.tasks, this.running);
      const sourceCwd = await realpath(params.directory ?? modeBindings[0]?.root ?? this.config.cwd);
      const boundary = modeBindings[0]?.root ?? this.config.cwd;
      const rel = relative(boundary, sourceCwd);
      const inside = rel === '' || (rel !== '..' && !rel.startsWith('../') && !isAbsolute(rel));
      if (!inside && (modeBindings.length || ![...this.tasks.values()].some(t => t.cwd === sourceCwd))) throw fail('Directory must be in the authorized workspace or an existing team worktree', 'PERMISSION');
      workflow.authorizeMode?.(this.modes.get(workflow.requiredMode), { directory: sourceCwd, task: taskText, dependsOn }, role, profile);
      // Validate only after mode/path admission: no model fallback or off-mode auth work.
      const resultDeclarations = this.resultInputs.prepare(id, resultInputSpecs, role);
      const resultRoutes = this.resultNotifications.prepare(id, publishTo);
      if (params.workItem?.execution !== 'artifact-sha256') await this.runner.validate(profile);
      const task = { id, name: assignTaskName(requestedName, role.name, this.tasks.values()), requestHash, dependsOn, state: 'queued', currentReport: null, role, profile, rootBudget: createRootBudget(this.limits), resultEpoch: 1, goalRevision: 1, executionSegment: 0, task: taskText, sourceCwd, cwd: sourceCwd, mailbox: [], delivering: [], seen: [], autoWakes: 0, result: '', handoff: '', createdAt: Date.now(), updatedAt: Date.now(), instruction: '', checkpoint: emptyCheckpoint(), usage: emptyUsageState(), usageOnlyTransportRetry: createUsageOnlyTransportRetryPolicy(), commandRegistrationFailureReceiptVersion: COMMAND_REGISTRATION_FAILURE_RECEIPT_VERSION };
      task.promptVersion = CHILD_PROMPT_VERSION;
      if (qualityContract) task.qualityContract = qualityContract;
      if (verificationChecks.length) { task.verificationChecks = verificationChecks; task.requiredResultChecks = requiredCheckIdentities(task); task.task = verificationAssignment(taskText, verificationChecks); }
      if (resultDeclarations.length) task.resultInputDeclarations = resultDeclarations;
      if (batchAdmissionHash) task.batchAdmissionHash = batchAdmissionHash;
      if (resultRoutes.length) task.resultRouteDeclarations = resultRoutes;
      task.resultContract = params.resultContract ?? 'primary';
      if (role.workspace === 'worktree') task.inputSnapshot = await this.readWorktreeSnapshot(sourceCwd, id, undefined, snapshotCommands);
      if (qualityContract?.ownedPaths.length && role.workspace === 'shared') {
        task.ownedResources = [];
        for (const path of qualityContract.ownedPaths) {
          // Canonicalize existing ancestors so a symlink cannot claim a different
          // lexical lock for the same shared destination. Missing tails stay literal.
          let parent = join(sourceCwd, path), suffix = [];
          for (;;) {
            try { parent = await realpath(parent); break; }
            catch (error) { if (error.code !== 'ENOENT') throw error; suffix.unshift(basename(parent)); parent = dirname(parent); }
          }
          const target = join(parent, ...suffix), ownedRelative = relative(sourceCwd, target);
          if (ownedRelative === '..' || ownedRelative.startsWith('../') || isAbsolute(ownedRelative)) throw fail('Owned path escapes its workspace', 'PERMISSION');
          task.ownedResources.push(`workspace:${target}`);
        }
      }
      if (params.workItem) { task.workItem = jsonClone(params.workItem); task.attempts = []; task.ownedResources = [...params.workItem.resources]; if (effectGrantHash) task.effectGrantHash = effectGrantHash; }
      task.dependencyPolicy = dependencyPolicy; task.dependencyHandoff = dependencyHandoff;
      if (modeBindings.length) task.modeBindings = modeBindings;
      Object.assign(task, workflow.authorityPatch(this.modes.get(workflow.requiredMode)), workflowPatch);
      const playbook = await createTaskPlaybookSnapshot(task);
      if (playbook) task.playbook = playbook;
      enablePublicWeb(task);
      await this.persist(task); this.tasks.set(id, task); this.resultInputs.register(resultDeclarations); this.resultNotifications.register(resultRoutes); this.resultNotifications.settleSlot(id, 'accepted'); this.kick(); return this.summary(task);
    }); } finally { releaseSnapshotSlot?.(); }
  }
  kick() {
    if (this.stopping) return;
    this.scheduleAgain = true;
    if (this.scheduling) return;
    this.scheduling = true;
    queueMicrotask(async () => {
      try {
        do {
          this.scheduleAgain = false;
          await this.mutateInternal(() => this.admit());
        } while (this.scheduleAgain && !this.stopping);
      } catch (error) {
        this.fatal ??= `Scheduler failed: ${safeError(error)}`; this.stopping = true;
        for (const r of this.running.values()) r.abort.abort(error);
      } finally { this.scheduling = false; }
    });
  }
  async verifyAppliedPredecessors(task) {
    await this.refreshConfiguration();
    for (const edge of workPredecessors(task.workItem).filter(edge => edge.use === 'applied')) {
      const parent = this.tasks.get(edge.id), receipt = this.effects.get(edge.id);
      if (!parent || parent.state !== 'idle' || this.running.has(edge.id) || parent.acceptedReport !== parent.currentReport || parent.acceptedUse !== 'publish' || receipt?.state !== 'confirmed' || receipt.reportArtifactId !== parent.currentReport) throw fail(`Predecessor ${edge.id} has no confirmed current receipt`, 'BLOCKED');
      authorizeModes(parent.role, parent.profile, this.modes, parent.modeBindings ?? []);
      const plan = await fileEffectPlan(this.config.trustedEffectGrant, parent.workItem);
      if (parent.effectGrantHash !== plan.grantHash) throw fail(`Predecessor ${edge.id} grant was revoked or changed`, 'PERMISSION');
      assertEffectJournal(receipt, plan, parent);
      const report = await this.store.artifactInfo(parent.currentReport);
      if (!this.trustedChecks?.check || !await this.trustedChecks.check(parent, report, { use: 'publish', reportArtifactId: parent.currentReport })) throw fail(`Predecessor ${edge.id} acceptance is stale`, 'STORAGE');
      await verifyPublished(plan, this.store.limits.responseBytes);
    }
  }
  async trackCommandProcess(owner, identity, started) {
    if (!this.tasks.get(owner)?.executionIntent) return;
    return this.mutateInternal(async () => {
      const task = this.get(owner), intent = task.executionIntent;
      if (!intent) throw fail('Command execution intent disappeared', 'STALE');
      if (started) this.running.get(owner)?.abort.signal.throwIfAborted();
      else if (await processIdentity(identity.pid) === identity.started) throw fail('Command executor remains alive', 'BUSY');
      const executor = intent.executor ?? identity;
      const guardians = (executor.guardians ?? []).filter(row => row.nonce !== identity.nonce);
      if (started) guardians.push(identity);
      intent.executor = { ...executor, guardians };
      this.admission.registerExecutor(owner, intent.executor); await this.persist(task);
    });
  }
  async reconcileExecutors() {
    for (const id of this.recoveringExecutors) {
      const task = this.tasks.get(id), executor = task?.executionIntent?.executor;
      if (!task?.recoveryPending || !executor) { this.recoveringExecutors.delete(id); continue; }
      if (!await executorStopped(executor)) { this.scheduleAdmissionWake(Date.now() + 500); continue; }
      settleRootSegment(task, { intentId: task.executionIntent.id, startedAt: task.executionIntent.startedAt, progress: rootProgressMarker(task) });
      const decision = recoveryDecision({ ...task, state: 'running' }, { executorStopped: true });
      task.recoveryPending = false; this.recoveringExecutors.delete(id);
      task.state = decision.state; task.error = decision.retry ? undefined : decision.reason; task.currentReport = null;
      this.admission.markUncertain(id, !automaticRecoverySafe(task));
      await this.releaseAdmissionReservation(task, 'executor_stopped');
      if (automaticRecoverySafe(task)) await this.admission.reconcile(task.admissionFence, { preserveRequestClaims: true });
      if (decision.retry) { task.recoveryAttempts = (task.recoveryAttempts ?? 0) + 1; delete task.executionIntent; task.continuationRequested = Boolean(task.checkpoint?.records?.length); }
      await this.persist(task);
      if (!decision.retry) await this.completionMail(task);
    }
  }
  resumeEligibleOverloads() {
    const now = Date.now();
    for (const task of this.tasks.values()) {
      if (task.state !== 'paused' || this.running.has(task.id) || task.cancelRequested || task.authorityRevoked || task.recoveryPending || this.reconciling.has(task.id) || this.autoOverloadResumePending.has(task.id) || !automaticRecoverySafe(task) || this.effects.has(task.id)) continue;
      const fault = task.resumeCapsule?.providerFault;
      if (fault?.kind !== 'provider_overload' || fault.requestOutcome !== 'rejected_before_execution' || this.autoOverloadResumeFailures.get(task.id) === fault.observedAt) continue;
      let capsule;
      try { capsule = assertCapsuleBinding(task, task.resumeCapsule); } catch { continue; }
      const eligibleAt = Math.max(overloadCooldownUntil(fault) ?? Infinity, capsule.nextEligibleAt ?? 0);
      if (eligibleAt > now) { if (Number.isSafeInteger(eligibleAt)) this.scheduleAdmissionWake(eligibleAt); continue; }
      const account = task.quotaHold?.account ?? task.profile.account ?? task.profile.provider;
      const pool = this.admission.quotaPools(now).get(account);
      if (pool?.retryAt > now) { this.scheduleAdmissionWake(pool.retryAt); continue; }
      if (pool?.held || this.connectionHeld && task.profile.allowBackground !== true || this.pressure || this.foregroundPressure || this.systemPressure) continue;
      this.autoOverloadResumePending.add(task.id);
      // Call public resume after the scheduler mutation completes: its full
      // authority/input/cancellation checks still gate the continuation.
      queueMicrotask(() => { void this.resume(task.id, '').catch(async error => {
        if (['BUSY', 'RATE_LIMIT'].includes(error.code) && !this.stopping) { this.scheduleAdmissionWake(Date.now() + 1000); return; }
        this.autoOverloadResumeFailures.set(task.id, fault.observedAt);
        try { await this.mutateInternal(async () => {
          if (task.state === 'paused' && task.resumeCapsule?.providerFault?.observedAt === fault.observedAt) { task.error = `Automatic overload continuation blocked: ${safeError(error)}`; await this.persist(task); }
        }); } catch { /* Persistence failures are already recorded as engine.fatal. */ }
      }).finally(() => this.autoOverloadResumePending.delete(task.id)); });
    }
  }

  async admit() {
    await this.reconcileExecutors();
    for (const task of this.tasks.values()) if (task.state === 'queued' && !task.cancelRequested && !task.authorityRevoked && task.resultInputDeclarations?.length) this.resultInputs.register(task.resultInputDeclarations);
    if (this.resultInputs.advance()) this.kick();
    for (const task of this.tasks.values()) if (task.resultRouteDeclarations?.length) this.resultNotifications.register(task.resultRouteDeclarations);
    if (this.resultNotifications.advance()) this.kick();
    this.resumeEligibleOverloads();
    // Dependency check, reservation and durable start are one serialized mutation.
    for (const task of this.tasks.values()) {
      if (this.stopping) return;
      this.admissionWaits.delete(task.id);
      if (task.state !== 'queued' || this.running.has(task.id)) continue;
      if (task.cancelRequested || task.authorityRevoked) { if (['pending', 'active'].includes(task.usageOnlyTransportRetry?.state)) task.usageOnlyTransportRetry.state = 'exhausted'; task.state = 'cancelled'; task.currentReport = null; task.error = 'Cancellation or authority revocation is durable'; await this.persist(task); await this.completionMail(task); continue; }
      this.restoreRequestPermitFence(task);
      if (task.requestPermitUncertain) {
        task.state = 'paused'; task.currentReport = null; task.error = 'Unresolved shared provider permit prevents local dispatch; inspect its claim before continuation.';
        await this.persist(task); await this.completionMail(task); continue;
      }
      if (task.resumeCapsule) {
        try { assertCapsuleBinding(task, task.resumeCapsule); }
        catch (error) { task.state = 'blocked'; task.error = safeError(error); await this.persist(task); await this.completionMail(task); continue; }
      }
      const budget = rootBudgetDecision(task);
      if (!budget.allowed) { task.state = 'paused'; task.error = budget.reason; await this.persist(task); await this.completionMail(task); continue; }
      try {
        const workflow = workflowAdapter(task);
        await workflow.refreshMode?.(this.config, this.store, this.modes);
        workflow.authorizeMode?.(this.modes.get(workflow.requiredMode), { directory: task.sourceCwd, task: task.task, projectInstructions: task.projectInstructions, dependsOn: task.dependsOn }, task.role, task.profile);
        authorizeModes(task.role, task.profile, this.modes, task.modeBindings ?? []);
      }
      catch (error) { task.state = 'blocked'; task.error = safeError(error); await this.persist(task); await this.completionMail(task); this.scheduleAgain = true; continue; }
      const data = this.resultInputs.readiness(task);
      task.dataReadiness = { state: data.blocked.length ? 'blocked_input' : data.ready ? 'ready' : 'waiting_input', blockedBy: data.blocked, waitingFor: data.waiting };
      if (data.blocked.length) {
        this.settlingInputs.add(task.id);
        task.state = 'blocked'; task.currentReport = null; task.blockedBy = [...new Set(data.blocked)];
        task.error = `Required result input unavailable from producer agentIds: ${task.blockedBy.join(', ')}. Inspect the pinned input; submit newly authorized work if its binding is no longer valid.`;
        await this.persist(task); await this.completionMail(task);
        this.settlingInputs.delete(task.id); this.scheduleAgain = true; continue;
      }
      if (!data.ready) continue;
      task.resultBindings = data.bindings;
      const dependency = dependencyState(task, this.tasks, this.running, this.effects);
      const staleDependency = dependency.state === 'ready' && !workflowAdapter(task).dependenciesFresh(task, this.tasks, this.running);
      if (staleDependency) { dependency.state = 'blocked'; dependency.blockedBy = task.dependsOn ?? []; }
      if (dependency.state === 'blocked') {
        task.state = 'blocked'; task.blockedBy = dependency.blockedBy;
        task.error = staleDependency ? 'Workflow dependency report changed after validation; inspect and spawn a fresh authorized projection.' : `Blocked by predecessor agentIds: ${dependency.blockedBy.join(', ')}. Repair predecessors, then resume this agent explicitly.`;
        await this.persist(task); await this.completionMail(task); this.scheduleAgain = true; continue;
      }
      if (dependency.state === 'waiting') continue;
      if (task.workItem) {
        if (this.workItemHasMail(task)) { task.state = 'blocked'; task.error = 'WorkItem mail boundary violated; no message was acknowledged, delivered, or exposed to the provider'; await this.persist(task); await this.completionMail(task); this.scheduleAgain = true; continue; }
        const identities = new Map();
        let unavailable;
        for (const ref of task.workItem.inputRefs) { try { identities.set(ref.artifactId, await boundedInputIdentity(this.store, ref)); } catch (error) { unavailable = safeError(error); break; } }
        try { if (this.gitSnapshotGrantHash || this.fileSnapshotGrantHash) await this.refreshConfiguration(); await this.assertGitSnapshotInputs(task.workItem); await this.assertFileSnapshotInputs(task.workItem); } catch (error) { unavailable = safeError(error); }
        if (unavailable) { task.state = 'blocked'; task.error = `Input snapshot unavailable: ${unavailable}`; await this.persist(task); await this.completionMail(task); this.scheduleAgain = true; continue; }
        const workDecision = admitWork(task.workItem, { mission: this.missions.get(task.workItem.missionId), tasks: this.tasks, running: this.running, inputIdentity: identities, appliedReceipts: this.effects, concurrency: this.limits.concurrency, pressure: this.pressure || this.foregroundPressure || this.systemPressure });
        if (workDecision.state === 'blocked') { task.state = 'blocked'; task.error = workDecision.reason; await this.persist(task); await this.completionMail(task); this.scheduleAgain = true; continue; }
        if (workDecision.state === 'waiting') continue;
      }
      if (task.workItem?.route) {
        try {
          await this.refreshConfiguration();
          this.verifyWorkRoute(task.workItem, task.profile, task.modeBindings ?? []);
        } catch (error) {
          task.state = 'blocked'; task.error = `Trusted route invalidated; no execution: ${safeError(error)}`;
          await this.persist(task); await this.completionMail(task); this.scheduleAgain = true; continue;
        }
      }
      // Recheck external bytes, accepted predecessors and current grants immediately before reservation.
      if (task.workItem && workPredecessors(task.workItem).some(edge => edge.use === 'applied')) {
        try { await this.verifyAppliedPredecessors(task); }
        catch (error) {
          task.state = 'blocked'; task.blockedBy = workPredecessors(task.workItem).filter(edge => edge.use === 'applied').map(edge => edge.id);
          task.error = `Applied predecessor verification failed; no execution: ${safeError(error)}`;
          await this.persist(task); await this.completionMail(task); this.scheduleAgain = true; continue;
        }
      }
      let evidenceInputs;
      if (task.workItem?.evidenceIds?.length) {
        try { evidenceInputs = await this.resolveEvidenceIndex(task.workItem); }
        catch (error) {
          task.state = 'blocked'; task.error = `Evidence handoff invalidated; no execution: ${safeError(error)}`;
          await this.persist(task); await this.completionMail(task); this.scheduleAgain = true; continue;
        }
      }
      const reservationDecision = await this.admission.reserveShared(task, { running: this.running, tasks: this.tasks, pressure: this.pressure || this.foregroundPressure || this.systemPressure, connectionHeld: this.connectionHeld, workers: this.runner.maxWorkers ?? this.limits.concurrency, memory: this.resourceObservation(), quotaPools: this.quotaPools, sharedMemory: Boolean(this.memoryAccountingEnabled), memoryObservationKnown: this.memoryObservationKnown, systemMemory: this.systemMemory, configEpoch: this.runner.configEpoch });
      this.lastMemoryAdmission = { agentId: task.id, admitted: reservationDecision.admitted, reason: reservationDecision.reason, sampledAt: Date.now() };
      if (!reservationDecision.admitted) {
        this.admissionWaits.set(task.id, { admitted: false, reason: reservationDecision.reason, sampledAt: this.lastMemoryAdmission.sampledAt });
        if (reservationDecision.retryAt > Date.now()) this.scheduleAdmissionWake(reservationDecision.retryAt);
        continue; // Observation does not release a fence, retry a provider, or settle a wait.
      }
      if (this.stopping) { await this.admission.releaseShared(task.id); return; }
      if (task.cancelRequested || task.authorityRevoked) { await this.admission.releaseShared(task.id); if (['pending', 'active'].includes(task.usageOnlyTransportRetry?.state)) task.usageOnlyTransportRetry.state = 'exhausted'; task.state = 'cancelled'; task.currentReport = null; task.error = 'Cancellation or authority revocation is durable'; await this.persist(task); await this.completionMail(task); continue; }
      const running = { abort: new AbortController(), progress: {}, promise: null, reservation: reservationDecision.reservation };
      try {
        task.dependencyInputs = dependencyInputs(task, this.tasks, this.effects);
        if (evidenceInputs) task.evidenceInputs = evidenceInputs; else delete task.evidenceInputs;
        const resumedCapsule = task.resumeCapsule;
        task.state = 'running'; task.currentReport = null;
        if (!resumedCapsule) { task.result = ''; task.resultRef = undefined; } // A resumed epoch retains every sealed reference until replacement succeeds.
        task.error = undefined; delete task.blockedBy; delete task.quotaBackoff;
        task.executionSegment = (task.executionSegment ?? 0) + 1;
        const usageRetry = task.usageOnlyTransportRetry && assertUsageOnlyTransportRetryPolicy(task.usageOnlyTransportRetry);
        const retryingUnknownUsage = usageRetry?.state === 'pending' && usageRetry.retries === 1;
        task.executionIntent = { id: randomUUID(), startedAt: Date.now(), segment: task.executionSegment,
          ...(resumedCapsule ? { resumeCapsuleSha256: hash(encodeJson(resumedCapsule, 32 * 1024)) } : {}),
          ...(usageRetry ? { usageOnlyTransportRetryPolicy: USAGE_ONLY_TRANSPORT_RETRY_POLICY, usageOnlyTransportRetryAttempt: retryingUnknownUsage ? 1 : 0 } : {}),
          ...(retryingUnknownUsage ? { usageOnlyTransportRetryOf: usageRetry.interruptions.at(-1).requestId } : {}),
        };
        if (retryingUnknownUsage) usageRetry.state = 'active';
        task.memoryPlan = reservationDecision.reservation.memoryPlan;
        if (reservationDecision.accounting) this.lastMemoryAccounting = reservationDecision.accounting;
        if (resumedCapsule) { task.lastResumeCapsule = resumedCapsule; delete task.resumeCapsule; delete task.providerFault; }
        this.persistAdmissionReservation(task, reservationDecision.reservation);
        if (task.workItem) {
          // Attempts count persisted execution segments, not completed work. Root
          // no-progress accounting and uncertainty fences govern continuation.
          task.acceptedReport = null; task.acceptedUse = null;
          task.attempts.push({ id: randomUUID(), state: 'running', startedAt: Date.now(), ...(task.workItem.execution === 'artifact-sha256' ? {} : { provider: task.profile.provider, model: task.profile.model }), execution: task.workItem.execution ?? 'model' });
        }
        task.mailbox = await this.mail.pending(task.id, { limit: Math.min(32, this.limits.mailboxCount), withBody: true });
        this.mail.deliver(task.mailbox.map(mail => mail.id));
        const unexpired = task.mailbox.filter(m => m.expiresAt >= Date.now());
        task.expiredMessages = (task.expiredMessages ?? 0) + task.mailbox.length - unexpired.length;
        task.delivering = [...task.delivering, ...unexpired].slice(-32); task.mailbox = [];
        this.running.set(task.id, running);
        await this.persist(task);
      } catch (error) {
        this.running.delete(task.id); await this.releaseAdmissionReservation(task, 'start_failed'); throw error;
      }
      if (this.stopping) running.abort.abort(fail('Host shutting down', 'SHUTDOWN'));
      running.promise = this.execute(task, running).catch(error => {
        running.fatalError = safeError(error);
        if (this.fatal) {
          if (task.state === 'running' || task.state === 'queued') task.state = 'interrupted';
          task.currentReport = null; task.error = this.fatal;
        }
      }).finally(async () => {
        try { await this.releaseAdmissionReservation(task); }
        catch (error) { this.fatal ??= `Admission release failed: ${safeError(error)}`; this.stopping = true; }
        finally { this.running.delete(task.id); this.publishChange(); this.kick(); }
      });
    }
  }
  async readWorktreeSnapshot(cwd, owner, signal, semaphore = this.commands) {
    const probe = await runProcess({ command: 'git', args: ['rev-parse', '--is-inside-work-tree'], cwd, signal, owner, store: this.store, semaphore, timeoutMs: 30_000, env: workspaceGitEnv() });
    if (probe.exitCode !== 0) {
      // A broken/unborn repository or Git environment must never silently become a copy snapshot.
      if (!/^fatal: not a git repository\b/.test(probe.stderr.preview)) throw fail('Git workspace detection failed; inspect before retrying', 'WORKSPACE');
      for (let path = await realpath(cwd); ; path = dirname(path)) {
        let marker; try { marker = await lstat(join(path, '.git')); } catch (error) { if (error.code !== 'ENOENT') throw error; }
        if (marker) throw fail('Invalid Git workspace; directory fallback refused', 'WORKSPACE');
        if (dirname(path) === path) break;
      }
      return captureDirectory(this.store, owner, cwd, signal);
    }
    if (probe.stdout.preview.trim() !== 'true') throw fail('Bare Git input is not a working directory', 'WORKSPACE');
    const head = await runProcess({ command: 'git', args: ['rev-parse', '--verify', 'HEAD'], cwd, signal, owner, store: this.store, semaphore, timeoutMs: 30_000, env: workspaceGitEnv() });
    if (head.exitCode !== 0) throw fail('Git workspace requires a valid HEAD', 'WORKSPACE');
    const base = head.stdout.preview.trim();
    if (!/^[0-9a-f]{40,64}$/.test(base)) throw fail('Invalid Git HEAD');
    const status = await runProcess({ command: 'git', args: ['status', '--porcelain=v1', '--untracked-files=all'], cwd, signal, owner, store: this.store, semaphore, timeoutMs: 30_000, env: workspaceGitEnv() });
    if (status.exitCode !== 0 || status.stdout.bytes || status.stdout.preview.trim()) throw fail('Dirty tracked/untracked Git input cannot be sandboxed safely; inspect scope and use explicit non-Git/shared authorization instead', 'WORKSPACE');
    return base;
  }
  assertRequiredResultInputs(task) {
    if (!(task.resultInputDeclarations ?? []).some(input => input.required)) return;
    const data = this.resultInputs.readiness(task);
    if (data.ready) return;
    task.dataReadiness = { state: 'blocked_input', blockedBy: data.blocked, waitingFor: data.waiting };
    throw fail('Required result input authority is no longer ready; inspect the pinned input before any new execution', 'BLOCKED_INPUT');
  }
  checkpointSha256(checkpoint) {
    return hash(encodeJson(assertCheckpointManifest(checkpoint, this.limits.checkpointBytes), this.limits.checkpointBytes));
  }
  async checkpointReferencesConsistent(checkpoint, owner) {
    const manifest = assertCheckpointManifest(checkpoint, this.limits.checkpointBytes);
    const refs = [
      ...manifest.records.map(record => record.ref),
      manifest.historyRef, manifest.continuityRef, manifest.compactionEnvelope?.ref,
      ...(manifest.resultSections?.sections ?? []).map(section => section.ref),
    ].filter(Boolean);
    for (const ref of refs) {
      const identity = await this.store.artifactIdentity(ref.artifactId);
      if (identity?.complete !== true || identity.owner !== owner || identity.bytes !== ref.bytes || identity.sha256 !== ref.sha256) return false;
    }
    return true;
  }
  async usageOnlyTransportRetryEvidence(task, error, provider, running) {
    const intent = task.executionIntent, boundary = intent?.requestBoundary;
    let checkpointConsistent = false;
    try {
      checkpointConsistent = Boolean(boundary?.checkpointSha256 && error.checkpoint && this.checkpointSha256(error.checkpoint) === boundary.checkpointSha256 && this.checkpointSha256(task.checkpoint) === boundary.checkpointSha256 && await this.checkpointReferencesConsistent(error.checkpoint, task.id));
    } catch { checkpointConsistent = false; }
    const completed = new Set(error.checkpoint?.completedToolCallIds ?? []);
    const effectsSettled = !task.worktreeIntent && !this.effects.has(task.id)
      && (task.pendingEffects ?? []).every(step => !effectRequiresReconciliation(step) && completed.has(step.toolCallId))
      && !(task.webReceipts ?? []).some(row => row.state === 'pending')
      && !(intent?.executor?.guardians ?? []).length;
    const requestOutcome = error.requestOutcome ?? provider?.fault?.requestOutcome;
    let authorityConsistent = false, inputConsistent = false;
    try {
      authorityConsistent = Boolean(boundary && boundary.authorityRevision === authorityRevision(task) && boundary.modelProfileFingerprint === modelProfileFingerprint(task));
      if (authorityConsistent) authorizeModes(task.role, task.profile, this.modes, task.modeBindings ?? []);
    } catch { authorityConsistent = false; }
    try {
      this.assertRequiredResultInputs(task);
      const dependency = dependencyState(task, this.tasks, this.running, this.effects);
      const projected = dependencyInputs(task, this.tasks, this.effects);
      inputConsistent = Boolean(boundary && boundary.inputRevision === inputRevision(task) && boundary.snapshotRevision === snapshotRevision(task)
        && boundary.dependencyInputsSha256 === hash(encodeJson(projected, this.limits.checkpointBytes))
        && dependency.state === 'ready' && workflowAdapter(task).dependenciesFresh(task, this.tasks, this.running));
      if (inputConsistent && task.workItem) {
        for (const ref of task.workItem.inputRefs) await boundedInputIdentity(this.store, ref, 'Retry input');
        await this.assertGitSnapshotInputs(task.workItem); await this.assertFileSnapshotInputs(task.workItem);
        if (task.workItem.evidenceIds?.length) await this.resolveEvidenceIndex(task.workItem);
        if (task.workItem.route) { await this.refreshConfiguration(); this.verifyWorkRoute(task.workItem, task.profile, task.modeBindings ?? []); }
      } else if (inputConsistent && task.inputSnapshot && typeof task.inputSnapshot === 'object') await boundedInputIdentity(this.store, task.inputSnapshot, 'Retry input snapshot');
    } catch { inputConsistent = false; }
    const account = task.profile.account ?? task.profile.provider, now = Date.now();
    const localPool = this.quotaPools.get(account), sharedPool = this.admission.quotaPools(now).get(account);
    const quotaClear = intent?.requestQuotaProbe !== true && !localPool?.held && !(localPool?.retryAt > now) && !sharedPool?.held && !(sharedPool?.retryAt > now);
    let memoryClear = !(this.pressure || this.foregroundPressure || this.systemPressure || this.systemMemoryVictim === task.id);
    if (memoryClear) { try { memoryClear = this.renewMemoryBoundary(task).admitted; } catch { memoryClear = false; } }
    const cancellation = this.store.metadata.get('steps', this.store.scope, `cancel-${hash(task.id)}`);
    const usageState = error.usageState;
    const usageOnly = requestOutcome === 'unknown' && task.usageUncertain === true && task.lastRequestReceipt?.outcome === 'unknown'
      && task.lastRequestReceipt?.id === boundary?.requestId && ['missing', 'partial', 'estimated', 'unknown'].includes(usageState?.state);
    return {
      fault: provider?.fault, executorSettled: true, checkpointConsistent, effectsSettled,
      modelRequest: boundary?.kind === 'model' && intent?.usageOperation === undefined, usageOnly, quotaClear,
      cancellationClear: !task.cancelRequested && !task.authorityRevoked && cancellation?.state !== 'requested',
      memoryClear, admissionClear: !this.stopping && !(this.connectionHeld && task.profile.allowBackground !== true),
      authorityConsistent, inputConsistent, budgetAvailable: rootBudgetDecision(task).allowed,
      currentExecution: this.running.get(task.id) === running && task.state === 'running' && !running.abort.signal.aborted && boundary?.requestId === intent?.activeRequestId,
    };
  }
  workspacePreparationExpectation(task, { kind, base, input, destination }) {
    return {
      kind, base, inputSha256: workspaceInputSha256(input), destination,
      inputRevision: inputRevision(task), snapshotRevision: snapshotRevision(task),
      authorityRevision: authorityRevision(task), modelProfileFingerprint: modelProfileFingerprint(task),
    };
  }

  async persistPreparedWorkspaceIntent(task, specification, signal) {
    const expected = this.workspacePreparationExpectation(task, specification);
    const intent = createPreparedWorkspaceIntent({
      intentId: randomUUID(), taskId: task.id, attemptId: task.executionIntent?.id,
      executionSegment: task.executionSegment, ...expected, owner: this.ownershipIdentity,
    });
    await this.mutateInternal(async () => {
      signal.throwIfAborted();
      if (task.worktree || task.worktreeIntent || task.executionIntent?.id !== intent.attemptId) throw fail('Workspace preparation fence changed', 'STALE');
      task.worktreeIntent = intent; await this.persist(task); this.admission.markUncertain(task.id, true);
    });
    return intent;
  }

  async persistWorkspaceMaterializationStart(task, signal) {
    await this.mutateInternal(async () => {
      signal.throwIfAborted();
      const intent = task.worktreeIntent;
      if (!intent || task.cancelRequested || task.authorityRevoked || intent.inputRevision !== inputRevision(task)
        || intent.snapshotRevision !== snapshotRevision(task) || intent.authorityRevision !== authorityRevision(task)
        || intent.modelProfileFingerprint !== modelProfileFingerprint(task)) throw fail('Workspace preparation fence changed before materialization', 'STALE');
      task.worktreeIntent = markWorkspaceMaterializationStarted(intent, this.ownershipIdentity);
      await this.persist(task);
    });
  }

  async workspaceRecoveryExpectation(task, intent) {
    const destination = join(this.store.root, 'worktrees', task.id);
    if (task.worktree || task.role?.workspace !== 'worktree' || intent.destination !== destination) throw fail('Workspace recovery destination changed', 'STALE');
    const productInputs = (task.resultBindings ?? []).filter(binding => binding.candidateSnapshot);
    if (intent.kind === 'result-product-snapshot') {
      if (!productInputs.length || task.productInput) throw fail('Pinned product input changed', 'STALE');
      const subject = productInputs[0].candidateSnapshot;
      if (productInputs.some(binding => binding.candidateSnapshot.productSha256 !== subject.productSha256)) throw fail('Pinned product subjects changed', 'STALE');
      for (const binding of productInputs) productSnapshot(this.store, binding.candidateSnapshot, binding.agentId);
      const actual = await this.store.artifactIdentity(subject.snapshotRef?.artifactId);
      if (!actual?.complete || actual.owner !== subject.ownerId || actual.bytes !== subject.snapshotRef?.bytes || actual.sha256 !== subject.snapshotRef?.sha256) throw fail('Pinned product snapshot identity changed', 'STORAGE');
      const readiness = this.resultInputs.readiness(task);
      if (!readiness.ready || hash(encodeJson(readiness.bindings, this.limits.checkpointBytes)) !== hash(encodeJson(task.resultBindings ?? [], this.limits.checkpointBytes))) throw fail('Pinned result input is no longer ready', 'BLOCKED_INPUT');
      return this.workspacePreparationExpectation(task, { kind: intent.kind, base: subject.productSha256, input: { kind: intent.kind, subject }, destination });
    }
    if (productInputs.length) throw fail('Pinned product workspace kind changed', 'STALE');
    if (intent.kind === 'directory-snapshot') {
      const ref = task.inputSnapshot;
      if (!ref || typeof ref !== 'object' || Array.isArray(ref)) throw fail('Pinned directory input changed', 'STALE');
      const actual = await this.store.artifactIdentity(ref.artifactId);
      if (!actual?.complete || actual.owner !== task.id || actual.bytes !== ref.bytes || actual.sha256 !== ref.sha256) throw fail('Pinned directory snapshot identity changed', 'STORAGE');
      return this.workspacePreparationExpectation(task, { kind: intent.kind, base: ref.sha256, input: { kind: intent.kind, ownerId: task.id, ref }, destination });
    }
    if (intent.kind !== 'git-worktree' || typeof task.inputSnapshot !== 'string' || !/^[a-f0-9]{40,64}$/.test(task.inputSnapshot)) throw fail('Pinned Git input changed', 'STALE');
    return this.workspacePreparationExpectation(task, { kind: intent.kind, base: task.inputSnapshot, input: { kind: intent.kind, sourceCwd: task.sourceCwd, base: task.inputSnapshot }, destination });
  }

  async preparedWorkspaceRecovery(task, { recordedExecutorsStopped }) {
    let expected, inputClear = false, authorityClear = false;
    try {
      expected = await this.workspaceRecoveryExpectation(task, task.worktreeIntent ?? {});
      this.assertRequiredResultInputs(task);
      const dependency = dependencyState(task, this.tasks, this.running, this.effects);
      const projected = dependencyInputs(task, this.tasks, this.effects);
      if (dependency.state !== 'ready' || !workflowAdapter(task).dependenciesFresh(task, this.tasks, this.running)
        || hash(encodeJson(projected, this.limits.checkpointBytes)) !== hash(encodeJson(task.dependencyInputs ?? [], this.limits.checkpointBytes))) throw fail('Pinned dependency input changed', 'BLOCKED_INPUT');
      inputClear = true;
      const workflow = workflowAdapter(task);
      workflow.authorizeMode?.(this.modes.get(workflow.requiredMode), { directory: task.sourceCwd, task: task.task, projectInstructions: task.projectInstructions, dependsOn: task.dependsOn }, task.role, task.profile);
      authorizeModes(task.role, task.profile, this.modes, task.modeBindings ?? []);
      authorityClear = true;
    } catch { /* An incomplete validation is a permanent no-replay decision for this startup. */ }
    const cancellation = this.store.metadata.get('steps', this.store.scope, `cancel-${hash(task.id)}`);
    const cancellationClear = !task.cancelRequested && !task.authorityRevoked && cancellation?.state !== 'requested';
    const usageClear = !task.requestPermitUncertain && !task.executionIntent?.activeRequestId && !task.executionIntent?.requestBoundary
      && !task.executionIntent?.usageOperation && !task.executionIntent?.usageOperationUncertain;
    const effectsClear = !task.worktree && !task.productBase && !this.effects.has(task.id)
      && !(task.pendingEffects ?? []).some(effectRequiresReconciliation) && !(task.webReceipts ?? []).some(row => row.state === 'pending');
    const prospective = { ...task }; delete prospective.worktreeIntent;
    const retryClear = (task.recoveryAttempts ?? 0) < 2 && rootBudgetDecision(task).allowed && recoveryDecision(prospective, { executorStopped: true }).retry === true;
    return preparedWorkspaceRecoveryDecision(task, {
      ownership: this.ownershipIdentity, expected, recordedExecutorsStopped, cancellationClear, usageClear, effectsClear, authorityClear, inputClear, retryClear,
    });
  }

  async prepareResultWorkspace(task, signal) {
    const inputs = (task.resultBindings ?? []).filter(binding => binding.candidateSnapshot);
    if (!inputs.length) return;
    const subject = inputs[0].candidateSnapshot;
    if (inputs.some(binding => binding.candidateSnapshot.productSha256 !== subject.productSha256)) throw fail('Multiple product subjects require separate checker tasks', 'BLOCKED');
    for (const binding of inputs) productSnapshot(this.store, binding.candidateSnapshot, binding.agentId);
    if (task.productInput) {
      if (task.productInput.productSha256 !== subject.productSha256 || !task.worktree || task.worktreeIntent) throw fail('Pinned product workspace changed', 'UNCERTAIN');
      return;
    }
    if (task.worktree || task.worktreeIntent) throw fail('Existing workspace cannot be replaced by a product input', 'UNCERTAIN');
    const parent = join(this.store.root, 'worktrees'), cwd = join(parent, task.id);
    await this.persistPreparedWorkspaceIntent(task, { kind: 'result-product-snapshot', base: subject.productSha256, input: { kind: 'result-product-snapshot', subject }, destination: cwd }, signal);
    await this.persistWorkspaceMaterializationStart(task, signal);
    await mkdir(parent, { recursive: true, mode: 0o700 });
    const path = await materializeDirectory(this.store, subject.ownerId, subject.snapshotRef, cwd, signal);
    await this.mutateInternal(async () => { signal.throwIfAborted(); task.cwd = path; task.productInput = subject; task.worktree = { path, base: subject.productSha256, source: 'pinned result product snapshot' }; delete task.worktreeIntent; await this.persist(task); this.admission.markUncertain(task.id, !automaticRecoverySafe(task)); });
  }
  async prepareWorkspace(task, signal) {
    if (task.role.workspace !== 'worktree' || task.worktree) return;
    const parent = join(this.store.root, 'worktrees'), cwd = join(parent, task.id);
    if (task.inputSnapshot && typeof task.inputSnapshot === 'object') {
      const ref = task.inputSnapshot;
      await this.persistPreparedWorkspaceIntent(task, { kind: 'directory-snapshot', base: ref.sha256, input: { kind: 'directory-snapshot', ownerId: task.id, ref }, destination: cwd }, signal);
      await this.persistWorkspaceMaterializationStart(task, signal);
      await mkdir(parent, { recursive: true, mode: 0o700 });
      const path = await materializeDirectory(this.store, task.id, ref, cwd, signal);
      await this.mutateInternal(async () => { task.cwd = path; task.worktree = { path, base: ref.sha256, source: 'pinned directory snapshot' }; delete task.worktreeIntent; await this.persist(task); this.admission.markUncertain(task.id, !automaticRecoverySafe(task)); });
      return;
    }
    const current = await this.readWorktreeSnapshot(task.sourceCwd, task.id, signal);
    if (task.inputSnapshot && task.inputSnapshot !== current) throw fail('Input snapshot changed before worktree creation; inspect and submit a fresh task', 'WORKSPACE');
    const base = task.inputSnapshot || current;
    if (!task.inputSnapshot) await this.mutateInternal(async () => { task.inputSnapshot = base; await this.persist(task); });
    const again = await this.readWorktreeSnapshot(task.sourceCwd, task.id, signal);
    if (again !== base) throw fail('Input snapshot changed before worktree creation; inspect and submit a fresh task', 'WORKSPACE');
    await this.persistPreparedWorkspaceIntent(task, { kind: 'git-worktree', base, input: { kind: 'git-worktree', sourceCwd: task.sourceCwd, base }, destination: cwd }, signal);
    await this.persistWorkspaceMaterializationStart(task, signal);
    await mkdir(parent, { recursive: true, mode: 0o700 });
    const add = await runProcess({ command: 'git', args: ['worktree', 'add', '--detach', '--', cwd, base], cwd: task.sourceCwd, signal, owner: task.id, store: this.store, semaphore: this.commands, timeoutMs: 60_000, env: workspaceGitEnv() });
    if (add.exitCode !== 0) throw fail(`Worktree creation failed; inspect artifacts ${add.stdout.id}, ${add.stderr.id}`, 'WORKSPACE');
    await this.mutateInternal(async () => { task.cwd = cwd; task.worktree = { path: cwd, base, source: 'snapshot fixed at submit; later HEAD is refused' }; delete task.worktreeIntent; await this.persist(task); this.admission.markUncertain(task.id, !automaticRecoverySafe(task)); });
  }
  async execute(task, running) {
    // jobTimeoutMs remains inert. The root execution-time snapshot is durable
    // across slices/retries and is distinct from a one-shot request timeout.
    const workflowTimeout = createWorkflowExpiryTimer(task, reason => running.abort.abort(reason));
    const root = rootBudgetDecision(task);
    const rootTimeout = root.allowed ? createExecutionBudgetTimer(root.remainingMs, reason => running.abort.abort(reason)) : undefined;
    let workerStarted = false;
    try {
      if (!root.allowed) running.abort.abort(fail(root.reason, 'ROOT_BUDGET'));
      running.abort.signal.throwIfAborted();
      if (task.worktreeIntent && !task.worktree) throw fail('Uncertain worktree creation. Inspect worktreeIntent in task metadata; do not replay blindly.', 'WORKSPACE');
      await this.prepareResultWorkspace(task, running.abort.signal);
      await this.prepareWorkspace(task, running.abort.signal);
      if (task.resultContract === 'code-change' && !task.productBase) {
        const base = await captureProductSnapshot(this.store, task, { signal: running.abort.signal });
        await this.mutateInternal(async () => { running.abort.signal.throwIfAborted(); task.productBase = base; await this.persist(task); });
      }
      const output = task.workItem?.execution === 'artifact-sha256'
        ? await this.digestPinnedArtifact(task, running.abort.signal)
        : await this.runner.run(jsonClone(task), running.abort.signal, {
          executorStarted: identity => {
            workerStarted = true;
            return this.mutateInternal(async () => {
              running.abort.signal.throwIfAborted();
              this.admission.registerExecutor(task.id, identity);
              task.executionIntent.executor = identity; await this.persist(task);
            });
          },
          send: p => this.send(task.id, p), inbox: () => this.takeMail(task.id, false), takeSteering: () => this.takeMail(task.id, true),
          checkpoint: summary => this.mutateInternal(async () => { task.handoff = summary; await this.persist(task); }),
          reserveUsage: options => this.mutateInternal(async () => {
            if (options && (options.kind !== 'compaction' || Object.keys(options).some(key => !['kind', 'operationId'].includes(key)))) throw fail('Invalid usage reservation', 'PERMISSION');
            if (options) identifier(options.operationId, 'compaction operation');
            running.abort.signal.throwIfAborted();
            const budget = rootBudgetDecision(task);
            if (!budget.allowed) { running.abort.abort(fail(budget.reason, 'ROOT_BUDGET')); throw running.abort.signal.reason; }
            if (task.requestPermitUncertain) throw fail('Provider permit acknowledgement is uncertain; inspect the shared ledger before continuation', 'UNCERTAIN');
            if (!options || task.executionIntent.usageOperation !== options.operationId) await this.auditUsageUncertainty(task);
            running.abort.signal.throwIfAborted();
            if (this.running.get(task.id) !== running || task.state !== 'running' || task.cancelRequested || task.authorityRevoked) throw fail('Provider executor fence is no longer current', 'STALE');
            authorizeModes(task.role, task.profile, this.modes, task.modeBindings ?? []);
            this.assertRequiredResultInputs(task);
            this.renewMemoryBoundary(task, true);
            if (task.usageUncertain && options && task.executionIntent.usageOperation === options.operationId) {
              task.executionIntent.usageOperationUncertain = true;
              task.executionIntent.usageOperationFirstRequestId ??= task.executionIntent.activeRequestId;
            }
            const requestId = randomUUID();
            // Persist the request identity before touching the separate shared ledger.
            // A crash after permit acquisition must leave a reconcilable task intent,
            // never an orphan claim that appears safe to replay.
            task.executionIntent.usageOperation = options?.operationId; task.executionIntent.activeRequestId = requestId;
            task.executionIntent.requestBoundary = {
              requestId, kind: options ? 'compaction' : 'model', checkpointSha256: this.checkpointSha256(task.checkpoint),
              inputRevision: inputRevision(task), authorityRevision: authorityRevision(task), snapshotRevision: snapshotRevision(task),
              modelProfileFingerprint: modelProfileFingerprint(task),
              dependencyInputsSha256: hash(encodeJson(task.dependencyInputs ?? [], this.limits.checkpointBytes)),
            };
            task.lastRequestReceipt = { id: requestId, outcome: 'unknown', observedAt: Date.now() };
            task.requestPermitUncertain = { requestId, observedAt: Date.now() };
            task.usageUncertain = true; await this.persist(task);
            let permitGranted = false;
            try {
              this.assertRequiredResultInputs(task);
              const permit = this.admission.requestPermit(task, requestId); permitGranted = true;
              task.executionIntent.requestQuotaProbe = permit?.probe === true;
              delete task.requestPermitUncertain;
              commitTurns(task, (task.executionIntent.reportedTurns ?? 0) + 1);
              await this.persist(task);
              // Durable writes yield to cancellation. Recheck before returning
              // the runtime-owned permit to the worker, not just at admission.
              this.assertRequiredResultInputs(task);
              return { requestId: task.executionIntent.activeRequestId };
            } catch (error) {
              if (error.code === 'BLOCKED_INPUT') {
                error.requestOutcome = 'rejected_before_execution';
                if (permitGranted) this.admission.settlePermit(task, requestId, 'rejected_before_execution');
              }
              if (['QUOTA_PERMIT_HELD', 'BLOCKED_INPUT'].includes(error.code) && error.requestOutcome === 'rejected_before_execution') {
                delete task.requestPermitUncertain;
                task.usageUncertain = task.executionIntent.usageOperationUncertain === true;
                task.lastRequestReceipt = { id: task.usageUncertain ? task.executionIntent.usageOperationFirstRequestId : requestId, outcome: task.usageUncertain ? 'unknown' : 'rejected_before_execution', observedAt: Date.now() };
                if (task.usageUncertain) task.executionIntent.activeRequestId = task.executionIntent.usageOperationFirstRequestId;
                else { delete task.executionIntent.usageOperation; delete task.executionIntent.activeRequestId; }
                await this.persist(task);
              }
              else task.requestPermitUncertain = { requestId, observedAt: Date.now() };
              throw error;
            }
          }),
          settleUsage: (usage, metadata = {}) => this.mutateInternal(async () => {
            if (!metadata || typeof metadata !== 'object' || Array.isArray(metadata) || Object.keys(metadata).some(key => !['outcome', 'observedAt', 'providerRequestId'].includes(key))) throw fail('Invalid usage receipt metadata', 'STORAGE');
            const outcome = metadata.outcome ?? 'completed';
            if (!['rejected_before_execution', 'completed', 'unknown'].includes(outcome)) throw fail('Invalid usage request outcome', 'STORAGE');
            const observation = usage?.state ? assertUsageState(usage) : usageStateFrom(usage, { outcome, observedAt: metadata.observedAt ?? Date.now() });
            for (const key of ['input', 'output', 'cacheRead', 'cacheWrite', 'cost']) {
              const prior = task.executionIntent.reportedUsage?.[key], amount = observation[key];
              // Missing accounting is not an execution failure. Keep the last known
              // cumulative value; reject only a measured decrease, never invent zero.
              if (Number.isFinite(prior) && Number.isFinite(amount) && amount < prior) throw fail('Incomplete or regressing usage receipt', 'STORAGE');
            }
            const unsettledOperation = outcome !== 'completed' && task.executionIntent.usageOperationUncertain === true;
            const receiptOutcome = unsettledOperation ? 'unknown' : outcome;
            const requestId = (unsettledOperation ? task.executionIntent.usageOperationFirstRequestId : task.executionIntent.activeRequestId) ?? task.lastRequestReceipt?.id ?? task.executionIntent.id;
            commitUsage(task, running, observation, receiptOutcome);
            task.lastRequestReceipt = { id: requestId, outcome: receiptOutcome, observedAt: metadata.observedAt ?? Date.now() };
            task.usageUncertain = receiptOutcome === 'unknown';
            if (receiptOutcome !== 'unknown') { delete task.executionIntent.usageOperation; delete task.executionIntent.activeRequestId; delete task.executionIntent.usageOperationUncertain; delete task.executionIntent.usageOperationFirstRequestId; }
            await this.auditUsageUncertainty(task);
            await this.persist(task);
            if (outcome === 'completed' || receiptOutcome === 'unknown') this.admission.settlePermit(task, requestId, receiptOutcome);
            this.admission.markUncertain(task.id, !automaticRecoverySafe(task));
          }),
          saveCheckpoint: (checkpoint, transcript) => this.mutateInternal(async () => {
            const previousSections = task.checkpoint?.resultSections;
            task.checkpoint = checkpoint; task.transcript = transcript;
            const confirmed = new Set(checkpoint.completedToolCallIds ?? []);
            task.pendingEffects = (task.pendingEffects ?? []).filter(step => !confirmed.has(step.toolCallId) || !(step.outcome === 'completed' || isKnownFailedEffect(step)));
            await this.persist(task); this.admission.markUncertain(task.id, !automaticRecoverySafe(task));
            const sections = checkpoint.resultSections, stamp = `${task.resultEpoch ?? 1}:${sections?.head}`;
            if (sections?.sections?.length && workflowAdapter(task).id === 'generic' && !task.workItem && task.sectionPublication?.stamp !== stamp && (task.continuationRequested || previousSections?.head !== sections.head)) {
              const assertCurrent = () => {
                running.abort.signal.throwIfAborted();
                if (this.running.get(task.id) !== running || task.state !== 'running' || task.cancelRequested || task.authorityRevoked) throw fail('Stale section publisher', 'STALE');
                authorizeModes(task.role, task.profile, this.modes, task.modeBindings ?? []);
              };
              assertCurrent();
              const head = resultHead(this.store, task), publicationId = `sections-${hash(stamp)}`;
              const committed = head && this.store.metadata.get('result_events', this.store.scope, hash(`${head.bundleId}\0${publicationId}`));
              if (committed) {
                task.sectionPublication = { stamp }; task.bundleRef = { agentId: task.id, bundleId: head.bundleId, revision: head.revision, resultEpoch: head.resultEpoch };
              } else {
                const primary = await persistSectionPrimary(this.store, task.id, sections, this.limits.previewBytes);
                const bundleRef = await publishResultRevision(this.store, task, { expectedHead: head?.revision ?? 0, stage: 'progress', files: [{ logicalPath: 'main.md', kind: 'main_report', contentRef: primary.resultRef, fileComplete: sections.complete }] }, { publicationId, attemptId: task.executionIntent.id, assertCurrent });
                task.sectionPublication = { stamp }; task.bundleRef = bundleRef;
              }
              await this.persist(task); this.kick();
            }
          }),
          readResult: request => this.mutateInternal(async () => {
            const assertCurrent = () => {
              running.abort.signal.throwIfAborted();
              if (this.running.get(task.id) !== running || task.state !== 'running' || task.cancelRequested || task.authorityRevoked || task.workItem || !task.role.tools.includes('agent_read')) throw fail('Result reader is not an active granted executor', 'PERMISSION');
              authorizeModes(task.role, task.profile, this.modes, task.modeBindings ?? []);
            };
            assertCurrent();
            const grant = request.inputBindingId?.startsWith('rd-') ? this.resultNotifications.grant(task, request) : this.resultInputs.grant(task, request);
            return readGrantedResult(this.store.root, request, grant, assertCurrent);
          }),
          publishResult: (request, toolCallId) => this.mutateInternal(async () => {
            const assertCurrent = () => {
              running.abort.signal.throwIfAborted();
              if (this.running.get(task.id) !== running || task.state !== 'running' || task.cancelRequested || task.authorityRevoked) throw fail('Stale result publication executor', 'STALE');
              if (task.workItem || !task.role.tools.includes('agent_result')) throw fail('Result reporting capability is not granted', 'PERMISSION');
              authorizeModes(task.role, task.profile, this.modes, task.modeBindings ?? []);
            };
            assertCurrent(); text(toolCallId, 'publication tool call', 256);
            const attemptId = identifier(task.executionIntent?.id, 'publication attempt');
            const bundleRef = await this.publishTaskResult(task, request, { attemptId, publicationId: `p-${hash(`${attemptId}\0${toolCallId}`)}`, assertCurrent, signal: running.abort.signal });
            task.bundleRef = bundleRef; await this.persist(task); this.kick(); return bundleRef;
          }),
          shouldHold: () => this.mutateInternal(async () => {
            running.abort.signal.throwIfAborted();
            if (this.running.get(task.id) !== running || task.cancelRequested || task.authorityRevoked) throw fail('Stale boundary executor', 'CANCELLED');
            if (this.connectionHeld && task.profile.allowBackground !== true) return 'Foreground client disconnected';
            if (this.pressure || this.foregroundPressure || this.systemMemoryVictim === task.id) return { kind: 'system_memory', reason: 'Runtime or foreground memory pressure' };
            const decision = this.renewMemoryBoundary(task);
            if (!decision.admitted) return { kind: 'system_memory', reason: `Memory growth renewal held: ${decision.reason}` };
            return undefined;
          }),
          beginEffect: effect => this.mutateInternal(async () => {
            running.abort.signal.throwIfAborted();
            if (this.running.get(task.id) !== running || task.state !== 'running' || task.cancelRequested || task.authorityRevoked) throw fail('Stale tool executor', 'STALE');
            this.assertRequiredResultInputs(task);
            this.renewMemoryBoundary(task, true);
            const prior = task.pendingEffects?.find(step => replayableEffect(step) && step.toolCallId === effect.toolCallId && step.tool === effect.tool && step.inputDigest === effect.inputDigest);
            if (prior) return { stepId: prior.stepId, replayRef: prior.resultRef };
            if (effect.kind !== 'read' && (task.pendingEffects ?? []).some(step => effectRequiresReconciliation(step) && !(step.outcome === 'completed' && step.attemptId === task.executionIntent.id))) throw fail('An unresolved tool effect blocks new mutations; bounded read diagnostics remain available', 'UNCERTAIN');
            const step = { ...effect, stepId: randomUUID(), attemptId: task.executionIntent.id, outcome: 'intent', createdAt: Date.now() };
            delete step.productSubject; delete step.productAfter;
            if (effect.tool === 'bash' && task.productInput) {
              const before = await captureProductSnapshot(this.store, task, { signal: running.abort.signal });
              if (before.productSha256 !== task.productInput.productSha256) throw fail('Checker workspace no longer matches its pinned product input', 'STALE');
              step.productSubject = task.productInput;
            }
            const artifact = await this.store.artifact(task.id, 'tool-effect-intent');
            await artifact.write(encodeJson(step)); await artifact.finish(true); step.intentRef = artifact.info.id;
            this.store.metadata.put('steps', this.store.scope, { ...step, id: step.stepId, agentId: task.id, state: 'intent' });
            task.pendingEffects ??= []; task.pendingEffects.push(step); await this.persist(task); this.admission.markUncertain(task.id, !automaticRecoverySafe(task));
            this.assertRequiredResultInputs(task);
            return { stepId: step.stepId };
          }),
          finishEffect: receipt => this.mutateInternal(async () => {
            const step = task.pendingEffects?.find(row => row.stepId === receipt.stepId);
            if (!step || step.attemptId !== task.executionIntent?.id) throw fail('Stale effect receipt', 'STALE');
            if (!['completed', 'failed', 'uncertain'].includes(receipt.outcome)) throw fail('Invalid effect receipt outcome', 'STORAGE');
            const identity = await this.store.artifactIdentity(receipt.resultRef?.artifactId);
            const info = receipt.resultRef && await this.store.artifactInfo(receipt.resultRef.artifactId);
            if (!identity?.complete || identity.owner !== task.id || identity.bytes !== receipt.resultRef.bytes || identity.sha256 !== receipt.resultRef.sha256 || receipt.resultRef.complete !== true || info.label !== 'tool-effect-outcome') throw fail('Effect receipt identity mismatch', 'STORAGE');
            const body = await readJson(join(this.store.root, 'artifacts', `${receipt.resultRef.artifactId}.bin`), this.store.limits.responseBytes + this.store.limits.inputBytes);
            if (!body || typeof body !== 'object' || Array.isArray(body)) throw fail('Effect receipt body is invalid', 'STORAGE');
            if (receipt.outcome === 'completed' && (!Object.hasOwn(body, 'result') || Object.hasOwn(body, 'error'))) throw fail('Completed effect receipt has no result', 'STORAGE');
            if (receipt.outcome !== 'completed' && (typeof body?.error !== 'string' || !body.error || typeof body.code !== 'string' || !body.code)) throw fail('Failed effect receipt has no error', 'STORAGE');
            if (receipt.outcome === 'failed') {
              const failure = await verifyKnownToolFailure(this.store, task.id, receipt.failure);
              if (JSON.stringify(body.failure) !== JSON.stringify(failure)) throw fail('Known failure receipt changed', 'STORAGE');
            } else if (body.failure || receipt.failure) throw fail('Uncertain or successful effect cannot claim a known failure receipt', 'STORAGE');
            Object.assign(step, receipt, { finishedAt: Date.now(), resultRef: { ...receipt.resultRef, sha256: identity.sha256 } });
            if (step.tool === 'bash' && task.productInput) {
              step.productSubject = task.productInput;
              delete step.productAfter;
              try { step.productAfter = await captureProductSnapshot(this.store, task, { signal: running.abort.signal }); }
              catch (error) { step.productObservationError = safeError(error); }
            }
            this.store.metadata.put('steps', this.store.scope, { ...step, id: step.stepId, agentId: task.id, state: receipt.outcome });
            if (receipt.outcome === 'completed') task.lastEffectProgress = hash(`${step.tool}\0${step.inputDigest}\0${identity.sha256}`);
            if (await recordExecutionEvidence(this.store, task, step)) task.executionReadiness = executionReadiness(this.store.metadata.database, this.store.scope, task.id, task.resultEpoch ?? 1);
            await this.persist(task); return { stepId: step.stepId };
          }),
          progress: p => { running.progress = { ...running.progress, ...p, ...(p?.usage ? { usage: pendingUsage(task, p.usage) } : {}) }; },
          beginWeb: (kind, input) => this.beginWeb(task.id, kind, input),
          finishWeb: (kind, attempt, receipt) => this.finishWeb(task.id, kind, attempt, receipt),
        });
      await this.mutateInternal(async () => {
        if (running.abort.signal.aborted || task.cancelRequested || task.authorityRevoked || ['cancelled', 'interrupted'].includes(task.state)) {
          commitUsage(task, running, output.usage ?? running.progress.usage); commitTurns(task, output.turns);
          await this.auditUsageUncertainty(task);
          if (task.usageOnlyTransportRetry?.state === 'active') task.usageOnlyTransportRetry.state = 'exhausted';
          if (output.checkpoint) task.checkpoint = output.checkpoint; if (output.transcript) task.transcript = output.transcript;
          settleRootSegment(task, { intentId: task.executionIntent?.id, startedAt: task.executionIntent?.startedAt, progress: rootProgressMarker(task) });
          const reason = running.abort.signal.reason;
          if (task.cancelRequested || task.authorityRevoked || reason?.code === 'CANCELLED' || reason?.code === 'AUTHORITY_REVOKED') { task.state = 'cancelled'; task.error = 'Cancelled or authority revoked before the result could be applied'; }
          else if (reason?.code === 'ROOT_BUDGET') { task.state = 'paused'; task.error = 'Root execution-time budget exhausted; usage and checkpoint retained'; }
          else { task.state = 'interrupted'; task.error = 'Execution interrupted without durable cancellation; inspect before retry'; }
          task.currentReport = null; task.acceptedReport = null; task.acceptedUse = null;
          await this.persist(task); await this.completionMail(task); return;
        }
        const workflow = workflowAdapter(task);
        workflow.assertFreshResult(task, this.tasks, this.running);
        const result = workflow.acceptResult(output.result);
        task.result = short(result, 24 * 1024); task.resultRef = output.resultRef; task.checkpoint = output.checkpoint; task.transcript = output.transcript;
        if (!task.resultRef) {
          const artifact = await this.store.artifact(task.id, 'agent-result-text');
          try { const body = Buffer.from(result, 'utf8'); for (let offset = 0; offset < body.length; offset += 65536) await artifact.write(body.subarray(offset, offset + 65536)); const sealed = await artifact.finish(true); task.resultRef = { artifactId: sealed.id, bytes: sealed.bytes, sha256: sealed.sha256, complete: true }; }
          catch (error) { await artifact.finish(false, safeError(error)).catch(() => {}); throw error; }
        }
        task.resultRef = await verifyResult(this.store, task.id, task.resultRef);
        if (task.qualityContract && output.state === 'idle') task.quality = bindQualityReceipt(output.quality, task.qualityContract, task.resultRef, task);
        commitUsage(task, running, output.usage ?? running.progress.usage);
        commitTurns(task, output.turns);
        await this.auditUsageUncertainty(task);
        this.assertResultCurrent(task);
        task.state = output.state;
        if (task.qualityContract && output.state === 'idle' && task.quality.state !== 'candidate_ready') { task.state = 'blocked'; task.qualityFailure = true; task.error = 'Quality criteria are unmet or inconclusive; inspect evidence and replan before explicit resume'; }
        if (task.usageOnlyTransportRetry?.state === 'active') task.usageOnlyTransportRetry.state = 'exhausted';
        settleRootSegment(task, { intentId: task.executionIntent?.id, startedAt: task.executionIntent?.startedAt, progress: rootProgressMarker(task), trackProgress: !['worker_memory', 'system_memory'].includes(output.continuation?.kind) });
        delete task.providerFault; delete task.quotaHold; delete task.resumeCapsule; task.holdReasons = [];
        if (task.rootBudget.maxTurns !== null && task.turns > task.rootBudget.maxTurns || task.rootBudget.maxExecutionMs !== null && task.rootBudget.executionMs > task.rootBudget.maxExecutionMs) {
          task.state = 'paused'; task.error = 'Reported execution exceeded the accepted root budget; original result retained but not accepted as success'; task.continuationRequested = false;
          await this.completionMail(task);
          if (task.workItem) { const attempt = task.attempts.at(-1); attempt.state = 'paused'; attempt.finishedAt = Date.now(); attempt.reportArtifactId = task.currentReport; await this.persist(task); }
          return;
        }
        const continuation = workflow.allowContinuation && continuationDecision(task, output);
        if (continuation?.continue) {
          if (['worker_memory', 'system_memory'].includes(continuation.kind)) task.memoryMaintenance = { kind: continuation.kind, progress: rootProgressMarker(task), at: Date.now(), growth: Boolean(task.memoryGrowthHold) };
          else delete task.memoryMaintenance;
          task.continuationRequested = true; task.heldForConnection = continuation.kind === 'connection' && Boolean(this.connectionHeld) && task.profile.allowBackground !== true;
          task.state = task.heldForConnection ? 'paused' : 'queued'; task.currentReport = null; delete task.executionIntent;
          if (task.workItem) { const attempt = task.attempts.at(-1); attempt.state = 'segment'; attempt.finishedAt = Date.now(); }
          await this.persist(task); return;
        }
        if (continuation?.reason) { task.state = 'paused'; task.error = continuation.reason; }
        task.continuationRequested = false;
        if (task.state === 'idle') { task.delivering = []; task.instruction = ''; }
        // completionMail publishes the immutable report before committing terminal
        // task state, so a report-write crash cannot leave durable false success.
        await this.completionMail(task);
        if (task.workItem) { const attempt = task.attempts.at(-1); attempt.state = task.state === 'idle' ? 'candidate' : task.state; attempt.finishedAt = Date.now(); attempt.reportArtifactId = task.currentReport; await this.persist(task); }
        if (!task.workItem && task.state === 'idle' && this.mail.hasWakeMail(task.id) && (this.limits.autoWakeLimit === null || task.autoWakes < this.limits.autoWakeLimit) && !this.stopping) { advanceResultEpoch(task); task.autoWakes++; task.state = 'queued'; task.currentReport = null; delete task.executionIntent; await this.persist(task); }
      });
    } catch (error) {
      if (this.fatal) throw error; // fail closed; never re-enter failed response/storage persistence
      await this.mutateInternal(async () => {
        // Selection failed before SDK execution. Completed workspace commands
        // can leave an executor record; confirm every preparation process is
        // stopped. A started SDK worker can never claim this readmission path.
        let admissionStale = error.code === 'WORKER_ADMISSION_STALE' && error.requestOutcome === 'rejected_before_execution'
          && !workerStarted && task.executionIntent && !task.executionIntent.requestBoundary
          && !task.executionIntent.activeRequestId && !task.executionIntent.usageOperation;
        if (admissionStale && task.executionIntent.executor) {
          try { admissionStale = await executorStopped(task.executionIntent.executor); } catch { admissionStale = false; }
          if (!admissionStale) { error.code = 'EXECUTOR_UNCERTAIN'; error.message = 'Preparation executor termination is not confirmed before worker readmission'; }
        }
        const jitterBound = Math.min(50, Math.max(1, this.limits.rateLimitBaseMs));
        const jitterMs = 1 + (Number.parseInt(hash(`${task.id}\0${task.executionIntent?.id ?? ''}`).slice(0, 8), 16) % jitterBound);
        const provider = running.abort.signal.aborted ? null : providerFailureDecision(error, { attempts: 1, limits: this.limits, profile: task.profile, providerBoundary: error.code === 'PROVIDER', jitterMs });
        const transportInterruption = provider?.kind === 'transport_interruption';
        const providerPause = provider && ['rate_limit', 'quota_window', 'quota_unknown', 'billing_required', 'auth_required', 'provider_overload'].includes(provider.kind);
        const requestOutcome = error.requestOutcome ?? provider?.fault?.requestOutcome ?? 'unknown';
        commitUsage(task, running, error.usageState ?? error.usage ?? running.progress.usage, requestOutcome);
        if (error.checkpoint && !transportInterruption) task.checkpoint = error.checkpoint;
        if (error.transcript) task.transcript = error.transcript;
        await this.auditUsageUncertainty(task);
        if (error.code !== 'EXECUTOR_UNCERTAIN') settleRootSegment(task, { intentId: task.executionIntent?.id, startedAt: task.executionIntent?.startedAt, progress: rootProgressMarker(task), trackProgress: error.code !== 'MEMORY_STAGE_HOLD' && !admissionStale });
        let transportEvidence, transportRetry;
        if (transportInterruption) {
          transportEvidence = await this.usageOnlyTransportRetryEvidence(task, error, provider, running);
          transportRetry = usageOnlyTransportRetryDecision(task, transportEvidence);
          if (transportEvidence.checkpointConsistent) task.checkpoint = error.checkpoint;
          const policy = task.usageOnlyTransportRetry ??= createUsageOnlyTransportRetryPolicy();
          assertUsageOnlyTransportRetryPolicy(policy);
          const boundary = task.executionIntent?.requestBoundary;
          const transcriptArtifactId = typeof error.transcript === 'string' && /^[a-zA-Z0-9][a-zA-Z0-9_.:/-]{0,255}$/.test(error.transcript) ? error.transcript : undefined;
          if (policy && boundary?.requestId && /^[a-zA-Z0-9][a-zA-Z0-9_.:/-]{0,255}$/.test(boundary.requestId) && boundary.checkpointSha256) {
            if (!policy.interruptions.some(row => row.requestId === boundary.requestId) && policy.interruptions.length < 2) policy.interruptions.push({ requestId: boundary.requestId, observedAt: provider.fault.observedAt, usageRisk: 'unknown', checkpointSha256: boundary.checkpointSha256, diagnosticType: provider.fault.diagnosticType, ...(transcriptArtifactId ? { transcriptArtifactId } : {}) });
            policy.retries = 1; policy.state = transportRetry.retry ? 'pending' : 'exhausted';
          }
        }
        if (!transportInterruption && task.usageOnlyTransportRetry?.state === 'active') task.usageOnlyTransportRetry.state = admissionStale ? 'pending' : 'exhausted';
        const abortReason = running.abort.signal.reason; let publishCompletion = true;
        if (error.code === 'EXECUTOR_UNCERTAIN') {
          task.recoveryPending = true; this.recoveringExecutors.add(task.id);
          task.state = task.cancelRequested || task.authorityRevoked ? 'cancelled' : 'interrupted'; task.error = safeError(error); task.currentReport = null;
          publishCompletion = false; this.scheduleAdmissionWake(Date.now() + 500);
        } else if (transportInterruption) {
          const now = Date.now(), additions = [];
          task.providerFault = provider.fault; task.currentReport = null; task.acceptedReport = null; task.acceptedUse = null;
          task.continuationRequested = Boolean(task.checkpoint?.records?.length || task.checkpoint?.historyRef || task.checkpoint?.compactionEnvelope || task.checkpoint?.resultSections);
          if (task.cancelRequested || task.authorityRevoked) {
            task.state = 'cancelled'; task.error = 'Cancellation or authority revocation is durable'; delete task.executionIntent;
          } else if (transportRetry?.retry) {
            task.state = 'queued'; task.error = undefined; task.holdReasons = []; delete task.resumeCapsule; delete task.quotaHold; delete task.executionIntent; publishCompletion = false;
          } else {
            if (this.pressure || this.foregroundPressure || this.systemPressure) additions.push(holdReason({ id: 'runtime-memory', kind: 'memory', source: 'runtime', since: now, detail: 'Runtime memory/admission pressure prevents automatic transport retry.' }));
            if (this.connectionHeld && task.profile.allowBackground !== true) additions.push(holdReason({ id: 'foreground-connection', kind: 'connection', source: 'runtime', since: now, detail: 'Foreground connectivity is required by the pinned profile.' }));
            if ((task.pendingEffects ?? []).some(effectRequiresReconciliation)) additions.push(holdReason({ id: 'effect-outcome', kind: 'effect', source: 'recovery', since: now, detail: 'A tool effect outcome remains uncertain; no model retry was sent.' }));
            task.state = 'paused'; task.error = `${provider.reason} Automatic retry refused: ${transportRetry?.reason ?? 'missing eligibility evidence'}`;
            task.holdReasons = continuationHoldReasons(mergeHoldReasons(task.holdReasons ?? [], additions)); delete task.executionIntent; delete task.quotaHold;
            task.resumeCapsule = createResumeCapsule(task, { fault: provider.fault, holdReasons: task.holdReasons, requestOutcome: 'unknown', now });
          }
        } else if (providerPause) {
          const account = task.profile.account ?? task.profile.provider, prior = this.quotaPools.get(account), failures = (prior?.failures ?? 0) + 1;
          // A later native-compaction subrequest rejection cannot settle earlier
          // subrequests whose operation checkpoint/receipt is still outstanding.
          const priorUncertain = task.executionIntent?.usageOperationUncertain === true;
          const receiptId = (priorUncertain ? task.executionIntent.usageOperationFirstRequestId : task.executionIntent?.activeRequestId) ?? task.lastRequestReceipt?.id ?? task.executionIntent?.id;
          const aggregateOutcome = priorUncertain ? 'unknown' : provider.fault.requestOutcome;
          if (receiptId) task.lastRequestReceipt = { id: receiptId, outcome: aggregateOutcome, observedAt: provider.fault.observedAt };
          task.usageUncertain = aggregateOutcome !== 'rejected_before_execution';
          if (!task.usageUncertain && task.executionIntent) { delete task.executionIntent.usageOperation; delete task.executionIntent.activeRequestId; }
          const cooling = provider.kind === 'provider_overload' && Number.isSafeInteger(provider.nextEligibleAt);
          const pool = { held: !cooling, ...(provider.nextEligibleAt === null ? {} : { retryAt: provider.nextEligibleAt }), reason: provider.reason, failures, fault: provider.fault };
          if (error.code !== 'QUOTA_PERMIT_HELD') { this.quotaPools.set(account, pool); this.persistQuotaPool(account, pool, cooling ? 'backoff' : 'held'); if (cooling) this.scheduleAdmissionWake(provider.nextEligibleAt); }
          if (receiptId && error.code !== 'QUOTA_PERMIT_HELD' && (!task.usageUncertain || provider.kind === 'provider_overload')) this.admission.settlePermit(task, receiptId, aggregateOutcome);
          const now = Date.now(), additions = [providerHold(provider.fault, account, now, provider.nextEligibleAt)];
          if (this.pressure || this.foregroundPressure || this.systemPressure) additions.push(holdReason({ id: 'runtime-memory', kind: 'memory', source: 'runtime', since: now, detail: 'Runtime memory/admission pressure must clear before explicit resume.' }));
          if (this.connectionHeld && task.profile.allowBackground !== true) additions.push(holdReason({ id: 'foreground-connection', kind: 'connection', source: 'runtime', since: now, detail: 'Foreground connectivity is required by the pinned profile.' }));
          if ((task.pendingEffects ?? []).some(effectRequiresReconciliation)) additions.push(holdReason({ id: 'effect-outcome', kind: 'effect', source: 'recovery', since: now, detail: 'A tool effect outcome remains uncertain; reconcile before resume.' }));
          task.state = 'paused'; task.currentReport = null; task.providerFault = provider.fault; task.quotaHold = { account, since: now };
          await this.auditUsageUncertainty(task);
          task.error = provider.reason; task.continuationRequested = Boolean(task.checkpoint?.records?.length || task.checkpoint?.historyRef || task.checkpoint?.compactionEnvelope || task.checkpoint?.resultSections);
          task.holdReasons = continuationHoldReasons(mergeHoldReasons(task.holdReasons ?? [], additions)); delete task.executionIntent;
          task.resumeCapsule = createResumeCapsule(task, { fault: provider.fault, holdReasons: task.holdReasons, requestOutcome: aggregateOutcome, now });
        } else if (error.code === 'BLOCKED_INPUT' && !running.abort.signal.aborted && !task.cancelRequested && !task.authorityRevoked) {
          task.state = 'blocked'; task.error = safeError(error); task.continuationRequested = false;
        } else if (admissionStale && !running.abort.signal.aborted && !task.cancelRequested && !task.authorityRevoked) {
          const budget = rootBudgetDecision(task), safe = automaticRecoverySafe(task);
          let readmit = budget.allowed && safe, capsule, capsuleError;
          if (readmit && task.executionIntent.resumeCapsuleSha256) {
            try {
              if (hash(encodeJson(task.lastResumeCapsule, 32 * 1024)) !== task.executionIntent.resumeCapsuleSha256) throw fail('Resume capsule identity changed before worker readmission', 'STALE');
              capsule = assertCapsuleBinding(task, task.lastResumeCapsule);
            } catch (error) { readmit = false; capsuleError = safeError(error); }
          }
          task.state = readmit ? 'queued' : 'paused';
          task.error = readmit ? undefined : capsuleError ?? budget.reason ?? 'Uncertain permit, workspace or effect requires reconciliation before readmission';
          // Preserve prior maintenance/crash counters, checkpoints, permissions
          // and continuation state. Fresh host admission recomputes cold cost.
          if (readmit) { if (capsule) task.resumeCapsule = capsule; delete task.executionIntent; task.currentReport = null; publishCompletion = false; }
        } else if (error.code === 'MEMORY_STAGE_HOLD') {
          const decision = continuationDecision(task, { continuation: { kind: 'system_memory' } });
          task.state = decision.continue ? 'queued' : 'paused'; task.error = decision.reason ?? error.message;
          task.continuationRequested = Boolean(decision.continue);
          task.memoryMaintenance = { kind: 'system_memory', progress: rootProgressMarker(task), at: Date.now(), growth: true };
          if (decision.continue) { delete task.executionIntent; task.currentReport = null; publishCompletion = false; }
        } else if (error.code === 'WORKER_MEMORY_EMERGENCY') {
          task.state = task.cancelRequested || task.authorityRevoked ? 'cancelled' : 'paused';
          task.error = 'Worker memory emergency: only the last confirmed checkpoint is recoverable; inspect uncertain effects before explicit resume. Unknown usage is retained as accepted audit.';
          task.continuationRequested = false;
          task.memoryMaintenance = { kind: 'worker_memory', emergency: true, at: Date.now() };
        } else if (error.code === 'WORKER_EXIT' && !running.abort.signal.aborted) {
          const decision = recoveryDecision(task, { executorStopped: error.executorStopped === true });
          task.state = decision.state; task.recoveryAttempts = (task.recoveryAttempts ?? 0) + 1; task.error = decision.reason;
          if (decision.retry) { delete task.executionIntent; if (task.checkpoint?.records?.length) task.continuationRequested = true; publishCompletion = false; }
        } else if (running.abort.signal.aborted) {
          if (task.cancelRequested || task.authorityRevoked || abortReason?.code === 'CANCELLED' || abortReason?.code === 'AUTHORITY_REVOKED') { task.state = 'cancelled'; task.error = 'Cancellation or authority revocation is durable'; }
          else if (abortReason?.code === 'ROOT_BUDGET') { task.state = 'paused'; task.error = 'Root execution-time budget exhausted; usage and checkpoint retained'; }
          else if (workflowAdapter(task).authorityFailure(abortReason)) { const failure = workflowAdapter(task).authorityFailure(abortReason); task.authorityRevoked = failure.revoked; task.state = 'cancelled'; task.error = failure.reason; }
          else { task.state = 'interrupted'; task.error = safeError(abortReason ?? error); }
        } else { task.state = 'failed'; task.error = safeError(error); if (error.code === 'QUALITY') task.qualityFailure = true; }
        await this.persist(task); if (publishCompletion) await this.completionMail(task);
        if (task.workItem) { const attempt = task.attempts.at(-1); attempt.state = task.state; attempt.finishedAt = Date.now(); attempt.reportArtifactId = task.currentReport; await this.persist(task); }
      });
    } finally { if (workflowTimeout) clearTimeout(workflowTimeout); rootTimeout?.cancel(); }
  }
  async beginWeb(id, kind, input) {
    return this.mutateInternal(async () => {
      const task = this.get(id);
      if (task.workItem) throw fail('Universal inspection cannot call network web tools', 'UNSUPPORTED');
      if (!workflowAdapter(task).allowWeb) throw fail('Workflow web calls are disabled', 'MODE');
      if (this.stopping || task.state !== 'running' || !this.running.has(id)) throw fail('Web task is not running', 'CLOSED');
      this.running.get(id).abort.signal.throwIfAborted();
      assertWebCall(task, kind, input);
      const attempt = task.webUsed[kind] + 1;
      if (!Number.isSafeInteger(attempt) || !Number.isSafeInteger(task.webUsed.search + task.webUsed.fetch + 1)) throw fail('Web accounting integer overflow', 'LIMIT');
      const reservationArtifactId = await this.writeWebArtifact(task, `web-reservation-${kind}-${attempt}`, { kind, attempt, state: 'pending', input });
      task.webUsed[kind] = attempt;
      task.webReceipts.push({ kind, attempt, state: 'pending', reservationArtifactId });
      task.webReceipts = task.webReceipts.slice(-WEB_RECEIPT_INDEX);
      await this.persist(task); return attempt;
    });
  }
  async finishWeb(id, kind, attempt, receipt) {
    return this.mutateInternal(async () => {
      const task = this.get(id), entry = task.webReceipts?.find(row => row.kind === kind && row.attempt === attempt);
      if (!entry || entry.state !== 'pending') throw fail('Invalid web receipt reservation', 'CONFLICT');
      entry.artifactId = await this.writeWebArtifact(task, `web-${kind}-${attempt}`, receipt);
      entry.state = receipt.outcome.ok ? 'succeeded' : 'failed';
      await this.persist(task); return jsonClone(entry);
    });
  }
  async writeWebArtifact(task, label, value) {
    let artifact;
    try {
      const bytes = encodeJson(value, WEB_RESULT_BYTES + 16384);
      artifact = await this.store.artifact(task.id, label);
      await artifact.write(bytes); await artifact.finish(true);
      return artifact.info.id;
    } catch (error) {
      if (artifact && !artifact.closed) await artifact.finish(false).catch(() => {});
      this.fatal ??= 'Web receipt persistence failed; network outcome may be uncertain'; this.stopping = true;
      for (const run of this.running.values()) run.abort.abort(error);
      throw fail(this.fatal, 'STORAGE');
    }
  }
  async saveResponse(task, type, value, metadata, assertCurrent) {
    try {
      const entry = await writeResponse(this.store, task.id, type, value, metadata, assertCurrent);
      assertCurrent?.();
      retainResponse(task, entry);
      if (type === 'report') task.currentReport = entry.artifactId;
      await this.persist(task); assertCurrent?.(); return entry;
    } catch (error) {
      if (assertCurrent && (error === this.running.get(task.id)?.abort.signal.reason || error.code === 'CANCELLED' && (task.cancelRequested || task.authorityRevoked))) throw error;
      this.fatal ??= `Response persistence failed: ${safeError(error)}`; this.stopping = true;
      task.state = 'failed'; task.currentReport = null; task.error = this.fatal;
      for (const run of this.running.values()) run.abort.abort(error);
      throw fail(this.fatal, 'STORAGE');
    }
  }
  async publishTaskResult(task, request, options) {
    if (task.resultContract !== 'code-change') return publishResultRevision(this.store, task, request, options);
    options.assertCurrent?.();
    const bundleId = resultBundleId(task.id, task.resultEpoch ?? 1);
    const sourceDigest = hash(encodeJson({ request, attemptId: options.attemptId, epoch: task.resultEpoch ?? 1, goalRevision: task.goalRevision ?? 1 }, 64 * 1024));
    const prior = this.store.metadata.get('result_events', this.store.scope, hash(`${bundleId}\0${options.publicationId}`));
    if (prior) { if (prior.sourceDigest !== sourceDigest) throw fail('Publication ID reused with different content', 'CONFLICT'); return prior.bundleRef; }
    const prepared = await prepareCodeChangePublication(this.store, task, request, options);
    return publishResultRevision(this.store, task, prepared.request, { ...options, subject: prepared.subject, generatedPaths: prepared.generatedPaths, sourceDigest });
  }
  async completionMail(task) {
    // Immutable, quota-accounted report is durable even when main's inbox is full.
    const successful = task.state === 'idle';
    const assertCurrent = () => { if (successful) this.assertResultCurrent(task); };
    assertCurrent();
    const entry = await this.saveResponse(task, 'report', { agentId: task.id, state: task.state, savedAt: Date.now(), result: task.result ?? '', resultRef: task.resultRef, error: task.error ?? '', handoff: task.handoff ?? '', workspace: task.cwd, transcriptArtifactId: task.transcript }, { state: task.state }, assertCurrent);
    assertCurrent();
    if (task.state === 'idle' && !task.cancelRequested && !task.authorityRevoked) {
      const head = resultHead(this.store, task);
      const files = [{ logicalPath: 'main.md', kind: 'main_report', contentRef: entry.resultRef, fileComplete: true }];
      if (!head?.outputComplete && task.executionReadiness?.executedCommands) {
        const summary = await evidenceSummaryArtifact(this.store, task);
        task.executionReadiness = summary.readiness;
        files.push({ logicalPath: 'checks/summary.json', kind: 'verification_summary', contentRef: summary.contentRef, fileComplete: true, mediaType: 'application/json' });
      }
      task.bundleRef = head?.outputComplete ? { agentId: task.id, bundleId: head.bundleId, revision: head.revision, resultEpoch: head.resultEpoch } : await this.publishTaskResult(task, { expectedHead: head?.revision ?? 0, stage: 'final', files }, { publicationId: `final-${entry.artifactId}`, attemptId: task.executionIntent?.id ?? entry.artifactId, assertCurrent });
      await this.persist(task);
    }
    assertCurrent();
    const mail = { id: entry.artifactId, from: task.id, to: 'main', kind: 'result', event: 'completion', state: task.state, reportArtifactId: entry.artifactId, delivery: 'followUp', createdAt: Date.now(), expiresAt: Date.now() + 7 * 86_400_000 };
    this.mail.outbox(mail);
  }
  async send(from, params) {
    return this.mutate(async () => {
      if (from !== 'main') this.get(from);
      const to = identifier(params.to, 'recipient'); if (from === to) throw fail('Self-messaging is disabled');
      const target = to === 'main' ? this.main : this.get(to);
      const sender = from === 'main' ? undefined : this.get(from);
      if (target.workItem || sender?.workItem) throw fail('WorkItems cannot send or receive mail; only pinned immutable inputs enter their context', 'UNSUPPORTED');
      if (target.quarantined || (from !== 'main' && this.get(from).quarantined)) throw fail('Quarantined task cannot exchange executable work', 'STORAGE');
      if (from !== 'main') workflowAdapter(this.get(from)).assertMail();
      if (to !== 'main') workflowAdapter(target).assertMail();
      if (from !== 'main') { const sender = this.get(from); authorizeModes(sender.role, sender.profile, this.modes, sender.modeBindings ?? []); }
      if (to !== 'main') {
        const check = from === 'main' ? authorizeMainMutation : authorizeModes;
        check(target.role, target.profile, this.modes, target.modeBindings ?? []);
      }
      const id = identifier(params.id ?? randomUUID(), 'message id');
      const body = text(params.body, 'message body', this.limits.messageBytes);
      const kind = params.kind ?? 'finding'; if (!['question', 'answer', 'finding', 'blocked', 'result'].includes(kind)) throw fail('Invalid message kind');
      const delivery = params.delivery ?? 'followUp'; if (!['steer', 'followUp'].includes(delivery)) throw fail('Invalid delivery');
      if (params.replyTo !== undefined) identifier(params.replyTo, 'replyTo');
      const digest = hash(encodeJson({ from, to, body, kind, delivery, replyTo: params.replyTo }));
      const prior = this.mail.prior(id);
      if (prior) { if (prior.digest !== digest) throw fail('Message ID reused with different content', 'CONFLICT'); return { id, accepted: true, duplicate: true }; }
      const entry = await this.saveResponse(to === 'main' ? this.get(from) : target, 'message', body, { messageId: id, kind, delivery, direction: to === 'main' ? 'outbound' : 'inbound' });
      const mail = { id, from, to, kind, delivery, replyTo: params.replyTo, bodyArtifactId: entry.artifactId, createdAt: Date.now(), expiresAt: Date.now() + 7 * 86_400_000 };
      this.mail.enqueue(mail, digest);
      if (to !== 'main') {
        target.mailbox = await this.mail.pending(to, { limit: Math.min(32, this.limits.mailboxCount), withBody: true });
        target.seen = [...target.seen, { id, digest, bodyArtifactId: entry.artifactId, createdAt: mail.createdAt }].slice(-32);
        if (!target.workItem && target.state === 'idle' && (this.limits.autoWakeLimit === null || target.autoWakes < this.limits.autoWakeLimit) && !this.stopping) { advanceResultEpoch(target); target.state = 'queued'; target.currentReport = null; target.autoWakes++; delete target.executionIntent; }
        await this.persist(target); this.kick();
      }
      return { id, accepted: true, recipientState: target.state ?? 'main', autoWakeBlocked: to !== 'main' && this.limits.autoWakeLimit !== null && target.autoWakes >= this.limits.autoWakeLimit };
    });
  }
  async takeMail(id, steeringOnly) {
    return this.mutateInternal(async () => {
      const task = this.get(id);
      if (task.workItem) throw fail('WorkItem mail delivery is disabled; pinned immutable inputs are the only context channel', 'UNSUPPORTED');
      authorizeModes(task.role, task.profile, this.modes, task.modeBindings ?? []);
      const taken = mailBatch(await this.mail.pending(id, { limit: Math.min(8, this.limits.mailboxCount), steeringOnly, withBody: true }));
      if (taken.length) {
        this.mail.deliver(taken.map(mail => mail.id));
        task.delivering = [...task.delivering, ...taken].slice(-32);
        task.mailbox = await this.mail.pending(id, { limit: Math.min(32, this.limits.mailboxCount), withBody: true });
        await this.persist(task);
      }
      return jsonClone(taken);
    });
  }
  // Project current-attempt identity at the inbox boundary. A report published
  // just before auto-wake is historical, not a new settled completion for main.
  inboxMain() {
    const messages = this.mail.list('main', { limit: 16 }).map(({ digest, ownerId, runId, state, updatedAt, ...mail }) => mail);
    return jsonClone(mailBatch([...this.mail.notifications(16), ...messages], this.limits.mailboxCount).map(mail => {
      if (mail.event !== 'completion') return mail;
      const task = this.tasks.get(mail.from);
      const superseded = task ? task.currentReport !== mail.reportArtifactId : true;
      return { ...mail, superseded, completionReady: !superseded && TERMINAL.has(task.state) && !this.running.has(task.id) && !task.recoveryPending && !this.settlingInputs.has(task.id) };
    }));
  }
  async acknowledge(ids) {
    if (!Array.isArray(ids) || ids.length > 32) throw fail('Invalid acknowledgment'); ids.forEach(id => identifier(id));
    return this.mutate(async () => {
      this.mail.acknowledge(ids);
      return { acknowledged: ids.length };
    });
  }
  async completeReconciliation(task) {
    await this.admission.reconcile(task.reconciliationFence); delete task.reconciliationFence; await this.persist(task);
  }
  async reconcileTask(task, requiredDecision) {
    const confirmation = await readReconciliation(this.config, this.store, task);
    if (!confirmation) throw fail('Uncertain effects/workspace/permit require an exact user-global reconciliation confirmation', 'UNCERTAIN');
    const { grant, effects } = confirmation;
    if (requiredDecision && grant.decision !== requiredDecision) throw fail('Archive confirmation must abandon, never resume, the task', 'PERMISSION');
    const artifact = await this.store.artifact(task.id, 'runtime-reconciliation');
    await artifact.write(encodeJson(grant, 65536)); const receipt = await artifact.finish(true);
    if (grant.expiresAt <= Date.now() || grant.expectedIdentity !== recoveryIdentity(task)) throw fail('Task changed during reconciliation; no authority or lock released', 'STALE');
    const replacements = new Map(effects.map(step => [step.stepId, step]));
    task.pendingEffects = (task.pendingEffects ?? []).map(step => replacements.get(step.stepId) ?? step);
    if (task.usageUncertain && grant.usage) { const { turns, state = 'observed', ...numbers } = grant.usage; task.turns = turns; task.usage = assertUsageState({ state, ...numbers, observedAt: Date.now() }); task.lastUsageState = { ...task.usage }; task.usageUncertain = false; }
    task.reconciliation = { artifactId: receipt.id, sha256: receipt.sha256, decision: grant.decision };
    task.reconciliationFence = task.admissionFence;
    if (grant.decision === 'abandon') { task.cancelRequested = true; task.state = 'cancelled'; task.currentReport = null; if (task.worktreeIntent) task.abandonedWorkspaceIntent = task.worktreeIntent; delete task.worktreeIntent; }
    await this.persist(task); await this.completeReconciliation(task);
    return grant.decision;
  }
  async resume(id, instruction = '') {
    return this.mutate(async () => {
      const task = this.get(id);
      if (!TERMINAL.has(task.state) || this.running.has(id) || task.recoveryPending) throw fail('Task is not settled', 'BUSY');
      const cancellation = this.store.metadata.get('steps', this.store.scope, `cancel-${hash(id)}`);
      if (task.state === 'cancelled' || task.cancelRequested || task.authorityRevoked || cancellation?.state === 'requested') throw fail('Cancellation or authority revocation is durable; submit newly authorized work instead', 'CANCELLED');
      if (task.quarantined) throw fail('Quarantined task must be reconciled before resubmission', 'STORAGE');
      if (this.stopping) throw fail('Host stopping', 'CLOSED');
      workflowAdapter(task).assertResume();
      authorizeMainMutation(task.role, task.profile, this.modes, task.modeBindings ?? []);
      text(instruction, 'instruction', 16 * 1024, { empty: true });
      if (this.restoreRequestPermitFence(task)) {
        await this.persist(task);
        throw fail('Unresolved shared provider request permit blocks resume; usage acceptance cannot settle its claim', 'QUOTA_PERMIT_HELD');
      }
      const capsule = task.resumeCapsule ? assertCapsuleBinding(task, task.resumeCapsule) : null;
      if (capsule?.cancellationIntent) throw fail('Resume capsule records durable cancellation/revocation', 'CANCELLED');
      if (capsule && instruction) throw fail('Usage pause resumes the pinned goal; amended instructions require new work', 'CONFLICT');
      if (!automaticRecoverySafe(task)) {
        this.reconciling.add(id);
        try { if (await this.reconcileTask(task) === 'abandon') return this.summary(task); } finally { this.reconciling.delete(id); }
        if (task.cancelRequested || task.authorityRevoked) throw fail('Reconciliation was cancelled', 'CANCELLED');
      }
      if (this.effects.has(id)) throw fail('A publication effect journal forbids resume; inspect its immutable metadata and destination', 'UNKNOWN_EFFECT');
      if (capsule) {
        assertCapsuleBinding(task, capsule);
        const now = Date.now();
        // Legacy capsules may have no nextEligibleAt. Their overload receipt
        // still imposes the new minimum and any longer provider reset.
        const overloadEligibleAt = overloadCooldownUntil(capsule.providerFault);
        if (overloadEligibleAt !== null && overloadEligibleAt > now) throw fail(`Provider overload cooldown has not arrived (${overloadEligibleAt})`, 'RATE_LIMIT');
        if (capsule.nextEligibleAt !== null && capsule.nextEligibleAt > now) throw fail(`Provider minimum eligibility time has not arrived (${capsule.nextEligibleAt})`, 'RATE_LIMIT');
        const account = task.quotaHold?.account ?? task.profile.account ?? task.profile.provider;
        const pool = this.quotaPools.get(account) ?? this.admission.quotaPools(now).get(account);
        if (pool?.held) {
          if (!Number.isSafeInteger(pool.retryAt) && capsule.nextEligibleAt === null) throw fail('Provider hold has no verified reset; release it explicitly only after external account/quota remediation', 'BLOCKED');
          const eligibleAt = Math.max(pool.retryAt ?? 0, capsule.nextEligibleAt ?? 0);
          if (eligibleAt > now) throw fail(`Provider minimum eligibility time has not arrived (${eligibleAt})`, 'RATE_LIMIT');
          this.quotaPools.delete(account); this.persistQuotaPool(account, pool, 'released');
        }
        for (const reason of assertHoldReasons(task.holdReasons ?? capsule.holdReasons)) {
          if (reason.kind === 'provider') continue;
          if (reason.kind === 'memory' && (this.pressure || this.foregroundPressure || this.systemPressure || (this.limits.memorySoftBytes != null && this.resourceObservation().rssBytes >= this.limits.memorySoftBytes))) throw fail('Runtime memory/admission pressure still holds this task', 'BUSY');
          if (reason.kind === 'connection' && this.connectionHeld && task.profile.allowBackground !== true) throw fail('Foreground connectivity still holds this task', 'BLOCKED');
          if (['request', 'effect'].includes(reason.kind) && !automaticRecoverySafe(task)) throw fail('Request/effect uncertainty still requires reconciliation', 'UNCERTAIN');
          if (reason.kind === 'authority') authorizeMainMutation(task.role, task.profile, this.modes, task.modeBindings ?? []);
        }
        const budget = rootBudgetDecision(task); if (!budget.allowed) throw fail(budget.reason, 'ROOT_BUDGET');
        if (this.pressure || this.foregroundPressure || this.systemPressure || (this.limits.memorySoftBytes != null && this.resourceObservation().rssBytes >= this.limits.memorySoftBytes)) throw fail('Runtime memory/admission pressure prevents resume', 'BUSY');
        if (this.connectionHeld && task.profile.allowBackground !== true) throw fail('Foreground connectivity prevents resume', 'BLOCKED');
        if (task.workItem) {
          for (const ref of task.workItem.inputRefs) await boundedInputIdentity(this.store, ref, 'Resume input');
          await this.assertGitSnapshotInputs(task.workItem); await this.assertFileSnapshotInputs(task.workItem);
          if (task.workItem.evidenceIds?.length) await this.resolveEvidenceIndex(task.workItem);
          if (task.workItem.route) { await this.refreshConfiguration(); this.verifyWorkRoute(task.workItem, task.profile, task.modeBindings ?? []); }
        } else if (task.inputSnapshot && typeof task.inputSnapshot === 'object') await boundedInputIdentity(this.store, task.inputSnapshot, 'Resume input snapshot');
        if (task.workItem?.execution !== 'artifact-sha256') await this.runner.validate?.(task.profile);
        assertCapsuleBinding(task, capsule);
      }
      const dependency = dependencyState(task, this.tasks, this.running, this.effects);
      if (dependency.state === 'blocked') throw fail(`Predecessors are not successful: ${dependency.blockedBy.join(', ')}. Repair/resume them first.`, 'BLOCKED');
      const data = this.resultInputs.readiness(task);
      if (data.blocked.length) throw fail(`Required result input unavailable from producer agentIds: ${[...new Set(data.blocked)].join(', ')}. Inspect the pinned binding before submitting newly authorized work.`, 'BLOCKED_INPUT');
      await this.auditUsageUncertainty(task);
      const latestCancellation = this.store.metadata.get('steps', this.store.scope, `cancel-${hash(id)}`);
      if (task.cancelRequested || task.authorityRevoked || latestCancellation?.state === 'requested') throw fail('Cancellation or authority revocation arrived during resume checks', 'CANCELLED');
      if (!capsule && (instruction || task.state === 'idle' || resultHead(this.store, task)?.outputComplete)) advanceResultEpoch(task, Boolean(instruction));
      Object.assign(task, workflowAdapter(task).resumePatch(task));
      if (!capsule) { task.result = ''; delete task.resultRef; }
      task.state = 'queued'; task.currentReport = null; task.acceptedReport = null; task.acceptedUse = null; task.instruction = instruction; task.autoWakes = 0; task.error = undefined; delete task.blockedBy; delete task.qualityFailure;
      delete task.executionIntent; delete task.evidenceInputs; delete task.quotaHold; task.holdReasons = [];
      await this.persist(task);
      this.kick(); return this.summary(task);
    });
  }
  async cancel(id) {
    const initial = this.get(id);
    const completed = task => ['idle', 'failed'].includes(task.state) && !this.running.has(id) && !task.recoveryPending && !this.reconciling.has(id) && !task.cancelRequested && !task.authorityRevoked && automaticRecoverySafe(task);
    const requestCancellation = task => {
      if (task.cancelRequested) return;
      this.store.metadata.put('steps', this.store.scope, { id: `cancel-${hash(id)}`, agentId: id, kind: 'cancellation', state: 'requested', nonce: randomUUID(), requestedAt: Date.now() });
      task.cancelRequested = true; task.cancellationRequestedAt = Date.now();
      if (['pending', 'active'].includes(task.usageOnlyTransportRetry?.state)) task.usageOnlyTransportRetry.state = 'exhausted';
      if (task.resumeCapsule) task.resumeCapsule = assertResumeCapsule({ ...task.resumeCapsule, cancellationIntent: true });
      this.running.get(id)?.abort.abort(fail('Cancelled by main', 'CANCELLED'));
    };
    // Active/paused/uncertain work is fenced before waiting for the mutation queue.
    if (!completed(initial)) requestCancellation(initial);
    // Cancellation must retain its bookkeeping even under public queue pressure.
    // Transport admission already bounds callers; never report BUSY after abort.
    return this.mutateInternal(async () => {
      const task = this.get(id);
      // Re-resolve after queued archive/resume: a historical no-op is not a stale
      // identity grant and must never resurrect an archived or reconciling task.
      if (completed(task) || task.state === 'cancelled' && task.currentReport) return { ...this.summary(task), cancellationRequested: true, noop: true };
      requestCancellation(task);
      if (!this.running.has(id) && (task.state !== 'cancelled' || !task.currentReport)) {
        task.state = 'cancelled'; task.currentReport = null; task.acceptedReport = null; task.acceptedUse = null;
        // Cancellation preserves checkpoints, result artifacts and capsule evidence.
        await this.completionMail(task);
      } else await this.persist(task);
      this.kick(); return { id, cancellationRequested: true, state: task.state, settled: !this.running.has(id) && !task.recoveryPending && TERMINAL.has(task.state) };
    });
  }
  async archive(id) {
    identifier(id);
    return this.mutate(async () => {
      const task = this.tasks.get(id);
      if (!task) {
        const prior = this.store.metadata.get('work_items', this.store.scope, id);
        if (prior?.state !== 'archived') throw fail('Archived task not found', 'NOT_FOUND');
        return { id, archived: true, duplicate: true, retained: 'artifacts, worktree and mail' };
      }
      if (!TERMINAL.has(task.state) || this.running.has(id) || task.recoveryPending) throw fail('Cannot archive active task', 'BUSY');
      if (this.effects.has(id)) throw fail('Publication effect journal must remain joined to its task; archive is blocked', 'BLOCKED');
      if (!automaticRecoverySafe(task)) await this.reconcileTask(task, 'abandon');
      if ([...this.tasks.values()].some(t => t.id !== id && t.dependsOn?.includes(id))) throw fail('Agent is referenced by dependsOn; archive dependents first', 'BUSY');
      // Archive is explicit abandonment, not execution or evidence deletion.
      const retainedMail = { pending: task.mailbox.length, uncertain: task.delivering.length };
      await this.store.archiveTask(id); this.tasks.delete(id);
      return { id, archived: true, retained: 'artifacts, worktree and mail', retainedMail };
    });
  }
  requestShutdown() {
    this.stopping = true;
    if (this.admissionWake) { clearTimeout(this.admissionWake.timer); this.admissionWake = null; }
    for (const running of this.running.values()) running.abort.abort(fail('Host shutting down', 'SHUTDOWN'));
  }
  async shutdown() {
    if (this.shutdownPromise) return this.shutdownPromise;
    this.requestShutdown();
    this.shutdownPromise = (async () => {
      // Unstarted accepted work remains queued. Running work persists its last
      // checkpoint/outcome on abort; uncertain effects are never cleared here.
      const errors = [];
      await this.tail; this.requestShutdown();
      const results = await Promise.allSettled([...this.running.values()].map(r => r.promise));
      for (const row of results) if (row.status === 'rejected') errors.push(row.reason);
      await this.tail;
      try { if (this.runner.shutdown) await this.runner.shutdown(); else await this.runner.close?.(); } catch (error) { errors.push(error); }
      await this.tail;
      if (this.fatal) errors.push(fail(`Shutdown persistence failed: ${this.fatal}`, 'STORAGE'));
      // This index covers every pending recovered executor; do not load all
      // historical task records merely to shut down the current runtime.
      for (const id of this.recoveringExecutors) {
        try { if (!await executorStopped(this.tasks.get(id)?.executionIntent?.executor)) errors.push(fail(`Recovered executor for ${id} is not confirmed stopped`, 'EXECUTOR_UNCERTAIN')); } catch (error) { errors.push(error); }
      }
      try { this.admission.close(); } catch (error) { errors.push(error); }
      if (errors.length) throw fail(`Shutdown incomplete: ${errors.map(safeError).join('; ')}`, 'SHUTDOWN_INCOMPLETE');
    })();
    return this.shutdownPromise;
  }
}
