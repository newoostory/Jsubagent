import { join } from 'node:path';
import { encodeJson, fail, hash } from './limits.mjs';
import { readBounded } from './storage.mjs';
import { productSnapshot } from './result-snapshots.mjs';

export function executionReadiness(database, scope, agentId, resultEpoch) {
  const args = [scope.runId, scope.ownerId, agentId, resultEpoch];
  const rows = database.prepare("SELECT state,COUNT(*) AS n FROM result_evidence WHERE run_id=? AND owner_id=? AND json_extract(data_json,'$.agentId')=? AND json_extract(data_json,'$.resultEpoch')=? GROUP BY state LIMIT 8").all(...args);
  const counts = { succeeded: 0, failed: 0, inconclusive: 0 };
  for (const row of rows) if (Object.hasOwn(counts, row.state)) counts[row.state] = Number(row.n);
  const total = counts.succeeded + counts.failed + counts.inconclusive;
  const taskRow = database.prepare('SELECT data_json FROM work_items WHERE run_id=? AND owner_id=? AND id=?').get(scope.runId, scope.ownerId, agentId);
  const definitions = taskRow ? JSON.parse(taskRow.data_json).requiredResultChecks ?? [] : [];
  if (!Array.isArray(definitions) || definitions.length > 16) throw fail('Invalid required checker metadata', 'STORAGE');
  const coverage = new Map(definitions.map(check => [check.id, { id: check.id, inputDigest: check.inputDigest, state: 'not_run' }]));
  let subjectState = 'unbound', after = 0, attested = 0;
  // Inspect every receipt in bounded pages. A four-item preview cannot hide an
  // older stale subject merely because newer commands succeeded.
  for (;;) {
    const page = database.prepare("SELECT seq,data_json FROM result_evidence WHERE run_id=? AND owner_id=? AND json_extract(data_json,'$.agentId')=? AND json_extract(data_json,'$.resultEpoch')=? AND seq>? ORDER BY seq LIMIT 32").all(...args, after);
    for (const row of page) {
      after = Number(row.seq); const value = JSON.parse(row.data_json), subject = value.subject;
      for (const id of value.checkIds ?? []) {
        const check = coverage.get(id);
        if (!check || check.inputDigest !== value.inputDigest) continue;
        if (value.state === 'failed') check.state = 'failed';
        else if (check.state !== 'failed') {
          // Repetition is not reconciliation: retain incomplete evidence even
          // when another invocation of this required command passed.
          if (value.state !== 'succeeded' || !subject.productSnapshotAttested) check.state = 'inconclusive';
          else if (check.state === 'not_run') check.state = 'passed';
        }
      }
      if (subject.productSnapshotAttested) { attested++; if (subjectState === 'unbound') subjectState = 'pinned_product_snapshot'; }
      if (subject.productChanged) subjectState = 'stale';
      for (const binding of subject.bindings) {
        const source = database.prepare('SELECT data_json FROM work_items WHERE run_id=? AND owner_id=? AND id=?').get(scope.runId, scope.ownerId, binding.agentId);
        const current = source && JSON.parse(source.data_json);
        const head = database.prepare('SELECT data_json FROM result_heads WHERE run_id=? AND owner_id=? AND id=?').get(scope.runId, scope.ownerId, binding.bundleId);
        const manifest = head && JSON.parse(head.data_json);
        const sameInput = binding.productSha256 ? manifest?.candidateSnapshot?.productSha256 === binding.productSha256 : manifest?.revision === binding.revision;
        if (!current || current.resultEpoch !== binding.resultEpoch || !sameInput) subjectState = 'stale';
        else if (subjectState === 'unbound') subjectState = 'pinned_result_inputs';
      }
    }
    if (page.length < 32) break;
  }
  const evidence = database.prepare("SELECT data_json FROM result_evidence WHERE run_id=? AND owner_id=? AND json_extract(data_json,'$.agentId')=? AND json_extract(data_json,'$.resultEpoch')=? ORDER BY CASE state WHEN 'failed' THEN 0 WHEN 'inconclusive' THEN 1 ELSE 2 END,seq DESC LIMIT 4").all(...args).map(row => {
    const value = JSON.parse(row.data_json);
    return { state: value.state, receiptRef: value.receiptRef, subjectDigest: hash(encodeJson(value.subject, 16 * 1024)), productSnapshotAttested: Boolean(value.subject.productSnapshotAttested), stepId: value.id };
  });
  const requiredChecks = [...coverage.values()], requiredPassed = requiredChecks.filter(check => check.state === 'passed').length;
  const verification = counts.failed ? 'failed' : subjectState === 'stale' ? 'stale' : requiredChecks.length ? requiredPassed === requiredChecks.length ? 'passed' : requiredChecks.some(check => check.state === 'inconclusive') ? 'inconclusive' : 'not_run' : total ? 'inconclusive' : 'not_run';
  return { verification, subjectState, acceptance: 'not_evaluated', candidate: 'unsupported', executionCounts: counts, executedCommands: total, productAttestedCommands: attested, requiredChecks, requiredCheckCount: requiredChecks.length, requiredPassed, evidence, omittedEvidence: Math.max(0, total - evidence.length),
    reason: verification === 'passed' ? 'All explicitly approved required checks have matching successful runtime receipts against the unchanged immutable product. Semantic acceptance and external application remain separate.' : total ? 'Trusted command outcomes; immutable product binding and required-check coverage are checked separately. Model PASS cannot override missing/stale/failed evidence.' : 'No trusted checker receipt for this result epoch.' };
}

export async function recordExecutionEvidence(store, task, step) {
  if (step.tool !== 'bash') return null;
  const prior = store.metadata.get('result_evidence', store.scope, step.stepId);
  if (prior) { if (prior.receiptRef.sha256 !== step.resultRef.sha256) throw fail('Checker receipt changed', 'STORAGE'); return prior; }
  const identity = await store.artifactIdentity(step.resultRef.artifactId);
  if (identity.owner !== task.id || !identity.complete || identity.bytes !== step.resultRef.bytes || identity.sha256 !== step.resultRef.sha256) throw fail('Untrusted checker receipt identity', 'PERMISSION');
  const bytes = await readBounded(join(store.root, 'artifacts', `${step.resultRef.artifactId}.bin`), store.limits.responseBytes + store.limits.inputBytes);
  if (bytes.length !== identity.bytes || hash(bytes) !== identity.sha256) throw fail('Checker receipt changed during read', 'STORAGE');
  const body = JSON.parse(bytes.toString('utf8'));
  let state = 'inconclusive', exitCode;
  if (body.code === 'COMMAND_FAILED' || body.code === 'TIMEOUT' && body.failure?.type === 'local-process-termination' && body.failure.reason === 'timeout') state = 'failed';
  else if (body.result && !body.error) {
    let output;
    try { output = JSON.parse(body.result.content?.[0]?.text); } catch {}
    if (Number.isInteger(output?.exitCode) && !output.cancelled) { exitCode = output.exitCode; state = exitCode === 0 ? 'succeeded' : 'failed'; }
  }
  const subject = { type: 'command-input-binding', bindings: (task.resultBindings ?? []).map(({ agentId, bundleId, revision, resultEpoch, candidateSnapshot }) => ({ agentId, bundleId, revision, resultEpoch, ...(candidateSnapshot ? { productSha256: candidateSnapshot.productSha256 } : {}) })), workspace: task.cwd, productSnapshotAttested: false };
  if (task.productInput && step.productSubject && step.productAfter) {
    const expected = productSnapshot(store, step.productSubject, task.productInput.ownerId), actual = productSnapshot(store, step.productAfter, task.id);
    if (expected.productSha256 !== task.productInput.productSha256) throw fail('Checker subject changed', 'STORAGE');
    subject.productSnapshot = { snapshotId: expected.snapshotId, ownerId: expected.ownerId, productSha256: expected.productSha256 };
    subject.productChanged = actual.productSha256 !== expected.productSha256;
    subject.productSnapshotAttested = !subject.productChanged;
  }
  if (step.productObservationError) subject.observation = 'inconclusive';
  const checkIds = step.inputCanonical === true ? (task.requiredResultChecks ?? []).filter(check => check.inputDigest === step.inputDigest).map(check => check.id) : [];
  const record = { id: step.stepId, state, agentId: task.id, resultEpoch: task.resultEpoch ?? 1, attemptId: step.attemptId, receiptRef: step.resultRef, inputDigest: step.inputDigest, subject, checkIds, ...(exitCode === undefined ? {} : { exitCode }) };
  encodeJson(record, 16 * 1024);
  store.metadata.put('result_evidence', store.scope, record, { ifAbsent: true });
  return record;
}

export async function evidenceSummaryArtifact(store, task) {
  const readiness = executionReadiness(store.metadata.database, store.scope, task.id, task.resultEpoch ?? 1);
  const index = await store.artifact(task.id, 'result-checker-evidence-index');
  try {
    let after = 0;
    for (;;) {
      const page = store.metadata.database.prepare("SELECT seq,data_json FROM result_evidence WHERE run_id=? AND owner_id=? AND json_extract(data_json,'$.agentId')=? AND json_extract(data_json,'$.resultEpoch')=? AND seq>? ORDER BY seq LIMIT 32").all(store.scope.runId, store.scope.ownerId, task.id, task.resultEpoch ?? 1, after);
      for (const row of page) { await index.write(Buffer.concat([encodeJson(JSON.parse(row.data_json), 16 * 1024), Buffer.from('\n')])); after = Number(row.seq); }
      if (page.length < 32) break;
    }
    const sealed = await index.finish(true);
    readiness.evidenceIndexRef = { artifactId: sealed.id, bytes: sealed.bytes, sha256: sealed.sha256, complete: true, indexedCommands: readiness.executedCommands };
  } catch (error) { if (!index.closed) await index.finish(false, 'checker evidence index persistence failed').catch(() => {}); throw error; }
  const artifact = await store.artifact(task.id, 'result-checker-summary');
  try {
    const body = encodeJson(readiness, 16 * 1024); await artifact.write(body); const info = await artifact.finish(true);
    return { readiness, contentRef: { artifactId: info.id, bytes: info.bytes, sha256: info.sha256, complete: true }, identity: hash(body) };
  } catch (error) { if (!artifact.closed) await artifact.finish(false, 'checker summary persistence failed').catch(() => {}); throw error; }
}
