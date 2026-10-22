import { executionReadiness } from './result-evidence.mjs';

// Current checker state is separate from an immutable publication's historical
// readiness. All accepted required-check tasks contribute; the preview is not
// the authority and cannot hide an omitted failure/not-run/stale participant.
export function productVerification(database, scope, agentId, resultEpoch, productSha256) {
  const counts = { passed: 0, failed: 0, not_run: 0, inconclusive: 0, stale: 0 }, examples = [];
  let after = 0, participants = 0;
  for (;;) {
    const page = database.prepare("SELECT seq,data_json FROM work_items WHERE run_id=? AND owner_id=? AND seq>? AND json_array_length(data_json,'$.requiredResultChecks')>0 AND EXISTS (SELECT 1 FROM json_each(data_json,'$.verificationSources') source WHERE json_extract(source.value,'$.sourceId')=? AND json_extract(source.value,'$.resultEpoch')=?) ORDER BY seq LIMIT 32").all(scope.runId, scope.ownerId, after, agentId, resultEpoch);
    for (const row of page) {
      after = Number(row.seq); const task = JSON.parse(row.data_json);
      const readiness = executionReadiness(database, scope, task.id, task.resultEpoch ?? 1);
      const changed = task.verificationSubject && task.verificationSubject.productSha256 !== productSha256;
      const verification = changed ? 'stale' : readiness.verification;
      counts[verification]++; participants++;
      const item = { agentId: task.id, state: task.state, verification, requiredCheckCount: readiness.requiredCheckCount, requiredPassed: readiness.requiredPassed, productSha256: task.verificationSubject?.productSha256 ?? null, read: { action: 'read', requests: [{ agentId: task.id, source: 'bundle', view: 'main' }] } };
      const severity = value => ['failed', 'stale', 'not_run', 'inconclusive', 'passed'].indexOf(value.verification);
      examples.push(item); examples.sort((a, b) => severity(a) - severity(b)); if (examples.length > 4) examples.pop();
    }
    if (page.length < 32) break;
  }
  const verification = counts.failed ? 'failed' : counts.stale ? 'stale' : counts.not_run ? 'not_run' : counts.inconclusive ? 'inconclusive' : participants ? 'passed' : 'not_run';
  return { productSha256: productSha256 ?? null, verification, acceptance: 'not_evaluated', participants, counts, examples, omittedParticipants: Math.max(0, participants - examples.length), reason: participants ? 'Runtime required-check receipts only. Semantic acceptance and external application require their separate authority.' : 'No explicitly approved required-check task for this product epoch.' };
}
