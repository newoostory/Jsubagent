const bytes = value => Number.isFinite(value) && value >= 0 ? `${(value / 1048576).toFixed(1)} MiB` : 'unknown';
export function inspectorSafetyLines(task, snapshot = {}) {
  const lines = [], capsule = task.resume ?? task.resumeCapsule;
  const holds = task.holdReasons ?? capsule?.holdReasons ?? [];
  if (task.state === 'paused' || holds.length) {
    const next = capsule?.nextEligibleAt;
    const time = Number.isSafeInteger(next) && next >= 0 && next < 8.64e15 ? new Intl.DateTimeFormat('en-GB', { timeZone: 'Asia/Seoul', dateStyle: 'medium', timeStyle: 'long' }).format(next) : 'unknown';
    lines.push('', ' WAIT / RECOVERY', ` Reasons: ${holds.map(row => row.kind).join(', ') || 'inspect saved error'}`, ` Earliest eligible: ${time} · explicit resume required`, ` Timing source: ${capsule?.providerFault?.source ?? task.providerFault?.source ?? 'unknown'}`);
  }
  const id = task.id ?? task.agentId;
  const admission = task.state === 'queued' && task.admission?.admitted === false ? task.admission
    : id && snapshot.memoryAdmission?.agentId === id && snapshot.memoryAdmission?.admitted === false
      && (task.state === 'queued' || (task.state === 'running' && snapshot.memoryAdmission.boundaryRenewal === true)) ? snapshot.memoryAdmission : undefined;
  if (task.state === 'queued' || admission) {
    const reason = admission?.reason ?? (task.dependencyState === 'waiting' ? 'waiting_for_predecessors' : task.dataReadiness?.state === 'waiting_input' ? 'waiting_for_result_input' : 'not_observed');
    lines.push('', ' QUEUE / ADMISSION', ` Admission hold: ${reason}`, ' A queue hold is not a failed task. Waiting does not clear locks or replay work.');
  }
  if (task.bundleRef) lines.push('', ' PUBLISHED RESULT', ` ${task.bundleRef.bundleId} · revision ${task.bundleRef.revision} · epoch ${task.bundleRef.resultEpoch}`, ' Read bundle/result_file offline; file EOF is not verification or acceptance.');
  const evidence = task.currentExecution ?? task.executionReadiness;
  if (evidence) lines.push(` Verification: ${evidence.verification} · acceptance: ${evidence.acceptance}`, ` Commands: ${evidence.executedCommands} · failed: ${evidence.executionCounts?.failed ?? 'unknown'} · inconclusive: ${evidence.executionCounts?.inconclusive ?? 'unknown'}`, ` Subject: ${evidence.subjectState ?? 'unbound'}; product snapshot acceptance is separate.`);
  else if (task.bundleRef) lines.push(' Verification: not_run / no trusted checker summary in this snapshot');
  if (evidence && task.state === 'running') lines.push(' Command failures are recorded history, not agent termination; wait still requires settlement or attention.');
  const product = task.currentProductVerification;
  if (product) lines.push(` Product verification: ${product.verification} · acceptance: ${product.acceptance}`, ` Required checker tasks: ${product.participants} · failed ${product.counts.failed} · not_run ${product.counts.not_run} · stale ${product.counts.stale}`, ` Product SHA-256: ${product.productSha256 ?? 'not_published'}`);
  if (task.dataReadiness) lines.push(` Input readiness: ${task.dataReadiness.state}`);
  if (task.resultRoutes?.length) lines.push(` Result routes: ${task.resultRoutes.map(row => `${row.targetId}:${row.state}`).join(', ')}`);
  const worker = snapshot.workers?.find(row => row.taskId === (task.id ?? task.agentId));
  if (worker || snapshot.memory || snapshot.memoryAccounting) {
    lines.push('', ' MEMORY · separate scopes, never summed', ` Supervisor RSS: ${bytes(snapshot.memory?.rss)}`);
    if (worker) lines.push(` Worker RSS: ${bytes(worker.memory?.rss)} · heap ${bytes(worker.memory?.heapUsed)} / ${bytes(worker.memory?.heapLimit)} · ${worker.memoryState ?? 'unknown'}`);
    if (snapshot.memoryAccounting) lines.push(` Deduplicated managed RSS: ${bytes(snapshot.memoryAccounting.actualBytes)} · projected: ${bytes(snapshot.memoryAccounting.projectedManagedBytes)}`);
    lines.push(' Sampled estimates, not OS hard limits; unsupported observations remain unknown.');
  }
  return lines;
}
