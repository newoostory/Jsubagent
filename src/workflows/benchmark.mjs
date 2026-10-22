import { authorizeBenchmarkJob, isBenchmark, loadBenchmarkGrant } from '../benchmark-access.mjs';
import { fail } from '../limits.mjs';
import { genericWorkflowAdapter } from './generic.mjs';

export function isBenchmarkWorkflow(task) {
  return isBenchmark(task?.role ?? task, task?.profile);
}

export function benchmarkWorkflowAdapter(task) {
  return {
    ...genericWorkflowAdapter(),
    id: 'benchmark',
    allowWeb: false,
    allowSteering: false,
    allowEffectRecovery: false,
    allowSectionContinuation: false,
    allowMail: false,
    canResume: false,
    allowContinuation: false,
    assertMail() { throw fail('Benchmark mail/steering disabled; use preregistered fresh stages', 'MODE'); },
    assertResume() { throw fail('Benchmark resume disabled; retries require a new preregistered trial', 'MODE'); },
    refreshMode: refreshWorkflowGrants,
    authorityPatch(grant) { return { benchmark: { expiresAt: grant.expiresAt } }; },
    authorityFailure(error) { return error?.code === 'TIMEOUT' && task.benchmark ? { revoked: { benchmarkExpiredAt: Date.now() }, reason: 'Benchmark authority expired' } : undefined; },
    requiredMode: 'benchmark',
    authorizeMode(grant, input, role, profile) { return authorizeBenchmarkJob(role, profile, grant, input); },
  };
}

export async function refreshWorkflowGrants(config, store, modes) {
  const grant = await loadBenchmarkGrant(config.agentDir, store.root);
  if (grant) modes.set('benchmark', grant); else modes.delete('benchmark');
  return grant;
}
export function assertMutableWorkflowMode(name) {
  if (name === 'benchmark') throw fail('Benchmark mode uses a trusted expiring run grant, not set_mode', 'MODE');
}
export function createWorkflowExpiryTimer(task, abort) {
  if (!task.benchmark) return undefined;
  return setTimeout(() => abort(fail('Benchmark grant expired', 'TIMEOUT')), Math.max(1, task.benchmark.expiresAt - Date.now()));
}
