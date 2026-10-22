import { fail, safeError } from '../limits.mjs';

const VERSION = 1;
const SHA256 = /^[a-f0-9]{64}$/;
const ID = /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$/;

function artifactRef(value, name) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw fail(`Invalid ${name} artifact receipt`, 'STORAGE');
  if (!ID.test(value.artifactId ?? '') || !Number.isSafeInteger(value.bytes) || value.bytes < 0 || !SHA256.test(value.sha256 ?? '') || value.complete !== true) throw fail(`Unsealed ${name} artifact receipt`, 'STORAGE');
  return { artifactId: value.artifactId, bytes: value.bytes, sha256: value.sha256, complete: true };
}

export function localProcessFailureReceipt(output) {
  const timedOut = output?.timedOut === true && output?.guardianStopped === true;
  if (!output || output.cancelled && !timedOut || output.targetTerminated !== true || !Number.isSafeInteger(output.targetPid) || output.targetPid < 1 || !Object.hasOwn(output, 'exitCode')) throw fail('Local process termination is not confirmed', 'PROCESS');
  const exitCode = output.exitCode;
  const signal = output.signal ?? null;
  if (!(exitCode === null || Number.isInteger(exitCode)) || !(signal === null || typeof signal === 'string' && signal.length > 0 && signal.length <= 32)) throw fail('Invalid local process termination receipt', 'PROCESS');
  if ((exitCode === 0 || exitCode === null) && signal === null && !timedOut) throw fail('No failed process termination in receipt', 'PROCESS');
  return {
    version: VERSION,
    type: 'local-process-termination',
    effect: 'possible',
    ...(timedOut ? { reason: 'timeout' } : {}),
    target: { pid: output.targetPid, exitCode, signal },
    outputs: [artifactRef({ artifactId: output.stdout?.id, bytes: output.stdout?.bytes, sha256: output.stdout?.sha256, complete: output.stdout?.complete }, 'stdout'), artifactRef({ artifactId: output.stderr?.id, bytes: output.stderr?.bytes, sha256: output.stderr?.sha256, complete: output.stderr?.complete }, 'stderr')],
  };
}

export function markPreDispatchFailure(error, reason = 'rejected-before-dispatch') {
  if (!error || typeof error !== 'object') error = fail(safeError(error));
  Object.defineProperty(error, 'toolFailureReceipt', {
    value: { version: VERSION, type: 'rejected-before-dispatch', effect: 'none', reason: String(reason).slice(0, 128) },
    configurable: true,
  });
  return error;
}

export function markToolFailure(error, receipt) {
  const trusted = assertKnownToolFailure(receipt);
  if (!error || typeof error !== 'object') error = fail(safeError(error));
  Object.defineProperty(error, 'toolFailureReceipt', { value: trusted, configurable: true });
  return error;
}

export function assertKnownToolFailure(value) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || value.version !== VERSION || !['read-failure', 'rejected-before-dispatch', 'local-process-termination'].includes(value.type)) throw fail('Invalid known tool failure receipt', 'STORAGE');
  if (value.type === 'read-failure') {
    if (value.effect !== 'none' || Object.keys(value).some(key => !['version', 'type', 'effect'].includes(key))) throw fail('Invalid read failure receipt', 'STORAGE');
    return { version: VERSION, type: value.type, effect: 'none' };
  }
  if (value.type === 'rejected-before-dispatch') {
    if (value.effect !== 'none' || typeof value.reason !== 'string' || !value.reason || value.reason.length > 128 || Object.keys(value).some(key => !['version', 'type', 'effect', 'reason'].includes(key))) throw fail('Invalid pre-dispatch failure receipt', 'STORAGE');
    return { version: VERSION, type: value.type, effect: 'none', reason: value.reason };
  }
  if (value.effect !== 'possible' || value.reason !== undefined && value.reason !== 'timeout' || Object.keys(value).some(key => !['version', 'type', 'effect', 'reason', 'target', 'outputs'].includes(key))) throw fail('Invalid local process failure receipt', 'STORAGE');
  const target = value.target;
  if (!target || typeof target !== 'object' || Array.isArray(target) || Object.keys(target).some(key => !['pid', 'exitCode', 'signal'].includes(key))) throw fail('Invalid target termination receipt', 'STORAGE');
  const exitCode = target.exitCode, signal = target.signal;
  if (!Number.isSafeInteger(target.pid) || target.pid < 1 || !(exitCode === null || Number.isInteger(exitCode)) || !(signal === null || typeof signal === 'string' && signal.length > 0 && signal.length <= 32) || (exitCode === 0 || exitCode === null) && signal === null && value.reason !== 'timeout') throw fail('Invalid target termination receipt', 'STORAGE');
  if (!Array.isArray(value.outputs) || value.outputs.length !== 2) throw fail('Invalid process output receipts', 'STORAGE');
  return { version: VERSION, type: value.type, effect: 'possible', ...(value.reason ? { reason: value.reason } : {}), target: { pid: target.pid, exitCode, signal }, outputs: value.outputs.map((ref, index) => artifactRef(ref, index ? 'stderr' : 'stdout')) };
}

export async function verifyKnownToolFailure(store, owner, value) {
  const receipt = assertKnownToolFailure(value);
  if (receipt.type !== 'local-process-termination') return receipt;
  for (const [index, ref] of receipt.outputs.entries()) {
    const identity = await store.artifactIdentity(ref.artifactId);
    const info = await store.artifactInfo(ref.artifactId);
    if (!identity.complete || identity.owner !== owner || identity.bytes !== ref.bytes || identity.sha256 !== ref.sha256 || info.label !== (index ? 'stderr' : 'stdout')) throw fail('Process termination output identity changed', 'STORAGE');
  }
  return receipt;
}

export function knownToolFailure(kind, error) {
  if (kind === 'read') return { version: VERSION, type: 'read-failure', effect: 'none' };
  if (!error?.toolFailureReceipt) return null;
  try { return assertKnownToolFailure(error.toolFailureReceipt); } catch { return null; }
}

export function isKnownFailedEffect(step) {
  if (step?.outcome !== 'failed') return false;
  try { assertKnownToolFailure(step.failure); artifactRef(step.resultRef, 'failure outcome'); return true; } catch { return false; }
}

export function effectRequiresReconciliation(step) {
  if (!step || step.kind === 'read' || step.reconciled === true) return false;
  return !isKnownFailedEffect(step);
}

export function replayableEffect(step) {
  return Boolean(step?.resultRef && (step.reconciled === true && step.outcome === 'completed' || isKnownFailedEffect(step)));
}
