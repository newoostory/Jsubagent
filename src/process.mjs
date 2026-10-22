import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { LIMITS, encodeJson, fail, safeError } from './limits.mjs';
import { randomUUID } from 'node:crypto';
import { processIdentity } from './runtime/ownership.mjs';
import { markPreDispatchFailure, markToolFailure } from './execution/failure-receipt.mjs';

const guardianPath = fileURLToPath(new URL('./guardian.mjs', import.meta.url));
export const COMMAND_REGISTRATION_FAILURE_RECEIPT_VERSION = 1;
export async function runProcess({ command, args = [], cwd, signal, timeoutMs = LIMITS.commandTimeoutMs, store, owner, onProgress, env = process.env, semaphore, commandRegistrationFailureReceipt = false }) {
  signal?.throwIfAborted();
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 86_400_000) throw fail('Invalid process timeout');
  const start = { type: 'start', command, args, cwd, timeoutMs };
  encodeJson(start, LIMITS.inputBytes);
  const release = await semaphore?.acquire(signal);
  let stdout, stderr, child, forced, deadline, result, processError, outputError, executor, exited, shutdownTimer, cleanup;
  let reads = [], stopped = false, detached = false, startAttempted = false, registrationError, finishShutdown;
  const shutdown = new Promise(resolve => { finishShutdown = resolve; });
  const cancel = (reason) => {
    if (!child || child.exitCode !== null) return;
    if (child.connected) child.send({ type: 'cancel', reason: reason === 'timeout' ? 'timeout' : 'cancel' }, () => {});
    forced ??= setTimeout(() => child.kill('SIGKILL'), 4000);
    shutdownTimer ??= setTimeout(() => { detached = true; child.stdout?.destroy(); child.stderr?.destroy(); finishShutdown(); }, 5000);
  };
  const reconcile = () => cleanup ??= (async () => {
    try { if (executor) await semaphore?.untrackProcess?.(owner, executor); }
    finally { await release?.(); }
  })();
  try {
    signal?.throwIfAborted();
    stdout = await store.artifact(owner, 'stdout'); stderr = await store.artifact(owner, 'stderr');
    child = spawn(process.execPath, [guardianPath], { cwd, env: { ...env, NODE_OPTIONS: '', PI_AGENT_TOOL: '1' }, stdio: ['ignore', 'pipe', 'pipe', 'ipc'], windowsHide: true });
    exited = new Promise(resolve => {
      child.once('error', e => { processError = e; stopped = true; resolve(); });
      child.once('close', () => { stopped = true; resolve(); });
    });
    child.on('message', message => { if (message?.type === 'result') result = message; });
    child.on('error', () => {});
    signal?.addEventListener('abort', cancel, { once: true }); if (signal?.aborted) cancel();
    deadline = setTimeout(() => { processError = fail('Command timed out', 'TIMEOUT'); cancel('timeout'); }, timeoutMs + 500);
    const consume = async (source, artifact) => {
      let lastUpdate = 0;
      try {
        for await (const chunk of source) {
          // Stream chunks are bounded by pipe read highWaterMark; never concatenate.
          await artifact.write(chunk);
          if (onProgress && Date.now() - lastUpdate >= 500) {
            lastUpdate = Date.now(); onProgress({ stdoutBytes: stdout.info.bytes, stderrBytes: stderr.info.bytes });
          }
        }
      } catch (e) { outputError ??= e; cancel(); }
    };
    reads = [consume(child.stdout, stdout), consume(child.stderr, stderr)];
    if (semaphore?.trackProcess) {
      executor = { pid: child.pid, started: await processIdentity(child.pid), nonce: randomUUID() };
      if (!executor.started) throw fail('Command guardian stopped before registration', 'PROCESS');
      try { await semaphore.trackProcess(owner, executor); }
      catch (error) { registrationError = error; throw error; }
    }
    signal?.throwIfAborted();
    startAttempted = true;
    child.send(start, e => { if (e) { processError = e; cancel(); } });
    await Promise.race([exited, shutdown]);
    if (detached) {
      processError = fail('Command guardian remains alive after bounded cancellation', 'EXECUTOR_UNCERTAIN'); processError.executorStopped = false;
      await Promise.allSettled(reads);
    } else await Promise.all(reads);
    // A local deadline is distinct from revocation/abort. Only an original
    // guardian outcome, stopped guardian and drained streams can confirm it.
    const knownTimeout = result?.timedOut === true && result.cancelled === true && stopped && !detached && !signal?.aborted && !outputError && (!processError || processError.code === 'TIMEOUT');
    const error = outputError ?? (knownTimeout ? null : processError) ?? (signal?.aborted ? fail('Command cancelled', 'CANCELLED') : !result ? fail('Command guardian exited without an outcome receipt', 'PROCESS') : result.error ? fail(`Command failed: ${result.error}`) : null);
    if (error && result?.error && !outputError && !processError && !signal?.aborted) markPreDispatchFailure(error, 'guardian-confirmed-spawn-rejection');
    const complete = !error && (!result?.cancelled || knownTimeout);
    const outputs = await Promise.all([stdout.finish(complete, error && safeError(error)), stderr.finish(complete, error && safeError(error))]);
    const response = { targetTerminated: Boolean(result && !result.error), targetPid: result?.targetPid, exitCode: result ? result.code : null, signal: result?.signal ?? null, cancelled: Boolean(signal?.aborted || result?.cancelled), ...(knownTimeout ? { timedOut: true, guardianStopped: true } : {}), stdout: outputs[0], stderr: outputs[1] };
    if (error) { error.artifacts = outputs.map(({ id, bytes, complete }) => ({ id, bytes, complete })); throw error; }
    return response;
  } catch (e) {
    cancel();
    if (exited && !stopped) await Promise.race([exited, shutdown]);
    if (detached && e.code !== 'EXECUTOR_UNCERTAIN') { e = fail(`${safeError(e)}; command guardian remains alive after bounded cancellation`, 'EXECUTOR_UNCERTAIN'); e.executorStopped = false; }
    await Promise.allSettled(reads);
    const seals = await Promise.allSettled([stdout?.finish(false, safeError(e)), stderr?.finish(false, safeError(e))]);
    const sealFailure = seals.find(row => row.status === 'rejected');
    const artifacts = [stdout, stderr].filter(Boolean).map(a => ({ id: a.info.id, bytes: a.info.bytes, complete: a.info.complete }));
    const registrationRejectedBeforeStart = commandRegistrationFailureReceipt === true && e === registrationError && !startAttempted && !detached && stopped && !sealFailure;
    let error = registrationRejectedBeforeStart && e && typeof e === 'object'
      ? e
      : sealFailure
        ? fail(`${safeError(e)}; command output could not be durably sealed: ${safeError(sealFailure.reason)}`, 'STORAGE')
        : fail(`${safeError(e)}${artifacts.length ? `\nOutput artifacts: ${encodeJson(artifacts, 4096).toString('utf8')}` : ''}`, e.code ?? 'PROCESS');
    error.artifacts = artifacts;
    if (registrationRejectedBeforeStart) {
      let guardianStopped = false;
      try { guardianStopped = await processIdentity(executor.pid) !== executor.started; } catch {}
      if (guardianStopped) {
        await reconcile();
        markPreDispatchFailure(error, 'command-registration-rejected-before-start');
      }
    } else if (e.toolFailureReceipt && !sealFailure) markToolFailure(error, e.toolFailureReceipt);
    if (e.executorStopped === false) error.executorStopped = false;
    throw error;
  } finally {
    clearTimeout(forced); clearTimeout(deadline); clearTimeout(shutdownTimer); signal?.removeEventListener('abort', cancel);
    if (!child || stopped) await reconcile(); else void exited?.then(reconcile).catch(() => {});
  }
}
