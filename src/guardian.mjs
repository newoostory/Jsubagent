// Tiny lifeline process: it does not buffer output or import the Pi SDK.
import { spawn } from 'node:child_process';
import { encodeJson, LIMITS } from './limits.mjs';
let child, stopping = false, stopReason, finished = false, timer, escalation, initialized = false;
function signalGroup(signal) {
  if (!child?.pid) return;
  try { if (process.platform === 'win32') child.kill(signal); else process.kill(-child.pid, signal); }
  catch (e) { if (e.code !== 'ESRCH') process.exitCode = 1; }
}
function stop(reason = 'cancel') {
  if (stopping || finished) return; stopping = true; stopReason = reason === 'timeout' ? 'timeout' : 'cancel'; signalGroup('SIGTERM');
  escalation = setTimeout(() => { signalGroup('SIGKILL'); setTimeout(() => process.exit(130), 100).unref(); }, 1000);
  if (!child) process.exit(130);
}
process.on('disconnect', stop);
process.on('SIGTERM', stop); process.on('SIGINT', stop);
process.on('message', msg => {
  if (msg?.type === 'cancel') { stop(msg.reason); return; }
  if (initialized || stopping) return;
  initialized = true;
  try {
    encodeJson(msg, LIMITS.inputBytes);
    if (msg?.type !== 'start' || typeof msg.command !== 'string' || !Array.isArray(msg.args) || typeof msg.cwd !== 'string') throw new Error('Invalid command');
    child = spawn(msg.command, msg.args, { cwd: msg.cwd, env: process.env, detached: process.platform !== 'win32', stdio: ['ignore', 1, 2], windowsHide: true });
    timer = setTimeout(() => stop('timeout'), msg.timeoutMs);
    child.on('error', e => { finished = true; process.send?.({ type: 'result', error: e.code ?? 'SPAWN_FAILED' }, () => { if (process.connected) process.disconnect(); }); clearTimeout(timer); process.exitCode = 1; });
    child.on('exit', (code, signal) => {
      if (finished) return;
      // Kill remaining same-group descendants even if the leader exited normally.
      finished = true; signalGroup('SIGKILL'); clearTimeout(timer); clearTimeout(escalation);
      process.send?.({ type: 'result', targetPid: child.pid, code, signal, cancelled: stopping, timedOut: stopReason === 'timeout' }, () => { if (process.connected) process.disconnect(); process.exitCode = stopping ? 130 : (code ?? 1); });
    });
    process.send?.({ type: 'started', pid: child.pid });
  } catch { finished = true; process.send?.({ type: 'result', error: 'INVALID_COMMAND' }, () => { if (process.connected) process.disconnect(); }); process.exitCode = 1; }
});
const startup = setTimeout(() => { if (!initialized) stop(); }, 10_000); startup.unref();
