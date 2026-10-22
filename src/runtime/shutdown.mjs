import { join } from 'node:path';
import { atomicJson } from '../storage.mjs';
import { fail, identifier, safeError } from '../limits.mjs';

// A receipt describes durable shutdown preparation, not proof that this process
// has exited. The parent must separately observe the original process identity die.
export async function persistShutdown({ root, identity, drain, stop, closeStore, closeLedger, release, write = atomicJson }) {
  const path = join(root, `shutdown-${identifier(identity.nonce)}.json`);
  const receipt = { version: 1, identity, state: 'stopping', startedAt: Date.now(), records: 'Existing task/checkpoint/artifact journals; no RAM dump or history deletion.' };
  const errors = [];
  const attempt = async (phase, operation) => {
    try { await operation(); }
    catch (error) { errors.push({ phase, code: String(error?.code ?? 'SHUTDOWN'), message: safeError(error) }); }
  };
  await attempt('intent', () => write(path, receipt, 32768));
  await attempt('drain', drain);
  await attempt('executors', stop);
  await attempt('store', closeStore);
  await attempt('ledger', closeLedger);
  // Publish readiness only after ownership release succeeds. This generation's
  // unique receipt path cannot collide with a replacement host's receipt.
  await attempt('ownership', release);
  receipt.finishedAt = Date.now();
  receipt.state = errors.length ? 'failed' : 'ready-to-exit';
  receipt.errors = [...errors];
  await attempt('receipt', () => write(path, receipt, 32768));
  if (errors.length) {
    receipt.state = 'failed'; receipt.errors = [...errors];
    // The filename is generation-specific: a replacement host cannot own it.
    try { await write(path, receipt, 32768); } catch { /* original failure remains explicit */ }
    throw fail(`Supervisor shutdown incomplete: ${errors.map(e => `${e.phase}: ${e.message}`).join('; ')}`, 'SHUTDOWN_INCOMPLETE');
  }
  return { saved: true, executorsStopped: true, receiptPath: path };
}
