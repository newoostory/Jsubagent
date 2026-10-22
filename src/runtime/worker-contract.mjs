import { fail } from '../limits.mjs';

// Nullable usage observations and identity-bound memory rechecks require a matching supervisor.
// Negotiate before SDK/auth initialization, never by probing a model request.
export const WORKER_PROTOCOL = 7;
export function assertWorkerProtocol(value) {
  if (value !== WORKER_PROTOCOL) throw fail('Worker/supervisor protocol mismatch; reload the runtime before starting new work. Saved tasks and uncertain effects must not be replayed.', 'RUNTIME_MISMATCH');
}
