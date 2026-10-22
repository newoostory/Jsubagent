import { encodeJson } from './limits.mjs';

// Bound the status before IPC, not after the client has received it. Keep every
// identity in the fallback so oversized history cannot prevent reconnection.
export function boundedHostStatus(engine) {
  const status = engine.status();
  try { encodeJson(status, 512 * 1024); return status; }
  catch (error) { if (error.code !== 'LIMIT') throw error; }
  const keys = ['id', 'name', 'role', 'profile', 'state', 'settled', 'turns', 'maxTurns', 'updatedAt', 'currentReport', 'latestReport', 'resultAvailable', 'errorAvailable', 'handoffAvailable'];
  const result = { ...status, tasks: status.tasks.map(row => Object.fromEntries(keys.filter(key => row[key] !== undefined).map(key => [key, row[key]]))), detailOmitted: true,
    note: 'Aggregate detail exceeded the IPC budget; inspect selected agentIds for details.' };
  encodeJson(result, 512 * 1024); return result;
}
