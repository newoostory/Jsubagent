import { Type } from 'typebox';
import { Check } from 'typebox/value';
import { fail, encodeJson, identifier } from './limits.mjs';

export const WEB_TOOLS = ['web_search', 'web_fetch'];
export const WEB_RECEIPT_INDEX = 48;
// Stored historical accounting is validated without accepting policy inputs.
const StoredWebPolicySchema = Type.Object({
  searchQueries: Type.Array(Type.String({ minLength: 1, maxLength: 2000, pattern: '\\S' }), { maxItems: 16, uniqueItems: true }),
  allowedHosts: Type.Array(Type.String({ minLength: 1, maxLength: 253 }), { minItems: 1, maxItems: 16, uniqueItems: true }),
  maxSearches: Type.Integer({ minimum: 0, maximum: 16 }),
  maxFetches: Type.Integer({ minimum: 0, maximum: 32 }),
}, { additionalProperties: false });
export function validateWebState(task) {
  if (!task.web) {
    if (task.webUsed || task.webReceipts?.length) throw fail('Corrupt stored web state', 'STORAGE');
    return;
  }
  if (task.web.mode !== 'public') {
    if (!Check(StoredWebPolicySchema, task.web)) throw fail('Corrupt stored web policy', 'STORAGE');
    encodeJson(task.web, 32 * 1024);
  }
  if (!task.webUsed || !Array.isArray(task.webReceipts) || task.webReceipts.length > WEB_RECEIPT_INDEX) throw fail('Missing stored web reservations; refusing to reset accounting', 'STORAGE');
  for (const kind of ['search', 'fetch']) {
    const used = task.webUsed[kind], rows = task.webReceipts.filter(row => row.kind === kind);
    if (!Number.isSafeInteger(used) || used < 0 || rows.some((row, i) => !Number.isSafeInteger(row.attempt) || row.attempt < 1 || row.attempt > used || (i && row.attempt !== rows[i - 1].attempt + 1) || !['pending', 'succeeded', 'failed'].includes(row.state) || (row.state !== 'pending' && !row.artifactId)) || (rows.length && rows.at(-1).attempt !== used)) throw fail('Corrupt web reservation accounting', 'STORAGE');
  }
  const total = task.webUsed.search + task.webUsed.fetch;
  if (!Number.isSafeInteger(total) || task.webReceipts.length !== Math.min(total, WEB_RECEIPT_INDEX)) throw fail('Corrupt web receipt index', 'STORAGE');
  for (const row of task.webReceipts) {
    if (!['search', 'fetch'].includes(row.kind)) throw fail('Invalid web reservation kind', 'STORAGE');
    if (row.artifactId) identifier(row.artifactId);
    if (row.reservationArtifactId) identifier(row.reservationArtifactId);
  }
}
export function enablePublicWeb(task) {
  validateWebState(task);
  task.web = { mode: 'public' };
  task.webUsed ??= { search: 0, fetch: 0 };
  task.webReceipts ??= [];
}
export function assertWebCall(_task, kind, input) {
  // Public network/credential/SSRF and parser limits are transport safeguards,
  // not role or task allowlists. The actual tool schema validates the full input.
  if (kind === 'search') {
    if (typeof input.query !== 'string' || !input.query.trim()) throw fail('Search query is required');
  } else if (kind === 'fetch') {
    let url; try { url = new URL(input.url); } catch { throw fail('Invalid public URL'); }
    if (!['http:', 'https:'].includes(url.protocol) || url.username || url.password || url.port) throw fail('Use a public HTTP(S) URL without credentials or custom ports', 'PERMISSION');
  } else throw fail('Unknown web operation');
}
