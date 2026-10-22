import { Check } from 'typebox/value';
import { encodeJson, fail } from './limits.mjs';
import { isOfficial } from './official-names.mjs';

export const WEB_RESULT_BYTES = 128 * 1024;
export function hasWebRuntime() {
  try { import.meta.resolve('pi-web-fusion/runtime'); return true; }
  catch (error) {
    if (error.code === 'ERR_MODULE_NOT_FOUND' && error.message.includes("'pi-web-fusion'")) return false;
    throw error;
  }
}
export async function loadWebRuntime({ optional = false } = {}) {
  if (optional && !hasWebRuntime()) return null;
  try {
    const runtime = await import('pi-web-fusion/runtime');
    if (typeof runtime.createFetchSession !== 'function') throw fail('Fetch snapshots require the current web runtime.', 'CAPABILITY');
    return runtime;
  }
  catch { throw fail('The configured pi-web-fusion/runtime is unavailable or incompatible. No provider fallback.', 'CAPABILITY'); }
}
function publicOutcome(runtime, error) {
  if (error?.code === 'CAPABILITY') return { ok: false, error: { code: 'search_backend_unavailable', message: 'Search requires an explicitly selected Codex model; no provider substitution was performed.' } };
  // Never persist arbitrary transport error messages, request headers or tokens.
  const { error: safe } = runtime.publicError(error);
  const code = /^[a-z_]{1,64}$/.test(safe.code) ? safe.code : 'internal_error';
  return { ok: false, error: { code, message: `Bounded web operation failed (${code}). Inspect authorization/scope before retrying.` } };
}
export function webSearchContext(ctx) {
  if (ctx?.model?.provider === 'openai-codex') return ctx;
  throw fail('No automatic cross-provider search backend. Use public URL fetch or a separately authorized search provider.', 'CAPABILITY');
}
export async function createWebTools({ task, hooks, evidence = [], runtime }) {
  if (task.workItem) throw fail('Universal inspection has no network web capability', 'UNSUPPORTED');
  const required = task.role?.requiredTools?.some(name => ['web_search', 'web_fetch'].includes(name));
  runtime ??= await loadWebRuntime({ optional: !required });
  if (!runtime) return [];
  const fetchSession = runtime.createFetchSession?.();
  const tools = ['search', 'fetch'].map(kind => {
    const schema = kind === 'search' ? runtime.WebSearchSchema : runtime.WebFetchSchema;
    return { name: `web_${kind}`, label: `Bounded web ${kind}`, parameters: schema,
      description: kind === 'search' ? 'Optional Codex-backed public search, disabled by default. Requires explicit host opt-in and an already selected Codex model; never changes another executor to Codex. Only an authorized public query may be sent. Returns bounded sources and a durable receipt; a link alone is not verification. Use web_fetch for known public URLs.' : 'Read an explicit public URL once, then page the cached snapshot using returned next (URL/snapshot_id/offset/focus). Default 4000 characters; find locates exact text in the complete snapshot. Expired/unavailable IDs never refetch; omit ID only for an intentional fresh fetch. Snapshots last 10min or until eviction/run end. Public-IP/DNS/TLS/redirect safety; no role/host quota, cookies, login or browser. A bounded receipt records evidence; inaccessible or clipped content remains unverified.',
      execute: async (_id, input, signal, _update, ctx) => {
        if (!Check(schema, input)) throw fail('Invalid bounded web arguments');
        encodeJson(input, 8192); signal?.throwIfAborted();
        // Reservation must be durably committed before authentication or network.
        const attempt = await hooks.beginWeb(kind, input);
        const combined = AbortSignal.any([signal, ctx?.signal, AbortSignal.timeout(kind === 'search' ? runtime.BUDGETS.searchDeadlineMs : runtime.BUDGETS.fetchDeadlineMs)].filter(Boolean));
        let outcome;
        try {
          combined.throwIfAborted();
          outcome = kind === 'search'
            ? await runtime.webSearch(input, combined, undefined, webSearchContext(ctx))
            : await (fetchSession ? fetchSession.fetch(input, combined, undefined) : runtime.webFetch(input, combined, undefined));
          combined.throwIfAborted();
          encodeJson(outcome, WEB_RESULT_BYTES);
        } catch (error) { outcome = publicOutcome(runtime, error); }
        // Submitted arguments + bounded public output are retained. Receipt
        // persistence failure stops the host; it is not converted to network success.
        const receipt = `web-${kind}-${attempt}`;
        const reference = await hooks.finishWeb(kind, attempt, { receipt, input, outcome });
        if (outcome.ok && isOfficial(task)) evidence.push({ kind, input, outcome, receipt });
        const value = { ...outcome, receipt, ...(!isOfficial(task) ? { artifactId: reference.artifactId } : {}) };
        if (!outcome.ok) throw fail(JSON.stringify(value), 'WEB');
        // The official lane gets public evidence only, never local artifact IDs.
        return { content: [{ type: 'text', text: encodeJson(value, WEB_RESULT_BYTES + 4096).toString('utf8') }] };
      },
    };
  });
  Object.defineProperty(tools, 'dispose', { value: () => fetchSession?.dispose() });
  return tools;
}
