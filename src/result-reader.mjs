import { encodeJson, fail } from './limits.mjs';
import { validatePublicAgentParameters } from './public-api.mjs';
import { readResultService } from './results.mjs';

// No authority comes from inputBindingId, cursor or view. The runtime resolves
// and supplies a durable grant before entering this read-only adapter.
export async function readGrantedResult(root, request, grant, assertCurrent) {
  const { inputBindingId, ...params } = request;
  if (!['bundle', 'result_file'].includes(params.source)) throw fail('Child result reader exposes no other agent actions', 'PERMISSION');
  validatePublicAgentParameters({ action: 'read', requests: [params] });
  let p = { ...params }, pageSize = 8, limitBytes = Math.min(params.limitBytes ?? 4096, 16384);
  if (grant && !p.cursor) { p.bundleId ??= grant.bundleId; p.revision ??= grant.revision; if (p.source === 'bundle') p.view ??= grant.selector.view ?? 'artifacts'; }
  const adaptedRead = read => ({ tool: 'agent_read', arguments: { ...read.requests[0], ...(grant ? { inputBindingId: grant.inputBindingId } : {}) } });
  for (;;) {
    assertCurrent();
    const page = await readResultService(root, { ...p, ...(p.source === 'result_file' ? { limitBytes } : {}) }, { pageSize, grant });
    assertCurrent();
    if (page.bundleId && !p.cursor) { p.bundleId = page.bundleId; p.revision = page.revision; }
    if (page.files) page.files = page.files.map(file => ({ ...file, read: adaptedRead(file.read) }));
    if (page.primary?.read) page.primary = { ...page.primary, read: adaptedRead(page.primary.read) };
    if (page.read) page.read = adaptedRead(page.read);
    if (page.eof === false) page.read = { tool: 'agent_read', arguments: { ...p, byteOffset: page.nextByteOffset, limitBytes: params.limitBytes ?? 4096, ...(grant ? { inputBindingId: grant.inputBindingId } : {}) } };
    try { encodeJson(page, 32768); return page; }
    catch (error) {
      if (page.source === 'bundle') { if (pageSize === 1) throw error; pageSize = Math.max(1, Math.floor(pageSize / 2)); }
      else { if (limitBytes === 1) throw error; limitBytes = Math.max(1, Math.floor(limitBytes / 2)); }
    }
  }
}
