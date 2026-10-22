import { fail, safeError, short } from '../limits.mjs';

export async function persistCanonicalResult(store, owner, result, previewBytes) {
  if (typeof result !== 'string') throw fail('Task result must be text', 'LIMIT');
  const artifact = await store.artifact(owner, 'agent-result-text');
  try {
    const body = Buffer.from(result, 'utf8');
    for (let offset = 0; offset < body.length; offset += 65536) await artifact.write(body.subarray(offset, offset + 65536));
    const sealed = await artifact.finish(true);
    if (!sealed.complete || sealed.bytes !== body.length || typeof sealed.sha256 !== 'string') throw fail('Canonical result artifact seal mismatch', 'STORAGE');
    return {
      resultRef: { artifactId: sealed.id, bytes: sealed.bytes, sha256: sealed.sha256, complete: true },
      preview: short(result, previewBytes),
    };
  } catch (error) {
    if (!artifact.closed) await artifact.finish(false, safeError(error)).catch(() => {});
    throw error;
  }
}
