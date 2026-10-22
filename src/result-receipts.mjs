// Consumer-local receipt helpers. Storage readers never write or ACK delivery.
const positive = value => Number.isSafeInteger(value) && value > 0;
const id = value => typeof value === 'string' && /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,79}$/.test(value);
const sha = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);
export function resultReceiptKey(page) {
  if (!page || !id(page.agentId)) return undefined;
  if (page.source === 'result_file') {
    const content = sha(page.contentSha256) ? page.contentSha256 : page.contentIdentity?.type === 'segment-index-sha256' && sha(page.contentIdentity.sha256) ? `segments:${page.contentIdentity.sha256}` : undefined;
    if (!sha(page.resultScope) || !positive(page.resultEpoch) || !id(page.bundleId) || !positive(page.revision) || !id(page.fileId) || !positive(page.fileRevision) || !content) return undefined;
    return JSON.stringify(['file', page.resultScope, page.agentId, page.resultEpoch, page.bundleId, page.revision, page.fileId, page.fileRevision, content]);
  }
  if (['result', 'error'].includes(page.source) && id(page.artifactId) && typeof page.state === 'string') return `${page.artifactId}:${page.source}`;
  return undefined;
}
export function storedResultReceipt(receipt) {
  return Boolean(resultReceiptKey(receipt) && typeof receipt.eof === 'boolean' && Number.isSafeInteger(receipt.nextByteOffset) && receipt.nextByteOffset >= 0);
}
export function receiveResultPrefix(page, prior) {
  const key = resultReceiptKey(page);
  if (!key || !storedResultReceipt(page) || !Number.isSafeInteger(page.byteOffset) || page.byteOffset < 0 || page.nextByteOffset < page.byteOffset) return undefined;
  if (prior && resultReceiptKey(prior) !== key) prior = undefined;
  if (page.byteOffset !== 0 && page.byteOffset !== prior?.nextByteOffset) return undefined;
  if (prior?.nextByteOffset > page.nextByteOffset || prior?.nextByteOffset === page.nextByteOffset && prior.eof === page.eof) return undefined;
  const fields = page.source === 'result_file'
    ? ['agentId', 'source', 'resultScope', 'resultEpoch', 'bundleId', 'revision', 'fileId', 'fileRevision', 'contentSha256', 'contentIdentity', 'nextByteOffset', 'eof']
    : ['agentId', 'source', 'artifactId', 'state', 'nextByteOffset', 'eof'];
  return Object.fromEntries(fields.map(field => [field, page[field]]));
}
