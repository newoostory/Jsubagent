import { join } from 'node:path';
import { encodeJson, fail, identifier, safeError } from './limits.mjs';
import { readJson } from './storage.mjs';
import { readResultPage } from './responses.mjs';

export const SEGMENT_RECORD_BYTES = 1024;
export const SEGMENT_INDEX_LABEL = 'agent-result-segment-index-v1';
const natural = value => Number.isSafeInteger(value) && value >= 0;
const sha = value => typeof value === 'string' && /^[a-f0-9]{64}$/.test(value);
export function segmentReference(value) {
  if (!value || value.type !== 'segments-v1' || !natural(value.bytes) || !natural(value.segments) || value.segments < 1 || value.segments > 256 || value.complete !== true) throw fail('Invalid composite result reference', 'STORAGE');
  const index = value.indexRef;
  if (!index || !sha(index.sha256) || index.complete !== true || index.bytes !== value.segments * SEGMENT_RECORD_BYTES) throw fail('Invalid composite index reference', 'STORAGE');
  identifier(index.artifactId);
  return { type: 'segments-v1', indexRef: { artifactId: index.artifactId, bytes: index.bytes, sha256: index.sha256, complete: true }, segments: value.segments, bytes: value.bytes, complete: true };
}
function indexRow(text) {
  let row; try { row = JSON.parse(text.trim()); } catch { throw fail('Invalid result segment record', 'STORAGE'); }
  if (!natural(row.start) || !natural(row.bytes) || !['utf8', 'base64'].includes(row.codec) || !row.ref || !natural(row.ref.bytes) || !sha(row.ref.sha256) || row.ref.complete !== true) throw fail('Invalid result segment record', 'STORAGE');
  identifier(row.ref.artifactId);
  if (row.codec === 'utf8' && row.bytes !== row.ref.bytes || row.codec === 'base64' && (row.ref.bytes % 4 || Math.ceil(row.bytes / 3) * 4 !== row.ref.bytes)) throw fail('Result segment codec/length mismatch', 'STORAGE');
  return row;
}
async function checkedIdentity(store, owner, ref) {
  const info = await store.artifactIdentity(ref.artifactId);
  if (info.owner !== owner || !info.complete || info.bytes !== ref.bytes || info.sha256 !== ref.sha256) throw fail('Result segment owner/identity mismatch', 'PERMISSION');
}
async function decodedSize(store, ref) {
  if (!ref.bytes) return 0;
  if (ref.bytes % 4) throw fail('Malformed legacy result section', 'STORAGE');
  const tail = await store.readArtifact(ref.artifactId, ref.bytes - 4, 4);
  if (!/^[A-Za-z0-9+/]{2}(?:[A-Za-z0-9+/]{2}|[A-Za-z0-9+/]=|==)$/.test(tail.text)) throw fail('Malformed legacy result section padding', 'STORAGE');
  return ref.bytes / 4 * 3 - (tail.text.endsWith('==') ? 2 : tail.text.endsWith('=') ? 1 : 0);
}
// Only the bounded index is written. Existing section blobs are reused, including
// historical base64 sections; no composite content blob or whole-file hash is invented.
export async function persistSectionPrimary(store, owner, manifest, previewBytes = 4096) {
  if (!manifest?.sections?.length || manifest.sections.length > 256) throw fail('Missing result sections', 'STORAGE');
  const index = await store.artifact(owner, SEGMENT_INDEX_LABEL);
  let bytes = 0, count = 0, preview = '';
  try {
    for (const sectionId of manifest.requiredSections) {
      const section = manifest.sections.find(row => row.sectionId === sectionId);
      if (!section) { if (manifest.complete) throw fail('Completed section coverage has a gap', 'STORAGE'); continue; }
      const ref = section.ref;
      await checkedIdentity(store, owner, ref);
      const codec = ref.codec ?? 'base64', logicalBytes = codec === 'utf8' ? ref.bytes : await decodedSize(store, ref);
      if (count) bytes++;
      const row = { start: bytes, bytes: logicalBytes, codec, ref: { artifactId: ref.artifactId, bytes: ref.bytes, sha256: ref.sha256, complete: true } };
      const encoded = encodeJson(row, SEGMENT_RECORD_BYTES - 1), record = Buffer.alloc(SEGMENT_RECORD_BYTES, 32);
      encoded.copy(record); record[record.length - 1] = 10; await index.write(record);
      const remaining = Math.max(0, previewBytes - Buffer.byteLength(preview));
      if (remaining > 4) {
        if (count) preview += '\n';
        const page = await store.readArtifact(ref.artifactId, 0, Math.min(ref.bytes || 1, codec === 'base64' ? Math.ceil((remaining - 1) / 3) * 4 : remaining - 1));
        const body = codec === 'base64' ? Buffer.from(page.text, 'base64') : Buffer.from(page.text);
        let end = Math.min(body.length, remaining - 1);
        while (end > 0 && end < body.length && (body[end] & 0xc0) === 0x80) end--;
        preview += body.subarray(0, end).toString('utf8');
      }
      bytes += logicalBytes; count++;
      if (!natural(bytes)) throw fail('Composite result size overflow', 'LIMIT');
    }
    const sealed = await index.finish(true);
    return { resultRef: segmentReference({ type: 'segments-v1', indexRef: { artifactId: sealed.id, bytes: sealed.bytes, sha256: sealed.sha256, complete: sealed.complete }, segments: count, bytes, complete: true }), preview };
  } catch (error) { if (!index.closed) await index.finish(false, safeError(error)).catch(() => {}); throw error; }
}
export async function verifySegmentReference(store, owner, value) {
  const reference = segmentReference(value), index = reference.indexRef;
  await checkedIdentity(store, owner, index);
  if ((await store.artifactInfo(index.artifactId)).label !== SEGMENT_INDEX_LABEL) throw fail('Wrong result segment index type', 'STORAGE');
  let end = 0;
  for (let n = 0; n < reference.segments; n++) {
    const page = await store.readArtifact(index.artifactId, n * SEGMENT_RECORD_BYTES, SEGMENT_RECORD_BYTES), row = indexRow(page.text);
    if (row.start !== end + (n ? 1 : 0)) throw fail('Non-contiguous result segment index', 'STORAGE');
    await checkedIdentity(store, owner, row.ref); end = row.start + row.bytes;
  }
  if (end !== reference.bytes) throw fail('Composite size/index mismatch', 'STORAGE');
  return reference;
}
async function offlineIdentity(dir, owner, ref, label) {
  const info = await readJson(join(dir, `${identifier(ref.artifactId)}.json`), 8192);
  if (info.owner !== owner || info.id !== ref.artifactId || !info.complete || info.bytes !== ref.bytes || info.sha256 !== ref.sha256 || label && info.label !== label) throw fail('Result segment owner/identity mismatch', 'PERMISSION');
}
function decodePrefix(body, eof) {
  if (body.length && (body[0] & 0xc0) === 0x80) throw fail('Range must begin at a UTF-8 boundary', 'INVALID_RANGE');
  for (let trim = 0; trim <= (eof ? 0 : Math.min(3, body.length)); trim++) {
    try { const bytes = body.subarray(0, body.length - trim); const text = new TextDecoder('utf8', { fatal: true }).decode(bytes); if (bytes.length || !body.length) return { text, bytesRead: bytes.length }; } catch {}
  }
  throw fail('Range must fit one UTF-8 code point', 'INVALID_RANGE');
}
export async function readSegmentPage(dir, owner, value, byteOffset, limitBytes) {
  const reference = segmentReference(value), index = reference.indexRef;
  if (!natural(byteOffset) || byteOffset > reference.bytes || !natural(limitBytes) || !limitBytes || limitBytes > 32768) throw fail('Invalid composite range', 'INVALID_RANGE');
  await offlineIdentity(dir, owner, index, SEGMENT_INDEX_LABEL);
  const rowAt = async n => indexRow((await readResultPage(join(dir, `${index.artifactId}.bin`), n * SEGMENT_RECORD_BYTES, SEGMENT_RECORD_BYTES, index.bytes, index.sha256)).text);
  // Seek directly in the immutable fixed-width index, then read at most one window.
  let lo = 0, hi = reference.segments;
  while (lo < hi) { const mid = Math.floor((lo + hi) / 2), row = await rowAt(mid); if (row.start + row.bytes < byteOffset) lo = mid + 1; else hi = mid; }
  let offset = byteOffset, text = '';
  for (let n = lo; n < reference.segments && offset < reference.bytes && offset - byteOffset < limitBytes; n++) {
    const row = await rowAt(n);
    if (offset < row.start) {
      if (offset !== row.start - 1 || n === 0) throw fail('Composite index gap', 'STORAGE');
      text += '\n'; offset++;
    }
    if (offset >= row.start + row.bytes || offset - byteOffset >= limitBytes) continue;
    await offlineIdentity(dir, owner, row.ref);
    const local = offset - row.start, length = Math.min(limitBytes - (offset - byteOffset), row.bytes - local);
    let part;
    try {
      if (row.codec === 'utf8') {
        part = await readResultPage(join(dir, `${row.ref.artifactId}.bin`), local, length, row.ref.bytes, row.ref.sha256);
        if (part.encoding !== 'utf8') throw fail('Range must fit one UTF-8 code point', 'INVALID_RANGE');
      } else {
        const start = Math.floor(local / 3) * 4, end = Math.min(row.ref.bytes, Math.ceil((local + length) / 3) * 4);
        const raw = await readResultPage(join(dir, `${row.ref.artifactId}.bin`), start, end - start, row.ref.bytes, row.ref.sha256);
        if (!/^[A-Za-z0-9+/]*={0,2}$/.test(raw.text)) throw fail('Malformed legacy section codec', 'STORAGE');
        const body = Buffer.from(raw.text, 'base64').subarray(local % 3, local % 3 + length);
        part = decodePrefix(body, local + length >= row.bytes);
      }
    } catch (error) { if (error.code === 'INVALID_RANGE' && offset > byteOffset) break; throw error; }
    text += part.text; offset += part.bytesRead;
    if (part.bytesRead < length) break;
  }
  if (offset === byteOffset && offset < reference.bytes) throw fail('Composite read made no progress', 'INVALID_RANGE');
  return { text, byteOffset, nextByteOffset: offset, bytesRead: offset - byteOffset, fileBytes: reference.bytes, eof: offset === reference.bytes, encoding: 'utf8', contentIdentity: { type: 'segment-index-sha256', sha256: index.sha256 }, complete: true };
}
