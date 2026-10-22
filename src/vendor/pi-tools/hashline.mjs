// SPDX-License-Identifier: MIT
// Copyright (c) 2026 newoostory
// Local hashline implementation; no third-party edit engine.
import { createHash } from 'node:crypto';

export const sha256 = value => createHash('sha256').update(value).digest('hex');
export function failure(message, code = 'INVALID') { return Object.assign(new Error(message), { code }); }
export const LINE_HASH_CHARS = 4;
export const ANCHOR_PATTERN = `^[1-9][0-9]*:[0-9a-f]{${LINE_HASH_CHARS}}$`;
export const ANCHOR_MAX_LENGTH = 17 + LINE_HASH_CHARS;
export const FILE_TOOL_PROTOCOL = `hashline-sha256-${LINE_HASH_CHARS}`;
const anchorPattern = new RegExp(ANCHOR_PATTERN);
export const anchor = (number, line) => `${number}:${sha256(line).slice(0, LINE_HASH_CHARS)}`;

// Read-only paging needs full-file validation/hash and the total line count,
// but not an edit snapshot object for every line. Retain only the requested
// window; edits continue using the complete snapshot below.
export function pageBuffer(buffer, input = {}, maxBytes = 50 * 1024) {
  const { offset = 1, limit = 2000 } = input;
  if (!Number.isSafeInteger(offset) || offset < 1 || !Number.isSafeInteger(limit) || limit < 1 || limit > 2000) throw failure('read offset >=1 and limit 1..2000 must be integers.');
  let raw;
  try { raw = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(buffer); }
  catch { throw failure('Hashline requires valid UTF-8; use a byte read for binary data.'); }
  if (raw.includes('\0')) throw failure('Hashline does not support NUL/binary files.');
  const body = raw.startsWith('\ufeff') ? raw.slice(1) : raw;
  const lines = { length: 0 }; let start = 0;
  while (start < body.length) {
    if (lines.length >= 200000) throw failure('Text snapshot exceeds 200000 lines; use raw byte paging.', 'LIMIT');
    const lf = body.indexOf('\n', start), end = lf < 0 ? body.length : lf + 1;
    const index = lines.length++;
    if (index >= offset - 1 && index < offset - 1 + limit) {
      const eolBytes = lf < 0 ? 0 : body[lf - 1] === '\r' ? 2 : 1;
      lines[index] = { text: body.slice(start, end - eolBytes) };
    }
    start = end;
  }
  const page = pageSnapshot({ sha256: sha256(buffer), lines }, input, maxBytes);
  // A one-line rope/slice may otherwise retain the entire decoded file behind
  // a tiny model result. Copy only the bounded returned page.
  return { ...page, text: Buffer.from(page.text, 'utf8').toString('utf8') };
}

export function snapshot(buffer) {
  let raw;
  try { raw = new TextDecoder('utf-8', { fatal: true, ignoreBOM: true }).decode(buffer); }
  catch { throw failure('Hashline requires valid UTF-8; use a byte read for binary data.'); }
  if (raw.includes('\0')) throw failure('Hashline does not support NUL/binary files.');
  const bom = raw.startsWith('\ufeff') ? '\ufeff' : '';
  const body = raw.slice(bom.length), lines = [];
  let start = 0;
  while (start < body.length) {
    if (lines.length >= 200000) throw failure('Text snapshot exceeds 200000 lines; use raw byte paging.', 'LIMIT');
    const lf = body.indexOf('\n', start);
    const end = lf < 0 ? body.length : lf + 1;
    const eol = lf < 0 ? '' : body[lf - 1] === '\r' ? '\r\n' : '\n';
    lines.push({ text: body.slice(start, end - eol.length), start, end, eol });
    start = end;
  }
  return { raw, body, bom, lines, sha256: sha256(buffer), eol: lines.find(line => line.eol)?.eol ?? '\n' };
}

export function applyHashline(doc, input, maxBytes = 2 * 1024 * 1024) {
  if (input.expectedSha256 !== doc.sha256) throw failure('File changed: expectedSha256 mismatch. Read or grep again; do not guess anchors.', 'CONFLICT');
  if (!Array.isArray(input.edits) || input.edits.length < 1 || input.edits.length > 32) throw failure('edit requires 1..32 edits.');
  const lookup = token => {
    if (typeof token !== 'string' || token.length > ANCHOR_MAX_LENGTH || !anchorPattern.test(token)) throw failure(`Expected a LINE:${LINE_HASH_CHARS}hex anchor from a fresh read/grep; old anchors are not converted.`);
    const number = Number(token.split(':')[0]), line = doc.lines[number - 1];
    if (!Number.isSafeInteger(number) || !line || anchor(number, line.text) !== token) throw failure('Stale or invalid hashline anchor. Read or grep again.', 'CONFLICT');
    return { number, ...line };
  };
  let addedBytes = 0;
  const changes = input.edits.map(edit => {
    if (!edit || !['replace', 'insert_before', 'insert_after'].includes(edit.op)) throw failure('Invalid hashline operation.');
    if (!Array.isArray(edit.newLines)) throw failure('newLines must be an array; [] deletes a replace range.');
    for (const line of edit.newLines) {
      if (typeof line !== 'string' || /[\r\n\0]/u.test(line) || !line.isWellFormed()) throw failure('Each newLines item must be valid text without CR, LF or NUL.');
      addedBytes += Buffer.byteLength(line) + 2;
      if (addedBytes > maxBytes) throw failure('Replacement input exceeds edit byte limit.', 'LIMIT');
    }
    const first = lookup(edit.start), last = edit.end === undefined ? first : lookup(edit.end);
    if (last.number < first.number) throw failure('Reversed hashline range.');
    if (edit.op !== 'replace' && (edit.end !== undefined || !edit.newLines.length)) throw failure('Insert requires nonempty newLines and no end.');
    let start = first.start, end = last.end, replacement = edit.newLines.join(doc.eol);
    if (edit.op === 'replace') {
      if (edit.newLines.length && (last.eol || edit.newLines.at(-1) === '')) replacement += doc.eol;
    } else if (edit.op === 'insert_before') {
      end = start; replacement += doc.eol;
    } else {
      start = end = first.end;
      replacement = first.eol ? replacement + doc.eol : doc.eol + replacement + (edit.newLines.at(-1) === '' ? doc.eol : '');
    }
    return { start, end, replacement, first: first.number, last: last.number };
  }).sort((a, b) => a.start - b.start || a.end - b.end);
  for (let i = 1; i < changes.length; i++) {
    const a = changes[i - 1], b = changes[i];
    if (b.start < a.end || b.start === a.start || (b.start === a.end && (a.start === a.end || b.start === b.end)) || b.first <= a.last) {
      throw failure('Overlapping edits or ambiguous insert boundary; merge them into one edit.', 'CONFLICT');
    }
  }
  let body = doc.body;
  for (const change of [...changes].reverse()) body = body.slice(0, change.start) + change.replacement + body.slice(change.end);
  const next = doc.bom + body;
  if (Buffer.byteLength(next) > maxBytes) throw failure('Edited file exceeds edit byte limit.', 'LIMIT');
  return next;
}

export function pageSnapshot(doc, { offset = 1, limit = 2000 } = {}, maxBytes = 50 * 1024) {
  if (!Number.isSafeInteger(offset) || offset < 1 || !Number.isSafeInteger(limit) || limit < 1 || limit > 2000) throw failure('read offset >=1 and limit 1..2000 must be integers.');
  if (offset > Math.max(1, doc.lines.length)) throw failure('Offset is beyond end of file.');
  const header = `Hashline — expectedSha256: ${doc.sha256}\n`;
  const rows = []; let bytes = Buffer.byteLength(header), nextOffset = offset;
  const end = Math.min(doc.lines.length, offset - 1 + limit);
  for (let i = offset - 1; i < end; i++) {
    const row = `${anchor(i + 1, doc.lines[i].text)}|${doc.lines[i].text}`;
    if (bytes + Buffer.byteLength(row) + 1 > maxBytes - 256) {
      if (!rows.length) throw failure(`Line ${i + 1} exceeds the output byte limit; use a raw byte read or bounded shell command. No partial anchor returned.`, 'LIMIT');
      break;
    }
    rows.push(row); bytes += Buffer.byteLength(row) + 1; nextOffset = i + 2;
  }
  const truncated = nextOffset <= doc.lines.length;
  const notice = truncated ? `\n[More lines. Use offset=${nextOffset} to continue; reread if expectedSha256 changes.]` : doc.lines.length ? '' : '[Empty file; use write to populate it.]';
  return { text: header + rows.join('\n') + notice, sha256: doc.sha256, format: FILE_TOOL_PROTOCOL, offset,
    nextOffset: truncated ? nextOffset : null, totalLines: doc.lines.length, outputLines: rows.length, truncated };
}
