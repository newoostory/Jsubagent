// SPDX-License-Identifier: MIT
// Copyright (c) 2026 newoostory
import { spawn } from 'node:child_process';
import { relative } from 'node:path';
import { anchor, failure, snapshot, FILE_TOOL_PROTOCOL } from './hashline.mjs';
import { localPath, MAX_FILE_BYTES, MAX_OUTPUT_BYTES, readFileBounded } from './file-io.mjs';
import { grepSchema, validate } from './schemas.mjs';

// Stream only bounded records; never use a shell, readline's unbounded line buffer,
// ambient ripgrep config/preprocessors, network download or an unbounded stderr.
async function collectMatches(cwd, path, input, signal, maxFileBytes, timeoutMs, guardianPath) {
  signal?.throwIfAborted();
  const limit = input.limit ?? 100;
  const args = ['--no-config', '--json', '--line-number', '--color=never', '--no-follow', '--hidden',
    '--threads', '2', '--max-filesize', String(maxFileBytes), '-g', '!.git',
    ...(input.literal ? ['--fixed-strings'] : []), ...(input.ignoreCase ? ['--ignore-case'] : []),
    ...(input.glob ? ['--glob', input.glob] : []), '--', input.pattern, path];
  return new Promise((resolve, reject) => {
    const child = guardianPath
      ? spawn(process.execPath, [guardianPath], { cwd, env: { ...process.env, NODE_OPTIONS: '', PI_AGENT_TOOL: '1' }, stdio: ['ignore', 'pipe', 'pipe', 'ipc'] })
      : spawn('rg', args, { cwd, stdio: ['ignore', 'pipe', 'pipe'] });
    const matches = []; let pending = Buffer.alloc(0), stderr = '', total = 0, error, limited = false, escalation, guardianResult;
    const stop = () => {
      if (guardianPath && child.connected) child.send({ type: 'cancel' }, () => {});
      else child.kill('SIGTERM');
      escalation ??= setTimeout(() => child.kill('SIGKILL'), guardianPath ? 4000 : 200);
      escalation.unref();
    };
    const abort = () => { error ??= signal.reason ?? failure('Search cancelled.', 'CANCELLED'); stop(); };
    signal?.addEventListener('abort', abort, { once: true });
    if (signal?.aborted) abort();
    child.stdout.on('data', chunk => {
      if (error || limited) return;
      total += chunk.length;
      if (total > 8 * 1024 * 1024) { error = failure('Search transport exceeded 8 MiB. Narrow pattern/path.', 'LIMIT'); stop(); return; }
      pending = Buffer.concat([pending, chunk]);
      let lf;
      while (!error && !limited && (lf = pending.indexOf(10)) >= 0) {
        if (lf > 1024 * 1024) { error = failure('Search record exceeded 1 MiB; narrow the search.', 'LIMIT'); break; }
        const line = pending.subarray(0, lf); pending = pending.subarray(lf + 1);
        try {
          const event = JSON.parse(line.toString('utf8'));
          if (event.type !== 'match') continue;
          const data = event.data;
          if (typeof data?.path?.text !== 'string' || typeof data?.lines?.text !== 'string' || !Number.isSafeInteger(data.line_number) || data.line_number < 1) throw failure('Unsupported ripgrep match encoding.');
          matches.push({ path: data.path.text, line: data.line_number, text: data.lines.text.replace(/\r?\n$/, '') });
          if (matches.length === limit) { limited = true; stop(); }
        } catch (cause) { error = failure(`Invalid or unsupported ripgrep record: ${cause.code ?? 'parse error'}`); }
      }
      if (pending.length > 1024 * 1024) error ??= failure('Search record exceeded 1 MiB; narrow the search.', 'LIMIT');
      if (error) stop();
    });
    child.stderr.on('data', chunk => { if (Buffer.byteLength(stderr) < 8192) stderr += chunk.subarray(0, 8192 - Buffer.byteLength(stderr)).toString('utf8'); });
    child.on('error', cause => { error ??= failure(`Could not start rg (${cause.code ?? 'error'}). Install ripgrep explicitly; no automatic download.`, 'COMMAND_FAILED'); });
    child.on('message', message => { if (message?.type === 'result') guardianResult = message; });
    if (guardianPath) child.send({ type: 'start', command: 'rg', args, cwd, timeoutMs }, cause => { if (cause) { error ??= failure('Could not start grep guardian.', 'COMMAND_FAILED'); stop(); } });
    child.on('close', code => {
      clearTimeout(escalation); signal?.removeEventListener('abort', abort);
      if (guardianResult?.error) error ??= failure(`rg guardian failed: ${guardianResult.error}`, 'COMMAND_FAILED');
      if (guardianResult?.cancelled && !limited) error ??= failure('grep cancelled or timed out.', 'TIMEOUT');
      if (guardianResult?.code !== undefined) code = guardianResult.code;
      if (error) { reject(error); return; }
      if (!limited && code !== 0 && code !== 1) { reject(failure(`rg failed (${code}): ${stderr.slice(0, 2000)}`, 'COMMAND_FAILED')); return; }
      if (!limited && pending.length) { reject(failure('Incomplete ripgrep record.')); return; }
      resolve({ matches, truncated: limited });
    });
  });
}

export async function grepHashline(cwd, input, signal, { resolvePath = localPath, maxOutputBytes = MAX_OUTPUT_BYTES, maxFileBytes = MAX_FILE_BYTES, timeoutMs = 30000, guardianPath, acquire } = {}) {
  validate(grepSchema, input);
  if (/[\r\n\0]/u.test(input.pattern)) throw failure('grep pattern must be a single line without NUL.');
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1) throw failure('Invalid grep timeout.');
  timeoutMs = Math.min(timeoutMs, 30000);
  const deadline = AbortSignal.timeout(timeoutMs);
  const boundedSignal = signal ? AbortSignal.any([signal, deadline]) : deadline;
  boundedSignal.throwIfAborted();
  const release = await acquire?.(boundedSignal);
  try {
    const path = await resolvePath(cwd, input.path ?? '.'); boundedSignal.throwIfAborted();
    const found = await collectMatches(cwd, path, input, boundedSignal, maxFileBytes, timeoutMs, guardianPath);
    const groups = new Map();
    for (const match of found.matches) {
      if (!groups.has(match.path)) groups.set(match.path, []);
      groups.get(match.path).push(match);
    }
    const blocks = []; let bytes = 0, outputLines = 0, matchCount = 0, truncated = found.truncated;
    outer: for (const [filePath, matches] of groups) {
      boundedSignal.throwIfAborted();
      const canonical = await resolvePath(cwd, filePath);
      const doc = snapshot(await readFileBounded(canonical, maxFileBytes, boundedSignal));
      // Never attach a new snapshot's hash to a match from different file content.
      for (const match of matches) {
        const text = match.line === 1 ? match.text.replace(/^\ufeff/, '') : match.text;
        if (doc.lines[match.line - 1]?.text !== text) throw failure('File changed between grep and snapshot. Search again.', 'CONFLICT');
      }
      const header = `File: ${JSON.stringify(relative(cwd, canonical) || canonical)}\nexpectedSha256: ${doc.sha256}\n`;
      const rows = [], seen = new Set(); let blockBytes = Buffer.byteLength(header);
      for (const match of matches) {
        const context = input.context ?? 0;
        for (let number = Math.max(1, match.line - context); number <= Math.min(doc.lines.length, match.line + context); number++) {
          if (seen.has(number)) continue;
          const line = doc.lines[number - 1].text;
          const row = `${anchor(number, line)}|${line}`;
          if (bytes + blockBytes + Buffer.byteLength(row) + 1 > maxOutputBytes - 256 || outputLines === 2000) {
            truncated = true;
            if (rows.length) blocks.push(header + rows.join('\n'));
            break outer;
          }
          seen.add(number); rows.push(row); blockBytes += Buffer.byteLength(row) + 1; outputLines++;
        }
        matchCount++;
      }
      blocks.push(header + rows.join('\n')); bytes += blockBytes + 2;
    }
    boundedSignal.throwIfAborted();
    let text = blocks.join('\n\n');
    if (truncated) text += '\n[Search output limited. Narrow path/pattern/glob or read the file; omitted/partial lines have no anchors.]';
    else if (!found.matches.length) text = 'No matches found.';
    return { text, format: FILE_TOOL_PROTOCOL, matchCount, outputLines, truncated };
  } finally { release?.(); }
}
