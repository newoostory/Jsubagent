import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import { dirname, posix } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const report = JSON.parse(execFileSync('npm', ['pack', '--dry-run', '--json', '--ignore-scripts'], {
  cwd: root, encoding: 'utf8', maxBuffer: 4 * 1024 * 1024,
}));
// npm versions report either an array or an object keyed by package name.
const pack = Array.isArray(report) ? report[0] : report.jsubagent;
assert.ok(Array.isArray(pack?.files), 'Unrecognized npm pack report');
const files = new Set(pack.files.map(file => file.path));
for (const required of ['index.js', 'package.json', 'README.md', 'LICENSE', 'THIRD_PARTY_NOTICES.md', 'config.example.json', 'agents/scout.md', 'agents/worker.md']) {
  assert.ok(files.has(required), `Missing package asset: ${required}`);
}
for (const file of files) {
  assert.ok(!/(^|\/)(?:node_modules|\.git|\.pi|\.super|wiki|auth\.json|models(?:-store)?\.json)(?:\/|$)/.test(file), `Unexpected private/legacy asset: ${file}`);
  assert.ok(/^(?:index\.js|package\.json|README\.md|LICENSE|THIRD_PARTY_NOTICES\.md|config\.example\.json|src\/|agents\/)/.test(file), `Unexpected package root: ${file}`);
  const text = await readFile(new URL(file, new URL('../', import.meta.url)), 'utf8');
  assert.ok(!/\/Users\/[A-Za-z0-9._-]+\//.test(text), `Personal absolute path in ${file}`);
  assert.ok(!/from ['"]pi-tools\//.test(text), `Unbundled private file-tool import in ${file}`);
  if (!/\.(?:mjs|js)$/.test(file)) continue;
  for (const match of text.matchAll(/(?:from\s*|import\s*\()\s*['"](\.[^'"]+)['"]/g)) {
    const target = posix.normalize(posix.join(dirname(file), match[1]));
    assert.ok(files.has(target), `Unshipped import: ${file} -> ${target}`);
  }
}
console.log(`PASS: ${files.size} package files; static local imports and required assets present; no private installation paths.`);
