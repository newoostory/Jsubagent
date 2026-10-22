import assert from 'node:assert/strict';
import { readdir, readFile } from 'node:fs/promises';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';
const root = fileURLToPath(new URL('../', import.meta.url));
const files = ['index.js'];
for (const dir of ['src', 'scripts', 'test']) for (const entry of await readdir(join(root, dir), { recursive: true })) if (/\.(mjs|js)$/.test(entry)) files.push(`${dir}/${entry}`);
for (const file of files) {
  const result = spawnSync(process.execPath, ['--check', join(root, file)], { encoding: 'utf8', maxBuffer: 256 * 1024, timeout: 10_000 });
  assert.equal(result.status, 0, `${file}: ${result.stderr ?? result.error}`);
}
const manifest = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'));
assert.deepEqual(manifest.pi.extensions, ['./index.js']);
console.log(`PASS: ${files.length} JS/ESM entrypoints parse; explicit Pi manifest is intact.`);
