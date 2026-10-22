import { spawn } from 'node:child_process';
import { readdir } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../', import.meta.url));
const testDir = join(root, 'test');
const files = (await readdir(testDir, { withFileTypes: true }))
  .filter(entry => entry.isFile() && entry.name.endsWith('.test.mjs'))
  .map(entry => join(testDir, entry.name))
  .sort();

if (!files.length) throw new Error('No test/*.test.mjs files found');

const env = { ...process.env };
delete env.PI_AGENT_HOST;
delete env.PI_AGENT_TOOL;

// Each integration file may itself start four SDK workers. Bound harness
// parallelism separately from the concurrency behavior exercised by the tests.
const concurrency = Number(process.env.PI_AGENT_TEST_CONCURRENCY ?? 2);
if (!Number.isSafeInteger(concurrency) || concurrency < 1 || concurrency > 8) throw new Error('PI_AGENT_TEST_CONCURRENCY must be 1..8');
const child = spawn(process.execPath, ['--test', `--test-concurrency=${concurrency}`, ...files], { cwd: root, env, stdio: 'inherit' });
const forwardInt = () => child.kill('SIGINT');
const forwardTerm = () => child.kill('SIGTERM');
process.once('SIGINT', forwardInt);
process.once('SIGTERM', forwardTerm);

child.once('error', error => {
  console.error(error);
  process.exitCode = 1;
});

child.once('exit', (code, signal) => {
  process.removeListener('SIGINT', forwardInt);
  process.removeListener('SIGTERM', forwardTerm);
  if (signal) process.kill(process.pid, signal);
  else process.exitCode = code ?? 1;
});
