import { registerHooks } from 'node:module';
import { join } from 'node:path';
import { pathToFileURL } from 'node:url';

// Pi aliases host peers while loading an extension. Native child processes
// need the same physical SDK without installing duplicate host packages.
const piPackageDir = process.env.JSUBAGENT_PI_PACKAGE_DIR;
if (piPackageDir) {
  const parentURL = pathToFileURL(join(piPackageDir, 'package.json')).href;
  const peers = new Set([
    '@earendil-works/pi-coding-agent', '@earendil-works/pi-ai',
    '@earendil-works/pi-tui', 'typebox', 'typebox/value',
  ]);
  registerHooks({
    resolve(specifier, context, nextResolve) {
      // Preserve import/require conditions; the SDK has import-only exports.
      return nextResolve(specifier, peers.has(specifier) ? { ...context, parentURL } : context);
    },
  });
}
