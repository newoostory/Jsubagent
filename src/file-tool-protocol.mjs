import { FILE_TOOL_PROTOCOL } from './vendor/pi-tools/schemas.mjs';
import { fail } from './limits.mjs';

export const BUILTIN_FILE_TOOL_PROTOCOL = 'pi-builtin';
export function fileToolProtocol(profile = {}) {
  const protocol = profile.fileToolProtocol === undefined ? FILE_TOOL_PROTOCOL : profile.fileToolProtocol;
  if (![FILE_TOOL_PROTOCOL, BUILTIN_FILE_TOOL_PROTOCOL].includes(protocol)) throw fail('Unsupported file tool protocol; no legacy fallback.', 'CAPABILITY');
  return protocol;
}
export function assertFileToolSelection(profile, allowedNames = []) {
  const protocol = fileToolProtocol(profile);
  // The opt-in native lane covers trans's existing read/write subset only.
  // Never silently mix hashline edit/grep into a native profile or add permissions.
  if (protocol === BUILTIN_FILE_TOOL_PROTOCOL && allowedNames.some(name => ['edit', 'grep'].includes(name))) throw fail('pi-builtin file tools currently support read/write only; incompatible role tool allowlist.', 'CAPABILITY');
  return protocol;
}
