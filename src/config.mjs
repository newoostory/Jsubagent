import { join, resolve, dirname, isAbsolute } from 'node:path';
import { fileURLToPath } from 'node:url';
import { readdir, realpath } from 'node:fs/promises';
import { LIMITS, MAX_RUNTIME_CONCURRENCY, text, identifier, integer, hash, encodeJson, fail } from './limits.mjs';
import { readBounded, readJson } from './storage.mjs';
import { fileEffectGrant } from './file-effect.mjs';
import { gitSnapshotGrant } from './git-snapshot.mjs';
import { fileSnapshotGrant } from './file-snapshot.mjs';
import { fileToolProtocol, assertFileToolSelection } from './file-tool-protocol.mjs';
import { PROFILE_INSTRUCTION_BYTES } from './model-guidance.mjs';
// Reserved benchmark names are normalized by the host, even for older main preflight runtimes.
const BENCHMARK_ROLES = new Set(['bench-solo', 'bench-draft', 'bench-critique', 'bench-integrate']);

export const packageRoot = fileURLToPath(new URL('../', import.meta.url));
export const BUILTIN_TOOLS = ['read', 'grep', 'find', 'ls', 'bash', 'edit', 'write', 'artifact_read', 'agent_send', 'agent_inbox', 'agent_checkpoint', 'agent_result', 'agent_read', 'web_search', 'web_fetch'];
const levels = ['off', 'minimal', 'low', 'medium', 'high', 'xhigh', 'max'];
function keys(value, allowed, label) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw fail(`${label} must be an object`);
  for (const key of Object.keys(value)) if (!allowed.includes(key)) throw fail(`Unknown ${label} key: ${key}`);
}
async function optionalJson(path) { try { return await readJson(path, 128 * 1024); } catch (e) { if (e.code === 'ENOENT') return {}; throw e; } }
export async function loadConfiguration({ agentDir, cwd, trusted = false, configPath }) {
  const globalPath = configPath ?? join(agentDir, 'pi-agent', 'config.json');
  const global = await optionalJson(globalPath);
  keys(global, ['limits', 'profiles', 'roles', 'providerModules', 'modelsPath', 'authPath', 'trustedEffectGrant', 'trustedGitSnapshotGrant', 'trustedFileSnapshotGrant', 'routingProfiles'], 'global config');
  const projectPath = join(cwd, '.pi', 'pi-agent.json');
  const project = trusted ? await optionalJson(projectPath) : {};
  keys(project, ['profiles', 'roles'], 'project config');
  for (const profile of Object.values(project.profiles ?? {})) if (Object.hasOwn(profile, 'allowBackground')) throw fail('Background approval must come from user-level configuration', 'PERMISSION');
  const limits = { ...LIMITS };
  if (global.limits) {
    keys(global.limits, ['concurrency', 'commandConcurrency', 'maxTasks', 'maxTurns', 'rootTurns', 'rootTimeMs', 'rootStagnantSegments', 'rateLimitBaseMs', 'rateLimitMaxMs', 'providerRetryLimit', 'autoWakeLimit', 'artifactBytes', 'diskBytes', 'minFreeBytes', 'commandTimeoutMs', 'jobTimeoutMs', 'memorySoftBytes', 'memoryStopBytes', 'aggregateBudgetMiB', 'taskMemoryBytes', 'workerStartupBytes', 'workerMemoryBytes', 'workerEmergencyBytes', 'workerInitial', 'workerMax', 'workerMaxRuns', 'workerHeapMiB', 'workerIdleKeep', 'workerIdleRetireMs'], 'limits');
    if (Object.hasOwn(global.limits, 'jobTimeoutMs')) throw fail('limits.jobTimeoutMs is unsupported and has no effect; remove it and use limits.rootTimeMs for the durable cumulative execution-time budget');
    for (const [key, value] of Object.entries(global.limits)) {
      if (['memorySoftBytes', 'memoryStopBytes', 'aggregateBudgetMiB', 'rootTurns', 'rootTimeMs', 'autoWakeLimit', 'workerHeapMiB', 'workerMemoryBytes', 'workerEmergencyBytes'].includes(key) && value === null) limits[key] = null;
      else limits[key] = integer(value, key, ['workerMaxRuns', 'workerIdleKeep'].includes(key) ? 0 : 1, Number.MAX_SAFE_INTEGER);
    }
  }
  // Transport/memory boundaries have tested upper bounds. Workload/storage limits remain configurable.
  integer(limits.concurrency, 'concurrency', 1, MAX_RUNTIME_CONCURRENCY); integer(limits.commandConcurrency, 'commandConcurrency', 1, MAX_RUNTIME_CONCURRENCY);
  integer(limits.maxTasks, 'maxTasks', 8, 128);
  integer(limits.commandTimeoutMs, 'commandTimeoutMs', 1, 86_400_000);
  integer(limits.maxTurns, 'maxTurns', 1, 4096);
  if (limits.rootTurns !== null) integer(limits.rootTurns, 'rootTurns', 1, 65536);
  if (limits.autoWakeLimit !== null) integer(limits.autoWakeLimit, 'autoWakeLimit', 1, 1024);
  if (limits.rootTimeMs !== null) integer(limits.rootTimeMs, 'rootTimeMs', 1, 365 * 86_400_000);
  integer(limits.rootStagnantSegments, 'rootStagnantSegments', 1, 1024);
  integer(limits.rateLimitBaseMs, 'rateLimitBaseMs', 1, 86_400_000);
  integer(limits.rateLimitMaxMs, 'rateLimitMaxMs', limits.rateLimitBaseMs, 86_400_000);
  integer(limits.providerRetryLimit, 'providerRetryLimit', 1, 100);
  integer(limits.taskMemoryBytes, 'taskMemoryBytes', 1, limits.memoryStopBytes ?? Number.MAX_SAFE_INTEGER);
  integer(limits.workerStartupBytes, 'workerStartupBytes', 1, limits.memoryStopBytes ?? Number.MAX_SAFE_INTEGER);
  if (limits.workerMemoryBytes !== null) integer(limits.workerMemoryBytes, 'workerMemoryBytes', 1, limits.memoryStopBytes ?? Number.MAX_SAFE_INTEGER);
  if (limits.workerEmergencyBytes !== null) integer(limits.workerEmergencyBytes, 'workerEmergencyBytes', limits.workerMemoryBytes ?? 1, Number.MAX_SAFE_INTEGER);
  if (limits.aggregateBudgetMiB !== null) integer(limits.aggregateBudgetMiB, 'aggregateBudgetMiB', 1, Math.floor(Number.MAX_SAFE_INTEGER / (1024 * 1024)));
  integer(limits.workerInitial, 'workerInitial', 1, MAX_RUNTIME_CONCURRENCY); integer(limits.workerMax, 'workerMax', limits.workerInitial, MAX_RUNTIME_CONCURRENCY);
  integer(limits.workerMaxRuns, 'workerMaxRuns', 0, 10_000);
  if (limits.workerHeapMiB !== null) integer(limits.workerHeapMiB, 'workerHeapMiB', 128, 8192);
  integer(limits.workerIdleKeep, 'workerIdleKeep', 0, limits.workerMax);
  integer(limits.workerIdleRetireMs, 'workerIdleRetireMs', 1000, 86_400_000);
  for (const key of ['frameBytes', 'queueBytes', 'previewBytes', 'readBytes', 'inputBytes', 'roleBytes', 'messageBytes', 'mailboxCount', 'mailboxBytes', 'checkpointBytes', 'stateBytes', 'responseBytes', 'contextBytes', 'editBytes']) {
    if (limits[key] > LIMITS[key]) throw fail(`${key} cannot exceed tested boundary ${LIMITS[key]}`);
  }
  if (limits.memorySoftBytes !== null && limits.memoryStopBytes !== null && limits.memorySoftBytes >= limits.memoryStopBytes) throw fail('memorySoftBytes must be below memoryStopBytes');
  if (limits.artifactBytes > limits.diskBytes) throw fail('artifactBytes must not exceed diskBytes');
  const profiles = { main: { inherit: true }, ...(global.profiles ?? {}), ...(project.profiles ?? {}) };
  if (Object.keys(profiles).length > 64) throw fail('Too many profiles');
  for (const [name, p] of Object.entries(profiles)) {
    identifier(name, 'profile'); keys(p, ['inherit', 'provider', 'model', 'thinkingLevel', 'maxConcurrent', 'modelConcurrent', 'account', 'memoryBytes', 'toolProcesses', 'allowBackground', 'hidden', 'requiredMode', 'instructions', 'fileToolProtocol'], 'profile');
    fileToolProtocol(p);
    if (name === 'bench-camel-auto') {
      if (p.hidden !== true || p.inherit || p.provider !== 'camel-stream' || p.model !== 'auto' || (p.requiredMode && p.requiredMode !== 'benchmark')) throw fail('Reserved benchmark profile must remain hidden and Camel Auto');
      p.requiredMode = 'benchmark';
    }
    if (p.instructions !== undefined) text(p.instructions, 'profile instructions', PROFILE_INSTRUCTION_BYTES, { empty: true });
    if (p.requiredMode !== undefined) identifier(p.requiredMode, 'profile requiredMode');
    if (p.hidden !== undefined && typeof p.hidden !== 'boolean') throw fail('Profile hidden must be boolean');
    if (p.allowBackground !== undefined && typeof p.allowBackground !== 'boolean') throw fail('Profile allowBackground must be boolean');
    if (p.inherit !== true) { text(p.provider, 'provider', 120); text(p.model, 'model', 256); }
    else if (p.provider || p.model) throw fail('Inherited profile cannot also specify provider/model');
    if (p.thinkingLevel && !levels.includes(p.thinkingLevel)) throw fail('Invalid thinkingLevel');
    if (p.maxConcurrent !== undefined) integer(p.maxConcurrent, 'maxConcurrent', 1, MAX_RUNTIME_CONCURRENCY);
    if (p.modelConcurrent !== undefined) integer(p.modelConcurrent, 'modelConcurrent', 1, MAX_RUNTIME_CONCURRENCY);
    if (p.account !== undefined) text(p.account, 'account', 120);
    if (p.memoryBytes !== undefined) integer(p.memoryBytes, 'memoryBytes', 1, limits.memoryStopBytes ?? Number.MAX_SAFE_INTEGER);
    if (p.toolProcesses !== undefined) integer(p.toolProcesses, 'toolProcesses', 1, limits.commandConcurrency);
  }
  const roles = {};
  const add = async (name, path, source) => {
    identifier(name, 'role'); const raw = (await readBounded(path, limits.roleBytes)).toString('utf8');
    // Strict JSON frontmatter avoids ambiguous YAML coercion and third-party parser startup.
    const match = /^---\r?\n([\s\S]*?)\r?\n---\r?\n([\s\S]*)$/.exec(raw);
    if (!match) throw fail(`Role ${path} needs JSON frontmatter between --- lines`);
    const meta = JSON.parse(match[1]); keys(meta, ['description', 'profile', 'tools', 'workspace', 'hidden', 'requiredTools', 'inputMode', 'requiredMode', 'maxTurns'], 'role');
    if (BENCHMARK_ROLES.has(name)) {
      if (meta.hidden !== true || (meta.requiredMode && meta.requiredMode !== 'benchmark')) throw fail('Reserved benchmark roles must remain hidden and benchmark-scoped');
      meta.requiredMode = 'benchmark';
    }
    if (meta.requiredMode !== undefined) identifier(meta.requiredMode, 'role requiredMode');
    if (meta.hidden !== undefined && typeof meta.hidden !== 'boolean') throw fail('Role hidden must be boolean');
    if (meta.inputMode !== undefined && meta.inputMode !== 'official-names') throw fail('Invalid role inputMode');
    // Role maxTurns can shorten a scheduling slice; continuation retains the task.
    if (meta.maxTurns !== undefined) integer(meta.maxTurns, 'role maxTurns', 1, limits.maxTurns);
    if (meta.requiredTools !== undefined) {
      if (!Array.isArray(meta.requiredTools) || meta.requiredTools.length > 16 || new Set(meta.requiredTools).size !== meta.requiredTools.length) throw fail('Invalid requiredTools');
      meta.requiredTools.forEach(tool => identifier(tool, 'required tool'));
    }
    text(meta.description, 'description', 256); identifier(meta.profile ?? 'main', 'profile');
    if (!profiles[meta.profile ?? 'main']) throw fail(`Unknown role profile ${meta.profile}`);
    if (!Array.isArray(meta.tools) || meta.tools.length > BUILTIN_TOOLS.length || meta.tools.some(t => !BUILTIN_TOOLS.includes(t))) throw fail('Invalid role tool allowlist');
    if (!['readonly', 'worktree', 'shared'].includes(meta.workspace)) throw fail('Role workspace must be readonly/worktree/shared');
    if (meta.workspace === 'readonly' && meta.tools.some(t => ['bash', 'edit', 'write'].includes(t))) throw fail('Readonly roles cannot have bash/edit/write');
    const prompt = text(match[2], 'role prompt', limits.roleBytes);
    roles[name] = { ...meta, name, profile: meta.profile ?? 'main', prompt, source, path: await realpath(path), hash: hash(raw) };
  };
  const builtins = join(packageRoot, 'agents');
  for (const file of (await readdir(builtins)).filter(f => f.endsWith('.md'))) await add(file.slice(0, -3), join(builtins, file), 'builtin');
  for (const [cfg, source, base] of [[global, 'user', dirname(globalPath)], [project, 'project', dirname(projectPath)]]) {
    if (cfg.roles) {
      keys(cfg.roles, Object.keys(cfg.roles), 'roles');
      if (Object.keys(cfg.roles).length > 32) throw fail('Too many roles');
      for (const [name, file] of Object.entries(cfg.roles)) await add(name, resolve(base, text(file, 'role path', 4096)), source);
    }
  }
  // User-level pins live separately so older running hosts can still refresh
  // the ordinary roles/config without encountering unknown configuration keys.
  const pinsPath = join(dirname(globalPath), 'profile-pins.json');
  const pins = await optionalJson(pinsPath);
  keys(pins, ['roles'], 'profile pins');
  if (pins.roles) {
    keys(pins.roles, Object.keys(roles), 'pinned roles');
    for (const [name, pin] of Object.entries(pins.roles)) {
      keys(pin, ['profile', 'provider', 'model', 'thinkingLevel'], 'profile pin');
      identifier(pin.profile, 'pinned profile'); text(pin.provider, 'pinned provider', 120); text(pin.model, 'pinned model', 256);
      if (!levels.includes(pin.thinkingLevel)) throw fail('Invalid pinned thinking level');
      roles[name].profileConstraint = { ...pin };
      assertProfilePin(roles[name], profiles[roles[name].profile], roles[name].profile);
    }
  }
  // Route labels are exact host-global declarations, never project or model-name heuristics.
  // The profile remains subject to the role's site pin and mode checks at admission.
  if (global.routingProfiles !== undefined) keys(global.routingProfiles, ['narrow', 'complex'], 'routing profiles');
  const routingProfiles = {};
  for (const [intent, mark] of Object.entries(global.routingProfiles ?? {})) {
    keys(mark, ['profile', 'provider', 'model', 'thinkingLevel', 'requiredMode'], 'routing profile mark');
    identifier(mark.profile, 'routing profile'); text(mark.provider, 'routing provider', 120); text(mark.model, 'routing model', 256);
    if (!levels.includes(mark.thinkingLevel)) throw fail('Invalid routing thinking level');
    if (mark.requiredMode !== undefined) identifier(mark.requiredMode, 'routing requiredMode');
    const profile = profiles[mark.profile];
    if (!profile || profile.inherit || profile.provider !== mark.provider || profile.model !== mark.model || (profile.thinkingLevel ?? 'off') !== mark.thinkingLevel || profile.requiredMode !== mark.requiredMode) throw fail('Trusted routing profile mark no longer matches the explicit model and mode authority', 'MODEL');
    if (intent === 'narrow' && (profile.maxConcurrent === undefined || profile.maxConcurrent > 2 || profile.modelConcurrent === undefined || profile.modelConcurrent > 2)) throw fail('Narrow route needs explicit provider and model concurrency <= 2');
    routingProfiles[intent] = { ...mark };
  }
  const providerModules = global.providerModules ?? [];
  if (!Array.isArray(providerModules) || providerModules.length > 8 || providerModules.some(p => typeof p !== 'string' || !isAbsolute(p))) throw fail('providerModules must be <=8 absolute trusted user module paths');
  const result = { limits, profiles, roles, routingProfiles, providerModules, agentDir, cwd, trusted, sources: [globalPath, ...(trusted ? [projectPath] : [])], modelsPath: global.modelsPath ? resolve(dirname(globalPath), text(global.modelsPath, 'modelsPath', 4096)) : join(agentDir, 'models.json'), authPath: global.authPath ? resolve(dirname(globalPath), text(global.authPath, 'authPath', 4096)) : join(agentDir, 'auth.json'), ...(global.trustedEffectGrant ? { trustedEffectGrant: fileEffectGrant(global.trustedEffectGrant) } : {}), ...(global.trustedGitSnapshotGrant === undefined ? {} : { trustedGitSnapshotGrant: gitSnapshotGrant(global.trustedGitSnapshotGrant) }), ...(global.trustedFileSnapshotGrant === undefined ? {} : { trustedFileSnapshotGrant: fileSnapshotGrant(global.trustedFileSnapshotGrant) }) };
  result.memoryPolicy = { name: 'process-first', explicitLegacyLimits: ['memorySoftBytes', 'memoryStopBytes', 'workerMaxRuns', 'workerMemoryBytes'].filter(key => Object.hasOwn(global.limits ?? {}, key)) };
  encodeJson(result, LIMITS.frameBytes);
  return result;
}
export function resolveProfile(config, roleName, override, mainModel) {
  const role = config.roles[roleName]; if (!role) throw fail(`Unknown role: ${roleName}`);
  const name = override ?? role.profile; const p = config.profiles[name]; if (!p) throw fail(`Unknown profile: ${name}`);
  const resolved = p.inherit ? { ...mainModel, thinkingLevel: p.thinkingLevel ?? mainModel?.thinkingLevel } : { ...p };
  text(resolved.provider, 'provider', 120); text(resolved.model, 'model', 256);
  if (resolved.thinkingLevel && !levels.includes(resolved.thinkingLevel)) throw fail('Invalid thinking level');
  assertProfilePin(role, resolved, name);
  return { name, provider: resolved.provider, model: resolved.model, thinkingLevel: resolved.thinkingLevel ?? 'off', account: p.account ?? resolved.account ?? resolved.provider, maxConcurrent: p.maxConcurrent ?? config.limits.concurrency, modelConcurrent: p.modelConcurrent ?? p.maxConcurrent ?? config.limits.concurrency, memoryBytes: p.memoryBytes ?? config.limits.taskMemoryBytes, toolProcesses: p.toolProcesses, ...(p.allowBackground === true ? { allowBackground: true } : {}), fileToolProtocol: assertFileToolSelection(p, role.tools), ...(p.requiredMode === undefined ? {} : { requiredMode: p.requiredMode }), ...(p.instructions === undefined ? {} : { instructions: text(p.instructions, 'profile instructions', PROFILE_INSTRUCTION_BYTES, { empty: true }) }) };
}

export function assertProfilePin(role, profile, name) {
  const pin = role?.profileConstraint;
  if (!pin) return;
  if (name !== pin.profile || profile?.inherit || ['provider', 'model', 'thinkingLevel'].some(key => profile?.[key] !== pin[key]))
    throw fail(`Role ${role.name} requires its configured ${pin.profile} profile (${pin.provider}/${pin.model}, ${pin.thinkingLevel}); no override or fallback.`, 'MODEL');
}
