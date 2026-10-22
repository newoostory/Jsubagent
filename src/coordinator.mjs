import { loadConfiguration } from './config.mjs';
import { WEB_TOOLS } from './web-policy.mjs';
import { hasWebRuntime } from './web-tools.mjs';
import { FILE_TOOL_PROTOCOL } from './vendor/pi-tools/schemas.mjs';
import { fileToolProtocol } from './file-tool-protocol.mjs';

export const COORDINATOR_MARKER = '## pi-agent coordinator (main only)';
const WORKFLOW_STAGE_REQUIREMENTS = Object.freeze({
  implement: { role: 'worker', workspace: 'worktree', tools: ['read', 'bash', 'agent_result'], anyTools: ['edit', 'write'] },
  reviewPinned: { role: 'reviewer', workspace: 'readonly', tools: ['read', 'agent_read'] },
  integrate: { role: 'integrator', workspace: 'shared', tools: ['read', 'bash', 'agent_read'], anyTools: ['edit', 'write'] },
  verifyPinned: { role: 'verifier', workspace: 'shared', tools: ['read', 'bash', 'agent_read'] },
  verifyDestination: { role: 'verifier', workspace: 'shared', tools: ['read', 'bash'] },
});

function roleCapabilities(role) {
  const tools = new Set(role.tools ?? []);
  return {
    filesystemRead: tools.has('read'), filesystemMutation: tools.has('edit') || tools.has('write'), shell: tools.has('bash'),
    resultRead: tools.has('agent_read'), resultPublish: tools.has('agent_result'),
  };
}

export function defaultWorkflowFromRoles(roles = []) {
  const visible = roles.filter(role => role.hidden !== true);
  const stages = Object.fromEntries(Object.entries(WORKFLOW_STAGE_REQUIREMENTS).map(([name, requirement]) => {
    const role = visible.find(candidate => candidate.name === requirement.role);
    const issues = [];
    if (!role) issues.push(`${requirement.role} is not visible/configured`);
    else {
      if (role.workspace !== requirement.workspace) issues.push(`${requirement.role} needs workspace=${requirement.workspace} (configured ${role.workspace})`);
      const missing = requirement.tools.filter(tool => !(role.tools ?? []).includes(tool));
      if (missing.length) issues.push(`${requirement.role} missing tools ${missing.join(',')}`);
      if (requirement.anyTools && !requirement.anyTools.some(tool => (role.tools ?? []).includes(tool))) issues.push(`${requirement.role} needs one of ${requirement.anyTools.join(',')}`);
    }
    return [name, { role: requirement.role, available: issues.length === 0, requires: { workspace: requirement.workspace, tools: [...requirement.tools], ...(requirement.anyTools ? { anyTools: [...requirement.anyTools] } : {}) }, ...(issues.length ? { issues } : {}) }];
  }));
  const blockers = Object.entries(stages).filter(([, stage]) => !stage.available).map(([name, stage]) => `${name}: ${stage.issues.join('; ')}`);
  return { name: 'implement-review-integrate-verify', available: blockers.length === 0, stages, blockers };
}

export async function loadCatalog(options) {
  // Configuration data only: never load provider modules, credentials or SDK sessions.
  return catalogFromConfiguration(await loadConfiguration(options));
}
export function catalogFromConfiguration(config) {
  const roles = Object.values(config.roles).filter(role => role.hidden !== true).map(({ name, description, workspace, tools, profile, profileConstraint, source, hash, requiredTools, maxTurns }) => ({
    name, description, workspace, tools: [...tools], capabilities: roleCapabilities({ tools }),
    ...(config.profiles[profile]?.hidden === true ? { profileHidden: true } : { profile, ...(profileConstraint ? { profilePinned: true } : {}) }),
    source, hash, ...(requiredTools ? { requiredTools: [...requiredTools] } : {}), ...(maxTurns === undefined ? {} : { maxTurns }),
  }));
  return {
    source: 'pi-agent configuration', configPaths: config.sources ?? [],
    concurrency: config.limits.concurrency, maxTurns: config.limits.maxTurns,
    universalWebTools: hasWebRuntime() ? WEB_TOOLS : [], webPolicyRequired: false, fileToolProtocol: FILE_TOOL_PROTOCOL,
    roles,
    profiles: Object.entries(config.profiles).filter(([, p]) => p.hidden !== true).map(([name, p]) => ({ name, ...Object.fromEntries(['inherit', 'provider', 'model', 'thinkingLevel', 'maxConcurrent', 'hidden', 'requiredMode'].filter(key => p[key] !== undefined).map(key => [key, p[key]])), fileToolProtocol: fileToolProtocol(p), ...(p.instructions ? { hasInstructions: true } : {}) })),
  };
}
export function coordinatorPrompt(catalog, activeTools, { coordinatorOnly = true } = {}) {
  const active = new Set(activeTools);
  if (!active.has('agent')) return '';
  const mode = coordinatorOnly
    ? 'Main is in the user-selected coordinator-only mode. Use the active inspection and lifecycle tools, and delegate execution.'
    : 'Main may work directly. Delegate only when parallelism, isolation, or a configured specialty is useful.';
  return `${COORDINATOR_MARKER}
${mode}

Use agent catalog when role or profile details are needed; catalogs are not preloaded into this prompt. Preserve configured role/profile pins, workspace limits, disabled tools, privacy, network, and approval restrictions. Child agents do not inherit hidden context or main-session approvals.

Give each spawned child a bounded task with goal, prohibited actions, owned paths, relevant input revision, acceptance checks, and stop condition. Keep tightly coupled work together. Spawn at most 8 independent ready tasks per call; the runtime enforces configured concurrency. Do not create a fixed role chain or automatic review/integration swarm.

Keep every agentId. Use status or wait to observe work, send for follow-up or turn-boundary steer, and read for bounded durable output. Aborting wait leaves children running. After cancel, confirm settlement. accepted is not completed; settled is not success. Inspect unknown outcomes before retrying. Before final, account for every child with a reviewed result or observed cancellation.`;
}
