import { fail, identifier } from './limits.mjs';
import { assertBenchmarkContract, isBenchmark } from './benchmark-access.mjs';

// Mode state is trusted parent-runtime control, never a public agent tool argument.
export function requiredModes(role, profile) {
  return [...new Set([role?.requiredMode, profile?.requiredMode, ...(isBenchmark(role, profile) ? ['benchmark'] : [])].filter(value => value !== undefined))].map(value => identifier(value, 'requiredMode'));
}
export function authorizeModes(role, profile, modes, bindings) {
  const required = requiredModes(role, profile);
  const result = required.map(name => {
    const active = modes.get(name);
    if (!active?.root) throw fail(`Agent requires active ${name} mode with an explicitly selected workspace.`, 'MODE');
    if (name === 'benchmark') assertBenchmarkContract(role, profile, active);
    if (bindings && !bindings.some(binding => binding.name === name && binding.root === active.root)) throw fail('Mode workspace changed; this agent belongs to its original workspace. Spawn a new agent or select the original workspace before explicit resume.', 'MODE');
    return { name, root: active.root };
  });
  if (new Set(result.map(binding => binding.root)).size > 1) throw fail('Required modes have different workspaces', 'MODE');
  return result;
}
export function authorizeMainMutation(role, profile, modes, bindings) {
  return authorizeModes(role, profile, modes, bindings);
}
