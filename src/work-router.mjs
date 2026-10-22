import { resolveProfile } from './config.mjs';

// Pure routing: category is a caller declaration, never inferred from prose,
// profile names, model strings, usage telemetry or claimed quality. No effects.
export function routeWork(item, config, mainModel) {
  const role = config.roles[item.role];
  const blocked = (reason, code = 'ROUTE') => ({ state: 'blocked', reason, code });
  if (!role) return blocked('Unknown role', 'CAPABILITY');
  if (role.workspace !== 'readonly' || item.capabilities.some(tool => tool !== 'artifact_read' || !role.tools.includes(tool)) || (role.requiredTools ?? []).some(tool => !item.capabilities.includes(tool))) return blocked('Work role lacks requested readonly rights', 'CAPABILITY');
  const pin = role.profileConstraint;
  // A site pin is checked before considering any route mark, even when a caller
  // offers an otherwise valid different profile. Never silently fall back.
  if (pin && item.route && config.routingProfiles?.[item.route]?.profile !== pin.profile) return blocked(`Role ${role.name} requires its pinned ${pin.profile} profile`, 'MODEL');
  if (item.execution === 'artifact-sha256') {
    if (item.route) return blocked('A host-owned deterministic operation cannot request a model route');
    try { return { state: 'selected', execution: 'artifact-sha256', profile: resolveProfile(config, item.role, item.profile, mainModel) }; }
    catch (error) { return blocked(error.message, error.code ?? 'MODEL'); }
  }
  if (!item.route) return blocked('No explicit route requested; use the existing direct profile selection', 'ROUTE');
  const mark = config.routingProfiles?.[item.route];
  if (!mark) {
    const candidates = Object.keys(config.profiles).filter(name => {
      const profile = config.profiles[name];
      return !profile.inherit && (!pin || name === pin.profile) && (item.profile === undefined || item.profile === name);
    }).sort();
    return { state: 'choices', reason: `No trusted ${item.route} profile mark; supply an exact host-global mark or use a direct profile`, eligibleProfiles: candidates };
  }
  if (item.profile !== undefined && item.profile !== mark.profile) return blocked('Explicit profile conflicts with the trusted route mark', 'MODEL');
  const configured = config.profiles[mark.profile];
  if (!configured || configured.inherit || ['provider', 'model', 'requiredMode'].some(field => configured[field] !== mark[field]) || (configured.thinkingLevel ?? 'off') !== mark.thinkingLevel) return blocked('Trusted route mark no longer matches its explicit profile and mode authority', 'MODEL');
  if (item.route === 'narrow' && (configured.maxConcurrent === undefined || configured.maxConcurrent > 2 || configured.modelConcurrent === undefined || configured.modelConcurrent > 2)) return blocked('Narrow profile requires explicit technical provider and model concurrency <= 2');
  try { return { state: 'selected', execution: 'model', route: item.route, profile: resolveProfile(config, item.role, mark.profile, undefined) }; }
  catch (error) { return blocked(error.message, error.code ?? 'MODEL'); }
}
