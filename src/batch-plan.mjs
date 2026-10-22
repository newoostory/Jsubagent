import { LIMITS, encodeJson, fail, identifier, text } from './limits.mjs';
import { normalizeDependencies } from './dependencies.mjs';
import { validateTaskName } from './task-name.mjs';
import { normalizeResultInputs } from './result-inputs.mjs';
import { normalizePublishTo } from './result-notifications.mjs';
import { normalizeVerificationChecks } from './result-checks.mjs';
import { normalizeQualityContract } from './quality-contract.mjs';

const DEFAULT_FIELDS = Object.freeze([
  'role', 'profile', 'directory', 'dependencyHandoff',
]);

function plainObject(value, name) {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw fail(`${name}: expected object`);
  return value;
}

function validateNormalizedTask(task, index, batchSize) {
  const label = `tasks[${index}]`;
  for (const field of Object.keys(task)) if (![...DEFAULT_FIELDS, 'name', 'task', 'dependsOn', 'resultContract', 'resultInputs', 'publishTo', 'verificationChecks', 'qualityContract'].includes(field)) throw fail(`${label}.${field} is not supported`);
  if (task.qualityContract !== undefined) task.qualityContract = normalizeQualityContract(task.qualityContract);
  if (task.resultContract !== undefined && !['primary', 'code-change'].includes(task.resultContract)) throw fail('Unsupported result contract', 'UNSUPPORTED');
  if (task.resultInputs !== undefined) task.resultInputs = normalizeResultInputs(task.resultInputs, { index, batchSize });
  if (task.publishTo !== undefined) task.publishTo = normalizePublishTo(task.publishTo, { index, batchSize });
  if (task.verificationChecks !== undefined) task.verificationChecks = normalizeVerificationChecks(task.verificationChecks);
  identifier(task.role, `${label}.role`);
  text(task.task, `${label}.task`, 24 * 1024);
  if (task.profile !== undefined) identifier(task.profile, `${label}.profile`);
  if (task.name !== undefined) validateTaskName(task.name);
  if (task.directory !== undefined) text(task.directory, `${label}.directory`, 4096);
  if (task.dependencyHandoff !== undefined && !['summary', 'none'].includes(task.dependencyHandoff)) throw fail(`${label}.dependencyHandoff: invalid value`);
  const dependencies = task.dependsOn ?? [];
  if (!Array.isArray(dependencies) || dependencies.length > 8) throw fail(`${label}.dependsOn: expected at most 8 dependencies`);
  if (new Set(dependencies).size !== dependencies.length) throw fail(`${label}.dependsOn contains duplicate dependencies`, 'CONFLICT');
  for (const dependency of dependencies) {
    if (typeof dependency === 'string') identifier(dependency, `${label}.dependsOn agentId`);
    else if (!Number.isSafeInteger(dependency) || dependency < 0 || dependency >= index)
      throw fail(`${label}.dependsOn batch index must be an integer referencing an earlier task`);
  }
}

function validateDefaults(defaults) {
  if (defaults.role !== undefined) identifier(defaults.role, 'defaults.role');
  if (defaults.profile !== undefined) identifier(defaults.profile, 'defaults.profile');
  if (defaults.directory !== undefined) text(defaults.directory, 'defaults.directory', 4096);
  if (defaults.dependencyHandoff !== undefined && !['summary', 'none'].includes(defaults.dependencyHandoff)) throw fail('defaults.dependencyHandoff: invalid value');
}

/**
 * Validate and expand a public batch plan without assigning operation IDs.
 * The returned array is detached from the caller. Numeric dependencies remain
 * indexes until all operation IDs have been allocated.
 */
export function planSpawnBatch(params) {
  plainObject(params, 'spawn batch');
  encodeJson(params, LIMITS.inputBytes);
  const defaults = params.defaults === undefined ? {} : plainObject(params.defaults, 'defaults');
  for (const field of Object.keys(defaults)) if (!DEFAULT_FIELDS.includes(field)) throw fail(`defaults.${field} is not supported`);
  validateDefaults(defaults);
  if (!Array.isArray(params.tasks) || params.tasks.length < 1 || params.tasks.length > 8) throw fail('Batch tasks: expected 1..8 items');

  const normalized = [];
  for (let index = 0; index < params.tasks.length; index++) {
    const input = plainObject(params.tasks[index], `tasks[${index}]`);
    const task = { ...input };
    for (const field of DEFAULT_FIELDS) if (task[field] === undefined && defaults[field] !== undefined) task[field] = defaults[field];
    if (Array.isArray(task.dependsOn)) task.dependsOn = [...task.dependsOn];
    validateNormalizedTask(task, index, params.tasks.length);
    normalized.push(task);
  }
  // Defaults can multiply large strings across eight tasks. Bound that
  // expanded representation before IDs or host state are allocated.
  encodeJson({ action: 'spawn', tasks: normalized }, LIMITS.inputBytes);
  return normalized;
}

export function allocateSpawnBatch(params, idFactory) {
  if (typeof idFactory !== 'function') throw fail('idFactory: expected function');
  const planned = planSpawnBatch(params);
  // Allocate every operation ID before resolving a single local dependency.
  const ids = planned.map((_, index) => identifier(idFactory(index), 'task id'));
  if (new Set(ids).size !== ids.length) throw fail('Duplicate batch task id', 'CONFLICT');
  const tasks = planned.map((task, index) => {
    const dependsOn = (task.dependsOn ?? []).map(dependency => typeof dependency === 'number' ? ids[dependency] : dependency);
    const canonical = { id: ids[index], ...task };
    if (task.resultInputs) canonical.resultInputs = task.resultInputs.map(input => ({ ...input, from: { agentId: input.from.agentId ?? ids[input.from.task] } }));
    if (task.publishTo) canonical.publishTo = task.publishTo.map(route => ({ ...route, to: { agentId: route.to.agentId ?? ids[route.to.task] } }));
    if (dependsOn.length) canonical.dependsOn = normalizeDependencies(dependsOn, canonical.id);
    else delete canonical.dependsOn;
    return canonical;
  });
  encodeJson({ tasks }, LIMITS.inputBytes);
  return { tasks };
}
