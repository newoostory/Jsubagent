export const MAIN_MODEL_TOOLS = Object.freeze(['agent', 'read', 'grep', 'lifecycle', 'playbook']);
export const DEFAULT_MAIN_COORDINATOR_ONLY = false;

const MAIN_MODEL_TOOL_SET = new Set(MAIN_MODEL_TOOLS);
const MAIN_LIFECYCLE_ACTIONS = new Set([
  'plan_status', 'plan_start', 'plan_update', 'plan_repair',
  'knowledge_list', 'knowledge_promote', 'knowledge_retire',
  'note_list', 'note_set', 'note_remove',
]);
const NONEXECUTING_CLOSE_OUTCOMES = new Set(['cancelled', 'superseded']);

function blocked(reason = 'Main is coordinator-only; this model tool call is not permitted.') {
  return { block: true, reason };
}

export function mainCoordinatorToolGate(event, enabled = true, activeTools = MAIN_MODEL_TOOLS) {
  if (!enabled) return undefined;
  const toolName = event?.toolName;
  if (typeof toolName !== 'string' || !MAIN_MODEL_TOOL_SET.has(toolName)) return blocked();
  if (!Array.isArray(activeTools) || !activeTools.includes(toolName))
    return blocked(`Main tool ${toolName} is disabled by the current tool selection.`);
  if (toolName === 'playbook') {
    if (event.input?.action === 'catalog_query' || event.input?.action === 'load' && event.input?.data?.readOnly === true) return undefined;
    return blocked('Coordinator playbook loads require readOnly:true; guidance must not write artifacts or execute work.');
  }
  if (toolName !== 'lifecycle') return undefined;

  const action = event?.input?.action;
  if (typeof action === 'string' && MAIN_LIFECYCLE_ACTIONS.has(action)) return undefined;
  if (action === 'plan_close' && event.input?.data && typeof event.input.data === 'object' && !Array.isArray(event.input.data)
    && NONEXECUTING_CLOSE_OUTCOMES.has(event.input.data.outcome)) return undefined;
  return blocked('Main may manage plans, but plan verification and completed closure must be delegated to the configured verifier route.');
}

export function intersectMainModelTools(activeTools) {
  if (!Array.isArray(activeTools)) throw new TypeError('Active tool catalog must be an array');
  return [...new Set(activeTools.filter(name => typeof name === 'string' && MAIN_MODEL_TOOL_SET.has(name)))];
}

export function narrowMainModelTools(pi, selectedTools) {
  let readError;
  let activeTools;
  try {
    activeTools = pi.getActiveTools();
  } catch (error) {
    readError = error;
    activeTools = Array.isArray(selectedTools) ? selectedTools : [];
  }

  const narrowed = intersectMainModelTools(activeTools);
  if (Array.isArray(selectedTools)) selectedTools.splice(0, selectedTools.length, ...narrowed);

  let updateError;
  try {
    pi.setActiveTools(narrowed);
  } catch (error) {
    updateError = error;
  }

  if (readError || updateError) {
    const error = new Error('Unable to enforce the main coordinator tool allowlist');
    error.cause = readError ?? updateError;
    throw error;
  }
  return narrowed;
}
