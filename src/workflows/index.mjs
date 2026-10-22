import { genericWorkflowAdapter } from './generic.mjs';
import { isBenchmarkWorkflow, benchmarkWorkflowAdapter } from './benchmark.mjs';
import { isOfficialResearchWorkflow, officialResearchWorkflowAdapter } from './official-research.mjs';
import { isSourceResearchWorkflow, sourceResearchWorkflowAdapter } from './research.mjs';
import { isTranslationWorkflow, translationWorkflowAdapter } from './translator.mjs';

export function workflowAdapter(task) {
  if (isOfficialResearchWorkflow(task)) return officialResearchWorkflowAdapter(task);
  if (isBenchmarkWorkflow(task)) return benchmarkWorkflowAdapter(task);
  if (isSourceResearchWorkflow(task)) return sourceResearchWorkflowAdapter(task);
  if (isTranslationWorkflow(task)) return translationWorkflowAdapter(task);
  return genericWorkflowAdapter(task);
}

export { researchResumePrompt } from './research.mjs';
export { refreshWorkflowGrants, assertMutableWorkflowMode, createWorkflowExpiryTimer } from './benchmark.mjs';
