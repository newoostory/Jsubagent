import { genericWorkflowAdapter } from './generic.mjs';

const TRANSLATION_ROLES = new Set(['translator', 'translation-reviewer']);

export function isTranslationWorkflow(task) {
  return TRANSLATION_ROLES.has(task?.role?.name);
}

export function translationWorkflowAdapter() {
  return { ...genericWorkflowAdapter(), id: 'translation' };
}
