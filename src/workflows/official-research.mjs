import { fail } from '../limits.mjs';
import {
  assertFreshOfficial,
  isOfficial,
  officialPromptExtension,
  officialDependenciesFresh,
  parseOfficialReport,
  validateOfficialResult,
  validateOfficialAdmission,
} from '../official-names.mjs';
import { genericWorkflowAdapter } from './generic.mjs';

const REPAIR_PROMPT = 'Repair the previous official-name response in this same assignment. Validation: {issue}\nPreserve supported findings and reuse completed search/fetch evidence already in this session. Correct only the stated format, ordering, coverage or evidence defect; research only candidates still lacking required evidence. Return the complete strict findings JSON, one finding per original capsule ID in order. Never invent evidence, drop candidates or mark skipped searches unresolved. If a substantive blocker remains, report it honestly. Do not request source context or another task.';

export function officialResearchWorkflowAdapter(task) {
  const repairIssues = new Set();
  let repairing = false;
  return {
    ...genericWorkflowAdapter(),
    id: 'official-name-research',
    allowSteering: false,
    allowEffectRecovery: false,
    allowMail: false,
    canResume: false,
    assertMail() { throw fail('Official-name tasks cannot send/receive mail or steering; use a fresh authorized capsule', 'PRIVACY'); },
    assertResume() { throw fail('Official-name tasks cannot resume; inspect receipts then spawn a fresh authorized capsule', 'PRIVACY'); },
    assertFreshResult(subject, tasks, running) {
      if (!officialDependenciesFresh(subject, tasks, running)) throw fail('Names dependency report changed during official research; result not accepted. Inspect receipts before a fresh authorized projection.', 'STALE');
    },
    admit(input, tasks, running) {
      const capsule = validateOfficialAdmission(input, tasks, running);
      return capsule ? { task: JSON.stringify(capsule), officialDependencyReports: input.dependsOn.map(agentId => ({ agentId, reportArtifactId: tasks.get(agentId).latestReport })) } : {};
    },
    dependenciesFresh(subject, tasks, running) { return officialDependenciesFresh(subject, tasks, running); },
    assertTask() { assertFreshOfficial(task); },
    extensions() { return [officialPromptExtension(task)]; },
    initialPrompt() { return task.task; },
    isRepairing() { return repairing; },
    repairToolFailure() {
      return fail('Official-name repair blocked by a failed tool; inspect retained receipts before any further action', 'EVIDENCE');
    },
    assembleSections(contents) {
      return JSON.stringify({ findings: contents.flatMap(content => parseOfficialReport(content).findings) });
    },
    resultStep({ raw, evidence, messages, boundary }) {
      try {
        return { complete: true, result: JSON.stringify(validateOfficialResult(task, raw, evidence)) };
      } catch (error) {
        if (error.code !== 'EVIDENCE') throw error;
        if (messages.some(message => message.role === 'toolResult' && message.isError)) throw this.repairToolFailure();
        if (repairIssues.has(error.message)) throw fail(`Official-name repair made no progress: ${error.message}`, 'EVIDENCE');
        if (boundary?.pause) throw fail(`Official-name repair cannot fit the remaining ${boundary.boundary} budget; inspect the retained incomplete attempt`, 'LIMIT');
        repairIssues.add(error.message);
        repairing = true;
        return { complete: false, prompt: REPAIR_PROMPT.replace('{issue}', error.message) };
      }
    },
    lengthTerminated() {
      throw fail('Official-name provider did not complete a full findings commit; partial output was retained but cannot satisfy candidate coverage', 'LIMIT');
    },
  };
}

export function isOfficialResearchWorkflow(task) {
  return isOfficial(task);
}
