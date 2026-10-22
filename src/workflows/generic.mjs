import { fail } from '../limits.mjs';
import { qualityPrompt, inspectQualityResult } from '../quality-contract.mjs';

export function genericWorkflowAdapter(task) {
  return {
    id: 'generic',
    allowWeb: true,
    allowSteering: true,
    allowEffectRecovery: true,
    allowSectionContinuation: true,
    allowMail: true,
    canResume: true,
    allowContinuation: true,
    assertMail() {},
    assertResume() {},
    assertFreshResult() {},
    authorityPatch() { return {}; },
    authorityFailure() { return undefined; },
    assertTask() {},
    admit() { return {}; },
    dependenciesFresh() { return true; },
    resumePatch() { return {}; },
    acceptResult(raw) {
      if (typeof raw !== 'string') throw fail('Task result must be text', 'LIMIT');
      return raw;
    },
    extensions() { return []; },
    initialPrompt(defaultPrompt) { return defaultPrompt + qualityPrompt(task?.qualityContract); },
    resultStep({ raw }) {
      if (typeof raw !== 'string') throw fail('Task result must be text', 'LIMIT');
      if (task?.qualityContract) inspectQualityResult(raw, task.qualityContract);
      return { complete: true, result: raw };
    },
    lengthTerminated() {
      throw fail('Provider response reached its output length limit before a complete result commit; partial output was retained as evidence but not published', 'LIMIT');
    },
  };
}
