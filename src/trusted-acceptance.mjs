import { join } from 'node:path';
import { readJson } from './storage.mjs';
import { REPORT_BYTES } from './responses.mjs';
import { fail, hash } from './limits.mjs';
import { boundedInputIdentity } from './universal.mjs';

// A precommitted exact-result comparison is deterministic, not a semantic verifier.
// It authorizes only inspection/analysis of a readonly result, never publishing a
// worktree, writing shared files, asserting a test passed, or calling an external API.
export function trustedAcceptance(store) {
  return {
    async check(task, report, { use, reportArtifactId }) {
      if (use !== task.workItem.intendedUse || !['inspect', 'publish'].includes(use) || (use === 'publish' && task.workItem.execution !== 'artifact-sha256')) throw fail('Only a trusted deterministic publication can be accepted for publish', 'UNSUPPORTED');
      if (task.role.workspace !== 'readonly' || task.role.tools.some(tool => tool !== 'artifact_read') || task.webUsed.search + task.webUsed.fetch !== 0 || task.webReceipts.length !== 0) throw fail('Effectful task requires an independent trusted effect checker', 'UNSUPPORTED');
      if ((task.mailbox?.length ?? 0) || (task.delivering?.length ?? 0) || (task.seen?.length ?? 0) || (task.checkpoint?.mailIds?.length ?? 0)) throw fail('Mail-tainted WorkItem cannot be trusted or accepted', 'UNSUPPORTED');
      if (task.currentReport !== reportArtifactId || report.id !== reportArtifactId || report.label !== 'agent-response-report') throw fail('Candidate report changed', 'CONFLICT');
      if (!report.complete || report.owner !== task.id || !task.workItem.acceptanceProof) throw fail('No precommitted acceptance proof', 'BLOCKED');
      for (const ref of task.workItem.inputRefs) await boundedInputIdentity(store, ref, 'Accepted input');
      const body = await readJson(join(store.root, 'artifacts', `${report.id}.bin`), REPORT_BYTES);
      if (body.agentId !== task.id || body.state !== 'idle' || typeof body.result !== 'string' || body.result !== task.result) throw fail('Report/task evidence mismatch', 'STORAGE');
      if (use === 'publish' && body.result !== task.workItem.inputRefs[0]?.sha256) throw fail('Published source digest is not the candidate', 'BLOCKED');
      return task.workItem.acceptanceProof.kind === 'exact-result-sha256' && hash(body.result) === task.workItem.acceptanceProof.sha256;
    },
  };
}
