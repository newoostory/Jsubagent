import { createRequire } from 'node:module';
import { dirname, join } from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { resolve } from 'node:path';
import { openTeamEvidence, loadTeam, verifyTeamPlanBinding } from './team-evidence.mjs';
import { fail, safeError } from './limits.mjs';

export async function verifyTeamDestination({ runRoot, teamId }) {
  const opened = await openTeamEvidence(runRoot);
  let team;
  try { team = await loadTeam(opened.store, teamId); } finally { opened.close(); }
  if (!team.plan) throw fail('No native PLAN is bound to this team', 'QUALITY');
  const cwd = team.spec.directory;
  await verifyTeamPlanBinding(team.plan.binding, { workspace: cwd, phase: 'before_checks', declaration: team.spec.plan.declaration });
  const require = createRequire(import.meta.url);
  const { executePlanAction } = await import(pathToFileURL(join(dirname(require.resolve('pi-super/package.json')), 'src/plan/index.js')).href);
  let state = await executePlanAction('plan_status', {}, { cwd });
  if (state.state !== 'active' || state.metadata?.planId !== team.plan.binding.planId) throw fail('Bound PLAN is not active', 'QUALITY');
  state = await executePlanAction('plan_update', { expectedRevision: state.revision, userRequestAppend: null, sections: team.spec.plan.completionSections }, { cwd });
  const result = await executePlanAction('plan_verify', { expectedRevision: state.revision, declaration: team.spec.plan.declaration, close: true }, { cwd });
  if (!result.passed || result.closeResult?.outcome !== 'completed') throw fail('Actual destination verification did not complete', 'QUALITY');
  return { passed: true, planId: team.plan.binding.planId, revision: result.revision, evidence: result.evidence, archive: result.closeResult.archivePath };
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    if (!process.argv[2] || process.argv[2].length > 16 * 1024 || process.argv.length !== 3) throw fail('Expected one bounded team identity');
    const payload = JSON.parse(Buffer.from(process.argv[2], 'base64').toString('utf8'));
    if (Object.keys(payload).sort().join(',') !== 'runRoot,teamId') throw fail('Invalid team identity');
    console.log(JSON.stringify(await verifyTeamDestination(payload)));
  } catch (error) { console.error(safeError(error)); process.exitCode = 1; }
}
