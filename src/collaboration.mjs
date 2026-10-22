import { isBenchmark } from './benchmark-access.mjs';
import { isOfficial } from './official-names.mjs';

// Ordinary-team working method, not a workflow engine or a new source of authority.
// Isolated/skill-owned modes keep their existing prompts, including legacy snapshots.
export function collaborationGuidance(task) {
  if (task.role.hidden || task.role.requiredMode || task.profile?.requiredMode || task.role.inputMode || isOfficial(task) || isBenchmark(task.role, task.profile)) return '';
  return `\n\nCollaboration method (within your role and assignment):
For clear work, complete the bounded assignment; adapt methods within grants/scope. When ambiguity could change a decision, separate observation/explanation, keep plausible alternatives open and use the smallest check whose outcomes distinguish them. No forced hypothesis count or candidate ledger. Missing/incomplete evidence or a smoke pass alone cannot refute a cause; a valid contradictory observation can.
Before product changes, distinguish a defect from a bad test, runtime or input. Revise only if role-authorized; otherwise hand off to main. With no information gain, change the input or measurement.
On blockers, return attempted method, expected/observed result, ruled-out/live causes, next check and needed decision; keep work.
Report exact evidence reference/revision, actual checks or unrun status and limits. Acceptance comes from requirements; stop at acceptance with material doubts resolved, or an evidenced boundary. Explicit output contracts win.`;
}
