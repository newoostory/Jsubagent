const values = (...items) => Object.freeze(items);
const record = value => Object.freeze(value);

const RESULT_CONTRACT_VALUES = values('primary', 'code-change');
const MILESTONE_VALUES = values('published', 'candidate', 'final');
const RESULT_VIEWS = values('main', 'reviewer', 'verifier', 'artifacts');

export const PUBLIC_RESULT_FEATURES = record({
  resultContract: record({
    values: RESULT_CONTRACT_VALUES,
    defaultValue: RESULT_CONTRACT_VALUES[0],
    codeChangeValue: RESULT_CONTRACT_VALUES[1],
  }),
  milestone: record({
    values: MILESTONE_VALUES,
    defaultValue: MILESTONE_VALUES[2],
    candidateValue: MILESTONE_VALUES[1],
    immutableProductValues: values(MILESTONE_VALUES[1], MILESTONE_VALUES[2]),
  }),
  selector: record({
    views: RESULT_VIEWS,
    verificationViews: values(RESULT_VIEWS[1], RESULT_VIEWS[2]),
    fileMaxItems: 64,
    fileMaxBytes: 512,
    kindMaxItems: 16,
    kindMaxBytes: 80,
  }),
  resultInputs: record({ maxItems: 8 }),
  publishTo: record({ maxItems: 8, followValues: values('once', 'updates') }),
  verificationChecks: record({
    maxItems: 16,
    commandMaxBytes: 8192,
    timeoutMinMs: 1,
    timeoutMaxMs: 86_400_000,
    requiredCapability: 'bash',
    requiredWorkflow: 'generic',
  }),
});

const listed = values_ => values_.join('|');
const resultContracts = listed(PUBLIC_RESULT_FEATURES.resultContract.values);
const milestones = listed(PUBLIC_RESULT_FEATURES.milestone.values);
const verificationMilestones = listed(PUBLIC_RESULT_FEATURES.milestone.immutableProductValues);
const verificationViews = listed(PUBLIC_RESULT_FEATURES.selector.verificationViews);
const timeoutRange = `${PUBLIC_RESULT_FEATURES.verificationChecks.timeoutMinMs}..${PUBLIC_RESULT_FEATURES.verificationChecks.timeoutMaxMs}`;

export const PUBLIC_RESULT_FIELD_DESCRIPTIONS = record({
  resultContract: `Select ${resultContracts} (default ${PUBLIC_RESULT_FEATURES.resultContract.defaultValue}). ${PUBLIC_RESULT_FEATURES.resultContract.codeChangeValue} requires an isolated owned worktree and runtime-captured immutable product snapshots; only this contract supports the ${PUBLIC_RESULT_FEATURES.milestone.candidateValue} milestone.`,
  inputMilestone: `Pin a ${milestones} result milestone (default ${PUBLIC_RESULT_FEATURES.milestone.defaultValue}). ${PUBLIC_RESULT_FEATURES.milestone.candidateValue} is valid only for a ${PUBLIC_RESULT_FEATURES.resultContract.codeChangeValue} producer's immutable product snapshot.`,
  routeMilestone: `Deliver a ${milestones} result milestone (default ${PUBLIC_RESULT_FEATURES.milestone.defaultValue}). ${PUBLIC_RESULT_FEATURES.milestone.candidateValue} is valid only when this task uses ${PUBLIC_RESULT_FEATURES.resultContract.codeChangeValue} and publishes an immutable product snapshot.`,
  resultInputs: 'Immutable result references to pin for this task. The consumer needs configured agent_read capability; required inputs wait without starting a worker.',
  publishTo: 'Typed immutable result references to deliver. The recipient needs configured agent_read capability; delivery does not automatically wake it or apply changes.',
  verificationChecks: `At most ${PUBLIC_RESULT_FEATURES.verificationChecks.maxItems} unique caller-approved exact bash checks. Allowed only for an ordinary ${PUBLIC_RESULT_FEATURES.verificationChecks.requiredWorkflow} task with ${PUBLIC_RESULT_FEATURES.verificationChecks.requiredCapability} capability and a required ${verificationMilestones} ${verificationViews} input from a ${PUBLIC_RESULT_FEATURES.resultContract.codeChangeValue} producer. Checks run against the pinned product snapshot and grant no additional authority.`,
  verificationCommand: `Exact approved bash command, at most ${PUBLIC_RESULT_FEATURES.verificationChecks.commandMaxBytes} UTF-8 bytes.`,
  verificationTimeout: `Optional exact bash timeout in milliseconds (${timeoutRange}).`,
});

export function describePublicResultFeatures() {
  const features = PUBLIC_RESULT_FEATURES;
  return `Result features: resultContract accepts ${resultContracts} (default ${features.resultContract.defaultValue}). ${features.resultContract.codeChangeValue} requires an isolated owned worktree and runtime-captured immutable product snapshots; the ${features.milestone.candidateValue} milestone is supported only for ${features.resultContract.codeChangeValue} snapshots. resultInputs and publishTo accept exactly one view/files/kinds selector and ${milestones} (default ${features.milestone.defaultValue}); consumers/recipients require configured agent_read capability. Required resultInputs pin an exact eligible immutable revision and wait without a worker; publishTo sends typed references without automatically waking recipients. verificationChecks accepts at most ${features.verificationChecks.maxItems} unique caller-approved exact bash argument sets (id, command, optional timeoutMs ${timeoutRange}) only on an ordinary ${features.verificationChecks.requiredWorkflow} task with ${features.verificationChecks.requiredCapability} capability and a required ${verificationMilestones} ${verificationViews} input from a ${features.resultContract.codeChangeValue} producer. Checks run against the runtime-materialized pinned product snapshot and grant no additional tool authority. Candidate publication, routing and check receipts never merge, apply or deploy changes; integration remains explicit.`;
}
