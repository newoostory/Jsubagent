import assert from 'node:assert/strict';
import test from 'node:test';
import { catalogFromConfiguration, coordinatorPrompt, COORDINATOR_MARKER } from '../src/coordinator.mjs';
import { DEFAULT_MAIN_COORDINATOR_ONLY } from '../src/main-coordinator-guard.mjs';
import { PublicAgentParameters, validatePublicAgentParameters } from '../src/public-api.mjs';

const SUBAGENT_ACTIONS = [
  'catalog', 'spawn', 'status', 'wait', 'send', 'cancel', 'resume', 'archive', 'read',
];

test('the public model schema exposes only subagent lifecycle actions', () => {
  assert.deepEqual(PublicAgentParameters.properties.action.enum, SUBAGENT_ACTIONS);
  for (const removed of [
    'team_start', 'team_status', 'team_advance', 'team_cancel',
    'mission_create', 'git_snapshot_capture', 'file_snapshot_capture',
    'work_submit', 'work_accept', 'work_apply', 'evidence_record',
    'quality_check', 'quality_supersede',
  ]) {
    assert.throws(() => validatePublicAgentParameters({ action: removed }), /Invalid agent action\/fields/);
  }
  for (const removedField of ['team', 'teamId', 'mission', 'missionId', 'workItem', 'workItemId', 'evidence', 'reviewerId', 'replacementId']) {
    assert.equal(PublicAgentParameters.properties[removedField], undefined, `${removedField} leaked into the public schema`);
  }
});

test('canonical subagent lifecycle inputs remain valid', () => {
  const cases = [
    { action: 'catalog' },
    { action: 'spawn', tasks: [{ role: 'scout', task: 'Inspect the bounded issue.' }] },
    { action: 'status' },
    { action: 'wait', agentIds: ['child-1'] },
    { action: 'send', agentIds: ['child-1'], body: 'Report the exact evidence.' },
    { action: 'cancel', agentIds: ['child-1'] },
    { action: 'resume', agentId: 'child-1', instruction: 'Continue with the corrected input.' },
    { action: 'archive', agentIds: ['child-1'] },
    { action: 'read', requests: [{ agentId: 'child-1' }] },
  ];
  for (const input of cases) assert.equal(validatePublicAgentParameters(input), input);
});

test('the stable injected prompt keeps direct work as the default and omits catalogs and swarm choreography', () => {
  assert.equal(DEFAULT_MAIN_COORDINATOR_ONLY, false);
  const catalogA = { concurrency: 2, roles: [{ name: 'SECRET_ROLE_A' }], profiles: [{ name: 'SECRET_PROFILE_A' }] };
  const catalogB = { concurrency: 2, roles: [{ name: 'SECRET_ROLE_B' }], profiles: [{ name: 'SECRET_PROFILE_B' }] };
  const direct = coordinatorPrompt(catalogA, ['agent', 'read'], { coordinatorOnly: false });
  assert.equal(direct, coordinatorPrompt(catalogB, ['agent', 'read'], { coordinatorOnly: false }));
  assert.match(direct, new RegExp(COORDINATOR_MARKER.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
  assert.match(direct, /Main may work directly/);
  assert.match(direct, /agent catalog/);
  assert.doesNotMatch(direct, /SECRET_ROLE|SECRET_PROFILE|team_start|team_advance|worker.*reviewer.*integrator.*verifier|Default workflow preflight/);
  assert.ok(Buffer.byteLength(direct) < 1800, `coordinator prompt grew to ${Buffer.byteLength(direct)} bytes`);
  assert.equal(coordinatorPrompt(catalogA, ['read'], { coordinatorOnly: false }), '');
});

test('the on-demand catalog does not prescribe a multi-stage workflow', () => {
  const catalog = catalogFromConfiguration({
    sources: [], limits: { concurrency: 4, maxTurns: 8 },
    roles: { scout: { name: 'scout', description: 'Inspect', workspace: 'readonly', tools: ['read'], profile: 'default', source: 'fixture', hash: 'fixture' } },
    profiles: { default: { inherit: true } },
  });
  assert.equal(catalog.defaultWorkflow, undefined);
  assert.deepEqual(catalog.roles.map(role => role.name), ['scout']);
});
