import assert from 'node:assert/strict';
import test from 'node:test';
import { AgentSession } from '@earendil-works/pi-coding-agent';

const notice = { customType: 'next-turn-contract-test', content: 'IMPORTANT_REFERENCE_ONLY', display: false, details: { id: 'notice-1' } };

function sessionDouble(isStreaming) {
  return {
    isStreaming,
    _pendingNextTurnMessages: [],
    _pendingCustomMessages: [],
    agent: {
      steer() { throw new Error('nextTurn must not steer the current response'); },
      followUp() { throw new Error('nextTurn must not enqueue follow-up work'); },
    },
    async _startRun() { throw new Error('nextTurn with triggerTurn:false must not start a run'); },
  };
}

test('Pi 1.3 queues nextTurn custom mail without steering, following up, or starting an idle run', async () => {
  for (const isStreaming of [true, false]) {
    const session = sessionDouble(isStreaming);
    await AgentSession.prototype.sendCustomMessage.call(session, notice, { triggerTurn: false, deliverAs: 'nextTurn' });
    assert.equal(session._pendingNextTurnMessages.length, 1);
    assert.deepEqual(session._pendingNextTurnMessages[0], {
      role: 'custom', customType: notice.customType, content: notice.content, display: false, details: notice.details,
      timestamp: session._pendingNextTurnMessages[0].timestamp,
    });
    assert.ok(Number.isFinite(session._pendingNextTurnMessages[0].timestamp));
  }
});

test('Pi 1.3 deliverAs steer feeds an active loop instead of the next run queue', async () => {
  const received = [];
  const session = sessionDouble(true);
  session.agent.steer = message => received.push(message);
  await AgentSession.prototype.sendCustomMessage.call(session, notice, { deliverAs: 'steer' });
  assert.equal(received.length, 1);
  assert.equal(received[0].customType, notice.customType);
  assert.deepEqual(session._pendingNextTurnMessages, []);
  assert.deepEqual(session._pendingCustomMessages, []);
});

test('Pi 1.3 consumes queued nextTurn mail immediately after the next run trigger', async () => {
  const queued = { role: 'custom', customType: notice.customType, content: notice.content, display: false, details: notice.details, timestamp: 2 };
  const options = { selectedTools: [] };
  const session = {
    _pendingNextTurnMessages: [queued], _baseSystemPromptOptions: options,
    _findLastAssistantMessage: () => undefined,
    _extensionRunner: { emitBeforeAgentStart: async () => ({ systemPromptOptions: options, messages: [] }) },
    getActiveToolNames: () => [],
    _normalizePromptImages: async images => ({ hints: [], images: images ?? [] }),
    _preparePromptAndToolLoadout: () => undefined,
  };
  const messages = await AgentSession.prototype._prepareRun.call(session, { messageType: 'user', prompt: 'user boundary' });
  assert.equal(messages[0].role, 'user');
  assert.equal(messages[1], queued);
  assert.deepEqual(session._pendingNextTurnMessages, []);
});
