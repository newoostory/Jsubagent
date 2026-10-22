import { fail } from '../limits.mjs';

function plain(value, name) {
  if (!value || typeof value !== 'object' || Array.isArray(value) || Object.getPrototypeOf(value) !== Object.prototype) throw fail(`Invalid ${name}`, 'STORAGE');
  return value;
}

export function jsonBytes(value, name = 'context record') {
  let serialized;
  try { serialized = JSON.stringify(value); }
  catch { throw fail(`${name} is not serializable`, 'STORAGE'); }
  if (serialized === undefined) throw fail(`${name} is not serializable`, 'STORAGE');
  return Buffer.from(serialized);
}

function callIds(message) {
  if (message?.role !== 'assistant' || !Array.isArray(message.content)) return [];
  const ids = message.content.filter(part => part?.type === 'toolCall').map(part => part.id);
  if (ids.some(id => typeof id !== 'string' || !id) || new Set(ids).size !== ids.length) throw fail('Assistant tool calls require unique nonempty IDs', 'STORAGE');
  return ids;
}

export function describeContextGroup(messages) {
  if (!Array.isArray(messages) || !messages.length) throw fail('Checkpoint context group must not be empty', 'STORAGE');
  messages.forEach((message, index) => plain(message, `checkpoint message ${index}`));
  const ids = callIds(messages[0]);
  if (!ids.length) {
    if (messages.length !== 1 || messages[0].role === 'toolResult') throw fail('Checkpoint contains an orphan tool result', 'STORAGE');
    return { type: 'message', messageCount: 1, roles: [messages[0].role] };
  }
  if (messages.length !== ids.length + 1) throw fail('Checkpoint tool group is incomplete', 'STORAGE');
  const expected = new Set(ids), seen = new Set();
  for (const message of messages.slice(1)) {
    if (message.role !== 'toolResult' || !expected.has(message.toolCallId) || seen.has(message.toolCallId)) throw fail('Checkpoint tool group does not match its assistant calls', 'STORAGE');
    seen.add(message.toolCallId);
  }
  if (seen.size !== expected.size) throw fail('Checkpoint tool group is incomplete', 'STORAGE');
  return { type: 'tool', messageCount: messages.length, roles: messages.map(message => message.role), toolCallIds: ids };
}

export function groupContextMessages(messages) {
  if (!Array.isArray(messages)) throw fail('Checkpoint messages must be an array', 'STORAGE');
  const groups = [];
  for (let index = 0; index < messages.length;) {
    const message = plain(messages[index], `checkpoint message ${index}`);
    if (message.role === 'toolResult') throw fail('Checkpoint contains an orphan tool result', 'STORAGE');
    const ids = callIds(message);
    if (!ids.length) {
      groups.push([message]);
      index++;
      continue;
    }
    const group = [message];
    for (let resultIndex = 0; resultIndex < ids.length; resultIndex++) {
      const result = messages[index + 1 + resultIndex];
      if (!result || result.role !== 'toolResult') throw fail('Checkpoint tool group is incomplete', 'STORAGE');
      group.push(result);
    }
    describeContextGroup(group);
    groups.push(group);
    index += group.length;
  }
  return groups;
}

export function measureResidentContext(messages) {
  if (!Array.isArray(messages)) throw fail('Resident context must be an array', 'STORAGE');
  let bytes = 0;
  for (const [index, message] of messages.entries()) {
    const size = jsonBytes(message, `context message ${index}`).length;
    if (!Number.isSafeInteger(bytes + size)) throw fail('Resident context byte count overflow', 'LIMIT');
    bytes += size;
  }
  return bytes;
}

export function addTranscriptBytes(current, bytes) {
  if (!Number.isSafeInteger(current) || current < 0 || !Number.isSafeInteger(bytes) || bytes < 0 || !Number.isSafeInteger(current + bytes)) throw fail('Transcript byte count overflow', 'LIMIT');
  return current + bytes;
}
