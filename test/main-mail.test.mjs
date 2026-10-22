import test from 'node:test';
import assert from 'node:assert/strict';
import { compactMailContext, restoreMailContext, mailReference, notification, persistedNotice, requiresMainAttention, hiddenMailRenderer, MAIL_CONTEXT_BYTES, MAIL_CONTEXT_COUNT } from '../src/main-mail.mjs';

const row = n => mailReference({ id: `mail-${n}`, from: 'reviewer', reportArtifactId: `body-${n}`, event: 'completion', state: 'idle', kind: 'result', body: 'PRIVATE_PEER_BODY', delivery: 'followUp', expiresAt: Date.now() + 10000 });
const message = (rows, binding = 'branch-a') => ({ role: 'custom', timestamp: 1, ...notification(rows, { notificationId: `notice-${rows[0]?.messageId}`, runBinding: binding }) });
const refs = messages => messages.flatMap(m => m.details?.references ?? []);

test('internal mail is hidden metadata with only bounded explicit-read references', () => {
  const notice = notification([row(1)]);
  assert.equal(notice.display, false); assert.doesNotMatch(notice.content, /PRIVATE_PEER_BODY/);
  assert.match(notice.content, /not user instructions/); assert.match(notice.content, /Process silently/);
  assert.deepEqual(hiddenMailRenderer().render(80), []);
  const entry = { type: 'custom_message', ...notice }; assert.equal(persistedNotice(entry), entry);
});

test('only questions, blockers and explicit steer intermediate mail become model notices', () => {
  const message = (n, kind, delivery = 'followUp') => ({ id: `message-${n}`, from: 'scout', bodyArtifactId: `message-body-${n}`, kind, delivery });
  assert.equal(requiresMainAttention(message(1, 'question')), true);
  assert.equal(requiresMainAttention(message(2, 'blocked')), true);
  assert.equal(requiresMainAttention(message(3, 'finding', 'steer')), true);
  assert.equal(requiresMainAttention(message(4, 'finding')), false);
  assert.equal(requiresMainAttention(message(5, 'answer')), false);
  const notice = notification([
    mailReference(message(1, 'question')),
    mailReference(message(2, 'blocked')),
    mailReference(message(3, 'finding', 'steer')),
    mailReference(message(4, 'finding')),
    mailReference(message(5, 'answer')),
  ]);
  assert.deepEqual(notice.details.messageIds, ['message-1', 'message-2', 'message-3']);
  assert.ok(notice.details.references.every(row => row.event === 'message' && row.read.requests[0].source === 'message'));
});

test('delayed notices keep their locations; reads never mutate prior mail or explicit evidence', () => {
  const user = { role: 'user', content: 'Team response notifications is text I am asking about.' };
  const tool = { role: 'toolResult', toolName: 'agent', content: [{ type: 'text', text: 'Explicit body evidence' }] };
  const notices = Array.from({ length: 15 }, (_, n) => message([row(n)]));
  const input = [user, ...notices, tool]; const before = structuredClone(input);
  const out = compactMailContext(input, { binding: 'branch-a' });
  assert.equal(refs(out).length, 15); assert.equal(out.filter(m => m.role === 'custom').length, 15);
  assert.equal(out[0], user); assert.equal(out.at(-1), tool); assert.deepEqual(input, before);
  assert.ok(out.filter(m => m.role === 'custom').every(m => m.display === false));
  assert.deepEqual(compactMailContext(input, { binding: 'branch-a', readArtifacts: new Set(['body-0', 'body-14']) }), out);
});

test('duplicates, old branches and legacy custom notices are isolated without relocating valid mail', () => {
  const first = message([row(1)]), second = message([row(1), row(2)]);
  const out = compactMailContext([first, second, message([row(3)], 'branch-other')], { binding: 'branch-a' });
  assert.deepEqual(refs(out).map(r => r.messageId), ['mail-1', 'mail-2']);
  assert.deepEqual(out[0].details.messageIds, ['mail-1']);
  const legacy = { role: 'custom', customType: 'pi-agent-mail', content: 'Team response notifications\n' + JSON.stringify([row(7)]), display: true };
  assert.deepEqual(compactMailContext([legacy], { binding: 'branch-a' }), [], 'unbound old notices are never reinterpreted');
  assert.deepEqual(compactMailContext([{ ...legacy, details: { runBinding: 'branch-a' } }], { binding: 'branch-a' }), [], 'body-only legacy records are not imported');
  const user = { role: 'user', content: legacy.content }; assert.equal(compactMailContext([user]), undefined);
  const intermediate = { ...row(8), event: undefined, kind: 'finding', read: { ...row(8).read, requests: [{ ...row(8).read.requests[0], source: 'message' }] } };
  assert.equal(notification([intermediate]).details.references.length, 0);
  assert.deepEqual(compactMailContext([{ ...legacy, content: 'Team response notifications\n' + JSON.stringify([intermediate]) }]), []);
  const repeatedArtifact = { ...row(1), messageId: 'different-mail-id' };
  assert.equal(notification([row(1), repeatedArtifact]).details.references.length, 1);
  assert.equal(refs(compactMailContext([message([row(1)]), message([repeatedArtifact])], { binding: 'branch-a' })).length, 1);
});

test('whole projected mail stays below 16KiB/32 refs; overflow does not rewrite earlier notices', () => {
  const rows = Array.from({ length: 100 }, (_, n) => ({ ...row(n), body: 'SECRET', enormous: 'x'.repeat(100000) }));
  const input = rows.map(r => message([r]));
  const out = compactMailContext(input, { binding: 'branch-a' });
  assert.ok(out.reduce((n, m) => n + Buffer.byteLength(m.content), 0) <= MAIL_CONTEXT_BYTES);
  assert.equal(refs(out).length, MAIL_CONTEXT_COUNT);
  assert.doesNotMatch(JSON.stringify(out), /SECRET|enormous/);
  assert.deepEqual(out, compactMailContext(input.slice(0, 32), { binding: 'branch-a' }));
  assert.equal(notification(rows).details.references.length, MAIL_CONTEXT_COUNT);
  assert.equal(persistedNotice({ type: 'message', message: out[0] }), out[0]);
  assert.equal(persistedNotice({ type: 'message', message: { role: 'user', content: out[0].content } }), undefined);
  const large = rows.map((_, n) => message([mailReference({ id: `mail-${n}`.padEnd(80, 'm'), from: 'a'.repeat(80),
    reportArtifactId: `body-${n}`.padEnd(80, 'b'), event: 'completion', state: 'idle', kind: 'result' })]));
  const bounded = compactMailContext(large, { binding: 'branch-a' });
  assert.ok(refs(bounded).length > 0 && refs(bounded).length < MAIL_CONTEXT_COUNT, 'byte cap independently limits maximum-length valid identifiers');
  assert.ok(bounded.reduce((n, m) => n + Buffer.byteLength(m.content), 0) <= MAIL_CONTEXT_BYTES);
});

test('running-loop repair uses durable predecessor rather than the current tail and deduplicates native mail', () => {
  const user = { role: 'user', content: 'question', timestamp: 10 };
  const assistant = { role: 'assistant', content: [], timestamp: 20 };
  const result = { role: 'toolResult', toolCallId: 'a', timestamp: 21, content: [] };
  const later = { role: 'assistant', content: [], timestamp: 30 };
  const mail = message([row(1)]), mail2 = message([row(2)]);
  const projected = [user, assistant, result, mail, mail2, later];
  const loop = structuredClone([user, assistant, result, later]);
  assert.deepEqual(restoreMailContext(loop, projected), projected);
  assert.equal(restoreMailContext(projected, projected), projected);
  assert.deepEqual(restoreMailContext([user, assistant, result, mail, later], projected), projected);
  assert.deepEqual(restoreMailContext([user, assistant, result, mail2, later], projected), projected);
});

test('missing or ambiguous predecessor cannot resurrect compacted/pruned notices', () => {
  const user = { role: 'user', content: 'question', timestamp: 10 };
  const mail = message([row(1)]);
  const compacted = [{ role: 'compactionSummary', content: 'summary', timestamp: 20 }];
  assert.equal(restoreMailContext(compacted, [user, mail]), compacted);
  assert.equal(restoreMailContext([user], compacted).length, 1, 'raw branch-only notices never enter the projection');
  const ambiguous = [user, { ...user }];
  assert.equal(restoreMailContext(ambiguous, [user, mail]), ambiguous);
});
