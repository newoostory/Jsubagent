import { identifier, fail } from '../limits.mjs';

export class Mailbox {
  constructor(store) { this.store = store; this.metadata = store.metadata; this.scope = store.scope; }
  prior(id) { return this.metadata.get('messages', this.scope, identifier(id)); }
  enqueue(mail, digest) {
    const prior = this.prior(mail.id);
    if (prior) { if (prior.digest !== digest) throw fail('Message ID reused with different content', 'CONFLICT'); return prior; }
    const { body, ...record } = mail;
    return this.metadata.put('messages', this.scope, { ...record, digest, state: 'pending' }, { ifAbsent: true });
  }
  page(to, { state = 'pending', after = 0, limit = 32, steeringOnly = false } = {}) {
    identifier(to); identifier(state);
    if (!Number.isSafeInteger(after) || after < 0 || !Number.isSafeInteger(limit) || limit < 1 || limit > 32) throw fail('Invalid mailbox page');
    const rows = this.metadata.database.prepare(`SELECT seq,data_json FROM messages WHERE owner_id=? AND run_id=? AND state=? AND seq>? AND json_extract(data_json,'$.to')=? ${steeringOnly ? "AND json_extract(data_json,'$.delivery')='steer'" : ''} ORDER BY seq LIMIT ?`).all(this.scope.ownerId, this.scope.runId, state, after, to, limit + 1);
    const more = rows.length > limit, selected = rows.slice(0, limit);
    return { items: selected.map(row => JSON.parse(row.data_json)), nextCursor: more ? Number(selected.at(-1).seq) : null };
  }
  list(to, options = {}) { return this.page(to, options).items; }
  hasWakeMail(to) {
    identifier(to);
    return Boolean(this.metadata.database.prepare("SELECT 1 FROM messages WHERE owner_id=? AND run_id=? AND state='pending' AND json_extract(data_json,'$.to')=? AND COALESCE(json_extract(data_json,'$.event'),'')!='result_available' AND json_extract(data_json,'$.expiresAt')>=? LIMIT 1").get(this.scope.ownerId, this.scope.runId, to, Date.now()));
  }
  async pending(to, { limit = 32, steeringOnly = false, withBody = false } = {}) {
    const page = this.list(to, { limit, steeringOnly }), result = [];
    for (const row of page) {
      if (row.expiresAt < Date.now()) { this.metadata.transition('messages', this.scope, row.id, { from: 'pending', to: 'expired' }); continue; }
      const { state, ownerId, runId, updatedAt, digest, ...mail } = row;
      if (withBody && mail.bodyArtifactId) {
        const body = await this.store.readArtifact(mail.bodyArtifactId, 0, this.store.limits.messageBytes);
        if (!body.eof || body.artifact.owner !== to || !body.artifact.complete) throw fail('Mailbox body identity mismatch', 'STORAGE');
        mail.body = body.text;
      }
      result.push(mail);
    }
    return result;
  }
  deliver(ids) { this.metadata.transaction(() => { for (const id of ids) this.metadata.transition('messages', this.scope, id, { from: 'pending', to: 'delivering' }); }); }
  outbox(mail) {
    const prior = this.metadata.get('notification_outbox', this.scope, identifier(mail.id));
    if (prior) {
      for (const key of ['from', 'to', 'event', 'reportArtifactId']) if (prior[key] !== mail[key]) throw fail('Notification ID reused with different content', 'CONFLICT');
      return prior;
    }
    return this.metadata.put('notification_outbox', this.scope, { ...mail, state: 'pending', taskState: mail.state }, { ifAbsent: true });
  }
  notifications(limit = 32) { return this.metadata.list('notification_outbox', this.scope, { state: 'pending', limit }).items.map(({ ownerId, runId, state, updatedAt, taskState, ...row }) => ({ ...row, state: taskState })); }
  acknowledge(ids) {
    this.metadata.transaction(() => {
      for (const id of ids) {
        const event = this.metadata.get('notification_outbox', this.scope, id);
        if (event && event.state !== 'acknowledged') this.metadata.transition('notification_outbox', this.scope, id, { from: 'pending', to: 'acknowledged' });
        const mail = this.prior(id);
        if (mail?.to === 'main' && mail.state !== 'acknowledged') this.metadata.transition('messages', this.scope, id, { from: 'pending', to: 'acknowledged' });
      }
    });
  }
}
