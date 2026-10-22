import { encodeJson, fail, hash, identifier } from './limits.mjs';
import { resultBundleId, resultSelector } from './results.mjs';
import { eligibleResultInput } from './result-inputs.mjs';
import { registerSubscription, firstSubscriptionRevision } from './result-subscriptions.mjs';

export function normalizePublishTo(values = [], { index, batchSize } = {}) {
  if (!Array.isArray(values) || values.length > 8) throw fail('publishTo must contain at most eight routes');
  const routes = new Map();
  for (const value of values) {
    if (!value || typeof value !== 'object' || Array.isArray(value) || Object.keys(value).some(key => !['to', 'view', 'files', 'kinds', 'when', 'follow'].includes(key))) throw fail('Invalid publishTo fields');
    const to = value.to;
    if (!to || typeof to !== 'object' || Array.isArray(to) || Object.keys(to).length !== 1) throw fail('Result recipient requires agentId or batch task');
    if (to.agentId !== undefined) identifier(to.agentId, 'result recipient');
    else if (!Number.isInteger(to.task) || index === undefined || to.task < 0 || to.task >= batchSize || to.task === index) throw fail('Invalid publishTo batch recipient');
    const selector = Object.fromEntries(['view', 'files', 'kinds'].filter(key => value[key] !== undefined).map(key => [key, Array.isArray(value[key]) ? [...value[key]].sort() : value[key]]));
    resultSelector(selector);
    const when = value.when ?? 'final', follow = value.follow ?? 'once';
    if (!['published', 'candidate', 'final'].includes(when) || !['once', 'updates'].includes(follow)) throw fail('Invalid result notification milestone/follow mode');
    const id = hash(encodeJson({ to, selector, when }, 32768));
    const previous = routes.get(id);
    routes.set(id, { to: { ...to }, ...selector, when, follow: previous?.follow === 'updates' ? 'updates' : follow });
  }
  return [...routes.values()];
}
const slotId = id => `rs-${hash(id)}`;
const selectorOf = row => Object.fromEntries(['view', 'files', 'kinds'].filter(key => row[key] !== undefined).map(key => [key, row[key]]));
// Pin the exact allocated admission, including main-model binding. Unknown slots
// may only resolve through a persisted task carrying this same admission digest.
export const resultAdmissionHash = params => hash(encodeJson(params));
export class ResultNotifications {
  constructor(store, tasks, mail, authorize) { this.store = store; this.tasks = tasks; this.mail = mail; this.authorize = authorize; this.cursor = 0; }
  prepareBatch(batchId, rows, mainModel) {
    this.store.metadata.transaction(() => {
      for (const row of rows) {
        const id = slotId(row.id), admissionHash = resultAdmissionHash({ ...row.params, id: row.id, mainModel });
        const prior = this.store.metadata.get('result_slots', this.store.scope, id);
        if (prior) { if (prior.batchId !== batchId || prior.admissionHash !== admissionHash) throw fail('Result batch slot was reused', 'CONFLICT'); continue; }
        this.store.metadata.put('result_slots', this.store.scope, { id, state: 'pending', batchId, taskId: row.id, admissionHash }, { ifAbsent: true });
      }
    });
  }
  checkAdmission(params) {
    const slot = params.id && this.store.metadata.get('result_slots', this.store.scope, slotId(params.id));
    if (!slot) return undefined;
    const digest = resultAdmissionHash(params);
    if (digest !== slot.admissionHash) throw fail('Admission does not match the reserved result batch slot', 'CONFLICT');
    if (['rejected', 'not_started'].includes(slot.state)) throw fail('Rejected result slot cannot be automatically replayed', 'CONFLICT');
    return digest;
  }
  settleSlot(id, state) {
    const slot = this.store.metadata.get('result_slots', this.store.scope, slotId(id));
    if (!slot) return;
    const task = this.tasks.get(id);
    if (task?.batchAdmissionHash === slot.admissionHash) state = 'accepted';
    else if (state === 'accepted') state = 'unknown';
    if (slot.state === 'accepted' || ['rejected', 'not_started'].includes(slot.state)) return;
    this.store.metadata.put('result_slots', this.store.scope, { ...slot, state });
  }
  prepare(sourceId, values) {
    return normalizePublishTo(values).map(value => {
      const targetId = value.to.agentId;
      if (targetId === sourceId) throw fail('Self result notification is not supported', 'CONFLICT');
      const target = this.tasks.get(targetId), slot = this.store.metadata.get('result_slots', this.store.scope, slotId(targetId));
      if (!target && !slot) throw fail('Unknown result notification recipient', 'NOT_FOUND');
      if (target && (target.workItem || !target.role.tools.includes('agent_read'))) throw fail('Result recipient lacks the explicitly configured agent_read capability', 'CAPABILITY');
      const selector = selectorOf(value), resultEpoch = 1;
      const route = { id: `rt-${hash(encodeJson({ sourceId, targetId, resultEpoch, selector, when: value.when }, 32768))}`, state: target ? 'watching' : 'pending_binding', sourceId, targetId, resultEpoch, bundleId: resultBundleId(sourceId, resultEpoch), selector, when: value.when, follow: value.follow, examinedRevision: 0, deliveredRevision: null };
      encodeJson(route, 30 * 1024); return route;
    });
  }
  register(rows) {
    this.store.metadata.transaction(() => { for (const row of rows) {
      const prior = this.store.metadata.get('result_routes', this.store.scope, row.id);
      if (prior) { for (const key of ['sourceId', 'targetId', 'resultEpoch', 'selector', 'when', 'follow']) if (JSON.stringify(prior[key]) !== JSON.stringify(row[key])) throw fail('Durable result route changed', 'STORAGE'); continue; }
      registerSubscription(this.store, row);
      this.store.metadata.put('result_routes', this.store.scope, row, { ifAbsent: true });
    } });
  }
  rows(sourceId) {
    return this.store.metadata.database.prepare("SELECT data_json FROM result_routes WHERE run_id=? AND owner_id=? AND json_extract(data_json,'$.sourceId')=? ORDER BY seq LIMIT 8").all(this.store.scope.runId, this.store.scope.ownerId, sourceId).map(row => JSON.parse(row.data_json));
  }
  deliver(route, manifest, target, next) {
    const source = this.tasks.get(route.sourceId);
    this.authorize(source, target);
    const eventId = `rd-${hash(encodeJson([route.resultEpoch, manifest.publicationId, route.id, target.id]))}`;
    const binding = { agentId: route.sourceId, resultEpoch: route.resultEpoch, bundleId: route.bundleId, revision: manifest.revision, selector: route.selector, inputBindingId: eventId };
    const payload = { type: 'result_available', eventId, routeId: route.id, sourceAgentId: route.sourceId, resultEpoch: route.resultEpoch, bundleId: route.bundleId, revision: manifest.revision, stage: manifest.stage, inputBindingId: eventId,
      read: { source: 'bundle', agentId: route.sourceId, bundleId: route.bundleId, revision: manifest.revision, view: route.selector.view ?? 'artifacts', inputBindingId: eventId } };
    const now = Date.now(), digest = hash(encodeJson(payload, 24 * 1024));
    // Grant, typed inbox event and cursor commit together. No artifact writes,
    // awaits, provider calls, task epoch changes, or auto-wake occur here.
    this.store.metadata.transaction(() => {
      this.authorize(source, target);
      const prior = this.store.metadata.get('result_deliveries', this.store.scope, eventId);
      if (prior && prior.digest !== digest) throw fail('Result delivery identity changed', 'STORAGE');
      if (!prior) {
        if (route.follow === 'updates' && route.lastEventId) {
          const old = this.mail.prior(route.lastEventId);
          if (old?.state === 'pending') this.store.metadata.transition('messages', this.store.scope, old.id, { from: 'pending', to: 'superseded' });
        }
        this.store.metadata.put('result_deliveries', this.store.scope, { id: eventId, state: 'delivered', sourceId: route.sourceId, targetId: target.id, routeId: route.id, binding, digest, payload }, { ifAbsent: true });
        this.mail.enqueue({ id: eventId, from: route.sourceId, to: target.id, kind: 'result', event: 'result_available', delivery: 'followUp', resultAvailable: payload, createdAt: now, expiresAt: now + 7 * 86_400_000 }, digest);
      }
      this.store.metadata.put('result_routes', this.store.scope, { ...next, lastEventId: eventId });
    });
  }
  grant(task, request) {
    if (!task.role.tools.includes('agent_read') || task.cancelRequested || task.authorityRevoked) throw fail('Result read capability revoked', 'PERMISSION');
    const row = request.inputBindingId && this.store.metadata.get('result_deliveries', this.store.scope, request.inputBindingId);
    if (!row || row.targetId !== task.id || row.sourceId !== request.agentId || (request.bundleId && request.bundleId !== row.binding.bundleId) || (request.revision && request.revision !== row.binding.revision)) throw fail('Result read requires an exact durable delivery grant', 'PERMISSION');
    return row.binding;
  }
  advance() {
    const db = this.store.metadata.database;
    const page = db.prepare("SELECT seq,data_json FROM result_routes WHERE run_id=? AND owner_id=? AND state IN ('watching','pending_binding') AND seq>? ORDER BY seq LIMIT 65").all(this.store.scope.runId, this.store.scope.ownerId, this.cursor);
    const rows = page.slice(0, 64); this.cursor = page.length > 64 ? Number(rows.at(-1).seq) : 0;
    let more = page.length > 64;
    for (const item of rows) {
      const route = JSON.parse(item.data_json), source = this.tasks.get(route.sourceId), target = this.tasks.get(route.targetId);
      const close = reason => this.store.metadata.put('result_routes', this.store.scope, { ...route, state: 'route_rejected', reason });
      if (!source || source.cancelRequested || source.authorityRevoked || ['failed', 'cancelled', 'archived'].includes(source.state) || (source.resultEpoch ?? 1) !== route.resultEpoch) { close('Producer cancelled, unavailable, or changed epoch'); continue; }
      const slot = this.store.metadata.get('result_slots', this.store.scope, slotId(route.targetId));
      if (slot && slot.state !== 'accepted') {
        if (['rejected', 'not_started'].includes(slot.state)) { close('Recipient admission rejected'); continue; }
        if (target?.batchAdmissionHash === slot.admissionHash) this.settleSlot(target.id, 'accepted');
        else continue;
      }
      if (!target) continue;
      if (target.cancelRequested || target.authorityRevoked || target.state === 'archived' || target.workItem || !target.role.tools.includes('agent_read')) { close('Recipient cancelled, archived, or lacks result-read capability'); continue; }
      let chosen, next = { ...route, state: 'watching' };
      if (route.deliveredRevision === null) {
        const selected = firstSubscriptionRevision(this.store, route, eligibleResultInput);
        chosen = selected.manifest; more ||= selected.more;
        if (chosen) { next.examinedRevision = chosen.revision; if (route.follow === 'updates') more = true; }
      } else {
        const publications = db.prepare("SELECT data_json FROM result_revisions WHERE run_id=? AND owner_id=? AND json_extract(data_json,'$.bundleId')=? AND json_extract(data_json,'$.revision')>? ORDER BY json_extract(data_json,'$.revision') LIMIT 32").all(this.store.scope.runId, this.store.scope.ownerId, route.bundleId, route.examinedRevision);
        more ||= publications.length === 32;
        for (const row of publications) {
          const manifest = JSON.parse(row.data_json); next.examinedRevision = manifest.revision;
          if (eligibleResultInput(this.store, manifest, route)) chosen = manifest;
        }
      }
      if (!chosen) { if (next.examinedRevision !== route.examinedRevision || next.state !== route.state) this.store.metadata.put('result_routes', this.store.scope, next); continue; }
      next.deliveredRevision = chosen.revision; next.state = route.follow === 'once' ? 'delivered' : 'watching';
      try { this.deliver(route, chosen, target, next); }
      catch (error) { if (['PERMISSION', 'CANCELLED', 'AUTHORITY_REVOKED', 'MODE', 'CAPABILITY', 'UNSUPPORTED'].includes(error.code)) close(error.message); else throw error; }
    }
    return more;
  }
}
