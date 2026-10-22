import { encodeJson, hash } from './limits.mjs';

// Inputs and notifications share the first eligible immutable revision. Explicit
// historical inputs intentionally have a separate identity from live routes.
export function subscriptionId(row) {
  return `rsu-${hash(encodeJson({ sourceId: row.sourceId, targetId: row.targetId, resultEpoch: row.resultEpoch, selector: row.selector, when: row.when, revision: row.explicitRevision ?? null }, 32768))}`;
}
export function subscription(store, row) {
  return store.metadata.get('result_subscriptions', store.scope, subscriptionId(row));
}
export function registerSubscription(store, row) {
  const prior = subscription(store, row);
  if (prior) {
    if (row.required && !prior.required) store.metadata.put('result_subscriptions', store.scope, { ...prior, required: true });
    return subscription(store, row);
  }
  const state = { id: subscriptionId(row), state: row.binding ? 'bound' : 'waiting', sourceId: row.sourceId, targetId: row.targetId, resultEpoch: row.resultEpoch, bundleId: row.bundleId, selector: row.selector, when: row.when, required: Boolean(row.required), examinedRevision: row.examinedRevision ?? 0, revision: row.binding?.revision ?? null };
  store.metadata.put('result_subscriptions', store.scope, state, { ifAbsent: true });
  return state;
}
export function firstSubscriptionRevision(store, row, eligible) {
  return store.metadata.transaction(() => {
    let state = registerSubscription(store, row);
    if (state.revision) return { manifest: store.metadata.get('result_revisions', store.scope, hash(`${row.bundleId}\0${state.revision}`)), more: false };
    const page = store.metadata.database.prepare("SELECT data_json FROM result_revisions WHERE run_id=? AND owner_id=? AND json_extract(data_json,'$.bundleId')=? AND json_extract(data_json,'$.revision')>? ORDER BY json_extract(data_json,'$.revision') LIMIT 32").all(store.scope.runId, store.scope.ownerId, row.bundleId, state.examinedRevision);
    for (const item of page) {
      const manifest = JSON.parse(item.data_json);
      state = { ...state, examinedRevision: manifest.revision };
      if (eligible(store, manifest, row)) {
        state = { ...state, state: 'bound', revision: manifest.revision };
        store.metadata.put('result_subscriptions', store.scope, state);
        return { manifest, more: false };
      }
    }
    if (page.length) store.metadata.put('result_subscriptions', store.scope, state);
    return { manifest: null, more: page.length === 32 };
  });
}
