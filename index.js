import { randomUUID } from 'node:crypto';
import { getPackageDir } from '@earendil-works/pi-coding-agent';
import { realpath } from 'node:fs/promises';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { presentAgentResult } from './src/agent-tool.mjs';
import { PublicAgentParameters, validatePublicAgentParameters } from './src/public-api.mjs';
import { publicRead } from './src/public-read.mjs';
import { publicOperation } from './src/public-operations.mjs';
import { HostClient, runRoot } from './src/client.mjs';
import { rememberSessionHost, closeProcessSessionHosts } from './src/session-hosts.mjs';
import { loadCatalog, coordinatorPrompt, COORDINATOR_MARKER } from './src/coordinator.mjs';
import { observe } from './src/observe.mjs';
import { Hub } from './src/hub.mjs';
import { teamStatus } from './src/task-status.mjs';
import { renderAgentCall, renderAgentResult } from './src/agent-renderer.mjs';
import { receiveWaitResponses } from './src/wait-responses.mjs';
import { resultReceiptKey, receiveResultPrefix, storedResultReceipt } from './src/result-receipts.mjs';
import { planSpawnBatch } from './src/batch-plan.mjs';
import { LIMITS, encodeJson, short, safeError, fail } from './src/limits.mjs';
import { loadConfiguration, assertProfilePin } from './src/config.mjs';
import { authorizeMainMutation } from './src/mode-access.mjs';
import { requiredModes } from './src/mode-access.mjs';
import { isBenchmark, loadBenchmarkGrant, authorizeBenchmarkJob } from './src/benchmark-access.mjs';
import { MAIL_TYPE, notification, mailReference, requiresMainAttention, persistedNotice, compactMailContext, restoreMailContext, hiddenMailRenderer } from './src/main-mail.mjs';
import { MAIN_MODEL_TOOLS, DEFAULT_MAIN_COORDINATOR_ONLY, mainCoordinatorToolGate, narrowMainModelTools } from './src/main-coordinator-guard.mjs';
import { accountChildUsage, childUsageRows, restoreChildUsage } from './src/parent-usage.mjs';

const textResult = (value, { details, usage } = {}) => ({
  content: [{ type: 'text', text: typeof value === 'string' ? value : encodeJson(value, LIMITS.frameBytes).toString('utf8') }],
  details,
  ...(usage ? { usage } : {}),
});
const MAIN_GUARD_ENTRY = 'pi-agent-main-tool-guard';
const MAIN_GUARD_BLOCKER_MARKER = '## pi-agent coordinator blocked (main only)';
const MAIN_GUARD_TOOL_SET = new Set(MAIN_MODEL_TOOLS);
function boundedToolNames(value) {
  if (!Array.isArray(value) || value.length > 256) return null;
  const names = []; let bytes = 0;
  for (const name of value) {
    if (typeof name !== 'string' || !name || Buffer.byteLength(name) > 128 || names.includes(name)) return null;
    bytes += Buffer.byteLength(name); if (bytes > 16 * 1024) return null; names.push(name);
  }
  return names;
}
function savedMainGuard(branch) {
  for (const entry of [...branch].reverse()) {
    if (entry.type !== 'custom' || entry.customType !== MAIN_GUARD_ENTRY) continue;
    if (Object.hasOwn(entry.data ?? {}, 'version')) throw fail('Incompatible saved main tool guard; inspect before continuing', 'STORAGE');
    if (!entry.data || typeof entry.data !== 'object' || Array.isArray(entry.data) || Object.keys(entry.data).some(key => !['enabled', 'baseline', 'applied'].includes(key)) || typeof entry.data.enabled !== 'boolean') throw fail('Invalid saved main tool guard', 'STORAGE');
    const baseline = boundedToolNames(entry.data.baseline), applied = boundedToolNames(entry.data.applied);
    if (!baseline || !applied) throw fail('Invalid saved main tool guard tools', 'STORAGE');
    return { enabled: entry.data.enabled, baseline, applied };
  }
  return undefined;
}
const sameTools = (left, right) => Array.isArray(left) && Array.isArray(right) && left.length === right.length && left.every((name, index) => name === right[index]);
export default function piAgent(pi) {
  if (process.env.PI_AGENT_HOST === '1' || process.env.PI_AGENT_TOOL === '1') return;
  let guardEnabled = DEFAULT_MAIN_COORDINATOR_ONLY, guardBaseline = [], guardApplied, guardPersisted, guardStorageFailure;
  pi.on('tool_call', event => {
    if (guardStorageFailure) return { block: true, reason: 'Incompatible saved main guard; use /agent to explicitly select a new guard state. Saved tool names will not be restored.' };
    if (!guardEnabled) return undefined;
    try { return mainCoordinatorToolGate(event, true, pi.getActiveTools()); }
    catch { return mainCoordinatorToolGate(event, true, []); }
  });
  let client, ctxNow, sessionId, runBinding, generation = 0, timer, polling = false, lastMonitor = 0, seen = new Set();
  let starting, hub, observedRoot, awaitingMail = new Map(), readArtifacts = new Set(), waitReceipts = new Map(), accountedChildUsage = new Map();
  let receiptTail = Promise.resolve();
  // Also hide historical display:true notices on a later user-controlled reload.
  pi.registerMessageRenderer(MAIL_TYPE, hiddenMailRenderer);
  const agentDirectory = () => process.env.PI_CODING_AGENT_DIR?.replace(/^~(?=\/)/, homedir()) ?? join(homedir(), '.pi', 'agent');
  function closeHub() { hub?.close(); hub = undefined; }
  async function catalog(ctx) {
    if (client?.info && !client.closed && !client.exited) return client.request('catalog');
    return loadCatalog({ cwd: await realpath(ctx.cwd), agentDir: await realpath(agentDirectory()), trusted: ctx.isProjectTrusted() });
  }
  async function preflightSpawn(ctx, params) {
    const config = await loadConfiguration({ cwd: await realpath(ctx.cwd), agentDir: await realpath(agentDirectory()), trusted: ctx.isProjectTrusted() });
    const tasks = params.tasks ?? [params];
    const selected = tasks.map(item => {
      const role = config.roles[item.role], profile = config.profiles[item.profile ?? role?.profile];
      if (!role) throw fail(`Unknown role: ${item.role}`);
      if (!profile) throw fail(`Unknown profile: ${item.profile ?? role.profile}`);
      assertProfilePin(role, profile, item.profile ?? role.profile);
      return { item, role, profile };
    });
    // Parent checks role pins and the host's actual runtime authority. A mode
    // receipt is advisory only: spawn rechecks it in the host.
    const names = [...new Set(selected.flatMap(({ role, profile }) => requiredModes(role, profile)).filter(name => name !== 'benchmark'))];
    const modes = new Map();
    if (names.length && client?.info && !client.closed && !client.exited) {
      const active = await client.request('mode_authority', { names });
      for (const name of names) if (active[name]?.root) modes.set(name, active[name]);
    }
    for (const { item, role, profile } of selected) {
      if (isBenchmark(role, profile)) {
        const root = observedRoot ?? (runBinding ? runRoot(config.agentDir, config.cwd, `${sessionId}:${runBinding}`) : undefined);
        const grant = root ? await loadBenchmarkGrant(config.agentDir, root) : null;
        if (grant) modes.set('benchmark', grant);
        authorizeMainMutation(role, profile, modes);
        authorizeBenchmarkJob(role, { ...profile, name: item.profile ?? role.profile }, grant, { directory: item.directory ? await realpath(item.directory) : grant.root, task: item.task, projectInstructions: item.projectInstructions, dependsOn: item.dependsOn });
      } else authorizeMainMutation(role, profile, modes);
    }
  }
  // Pi creates fresh context wrappers for individual tools/commands. Object identity
  // is NOT a session identity; compare the stable ID plus this runtime generation.
  function alive(ctx, gen) {
    if (!ctxNow || generation !== gen) return false;
    try { return ctx.sessionManager.getSessionId() === sessionId; } catch { return false; }
  }
  function notify(ctx, content, level = 'info') { if (ctx.hasUI) ctx.ui.notify(short(content, 2048), level); }
  function persistMainGuard() {
    const data = { enabled: guardEnabled, baseline: boundedToolNames(guardBaseline) ?? [], applied: boundedToolNames(guardApplied ?? []) ?? [] };
    const encoded = JSON.stringify(data);
    if (encoded === guardPersisted) return;
    pi.appendEntry(MAIN_GUARD_ENTRY, data); guardPersisted = encoded;
  }
  function reconcileMainGuard(active) {
    if (!guardApplied || sameTools(active, guardApplied)) return;
    const current = new Set(active);
    guardBaseline = guardBaseline.filter(name => !MAIN_GUARD_TOOL_SET.has(name) || current.has(name));
    for (const name of active) if (!guardBaseline.includes(name)) guardBaseline.push(name);
  }
  function enforceMainGuard(selectedTools) {
    const active = pi.getActiveTools();
    reconcileMainGuard(active);
    const narrowed = narrowMainModelTools(pi, selectedTools);
    guardApplied = narrowed; persistMainGuard(); return narrowed;
  }
  function restoreMainGuard(ctx) {
    const stored = savedMainGuard(ctx.sessionManager.getBranch());
    guardEnabled = stored?.enabled ?? DEFAULT_MAIN_COORDINATOR_ONLY;
    guardPersisted = stored ? JSON.stringify({ enabled: stored.enabled, baseline: stored.baseline, applied: stored.applied }) : undefined;
    const active = pi.getActiveTools();
    if (!guardEnabled) { guardBaseline = active; guardApplied = undefined; persistMainGuard(); return active; }
    if (stored && sameTools(active, stored.applied)) { guardBaseline = stored.baseline; guardApplied = stored.applied; }
    else { guardBaseline = active; guardApplied = undefined; }
    return enforceMainGuard();
  }
  function recoverMainGuard(enabled) {
    // Only the user's picker may supersede incompatible state. Never trust its
    // baseline or activate registered-but-currently-disabled tools during recovery.
    const baseline = boundedToolNames(pi.getActiveTools());
    if (!baseline) throw fail('Invalid active main tools during guard recovery', 'STORAGE');
    const applied = enabled ? baseline.filter(name => MAIN_GUARD_TOOL_SET.has(name)) : baseline;
    const data = { enabled, baseline, applied: enabled ? applied : [] };
    pi.setActiveTools(applied);
    // Keep the fail-closed blocker until the append is successful.
    pi.appendEntry(MAIN_GUARD_ENTRY, data);
    guardEnabled = enabled; guardBaseline = baseline; guardApplied = enabled ? applied : undefined;
    guardPersisted = JSON.stringify(data); guardStorageFailure = undefined;
  }
  function enableMainGuard() {
    if (guardEnabled) { enforceMainGuard(); return false; }
    guardBaseline = pi.getActiveTools(); guardApplied = undefined; guardEnabled = true;
    enforceMainGuard(); return true;
  }
  function disableMainGuard() {
    if (!guardEnabled) return false;
    const active = pi.getActiveTools(); reconcileMainGuard(active);
    const registered = new Set(pi.getAllTools().map(tool => tool.name));
    const restored = guardBaseline.filter(name => registered.has(name));
    pi.setActiveTools(restored); guardEnabled = false; guardBaseline = restored; guardApplied = undefined; persistMainGuard(); return true;
  }
  function guardBlockerPrompt(systemPrompt, detail) {
    return `${systemPrompt}\n\n${MAIN_GUARD_BLOCKER_MARKER}\nMain is coordinator-only, but its delegation/tool guard is unavailable: ${short(detail, 256)}. Do not execute the work in main or use another execution route. Report this blocker so configuration or capability can be corrected.`;
  }
  async function ensure(ctx) {
    if (!alive(ctx, generation)) throw fail('Main session is not active');
    if (client && !client.exited && !client.closed) {
      const instance = client; await starting;
      if (client !== instance || instance.closed || instance.exited) throw fail('Host changed while awaiting readiness');
      return instance;
    }
    if (starting) return starting;
    const gen = generation;
    if (!runBinding) { runBinding = randomUUID(); pi.appendEntry('pi-agent-run-binding', { token: runBinding }); }
    const binding = runBinding;
    starting = (async () => {
      const cwd = await realpath(ctx.cwd);
      const agentDir = await realpath(agentDirectory());
      if (!alive(ctx, gen)) throw fail('Session changed before host startup');
      const instance = new HostClient({ cwd, agentDir, mainSessionId: `${sessionId}:${binding}`, trusted: ctx.isProjectTrusted(), piPackageDir: getPackageDir(), onExit: info => {
        if (alive(ctx, gen) && client === instance && !instance.closed) {
          notify(ctx, `pi-agent host exited (${info.code ?? info.signal}). Records retained; the next agent operation reconnects automatically. Inspect uncertain work before explicit resume.`, 'warning');
          if (ctx.hasUI) { ctx.ui.setStatus('pi-agent', 'agents offline'); ctx.ui.setWidget('pi-agent', ['pi-agent offline · saved states in /hub; liveness unknown'], { placement: 'belowEditor' }); }
        }
      } });
      client = instance; observedRoot = instance.options.root;
      try {
        await instance.start();
        // A session change can race startup. Remember every verified connection
        // before detaching so process quit can still stop that owned supervisor.
        rememberSessionHost(instance);
        if (!alive(ctx, gen) || client !== instance || instance.closed || instance.exited) { await instance.detach(); throw fail('Session or host changed during host startup'); }
        schedule(); return instance;
      } catch (e) { if (client === instance) client = undefined; await instance.detach().catch(() => {}); throw e; }
    })();
    try { return await starting; } finally { starting = undefined; }
  }
  function rememberReadArtifacts(ids) {
    const fresh = [...new Set(ids)].filter(id => id && !readArtifacts.has(id));
    if (!fresh.length) return;
    pi.appendEntry('pi-agent-read-artifacts', { ids: fresh });
    for (const id of fresh) readArtifacts.add(id);
    readArtifacts = new Set([...readArtifacts].slice(-1024));
  }
  function rememberWaitPages(pages) {
    const fresh = [];
    for (const page of pages) {
      const key = resultReceiptKey(page);
      if (!key) continue;
      const receipt = receiveResultPrefix(page, waitReceipts.get(key));
      if (!receipt) continue;
      waitReceipts.set(key, receipt); fresh.push(receipt);
    }
    if (fresh.length) pi.appendEntry('pi-agent-wait-receipts', { receipts: fresh });
    waitReceipts = new Map([...waitReceipts].slice(-1024));
  }
  function receiveWait(ctx, gen, host, waited, options) {
    // Keep host waits concurrent; only the short receipt transaction is serial.
    // A second results:new call must inspect receipts committed by the first.
    const work = receiptTail.catch(() => {}).then(async () => {
      options.signal?.throwIfAborted();
      if (!alive(ctx, gen) || client !== host) throw fail('Session or host changed before response receipt');
      const received = await receiveWaitResponses(host.options.root, presentAgentResult('wait', waited, { readArtifacts, compact: true }), { ...options, receipts: waitReceipts });
      options.signal?.throwIfAborted();
      if (!alive(ctx, gen) || client !== host) throw fail('Session or host changed during response receipt');
      rememberReadArtifacts(received.received);
      rememberWaitPages(received.rows.map(row => row.response));
      return received;
    });
    receiptTail = work.then(() => {}, () => {});
    return work;
  }
  function render(ctx, status) {
    if (!ctx.hasUI) return;
    const { lines } = teamStatus(status);
    ctx.ui.setWidget('pi-agent', lines.length ? lines : undefined, { placement: 'belowEditor' });
    // The widget owns counters/usage. Repeating counts in the native footer
    // renders a second row below cwd/model stats; also clear any stale status.
    ctx.ui.setStatus('pi-agent', undefined);
  }
  async function poll() {
    if (polling || !client || client.closed || client.exited || !ctxNow) return;
    polling = true; const instance = client, ctx = ctxNow, gen = generation;
    try {
      const status = await instance.request('status'); if (!alive(ctx, gen) || client !== instance) return;
      render(ctx, status);
      // Pi defers the initial persistent JSONL write until an assistant exists.
      // A custom entry in getBranch() alone is not durable on the first mail turn.
      // Ephemeral sessions deliberately have no JSONL; child artifacts still persist.
      const historyReady = !ctx.sessionManager.getSessionFile?.() || ctx.sessionManager.getEntries().some(entry => entry.type === 'message' && entry.message.role === 'assistant');
      // sendMessage queues synchronously but persistence happens later. Do not
      // acknowledge merely queued mail: it must survive abort/reload before delivery.
      const persisted = new Set(ctx.sessionManager.getBranch().map(entry => persistedNotice(entry)?.details?.notificationId).filter(Boolean));
      for (const [notificationId, pending] of awaitingMail) {
        if (persisted.has(notificationId) && historyReady) {
          pi.appendEntry('pi-agent-delivered', { ids: pending.ids });
          for (const id of pending.ids) seen.add(id);
          seen = new Set([...seen].slice(-1024)); awaitingMail.delete(notificationId);
        } else if (!persisted.has(notificationId) && pending.requeueWhenIdle && ctx.isIdle()) {
          // Interactive abort/clearQueue can discard an active-run steer before
          // Pi persists it. Keep the durable inbox row and let the idle routing
          // below requeue it as nextTurn. Intentional nextTurn mail has no such
          // recovery flag and must remain pending without periodic duplicates.
          awaitingMail.delete(notificationId);
        }
      }
      const mail = await instance.request('inbox'); if (!alive(ctx, gen) || client !== instance) return;
      // Routine progress stays stored-only. Questions, blockers, explicit steer
      // messages and settled completions enter context without starting a turn.
      const storedOnly = m => (m.event !== 'completion' && !requiresMainAttention(m) || m.superseded === true) && Boolean(mailReference(m));
      const pendingIds = new Set([...awaitingMail.values()].flatMap(row => row.ids));
      const deliveredOrRead = m => seen.has(m.id) || readArtifacts.has(m.reportArtifactId ?? m.bodyArtifactId);
      const handledForDelivery = m => storedOnly(m) || pendingIds.has(m.id) || deliveredOrRead(m) || m.expiresAt < Date.now();
      const completionReady = mail.filter(m => !handledForDelivery(m) && m.event === 'completion' && m.completionReady !== false);
      const attentionReady = mail.filter(m => !handledForDelivery(m) && m.event !== 'completion' && requiresMainAttention(m));
      const queueNotice = (ready, options) => {
        if (!ready.length) return;
        const references = ready.map(mailReference);
        if (references.some(row => !row)) throw fail('Internal mail lacks a valid durable read reference; retained in inbox.');
        const id = randomUUID(), message = notification(references, { notificationId: id, runBinding });
        const pending = { id, ids: message.details.messageIds, ...(options.deliverAs === 'steer' ? { requeueWhenIdle: true } : {}) };
        if (!pending.ids.length) throw fail('Internal mail notification selected no valid references; retained in inbox.');
        awaitingMail.set(id, pending);
        try {
          pi.sendMessage(message, options);
        } catch (error) { awaitingMail.delete(id); throw error; }
      };
      if (pi.getActiveTools().includes('agent')) {
        // Completions are context-only and never extend a running loop. Important
        // mail steers an active loop so Pi consumes it at the next provider call;
        // while idle it waits for a user-started run and never wakes the model.
        queueNotice(completionReady, { triggerTurn: false, deliverAs: 'nextTurn' });
        queueNotice(attentionReady, ctx.isIdle() ? { triggerTurn: false, deliverAs: 'nextTurn' } : { deliverAs: 'steer' });
      }
      // Merely queuing nextTurn mail is not delivery evidence. Keep it durable
      // until the notice appears in session history (seen) or is explicitly read.
      const acknowledged = mail.filter(m => storedOnly(m) || m.expiresAt < Date.now() || (historyReady && deliveredOrRead(m))).map(m => m.id);
      if (acknowledged.length) await instance.request('ack', { ids: acknowledged });
      if (!alive(ctx, gen) || client !== instance) return;
      if (Date.now() - lastMonitor >= 2500) { lastMonitor = Date.now(); await instance.monitor(); }
    } catch (error) {
      if (alive(ctx, gen) && client === instance && error.code === 'MEMORY') {
        notify(ctx, safeError(error), 'warning');
        if (ctx.hasUI) { ctx.ui.setStatus('pi-agent', 'waiting: memory pressure'); ctx.ui.setWidget('pi-agent', ['pi-agent admission held by memory pressure. Original records retained.'], { placement: 'belowEditor' }); }
      } else if (alive(ctx, gen) && client === instance && !instance.closed) notify(ctx, `pi-agent: ${safeError(error)}`, 'warning');
    }
    finally { polling = false; }
  }
  function schedule() { if (timer) return; timer = setInterval(() => { void poll(); }, 1000); timer.unref?.(); }
  async function stop({ terminate = false } = {}) {
    clearInterval(timer); timer = undefined;
    const instance = client; client = undefined; awaitingMail = new Map();
    if (terminate) await closeProcessSessionHosts(instance);
    else await instance?.detach();
  }
  pi.on('session_start', async (_event, ctx) => {
    const gen = ++generation; ctxNow = undefined; receiptTail = Promise.resolve(); closeHub(); await stop();
    if (generation !== gen) return;
    let guardError;
    try { restoreMainGuard(ctx); } catch (error) { guardError = error; }
    guardStorageFailure = guardError?.code === 'STORAGE' ? guardError : undefined;
    ctxNow = ctx; sessionId = ctx.sessionManager.getSessionId(); runBinding = undefined; observedRoot = undefined; seen = new Set(); awaitingMail = new Map(); readArtifacts = new Set(); waitReceipts = new Map();
    const branch = ctx.sessionManager.getBranch(); accountedChildUsage = restoreChildUsage(branch);
    for (const entry of branch) {
      if (entry.type === 'custom' && entry.customType === 'pi-agent-delivered') for (const id of entry.data?.ids ?? []) seen.add(id);
      if (entry.type === 'custom' && entry.customType === 'pi-agent-read-artifacts') for (const id of entry.data?.ids ?? []) readArtifacts.add(id);
      if (entry.type === 'custom' && entry.customType === 'pi-agent-wait-receipts') for (const receipt of entry.data?.receipts ?? []) {
        if (storedResultReceipt(receipt)) waitReceipts.set(resultReceiptKey(receipt), receipt);
      }
      for (const id of persistedNotice(entry)?.details?.messageIds ?? []) seen.add(id);
      if (entry.type === 'custom' && entry.customType === 'pi-agent-run-binding' && typeof entry.data?.token === 'string') {
        runBinding = entry.data.token;
      }
    }
    seen = new Set([...seen].slice(-1024)); readArtifacts = new Set([...readArtifacts].slice(-1024)); waitReceipts = new Map([...waitReceipts].slice(-1024));
    if (guardError) notify(ctx, `pi-agent main tool guard: ${safeError(guardError)}`, 'warning');
    if (!runBinding) render(ctx, { tasks: [], active: 0, concurrency: 8 }); // empty widget without starting a host
  });
  pi.on('session_tree', async (_event, ctx) => {
    const gen = ++generation; ctxNow = undefined; receiptTail = Promise.resolve(); closeHub();
    const previousRunRoot = client?.options.root ?? observedRoot;
    observedRoot = undefined;
    await stop(); if (generation !== gen) return;
    let guardError;
    try { restoreMainGuard(ctx); } catch (error) { guardError = error; }
    guardStorageFailure = guardError?.code === 'STORAGE' ? guardError : undefined;
    ctxNow = ctx; sessionId = ctx.sessionManager.getSessionId();
    runBinding = randomUUID(); seen = new Set(); awaitingMail = new Map(); readArtifacts = new Set(); waitReceipts = new Map(); accountedChildUsage = new Map();
    pi.appendEntry('pi-agent-run-binding', { token: runBinding, previousRunRoot });
    if (guardError) notify(ctx, `pi-agent main tool guard: ${safeError(guardError)}`, 'warning');
    if (ctx.hasUI) { ctx.ui.setWidget('pi-agent', undefined); ctx.ui.setStatus('pi-agent', undefined); }
  });
  pi.on('session_shutdown', async event => {
    const gen = ++generation; const ctx = ctxNow; ctxNow = undefined; closeHub();
    // Reload/session replacement keeps durable children available for reconnect.
    // An explicit Pi quit owns the whole run and requests durable cancellation
    // plus executor/supervisor shutdown. Tool/wait aborts never enter this hook.
    await stop({ terminate: event.reason === 'quit' });
    if (generation === gen && ctx?.hasUI) { ctx.ui.setWidget('pi-agent', undefined); ctx.ui.setStatus('pi-agent', undefined); }
  });
  pi.on('context', (event, ctx) => {
    if (!alive(ctx, generation)) return;
    // Join newly persisted mail at its original boundary in a running SDK loop.
    // Raw branch replay would resurrect compacted notices; tail replay moves the
    // same unread notification on every request and breaks the cached prefix.
    const projected = ctx.sessionManager.buildSessionProjection?.().messages ?? [];
    const joined = restoreMailContext(event.messages, projected);
    const messages = compactMailContext(joined, { binding: runBinding });
    if (messages) return { messages };
  });
  pi.on('before_agent_start', async (event, ctx) => {
    const gen = generation;
    if (!alive(ctx, gen)) return;
    if (guardStorageFailure) return { systemPrompt: guardBlockerPrompt(event.systemPrompt, safeError(guardStorageFailure)) };
    let active;
    if (guardEnabled) {
      try { active = enforceMainGuard(event.systemPromptOptions?.selectedTools); }
      catch (error) { if (alive(ctx, gen)) return { systemPrompt: guardBlockerPrompt(event.systemPrompt, safeError(error)) }; return; }
      if (!active.includes('agent')) {
        if (event.systemPrompt.includes(MAIN_GUARD_BLOCKER_MARKER)) return;
        return { systemPrompt: guardBlockerPrompt(event.systemPrompt, 'the agent delegation tool is disabled') };
      }
    } else {
      try { active = pi.getActiveTools(); }
      catch (error) { if (alive(ctx, gen)) return { systemPrompt: `${event.systemPrompt}\n\npi-agent tool catalog unavailable; unrestricted main workflow remains active. ${short(safeError(error), 256)}` }; return; }
      if (!active.includes('agent')) return;
    }
    if (event.systemPrompt.includes(COORDINATOR_MARKER) || event.systemPrompt.includes(MAIN_GUARD_BLOCKER_MARKER)) return;
    try {
      // Keep the injected prefix independent of catalog contents and runtime
      // status. Models fetch role/profile details explicitly with agent catalog.
      const guidance = coordinatorPrompt(undefined, active, { coordinatorOnly: guardEnabled });
      if (guidance && alive(ctx, gen)) return { systemPrompt: `${event.systemPrompt}\n\n${guidance}` };
    } catch (e) {
      if (!alive(ctx, gen)) return;
      if (guardEnabled) return { systemPrompt: guardBlockerPrompt(event.systemPrompt, `pi-agent catalog unavailable: ${safeError(e)}`) };
      return { systemPrompt: `${event.systemPrompt}\n\npi-agent catalog unavailable; delegation is unavailable for this turn while unrestricted mode remains user-controlled. ${short(safeError(e), 256)}` };
    }
  });
  pi.registerCommand('agent', { description: '메인 작업 모드 선택 (기본: 직접 작업, 선택: 코디네이터 제한)',
    handler: async (args, ctx) => {
      if (args.trim()) { if (ctx.hasUI) ctx.ui.notify('사용법: /agent', 'warning'); return; }
      if (!ctx.hasUI) { ctx.ui.notify('/agent는 선택 UI가 있는 모드에서만 사용할 수 있습니다. 제한 상태는 바뀌지 않았습니다.', 'warning'); return; }
      const gen = generation;
      if (!alive(ctx, gen)) return;
      try {
        await ctx.waitForIdle();
        if (!alive(ctx, gen)) return;
        const state = guardEnabled ? '켜짐' : '꺼짐';
        const recovery = guardStorageFailure ? '\n저장된 가드가 호환되지 않습니다. 선택하면 현재 활성 도구만 기준으로 새 상태를 저장하며, 기존 기록의 도구는 복원하지 않습니다.' : '';
        const choice = await ctx.ui.select(`메인 도구 제한: ${state}${recovery}\n제한 켜기: agent/read/grep/lifecycle과 읽기 전용 playbook을 사용합니다.\n제한 끄기: 현재 선택된 도구로 직접 작업합니다. 비활성 도구는 추가하지 않습니다.`, ['제한 켜기', '제한 끄기']);
        if (!choice || !alive(ctx, gen)) return;
        if (guardStorageFailure && ['제한 켜기', '제한 끄기'].includes(choice)) recoverMainGuard(choice === '제한 켜기');
        else if (choice === '제한 켜기') enableMainGuard();
        else if (choice === '제한 끄기') disableMainGuard();
        else return;
        if (alive(ctx, gen)) ctx.ui.notify(`메인 도구 제한이 ${guardEnabled ? '켜졌습니다' : '꺼졌습니다'}.`, 'info');
      } catch (error) { if (alive(ctx, gen)) ctx.ui.notify(`메인 도구 제한 변경 실패: ${safeError(error)}`, 'error'); }
    },
  });
  pi.registerCommand('hub', { description: 'Read-only agent hub: roster, detail, reported usage and bounded transcript tail',
    handler: async (_args, ctx) => {
      if (ctx.mode !== 'tui') { notify(ctx, '/hub requires interactive TUI mode; no host was started.'); return; }
      const gen = generation;
      if (!alive(ctx, gen)) return;
      closeHub();
      let component;
      try {
        await ctx.ui.custom((tui, theme, _keys, done) => {
          component = new Hub({ theme, done, requestRender: () => tui.requestRender(),
            height: () => tui.terminal.rows || 24, isAlive: () => alive(ctx, gen),
            read: async selectedId => {
              const instance = client;
              let root = observedRoot;
              if (!root && runBinding) root = runRoot(await realpath(agentDirectory()), await realpath(ctx.cwd), `${sessionId}:${runBinding}`);
              if (!alive(ctx, gen) || client !== instance) throw fail('Observation superseded');
              const snapshot = await observe({ client: instance, root }, selectedId);
              if (!alive(ctx, gen) || client !== instance) throw fail('Observation superseded');
              return snapshot;
            },
          });
          hub = component; component.start(); return component;
        }, { overlay: true, overlayOptions: { width: '100%', maxHeight: '100%', row: 0, col: 0, margin: 0 } });
      } finally { component?.dispose(); if (hub === component) hub = undefined; }
    },
  });
  async function executeAction(input, signal, ctx) {
    signal?.throwIfAborted();
    const params = validatePublicAgentParameters(input), gen = generation;
    if (guardStorageFailure) throw fail('Incompatible saved main guard; use /agent to explicitly select a new guard state without restoring incompatible tool names', 'STORAGE');
    if (!alive(ctx, gen)) throw fail('Main session is not active');
    if (params.action === 'read') {
      let root = client?.options.root ?? observedRoot;
      if (!root && runBinding) root = runRoot(await realpath(agentDirectory()), await realpath(ctx.cwd), `${sessionId}:${runBinding}`);
      if (!alive(ctx, gen)) throw fail('Session changed before response read');
      const result = await publicRead(root, params.requests, { signal });
      signal?.throwIfAborted();
      if (!alive(ctx, gen)) throw fail('Session changed during response read');
      const pages = result.results.filter(row => row.ok);
      rememberReadArtifacts(pages.filter(page => ['result', 'error', 'handoff', 'message'].includes(page.source)).map(page => page.artifactId));
      rememberWaitPages(pages);
      return result;
    }
    if (params.action === 'catalog') {
      const result = await catalog(ctx); signal?.throwIfAborted();
      if (!alive(ctx, gen)) throw fail('Session changed during catalog read');
      return result;
    }
    if (params.action === 'spawn') await preflightSpawn(ctx, { tasks: planSpawnBatch(params) });
    const host = await ensure(ctx); signal?.throwIfAborted();
    if (!alive(ctx, gen) || client !== host) throw fail('Session or host changed before operation');
    const result = await publicOperation(host, params, { signal, readArtifacts,
      mainModel: ctx.model ? { provider: ctx.model.provider, model: ctx.model.id, thinkingLevel: ctx.thinkingLevel } : undefined,
      receiveWait: (rows, options) => receiveWait(ctx, gen, host, rows, options),
    });
    if (!alive(ctx, gen)) throw fail('Main session changed; inspect the original run before retrying');
    return result;
  }
  pi.registerTool({ name: 'agent', label: 'agent', parameters: PublicAgentParameters,
    executionMode: 'sequential',
    renderCall: renderAgentCall, renderResult: renderAgentResult,
    description: `Run and supervise configured child agents.
catalog lists roles and profiles on demand. spawn starts 1–8 tasks and preserves configured role/profile pins; dependencies may reference existing agents or earlier tasks in the same batch. status inspects agents. wait blocks until settlement or an unread question/blocker; aborting a wait does not cancel children. send delivers follow-up or turn-boundary steer. cancel requires a later status/wait check. resume continues a settled agent with a new instruction. archive retains records. read retrieves bounded durable results, messages, transcripts and result files.
Keep agentIds and durable read references. accepted is not completed; settled is not success. Inspect unknown outcomes before retrying. Completion notices do not start a main turn.`,
    promptSnippet: 'Use child agents for bounded parallel or specialized work when useful; inspect and settle every child you start.',
    async execute(_id, params, signal, _onUpdate, ctx) {
      const result = await executeAction(params, signal, ctx);
      // Serialize model-facing output before advancing the in-memory ledger. If
      // encoding fails, a later successful observation must still report usage.
      return Object.assign(textResult(result), accountChildUsage(childUsageRows(result), accountedChildUsage));
    },
  });
}
