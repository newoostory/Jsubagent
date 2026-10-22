import { LIMITS, encodeJson, fail, hash, safeError, short } from '../limits.mjs';

const FAULT_KINDS = new Set([
  'rate_limit', 'quota_window', 'quota_unknown', 'billing_required', 'auth_required',
  'provider_overload', 'transport_interruption', 'context_full', 'output_limit', 'local_execution_limit',
  'disk_full', 'integrity_error',
]);
const OUTCOMES = new Set(['rejected_before_execution', 'completed', 'unknown']);
const SOURCES = new Set(['typed_metadata', 'adapter_headers', 'message_pattern', 'sdk_diagnostic', 'unknown']);
const RESET_SEMANTICS = new Set(['minimum', 'fixed_window', 'unknown']);
const FAULT_KEYS = new Set([
  'provider', 'credentialProfileId', 'quotaScopeIds', 'modelFamily', 'kind', 'providerCode',
  'httpStatus', 'requestId', 'requestOutcome', 'retryNotBefore', 'resetAt', 'resetSemantics',
  'source', 'observedAt', 'safeMessage', 'usageRef', 'diagnosticType', 'transportPhase',
]);
const USAGE_KEYS = ['input', 'output', 'cacheRead', 'cacheWrite', 'cost'];
const USAGE_STATES = new Set(['observed', 'partial', 'estimated', 'missing', 'unknown']);
const USAGE_STATE_KEYS = new Set(['state', ...USAGE_KEYS, 'observedAt']);
const REF_KEYS = new Set(['artifactId', 'bytes', 'sha256', 'complete']);
const SAFE_ID = /^[a-zA-Z0-9][a-zA-Z0-9_.:/-]{0,255}$/;
const SAFE_CODE = /^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$/;
const CAPTURE_FETCH_APIS = new Set([
  'openai-completions', 'openai-responses', 'openai-codex-responses',
  'azure-openai-responses', 'anthropic-messages', 'pi-messages',
]);

function plain(value) {
  return Boolean(value && typeof value === 'object' && !Array.isArray(value) && (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null));
}

function exact(value, keys, name) {
  if (!plain(value)) throw fail(`Invalid ${name}`, 'PROTOCOL');
  for (const key of Object.keys(value)) if (!keys.has(key)) throw fail(`Unsupported ${name} field: ${key}`, 'PROTOCOL');
  return value;
}

function safeId(value, name, maximum = 256) {
  if (typeof value !== 'string' || !value || Buffer.byteLength(value) > maximum || !SAFE_ID.test(value)) throw fail(`Invalid ${name}`, 'PROTOCOL');
  return value;
}

function optionalCode(value, name = 'provider code') {
  if (value === undefined) return undefined;
  if (typeof value !== 'string' || Buffer.byteLength(value) > 128 || !SAFE_CODE.test(value)) throw fail(`Invalid ${name}`, 'PROTOCOL');
  return value;
}

function timestamp(value, name) {
  if (!Number.isSafeInteger(value) || value < 0) throw fail(`Invalid ${name}`, 'PROTOCOL');
  return value;
}

function optionalTimestamp(value) {
  return Number.isSafeInteger(value) && value >= 0 ? value : undefined;
}

function usageReference(value) {
  if (value === undefined) return undefined;
  exact(value, REF_KEYS, 'usage reference');
  safeId(value.artifactId, 'usage artifactId', 80);
  if (!Number.isSafeInteger(value.bytes) || value.bytes < 0 || typeof value.sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(value.sha256) || value.complete !== true) throw fail('Invalid usage reference', 'PROTOCOL');
  return { artifactId: value.artifactId, bytes: value.bytes, sha256: value.sha256, complete: true };
}

export function assertProviderFault(value) {
  exact(value, FAULT_KEYS, 'provider fault');
  safeId(value.provider, 'provider', 120);
  safeId(value.credentialProfileId, 'credential profile', 120);
  if (!Array.isArray(value.quotaScopeIds) || value.quotaScopeIds.length < 1 || value.quotaScopeIds.length > 8 || new Set(value.quotaScopeIds).size !== value.quotaScopeIds.length) throw fail('Invalid quota scope IDs', 'PROTOCOL');
  value.quotaScopeIds.forEach(scope => safeId(scope, 'quota scope', 256));
  if (value.modelFamily !== undefined) safeId(value.modelFamily, 'model family', 256);
  if (!FAULT_KINDS.has(value.kind)) throw fail('Invalid provider fault kind', 'PROTOCOL');
  optionalCode(value.providerCode);
  if (value.httpStatus !== undefined && (!Number.isSafeInteger(value.httpStatus) || value.httpStatus < 100 || value.httpStatus > 599)) throw fail('Invalid provider HTTP status', 'PROTOCOL');
  if (value.requestId !== undefined) safeId(value.requestId, 'provider request ID', 160);
  if (!OUTCOMES.has(value.requestOutcome)) throw fail('Invalid provider request outcome', 'PROTOCOL');
  if (value.retryNotBefore !== undefined) timestamp(value.retryNotBefore, 'retryNotBefore');
  if (value.resetAt !== undefined) timestamp(value.resetAt, 'resetAt');
  if (value.resetSemantics !== undefined && !RESET_SEMANTICS.has(value.resetSemantics)) throw fail('Invalid reset semantics', 'PROTOCOL');
  if (!SOURCES.has(value.source)) throw fail('Invalid provider fault source', 'PROTOCOL');
  timestamp(value.observedAt, 'provider fault observedAt');
  if (typeof value.safeMessage !== 'string' || !value.safeMessage || Buffer.byteLength(value.safeMessage) > 512 || /[\r\n]/.test(value.safeMessage)) throw fail('Invalid safe provider message', 'PROTOCOL');
  if (value.diagnosticType !== undefined) optionalCode(value.diagnosticType, 'provider diagnostic type');
  if (value.transportPhase !== undefined) optionalCode(value.transportPhase, 'provider transport phase');
  if (value.kind === 'transport_interruption') {
    if (value.source !== 'sdk_diagnostic' || value.requestOutcome !== 'unknown' || value.diagnosticType !== 'provider_transport_failure' || value.transportPhase !== 'after_message_stream_start') throw fail('Invalid provider transport diagnostic fault', 'PROTOCOL');
  } else if (value.diagnosticType !== undefined || value.transportPhase !== undefined) throw fail('Unexpected transport diagnostic metadata', 'PROTOCOL');
  usageReference(value.usageRef);
  encodeJson(value, 4096);
  return JSON.parse(JSON.stringify(value));
}

export function emptyUsageState(observedAt = Date.now()) {
  return { state: 'missing', input: null, output: null, cacheRead: null, cacheWrite: null, cost: null, observedAt };
}

export function assertUsageState(value) {
  exact(value, USAGE_STATE_KEYS, 'usage state');
  if (!USAGE_STATES.has(value.state)) throw fail('Invalid usage observation state', 'PROTOCOL');
  for (const key of USAGE_KEYS) if (value[key] !== null && (!Number.isFinite(value[key]) || value[key] < 0)) throw fail(`Invalid usage ${key}`, 'PROTOCOL');
  timestamp(value.observedAt, 'usage observedAt');
  encodeJson(value, 1024);
  return { ...value };
}

export function accumulateUsageState(current, value, observedAt = Date.now()) {
  const next = current ? assertUsageState(current) : emptyUsageState(observedAt), unknown = next.state === 'unknown';
  let present = 0;
  for (const key of USAGE_KEYS) {
    const raw = key === 'cost' && plain(value?.cost) ? value.cost.total : value?.[key];
    if (Number.isFinite(raw) && raw >= 0) {
      next[key] = (next[key] ?? 0) + raw;
      present++;
    }
  }
  if (unknown) next.state = 'unknown';
  else if (present === USAGE_KEYS.length) next.state = 'observed';
  else if (present > 0) next.state = next.state === 'estimated' ? 'estimated' : 'partial';
  next.observedAt = observedAt;
  return assertUsageState(next);
}

export function usageStateFrom(value, { outcome = 'completed', observedAt = Date.now() } = {}) {
  let state = accumulateUsageState(emptyUsageState(observedAt), value, observedAt);
  if (outcome === 'unknown') state.state = 'unknown';
  else if (outcome === 'rejected_before_execution' && USAGE_KEYS.every(key => state[key] === 0 || state[key] === null)) state = emptyUsageState(observedAt);
  return assertUsageState(state);
}

function headerGetter(headers) {
  if (headers instanceof Headers) return name => headers.get(name);
  if (plain(headers)) {
    const entries = new Map(Object.entries(headers).map(([key, value]) => [key.toLowerCase(), String(value)]));
    return name => entries.get(name.toLowerCase()) ?? null;
  }
  return () => null;
}

function boundedHeader(get, name) {
  const value = get(name);
  return typeof value === 'string' && value.length <= 256 && Buffer.byteLength(value) <= 512 ? value.trim() : undefined;
}

function parseDelay(value, now, milliseconds = false) {
  if (!value) return undefined;
  const numeric = Number(value);
  if (Number.isFinite(numeric) && numeric >= 0) {
    const delay = numeric * (milliseconds ? 1 : 1000);
    return Number.isSafeInteger(delay) && Number.isSafeInteger(now + delay) ? now + delay : undefined;
  }
  if (milliseconds) return undefined;
  const parsed = Date.parse(value);
  return optionalTimestamp(parsed);
}

function parseReset(value, now) {
  if (!value) return undefined;
  const duration = /^(\d+(?:\.\d+)?)(ms|s|m|h)$/i.exec(value);
  if (duration) {
    const scale = { ms: 1, s: 1000, m: 60_000, h: 3_600_000 }[duration[2].toLowerCase()];
    const delay = Number(duration[1]) * scale;
    return Number.isSafeInteger(delay) && Number.isSafeInteger(now + delay) ? now + delay : undefined;
  }
  const numeric = Number(value);
  if (Number.isFinite(numeric) && numeric >= 0) {
    const absolute = numeric < 10_000_000_000 ? numeric * 1000 : numeric;
    return optionalTimestamp(absolute);
  }
  return optionalTimestamp(Date.parse(value));
}

export function providerResponseObservation(response, now = Date.now(), { api } = {}) {
  const status = response?.status;
  const get = headerGetter(response?.headers);
  const retryMs = boundedHeader(get, 'retry-after-ms');
  const retry = boundedHeader(get, 'retry-after');
  // Reset headers are API-specific and only constrain an exhausted bucket.
  // In particular API-key reset names are not an OAuth/Codex contract.
  const pairs = ['openai-completions', 'openai-responses', 'azure-openai-responses'].includes(api)
    ? [['x-ratelimit-remaining-requests', 'x-ratelimit-reset-requests'], ['x-ratelimit-remaining-tokens', 'x-ratelimit-reset-tokens']]
    : api === 'anthropic-messages' ? [['anthropic-ratelimit-requests-remaining', 'anthropic-ratelimit-requests-reset'], ['anthropic-ratelimit-tokens-remaining', 'anthropic-ratelimit-tokens-reset']] : [];
  const resetValues = pairs.filter(([remaining]) => { const value = boundedHeader(get, remaining); return value !== undefined && /^0(?:\.0+)?$/.test(value); }).map(([, reset]) => boundedHeader(get, reset)).filter(Boolean);
  const requestId = ['x-request-id', 'request-id', 'anthropic-request-id', 'cf-ray']
    .map(name => boundedHeader(get, name)).find(value => value && SAFE_ID.test(value));
  let retryNotBefore = parseDelay(retryMs, now, true) ?? parseDelay(retry, now, false);
  if (!retryMs && retry && !Number.isFinite(Number(retry))) {
    const serverDate = Date.parse(boundedHeader(get, 'date') ?? ''), retryDate = Date.parse(retry);
    if (Number.isSafeInteger(serverDate) && Number.isSafeInteger(retryDate) && retryDate >= serverDate && Number.isSafeInteger(now + retryDate - serverDate)) retryNotBefore = Math.max(retryNotBefore ?? 0, now + retryDate - serverDate);
  }
  const resetTimes = resetValues.map(value => parseReset(value, now)).filter(value => value !== undefined);
  const resetAt = resetTimes.length ? Math.max(...resetTimes) : undefined;
  const invalidTiming = Boolean((retryMs || retry) && retryNotBefore === undefined || resetValues.length && !resetTimes.length);
  return {
    ...(Number.isSafeInteger(status) && status >= 100 && status <= 599 ? { httpStatus: status } : {}),
    ...(requestId ? { requestId } : {}),
    ...(retryNotBefore === undefined ? {} : { retryNotBefore }),
    ...(resetAt === undefined ? {} : { resetAt }),
    ...((retryNotBefore !== undefined || resetAt !== undefined) ? { resetSemantics: 'minimum' } : {}),
    ...(invalidTiming ? { invalidTiming: true } : {}),
    observedAt: now,
  };
}

export function shouldCaptureProviderFetch(model) {
  return CAPTURE_FETCH_APIS.has(model?.api);
}

const KIND_MESSAGES = Object.freeze({
  rate_limit: 'Provider rate limit reached; the selected credential and model remain pinned.',
  quota_window: 'Provider quota window is exhausted; explicit resume is required after the verified minimum eligibility time.',
  quota_unknown: 'Provider quota is exhausted with no verified reset; explicit recheck and resume are required.',
  billing_required: 'Provider billing or credit is required; explicit recheck and resume are required.',
  auth_required: 'Provider authentication is required; explicit recheck and resume are required.',
  provider_overload: 'Provider is temporarily overloaded; the selected credential and model remain pinned.',
  transport_interruption: 'Provider transport ended after the model request started; request usage is unknown.',
  context_full: 'Provider context capacity is full; retained context requires explicit handling.',
  output_limit: 'Provider output limit was reached; retained output references remain available.',
  local_execution_limit: 'Local execution safety limit was reached.',
  disk_full: 'Durable storage capacity is unavailable.',
  integrity_error: 'Provider or runtime integrity validation failed.',
});

function knownCode(error) {
  const value = String(error?.providerCode ?? error?.code ?? '').trim();
  return SAFE_CODE.test(value) ? value : undefined;
}

function classify(error, observation) {
  const code = knownCode(error), lowerCode = code?.toLowerCase(), message = safeError(error), lower = message.toLowerCase();
  const status = observation?.httpStatus ?? (Number.isSafeInteger(error?.httpStatus) ? error.httpStatus : Number.isSafeInteger(error?.status) ? error.status : undefined);
  const typed = values => values.includes(lowerCode);
  if (typed(['context_length_exceeded', 'context_full']) || /context (?:length|window).*(?:exceed|full)|maximum context length/i.test(message) || status === 413) return ['context_full', typed(['context_length_exceeded', 'context_full']) ? 'typed_metadata' : status === 413 ? 'adapter_headers' : 'message_pattern'];
  if (typed(['output_limit', 'max_tokens']) || /output (?:token )?limit|maximum output|finish_reason.?length/i.test(message)) return ['output_limit', typed(['output_limit', 'max_tokens']) ? 'typed_metadata' : 'message_pattern'];
  if (typed(['disk_full', 'enospc']) || /no space left on device|disk (?:quota|full)/i.test(message)) return ['disk_full', typed(['disk_full', 'enospc']) ? 'typed_metadata' : 'message_pattern'];
  if (typed(['integrity_error', 'storage_schema', 'protocol']) || /integrity (?:error|check)|checksum mismatch/i.test(message)) return ['integrity_error', typed(['integrity_error', 'storage_schema', 'protocol']) ? 'typed_metadata' : 'message_pattern'];
  if (typed(['limit', 'root_budget', 'memory'])) return ['local_execution_limit', 'typed_metadata'];
  const quotaCode = typed(['insufficient_quota', 'quota_exceeded', 'usage_limit_reached']);
  if (quotaCode || /insufficient[_ ]quota|quota exceeded|usage limit reached|monthly usage limit|weekly usage limit|freeusagelimiterror|gousagelimiterror/i.test(message)) {
    const exactWindow = !observation?.invalidTiming && observation?.resetAt !== undefined;
    return [exactWindow ? 'quota_window' : 'quota_unknown', quotaCode ? 'typed_metadata' : 'message_pattern'];
  }
  const billingCode = typed(['billing_required', 'billing_hard_limit_reached', 'payment_required']);
  if (billingCode || /billing (?:is )?(?:required|inactive|not active)|payment required|credit balance.{0,40}(?:low|exhaust)|out of budget/i.test(message) || status === 402) return ['billing_required', billingCode ? 'typed_metadata' : status === 402 ? 'adapter_headers' : 'message_pattern'];
  const authCode = typed(['auth', 'unauthorized', 'invalid_api_key', 'authentication_error']);
  if (authCode || /authentication unavailable|invalid api key|unauthorized|credential.{0,40}expired|use .*login/i.test(message) || status === 401) return ['auth_required', authCode ? 'typed_metadata' : status === 401 ? 'adapter_headers' : 'message_pattern'];
  const rateCode = typed(['rate_limit', 'too_many_requests', 'rate_limit_exceeded']);
  if (rateCode || /\b429\b|rate[ -]?limit|too many requests|temporarily throttled/i.test(message) || status === 429) return ['rate_limit', rateCode ? 'typed_metadata' : status === 429 ? 'adapter_headers' : 'message_pattern'];
  const overloadCode = typed(['overloaded', 'service_unavailable']);
  if (overloadCode || /overloaded|high demand|service unavailable|server (?:is )?busy/i.test(message) || Number.isSafeInteger(status) && status >= 500) return ['provider_overload', overloadCode ? 'typed_metadata' : Number.isSafeInteger(status) ? 'adapter_headers' : 'message_pattern'];
  if (lower.includes('local execution')) return ['local_execution_limit', 'message_pattern'];
  return null;
}

function profileFields(profile = {}) {
  const provider = typeof profile.provider === 'string' && SAFE_ID.test(profile.provider) ? profile.provider : 'unknown-provider';
  const credentialProfileId = typeof profile.name === 'string' && SAFE_ID.test(profile.name)
    ? profile.name : typeof profile.account === 'string' && SAFE_ID.test(profile.account) ? profile.account : provider;
  const account = typeof profile.account === 'string' ? profile.account : credentialProfileId;
  const quotaScopeIds = [`credential:${credentialProfileId}`, `account:${hash(account).slice(0, 24)}`];
  const modelFamily = typeof profile.model === 'string' && SAFE_ID.test(profile.model) ? profile.model : undefined;
  return { provider, credentialProfileId, quotaScopeIds, ...(modelFamily ? { modelFamily } : {}) };
}

function trustedTransportDiagnostic(message) {
  if (!plain(message) || message.role !== 'assistant' || message.api !== 'openai-codex-responses' || message.stopReason !== 'error' || !Array.isArray(message.diagnostics)) return null;
  const matches = message.diagnostics.filter(row => plain(row) && row.type === 'provider_transport_failure');
  if (matches.length !== 1) return null;
  const diagnostic = matches[0], details = diagnostic.details, error = diagnostic.error;
  if (!Number.isSafeInteger(diagnostic.timestamp) || diagnostic.timestamp < 0 || !plain(details) || !plain(error)) return null;
  if (Object.keys(details).some(key => !['configuredTransport', 'fallbackTransport', 'eventsEmitted', 'phase', 'requestBytes'].includes(key))) return null;
  if (!['auto', 'websocket', 'websocket-cached'].includes(details.configuredTransport) || details.fallbackTransport !== undefined || details.eventsEmitted !== true || details.phase !== 'after_message_stream_start' || !Number.isSafeInteger(details.requestBytes) || details.requestBytes < 1) return null;
  if (typeof error.message !== 'string' || !error.message || Buffer.byteLength(error.message) > 4096) return null;
  return diagnostic;
}

export function providerTransportFailureDecision(message, { profile = {} } = {}) {
  const diagnostic = trustedTransportDiagnostic(message);
  if (!diagnostic) return null;
  const fault = assertProviderFault({
    ...profileFields(profile), kind: 'transport_interruption', requestOutcome: 'unknown',
    source: 'sdk_diagnostic', observedAt: diagnostic.timestamp,
    safeMessage: KIND_MESSAGES.transport_interruption, diagnosticType: diagnostic.type, transportPhase: diagnostic.details.phase,
  });
  return { fault, kind: fault.kind, reason: fault.safeMessage, nextEligibleAt: null };
}

export const OVERLOAD_COOLDOWN_MS = 5 * 60_000;

export function overloadCooldownUntil(fault, since = fault?.observedAt, retryAt = 0) {
  if (fault?.kind !== 'provider_overload' || !Number.isSafeInteger(since) || since < 0) return null;
  const minimum = since + OVERLOAD_COOLDOWN_MS;
  if (!Number.isSafeInteger(minimum)) return null;
  return Math.max(minimum, fault.retryNotBefore ?? 0, fault.resetAt ?? 0, retryAt ?? 0);
}

export function providerFaultNextEligible(fault, { jitterMs = 0 } = {}) {
  assertProviderFault(fault);
  if (!Number.isSafeInteger(jitterMs) || jitterMs < 0) throw fail('Invalid provider retry jitter', 'PROTOCOL');
  const candidates = [fault.retryNotBefore, fault.resetAt].filter(Number.isSafeInteger);
  const base = fault.kind === 'provider_overload' ? overloadCooldownUntil(fault) : candidates.length ? Math.max(...candidates) : null;
  if (base === null) return null;
  return Number.isSafeInteger(base + jitterMs) ? base + jitterMs : null;
}

export function providerFailureDecision(error, { attempts = 1, now = Date.now(), limits = LIMITS, profile = {}, observation, providerBoundary = false, jitterMs = 0 } = {}) {
  if (error?.providerFault !== undefined) {
    const fault = assertProviderFault(error.providerFault);
    return { fault, kind: fault.kind, reason: fault.safeMessage, nextEligibleAt: providerFaultNextEligible(fault, { jitterMs }) };
  }
  const trustedCode = ['AUTH', 'UNAUTHORIZED', 'INVALID_API_KEY', 'AUTHENTICATION_ERROR', 'RATE_LIMIT', 'TOO_MANY_REQUESTS', 'RATE_LIMIT_EXCEEDED', 'INSUFFICIENT_QUOTA', 'QUOTA_EXCEEDED', 'USAGE_LIMIT_REACHED', 'BILLING_REQUIRED', 'PAYMENT_REQUIRED', 'OVERLOADED', 'SERVICE_UNAVAILABLE', 'PROVIDER', 'CONTEXT', 'CONTEXT_FULL', 'OUTPUT_LIMIT', 'LIMIT', 'ROOT_BUDGET', 'MEMORY', 'DISK_FULL', 'ENOSPC', 'INTEGRITY_ERROR'].includes(String(error?.code ?? '').toUpperCase());
  if (!providerBoundary && !trustedCode) return null; // Tool/web text is never a provider fault classifier input.
  const observed = observation ?? {};
  const classified = classify(error, observed);
  if (!classified) return null;
  let [kind, source] = classified;
  const invalidDirectTiming = (error?.retryNotBefore !== undefined && optionalTimestamp(error.retryNotBefore) === undefined)
    || (error?.resetAt !== undefined && optionalTimestamp(error.resetAt) === undefined)
    || (error?.retryAfterMs !== undefined && (!Number.isSafeInteger(error.retryAfterMs) || error.retryAfterMs < 0))
    || observed.invalidTiming;
  let retryNotBefore = observed.retryNotBefore ?? optionalTimestamp(error?.retryNotBefore);
  let resetAt = observed.resetAt ?? optionalTimestamp(error?.resetAt);
  if (retryNotBefore === undefined && Number.isSafeInteger(error?.retryAfterMs) && error.retryAfterMs >= 0 && Number.isSafeInteger(now + error.retryAfterMs)) retryNotBefore = now + error.retryAfterMs;
  if (invalidDirectTiming && kind === 'quota_window') kind = 'quota_unknown';
  if (invalidDirectTiming) { retryNotBefore = undefined; resetAt = undefined; }
  if (kind === 'provider_overload') {
    const minimum = now + OVERLOAD_COOLDOWN_MS;
    retryNotBefore = Number.isSafeInteger(minimum) ? Math.max(minimum, retryNotBefore ?? 0) : undefined;
  } else if (!invalidDirectTiming && kind === 'rate_limit' && retryNotBefore === undefined && resetAt === undefined) {
    const exponent = limits.rateLimitBaseMs * (2 ** Math.max(0, attempts - 1));
    const fallback = Math.min(limits.rateLimitMaxMs, Number.isSafeInteger(exponent) ? exponent : limits.rateLimitMaxMs);
    retryNotBefore = Number.isSafeInteger(now + fallback) ? now + fallback : undefined;
  }
  const httpStatus = observed.httpStatus ?? (Number.isSafeInteger(error?.httpStatus) ? error.httpStatus : Number.isSafeInteger(error?.status) ? error.status : undefined);
  const rawCode = knownCode(error), genericCodes = new Set(['PROVIDER', 'AUTH', 'RATE_LIMIT', 'CONTEXT', 'LIMIT', 'ROOT_BUDGET', 'MEMORY', 'DISK_FULL', 'INTEGRITY_ERROR']);
  const providerCode = error?.providerCode !== undefined || providerBoundary && rawCode && !genericCodes.has(rawCode.toUpperCase()) ? rawCode : undefined;
  const requestId = observed.requestId ?? (typeof error?.requestId === 'string' && SAFE_ID.test(error.requestId) ? error.requestId : undefined);
  const rejectionKinds = new Set(['rate_limit', 'quota_window', 'quota_unknown', 'billing_required', 'auth_required', 'context_full']);
  const requestOutcome = OUTCOMES.has(error?.requestOutcome) ? error.requestOutcome
    : kind === 'provider_overload' ? 'unknown' : rejectionKinds.has(kind) && (httpStatus === undefined || httpStatus >= 400) ? 'rejected_before_execution' : 'unknown';
  const fields = profileFields(profile);
  const fault = assertProviderFault({
    ...fields, kind,
    ...(providerCode ? { providerCode } : {}),
    ...(httpStatus === undefined ? {} : { httpStatus }),
    ...(requestId ? { requestId } : {}),
    requestOutcome,
    ...(retryNotBefore === undefined ? {} : { retryNotBefore }),
    ...(resetAt === undefined ? {} : { resetAt }),
    ...((retryNotBefore !== undefined || resetAt !== undefined) ? { resetSemantics: observed.resetSemantics ?? 'minimum' } : {}),
    source: source ?? 'unknown', observedAt: optionalTimestamp(observed.observedAt) ?? now,
    safeMessage: KIND_MESSAGES[kind],
    ...(error?.usageRef ? { usageRef: usageReference(error.usageRef) } : {}),
  });
  return { fault, kind, reason: fault.safeMessage, nextEligibleAt: providerFaultNextEligible(fault, { jitterMs }) };
}

export function providerErrorIpcFields(error) {
  const fields = {};
  if (error?.providerFault !== undefined) fields.providerFault = assertProviderFault(error.providerFault);
  if (error?.requestOutcome !== undefined) {
    if (!OUTCOMES.has(error.requestOutcome)) throw fail('Invalid provider request outcome', 'PROTOCOL');
    fields.requestOutcome = error.requestOutcome;
  }
  if (error?.usageState !== undefined) fields.usageState = assertUsageState(error.usageState);
  encodeJson(fields, 8192);
  return fields;
}

export function applyProviderErrorIpcFields(error, fields) {
  if (fields?.providerFault !== undefined) error.providerFault = assertProviderFault(fields.providerFault);
  if (fields?.requestOutcome !== undefined) {
    if (!OUTCOMES.has(fields.requestOutcome)) throw fail('Invalid provider request outcome', 'PROTOCOL');
    error.requestOutcome = fields.requestOutcome;
  }
  if (fields?.usageState !== undefined) error.usageState = assertUsageState(fields.usageState);
  return error;
}

export function safeProviderMessage(error) {
  return short(safeError(error), 512).replace(/[\r\n]+/g, ' ');
}
