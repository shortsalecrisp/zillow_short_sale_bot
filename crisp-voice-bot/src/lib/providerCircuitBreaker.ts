const D17_SIGNATURE = "sip_403_account_disabled_d17";
const QUOTA_SIGNATURE = "elevenlabs_quota_exceeded";
const LLM_FAILURE_SIGNATURE = "elevenlabs_llm_failure";
const D17_WINDOW_MS = 10 * 60_000;
const D17_TRIP_THRESHOLD = 2;
const ALERT_RETRY_MS = 10 * 60_000;

export type ProviderCircuitFailureEvidence = {
  conversationId: string;
  rowNumber: number;
  callAttemptNumber: number;
  occurredAt: string;
  reason: string;
};

export type ProviderCircuitStatus = {
  open: boolean;
  signature: string;
  consecutiveFailures: number;
  threshold: number;
  windowMinutes: number;
  firstFailureAt?: string;
  lastFailureAt?: string;
  openedAt?: string;
  alertSentAt?: string;
  resetAt?: string;
  resetReason?: string;
  evidence: ProviderCircuitFailureEvidence[];
};

type MutableProviderCircuitState = ProviderCircuitStatus & {
  lastAlertAttemptAt?: string;
};

const state: MutableProviderCircuitState = {
  open: false,
  signature: D17_SIGNATURE,
  consecutiveFailures: 0,
  threshold: D17_TRIP_THRESHOLD,
  windowMinutes: D17_WINDOW_MS / 60_000,
  evidence: [],
};

function iso(now: Date): string {
  return now.toISOString();
}

function validTime(value?: string): number | undefined {
  if (!value) return undefined;
  const parsed = new Date(value).getTime();
  return Number.isNaN(parsed) ? undefined : parsed;
}

export function getProviderCircuitStatus(): ProviderCircuitStatus {
  return {
    open: state.open,
    signature: state.signature,
    consecutiveFailures: state.consecutiveFailures,
    threshold: state.threshold,
    windowMinutes: state.windowMinutes,
    firstFailureAt: state.firstFailureAt,
    lastFailureAt: state.lastFailureAt,
    openedAt: state.openedAt,
    alertSentAt: state.alertSentAt,
    resetAt: state.resetAt,
    resetReason: state.resetReason,
    evidence: state.evidence.map((item) => ({ ...item })),
  };
}

export function recordTelnyxD17Failure(
  evidence: Omit<ProviderCircuitFailureEvidence, "occurredAt"> & { occurredAt?: string },
  now = new Date(),
): { justOpened: boolean; status: ProviderCircuitStatus } {
  if (!state.open && state.signature !== D17_SIGNATURE) {
    state.signature = D17_SIGNATURE;
    state.threshold = D17_TRIP_THRESHOLD;
    state.windowMinutes = D17_WINDOW_MS / 60_000;
    state.consecutiveFailures = 0;
    state.firstFailureAt = undefined;
    state.lastFailureAt = undefined;
    state.evidence = [];
  }
  const nowMs = now.getTime();
  const lastFailureMs = validTime(state.lastFailureAt);
  const withinWindow = lastFailureMs !== undefined && nowMs - lastFailureMs >= 0 && nowMs - lastFailureMs <= D17_WINDOW_MS;

  if (!state.open) {
    state.consecutiveFailures = withinWindow ? state.consecutiveFailures + 1 : 1;
    state.firstFailureAt = withinWindow && state.firstFailureAt ? state.firstFailureAt : iso(now);
  }

  state.lastFailureAt = iso(now);
  state.evidence = [
    ...state.evidence,
    {
      ...evidence,
      occurredAt: evidence.occurredAt || iso(now),
    },
  ].slice(-D17_TRIP_THRESHOLD);

  const justOpened = !state.open && state.consecutiveFailures >= D17_TRIP_THRESHOLD;
  if (justOpened) {
    state.open = true;
    state.openedAt = iso(now);
    state.alertSentAt = undefined;
    state.lastAlertAttemptAt = undefined;
  }

  return { justOpened, status: getProviderCircuitStatus() };
}

export function recordProviderQuotaFailure(
  evidence: Omit<ProviderCircuitFailureEvidence, "occurredAt"> & { occurredAt?: string },
  now = new Date(),
): { justOpened: boolean; status: ProviderCircuitStatus } {
  return recordImmediateProviderFailure(QUOTA_SIGNATURE, evidence, now);
}

export function recordElevenLabsLlmFailure(
  evidence: Omit<ProviderCircuitFailureEvidence, "occurredAt"> & { occurredAt?: string },
  now = new Date(),
): { justOpened: boolean; status: ProviderCircuitStatus } {
  return recordImmediateProviderFailure(LLM_FAILURE_SIGNATURE, evidence, now);
}

function recordImmediateProviderFailure(
  signature: string,
  evidence: Omit<ProviderCircuitFailureEvidence, "occurredAt"> & { occurredAt?: string },
  now: Date,
): { justOpened: boolean; status: ProviderCircuitStatus } {
  const justOpened = !state.open;
  const sameSignature = state.signature === signature;
  state.open = true;
  state.signature = signature;
  state.consecutiveFailures = sameSignature ? Math.max(1, state.consecutiveFailures + 1) : 1;
  state.threshold = 1;
  state.windowMinutes = 0;
  state.firstFailureAt = sameSignature ? state.firstFailureAt ?? iso(now) : iso(now);
  state.lastFailureAt = iso(now);
  state.openedAt = sameSignature ? state.openedAt ?? iso(now) : iso(now);
  state.alertSentAt = undefined;
  state.lastAlertAttemptAt = undefined;
  state.evidence = [
    ...(sameSignature ? state.evidence : []),
    { ...evidence, occurredAt: evidence.occurredAt || iso(now) },
  ].slice(-D17_TRIP_THRESHOLD);
  return { justOpened, status: getProviderCircuitStatus() };
}

export function claimProviderCircuitAlertAttempt(now = new Date()): ProviderCircuitStatus | undefined {
  if (!state.open || state.alertSentAt) return undefined;

  const lastAttemptMs = validTime(state.lastAlertAttemptAt);
  if (lastAttemptMs !== undefined && now.getTime() - lastAttemptMs < ALERT_RETRY_MS) {
    return undefined;
  }

  state.lastAlertAttemptAt = iso(now);
  return getProviderCircuitStatus();
}

export function markProviderCircuitAlertSent(now = new Date()): void {
  if (state.open && !state.alertSentAt) {
    state.alertSentAt = iso(now);
  }
}

export function resetProviderCircuit(reason: string, now = new Date()): ProviderCircuitStatus {
  state.open = false;
  state.signature = D17_SIGNATURE;
  state.consecutiveFailures = 0;
  state.threshold = D17_TRIP_THRESHOLD;
  state.windowMinutes = D17_WINDOW_MS / 60_000;
  state.firstFailureAt = undefined;
  state.lastFailureAt = undefined;
  state.openedAt = undefined;
  state.alertSentAt = undefined;
  state.lastAlertAttemptAt = undefined;
  state.evidence = [];
  state.resetAt = iso(now);
  state.resetReason = reason.trim() || "Provider restoration confirmed";
  return getProviderCircuitStatus();
}

export function resetProviderCircuitForTests(): void {
  state.open = false;
  state.signature = D17_SIGNATURE;
  state.consecutiveFailures = 0;
  state.threshold = D17_TRIP_THRESHOLD;
  state.windowMinutes = D17_WINDOW_MS / 60_000;
  state.firstFailureAt = undefined;
  state.lastFailureAt = undefined;
  state.openedAt = undefined;
  state.alertSentAt = undefined;
  state.lastAlertAttemptAt = undefined;
  state.resetAt = undefined;
  state.resetReason = undefined;
  state.evidence = [];
}
