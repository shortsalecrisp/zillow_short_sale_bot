const D17_SIGNATURE = "sip_403_account_disabled_d17";
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
  state.consecutiveFailures = 0;
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
  state.consecutiveFailures = 0;
  state.firstFailureAt = undefined;
  state.lastFailureAt = undefined;
  state.openedAt = undefined;
  state.alertSentAt = undefined;
  state.lastAlertAttemptAt = undefined;
  state.resetAt = undefined;
  state.resetReason = undefined;
  state.evidence = [];
}
