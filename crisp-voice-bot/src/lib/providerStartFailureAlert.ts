import { createEmailTransporter, requireEmailConfig } from "./emailAlerts";
import { logger } from "./logger";
import type { ProviderCircuitFailureEvidence } from "./providerCircuitBreaker";

const WINDOW_MS = 30 * 60_000;
const THRESHOLD = 3;
const COOLDOWN_MS = 60 * 60_000;
const RETRY_MS = 10 * 60_000;
let evidence: ProviderCircuitFailureEvidence[] = [];
let lastAttemptAt: number | undefined;
let lastSentAt: number | undefined;
const seen = new Set<string>();

export function getProviderStartFailureStatus(now = new Date()) {
  evidence = evidence.filter((item) => {
    const age = now.getTime() - Date.parse(item.occurredAt);
    return age >= 0 && age <= WINDOW_MS;
  });
  return { recentFailures: evidence.length, threshold: THRESHOLD, windowMinutes: WINDOW_MS / 60_000,
    monitoringSinceRestart: true, pausesCalls: false,
    alertSentAt: lastSentAt === undefined ? null : new Date(lastSentAt).toISOString() };
}

export function recordProviderStartFailure(item: ProviderCircuitFailureEvidence, now = new Date()): void {
  if (seen.has(item.conversationId)) return;
  seen.add(item.conversationId);
  if (seen.size > 1000) seen.delete(seen.values().next().value!);
  getProviderStartFailureStatus(now);
  evidence.push({ ...item });
}

export function buildProviderStartFailureAlertCopy() {
  return {
    subject: "CRISP VOICE - REPEATED PROVIDER START FAILURES",
    text: `${evidence.length} provider starts failed within 30 minutes. These attempts did not reach a live conversation and are excluded from the sales funnel. This alert does not pause calls or authorize retries.\n\n` +
      evidence.map((item) => `Row ${item.rowNumber}, conversation ${item.conversationId}, ${item.occurredAt}: ${item.reason}`).join("\n"),
  };
}

async function sendAlert(copy: ReturnType<typeof buildProviderStartFailureAlertCopy>): Promise<void> {
  const emailConfig = requireEmailConfig();
  await createEmailTransporter(emailConfig).sendMail({ to: emailConfig.to, from: emailConfig.from, ...copy });
}

export async function ensureProviderStartFailureAlert(now = new Date(), send = sendAlert): Promise<void> {
  const status = getProviderStartFailureStatus(now);
  if (status.recentFailures < THRESHOLD ||
    (lastSentAt !== undefined && now.getTime() - lastSentAt < COOLDOWN_MS) ||
    (lastAttemptAt !== undefined && now.getTime() - lastAttemptAt < RETRY_MS)) return;
  lastAttemptAt = now.getTime();
  try {
    await send(buildProviderStartFailureAlertCopy());
    lastSentAt = now.getTime();
    logger.info("Repeated provider start failure alert sent", { recentFailures: status.recentFailures });
  } catch (error) {
    logger.error("Repeated provider start failure alert failed", { message: error instanceof Error ? error.message : String(error) });
  }
}

export function resetProviderStartFailureAlertForTests(): void {
  evidence = [];
  seen.clear();
  lastAttemptAt = undefined;
  lastSentAt = undefined;
}
