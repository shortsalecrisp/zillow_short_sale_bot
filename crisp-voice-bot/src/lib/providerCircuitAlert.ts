import { createEmailTransporter, escapeHtml, requireEmailConfig } from "./emailAlerts";
import { logger } from "./logger";
import {
  claimProviderCircuitAlertAttempt,
  markProviderCircuitAlertSent,
  type ProviderCircuitStatus,
} from "./providerCircuitBreaker";

async function sendProviderCircuitAlert(status: ProviderCircuitStatus): Promise<void> {
  const emailConfig = requireEmailConfig();
  const transporter = createEmailTransporter(emailConfig);
  const evidenceText = status.evidence
    .map(
      (item) =>
        `Row ${item.rowNumber}, attempt ${item.callAttemptNumber}, conversation ${item.conversationId}, ` +
        `${item.occurredAt}: ${item.reason}`,
    )
    .join("\n");
  const text =
    `Crisp voice calling paused after ${status.consecutiveFailures} consecutive Telnyx SIP 403 ` +
    `Account is disabled D17 failures within ${status.windowMinutes} minutes.\n\n${evidenceText}\n\n` +
    "Rows were preserved for retry. Resume only with a successful proof call or confirmed provider restoration.";
  const html = `<p><strong>Crisp voice calling is paused.</strong></p>
<p>${escapeHtml(
    `${status.consecutiveFailures} consecutive Telnyx SIP 403 Account is disabled D17 failures occurred within ${status.windowMinutes} minutes.`,
  )}</p>
<pre>${escapeHtml(evidenceText)}</pre>
<p>Rows were preserved for retry. Resume only with a successful proof call or confirmed provider restoration.</p>`;

  await transporter.sendMail({
    to: emailConfig.to,
    from: emailConfig.from,
    subject: "CRISP VOICE CALLING PAUSED - TELNYX D17",
    text,
    html,
  });
}

export async function ensureProviderCircuitAlert(): Promise<void> {
  const status = claimProviderCircuitAlertAttempt();
  if (!status) return;

  try {
    await sendProviderCircuitAlert(status);
    markProviderCircuitAlertSent();
    logger.info("Provider circuit alert email sent", {
      signature: status.signature,
      openedAt: status.openedAt,
      rows: status.evidence.map((item) => item.rowNumber),
    });
  } catch (error) {
    logger.error("Provider circuit alert email failed", {
      signature: status.signature,
      openedAt: status.openedAt,
      message: error instanceof Error ? error.message : String(error),
    });
  }
}
