import { createEmailTransporter, escapeHtml, requireEmailConfig } from "./emailAlerts";
import { logger } from "./logger";
import {
  claimProviderCircuitAlertAttempt,
  markProviderCircuitAlertSent,
  type ProviderCircuitStatus,
} from "./providerCircuitBreaker";

type ProviderCircuitAlertCopy = {
  subject: string;
  text: string;
  html: string;
};

function plural(value: number, singular: string, pluralValue = `${singular}s`): string {
  return value === 1 ? singular : pluralValue;
}

function providerCircuitAlertDescription(status: ProviderCircuitStatus): {
  subjectLabel: string;
  summary: string;
  resumeInstruction: string;
} {
  if (status.signature === "sip_403_account_disabled_d17") {
    return {
      subjectLabel: "TELNYX D17",
      summary:
        `${status.consecutiveFailures} consecutive Telnyx SIP 403 Account is disabled D17 ` +
        `${plural(status.consecutiveFailures, "failure")} occurred within ${status.windowMinutes} minutes.`,
      resumeInstruction:
        "Rows were preserved for retry. Resume only with a successful proof call or confirmed Telnyx/provider restoration.",
    };
  }

  if (status.signature === "elevenlabs_quota_exceeded") {
    return {
      subjectLabel: "ELEVENLABS QUOTA",
      summary:
        `${status.consecutiveFailures} ElevenLabs quota ${plural(status.consecutiveFailures, "failure")} occurred. ` +
        "This is a billing or credits/minutes condition, not a Telnyx account-disabled D17 failure.",
      resumeInstruction:
        "Rows were preserved for retry. Resume only after ElevenLabs quota/billing is restored or a successful proof call confirms calls are working.",
    };
  }

  if (status.signature === "elevenlabs_llm_failure") {
    return {
      subjectLabel: "ELEVENLABS LLM FAILURE",
      summary:
        `${status.consecutiveFailures} ElevenLabs LLM cascade ${plural(status.consecutiveFailures, "failure")} occurred. ` +
        "This means ElevenLabs could not generate the assistant response during the call; it is not a Telnyx D17 failure and is not confirmed quota exhaustion.",
      resumeInstruction:
        "Rows were preserved for retry. Resume after the bot fix is deployed and a successful proof call or provider readback confirms ElevenLabs is responding normally.",
    };
  }

  return {
    subjectLabel: "PROVIDER CIRCUIT",
    summary:
      `${status.consecutiveFailures} provider ${plural(status.consecutiveFailures, "failure")} occurred for signature ${status.signature}.`,
    resumeInstruction:
      "Rows were preserved for retry. Resume only with a successful proof call or confirmed provider restoration.",
  };
}

export function buildProviderCircuitAlertCopy(status: ProviderCircuitStatus): ProviderCircuitAlertCopy {
  const description = providerCircuitAlertDescription(status);
  const evidenceText = status.evidence.length > 0
    ? status.evidence
        .map(
          (item) =>
            `Row ${item.rowNumber}, attempt ${item.callAttemptNumber}, conversation ${item.conversationId}, ` +
            `${item.occurredAt}: ${item.reason}`,
        )
        .join("\n")
    : "No row evidence captured.";
  const text =
    `Crisp voice calling paused: ${description.summary}\n\n${evidenceText}\n\n` +
    description.resumeInstruction;
  const html = `<p><strong>Crisp voice calling is paused.</strong></p>
<p>${escapeHtml(description.summary)}</p>
<pre>${escapeHtml(evidenceText)}</pre>
<p>${escapeHtml(description.resumeInstruction)}</p>`;

  return {
    subject: `CRISP VOICE CALLING PAUSED - ${description.subjectLabel}`,
    text,
    html,
  };
}

async function sendProviderCircuitAlert(status: ProviderCircuitStatus): Promise<void> {
  const emailConfig = requireEmailConfig();
  const transporter = createEmailTransporter(emailConfig);
  const copy = buildProviderCircuitAlertCopy(status);

  await transporter.sendMail({
    to: emailConfig.to,
    from: emailConfig.from,
    subject: copy.subject,
    text: copy.text,
    html: copy.html,
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
