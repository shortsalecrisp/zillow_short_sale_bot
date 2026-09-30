import type { sheets_v4 } from "googleapis";
import { logger } from "./logger";
import {
  normalizePhoneToE164,
  normalizeString,
  parseVoiceBotDate,
  VOICE_BOT_COL_CALL_1_RESULT,
  VOICE_BOT_COL_CALL_1_SENT,
  VOICE_BOT_COL_CALL_2_RESULT,
  VOICE_BOT_COL_CALL_2_SENT,
} from "./voiceSheet";

export const FINAL_RECEIPT_STALE_AFTER_MINUTES = 60;
export const FINAL_RECEIPT_CIRCUIT_THRESHOLD = 2;
export const INBOUND_QUIET_PERIOD_MS = 2 * 60_000;

export type FinalReceiptEvidence = {
  rowNumber: number;
  attempt: 1 | 2;
  startedAt: string;
};

export type FinalReceiptCircuitStatus = {
  open: boolean;
  threshold: number;
  staleAfterMinutes: number;
  monitorStartedAt: string;
  evidence: FinalReceiptEvidence[];
};

export function evaluateFinalReceiptCircuit(
  rows: Array<{ rowNumber: number; values: unknown[] }>,
  now = new Date(),
  monitorStartedAt = new Date(0),
): FinalReceiptCircuitStatus {
  const staleBefore = now.getTime() - FINAL_RECEIPT_STALE_AFTER_MINUTES * 60_000;
  const monitorStartedAtMs = monitorStartedAt.getTime();
  const attempts: FinalReceiptEvidence[] = [];

  for (const row of rows) {
    for (const attempt of [1, 2] as const) {
      const sentColumn = attempt === 1 ? VOICE_BOT_COL_CALL_1_SENT : VOICE_BOT_COL_CALL_2_SENT;
      const resultColumn = attempt === 1 ? VOICE_BOT_COL_CALL_1_RESULT : VOICE_BOT_COL_CALL_2_RESULT;
      const sentAt = parseVoiceBotDate(row.values[sentColumn - 1]);
      const result = normalizeString(row.values[resultColumn - 1]);
      if (
        sentAt &&
        sentAt.getTime() >= monitorStartedAtMs &&
        sentAt.getTime() <= staleBefore &&
        !result
      ) {
        attempts.push({ rowNumber: row.rowNumber, attempt, startedAt: sentAt.toISOString() });
      }
    }
  }

  attempts.sort((a, b) => b.startedAt.localeCompare(a.startedAt));
  const evidence = attempts.slice(0, FINAL_RECEIPT_CIRCUIT_THRESHOLD);
  return {
    open: evidence.length >= FINAL_RECEIPT_CIRCUIT_THRESHOLD,
    threshold: FINAL_RECEIPT_CIRCUIT_THRESHOLD,
    staleAfterMinutes: FINAL_RECEIPT_STALE_AFTER_MINUTES,
    monitorStartedAt: monitorStartedAt.toISOString(),
    evidence,
  };
}

let lastAlertKey = "";

export async function ensureFinalReceiptCircuitAlert(status: FinalReceiptCircuitStatus): Promise<void> {
  if (!status.open) return;
  const key = status.evidence.map((item) => `${item.rowNumber}:${item.attempt}:${item.startedAt}`).join("|");
  if (!key || key === lastAlertKey) return;

  const { createEmailTransporter, escapeHtml, requireEmailConfig } = await import("./emailAlerts");
  const emailConfig = requireEmailConfig();
  const evidence = status.evidence
    .map((item) => `Row ${item.rowNumber}, attempt ${item.attempt}, started ${item.startedAt}`)
    .join("\n");
  const transporter = createEmailTransporter(emailConfig);
  await transporter.sendMail({
    to: emailConfig.to,
    from: emailConfig.from,
    subject: "CRISP VOICE CALLING PAUSED - FINAL RECEIPTS MISSING",
    text: `Crisp voice calling is paused because ${status.threshold} calls exceeded ${status.staleAfterMinutes} minutes without final row receipts.\n\n${evidence}\n\nOnly read-only health checks are safe until the rows are reconciled or receipts recover.`,
    html: `<p><strong>Crisp voice calling is paused.</strong></p><p>${status.threshold} calls exceeded ${status.staleAfterMinutes} minutes without final row receipts.</p><pre>${escapeHtml(evidence)}</pre><p>Only read-only health checks are safe until the rows are reconciled or receipts recover.</p>`,
  });
  lastAlertKey = key;
  logger.info("Final-receipt circuit alert email sent", { evidence: status.evidence });
}

export type InboundQuietGate = {
  blocked: boolean;
  reason?: "unprocessed_inbound" | "recent_inbound";
  receivedAt?: string;
};

export function evaluateInboundQuietGate(
  phone: string,
  rows: unknown[][],
  now = new Date(),
): InboundQuietGate {
  const target = normalizePhoneToE164(phone);
  if (!target) return { blocked: false };

  for (const row of [...rows].reverse()) {
    if (normalizePhoneToE164(row[5]) !== target) continue;
    const status = normalizeString(row[1]).toLowerCase();
    const receivedAt = parseVoiceBotDate(row[7]) ?? parseVoiceBotDate(row[0]);
    if (status !== "processed" && status !== "coalesced") {
      return { blocked: true, reason: "unprocessed_inbound", receivedAt: receivedAt?.toISOString() };
    }
    if (receivedAt && now.getTime() - receivedAt.getTime() < INBOUND_QUIET_PERIOD_MS) {
      return { blocked: true, reason: "recent_inbound", receivedAt: receivedAt.toISOString() };
    }
  }
  return { blocked: false };
}

export async function getInboundQuietGate(
  sheets: sheets_v4.Sheets,
  phone: string,
  now = new Date(),
): Promise<InboundQuietGate> {
  const { config } = await import("./config");
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: config.googleSheets.sheetId,
    range: "'sms_inbound_queue'!A2:H",
  });
  const rows = response.data.values ?? [];
  return evaluateInboundQuietGate(phone, rows.slice(-500), now);
}
