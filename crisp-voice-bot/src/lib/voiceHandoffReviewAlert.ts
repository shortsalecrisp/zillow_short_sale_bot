import { createHash } from "node:crypto";
import { readGmailImportState, writeGmailImportState } from "./gmailImportState";
import { createEmailTransporter, escapeHtml, requireEmailConfig } from "./emailAlerts";
import { buildElevenLabsPlaybackUrl } from "./elevenLabsPlayback";
import { getGoogleSheetsClient } from "./googleSheets";
import { config } from "./config";
import { buildVoiceBotListingAddress, normalizePhoneToE164, normalizeString } from "./voiceSheet";
import type { CallMetadata } from "../types";

export type MissingHandoffReview = { offer: string; reply: string; evidence: "explicit_live_now" | "interrupted_offer" };
type AlertState = { conversationId: string; status: "sending" | "sent" | "uncertain"; messageId: string };
type Input = { conversationId: string; metadata: CallMetadata; review: MissingHandoffReview; transcript: string };
export type HandoffReviewDependencies = {
  current?: (input: Input) => Promise<boolean>;
  read?: (key: string) => Promise<AlertState | undefined>;
  write?: (key: string, state: AlertState) => Promise<void>;
  send?: (message: { subject: string; text: string; html: string; messageId: string }) => Promise<unknown>;
  playback?: (id: string) => string;
};
const locks = new Map<string, Promise<unknown>>();

async function currentIdentityAllowsReview({ conversationId, metadata }: Input): Promise<boolean> {
  const sheets = await getGoogleSheetsClient();
  const response = await sheets.spreadsheets.values.get({ spreadsheetId: config.googleSheets.sheetId,
    range: `'${config.googleSheets.tabName.replace(/'/g, "''")}'!A${metadata.rowNumber}:AS${metadata.rowNumber}` });
  const row = response.data.values?.[0] ?? [];
  const marked = (value: unknown) => !["", "no", "false", "0"].includes(normalizeString(value).toLowerCase());
  return normalizePhoneToE164(row[2]) === metadata.requestedPhone && metadata.requestedPhone === metadata.dialedPhone &&
    row.slice(0, 2).map(normalizeString).filter(Boolean).join(" ") === metadata.fullName &&
    buildVoiceBotListingAddress(row) === metadata.listingAddress && normalizeString(row[10]) === "Y" &&
    normalizeString(row[metadata.callAttemptNumber === 2 ? 40 : 33]) === "interested_followup_review" &&
    [12, 13, 15, 16, 19, 28, 35, 36, 37, 38, 43, 44].every(index => !marked(row[index])) &&
    normalizeString(row[41]).includes(conversationId);
}

export function buildHandoffReviewMessage(input: Input, playback: (id: string) => string = buildElevenLabsPlaybackUrl) {
  const { conversationId, metadata, review, transcript } = input;
  const messageId = `<voice-handoff-review-${createHash("sha256").update(conversationId).digest("hex")}@crisp-voice-bot.onrender.com>`;
  const subject = `ACTION REQUIRED - LIVE CONNECTION NOT CAPTURED - ${metadata.fullName}`;
  const text = `An interested listener's live connection was not captured. This is an owner review, NOT a scheduled callback or completed transfer. No automated redial is authorized.\n\nSheet contact (verify the spoken identity): ${metadata.fullName}\nPhone: ${metadata.requestedPhone}\nListing: ${metadata.listingAddress}\nRow: ${metadata.rowNumber}\nConversation: ${conversationId}\nOffer evidence: ${review.evidence}\nAssistant offer: ${review.offer}\nCaller reply: ${review.reply}\nNo availability/transfer tool receipt exists. No separate later callback consent or time was captured. Review the caller's introduction and latest preferences before any manual follow-up.\nPlayback: ${playback(conversationId)}\n\nFinal transcript:\n${transcript}`;
  return { subject, text, html: `<pre>${escapeHtml(text)}</pre>`, messageId };
}

// BotState is durable but not compare-and-swap: deployment must remain single-instance.
// A persisted sending/uncertain claim is never automatically resent after restart.
export async function notifyMissingHandoffReview(input: Input, deps: HandoffReviewDependencies = {}): Promise<string> {
  if (input.metadata.testMode || input.metadata.providerProofCall) return "test_excluded";
  const key = `voice_missing_handoff_review_v1:${input.conversationId}`;
  if (locks.has(key)) return locks.get(key) as Promise<string>;
  const operation = (async () => {
    const read = deps.read ?? ((key: string) => readGmailImportState<AlertState | undefined>(key, undefined, true));
    const write = deps.write ?? writeGmailImportState;
    const existing = await read(key);
    if (existing) {
      if (existing.conversationId !== input.conversationId || !["sending", "sent", "uncertain"].includes(existing.status)) {
        throw new Error("Invalid handoff review notification state; reconcile manually");
      }
      return `already_${existing.status}`;
    }
    if (!await (deps.current ?? currentIdentityAllowsReview)(input)) return "protected_or_changed";
    const message = buildHandoffReviewMessage(input, deps.playback);
    const state: AlertState = { conversationId: input.conversationId, status: "sending", messageId: message.messageId };
    await write(key, state);
    const claim = await read(key);
    if (claim?.messageId !== state.messageId || claim.status !== "sending") throw new Error("Handoff review claim not confirmed");
    try {
      if (!await (deps.current ?? currentIdentityAllowsReview)(input)) return "protected_after_claim";
      await (deps.send ?? (async message => {
        const email = requireEmailConfig();
        return createEmailTransporter(email).sendMail({ ...message, to: email.to, from: email.from });
      }))(message);
      await write(key, { ...state, status: "sent" });
      return "sent";
    } catch (error) {
      await write(key, { ...state, status: "uncertain" });
      throw error;
    }
  })();
  locks.set(key, operation);
  try { return await operation; } finally { if (locks.get(key) === operation) locks.delete(key); }
}
