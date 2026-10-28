import { finalUnansweredPreservationReason } from "./voiceFinalUnanswered";
import { createHash } from "node:crypto";
import { buildVoiceBotListingAddress, normalizePhoneToE164, normalizeString, parseVoiceBotDate } from "./voiceSheet";

export function voiceReconciliationStateHash(row: unknown[]): string {
  const dates = new Set([31, 32, 39]);
  const columns = [0, 1, 2, 3, 4, 5, 6, 9, 10, 13, 15, 16, 17, 19, 28, 29, 30, 31, 32, 33, 35, 36, 37, 38, 39, 40, 41];
  return createHash("sha256").update(JSON.stringify(columns.map(i => dates.has(i)
    ? parseVoiceBotDate(row[i])?.toISOString() ?? normalizeString(row[i]) : normalizeString(row[i])))).digest("hex");
}

export type VoiceOutcomeRecoveryProof = {
  phone: string;
  callAttemptNumber: 1 | 2;
  sentAt: string;
  previousResult: string;
  fullName: string;
  listingAddress: string;
  expectedLeadStatusCode?: "N";
  reconciliationReason?: "verified_unresolved_ivr_first_attempt";
};

export function voiceOutcomeRecoveryBlockReason(row: unknown[], proof: VoiceOutcomeRecoveryProof): string | undefined {
  if (normalizeString(row[41]).includes("CODEX_VOICE_EXPIRED_TO_MAILSHAKE_V1")) return "expired_to_mailshake";
  if (normalizePhoneToE164(row[2]) !== proof.phone) return "phone_changed";
  if (row.slice(0, 2).map(normalizeString).filter(Boolean).join(" ") !== proof.fullName ||
      buildVoiceBotListingAddress(row) !== proof.listingAddress) return "identity_or_listing_changed";
  const sent = proof.callAttemptNumber === 1 ? 32 : 39;
  if (parseVoiceBotDate(row[sent])?.toISOString() !== proof.sentAt) return "attempt_changed";
  if (normalizeString(row[sent + 1]) !== proof.previousResult) return "outcome_changed";
  if (proof.callAttemptNumber === 1 && parseVoiceBotDate(row[39])) return "second_attempt_already_started";
  if (proof.expectedLeadStatusCode !== undefined) {
    if (normalizeString(row[10]) !== proof.expectedLeadStatusCode || proof.previousResult !== "call_received_agent_hung_up") {
      return "reconciliation_status_changed";
    }
    if (proof.reconciliationReason && (proof.reconciliationReason !== "verified_unresolved_ivr_first_attempt" ||
        proof.callAttemptNumber !== 1)) return "reconciliation_scope_changed";
    if (proof.reconciliationReason && ["sent", "pushed"].includes(normalizeString(row[28]).toLowerCase())) {
      return "campaign_already_enrolled";
    }
    const unmarked = [...row]; unmarked[10] = "";
    return finalUnansweredPreservationReason(unmarked);
  }
  return finalUnansweredPreservationReason(row);
}

export function getPendingVoiceOutcomeRecoveries(
  rows: Array<{ rowNumber: number; values: unknown[] }>, now: Date, monitorStartedAt: Date,
  includeProtectedReceipts = false,
): Array<{ rowNumber: number; proof: VoiceOutcomeRecoveryProof; receiptOnly?: boolean }> {
  return rows.flatMap(({ rowNumber, values }) => {
    const attempt = parseVoiceBotDate(values[39]) ? 2 : 1;
    const index = attempt === 1 ? 32 : 39;
    const sent = parseVoiceBotDate(values[index]);
    const result = normalizeString(values[index + 1]);
    const phone = normalizePhoneToE164(values[2]);
    if (!sent || !phone || sent < monitorStartedAt || now.getTime() - sent.getTime() < 90_000 ||
        !["", "call_start_reconciled"].includes(result)) return [];
    const proof: VoiceOutcomeRecoveryProof = { phone, callAttemptNumber: attempt, sentAt: sent.toISOString(), previousResult: result,
      fullName: values.slice(0, 2).map(normalizeString).filter(Boolean).join(" "), listingAddress: buildVoiceBotListingAddress(values) };
    const blocked = voiceOutcomeRecoveryBlockReason(values, proof);
    if (!blocked) return [{ rowNumber, proof }];
    return includeProtectedReceipts && blocked !== "expired_to_mailshake" &&
      ["existing_lead_status", "handoff_callback_or_human_takeover", "contact_preference", "response_requires_review", "protected_contact_outcome"].includes(blocked)
      ? [{ rowNumber, proof, receiptOnly: true }] : [];
  });
}

// Recovery may fetch and persist a receipt, but never infer permission to dial.
export function isMatchingTerminalVoiceReceipt(
  value: unknown, rowNumber: number, proof: VoiceOutcomeRecoveryProof, agentId: string,
): boolean {
  if (!value || typeof value !== "object") return false;
  const d = value as {
    conversation_id?: string; agent_id?: string; status?: string;
    conversation_initiation_client_data?: { dynamic_variables?: Record<string, unknown> };
    metadata?: { start_time_unix_secs?: number; phone_call?: { external_number?: string; direction?: string } };
  };
  const v = d.conversation_initiation_client_data?.dynamic_variables ?? {};
  const phone = d.metadata?.phone_call;
  const start = d.metadata?.start_time_unix_secs;
  const sent = Date.parse(proof.sentAt) / 1000;
  return typeof d.conversation_id === "string" && d.agent_id === agentId &&
    ["done", "failed"].includes(d.status ?? "") && Number(v.rowNumber) === rowNumber &&
    Number(v.callAttemptNumber) === proof.callAttemptNumber &&
    normalizePhoneToE164(v.phone) === proof.phone && normalizePhoneToE164(phone?.external_number) === proof.phone &&
    phone?.direction === "outbound" && typeof start === "number" && start >= sent - 10 && start <= sent + 600 &&
    ![v.testMode, v.providerProof, v.providerProofCall, v.isTestCall].some(x => x === true || String(x).toLowerCase() === "true");
}

export const POST_CALL_FALLBACK_MAX_ATTEMPTS = 120;
export function shouldContinuePostCallFallback(attempt: number): boolean {
  return attempt < POST_CALL_FALLBACK_MAX_ATTEMPTS;
}
