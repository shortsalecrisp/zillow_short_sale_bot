import axios from "axios";
import { config } from "./config";
import { isMatchingTerminalVoiceReceipt } from "./voiceOutcomeRecovery";
import { buildVoiceBotListingAddress, normalizePhoneToE164, normalizeString, parseVoiceBotDate } from "./voiceSheet";

export type VoiceTerminalReceiptProof = {
  conversationId: string;
  phone: string;
  fullName: string;
  listingAddress: string;
};

export async function fetchVoiceTerminalReceipt(id: string): Promise<unknown> {
  if (!/^conv_[a-zA-Z0-9]+$/.test(id) || !config.elevenLabs.apiKey) throw new Error("Terminal receipt lookup unavailable");
  const response = await axios.get(`${config.elevenLabs.baseUrl}/v1/convai/conversations/${id}`, {
    headers: { "xi-api-key": config.elevenLabs.apiKey }, timeout: 15_000,
  });
  return response.data;
}

export function verifyVoiceTerminalReceipt(
  rowNumber: number, row: unknown[], attempt: number | undefined,
  proof: VoiceTerminalReceiptProof, receipt: unknown,
  allowUnplacedFailure = false,
): void {
  if (attempt !== 1 && attempt !== 2) throw new Error("Terminal receipt attempt is invalid");
  const sentAt = parseVoiceBotDate(row[attempt === 1 ? 32 : 39])?.toISOString();
  const phone = normalizePhoneToE164(row[2]);
  if (!sentAt || !phone || phone !== proof.phone ||
      row.slice(0, 2).map(normalizeString).filter(Boolean).join(" ") !== proof.fullName ||
      buildVoiceBotListingAddress(row) !== proof.listingAddress) throw new Error("Terminal receipt owning identity changed");
  const detail = receipt as { conversation_id?: string; status?: string;
    metadata?: { call_duration_secs?: number; accepted_time_unix_secs?: number | null } } | null;
  const unplacedFailure = allowUnplacedFailure && detail?.status === "failed" &&
    detail.metadata?.call_duration_secs === 0 && !detail.metadata.accepted_time_unix_secs;
  if (!config.elevenLabs.agentId || detail?.conversation_id !== proof.conversationId ||
      (!(Number(detail?.metadata?.call_duration_secs) > 0) && !unplacedFailure) ||
      !isMatchingTerminalVoiceReceipt(receipt, rowNumber, {
        phone, callAttemptNumber: attempt, sentAt, previousResult: normalizeString(row[attempt === 1 ? 33 : 40]),
        fullName: proof.fullName, listingAddress: proof.listingAddress,
      }, config.elevenLabs.agentId)) throw new Error("Terminal receipt does not match a genuine completed owning attempt");
}
