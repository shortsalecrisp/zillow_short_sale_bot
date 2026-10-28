import axios from "axios";
import { config } from "./config";
import { isMatchingTerminalVoiceReceipt } from "./voiceOutcomeRecovery";
import { buildVoiceBotListingAddress, normalizePhoneToE164, normalizeString, parseVoiceBotDate } from "./voiceSheet";
import { isRecordingOrScreeningArtifact } from "./elevenLabsRecordingState";

export function verifyUnresolvedIvrFirstAttemptReceipt(receipt: unknown): void {
  const value = receipt as { status?: string; metadata?: { accepted_time_unix_secs?: number | null };
    transcript?: Array<{ role?: string; message?: string | null;
      tool_calls?: Array<{ tool_name?: string; name?: string }>;
      tool_results?: Array<{ tool_name?: string }> }> } | null;
  const transcript = value?.transcript ?? [];
  const hasMenu = transcript.some(t => t.role === "user" &&
    /\b(?:press|dial)\s+(?:one|two|three|four|five|six|seven|eight|nine|zero|[0-9])\b/i.test(t.message ?? ""));
  const assistantSpoke = transcript.some(t => ["agent", "assistant"].includes(t.role ?? "") && /[\p{L}\p{N}]/u.test(t.message ?? ""));
  const voicemailTool = transcript.some(t => [...(t.tool_calls ?? []), ...(t.tool_results ?? [])]
    .some(tool => tool.tool_name === "voicemail_detection" || ("name" in tool && tool.name === "voicemail_detection")));
  if (value?.status !== "done" || !value.metadata?.accepted_time_unix_secs || !hasMenu || assistantSpoke || voicemailTool ||
      !isRecordingOrScreeningArtifact(transcript)) throw new Error("IVR reconciliation requires a verified unresolved menu, not human contact or voicemail");
}

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
