import { getNextVoiceBotFirstAttemptWindowStart, getNextVoiceBotFollowupAttemptWindowStart } from "./voiceSheet";

export const VOICE_START_RESOLUTION_MARKER = "CODEX_VOICE_CALL_START_RESOLUTION_V1";

type TransportReceipt = {
  status?: unknown; has_audio?: unknown; has_user_audio?: unknown; has_response_audio?: unknown;
  metadata?: { accepted_time_unix_secs?: unknown; call_duration_secs?: unknown; error?: { reason?: unknown; error_type?: unknown } | null };
  transcript?: Array<{ message?: unknown }>;
};

export function transportFailureKind(conversation: TransportReceipt): "destination_blocked" | "transient" | undefined {
  const m = conversation?.metadata ?? {};
  if (conversation?.status !== "failed" || m.accepted_time_unix_secs || m.call_duration_secs !== 0 ||
      conversation.has_audio === true || conversation.has_user_audio === true || conversation.has_response_audio === true ||
      (conversation.transcript ?? []).some(t => typeof t.message === "string" && t.message.trim())) return undefined;
  const reason = String(m.error?.reason ?? "").toLowerCase();
  if (/not included in whitelisted countries|network blocked/.test(reason)) return "destination_blocked";
  if (m.error?.error_type === "call_initialization_error" && /sip request timed out/.test(reason)) return "transient";
  return undefined;
}

export function nextTransportRecoverySlot(now: Date, timeZone: string, phone: string, attempt: number): Date {
  // A proven transport failure is not a human miss. Never recycle it in the same local window.
  const tomorrow = new Date(now.getTime() + 24 * 60 * 60_000);
  return attempt === 1 ? getNextVoiceBotFirstAttemptWindowStart(tomorrow, timeZone, phone)
    : getNextVoiceBotFollowupAttemptWindowStart(now, timeZone, 1);
}

export function hasUnresolvedVoiceStart(notes: string): boolean {
  const resolved = new Set<string>();
  const pending: string[] = [];
  for (const line of notes.split(/\r?\n/)) {
    for (const [marker, isResolution] of [[VOICE_START_RESOLUTION_MARKER, true], ["CODEX_VOICE_CALL_START_UNCERTAIN_V1", false]] as const) {
      const index = line.indexOf(marker);
      if (index < 0) continue;
      try {
        const d = JSON.parse(line.slice(index + marker.length).trim());
        if (typeof d.callStartRequestId !== "string" || !d.callStartRequestId) return true;
        if (isResolution) {
          if (d.status === "definitive_failure" && typeof d.conversationId === "string" && d.conversationId &&
              typeof d.phone === "string" && Number.isInteger(d.rowNumber) && [1, 2].includes(d.callAttemptNumber)) {
            resolved.add(`${d.rowNumber}:${d.callAttemptNumber}:${d.callStartRequestId}`);
          }
        } else pending.push(`${d.rowNumber}:${d.callAttemptNumber}:${d.callStartRequestId}`);
      } catch { return true; }
    }
  }
  return pending.some(key => !resolved.has(key));
}
