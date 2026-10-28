import { normalizePhoneToE164, normalizeString, parseVoiceBotDate } from "./voiceSheet";
import { VOICE_START_RESOLUTION_MARKER } from "./voiceTransportRecovery";

export const VOICE_ATTEMPT_IDENTITY_MARKER = "CODEX_VOICE_ATTEMPT_IDENTITY_V1";

export function formatVoiceAttemptIdentity(rowNumber: number, phone: string, attempt: number, sentAt: Date): string {
  return `${VOICE_ATTEMPT_IDENTITY_MARKER} ${JSON.stringify({ rowNumber, phone: normalizePhoneToE164(phone),
    callAttemptNumber: attempt, sentAt: sentAt.toISOString() })}`;
}

export function getVoicePhoneAttemptIdentity(row: unknown[]): {
  currentPhone: string | undefined;
  currentPhoneAttempts: number;
  unknownAttempts: number;
  historicalPhones: string[];
  reviewReason?: string;
} {
  const currentPhone = normalizePhoneToE164(row[2]);
  const phones = new Map<number, Set<string>>();
  const genuineCurrentAttempts = new Set<number>();
  let malformed = false;
  let changedPhone = false;
  const add = (attempt: unknown, phone: unknown) => {
    const n = Number(attempt), normalized = normalizePhoneToE164(phone);
    if (![1, 2].includes(n) || !normalized) { malformed = true; return; }
    if (normalized !== currentPhone) changedPhone = true;
    const set = phones.get(n) ?? new Set<string>(); set.add(normalized); phones.set(n, set);
  };
  const lines = normalizeString(row[41]).split(/\r?\n/);
  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    for (const marker of [VOICE_ATTEMPT_IDENTITY_MARKER, VOICE_START_RESOLUTION_MARKER, "CODEX_VOICE_CALL_METRICS_V1"]) {
      const metricsBlock = marker === "CODEX_VOICE_CALL_METRICS_V1" && line === `--- ${marker} ---`;
      if (!metricsBlock && !line.startsWith(`${marker} `)) continue;
      try {
        const data = JSON.parse(metricsBlock ? lines[i + 1] : line.slice(marker.length).trim());
        if (marker === VOICE_START_RESOLUTION_MARKER) {
          // Legacy unaccepted starts have no sent slot or reservation after recovery.
          if (data.status !== "definitive_failure" || !Number.isInteger(data.rowNumber) || data.rowNumber < 2 ||
              typeof data.callStartRequestId !== "string" || !data.callStartRequestId.trim() ||
              typeof data.conversationId !== "string" || !data.conversationId.trim()) {
            malformed = true;
          } else add(data.callAttemptNumber, data.phone);
        } else if (marker === VOICE_ATTEMPT_IDENTITY_MARKER) {
          const attempt = Number(data.callAttemptNumber);
          // A released transport failure is still historical phone identity,
          // not permission to reuse its old SMS/schedule anchors for a new phone.
          add(attempt, data.phone);
        } else if (data.schema === "voice_call_metrics_v1" && data.providerIdentity?.source === "final_conversation_receipt") {
          if ([data.call?.testMode, data.call?.providerProofCall].some(v => v === true || v === "true")) continue;
          add(data.call?.callAttemptNumber, data.call?.requestedPhone);
          add(data.call?.callAttemptNumber, data.call?.dialedPhone);
          if (typeof data.call?.conversationId === "string" && Number(data.metrics?.durationSecs) > 0 &&
              ["done", "failed"].includes(data.call?.status) &&
              normalizePhoneToE164(data.call?.requestedPhone) === currentPhone &&
              normalizePhoneToE164(data.call?.dialedPhone) === currentPhone) {
            genuineCurrentAttempts.add(Number(data.call.callAttemptNumber));
          }
        }
      } catch { malformed = true; }
    }
  }
  let currentPhoneAttempts = 0, unknownAttempts = 0;
  let reviewReason = changedPhone ? "attempt_phone_changed" : malformed ? "malformed_attempt_phone_evidence" : undefined;
  for (const attempt of [1, 2]) {
    if (!parseVoiceBotDate(row[attempt === 1 ? 32 : 39])) continue;
    const evidence = phones.get(attempt);
    if (!evidence?.size) { unknownAttempts += 1; continue; }
    if (!currentPhone || evidence.size !== 1 || !evidence.has(currentPhone)) reviewReason = "attempt_phone_changed";
    if (genuineCurrentAttempts.has(attempt)) currentPhoneAttempts += 1;
  }
  return { currentPhone, currentPhoneAttempts, unknownAttempts,
    historicalPhones: [...new Set([...phones.values()].flatMap(set => [...set]))], reviewReason };
}
