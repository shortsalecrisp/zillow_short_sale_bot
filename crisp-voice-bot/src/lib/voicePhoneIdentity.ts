import { normalizePhoneToE164, normalizeString, parseVoiceBotDate } from "./voiceSheet";
import { VOICE_START_RESOLUTION_MARKER } from "./voiceTransportRecovery";
import { createHash } from "node:crypto";

export const VOICE_ATTEMPT_IDENTITY_MARKER = "CODEX_VOICE_ATTEMPT_IDENTITY_V1";
export const VOICE_CORRECTED_PHONE_RELEASE_MARKER = "CODEX_VOICE_CORRECTED_PHONE_RELEASE_V1";

export function correctedPhoneReleaseDigest(row: unknown[]): string {
  const phone = normalizePhoneToE164(row[2]);
  const bindings = normalizeString(row[41]).split(/\r?\n/).filter(line => {
    if (![VOICE_ATTEMPT_IDENTITY_MARKER, VOICE_START_RESOLUTION_MARKER].some(m => line.startsWith(m + " "))) return false;
    try { return normalizePhoneToE164(JSON.parse(line.slice(line.indexOf(" ") + 1)).phone) !== phone; }
    catch { return true; }
  });
  return createHash("sha256").update(JSON.stringify({
    identity: [0, 1, 2, 3, 4, 5, 6, 27].map(i => normalizeString(row[i])),
    initial: parseVoiceBotDate(row[22])?.toISOString(), followup: parseVoiceBotDate(row[23])?.toISOString(), bindings,
  })).digest("hex");
}

function reviewedFormerPhones(row: unknown[]): Set<string> {
  const lines = normalizeString(row[41]).split(/\r?\n/).filter(l => l.startsWith(VOICE_CORRECTED_PHONE_RELEASE_MARKER + " "));
  if (lines.length !== 1) return new Set();
  try {
    const release = JSON.parse(lines[0].slice(VOICE_CORRECTED_PHONE_RELEASE_MARKER.length + 1));
    if (release.phone !== normalizePhoneToE164(row[2]) || release.digest !== correctedPhoneReleaseDigest(row) ||
        !Array.isArray(release.formerPhones) || !release.formerPhones.length ||
        !release.formerPhones.every((p: unknown) => typeof p === "string" && normalizePhoneToE164(p) === p && p !== release.phone)) return new Set();
    return new Set(release.formerPhones);
  } catch { return new Set(); }
}

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
  const historicalPhones = new Set<string>();
  const reviewed = reviewedFormerPhones(row);
  const genuineCurrentAttempts = new Set<number>();
  let malformed = false;
  let changedPhone = false;
  const add = (attempt: unknown, phone: unknown) => {
    const n = Number(attempt), normalized = normalizePhoneToE164(phone);
    if (![1, 2].includes(n) || !normalized) { malformed = true; return; }
    historicalPhones.add(normalized);
    if (reviewed.has(normalized)) return;
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
          // A reviewed zero-acceptance episode can never hide a genuine old call.
          if (reviewed.has(normalizePhoneToE164(data.call?.requestedPhone) ?? "")) malformed = true;
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
    historicalPhones: [...historicalPhones], reviewReason };
}
