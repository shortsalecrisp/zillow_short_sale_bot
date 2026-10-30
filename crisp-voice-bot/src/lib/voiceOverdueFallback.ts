import axios, { type AxiosInstance } from "axios";
import { config } from "./config";
import { logger } from "./logger";
import { hasUnresolvedVoiceStart } from "./voiceTransportRecovery";
import { getVoicePhoneAttemptIdentity } from "./voicePhoneIdentity";
import { currentPhoneVoiceRow } from "./voicePhoneEpisode";
import {
  buildVoiceBotDateInTimeZone,
  getNextVoiceBotFirstAttemptWindowStart,
  getNextVoiceBotFollowupAttemptWindowStart,
  getVoiceBotAgentTimeZone,
  getVoiceBotLocalDateKey,
  getVoiceBotLocalMinutes,
  getVoiceBotCallWindowsForDay,
  isRetryableVoiceBotResult,
  normalizeMarker,
  normalizePhoneToE164,
  normalizeString,
  parseVoiceBotDate,
} from "./voiceSheet";

export const VOICE_OVERDUE_FALLBACK_MARKER = "CODEX_VOICE_EXPIRED_TO_MAILSHAKE_V1";
export const VOICE_OVERDUE_FALLBACK_BUSINESS_DAYS = 2;

export type VoiceOverdueFallbackPlan = {
  rowNumber: number;
  phone: string;
  callAttemptNumber: 1 | 2;
  agentTimeZone: string;
  scheduledFor: Date;
  deadline: Date;
  evidenceSince: Date;
  providerRecovery: boolean;
};

function marked(value: unknown): boolean {
  const text = normalizeMarker(value);
  return Boolean(text) && !["no", "false", "0"].includes(text);
}

function originalNoStartDueAt(notes: string, row: number, attempt: number): Date | undefined {
  const dates: Date[] = [];
  const marker = "CODEX_VOICE_SCHEDULED_NO_START_V1";
  for (const line of notes.split(/\r?\n/)) {
    const index = line.indexOf(marker);
    if (index < 0) continue;
    try {
      const parsed = JSON.parse(line.slice(index + marker.length).trim());
      if (parsed.rowNumber === row && parsed.callAttemptNumber === attempt) {
        const date = parseVoiceBotDate(parsed.candidateDueAt);
        if (date) dates.push(date);
      }
    } catch { /* Other note lines are not scheduling evidence. */ }
  }
  return dates.sort((a, b) => a.getTime() - b.getTime())[0];
}

export function getVoiceMissedWindowDeadline(scheduledFor: Date, timeZone: string): Date {
  let key = getVoiceBotLocalDateKey(scheduledFor, timeZone);
  const minutes = getVoiceBotLocalMinutes(scheduledFor, timeZone);
  const windows = getVoiceBotCallWindowsForDay(1);
  const window = windows.find((w) => minutes >= w.startMinutes && minutes < w.endMinutes);
  // Unknown legacy slots receive a conservative 60-minute window, not an
  // earlier deadline inferred from today's narrower cold-call windows.
  const endMinutes = window?.endMinutes ?? Math.min(minutes + 60, 23 * 60 + 59);
  for (let remaining = VOICE_OVERDUE_FALLBACK_BUSINESS_DAYS; remaining > 0;) {
    const cursor = new Date(`${key}T12:00:00Z`);
    cursor.setUTCDate(cursor.getUTCDate() + 1);
    key = cursor.toISOString().slice(0, 10);
    if (cursor.getUTCDay() !== 0 && cursor.getUTCDay() !== 6) remaining -= 1;
  }
  return buildVoiceBotDateInTimeZone(key, endMinutes, timeZone);
}

export function getVoiceOverdueFallbackPlan(
  rowNumber: number, values: unknown[], now: Date,
): VoiceOverdueFallbackPlan | undefined {
  values = currentPhoneVoiceRow(values);
  if (normalizeString(values[10]) || normalizeMarker(values[8]) !== "x") return undefined;
  if (getVoicePhoneAttemptIdentity(values).reviewReason) return undefined;
  const notes = normalizeString(values[41]);
  if (notes.includes(VOICE_OVERDUE_FALLBACK_MARKER)) return undefined;
  if ([15, 16, 19, 35, 36, 37, 38].some((index) => marked(values[index]))) return undefined;
  if (/handoff|callback|booking|takeover|opt.?out|do.?not.?contact/i.test(normalizeString(values[13]))) return undefined;
  if (/interested|callback|call me|send.{0,12}info|email|transfer|handoff|remove|stop|wrong number|not a short sale/i
    .test(normalizeString(values[9]))) return undefined;
  const phone = normalizePhoneToE164(values[2]), agentTimeZone = getVoiceBotAgentTimeZone(values);
  if (!phone || !agentTimeZone) return undefined;
  const firstResult = normalizeString(values[33]).toLowerCase(), secondResult = normalizeString(values[40]).toLowerCase();
  const recordedFirstStart = parseVoiceBotDate(values[32]), secondSent = parseVoiceBotDate(values[39]);
  if ((normalizeString(values[32]) && !recordedFirstStart) || (normalizeString(values[39]) && !secondSent)) return undefined;
  // A provider-blocked, zero-duration initiation is unplaced work, not attempt two.
  // The complete provider-history scan still vetoes expiry if anything was accepted.
  const blockedBeforeConnection = firstResult === "provider_destination_blocked";
  const firstSent = blockedBeforeConnection ? undefined : recordedFirstStart;
  if (secondSent || secondResult) return undefined;
  if (firstSent && (!firstResult || !isRetryableVoiceBotResult(firstResult))) return undefined;
  if (!firstSent && firstResult && !blockedBeforeConnection) return undefined;
  if (/call_start_uncertain|pending|in_progress|live_transfer_requested/.test(firstResult)) return undefined;
  if (hasUnresolvedVoiceStart(notes)) return undefined;
  const callAttemptNumber = firstSent ? 2 : 1;
  const followupSent = parseVoiceBotDate(values[23]);
  if (!firstSent && !followupSent) return undefined;
  const providerRecovery = ["call_start_failed", "call_start_receipt_missing"].includes(firstResult);
  const policyDue = firstSent
    ? getNextVoiceBotFollowupAttemptWindowStart(firstSent, agentTimeZone, providerRecovery ? 1 : 0)
    : getNextVoiceBotFirstAttemptWindowStart(followupSent!, agentTimeZone, phone);
  const scheduled = parseVoiceBotDate(values[31]);
  const original = originalNoStartDueAt(notes, rowNumber, callAttemptNumber);
  const automaticRetry = firstSent && normalizeString(values[30]) === "voice_call_2_due";
  const anchors = [original, blockedBeforeConnection ? recordedFirstStart : undefined,
    automaticRetry ? policyDue : scheduled ?? policyDue].filter((d): d is Date => Boolean(d));
  const scheduledFor = anchors.sort((a, b) => a.getTime() - b.getTime())[0];
  if (scheduledFor < config.voiceQueue.minCandidateDueAt) return undefined;
  const deadline = getVoiceMissedWindowDeadline(scheduledFor, agentTimeZone);
  if (now < deadline) return undefined;
  const evidenceSince = new Date(Math.min(scheduledFor.getTime(), (followupSent ?? firstSent ?? scheduledFor).getTime()));
  return { rowNumber, phone, callAttemptNumber, agentTimeZone, scheduledFor, deadline, evidenceSince, providerRecovery };
}

export type VoiceProviderEvidence = {
  conversationId: string;
  phone: string;
  attempt?: number;
  startedAt: number;
  accepted: boolean;
};
export type VoiceFallbackEvidence = { since: Date; verifiedAt: Date; calls: VoiceProviderEvidence[] };
const terminalEvidenceCache = new Map<string, VoiceProviderEvidence | null>();

export function parseVoiceProviderEvidence(detail: Record<string, any>): VoiceProviderEvidence | null {
  const vars = detail.conversation_initiation_client_data?.dynamic_variables ?? {};
  if ([true, "true"].includes(vars.testMode) || [true, "true"].includes(vars.providerProofCall)) return null;
  const phone = normalizePhoneToE164(vars.phone ?? detail.phone_call?.external_number ?? detail.metadata?.phone_call?.external_number);
  const startedAt = detail.metadata?.start_time_unix_secs ?? detail.start_time_unix_secs;
  if (!phone || !Number.isFinite(startedAt) || !detail.conversation_id || !detail.status) {
    throw new Error("Provider evidence is incomplete; overdue fallback held");
  }
  return {
    conversationId: detail.conversation_id, phone,
    attempt: Number(vars.callAttemptNumber) || undefined,
    startedAt,
    accepted: detail.status !== "failed" || Boolean(detail.metadata?.accepted_time_unix_secs) ||
      Number(detail.metadata?.call_duration_secs ?? detail.call_duration_secs ?? 0) > 0 ||
      detail.has_audio === true || detail.has_user_audio === true || detail.has_response_audio === true,
  };
}

export function hasAcceptedVoiceFallbackEvidence(plan: VoiceOverdueFallbackPlan, proof: VoiceFallbackEvidence): boolean {
  if (proof.since > plan.evidenceSince) throw new Error("Provider evidence does not cover the overdue attempt");
  return proof.calls.some((call) => call.accepted && call.phone === plan.phone && (
    plan.providerRecovery || !call.attempt || call.attempt >= plan.callAttemptNumber || call.startedAt * 1000 >= plan.scheduledFor.getTime()
  ));
}

export async function scanVoiceFallbackEvidence(
  since: Date,
  client: Pick<AxiosInstance, "get"> = axios.create({
    baseURL: config.elevenLabs.baseUrl, timeout: 30_000,
    headers: { "xi-api-key": config.elevenLabs.apiKey },
  }),
): Promise<VoiceFallbackEvidence> {
  if (!config.elevenLabs.agentId) throw new Error("ElevenLabs agent ID is required for overdue proof");
  const scanStarted = new Date();
  const calls = new Map<string, VoiceProviderEvidence>();
  const read = async (after: number) => {
    let cursor: string | undefined;
    const cursors = new Set<string>();
    for (let page = 0; page < 100; page++) {
      const response = await client.get("/v1/convai/conversations", { params: {
        agent_id: config.elevenLabs.agentId, page_size: 100, cursor,
        call_start_after_unix: after, sort_direction: "desc",
      } });
      const list = response.data;
      if (!Array.isArray(list.conversations) || typeof list.has_more !== "boolean") {
        throw new Error("Invalid provider conversation list; fallback held");
      }
      for (const summary of list.conversations) {
        const id = summary.conversation_id;
        if (!id) throw new Error("Provider conversation missing identity; fallback held");
        let record = terminalEvidenceCache.get(id);
        if (!terminalEvidenceCache.has(id)) {
          const { data: detail } = await client.get(`/v1/convai/conversations/${id}`);
          if (detail.conversation_id !== id) throw new Error("Provider receipt identity mismatch; fallback held");
          record = parseVoiceProviderEvidence(detail);
          if (["done", "failed"].includes(detail.status)) {
            terminalEvidenceCache.set(id, record);
            if (terminalEvidenceCache.size > 5000) terminalEvidenceCache.delete(terminalEvidenceCache.keys().next().value!);
          }
        }
        if (record) calls.set(id, record);
      }
      if (list.has_more !== true) return;
      if (!list.next_cursor || cursors.has(list.next_cursor)) throw new Error("Provider pagination is incomplete; fallback held");
      cursor = list.next_cursor;
      cursors.add(cursor!);
    }
    throw new Error("Provider evidence pagination limit reached; fallback held");
  };
  await read(Math.floor(since.getTime() / 1000) - 10);
  // Include starts accepted while the historical pages were being read.
  await read(Math.floor(scanStarted.getTime() / 1000) - 2);
  return { since, verifiedAt: new Date(), calls: [...calls.values()] };
}

let proof: VoiceFallbackEvidence | undefined;
let scanning: Promise<void> | undefined;
let retryAfter = 0;
let lastError: string | undefined;
let released = 0;

export function getVoiceOverdueFallbackStatus() {
  return {
    enabled: true, businessDays: VOICE_OVERDUE_FALLBACK_BUSINESS_DAYS,
    timeBasis: "listing_local_window_end", businessWeek: "Monday-Friday",
    code: "N", marker: VOICE_OVERDUE_FALLBACK_MARKER,
    providerEvidence: scanning ? "scanning" : lastError ? "held" : proof ? "ready" : "not_needed_yet",
    proofVerifiedAt: proof?.verifiedAt.toISOString() ?? null,
    releasedSinceRestart: released, heldReason: lastError ?? null,
  };
}

export function noteVoiceFallbackRelease(): void { released += 1; }

export function getOrWarmVoiceFallbackEvidence(
  since: Date, now: Date, onReady: () => Promise<unknown>,
): VoiceFallbackEvidence | undefined {
  if (proof && proof.since <= since && now.getTime() - proof.verifiedAt.getTime() <= 30_000) return proof;
  if (!scanning && Date.now() >= retryAfter) {
    scanning = scanVoiceFallbackEvidence(since).then((result) => {
      proof = result;
      lastError = undefined;
    }).catch((error) => {
      lastError = error instanceof Error ? error.message : "Provider evidence unavailable";
      retryAfter = Date.now() + 60_000;
    }).finally(() => {
      scanning = undefined;
      if (!lastError) void onReady().catch((error) => {
        lastError = error instanceof Error ? error.message : "Fallback reconciliation failed";
        logger.error("Overdue voice fallback reconciliation failed", { message: lastError });
      });
    });
  }
  return undefined;
}
