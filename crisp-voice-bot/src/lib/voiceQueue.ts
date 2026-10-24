import axios, { AxiosError } from "axios";
import type { sheets_v4 } from "googleapis";
import { config } from "./config";
import listingTimeZones from "../config/us-city-timezones.json";
import {
  getVoiceOverdueFallbackPlan,
  getOrWarmVoiceFallbackEvidence,
  hasAcceptedVoiceFallbackEvidence,
  noteVoiceFallbackRelease,
  VOICE_OVERDUE_FALLBACK_MARKER,
} from "./voiceOverdueFallback";
import {
  reconcilePendingElevenLabsCallStart,
  recoverAcceptedElevenLabsCallStart,
} from "./elevenLabs";
import { getGoogleSheetsClient } from "./googleSheets";
import { logger } from "./logger";
import { processPostCallOutcomeFromConversationId } from "./elevenLabsPostCall";
import { updateVoiceLeadRow } from "./updateVoiceLeadRow";
import { getPendingVoiceOutcomeRecoveries, isMatchingTerminalVoiceReceipt, voiceOutcomeRecoveryBlockReason } from "./voiceOutcomeRecovery";
import { nextTransportRecoverySlot, transportFailureKind, VOICE_START_RESOLUTION_MARKER } from "./voiceTransportRecovery";
import { getOutboundCallPause } from "./outboundCallPause";
import { ensureProviderCircuitAlert } from "./providerCircuitAlert";
import { ensureProviderStartFailureAlert } from "./providerStartFailureAlert";
import { getProviderCircuitStatus } from "./providerCircuitBreaker";
import {
  ensureFinalReceiptCircuitAlert,
  evaluateFinalReceiptCircuit,
  getInboundQuietGate,
  evaluateInboundQuietGate,
} from "./voiceSafety";
import {
  appendVoiceNotesValue,
  buildVoiceBotListingAddress,
  cellRange,
  columnToLetter,
  formatVoiceBotDateEt,
  getNextVoiceBotFirstAttemptWindowStart,
  getNextVoiceBotFollowupAttemptWindowStart,
  getVoiceBotAgentTimeZone,
  getVoiceBotTimeZoneResolution,
  getVoiceBotPreferredCallWindowName,
  isRetryableVoiceBotResult,
  isWithinVoiceBotQueueRunWindow,
  normalizeMarker,
  normalizePhoneToE164,
  normalizeString,
  parseVoiceBotDate,
  VOICE_BOT_ACTIVE_CALL_STALE_AFTER_MINUTES,
  VOICE_BOT_COL_CALL_1_RESULT,
  VOICE_BOT_COL_CALL_1_SENT,
  VOICE_BOT_COL_CALL_2_RESULT,
  VOICE_BOT_COL_CALL_2_SENT,
  VOICE_BOT_COL_CALL_ELIGIBLE,
  VOICE_BOT_COL_CALL_SCHEDULED_FOR,
  VOICE_BOT_COL_CALL_TIME_BUCKET,
  VOICE_BOT_COL_CALLBACK_REQUESTED,
  VOICE_BOT_COL_CALLBACK_TIME,
  VOICE_BOT_COL_LIVE_TRANSFER_REQUESTED,
  VOICE_BOT_COL_LIVE_TRANSFER_COMPLETED,
  VOICE_BOT_COL_CITY,
  VOICE_BOT_COL_CREATED_AT,
  VOICE_BOT_COL_EMAIL,
  VOICE_BOT_COL_FIRST_NAME,
  VOICE_BOT_COL_FOLLOWUP_SENT_AT_PROXY,
  VOICE_BOT_COL_FOLLOWUP_TEXT_SENT,
  VOICE_BOT_COL_LAST_NAME,
  VOICE_BOT_COL_LEAD_STATUS_CODE,
  VOICE_BOT_COL_LISTING_ADDRESS,
  VOICE_BOT_COL_PHONE,
  VOICE_BOT_COL_RESPONSE_STATUS,
  VOICE_BOT_COL_STATE,
  VOICE_BOT_COL_VOICE_NOTES,
  VOICE_BOT_MAX_ACTIVE_CALLS,
  VOICE_BOT_MAX_CALLS_PER_QUEUE_RUN,
} from "./voiceSheet";
import type { CallMetadata, StartCallRequest } from "../types";

export type VoiceQueueCandidate = {
  rowNumber: number;
  callAttemptNumber: 1 | 2;
  firstName: string;
  lastName: string;
  fullName: string;
  phone: string;
  email?: string;
  listingAddress: string;
  createdAt?: string;
  existingResponseStatus?: string;
  dueAt: Date;
  callWindow: string;
  agentTimeZone: string;
  overdueNoStartRecovery: boolean;
};

export type VoiceQueueResult = {
  ok: true;
  queued: boolean;
  reason?: string;
  dryRun?: boolean;
  queuedCount?: number;
  activeCallCount?: number;
  activeCallCountBeforeRun?: number;
  availableSlots?: number;
  candidateCount?: number;
  eligibleCandidateCount?: number;
  overdueFallbackCandidates?: Array<{ rowNumber: number; callAttemptNumber: number; scheduledFor: string; deadline: string }>;
  timeZoneReviewRequired?: Array<{ rowNumber: number; reason: string }>;
  maxCallsPerRun?: number;
  maxActiveCalls?: number;
  nowEt?: string;
  pausedUntil?: string;
  pauseReason?: string;
  calls?: Array<{
    rowNumber: number;
    callAttemptNumber: 1 | 2;
    dueAtEt: string;
    localWindow: string;
    agentTimeZone: string;
  }>;
  candidates?: Array<{
    rowNumber: number;
    callAttemptNumber: 1 | 2;
    dueAtEt: string;
    localWindow: string;
    agentTimeZone: string;
  }>;
};

type SheetCellWrite = {
  columnNumber: number;
  value: string;
  label?: string;
};

export type VoiceSafeReplacementReadback = {
  leadStatusCode: string;
  callEligible: string;
  callTimeBucket: string;
  callScheduledFor: string;
};

const CALL_START_UNCERTAIN_MARKER = "CODEX_VOICE_CALL_START_UNCERTAIN_V1";
const CALL_START_UNCERTAIN_GRACE_MS = 15 * 60_000;
const SCHEDULED_NO_START_MARKER = "CODEX_VOICE_SCHEDULED_NO_START_V1";
const SCHEDULED_NO_START_OVERDUE_MS = 12 * 60 * 60_000;

export type VoiceScheduledNoStartMarker = {
  rowNumber: number;
  callAttemptNumber: 1 | 2;
  detectedAt: string;
  candidateDueAt: string;
};

export type VoiceScheduledNoStartRecoveryPlan = {
  rowNumber: number;
  callAttemptNumber: 1 | 2;
  originalScheduledFor: Date;
  replacementAt: Date;
  agentTimeZone: string;
  marker: VoiceScheduledNoStartMarker;
};

export function formatVoiceScheduledNoStartMarker(marker: VoiceScheduledNoStartMarker): string {
  return `${SCHEDULED_NO_START_MARKER} ${JSON.stringify(marker)}`;
}

export function parseVoiceScheduledNoStartMarker(value: unknown): VoiceScheduledNoStartMarker | undefined {
  const notes = normalizeString(value);
  const lines = notes.split(/\r?\n/).filter((line) => line.includes(SCHEDULED_NO_START_MARKER));
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    const jsonText = lines[index].slice(lines[index].indexOf(SCHEDULED_NO_START_MARKER) + SCHEDULED_NO_START_MARKER.length).trim();
    try {
      const parsed = JSON.parse(jsonText) as Partial<VoiceScheduledNoStartMarker>;
      if (
        Number.isInteger(parsed.rowNumber) &&
        (parsed.callAttemptNumber === 1 || parsed.callAttemptNumber === 2) &&
        typeof parsed.detectedAt === "string" && !Number.isNaN(Date.parse(parsed.detectedAt)) &&
        typeof parsed.candidateDueAt === "string" && !Number.isNaN(Date.parse(parsed.candidateDueAt))
      ) {
        return parsed as VoiceScheduledNoStartMarker;
      }
    } catch (_) {}
  }
  return undefined;
}

export function getScheduledVoiceNoStartRecoveryPlan(
  rowNumber: number,
  rowValues: unknown[],
  now: Date,
): VoiceScheduledNoStartRecoveryPlan | undefined {
  if (normalizeString(rowValues[VOICE_BOT_COL_VOICE_NOTES - 1]).includes(VOICE_OVERDUE_FALLBACK_MARKER) ||
      getVoiceOverdueFallbackPlan(rowNumber, rowValues, now)) return undefined;
  const leadStatus = normalizeString(rowValues[VOICE_BOT_COL_LEAD_STATUS_CODE - 1]).toUpperCase();
  if (leadStatus && leadStatus !== "N") return undefined;
  if (normalizeMarker(rowValues[VOICE_BOT_COL_FOLLOWUP_TEXT_SENT - 1]) !== "x") return undefined;
  if (normalizeMarker(rowValues[VOICE_BOT_COL_CALL_ELIGIBLE - 1]) !== "yes") return undefined;

  const bucket = normalizeString(rowValues[VOICE_BOT_COL_CALL_TIME_BUCKET - 1]);
  const callAttemptNumber = bucket === "voice_call_1_due" ? 1 : bucket === "voice_call_2_due" ? 2 : undefined;
  if (!callAttemptNumber) return undefined;

  const scheduledFor = parseVoiceBotDate(rowValues[VOICE_BOT_COL_CALL_SCHEDULED_FOR - 1]);
  if (!scheduledFor || now.getTime() - scheduledFor.getTime() < SCHEDULED_NO_START_OVERDUE_MS) return undefined;
  if (config.voiceQueue.scheduledNoStartMonitorStartedAt.getTime() - scheduledFor.getTime() >= SCHEDULED_NO_START_OVERDUE_MS) {
    return undefined;
  }

  const sentColumn = callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_SENT : VOICE_BOT_COL_CALL_1_SENT;
  const resultColumn = callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_RESULT : VOICE_BOT_COL_CALL_1_RESULT;
  if (parseVoiceBotDate(rowValues[sentColumn - 1]) || normalizeString(rowValues[resultColumn - 1])) return undefined;
  const hasHandoff = [VOICE_BOT_COL_CALLBACK_REQUESTED, VOICE_BOT_COL_CALLBACK_TIME,
    VOICE_BOT_COL_LIVE_TRANSFER_REQUESTED, VOICE_BOT_COL_LIVE_TRANSFER_COMPLETED]
    .some((column) => {
      const value = normalizeMarker(rowValues[column - 1]);
      return Boolean(value) && value !== "no" && value !== "false";
    });
  if (hasHandoff) return undefined;

  const agentTimeZone = getVoiceBotAgentTimeZone(rowValues);
  const normalizedPhone = normalizePhoneToE164(rowValues[VOICE_BOT_COL_PHONE - 1]);
  if (!agentTimeZone || !normalizedPhone) return undefined;
  const replacementAt = getNextVoiceBotFirstAttemptWindowStart(
    new Date(now.getTime() + 24 * 60 * 60_000),
    agentTimeZone,
    normalizedPhone,
  );
  const existingMarker = parseVoiceScheduledNoStartMarker(rowValues[VOICE_BOT_COL_VOICE_NOTES - 1]);
  const marker = existingMarker ?? {
    rowNumber,
    callAttemptNumber,
    detectedAt: now.toISOString(),
    candidateDueAt: scheduledFor.toISOString(),
  };
  return { rowNumber, callAttemptNumber, originalScheduledFor: scheduledFor, replacementAt, agentTimeZone, marker };
}

export type VoiceCallStartUncertainMarker = {
  rowNumber: number;
  callAttemptNumber: 1 | 2;
  callStartRequestId: string;
  requestStartedAtUnixSecs: number;
  scheduledWindow?: string;
  agentTimeZone?: string;
};

export function formatVoiceCallStartUncertainMarker(marker: VoiceCallStartUncertainMarker): string {
  return `${CALL_START_UNCERTAIN_MARKER} ${JSON.stringify(marker)}`;
}

export function parseVoiceCallStartUncertainMarker(value: unknown): VoiceCallStartUncertainMarker | undefined {
  const notes = normalizeString(value);
  const lines = notes.split(/\r?\n/).filter((line) => line.includes(CALL_START_UNCERTAIN_MARKER));
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    const jsonText = lines[index].slice(lines[index].indexOf(CALL_START_UNCERTAIN_MARKER) + CALL_START_UNCERTAIN_MARKER.length).trim();
    try {
      const parsed = JSON.parse(jsonText) as Partial<VoiceCallStartUncertainMarker>;
      if (
        Number.isInteger(parsed.rowNumber) &&
        (parsed.callAttemptNumber === 1 || parsed.callAttemptNumber === 2) &&
        typeof parsed.callStartRequestId === "string" && parsed.callStartRequestId &&
        typeof parsed.requestStartedAtUnixSecs === "number" && Number.isFinite(parsed.requestStartedAtUnixSecs)
      ) {
        return parsed as VoiceCallStartUncertainMarker;
      }
    } catch (_) {}
  }
  return undefined;
}

function getCallStartUncertainPayload(error: unknown): Omit<VoiceCallStartUncertainMarker, "rowNumber" | "scheduledWindow" | "agentTimeZone"> | undefined {
  if (!(error instanceof AxiosError)) return undefined;
  const data = error.response?.data as Record<string, unknown> | undefined;
  if (
    data?.callStartUncertain !== true ||
    typeof data.callStartRequestId !== "string" || !data.callStartRequestId ||
    typeof data.requestStartedAtUnixSecs !== "number" || !Number.isFinite(data.requestStartedAtUnixSecs) ||
    (data.callAttemptNumber !== 1 && data.callAttemptNumber !== 2)
  ) {
    return undefined;
  }
  return {
    callStartRequestId: data.callStartRequestId,
    requestStartedAtUnixSecs: data.requestStartedAtUnixSecs,
    callAttemptNumber: data.callAttemptNumber,
  };
}

let activeQueueRun: Promise<VoiceQueueResult> | undefined;
let schedulerTimer: NodeJS.Timeout | undefined;
export const VOICE_QUEUE_MAX_PARALLEL_STARTS = 2;

export async function dispatchVoiceQueueBatch<T>(
  candidates: T[],
  dispatch: (candidate: T) => Promise<void>,
  maxParallelStarts = VOICE_QUEUE_MAX_PARALLEL_STARTS,
): Promise<void> {
  let index = 0;
  let stopped = false;
  const workers = Math.min(candidates.length, Math.max(1, Math.floor(maxParallelStarts) || 1));
  // Wait for every worker even if persistence fails, so the single-flight lock
  // cannot release while another accepted/uncertain start is still settling.
  const results = await Promise.allSettled(Array.from({ length: workers }, async () => {
    while (!stopped && index < candidates.length) {
      try {
        await dispatch(candidates[index++]);
      } catch (error) {
        stopped = true;
        throw error;
      }
    }
  }));
  const failed = results.find((result) => result.status === "rejected");
  if (failed?.status === "rejected") throw failed.reason;
}

export function hasVoiceQueueDispatchHeadroom(candidate: VoiceQueueCandidate, now: Date): boolean {
  return getVoiceBotPreferredCallWindowName(now, candidate.agentTimeZone) === candidate.callWindow &&
    getVoiceBotPreferredCallWindowName(new Date(now.getTime() + 60_000), candidate.agentTimeZone) === candidate.callWindow;
}

export function countAllocatedVoiceBotSlots(
  rows: Array<{ rowNumber: number; values: unknown[] }>,
  now: Date,
  reservations: VoiceQueueCandidate[],
): number {
  return countActiveVoiceBotCalls(rows, now) + reservations.filter((candidate) => {
    const values = rows.find((row) => row.rowNumber === candidate.rowNumber)?.values;
    const sentColumn = candidate.callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_SENT : VOICE_BOT_COL_CALL_1_SENT;
    return !parseVoiceBotDate(values?.[sentColumn - 1]);
  }).length;
}

function candidateSummary(candidate: VoiceQueueCandidate) {
  return {
    rowNumber: candidate.rowNumber,
    callAttemptNumber: candidate.callAttemptNumber,
    dueAtEt: formatVoiceBotDateEt(candidate.dueAt),
    localWindow: candidate.callWindow,
    agentTimeZone: candidate.agentTimeZone,
  };
}

async function getVoiceBotRows(sheets: sheets_v4.Sheets): Promise<Array<{ rowNumber: number; values: unknown[] }>> {
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: config.googleSheets.sheetId,
    range: `'${config.googleSheets.tabName.replace(/'/g, "''")}'!A2:AP`,
    valueRenderOption: "UNFORMATTED_VALUE",
  });
  const values = response.data.values ?? [];

  return values.map((row, index) => ({
    rowNumber: index + 2,
    values: row,
  }));
}

async function getVoiceBotRowByNumber(sheets: sheets_v4.Sheets, rowNumber: number): Promise<unknown[]> {
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: config.googleSheets.sheetId,
    range: `'${config.googleSheets.tabName.replace(/'/g, "''")}'!A${rowNumber}:AP${rowNumber}`,
    valueRenderOption: "UNFORMATTED_VALUE",
  });

  return response.data.values?.[0] ?? [];
}

function isVoiceBotAttemptActivelyCalling(sentAtValue: unknown, resultValue: unknown, now: Date): boolean {
  const sentAt = parseVoiceBotDate(sentAtValue);
  const normalizedResult = normalizeString(resultValue).toLowerCase();
  const resultStillInProgress = !normalizedResult || normalizedResult === "live_transfer_requested";

  if (!sentAt || !resultStillInProgress) {
    return false;
  }

  const ageMinutes = (now.getTime() - sentAt.getTime()) / 60_000;
  return ageMinutes >= 0 && ageMinutes <= VOICE_BOT_ACTIVE_CALL_STALE_AFTER_MINUTES;
}

export function isStaleVoiceBotStartWithoutReceipt(
  sentAtValue: unknown,
  resultValue: unknown,
  now: Date,
): boolean {
  const sentAt = parseVoiceBotDate(sentAtValue);
  if (!sentAt || normalizeString(resultValue)) return false;
  const ageMinutes = (now.getTime() - sentAt.getTime()) / 60_000;
  return ageMinutes > VOICE_BOT_ACTIVE_CALL_STALE_AFTER_MINUTES;
}

function isVoiceBotRowActivelyCalling(rowValues: unknown[], now: Date): boolean {
  if (normalizeString(rowValues[VOICE_BOT_COL_LEAD_STATUS_CODE - 1])) {
    return false;
  }

  return (
    isVoiceBotAttemptActivelyCalling(
      rowValues[VOICE_BOT_COL_CALL_1_SENT - 1],
      rowValues[VOICE_BOT_COL_CALL_1_RESULT - 1],
      now,
    ) ||
    isVoiceBotAttemptActivelyCalling(
      rowValues[VOICE_BOT_COL_CALL_2_SENT - 1],
      rowValues[VOICE_BOT_COL_CALL_2_RESULT - 1],
      now,
    )
  );
}

function getDueAtCallWindowName(dueAt: Date, agentTimeZone: string): string {
  return getVoiceBotPreferredCallWindowName(dueAt, agentTimeZone);
}

function countActiveVoiceBotCalls(rows: Array<{ values: unknown[] }>, now: Date): number {
  return rows.filter((row) => isVoiceBotRowActivelyCalling(row.values, now)).length;
}

function buildVoiceBotCandidate(
  rowNumber: number,
  rowValues: unknown[],
  callAttemptNumber: 1 | 2,
  dueAt: Date,
  callWindow: string,
  agentTimeZone: string,
  overdueNoStartRecovery: boolean,
): VoiceQueueCandidate | undefined {
  const firstName = normalizeString(rowValues[VOICE_BOT_COL_FIRST_NAME - 1]);
  const lastName = normalizeString(rowValues[VOICE_BOT_COL_LAST_NAME - 1]);
  const phone = normalizePhoneToE164(rowValues[VOICE_BOT_COL_PHONE - 1]);
  const listingAddress = buildVoiceBotListingAddress(rowValues);

  if (!firstName || !phone || !listingAddress) {
    logger.info("Voice queue row skipped: missing required values", {
      rowNumber,
      callAttemptNumber,
      hasFirstName: Boolean(firstName),
      hasPhone: Boolean(phone),
      hasListingAddress: Boolean(listingAddress),
    });
    return undefined;
  }

  const email = normalizeString(rowValues[VOICE_BOT_COL_EMAIL - 1]);
  const createdAt = normalizeString(rowValues[VOICE_BOT_COL_CREATED_AT - 1]);
  const existingResponseStatus = normalizeString(rowValues[VOICE_BOT_COL_RESPONSE_STATUS - 1]);

  return {
    rowNumber,
    callAttemptNumber,
    firstName,
    lastName,
    fullName: [firstName, lastName].filter(Boolean).join(" "),
    phone,
    email: email || undefined,
    listingAddress,
    createdAt: createdAt || undefined,
    existingResponseStatus: existingResponseStatus || undefined,
    dueAt,
    callWindow,
    agentTimeZone,
    overdueNoStartRecovery,
  };
}

export function getVoiceBotCallCandidateFromRowValues(
  rowNumber: number,
  rowValues: unknown[],
  now: Date,
): VoiceQueueCandidate | undefined {
  if (normalizeString(rowValues[VOICE_BOT_COL_VOICE_NOTES - 1]).includes(VOICE_OVERDUE_FALLBACK_MARKER) ||
      getVoiceOverdueFallbackPlan(rowNumber, rowValues, now)) return undefined;
  if (normalizeString(rowValues[VOICE_BOT_COL_LEAD_STATUS_CODE - 1])) {
    return undefined;
  }

  if (normalizeMarker(rowValues[VOICE_BOT_COL_FOLLOWUP_TEXT_SENT - 1]) !== "x") {
    return undefined;
  }

  const firstAttemptSentAt = parseVoiceBotDate(rowValues[VOICE_BOT_COL_CALL_1_SENT - 1]);
  const secondAttemptSentAt = parseVoiceBotDate(rowValues[VOICE_BOT_COL_CALL_2_SENT - 1]);
  const firstAttemptResult = normalizeString(rowValues[VOICE_BOT_COL_CALL_1_RESULT - 1]);
  // An old timestamp without a final receipt is not proof the call failed.
  if (firstAttemptSentAt && ["", "call_start_reconciled", "call_start_receipt_missing"].includes(firstAttemptResult)) return undefined;
  const scheduledFor = parseVoiceBotDate(rowValues[VOICE_BOT_COL_CALL_SCHEDULED_FOR - 1]);
  const providerStartRecovery = firstAttemptResult === "call_start_failed";
  const hasHandoff = [VOICE_BOT_COL_CALLBACK_REQUESTED, VOICE_BOT_COL_CALLBACK_TIME,
    VOICE_BOT_COL_LIVE_TRANSFER_REQUESTED, VOICE_BOT_COL_LIVE_TRANSFER_COMPLETED]
    .some((column) => {
      const value = normalizeMarker(rowValues[column - 1]);
      return Boolean(value) && value !== "no" && value !== "false";
    });
  if (firstAttemptSentAt && hasHandoff) return undefined;
  // Only replace an automatically generated cold retry date. Explicit dates
  // outside this lane and the historical no-start freeze remain authoritative.
  const automaticColdRetry = Boolean(firstAttemptSentAt && !providerStartRecovery &&
    normalizeString(rowValues[VOICE_BOT_COL_CALL_TIME_BUCKET - 1]) === "voice_call_2_due" &&
    normalizeMarker(rowValues[VOICE_BOT_COL_CALL_ELIGIBLE - 1]) === "yes" &&
    isRetryableVoiceBotResult(firstAttemptResult));
  const agentTimeZone = getVoiceBotAgentTimeZone(rowValues);
  const normalizedPhone = normalizePhoneToE164(rowValues[VOICE_BOT_COL_PHONE - 1]);
  if (!agentTimeZone || !normalizedPhone) return undefined;
  const currentWindow = getVoiceBotPreferredCallWindowName(now, agentTimeZone);
  const overdueNoStartRecovery = Boolean(
    scheduledFor && now.getTime() - scheduledFor.getTime() >= SCHEDULED_NO_START_OVERDUE_MS,
  );
  const wasAlreadyOverdueBeforeMonitor = Boolean(
    scheduledFor &&
    config.voiceQueue.scheduledNoStartMonitorStartedAt.getTime() - scheduledFor.getTime() >= SCHEDULED_NO_START_OVERDUE_MS,
  );

  if (overdueNoStartRecovery && wasAlreadyOverdueBeforeMonitor) {
    return undefined;
  }

  if (!currentWindow) {
    return undefined;
  }

  if (scheduledFor && now < scheduledFor && !automaticColdRetry) {
    return undefined;
  }

  if (!firstAttemptSentAt) {
    const followupSentAt = parseVoiceBotDate(rowValues[VOICE_BOT_COL_FOLLOWUP_SENT_AT_PROXY - 1]);
    if (!followupSentAt) {
      return undefined;
    }

    const dueAt = getNextVoiceBotFirstAttemptWindowStart(followupSentAt, agentTimeZone, normalizedPhone);
    const candidateDueAt = getNextVoiceBotFirstAttemptWindowStart(
      scheduledFor && scheduledFor > dueAt ? scheduledFor : dueAt, agentTimeZone, normalizedPhone,
    );
    if (candidateDueAt < config.voiceQueue.minCandidateDueAt) {
      return undefined;
    }

    const candidateWindow = getDueAtCallWindowName(candidateDueAt, agentTimeZone);
    if (!candidateWindow || currentWindow !== candidateWindow) {
      return undefined;
    }

    if (now < dueAt) {
      return undefined;
    }

    return buildVoiceBotCandidate(
      rowNumber,
      rowValues,
      1,
      candidateDueAt,
      candidateWindow,
      agentTimeZone,
      overdueNoStartRecovery,
    );
  }

  if (secondAttemptSentAt || !isRetryableVoiceBotResult(firstAttemptResult)) {
    return undefined;
  }

  const recoveryDays = providerStartRecovery ? 1 : 0;
  const nextAttemptAt = getNextVoiceBotFollowupAttemptWindowStart(firstAttemptSentAt, agentTimeZone, recoveryDays);
  const candidateDueAt = getNextVoiceBotFirstAttemptWindowStart(
    !automaticColdRetry && scheduledFor && scheduledFor > nextAttemptAt ? scheduledFor : nextAttemptAt,
    agentTimeZone, normalizedPhone, getDueAtCallWindowName(nextAttemptAt, agentTimeZone),
  );
  if (candidateDueAt < config.voiceQueue.minCandidateDueAt) {
    return undefined;
  }

  const candidateWindow = getDueAtCallWindowName(candidateDueAt, agentTimeZone);
  if (!candidateWindow || currentWindow !== candidateWindow) {
    return undefined;
  }

  if (now < nextAttemptAt) {
    return undefined;
  }

  return buildVoiceBotCandidate(
    rowNumber,
    rowValues,
    2,
    candidateDueAt,
    candidateWindow,
    agentTimeZone,
    overdueNoStartRecovery,
  );
}

export function getVoiceBotCallCandidatesFromRows(
  rows: Array<{ rowNumber: number; values: unknown[] }>,
  now: Date,
  maxCandidates: number,
): VoiceQueueCandidate[] {
  const limit = Math.max(1, Number(maxCandidates) || 1);
  const candidates: VoiceQueueCandidate[] = [];

  for (const row of rows) {
    const candidate = getVoiceBotCallCandidateFromRowValues(row.rowNumber, row.values, now);
    if (!candidate || isVoiceBotPhoneBlocked(candidate, rows, now)) {
      continue;
    }

    candidates.push(candidate);
  }

  const seenPhones = new Set<string>();
  return candidates
    .sort((left, right) => {
      const recoveryOrder = Number(right.overdueNoStartRecovery) - Number(left.overdueNoStartRecovery);
      if (recoveryOrder !== 0) {
        return recoveryOrder;
      }

      const attemptOrder = left.callAttemptNumber - right.callAttemptNumber;
      if (attemptOrder !== 0) {
        return attemptOrder;
      }

      const dueOrder = left.dueAt.getTime() - right.dueAt.getTime();
      return dueOrder !== 0 ? dueOrder : left.rowNumber - right.rowNumber;
    })
    .filter((candidate) => {
      if (seenPhones.has(candidate.phone)) return false;
      seenPhones.add(candidate.phone);
      return true;
    })
    .slice(0, limit);
}

export function isVoiceBotPhoneBlocked(
  candidate: VoiceQueueCandidate,
  rows: Array<{ rowNumber: number; values: unknown[] }>,
  now: Date,
): boolean {
  let acceptedAttempts = 0;
  for (const row of rows) {
    if (normalizePhoneToE164(row.values[VOICE_BOT_COL_PHONE - 1]) !== candidate.phone) continue;
    if (normalizeString(row.values[VOICE_BOT_COL_LEAD_STATUS_CODE - 1])) return true;
    if (isVoiceBotRowActivelyCalling(row.values, now)) return true;
    for (const [sentColumn, resultColumn] of [
      [VOICE_BOT_COL_CALL_1_SENT, VOICE_BOT_COL_CALL_1_RESULT],
      [VOICE_BOT_COL_CALL_2_SENT, VOICE_BOT_COL_CALL_2_RESULT],
    ]) {
      const result = normalizeString(row.values[resultColumn - 1]).toLowerCase();
      if (result && !isRetryableVoiceBotResult(result)) return true;
      if (!parseVoiceBotDate(row.values[sentColumn - 1])) continue;
      // Another listing must not restart a phone's already-established cadence.
      if (row.rowNumber !== candidate.rowNumber) return true;
      if (result !== "call_start_failed" && result !== "call_start_receipt_missing") acceptedAttempts += 1;
    }
  }
  return acceptedAttempts >= 2;
}

export function getVoiceBotStartableCallCandidatesFromRows(
  rows: Array<{ rowNumber: number; values: unknown[] }>,
  now: Date,
  maxCandidates: number,
  maxActiveCalls: number,
): VoiceQueueCandidate[] {
  const activeCallCount = countActiveVoiceBotCalls(rows, now);
  const activeLimit = Math.max(1, Number(maxActiveCalls) || 1);
  const availableSlots = Math.max(0, activeLimit - activeCallCount);

  if (availableSlots <= 0) {
    return [];
  }

  const candidateLimit = Math.min(Math.max(1, Number(maxCandidates) || 1), availableSlots);
  return getVoiceBotCallCandidatesFromRows(rows, now, candidateLimit);
}

function buildStartCallPayload(candidate: VoiceQueueCandidate): StartCallRequest {
  return {
    rowNumber: candidate.rowNumber,
    callAttemptNumber: candidate.callAttemptNumber,
    firstName: candidate.firstName,
    lastName: candidate.lastName,
    fullName: candidate.fullName,
    phone: candidate.phone,
    email: candidate.email,
    listingAddress: candidate.listingAddress,
    createdAt: candidate.createdAt,
    scheduledForEt: formatVoiceBotDateEt(candidate.dueAt),
    scheduledWindow: candidate.callWindow,
    agentTimeZone: candidate.agentTimeZone,
    responseStatus: candidate.existingResponseStatus,
    sheetName: config.googleSheets.tabName,
  };
}

async function writeCells(sheets: sheets_v4.Sheets, rowNumber: number, writes: SheetCellWrite[]): Promise<void> {
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: config.googleSheets.sheetId,
    requestBody: {
      valueInputOption: "USER_ENTERED",
      data: writes.map((write) => ({
        range: cellRange(config.googleSheets.tabName, columnToLetter(write.columnNumber), rowNumber),
        values: [[write.value]],
      })),
    },
  });
}

export async function reconcileOverdueVoiceMailshakeFallback(
  sheets: sheets_v4.Sheets,
  rows: Array<{ rowNumber: number; values: unknown[] }>,
  now: Date,
  dependencies: { getEvidence?: typeof getOrWarmVoiceFallbackEvidence; clock?: () => Date } = {},
): Promise<number> {
  const clock = dependencies.clock ?? (() => new Date());
  const plans = rows.map((row) => getVoiceOverdueFallbackPlan(row.rowNumber, row.values, now))
    .filter((plan): plan is NonNullable<typeof plan> => Boolean(plan));
  if (!plans.length) return 0;
  const since = new Date(Math.min(...plans.map((plan) => plan.evidenceSince.getTime())));
  // Warm the provider index in the background: it must not hold the dispatch
  // single-flight lock or turn receipt verification into a new queue bottleneck.
  const proof = (dependencies.getEvidence ?? getOrWarmVoiceFallbackEvidence)(since, now, () =>
    processVoiceQueue({ fallbackOnly: true, refill: true }));
  if (!proof) return 0;
  const inbound = await sheets.spreadsheets.values.get({
    spreadsheetId: config.googleSheets.sheetId, range: "'sms_inbound_queue'!A2:H",
  });
  let released = 0;
  for (const oldPlan of plans) {
    if (released >= 20) break;
    if (clock().getTime() - proof.verifiedAt.getTime() > 30_000) break;
    const values = await getVoiceBotRowByNumber(sheets, oldPlan.rowNumber);
    const checkNow = clock();
    const plan = getVoiceOverdueFallbackPlan(oldPlan.rowNumber, values, checkNow);
    if (!plan || plan.phone !== oldPlan.phone || plan.callAttemptNumber !== oldPlan.callAttemptNumber ||
        plan.scheduledFor.getTime() !== oldPlan.scheduledFor.getTime()) continue;
    const duplicate = rows.some((row) => row.rowNumber !== plan.rowNumber &&
      normalizePhoneToE164(row.values[VOICE_BOT_COL_PHONE - 1]) === plan.phone);
    if (duplicate || evaluateInboundQuietGate(plan.phone, (inbound.data.values ?? []).slice(-500), checkNow).blocked ||
        hasAcceptedVoiceFallbackEvidence(plan, proof)) continue;
    const notes = appendVoiceNotesValue(values[VOICE_BOT_COL_VOICE_NOTES - 1],
      `${VOICE_OVERDUE_FALLBACK_MARKER} ${JSON.stringify({
        rowNumber: plan.rowNumber, phone: plan.phone, callAttemptNumber: plan.callAttemptNumber,
        scheduledFor: plan.scheduledFor.toISOString(), deadline: plan.deadline.toISOString(),
        agentTimeZone: plan.agentTimeZone, releasedAt: checkNow.toISOString(),
        providerEvidenceSince: proof.since.toISOString(), providerEvidenceVerifiedAt: proof.verifiedAt.toISOString(),
      })}\nScheduled voice attempt was not placed within two listing-local business days after its window; released to Mailshake with K=N. No call outcome or sent timestamp was fabricated.`);
    await writeCells(sheets, plan.rowNumber, [
      { columnNumber: VOICE_BOT_COL_LEAD_STATUS_CODE, value: "N" },
      { columnNumber: VOICE_BOT_COL_CALL_ELIGIBLE, value: "" },
      { columnNumber: VOICE_BOT_COL_CALL_TIME_BUCKET, value: "" },
      { columnNumber: VOICE_BOT_COL_CALL_SCHEDULED_FOR, value: "" },
      { columnNumber: VOICE_BOT_COL_VOICE_NOTES, value: notes },
    ]);
    const after = await getVoiceBotRowByNumber(sheets, plan.rowNumber);
    if (normalizePhoneToE164(after[2]) !== plan.phone || normalizeString(after[10]) !== "N" ||
        [29, 30, 31].some((index) => normalizeString(after[index])) ||
        !normalizeString(after[41]).includes(VOICE_OVERDUE_FALLBACK_MARKER) ||
        [32, 33, 39, 40].some((index) => normalizeString(after[index]) !== normalizeString(values[index]))) {
      throw new Error(`Overdue voice fallback readback mismatch for row ${plan.rowNumber}`);
    }
    const localRow = rows.find((row) => row.rowNumber === plan.rowNumber);
    if (localRow) localRow.values = after;
    noteVoiceFallbackRelease();
    released += 1;
    logger.info("Overdue voice attempt released to Mailshake", {
      rowNumber: plan.rowNumber, callAttemptNumber: plan.callAttemptNumber,
      scheduledFor: plan.scheduledFor.toISOString(), deadline: plan.deadline.toISOString(),
      agentTimeZone: plan.agentTimeZone,
    });
  }
  return released;
}

export function buildVoiceSafeReplacementWrites(
  callAttemptNumber: 1 | 2,
  replacementAt: Date,
  voiceNotes: string,
): SheetCellWrite[] {
  return [
    { columnNumber: callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_SENT : VOICE_BOT_COL_CALL_1_SENT, value: "", label: `voice_call_${callAttemptNumber}_sent_released` },
    { columnNumber: callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_RESULT : VOICE_BOT_COL_CALL_1_RESULT, value: "", label: `voice_call_${callAttemptNumber}_result_released` },
    { columnNumber: VOICE_BOT_COL_RESPONSE_STATUS, value: "No accepted provider receipt; replacement attempt safely scheduled", label: "response_status" },
    { columnNumber: VOICE_BOT_COL_LEAD_STATUS_CODE, value: "", label: "leadStatusCode_replacement_cleared" },
    { columnNumber: VOICE_BOT_COL_CALL_ELIGIBLE, value: "yes", label: "call_eligible" },
    { columnNumber: VOICE_BOT_COL_CALL_TIME_BUCKET, value: `voice_call_${callAttemptNumber}_due`, label: "call_time_bucket" },
    { columnNumber: VOICE_BOT_COL_CALL_SCHEDULED_FOR, value: replacementAt.toISOString(), label: "call_scheduled_for" },
    {
      columnNumber: VOICE_BOT_COL_VOICE_NOTES,
      value: voiceNotes,
      label: "voiceNotes_absence_proved",
    },
  ];
}

export async function readVoiceSafeReplacementState(
  sheets: sheets_v4.Sheets,
  rowNumber: number,
): Promise<VoiceSafeReplacementReadback> {
  const columns = [
    VOICE_BOT_COL_LEAD_STATUS_CODE,
    VOICE_BOT_COL_CALL_ELIGIBLE,
    VOICE_BOT_COL_CALL_TIME_BUCKET,
    VOICE_BOT_COL_CALL_SCHEDULED_FOR,
  ];
  const response = await sheets.spreadsheets.values.batchGet({
    spreadsheetId: config.googleSheets.sheetId,
    ranges: columns.map((columnNumber) =>
      cellRange(config.googleSheets.tabName, columnToLetter(columnNumber), rowNumber),
    ),
    valueRenderOption: "UNFORMATTED_VALUE",
  });
  const values = (response.data.valueRanges ?? []).map((range) =>
    normalizeString(range.values?.[0]?.[0]),
  );
  return {
    leadStatusCode: values[0] ?? "",
    callEligible: values[1] ?? "",
    callTimeBucket: values[2] ?? "",
    callScheduledFor: values[3] ?? "",
  };
}

export async function verifyVoiceSafeReplacementReadback(
  sheets: sheets_v4.Sheets,
  rowNumber: number,
  callAttemptNumber: 1 | 2,
  replacementAt: Date,
): Promise<VoiceSafeReplacementReadback> {
  const actual = await readVoiceSafeReplacementState(sheets, rowNumber);
  const expected: VoiceSafeReplacementReadback = {
    leadStatusCode: "",
    callEligible: "yes",
    callTimeBucket: `voice_call_${callAttemptNumber}_due`,
    callScheduledFor: replacementAt.toISOString(),
  };
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`Safe replacement readback mismatch for row ${rowNumber}: ${JSON.stringify({ expected, actual })}`);
  }
  return actual;
}

export async function reconcileScheduledVoiceNoStarts(
  sheets: sheets_v4.Sheets,
  rows: Array<{ rowNumber: number; values: unknown[] }>,
  now: Date,
): Promise<number> {
  let recovered = 0;
  for (const row of rows) {
    const plan = getScheduledVoiceNoStartRecoveryPlan(row.rowNumber, row.values, now);
    if (!plan) continue;
    const markerLine = formatVoiceScheduledNoStartMarker(plan.marker);
    const notes = appendVoiceNotesValue(
      row.values[VOICE_BOT_COL_VOICE_NOTES - 1],
      `${markerLine}\nScheduled attempt had no start or provider receipt; safely carried forward from ${plan.originalScheduledFor.toISOString()} to ${plan.replacementAt.toISOString()}.`,
    );
    await writeCells(
      sheets,
      row.rowNumber,
      buildVoiceSafeReplacementWrites(plan.callAttemptNumber, plan.replacementAt, notes),
    );
    await verifyVoiceSafeReplacementReadback(
      sheets,
      row.rowNumber,
      plan.callAttemptNumber,
      plan.replacementAt,
    );
    recovered += 1;
    logger.warn("Scheduled voice call had no start receipt; replacement safely scheduled", {
      rowNumber: row.rowNumber,
      callAttemptNumber: plan.callAttemptNumber,
      originalScheduledFor: plan.originalScheduledFor.toISOString(),
      replacementAt: plan.replacementAt.toISOString(),
      agentTimeZone: plan.agentTimeZone,
    });
  }
  return recovered;
}

async function markVoiceBotAttemptStarted(
  sheets: sheets_v4.Sheets,
  rowValues: unknown[],
  candidate: VoiceQueueCandidate,
  now: Date,
): Promise<void> {
  const sentColumn = candidate.callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_SENT : VOICE_BOT_COL_CALL_1_SENT;
  const timeBucket = candidate.callAttemptNumber === 2 ? "voice_call_2_due" : "voice_call_1_due";

  const writes: SheetCellWrite[] = [
    { columnNumber: sentColumn, value: now.toISOString() },
    { columnNumber: VOICE_BOT_COL_CALL_ELIGIBLE, value: "queued" },
    { columnNumber: VOICE_BOT_COL_CALL_TIME_BUCKET, value: timeBucket },
    { columnNumber: VOICE_BOT_COL_CALL_SCHEDULED_FOR, value: candidate.dueAt.toISOString() },
  ];
  if (
    candidate.callAttemptNumber === 2 &&
    isStaleVoiceBotStartWithoutReceipt(
      rowValues[VOICE_BOT_COL_CALL_1_SENT - 1],
      rowValues[VOICE_BOT_COL_CALL_1_RESULT - 1],
      now,
    )
  ) {
    writes.push(
      { columnNumber: VOICE_BOT_COL_CALL_1_RESULT, value: "call_start_receipt_missing" },
      { columnNumber: VOICE_BOT_COL_RESPONSE_STATUS, value: "Prior call start had no final receipt; one bounded retry started" },
      {
        columnNumber: VOICE_BOT_COL_VOICE_NOTES,
        value: appendVoiceNotesValue(
          rowValues[VOICE_BOT_COL_VOICE_NOTES - 1],
          `Prior call start became stale without a final provider or transcript receipt; bounded retry started at ${formatVoiceBotDateEt(now)}.`,
        ),
      },
    );
  }
  await writeCells(sheets, candidate.rowNumber, writes);
}

async function markScheduledNoStartRecoveryDetected(
  sheets: sheets_v4.Sheets,
  rowValues: unknown[],
  candidate: VoiceQueueCandidate,
  now: Date,
): Promise<void> {
  if (!candidate.overdueNoStartRecovery || parseVoiceScheduledNoStartMarker(rowValues[VOICE_BOT_COL_VOICE_NOTES - 1])) {
    return;
  }
  const marker = formatVoiceScheduledNoStartMarker({
    rowNumber: candidate.rowNumber,
    callAttemptNumber: candidate.callAttemptNumber,
    detectedAt: now.toISOString(),
    candidateDueAt: candidate.dueAt.toISOString(),
  });
  await writeCells(sheets, candidate.rowNumber, [
    {
      columnNumber: VOICE_BOT_COL_RESPONSE_STATUS,
      value: "Scheduled call start was missed; bounded carry-forward pending",
      label: "scheduled_no_start_response_status",
    },
    {
      columnNumber: VOICE_BOT_COL_VOICE_NOTES,
      value: appendVoiceNotesValue(rowValues[VOICE_BOT_COL_VOICE_NOTES - 1], marker),
      label: "scheduled_no_start_marker",
    },
  ]);
}

async function markVoiceBotAttemptStartFailed(
  sheets: sheets_v4.Sheets,
  rowValues: unknown[],
  candidate: VoiceQueueCandidate,
  now: Date,
  error: unknown,
): Promise<string[]> {
  const sentColumn = candidate.callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_SENT : VOICE_BOT_COL_CALL_1_SENT;
  const resultColumn = candidate.callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_RESULT : VOICE_BOT_COL_CALL_1_RESULT;
  const errorMessage = error instanceof Error ? error.message : String(error);
  const voiceNotes =
    `Voice call start failed before connecting at ${formatVoiceBotDateEt(now)}: ` +
    normalizeString(errorMessage).slice(0, 500);
  const recoveryAt = candidate.callAttemptNumber === 1
    ? getNextVoiceBotFollowupAttemptWindowStart(now, candidate.agentTimeZone, 1)
    : undefined;
  const writes = [
    { columnNumber: sentColumn, value: now.toISOString(), label: `voice_call_${candidate.callAttemptNumber}_sent` },
    { columnNumber: resultColumn, value: "call_start_failed", label: `voice_call_${candidate.callAttemptNumber}_result` },
    { columnNumber: VOICE_BOT_COL_RESPONSE_STATUS, value: "Call start failed before connecting", label: "response_status" },
    { columnNumber: VOICE_BOT_COL_LEAD_STATUS_CODE, value: "", label: "leadStatusCode_retry_cleared" },
    { columnNumber: VOICE_BOT_COL_CALL_ELIGIBLE, value: recoveryAt ? "yes" : "", label: "call_eligible" },
    { columnNumber: VOICE_BOT_COL_CALL_TIME_BUCKET, value: recoveryAt ? "voice_call_2_due" : "", label: "call_time_bucket" },
    { columnNumber: VOICE_BOT_COL_CALL_SCHEDULED_FOR, value: recoveryAt ? recoveryAt.toISOString() : "", label: "call_scheduled_for" },
    {
      columnNumber: VOICE_BOT_COL_VOICE_NOTES,
      value: appendVoiceNotesValue(rowValues[VOICE_BOT_COL_VOICE_NOTES - 1], voiceNotes),
      label: "voiceNotes_appended",
    },
  ];

  await writeCells(sheets, candidate.rowNumber, writes);
  return writes.map((write) => `${columnToLetter(write.columnNumber)}:${write.label}`);
}

async function markVoiceBotAttemptStartUncertain(
  sheets: sheets_v4.Sheets,
  rowValues: unknown[],
  candidate: VoiceQueueCandidate,
  now: Date,
  marker: VoiceCallStartUncertainMarker,
): Promise<string[]> {
  const sentColumn = candidate.callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_SENT : VOICE_BOT_COL_CALL_1_SENT;
  const resultColumn = candidate.callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_RESULT : VOICE_BOT_COL_CALL_1_RESULT;
  const writes = [
    { columnNumber: sentColumn, value: now.toISOString(), label: `voice_call_${candidate.callAttemptNumber}_sent` },
    { columnNumber: resultColumn, value: "call_start_uncertain", label: `voice_call_${candidate.callAttemptNumber}_result` },
    { columnNumber: VOICE_BOT_COL_RESPONSE_STATUS, value: "Call start delivery uncertain; provider receipt reconciliation pending", label: "response_status" },
    { columnNumber: VOICE_BOT_COL_LEAD_STATUS_CODE, value: "", label: "leadStatusCode_retry_cleared" },
    { columnNumber: VOICE_BOT_COL_CALL_ELIGIBLE, value: "", label: "call_eligible_paused" },
    { columnNumber: VOICE_BOT_COL_CALL_TIME_BUCKET, value: "", label: "call_time_bucket_paused" },
    { columnNumber: VOICE_BOT_COL_CALL_SCHEDULED_FOR, value: "", label: "call_scheduled_for_paused" },
    {
      columnNumber: VOICE_BOT_COL_VOICE_NOTES,
      value: appendVoiceNotesValue(rowValues[VOICE_BOT_COL_VOICE_NOTES - 1], formatVoiceCallStartUncertainMarker(marker)),
      label: "voiceNotes_uncertain_marker_appended",
    },
  ];
  await writeCells(sheets, candidate.rowNumber, writes);
  return writes.map((write) => `${columnToLetter(write.columnNumber)}:${write.label}`);
}

function buildRecoveredCallMetadata(
  rowNumber: number,
  rowValues: unknown[],
  marker: VoiceCallStartUncertainMarker,
) {
  const firstName = normalizeString(rowValues[VOICE_BOT_COL_FIRST_NAME - 1]);
  const lastName = normalizeString(rowValues[VOICE_BOT_COL_LAST_NAME - 1]);
  const phone = normalizePhoneToE164(rowValues[VOICE_BOT_COL_PHONE - 1]);
  return {
    rowNumber,
    firstName: firstName || undefined,
    lastName: lastName || undefined,
    fullName: [firstName, lastName].filter(Boolean).join(" "),
    email: normalizeString(rowValues[VOICE_BOT_COL_EMAIL - 1]) || undefined,
    callAttemptNumber: marker.callAttemptNumber,
    listingAddress: buildVoiceBotListingAddress(rowValues),
    sheetName: config.googleSheets.tabName,
    scheduledWindow: marker.scheduledWindow,
    agentTimeZone: marker.agentTimeZone || getVoiceBotAgentTimeZone(rowValues),
    requestedPhone: phone,
    dialedPhone: phone,
    testMode: config.testMode,
    callStartRequestId: marker.callStartRequestId,
  };
}

export async function reconcileUncertainVoiceCallStarts(
  sheets: sheets_v4.Sheets,
  rows: Array<{ rowNumber: number; values: unknown[] }>,
  now: Date,
  resolveReceipt: typeof reconcilePendingElevenLabsCallStart = reconcilePendingElevenLabsCallStart,
): Promise<number> {
  let reconciled = 0;
  for (const row of rows) {
    const firstResult = normalizeString(row.values[VOICE_BOT_COL_CALL_1_RESULT - 1]).toLowerCase();
    const secondResult = normalizeString(row.values[VOICE_BOT_COL_CALL_2_RESULT - 1]).toLowerCase();
    const uncertainAttempt = firstResult === "call_start_uncertain" ? 1 : secondResult === "call_start_uncertain" ? 2 : undefined;
    if (!uncertainAttempt) continue;
    const marker = parseVoiceCallStartUncertainMarker(row.values[VOICE_BOT_COL_VOICE_NOTES - 1]);
    if (!marker || marker.rowNumber !== row.rowNumber || marker.callAttemptNumber !== uncertainAttempt) {
      logger.error("Uncertain call start is missing its durable reconciliation marker", { rowNumber: row.rowNumber, uncertainAttempt });
      continue;
    }
    if (now.getTime() - marker.requestStartedAtUnixSecs * 1000 < CALL_START_UNCERTAIN_GRACE_MS) continue;

    const metadata = buildRecoveredCallMetadata(row.rowNumber, row.values, marker);
    try {
      const receipt = await resolveReceipt({
        metadata,
        requestStartedAtUnixSecs: marker.requestStartedAtUnixSecs,
      });
      const resultColumn = uncertainAttempt === 2 ? VOICE_BOT_COL_CALL_2_RESULT : VOICE_BOT_COL_CALL_1_RESULT;
      if (receipt?.status === "accepted") {
        recoverAcceptedElevenLabsCallStart(receipt, metadata);
        await writeCells(sheets, row.rowNumber, [
          { columnNumber: resultColumn, value: "call_start_reconciled", label: "call_start_reconciled" },
          { columnNumber: VOICE_BOT_COL_RESPONSE_STATUS, value: "Provider receipt recovered; final call outcome pending", label: "response_status" },
          {
            columnNumber: VOICE_BOT_COL_VOICE_NOTES,
            value: appendVoiceNotesValue(row.values[VOICE_BOT_COL_VOICE_NOTES - 1], `Recovered accepted provider receipt ${receipt.conversationId} for ${marker.callStartRequestId}.`),
            label: "voiceNotes_receipt_recovered",
          },
        ]);
      } else if (receipt?.status === "definitive_failure") {
        const fresh = await sheets.spreadsheets.values.get({ spreadsheetId: config.googleSheets.sheetId,
          range: `'${config.googleSheets.tabName.replace(/'/g, "''")}'!A${row.rowNumber}:AP${row.rowNumber}`, valueRenderOption: "UNFORMATTED_VALUE" });
        const values = fresh.data.values?.[0] ?? [];
        if (normalizePhoneToE164(values[2]) !== metadata.requestedPhone ||
            buildVoiceBotListingAddress(values) !== metadata.listingAddress ||
            values.slice(0, 2).map(normalizeString).filter(Boolean).join(" ") !== metadata.fullName ||
            normalizeString(values[resultColumn - 1]) !== "call_start_uncertain" ||
            parseVoiceCallStartUncertainMarker(values[41])?.callStartRequestId !== marker.callStartRequestId ||
            normalizeString(values[10]) || [15, 16, 19, 35, 36, 37, 38].some(i => {
              const value = normalizeMarker(values[i]); return value && !["no", "false", "0"].includes(value);
            }) || /reply|interested|callback|handoff|opt.?out|stop|remove|information/i.test(`${values[9] ?? ""} ${values[13] ?? ""}`)) continue;
        const agentTimeZone = getVoiceBotAgentTimeZone(values);
        if (!agentTimeZone || (marker.agentTimeZone && marker.agentTimeZone !== agentTimeZone) ||
            rows.some(other => other.rowNumber !== row.rowNumber && normalizePhoneToE164(other.values[2]) === metadata.requestedPhone) ||
            (await getInboundQuietGate(sheets, metadata.requestedPhone, now)).blocked) continue;
        const replacementAt = nextTransportRecoverySlot(now, agentTimeZone, metadata.requestedPhone, uncertainAttempt);
        const resolution = `${VOICE_START_RESOLUTION_MARKER} ${JSON.stringify({ rowNumber: row.rowNumber,
          phone: metadata.requestedPhone, callAttemptNumber: uncertainAttempt, callStartRequestId: marker.callStartRequestId,
          conversationId: receipt.conversationId, status: "definitive_failure", resolvedAt: now.toISOString() })}`;
        const proof = `Provider receipt ${receipt.conversationId} proves the call failed before acceptance.`;
        if (transportFailureKind(receipt.conversation) === "destination_blocked") {
          await writeCells(sheets, row.rowNumber, [
            { columnNumber: resultColumn, value: "provider_destination_blocked", label: "callResult" },
            { columnNumber: VOICE_BOT_COL_RESPONSE_STATUS, value: "Provider blocked destination; contact review required, no cold retry", label: "response_status" },
            ...[VOICE_BOT_COL_CALL_ELIGIBLE, VOICE_BOT_COL_CALL_TIME_BUCKET, VOICE_BOT_COL_CALL_SCHEDULED_FOR].map(columnNumber => ({ columnNumber, value: "", label: "transport_hold" })),
            { columnNumber: VOICE_BOT_COL_VOICE_NOTES, value: appendVoiceNotesValue(values[41], resolution), label: "voiceNotes_transport_hold" },
          ]);
          const after = await getVoiceBotRowByNumber(sheets, row.rowNumber);
          if (normalizeString(after[resultColumn - 1]) !== "provider_destination_blocked" ||
              [29, 30, 31].some(i => normalizeString(after[i])) ||
              normalizePhoneToE164(after[2]) !== metadata.requestedPhone) throw new Error("Transport hold readback mismatch");
          continue;
        }
        const replacementWrites = buildVoiceSafeReplacementWrites(
          uncertainAttempt,
          replacementAt,
          appendVoiceNotesValue(
            values[VOICE_BOT_COL_VOICE_NOTES - 1],
            `${resolution}\n${proof} Safe replacement for attempt ${uncertainAttempt} scheduled for ${formatVoiceBotDateEt(replacementAt)}.`,
          ),
        );
        await writeCells(sheets, row.rowNumber, replacementWrites);
        await verifyVoiceSafeReplacementReadback(sheets, row.rowNumber, uncertainAttempt, replacementAt);
      } else {
        logger.warn("No exact provider receipt; uncertain start remains held without retry", { rowNumber: row.rowNumber });
        continue;
      }
      reconciled += 1;
    } catch (error) {
      logger.warn("Delayed call-start receipt reconciliation failed; row remains paused", {
        rowNumber: row.rowNumber,
        callAttemptNumber: uncertainAttempt,
        callStartRequestId: marker.callStartRequestId,
        message: error instanceof Error ? error.message : String(error),
      });
    }
  }
  return reconciled;
}

async function postStartCall(candidate: VoiceQueueCandidate): Promise<unknown> {
  const url = `${config.baseUrl}/start-call`;
  const response = await axios.post(url, buildStartCallPayload(candidate), {
    // The provider call may use its full 45-second timeout, perform bounded
    // receipt reconciliation, and make one receipt-proven safe retry.
    timeout: 120_000,
    headers: { "Content-Type": "application/json" },
  });

  return response.data;
}

const finalReceiptRecoveryChecks = new Map<string, number>();

async function reconcilePendingVoiceFinalReceipts(
  sheets: sheets_v4.Sheets, rows: Array<{ rowNumber: number; values: unknown[] }>, now: Date,
): Promise<boolean> {
  const candidates = getPendingVoiceOutcomeRecoveries(rows, now, config.voiceQueue.finalReceiptMonitorStartedAt)
    .filter(c => now.getTime() - (finalReceiptRecoveryChecks.get(`${c.rowNumber}:${c.proof.sentAt}`) ?? 0) >= 120_000)
    .sort((a, b) => (finalReceiptRecoveryChecks.get(`${a.rowNumber}:${a.proof.sentAt}`) ?? 0) -
      (finalReceiptRecoveryChecks.get(`${b.rowNumber}:${b.proof.sentAt}`) ?? 0)).slice(0, 4);
  let changed = false;
  for (const { rowNumber, proof } of candidates) {
    finalReceiptRecoveryChecks.set(`${rowNumber}:${proof.sentAt}`, now.getTime());
    const values = rows.find(r => r.rowNumber === rowNumber)!.values;
    const metadata: CallMetadata = {
      rowNumber, callAttemptNumber: proof.callAttemptNumber, firstName: normalizeString(values[0]),
      lastName: normalizeString(values[1]), fullName: [values[0], values[1]].filter(Boolean).join(" "),
      listingAddress: buildVoiceBotListingAddress(values), requestedPhone: proof.phone, dialedPhone: proof.phone,
      testMode: false, sheetName: config.googleSheets.tabName,
    };
    try {
      const receipt = await reconcilePendingElevenLabsCallStart({ metadata, requestStartedAtUnixSecs: Date.parse(proof.sentAt) / 1000 });
      if (!receipt || !isMatchingTerminalVoiceReceipt(receipt.conversation, rowNumber, proof, config.elevenLabs.agentId ?? "")) continue;
      const done = await processPostCallOutcomeFromConversationId(receipt.conversationId, {
        requestQueueRefill: async () => {},
        postSheetUpdate: async payload => {
          const freshRows = await getVoiceBotRows(sheets);
          const current = freshRows.find(r => r.rowNumber === rowNumber)?.values ?? [];
          const blocked = voiceOutcomeRecoveryBlockReason(current, proof);
          if (blocked) throw new Error(`Fresh final-receipt recovery held: ${blocked}`);
          if (freshRows.some(r => r.rowNumber !== rowNumber && normalizePhoneToE164(r.values[2]) === proof.phone)) {
            throw new Error("Final-receipt recovery requires same-phone duplicate review");
          }
          const quiet = await getInboundQuietGate(sheets, proof.phone, new Date());
          if (quiet.blocked) throw new Error("Final-receipt recovery held by inbound quiet gate");
          await updateVoiceLeadRow(rowNumber, { ...payload, voiceOutcomeRecoveryProof: proof }, async () => sheets);
          changed = true;
        },
      });
      changed ||= done;
    } catch (error) {
      logger.warn("Pending final receipt remains held; no replacement call authorized", {
        rowNumber, callAttemptNumber: proof.callAttemptNumber, message: error instanceof Error ? error.message : String(error),
      });
    }
  }
  if (finalReceiptRecoveryChecks.size > 1000) {
    for (const [key, checked] of finalReceiptRecoveryChecks) if (now.getTime() - checked > 3_600_000) finalReceiptRecoveryChecks.delete(key);
  }
  return changed;
}

async function processVoiceQueueUnlocked(options: { dryRun?: boolean; now?: Date; fallbackOnly?: boolean } = {}): Promise<VoiceQueueResult> {
  const now = options.now ?? new Date();
  const outboundPause = getOutboundCallPause(now);
  const providerCircuit = getProviderCircuitStatus();

  const sheets = await getGoogleSheetsClient();
  let rows = await getVoiceBotRows(sheets);
  if (!options.dryRun && await reconcilePendingVoiceFinalReceipts(sheets, rows, now)) rows = await getVoiceBotRows(sheets);
  if (!options.dryRun) {
    try {
      await reconcileOverdueVoiceMailshakeFallback(sheets, rows, now);
    } catch (error) {
      // A held or failed maintenance write must not block unrelated scheduled calls.
      logger.error("Overdue voice fallback held; normal guarded queue continues", {
        message: error instanceof Error ? error.message : "Fallback maintenance failed",
      });
    }
  }
  if (options.fallbackOnly) return { ok: true, queued: false, reason: "overdue_fallback_maintenance" };

  if (!config.outboundVoice.enabled) {
    logger.info("Voice queue paused by owner control", {
      nowEt: formatVoiceBotDateEt(now),
      reason: config.outboundVoice.pauseReason,
    });
    return {
      ok: true,
      queued: false,
      reason: "outbound_voice_owner_paused",
      nowEt: formatVoiceBotDateEt(now),
      pauseReason: config.outboundVoice.pauseReason,
    };
  }

  // Receipt reconciliation is a safety operation, not a new outbound call.
  // Run it even while starts are paused or outside the dialing window so an
  // uncertain attempt cannot be mistaken for a confirmed failure.
  if (!options.dryRun) {
    await ensureProviderStartFailureAlert(now);
    await reconcileUncertainVoiceCallStarts(sheets, rows, now);
    await reconcileScheduledVoiceNoStarts(sheets, rows, now);
  }

  if (outboundPause || providerCircuit.open) {
    if (providerCircuit.open && !options.dryRun) {
      await ensureProviderCircuitAlert();
    }
    logger.info("Voice queue paused", {
      nowEt: formatVoiceBotDateEt(now),
      pausedUntil: outboundPause?.pausedUntil.toISOString(),
      reason: outboundPause?.reason ?? `Provider circuit is open: ${providerCircuit.signature}`,
    });

    return {
      ok: true,
      queued: false,
      reason: providerCircuit.open ? "provider_circuit_open" : "paused",
      nowEt: formatVoiceBotDateEt(now),
      pausedUntil: outboundPause?.pausedUntil.toISOString(),
      pauseReason: outboundPause?.reason ?? `Provider circuit is open: ${providerCircuit.signature}`,
    };
  }

  if (!isWithinVoiceBotQueueRunWindow(now)) {
    logger.info("Voice queue skipped outside queue run window", {
      nowEt: formatVoiceBotDateEt(now),
    });

    return {
      ok: true,
      queued: false,
      reason: "outside_queue_run_window",
      nowEt: formatVoiceBotDateEt(now),
    };
  }

  const finalReceiptCircuit = evaluateFinalReceiptCircuit(
    rows,
    now,
    config.voiceQueue.finalReceiptMonitorStartedAt,
  );
  if (finalReceiptCircuit.open) {
    if (!options.dryRun) await ensureFinalReceiptCircuitAlert(finalReceiptCircuit);
    logger.error("Voice queue paused by final-receipt circuit", finalReceiptCircuit);
    return {
      ok: true,
      queued: false,
      reason: "final_receipt_circuit_open",
      nowEt: formatVoiceBotDateEt(now),
      pauseReason: `${finalReceiptCircuit.evidence.length} calls exceeded ${finalReceiptCircuit.staleAfterMinutes} minutes without final receipts`,
    };
  }
  const activeCallCount = countActiveVoiceBotCalls(rows, now);
  const eligibleCandidateCount = getVoiceBotCallCandidatesFromRows(rows, now, rows.length).length;
  const candidates = getVoiceBotStartableCallCandidatesFromRows(
    rows,
    now,
    VOICE_BOT_MAX_CALLS_PER_QUEUE_RUN,
    VOICE_BOT_MAX_ACTIVE_CALLS,
  );
  const availableSlots = Math.max(0, VOICE_BOT_MAX_ACTIVE_CALLS - activeCallCount);

  if (options.dryRun) {
    const timeZoneReviewRequired = rows
      .filter((row) => !normalizeString(row.values[VOICE_BOT_COL_LEAD_STATUS_CODE - 1]) &&
        normalizeMarker(row.values[VOICE_BOT_COL_FOLLOWUP_TEXT_SENT - 1]) === "x" &&
        !parseVoiceBotDate(row.values[VOICE_BOT_COL_CALL_2_SENT - 1]))
      .map((row) => ({ rowNumber: row.rowNumber, resolution: getVoiceBotTimeZoneResolution(row.values) }))
      .filter(({ resolution }) => !resolution.timeZone)
      .map(({ rowNumber, resolution }) => ({ rowNumber, reason: resolution.reason }));
    return {
      ok: true,
      queued: false,
      dryRun: true,
      timeZoneReviewRequired,
      activeCallCount,
      availableSlots,
      maxCallsPerRun: VOICE_BOT_MAX_CALLS_PER_QUEUE_RUN,
      maxActiveCalls: VOICE_BOT_MAX_ACTIVE_CALLS,
      candidateCount: candidates.length,
      eligibleCandidateCount,
      overdueFallbackCandidates: rows.map((row) => getVoiceOverdueFallbackPlan(row.rowNumber, row.values, now))
        .filter((plan): plan is NonNullable<typeof plan> => Boolean(plan))
        .map((plan) => ({ rowNumber: plan.rowNumber, callAttemptNumber: plan.callAttemptNumber,
          scheduledFor: plan.scheduledFor.toISOString(), deadline: plan.deadline.toISOString() })),
      nowEt: formatVoiceBotDateEt(now),
      candidates: candidates.map(candidateSummary),
    };
  }

  if (availableSlots <= 0) {
    return {
      ok: true,
      queued: false,
      reason: "active_call_limit",
      activeCallCount,
      eligibleCandidateCount,
      maxActiveCalls: VOICE_BOT_MAX_ACTIVE_CALLS,
    };
  }

  if (candidates.length === 0) {
    return {
      ok: true,
      queued: false,
      reason: "no_candidate",
      activeCallCount,
      availableSlots,
    };
  }

  const queuedCalls: VoiceQueueResult["calls"] = [];

  const reservations = new Map<string, VoiceQueueCandidate>();
  await dispatchVoiceQueueBatch(candidates, async (candidate) => {
    const refreshedRows = await getVoiceBotRows(sheets);
    let startNow = options.now ?? new Date();
    const refreshedValues = refreshedRows.find((row) => row.rowNumber === candidate.rowNumber)?.values ?? [];
    const refreshedCandidate = getVoiceBotCallCandidateFromRowValues(candidate.rowNumber, refreshedValues, startNow);

    if (!refreshedCandidate || isVoiceBotPhoneBlocked(refreshedCandidate, refreshedRows, startNow) ||
        countAllocatedVoiceBotSlots(refreshedRows, startNow, [...reservations.values()]) >= VOICE_BOT_MAX_ACTIVE_CALLS) {
      logger.info("Voice queue candidate no longer eligible", {
        rowNumber: candidate.rowNumber,
      });
      return;
    }

    await markScheduledNoStartRecoveryDetected(sheets, refreshedValues, refreshedCandidate, startNow);

    const inboundQuietGate = await getInboundQuietGate(sheets, refreshedCandidate.phone, startNow);
    if (inboundQuietGate.blocked) {
      logger.info("Voice queue candidate blocked by final inbound quiet gate", {
        ...candidateSummary(refreshedCandidate),
        ...inboundQuietGate,
      });
      return;
    }

    startNow = options.now ?? new Date();
    if (!getVoiceBotCallCandidateFromRowValues(candidate.rowNumber, refreshedValues, startNow) ||
        !hasVoiceQueueDispatchHeadroom(refreshedCandidate, startNow) ||
        countAllocatedVoiceBotSlots(refreshedRows, startNow, [...reservations.values()]) >= VOICE_BOT_MAX_ACTIVE_CALLS) {
      logger.info("Voice queue window closed before dispatch; attempt not started", { rowNumber: candidate.rowNumber });
      return;
    }

    reservations.set(refreshedCandidate.phone, refreshedCandidate);
    try {
      const startCallResult = await postStartCall(refreshedCandidate);
      await markVoiceBotAttemptStarted(sheets, refreshedValues, refreshedCandidate, startNow);
      queuedCalls.push(candidateSummary(refreshedCandidate));
      logger.info("Voice queue call started", {
        ...candidateSummary(refreshedCandidate),
        startCallResult,
      });
    } catch (error) {
      const uncertainPayload = getCallStartUncertainPayload(error);
      const fieldsWritten = uncertainPayload
        ? await markVoiceBotAttemptStartUncertain(sheets, refreshedValues, refreshedCandidate, startNow, {
            rowNumber: refreshedCandidate.rowNumber,
            callAttemptNumber: refreshedCandidate.callAttemptNumber,
            callStartRequestId: uncertainPayload.callStartRequestId,
            requestStartedAtUnixSecs: uncertainPayload.requestStartedAtUnixSecs,
            scheduledWindow: refreshedCandidate.callWindow,
            agentTimeZone: refreshedCandidate.agentTimeZone,
          })
        : await markVoiceBotAttemptStartFailed(sheets, refreshedValues, refreshedCandidate, startNow, error);
      logger.error("Voice queue call start failed", {
        ...candidateSummary(refreshedCandidate),
        fieldsWritten,
        status: error instanceof AxiosError ? error.response?.status : undefined,
        message: error instanceof Error ? error.message : String(error),
      });
    } finally {
      reservations.delete(refreshedCandidate.phone);
    }
  });

  return {
    ok: true,
    queued: queuedCalls.length > 0,
    queuedCount: queuedCalls.length,
    maxCallsPerRun: VOICE_BOT_MAX_CALLS_PER_QUEUE_RUN,
    maxActiveCalls: VOICE_BOT_MAX_ACTIVE_CALLS,
    activeCallCountBeforeRun: activeCallCount,
    eligibleCandidateCount,
    calls: queuedCalls,
  };
}

export async function processVoiceQueue(options: { dryRun?: boolean; now?: Date; refill?: boolean; fallbackOnly?: boolean } = {}): Promise<VoiceQueueResult> {
  // A read-only inspection must never join a dispatching run and return its starts.
  if (options.dryRun) return processVoiceQueueUnlocked(options);
  if (activeQueueRun) {
    if (options.refill) {
      const previousRun = activeQueueRun;
      logger.info("Voice queue refill deferred until active dispatch settles");
      await previousRun;
      return processVoiceQueue({ now: options.now, fallbackOnly: options.fallbackOnly });
    }
    logger.info("Voice queue run already active; joining existing run");
    return activeQueueRun;
  }

  activeQueueRun = processVoiceQueueUnlocked(options).finally(() => {
    activeQueueRun = undefined;
  });

  return activeQueueRun;
}

function voiceQueueMaintenanceIntervalMs(): number {
  return Math.max(1, config.voiceQueue.intervalMinutes) * 60_000;
}

export function isVoiceQueueDialingWindow(now: Date): boolean {
  return isWithinVoiceBotQueueRunWindow(now) && listingTimeZones.zones.some((zone) =>
    Boolean(getVoiceBotPreferredCallWindowName(now, zone)));
}

export function shouldPollVoiceQueue(now: Date, lastRunAt?: Date): boolean {
  return !lastRunAt || isVoiceQueueDialingWindow(now) ||
    now.getTime() - lastRunAt.getTime() >= voiceQueueMaintenanceIntervalMs();
}

export function getVoiceQueueDispatchStatus() {
  return {
    version: "capacity-refill-v2",
    schedulerEnabled: config.voiceQueue.schedulerEnabled,
    pollingSecondsInLocalWindows: Math.min(60, voiceQueueMaintenanceIntervalMs() / 1000),
    maintenanceIntervalMinutes: voiceQueueMaintenanceIntervalMs() / 60_000,
    maxActiveCalls: VOICE_BOT_MAX_ACTIVE_CALLS,
    maxParallelStarts: VOICE_QUEUE_MAX_PARALLEL_STARTS,
    dispatchWindowHeadroomSeconds: 60,
    refillMode: "direct_backend",
    freshPerStartEligibility: true,
  };
}

export function startVoiceQueueScheduler(): void {
  if (!config.voiceQueue.schedulerEnabled) {
    logger.info("Voice queue scheduler disabled");
    return;
  }

  if (schedulerTimer) {
    return;
  }

  let lastRunAt: Date | undefined;
  const run = () => {
    const now = new Date();
    if (!shouldPollVoiceQueue(now, lastRunAt)) return;
    lastRunAt = now;
    void processVoiceQueue().then((result) => {
      logger.info("Voice queue scheduler pass completed", {
        reason: result.reason,
        queuedCount: result.queuedCount ?? 0,
        activeCallCount: result.activeCallCount ?? result.activeCallCountBeforeRun,
        availableSlots: result.availableSlots,
        eligibleCandidateCount: result.eligibleCandidateCount,
      });
    }).catch((error) => {
      logger.error("Voice queue scheduler run failed", {
        message: error instanceof Error ? error.message : String(error),
      });
    });
  };

  logger.info("Voice queue scheduler enabled", getVoiceQueueDispatchStatus());

  schedulerTimer = setInterval(run, Math.min(60_000, voiceQueueMaintenanceIntervalMs()));
  setTimeout(run, 30_000);
}
