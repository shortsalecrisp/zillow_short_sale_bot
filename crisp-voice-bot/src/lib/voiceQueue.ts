import axios, { AxiosError } from "axios";
import type { sheets_v4 } from "googleapis";
import { config } from "./config";
import {
  reconcilePendingElevenLabsCallStart,
  recoverAcceptedElevenLabsCallStart,
} from "./elevenLabs";
import { getGoogleSheetsClient } from "./googleSheets";
import { logger } from "./logger";
import { getOutboundCallPause } from "./outboundCallPause";
import { ensureProviderCircuitAlert } from "./providerCircuitAlert";
import { getProviderCircuitStatus } from "./providerCircuitBreaker";
import {
  ensureFinalReceiptCircuitAlert,
  evaluateFinalReceiptCircuit,
  getInboundQuietGate,
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
import type { StartCallRequest } from "../types";

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
  if (normalizeString(rowValues[VOICE_BOT_COL_LEAD_STATUS_CODE - 1])) {
    return undefined;
  }

  if (normalizeMarker(rowValues[VOICE_BOT_COL_FOLLOWUP_TEXT_SENT - 1]) !== "x") {
    return undefined;
  }

  const firstAttemptSentAt = parseVoiceBotDate(rowValues[VOICE_BOT_COL_CALL_1_SENT - 1]);
  const secondAttemptSentAt = parseVoiceBotDate(rowValues[VOICE_BOT_COL_CALL_2_SENT - 1]);
  const firstAttemptResult = normalizeString(rowValues[VOICE_BOT_COL_CALL_1_RESULT - 1]);
  const firstAttemptStartReceiptMissing = isStaleVoiceBotStartWithoutReceipt(
    rowValues[VOICE_BOT_COL_CALL_1_SENT - 1],
    rowValues[VOICE_BOT_COL_CALL_1_RESULT - 1],
    now,
  );
  const scheduledFor = parseVoiceBotDate(rowValues[VOICE_BOT_COL_CALL_SCHEDULED_FOR - 1]);
  const providerStartRecovery = ["call_start_failed", "call_start_receipt_missing"].includes(firstAttemptResult) || firstAttemptStartReceiptMissing;
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

  if (secondAttemptSentAt || (!isRetryableVoiceBotResult(firstAttemptResult) && !firstAttemptStartReceiptMissing)) {
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

function getVoiceBotWindowStartLimit(callWindow: string | undefined, activeLimit: number): number {
  return callWindow === "morning_probe" || callWindow === "reach_morning_v1" ? 1 : activeLimit;
}

export function getVoiceBotStartableCallCandidatesFromRows(
  rows: Array<{ rowNumber: number; values: unknown[] }>,
  now: Date,
  maxCandidates: number,
  maxActiveCalls: number,
): VoiceQueueCandidate[] {
  const activeCallCount = countActiveVoiceBotCalls(rows, now);
  const activeLimit = Math.max(1, Number(maxActiveCalls) || 1);
  const currentCallWindow = rows
    .map((row) => getVoiceBotCallCandidateFromRowValues(row.rowNumber, row.values, now)?.callWindow)
    .find(Boolean);
  const windowStartLimit = getVoiceBotWindowStartLimit(currentCallWindow, activeLimit);
  const availableSlots = Math.max(0, Math.min(activeLimit, windowStartLimit) - activeCallCount);

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

async function reconcileUncertainVoiceCallStarts(
  sheets: sheets_v4.Sheets,
  rows: Array<{ rowNumber: number; values: unknown[] }>,
  now: Date,
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
      const receipt = await reconcilePendingElevenLabsCallStart({
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
      } else {
        const sentColumn = uncertainAttempt === 2 ? VOICE_BOT_COL_CALL_2_SENT : VOICE_BOT_COL_CALL_1_SENT;
        const agentTimeZone = marker.agentTimeZone || getVoiceBotAgentTimeZone(row.values);
        if (!agentTimeZone) continue;
        const replacementAt = getNextVoiceBotFirstAttemptWindowStart(
          new Date(now.getTime() + 5 * 60_000),
          agentTimeZone,
          metadata.requestedPhone,
        );
        const proof = receipt?.status === "definitive_failure"
          ? `Provider receipt ${receipt.conversationId} proves the call failed before acceptance.`
          : "Provider conversation reconciliation completed after the grace period with no matching receipt.";
        await writeCells(sheets, row.rowNumber, [
          { columnNumber: sentColumn, value: "", label: `voice_call_${uncertainAttempt}_sent_released` },
          { columnNumber: resultColumn, value: "", label: `voice_call_${uncertainAttempt}_result_released` },
          { columnNumber: VOICE_BOT_COL_RESPONSE_STATUS, value: "No accepted provider receipt; replacement attempt safely scheduled", label: "response_status" },
          { columnNumber: VOICE_BOT_COL_CALL_ELIGIBLE, value: "yes", label: "call_eligible" },
          { columnNumber: VOICE_BOT_COL_CALL_TIME_BUCKET, value: `voice_call_${uncertainAttempt}_due`, label: "call_time_bucket" },
          { columnNumber: VOICE_BOT_COL_CALL_SCHEDULED_FOR, value: replacementAt.toISOString(), label: "call_scheduled_for" },
          {
            columnNumber: VOICE_BOT_COL_VOICE_NOTES,
            value: appendVoiceNotesValue(row.values[VOICE_BOT_COL_VOICE_NOTES - 1], `${proof} Safe replacement for attempt ${uncertainAttempt} scheduled for ${formatVoiceBotDateEt(replacementAt)}.`),
            label: "voiceNotes_absence_proved",
          },
        ]);
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

async function processVoiceQueueUnlocked(options: { dryRun?: boolean; now?: Date } = {}): Promise<VoiceQueueResult> {
  const now = options.now ?? new Date();
  const outboundPause = getOutboundCallPause(now);
  const providerCircuit = getProviderCircuitStatus();

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
  const sheets = await getGoogleSheetsClient();
  const rows = await getVoiceBotRows(sheets);
  if (!options.dryRun) {
    await reconcileUncertainVoiceCallStarts(sheets, rows, now);
  }

  if (outboundPause || providerCircuit.open) {
    if (providerCircuit.open) {
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
    await ensureFinalReceiptCircuitAlert(finalReceiptCircuit);
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
  const candidates = getVoiceBotStartableCallCandidatesFromRows(
    rows,
    now,
    VOICE_BOT_MAX_CALLS_PER_QUEUE_RUN,
    VOICE_BOT_MAX_ACTIVE_CALLS,
  );
  const windowStartLimit = getVoiceBotWindowStartLimit(candidates[0]?.callWindow, VOICE_BOT_MAX_ACTIVE_CALLS);
  const availableSlots = Math.max(0, Math.min(VOICE_BOT_MAX_ACTIVE_CALLS, windowStartLimit) - activeCallCount);

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

  for (const candidate of candidates) {
    const refreshedRows = await getVoiceBotRows(sheets);
    const refreshedValues = refreshedRows.find((row) => row.rowNumber === candidate.rowNumber)?.values ?? [];
    const refreshedCandidate = getVoiceBotCallCandidateFromRowValues(candidate.rowNumber, refreshedValues, now);

    if (!refreshedCandidate || isVoiceBotPhoneBlocked(refreshedCandidate, refreshedRows, now)) {
      logger.info("Voice queue candidate no longer eligible", {
        rowNumber: candidate.rowNumber,
      });
      continue;
    }

    await markScheduledNoStartRecoveryDetected(sheets, refreshedValues, refreshedCandidate, now);

    const inboundQuietGate = await getInboundQuietGate(sheets, refreshedCandidate.phone, now);
    if (inboundQuietGate.blocked) {
      logger.info("Voice queue candidate blocked by final inbound quiet gate", {
        ...candidateSummary(refreshedCandidate),
        ...inboundQuietGate,
      });
      continue;
    }

    try {
      const startCallResult = await postStartCall(refreshedCandidate);
      await markVoiceBotAttemptStarted(sheets, refreshedValues, refreshedCandidate, now);
      queuedCalls.push(candidateSummary(refreshedCandidate));
      logger.info("Voice queue call started", {
        ...candidateSummary(refreshedCandidate),
        startCallResult,
      });
    } catch (error) {
      const uncertainPayload = getCallStartUncertainPayload(error);
      const fieldsWritten = uncertainPayload
        ? await markVoiceBotAttemptStartUncertain(sheets, refreshedValues, refreshedCandidate, now, {
            rowNumber: refreshedCandidate.rowNumber,
            callAttemptNumber: refreshedCandidate.callAttemptNumber,
            callStartRequestId: uncertainPayload.callStartRequestId,
            requestStartedAtUnixSecs: uncertainPayload.requestStartedAtUnixSecs,
            scheduledWindow: refreshedCandidate.callWindow,
            agentTimeZone: refreshedCandidate.agentTimeZone,
          })
        : await markVoiceBotAttemptStartFailed(sheets, refreshedValues, refreshedCandidate, now, error);
      logger.error("Voice queue call start failed", {
        ...candidateSummary(refreshedCandidate),
        fieldsWritten,
        status: error instanceof AxiosError ? error.response?.status : undefined,
        message: error instanceof Error ? error.message : String(error),
      });
    }
  }

  return {
    ok: true,
    queued: queuedCalls.length > 0,
    queuedCount: queuedCalls.length,
    maxCallsPerRun: VOICE_BOT_MAX_CALLS_PER_QUEUE_RUN,
    maxActiveCalls: VOICE_BOT_MAX_ACTIVE_CALLS,
    activeCallCountBeforeRun: activeCallCount,
    calls: queuedCalls,
  };
}

export async function processVoiceQueue(options: { dryRun?: boolean; now?: Date } = {}): Promise<VoiceQueueResult> {
  if (activeQueueRun) {
    logger.info("Voice queue run already active; joining existing run");
    return activeQueueRun;
  }

  activeQueueRun = processVoiceQueueUnlocked(options).finally(() => {
    activeQueueRun = undefined;
  });

  return activeQueueRun;
}

function voiceQueueIntervalMs(): number {
  return Math.max(1, config.voiceQueue.intervalMinutes) * 60_000;
}

export function startVoiceQueueScheduler(): void {
  if (!config.voiceQueue.schedulerEnabled) {
    logger.info("Voice queue scheduler disabled");
    return;
  }

  if (schedulerTimer) {
    return;
  }

  const run = () => {
    void processVoiceQueue().catch((error) => {
      logger.error("Voice queue scheduler run failed", {
        message: error instanceof Error ? error.message : String(error),
      });
    });
  };

  logger.info("Voice queue scheduler enabled", {
    intervalMinutes: config.voiceQueue.intervalMinutes,
  });

  schedulerTimer = setInterval(run, voiceQueueIntervalMs());
  setTimeout(run, 30_000);
}
