import axios, { AxiosError } from "axios";
import type { sheets_v4 } from "googleapis";
import { config } from "./config";
import { getGoogleSheetsClient } from "./googleSheets";
import { logger } from "./logger";
import { getOutboundCallPause } from "./outboundCallPause";
import {
  appendVoiceNotesValue,
  buildVoiceBotListingAddress,
  cellRange,
  columnToLetter,
  formatVoiceBotDateEt,
  getNextVoiceBotFirstAttemptWindowStart,
  getNextVoiceBotFollowupAttemptWindowStart,
  getVoiceBotAgentTimeZone,
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
};

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
  const scheduledFor = parseVoiceBotDate(rowValues[VOICE_BOT_COL_CALL_SCHEDULED_FOR - 1]);
  const agentTimeZone = getVoiceBotAgentTimeZone(rowValues);
  const currentWindow = getVoiceBotPreferredCallWindowName(now, agentTimeZone);

  if (!currentWindow) {
    return undefined;
  }

  if (scheduledFor && now < scheduledFor) {
    return undefined;
  }

  if (!firstAttemptSentAt) {
    const followupSentAt = parseVoiceBotDate(rowValues[VOICE_BOT_COL_FOLLOWUP_SENT_AT_PROXY - 1]);
    if (!followupSentAt) {
      return undefined;
    }

    const dueAt = getNextVoiceBotFirstAttemptWindowStart(followupSentAt, agentTimeZone);
    if (now < dueAt) {
      return undefined;
    }

    return buildVoiceBotCandidate(rowNumber, rowValues, 1, now, currentWindow, agentTimeZone);
  }

  if (secondAttemptSentAt || !isRetryableVoiceBotResult(firstAttemptResult)) {
    return undefined;
  }

  const nextAttemptAt = getNextVoiceBotFollowupAttemptWindowStart(firstAttemptSentAt, agentTimeZone);
  if (now < nextAttemptAt) {
    return undefined;
  }

  return buildVoiceBotCandidate(rowNumber, rowValues, 2, nextAttemptAt, currentWindow, agentTimeZone);
}

function getVoiceBotCallCandidatesFromRows(
  rows: Array<{ rowNumber: number; values: unknown[] }>,
  now: Date,
  maxCandidates: number,
): VoiceQueueCandidate[] {
  const limit = Math.max(1, Number(maxCandidates) || 1);
  const candidates: VoiceQueueCandidate[] = [];

  for (const row of rows) {
    const candidate = getVoiceBotCallCandidateFromRowValues(row.rowNumber, row.values, now);
    if (!candidate) {
      continue;
    }

    candidates.push(candidate);
    if (candidates.length >= limit) {
      break;
    }
  }

  return candidates;
}

function getVoiceBotStartableCallCandidatesFromRows(
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

async function markVoiceBotAttemptStarted(
  sheets: sheets_v4.Sheets,
  candidate: VoiceQueueCandidate,
  now: Date,
): Promise<void> {
  const sentColumn = candidate.callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_SENT : VOICE_BOT_COL_CALL_1_SENT;
  const timeBucket = candidate.callAttemptNumber === 2 ? "voice_call_2_due" : "voice_call_1_due";

  await writeCells(sheets, candidate.rowNumber, [
    { columnNumber: sentColumn, value: now.toISOString() },
    { columnNumber: VOICE_BOT_COL_CALL_ELIGIBLE, value: "queued" },
    { columnNumber: VOICE_BOT_COL_CALL_TIME_BUCKET, value: timeBucket },
    { columnNumber: VOICE_BOT_COL_CALL_SCHEDULED_FOR, value: candidate.dueAt.toISOString() },
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
  const writes = [
    { columnNumber: sentColumn, value: now.toISOString(), label: `voice_call_${candidate.callAttemptNumber}_sent` },
    { columnNumber: resultColumn, value: "call_start_failed", label: `voice_call_${candidate.callAttemptNumber}_result` },
    { columnNumber: VOICE_BOT_COL_RESPONSE_STATUS, value: "Call start failed before connecting", label: "response_status" },
    { columnNumber: VOICE_BOT_COL_CALL_ELIGIBLE, value: "", label: "call_eligible" },
    { columnNumber: VOICE_BOT_COL_CALL_TIME_BUCKET, value: "", label: "call_time_bucket" },
    { columnNumber: VOICE_BOT_COL_CALL_SCHEDULED_FOR, value: "", label: "call_scheduled_for" },
    {
      columnNumber: VOICE_BOT_COL_VOICE_NOTES,
      value: appendVoiceNotesValue(rowValues[VOICE_BOT_COL_VOICE_NOTES - 1], voiceNotes),
      label: "voiceNotes_appended",
    },
  ];

  await writeCells(sheets, candidate.rowNumber, writes);
  return writes.map((write) => `${columnToLetter(write.columnNumber)}:${write.label}`);
}

async function postStartCall(candidate: VoiceQueueCandidate): Promise<unknown> {
  const url = `${config.baseUrl}/start-call`;
  const response = await axios.post(url, buildStartCallPayload(candidate), {
    timeout: 45_000,
    headers: { "Content-Type": "application/json" },
  });

  return response.data;
}

async function processVoiceQueueUnlocked(options: { dryRun?: boolean; now?: Date } = {}): Promise<VoiceQueueResult> {
  const now = options.now ?? new Date();
  const outboundPause = getOutboundCallPause(now);

  if (outboundPause) {
    logger.info("Voice queue paused", {
      nowEt: formatVoiceBotDateEt(now),
      pausedUntil: outboundPause.pausedUntil.toISOString(),
      reason: outboundPause.reason,
    });

    return {
      ok: true,
      queued: false,
      reason: "paused",
      nowEt: formatVoiceBotDateEt(now),
      pausedUntil: outboundPause.pausedUntil.toISOString(),
      pauseReason: outboundPause.reason,
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

  const sheets = await getGoogleSheetsClient();
  const rows = await getVoiceBotRows(sheets);
  const activeCallCount = countActiveVoiceBotCalls(rows, now);
  const availableSlots = Math.max(0, VOICE_BOT_MAX_ACTIVE_CALLS - activeCallCount);
  const candidates = getVoiceBotStartableCallCandidatesFromRows(
    rows,
    now,
    VOICE_BOT_MAX_CALLS_PER_QUEUE_RUN,
    VOICE_BOT_MAX_ACTIVE_CALLS,
  );

  if (options.dryRun) {
    return {
      ok: true,
      queued: false,
      dryRun: true,
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
    const refreshedValues = await getVoiceBotRowByNumber(sheets, candidate.rowNumber);
    const refreshedCandidate = getVoiceBotCallCandidateFromRowValues(candidate.rowNumber, refreshedValues, now);

    if (!refreshedCandidate) {
      logger.info("Voice queue candidate no longer eligible", {
        rowNumber: candidate.rowNumber,
      });
      continue;
    }

    try {
      const startCallResult = await postStartCall(refreshedCandidate);
      await markVoiceBotAttemptStarted(sheets, refreshedCandidate, now);
      queuedCalls.push(candidateSummary(refreshedCandidate));
      logger.info("Voice queue call started", {
        ...candidateSummary(refreshedCandidate),
        startCallResult,
      });
    } catch (error) {
      const fieldsWritten = await markVoiceBotAttemptStartFailed(sheets, refreshedValues, refreshedCandidate, now, error);
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
