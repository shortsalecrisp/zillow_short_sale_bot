import type { sheets_v4 } from "googleapis";
import { config } from "./config";
import { getGoogleSheetsClient } from "./googleSheets";
import { logger } from "./logger";
import {
  appendVoiceNotesValue,
  cellRange,
  columnToLetter,
  getNextVoiceBotFollowupAttemptWindowStart,
  getVoiceBotAgentTimeZone,
  isRetryableVoiceBotResult,
  parseVoiceBotDate,
  VOICE_BOT_COL_CALL_1_RESULT,
  VOICE_BOT_COL_CALL_1_SENT,
  VOICE_BOT_COL_CALL_2_RESULT,
  VOICE_BOT_COL_CALL_2_SENT,
  VOICE_BOT_COL_CALL_ELIGIBLE,
  VOICE_BOT_COL_CALL_SCHEDULED_FOR,
  VOICE_BOT_COL_CALL_TIME_BUCKET,
  VOICE_BOT_COL_CALLBACK_REQUESTED,
  VOICE_BOT_COL_CALLBACK_TIME,
  VOICE_BOT_COL_LEAD_STATUS_CODE,
  VOICE_BOT_COL_LIVE_TRANSFER_COMPLETED,
  VOICE_BOT_COL_LIVE_TRANSFER_REQUESTED,
  VOICE_BOT_COL_RESPONSE_STATUS,
  VOICE_BOT_COL_VM_LEFT,
  VOICE_BOT_COL_VOICE_NOTES,
  VOICE_BOT_PROVIDER_QUOTA_RETRY_DELAY_MINUTES,
} from "./voiceSheet";

export type VoiceLeadRowUpdates = {
  responseStatus?: string;
  leadStatusCode?: string;
  voiceCall1Sent?: string;
  voiceCall1Result?: string;
  callResult?: string;
  callAttemptNumber?: number;
  callScheduledFor?: string;
  callbackRequested?: string | boolean;
  callbackTime?: string;
  liveTransferRequested?: string;
  liveTransferCompleted?: string;
  vmLeft?: string;
  voiceNotes?: string;
  providerQuotaExceeded?: boolean;
};

type SheetCellWrite = {
  columnNumber: number;
  field: string;
  value: string;
};

function normalizeCallbackRequested(value: string | boolean | undefined): string | undefined {
  if (value === undefined) {
    return undefined;
  }

  if (typeof value === "boolean") {
    return value ? "yes" : "";
  }

  const normalized = value.trim().toLowerCase();
  return ["1", "true", "y", "yes"].includes(normalized) ? "yes" : "";
}

function normalizeCallAttemptNumber(value: unknown): 1 | 2 {
  const parsed = Number(value);
  return Number.isInteger(parsed) && parsed > 1 ? 2 : 1;
}

function addWrite(
  writes: SheetCellWrite[],
  columnNumber: number,
  field: string,
  value: string | undefined | null,
): void {
  if (value === undefined || value === null) {
    return;
  }

  writes.push({ columnNumber, field, value });
}

function clearWrite(writes: SheetCellWrite[], columnNumber: number, field: string): void {
  writes.push({ columnNumber, field, value: "" });
}

async function readVoiceLeadRow(sheets: sheets_v4.Sheets, rowNumber: number): Promise<unknown[]> {
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: config.googleSheets.sheetId,
    range: `'${config.googleSheets.tabName.replace(/'/g, "''")}'!A${rowNumber}:AP${rowNumber}`,
  });

  return response.data.values?.[0] ?? [];
}

function appendVoiceNotesWrite(writes: SheetCellWrite[], rowValues: unknown[], voiceNotes: string | undefined): void {
  if (voiceNotes === undefined || voiceNotes === null) {
    return;
  }

  writes.push({
    columnNumber: VOICE_BOT_COL_VOICE_NOTES,
    field: "voiceNotes_appended",
    value: appendVoiceNotesValue(rowValues[VOICE_BOT_COL_VOICE_NOTES - 1], voiceNotes),
  });
}

function addSchedulingWrites(
  writes: SheetCellWrite[],
  rowValues: unknown[],
  updates: VoiceLeadRowUpdates,
  callAttemptNumber: 1 | 2,
  now: Date,
): void {
  const callResult = updates.callResult?.trim() ?? "";
  const leadStatusCode = updates.leadStatusCode?.trim() ?? "";

  if (!callResult && !leadStatusCode) {
    return;
  }

  const retryableFirstAttempt = callAttemptNumber === 1 && isRetryableVoiceBotResult(callResult);

  if (retryableFirstAttempt) {
    const firstAttemptSentAt = parseVoiceBotDate(rowValues[VOICE_BOT_COL_CALL_1_SENT - 1]) ?? now;
    const nextAttemptAt = getNextVoiceBotFollowupAttemptWindowStart(firstAttemptSentAt, getVoiceBotAgentTimeZone(rowValues));

    addWrite(writes, VOICE_BOT_COL_CALL_SCHEDULED_FOR, "call_scheduled_for", nextAttemptAt.toISOString());
    addWrite(writes, VOICE_BOT_COL_CALL_ELIGIBLE, "call_eligible", "yes");
    addWrite(writes, VOICE_BOT_COL_CALL_TIME_BUCKET, "call_time_bucket", "voice_call_2_due");
    return;
  }

  if (leadStatusCode || callAttemptNumber === 2) {
    clearWrite(writes, VOICE_BOT_COL_CALL_ELIGIBLE, "call_eligible");
    clearWrite(writes, VOICE_BOT_COL_CALL_TIME_BUCKET, "call_time_bucket");
    clearWrite(writes, VOICE_BOT_COL_CALL_SCHEDULED_FOR, "call_scheduled_for");
  }
}

function buildWrites(rowValues: unknown[], updates: VoiceLeadRowUpdates, now: Date): SheetCellWrite[] {
  const writes: SheetCellWrite[] = [];
  const callAttemptNumber = normalizeCallAttemptNumber(updates.callAttemptNumber);
  const callSentColumn = callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_SENT : VOICE_BOT_COL_CALL_1_SENT;
  const callResultColumn = callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_RESULT : VOICE_BOT_COL_CALL_1_RESULT;
  const providerQuotaExceeded = updates.providerQuotaExceeded || updates.callResult === "provider_quota_exceeded";

  if (providerQuotaExceeded) {
    const retryAt = new Date(now.getTime() + VOICE_BOT_PROVIDER_QUOTA_RETRY_DELAY_MINUTES * 60_000);
    clearWrite(writes, callSentColumn, `voice_call_${callAttemptNumber}_sent_provider_quota_cleared`);
    clearWrite(writes, callResultColumn, `voice_call_${callAttemptNumber}_result_provider_quota_cleared`);
    addWrite(writes, VOICE_BOT_COL_CALL_ELIGIBLE, "call_eligible", "provider_quota_pause");
    addWrite(writes, VOICE_BOT_COL_CALL_TIME_BUCKET, "call_time_bucket", "provider_quota_retry");
    addWrite(writes, VOICE_BOT_COL_CALL_SCHEDULED_FOR, "call_scheduled_for", retryAt.toISOString());
  } else {
    addWrite(writes, callResultColumn, "callResult", updates.callResult);
    if (!rowValues[callSentColumn - 1]) {
      addWrite(writes, callSentColumn, `voice_call_${callAttemptNumber}_sent`, now.toISOString());
    }
  }

  addWrite(writes, VOICE_BOT_COL_RESPONSE_STATUS, "responseStatus", updates.responseStatus);
  addWrite(writes, VOICE_BOT_COL_LEAD_STATUS_CODE, "leadStatusCode", updates.leadStatusCode);
  appendVoiceNotesWrite(writes, rowValues, updates.voiceNotes);
  addWrite(writes, VOICE_BOT_COL_VM_LEFT, "vmLeft", updates.vmLeft);
  addWrite(writes, VOICE_BOT_COL_LIVE_TRANSFER_REQUESTED, "liveTransferRequested", updates.liveTransferRequested);
  addWrite(writes, VOICE_BOT_COL_LIVE_TRANSFER_COMPLETED, "liveTransferCompleted", updates.liveTransferCompleted);
  addWrite(
    writes,
    VOICE_BOT_COL_CALLBACK_REQUESTED,
    "callbackRequested",
    normalizeCallbackRequested(updates.callbackRequested),
  );
  addWrite(writes, VOICE_BOT_COL_CALLBACK_TIME, "callbackTime", updates.callbackTime);

  if (!providerQuotaExceeded) {
    addWrite(writes, VOICE_BOT_COL_CALL_SCHEDULED_FOR, "callScheduledFor", updates.callScheduledFor);
    addSchedulingWrites(writes, rowValues, updates, callAttemptNumber, now);
  }

  return writes;
}

function fieldsWritten(writes: SheetCellWrite[]): string[] {
  return writes.map((write) => `${columnToLetter(write.columnNumber)}:${write.field}`);
}

export async function updateVoiceLeadRow(rowNumber: number, updates: VoiceLeadRowUpdates): Promise<string[]> {
  if (!Number.isInteger(rowNumber) || rowNumber < 2) {
    throw new Error(`rowNumber must be a sheet row number greater than 1. Received: ${rowNumber}`);
  }

  const now = new Date();
  const sheets = await getGoogleSheetsClient();
  const rowValues = await readVoiceLeadRow(sheets, rowNumber);
  const writes = buildWrites(rowValues, updates, now);
  const written = fieldsWritten(writes);

  if (writes.length === 0) {
    logger.info("Google Sheets row update skipped: no writable fields present", {
      spreadsheetId: config.googleSheets.sheetId,
      tabName: config.googleSheets.tabName,
      rowNumber,
    });
    return [];
  }

  logger.info("Writing Google Sheets voice lead row", {
    spreadsheetId: config.googleSheets.sheetId,
    tabName: config.googleSheets.tabName,
    rowNumber,
    fields: written,
  });

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

  logger.info("Google Sheets voice lead row updated", {
    spreadsheetId: config.googleSheets.sheetId,
    tabName: config.googleSheets.tabName,
    rowNumber,
    fields: written,
  });

  return written;
}
