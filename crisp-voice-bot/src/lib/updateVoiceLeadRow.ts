import type { sheets_v4 } from "googleapis";
import { config } from "./config";
import { getGoogleSheetsClient } from "./googleSheets";
import { logger } from "./logger";
import { VOICE_OVERDUE_FALLBACK_MARKER } from "./voiceOverdueFallback";
import { finalUnansweredPreservationReason, isFinalUnansweredVoiceResult } from "./voiceFinalUnanswered";
import { voiceOutcomeRecoveryBlockReason, voiceReconciliationStateHash, type VoiceOutcomeRecoveryProof } from "./voiceOutcomeRecovery";
import { getInboundQuietGate } from "./voiceSafety";
import { fetchVoiceTerminalReceipt, verifyVoiceTerminalReceipt, type VoiceTerminalReceiptProof } from "./voiceTerminalReceipt";
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
  voiceReceiptOnly?: boolean;
  voiceTerminalReceipt?: VoiceTerminalReceiptProof;
  voiceExpectedStateHash?: string;
  voiceOutcomeRecoveryProof?: VoiceOutcomeRecoveryProof;
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
  providerD17Failure?: boolean;
  providerLlmFailure?: boolean;
};

type SheetCellWrite = {
  columnNumber: number;
  field: string;
  value: string;
};

const pendingRowUpdates = new Map<number, Promise<void>>();

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
    valueRenderOption: "UNFORMATTED_VALUE",
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
    const timeZone = getVoiceBotAgentTimeZone(rowValues);
    if (!timeZone) {
      clearWrite(writes, VOICE_BOT_COL_CALL_ELIGIBLE, "call_eligible");
      clearWrite(writes, VOICE_BOT_COL_CALL_SCHEDULED_FOR, "call_scheduled_for");
      addWrite(writes, VOICE_BOT_COL_CALL_TIME_BUCKET, "call_time_bucket", "listing_timezone_review_required");
      return;
    }
    const nextAttemptAt = getNextVoiceBotFollowupAttemptWindowStart(firstAttemptSentAt, timeZone, callResult === "call_start_failed" ? 1 : 0);

    // Protected dispositions are checked above; only the blank retry lane is released.
    clearWrite(writes, VOICE_BOT_COL_LEAD_STATUS_CODE, "leadStatusCode_retry_cleared");
    addWrite(writes, VOICE_BOT_COL_CALL_SCHEDULED_FOR, "call_scheduled_for", nextAttemptAt.toISOString());
    addWrite(writes, VOICE_BOT_COL_CALL_ELIGIBLE, "call_eligible", "yes");
    addWrite(writes, VOICE_BOT_COL_CALL_TIME_BUCKET, "call_time_bucket", "voice_call_2_due");
    return;
  }

  if (callResult || leadStatusCode) {
    clearWrite(writes, VOICE_BOT_COL_CALL_ELIGIBLE, "call_eligible");
    clearWrite(writes, VOICE_BOT_COL_CALL_TIME_BUCKET, "call_time_bucket");
    clearWrite(writes, VOICE_BOT_COL_CALL_SCHEDULED_FOR, "call_scheduled_for");
  }
}

export function buildVoiceLeadRowWrites(
  rowValues: unknown[],
  updates: VoiceLeadRowUpdates,
  now: Date,
  terminalReceiptVerified = false,
): SheetCellWrite[] {
  const writes: SheetCellWrite[] = [];
  if (updates.voiceExpectedStateHash && voiceReconciliationStateHash(rowValues) !== updates.voiceExpectedStateHash) {
    throw new Error("Voice reconciliation held: owning state changed");
  }
  const protectedReason = finalUnansweredPreservationReason(rowValues);
  if (updates.voiceReceiptOnly && !updates.voiceTerminalReceipt) throw new Error("Receipt-only recovery requires terminal proof");
  if (updates.voiceTerminalReceipt && (updates.voiceReceiptOnly || (protectedReason && protectedReason !== "missing_first_attempt_history"))) {
    const id = updates.voiceTerminalReceipt.conversationId;
    if (!terminalReceiptVerified || !updates.callResult || !updates.voiceNotes?.includes(id)) {
      throw new Error("Protected terminal receipt has not been verified");
    }
    const resultColumn = updates.callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_RESULT : VOICE_BOT_COL_CALL_1_RESULT;
    const oldResult = String(rowValues[resultColumn - 1] ?? "").trim();
    if (!oldResult || oldResult === "call_start_reconciled") addWrite(writes, resultColumn, "callResult", updates.callResult);
    // Retire only this completed cold lane, never an agreed callback schedule.
    if (String(rowValues[30] ?? "") === `voice_call_${updates.callAttemptNumber}_due` &&
        !String(rowValues[38] ?? "").trim() && ["", "no", "false", "0"].includes(String(rowValues[37] ?? "").trim().toLowerCase())) {
      clearWrite(writes, VOICE_BOT_COL_CALL_ELIGIBLE, "call_eligible");
      clearWrite(writes, VOICE_BOT_COL_CALL_TIME_BUCKET, "call_time_bucket");
      clearWrite(writes, VOICE_BOT_COL_CALL_SCHEDULED_FOR, "call_scheduled_for");
    }
    const marker = `CODEX_VOICE_TERMINAL_RECEIPT_V1 ${JSON.stringify({ conversationId: id,
      callAttemptNumber: updates.callAttemptNumber, observedResult: updates.callResult,
      protectedReason, preservedLeadStatus: rowValues[10] ?? "", recordedAt: now.toISOString() })}`;
    const existing = String(rowValues[41] ?? "");
    const notes = existing.includes(`CODEX_VOICE_TERMINAL_RECEIPT_V1 {"conversationId":"${id}"`)
      ? existing : appendVoiceNotesValue(existing, `${updates.voiceNotes}\n${marker}`);
    addWrite(writes, VOICE_BOT_COL_VOICE_NOTES, "voiceTerminalReceipt_verified", notes);
    return writes;
  }
  if (updates.voiceOutcomeRecoveryProof) {
    if (updates.voiceOutcomeRecoveryProof.expectedLeadStatusCode && updates.callResult !== "answered_not_interested") {
      throw new Error("A prior generic N may only be reconciled to a verified decline");
    }
    const reason = voiceOutcomeRecoveryBlockReason(rowValues, updates.voiceOutcomeRecoveryProof);
    if (reason) throw new Error(`Voice outcome recovery held: ${reason}`);
  }
  const callAttemptNumber = normalizeCallAttemptNumber(updates.callAttemptNumber);
  const callSentColumn = callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_SENT : VOICE_BOT_COL_CALL_1_SENT;
  const callResultColumn = callAttemptNumber === 2 ? VOICE_BOT_COL_CALL_2_RESULT : VOICE_BOT_COL_CALL_1_RESULT;
  const providerQuotaExceeded = updates.providerQuotaExceeded || updates.callResult === "provider_quota_exceeded";
  const providerD17Failure = updates.providerD17Failure || updates.callResult === "provider_d17_failure";
  const providerLlmFailure = updates.providerLlmFailure || updates.callResult === "provider_llm_failure";

  if (isFinalUnansweredVoiceResult(updates.callResult, updates.callAttemptNumber)) {
    const reason = finalUnansweredPreservationReason(rowValues);
    if (reason) {
      appendVoiceNotesWrite(writes, rowValues,
        "Final unanswered Mailshake closeout preserved existing state: " + reason + "." + (updates.voiceNotes ? " " + updates.voiceNotes : ""));
      return writes;
    }
    updates = { ...updates, leadStatusCode: "N" };
  }

  const existingResults = [VOICE_BOT_COL_CALL_1_RESULT, VOICE_BOT_COL_CALL_2_RESULT]
    .map((column) => String(rowValues[column - 1] ?? "").trim().toLowerCase());
  const protectedContactOutcome = String(rowValues[VOICE_BOT_COL_VOICE_NOTES - 1] ?? "").includes(VOICE_OVERDUE_FALLBACK_MARKER) || existingResults.some((result) =>
    ["do_not_call", "call_end_requested", "call_ended_by_request", "contact_request_review", "interested_followup_review"].includes(result));
  const wouldReplaceDoNotCall = existingResults.includes("do_not_call") && updates.callResult !== "do_not_call";
  if (wouldReplaceDoNotCall || (protectedContactOutcome && (isRetryableVoiceBotResult(updates.callResult) ||
    providerQuotaExceeded || providerD17Failure || providerLlmFailure))) {
    // A delayed generic classifier cannot reopen contact after a terminal
    // caller request. Keep any earlier callback/DNC fields, and close stale cadence.
    clearWrite(writes, VOICE_BOT_COL_CALL_ELIGIBLE, "call_eligible");
    clearWrite(writes, VOICE_BOT_COL_CALL_TIME_BUCKET, "call_time_bucket");
    clearWrite(writes, VOICE_BOT_COL_CALL_SCHEDULED_FOR, "call_scheduled_for");
    appendVoiceNotesWrite(writes, rowValues,
      `Ignored stale outcome ${updates.callResult ?? "update"}; existing terminal contact outcome retained.${updates.voiceNotes ? ` ${updates.voiceNotes}` : ""}`);
    return writes;
  }
  if (isRetryableVoiceBotResult(updates.callResult) ||
      ["answered_not_interested", "provider_destination_blocked"].includes(updates.callResult ?? "")) {
    const preservationValues = updates.voiceOutcomeRecoveryProof?.expectedLeadStatusCode === "N" &&
      updates.callResult === "answered_not_interested" ? rowValues.map((v, i) => i === 10 ? "" : v) : rowValues;
    const reason = finalUnansweredPreservationReason(preservationValues);
    if (reason && reason !== "missing_first_attempt_history") {
      appendVoiceNotesWrite(writes, rowValues, `Ignored delayed retry outcome; existing contact state retained: ${reason}.`);
      return writes;
    }
  }

  if (providerQuotaExceeded || providerD17Failure || providerLlmFailure) {
    const retryAt = new Date(now.getTime() + VOICE_BOT_PROVIDER_QUOTA_RETRY_DELAY_MINUTES * 60_000);
    const reason = providerD17Failure ? "provider_d17" : providerQuotaExceeded ? "provider_quota" : "provider_llm";
    clearWrite(writes, callSentColumn, `voice_call_${callAttemptNumber}_sent_${reason}_cleared`);
    clearWrite(writes, callResultColumn, `voice_call_${callAttemptNumber}_result_${reason}_cleared`);
    addWrite(writes, VOICE_BOT_COL_CALL_ELIGIBLE, "call_eligible", `${reason}_pause`);
    addWrite(writes, VOICE_BOT_COL_CALL_TIME_BUCKET, "call_time_bucket", `${reason}_retry`);
    addWrite(
      writes,
      VOICE_BOT_COL_CALL_SCHEDULED_FOR,
      "call_scheduled_for",
      providerD17Failure ? "" : retryAt.toISOString(),
    );
  } else {
    addWrite(writes, callResultColumn, "callResult", updates.callResult);
    if (updates.callResult && !rowValues[callSentColumn - 1]) {
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

  if (!providerQuotaExceeded && !providerD17Failure && !providerLlmFailure) {
    addWrite(writes, VOICE_BOT_COL_CALL_SCHEDULED_FOR, "callScheduledFor", updates.callScheduledFor);
    addSchedulingWrites(writes, rowValues, updates, callAttemptNumber, now);
  }

  return writes;
}

function fieldsWritten(writes: SheetCellWrite[]): string[] {
  return writes.map((write) => `${columnToLetter(write.columnNumber)}:${write.field}`);
}

async function writeVoiceLeadRow(
  rowNumber: number,
  updates: VoiceLeadRowUpdates,
  getClient: () => Promise<sheets_v4.Sheets>,
  getTerminalReceipt: typeof fetchVoiceTerminalReceipt,
): Promise<string[]> {
  if (!Number.isInteger(rowNumber) || rowNumber < 2) {
    throw new Error(`rowNumber must be a sheet row number greater than 1. Received: ${rowNumber}`);
  }

  const now = new Date();
  const sheets = await getClient();
  let rowValues = await readVoiceLeadRow(sheets, rowNumber);
  if (updates.voiceExpectedStateHash) {
    const phone = String(rowValues[2] ?? "").replace(/\D/g, "");
    if ((await getInboundQuietGate(sheets, phone, now)).blocked) throw new Error("Voice reconciliation held: inbound preference pending");
  }
  let terminalReceiptVerified = false;
  const protectedReason = finalUnansweredPreservationReason(rowValues);
  const receiptOnlyPersistence = updates.voiceReceiptOnly || (protectedReason && protectedReason !== "missing_first_attempt_history");
  if (updates.voiceTerminalReceipt) {
    const before = voiceReconciliationStateHash(rowValues);
    const receipt = await getTerminalReceipt(updates.voiceTerminalReceipt.conversationId);
    const fresh = await readVoiceLeadRow(sheets, rowNumber);
    if (voiceReconciliationStateHash(fresh) !== before) throw new Error("Terminal receipt owning state changed during verification");
    const unplacedTechnicalFailure = !receiptOnlyPersistence && ["provider_quota_exceeded", "provider_d17_failure",
      "provider_llm_failure", "call_failed_before_completion", "call_failed_before_completion_retryable"]
      .includes(updates.callResult ?? "");
    verifyVoiceTerminalReceipt(rowNumber, fresh, updates.callAttemptNumber, updates.voiceTerminalReceipt, receipt, unplacedTechnicalFailure);
    rowValues = fresh;
    terminalReceiptVerified = true;
  }
  const writes = buildVoiceLeadRowWrites(rowValues, updates, now, terminalReceiptVerified);
  if (updates.callResult && !writes.some(w => /callResult|voiceTerminalReceipt_verified/.test(w.field))) {
    throw new Error("Terminal outcome held: no result or verified receipt can be persisted");
  }
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

  const finalUnanswered = isFinalUnansweredVoiceResult(updates.callResult, updates.callAttemptNumber);
  if (terminalReceiptVerified || updates.voiceExpectedStateHash || updates.voiceOutcomeRecoveryProof || finalUnanswered ||
      ["answered_not_interested", "provider_destination_blocked", "contact_request_review"].includes(updates.callResult ?? "") ||
      (normalizeCallAttemptNumber(updates.callAttemptNumber) === 1 && isRetryableVoiceBotResult(updates.callResult))) {
    const verifiedRow = await readVoiceLeadRow(sheets, rowNumber);
    const proofColumns = new Set([
      VOICE_BOT_COL_CALL_1_RESULT,
      ...(finalUnanswered || normalizeCallAttemptNumber(updates.callAttemptNumber) === 2 ? [VOICE_BOT_COL_CALL_2_RESULT] : []),
      VOICE_BOT_COL_LEAD_STATUS_CODE,
      VOICE_BOT_COL_CALL_ELIGIBLE,
      VOICE_BOT_COL_CALL_TIME_BUCKET,
      VOICE_BOT_COL_CALL_SCHEDULED_FOR,
    ]);
    const expectedProof = writes.filter((write) => terminalReceiptVerified || updates.voiceExpectedStateHash || updates.voiceOutcomeRecoveryProof || proofColumns.has(write.columnNumber));
    if (!expectedProof.length) throw new Error("Terminal outcome readback has no owning proof fields");
    if (terminalReceiptVerified && receiptOnlyPersistence && [0, 1, 2, 4, 5, 6, 9, 10, 13, 15, 16, 19, 32, 35, 36, 37, 38, 39]
      .some(i => String(verifiedRow[i] ?? "") !== String(rowValues[i] ?? ""))) {
      throw new Error("Protected terminal receipt readback changed owning history or preferences");
    }
    if (updates.voiceOutcomeRecoveryProof &&
        parseVoiceBotDate(verifiedRow[updates.voiceOutcomeRecoveryProof.callAttemptNumber === 1 ? 32 : 39])?.toISOString() !== updates.voiceOutcomeRecoveryProof.sentAt) {
      throw new Error(`Recovery attempt timestamp changed for row ${rowNumber}`);
    }
    const mismatches = expectedProof.filter((write) =>
      String(verifiedRow[write.columnNumber - 1] ?? "").trim() !== write.value.trim());
    if (mismatches.length > 0) {
      throw new Error(`${updates.voiceOutcomeRecoveryProof ? "Recovered outcome" : finalUnanswered ? "Final unanswered disposition" : "Voice disposition"} readback mismatch for row ${rowNumber}: ${fieldsWritten(mismatches).join(", ")}`);
    }
    logger.info(terminalReceiptVerified && receiptOnlyPersistence ? "Google Sheets protected terminal receipt verified by readback" : finalUnanswered ? "Google Sheets final unanswered disposition verified by readback" : "Google Sheets retry scheduling verified by readback", {
      spreadsheetId: config.googleSheets.sheetId,
      tabName: config.googleSheets.tabName,
      rowNumber,
      fields: fieldsWritten(expectedProof),
    });
  }

  logger.info("Google Sheets voice lead row updated", {
    spreadsheetId: config.googleSheets.sheetId,
    tabName: config.googleSheets.tabName,
    rowNumber,
    fields: written,
  });

  return written;
}

export async function updateVoiceLeadRow(
  rowNumber: number,
  updates: VoiceLeadRowUpdates,
  getClient: () => Promise<sheets_v4.Sheets> = getGoogleSheetsClient,
  getTerminalReceipt: typeof fetchVoiceTerminalReceipt = fetchVoiceTerminalReceipt,
): Promise<string[]> {
  // Serialize this service's read/build/write operations for the same row so
  // a concurrent fallback sees a terminal outcome written by the live tool.
  const prior = pendingRowUpdates.get(rowNumber) ?? Promise.resolve();
  const operation = prior.then(() => writeVoiceLeadRow(rowNumber, updates, getClient, getTerminalReceipt));
  const settled = operation.then(() => undefined, () => undefined);
  pendingRowUpdates.set(rowNumber, settled);
  void settled.then(() => {
    if (pendingRowUpdates.get(rowNumber) === settled) pendingRowUpdates.delete(rowNumber);
  });
  return operation;
}
