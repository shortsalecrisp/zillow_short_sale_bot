import type { sheets_v4 } from "googleapis";
import { config } from "./config";
import { getGoogleSheetsClient } from "./googleSheets";
import { logger } from "./logger";

export type VoiceLeadRowUpdates = {
  responseStatus?: string;
  voiceCall1Sent?: string;
  voiceCall1Result?: string;
  callbackRequested?: string | boolean;
  callbackTime?: string;
  voiceNotes?: string;
};

type SheetCellWrite = {
  column: "J" | "AG" | "AH" | "AL" | "AM" | "AP";
  field: keyof VoiceLeadRowUpdates;
  value: string;
};

function escapeSheetName(sheetName: string): string {
  return sheetName.replace(/'/g, "''");
}

function cellRange(column: string, rowNumber: number): string {
  return `'${escapeSheetName(config.googleSheets.tabName)}'!${column}${rowNumber}`;
}

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

function addWrite(
  writes: SheetCellWrite[],
  column: SheetCellWrite["column"],
  field: keyof VoiceLeadRowUpdates,
  value: string | undefined,
): void {
  if (value === undefined) {
    return;
  }

  writes.push({ column, field, value });
}

function buildWrites(updates: VoiceLeadRowUpdates): SheetCellWrite[] {
  const writes: SheetCellWrite[] = [];

  addWrite(writes, "J", "responseStatus", updates.responseStatus);
  addWrite(writes, "AG", "voiceCall1Sent", updates.voiceCall1Sent);
  addWrite(writes, "AH", "voiceCall1Result", updates.voiceCall1Result);
  addWrite(writes, "AL", "callbackRequested", normalizeCallbackRequested(updates.callbackRequested));
  addWrite(writes, "AM", "callbackTime", updates.callbackTime);
  addWrite(writes, "AP", "voiceNotes", updates.voiceNotes);

  return writes;
}

async function isVoiceCallOneSentBlank(sheets: sheets_v4.Sheets, rowNumber: number): Promise<boolean> {
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: config.googleSheets.sheetId,
    range: cellRange("AG", rowNumber),
  });
  const value = response.data.values?.[0]?.[0];

  return value === undefined || value === null || String(value).trim() === "";
}

export async function updateVoiceLeadRow(rowNumber: number, updates: VoiceLeadRowUpdates): Promise<void> {
  if (!Number.isInteger(rowNumber) || rowNumber < 2) {
    throw new Error(`rowNumber must be a sheet row number greater than 1. Received: ${rowNumber}`);
  }

  const sheets = await getGoogleSheetsClient();
  const writes = buildWrites(updates);
  let addedVoiceCallOneSentTimestamp = false;

  if (!updates.voiceCall1Sent && (await isVoiceCallOneSentBlank(sheets, rowNumber))) {
    writes.unshift({
      column: "AG",
      field: "voiceCall1Sent",
      value: new Date().toISOString(),
    });
    addedVoiceCallOneSentTimestamp = true;
  }

  if (writes.length === 0) {
    logger.info("Google Sheets row update skipped: no writable fields present", {
      spreadsheetId: config.googleSheets.sheetId,
      tabName: config.googleSheets.tabName,
      rowNumber,
    });
    return;
  }

  logger.info("Writing Google Sheets voice lead row", {
    spreadsheetId: config.googleSheets.sheetId,
    tabName: config.googleSheets.tabName,
    rowNumber,
    fields: writes.map((write) => `${write.column}:${write.field}`),
    addedVoiceCallOneSentTimestamp,
  });

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: config.googleSheets.sheetId,
    requestBody: {
      valueInputOption: "USER_ENTERED",
      data: writes.map((write) => ({
        range: cellRange(write.column, rowNumber),
        values: [[write.value]],
      })),
    },
  });

  logger.info("Google Sheets voice lead row updated", {
    spreadsheetId: config.googleSheets.sheetId,
    tabName: config.googleSheets.tabName,
    rowNumber,
    fields: writes.map((write) => `${write.column}:${write.field}`),
    addedVoiceCallOneSentTimestamp,
  });
}
