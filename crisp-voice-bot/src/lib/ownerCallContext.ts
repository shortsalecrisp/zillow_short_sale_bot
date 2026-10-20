import { config } from "./config";
import { getGoogleSheetsClient } from "./googleSheets";
import { logger } from "./logger";
import { buildVoiceBotListingAddress, escapeSheetName, normalizePhoneToE164, normalizeString } from "./voiceSheet";
import type { OwnerCallContext } from "./ownerCallAnnouncement";

export type OwnerCallRow = { rowNumber: number; values: unknown[] };
export type OwnerCallContextReader = (phone: string, rowNumber?: number) => Promise<OwnerCallRow[]>;

export function matchOwnerCallContext(phone: string, rows: OwnerCallRow[], rowNumber?: number): OwnerCallContext {
  const normalizedPhone = normalizePhoneToE164(phone);
  if (!normalizedPhone) return { status: "unknown" };
  const matches = rows.filter((row) => (!rowNumber || row.rowNumber === rowNumber) &&
    normalizePhoneToE164(row.values[2]) === normalizedPhone).map((row) => ({
      rowNumber: row.rowNumber,
      agentName: [normalizeString(row.values[0]), normalizeString(row.values[1])].filter(Boolean).join(" "),
      listingAddress: buildVoiceBotListingAddress(row.values),
    }));
  if (!matches.length) return { status: "unknown" };
  const names = new Set(matches.map((row) => row.agentName.toLowerCase()));
  if (names.size !== 1 || !matches[0]!.agentName) return { status: "ambiguous" };
  if (matches.some((row) => !normalizeString(rows.find((r) => r.rowNumber === row.rowNumber)?.values[4]))) {
    return { status: "unknown" };
  }
  const listings = new Set(matches.map((row) => row.listingAddress.toLowerCase()));
  if (listings.size !== 1) return { status: "multiple_listings", agentName: matches[0]!.agentName };
  return { status: "matched", ...matches[0]! };
}

const readOwnerCallRows: OwnerCallContextReader = async (phone, rowNumber) => {
  const sheets = await getGoogleSheetsClient();
  const tab = escapeSheetName(config.googleSheets.tabName);
  let rowNumbers: number[];
  if (rowNumber) {
    rowNumbers = [rowNumber];
  } else {
    const phones = await sheets.spreadsheets.values.get({
      spreadsheetId: config.googleSheets.sheetId, range: `${tab}!C2:C`,
    }, { timeout: 1500, retry: false });
    rowNumbers = (phones.data.values ?? []).flatMap((values, index) =>
      normalizePhoneToE164(values[0]) === phone ? [index + 2] : []);
  }
  if (!rowNumbers.length) return [];
  if (rowNumbers.length > 20) throw new Error("Too many caller-number matches");
  const response = await sheets.spreadsheets.values.batchGet({
    spreadsheetId: config.googleSheets.sheetId,
    ranges: rowNumbers.map((row) => `${tab}!A${row}:G${row}`),
  }, { timeout: 1500, retry: false });
  return rowNumbers.map((row, index) => ({ rowNumber: row, values: response.data.valueRanges?.[index]?.values?.[0] ?? [] }));
};

export async function lookupOwnerCallContext(
  phone: string, rowNumber?: number, readRows: OwnerCallContextReader = readOwnerCallRows,
): Promise<OwnerCallContext> {
  const normalizedPhone = normalizePhoneToE164(phone);
  if (!normalizedPhone || (rowNumber !== undefined && (!Number.isInteger(rowNumber) || rowNumber < 2))) return { status: "unknown" };
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const rows = await Promise.race([
      readRows(normalizedPhone, rowNumber),
      new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("Caller context lookup timed out")), 2500); }),
    ]);
    return matchOwnerCallContext(normalizedPhone, rows, rowNumber);
  } catch {
    logger.warn("Owner call announcement context unavailable; preserving call routing", { rowNumber });
    return { status: "unavailable" };
  } finally {
    if (timer) clearTimeout(timer);
  }
}

export async function resolveLiveTransferOwnerContext(
  input: { rowNumber: number; phone: string; agentName: string; listingAddress: string },
  lookup = lookupOwnerCallContext,
): Promise<{ agentName: string; listingAddress: string }> {
  const context = await lookup(input.phone, input.rowNumber);
  return context.status === "matched"
    ? { agentName: context.agentName!, listingAddress: context.listingAddress! }
    : { agentName: input.agentName, listingAddress: input.listingAddress };
}
