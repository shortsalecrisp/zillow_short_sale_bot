import axios, { AxiosError } from "axios";
import type { sheets_v4 } from "googleapis";
import { config } from "./config";
import { getGoogleSheetsClient } from "./googleSheets";
import { logger } from "./logger";
import { cellRange, columnToLetter, normalizeString } from "./voiceSheet";

const MAILSHAKE_CAMPAIGNS: Record<string, number> = {
  G: 1476801,
  Y: 1476826,
  N: 1476827,
  O: 1534028,
};

const COL_FIRST_NAME = 1; // A
const COL_LAST_NAME = 2; // B
const COL_EMAIL = 4; // D
const COL_LISTING_ADDRESS = 5; // E
const COL_STATUS = 11; // K
const COL_SYNCED = 29; // AC

type MailshakeRecipient = {
  row: number;
  email: string;
  firstName: string;
  lastName: string;
  fullName: string;
  listingAddress: string;
};

export type MailshakeSyncResult = {
  ok: true;
  dryRun?: boolean;
  scanned: {
    anchor: number;
    startRow: number;
    endRow: number;
    count: number;
  };
  batchesSummary: Record<string, number>;
  pushedRows: number[];
  invalidEmailRows: number[];
  skipped: {
    already: number;
    noStatus: number;
    badStatus: number;
    statusR: number;
    noEmail: number;
    invalid: number;
  };
};

let activeMailshakeSync: Promise<MailshakeSyncResult> | undefined;
let schedulerTimer: NodeJS.Timeout | undefined;

function extractEmail(raw: unknown): string {
  let text = normalizeString(raw);
  text = text.replace(/^mailto:/i, "");
  text = text.replace(/[\u00A0\u1680\u180E\u2000-\u200D\u202F\u205F\u2060\u3000\uFEFF]/g, " ");
  text = text.replace(/\s+/g, " ").trim();

  const match = text.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  return match ? match[0].toLowerCase() : "";
}

function isValidEmail(email: string): boolean {
  return /^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$/i.test(email.trim());
}

async function findLastSyncableStatusRow(sheets: sheets_v4.Sheets): Promise<number> {
  const startAtRow = config.mailshakeSync.startAtRow;
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: config.googleSheets.sheetId,
    range: `'${config.googleSheets.tabName.replace(/'/g, "''")}'!K${startAtRow}:K`,
  });
  const values = response.data.values ?? [];

  for (let i = values.length - 1; i >= 0; i -= 1) {
    const status = normalizeString(values[i]?.[0]).toUpperCase();
    if (["G", "Y", "N", "O"].includes(status)) {
      return startAtRow + i;
    }
  }

  return 0;
}

async function readSyncWindow(sheets: sheets_v4.Sheets, startRow: number, endRow: number): Promise<unknown[][]> {
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: config.googleSheets.sheetId,
    range: `'${config.googleSheets.tabName.replace(/'/g, "''")}'!A${startRow}:AC${endRow}`,
  });

  return response.data.values ?? [];
}

function addRecipient(
  batches: Record<string, MailshakeRecipient[]>,
  campaignId: number,
  recipient: MailshakeRecipient,
): void {
  const key = String(campaignId);
  batches[key] = batches[key] ?? [];
  batches[key].push(recipient);
}

function classifyRows(
  rows: unknown[][],
  startRow: number,
): {
  batches: Record<string, MailshakeRecipient[]>;
  invalidEmailRows: number[];
  skipped: MailshakeSyncResult["skipped"];
} {
  const batches: Record<string, MailshakeRecipient[]> = {};
  const invalidEmailRows: number[] = [];
  const skipped = { already: 0, noStatus: 0, badStatus: 0, statusR: 0, noEmail: 0, invalid: 0 };

  for (let i = rows.length - 1; i >= 0; i -= 1) {
    const row = rows[i] ?? [];
    const rowNumber = startRow + i;
    const sentMark = normalizeString(row[COL_SYNCED - 1]).toLowerCase();

    if (["sent", "invalid_email", "pushed"].includes(sentMark)) {
      skipped.already += 1;
      continue;
    }

    const status = normalizeString(row[COL_STATUS - 1]).toUpperCase();
    if (!status) {
      skipped.noStatus += 1;
      continue;
    }
    if (status === "R") {
      skipped.statusR += 1;
      continue;
    }
    if (!["G", "Y", "N", "O"].includes(status)) {
      skipped.badStatus += 1;
      continue;
    }

    if (!normalizeString(row[COL_EMAIL - 1])) {
      skipped.noEmail += 1;
      continue;
    }

    const email = extractEmail(row[COL_EMAIL - 1]);
    if (!email || !isValidEmail(email)) {
      invalidEmailRows.push(rowNumber);
      skipped.invalid += 1;
      continue;
    }

    const firstName = normalizeString(row[COL_FIRST_NAME - 1]);
    const lastName = normalizeString(row[COL_LAST_NAME - 1]);
    const listingAddress = normalizeString(row[COL_LISTING_ADDRESS - 1]);
    const fullName = [firstName, lastName].filter(Boolean).join(" ").trim();
    const campaignId = MAILSHAKE_CAMPAIGNS[status];

    if (!campaignId) {
      continue;
    }

    addRecipient(batches, campaignId, {
      row: rowNumber,
      email,
      firstName,
      lastName,
      fullName,
      listingAddress,
    });
  }

  return { batches, invalidEmailRows, skipped };
}

async function markRows(sheets: sheets_v4.Sheets, rows: number[], value: string): Promise<void> {
  if (rows.length === 0) {
    return;
  }

  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: config.googleSheets.sheetId,
    requestBody: {
      valueInputOption: "USER_ENTERED",
      data: rows.map((rowNumber) => ({
        range: cellRange(config.googleSheets.tabName, columnToLetter(COL_SYNCED), rowNumber),
        values: [[value]],
      })),
    },
  });
}

async function pushCampaignRecipients(campaignId: string, recipients: MailshakeRecipient[]): Promise<void> {
  if (!config.mailshakeSync.apiKey) {
    throw new Error("MAILSHAKE_API_KEY is required for Mailshake sync");
  }

  const addresses = recipients.map(({ email, firstName, lastName, fullName, listingAddress }) => ({
    emailAddress: email,
    fullName,
    fields: {
      first_name: firstName,
      last_name: lastName,
      listing_address: listingAddress,
    },
  }));

  await axios.post(
    "https://api.mailshake.com/2017-04-01/recipients/add",
    {
      campaignID: campaignId,
      addAsNewList: true,
      addresses,
    },
    {
      timeout: 30_000,
      auth: {
        username: config.mailshakeSync.apiKey,
        password: "",
      },
    },
  );
}

async function runMailshakeSyncUnlocked(options: { dryRun?: boolean } = {}): Promise<MailshakeSyncResult> {
  const sheets = await getGoogleSheetsClient();
  const anchor = await findLastSyncableStatusRow(sheets);

  if (!anchor) {
    return {
      ok: true,
      dryRun: options.dryRun,
      scanned: { anchor: 0, startRow: 0, endRow: 0, count: 0 },
      batchesSummary: {},
      pushedRows: [],
      invalidEmailRows: [],
      skipped: { already: 0, noStatus: 0, badStatus: 0, statusR: 0, noEmail: 0, invalid: 0 },
    };
  }

  const endRow = anchor;
  const startRow = Math.max(config.mailshakeSync.startAtRow, endRow - config.mailshakeSync.windowSize + 1);
  const count = endRow - startRow + 1;
  const rows = await readSyncWindow(sheets, startRow, endRow);
  const { batches, invalidEmailRows, skipped } = classifyRows(rows, startRow);
  const batchesSummary = Object.fromEntries(Object.entries(batches).map(([campaignId, recipients]) => [campaignId, recipients.length]));
  const pushedRows: number[] = [];

  if (!options.dryRun) {
    await markRows(sheets, invalidEmailRows, "invalid_email");

    for (const [campaignId, recipients] of Object.entries(batches)) {
      if (recipients.length === 0) {
        continue;
      }

      try {
        await pushCampaignRecipients(campaignId, recipients);
        const rowsToMark = recipients.map((recipient) => recipient.row);
        await markRows(sheets, rowsToMark, "sent");
        pushedRows.push(...rowsToMark);
        logger.info("Mailshake recipients pushed", {
          campaignId,
          count: recipients.length,
          rows: rowsToMark,
        });
      } catch (error) {
        logger.error("Mailshake recipient push failed", {
          campaignId,
          count: recipients.length,
          status: error instanceof AxiosError ? error.response?.status : undefined,
          message: error instanceof Error ? error.message : String(error),
        });
      }
    }
  }

  return {
    ok: true,
    dryRun: options.dryRun,
    scanned: { anchor, startRow, endRow, count },
    batchesSummary,
    pushedRows,
    invalidEmailRows,
    skipped,
  };
}

export async function runMailshakeSync(options: { dryRun?: boolean } = {}): Promise<MailshakeSyncResult> {
  if (activeMailshakeSync) {
    logger.info("Mailshake sync already active; joining existing run");
    return activeMailshakeSync;
  }

  activeMailshakeSync = runMailshakeSyncUnlocked(options).finally(() => {
    activeMailshakeSync = undefined;
  });

  return activeMailshakeSync;
}

function intervalMs(): number {
  return Math.max(1, config.mailshakeSync.intervalMinutes) * 60_000;
}

export function startMailshakeSyncScheduler(): void {
  if (!config.mailshakeSync.schedulerEnabled) {
    logger.info("Mailshake sync scheduler disabled");
    return;
  }

  if (schedulerTimer) {
    return;
  }

  const run = () => {
    void runMailshakeSync().catch((error) => {
      logger.error("Mailshake sync scheduler run failed", {
        message: error instanceof Error ? error.message : String(error),
      });
    });
  };

  logger.info("Mailshake sync scheduler enabled", {
    intervalMinutes: config.mailshakeSync.intervalMinutes,
    startAtRow: config.mailshakeSync.startAtRow,
    windowSize: config.mailshakeSync.windowSize,
  });

  schedulerTimer = setInterval(run, intervalMs());
  setTimeout(run, 45_000);
}
