import { config } from "./config";
import { getGoogleSheetsClient } from "./googleSheets";

let ensuredStateSheet: Promise<void> | undefined;

function quoteSheetName(name: string): string {
  return `'${name.replace(/'/g, "''")}'`;
}

async function ensureStateSheetExists(): Promise<void> {
  if (ensuredStateSheet) {
    return ensuredStateSheet;
  }

  ensuredStateSheet = (async () => {
    const sheets = await getGoogleSheetsClient();
    const spreadsheetId = config.googleSheets.sheetId;
    const title = config.gmailImporters.stateSheetName;
    const spreadsheet = await sheets.spreadsheets.get({
      spreadsheetId,
      fields: "sheets.properties.title",
    });
    const exists = (spreadsheet.data.sheets ?? []).some((sheet) => sheet.properties?.title === title);

    if (!exists) {
      await sheets.spreadsheets.batchUpdate({
        spreadsheetId,
        requestBody: {
          requests: [{ addSheet: { properties: { title } } }],
        },
      });
    }

    const range = `${quoteSheetName(title)}!A1:C1`;
    const header = await sheets.spreadsheets.values.get({ spreadsheetId, range }).catch(() => undefined);
    const first = header?.data.values?.[0] ?? [];
    if (first[0] !== "key" || first[1] !== "json" || first[2] !== "updated_at") {
      await sheets.spreadsheets.values.update({
        spreadsheetId,
        range,
        valueInputOption: "RAW",
        requestBody: { values: [["key", "json", "updated_at"]] },
      });
    }
  })();

  return ensuredStateSheet;
}

async function readStateRows(): Promise<string[][]> {
  await ensureStateSheetExists();
  const sheets = await getGoogleSheetsClient();
  const response = await sheets.spreadsheets.values.get({
    spreadsheetId: config.googleSheets.sheetId,
    range: `${quoteSheetName(config.gmailImporters.stateSheetName)}!A:C`,
  });
  return (response.data.values ?? []) as string[][];
}

export async function readGmailImportState<T>(key: string, fallback: T): Promise<T> {
  const rows = await readStateRows();
  const row = rows.find((candidate) => candidate[0] === key);
  if (!row?.[1]) {
    return fallback;
  }

  try {
    return JSON.parse(row[1]) as T;
  } catch {
    return fallback;
  }
}

export async function writeGmailImportState(key: string, value: unknown): Promise<void> {
  const rows = await readStateRows();
  const sheets = await getGoogleSheetsClient();
  const spreadsheetId = config.googleSheets.sheetId;
  const title = quoteSheetName(config.gmailImporters.stateSheetName);
  const payload = [[key, JSON.stringify(value), new Date().toISOString()]];
  const existingIndex = rows.findIndex((row) => row[0] === key);

  if (existingIndex >= 0) {
    await sheets.spreadsheets.values.update({
      spreadsheetId,
      range: `${title}!A${existingIndex + 1}:C${existingIndex + 1}`,
      valueInputOption: "RAW",
      requestBody: { values: payload },
    });
    return;
  }

  await sheets.spreadsheets.values.append({
    spreadsheetId,
    range: `${title}!A:C`,
    valueInputOption: "RAW",
    insertDataOption: "INSERT_ROWS",
    requestBody: { values: payload },
  });
}
