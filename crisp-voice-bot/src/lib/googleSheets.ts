import { google, sheets_v4 } from "googleapis";
import { getGoogleAuthClient } from "./googleAuth";

let sheetsClientPromise: Promise<sheets_v4.Sheets> | undefined;

export async function getGoogleSheetsClient(): Promise<sheets_v4.Sheets> {
  if (!sheetsClientPromise) {
    sheetsClientPromise = getGoogleAuthClient().then((auth) => google.sheets({ version: "v4", auth }));
  }

  return sheetsClientPromise;
}
