import { updateVoiceLeadRow } from "../lib/updateVoiceLeadRow";

async function main(): Promise<void> {
  const rawRowNumber = process.argv[2] ?? process.env.TEST_SHEET_ROW ?? "3250";
  const rowNumber = Number(rawRowNumber);
  const timestamp = new Date().toISOString();

  await updateVoiceLeadRow(rowNumber, {
    callbackRequested: true,
    voiceNotes: `Direct OAuth test write at ${timestamp}`,
  });

  console.log(`Wrote Google Sheets OAuth test values to row ${rowNumber}`);
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : error);
  process.exitCode = 1;
});
