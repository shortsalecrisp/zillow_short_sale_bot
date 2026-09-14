import assert from "node:assert/strict";
import test from "node:test";
import type { sheets_v4 } from "googleapis";
import {
  VOICE_BOT_COL_CALL_1_RESULT,
  VOICE_BOT_COL_CALL_1_SENT,
  VOICE_BOT_COL_CALL_2_RESULT,
  VOICE_BOT_COL_CALL_ELIGIBLE,
  VOICE_BOT_COL_CALL_SCHEDULED_FOR,
  VOICE_BOT_COL_CALL_TIME_BUCKET,
  VOICE_BOT_COL_LEAD_STATUS_CODE,
  VOICE_BOT_COL_CALLBACK_REQUESTED,
  VOICE_BOT_COL_CALLBACK_TIME,
  VOICE_BOT_COL_LIVE_TRANSFER_REQUESTED,
  VOICE_BOT_COL_LIVE_TRANSFER_COMPLETED,
  isRetryableVoiceBotResult,
} from "../src/lib/voiceSheet";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.GOOGLE_SHEETS_SPREADSHEET_ID = "synthetic-test-sheet";
process.env.GOOGLE_SHEETS_TAB_NAME = "SyntheticTestLeads";

test("D17 provider failures clear the attempt and preserve the row for retry", async () => {
  const { buildVoiceLeadRowWrites } = await import("../src/lib/updateVoiceLeadRow");
  const row = Array.from({ length: 42 }, () => "");
  row[VOICE_BOT_COL_CALL_1_SENT - 1] = "2026-07-24T12:00:00.000Z";
  const writes = buildVoiceLeadRowWrites(
    row,
    {
      callAttemptNumber: 1,
      callResult: "provider_d17_failure",
      providerD17Failure: true,
      responseStatus: "Telnyx account disabled (D17) - call not counted",
    },
    new Date("2026-07-24T12:01:00.000Z"),
  );
  const byColumn = new Map(writes.map((write) => [write.columnNumber, write.value]));

  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_1_SENT), "");
  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_1_RESULT), "");
  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_ELIGIBLE), "provider_d17_pause");
  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_TIME_BUCKET), "provider_d17_retry");
  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_SCHEDULED_FOR), "");
});

test("ElevenLabs LLM failures clear the attempt and schedule provider retry", async () => {
  const { buildVoiceLeadRowWrites } = await import("../src/lib/updateVoiceLeadRow");
  const row = Array.from({ length: 42 }, () => "");
  row[VOICE_BOT_COL_CALL_1_SENT - 1] = "2026-09-03T13:15:00.000Z";
  row[VOICE_BOT_COL_CALL_1_RESULT - 1] = "in_progress";
  const writes = buildVoiceLeadRowWrites(
    row,
    {
      callAttemptNumber: 1,
      callResult: "provider_llm_failure",
      providerLlmFailure: true,
      responseStatus: "ElevenLabs LLM cascade failure - call not counted",
    },
    new Date("2026-09-03T13:16:00.000Z"),
  );
  const byColumn = new Map(writes.map((write) => [write.columnNumber, write.value]));

  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_1_SENT), "");
  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_1_RESULT), "");
  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_ELIGIBLE), "provider_llm_pause");
  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_TIME_BUCKET), "provider_llm_retry");
  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_SCHEDULED_FOR), "2026-09-03T17:16:00.000Z");
});

test("terminal first-attempt failures persist an outcome and clear stale scheduling", async () => {
  const { buildVoiceLeadRowWrites } = await import("../src/lib/updateVoiceLeadRow");
  const row = Array.from({ length: 42 }, () => "");
  row[VOICE_BOT_COL_CALL_ELIGIBLE - 1] = "yes";
  row[VOICE_BOT_COL_CALL_TIME_BUCKET - 1] = "voice_call_2_due";
  row[VOICE_BOT_COL_CALL_SCHEDULED_FOR - 1] = "2026-09-09T18:30:00.000Z";
  const writes = buildVoiceLeadRowWrites(
    row,
    {
      callAttemptNumber: 1,
      callResult: "call_failed_before_completion",
      responseStatus: "Call failed before completion",
    },
    new Date("2026-09-09T13:00:00.000Z"),
  );
  const byColumn = new Map(writes.map((write) => [write.columnNumber, write.value]));

  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_1_RESULT), "call_failed_before_completion");
  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_ELIGIBLE), "");
  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_TIME_BUCKET), "");
  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_SCHEDULED_FOR), "");
});

test("retryable first attempts clear stale terminal lead status before scheduling call two", async () => {
  const { buildVoiceLeadRowWrites } = await import("../src/lib/updateVoiceLeadRow");
  const row = Array.from({ length: 42 }, () => "");
  row[VOICE_BOT_COL_CALL_1_SENT - 1] = "2026-09-09T13:03:44.000Z";
  row[VOICE_BOT_COL_LEAD_STATUS_CODE - 1] = "N";

  const writes = buildVoiceLeadRowWrites(
    row,
    {
      callAttemptNumber: 1,
      callResult: "agent_not_available",
      responseStatus: "Agent was not available",
    },
    new Date("2026-09-09T13:04:00.000Z"),
  );
  const byColumn = new Map(writes.map((write) => [write.columnNumber, write.value]));

  assert.equal(byColumn.get(VOICE_BOT_COL_LEAD_STATUS_CODE), "");
  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_ELIGIBLE), "yes");
  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_TIME_BUCKET), "voice_call_2_due");
  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_SCHEDULED_FOR), "2026-09-10T18:30:00.000Z");
});

test("current-call ending and contact review clear cadence without changing lead K or erasing existing history", async () => {
  const { buildVoiceLeadRowWrites } = await import("../src/lib/updateVoiceLeadRow");
  const { buildVoiceContactOutcomeUpdates } = await import("../src/lib/elevenLabsPostCall");
  for (const callResult of ["call_ended_by_request", "contact_request_review"] as const) {
    for (const callAttemptNumber of [1, 2]) {
      for (const previousLeadStatus of ["", "R"]) {
        const row = Array.from({ length: 42 }, () => "");
        row[VOICE_BOT_COL_LEAD_STATUS_CODE - 1] = previousLeadStatus;
        row[VOICE_BOT_COL_CALL_1_RESULT - 1] = "no_answer_first_attempt";
        row[VOICE_BOT_COL_CALL_ELIGIBLE - 1] = "yes";
        row[VOICE_BOT_COL_CALL_TIME_BUCKET - 1] = "voice_call_2_due";
        row[VOICE_BOT_COL_CALL_SCHEDULED_FOR - 1] = "2026-09-14T18:30:00.000Z";
        row[VOICE_BOT_COL_CALLBACK_REQUESTED - 1] = "yes";
        row[VOICE_BOT_COL_CALLBACK_TIME - 1] = "asap";
        row[VOICE_BOT_COL_LIVE_TRANSFER_REQUESTED - 1] = "yes";
        row[VOICE_BOT_COL_LIVE_TRANSFER_COMPLETED - 1] = "yes";
        const writes = buildVoiceLeadRowWrites(row, {
          ...buildVoiceContactOutcomeUpdates(callResult), callAttemptNumber,
        }, new Date("2026-09-13T13:00:00.000Z"));
        const byColumn = new Map(writes.map((write) => [write.columnNumber, write.value]));
        assert.equal(isRetryableVoiceBotResult(callResult), false);
        assert.equal(byColumn.get(callAttemptNumber === 1 ? VOICE_BOT_COL_CALL_1_RESULT : VOICE_BOT_COL_CALL_2_RESULT), callResult);
        assert.equal(byColumn.has(VOICE_BOT_COL_LEAD_STATUS_CODE), false, "a scoped ending cannot grant outreach permission or clear prior DNC");
        for (const column of [VOICE_BOT_COL_CALL_ELIGIBLE, VOICE_BOT_COL_CALL_TIME_BUCKET,
          VOICE_BOT_COL_CALL_SCHEDULED_FOR]) {
          assert.equal(byColumn.get(column), "", `column ${column} must be cleared`);
        }
        for (const column of [VOICE_BOT_COL_CALLBACK_REQUESTED, VOICE_BOT_COL_CALLBACK_TIME, VOICE_BOT_COL_LIVE_TRANSFER_COMPLETED]) {
          assert.equal(byColumn.has(column), false, `column ${column} history must be preserved`);
        }
        assert.equal(byColumn.get(VOICE_BOT_COL_LIVE_TRANSFER_REQUESTED), callResult === "call_ended_by_request" ? "" : undefined);
        if (callAttemptNumber === 2) assert.equal(byColumn.has(VOICE_BOT_COL_CALL_1_RESULT), false);
      }
    }
  }
});

test("late retryable or provider-failure writes cannot downgrade protected contact outcomes", async () => {
  const { buildVoiceLeadRowWrites } = await import("../src/lib/updateVoiceLeadRow");
  for (const existing of ["do_not_call", "call_ended_by_request", "contact_request_review", "interested_followup_review"]) {
    for (const existingColumn of [VOICE_BOT_COL_CALL_1_RESULT, VOICE_BOT_COL_CALL_2_RESULT]) {
      for (const incoming of ["no_answer_first_attempt", "agent_not_available", "voicemail_left", "call_start_failed",
        "provider_llm_failure", "provider_quota_exceeded", "provider_d17_failure"]) {
        const row = Array.from({ length: 42 }, () => "");
        row[existingColumn - 1] = existing;
        row[VOICE_BOT_COL_LEAD_STATUS_CODE - 1] = existing === "do_not_call" ? "R" : "";
        row[VOICE_BOT_COL_CALLBACK_REQUESTED - 1] = "yes";
        row[VOICE_BOT_COL_CALLBACK_TIME - 1] = "tomorrow 2 pm";
        const writes = buildVoiceLeadRowWrites(row, {
          callAttemptNumber: 1, callResult: incoming, leadStatusCode: "N",
          callbackRequested: "", callbackTime: "", voiceNotes: "A delayed generic fallback.",
        }, new Date("2026-09-13T13:00:00.000Z"));
        const byColumn = new Map(writes.map((write) => [write.columnNumber, write.value]));
        for (const column of [VOICE_BOT_COL_CALL_1_RESULT, VOICE_BOT_COL_CALL_2_RESULT,
          VOICE_BOT_COL_LEAD_STATUS_CODE, VOICE_BOT_COL_CALLBACK_REQUESTED, VOICE_BOT_COL_CALLBACK_TIME]) {
          assert.equal(byColumn.has(column), false, `${existing} must survive ${incoming} at column ${column}`);
        }
        assert.equal(byColumn.get(VOICE_BOT_COL_CALL_ELIGIBLE), "");
        assert.equal(byColumn.get(VOICE_BOT_COL_CALL_TIME_BUCKET), "");
        assert.equal(byColumn.get(VOICE_BOT_COL_CALL_SCHEDULED_FOR), "");
      }
    }
  }
});

test("terminal review still allows an explicit future opt-out upgrade", async () => {
  const { buildVoiceLeadRowWrites } = await import("../src/lib/updateVoiceLeadRow");
  const row = Array.from({ length: 42 }, () => "");
  row[VOICE_BOT_COL_CALL_1_RESULT - 1] = "contact_request_review";
  const writes = buildVoiceLeadRowWrites(row, {
    callAttemptNumber: 1, callResult: "do_not_call", leadStatusCode: "R", responseStatus: "Do not call",
  }, new Date("2026-09-13T13:00:00.000Z"));
  const byColumn = new Map(writes.map((write) => [write.columnNumber, write.value]));
  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_1_RESULT), "do_not_call");
  assert.equal(byColumn.get(VOICE_BOT_COL_LEAD_STATUS_CODE), "R");
  assert.equal(byColumn.get(VOICE_BOT_COL_CALL_ELIGIBLE), "");
});

test("an earlier do-not-call result cannot be replaced by current-call ending or missing-evidence review", async () => {
  const { buildVoiceLeadRowWrites } = await import("../src/lib/updateVoiceLeadRow");
  const { buildVoiceContactOutcomeUpdates } = await import("../src/lib/elevenLabsPostCall");
  const row = Array.from({ length: 42 }, () => "");
  row[VOICE_BOT_COL_CALL_1_RESULT - 1] = "do_not_call";
  row[VOICE_BOT_COL_LEAD_STATUS_CODE - 1] = "R";
  for (const callResult of ["call_ended_by_request", "contact_request_review"] as const) {
    const writes = buildVoiceLeadRowWrites(row, {
      ...buildVoiceContactOutcomeUpdates(callResult), callAttemptNumber: 1,
    }, new Date("2026-09-13T13:00:00.000Z"));
    const byColumn = new Map(writes.map((write) => [write.columnNumber, write.value]));
    assert.equal(byColumn.has(VOICE_BOT_COL_CALL_1_RESULT), false);
    assert.equal(byColumn.has(VOICE_BOT_COL_LEAD_STATUS_CODE), false);
    assert.equal(byColumn.get(VOICE_BOT_COL_CALL_ELIGIBLE), "");
  }
});

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>((done) => { resolve = done; });
  return { promise, resolve };
}

function fakeSheet(row: string[], beforeWrite: () => Promise<void> = async () => {}) {
  let reads = 0;
  const client = { spreadsheets: { values: {
    get: async () => { reads += 1; return { data: { values: [[...row]] } }; },
    batchUpdate: async (request: { requestBody: { data: Array<{ range: string; values: string[][] }> } }) => {
      await beforeWrite();
      for (const write of request.requestBody.data) {
        const letters = write.range.match(/!([A-Z]+)\d+$/)![1];
        const column = [...letters].reduce((total, letter) => total * 26 + letter.charCodeAt(0) - 64, 0);
        row[column - 1] = write.values[0][0];
      }
      return { data: {} };
    },
  } } } as unknown as sheets_v4.Sheets;
  return { client, reads: () => reads };
}

test("same-row concurrent direct writes reread terminal intent before applying a late retry", async () => {
  const { updateVoiceLeadRow } = await import("../src/lib/updateVoiceLeadRow");
  const started = deferred();
  const release = deferred();
  const row = Array.from({ length: 42 }, () => "");
  let writes = 0;
  const sheet = fakeSheet(row, async () => {
    writes += 1;
    if (writes === 1) { started.resolve(); await release.promise; }
  });
  const terminal = updateVoiceLeadRow(501, { callAttemptNumber: 1, callResult: "do_not_call", leadStatusCode: "R" }, async () => sheet.client);
  await started.promise;
  const stale = updateVoiceLeadRow(501, { callAttemptNumber: 1, callResult: "no_answer_first_attempt" }, async () => sheet.client);
  await new Promise<void>((done) => setImmediate(done));
  assert.equal(sheet.reads(), 1, "the second write cannot read an obsolete pre-terminal row");
  release.resolve();
  await Promise.all([terminal, stale]);
  assert.equal(sheet.reads(), 2);
  assert.equal(row[VOICE_BOT_COL_CALL_1_RESULT - 1], "do_not_call");
  assert.equal(row[VOICE_BOT_COL_LEAD_STATUS_CODE - 1], "R");
  assert.equal(row[VOICE_BOT_COL_CALL_ELIGIBLE - 1], "");
  assert.equal(row[VOICE_BOT_COL_CALL_SCHEDULED_FOR - 1], "");
});

test("a failed same-row predecessor releases the queue for outcome reconciliation", async () => {
  const { updateVoiceLeadRow } = await import("../src/lib/updateVoiceLeadRow");
  const row = Array.from({ length: 42 }, () => "");
  let writes = 0;
  const sheet = fakeSheet(row, async () => {
    writes += 1;
    if (writes === 1) throw new Error("Synthetic write failure");
  });
  const first = updateVoiceLeadRow(502, { callAttemptNumber: 1, callResult: "call_ended_by_request" }, async () => sheet.client);
  const second = updateVoiceLeadRow(502, { callAttemptNumber: 1, callResult: "call_ended_by_request" }, async () => sheet.client);
  const results = await Promise.allSettled([first, second]);
  assert.equal(results[0].status, "rejected");
  assert.equal(results[1].status, "fulfilled");
  assert.equal(row[VOICE_BOT_COL_CALL_1_RESULT - 1], "call_ended_by_request");
  assert.equal(row[VOICE_BOT_COL_CALL_ELIGIBLE - 1], "");
});
