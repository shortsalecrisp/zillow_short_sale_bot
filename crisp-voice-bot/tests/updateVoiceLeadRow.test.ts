import assert from "node:assert/strict";
import test from "node:test";
import {
  VOICE_BOT_COL_CALL_1_RESULT,
  VOICE_BOT_COL_CALL_1_SENT,
  VOICE_BOT_COL_CALL_ELIGIBLE,
  VOICE_BOT_COL_CALL_SCHEDULED_FOR,
  VOICE_BOT_COL_CALL_TIME_BUCKET,
  VOICE_BOT_COL_LEAD_STATUS_CODE,
} from "../src/lib/voiceSheet";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";

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
