import assert from "node:assert/strict";
import test from "node:test";
import {
  VOICE_BOT_COL_CALL_1_RESULT,
  VOICE_BOT_COL_CALL_1_SENT,
  VOICE_BOT_COL_CALL_ELIGIBLE,
  VOICE_BOT_COL_CALL_SCHEDULED_FOR,
  VOICE_BOT_COL_CALL_TIME_BUCKET,
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
