import assert from "node:assert/strict";
import test from "node:test";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";

function row(followupSentAt: string, options: { call1SentAt?: string; call1Result?: string } = {}): unknown[] {
  const values = Array(42).fill("");
  values[0] = "Test";
  values[1] = "Agent";
  values[2] = "603-325-5909";
  values[4] = "20 Pearl Street";
  values[5] = "Hillsboro";
  values[6] = "NH";
  values[8] = "x";
  values[23] = followupSentAt;
  values[32] = options.call1SentAt ?? "";
  values[33] = options.call1Result ?? "";
  return values;
}

test("Render queue prioritizes first calls, then oldest due time", async () => {
  const { getVoiceBotCallCandidatesFromRows } = await import("../src/lib/voiceQueue");
  const candidates = getVoiceBotCallCandidatesFromRows(
    [
      {
        rowNumber: 6001,
        values: row("", {
          call1SentAt: "2026-08-23T13:15:00Z",
          call1Result: "agent_not_available",
        }),
      },
      { rowNumber: 6002, values: row("2026-08-24T13:30:00Z") },
      { rowNumber: 6003, values: row("2026-08-23T22:45:00Z") },
    ],
    new Date("2026-08-24T18:45:00Z"),
    10,
  );

  assert.deepEqual(candidates.map((candidate) => candidate.rowNumber), [6003, 6002, 6001]);
  assert.deepEqual(candidates.map((candidate) => candidate.callAttemptNumber), [1, 1, 2]);
  assert.equal(candidates[0].dueAt.toISOString(), "2026-08-24T13:00:00.000Z");
  assert.equal(candidates[1].dueAt.toISOString(), "2026-08-24T13:30:00.000Z");
});
