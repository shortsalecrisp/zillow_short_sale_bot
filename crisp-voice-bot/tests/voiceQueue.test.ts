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

function scheduledRow(
  followupSentAt: string,
  options: { call1SentAt?: string; call1Result?: string; scheduledFor?: string; call2SentAt?: string } = {},
): unknown[] {
  const values = row(followupSentAt, options);
  values[31] = options.scheduledFor ?? "";
  values[39] = options.call2SentAt ?? "";
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

test("Render queue starts one morning call but keeps two mid-afternoon slots available", async () => {
  const { getVoiceBotStartableCallCandidatesFromRows } = await import("../src/lib/voiceQueue");
  const rows = [
    { rowNumber: 6101, values: row("2026-08-23T13:14:00-04:00") },
    { rowNumber: 6102, values: row("2026-08-23T13:15:00-04:00") },
    { rowNumber: 6103, values: row("2026-08-23T13:16:00-04:00") },
  ];

  assert.deepEqual(
    getVoiceBotStartableCallCandidatesFromRows(rows, new Date("2026-08-24T13:45:00Z"), 10, 2)
      .map((candidate) => candidate.rowNumber),
    [6101],
  );
  assert.deepEqual(
    getVoiceBotStartableCallCandidatesFromRows(rows, new Date("2026-08-24T18:45:00Z"), 10, 2)
      .map((candidate) => candidate.rowNumber),
    [6101, 6102],
  );
});

test("Render queue prioritizes an overdue scheduled no-start until the attempt is marked started", async () => {
  const { getVoiceBotCallCandidatesFromRows } = await import("../src/lib/voiceQueue");
  const candidates = getVoiceBotCallCandidatesFromRows(
    [
      {
        rowNumber: 5850,
        values: scheduledRow("", {
          call1SentAt: "2026-09-24T13:11:43.726Z",
          call1Result: "voicemail_left",
          scheduledFor: "2026-09-25T18:00:00.000Z",
        }),
      },
      { rowNumber: 5900, values: row("2026-09-25T22:45:00.000Z") },
      {
        rowNumber: 5901,
        values: scheduledRow("", {
          call1SentAt: "2026-09-24T13:11:43.726Z",
          call1Result: "voicemail_left",
          scheduledFor: "2026-09-25T18:00:00.000Z",
          call2SentAt: "2026-09-26T18:05:00.000Z",
        }),
      },
    ],
    new Date("2026-09-26T18:45:00.000Z"),
    10,
  );

  assert.deepEqual(candidates.map((candidate) => [candidate.rowNumber, candidate.overdueNoStartRecovery]), [
    [5850, true],
    [5900, false],
  ]);
});
