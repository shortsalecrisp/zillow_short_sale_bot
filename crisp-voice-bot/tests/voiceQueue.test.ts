import assert from "node:assert/strict";
import test from "node:test";
import { getVoiceBotFirstAttemptWindowName } from "../src/lib/voiceSheet";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";

let nextFixturePhone = 21_755_50200;
function fixturePhone(window = "reach_afternoon_v1") {
  let phone: string;
  do { phone = `+1${nextFixturePhone++}`; } while (getVoiceBotFirstAttemptWindowName(phone) !== window);
  return phone;
}

function row(followupSentAt: string, options: { call1SentAt?: string; call1Result?: string } = {}): unknown[] {
  const values = Array(42).fill("");
  values[0] = "Test";
  values[1] = "Agent";
  values[2] = fixturePhone();
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
          call1SentAt: "2026-08-20T13:15:00Z",
          call1Result: "agent_not_available",
        }),
      },
      { rowNumber: 6002, values: row("2026-08-24T19:05:00Z") },
      { rowNumber: 6003, values: row("2026-08-23T22:45:00Z") },
    ],
    new Date("2026-08-24T19:30:00Z"),
    10,
  );

  assert.deepEqual(candidates.map((candidate) => candidate.rowNumber), [6003, 6002, 6001]);
  assert.deepEqual(candidates.map((candidate) => candidate.callAttemptNumber), [1, 1, 2]);
  assert.equal(candidates[0].dueAt.toISOString(), "2026-08-24T19:00:00.000Z");
  assert.equal(candidates[1].dueAt.toISOString(), "2026-08-24T19:05:00.000Z");
});

test("Render queue starts one morning call but keeps two mid-afternoon slots available", async () => {
  const { getVoiceBotStartableCallCandidatesFromRows } = await import("../src/lib/voiceQueue");
  const rows = [
    { rowNumber: 6101, values: row("2026-08-23T13:14:00-04:00") },
    { rowNumber: 6102, values: row("2026-08-23T13:15:00-04:00") },
    { rowNumber: 6103, values: row("2026-08-23T13:16:00-04:00") },
  ];
  for (const item of rows) item.values[2] = fixturePhone("reach_morning_v1");

  assert.deepEqual(
    getVoiceBotStartableCallCandidatesFromRows(rows, new Date("2026-08-24T13:30:00Z"), 10, 2)
      .map((candidate) => candidate.rowNumber),
    [6101],
  );
  for (const item of rows) item.values[2] = fixturePhone();
  assert.deepEqual(
    getVoiceBotStartableCallCandidatesFromRows(rows, new Date("2026-08-24T19:30:00Z"), 10, 2)
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
          call1SentAt: "2026-10-06T13:11:43.726Z",
          call1Result: "voicemail_left",
          scheduledFor: "2026-10-07T18:00:00.000Z",
        }),
      },
      { rowNumber: 5900, values: row("2026-10-07T22:45:00.000Z") },
      {
        rowNumber: 5901,
        values: scheduledRow("", {
          call1SentAt: "2026-10-06T13:11:43.726Z",
          call1Result: "voicemail_left",
          scheduledFor: "2026-10-07T18:00:00.000Z",
          call2SentAt: "2026-10-08T18:05:00.000Z",
        }),
      },
    ],
    new Date("2026-10-08T19:15:00.000Z"),
    10,
  );

  assert.deepEqual(candidates.map((candidate) => [candidate.rowNumber, candidate.overdueNoStartRecovery]), [
    [5850, true],
    [5900, false],
  ]);
});

test("pre-monitor overdue scheduled no-start rows stay frozen without a candidate", async () => {
  const { getVoiceBotCallCandidateFromRowValues } = await import("../src/lib/voiceQueue");
  const values = scheduledRow("", {
    call1SentAt: "2026-09-24T13:11:43.726Z",
    call1Result: "voicemail_left",
    scheduledFor: "2026-09-25T18:00:00.000Z",
  });
  assert.equal(getVoiceBotCallCandidateFromRowValues(5953, values, new Date("2026-10-06T19:15:00.000Z")), undefined);
});

test("scheduled no-start marker is durable and parseable", async () => {
  const { formatVoiceScheduledNoStartMarker, parseVoiceScheduledNoStartMarker } = await import("../src/lib/voiceQueue");
  const marker = {
    rowNumber: 6005,
    callAttemptNumber: 2 as const,
    detectedAt: "2026-10-08T19:15:00.000Z",
    candidateDueAt: "2026-10-07T18:00:00.000Z",
  };
  const formatted = formatVoiceScheduledNoStartMarker(marker);
  assert.match(formatted, /CODEX_VOICE_SCHEDULED_NO_START_V1/);
  assert.deepEqual(parseVoiceScheduledNoStartMarker(`older\n\n---\n\n${formatted}`), marker);
});

test("a stale first-attempt timestamp with no result receives exactly one bounded second-attempt candidate", async () => {
  const { getVoiceBotCallCandidateFromRowValues, isStaleVoiceBotStartWithoutReceipt } = await import("../src/lib/voiceQueue");
  const values = scheduledRow("", {
    call1SentAt: "2026-10-07T18:15:00.000Z",
    scheduledFor: "2026-10-07T18:00:00.000Z",
  });
  const now = new Date("2026-10-08T13:30:00.000Z");
  assert.equal(isStaleVoiceBotStartWithoutReceipt(values[32], values[33], now), true);
  assert.equal(getVoiceBotCallCandidateFromRowValues(5916, values, now)?.callAttemptNumber, 2);
  values[39] = "2026-10-08T13:26:00.000Z";
  assert.equal(getVoiceBotCallCandidateFromRowValues(5916, values, now), undefined);
});

test("uncertain call starts remain paused and carry a durable provider receipt key", async () => {
  const {
    formatVoiceCallStartUncertainMarker,
    getVoiceBotCallCandidateFromRowValues,
    parseVoiceCallStartUncertainMarker,
  } = await import("../src/lib/voiceQueue");
  const marker = {
    rowNumber: 5939,
    callAttemptNumber: 1 as const,
    callStartRequestId: "5939-1-request-safe",
    requestStartedAtUnixSecs: 1_790_866_846,
    scheduledWindow: "mid_afternoon",
    agentTimeZone: "America/New_York",
  };
  const values = scheduledRow("2026-10-01T14:00:00.000Z", {
    call1SentAt: "2026-10-01T15:00:46.110Z",
    call1Result: "call_start_uncertain",
  });
  values[29] = "";
  values[30] = "";
  values[31] = "";
  values[41] = `Earlier note\n\n---\n\n${formatVoiceCallStartUncertainMarker(marker)}`;

  assert.deepEqual(parseVoiceCallStartUncertainMarker(values[41]), marker);
  assert.equal(getVoiceBotCallCandidateFromRowValues(5939, values, new Date("2026-10-02T15:00:00Z")), undefined);
});

test("phone-scoped guards preserve no-call preferences and prevent duplicate-listing resets", async () => {
  const { getVoiceBotCallCandidatesFromRows } = await import("../src/lib/voiceQueue");
  const now = new Date("2026-10-05T19:15:00Z");
  const first = row("2026-10-02T21:00:00Z");
  const duplicate = row("2026-10-02T21:00:00Z");
  duplicate[2] = first[2];
  const rows = [{ rowNumber: 6001, values: first }, { rowNumber: 6002, values: duplicate }];
  assert.deepEqual(getVoiceBotCallCandidatesFromRows(rows, now, 10).map((item) => item.rowNumber), [6001]);
  duplicate[10] = "Y";
  assert.equal(getVoiceBotCallCandidatesFromRows(rows, now, 10).length, 0);
  duplicate[10] = "";
  duplicate[33] = "contact_request_review";
  assert.equal(getVoiceBotCallCandidatesFromRows(rows, now, 10).length, 0);
  duplicate[33] = "voicemail_left";
  duplicate[32] = "2026-10-01T13:30:00Z";
  assert.deepEqual(getVoiceBotCallCandidatesFromRows(rows, now, 10).map((item) => [item.rowNumber, item.callAttemptNumber]), [[6002, 2]]);
  duplicate[39] = "2026-10-05T19:10:00Z";
  duplicate[40] = "voicemail_left";
  assert.equal(getVoiceBotCallCandidatesFromRows(rows, now, 10).length, 0);
});

test("unknown listing timezone fails closed and legacy AF is realigned", async () => {
  const { getVoiceBotCallCandidateFromRowValues } = await import("../src/lib/voiceQueue");
  const values = scheduledRow("2026-10-05T13:00:00Z", { scheduledFor: "2026-10-05T18:00:00Z" });
  assert.equal(getVoiceBotCallCandidateFromRowValues(6001, values, new Date("2026-10-05T18:15:00Z")), undefined);
  assert.equal(getVoiceBotCallCandidateFromRowValues(6001, values, new Date("2026-10-05T19:15:00Z"))?.dueAt.toISOString(), "2026-10-05T19:00:00.000Z");
  values[5] = "Unknown";
  values[6] = "FL";
  assert.equal(getVoiceBotCallCandidateFromRowValues(6001, values, new Date("2026-10-05T19:15:00Z")), undefined);
});
