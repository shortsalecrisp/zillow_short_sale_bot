import assert from "node:assert/strict";
import test from "node:test";
import { getVoiceBotFirstAttemptWindowName } from "../src/lib/voiceSheet";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.GOOGLE_SHEETS_TAB_NAME = "Sheet1";

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

test("Indian Harbour Beach becomes eligible only in its assigned Eastern window and retries next slot", async () => {
  const { getVoiceBotCallCandidateFromRowValues } = await import("../src/lib/voiceQueue");
  const values = row("2026-10-16T14:18:32-04:00");
  values[2] = fixturePhone("reach_morning_v1");
  values[4] = "159 Kristi Drive";
  values[5] = "Indian Harbour Beach";
  values[6] = "FL";
  assert.equal(getVoiceBotCallCandidateFromRowValues(6270, values, new Date("2026-10-19T13:14:59Z")), undefined);
  const candidate = getVoiceBotCallCandidateFromRowValues(6270, values, new Date("2026-10-19T13:15:00Z"));
  assert.equal(candidate?.agentTimeZone, "America/New_York");
  assert.equal(candidate?.callWindow, "reach_morning_v1");
  assert.equal(candidate?.callAttemptNumber, 1);
  assert.equal(getVoiceBotCallCandidateFromRowValues(6270, values, new Date("2026-10-19T13:45:00Z")), undefined);
  values[32] = "2026-10-19T13:20:00Z";
  values[33] = "voicemail_left";
  const retry = getVoiceBotCallCandidateFromRowValues(6270, values, new Date("2026-10-19T19:00:00Z"));
  assert.equal(retry?.callAttemptNumber, 2);
  assert.equal(retry?.callWindow, "reach_afternoon_v1");
  assert.equal(retry?.dueAt.toISOString(), "2026-10-19T19:00:00.000Z");
});

test("automatic voicemail retries advance to the next local slot despite old two-day AF dates", async () => {
  const { getVoiceBotCallCandidateFromRowValues } = await import("../src/lib/voiceQueue");
  for (const fixture of [
    { first: "2026-10-07T13:30:00Z", oldDate: "2026-10-09T19:00:00Z", before: "2026-10-07T13:35:00Z", next: "2026-10-07T19:15:00Z", due: "2026-10-07T19:00:00.000Z" },
    { first: "2026-10-07T19:30:00Z", oldDate: "2026-10-09T13:15:00Z", before: "2026-10-07T19:35:00Z", next: "2026-10-08T13:30:00Z", due: "2026-10-08T13:15:00.000Z" },
  ]) {
    const values = scheduledRow("", { call1SentAt: fixture.first, call1Result: "voicemail_left", scheduledFor: fixture.oldDate });
    values[29] = "yes";
    values[30] = "voice_call_2_due";
    assert.equal(getVoiceBotCallCandidateFromRowValues(6001, values, new Date(fixture.before)), undefined);
    const candidate = getVoiceBotCallCandidateFromRowValues(6001, values, new Date(fixture.next));
    assert.equal(candidate?.callAttemptNumber, 2);
    assert.equal(candidate?.dueAt.toISOString(), fixture.due);
    assert.equal(values[31], fixture.oldDate, "eligibility evaluation must not rewrite CRM cells");
    values[39] = fixture.next;
    assert.equal(getVoiceBotCallCandidateFromRowValues(6001, values, new Date(fixture.next)), undefined);
  }
});

test("next-slot retry keeps explicit schedules, handoffs, Mailshake and historical freezes protected", async () => {
  const { getVoiceBotCallCandidateFromRowValues } = await import("../src/lib/voiceQueue");
  const now = new Date("2026-10-07T19:15:00Z");
  const values = scheduledRow("", {
    call1SentAt: "2026-10-07T13:30:00Z", call1Result: "voicemail_left", scheduledFor: "2026-10-09T19:00:00Z",
  });
  assert.equal(getVoiceBotCallCandidateFromRowValues(6001, values, now), undefined, "an explicit non-cadence date must be honored");
  values[29] = "yes";
  values[30] = "voice_call_2_due";
  for (const columnIndex of [10, 35, 36, 37, 38]) {
    values[columnIndex] = columnIndex === 10 ? "N" : "yes";
    assert.equal(getVoiceBotCallCandidateFromRowValues(6001, values, now), undefined);
    values[columnIndex] = "";
  }
  values[32] = "2026-09-24T13:30:00Z";
  values[31] = "2026-09-28T19:00:00Z";
  assert.equal(getVoiceBotCallCandidateFromRowValues(6001, values, now), undefined);
});

test("Render queue prioritizes first calls, then oldest due time", async () => {
  const { getVoiceBotCallCandidatesFromRows } = await import("../src/lib/voiceQueue");
  const candidates = getVoiceBotCallCandidatesFromRows(
    [
      {
        rowNumber: 6001,
        values: row("", {
          call1SentAt: "2026-08-24T13:15:00Z",
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

test("an overdue scheduled no-start gets one future safe-replacement plan, including status N", async () => {
  const { getScheduledVoiceNoStartRecoveryPlan } = await import("../src/lib/voiceQueue");
  const now = new Date("2026-10-16T12:00:00.000Z");
  const values = scheduledRow("2026-10-14T14:00:00.000Z", {
    scheduledFor: "2026-10-15T13:15:00.000Z",
  });
  values[10] = "N";
  values[29] = "yes";
  values[30] = "voice_call_1_due";

  const plan = getScheduledVoiceNoStartRecoveryPlan(6188, values, now);
  assert.equal(plan?.rowNumber, 6188);
  assert.equal(plan?.callAttemptNumber, 1);
  assert.equal(plan?.originalScheduledFor.toISOString(), "2026-10-15T13:15:00.000Z");
  assert.ok(plan && plan.replacementAt.getTime() - now.getTime() >= 24 * 60 * 60_000);
  assert.equal(plan?.marker.candidateDueAt, "2026-10-15T13:15:00.000Z");

  values[32] = "2026-10-15T13:16:00.000Z";
  assert.equal(getScheduledVoiceNoStartRecoveryPlan(6188, values, now), undefined);
});

test("scheduled no-start recovery ignores current schedules, handoffs, and historical rows", async () => {
  const { getScheduledVoiceNoStartRecoveryPlan } = await import("../src/lib/voiceQueue");
  const now = new Date("2026-10-16T12:00:00.000Z");
  const values = scheduledRow("2026-10-14T14:00:00.000Z", {
    scheduledFor: "2026-10-15T13:15:00.000Z",
  });
  values[29] = "yes";
  values[30] = "voice_call_1_due";
  values[35] = "yes";
  assert.equal(getScheduledVoiceNoStartRecoveryPlan(6188, values, now), undefined);
  values[35] = "";
  values[31] = "2026-10-16T13:15:00.000Z";
  assert.equal(getScheduledVoiceNoStartRecoveryPlan(6188, values, now), undefined);
  values[31] = "2026-09-25T13:15:00.000Z";
  assert.equal(getScheduledVoiceNoStartRecoveryPlan(6188, values, now), undefined);
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

test("safe uncertain-start replacement clears the blocking lead status and verifies K/AD/AE/AF", async () => {
  const {
    buildVoiceSafeReplacementWrites,
    verifyVoiceSafeReplacementReadback,
  } = await import("../src/lib/voiceQueue");
  const replacementAt = new Date("2026-10-14T13:15:00.000Z");
  const writes = buildVoiceSafeReplacementWrites(1, replacementAt, "Replacement proof");
  assert.deepEqual(
    writes.filter((write) => [11, 30, 31, 32].includes(write.columnNumber)).map((write) => [write.columnNumber, write.value]),
    [[11, ""], [30, "yes"], [31, "voice_call_1_due"], [32, replacementAt.toISOString()]],
  );

  const requestedRanges: string[] = [];
  const fakeSheets = {
    spreadsheets: {
      values: {
        batchGet: async ({ ranges }: { ranges: string[] }) => {
          requestedRanges.push(...ranges);
          return {
            data: {
              valueRanges: [
                {},
                { values: [["yes"]] },
                { values: [["voice_call_1_due"]] },
                { values: [[replacementAt.toISOString()]] },
              ],
            },
          };
        },
      },
    },
  };
  assert.deepEqual(
    await verifyVoiceSafeReplacementReadback(fakeSheets as never, 6130, 1, replacementAt),
    {
      leadStatusCode: "",
      callEligible: "yes",
      callTimeBucket: "voice_call_1_due",
      callScheduledFor: replacementAt.toISOString(),
    },
  );
  assert.deepEqual(requestedRanges, ["'Sheet1'!K6130", "'Sheet1'!AD6130", "'Sheet1'!AE6130", "'Sheet1'!AF6130"]);
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
