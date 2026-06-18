const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

function getLocalParts(date, timeZone) {
  const formatter = new Intl.DateTimeFormat("en-US", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
    weekday: "short",
  });
  const parts = {};

  for (const part of formatter.formatToParts(date)) {
    if (part.type !== "literal") {
      parts[part.type] = part.value;
    }
  }

  return parts;
}

function getOffset(date, timeZone) {
  const parts = getLocalParts(date, timeZone);
  const utcForLocal = Date.UTC(
    Number(parts.year),
    Number(parts.month) - 1,
    Number(parts.day),
    Number(parts.hour),
    Number(parts.minute),
  );
  const offsetMinutes = Math.round((utcForLocal - date.getTime()) / 60000);
  const sign = offsetMinutes >= 0 ? "+" : "-";
  const absolute = Math.abs(offsetMinutes);
  const hours = String(Math.floor(absolute / 60)).padStart(2, "0");
  const minutes = String(absolute % 60).padStart(2, "0");

  return `${sign}${hours}${minutes}`;
}

function formatDate(date, timeZone, pattern) {
  if (timeZone === "UTC") {
    const yyyy = date.getUTCFullYear();
    const mm = String(date.getUTCMonth() + 1).padStart(2, "0");
    const dd = String(date.getUTCDate()).padStart(2, "0");
    if (pattern === "yyyy-MM-dd") {
      return `${yyyy}-${mm}-${dd}`;
    }
  }

  const parts = getLocalParts(date, timeZone);
  if (pattern === "u") {
    return String(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"].indexOf(parts.weekday) + 1);
  }
  if (pattern === "H") {
    return String(Number(parts.hour));
  }
  if (pattern === "m") {
    return String(Number(parts.minute));
  }
  if (pattern === "yyyy-MM-dd") {
    return `${parts.year}-${parts.month}-${parts.day}`;
  }
  if (pattern === "Z") {
    return getOffset(date, timeZone);
  }
  if (pattern === "yyyy-MM-dd HH:mm:ss") {
    return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute}:00`;
  }

  throw new Error(`Unsupported date pattern in test: ${pattern}`);
}

function loadSchedulerContext() {
  const source = fs.readFileSync(
    path.resolve(__dirname, "../apps-script/voice-bot-callback.gs"),
    "utf8",
  );
  const context = vm.createContext({
    Date,
    Logger: { log() {} },
    Utilities: { formatDate },
  });

  vm.runInContext(source, context);
  return context;
}

function runSchedulerExpression(expression) {
  return vm.runInContext(expression, loadSchedulerContext());
}

function runSchedulerScript(script) {
  return vm.runInContext(script, loadSchedulerContext());
}

test("weekday calls are eligible during late morning, early afternoon, and late-afternoon control windows", () => {
  const beforeLateMorning = runSchedulerExpression(
    'getVoiceBotPreferredCallWindowName_(new Date("2026-05-04T14:00:00Z"), "America/New_York")',
  );
  const lateMorning = runSchedulerExpression(
    'getVoiceBotPreferredCallWindowName_(new Date("2026-05-04T14:45:00Z"), "America/New_York")',
  );
  const earlyAfternoon = runSchedulerExpression(
    'getVoiceBotPreferredCallWindowName_(new Date("2026-05-04T17:45:00Z"), "America/New_York")',
  );
  const betweenAfternoonWindows = runSchedulerExpression(
    'getVoiceBotPreferredCallWindowName_(new Date("2026-05-04T19:15:00Z"), "America/New_York")',
  );
  const lateAfternoonControl = runSchedulerExpression(
    'getVoiceBotPreferredCallWindowName_(new Date("2026-05-04T20:15:00Z"), "America/New_York")',
  );
  const afterWindow = runSchedulerExpression(
    'getVoiceBotPreferredCallWindowName_(new Date("2026-05-04T22:00:00Z"), "America/New_York")',
  );

  assert.equal(beforeLateMorning, "");
  assert.equal(lateMorning, "late_morning");
  assert.equal(earlyAfternoon, "early_afternoon");
  assert.equal(betweenAfternoonWindows, "");
  assert.equal(lateAfternoonControl, "late_afternoon_control");
  assert.equal(afterWindow, "");
});

test("weekend calls stay in the late-afternoon control window only", () => {
  const beforeWindow = runSchedulerExpression(
    'getVoiceBotPreferredCallWindowName_(new Date("2026-05-09T19:59:00Z"), "America/New_York")',
  );
  const saturdayWindow = runSchedulerExpression(
    'getVoiceBotPreferredCallWindowName_(new Date("2026-05-09T20:15:00Z"), "America/New_York")',
  );
  const sundayWindow = runSchedulerExpression(
    'getVoiceBotPreferredCallWindowName_(new Date("2026-05-10T20:15:00Z"), "America/New_York")',
  );
  const afterWindow = runSchedulerExpression(
    'getVoiceBotPreferredCallWindowName_(new Date("2026-05-09T22:00:00Z"), "America/New_York")',
  );

  assert.equal(beforeWindow, "");
  assert.equal(saturdayWindow, "late_afternoon_control");
  assert.equal(sundayWindow, "late_afternoon_control");
  assert.equal(afterWindow, "");
});

test("queue runner is open on weekends so weekend-local calls can be placed", () => {
  const saturdayAfternoonEt = runSchedulerExpression(
    'isWithinVoiceBotQueueRunWindow_(new Date("2026-05-09T20:30:00Z"))',
  );
  const sundayAfternoonEt = runSchedulerExpression(
    'isWithinVoiceBotQueueRunWindow_(new Date("2026-05-10T20:30:00Z"))',
  );

  assert.equal(saturdayAfternoonEt, true);
  assert.equal(sundayAfternoonEt, true);
});

test("queue runner is paused through Memorial Day", () => {
  const memorialDayAfternoon = runSchedulerExpression(
    'isVoiceBotQueuePaused_(new Date("2026-05-25T21:00:00Z"))',
  );
  const nextDayEt = runSchedulerExpression(
    'isVoiceBotQueuePaused_(new Date("2026-05-26T04:00:00Z"))',
  );

  assert.equal(memorialDayAfternoon, true);
  assert.equal(nextDayEt, false);
});

test("voice performance logs append to the one AP cell instead of replacing earlier attempts", () => {
  const stored = runSchedulerScript(`
    let value = "--- CODEX_VOICE_CALL_METRICS_V1 ---\\n{\\"call\\":{\\"callAttemptNumber\\":1}}";
    const writes = [];
    const sheet = {
      getRange(rowNumber, columnNumber) {
        return {
          getValue() {
            return value;
          },
          setValue(nextValue) {
            value = nextValue;
          }
        };
      }
    };

    appendVoiceBotFieldIfPresent_(sheet, 3264, VOICE_BOT_COL_VOICE_NOTES, {
      voiceNotes: "--- CODEX_VOICE_CALL_METRICS_V1 ---\\n{\\"call\\":{\\"callAttemptNumber\\":2}}"
    }, "voiceNotes", writes);

    value;
  `);

  assert.match(stored, /"callAttemptNumber":1/);
  assert.match(stored, /"callAttemptNumber":2/);
  assert.match(stored, /\n\n---\n\n--- CODEX_VOICE_CALL_METRICS_V1 ---/);
});

test("start-call payload carries the scheduled call window but not the AP analysis cell as runtime notes", () => {
  const payload = JSON.parse(runSchedulerExpression(`JSON.stringify(buildVoiceBotStartCallPayload_({
    rowNumber: 3264,
    callAttemptNumber: 2,
    firstName: "Jane",
    lastName: "Agent",
    fullName: "Jane Agent",
    phone: "+12175550123",
    email: "jane@example.com",
    listingAddress: "123 Main St, Tampa, FL",
    createdAt: "2026-05-01T10:00:00-04:00",
    existingResponseStatus: "Left Vm",
    voiceNotes: "--- CODEX_VOICE_CALL_METRICS_V1 ---",
    dueAt: new Date("2026-05-02T20:00:00Z"),
    callWindow: "late_afternoon_control",
    agentTimeZone: "America/New_York"
  }))`));

  assert.equal(payload.notes, undefined);
  assert.equal(payload.scheduledWindow, "late_afternoon_control");
  assert.equal(payload.agentTimeZone, "America/New_York");
});

test("voice performance log-only updates do not change scheduling cells", () => {
  const result = JSON.parse(runSchedulerScript(`
    const cleared = [];
    const fields = [];
    const sheet = {
      getRange(rowNumber, columnNumber) {
        return {
          getValue() {
            return "existing";
          },
          clearContent() {
            cleared.push(columnNumber);
          },
          setValue() {
            throw new Error("log-only update should not write scheduling fields");
          }
        };
      }
    };

    updateVoiceBotSchedulingCells_(sheet, 3264, { voiceNotes: "metrics only" }, 2, fields);
    JSON.stringify({ cleared, fields });
  `));

  assert.deepEqual(result, { cleared: [], fields: [] });
});

test("first voice call uses the next local test window after follow-up text", () => {
  const beforeLateMorning = runSchedulerExpression(
    'getNextVoiceBotFirstAttemptWindowStart_(new Date("2026-05-04T13:00:00Z"), "America/New_York").toISOString()',
  );
  const duringLateMorning = runSchedulerExpression(
    'getNextVoiceBotFirstAttemptWindowStart_(new Date("2026-05-04T15:00:00Z"), "America/New_York").toISOString()',
  );
  const betweenMorningAndAfternoon = runSchedulerExpression(
    'getNextVoiceBotFirstAttemptWindowStart_(new Date("2026-05-04T16:00:00Z"), "America/New_York").toISOString()',
  );
  const betweenAfternoonWindows = runSchedulerExpression(
    'getNextVoiceBotFirstAttemptWindowStart_(new Date("2026-05-04T19:15:00Z"), "America/New_York").toISOString()',
  );
  const duringLateAfternoon = runSchedulerExpression(
    'getNextVoiceBotFirstAttemptWindowStart_(new Date("2026-05-04T20:30:00Z"), "America/New_York").toISOString()',
  );
  const afterFridayWindow = runSchedulerExpression(
    'getNextVoiceBotFirstAttemptWindowStart_(new Date("2026-05-08T22:15:00Z"), "America/New_York").toISOString()',
  );
  const beforeSaturdayWindow = runSchedulerExpression(
    'getNextVoiceBotFirstAttemptWindowStart_(new Date("2026-05-09T18:00:00Z"), "America/New_York").toISOString()',
  );
  const duringSaturdayWindow = runSchedulerExpression(
    'getNextVoiceBotFirstAttemptWindowStart_(new Date("2026-05-09T20:30:00Z"), "America/New_York").toISOString()',
  );
  const afterSundayWindow = runSchedulerExpression(
    'getNextVoiceBotFirstAttemptWindowStart_(new Date("2026-05-10T22:30:00Z"), "America/New_York").toISOString()',
  );

  assert.equal(beforeLateMorning, "2026-05-04T14:30:00.000Z");
  assert.equal(duringLateMorning, "2026-05-04T15:00:00.000Z");
  assert.equal(betweenMorningAndAfternoon, "2026-05-04T17:30:00.000Z");
  assert.equal(betweenAfternoonWindows, "2026-05-04T20:00:00.000Z");
  assert.equal(duringLateAfternoon, "2026-05-04T20:30:00.000Z");
  assert.equal(afterFridayWindow, "2026-05-09T20:00:00.000Z");
  assert.equal(beforeSaturdayWindow, "2026-05-09T20:00:00.000Z");
  assert.equal(duringSaturdayWindow, "2026-05-09T20:30:00.000Z");
  assert.equal(afterSundayWindow, "2026-05-11T14:30:00.000Z");
});

test("second voice call uses the first local call window on the next calendar day", () => {
  const nextDay = runSchedulerExpression(
    'getNextVoiceBotFollowupAttemptWindowStart_(new Date("2026-05-04T20:45:00Z"), "America/New_York").toISOString()',
  );
  const saturdayAfterFriday = runSchedulerExpression(
    'getNextVoiceBotFollowupAttemptWindowStart_(new Date("2026-05-08T20:45:00Z"), "America/New_York").toISOString()',
  );
  const sundayAfterSaturday = runSchedulerExpression(
    'getNextVoiceBotFollowupAttemptWindowStart_(new Date("2026-05-09T20:15:00Z"), "America/New_York").toISOString()',
  );
  const mondayAfterSunday = runSchedulerExpression(
    'getNextVoiceBotFollowupAttemptWindowStart_(new Date("2026-05-10T20:15:00Z"), "America/New_York").toISOString()',
  );

  assert.equal(nextDay, "2026-05-05T14:30:00.000Z");
  assert.equal(saturdayAfterFriday, "2026-05-09T20:00:00.000Z");
  assert.equal(sundayAfterSaturday, "2026-05-10T20:00:00.000Z");
  assert.equal(mondayAfterSunday, "2026-05-11T14:30:00.000Z");
});

test("queue scan can return multiple eligible rows in the same local call window", () => {
  const rowNumbers = Array.from(runSchedulerScript(`
    function row(first, last, state, followupSentAt) {
      const values = Array(42).fill("");
      values[0] = first;
      values[1] = last;
      values[2] = "603-325-5909";
      values[4] = "20 Pearl Street";
      values[5] = "Hillsboro";
      values[6] = state;
      values[7] = "x";
      values[8] = "x";
      values[23] = followupSentAt;
      return values;
    }

    getVoiceBotCallCandidatesFromRows_([
      { rowNumber: 3366, values: row("Colleen", "Whitney", "NH", "2026-05-06T17:14:41.692330-04:00") },
      { rowNumber: 3367, values: row("Robert", "DeFalco", "NJ", "2026-05-06T19:49:46.290207-04:00") },
      { rowNumber: 3369, values: row("Silvia", "Andion", "FL", "2026-05-07T16:03:22.841775-04:00") },
    ], new Date("2026-05-07T20:15:00Z"), 10).map(function(candidate) {
      return candidate.rowNumber;
    });
  `));

  assert.deepEqual(rowNumbers, [3366, 3367, 3369]);
});

test("queue only starts enough calls to fill the two active call slots", () => {
  const rowNumbers = Array.from(runSchedulerScript(`
    function row(first, last, state, followupSentAt, options) {
      const values = Array(42).fill("");
      values[0] = first;
      values[1] = last;
      values[2] = "603-325-5909";
      values[4] = "20 Pearl Street";
      values[5] = "Hillsboro";
      values[6] = state;
      values[7] = "x";
      values[8] = "x";
      values[23] = followupSentAt;
      if (options && options.call1SentAt) {
        values[32] = options.call1SentAt;
      }
      if (options && options.call1Result) {
        values[33] = options.call1Result;
      }
      return values;
    }

    getVoiceBotStartableCallCandidatesFromRows_([
      { rowNumber: 3400, values: row("Active", "Call", "NH", "2026-05-08T17:14:41.692330-04:00", { call1SentAt: "2026-05-09T20:05:00Z" }) },
      { rowNumber: 3401, values: row("First", "Queued", "NH", "2026-05-08T17:14:41.692330-04:00") },
      { rowNumber: 3402, values: row("Second", "Queued", "NH", "2026-05-08T17:14:41.692330-04:00") },
    ], new Date("2026-05-09T20:15:00Z"), 10, 2).map(function(candidate) {
      return candidate.rowNumber;
    });
  `));

  assert.deepEqual(rowNumbers, [3401]);
});

test("stale unfinished call rows do not block new calls forever", () => {
  const rowNumbers = Array.from(runSchedulerScript(`
    function row(first, last, state, followupSentAt, options) {
      const values = Array(42).fill("");
      values[0] = first;
      values[1] = last;
      values[2] = "603-325-5909";
      values[4] = "20 Pearl Street";
      values[5] = "Hillsboro";
      values[6] = state;
      values[7] = "x";
      values[8] = "x";
      values[23] = followupSentAt;
      if (options && options.call1SentAt) {
        values[32] = options.call1SentAt;
      }
      return values;
    }

    getVoiceBotStartableCallCandidatesFromRows_([
      { rowNumber: 3400, values: row("Stale", "Call", "NH", "2026-05-08T17:14:41.692330-04:00", { call1SentAt: "2026-05-09T18:00:00Z" }) },
      { rowNumber: 3401, values: row("First", "Queued", "NH", "2026-05-08T17:14:41.692330-04:00") },
      { rowNumber: 3402, values: row("Second", "Queued", "NH", "2026-05-08T17:14:41.692330-04:00") },
      { rowNumber: 3403, values: row("Third", "Queued", "NH", "2026-05-08T17:14:41.692330-04:00") },
    ], new Date("2026-05-09T20:15:00Z"), 10, 2).map(function(candidate) {
      return candidate.rowNumber;
    });
  `));

  assert.deepEqual(rowNumbers, [3401, 3402]);
});

test("live-transfer-requested rows still count against active call slots", () => {
  const rowNumbers = Array.from(runSchedulerScript(`
    function row(first, last, state, followupSentAt, options) {
      const values = Array(42).fill("");
      values[0] = first;
      values[1] = last;
      values[2] = "603-325-5909";
      values[4] = "20 Pearl Street";
      values[5] = "Hillsboro";
      values[6] = state;
      values[7] = "x";
      values[8] = "x";
      values[23] = followupSentAt;
      if (options && options.call1SentAt) {
        values[32] = options.call1SentAt;
      }
      if (options && options.call1Result) {
        values[33] = options.call1Result;
      }
      return values;
    }

    getVoiceBotStartableCallCandidatesFromRows_([
      { rowNumber: 3400, values: row("Transfer", "Pending", "NH", "2026-05-08T17:14:41.692330-04:00", { call1SentAt: "2026-05-09T20:05:00Z", call1Result: "live_transfer_requested" }) },
      { rowNumber: 3401, values: row("First", "Queued", "NH", "2026-05-08T17:14:41.692330-04:00") },
      { rowNumber: 3402, values: row("Second", "Queued", "NH", "2026-05-08T17:14:41.692330-04:00") },
    ], new Date("2026-05-09T20:15:00Z"), 10, 2).map(function(candidate) {
      return candidate.rowNumber;
    });
  `));

  assert.deepEqual(rowNumbers, [3401]);
});
