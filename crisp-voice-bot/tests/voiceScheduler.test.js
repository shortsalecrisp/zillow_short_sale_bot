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

test("weekday calls are only eligible during the 4:00-5:00pm local window", () => {
  const morning = runSchedulerExpression(
    'getVoiceBotPreferredCallWindowName_(new Date("2026-05-04T14:00:00Z"), "America/New_York")',
  );
  const beforeWindow = runSchedulerExpression(
    'getVoiceBotPreferredCallWindowName_(new Date("2026-05-04T19:59:00Z"), "America/New_York")',
  );
  const afternoon = runSchedulerExpression(
    'getVoiceBotPreferredCallWindowName_(new Date("2026-05-04T20:15:00Z"), "America/New_York")',
  );
  const afterWindow = runSchedulerExpression(
    'getVoiceBotPreferredCallWindowName_(new Date("2026-05-04T21:00:00Z"), "America/New_York")',
  );

  assert.equal(morning, "");
  assert.equal(beforeWindow, "");
  assert.equal(afternoon, "afternoon");
  assert.equal(afterWindow, "");
});

test("weekend calls are only eligible during the 4:00-5:00pm local window", () => {
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
    'getVoiceBotPreferredCallWindowName_(new Date("2026-05-09T21:00:00Z"), "America/New_York")',
  );

  assert.equal(beforeWindow, "");
  assert.equal(saturdayWindow, "afternoon");
  assert.equal(sundayWindow, "afternoon");
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

test("first Emmy call uses the first afternoon window after follow-up text", () => {
  const beforeWindow = runSchedulerExpression(
    'getNextVoiceBotFirstAttemptWindowStart_(new Date("2026-05-04T19:00:00Z"), "America/New_York").toISOString()',
  );
  const duringWindow = runSchedulerExpression(
    'getNextVoiceBotFirstAttemptWindowStart_(new Date("2026-05-04T20:30:00Z"), "America/New_York").toISOString()',
  );
  const afterFridayWindow = runSchedulerExpression(
    'getNextVoiceBotFirstAttemptWindowStart_(new Date("2026-05-08T21:45:00Z"), "America/New_York").toISOString()',
  );
  const beforeSaturdayWindow = runSchedulerExpression(
    'getNextVoiceBotFirstAttemptWindowStart_(new Date("2026-05-09T18:00:00Z"), "America/New_York").toISOString()',
  );
  const duringSaturdayWindow = runSchedulerExpression(
    'getNextVoiceBotFirstAttemptWindowStart_(new Date("2026-05-09T20:30:00Z"), "America/New_York").toISOString()',
  );
  const afterSundayWindow = runSchedulerExpression(
    'getNextVoiceBotFirstAttemptWindowStart_(new Date("2026-05-10T21:30:00Z"), "America/New_York").toISOString()',
  );

  assert.equal(beforeWindow, "2026-05-04T20:00:00.000Z");
  assert.equal(duringWindow, "2026-05-04T20:30:00.000Z");
  assert.equal(afterFridayWindow, "2026-05-09T20:00:00.000Z");
  assert.equal(beforeSaturdayWindow, "2026-05-09T20:00:00.000Z");
  assert.equal(duringSaturdayWindow, "2026-05-09T20:30:00.000Z");
  assert.equal(afterSundayWindow, "2026-05-11T20:00:00.000Z");
});

test("second Emmy call uses the next calendar day afternoon window", () => {
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

  assert.equal(nextDay, "2026-05-05T20:00:00.000Z");
  assert.equal(saturdayAfterFriday, "2026-05-09T20:00:00.000Z");
  assert.equal(sundayAfterSaturday, "2026-05-10T20:00:00.000Z");
  assert.equal(mondayAfterSunday, "2026-05-11T20:00:00.000Z");
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
