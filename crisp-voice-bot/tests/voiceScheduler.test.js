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

test("only the 4:30-5:30pm local window is eligible", () => {
  const morning = runSchedulerExpression(
    'getVoiceBotPreferredCallWindowName_(new Date("2026-05-04T14:00:00Z"), "America/New_York")',
  );
  const afternoon = runSchedulerExpression(
    'getVoiceBotPreferredCallWindowName_(new Date("2026-05-04T20:45:00Z"), "America/New_York")',
  );

  assert.equal(morning, "");
  assert.equal(afternoon, "afternoon");
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

  assert.equal(beforeWindow, "2026-05-04T20:30:00.000Z");
  assert.equal(duringWindow, "2026-05-05T20:30:00.000Z");
  assert.equal(afterFridayWindow, "2026-05-11T20:30:00.000Z");
});

test("second Emmy call uses the next business day afternoon window", () => {
  const nextDay = runSchedulerExpression(
    'getNextVoiceBotFollowupAttemptWindowStart_(new Date("2026-05-04T20:45:00Z"), "America/New_York").toISOString()',
  );
  const mondayAfterFriday = runSchedulerExpression(
    'getNextVoiceBotFollowupAttemptWindowStart_(new Date("2026-05-08T20:45:00Z"), "America/New_York").toISOString()',
  );

  assert.equal(nextDay, "2026-05-05T20:30:00.000Z");
  assert.equal(mondayAfterFriday, "2026-05-11T20:30:00.000Z");
});
