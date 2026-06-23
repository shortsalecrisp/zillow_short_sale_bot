import assert from "node:assert/strict";
import test from "node:test";

import { getStartCallWindowBlockReason } from "../src/lib/callWindowGuard";

test("blocks old early-afternoon queue requests before dialing", () => {
  const reason = getStartCallWindowBlockReason(
    {
      scheduledWindow: "early_afternoon",
      agentTimeZone: "America/Chicago",
    },
    new Date("2026-06-23T19:30:00Z"),
  );

  assert.equal(reason, "scheduledWindow must be late_afternoon_control");
});

test("blocks late-afternoon requests before the listing local 4pm window", () => {
  const reason = getStartCallWindowBlockReason(
    {
      scheduledWindow: "late_afternoon_control",
      agentTimeZone: "America/Chicago",
    },
    new Date("2026-06-23T19:30:00Z"),
  );

  assert.equal(reason, "current listing-local time is outside the 4:00-5:00pm call window");
});

test("allows late-afternoon requests during the listing local 4pm window", () => {
  const reason = getStartCallWindowBlockReason(
    {
      scheduledWindow: "late_afternoon_control",
      agentTimeZone: "America/Chicago",
    },
    new Date("2026-06-23T21:30:00Z"),
  );

  assert.equal(reason, null);
});
