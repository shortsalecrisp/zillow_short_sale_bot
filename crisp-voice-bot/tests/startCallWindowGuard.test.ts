import assert from "node:assert/strict";
import test from "node:test";

import { getStartCallWindowBlockReason } from "../src/lib/callWindowGuard";

test("blocks unknown queue windows before dialing", () => {
  const reason = getStartCallWindowBlockReason(
    {
      scheduledWindow: "dinner",
      agentTimeZone: "America/Chicago",
    },
    new Date("2026-06-23T19:30:00Z"),
  );

  assert.equal(
    reason,
    "scheduledWindow must be one of morning_probe, early_afternoon, mid_afternoon, late_afternoon_control",
  );
});

test("blocks approved windows outside their listing-local time", () => {
  const reason = getStartCallWindowBlockReason(
    {
      scheduledWindow: "late_afternoon_control",
      agentTimeZone: "America/Chicago",
    },
    new Date("2026-06-23T19:30:00Z"),
  );

  assert.equal(reason, "current listing-local time is outside the late_afternoon_control call window");
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

test("allows morning and afternoon experiment buckets during their listing-local windows", () => {
  assert.equal(
    getStartCallWindowBlockReason(
      {
        scheduledWindow: "morning_probe",
        agentTimeZone: "America/Chicago",
      },
      new Date("2026-06-23T14:30:00Z"),
    ),
    null,
  );
  assert.equal(
    getStartCallWindowBlockReason(
      {
        scheduledWindow: "early_afternoon",
        agentTimeZone: "America/Chicago",
      },
      new Date("2026-06-23T18:00:00Z"),
    ),
    null,
  );
  assert.equal(
    getStartCallWindowBlockReason(
      {
        scheduledWindow: "mid_afternoon",
        agentTimeZone: "America/Chicago",
      },
      new Date("2026-06-23T20:00:00Z"),
    ),
    null,
  );
});
