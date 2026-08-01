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
    "scheduledWindow must be one of morning_probe, mid_afternoon",
  );
});

test("blocks removed late-afternoon control window before dialing", () => {
  const reason = getStartCallWindowBlockReason(
    {
      scheduledWindow: "late_afternoon_control",
      agentTimeZone: "America/Chicago",
    },
    new Date("2026-06-23T19:30:00Z"),
  );

  assert.equal(reason, "scheduledWindow must be one of morning_probe, mid_afternoon");
});

test("allows the approved weekday experiment buckets during their listing-local windows", () => {
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
    "scheduledWindow must be one of morning_probe, mid_afternoon",
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
