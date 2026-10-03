import assert from "node:assert/strict";
import test from "node:test";
import { getStartCallWindowBlockReason } from "../src/lib/callWindowGuard";

test("cold window guard uses listing local time, exact boundaries, and weekdays", () => {
  const payload = { agentTimeZone: "America/Los_Angeles", scheduledWindow: "reach_morning_v1" };
  assert.ok(getStartCallWindowBlockReason(payload, new Date("2026-10-05T16:14:59Z")));
  assert.equal(getStartCallWindowBlockReason(payload, new Date("2026-10-05T16:15:00Z")), null);
  assert.equal(getStartCallWindowBlockReason(payload, new Date("2026-10-05T16:44:59Z")), null);
  assert.ok(getStartCallWindowBlockReason(payload, new Date("2026-10-05T16:45:00Z")));
  assert.ok(getStartCallWindowBlockReason(payload, new Date("2026-10-03T16:30:00Z")));
  assert.equal(getStartCallWindowBlockReason(payload, new Date("2026-11-02T17:30:00Z")), null);
});

test("guard preserves broader owner-approved manual routes without widening cold slots", () => {
  const now = new Date("2026-10-05T13:50:00Z");
  assert.equal(getStartCallWindowBlockReason({ agentTimeZone: "America/New_York", scheduledWindow: "morning_probe" }, now), null);
  assert.ok(getStartCallWindowBlockReason({ agentTimeZone: "America/New_York", scheduledWindow: "reach_morning_v1" }, now));
  assert.ok(getStartCallWindowBlockReason({ agentTimeZone: "", scheduledWindow: "reach_morning_v1" }, now));
  assert.ok(getStartCallWindowBlockReason({ agentTimeZone: "invalid", scheduledWindow: "reach_morning_v1" }, now));
});
