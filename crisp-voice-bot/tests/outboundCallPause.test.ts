import assert from "node:assert/strict";
import test from "node:test";
import { getOutboundCallPause } from "../src/lib/outboundCallPause";

test("outbound calling is paused through Memorial Day", () => {
  const pause = getOutboundCallPause(new Date("2026-05-25T21:00:00.000Z"));

  assert.equal(pause?.reason, "Memorial Day pause");
  assert.equal(pause?.pausedUntil.toISOString(), "2026-05-26T04:00:00.000Z");
});

test("outbound calling resumes on May 26 in Eastern time", () => {
  const pause = getOutboundCallPause(new Date("2026-05-26T04:00:00.000Z"));

  assert.equal(pause, undefined);
});
