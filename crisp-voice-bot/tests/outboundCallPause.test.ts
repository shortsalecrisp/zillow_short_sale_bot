import assert from "node:assert/strict";
import test from "node:test";
import { getOutboundCallPause } from "../src/lib/outboundCallPause";

test("outbound calling is paused until the ElevenLabs billing cycle refresh", () => {
  const pause = getOutboundCallPause(new Date("2026-07-21T21:00:00.000Z"));

  assert.equal(pause?.reason, "Paused until ElevenLabs billing cycle refreshes on July 22");
  assert.equal(pause?.pausedUntil.toISOString(), "2026-07-22T13:00:00.000Z");
});

test("outbound calling resumes after the billing-cycle refresh pause window", () => {
  const pause = getOutboundCallPause(new Date("2026-07-22T13:00:00.000Z"));

  assert.equal(pause, undefined);
});
