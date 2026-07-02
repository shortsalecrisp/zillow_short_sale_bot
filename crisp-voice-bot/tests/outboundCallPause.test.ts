import assert from "node:assert/strict";
import test from "node:test";
import { getOutboundCallPause } from "../src/lib/outboundCallPause";

test("outbound calling is paused while ElevenLabs quota is exhausted", () => {
  const pause = getOutboundCallPause(new Date("2026-07-02T21:00:00.000Z"));

  assert.equal(pause?.reason, "Paused while ElevenLabs quota is exhausted");
  assert.equal(pause?.pausedUntil.toISOString(), "2026-08-01T03:59:59.000Z");
});

test("outbound calling resumes after the emergency quota pause window", () => {
  const pause = getOutboundCallPause(new Date("2026-08-01T03:59:59.000Z"));

  assert.equal(pause, undefined);
});
