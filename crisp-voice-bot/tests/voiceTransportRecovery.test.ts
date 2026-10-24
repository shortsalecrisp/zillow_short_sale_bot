import assert from "node:assert/strict";
import test from "node:test";
import { hasUnresolvedVoiceStart, nextTransportRecoverySlot, transportFailureKind, VOICE_START_RESOLUTION_MARKER } from "../src/lib/voiceTransportRecovery";

test("only terminal proven unaccepted transport errors are classifiable", () => {
  const failed = { status: "failed", metadata: { call_duration_secs: 0, error: {
    error_type: "call_initialization_error", reason: "sip request timed out" } }, transcript: [] };
  assert.equal(transportFailureKind(failed), "transient");
  for (const reason of ["Network Blocked (SIP 603)", "Dialed number is not included in whitelisted countries USA,CAN D13"]) {
    assert.equal(transportFailureKind({ ...failed, metadata: { ...failed.metadata, error: { ...failed.metadata.error, reason } } }), "destination_blocked");
  }
  for (const extra of [{ status: "initiated" }, { has_audio: true }, { has_user_audio: true },
    { metadata: { ...failed.metadata, accepted_time_unix_secs: 123 } }, { metadata: { ...failed.metadata, call_duration_secs: 1 } },
    { transcript: [{ role: "user", message: "Hello" }] }]) assert.equal(transportFailureKind({ ...failed, ...extra }), undefined);
});

test("transport replacement uses a future weekday lane, never same-window recycling", () => {
  const now = new Date("2026-10-23T20:05:00Z");
  const next = nextTransportRecoverySlot(now, "America/Chicago", "+19013323708", 1);
  assert.equal(next.toISOString(), "2026-10-26T20:00:00.000Z");
  assert.ok(nextTransportRecoverySlot(now, "America/New_York", "+12025550123", 2) > now);
});

test("old uncertain audit markers are released only by exact structured terminal resolution", () => {
  const pending = 'CODEX_VOICE_CALL_START_UNCERTAIN_V1 {"rowNumber":123,"callAttemptNumber":1,"callStartRequestId":"request-1"}';
  const resolution = { rowNumber: 123, phone: "+12025550123", callAttemptNumber: 1, callStartRequestId: "request-1", conversationId: "conv_test", status: "definitive_failure" };
  const line = (d: unknown) => `${VOICE_START_RESOLUTION_MARKER} ${JSON.stringify(d)}`;
  assert.equal(hasUnresolvedVoiceStart(pending), true);
  assert.equal(hasUnresolvedVoiceStart(pending + "\n" + line(resolution)), false);
  for (const change of [{ status: "initiated" }, { rowNumber: 124 }, { callAttemptNumber: 2 },
    { callStartRequestId: "other" }, { conversationId: "" }, { phone: null }]) {
    assert.equal(hasUnresolvedVoiceStart(pending + "\n" + line({ ...resolution, ...change })), true);
  }
  assert.equal(hasUnresolvedVoiceStart(pending + "\nProvider failed; safe replacement scheduled."), true);
  assert.equal(hasUnresolvedVoiceStart("CODEX_VOICE_CALL_START_UNCERTAIN_V1 {broken"), true);
});
