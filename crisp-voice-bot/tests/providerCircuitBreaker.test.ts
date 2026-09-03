import assert from "node:assert/strict";
import test from "node:test";
import {
  claimProviderCircuitAlertAttempt,
  getProviderCircuitStatus,
  markProviderCircuitAlertSent,
  recordElevenLabsLlmFailure,
  recordProviderQuotaFailure,
  recordTelnyxD17Failure,
  resetProviderCircuit,
  resetProviderCircuitForTests,
} from "../src/lib/providerCircuitBreaker";

function failure(rowNumber: number, conversationId: string) {
  return {
    rowNumber,
    conversationId,
    callAttemptNumber: 1,
    reason: "SIP status: 403. Account is disabled. D17",
  };
}

test("two consecutive D17 failures within ten minutes open the provider circuit", () => {
  resetProviderCircuitForTests();
  const first = recordTelnyxD17Failure(failure(4593, "conv_one"), new Date("2026-07-24T12:00:00Z"));
  const second = recordTelnyxD17Failure(failure(4595, "conv_two"), new Date("2026-07-24T12:09:59Z"));

  assert.equal(first.status.open, false);
  assert.equal(second.justOpened, true);
  assert.equal(second.status.open, true);
  assert.equal(second.status.consecutiveFailures, 2);
  assert.deepEqual(second.status.evidence.map((item) => item.rowNumber), [4593, 4595]);
});

test("D17 failures outside the ten-minute window do not accumulate", () => {
  resetProviderCircuitForTests();
  recordTelnyxD17Failure(failure(4593, "conv_one"), new Date("2026-07-24T12:00:00Z"));
  const result = recordTelnyxD17Failure(failure(4595, "conv_two"), new Date("2026-07-24T12:10:01Z"));

  assert.equal(result.status.open, false);
  assert.equal(result.status.consecutiveFailures, 1);
});

test("alert is claimed once and confirmed restoration resets the circuit", () => {
  resetProviderCircuitForTests();
  recordTelnyxD17Failure(failure(4593, "conv_one"), new Date("2026-07-24T12:00:00Z"));
  recordTelnyxD17Failure(failure(4595, "conv_two"), new Date("2026-07-24T12:01:00Z"));

  assert.ok(claimProviderCircuitAlertAttempt(new Date("2026-07-24T12:01:01Z")));
  markProviderCircuitAlertSent(new Date("2026-07-24T12:01:02Z"));
  assert.equal(claimProviderCircuitAlertAttempt(new Date("2026-07-24T12:20:00Z")), undefined);

  const restored = resetProviderCircuit("Telnyx support confirmed restoration", new Date("2026-07-24T12:30:00Z"));
  assert.equal(restored.open, false);
  assert.equal(restored.consecutiveFailures, 0);
  assert.equal(getProviderCircuitStatus().resetReason, "Telnyx support confirmed restoration");
});

test("one provider quota failure opens the global circuit immediately", () => {
  resetProviderCircuitForTests();
  const result = recordProviderQuotaFailure(
    { ...failure(5001, "conv_quota"), reason: "ElevenLabs quota exceeded" },
    new Date("2026-08-03T12:00:00Z"),
  );

  assert.equal(result.justOpened, true);
  assert.equal(result.status.open, true);
  assert.equal(result.status.signature, "elevenlabs_quota_exceeded");
  assert.equal(result.status.threshold, 1);
});

test("one ElevenLabs LLM cascade failure opens its own circuit signature", () => {
  resetProviderCircuitForTests();
  const result = recordElevenLabsLlmFailure(
    { ...failure(5589, "conv_llm"), reason: "LLM Cascade Error:" },
    new Date("2026-09-03T13:16:01Z"),
  );

  assert.equal(result.justOpened, true);
  assert.equal(result.status.open, true);
  assert.equal(result.status.signature, "elevenlabs_llm_failure");
  assert.equal(result.status.threshold, 1);
  assert.deepEqual(result.status.evidence.map((item) => item.rowNumber), [5589]);
});
