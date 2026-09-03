import assert from "node:assert/strict";
import test from "node:test";
import type { ProviderCircuitStatus } from "../src/lib/providerCircuitBreaker";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";

function status(signature: string, reason: string): ProviderCircuitStatus {
  return {
    open: true,
    signature,
    consecutiveFailures: signature === "sip_403_account_disabled_d17" ? 2 : 1,
    threshold: signature === "sip_403_account_disabled_d17" ? 2 : 1,
    windowMinutes: signature === "sip_403_account_disabled_d17" ? 10 : 0,
    openedAt: "2026-09-03T13:16:01.544Z",
    evidence: [
      {
        rowNumber: 5589,
        callAttemptNumber: 1,
        conversationId: "conv_9901m1kpg3n4f42a0f3aj0rfq0rh",
        occurredAt: "2026-09-03T13:16:01.544Z",
        reason,
      },
    ],
  };
}

test("provider circuit alert labels D17 failures as Telnyx D17", async () => {
  const { buildProviderCircuitAlertCopy } = await import("../src/lib/providerCircuitAlert");
  const copy = buildProviderCircuitAlertCopy(
    status("sip_403_account_disabled_d17", "SIP status: 403. Account is disabled. D17"),
  );

  assert.equal(copy.subject, "CRISP VOICE CALLING PAUSED - TELNYX D17");
  assert.match(copy.text, /Telnyx SIP 403 Account is disabled D17/);
});

test("provider circuit alert labels quota failures without saying D17", async () => {
  const { buildProviderCircuitAlertCopy } = await import("../src/lib/providerCircuitAlert");
  const copy = buildProviderCircuitAlertCopy(
    status("elevenlabs_quota_exceeded", "This request exceeds your quota limit."),
  );

  assert.equal(copy.subject, "CRISP VOICE CALLING PAUSED - ELEVENLABS QUOTA");
  assert.match(copy.text, /quota/);
  assert.doesNotMatch(copy.text, /Telnyx SIP 403/);
  assert.doesNotMatch(copy.text, /D17 failures occurred/);
});

test("provider circuit alert labels LLM cascade failures without saying quota or D17", async () => {
  const { buildProviderCircuitAlertCopy } = await import("../src/lib/providerCircuitAlert");
  const copy = buildProviderCircuitAlertCopy(status("elevenlabs_llm_failure", "LLM Cascade Error:"));

  assert.equal(copy.subject, "CRISP VOICE CALLING PAUSED - ELEVENLABS LLM FAILURE");
  assert.match(copy.text, /LLM cascade/);
  assert.match(copy.text, /not confirmed quota exhaustion/);
  assert.doesNotMatch(copy.text, /Telnyx SIP 403/);
  assert.doesNotMatch(copy.text, /billing or credits/);
});
