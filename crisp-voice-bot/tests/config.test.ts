import assert from "node:assert/strict";
import test from "node:test";

process.env.BASE_URL = "https://voice.example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
delete process.env.CALL_TRANSCRIPT_EMAILS_ENABLED;

test("call transcript emails are opt-in by default", async () => {
  const { config } = await import("../src/lib/config");

  assert.equal(config.emailAlerts.sendCallTranscripts, false);
});
