import assert from "node:assert/strict";
import test from "node:test";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.GOOGLE_APPS_SCRIPT_TOKEN = "queue-secret";

test("voice queue refill payload uses the shared Apps Script token", async () => {
  const { buildVoiceQueueRefillPayload } = await import("../src/lib/sheetUpdateClient");

  assert.deepEqual(buildVoiceQueueRefillPayload(), {
    token: "queue-secret",
    action: "process_voice_queue",
  });
});
