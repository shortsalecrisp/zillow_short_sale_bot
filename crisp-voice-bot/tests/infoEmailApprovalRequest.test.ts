import assert from "node:assert/strict";
import test from "node:test";

process.env.BASE_URL = "https://voice.example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.ELEVENLABS_TOOL_SECRET = "playback-signing-secret";
process.env.GOOGLE_APPS_SCRIPT_WEBHOOK_URL = "https://script.google.com/macros/s/test/exec";
process.env.GOOGLE_APPS_SCRIPT_TOKEN = "apps-script-secret";

test("voice information request creates one transcript-rich Apps Script approval payload", async () => {
  const { buildInfoEmailApprovalRequestPayload } = await import("../src/lib/requestInfoEmailApproval");

  const result = buildInfoEmailApprovalRequestPayload({
    rowNumber: 5860,
    phone: "+16892666941",
    email: " kemarie.johnson@c21edge.com ",
    conversationId: "conv_9301m3a9w6k1ech9epvgscvbd29x",
    conversationSummary: "Caller requested information by email.",
    conversationTranscript: "Maya: Would information help?\nAgent: Yes, email it.",
  });

  assert.equal(result.action, "request_info_email_approval");
  assert.equal(result.row, 5860);
  assert.equal(result.email, "kemarie.johnson@c21edge.com");
  assert.equal(result.conversation_id, "conv_9301m3a9w6k1ech9epvgscvbd29x");
  assert.match(String(result.playback_url), /^https:\/\/voice\.example\.com\/elevenlabs\/playback\/conv_9301m3a9w6k1ech9epvgscvbd29x\?sig=[a-f0-9]{32}$/);
  assert.match(String(result.conversation_transcript), /Agent: Yes, email it\./);
  assert.equal(result.token, "apps-script-secret");
});

test("post-call recovery prefers the caller-confirmed tool email and rejects an empty tool value", async () => {
  const { extractInformationRequestEmail } = await import("../src/lib/elevenLabsPostCall");

  assert.equal(
    extractInformationRequestEmail({
      transcript: [{
        tool_calls: [{
          tool_name: "information_requested",
          params_as_json: '{"email":"Confirmed.Agent@Example.com"}',
        }],
      }],
    }),
    "confirmed.agent@example.com",
  );
  assert.equal(
    extractInformationRequestEmail({
      transcript: [{
        tool_calls: [{ tool_name: "information_requested", params_as_json: '{"email":""}' }],
      }],
    }),
    "",
  );
});
