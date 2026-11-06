import assert from "node:assert/strict";
import test from "node:test";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.ELEVENLABS_API_KEY = "test";
process.env.CALL_TRANSCRIPT_EMAILS_ENABLED = "false";

test("listing confirmation plus explicit decline is not interested without owner review", async () => {
  const lib = await import("../src/lib/elevenLabsPostCall");
  const conversation = {
    status: "done",
    metadata: { termination_reason: "Client disconnected: 1000", call_duration_secs: 17 },
    transcript: [
      { role: "user", message: "Hello, this is Tammy." },
      { role: "agent", message: "Hi, this is Maya with Crisp Short Sales. We help with short-sale lender paperwork. Is Two Oh Nine 8th your listing?" },
      { role: "user", message: "It is, but I'm good. Thank you." },
      { role: "agent", message: "Understood. Thanks for letting me know." },
    ],
  };
  assert.equal(lib.shouldTreatAsNotInterested(conversation), true);
  assert.equal(lib.getAuthoritativeTerminalCallResult(conversation, 1, { firstName: "Tammy" }), "answered_not_interested");
  assert.equal(lib.shouldTreatAsAgentHungUp(conversation), false);
});

test("terminal press-zero operator menu is IVR unavailable, not human hangup", async () => {
  const lib = await import("../src/lib/elevenLabsPostCall");
  const menu = "Thank you for calling Vantage Point Brokers. Press one if you have questions about a property. Press two if you are interested in listing a property. Press three if you are interested in a career. If you know the extension number of the person you are trying to reach, please enter it now. Otherwise, please press zero to connect to the operator.";
  const repeat = "If you know the extension number of the person you are trying to reach, please enter it now. Otherwise, please press zero to connect to the operator.";
  const conversation = {
    status: "done",
    metadata: { termination_reason: "Client disconnected: 1000", call_duration_secs: 55 },
    transcript: [
      { role: "user", message: menu },
      { role: "agent", tool_calls: [{ tool_name: "skip_turn" }] },
      { role: "user", message: repeat },
      { role: "agent", tool_calls: [{ tool_name: "skip_turn" }] },
      { role: "user", message: repeat },
      { role: "user", message: "Goodbye." },
    ],
  };
  assert.equal(lib.shouldTreatAsRecordingArtifact(conversation), true);
  assert.equal(lib.getAuthoritativeTerminalCallResult(conversation, 1, { firstName: "Adam" }), "agent_not_available");
  assert.equal(lib.shouldTreatAsAgentHungUp(conversation), false);
});
