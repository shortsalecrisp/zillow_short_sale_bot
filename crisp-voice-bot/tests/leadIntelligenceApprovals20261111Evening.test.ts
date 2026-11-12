import assert from "node:assert/strict";
import test from "node:test";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.ELEVENLABS_API_KEY = "test";
process.env.GOOGLE_SHEETS_SPREADSHEET_ID = "synthetic-test-sheet";
process.env.GOOGLE_SHEETS_TAB_NAME = "SyntheticTestLeads";
process.env.CALL_TRANSCRIPT_EMAILS_ENABLED = "false";
process.env.SMTP_HOST = "";
process.env.SMTP_USER = "";
process.env.SMTP_PASS = "";
process.env.ALERT_EMAIL_TO = "";

const done = (transcript: Array<{ role: string; message: string }>) => ({
  status: "done",
  has_user_audio: true,
  metadata: { termination_reason: "Client disconnected: 1000", call_duration_secs: 42 },
  transcript,
});

test("voicemail-management system failure remains IVR unavailable", async () => {
  const lib = await import("../src/lib/elevenLabsPostCall");
  const { buildVoicePerformanceLog } = await import("../src/lib/elevenLabsPerformanceLog");
  const conversation = done([
    { role: "user", message: "To review your message, press one. To erase and rerecord, press two." },
    { role: "assistant", message: "Goodbye." },
    { role: "user", message: "The system cannot process your entries at this time. Goodbye." },
  ]);
  assert.equal(lib.getAuthoritativeTerminalCallResult(conversation, 1, { firstName: "Jennifer" }), "agent_not_available");
  assert.equal(lib.shouldTreatAsAgentHungUp(conversation), false);
  const metrics = buildVoicePerformanceLog({
    callbackConsent: null,
    conversationId: "conv-jennifer",
    metadata: { rowNumber: 6808, fullName: "Jennifer Lipman", callAttemptNumber: 1,
      listingAddress: "Synthetic address", requestedPhone: "+15555550100", dialedPhone: "+15555550100", testMode: true },
    conversation,
    outcome: "Agent was not available",
    summary: "Automated voicemail menu could not process the entry.",
    transcript: "",
    terminalCallResult: "agent_not_available",
  });
  assert.match(metrics, /"reviewBucket":"ivr"/);
});

test("repeated brokerage press-one menu with cannot-connect ending remains IVR", async () => {
  const lib = await import("../src/lib/elevenLabsPostCall");
  const conversation = done([
    { role: "user", message: "Thank you for calling Coldwell Banker. To continue, say or press one." },
    { role: "assistant", message: "One." },
    { role: "user", message: "To continue, say or press one." },
    { role: "user", message: "The system is unable to connect your call at this time. Goodbye." },
  ]);
  assert.equal(lib.shouldTreatAsRecordingArtifact(conversation), true);
  assert.equal(lib.shouldTreatAsGatekeeperAdminEarlyExit(conversation, "Mizell"), false);
  assert.equal(lib.getAuthoritativeTerminalCallResult(conversation, 1, { firstName: "Mizell" }), "agent_not_available");
});

test("different named brokerage greeter is gatekeeper until target identity is confirmed", async () => {
  const lib = await import("../src/lib/elevenLabsPostCall");
  const officePickup = done([
    { role: "user", message: "Keller Williams North Atlanta, this is Ashley." },
    { role: "assistant", message: "Hi Ashley. I'm trying to reach Annette about her short-sale listing." },
  ]);
  assert.equal(lib.shouldTreatAsGatekeeperAdminEarlyExit(officePickup, "Annette"), true);
  assert.equal(lib.getAuthoritativeTerminalCallResult(officePickup, 1, { firstName: "Annette" }), "gatekeeper_admin_early_exit");

  const targetJoins = done([
    { role: "user", message: "Keller Williams North Atlanta, this is Ashley." },
    { role: "assistant", message: "Hi Ashley. I'm trying to reach Annette about her short-sale listing." },
    { role: "user", message: "One moment." },
    { role: "user", message: "This is Annette." },
    { role: "assistant", message: "Hi Annette. Is Oak Street your listing?" },
  ]);
  assert.equal(lib.shouldTreatAsGatekeeperAdminEarlyExit(targetJoins, "Annette"), false);
  assert.equal(lib.getAuthoritativeTerminalCallResult(targetJoins, 1, { firstName: "Annette" }), undefined);
});
