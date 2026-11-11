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

const done = (transcript: Array<{ role: string; message?: string }>) => ({
  status: "done",
  has_user_audio: true,
  metadata: { termination_reason: "Client disconnected: 1000", call_duration_secs: 42 },
  transcript,
});

test("explicit no thank you is not persisted as a generic hangup", async () => {
  const lib = await import("../src/lib/elevenLabsPostCall");
  const conversation = done([
    { role: "user", message: "Hello, this is Kim." },
    { role: "assistant", message: "Hi, this is Maya with Crisp Short Sales. We help with short-sale lender paperwork. Is Seventeen Sixty Two..." },
    { role: "user", message: "No, thank you." },
  ]);
  assert.equal(lib.shouldTreatAsNotInterested(conversation), true);
  assert.equal(lib.getAuthoritativeTerminalCallResult(conversation, 2, { firstName: "Kimberly" }), "answered_not_interested");
});

test("clear wrong-listing correction becomes wrong_contact", async () => {
  const lib = await import("../src/lib/elevenLabsPostCall");
  const conversation = done([
    { role: "user", message: "Good afternoon. Century 21 Circle." },
    { role: "assistant", message: "Hi, this is Maya with Crisp Short Sales. Is Ten Sixty Five Orchard your listing?" },
    { role: "user", message: "No." },
    { role: "assistant", message: "Sorry, I reached the wrong contact. Thanks for correcting me." },
  ]);
  assert.equal(lib.shouldTreatAsWrongListingContact(conversation), true);
  assert.equal(lib.getAuthoritativeTerminalCallResult(conversation, 1, { firstName: "Denise" }), "wrong_contact");
});

test("matching named target after screening outranks gatekeeper classification", async () => {
  const lib = await import("../src/lib/elevenLabsPostCall");
  const { buildVoicePerformanceLog } = await import("../src/lib/elevenLabsPerformanceLog");
  const conversation = done([
    { role: "user", message: "If you record your name and reason for calling, I'll see if this person is available." },
    { role: "assistant", message: "Maya with Crisp Short Sales, about the short-sale listing at Seven Oh One West Franklin." },
    { role: "user", message: "Thanks, Maya. Please stay on the line." },
    { role: "user", message: "This is Lucy. How can I help you?" },
    { role: "assistant", message: "Hi, this is Maya with Crisp Short Sales. Is Seven Oh One West Franklin your listing?" },
  ]);
  assert.equal(lib.shouldTreatAsGatekeeperAdminEarlyExit(conversation, "Lucy"), false);
  assert.equal(lib.getAuthoritativeTerminalCallResult(conversation, 2, { firstName: "Lucy" }), undefined);
  const metrics = buildVoicePerformanceLog({
    callbackConsent: null,
    conversationId: "conv-lucy",
    metadata: { rowNumber: 6743, fullName: "Lucy Gonzalez", callAttemptNumber: 2,
      listingAddress: "701 W Franklin", requestedPhone: "+15555550100", dialedPhone: "+15555550100", testMode: false },
    conversation,
    outcome: "Call received but agent hung up on Maya",
    summary: "",
    transcript: "",
    terminalCallResult: undefined,
  });
  assert.match(metrics, /"reviewBucket":"target_live"/);
});
