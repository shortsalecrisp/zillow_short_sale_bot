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

const load = () => import("../src/lib/elevenLabsPostCall");

test("Heesun-style explained-service refusal outranks the hangup fallback", async () => {
  const { getAuthoritativeTerminalCallResult, shouldTreatAsAgentHungUp, shouldTreatAsNotInterested } = await load();
  const conversation = {
    status: "done",
    metadata: { termination_reason: "Client disconnected: 1000", call_duration_secs: 75 },
    transcript: [
      { role: "user", message: "Hello?" },
      { role: "agent", message: "Hi, this is Maya with Crisp Short Sales. We help with short-sale lender paperwork. Is Seventeen Oh Six Northwest Taft your listing?" },
      { role: "user", message: "Yes." },
      { role: "agent", message: "Would help with lender paperwork or calls be useful for this listing?" },
      { role: "user", message: "Lender? I don't understand." },
      { role: "agent", message: "We help prepare the short-sale paperwork and follow up with the bank or mortgage company, so you don't have to handle all the calls and documents yourself. Does that sound like something you might need for this listing?" },
      { role: "user", message: "Oh, no, thank you." },
      { role: "agent", message: "Understood. Goodbye." },
    ],
  };
  assert.equal(shouldTreatAsNotInterested(conversation), true);
  assert.equal(shouldTreatAsAgentHungUp(conversation), false);
  assert.equal(getAuthoritativeTerminalCallResult(conversation, 2, { firstName: "Heesun" }), "answered_not_interested");
});

test("Cesar-style affirmative needs answer outranks later silence without inventing consent", async () => {
  const { buildVoiceContactOutcomeUpdates, getAuthoritativeTerminalCallResult,
    getVoiceContactRequestResult, shouldTreatAsPositiveNeedsAnswer } = await load();
  const conversation = {
    status: "done",
    metadata: { termination_reason: "Ending conversation after 45 seconds of silence.", call_duration_secs: 130 },
    transcript: [
      { role: "user", message: "Hello, this is Cesar." },
      { role: "agent", message: "Hi, this is Maya with Crisp Short Sales. We help with short-sale lender paperwork. Is Ten Twenty Four Harbor Town your listing?" },
      { role: "user", message: "Yeah." },
      { role: "agent", message: "Would help with lender paperwork..." },
      { role: "user", message: "Yes, thank you." },
      { role: "agent", message: "We help collect the documents and follow up with the lender, so you can stay focused on the sale. Would you like me to see if Yoni, our short-sale specialist, can join this call now?" },
      { role: "user", message: "..." },
    ],
  };
  assert.equal(shouldTreatAsPositiveNeedsAnswer(conversation), true);
  assert.equal(getAuthoritativeTerminalCallResult(conversation, 1, { firstName: "Cesar" }), undefined);
  assert.equal(getVoiceContactRequestResult(conversation), "interested_followup_review");
  const updates = buildVoiceContactOutcomeUpdates("interested_followup_review", conversation);
  assert.equal(updates.leadStatusCode, "Y");
  assert.equal(updates.callbackRequested, "");
  assert.equal(updates.liveTransferRequested, "");
  assert.equal(updates.liveTransferCompleted, "");
});
