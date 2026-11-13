import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { OPENING_HANDOFF_POLICY, VOICE_SHORT_LISTING_RECOVERY } from "../src/lib/elevenLabsConversationPolicy";

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

const done = (transcript: Array<{ role: string; message: string }>, summary = "") => ({
  status: "done",
  has_user_audio: true,
  metadata: { termination_reason: "Client disconnected: 1000", call_duration_secs: 30 },
  analysis: { transcript_summary: summary },
  transcript,
});

test("self-identified target after screening gets only the short listing question", () => {
  const prompt = readFileSync(new URL("../docs/elevenlabs-agent-prompt.md", import.meta.url), "utf8");
  assert.match(OPENING_HANDOFF_POLICY, /explicitly identifies themself as the expected target/);
  assert.ok(OPENING_HANDOFF_POLICY.includes(VOICE_SHORT_LISTING_RECOVERY));
  assert.match(prompt, /explicitly identifies themself as the expected \{\{firstName\}\}/);
  assert.match(prompt, /Never repeat the screener response, Maya\/Crisp identity/);
});

test("named target after screening outranks gatekeeper persistence", async () => {
  const lib = await import("../src/lib/elevenLabsPostCall");
  const { buildVoicePerformanceLog } = await import("../src/lib/elevenLabsPerformanceLog");
  const conversation = done([
    { role: "user", message: "Hi. If you record your name and reason for calling, I'll see if this person is available." },
    { role: "assistant", message: "Maya with Crisp Short Sales, about the short-sale listing at Seventy Five Forty Six Northwest 44th." },
    { role: "user", message: "..." },
    { role: "user", message: "This is Lorna." },
    { role: "assistant", message: "Is Seventy Five Forty Six Northwest 44th your listing?" },
  ]);
  assert.equal(lib.shouldTreatAsGatekeeperAdminEarlyExit(conversation, "Lorna"), false);
  assert.equal(lib.getAuthoritativeTerminalCallResult(conversation, 1, { firstName: "Lorna" }), undefined);
  const metrics = buildVoicePerformanceLog({
    callbackConsent: null,
    conversationId: "conv-lorna",
    metadata: { rowNumber: 6827, fullName: "Lorna Leslie Grant", callAttemptNumber: 1,
      listingAddress: "7546 NW 44th Court #E", requestedPhone: "+16177551559",
      dialedPhone: "+16177551559", testMode: true },
    conversation,
    outcome: "Target identity confirmed after screening",
    summary: "Lorna identified herself after automated screening.",
    transcript: "",
    terminalCallResult: undefined,
  });
  assert.match(metrics, /"reviewBucket":"target_live"/);
  assert.match(metrics, /"targetAgentAnswered":true/);
});

test("recorded brokerage text greeting is voicemail, not agent hangup", async () => {
  const lib = await import("../src/lib/elevenLabsPostCall");
  const conversation = done([
    { role: "user", message: "..." },
    { role: "user", message: "You have reached William Johnson Brokerage. We can't take your call right now. Please text this number, and we will get back to you." },
  ]);
  assert.equal(lib.shouldTreatAsVoicemail(conversation), true);
  assert.equal(lib.shouldTreatAsAgentHungUp(conversation), false);
  assert.equal(lib.getAuthoritativeTerminalCallResult(conversation, 1, { firstName: "William" }), "voicemail_reached");
});

for (const [name, address] of [
  ["Adam", "Thirty Five Fifty Eight Two SPARROW NEST"],
  ["Tamara", "Eighty Five Oh Eight Orly"],
]) {
  test("Google Call Assist cannot-take-call ending remains recorded screening for " + name, async () => {
    const lib = await import("../src/lib/elevenLabsPostCall");
    const conversation = done([
      { role: "user", message: "Hi, I'm Call Assist by Google, recording this call for the person you're trying to reach. Before I try to connect you, can I ask what you're calling about?" },
      { role: "assistant", message: "Maya with Crisp Short Sales, about the short-sale listing at " + address + "." },
      { role: "user", message: "Unfortunately, the person you're calling cannot take your call right now. Have a nice day." },
    ]);
    assert.equal(lib.hasLiveHumanAfterAutomatedScreening(conversation), false);
    assert.equal(lib.shouldTreatAsRecordedScreenerTerminal(conversation), true);
    assert.equal(lib.shouldTreatAsAgentHungUp(conversation), false);
    assert.equal(lib.getAuthoritativeTerminalCallResult(conversation, 1, { firstName: name }), "agent_not_available");
  });
}
