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
const done = (transcript: Array<{ role: string; message: string }>) => ({
  status: "done",
  has_user_audio: true,
  metadata: { termination_reason: "Client disconnected: 1000", call_duration_secs: 30 },
  transcript,
});

test("live target pickup after recorded screening outranks the screener", async () => {
  const lib = await load();
  const conversation = done([
    { role: "user", message: "This is Call Assist by Google. Please state your name and reason." },
    { role: "assistant", message: "Maya with Crisp Short Sales about Kevin's short-sale listing." },
    { role: "user", message: "Hello, this is Kevin." },
    { role: "assistant", message: "Hi Kevin. We help with short-sale lender paperwork." },
  ]);
  assert.equal(lib.hasLiveHumanAfterAutomatedScreening(conversation), true);
  assert.equal(lib.shouldTreatAsRecordingArtifact(conversation), false);
  assert.equal(lib.getAuthoritativeTerminalCallResult(conversation, 1, { firstName: "Kevin" }), undefined);
});

test("repeated phone menu ending in good day remains IVR automation", async () => {
  const lib = await load();
  const conversation = done([
    { role: "user", message: "Thank you for calling Dwelling Network Advisors. Press one for sales or press two for support." },
    { role: "assistant", message: "I'm trying to reach Gladimir about a short-sale listing." },
    { role: "user", message: "Thank you for calling Dwelling Network Advisors." },
    { role: "user", message: "Good day." },
  ]);
  assert.equal(lib.shouldTreatAsRecordingArtifact(conversation), true);
  assert.equal(lib.getAuthoritativeTerminalCallResult(conversation, 2, { firstName: "Gladimir" }), "agent_not_available");
});

test("different named live pickup is a gatekeeper even before the assistant speaks", async () => {
  const lib = await load();
  const conversation = done([
    { role: "user", message: "This is Donna." },
    { role: "assistant", message: "Hi Donna. I'm trying to reach Johnna about her short-sale listing." },
  ]);
  assert.equal(lib.shouldTreatAsGatekeeperAdminEarlyExit(conversation, "Johnna"), true);
  assert.equal(lib.getAuthoritativeTerminalCallResult(conversation, 1, { firstName: "Johnna" }), "gatekeeper_admin_early_exit");
});

test("already have a team is terminal existing coverage", async () => {
  const lib = await load();
  const conversation = done([
    { role: "assistant", message: "Would help with lender paperwork or calls be useful for this listing?" },
    { role: "user", message: "No, I already have a team." },
  ]);
  assert.equal(lib.shouldTreatAsAlreadyHasShortSaleHelp(conversation), true);
  assert.equal(lib.getAuthoritativeTerminalCallResult(conversation, 1, { firstName: "Devonte" }), "answered_not_interested");
});
