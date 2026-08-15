import assert from "node:assert/strict";
import test from "node:test";

process.env.BASE_URL = "https://voice.example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.INFO_EMAIL_API_SECRET = "info-secret";
process.env.INFO_EMAIL_SMTP_PASS = "private-email-password";

test("info email input validation accepts the approved template payload", async () => {
  const { validateAgentInfoEmailInput } = await import("../src/lib/infoEmail");

  const result = validateAgentInfoEmailInput({
    to: "LROGERSRE@GMAIL.COM ",
    subject: "Short Sale Specialist - 2580 Ranchland Way",
    body: "Hi Laurie,\n\nThanks so much!\n\nYoni Kutler",
  });

  assert.deepEqual(result, {
    to: "lrogersre@gmail.com",
    subject: "Short Sale Specialist - 2580 Ranchland Way",
    body: "Hi Laurie,\n\nThanks so much!\n\nYoni Kutler",
  });
});

test("info email input validation rejects unsafe or empty payloads", async () => {
  const { validateAgentInfoEmailInput } = await import("../src/lib/infoEmail");

  assert.throws(
    () => validateAgentInfoEmailInput({ to: "not-an-email", subject: "Short Sale Specialist", body: "Body" }),
    /Invalid info email recipient/,
  );
  assert.throws(
    () => validateAgentInfoEmailInput({ to: "agent@example.com", subject: "", body: "Body" }),
    /Invalid info email subject/,
  );
  assert.throws(
    () => validateAgentInfoEmailInput({ to: "agent@example.com", subject: "Short Sale Specialist", body: "" }),
    /Invalid info email body/,
  );
});

test("mobile approval gateway only relays to an Apps Script exec deployment", async () => {
  const { buildAppsScriptApprovalUrl } = await import("../src/routes/infoEmail");
  const id = "f645697d-ee0b-40c8-a3dd-0aee4e3892cb";
  const result = buildAppsScriptApprovalUrl(
    "https://script.google.com/macros/s/AKfycb-test_123/exec?ignored=true",
    id,
  );

  assert.equal(result.origin, "https://script.google.com");
  assert.equal(result.pathname, "/macros/s/AKfycb-test_123/exec");
  assert.equal(result.searchParams.get("action"), "approve_info_email");
  assert.equal(result.searchParams.get("id"), id);
  assert.equal(result.searchParams.has("ignored"), false);

  assert.throws(
    () => buildAppsScriptApprovalUrl("https://example.com/steal", id),
    /Invalid approval target/,
  );
  assert.throws(
    () => buildAppsScriptApprovalUrl("https://script.google.com/macros/s/test/exec", "bad-id"),
    /Invalid approval link/,
  );
});

test("mobile approval gateway accepts only a confirmed Apps Script send page", async () => {
  const { relayInfoEmailApproval } = await import("../src/routes/infoEmail");
  const target = "https://script.google.com/macros/s/AKfycb-test_123/exec";
  const id = "f645697d-ee0b-40c8-a3dd-0aee4e3892cb";
  let requestedUrl = "";

  await relayInfoEmailApproval(target, id, async (input) => {
    requestedUrl = String(input);
    return new globalThis.Response("<h2>Info email sent</h2>", { status: 200 });
  });
  assert.match(requestedUrl, /action=approve_info_email/);
  assert.match(requestedUrl, new RegExp(id));

  await assert.rejects(
    relayInfoEmailApproval(
      target,
      id,
      async () => new globalThis.Response("<h2>Info email not sent</h2>", { status: 200 }),
    ),
    /approval could not be completed/i,
  );
});
