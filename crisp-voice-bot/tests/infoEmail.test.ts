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
