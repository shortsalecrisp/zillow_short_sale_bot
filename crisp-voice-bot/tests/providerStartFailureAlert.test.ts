import assert from "node:assert/strict";
import test from "node:test";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";

test("provider start alert deduplicates receipts, requires a cluster and limits reminders", async () => {
  const alert = await import("../src/lib/providerStartFailureAlert");
  alert.resetProviderStartFailureAlertForTests();
  const now = new Date("2026-10-09T16:00:00Z");
  let sends = 0;
  const send = async (copy: { text: string }) => { sends++; assert.match(copy.text, /excluded from the sales funnel/); };
  const record = (id: string) => alert.recordProviderStartFailure({ conversationId: id, rowNumber: 5998,
    callAttemptNumber: 1, occurredAt: now.toISOString(), reason: "request timed out" }, now);
  record("a"); record("a"); record("b");
  await alert.ensureProviderStartFailureAlert(now, send);
  assert.equal(sends, 0);
  record("c");
  await alert.ensureProviderStartFailureAlert(now, send);
  await alert.ensureProviderStartFailureAlert(new Date(now.getTime() + 60_000), send);
  assert.equal(sends, 1);
  assert.equal(alert.getProviderStartFailureStatus(now).recentFailures, 3);
  assert.equal(alert.getProviderStartFailureStatus(new Date(now.getTime() + 31 * 60_000)).recentFailures, 0);
});

test("an email failure is retried after ten minutes without changing call control", async () => {
  const alert = await import("../src/lib/providerStartFailureAlert");
  alert.resetProviderStartFailureAlertForTests();
  const now = new Date("2026-10-09T16:00:00Z");
  for (let i = 0; i < 3; i++) alert.recordProviderStartFailure({ conversationId: String(i), rowNumber: 1,
    callAttemptNumber: 1, occurredAt: now.toISOString(), reason: "no response from servers" }, now);
  let attempts = 0;
  const send = async () => { attempts++; if (attempts === 1) throw new Error("synthetic SMTP failure"); };
  await alert.ensureProviderStartFailureAlert(now, send);
  await alert.ensureProviderStartFailureAlert(new Date(now.getTime() + 9 * 60_000), send);
  await alert.ensureProviderStartFailureAlert(new Date(now.getTime() + 10 * 60_000), send);
  assert.equal(attempts, 2);
  assert.equal(alert.getProviderStartFailureStatus(now).pausesCalls, false);
});

test("historical final receipts do not create a fresh failure cluster", async () => {
  const alert = await import("../src/lib/providerStartFailureAlert");
  alert.resetProviderStartFailureAlertForTests();
  const now = new Date("2026-10-09T16:00:00Z");
  for (let i = 0; i < 3; i++) alert.recordProviderStartFailure({ conversationId: String(i), rowNumber: 1,
    callAttemptNumber: 1, occurredAt: "2026-10-08T14:36:00Z", reason: "request timed out" }, now);
  let sends = 0;
  await alert.ensureProviderStartFailureAlert(now, async () => { sends++; });
  assert.equal(sends, 0);
});
