import assert from "node:assert/strict";
import test from "node:test";
import { formatVoiceAttemptIdentity, getVoicePhoneAttemptIdentity } from "../src/lib/voicePhoneIdentity";

Object.assign(process.env, { BASE_URL: "https://example.com", TELNYX_API_KEY: "test", TELNYX_CALLER_ID: "+12175550100",
  TELNYX_CONNECTION_ID: "test", TELNYX_OUTBOUND_VOICE_PROFILE_ID: "test", TEST_DESTINATION_NUMBER: "+12175550101" });

function row() {
  const r: unknown[] = Array(42).fill("");
  Object.assign(r, { 0: "Synthetic", 1: "Owner", 2: "+12025550123", 4: "123 Main St", 5: "Miami", 6: "FL",
    8: "x", 23: "2026-10-26T12:00Z", 29: "yes", 30: "voice_call_2_due", 31: "2026-10-27T19:00Z",
    32: "2026-10-27T13:15:00Z", 33: "voicemail_left" });
  return r;
}
function metrics(phone: string, attempt = 1) {
  return '--- CODEX_VOICE_CALL_METRICS_V1 ---\n' + JSON.stringify({ schema: "voice_call_metrics_v1",
    call: { conversationId: `conv_synthetic${attempt}`, status: "done", callAttemptNumber: attempt, requestedPhone: phone, dialedPhone: phone },
    metrics: { durationSecs: 30 },
    providerIdentity: { source: "final_conversation_receipt" } });
}
test("old-number receipts never count as attempts at the corrected number or reset row history", async () => {
  const { getVoiceBotCallCandidateFromRowValues } = await import("../src/lib/voiceQueue");
  const { finalUnansweredPreservationReason } = await import("../src/lib/voiceFinalUnanswered");
  const { getVoiceOverdueFallbackPlan } = await import("../src/lib/voiceOverdueFallback");
  const r = row(); r[41] = metrics("+12025550199"); const before = [...r];
  assert.deepEqual(getVoicePhoneAttemptIdentity(r), { currentPhone: "+12025550123", currentPhoneAttempts: 0,
    unknownAttempts: 0, historicalPhones: ["+12025550199"], reviewReason: "attempt_phone_changed" });
  assert.equal(getVoiceBotCallCandidateFromRowValues(7000, r, new Date("2026-10-27T19:05Z")), undefined);
  assert.equal(finalUnansweredPreservationReason(r), "attempt_phone_requires_review");
  assert.equal(getVoiceOverdueFallbackPlan(7000, r, new Date("2026-10-30T19:05Z")), undefined);
  r[39] = "2026-10-27T19:00Z"; r[41] += '\n' + metrics("+12025550123", 2);
  assert.equal(getVoicePhoneAttemptIdentity(r).currentPhoneAttempts, 1);
  assert.equal(getVoicePhoneAttemptIdentity(r).reviewReason, "attempt_phone_changed");
  assert.equal(r[32], before[32]); assert.equal(r[33], before[33]);
});
test("start identity protects the gap before a provider final receipt, including formatted phones", () => {
  const r = row(); r[2] = "(202) 555-0123";
  r[41] = formatVoiceAttemptIdentity(7000, "+12025550123", 1, new Date(String(r[32])));
  assert.equal(getVoicePhoneAttemptIdentity(r).currentPhoneAttempts, 0, "a reservation is not proof of an accepted call");
  assert.equal(getVoicePhoneAttemptIdentity(r).reviewReason, undefined);
  r[2] = "+12025550199";
  assert.equal(getVoicePhoneAttemptIdentity(r).currentPhoneAttempts, 0);
  assert.equal(getVoicePhoneAttemptIdentity(r).reviewReason, "attempt_phone_changed");
  r[32] = ""; r[33] = "";
  assert.equal(getVoicePhoneAttemptIdentity(r).currentPhoneAttempts, 0);
  assert.equal(getVoicePhoneAttemptIdentity(r).reviewReason, "attempt_phone_changed", "transport recovery cannot release a different phone");
});

test("legacy terminal transport resolutions bind the old phone even after sent slots were safely released", async () => {
  const { getVoiceBotCallCandidateFromRowValues, isVoiceBotPhoneBlocked } = await import("../src/lib/voiceQueue");
  const { getVoiceOverdueFallbackPlan } = await import("../src/lib/voiceOverdueFallback");
  const r = row(); r[32] = ""; r[33] = ""; r[30] = "voice_call_1_due";
  const resolution = { rowNumber: 7000, phone: "+12025550199", callAttemptNumber: 1,
    callStartRequestId: "synthetic-old-start", conversationId: "conv_synthetic_failure", status: "definitive_failure" };
  r[41] = "CODEX_VOICE_CALL_START_RESOLUTION_V1 " + JSON.stringify(resolution);
  assert.deepEqual(getVoicePhoneAttemptIdentity(r), { currentPhone: "+12025550123", currentPhoneAttempts: 0,
    unknownAttempts: 0, historicalPhones: ["+12025550199"], reviewReason: "attempt_phone_changed" });
  assert.equal(getVoiceBotCallCandidateFromRowValues(7000, r, new Date("2026-10-27T19:05Z")), undefined);
  assert.equal(getVoiceOverdueFallbackPlan(7000, r, new Date("2026-10-30T19:05Z")), undefined);
  assert.equal(isVoiceBotPhoneBlocked({ rowNumber: 7001, phone: "+12025550199" } as never,
    [{ rowNumber: 7000, values: r }], new Date()), true);
  const samePhone = [...r]; samePhone[2] = "+12025550199";
  assert.equal(getVoicePhoneAttemptIdentity(samePhone).reviewReason, undefined);
  assert.equal(getVoicePhoneAttemptIdentity(samePhone).currentPhoneAttempts, 0, "a failed start is never a genuine call");
  for (const change of [{ phone: null }, { callAttemptNumber: 3 }, { status: "accepted" }, { conversationId: "" }, { callStartRequestId: "" }]) {
    const malformed = [...r]; malformed[41] = "CODEX_VOICE_CALL_START_RESOLUTION_V1 " + JSON.stringify({ ...resolution, ...change });
    assert.equal(getVoicePhoneAttemptIdentity(malformed).reviewReason, "malformed_attempt_phone_evidence");
  }
  const transcript = [...r]; transcript[41] = JSON.stringify({ transcript: r[41] });
  assert.equal(getVoicePhoneAttemptIdentity(transcript).reviewReason, undefined);
});
test("malformed/conflicting evidence holds, synthetic metrics and arbitrary transcript text do not count", () => {
  const r = row(); r[41] = 'CODEX_VOICE_ATTEMPT_IDENTITY_V1 broken';
  assert.equal(getVoicePhoneAttemptIdentity(r).reviewReason, "malformed_attempt_phone_evidence");
  r[41] = metrics("+12025550123") + '\n' + metrics("+12025550199");
  assert.equal(getVoicePhoneAttemptIdentity(r).reviewReason, "attempt_phone_changed");
  r[41] = JSON.stringify({ transcript: metrics("+12025550199") });
  assert.equal(getVoicePhoneAttemptIdentity(r).currentPhoneAttempts, 0);
  assert.equal(getVoicePhoneAttemptIdentity(r).unknownAttempts, 1);
  r[41] = '--- CODEX_VOICE_CALL_METRICS_V1 ---\n' + JSON.stringify({ schema: "voice_call_metrics_v1",
    call: { callAttemptNumber: 1, requestedPhone: "+12025550199", dialedPhone: "+12025550199", testMode: true },
    providerIdentity: { source: "final_conversation_receipt" } });
  assert.equal(getVoicePhoneAttemptIdentity(r).currentPhoneAttempts, 0);
  assert.equal(getVoicePhoneAttemptIdentity(r).reviewReason, undefined);
});

test("unknown history cannot authorize a retry and protected preferences survive phone changes", async () => {
  const { getVoiceBotCallCandidateFromRowValues } = await import("../src/lib/voiceQueue");
  const r = row();
  assert.equal(getVoiceBotCallCandidateFromRowValues(7000, r, new Date("2026-10-27T19:05Z")), undefined);
  for (const [column, value] of [[19, true], [16, true], [37, "yes"], [13, "opt-out"], [10, "Y"]] as const) {
    const protectedRow = row(); protectedRow[32] = ""; protectedRow[33] = "";
    protectedRow[41] = ""; protectedRow[column] = value;
    assert.equal(getVoiceBotCallCandidateFromRowValues(7000, protectedRow, new Date("2026-10-27T19:05Z")), undefined);
    assert.equal(protectedRow[column], value);
  }
});

test("a phone correction preserves the old phone's duplicate guard across other listing rows", async () => {
  const { isVoiceBotPhoneBlocked } = await import("../src/lib/voiceQueue");
  const corrected = row(); corrected[41] = metrics("+12025550199");
  const other = row(); other[2] = "+12025550199"; other[32] = ""; other[33] = "";
  assert.equal(isVoiceBotPhoneBlocked({ rowNumber: 7001, phone: "+12025550199" } as never,
    [{ rowNumber: 7000, values: corrected }, { rowNumber: 7001, values: other }], new Date()), true);
  corrected[41] = JSON.stringify({ transcript: "The owner mentioned +12025550199, not a placed call." });
  assert.equal(isVoiceBotPhoneBlocked({ rowNumber: 7001, phone: "+12025550199" } as never,
    [{ rowNumber: 7000, values: corrected }, { rowNumber: 7001, values: other }], new Date()), false);
});

test("identity is durably bound before dispatch without fabricating a sent time, and changed owners stop dispatch", async () => {
  const { bindVoiceBotAttemptIdentity } = await import("../src/lib/voiceQueue");
  const { buildVoiceLeadRowWrites } = await import("../src/lib/updateVoiceLeadRow");
  for (const changed of [false, true]) {
    const r = row(); r[32] = ""; r[33] = ""; r[41] = ""; const live = [...r];
    const sheets = { spreadsheets: { values: {
      batchUpdate: async ({ requestBody }: any) => {
        assert.deepEqual(requestBody.data.map((d: any) => d.range), ["'ShortSaleLeads'!AP7000"]);
        live[41] = requestBody.data[0].values[0][0]; if (changed) live[19] = true;
      },
      get: async () => ({ data: { values: [[...live]] } }),
    } } };
    const bound = await bindVoiceBotAttemptIdentity(sheets as never, r, { rowNumber: 7000,
      phone: String(r[2]), callAttemptNumber: 1 } as never, new Date("2026-10-27T13:15Z"));
    assert.equal(Boolean(bound), !changed); assert.equal(live[32], ""); assert.equal(live[39], "");
    assert.match(String(live[41]), /CODEX_VOICE_ATTEMPT_IDENTITY_V1/);
  }
  const changed = row(); changed[41] = metrics("+12025550199"); changed[39] = "2026-10-27T19:00Z";
  const writes = buildVoiceLeadRowWrites(changed, { callAttemptNumber: 2,
    callResult: "voicemail_reached_final_attempt", voiceNotes: "Final receipt requires phone reconciliation" }, new Date());
  assert.ok(writes.every(w => w.columnNumber === 42), "a former-number first call cannot close the corrected phone as two unanswered attempts");
});
