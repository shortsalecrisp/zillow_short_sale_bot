import assert from "node:assert/strict";
import test from "node:test";
import { createHash } from "node:crypto";
import { correctedPhoneReleaseDigest, getVoicePhoneAttemptIdentity, VOICE_CORRECTED_PHONE_RELEASE_MARKER } from "../src/lib/voicePhoneIdentity";

Object.assign(process.env, { BASE_URL: "https://example.com", TELNYX_API_KEY: "test", TELNYX_CALLER_ID: "+12175550100",
  TELNYX_CONNECTION_ID: "test", TELNYX_OUTBOUND_VOICE_PROFILE_ID: "test", TEST_DESTINATION_NUMBER: "+12175550101" });

function fixture() {
  const row: unknown[] = Array(45).fill("");
  Object.assign(row, { 0: "Synthetic", 1: "Owner", 2: "+12145007663", 4: "123 Main St", 5: "Fort Worth", 6: "TX",
    7: "x", 8: "x", 22: "2026-10-28T15:00:01.000Z", 23: "2026-10-28T22:00:01.000Z", 27: "listing",
    30: "phone_identity_review_required" });
  const resolution = { rowNumber: 7000, phone: "+12193970173", callAttemptNumber: 1,
    conversationId: "conv_old", callStartRequestId: "old", status: "definitive_failure" };
  row[41] = "CODEX_VOICE_CALL_START_RESOLUTION_V1 " + JSON.stringify(resolution);
  const initial = ["2026-10-28T14:59:00.000Z", "sent", "render-initial-7000-0123456789abcdef", "initial-7000-0123456789abcdef", "2145007663", "initial",
    '__initial_outreach__:{"crm_row":7000,"stable_id":"listing"}', "", "", "", "", "", "", "2026-10-28T15:00:00.000Z", row[22], "", "", 7000, "worker"];
  const descriptor = { request_id: initial[2], message_id: initial[3], phone: "2145007663", created_at: initial[0], send_started_at: initial[13], sent_at: initial[14] };
  const episode = { rowNumber: 7000, phone: "2145007663", formerPhone: "2193970173", stableId: "listing", textOnly: true,
    identity: [0, 1, 2, 3, 4, 5, 6, 27].map(i => row[i]), initialReceipt: descriptor,
    initialMessageSha256: createHash("sha256").update("initial").digest("hex"),
    followupMessageSha256: createHash("sha256").update("followup").digest("hex") };
  row[25] = "CODEX_CORRECTED_PHONE_CONTACT_V1 " + JSON.stringify(episode);
  const followup = [...initial];
  Object.assign(followup, { 0: "2026-10-28T21:59:00.000Z", 2: "scheduler-followup-current", 3: "followup-current", 5: "followup",
    6: "__scheduled_followup__:" + JSON.stringify({ crm_row: 7000, stable_id: "listing", video_variant: "text", initial_receipt: descriptor }),
    13: "2026-10-28T22:00:00.000Z", 14: row[23] });
  return { row, entries: [initial, followup] };
}

test("release proof requires two unique current-phone sent receipts and owning timestamps", async () => {
  const { correctedPhoneContactProof } = await import("../src/lib/voiceCorrectedPhoneRelease");
  const { row, entries } = fixture();
  assert.equal(correctedPhoneContactProof(7000, row, entries, new Date("2026-10-29T12:00Z")).phone, row[2]);
  for (const status of ["queued", "claimed", "send_started", "uncertain", "failed"]) {
    const changed = entries.map(e => [...e]); changed[1][1] = status;
    assert.throws(() => correctedPhoneContactProof(7000, row, changed, new Date("2026-10-29T12:00Z")));
  }
  assert.throws(() => correctedPhoneContactProof(7000, row, [entries[0]], new Date("2026-10-29T12:00Z")));
  assert.throws(() => correctedPhoneContactProof(7000, row, [...entries, entries[1]], new Date("2026-10-29T12:00Z")));
  for (const column of [9, 10, 13, 16, 19, 20, 21, 32, 37, 39, 43]) {
    const changed = [...row]; changed[column] = "protected";
    assert.throws(() => correctedPhoneContactProof(7000, changed, entries, new Date("2026-10-29T12:00Z")));
  }
});

test("reviewed former failures retain old-phone duplicate protection without counting at the new phone", async () => {
  const { row } = fixture();
  assert.equal(getVoicePhoneAttemptIdentity(row).reviewReason, "attempt_phone_changed");
  row[41] += "\n" + VOICE_CORRECTED_PHONE_RELEASE_MARKER + " " + JSON.stringify({ phone: row[2], formerPhones: ["+12193970173"], digest: correctedPhoneReleaseDigest(row) });
  assert.equal(getVoicePhoneAttemptIdentity(row).reviewReason, undefined);
  assert.equal(getVoicePhoneAttemptIdentity(row).currentPhoneAttempts, 0);
  assert.deepEqual(getVoicePhoneAttemptIdentity(row).historicalPhones, ["+12193970173"]);
  const { isVoiceBotPhoneBlocked } = await import("../src/lib/voiceQueue");
  assert.equal(isVoiceBotPhoneBlocked({ rowNumber: 7001, phone: "+12193970173" } as never, [{ rowNumber: 7000, values: row }], new Date()), true);
  for (const [column, value] of [[2, "+12145551111"], [4, "Changed listing"], [23, "2026-10-29T22:00Z"]] as const) {
    const changed = [...row]; changed[column] = value;
    assert.equal(getVoicePhoneAttemptIdentity(changed).reviewReason, "attempt_phone_changed");
  }
  row[41] += '\n--- CODEX_VOICE_CALL_METRICS_V1 ---\n' + JSON.stringify({ schema: "voice_call_metrics_v1", call: { requestedPhone: "+12193970173", dialedPhone: "+12193970173", callAttemptNumber: 1 }, providerIdentity: { source: "final_conversation_receipt" } });
  assert.equal(getVoicePhoneAttemptIdentity(row).reviewReason, "malformed_attempt_phone_evidence");
});
