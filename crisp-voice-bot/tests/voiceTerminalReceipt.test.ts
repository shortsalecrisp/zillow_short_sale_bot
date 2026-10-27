import assert from "node:assert/strict";
import test from "node:test";
import type { sheets_v4 } from "googleapis";

Object.assign(process.env, { BASE_URL: "https://example.com", TELNYX_API_KEY: "test", TELNYX_CALLER_ID: "+12175550100",
  TELNYX_CONNECTION_ID: "test", TELNYX_OUTBOUND_VOICE_PROFILE_ID: "test", TEST_DESTINATION_NUMBER: "+12175550101",
  ELEVENLABS_AGENT_ID: "agent_synthetic", GOOGLE_SHEETS_SPREADSHEET_ID: "synthetic-test-sheet" });

function fixture() {
  const row: unknown[] = Array(42).fill("");
  Object.assign(row, { 0: "Synthetic", 1: "Owner", 2: "202-555-0123", 4: "123 Main St", 5: "Clarksville", 6: "TN",
    10: "N", 13: "handoff", 16: true, 19: true, 29: "queued", 30: "voice_call_1_due",
    31: "2026-10-26T14:15:00Z", 32: "2026-10-26T14:15:22Z" });
  const proof = { conversationId: "conv_synthetic", phone: "+12025550123", fullName: "Synthetic Owner", listingAddress: "123 Main St, Clarksville, TN" };
  const receipt = { conversation_id: proof.conversationId, agent_id: "agent_synthetic", status: "done",
    metadata: { start_time_unix_secs: Date.parse("2026-10-26T14:15:25Z") / 1000, call_duration_secs: 42,
      phone_call: { external_number: proof.phone, direction: "outbound" } },
    conversation_initiation_client_data: { dynamic_variables: { rowNumber: 6370, callAttemptNumber: 1, phone: proof.phone, testMode: false } } };
  const payload = { callAttemptNumber: 1, callResult: "voicemail_left", leadStatusCode: "", callbackRequested: "", callbackTime: "",
    voiceTerminalReceipt: proof, voiceNotes: `Verified metrics for ${proof.conversationId}` };
  return { row, proof, receipt, payload };
}

function sheet(row: unknown[]) {
  return { spreadsheets: { values: {
    get: async () => ({ data: { values: [[...row]] } }),
    batchUpdate: async ({ requestBody }: { requestBody: { data: Array<{ range: string; values: string[][] }> } }) => {
      for (const write of requestBody.data) {
        const letters = write.range.match(/!([A-Z]+)/)![1]!;
        const col = [...letters].reduce((n, c) => n * 26 + c.charCodeAt(0) - 64, 0);
        row[col - 1] = write.values[0]![0];
      }
      return { data: {} };
    },
  } } } as unknown as sheets_v4.Sheets;
}

test("verified protected receipt fills only the missing outcome, retires its cold lane and preserves takeover/history", async () => {
  const { updateVoiceLeadRow } = await import("../src/lib/updateVoiceLeadRow");
  const { row, receipt, payload } = fixture(); const before = [...row];
  const fields = await updateVoiceLeadRow(6370, payload, async () => sheet(row), async () => receipt);
  assert.equal(row[33], "voicemail_left");
  assert.deepEqual([row[29], row[30], row[31]], ["", "", ""]);
  for (const i of [10, 13, 16, 19, 32, 35, 36, 37, 38, 39, 40]) assert.equal(row[i], before[i], String(i));
  assert.ok(String(row[41]).includes("CODEX_VOICE_TERMINAL_RECEIPT_V1"));
  assert.ok(fields.includes("AP:voiceTerminalReceipt_verified"));
});

test("receipt-only persistence preserves opt-out outcomes and agreed callbacks without rescheduling", async () => {
  const { updateVoiceLeadRow } = await import("../src/lib/updateVoiceLeadRow");
  for (const callback of ["yes", "x", "y", "requested"]) {
    const { row, receipt, payload } = fixture(); row[33] = "do_not_call"; row[10] = "R";
    row[37] = callback; row[38] = "tomorrow at 2"; row[30] = "callback_due";
    const before = [...row];
    await updateVoiceLeadRow(6370, payload, async () => sheet(row), async () => receipt);
    for (const i of [10, 13, 16, 19, 29, 30, 31, 32, 33, 37, 38, 39]) assert.equal(row[i], before[i], String(i));
  }
});

test("mismatched, synthetic, pending and changed-owner receipts fail without writing", async () => {
  const { updateVoiceLeadRow } = await import("../src/lib/updateVoiceLeadRow");
  for (const change of [
    (r: ReturnType<typeof fixture>["receipt"]) => { r.status = "in-progress"; },
    (r: ReturnType<typeof fixture>["receipt"]) => { r.metadata.call_duration_secs = 0; },
    (r: ReturnType<typeof fixture>["receipt"]) => { r.metadata.phone_call.external_number = "+12025550199"; },
    (r: ReturnType<typeof fixture>["receipt"]) => { r.conversation_initiation_client_data.dynamic_variables.testMode = true; },
    (r: ReturnType<typeof fixture>["receipt"]) => { r.conversation_initiation_client_data.dynamic_variables.rowNumber = 999; },
  ]) {
    const { row, receipt, payload } = fixture(); const before = [...row]; change(receipt);
    await assert.rejects(updateVoiceLeadRow(6370, payload, async () => sheet(row), async () => receipt), /does not match/);
    assert.deepEqual(row, before);
  }
  const { row, receipt, payload } = fixture();
  await assert.rejects(updateVoiceLeadRow(6370, payload, async () => sheet(row), async () => { row[19] = "changed"; return receipt; }), /owning state changed/);
  assert.equal(row[33], "");
});

test("even an unprotected blank-K row rejects a delayed former-phone final receipt", async () => {
  const { updateVoiceLeadRow } = await import("../src/lib/updateVoiceLeadRow");
  const { row, receipt, payload } = fixture();
  row[10] = ""; row[13] = ""; row[16] = ""; row[19] = ""; row[2] = "202-555-0199";
  const before = [...row];
  await assert.rejects(updateVoiceLeadRow(6370, payload, async () => sheet(row), async () => receipt), /identity changed/);
  assert.deepEqual(row, before);
});

test("a verified unprotected current-phone decline still writes R and preserves its sent timestamp", async () => {
  const { updateVoiceLeadRow } = await import("../src/lib/updateVoiceLeadRow");
  const { row, receipt, payload } = fixture();
  row[10] = ""; row[13] = ""; row[16] = ""; row[19] = "";
  const sent = row[32];
  await updateVoiceLeadRow(6370, { ...payload, callResult: "answered_not_interested", leadStatusCode: "R" },
    async () => sheet(row), async () => receipt);
  assert.equal(row[10], "R"); assert.equal(row[33], "answered_not_interested"); assert.equal(row[32], sent);
  assert.deepEqual([row[29], row[30], row[31]], ["", "", ""]);
});

test("zero-duration technical recovery remains distinct from a genuine call and still checks phone identity", async () => {
  const { updateVoiceLeadRow } = await import("../src/lib/updateVoiceLeadRow");
  const { row, receipt, payload } = fixture();
  row[10] = ""; row[13] = ""; row[16] = ""; row[19] = "";
  receipt.status = "failed"; receipt.metadata.call_duration_secs = 0;
  await assert.rejects(updateVoiceLeadRow(6370, { ...payload, callResult: "answered_not_interested", leadStatusCode: "R" },
    async () => sheet(row), async () => receipt), /does not match/);
  const technical = { ...payload, callResult: "call_failed_before_completion", leadStatusCode: undefined };
  await updateVoiceLeadRow(6370, technical, async () => sheet(row), async () => receipt);
  assert.equal(row[33], "call_failed_before_completion");
  assert.equal(row[10], ""); assert.equal(row[29], "");
  const changed = fixture(); changed.row[10] = ""; changed.row[13] = ""; changed.row[16] = ""; changed.row[19] = "";
  changed.row[2] = "+12025550199";
  await assert.rejects(updateVoiceLeadRow(6370, technical, async () => sheet(changed.row), async () => receipt), /identity changed/);
});

test("protected notes without receipt proof cannot be acknowledged as a final outcome", async () => {
  const { updateVoiceLeadRow } = await import("../src/lib/updateVoiceLeadRow");
  const { row } = fixture(); const before = [...row];
  await assert.rejects(updateVoiceLeadRow(6370, { callAttemptNumber: 1, callResult: "voicemail_left", voiceNotes: "notes only" }, async () => sheet(row)), /no result or verified receipt/);
  assert.deepEqual(row, before);
});

test("receipt recovery is idempotent and cannot clear another accepted attempt's lane", async () => {
  const { updateVoiceLeadRow } = await import("../src/lib/updateVoiceLeadRow");
  const { row, receipt, payload } = fixture(); row[39] = "2026-10-27T13:15:00Z"; row[30] = "voice_call_2_due";
  await updateVoiceLeadRow(6370, payload, async () => sheet(row), async () => receipt);
  const after = [...row];
  await updateVoiceLeadRow(6370, payload, async () => sheet(row), async () => receipt);
  assert.deepEqual(row, after);
  assert.equal(row[30], "voice_call_2_due");
  assert.equal(row[39], "2026-10-27T13:15:00Z");
});
