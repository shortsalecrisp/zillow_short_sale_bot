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
  Object.assign(receipt.metadata, { accepted_time_unix_secs: null });
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

test("exact invalid-number initiation records its failure without inventing N, a retry or a genuine call", async () => {
  const { updateVoiceLeadRow } = await import("../src/lib/updateVoiceLeadRow");
  for (const attempt of [1, 2]) {
    const { row, receipt, payload } = fixture();
    row[10] = ""; row[13] = ""; row[16] = ""; row[19] = "";
    if (attempt === 2) { row[39] = row[32]; row[30] = "voice_call_2_due"; }
    receipt.status = "failed"; receipt.metadata.call_duration_secs = 0;
    Object.assign(receipt.metadata, { accepted_time_unix_secs: null });
    receipt.conversation_initiation_client_data.dynamic_variables.callAttemptNumber = attempt;
    const before = [...row];
    await updateVoiceLeadRow(6370, { ...payload, callAttemptNumber: attempt, callResult: "call_failed_invalid_number",
      leadStatusCode: undefined, responseStatus: "Invalid destination number; contact review required, no cold retry" },
      async () => sheet(row), async () => receipt);
    assert.equal(row[attempt === 1 ? 33 : 40], "call_failed_invalid_number");
    assert.equal(row[10], "");
    assert.deepEqual(row.slice(29, 32), ["", "", ""]);
    for (const i of [2, 13, 16, 19, 32, 35, 36, 37, 38, 39]) assert.equal(row[i], before[i], String(i));
  }
});

test("unaccepted technical receipt can fill a protected receipt gap without replacing callback, opt-out or takeover", async () => {
  const { updateVoiceLeadRow } = await import("../src/lib/updateVoiceLeadRow");
  for (const result of ["call_failed_invalid_number", "provider_destination_blocked", "call_failed_before_completion"]) {
    const { row, receipt, payload } = fixture();
    row[10] = "R"; row[9] = "Do not call"; row[37] = "yes"; row[38] = "tomorrow at two"; row[30] = "callback_due";
    receipt.status = "failed"; receipt.metadata.call_duration_secs = 0;
    Object.assign(receipt.metadata, { accepted_time_unix_secs: null });
    const before = [...row];
    await updateVoiceLeadRow(6370, { ...payload, callResult: result }, async () => sheet(row), async () => receipt);
    assert.equal(row[33], result);
    for (const i of [2, 9, 10, 13, 16, 19, 29, 30, 31, 32, 35, 36, 37, 38, 39, 40]) assert.equal(row[i], before[i], String(i));
    assert.ok(String(row[41]).includes("CODEX_VOICE_TERMINAL_RECEIPT_V1"));
  }
});

test("invalid-number receipt permission cannot accept active, audible, answered or unproven zero-duration calls", async () => {
  const { updateVoiceLeadRow } = await import("../src/lib/updateVoiceLeadRow");
  for (const change of [
    { status: "initiated" }, { has_audio: true }, { has_user_audio: true }, { has_response_audio: true },
    { transcript: [{ message: "Hello" }] }, { accepted: 123 }, { accepted: undefined }, { duration: null },
  ]) {
    const { row, receipt, payload } = fixture();
    row[10] = ""; row[13] = ""; row[16] = ""; row[19] = "";
    const failed = { ...receipt, status: "failed", metadata: { ...receipt.metadata, call_duration_secs: 0 as number | null,
      accepted_time_unix_secs: null as number | null | undefined }, ...change };
    if (Object.hasOwn(change, "accepted")) failed.metadata.accepted_time_unix_secs = change.accepted;
    if (Object.hasOwn(change, "duration")) failed.metadata.call_duration_secs = change.duration ?? null;
    const before = [...row];
    await assert.rejects(updateVoiceLeadRow(6370, { ...payload, callResult: "call_failed_invalid_number", leadStatusCode: undefined },
      async () => sheet(row), async () => failed), /does not match/);
    assert.deepEqual(row, before);
  }
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

async function ivrFixture() {
  const { voiceReconciliationStateHash } = await import("../src/lib/voiceOutcomeRecovery");
  const { formatVoiceAttemptIdentity } = await import("../src/lib/voicePhoneIdentity");
  const { row, receipt, proof } = fixture();
  row[13] = ""; row[16] = ""; row[19] = ""; row[8] = "x";
  row[33] = "call_received_agent_hung_up";
  row[41] = formatVoiceAttemptIdentity(6370, proof.phone, 1, new Date(String(row[32])));
  const conversation = { ...receipt, metadata: { ...receipt.metadata, accepted_time_unix_secs: receipt.metadata.start_time_unix_secs },
    transcript: [{ role: "user", message: "Thank you for calling Synthetic Realty. For property inquiries, press one." },
      { role: "user", message: "Thank you for calling. Good" }] };
  const payload = { callAttemptNumber: 1, callResult: "agent_not_available", leadStatusCode: "", responseStatus: "Agent was not available",
    voiceTerminalReceipt: proof, voiceExpectedStateHash: voiceReconciliationStateHash(row),
    voiceOutcomeRecoveryProof: { phone: proof.phone, callAttemptNumber: 1 as const, sentAt: new Date(String(row[32])).toISOString(),
      previousResult: "call_received_agent_hung_up", fullName: proof.fullName, listingAddress: proof.listingAddress,
      expectedLeadStatusCode: "N" as const, reconciliationReason: "verified_unresolved_ivr_first_attempt" as const },
    voiceNotes: `Owner-approved exact IVR reconciliation ${proof.conversationId}` };
  return { row, conversation, payload };
}

test("owner-reviewed generic N from a verified unresolved first-call IVR can recover without resetting history", async () => {
  const { updateVoiceLeadRow } = await import("../src/lib/updateVoiceLeadRow");
  const { row, conversation, payload } = await ivrFixture(); const sent = row[32];
  await updateVoiceLeadRow(6370, payload, async () => sheet(row), async () => conversation);
  assert.equal(row[10], ""); assert.equal(row[33], "agent_not_available"); assert.equal(row[32], sent);
  assert.equal(row[29], "yes"); assert.equal(row[30], "voice_call_2_due");
  assert.equal(row[31], "2026-10-26T20:00:00.000Z");
  assert.equal(row[39], ""); assert.equal(row[40], "");
});

test("IVR reconciliation rejects real contact, voicemail, uncertain receipts and any protected or changed owner state", async () => {
  const { updateVoiceLeadRow } = await import("../src/lib/updateVoiceLeadRow");
  const { voiceReconciliationStateHash } = await import("../src/lib/voiceOutcomeRecovery");
  for (const message of ["This is Ellen, how can I help?", "Don't call again.", "Call me tomorrow.", "Can you email me instead?"]) {
    const { row, conversation, payload } = await ivrFixture(); const before = [...row];
    conversation.transcript.push({ role: "user", message });
    await assert.rejects(updateVoiceLeadRow(6370, payload, async () => sheet(row), async () => conversation), /IVR reconciliation/);
    assert.deepEqual(row, before);
  }
  for (const [index, value] of [[10, "R"], [19, true], [16, true], [37, "yes"], [38, "tomorrow"],
    [33, "do_not_call"], [39, "2026-10-27T13:15:00Z"], [2, "202-555-0199"], [28, "sent"], [28, "pushed"]] as Array<[number, unknown]>) {
    const { row, conversation, payload } = await ivrFixture(); row[index] = value;
    payload.voiceExpectedStateHash = voiceReconciliationStateHash(row); const before = [...row];
    await assert.rejects(updateVoiceLeadRow(6370, payload, async () => sheet(row), async () => conversation));
    assert.deepEqual(row, before);
  }
  const { row, conversation, payload } = await ivrFixture();
  await assert.rejects(updateVoiceLeadRow(6370, { ...payload, voiceTerminalReceipt: undefined }, async () => sheet(row)), /requires exact verified/);
  await assert.rejects(updateVoiceLeadRow(6370, { ...payload, callAttemptNumber: 2 }, async () => sheet(row), async () => conversation));
  await assert.rejects(updateVoiceLeadRow(6370, payload, async () => sheet(row), async () => ({ ...conversation, status: "initiated" })));
  await assert.rejects(updateVoiceLeadRow(6370, payload, async () => sheet(row), async () => ({ ...conversation,
    transcript: [...conversation.transcript, { role: "agent", message: "I left a voicemail." }] })), /IVR reconciliation/);
  assert.equal(row[10], "N");
});
