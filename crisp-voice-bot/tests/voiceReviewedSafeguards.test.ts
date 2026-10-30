import assert from "node:assert/strict";
import test from "node:test";
import { createVoicePhoneEpisode, currentPhoneVoiceRow, getVoicePhoneEpisode, VOICE_PHONE_EPISODE_MARKER } from "../src/lib/voicePhoneEpisode";
import { formatVoiceAttemptIdentity, getVoicePhoneAttemptIdentity } from "../src/lib/voicePhoneIdentity";
import { resolveListingTimeZone } from "../src/lib/listingTimeZone";

Object.assign(process.env, { BASE_URL: "https://example.com", TELNYX_API_KEY: "test", TELNYX_CALLER_ID: "+12175550100",
  TELNYX_CONNECTION_ID: "test", TELNYX_OUTBOUND_VOICE_PROFILE_ID: "test", TEST_DESTINATION_NUMBER: "+12175550101",
  ELEVENLABS_AGENT_ID: "agent_synthetic", GOOGLE_SHEETS_SPREADSHEET_ID: "synthetic", GOOGLE_SHEETS_TAB_NAME: "Synthetic" });

function row() {
  const r: unknown[] = Array(45).fill("");
  Object.assign(r, { 0: "Synthetic", 1: "Owner", 2: "+12025550123", 3: "synthetic@example.invalid", 4: "123 Main St",
    5: "Miami", 6: "FL", 7: "x", 8: "x", 22: "2026-10-26T13:00:05Z", 23: "2026-10-27T13:00:05Z", 27: "synthetic-listing",
    29: "yes", 30: "voice_call_1_due", 31: "2026-10-30T13:15Z", 32: "2026-10-15T19:00Z", 33: "voicemail_reached" });
  return r;
}
function metrics(phone: string, attempt: number, id: string) {
  return '--- CODEX_VOICE_CALL_METRICS_V1 ---\n' + JSON.stringify({ schema: "voice_call_metrics_v1",
    call: { conversationId: id, status: "done", callAttemptNumber: attempt, requestedPhone: phone, dialedPhone: phone },
    metrics: { durationSecs: 30 }, providerIdentity: { source: "final_conversation_receipt" } });
}
function release(r: unknown[]) {
  r[41] = metrics("+12025550199", 1, "conv_former") + "\n" + formatVoiceAttemptIdentity(7000, "+12025550199", 1, new Date(String(r[32]))) +
    "\nCODEX_VOICE_SCHEDULED_NO_START_V1 " + JSON.stringify({ detectedAt: "2026-10-20T13:15Z", originalWindowEnd: "2026-10-20T13:45Z" });
  r[41] += "\n" + VOICE_PHONE_EPISODE_MARKER + " " + JSON.stringify(createVoicePhoneEpisode(r, "+12025550199", ["conv_former"], "initial-new", "followup-new", new Date("2026-10-30T12:00Z")));
}

test("reviewed phone episodes are pure current-phone projections with old duplicate locks retained", async () => {
  const { getVoiceBotCallCandidateFromRowValues, isVoiceBotPhoneBlocked } = await import("../src/lib/voiceQueue");
  const r = row(); release(r); const before = [...r];
  const projected = currentPhoneVoiceRow(r);
  assert.equal(projected[32], ""); assert.equal(projected[33], "");
  assert.doesNotMatch(String(projected[41]), /CODEX_VOICE_SCHEDULED_NO_START_V1/);
  assert.deepEqual(r, before, "projection never clears real owning history");
  assert.equal(getVoicePhoneAttemptIdentity(r).reviewReason, undefined);
  assert.equal(getVoicePhoneAttemptIdentity(r).currentPhoneAttempts, 0);
  const due = new Date("2026-10-30T13:15Z");
  assert.equal(getVoiceBotCallCandidateFromRowValues(7000, r, due)?.callAttemptNumber, 1);
  assert.equal(getVoiceBotCallCandidateFromRowValues(7000, r, new Date("2026-10-30T12:00Z")), undefined);
  assert.equal(getVoiceBotCallCandidateFromRowValues(7000, r, new Date("2026-10-30T13:46Z")), undefined);
  assert.equal(isVoiceBotPhoneBlocked({ rowNumber: 7001, phone: "+12025550199" } as never, [{ rowNumber: 7000, values: r }], due), true);
  for (const i of [2, 4, 22, 23, 27]) {
    const changed = [...r]; changed[i] = "changed";
    assert.equal(getVoicePhoneEpisode(changed), undefined, `changed ownership ${i} holds`);
  }
  const unknown = [...r]; unknown[41] += "\n" + metrics("+12025550199", 1, "conv_unreviewed");
  assert.equal(getVoicePhoneAttemptIdentity(unknown).reviewReason, "attempt_phone_changed");
  const repeated = [...r]; repeated[41] += "\n" + String(r[41]).split("\n").at(-1);
  assert.equal(getVoicePhoneEpisode(repeated), undefined);
  for (const i of [10, 13, 16, 19, 37]) {
    const held = [...r]; held[i] = i === 10 ? "N" : i === 13 ? "opt-out" : "yes";
    assert.equal(getVoiceBotCallCandidateFromRowValues(7000, held, due), undefined);
  }
});

test("current-number genuine attempts replace only the policy view, never allow a third call", async () => {
  const { getVoiceBotCallCandidateFromRowValues } = await import("../src/lib/voiceQueue");
  const r = row(); release(r);
  r[32] = "2026-10-30T13:15Z"; r[33] = "voicemail_left";
  r[41] += "\n" + metrics("+12025550123", 1, "conv_currentfirst");
  r[30] = "voice_call_2_due"; r[31] = "2026-10-30T19:00Z";
  assert.equal(getVoicePhoneAttemptIdentity(r).currentPhoneAttempts, 1);
  assert.equal(getVoiceBotCallCandidateFromRowValues(7000, r, new Date("2026-10-30T19:00Z"))?.callAttemptNumber, 2);
  r[39] = "2026-10-30T19:01Z"; r[40] = "call_end_requested";
  r[41] += "\n" + metrics("+12025550123", 2, "conv_currentsecond");
  assert.equal(getVoicePhoneAttemptIdentity(r).currentPhoneAttempts, 2);
  assert.equal(getVoiceBotCallCandidateFromRowValues(7000, r, new Date("2026-11-02T14:15Z")), undefined);
});

test("Florida City resolves only its vetted municipality/ZIP and respects the DST weekend", async () => {
  const { getNextVoiceBotFirstAttemptWindowStart } = await import("../src/lib/voiceSheet");
  assert.equal(resolveListingTimeZone({ city: "Florida City", state: "FL", streetAddress: "457 NE 12th St 33034" }).timeZone, "America/New_York");
  assert.equal(resolveListingTimeZone({ city: "Florida City", state: "FL", streetAddress: "457 NE 12th St 32501" }).reason, "listing_zip_conflict");
  assert.equal(resolveListingTimeZone({ state: "FL" }).timeZone, "");
  assert.equal(getNextVoiceBotFirstAttemptWindowStart(new Date("2026-10-30T16:00Z"), "America/New_York", "+12025550123").toISOString(), "2026-11-02T14:15:00.000Z");
});

test("only a Spanish affirmative to the help question establishes interest, never callback or transfer consent", async () => {
  const c = await import("../src/lib/elevenLabsPostCall");
  const conversation = { status: "done", has_user_audio: true, metadata: { call_duration_secs: 40 }, transcript: [
    { role: "agent", message: "Te seria util apoyo con esos tramites para tu propiedad en Main Street?" }, { role: "user", message: "Si." },
  ] };
  assert.equal(c.shouldTreatAsPositiveNeedsAnswer(conversation), true);
  assert.equal(c.getVoiceContactRequestResult(conversation), "interested_followup_review");
  assert.equal(c.getExplicitCallbackConsent(conversation), undefined);
  assert.equal(c.shouldTreatAsAcceptedTransferCallback(conversation), false);
  for (const question of ["Is this your listing?", "Would you like Yoni to join now?", "Es tu propiedad?"]) {
    const changed = structuredClone(conversation); changed.transcript[0].message = question;
    assert.equal(c.shouldTreatAsPositiveNeedsAnswer(changed), false);
  }
  for (const message of ["No quiero.", "No necesito.", "Do not call me again.", "I'll get back to you."]) {
    const changed = structuredClone(conversation); changed.transcript.push({ role: "user", message });
    assert.equal(c.shouldTreatAsPositiveNeedsAnswer(changed), false);
  }
});

test("a live goodbye is current-call cancellation, not unanswered or future opt-out; recordings remain recordings", async () => {
  const c = await import("../src/lib/elevenLabsPostCall");
  const goodbye = { status: "done", has_user_audio: true, metadata: { call_duration_secs: 5 }, transcript: [{ role: "user", message: "Bye." }] };
  assert.equal(c.getAuthoritativeTerminalCallResult(goodbye, 2), "call_end_requested");
  assert.equal(c.shouldTreatAsDoNotCall(goodbye), false);
  assert.equal(c.getExplicitCallbackConsent(goodbye), undefined);
  const recording = structuredClone(goodbye); recording.transcript[0].message = "Please leave a message after the beep. Bye.";
  assert.equal(c.shouldTreatAsCallEndedByRequest(recording), false);
  const optout = structuredClone(goodbye); optout.transcript[0].message = "Do not call me again. Bye.";
  assert.equal(c.shouldTreatAsDoNotCall(optout), true);
});

test("reviewed classification writes require independent proof/fresh state and cannot erase protected preferences", async () => {
  const { buildVoiceLeadRowWrites } = await import("../src/lib/updateVoiceLeadRow");
  const { voiceReconciliationStateHash } = await import("../src/lib/voiceOutcomeRecovery");
  for (const [reason, attempt, before, K, result, nextK] of [
    ["verified_final_decline", 1, "contact_request_review", "", "answered_not_interested", "R"],
    ["verified_positive_needs", 1, "call_received_agent_hung_up", "N", "interested_followup_review", "Y"],
    ["verified_caller_goodbye", 2, "no_response_second_attempt", "N", "call_end_requested", ""],
  ] as const) {
    const r = row(); r[41] = metrics(String(r[2]), 1, "conv_currentfirst"); r[10] = K;
    r[attempt === 1 ? 33 : 40] = before;
    if (attempt === 2) { r[39] = "2026-10-30T19:00Z"; r[41] += "\n" + metrics(String(r[2]), 2, "conv_currentsecond"); }
    const updates = { callAttemptNumber: attempt, callResult: result, leadStatusCode: nextK, responseStatus: "reviewed",
      voiceReviewedDisposition: reason, voiceExpectedStateHash: voiceReconciliationStateHash(r),
      voiceTerminalReceipt: { conversationId: "conv_current", phone: String(r[2]), fullName: "Synthetic Owner", listingAddress: "123 Main St, Miami, FL" }, voiceNotes: "Verified conv_current" };
    assert.throws(() => buildVoiceLeadRowWrites(r, updates, new Date()), /independently verified/);
    const writes = buildVoiceLeadRowWrites(r, updates, new Date(), true, reason);
    assert.equal(writes.find(w => w.columnNumber === 11)?.value, nextK);
    assert.ok(writes.every(w => [10, 11, 30, 31, 32, attempt === 1 ? 34 : 41, 42].includes(w.columnNumber)));
    for (const i of [13, 16, 19, 28, 35, 36, 37, 38, 43, 44]) {
      const held = [...r]; held[i] = "yes";
      assert.throws(() => buildVoiceLeadRowWrites(held, { ...updates, voiceExpectedStateHash: voiceReconciliationStateHash(held) }, new Date(), true, reason), /protected/);
    }
    assert.throws(() => buildVoiceLeadRowWrites(r, { ...updates, voiceExpectedStateHash: "stale" }, new Date(), true, reason), /state changed/);
  }
});

test("identity release requires exact current-number sent receipts and owning times, not old sent flags", async () => {
  const { reviewedIdentitySmsProof, assertReviewedIdentityState } = await import("../src/lib/voiceReviewedIdentity");
  const r = row(); r[32] = ""; r[33] = "";
  const entries = [false, true].map(followup => {
    const e: unknown[] = Array(19).fill(""); const day = followup ? "27" : "26";
    Object.assign(e, { 0: `2026-10-${day}T13:00:00Z`, 1: "sent", 2: followup ? "followup-new" : "initial-new", 3: followup ? "fm" : "im",
      4: r[2], 13: `2026-10-${day}T13:00:01Z`, 14: `2026-10-${day}T13:00:10Z`, 17: 7000 });
    e[6] = (followup ? "__scheduled_followup__:" : "__initial_outreach__:") + JSON.stringify({ crm_row: 7000, stable_id: r[27],
      ...(followup ? { initial_receipt: { request_id: "initial-new", message_id: "im", phone: r[2], stable_id: r[27], sent_at: "2026-10-26T13:00:10Z" } } : {}) });
    return e;
  });
  assert.equal(reviewedIdentitySmsProof(7000, r, entries, new Date("2026-10-30T16:00Z")).phone, r[2]);
  for (const mutate of [(e: unknown[][]) => { e[1][1] = "queued"; }, (e: unknown[][]) => { e[0][4] = "+12025550199"; },
    (e: unknown[][]) => { e.push([...e[1]]); }, (e: unknown[][]) => { e[0][14] = "2026-10-26T12:00Z"; }]) {
    const changed = structuredClone(entries); mutate(changed);
    assert.throws(() => reviewedIdentitySmsProof(7000, r, changed, new Date("2026-10-30T16:00Z")));
  }
  for (const i of [10, 13, 16, 19, 28, 35, 36, 37, 38, 39, 43, 44]) { const held = [...r]; held[i] = "yes"; assert.throws(() => assertReviewedIdentityState(held)); }
});

test("direct reviewed writes independently fetch final proof, persist owning columns, and verify preserved history", async () => {
  const { updateVoiceLeadRow } = await import("../src/lib/updateVoiceLeadRow");
  const { voiceReconciliationStateHash } = await import("../src/lib/voiceOutcomeRecovery");
  const { buildVoiceBotListingAddress } = await import("../src/lib/voiceSheet");
  const { config } = await import("../src/lib/config");
  for (const [reason, attempt, oldResult, K, nextResult, nextK, transcript] of [
    ["verified_final_decline", 1, "contact_request_review", "", "answered_not_interested", "R", [
      { role: "agent", message: "Would help with lender paperwork or calls be useful for this listing?" }, { role: "user", message: "No." }]],
    ["verified_positive_needs", 1, "call_received_agent_hung_up", "N", "interested_followup_review", "Y", [
      { role: "agent", message: "Te seria util apoyo con esos tramites para tu propiedad en Main Street?" }, { role: "user", message: "Si." }]],
    ["verified_caller_goodbye", 2, "no_response_second_attempt", "N", "call_end_requested", "", [{ role: "user", message: "Bye." }]],
  ] as const) {
    const r = row(); r[10] = K; r[attempt === 1 ? 33 : 40] = oldResult;
    r[41] = metrics(String(r[2]), 1, "conv_currentfirst");
    if (attempt === 2) { r[39] = "2026-10-30T13:15Z"; r[41] += "\n" + metrics(String(r[2]), 2, "conv_currentsecond"); }
    const original = [...r], fetched: string[] = [];
    const proof = { conversationId: "conv_current", phone: String(r[2]), fullName: "Synthetic Owner", listingAddress: buildVoiceBotListingAddress(r) };
    const receipt = (id: string, n: number, turns: unknown[]) => ({ conversation_id: id, agent_id: config.elevenLabs.agentId, status: "done", has_user_audio: true,
      metadata: { start_time_unix_secs: Date.parse(String(r[n === 1 ? 32 : 39])) / 1000 + 5, accepted_time_unix_secs: Date.parse(String(r[n === 1 ? 32 : 39])) / 1000 + 6,
        call_duration_secs: 30, phone_call: { external_number: r[2], direction: "outbound" } },
      conversation_initiation_client_data: { dynamic_variables: { rowNumber: 7000, callAttemptNumber: n, phone: r[2], requestedPhone: r[2] } }, transcript: turns });
    const sheets = { spreadsheets: { values: {
      get: async ({ range }: any) => ({ data: { values: String(range).includes("sms_inbound_queue") ? [] : [[...r]] } }),
      batchUpdate: async ({ requestBody }: any) => { for (const d of requestBody.data) {
        const letters = d.range.match(/!([A-Z]+)7000$/)[1]; let index = 0; for (const char of letters) index = index * 26 + char.charCodeAt(0) - 64;
        r[index - 1] = d.values[0][0];
      } },
    } } };
    const fetchReceipt = async (id: string) => { fetched.push(id); return receipt(id, id === "conv_first" ? 1 : attempt, id === "conv_first" ? [{ role: "user", message: "Please leave a message after the beep." }] : [...transcript]); };
    const updates = { callAttemptNumber: attempt, callResult: nextResult, leadStatusCode: nextK, voiceReviewedDisposition: reason,
      voiceExpectedStateHash: voiceReconciliationStateHash(r), voiceReviewedFirstReceipt: attempt === 2 ? "conv_first" : undefined,
      voiceTerminalReceipt: proof, voiceNotes: "Reviewed exact conv_current" };
    await updateVoiceLeadRow(7000, updates, async () => sheets as never, fetchReceipt);
    assert.equal(r[10], nextK); assert.equal(r[attempt === 1 ? 33 : 40], nextResult);
    assert.equal(r[32], original[32]); assert.equal(r[39], original[39]);
    assert.equal(r[29], ""); assert.equal(r[30], ""); assert.equal(r[31], "");
    assert.match(String(r[41]), /CODEX_VOICE_CLASSIFICATION_RECONCILED_V1/);
    assert.deepEqual(fetched, attempt === 2 ? ["conv_current", "conv_first"] : ["conv_current"]);
    for (const i of [0, 1, 2, 4, 5, 6, 13, 16, 19, 35, 36, 37, 38]) assert.equal(r[i], original[i]);
    r.splice(0, r.length, ...original); r[19] = true;
    await assert.rejects(updateVoiceLeadRow(7000, { ...updates, voiceExpectedStateHash: voiceReconciliationStateHash(r) }, async () => sheets as never, fetchReceipt), /protected/);
    assert.equal(r[10], K); assert.equal(r[19], true);
  }
});
