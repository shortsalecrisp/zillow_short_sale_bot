import assert from "node:assert/strict";
import test from "node:test";
import axios from "axios";
import { looksLikeCallEndingRequest, looksLikeDoNotCall } from "../src/lib/elevenLabsDoNotCall";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.GOOGLE_APPS_SCRIPT_WEBHOOK_URL = "https://sheet.invalid/test";
process.env.GOOGLE_APPS_SCRIPT_TOKEN = "";
process.env.GOOGLE_SHEETS_SPREADSHEET_ID = "synthetic-test-sheet";
process.env.GOOGLE_SHEETS_TAB_NAME = "SyntheticTestLeads";
process.env.ELEVENLABS_TOOL_SECRET = "";
process.env.CALL_TRANSCRIPT_EMAILS_ENABLED = "false";
process.env.SMTP_HOST = "";
process.env.SMTP_USER = "";
process.env.SMTP_PASS = "";
process.env.ALERT_EMAIL_TO = "";

const receivedWrites: Record<string, unknown>[] = [];
const conversations = new Map<string, unknown>();
const receivedGets: Array<{ id: string; timeout: number | undefined }> = [];
let receiptMode: "confirmed" | "unknown" | "failed" = "confirmed";
let refillCount = 0;
axios.defaults.adapter = async (request) => {
  let data: unknown;
  if (request.method === "get") {
    const id = request.url!.split("/").pop()!;
    receivedGets.push({ id, timeout: request.timeout });
    if (id === "test-live-evidence-timeout") throw new Error("Synthetic evidence timeout");
    if (!conversations.has(id)) throw new Error("Local fixture has no conversation evidence");
    data = conversations.get(id);
  } else {
    assert.equal(request.url, "https://sheet.invalid/test", "Every outbound request must remain in the local adapter");
    const body = JSON.parse(request.data);
    if (body.action === "process_voice_queue") {
      refillCount += 1;
      data = { ok: true };
    } else {
      receivedWrites.push(body);
      if (receiptMode === "failed") throw new Error("Synthetic write failure");
      data = receiptMode === "unknown" ? { ok: true } : {
        ok: true,
        rowNumber: body.rowNumber,
        fieldsWritten: ["AH:callResult", "J:responseStatus", "K:leadStatusCode", "AD:call_eligible", "AE:call_time_bucket", "AF:call_scheduled_for",
          "AL:callbackRequested", "AM:callbackTime", "AJ:liveTransferRequested", "AK:liveTransferCompleted"],
      };
    }
  }
  return { data, status: 200, statusText: "OK", headers: {}, config: request };
};
const load = () => import("../src/lib/elevenLabsPostCall");

function conversation(messages: string[], summary = "The caller asked to stop calling and requested no more calls.") {
  return {
    status: "done",
    has_user_audio: true,
    metadata: { termination_reason: "end_call tool was called." },
    analysis: { transcript_summary: summary },
    transcript: [
      { role: "assistant", message: "This is Maya with Crisp Short Sales. Are you handling the bank side yourself?" },
      ...messages.map((message) => ({ role: "user", message })),
    ],
    conversation_initiation_client_data: { dynamic_variables: {
      rowNumber: 123, callAttemptNumber: 1, listingAddress: "123 Fictional Street",
      requestedPhone: "+12025550123", phone: "+12025550123", agentName: "Test Caller", testMode: true,
    } },
  };
}

test("scoped call ending is distinct from protected bare STOP and list removal", () => {
  for (const value of ["Please stop the call.", "End this call.", "Let's stop here.", "Could you end the call?"]) {
    assert.equal(looksLikeCallEndingRequest(value), true, value);
    assert.equal(looksLikeDoNotCall(value), false, value);
  }
  for (const value of ["STOP", "Please stop.", "Okay, stop.", "Stop. Goodbye.", "Remove me from your list.", "No more calls.", "Don't call again."]) {
    assert.equal(looksLikeDoNotCall(value), true, value);
  }
  for (const value of ["Do not stop the call.", "How do you end this call?", "The call stopped.", "Stop charging that fee."]) {
    assert.equal(looksLikeCallEndingRequest(value), false, value);
    assert.equal(looksLikeDoNotCall(value), false, value);
  }
});

test("S4 actual stop overrides fabricated DNC summaries and stale callback tools", async () => {
  const lib = await load();
  const call = conversation(["I cannot hear you.", "No, I still cannot hear what you are saying.", "Please stop the call."]);
  call.transcript.push({ role: "assistant", message: "We will not call again." });
  const withTools = { ...call, transcript: [...call.transcript, { role: "assistant", tool_calls: [
    { tool_name: "not_interested" }, { tool_name: "callback_requested" }, { tool_name: "live_transfer_requested" },
  ] }] };
  assert.equal(lib.shouldTreatAsDoNotCall(withTools), false);
  assert.equal(lib.getVoiceContactRequestResult(withTools), "call_ended_by_request");
  assert.equal(lib.shouldTreatAsCallback(withTools), false);
  assert.equal(lib.shouldTreatAsAgentHungUp(withTools), false);
  assert.equal(lib.shouldTreatAsAgentUnavailable(withTools), false);
  assert.equal(lib.shouldTreatAsAcceptedTransferCallback(withTools), false);
  const update = lib.buildVoiceContactOutcomeUpdates("call_ended_by_request", withTools);
  assert.equal(update.leadStatusCode, undefined);
  assert.equal(update.callbackRequested, undefined, "absence of a callback in this transcript cannot erase older history");
  assert.equal(update.liveTransferRequested, "");
  assert.equal(update.responseStatus, "Call ended by request");
});

test("actual future opt-out wins over deferred/current-ending markers and a benign summary", async () => {
  const lib = await load();
  const call = conversation(["I'll get back to you.", "Please stop the call. And do not call me again."],
    "CALL ENDED BY REQUEST: caller asked to end the current call only. DEFERRED CONTACT.");
  assert.equal(lib.classifyElevenLabsNotInterested(call), "do_not_call");
  assert.equal(lib.shouldTreatAsDeferredContact(call), false);
  assert.equal(lib.buildVoiceContactOutcomeUpdates("do_not_call", call).leadStatusCode, "R");
});

test("a named Yoni refusal or use of call as a label is not the caller's future opt-out", () => {
  for (const value of [
    "Email me information only. Do not call Yoni now.",
    "Do not call Yoni now and just email me.",
    "Do not call Yoni, and just email me.",
    "Do not call Yoni. Is the email for him or me?",
    "Don't call Yoni Kutler right now.",
    "Don't call that a commitment.",
  ]) {
    assert.equal(looksLikeDoNotCall(value), false, value);
  }
  for (const value of [
    "Do not call me.", "Don't call us.", "Do not call.", "Stop calling me.", "STOP",
    "Do not call Yoni now; stop calling me.",
    "Do not call Yoni now. And do not call me again.",
    "Do not call Yoni now and do not call me again.",
    "Do not call Yoni or me.",
    "Don't call that a commitment. No more calls.",
    "Remove me from your list.",
  ]) {
    assert.equal(looksLikeDoNotCall(value), true, value);
  }
});

test("coordinated caller recipients remain protected after a named Yoni refusal", async () => {
  const lib = await load();
  for (const value of [
    "Do not call Yoni, or me.",
    "Do not call Yoni or this number again.",
    "Do not call Yoni and my number.",
    "Don't call Yoni right now, or our phone number.",
    "Do not call Yoni, me, or this number.",
    "Do not call Yoni, the bank, or this number again.",
  ]) {
    const call = conversation([value], "The caller declined a live handoff.");
    assert.equal(looksLikeDoNotCall(value), true, value);
    assert.equal(lib.getVoiceContactRequestResult(call), "do_not_call", value);
    assert.equal(lib.buildVoiceContactOutcomeUpdates("do_not_call", call).leadStatusCode, "R", value);
  }
});

test("M6 full observed transcript remains information-only without inventing DNC or callback consent", async () => {
  const lib = await load();
  const call = { ...conversation([], "The user requested information to be sent via email to \"morgan@example.invalide\" and explicitly stated not to call Yoni or schedule any callbacks. The agent confirmed the email address and acknowledged the request, then ended the call."), transcript: [
    { role: "user", message: "Email me information only. Do not call Yoni now." },
    { role: "agent", message: "Could you please confirm the best email address for receiving the information?" },
    { role: "user", message: "Send it to morgan@example.invalide." },
    { role: "agent", tool_calls: [{ tool_name: "information_requested" }] },
    { role: "agent", message: "I've noted your request for information. Thanks. Can I help you with anything else?" },
    { role: "user", message: "Do not schedule a callback. Email only." },
    { role: "agent", message: "Ok, thanks. Talk to you soon." },
    { role: "agent", tool_calls: [{ tool_name: "end_call" }] },
  ] };
  assert.equal(lib.shouldTreatAsDoNotCall(call), false);
  assert.equal(lib.getVoiceContactRequestResult(call), undefined);
  assert.equal(lib.getExplicitCallbackConsent(call), undefined);
  const id = "test-m6-information-only";
  conversations.set(id, call);
  const before = receivedWrites.length;
  // Email is deliberately unconfigured. Assert only the preceding local mock
  // outcome write; no send or completed post-call processing is claimed.
  await assert.rejects(lib.processPostCallOutcomeFromConversationId(id), /Missing email alert config/);
  assert.equal(receivedWrites.length, before + 1);
  assert.equal(receivedWrites.at(-1)!.callResult, "information_requested");
  assert.equal(receivedWrites.at(-1)!.leadStatusCode, "G");
  assert.equal(receivedWrites.at(-1)!.callbackRequested, "");
  assert.equal(receivedWrites.at(-1)!.callbackTime, "");
});

test("summary-only and missing evidence are review, never evidence of consent or opt-out", async () => {
  const lib = await load();
  assert.equal(lib.classifyElevenLabsNotInterested(), "contact_request_review");
  const noAudio = { ...conversation(["Do not call again."]), has_user_audio: false };
  assert.equal(lib.shouldTreatAsDoNotCall(noAudio), false);
  assert.equal(lib.classifyElevenLabsNotInterested(noAudio), "contact_request_review");
  const fabricated = conversation(["What exactly do you do?"]);
  assert.equal(lib.shouldTreatAsDoNotCall(fabricated), false);
  assert.equal(lib.getVoiceContactRequestResult(fabricated), "contact_request_review");
  assert.equal(lib.buildVoiceContactOutcomeUpdates("contact_request_review").leadStatusCode, undefined);
});

test("live in-progress evidence can be absent or partial and cannot be treated as a completed outcome", async () => {
  const lib = await load();
  const call = { ...conversation([], ""), status: "in-progress", transcript: [] };
  assert.equal(lib.classifyElevenLabsNotInterested(call), "contact_request_review");
  const partial = { ...conversation(["What do you do?"], ""), status: "in-progress" };
  assert.equal(lib.classifyElevenLabsNotInterested(partial), "contact_request_review");
  const id = "test-partial-stop";
  conversations.set(id, { ...conversation(["Please stop the call."]), status: "in-progress" });
  const before = receivedWrites.length;
  assert.equal(await lib.processPostCallOutcomeFromConversationId(id), false);
  assert.equal(receivedWrites.length, before, "post-call processing must wait for final evidence");
});

test("caller evidence is bound to the known context or matching provider row and attempt", async () => {
  const { isElevenLabsContactEvidenceBound } = await load();
  const expected = { rowNumber: 123, callAttemptNumber: 1, matchesKnownCallContext: false };
  assert.equal(isElevenLabsContactEvidenceBound(conversation(["STOP"]), expected), true);
  assert.equal(isElevenLabsContactEvidenceBound({}, expected), false);
  assert.equal(isElevenLabsContactEvidenceBound({}, { ...expected, matchesKnownCallContext: true }), true);
  assert.equal(isElevenLabsContactEvidenceBound(conversation(["STOP"]), { ...expected, rowNumber: 124 }), false);
  assert.equal(isElevenLabsContactEvidenceBound(conversation(["STOP"]), { ...expected, callAttemptNumber: 2 }), false);
  assert.equal(isElevenLabsContactEvidenceBound(conversation(["STOP"]), { ...expected, rowNumber: 124, matchesKnownCallContext: true }), false);
});

test("recorded screening and voicemail wording do not become live caller intent", async () => {
  const lib = await load();
  for (const call of [
    conversation(["This is a prerecorded message. Please stop the call."], "A recorded message answered."),
    conversation(["This is a prerecorded message. Do not call again."], "A recorded message answered."),
    conversation(["Please leave a message after the tone. Stop."], "Voicemail answered."),
  ]) {
    assert.equal(lib.shouldTreatAsDoNotCall(call), false);
    assert.equal(lib.shouldTreatAsCallEndedByRequest(call), false);
  }
});

test("live authorized administrator can opt out after screening", async () => {
  const lib = await load();
  const call = conversation(["Please record your name and reason for calling.", "I'm the office administrator. Remove me from your list."], "An administrator responded.");
  assert.equal(lib.shouldTreatAsDoNotCall(call), true);
  assert.equal(lib.shouldTreatAsAgentUnavailable(call), false);
});

test("a live complaint about voicemails or an earlier voicemail summary cannot erase explicit opt-out", async () => {
  const lib = await load();
  for (const call of [
    conversation(["Stop calling me. You keep leaving voicemail."], "The caller complained about earlier voicemail."),
    conversation(["Do not call me again."], "Earlier voicemail was mentioned."),
    conversation(["Please leave a message after the tone.", "Hello, I picked up. Stop calling me."], "Voicemail was interrupted by the live caller."),
  ]) {
    assert.equal(lib.shouldTreatAsDoNotCall(call), true);
    assert.equal(lib.getVoiceContactRequestResult(call), "do_not_call");
  }
});

test("prior explicit callback is retained as history but does not create a new action after ending", async () => {
  const lib = await load();
  const call = conversation(["Call me back tomorrow at 2 pm.", "Please stop the call."], "Call ended.");
  const update = lib.buildVoiceContactOutcomeUpdates("call_ended_by_request", call);
  assert.equal(update.callbackRequested, "yes");
  assert.equal(update.callbackTime, "tomorrow at 2 pm");
  assert.equal(update.callResult, "call_ended_by_request");
  assert.equal(lib.shouldTreatAsCallback(call), false);
  const revoked = conversation(["Call me back tomorrow at 2 pm.", "Please stop the call. Cancel the callback."], "Call ended.");
  assert.equal(lib.buildVoiceContactOutcomeUpdates("call_ended_by_request", revoked).callbackRequested, "");
  const revokedBeforeEnding = conversation(["Call me back tomorrow at 2 pm.", "Cancel that callback.", "Please stop the call."], "Call ended.");
  assert.equal(lib.buildVoiceContactOutcomeUpdates("call_ended_by_request", revokedBeforeEnding).callbackRequested, "");
  const invented = conversation(["Please stop the call."], "The caller requested a callback tomorrow at 2 pm.");
  assert.equal(lib.buildVoiceContactOutcomeUpdates("call_ended_by_request", invented).callbackRequested, undefined);
  for (const request of ["Call me back tomorrow at 2 pm Central.", "Call me back tomorrow at two Central."]) {
    const withZone = conversation([request, "Please stop the call."], "Call ended.");
    assert.equal(lib.buildVoiceContactOutcomeUpdates("call_ended_by_request", withZone).callbackTime, request.replace("Call me back ", "").replace(/\.$/, ""));
  }
});

test("normal goodbye after callback and late questions without a stop retain their existing classifications", async () => {
  const lib = await load();
  const callback = conversation(["Call me back tomorrow at 2 pm.", "Thanks. Goodbye."], "The caller requested a callback.");
  assert.equal(lib.shouldTreatAsCallback(callback), true);
  assert.equal(lib.getVoiceContactRequestResult(callback), undefined);
  const question = conversation(["Who pays you?", "What do you charge exactly?"], "The caller asked fee questions.");
  assert.equal(lib.getVoiceContactRequestResult(question), undefined);
  assert.equal(lib.shouldTreatAsCallEndedByRequest(question), false);
});

test("final direct-callback timing corrects model-added PM only from attributable caller words", async () => {
  const lib = await load();
  const call = { ...conversation([], "The caller requested a callback tomorrow at 2:00 PM Pacific."), transcript: [
    { role: "user", message: "Have him call tomorrow at two Pacific." },
    { role: "agent", tool_calls: [{ tool_name: "callback_requested", parameters: { callbackTime: "tomorrow at 2:00 PM Pacific" } }] },
    { role: "agent", message: "I've noted your request for tomorrow at 2:00 PM Pacific." },
    { role: "user", message: "Goodbye." },
  ] };
  assert.equal(lib.getExplicitCallbackConsent(call)?.callbackTime, "tomorrow at two Pacific");
  const id = "test-direct-callback-time-correction";
  conversations.set(id, call);
  const before = receivedWrites.length;
  await assert.rejects(lib.processPostCallOutcomeFromConversationId(id), /Missing email alert config/);
  assert.equal(receivedWrites.length, before + 1);
  assert.equal(receivedWrites.at(-1)!.callResult, "callback_requested", "Final evidence repairs a missing live request write");
  assert.equal(receivedWrites.at(-1)!.callbackRequested, "yes");
  assert.equal(receivedWrites.at(-1)!.leadStatusCode, undefined, "Existing lead qualification is not downgraded");
  assert.equal(receivedWrites.at(-1)!.callbackTime, "tomorrow at two Pacific");
  assert.match(String(receivedWrites.at(-1)!.responseStatus), /tomorrow at two Pacific/);
  assert.doesNotMatch(String(receivedWrites.at(-1)!.responseStatus), /PM/);
});

test("incidental showing or closing times do not replace requested callback timing", async () => {
  const lib = await load();
  for (const [index, comment] of [
    "I have a showing at three. Goodbye.",
    "I have a closing tomorrow afternoon. Goodbye.",
    "Actually, I have a meeting at four tomorrow.",
  ].entries()) {
    const call = { ...conversation([], "The caller requested a callback."), transcript: [
      { role: "user", message: "Have him call tomorrow at two Pacific." },
      { role: "agent", tool_calls: [{ tool_name: "callback_requested" }] },
      { role: "agent", message: "Thanks. I've received your callback request." },
      { role: "user", message: comment },
    ] };
    assert.equal(lib.getExplicitCallbackConsent(call)?.callbackTime, "tomorrow at two Pacific", comment);
    const id = `test-incidental-callback-time-${index}`;
    conversations.set(id, call);
    const before = receivedWrites.length;
    await assert.rejects(lib.processPostCallOutcomeFromConversationId(id), /Missing email alert config/);
    assert.equal(receivedWrites.length, before + 1);
    assert.equal(receivedWrites.at(-1)!.callbackTime, "tomorrow at two Pacific", comment);
  }
});

test("a final callback without attributable timing does not replace the earlier sheet time", async () => {
  const lib = await load();
  const call = { ...conversation([], "The callback was requested for 2 PM."), transcript: [
    { role: "user", message: "Please call me back." },
    { role: "agent", tool_calls: [{ tool_name: "callback_requested", parameters: { callbackTime: "2 PM" } }] },
    { role: "user", message: "Goodbye." },
  ] };
  assert.equal(lib.getExplicitCallbackConsent(call)?.callbackTime, "unspecified");
  const id = "test-direct-callback-no-time-evidence";
  conversations.set(id, call);
  const before = receivedWrites.length;
  await assert.rejects(lib.processPostCallOutcomeFromConversationId(id), /Missing email alert config/);
  assert.equal(receivedWrites.length, before + 1);
  assert.equal(receivedWrites.at(-1)!.callbackTime, undefined);
  assert.equal(receivedWrites.at(-1)!.responseStatus, undefined);
});

test("a later scoped ending does not erase an already completed, explicitly accepted transfer", async () => {
  const lib = await load();
  const call = { ...conversation([], "Transfer completed before the caller ended the call."), transcript: [
    { role: "assistant", message: "Do you want me to connect you to Yoni right now?" },
    { role: "user", message: "Yes, connect me now." },
    { role: "assistant", tool_calls: [{ tool_name: "live_transfer_requested" }] },
    { role: "assistant", tool_results: [{ tool_name: "transfer_to_number", result: { status: "success" } }] },
    { role: "user", message: "Please stop the call." },
  ] };
  const update = lib.buildVoiceContactOutcomeUpdates("call_ended_by_request", call);
  assert.equal(update.liveTransferCompleted, "yes");
  assert.equal(update.liveTransferRequested, "yes");
  assert.equal(update.callbackRequested, undefined);
  assert.equal(update.callResult, "call_ended_by_request");
});

test("strict terminal write rejects HTTP-only acceptance and real transport failures", async () => {
  const { persistVoiceContactOutcome } = await load();
  const update = { rowNumber: 123, callAttemptNumber: 1, callResult: "call_ended_by_request" };
  receiptMode = "unknown";
  await assert.rejects(persistVoiceContactOutcome(update), /unconfirmed/);
  receiptMode = "failed";
  await assert.rejects(persistVoiceContactOutcome(update), /Synthetic write failure/);
  receiptMode = "confirmed";
  await persistVoiceContactOutcome(update);
});

test("post-call stop writes terminal status before generic tool fallback and does not mark a failed write processed", async () => {
  const lib = await load();
  const id = "test-final-stop";
  const call = conversation(["Please stop the call."]);
  conversations.set(id, { ...call, transcript: [...call.transcript, { role: "assistant", tool_calls: [
    { tool_name: "callback_requested" }, { tool_name: "information_requested" }, { tool_name: "not_interested" },
  ] }] });
  const before = receivedWrites.length;
  const refillBefore = refillCount;
  receiptMode = "failed";
  await assert.rejects(lib.processPostCallOutcomeFromConversationId(id), /Synthetic write failure/);
  assert.equal(refillCount, refillBefore);
  receiptMode = "confirmed";
  assert.equal(await lib.processPostCallOutcomeFromConversationId(id), true);
  assert.equal(receivedWrites.length, before + 2, "the failed attempt must remain eligible for outcome reconciliation");
  for (const write of receivedWrites.slice(before)) {
    assert.equal(write.callResult, "call_ended_by_request");
    assert.equal(write.leadStatusCode, undefined);
    assert.equal(write.callbackRequested, undefined);
    assert.equal(write.liveTransferRequested, "");
  }
});

async function invokeNotInterested(body: Record<string, unknown>) {
  const { default: router } = await import("../src/routes/elevenLabs");
  const layer = router.stack.find((item: { route?: { path?: string } }) => item.route?.path === "/tool/not-interested");
  let response: Record<string, unknown> | undefined;
  const req = { body, header: () => undefined };
  const res = { status: () => res, json: (value: Record<string, unknown>) => { response = value; return res; } };
  await layer.route.stack[0].handle(req, res, (error?: unknown) => { if (error) throw error; });
  return response!;
}

test("not-interested route uses actual DNC first and never promises unconfirmed suppression", async () => {
  const id = "test-route-dnc";
  conversations.set(id, conversation(["Remove me from your list."], "The caller will call us if interested."));
  const payload = { rowNumber: 123, callAttemptNumber: 1, agentName: "Test Caller", phone: "+12025550123",
    listingAddress: "123 Fictional Street", conversationId: id,
    conversationSummary: "DEFERRED CONTACT. CALL ENDED BY REQUEST: caller asked to end the current call only." };
  receiptMode = "confirmed";
  let response = await invokeNotInterested(payload);
  assert.equal(response.intent, "do_not_call");
  assert.equal(response.persistenceStatus, "confirmed");
  assert.equal(receivedWrites.at(-1)!.callResult, response.intent);
  assert.equal(receivedWrites.at(-1)!.leadStatusCode, "R");
  assert.match(String(response.nextAction), /Understood\. Goodbye\./);
  assert.doesNotMatch(String(response.nextAction), /won't call|will not call|removed you|you're removed/i);
  receiptMode = "unknown";
  response = await invokeNotInterested(payload);
  assert.equal(response.ok, false);
  assert.equal(response.requiresReview, true);
  assert.equal(response.persistenceStatus, "unconfirmed");
  receiptMode = "confirmed";
});

test("not-interested route ignores invented DNC tool summary and writes current-call result without lead rejection", async () => {
  const id = "test-route-stop";
  conversations.set(id, conversation(["Please stop the call."]));
  const response = await invokeNotInterested({ rowNumber: 123, callAttemptNumber: 1, conversationId: id,
    agentName: "Test Caller", phone: "+12025550123", listingAddress: "123 Fictional Street",
    conversationSummary: "The caller says do not call again." });
  assert.equal(response.intent, "call_ended_by_request");
  assert.equal(receivedWrites.at(-1)!.leadStatusCode, undefined);
  assert.match(String(receivedWrites.at(-1)!.voiceNotes), /^CALL ENDED BY REQUEST:/);
});

test("not-interested route with unavailable evidence records terminal review instead of trusting the summary", async () => {
  const response = await invokeNotInterested({ rowNumber: 123, callAttemptNumber: 1, conversationId: "missing-evidence",
    agentName: "Test Caller", phone: "+12025550123", listingAddress: "123 Fictional Street",
    conversationSummary: "Do not call again." });
  assert.equal(response.intent, "contact_request_review");
  assert.equal(response.requiresReview, true);
  assert.equal(receivedWrites.at(-1)!.leadStatusCode, undefined);
});

test("live evidence GET is bounded at 2500ms without retry; general post-call retrieval retains 15 seconds", async () => {
  const response = await invokeNotInterested({ rowNumber: 123, callAttemptNumber: 1, conversationId: "test-live-evidence-timeout",
    agentName: "Test Caller", phone: "+12025550123", listingAddress: "123 Fictional Street",
    conversationSummary: "Do not call again." });
  const liveGets = receivedGets.filter((request) => request.id === "test-live-evidence-timeout");
  assert.equal(liveGets.length, 1);
  assert.equal(liveGets[0].timeout, 2_500);
  assert.equal(response.intent, "contact_request_review");
  assert.equal(response.requiresReview, true);
  assert.equal(receivedGets.find((request) => request.id === "test-final-stop")!.timeout, 15_000);
});

test("not-interested route does not apply another lead's opt-out transcript", async () => {
  const id = "test-wrong-lead";
  conversations.set(id, conversation(["STOP"]));
  const response = await invokeNotInterested({ rowNumber: 124, callAttemptNumber: 1, conversationId: id,
    agentName: "Another Caller", phone: "+12025550124", listingAddress: "456 Fictional Street",
    conversationSummary: "Do not call again." });
  assert.equal(response.intent, "contact_request_review");
  assert.equal(receivedWrites.at(-1)!.rowNumber, 124);
  assert.equal(receivedWrites.at(-1)!.leadStatusCode, undefined);
});

function failedTransferConversation(messages: Array<{ role: string; message?: string; tool_calls?: Array<{ tool_name: string }> }> = []) {
  return { ...conversation([], "The transfer did not complete."), status: "failed", transcript: [
    { role: "assistant", message: "Would you like me to connect you with Yoni right now?" },
    { role: "user", message: "Yes, please connect me now." },
    { role: "assistant", tool_calls: [{ tool_name: "live_transfer_requested" }] },
    ...messages,
  ] };
}

test("an accepted live-now transfer or model callback summary is not future callback consent", async () => {
  const lib = await load();
  const call = failedTransferConversation([
    { role: "assistant", message: "He did not answer. I will have him call you ASAP. Is that okay?" },
    { role: "assistant", tool_calls: [{ tool_name: "callback_requested" }] },
  ]);
  call.analysis.transcript_summary = "The caller requested a callback ASAP.";
  assert.equal(lib.getExplicitCallbackConsent(call), undefined);
  assert.equal(lib.shouldTreatAsAcceptedTransferCallback(call), false);
  assert.equal(lib.getUnconsentedTransferReviewResult(call), "interested_followup_review");
  const update = lib.buildVoiceContactOutcomeUpdates("interested_followup_review", call);
  assert.equal(update.leadStatusCode, "Y");
  assert.equal(update.callbackRequested, "");
  assert.equal(update.callbackTime, "");
  assert.equal(update.liveTransferRequested, "");
});

test("explicit callback consent accepts direct requests and a short yes only to a callback permission offer", async () => {
  const lib = await load();
  for (const [request, expectedTime] of [
    ["Call me back tomorrow at 2 pm Central.", "tomorrow at 2 pm Central"],
    ["Please have Yoni call me tomorrow at two Central.", "tomorrow at two Central"],
  ]) {
    const call = failedTransferConversation([{ role: "user", message: request }]);
    assert.equal(lib.getExplicitCallbackConsent(call)?.callbackTime, expectedTime);
    assert.equal(lib.getExplicitCallbackConsent(call)?.callerText, request);
    assert.equal(lib.shouldTreatAsAcceptedTransferCallback(call), true);
  }
  const accepted = failedTransferConversation([
    { role: "assistant", message: "He is unavailable. Would you like me to ask Yoni to call you back ASAP?" },
    { role: "user", message: "Yes, that's fine." },
  ]);
  assert.equal(lib.getExplicitCallbackConsent(accepted)?.callbackTime, "asap");
  assert.equal(lib.shouldTreatAsAcceptedTransferCallback(accepted), true);
  const qualification = failedTransferConversation([
    { role: "assistant", message: "Are you handling the bank side yourself?" },
    { role: "user", message: "Yes." },
  ]);
  assert.equal(lib.getExplicitCallbackConsent(qualification), undefined);
  const choice = failedTransferConversation([
    { role: "assistant", message: "Would you like a callback or should I send information?" },
    { role: "user", message: "Yes." },
  ]);
  assert.equal(lib.getExplicitCallbackConsent(choice), undefined);
  for (const offer of [
    "Would you like a callback? Are you handling the bank side yourself?",
    "Would you like a callback and are you handling the bank side yourself?",
  ]) {
    const compound = failedTransferConversation([
      { role: "assistant", message: offer }, { role: "user", message: "Yes." },
    ]);
    assert.equal(lib.getExplicitCallbackConsent(compound), undefined, offer);
  }
});

test("later revocation and recorded speech invalidate callback consent", async () => {
  const lib = await load();
  for (const ending of ["Cancel that callback.", "Never mind.", "No thanks.", "Do not call me again."]) {
    const call = failedTransferConversation([
      { role: "user", message: "Call me back tomorrow at 2 pm." },
      { role: "user", message: ending },
    ]);
    assert.equal(lib.getExplicitCallbackConsent(call), undefined, ending);
    assert.equal(lib.shouldTreatAsAcceptedTransferCallback(call), false, ending);
  }
  const recorded = conversation(["This is a prerecorded message. Call me back tomorrow."], "A recording answered.");
  assert.equal(lib.getExplicitCallbackConsent(recorded), undefined);
});

test("a caller question or correction consumes an old callback offer before a later bare yes", async () => {
  const { getExplicitCallbackConsent } = await load();
  for (const interruption of ["Who pays you?", "I am Michael, not Taylor.", "No thanks."]) {
    const call = failedTransferConversation([
      { role: "assistant", message: "Would you like me to ask Yoni to call you back tomorrow at 2 pm?" },
      { role: "user", message: interruption },
      { role: "user", message: "Yes." },
    ]);
    assert.equal(getExplicitCallbackConsent(call), undefined, interruption);
    const freshOffer = { ...call, transcript: [...call.transcript,
      { role: "assistant", message: "Would you like me to ask Yoni to call you back tomorrow at 2 pm Central?" },
      { role: "user", message: "Yes." },
    ] };
    assert.match(getExplicitCallbackConsent(freshOffer)!.callbackTime, /tomorrow at 2 pm Central/);
  }
});

test("callback permission and later timing answers are separate; the latest actual correction wins", async () => {
  const { getExplicitCallbackConsent } = await load();
  const accepted = failedTransferConversation([
    { role: "assistant", message: "Would you like a callback?" },
    { role: "user", message: "Yes." },
  ]);
  const permission = getExplicitCallbackConsent(accepted)!;
  assert.equal(permission.callbackTime, "unspecified");
  assert.equal(permission.callerText, "Yes.");
  assert.equal(permission.offerText, "Would you like a callback?");
  assert.equal(permission.timingText, undefined);

  const timed = { ...accepted, transcript: [...accepted.transcript,
    { role: "assistant", message: "What time should Yoni call?" },
    { role: "user", message: "Tomorrow at 2 Pacific." },
  ] };
  const actualTime = getExplicitCallbackConsent(timed)!;
  assert.equal(actualTime.callbackTime, "Tomorrow at 2 Pacific");
  assert.equal(actualTime.callerText, "Yes.");
  assert.equal(actualTime.timingText, "Tomorrow at 2 Pacific.");
  assert.equal(actualTime.timingSource, "caller");

  const corrected = { ...timed, transcript: [...timed.transcript,
    { role: "assistant", message: "I have tomorrow at 2 Pacific." },
    { role: "user", message: "Actually, tomorrow at two Central, not now." },
  ] };
  const correctedTime = getExplicitCallbackConsent(corrected)!;
  assert.equal(correctedTime.callbackTime, "tomorrow at two Central, not now");
  assert.equal(correctedTime.timingText, "Actually, tomorrow at two Central, not now.");
  assert.doesNotMatch(correctedTime.callbackTime, /\b(?:am|pm)\b/i);

  const negativeCorrection = { ...timed, transcript: [...timed.transcript,
    { role: "user", message: "Not tomorrow; Thursday at 3 Pacific instead." },
  ] };
  assert.equal(getExplicitCallbackConsent(negativeCorrection)!.callbackTime, "Not tomorrow; Thursday at 3 Pacific instead");
});

test("a scoped not-now or meeting lead-in does not revoke a separate explicit later callback request", async () => {
  const { getExplicitCallbackConsent } = await load();
  for (const [request, expectedTime] of [
    ["Wait, not now. Have Yoni call me tomorrow at two PM Central.", "tomorrow at two PM Central"],
    ["I'm in a meeting, can you have Yoni call me tomorrow at two?", "tomorrow at two"],
    ["Have him call tomorrow at two Pacific.", "tomorrow at two Pacific"],
    ["Cancel the live transfer. Have Yoni call me tomorrow at two PM Central.", "tomorrow at two PM Central"],
  ]) {
    const call = failedTransferConversation([{ role: "user", message: request }]);
    const consent = getExplicitCallbackConsent(call)!;
    assert.equal(consent?.callbackTime, expectedTime, request);
    assert.equal(consent.callerText, request);
  }
  for (const request of [
    "Wait, not now. What exactly do you charge?",
    "Have him call the bank tomorrow.",
    "Have Yoni call me tomorrow. Cancel that callback.",
    "If I need help, have Yoni call me tomorrow.",
  ]) {
    assert.equal(getExplicitCallbackConsent(failedTransferConversation([{ role: "user", message: request }])), undefined, request);
  }
});

test("a partial spoken timezone correction retains the supplied day and survives the current-call ending", async () => {
  const lib = await load();
  const corrected = failedTransferConversation([
    { role: "user", message: "Have Yoni call me tomorrow at two Central." },
    { role: "assistant", message: "At two Eastern?" },
    { role: "user", message: "Not Eastern. Two Pacific, and use this number." },
  ]);
  const consent = lib.getExplicitCallbackConsent(corrected)!;
  assert.equal(consent.callbackTime, "tomorrow, Not Eastern. Two Pacific, and use this number");
  assert.equal(consent.timingText, "Not Eastern. Two Pacific, and use this number.");
  assert.doesNotMatch(consent.callbackTime, /Central|\b(?:AM|PM)\b/);
  const ended = { ...corrected, transcript: [...corrected.transcript, { role: "user", message: "Please end this call." }] };
  const updates = lib.buildVoiceContactOutcomeUpdates("call_ended_by_request", ended);
  assert.equal(updates.callbackTime, consent.callbackTime);
  assert.equal(updates.callResult, "call_ended_by_request");
  assert.equal(updates.leadStatusCode, undefined);
  assert.equal(updates.liveTransferRequested, "");
});

test("an ambiguous transfer misfire becomes a callback only after actual separate callback consent", async () => {
  const lib = await load();
  const ambiguous = { ...conversation([], "The model thought the caller was interested."), transcript: [
    { role: "assistant", message: "Are you handling the bank side yourself?" },
    { role: "user", message: "Yes." },
    { role: "assistant", tool_calls: [{ tool_name: "live_transfer_requested" }] },
  ] };
  assert.equal(lib.getUnconsentedTransferReviewResult(ambiguous), "contact_request_review");
  assert.equal(lib.shouldTreatAsMisfiredTransferInterestedCallback(ambiguous), false);
  const consented = { ...ambiguous, transcript: [...ambiguous.transcript,
    { role: "user", message: "Please call me back tomorrow at 2 pm Central." },
  ] };
  assert.equal(lib.shouldTreatAsMisfiredTransferInterestedCallback(consented), true);
  assert.equal(lib.getExplicitCallbackConsent(consented)?.callbackTime, "tomorrow at 2 pm Central");
});

test("failed-transfer final processing records review before any callback tool or generic ASAP fallback", async () => {
  const lib = await load();
  const id = "test-no-callback-consent";
  conversations.set(id, failedTransferConversation([
    { role: "assistant", message: "I will have him call you back ASAP. Is that okay?" },
    { role: "assistant", tool_calls: [{ tool_name: "callback_requested" }] },
  ]));
  const before = receivedWrites.length;
  assert.equal(await lib.processPostCallOutcomeFromConversationId(id), true);
  assert.equal(receivedWrites.length, before + 1);
  const write = receivedWrites.at(-1)!;
  assert.equal(write.callResult, "interested_followup_review");
  assert.equal(write.leadStatusCode, "Y");
  assert.equal(write.callbackRequested, "");
  assert.equal(write.callbackTime, "");
  assert.equal(write.liveTransferRequested, "");
});
