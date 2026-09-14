import assert from "node:assert/strict";
import test, { after } from "node:test";
import axios from "axios";
import {
  buildElevenLabsCallbackRequestResponse,
  buildElevenLabsContactOutcomeResponse,
  buildElevenLabsInformationRequestResponse,
  buildElevenLabsLiveTransferResponse,
} from "../src/lib/elevenLabsRequestResponse";
import {
  rememberElevenLabsCallContext,
  resetElevenLabsCallContextsForTest,
} from "../src/lib/elevenLabsCallContext";
import type { VoiceContactResult } from "../src/lib/elevenLabsPostCall";

const availabilityStatuses = ["pending", "accepted", "declined", "timeout", "call_failed"] as const;

test("handled availability responses distinguish status from caller consent or connection", () => {
  for (const status of availabilityStatuses) {
    const response = buildElevenLabsLiveTransferResponse(status);
    assert.equal(response.ok, true, status);
    assert.equal(response.intent, "live_transfer");
    assert.equal(response.transferApproved, status === "accepted");
    assert.equal(response.approvalStatus, status === "pending" ? "in_progress" : status);
    assert.match(response.nextAction, /Read the latest caller turn before acting on this result/);
    assert.match(response.nextAction, /unanswered question, correction, cancellation, or changed contact preference takes priority/);
    assert.match(response.nextAction, /Answer a current question first/);
    assert.match(response.nextAction, /An earlier yes does not override a later callback-only or email-only preference, refusal, or request to wait/);
    assert.doesNotMatch(response.nextAction, /Then immediately call end_call|call callback_requested with callbackTime=asap|I will text him/);
  }
});

test("pending availability does not invent another attempt or claim cancellation", () => {
  const { nextAction } = buildElevenLabsLiveTransferResponse("pending");
  assert.match(nextAction, /original availability check is still in progress/);
  assert.match(nextAction, /Do not start another availability attempt or automatically create a callback as fallback/);
  assert.match(nextAction, /If the caller asks for a status, say briefly: I'm still checking/);
  assert.match(nextAction, /Otherwise, while the caller still wants the live transfer, wait quietly for the original result while listening/);
  assert.match(nextAction, /Do not claim another place was tried, that the underlying attempt was cancelled, or that a transfer completed/);
  assert.doesNotMatch(nextAction, /let me just try him one other place|Hold on one minute/);
});

test("pending instructions forbid automatic callback fallback but honor a new explicit callback or email request", () => {
  const { nextAction } = buildElevenLabsLiveTransferResponse("pending");
  assert.match(nextAction, /Do not start another availability attempt or automatically create a callback as fallback/);
  assert.match(nextAction, /If the caller now explicitly chooses a callback or email instead, honor that new request using the corresponding request tool without waiting for the old result/);
  assert.match(nextAction, /An earlier yes does not override a later callback-only or email-only preference/);
  assert.doesNotMatch(nextAction, /Do not call callback_requested|Do not start another attempt or call callback_requested/);
});

test("accepted availability cannot revive stale consent after a question or cancellation", () => {
  const { nextAction } = buildElevenLabsLiveTransferResponse("accepted");
  assert.match(nextAction, /not proof of caller consent or a completed connection/);
  assert.match(nextAction, /only if the caller explicitly chose to speak with Yoni now and that choice remains current/);
  assert.match(nextAction, /no unanswered question or request to wait/);
  assert.match(nextAction, /stale accepted result never authorizes a transfer after cancellation or a choice of callback or email instead/);
  assert.match(nextAction, /Do not promise a connection before it completes/);
});

test("all expected unavailable outcomes share a neutral permission-first callback contract", () => {
  const expected = buildElevenLabsLiveTransferResponse("declined").nextAction;
  for (const status of ["declined", "timeout", "call_failed"] as const) {
    const { nextAction } = buildElevenLabsLiveTransferResponse(status);
    assert.equal(nextAction, expected);
    assert.match(nextAction, /could not be completed now; do not call transfer_to_number or retry the availability tool/);
    assert.match(nextAction, /already chosen a next action, honor that choice without offering a different one/);
    assert.match(nextAction, /when no question or stop request is pending, ask once: I couldn't connect with Yoni right now\. Would you like to request a callback\? Then listen/);
    assert.match(nextAction, /Only an explicit callback request or an unambiguous direct answer to that current callback offer permits callback_requested/);
    assert.match(nextAction, /intervening question or correction makes the old offer no longer current/);
    assert.match(nextAction, /do not treat a later bare yes as callback consent without clarifying its meaning/);
    assert.match(nextAction, /Use only the caller's requested timing; leave timing unspecified when they gave none/);
    assert.match(nextAction, /Do not invent ASAP, promise a text or a callback, or say an appointment is booked/);
    assert.match(nextAction, /Questions, thanks, and tool results do not authorize ending/);
    assert.doesNotMatch(nextAction, /he was not available|call you back ASAP|yes, sure, ok, sounds good/);
  }
});

test("contact outcome receipts keep persistence, review and guarded ending separate", () => {
  const results: VoiceContactResult[] = ["do_not_call", "call_ended_by_request", "contact_request_review", "interested_followup_review",
    "deferred_contact", "not_short_sale", "already_working_with_negotiator", "answered_not_interested"];
  for (const result of results) for (const persistence of ["confirmed", "unconfirmed"] as const) {
    const response = buildElevenLabsContactOutcomeResponse(result, persistence);
    assert.equal(response.ok, persistence === "confirmed");
    assert.equal(response.intent, result);
    assert.equal(response.persistenceStatus, persistence);
    assert.equal(response.requiresReview, persistence === "unconfirmed" || result.endsWith("_review"));
    assert.match(response.nextAction, /contact-outcome receipt, not permission to end the call/);
    assert.match(response.nextAction, /question or correction is current, address it without restarting the sales pitch/);
    assert.match(response.nextAction, /still current, use the guarded ending workflow; do not ask them to repeat it/);
    assert.match(response.nextAction, /Do not manufacture a goodbye, call end_call directly, retry this tool, or infer a callback or transfer/);
    assert.match(response.nextAction, /callback cancellation alone is not a request to end this call/);
    assert.match(response.nextAction, /Do not promise future contact or claim suppression unless it was confirmed/);
    assert.match(response.nextAction, /unconfirmed or review-needed disposition is not evidence that the caller declined the service/);
    assert.doesNotMatch(response.nextAction, /Say exactly: Understood\. Goodbye|Then immediately call end_call|won't call again|removed you/i);
  }
});

test("callback receipts preserve requested timing without confirming an appointment", () => {
  const action = buildElevenLabsCallbackRequestResponse("").nextAction;
  for (const time of ["tomorrow at two Pacific", "tomorrow at two PM Central", "Not Eastern. Two Pacific, and use this number", "Not tomorrow; Thursday instead", "asap", "unspecified", ""]) {
    const response = buildElevenLabsCallbackRequestResponse(time);
    assert.equal(response.callbackTime, time);
    assert.equal(response.requestCaptured, true);
    assert.equal(response.queued, true);
    assert.equal(response.persistenceStatus, "queued");
    assert.equal(response.durablePersistenceConfirmed, false);
    assert.equal(response.appointmentConfirmed, false);
    assert.match(response.nextAction, /acknowledge once: Thanks\. I've received your callback request\. Only if the caller asks/);
    assert.match(response.nextAction, /request was received but no appointment is confirmed/);
    assert.equal(response.nextAction, action, "Spoken instructions must not interpolate unverified tool timing");
    assert.match(response.nextAction, /Keep the acknowledgment time-free/);
    assert.match(response.nextAction, /Do not repeat, reinterpret, or complete callbackTime from this tool result/);
    assert.match(response.nextAction, /not independently verified caller wording/);
    assert.match(response.nextAction, /quote their own words from the conversation without adding missing details/);
    assert.match(response.nextAction, /ASAP is a timing request, not a promised response time/);
    assert.doesNotMatch(response.nextAction, /I set (?:that|up)|I'll have (?:him|Yoni) (?:reach|call)|If mentioning callbackTime/);
  }
});

test("information receipts preserve the address without claiming sending, delivery, or callback permission", () => {
  const response = buildElevenLabsInformationRequestResponse("morgan@example.invalid");
  assert.equal(response.email, "morgan@example.invalid");
  assert.equal(response.requestCaptured, true);
  assert.equal(response.queued, true);
  assert.equal(response.persistenceStatus, "queued");
  assert.equal(response.durablePersistenceConfirmed, false);
  assert.equal(response.emailSent, false);
  assert.match(response.nextAction, /requires information_requested to have returned requestCaptured: true for this request/);
  assert.match(response.nextAction, /Missing or failed tool results are not confirmation/);
  assert.match(response.nextAction, /a verbal promise is not tool execution/);
  assert.match(response.nextAction, /acknowledge once: Thanks\. I've received your request for information\. Only if the caller asks/);
  assert.match(response.nextAction, /request was received but sending or delivery is not confirmed/);
  assert.match(response.nextAction, /information request is not permission for a callback or transfer/);
  assert.doesNotMatch(response.nextAction, /I'll have Yoni send|Then immediately call end_call/);
});

test("both receipts prioritize unanswered questions and corrections, then wait for a new caller turn", () => {
  for (const response of [buildElevenLabsCallbackRequestResponse("tomorrow at two Pacific"), buildElevenLabsInformationRequestResponse("")]) {
    assert.match(response.nextAction, /latest caller turn contains an unanswered question or correction, address it first/);
    assert.match(response.nextAction, /pause for a NEW caller turn/);
    assert.match(response.nextAction, /Do not ask an anything-else question/);
    assert.match(response.nextAction, /Questions and corrections are not farewells/);
    assert.match(response.nextAction, /Honor new corrections or cancellations; they supersede the earlier request/);
    assert.match(response.nextAction, /stop or cancellation received while this tool was pending also overrides acknowledgment and pause/);
    assert.match(response.nextAction, /Only the guarded ending workflow may end the call, based on a current explicit goodbye or request to stop or end this call/);
    assert.match(response.nextAction, /Do not make the caller repeat a still-current explicit stop received while this tool was pending/);
    assert.match(response.nextAction, /Cancelling a callback is not by itself a request to end the call/);
    assert.match(response.nextAction, /Do not call end_call or manufacture a goodbye because this tool returned/);
    assert.doesNotMatch(response.nextAction, /is there anything else|Then immediately call end_call/i);
  }
});

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.LIVE_TRANSFER_NUMBER = "+12025550124";
process.env.GOOGLE_APPS_SCRIPT_WEBHOOK_URL = "https://sheet.invalid/request-response-test";
process.env.GOOGLE_APPS_SCRIPT_TOKEN = "";
process.env.GOOGLE_SHEETS_SPREADSHEET_ID = "synthetic-request-response-sheet";
process.env.GOOGLE_SHEETS_TAB_NAME = "SyntheticRequestResponseLeads";
process.env.ELEVENLABS_TOOL_SECRET = "";
process.env.CALL_TRANSCRIPT_EMAILS_ENABLED = "false";
process.env.SMTP_HOST = "";
process.env.SMTP_USER = "";
process.env.SMTP_PASS = "";
process.env.ALERT_EMAIL_TO = "";

const pendingWrites: Array<{ body: Record<string, unknown>; finish: (fail?: boolean) => void }> = [];
type ApprovalScenario = "accepted" | "declined" | "timeout" | "call_failed" | "hold";
let approvalScenario: ApprovalScenario | undefined;
let advanceApprovalTimeout: (() => void) | undefined;
const approvalCalls: Array<{ originalCallControlId: string; approvalCallControlId: string }> = [];
const unexpectedRequests: string[] = [];
axios.defaults.adapter = async (request) => {
  if (request.method === "post" && request.url === "https://sheet.invalid/request-response-test") {
    const body = JSON.parse(request.data);
    return new Promise((resolve, reject) => {
      pendingWrites.push({
        body,
        finish: (fail = false) => fail ? reject(new Error("Synthetic pending write failure")) :
          resolve({ data: { ok: true }, status: 200, statusText: "OK", headers: {}, config: request }),
      });
    });
  }
  if (request.method === "post" && request.baseURL === "https://api.telnyx.com/v2") {
    if (request.url === "/calls" && approvalScenario) {
      const body = JSON.parse(request.data);
      assert.equal(body.to, "+12025550124");
      const metadata = JSON.parse(Buffer.from(body.client_state, "base64").toString("utf8"));
      assert.equal(metadata.kind, "live_transfer_yoni");
      assert.match(metadata.originalCallControlId, /^elevenlabs-transfer-123-/);
      const call = { originalCallControlId: metadata.originalCallControlId, approvalCallControlId: `synthetic-approval-${approvalCalls.length + 1}` };
      approvalCalls.push(call);
      if (approvalScenario === "call_failed") throw new Error("Synthetic approval-call failure");
      if (approvalScenario === "timeout") {
        assert.ok(advanceApprovalTimeout, "Timeout must use an explicitly injected local clock");
        advanceApprovalTimeout();
      } else if (approvalScenario !== "hold") {
        const { resolveTransfer } = await import("../src/lib/liveTransferStore");
        assert.equal(resolveTransfer(call.originalCallControlId, approvalScenario === "accepted" ? "yes" : "no"), true);
      }
      return { data: { data: { call_control_id: call.approvalCallControlId } }, status: 200, statusText: "OK", headers: {}, config: request };
    }
    if (approvalCalls.some(call => ["speak", "hangup"].some(action => request.url === `/calls/${call.approvalCallControlId}/actions/${action}`))) {
      return { data: { data: { result: "ok" } }, status: 200, statusText: "OK", headers: {}, config: request };
    }
  }
  const target = `${request.method} ${request.baseURL ?? ""}${request.url ?? ""}`;
  unexpectedRequests.push(target);
  throw new Error("Unexpected request blocked by local test adapter: " + target);
};
after(() => assert.deepEqual(unexpectedRequests, [], "Even caught transport failures must not hide an unexpected URL"));

async function invokeRequestTool(path: string, fields: Record<string, unknown>) {
  const { default: router } = await import("../src/routes/elevenLabs");
  const layer = router.stack.find((item: { route?: { path?: string } }) => item.route?.path === path);
  assert.ok(layer?.route, "Expected actual route handler");
  let status: number | undefined;
  let response: Record<string, unknown> | undefined;
  const req = { body: {
    rowNumber: 123, callAttemptNumber: 1, agentName: "Synthetic Caller", phone: "+12025550123",
    email: "morgan@example.invalid", listingAddress: "123 Fictional Street",
    conversationId: "synthetic-request-response-conversation", conversationSummary: "Synthetic caller request.",
    ...fields,
  }, header: () => undefined };
  const res = {
    status: (value: number) => { status = value; return res; },
    json: (value: Record<string, unknown>) => { response = value; return res; },
  };
  await layer.route.stack[0].handle(req, res, (error?: unknown) => { if (error) throw error; });
  await new Promise(setImmediate);
  assert.equal(status, 200);
  return response!;
}

function rememberTransferContext() {
  resetElevenLabsCallContextsForTest();
  rememberElevenLabsCallContext({ rowNumber: 123, callAttemptNumber: 1, fullName: "Synthetic Caller",
    listingAddress: "123 Fictional Street", requestedPhone: "+12025550123", dialedPhone: "+12025550123", testMode: true },
  "synthetic-request-response-conversation");
}

async function finishTransferTest(writeIndex: number) {
  const { clearTransfer } = await import("../src/lib/liveTransferStore");
  approvalCalls.forEach(call => clearTransfer(call.originalCallControlId));
  pendingWrites.slice(writeIndex).forEach(write => write.finish());
  approvalScenario = undefined;
  advanceApprovalTimeout = undefined;
  resetElevenLabsCallContextsForTest();
  await new Promise(setImmediate);
}

for (const status of ["accepted", "declined", "timeout", "call_failed"] as const) {
  test(`fresh and cached ${status} HTTP responses use the same handled-200 builder without a second attempt`, async t => {
    const writesBefore = pendingWrites.length, callsBefore = approvalCalls.length;
    rememberTransferContext();
    approvalScenario = status;
    if (status === "timeout") {
      t.mock.timers.enable({ apis: ["setTimeout"] });
      advanceApprovalTimeout = () => t.mock.timers.tick(35_000);
    }
    try {
      const fresh = await invokeRequestTool("/tool/live-transfer-requested", {});
      assert.deepEqual(fresh, buildElevenLabsLiveTransferResponse(status));
      assert.equal(approvalCalls.length, callsBefore + 1, "The first route invocation makes only one mocked availability attempt");
      assert.equal(pendingWrites.length, writesBefore + 1);
      const cached = await invokeRequestTool("/tool/live-transfer-requested", {});
      assert.deepEqual(cached, fresh, "Cached status must not change HTTP handling or caller instructions");
      assert.equal(approvalCalls.length, callsBefore + 1, "A cached result must not dial again");
      assert.equal(pendingWrites.length, writesBefore + 1, "A cached result must not enqueue a callback or duplicate sheet update");
    } finally {
      await finishTransferTest(writesBefore);
      if (status === "timeout") t.mock.timers.reset();
    }
  });
}

test("without a new caller request, a duplicate pending HTTP request does not create a callback fallback", async () => {
  const writesBefore = pendingWrites.length, callsBefore = approvalCalls.length;
  rememberTransferContext();
  approvalScenario = "hold";
  const original = invokeRequestTool("/tool/live-transfer-requested", {});
  try {
    await new Promise(setImmediate);
    assert.equal(approvalCalls.length, callsBefore + 1);
    const pending = await invokeRequestTool("/tool/live-transfer-requested", {});
    assert.deepEqual(pending, buildElevenLabsLiveTransferResponse("pending"));
    assert.equal(approvalCalls.length, callsBefore + 1);
    assert.equal(pendingWrites.length, writesBefore + 1);
    const { resolveTransfer } = await import("../src/lib/liveTransferStore");
    assert.equal(resolveTransfer(approvalCalls.at(-1)!.originalCallControlId, "no"), true);
    assert.deepEqual(await original, buildElevenLabsLiveTransferResponse("declined"));
  } finally {
    const { resolveTransfer } = await import("../src/lib/liveTransferStore");
    const call = approvalCalls[callsBefore];
    if (call) resolveTransfer(call.originalCallControlId, "no");
    await original;
    await finishTransferTest(writesBefore);
  }
});

test("a new explicit callback request is handled while the original availability attempt remains pending", async () => {
  const writesBefore = pendingWrites.length, callsBefore = approvalCalls.length;
  rememberTransferContext();
  approvalScenario = "hold";
  let originalResolved = false;
  const original = invokeRequestTool("/tool/live-transfer-requested", {}).then(response => {
    originalResolved = true;
    return response;
  });
  try {
    await new Promise(setImmediate);
    const pending = await invokeRequestTool("/tool/live-transfer-requested", {});
    assert.deepEqual(pending, buildElevenLabsLiveTransferResponse("pending"));
    assert.equal(pendingWrites.length, writesBefore + 1, "Pending status alone must not create callback fallback");
    const callbackTime = "tomorrow at two Pacific";
    const callback = await invokeRequestTool("/tool/callback-requested", {
      callbackTime,
      conversationSummary: "Caller explicitly requested: Have Yoni call me tomorrow at two Pacific instead.",
    });
    assert.deepEqual(callback, buildElevenLabsCallbackRequestResponse(callbackTime));
    assert.equal(originalResolved, false, "The requested callback must not wait for the older availability result");
    assert.equal(approvalCalls.length, callsBefore + 1, "The changed preference must not create another availability attempt");
    assert.equal(pendingWrites.length, writesBefore + 2);
    assert.equal(pendingWrites.at(-1)!.body.callResult, "callback_requested");
    assert.equal(pendingWrites.at(-1)!.body.callbackTime, callbackTime);
    const { resolveTransfer } = await import("../src/lib/liveTransferStore");
    assert.equal(resolveTransfer(approvalCalls.at(-1)!.originalCallControlId, "yes"), true);
    const lateAccepted = await original;
    assert.deepEqual(lateAccepted, buildElevenLabsLiveTransferResponse("accepted"));
    assert.match(String(lateAccepted.nextAction), /stale accepted result never authorizes a transfer after cancellation or a choice of callback or email instead/);
  } finally {
    const { resolveTransfer } = await import("../src/lib/liveTransferStore");
    const call = approvalCalls[callsBefore];
    if (call) resolveTransfer(call.originalCallControlId, "no");
    await original;
    await finishTransferTest(writesBefore);
  }
});

for (const callbackTime of ["tomorrow at two Pacific", "asap"]) {
  test(`callback HTTP response uses the shared receipt before the unchanged ${callbackTime} write finishes`, async () => {
    const before = pendingWrites.length;
    try {
      const response = await invokeRequestTool("/tool/callback-requested", { callbackTime });
      assert.deepEqual(response, buildElevenLabsCallbackRequestResponse(callbackTime));
      assert.equal(pendingWrites.length, before + 1, "Only the existing sheet update is queued; email remains deferred to post-call");
      assert.deepEqual(pendingWrites.at(-1)!.body, {
        rowNumber: 123, callAttemptNumber: 1, callResult: "callback_requested",
        responseStatus: callbackTime === "asap" ? "Requested callback ASAP" : `Requested callback at ${callbackTime}`,
        leadStatusCode: "Y", callbackRequested: "yes", callbackTime, voiceNotes: "Synthetic caller request.",
      });
    } finally {
      pendingWrites.slice(before).forEach((write) => write.finish());
      await new Promise(setImmediate);
    }
  });
}

test("information HTTP response uses the shared unsent receipt without changing its queued update", async () => {
  const before = pendingWrites.length;
  try {
    const response = await invokeRequestTool("/tool/information-requested", {});
    assert.deepEqual(response, buildElevenLabsInformationRequestResponse("morgan@example.invalid"));
    assert.equal(pendingWrites.length, before + 1, "Only the existing sheet update is queued; email remains deferred to post-call");
    assert.deepEqual(pendingWrites.at(-1)!.body, {
      rowNumber: 123, callAttemptNumber: 1, callResult: "information_requested", responseStatus: "Information requested - handoff ready",
      leadStatusCode: "G", callbackRequested: "", callbackTime: "", liveTransferRequested: "", liveTransferCompleted: "",
      voiceNotes: "Synthetic caller request.",
    });
    pendingWrites.at(-1)!.finish(true);
    await new Promise(setImmediate);
    assert.equal(response.persistenceStatus, "queued", "The initial receipt represents queueing, not its later outcome");
    assert.equal(response.durablePersistenceConfirmed, false, "A background failure cannot become a confirmed receipt");
    assert.equal(response.emailSent, false);
  } finally {
    pendingWrites.slice(before).forEach((write) => write.finish());
    await new Promise(setImmediate);
  }
});
