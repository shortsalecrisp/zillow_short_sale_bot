import assert from "node:assert/strict";
import test from "node:test";
import axios from "axios";
import {
  buildElevenLabsCallbackRequestResponse,
  buildElevenLabsInformationRequestResponse,
} from "../src/lib/elevenLabsRequestResponse";

test("callback receipts preserve requested timing without confirming an appointment", () => {
  for (const time of ["tomorrow at two Pacific", "tomorrow at two PM Central", "asap", "unspecified", ""]) {
    const response = buildElevenLabsCallbackRequestResponse(time);
    assert.equal(response.callbackTime, time);
    assert.equal(response.requestCaptured, true);
    assert.equal(response.queued, true);
    assert.equal(response.persistenceStatus, "queued");
    assert.equal(response.durablePersistenceConfirmed, false);
    assert.equal(response.appointmentConfirmed, false);
    assert.match(response.nextAction, /acknowledge once: Thanks\. I've received your callback request\. Only if the caller asks/);
    assert.match(response.nextAction, /request was received but no appointment is confirmed/);
    assert.match(response.nextAction, /do not infer AM\/PM or a calendar date/);
    assert.match(response.nextAction, /ASAP is a timing request, not a promised response time/);
    assert.doesNotMatch(response.nextAction, /I set (?:that|up)|I'll have (?:him|Yoni) (?:reach|call)/);
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
    assert.match(response.nextAction, /End only after a NEW explicit goodbye or request to stop or end the current call/);
    assert.match(response.nextAction, /Do not call end_call merely because this tool returned/);
    assert.doesNotMatch(response.nextAction, /is there anything else|Then immediately call end_call/i);
  }
});

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
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
axios.defaults.adapter = async (request) => {
  assert.equal(request.method, "post", "The response tests must not fetch provider evidence");
  assert.equal(request.url, "https://sheet.invalid/request-response-test", "Every request remains inside the local adapter");
  const body = JSON.parse(request.data);
  return new Promise((resolve, reject) => {
    pendingWrites.push({
      body,
      finish: (fail = false) => fail ? reject(new Error("Synthetic pending write failure")) :
        resolve({ data: { ok: true }, status: 200, statusText: "OK", headers: {}, config: request }),
    });
  });
};

async function invokeRequestTool(path: string, fields: Record<string, unknown>) {
  const { default: router } = await import("../src/routes/elevenLabs");
  const layer = router.stack.find((item: { route?: { path?: string } }) => item.route?.path === path);
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
