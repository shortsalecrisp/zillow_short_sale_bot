import assert from "node:assert/strict";
import test, { after } from "node:test";
import axios from "axios";

Object.assign(process.env, {
  BASE_URL: "https://example.com",
  TELNYX_API_KEY: "test",
  TELNYX_CALLER_ID: "+12175550100",
  TELNYX_CONNECTION_ID: "test",
  TELNYX_OUTBOUND_VOICE_PROFILE_ID: "test",
  TEST_DESTINATION_NUMBER: "+12175550101",
  ELEVENLABS_API_KEY: "synthetic-test-key",
  ELEVENLABS_BASE_URL: "https://elevenlabs.invalid/approved-20261124",
  GOOGLE_APPS_SCRIPT_WEBHOOK_URL: "https://sheet.invalid/approved-20261124",
  GOOGLE_SHEETS_SPREADSHEET_ID: "synthetic-sheet",
  GOOGLE_SHEETS_TAB_NAME: "Synthetic",
  CALL_TRANSCRIPT_EMAILS_ENABLED: "false",
  SMTP_HOST: "",
  SMTP_USER: "",
  SMTP_PASS: "",
  ALERT_EMAIL_TO: "",
});

const conversations = new Map<string, Record<string, unknown>>();
const originalAdapter = axios.defaults.adapter;
axios.defaults.adapter = async request => {
  const id = request.url?.split("/").at(-1) ?? "";
  const conversation = conversations.get(id);
  if (request.method !== "get" || request.baseURL !== process.env.ELEVENLABS_BASE_URL || !conversation) {
    throw new Error(`Unexpected request in approved 2026-11-24 test: ${request.method} ${request.baseURL}${request.url}`);
  }
  return { data: structuredClone(conversation), status: 200, statusText: "OK", headers: {}, config: request };
};
after(() => { axios.defaults.adapter = originalAdapter; });

const context = (rowNumber: number, name: string, phone = "+12025550123") => ({
  dynamic_variables: { rowNumber, callAttemptNumber: 1, agentName: name, firstName: name.split(" ")[0],
    listingAddress: "123 Synthetic St, Test, FL", requestedPhone: phone, phone, testMode: true,
    assistantName: "Maya" },
});

test("approved terminal precedence resolves the exact DNC, IVR, and recorded greetings", async () => {
  const api = await import("../src/lib/elevenLabsPostCall");
  const neville = { status: "done", has_user_audio: true, metadata: { termination_reason: "Client disconnected: 1000" }, transcript: [
    { role: "user", message: "Hello." },
    { role: "agent", message: "Hi, this is Maya with Crisp Short Sales. We help with short-sale lender paperwork." },
    { role: "user", message: "No, sir." },
    { role: "agent", message: "Sorry, I reached the wrong contact." },
    { role: "user", message: "Yes, you did." },
    { role: "user", message: "Don't ever call here again." },
  ] };
  assert.equal(api.getAuthoritativeTerminalCallResult(neville, 1, { firstName: "Neville" }), "do_not_call");

  const kyler = { status: "done", metadata: { termination_reason: "Client disconnected: 1000" }, transcript: [
    { role: "user", message: "Hello. Thank you for calling Century 21 Everest. Press one for our front desk or operator. Press two for closing. Press three for accounting." },
    { role: "user", message: "I'm sorry. I did not understand your response." },
    { role: "user", message: "Hello. Thank you for calling Century 21 Everest. Press one for our front desk or operator. Press two for closing. Press three for accounting." },
    { role: "user", message: "I'm sorry. I did not understand your response. Goodbye." },
  ] };
  assert.equal(api.getAuthoritativeTerminalCallResult(kyler, 1, { firstName: "Kyler" }), "agent_not_available");

  const erica = { status: "done", has_user_audio: true, transcript: [
    { role: "user", message: "Aloha, this is Erica Yun with Real Broker. I'm not available at this moment, but if you leave me a brief text message, I'll get back to you as soon as I can. Thank you for calling me and have a nice day." },
  ] };
  assert.equal(api.shouldTreatAsVoicemail(erica), true);
  assert.equal(api.shouldTreatAsDeferredContact(erica), false);
  assert.equal(api.getAuthoritativeTerminalCallResult(erica, 1, { firstName: "Erica" }), "voicemail_reached");

  const roslyn = { status: "done", has_user_audio: true, transcript: [
    { role: "user", message: "Hello. You have reached RM International Enterprises. We cannot take your call right now, so please leave your name, telephone number, and the reason for your call, and we'll get back to you as soon as possible. Have a great day." },
  ] };
  assert.equal(api.shouldTreatAsVoicemail(roslyn), true);
  assert.equal(api.getAuthoritativeTerminalCallResult(roslyn, 1, { firstName: "Roslyn" }), "voicemail_reached");
});

test("Spanish explicit refusal and greeting-only silence cannot create contact-request review", async () => {
  const api = await import("../src/lib/elevenLabsPostCall");
  const goldberg = { status: "done", has_user_audio: true, metadata: { termination_reason: "Client disconnected: 1000" }, transcript: [
    { role: "user", message: "Yes. ¿Eso es una computadora?" },
    { role: "agent", message: "Sí. Ya soy una asistente de inteligencia artificial de Crisp Short Sales." },
    { role: "agent", message: "¿Te gustaría ayuda con el papeleo del banco o las llamadas para este listado?" },
    { role: "user", message: "No." },
    { role: "agent", message: "Entendido. Gracias por avisarme." },
  ] };
  assert.equal(api.shouldTreatAsNotInterested(goldberg), true);
  assert.equal(api.getAuthoritativeTerminalCallResult(goldberg, 1, { firstName: "Goldberg" }), "answered_not_interested");

  const id = "conv_tahjacoxapproved";
  conversations.set(id, { conversation_id: id, status: "done", has_user_audio: true,
    metadata: { termination_reason: "Ending conversation after 45 seconds of silence.", call_duration_secs: 48 },
    conversation_initiation_client_data: context(7076, "Tahjá Cox", "+19038063203"),
    transcript: [
      { role: "user", message: "Hello?" },
      { role: "agent", message: "Hi, this is Maya with Crisp Short Sales. We help with short-sale lender paperwork. Is Sixty Eight Fifty Two Louis your listing?" },
      { role: "user", message: "..." },
    ],
  });
  const writes: Array<Record<string, unknown>> = [];
  assert.equal(await api.processPostCallOutcomeFromConversationId(id, {
    postSheetUpdate: async payload => { writes.push(payload); }, requestQueueRefill: async () => {},
  }), true);
  assert.equal(writes.length, 1);
  assert.equal(writes[0].callResult, "no_answer_first_attempt");
  assert.notEqual(writes[0].callResult, "contact_request_review");
});
