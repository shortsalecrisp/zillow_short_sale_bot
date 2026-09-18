import assert from "node:assert/strict";
import test, { after } from "node:test";
import axios from "axios";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.ELEVENLABS_ERYN_VOICE_ID = "eryn-voice-id";
process.env.ELEVENLABS_FINCH_VOICE_ID = "finch-voice-id";
process.env.ELEVENLABS_RACHEL_VOICE_ID = "rachel-voice-id";
process.env.ELEVENLABS_BELLA_VOICE_ID = "bella-voice-id";
process.env.ELEVENLABS_FINCH_TTS_SPEED = "0.93";
process.env.ELEVENLABS_VOICE_AB_TEST_ENABLED = "true";
process.env.ELEVENLABS_API_KEY = "synthetic-test-key";
process.env.ELEVENLABS_BASE_URL = "https://elevenlabs.invalid/measurement-start-test";
process.env.ELEVENLABS_AGENT_ID = "agent_syntheticmeasurement";
process.env.ELEVENLABS_AGENT_PHONE_NUMBER_ID = "phone_syntheticmeasurement";

const originalAdapter = axios.defaults.adapter;
const outboundBodies: Array<Record<string, any>> = [];
const unexpectedRequests: string[] = [];
axios.defaults.adapter = async (request) => {
  if (request.method === "post" && request.baseURL === "https://elevenlabs.invalid/measurement-start-test" &&
    request.url === "/v1/convai/sip-trunk/outbound-call") {
    const body = JSON.parse(request.data);
    assert.equal(body.agent_id, "agent_syntheticmeasurement");
    assert.equal(body.to_number, "+12025550123");
    outboundBodies.push(body);
    return { status: 200, statusText: "OK", headers: {}, config: request,
      data: { success: true, conversation_id: "conv_syntheticmeasurement" } };
  }
  unexpectedRequests.push(`${request.method} ${request.baseURL ?? ""}${request.url ?? ""}`);
  throw new Error("Unexpected network request in measurement-start test");
};
after(() => {
  axios.defaults.adapter = originalAdapter;
  assert.deepEqual(unexpectedRequests, []);
});

test("ElevenLabs calls rotate deterministically across Eryn and Finch by row", async () => {
  const { selectElevenLabsVoiceVariant } = await import("../src/lib/elevenLabsVoiceVariant");

  assert.deepEqual(selectElevenLabsVoiceVariant({ rowNumber: 3480 }), {
    key: "eryn",
    assistantName: "Maya",
    voiceName: "Eryn",
    voiceId: "eryn-voice-id",
  });
  assert.deepEqual(selectElevenLabsVoiceVariant({ rowNumber: 3481 }), {
    key: "finch",
    assistantName: "Finn",
    voiceName: "Finch",
    voiceId: "finch-voice-id",
    ttsSpeed: 0.93,
  });
  assert.deepEqual(selectElevenLabsVoiceVariant({ rowNumber: 3482 }), {
    key: "eryn",
    assistantName: "Maya",
    voiceName: "Eryn",
    voiceId: "eryn-voice-id",
  });
  assert.deepEqual(selectElevenLabsVoiceVariant({ rowNumber: 3483 }), {
    key: "finch",
    assistantName: "Finn",
    voiceName: "Finch",
    voiceId: "finch-voice-id",
    ttsSpeed: 0.93,
  });
});

test("ElevenLabs post-intro assignment preserves two deterministic plain-language variants", async () => {
  const { buildElevenLabsOpenerVariant } = await import("../src/lib/elevenLabsOpenerVariant");

  assert.deepEqual(buildElevenLabsOpenerVariant({ rowNumber: 3700, firstName: "Karimah", assistantName: "Maya" }), {
    key: "direct_reason",
    label: "Permission-first handling check",
    script: "I was calling about the short-sale paperwork and lender calls. Is it okay if I ask one quick question about that?",
  });
  assert.equal(
    buildElevenLabsOpenerVariant({ rowNumber: 3701, firstName: "Norma", assistantName: "Maya" }).key,
    "direct_reason",
  );
  assert.equal(
    buildElevenLabsOpenerVariant({ rowNumber: 3708, firstName: "Miriam", assistantName: "Maya" }).key,
    "direct_reason",
  );
  assert.equal(
    buildElevenLabsOpenerVariant({ rowNumber: 3708, firstName: "Miriam", assistantName: "Maya" }).script,
    "I was calling about the short-sale paperwork and lender calls. Is it okay if I ask one quick question about that?",
  );
  assert.equal(
    buildElevenLabsOpenerVariant({ rowNumber: 3709, firstName: "Marta", assistantName: "Maya" }).key,
    "direct_reason",
  );
  assert.equal(
    buildElevenLabsOpenerVariant({ rowNumber: 3703, firstName: "Miriam", assistantName: "Maya" }).script,
    "We help agents with short-sale paperwork and lender calls. Is it worth a quick minute to see if that would be useful on this one?",
  );

  for (let rowNumber = 3700; rowNumber < 3710; rowNumber += 1) {
    const opener = buildElevenLabsOpenerVariant({ rowNumber, firstName: "Taylor", assistantName: "Maya" });
    assert.doesNotMatch(opener.script, /^This is Maya with Crisp Short Sales\./);
  }
});

test("ElevenLabs outbound payload overrides the voice and assistant name per call", async () => {
  const { buildElevenLabsOutboundCallBody, INITIAL_OPENING_POLICY } = await import("../src/lib/elevenLabs");
  const { VOICE_CONVERSATION_POLICY_VERSION } = await import("../src/lib/elevenLabsConversationPolicy");

  const body = buildElevenLabsOutboundCallBody({
    agentId: "agent_123",
    agentPhoneNumberId: "phone_123",
    to: "+14043009526",
    metadata: {
      rowNumber: 3483,
      firstName: "Tina",
      lastName: "Agent",
      fullName: "Tina Agent",
      email: "tina@example.com",
      callAttemptNumber: 1,
      listingAddress: "123 Main St, Atlanta, GA",
      scheduledWindow: "late_morning",
      agentTimeZone: "America/New_York",
      requestedPhone: "+14045550123",
      dialedPhone: "+14043009526",
      testMode: false,
    },
  });

  assert.equal(body.conversation_initiation_client_data.dynamic_variables.assistantName, "Finn");
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.voiceVariant, "finch");
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.voiceName, "Finch");
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.openerVariant, "benefit_hook");
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.openerVariantLabel, "Permission-first help check");
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.initialOpeningPolicy, INITIAL_OPENING_POLICY);
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.declaredConversationPolicyVersion, VOICE_CONVERSATION_POLICY_VERSION);
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.terminal_permission, false);
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.terminal_decision, "reset");
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.scheduledWindow, "late_morning");
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.agentTimeZone, "America/New_York");
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.email, "tina@example.com");
  assert.equal(
    body.conversation_initiation_client_data.dynamic_variables.openerScript,
    "We help agents with short-sale paperwork and lender calls. Is it worth a quick minute to see if that would be useful on this one?",
  );
  assert.equal(
    body.conversation_initiation_client_data.conversation_config_override.tts.voice_id,
    "finch-voice-id",
  );
  assert.equal(body.conversation_initiation_client_data.conversation_config_override.tts.speed, 0.93);
  assert.match(
    body.conversation_initiation_client_data.dynamic_variables.voicemailMessage,
    /^Hi, this is Finn with Crisp Short Sales/,
  );
});

test("voice and post-intro arms rotate independently in a 2x2 assignment", async () => {
  const { buildElevenLabsOutboundCallBody } = await import("../src/lib/elevenLabs");
  const { VOICE_CONVERSATION_POLICY_VERSION } = await import("../src/lib/elevenLabsConversationPolicy");
  for (const [rowNumber, voiceVariant, openerVariant] of [
    [3480, "eryn", "direct_reason"],
    [3481, "finch", "direct_reason"],
    [3482, "eryn", "benefit_hook"],
    [3483, "finch", "benefit_hook"],
  ] as const) {
    const body = buildElevenLabsOutboundCallBody({
      agentId: "agent_syntheticmeasurement", agentPhoneNumberId: "phone_syntheticmeasurement", to: "+12025550123",
      metadata: { rowNumber, fullName: "Synthetic Caller", callAttemptNumber: 1, listingAddress: "123 Fictional Street",
        requestedPhone: "+12025550123", dialedPhone: "+12025550123", testMode: true,
        scheduledWindow: "late_morning", agentTimeZone: "America/New_York" },
    });
    const variables = body.conversation_initiation_client_data.dynamic_variables;
    assert.equal(variables.voiceVariant, voiceVariant);
    assert.equal(variables.openerVariant, openerVariant);
    assert.equal(variables.initialOpeningPolicy, "listen_first_uniform_v1");
    assert.equal(variables.declaredConversationPolicyVersion, VOICE_CONVERSATION_POLICY_VERSION);
    assert.equal(variables.terminal_permission, false);
    assert.equal(variables.terminal_decision, "reset");
    assert.equal(variables.scheduledWindow, "late_morning");
    assert.equal(variables.agentTimeZone, "America/New_York");
    assert.deepEqual(Object.keys(body.conversation_initiation_client_data.conversation_config_override), ["tts"]);
  }
});

test("new-call declarations are captured with outbound metadata without mutating input or asserting provider version", async () => {
  const { placeElevenLabsOutboundCall } = await import("../src/lib/elevenLabs");
  const { VOICE_CONVERSATION_POLICY_VERSION } = await import("../src/lib/elevenLabsConversationPolicy");
  const { getElevenLabsCallContextByConversationId, resetElevenLabsCallContextsForTest } = await import("../src/lib/elevenLabsCallContext");
  const metadata = { rowNumber: 3483, fullName: "Synthetic Caller", callAttemptNumber: 1,
    listingAddress: "123 Fictional Street", requestedPhone: "+12025550123", dialedPhone: "+12025550123", testMode: true,
    scheduledWindow: "late_morning", agentTimeZone: "America/New_York",
    initialOpeningPolicy: "older-declaration", declaredConversationPolicyVersion: "older-code-label",
    terminal_permission: true, terminal_decision: "allow_end" };
  const before = structuredClone(metadata), count = outboundBodies.length;
  resetElevenLabsCallContextsForTest();
  try {
    await placeElevenLabsOutboundCall({ to: "+12025550123", metadata, schedulePostCallFallback: false });
    assert.deepEqual(metadata, before);
    assert.equal(outboundBodies.length, count + 1);
    const captured = getElevenLabsCallContextByConversationId("conv_syntheticmeasurement")!;
    const variables = outboundBodies.at(-1)!.conversation_initiation_client_data.dynamic_variables;
    assert.equal(variables.terminal_permission, false);
    assert.equal(variables.terminal_decision, "reset");
    for (const value of [captured, variables]) {
      assert.equal(value.initialOpeningPolicy, "listen_first_uniform_v1");
      assert.equal(value.declaredConversationPolicyVersion, VOICE_CONVERSATION_POLICY_VERSION);
      assert.equal(value.openerVariant, "benefit_hook");
      assert.equal(value.voiceVariant, "finch");
      assert.equal(value.scheduledWindow, "late_morning");
      assert.equal(value.agentTimeZone, "America/New_York");
      assert.equal("providerIdentity" in value, false);
      assert.equal("version_id" in value, false);
    }
  } finally {
    resetElevenLabsCallContextsForTest();
  }
});

test("outbound payload always initializes protected guard variables instead of copying metadata", async () => {
  const { buildElevenLabsOutboundCallBody } = await import("../src/lib/elevenLabs");
  for (const [terminal_permission, terminal_decision] of [[true, "allow_end"], ["true", "pending_question_or_correction"], [null, null]]) {
    const metadata = { rowNumber: 123, fullName: "Synthetic Caller", callAttemptNumber: 1,
      listingAddress: "123 Fictional Street", requestedPhone: "+12025550123", dialedPhone: "+12025550123", testMode: false,
      terminal_permission, terminal_decision };
    const variables = buildElevenLabsOutboundCallBody({
      agentId: "agent_syntheticmeasurement", agentPhoneNumberId: "phone_syntheticmeasurement", to: "+12025550123", metadata,
    }).conversation_initiation_client_data.dynamic_variables;
    assert.equal(variables.terminal_permission, false);
    assert.equal(typeof variables.terminal_permission, "boolean");
    assert.equal(variables.terminal_decision, "reset");
  }
});
