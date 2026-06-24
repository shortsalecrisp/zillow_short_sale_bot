import assert from "node:assert/strict";
import test from "node:test";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.ELEVENLABS_ERYN_VOICE_ID = "eryn-voice-id";
process.env.ELEVENLABS_FINCH_VOICE_ID = "finch-voice-id";

test("ElevenLabs calls use Eryn voice with Maya assistant name for every row", async () => {
  const { selectElevenLabsVoiceVariant } = await import("../src/lib/elevenLabsVoiceVariant");

  assert.deepEqual(selectElevenLabsVoiceVariant({ rowNumber: 3480 }), {
    key: "eryn",
    assistantName: "Maya",
    voiceName: "Eryn",
    voiceId: "eryn-voice-id",
  });
  assert.deepEqual(selectElevenLabsVoiceVariant({ rowNumber: 3481 }), {
    key: "eryn",
    assistantName: "Maya",
    voiceName: "Eryn",
    voiceId: "eryn-voice-id",
  });
});

test("ElevenLabs opener test assigns weighted opener variants by row", async () => {
  const { buildElevenLabsOpenerVariant } = await import("../src/lib/elevenLabsOpenerVariant");

  assert.deepEqual(buildElevenLabsOpenerVariant({ rowNumber: 3700, firstName: "Karimah", assistantName: "Maya" }), {
    key: "identity_check_short",
    label: "Short identity check control",
    script: "Hey, this is Maya with Crisp Short Sales. Is this Karimah?",
  });
  assert.equal(
    buildElevenLabsOpenerVariant({ rowNumber: 3701, firstName: "Norma", assistantName: "Maya" }).key,
    "direct_reason",
  );
  assert.equal(
    buildElevenLabsOpenerVariant({ rowNumber: 3704, firstName: "Miriam", assistantName: "Maya" }).key,
    "yoni_name",
  );
  assert.equal(
    buildElevenLabsOpenerVariant({ rowNumber: 3707, firstName: "Marta", assistantName: "Maya" }).key,
    "benefit_hook",
  );
});

test("ElevenLabs outbound payload overrides the voice and assistant name per call", async () => {
  const { buildElevenLabsOutboundCallBody } = await import("../src/lib/elevenLabs");

  const body = buildElevenLabsOutboundCallBody({
    agentId: "agent_123",
    agentPhoneNumberId: "phone_123",
    to: "+14043009526",
    metadata: {
      rowNumber: 3481,
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

  assert.equal(body.conversation_initiation_client_data.dynamic_variables.assistantName, "Maya");
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.voiceVariant, "eryn");
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.voiceName, "Eryn");
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.openerVariant, "direct_reason");
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.openerVariantLabel, "Direct short sale reason");
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.scheduledWindow, "late_morning");
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.agentTimeZone, "America/New_York");
  assert.equal(
    body.conversation_initiation_client_data.dynamic_variables.openerScript,
    "Hi Tina, this is Maya with Crisp Short Sales about your short sale listing. Are you handling the bank side yourself?",
  );
  assert.equal(
    body.conversation_initiation_client_data.conversation_config_override.tts.voice_id,
    "eryn-voice-id",
  );
  assert.match(
    body.conversation_initiation_client_data.dynamic_variables.voicemailMessage,
    /^Hi, this is Maya with Crisp Short Sales/,
  );
});
