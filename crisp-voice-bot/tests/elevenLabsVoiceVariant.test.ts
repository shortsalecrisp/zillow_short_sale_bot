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

test("ElevenLabs calls alternate deterministically between Eryn and Finch by row", async () => {
  const { selectElevenLabsVoiceVariant } = await import("../src/lib/elevenLabsVoiceVariant");

  assert.deepEqual(selectElevenLabsVoiceVariant({ rowNumber: 3480 }), {
    key: "eryn",
    assistantName: "Emmy",
    voiceName: "Eryn",
    voiceId: "eryn-voice-id",
  });
  assert.deepEqual(selectElevenLabsVoiceVariant({ rowNumber: 3481 }), {
    key: "finch",
    assistantName: "Finch",
    voiceName: "Finch",
    voiceId: "finch-voice-id",
  });
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
      requestedPhone: "+14045550123",
      dialedPhone: "+14043009526",
      testMode: false,
    },
  });

  assert.equal(body.conversation_initiation_client_data.dynamic_variables.assistantName, "Finch");
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.voiceVariant, "finch");
  assert.equal(body.conversation_initiation_client_data.dynamic_variables.voiceName, "Finch");
  assert.equal(
    body.conversation_initiation_client_data.conversation_config_override.tts.voice_id,
    "finch-voice-id",
  );
  assert.match(
    body.conversation_initiation_client_data.dynamic_variables.voicemailMessage,
    /^Hi, this is Finch with Crisp Short Sales/,
  );
});
