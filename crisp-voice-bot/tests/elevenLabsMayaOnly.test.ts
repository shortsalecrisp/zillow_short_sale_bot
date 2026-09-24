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
process.env.ELEVENLABS_VOICE_AB_TEST_ENABLED = "false";

test("disabled voice experiment exposes only Eryn as Maya", async () => {
  const { getElevenLabsVoiceExperimentStatus, selectElevenLabsVoiceVariant } = await import(
    "../src/lib/elevenLabsVoiceVariant"
  );

  assert.deepEqual(getElevenLabsVoiceExperimentStatus(), {
    enabled: false,
    selectionRule: "fixed_primary_voice",
    publicAssistantName: "Maya",
    variants: [{
      key: "eryn",
      voiceName: "Eryn",
      assistantName: "Maya",
      voiceIdConfigured: true,
      ttsSpeed: null,
    }],
  });
  assert.equal(selectElevenLabsVoiceVariant({ rowNumber: 3481 }).key, "eryn");
});

test("fixed-primary mode ignores a stale Finch override on a queued call", async () => {
  const { buildElevenLabsOutboundCallBody } = await import("../src/lib/elevenLabs");
  const body = buildElevenLabsOutboundCallBody({
    agentId: "agent_test",
    agentPhoneNumberId: "phone_test",
    to: "+12025550123",
    metadata: {
      rowNumber: 3481,
      fullName: "Synthetic Caller",
      callAttemptNumber: 1,
      listingAddress: "123 Fictional Street",
      requestedPhone: "+12025550123",
      dialedPhone: "+12025550123",
      testMode: true,
      voiceVariant: "finch",
      assistantName: "Finn",
      voiceName: "Finch",
      voiceId: "finch-voice-id",
    },
  });
  const clientData = body.conversation_initiation_client_data;

  assert.equal(clientData.dynamic_variables.voiceVariant, "eryn");
  assert.equal(clientData.dynamic_variables.assistantName, "Maya");
  assert.equal(clientData.dynamic_variables.voiceName, "Eryn");
  assert.equal(clientData.conversation_config_override.tts.voice_id, "eryn-voice-id");
  assert.match(String(clientData.dynamic_variables.voicemailMessage), /^Hi, this is Maya with Crisp Short Sales/);
});
