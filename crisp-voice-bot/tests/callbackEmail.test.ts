import assert from "node:assert/strict";
import test from "node:test";

process.env.BASE_URL = "https://voice.example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.ELEVENLABS_API_KEY = "elevenlabs-api-key";
process.env.ELEVENLABS_TOOL_SECRET = "playback-signing-secret";

test("callback email includes a signed playback link and full transcript when a conversation id is available", async () => {
  const { buildCallbackEmailMessage } = await import("../src/lib/sendCallbackEmail");

  const message = buildCallbackEmailMessage({
    agentName: "James Louis Grekousis",
    phone: "+19124338239",
    listingAddress: "626 Trevor Street, Hinesville, GA",
    rowNumber: 5565,
    callbackTime: "send info",
    conversationId: "conv_2201m1f4kmddfr78e6fnbhxbsx6p",
    conversationDescription: "handoff-ready interested callback",
    conversationTranscript: "Finn: We help agents with lender calls.\nAgent: Send me the information.",
  });

  assert.match(message.text, /Conversation ID: conv_2201m1f4kmddfr78e6fnbhxbsx6p/);
  assert.match(
    message.text,
    /Playback: https:\/\/voice\.example\.com\/elevenlabs\/playback\/conv_2201m1f4kmddfr78e6fnbhxbsx6p\?sig=[a-f0-9]{32}/,
  );
  assert.match(message.text, /Full Convo:\nFinn: We help agents with lender calls\.\nAgent: Send me the information\./);
  assert.match(
    message.html,
    /<a href="https:\/\/voice\.example\.com\/elevenlabs\/playback\/conv_2201m1f4kmddfr78e6fnbhxbsx6p\?sig=[a-f0-9]{32}"[^>]*>Play call recording<\/a>/,
  );
});

test("callback email remains usable before a voice conversation id exists", async () => {
  const { buildCallbackEmailMessage } = await import("../src/lib/sendCallbackEmail");

  const message = buildCallbackEmailMessage({
    agentName: "Legacy Telnyx Lead",
    phone: "+14043009526",
    listingAddress: "123 Main St, Atlanta, GA",
    rowNumber: 1234,
    callbackTime: "ASAP",
    conversationDescription: "Lead requested a callback.",
  });

  assert.doesNotMatch(message.text, /Playback:/);
  assert.match(message.text, /Full Convo:\nLead requested a callback\./);
});
