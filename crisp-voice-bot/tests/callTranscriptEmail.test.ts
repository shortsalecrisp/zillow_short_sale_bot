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

test("call transcript email includes a signed playback link", async () => {
  const { buildCallTranscriptEmailMessage } = await import("../src/lib/sendCallTranscriptEmail");

  const message = buildCallTranscriptEmailMessage({
    agentName: "Michael Boyd",
    requestedPhone: "+13173395392",
    dialedPhone: "+13173395392",
    listingAddress: "385 Amber Glenn, Maxwell, TX",
    rowNumber: 3836,
    callAttemptNumber: 1,
    conversationId: "conv_0901kvv5dzdff9xv1egk9s1jb9ec",
    outcome: "Requested callback at unspecified",
    summary: "Taylor requested a callback in 30 minutes.",
    transcript: "Maya: Hi\nAgent: Call in 30 minutes.",
    testMode: false,
    assistantName: "Maya",
    voiceName: "Eryn",
    voiceVariant: "eryn",
  });

  assert.match(
    message.text,
    /Playback: https:\/\/voice\.example\.com\/elevenlabs\/playback\/conv_0901kvv5dzdff9xv1egk9s1jb9ec\?sig=[a-f0-9]{32}/,
  );
  assert.match(
    message.html,
    /<a href="https:\/\/voice\.example\.com\/elevenlabs\/playback\/conv_0901kvv5dzdff9xv1egk9s1jb9ec\?sig=[a-f0-9]{32}"[^>]*>Play call recording<\/a>/,
  );
});
