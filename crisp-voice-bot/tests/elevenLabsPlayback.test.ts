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

test("builds signed no-login ElevenLabs playback URLs", async () => {
  const { buildElevenLabsPlaybackUrl, verifyElevenLabsPlaybackSignature } = await import(
    "../src/lib/elevenLabsPlayback"
  );

  const playbackUrl = buildElevenLabsPlaybackUrl("conv_test123");
  const url = new URL(playbackUrl);
  const sig = url.searchParams.get("sig");

  assert.equal(url.origin, "https://voice.example.com");
  assert.equal(url.pathname, "/elevenlabs/playback/conv_test123");
  assert.match(sig ?? "", /^[a-f0-9]{32}$/);
  assert.equal(verifyElevenLabsPlaybackSignature("conv_test123", sig), true);
  assert.equal(verifyElevenLabsPlaybackSignature("conv_other", sig), false);
});

test("rejects malformed conversation ids for playback links", async () => {
  const { buildElevenLabsPlaybackUrl } = await import("../src/lib/elevenLabsPlayback");

  assert.throws(() => buildElevenLabsPlaybackUrl("../secret"), /Invalid ElevenLabs conversation id/);
});
