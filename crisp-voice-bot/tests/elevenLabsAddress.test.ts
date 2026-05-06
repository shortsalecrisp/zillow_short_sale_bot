import assert from "node:assert/strict";
import test from "node:test";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.ELEVENLABS_AGENT_ID = "agent";
process.env.ELEVENLABS_AGENT_PHONE_NUMBER_ID = "phone";

test("formats street numbers for natural spoken addresses", async () => {
  const { getStreetAddress } = await import("../src/lib/elevenLabs");
  assert.equal(typeof getStreetAddress, "function");

  assert.equal(
    getStreetAddress("641 SW 14th Ct, Deerfield Beach, FL"),
    "Six Forty One Southwest 14th",
  );
  assert.equal(
    getStreetAddress("7855 S Kalispell Circle, Englewood, CO"),
    "Seventy Eight Fifty Five South Kalispell",
  );
  assert.equal(getStreetAddress("1025 W Main St, Tampa, FL"), "Ten Twenty Five West Main");
  assert.equal(getStreetAddress("21 N Main St, Tampa, FL"), "Twenty One North Main");
});
