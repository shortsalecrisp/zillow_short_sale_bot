import assert from "node:assert/strict";
import test from "node:test";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";

const rodrigoConversation = {
  status: "done",
  metadata: {
    termination_reason: "client disconnected",
  },
  analysis: {
    transcript_summary:
      "Rodrigo clarified that the listing was not a short sale and he was unavailable to talk.",
  },
  transcript: [
    { role: "assistant", message: '<break time="1.0s" /> Hi, is this Rodrigo?' },
    { role: "user", message: "This is Rodrigo. Yes, who's calling?" },
    {
      role: "assistant",
      message: "Hi Rodrigo, this is Emmy with Crisp Short Sales about your listing at 3720 Royal Crest. Got a quick second?",
    },
    { role: "user", message: "Uh, no, I don't, and it's not a short sale." },
    {
      role: "assistant",
      message:
        "Ok, thanks for letting me know. If anything ever changes in the future and you're looking for some additional help with these deals, please just keep us in mind. Thanks.",
    },
    { role: "user", message: "Thank you." },
  ],
} as const;

test("post-call fallback classifies an agent saying it is not a short sale as not_short_sale", async () => {
  const { buildVoiceResponseStatus, shouldTreatAsAgentHungUp, shouldTreatAsNotShortSale } = await import(
    "../src/lib/elevenLabsPostCall"
  );

  assert.equal(shouldTreatAsNotShortSale(rodrigoConversation), true);
  assert.equal(shouldTreatAsAgentHungUp(rodrigoConversation), false);
  assert.equal(buildVoiceResponseStatus("not_short_sale"), "Not a short sale");
});
