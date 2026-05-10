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

const taniaConversation = {
  status: "done",
  metadata: {
    termination_reason: "end_call tool was called.",
  },
  analysis: {
    transcript_summary:
      "An agent from Crisp Short Sales contacted Tania. Tania informed the agent that she already has a short sale negotiator, leading the agent to conclude the call.",
  },
  transcript: [
    { role: "assistant", message: "What's your plan for handling the short sale with the bank?" },
    { role: "user", message: "I have a short sale negotiator." },
    {
      role: "assistant",
      message:
        "Ok, well thanks for letting me know. If anything changes in the future and you're looking for some additional help, please just keep me in mind. Thanks!",
    },
  ],
} as const;

const danielConversation = {
  status: "done",
  metadata: {
    termination_reason: "Client disconnected: 1000",
  },
  analysis: {
    transcript_summary:
      "Dan clarified that he had already purchased the property from the bank and was not involved in a short sale.",
  },
  transcript: [
    { role: "assistant", message: "What's your plan for handling the short sale with the bank?" },
    { role: "user", message: "Uh, I don't have a short sale. I bought it from the bank." },
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

test("post-call fallback classifies no short sale ownership explanations as not_short_sale", async () => {
  const { shouldTreatAsAgentHungUp, shouldTreatAsNotShortSale } = await import("../src/lib/elevenLabsPostCall");

  assert.equal(shouldTreatAsNotShortSale(danielConversation), true);
  assert.equal(shouldTreatAsAgentHungUp(danielConversation), false);
});

test("post-call fallback marks existing short sale help as already working with negotiator", async () => {
  const { buildVoiceResponseStatus, shouldTreatAsAgentHungUp, shouldTreatAsAlreadyHasShortSaleHelp } = await import(
    "../src/lib/elevenLabsPostCall"
  );

  assert.equal(shouldTreatAsAlreadyHasShortSaleHelp(taniaConversation), true);
  assert.equal(shouldTreatAsAgentHungUp(taniaConversation), false);
  assert.equal(buildVoiceResponseStatus("already_working_with_negotiator"), "Already working with negotiator");
});

test("post-call fallback treats stale initiated conversations with no audio as no-connect", async () => {
  const { shouldRetryUnconnectedConversation, shouldTreatAsUnconnectedInitiatedConversation } = await import(
    "../src/lib/elevenLabsPostCall"
  );
  const unconnectedConversation = {
    status: "initiated",
    has_audio: false,
    has_user_audio: false,
    has_response_audio: false,
    metadata: {
      accepted_time_unix_secs: null,
      call_duration_secs: 0,
    },
    transcript: [],
  };

  assert.equal(shouldTreatAsUnconnectedInitiatedConversation(unconnectedConversation), true);
  assert.equal(shouldRetryUnconnectedConversation(unconnectedConversation, { callConnectRetryCount: 0 }), true);
  assert.equal(shouldRetryUnconnectedConversation(unconnectedConversation, { callConnectRetryCount: 1 }), false);

  assert.equal(
    shouldTreatAsUnconnectedInitiatedConversation({
      status: "initiated",
      has_audio: true,
      metadata: {
        accepted_time_unix_secs: 1778106342,
        call_duration_secs: 53,
      },
      transcript: [{ role: "user", message: "Hello?" }],
    }),
    false,
  );
});
