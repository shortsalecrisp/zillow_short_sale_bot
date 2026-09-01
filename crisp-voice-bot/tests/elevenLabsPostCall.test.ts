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

const parvanehGatekeeperHoldConversation = {
  status: "done",
  metadata: {
    termination_reason: "Client disconnected: 1000",
    call_duration_secs: 42,
  },
  analysis: {
    transcript_summary:
      "Maya contacted a receptionist regarding Parvaneh's short sale listing. The receptionist asked for Maya's name and reason for calling, then asked Maya to stay on the line while they checked availability.",
  },
  transcript: [
    {
      role: "assistant",
      message:
        "Hi Parvaneh, this is Maya with Crisp Short Sales about your short sale listing. Are you handling the bank side yourself?",
    },
    { role: "user", message: "Hi, if you record your name and reason for calling I'll see if this person is available." },
    {
      role: "assistant",
      message: "Hi, this is Maya with Crisp Short Sales, calling about Parvaneh's short sale listing at Thirteen Thirty Five Opechee.",
    },
    { role: "user", message: "Thanks. Please stay on the line." },
    { role: "assistant", message: "Sure, I'll wait." },
    { role: "user", message: "..." },
  ],
} as const;

const parvanehLiveSummaryGatekeeperHoldConversation = {
  status: "done",
  metadata: {
    termination_reason: "Client disconnected: 1000",
    call_duration_secs: 42,
  },
  analysis: {
    transcript_summary:
      "An agent from Crisp Short Sales contacted the recipient regarding Parvaneh's short sale listing. After the agent identified herself and the purpose of the call, the recipient requested the agent's name and reason for calling, then placed the agent on hold.",
  },
  transcript: [
    {
      role: "assistant",
      message:
        "Hi Parvaneh, this is Maya with Crisp Short Sales about your short sale listing. Are you handling the bank side yourself?",
    },
    { role: "user", message: "Hi, if you record your name and reason for calling I'll see if this person is available." },
    {
      role: "assistant",
      message: "Hi, this is Maya with Crisp Short Sales, calling about Parvaneh's short sale listing at Thirteen Thirty Five Opechee.",
    },
    { role: "user", message: "Thanks. Please stay on the line." },
    { role: "assistant", message: "Sure, I\u2019ll wait." },
    { role: "user", message: "..." },
  ],
} as const;

const lorettaMisfiredTransferConversation = {
  status: "done",
  metadata: {
    termination_reason: "Client disconnected: 1000",
    call_duration_secs: 69,
  },
  analysis: {
    transcript_summary:
      "The agent agreed to a call with Yoni Kutler, a short sale specialist, but the call ended during the attempted transfer.",
  },
  transcript: [
    {
      role: "agent",
      message:
        "Hi Loretta, Maya calling for Yoni Kutler about your short sale listing. Are you handling the bank side yourself?",
    },
    { role: "user", message: "Um, no, I'm not. Um, who is this?" },
    {
      role: "agent",
      message:
        "I'm with Crisp Short Sales, working with Yoni Kutler, our short sale specialist. We help agents with short sale bank paperwork and lender calls. Are you handling the bank side yourself?",
    },
    { role: "user", message: "No, we're not." },
    {
      role: "agent",
      message:
        "Got it. We can take the lender paperwork and follow-up off your plate at no cost to you or the seller. Would you rather have Yoni give you a quick call, or should I send over info?",
    },
    { role: "user", message: "Yeah, sure." },
    { role: "agent", message: "Ok, hold on, let me see if he's available one second." },
    { role: "agent", tool_calls: [{ tool_name: "live_transfer_requested" }] },
    {
      role: "agent",
      tool_results: [
        {
          tool_name: "live_transfer_requested",
          result_value: "Tool execution was abandoned because the call ended before the tool could complete",
        },
      ],
    },
  ],
} as const;

const pattyAcceptedTransferFallbackConversation = {
  status: "done",
  metadata: {
    termination_reason: "Client disconnected: 1000",
    call_duration_secs: 84,
  },
  analysis: {
    transcript_summary:
      "The agent agreed to be connected with Yoni. The transfer attempt failed, so Maya offered to have Yoni call the agent back.",
  },
  transcript: [
    { role: "assistant", message: "Yoni can explain the details better than I can. Want me to see if he's available now?" },
    { role: "user", message: "Okay." },
    { role: "assistant", message: "Would you like me to bring him in to the call?" },
    { role: "user", message: "Yes." },
    { role: "assistant", message: "Ok, hold on, let me see if he's available one second." },
    { role: "assistant", tool_calls: [{ tool_name: "live_transfer_requested" }] },
    {
      role: "assistant",
      message:
        "Sorry, I am having trouble patching him in, but I will text him and ask him to call you back ASAP. Is that ok?",
    },
  ],
} as const;

const providerQuotaFailureConversation = {
  status: "failed",
  metadata: {
    termination_reason: "error",
    error: {
      code: 1002,
      reason: "This request exceeds your quota limit.",
    },
  },
  analysis: {
    transcript_summary: "The conversation begins with Maya introducing herself before the provider failed.",
  },
  transcript: [{ role: "assistant", message: "Hi Celeste, this is Maya with Crisp Short Sales..." }],
} as const;

const telnyxD17FailureConversation = {
  status: "failed",
  metadata: {
    termination_reason: "error",
    error: {
      code: 403,
      reason: "SIP status: 403. Account is disabled. D17",
    },
  },
  transcript: [],
} as const;

test("post-call fallback detects ElevenLabs quota failures as provider quota and not a prospect outcome", async () => {
  const { buildVoiceResponseStatus, shouldTreatAsProviderQuotaExceeded } = await import(
    "../src/lib/elevenLabsPostCall"
  );

  assert.equal(shouldTreatAsProviderQuotaExceeded(providerQuotaFailureConversation), true);
  assert.equal(
    buildVoiceResponseStatus("provider_quota_exceeded"),
    "ElevenLabs quota exceeded - call not counted",
  );
});

test("post-call fallback detects Telnyx D17 as a provider failure and not a prospect outcome", async () => {
  const { buildVoiceResponseStatus, shouldTreatAsProviderQuotaExceeded, shouldTreatAsTelnyxD17Failure } = await import(
    "../src/lib/elevenLabsPostCall"
  );

  assert.equal(shouldTreatAsTelnyxD17Failure(telnyxD17FailureConversation), true);
  assert.equal(shouldTreatAsProviderQuotaExceeded(telnyxD17FailureConversation), false);
  assert.equal(
    buildVoiceResponseStatus("provider_d17_failure"),
    "Telnyx account disabled (D17) - call not counted",
  );
});

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

test("post-call fallback classifies gatekeeper hold as agent unavailable instead of agent hangup", async () => {
  const { buildVoiceResponseStatus, shouldTreatAsAgentHungUp, shouldTreatAsAgentUnavailable } = await import(
    "../src/lib/elevenLabsPostCall"
  );

  assert.equal(shouldTreatAsAgentUnavailable(parvanehGatekeeperHoldConversation), true);
  assert.equal(shouldTreatAsAgentHungUp(parvanehGatekeeperHoldConversation), false);
  assert.equal(buildVoiceResponseStatus("agent_not_available"), "Agent was not available");
});

test("post-call fallback classifies live gatekeeper hold summaries as agent unavailable", async () => {
  const { shouldTreatAsAgentHungUp, shouldTreatAsAgentUnavailable } = await import("../src/lib/elevenLabsPostCall");

  assert.equal(shouldTreatAsAgentUnavailable(parvanehLiveSummaryGatekeeperHoldConversation), true);
  assert.equal(shouldTreatAsAgentHungUp(parvanehLiveSummaryGatekeeperHoldConversation), false);
});

test("post-call fallback keeps ambiguous quick-call transfer misfires as interested callbacks", async () => {
  const {
    shouldTreatAsAgentHungUp,
    shouldTreatAsCallback,
    shouldTreatAsMisfiredTransferInterestedCallback,
  } = await import("../src/lib/elevenLabsPostCall");

  assert.equal(shouldTreatAsCallback(lorettaMisfiredTransferConversation), false);
  assert.equal(shouldTreatAsMisfiredTransferInterestedCallback(lorettaMisfiredTransferConversation), true);
  assert.equal(shouldTreatAsAgentHungUp(lorettaMisfiredTransferConversation), false);
});

test("post-call fallback records an accepted transfer fallback as an ASAP callback instead of a hangup", async () => {
  const {
    shouldTreatAsAcceptedTransferCallback,
    shouldTreatAsAgentHungUp,
    shouldTreatAsMisfiredTransferInterestedCallback,
  } = await import("../src/lib/elevenLabsPostCall");

  assert.equal(shouldTreatAsAcceptedTransferCallback(pattyAcceptedTransferFallbackConversation), true);
  assert.equal(shouldTreatAsMisfiredTransferInterestedCallback(pattyAcceptedTransferFallbackConversation), false);
  assert.equal(shouldTreatAsAgentHungUp(pattyAcceptedTransferFallbackConversation), false);
});

test("post-call fallback treats screening recordings and canned ASAP fragments as unavailable", async () => {
  const {
    shouldTreatAsAgentHungUp,
    shouldTreatAsAgentUnavailable,
    shouldTreatAsCallback,
    shouldTreatAsRecordingArtifact,
  } = await import("../src/lib/elevenLabsPostCall") as typeof import("../src/lib/elevenLabsPostCall") & {
    shouldTreatAsCallback: (conversation: unknown) => boolean;
  };
  const conversation = {
    status: "done",
    metadata: { termination_reason: "Client disconnected: 1000" },
    analysis: { transcript_summary: "An automated call-screening service asked Maya to record her name and reason." },
    transcript: [
      { role: "user", message: "Please record your name and reason for calling." },
      { role: "assistant", message: "Maya with Crisp Short Sales, calling about a short sale listing." },
      { role: "user", message: "As soon as possible. Thank you." },
      { role: "assistant", tool_calls: [{ tool_name: "callback_requested" }] },
    ],
  };

  assert.equal(shouldTreatAsRecordingArtifact(conversation), true);
  assert.equal(shouldTreatAsAgentUnavailable(conversation), true);
  assert.equal(shouldTreatAsAgentHungUp(conversation), false);
  assert.equal(shouldTreatAsCallback(conversation), false);
});

test("post-call fallback identifies wrong-person and unrelated-business voicemail", async () => {
  const { buildVoiceResponseStatus, shouldTreatAsIdentityMismatchVoicemail } = await import(
    "../src/lib/elevenLabsPostCall"
  );
  const wrongPerson = {
    status: "done",
    transcript: [{ role: "user", message: "Hi, you've reached Tina. Please leave a message." }],
  };
  const unrelatedBusiness = {
    status: "done",
    transcript: [
      { role: "user", message: "Thank you for calling Dale's Superstore customer service hotline." },
    ],
  };
  const matchingMailbox = {
    status: "done",
    transcript: [{ role: "user", message: "Hi, you've reached DeAnn. Please leave a message." }],
  };
  const matchingBusinessMailbox = {
    status: "done",
    transcript: [
      { role: "user", message: "Ms. Foster at Foster and Williams Real Estate is currently unavailable. Please leave a message." },
    ],
  };
  const similarSoundingMailbox = {
    status: "done",
    transcript: [{ role: "user", message: "Hi, you've reached Janine Peeler. Please leave a message." }],
  };

  assert.equal(shouldTreatAsIdentityMismatchVoicemail(wrongPerson, "DeAnn"), true);
  assert.equal(shouldTreatAsIdentityMismatchVoicemail(unrelatedBusiness, "Vanessa"), true);
  assert.equal(shouldTreatAsIdentityMismatchVoicemail(matchingMailbox, "DeAnn"), false);
  assert.equal(shouldTreatAsIdentityMismatchVoicemail(matchingBusinessMailbox, "Seatrice", "Foster"), false);
  assert.equal(shouldTreatAsIdentityMismatchVoicemail(similarSoundingMailbox, "Deneen", "Peeler"), false);
  assert.equal(
    buildVoiceResponseStatus("identity_mismatch_voicemail"),
    "Identity mismatch voicemail - target not reached",
  );
});

test("post-call fallback classifies a completed automated screener as agent unavailable", async () => {
  const { shouldTreatAsAgentUnavailable } = await import("../src/lib/elevenLabsPostCall");
  const conversation = {
    status: "done",
    metadata: { termination_reason: "end_call tool was called." },
    analysis: { transcript_summary: "An automated call screening service held Maya, then said the person was unavailable." },
    transcript: [
      { role: "user", message: "Please say your name and reason for calling, then stay on the line." },
      { role: "assistant", message: "Maya with Crisp Short Sales, calling about a short sale listing." },
      { role: "user", message: "Please stay on the line while I try to reach them." },
      { role: "assistant", message: "Sure, I'll wait." },
      { role: "user", message: "The person you are calling is not available. You may leave an additional message." },
    ],
  };

  assert.equal(shouldTreatAsAgentUnavailable(conversation), true);
});

test("post-call fallback distinguishes a confirmed self-handling target from automated screening", async () => {
  const {
    buildVoiceResponseStatus,
    shouldTreatAsAgentHungUp,
    shouldTreatAsAgentUnavailable,
    shouldTreatAsTargetReachedSelfHandlingDisconnect,
  } = await import("../src/lib/elevenLabsPostCall");
  const confirmedTarget = {
    status: "done",
    metadata: { termination_reason: "Client disconnected: 1000" },
    transcript: [
      { role: "assistant", message: "Hey, is this Valerie?" },
      { role: "user", message: "This is Valerie." },
      { role: "assistant", message: "Are you handling the bank side of the short sale yourself?" },
      { role: "user", message: "Yes, I am." },
      { role: "assistant", message: "We can take the lender paperwork and follow-up off your plate." },
    ],
  };
  const automatedScreen = {
    status: "done",
    metadata: { termination_reason: "Client disconnected: 1000" },
    analysis: { transcript_summary: "A Homes.com automated call-screening system answered; the agent was never reached." },
    transcript: [
      { role: "user", message: "Press one to connect. We are not able to connect your call. State your reason for calling." },
      { role: "assistant", message: "Crisp Short Sales, calling about a short sale listing." },
    ],
  };

  assert.equal(shouldTreatAsTargetReachedSelfHandlingDisconnect(confirmedTarget, "Valerie"), true);
  assert.equal(shouldTreatAsAgentUnavailable(confirmedTarget), false);
  assert.equal(shouldTreatAsAgentHungUp(confirmedTarget), true);
  assert.equal(
    buildVoiceResponseStatus("agent_reached_self_handling_disconnected"),
    "Agent reached; handling bank side; call disconnected",
  );
  assert.equal(shouldTreatAsTargetReachedSelfHandlingDisconnect(automatedScreen, "Kevin"), false);
  assert.equal(shouldTreatAsAgentUnavailable(automatedScreen), true);
  assert.equal(shouldTreatAsAgentHungUp(automatedScreen), false);
});

test("post-call fallback treats a completed screening prompt without live target contact as unavailable", async () => {
  const { shouldTreatAsAgentHungUp, shouldTreatAsAgentUnavailable } = await import(
    "../src/lib/elevenLabsPostCall"
  );
  const conversation = {
    status: "done",
    metadata: { termination_reason: "Client disconnected: 1000" },
    analysis: { transcript_summary: "An automated call-screening service asked Maya to record her name and reason." },
    transcript: [
      { role: "user", message: "Please record your name and reason for calling." },
      { role: "assistant", message: "Maya with Crisp Short Sales, calling about a short sale listing." },
    ],
  };

  assert.equal(shouldTreatAsAgentUnavailable(conversation), true);
  assert.equal(shouldTreatAsAgentHungUp(conversation), false);
});

test("post-call fallback distinguishes a live do-not-call request from generic not interested", async () => {
  const { buildVoiceResponseStatus, shouldTreatAsAgentHungUp, shouldTreatAsDoNotCall } = await import(
    "../src/lib/elevenLabsPostCall"
  );
  const conversation = {
    status: "done",
    metadata: { termination_reason: "end_call tool was called." },
    analysis: { transcript_summary: "The live caller explicitly requested no further calls." },
    transcript: [
      { role: "assistant", message: "Are you handling the bank side yourself?" },
      { role: "user", message: "Take me off your list and do not call again." },
      { role: "assistant", tool_calls: [{ tool_name: "not_interested" }] },
    ],
  };

  assert.equal(shouldTreatAsDoNotCall(conversation), true);
  assert.equal(shouldTreatAsAgentHungUp(conversation), false);
  assert.equal(buildVoiceResponseStatus("do_not_call"), "Do not call");
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

test("post-call transcript labels assistant turns with the selected assistant name", async () => {
  const { buildVoiceResponseStatus, transcriptForEmail } = await import("../src/lib/elevenLabsPostCall");
  const conversation = {
    transcript: [
      { role: "assistant", message: "Hey, is this Chris?" },
      { role: "user", message: "This is Chris." },
    ],
  };

  assert.equal(transcriptForEmail(conversation, "Finch"), "Finch: Hey, is this Chris?\nAgent: This is Chris.");
  assert.equal(buildVoiceResponseStatus("call_received_agent_hung_up", undefined, "Finch"), "Call received but agent hung up on Finch");
});
