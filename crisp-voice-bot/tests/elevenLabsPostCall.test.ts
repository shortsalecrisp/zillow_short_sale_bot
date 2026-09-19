import assert from "node:assert/strict";
import test, { after } from "node:test";
import axios from "axios";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.ELEVENLABS_API_KEY = "synthetic-test-key";
process.env.ELEVENLABS_BASE_URL = "https://elevenlabs.invalid/measurement-post-call-test";
process.env.GOOGLE_APPS_SCRIPT_WEBHOOK_URL = "https://sheet.invalid/measurement-post-call-test";
process.env.GOOGLE_APPS_SCRIPT_TOKEN = "";
process.env.GOOGLE_SHEETS_SPREADSHEET_ID = "synthetic-measurement-sheet";
process.env.GOOGLE_SHEETS_TAB_NAME = "SyntheticMeasurementLeads";
process.env.CALL_TRANSCRIPT_EMAILS_ENABLED = "false";
process.env.SMTP_HOST = "";
process.env.SMTP_USER = "";
process.env.SMTP_PASS = "";
process.env.ALERT_EMAIL_TO = "";

const originalAdapter = axios.defaults.adapter;
const measurementConversations = new Map<string, Record<string, unknown>>();
const measurementWrites: Array<Record<string, unknown>> = [];
const unexpectedRequests: string[] = [];
axios.defaults.adapter = async (request) => {
  if (request.method === "get" && request.baseURL === "https://elevenlabs.invalid/measurement-post-call-test") {
    const match = request.url?.match(/^\/v1\/convai\/conversations\/(conv_measurement_[a-z_]+)$/);
    const conversation = match && measurementConversations.get(match[1]);
    if (conversation) return { data: structuredClone(conversation), status: 200, statusText: "OK", headers: {}, config: request };
  }
  if (request.method === "post" && request.url === "https://sheet.invalid/measurement-post-call-test") {
    const body = JSON.parse(request.data);
    if (body.action === "process_voice_queue" || (body.rowNumber === 123 && body.callAttemptNumber === 1)) {
      measurementWrites.push(body);
      const data = body.action === "process_voice_queue" ? { ok: true } : {
        ok: true, rowNumber: 123,
        fieldsWritten: ["synthetic:callResult", "synthetic:call_eligible", "synthetic:call_time_bucket", "synthetic:call_scheduled_for"],
      };
      return { data, status: 200, statusText: "OK", headers: {}, config: request };
    }
  }
  unexpectedRequests.push(`${request.method} ${request.baseURL ?? ""}${request.url ?? ""}`);
  throw new Error("Unexpected network request in measurement post-call test");
};
after(() => {
  axios.defaults.adapter = originalAdapter;
  assert.deepEqual(unexpectedRequests, []);
});

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

const cinthiaFullMailboxConversation = {
  status: "done",
  metadata: {
    termination_reason: "Client disconnected: 1000",
    call_duration_secs: 22,
  },
  analysis: {
    transcript_summary:
      "The agent, Finn from Crisp Short Sales, attempted to contact the user regarding a short sale listing. The call went to voicemail, which indicated the user was unavailable. However, the voicemail system then stated that the mailbox was full and could not accept any messages, ending the call without the agent being able to leave a message.",
  },
  transcript: [
    { role: "agent", message: "Hi, this is Finn with Crisp Short Sales. I'm calling about your short sale listing." },
    {
      role: "user",
      message:
        "... not able to take your call right now. Please leave me a message and I will get back to you. Hola, se ha comunicado con Cynthia Martínez. Ahorita no está disponible para recibir su llamada. Por favor, de-dejar un mensaje. Gracias. The mailbox is full and cannot accept any messages at this time. Goodbye.",
    },
  ],
} as const;

const summerVoicemailConversation = {
  status: "done",
  metadata: {
    termination_reason: "voicemail_detection tool was called.",
    call_duration_secs: 43,
    features_usage: { voicemail_detection: { used: true } },
  },
  analysis: {
    transcript_summary:
      "The agent, Maya from Crisp Short Sales, attempted to discuss a short sale listing. Upon encountering an automated voicemail greeting, the agent left a detailed message.",
  },
  transcript: [
    { role: "agent", message: "Hi, this is Maya with Crisp Short Sales. I'm calling about your short sale listing." },
    {
      role: "user",
      message:
        "So your message is very important to me. Leave your name, number, and a brief message, and I'll get back to you shortly. Have a great day.",
    },
    {
      role: "agent",
      message:
        "Hi, this is Maya with Crisp Short Sales calling about the short sale listing at Ninety Five Oh Six One Waikalani. We specialize in helping agents with the short sale process and can handle the paperwork, phone calls, and the whole process with the lender to take that work off your shoulders. Yoni is our short sale specialist, and he can answer any questions you have. Give him a call back at 404-300-9526 when you get a chance. Thanks.",
      tool_calls: [{ tool_name: "voicemail_detection" }],
    },
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

const providerLlmFailureConversation = {
  status: "failed",
  metadata: {
    termination_reason: "All LLMs have failed",
    error: {
      code: 1002,
      reason: "LLM Cascade Error: ",
      error_type: "llm_error",
    },
  },
  analysis: {
    transcript_summary:
      "The agent, identifying as Finn from Crisp Short Sales, reached a call screening assistant before the LLM failed.",
  },
  transcript: [
    { role: "agent", message: "This is Finn with Crisp Short Sales." },
    {
      role: "user",
      message: "Can you record your name and reason for calling? I'll see if this person is available.",
    },
    { role: "agent", tool_calls: [{ tool_name: "skip_turn" }] },
  ],
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

const georgeNetworkBlockedConversation = {
  status: "failed",
  metadata: {
    termination_reason: "error",
    error: {
      code: 603,
      reason: "SIP status: 603. Network Blocked",
    },
  },
  analysis: {
    transcript_summary: "The outbound call failed before the agent connected.",
  },
  transcript: [],
} as const;

test("post-call fallback detects ElevenLabs quota failures as provider quota and not a prospect outcome", async () => {
  const {
    buildVoiceResponseStatus,
    shouldTreatAsElevenLabsLlmFailure,
    shouldTreatAsProviderQuotaExceeded,
  } = await import("../src/lib/elevenLabsPostCall");

  assert.equal(shouldTreatAsProviderQuotaExceeded(providerQuotaFailureConversation), true);
  assert.equal(shouldTreatAsElevenLabsLlmFailure(providerQuotaFailureConversation), false);
  assert.equal(
    buildVoiceResponseStatus("provider_quota_exceeded"),
    "ElevenLabs quota exceeded - call not counted",
  );
});

test("post-call fallback separates ElevenLabs LLM cascade failures from quota", async () => {
  const {
    buildVoiceResponseStatus,
    shouldTreatAsElevenLabsLlmFailure,
    shouldTreatAsProviderQuotaExceeded,
    shouldTreatAsTelnyxD17Failure,
  } = await import("../src/lib/elevenLabsPostCall");

  assert.equal(shouldTreatAsElevenLabsLlmFailure(providerLlmFailureConversation), true);
  assert.equal(shouldTreatAsProviderQuotaExceeded(providerLlmFailureConversation), false);
  assert.equal(shouldTreatAsTelnyxD17Failure(providerLlmFailureConversation), false);
  assert.equal(
    buildVoiceResponseStatus("provider_llm_failure"),
    "ElevenLabs LLM cascade failure - call not counted",
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

test("post-call fallback gives SIP 603 Network Blocked a durable terminal outcome", async () => {
  const { buildVoiceResponseStatus, getTerminalFailedConversationCallResult } = await import(
    "../src/lib/elevenLabsPostCall"
  );

  const callResult = getTerminalFailedConversationCallResult(georgeNetworkBlockedConversation);
  assert.equal(callResult, "call_failed_before_completion");
  assert.equal(buildVoiceResponseStatus(callResult), "Call failed before completion");
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

test("post-call fallback reviews an ambiguous call-or-info choice without inventing callback consent", async () => {
  const {
    shouldTreatAsAgentHungUp,
    shouldTreatAsCallback,
    shouldTreatAsMisfiredTransferInterestedCallback,
    getUnconsentedTransferReviewResult,
  } = await import("../src/lib/elevenLabsPostCall");

  assert.equal(shouldTreatAsCallback(lorettaMisfiredTransferConversation), false);
  assert.equal(shouldTreatAsMisfiredTransferInterestedCallback(lorettaMisfiredTransferConversation), false);
  assert.equal(getUnconsentedTransferReviewResult(lorettaMisfiredTransferConversation), "contact_request_review");
  assert.equal(shouldTreatAsAgentHungUp(lorettaMisfiredTransferConversation), false);
});

test("post-call fallback preserves accepted-transfer interest without treating an unanswered callback offer as consent", async () => {
  const {
    shouldTreatAsAcceptedTransferCallback,
    shouldTreatAsAgentHungUp,
    shouldTreatAsMisfiredTransferInterestedCallback,
    getUnconsentedTransferReviewResult,
  } = await import("../src/lib/elevenLabsPostCall");

  assert.equal(shouldTreatAsAcceptedTransferCallback(pattyAcceptedTransferFallbackConversation), false);
  assert.equal(getUnconsentedTransferReviewResult(pattyAcceptedTransferFallbackConversation), "interested_followup_review");
  assert.equal(shouldTreatAsMisfiredTransferInterestedCallback(pattyAcceptedTransferFallbackConversation), false);
  assert.equal(shouldTreatAsAgentHungUp(pattyAcceptedTransferFallbackConversation), false);
});

test("post-call fallback accepts a separate actual yes to the callback offer after a failed transfer", async () => {
  const { shouldTreatAsAcceptedTransferCallback, getExplicitCallbackConsent } = await import("../src/lib/elevenLabsPostCall");
  const withConsent = { ...pattyAcceptedTransferFallbackConversation,
    transcript: [...pattyAcceptedTransferFallbackConversation.transcript, { role: "user", message: "Yes, that's fine." }],
  };
  assert.equal(shouldTreatAsAcceptedTransferCallback(withConsent), true);
  assert.equal(getExplicitCallbackConsent(withConsent)?.callbackTime, "asap");
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

test("full-mailbox recording is voicemail, not a human hangup", async () => {
  const { shouldTreatAsAgentHungUp, shouldTreatAsAgentUnavailable } = await import("../src/lib/elevenLabsPostCall");
  const conversation = {
    status: "done",
    metadata: { termination_reason: "client disconnected" },
    analysis: { transcript_summary: "The mailbox is full and cannot accept any messages at this time." },
    transcript: [
      { role: "assistant", message: "Hi, is this Jelenia?" },
      { role: "user", message: "The mailbox is full and cannot accept any messages at this time. Goodbye." },
    ],
  };
  assert.equal(shouldTreatAsAgentHungUp(conversation), false);
  assert.equal(shouldTreatAsAgentUnavailable(conversation), false);
});

test("Google Call Assist follow-up prompts remain screening, with real human reply preserved", async () => {
  const { shouldTreatAsAgentHungUp, shouldTreatAsAgentUnavailable } = await import("../src/lib/elevenLabsPostCall");
  const transcript = [
    { role: "user", message: "Hi, I'm Call Assist by Google, recording this call." },
    { role: "assistant", message: "I'm calling about the listing." },
    { role: "user", message: "One sec. Checking with the person you called." },
    { role: "user", message: "Thanks. Can you tell me more about the details?" },
  ];
  const conversation = {
    status: "done",
    metadata: { termination_reason: "client disconnected" },
    analysis: { transcript_summary: "Google Call Assist screened the call." },
    transcript,
  };
  assert.equal(shouldTreatAsAgentUnavailable(conversation), true);
  assert.equal(shouldTreatAsAgentHungUp(conversation), false);
  const withHuman = { ...conversation, transcript: [...transcript, { role: "user", message: "This is Queeneth. What is this about?" }] };
  assert.equal(shouldTreatAsAgentUnavailable(withHuman), false);
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

test("post-call fallback accepts a lead's email-verified middle name on voicemail only", async () => {
  const { shouldTreatAsIdentityMismatchVoicemail } = await import("../src/lib/elevenLabsPostCall");
  const oliverGreeting = {
    status: "done",
    analysis: { transcript_summary: "Oliver's automated voicemail greeting prompted the caller to leave a message." },
    transcript: [
      { role: "user", message: "This is Oliver with AmeriFamily Realty. Please leave your name and phone number, and I will call you back." },
    ],
  };

  assert.equal(
    shouldTreatAsIdentityMismatchVoicemail(
      oliverGreeting,
      "Constantin",
      "Oliver Ene",
      "Constantin Oliver Ene",
      "oliver@ameropanrealty.com",
    ),
    false,
  );
  assert.equal(
    shouldTreatAsIdentityMismatchVoicemail(
      oliverGreeting,
      "Constantin",
      "Oliver Ene",
      "Constantin Oliver Ene",
      "office@ameropanrealty.com",
    ),
    true,
  );
  assert.equal(
    shouldTreatAsIdentityMismatchVoicemail(
      { status: "done", transcript: [{ role: "user", message: "This is Tina. Please leave a message." }] },
      "Constantin",
      "Oliver Ene",
      "Constantin Oliver Ene",
      "oliver@ameropanrealty.com",
    ),
    true,
  );
});

test("post-call fallback gives live office gatekeeper evidence precedence over identity-mismatch voicemail", async () => {
  const {
    buildVoiceResponseStatus,
    hasLiveHumanGatekeeperEvidence,
    shouldTreatAsAgentHungUp,
    shouldTreatAsAgentUnavailable,
    shouldTreatAsIdentityMismatchVoicemail,
  } = await import("../src/lib/elevenLabsPostCall");
  const conversation = {
    status: "done",
    metadata: { termination_reason: "Client disconnected: 1000" },
    transcript: [
      { role: "assistant", message: "This is Finn with Crisp Short Sales." },
      { role: "user", message: "Uh, Byrne Real Estate Group, this is Erica." },
      {
        role: "assistant",
        message: "I'm calling about Clay's short sale listing. Yoni's direct callback number is 404-300-9526.",
      },
      { role: "user", message: "No. Can I have that phone number again?" },
      { role: "assistant", message: "Sure, it's 404-300-9526." },
      { role: "user", message: "..." },
    ],
  };

  assert.equal(hasLiveHumanGatekeeperEvidence(conversation), true);
  assert.equal(shouldTreatAsIdentityMismatchVoicemail(conversation, "Clay", "Byrne"), false);
  assert.equal(shouldTreatAsAgentUnavailable(conversation), true);
  assert.equal(shouldTreatAsAgentHungUp(conversation), false);
  assert.equal(buildVoiceResponseStatus("agent_not_available"), "Agent was not available");
});

test("post-call fallback treats a different person answering for the target's realty team as unavailable", async () => {
  const { hasLiveHumanGatekeeperEvidence, shouldTreatAsAgentUnavailable } = await import(
    "../src/lib/elevenLabsPostCall"
  );
  const conversation = {
    status: "done",
    metadata: { termination_reason: "end_call tool was called." },
    transcript: [
      { role: "assistant", message: "Hi Christina, this is Maya with Crisp Short Sales." },
      { role: "user", message: "Good afternoon. This is Lalaina with The Welch Team." },
      { role: "assistant", message: "We help agents with short sale lender paperwork and follow-up." },
      { role: "user", message: "No, thank you." },
    ],
  };

  assert.equal(hasLiveHumanGatekeeperEvidence(conversation, "Christina"), true);
  assert.equal(shouldTreatAsAgentUnavailable(conversation, "Christina"), true);
});

test("post-call fallback does not treat the named target's own team introduction as a gatekeeper", async () => {
  const { hasLiveHumanGatekeeperEvidence } = await import("../src/lib/elevenLabsPostCall");
  const conversation = {
    status: "done",
    transcript: [
      { role: "assistant", message: "Hi Christina, this is Maya with Crisp Short Sales." },
      { role: "user", message: "Good afternoon. This is Christina with The Welch Team." },
    ],
  };

  assert.equal(hasLiveHumanGatekeeperEvidence(conversation, "Christina"), false);
});

test("post-call fallback accepts a different team responder who explicitly confirms listing authority", async () => {
  const { hasLiveHumanGatekeeperEvidence } = await import("../src/lib/elevenLabsPostCall");
  const conversation = {
    status: "done",
    transcript: [
      { role: "assistant", message: "Hi Christina, this is Maya with Crisp Short Sales." },
      { role: "user", message: "The Welch Team, this is Lalaina. I handle this short sale listing." },
    ],
  };

  assert.equal(hasLiveHumanGatekeeperEvidence(conversation, "Christina"), false);
});

test("post-call fallback treats information-request tooling as a handoff, not a callback or hangup", async () => {
  const { buildVoiceResponseStatus, shouldTreatAsAgentHungUp, shouldTreatAsCallback } = await import(
    "../src/lib/elevenLabsPostCall"
  );
  const conversation = {
    status: "done",
    metadata: { termination_reason: "end_call tool was called." },
    transcript: [
      { role: "assistant", message: "Would you rather have Yoni give you a quick call, or should I send over info?" },
      { role: "user", message: "Send me the information by email." },
      { role: "assistant", tool_calls: [{ tool_name: "information_requested" }] },
    ],
  };

  assert.equal(shouldTreatAsCallback(conversation), false);
  assert.equal(shouldTreatAsAgentHungUp(conversation), false);
  assert.equal(buildVoiceResponseStatus("information_requested"), "Information requested - handoff ready");
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

test("post-call fallback retains an earlier hold instruction when the final screener turn is truncated", async () => {
  const { shouldTreatAsAgentHungUp, shouldTreatAsAgentUnavailable } = await import(
    "../src/lib/elevenLabsPostCall"
  );
  const conversation = {
    status: "done",
    metadata: { termination_reason: "Client disconnected: 1000" },
    transcript: [
      { role: "assistant", message: "Hello, may I speak with Jacquelyn?" },
      { role: "user", message: "Let me see if the person is available." },
      { role: "assistant", message: "I'm calling about a short sale listing." },
      { role: "user", message: "Thanks. Please stay on the line." },
      { role: "assistant", message: "Sure, I'll wait." },
      { role: "user", message: "..." },
      { role: "user", message: "I'm sorry, this per-" },
    ],
  };

  assert.equal(shouldTreatAsAgentUnavailable(conversation), true);
  assert.equal(shouldTreatAsAgentHungUp(conversation), false);
});

test("post-call fallback preserves full voicemail after automated screening", async () => {
  const { shouldTreatAsAgentHungUp, shouldTreatAsAgentUnavailable } = await import(
    "../src/lib/elevenLabsPostCall"
  );
  const conversation = {
    status: "done",
    metadata: { termination_reason: "voicemail_detection tool was called." },
    analysis: {
      transcript_summary:
        "An automated call screening service asked Finn to record his name and reason, then routed the call to voicemail.",
    },
    transcript: [
      { role: "user", message: "Please record your name and reason for calling." },
      {
        role: "assistant",
        message: "This is Finn calling from Crisp Short Sales about your listing at 626 Trevor Street.",
      },
      { role: "user", message: "Please stay on the line while I try to reach them." },
      { role: "assistant", message: "Sure, I'll wait." },
      { role: "user", message: "The person you are calling is not available. Please leave a message after the tone." },
      {
        role: "assistant",
        message:
          "Hi, this is Finn with Crisp Short Sales calling about the short sale listing at 626 Trevor Street. We help agents with the short sale bank paperwork, lender calls, and approval process. Give him a call back at 404-300-9526.",
      },
    ],
  };

  assert.equal(shouldTreatAsAgentUnavailable(conversation), false);
  assert.equal(shouldTreatAsAgentHungUp(conversation), false);
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
  const naturalTarget = {
    status: "done",
    metadata: { termination_reason: "Client disconnected: 1000" },
    transcript: [
      { role: "assistant", message: "This is Finn with Crisp Short Sales." },
      { role: "user", message: "It's Lisa." },
      { role: "assistant", message: "Are you handling the bank side of the short sale yourself?" },
      { role: "user", message: "Yes." },
    ],
  };

  assert.equal(shouldTreatAsTargetReachedSelfHandlingDisconnect(confirmedTarget, "Valerie"), true);
  assert.equal(shouldTreatAsTargetReachedSelfHandlingDisconnect(naturalTarget, "Lisa"), true);
  assert.equal(
    shouldTreatAsTargetReachedSelfHandlingDisconnect(
      {
        ...naturalTarget,
        transcript: naturalTarget.transcript.map((item, index) =>
          index === 1 ? { ...item, message: "Lisa here." } : item,
        ),
      },
      "Lisa",
    ),
    true,
  );
  assert.equal(
    shouldTreatAsTargetReachedSelfHandlingDisconnect(
      {
        ...naturalTarget,
        transcript: naturalTarget.transcript.map((item, index) =>
          index === 1 ? { ...item, message: "It's Maria." } : item,
        ),
      },
      "Lisa",
    ),
    false,
  );
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

test("post-call fallback distinguishes self-initiated future contact from callback and rejection", async () => {
  const {
    buildVoiceResponseStatus,
    shouldTreatAsCallback,
    shouldTreatAsDeferredContact,
  } = await import("../src/lib/elevenLabsPostCall");
  const eugeneConversation = {
    status: "done",
    analysis: { transcript_summary: "The agent said he would get back to Crisp later." },
    transcript: [
      { role: "assistant", message: "Would you like help with the short sale?" },
      { role: "user", message: "I'm gonna get back to you as soon as I can." },
      { role: "assistant", tool_calls: [{ tool_name: "not_interested" }] },
    ],
  };
  const requestedCallback = {
    status: "done",
    transcript: [{ role: "user", message: "Have Yoni call me later." }],
  };
  const hardNo = {
    status: "done",
    transcript: [{ role: "user", message: "I'm not interested, but I'll call you if that changes." }],
  };

  assert.equal(shouldTreatAsDeferredContact(eugeneConversation), true);
  assert.equal(shouldTreatAsCallback(eugeneConversation), false);
  assert.equal(buildVoiceResponseStatus("deferred_contact"), "Will follow up when ready");
  assert.equal(shouldTreatAsDeferredContact(requestedCallback), false);
  assert.equal(shouldTreatAsCallback(requestedCallback), true);
  assert.equal(shouldTreatAsDeferredContact(hardNo), false);
});

test("post-call fallback gives Cinthia and Summer voicemail evidence precedence over automated callback wording", async () => {
  const { shouldTreatAsDeferredContact } = await import("../src/lib/elevenLabsPostCall");

  assert.equal(shouldTreatAsDeferredContact(cinthiaFullMailboxConversation), false);
  assert.equal(shouldTreatAsDeferredContact(summerVoicemailConversation), false);
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

test("a live answer from an unrelated business is unavailable, not an agent hangup", async () => {
  const { shouldTreatAsUnrelatedLiveBusiness, shouldTreatAsAgentHungUp } = await import("../src/lib/elevenLabsPostCall");
  const conversation = {
    status: "done",
    metadata: { termination_reason: "client disconnected" },
    transcript: [
      { role: "assistant", message: "Hi, is this Michael?" },
      { role: "user", message: "AWU Thermostats, this is Kenzie." },
    ],
  };
  assert.equal(shouldTreatAsUnrelatedLiveBusiness(conversation, "Michael"), true);
  assert.equal(shouldTreatAsAgentHungUp(conversation), false);
});

for (const scenario of [
  {
    name: "paola_completed_self_handling_decline",
    expectedCallResult: "answered_not_interested",
    expectedResponseStatus: "Not interested",
    terminationReason: "Client disconnected: 1000",
    summary: "The agent confirmed she is handling the short-sale work herself and declined assistance at this time.",
    transcript: [
      { role: "user", message: "Good afternoon. Can I help you?" },
      { role: "assistant", message: "Is it okay if I ask one quick question about the short-sale paperwork?" },
      { role: "user", message: "Yes." },
      { role: "assistant", message: "Are you handling the short-sale paperwork and lender calls yourself?" },
      { role: "user", message: "Yes." },
      { role: "assistant", message: "Is any part of the lender follow-up something you'd like help with?" },
      { role: "user", message: "Not right now." },
      { role: "assistant", message: "No problem. If you ever need help, just let us know." },
      { role: "user", message: "Okay." },
    ],
  },
  {
    name: "parul_live_answer_before_disconnect",
    expectedCallResult: "call_received_agent_hung_up",
    expectedResponseStatus: "Call received but agent hung up on Maya",
    terminationReason: "Client disconnected: 1000",
    summary: "A person answered hello and heard Maya begin the listing-specific reason for calling.",
    transcript: [
      { role: "user", message: "..." },
      { role: "user", message: "Hello?" },
      { role: "assistant", message: "Hi, this is Maya with Crisp Short Sales. I was calling about the listing at..." },
    ],
  },
  {
    name: "feng_completed_role_question_decline",
    expectedCallResult: "answered_not_interested",
    expectedResponseStatus: "Not interested",
    terminationReason: "Ending conversation after 45 seconds of silence.",
    summary: "The user asked whether Crisp was part of the bank, declined further details, and concluded politely.",
    transcript: [
      { role: "user", message: "Hello?" },
      { role: "assistant", message: "Is it okay if I ask one quick question about that?" },
      { role: "user", message: "Yes." },
      { role: "assistant", message: "Has the seller completed all of the required short-sale paperwork?" },
      { role: "user", message: "I believe everything is good as of now." },
      { role: "assistant", message: "Is there anything else I can assist you with regarding this listing?" },
      { role: "user", message: "Wait, so you are part of the bank?" },
      { role: "assistant", message: "No. Crisp supports agents. Would you like more details about our role?" },
      { role: "user", message: "Yeah." },
      { role: "assistant", message: "Do you have any specific questions about our services?" },
      { role: "user", message: "..." },
      { role: "user", message: "Uh, no." },
      { role: "assistant", message: "Alright, thank you for your time. Have a great day!" },
      { role: "user", message: "Sounds good. Thank you." },
    ],
  },
]) {
  test(`post-call fallback preserves the live human outcome for ${scenario.name}`, async () => {
    const { processPostCallOutcomeFromConversationId } = await import("../src/lib/elevenLabsPostCall");
    const conversationId = `conv_measurement_${scenario.name}`;
    const writesBefore = measurementWrites.length;
    measurementConversations.set(conversationId, {
      conversation_id: conversationId,
      status: "done",
      metadata: { termination_reason: scenario.terminationReason, call_duration_secs: 47 },
      analysis: { transcript_summary: scenario.summary },
      conversation_initiation_client_data: {
        dynamic_variables: {
          rowNumber: 123,
          callAttemptNumber: 1,
          agentName: "Synthetic Agent",
          listingAddress: "123 Fictional Street",
          requestedPhone: "+12025550123",
          phone: "+12025550123",
          assistantName: "Maya",
          testMode: true,
        },
      },
      transcript: scenario.transcript,
    });
    try {
      assert.equal(await processPostCallOutcomeFromConversationId(conversationId), true);
      const update = measurementWrites.slice(writesBefore).find((write) => typeof write.voiceNotes === "string")!;
      assert.equal(update.callResult, scenario.expectedCallResult);
      assert.equal(update.responseStatus, scenario.expectedResponseStatus);
    } finally {
      measurementConversations.delete(conversationId);
    }
  });
}

for (const scenario of [
  { name: "new", declarations: { initialOpeningPolicy: "listen_first_uniform_v1", declaredConversationPolicyVersion: "captured-code-version" },
    expectedOpening: "listen_first_uniform_v1", expectedPolicy: "captured-code-version" },
  { name: "historical", declarations: {}, expectedOpening: null, expectedPolicy: null },
  { name: "malformed", declarations: { initialOpeningPolicy: 7, declaredConversationPolicyVersion: "   " },
    expectedOpening: null, expectedPolicy: null },
  { name: "partial", declarations: { initialOpeningPolicy: "  older-opening-policy  " },
    expectedOpening: "older-opening-policy", expectedPolicy: null },
]) {
  test(`post-call metadata reconstruction keeps ${scenario.name} declarations separate from final provider identity`, async () => {
    const { processPostCallOutcomeFromConversationId } = await import("../src/lib/elevenLabsPostCall");
    const { VOICE_PERFORMANCE_LOG_MARKER } = await import("../src/lib/elevenLabsPerformanceLog");
    const conversationId = `conv_measurement_${scenario.name}`;
    const conversation = {
      conversation_id: conversationId, agent_id: "agent_receipt", version_id: "agtvrsn_receipt", branch_id: "agtbrch_receipt",
      status: "done", metadata: { call_duration_secs: 5 },
      conversation_initiation_client_data: { dynamic_variables: {
        rowNumber: "123", callAttemptNumber: "1", agentName: "Synthetic Caller", listingAddress: "123 Fictional Street",
        requestedPhone: "+12025550123", phone: "+12025550123", testMode: true,
        assistantName: "Finn", voiceVariant: "finch", openerVariant: "benefit_hook",
        openerVariantLabel: "Permission-first help check", openerScript: "Assigned continuation only",
        scheduledWindow: "late_morning", agentTimeZone: "America/New_York",
        agent_id: "agent_dynamic_not_receipt", version_id: "agtvrsn_dynamic_not_receipt", branch_id: "agtbrch_dynamic_not_receipt",
        ...scenario.declarations,
      } },
      transcript: [{ role: "user", message: "Please end this call.", time_in_call_secs: 1 }],
    };
    const original = structuredClone(conversation), writesBefore = measurementWrites.length;
    measurementConversations.set(conversationId, conversation);
    try {
      assert.equal(await processPostCallOutcomeFromConversationId(conversationId), true);
      const writes = measurementWrites.slice(writesBefore);
      assert.equal(writes.length, 2);
      const update = writes.find(write => typeof write.voiceNotes === "string")!;
      assert.equal(update.callResult, "call_ended_by_request");
      assert.equal(writes.filter(write => write.action === "process_voice_queue").length, 1);
      const metrics = JSON.parse(String(update.voiceNotes).replace(`--- ${VOICE_PERFORMANCE_LOG_MARKER} ---\n`, ""));
      assert.equal(metrics.call.initialOpeningPolicy, scenario.expectedOpening);
      assert.equal(metrics.call.declaredConversationPolicyVersion, scenario.expectedPolicy);
      assert.equal(metrics.call.openerVariant, "benefit_hook");
      assert.equal(metrics.call.openerScript, "Assigned continuation only");
      assert.equal(metrics.call.voiceVariant, "finch");
      assert.equal(metrics.call.scheduledWindow, "late_morning");
      assert.equal(metrics.call.agentTimeZone, "America/New_York");
      assert.deepEqual(metrics.providerIdentity, {
        source: "final_conversation_receipt", agentId: "agent_receipt", versionId: "agtvrsn_receipt", branchId: "agtbrch_receipt",
      });
      assert.equal(metrics.flags.openingQuestionDelivered, false);
      assert.deepEqual(conversation, original);
    } finally {
      measurementConversations.delete(conversationId);
    }
  });
}

test("post-call reconstruction does not manufacture historical provider versions from dynamic metadata", async () => {
  const { processPostCallOutcomeFromConversationId } = await import("../src/lib/elevenLabsPostCall");
  const { VOICE_PERFORMANCE_LOG_MARKER } = await import("../src/lib/elevenLabsPerformanceLog");
  const conversationId = "conv_measurement_legacy_receipt", writesBefore = measurementWrites.length;
  measurementConversations.set(conversationId, {
    conversation_id: conversationId, agent_id: "agent_legacy_receipt", status: "done",
    conversation_initiation_client_data: { dynamic_variables: {
      rowNumber: 123, callAttemptNumber: 1, agentName: "Synthetic Caller", listingAddress: "123 Fictional Street",
      phone: "+12025550123", version_id: "agtvrsn_dynamic_only", branch_id: "agtbrch_dynamic_only",
    } },
    transcript: [{ role: "user", message: "Please end this call." }],
  });
  try {
    assert.equal(await processPostCallOutcomeFromConversationId(conversationId), true);
    const update = measurementWrites.slice(writesBefore).find(write => typeof write.voiceNotes === "string")!;
    const metrics = JSON.parse(String(update.voiceNotes).replace(`--- ${VOICE_PERFORMANCE_LOG_MARKER} ---\n`, ""));
    assert.equal(metrics.call.initialOpeningPolicy, null);
    assert.equal(metrics.call.declaredConversationPolicyVersion, null);
    assert.deepEqual(metrics.providerIdentity, {
      source: "final_conversation_receipt", agentId: "agent_legacy_receipt", versionId: null, branchId: null,
    });
  } finally {
    measurementConversations.delete(conversationId);
  }
});
