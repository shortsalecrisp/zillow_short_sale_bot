import assert from "node:assert/strict";
import test from "node:test";
import type { CallMetadata } from "../src/types";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";

test("voice performance log stores codex-readable cohort metrics in one cell block", async () => {
  const { buildVoicePerformanceLog, VOICE_PERFORMANCE_LOG_MARKER } = await import(
    "../src/lib/elevenLabsPerformanceLog"
  );

  const log = buildVoicePerformanceLog({
    conversationId: "conv_test",
    outcome: "Call received but agent hung up on Maya",
    summary: "The agent asked if Maya was a chatbot and hung up early.",
    transcript:
      "Maya: Hi, this is Maya with Crisp Short Sales. I'm calling about your short sale listing.\nAgent: Hello.\nMaya: Are you handling the short sale paperwork and lender calls yourself?\nAgent: Are you a chatbot?",
    metadata: {
      rowNumber: 3481,
      fullName: "Chris Agent",
      callAttemptNumber: 1,
      listingAddress: "123 Main St, Tampa, FL",
      requestedPhone: "+18135550123",
      dialedPhone: "+18135550123",
      testMode: false,
      voiceVariant: "eryn",
      voiceName: "Eryn",
      assistantName: "Maya",
      voiceId: "voice_eryn",
      openerVariant: "direct_reason",
      openerVariantLabel: "Permission-first handling check",
      openerScript: "I was calling about the short-sale paperwork and lender calls. Is it okay if I ask one quick question about that?",
      initialOpeningPolicy: "listen_first_uniform_v1",
      declaredConversationPolicyVersion: "declared-test-policy",
      scheduledWindow: "late_morning",
      agentTimeZone: "America/New_York",
    },
    conversation: {
      conversation_id: "conv_test",
      agent_id: "agent_actualtest",
      version_id: "agtvrsn_actualtest",
      branch_id: "agtbrch_actualtest",
      status: "done",
      metadata: {
        termination_reason: "Client disconnected: 1000",
        call_duration_secs: 18,
      },
      transcript: [
        {
          role: "agent",
          message: "Hi, this is Maya with Crisp Short Sales. I'm calling about your short sale listing.",
          time_in_call_secs: 0.7,
        },
        { role: "user", message: "Hello.", time_in_call_secs: 3 },
        {
          role: "agent",
          message: "Are you handling the short sale paperwork and lender calls yourself?",
          time_in_call_secs: 4,
        },
        { role: "user", message: "Are you a chatbot?", time_in_call_secs: 12 },
      ],
    },
  });

  assert.match(log, new RegExp(`^--- ${VOICE_PERFORMANCE_LOG_MARKER} ---\\n`));

  const parsed = JSON.parse(log.replace(`--- ${VOICE_PERFORMANCE_LOG_MARKER} ---\n`, ""));
  assert.equal(parsed.schema, "voice_call_metrics_v1");
  assert.equal(parsed.abTestScope.cohort, "time_bucket_and_voice_rotation");
  assert.deepEqual(parsed.abTestScope.includeOnlyVoiceVariants, ["eryn", "finch"]);
  assert.equal(parsed.abTestScope.excludePriorSingleVoiceEmmyCalls, true);
  assert.match(parsed.abTestScope.analysisRule, /Exclude all previous single-voice Emmy calls/i);
  assert.equal(parsed.proveItCohort.startedAt, "2026-09-03T14:20:21Z");
  assert.equal(parsed.proveItCohort.baselineConversationCount, 1063);
  assert.equal(parsed.proveItCohort.targetAdditionalCallsMin, 300);
  assert.equal(parsed.proveItCohort.targetAdditionalCallsMax, 400);
  assert.equal(parsed.ttsModelExperiment.startedAt, "2026-09-28T22:30:50.974Z");
  assert.equal(parsed.ttsModelExperiment.commonTurnModel, "turn_v3");
  assert.deepEqual(
    Object.values(parsed.ttsModelExperiment.arms).map((arm: any) => arm.ttsModel),
    ["eleven_flash_v2", "eleven_v4_turbo"],
  );
  assert.equal(parsed.call.voiceVariant, "eryn");
  assert.equal(parsed.call.assistantName, "Maya");
  assert.equal(parsed.call.openerVariant, "direct_reason");
  assert.equal(parsed.call.openerVariantLabel, "Permission-first handling check");
  assert.match(parsed.call.openerScript, /short-sale paperwork and lender calls/);
  assert.equal(parsed.call.initialOpeningPolicy, "listen_first_uniform_v1");
  assert.equal(parsed.call.declaredConversationPolicyVersion, "declared-test-policy");
  assert.deepEqual(parsed.providerIdentity, {
    source: "final_conversation_receipt",
    agentId: "agent_actualtest",
    versionId: "agtvrsn_actualtest",
    branchId: "agtbrch_actualtest",
  });
  assert.equal(parsed.call.scheduledWindow, "late_morning");
  assert.equal(parsed.call.agentTimeZone, "America/New_York");
  assert.equal(parsed.metrics.durationSecs, 18);
  assert.equal(parsed.metrics.agentTurns, 2);
  assert.equal(parsed.metrics.assistantTurns, 2);
  assert.equal(parsed.metrics.reasonMentionedAtSecs, 0.7);
  assert.equal(parsed.metrics.openingQuestionAtSecs, 4);
  assert.equal(parsed.metrics.identityStatementCount, 1);
  assert.equal(parsed.flags.aiSuspicion, true);
  assert.equal(parsed.flags.reasonDelivered, true);
  assert.equal(parsed.flags.openingQuestionDelivered, true);
  assert.equal(parsed.flags.agentRespondedAfterReason, true);
  assert.equal(parsed.flags.agentRespondedAfterOpeningQuestion, true);
  assert.equal(parsed.flags.hangupBeforeReason, false);
  assert.equal(parsed.flags.hangupBeforeOpeningQuestion, false);
  assert.equal(parsed.flags.repeatedIdentityStatement, false);
  assert.equal(parsed.flags.liveYoniNowOfferDelivered, false);
  assert.match(parsed.codexInstructions, /Compare voiceVariant/i);
  assert.match(parsed.codexInstructions, /scheduledWindow by agent local time bucket/i);
  assert.match(parsed.codexInstructions, /openerVariant/i);
  assert.match(parsed.codexInstructions, /hangupBeforeReason/i);
  assert.match(parsed.codexInstructions, /Current calls under eryn-self-handler-ai-optout-20260925 should show Eryn\/Maya only/i);
  assert.match(parsed.codexInstructions, /previous single-voice Emmy calls/i);
  assert.match(parsed.codexInstructions, /Pro prove-it cohort/i);
  assert.match(parsed.codexInstructions, /transcript\/playback-verified handoff-ready leads/i);
  assert.match(parsed.codexInstructions, /First stratify by call.initialOpeningPolicy, call.declaredConversationPolicyVersion/);
  assert.match(parsed.codexInstructions, /providerIdentity.agentId, versionId and branchId/);
  assert.match(parsed.codexInstructions, /post-intro continuation assignment, not proof of delivery/);
  assert.match(parsed.codexInstructions, /initial introduction is uniform and permission-first/);
  assert.match(parsed.codexInstructions, /Before that policy, row-parity assignments paired Eryn\/direct_reason and Finch\/benefit_hook/);
  assert.match(parsed.codexInstructions, /Starting with permission-screener-20260918, voiceVariant and openerVariant rotated independently/);
  assert.match(parsed.codexInstructions, /Starting with eryn-self-handler-ai-optout-20260925, voice is owner-fixed to Eryn\/Maya/);
  assert.match(parsed.codexInstructions, /Starting with maya-short-opener-ai-closeout-20261002 and listen_first_listing_agent_v2/);
  assert.match(parsed.codexInstructions, /do not pool this stratum with the earlier permission-first opening/);
  assert.match(parsed.codexInstructions, /main branch is Flash v2 control and the experiment branch is v4 Turbo/);
  assert.match(parsed.abTestScope.analysisRule, /Missing or null historical values are unknown/);
  assert.match(parsed.abTestScope.analysisRule, /not proof of audible delivery/);
  assert.match(parsed.transcript, /Are you a chatbot/);
});

async function measurementLog(input: { metadata?: Partial<CallMetadata>; conversation?: Record<string, unknown>;
  callbackConsent?: { callbackTime: string } | null } = {}) {
  const { buildVoicePerformanceLog, VOICE_PERFORMANCE_LOG_MARKER } = await import("../src/lib/elevenLabsPerformanceLog");
  return JSON.parse(buildVoicePerformanceLog({
    conversationId: "conv_measurement",
    outcome: "Synthetic measurement only",
    summary: "No delivered continuation is asserted.",
    transcript: "",
    callbackConsent: input.callbackConsent,
    metadata: {
      rowNumber: 123, fullName: "Synthetic Caller", callAttemptNumber: 1,
      listingAddress: "123 Fictional Street", requestedPhone: "+12025550123", dialedPhone: "+12025550123",
      testMode: true, ...input.metadata,
    },
    conversation: { conversation_id: "conv_measurement", status: "done", ...input.conversation },
  }).replace(`--- ${VOICE_PERFORMANCE_LOG_MARKER} ---\n`, ""));
}

for (const status of ["done", "failed"]) {
  test(`provider identity comes from a matching final ${status} receipt, separately from declarations`, async () => {
    const parsed = await measurementLog({
      metadata: { initialOpeningPolicy: "historical-opening-policy", declaredConversationPolicyVersion: "declared-code-label" },
      conversation: {
        status, agent_id: "agent_receipt", version_id: "agtvrsn_receipt", branch_id: "agtbrch_receipt",
        conversation_initiation_client_data: { dynamic_variables: {
          agent_id: "agent_not_the_receipt", version_id: "agtvrsn_not_the_receipt", branch_id: "agtbrch_not_the_receipt",
        } },
      },
    });
    assert.equal(parsed.call.declaredConversationPolicyVersion, "declared-code-label");
    assert.equal(parsed.call.initialOpeningPolicy, "historical-opening-policy");
    assert.deepEqual(parsed.providerIdentity, {
      source: "final_conversation_receipt", agentId: "agent_receipt", versionId: "agtvrsn_receipt", branchId: "agtbrch_receipt",
    });
  });
}

test("provider identity stays unknown for an unfinished, missing or mismatched final receipt", async () => {
  for (const receipt of [
    { status: "processing" }, { status: undefined }, { conversation_id: undefined }, { conversation_id: "conv_other" },
  ]) {
    const parsed = await measurementLog({ conversation: {
      agent_id: "agent_notfinal", version_id: "agtvrsn_notfinal", branch_id: "agtbrch_notfinal", ...receipt,
    } });
    assert.deepEqual(parsed.providerIdentity, { source: null, agentId: null, versionId: null, branchId: null });
  }
});

test("historical missing declarations and provider versions remain unknown without current-policy backfill", async () => {
  const parsed = await measurementLog({ conversation: { agent_id: "agent_legacy" } });
  assert.equal(parsed.call.initialOpeningPolicy, null);
  assert.equal(parsed.call.declaredConversationPolicyVersion, null);
  assert.deepEqual(parsed.providerIdentity, {
    source: "final_conversation_receipt", agentId: "agent_legacy", versionId: null, branchId: null,
  });
  for (const absent of [undefined, null, "", "   ", 7, {}, []]) {
    const result = await measurementLog({ conversation: { agent_id: absent, version_id: absent, branch_id: absent } });
    assert.equal(result.providerIdentity.agentId, null);
    assert.equal(result.providerIdentity.versionId, null);
    assert.equal(result.providerIdentity.branchId, null);
  }
  const blank = await measurementLog({ metadata: { initialOpeningPolicy: " ", declaredConversationPolicyVersion: "" } });
  assert.equal(blank.call.initialOpeningPolicy, null);
  assert.equal(blank.call.declaredConversationPolicyVersion, null);
});

test("post-intro assignment alone does not establish a delivered continuation", async () => {
  for (const openerVariant of ["direct_reason", "benefit_hook"]) {
    const parsed = await measurementLog({
      metadata: { openerVariant, initialOpeningPolicy: "listen_first_uniform_v1" },
      conversation: { transcript: [
        { role: "user", message: "Hello.", time_in_call_secs: 0 },
        { role: "agent", message: "Hi, this is Maya with Crisp Short Sales. I'm calling about your short sale listing.", time_in_call_secs: 1 },
      ] },
    });
    assert.equal(parsed.call.openerVariant, openerVariant);
    assert.equal(parsed.call.initialOpeningPolicy, "listen_first_uniform_v1");
    assert.equal(parsed.flags.reasonDelivered, true);
    assert.equal(parsed.flags.openingQuestionDelivered, false);
    assert.equal(parsed.metrics.openingQuestionAtSecs, null);
  }
});

test("listing-agent opening keeps identity response separate from the service reason", async () => {
  const parsed = await measurementLog({
    metadata: {
      initialOpeningPolicy: "listen_first_listing_agent_v2",
      declaredConversationPolicyVersion: "maya-short-opener-ai-closeout-20261002",
    },
    conversation: { transcript: [
      { role: "user", message: "Hello.", time_in_call_secs: 0 },
      { role: "agent", message: "Hi, this is Maya with Crisp Short Sales. Are you the listing agent for the short sale at 123 Main Street?", time_in_call_secs: 1 },
      { role: "user", message: "Yes.", time_in_call_secs: 4 },
      { role: "agent", message: "We help with lender paperwork and calls. Are you handling those yourself?", time_in_call_secs: 5 },
      { role: "user", message: "I am.", time_in_call_secs: 8 },
    ] },
  });
  assert.equal(parsed.flags.openingQuestionDelivered, true);
  assert.equal(parsed.flags.agentRespondedAfterOpeningQuestion, true);
  assert.equal(parsed.metrics.openingQuestionAtSecs, 1);
  assert.equal(parsed.flags.reasonDelivered, true);
  assert.equal(parsed.flags.agentRespondedAfterReason, true);
  assert.equal(parsed.metrics.reasonMentionedAtSecs, 5);
});

test("listing-agent opening does not count the address check as the service reason", async () => {
  const parsed = await measurementLog({
    metadata: { initialOpeningPolicy: "listen_first_listing_agent_v2" },
    conversation: { metadata: { call_duration_secs: 10, termination_reason: "Client disconnected: 1000" }, transcript: [
      { role: "user", message: "Hello.", time_in_call_secs: 0 },
      { role: "agent", message: "Hi, this is Maya with Crisp Short Sales. Are you the listing agent for the short sale at 123 Main Street?", time_in_call_secs: 1 },
      { role: "user", message: "This is she.", time_in_call_secs: 4 },
    ] },
  });
  assert.equal(parsed.flags.openingQuestionDelivered, true);
  assert.equal(parsed.flags.agentRespondedAfterOpeningQuestion, true);
  assert.equal(parsed.flags.reasonDelivered, false);
  assert.equal(parsed.flags.hangupBeforeReason, true);
});

test("voice performance log does not count confused live-transfer tool fire as clear consent", async () => {
  const { buildVoicePerformanceLog } = await import("../src/lib/elevenLabsPerformanceLog");

  const log = buildVoicePerformanceLog({
    conversationId: "conv_3701ktskd6ehedgrg0n56rptbx38",
    outcome: "Requested callback later",
    summary: "The caller was in a meeting and said she would call back later or tomorrow.",
    transcript:
      "Maya: Should I see if Yoni can hop on for sixty seconds and explain it?\nAgent: I, so... Okay. Okay.\nMaya: Ok, hold on, let me see if he's available one second.\nAgent: Right now, I am on the meeting. On the afternoon or tomorrow, I call you back.",
    metadata: {
      rowNumber: 3651,
      fullName: "Norma Lagonell",
      callAttemptNumber: 1,
      listingAddress: "7733 Stone Creek Trl, Kissimmee, FL",
      requestedPhone: "+19545441676",
      dialedPhone: "+19545441676",
      testMode: false,
      voiceVariant: "eryn",
      voiceName: "Eryn",
      assistantName: "Maya",
      voiceId: "dMyQqiVXTU80dDl2eNK8",
    },
    conversation: {
      status: "done",
      metadata: {
        termination_reason: "Client disconnected: 1000",
        call_duration_secs: 72,
      },
      transcript: [
        {
          role: "agent",
          message:
            "Got it. We can take the lender paperwork and bank calls off your plate at no cost to you or the seller. Should I see if Yoni, our short sale specialist, can hop on for sixty seconds and explain it?",
          time_in_call_secs: 25,
        },
        { role: "user", message: "I, so... Okay. Okay.", time_in_call_secs: 26 },
        {
          role: "agent",
          message: "Ok, hold on, let me see if he's available one second.",
          time_in_call_secs: 38,
        },
        { role: "agent", tool_calls: [{ tool_name: "live_transfer_requested" }], time_in_call_secs: 38 },
        {
          role: "user",
          message:
            "Right now, I am on the meeting. On the afternoon or tomorrow, I call you back. Okay.",
          time_in_call_secs: 37,
        },
      ],
    },
  });

  const parsed = JSON.parse(log.replace(`--- CODEX_VOICE_CALL_METRICS_V1 ---\n`, ""));
  assert.equal(parsed.flags.liveTransferToolFired, true);
  assert.equal(parsed.flags.liveTransferRequested, false);
  assert.equal(parsed.flags.clearLiveTransferConsent, false);
  assert.equal(parsed.flags.misfiredLiveTransferRequest, true);
  assert.equal(parsed.flags.callbackOrLaterSignal, true);
  assert.equal(parsed.flags.transferCompleted, false);
  assert.match(parsed.codexInstructions, /clearLiveTransferConsent/);
});

test("voice performance log keeps transfer consent separate from later callback fallback", async () => {
  const { buildVoicePerformanceLog } = await import("../src/lib/elevenLabsPerformanceLog");

  const transcript = [
    { role: "agent", message: "Want me to try to get Yoni on the phone now?", time_in_call_secs: 20 },
    { role: "user", message: "Yes, go ahead.", time_in_call_secs: 21 },
    { role: "agent", tool_calls: [{ tool_name: "live_transfer_requested" }], time_in_call_secs: 22 },
    { role: "agent", message: "He was not available. Should I have him call you back ASAP?", time_in_call_secs: 35 },
    { role: "user", message: "Yes, please.", time_in_call_secs: 36 },
    { role: "agent", tool_calls: [{ tool_name: "callback_requested" }], time_in_call_secs: 37 },
  ];
  const log = buildVoicePerformanceLog({
    conversationId: "conv_transfer_then_callback",
    callbackConsent: { callbackTime: "asap" },
    outcome: "Requested callback ASAP",
    summary: "The caller agreed to a live transfer; it did not complete, so an ASAP callback was arranged.",
    transcript: "Agent explicitly consented to a live transfer and then accepted a callback fallback.",
    metadata: {
      rowNumber: 4001,
      fullName: "Aira Agent",
      callAttemptNumber: 1,
      listingAddress: "1 Main St",
      requestedPhone: "+14045550123",
      dialedPhone: "+14045550123",
      testMode: false,
    },
    conversation: {
      status: "done",
      metadata: { call_duration_secs: 42 },
      transcript,
    },
  });

  const parsed = JSON.parse(log.replace(`--- CODEX_VOICE_CALL_METRICS_V1 ---\n`, ""));
  assert.equal(parsed.flags.liveTransferToolFired, true);
  assert.equal(parsed.flags.liveTransferRequested, true);
  assert.equal(parsed.flags.clearLiveTransferConsent, true);
  assert.equal(parsed.flags.callbackRequested, true);
  assert.equal(parsed.flags.transferCompleted, false);
  assert.equal(parsed.flags.misfiredLiveTransferRequest, false);
  assert.equal(parsed.flags.liveYoniNowOfferDelivered, true);
  assert.equal(parsed.flags.agentRespondedAfterLiveYoniNowOffer, true);
  assert.equal(parsed.metrics.liveYoniNowOfferAtSecs, 20);
});

test("voice performance log excludes punctuation-only silence from engagement", async () => {
  const { buildVoicePerformanceLog } = await import("../src/lib/elevenLabsPerformanceLog");
  const log = buildVoicePerformanceLog({
    conversationId: "conv_punctuation_silence",
    outcome: "Agent was not available",
    summary: "Only placeholder silence followed the opening question.",
    transcript: "Maya: Are you handling the bank side yourself?\nAgent: ...",
    metadata: {
      rowNumber: 5391,
      fullName: "Phillis Nealy",
      callAttemptNumber: 1,
      listingAddress: "1 Main St",
      requestedPhone: "+14045550123",
      dialedPhone: "+14045550123",
      testMode: false,
    },
    conversation: {
      status: "done",
      metadata: { call_duration_secs: 21, termination_reason: "Client disconnected: 1000" },
      transcript: [
        { role: "agent", message: "Are you handling the bank side yourself?", time_in_call_secs: 4 },
        { role: "user", message: "...", time_in_call_secs: 8 },
        { role: "user", message: "?!", time_in_call_secs: 10 },
      ],
    },
  });

  const parsed = JSON.parse(log.replace(`--- CODEX_VOICE_CALL_METRICS_V1 ---\n`, ""));
  assert.equal(parsed.metrics.agentTurns, 0);
  assert.equal(parsed.metrics.agentWords, 0);
  assert.equal(parsed.flags.liveAnswered, false);
  assert.equal(parsed.flags.agentRespondedAfterOpeningQuestion, false);
});

for (const recording of [
  "Your call has been forwarded to voicemail. Please leave a message after the tone.",
  "Hello, you've reached Morgan. Leave your name and number and I will call back.",
  "The mailbox is full and cannot accept messages.",
  "The voicemail box has not been set up.",
  "The voice mail box is not initialized.",
  "Hello, this is Morgan at Example Realty. Please leave your name, phone number, and I'll call back.",
  "Hi. Sorry I missed your call. If you'll leave me your name and your number, I'll call back.",
  "Leave a brief message with your name, phone number, and how I can help, and I'll return your call.",
  "Your voicemail is being transcribed by YouMail.",
  "Please record your message. When you have finished recording, you may hang up.",
  "Hi, this is Kevin. I'm away from my phone right now; please leave a message or send me a text.",
]) {
  test(`recorded endpoint is not human contact: ${recording}`, async () => {
    const result = await measurementLog({ conversation: {
      metadata: { call_duration_secs: 15, termination_reason: "Client disconnected: 1000" },
      transcript: [
        { role: "user", message: recording, time_in_call_secs: 0 },
        { role: "agent", message: "I'm calling about your short sale listing. Call back when you can.", time_in_call_secs: 8 },
        { role: "user", message: "Thank you. Goodbye.", time_in_call_secs: 12 },
      ],
    } });
    assert.equal(result.measurementRevision, "handoff-evidence-v3");
    assert.equal(result.rawSignals.hasMeaningfulUserTranscript, true);
    assert.equal(result.rawSignals.userTurns, 2);
    assert.equal(result.contactEvidence.category, "voicemail");
    assert.equal(result.flags.liveAnswered, false);
    assert.equal(result.flags.humanAnswered, false);
    assert.equal(result.flags.targetAgentAnswered, false);
    assert.equal(result.metrics.agentTurns, 0);
    assert.equal(result.flags.reasonDelivered, false);
    assert.equal(result.flags.agentRespondedAfterReason, false);
    assert.equal(result.flags.callbackOrLaterSignal, false);
    assert.equal(result.flags.earlyHangupUnder20Secs, false);
    assert.equal(result.flags.hangupBeforeReason, false);
    assert.equal(result.metrics.avgAgentToAssistantDelaySecs, null);
  });
}

test("greeting split across a voicemail recording cannot create human or target identity evidence", async () => {
  const result = await measurementLog({ conversation: { transcript: [
    { role: "user", message: "Hello. This is Synthetic Caller." },
    { role: "user", message: "I can't get to the phone. Please leave a message." },
  ] } });
  assert.equal(result.contactEvidence.category, "voicemail");
  assert.equal(result.flags.humanAnswered, false);
  assert.equal(result.flags.targetAgentAnswered, false);
});

test("screening then voicemail and canned thanks never establish engagement", async () => {
  const result = await measurementLog({ conversation: { transcript: [
    { role: "user", message: "Record your name and reason for calling. I'll see if this person is available." },
    { role: "agent", message: "Maya with Crisp Short Sales about the short sale listing." },
    { role: "user", message: "Thanks. Please stay on the line." },
    { role: "user", message: "Please leave a message after the beep." },
  ] } });
  assert.deepEqual(result.contactEvidence.automatedStages, ["screening", "voicemail"]);
  assert.equal(result.flags.humanAnswered, false);
  assert.equal(result.flags.agentRespondedAfterReason, false);
  assert.equal(result.metrics.agentTurns, 0);
});

test("Google screening detail questions remain automation until a human pickup", async () => {
  const result = await measurementLog({ conversation: { transcript: [
    { role: "user", message: "This is Call Assist by Google. May I ask who's calling?" },
    { role: "agent", message: "Maya with Crisp Short Sales about the short sale listing." },
    { role: "user", message: "Can you tell me more about the details?" },
    { role: "agent", message: "We help with lender paperwork and calls." },
    { role: "user", message: "I'm checking with the person you called." },
    { role: "user", message: "Thank you." },
  ] } });
  assert.equal(result.contactEvidence.category, "screening");
  assert.equal(result.flags.humanAnswered, false);
  assert.equal(result.flags.agentRespondedAfterReason, false);
});

test("clipped screen announcements and mailbox digits do not become human responses", async () => {
  for (const tail of ["Thanks, Maya. Please stay on the", "Thanks, Maya. Please-", "I'm sorry, this per-"]) {
    const result = await measurementLog({ conversation: { transcript: [
      { role: "agent", message: "Hi, I'm calling about your short sale listing." },
      { role: "user", message: "... is available." },
      { role: "agent", message: "Are you handling the short sale paperwork yourself?" },
      { role: "user", message: "Thanks. Please stay on the line." },
      { role: "user", message: tail },
    ] } });
    assert.equal(result.flags.humanAnswered, false);
    assert.equal(result.flags.agentRespondedAfterOpeningQuestion, false);
  }
  const mailbox = await measurementLog({ conversation: { transcript: [
    { role: "agent", message: "Hi, I'm calling about your short sale listing." },
    { role: "user", message: "2025550123." },
    { role: "user", message: "Nothing has been recorded. Record your message after the tone." },
  ] } });
  assert.equal(mailbox.flags.humanAnswered, false);
  assert.equal(mailbox.metrics.agentTurns, 0);
});

test("IVR menus and explicit AI receptionists are not human agents", async () => {
  for (const message of [
    "Thank you for calling. Press one for sales or two for the directory.",
    "I'm Morgan's AI assistant. How may I help?",
  ]) {
    const result = await measurementLog({ conversation: { transcript: [{ role: "user", message }] } });
    assert.equal(result.flags.humanAnswered, false);
    assert.equal(result.flags.liveAnswered, false);
    assert.notEqual(result.contactEvidence.category, "apparent_human");
  }
});

test("greeting-only pickup is apparent human without invented target identity or engagement", async () => {
  const result = await measurementLog({ conversation: { transcript: [
    { role: "user", message: "Hello?" },
    { role: "agent", message: "Hi, I'm calling about your short sale listing." },
  ] } });
  assert.equal(result.contactEvidence.category, "apparent_human");
  assert.equal(result.contactEvidence.greetingOnly, true);
  assert.equal(result.flags.humanAnswered, true);
  assert.equal(result.flags.targetAgentAnswered, null);
  assert.equal(result.flags.agentRespondedAfterReason, false);
});

test("missing transcript and unresolved speech remain unknown even when provider reports success", async () => {
  for (const transcript of [undefined, [], [{ role: "user", message: "..." }], [{ role: "user", message: "Mm." }]]) {
    const result = await measurementLog({ conversation: { analysis: { call_successful: "success" }, transcript } });
    assert.equal(result.flags.liveAnswered, false);
    assert.equal(result.flags.humanAnswered, null);
    assert.equal(result.flags.targetAgentAnswered, null);
    assert.equal(result.contactEvidence.humanRespondedAfterReason, null);
    assert.equal(result.rawSignals.providerCallSuccessful, "success");
    if (transcript === undefined) {
      assert.equal(result.rawSignals.hasMeaningfulUserTranscript, null);
      assert.equal(result.metrics.agentTurns, null);
    }
  }
});

test("screener followed by human counts only the human exchange", async () => {
  const result = await measurementLog({ metadata: { fullName: "Morgan Example" }, conversation: { transcript: [
    { role: "user", message: "Record your name and reason for calling." },
    { role: "agent", message: "Maya with Crisp Short Sales about the short sale listing." },
    { role: "user", message: "Thanks. Please stay on the line." },
    { role: "user", message: "Hello, this is Morgan." },
    { role: "agent", message: "Are you handling the short sale paperwork yourself?" },
    { role: "user", message: "Yes, I am." },
  ] } });
  assert.equal(result.contactEvidence.category, "target_agent");
  assert.equal(result.flags.humanAnswered, true);
  assert.equal(result.flags.targetAgentAnswered, true);
  assert.equal(result.metrics.agentTurns, 2);
  assert.deepEqual(result.contactEvidence.humanTurnIndexes, [3, 5]);
  assert.equal(result.flags.agentRespondedAfterOpeningQuestion, true);
});

test("admin contact and subsequent target pickup are tracked separately", async () => {
  const transcript = [
    { role: "user", message: "Hello, this is her admin. How can I help?" },
    { role: "agent", message: "I'm calling about the short sale listing." },
    { role: "user", message: "Could you repeat the number?" },
  ];
  const admin = await measurementLog({ conversation: { transcript } });
  assert.equal(admin.contactEvidence.category, "human_gatekeeper");
  assert.equal(admin.flags.humanAnswered, true);
  assert.equal(admin.flags.humanGatekeeperAnswered, true);
  assert.equal(admin.flags.targetAgentAnswered, false);
  const target = await measurementLog({ conversation: { transcript: [
    ...transcript,
    { role: "user", message: "I'm the listing agent." },
  ] } });
  assert.equal(target.contactEvidence.category, "target_agent");
  assert.equal(target.flags.humanGatekeeperAnswered, true);
  assert.equal(target.flags.targetAgentAnswered, true);
});

test("a different named office responder is not silently counted as the target agent", async () => {
  const result = await measurementLog({ metadata: { fullName: "Morgan Example" }, conversation: { transcript: [
    { role: "user", message: "Good morning, thank you for calling Example Realty. This is Alex. How can I help?" },
    { role: "agent", message: "Maya with Crisp Short Sales about a short sale." },
    { role: "user", message: "May I take a number?" },
  ] } });
  assert.equal(result.contactEvidence.category, "human_gatekeeper");
  assert.equal(result.flags.targetAgentAnswered, false);
});

test("a named pickup after a business name is human but its target role remains unknown", async () => {
  const result = await measurementLog({ metadata: { fullName: "Morgan Example" }, conversation: { transcript: [
    { role: "user", message: "Example Services, this is Alex speaking. Hello?" },
    { role: "agent", message: "Maya with Crisp Short Sales about a short sale." },
  ] } });
  assert.equal(result.contactEvidence.category, "apparent_human");
  assert.equal(result.flags.targetAgentAnswered, null);
});

test("service-first opener measures service and listing question while yes only confirms the listing", async () => {
  const result = await measurementLog({ metadata: { declaredConversationPolicyVersion: "maya-service-first-recovery-20261003" }, conversation: { transcript: [
    { role: "user", message: "Hello." },
    { role: "agent", message: "Hi, this is Maya with Crisp Short Sales. We help with short-sale lender paperwork. Is 123 Fictional Street your listing?" },
    { role: "user", message: "Yes." },
  ] } });
  assert.equal(result.flags.reasonDelivered, true);
  assert.equal(result.flags.openingQuestionDelivered, true);
  assert.equal(result.flags.targetAgentAnswered, true);
  assert.equal(result.flags.clearLiveTransferConsent, false);
  assert.equal(result.flags.callbackRequested, false);
});

test("Call Assist recordings have their own review bucket without target-agent credit", async () => {
  const result = await measurementLog({ conversation: { transcript: [
    { role: "user", message: "This is Call Assist. Please say your name and reason." },
    { role: "agent", message: "Maya with Crisp Short Sales about the listing." },
    { role: "user", message: "Please remove this number from your mailing list." },
    { role: "agent", tool_calls: [{ tool_name: "callback_requested" }] },
  ] }, callbackConsent: null });
  assert.equal(result.contactEvidence.reviewBucket, "recorded_screener");
  assert.equal(result.flags.targetAgentAnswered, false);
  assert.equal(result.handoffEvidence.callbackRequested, false);
  assert.equal(result.handoffEvidence.callbackToolFired, true);
  assert.equal(result.handoffEvidence.callbackRequestCaptured, false);
});

test("callback tool fire, caller request, capture receipt and completion are distinct", async () => {
  const conversation = { transcript: [
    { role: "user", message: "Hello, I'm the listing agent." },
    { role: "agent", message: "Would help be useful?" },
    { role: "user", message: "Have Yoni call me tomorrow at four." },
    { role: "agent", tool_calls: [{ tool_name: "callback_requested" }] },
    { role: "agent", tool_results: [{ tool_name: "callback_requested", result_value: '{"queued":true}' }] },
  ] };
  const unconsented = await measurementLog({ conversation, callbackConsent: null });
  assert.equal(unconsented.flags.callbackRequested, false);
  const requested = await measurementLog({ conversation, callbackConsent: { callbackTime: "tomorrow at four" } });
  assert.equal(requested.handoffEvidence.callbackRequested, true);
  assert.equal(requested.handoffEvidence.callbackRequestCaptured, false);
  conversation.transcript[4].tool_results![0].result_value = '{"requestCaptured":true,"queued":true,"callbackTime":"tomorrow at four"}';
  const captured = await measurementLog({ conversation, callbackConsent: { callbackTime: "tomorrow at four" } });
  assert.equal(captured.handoffEvidence.callbackRequestCaptured, true);
  assert.equal(captured.handoffEvidence.callbackTime, "tomorrow at four");
  assert.equal(captured.handoffEvidence.callbackCompleted, null);
  assert.equal(captured.handoffEvidence.positiveHandoffVerified, null);
  const corrected = await measurementLog({ conversation, callbackConsent: { callbackTime: "tomorrow at five" } });
  assert.equal(corrected.handoffEvidence.callbackRequestCaptured, false);
  const unfinished = await measurementLog({ conversation: { ...conversation, status: "in-progress" },
    callbackConsent: { callbackTime: "tomorrow at four" } });
  assert.equal(unfinished.handoffEvidence.callbackRequestCaptured, false);
});

test("transfer completion requires consent and a parsed result from a matching final receipt", async () => {
  const transcript = [
    { role: "user", message: "I'm the listing agent." },
    { role: "agent", message: "Want me to try to get Yoni on the phone now?" },
    { role: "user", message: "Yes, go ahead." },
    { role: "agent", tool_calls: [{ tool_name: "live_transfer_requested" }] },
    { role: "agent", tool_results: [{ tool_name: "transfer_to_number", result_value: 'unparsed text "status":"success"' }] },
  ];
  assert.equal((await measurementLog({ conversation: { transcript } })).flags.transferCompleted, false);
  transcript[4].tool_results![0].result_value = '{"status":"success"}';
  assert.equal((await measurementLog({ conversation: { transcript } })).flags.transferCompleted, true);
  assert.equal((await measurementLog({ conversation: { transcript, conversation_id: "other" } })).flags.transferCompleted, false);
  transcript[2].message = "No, email only.";
  assert.equal((await measurementLog({ conversation: { transcript } })).flags.transferCompleted, false);
});
