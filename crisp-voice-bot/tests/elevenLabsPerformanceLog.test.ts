import assert from "node:assert/strict";
import test from "node:test";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";

test("voice performance log stores codex-readable A/B metrics in one cell block", async () => {
  const { buildVoicePerformanceLog, VOICE_PERFORMANCE_LOG_MARKER } = await import(
    "../src/lib/elevenLabsPerformanceLog"
  );

  const log = buildVoicePerformanceLog({
    conversationId: "conv_test",
    outcome: "Call received but agent hung up on Maya",
    summary: "The agent asked if Maya was a chatbot and hung up early.",
    transcript:
      "Maya: Hi Chris, this is Maya with Crisp Short Sales about your short sale listing. Are you handling the bank side yourself?\nAgent: Are you a chatbot?",
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
      openerVariantLabel: "Direct short sale reason",
      openerScript:
        "Hi Chris, this is Maya with Crisp Short Sales about your short sale listing. Are you handling the bank side yourself?",
      scheduledWindow: "late_morning",
      agentTimeZone: "America/New_York",
    },
    conversation: {
      status: "done",
      metadata: {
        termination_reason: "Client disconnected: 1000",
        call_duration_secs: 18,
      },
      transcript: [
        {
          role: "agent",
          message:
            "Hi Chris, this is Maya with Crisp Short Sales about your short sale listing. Are you handling the bank side yourself?",
          time_in_call_secs: 0.7,
        },
        { role: "user", message: "Are you a chatbot?", time_in_call_secs: 12 },
      ],
    },
  });

  assert.match(log, new RegExp(`^--- ${VOICE_PERFORMANCE_LOG_MARKER} ---\\n`));

  const parsed = JSON.parse(log.replace(`--- ${VOICE_PERFORMANCE_LOG_MARKER} ---\n`, ""));
  assert.equal(parsed.schema, "voice_call_metrics_v1");
  assert.equal(parsed.abTestScope.cohort, "eryn_vs_finch");
  assert.deepEqual(parsed.abTestScope.includeOnlyVoiceVariants, ["eryn", "finch"]);
  assert.equal(parsed.abTestScope.excludePriorSingleVoiceEmmyCalls, true);
  assert.match(parsed.abTestScope.analysisRule, /Exclude all previous single-voice Emmy calls/i);
  assert.equal(parsed.call.voiceVariant, "eryn");
  assert.equal(parsed.call.assistantName, "Maya");
  assert.equal(parsed.call.openerVariant, "direct_reason");
  assert.equal(parsed.call.openerVariantLabel, "Direct short sale reason");
  assert.match(parsed.call.openerScript, /short sale listing/);
  assert.equal(parsed.call.scheduledWindow, "late_morning");
  assert.equal(parsed.call.agentTimeZone, "America/New_York");
  assert.equal(parsed.metrics.durationSecs, 18);
  assert.equal(parsed.metrics.agentTurns, 1);
  assert.equal(parsed.metrics.assistantTurns, 1);
  assert.equal(parsed.metrics.reasonMentionedAtSecs, 0.7);
  assert.equal(parsed.metrics.openingQuestionAtSecs, 0.7);
  assert.equal(parsed.flags.aiSuspicion, true);
  assert.equal(parsed.flags.reasonDelivered, true);
  assert.equal(parsed.flags.openingQuestionDelivered, true);
  assert.equal(parsed.flags.agentRespondedAfterReason, true);
  assert.equal(parsed.flags.agentRespondedAfterOpeningQuestion, true);
  assert.equal(parsed.flags.hangupBeforeReason, false);
  assert.equal(parsed.flags.hangupBeforeOpeningQuestion, false);
  assert.match(parsed.codexInstructions, /Compare voiceVariant/i);
  assert.match(parsed.codexInstructions, /openerVariant/i);
  assert.match(parsed.codexInstructions, /hangupBeforeReason/i);
  assert.match(parsed.codexInstructions, /current production baseline as Eryn voice with Maya assistant name/i);
  assert.match(parsed.codexInstructions, /without call\.voiceVariant of eryn or finch/i);
  assert.match(parsed.codexInstructions, /previous single-voice Emmy calls/i);
  assert.match(parsed.transcript, /Are you a chatbot/);
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
