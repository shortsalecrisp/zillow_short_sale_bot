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
    outcome: "Call received but agent hung up on Finch",
    summary: "The agent asked if Finch was a chatbot and hung up early.",
    transcript:
      "Finch: Hey, is this Chris?\nAgent: This is Chris.\nFinch: Quick reason for my call...\nAgent: Are you a chatbot?",
    metadata: {
      rowNumber: 3481,
      fullName: "Chris Agent",
      callAttemptNumber: 1,
      listingAddress: "123 Main St, Tampa, FL",
      requestedPhone: "+18135550123",
      dialedPhone: "+18135550123",
      testMode: false,
      voiceVariant: "finch",
      voiceName: "Finch",
      assistantName: "Finch",
      voiceId: "voice_finch",
    },
    conversation: {
      status: "done",
      metadata: {
        termination_reason: "Client disconnected: 1000",
        call_duration_secs: 18,
      },
      transcript: [
        { role: "assistant", message: "Hey, is this Chris?", time_in_call_secs: 1 },
        { role: "user", message: "This is Chris.", time_in_call_secs: 3 },
        { role: "assistant", message: "Quick reason for my call...", time_in_call_secs: 4.4 },
        { role: "user", message: "Are you a chatbot?", time_in_call_secs: 12 },
      ],
    },
  });

  assert.match(log, new RegExp(`^--- ${VOICE_PERFORMANCE_LOG_MARKER} ---\\n`));

  const parsed = JSON.parse(log.replace(`--- ${VOICE_PERFORMANCE_LOG_MARKER} ---\n`, ""));
  assert.equal(parsed.schema, "voice_call_metrics_v1");
  assert.equal(parsed.call.voiceVariant, "finch");
  assert.equal(parsed.call.assistantName, "Finch");
  assert.equal(parsed.metrics.durationSecs, 18);
  assert.equal(parsed.metrics.agentTurns, 2);
  assert.equal(parsed.metrics.assistantTurns, 2);
  assert.equal(parsed.metrics.firstAgentToAssistantDelaySecs, 1.4);
  assert.equal(parsed.flags.aiSuspicion, true);
  assert.match(parsed.codexInstructions, /Compare voiceVariant/i);
  assert.match(parsed.transcript, /Are you a chatbot/);
});
