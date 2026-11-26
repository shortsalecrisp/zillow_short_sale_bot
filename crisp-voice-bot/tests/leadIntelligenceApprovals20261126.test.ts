import assert from "node:assert/strict";
import test from "node:test";
import { isClippedScreeningInstruction, isRecordingOrScreeningArtifact } from "../src/lib/elevenLabsRecordingState";

Object.assign(process.env, { BASE_URL: "https://example.com", TELNYX_API_KEY: "test", TELNYX_CALLER_ID: "+12175550100",
  TELNYX_CONNECTION_ID: "test", TELNYX_OUTBOUND_VOICE_PROFILE_ID: "test", TEST_DESTINATION_NUMBER: "+12175550101",
  CALL_TRANSCRIPT_EMAILS_ENABLED: "false", SMTP_HOST: "", SMTP_USER: "", SMTP_PASS: "", ALERT_EMAIL_TO: "" });

const fragment = "Hi, if you record your...";
const transcript = [{ role: "user", message: fragment },
  { role: "agent", message: "I'm Maya, an AI assistant with Crisp Short Sales. We help listing agents with short-sale lender paperwork. How can I help?" },
  { role: "user", message: "..." }];

test("clipped screening is not human contact or a human hangup, and never creates contact consent", async () => {
  const api = await import("../src/lib/elevenLabsPostCall");
  const { buildVoicePerformanceLog } = await import("../src/lib/elevenLabsPerformanceLog");
  const conversation = { status: "done", has_user_audio: true,
    metadata: { termination_reason: "Client disconnected: 1000", call_duration_secs: 120 }, transcript };
  assert.equal(isClippedScreeningInstruction(fragment), true);
  assert.equal(isRecordingOrScreeningArtifact(transcript), true);
  assert.equal(api.getAuthoritativeTerminalCallResult(conversation, 2, { firstName: "Andrew" }), "agent_not_available");
  assert.equal(api.getExplicitCallbackConsent(conversation), undefined);
  assert.equal(api.getVoiceContactRequestResult(conversation), undefined);
  const log = JSON.parse(buildVoicePerformanceLog({ conversationId: "conv_synthetic_fragment", metadata: { rowNumber: 123,
    fullName: "Andrew Synthetic", callAttemptNumber: 2 }, conversation, terminalCallResult: "agent_not_available",
    outcome: "Agent was not available", summary: "", transcript: "" }).split("\n").slice(1).join("\n"));
  assert.equal(log.contactEvidence.reviewBucket, "recorded_screener");
  assert.equal(log.contactEvidence.humanAnswered, false);
  assert.equal(log.contactEvidence.targetAgentAnswered, false);
  assert.equal(log.flags.callbackRequested, false);
});

test("fragment detection is narrow and later live questions, callbacks, opt-outs and takeover remain authoritative", async () => {
  const api = await import("../src/lib/elevenLabsPostCall");
  for (const message of ["If you record your name, can you send it to me?", "My greeting says if you record your name.",
    "I'm here. Please hold while I find the listing.", "Hi, this is Andrew.", "Please record your callback request."]) {
    assert.equal(isClippedScreeningInstruction(message), false, message);
  }
  for (const message of ["This is Andrew, the listing agent. What is this about?", "Can you call me back tomorrow at ten?",
    "Don't call me again.", "I am taking over this call."]) {
    const live = { status: "done", transcript: [...transcript, { role: "user", message }] };
    assert.equal(isRecordingOrScreeningArtifact(live.transcript), false, message);
    assert.equal(api.shouldTreatAsRecordingArtifact(live), false, message);
  }
  const callback = { status: "done", transcript: [...transcript, { role: "user", message: "Can you call me back tomorrow at ten?" }] };
  assert.equal(api.getExplicitCallbackConsent(callback)?.callbackTime, "tomorrow at ten");
  assert.equal(api.getAuthoritativeTerminalCallResult({ transcript: [...transcript, { role: "user", message: "Don't call me again." }] }, 2), "do_not_call");
});
