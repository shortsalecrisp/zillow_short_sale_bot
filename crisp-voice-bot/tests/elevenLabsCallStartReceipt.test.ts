import assert from "node:assert/strict";
import test from "node:test";
import { AxiosError } from "axios";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.ELEVENLABS_AGENT_ID = "agent";
process.env.ELEVENLABS_AGENT_PHONE_NUMBER_ID = "phone";

const metadata = {
  rowNumber: 5590,
  callAttemptNumber: 1,
  dialedPhone: "+12025550123",
  callStartRequestId: "5590-1-request-a",
};

function conversation(overrides: Record<string, unknown> = {}) {
  return {
    conversation_id: "conv_receipt",
    status: "failed",
    start_time_unix_secs: 1_788_450_000,
    conversation_initiation_client_data: {
      dynamic_variables: {
        rowNumber: 5590,
        callAttemptNumber: 1,
        phone: "+12025550123",
        callStartRequestId: "5590-1-request-a",
      },
    },
    metadata: {
      accepted_time_unix_secs: null,
      call_duration_secs: 0,
      error: { code: 1011, reason: "sip request timed out" },
    },
    ...overrides,
  };
}

test("call-start receipt classification permits one retry only for a matching definitive failure", async () => {
  const { classifyElevenLabsCallStartConversation } = await import("../src/lib/elevenLabs");
  const receipt = classifyElevenLabsCallStartConversation(conversation(), metadata, 1_788_450_000);

  assert.equal(receipt?.status, "definitive_failure");
  assert.equal(receipt?.conversationId, "conv_receipt");
  assert.equal(
    classifyElevenLabsCallStartConversation(
      conversation({
        conversation_initiation_client_data: {
          dynamic_variables: {
            rowNumber: 5590,
            callAttemptNumber: 1,
            phone: "+12025550123",
            callStartRequestId: "different-request",
          },
        },
      }),
      metadata,
      1_788_450_000,
    ),
    undefined,
  );
});

test("call-start receipt classification never retries a created or accepted conversation", async () => {
  const { classifyElevenLabsCallStartConversation } = await import("../src/lib/elevenLabs");
  const initiated = classifyElevenLabsCallStartConversation(
    conversation({ status: "initiated", metadata: { accepted_time_unix_secs: null, call_duration_secs: 0 } }),
    metadata,
    1_788_450_000,
  );
  const acceptedThenFailed = classifyElevenLabsCallStartConversation(
    conversation({ status: "failed", metadata: { accepted_time_unix_secs: 1_788_450_005, call_duration_secs: 3 } }),
    metadata,
    1_788_450_000,
  );

  assert.equal(initiated?.status, "accepted");
  assert.equal(acceptedThenFailed?.status, "accepted");
});

test("outbound body carries the unique receipt key and timeout detection is narrow", async () => {
  const { buildElevenLabsOutboundCallBody, isElevenLabsCallStartTimeout } = await import("../src/lib/elevenLabs");
  const body = buildElevenLabsOutboundCallBody({
    agentId: "agent_test",
    agentPhoneNumberId: "phone_test",
    to: "+12025550123",
    metadata: {
      ...metadata,
      fullName: "David Test",
      listingAddress: "100 Main St, Atlanta, GA",
      requestedPhone: "+12025550123",
      testMode: true,
      assistantName: "Eryn",
      voiceVariant: "eryn",
      voiceName: "Eryn",
      voiceId: "voice_test",
    },
  });

  assert.equal(
    body.conversation_initiation_client_data.dynamic_variables.callStartRequestId,
    "5590-1-request-a",
  );
  assert.equal(isElevenLabsCallStartTimeout(new AxiosError("timeout of 45000ms exceeded", "ECONNABORTED")), true);
  assert.equal(isElevenLabsCallStartTimeout(new AxiosError("Request failed with status code 500", "ERR_BAD_RESPONSE")), false);
  assert.equal(isElevenLabsCallStartTimeout(new Error("timeout")), false);
});
