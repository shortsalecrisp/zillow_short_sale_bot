import assert from "node:assert/strict";
import test from "node:test";
import type { CallMetadata } from "../src/types";

function metadata(rowNumber: number, callAttemptNumber: number): CallMetadata {
  return {
    rowNumber,
    callAttemptNumber,
    fullName: `Agent ${rowNumber}`,
    listingAddress: `${rowNumber} Test St`,
    requestedPhone: "+14045550100",
    dialedPhone: "+14045550100",
    testMode: false,
  };
}

test("ElevenLabs live transfer state is isolated per active call", async () => {
  const {
    beginElevenLabsLiveTransferAttempt,
    buildElevenLabsCallContextKey,
    completeElevenLabsLiveTransferAttempt,
    rememberElevenLabsCallContext,
    resetElevenLabsCallContextsForTest,
  } = await import("../src/lib/elevenLabsCallContext");

  resetElevenLabsCallContextsForTest();

  rememberElevenLabsCallContext(metadata(3401, 1), "conv_one");
  rememberElevenLabsCallContext(metadata(3402, 1), "conv_two");

  const firstCallKey = buildElevenLabsCallContextKey({ rowNumber: 3401, callAttemptNumber: 1 });
  const secondCallKey = buildElevenLabsCallContextKey({ rowNumber: 3402, callAttemptNumber: 1 });

  assert.equal(beginElevenLabsLiveTransferAttempt(firstCallKey), "started");
  assert.equal(beginElevenLabsLiveTransferAttempt(firstCallKey), "pending");
  assert.equal(beginElevenLabsLiveTransferAttempt(secondCallKey), "started");

  completeElevenLabsLiveTransferAttempt("accepted", secondCallKey);

  assert.equal(beginElevenLabsLiveTransferAttempt(secondCallKey), "accepted");
  assert.equal(beginElevenLabsLiveTransferAttempt(firstCallKey), "pending");
});

test("ElevenLabs call context can be recovered by conversation id", async () => {
  const {
    getElevenLabsCallContextByConversationId,
    rememberElevenLabsCallContext,
    resetElevenLabsCallContextsForTest,
  } = await import("../src/lib/elevenLabsCallContext");

  resetElevenLabsCallContextsForTest();

  rememberElevenLabsCallContext(metadata(3403, 2), "conv_three");

  assert.equal(getElevenLabsCallContextByConversationId("conv_three")?.rowNumber, 3403);
  assert.equal(getElevenLabsCallContextByConversationId("missing"), undefined);
});
