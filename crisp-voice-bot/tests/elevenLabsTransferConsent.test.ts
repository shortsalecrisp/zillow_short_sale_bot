import assert from "node:assert/strict";
import test from "node:test";

test("transfer consent rejects overlapped okay plus meeting/callback-later signal", async () => {
  const {
    hasCallbackOrLaterSignal,
    hasClearLiveTransferConsent,
    hasLiveTransferToolCall,
    isMisfiredLiveTransferRequest,
  } = await import("../src/lib/elevenLabsTransferConsent");

  const transcript = [
    {
      role: "agent",
      message:
        "Got it. We can take the lender paperwork and bank calls off your plate at no cost to you or the seller. Should I see if Yoni, our short sale specialist, can hop on for sixty seconds and explain it?",
    },
    { role: "user", message: "I, so... Okay. Okay." },
    { role: "agent", message: "Ok, hold on, let me see if he's available one second." },
    { role: "agent", tool_calls: [{ tool_name: "live_transfer_requested" }] },
    {
      role: "user",
      message:
        "Right now, I am on the meeting. On the afternoon or tomorrow, I call you back. Okay.",
    },
  ];
  const summary = "The caller was in a meeting and said she would call back later or tomorrow.";

  assert.equal(hasLiveTransferToolCall(transcript), true);
  assert.equal(hasCallbackOrLaterSignal(transcript, summary), true);
  assert.equal(hasClearLiveTransferConsent(transcript, summary), false);
  assert.equal(isMisfiredLiveTransferRequest(transcript, summary), true);
});

test("transfer consent accepts a clear yes after a Yoni-now offer", async () => {
  const { hasClearLiveTransferConsent, isMisfiredLiveTransferRequest } = await import(
    "../src/lib/elevenLabsTransferConsent"
  );

  const transcript = [
    {
      role: "agent",
      message: "Want me to try to get Yoni on the phone now?",
    },
    { role: "user", message: "Sure, go ahead." },
    { role: "agent", tool_calls: [{ tool_name: "live_transfer_requested" }] },
  ];

  assert.equal(hasClearLiveTransferConsent(transcript), true);
  assert.equal(isMisfiredLiveTransferRequest(transcript), false);
});

test("transfer consent stays true when callback fallback happens only after the transfer attempt", async () => {
  const { hasClearLiveTransferConsent, isMisfiredLiveTransferRequest } = await import(
    "../src/lib/elevenLabsTransferConsent"
  );

  const transcript = [
    { role: "agent", message: "Want me to try to get Yoni on the phone now?" },
    { role: "user", message: "Yes, go ahead." },
    { role: "agent", tool_calls: [{ tool_name: "live_transfer_requested" }] },
    { role: "agent", message: "Sorry, he was not available. Should I have him call you back ASAP?" },
    { role: "user", message: "Yes, have him call me back." },
    { role: "agent", tool_calls: [{ tool_name: "callback_requested" }] },
  ];

  assert.equal(hasClearLiveTransferConsent(transcript, "Transfer failed, so an ASAP callback was arranged."), true);
  assert.equal(isMisfiredLiveTransferRequest(transcript, "Transfer failed, so an ASAP callback was arranged."), false);
});
