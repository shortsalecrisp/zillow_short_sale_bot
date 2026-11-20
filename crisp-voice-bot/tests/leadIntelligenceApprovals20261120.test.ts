import assert from "node:assert/strict";
import test from "node:test";

Object.assign(process.env, {
  BASE_URL: "https://example.com",
  TELNYX_API_KEY: "test",
  TELNYX_CALLER_ID: "+12175550100",
  TELNYX_CONNECTION_ID: "test",
  TELNYX_OUTBOUND_VOICE_PROFILE_ID: "test",
  TEST_DESTINATION_NUMBER: "+12175550101",
  ELEVENLABS_API_KEY: "test",
  CALL_TRANSCRIPT_EMAILS_ENABLED: "false",
});

const load = () => import("../src/lib/elevenLabsPostCall");

test("Patrick refusal after listing confirmation outranks recording-artifact hangup fallback", async () => {
  const lib = await load();
  const conversation = {
    status: "done",
    metadata: { termination_reason: "Client disconnected: 1000", call_duration_secs: 58 },
    transcript: [
      { role: "user", message: "Hello?" },
      { role: "agent", message: "Hi, this is Maya with Crisp Short Sales. We help with short-sale lender paperwork. Is Sixty One Forty Buckskin your listing?" },
      { role: "user", message: "Operator." },
      { role: "agent", message: "Is Sixty One Forty Buckskin your listing?" },
      { role: "user", message: "Maybe, maybe not." },
      { role: "agent", message: "Just to clarify, do you have the listing at Sixty One Forty Buckskin?" },
      { role: "user", message: "I don't talk to Bonnie." },
      { role: "agent", message: "Do you handle that listing?" },
      { role: "user", message: "I do, but I don't need your help." },
      { role: "agent", message: "Understood. Thanks for letting me know." },
    ],
  };
  assert.equal(lib.shouldTreatAsNotInterested(conversation), true);
  assert.equal(lib.getAuthoritativeTerminalCallResult(conversation, 1, { firstName: "Patrick" }), "answered_not_interested");
  assert.equal(lib.shouldTreatAsAgentHungUp(conversation), false);
});

test("Sherman organization greeting is live gatekeeper evidence, not a generic hangup", async () => {
  const lib = await load();
  const conversation = {
    status: "done",
    metadata: { termination_reason: "Client disconnected: 1000", call_duration_secs: 9 },
    transcript: [
      { role: "user", message: "Sherman Hofstetter Auctioneers. Can I help you?" },
      { role: "agent", message: "Maya with Crisp Short Sales. We help with short-sale lender paperwork. I'm calling about Sherman's listing at Eleven..." },
    ],
  };
  assert.equal(lib.hasLiveHumanGatekeeperEvidence(conversation, "Sherman"), true);
  assert.equal(lib.getAuthoritativeTerminalCallResult(conversation, 1, { firstName: "Sherman" }), "gatekeeper_admin_early_exit");

  const recorded = {
    ...conversation,
    transcript: [
      { role: "user", message: "Sherman Hofstetter Auctioneers. We cannot take your call. Please leave a message after the beep." },
    ],
  };
  assert.equal(lib.hasLiveHumanGatekeeperEvidence(recorded, "Sherman"), false);
  assert.notEqual(lib.getAuthoritativeTerminalCallResult(recorded, 1, { firstName: "Sherman" }), "gatekeeper_admin_early_exit");
});

test("Taylor hesitation-prefaced callback request preserves timing and different responder identity", async () => {
  const lib = await load();
  const conversation = {
    status: "done",
    metadata: { termination_reason: "Client disconnected: 1000", call_duration_secs: 57 },
    transcript: [
      { role: "user", message: "This is Sam." },
      { role: "agent", message: "Hi, this is Maya with Crisp Short Sales. We help with short-sale lender paperwork. Is Twenty Nine Twelve Eastgate your listing?" },
      { role: "user", message: "Yes." },
      { role: "agent", message: "Would help with lender paperwork or calls be useful for this listing?" },
      { role: "user", message: "Um, yes, please." },
      { role: "agent", message: "Would you like me to see if Yoni, our short sale specialist, can join this call now?" },
      { role: "user", message: "Um, can you actually call me back in, like, 20 minutes? I'm in a meeting." },
      { role: "agent", message: "Thanks. I've received your callback request." },
    ],
  };
  const consent = lib.getExplicitCallbackConsent(conversation);
  assert.equal(consent?.callerText, "Um, can you actually call me back in, like, 20 minutes? I'm in a meeting.");
  assert.equal(consent?.callbackTime, "in, like, 20 minutes");
  assert.equal(lib.getDifferentNamedLiveResponder(conversation, "Taylor"), "Sam");
  assert.equal(lib.getDifferentNamedLiveResponder(conversation, "Sam"), undefined);
});
