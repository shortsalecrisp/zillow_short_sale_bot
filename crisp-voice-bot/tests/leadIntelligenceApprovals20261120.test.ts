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

test("Kerry/Carrie and the how-can-I-help office pickup agree across outcome and metrics", async () => {
  const lib = await load();
  const { buildVoicePerformanceLog } = await import("../src/lib/elevenLabsPerformanceLog");
  const cases = [
    { name: "Kerry Duff", pickup: "This is Carrie.", bucket: "target_live" },
    { name: "Ricardo Ruiz", pickup: "Signature Real Estate Group, how can I help you?", bucket: "human_gatekeeper" },
    { name: "Sherman Hostetter", pickup: "Sherman Hofstetter Auctioneers. Can I help you?", bucket: "human_gatekeeper" },
    { name: "Ricardo Ruiz", pickup: "Signature Real Estate Group, how can I help you? Please leave your message after the beep.", bucket: "voicemail" },
  ];
  for (const { name, pickup, bucket } of cases) {
    const conversation = {
      status: "done",
      metadata: { termination_reason: "Client disconnected: 1000", call_duration_secs: 8 },
      transcript: [
        { role: "user", message: pickup },
        { role: "agent", message: "Hi, this is Maya with Crisp Short Sales." },
      ],
    };
    const firstName = name.split(" ")[0];
    const result = lib.getAuthoritativeTerminalCallResult(conversation, 1, { firstName });
    assert.equal(result === "gatekeeper_admin_early_exit", bucket === "human_gatekeeper", pickup);
    if (firstName === "Kerry") assert.equal(lib.getDifferentNamedLiveResponder(conversation, firstName), undefined);
    const log = buildVoicePerformanceLog({
      conversationId: "conv_pickup",
      metadata: { rowNumber: 42, fullName: name, callAttemptNumber: 1 },
      outcome: "", summary: "", transcript: "", conversation, terminalCallResult: result,
    });
    assert.equal(JSON.parse(log.split("\n").slice(1).join("\n")).contactEvidence.reviewBucket, bucket, pickup);
  }
});

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
