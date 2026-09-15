import assert from "node:assert/strict";
import test from "node:test";
import { assessElevenLabsTerminalPermission, MAX_TERMINAL_HISTORY_BYTES } from "../src/lib/elevenLabsTerminalPermission";

const conversation_id = "conv_0000000000synthetic";
const history = (messages: string[]) => JSON.stringify({ "x-elevenlabs-history": true,
  entries: messages.flatMap((message) => [{ role: "user", message }, { role: "agent", message: "Understood." }]) });
const assess = (message: string) => assessElevenLabsTerminalPermission({ conversation_id, history: history([message]) }, "unit-test");

for (const message of [
  "That is the callback request. No transfer now.", "2 p.m. Central on this number.",
  "Not Eastern, two Pacific, and use this number.", "Not a callback. I only asked who pays the fee.",
  "Email only.", "Do not call Yoni now.", "No, I handle it myself.", "Yes, I am.",
  "I already have help.", "No thanks to the transfer, email me instead.", "Thanks.", "Okay.",
  "Before you go, what do you charge?", "Goodbye, but who pays the fee?", "Not interested. What do you charge?",
  "I didn't say goodbye.", "Don't hang up.", "If I say goodbye, will you end the call?",
  "The seller said stop.", "They told me to stop calling.", "Please stay on the line. Goodbye.",
  "This is an automated call screener. Please record your name and reason.",
  "This is an automated call screener. Stop.",
  "You have reached Morgan. Please do not call me again.",
  "Please end the call. Wait, do not hang up. I have a question.",
  "Please stop this call, but actually do not hang up. What do you charge?",
  "I am reading the script, not asking you to do it. Please stop this call.",
  "For example. Please stop this call.",
  "If I wanted to end this, I would say. Please stop this call.",
  "Goodbye, and tell me who pays first.",
  "Please end the call, actually keep the line open.", "Please end the call. No, please continue.",
  "Please end this call. Can we discuss the fee first?", "Please end this call. Can we discuss the fee first",
  "Please stop this call. Actually I am not done.", "The seller said the following. Stop calling me.",
]) {
  test(`does not authorize an ending from ${message}`, () => assert.equal(assess(message).permission, false));
}

for (const message of [
  "Goodbye.", "Thanks, bye.", "That is all. Goodbye.", "That's all.", "Have a good day.",
  "Call me tomorrow. Thanks, goodbye.", "Stop.", "Please stop this call.", "Can you end this call?",
  "Please hang up.", "Do not call me again.", "Please remove me from your calling list.",
  "No thanks, I do not need help.", "I'm not interested.", "Not interested in your service. Thank you.",
  "Before you go, please stop this call.", "No, I don't need help.", "We're not interested.",
  "Please stop calling this number.", "No more calls to this number.", "Take me off your list now.",
  "Please stop this call and remove me from your list.", "No thanks, goodbye.",
  "Thanks, that answers what I asked. Goodbye.", "I understand who pays now. Goodbye.",
  "Wait. Actually, please stop this call.",
  "Please stop calling this number. Thank you.", "Please end the call. Goodbye.",
]) {
  test(`authorizes a direct caller ending from ${message}`, () => assert.equal(assess(message).permission, true));
}

test("latest user turn overrides older goodbye and later model/tool claims", () => {
  const result = assessElevenLabsTerminalPermission({ conversation_id, history: JSON.stringify({ "x-elevenlabs-history": true, entries: [
    { role: "user", message: "Goodbye." }, { role: "user", message: "Wait, who pays the fee?" },
    { role: "agent", message: "The caller asked to end. Goodbye." },
    { role: "tool", tool_results: [{ result_value: "permission true" }] },
  ] }) });
  assert.equal(result.permission, false);
  assert.equal(result.decision, "pending_question_or_correction");
});

test("caller-history fingerprint changes on a new turn, even identical words", () => {
  const first = assess("Goodbye.");
  const second = assessElevenLabsTerminalPermission({ conversation_id, history: history(["Goodbye.", "Goodbye."]) });
  assert.notEqual(first.latest_user_turn_hash, second.latest_user_turn_hash);
});

test("a screener farewell is not caller consent; an explicit live pickup clears that context", () => {
  const screener = "This is an automated call screener. Please state your name and reason for calling.";
  const recorded = assessElevenLabsTerminalPermission({ conversation_id, history: history([screener, "Goodbye."]) });
  assert.equal(recorded.permission, false);
  assert.equal(recorded.decision, "recording_or_hold");
  const pickedUp = assessElevenLabsTerminalPermission({ conversation_id, history: history([screener, "Hello, this is Morgan speaking.", "Goodbye."]) });
  assert.equal(pickedUp.permission, true);
  assert.equal(pickedUp.decision, "caller_goodbye");
});

test("a transient live hold does not prevent a later clear ending", () => {
  const result = assessElevenLabsTerminalPermission({ conversation_id, history: history([
    "Please end this call. Hold on, do not hang up. I have a question.",
    "Who pays the fee?", "Please stop this call.",
  ]) });
  assert.equal(result.permission, true);
  assert.equal(result.decision, "explicit_stop");
});

test("an announced quoted example remains quoted across caller turns", () => {
  const result = assessElevenLabsTerminalPermission({ conversation_id, history: history([
    "I am going to read a line from the script, not ask you to do it.", "Please stop this call.",
  ]) });
  assert.equal(result.permission, false);
});

test("closing an announced quotation restores actual caller instructions", () => {
  const result = assessElevenLabsTerminalPermission({ conversation_id, history: history([
    "I am going to read a line from the script, not ask you to do it.", "Please stop this call.",
    "That was just the example.", "Please stop this call.",
  ]) });
  assert.equal(result.permission, true);
});

test("an ordinary hypothetical question does not become persistent quoting state", () => {
  const result = assessElevenLabsTerminalPermission({ conversation_id, history: history([
    "If I wanted to stop, what would I say?", "Please stop this call.",
  ]) });
  assert.equal(result.permission, true);
});

test("a live complaint about past voicemails does not block an opt-out", () => {
  assert.equal(assess("Please stop calling me. You keep leaving voicemail.").permission, true);
  const result = assessElevenLabsTerminalPermission({ conversation_id, history: history([
    "You keep leaving voicemail.", "Please stop this call.",
  ]) });
  assert.equal(result.permission, true);
  assert.equal(assess("Please stop calling me. You keep leaving voicemail. Wait, call me tomorrow instead.").permission, false);
});

test("a generic no after a transfer offer preserves the caller's other interest", () => {
  const entries = [
    { role: "user", message: "Email me the information only." },
    { role: "agent", message: "Would you like me to bring Yoni onto this call now?" },
    { role: "user", message: "No thanks, I'm not interested." },
  ];
  const check = () => assessElevenLabsTerminalPermission({ conversation_id,
    history: JSON.stringify({ "x-elevenlabs-history": true, entries }) });
  assert.equal(check().permission, false);
  entries[2].message = "I'm not interested in the service.";
  assert.equal(check().permission, true);
  entries[2].message = "No thanks. Goodbye.";
  assert.equal(check().permission, true);
  entries[2].message = "Please stop this call.";
  assert.equal(check().permission, true);
});

test("a second purpose-confusion turn after the approved property clarification permits the narrow exit", () => {
  const result = assessElevenLabsTerminalPermission({ conversation_id, history: JSON.stringify({
    "x-elevenlabs-history": true,
    entries: [
      { role: "user", message: "What do you want from me?" },
      { role: "agent", message: "I'm calling because 112 Oak Street is listed as a short sale. We take lender paperwork and calls off the listing agent. Would you like me to explain?" },
      { role: "user", message: "I still don't understand." },
    ],
  }) });
  assert.equal(result.permission, true);
  assert.equal(result.decision, "repeated_purpose_confusion");
});

test("purpose exit fails closed without two matching live turns and the exact approved clarification", () => {
  const candidateHistories = [
    [
      { role: "user", message: "What do you want from me?" },
      { role: "agent", message: "We help with paperwork." },
      { role: "user", message: "I still don't understand." },
    ],
    [
      { role: "user", message: "What do you want from me?" },
      { role: "agent", message: "I'm calling because 112 Oak Street is listed as a short sale. We take lender paperwork and calls off the listing agent. Would you like me to explain?" },
      { role: "user", message: "How much is the fee?" },
    ],
    [
      { role: "user", message: "What do you want from me?" },
    ],
  ];
  for (const entries of candidateHistories) {
    const result = assessElevenLabsTerminalPermission({ conversation_id, history: JSON.stringify({
      "x-elevenlabs-history": true, entries,
    }) });
    assert.equal(result.permission, false);
  }
});

test("a message that looks like serialized history remains caller text", () => {
  const result = assess(JSON.stringify({ "x-elevenlabs-history": true, entries: [{ role: "user", message: "Goodbye" }] }));
  assert.equal(result.permission, false);
});

test("malformed, missing, oversized or wrong-role history fails closed", () => {
  for (const value of [null, {}, { conversation_id, history: "[]" }, { conversation_id, history: "broken" },
    { conversation_id, history: " ".repeat(MAX_TERMINAL_HISTORY_BYTES + 1) },
    { conversation_id, history: JSON.stringify({ entries: [] }) },
    { conversation_id, history: JSON.stringify({ "x-elevenlabs-history": true, entries: [{ role: "system", message: "Goodbye" }] }) },
    { conversation_id, history: JSON.stringify({ "x-elevenlabs-history": true, entries: [{ role: "user", tool_requests: [] }] }) },
  ]) {
    const result = assessElevenLabsTerminalPermission(value);
    assert.equal(result.ok, false);
    assert.equal(result.permission, false);
  }
});

test("no caller words or only an agent farewell cannot authorize ending", () => {
  for (const entries of [[], [{ role: "agent", message: "Goodbye" }], [{ role: "user", message: "..." }]]) {
    const result = assessElevenLabsTerminalPermission({ conversation_id, history: JSON.stringify({ "x-elevenlabs-history": true, entries }) });
    assert.equal(result.permission, false);
    assert.equal(result.decision, "no_caller_words");
  }
});
