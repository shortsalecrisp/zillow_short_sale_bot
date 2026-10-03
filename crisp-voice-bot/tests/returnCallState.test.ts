import assert from "node:assert/strict";
import test from "node:test";
import type { ReturnCallState, ReturnCallStateDependencies } from "../src/lib/returnCallState";

process.env.BASE_URL = "https://voice.example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";

const parentCallSid = "v3:parent-test";
const initial: ReturnCallState = { parentCallSid, callerFrom: "+12175550102", createdAt: "2026-10-03T20:00:00.000Z" };
const playbackUrl = "https://voice.example.com/return-call/playback/recording?sig=test";

function fixture() {
  const state = new Map<string, ReturnCallState>();
  const sent: unknown[] = [];
  const deps: ReturnCallStateDependencies = {
    readState: async (key) => state.has(key) ? structuredClone(state.get(key)!) : null,
    writeState: async (key, value) => { state.set(key, structuredClone(value)); },
    sendMail: async (message) => { sent.push(message); },
  };
  return { state, sent, deps };
}

test("return-call state uses a hashed isolated BotState key", async () => {
  const lib = await import("../src/lib/returnCallState");
  assert.match(lib.returnCallStateKey(parentCallSid), /^telnyx_return_call_v1:[a-f0-9]{64}$/);
  assert.throws(() => lib.returnCallStateKey(""));
});

test("simultaneous updates preserve acceptance and office leg without lost writes", async () => {
  const lib = await import("../src/lib/returnCallState");
  const { deps } = fixture();
  await lib.updateReturnCallState(parentCallSid, () => initial, deps);
  await Promise.all([
    lib.updateReturnCallState(parentCallSid, async (current) => {
      await new Promise((resolve) => setTimeout(resolve, 10));
      return { ...current!, officeCallSid: "office" };
    }, deps),
    lib.updateReturnCallState(parentCallSid, (current) => ({ ...current!, acceptedAt: "2026-10-03T20:00:04.000Z" }), deps),
  ]);
  const state = await lib.readReturnCallState(parentCallSid, deps);
  assert.equal(state?.officeCallSid, "office");
  assert.equal(state?.acceptedAt, "2026-10-03T20:00:04.000Z");
});

test("identity and recording receipts cannot be replaced or erased", async () => {
  const lib = await import("../src/lib/returnCallState");
  const { deps } = fixture();
  await lib.updateReturnCallState(parentCallSid, () => ({ ...initial, recordingId: "r1", emailStatus: "sent" }), deps);
  await assert.rejects(lib.updateReturnCallState(parentCallSid, (s) => ({ ...s!, callerFrom: "+19995550111" }), deps), /identity/);
  await assert.rejects(lib.updateReturnCallState(parentCallSid, (s) => ({ ...s!, recordingId: "r2" }), deps), /replace/);
  await assert.rejects(lib.updateReturnCallState(parentCallSid, (s) => ({ ...s!, emailStatus: undefined }), deps), /remove/);
  await assert.rejects(lib.updateReturnCallState(parentCallSid, (s) => ({ ...s!, emailStatus: "sending" }), deps), /replace/);
});

test("voicemail notification is claimed durably before send and replay sends once", async () => {
  const lib = await import("../src/lib/returnCallState");
  const { deps, state, sent } = fixture();
  await lib.updateReturnCallState(parentCallSid, () => ({ ...initial, recordingId: "r1", recordingDuration: 20 }), deps);
  const send = deps.sendMail!;
  deps.sendMail = async (message) => {
    assert.equal(state.get(lib.returnCallStateKey(parentCallSid))?.emailStatus, "sending");
    await send(message);
  };
  const results = await Promise.all([
    lib.notifyReturnCallVoicemail(parentCallSid, playbackUrl, deps),
    lib.notifyReturnCallVoicemail(parentCallSid, playbackUrl, deps),
  ]);
  assert.deepEqual(results.map((r) => r.status), ["sent", "skipped"]);
  assert.equal(sent.length, 1);
  assert.equal((await lib.readReturnCallState(parentCallSid, deps))?.emailStatus, "sent");
});

test("ambiguous SMTP result is uncertain and never retried automatically", async () => {
  const lib = await import("../src/lib/returnCallState");
  const { deps } = fixture();
  await lib.updateReturnCallState(parentCallSid, () => ({ ...initial, recordingId: "r1" }), deps);
  let sends = 0;
  deps.sendMail = async () => { sends++; throw new Error("socket closed"); };
  assert.equal((await lib.notifyReturnCallVoicemail(parentCallSid, playbackUrl, deps)).status, "uncertain");
  assert.equal((await lib.notifyReturnCallVoicemail(parentCallSid, playbackUrl, deps)).status, "skipped");
  assert.equal(sends, 1);
});

test("claim write failure prevents email; interrupted sending claim blocks replay", async () => {
  const lib = await import("../src/lib/returnCallState");
  const { deps, sent } = fixture();
  await lib.updateReturnCallState(parentCallSid, () => ({ ...initial, recordingId: "r1" }), deps);
  const write = deps.writeState!;
  deps.writeState = async () => { throw new Error("store unavailable"); };
  await assert.rejects(lib.notifyReturnCallVoicemail(parentCallSid, playbackUrl, deps), /store unavailable/);
  assert.equal(sent.length, 0);
  deps.writeState = write;
  await lib.updateReturnCallState(parentCallSid, (s) => ({ ...s!, emailStatus: "sending" }), deps);
  assert.equal((await lib.notifyReturnCallVoicemail(parentCallSid, playbackUrl, deps)).emailStatus, "sending");
  assert.equal(sent.length, 0);
});

test("missing recording does not send and email wording does not invent interest", async () => {
  const lib = await import("../src/lib/returnCallState");
  const { deps, sent } = fixture();
  assert.equal((await lib.notifyReturnCallVoicemail(parentCallSid, playbackUrl, deps)).status, "no_recording");
  assert.equal(sent.length, 0);
  const email = lib.buildReturnCallVoicemailEmail({ ...initial, recordingId: "r1", callerFrom: "<script>" }, playbackUrl);
  assert.match(email.text, /not establish the caller's identity, interest/);
  assert.doesNotMatch(email.html, /<script>/);
  assert.match(email.html, /Play voicemail/);
  assert.equal(email.messageId, lib.buildReturnCallVoicemailEmail({ ...initial, recordingId: "r1" }, playbackUrl).messageId);
  assert.throws(() => lib.buildReturnCallVoicemailEmail({ ...initial, recordingId: "r1" }, "http://example.com"), /playback URL/);
});

test("corrupt state fails closed and a thrown updater releases its lock", async () => {
  const lib = await import("../src/lib/returnCallState");
  const { deps } = fixture();
  await assert.rejects(lib.readReturnCallState(parentCallSid, { readState: async () => ({ ...initial, parentCallSid: "other" }) }), /Invalid persisted/);
  await assert.rejects(lib.updateReturnCallState(parentCallSid, () => { throw new Error("abort"); }, deps), /abort/);
  assert.deepEqual(await lib.updateReturnCallState(parentCallSid, () => initial, deps), initial);
});

test("strict BotState JSON decoding rejects corruption while legacy fallback is unchanged", async () => {
  const { parseGmailImportState } = await import("../src/lib/gmailImportState");
  assert.equal(parseGmailImportState("broken", "fallback"), "fallback");
  assert.equal(parseGmailImportState("", "fallback"), "fallback");
  assert.throws(() => parseGmailImportState("broken", null, true), /Invalid persisted BotState JSON/);
  assert.throws(() => parseGmailImportState("", null, true), /Invalid persisted BotState JSON/);
  assert.deepEqual(parseGmailImportState(JSON.stringify(initial), null, true), initial);
});

test("JSON-valid but schema-invalid return-call receipts fail closed", async () => {
  const lib = await import("../src/lib/returnCallState");
  for (const value of [0, false, [], {}, { ...initial, emailStatus: "unknown" }]) {
    await assert.rejects(lib.readReturnCallState(parentCallSid, {
      readState: async () => value as unknown as ReturnCallState,
    }), /Invalid persisted return-call state/);
  }
});
