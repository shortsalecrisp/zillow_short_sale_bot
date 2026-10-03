import assert from "node:assert/strict";
import test from "node:test";
import type { ReturnCallState, ReturnCallStateDependencies } from "../src/lib/returnCallState";
import type { ReturnCallStage } from "../src/lib/returnCallFlow";

process.env.BASE_URL = "https://voice.example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12176341017";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";

const parent = "v3:return-call-parent";
const child = "v3:return-call-office";
const caller = "+12175550102";
const office = "+14043009526";
const incoming = { CallSid: parent, From: caller, To: "+12176341017" };
const screened = { CallSid: child, ParentCallSid: parent, From: "+12176341017", To: office };
const recording = {
  CallSid: parent, RecordingStatus: "completed", RecordingSid: "77777777-1111-4111-8111-222222222222", RecordingDuration: "18",
};
const url = (stage: ReturnCallStage) => `https://voice.example.com/return-calls/${stage}?call=parent&sig=test`;

function fixture() {
  let state: ReturnCallState | null = null;
  const sent: unknown[] = [];
  const deps: ReturnCallStateDependencies = {
    readState: async () => state ? structuredClone(state) : null,
    writeState: async (_key, next) => { state = structuredClone(next); },
    sendMail: async (message) => { sent.push(message); },
  };
  return { deps, sent, get: () => state, set: (value: ReturnCallState) => { state = value; } };
}

test("incoming call dials only approved office without recording connected conversation", async () => {
  const { handleReturnCall } = await import("../src/routes/returnCalls");
  const f = fixture();
  const xml = await handleReturnCall("incoming", parent, incoming, url, f.deps);
  assert.match(xml, /<Number[^>]*>\+14043009526<\/Number>/);
  assert.match(xml, /record="do-not-record"/);
  assert.doesNotMatch(xml, /<Record /);
  assert.equal(f.get()?.callerFrom, caller);
});

test("office screening is private, requires keypad acceptance, and hangs up on timeout", async () => {
  const { handleReturnCall } = await import("../src/routes/returnCalls");
  const f = fixture();
  await handleReturnCall("incoming", parent, incoming, url, f.deps);
  const xml = await handleReturnCall("screen", parent, screened, url, f.deps);
  assert.match(xml, /Crisp return call\. Press 1 to connect\./);
  assert.match(xml, /<\/Gather><Hangup\/>/);
  assert.doesNotMatch(xml, /agent|listing|property/i);
  assert.equal(f.get()?.officeCallSid, child);
});

test("only press1 accepts; silence, wrong key and office voicemail do not bridge", async () => {
  const { handleReturnCall } = await import("../src/routes/returnCalls");
  const f = fixture();
  await handleReturnCall("incoming", parent, incoming, url, f.deps);
  await handleReturnCall("screen", parent, screened, url, f.deps);
  for (const digit of ["", "2", "11", "yes"]) {
    assert.match(await handleReturnCall("accept", parent, { ...screened, Digits: digit }, url, f.deps), /Hangup/);
    assert.equal(f.get()?.acceptedAt, undefined);
  }
  const vm = await handleReturnCall("dial-result", parent, { CallSid: parent, DialCallStatus: "completed" }, url, f.deps);
  assert.match(vm, /<Record /);
  assert.equal(f.get()?.acceptedAt, undefined);
  assert.ok(f.get()?.voicemailStartedAt);
});

test("accepted call and duplicate acceptance preserve receipt and do not offer voicemail", async () => {
  const { handleReturnCall } = await import("../src/routes/returnCalls");
  const f = fixture();
  await handleReturnCall("incoming", parent, incoming, url, f.deps);
  await handleReturnCall("screen", parent, screened, url, f.deps);
  const accepted = await handleReturnCall("accept", parent, { ...screened, Digits: "1" }, url, f.deps);
  assert.match(accepted, /<Response><\/Response>/);
  const acceptedAt = f.get()?.acceptedAt;
  await handleReturnCall("accept", parent, { ...screened, Digits: "1" }, url, f.deps);
  assert.equal(f.get()?.acceptedAt, acceptedAt);
  assert.match(await handleReturnCall("dial-result", parent, { CallSid: parent, DialCallStatus: "completed" }, url, f.deps), /Hangup/);
  assert.equal(f.get()?.voicemailStartedAt, undefined);
  assert.doesNotMatch(await handleReturnCall("incoming", parent, incoming, url, f.deps), /<Dial /);
});

test("no-answer and busy lead to bounded voicemail; office cannot accept after voicemail starts", async () => {
  const { handleReturnCall } = await import("../src/routes/returnCalls");
  for (const status of ["no-answer", "busy", "failed", "canceled"]) {
    const f = fixture();
    await handleReturnCall("incoming", parent, incoming, url, f.deps);
    await handleReturnCall("screen", parent, screened, url, f.deps);
    const xml = await handleReturnCall("dial-result", parent, { CallSid: parent, DialCallStatus: status }, url, f.deps);
    assert.match(xml, /maxLength="120"/);
    assert.match(xml, /playBeep="true"/);
    assert.match(await handleReturnCall("accept", parent, { ...screened, Digits: "1" }, url, f.deps), /Hangup/);
    assert.equal(f.get()?.acceptedAt, undefined);
    assert.doesNotMatch(await handleReturnCall("incoming", parent, incoming, url, f.deps), /<Dial /);
  }
});

test("stage mismatches, unknown calls and office leg substitution fail closed", async () => {
  const { handleReturnCall } = await import("../src/routes/returnCalls");
  const f = fixture();
  await assert.rejects(handleReturnCall("screen", parent, screened, url, f.deps), /Unknown/);
  await assert.rejects(handleReturnCall("incoming", parent, { ...incoming, To: office }, url, f.deps), /Invalid/);
  await handleReturnCall("incoming", parent, incoming, url, f.deps);
  await assert.rejects(handleReturnCall("screen", parent, { ...screened, ParentCallSid: "other" }, url, f.deps), /Invalid/);
  await handleReturnCall("screen", parent, screened, url, f.deps);
  await assert.rejects(handleReturnCall("accept", parent, { ...screened, CallSid: "other-child", Digits: "1" }, url, f.deps), /mismatch/);
  await assert.rejects(handleReturnCall("dial-result", parent, { CallSid: child }, url, f.deps), /mismatch/);
  await assert.rejects(handleReturnCall("recorded", parent, recording, url, f.deps), /not offered/);
});

test("recorded inbound account cannot be replaced on initial retries or later stages", async () => {
  const { handleReturnCall } = await import("../src/routes/returnCalls");
  const f = fixture();
  await handleReturnCall("incoming", parent, { ...incoming, AccountSid: "account-one" }, url, f.deps);
  await assert.rejects(handleReturnCall("incoming", parent, { ...incoming, AccountSid: "account-two" }, url, f.deps), /identity mismatch/);
  await assert.rejects(handleReturnCall("screen", parent, { ...screened, AccountSid: "account-two" }, url, f.deps), /account mismatch/);
  await handleReturnCall("screen", parent, { ...screened, AccountSid: "account-one" }, url, f.deps);
  assert.equal(f.get()?.officeCallSid, child);
});

test("voicemail recording callback replay sends one owner email and cannot substitute recording", async () => {
  const { handleReturnCall } = await import("../src/routes/returnCalls");
  const f = fixture();
  await handleReturnCall("incoming", parent, incoming, url, f.deps);
  await handleReturnCall("dial-result", parent, { CallSid: parent, DialCallStatus: "no-answer" }, url, f.deps);
  await Promise.all([
    handleReturnCall("recorded", parent, recording, url, f.deps),
    handleReturnCall("recorded", parent, recording, url, f.deps),
  ]);
  assert.equal(f.sent.length, 1);
  assert.equal(f.get()?.emailStatus, "sent");
  await assert.rejects(handleReturnCall("recorded", parent, { ...recording, RecordingSid: "99999999-1111-4111-8111-222222222222" }, url, f.deps), /replace/);
  assert.equal(f.sent.length, 1);
  assert.doesNotMatch(await handleReturnCall("dial-result", parent, { CallSid: parent }, url, f.deps), /<Record /);
});

test("recording metadata validation and record-done do not send owner email early", async () => {
  const { handleReturnCall } = await import("../src/routes/returnCalls");
  const f = fixture();
  await handleReturnCall("incoming", parent, incoming, url, f.deps);
  await handleReturnCall("dial-result", parent, { CallSid: parent }, url, f.deps);
  await handleReturnCall("recorded", parent, { ...recording, RecordingStatus: "in-progress" }, url, f.deps);
  await assert.rejects(handleReturnCall("recorded", parent, { ...recording, RecordingDuration: "NaN" }, url, f.deps), /duration/);
  await assert.rejects(handleReturnCall("recorded", parent, { ...recording, RecordingDuration: "999" }, url, f.deps), /duration/);
  await assert.rejects(handleReturnCall("recorded", parent, { ...recording, RecordingSid: "not-a-recording" }, url, f.deps), /recording ID/);
  assert.match(await handleReturnCall("record-done", parent, { CallSid: parent }, url, f.deps), /Thank you/);
  assert.equal(f.sent.length, 0);
});

test("acceptance committed during dial-result lookup must win over voicemail fallback", async () => {
  const { handleReturnCall } = await import("../src/routes/returnCalls");
  const f = fixture();
  await handleReturnCall("incoming", parent, incoming, url, f.deps);
  await handleReturnCall("screen", parent, screened, url, f.deps);
  const read = f.deps.readState!;
  let inject = true;
  f.deps.readState = async (key) => {
    const stale = await read(key);
    if (inject) {
      inject = false;
      f.set({ ...f.get()!, acceptedAt: "2026-10-03T20:01:00Z" });
    }
    return stale;
  };
  const xml = await handleReturnCall("dial-result", parent, { CallSid: parent }, url, f.deps);
  assert.doesNotMatch(xml, /<Record /);
  assert.equal(f.get()?.voicemailStartedAt, undefined);
});

test("voicemail committed during office screening lookup prevents a new gather", async () => {
  const { handleReturnCall } = await import("../src/routes/returnCalls");
  const f = fixture();
  await handleReturnCall("incoming", parent, incoming, url, f.deps);
  const read = f.deps.readState!;
  let inject = true;
  f.deps.readState = async (key) => {
    const stale = await read(key);
    if (inject) {
      inject = false;
      f.set({ ...f.get()!, voicemailStartedAt: "2026-10-03T20:01:00Z" });
    }
    return stale;
  };
  const result = await handleReturnCall("screen", parent, screened, url, f.deps).then(
    (xml) => ({ xml, error: undefined }),
    (error: unknown) => ({ xml: undefined, error }),
  );
  if (result.error) assert.match(String(result.error), /no longer|voicemail/i);
  else assert.doesNotMatch(result.xml!, /<Gather /);
  assert.equal(f.get()?.acceptedAt, undefined);
});
