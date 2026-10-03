import test from "node:test";
import assert from "node:assert/strict";
import { generateKeyPairSync, sign } from "node:crypto";
import { parseTelnyxForm, returnCallIncomingToken, returnCallUrl, verifyReturnCallLink, verifyTelnyxForm } from "../src/lib/returnCallFlow";

process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12176341017";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";

const { publicKey, privateKey } = generateKeyPairSync("ed25519");
const rawKey = publicKey.export({ format: "der", type: "spki" }).subarray(-32).toString("base64");
const secret = "a".repeat(64);
const settings = { enabled: true, publicKey: rawKey, signingSecret: secret, applicationId: "app", baseUrl: "https://example.com" };
const form = { AccountSid: "account", ConnectionId: "app", CallSid: "parent", From: "+12175550101", To: "+12176341017" };
const body = new URLSearchParams(form).toString();

test("signature binds exact form bytes and rejects stale or tampered callbacks", () => {
  const now = Date.now();
  const timestamp = String(Math.floor(now / 1000));
  const signature = sign(null, Buffer.from(`${timestamp}|${body}`), privateKey).toString("base64");
  assert.equal(verifyTelnyxForm(body, timestamp, signature, rawKey, now), true);
  assert.equal(verifyTelnyxForm(body + "&Digits=1", timestamp, signature, rawKey, now), false);
  assert.equal(verifyTelnyxForm(body, timestamp, signature, rawKey, now + 301000), false);
  assert.throws(() => parseTelnyxForm("Digits=1&Digits=2"), /Duplicate/);
});

test("call-stage signatures cannot cross calls, stages, or expiry", () => {
  const now = Date.now();
  const url = new URL(returnCallUrl(settings.baseUrl, "parent", "accept", secret, now));
  const expires = url.searchParams.get("expires")!;
  const signature = url.searchParams.get("sig")!;
  assert.equal(verifyReturnCallLink("parent", "accept", expires, signature, secret, now), true);
  assert.equal(verifyReturnCallLink("parent", "playback", expires, signature, secret, now), false);
  assert.equal(verifyReturnCallLink("other", "accept", expires, signature, secret, now), false);
  assert.equal(verifyReturnCallLink("parent", "accept", expires, signature, secret, now + 7 * 3600000), false);
});

test("initial instructions require capability and application; an invalid supplied signature never falls back", async () => {
  const { authorizeReturnCallRequest } = await import("../src/routes/returnCalls");
  const query = { token: returnCallIncomingToken(secret) };
  assert.equal(authorizeReturnCallRequest("incoming", query, body, undefined, undefined, settings).parent, "parent");
  assert.throws(() => authorizeReturnCallRequest("incoming", {}, body, undefined, undefined, settings), /authorization/);
  assert.throws(() => authorizeReturnCallRequest("incoming", query, body.replace("ConnectionId=app", "ConnectionId=other"), undefined, undefined, settings), /application/);
  assert.throws(() => authorizeReturnCallRequest("incoming", query, body, String(Math.floor(Date.now() / 1000)), "bad", settings), /signature/);
});

test("Gather permits documented missing ConnectionId but still requires stage capability", async () => {
  const { authorizeReturnCallRequest } = await import("../src/routes/returnCalls");
  const query = Object.fromEntries(new URL(returnCallUrl(settings.baseUrl, "parent", "accept", secret)).searchParams);
  const gather = new URLSearchParams({ AccountSid: "account", CallSid: "child", Digits: "1", To: "+14043009526" }).toString();
  assert.equal(authorizeReturnCallRequest("accept", query, gather, undefined, undefined, settings).parent, "parent");
  assert.throws(() => authorizeReturnCallRequest("screen", query, gather, undefined, undefined, settings), /stage signature/);
  assert.throws(() => authorizeReturnCallRequest("accept", {}, gather, undefined, undefined, settings), /identifier/);
});

test("recording notifications require provider signature in addition to call capability", async () => {
  const { authorizeReturnCallRequest } = await import("../src/routes/returnCalls");
  const query = Object.fromEntries(new URL(returnCallUrl(settings.baseUrl, "parent", "recorded", secret)).searchParams);
  assert.throws(() => authorizeReturnCallRequest("recorded", query, body, undefined, undefined, settings), /signature/);
  const timestamp = String(Math.floor(Date.now() / 1000));
  const signature = sign(null, Buffer.from(`${timestamp}|${body}`), privateKey).toString("base64");
  assert.equal(authorizeReturnCallRequest("recorded", query, body, timestamp, signature, settings).parent, "parent");
});
