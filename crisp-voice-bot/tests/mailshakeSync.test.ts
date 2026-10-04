import assert from "node:assert/strict";
import test from "node:test";
import axios, { AxiosError } from "axios";
import { google } from "googleapis";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.GOOGLE_SERVICE_ACCOUNT_JSON = '{"type":"service_account","client_email":"test@example.com"}';
process.env.MAILSHAKE_API_KEY = "test-mailshake-key";
process.env.MAILSHAKE_SYNC_START_AT_ROW = "3000";

function httpError(status: number, code?: string): AxiosError {
  return new AxiosError("Provider request failed", undefined, undefined, undefined, {
    status, data: { code, error: "Do not copy raw errors or recipient PII to logs" }, headers: {}, statusText: "error", config: {} as never,
  });
}

test("legacy sync preflights campaigns and leaves blocked rows pending", async (t) => {
  const row = Array(29).fill("");
  row[0] = "Pat";
  row[3] = "pat@example.com";
  row[10] = "Y";
  const writes: unknown[] = [];
  const posts: unknown[] = [];
  let getResult: unknown;
  let getFailure: unknown;
  let postFailure: unknown;
  const fakeSheets = { spreadsheets: { values: {
    get: async ({ range }: { range: string }) => ({ data: { values: range.includes("!K") ? [["Y"]] : [row] } }),
    batchUpdate: async (value: unknown) => { writes.push(value); return {}; },
  } } };
  t.mock.method(google.auth, "fromJSON", () => ({}));
  t.mock.method(google, "sheets", () => fakeSheets);
  t.mock.method(axios, "get", async (url: string) => {
    assert.equal(url, "https://api.mailshake.com/2017-04-01/campaigns/get");
    if (getFailure) throw getFailure;
    return { data: getResult };
  });
  t.mock.method(axios, "post", async (url: string) => {
    assert.equal(url, "https://api.mailshake.com/2017-04-01/recipients/add");
    posts.push(url);
    if (postFailure) throw postFailure;
    return { data: { statusID: "mock-only" } };
  });
  const { runMailshakeSync } = await import("../src/lib/mailshakeSync");
  const scenarios = [
    { name: "archived even when unpaused", data: { id: 1476826, isArchived: true, isPaused: false }, reason: "campaign_archived" },
    { name: "paused", data: { id: 1476826, isArchived: false, isPaused: true }, reason: "campaign_paused" },
    { name: "missing", error: httpError(404, "not_found"), reason: "campaign_missing" },
    { name: "provider not_found at 400", error: httpError(400, "not_found"), reason: "campaign_missing" },
    { name: "provider unavailable", error: httpError(500, "internal_error"), reason: "campaign_unverified" },
    { name: "incomplete status", data: { id: 1476826 }, reason: "campaign_unverified" },
    { name: "wrong returned campaign", data: { id: 1525991, isArchived: false, isPaused: false }, reason: "campaign_unverified" },
  ];
  for (const scenario of scenarios) {
    await t.test(scenario.name, async () => {
      getResult = scenario.data;
      getFailure = scenario.error;
      writes.length = 0;
      posts.length = 0;
      const result = await runMailshakeSync();
      assert.equal(result.ok, false);
      assert.equal(result.blockedBatches[0]?.reason, scenario.reason);
      assert.deepEqual(result.blockedBatches[0]?.rows, [3000]);
      assert.deepEqual(result.pushedRows, []);
      assert.deepEqual(posts, []);
      assert.deepEqual(writes, []);
    });
  }
  await t.test("push failure is not reported as success or marked sent", async () => {
    getFailure = undefined;
    getResult = { id: 1476826, isArchived: false, isPaused: false };
    postFailure = httpError(400, "invalid_request");
    const result = await runMailshakeSync();
    assert.equal(result.ok, false);
    assert.equal(result.blockedBatches[0]?.reason, "recipient_push_failed");
    assert.equal(result.blockedBatches[0]?.providerCode, "invalid_request");
    assert.deepEqual(writes, []);
    assert.deepEqual(result.pushedRows, []);
  });
  await t.test("dry run reports archived destination without importing or marking rows", async () => {
    posts.length = 0;
    getFailure = undefined;
    getResult = { id: 1476826, isArchived: true, isPaused: true };
    const result = await runMailshakeSync({ dryRun: true });
    assert.equal(result.ok, false);
    assert.equal(result.blockedBatches[0]?.reason, "campaign_archived");
    assert.deepEqual(posts, []);
    assert.deepEqual(writes, []);
  });
  await t.test("dry run on an active destination does not import or mark rows", async () => {
    getResult = { id: 1476826, isArchived: false, isPaused: false };
    const result = await runMailshakeSync({ dryRun: true });
    assert.equal(result.ok, true);
    assert.deepEqual(result.blockedBatches, []);
    assert.deepEqual(posts, []);
    assert.deepEqual(writes, []);
  });
});
