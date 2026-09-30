import assert from "node:assert/strict";
import test from "node:test";

process.env.BASE_URL = "https://example.com";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.GOOGLE_APPS_SCRIPT_TOKEN = "queue-secret";
process.env.GOOGLE_APPS_SCRIPT_WEBHOOK_URL = "https://script.example.com/callback";

test("voice queue refill payload uses the shared Apps Script token", async () => {
  const { buildVoiceQueueRefillPayload } = await import("../src/lib/sheetUpdateClient");

  assert.deepEqual(buildVoiceQueueRefillPayload(), {
    token: "queue-secret",
    action: "process_voice_queue",
  });
});

test("Apps Script sheet response requires an explicit ok result", async () => {
  const { isAppsScriptSheetUpdateAccepted } = await import("../src/lib/sheetUpdateClient");

  assert.equal(isAppsScriptSheetUpdateAccepted({ ok: true }), true);
  assert.equal(isAppsScriptSheetUpdateAccepted('{"ok":true}'), true);
  assert.equal(isAppsScriptSheetUpdateAccepted({ ok: false, code: "row_update_failed" }), false);
  assert.equal(isAppsScriptSheetUpdateAccepted("<html>temporarily unavailable</html>"), false);
});

test("rejected Apps Script sheet update falls back to the direct writer", async () => {
  const { postSheetUpdate } = await import("../src/lib/sheetUpdateClient");
  const directCalls: Array<{ rowNumber: number; callResult?: string }> = [];

  await postSheetUpdate({
    rowNumber: 5890,
    callAttemptNumber: 2,
    callResult: "voicemail_reached_final_attempt",
    responseStatus: "Voicemail reached on final attempt - message not confirmed",
    leadStatusCode: "N",
  }, {
    appsScriptPost: async () => ({
      data: { ok: false, code: "row_update_failed", error: "Could not obtain lock" },
    }),
    directSheetUpdate: async (rowNumber, payload) => {
      directCalls.push({ rowNumber, callResult: payload.callResult });
      return ["AO:callResult"];
    },
  });

  assert.deepEqual(directCalls, [{
    rowNumber: 5890,
    callResult: "voicemail_reached_final_attempt",
  }]);
});

test("Apps Script transport timeout falls back to the direct writer", async () => {
  const { postSheetUpdate } = await import("../src/lib/sheetUpdateClient");
  let directCallCount = 0;

  await postSheetUpdate({
    rowNumber: 5887,
    callAttemptNumber: 2,
    callResult: "voicemail_reached_final_attempt",
  }, {
    appsScriptPost: async () => {
      throw new Error("timeout of 10000ms exceeded");
    },
    directSheetUpdate: async () => {
      directCallCount += 1;
      return ["AO:callResult"];
    },
  });

  assert.equal(directCallCount, 1);
});

test("successful Apps Script sheet update does not invoke the fallback", async () => {
  const { postSheetUpdate } = await import("../src/lib/sheetUpdateClient");
  let directCallCount = 0;

  await postSheetUpdate({
    rowNumber: 5891,
    callAttemptNumber: 2,
    callResult: "no_response_second_attempt",
  }, {
    appsScriptPost: async () => ({ data: { ok: true, fieldsWritten: ["AO:callResult"] } }),
    directSheetUpdate: async () => {
      directCallCount += 1;
      return [];
    },
  });

  assert.equal(directCallCount, 0);
});
