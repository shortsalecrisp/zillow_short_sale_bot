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

test("rejected Apps Script queue refill runs the direct queue", async () => {
  const { requestVoiceQueueRefill } = await import("../src/lib/sheetUpdateClient");
  let directQueueRuns = 0;

  await requestVoiceQueueRefill({ rowNumber: 5890, callAttemptNumber: 2 }, {
    appsScriptPost: async () => ({
      data: { ok: false, code: "unauthorized", error: "Missing or invalid voice bot token" },
    }),
    processQueue: async () => {
      directQueueRuns += 1;
      return { ok: true, queued: false };
    },
  });

  assert.equal(directQueueRuns, 1);
});

test("accepted Apps Script queue refill does not run the direct queue", async () => {
  const { requestVoiceQueueRefill } = await import("../src/lib/sheetUpdateClient");
  let directQueueRuns = 0;

  await requestVoiceQueueRefill({ rowNumber: 5891, callAttemptNumber: 1 }, {
    appsScriptPost: async () => ({ data: { ok: true, queued: false } }),
    processQueue: async () => {
      directQueueRuns += 1;
      return { ok: true, queued: false };
    },
  });

  assert.equal(directQueueRuns, 0);
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

test("accepted terminal call failure is reconciled directly when Apps Script omits scheduling cleanup proof", async () => {
  const { postSheetUpdate } = await import("../src/lib/sheetUpdateClient");
  const directCalls: Array<{ rowNumber: number; callResult?: string; responseStatus?: string }> = [];

  await postSheetUpdate({
    rowNumber: 5965,
    callAttemptNumber: 1,
    callResult: "call_failed_before_completion",
    responseStatus: "Call failed before completion",
    voiceNotes: "provider details",
  }, {
    appsScriptPost: async () => ({ data: { ok: true, fieldsWritten: ["AH:callResult", "J:responseStatus"] } }),
    directSheetUpdate: async (rowNumber, payload) => {
      directCalls.push({ rowNumber, callResult: payload.callResult, responseStatus: payload.responseStatus });
      return ["AD:call_eligible", "AE:call_time_bucket", "AF:call_scheduled_for"];
    },
  });

  assert.deepEqual(directCalls, [{
    rowNumber: 5965,
    callResult: "call_failed_before_completion",
    responseStatus: undefined,
  }]);
});

test("accepted terminal call failure with complete scheduling proof does not run direct reconciliation", async () => {
  const { postSheetUpdate } = await import("../src/lib/sheetUpdateClient");
  let directCallCount = 0;

  await postSheetUpdate({
    rowNumber: 5965,
    callAttemptNumber: 1,
    callResult: "call_failed_before_completion",
  }, {
    appsScriptPost: async () => ({ data: { ok: true, fieldsWritten: [
      "AH:callResult", "AD:call_eligible", "AE:call_time_bucket", "AF:call_scheduled_for",
    ] } }),
    directSheetUpdate: async () => {
      directCallCount += 1;
      return [];
    },
  });

  assert.equal(directCallCount, 0);
});
