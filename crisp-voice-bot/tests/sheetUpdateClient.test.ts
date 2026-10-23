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

test("voice queue refill uses backend guards without calling Apps Script", async () => {
  const { requestVoiceQueueRefill } = await import("../src/lib/sheetUpdateClient");
  let directQueueRuns = 0;

  await requestVoiceQueueRefill({ rowNumber: 5890, callAttemptNumber: 2 }, {
    processQueue: async () => {
      directQueueRuns += 1;
      return { ok: true, queued: false };
    },
  });

  assert.equal(directQueueRuns, 1);
});

test("an empty backend pass completes without retrying or claiming a call started", async () => {
  const { requestVoiceQueueRefill } = await import("../src/lib/sheetUpdateClient");
  let directQueueRuns = 0;

  await requestVoiceQueueRefill({ rowNumber: 5891, callAttemptNumber: 1 }, {
    processQueue: async () => {
      directQueueRuns += 1;
      return { ok: true, queued: false };
    },
  });

  assert.equal(directQueueRuns, 1);
});

test("backend refill failures propagate instead of claiming a successful refill", async () => {
  const { requestVoiceQueueRefill } = await import("../src/lib/sheetUpdateClient");
  await assert.rejects(requestVoiceQueueRefill({}, {
    processQueue: async () => { throw new Error("queue unavailable"); },
  }), /queue unavailable/);
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

test("final unanswered closeout uses the guarded direct writer even when the relay would accept", async () => {
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

  assert.equal(directCallCount, 1);
});

test("final unanswered closeout supplies N and propagates readback failures", async () => {
  const { postSheetUpdate } = await import("../src/lib/sheetUpdateClient");
  let relayCalls = 0;
  await assert.rejects(postSheetUpdate({
    rowNumber: 6322, callAttemptNumber: 2, callResult: "voicemail_reached_final_attempt",
  }, {
    appsScriptPost: async () => { relayCalls += 1; return { data: { ok: true } }; },
    directSheetUpdate: async (_row, payload) => {
      assert.equal(payload.leadStatusCode, "N");
      throw new Error("Synthetic final disposition readback mismatch");
    },
  }), /final disposition readback mismatch/);
  assert.equal(relayCalls, 0);
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

test("accepted retryable call failure also requires scheduling proof", async () => {
  const { postSheetUpdate } = await import("../src/lib/sheetUpdateClient");
  let directCallCount = 0;
  await postSheetUpdate({
    rowNumber: 6044,
    callAttemptNumber: 1,
    callResult: "call_failed_before_completion_retryable",
  }, {
    appsScriptPost: async () => ({ data: { ok: true, fieldsWritten: ["AH:callResult"] } }),
    directSheetUpdate: async () => {
      directCallCount += 1;
      return ["AD:call_eligible", "AE:call_time_bucket", "AF:call_scheduled_for"];
    },
  });
  assert.equal(directCallCount, 1);
});

test("accepted human-answer no-bot-response always gets direct scheduling readback", async () => {
  const { postSheetUpdate } = await import("../src/lib/sheetUpdateClient");
  const directCalls: Array<{ rowNumber: number; callResult?: string }> = [];
  await postSheetUpdate({
    rowNumber: 6055,
    callAttemptNumber: 1,
    callResult: "human_answered_no_bot_response",
    responseStatus: "Human answered; bot did not respond",
  }, {
    appsScriptPost: async () => ({ data: { ok: true, fieldsWritten: [
      "AH:callResult", "AD:call_eligible", "AE:call_time_bucket", "AF:call_scheduled_for",
    ] } }),
    directSheetUpdate: async (rowNumber, payload) => {
      directCalls.push({ rowNumber, callResult: payload.callResult });
      return ["AH:callResult", "AD:call_eligible", "AE:call_time_bucket", "AF:call_scheduled_for"];
    },
  });
  assert.deepEqual(directCalls, [{ rowNumber: 6055, callResult: "human_answered_no_bot_response" }]);
});

test("retry scheduling uses fresh direct guards before any relay can overwrite caller preferences", async () => {
  const { postSheetUpdate } = await import("../src/lib/sheetUpdateClient");
  let writes = 0;
  await postSheetUpdate({ rowNumber: 123, callAttemptNumber: 1, callResult: "agent_not_available" }, {
    appsScriptPost: async () => { assert.fail("retry must not reach the unguarded relay first"); },
    directSheetUpdate: async () => { writes++; return ["AF:call_scheduled_for"]; },
  });
  assert.equal(writes, 1);
});

test("accepted retryable outcome fails closed when scheduling readback cannot be proved", async () => {
  const { postSheetUpdate } = await import("../src/lib/sheetUpdateClient");
  await assert.rejects(postSheetUpdate({
    rowNumber: 6055,
    callAttemptNumber: 1,
    callResult: "human_answered_no_bot_response",
  }, {
    appsScriptPost: async () => ({ data: { ok: true, fieldsWritten: ["AH:callResult"] } }),
    directSheetUpdate: async () => { throw new Error("Synthetic readback mismatch"); },
  }), /Synthetic readback mismatch/);
});
