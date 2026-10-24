import assert from "node:assert/strict";
import test from "node:test";
import type { AxiosInstance } from "axios";
import type { sheets_v4 } from "googleapis";

process.env.BASE_URL = "https://fallback-test.invalid";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";
process.env.ELEVENLABS_AGENT_ID = "synthetic-fallback-agent";

function row(): unknown[] {
  const v = Array(42).fill("");
  v[0] = "Synthetic"; v[2] = "+12175550100";
  v[4] = "20 Pearl Street"; v[5] = "Hillsboro"; v[6] = "NH";
  v[8] = "x"; v[23] = "2026-10-12T12:00:00Z";
  v[29] = "yes"; v[30] = "voice_call_1_due"; v[31] = "2026-10-12T13:15:00Z";
  return v;
}

test("two business days are measured from local window end, across weekends and DST", async () => {
  const { getVoiceMissedWindowDeadline } = await import("../src/lib/voiceOverdueFallback");
  for (const [due, deadline] of [
    ["2026-10-12T13:15:00Z", "2026-10-14T13:45:00.000Z"],
    ["2026-10-16T13:15:00Z", "2026-10-20T13:45:00.000Z"],
    ["2026-10-16T19:00:00Z", "2026-10-20T19:45:00.000Z"],
    ["2026-10-30T13:15:00Z", "2026-11-03T14:45:00.000Z"],
  ]) assert.equal(getVoiceMissedWindowDeadline(new Date(due), "America/New_York").toISOString(), deadline);
  assert.equal(getVoiceMissedWindowDeadline(new Date("2026-10-16T16:15:00Z"), "America/Los_Angeles").toISOString(), "2026-10-20T16:45:00.000Z");
});

test("exact deadline expires once without inventing a call outcome", async () => {
  const { getVoiceOverdueFallbackPlan } = await import("../src/lib/voiceOverdueFallback");
  const v = row(), before = structuredClone(v);
  assert.equal(getVoiceOverdueFallbackPlan(6000, v, new Date("2026-10-14T13:44:59.999Z")), undefined);
  const p = getVoiceOverdueFallbackPlan(6000, v, new Date("2026-10-14T13:45:00Z"))!;
  assert.equal(p.callAttemptNumber, 1);
  assert.equal(p.deadline.toISOString(), "2026-10-14T13:45:00.000Z");
  assert.deepEqual(v, before);
  v[10] = "N";
  assert.equal(getVoiceOverdueFallbackPlan(6000, v, new Date("2026-10-19T17:00:00Z")), undefined);
});

test("native Sheets timestamps retain local clock time instead of expiring Friday evening texts early", async () => {
  const { parseVoiceBotDate, getVoiceBotFirstAttemptWindowName } = await import("../src/lib/voiceSheet");
  const { getVoiceOverdueFallbackPlan } = await import("../src/lib/voiceOverdueFallback");
  const serial = (Date.UTC(2026, 9, 16, 17, 5, 2) - Date.UTC(1899, 11, 30)) / 86_400_000;
  assert.equal(parseVoiceBotDate(serial)?.toISOString(), "2026-10-16T21:05:02.000Z");
  assert.equal(parseVoiceBotDate(Number.NaN), undefined);
  assert.equal(parseVoiceBotDate(1792420000000), undefined);
  const v = row(); v[23] = serial; v[31] = "";
  let phone = 2175550100;
  while (getVoiceBotFirstAttemptWindowName(`+1${phone}`) !== "reach_morning_v1") phone++;
  v[2] = `+1${phone}`;
  assert.equal(getVoiceOverdueFallbackPlan(6000, v, new Date("2026-10-20T13:45:00Z")), undefined);
  assert.equal(getVoiceOverdueFallbackPlan(6000, v, new Date("2026-10-21T13:45:00Z"))?.scheduledFor.toISOString(), "2026-10-19T13:15:00.000Z");
});

test("automatic replacement dates cannot reset the original missed-window clock", async () => {
  const { getVoiceOverdueFallbackPlan } = await import("../src/lib/voiceOverdueFallback");
  const v = row(); v[31] = "2026-10-20T13:15:00Z";
  v[41] = 'CODEX_VOICE_SCHEDULED_NO_START_V1 {"rowNumber":6000,"callAttemptNumber":1,"candidateDueAt":"2026-10-12T13:15:00Z"}';
  assert.equal(getVoiceOverdueFallbackPlan(6000, v, new Date("2026-10-19T17:00:00Z"))?.deadline.toISOString(), "2026-10-14T13:45:00.000Z");
  v[41] = "";
  assert.equal(getVoiceOverdueFallbackPlan(6000, v, new Date("2026-10-19T17:00:00Z")), undefined, "an explicit future schedule remains authoritative without an older automatic marker");
});

test("an unplaced automatic second attempt expires from the approved next slot, not an obsolete two-day date", async () => {
  const { getVoiceOverdueFallbackPlan } = await import("../src/lib/voiceOverdueFallback");
  const v = row(); v[32] = "2026-10-12T13:30:00Z"; v[33] = "voicemail_left";
  v[30] = "voice_call_2_due"; v[31] = "2026-10-14T19:00:00Z";
  const p = getVoiceOverdueFallbackPlan(6000, v, new Date("2026-10-14T19:45:00Z"))!;
  assert.equal(p.callAttemptNumber, 2);
  assert.equal(p.scheduledFor.toISOString(), "2026-10-12T19:00:00.000Z");
  assert.equal(p.deadline.toISOString(), "2026-10-14T19:45:00.000Z");
});

test("resolved failed starts do not freeze the original two-business-day deadline forever", async () => {
  const { getVoiceOverdueFallbackPlan } = await import("../src/lib/voiceOverdueFallback");
  const v = row(); v[31] = "2026-10-26T13:15:00Z";
  const pending = { rowNumber: 6000, callAttemptNumber: 1, callStartRequestId: "known-request" };
  v[41] = 'CODEX_VOICE_SCHEDULED_NO_START_V1 {"rowNumber":6000,"callAttemptNumber":1,"candidateDueAt":"2026-10-12T13:15:00Z"}\n' +
    'CODEX_VOICE_CALL_START_UNCERTAIN_V1 ' + JSON.stringify(pending);
  assert.equal(getVoiceOverdueFallbackPlan(6000, v, new Date("2026-10-23T22:00:00Z")), undefined);
  v[41] += '\nCODEX_VOICE_CALL_START_RESOLUTION_V1 ' + JSON.stringify({ ...pending, phone: v[2], conversationId: "conv_failed", status: "definitive_failure" });
  assert.equal(getVoiceOverdueFallbackPlan(6000, v, new Date("2026-10-23T22:00:00Z"))?.deadline.toISOString(), "2026-10-14T13:45:00.000Z");
});

test("blocked initiation retains its real request history and expires as an unplaced first attempt", async () => {
  const { getVoiceOverdueFallbackPlan } = await import("../src/lib/voiceOverdueFallback");
  const v = row(); v[23] = "2026-10-23T12:00:00Z"; v[31] = "";
  v[32] = "2026-10-23T13:15:18Z"; v[33] = "provider_destination_blocked";
  const p = getVoiceOverdueFallbackPlan(6000, v, new Date("2026-10-27T13:45:00Z"))!;
  assert.equal(p.callAttemptNumber, 1);
  assert.equal(p.deadline.toISOString(), "2026-10-27T13:45:00.000Z");
  assert.equal(v[32], "2026-10-23T13:15:18Z");
});

test("campaign codes, human preferences, pending starts, malformed evidence and pre-cohort rows stay protected", async () => {
  const { getVoiceOverdueFallbackPlan } = await import("../src/lib/voiceOverdueFallback");
  const now = new Date("2026-10-19T17:00:00Z");
  for (const [index, value] of [[10, "N"], [10, "Y"], [10, "G"], [10, "O"], [15, "requested"], [16, "yes"], [19, "yes"],
    [35, "yes"], [36, "yes"], [37, "yes"], [38, "tomorrow"], [9, "Email information requested"], [9, "Please remove me"],
    [32, "unknown"], [32, "2026-10-12T13:16:00Z"], [39, "2026-10-13T13:16:00Z"],
    [41, "CODEX_VOICE_CALL_START_UNCERTAIN_V1 pending"], [31, "2026-04-23T13:15:00Z"], [6, "ZZ"],
  ] as Array<[number, unknown]>) {
    const v = row(); v[index] = value;
    assert.equal(getVoiceOverdueFallbackPlan(6000, v, now), undefined, `column index ${index}: ${value}`);
  }
});

test("terminal fallback cannot be reopened by the no-start recovery or a delayed generic classifier", async () => {
  const lib = await import("../src/lib/voiceOverdueFallback");
  const q = await import("../src/lib/voiceQueue");
  const { buildVoiceLeadRowWrites } = await import("../src/lib/updateVoiceLeadRow");
  const v = row(); v[41] = lib.VOICE_OVERDUE_FALLBACK_MARKER + " {}";
  const now = new Date("2026-10-19T13:30:00Z");
  assert.equal(q.getVoiceBotCallCandidateFromRowValues(6000, v, now), undefined);
  assert.equal(q.getScheduledVoiceNoStartRecoveryPlan(6000, v, now), undefined);
  v[10] = "N";
  const writes = buildVoiceLeadRowWrites(v, { callAttemptNumber: 1, callResult: "voicemail_left" }, now);
  assert.equal(writes.some((w) => [11, 33, 34].includes(w.columnNumber)), false);
});

function detail(id: string, changes: Record<string, unknown> = {}) {
  return { conversation_id: id, status: "done", metadata: { start_time_unix_secs: Date.parse("2026-10-13T13:20:00Z") / 1000, call_duration_secs: 12 },
    conversation_initiation_client_data: { dynamic_variables: { phone: "+12175550100", callAttemptNumber: 1, testMode: false } }, ...changes };
}

test("provider receipt proof distinguishes accepted, failed, test and incomplete calls", async () => {
  const lib = await import("../src/lib/voiceOverdueFallback");
  assert.equal(lib.parseVoiceProviderEvidence(detail("proof_accepted"))?.accepted, true);
  assert.equal(lib.parseVoiceProviderEvidence(detail("proof_failed", { status: "failed", metadata: { start_time_unix_secs: 1791897600, call_duration_secs: 0 } }))?.accepted, false);
  assert.equal(lib.parseVoiceProviderEvidence(detail("proof_test", { conversation_initiation_client_data: { dynamic_variables: { testMode: true } } })), null);
  assert.throws(() => lib.parseVoiceProviderEvidence({ conversation_id: "proof_unknown", status: "in-progress" }), /incomplete/);
  const p = lib.getVoiceOverdueFallbackPlan(6000, row(), new Date("2026-10-19T17:00:00Z"))!;
  const proof = { since: p.evidenceSince, verifiedAt: new Date(), calls: [lib.parseVoiceProviderEvidence(detail("proof_match"))!] };
  assert.equal(lib.hasAcceptedVoiceFallbackEvidence(p, proof), true);
  assert.equal(lib.hasAcceptedVoiceFallbackEvidence({ ...p, phone: "+12025550123" }, proof), false);
  assert.throws(() => lib.hasAcceptedVoiceFallbackEvidence(p, { ...proof, since: new Date("2026-10-19T00:00:00Z") }), /does not cover/);
});

test("provider history is fully paginated and rechecks the head for calls arriving during the scan", async () => {
  const lib = await import("../src/lib/voiceOverdueFallback");
  const seen: string[] = [];
  const client = { get: async (url: string, options?: any) => {
    seen.push(url);
    if (url.endsWith("/conversations")) {
      if (options.params.call_start_after_unix > 1792300000) return { data: { conversations: [{ conversation_id: "fallback_head_new" }], has_more: false } };
      return { data: options.params.cursor
        ? { conversations: [{ conversation_id: "fallback_page_2" }], has_more: false }
        : { conversations: [{ conversation_id: "fallback_page_1" }], has_more: true, next_cursor: "cursor_2" } };
    }
    return { data: detail(url.split("/").at(-1)!) };
  } } as unknown as Pick<AxiosInstance, "get">;
  const proof = await lib.scanVoiceFallbackEvidence(new Date("2026-10-12T00:00:00Z"), client);
  assert.equal(proof.calls.length, 3);
  assert.equal(seen.filter((url) => url.endsWith("/conversations")).length, 3);
});

test("missing pagination or inaccessible provider details fail closed", async () => {
  const { scanVoiceFallbackEvidence } = await import("../src/lib/voiceOverdueFallback");
  for (const data of [
    { conversations: [] },
    { conversations: [], has_more: true },
    { conversations: [{ conversation_id: "fallback_inaccessible" }], has_more: false },
  ]) {
    const client = { get: async (url: string) => {
      if (!url.endsWith("/conversations")) throw new Error("Provider read unavailable");
      return { data };
    } } as unknown as Pick<AxiosInstance, "get">;
    await assert.rejects(scanVoiceFallbackEvidence(new Date("2026-10-12T00:00:00Z"), client), /fallback held|unavailable/);
  }
});

test("fallback writes only K, cold schedule cells and audit notes, then verifies and stays idempotent", async () => {
  const lib = await import("../src/lib/voiceOverdueFallback");
  const { reconcileOverdueVoiceMailshakeFallback } = await import("../src/lib/voiceQueue");
  const now = new Date("2026-10-19T17:00:00Z");
  let current = row(), writes = 0;
  const original = structuredClone(current);
  const sheets = { spreadsheets: { values: {
    get: async ({ range }: { range: string }) => ({ data: { values: range.includes("sms_inbound_queue") ? [] : [structuredClone(current)] } }),
    batchUpdate: async ({ requestBody }: { requestBody: { data: Array<{ range: string; values: string[][] }> } }) => {
      writes++;
      for (const item of requestBody.data) {
        const letters = item.range.match(/!([A-Z]+)/)![1];
        const column = [...letters].reduce((n, ch) => n * 26 + ch.charCodeAt(0) - 64, 0);
        assert.ok([11, 30, 31, 32, 42].includes(column));
        current[column - 1] = item.values[0][0];
      }
      return { data: {} };
    },
  } } } as unknown as sheets_v4.Sheets;
  const dependencies = { clock: () => now,
    getEvidence: (since: Date) => ({ since, verifiedAt: now, calls: [] }),
  };
  const rows = [{ rowNumber: 6000, values: structuredClone(current) }];
  assert.equal(await reconcileOverdueVoiceMailshakeFallback(sheets, rows, now, dependencies), 1);
  assert.equal(current[10], "N");
  assert.deepEqual(current.slice(29, 32), ["", "", ""]);
  assert.ok(String(current[41]).includes(lib.VOICE_OVERDUE_FALLBACK_MARKER));
  for (let i = 0; i < 42; i++) if (![10, 29, 30, 31, 41].includes(i)) assert.equal(current[i], original[i]);
  assert.equal(await reconcileOverdueVoiceMailshakeFallback(sheets, rows, now, dependencies), 0);
  assert.equal(writes, 1);
});

test("fresh owner state, duplicate phones, inbound replies and accepted provider starts veto release", async () => {
  const lib = await import("../src/lib/voiceOverdueFallback");
  const { reconcileOverdueVoiceMailshakeFallback } = await import("../src/lib/voiceQueue");
  const now = new Date("2026-10-19T17:00:00Z");
  for (const reason of ["owner", "duplicate", "inbound", "provider", "stale-proof"]) {
    const current = row();
    if (reason === "owner") current[10] = "O";
    const inbound = [["", "pending", "", "", "", "+12175550100", "", now.toISOString()]];
    const sheets = { spreadsheets: { values: {
      get: async ({ range }: { range: string }) => ({ data: { values: range.includes("sms_inbound_queue")
        ? reason === "inbound" ? inbound : [] : [structuredClone(current)] } }),
      batchUpdate: async () => { throw new Error(`Must not release: ${reason}`); },
    } } } as unknown as sheets_v4.Sheets;
    const rows = [{ rowNumber: 6000, values: row() }];
    if (reason === "duplicate") rows.push({ rowNumber: 6001, values: row() });
    const released = await reconcileOverdueVoiceMailshakeFallback(sheets, rows, now, {
      clock: () => now,
      getEvidence: (since) => ({ since, verifiedAt: new Date(now.getTime() - (reason === "stale-proof" ? 31_000 : 0)),
        calls: reason === "provider" ? [lib.parseVoiceProviderEvidence(detail("veto_accepted"))!] : [] }),
    });
    assert.equal(released, 0, reason);
  }
});
