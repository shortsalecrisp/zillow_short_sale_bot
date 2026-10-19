import assert from "node:assert/strict";
import test, { type TestContext } from "node:test";
import { setImmediate as flush } from "node:timers/promises";

process.env.BASE_URL = "https://capacity-test.invalid";
process.env.TELNYX_API_KEY = "test";
process.env.TELNYX_CALLER_ID = "+12175550100";
process.env.TELNYX_CONNECTION_ID = "test";
process.env.TELNYX_OUTBOUND_VOICE_PROFILE_ID = "test";
process.env.TEST_DESTINATION_NUMBER = "+12175550101";

async function fixtures() {
  const q = await import("../src/lib/voiceQueue");
  const s = await import("../src/lib/voiceSheet");
  let number = 2025550100;
  const rows = Array.from({ length: 40 }, (_, index) => {
    let phone: string;
    do { phone = `+1${number++}`; } while (s.getVoiceBotFirstAttemptWindowName(phone) !== "reach_morning_v1");
    const values: unknown[] = Array(42).fill("");
    values[0] = "Synthetic";
    values[1] = "Agent";
    values[2] = phone;
    values[4] = "20 Pearl Street";
    values[5] = "Hillsboro";
    values[6] = "NH";
    values[8] = "x";
    values[23] = "2026-10-16T12:00:00Z";
    return { rowNumber: 6000 + index, values };
  });
  return { q, rows };
}

test("parallel dispatch is bounded, visits each candidate once, and settles before unlocking", async () => {
  const { q } = await fixtures();
  const releases: Array<() => void> = [];
  const visited: number[] = [];
  let settled = false;
  const run = q.dispatchVoiceQueueBatch([1, 2, 3, 4], async (candidate) => {
    visited.push(candidate);
    await new Promise<void>((resolve) => releases.push(resolve));
  }).then(() => { settled = true; });
  await flush();
  assert.deepEqual(visited, [1, 2]);
  releases.shift()!();
  await flush();
  assert.deepEqual(visited, [1, 2, 3]);
  assert.equal(settled, false);
  releases.shift()!();
  await flush();
  assert.deepEqual(visited, [1, 2, 3, 4]);
  while (releases.length) releases.shift()!();
  await run;
});

test("a persistence error stops new dispatches and waits for the other in-flight worker", async () => {
  const { q } = await fixtures();
  let release!: () => void;
  const visited: number[] = [];
  const run = q.dispatchVoiceQueueBatch([1, 2, 3, 4], async (candidate) => {
    visited.push(candidate);
    if (candidate === 1) throw new Error("synthetic persistence failure");
    await new Promise<void>((resolve) => { release = resolve; });
  });
  const rejected = assert.rejects(run, /synthetic persistence failure/);
  await flush();
  assert.deepEqual(visited, [1, 2]);
  release();
  await rejected;
  assert.deepEqual(visited, [1, 2]);
});

test("slot reservations cover starts not yet written and do not double-count durable attempts", async () => {
  const { q, rows } = await fixtures();
  const now = new Date("2026-10-20T13:16:00Z");
  const candidates = q.getVoiceBotCallCandidatesFromRows(rows, now, 4);
  assert.equal(q.countAllocatedVoiceBotSlots(rows, now, candidates.slice(0, 2)), 2);
  rows[0].values[32] = now.toISOString();
  assert.equal(q.countAllocatedVoiceBotSlots(rows, now, candidates.slice(0, 2)), 2);
  rows[0].values[33] = "voicemail_left";
  assert.equal(q.countAllocatedVoiceBotSlots(rows, now, candidates.slice(0, 2)), 1);
  assert.equal(q.hasVoiceQueueDispatchHeadroom(candidates[0], new Date("2026-10-20T13:43:59Z")), true);
  assert.equal(q.hasVoiceQueueDispatchHeadroom(candidates[0], new Date("2026-10-20T13:44:00Z")), false);
  assert.equal(q.hasVoiceQueueDispatchHeadroom(candidates[0], new Date("2026-10-20T13:45:00Z")), false);
});

async function replay(t: TestContext, cap: number, parallel: number, includeLongCalls = false) {
  const { q, rows } = await fixtures();
  const start = new Date("2026-10-20T13:15:00Z");
  t.mock.timers.enable({ apis: ["Date", "setTimeout"], now: start });
  let active: Promise<void> | undefined;
  let inFlight = 0, peakInFlight = 0, peakAllocated = 0;
  const starts: number[] = [];
  const run = (refill = false) => {
    if (active) {
      if (refill) void active.then(() => run());
      return;
    }
    const now = new Date();
    const candidates = q.getVoiceBotStartableCallCandidatesFromRows(rows, now, 10, cap);
    const reservations = new Map<string, typeof candidates[number]>();
    active = q.dispatchVoiceQueueBatch(candidates, async (candidate) => {
      const now = new Date();
      if (!q.hasVoiceQueueDispatchHeadroom(candidate, now)) return;
      if (q.countAllocatedVoiceBotSlots(rows, now, [...reservations.values()]) >= cap) return;
      reservations.set(candidate.phone, candidate);
      peakAllocated = Math.max(peakAllocated, q.countAllocatedVoiceBotSlots(rows, now, [...reservations.values()]));
      peakInFlight = Math.max(peakInFlight, ++inFlight);
      // Offline stress case: 45s start, 45s call, 60s final-receipt delay.
      await new Promise<void>((resolve) => setTimeout(resolve, 45_000));
      const row = rows.find((r) => r.rowNumber === candidate.rowNumber)!;
      row.values[32] = new Date().toISOString();
      starts.push(Date.now() - start.getTime());
      reservations.delete(candidate.phone);
      inFlight -= 1;
      const conversationMs = includeLongCalls && (candidate.rowNumber - 6000) % 20 === 0 ? 300_000 : 45_000;
      setTimeout(() => {
        row.values[33] = "voicemail_left";
        run(true);
      }, conversationMs + 60_000);
    }, parallel).finally(() => { active = undefined; });
  };
  run();
  for (let elapsed = 1_000; elapsed <= 30 * 60_000; elapsed += 1_000) {
    t.mock.timers.tick(1_000);
    await flush();
    if (elapsed % 60_000 === 0) run();
  }
  t.mock.timers.reset();
  return { callsStarted: starts.length, lastStartMinutes: Math.max(...starts) / 60_000, peakAllocated, peakInFlight };
}

test("two-slot sequential baseline cannot clear 40 calls in the stress replay", async (t) => {
  const result = await replay(t, 2, 1);
  t.diagnostic(JSON.stringify(result));
  assert.ok(result.callsStarted < 40);
  assert.ok(result.peakAllocated <= 2);
});

test("four slots and two parallel starts clear 40 within the unchanged Eastern morning window", async (t) => {
  const result = await replay(t, 4, 2);
  t.diagnostic(JSON.stringify(result));
  assert.equal(result.callsStarted, 40);
  assert.ok(result.lastStartMinutes < 29, "leave at least a minute before the local cutoff");
  assert.ok(result.peakAllocated <= 4);
  assert.equal(result.peakInFlight, 2);
});

test("the 40-call replay retains headroom with two five-minute conversations", async (t) => {
  const result = await replay(t, 4, 2, true);
  t.diagnostic(JSON.stringify(result));
  assert.equal(result.callsStarted, 40);
  assert.ok(result.lastStartMinutes < 29);
  assert.ok(result.peakAllocated <= 4);
});
