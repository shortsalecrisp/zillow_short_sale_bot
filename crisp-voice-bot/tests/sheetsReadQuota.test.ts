import assert from "node:assert/strict";
import { test } from "node:test";
import { createSheetsReadQuota } from "../src/lib/sheetsReadQuota";

function fake() {
  let now = 0;
  const waits: number[] = [];
  const read = createSheetsReadQuota({ now: () => now, jitter: () => 0,
    sleep: async ms => { waits.push(ms); now += ms; } });
  return { read, now: () => now, waits };
}

test("429 waits for replenishment and performs a fresh readback", async () => {
  const { read, waits } = fake();
  let freshReads = 0;
  const result = await read(async () => {
    if (++freshReads === 1) throw { response: { status: 429 } };
    return "verified current receipt";
  });
  assert.equal(result, "verified current receipt");
  assert.equal(freshReads, 2);
  assert.deepEqual(waits, [60_000]);
});

test("repeated 429 leaves unverified result pending without swallowing the error", async () => {
  const { read, waits } = fake();
  let reads = 0;
  const error = { status: 429 };
  await assert.rejects(read(async () => { reads++; throw error; }), value => value === error);
  assert.equal(reads, 2);
  assert.deepEqual(waits, [60_000]);
});

test("read budget coordinates independent callers and never serves cached data", async () => {
  const { read, now } = fake();
  let version = 0;
  for (let i = 0; i < 45; i++) await read(async () => ++version);
  const result = await read(async () => ++version);
  assert.equal(result, 46);
  assert.equal(now(), 60_000);
});

test("non-quota failures propagate without retrying", async () => {
  const { read, waits } = fake();
  const error = new Error("forbidden");
  let reads = 0;
  await assert.rejects(read(async () => { reads++; throw error; }), error);
  assert.equal(reads, 1);
  assert.deepEqual(waits, []);
});
