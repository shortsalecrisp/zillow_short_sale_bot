import assert from "node:assert/strict";
import test from "node:test";
import { evaluateFinalReceiptCircuit, evaluateInboundQuietGate } from "../src/lib/voiceSafety";

function voiceRow(rowNumber: number, sentAt: string, result = "") {
  const values: unknown[] = [];
  values[32] = sentAt;
  values[33] = result;
  return { rowNumber, values };
}

test("two stale blank final receipts open the circuit", () => {
  const status = evaluateFinalReceiptCircuit(
    [voiceRow(5926, "2026-09-29T18:04:18Z"), voiceRow(5927, "2026-09-29T18:06:00Z")],
    new Date("2026-09-29T20:00:00Z"),
    new Date("2026-09-29T18:00:00Z"),
  );
  assert.equal(status.open, true);
  assert.deepEqual(status.evidence.map((item) => item.rowNumber), [5927, 5926]);
});

test("a completed receipt and a fresh start do not open the circuit", () => {
  const status = evaluateFinalReceiptCircuit(
    [voiceRow(5926, "2026-09-29T18:04:18Z", "voicemail_reached"), voiceRow(5927, "2026-09-29T19:45:00Z")],
    new Date("2026-09-29T20:00:00Z"),
    new Date("2026-09-29T18:00:00Z"),
  );
  assert.equal(status.open, false);
});

test("legacy blank receipts before the monitor rollout do not open the circuit", () => {
  const monitorStartedAt = new Date("2026-09-30T09:46:05.445Z");
  const status = evaluateFinalReceiptCircuit(
    [voiceRow(5552, "2026-09-01T13:18:01.757Z"), voiceRow(5632, "2026-09-08T16:03:56.631Z")],
    new Date("2026-09-30T12:16:00Z"),
    monitorStartedAt,
  );
  assert.equal(status.open, false);
  assert.equal(status.monitorStartedAt, monitorStartedAt.toISOString());
  assert.deepEqual(status.evidence, []);
});

test("unprocessed or just-processed inbound blocks a call for the same phone", () => {
  const now = new Date("2026-09-29T18:04:18Z");
  const unprocessed = [["2026-09-29T18:03:56Z", "pending", "q1", "d1", "m1", "+14045550123", "STOP", "2026-09-29T18:03:56Z"]];
  assert.equal(evaluateInboundQuietGate("404-555-0123", unprocessed, now).reason, "unprocessed_inbound");

  const processed = [["2026-09-29T18:03:56Z", "processed", "q1", "d1", "m1", "+14045550123", "STOP", "2026-09-29T18:03:56Z"]];
  assert.equal(evaluateInboundQuietGate("404-555-0123", processed, now).reason, "recent_inbound");
  assert.equal(evaluateInboundQuietGate("404-555-9999", processed, now).blocked, false);
});
