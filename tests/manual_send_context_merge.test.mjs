import assert from "node:assert/strict";
import fs from "node:fs";
import test from "node:test";
import vm from "node:vm";

const source = fs.readFileSync(new URL("../apps_script/sms_outbox.js", import.meta.url), "utf8");
const start = source.indexOf("function mergeVerifiedManualSmsReceiptIntoHistoryV28_");
const end = source.indexOf("function mergeVerifiedManualSendReceiptsIntoConversationContextV28_", start);
assert.ok(start >= 0 && end > start);

const context = {
  normalizePendingSmsInboundText_: value => String(value ?? "").trim().replace(/\s+/g, " ").toLowerCase(),
};
vm.createContext(context);
vm.runInContext(source.slice(start, end), context);

test("verified manual receipt is inserted after its matching inbound and before later turns", () => {
  const history = [
    { role: "agent", text: "What about the buyer fee?", ts: "2026-10-26T15:29:00Z" },
    { role: "agent", text: "Let's talk tomorrow", ts: "2026-10-26T15:37:00Z" },
  ];
  const result = context.mergeVerifiedManualSmsReceiptIntoHistoryV28_(history, {
    receipt_id: "manual-proof-1",
    inbound_text: "What about the buyer fee?",
    reply_text: "We disclose the fee up front.",
    sent_at: "2026-10-26T15:30:49.000Z",
  });
  assert.equal(result.added, true);
  assert.deepEqual(JSON.parse(JSON.stringify(result.history)), [
    history[0],
    { role: "assistant", text: "We disclose the fee up front.", ts: "2026-10-26T15:30:49.000Z", receipt_id: "manual-proof-1", manual: true },
    history[1],
  ]);
});

test("verified manual receipt merge is idempotent", () => {
  const history = [
    { role: "agent", text: "What about the buyer fee?" },
    { role: "assistant", text: "We disclose the fee up front.", receipt_id: "manual-proof-1", manual: true },
  ];
  const result = context.mergeVerifiedManualSmsReceiptIntoHistoryV28_(history, {
    receipt_id: "manual-proof-1",
    inbound_text: "What about the buyer fee?",
    reply_text: "We disclose the fee up front.",
    sent_at: "2026-10-26T15:30:49.000Z",
  });
  assert.equal(result.duplicate, true);
  assert.equal(result.history.length, 2);
});

test("unverified or unmatched receipts do not alter history", () => {
  const history = [{ role: "agent", text: "Different inbound" }];
  const result = context.mergeVerifiedManualSmsReceiptIntoHistoryV28_(history, {
    receipt_id: "manual-proof-1",
    inbound_text: "What about the buyer fee?",
    reply_text: "We disclose the fee up front.",
    sent_at: "2026-10-26T15:30:49.000Z",
  });
  assert.equal(result.added, false);
  assert.equal(result.history.length, 1);
});
