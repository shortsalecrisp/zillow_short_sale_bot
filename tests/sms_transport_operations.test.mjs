import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import fs from "node:fs";
import test from "node:test";
import vm from "node:vm";

const outbox = fs.readFileSync(new URL("../apps_script/sms_outbox.js", import.meta.url), "utf8");
const chatbot = fs.readFileSync(new URL("../apps_script/sms_chatbot.js", import.meta.url), "utf8");
function fixture() {
  const mails = [], writes = [];
  let locks = 0;
  const context = {
    Date, JSON, String, Number, Object, Array, Math,
    PropertiesService: { getScriptProperties: () => ({ getProperty: () => "owner@example.test" }) },
    Utilities: { DigestAlgorithm: { SHA_256: "sha256" },
      computeDigest: (_, value) => [...createHash("sha256").update(value).digest()] },
    normalizePhone_: value => String(value || "").replace(/\D/g, "").slice(-10),
    normalizeWhitespace_: value => String(value || "").trim().replace(/\s+/g, " "),
    getHandoffDisplayName_: data => data.agent_name,
    formatPhoneForEmail_: phone => phone,
    formatPropertyAddressForEmail_: data => data.listing_address || "",
    formatConversationHistory_: () => "No conversation history available.",
    queueHandoffEmailV11_: payload => { mails.push(payload); return { ok: true }; },
  };
  vm.createContext(context);
  vm.runInContext(outbox, context);
  context.queueHandoffEmailV11_ = payload => { mails.push(payload); return { ok: true }; };
  vm.runInContext(chatbot.match(/^function sendHandoffEmail_\([\s\S]*?^}/m)[0], context);
  context.findConfirmedSmsReplyReceiptForPendingRow_ = () => ({ confirmed: false });
  context.getSmsLeadContextByPhone_ = () => ({ agent_name: "Heather", history: [] });
  context.lockSmsConversationAfterTransportAlertV10_ = () => { locks++; };
  const sheet = { getRange: (...args) => ({ setValue: value => writes.push({ args, value }) }) };
  const row = Array(19).fill("");
  Object.assign(row, { 1: "send_started", 2: "original-request", 3: "original-message",
    4: "7192105557", 5: "Outgoing reminder, not an incoming expression of interest" });
  return { context, mails, writes, row, sheet, locks: () => locks };
}

for (const reason of ["SMS SEND RESULT UNCERTAIN", "SMS SEND NOT CONFIRMED"]) {
  test(`${reason} queues an operational warning without creating a CRM takeover`, () => {
    const h = fixture();
    assert.equal(h.context.alertSmsOutboxProblem_(h.sheet, 2, h.row, reason), true);
    assert.equal(h.locks(), 0);
    assert.equal(h.mails.length, 1);
    assert.match(h.mails[0].subject, /^SMS DELIVERY WARNING/);
    assert.doesNotMatch(h.mails[0].subject, /NEW LEAD/);
    assert.match(h.mails[0].body, /not evidence of agent interest/);
    assert.match(h.mails[0].body, /Outgoing SMS awaiting confirmation:/);
    assert.match(h.mails[0].body, /original-request/);
    assert.doesNotMatch(h.mails[0].body, /Last message received:/);
  });
}

for (const reason of ["CALL REQUESTED", "DEFERRED HOT LEAD", "HUMAN HANDOFF UPDATE", "COMPLIANCE / LEGAL NOTICE"]) {
  test(`real owner handoff semantics remain separate: ${reason}`, () => {
    const h = fixture();
    h.context.queueCoalescedHandoffEmailV18_ = payload => { h.mails.push(payload); return { ok: true }; };
    h.context.sendHandoffEmail_({ handoff_type: reason, agent_name: "Heather", phone: "7192105557", last_message: "A real inbound question" });
    assert.equal(h.mails.length, 1);
    assert.doesNotMatch(h.mails[0].subject, /SMS DELIVERY WARNING/);
    assert.match(h.mails[0].body, /Last message received:/);
    assert.doesNotMatch(h.mails[0].body, /Outgoing SMS awaiting/);
  });
}

test("a correlated confirmed receipt still settles instead of emailing or locking", () => {
  const h = fixture();
  let settled = 0;
  h.context.findConfirmedSmsReplyReceiptForPendingRow_ = () => ({ confirmed: true });
  h.context.recordRecoveredSmsReplyReceipt_ = () => { settled++; };
  assert.equal(h.context.alertSmsOutboxProblem_(h.sheet, 2, h.row, "SMS SEND RESULT UNCERTAIN"), false);
  assert.equal(settled, 1);
  assert.equal(h.mails.length, 0);
  assert.equal(h.locks(), 0);
});

test("uncertain follow-ups never gain an automatic retry from this repair", () => {
  const h = fixture();
  h.row[3] = "followup-video-" + "a".repeat(24);
  h.row[9] = 1;
  assert.equal(h.context.buildAutomaticSmsRetryRowV15_(h.row, Date.now()), null);
});
