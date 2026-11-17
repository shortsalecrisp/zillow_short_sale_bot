import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import test from "node:test";

const dir = path.join(import.meta.dirname, "../apps_script");
const diagnostic = fs.readFileSync(path.join(dir, "sms_diagnostics.js"), "utf8");
const unified = fs.readFileSync(path.join(dir, "zz_unified_post.js"), "utf8");
const chatbot = fs.readFileSync(path.join(dir, "sms_chatbot.js"), "utf8");
const pick = (src, name) => src.match(new RegExp(`^function ${name}\\([\\s\\S]*?^}`, "m"))[0];

function fixture() {
  const values = {}, logs = [], mails = [];
  let now = 1800000000000, failMail = false, failLog = false;
  class Clock extends Date { constructor(...args) { super(...(args.length ? args : [now])); } static now() { return now; } }
  const lock = { tryLock: () => true, releaseLock() {} };
  const props = { getProperty: k => values[k] || null, setProperty(k, v) { values[k] = v; },
    deleteProperty(k) { delete values[k]; } };
  const context = { Date: Clock, JSON, Object, Array, String, Number, Math,
    console: { log(line) { if (failLog) throw Error("log unavailable"); logs.push(JSON.parse(line)); } },
    PropertiesService: { getScriptProperties: () => props }, LockService: { getScriptLock: () => lock },
    MailApp: { sendEmail(mail) { mails.push(mail); if (failMail) throw Error("uncertain email acceptance"); } },
    SpreadsheetApp: new Proxy({}, { get() { throw Error("diagnostics must not access Sheets"); } }),
    getSmsSpreadsheet_() { throw Error("diagnostics must not access the SMS workbook"); } };
  vm.createContext(context);
  vm.runInContext(pick(unified, "maskSensitiveDebugText_"), context);
  vm.runInContext(diagnostic, context);
  vm.runInContext(pick(unified, "appendSmsDebugLog_"), context);
  vm.runInContext(pick(chatbot, "sendSystemAlertEmail_"), context);
  return { context, values, logs, mails, lock, advance(ms) { now += ms; },
    failMail() { failMail = true; }, failLog() { failLog = true; } };
}

test("native diagnostics are bounded and redact nested and embedded credentials without Sheet writes", () => {
  const h = fixture();
  h.context.appendSmsDebugLog_("followup", { request_id: "exact-request", lease_token: "lease-secret",
    raw_body: JSON.stringify({ token: "raw-secret", nested: { authorization: "Bearer nested-secret" } }),
    parameters: "token=url-secret&action=claim_pending_send", long: "x".repeat(200000) });
  const encoded = JSON.stringify(h.logs);
  assert.doesNotMatch(encoded, /lease-secret|raw-secret|nested-secret|url-secret/);
  assert.ok(encoded.length < 16000);
  assert.equal(h.logs[0].data.request_id, "exact-request");
});

test("oversized collections keep exact request anchors and a bounded record", () => {
  const h = fixture(), details = { request_id: "request", message_id: "message" };
  for (let i = 0; i < 35; i++) details["field" + i] = "\\\"".repeat(2000);
  h.context.appendSmsDebugLog_("large", details);
  assert.ok(JSON.stringify(h.logs[0]).length < 16000);
  assert.equal(h.logs[0].data.request_id, "request");
  assert.equal(h.logs[0].data.message_id, "message");
  assert.equal(h.logs[0].data.truncated, true);
});

test("repeated storage errors create one notice with durable counts, without suppressing other alerts", () => {
  const h = fixture();
  for (let i = 0; i < 10; i++) h.context.sendSystemAlertEmail_("SMS BOT ERROR", "Exception: The document cannot be modified. Perhaps it has grown too large?");
  assert.equal(h.mails.length, 1);
  assert.equal(JSON.parse(h.values.SMS_STORAGE_INCIDENT_V37).count, 10);
  h.context.sendSystemAlertEmail_("SMS BOT ERROR", "A different programming error");
  h.context.sendSystemAlertEmail_("LEAD UPDATE", "The document cannot be modified");
  assert.equal(h.mails.length, 3);
  h.advance(30 * 60 * 1000);
  h.context.sendSystemAlertEmail_("SMS BOT ERROR", "Service Spreadsheets failed");
  assert.equal(h.mails.length, 4);
  assert.match(h.mails[3].body, /11/);
});

test("uncertain email acceptance is never blindly repeated or marked delivered", () => {
  const h = fixture(); h.failMail();
  assert.throws(() => h.context.sendSystemAlertEmail_("SMS BOT ERROR", "Service Spreadsheets failed"), /uncertain/);
  h.context.sendSystemAlertEmail_("SMS BOT ERROR", "Service Spreadsheets failed");
  assert.equal(h.mails.length, 1);
  assert.equal(JSON.parse(h.values.SMS_STORAGE_INCIDENT_V37).notice_state, "attempted");
});

test("only a later successful queue write can cause the one recovery notice", () => {
  const h = fixture();
  h.context.sendSystemAlertEmail_("SMS BOT ERROR", "Service Spreadsheets failed");
  assert.equal(h.context.flushSmsStorageRecoveryV37_(), false);
  h.advance(1); h.context.noteSmsStorageWriteV37_("existing-scheduled-request");
  h.advance(1); h.context.sendSystemAlertEmail_("SMS BOT ERROR", "Service Spreadsheets failed");
  assert.equal(h.context.flushSmsStorageRecoveryV37_(), false);
  h.advance(1); h.context.noteSmsStorageWriteV37_("later-scheduled-request");
  assert.equal(h.context.flushSmsStorageRecoveryV37_(), true);
  assert.equal(h.mails.length, 2);
  assert.match(h.mails[1].body, /not handset delivery/);
  assert.equal(h.context.flushSmsStorageRecoveryV37_(), false);
});

test("busy incident lock sends no flood and performs no authorization or outbound action", () => {
  const h = fixture(); h.lock.tryLock = () => false;
  assert.equal(h.context.sendSystemAlertEmail_("SMS BOT ERROR", "Service Spreadsheets failed"), false);
  assert.equal(h.mails.length, 0);
  assert.equal(h.logs[0].stage, "storage_incident_lock_busy");
});

test("a native-log outage does not suppress the deduplicated operational alert", () => {
  const h = fixture(); h.failLog();
  h.context.sendSystemAlertEmail_("SMS BOT ERROR", "Service Spreadsheets failed");
  h.context.sendSystemAlertEmail_("SMS BOT ERROR", "Service Spreadsheets failed");
  assert.equal(h.mails.length, 1);
  assert.equal(JSON.parse(h.values.SMS_STORAGE_INCIDENT_V37).count, 2);
});
