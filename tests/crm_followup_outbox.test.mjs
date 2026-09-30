import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import vm from "node:vm";

const source = fs.readFileSync(path.join(import.meta.dirname, "../apps_script/sms_outbox.js"), "utf8");
const extract = (name, nextName) => source.slice(
  source.indexOf(`function ${name}(`),
  source.indexOf(`function ${nextName}(`),
);

function harness() {
  const rows = [];
  const sheet = {
    getLastRow: () => rows.length + 1,
    getRange: (_row, _col, count) => ({ getValues: () => rows.slice(0, count) }),
    appendRow: (row) => rows.push(row),
  };
  const context = {
    Date,
    JSON,
    Number,
    String,
    SMS_PENDING_SEND_HEADERS_: Array.from({ length: 19 }),
    normalizePhone_: (value) => String(value).replace(/\D/g, "").replace(/^1(?=\d{10}$)/, ""),
    normalizeWhitespace_: (value) => value.trim().replace(/\s+/g, " "),
    normalizePendingSmsInboundText_: (value) => String(value || "").trim().toLowerCase(),
    installSmsOutboxTriggers_: () => {},
    ensureSmsSheetHeaders_: () => {},
    getSmsSpreadsheet_: () => ({ getSheetByName: (name) => name === "sms_pending_sends" ? sheet : null }),
    LockService: { getScriptLock: () => ({ tryLock: () => true, releaseLock: () => {} }) },
    Utilities: { getUuid: () => "generated-id" },
    getSheet_: () => ({}),
    getSheetData_: () => [],
    HEADERS: { phone: "phone" },
  };
  vm.createContext(context);
  vm.runInContext(extract("getCrmFollowupSmsStatusV18_", "enqueueFollowupSmsV13_"), context);
  vm.runInContext(extract("enqueueFollowupSmsV13_", "resolveInitialSmsReceiptRowV14_"), context);
  vm.runInContext(extract("getPendingSmsStaleReason_", "findSmsCrmRowByPhone_"), context);
  return { context, rows };
}

const identity = {
  crm_prospect_id: "e1f3e796-ff41-4c8c-8991-da6ca179d988",
  phone: "14057723242",
  message: "Approved follow-up",
  message_id: "crm-followup-0123456789abcdef01234567",
  request_id: "render-crm-followup-0123456789abcdef01234567",
};

test("CRM-only approved text enters the existing Tasker outbox and dedupes", () => {
  const { context, rows } = harness();
  const first = context.enqueueFollowupSmsV13_(identity, "webhook-id");
  assert.equal(first.status, "queued");
  assert.equal(rows.length, 1);
  assert.equal(rows[0][17], 0);
  assert.match(rows[0][6], /^__crm_followup__:/);
  assert.equal(context.getPendingSmsStaleReason_(rows[0]), "");

  const duplicate = context.enqueueFollowupSmsV13_(identity, "another-webhook-id");
  assert.equal(duplicate.duplicate, true);
  assert.equal(rows.length, 1);

  rows[0][1] = "sent";
  rows[0][14] = new Date();
  assert.equal(context.getCrmFollowupSmsStatusV18_({ request_id: identity.request_id }).status, "sent");
});

test("CRM-only outbox stops uncertain duplicates and expired sends", () => {
  const { context, rows } = harness();
  context.enqueueFollowupSmsV13_(identity, "webhook-id");
  rows[0][1] = "uncertain";
  const duplicate = context.enqueueFollowupSmsV13_(identity, "another-webhook-id");
  assert.equal(duplicate.ok, false);
  assert.equal(rows.length, 1);

  rows[0][0] = new Date(Date.now() - 31 * 60 * 1000);
  assert.equal(context.getPendingSmsStaleReason_(rows[0]), "CRM follow-up expired before handset send");
});
